from __future__ import annotations

import shutil
import subprocess
import re
import hashlib
import base64
import os
import signal
from datetime import datetime, timezone
from urllib.parse import urlparse
from pathlib import Path
from typing import Any

from app.adapters.browser_ytdlp import BrowserYtDlpAdapter
from app.adapters.browser_search import content_links_from_anchors
from app.domain.enums import Capability, CollectorErrorCode, Platform
from app.domain.models import CollectedAccount, CollectedContent, CollectionBatch, CollectionRequest, CollectorError, MediaAsset
from app.adapters.browser_comments import comments_from_text_blocks
from app.adapters.streamlink_media import StreamlinkUnavailable, resolve_stream_url


class TikTokAdapter(BrowserYtDlpAdapter):
    # Observed signed-resource fallback payloads served by TikTok when a
    # video URL is not actually available to the logged-in browser.  Both
    # files are the same 2.066s TikTok logo/placeholder clip, not content.
    # Keep the exact fingerprints as a first-line guard; duration validation
    # below covers future variants of the same fallback.
    _PLACEHOLDER_VIDEO_SHA256 = {
        "821065e7f5f631c5aedeb4b23f6f456d5b268b430c17e828880796f6bc113894",
        "646701806dfbbea45d93da2a84ab0eb9a0173353f6f3e2cfd4eeb4b2620888b0",
    }
    _MIN_REAL_VIDEO_SECONDS = 3.0
    platform = Platform.TIKTOK
    content_type = "SHORT_VIDEO"

    @property
    def enabled(self) -> bool:
        return bool(self.settings.tiktok_enabled)

    @property
    def browser_name(self) -> str:
        return self.settings.tiktok_browser or "chromium"

    @property
    def browser_profile_path(self) -> Path | None:
        return self.settings.tiktok_browser_profile

    @property
    def output_root(self) -> Path:
        return self.settings.tiktok_output_root

    def _account_url(self, value: str) -> str:
        return f"https://www.tiktok.com/@{value.lstrip('@')}"

    def _target_to_url(self, request: CollectionRequest) -> str:
        """Use the stable profile's live entrypoint for every live probe.

        TikTok room/video URLs are ephemeral.  A continuous monitor must
        resolve the current room from ``/@handle/live`` on every polling
        round; only content jobs may use a direct /video or /t URL.
        """
        if request.capability == Capability.LIVE_RESOLVE:
            source = str(request.target.source_url or request.target.target_value or "").strip()
            match = re.search(r"tiktok\.com/@([^/?#]+)", source, flags=re.IGNORECASE)
            handle = match.group(1) if match else ""
            if not handle:
                for key in ("handle", "username", "uid"):
                    value = str(request.target.metadata.get(key) or "").strip()
                    if value:
                        handle = value.lstrip("@").split("/", 1)[0]
                        break
            if handle:
                return f"https://www.tiktok.com/@{handle}/live"
        return super()._target_to_url(request)

    def _content_url(self, value: str) -> str:
        if value.startswith("@"):
            return self._account_url(value)
        return f"https://www.tiktok.com/t/{value.lstrip('/')}"

    def discover(self, request: CollectionRequest, credential=None) -> CollectionBatch:
        target = str(request.target.source_url or request.target.target_value or "").lower()
        if request.capability.value == "LIVE_RESOLVE" and "/live" in target:
            return self._collect_live(request)
        try:
            return super().discover(request, credential)
        except CollectorError:
            if request.target.target_type == "ACCOUNT":
                browser_batch = self._collect_account_via_browser_links(request)
                if browser_batch.contents:
                    return browser_batch
            raise

    def _collect_keyword_content(self, request: CollectionRequest) -> CollectionBatch:
        """Resolve TikTok keyword links through the signed browser first.

        Keyword search already obtains concrete video URLs from the logged-in
        TikTok page.  Sending those URLs to yt-dlp first is both slower and
        less reliable: TikTok commonly returns a challenge page to that
        extractor path.  The shared CDP browser is the authoritative session
        for this stage and preserves the existing per-platform serialization.
        """
        return self._collect_content_via_browser(request)

    def _collect_account_via_browser_links(self, request: CollectionRequest) -> CollectionBatch:
        profile = self._browser_profile()
        if not profile:
            raise CollectorError(CollectorErrorCode.AUTH_EXPIRED, "TikTok browser profile is not configured", retryable=False)
        account_url = self._target_to_url(request)
        links: list[str] = []
        # TikTok's own Refresh button retries the signed video-grid API.  It
        # is more reliable than waiting for the generic CDP scraper to guess
        # when the SPA has finished its retry cycle.
        cdp_url = self._browser_cdp_url()
        if cdp_url:
            try:
                from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
                with sync_playwright() as playwright:
                    browser = playwright.chromium.connect_over_cdp(cdp_url, timeout=15_000)
                    context = browser.contexts[0] if browser.contexts else None
                    if not context:
                        raise RuntimeError("TikTok browser has no active context")
                    # The persistent page belongs to the login browser.  Do
                    # not navigate it during collection: TikTok's SPA keeps
                    # video resources and script caches alive for hours.
                    page = context.new_page()
                    try:
                        try:
                            page.goto(account_url, wait_until="domcontentloaded", timeout=15_000)
                        except PlaywrightTimeoutError:
                            try:
                                page.evaluate("window.stop()")
                            except Exception:
                                pass
                        page.wait_for_timeout(4_000)
                        for _ in range(2):
                            clicked = page.evaluate(
                                """() => {
                                  const node = [...document.querySelectorAll('button, a, [role="button"]')]
                                    .find(item => /refresh|重试/i.test(item.innerText || item.getAttribute('aria-label') || ''));
                                  if (node) { node.click(); return true; }
                                  return false;
                                }"""
                            )
                            if clicked:
                                page.wait_for_timeout(8_000)
                            anchors = page.evaluate("() => [...document.querySelectorAll('a[href]')].map(a => a.href || '')")
                            links = content_links_from_anchors([str(item) for item in anchors], platform=Platform.TIKTOK, limit=max(request.limit * 10, 10))
                            if links:
                                break
                    finally:
                        page.close()
            except Exception:
                links = []
        if not links:
            links = self.browser_search_collector.collect(
                url=account_url,
                platform=Platform.TIKTOK,
                profile=profile,
                browser=self.browser_name,
                limit=max(request.limit * 10, 10),
                timeout_seconds=int(request.target.metadata.get("browser_search_timeout_seconds") or request.target.metadata.get("timeout_seconds") or 60),
                cdp_url=cdp_url,
            )
        merged = CollectionBatch(raw={"target_url": account_url, "account_browser_links": links, "item_count": 0})
        seen_accounts: set[str] = set()
        seen_contents: set[str] = set()
        seen_assets: set[tuple[str, str, str, str]] = set()
        for link in links:
            if len(merged.contents) >= request.limit:
                break
            content_request = request.model_copy(
                update={"limit": 1, "target": request.target.model_copy(update={"target_type": "CONTENT", "target_value": link, "source_url": link})}
            )
            try:
                # The account-page parser is the path known to be blocked;
                # go straight to the browser-rendered content fallback for
                # the concrete video link instead of spending another retry
                # window in yt-dlp first.
                batch = self._collect_content_via_browser(content_request)
            except CollectorError:
                continue
            for account in batch.accounts:
                if account.external_id not in seen_accounts:
                    seen_accounts.add(account.external_id)
                    merged.accounts.append(account)
            for content in batch.contents:
                if content.external_id not in seen_contents:
                    seen_contents.add(content.external_id)
                    merged.contents.append(content)
            merged.comments.extend(item for item in batch.comments if item.external_id not in {c.external_id for c in merged.comments})
            for asset in batch.assets:
                key = (asset.content_external_id, asset.asset_type, asset.local_path, asset.url)
                if key not in seen_assets:
                    seen_assets.add(key)
                    merged.assets.append(asset)
        merged.next_cursor = {"last_external_id": merged.contents[-1].external_id} if merged.contents else {}
        merged.raw["item_count"] = len(merged.contents)
        return merged

    def _collect_content_via_browser(self, request: CollectionRequest) -> CollectionBatch:
        """Resolve a TikTok video from the logged-in page when yt-dlp's
        private API parser is blocked.

        TikTok still exposes the signed MP4 request in the page's resource
        timeline even when ``/api/post/item_list`` returns an empty body.  We
        use that browser-rendered URL for the media asset and keep the
        normalized content contract identical to the yt-dlp path.
        """
        cdp_url = str(self.settings.tiktok_browser_cdp_url or "").strip()
        if not cdp_url:
            raise CollectorError(CollectorErrorCode.AUTH_EXPIRED, "TikTok browser CDP is not configured", retryable=False)
        target_url = self._target_to_url(request)
        output_dir = (self.output_root / request.job_id).resolve()
        output_dir.mkdir(parents=True, exist_ok=True)
        try:
            from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
            with sync_playwright() as playwright:
                browser = playwright.chromium.connect_over_cdp(cdp_url, timeout=15_000)
                context = browser.contexts[0] if browser.contexts else None
                if not context:
                    raise RuntimeError("TikTok browser has no active context")
                # Keep the authenticated browser alive, but isolate each
                # video in a disposable renderer so media buffers cannot
                # accumulate in the login tab across the keyword batch.
                page = context.new_page()
                try:
                    page.goto(target_url, wait_until="domcontentloaded", timeout=15_000)
                except Exception:
                    try:
                        page.evaluate("window.stop()")
                    except Exception:
                        pass
                # Detail JSON normally arrives with the initial document.
                # Keep a short settle window for high-volume collection;
                # records still fail closed on missing core metadata/media.
                page.wait_for_timeout(1_800)
                # TikTok does not consistently expose a literal ``.mp4``
                # request.  A signed play URL can be a query-only CDN path,
                # or live only in SIGI_STATE / rehydration JSON until the
                # video element is explicitly played.  Extract all of those
                # sources, then validate the downloaded bytes as MP4 below.
                payload = page.evaluate(
                    """async () => {
                      const unique = (items) => [...new Set(items.filter((item) => typeof item === 'string' && /^https?:\\/\\//.test(item)))];
                      const urls = [];
                      const visited = new WeakSet();
                      const add = (value, depth = 0) => {
                        if (typeof value === 'string') urls.push(value.replace(/\\\\u002F/g, '/'));
                        if (Array.isArray(value) && depth < 12) value.forEach((item) => add(item, depth + 1));
                        if (value && typeof value === 'object') {
                          if (visited.has(value) || depth >= 12) return;
                          visited.add(value);
                          for (const [key, item] of Object.entries(value)) {
                            // Rehydration payloads keep the target video under
                            // arbitrary item IDs, so recurse through the
                            // bounded tree instead of assuming one schema.
                            if (/url|uri|play|download|video/i.test(key) || depth < 8) add(item, depth + 1);
                          }
                        }
                      };
                      const parseScript = (selector) => {
                        const text = document.querySelector(selector)?.textContent || '';
                        if (!text) return;
                        try { add(JSON.parse(text)); } catch (_) {}
                      };
                      parseScript('#SIGI_STATE');
                      parseScript('#__UNIVERSAL_DATA_FOR_REHYDRATION__');
                      parseScript('#__NEXT_DATA__');
                      try { add(window.SIGI_STATE); } catch (_) {}
                      try { add(window.__UNIVERSAL_DATA_FOR_REHYDRATION__); } catch (_) {}
                      // A rendered TikTok detail page carries the canonical
                      // metadata here.  Do not replace it with document.title:
                      // document.title is frequently just "TikTok - Make Your
                      // Day", which is neither a title nor a useful record.
                      let item = {};
                      try {
                        const hydrated = JSON.parse(document.querySelector('#__UNIVERSAL_DATA_FOR_REHYDRATION__')?.textContent || '{}');
                        const detail = hydrated?.__DEFAULT_SCOPE__?.['webapp.video-detail']?.itemInfo?.itemStruct || {};
                        item = {
                          id: detail.id || '', desc: detail.desc || '', createTime: detail.createTime || '',
                          author: {id: detail.author?.id || '', uniqueId: detail.author?.uniqueId || '', nickname: detail.author?.nickname || ''},
                          video: {duration: detail.video?.duration || 0, cover: detail.video?.cover || detail.video?.originCover || '', originCover: detail.video?.originCover || '', playAddr: detail.video?.playAddr || '', downloadAddr: detail.video?.downloadAddr || ''},
                          stats: {playCount: detail.stats?.playCount || 0, commentCount: detail.stats?.commentCount || 0, diggCount: detail.stats?.diggCount || 0, shareCount: detail.stats?.shareCount || 0}
                        };
                        add(item.video.playAddr); add(item.video.downloadAddr);
                      } catch (_) {}
                      const video = document.querySelector('video');
                      if (video) {
                        video.muted = true;
                        video.playsInline = true;
                        try { await video.play(); } catch (_) {}
                        await new Promise((resolve) => setTimeout(resolve, 3500));
                        add(video.currentSrc);
                        add(video.src);
                        for (const node of [...video.querySelectorAll('source')]) add(node.src || node.getAttribute('src'));
                      }
                      const resourceUrls = performance.getEntriesByType('resource').map((item) => item.name || '');
                      for (const url of resourceUrls) {
                        // Keep only plausible video delivery routes.  The
                        // final file-header check rejects playlists, HTML and
                        // subtitle responses before they become media assets.
                        if (/mime_type=video_mp4|\\.mp4(?:\\?|$)|aweme\\/v\d+\\/play|video\\/play|play_addr|playwm|downloadaddr|video_id=/i.test(url)) add(url);
                      }
                      const body = document.body?.innerText || '';
                      return {
                        title: document.title || '',
                        body: body.slice(0, 4000),
                        userAgent: navigator.userAgent || '',
                        media: unique(urls).slice(-30),
                        item
                      };
                    }"""
                ) or {}
                page.close()
                page = None
        except Exception as exc:
            raise CollectorError(CollectorErrorCode.NETWORK_TEMPORARY, f"TikTok browser content fallback failed: {exc}", retryable=True) from exc

        media_urls = [str(item) for item in (payload.get("media") or []) if str(item).startswith(("http://", "https://"))]
        media_url = next((item for item in reversed(media_urls) if "mime_type=video_mp4" in item or ".mp4" in item), media_urls[-1] if media_urls else "")
        if not media_url:
            raise CollectorError(CollectorErrorCode.NETWORK_TEMPORARY, "TikTok browser did not expose a signed video URL", retryable=True)
        parsed = urlparse(target_url)
        match = re.search(r"/video/(\d+)", parsed.path)
        item = payload.get("item") if isinstance(payload.get("item"), dict) else {}
        item_author = item.get("author") if isinstance(item.get("author"), dict) else {}
        item_video = item.get("video") if isinstance(item.get("video"), dict) else {}
        external_id = str(item.get("id") or (match.group(1) if match else parsed.path.rstrip("/").split("/")[-1])).strip()
        handle_match = re.search(r"/@([^/]+)", parsed.path)
        handle = str(item_author.get("uniqueId") or (handle_match.group(1) if handle_match else "")).strip()
        account_id = str(item_author.get("id") or handle or request.target.metadata.get("uid") or "").strip()
        published_at = None
        try:
            published_at = datetime.fromtimestamp(float(item.get("createTime")), timezone.utc)
        except (TypeError, ValueError, OSError, OverflowError):
            pass
        description = str(item.get("desc") or "").strip()
        title = description or str(payload.get("title") or "").strip()
        if title.lower() in {"tiktok - make your day", "tiktok"}:
            title = ""
        if not title:
            raise CollectorError(CollectorErrorCode.NETWORK_TEMPORARY, "TikTok detail metadata did not include a title/description", retryable=True)
        if not published_at:
            raise CollectorError(CollectorErrorCode.NETWORK_TEMPORARY, "TikTok detail metadata did not include createTime", retryable=True)
        detail_media_urls = [str(item_video.get(key) or "") for key in ("playAddr", "downloadAddr")]
        candidate_urls = [url for url in dict.fromkeys(detail_media_urls + list(reversed(media_urls))) if url.startswith(("http://", "https://"))]
        content = CollectedContent(
            platform=Platform.TIKTOK,
            external_id=external_id,
            url=target_url,
            content_type="SHORT_VIDEO",
            title=title,
            description=description,
            author_external_id=account_id,
            published_at=published_at,
            raw={"browser_fallback": True, "media_url": media_url, "author_handle": handle, "author_nickname": str(item_author.get("nickname") or ""), "duration_seconds": item_video.get("duration") or 0, "stats": item.get("stats") or {}},
        )
        batch = CollectionBatch(
            accounts=[CollectedAccount(platform=Platform.TIKTOK, external_id=account_id, username=f"@{handle}" if handle else "", nickname=str(item_author.get("nickname") or ""), profile_url=f"https://www.tiktok.com/@{handle}" if handle else "")],
            contents=[content],
            raw={"target_url": target_url, "item_count": 1, "browser_fallback": True},
        )
        if request.download_media:
            video_dir = output_dir / external_id
            video_dir.mkdir(parents=True, exist_ok=True)
            video_path = video_dir / f"{external_id}.mp4"
            download_error = ""
            # The signed CDN URL is issued to the logged-in browser.  Try
            # that session first; a standalone process is both slower and
            # frequently served a 2-second placeholder/challenge body.
            downloaded, download_error = self._download_video_via_browser(
                cdp_url=cdp_url,
                target_url=target_url,
                candidate_urls=candidate_urls[:3],
                video_path=video_path,
            )
            # A page can expose several CDN variants.  Try a small bounded
            # set only if the browser path itself is temporarily unavailable.
            if not downloaded:
                curl = shutil.which("curl")
                if curl:
                    for candidate_url in candidate_urls[:1]:
                        try:
                            video_path.unlink(missing_ok=True)
                        except OSError:
                            pass
                        command = [curl, "-L", "--fail", "--silent", "--show-error", "--connect-timeout", "10", "--max-time", "30", "-A", str(payload.get("userAgent") or "Mozilla/5.0"), "-e", "https://www.tiktok.com/", "-o", str(video_path), candidate_url]
                        result = subprocess.run(command, capture_output=True, text=True, timeout=35, check=False)
                        validation_error = self._video_quality_error(video_path) if result.returncode == 0 else "download failed"
                        if result.returncode == 0 and not validation_error:
                            media_url = candidate_url
                            downloaded = True
                            break
                        download_error = validation_error or result.stderr.strip()[:300] or "response was not a valid MP4"
            if not downloaded:
                try:
                    video_path.unlink(missing_ok=True)
                except OSError:
                    pass
                raise CollectorError(CollectorErrorCode.NETWORK_TEMPORARY, f"TikTok browser video download failed: {download_error}", retryable=True)
            batch.assets.append(MediaAsset(platform=Platform.TIKTOK, content_external_id=external_id, asset_type="VIDEO", local_path=str(video_path)))
            # Prefer TikTok's signed cover.  If it expires before sync, the
            # common path can still extract a thumbnail from the verified MP4.
            batch.assets.append(MediaAsset(platform=Platform.TIKTOK, content_external_id=external_id, asset_type="THUMBNAIL", url=str(item_video.get("cover") or item_video.get("originCover") or "")))
        batch.raw["media_url"] = media_url
        batch.raw["media_candidates"] = len(candidate_urls)
        profile = self._browser_profile()
        if profile and request.collect_comments:
            self._add_browser_comments(batch, request, target_url, profile)
        return batch

    def _download_video_via_browser(self, *, cdp_url: str, target_url: str, candidate_urls: list[str], video_path: Path) -> tuple[bool, str]:
        """Fetch a signed TikTok MP4 inside the authenticated browser.

        The transfer is deliberately single-file and capped at 50 MiB.  This
        path is a recovery path for session-bound CDN URLs, not a parallel
        downloader: it keeps the small collector host from accumulating large
        renderer buffers while retaining the browser's valid signed session.
        """
        if not candidate_urls:
            return False, "TikTok detail did not expose a playable media URL"
        try:
            from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
            with sync_playwright() as playwright:
                browser = playwright.chromium.connect_over_cdp(cdp_url, timeout=15_000)
                context = browser.contexts[0] if browser.contexts else None
                if not context:
                    return False, "TikTok browser has no active context"
                page = context.new_page()
                try:
                    try:
                        page.goto(target_url, wait_until="domcontentloaded", timeout=8_000)
                    except Exception:
                        try:
                            page.evaluate("window.stop()")
                        except Exception:
                            pass
                    page.wait_for_timeout(500)
                    last_error = ""
                    for candidate_url in candidate_urls:
                        response = page.evaluate(
                            """async (url) => {
                              try {
                                const response = await fetch(url, {credentials: 'include'});
                                const size = Number(response.headers.get('content-length') || 0);
                                const type = response.headers.get('content-type') || '';
                                if (!response.ok) return {error: `HTTP ${response.status}`};
                                if (!/video\\/mp4/i.test(type)) return {error: `unexpected content-type ${type}`};
                                if (size > 50 * 1024 * 1024) return {error: 'video exceeds 50MiB browser fallback limit'};
                                const bytes = new Uint8Array(await response.arrayBuffer());
                                if (bytes.byteLength > 50 * 1024 * 1024) return {error: 'video exceeds 50MiB browser fallback limit'};
                                let binary = '';
                                for (let start = 0; start < bytes.length; start += 0x8000) binary += String.fromCharCode(...bytes.subarray(start, start + 0x8000));
                                return {data: btoa(binary), size: bytes.byteLength};
                              } catch (error) { return {error: String(error)}; }
                            }""",
                            candidate_url,
                        ) or {}
                        encoded = str(response.get("data") or "") if isinstance(response, dict) else ""
                        if not encoded:
                            last_error = str(response.get("error") or "browser fetch returned no data") if isinstance(response, dict) else "browser fetch returned no data"
                            continue
                        try:
                            video_path.write_bytes(base64.b64decode(encoded, validate=True))
                        except (OSError, ValueError) as exc:
                            last_error = f"cannot save browser media response: {exc}"
                            continue
                        validation_error = self._video_quality_error(video_path)
                        if not validation_error:
                            return True, ""
                        video_path.unlink(missing_ok=True)
                        last_error = validation_error
                    return False, last_error or "browser media fetch failed"
                finally:
                    page.close()
        except Exception as exc:
            return False, f"browser media fetch failed: {type(exc).__name__}: {exc}"

    @staticmethod
    def _is_mp4_file(path: Path) -> bool:
        """Validate the ISO Base Media File Format signature before sync."""
        try:
            with path.open("rb") as handle:
                header = handle.read(12)
        except OSError:
            return False
        # A normal MP4 begins with a box size followed by the ftyp box type.
        return len(header) >= 8 and header[4:8] == b"ftyp"

    @classmethod
    def _video_quality_error(cls, path: Path) -> str:
        """Reject platform placeholder clips before they enter the backend."""
        if not path.is_file() or path.stat().st_size <= 0 or not cls._is_mp4_file(path):
            return "response was not a valid MP4"
        try:
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
        except OSError:
            return "cannot read downloaded MP4"
        if digest in cls._PLACEHOLDER_VIDEO_SHA256:
            return "TikTok returned a known 2-second placeholder clip"
        ffprobe = shutil.which("ffprobe")
        if not ffprobe:
            return "ffprobe is required to verify TikTok video duration"
        try:
            result = subprocess.run(
                [ffprobe, "-v", "error", "-show_entries", "format=duration", "-of", "default=nw=1:nk=1", str(path)],
                capture_output=True,
                text=True,
                timeout=15,
                check=False,
            )
            duration = float(result.stdout.strip()) if result.returncode == 0 else 0.0
        except (OSError, ValueError, subprocess.SubprocessError):
            duration = 0.0
        if duration < cls._MIN_REAL_VIDEO_SECONDS:
            return f"TikTok video duration {duration:.3f}s is below quality threshold"
        return ""

    def _collect_live(self, request: CollectionRequest) -> CollectionBatch:
        """Resolve once with yt-dlp, then bound every HLS read with ffmpeg.

        TikTok's extractor represents a live room as a normal video and its
        default downloader follows the HLS playlist forever.  A bounded ffmpeg
        segment is required so the coordinator can upload a segment and start
        the next polling cycle.
        """
        if not self.enabled:
            raise CollectorError(CollectorErrorCode.ACCESS_DENIED, "TIKTOK adapter is disabled", retryable=False)
        profile = self._browser_profile()
        if not profile:
            raise CollectorError(CollectorErrorCode.AUTH_EXPIRED, "TIKTOK browser profile is not configured", retryable=False)
        try:
            from yt_dlp import YoutubeDL
        except Exception as exc:
            raise CollectorError(CollectorErrorCode.COLLECTOR_BUG, f"yt-dlp is not installed: {exc}", retryable=False) from exc

        target_url = self._target_to_url(request)
        output_dir = (self.output_root / request.job_id).resolve()
        output_dir.mkdir(parents=True, exist_ok=True)
        params = self._ydl_params(request.model_copy(update={"download_media": False}), output_dir, profile)
        try:
            with YoutubeDL(params | {"skip_download": True, "simulate": True}) as ydl:
                info = ydl.extract_info(target_url, download=False)
        except Exception as exc:
            raise self._map_error(exc) from exc
        if isinstance(info, dict) and not info.get("id") and isinstance(info.get("entries"), list):
            info = next((item for item in info["entries"] if isinstance(item, dict) and item.get("id")), info)
        if not isinstance(info, dict) or not info.get("id"):
            if self._streamlink_pilot_enabled(request):
                # Streamlink is the selected live-media resolver for this
                # pilot. Do not fall through to a dead CDP browser when a
                # single metadata probe flakes; the coordinator will retry
                # the source and preserve the live session heartbeat.
                raise CollectorError(
                    CollectorErrorCode.NETWORK_TEMPORARY,
                    "TikTok live metadata temporarily unavailable; retrying Streamlink source",
                    retryable=True,
                )
            return self._collect_live_via_browser(request, target_url, output_dir)

        batch = self._batch_from_info(info, request)
        if not batch.contents:
            raise CollectorError(CollectorErrorCode.ACCESS_DENIED, f"TIKTOK returned no collectable live content for {target_url}", retryable=False)
        # The profile/live page is not a browser-playable media URL.  Expose
        # the resolved HLS manifest separately so the downstream player can
        # initialize hls.js instead of trying to render the TikTok HTML page.
        stream_url = self._select_live_stream_url(info, request)
        download_info = info
        media_engine = "yt-dlp"
        # Realtime HLS relay jobs intentionally pass download_media=False:
        # the persistent relay, rather than this adapter, owns recording.
        # They still need Streamlink to resolve TikTok's playable source;
        # gating this on download_media silently falls back to a yt-dlp HLS
        # URL that commonly stalls after its first two segments.
        if request.capability == Capability.LIVE_RESOLVE and self._streamlink_pilot_enabled(request):
            cached_stream_url = str(request.target.metadata.get("tiktok_live_stream_url") or "").strip()
            try:
                # TikTok's ``best`` Streamlink rendition can be an RTC/FLV
                # variant whose video track is exposed as private data by
                # FFmpeg.  Prefer the H.264 rendition for deterministic
                # remuxing and browser playback; fall back to the configured
                # quality only if the rendition is unavailable.
                configured_quality = str(self.settings.streamlink_quality or "best")
                streamlink_quality = "h264_hd" if configured_quality.lower() == "best" else configured_quality
                stream_url, streamlink_diagnostics = resolve_stream_url(
                    target_url,
                    quality=streamlink_quality,
                )
                media_engine = "streamlink"
                batch.raw["streamlink"] = streamlink_diagnostics
                # _download_live_with_ffmpeg consumes the selected format
                # from the info dict. Replace it with Streamlink's resolved
                # URL so the pilot actually records through Streamlink's
                # resolver instead of silently falling back to yt-dlp's URL.
                download_info = dict(info)
                download_info["formats"] = [
                    {"url": stream_url, "protocol": "httpstream", "height": 0}
                ]
            except StreamlinkUnavailable as exc:
                # The pilot must never make an otherwise working TikTok live
                # unavailable.  Keep the resolved yt-dlp URL and record only a
                # short diagnostic (never cookies or signed URLs).
                batch.raw["streamlinkFallback"] = str(exc)
                # Reuse the last signed URL for a short reconnect window. A
                # transient resolver/CDN failure should not force the browser
                # CDP fallback (which may be unavailable on a headless node).
                if cached_stream_url:
                    stream_url = cached_stream_url
                    download_info = dict(info)
                    download_info["formats"] = [
                        {"url": stream_url, "protocol": "httpstream", "height": 0}
                    ]
                    media_engine = "streamlink_cached"
        if stream_url:
            request.target.metadata["tiktok_live_stream_url"] = stream_url
            batch.contents[0].raw["live_stream_url"] = stream_url
            batch.raw["live_stream_url"] = stream_url
            batch.contents[0].raw["media_engine"] = media_engine
            batch.raw["media_engine"] = media_engine
        # Live monitoring downloads rolling media. Metadata-only resolution
        # must not be overridden by whichever account happens to be open in a
        # shared browser tab.
        if request.download_media or request.target.metadata.get("tiktok_live_browser_verify"):
            self._augment_live_snapshot(batch, request, target_url, info)
        # A canonical /live URL can still resolve metadata for a finished
        # room. Confirm browser state before invoking ffmpeg, otherwise a
        # stale HLS playlist delays the end notification.
        if request.download_media and batch.contents[0].raw.get("is_live") is not False:
            self._download_live_with_ffmpeg(download_info, request, output_dir, batch)
        batch.raw.update(
            {
                "target_url": target_url,
                "output_dir": str(output_dir),
                "item_count": len(batch.contents),
                "is_live": batch.contents[0].raw.get("is_live") is not False,
                "chat_available": bool(batch.comments),
            }
        )
        return batch

    def _streamlink_pilot_enabled(self, request: CollectionRequest) -> bool:
        """Return true only for an explicit, allow-listed pilot target."""
        if not bool(self.settings.streamlink_enabled):
            return False
        platforms = self._csv_values(self.settings.streamlink_platforms)
        if platforms and self.platform.value.lower() not in platforms and "tiktok" not in platforms:
            return False
        targets = self._csv_values(self.settings.streamlink_pilot_targets)
        if not targets:
            return True
        haystack = " ".join(
            str(value or "")
            for value in (
                request.target.target_value,
                request.target.source_url,
                request.target.metadata.get("username"),
                request.target.metadata.get("handle"),
                request.target.metadata.get("uid"),
                request.target.metadata.get("accountId"),
            )
        ).lower()
        return any(token in haystack for token in targets)

    @staticmethod
    def _csv_values(value: Any) -> set[str]:
        return {
            item.strip().lower().lstrip("@")
            for item in str(value or "").split(",")
            if item.strip()
        }

    def _collect_live_via_browser(self, request: CollectionRequest, target_url: str, output_dir: Path) -> CollectionBatch:
        """Use the already logged-in TikTok player when yt-dlp metadata flakes."""
        cdp_url = str(self.settings.tiktok_browser_cdp_url or "").strip()
        if not cdp_url:
            raise CollectorError(CollectorErrorCode.NETWORK_TEMPORARY, "TikTok live metadata unavailable and browser CDP is not configured", retryable=True)
        try:
            from playwright.sync_api import sync_playwright
            with sync_playwright() as playwright:
                browser = playwright.chromium.connect_over_cdp(cdp_url, timeout=30_000)
                try:
                    context = browser.contexts[0] if browser.contexts else None
                    if not context:
                        raise ValueError("TikTok browser has no active context")
                    page = next((item for item in context.pages if "/live" in item.url and "tiktok.com" in item.url), None)
                    if not page:
                        page = next((item for item in context.pages if item.url.startswith("https://www.tiktok.com/")), None)
                        page = page or context.new_page()
                        page.goto(target_url, wait_until="domcontentloaded", timeout=30_000)
                        page.wait_for_timeout(5_000)
                    page.evaluate("""() => {
                        const video = document.querySelector('video');
                        if (video) { video.muted = true; video.play().catch(() => {}); }
                    }""")
                    page.wait_for_timeout(8_000)
                    payload = page.evaluate("""() => {
                        const resources = [...new Set(performance.getEntriesByType('resource').map(item => item.name))];
                        const manifests = resources.filter(url => url.includes('.m3u8') && (url.includes('tiktokcdn') || url.includes('pull-hls')));
                        const selectors = ['[data-e2e*="comment"]', '[data-e2e*="chat"]', '[data-e2e="chat-message"]', '[data-e2e="live-chat-container"]', '[class*="comment"]', '[class*="Comment"]', '[class*="chat"]', '[class*="Chat"]'];
                        const blocks = [];
                        for (const selector of selectors) {
                            for (const node of document.querySelectorAll(selector)) {
                                const text = (node.innerText || node.textContent || '').trim();
                                if (text && text.length < 1000) blocks.push(text);
                            }
                        }
                        const chatMessages = [...document.querySelectorAll('[data-e2e="chat-message"]')].map(node => ({text: (node.innerText || '').trim(), author: node.querySelector('[data-e2e="message-owner-name"]')?.textContent?.trim() || ''})).filter(item => item.text);
                        const bodyText = document.body?.innerText || '';
                        return {streamUrl: manifests.at(-1) || '', title: document.title || '', userAgent: navigator.userAgent || '', bodyText, liveEnded: /live has ended|直播已结束/i.test(bodyText), chatBlocks: [...new Set(blocks)], chatMessages};
                    }""")
                finally:
                    browser.close()
        except Exception as exc:
            raise CollectorError(CollectorErrorCode.NETWORK_TEMPORARY, f"TikTok browser live fallback failed: {exc}", retryable=True) from exc
        if bool(payload.get("liveEnded")):
            raise CollectorError(CollectorErrorCode.ACCESS_DENIED, "TikTok live has ended", retryable=False)
        stream_url = str(payload.get("streamUrl") or "")
        if not stream_url:
            raise CollectorError(CollectorErrorCode.NETWORK_TEMPORARY, "TikTok browser did not expose a live HLS manifest", retryable=True)
        handle_match = re.search(r"tiktok\.com/@([^/?#]+)", target_url, flags=re.IGNORECASE)
        handle = handle_match.group(1) if handle_match else "tiktok-live"
        stream_match = re.search(r"stream-([0-9]+)", stream_url)
        external_id = str(request.target.metadata.get("liveExternalId") or request.target.metadata.get("live_external_id") or (stream_match.group(1) if stream_match else handle))
        account_id = str(request.target.metadata.get("uid") or request.target.metadata.get("externalId") or handle)
        batch = CollectionBatch(
            accounts=[CollectedAccount(platform=Platform.TIKTOK, external_id=account_id, username=f"@{handle}", nickname=handle, profile_url=f"https://www.tiktok.com/@{handle}")],
            contents=[CollectedContent(platform=Platform.TIKTOK, external_id=external_id, url=target_url, content_type="LIVE", title=str(payload.get("title") or f"{handle} TikTok LIVE"), author_external_id=account_id, raw={"is_live": True, "live_status": "is_live", "browser_live_fallback": True})],
            raw={"target_url": target_url, "output_dir": str(output_dir), "is_live": True, "browser_live_fallback": True, "chat_available": False},
        )
        batch.contents[0].raw["live_stream_url"] = stream_url
        batch.raw["live_stream_url"] = stream_url
        self._apply_live_snapshot(batch, payload, request)
        if request.download_media:
            self._download_live_with_ffmpeg(
                {"formats": [{"url": stream_url, "protocol": "m3u8", "height": 360}], "http_headers": {"User-Agent": str(payload.get("userAgent") or "")}},
                request,
                output_dir,
                batch,
            )
        return batch

    def _augment_live_snapshot(self, batch: CollectionBatch, request: CollectionRequest, target_url: str, info: dict[str, Any]) -> None:
        """Best-effort viewer/chat snapshot from the logged-in live page.

        TikTok's live HLS extractor does not expose chat, while the browser
        renders both the audience count and chat entries.  The snapshot is
        deliberately non-fatal: media collection must continue if the page is
        unavailable or TikTok changes its DOM.
        """
        payload: dict[str, Any] = {}
        cdp_url = str(self.settings.tiktok_browser_cdp_url or "").strip()
        handle_match = re.search(r"tiktok\.com/@([^/?#]+)", target_url, flags=re.IGNORECASE)
        handle = (handle_match.group(1).lower() if handle_match else "")
        if cdp_url:
            try:
                from playwright.sync_api import sync_playwright
                with sync_playwright() as playwright:
                    browser = playwright.chromium.connect_over_cdp(cdp_url, timeout=8_000)
                    try:
                        context = browser.contexts[0] if browser.contexts else None
                        page = next((item for item in (context.pages if context else []) if handle and f"@{handle}" in item.url.lower() and "/live" in item.url.lower()), None)
                        if not page and context:
                            page = context.new_page()
                            page.goto(target_url, wait_until="domcontentloaded", timeout=12_000)
                            page.wait_for_timeout(2_500)
                        if page:
                            payload = page.evaluate("""() => {
                                const selectors = ['[data-e2e*="comment"]', '[data-e2e*="chat"]', '[data-e2e="chat-message"]', '[data-e2e="live-chat-container"]', '[class*="comment"]', '[class*="Comment"]', '[class*="chat"]', '[class*="Chat"]'];
                                const blocks = [];
                                for (const selector of selectors) for (const node of document.querySelectorAll(selector)) {
                                    const text = (node.innerText || node.textContent || '').trim();
                                    if (text && text.length < 1000) blocks.push(text);
                                }
                                const chatMessages = [...document.querySelectorAll('[data-e2e="chat-message"]')].map(node => ({text: (node.innerText || '').trim(), author: node.querySelector('[data-e2e="message-owner-name"]')?.textContent?.trim() || ''})).filter(item => item.text);
                                const bodyText = document.body?.innerText || '';
                                return {bodyText, liveEnded: /live has ended|直播已结束/i.test(bodyText), chatBlocks: [...new Set(blocks)], chatMessages};
                            }""") or {}
                    finally:
                        browser.close()
            except Exception:
                payload = {}
        if not payload:
            raw_count = info.get("concurrent_view_count") or info.get("view_count")
            if raw_count is not None:
                batch.contents[0].raw["concurrent_view_count"] = raw_count
            return
        self._apply_live_snapshot(batch, payload, request)

    @staticmethod
    def _apply_live_snapshot(batch: CollectionBatch, payload: dict[str, Any], request: CollectionRequest) -> None:
        if not batch.contents:
            return
        content = batch.contents[0]
        if bool(payload.get("liveEnded")):
            content.raw["is_live"] = False
            content.raw["live_status"] = "ended"
            batch.raw["is_live"] = False
            batch.raw["live_status"] = "ended"
            batch.raw["chat_available"] = False
            return
        body = str(payload.get("bodyText") or "")
        viewer = TikTokAdapter._parse_viewer_count(body)
        if viewer is not None:
            content.raw["concurrent_view_count"] = viewer
            batch.raw["concurrent_view_count"] = viewer
        blocks = payload.get("chatBlocks") if isinstance(payload.get("chatBlocks"), list) else []
        if request.collect_comments:
            comments = TikTokAdapter._live_chat_comments(payload.get("chatMessages"), content.external_id)
            if not comments and blocks:
                comments = comments_from_text_blocks(blocks, platform=Platform.TIKTOK, content_id=content.external_id, limit=50)
            existing = {item.external_id for item in batch.comments}
            batch.comments.extend(item for item in comments if item.external_id not in existing)
        batch.raw["chat_available"] = bool(batch.comments)

    @staticmethod
    def _parse_viewer_count(text: str) -> int | None:
        # English and Chinese labels used by the web player, including 1.2K.
        pattern = r"(?i)([0-9][0-9,]*(?:\.[0-9]+)?\s*[KMB]?)\s*(?:viewers?|观看人数|人观看|观众)"
        match = re.search(pattern, text)
        if not match:
            match = re.search(r"(?i)viewers?\s*[·•:]\s*([0-9][0-9,]*(?:\.[0-9]+)?\s*[KMB]?)", text)
        if not match:
            return None
        value = match.group(1).replace(",", "").replace(" ", "").upper()
        multiplier = 1
        if value.endswith("K"):
            multiplier, value = 1_000, value[:-1]
        elif value.endswith("M"):
            multiplier, value = 1_000_000, value[:-1]
        elif value.endswith("B"):
            multiplier, value = 1_000_000_000, value[:-1]
        try:
            return int(float(value) * multiplier)
        except ValueError:
            return None

    @staticmethod
    def _live_chat_comments(items: Any, content_id: str) -> list[Any]:
        if not isinstance(items, list):
            return []
        comments = []
        for item in items[-50:]:
            if not isinstance(item, dict):
                continue
            text = str(item.get("text") or "").strip()
            author = str(item.get("author") or "").strip()
            lines = [line.strip() for line in text.splitlines() if line.strip()]
            if not text or not author:
                continue
            body = "\n".join(line for line in lines if line != author).strip()
            if not body:
                continue
            external_id = hashlib.sha1(f"tiktok:{content_id}:{author}:{body}".encode()).hexdigest()
            from app.domain.models import CollectedComment
            comments.append(CollectedComment(platform=Platform.TIKTOK, external_id=external_id, content_external_id=content_id, author_external_id=author, text=body, raw={"author": author, "source": "tiktok_live_dom"}))
        return comments

    def _download_live_with_ffmpeg(self, info: dict[str, Any], request: CollectionRequest, output_dir: Path, batch: CollectionBatch) -> None:
        ffmpeg = shutil.which("ffmpeg")
        if not ffmpeg:
            raise CollectorError(CollectorErrorCode.COLLECTOR_BUG, "ffmpeg is required for TikTok live clip recording", retryable=False)
        stream_url = self._select_live_stream_url(info, request)
        if not stream_url:
            raise CollectorError(CollectorErrorCode.ACCESS_DENIED, "TikTok live stream manifest URL was not found", retryable=False)
        seconds = min(max(int(request.target.metadata.get("tiktok_live_clip_seconds") or request.target.metadata.get("liveClipSeconds") or 15), 5), 3600)
        content = batch.contents[0]
        content_dir = output_dir / content.external_id
        content_dir.mkdir(parents=True, exist_ok=True)
        # MPEG-TS stays readable when a live HLS connection is stopped at a
        # wall-clock boundary.  MP4 needs its trailer written, which TikTok's
        # long playlist cadence can postpone indefinitely.
        video_path = content_dir / f"{content.external_id}-live-{seconds}s.ts"
        headers = self._ffmpeg_headers(info.get("http_headers"))
        # TikTok CDN rejects FFmpeg's default user agent for some FLV
        # renditions even though the same signed URL is readable by a normal
        # browser/curl client.  Always send a browser-like UA when the
        # extractor did not provide request headers (Streamlink pilot path).
        if not headers:
            headers = "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/145.0 Safari/537.36\r\nAccept: */*\r\n"
        command = [ffmpeg, "-y", "-hide_banner", "-loglevel", "error"]
        if headers:
            command.extend(["-headers", headers])
        command.extend([
            "-fflags", "+genpts",
            "-i", stream_url,
            # TikTok FLV streams may expose metadata/private-data tracks
            # before the actual H.264 track.  Explicit mapping prevents
            # FFmpeg from selecting an empty data stream and producing a
            # zero-byte clip.
            "-map", "0:v:0",
            "-map", "0:a:0?",
            "-t", str(seconds),
            "-c", "copy",
            "-avoid_negative_ts", "make_zero",
            "-f", "mpegts",
            str(video_path),
        ])
        # Some TikTok game/live rooms publish the first HLS segment slowly;
        # keep the wall-clock clip at 15s but allow up to 60s for startup.
        timeout = seconds + int(request.target.metadata.get("tiktok_live_ffmpeg_timeout_margin_seconds") or 60)
        stderr = ""
        try:
            process = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True, start_new_session=True)
            try:
                _, stderr = process.communicate(timeout=timeout)
            except subprocess.TimeoutExpired:
                # TikTok can hold a playlist connection after the requested
                # media duration.  Stop on our wall-clock segment boundary;
                # TS is already a valid independently-uploadable segment.
                # ffmpeg may be blocked inside an HLS socket and ignore a
                # regular SIGTERM.  Kill the whole process group so the
                # collector can still publish the bounded clip/snapshot.
                try:
                    os.killpg(process.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                try:
                    _, stderr = process.communicate(timeout=10)
                except subprocess.TimeoutExpired:
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    _, stderr = process.communicate()
        except OSError as exc:
            raise CollectorError(CollectorErrorCode.NETWORK_TEMPORARY, f"ffmpeg TikTok live clip could not start: {exc}", retryable=True) from exc
        # Some TikTok CDN edges answer FFmpeg's direct HTTP probe with a 5xx
        # while accepting the same signed URL through curl.  Retry the source
        # through curl, then let FFmpeg remux the captured FLV locally.  This
        # keeps the signed URL and cookies out of the browser-facing payload.
        if (not video_path.is_file() or video_path.stat().st_size <= 0) and shutil.which("curl"):
            raw_path = video_path.with_suffix(".flv")
            curl_timeout = max(15, min(seconds + 10, 90))
            curl_command = [
                shutil.which("curl") or "curl",
                "-L", "--fail", "--silent", "--show-error",
                "--connect-timeout", "10", "--max-time", str(curl_timeout),
                "-A", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/145.0 Safari/537.36",
                "-o", str(raw_path), stream_url,
            ]
            try:
                subprocess.run(curl_command, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, timeout=curl_timeout + 15, check=False)
                if raw_path.is_file() and raw_path.stat().st_size > 0:
                    fallback_command = [
                        ffmpeg, "-y", "-hide_banner", "-loglevel", "error",
                        "-i", str(raw_path),
                        "-map", "0:v:0", "-map", "0:a:0?",
                        "-t", str(seconds), "-c", "copy", "-avoid_negative_ts", "make_zero",
                        "-f", "mpegts", str(video_path),
                    ]
                    subprocess.run(fallback_command, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, timeout=30, check=False)
            except (OSError, subprocess.TimeoutExpired):
                pass
            finally:
                try:
                    raw_path.unlink(missing_ok=True)
                except OSError:
                    pass
        if not video_path.is_file() or video_path.stat().st_size <= 0:
            detail = stderr.strip()[:500]
            raise CollectorError(CollectorErrorCode.NETWORK_TEMPORARY, f"ffmpeg TikTok live clip produced no media: {detail}", retryable=True)
        playable_path = video_path.with_suffix(".mp4")
        remux = [
            ffmpeg,
            "-y",
            "-hide_banner",
            "-loglevel",
            "error",
            "-i",
            str(video_path),
            "-c",
            "copy",
            "-movflags",
            "+faststart",
            str(playable_path),
        ]
        try:
            remux_result = subprocess.run(remux, capture_output=True, text=True, timeout=30)
        except subprocess.TimeoutExpired as exc:
            raise CollectorError(CollectorErrorCode.NETWORK_TEMPORARY, "ffmpeg TikTok live clip remux timed out", retryable=True) from exc
        if remux_result.returncode != 0 or not playable_path.is_file() or playable_path.stat().st_size <= 0:
            detail = remux_result.stderr.strip()[:500]
            raise CollectorError(CollectorErrorCode.NETWORK_TEMPORARY, f"ffmpeg TikTok live clip remux failed: {detail}", retryable=True)
        batch.assets = [asset for asset in batch.assets if not (asset.content_external_id == content.external_id and asset.asset_type in {"VIDEO", "MEDIA"})]
        batch.assets.insert(0, MediaAsset(platform=Platform.TIKTOK, content_external_id=content.external_id, asset_type="VIDEO", local_path=str(playable_path)))
        content.raw.update({"live_clip_seconds": seconds, "live_direct_ffmpeg": True})
        batch.raw.update({"live_clip_seconds": seconds, "live_direct_ffmpeg": True})

    @staticmethod
    def _ffmpeg_headers(value: Any) -> str:
        if not isinstance(value, dict):
            return ""
        lines = []
        for key, item in value.items():
            name = str(key).replace("\r", "").replace("\n", "").strip()
            text = str(item).replace("\r", "").replace("\n", "").strip()
            if name and text:
                lines.append(f"{name}: {text}")
        return "\r\n".join(lines) + ("\r\n" if lines else "")

    @staticmethod
    def _select_live_stream_url(info: dict[str, Any], request: CollectionRequest) -> str:
        formats = [item for item in (info.get("formats") or []) if isinstance(item, dict) and item.get("url")]
        max_height = int(request.target.metadata.get("max_height") or 360)
        candidates = [item for item in formats if (item.get("height") or 0) <= max_height and str(item.get("protocol") or "").startswith(("m3u8", "dash"))]
        if not candidates:
            candidates = [item for item in formats if (item.get("height") or 0) <= max_height]
        if not candidates:
            candidates = formats
        if not candidates:
            return str(info.get("url") or "")
        selected = max(candidates, key=lambda item: ((item.get("height") or 0), 1 if item.get("acodec") != "none" else 0))
        return str(selected.get("url") or "")

    def _batch_from_info(self, info: dict, request: CollectionRequest) -> CollectionBatch:
        batch = super()._batch_from_info(info, request)
        # TikTok's extractor often returns the current Live room as a normal
        # video record without `is_live`.  A successful resolution of the
        # account's canonical `/live` URL is nevertheless the current room;
        # preserve that fact for the live-monitor state machine.
        target = str(request.target.source_url or request.target.target_value or "").lower()
        if request.capability.value == "LIVE_RESOLVE" and "/live" in target:
            for content in batch.contents:
                content.content_type = "LIVE"
                content.raw["is_live"] = True
                content.raw["live_status"] = "is_live"
        return batch
