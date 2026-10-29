from __future__ import annotations

import re
import shutil
import subprocess
import time
from pathlib import Path
from urllib.parse import urlparse

from app.adapters.browser_ytdlp import BrowserYtDlpAdapter
from app.domain.enums import Capability, CollectorErrorCode, CredentialAction, Platform
from app.domain.models import CollectedAccount, CollectedContent, CollectionBatch, CollectionRequest, CollectionTarget, CollectorError, MediaAsset


class FacebookAdapter(BrowserYtDlpAdapter):
    platform = Platform.FACEBOOK
    content_type = "VIDEO"

    @property
    def enabled(self) -> bool:
        return bool(self.settings.facebook_enabled)

    @property
    def browser_name(self) -> str:
        return self.settings.facebook_browser or "chromium"

    @property
    def browser_profile_path(self) -> Path | None:
        return self.settings.facebook_browser_profile

    @property
    def output_root(self) -> Path:
        return self.settings.facebook_output_root

    def _ydl_params(self, request: CollectionRequest, output_dir: Path, profile: Path) -> dict:
        """Make Facebook Live downloads finite before handing them to the job loop.

        Without an explicit range yt-dlp follows Facebook's fragmented DASH
        manifest forever, so no media segment or heartbeat is ever produced.
        """
        params = super()._ydl_params(request, output_dir, profile)
        if request.capability == Capability.LIVE_RESOLVE and request.download_media:
            seconds = self._live_clip_seconds(request)
            # Facebook's portrait live formats are often taller than 720px,
            # so the generic height-only selector silently falls back to a
            # 3.5Mbps 1080p stream.  That takes longer to download than the
            # live window itself on the collector link.  Prefer the smallest
            # video/audio pair to keep each rolling clip real-time; callers
            # can opt into another yt-dlp selector explicitly.
            params["format"] = str(
                request.target.metadata.get("facebook_live_format")
                or "bv*[width<=480]+ba/b[width<=480]/worstvideo+worstaudio/worst"
            )
            try:
                from yt_dlp.utils import download_range_func

                # The same video id is used for every Live polling round.  Do
                # not let yt-dlp regard the preceding clip as a completed
                # download, otherwise the downstream receives the first
                # segment repeatedly instead of a rolling live edge.
                params["overwrites"] = True
                params["continuedl"] = False
                params["download_ranges"] = download_range_func([], [(0, float(seconds))])
                params["force_keyframes_at_cuts"] = True
                params["external_downloader"] = "ffmpeg"
                # Keep the input bound for live manifests and also cap the
                # output.  Facebook sometimes exposes a fragmented MP4 URL
                # that never signals EOF; without the output-side limit the
                # second polling round can block forever after the first clip.
                params["external_downloader_args"] = {
                    "ffmpeg_i": ["-t", str(seconds)],
                    "ffmpeg_o": ["-t", str(seconds)],
                }
            except Exception:
                # The browser fallback remains available when a bundled
                # yt-dlp lacks range-download support.
                pass
        return params

    def _target_to_url(self, request: CollectionRequest) -> str:
        """Normalize account live probes to Facebook's current live page.

        A persisted profile/page URL identifies an account, whereas /watch
        and /videos URLs identify an old broadcast.  Continuous monitoring
        must never pin itself to the latter after a restart.
        """
        if request.capability == Capability.LIVE_RESOLVE:
            source = str(request.target.source_url or request.target.target_value or "").strip()
            parsed = urlparse(source) if source.startswith(("http://", "https://")) else None
            if parsed and "facebook.com" in parsed.netloc.lower():
                path = parsed.path.rstrip("/")
                lowered = path.lower()
                if lowered.endswith("/live") or lowered.endswith("/live_videos"):
                    return source
                if "/watch" not in lowered and "/videos/" not in lowered and "/reel/" not in lowered:
                    if lowered == "/profile.php":
                        match = re.search(r"(?:^|&)id=([^&]+)", parsed.query, flags=re.IGNORECASE)
                        if match:
                            return f"https://www.facebook.com/{match.group(1)}/live"
                    elif path:
                        return f"https://{parsed.netloc}{path}/live"
            for key in ("uid", "username", "handle"):
                value = str(request.target.metadata.get(key) or "").strip().lstrip("@")
                if value:
                    return f"https://www.facebook.com/{value}/live"
        return super()._target_to_url(request)

    def _map_error(self, exc: Exception) -> CollectorError:
        """Classify Facebook's checkpoint wall as a session failure.

        The generic extractor reports a checkpoint URL as an unsupported URL.
        Treating that as a parser change made scheduled account batches retry
        every page individually.  A checkpoint is instead a durable signal
        that the browser login must be refreshed.
        """
        text = str(exc)
        lowered = text.lower()
        if "facebook.com/checkpoint" in lowered or "/checkpoint/" in lowered:
            return CollectorError(
                CollectorErrorCode.AUTH_EXPIRED,
                "FACEBOOK checkpoint detected; browser login requires reauthentication",
                retryable=False,
                credential_action=CredentialAction.REAUTH,
            )
        return super()._map_error(exc)


    def discover(self, request: CollectionRequest, credential=None) -> CollectionBatch:
        # Facebook page live URLs are listing pages and are not accepted by
        # yt-dlp directly. Resolve one live/replay video link in the logged-in
        # browser first, then let the normal extractor handle the media.
        if request.target.target_type == "CONTENT":
            resolved = str(request.target.metadata.get("facebook_resolved_live_url") or "").strip()
            source_url = str(request.target.source_url or request.target.target_value or "").strip()
            if resolved:
                request = request.model_copy(
                    update={
                        "target": request.target.model_copy(
                            update={"source_url": resolved, "target_value": resolved}
                        )
                    }
                )
            elif self._is_live_listing_url(source_url):
                profile = self._browser_profile()
                if profile:
                    links = self.browser_search_collector.collect(
                        url=source_url,
                        platform=self.platform,
                        profile=profile,
                        browser=self.browser_name,
                        limit=5,
                        timeout_seconds=int(request.target.metadata.get("browser_search_timeout_seconds") or 30),
                        cdp_url=self._browser_cdp_url(),
                    )
                    for link in links:
                        candidate = request.model_copy(
                            update={
                                "target": request.target.model_copy(
                                    update={"source_url": link, "target_value": link}
                                )
                            }
                        )
                        try:
                            batch = super().discover(candidate, credential)
                        except CollectorError:
                            continue
                        if batch.contents:
                            request.target.metadata["facebook_resolved_live_url"] = link
                            return batch
        if request.target.target_type == "ACCOUNT" and request.download_media and request.target.metadata.get("facebook_account_browser_links", True):
            batch = self._collect_account_via_browser_links(request)
            if batch.contents:
                return batch
        return super().discover(request, credential)

    def _collect(self, request: CollectionRequest) -> CollectionBatch:
        try:
            batch = super()._collect(request)
        except CollectorError as exc:
            if exc.code not in {CollectorErrorCode.PLATFORM_CHANGED, CollectorErrorCode.ACCESS_DENIED} or request.target.target_type != "CONTENT":
                raise
            batch = self._collect_live_via_browser(request, original_error=exc)
        if request.capability == Capability.LIVE_RESOLVE:
            # yt-dlp's Facebook metadata does not report the duration for a
            # live DASH window.  The bounded capture duration is the durable
            # timing contract for media segments and their playback timeline.
            for content in batch.contents:
                content.raw.setdefault("live_clip_seconds", self._live_clip_seconds(request))
                self._mark_expired_replay(content)
        return batch

    @staticmethod
    def _mark_expired_replay(content: CollectedContent) -> None:
        """Turn a Facebook VOD returned from a stale ``/live`` URL into ENDED.

        Facebook often omits ``is_live`` and ``live_status`` after a broadcast
        ends.  The extractor still exposes the original publication timestamp
        and finite duration, but may briefly retain a stale non-zero viewer
        count. Once timestamp + duration is safely behind the current clock,
        this cannot be the live edge and must not be polled/uploaded as a live
        stream.
        """
        raw = content.raw or {}
        if raw.get("is_live") is True or raw.get("live_status"):
            return
        try:
            timestamp = float(raw.get("timestamp"))
            duration = float(raw.get("duration"))
        except (TypeError, ValueError):
            return
        if duration <= 0:
            return
        if time.time() - (timestamp + duration) <= 120:
            return
        raw["is_live"] = False
        raw["live_status"] = "ended"
        raw["end_detection"] = "facebook_vod_duration_elapsed"

    @staticmethod
    def _live_clip_seconds(request: CollectionRequest) -> int:
        return min(
            max(
                int(
                    request.target.metadata.get("facebook_live_clip_seconds")
                    or request.target.metadata.get("liveClipSeconds")
                    or 15
                ),
                5,
            ),
            3600,
        )

    def _collect_live_via_browser(self, request: CollectionRequest, *, original_error: CollectorError) -> CollectionBatch:
        """Fallback for Facebook's current Comet pages, which yt-dlp cannot parse.

        The logged-in browser has already established the live DASH requests.  We
        reuse its authenticated request context to obtain the latest audio/video
        fragments, rather than attempting to reproduce Facebook's private API.
        """
        cdp_url = self._browser_cdp_url()
        source_url = self._target_to_url(request)
        if not cdp_url:
            raise original_error
        try:
            from playwright.sync_api import sync_playwright
        except Exception:
            raise original_error

        video_id = self._facebook_video_id(source_url)
        output_dir = (self.output_root / request.job_id / video_id).resolve()
        output_dir.mkdir(parents=True, exist_ok=True)
        try:
            with sync_playwright() as playwright:
                browser = playwright.chromium.connect_over_cdp(cdp_url, timeout=30_000)
                try:
                    context = browser.contexts[0] if browser.contexts else browser.new_context(viewport={"width": 1366, "height": 900})
                    page = next((item for item in context.pages if item.url.startswith(source_url)), None)
                    created_page = page is None
                    page = page or context.new_page()
                    if not page.url.startswith(source_url):
                        page.goto(source_url, wait_until="domcontentloaded", timeout=30_000)
                    page.wait_for_timeout(5_000)
                    page.evaluate("""() => {
                        const video = document.querySelector('video');
                        if (video) { video.muted = true; video.play().catch(() => {}); }
                    }""")
                    page.wait_for_timeout(4_000)
                    payload = page.evaluate("""() => {
                        const resources = [...new Set(performance.getEntriesByType('resource').map(item => item.name))];
                        const live = resources.filter(item => item.includes('/live-dash/'));
                        return {
                            title: document.title || '',
                            body: (document.body?.innerText || '').slice(0, 8000),
                            videos: live.filter(item => /\\.m4v(?:\\?|$)/.test(item)).slice(-8),
                            audios: live.filter(item => /\\.m4a(?:\\?|$)/.test(item)).slice(-8),
                        };
                    }""")
                    segment_seconds = max(5, int(request.target.metadata.get("segmentSeconds") or request.target.metadata.get("liveClipSeconds") or 10))
                    fragment_count = min(6, max(1, (segment_seconds + 3) // 4 + 1))
                    video_urls = [str(item) for item in payload.get("videos") or []][-fragment_count:]
                    audio_urls = [str(item) for item in payload.get("audios") or []][-fragment_count:]
                    if not video_urls:
                        raise ValueError("Facebook browser did not expose a live video fragment")
                    body = str(payload.get("body") or "")
                    if not self._is_live_page(body):
                        raise ValueError("Facebook page is not currently live")
                    stamp = str(int(time.time() * 1000))
                    assets = []
                    if request.download_media:
                        video_fragment_path = output_dir / f"{video_id}-live-{stamp}.video.m4v"
                        video_fragment_path.write_bytes(self._live_fragment_file(context, source_url, video_urls, ".m4v"))
                        audio_fragment_path = output_dir / f"{video_id}-live-{stamp}.audio.m4a"
                        if audio_urls:
                            audio_fragment_path.write_bytes(self._live_fragment_file(context, source_url, audio_urls, ".m4a"))
                        video_path = output_dir / f"{video_id}-live-{stamp}.mp4"
                        self._mux_live_fragment(
                            video_fragment_path,
                            audio_fragment_path if audio_urls else None,
                            video_path,
                            segment_seconds,
                        )
                        video_fragment_path.unlink(missing_ok=True)
                        audio_fragment_path.unlink(missing_ok=True)
                        assets.append(MediaAsset(platform=self.platform, content_external_id=video_id, asset_type="VIDEO", local_path=str(video_path)))
                    if created_page:
                        page.close()
                finally:
                    browser.close()
        except Exception as fallback_error:
            raise CollectorError(
                CollectorErrorCode.PLATFORM_CHANGED,
                f"FACEBOOK media parser failed and browser fallback failed: {fallback_error}",
                retryable=True,
            ) from fallback_error

        account_name = self._facebook_account_name(source_url)
        title = self._facebook_live_title(str(payload.get("body") or ""), account_name)
        batch = CollectionBatch(
            accounts=[CollectedAccount(platform=self.platform, external_id=account_name, username=f"@{account_name}", nickname=account_name)],
            contents=[
                CollectedContent(
                    platform=self.platform,
                    external_id=video_id,
                    url=source_url,
                    content_type="LIVE",
                    title=title,
                    author_external_id=account_name,
                    raw={
                        "is_live": True,
                        "live_status": "is_live",
                        "live_clip_seconds": self._live_clip_seconds(request),
                        "browser_fallback": True,
                    },
                )
            ],
            assets=assets,
            raw={"target_url": source_url, "browser_fallback": True, "item_count": 1},
        )
        profile = self._browser_profile()
        if profile:
            self._add_browser_comments(batch, request, source_url, profile)
        return batch

    @staticmethod
    def _facebook_video_id(value: str) -> str:
        match = re.search(r"(?:videos/|[?&]v=)([0-9]+)", value)
        return match.group(1) if match else "facebook-live"

    @staticmethod
    def _facebook_account_name(value: str) -> str:
        parsed = urlparse(value)
        parts = [part for part in parsed.path.split("/") if part]
        return parts[0] if parts else "facebook"

    @staticmethod
    def _is_live_page(body: str) -> bool:
        lowered = body.lower()
        return any(token in body or token in lowered for token in ("正在直播", "直播", "live now", "en vivo", "en directo"))

    @staticmethod
    def _facebook_live_title(body: str, account_name: str) -> str:
        lines = [line.strip() for line in body.splitlines() if line.strip()]
        for index, line in enumerate(lines):
            if "直播" in line or line.lower() in {"live", "en vivo", "en directo"}:
                return lines[index - 1] if index else f"{account_name} live"
        return f"{account_name} live"

    @staticmethod
    def _live_fragment_file(context, source_url: str, urls: list[str], extension: str) -> bytes:
        latest_url = urls[-1]
        path, separator, query = latest_url.partition("?")
        init_path = re.sub(rf"-[0-9]+{re.escape(extension)}$", f"-init{extension}", path)
        init_url = init_path + (f"?{query}" if separator else "")
        init_response = context.request.get(init_url, headers={"Referer": source_url}, timeout=30_000)
        if not init_response.ok:
            raise ValueError(f"Facebook live initialization fragment returned HTTP {init_response.status}")
        chunks = [init_response.body()]
        for url in urls:
            response = context.request.get(url, headers={"Referer": source_url}, timeout=30_000)
            if not response.ok:
                raise ValueError(f"Facebook live media fragment returned HTTP {response.status}")
            chunks.append(response.body())
        return b"".join(chunks)

    @staticmethod
    def _mux_live_fragment(video_path: Path, audio_path: Path | None, output_path: Path, segment_seconds: int) -> None:
        ffmpeg = shutil.which("ffmpeg")
        if not ffmpeg:
            raise ValueError("ffmpeg is required to build a playable Facebook live segment")
        command = [ffmpeg, "-y", "-v", "error", "-i", str(video_path)]
        filter_parts = ["[0:v]setpts=PTS-STARTPTS[v]"]
        if audio_path and audio_path.is_file():
            command.extend(["-i", str(audio_path)])
            filter_parts.append("[1:a]asetpts=PTS-STARTPTS[a]")
        else:
            command.extend(["-f", "lavfi", "-i", "anullsrc=channel_layout=stereo:sample_rate=44100"])
            filter_parts.append("[1:a]asetpts=PTS-STARTPTS[a]")
        command.extend(
            [
                "-filter_complex",
                ";".join(filter_parts),
                "-map",
                "[v]",
                "-map",
                "[a]",
                "-t",
                str(segment_seconds),
                "-c:v",
                "libx264",
                "-preset",
                "veryfast",
                "-crf",
                "28",
                "-c:a",
                "aac",
                "-b:a",
                "96k",
                "-movflags",
                "+faststart",
                str(output_path),
            ]
        )
        try:
            subprocess.run(command, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=max(60, segment_seconds * 6))
        except (OSError, subprocess.SubprocessError) as exc:
            raise ValueError(f"cannot mux Facebook live fragment: {exc}") from exc
        if not output_path.is_file() or output_path.stat().st_size <= 0:
            raise ValueError("ffmpeg produced an empty Facebook live segment")

    @staticmethod
    def _is_live_listing_url(value: str) -> bool:
        parsed = urlparse(value)
        if parsed.netloc and "facebook.com" not in parsed.netloc.lower():
            return False
        path = parsed.path.rstrip("/").lower()
        return path.endswith("/live") or path.endswith("/live_videos") or path.endswith("/live_videos/")

    def _collect_account_via_browser_links(self, request: CollectionRequest) -> CollectionBatch:
        profile = self._browser_profile()
        if not profile:
            raise CollectorError(
                CollectorErrorCode.AUTH_EXPIRED,
                "FACEBOOK browser profile is not configured",
                retryable=False,
            )
        account_url = self._account_url(request.target.target_value)
        # The ``?sk=videos`` view is not consistently rendered for numeric
        # profiles (it often returns only the shell).  The normal profile
        # page contains the same reel/video anchors and is the reliable first
        # attempt; keep the videos view as a fallback for pages that expose it.
        search_urls = [request.target.source_url or account_url]
        videos_url = self._account_videos_url(account_url)
        if videos_url not in search_urls:
            search_urls.append(videos_url)
        candidate_limit = int(request.target.metadata.get("facebook_account_candidate_limit") or max(request.limit * 3, request.limit))
        links: list[str] = []
        search_url = search_urls[0]
        for candidate_url in search_urls:
            search_url = candidate_url
            links = self.browser_search_collector.collect(
                url=candidate_url,
                platform=self.platform,
                profile=profile,
                browser=self.browser_name,
                limit=candidate_limit,
                timeout_seconds=int(request.target.metadata.get("browser_search_timeout_seconds") or request.target.metadata.get("timeout_seconds") or 60),
                cdp_url=self._browser_cdp_url(),
            )
            if links:
                break
        if not links:
            return CollectionBatch(raw={"target_url": search_url, "account_browser_links": [], "item_count": 0})

        merged = CollectionBatch(raw={"target_url": search_url, "account_browser_links": links, "item_count": 0, "skipped": []})
        seen_accounts: set[str] = set()
        seen_contents: set[str] = set()
        seen_comments: set[str] = set()
        seen_assets: set[tuple[str, str, str, str]] = set()
        for link in links:
            if len(merged.contents) >= request.limit:
                break
            content_request = request.model_copy(
                update={
                    "limit": 1,
                    "target": CollectionTarget(
                        platform=request.platform,
                        target_type="CONTENT",
                        target_value=link,
                        source_url=link,
                        interval_seconds=request.target.interval_seconds,
                        capabilities=request.target.capabilities,
                        metadata=request.target.metadata,
                    ),
                }
            )
            try:
                batch = self._collect(content_request)
            except CollectorError as exc:
                merged.raw.setdefault("skipped", []).append({"url": link, "reason": exc.message})
                continue
            for account in batch.accounts:
                if account.external_id in seen_accounts:
                    continue
                seen_accounts.add(account.external_id)
                merged.accounts.append(account)
            for content in batch.contents:
                if content.external_id in seen_contents:
                    continue
                seen_contents.add(content.external_id)
                merged.contents.append(content)
            for comment in batch.comments:
                if comment.external_id in seen_comments:
                    continue
                seen_comments.add(comment.external_id)
                merged.comments.append(comment)
            for asset in batch.assets:
                key = (asset.content_external_id, asset.asset_type, asset.local_path, asset.url)
                if key in seen_assets:
                    continue
                seen_assets.add(key)
                merged.assets.append(asset)
        merged.next_cursor = {"last_external_id": merged.contents[-1].external_id} if merged.contents else {}
        merged.raw["item_count"] = len(merged.contents)
        return merged

    def _account_videos_url(self, account_url: str) -> str:
        separator = "&" if "?" in account_url else "?"
        if "profile.php" in account_url:
            return f"{account_url}{separator}sk=videos"
        return account_url.rstrip("/") + "/videos"

    def _account_url(self, value: str) -> str:
        cleaned = value.lstrip("@").strip("/").strip()
        if cleaned.isdigit():
            return f"https://www.facebook.com/profile.php?id={cleaned}"
        if cleaned.startswith("profile.php"):
            return f"https://www.facebook.com/{cleaned}"
        return f"https://www.facebook.com/{cleaned}"

    def _content_url(self, value: str) -> str:
        cleaned = value.strip("/")
        if cleaned.startswith("watch") or cleaned.startswith("videos/"):
            return f"https://www.facebook.com/{cleaned}"
        return f"https://www.facebook.com/watch/?v={cleaned}"
