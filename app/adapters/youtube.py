from __future__ import annotations

import json
import shutil
import subprocess
import re
import hashlib
import threading
import time
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, quote_plus, urlparse

from app.adapters.base import PlatformAdapter
from app.adapters.browser_search import BrowserSearchCollector
from app.adapters.browser_comments import collect_youtube_browser_comments, youtube_author_profile_url
from app.core.config import get_settings
from app.domain.enums import Capability, CollectorErrorCode, CredentialAction, CredentialType, Platform
from app.domain.models import (
    AdapterHealth,
    CollectedAccount,
    CollectedComment,
    CollectedContent,
    CollectionBatch,
    CollectionRequest,
    CollectorError,
    CredentialLease,
    MediaAsset,
)


class _YtDlpErrorRecorder:
    """Retain suppressed yt-dlp errors when ``ignoreerrors`` is enabled.

    yt-dlp reports per-entry failures through its logger and then returns an
    empty playlist.  Without retaining those messages the collector turned an
    expired YouTube session into the misleading generic result "no
    collectable content", so neither the scheduler nor an operator could
    distinguish reauthentication from an empty channel.
    """

    def __init__(self) -> None:
        self.errors: list[str] = []

    def debug(self, message: str) -> None:
        return None

    def warning(self, message: str) -> None:
        return None

    def error(self, message: str) -> None:
        text = str(message or "").strip()
        if text:
            self.errors.append(text)


class YouTubeAdapter(PlatformAdapter):
    platform = Platform.YOUTUBE
    capabilities = {
        Capability.SEARCH_KEYWORD,
        Capability.ACCOUNT_TIMELINE,
        Capability.CONTENT_DETAIL,
        Capability.COMMENTS,
        Capability.PROFILE,
        Capability.MEDIA_DOWNLOAD,
        Capability.LIVE_RESOLVE,
    }
    # Adapter instances are short-lived in some job paths, so pool state must
    # be shared at process level. It only chooses a credential; platform work
    # remains serialized by the existing account/platform locks.
    _cookie_pool_lock = threading.Lock()
    _cookie_pool_cursor = 0
    _cookie_pool_cooldown_until: dict[str, float] = {}
    _browser_profile_pool_cursor = 0
    _browser_profile_cooldown_until: dict[str, float] = {}

    def __init__(self) -> None:
        self.settings = get_settings()
        self._selected_browser_profile: tuple[str, Path] | None = None

    def healthcheck(self) -> AdapterHealth:
        try:
            import yt_dlp  # noqa: F401

            yt_dlp_available = True
        except Exception:
            yt_dlp_available = False

        cookie_file = self._configured_cookie_file(None)
        cookie_count = self._cookie_pool_count()
        browser_profile = self._configured_browser_profile(None)
        message_parts = []
        if not yt_dlp_available:
            message_parts.append("yt-dlp is not installed")
        if cookie_count:
            message_parts.append(f"cookie_pool={cookie_count}")
        elif cookie_file:
            message_parts.append(f"cookie_file={cookie_file}")
        elif browser_profile:
            browser, profile = browser_profile
            message_parts.append(f"browser_profile={browser}:{profile}")
        else:
            message_parts.append("cookie/profile not configured; public metadata only")
        ffmpeg = shutil.which("ffmpeg") or ""
        if not ffmpeg:
            message_parts.append("ffmpeg not found; media merge may be limited")

        return AdapterHealth(
            platform=self.platform,
            ok=bool(self.settings.youtube_enabled and yt_dlp_available),
            mode="yt-dlp",
            message="; ".join(message_parts),
        )

    def discover(
        self,
        request: CollectionRequest,
        credential: CredentialLease | None = None,
    ) -> CollectionBatch:
        if request.cursor.get("dry_run") or request.target.metadata.get("dry_run"):
            return self._dry_run_batch(request)
        return self._collect(request, credential)

    def collect_comments(
        self,
        request: CollectionRequest,
        credential: CredentialLease | None = None,
    ) -> CollectionBatch:
        return self.discover(request, credential)

    def collect_profile(
        self,
        request: CollectionRequest,
        credential: CredentialLease | None = None,
    ) -> CollectionBatch:
        return self.discover(request, credential)

    def resolve_media(
        self,
        request: CollectionRequest,
        credential: CredentialLease | None = None,
    ) -> CollectionBatch:
        return self.discover(request, credential)

    def _dry_run_batch(self, request: CollectionRequest) -> CollectionBatch:
        target = request.target
        external_id = self._safe_external_id(target.target_value)
        url = target.source_url or self._target_to_url(request)
        account = CollectedAccount(
            platform=Platform.YOUTUBE,
            external_id=f"dryrun-account-{external_id}",
            username=target.target_value if target.target_type == "ACCOUNT" else "",
            nickname="YouTube dry-run account",
            profile_url=url,
            raw={"dry_run": True, "target": target.model_dump(mode="json")},
        )
        content = CollectedContent(
            platform=Platform.YOUTUBE,
            external_id=f"dryrun-video-{external_id}",
            url=url,
            content_type="VIDEO",
            title=f"Dry-run YouTube collection for {target.target_value}",
            author_external_id=account.external_id,
            raw={"dry_run": True, "capability": request.capability},
        )
        comments = []
        if request.collect_comments:
            comments.append(
                CollectedComment(
                    platform=Platform.YOUTUBE,
                    external_id=f"dryrun-comment-{external_id}",
                    content_external_id=content.external_id,
                    author_external_id=account.external_id,
                    text="Dry-run comment placeholder",
                    raw={"dry_run": True},
                )
            )
        return CollectionBatch(accounts=[account], contents=[content], comments=comments)

    def _collect(self, request: CollectionRequest, credential: CredentialLease | None) -> CollectionBatch:
        if not self.settings.youtube_enabled:
            raise CollectorError(
                CollectorErrorCode.ACCESS_DENIED,
                "YouTube adapter is disabled",
                retryable=False,
                credential_action=CredentialAction.NONE,
            )
        try:
            from yt_dlp import YoutubeDL
        except Exception as exc:
            raise CollectorError(
                CollectorErrorCode.COLLECTOR_BUG,
                f"yt-dlp is not installed: {exc}",
                retryable=False,
            ) from exc

        output_dir = (self.settings.youtube_output_root / request.job_id).resolve()
        output_dir.mkdir(parents=True, exist_ok=True)
        # Account discovery must always use the channel's videos feed.  In
        # particular, a priority watcher performs a metadata-only request;
        # routing that request through the generic channel-page extractor
        # returns the channel id as if it were a video id.
        if (
            request.capability == Capability.ACCOUNT_TIMELINE
            and (request.download_media or request.target.metadata.get("priority_metadata_probe", False))
            and request.target.metadata.get("youtube_incremental_account", True)
        ):
            return self._collect_account_incremental(request, credential, output_dir, YoutubeDL)

        params = self._ydl_params(request, credential, output_dir)
        target_url = self._target_to_url(request)

        try:
            if self._should_use_direct_live_ffmpeg(request):
                with YoutubeDL(params | {"skip_download": True, "simulate": True}) as ydl:
                    info = ydl.extract_info(target_url, download=False)
                batch = self._batch_from_info(info or {}, request)
                self._download_live_with_ffmpeg(info or {}, request, output_dir, batch)
            else:
                with YoutubeDL(params) as ydl:
                    info = ydl.extract_info(target_url, download=bool(request.download_media))
                batch = self._batch_from_info(info or {}, request)
        except Exception as exc:
            mapped = self._map_youtube_error(exc)
            self._cool_down_cookie_after_error(params.get("cookiefile"), mapped)
            self._cool_down_browser_profile_after_error(params, mapped)
            raise mapped from exc

        if not batch.contents:
            suppressed_error = self._suppressed_collection_error(target_url, params)
            if suppressed_error is not None:
                self._cool_down_cookie_after_error(params.get("cookiefile"), suppressed_error)
                self._cool_down_browser_profile_after_error(params, suppressed_error)
                raise suppressed_error
            empty_error = CollectorError(
                CollectorErrorCode.ACCESS_DENIED,
                f"YouTube returned no collectable content for {target_url}",
                retryable=False,
                credential_action=CredentialAction.NONE,
            )
            # A channel page may legitimately have no entries. Conversely, a
            # playlist with entries that all become unavailable during detail
            # conversion is evidence that this session is unhealthy. Quarantine
            # it briefly so the next account uses the validated fallback.
            entries = (info or {}).get("entries") if isinstance(info, dict) else None
            if isinstance(entries, list) and entries:
                self._cool_down_cookie_file(params.get("cookiefile"))
            raise empty_error
        if request.capability == Capability.LIVE_RESOLVE:
            # Realtime relay mode resolves metadata without first recording a
            # bounded MP4. Preserve the authorized manifest so the coordinator
            # can start one persistent HLS relay immediately.
            if batch.contents:
                stream_url = self._select_live_stream_url(info or {}, request)
                if stream_url:
                    batch.contents[0].raw["live_stream_url"] = stream_url
                    batch.raw["live_stream_url"] = stream_url
            self._augment_live_browser_snapshot(batch, request, target_url)
        self._add_youtube_browser_comment_fallback(batch, request)
        batch.raw = {
            "target_url": target_url,
            "output_dir": str(output_dir),
            "download_media": request.download_media,
            "item_count": len(batch.contents),
        }
        if batch.contents and batch.contents[0].raw.get("live_stream_url"):
            batch.raw["live_stream_url"] = batch.contents[0].raw["live_stream_url"]
        return batch

    def _add_youtube_browser_comment_fallback(self, batch: CollectionBatch, request: CollectionRequest) -> None:
        """Use the browser only for opted-in requests with empty yt-dlp comments.

        Large account sweeps never set the opt-in flag.  That keeps a missing
        yt-dlp comment response from becoming hundreds of browser launches,
        while giving analyst-selected account repairs a deterministic fallback.
        """
        metadata = request.target.metadata or {}
        if not (
            request.collect_comments
            and bool(metadata.get("youtube_browser_comment_fallback", False))
            and bool(self.settings.youtube_browser_comment_fallback_enabled)
            and request.capability != Capability.LIVE_RESOLVE
        ):
            return
        profile_config = self._configured_browser_profile(None)
        if not profile_config:
            for content in batch.contents:
                if not any(comment.content_external_id == content.external_id for comment in batch.comments):
                    content.raw["commentCollection"] = {"status": "FALLBACK_UNAVAILABLE", "source": "youtube_browser_dom", "count": 0}
            return
        browser, profile = profile_config
        limit = max(1, min(
            int(metadata.get("browser_comment_limit") or self.settings.browser_comment_limit),
            int(metadata.get("max_comments") or self.settings.youtube_default_comments),
        ))
        max_videos = max(1, int(self.settings.youtube_browser_comment_fallback_max_videos or 1))
        timeout_seconds = max(10, int(metadata.get("browser_comment_timeout_seconds") or self.settings.youtube_browser_comment_fallback_timeout_seconds))
        existing = {comment.external_id for comment in batch.comments}
        attempted = 0
        for content in batch.contents:
            content_comments = [comment for comment in batch.comments if comment.content_external_id == content.external_id]
            if content_comments:
                content.raw["commentCollection"] = {"status": "COLLECTED", "source": "yt_dlp", "count": len(content_comments)}
                continue
            if attempted >= max_videos:
                content.raw["commentCollection"] = {"status": "FALLBACK_LIMIT_REACHED", "source": "youtube_browser_dom", "count": 0}
                continue
            attempted += 1
            comments, status = collect_youtube_browser_comments(
                url=content.url or f"https://www.youtube.com/watch?v={content.external_id}",
                content_id=content.external_id,
                profile=profile,
                browser=browser,
                limit=limit,
                timeout_seconds=timeout_seconds,
            )
            for comment in comments:
                if comment.external_id not in existing:
                    existing.add(comment.external_id)
                    batch.comments.append(comment)
            content.raw["commentCollection"] = {"status": status, "source": "youtube_browser_dom", "count": len(comments)}

    def _augment_live_browser_snapshot(self, batch: CollectionBatch, request: CollectionRequest, target_url: str) -> None:
        """Fill gaps left by yt-dlp's intermittent Live metadata/chat export.

        The normal extractor remains the primary source.  The logged-in
        YouTube browser is a best-effort secondary source for the live watch
        page's concurrent-viewer text and its ``live_chat`` iframe.  A DOM
        failure must never stop media capture or a heartbeat.
        """
        if not batch.contents:
            return
        cdp_url = str(self.settings.youtube_browser_cdp_url or "").strip()
        if not cdp_url:
            return
        content = batch.contents[0]
        try:
            from playwright.sync_api import sync_playwright
            with sync_playwright() as playwright:
                browser = playwright.chromium.connect_over_cdp(cdp_url, timeout=8_000)
                try:
                    context = browser.contexts[0] if browser.contexts else None
                    if not context:
                        return
                    video_id = content.external_id
                    page = next((item for item in context.pages if video_id and f"v={video_id}" in item.url), None)
                    if not page:
                        page = context.new_page()
                        page.goto(target_url, wait_until="domcontentloaded", timeout=15_000)
                        page.wait_for_timeout(3_000)
                    snapshot = page.evaluate("""() => {
                        const selectors = [
                          'yt-formatted-string#view-count',
                          '#info #count',
                          '#count.ytd-video-view-count-renderer',
                          'ytd-video-view-count-renderer #count',
                        ];
                        const viewerTexts = selectors.flatMap(selector => [...document.querySelectorAll(selector)].map(node => (node.innerText || node.textContent || '').trim())).filter(Boolean);
                        return {viewerTexts, bodyText: document.body?.innerText || ''};
                    }""") or {}
                    chat_items: list[dict[str, str]] = []
                    for frame in page.frames:
                        if "/live_chat" not in frame.url:
                            continue
                        try:
                            items = frame.evaluate("""() => [...document.querySelectorAll('yt-live-chat-text-message-renderer, yt-live-chat-paid-message-renderer, yt-live-chat-membership-item-renderer')].slice(-100).map(node => ({
                                id: node.getAttribute('id') || node.getAttribute('data-message-id') || '',
                                author: (node.querySelector('#author-name')?.textContent || '').trim(),
                                text: (node.querySelector('#message')?.textContent || node.querySelector('#header-subtext')?.textContent || '').trim(),
                            })).filter(item => item.author && item.text)""") or []
                            chat_items.extend(item for item in items if isinstance(item, dict))
                        except Exception:
                            continue
                finally:
                    browser.close()
        except Exception:
            return
        viewer = self._parse_youtube_viewer_count(snapshot.get("viewerTexts"), snapshot.get("bodyText"))
        if viewer is not None:
            content.raw["concurrent_view_count"] = viewer
            content.raw["viewer_source"] = "youtube_live_browser"
        if request.collect_comments:
            known = {comment.external_id for comment in batch.comments}
            for item in chat_items:
                author = str(item.get("author") or "").strip()
                text = str(item.get("text") or "").strip()
                if not author or not text:
                    continue
                message_id = str(item.get("id") or "").strip()
                if not message_id:
                    message_id = hashlib.sha1(f"youtube-live:{content.external_id}:{author}:{text}".encode()).hexdigest()
                if message_id in known:
                    continue
                known.add(message_id)
                batch.comments.append(
                    CollectedComment(
                        platform=Platform.YOUTUBE,
                        external_id=message_id,
                        content_external_id=content.external_id,
                        author_external_id=author,
                        text=text,
                        raw={"author": author, "source": "youtube_live_browser"},
                    )
                )

    @staticmethod
    def _parse_youtube_viewer_count(viewer_texts: Any, body_text: Any) -> int | None:
        values = viewer_texts if isinstance(viewer_texts, list) else []
        values.append(str(body_text or ""))
        patterns = (
            r"(?i)([0-9][0-9,]*(?:\.[0-9]+)?\s*[KMB]?)\s+(?:watching|watching now)",
            r"([0-9][0-9,]*(?:\.[0-9]+)?\s*[万亿KMB]?)\s*人正在观看",
            r"([0-9][0-9,]*(?:\.[0-9]+)?\s*[万亿KMB]?)\s*位观众",
        )
        for value in values:
            for pattern in patterns:
                match = re.search(pattern, str(value))
                if not match:
                    continue
                raw = match.group(1).replace(",", "").replace(" ", "").upper()
                factor = 1
                for suffix, multiplier in (("K", 1_000), ("M", 1_000_000), ("B", 1_000_000_000), ("万", 10_000), ("亿", 100_000_000)):
                    if raw.endswith(suffix):
                        raw, factor = raw[:-len(suffix)], multiplier
                        break
                try:
                    return int(float(raw) * factor)
                except ValueError:
                    continue
        return None


    def _collect_account_incremental(self, request: CollectionRequest, credential: CredentialLease | None, output_dir: Path, ydl_cls) -> CollectionBatch:
        target_url = self._target_to_url(request)
        if request.target.metadata.get("youtube_prefer_cached_account", True):
            cached = self._cached_account_batch(request, output_dir)
            if cached and cached.contents:
                cached.raw.update({"target_url": target_url, "output_dir": str(output_dir), "incremental": True, "account_cache_fast_path": True})
                # Cached media is still a valid target for a bounded comment
                # repair. Do not force an expensive YouTube detail download
                # merely to learn whether comments are available.
                self._add_youtube_browser_comment_fallback(cached, request)
                return cached
        list_params = self._ydl_params(request.model_copy(update={"download_media": False, "collect_comments": False}), credential, output_dir)
        list_params["extract_flat"] = "in_playlist"
        list_params["skip_download"] = True
        list_params["simulate"] = True
        candidate_limit = max(int(request.target.metadata.get("youtube_playlist_end") or (request.limit * 2)), request.limit)
        list_params["playlistend"] = candidate_limit
        try:
            with ydl_cls(list_params) as ydl:
                info = ydl.extract_info(target_url, download=False)
        except Exception as exc:
            mapped = self._map_youtube_error(exc)
            fallback = self._collect_account_from_browser(request, target_url, mapped)
            if fallback is not None:
                return fallback
            raise mapped from exc

        candidate_batch = self._batch_from_info(
            info or {},
            request.model_copy(update={"download_media": False, "collect_comments": False, "limit": candidate_limit}),
        )
        if not candidate_batch.contents:
            suppressed_error = self._suppressed_collection_error(target_url, list_params)
            if suppressed_error is not None:
                self._cool_down_cookie_after_error(list_params.get("cookiefile"), suppressed_error)
                self._cool_down_browser_profile_after_error(list_params, suppressed_error)
                fallback = self._collect_account_from_browser(request, target_url, suppressed_error)
                if fallback is not None:
                    return fallback
                raise suppressed_error
            raise CollectorError(
                CollectorErrorCode.ACCESS_DENIED,
                f"YouTube returned no collectable content for {target_url}",
                retryable=False,
                credential_action=CredentialAction.NONE,
            )

        # The priority watcher needs only an authoritative newest video id at
        # this point.  Do not enter the media/cache/detail path for a probe:
        # it would turn each five-minute check into a download and could mask
        # a newly published item with an older cached result.
        if not request.download_media:
            candidate_batch.raw = {
                "target_url": target_url,
                "output_dir": str(output_dir),
                "download_media": False,
                "item_count": len(candidate_batch.contents),
                "incremental": True,
                "metadata_only": True,
            }
            candidate_batch.next_cursor = {"last_external_id": candidate_batch.contents[-1].external_id}
            return candidate_batch

        merged = CollectionBatch(raw={"target_url": target_url, "output_dir": str(output_dir), "incremental": True, "skipped": []})
        seen_accounts: set[str] = set()
        seen_contents: set[str] = set()
        seen_assets: set[tuple[str, str, str, str]] = set()
        detail_circuit_error: CollectorError | None = None
        recency_cutoff = self._account_recency_cutoff(request)
        for account in candidate_batch.accounts:
            if account.external_id not in seen_accounts:
                seen_accounts.add(account.external_id)
                merged.accounts.append(account)

        for candidate in candidate_batch.contents:
            if len(merged.contents) >= request.limit:
                break
            # Flat playlist discovery already includes a timestamp for normal
            # video cards.  Reject stale cards before opening the expensive
            # video-detail/download request.  The job-level filter remains as
            # a safety net for platforms/cards without a publish timestamp.
            if recency_cutoff and candidate.published_at and candidate.published_at < recency_cutoff:
                merged.raw.setdefault("skipped", []).append({
                    "videoId": candidate.external_id,
                    "reason": "older than account recency cutoff; media download skipped",
                })
                continue
            cached = self._cached_content_batch(candidate, request, output_dir) if request.target.metadata.get("youtube_reuse_local_assets", True) else None
            if cached and cached.assets:
                self._merge_batch(merged, cached, seen_accounts, seen_contents, seen_assets)
                continue

            if detail_circuit_error is not None:
                candidate.raw["media_error"] = self._media_failure_reason(detail_circuit_error)
                candidate.raw["media_failure_code"] = detail_circuit_error.code.value
                candidate.raw["media_retry_eligible"] = False
                if candidate.external_id not in seen_contents:
                    seen_contents.add(candidate.external_id)
                    merged.contents.append(candidate)
                merged.raw.setdefault("skipped", []).append({
                    "videoId": candidate.external_id,
                    "reason": candidate.raw["media_error"],
                    "failureCode": detail_circuit_error.code.value,
                    "retryEligible": False,
                })
                continue

            if not request.target.metadata.get("youtube_try_uncached_details", False):
                merged.raw.setdefault("skipped", []).append({
                    "videoId": candidate.external_id,
                    "reason": "local media cache missing; skipped uncached detail download to avoid YouTube bot challenge",
                })
                continue
            detail_request = request.model_copy(
                update={
                    "capability": Capability.CONTENT_DETAIL,
                    "limit": 1,
                    "target": request.target.model_copy(
                        update={
                            "target_type": "CONTENT",
                            "target_value": candidate.external_id,
                            "source_url": candidate.url or f"https://www.youtube.com/watch?v={candidate.external_id}",
                        }
                    ),
                }
            )
            try:
                detail_batch = self._collect(detail_request, credential)
            except CollectorError as exc:
                # Preserve discoverable content when only its media download
                # fails. The backend then receives metadata plus a precise
                # UNAVAILABLE reason instead of a false "no data" outcome.
                candidate.raw["media_error"] = self._media_failure_reason(exc)
                candidate.raw["media_failure_code"] = exc.code.value
                candidate.raw["media_retry_eligible"] = exc.code in {
                    CollectorErrorCode.RATE_LIMITED,
                    CollectorErrorCode.PLATFORM_CHANGED,
                    CollectorErrorCode.NETWORK_TEMPORARY,
                }
                if candidate.external_id not in seen_contents:
                    seen_contents.add(candidate.external_id)
                    merged.contents.append(candidate)
                merged.raw.setdefault("skipped", []).append({
                    "videoId": candidate.external_id,
                    "reason": candidate.raw["media_error"],
                    "failureCode": exc.code.value,
                    "retryEligible": candidate.raw["media_retry_eligible"],
                })
                if exc.code in {CollectorErrorCode.AUTH_EXPIRED, CollectorErrorCode.RATE_LIMITED}:
                    # A bot/auth response is platform-wide for the current
                    # session. Keep this account's already-discovered
                    # metadata, but do not issue two more detail requests nor
                    # continue hammering the remaining accounts in the batch.
                    detail_circuit_error = exc
                    merged.raw["account_platform_circuit"] = {
                        "platform": Platform.YOUTUBE.value,
                        "code": exc.code.value,
                        "reason": self._media_failure_reason(exc),
                    }
                self._pace_uncached_download(request)
                continue
            self._pace_uncached_download(request)
            self._merge_batch(merged, detail_batch, seen_accounts, seen_contents, seen_assets)

        if not merged.contents:
            candidate_batch.raw = {
                "target_url": target_url,
                "output_dir": str(output_dir),
                "download_media": request.download_media,
                "item_count": len(candidate_batch.contents),
                "incremental": True,
                "skipped": merged.raw.get("skipped", []),
                "media_error": "candidate metadata collected, but every media detail/download attempt failed",
            }
            self._add_youtube_browser_comment_fallback(candidate_batch, request)
            return candidate_batch
        merged.next_cursor = {"last_external_id": merged.contents[-1].external_id}
        self._add_youtube_browser_comment_fallback(merged, request)
        return merged

    def _collect_account_from_browser(
        self,
        request: CollectionRequest,
        target_url: str,
        original_error: CollectorError,
    ) -> CollectionBatch | None:
        """Fallback to the already authenticated browser for account links.

        This path is deliberately metadata-only. It is used only when
        yt-dlp's account API is challenged; media is marked unavailable so
        downstream still receives the content rather than a false "no data".
        """
        if not self.settings.youtube_browser_fallback_enabled:
            return None
        cdp_url = str(self.settings.youtube_browser_cdp_url or "").strip()
        profile = self.settings.youtube_browser_profile
        if not cdp_url or not profile or not Path(profile).exists():
            return None
        result = BrowserSearchCollector().collect_with_context(
            url=target_url,
            platform=Platform.YOUTUBE,
            profile=Path(profile),
            browser=self.settings.youtube_browser,
            limit=request.limit,
            timeout_seconds=int(request.target.metadata.get("youtube_browser_fallback_timeout_seconds") or 45),
            cdp_url=cdp_url,
        )
        if not result.links:
            return None
        account_value = str(request.target.target_value or "").strip()
        canonical_match = re.search(r"youtube\.com/channel/(UC[\w-]{20,})", result.canonical_url or "", flags=re.IGNORECASE)
        browser_channel_id = str(result.platform_account_id or "").strip()
        channel_id = (
            browser_channel_id
            if re.fullmatch(r"UC[\w-]{20,}", browser_channel_id, flags=re.IGNORECASE)
            else canonical_match.group(1)
            if canonical_match
            else f"youtube-browser-{self._safe_external_id(account_value)}"
        )
        account = CollectedAccount(
            platform=Platform.YOUTUBE,
            external_id=channel_id,
            username=account_value,
            nickname=result.title.replace(" - YouTube", "").strip() or account_value.lstrip("@"),
            profile_url=target_url,
            raw={"source": "browser_cdp", "page_url": result.page_url},
        )
        contents: list[CollectedContent] = []
        failure_reason = self._media_failure_reason(original_error)
        for link in result.links[: request.limit]:
            video_id = str(parse_qs(urlparse(link).query).get("v", [""])[0]).strip()
            if not video_id:
                continue
            contents.append(
                CollectedContent(
                    platform=Platform.YOUTUBE,
                    external_id=video_id,
                    url=link,
                    content_type="VIDEO",
                    # The backend validates title as non-blank. A browser
                    # fallback has no reliable per-card title yet, so retain
                    # a truthful, stable placeholder rather than dropping
                    # the entire metadata record.
                    title=f"YouTube video {video_id}",
                    description="Collected from the authenticated YouTube browser; media retrieval is pending.",
                    author_external_id=account.external_id,
                    raw={
                        "source": "browser_cdp_fallback",
                        "media_error": failure_reason,
                        "media_failure_code": original_error.code.value,
                        "media_retry_eligible": False,
                    },
                )
            )
        if not contents:
            return None
        return CollectionBatch(
            accounts=[account],
            contents=contents,
            next_cursor={"last_external_id": contents[-1].external_id},
            raw={
                "target_url": target_url,
                "browser_fallback": True,
                "account_platform_circuit": {
                    "platform": Platform.YOUTUBE.value,
                    "code": original_error.code.value,
                    "reason": failure_reason,
                },
            },
        )

    def _should_use_direct_live_ffmpeg(self, request: CollectionRequest) -> bool:
        return bool(
            request.download_media
            and request.capability == Capability.LIVE_RESOLVE
            and request.target.metadata.get("youtube_live_direct_ffmpeg", True)
        )

    def _download_live_with_ffmpeg(self, info: dict[str, Any], request: CollectionRequest, output_dir: Path, batch: CollectionBatch) -> None:
        if not batch.contents:
            return
        content = batch.contents[0]
        raw = content.raw or {}
        is_live = content.content_type == "LIVE" or raw.get("is_live") or str(raw.get("live_status") or "").lower() == "is_live"
        if not is_live:
            return
        seconds = int(request.target.metadata.get("youtube_live_clip_seconds") or self.settings.youtube_live_clip_seconds or 60)
        seconds = min(max(seconds, 5), 3600)
        stream_url = self._select_live_stream_url(info, request)
        if not stream_url:
            raise CollectorError(CollectorErrorCode.ACCESS_DENIED, "YouTube live stream URL was not found", retryable=False)
        content_dir = output_dir / content.external_id
        content_dir.mkdir(parents=True, exist_ok=True)
        video_path = content_dir / f"{content.external_id}-live-{seconds}s.mp4"
        cmd = [
            "ffmpeg",
            "-y",
            "-hide_banner",
            "-loglevel",
            "error",
            "-i",
            stream_url,
            "-t",
            str(seconds),
            "-c",
            "copy",
            "-movflags",
            "+faststart",
            str(video_path),
        ]
        timeout = seconds + int(request.target.metadata.get("youtube_live_ffmpeg_timeout_margin_seconds") or 60)
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        if result.returncode != 0:
            raise CollectorError(
                CollectorErrorCode.NETWORK_TEMPORARY,
                f"ffmpeg live clip failed: {result.stderr.strip()[:500]}",
                retryable=True,
            )
        if not video_path.is_file() or video_path.stat().st_size <= 0:
            raise CollectorError(CollectorErrorCode.NETWORK_TEMPORARY, "ffmpeg live clip produced no media", retryable=True)
        batch.assets = [asset for asset in batch.assets if not (asset.content_external_id == content.external_id and asset.asset_type in {"VIDEO", "MEDIA"})]
        batch.assets.insert(0, MediaAsset(platform=Platform.YOUTUBE, content_external_id=content.external_id, asset_type="VIDEO", local_path=str(video_path)))
        content.raw["live_stream_url"] = stream_url
        batch.raw["live_stream_url"] = stream_url
        content.raw.setdefault("live_clip_seconds", seconds)
        content.raw.setdefault("live_direct_ffmpeg", True)
        batch.raw.setdefault("live_direct_ffmpeg", True)
        batch.raw.setdefault("live_clip_seconds", seconds)

    def _select_live_stream_url(self, info: dict[str, Any], request: CollectionRequest) -> str:
        formats = [item for item in (info.get("formats") or []) if isinstance(item, dict) and item.get("url")]
        max_height = int(request.target.metadata.get("max_height") or 360)
        candidates = [item for item in formats if (item.get("height") or 0) <= max_height and str(item.get("protocol") or "").startswith("m3u8")]
        if not candidates:
            candidates = [item for item in formats if (item.get("height") or 0) <= max_height]
        if not candidates:
            candidates = formats
        if not candidates:
            return str(info.get("url") or "")
        selected = max(candidates, key=lambda item: ((item.get("height") or 0), 1 if item.get("acodec") != "none" else 0))
        return str(selected.get("url") or "")


    def _cached_account_batch(self, request: CollectionRequest, output_dir: Path) -> CollectionBatch | None:
        candidates = self._cached_account_candidates(request)
        if not candidates:
            return None
        merged = CollectionBatch(raw={"account_cache_fast_path": True, "skipped": []})
        seen_accounts: set[str] = set()
        seen_contents: set[str] = set()
        seen_assets: set[tuple[str, str, str, str]] = set()
        for candidate in candidates:
            if len(merged.contents) >= request.limit:
                break
            cached = self._cached_content_batch(candidate, request, output_dir)
            if cached and cached.assets:
                self._merge_batch(merged, cached, seen_accounts, seen_contents, seen_assets)
            else:
                merged.raw.setdefault("skipped", []).append({"videoId": candidate.external_id, "reason": "historical account item has no local media cache"})
        if not merged.contents:
            return None
        merged.next_cursor = {"last_external_id": merged.contents[-1].external_id}
        return merged

    def _cached_account_candidates(self, request: CollectionRequest) -> list[CollectedContent]:
        aliases = self._account_aliases(request)
        if not aliases:
            return []
        runs_dir = Path("data/pipeline-runs")
        if not runs_dir.is_dir():
            return []
        candidates: list[CollectedContent] = []
        seen: set[str] = set()
        summaries = sorted(runs_dir.glob("account-*/summary.json"), key=lambda path: path.stat().st_mtime, reverse=True)
        for summary_path in summaries:
            try:
                payload = json.loads(summary_path.read_text(encoding="utf-8"))
            except Exception:
                continue
            for item in payload.get("items") or []:
                if not isinstance(item, dict) or str(item.get("platform") or "").upper() != Platform.YOUTUBE.value:
                    continue
                item_account = str(item.get("account") or "").strip()
                if item_account and item_account.lower() not in aliases:
                    continue
                backend_items = ((item.get("backend_sync") or {}).get("items") or []) if isinstance(item.get("backend_sync"), dict) else []
                for backend_item in backend_items:
                    if not isinstance(backend_item, dict):
                        continue
                    video_id = str(backend_item.get("sourceVideoId") or backend_item.get("videoId") or "").strip()
                    if not video_id or video_id in seen:
                        continue
                    seen.add(video_id)
                    candidates.append(
                        CollectedContent(
                            platform=Platform.YOUTUBE,
                            external_id=video_id,
                            url=f"https://www.youtube.com/watch?v={video_id}",
                            content_type="VIDEO",
                            author_external_id=request.target.target_value,
                            raw={"source": "historical_account_summary", "summaryPath": str(summary_path)},
                        )
                    )
                    if len(candidates) >= max(request.limit * 3, request.limit):
                        return candidates
        return candidates

    def _account_aliases(self, request: CollectionRequest) -> set[str]:
        values = {request.target.target_value, request.target.source_url}
        metadata = request.target.metadata or {}
        for key in ("account_handle", "account_display_name", "accountDisplayName", "displayName"):
            value = metadata.get(key)
            if value:
                values.add(str(value))
        aliases: set[str] = set()
        for value in values:
            text = str(value or "").strip()
            if not text:
                continue
            aliases.add(text.lower())
            normalized = self._normalize_target_value(text)
            if normalized:
                aliases.add(normalized.lower())
            handle = self._handle_from_text(text)
            if handle:
                aliases.add(handle.lower())
        return aliases

    def _merge_batch(self, target: CollectionBatch, source: CollectionBatch, seen_accounts: set[str], seen_contents: set[str], seen_assets: set[tuple[str, str, str, str]]) -> None:
        for account in source.accounts:
            if account.external_id in seen_accounts:
                continue
            seen_accounts.add(account.external_id)
            target.accounts.append(account)
        for content in source.contents:
            if content.external_id in seen_contents:
                continue
            seen_contents.add(content.external_id)
            target.contents.append(content)
        target.comments.extend(source.comments)
        for asset in source.assets:
            key = (asset.content_external_id, asset.asset_type, asset.local_path, asset.url)
            if key in seen_assets:
                continue
            seen_assets.add(key)
            target.assets.append(asset)

    def _cached_content_batch(self, candidate: CollectedContent, request: CollectionRequest, output_dir: Path) -> CollectionBatch | None:
        video_id = candidate.external_id
        cached_video = self._cached_video_path(video_id)
        cached_thumbnail = self._cached_thumbnail_path(video_id) or self._youtube_static_thumbnail(video_id)
        if not cached_video or not cached_thumbnail:
            return None
        content_dir = output_dir / video_id
        content_dir.mkdir(parents=True, exist_ok=True)
        video = content_dir / f"{video_id}{cached_video.suffix or '.mp4'}"
        thumbnail = content_dir / f"{video_id}-thumbnail{cached_thumbnail.suffix or '.jpg'}"
        if not video.exists() or video.stat().st_size != cached_video.stat().st_size:
            shutil.copy2(cached_video, video)
        if not thumbnail.exists() or thumbnail.stat().st_size != cached_thumbnail.stat().st_size:
            shutil.copy2(cached_thumbnail, thumbnail)
        account = CollectedAccount(
            platform=Platform.YOUTUBE,
            external_id=candidate.author_external_id or request.target.target_value,
            username=request.target.target_value if request.target.target_type == "ACCOUNT" else "",
            nickname="",
            profile_url=self._target_to_url(request),
        )
        content = candidate.model_copy(update={"author_external_id": account.external_id})
        return CollectionBatch(
            accounts=[account],
            contents=[content],
            assets=[
                MediaAsset(platform=Platform.YOUTUBE, content_external_id=video_id, asset_type="VIDEO", local_path=str(video)),
                MediaAsset(platform=Platform.YOUTUBE, content_external_id=video_id, asset_type="THUMBNAIL", local_path=str(thumbnail)),
            ],
            raw={"cached_assets": True},
        )

    def _cached_video_path(self, video_id: str) -> Path | None:
        candidates: list[Path] = []
        for base in self.settings.youtube_output_root.glob("*"):
            content_dir = base / video_id
            candidates.extend(
                [
                    content_dir / f"{video_id}-collector-compatible.mp4",
                    content_dir / f"{video_id}.mp4",
                    content_dir / f"{video_id}.webm",
                    content_dir / f"{video_id}.mkv",
                ]
            )
        existing = [path for path in candidates if path.is_file() and path.stat().st_size > 0]
        return max(existing, key=lambda path: path.stat().st_mtime) if existing else None

    def _cached_thumbnail_path(self, video_id: str) -> Path | None:
        existing = [
            path
            for base in self.settings.youtube_output_root.glob("*")
            for path in (base / video_id).glob(f"{video_id}-thumbnail.*")
            if path.is_file() and path.stat().st_size > 0
        ]
        return max(existing, key=lambda path: path.stat().st_mtime) if existing else None

    def _youtube_static_thumbnail(self, video_id: str) -> Path | None:
        return None

    def _ydl_params(
        self,
        request: CollectionRequest,
        credential: CredentialLease | None,
        output_dir: Path,
    ) -> dict[str, Any]:
        playlist_end = int(request.target.metadata.get("youtube_playlist_end") or request.limit)
        collect_live_chat = bool(request.collect_comments and request.capability == Capability.LIVE_RESOLVE)
        params: dict[str, Any] = {
            "quiet": True,
            "no_warnings": True,
            "ignoreerrors": True,
            "skip_download": not request.download_media,
            "playlistend": max(1, playlist_end),
            "noplaylist": request.capability == Capability.CONTENT_DETAIL,
            "getcomments": bool(request.collect_comments and not collect_live_chat),
            "outtmpl": {"default": str(output_dir / "%(id)s" / "%(id)s.%(ext)s")},
            "paths": {"home": str(output_dir)},
            "socket_timeout": min(int(request.target.metadata.get("socket_timeout", 30)), 120),
            "retries": int(request.target.metadata.get("retries", 2)),
            "noprogress": bool(request.target.metadata.get("youtube_no_progress", True)),
            "logger": _YtDlpErrorRecorder(),
        }
        if collect_live_chat:
            params.update(
                {
                    "writesubtitles": True,
                    "writeautomaticsub": True,
                    "subtitleslangs": ["live_chat"],
                    "subtitlesformat": "json",
                    "skip_unavailable_fragments": True,
                    "fragment_retries": int(request.target.metadata.get("live_chat_fragment_retries", 2)),
                }
            )
        self._add_youtube_runtime_params(params, request)
        if request.download_media:
            max_height = int(request.target.metadata.get("max_height") or 360)
            if request.capability == Capability.LIVE_RESOLVE:
                self._add_live_download_range(params, request)
            if request.target.metadata.get("youtube_prefer_backend_compatible", True):
                params["format"] = (
                    f"bv*[vcodec^=avc1][height<={max_height}]+ba[acodec^=mp4a]/"
                    f"b[vcodec^=avc1][height<={max_height}]/"
                    f"bv*[height<={max_height}]+ba/"
                    f"b[height<={max_height}]/"
                    "bv*+ba/b"
                )
            else:
                params["format"] = (
                    f"bv*[height<={max_height}]+ba/"
                    f"b[height<={max_height}]/"
                    "bv*+ba/b"
                )
            params["merge_output_format"] = str(request.target.metadata.get("merge_output_format") or "mp4")
        extractor_args: dict[str, dict[str, list[str]]] = {}
        max_comments = int(request.target.metadata.get("max_comments") or self.settings.youtube_default_comments)
        if request.collect_comments and max_comments > 0:
            extractor_args.setdefault("youtube", {})["max_comments"] = [str(max_comments)]
        if request.capability == Capability.ACCOUNT_TIMELINE and request.target.metadata.get("youtube_skip_authcheck", True):
            extractor_args.setdefault("youtubetab", {})["skip"] = ["authcheck"]
        if self.settings.youtube_po_token_enabled:
            # yt-dlp's recommended mweb path requests PO Tokens through the
            # local provider. The provider is bound to loopback only.
            extractor_args.setdefault("youtube", {})["player_client"] = ["mweb"]
            provider_url = str(self.settings.youtube_po_token_provider_url or "").strip()
            if provider_url:
                extractor_args.setdefault("youtubepot-bgutilhttp", {})["base_url"] = [provider_url]
        if extractor_args:
            params["extractor_args"] = extractor_args

        cookie_file = self._configured_session_cookie(credential)
        # An explicitly leased browser profile is an operator-selected
        # credential and must win over the shared fallback pool.
        leased_browser_profile = self._configured_browser_profile(credential) if credential else None
        configured_browser_profile = self._configured_browser_profile(None)
        prefer_browser_cookies = bool(
            not credential
            and self.settings.youtube_prefer_browser_cookies
            and configured_browser_profile
            and not request.target.metadata.get("youtube_skip_browser_cookies", False)
        )
        if prefer_browser_cookies:
            # Exported Netscape files are snapshots.  Once YouTube challenges
            # a snapshot, keep using the administrator's refreshed Chromium
            # profile instead of repeatedly burning that stale cookie.
            browser, profile = configured_browser_profile
            params["cookiesfrombrowser"] = (browser, str(profile), None, None)
        elif not cookie_file and not leased_browser_profile:
            # Prefer exported cookie files to a live Chromium profile. This
            # keeps normal collection from retaining multiple browsers in RAM.
            cookie_file = self._select_cookie_from_pool()
        if prefer_browser_cookies:
            pass
        elif cookie_file:
            params["cookiefile"] = str(cookie_file)
        elif not request.target.metadata.get("youtube_skip_browser_cookies", False):
            browser_profile = leased_browser_profile or self._configured_browser_profile(None)
            if browser_profile:
                browser, profile = browser_profile
                params["cookiesfrombrowser"] = (browser, str(profile), None, None)
            else:
                cookie_file = self._configured_cookie_file(None)
                if cookie_file:
                    params["cookiefile"] = str(cookie_file)
        else:
            cookie_file = self._configured_cookie_file(None)
            if cookie_file:
                params["cookiefile"] = str(cookie_file)
        proxy = str(request.target.metadata.get("proxy") or self.settings.youtube_proxy or "").strip()
        if proxy:
            params["proxy"] = proxy
        if not request.download_media:
            params["simulate"] = True
        return params

    def _add_live_download_range(self, params: dict[str, Any], request: CollectionRequest) -> None:
        configured_seconds = request.target.metadata.get("youtube_live_clip_seconds")
        if configured_seconds is None:
            configured_seconds = self.settings.youtube_live_clip_seconds
        seconds = int(configured_seconds or 0)
        if seconds <= 0:
            return
        seconds = min(max(seconds, 5), 3600)
        try:
            from yt_dlp.utils import download_range_func
        except Exception:
            return
        params["download_ranges"] = download_range_func([], [(0, float(seconds))])
        params["force_keyframes_at_cuts"] = bool(
            request.target.metadata.get("youtube_force_keyframes_at_cuts", True)
        )
        if request.target.metadata.get("youtube_live_use_ffmpeg_timeout", True):
            params["external_downloader"] = "ffmpeg"
            params["external_downloader_args"] = {
                "ffmpeg_i": ["-t", str(seconds)],
            }

    def _configured_session_cookie(self, credential: CredentialLease | None) -> Path | None:
        if credential and credential.credential.credential_type == CredentialType.SESSION_COOKIE:
            candidate = Path(credential.credential.secret_ref)
            return candidate if candidate.is_file() else None
        return None

    def _add_youtube_runtime_params(self, params: dict[str, Any], request: CollectionRequest) -> None:
        disable_plugins = request.target.metadata.get("youtube_disable_plugins", self.settings.youtube_disable_plugins)
        if self.settings.youtube_po_token_enabled:
            disable_plugins = False
        if disable_plugins:
            params["plugin_dirs"] = []

        js_runtime = str(request.target.metadata.get("youtube_js_runtime") or self.settings.youtube_js_runtime or "").strip()
        if js_runtime and Path(js_runtime).exists():
            params["js_runtimes"] = {"deno": {"path": js_runtime}}

        remote_components = str(
            request.target.metadata.get("youtube_remote_components") or self.settings.youtube_remote_components or ""
        ).strip()
        if remote_components:
            params["remote_components"] = [item.strip() for item in remote_components.split(",") if item.strip()]

    def _configured_cookie_file(self, credential: CredentialLease | None) -> Path | None:
        pool = self._configured_cookie_pool(credential)
        return pool[0] if pool else None

    def _configured_cookie_pool(self, credential: CredentialLease | None) -> list[Path]:
        explicit = self._configured_session_cookie(credential)
        if explicit:
            return [explicit]
        candidates: list[Path] = []
        if self.settings.youtube_cookie_file:
            candidates.append(Path(self.settings.youtube_cookie_file))
        for pattern in ("*.txt", "*.cookies", "*.cookie", "*.netscape"):
            candidates.extend(sorted(self.settings.youtube_cookie_dir.glob(pattern)))
        unique: dict[str, Path] = {}
        for candidate in candidates:
            try:
                resolved = candidate.resolve()
                if resolved.is_file() and resolved.stat().st_size > 0:
                    unique[str(resolved)] = resolved
            except OSError:
                continue
        # Keep the explicitly configured file first: it is the designated
        # primary session, while files discovered in the directory are its
        # fallbacks.
        return list(unique.values())

    def _select_cookie_from_pool(self) -> Path | None:
        pool = self._configured_cookie_pool(None)
        if not pool:
            return None
        with self._cookie_pool_lock:
            now = time.monotonic()
            available = [
                cookie for cookie in pool
                if self._cookie_pool_cooldown_until.get(str(cookie), 0) <= now
            ]
            # If all sessions are cooling down, retain a deterministic
            # fallback rather than silently switching to an unauthenticated
            # request. The next task can still succeed after cooldown expiry.
            candidates = available or pool
            index = self._cookie_pool_cursor % len(candidates)
            self._cookie_pool_cursor += 1
            return candidates[index]

    def _cool_down_cookie_after_error(self, cookie_file: Any, error: CollectorError) -> None:
        if not cookie_file or error.code not in {
            CollectorErrorCode.RATE_LIMITED,
            CollectorErrorCode.AUTH_EXPIRED,
        }:
            return
        self._cool_down_cookie_file(cookie_file)

    def _cool_down_cookie_file(self, cookie_file: Any) -> None:
        """Temporarily quarantine a session without deleting its credentials."""
        if not cookie_file:
            return
        cooldown = max(60, int(self.settings.youtube_cookie_pool_cooldown_seconds or 0))
        try:
            key = str(Path(str(cookie_file)).resolve())
        except OSError:
            return
        with self._cookie_pool_lock:
            self._cookie_pool_cooldown_until[key] = time.monotonic() + cooldown

    def _cool_down_browser_profile_after_error(self, params: dict[str, Any], error: CollectorError) -> None:
        if error.code not in {CollectorErrorCode.RATE_LIMITED, CollectorErrorCode.AUTH_EXPIRED}:
            return
        browser_cookie = params.get("cookiesfrombrowser")
        if not isinstance(browser_cookie, (tuple, list)) or len(browser_cookie) < 2:
            return
        try:
            key = str(Path(str(browser_cookie[1])).resolve())
        except OSError:
            return
        cooldown = max(60, int(self.settings.youtube_cookie_pool_cooldown_seconds or 0))
        with self._cookie_pool_lock:
            self._browser_profile_cooldown_until[key] = time.monotonic() + cooldown

    def _configured_browser_profile(self, credential: CredentialLease | None) -> tuple[str, Path] | None:
        if credential and credential.credential.credential_type == CredentialType.BROWSER_PROFILE:
            profile = Path(credential.credential.secret_ref)
            browser = str(credential.credential.metadata.get("browser") or self.settings.youtube_browser or "chrome")
            return (browser, profile) if profile.exists() else None
        if self._selected_browser_profile:
            return self._selected_browser_profile
        profiles = self._configured_browser_profiles()
        if not profiles:
            return None
        with self._cookie_pool_lock:
            now = time.monotonic()
            available = [item for item in profiles if self._browser_profile_cooldown_until.get(str(item[1]), 0) <= now]
            candidates = available or profiles
            selected = candidates[self._browser_profile_pool_cursor % len(candidates)]
            self.__class__._browser_profile_pool_cursor += 1
        self._selected_browser_profile = selected
        return selected

    def _configured_browser_profiles(self) -> list[tuple[str, Path]]:
        candidates: list[Path] = []
        candidates.extend(Path(item.strip()) for item in str(self.settings.youtube_browser_profiles or "").split(",") if item.strip())
        if self.settings.youtube_browser_profile:
            candidates.append(Path(self.settings.youtube_browser_profile))
        unique: dict[str, tuple[str, Path]] = {}
        for profile in candidates:
            if not self._is_explicit_browser_profile(profile):
                continue
            try:
                resolved = profile.resolve()
            except OSError:
                continue
            unique[str(resolved)] = (self.settings.youtube_browser or "chrome", resolved)
        return list(unique.values())

    def _is_explicit_browser_profile(self, profile: Path) -> bool:
        if str(profile).strip() in {"", "."}:
            return False
        return profile.exists()

    def _cookie_pool_count(self) -> int:
        return len(self._configured_cookie_pool(None))

    def _target_to_url(self, request: CollectionRequest) -> str:
        target = request.target
        value = self._normalize_target_value(target.target_value)
        # A persisted account task may still carry the video ID from the
        # previous broadcast. For live monitoring, resolve the channel's
        # canonical /live entrypoint whenever an account handle is available;
        # YouTube then redirects to the current broadcast instead of making
        # the collector retry a stale ``watch?v=...`` URL forever.
        if request.capability == Capability.LIVE_RESOLVE:
            handle_value = str(
                target.metadata.get("handle")
                or target.metadata.get("username")
                or target.metadata.get("account_handle")
                or ""
            ).strip()
            handle_match = re.search(r"@([A-Za-z0-9._-]{3,})", handle_value)
            if handle_match:
                return f"https://www.youtube.com/@{handle_match.group(1)}/live"
        if target.source_url and not self._source_url_conflicts_with_target(target.source_url, value):
            source_url = target.source_url.rstrip("/")
            # A profile root (for example ``/@handle``) is a channel page,
            # whose top-level metadata has the channel id.  Account timeline
            # collection must ask yt-dlp for the videos tab so entries are
            # actual video ids.  Keep an explicit tab/link untouched.
            if request.capability == Capability.ACCOUNT_TIMELINE and re.fullmatch(
                r"https?://(?:www\.)?youtube\.com/(?:@[A-Za-z0-9._-]+|channel/UC[A-Za-z0-9_-]{20,}|c/[^/?#]+|user/[^/?#]+)",
                source_url,
                flags=re.IGNORECASE,
            ):
                return f"{source_url}/videos"
            return source_url
        if target.target_type == "KEYWORD":
            search_prefix = "ytsearchdate" if target.metadata.get("youtube_keyword_sort") == "date" else "ytsearch"
            return f"{search_prefix}{max(1, request.limit)}:{value}"
        if value.startswith("http://") or value.startswith("https://"):
            return value
        if value.startswith("@"):
            return f"https://www.youtube.com/{value}/videos"
        if value.startswith("UC") and len(value) >= 20:
            return f"https://www.youtube.com/channel/{value}/videos"
        if target.target_type == "CONTENT":
            return f"https://www.youtube.com/watch?v={value}"
        return f"https://www.youtube.com/{value}/videos"

    def _normalize_target_value(self, value: str) -> str:
        stripped = value.strip()
        url_match = re.search(r"https?://(?:www\.)?youtube\.com/[^\s]+", stripped, flags=re.IGNORECASE)
        if url_match:
            return url_match.group(0).rstrip(".,;)")
        channel_match = re.search(r"\b(UC[A-Za-z0-9_-]{20,})\b", stripped)
        if channel_match:
            return channel_match.group(1)
        handle_match = re.search(r"(?<![\w.])@([A-Za-z0-9._-]{3,})\b", stripped)
        if handle_match:
            return f"@{handle_match.group(1)}"
        return stripped

    def _source_url_conflicts_with_target(self, source_url: str, target_value: str) -> bool:
        if not target_value.startswith("@"):
            return False
        source_handle = self._handle_from_text(source_url)
        return bool(source_handle and source_handle.lower() != target_value.lower())

    def _handle_from_text(self, value: str) -> str:
        handle_match = re.search(r"(?<![\w.])@([A-Za-z0-9._-]{3,})\b", value)
        if handle_match:
            return f"@{handle_match.group(1)}"
        return ""

    def _batch_from_info(self, info: dict[str, Any], request: CollectionRequest) -> CollectionBatch:
        entries = info.get("entries")
        if isinstance(entries, list):
            raw_items = [item for item in entries if isinstance(item, dict)]
        else:
            raw_items = [info]

        accounts_by_id: dict[str, CollectedAccount] = {}
        contents: list[CollectedContent] = []
        comments: list[CollectedComment] = []
        assets: list[MediaAsset] = []

        for item in raw_items[: request.limit]:
            video_id = str(item.get("id") or item.get("display_id") or "").strip()
            if not video_id:
                continue
            account = self._account_from_item(item)
            if account and account.external_id not in accounts_by_id:
                accounts_by_id[account.external_id] = account
            contents.append(self._content_from_item(item, account.external_id if account else ""))
            comments.extend(self._comments_from_item(item, video_id))
            comments.extend(self._live_chat_comments_from_files(request, video_id))
            assets.extend(self._assets_from_item(item, video_id))

        return CollectionBatch(
            accounts=list(accounts_by_id.values()),
            contents=contents,
            comments=comments,
            assets=assets,
            next_cursor={"last_external_id": contents[-1].external_id} if contents else {},
        )

    def _account_from_item(self, item: dict[str, Any]) -> CollectedAccount | None:
        external_id = str(item.get("channel_id") or item.get("uploader_id") or item.get("channel") or "").strip()
        if not external_id:
            return None
        return CollectedAccount(
            platform=Platform.YOUTUBE,
            external_id=external_id,
            username=str(item.get("uploader_id") or ""),
            nickname=str(item.get("channel") or item.get("uploader") or ""),
            profile_url=str(item.get("channel_url") or item.get("uploader_url") or ""),
            raw=self._small_raw(item, ("channel_id", "uploader_id", "channel", "uploader", "channel_url")),
        )

    def _content_from_item(self, item: dict[str, Any], author_external_id: str) -> CollectedContent:
        webpage_url = str(item.get("webpage_url") or item.get("original_url") or "")
        if not webpage_url and item.get("id"):
            webpage_url = f"https://www.youtube.com/watch?v={item['id']}"
        is_live = bool(item.get("is_live")) or str(item.get("live_status") or "").lower() == "is_live"
        return CollectedContent(
            platform=Platform.YOUTUBE,
            external_id=str(item.get("id") or item.get("display_id")),
            url=webpage_url,
            content_type="LIVE" if is_live else "SHORT_VIDEO" if "/shorts/" in webpage_url else "VIDEO",
            title=str(item.get("title") or ""),
            description=str(item.get("description") or ""),
            author_external_id=author_external_id,
            published_at=self._timestamp_to_datetime(item.get("timestamp")),
            raw=self._small_raw(
                item,
                (
                    "id",
                    "title",
                    "description",
                    "duration",
                    "view_count",
                    "like_count",
                    "comment_count",
                    "webpage_url",
                    "thumbnail",
                    "timestamp",
                    "upload_date",
                    "is_live",
                    "live_status",
                    "concurrent_view_count",
                    "was_live",
                    "release_timestamp",
                ),
            ),
        )

    def _comments_from_item(self, item: dict[str, Any], video_id: str) -> list[CollectedComment]:
        raw_comments = item.get("comments")
        if not isinstance(raw_comments, list):
            return []
        comments: list[CollectedComment] = []
        for index, raw in enumerate(raw_comments):
            if not isinstance(raw, dict):
                continue
            text = str(raw.get("text") or "").strip()
            if not text:
                continue
            comment_id = str(raw.get("id") or raw.get("comment_id") or f"{video_id}-{index}")
            comments.append(
                CollectedComment(
                    platform=Platform.YOUTUBE,
                    external_id=comment_id,
                    content_external_id=video_id,
                    # `author` is display-only; only author_id is a stable
                    # channel identity suitable for follow-up collection.
                    author_external_id=str(raw.get("author_id") or ""),
                    author_profile_url=youtube_author_profile_url(
                        str(raw.get("author_id") or ""), str(raw.get("author_url") or "")
                    ),
                    text=text,
                    published_at=self._timestamp_to_datetime(raw.get("timestamp")),
                    raw=self._small_raw(
                        raw,
                        (
                            "id",
                            "comment_id",
                            "author",
                            "author_id",
                            "author_url",
                            "text",
                            "timestamp",
                            "like_count",
                            "parent",
                        ),
                    ),
                )
            )
        return comments


    def _live_chat_comments_from_files(self, request: CollectionRequest, video_id: str) -> list[CollectedComment]:
        if not (request.collect_comments and request.capability == Capability.LIVE_RESOLVE):
            return []
        output_dir = (self.settings.youtube_output_root / request.job_id).resolve()
        content_dir = output_dir / video_id
        candidates = []
        if content_dir.is_dir():
            candidates.extend(content_dir.glob(f"{video_id}.live_chat*.json*"))
            candidates.extend(content_dir.glob("*.live_chat*.json*"))
        candidates.extend(output_dir.glob(f"**/{video_id}.live_chat*.json*"))
        candidates.extend(output_dir.glob("**/*.live_chat*.json*"))
        seen_paths: set[Path] = set()
        comments: list[CollectedComment] = []
        max_comments = int(request.target.metadata.get("max_comments") or self.settings.youtube_default_comments or 100)
        for path in candidates:
            if path in seen_paths or not path.is_file() or path.stat().st_size <= 0:
                continue
            seen_paths.add(path)
            for raw in self._iter_live_chat_records(path):
                comment = self._live_chat_comment_from_record(raw, video_id, len(comments))
                if comment:
                    comments.append(comment)
                    if len(comments) >= max_comments:
                        return comments
        return comments

    def _iter_live_chat_records(self, path: Path):
        try:
            text = path.read_text(encoding="utf-8", errors="ignore").strip()
        except Exception:
            return
        if not text:
            return
        if text.startswith("["):
            try:
                payload = json.loads(text)
            except Exception:
                payload = None
            if isinstance(payload, list):
                for item in payload:
                    if isinstance(item, dict):
                        yield item
                return
        for line in text.splitlines():
            line = line.strip().rstrip(",")
            if not line:
                continue
            try:
                payload = json.loads(line)
            except Exception:
                continue
            if isinstance(payload, dict):
                yield payload

    def _live_chat_comment_from_record(self, raw: dict[str, Any], video_id: str, index: int) -> CollectedComment | None:
        action, renderer = self._live_chat_renderer(raw)
        if not renderer:
            return None
        text = self._youtube_runs_text(renderer.get("message")) or self._youtube_runs_text(renderer.get("headerSubtext"))
        if not text:
            text = str(renderer.get("text") or raw.get("text") or "").strip()
        if not text:
            return None
        author = self._youtube_runs_text(renderer.get("authorName")) or str(renderer.get("authorName") or raw.get("author") or "").strip()
        author_id = str(renderer.get("authorExternalChannelId") or raw.get("author_id") or "").strip()
        timestamp_usec = renderer.get("timestampUsec")
        published_at = None
        try:
            if timestamp_usec:
                value = float(timestamp_usec)
                if value > 10_000_000_000:
                    value = value / 1_000_000
                published_at = self._timestamp_to_datetime(value)
        except Exception:
            published_at = None
        comment_id = str(renderer.get("id") or raw.get("id") or f"{video_id}-live-chat-{index}")
        raw_small = {
            "source": "youtube_live_chat",
            "author": author,
            "author_id": author_id,
            "timestampUsec": renderer.get("timestampUsec"),
            "videoOffsetTimeMsec": action.get("videoOffsetTimeMsec") if action else None,
        }
        return CollectedComment(
            platform=Platform.YOUTUBE,
            external_id=comment_id,
            content_external_id=video_id,
            author_external_id=author_id,
            author_profile_url=(f"https://www.youtube.com/channel/{author_id}" if author_id.startswith("UC") else ""),
            text=text,
            published_at=published_at,
            raw=raw_small,
        )

    def _live_chat_renderer(self, raw: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
        containers: list[dict[str, Any]] = []
        top_action = self._first_dict(raw, "replayChatItemAction", "addChatItemAction", "addLiveChatTickerItemAction")
        if top_action:
            containers.append(top_action)
            nested_actions = top_action.get("actions")
            if isinstance(nested_actions, list):
                containers.extend(item for item in nested_actions if isinstance(item, dict))
        containers.append(raw)
        for container in containers:
            action = self._first_dict(container, "addChatItemAction", "addLiveChatTickerItemAction", "replayChatItemAction") or container
            item = self._first_dict(action, "item") or self._first_dict(container, "item") or action
            renderer = self._first_dict(
                item,
                "liveChatTextMessageRenderer",
                "liveChatPaidMessageRenderer",
                "liveChatMembershipItemRenderer",
                "liveChatViewerEngagementMessageRenderer",
            )
            if renderer:
                if "videoOffsetTimeMsec" not in action and top_action and top_action.get("videoOffsetTimeMsec") is not None:
                    action = dict(action)
                    action["videoOffsetTimeMsec"] = top_action.get("videoOffsetTimeMsec")
                return action, renderer
        return {}, {}

    def _first_dict(self, source: dict[str, Any] | None, *keys: str) -> dict[str, Any]:
        if not isinstance(source, dict):
            return {}
        for key in keys:
            value = source.get(key)
            if isinstance(value, dict):
                return value
        return {}

    def _youtube_runs_text(self, value: Any) -> str:
        if isinstance(value, str):
            return value.strip()
        if not isinstance(value, dict):
            return ""
        simple = value.get("simpleText")
        if isinstance(simple, str):
            return simple.strip()
        runs = value.get("runs")
        if not isinstance(runs, list):
            return ""
        parts = []
        for run in runs:
            if isinstance(run, dict):
                text = run.get("text")
                if isinstance(text, str):
                    parts.append(text)
        return "".join(parts).strip()

    def _assets_from_item(self, item: dict[str, Any], video_id: str) -> list[MediaAsset]:
        assets: list[MediaAsset] = []
        requested_downloads = item.get("requested_downloads")
        if isinstance(requested_downloads, list):
            for download in requested_downloads:
                if not isinstance(download, dict):
                    continue
                filepath = str(download.get("filepath") or "")
                if filepath:
                    assets.append(
                        MediaAsset(
                            platform=Platform.YOUTUBE,
                            content_external_id=video_id,
                            asset_type="VIDEO",
                            local_path=filepath,
                            raw=self._small_raw(download, ("format_id", "ext", "width", "height", "filepath")),
                        )
                    )
        thumbnail = str(item.get("thumbnail") or "")
        if thumbnail:
            assets.append(
                MediaAsset(
                    platform=Platform.YOUTUBE,
                    content_external_id=video_id,
                    asset_type="THUMBNAIL",
                    url=thumbnail,
                )
            )
        return assets

    def _map_youtube_error(self, exc: Exception) -> CollectorError:
        text = str(exc)
        lowered = text.lower()
        if "429" in lowered or "rate" in lowered:
            return CollectorError(
                CollectorErrorCode.RATE_LIMITED,
                text,
                retryable=True,
                credential_action=CredentialAction.COOLDOWN,
            )
        if "login" in lowered or "sign in" in lowered or "cookie" in lowered:
            return CollectorError(
                CollectorErrorCode.AUTH_EXPIRED,
                text,
                retryable=False,
                credential_action=CredentialAction.REAUTH,
            )
        if "private" in lowered or "unavailable" in lowered or "permission" in lowered:
            return CollectorError(CollectorErrorCode.ACCESS_DENIED, text, retryable=False)
        if "timed out" in lowered or "network" in lowered or "connection" in lowered:
            return CollectorError(CollectorErrorCode.NETWORK_TEMPORARY, text, retryable=True)
        return CollectorError(CollectorErrorCode.PLATFORM_CHANGED, text, retryable=False)

    def _suppressed_collection_error(self, target_url: str, params: dict[str, Any]) -> CollectorError | None:
        """Recover a meaningful error from yt-dlp's ``ignoreerrors`` path."""
        logger = params.get("logger")
        errors = getattr(logger, "errors", [])
        if not isinstance(errors, list) or not errors:
            return None
        message = " | ".join(str(item).strip() for item in errors[-3:] if str(item).strip())
        if not message:
            return None
        mapped = self._map_youtube_error(RuntimeError(message))
        # Preserve the target because this error becomes the persisted job
        # reason and the operator needs to know which refresh it came from.
        return CollectorError(
            mapped.code,
            f"YouTube collection failed for {target_url}: {mapped.message}"[:1000],
            retryable=mapped.retryable,
            credential_action=mapped.credential_action,
        )

    @staticmethod
    def _media_failure_reason(error: CollectorError) -> str:
        """Stable, actionable media reason retained with metadata-only sync."""
        text = str(error.message or "").strip()
        lowered = text.lower()
        if "age" in lowered or "confirm your age" in lowered:
            category = "AGE_RESTRICTED"
        elif "sign in" in lowered or "login" in lowered or "cookie" in lowered:
            category = "AUTH_REQUIRED"
        elif "private" in lowered:
            category = "PRIVATE"
        elif "geo" in lowered or "not available in your country" in lowered:
            category = "GEO_BLOCKED"
        elif error.code == CollectorErrorCode.RATE_LIMITED:
            category = "RATE_LIMITED"
        elif "deleted" in lowered or "removed" in lowered or "does not exist" in lowered:
            category = "DELETED"
        elif error.code == CollectorErrorCode.NETWORK_TEMPORARY:
            category = "NETWORK_TEMPORARY"
        elif error.code == CollectorErrorCode.PLATFORM_CHANGED:
            category = "PARSER_FAILED"
        else:
            category = "UNAVAILABLE"
        return f"{category}: {text}"[:1000]

    def _pace_uncached_download(self, request: CollectionRequest) -> None:
        """Pace only account-media detail attempts; live and metadata paths stay fast."""
        if request.capability != Capability.ACCOUNT_TIMELINE or not request.download_media:
            return
        seconds = float(request.target.metadata.get("youtube_download_pacing_seconds") or self.settings.youtube_download_pacing_seconds or 0)
        if seconds > 0:
            time.sleep(min(seconds, 30.0))

    def _account_recency_cutoff(self, request: CollectionRequest):
        """Return an aware UTC cutoff for pre-download account filtering."""
        if request.capability != Capability.ACCOUNT_TIMELINE:
            return None
        raw_days = request.target.metadata.get("account_recency_days")
        if raw_days is None:
            raw_days = request.target.metadata.get("accountRecencyDays")
        if raw_days is None:
            return None
        try:
            days = int(raw_days)
        except (TypeError, ValueError):
            return None
        if days <= 0:
            return None
        from datetime import datetime, timedelta, timezone

        raw_now = request.target.metadata.get("account_recency_now") or request.target.metadata.get("recency_now")
        if raw_now:
            try:
                now = datetime.fromisoformat(str(raw_now).replace("Z", "+00:00"))
                if now.tzinfo is None:
                    now = now.replace(tzinfo=timezone.utc)
                else:
                    now = now.astimezone(timezone.utc)
            except ValueError:
                now = datetime.now(timezone.utc)
        else:
            now = datetime.now(timezone.utc)
        return now - timedelta(days=days)

    def _timestamp_to_datetime(self, value: Any):
        if value is None:
            return None
        try:
            from datetime import datetime, timezone

            return datetime.fromtimestamp(float(value), timezone.utc)
        except Exception:
            return None

    def _safe_external_id(self, value: str) -> str:
        return "".join(ch if ch.isalnum() else "-" for ch in value.lower()).strip("-")[:80] or "target"

    def _small_raw(self, source: dict[str, Any], keys: tuple[str, ...]) -> dict[str, Any]:
        return {key: source.get(key) for key in keys if key in source}
