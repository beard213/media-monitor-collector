from __future__ import annotations

from pathlib import Path
import shutil
import subprocess
from typing import Any

from app.adapters.base import PlatformAdapter
from app.adapters.browser_comments import BrowserCommentCollector
from app.adapters.browser_search import BrowserSearchCollector, search_url_for_keyword
from app.core.config import get_settings
from app.domain.enums import Capability, CollectorErrorCode, CredentialAction, Platform
from app.domain.models import (
    AdapterHealth,
    CollectedAccount,
    CollectedContent,
    CollectionBatch,
    CollectionRequest,
    CollectionTarget,
    CollectorError,
    CredentialLease,
    MediaAsset,
)


class InstagramAdapter(PlatformAdapter):
    platform = Platform.INSTAGRAM
    capabilities = {
        Capability.ACCOUNT_TIMELINE,
        Capability.CONTENT_DETAIL,
        Capability.COMMENTS,
        Capability.MEDIA_DOWNLOAD,
        Capability.LIVE_RESOLVE,
        Capability.PROFILE,
        Capability.SEARCH_KEYWORD,
    }

    def __init__(self) -> None:
        self.settings = get_settings()
        self.browser_comment_collector = BrowserCommentCollector()
        self.browser_search_collector = BrowserSearchCollector()

    def healthcheck(self) -> AdapterHealth:
        try:
            import yt_dlp  # noqa: F401

            yt_dlp_available = True
        except Exception:
            yt_dlp_available = False
        profile = self._browser_profile()
        message = (
            f"browser_profile={self.settings.instagram_browser}:{profile}"
            if profile
            else "browser profile not configured"
        )
        if not yt_dlp_available:
            message = "yt-dlp is not installed"
        return AdapterHealth(
            platform=self.platform,
            ok=bool(self.settings.instagram_enabled and yt_dlp_available and profile),
            mode="yt-dlp",
            message=message,
        )

    def discover(self, request: CollectionRequest, credential: CredentialLease | None = None) -> CollectionBatch:
        return self._collect(request)

    def collect_comments(self, request: CollectionRequest, credential: CredentialLease | None = None) -> CollectionBatch:
        return self._collect(request)

    def collect_profile(self, request: CollectionRequest, credential: CredentialLease | None = None) -> CollectionBatch:
        return self._collect(request)

    def resolve_media(self, request: CollectionRequest, credential: CredentialLease | None = None) -> CollectionBatch:
        return self._collect(request)

    def _collect(self, request: CollectionRequest) -> CollectionBatch:
        if not self.settings.instagram_enabled:
            raise CollectorError(CollectorErrorCode.ACCESS_DENIED, "Instagram adapter is disabled", retryable=False)
        try:
            from yt_dlp import YoutubeDL
        except Exception as exc:
            raise CollectorError(CollectorErrorCode.COLLECTOR_BUG, f"yt-dlp is not installed: {exc}") from exc

        profile = self._browser_profile()
        if not profile:
            raise CollectorError(
                CollectorErrorCode.AUTH_EXPIRED,
                "Instagram browser profile is not configured",
                retryable=False,
                credential_action=CredentialAction.REAUTH,
            )
        if request.target.target_type == "KEYWORD":
            return self._collect_keyword(request, profile)

        if request.capability == Capability.LIVE_RESOLVE:
            return self._collect_live(request, profile)

        target_url = self._target_to_url(request)
        output_dir = (self.settings.instagram_output_root / request.job_id).resolve()
        output_dir.mkdir(parents=True, exist_ok=True)
        params: dict[str, Any] = {
            "quiet": True,
            "no_warnings": True,
            "ignoreerrors": True,
            "skip_download": not request.download_media,
            "playlistend": max(1, request.limit),
            "noplaylist": request.target.target_type == "CONTENT",
            "outtmpl": {"default": str(output_dir / "%(id)s" / "%(id)s.%(ext)s")},
            "paths": {"home": str(output_dir)},
            "socket_timeout": min(int(request.target.metadata.get("socket_timeout", 30)), 120),
            "retries": int(request.target.metadata.get("retries", 2)),
            "cookiesfrombrowser": (self.settings.instagram_browser or "chromium", str(profile), None, None),
        }
        if request.download_media:
            params["merge_output_format"] = str(request.target.metadata.get("merge_output_format") or "mp4")
            max_height = int(request.target.metadata.get("max_height") or 720)
            params["format"] = f"bv*[height<={max_height}]+ba/b[height<={max_height}]/bv*+ba/b"
        else:
            params["simulate"] = True

        try:
            with YoutubeDL(params) as ydl:
                info = ydl.extract_info(target_url, download=bool(request.download_media))
        except Exception as exc:
            raise self._map_error(exc) from exc

        batch = self._batch_from_info(info or {}, request)
        if not batch.contents:
            # Instagram's user extractor is currently broken in yt-dlp, but
            # the logged-in web profile still renders the account grid.  Use
            # the browser to discover concrete /reel/ and /p/ URLs, then feed
            # those stable content URLs back through yt-dlp.
            if request.target.target_type == "ACCOUNT":
                browser_batch = self._collect_account_via_browser_links(request, profile)
                if browser_batch.contents:
                    return browser_batch
            raise CollectorError(
                CollectorErrorCode.ACCESS_DENIED,
                f"Instagram returned no collectable content for {target_url}",
                retryable=False,
            )
        self._add_browser_comments(batch, request, target_url, profile)
        batch.raw = {"target_url": target_url, "output_dir": str(output_dir), "item_count": len(batch.contents)}
        return batch

    def _collect_account_via_browser_links(self, request: CollectionRequest, profile: Path) -> CollectionBatch:
        account_url = self._target_to_url(request)
        links = self._browser_search_links(
            url=account_url,
            profile=profile,
            # A profile grid contains photos mixed with reels and some old
            # reels may be deleted.  Fetch a wider candidate window so one
            # unavailable post does not make the whole account look empty.
            limit=max(request.limit * 10, 10),
            timeout_seconds=int(request.target.metadata.get("browser_search_timeout_seconds") or request.target.metadata.get("timeout_seconds") or 60),
        )
        # Reels are the requested short-video type; keep normal posts as a
        # fallback because Instagram also publishes videos under /p/.
        links = sorted(links, key=lambda link: ("/reel/" not in link, link))
        merged = CollectionBatch(raw={"target_url": account_url, "account_browser_links": links, "item_count": 0})
        seen_accounts: set[str] = set()
        seen_contents: set[str] = set()
        seen_assets: set[tuple[str, str, str, str]] = set()
        for link in links:
            if len(merged.contents) >= request.limit:
                break
            content_request = request.model_copy(
                update={
                    "limit": 1,
                    "target": request.target.model_copy(
                        update={"target_type": "CONTENT", "target_value": link, "source_url": link}
                    ),
                }
            )
            try:
                batch = self._collect(content_request)
            except CollectorError as exc:
                # A content URL redirecting to login means the whole profile
                # is unauthenticated, not that this one reel disappeared.
                # Bubble it up so the account scheduler opens its cooldown.
                if exc.code == CollectorErrorCode.AUTH_EXPIRED:
                    raise
                continue
            for account in batch.accounts:
                if account.external_id not in seen_accounts:
                    seen_accounts.add(account.external_id)
                    merged.accounts.append(account)
            for content in batch.contents:
                if content.external_id in seen_contents:
                    continue
                seen_contents.add(content.external_id)
                merged.contents.append(content)
            for comment in batch.comments:
                if comment.external_id not in {item.external_id for item in merged.comments}:
                    merged.comments.append(comment)
            for asset in batch.assets:
                key = (asset.content_external_id, asset.asset_type, asset.local_path, asset.url)
                if key not in seen_assets:
                    seen_assets.add(key)
                    merged.assets.append(asset)
        merged.next_cursor = {"last_external_id": merged.contents[-1].external_id} if merged.contents else {}
        merged.raw["item_count"] = len(merged.contents)
        return merged

    def _collect_live(self, request: CollectionRequest, profile: Path) -> CollectionBatch:
        """Resolve an Instagram Live and record only a bounded, confirmed-live clip."""
        try:
            from yt_dlp import YoutubeDL
        except Exception as exc:
            raise CollectorError(CollectorErrorCode.COLLECTOR_BUG, f"yt-dlp is not installed: {exc}") from exc

        target_url = self._target_to_url(request)
        output_dir = (self.settings.instagram_output_root / request.job_id).resolve()
        output_dir.mkdir(parents=True, exist_ok=True)
        params = self._ydl_params(request, profile, output_dir)
        try:
            with YoutubeDL(params | {"skip_download": True, "simulate": True}) as ydl:
                info = ydl.extract_info(target_url, download=False)
        except Exception as exc:
            raise self._map_error(exc) from exc

        batch = self._batch_from_info(info or {}, request)
        if not batch.contents:
            raise CollectorError(
                CollectorErrorCode.ACCESS_DENIED,
                f"Instagram returned no collectable live metadata for {target_url}",
                retryable=False,
            )
        if not self._info_is_live(info or {}):
            batch.raw = {"target_url": target_url, "output_dir": str(output_dir), "item_count": len(batch.contents), "is_live": False}
            return batch
        if request.download_media:
            self._download_live_with_ffmpeg(info or {}, request, output_dir, batch)
        batch.raw = {"target_url": target_url, "output_dir": str(output_dir), "item_count": len(batch.contents), "is_live": True}
        return batch

    def _ydl_params(self, request: CollectionRequest, profile: Path, output_dir: Path) -> dict[str, Any]:
        params: dict[str, Any] = {
            "quiet": True,
            "no_warnings": True,
            "ignoreerrors": True,
            "skip_download": not request.download_media,
            "playlistend": max(1, request.limit),
            "noplaylist": request.target.target_type == "CONTENT",
            "outtmpl": {"default": str(output_dir / "%(id)s" / "%(id)s.%(ext)s")},
            "paths": {"home": str(output_dir)},
            "socket_timeout": min(int(request.target.metadata.get("socket_timeout", 30)), 120),
            "retries": int(request.target.metadata.get("retries", 2)),
            "cookiesfrombrowser": (self.settings.instagram_browser or "chromium", str(profile), None, None),
        }
        if request.download_media:
            params["merge_output_format"] = str(request.target.metadata.get("merge_output_format") or "mp4")
        return params

    def _info_is_live(self, info: dict[str, Any]) -> bool:
        if bool(info.get("is_live")):
            return True
        return str(info.get("live_status") or "").strip().lower() in {"is_live", "live"}

    def _download_live_with_ffmpeg(self, info: dict[str, Any], request: CollectionRequest, output_dir: Path, batch: CollectionBatch) -> None:
        if not batch.contents:
            return
        ffmpeg = shutil.which("ffmpeg")
        if not ffmpeg:
            raise CollectorError(CollectorErrorCode.COLLECTOR_BUG, "ffmpeg is required for Instagram live clip recording", retryable=False)
        stream_url = self._select_live_stream_url(info, request)
        if not stream_url:
            raise CollectorError(CollectorErrorCode.ACCESS_DENIED, "Instagram live stream manifest URL was not found", retryable=False)
        seconds = min(max(int(request.target.metadata.get("instagram_live_clip_seconds") or request.target.metadata.get("liveClipSeconds") or 60), 5), 3600)
        content = batch.contents[0]
        content_dir = output_dir / content.external_id
        content_dir.mkdir(parents=True, exist_ok=True)
        video_path = content_dir / f"{content.external_id}-live-{seconds}s.mp4"
        command = [
            ffmpeg, "-y", "-hide_banner", "-loglevel", "error", "-i", stream_url,
            "-t", str(seconds), "-c", "copy", "-movflags", "+faststart", str(video_path),
        ]
        timeout = seconds + int(request.target.metadata.get("instagram_live_ffmpeg_timeout_margin_seconds") or 60)
        try:
            result = subprocess.run(command, capture_output=True, text=True, timeout=timeout)
        except subprocess.TimeoutExpired as exc:
            raise CollectorError(CollectorErrorCode.NETWORK_TEMPORARY, "ffmpeg Instagram live clip timed out", retryable=True) from exc
        if result.returncode != 0:
            raise CollectorError(CollectorErrorCode.NETWORK_TEMPORARY, f"ffmpeg Instagram live clip failed: {result.stderr.strip()[:500]}", retryable=True)
        if not video_path.is_file() or video_path.stat().st_size <= 0:
            raise CollectorError(CollectorErrorCode.NETWORK_TEMPORARY, "ffmpeg Instagram live clip produced no media", retryable=True)
        batch.assets = [asset for asset in batch.assets if not (asset.content_external_id == content.external_id and asset.asset_type in {"VIDEO", "MEDIA"})]
        batch.assets.insert(0, MediaAsset(platform=Platform.INSTAGRAM, content_external_id=content.external_id, asset_type="VIDEO", local_path=str(video_path)))
        content.raw.update({"live_clip_seconds": seconds, "live_direct_ffmpeg": True})
        batch.raw.update({"live_clip_seconds": seconds, "live_direct_ffmpeg": True})

    def _select_live_stream_url(self, info: dict[str, Any], request: CollectionRequest) -> str:
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

    def _collect_keyword(self, request: CollectionRequest, profile: Path) -> CollectionBatch:
        keyword = request.target.target_value.strip()
        search_url = request.target.source_url or search_url_for_keyword(Platform.INSTAGRAM, keyword)
        candidate_limit = _instagram_candidate_limit(request)
        links = self._browser_search_links(
            url=search_url,
            profile=profile,
            limit=candidate_limit,
            timeout_seconds=int(request.target.metadata.get("browser_search_timeout_seconds") or request.target.metadata.get("timeout_seconds") or 60),
        )
        links = _prioritize_instagram_video_links(links)
        if not links:
            raise CollectorError(
                CollectorErrorCode.ACCESS_DENIED,
                f"Instagram returned no keyword content links for {search_url}",
                retryable=False,
            )
        merged = CollectionBatch(raw={"target_url": search_url, "item_count": 0})
        seen_accounts: set[str] = set()
        seen_contents: set[str] = set()
        seen_comments: set[str] = set()
        seen_assets: set[tuple[str, str, str, str]] = set()
        attempted_links: list[str] = []
        for link in links:
            if len(merged.contents) >= request.limit:
                break
            attempted_links.append(link)
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
                # Do not burn the remaining keyword candidates after the
                # first authenticated request confirms that the profile has
                # fallen back to Instagram's login wall.
                if exc.code == CollectorErrorCode.AUTH_EXPIRED:
                    raise
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
        merged.raw = {
            "target_url": search_url,
            "item_count": len(merged.contents),
            "content_links": attempted_links,
            "candidate_count": len(links),
        }
        return merged

    def _browser_search_links(self, *, url: str, profile: Path, limit: int, timeout_seconds: int) -> list[str]:
        """Return rendered links, but fail closed when the session is at login.

        BrowserSearchCollector intentionally returns an empty result for a
        rendered page.  Without checking the final URL, an expired Instagram
        session looked exactly like a keyword/account with no videos and was
        retried account-by-account.  The account scheduler can now open its
        platform cooldown on this explicit AUTH_EXPIRED signal.
        """
        result = self.browser_search_collector.collect_with_context(
            url=url,
            platform=Platform.INSTAGRAM,
            profile=profile,
            browser=self.settings.instagram_browser or "chromium",
            limit=limit,
            timeout_seconds=timeout_seconds,
            cdp_url=str(self.settings.instagram_browser_cdp_url or "").strip(),
        )
        page_url = str(result.page_url or "").lower()
        body = str(result.body_text or "").lower()
        if "/accounts/login" in page_url or "instagram.com/accounts/login" in page_url:
            raise CollectorError(
                CollectorErrorCode.AUTH_EXPIRED,
                "INSTAGRAM browser session redirected to login; reauthentication required",
                retryable=False,
                credential_action=CredentialAction.REAUTH,
            )
        if not result.links and ("log in" in body or "登录" in body) and "instagram" in body:
            raise CollectorError(
                CollectorErrorCode.AUTH_EXPIRED,
                "INSTAGRAM browser session is no longer authenticated",
                retryable=False,
                credential_action=CredentialAction.REAUTH,
            )
        return result.links

    def _add_browser_comments(self, batch: CollectionBatch, request: CollectionRequest, target_url: str, profile: Path) -> None:
        if not bool(getattr(self.settings, "browser_comment_collection_enabled", False)):
            return
        if not (request.collect_comments or request.capability == Capability.COMMENTS):
            return
        existing = {comment.external_id for comment in batch.comments}
        limit = int(request.target.metadata.get("browser_comment_limit") or self.settings.browser_comment_limit)
        timeout_seconds = int(request.target.metadata.get("browser_comment_timeout_seconds") or self.settings.browser_comment_timeout_seconds)
        for content in batch.contents:
            comments = self.browser_comment_collector.collect(
                url=content.url or target_url,
                platform=self.platform,
                content_id=content.external_id,
                profile=profile,
                browser=self.settings.instagram_browser or "chromium",
                limit=limit,
                timeout_seconds=timeout_seconds,
                cdp_url=str(self.settings.instagram_browser_cdp_url or "").strip(),
            )
            for comment in comments:
                if comment.external_id in existing:
                    continue
                existing.add(comment.external_id)
                batch.comments.append(comment)

    def _batch_from_info(self, info: dict[str, Any], request: CollectionRequest) -> CollectionBatch:
        entries = info.get("entries")
        if isinstance(entries, list):
            raw_items = [item for item in entries if isinstance(item, dict)]
        else:
            raw_items = [info]

        accounts_by_id: dict[str, CollectedAccount] = {}
        contents: list[CollectedContent] = []
        assets: list[MediaAsset] = []
        for item in raw_items[: request.limit]:
            content_id = str(item.get("id") or item.get("display_id") or "").strip()
            if not content_id:
                continue
            account = self._account_from_item(item)
            if account and account.external_id not in accounts_by_id:
                accounts_by_id[account.external_id] = account
            contents.append(self._content_from_item(item, account.external_id if account else ""))
            assets.extend(self._assets_from_item(item, content_id))
        return CollectionBatch(
            accounts=list(accounts_by_id.values()),
            contents=contents,
            assets=assets,
            next_cursor={"last_external_id": contents[-1].external_id} if contents else {},
        )

    def _account_from_item(self, item: dict[str, Any]) -> CollectedAccount | None:
        external_id = str(item.get("uploader_id") or item.get("channel_id") or item.get("channel") or item.get("uploader") or "").strip()
        if not external_id:
            return None
        username = str(item.get("channel") or item.get("uploader") or "")
        return CollectedAccount(
            platform=Platform.INSTAGRAM,
            external_id=external_id,
            username=username,
            nickname=str(item.get("uploader") or item.get("channel") or ""),
            profile_url=f"https://www.instagram.com/{username}/" if username else "",
            raw=self._small_raw(item, ("uploader_id", "uploader", "channel", "channel_id")),
        )

    def _content_from_item(self, item: dict[str, Any], author_external_id: str) -> CollectedContent:
        webpage_url = str(item.get("webpage_url") or item.get("original_url") or "")
        is_live = self._info_is_live(item)
        return CollectedContent(
            platform=Platform.INSTAGRAM,
            external_id=str(item.get("id") or item.get("display_id")),
            url=webpage_url,
            content_type="LIVE" if is_live else "SHORT_VIDEO",
            title=str(item.get("title") or ""),
            description=str(item.get("description") or ""),
            author_external_id=author_external_id,
            published_at=self._timestamp_to_datetime(item.get("timestamp")),
            raw={
                **self._small_raw(
                    item,
                    ("id", "title", "description", "duration", "view_count", "like_count", "comment_count", "webpage_url", "thumbnail", "timestamp", "live_status", "availability", "concurrent_view_count"),
                ),
                "is_live": is_live,
                "live_status": "is_live" if is_live else str(item.get("live_status") or "not_live").lower(),
            },
        )

    def _assets_from_item(self, item: dict[str, Any], content_id: str) -> list[MediaAsset]:
        assets: list[MediaAsset] = []
        requested_downloads = item.get("requested_downloads")
        if isinstance(requested_downloads, list):
            for download in requested_downloads:
                if isinstance(download, dict) and download.get("filepath"):
                    assets.append(
                        MediaAsset(
                            platform=Platform.INSTAGRAM,
                            content_external_id=content_id,
                            asset_type="VIDEO",
                            local_path=str(download["filepath"]),
                            raw=self._small_raw(download, ("format_id", "ext", "width", "height", "filepath")),
                        )
                    )
        thumbnail = str(item.get("thumbnail") or "")
        if thumbnail:
            assets.append(MediaAsset(platform=Platform.INSTAGRAM, content_external_id=content_id, asset_type="THUMBNAIL", url=thumbnail))
        return assets

    def _browser_profile(self) -> Path | None:
        if self.settings.instagram_browser_profile:
            profile = Path(self.settings.instagram_browser_profile)
            if str(profile).strip() not in {"", "."} and profile.exists():
                return profile
        return None

    def _target_to_url(self, request: CollectionRequest) -> str:
        value = request.target.source_url or request.target.target_value.strip()
        if value.startswith("http://") or value.startswith("https://"):
            return value
        if request.target.target_type == "CONTENT":
            return f"https://www.instagram.com/p/{value.strip('/')}/"
        return f"https://www.instagram.com/{value.lstrip('@')}/"

    def _map_error(self, exc: Exception) -> CollectorError:
        text = str(exc)
        lowered = text.lower()
        if "login" in lowered or "cookie" in lowered or "auth" in lowered:
            return CollectorError(CollectorErrorCode.AUTH_EXPIRED, text, retryable=False, credential_action=CredentialAction.REAUTH)
        if "429" in lowered or "rate" in lowered:
            return CollectorError(CollectorErrorCode.RATE_LIMITED, text, retryable=True, credential_action=CredentialAction.COOLDOWN)
        if "timed out" in lowered or "network" in lowered or "connection" in lowered:
            return CollectorError(CollectorErrorCode.NETWORK_TEMPORARY, text, retryable=True)
        return CollectorError(CollectorErrorCode.PLATFORM_CHANGED, text, retryable=False)

    def _timestamp_to_datetime(self, value: Any):
        if value is None:
            return None
        try:
            from datetime import datetime, timezone

            return datetime.fromtimestamp(float(value), timezone.utc)
        except Exception:
            return None

    def _small_raw(self, source: dict[str, Any], keys: tuple[str, ...]) -> dict[str, Any]:
        return {key: source.get(key) for key in keys if key in source}


def _instagram_candidate_limit(request: CollectionRequest) -> int:
    metadata = request.target.metadata or {}
    raw_value = metadata.get("instagram_search_candidate_limit") or metadata.get("browser_search_candidate_limit")
    if raw_value is not None:
        return max(request.limit, int(raw_value))
    return max(request.limit, min(50, request.limit * 5))


def _prioritize_instagram_video_links(links: list[str]) -> list[str]:
    reels = [link for link in links if "/reel/" in link]
    others = [link for link in links if "/reel/" not in link]
    return reels + others
