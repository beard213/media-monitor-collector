from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.adapters.browser_comments import BrowserCommentCollector
from app.adapters.browser_search import BrowserSearchCollector, search_url_for_keyword
from app.adapters.base import PlatformAdapter
from app.core.config import get_settings
from app.domain.enums import Capability, CollectorErrorCode, CredentialAction, Platform
from app.domain.models import (
    AdapterHealth,
    CollectedAccount,
    CollectedContent,
    CollectionBatch,
    CollectionRequest,
    CollectorError,
    CredentialLease,
    MediaAsset,
)


class XAdapter(PlatformAdapter):
    platform = Platform.X
    capabilities = {
        Capability.SEARCH_KEYWORD,
        Capability.CONTENT_DETAIL,
        Capability.COMMENTS,
        Capability.MEDIA_DOWNLOAD,
        Capability.PROFILE,
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
            f"browser_profile={self.settings.x_browser}:{profile}"
            if profile
            else "browser profile not configured"
        )
        if not yt_dlp_available:
            message = "yt-dlp is not installed"
        return AdapterHealth(
            platform=self.platform,
            ok=bool(self.settings.x_enabled and yt_dlp_available and profile),
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
        if not self.settings.x_enabled:
            raise CollectorError(CollectorErrorCode.ACCESS_DENIED, "X adapter is disabled", retryable=False)
        try:
            from yt_dlp import YoutubeDL
        except Exception as exc:
            raise CollectorError(CollectorErrorCode.COLLECTOR_BUG, f"yt-dlp is not installed: {exc}") from exc

        if request.target.target_type == "KEYWORD":
            return self._collect_keyword(request)

        target_url = self._target_to_url(request)
        output_dir = (self.settings.x_output_root / request.job_id).resolve()
        output_dir.mkdir(parents=True, exist_ok=True)
        params: dict[str, Any] = {
            "quiet": True,
            "no_warnings": True,
            "ignoreerrors": True,
            "skip_download": not request.download_media,
            "noplaylist": True,
            "outtmpl": {"default": str(output_dir / "%(id)s" / "%(id)s.%(ext)s")},
            "paths": {"home": str(output_dir)},
            "socket_timeout": min(int(request.target.metadata.get("socket_timeout", 30)), 120),
            "retries": int(request.target.metadata.get("retries", 2)),
        }
        profile = self._browser_profile()
        if profile:
            params["cookiesfrombrowser"] = (self.settings.x_browser or "chromium", str(profile), None, None)
        if request.download_media:
            params["merge_output_format"] = str(request.target.metadata.get("merge_output_format") or "mp4")

        try:
            with YoutubeDL(params) as ydl:
                info = ydl.extract_info(target_url, download=bool(request.download_media))
        except Exception as exc:
            raise self._map_error(exc) from exc

        if not isinstance(info, dict) or not info.get("id"):
            raise CollectorError(
                CollectorErrorCode.ACCESS_DENIED,
                f"X returned no collectable content for {target_url}",
                retryable=False,
            )
        target_url = str(info.get("webpage_url") or target_url)
        account_id = str(info.get("uploader_id") or info.get("uploader") or info.get("channel") or "")
        accounts = []
        if account_id:
            accounts.append(
                CollectedAccount(
                    platform=Platform.X,
                    external_id=account_id,
                    username=str(info.get("uploader_id") or ""),
                    nickname=str(info.get("uploader") or info.get("channel") or ""),
                    profile_url=str(info.get("channel_url") or info.get("uploader_url") or ""),
                    raw=self._small_raw(info, ("uploader", "uploader_id", "channel", "channel_url")),
                )
            )
        content = CollectedContent(
            platform=Platform.X,
            external_id=str(info.get("id")),
            url=str(info.get("webpage_url") or target_url),
            content_type="POST",
            title=str(info.get("title") or ""),
            description=str(info.get("description") or ""),
            author_external_id=account_id,
            published_at=self._timestamp_to_datetime(info.get("timestamp")),
            raw=self._small_raw(info, ("id", "title", "description", "webpage_url", "like_count", "repost_count", "timestamp")),
        )
        assets = []
        requested_downloads = info.get("requested_downloads")
        if isinstance(requested_downloads, list):
            for download in requested_downloads:
                if isinstance(download, dict) and download.get("filepath"):
                    assets.append(
                        MediaAsset(
                            platform=Platform.X,
                            content_external_id=content.external_id,
                            asset_type="MEDIA",
                            local_path=str(download["filepath"]),
                            raw=self._small_raw(download, ("format_id", "ext", "filepath")),
                        )
                    )
        thumbnail = str(info.get("thumbnail") or "")
        if thumbnail:
            assets.append(MediaAsset(platform=Platform.X, content_external_id=content.external_id, asset_type="THUMBNAIL", url=thumbnail))
        batch = CollectionBatch(accounts=accounts, contents=[content], assets=assets, raw={"target_url": target_url, "output_dir": str(output_dir)})
        self._add_browser_comments(batch, request, target_url)
        return batch

    def _collect_keyword(self, request: CollectionRequest) -> CollectionBatch:
        profile = self._browser_profile()
        if not profile:
            raise CollectorError(
                CollectorErrorCode.AUTH_EXPIRED,
                "X browser profile is not configured",
                retryable=False,
                credential_action=CredentialAction.REAUTH,
            )
        keyword = request.target.target_value.strip()
        search_url = request.target.source_url or search_url_for_keyword(Platform.X, keyword)
        links = self.browser_search_collector.collect(
            url=search_url,
            platform=Platform.X,
            profile=profile,
            browser=self.settings.x_browser or "chromium",
            limit=request.limit,
            timeout_seconds=int(request.target.metadata.get("browser_search_timeout_seconds") or request.target.metadata.get("timeout_seconds") or 60),
            cdp_url=str(self.settings.x_browser_cdp_url or "").strip(),
        )
        if not links:
            raise CollectorError(
                CollectorErrorCode.ACCESS_DENIED,
                f"X returned no keyword content links for {search_url}",
                retryable=False,
            )

        merged = CollectionBatch(raw={"target_url": search_url, "item_count": 0})
        seen_accounts: set[str] = set()
        seen_contents: set[str] = set()
        seen_comments: set[str] = set()
        seen_assets: set[tuple[str, str, str, str]] = set()
        for link in links[: request.limit]:
            content_request = request.model_copy(
                update={
                    "limit": 1,
                    "target": request.target.model_copy(
                        update={
                            "target_type": "CONTENT",
                            "target_value": link,
                            "source_url": link,
                        }
                    ),
                }
            )
            try:
                batch = self._collect(content_request)
            except CollectorError:
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
        merged.raw = {"target_url": search_url, "item_count": len(merged.contents), "content_links": links[: request.limit]}
        return merged

    def _add_browser_comments(self, batch: CollectionBatch, request: CollectionRequest, target_url: str) -> None:
        if not bool(getattr(self.settings, "browser_comment_collection_enabled", False)):
            return
        if not (request.collect_comments or request.capability == Capability.COMMENTS):
            return
        profile = self._browser_profile()
        if not profile:
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
                browser=self.settings.x_browser or "chromium",
                limit=limit,
                timeout_seconds=timeout_seconds,
                cdp_url=str(self.settings.x_browser_cdp_url or "").strip(),
            )
            for comment in comments:
                if comment.external_id in existing:
                    continue
                existing.add(comment.external_id)
                batch.comments.append(comment)

    def _browser_profile(self) -> Path | None:
        if self.settings.x_browser_profile:
            profile = Path(self.settings.x_browser_profile)
            if profile.exists():
                return profile
        return None

    def _target_to_url(self, request: CollectionRequest) -> str:
        value = request.target.source_url or request.target.target_value.strip()
        if value.startswith("http://") or value.startswith("https://"):
            return value
        return f"https://x.com/{value}"

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
            return datetime.fromtimestamp(float(value), timezone.utc)
        except Exception:
            return None

    def _small_raw(self, source: dict[str, Any], keys: tuple[str, ...]) -> dict[str, Any]:
        return {key: source.get(key) for key in keys if key in source}
