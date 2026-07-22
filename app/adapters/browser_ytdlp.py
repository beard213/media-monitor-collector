from __future__ import annotations

from pathlib import Path
from typing import Any

from app.adapters.base import PlatformAdapter
from app.adapters.browser_comments import BrowserCommentCollector
from app.core.config import get_settings
from app.domain.enums import Capability, CollectorErrorCode, CredentialAction, Platform
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


class BrowserYtDlpAdapter(PlatformAdapter):
    platform: Platform
    capabilities = {
        Capability.ACCOUNT_TIMELINE,
        Capability.CONTENT_DETAIL,
        Capability.COMMENTS,
        Capability.PROFILE,
        Capability.MEDIA_DOWNLOAD,
    }
    content_type = "VIDEO"

    def __init__(self) -> None:
        self.settings = get_settings()
        self.browser_comment_collector = BrowserCommentCollector()

    @property
    def enabled(self) -> bool:
        raise NotImplementedError

    @property
    def browser_name(self) -> str:
        raise NotImplementedError

    @property
    def browser_profile_path(self) -> Path | None:
        raise NotImplementedError

    @property
    def output_root(self) -> Path:
        raise NotImplementedError

    def healthcheck(self) -> AdapterHealth:
        try:
            import yt_dlp  # noqa: F401

            yt_dlp_available = True
        except Exception:
            yt_dlp_available = False

        profile = self._browser_profile()
        message = f"browser_profile={self.browser_name}:{profile}" if profile else "browser profile not configured"
        if not yt_dlp_available:
            message = "yt-dlp is not installed"
        return AdapterHealth(
            platform=self.platform,
            ok=bool(self.enabled and yt_dlp_available and profile),
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
        if not self.enabled:
            raise CollectorError(CollectorErrorCode.ACCESS_DENIED, f"{self.platform} adapter is disabled", retryable=False)
        try:
            from yt_dlp import YoutubeDL
        except Exception as exc:
            raise CollectorError(CollectorErrorCode.COLLECTOR_BUG, f"yt-dlp is not installed: {exc}") from exc

        profile = self._browser_profile()
        if not profile:
            raise CollectorError(
                CollectorErrorCode.AUTH_EXPIRED,
                f"{self.platform} browser profile is not configured",
                retryable=False,
                credential_action=CredentialAction.REAUTH,
            )

        target_url = self._target_to_url(request)
        output_dir = (self.output_root / request.job_id).resolve()
        output_dir.mkdir(parents=True, exist_ok=True)
        params = self._ydl_params(request, output_dir, profile)

        try:
            with YoutubeDL(params) as ydl:
                info = ydl.extract_info(target_url, download=bool(request.download_media))
        except Exception as exc:
            raise self._map_error(exc) from exc

        batch = self._batch_from_info(info or {}, request)
        if not batch.contents:
            raise CollectorError(
                CollectorErrorCode.ACCESS_DENIED,
                f"{self.platform} returned no collectable content for {target_url}",
                retryable=False,
            )
        self._add_browser_comments(batch, request, target_url, profile)
        batch.raw = {"target_url": target_url, "output_dir": str(output_dir), "item_count": len(batch.contents)}
        return batch

    def _add_browser_comments(self, batch: CollectionBatch, request: CollectionRequest, target_url: str, profile: Path) -> None:
        if not bool(getattr(self.settings, "browser_comment_collection_enabled", False)):
            return
        if not (request.collect_comments or request.capability == Capability.COMMENTS):
            return
        if not batch.contents:
            return
        existing = {comment.external_id for comment in batch.comments}
        limit = int(request.target.metadata.get("browser_comment_limit") or self.settings.browser_comment_limit)
        timeout_seconds = int(request.target.metadata.get("browser_comment_timeout_seconds") or self.settings.browser_comment_timeout_seconds)
        for content in batch.contents:
            if len([comment for comment in batch.comments if comment.content_external_id == content.external_id]) >= limit:
                continue
            comments = self.browser_comment_collector.collect(
                url=content.url or target_url,
                platform=self.platform,
                content_id=content.external_id,
                profile=profile,
                browser=self.browser_name,
                limit=limit,
                timeout_seconds=timeout_seconds,
            )
            for comment in comments:
                if comment.external_id in existing:
                    continue
                existing.add(comment.external_id)
                batch.comments.append(comment)

    def _ydl_params(self, request: CollectionRequest, output_dir: Path, profile: Path) -> dict[str, Any]:
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
            "cookiesfrombrowser": (self.browser_name or "chromium", str(profile), None, None),
            "getcomments": bool(request.collect_comments or request.capability == Capability.COMMENTS),
        }
        if request.download_media:
            max_height = int(request.target.metadata.get("max_height") or 720)
            params["format"] = f"bv*[height<={max_height}]+ba/b[height<={max_height}]/bv*+ba/b"
            params["merge_output_format"] = str(request.target.metadata.get("merge_output_format") or "mp4")
        else:
            params["simulate"] = True
        return params

    def _batch_from_info(self, info: dict[str, Any], request: CollectionRequest) -> CollectionBatch:
        entries = info.get("entries")
        raw_items = [item for item in entries if isinstance(item, dict)] if isinstance(entries, list) else [info]

        accounts_by_id: dict[str, CollectedAccount] = {}
        contents: list[CollectedContent] = []
        comments: list[CollectedComment] = []
        assets: list[MediaAsset] = []
        for item in raw_items[: request.limit]:
            content_id = str(item.get("id") or item.get("display_id") or "").strip()
            if not content_id:
                continue
            account = self._account_from_item(item)
            if account and account.external_id not in accounts_by_id:
                accounts_by_id[account.external_id] = account
            contents.append(self._content_from_item(item, account.external_id if account else ""))
            comments.extend(self._comments_from_item(item, content_id))
            assets.extend(self._assets_from_item(item, content_id))
        return CollectionBatch(
            accounts=list(accounts_by_id.values()),
            contents=contents,
            comments=comments,
            assets=assets,
            next_cursor={"last_external_id": contents[-1].external_id} if contents else {},
        )

    def _account_from_item(self, item: dict[str, Any]) -> CollectedAccount | None:
        external_id = str(item.get("uploader_id") or item.get("channel_id") or item.get("channel") or item.get("uploader") or "").strip()
        if not external_id:
            return None
        username = str(item.get("channel") or item.get("uploader_id") or item.get("uploader") or "")
        return CollectedAccount(
            platform=self.platform,
            external_id=external_id,
            username=username,
            nickname=str(item.get("uploader") or item.get("channel") or ""),
            profile_url=str(item.get("channel_url") or item.get("uploader_url") or ""),
            raw=self._small_raw(item, ("uploader_id", "uploader", "channel", "channel_id", "channel_url", "uploader_url")),
        )

    def _content_from_item(self, item: dict[str, Any], author_external_id: str) -> CollectedContent:
        return CollectedContent(
            platform=self.platform,
            external_id=str(item.get("id") or item.get("display_id")),
            url=str(item.get("webpage_url") or item.get("original_url") or ""),
            content_type=self.content_type,
            title=str(item.get("title") or ""),
            description=str(item.get("description") or ""),
            author_external_id=author_external_id,
            published_at=self._timestamp_to_datetime(item.get("timestamp")),
            raw=self._small_raw(
                item,
                ("id", "title", "description", "duration", "view_count", "like_count", "comment_count", "webpage_url", "thumbnail", "timestamp"),
            ),
        )

    def _assets_from_item(self, item: dict[str, Any], content_id: str) -> list[MediaAsset]:
        assets: list[MediaAsset] = []
        requested_downloads = item.get("requested_downloads")
        if isinstance(requested_downloads, list):
            for download in requested_downloads:
                if isinstance(download, dict) and download.get("filepath"):
                    assets.append(
                        MediaAsset(
                            platform=self.platform,
                            content_external_id=content_id,
                            asset_type="VIDEO",
                            local_path=str(download["filepath"]),
                            raw=self._small_raw(download, ("format_id", "ext", "width", "height", "filepath")),
                        )
                    )
        thumbnail = str(item.get("thumbnail") or "")
        if thumbnail:
            assets.append(MediaAsset(platform=self.platform, content_external_id=content_id, asset_type="THUMBNAIL", url=thumbnail))
        return assets

    def _comments_from_item(self, item: dict[str, Any], content_id: str) -> list[CollectedComment]:
        raw_comments = item.get("comments")
        if not isinstance(raw_comments, list):
            return []
        comments: list[CollectedComment] = []
        for index, raw in enumerate(raw_comments):
            if not isinstance(raw, dict):
                continue
            text = str(raw.get("text") or raw.get("content") or "").strip()
            if not text:
                continue
            comment_id = str(raw.get("id") or raw.get("comment_id") or f"{content_id}-{index}")
            comments.append(
                CollectedComment(
                    platform=self.platform,
                    external_id=comment_id,
                    content_external_id=content_id,
                    author_external_id=str(raw.get("author_id") or raw.get("author") or ""),
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
                            "content",
                            "timestamp",
                            "like_count",
                            "parent",
                        ),
                    ),
                )
            )
        return comments

    def _browser_profile(self) -> Path | None:
        profile = self.browser_profile_path
        if profile:
            candidate = Path(profile)
            if str(candidate).strip() not in {"", "."} and candidate.exists():
                return candidate
        return None

    def _target_to_url(self, request: CollectionRequest) -> str:
        value = request.target.source_url or request.target.target_value.strip()
        if value.startswith("http://") or value.startswith("https://"):
            return value
        if request.target.target_type == "CONTENT":
            return self._content_url(value)
        return self._account_url(value)

    def _account_url(self, value: str) -> str:
        raise NotImplementedError

    def _content_url(self, value: str) -> str:
        return self._account_url(value)

    def _map_error(self, exc: Exception) -> CollectorError:
        text = str(exc)
        lowered = text.lower()
        if "login" in lowered or "sign in" in lowered or "cookie" in lowered or "auth" in lowered:
            return CollectorError(CollectorErrorCode.AUTH_EXPIRED, text, retryable=False, credential_action=CredentialAction.REAUTH)
        if "429" in lowered or "rate" in lowered:
            return CollectorError(CollectorErrorCode.RATE_LIMITED, text, retryable=True, credential_action=CredentialAction.COOLDOWN)
        if "timed out" in lowered or "network" in lowered or "connection" in lowered:
            return CollectorError(CollectorErrorCode.NETWORK_TEMPORARY, text, retryable=True)
        if "private" in lowered or "unavailable" in lowered or "permission" in lowered:
            return CollectorError(CollectorErrorCode.ACCESS_DENIED, text, retryable=False)
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
