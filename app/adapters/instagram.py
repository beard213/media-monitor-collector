from __future__ import annotations

from pathlib import Path
from typing import Any

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


class InstagramAdapter(PlatformAdapter):
    platform = Platform.INSTAGRAM
    capabilities = {
        Capability.ACCOUNT_TIMELINE,
        Capability.CONTENT_DETAIL,
        Capability.MEDIA_DOWNLOAD,
        Capability.PROFILE,
    }

    def __init__(self) -> None:
        self.settings = get_settings()

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
        raise CollectorError(
            CollectorErrorCode.ACCESS_DENIED,
            "Instagram comments collection is not implemented yet",
            retryable=False,
        )

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
            raise CollectorError(
                CollectorErrorCode.ACCESS_DENIED,
                f"Instagram returned no collectable content for {target_url}",
                retryable=False,
            )
        batch.raw = {"target_url": target_url, "output_dir": str(output_dir), "item_count": len(batch.contents)}
        return batch

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
        return CollectedContent(
            platform=Platform.INSTAGRAM,
            external_id=str(item.get("id") or item.get("display_id")),
            url=webpage_url,
            content_type="SHORT_VIDEO",
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
