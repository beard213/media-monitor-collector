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


class XAdapter(PlatformAdapter):
    platform = Platform.X
    capabilities = {
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
        raise CollectorError(
            CollectorErrorCode.ACCESS_DENIED,
            "X comments/replies collection is not implemented yet",
            retryable=False,
        )

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
            published_at=None,
            raw=self._small_raw(info, ("id", "title", "description", "webpage_url", "like_count", "repost_count")),
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
        return CollectionBatch(accounts=accounts, contents=[content], assets=assets, raw={"target_url": target_url, "output_dir": str(output_dir)})

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

    def _small_raw(self, source: dict[str, Any], keys: tuple[str, ...]) -> dict[str, Any]:
        return {key: source.get(key) for key in keys if key in source}
