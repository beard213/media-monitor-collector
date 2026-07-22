from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus

from app.adapters.base import PlatformAdapter
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

    def __init__(self) -> None:
        self.settings = get_settings()

    def healthcheck(self) -> AdapterHealth:
        try:
            import yt_dlp  # noqa: F401

            yt_dlp_available = True
        except Exception:
            yt_dlp_available = False

        cookie_file = self._configured_cookie_file(None)
        browser_profile = self._configured_browser_profile(None)
        cookie_count = self._cookie_pool_count()
        message_parts = []
        if not yt_dlp_available:
            message_parts.append("yt-dlp is not installed")
        if cookie_file:
            message_parts.append(f"cookie_file={cookie_file}")
        elif browser_profile:
            browser, profile = browser_profile
            message_parts.append(f"browser_profile={browser}:{profile}")
        elif cookie_count:
            message_parts.append(f"cookie_pool={cookie_count}")
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
        params = self._ydl_params(request, credential, output_dir)
        target_url = self._target_to_url(request)

        try:
            with YoutubeDL(params) as ydl:
                info = ydl.extract_info(target_url, download=bool(request.download_media))
        except Exception as exc:
            raise self._map_youtube_error(exc) from exc

        batch = self._batch_from_info(info or {}, request)
        if not batch.contents:
            raise CollectorError(
                CollectorErrorCode.ACCESS_DENIED,
                f"YouTube returned no collectable content for {target_url}",
                retryable=False,
                credential_action=CredentialAction.NONE,
            )
        batch.raw = {
            "target_url": target_url,
            "output_dir": str(output_dir),
            "download_media": request.download_media,
            "item_count": len(batch.contents),
        }
        return batch

    def _ydl_params(
        self,
        request: CollectionRequest,
        credential: CredentialLease | None,
        output_dir: Path,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "quiet": True,
            "no_warnings": True,
            "ignoreerrors": True,
            "skip_download": not request.download_media,
            "playlistend": max(1, request.limit),
            "noplaylist": request.capability == Capability.CONTENT_DETAIL,
            "getcomments": bool(request.collect_comments),
            "outtmpl": {"default": str(output_dir / "%(id)s" / "%(id)s.%(ext)s")},
            "paths": {"home": str(output_dir)},
            "socket_timeout": min(int(request.target.metadata.get("socket_timeout", 30)), 120),
            "retries": int(request.target.metadata.get("retries", 2)),
        }
        self._add_youtube_runtime_params(params, request)
        if request.download_media:
            max_height = int(request.target.metadata.get("max_height") or 360)
            params["format"] = (
                f"bv*[height<={max_height}]+ba/"
                f"b[height<={max_height}]/"
                "bv*+ba/b"
            )
            params["merge_output_format"] = str(request.target.metadata.get("merge_output_format") or "mp4")
        max_comments = int(request.target.metadata.get("max_comments") or self.settings.youtube_default_comments)
        if request.collect_comments and max_comments > 0:
            params["extractor_args"] = {"youtube": {"max_comments": [str(max_comments)]}}

        cookie_file = self._configured_cookie_file(credential)
        if cookie_file:
            params["cookiefile"] = str(cookie_file)
        else:
            browser_profile = self._configured_browser_profile(credential)
            if browser_profile:
                browser, profile = browser_profile
                params["cookiesfrombrowser"] = (browser, str(profile), None, None)
        proxy = str(request.target.metadata.get("proxy") or self.settings.youtube_proxy or "").strip()
        if proxy:
            params["proxy"] = proxy
        if not request.download_media:
            params["simulate"] = True
        return params

    def _add_youtube_runtime_params(self, params: dict[str, Any], request: CollectionRequest) -> None:
        disable_plugins = request.target.metadata.get("youtube_disable_plugins", self.settings.youtube_disable_plugins)
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
        if credential and credential.credential.credential_type == CredentialType.SESSION_COOKIE:
            candidate = Path(credential.credential.secret_ref)
            return candidate if candidate.is_file() else None
        if self.settings.youtube_cookie_file:
            candidate = Path(self.settings.youtube_cookie_file)
            if candidate.is_file() and candidate.stat().st_size > 0:
                return candidate
        for pattern in ("*.txt", "*.cookies", "*.cookie", "*.netscape"):
            for candidate in sorted(self.settings.youtube_cookie_dir.glob(pattern)):
                if candidate.is_file() and candidate.stat().st_size > 0:
                    return candidate
        return None

    def _configured_browser_profile(self, credential: CredentialLease | None) -> tuple[str, Path] | None:
        if credential and credential.credential.credential_type == CredentialType.BROWSER_PROFILE:
            profile = Path(credential.credential.secret_ref)
            browser = str(credential.credential.metadata.get("browser") or self.settings.youtube_browser or "chrome")
            return (browser, profile) if profile.exists() else None
        if self.settings.youtube_browser_profile:
            profile = Path(self.settings.youtube_browser_profile)
            if self._is_explicit_browser_profile(profile):
                return (self.settings.youtube_browser or "chrome", profile)
        return None

    def _is_explicit_browser_profile(self, profile: Path) -> bool:
        if str(profile).strip() in {"", "."}:
            return False
        return profile.exists()

    def _cookie_pool_count(self) -> int:
        if not self.settings.youtube_cookie_dir.is_dir():
            return 0
        return sum(
            1
            for pattern in ("*.txt", "*.cookies", "*.cookie", "*.netscape")
            for path in self.settings.youtube_cookie_dir.glob(pattern)
            if path.is_file() and path.stat().st_size > 0
        )

    def _target_to_url(self, request: CollectionRequest) -> str:
        target = request.target
        if target.source_url:
            return target.source_url
        value = target.target_value.strip()
        if target.target_type == "KEYWORD":
            return f"ytsearchdate{max(1, request.limit)}:{value}"
        if value.startswith("http://") or value.startswith("https://"):
            return value
        if value.startswith("@"):
            return f"https://www.youtube.com/{value}/videos"
        if value.startswith("UC") and len(value) >= 20:
            return f"https://www.youtube.com/channel/{value}/videos"
        if target.target_type == "CONTENT":
            return f"https://www.youtube.com/watch?v={value}"
        return f"https://www.youtube.com/{value}/videos"

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
        return CollectedContent(
            platform=Platform.YOUTUBE,
            external_id=str(item.get("id") or item.get("display_id")),
            url=webpage_url,
            content_type="SHORT_VIDEO" if "/shorts/" in webpage_url else "VIDEO",
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
                            "timestamp",
                            "like_count",
                            "parent",
                        ),
                    ),
                )
            )
        return comments

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
