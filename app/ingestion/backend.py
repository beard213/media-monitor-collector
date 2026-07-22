from __future__ import annotations

import mimetypes
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx

from app.core.config import Settings, get_settings
from app.domain.enums import Capability, CollectorErrorCode, CredentialAction, Platform
from app.domain.models import CollectionBatch, CollectionRequest, CollectedContent, CollectorError, MediaAsset


@dataclass(frozen=True)
class UploadedObject:
    object_key: str
    url: str
    size: int
    content_type: str
    object_type: str


class CollectorBackendClient:
    def __init__(
        self,
        *,
        base_url: str,
        access_key: str,
        access_token: str,
        timeout_seconds: int = 300,
        http_client: httpx.Client | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.access_key = access_key
        self.access_token = access_token
        self._client = http_client or httpx.Client(timeout=timeout_seconds)

    @classmethod
    def from_settings(cls, settings: Settings | None = None) -> "CollectorBackendClient":
        resolved = settings or get_settings()
        return cls(
            base_url=resolved.collector_backend_base_url,
            access_key=resolved.collector_access_key,
            access_token=resolved.collector_access_token,
            timeout_seconds=resolved.collector_backend_timeout_seconds,
        )

    def upload_object(self, path: Path, *, object_type: str, object_key: str) -> UploadedObject:
        if not self.base_url:
            raise CollectorError(
                CollectorErrorCode.COLLECTOR_BUG,
                "collector backend base URL is not configured",
                retryable=False,
            )
        if not path.is_file():
            raise CollectorError(
                CollectorErrorCode.COLLECTOR_BUG,
                f"upload file does not exist: {path}",
                retryable=False,
            )
        content_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
        try:
            with path.open("rb") as file_obj:
                response = self._client.post(
                    f"{self.base_url}/ingestion/collector/objects",
                    headers=self._headers(),
                    data={"objectType": object_type, "objectKey": object_key},
                    files={"file": (path.name, file_obj, content_type)},
                )
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise self._map_backend_error(exc.response, "upload object") from exc
        except httpx.HTTPError as exc:
            raise CollectorError(
                CollectorErrorCode.NETWORK_TEMPORARY,
                f"collector backend upload failed: {exc}",
                retryable=True,
                credential_action=CredentialAction.NONE,
            ) from exc
        payload = response.json()
        return UploadedObject(
            object_key=str(payload.get("objectKey") or object_key),
            url=str(payload.get("url") or ""),
            size=int(payload.get("size") or path.stat().st_size),
            content_type=str(payload.get("contentType") or content_type),
            object_type=str(payload.get("objectType") or object_type),
        )

    def sync_content(self, payload: dict[str, Any]) -> dict[str, Any]:
        if not self.base_url:
            raise CollectorError(
                CollectorErrorCode.COLLECTOR_BUG,
                "collector backend base URL is not configured",
                retryable=False,
            )
        try:
            response = self._client.post(
                f"{self.base_url}/ingestion/collector/contents",
                headers={**self._headers(), "Content-Type": "application/json"},
                json=payload,
            )
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise self._map_backend_error(exc.response, "sync content") from exc
        except httpx.HTTPError as exc:
            raise CollectorError(
                CollectorErrorCode.NETWORK_TEMPORARY,
                f"collector backend content sync failed: {exc}",
                retryable=True,
                credential_action=CredentialAction.NONE,
            ) from exc
        return response.json()

    def download_file(self, url: str, destination: Path) -> Path:
        try:
            response = self._client.get(url)
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise self._map_backend_error(exc.response, "download thumbnail") from exc
        except httpx.HTTPError as exc:
            raise CollectorError(
                CollectorErrorCode.NETWORK_TEMPORARY,
                f"thumbnail download failed: {exc}",
                retryable=True,
                credential_action=CredentialAction.NONE,
            ) from exc
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(response.content)
        return destination

    def _headers(self) -> dict[str, str]:
        return {
            "accessKey": self.access_key,
            "accessToken": self.access_token,
        }

    def _map_backend_error(self, response: httpx.Response, action: str) -> CollectorError:
        retryable = response.status_code in {408, 429, 500, 502, 503, 504}
        code = CollectorErrorCode.NETWORK_TEMPORARY if retryable else CollectorErrorCode.ACCESS_DENIED
        message = response.text.strip() or response.reason_phrase
        return CollectorError(code, f"collector backend {action} failed: HTTP {response.status_code} {message}", retryable=retryable)


def sync_youtube_batch_to_backend(
    batch: CollectionBatch,
    request: CollectionRequest,
    *,
    client: CollectorBackendClient | None = None,
    settings: Settings | None = None,
) -> dict[str, Any]:
    return sync_batch_to_backend(batch, request, client=client, settings=settings)


def sync_batch_to_backend(
    batch: CollectionBatch,
    request: CollectionRequest,
    *,
    client: CollectorBackendClient | None = None,
    settings: Settings | None = None,
) -> dict[str, Any]:
    if request.platform not in {Platform.YOUTUBE, Platform.TIKTOK, Platform.FACEBOOK, Platform.INSTAGRAM}:
        return {"items": []}
    resolved_settings = settings or get_settings()
    backend = client or CollectorBackendClient.from_settings(resolved_settings)
    uploads = _upload_assets(batch, request, backend, resolved_settings)
    results = []
    for content in batch.contents:
        video = uploads.get((content.external_id, "VIDEO"))
        thumbnail = uploads.get((content.external_id, "THUMBNAIL"))
        if not video or not thumbnail:
            continue
        payload = _content_payload(batch, content, request, video=video, thumbnail=thumbnail)
        results.append(backend.sync_content(payload))
    batch.raw["collector_backend_sync"] = {"items": results}
    return {"items": results}


def _upload_assets(
    batch: CollectionBatch,
    request: CollectionRequest,
    client: CollectorBackendClient,
    settings: Settings,
) -> dict[tuple[str, str], UploadedObject]:
    uploaded: dict[tuple[str, str], UploadedObject] = {}
    for content in batch.contents:
        for object_type in ("VIDEO", "THUMBNAIL"):
            asset = _find_asset(batch.assets, content.external_id, object_type)
            if not asset:
                continue
            path = _asset_path(asset, content.external_id, request, client, settings)
            if not path:
                continue
            object_key = _object_key(request.job_id, content.external_id, object_type, path)
            result = client.upload_object(path, object_type=object_type, object_key=object_key)
            asset.object_key = result.object_key
            asset.url = result.url
            uploaded[(content.external_id, object_type)] = result
    return uploaded


def _find_asset(assets: list[MediaAsset], content_id: str, asset_type: str) -> MediaAsset | None:
    for asset in assets:
        if asset.content_external_id == content_id and asset.asset_type == asset_type:
            return asset
    return None


def _asset_path(
    asset: MediaAsset,
    content_id: str,
    request: CollectionRequest,
    client: CollectorBackendClient,
    settings: Settings,
) -> Path | None:
    if asset.local_path:
        path = Path(asset.local_path)
        if path.is_file():
            return path
    if asset.asset_type == "THUMBNAIL" and asset.url:
        extension = _extension_from_url(asset.url) or ".jpg"
        destination = _output_root(settings, request.platform) / request.job_id / content_id / f"{content_id}-thumbnail{extension}"
        path = client.download_file(asset.url, destination)
        asset.local_path = str(path)
        return path
    return None


def _object_key(job_id: str, content_id: str, object_type: str, path: Path) -> str:
    directory = "videos" if object_type == "VIDEO" else "thumbnails"
    suffix = path.suffix or (".mp4" if object_type == "VIDEO" else ".jpg")
    return f"ingestion/{job_id}/{directory}/{content_id}{suffix}"


def _content_payload(
    batch: CollectionBatch,
    content: CollectedContent,
    request: CollectionRequest,
    *,
    video: UploadedObject,
    thumbnail: UploadedObject,
) -> dict[str, Any]:
    account = next((item for item in batch.accounts if item.external_id == content.author_external_id), None)
    raw = content.raw or {}
    return {
        "platform": request.platform.value,
        "user": {
            "uid": account.username or account.external_id if account else content.author_external_id,
            "username": account.username if account else "",
            "nickname": account.nickname if account else "",
        },
        "contentType": "LIVE" if content.content_type == "LIVE" else "SHORT_VIDEO",
        "collectionType": "KEYWORD" if request.target.target_type == "KEYWORD" else "USERNAME",
        "videoId": content.external_id,
        "title": content.title,
        "description": content.description,
        "sourceUrl": content.url,
        "publishedAt": _format_backend_datetime(content.published_at),
        "durationSeconds": _optional_int(raw.get("duration")),
        "mediaSizeBytes": video.size,
        "viewCount": _optional_int(raw.get("view_count")),
        "likeCount": _optional_int(raw.get("like_count")),
        "commentCount": _optional_int(raw.get("comment_count")),
        "videoUrl": video.url or video.object_key,
        "thumbnailUrl": thumbnail.url or thumbnail.object_key,
        "collectedAt": _format_backend_datetime(content.collected_at),
        "comments": [
            {
                "commentId": comment.external_id,
                "author": str((comment.raw or {}).get("author") or comment.author_external_id),
                "content": comment.text,
                "likeCount": _optional_int((comment.raw or {}).get("like_count")),
                "publishedAt": _format_backend_datetime(comment.published_at),
            }
            for comment in batch.comments
            if comment.content_external_id == content.external_id
        ],
    }


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _format_backend_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is not None:
        value = value.astimezone().replace(tzinfo=None)
    return value.isoformat(timespec="seconds")


def _extension_from_url(url: str) -> str:
    suffix = Path(url.split("?", 1)[0]).suffix.lower()
    return suffix if suffix in {".jpg", ".jpeg", ".png", ".webp"} else ""


def _output_root(settings: Settings, platform: Platform) -> Path:
    if platform == Platform.INSTAGRAM:
        return settings.instagram_output_root
    if platform == Platform.TIKTOK:
        return settings.tiktok_output_root
    if platform == Platform.FACEBOOK:
        return settings.facebook_output_root
    return settings.youtube_output_root
