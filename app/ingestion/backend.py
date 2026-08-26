from __future__ import annotations

import mimetypes
import json
import shutil
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx

from app.core.config import Settings, get_settings
from app.domain.enums import Capability, CollectorErrorCode, CredentialAction, Platform
from app.domain.models import CollectionBatch, CollectionRequest, CollectedContent, CollectorError, MediaAsset
from app.ingestion.agent import AgentClient, build_agent_payload
from app.ingestion.runtime_cleanup import cleanup_related_media_files


@dataclass(frozen=True)
class ActiveKeyword:
    id: int
    platform: str
    keyword: str
    match_type: str = ""
    risk_level: str = ""


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
        self._client = http_client or httpx.Client(timeout=timeout_seconds, trust_env=False)
        self._external_client = http_client or httpx.Client(timeout=timeout_seconds)
        self.max_attempts = 3

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
        response = self._request_with_retry(
            "upload object",
            lambda: self._post_object(path, object_type=object_type, object_key=object_key, content_type=content_type),
        )
        payload = response.json()
        return UploadedObject(
            object_key=str(payload.get("objectKey") or object_key),
            url=str(payload.get("url") or ""),
            size=int(payload.get("size") or path.stat().st_size),
            content_type=str(payload.get("contentType") or content_type),
            object_type=str(payload.get("objectType") or object_type),
        )

    def _post_object(self, path: Path, *, object_type: str, object_key: str, content_type: str) -> httpx.Response:
        with path.open("rb") as file_obj:
            response = self._client.post(
                f"{self.base_url}/ingestion/collector/objects",
                headers=self._headers(),
                data={"objectType": object_type, "objectKey": object_key},
                files={"file": (path.name, file_obj, content_type)},
            )
        response.raise_for_status()
        return response

    def active_keywords(self, *, platforms: list[Platform | str] | None = None) -> list[ActiveKeyword]:
        if not self.base_url:
            raise CollectorError(
                CollectorErrorCode.COLLECTOR_BUG,
                "collector backend base URL is not configured",
                retryable=False,
            )
        platform_values = _platform_values_with_aliases(platforms or [])
        def send() -> httpx.Response:
            response = self._client.post(
                f"{self.base_url}/ingestion/collector/keywords/active",
                headers={**self._headers(), "Content-Type": "application/json"},
                json={"platforms": platform_values},
            )
            response.raise_for_status()
            return response

        response = self._request_with_retry("fetch active keywords", send)
        payload = response.json()
        items = payload.get("items") if isinstance(payload, dict) else []
        return [
            ActiveKeyword(
                id=int(item.get("id")),
                platform=str(item.get("platform") or ""),
                keyword=str(item.get("keyword") or ""),
                match_type=str(item.get("matchType") or item.get("match_type") or ""),
                risk_level=str(item.get("riskLevel") or item.get("risk_level") or ""),
            )
            for item in items or []
            if isinstance(item, dict) and item.get("id") is not None and str(item.get("keyword") or "").strip()
        ]

    def sync_content(self, payload: dict[str, Any]) -> dict[str, Any]:
        if not self.base_url:
            raise CollectorError(
                CollectorErrorCode.COLLECTOR_BUG,
                "collector backend base URL is not configured",
                retryable=False,
            )
        def send() -> httpx.Response:
            response = self._client.post(
                f"{self.base_url}/ingestion/collector/contents",
                headers={**self._headers(), "Content-Type": "application/json"},
                json=payload,
            )
            response.raise_for_status()
            return response

        response = self._request_with_retry("sync content", send)
        return response.json()

    def download_file(self, url: str, destination: Path) -> Path:
        def send() -> httpx.Response:
            response = self._external_client.get(url)
            response.raise_for_status()
            return response

        response = self._request_with_retry("download thumbnail", send)
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

    def _request_with_retry(self, action: str, send) -> httpx.Response:
        last_error: httpx.HTTPError | None = None
        for attempt in range(1, self.max_attempts + 1):
            try:
                return send()
            except httpx.HTTPStatusError as exc:
                error = self._map_backend_error(exc.response, action)
                if not error.retryable or attempt >= self.max_attempts:
                    raise error from exc
                time.sleep(0.5 * attempt)
            except httpx.HTTPError as exc:
                last_error = exc
                if attempt >= self.max_attempts:
                    raise CollectorError(
                        CollectorErrorCode.NETWORK_TEMPORARY,
                        f"collector backend {action} failed: {exc}",
                        retryable=True,
                        credential_action=CredentialAction.NONE,
                    ) from exc
                time.sleep(0.5 * attempt)
        raise CollectorError(
            CollectorErrorCode.NETWORK_TEMPORARY,
            f"collector backend {action} failed: {last_error}",
            retryable=True,
            credential_action=CredentialAction.NONE,
        )


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
    if request.platform not in {Platform.YOUTUBE, Platform.TIKTOK, Platform.FACEBOOK, Platform.INSTAGRAM, Platform.X}:
        return {"items": []}
    resolved_settings = settings or get_settings()
    backend = client or CollectorBackendClient.from_settings(resolved_settings)
    agent = AgentClient.from_settings(resolved_settings)
    uploads = _upload_assets(batch, request, backend, resolved_settings)
    results = []
    agent_items = []
    for content in batch.contents:
        video = uploads.get((content.external_id, "VIDEO"))
        thumbnail = uploads.get((content.external_id, "THUMBNAIL"))
        if not video or not thumbnail:
            continue
        payload = _content_payload(batch, content, request, video=video, thumbnail=thumbnail)
        sync_result = backend.sync_content(payload)
        results.append(sync_result)
        agent_payload = build_agent_payload(
            batch,
            content,
            request,
            video=video,
            thumbnail=thumbnail,
            strategy=getattr(resolved_settings, "agent_ingest_strategy", "rag_repair"),
        )
        agent_result = agent.ingest(agent_payload)
        agent_items.append(
            {
                "video_id": content.external_id,
                "data_source": agent_payload.get("data_source"),
                "status": agent_result.get("status"),
                "case_id": agent_result.get("case_id"),
                "report": agent_result.get("report"),
                "evaluation": agent_result.get("evaluation"),
                "trace": agent_result.get("trace"),
                "attempts": agent_result.get("attempts"),
                "retryable": agent_result.get("retryable"),
                "error": agent_result.get("error"),
                "raw": agent_result,
            }
        )
        _cleanup_synced_assets(batch, content.external_id)
    collector_sync = {"items": results}
    agent_ingest = {"enabled": agent.enabled, "items": agent_items}
    batch.raw["collector_backend_sync"] = collector_sync
    batch.raw["agent_ingest"] = agent_ingest
    return {"items": results, "agent_ingest": agent_ingest}


def _cleanup_synced_assets(batch: CollectionBatch, content_id: str) -> None:
    for asset in batch.assets:
        if asset.content_external_id != content_id:
            continue
        if asset.asset_type not in {"VIDEO", "THUMBNAIL", "MEDIA"}:
            continue
        if not asset.local_path:
            continue
        cleanup_related_media_files(asset.local_path)


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
            object_key = _object_key(request, content.external_id, object_type, path)
            result = client.upload_object(path, object_type=object_type, object_key=object_key)
            asset.object_key = result.object_key
            asset.url = result.url
            uploaded[(content.external_id, object_type)] = result
    return uploaded


def _find_asset(assets: list[MediaAsset], content_id: str, asset_type: str) -> MediaAsset | None:
    for asset in assets:
        if asset.content_external_id == content_id and asset.asset_type == asset_type:
            return asset
        if asset.content_external_id == content_id and asset_type == "VIDEO" and asset.asset_type == "MEDIA":
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
            if asset.asset_type in {"VIDEO", "MEDIA"}:
                return _backend_compatible_video_path(path)
            return path
    if asset.asset_type == "THUMBNAIL" and asset.url:
        extension = _extension_from_url(asset.url) or ".jpg"
        destination = _output_root(settings, request.platform) / request.job_id / content_id / f"{content_id}-thumbnail{extension}"
        path = client.download_file(asset.url, destination)
        asset.local_path = str(path)
        return path
    return None


def _object_key(request: CollectionRequest, content_id: str, object_type: str, path: Path) -> str:
    directory = "videos" if object_type == "VIDEO" else "thumbnails"
    suffix = path.suffix or (".mp4" if object_type == "VIDEO" else ".jpg")
    if request.target.target_type == "KEYWORD":
        keyword_id = _safe_object_key_component(str(request.target.metadata.get("keyword_id") or request.target.metadata.get("keywordId") or "unknown"))
        platform = _safe_object_key_component(request.platform.value.lower())
        return f"ingestion/{platform}-keyword/{keyword_id}/{directory}/{content_id}{suffix}"
    return f"ingestion/{request.job_id}/{directory}/{content_id}{suffix}"


def _backend_compatible_video_path(path: Path) -> Path:
    if _video_is_backend_compatible(path):
        return path
    ffmpeg = _ffmpeg_binary()
    if not ffmpeg:
        return path
    output = path.with_name(f"{path.stem}-collector-compatible.mp4")
    if output.is_file() and output.stat().st_mtime >= path.stat().st_mtime:
        return output
    command = _ffmpeg_compatible_command(ffmpeg, path, output, has_audio=_video_has_audio(path))
    try:
        subprocess.run(command, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=900)
    except (OSError, subprocess.SubprocessError):
        return path
    return output if output.is_file() and output.stat().st_size > 0 else path


def _ffmpeg_compatible_command(ffmpeg: str, source: Path, output: Path, *, has_audio: bool) -> list[str]:
    base = [ffmpeg, "-y", "-i", str(source)]
    if has_audio:
        return [
            *base,
            "-map",
            "0:v:0",
            "-map",
            "0:a:0",
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
            str(output),
        ]
    return [
        *base,
        "-f",
        "lavfi",
        "-i",
        "anullsrc=channel_layout=stereo:sample_rate=44100",
        "-map",
        "0:v:0",
        "-map",
        "1:a:0",
        "-shortest",
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
        str(output),
    ]


def _video_is_backend_compatible(path: Path) -> bool:
    streams = _video_streams(path)
    if streams is None:
        return path.suffix.lower() == ".mp4"
    has_video = any(item.get("codec_type") == "video" and item.get("codec_name") == "h264" for item in streams)
    has_audio = any(item.get("codec_type") == "audio" for item in streams)
    has_aac_audio = any(item.get("codec_type") == "audio" and item.get("codec_name") == "aac" for item in streams)
    return path.suffix.lower() == ".mp4" and has_video and has_audio and has_aac_audio


def _video_has_audio(path: Path) -> bool:
    streams = _video_streams(path)
    if streams is None:
        return True
    return any(item.get("codec_type") == "audio" for item in streams)


def _video_streams(path: Path) -> list[dict[str, Any]] | None:
    ffprobe = _ffprobe_binary()
    if not ffprobe:
        return None
    try:
        result = subprocess.run(
            [ffprobe, "-v", "error", "-show_streams", "-of", "json", str(path)],
            check=True,
            capture_output=True,
            text=True,
            timeout=60,
        )
        payload = json.loads(result.stdout or "{}")
    except (OSError, subprocess.SubprocessError, json.JSONDecodeError):
        return None
    streams = payload.get("streams") if isinstance(payload, dict) else None
    if not isinstance(streams, list):
        return None
    return [item for item in streams if isinstance(item, dict)]


def _ffmpeg_binary() -> str:
    return shutil.which("ffmpeg") or _runtime_ffmpeg_tool("ffmpeg")


def _ffprobe_binary() -> str:
    return shutil.which("ffprobe") or _runtime_ffmpeg_tool("ffprobe")


def _runtime_ffmpeg_tool(name: str) -> str:
    candidate = Path("data/runtime/ffmpeg-conda/bin") / name
    return str(candidate) if candidate.exists() else ""


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
    payload = {
        "platform": _backend_platform_value(request.platform),
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
        "videoUrl": video.object_key,
        "thumbnailUrl": thumbnail.object_key,
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
    if request.target.target_type == "KEYWORD":
        keyword_id = request.target.metadata.get("keyword_id") or request.target.metadata.get("keywordId")
        if keyword_id is not None:
            payload["keywordId"] = int(keyword_id)
        payload["keyword"] = str(request.target.metadata.get("keyword") or request.target.target_value)
    return payload


def _safe_object_key_component(value: str) -> str:
    cleaned = "".join(ch for ch in value.strip() if ch.isalnum() or ch in {"-", "_"})
    return cleaned or "unknown"


def _backend_platform_value(platform: Platform) -> str:
    if platform == Platform.X:
        return "TWITTER"
    return platform.value


def _platform_values_with_aliases(platforms: list[Platform | str]) -> list[str]:
    values: list[str] = []
    for platform in platforms:
        value = platform.value if isinstance(platform, Platform) else str(platform)
        normalized = value.strip().upper()
        if not normalized:
            continue
        for candidate in (normalized, *_platform_aliases(normalized)):
            if candidate not in values:
                values.append(candidate)
    return values


def _platform_aliases(value: str) -> tuple[str, ...]:
    if value == Platform.X.value:
        return ("TWITTER",)
    if value == "TWITTER":
        return (Platform.X.value,)
    return ()


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
    if platform == Platform.X:
        return settings.x_output_root
    return settings.youtube_output_root
