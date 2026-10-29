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
from app.ingestion.runtime_cleanup import cleanup_media_parent_dir, cleanup_related_media_files


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
    def from_settings(
        cls,
        settings: Settings | None = None,
        *,
        base_url: str | None = None,
        timeout_seconds: int | None = None,
    ) -> "CollectorBackendClient":
        resolved = settings or get_settings()
        return cls(
            base_url=base_url or resolved.collector_backend_base_url,
            access_key=resolved.collector_access_key,
            access_token=resolved.collector_access_token,
            timeout_seconds=timeout_seconds or resolved.collector_backend_timeout_seconds,
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

    def active_accounts(
        self,
        *,
        platforms: list[Platform | str] | None = None,
        account_types: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Return active monitoring accounts for accountId-only live starts.

        The business UI normally sends the saved homepage/handle as well as
        the numeric accountId.  Older UI builds sent only accountId, however,
        and a live room URL is intentionally ephemeral.  This small lookup
        lets the collector recover the persisted stable UID without requiring
        the operator to re-enter the account for every broadcast.
        """
        if not self.base_url:
            raise CollectorError(
                CollectorErrorCode.COLLECTOR_BUG,
                "collector backend base URL is not configured",
                retryable=False,
            )
        platform_values = _platform_values_with_aliases(platforms or [])

        def send() -> httpx.Response:
            response = self._client.post(
                f"{self.base_url}/ingestion/collector/accounts/active",
                headers={**self._headers(), "Content-Type": "application/json"},
                json={
                    "platforms": platform_values,
                    "accountTypes": list(account_types or ["LIVE", "BOTH"]),
                },
            )
            response.raise_for_status()
            return response

        response = self._request_with_retry("fetch active accounts", send)
        payload = response.json()
        items = payload.get("items") if isinstance(payload, dict) else []
        return [item for item in items or [] if isinstance(item, dict)]

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

    def upsert_live_stream(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._post_json("upsert live stream", "/ingestion/live-streams/upsert", payload)

    def live_heartbeat(self, stream_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        return self._post_json("live heartbeat", f"/ingestion/live-streams/{stream_id}/heartbeat", payload)

    def live_segments(self, stream_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        return self._post_json("live segments", f"/ingestion/live-streams/{stream_id}/segments", payload)

    def live_messages(self, stream_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        return self._post_json("live messages", f"/ingestion/live-streams/{stream_id}/messages/batch", payload)

    def live_metrics(self, stream_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        return self._post_json("live metrics", f"/ingestion/live-streams/{stream_id}/metrics", payload)

    def live_ended(self, stream_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        return self._post_json("live ended", f"/ingestion/live-streams/{stream_id}/ended", payload)

    def collector_task_status(self, task_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        """Report the collector task lifecycle to the business backend.

        Live-stream callbacks describe a registered stream, while this
        endpoint describes the upstream account task itself.  Both are needed
        so a task that fails before stream registration does not remain stuck
        in the business UI as ``接入中``.
        """
        from urllib.parse import quote

        return self._post_json(
            "collector task status",
            f"/ingestion/collector/tasks/{quote(str(task_id), safe='')}/status",
            payload,
        )

    def _post_json(self, action: str, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        def send() -> httpx.Response:
            response = self._client.post(
                f"{self.base_url}{path}",
                headers={**self._headers(), "Content-Type": "application/json"},
                json=payload,
            )
            response.raise_for_status()
            return response

        response = self._request_with_retry(action, send)
        # Heartbeat endpoints commonly acknowledge with 204/empty bodies.
        # HTTP success is still a real ACK; do not turn it into a JSON decode
        # failure and falsely mark the collector as disconnected.
        if not response.content:
            return {}
        try:
            payload = response.json()
        except ValueError:
            return {}
        return payload if isinstance(payload, dict) else {}

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
    backend = client or CollectorBackendClient.from_settings(resolved_settings, base_url=_callback_base_url(request) or None)
    agent = AgentClient.from_settings(resolved_settings)
    results = []
    media_results = []
    metadata_only_results = []
    unavailable_results = []
    agent_items = []
    for content in batch.contents:
        # A card without a playable file is unusable in the monitoring UI and
        # renders as a black player.  When metadata-only sync is disabled,
        # decide this before fetching/uploading a thumbnail so failed media
        # collection neither creates an empty content row nor holds a worker
        # on an unnecessary remote thumbnail request.
        if not getattr(resolved_settings, "collector_metadata_only_sync_enabled", False):
            source_video = _find_asset(batch.assets, content.external_id, "VIDEO")
            if not source_video or not source_video.local_path or not Path(source_video.local_path).is_file():
                continue
        uploads = _upload_content_assets(batch, request, backend, resolved_settings, content.external_id)
        video = uploads.get((content.external_id, "VIDEO"))
        thumbnail = uploads.get((content.external_id, "THUMBNAIL"))
        if not video or not thumbnail:
            # A missing local media file must not erase an otherwise valid
            # discovery result.  Newer business backends accept a metadata
            # upsert with nullable object keys; preserve compatibility with
            # older deployments through an explicit rollout switch.
            if not getattr(resolved_settings, "collector_metadata_only_sync_enabled", False):
                continue
            media_reason = _media_unavailable_reason(batch, content)
            payload = _content_payload(
                batch,
                content,
                request,
                video=video,
                thumbnail=thumbnail,
                media_status="UNAVAILABLE",
                media_failure_reason=media_reason,
            )
            sync_result = backend.sync_content(payload)
            result_item = {**sync_result, "mediaStatus": "UNAVAILABLE", "mediaFailureReason": media_reason}
            results.append(result_item)
            metadata_only_results.append(result_item)
            unavailable_results.append(result_item)
            continue
        payload = _content_payload(batch, content, request, video=video, thumbnail=thumbnail, media_status="READY")
        sync_result = backend.sync_content(payload)
        result_item = {**sync_result, "mediaStatus": "READY"}
        results.append(result_item)
        media_results.append(result_item)
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
    collector_sync = {
        "items": results,
        "metadataSynced": len(results),
        "mediaSynced": len(media_results),
        "mediaUnavailable": len(unavailable_results),
        "metadataOnlyItems": metadata_only_results,
    }
    agent_ingest = {"enabled": agent.enabled, "items": agent_items}
    batch.raw["collector_backend_sync"] = collector_sync
    batch.raw["agent_ingest"] = agent_ingest
    return {**collector_sync, "agent_ingest": agent_ingest}


def _cleanup_synced_assets(batch: CollectionBatch, content_id: str) -> None:
    for asset in batch.assets:
        if asset.content_external_id != content_id:
            continue
        if asset.asset_type not in {"VIDEO", "THUMBNAIL", "MEDIA"}:
            continue
        if not asset.local_path:
            continue
        cleanup_related_media_files(asset.local_path)
        cleanup_media_parent_dir(asset.local_path)


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


def _upload_content_assets(
    batch: CollectionBatch,
    request: CollectionRequest,
    client: CollectorBackendClient,
    settings: Settings,
    content_id: str,
) -> dict[tuple[str, str], UploadedObject]:
    uploaded: dict[tuple[str, str], UploadedObject] = {}
    for object_type in ("VIDEO", "THUMBNAIL"):
        asset = _find_asset(batch.assets, content_id, object_type)
        if not asset:
            continue
        path = None
        if object_type == "THUMBNAIL" and not asset.local_path:
            video_asset = _find_asset(batch.assets, content_id, "VIDEO")
            video_path = Path(video_asset.local_path) if video_asset and video_asset.local_path else None
            if video_path and video_path.is_file():
                path = _generate_thumbnail_from_video(
                    video_path,
                    _output_root(settings, request.platform) / request.job_id / content_id / f"{content_id}-thumbnail.jpg",
                )
                if path:
                    asset.local_path = str(path)
        if not path:
            path = _asset_path(asset, content_id, request, client, settings)
        if not path:
            continue
        object_key = _object_key(request, content_id, object_type, path)
        result = client.upload_object(path, object_type=object_type, object_key=object_key)
        asset.object_key = result.object_key
        asset.url = result.url
        uploaded[(content_id, object_type)] = result
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
                return _backend_compatible_video_path(
                    path,
                    allow_web_native_codecs=_allow_facebook_keyword_fast_media(request, settings),
                )
            return path
    if asset.asset_type == "THUMBNAIL" and asset.url:
        extension = _extension_from_url(asset.url) or ".jpg"
        destination = _output_root(settings, request.platform) / request.job_id / content_id / f"{content_id}-thumbnail{extension}"
        path = client.download_file(asset.url, destination)
        asset.local_path = str(path)
        return path
    return None


def _generate_thumbnail_from_video(video_path: Path, destination: Path) -> Path | None:
    ffmpeg = _ffmpeg_binary()
    if not ffmpeg:
        return None
    destination.parent.mkdir(parents=True, exist_ok=True)
    command = [ffmpeg, "-y", "-ss", "1", "-i", str(video_path), "-frames:v", "1", "-q:v", "3", str(destination)]
    try:
        subprocess.run(command, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=60)
    except (OSError, subprocess.SubprocessError):
        return None
    return destination if destination.is_file() and destination.stat().st_size > 0 else None


def _object_key(request: CollectionRequest, content_id: str, object_type: str, path: Path) -> str:
    directory = "videos" if object_type == "VIDEO" else "thumbnails"
    suffix = path.suffix or (".mp4" if object_type == "VIDEO" else ".jpg")
    if _collection_type(request) == "HOME_FEED":
        batch_id = _safe_object_key_component(str(_home_feed_batch_id(request) or request.job_id))
        platform = _safe_object_key_component(request.platform.value)
        return f"ingestion/{batch_id}/{platform}/{directory}/{content_id}{suffix}"
    if request.target.target_type == "KEYWORD":
        keyword_id = _safe_object_key_component(str(request.target.metadata.get("keyword_id") or request.target.metadata.get("keywordId") or "unknown"))
        platform = _safe_object_key_component(request.platform.value.lower())
        return f"ingestion/{platform}-keyword/{keyword_id}/{directory}/{content_id}{suffix}"
    return f"ingestion/{request.job_id}/{directory}/{content_id}{suffix}"


def _allow_facebook_keyword_fast_media(request: CollectionRequest, settings: Settings) -> bool:
    """Allow browser-native MP4 codecs only for the explicit FB keyword mode.

    This does not weaken the normal account, live, or other-platform media
    contract.  It removes the avoidable per-item H.264 transcode in the one
    high-throughput workflow where modern Chromium playback is the target.
    """
    return bool(
        getattr(settings, "collector_facebook_keyword_fast_media_enabled", False)
        and request.platform == Platform.FACEBOOK
        and request.target.target_type == "KEYWORD"
    )


def _backend_compatible_video_path(path: Path, *, allow_web_native_codecs: bool = False) -> Path:
    if _video_is_backend_compatible(path, allow_web_native_codecs=allow_web_native_codecs):
        return path
    ffmpeg = _ffmpeg_binary()
    if not ffmpeg:
        return path
    output = path.with_name(f"{path.stem}-collector-compatible.mp4")
    if output.is_file() and output.stat().st_mtime >= path.stat().st_mtime:
        return output
    command = _ffmpeg_compatible_command(ffmpeg, path, output, has_audio=_video_has_audio(path))
    try:
        subprocess.run(command, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=180)
    except (OSError, subprocess.SubprocessError):
        return path
    return output if output.is_file() and output.stat().st_size > 0 else path


def _ffmpeg_compatible_command(ffmpeg: str, source: Path, output: Path, *, has_audio: bool) -> list[str]:
    base = [ffmpeg, "-y", "-i", str(source)]
    video_options = [
        "-vf",
        "scale='if(gt(ih,720),-2,iw)':'if(gt(ih,720),720,ih)'",
        "-c:v",
        "libx264",
        "-preset",
        "veryfast",
        "-crf",
        "28",
    ]
    if has_audio:
        return [
            *base,
            "-map",
            "0:v:0",
            "-map",
            "0:a:0",
            *video_options,
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
        *video_options,
        "-c:a",
        "aac",
        "-b:a",
        "96k",
        "-movflags",
        "+faststart",
        str(output),
    ]


def _video_is_backend_compatible(path: Path, *, allow_web_native_codecs: bool = False) -> bool:
    streams = _video_streams(path)
    if streams is None:
        return path.suffix.lower() == ".mp4"
    allowed_video_codecs = {"h264"}
    if allow_web_native_codecs:
        # Chromium can natively decode AV1/VP9 in MP4.  Keep the scope above
        # narrow because Safari/legacy clients still need the H.264 fallback.
        allowed_video_codecs.update({"av1", "vp9"})
    has_video = any(
        item.get("codec_type") == "video" and item.get("codec_name") in allowed_video_codecs
        for item in streams
    )
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



def _content_user_payload(account, content: CollectedContent, request: CollectionRequest) -> dict[str, str]:
    # Backend validation expects a stable platform UID. Handles can be
    # renamed and (for browser fallback) are not accepted as YouTube UIDs.
    fallback_uid = (account.external_id or account.username) if account else content.author_external_id
    username = account.username if account else ""
    nickname = account.nickname if account else ""
    preferred = _account_display_name_from_request(request)
    return {
        "uid": fallback_uid,
        "username": username,
        "nickname": preferred or nickname,
    }


def _account_display_name_from_request(request: CollectionRequest) -> str:
    if request.target.target_type != "ACCOUNT":
        return ""
    metadata = request.target.metadata or {}
    value = metadata.get("account_display_name") or metadata.get("accountDisplayName") or metadata.get("display_name") or metadata.get("displayName")
    return str(value or "").strip()

def _content_payload(
    batch: CollectionBatch,
    content: CollectedContent,
    request: CollectionRequest,
    *,
    video: UploadedObject | None,
    thumbnail: UploadedObject | None,
    media_status: str = "READY",
    media_failure_reason: str = "",
) -> dict[str, Any]:
    account = next((item for item in batch.accounts if item.external_id == content.author_external_id), None)
    raw = content.raw or {}
    payload = {
        "platform": _backend_platform_value(request.platform),
        "user": _content_user_payload(account, content, request),
        "contentType": _backend_content_type(content, request),
        "collectionType": _collection_type(request),
        "videoId": content.external_id,
        "title": content.title,
        "description": content.description,
        "sourceUrl": content.url,
        "publishedAt": _format_backend_datetime(content.published_at),
        "durationSeconds": _optional_int(raw.get("duration")),
        "mediaSizeBytes": video.size if video else None,
        "viewCount": _optional_int(raw.get("view_count")),
        "likeCount": _optional_int(raw.get("like_count")),
        "commentCount": _optional_int(raw.get("comment_count")),
        "videoUrl": video.object_key if video else None,
        "thumbnailUrl": thumbnail.object_key if thumbnail else None,
        "mediaStatus": media_status,
        "mediaFailureReason": media_failure_reason or None,
        "collectedAt": _format_backend_datetime(content.collected_at),
        "comments": [
            {
                "commentId": comment.external_id,
                "author": str((comment.raw or {}).get("author") or comment.author_external_id),
                # `user` above is the video author.  Comment identity must be
                # carried on the individual comment, otherwise a discovered
                # account cannot be collected again from its real profile.
                "authorId": str(comment.author_external_id or ""),
                "authorUrl": str(
                    comment.author_profile_url
                    or (comment.raw or {}).get("author_url")
                    or (comment.raw or {}).get("profile_url")
                    or ""
                ),
                "content": comment.text,
                "likeCount": _optional_int((comment.raw or {}).get("like_count")),
                "publishedAt": _format_backend_datetime(comment.published_at),
            }
            for comment in batch.comments
            if comment.content_external_id == content.external_id
        ],
    }
    collection_type = _collection_type(request)
    if request.target.target_type == "KEYWORD" and collection_type == "KEYWORD":
        keyword_id = request.target.metadata.get("keyword_id") or request.target.metadata.get("keywordId")
        if keyword_id is not None:
            payload["keywordId"] = int(keyword_id)
        payload["keyword"] = str(request.target.metadata.get("keyword") or request.target.target_value)
    if collection_type == "HOME_FEED":
        payload["homeFeedBatchId"] = _home_feed_batch_id(request)
        payload["loginAccountId"] = _login_account_id(request)
        payload["recommendationRank"] = _recommendation_rank(request, content.external_id)
    if collection_type == "LIVE" or request.capability == Capability.LIVE_RESOLVE:
        metadata = request.target.metadata or {}
        payload["liveUrl"] = str(metadata.get("liveUrl") or request.target.source_url or request.target.target_value)
        payload["liveClipSeconds"] = _optional_int(metadata.get("liveClipSeconds") or metadata.get("youtube_live_clip_seconds"))
    return payload


def _media_unavailable_reason(batch: CollectionBatch, content: CollectedContent) -> str:
    """Return a useful per-content reason for metadata-only upserts."""
    raw = content.raw or {}
    for value in (
        raw.get("media_error"),
        raw.get("mediaError"),
        batch.raw.get("media_error") if batch.raw else None,
        batch.raw.get("mediaError") if batch.raw else None,
    ):
        text = str(value or "").strip()
        if text:
            return text[:1000]
    return "media asset not produced by collector"


def _backend_content_type(content: CollectedContent, request: CollectionRequest) -> str:
    metadata = request.target.metadata or {}
    explicit = str(metadata.get("backendContentType") or metadata.get("backend_content_type") or "").strip().upper()
    if explicit:
        return explicit
    if request.capability == Capability.LIVE_RESOLVE or str(content.content_type or "").strip().upper() == "LIVE":
        return "LIVE"
    return "SHORT_VIDEO"


def _collection_type(request: CollectionRequest) -> str:
    metadata = request.target.metadata or {}
    value = str(
        metadata.get("backendCollectionType")
        or metadata.get("backend_collection_type")
        or metadata.get("collectionType")
        or metadata.get("collection_type")
        or metadata.get("mode")
        or ""
    ).strip().upper()
    if value == "HOME_FEED":
        return "HOME_FEED"
    if value in {"USERNAME", "ACCOUNT"}:
        return "USERNAME"
    if value == "LIVE":
        return "USERNAME"
    return "KEYWORD" if request.target.target_type == "KEYWORD" else "USERNAME"


def _home_feed_batch_id(request: CollectionRequest) -> str:
    metadata = request.target.metadata or {}
    return str(metadata.get("homeFeedBatchId") or metadata.get("home_feed_batch_id") or metadata.get("taskId") or request.job_id)


def _login_account_id(request: CollectionRequest) -> str:
    metadata = request.target.metadata or {}
    return str(metadata.get("loginAccountId") or metadata.get("login_account_id") or "")


def _recommendation_rank(request: CollectionRequest, content_id: str) -> int | None:
    metadata = request.target.metadata or {}
    ranks = metadata.get("home_feed_ranks") or metadata.get("homeFeedRanks")
    if isinstance(ranks, dict):
        value = ranks.get(content_id)
        if value is not None:
            return _optional_int(value)
    return _optional_int(metadata.get("recommendationRank") or metadata.get("recommendation_rank"))


def _callback_base_url(request: CollectionRequest) -> str:
    metadata = request.target.metadata or {}
    return str(metadata.get("callbackBaseUrl") or metadata.get("callback_base_url") or "").strip()


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
