from __future__ import annotations

import hashlib
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from zoneinfo import ZoneInfo

from app.core.config import Settings, get_settings
from app.domain.enums import Platform
from app.domain.models import CollectionBatch, CollectionRequest, CollectedComment
from app.ingestion.backend import CollectorBackendClient


_ENDED_LIVE_STATUSES = {
    "ended", "end", "offline", "not_live", "not live", "post_live", "was_live",
    "replay", "complete", "completed", "finished", "unavailable",
}
_BACKEND_TIMEZONE = ZoneInfo("Asia/Shanghai")


def live_end_reason(batch: CollectionBatch) -> str | None:
    """Return a terminal platform signal without mistaking a transient error for an end.

    Adapters preserve the platform's explicit `is_live`/`live_status` fields.
    The coordinator must inspect them *before* it supplies legacy LIVE
    defaults, otherwise a finished room remains falsely marked RUNNING.
    """
    if not batch.contents:
        if batch.raw.get("is_live") is False:
            return "PLATFORM_REPORTED_OFFLINE"
        return None
    content = batch.contents[0]
    raw = content.raw or {}
    if raw.get("is_live") is False or batch.raw.get("is_live") is False:
        return "PLATFORM_REPORTED_OFFLINE"
    values = (raw.get("live_status"), raw.get("status"), raw.get("availability"), batch.raw.get("live_status"))
    for value in values:
        normalized = str(value or "").strip().lower().replace("-", "_")
        if normalized in _ENDED_LIVE_STATUSES or normalized.replace("_", " ") in _ENDED_LIVE_STATUSES:
            return f"PLATFORM_REPORTED_{normalized.upper()}"
    return None


def live_error_indicates_end(error: Exception) -> bool:
    text = str(error).lower()
    terminal_signals = (
        "live has ended", "stream has ended", "not currently live", "is not live",
        "live is over", "broadcast has ended", "offline", "room has ended",
    )
    return any(signal in text for signal in terminal_signals)


def run_realtime_live_push(
    batch: CollectionBatch,
    request: CollectionRequest,
    *,
    backend: CollectorBackendClient | None = None,
    settings: Settings | None = None,
    progress: Any | None = None,
    on_stream_registered: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    """Register a live session and push bounded rolling clips to the business backend.

    The media adapter remains responsible for obtaining the platform-authorized source.
    This module only normalizes the callback contract and uploads already collected files.
    """
    resolved = settings or get_settings()
    client = backend
    if client is None:
        media_timeout = max(
            10,
            min(int(getattr(resolved, "collector_media_timeout_seconds", 30) or 30), 60),
        )
        client = CollectorBackendClient.from_settings(resolved, timeout_seconds=media_timeout)
        # The live coordinator owns retries and must not wait for the generic
        # three-attempt batch retry before its next heartbeat/segment cycle.
        client.max_attempts = 1
    metadata = request.target.metadata or {}
    content = batch.contents[0] if batch.contents else None
    if content is None:
        raise ValueError("live batch has no content")

    now = _iso_now()
    account_id = _int_or_none(metadata.get("accountId") or metadata.get("account_id"))
    if not account_id:
        raise ValueError("live monitoring accountId is required for downstream stream registration")
    # Platform pages (notably TikTok) are HTML and cannot be fed to a video
    # element. Adapters expose the resolved HLS/DASH URL in raw for playback.
    # Uploaded MP4/TS files are analysis artifacts. They are finite clips and
    # must never be advertised as the live player source. Facebook only gets a
    # continuous player URL when the adapter supplied HLS; otherwise the
    # original Facebook URL is exposed as an official embed fallback.
    playback_hls_url = str(metadata.get("playbackHlsUrl") or metadata.get("playback_hls_url") or "").strip()
    playback_ready = bool(metadata.get("playbackReady")) if playback_hls_url else bool(batch.assets)
    source_url = playback_hls_url or str(content.raw.get("live_stream_url") or metadata.get("liveStreamUrl") or metadata.get("liveUrl") or content.url or request.target.source_url or request.target.target_value)
    sequence_start = int(metadata.get("liveSequenceStart") or 0)
    preuploaded: dict[int, tuple[Any, int]] = {}
    latest_video_url = ""
    if request.platform == Platform.FACEBOOK and batch.assets:
        pre_sequence = sequence_start
        for index, asset in enumerate(batch.assets):
            if asset.asset_type not in {"VIDEO", "MEDIA", "AUDIO"} or not asset.local_path:
                continue
            path = Path(asset.local_path)
            if not path.is_file() or path.stat().st_size <= 0:
                continue
            pre_sequence += 1
            object_type = "AUDIO" if asset.asset_type == "AUDIO" else "VIDEO"
            object_key = f"live/OVERSEAS/{request.platform.value}/{content.external_id}/{datetime.now(timezone.utc):%Y%m%d}/{object_type.lower()}/{pre_sequence}{path.suffix or '.mp4'}"
            uploaded = client.upload_object(path, object_type=object_type, object_key=object_key)
            preuploaded[index] = (uploaded, pre_sequence)
            if object_type == "VIDEO" and uploaded.url:
                latest_video_url = uploaded.url
    if latest_video_url and request.platform != Platform.FACEBOOK:
        source_url = latest_video_url
    is_official_embed = (request.platform == Platform.YOUTUBE and not playback_hls_url) or (
        request.platform == Platform.FACEBOOK and not playback_hls_url
    )
    playback_mode = "OFFICIAL_EMBED" if is_official_embed else ("HLS" if playback_hls_url or content.raw.get("live_stream_url") else "SOURCE_URL")
    # ``platform=YOUTUBE`` must not make a client render a collector HLS URL
    # in a YouTube iframe. This explicit media contract is required by the
    # frontend to select hls.js/native HLS for an HLS-backed live stream.
    player_type = "HLS" if playback_mode == "HLS" else "YOUTUBE_IFRAME" if request.platform == Platform.YOUTUBE else "FACEBOOK_IFRAME" if request.platform == Platform.FACEBOOK else "URL"
    browser_playback_url = source_url
    if is_official_embed:
        # Never let the latest uploaded Facebook clip replace the page/video
        # URL used by the official player.
        browser_playback_url = str(
            content.url
            or request.target.source_url
            or request.target.target_value
        ).strip()
        source_url = browser_playback_url
    # Registration is control-plane traffic. Never let the generic media
    # upload timeout (often 300s) block the first heartbeat for a live task.
    registration_client = client
    if backend is None:
        registration_timeout = max(
            3,
            min(int(getattr(resolved, "collector_heartbeat_timeout_seconds", 10) or 10), 15),
        )
        registration_client = CollectorBackendClient.from_settings(
            resolved,
            timeout_seconds=registration_timeout,
        )
        registration_client.max_attempts = 1
    upsert = registration_client.upsert_live_stream(
        {
            "taskId": request.job_id,
            "platform": request.platform.value,
            "accountId": account_id,
            "externalLiveId": content.external_id,
            "roomId": content.external_id,
            "title": content.title,
            "sourceUrl": browser_playback_url,
            # Some downstream versions expose ``playUrl`` separately from
            # ``sourceUrl``. Send both so a rolling HLS source cannot be
            # replaced by the latest finite MP4 object URL.
            "playUrl": browser_playback_url,
            "mediaUrl": latest_video_url or browser_playback_url,
            "videoUrl": browser_playback_url,
            "hlsUrl": playback_hls_url,
            "embedId": content.external_id if is_official_embed else "",
            "playbackMode": playback_mode,
            "playerType": player_type,
            "status": "LIVE",
            "mediaAvailable": bool(batch.assets) or playback_ready,
            "playbackReady": playback_ready,
            "audioAvailable": bool(batch.assets),
            "chatAvailable": bool(request.collect_comments),
            "metricsAvailable": True,
            "accessLevel": str(metadata.get("accessLevel") or "A"),
            "sourceProvider": str(metadata.get("sourceProvider") or "COLLECTOR"),
            "startedAt": _content_time(content.raw.get("release_timestamp") or content.published_at) or now,
            "discoveredAt": now,
        }
    )
    stream_id = str(upsert.get("streamId") or upsert.get("id") or upsert.get("data", {}).get("streamId") or "")
    if not stream_id:
        raise RuntimeError(f"live stream upsert returned no streamId: {upsert}")
    # The stream id is the earliest point at which a heartbeat can be sent.
    # Start it before any potentially slow object upload or segment callback.
    if on_stream_registered:
        on_stream_registered(stream_id)

    uploaded_segments = 0
    uploaded_messages = 0
    latest_uploaded_video_url = ""
    sequence = sequence_start
    offset_start = int(metadata.get("liveOffsetStart") or 0)
    for index, asset in enumerate(batch.assets):
        if asset.asset_type not in {"VIDEO", "MEDIA", "AUDIO"} or not asset.local_path:
            continue
        path = Path(asset.local_path)
        if not path.is_file() or path.stat().st_size <= 0:
            continue
        sequence += 1
        object_type = "AUDIO" if asset.asset_type == "AUDIO" else "VIDEO"
        object_key = f"live/OVERSEAS/{request.platform.value}/{content.external_id}/{datetime.now(timezone.utc):%Y%m%d}/{object_type.lower()}/{sequence}{path.suffix or '.mp4'}"
        preloaded = preuploaded.get(index)
        if preloaded:
            uploaded, preloaded_sequence = preloaded
            # Keep the segment sequence identical to the object key reserved
            # before registration, even when the asset list contains skipped
            # entries.
            sequence = preloaded_sequence
            object_key = uploaded.object_key
        else:
            uploaded = client.upload_object(path, object_type=object_type, object_key=object_key)
        object_ref = {
            "objectKey": uploaded.object_key,
            "url": uploaded.url,
            "sizeBytes": uploaded.size,
            "sha256": _sha256(path),
            "contentType": uploaded.content_type,
            "durationMs": _int_or_none(content.raw.get("live_clip_seconds"), default=0) * 1000,
        }
        # A live analysis clip is an archive artifact, never the browser
        # source.  Some downstream display code still uses videoObject.url
        # despite playbackMode. For HLS sessions expose the stable playlist
        # there as well, while preserving the uploaded MP4/TS object under an
        # explicit analysis field (and its original objectKey) for analysis.
        analysis_object_ref = dict(object_ref)
        playback_object_ref = dict(object_ref)
        if playback_hls_url and object_type == "VIDEO":
            playback_object_ref.update(
                {
                    "url": playback_hls_url,
                    "contentType": "application/vnd.apple.mpegurl",
                    "playbackMode": "HLS",
                    "analysisUrl": uploaded.url,
                }
            )
        if object_type == "VIDEO" and uploaded.url:
            latest_uploaded_video_url = uploaded.url
        payload = {
            "sequenceNo": sequence,
            "sourceStartedAt": now,
            "sourceEndedAt": _iso_now(),
            "startOffsetMs": offset_start + max(0, sequence - sequence_start - 1) * int(content.raw.get("live_clip_seconds") or 0) * 1000,
            "endOffsetMs": offset_start + (sequence - sequence_start) * int(content.raw.get("live_clip_seconds") or 0) * 1000,
            "durationMs": object_ref["durationMs"],
            "audioObject": object_ref if object_type == "AUDIO" else None,
            "videoObject": playback_object_ref if object_type == "VIDEO" else None,
            "analysisVideoObject": analysis_object_ref if object_type == "VIDEO" else None,
            "frames": [],
            "timestampDiscontinuity": False,
            # Keep the browser playback source independent from the uploaded
            # processing artifact. Downstream must not promote videoObject.url
            # to playUrl when this stream is HLS-backed.
            "playUrl": playback_hls_url or (browser_playback_url if is_official_embed else uploaded.url),
            "playbackMode": "HLS" if playback_hls_url else playback_mode,
            "playerType": "HLS" if playback_hls_url else player_type,
            "playbackReady": playback_ready,
            "sourceUrl": playback_hls_url or (browser_playback_url if is_official_embed else uploaded.url),
            "mediaUrl": uploaded.url,
            "videoUrl": playback_hls_url or (browser_playback_url if is_official_embed else uploaded.url),
            "hlsUrl": playback_hls_url,
        }
        client.live_segments(stream_id, payload)
        uploaded_segments += 1
        if progress:
            progress({"stage": "SEGMENT_UPLOADED", "streamId": stream_id, "sequenceNo": sequence})

    # When a local rolling HLS relay is available it is the browser playback
    # source. Keep the MP4 object upload for downstream processing, but do not
    # overwrite the stable playlist with the last finite segment URL.
    if request.platform == Platform.TIKTOK and latest_uploaded_video_url and not playback_hls_url:
        try:
            client.upsert_live_stream(
                {
                    "taskId": request.job_id,
                    "platform": request.platform.value,
                    "accountId": account_id,
                    "externalLiveId": content.external_id,
                    "roomId": content.external_id,
                    "title": content.title,
                    "sourceUrl": latest_uploaded_video_url,
                    "playbackMode": "SOURCE_URL",
                    "status": "LIVE",
                    "mediaAvailable": True,
                    "audioAvailable": True,
                    "chatAvailable": bool(request.collect_comments),
                    "metricsAvailable": True,
                    "sourceProvider": "COLLECTOR_OBJECT",
                    "discoveredAt": _iso_now(),
                }
            )
        except Exception as exc:  # noqa: BLE001 - playback promotion must not stop capture.
            if progress:
                progress({"stage": "PLAYBACK_PROMOTION_FAILED", "streamId": stream_id, "error": str(exc)[:300]})

    comments = _comments_for_content(batch, content.external_id)
    if comments and request.collect_comments:
        client.live_messages(
            stream_id,
            {
                "batchId": f"{content.external_id}-chat-{sequence:06d}",
                "items": [_comment_payload(comment) for comment in comments],
            },
        )
        uploaded_messages = len(comments)

    client.live_metrics(
        stream_id,
        {
            "viewerCount": _int_or_none(
                content.raw.get("concurrent_view_count")
                or content.raw.get("concurrentViewers")
                or content.raw.get("live_viewers")
            ),
            "likeCount": _int_or_none(content.raw.get("like_count")),
            "messageCount": uploaded_messages,
            "streamStatus": "LIVE",
            "collectedAt": _iso_now(),
        },
    )
    client.live_heartbeat(
        stream_id,
        {
            "taskId": request.job_id,
            "collectorNode": resolved.worker_id,
            "liveStatus": "LIVE",
            "mediaStatus": "RUNNING" if uploaded_segments else "UNAVAILABLE",
            "chatStatus": "RUNNING" if request.collect_comments else "UNAVAILABLE",
            "metricsStatus": "RUNNING",
            # Reassert the stable playback source on every media heartbeat;
            # some downstream versions update the stream row from heartbeat
            # data and otherwise promote the most recent 15s MP4 artifact.
            "playUrl": playback_hls_url or browser_playback_url,
            "playbackMode": "HLS" if playback_hls_url else playback_mode,
            "playerType": "HLS" if playback_hls_url else player_type,
            "playbackReady": playback_ready,
            "sourceUrl": playback_hls_url or browser_playback_url,
            "mediaUrl": latest_uploaded_video_url or browser_playback_url,
            "videoUrl": playback_hls_url or browser_playback_url,
            "hlsUrl": playback_hls_url,
            "lastMediaAt": _iso_now() if uploaded_segments else None,
            "lastMessageAt": _iso_now() if uploaded_messages else None,
            "outboxPending": 0,
            "diskSpoolBytes": 0,
            "sentAt": _iso_now(),
        },
    )
    return {
        "streamId": stream_id,
        "externalLiveId": content.external_id,
        "segmentsUploaded": uploaded_segments,
        "messagesUploaded": uploaded_messages,
        "playUrl": playback_hls_url or browser_playback_url,
        "playbackMode": "HLS" if playback_hls_url else playback_mode,
        "playerType": "HLS" if playback_hls_url else player_type,
        "playbackReady": playback_ready,
        "upsert": upsert,
    }


def finish_realtime_live_push(
    stream_id: str,
    request: CollectionRequest,
    *,
    segment_count: int,
    message_count: int,
    backend: CollectorBackendClient | None = None,
    settings: Settings | None = None,
    reason: str = "COLLECTOR_STOPPED",
    detection_source: str = "collector",
) -> dict[str, Any]:
    client = backend or CollectorBackendClient.from_settings(settings or get_settings())
    ended_at = _iso_now()
    return client.live_ended(
        stream_id,
        {
            "reason": reason,
            # These explicit fields allow downstream state machines to move
            # directly to ENDED instead of waiting for heartbeat expiry.
            "status": "ENDED",
            "liveStatus": "ENDED",
            "collectorStatus": "ENDED",
            "endDetected": True,
            "detectionSource": detection_source,
            "endedAt": ended_at,
            "endDetectedAt": ended_at,
            "lastSequenceNo": segment_count,
            "segmentCount": segment_count,
            "messageCount": message_count,
            "hasMediaGap": False,
        },
    )


def _comments_for_content(batch: CollectionBatch, content_id: str) -> list[CollectedComment]:
    return [item for item in batch.comments if item.content_external_id == content_id]


def _comment_payload(comment: CollectedComment) -> dict[str, Any]:
    return {
        "externalMessageId": comment.external_id,
        "messageType": "COMMENT",
        "authorName": str((comment.raw or {}).get("author") or comment.author_external_id),
        "authorUid": comment.author_external_id,
        "body": comment.text,
        "occurredAt": _content_time(comment.published_at) or _iso_now(),
    }


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _iso_now() -> str:
    # The downstream service currently persists callback timestamps as local
    # datetimes. It normalizes offset-bearing values to UTC and then drops
    # their offset, while calculating lag with local time. Send the expected
    # Shanghai wall-clock value without an offset until that downstream
    # contract is migrated to OffsetDateTime.
    return datetime.now(_BACKEND_TIMEZONE).replace(tzinfo=None).isoformat()


def _content_time(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc).astimezone(_BACKEND_TIMEZONE).replace(tzinfo=None).isoformat()
    if hasattr(value, "isoformat"):
        try:
            return value.astimezone(_BACKEND_TIMEZONE).replace(tzinfo=None).isoformat()
        except (TypeError, ValueError):
            return value.isoformat()
    text = str(value).strip()
    if text.isdigit():
        return datetime.fromtimestamp(int(text), tz=timezone.utc).astimezone(_BACKEND_TIMEZONE).replace(tzinfo=None).isoformat()
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is not None:
            return parsed.astimezone(_BACKEND_TIMEZONE).replace(tzinfo=None).isoformat()
    except ValueError:
        pass
    return text or None


def _int_or_none(value: Any, *, default: int | None = None) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default
