from __future__ import annotations

import json
import shutil
import subprocess
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4
from urllib.parse import quote

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import AliasChoices, BaseModel, ConfigDict, Field, field_validator

from app.adapters.registry import get_adapter
from app.core.config import get_settings
from app.domain.enums import Capability, Platform
from app.domain.models import CollectionRequest, CollectionTarget, CollectorError
from app.ingestion.backend import sync_batch_to_backend
from app.ingestion.backend import CollectorBackendClient
from app.ingestion.live_stream import _iso_now, finish_realtime_live_push, live_end_reason, live_error_indicates_end, run_realtime_live_push
from app.ingestion.hls_relay import get_hls_relay_manager
from app.ingestion.runtime_cleanup import cleanup_after_collection

router = APIRouter(prefix="/api/live-monitor", tags=["live-monitor"])
v1_router = APIRouter(prefix="/api/v1/live-monitor", tags=["live-monitor-v1"])

TERMINAL_STATUSES = {"STOPPED", "COMPLETED", "ENDED", "NOT_LIVE", "FAILED"}
ACTIVE_STATUSES = {"STARTING", "RUNNING", "STOPPING"}

STAGE_MESSAGES = {
    "QUEUED": "live collection queued",
    "RESOLVING": "resolving live stream metadata",
    "NOT_LIVE": "live source is not currently live",
    "RECORDING": "recording foreign live media clip",
    "COLLECTED": "media clip collected",
    "SYNCING": "syncing live media downstream",
    "COMPLETED": "foreign live session finished",
    "FAILED": "foreign live session failed",
    "STOPPING": "stop requested",
    "STOPPED": "stopped",
}
SEGMENTS_PER_BATCH = 1
MAX_ACTIVE_SESSIONS = 4


class _CamelModel(BaseModel):
    model_config = ConfigDict(populate_by_name=True)


class LiveMonitorStart(_CamelModel):
    platform: Platform = Field(default=Platform.YOUTUBE)
    url: str = Field(
        default="",
        validation_alias=AliasChoices("url", "sourceUrl", "source_url", "liveUrl", "live_url", "targetValue", "target_value"),
    )
    room_id: str = Field(default="", alias="roomId", validation_alias=AliasChoices("roomId", "room_id", "roomNo", "room_no"))
    account_id: str = Field(default="", alias="accountId", validation_alias=AliasChoices("accountId", "account_id"))
    uid: str = ""
    username: str = ""
    channel_id: str = Field(default="", alias="channelId", validation_alias=AliasChoices("channelId", "channel_id"))
    handle: str = ""
    user_id: str = Field(default="", alias="userId", validation_alias=AliasChoices("userId", "user_id"))
    page_id: str = Field(default="", alias="pageId", validation_alias=AliasChoices("pageId", "page_id"))
    video_id: str = Field(default="", alias="videoId", validation_alias=AliasChoices("videoId", "video_id"))
    status_id: str = Field(default="", alias="statusId", validation_alias=AliasChoices("statusId", "status_id", "tweetId", "tweet_id"))
    segment_seconds: int = Field(default=300, ge=5, le=3600, alias="segmentSeconds")
    collect_comments: bool = Field(default=False, alias="collectComments")
    download_media: bool = Field(default=True, alias="downloadMedia")
    run_in_background: bool = Field(default=True, alias="runInBackground")
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("platform", mode="before")
    @classmethod
    def _normalize_platform(cls, value):
        if isinstance(value, Platform):
            return value
        text = str(value or "").strip().upper()
        aliases = {"TWITTER": "X", "YT": "YOUTUBE"}
        return aliases.get(text, text)


class LiveMonitorTaskCreate(_CamelModel):
    account_id: str = Field(default="", alias="accountId", validation_alias=AliasChoices("accountId", "account_id"))
    platform: Platform = Field(default=Platform.YOUTUBE)
    source_url: str = Field(default="", alias="sourceUrl", validation_alias=AliasChoices("sourceUrl", "source_url", "url", "liveUrl", "live_url"))
    room_id: str = Field(default="", alias="roomId", validation_alias=AliasChoices("roomId", "room_id"))
    channel_id: str = Field(default="", alias="channelId", validation_alias=AliasChoices("channelId", "channel_id"))
    handle: str = ""
    username: str = ""
    video_id: str = Field(default="", alias="videoId", validation_alias=AliasChoices("videoId", "video_id"))
    audio_segment_seconds: int | None = Field(default=None, ge=5, le=3600, alias="audioSegmentSeconds")
    segment_seconds: int | None = Field(default=None, ge=5, le=3600, alias="segmentSeconds")
    live_clip_seconds: int | None = Field(default=None, ge=5, le=3600, alias="liveClipSeconds")
    collect_comments: bool = Field(default=True, alias="collectComments")
    download_media: bool = Field(default=True, alias="downloadMedia")
    run_in_background: bool = Field(default=True, alias="runInBackground")
    max_analysis_lag_seconds: int | None = Field(default=None, alias="maxAnalysisLagSeconds")
    visual_sample_seconds: int | None = Field(default=None, alias="visualSampleSeconds")
    deep_analysis_interval_seconds: int | None = Field(default=None, alias="deepAnalysisIntervalSeconds")
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("platform", mode="before")
    @classmethod
    def _normalize_platform(cls, value):
        return LiveMonitorStart._normalize_platform(value)

    def to_start_request(self) -> LiveMonitorStart:
        clip_seconds = self.live_clip_seconds or self.segment_seconds or self.audio_segment_seconds or 300
        metadata = dict(self.metadata or {})
        metadata.setdefault("maxAnalysisLagSeconds", self.max_analysis_lag_seconds)
        metadata.setdefault("visualSampleSeconds", self.visual_sample_seconds)
        metadata.setdefault("deepAnalysisIntervalSeconds", self.deep_analysis_interval_seconds)
        metadata.setdefault("realtimeChat", bool(self.run_in_background))
        metadata.setdefault("continuousLive", bool(self.run_in_background))
        metadata.setdefault("maxRuntimeSeconds", 86400)
        return LiveMonitorStart(
            platform=self.platform,
            sourceUrl=self.source_url,
            roomId=self.room_id,
            accountId=str(self.account_id or ""),
            channelId=self.channel_id,
            handle=self.handle,
            username=self.username,
            videoId=self.video_id,
            segmentSeconds=clip_seconds,
            collectComments=self.collect_comments,
            downloadMedia=self.download_media,
            runInBackground=self.run_in_background,
            metadata={key: value for key, value in metadata.items() if value is not None},
        )


class LiveMonitorService:
    def __init__(self, *, base_dir: Path = Path("data/live-captures/foreign")) -> None:
        self.base_dir = base_dir
        self._sessions: dict[str, dict[str, Any]] = {}
        self._lock = threading.Lock()
        self._chat_stops: dict[str, threading.Event] = {}
        self._chat_processes: dict[str, subprocess.Popen] = {}
        self._session_stops: dict[str, threading.Event] = {}

    def start(self, request: LiveMonitorStart) -> dict[str, Any]:
        source_url = self._resolve_source_url(request)
        if not source_url:
            raise ValueError("url/sourceUrl/liveUrl/roomId/accountId/uid/username is required")
        existing = self._active_session_for(request.platform, source_url)
        if existing:
            return existing
        active_count = self._active_count()
        if active_count >= MAX_ACTIVE_SESSIONS:
            raise RuntimeError("too many active live sessions")
        session_id = self._new_session_id(request.platform)
        session = self._initial_session(session_id, request, source_url)
        self._save_session(session)
        self._session_stops[session_id] = threading.Event()
        if request.run_in_background:
            # Do not let a long-running live session block API shutdown. The
            # session is persisted and the collector recovery path resumes it
            # after a managed restart.
            thread = threading.Thread(target=lambda: self._run_session(session_id, request, source_url), name=f"live-monitor-{session_id}", daemon=True)
            thread.start()
        else:
            self._run_session(session_id, request, source_url)
        return self.get(session_id)

    def get(self, session_id: str) -> dict[str, Any]:
        with self._lock:
            session = self._sessions.get(session_id)
        if session:
            return dict(session)
        path = self._session_path_by_id(session_id)
        if not path:
            raise KeyError(session_id)
        return json.loads(path.read_text(encoding="utf-8"))

    def stop(self, session_id: str) -> dict[str, Any]:
        session = self.get(session_id)
        if session.get("status") in TERMINAL_STATUSES:
            return session
        self._update_session(
            session_id,
            {
                "status": "STOPPING",
                "stopRequestedAt": self._now(),
                "progressMessage": "stop requested; current foreign live clip will finish if already recording",
            },
        )
        stop_event = self._session_stops.get(session_id)
        if stop_event:
            stop_event.set()
        session = self.get(session_id)
        if not session.get("batches"):
            self._update_session(session_id, {"status": "STOPPED", "finishedAt": self._now(), "progressMessage": "stopped before media recording"})
        return self.get(session_id)

    def _run_session(self, session_id: str, request: LiveMonitorStart, source_url: str) -> None:
        settings = get_settings()
        hls_manager = get_hls_relay_manager(settings.hls_output_root, settings.collector_public_base_url)
        started = datetime.now(timezone.utc)
        batch_id = f"{session_id}-batch-000000"
        metadata = dict(request.metadata or {})
        metadata.update(
            {
                "mode": "LIVE",
                "collectorJobType": "LIVE",
                "collectionType": str(metadata.get("backendCollectionType") or "USERNAME"),
                "liveUrl": source_url,
                "isLive": True,
                "segmentSeconds": request.segment_seconds,
                "liveClipSeconds": request.segment_seconds,
                "taskId": session_id,
            }
        )
        if request.platform == Platform.YOUTUBE:
            metadata["youtube_live_clip_seconds"] = request.segment_seconds
            metadata.setdefault("youtube_no_progress", True)
        elif request.platform == Platform.X:
            metadata["x_live_clip_seconds"] = request.segment_seconds
        elif request.platform == Platform.INSTAGRAM:
            metadata["instagram_live_clip_seconds"] = request.segment_seconds
        elif request.platform == Platform.TIKTOK:
            metadata["tiktok_live_clip_seconds"] = request.segment_seconds
        metadata.setdefault("max_height", 360)
        collection_request = CollectionRequest(
            job_id=session_id,
            platform=request.platform,
            capability=Capability.LIVE_RESOLVE,
            target=CollectionTarget(
                platform=request.platform,
                target_type="CONTENT",
                target_value=source_url,
                source_url=source_url,
                capabilities=[Capability.LIVE_RESOLVE],
                metadata=metadata,
            ),
            limit=1,
            collect_comments=request.collect_comments,
            download_media=False,
        )
        try:
            self._append_log(session_id, f"[LIVE] start platform={request.platform.value} url={source_url} segmentSeconds={request.segment_seconds}")
            self._update_session(
                session_id,
                {
                    "status": "STARTING",
                    "stage": "RESOLVING",
                    "progressMessage": STAGE_MESSAGES["RESOLVING"],
                    "diagnostics": self._runtime_diagnostics(request.platform, settings),
                },
            )
            adapter = get_adapter(request.platform)
            probe_batch = None
            probe_deadline = time.monotonic() + (120 if metadata.get("continuousLive") else 0)
            while probe_batch is None:
                try:
                    candidate = adapter.discover(collection_request)
                except CollectorError as exc:
                    if not (metadata.get("continuousLive") and exc.retryable and time.monotonic() < probe_deadline):
                        raise
                    self._update_session(
                        session_id,
                        {
                            "status": "RUNNING",
                            "stage": "WAITING_FOR_LIVE",
                            "retryable": True,
                            "failedReason": f"temporary live source resolution failure: {exc}",
                            "progressMessage": "waiting for live source; retrying without ending the task",
                        },
                    )
                    time.sleep(5)
                    continue
                if not self._batch_is_live(candidate) and metadata.get("continuousLive") and time.monotonic() < probe_deadline:
                    self._update_session(
                        session_id,
                        {
                            "status": "RUNNING",
                            "stage": "WAITING_FOR_LIVE",
                            "retryable": True,
                            "progressMessage": "live source is offline; waiting for it to start",
                        },
                    )
                    time.sleep(5)
                    continue
                probe_batch = candidate
            metadata_payload = self._metadata_from_batch(probe_batch, source_url)
            metadata_payload["realtimeChat"] = bool(metadata.get("realtimeChat"))
            metadata_payload["chatStatus"] = "STARTING" if request.collect_comments else "UNAVAILABLE"
            self._update_session(session_id, {"metadata": metadata_payload})
            is_live = self._batch_is_live(probe_batch)
            if not is_live and request.platform in {Platform.YOUTUBE, Platform.FACEBOOK, Platform.X, Platform.INSTAGRAM, Platform.TIKTOK}:
                self._update_session(
                    session_id,
                    {
                        "status": "NOT_LIVE",
                        "stage": "NOT_LIVE",
                        "finishedAt": self._now(),
                        "progressMessage": STAGE_MESSAGES["NOT_LIVE"],
                        "metadata": metadata_payload,
                    },
                )
                self._write_summary(session_id)
                return

            # Resolve and publish the real HLS playback URL before the first
            # downstream registration. Previously YouTube was registered as
            # OFFICIAL_EMBED here, then replaced after the first 15s media
            # upload; consumers could start the wrong player and freeze when
            # that finite clip ended.
            initial_hls_url = ""
            initial_hls_ready = False
            if metadata.get("continuousLive") and request.platform in {Platform.TIKTOK, Platform.YOUTUBE}:
                initial_content = probe_batch.contents[0] if probe_batch.contents else None
                initial_upstream = str((initial_content.raw if initial_content else {}).get("live_stream_url") or probe_batch.raw.get("live_stream_url") or "").strip()
                if initial_upstream:
                    initial_hls_manager = get_hls_relay_manager(settings.hls_output_root, settings.collector_public_base_url)
                    initial_hls_url = initial_hls_manager.ensure(
                        session_id,
                        initial_upstream,
                        platform=request.platform.value.lower(),
                        upstream_proxy=settings.youtube_proxy if request.platform == Platform.YOUTUBE else "",
                    )
                    # Allow the relay to fill its burst-tolerant startup
                    # buffer before exposing playback to the client.
                    ready_deadline = time.monotonic() + 35
                    while time.monotonic() < ready_deadline and not initial_hls_manager.ready(session_id, platform=request.platform.value.lower()):
                        time.sleep(0.25)
                    initial_hls_ready = initial_hls_manager.ready(session_id, platform=request.platform.value.lower())
                    collection_request.target.metadata["playbackHlsUrl"] = initial_hls_url
                    collection_request.target.metadata["playbackMode"] = "HLS"
                    collection_request.target.metadata["playbackReady"] = initial_hls_ready
                    self._update_session(
                        session_id,
                        {
                            "playUrl": initial_hls_url,
                            "embedUrl": initial_hls_url,
                            "playbackMode": "HLS",
                            "playerType": "HLS",
                            "playbackReady": initial_hls_ready,
                            "mediaAvailable": initial_hls_ready,
                        },
                    )

            if (request.collect_comments or metadata.get("continuousLive")) and request.platform in {Platform.YOUTUBE, Platform.FACEBOOK, Platform.X, Platform.INSTAGRAM, Platform.TIKTOK} and metadata.get("realtimeChat"):
                downstream_stream_id = ""
                downstream_client = None
                if settings.collector_sync_enabled and (request.account_id or metadata.get("accountId")):
                    downstream_client = CollectorBackendClient.from_settings(settings)
                    try:
                        downstream = downstream_client.upsert_live_stream(
                            {
                            "taskId": session_id,
                            "platform": request.platform.value,
                            "accountId": int(request.account_id or metadata.get("accountId") or 0),
                            "externalLiveId": probe_batch.contents[0].external_id if probe_batch.contents else "",
                            "roomId": probe_batch.contents[0].external_id if probe_batch.contents else "",
                            "title": metadata_payload.get("title") or "",
                            "sourceUrl": initial_hls_url or source_url,
                            "playUrl": initial_hls_url or source_url,
                            "mediaUrl": initial_hls_url or source_url,
                            "videoUrl": initial_hls_url or source_url,
                            "hlsUrl": initial_hls_url,
                            "embedId": probe_batch.contents[0].external_id if probe_batch.contents and not initial_hls_url else "",
                            "playbackMode": "HLS" if initial_hls_url else ("OFFICIAL_EMBED" if request.platform in {Platform.YOUTUBE, Platform.FACEBOOK} else "SOURCE_URL"),
                            "playerType": "HLS" if initial_hls_url else ("YOUTUBE_IFRAME" if request.platform == Platform.YOUTUBE else "FACEBOOK_IFRAME" if request.platform == Platform.FACEBOOK else "URL"),
                            "status": "LIVE",
                            "playbackReady": initial_hls_ready,
                            "mediaAvailable": initial_hls_ready,
                            "audioAvailable": False,
                            "chatAvailable": bool(request.collect_comments),
                            "metricsAvailable": True,
                            "accessLevel": str(metadata.get("accessLevel") or "A"),
                            "sourceProvider": str(metadata.get("sourceProvider") or "COLLECTOR"),
                            "discoveredAt": self._now(),
                            }
                        )
                        downstream_stream_id = str(downstream.get("streamId") or downstream.get("id") or downstream.get("data", {}).get("streamId") or "")
                        self._update_session(session_id, {"downstreamStreamId": downstream_stream_id})
                    except Exception as exc:  # noqa: BLE001 - local chat remains available if downstream is degraded.
                        self._append_log(session_id, f"[CHAT] downstream live registration failed: {type(exc).__name__}: {exc}")
                        downstream_client = None
                elif settings.collector_sync_enabled:
                    self._append_log(session_id, "[LIVE] downstream live registration skipped: monitoring accountId is required")
                if request.collect_comments and request.platform == Platform.YOUTUBE:
                    self._start_youtube_chat_collector(
                        session_id,
                        source_url,
                        probe_batch.contents[0].external_id if probe_batch.contents else "",
                        request,
                        settings,
                        downstream_client,
                        downstream_stream_id,
                    )
                elif request.collect_comments and request.platform == Platform.X:
                    self._start_x_reply_collector(
                        session_id,
                        source_url,
                        probe_batch.contents[0].external_id if probe_batch.contents else "",
                        request,
                        downstream_client,
                        downstream_stream_id,
                    )
                elif request.collect_comments and request.platform == Platform.INSTAGRAM:
                    self._start_instagram_comment_collector(
                        session_id,
                        source_url,
                        probe_batch.contents[0].external_id if probe_batch.contents else "",
                        request,
                        downstream_client,
                        downstream_stream_id,
                    )
                elif request.collect_comments:
                    self._start_facebook_comment_collector(
                        session_id,
                        source_url,
                        probe_batch.contents[0].external_id if probe_batch.contents else "",
                        request,
                        settings,
                        downstream_client,
                        downstream_stream_id,
                    )
                self._update_session(session_id, {"chatStatus": "RUNNING", "lastMessageAt": None})
            if not request.download_media:
                self._update_session(
                    session_id,
                    {
                        "status": "COMPLETED",
                        "stage": "COMPLETED",
                        "finishedAt": self._now(),
                        "progressMessage": "live metadata resolved without media download",
                    },
                )
                self._write_summary(session_id)
                return

            if metadata.get("continuousLive"):
                self._run_continuous_media(
                    session_id=session_id,
                    request=request,
                    collection_request=collection_request,
                    adapter=adapter,
                    settings=settings,
                    started=started,
                )
                return

            batch_record = {
                "batchId": batch_id,
                "sequence": 0,
                "status": "PENDING_UPLOAD",
                "startedAt": self._now(),
                "endedAt": None,
                "videoId": "",
                "durationSeconds": request.segment_seconds,
                "backendSync": {"items": []},
            }
            self._update_session(
                session_id,
                {
                    "status": "RUNNING",
                    "stage": "RECORDING",
                    "batches": [batch_record],
                    "progressMessage": STAGE_MESSAGES["RECORDING"],
                },
            )
            media_request = collection_request.model_copy(update={"download_media": True})
            media_batch = adapter.discover(media_request)
            for content in media_batch.contents:
                content.content_type = "LIVE"
                content.raw.setdefault("is_live", True)
                content.raw.setdefault("live_status", "is_live")
            if not media_batch.contents:
                raise RuntimeError("live collector returned no media content")
            content = media_batch.contents[0]
            comments = self._comments_payload(media_batch.comments, platform=request.platform, limit=int(request.metadata.get("max_display_comments") or 50))
            danmaku = self._write_danmaku(session_id, request.platform, comments, enabled=request.collect_comments)
            batch_record.update(
                {
                    "status": "PENDING_UPLOAD",
                    "endedAt": self._now(),
                    "videoId": content.external_id,
                    "title": content.title,
                    "sourceUrl": content.url or source_url,
                    "assetsCollected": len(media_batch.assets),
                    "commentsCollected": len(media_batch.comments),
                    "commentsPath": danmaku.get("path", ""),
                }
            )
            self._update_session(
                session_id,
                {
                    "contentsCollected": len(media_batch.contents),
                    "commentsCollected": len(media_batch.comments),
                    "assetsCollected": len(media_batch.assets),
                    "danmaku": danmaku,
                    "comments": comments,
                    "batches": [batch_record],
                    "stage": "SYNCING",
                    "progressMessage": "media clip collected; syncing downstream",
                },
            )
            backend_sync = {"items": [], "agent_ingest": {"enabled": False, "items": []}}
            if settings.collector_sync_enabled:
                try:
                    backend_sync = sync_batch_to_backend(media_batch, media_request, settings=settings)
                except CollectorError:
                    if not metadata.get("realtimeChat"):
                        raise
                    backend_sync = {"items": [], "agent_ingest": {"enabled": False, "items": []}}
            synced = len(backend_sync.get("items") or [])
            batch_record["backendSync"] = backend_sync
            batch_record["status"] = "SYNCED" if synced else "UPLOAD_FAILED"
            results = self._display_results_from_batch(batch_record, request.platform, source_url)
            terminal = "COMPLETED" if synced or not settings.collector_sync_enabled or metadata.get("realtimeChat") else "FAILED"
            update = {
                "status": terminal,
                "stage": "COMPLETED" if terminal == "COMPLETED" else "FAILED",
                "finishedAt": self._now(),
                "contentsSynced": synced,
                "batches": [batch_record],
                "results": results,
                "latestResult": results[-1] if results else None,
                "hasDisplayableResult": bool(results),
                "danmaku": danmaku,
                "comments": comments,
                "progressMessage": f"foreign live session finished synced={synced}",
                "elapsedSeconds": round((datetime.now(timezone.utc) - started).total_seconds(), 3),
            }
            if terminal == "FAILED":
                update["failedReason"] = self._sync_failure_reason(backend_sync)
                update["failedCode"] = "DOWNSTREAM_SYNC_FAILED"
                update["failedAction"] = "CHECK_DOWNSTREAM_PAYLOAD_OR_SERVICE"
            self._update_session(session_id, update)
            self._append_log(session_id, f"[LIVE] done platform={request.platform.value} status={terminal} synced={synced} videoId={content.external_id}")
            self._write_summary(session_id)
        except Exception as exc:  # noqa: BLE001 - session service must persist terminal failures.
            failure = self._failure_payload(exc)
            self._append_log(session_id, f"[LIVE] failed platform={request.platform.value} code={failure['failedCode']} reason={type(exc).__name__}: {exc}")
            self._update_session(
                session_id,
                {
                    "status": "FAILED",
                    "stage": "FAILED",
                    "finishedAt": self._now(),
                    "progressMessage": STAGE_MESSAGES["FAILED"],
                    **failure,
                },
            )
            self._write_summary(session_id)
        finally:
            if (request.metadata or {}).get("continuousLive") or not (request.metadata or {}).get("realtimeChat"):
                stop_event = self._chat_stops.pop(session_id, None)
                if stop_event:
                    stop_event.set()
                process = self._chat_processes.pop(session_id, None)
                if process and process.poll() is None:
                    process.terminate()
            self._session_stops.pop(session_id, None)
            try:
                cleanup_after_collection(settings=settings)
            except Exception:
                pass

    def _run_continuous_media(self, *, session_id: str, request: LiveMonitorStart, collection_request: CollectionRequest, adapter, settings, started: datetime) -> None:
        hls_manager = get_hls_relay_manager(settings.hls_output_root, settings.collector_public_base_url)
        stop_event = self._session_stops.get(session_id) or threading.Event()
        max_runtime = int(collection_request.target.metadata.get("maxRuntimeSeconds") or 86400)
        media_request = collection_request.model_copy(update={"download_media": True})
        sequence = 0
        batches: list[dict[str, Any]] = []
        downstream_stream_id = ""
        end_reason = ""
        heartbeat_stop: threading.Event | None = None
        heartbeat_thread: threading.Thread | None = None

        def ensure_heartbeat(stream_id: str) -> None:
            nonlocal heartbeat_stop, heartbeat_thread, downstream_stream_id
            downstream_stream_id = str(stream_id or downstream_stream_id)
            if heartbeat_thread is None and downstream_stream_id:
                heartbeat_stop, heartbeat_thread = self._start_downstream_heartbeat(
                    session_id,
                    request,
                    downstream_stream_id,
                    settings,
                )

        try:
            downstream_stream_id = str(self.get(session_id).get("downstreamStreamId") or "")
        except (KeyError, TypeError, ValueError):
            downstream_stream_id = ""
        if downstream_stream_id and settings.collector_sync_enabled:
            ensure_heartbeat(downstream_stream_id)

        self._update_session(
            session_id,
            {
                "status": "RUNNING",
                "stage": "RECORDING",
                "continuous": True,
                "maxRuntimeSeconds": max_runtime,
                "progressMessage": "continuous live capture is running",
                "lastHeartbeatAt": None,
                "lastHeartbeatAckAt": None,
                "lastHeartbeatAttemptAt": None,
                "heartbeatDeliveryStatus": "NOT_STARTED",
                "heartbeatHealth": "UNKNOWN",
                "heartbeatSequence": 0,
                "heartbeatAckLatencyMs": None,
                "heartbeatBackendAcked": False,
                "heartbeatError": None,
                "heartbeatConsecutiveFailures": 0,
            },
        )
        consecutive_source_failures = 0
        while not stop_event.is_set() and (datetime.now(timezone.utc) - started).total_seconds() < max_runtime:
            try:
                media_request.target.metadata["liveSequenceStart"] = sequence
                media_batch = adapter.discover(media_request)
                detected_end = live_end_reason(media_batch)
                if detected_end:
                    end_reason = detected_end
                    self._append_log(session_id, f"[LIVE] platform ended reason={end_reason}")
                    break
                for content in media_batch.contents:
                    content.content_type = "LIVE"
                    content.raw.setdefault("is_live", True)
                    content.raw.setdefault("live_status", "is_live")
                if not media_batch.contents:
                    raise RuntimeError("live collector returned no media content")
                consecutive_source_failures = 0
                content = media_batch.contents[0]
                if request.platform in {Platform.TIKTOK, Platform.YOUTUBE}:
                    # Start/refresh one persistent sliding HLS relay. MP4
                    # uploads remain available for downstream processing, but
                    # the browser no longer has to jump between finite files.
                    upstream_url = str(content.raw.get("live_stream_url") or "")
                    if upstream_url:
                        hls_url = hls_manager.ensure(
                            session_id,
                            upstream_url,
                            platform=request.platform.value.lower(),
                            upstream_proxy=settings.youtube_proxy if request.platform == Platform.YOUTUBE else "",
                        )
                        media_request.target.metadata["playbackHlsUrl"] = hls_url
                        media_request.target.metadata["playbackMode"] = "HLS"
                        # Do not advertise a playable stream until the relay
                        # has actually written a live playlist. This avoids a
                        # player starting on an empty/stale file and freezing
                        # at the first segment.
                        if not hls_manager.ready(session_id, platform=request.platform.value.lower()):
                            ready_deadline = time.monotonic() + 35
                            while time.monotonic() < ready_deadline and not hls_manager.ready(
                                session_id,
                                platform=request.platform.value.lower(),
                            ):
                                time.sleep(0.25)
                        media_request.target.metadata["playbackReady"] = hls_manager.ready(
                            session_id,
                            platform=request.platform.value.lower(),
                        )
                push_result = {"segmentsUploaded": 0, "messagesUploaded": 0}
                if settings.collector_sync_enabled:
                    push_result = run_realtime_live_push(
                        media_batch,
                        media_request,
                        settings=settings,
                        on_stream_registered=ensure_heartbeat,
                    )
                    downstream_stream_id = str(push_result.get("streamId") or downstream_stream_id)
                    if downstream_stream_id and heartbeat_thread is None:
                        heartbeat_stop, heartbeat_thread = self._start_downstream_heartbeat(
                            session_id,
                            request,
                            downstream_stream_id,
                            settings,
                        )
                # Keep the continuous-session playback contract in sync with
                # the rolling media uploader.  Without this update the
                # session serializer falls back to the TikTok HTML /live URL,
                # so the browser has no playable source even though the
                # collector has uploaded a valid MP4 segment.
                latest_play_url = str(push_result.get("playUrl") or "").strip()
                latest_playback_mode = str(push_result.get("playbackMode") or "").strip().upper()
                if latest_play_url:
                    latest_result = {
                        "streamId": downstream_stream_id,
                        "mediaUrl": latest_play_url,
                        "playUrl": latest_play_url,
                        "playbackMode": latest_playback_mode or "SOURCE_URL",
                        "playbackReady": bool(push_result.get("playbackReady", True)),
                        "mediaAvailable": bool(push_result.get("playbackReady", True)) or bool(media_batch.assets),
                        "segmentsUploaded": int(push_result.get("segmentsUploaded") or 0),
                        "messagesUploaded": int(push_result.get("messagesUploaded") or 0),
                        "updatedAt": self._now(),
                    }
                else:
                    latest_result = None
                sequence += int(push_result.get("segmentsUploaded") or 0)
                comments = self._comments_payload(media_batch.comments, platform=request.platform, limit=int(request.metadata.get("max_display_comments") or 50))
                batch_record = {
                    "batchId": f"{session_id}-batch-{len(batches):06d}",
                    "sequence": len(batches),
                    "status": "SYNCED" if push_result.get("segmentsUploaded") or not settings.collector_sync_enabled else "COLLECTED",
                    "startedAt": self._now(),
                    "endedAt": self._now(),
                    "videoId": content.external_id,
                    "title": content.title,
                    "sourceUrl": content.url or request.url,
                    "durationSeconds": request.segment_seconds,
                    "assetsCollected": len(media_batch.assets),
                    "commentsCollected": len(media_batch.comments),
                    "mediaUrl": latest_play_url,
                    "playUrl": latest_play_url,
                    "playbackMode": latest_playback_mode or "SOURCE_URL",
                    "backendSync": {"items": [push_result] if push_result.get("streamId") else []},
                }
                batches.append(batch_record)
                batches = batches[-100:]
                self._update_session(
                    session_id,
                    {
                        "status": "RUNNING",
                        "stage": "RECORDING",
                        "batches": batches,
                        "contentsCollected": len(batches),
                        "commentsCollected": sum(int(item.get("commentsCollected") or 0) for item in batches),
                        "assetsCollected": sum(int(item.get("assetsCollected") or 0) for item in batches),
                        "contentsSynced": sequence,
                        "downstreamStreamId": downstream_stream_id,
                        "playUrl": latest_play_url or self.get(session_id).get("playUrl") or "",
                        "playbackMode": latest_playback_mode or self.get(session_id).get("playbackMode") or "SOURCE_URL",
                        "playbackReady": bool(latest_play_url) or bool(self.get(session_id).get("playbackReady")),
                        "mediaAvailable": bool(latest_play_url) or bool(self.get(session_id).get("mediaAvailable")),
                        "lastMediaAt": self._now() if int(push_result.get("segmentsUploaded") or 0) > 0 else self.get(session_id).get("lastMediaAt"),
                        **({"latestResult": latest_result} if latest_result else {}),
                        "progressMessage": f"continuous live capture running; segments={sequence}",
                    },
                )
            except Exception as exc:  # noqa: BLE001 - keep the session alive across transient platform disconnects.
                if live_error_indicates_end(exc):
                    end_reason = "PLATFORM_REPORTED_ENDED"
                    self._append_log(session_id, f"[LIVE] platform ended while resolving segment: {exc}")
                    break
                consecutive_source_failures += 1
                error_text = str(exc).lower()
                if consecutive_source_failures >= 5 and (
                    "connect econnrefused" in error_text
                    or "browser fallback failed" in error_text
                    or "no live content" in error_text
                ):
                    end_reason = "COLLECTOR_SOURCE_UNAVAILABLE"
                    self._append_log(session_id, f"[LIVE] source unavailable after retries; ending session: {exc}")
                    break
                self._append_log(session_id, f"[LIVE] continuous segment failed: {type(exc).__name__}: {exc}")
                self._update_session(
                    session_id,
                    {
                        "status": "RUNNING",
                        "stage": "DEGRADED",
                        "failedReason": f"temporary segment failure: {exc}",
                        "retryable": True,
                        "progressMessage": "live source temporarily unavailable; retrying",
                    },
                )
                stop_event.wait(5.0)
        if heartbeat_stop:
            heartbeat_stop.set()
        if heartbeat_thread:
            heartbeat_thread.join(timeout=3)
        if downstream_stream_id and settings.collector_sync_enabled:
            try:
                finish_realtime_live_push(
                    downstream_stream_id,
                    media_request,
                    segment_count=sequence,
                    message_count=0,
                    settings=settings,
                    reason=end_reason or ("COLLECTOR_STOPPED" if stop_event.is_set() else "COLLECTOR_WINDOW_COMPLETE"),
                    detection_source="platform_metadata" if end_reason else "collector_window",
                )
            except Exception as exc:  # noqa: BLE001
                self._append_log(session_id, f"[LIVE] failed to close downstream stream: {type(exc).__name__}: {exc}")
        if request.platform in {Platform.TIKTOK, Platform.YOUTUBE}:
            hls_manager.stop(session_id, platform=request.platform.value.lower())
        final_status = "ENDED" if end_reason else "STOPPED" if stop_event.is_set() else "COMPLETED"
        self._update_session(
            session_id,
            {
                "status": final_status,
                "stage": final_status,
                "finishedAt": self._now(),
                "batches": batches,
                "contentsSynced": sequence,
                "liveStatus": "ENDED" if end_reason else None,
                "endDetected": bool(end_reason),
                "endReason": end_reason or None,
                "progressMessage": "platform reported that the live stream ended" if end_reason else "continuous live capture stopped" if final_status == "STOPPED" else "continuous live capture window complete",
                "elapsedSeconds": round((datetime.now(timezone.utc) - started).total_seconds(), 3),
            },
        )
        self._write_summary(session_id)

    def _start_downstream_heartbeat(
        self,
        session_id: str,
        request: LiveMonitorStart,
        stream_id: str,
        settings,
    ) -> tuple[threading.Event, threading.Thread]:
        """Keep downstream collector liveness current during media resolution."""
        stop_event = threading.Event()

        def send_heartbeat() -> None:
            interval = max(3, min(int(getattr(settings, "collector_heartbeat_interval_seconds", 5) or 5), 30))
            timeout = max(3, min(int(getattr(settings, "collector_heartbeat_timeout_seconds", 10) or 10), 30))
            client = CollectorBackendClient.from_settings(settings, timeout_seconds=timeout)
            client.max_attempts = 1
            try:
                heartbeat_sequence = int(self.get(session_id).get("heartbeatSequence") or 0)
            except (KeyError, TypeError, ValueError):
                heartbeat_sequence = 0
            consecutive_failures = 0
            last_ack_monotonic: float | None = None
            while not stop_event.is_set():
                heartbeat_sequence += 1
                sent_at = _iso_now()
                attempted_at = datetime.now(timezone.utc).isoformat()
                attempt_started = time.monotonic()
                try:
                    heartbeat_payload = {
                        "taskId": session_id,
                        "collectorNode": settings.worker_id,
                        "liveStatus": "LIVE",
                        "mediaStatus": "RUNNING",
                        "chatStatus": "RUNNING" if request.collect_comments else "UNAVAILABLE",
                        "metricsStatus": "RUNNING",
                        "heartbeatSeq": heartbeat_sequence,
                        "heartbeatId": f"{session_id}:{heartbeat_sequence}",
                        "heartbeatIntervalSeconds": interval,
                        "sentAt": sent_at,
                    }
                    current_session = self.get(session_id)
                    current_play_url = str(current_session.get("playUrl") or "").strip()
                    if current_play_url and str(current_session.get("playbackMode") or "").upper() == "HLS":
                        heartbeat_payload.update(
                            {
                                "playUrl": current_play_url,
                                "sourceUrl": current_play_url,
                                "mediaUrl": current_play_url,
                                "videoUrl": current_play_url,
                                "hlsUrl": current_play_url,
                                "playbackMode": "HLS",
                                "playerType": "HLS",
                                "playbackReady": bool(current_session.get("playbackReady")),
                            }
                        )
                    acknowledgement = client.live_heartbeat(
                        stream_id,
                        heartbeat_payload,
                    )
                    acknowledged_at = datetime.now(timezone.utc).isoformat()
                    ack_latency_ms = round((time.monotonic() - attempt_started) * 1000, 1)
                    consecutive_failures = 0
                    last_ack_monotonic = time.monotonic()
                    self._update_session(
                        session_id,
                        {
                            "status": "RUNNING",
                            "stage": "RECORDING",
                            "lastHeartbeatAt": acknowledged_at,
                            "lastHeartbeatAckAt": acknowledged_at,
                            "lastHeartbeatAttemptAt": attempted_at,
                            "heartbeatDeliveryStatus": "ACKED",
                            "heartbeatSequence": heartbeat_sequence,
                            "heartbeatIntervalSeconds": interval,
                            "heartbeatAckLatencyMs": ack_latency_ms,
                            "heartbeatConsecutiveFailures": 0,
                            "heartbeatHealth": "HEALTHY",
                            "heartbeatError": None,
                            "heartbeatBackendAcked": isinstance(acknowledgement, dict) or acknowledgement is not None,
                        },
                    )
                except Exception as exc:  # noqa: BLE001 - retry on next tick.
                    consecutive_failures += 1
                    age = (time.monotonic() - last_ack_monotonic) if last_ack_monotonic is not None else float("inf")
                    self._append_log(session_id, f"[LIVE] heartbeat retryable failure: {type(exc).__name__}: {exc}")
                    self._update_session(
                        session_id,
                        {
                            "status": "RUNNING",
                            "stage": "RECONNECTING",
                            "lastHeartbeatAttemptAt": attempted_at,
                            "heartbeatDeliveryStatus": "RETRYING",
                            "heartbeatSequence": heartbeat_sequence,
                            "heartbeatIntervalSeconds": interval,
                            "heartbeatConsecutiveFailures": consecutive_failures,
                            "heartbeatHealth": "DISCONNECTED" if consecutive_failures >= 3 or age > interval * 3 else "DEGRADED",
                            "heartbeatError": f"{type(exc).__name__}: {exc}",
                        },
                    )
                if stop_event.wait(float(interval)):
                    break

        thread = threading.Thread(target=send_heartbeat, name=f"live-monitor-heartbeat-{session_id}", daemon=True)
        thread.start()
        return stop_event, thread

    def _initial_session(self, session_id: str, request: LiveMonitorStart, source_url: str) -> dict[str, Any]:
        playback_url = (
            _youtube_embed_url(source_url)
            if request.platform == Platform.YOUTUBE
            else _facebook_embed_url(source_url)
            if request.platform == Platform.FACEBOOK
            else source_url
        )
        return {
            "sessionId": session_id,
            "platform": request.platform.value,
            "roomId": request.room_id or "",
            "accountId": request.account_id or "",
            "uid": request.uid or "",
            "username": request.username or "",
            "sourceUrl": source_url,
            "playUrl": playback_url,
            "embedUrl": playback_url,
            "playbackMode": "OFFICIAL_EMBED" if request.platform in {Platform.YOUTUBE, Platform.FACEBOOK} else "SOURCE_URL",
            "playerType": "YOUTUBE_IFRAME" if request.platform == Platform.YOUTUBE else "FACEBOOK_IFRAME" if request.platform == Platform.FACEBOOK else "X_STATUS_URL" if request.platform == Platform.X else "INSTAGRAM_URL" if request.platform == Platform.INSTAGRAM else "URL",
            "playbackReady": bool(playback_url),
            "liveTarget": self._live_target(request),
            "status": "STARTING",
            "stage": "QUEUED",
            "segmentSeconds": request.segment_seconds,
            "segmentsPerBatch": SEGMENTS_PER_BATCH,
            "createdAt": self._now(),
            "startedAt": None,
            "finishedAt": None,
            "metadata": {},
            "diagnostics": {},
            "danmaku": {"enabled": bool(request.collect_comments), "count": 0, "path": "", "items": []},
            "comments": [],
            "segments": [],
            "batches": [],
            "results": [],
            "latestResult": None,
            "hasDisplayableResult": False,
            "contentsCollected": 0,
            "commentsCollected": 0,
            "assetsCollected": 0,
            "contentsSynced": 0,
            "failedReason": None,
            "failedCode": None,
            "failedAction": None,
            "retryable": False,
            "progressMessage": STAGE_MESSAGES["QUEUED"],
            "lastHeartbeatAt": self._now(),
            "lastHeartbeatAckAt": None,
            "lastHeartbeatAttemptAt": None,
            "heartbeatDeliveryStatus": "NOT_STARTED",
            "heartbeatHealth": "UNKNOWN",
            "heartbeatConsecutiveFailures": 0,
            "updatedAt": self._now(),
            "sessionPath": str(self._session_path(request.platform, session_id)),
            "logPath": str(self._log_path(request.platform, session_id)),
        }

    def _start_youtube_chat_collector(self, session_id: str, source_url: str, video_id: str, request: LiveMonitorStart, settings, downstream_client=None, downstream_stream_id: str = "") -> None:
        stop_event = threading.Event()
        self._chat_stops[session_id] = stop_event
        thread = threading.Thread(
            target=self._run_youtube_chat_collector,
            args=(session_id, source_url, video_id, request, settings, stop_event, downstream_client, downstream_stream_id),
            name=f"youtube-live-chat-{session_id}",
            daemon=True,
        )
        thread.start()

    def _start_facebook_comment_collector(self, session_id: str, source_url: str, video_id: str, request: LiveMonitorStart, settings, downstream_client=None, downstream_stream_id: str = "") -> None:
        stop_event = threading.Event()
        self._chat_stops[session_id] = stop_event
        thread = threading.Thread(
            target=self._run_facebook_comment_collector,
            args=(session_id, source_url, video_id, request, stop_event, downstream_client, downstream_stream_id),
            name=f"facebook-live-comments-{session_id}",
            daemon=True,
        )
        thread.start()

    def _start_x_reply_collector(self, session_id: str, source_url: str, content_id: str, request: LiveMonitorStart, downstream_client=None, downstream_stream_id: str = "") -> None:
        """Poll replies to the live X post/Space without labelling them as native chat.

        X does not expose a uniform public live-chat protocol for Spaces and live
        video.  The supported interaction stream is therefore browser-collected
        replies, persisted as `reply` records so downstream consumers can keep it
        distinct from YouTube live chat.
        """
        stop_event = threading.Event()
        self._chat_stops[session_id] = stop_event
        thread = threading.Thread(
            target=self._run_x_reply_collector,
            args=(session_id, source_url, content_id, request, stop_event, downstream_client, downstream_stream_id),
            name=f"x-live-replies-{session_id}",
            daemon=True,
        )
        thread.start()

    def _run_x_reply_collector(self, session_id: str, source_url: str, content_id: str, request: LiveMonitorStart, stop_event: threading.Event, downstream_client=None, downstream_stream_id: str = "") -> None:
        adapter = get_adapter(Platform.X)
        collection_request = CollectionRequest(
            job_id=session_id,
            platform=Platform.X,
            capability=Capability.COMMENTS,
            target=CollectionTarget(
                platform=Platform.X,
                target_type="CONTENT",
                target_value=source_url,
                source_url=source_url,
                metadata={"browser_comment_limit": 100, "browser_comment_timeout_seconds": 20},
            ),
            limit=1,
            collect_comments=True,
            download_media=False,
        )
        seen_ids: set[str] = set()
        started = time.monotonic()
        max_seconds = int((request.metadata or {}).get("chatMaxSeconds") or 900)
        while not stop_event.is_set() and time.monotonic() - started < max_seconds:
            try:
                batch = adapter.collect_comments(collection_request)
                fresh = [item for item in batch.comments if item.external_id not in seen_ids]
                if fresh:
                    seen_ids.update(item.external_id for item in fresh)
                    payload = self._comments_payload(fresh, platform=Platform.X, limit=500)
                    current = self.get(session_id)
                    merged = ((current.get("comments") or []) + payload)[-500:]
                    if downstream_client and downstream_stream_id:
                        try:
                            downstream_client.live_messages(
                                downstream_stream_id,
                                {
                                    "batchId": f"{content_id}-replies-{len(seen_ids):06d}",
                                    "items": [
                                        {
                                            "externalMessageId": item["id"],
                                            "messageType": "COMMENT",
                                            "authorName": item.get("authorName") or "",
                                            "authorUid": item.get("authorId") or "",
                                            "body": item.get("text") or "",
                                            "occurredAt": item.get("publishedAt") or self._now(),
                                        }
                                        for item in payload
                                    ],
                                },
                            )
                        except Exception as exc:  # noqa: BLE001 - reply delivery cannot stop media recording.
                            self._append_log(session_id, f"[REPLIES] downstream messages failed: {type(exc).__name__}: {exc}")
                    self._update_session(
                        session_id,
                        {
                            "comments": merged,
                            "commentsCollected": len(merged),
                            "chatStatus": "RUNNING",
                            "lastMessageAt": self._now(),
                            "danmaku": self._write_danmaku(session_id, Platform.X, merged, enabled=True),
                        },
                    )
                    self._append_log(session_id, f"[REPLIES] X received={len(fresh)} total={len(merged)}")
            except Exception as exc:  # noqa: BLE001 - X page changes and login prompts are temporary/degraded state.
                self._append_log(session_id, f"[REPLIES] X poll failed: {type(exc).__name__}: {exc}")
            stop_event.wait(10.0)

    def _start_instagram_comment_collector(self, session_id: str, source_url: str, content_id: str, request: LiveMonitorStart, downstream_client=None, downstream_stream_id: str = "") -> None:
        """Poll visible Instagram Live comments through the authenticated browser profile."""
        stop_event = threading.Event()
        self._chat_stops[session_id] = stop_event
        thread = threading.Thread(
            target=self._run_instagram_comment_collector,
            args=(session_id, source_url, content_id, request, stop_event, downstream_client, downstream_stream_id),
            name=f"instagram-live-comments-{session_id}",
            daemon=True,
        )
        thread.start()

    def _run_instagram_comment_collector(self, session_id: str, source_url: str, content_id: str, request: LiveMonitorStart, stop_event: threading.Event, downstream_client=None, downstream_stream_id: str = "") -> None:
        adapter = get_adapter(Platform.INSTAGRAM)
        collection_request = CollectionRequest(
            job_id=session_id,
            platform=Platform.INSTAGRAM,
            capability=Capability.COMMENTS,
            target=CollectionTarget(
                platform=Platform.INSTAGRAM,
                target_type="CONTENT",
                target_value=source_url,
                source_url=source_url,
                metadata={"browser_comment_limit": 100, "browser_comment_timeout_seconds": 20},
            ),
            limit=1,
            collect_comments=True,
            download_media=False,
        )
        seen_ids: set[str] = set()
        started = time.monotonic()
        max_seconds = int((request.metadata or {}).get("chatMaxSeconds") or 900)
        while not stop_event.is_set() and time.monotonic() - started < max_seconds:
            try:
                batch = adapter.collect_comments(collection_request)
                fresh = [item for item in batch.comments if item.external_id not in seen_ids]
                if fresh:
                    seen_ids.update(item.external_id for item in fresh)
                    payload = self._comments_payload(fresh, platform=Platform.INSTAGRAM, limit=500)
                    current = self.get(session_id)
                    merged = ((current.get("comments") or []) + payload)[-500:]
                    if downstream_client and downstream_stream_id:
                        try:
                            downstream_client.live_messages(
                                downstream_stream_id,
                                {
                                    "batchId": f"{content_id}-comments-{len(seen_ids):06d}",
                                    "items": [
                                        {
                                            "externalMessageId": item["id"],
                                            "messageType": "COMMENT",
                                            "authorName": item.get("authorName") or "",
                                            "authorUid": item.get("authorId") or "",
                                            "body": item.get("text") or "",
                                            "occurredAt": item.get("publishedAt") or self._now(),
                                        }
                                        for item in payload
                                    ],
                                },
                            )
                        except Exception as exc:  # noqa: BLE001 - comments are independently degradable.
                            self._append_log(session_id, f"[COMMENTS] Instagram downstream messages failed: {type(exc).__name__}: {exc}")
                    self._update_session(
                        session_id,
                        {
                            "comments": merged,
                            "commentsCollected": len(merged),
                            "chatStatus": "RUNNING",
                            "lastMessageAt": self._now(),
                            "danmaku": self._write_danmaku(session_id, Platform.INSTAGRAM, merged, enabled=True),
                        },
                    )
                    self._append_log(session_id, f"[COMMENTS] Instagram received={len(fresh)} total={len(merged)}")
            except Exception as exc:  # noqa: BLE001 - login prompts/page changes should not stop media collection.
                self._append_log(session_id, f"[COMMENTS] Instagram poll failed: {type(exc).__name__}: {exc}")
            stop_event.wait(10.0)

    def _run_facebook_comment_collector(self, session_id: str, source_url: str, video_id: str, request: LiveMonitorStart, stop_event: threading.Event, downstream_client=None, downstream_stream_id: str = "") -> None:
        adapter = get_adapter(Platform.FACEBOOK)
        collection_request = CollectionRequest(
            job_id=session_id,
            platform=Platform.FACEBOOK,
            capability=Capability.COMMENTS,
            target=CollectionTarget(
                platform=Platform.FACEBOOK,
                target_type="CONTENT",
                target_value=source_url,
                source_url=source_url,
                metadata={"browser_comment_limit": 100, "browser_comment_timeout_seconds": 20},
            ),
            limit=1,
            collect_comments=True,
            download_media=False,
        )
        seen_ids: set[str] = set()
        started = time.monotonic()
        max_seconds = int((request.metadata or {}).get("chatMaxSeconds") or 900)
        while not stop_event.is_set() and time.monotonic() - started < max_seconds:
            try:
                batch = adapter.collect_comments(collection_request)
                fresh = [item for item in batch.comments if item.external_id not in seen_ids]
                if fresh:
                    seen_ids.update(item.external_id for item in fresh)
                    payload = self._comments_payload(fresh, platform=Platform.FACEBOOK, limit=500)
                    current = self.get(session_id)
                    existing = current.get("comments") or []
                    merged = (existing + payload)[-500:]
                    if downstream_client and downstream_stream_id:
                        try:
                            downstream_client.live_messages(
                                downstream_stream_id,
                                {"batchId": f"{video_id}-comments-{len(seen_ids):06d}", "items": [
                                    {"externalMessageId": item["id"], "messageType": "COMMENT", "authorName": item.get("authorName") or "", "authorUid": item.get("authorId") or "", "body": item.get("text") or "", "occurredAt": item.get("publishedAt") or self._now()}
                                    for item in payload
                                ]},
                            )
                        except Exception as exc:  # noqa: BLE001
                            self._append_log(session_id, f"[COMMENTS] downstream messages failed: {type(exc).__name__}: {exc}")
                    self._update_session(session_id, {"comments": merged, "commentsCollected": len(merged), "chatStatus": "RUNNING", "lastMessageAt": self._now(), "danmaku": self._write_danmaku(session_id, Platform.FACEBOOK, merged, enabled=True)})
                    self._append_log(session_id, f"[COMMENTS] Facebook received={len(fresh)} total={len(merged)}")
            except Exception as exc:  # noqa: BLE001
                self._append_log(session_id, f"[COMMENTS] Facebook poll failed: {type(exc).__name__}: {exc}")
            stop_event.wait(10.0)

    def _run_youtube_chat_collector(self, session_id: str, source_url: str, video_id: str, request: LiveMonitorStart, settings, stop_event: threading.Event, downstream_client=None, downstream_stream_id: str = "") -> None:
        chat_dir = self._session_path(request.platform, session_id).parent / "youtube-chat"
        chat_dir.mkdir(parents=True, exist_ok=True)
        output_template = str(chat_dir / "%(id)s.live_chat.%(ext)s")
        ytdlp = shutil.which("yt-dlp") or str(Path(".venv/bin/yt-dlp"))
        command = [
            ytdlp,
            "--quiet",
            "--no-warnings",
            "--no-playlist",
            "--skip-download",
            "--write-subs",
            "--sub-langs",
            "live_chat",
            "--sub-format",
            "json",
            "--output",
            output_template,
            source_url,
        ]
        browser_profile = settings.youtube_browser_profile
        if browser_profile and Path(browser_profile).exists():
            command[1:1] = ["--cookies-from-browser", f"{settings.youtube_browser}:{browser_profile}"]
        else:
            cookie_file = settings.youtube_cookie_file
            if cookie_file and Path(cookie_file).is_file():
                command[1:1] = ["--cookies", str(cookie_file)]
        js_runtime = str(settings.youtube_js_runtime or "").strip()
        if js_runtime and Path(js_runtime).exists():
            command[1:1] = ["--js-runtimes", f"deno:{js_runtime}"]
        remote_components = str(settings.youtube_remote_components or "").strip()
        if remote_components:
            command[1:1] = ["--remote-components", remote_components]
        try:
            process = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            self._chat_processes[session_id] = process
            self._append_log(session_id, "[CHAT] YouTube live chat collector started")
            self._poll_youtube_chat_files(session_id, video_id, request, chat_dir, stop_event, process, downstream_client, downstream_stream_id)
        except Exception as exc:  # noqa: BLE001 - chat is an independent degraded-able channel.
            self._append_log(session_id, f"[CHAT] collector failed: {type(exc).__name__}: {exc}")
            self._update_session(session_id, {"chatStatus": "FAILED", "chatFailedReason": str(exc)})

    def _poll_youtube_chat_files(self, session_id: str, video_id: str, request: LiveMonitorStart, chat_dir: Path, stop_event: threading.Event, process: subprocess.Popen, downstream_client=None, downstream_stream_id: str = "") -> None:
        adapter = get_adapter(Platform.YOUTUBE)
        seen_ids: set[str] = set()
        started = time.monotonic()
        max_seconds = int((request.metadata or {}).get("chatMaxSeconds") or 900)
        while not stop_event.is_set() and time.monotonic() - started < max_seconds:
            comments = self._read_youtube_chat_comments(adapter, request, video_id, chat_dir)
            fresh = [item for item in comments if item.external_id not in seen_ids]
            if fresh:
                seen_ids.update(item.external_id for item in fresh)
                payload = self._comments_payload(fresh, platform=Platform.YOUTUBE, limit=500)
                current = self.get(session_id)
                existing = current.get("comments") or []
                merged = (existing + payload)[-500:]
                if downstream_client and downstream_stream_id:
                    try:
                        downstream_client.live_messages(
                            downstream_stream_id,
                            {
                                "batchId": f"{video_id}-chat-{len(seen_ids):06d}",
                                "items": [
                                    {
                                        "externalMessageId": item["id"],
                                        "messageType": "COMMENT",
                                        "authorName": item.get("authorName") or "",
                                        "authorUid": item.get("authorId") or "",
                                        "body": item.get("text") or "",
                                        "occurredAt": item.get("publishedAt") or self._now(),
                                    }
                                    for item in payload
                                ],
                            },
                        )
                    except Exception as exc:  # noqa: BLE001 - downstream chat failure must not stop local display.
                        self._append_log(session_id, f"[CHAT] downstream messages failed: {type(exc).__name__}: {exc}")
                self._update_session(
                    session_id,
                    {
                        "comments": merged,
                        "commentsCollected": len(merged),
                        "chatStatus": "RUNNING",
                        "lastMessageAt": self._now(),
                        "danmaku": self._write_danmaku(session_id, Platform.YOUTUBE, merged, enabled=True),
                    },
                )
                self._append_log(session_id, f"[CHAT] received={len(fresh)} total={len(merged)}")
            if process.poll() is not None and not comments:
                break
            stop_event.wait(2.0)

    def _read_youtube_chat_comments(self, adapter, request: LiveMonitorStart, video_id: str, chat_dir: Path):
        collection_request = CollectionRequest(
            job_id=request.metadata.get("taskId") or request.video_id or video_id,
            platform=Platform.YOUTUBE,
            capability=Capability.LIVE_RESOLVE,
            target=CollectionTarget(
                platform=Platform.YOUTUBE,
                target_type="CONTENT",
                target_value=video_id,
                source_url=request.url,
                metadata={"max_comments": 500},
            ),
            limit=1,
            collect_comments=True,
            download_media=False,
        )
        comments = []
        for path in sorted(chat_dir.glob("*.live_chat*.json*")):
            if not path.is_file() or path.stat().st_size <= 0:
                continue
            for raw in adapter._iter_live_chat_records(path):
                comment = adapter._live_chat_comment_from_record(raw, video_id, len(comments))
                if comment and comment.external_id not in {item.external_id for item in comments}:
                    comments.append(comment)
        return comments


    def _comments_payload(self, comments, *, platform: Platform, limit: int = 50) -> list[dict[str, Any]]:
        payload: list[dict[str, Any]] = []
        for comment in comments[: max(0, limit)]:
            raw = comment.raw or {}
            source = str(raw.get("source") or self._default_comment_source(platform))
            payload.append(
                {
                    "id": comment.external_id,
                    "platform": platform.value,
                    "kind": self._comment_kind(platform, source),
                    "source": source,
                    "contentId": comment.content_external_id,
                    "authorId": comment.author_external_id,
                    "authorName": str(raw.get("author") or comment.author_external_id or ""),
                    "text": comment.text,
                    "publishedAt": comment.published_at.isoformat() if comment.published_at else None,
                    "collectedAt": comment.collected_at.isoformat() if comment.collected_at else None,
                    "likeCount": raw.get("like_count"),
                    "parentId": raw.get("parent"),
                    "videoOffsetTimeMsec": raw.get("videoOffsetTimeMsec"),
                    "relativeTime": raw.get("relative_time"),
                }
            )
        return payload

    def _default_comment_source(self, platform: Platform) -> str:
        return {
            Platform.YOUTUBE: "youtube_comment",
            Platform.TIKTOK: "tiktok_comment",
            Platform.INSTAGRAM: "instagram_comment",
            Platform.FACEBOOK: "facebook_comment",
            Platform.X: "x_reply",
        }.get(platform, "comment")

    def _comment_kind(self, platform: Platform, source: str) -> str:
        lowered = source.lower()
        if platform == Platform.YOUTUBE and "live_chat" in lowered:
            return "live_chat"
        if platform == Platform.X:
            return "reply"
        if "live" in lowered or "danmaku" in lowered:
            return "live_comment"
        return "comment"

    def _write_danmaku(self, session_id: str, platform: Platform, comments: list[dict[str, Any]], *, enabled: bool) -> dict[str, Any]:
        session_dir = self._session_path(platform, session_id).parent
        path = session_dir / "comments.json"
        session_dir.mkdir(parents=True, exist_ok=True)
        if comments:
            path.write_text(json.dumps(comments, ensure_ascii=False, indent=2), encoding="utf-8")
        return {
            "enabled": bool(enabled),
            "count": len(comments),
            "path": str(path) if comments else "",
            "items": comments[:50],
        }

    def _display_results_from_batch(self, batch_record: dict[str, Any], platform: Platform, source_url: str) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        backend_items = ((batch_record.get("backendSync") or {}).get("items") or [])
        for item in backend_items:
            if not isinstance(item, dict):
                continue
            results.append(
                {
                    "platform": platform.value,
                    "isLive": True,
                    "sequence": int(batch_record.get("sequence") or 0),
                    "batchId": str(batch_record.get("batchId") or ""),
                    "videoId": str(item.get("sourceVideoId") or batch_record.get("videoId") or ""),
                    "title": str(batch_record.get("title") or ""),
                    "sourceUrl": str(batch_record.get("sourceUrl") or source_url),
                    "contentId": item.get("contentId"),
                    "status": str(item.get("status") or batch_record.get("status") or ""),
                    "mediaUrl": str(item.get("mediaUrl") or ""),
                    "thumbnailUrl": str(item.get("thumbnailUrl") or ""),
                    "mediaObject": str(item.get("mediaObject") or ""),
                    "thumbnailObject": str(item.get("thumbnailObject") or ""),
                    "durationSeconds": batch_record.get("durationSeconds"),
                    "startedAt": batch_record.get("startedAt"),
                    "endedAt": batch_record.get("endedAt"),
                }
            )
        return results

    def _sync_failure_reason(self, backend_sync: dict[str, Any]) -> str:
        agent = backend_sync.get("agent_ingest") if isinstance(backend_sync, dict) else None
        if isinstance(agent, dict):
            failed = [item for item in agent.get("items") or [] if isinstance(item, dict) and item.get("status") == "FAILED"]
            if failed:
                error = str(failed[0].get("error") or "").strip()
                if error:
                    return f"live media synced to collector but agent ingest failed: {error}"
        return "live media was collected but not synced downstream"

    def _failure_payload(self, exc: Exception) -> dict[str, Any]:
        if isinstance(exc, CollectorError):
            return {
                "failedReason": f"{exc.code.value}: {exc.message}",
                "failedCode": exc.code.value,
                "failedAction": exc.credential_action.value,
                "retryable": bool(exc.retryable),
            }
        return {
            "failedReason": f"{type(exc).__name__}: {exc}",
            "failedCode": type(exc).__name__,
            "failedAction": "CHECK_COLLECTOR_LOG",
            "retryable": False,
        }

    def _runtime_diagnostics(self, platform: Platform, settings) -> dict[str, Any]:
        if platform != Platform.YOUTUBE:
            return {"platform": platform.value}
        cookie_file = settings.youtube_cookie_file
        browser_profile = settings.youtube_browser_profile
        cookie_pool_count = self._count_cookie_pool(settings.youtube_cookie_dir)
        if cookie_file and Path(cookie_file).is_file():
            cookie_source = "cookie_file"
        elif cookie_pool_count:
            cookie_source = "cookie_pool"
        elif browser_profile and Path(browser_profile).exists():
            cookie_source = "browser_profile"
        else:
            cookie_source = "none"
        return {
            "platform": platform.value,
            "cookieSource": cookie_source,
            "cookieFileConfigured": bool(cookie_file),
            "cookieFileExists": bool(cookie_file and Path(cookie_file).is_file()),
            "cookiePoolCount": cookie_pool_count,
            "browser": settings.youtube_browser,
            "browserProfileConfigured": bool(browser_profile),
            "browserProfileExists": bool(browser_profile and Path(browser_profile).exists()),
            "proxyConfigured": bool(settings.youtube_proxy),
            "liveClipSeconds": settings.youtube_live_clip_seconds,
        }

    def _count_cookie_pool(self, cookie_dir: Path) -> int:
        try:
            if not cookie_dir.is_dir():
                return 0
            return sum(
                1
                for pattern in ("*.txt", "*.cookies", "*.cookie", "*.netscape")
                for path in cookie_dir.glob(pattern)
                if path.is_file() and path.stat().st_size > 0
            )
        except Exception:
            return 0

    def _metadata_from_batch(self, batch, source_url: str) -> dict[str, Any]:
        content = batch.contents[0] if batch.contents else None
        account = batch.accounts[0] if batch.accounts else None
        raw = content.raw if content else {}
        return {
            "title": content.title if content else "",
            "anchor": account.nickname if account else "",
            "anchorId": account.external_id if account else "",
            "username": account.username if account else "",
            "cover": raw.get("thumbnail") if isinstance(raw, dict) else "",
            "sourceUrl": content.url if content and content.url else source_url,
            "isLive": bool(raw.get("is_live")) if isinstance(raw, dict) else False,
            "liveStatus": str(raw.get("live_status") or "") if isinstance(raw, dict) else "",
        }

    def _batch_is_live(self, batch) -> bool:
        for content in batch.contents:
            raw = content.raw or {}
            if content.content_type == "LIVE" or raw.get("is_live") or str(raw.get("live_status") or "").lower() == "is_live":
                return True
        return False

    def _resolve_source_url(self, request: LiveMonitorStart) -> str:
        explicit = (request.url or "").strip()
        if explicit:
            return explicit
        if request.platform == Platform.YOUTUBE:
            return self._youtube_live_url(request)
        if request.platform == Platform.TIKTOK:
            return self._tiktok_live_url(request)
        if request.platform == Platform.INSTAGRAM:
            return self._instagram_live_url(request)
        if request.platform == Platform.FACEBOOK:
            return self._facebook_live_url(request)
        if request.platform == Platform.X:
            return self._x_live_url(request)
        return ""

    def _live_target(self, request: LiveMonitorStart) -> dict[str, str]:
        return {
            "platform": request.platform.value,
            "url": request.url or "",
            "roomId": request.room_id or "",
            "accountId": request.account_id or "",
            "uid": request.uid or "",
            "username": request.username or "",
            "handle": request.handle or "",
            "channelId": request.channel_id or "",
            "userId": request.user_id or "",
            "pageId": request.page_id or "",
            "videoId": request.video_id or "",
            "statusId": request.status_id or "",
        }

    def _youtube_live_url(self, request: LiveMonitorStart) -> str:
        value = self._first_value(request.video_id, request.room_id)
        if value:
            if value.startswith(("http://", "https://")):
                return value
            return f"https://www.youtube.com/watch?v={value}"
        channel = self._first_value(request.channel_id, request.uid)
        if channel:
            if channel.startswith(("http://", "https://")):
                return channel
            return f"https://www.youtube.com/channel/{channel}/live"
        handle = self._handle_value(request.handle, request.username, request.account_id)
        return f"https://www.youtube.com/{handle}/live" if handle else ""

    def _tiktok_live_url(self, request: LiveMonitorStart) -> str:
        handle = self._handle_value(request.handle, request.username, request.uid, request.account_id, request.room_id)
        return f"https://www.tiktok.com/{handle}/live" if handle else ""

    def _instagram_live_url(self, request: LiveMonitorStart) -> str:
        handle = self._plain_handle(request.handle, request.username, request.uid, request.account_id, request.room_id)
        return f"https://www.instagram.com/{handle}/live/" if handle else ""

    def _facebook_live_url(self, request: LiveMonitorStart) -> str:
        video_id = self._first_value(request.video_id, request.room_id)
        if video_id:
            if video_id.startswith(("http://", "https://")):
                return video_id
            return f"https://www.facebook.com/watch/?v={video_id}"
        page = self._first_value(request.page_id, request.user_id, request.uid, request.username, request.account_id)
        if not page:
            return ""
        if page.startswith(("http://", "https://")):
            return page.rstrip("/") + "/live" if "/live" not in page.rstrip("/").lower() else page
        return f"https://www.facebook.com/{page.strip('/').lstrip('@')}/live"

    def _x_live_url(self, request: LiveMonitorStart) -> str:
        status_id = self._first_value(request.status_id, request.video_id, request.room_id)
        handle = self._plain_handle(request.handle, request.username, request.uid, request.account_id)
        if status_id:
            if status_id.startswith(("http://", "https://")):
                return status_id
            return f"https://x.com/{handle}/status/{status_id}" if handle else ""
        return f"https://x.com/{handle}" if handle else ""

    def _first_value(self, *values: str) -> str:
        for value in values:
            text = str(value or "").strip()
            if text:
                return text
        return ""

    def _handle_value(self, *values: str) -> str:
        handle = self._plain_handle(*values)
        return f"@{handle}" if handle and not handle.startswith("@") else handle

    def _plain_handle(self, *values: str) -> str:
        value = self._first_value(*values)
        if not value:
            return ""
        if value.startswith(("http://", "https://")):
            return value
        return value.strip().strip("/").lstrip("@")

    def _active_session_for(self, platform: Platform, source_url: str) -> dict[str, Any] | None:
        with self._lock:
            for session in self._sessions.values():
                if session.get("platform") == platform.value and session.get("sourceUrl") == source_url and session.get("status") in ACTIVE_STATUSES:
                    return dict(session)
        for path in self.base_dir.glob("*/*/session.json") if self.base_dir.exists() else []:
            try:
                session = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue
            if session.get("platform") == platform.value and session.get("sourceUrl") == source_url and session.get("status") in ACTIVE_STATUSES:
                return session
        return None

    def _active_count(self) -> int:
        count = 0
        with self._lock:
            count += sum(1 for session in self._sessions.values() if session.get("status") in ACTIVE_STATUSES)
        return count

    def list_sessions(self, *, platform: Platform | None = None, limit: int = 50) -> list[dict[str, Any]]:
        sessions: dict[str, dict[str, Any]] = {}
        with self._lock:
            for session_id, session in self._sessions.items():
                sessions[session_id] = dict(session)
        if self.base_dir.exists():
            for path in self.base_dir.glob("*/*/session.json"):
                try:
                    session = json.loads(path.read_text(encoding="utf-8"))
                except Exception:
                    continue
                session_id = str(session.get("sessionId") or path.parent.name)
                sessions.setdefault(session_id, session)
        values = [session for session in sessions.values() if not platform or session.get("platform") == platform.value]
        values.sort(key=lambda item: str(item.get("createdAt") or item.get("updatedAt") or ""), reverse=True)
        return values[: max(1, min(limit, 200))]

    def _write_summary(self, session_id: str) -> None:
        session = self.get(session_id)
        path = Path(session["sessionPath"]).parent / "summary.json"
        path.write_text(json.dumps(session, ensure_ascii=False, indent=2), encoding="utf-8")

    def _save_session(self, session: dict[str, Any]) -> None:
        with self._lock:
            self._sessions[session["sessionId"]] = dict(session)
        self._write_session_file(session)

    def _update_session(self, session_id: str, updates: dict[str, Any]) -> None:
        with self._lock:
            session = self._sessions.get(session_id)
            if session is None:
                existing_path = self._session_path_by_id(session_id)
                session = json.loads(existing_path.read_text(encoding="utf-8")) if existing_path else {"sessionId": session_id}
            session = dict(session)
            now = self._now()
            if updates.get("status") == "RUNNING" and not session.get("startedAt"):
                session["startedAt"] = now
            session.update(updates)
            session["updatedAt"] = now
            self._sessions[session_id] = session
        self._write_session_file(session)

    def _write_session_file(self, session: dict[str, Any]) -> None:
        path = Path(session["sessionPath"])
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(session, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(path)

    def _append_log(self, session_id: str, line: str) -> None:
        session = self.get(session_id)
        path = Path(session["logPath"])
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(f"{line}\n")

    def _session_path(self, platform: Platform, session_id: str) -> Path:
        return self.base_dir / platform.value.lower() / session_id / "session.json"

    def _log_path(self, platform: Platform, session_id: str) -> Path:
        return self.base_dir / platform.value.lower() / session_id / "live.log"

    def _session_path_by_id(self, session_id: str) -> Path | None:
        for path in self.base_dir.glob(f"*/{session_id}/session.json") if self.base_dir.exists() else []:
            return path
        return None

    def _new_session_id(self, platform: Platform) -> str:
        return f"{platform.value.lower()}-live-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{uuid4().hex[:8]}"

    def _now(self) -> str:
        return datetime.now(timezone.utc).isoformat()


live_monitor_service = LiveMonitorService()


@router.post("/start", status_code=202)
def start_live_monitor(body: LiveMonitorStart) -> dict[str, Any]:
    try:
        return live_monitor_service.start(body)
    except RuntimeError as exc:
        raise HTTPException(status_code=429, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/{session_id}")
def get_live_monitor(session_id: str) -> dict[str, Any]:
    try:
        session = live_monitor_service.get(session_id)
        return _compat_session_playback_fields(session)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="live session not found") from exc


@router.post("/{session_id}/stop")
def stop_live_monitor(session_id: str) -> dict[str, Any]:
    try:
        session = live_monitor_service.stop(session_id)
        return _compat_session_playback_fields(session)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="live session not found") from exc


def _session_to_stream(session: dict[str, Any]) -> dict[str, Any]:
    metadata = session.get("metadata") or {}
    latest = session.get("latestResult") or None
    failed_code = session.get("failedCode")
    status = str(session.get("status") or "")
    stage = str(session.get("stage") or "")
    live_status = str(metadata.get("liveStatus") or "")
    if stage in {"WAITING_FOR_LIVE", "HLS_NOT_READY", "RESOLVING"}:
        stream_status = "CONNECTING"
    elif live_status == "is_live" or status == "RUNNING":
        stream_status = "LIVE"
    elif status == "NOT_LIVE":
        stream_status = "NOT_LIVE"
    elif status in {"COMPLETED", "STOPPED"}:
        stream_status = "ENDED"
    elif status == "FAILED":
        stream_status = "ERROR"
    else:
        stream_status = "CONNECTING"
    media_url = str(
        ((latest or {}).get("mediaUrl") if isinstance(latest, dict) else "")
        or ((latest or {}).get("mediaObject") if isinstance(latest, dict) else "")
        or session.get("playUrl")
        or ""
    )
    playback_mode = "OFFICIAL_EMBED" if session.get("platform") in {Platform.YOUTUBE.value, Platform.FACEBOOK.value} else "SOURCE_URL"
    embed_url = (
        _youtube_embed_url(str(session.get("sourceUrl") or ""))
        if session.get("platform") == Platform.YOUTUBE.value
        else _facebook_embed_url(str(session.get("sourceUrl") or ""))
        if session.get("platform") == Platform.FACEBOOK.value
        else media_url
        if session.get("platform") == Platform.TIKTOK.value and media_url
        else session.get("sourceUrl")
    )
    if session.get("platform") == Platform.TIKTOK.value and media_url:
        playback_mode = "SOURCE_URL"
    if isinstance(latest, dict) and str(latest.get("playbackMode") or "").upper() == "HLS" and media_url:
        playback_mode = "HLS"
        embed_url = media_url
    if session.get("platform") == Platform.TIKTOK.value and not (latest and (latest.get("playbackMode") == "HLS" or str(media_url).lower().split("?", 1)[0].endswith((".m3u8", ".mp4", ".ts")))):
        embed_url = ""
        playback_mode = "SOURCE_URL"
    return {
        "streamId": session.get("sessionId"),
        "taskId": session.get("sessionId"),
        "downstreamStreamId": session.get("downstreamStreamId") or "",
        "platform": session.get("platform"),
        "sourceUrl": session.get("sourceUrl"),
        "playUrl": embed_url,
        "embedUrl": embed_url,
        "playbackMode": playback_mode,
        "playerType": "HLS" if playback_mode == "HLS" else "YOUTUBE_IFRAME" if session.get("platform") == Platform.YOUTUBE.value else "FACEBOOK_IFRAME" if session.get("platform") == Platform.FACEBOOK.value else "X_STATUS_URL" if session.get("platform") == Platform.X.value else "INSTAGRAM_URL" if session.get("platform") == Platform.INSTAGRAM.value else "URL",
        "playbackReady": bool(embed_url),
        "mediaAvailable": bool(embed_url),
        "playback": {
            "mode": playback_mode,
            "url": embed_url,
            "autoplay": True,
            "muted": True,
        },
        "title": metadata.get("title") or (latest or {}).get("title") or "",
        "anchor": metadata.get("anchor") or "",
        "anchorId": metadata.get("anchorId") or "",
        "username": metadata.get("username") or session.get("username") or "",
        "cover": metadata.get("cover") or (latest or {}).get("thumbnailUrl") or "",
        "status": stream_status,
        "rawStatus": status,
        "liveStatus": live_status,
        "collectorStatus": stage or status,
        "heartbeatHealth": session.get("heartbeatHealth") or "UNKNOWN",
        "heartbeatDeliveryStatus": session.get("heartbeatDeliveryStatus") or "NOT_STARTED",
        "heartbeatSequence": session.get("heartbeatSequence") or 0,
        "heartbeatAckLatencyMs": session.get("heartbeatAckLatencyMs"),
        "heartbeatConsecutiveFailures": session.get("heartbeatConsecutiveFailures") or 0,
        "heartbeatIntervalSeconds": 5,
        "chatStatus": session.get("chatStatus") or metadata.get("chatStatus") or ("RUNNING" if session.get("commentsCollected") else "UNKNOWN"),
        "lastMessageAt": session.get("lastMessageAt"),
        "analysisStatus": "NOT_STARTED",
        "viewerCount": None,
        "startedAt": session.get("startedAt"),
        "endedAt": session.get("finishedAt"),
        "createdAt": session.get("createdAt"),
        "updatedAt": session.get("updatedAt"),
        "lastHeartbeatAt": session.get("lastHeartbeatAt"),
        "lastHeartbeatAckAt": session.get("lastHeartbeatAckAt"),
        "lastHeartbeatAttemptAt": session.get("lastHeartbeatAttemptAt"),
        "analysisLagSeconds": None,
        "currentRiskLevel": "UNKNOWN",
        "currentRiskScore": None,
        "latestSummary": session.get("progressMessage") or "",
        "hasDisplayableResult": bool(session.get("hasDisplayableResult")) or (bool(embed_url) and status in {"STARTING", "RUNNING"}),
        "latestResult": latest,
        "results": session.get("results") or [],
        "diagnostics": session.get("diagnostics") or {},
        "failedCode": failed_code,
        "failedAction": session.get("failedAction"),
        "failedReason": session.get("failedReason"),
        "retryable": bool(session.get("retryable")),
        "progressMessage": session.get("progressMessage") or "",
        "counts": {
            "contentsCollected": session.get("contentsCollected") or 0,
            "commentsCollected": session.get("commentsCollected") or 0,
            "assetsCollected": session.get("assetsCollected") or 0,
            "contentsSynced": session.get("contentsSynced") or 0,
        },
    }


def _compat_session_playback_fields(session: dict[str, Any]) -> dict[str, Any]:
    payload = dict(session)
    stream = _session_to_stream(payload)
    for key in ("playUrl", "embedUrl", "playbackMode", "playerType", "playbackReady"):
        payload[key] = stream.get(key)
    payload["stream"] = stream
    return payload


def _youtube_embed_url(source_url: str) -> str:
    import re

    match = re.search(r"(?:watch\?v=|youtu\.be/)([A-Za-z0-9_-]{6,})", source_url)
    if match:
        return f"https://www.youtube.com/embed/{match.group(1)}"
    return source_url


def _facebook_embed_url(source_url: str) -> str:
    if not source_url:
        return source_url
    if "plugins/video.php" in source_url:
        missing = []
        if "autoplay=" not in source_url:
            missing.append("autoplay=true")
        if "mute=" not in source_url:
            missing.append("mute=true")
        if missing:
            separator = "&" if "?" in source_url else "?"
            source_url = f"{source_url}{separator}{'&'.join(missing)}"
        return source_url
    return f"https://www.facebook.com/plugins/video.php?href={quote(source_url, safe='')}&show_text=false&autoplay=true&mute=true"


def _session_segments(session: dict[str, Any]) -> list[dict[str, Any]]:
    segments: list[dict[str, Any]] = []
    for batch in session.get("batches") or []:
        if not isinstance(batch, dict):
            continue
        backend_items = ((batch.get("backendSync") or {}).get("items") or [])
        item = backend_items[0] if backend_items and isinstance(backend_items[0], dict) else {}
        segments.append(
            {
                "segmentId": batch.get("batchId") or f"{session.get('sessionId')}-segment-{batch.get('sequence', 0)}",
                "streamId": session.get("sessionId"),
                "sequenceNo": batch.get("sequence") or 0,
                "status": batch.get("status") or "",
                "videoId": batch.get("videoId") or item.get("sourceVideoId") or "",
                "title": batch.get("title") or "",
                "sourceUrl": batch.get("sourceUrl") or session.get("sourceUrl"),
                "videoUrl": item.get("mediaUrl") or "",
                "thumbnailUrl": item.get("thumbnailUrl") or "",
                "videoObject": item.get("mediaObject") or "",
                "thumbnailObject": item.get("thumbnailObject") or "",
                "durationSeconds": batch.get("durationSeconds"),
                "startedAt": batch.get("startedAt"),
                "endedAt": batch.get("endedAt"),
                "asrStatus": "NOT_STARTED",
                "visionStatus": "NOT_STARTED",
                "semanticStatus": "NOT_STARTED",
                "transcript": "",
            }
        )
    return segments


def _session_messages(session: dict[str, Any]) -> list[dict[str, Any]]:
    messages = session.get("comments") or (session.get("danmaku") or {}).get("items") or []
    normalized = []
    for index, item in enumerate(messages):
        if not isinstance(item, dict):
            continue
        normalized.append(
            {
                "messageId": item.get("id") or f"{session.get('sessionId')}-message-{index}",
                "streamId": session.get("sessionId"),
                "platform": item.get("platform") or session.get("platform"),
                "kind": item.get("kind") or "live_chat",
                "source": item.get("source") or "",
                "authorId": item.get("authorId") or "",
                "authorName": item.get("authorName") or "",
                "text": item.get("text") or "",
                "publishedAt": item.get("publishedAt"),
                "collectedAt": item.get("collectedAt"),
                "relativeTime": item.get("relativeTime"),
                "videoOffsetTimeMsec": item.get("videoOffsetTimeMsec"),
                "riskLevel": "UNKNOWN",
                "riskScore": None,
            }
        )
    return normalized


def _session_analysis_events(session: dict[str, Any]) -> list[dict[str, Any]]:
    if session.get("status") != "FAILED":
        return []
    return [
        {
            "eventId": f"{session.get('sessionId')}-failure",
            "streamId": session.get("sessionId"),
            "eventType": "COLLECTOR_FAILURE",
            "riskLevel": "UNKNOWN",
            "riskScore": None,
            "title": "直播采集失败",
            "summary": session.get("failedReason") or session.get("progressMessage") or "直播采集失败",
            "categories": [],
            "evidence": {"failedCode": session.get("failedCode"), "failedAction": session.get("failedAction")},
            "occurredAt": session.get("finishedAt") or session.get("updatedAt"),
        }
    ]


def _session_snapshot(session: dict[str, Any]) -> dict[str, Any]:
    stream = _session_to_stream(session)
    messages = _session_messages(session)
    segments = _session_segments(session)
    events = _session_analysis_events(session)
    return {
        "stream": stream,
        "latestResult": session.get("latestResult"),
        "results": session.get("results") or [],
        "segments": segments[-20:],
        "messages": messages[-100:],
        "analysisEvents": events[-50:],
        "metrics": {
            "viewerCount": None,
            "messageCount": len(messages),
            "segmentCount": len(segments),
            "analysisLagSeconds": None,
            "collectorHealth": session.get("heartbeatHealth") or ("ERROR" if session.get("status") == "FAILED" else "UNKNOWN"),
            "collectedAt": session.get("updatedAt") or session.get("createdAt"),
        },
        "health": {
            "status": session.get("status"),
            "stage": session.get("stage"),
            "progressMessage": session.get("progressMessage"),
            "failedCode": session.get("failedCode"),
            "failedReason": session.get("failedReason"),
            "failedAction": session.get("failedAction"),
            "retryable": bool(session.get("retryable")),
            "diagnostics": session.get("diagnostics") or {},
            "lastHeartbeatAt": session.get("lastHeartbeatAt"),
            "lastHeartbeatAckAt": session.get("lastHeartbeatAckAt"),
            "lastHeartbeatAttemptAt": session.get("lastHeartbeatAttemptAt"),
            "heartbeatDeliveryStatus": session.get("heartbeatDeliveryStatus") or "NOT_STARTED",
            "heartbeatHealth": session.get("heartbeatHealth") or "UNKNOWN",
            "heartbeatSequence": session.get("heartbeatSequence") or 0,
            "heartbeatAckLatencyMs": session.get("heartbeatAckLatencyMs"),
            "heartbeatConsecutiveFailures": session.get("heartbeatConsecutiveFailures") or 0,
            "updatedAt": session.get("updatedAt"),
        },
        "danmaku": session.get("danmaku") or {"enabled": False, "count": 0, "items": []},
    }


def _get_session_or_404(stream_id: str) -> dict[str, Any]:
    try:
        return live_monitor_service.get(stream_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="live stream not found") from exc


@v1_router.post("/tasks", status_code=202)
def create_live_monitor_task(body: LiveMonitorTaskCreate) -> dict[str, Any]:
    if body.platform not in {Platform.YOUTUBE, Platform.FACEBOOK, Platform.X, Platform.INSTAGRAM, Platform.TIKTOK}:
        raise HTTPException(status_code=400, detail="live monitoring currently supports YouTube, Facebook, X, Instagram, and TikTok")
    try:
        session = live_monitor_service.start(body.to_start_request())
    except RuntimeError as exc:
        raise HTTPException(status_code=429, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {
        "taskId": session.get("sessionId"),
        "streamId": session.get("sessionId"),
        "status": session.get("status"),
        "stage": session.get("stage"),
        "stream": _session_to_stream(session),
        "snapshot": _session_snapshot(session),
    }


@v1_router.post("/tasks/{task_id}/stop")
def stop_live_monitor_task(task_id: str) -> dict[str, Any]:
    try:
        session = live_monitor_service.stop(task_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="live task not found") from exc
    return {"taskId": task_id, "streamId": task_id, "status": session.get("status"), "stream": _session_to_stream(session)}


@v1_router.post("/tasks/{task_id}/restart", status_code=202)
def restart_live_monitor_task(task_id: str) -> dict[str, Any]:
    old = _get_session_or_404(task_id)
    request = LiveMonitorTaskCreate(
        platform=Platform(str(old.get("platform") or Platform.YOUTUBE.value)),
        sourceUrl=str(old.get("sourceUrl") or ""),
        segmentSeconds=int(old.get("segmentSeconds") or 300),
        collectComments=bool((old.get("danmaku") or {}).get("enabled")),
        downloadMedia=True,
        runInBackground=True,
    )
    return create_live_monitor_task(request)


@v1_router.get("/streams")
def list_live_monitor_streams(platform: Platform | None = None, limit: int = 50) -> dict[str, Any]:
    sessions = live_monitor_service.list_sessions(platform=platform, limit=limit)
    streams = [_session_to_stream(session) for session in sessions]
    return {"items": streams, "streams": streams, "total": len(streams)}


@v1_router.get("/streams/{stream_id}")
def get_live_monitor_stream(stream_id: str) -> dict[str, Any]:
    return _session_to_stream(_get_session_or_404(stream_id))


@v1_router.get("/streams/{stream_id}/snapshot")
def get_live_monitor_snapshot(stream_id: str) -> dict[str, Any]:
    return _session_snapshot(_get_session_or_404(stream_id))


@v1_router.get("/streams/{stream_id}/segments")
def get_live_monitor_segments(stream_id: str) -> dict[str, Any]:
    items = _session_segments(_get_session_or_404(stream_id))
    return {"items": items, "segments": items, "total": len(items)}


@v1_router.get("/streams/{stream_id}/messages")
def get_live_monitor_messages(stream_id: str) -> dict[str, Any]:
    items = _session_messages(_get_session_or_404(stream_id))
    return {"items": items, "messages": items, "total": len(items)}


@v1_router.get("/streams/{stream_id}/analysis-events")
def get_live_monitor_analysis_events(stream_id: str) -> dict[str, Any]:
    items = _session_analysis_events(_get_session_or_404(stream_id))
    return {"items": items, "events": items, "total": len(items)}


@v1_router.post("/streams/{stream_id}/deep-analysis", status_code=202)
def create_live_monitor_deep_analysis(stream_id: str) -> dict[str, Any]:
    session = _get_session_or_404(stream_id)
    return {
        "streamId": stream_id,
        "status": "ACCEPTED",
        "analysisStatus": "NOT_STARTED",
        "message": "deep analysis is reserved for the next phase; YouTube collection and downstream sync are already available",
        "stream": _session_to_stream(session),
    }


@v1_router.get("/streams/{stream_id}/events")
def stream_live_monitor_events(stream_id: str):
    def event_stream():
        last_updated = ""
        last_message_count = -1
        last_heartbeat_ack = ""
        ended_emitted = False
        started = time.monotonic()
        while time.monotonic() - started < 900:
            try:
                session = live_monitor_service.get(stream_id)
            except KeyError:
                return
            snapshot = _session_snapshot(session)
            updated = str(session.get("updatedAt") or "")
            messages = snapshot.get("messages") or []
            if updated != last_updated:
                yield f"event: live.snapshot\ndata: {json.dumps(snapshot, ensure_ascii=False)}\n\n"
                last_updated = updated
            if len(messages) != last_message_count:
                yield f"event: live.messages\ndata: {json.dumps({'streamId': stream_id, 'items': messages[-100:], 'total': len(messages)}, ensure_ascii=False)}\n\n"
                last_message_count = len(messages)
            if session.get("status") in TERMINAL_STATUSES and not ended_emitted:
                yield f"event: live.ended\ndata: {json.dumps({'streamId': stream_id, 'status': session.get('status'), 'stage': session.get('stage'), 'updatedAt': updated}, ensure_ascii=False)}\n\n"
                ended_emitted = True
                return
            heartbeat_ack = str(session.get("lastHeartbeatAckAt") or "")
            if heartbeat_ack and heartbeat_ack != last_heartbeat_ack:
                yield f"event: live.heartbeat\ndata: {json.dumps({'streamId': stream_id, 'status': session.get('status'), 'stage': session.get('stage'), 'updatedAt': updated, 'lastHeartbeatAt': heartbeat_ack, 'heartbeatHealth': session.get('heartbeatHealth') or 'UNKNOWN', 'heartbeatDeliveryStatus': session.get('heartbeatDeliveryStatus') or 'NOT_STARTED', 'heartbeatSequence': session.get('heartbeatSequence') or 0, 'heartbeatAckLatencyMs': session.get('heartbeatAckLatencyMs'), 'heartbeatConsecutiveFailures': session.get('heartbeatConsecutiveFailures') or 0, 'messageCount': len(messages)}, ensure_ascii=False)}\n\n"
                last_heartbeat_ack = heartbeat_ack
            else:
                # SSE keepalive is deliberately not called `live.heartbeat`:
                # the frontend must never treat a transport keepalive or a
                # chat update as proof that the collector is alive.
                yield ": keepalive\n\n"
            time.sleep(2)

    return StreamingResponse(event_stream(), media_type="text/event-stream")
