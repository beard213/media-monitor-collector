from __future__ import annotations

import json
import os
import re
import shutil
import signal
import subprocess
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse
from uuid import uuid4

from fastapi import APIRouter, HTTPException
from pydantic import AliasChoices, BaseModel, ConfigDict, Field, field_validator

from app.adapters.browser_search import BrowserSearchCollector
from app.adapters.registry import get_adapter, platform_statuses
from app.core.config import get_settings
from app.domain.enums import Capability, CollectorErrorCode, Platform
from app.domain.models import CollectionBatch, CollectionRequest, CollectionTarget, CollectorError, MediaAsset
from app.ingestion.backend import CollectorBackendClient, sync_batch_to_backend
from app.ingestion.hls_relay import get_hls_relay_manager
from app.ingestion.live_stream import _iso_now, finish_realtime_live_push, live_end_reason, live_error_indicates_end, run_realtime_live_push
from app.ingestion.runtime_cleanup import cleanup_after_collection
from app.pipeline.runner import PipelineOptions, run_pipeline

router = APIRouter(prefix="/collector", tags=["collector-jobs"])
ACTIVE_JOB_STATUSES = {"PENDING", "RUNNING"}
# The business scheduler attaches a fresh taskId/requestedAt to every polling
# tick.  Those fields describe the submission, not the work: including them
# in an account-job signature turns one 259-account refresh into a new job
# every 15 minutes and rapidly burns platform login sessions.
ACCOUNT_JOB_VOLATILE_METADATA_KEYS = {
    "taskId",
    "task_id",
    "requestedAt",
    "requested_at",
    "submittedAt",
    "submitted_at",
    "recoveredFromRunId",
    "recoveryCount",
}
# A live room can remain active for many hours.  Recovery still performs a
# fresh platform probe before registering media, so a 24-hour window does not
# resurrect an ended room; it only preserves tasks across planned restarts or
# multi-minute outages.
LIVE_RECOVERY_WINDOW_SECONDS = 24 * 60 * 60
# The host has capacity for several relay encoders, but restarting every
# historical live task at once would recreate the same resource pressure that
# recovery is meant to avoid. This is a source-level limit, not a platform
# limit: distinct YouTube channels must never evict one another.
# HLS relays remux the selected H.264/AAC rendition instead of transcoding it,
# so their steady-state CPU cost is low. Keep a finite recovery ceiling to
# protect the host from a corrupted persisted queue, while allowing the normal
# set of monitored live accounts to resume after an API restart.
LIVE_RECOVERY_MAX_CONCURRENT = max(1, int(os.getenv("MEDIA_MONITOR_LIVE_RECOVERY_MAX_CONCURRENT", "8")))
# Account/video crawls are intentionally bounded. Live jobs have their own
# priority path and are not counted here. Two ordinary jobs leave headroom for
# ffmpeg, object uploads, and a platform browser on the 8 GiB collector host.
ACCOUNT_JOB_MAX_CONCURRENT = 2
# Account refreshes have short, idempotent units of work.  Preserve that
# property across an API restart by re-queuing only accounts that have not
# completed a downstream sync.  The finite ceiling prevents one permanently
# bad request from looping forever after every deployment.
# A controlled collector restart must not discard a large recovered account
# batch merely because an earlier browser process was interrupted.  Six keeps
# retries bounded while allowing maintenance deployments to preserve work.
ACCOUNT_RECOVERY_MAX_ATTEMPTS = max(1, int(os.getenv("MEDIA_MONITOR_ACCOUNT_RECOVERY_MAX_ATTEMPTS", "6")))


def _normalize_platform(value):
    """Normalize platform aliases before startup recovery and validation."""
    if isinstance(value, str):
        normalized = value.strip().upper()
        if normalized == "TWITTER":
            normalized = "X"
        return normalized
    return value

LIVE_STAGE_MESSAGES = {
    "QUEUED": "live collection queued",
    "COLLECTING": "collecting live stream metadata and media clip",
    "SYNCING": "syncing live media downstream",
    "ENDED": "platform reported that the live stream ended",
    "COMPLETED": "live collection finished",
    "FAILED": "live collection failed",
}


class CollectorJobConflict(Exception):
    pass


class _CamelModel(BaseModel):
    model_config = ConfigDict(populate_by_name=True)


class KeywordJobCreate(_CamelModel):
    platforms: list[Platform]
    keyword_ids: list[int] = Field(default_factory=list, alias="keywordIds")
    # Keyword expansion is serialized by platform lock.  Thirty results give
    # the replenishment lanes useful depth while still keeping one browser
    # session and bounded per-keyword media work on the 8 GiB collector.
    # Account jobs retain their separate, lower per-account ceiling.
    limit: int = Field(default=10, ge=1, le=30)
    collect_comments: bool = Field(default=True, alias="collectComments")
    download_media: bool = Field(default=True, alias="downloadMedia")
    recency_months: int | None = Field(default=None, alias="recencyMonths")
    parallel_platforms: bool = Field(default=False, alias="parallelPlatforms")
    continue_on_error: bool = Field(default=True, alias="continueOnError")
    metadata: dict[str, Any] = Field(default_factory=dict)
    run_in_background: bool = Field(default=True, alias="runInBackground")

    @field_validator("platforms", mode="before")
    @classmethod
    def _normalize_platforms(cls, value):
        if isinstance(value, list):
            return [_normalize_platform(item) for item in value]
        return value


class AccountTargetCreate(_CamelModel):
    platform: Platform
    account: str = Field(min_length=1, max_length=4000)
    source_url: str = Field(default="", alias="sourceUrl")
    display_name: str = Field(
        default="",
        alias="displayName",
        validation_alias=AliasChoices("displayName", "display_name", "nickname", "name", "accountName", "account_name"),
    )
    # The business service marks a newly added or analyst-selected account as
    # priority.  This is intentionally opt-in: normal scheduled accounts keep
    # the low-impact incremental profile.
    priority: bool = Field(default=False, alias="priority")

    @field_validator("platform", mode="before")
    @classmethod
    def _normalize_account_platform(cls, value):
        return _normalize_platform(value)


class AccountJobCreate(_CamelModel):
    accounts: list[AccountTargetCreate]
    limit: int = Field(default=10, ge=1, le=10)
    collect_comments: bool = Field(default=True, alias="collectComments")
    download_media: bool = Field(default=True, alias="downloadMedia")
    recency_months: int | None = Field(default=None, alias="recencyMonths")
    recency_days: int | None = Field(default=365, ge=0, le=365, alias="recencyDays")
    continue_on_error: bool = Field(default=True, alias="continueOnError")
    metadata: dict[str, Any] = Field(default_factory=dict)
    run_in_background: bool = Field(default=True, alias="runInBackground")


class HomeFeedJobCreate(_CamelModel):
    platforms: list[Platform]
    limit_per_platform: int = Field(default=5, ge=1, le=5, alias="limitPerPlatform")
    refresh_home: bool = Field(default=True, alias="refreshHome")
    collect_comments: bool = Field(default=True, alias="collectComments")
    download_media: bool = Field(default=True, alias="downloadMedia")
    continue_on_error: bool = Field(default=True, alias="continueOnError")
    metadata: dict[str, Any] = Field(default_factory=dict)
    run_in_background: bool = Field(default=True, alias="runInBackground")

    @field_validator("platforms", mode="before")
    @classmethod
    def _normalize_platforms(cls, value):
        if isinstance(value, list):
            return [_normalize_platform(item) for item in value]
        return value




class LiveJobCreate(_CamelModel):
    platform: Platform = Field(default=Platform.YOUTUBE)
    url: str = Field(
        min_length=1,
        max_length=4000,
        alias="url",
        validation_alias=AliasChoices("url", "sourceUrl", "source_url", "targetValue", "target_value", "liveUrl", "live_url"),
    )
    collect_comments: bool = Field(default=False, alias="collectComments")
    download_media: bool = Field(default=True, alias="downloadMedia")
    live_clip_seconds: int | None = Field(default=None, ge=0, le=3600, alias="liveClipSeconds")
    timeout_seconds: int | None = Field(default=None, ge=5, le=3600, alias="timeoutSeconds")
    continue_on_error: bool = Field(default=True, alias="continueOnError")
    metadata: dict[str, Any] = Field(default_factory=dict)
    run_in_background: bool = Field(default=True, alias="runInBackground")
    realtime: bool = Field(default=False, alias="realtime")
    max_runtime_seconds: int | None = Field(default=None, ge=5, le=86400, alias="maxRuntimeSeconds")

    @field_validator("platform", mode="before")
    @classmethod
    def _normalize_live_platform(cls, value):
        return _normalize_platform(value)


def _first_non_empty(*values: Any) -> str:
    """Return the first useful request value without turning None into ``"None"``."""
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def _live_url_from_legacy_account(platform: Platform, account: dict[str, Any], body: dict[str, Any]) -> str:
    """Resolve the old account-job shape to the URL required by LIVE_RESOLVE.

    The business backend historically submits live work to ``/jobs/accounts``.
    In that payload the account home page commonly lives at ``account.sourceUrl``;
    for TikTok a deterministic ``/@handle/live`` URL is the actual live entrypoint.
    """
    candidate = _first_non_empty(
        body.get("liveUrl"), body.get("live_url"), body.get("url"),
        account.get("liveUrl"), account.get("live_url"), account.get("url"),
        account.get("sourceUrl"), account.get("source_url"), body.get("sourceUrl"), body.get("source_url"),
    )
    if platform == Platform.TIKTOK:
        if candidate and "/live" in candidate.lower():
            return candidate
        handle = _first_non_empty(account.get("username"), account.get("handle"), account.get("account"), body.get("username"), body.get("handle"))
        if candidate:
            match = re.search(r"tiktok\.com/@([^/?#]+)", candidate, flags=re.IGNORECASE)
            if match:
                return f"https://www.tiktok.com/@{match.group(1)}/live"
        if handle:
            handle = handle.rsplit("/", 1)[-1].lstrip("@")
            if handle:
                return f"https://www.tiktok.com/@{handle}/live"
    if platform == Platform.FACEBOOK:
        # A saved Facebook profile URL is an account identity, not a live
        # broadcast.  Always enter through the platform's live listing so a
        # restarted monitor discovers the current broadcast instead of
        # replaying a previous /watch or /videos URL.
        if candidate and "facebook.com/" in candidate.lower():
            parsed = urlparse(candidate)
            path = parsed.path.rstrip("/").lower()
            if path.endswith("/live") or path.endswith("/live_videos"):
                return candidate
            if "/watch" not in path and "/videos/" not in path and "/reel/" not in path:
                if path == "/profile.php":
                    # profile.php?id=... cannot safely accept a /live suffix;
                    # the numeric account fallback below is canonical.
                    query_id = re.search(r"(?:^|&)id=([^&]+)", parsed.query, flags=re.IGNORECASE)
                    if query_id:
                        return f"https://www.facebook.com/{query_id.group(1)}/live"
                elif parsed.netloc:
                    return f"https://{parsed.netloc}{parsed.path.rstrip('/')}/live"
        if handle := _first_non_empty(account.get("uid"), account.get("username"), account.get("handle"), account.get("account"), body.get("uid"), body.get("username"), body.get("handle")):
            return f"https://www.facebook.com/{handle.lstrip('@').strip('/')}/live"
    return candidate


def _stable_account_live_url(platform: Platform, account: dict[str, Any]) -> str:
    """Build a platform entrypoint from persisted stable account fields."""
    homepage = _first_non_empty(
        account.get("homepageUrl"), account.get("homepage_url"),
        account.get("sourceUrl"), account.get("source_url"),
    )
    uid = _first_non_empty(
        account.get("uid"), account.get("username"), account.get("handle"),
        account.get("account"), account.get("nickname"),
    )
    if platform == Platform.YOUTUBE:
        if homepage and "youtube.com" in homepage.lower():
            if "/live" not in homepage.lower():
                return homepage.rstrip("/") + "/live"
            return homepage
        if uid.startswith("UC") and len(uid) >= 20:
            return f"https://www.youtube.com/channel/{uid}/live"
        if uid.startswith("@"):
            return f"https://www.youtube.com/{uid}/live"
        return ""
    if platform == Platform.TIKTOK:
        if homepage and "tiktok.com/" in homepage.lower():
            match = re.search(r"tiktok\.com/@([^/?#]+)", homepage, flags=re.IGNORECASE)
            if match:
                return f"https://www.tiktok.com/@{match.group(1)}/live"
        if uid:
            return f"https://www.tiktok.com/@{uid.lstrip('@')}/live"
        return ""
    if platform == Platform.INSTAGRAM:
        if homepage and "instagram.com/" in homepage.lower():
            return homepage.rstrip("/") + "/live"
        return f"https://www.instagram.com/{uid.lstrip('@')}/live" if uid else ""
    if platform == Platform.FACEBOOK:
        if homepage and "facebook.com/" in homepage.lower():
            parsed = urlparse(homepage)
            path = parsed.path.rstrip("/").lower()
            if path.endswith("/live") or path.endswith("/live_videos"):
                return homepage
            if path == "/profile.php":
                query_id = re.search(r"(?:^|&)id=([^&]+)", parsed.query, flags=re.IGNORECASE)
                if query_id:
                    return f"https://www.facebook.com/{query_id.group(1)}/live"
            if parsed.netloc and "/watch" not in path and "/videos/" not in path and "/reel/" not in path:
                return f"https://{parsed.netloc}{parsed.path.rstrip('/')}/live"
        return f"https://www.facebook.com/{uid.lstrip('@')}/live" if uid else ""
    if platform == Platform.X:
        if homepage and ("x.com/" in homepage.lower() or "twitter.com/" in homepage.lower()):
            return homepage
        return f"https://x.com/{uid.lstrip('@')}" if uid else ""
    return homepage


def _hydrate_live_account_from_backend(platform: Platform, account_id: str, metadata: dict[str, Any]) -> str:
    """Fill a legacy accountId-only live request from the saved account row."""
    if not account_id:
        return ""
    try:
        settings = get_settings()
        client = CollectorBackendClient.from_settings(settings, timeout_seconds=10)
        client.max_attempts = 1
        items = client.active_accounts(platforms=[platform], account_types=["LIVE", "BOTH"])
    except Exception:
        # URL validation below produces the actionable request error. A
        # temporary business-backend lookup failure must not crash the API.
        return ""
    for item in items:
        candidate_id = _first_non_empty(item.get("accountId"), item.get("account_id"), item.get("id"))
        if candidate_id != str(account_id):
            continue
        resolved_account = dict(item)
        for key in ("uid", "username", "handle", "nickname", "homepageUrl", "homepage_url"):
            value = _first_non_empty(item.get(key))
            if value:
                metadata.setdefault(key, value)
        # Some business-backend versions expose only a display nickname in
        # /active.  Preserve any stable handle/homepage already supplied by
        # the UI and use it to avoid constructing e.g. TikTok /@Pai/live when
        # the actual saved handle is /@bigmouth556/live.
        resolved_account.update(metadata)
        return _stable_account_live_url(platform, resolved_account)
    return ""


def _normalize_live_job_payload(body: dict[str, Any], *, legacy_account_job: bool = False) -> dict[str, Any]:
    """Accept both the dedicated and the pre-migration account-job live contracts."""
    payload = dict(body)
    account = payload.get("account") if isinstance(payload.get("account"), dict) else {}
    if not account and isinstance(payload.get("accounts"), list) and payload["accounts"]:
        first_account = payload["accounts"][0]
        account = first_account if isinstance(first_account, dict) else {}

    platform_value = _first_non_empty(payload.get("platform"), account.get("platform"))
    if not platform_value:
        raise ValueError("platform is required for a live job")
    platform = Platform(_normalize_platform(platform_value))
    metadata = dict(payload.get("metadata") or {})
    account_id = _first_non_empty(
        metadata.get("accountId"), metadata.get("account_id"), payload.get("accountId"), payload.get("account_id"),
        account.get("accountId"), account.get("account_id"),
    )
    if account_id:
        metadata["accountId"] = account_id
    for key in ("uid", "externalId", "external_id", "username", "handle", "roomId", "room_id"):
        value = _first_non_empty(account.get(key), payload.get(key))
        if value:
            metadata.setdefault(key, value)

    # A numeric business accountId is the durable identity. Resolve it before
    # considering an old liveUrl/sourceUrl so a restart always discovers the
    # current TikTok/Facebook/YouTube broadcast instead of reusing yesterday's
    # watch URL. If the business lookup is temporarily unavailable, retain the
    # submitted URL as a compatibility fallback.
    url = _hydrate_live_account_from_backend(platform, account_id, metadata) if account_id else ""
    # A stable handle/homepage carried by the request is still authoritative
    # when the lookup endpoint is temporarily unavailable.  Only fall back to
    # a submitted liveUrl after these durable identity fields are exhausted.
    if not url:
        url = _stable_account_live_url(platform, metadata)
    if not url:
        url = _live_url_from_legacy_account(platform, account, payload)
    if not url:
        url = _stable_account_live_url(platform, metadata)
    if not url:
        raise ValueError("liveUrl/url is required; provide a stable accountId plus saved homepage/handle")

    options = payload.get("options") if isinstance(payload.get("options"), dict) else {}
    live_clip_seconds = _first_non_empty(
        payload.get("liveClipSeconds"), payload.get("live_clip_seconds"),
        options.get("videoSegmentSeconds"), options.get("audioSegmentSeconds"),
    )
    normalized: dict[str, Any] = {
        "platform": platform.value,
        "url": url,
        "collectComments": payload.get("collectComments", options.get("collectChat", False)),
        "downloadMedia": payload.get("downloadMedia", options.get("collectMedia", True)),
        "metadata": metadata,
        "runInBackground": payload.get("runInBackground", True),
        # A legacy jobType=LIVE request means continuous monitoring.  Dedicated
        # callers retain control through their explicit realtime flag.
        "realtime": payload.get("realtime", legacy_account_job),
        "maxRuntimeSeconds": payload.get("maxRuntimeSeconds") or payload.get("max_runtime_seconds") or (86400 if legacy_account_job else None),
    }
    if live_clip_seconds:
        try:
            parsed_clip_seconds = int(live_clip_seconds)
            normalized["liveClipSeconds"] = parsed_clip_seconds if parsed_clip_seconds >= 5 else 15
        except (TypeError, ValueError):
            normalized["liveClipSeconds"] = 15
    if payload.get("timeoutSeconds") or payload.get("timeout_seconds"):
        normalized["timeoutSeconds"] = payload.get("timeoutSeconds") or payload.get("timeout_seconds")
    upstream_task_id = _first_non_empty(payload.get("taskId"), metadata.get("taskId"), metadata.get("task_id"))
    if upstream_task_id:
        normalized["metadata"]["upstreamTaskId"] = upstream_task_id
    return normalized


def _has_live_account_url(body: dict[str, Any]) -> bool:
    """Recognize the old account payload when it already points to a live room."""
    accounts = body.get("accounts")
    if not isinstance(accounts, list):
        return False
    for item in accounts:
        if not isinstance(item, dict):
            continue
        url = _first_non_empty(item.get("liveUrl"), item.get("live_url"), item.get("sourceUrl"), item.get("source_url"))
        if "/live" in url.lower():
            return True
    return False


def _has_live_task_marker(body: dict[str, Any]) -> bool:
    """Recognize the business live-monitor markers on legacy account payloads."""
    metadata = body.get("metadata") if isinstance(body.get("metadata"), dict) else {}
    task_kind = _first_non_empty(
        body.get("taskKind"), body.get("task_kind"),
        metadata.get("taskKind"), metadata.get("task_kind"),
    ).upper()
    return (
        task_kind in {"LIVE", "LIVE_PROBE", "LIVE_CONTINUOUS"}
        or bool(body.get("realtime") or body.get("continuousLive") or body.get("continuous_live"))
    )


def _validate_realtime_live_account(request: LiveJobCreate) -> None:
    """Fail at submission time instead of creating an unpushable live task."""
    if not request.realtime or not get_settings().collector_sync_enabled:
        return
    account_id = (request.metadata or {}).get("accountId") or (request.metadata or {}).get("account_id")
    try:
        valid = int(str(account_id)) > 0
    except (TypeError, ValueError):
        valid = False
    if not valid:
        raise ValueError(
            "realtime live monitoring requires the downstream numeric accountId; "
            "a display name such as 'Pai' is not an accountId"
        )


def _failure_payload(exc: Exception) -> dict[str, Any]:
    """Normalize a worker exception before recovery threads can use it."""
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


def _is_live_source_not_started(request: LiveJobCreate, exc: Exception) -> bool:
    """Distinguish an offline/scheduled room from a collector failure.

    A live-account probe legitimately returns no media while the channel is
    offline or a broadcast is only scheduled.  Treating that response as an
    ACCESS_DENIED failure leaves the business UI in ``接入中`` forever when no
    stream row was created.  Authentication, network, and parser failures
    must remain hard failures so they are not hidden as a normal offline state.
    """
    text = str(exc).lower()
    target = str(request.url or "").lower()
    if request.platform == Platform.YOUTUBE:
        explicit_offline = "returned no collectable content" in text or "returned no live content" in text
    elif request.platform == Platform.TIKTOK:
        explicit_offline = (
            "returned no collectable live content" in text
            or "tiktok live has ended" in text
            or "not currently live" in text
            or "room is not live" in text
        )
    elif request.platform == Platform.FACEBOOK:
        explicit_offline = (
            "page is not currently live" in text
            or "did not expose a live video fragment" in text
            or "no live video fragment" in text
        )
    else:
        return False
    if not explicit_offline:
        return False
    # Account live entrypoints are authoritative for the current broadcast;
    # a stale watch URL is still reported as a source failure unless the
    # adapter explicitly says the platform reported the room offline.
    return "/live" in target or "/@" in target or "youtube.com" in target


class CollectorJobService:
    def __init__(
        self,
        *,
        base_dir: Path = Path("data/collector-jobs"),
        pipeline_output_dir: Path = Path("data/pipeline-runs"),
        recover_live_jobs: bool = False,
        reconcile_batch_jobs: bool = True,
        priority_watch_enabled: bool = False,
    ) -> None:
        self.base_dir = base_dir
        self.pipeline_output_dir = pipeline_output_dir
        self._jobs: dict[str, dict[str, Any]] = {}
        self._lock = threading.Lock()
        self._live_stop_events: dict[str, threading.Event] = {}
        self._account_slots = threading.BoundedSemaphore(ACCOUNT_JOB_MAX_CONCURRENT)
        self._account_platform_circuit_until: dict[Platform, tuple[float, str]] = {}
        # A platform may have one browser instance, but Chromium itself uses
        # several child processes.  Serialize browser-backed account work per
        # platform so two account jobs never open competing contexts/tabs at
        # the same time.  A YouTube account batch is additionally deferred
        # while the single configured profile owns a live relay.
        # Batch submissions from the business scheduler commonly arrive as a
        # keyword refresh followed immediately by an account refresh.  Keep
        # browser-backed work single-file per platform, but queue the latter
        # instead of rejecting it with 409.  LIVE deliberately does not take
        # these locks because its heartbeat/media loop has its own priority
        # and concurrency controls.
        self._batch_platform_locks = {platform: threading.Lock() for platform in Platform}
        # Priority checks are metadata-only (one newest content id, no media
        # or comments).  Keep them serialized among themselves, but do not
        # let a long-running bulk job hold the priority path hostage for its
        # entire lifetime.  Follow-up downloads still use the normal account
        # job path and its regular platform lock.
        self._priority_probe_platform_locks = {platform: threading.Lock() for platform in Platform}
        self._priority_watch_enabled = priority_watch_enabled
        self._priority_watch_state_path = self.base_dir.parent / "runtime" / "priority-account-watch.json"
        self._priority_watch_lock = threading.Lock()
        self._priority_polling_platforms: set[Platform] = set()
        self._priority_watch_stop = threading.Event()
        self._priority_watch_thread: threading.Thread | None = None
        if reconcile_batch_jobs:
            self._reconcile_interrupted_batch_jobs()
        if recover_live_jobs:
            self._recover_live_jobs()
        else:
            # A planned maintenance restart may deliberately disable live
            # recovery to release browser and relay capacity for video work.
            # Persisted LIVE jobs otherwise remain displayed as RUNNING even
            # though no worker owns them, and their stale state blocks normal
            # account/keyword scheduling on the next dispatcher tick.
            self._close_unrecovered_live_jobs()
        if self._priority_watch_enabled:
            self._priority_watch_thread = threading.Thread(
                target=self._run_priority_account_watch,
                name="priority-account-watch",
                daemon=True,
            )
            self._priority_watch_thread.start()

    def _load_priority_watch_state(self) -> dict[str, dict[str, Any]]:
        try:
            raw = json.loads(self._priority_watch_state_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
        entries = raw.get("accounts") if isinstance(raw, dict) else None
        if not isinstance(entries, dict):
            return {}
        return {str(key): dict(value) for key, value in entries.items() if isinstance(value, dict)}

    def _save_priority_watch_state(self, entries: dict[str, dict[str, Any]]) -> None:
        self._priority_watch_state_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"updatedAt": self._now(), "accounts": entries}
        temporary = self._priority_watch_state_path.with_suffix(".tmp")
        temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        temporary.replace(self._priority_watch_state_path)

    @staticmethod
    def _priority_watch_key(platform: Platform, account: str) -> str:
        return f"{platform.value}:{account.strip().lower()}"

    def _register_priority_accounts(self, request: AccountJobCreate) -> int:
        """Persist explicitly-prioritized accounts for independent polling."""
        if bool((request.metadata or {}).get("priorityWatchFollowup")):
            return 0
        settings = get_settings()
        registered = 0
        baseline_only = bool((request.metadata or {}).get("priorityWatchBaselineOnly"))
        with self._priority_watch_lock:
            entries = self._load_priority_watch_state()
            for target in request.accounts:
                if not target.priority:
                    continue
                normalized = self._normalize_account_target(target)
                if not _account_target_is_usable(normalized):
                    continue
                key = self._priority_watch_key(normalized.platform, normalized.account)
                if key not in entries and len(entries) >= max(1, int(settings.priority_account_watch_max_accounts)):
                    continue
                existing = dict(entries.get(key) or {})
                entries[key] = {
                    **existing,
                    "platform": normalized.platform.value,
                    "account": normalized.account,
                    "sourceUrl": normalized.source_url,
                    "displayName": target.display_name,
                    "enabled": True,
                    "intervalSeconds": max(60, int((request.metadata or {}).get("priorityPollIntervalSeconds") or settings.priority_account_watch_interval_seconds)),
                    "nextProbeAt": existing.get("nextProbeAt") or self._now(),
                    "knownContentIds": list(existing.get("knownContentIds") or [])[-50:],
                    # Bulk-registering a large set must not create a burst of
                    # historical downloads.  Its first probe establishes the
                    # current newest id; every later new id downloads media
                    # and comments immediately.
                    "bootstrap": bool(existing.get("bootstrap", baseline_only)),
                    "updatedAt": self._now(),
                }
                registered += 1
            self._save_priority_watch_state(entries)
        return registered

    def _exclude_priority_watch_accounts(self, accounts: list[AccountTargetCreate]) -> tuple[list[AccountTargetCreate], int]:
        """Keep independently watched accounts out of a scheduler bulk run.

        A scheduled 1,000-account job otherwise obtains the same platform
        browser lock for a selected account and makes its five-minute watcher
        wait behind the entire batch.  The watched path has its own discovery
        and follow-up job, so excluding it here prevents duplicate downloads
        and gives the fast lane real priority.
        """
        with self._priority_watch_lock:
            entries = self._load_priority_watch_state()
        filtered: list[AccountTargetCreate] = []
        excluded = 0
        for target in accounts:
            normalized = self._normalize_account_target(target)
            key = self._priority_watch_key(normalized.platform, normalized.account)
            if bool((entries.get(key) or {}).get("enabled", True)) and key in entries:
                excluded += 1
                continue
            filtered.append(target)
        return filtered, excluded

    @staticmethod
    def _priority_watch_due(value: Any, now: datetime) -> bool:
        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00")) <= now
        except ValueError:
            return True

    @staticmethod
    def _priority_watch_content_id_valid(platform: Platform, content_id: str) -> bool:
        """Reject account identifiers accidentally returned as media ids."""
        value = str(content_id or "").strip()
        if not value:
            return False
        # Only canonical eleven-character video ids are valid YouTube
        # timeline candidates.  Channel ids, handles and the literal
        # ``videos`` can be returned by a channel-page extractor and must
        # never poison the priority watch baseline.
        if platform == Platform.YOUTUBE:
            return bool(re.fullmatch(r"[A-Za-z0-9_-]{11}", value))
        return True

    @staticmethod
    def _priority_watch_failure_delay(platform: Platform, error: Exception, settings, fallback_seconds: int) -> int:
        """Use the platform cooldown when a priority probe is challenged.

        Five-minute freshness must not turn into a one-minute bot-challenge
        loop.  The fast retry remains for transient network/browser errors;
        auth and rate failures defer to the same session-protection windows
        used by normal account jobs.
        """
        delay = max(15, int(fallback_seconds))
        if not isinstance(error, CollectorError):
            return delay
        if error.code in {CollectorErrorCode.ACCESS_DENIED, CollectorErrorCode.PLATFORM_CHANGED}:
            # A deleted/private account or a changed page is not a transient
            # network fault. Keep it visible to operators but do not let a
            # large priority pool relaunch the same browser probe every
            # minute.
            return max(delay, 900)
        if platform != Platform.YOUTUBE:
            return delay
        if error.code == CollectorErrorCode.AUTH_EXPIRED:
            return max(delay, int(settings.youtube_auth_circuit_breaker_seconds or 0), 300)
        if error.code == CollectorErrorCode.RATE_LIMITED:
            return max(delay, int(settings.youtube_cookie_pool_cooldown_seconds or 0), 60)
        return delay

    def _update_priority_watch_entry(self, key: str, **changes: Any) -> dict[str, Any] | None:
        with self._priority_watch_lock:
            entries = self._load_priority_watch_state()
            entry = entries.get(key)
            if not entry:
                return None
            entry.update(changes)
            entry["updatedAt"] = self._now()
            entries[key] = entry
            self._save_priority_watch_state(entries)
            return dict(entry)

    def _run_priority_account_watch(self) -> None:
        """Probe registered priority accounts without turning bulk sweeps into a hot loop."""
        while not self._priority_watch_stop.wait(5):
            settings = get_settings()
            now = datetime.now(timezone.utc)
            with self._priority_watch_lock:
                entries = self._load_priority_watch_state()
            due = [
                (key, entry)
                for key, entry in entries.items()
                if entry.get("enabled", True) and self._priority_watch_due(entry.get("nextProbeAt"), now)
            ]
            # Do not let a slow/challenged platform hold every other priority
            # account hostage.  One metadata probe per platform may run at a
            # time; the platform-specific lock in `_poll_priority_account`
            # preserves same-platform serialization and browser safety.
            launched = 0
            for key, entry in due:
                if self._priority_watch_stop.is_set():
                    return
                if launched >= max(1, int(settings.priority_account_watch_max_accounts)):
                    break
                try:
                    platform = Platform(str(entry.get("platform") or ""))
                except ValueError:
                    self._update_priority_watch_entry(key, enabled=False, lastError="invalid platform")
                    continue
                with self._priority_watch_lock:
                    if platform in self._priority_polling_platforms:
                        continue
                    self._priority_polling_platforms.add(platform)
                threading.Thread(
                    target=self._poll_priority_account_background,
                    args=(key, entry, settings, platform),
                    name=f"priority-probe-{platform.value.lower()}",
                    daemon=True,
                ).start()
                launched += 1

    def _poll_priority_account_background(self, key: str, entry: dict[str, Any], settings, platform: Platform) -> None:
        try:
            self._poll_priority_account(key, entry, settings)
        finally:
            with self._priority_watch_lock:
                self._priority_polling_platforms.discard(platform)

    def _poll_priority_account(self, key: str, entry: dict[str, Any], settings) -> None:
        now = datetime.now(timezone.utc)
        interval = max(60, int(entry.get("intervalSeconds") or settings.priority_account_watch_interval_seconds))
        retry_seconds = max(15, int(settings.priority_account_watch_retry_seconds))
        platform = Platform(str(entry.get("platform") or ""))
        account = str(entry.get("account") or "").strip()
        if not account:
            self._update_priority_watch_entry(key, enabled=False, lastError="missing account")
            return
        pending_run_id = str(entry.get("pendingRunId") or "")
        pending_content_id = str(entry.get("pendingContentId") or "")
        if pending_run_id:
            try:
                pending = self.get_job(pending_run_id)
            except (KeyError, OSError, json.JSONDecodeError):
                pending = {}
            if pending.get("status") in ACTIVE_JOB_STATUSES:
                self._update_priority_watch_entry(key, nextProbeAt=(now + timedelta(seconds=retry_seconds)).isoformat())
                return
            synced = int(pending.get("contentsSynced") or ((pending.get("summary") or {}).get("contents_synced") or 0))
            known = list(entry.get("knownContentIds") or [])
            if (
                synced > 0
                and self._priority_watch_content_id_valid(platform, pending_content_id)
                and pending_content_id not in known
            ):
                known.append(pending_content_id)
            self._update_priority_watch_entry(
                key,
                knownContentIds=known[-50:],
                pendingRunId="",
                pendingContentId="",
                nextProbeAt=(now + timedelta(seconds=interval)).isoformat(),
                lastError="" if synced > 0 else f"follow-up job ended without sync: {pending.get('status') or 'missing'}",
            )
            return
        # A priority probe only asks the channel videos feed for its newest
        # content id; it neither opens a media download nor collects comments.
        # Blocking that cheap operation for the entire lifetime of an
        # unrelated YouTube live relay made priority accounts effectively
        # unmonitored whenever any live task was marked RUNNING.  The regular
        # per-platform lock below still serializes probes, and a full download
        # is scheduled only when a genuinely new id is found.
        lock = self._priority_probe_platform_locks[platform]
        if not lock.acquire(timeout=5):
            self._update_priority_watch_entry(key, nextProbeAt=(now + timedelta(seconds=retry_seconds)).isoformat(), lastError="platform collector busy")
            return
        try:
            target = AccountTargetCreate(
                platform=platform,
                account=account,
                sourceUrl=str(entry.get("sourceUrl") or ""),
                displayName=str(entry.get("displayName") or ""),
                priority=True,
            )
            probe = CollectionRequest(
                job_id=f"priority-probe-{uuid4()}",
                platform=platform,
                capability=Capability.ACCOUNT_TIMELINE,
                target=CollectionTarget(
                    platform=platform,
                    target_type="ACCOUNT",
                    target_value=account,
                    source_url=target.source_url,
                    capabilities=[Capability.ACCOUNT_TIMELINE],
                    metadata={
                        "priority_metadata_probe": True,
                        "youtube_incremental_account": True,
                        "youtube_prefer_cached_account": False,
                        "youtube_playlist_end": 1,
                    },
                ),
                limit=1,
                collect_comments=False,
                download_media=False,
            )
            batch = get_adapter(platform).discover(probe)
        except Exception as exc:  # noqa: BLE001 - a probe must never stop the watch loop.
            delay = self._priority_watch_failure_delay(platform, exc, settings, retry_seconds)
            self._update_priority_watch_entry(key, nextProbeAt=(now + timedelta(seconds=delay)).isoformat(), lastError=f"{type(exc).__name__}: {exc}"[:500])
            return
        finally:
            lock.release()
        known = {
            str(item)
            for item in (entry.get("knownContentIds") or [])
            if self._priority_watch_content_id_valid(platform, str(item))
        }
        candidate = next(
            (
                content
                for content in batch.contents
                if self._priority_watch_content_id_valid(platform, content.external_id)
                and content.external_id not in known
            ),
            None,
        )
        if candidate is None:
            self._update_priority_watch_entry(
                key,
                knownContentIds=list(known)[-50:],
                lastProbeAt=self._now(),
                nextProbeAt=(now + timedelta(seconds=interval)).isoformat(),
                lastError="",
            )
            return
        if entry.get("bootstrap"):
            known = list(entry.get("knownContentIds") or [])
            if candidate.external_id not in known:
                known.append(candidate.external_id)
            self._update_priority_watch_entry(
                key,
                knownContentIds=known[-50:],
                bootstrap=False,
                lastProbeAt=self._now(),
                lastDetectedContentId=candidate.external_id,
                lastDetectedAt=self._now(),
                nextProbeAt=(now + timedelta(seconds=interval)).isoformat(),
                lastError="",
            )
            return
        follow_up = AccountJobCreate(
            accounts=[target],
            limit=1,
            collectComments=True,
            downloadMedia=True,
            recencyDays=0,
            continueOnError=True,
            runInBackground=True,
            metadata={
                "taskKind": "PRIORITY_WATCH",
                "priorityWatchFollowup": True,
                "priorityWatchContentId": candidate.external_id,
                "priorityWatchSource": key,
                # A watch follow-up is intentionally one current video, not
                # a bulk sweep.  It is therefore allowed to open the detail
                # page and download uncached media; the probe above remains
                # metadata-only and cheap.
                "youtube_try_uncached_details": platform == Platform.YOUTUBE,
                "youtube_prefer_cached_account": False,
                "youtube_playlist_end": 1,
            },
        )
        job = self.create_account_job(follow_up)
        self._update_priority_watch_entry(
            key,
            pendingRunId=str(job.get("runId") or ""),
            pendingContentId=candidate.external_id,
            lastProbeAt=self._now(),
            lastDetectedContentId=candidate.external_id,
            lastDetectedAt=self._now(),
            nextProbeAt=(now + timedelta(seconds=retry_seconds)).isoformat(),
            lastError="",
        )

    def _reconcile_interrupted_batch_jobs(self) -> None:
        """Recover account work and close unrecoverable non-live batch jobs.

        Account jobs checkpoint each successfully synced account.  After a
        restart we can safely create a continuation containing only the
        accounts without a confirmed sync.  Keyword and home-feed jobs have
        no equivalent per-target checkpoint and therefore remain terminal.
        """
        if not self.base_dir.exists():
            return
        account_candidates: list[tuple[float, dict[str, Any]]] = []
        for path in self.base_dir.glob("*.json"):
            try:
                job = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if job.get("jobType") not in {"ACCOUNT", "KEYWORD", "HOME_FEED"}:
                continue
            if job.get("status") not in ACTIVE_JOB_STATUSES:
                continue
            if job.get("jobType") == "ACCOUNT":
                # Preserve an explicit stop request across a collector
                # restart.  A queued account job has no completed work to
                # recover and must not keep its platform lock alive.
                if job.get("cancelRequestedAt"):
                    now = self._now()
                    job.update(
                        {
                            "status": "CANCELLED",
                            "stage": "CANCELLED",
                            "finishedAt": now,
                            "updatedAt": now,
                            "progressMessage": "queued account collection remained cancelled after collector restart",
                        }
                    )
                    self._write_job_file(job)
                    continue
                try:
                    modified_at = path.stat().st_mtime
                except OSError:
                    modified_at = 0.0
                account_candidates.append((modified_at, job))
                continue
            has_progress = any(
                int(job.get(key) or 0) > 0
                for key in ("contentsCollected", "contentsSynced", "metadataSynced", "assetsCollected")
            )
            now = self._now()
            job.update(
                {
                    "status": "PARTIAL_SUCCESS" if has_progress else "FAILED",
                    "stage": "INTERRUPTED",
                    "finishedAt": now,
                    "updatedAt": now,
                    "failedReason": "COLLECTOR_RESTARTED: non-live batch worker ended before completion",
                    "failedCode": "COLLECTOR_RESTARTED",
                    "failedAction": "SUBMIT_BATCH_TASK_AGAIN",
                    "retryable": True,
                    "progressMessage": "previous collector process ended; previous progress was retained and the task was closed",
                }
            )
            self._write_job_file(job)

        # The scheduler may have submitted the same scope more than once just
        # before a restart.  Recover only the newest persisted version; its
        # continuation uses the original scope signature, so a new scheduler
        # tick still deduplicates against it.
        newest_by_scope: dict[str, tuple[float, dict[str, Any]]] = {}
        for candidate in account_candidates:
            scope = self._account_scope_signature_from_job(candidate[1])
            previous = newest_by_scope.get(scope)
            if previous is None or candidate[0] > previous[0]:
                newest_by_scope[scope] = candidate
        selected_ids = {id(job) for _, job in newest_by_scope.values()}
        for _, job in account_candidates:
            if id(job) in selected_ids:
                self._recover_interrupted_account_job(job)
            else:
                self._close_superseded_account_recovery(job)

    def _recover_interrupted_account_job(self, job: dict[str, Any]) -> None:
        """Queue the unconfirmed accounts of one interrupted account job."""
        run_id = str(job.get("runId") or "")
        recovery_count = int(job.get("recoveryCount") or 0)
        if recovery_count >= ACCOUNT_RECOVERY_MAX_ATTEMPTS:
            self._mark_account_recovery_exhausted(job)
            self._write_job_file(job)
            return
        try:
            request = AccountJobCreate.model_validate(job.get("request") or {})
        except Exception as exc:  # noqa: BLE001 - malformed persisted task is terminal.
            self._mark_account_recovery_exhausted(job, reason=f"cannot parse persisted account request: {exc}")
            self._write_job_file(job)
            return

        remaining = self._remaining_accounts_for_recovery(request, job)
        if not remaining:
            now = self._now()
            job.update(
                {
                    "status": "SUCCESS",
                    "stage": "RECOVERED",
                    "finishedAt": now,
                    "updatedAt": now,
                    "failedReason": None,
                    "failedCode": None,
                    "failedAction": None,
                    "retryable": False,
                    "progressMessage": "collector process restarted after every account had completed its confirmed sync",
                }
            )
            self._write_job_file(job)
            return

        original_scope = self._account_scope_signature_from_job(job) or self._account_scope_signature(request)
        metadata = dict(request.metadata or {})
        metadata.update({"recoveredFromRunId": run_id, "recoveryCount": recovery_count + 1})
        continuation_request = request.model_copy(
            update={"accounts": remaining, "metadata": metadata, "run_in_background": True}
        )
        # The parent must become terminal before creating the continuation,
        # otherwise normal account-scope deduplication returns the dead job.
        now = self._now()
        job.update(
            {
                "status": "PARTIAL_SUCCESS",
                "stage": "RECOVERY_QUEUED",
                "finishedAt": now,
                "updatedAt": now,
                "failedReason": "COLLECTOR_RESTARTED: remaining accounts were queued for recovery",
                "failedCode": "COLLECTOR_RESTARTED",
                "failedAction": "RECOVERY_QUEUED",
                "retryable": False,
                "recoveredAccountCount": len(remaining),
                "progressMessage": f"collector process restarted; queued recovery for {len(remaining)} unconfirmed account(s)",
            }
        )
        self._write_job_file(job)
        continuation = self.create_account_job(continuation_request)
        continuation_run_id = str(continuation.get("runId") or "")
        if continuation_run_id:
            # Keep the original full-scope identity.  A periodic scheduler
            # submission for the original account set will now deduplicate
            # against this continuation instead of opening a second batch.
            self._update_job(
                continuation_run_id,
                {
                    "accountScopeSignature": original_scope,
                    "recoveredFromRunId": run_id,
                    "recoveryCount": recovery_count + 1,
                    "recoveryOfAccountsTotal": len(request.accounts),
                },
            )
            job["recoveryRunId"] = continuation_run_id
            self._write_job_file(job)

    def _remaining_accounts_for_recovery(self, request: AccountJobCreate, job: dict[str, Any]) -> list[AccountTargetCreate]:
        checkpoints = job.get("accountCheckpoints") or ((job.get("summary") or {}).get("items") or [])
        completed: set[str] = set()
        for checkpoint in checkpoints:
            if not isinstance(checkpoint, dict) or checkpoint.get("status") != "success":
                continue
            platform = str(checkpoint.get("platform") or "").strip().upper()
            target = str(checkpoint.get("targetAccount") or "").strip().lower()
            if platform and target:
                completed.add(f"{platform}:{target}")
        remaining: list[AccountTargetCreate] = []
        for account in request.accounts:
            normalized = self._normalize_account_target(account)
            key = f"{normalized.platform.value}:{normalized.account.strip().lower()}"
            if key not in completed:
                remaining.append(account)
        return remaining

    def _close_superseded_account_recovery(self, job: dict[str, Any]) -> None:
        now = self._now()
        job.update(
            {
                "status": "PARTIAL_SUCCESS",
                "stage": "RECOVERY_SUPERSEDED",
                "finishedAt": now,
                "updatedAt": now,
                "failedReason": "COLLECTOR_RESTARTED: superseded by a newer interrupted task for the same account scope",
                "failedCode": "COLLECTOR_RESTARTED",
                "failedAction": "RECOVERED_BY_NEWER_TASK",
                "retryable": False,
                "progressMessage": "newer account task for the same scope was selected for restart recovery",
            }
        )
        self._write_job_file(job)

    def _mark_account_recovery_exhausted(self, job: dict[str, Any], *, reason: str = "restart recovery limit reached") -> None:
        now = self._now()
        job.update(
            {
                "status": "FAILED",
                "stage": "RECOVERY_EXHAUSTED",
                "finishedAt": now,
                "updatedAt": now,
                "failedReason": f"COLLECTOR_RESTARTED: {reason}",
                "failedCode": "COLLECTOR_RESTARTED",
                "failedAction": "SUBMIT_ACCOUNT_TASK_AGAIN",
                "retryable": True,
                "progressMessage": "automatic restart recovery reached its safety limit; submit a fresh account task",
            }
        )

    def _recover_live_jobs(self) -> None:
        """Resume fresh live jobs after a collector process restart."""
        if not self.base_dir.exists():
            return
        now = time.time()
        candidates: list[tuple[float, Path, dict[str, Any]]] = []
        for path in self.base_dir.glob("live-*.json"):
            try:
                job = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if job.get("status") not in ACTIVE_JOB_STATUSES:
                continue
            request_payload = job.get("request") or {}
            if not request_payload.get("realtime"):
                continue
            try:
                mtime = path.stat().st_mtime
            except OSError:
                continue
            if now - mtime <= LIVE_RECOVERY_WINDOW_SECONDS:
                candidates.append((mtime, path, job))
            else:
                self._mark_recovered_stale(job, "collector process restarted after heartbeat expired")
                self._write_job_file(job)

        # Deduplicate only multiple persisted submissions for the *same live
        # source*. The former platform-wide grouping allowed the newest
        # YouTube channel to evict every other YouTube relay after a restart;
        # their old HLS manifests then stopped advancing and appeared frozen.
        newest_by_source: dict[str, tuple[float, Path, dict[str, Any]]] = {}
        for candidate in candidates:
            source_key = self._live_recovery_source_key(candidate[2])
            previous = newest_by_source.get(source_key)
            if previous is None or candidate[0] > previous[0]:
                newest_by_source[source_key] = candidate
        selected_candidates = sorted(newest_by_source.values(), key=lambda item: item[0], reverse=True)
        selected_candidates = selected_candidates[:LIVE_RECOVERY_MAX_CONCURRENT]
        selected = {id(item[2]) for item in selected_candidates}
        for _, _, job in candidates:
            if id(job) not in selected:
                self._mark_recovered_stale(job, "superseded by a newer task for the same source or recovery capacity limit")
                self._write_job_file(job)

        for _, _, job in selected_candidates:
            run_id = str(job.get("runId") or "")
            try:
                request = LiveJobCreate.model_validate(job.get("request") or {})
            except Exception as exc:  # noqa: BLE001 - corrupt recovery input is terminal.
                self._mark_recovered_stale(job, f"cannot resume persisted live request: {exc}")
                self._write_job_file(job)
                continue
            request = request.model_copy(update={"run_in_background": True})
            job["recoveryCount"] = int(job.get("recoveryCount") or 0) + 1
            job["progressMessage"] = "collector process restarted; resuming live task"
            job["recoveredAt"] = self._now()
            self._jobs[run_id] = dict(job)
            self._write_job_file(job)
            thread = threading.Thread(
                target=lambda run_id=run_id, request=request: self._run_live_job(run_id, request),
                name=f"collector-recovery-{run_id}",
                # A live loop must not hold systemd shutdown hostage. The
                # persisted job is recovered on the next API start and the
                # downstream heartbeat already has its own liveness timeout.
                daemon=True,
            )
            thread.start()

    def _close_unrecovered_live_jobs(self) -> None:
        """Terminally close orphaned live jobs when restart recovery is disabled."""
        if not self.base_dir.exists():
            return
        now = self._now()
        for path in self.base_dir.glob("live-*.json"):
            try:
                job = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if job.get("status") not in ACTIVE_JOB_STATUSES:
                continue
            job.update(
                {
                    "status": "CANCELLED",
                    "stage": "CANCELLED",
                    "finishedAt": now,
                    "updatedAt": now,
                    "failedReason": "COLLECTOR_RESTARTED: live recovery disabled during video-collection maintenance",
                    "failedCode": "LIVE_RECOVERY_DISABLED",
                    "failedAction": "SUBMIT_LIVE_TASK_AGAIN",
                    "retryable": False,
                    "progressMessage": "live task closed during video-collection maintenance; submit again when live monitoring is re-enabled",
                }
            )
            self._write_job_file(job)

    @staticmethod
    def _live_recovery_source_key(job: dict[str, Any]) -> str:
        """Return a stable identity for one monitored live source."""
        request = job.get("request") or {}
        metadata = request.get("metadata") or {}
        platform = str(request.get("platform") or "").strip().upper()
        account = str(
            metadata.get("accountId")
            or metadata.get("account_id")
            or metadata.get("channelId")
            or metadata.get("channel_id")
            or metadata.get("username")
            or metadata.get("handle")
            or ""
        ).strip().lower()
        if account:
            return f"{platform}:account:{account}"
        source = str(request.get("url") or request.get("sourceUrl") or "").strip()
        parsed = urlparse(source)
        normalized = f"{parsed.netloc.lower()}{parsed.path.rstrip('/').lower()}" if parsed.netloc else source.lower().rstrip("/")
        return f"{platform}:url:{normalized}"

    def _mark_recovered_stale(self, job: dict[str, Any], reason: str) -> None:
        now = self._now()
        job.update(
            {
                "status": "FAILED",
                "stage": "FAILED",
                "finishedAt": now,
                "updatedAt": now,
                "failedReason": f"COLLECTOR_RESTARTED: {reason}",
                "failedCode": "COLLECTOR_RESTARTED",
                "failedAction": "SUBMIT_LIVE_TASK_AGAIN",
                "retryable": True,
                "progressMessage": "previous collector process ended; submit or recover a new live task",
            }
        )

    def create_keyword_job(self, request: KeywordJobCreate) -> dict[str, Any]:
        run_id = self._new_run_id("keyword")
        job = self._initial_job(run_id, "KEYWORD", request.model_dump(by_alias=True))
        job["signature"] = self._job_signature("KEYWORD", request.model_dump(by_alias=True, exclude={"run_in_background"}))
        job["lockedPlatforms"] = sorted(platform.value for platform in request.platforms)
        # Replenishment jobs are admitted as a durable low-priority queue.
        # They wait for existing account work below instead of receiving a
        # 409 for every platform that currently has a scheduled account batch.
        low_priority_queue = bool((request.metadata or {}).get("lowPriorityQueue"))
        existing = self._active_job_for(
            job["signature"],
            set(job["lockedPlatforms"]),
            allow_account_overlap=low_priority_queue,
        )
        if existing:
            return existing
        self._save_job(job)
        self._run_or_spawn(job, request.run_in_background, lambda: self._run_keyword_job(run_id, request))
        return self.get_job(run_id)

    def create_account_job(self, request: AccountJobCreate) -> dict[str, Any]:
        # Upstream account rows sometimes carry a display name (or a
        # synthetic ``__starter__...`` placeholder) in ``account`` while the
        # real profile URL is in ``sourceUrl``.  Normalize from the URL and
        # drop entries that cannot form a platform account URL before they
        # consume a worker slot and are reported as platform failures.
        normalized_accounts = []
        for account in request.accounts:
            normalized = self._normalize_account_target(account)
            # Keep the original YouTube row so its display-name metadata can
            # still be derived later in _run_account_job.  The runtime
            # normalizer already resolves its durable @handle/channel ID.
            candidate = account if account.platform == Platform.YOUTUBE else normalized
            if _account_target_is_usable(candidate):
                normalized_accounts.append(candidate)
        request = request.model_copy(update={"accounts": normalized_accounts})
        if not request.accounts:
            raise ValueError("at least one account is required")
        settings = get_settings()
        if _is_scheduled_account_bulk(request):
            if not bool(getattr(settings, "collector_scheduled_bulk_enabled", False)):
                priority_accounts = [account for account in request.accounts if account.priority]
                suppressed = len(request.accounts) - len(priority_accounts)
                if not priority_accounts:
                    # Return a terminal acknowledgement instead of creating
                    # a PENDING job that reserves a browser slot forever.
                    # Keyword replenishment is the selected expansion path.
                    return {
                        "jobType": "ACCOUNT",
                        "status": "CANCELLED",
                        "stage": "BULK_ACCOUNT_SUSPENDED",
                        "accountsTotal": 0,
                        "suppressedAccounts": suppressed,
                        "progressMessage": "scheduled bulk account sweep is suspended; keyword replenishment is active",
                    }
                metadata = dict(request.metadata or {})
                metadata["suppressedBulkAccounts"] = suppressed
                request = request.model_copy(update={"accounts": priority_accounts, "metadata": metadata})
            filtered_accounts, excluded_accounts = self._exclude_priority_watch_accounts(request.accounts)
            if excluded_accounts:
                metadata = dict(request.metadata or {})
                metadata["priorityWatchExcludedAccounts"] = excluded_accounts
                request = request.model_copy(update={"accounts": filtered_accounts, "metadata": metadata})
            if not request.accounts:
                return {
                    "jobType": "ACCOUNT",
                    "status": "SUCCESS",
                    "stage": "SKIPPED_PRIORITY_WATCH",
                    "accountsTotal": 0,
                    "priorityWatchExcludedAccounts": excluded_accounts,
                    "progressMessage": "all scheduled accounts are independently watched by the priority pool",
                }
        # ``priority`` is durable monitoring intent, not merely an ordering
        # hint: retain it locally so the priority watcher can poll this small
        # set independently of later bulk scheduler submissions.
        register_only = bool((request.metadata or {}).get("priorityWatchRegisterOnly"))
        if register_only and any(not account.priority for account in request.accounts):
            raise ValueError("priority watch registration requires priority=true for every account")
        registered_priority_accounts = self._register_priority_accounts(request)
        if register_only:
            if not request.accounts:
                raise ValueError("priority watch registration requires priority=true for every account")
            return {
                "jobType": "PRIORITY_WATCH_REGISTRATION",
                "status": "SUCCESS",
                "accountsRegistered": registered_priority_accounts,
                "accountsRequested": len(request.accounts),
                "bootstrapOnly": bool((request.metadata or {}).get("priorityWatchBaselineOnly")),
            }
        # A new/analyst-selected account must never wait behind a thousand
        # existing rows in the same scheduler submission.  Preserve the
        # relative order inside each group, and leave ordinary submissions
        # byte-for-byte behaviourally identical.
        if _is_scheduled_account_bulk(request) and any(account.priority for account in request.accounts):
            request = request.model_copy(
                update={
                    "accounts": [
                        *[account for account in request.accounts if account.priority],
                        *[account for account in request.accounts if not account.priority],
                    ]
                }
            )
        run_id = self._new_run_id("account")
        job = self._initial_job(run_id, "ACCOUNT", request.model_dump(by_alias=True))
        job["accountsTotal"] = len(request.accounts)
        job["signature"] = self._job_signature("ACCOUNT", request.model_dump(by_alias=True, exclude={"run_in_background"}))
        # Keep a second, stable signature specifically for scheduler-driven
        # account refreshes.  It intentionally ignores per-submission IDs and
        # timestamps, while retaining all collection options and meaningful
        # metadata such as comment limits or profile selection.
        job["accountScopeSignature"] = self._account_scope_signature(request)
        job["lockedPlatforms"] = sorted({self._effective_account_platform(account).value for account in request.accounts})
        # Account refreshes can wait behind a keyword refresh for the same
        # platform.  Returning 409 here made the scheduler permanently drop
        # its second, account-oriented task on every refresh cycle.
        existing = self._active_account_job_for_scope(job["accountScopeSignature"])
        if existing:
            return existing
        self._save_job(job)
        self._run_or_spawn(job, request.run_in_background, lambda: self._run_account_job(run_id, request))
        return self.get_job(run_id)

    def stop_account_job(self, run_id: str) -> dict[str, Any]:
        """Request an orderly stop after the current account unit completes."""
        with self._lock:
            job = self._jobs.get(run_id)
            if not job:
                raise KeyError(run_id)
            if job.get("jobType") != "ACCOUNT":
                raise ValueError("only ACCOUNT jobs can be stopped through this endpoint")
            if job.get("status") not in ACTIVE_JOB_STATUSES:
                return dict(job)
            if job.get("status") == "PENDING":
                # This batch has not acquired a worker or touched an account;
                # release its platform reservation immediately.  Keep the
                # cancellation marker as well: a worker can already have
                # passed its status update and be waiting on the browser lock
                # when this request arrives.  Without the marker that worker
                # could overwrite CANCELLED and collect an unintended account.
                now = self._now()
                job.update(
                    {
                        "status": "CANCELLED",
                        "stage": "CANCELLED",
                        "cancelRequestedAt": now,
                        "finishedAt": now,
                        "updatedAt": now,
                        "progressMessage": "queued account collection cancelled before a collector slot was acquired",
                    }
                )
                self._write_job_file(job)
                return dict(job)
        now = self._now()
        self._update_job(
            run_id,
            {
                "cancelRequestedAt": now,
                "stage": "CANCELLING",
                "progressMessage": "stop requested; collector will finish its current account and exit",
            },
        )
        return self.get_job(run_id)

    def _account_stop_requested(self, run_id: str) -> bool:
        with self._lock:
            job = self._jobs.get(run_id)
            return bool(job and job.get("cancelRequestedAt"))

    def create_home_feed_job(self, request: HomeFeedJobCreate) -> dict[str, Any]:
        if not request.platforms:
            raise ValueError("at least one platform is required")
        task_id = _home_feed_task_id(request)
        existing = self._job_for_home_feed_task(task_id)
        if existing:
            duplicate = dict(existing)
            duplicate["deduplicated"] = True
            return duplicate
        run_id = self._new_run_id("home-feed")
        job = self._initial_job(run_id, "HOME_FEED", request.model_dump(by_alias=True))
        job["taskId"] = task_id
        job["platformsTotal"] = len(request.platforms)
        job["signature"] = self._job_signature("HOME_FEED", request.model_dump(by_alias=True, exclude={"run_in_background"}))
        job["lockedPlatforms"] = sorted(platform.value for platform in request.platforms)
        active = self._active_job_for(job["signature"], set(job["lockedPlatforms"]))
        if active:
            return active
        self._save_job(job)
        self._run_or_spawn(job, request.run_in_background, lambda: self._run_home_feed_job(run_id, request, task_id))
        return self.get_job(run_id)

    def create_live_job(self, request: LiveJobCreate) -> dict[str, Any]:
        run_id = self._new_run_id("live")
        job = self._initial_job(run_id, "LIVE", request.model_dump(by_alias=True))
        job["contentsTotal"] = 1
        job["signature"] = self._job_signature("LIVE", request.model_dump(by_alias=True, exclude={"run_in_background"}))
        job["lockedPlatforms"] = [request.platform.value]
        job["liveSourceKey"] = self._live_recovery_source_key(job)
        # A live probe has its own bounded media/heartbeat loop and must be
        # able to run while an account timeline or keyword refresh is using the
        # same platform. Keep duplicate LIVE requests idempotent, but do not
        # let an unrelated batch job block real-time monitoring.
        # Backend task IDs and requestedAt change on every polling cycle, so
        # the full request signature cannot identify a repeated LIVE request.
        # Deduplicate by the stable account/source identity first; otherwise
        # one account can start multiple FFmpeg relays and starve every live
        # player on a small collector.
        existing = self._active_live_job_for_source(job["liveSourceKey"])
        if existing:
            return existing
        existing = self._active_job_for(
            job["signature"],
            set(job["lockedPlatforms"]),
            allow_live_overlap=True,
        )
        if existing:
            return existing
        self._save_job(job)
        self._run_or_spawn(job, request.run_in_background, lambda: self._run_live_job(run_id, request))
        return self.get_job(run_id)

    def get_job(self, run_id: str) -> dict[str, Any]:
        with self._lock:
            job = self._jobs.get(run_id)
        if job:
            return dict(job)
        path = self._job_path(run_id)
        if not path.exists():
            raise KeyError(run_id)
        return json.loads(path.read_text(encoding="utf-8"))

    def stop_live_job(self, run_id: str) -> dict[str, Any]:
        """Request an orderly stop for one persistent live relay."""
        with self._lock:
            job = self._jobs.get(run_id)
            if not job:
                raise KeyError(run_id)
            if job.get("jobType") != "LIVE":
                raise ValueError("only LIVE jobs can be stopped through this endpoint")
            if job.get("status") not in ACTIVE_JOB_STATUSES:
                return dict(job)
            stop_event = self._live_stop_events.setdefault(run_id, threading.Event())
            stop_event.set()
        self._update_job(
            run_id,
            {
                "stage": "STOPPING",
                "progressMessage": "stop requested; closing live relay",
                "retryable": False,
            },
        )
        return self.get_job(run_id)

    def _job_for_home_feed_task(self, task_id: str) -> dict[str, Any] | None:
        with self._lock:
            for job in self._jobs.values():
                if job.get("jobType") == "HOME_FEED" and job.get("taskId") == task_id:
                    return dict(job)
        if not self.base_dir.exists():
            return None
        for path in sorted(self.base_dir.glob("home-feed-*.json"), key=lambda item: item.stat().st_mtime, reverse=True):
            try:
                job = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue
            if job.get("jobType") == "HOME_FEED" and job.get("taskId") == task_id:
                return job
        return None

    def tail_logs(self, run_id: str, *, tail: int) -> list[str]:
        log_path = self._log_path(run_id)
        if not log_path.exists():
            return []
        lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
        return lines[-max(1, min(tail, 1000)) :]

    def _run_or_spawn(self, job: dict[str, Any], run_in_background: bool, target) -> None:
        if run_in_background:
            thread = threading.Thread(
                target=target,
                name=f"collector-job-{job['runId']}",
                daemon=(job.get("jobType") == "LIVE"),
            )
            thread.start()
            return
        target()

    def _run_keyword_job(self, run_id: str, request: KeywordJobCreate) -> None:
        self._mark_running(run_id)
        metadata = dict(request.metadata or {})
        if request.recency_months is not None:
            metadata["keyword_recency_months"] = request.recency_months
        if request.keyword_ids:
            metadata["keyword_ids"] = request.keyword_ids
        result: dict[str, Any] = {}
        try:
            if bool(metadata.get("lowPriorityQueue")):
                self._wait_for_account_priority(run_id, request.platforms)
            with self._batch_platform_lock(run_id, request.platforms, job_label="keyword refresh"):
                result = run_pipeline(
                    PipelineOptions(
                        platforms=request.platforms,
                        limit_per_keyword=request.limit,
                        collect_comments=request.collect_comments,
                        download_media=request.download_media,
                        continue_on_error=request.continue_on_error,
                        parallel_platforms=request.parallel_platforms,
                        output_dir=self.pipeline_output_dir,
                        run_id=run_id,
                        metadata=metadata,
                    ),
                    output=self._log_writer(run_id),
                )
            self._finish_keyword_job(run_id, result)
        except Exception as exc:  # noqa: BLE001 - job API must persist terminal failure.
            self._fail_job(run_id, type(exc).__name__, str(exc), result)
        finally:
            self._cleanup_after_job(run_id)

    def _run_account_job(self, run_id: str, request: AccountJobCreate) -> None:
        slot_acquired = False
        try:
            self._update_job(
                run_id,
                {
                    "status": "PENDING",
                    "stage": "QUEUED",
                    "progressMessage": f"waiting for account collector slot ({ACCOUNT_JOB_MAX_CONCURRENT} max; live jobs have priority)",
                },
            )
            self._wait_for_youtube_live_priority(run_id, request)
            # Backpressure happens before any adapter/browser is created.  A
            # burst of requests therefore consumes a few waiting Python
            # threads, not a burst of Chromium processes.
            self._account_slots.acquire()
            slot_acquired = True
            if self._account_stop_requested(run_id):
                self._update_job(
                    run_id,
                    {
                        "status": "CANCELLED",
                        "stage": "CANCELLED",
                        "finishedAt": self._now(),
                        "progressMessage": "account collection stopped before a collector slot was used",
                    },
                )
                return
            self._mark_running(run_id)
            self._update_job(
                run_id,
                {
                    "status": "RUNNING",
                    "progressMessage": "account collector slot acquired; processing accounts",
                },
            )
            settings = get_settings()
            started = datetime.now(timezone.utc)
            items: list[dict[str, Any]] = []
            totals = {
                "accountsProcessed": 0,
                "accountsCollected": 0,
                "contentsCollected": 0,
                "commentsCollected": 0,
                "assetsCollected": 0,
                "contentsSynced": 0,
                "metadataSynced": 0,
                "mediaSynced": 0,
                "mediaUnavailable": 0,
                "agentSucceeded": 0,
                "agentFailed": 0,
            }
            failed_reason = None
            failed_count = 0
            zero_sync_count = 0
            cancelled = False
            # A platform-wide circuit breaker is a safety valve for a large
            # scheduled refresh: it prevents hundreds of yt-dlp detail
            # requests from extending a YouTube bot challenge.  It must not
            # turn into an outage for an operator's one-account request.  A
            # manual request is still serialized by the normal platform lock
            # and makes only its configured, bounded attempt.
            is_bulk_refresh = len(request.accounts) > 1
            is_scheduled_bulk = _is_scheduled_account_bulk(request)
            if is_scheduled_bulk:
                self._update_job(
                    run_id,
                    {
                        "collectionProfile": "SCHEDULED_INCREMENTAL",
                        "progressMessage": "scheduled bulk refresh: incremental media, comments deferred, paced by account",
                    },
                )
            for account_index, account in enumerate(request.accounts, start=1):
                if self._account_stop_requested(run_id):
                    cancelled = True
                    self._append_log(run_id, "[ACCOUNT] stop requested; ending after completed account checkpoints")
                    break
                # A live room may have started after this task was admitted.
                # Check between accounts (never while an account is mid-sync)
                # so an automated refresh cannot burn the login session that
                # keeps a realtime relay alive.
                if account_index > 1:
                    self._wait_for_youtube_live_priority(run_id, request)
                totals["accountsProcessed"] += 1
                normalized_account = self._normalize_account_target(account)
                display_name = _account_display_name_from_target(account, normalized_account.account) or _account_display_name_for_sync(normalized_account.platform, account.account, normalized_account.account)
                account_label = display_name or account.account.strip() or normalized_account.account
                circuit_reason = (
                    self._account_platform_circuit_reason(normalized_account.platform)
                    if is_bulk_refresh
                    else ""
                )
                if circuit_reason:
                    failed_count += 1
                    failed_reason = circuit_reason
                    item = {
                        "platform": normalized_account.platform.value,
                        "account": account_label,
                        "targetAccount": normalized_account.account,
                        "status": "skipped",
                        "reason": circuit_reason,
                        "failedReason": circuit_reason,
                    }
                    items.append(item)
                    self._checkpoint_account_result(run_id, item)
                    self._append_log(
                        run_id,
                        f"[ACCOUNT] skipped {normalized_account.platform.value.lower()} "
                        f"account={account_label} reason={circuit_reason}",
                    )
                    self._update_job(
                        run_id,
                        {
                            "status": "RUNNING",
                            "currentAccount": account_label,
                            "currentTargetAccount": normalized_account.account,
                            "accountsProcessed": totals["accountsProcessed"],
                            "failedAccounts": failed_count,
                            "progressMessage": f"skipped account {totals['accountsProcessed']}/{len(request.accounts)}: {circuit_reason}",
                        },
                    )
                    continue
                self._update_job(
                    run_id,
                    {
                        "status": "RUNNING",
                        "stage": "COLLECTING",
                        "currentAccount": account_label,
                        "currentTargetAccount": normalized_account.account,
                        "accountsProcessed": totals["accountsProcessed"],
                        "failedAccounts": failed_count,
                        "progressMessage": f"collecting account {totals['accountsProcessed']}/{len(request.accounts)}: {account_label}",
                    },
                )
                self._append_log(run_id, f"[ACCOUNT] start {normalized_account.platform.value.lower()} account={account_label} target={normalized_account.account}")
                try:
                    collection_profile = _account_collection_profile(
                        normalized_account.platform,
                        request,
                        settings,
                        account=account,
                    )
                    collection_limit = _account_collection_limit(request, settings, collection_profile)
                    metadata = _account_collection_metadata(normalized_account.platform, request, collection_limit)
                    metadata.update(collection_profile["metadata"])
                    if display_name:
                        metadata.setdefault("account_display_name", display_name)
                    if normalized_account.platform == Platform.YOUTUBE and normalized_account.account.startswith("@"):
                        metadata.setdefault("account_handle", normalized_account.account)
                    collection_request = CollectionRequest(
                        job_id=str(uuid4()),
                        platform=normalized_account.platform,
                        capability=Capability.ACCOUNT_TIMELINE,
                        target=CollectionTarget(
                            platform=normalized_account.platform,
                            target_type="ACCOUNT",
                            target_value=normalized_account.account,
                            source_url=normalized_account.source_url,
                            capabilities=[Capability.ACCOUNT_TIMELINE],
                            metadata=metadata,
                        ),
                        limit=collection_limit,
                        collect_comments=collection_profile["collectComments"],
                        download_media=request.download_media,
                    )
                    # A scheduled sweep must not keep one of the global account
                    # slots while yielding to a priority account.  Holding that
                    # slot here created a circular wait: two bulk jobs could
                    # occupy both slots and wait for priority jobs, while those
                    # priority jobs were themselves waiting for a free slot.
                    # This point is deliberately before the per-platform lock,
                    # so releasing the global slot cannot leave a browser lock
                    # orphaned.
                    if is_scheduled_bulk:
                        self._account_slots.release()
                        slot_acquired = False
                        try:
                            self._wait_for_queued_priority_account(
                                run_id,
                                request,
                                normalized_account.platform,
                            )
                        finally:
                            self._account_slots.acquire()
                            slot_acquired = True
                    # Only the browser-backed discovery is serialized.  The
                    # downstream sync below can proceed while another
                    # platform is discovering its next account.
                    with self._batch_platform_lock(
                        run_id,
                        [normalized_account.platform],
                        job_label=f"account {account_label}",
                    ):
                        adapter = get_adapter(normalized_account.platform)
                        batch = adapter.discover(collection_request)
                    batch, recency_filter = _filter_account_batch_by_recency(batch, _account_recency_cutoff(request, metadata))
                    self._update_job(
                        run_id,
                        {
                            "status": "RUNNING",
                            "currentAccount": account_label,
                            "currentTargetAccount": normalized_account.account,
                            "accountsProcessed": totals["accountsProcessed"],
                            "contentsCollected": totals["contentsCollected"] + len(batch.contents),
                            "commentsCollected": totals["commentsCollected"] + len(batch.comments),
                            "assetsCollected": totals["assetsCollected"] + len(batch.assets),
                            "progressMessage": f"collected account={account_label} contents={len(batch.contents)} assets={len(batch.assets)}; syncing downstream",
                        },
                    )
                    self._append_log(run_id, f"[ACCOUNT] collected {normalized_account.platform.value.lower()} account={account_label} target={normalized_account.account} contents={len(batch.contents)} assets={len(batch.assets)}")
                    circuit = batch.raw.get("account_platform_circuit") if isinstance(batch.raw, dict) else None
                    if (
                        is_bulk_refresh
                        and isinstance(circuit, dict)
                        and str(circuit.get("code") or "") in {CollectorErrorCode.AUTH_EXPIRED.value, CollectorErrorCode.RATE_LIMITED.value}
                    ):
                        try:
                            circuit_code = CollectorErrorCode(str(circuit["code"]))
                            circuit_error = CollectorError(circuit_code, str(circuit.get("reason") or circuit_code.value), retryable=False)
                            if self._should_open_account_platform_circuit(normalized_account.platform, circuit_error):
                                circuit_reason = self._open_account_platform_circuit(normalized_account.platform, circuit_error)
                                failed_count += 1
                                failed_reason = circuit_reason
                                self._append_log(run_id, f"[ACCOUNT] circuit_open platform={normalized_account.platform.value.lower()} reason={circuit_reason}")
                        except (KeyError, ValueError):
                            pass
                    backend_sync = {"items": []}
                    if settings.collector_sync_enabled:
                        backend_sync = sync_batch_to_backend(batch, self._account_sync_request(normalized_account, request, metadata), settings=settings)
                    synced = len(backend_sync.get("items") or [])
                    # Zero is meaningful for metadata-only recovery: content
                    # can be synced while no video object was uploaded.
                    metadata_value = backend_sync.get("metadataSynced")
                    media_value = backend_sync.get("mediaSynced")
                    metadata_synced = int(synced if metadata_value is None else metadata_value)
                    media_synced = int(synced if media_value is None else media_value)
                    media_unavailable = int(backend_sync.get("mediaUnavailable") or 0)
                    agent_ingest = backend_sync.get("agent_ingest") or {}
                    agent_items = agent_ingest.get("items") or []
                    agent_succeeded = sum(1 for item in agent_items if isinstance(item, dict) and item.get("status") == "SUCCESS")
                    agent_failed = sum(1 for item in agent_items if isinstance(item, dict) and item.get("status") == "FAILED")
                    totals["accountsCollected"] += 1
                    totals["contentsCollected"] += len(batch.contents)
                    totals["commentsCollected"] += len(batch.comments)
                    totals["assetsCollected"] += len(batch.assets)
                    totals["contentsSynced"] += synced
                    totals["metadataSynced"] += metadata_synced
                    totals["mediaSynced"] += media_synced
                    totals["mediaUnavailable"] += media_unavailable
                    totals["agentSucceeded"] += agent_succeeded
                    totals["agentFailed"] += agent_failed
                    sync_skip_reason = _account_sync_skip_reason(batch, synced, settings)
                    if synced == 0:
                        zero_sync_count += 1
                    # Persist the post-sync totals immediately.  The previous
                    # update was emitted before the backend call, leaving the
                    # job header at zero until a later account or terminal
                    # summary even though content had already been delivered.
                    self._update_job(
                        run_id,
                        {
                            "status": "RUNNING",
                            "stage": "SYNCED",
                            "currentAccount": account_label,
                            "currentTargetAccount": normalized_account.account,
                            "accountsProcessed": totals["accountsProcessed"],
                            "accountsCollected": totals["accountsCollected"],
                            "contentsCollected": totals["contentsCollected"],
                            "commentsCollected": totals["commentsCollected"],
                            "assetsCollected": totals["assetsCollected"],
                            "contentsSynced": totals["contentsSynced"],
                            "metadataSynced": totals["metadataSynced"],
                            "mediaSynced": totals["mediaSynced"],
                            "mediaUnavailable": totals["mediaUnavailable"],
                            "agentSucceeded": totals["agentSucceeded"],
                            "agentFailed": totals["agentFailed"],
                            "failedAccounts": failed_count,
                            "progressMessage": f"synced account={account_label} contents={len(batch.contents)} synced={synced}",
                        },
                    )
                    item = {
                        "platform": normalized_account.platform.value,
                        "account": account_label,
                        "targetAccount": normalized_account.account,
                        "status": "success" if synced > 0 else "collected_without_sync",
                        "counts": {
                            "accounts": len(batch.accounts),
                            "contents": len(batch.contents),
                            "comments": len(batch.comments),
                            "assets": len(batch.assets),
                            "synced": synced,
                            "metadataSynced": metadata_synced,
                            "mediaSynced": media_synced,
                            "mediaUnavailable": media_unavailable,
                            "agentSucceeded": agent_succeeded,
                            "agentFailed": agent_failed,
                        },
                        "sync_skip_reason": sync_skip_reason,
                        "skipSyncReason": sync_skip_reason,
                        "recency_filter": recency_filter,
                        "backend_sync": backend_sync,
                        "agent_ingest": agent_ingest,
                    }
                    if recency_filter.get("enabled"):
                        self._append_log(
                            run_id,
                            "[ACCOUNT] recency "
                            f"account={account_label} "
                            f"cutoff={recency_filter.get('cutoff')} "
                            f"filtered_old={recency_filter.get('filteredOld', 0)}",
                        )
                    log_suffix = (
                        f" synced={synced} metadata_synced={metadata_synced} "
                        f"media_synced={media_synced} media_unavailable={media_unavailable} "
                        f"contents={len(batch.contents)} assets={len(batch.assets)}"
                    )
                    if sync_skip_reason:
                        log_suffix += f" sync_skip={sync_skip_reason}"
                    self._append_log(run_id, f"[ACCOUNT] done {normalized_account.platform.value.lower()} account={account_label} target={normalized_account.account}{log_suffix}")
                except Exception as exc:  # noqa: BLE001 - one account failure should be capturable.
                    failed_reason = f"{type(exc).__name__}: {exc}"
                    failed_count += 1
                    failure_code = exc.code.value if isinstance(exc, CollectorError) else type(exc).__name__
                    retryable = bool(exc.retryable) if isinstance(exc, CollectorError) else False
                    if (
                        is_bulk_refresh
                        and isinstance(exc, CollectorError)
                        and self._should_open_account_platform_circuit(normalized_account.platform, exc)
                    ):
                        circuit_reason = self._open_account_platform_circuit(normalized_account.platform, exc)
                        self._append_log(run_id, f"[ACCOUNT] circuit_open platform={normalized_account.platform.value.lower()} reason={circuit_reason}")
                    item = {
                        "platform": normalized_account.platform.value,
                        "account": account_label,
                        "targetAccount": normalized_account.account,
                        "status": "failed",
                        "reason": failed_reason,
                        "failedReason": failed_reason,
                        "failureCode": failure_code,
                        "retryable": retryable,
                    }
                    self._append_log(run_id, f"[ACCOUNT] failed {normalized_account.platform.value.lower()} account={account_label} target={normalized_account.account} reason={failed_reason}")
                    self._update_job(
                        run_id,
                        {
                            "status": "RUNNING",
                            "currentAccount": account_label,
                            "currentTargetAccount": normalized_account.account,
                            "accountsProcessed": totals["accountsProcessed"],
                            "failedAccounts": failed_count,
                            "failedReason": f"ACCOUNT_PARTIAL_FAILED: {failed_reason}",
                            "progressMessage": f"account {totals['accountsProcessed']}/{len(request.accounts)} failed; continuing with next account",
                        },
                    )
                    if not request.continue_on_error:
                        items.append(item)
                        self._checkpoint_account_result(run_id, item)
                        break
                items.append(item)
                self._checkpoint_account_result(run_id, item)
                self._pace_scheduled_account_bulk(
                    run_id,
                    request,
                    account_index=account_index,
                    accounts_total=len(request.accounts),
                    settings=settings,
                )
            terminal_status = (
                "CANCELLED"
                if cancelled
                else _account_terminal_status(
                    failed_count=failed_count,
                    zero_sync_count=zero_sync_count,
                    accounts_total=len(request.accounts),
                    contents_synced=totals["contentsSynced"],
                    contents_collected=totals["contentsCollected"],
                )
            )
            summary = {
                "run_id": run_id,
                "status": terminal_status.lower(),
                "job_type": "ACCOUNT",
                "accounts_total": len(request.accounts),
                "accounts_processed": totals["accountsProcessed"],
                "accounts_collected": totals["accountsCollected"],
                "contents_collected": totals["contentsCollected"],
                "comments_collected": totals["commentsCollected"],
                "assets_collected": totals["assetsCollected"],
                "contents_synced": totals["contentsSynced"],
                "metadata_synced": totals["metadataSynced"],
                "media_synced": totals["mediaSynced"],
                "media_unavailable": totals["mediaUnavailable"],
                "agent_succeeded": totals["agentSucceeded"],
                "agent_failed": totals["agentFailed"],
                "failed_accounts": failed_count,
                "zero_sync_accounts": zero_sync_count,
                "items": items,
                "elapsed_seconds": round((datetime.now(timezone.utc) - started).total_seconds(), 3),
            }
            self._run_dir(run_id).mkdir(parents=True, exist_ok=True)
            (self._run_dir(run_id) / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
            update = {
                "status": terminal_status,
                "finishedAt": self._now(),
                "accountsTotal": len(request.accounts),
                "accountsProcessed": totals["accountsProcessed"],
                "accountsCollected": totals["accountsCollected"],
                "contentsCollected": totals["contentsCollected"],
                "commentsCollected": totals["commentsCollected"],
                "assetsCollected": totals["assetsCollected"],
                "contentsSynced": totals["contentsSynced"],
                "metadataSynced": totals["metadataSynced"],
                "mediaSynced": totals["mediaSynced"],
                "mediaUnavailable": totals["mediaUnavailable"],
                "agentSucceeded": totals["agentSucceeded"],
                "agentFailed": totals["agentFailed"],
                "failedAccounts": failed_count,
                "zeroSyncAccounts": zero_sync_count,
                "summaryPath": str(self._run_dir(run_id) / "summary.json"),
                "summary": summary,
            }
            if failed_reason:
                update["failedReason"] = f"ACCOUNT_PARTIAL_FAILED: {failed_reason}" if terminal_status != "FAILED" else f"ACCOUNT_FAILED: {failed_reason}"
            if cancelled:
                update["progressMessage"] = "account collection stopped by operator after completed account checkpoints"
            self._update_job(run_id, update)
        finally:
            if slot_acquired:
                self._account_slots.release()
            self._cleanup_after_job(run_id)

    def _run_live_job(self, run_id: str, request: LiveJobCreate) -> None:
        heartbeat_stop: threading.Event | None = None
        heartbeat_thread: threading.Thread | None = None
        chat_stop: threading.Event | None = None
        chat_thread: threading.Thread | None = None
        hls_relay_manager = None
        hls_relay_enabled = False
        chat_state: dict[str, int] = {"messages": 0}
        with self._lock:
            stop_event = self._live_stop_events.setdefault(run_id, threading.Event())
        try:
            self._mark_running(run_id)
            settings = get_settings()
            self._report_upstream_task_status(
                request,
                "RUNNING",
                "live collector is connecting to the platform source",
            )
            # The business UI still submits live work through /collector/jobs.
            # Keep that path on the same persistent HLS relay as the newer
            # continuous-monitor API.  Returning a finite MP4 here makes a
            # browser play until EOF (often ~2 seconds) and then appear frozen.
            hls_relay_enabled = bool(
                request.realtime
                and request.platform in {Platform.TIKTOK, Platform.YOUTUBE}
            )
            if hls_relay_enabled:
                hls_relay_manager = get_hls_relay_manager(
                    settings.hls_output_root,
                    settings.collector_public_base_url,
                )
                # A recovered job may still contain playbackReady=true from
                # before a service restart. Do not advertise the new relay
                # until its deeper startup buffer has actually filled.
                self._update_job(run_id, {"playbackReady": False, "mediaAvailable": False})
            started = datetime.now(timezone.utc)
            metadata = dict(request.metadata or {})
            metadata.update(
                {
                    "mode": "LIVE",
                    "collectorJobType": "LIVE",
                    "collectionType": str(metadata.get("backendCollectionType") or "USERNAME"),
                    "liveUrl": request.url,
                    "isLive": True,
                    "taskId": run_id,
                }
            )
            if request.live_clip_seconds is not None:
                metadata["youtube_live_clip_seconds"] = request.live_clip_seconds
                metadata["tiktok_live_clip_seconds"] = request.live_clip_seconds
                metadata["liveClipSeconds"] = request.live_clip_seconds
            elif request.platform == Platform.YOUTUBE:
                metadata.setdefault("liveClipSeconds", settings.youtube_live_clip_seconds)
            if request.timeout_seconds is not None:
                metadata["timeout_seconds"] = request.timeout_seconds
            metadata["realtime"] = request.realtime
            if request.realtime:
                # A platform live room can be resumed by a new collector task.
                # Do not replay sequence 1 into a still-open downstream stream:
                # the downstream segment API is idempotent by sequence number.
                try:
                    configured_sequence = int(metadata.get("liveSequenceStart") or 0)
                except (TypeError, ValueError):
                    configured_sequence = 0
                metadata["liveSequenceStart"] = configured_sequence if configured_sequence > 0 else int(time.time())
                metadata.setdefault("liveOffsetStart", 0)
            if request.max_runtime_seconds is not None:
                metadata["maxRuntimeSeconds"] = request.max_runtime_seconds
            collection_request = CollectionRequest(
                job_id=run_id,
                platform=request.platform,
                capability=Capability.LIVE_RESOLVE,
                target=CollectionTarget(
                    platform=request.platform,
                    target_type="CONTENT",
                    target_value=request.url,
                    source_url=request.url,
                    capabilities=[Capability.LIVE_RESOLVE],
                    metadata=metadata,
                ),
                limit=1,
                collect_comments=request.collect_comments,
                # HLS relay jobs never make the platform adapter own a
                # bounded 15s recording. It only resolves the authorized
                # source; one persistent relay owns the media connection.
                download_media=False if hls_relay_enabled else request.download_media,
            )
            self._append_log(run_id, f"[LIVE] start platform={request.platform.value} url={request.url} clipSeconds={metadata.get('liveClipSeconds')}")
            self._update_job(
                run_id,
                {
                    "status": "RUNNING",
                    "currentPlatform": request.platform.value,
                    "currentUrl": request.url,
                    "stage": "COLLECTING",
                    "progressMessage": LIVE_STAGE_MESSAGES["COLLECTING"],
                    "diagnostics": _runtime_diagnostics(request.platform, settings),
                },
            )
            if request.realtime:
                self._update_job(
                    run_id,
                    {
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
                # Recovery may already have a downstream stream id. Start
                # liveness immediately instead of waiting for the next media
                # upload or a slow platform resolver.
                try:
                    recovered_stream_id = str(self.get_job(run_id).get("liveStreamId") or "")
                except (KeyError, TypeError, ValueError):
                    recovered_stream_id = ""
                if recovered_stream_id and settings.collector_sync_enabled:
                    heartbeat_stop, heartbeat_thread = self._start_live_heartbeat(
                        run_id,
                        request,
                        recovered_stream_id,
                        settings,
                    )
            adapter = get_adapter(request.platform)
            # Platform live pages can briefly return an empty/unavailable
            # response while the CDN room is being created. Resolve a live
            # source with a short bounded retry window before declaring the
            # task failed. The retry never advertises a stale watch URL as a
            # playable stream.
            batch = self._discover_live_with_retry(run_id, adapter, collection_request, request)
            resolved_target_url = str(batch.raw.get("target_url") or collection_request.target.source_url or request.url)
            self._update_job(
                run_id,
                {
                    "currentUrl": resolved_target_url,
                    "resolvedSourceUrl": resolved_target_url,
                },
            )
            self._append_log(
                run_id,
                f"[LIVE] media_engine={str((batch.contents[0].raw if batch.contents else {}).get('media_engine') or 'legacy').lower()}",
            )
            initial_end_reason = live_end_reason(batch)
            if initial_end_reason:
                self._append_log(run_id, f"[LIVE] platform already ended reason={initial_end_reason}")
                self._update_job(
                    run_id,
                    {
                        "status": "SUCCESS",
                        "stage": "ENDED",
                        "finishedAt": self._now(),
                        "liveStatus": "ENDED",
                        "endDetected": True,
                        "endReason": initial_end_reason,
                        "progressMessage": LIVE_STAGE_MESSAGES["ENDED"],
                    },
                )
                return
            for content in batch.contents:
                content.content_type = "LIVE"
                content.raw.setdefault("is_live", True)
                content.raw.setdefault("live_status", "is_live")

            def refresh_hls_relay(current_batch: CollectionBatch, *, wait_for_ready: bool = False) -> None:
                """Publish a stable local HLS URL for the current signed CDN URL."""
                if not hls_relay_enabled or hls_relay_manager is None:
                    return
                content = current_batch.contents[0] if current_batch.contents else None
                source_url = str(
                    (content.raw.get("live_stream_url") if content else "")
                    or current_batch.raw.get("live_stream_url")
                    or collection_request.target.metadata.get("tiktok_live_stream_url")
                    or collection_request.target.metadata.get("liveStreamUrl")
                    or ""
                ).strip()
                if not source_url:
                    self._append_log(run_id, "[LIVE] HLS relay waiting: adapter returned no live_stream_url")
                    return
                playback_url = hls_relay_manager.ensure(
                    run_id,
                    source_url,
                    platform=request.platform.value,
                    upstream_proxy=settings.youtube_proxy if request.platform == Platform.YOUTUBE else "",
                )
                if wait_for_ready:
                    # Upstream HLS can arrive in 4-5 second bursts. Wait for
                    # the full rolling cushion instead of publishing a
                    # playlist that a player exhausts after a few seconds.
                    ready_deadline = time.monotonic() + 35
                    while time.monotonic() < ready_deadline and not hls_relay_manager.ready(
                        run_id,
                        platform=request.platform.value,
                    ):
                        time.sleep(0.25)
                playback_ready = hls_relay_manager.ready(run_id, platform=request.platform.value)
                collection_request.target.metadata["playbackHlsUrl"] = playback_url
                collection_request.target.metadata["playbackMode"] = "HLS"
                collection_request.target.metadata["playbackReady"] = playback_ready
                self._update_job(
                    run_id,
                    {
                        "playUrl": playback_url,
                        "playbackMode": "HLS",
                        "playerType": "HLS",
                        "mediaAvailable": playback_ready,
                        "playbackReady": playback_ready,
                    },
                )
                relay_state = hls_relay_manager.diagnostic(run_id, platform=request.platform.value)
                self._append_log(run_id, f"[LIVE] HLS relay active platform={request.platform.value} ready={playback_ready} state={relay_state} url={playback_url}")

            # Start the relay before registration so the first downstream
            # stream record already contains the playable .m3u8 URL.
            refresh_hls_relay(batch, wait_for_ready=True)
            realtime_result = None
            if request.realtime and settings.collector_sync_enabled:
                def ensure_heartbeat(stream_id: str) -> None:
                    nonlocal heartbeat_stop, heartbeat_thread
                    if heartbeat_thread is None:
                        heartbeat_stop, heartbeat_thread = self._start_live_heartbeat(
                            run_id,
                            request,
                            stream_id,
                            settings,
                        )

                if request.live_clip_seconds is None and request.platform == Platform.YOUTUBE:
                    collection_request.target.metadata["youtube_live_clip_seconds"] = 10
                    collection_request.target.metadata["liveClipSeconds"] = 10
                realtime_result = self._push_live_with_retry(
                    run_id,
                    batch,
                    collection_request,
                    settings=settings,
                    on_stream_registered=ensure_heartbeat,
                )
                self._update_job(
                    run_id,
                    {
                        "stage": "RECORDING",
                        "progressMessage": "live stream registered; pushing rolling media segments",
                        "liveStreamId": realtime_result.get("streamId"),
                        "segmentsUploaded": realtime_result.get("segmentsUploaded", 0),
                        "messagesUploaded": realtime_result.get("messagesUploaded", 0),
                        "playUrl": realtime_result.get("playUrl") or "",
                        "playbackMode": realtime_result.get("playbackMode") or "",
                    },
                )
                # Normally ensure_heartbeat() has already started the loop as
                # soon as upsert returned. Keep this fallback for mocked or
                # legacy push implementations that do not invoke the hook.
                if heartbeat_thread is None and realtime_result.get("streamId"):
                    heartbeat_stop, heartbeat_thread = self._start_live_heartbeat(
                        run_id,
                        request,
                        str(realtime_result.get("streamId")),
                        settings,
                    )
                if request.platform == Platform.YOUTUBE and request.collect_comments:
                    chat_stop, chat_thread = self._start_youtube_live_chat_collector(
                        run_id,
                        request,
                        str(batch.contents[0].external_id if batch.contents else ""),
                        str(realtime_result.get("streamId") or ""),
                        settings,
                        chat_state,
                    )
                max_runtime = int(request.max_runtime_seconds or metadata.get("maxRuntimeSeconds") or 60)
                realtime_started = time.monotonic()
                end_reason = ""
                consecutive_source_failures = 0
                # A relay-backed live job has exactly one owner of the upstream
                # media connection: HlsRelay/FFmpeg.  Do *not* call the
                # platform adapter for a new MP4 clip inside this loop.  That
                # old model made a finite 15-second file the playback source,
                # so every browser necessarily hit EOF and froze.  Instead we
                # tail the relay's continuously produced TS files for analysis
                # while the browser stays on its single growing m3u8 playlist.
                relay_seen_segments: set[str] = set()
                relay_probe_at = time.monotonic() + 45.0
                relay_unavailable_since: float | None = None
                relay_dir = (
                    settings.hls_output_root
                    / request.platform.value.lower()
                    / run_id
                )
                # The relay emits three-second independent HLS chunks. Keep
                # downstream offsets/durations aligned with the actual TS
                # files; a mismatched value makes the analysis timeline drift
                # even when playback itself is smooth.
                relay_segment_seconds = 3
                while time.monotonic() - realtime_started < max_runtime:
                    time.sleep(1)
                    if stop_event.is_set():
                        end_reason = "STOP_REQUESTED"
                        self._append_log(run_id, "[LIVE] stop requested by upstream; closing relay")
                        break
                    if time.monotonic() - realtime_started + 1 >= max_runtime:
                        break
                    if hls_relay_enabled:
                        # Read locally completed HLS chunks.  The playlist
                        # remains public and continuous; these assets are only
                        # the analysis/downstream side-channel.
                        try:
                            # Follow the *current playlist* rather than the
                            # entire directory. The relay intentionally
                            # deletes files outside its sliding window; a
                            # directory scan races that deletion and retries
                            # already-expired analysis files.
                            playlist_path = relay_dir / "index.m3u8"
                            playlist_text = playlist_path.read_text(encoding="utf-8", errors="ignore")
                            # FFmpeg writes ENDLIST when the upstream live
                            # source has terminated. Do not keep the job and
                            # browser on the last sliding-window segments;
                            # that looks like a frozen/repeated broadcast.
                            if "#EXT-X-ENDLIST" in playlist_text:
                                end_reason = "RELAY_END_OF_STREAM"
                                self._append_log(run_id, "[LIVE] HLS relay emitted ENDLIST; marking platform stream ended")
                                break
                            playlist_names = re.findall(r"(?m)^segment-[^\r\n]+$", playlist_text)
                            relay_paths = [
                                relay_dir / name
                                for name in playlist_names
                                if (relay_dir / name).is_file() and (relay_dir / name).stat().st_size > 0
                            ]
                        except OSError:
                            relay_paths = []
                        if relay_paths:
                            relay_unavailable_since = None
                        elif not hls_relay_manager.ready(run_id, platform=request.platform.value):
                            if relay_unavailable_since is None:
                                relay_unavailable_since = time.monotonic()
                            unavailable_seconds = time.monotonic() - relay_unavailable_since
                            if unavailable_seconds >= 15:
                                relay_state = hls_relay_manager.diagnostic(run_id, platform=request.platform.value)
                                self._update_job(
                                    run_id,
                                    {
                                        "stage": "DEGRADED",
                                        "mediaAvailable": False,
                                        "playbackReady": False,
                                        "retryable": True,
                                        "failedCode": "LIVE_RELAY_UNAVAILABLE",
                                        "failedReason": f"HLS relay has produced no playable segment for {int(unavailable_seconds)}s ({relay_state})",
                                        "progressMessage": "live metadata is available but media relay is unavailable; retrying",
                                    },
                                )
                        new_paths = [path for path in relay_paths if path.name not in relay_seen_segments][:3]
                        if new_paths and batch.contents:
                            # FFmpeg owns this rolling directory and can
                            # delete a playlist entry between our existence
                            # check and downstream upload.  Snapshot finished
                            # chunks into non-public collector storage first;
                            # that removes the FileNotFoundError race without
                            # retaining every 2-second file for the full live.
                            staging_dir = settings.youtube_output_root / run_id / "relay-staging"
                            staging_dir.mkdir(parents=True, exist_ok=True)
                            staged_paths: list[Path] = []
                            for path in new_paths:
                                staged = staging_dir / path.name
                                try:
                                    shutil.copy2(path, staged)
                                except OSError:
                                    continue
                                if staged.is_file() and staged.stat().st_size > 0:
                                    staged_paths.append(staged)
                            if not staged_paths:
                                continue
                            previous_segments = int(realtime_result.get("segmentsUploaded", 0))
                            collection_request.target.metadata["liveSequenceStart"] = (
                                int(metadata["liveSequenceStart"]) + previous_segments
                            )
                            collection_request.target.metadata["liveOffsetStart"] = (
                                previous_segments * relay_segment_seconds * 1000
                            )
                            collection_request.target.metadata["liveClipSeconds"] = relay_segment_seconds
                            relay_content = batch.contents[0].model_copy(deep=True)
                            relay_content.content_type = "LIVE"
                            relay_content.raw = dict(relay_content.raw)
                            relay_content.raw.update(
                                {
                                    "is_live": True,
                                    "live_status": "is_live",
                                    "live_clip_seconds": relay_segment_seconds,
                                    "live_stream_url": collection_request.target.metadata.get("playbackHlsUrl", ""),
                                    "media_engine": "persistent_hls_relay",
                                }
                            )
                            relay_batch = CollectionBatch(
                                contents=[relay_content],
                                assets=[
                                    MediaAsset(
                                        platform=request.platform,
                                        content_external_id=relay_content.external_id,
                                        asset_type="VIDEO",
                                        local_path=str(path),
                                    )
                                    for path in staged_paths
                                ],
                                raw={
                                    "is_live": True,
                                    "live_stream_url": collection_request.target.metadata.get("playbackHlsUrl", ""),
                                    "media_engine": "persistent_hls_relay",
                                },
                            )
                            try:
                                next_result = self._push_live_with_retry(
                                    run_id,
                                    relay_batch,
                                    collection_request,
                                    settings=settings,
                                )
                            except Exception as exc:
                                # Never stop or recreate the relay merely
                                # because analysis delivery is briefly down.
                                self._append_log(run_id, f"[LIVE] relay segment delivery retry: {type(exc).__name__}: {exc}")
                            else:
                                relay_seen_segments.update(path.name for path in staged_paths)
                                realtime_result["segmentsUploaded"] = previous_segments + int(next_result.get("segmentsUploaded", 0))
                                realtime_result["messagesUploaded"] = int(realtime_result.get("messagesUploaded", 0)) + int(next_result.get("messagesUploaded", 0))
                                self._update_job(
                                    run_id,
                                    {
                                        "stage": "RECORDING",
                                        "progressMessage": "persistent HLS relay segment delivered",
                                        "segmentsUploaded": realtime_result["segmentsUploaded"],
                                        "messagesUploaded": realtime_result["messagesUploaded"],
                                        "failedReason": None,
                                        "retryable": False,
                                    },
                                )
                            finally:
                                for staged in staged_paths:
                                    try:
                                        staged.unlink(missing_ok=True)
                                    except OSError:
                                        pass
                        # Platform probes are metadata-only and intentionally
                        # infrequent. They refresh a signed source or detect
                        # a real end-of-stream; they never replace playback
                        # with a finite media clip.
                        if time.monotonic() >= relay_probe_at:
                            try:
                                probe_batch = adapter.discover(collection_request)
                                probe_end_reason = live_end_reason(probe_batch)
                                if probe_end_reason:
                                    end_reason = probe_end_reason
                                    self._append_log(run_id, f"[LIVE] platform ended reason={end_reason}")
                                    break
                                for probe_content in probe_batch.contents:
                                    probe_content.content_type = "LIVE"
                                    probe_content.raw.setdefault("is_live", True)
                                    probe_content.raw.setdefault("live_status", "is_live")
                                if probe_batch.contents:
                                    batch = probe_batch
                                    refresh_hls_relay(probe_batch)
                            except Exception as exc:
                                if live_error_indicates_end(exc):
                                    end_reason = "PLATFORM_REPORTED_ENDED"
                                    self._append_log(run_id, f"[LIVE] platform ended during relay probe: {exc}")
                                    break
                                self._append_log(run_id, f"[LIVE] metadata probe deferred; relay continues: {type(exc).__name__}: {exc}")
                            finally:
                                relay_probe_at = time.monotonic() + 45.0
                        continue
                    previous_segments = int(realtime_result.get("segmentsUploaded", 0))
                    collection_request.target.metadata["liveSequenceStart"] = int(metadata["liveSequenceStart"]) + previous_segments
                    clip_ms = int(collection_request.target.metadata.get("liveClipSeconds") or 0) * 1000
                    collection_request.target.metadata["liveOffsetStart"] = previous_segments * clip_ms
                    try:
                        next_batch = adapter.discover(collection_request)
                    except Exception as exc:
                        if live_error_indicates_end(exc):
                            end_reason = "PLATFORM_REPORTED_ENDED"
                            self._append_log(run_id, f"[LIVE] platform ended while resolving next segment: {exc}")
                            break
                        consecutive_source_failures += 1
                        error_text = str(exc).lower()
                        # Do not leave a dead task marked LIVE forever when
                        # both the resolver and the configured browser fallback
                        # are unavailable. Five failed probes (~10 seconds)
                        # is long enough for a transient CDN reconnect but
                        # short enough to keep the UI truthful.
                        if consecutive_source_failures >= 5 and (
                            "connect econnrefused" in error_text
                            or "browser fallback failed" in error_text
                            or "no live content" in error_text
                        ):
                            end_reason = "COLLECTOR_SOURCE_UNAVAILABLE"
                            self._append_log(run_id, f"[LIVE] source unavailable after retries; ending task: {exc}")
                            break
                        # A single CDN/browser/source-resolution failure must
                        # not terminate an otherwise healthy live session.
                        # Keep the heartbeat alive and retry the next segment
                        # immediately with a bounded backoff.
                        self._append_log(run_id, f"[LIVE] transient source failure; reconnecting next segment: {type(exc).__name__}: {exc}")
                        self._update_job(
                            run_id,
                            {
                                "status": "RUNNING",
                                "stage": "RECONNECTING",
                                "progressMessage": "live source reconnecting; heartbeat remains active",
                                "retryable": True,
                                "failedReason": f"temporary segment source failure: {exc}",
                            },
                        )
                        time.sleep(2)
                        continue
                    consecutive_source_failures = 0
                    next_end_reason = live_end_reason(next_batch)
                    if next_end_reason:
                        end_reason = next_end_reason
                        self._append_log(run_id, f"[LIVE] platform ended reason={end_reason}")
                        break
                    for next_content in next_batch.contents:
                        next_content.content_type = "LIVE"
                        next_content.raw.setdefault("is_live", True)
                        next_content.raw.setdefault("live_status", "is_live")
                    refresh_hls_relay(next_batch)
                    if not next_batch.contents:
                        end_reason = "PLATFORM_NO_LIVE_CONTENT"
                        break
                    self._append_log(
                        run_id,
                        f"[LIVE] media_engine={str((next_batch.contents[0].raw if next_batch.contents else {}).get('media_engine') or 'legacy').lower()}",
                    )
                    try:
                        next_result = self._push_live_with_retry(
                            run_id,
                            next_batch,
                            collection_request,
                            settings=settings,
                        )
                    except Exception as exc:
                        # Downstream outages are recoverable for continuous
                        # monitoring. Do not convert a temporary upload or
                        # registration failure into a terminal LIVE failure.
                        self._append_log(run_id, f"[LIVE] transient downstream failure; reconnecting next segment: {type(exc).__name__}: {exc}")
                        self._update_job(
                            run_id,
                            {
                                "status": "RUNNING",
                                "stage": "RECONNECTING",
                                "progressMessage": "downstream reconnecting; live capture continues",
                                "retryable": True,
                                "failedReason": f"temporary downstream failure: {exc}",
                            },
                        )
                        time.sleep(2)
                        continue
                    realtime_result["segmentsUploaded"] = int(realtime_result.get("segmentsUploaded", 0)) + int(next_result.get("segmentsUploaded", 0))
                    realtime_result["messagesUploaded"] = int(realtime_result.get("messagesUploaded", 0)) + int(next_result.get("messagesUploaded", 0))
                    if next_result.get("playUrl"):
                        realtime_result["playUrl"] = next_result.get("playUrl")
                    if next_result.get("playbackMode"):
                        realtime_result["playbackMode"] = next_result.get("playbackMode")
                    self._update_job(
                        run_id,
                        {
                            "stage": "RECORDING",
                            "progressMessage": "live stream segment uploaded",
                            "liveStreamId": realtime_result.get("streamId"),
                            "segmentsUploaded": realtime_result.get("segmentsUploaded", 0),
                            "messagesUploaded": realtime_result.get("messagesUploaded", 0),
                            "playUrl": realtime_result.get("playUrl") or "",
                            "playbackMode": realtime_result.get("playbackMode") or "",
                            "failedReason": None,
                            "retryable": False,
                        },
                    )
                if heartbeat_stop:
                    heartbeat_stop.set()
                if heartbeat_thread:
                    heartbeat_thread.join(timeout=3)
                heartbeat_stop = None
                heartbeat_thread = None
                if chat_stop:
                    chat_stop.set()
                if chat_thread:
                    chat_thread.join(timeout=5)
                chat_stop = None
                chat_thread = None
                finish_realtime_live_push(
                    str(realtime_result.get("streamId")),
                    collection_request,
                    segment_count=int(realtime_result.get("segmentsUploaded", 0)),
                    message_count=int(realtime_result.get("messagesUploaded", 0)) + int(chat_state.get("messages", 0)),
                    settings=settings,
                    reason=end_reason or "COLLECTOR_WINDOW_COMPLETE",
                    detection_source="platform_metadata" if end_reason else "collector_window",
                )
            self._append_log(run_id, f"[LIVE] collected platform={request.platform.value} contents={len(batch.contents)} assets={len(batch.assets)}; syncing downstream")
            self._update_job(
                run_id,
                {
                    "contentsCollected": len(batch.contents),
                    "commentsCollected": len(batch.comments) + int(chat_state.get("messages", 0)),
                    "assetsCollected": len(batch.assets),
                    "stage": "SYNCING",
                    "progressMessage": f"collected live contents={len(batch.contents)} assets={len(batch.assets)}; syncing downstream",
                },
            )
            backend_sync = {"items": [], "agent_ingest": {"enabled": False, "items": []}}
            if settings.collector_sync_enabled and request.realtime and realtime_result:
                backend_sync = {
                    "items": [{
                        "streamId": realtime_result.get("streamId"),
                        "status": "QUEUED",
                        "segmentsUploaded": realtime_result.get("segmentsUploaded", 0),
                        "messagesUploaded": int(realtime_result.get("messagesUploaded", 0)) + int(chat_state.get("messages", 0)),
                    }],
                    "agent_ingest": {"enabled": False, "items": []},
                }
            elif settings.collector_sync_enabled:
                backend_sync = sync_batch_to_backend(batch, collection_request, settings=settings)
            synced = len(backend_sync.get("items") or [])
            agent_ingest = backend_sync.get("agent_ingest") or {}
            agent_items = agent_ingest.get("items") or []
            agent_succeeded = sum(1 for item in agent_items if isinstance(item, dict) and item.get("status") == "SUCCESS")
            agent_failed = sum(1 for item in agent_items if isinstance(item, dict) and item.get("status") == "FAILED")
            sync_skip_reason = _account_sync_skip_reason(batch, synced, settings)
            item_status = "success" if synced > 0 or not settings.collector_sync_enabled else "collected_without_sync"
            item = {
                "platform": request.platform.value,
                "url": request.url,
                "isLive": True,
                "liveClipSeconds": metadata.get("liveClipSeconds"),
                "status": item_status,
                "counts": {
                    "contents": len(batch.contents),
                    "comments": len(batch.comments),
                    "assets": len(batch.assets),
                    "synced": synced,
                    "agentSucceeded": agent_succeeded,
                    "agentFailed": agent_failed,
                },
                "sync_skip_reason": sync_skip_reason,
                "skipSyncReason": sync_skip_reason,
                "backend_sync": backend_sync,
                "agent_ingest": agent_ingest,
            }
            terminal_status = "SUCCESS" if batch.contents and (synced > 0 or not settings.collector_sync_enabled) else "FAILED"
            failed_reason = "" if terminal_status == "SUCCESS" else (sync_skip_reason or "LIVE_SYNC_FAILED: live content was not synced downstream")
            summary = {
                "run_id": run_id,
                "status": terminal_status.lower(),
                "job_type": "LIVE",
                "platform": request.platform.value,
                "url": request.url,
                "is_live": True,
                "live_clip_seconds": metadata.get("liveClipSeconds"),
                "downstream_collection_type": metadata.get("collectionType"),
                "contents_collected": len(batch.contents),
                "comments_collected": len(batch.comments),
                "assets_collected": len(batch.assets),
                "contents_synced": synced,
                "agent_succeeded": agent_succeeded,
                "agent_failed": agent_failed,
                "items": [item],
                "elapsed_seconds": round((datetime.now(timezone.utc) - started).total_seconds(), 3),
            }
            self._run_dir(run_id).mkdir(parents=True, exist_ok=True)
            (self._run_dir(run_id) / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
            update = {
                "status": terminal_status,
                "stage": "ENDED" if terminal_status == "SUCCESS" and request.realtime and 'end_reason' in locals() and end_reason else "COMPLETED" if terminal_status == "SUCCESS" else "FAILED",
                "finishedAt": self._now(),
                "platformsTotal": 1,
                "platformsProcessed": 1,
                "platformsCollected": 1 if batch.contents else 0,
                "contentsCollected": len(batch.contents),
                "commentsCollected": len(batch.comments),
                "assetsCollected": len(batch.assets),
                "contentsSynced": synced,
                "agentSucceeded": agent_succeeded,
                "agentFailed": agent_failed,
                "summaryPath": str(self._run_dir(run_id) / "summary.json"),
                "summary": summary,
                "liveStatus": "ENDED" if request.realtime and 'end_reason' in locals() and end_reason else None,
                "endDetected": bool(request.realtime and 'end_reason' in locals() and end_reason),
                "endReason": end_reason if request.realtime and 'end_reason' in locals() and end_reason else None,
                "progressMessage": LIVE_STAGE_MESSAGES["ENDED"] if request.realtime and 'end_reason' in locals() and end_reason else f"live collection finished synced={synced}",
            }
            if request.realtime:
                self._report_upstream_task_status(
                    request,
                    "COMPLETED",
                    LIVE_STAGE_MESSAGES["ENDED"] if 'end_reason' in locals() and end_reason else "live collector completed",
                    discovered_count=int(realtime_result.get("segmentsUploaded", 0)) if realtime_result else len(batch.contents),
                )
            if request.realtime:
                # A finished relay must never remain advertised as a playable
                # live source. Otherwise the browser keeps the last sliding
                # window and appears to replay the same fragment forever.
                update.update(
                    {
                        "mediaStatus": "ENDED" if end_reason else "STOPPED",
                        "mediaAvailable": False,
                        "playbackReady": False,
                    }
                )
            if failed_reason:
                update["failedReason"] = failed_reason
                update["failedCode"] = "DOWNSTREAM_SYNC_FAILED"
                update["failedAction"] = "CHECK_DOWNSTREAM_PAYLOAD_OR_SERVICE"
                update["retryable"] = False
            self._append_log(run_id, f"[LIVE] done platform={request.platform.value} url={request.url} status={terminal_status} synced={synced} contents={len(batch.contents)} assets={len(batch.assets)}")
            self._update_job(run_id, update)
        except Exception as exc:  # noqa: BLE001 - job API must persist terminal failure.
            failure = _failure_payload(exc)
            not_started = _is_live_source_not_started(request, exc)
            self._append_log(
                run_id,
                f"[LIVE] {'not live' if not_started else 'failed'} platform={request.platform.value} "
                f"url={request.url} code={failure['failedCode']} reason={type(exc).__name__}: {exc}",
            )
            self._report_upstream_task_status(
                request,
                "NO_NEW" if not_started else "FAILED",
                "platform is not currently live or has only a scheduled broadcast"
                if not_started
                else failure["failedReason"],
            )
            # A collector restart or source-resolution failure can happen
            # after downstream registration. Close that remote live session
            # as well; otherwise its old HLS URL remains RUNNING and clients
            # keep replaying the last cached segment.
            if request.realtime:
                active_result = locals().get("realtime_result") or {}
                stream_id = str(active_result.get("streamId") or "")
                if not stream_id:
                    try:
                        stream_id = str(self.get_job(run_id).get("liveStreamId") or "")
                    except (KeyError, TypeError, ValueError):
                        stream_id = ""
                if stream_id:
                    try:
                        finish_realtime_live_push(
                            stream_id,
                            collection_request,
                            segment_count=int(active_result.get("segmentsUploaded") or 0),
                            message_count=int(active_result.get("messagesUploaded") or 0),
                            settings=get_settings(),
                            reason="COLLECTOR_SOURCE_UNAVAILABLE",
                            detection_source="collector_error",
                        )
                    except Exception as finish_exc:  # noqa: BLE001 - preserve original failure.
                        self._append_log(run_id, f"[LIVE] failed to close downstream stream: {type(finish_exc).__name__}: {finish_exc}")
            if not_started:
                self._update_job(
                    run_id,
                    {
                        "status": "SUCCESS",
                        "stage": "ENDED",
                        "finishedAt": self._now(),
                        "liveStatus": "NOT_LIVE",
                        "endDetected": False,
                        "progressMessage": "platform is not currently live; waiting for the next broadcast",
                        "failedReason": None,
                        "failedCode": None,
                        "failedAction": None,
                        "retryable": False,
                        "mediaStatus": "NOT_LIVE",
                        "mediaAvailable": False,
                        "playbackReady": False,
                        "summary": {
                            "job_type": "LIVE",
                            "platform": request.platform.value,
                            "url": request.url,
                            "stage": "ENDED",
                            "liveStatus": "NOT_LIVE",
                            "reason": "platform is not currently live or has only a scheduled broadcast",
                        },
                    },
                )
            else:
                self._fail_job(
                    run_id,
                    failure["failedCode"],
                    failure["failedReason"],
                    {"job_type": "LIVE", "platform": request.platform.value, "url": request.url, "stage": "FAILED", **failure},
                )
        finally:
            if heartbeat_stop:
                heartbeat_stop.set()
            if heartbeat_thread:
                heartbeat_thread.join(timeout=3)
            if chat_stop:
                chat_stop.set()
            if chat_thread:
                chat_thread.join(timeout=5)
            if hls_relay_manager is not None:
                hls_relay_manager.stop(run_id, platform=request.platform.value)
            with self._lock:
                self._live_stop_events.pop(run_id, None)
            self._cleanup_after_job(run_id)

    def _start_youtube_live_chat_collector(
        self,
        run_id: str,
        request: LiveJobCreate,
        video_id: str,
        stream_id: str,
        settings,
        state: dict[str, int],
    ) -> tuple[threading.Event, threading.Thread]:
        """Start a bounded, browser-free YouTube live-chat collector.

        The media loop and the chat loop are independent. A chat/login
        failure must not interrupt video segments or heartbeats.
        """
        stop_event = threading.Event()
        thread = threading.Thread(
            target=self._run_youtube_live_chat_collector,
            args=(run_id, request, video_id, stream_id, settings, stop_event, state),
            name=f"youtube-live-chat-{run_id}",
            daemon=True,
        )
        thread.start()
        return stop_event, thread

    def _run_youtube_live_chat_collector(
        self,
        run_id: str,
        request: LiveJobCreate,
        video_id: str,
        stream_id: str,
        settings,
        stop_event: threading.Event,
        state: dict[str, int],
    ) -> None:
        if not video_id or not stream_id:
            self._append_log(run_id, "[CHAT] YouTube live chat skipped: missing videoId or streamId")
            return
        chat_dir = (settings.youtube_output_root / run_id / "live-chat").resolve()
        chat_dir.mkdir(parents=True, exist_ok=True)
        output_template = str(chat_dir / "%(id)s.live_chat.%(ext)s")
        ytdlp = shutil.which("yt-dlp") or str(Path(".venv/bin/yt-dlp"))
        if not Path(ytdlp).exists() and not shutil.which(ytdlp):
            self._append_log(run_id, "[CHAT] YouTube live chat skipped: yt-dlp executable not found")
            return
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
            request.url,
        ]
        # Prefer the browser profile for live chat.  Netscape cookie exports
        # can authenticate media metadata while still failing the live_chat
        # endpoint with a bot challenge; the same profile used by the media
        # adapter contains the current session cookies.
        browser_profile = settings.youtube_browser_profile
        if browser_profile and Path(browser_profile).exists():
            command[1:1] = [
                "--cookies-from-browser",
                f"{settings.youtube_browser}:{browser_profile}",
            ]
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

        process: subprocess.Popen | None = None
        seen_ids: set[str] = set()
        try:
            process = subprocess.Popen(
                command,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                text=True,
                start_new_session=True,
            )
            self._append_log(run_id, "[CHAT] YouTube live chat collector started")
            adapter = get_adapter(Platform.YOUTUBE)
            started = time.monotonic()
            max_seconds = int((request.metadata or {}).get("chatMaxSeconds") or request.max_runtime_seconds or 86400)
            while not stop_event.is_set() and time.monotonic() - started < max_seconds:
                fresh = []
                for path in sorted(chat_dir.glob("*.live_chat*.json*")):
                    if not path.is_file() or path.stat().st_size <= 0:
                        continue
                    for raw in adapter._iter_live_chat_records(path):
                        comment = adapter._live_chat_comment_from_record(raw, video_id, len(seen_ids))
                        if comment and comment.external_id not in seen_ids:
                            seen_ids.add(comment.external_id)
                            fresh.append(comment)
                if fresh:
                    client = CollectorBackendClient.from_settings(settings)
                    try:
                        client.live_messages(
                            stream_id,
                            {
                                "batchId": f"{video_id}-chat-{len(seen_ids):06d}",
                                "items": [
                                    {
                                        "externalMessageId": item.external_id,
                                        "messageType": "COMMENT",
                                        "authorName": str((item.raw or {}).get("author") or item.author_external_id or ""),
                                        "authorUid": item.author_external_id,
                                        "body": item.text,
                                        "occurredAt": item.published_at.isoformat() if item.published_at else _iso_now(),
                                    }
                                    for item in fresh
                                ],
                            },
                        )
                        state["messages"] = int(state.get("messages", 0)) + len(fresh)
                        self._update_job(
                            run_id,
                            {
                                "status": "RUNNING",
                                "stage": "RECORDING",
                                "messagesUploaded": state["messages"],
                                "commentsCollected": state["messages"],
                                "lastMessageAt": self._now(),
                                "progressMessage": f"live chat collector active; messages={state['messages']}",
                            },
                        )
                        self._append_log(run_id, f"[CHAT] YouTube received={len(fresh)} total={state['messages']}")
                    except Exception as exc:  # noqa: BLE001 - chat sync is independently retryable.
                        self._append_log(run_id, f"[CHAT] downstream messages failed: {type(exc).__name__}: {exc}")
                if process.poll() is not None and not fresh:
                    detail = ""
                    if process.stderr:
                        try:
                            detail = process.stderr.read().strip().replace("\n", " ")[:500]
                        except Exception:
                            detail = ""
                    suffix = f" detail={detail}" if detail else ""
                    self._append_log(run_id, f"[CHAT] YouTube live chat process exited code={process.returncode}{suffix}")
                    break
                stop_event.wait(2.0)
        except Exception as exc:  # noqa: BLE001 - chat must not stop media capture.
            self._append_log(run_id, f"[CHAT] YouTube live chat collector failed: {type(exc).__name__}: {exc}")
        finally:
            if process and process.poll() is None:
                try:
                    os.killpg(process.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                if process.stderr:
                    try:
                        process.stderr.close()
                    except Exception:
                        pass

    def _start_live_heartbeat(
        self,
        run_id: str,
        request: LiveJobCreate,
        stream_id: str,
        settings,
    ) -> tuple[threading.Event, threading.Thread]:
        """Send liveness independently while a platform clip is downloading."""
        stop_event = threading.Event()

        def send_heartbeat() -> None:
            interval = max(3, min(int(getattr(settings, "collector_heartbeat_interval_seconds", 5) or 5), 30))
            timeout = max(3, min(int(getattr(settings, "collector_heartbeat_timeout_seconds", 10) or 10), 30))
            client = CollectorBackendClient.from_settings(settings, timeout_seconds=timeout)
            # A heartbeat is a control-plane signal. Do not let the generic
            # backend timeout (often 300s) block the live loop.
            client.max_attempts = 1
            try:
                heartbeat_sequence = int(self.get_job(run_id).get("heartbeatSequence") or 0)
            except (KeyError, TypeError, ValueError):
                heartbeat_sequence = 0
            consecutive_failures = 0
            last_ack_monotonic: float | None = None
            # Send immediately. Waiting for the first five-second tick made a
            # newly-started stream look stale during the most visible window.
            while not stop_event.is_set():
                heartbeat_sequence += 1
                sent_at = _iso_now()
                attempted_at = datetime.now(timezone.utc).isoformat()
                attempt_started = time.monotonic()
                payload = {
                    "taskId": run_id,
                    "collectorNode": settings.worker_id,
                    "liveStatus": "LIVE",
                    "mediaStatus": "RUNNING",
                    "chatStatus": "RUNNING" if request.collect_comments else "UNAVAILABLE",
                    "metricsStatus": "RUNNING",
                    "heartbeatSeq": heartbeat_sequence,
                    "heartbeatId": f"{run_id}:{heartbeat_sequence}",
                    "heartbeatIntervalSeconds": interval,
                    "sentAt": sent_at,
                }
                # Keep the control-plane record pinned to the rolling HLS
                # playlist. The media loop may be busy producing a 15/30s
                # analysis clip while this heartbeat still runs every 5s.
                try:
                    current_job = self.get_job(run_id)
                    current_play_url = str(current_job.get("playUrl") or "").strip()
                    current_playback_mode = str(current_job.get("playbackMode") or "").upper()
                    if current_play_url and current_playback_mode == "HLS":
                        payload.update(
                            {
                                "playUrl": current_play_url,
                                "sourceUrl": current_play_url,
                                "mediaUrl": current_play_url,
                                "videoUrl": current_play_url,
                                "hlsUrl": current_play_url,
                                "playbackMode": "HLS",
                                "playerType": "HLS",
                                "playbackReady": bool(current_job.get("playbackReady", True)),
                                # Playback media and archival/analysis
                                # segment delivery are separate pipelines.
                                # A rolling HLS relay is live even when an
                                # individual analysis TS has already rotated
                                # out of the short local window.
                                "mediaStatus": "RUNNING" if bool(current_job.get("playbackReady", True)) else "RECOVERING",
                                "lastMediaAt": _iso_now() if bool(current_job.get("playbackReady", True)) else None,
                            }
                        )
                except (KeyError, TypeError, ValueError):
                    pass
                try:
                    acknowledgement = client.live_heartbeat(stream_id, payload)
                    acknowledged_at = datetime.now(timezone.utc).isoformat()
                    ack_latency_ms = round((time.monotonic() - attempt_started) * 1000, 1)
                    consecutive_failures = 0
                    last_ack_monotonic = time.monotonic()
                    self._update_job(
                        run_id,
                        {
                            "status": "RUNNING",
                            "stage": "RECORDING",
                            # lastHeartbeatAt is kept as the backwards-
                            # compatible alias for the actual ACK time.
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
                            "lastMediaAt": acknowledged_at if payload.get("mediaStatus") == "RUNNING" else None,
                            "progressMessage": "live collector heartbeat active; waiting for next media segment",
                        },
                    )
                except Exception as exc:  # noqa: BLE001 - a heartbeat outage is retryable.
                    consecutive_failures += 1
                    age = (time.monotonic() - last_ack_monotonic) if last_ack_monotonic is not None else float("inf")
                    self._append_log(run_id, f"[LIVE] heartbeat retryable failure: {type(exc).__name__}: {exc}")
                    self._update_job(
                        run_id,
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
                            "progressMessage": "live source is running; downstream heartbeat retrying",
                        },
                    )
                # The wait is interruptible so stopping a task never leaves a
                # heartbeat thread sleeping for a full interval.
                if stop_event.wait(float(interval)):
                    break

        thread = threading.Thread(target=send_heartbeat, name=f"live-heartbeat-{run_id}", daemon=True)
        thread.start()
        return stop_event, thread

    def _push_live_with_retry(
        self,
        run_id: str,
        batch: CollectionBatch,
        request: CollectionRequest,
        *,
        settings,
        on_stream_registered=None,
    ) -> dict[str, Any]:
        """Keep a live session alive across short downstream outages.

        A transient connection refusal used to escape the live loop after the
        HTTP client's three sub-second retries, leaving the UI stuck at
        ``采集端失联`` even though the source was still live.  Retry the whole
        idempotent upsert/segment/metrics push with bounded exponential
        backoff; the next successful heartbeat restores the downstream state.
        """
        max_attempts = 8
        progress = lambda event: self._append_log(run_id, f"[LIVE] {event}")
        for attempt in range(1, max_attempts + 1):
            try:
                return run_realtime_live_push(
                    batch,
                    request,
                    settings=settings,
                    progress=progress,
                    on_stream_registered=on_stream_registered,
                )
            except CollectorError as exc:
                if exc.code != CollectorErrorCode.NETWORK_TEMPORARY or attempt >= max_attempts:
                    raise
                delay = min(15, 2 ** (attempt - 1))
                self._append_log(
                    run_id,
                    f"[LIVE] downstream unavailable; retrying push attempt={attempt + 1}/{max_attempts} in {delay}s: {exc.message}",
                )
                self._update_job(
                    run_id,
                    {
                        "status": "RUNNING",
                        "stage": "RECONNECTING",
                        "progressMessage": f"downstream unavailable; retrying live push ({attempt + 1}/{max_attempts})",
                    },
                )
                time.sleep(delay)

    def _run_home_feed_job(self, run_id: str, request: HomeFeedJobCreate, task_id: str) -> None:
        try:
            self._mark_running(run_id)
            settings = get_settings()
            browser_search = BrowserSearchCollector()
            started = datetime.now(timezone.utc)
            totals = {
                "platformsProcessed": 0,
                "platformsCollected": 0,
                "contentsCollected": 0,
                "commentsCollected": 0,
                "assetsCollected": 0,
                "contentsSynced": 0,
                "failedPlatforms": 0,
                "zeroSyncPlatforms": 0,
                "agentSucceeded": 0,
                "agentFailed": 0,
            }
            items: list[dict[str, Any]] = []
            failed_reason = None
            for platform in request.platforms:
                totals["platformsProcessed"] += 1
                login_account_id = _home_feed_login_account_id(settings, platform)
                home_url = _home_feed_url(platform)
                self._append_log(run_id, f"[HOME_FEED] start platform={platform.value} taskId={task_id} loginAccountId={login_account_id}")
                try:
                    profile = _home_feed_browser_profile(settings, platform)
                    if not profile:
                        raise RuntimeError("NO_LOGIN_ACCOUNT: browser profile is not configured")
                    search_result = browser_search.collect_with_context(
                        url=home_url,
                        platform=platform,
                        profile=profile,
                        browser=_home_feed_browser(settings, platform),
                        limit=request.limit_per_platform,
                        timeout_seconds=int(request.metadata.get("browser_search_timeout_seconds") or request.metadata.get("timeout_seconds") or 60),
                        cdp_url=_home_feed_cdp_url(settings, platform),
                        refresh=request.refresh_home,
                    )
                    links = search_result.links
                    if not links and _home_feed_requires_login(platform, search_result.body_text, search_result.page_url):
                        raise RuntimeError("LOGIN_EXPIRED: home feed requires a signed-in browser profile")
                    self._append_log(
                        run_id,
                        f"[HOME_FEED] refreshed platform={platform.value} taskId={task_id} loginAccountId={login_account_id} links={len(links)}",
                    )
                    self._update_job(
                        run_id,
                        {
                            "status": "RUNNING",
                            "currentPlatform": platform.value,
                            "currentLoginAccountId": login_account_id,
                            "platformsProcessed": totals["platformsProcessed"],
                            "contentsCollected": totals["contentsCollected"],
                            "assetsCollected": totals["assetsCollected"],
                            "contentsSynced": totals["contentsSynced"],
                            "progressMessage": f"refreshed home feed platform={platform.value} links={len(links)}; collecting details",
                        },
                    )
                    platform_item, counts = self._collect_home_feed_platform(
                        run_id=run_id,
                        request=request,
                        platform=platform,
                        links=links,
                        task_id=task_id,
                        login_account_id=login_account_id,
                        settings=settings,
                    )
                    totals["platformsCollected"] += 1
                    for key in ("contentsCollected", "commentsCollected", "assetsCollected", "contentsSynced", "agentSucceeded", "agentFailed"):
                        totals[key] += counts.get(key, 0)
                    if counts.get("contentsSynced", 0) == 0:
                        totals["zeroSyncPlatforms"] += 1
                    items.append(platform_item)
                    self._append_log(
                        run_id,
                        f"[HOME_FEED] done platform={platform.value} taskId={task_id} loginAccountId={login_account_id} "
                        f"synced={counts.get('contentsSynced', 0)} contents={counts.get('contentsCollected', 0)} assets={counts.get('assetsCollected', 0)}",
                    )
                except Exception as exc:  # noqa: BLE001 - one platform failure must not stop the batch.
                    failed_reason = f"{type(exc).__name__}: {exc}"
                    totals["failedPlatforms"] += 1
                    item = {
                        "platform": platform.value,
                        "loginAccountId": login_account_id,
                        "status": "failed",
                        "errorCode": _home_feed_error_code(failed_reason),
                        "reason": failed_reason,
                        "failedReason": failed_reason,
                    }
                    items.append(item)
                    self._append_log(
                        run_id,
                        f"[HOME_FEED] failed platform={platform.value} taskId={task_id} loginAccountId={login_account_id} reason={failed_reason}",
                    )
                    if not request.continue_on_error:
                        break
            terminal_status = _home_feed_terminal_status(
                failed_platforms=totals["failedPlatforms"],
                platforms_total=len(request.platforms),
                contents_synced=totals["contentsSynced"],
                contents_collected=totals["contentsCollected"],
            )
            summary = {
                "run_id": run_id,
                "task_id": task_id,
                "status": terminal_status.lower(),
                "job_type": "HOME_FEED",
                "platforms_total": len(request.platforms),
                "platforms_processed": totals["platformsProcessed"],
                "platforms_collected": totals["platformsCollected"],
                "contents_collected": totals["contentsCollected"],
                "comments_collected": totals["commentsCollected"],
                "assets_collected": totals["assetsCollected"],
                "contents_synced": totals["contentsSynced"],
                "failed_platforms": totals["failedPlatforms"],
                "zero_sync_platforms": totals["zeroSyncPlatforms"],
                "agent_succeeded": totals["agentSucceeded"],
                "agent_failed": totals["agentFailed"],
                "items": items,
                "elapsed_seconds": round((datetime.now(timezone.utc) - started).total_seconds(), 3),
            }
            self._run_dir(run_id).mkdir(parents=True, exist_ok=True)
            (self._run_dir(run_id) / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
            update = {
                "status": terminal_status,
                "finishedAt": self._now(),
                "taskId": task_id,
                "platformsTotal": len(request.platforms),
                "platformsProcessed": totals["platformsProcessed"],
                "platformsCollected": totals["platformsCollected"],
                "contentsCollected": totals["contentsCollected"],
                "commentsCollected": totals["commentsCollected"],
                "assetsCollected": totals["assetsCollected"],
                "contentsSynced": totals["contentsSynced"],
                "failedPlatforms": totals["failedPlatforms"],
                "zeroSyncPlatforms": totals["zeroSyncPlatforms"],
                "agentSucceeded": totals["agentSucceeded"],
                "agentFailed": totals["agentFailed"],
                "summaryPath": str(self._run_dir(run_id) / "summary.json"),
                "summary": summary,
            }
            if failed_reason:
                update["failedReason"] = f"HOME_FEED_PARTIAL_FAILED: {failed_reason}" if terminal_status != "FAILED" else f"HOME_FEED_FAILED: {failed_reason}"
            self._update_job(run_id, update)
        finally:
            self._cleanup_after_job(run_id)

    def _collect_home_feed_platform(
        self,
        *,
        run_id: str,
        request: HomeFeedJobCreate,
        platform: Platform,
        links: list[str],
        task_id: str,
        login_account_id: str,
        settings,
    ) -> tuple[dict[str, Any], dict[str, int]]:
        adapter = get_adapter(platform)
        contents: list[dict[str, Any]] = []
        counts = {
            "contentsCollected": 0,
            "commentsCollected": 0,
            "assetsCollected": 0,
            "contentsSynced": 0,
            "agentSucceeded": 0,
            "agentFailed": 0,
            "itemFailures": 0,
        }
        for rank, link in enumerate(links[: request.limit_per_platform], start=1):
            metadata = dict(request.metadata or {})
            metadata.update(
                {
                    "mode": "HOME_FEED",
                    "collectionType": "HOME_FEED",
                    "homeFeedBatchId": task_id,
                    "taskId": task_id,
                    "loginAccountId": login_account_id,
                    "recommendationRank": rank,
                }
            )
            try:
                batch = adapter.discover(
                    CollectionRequest(
                        job_id=str(uuid4()),
                        platform=platform,
                        capability=Capability.CONTENT_DETAIL,
                        target=CollectionTarget(
                            platform=platform,
                            target_type="CONTENT",
                            target_value=link,
                            source_url=link,
                            capabilities=[Capability.CONTENT_DETAIL],
                            metadata=metadata,
                        ),
                        limit=1,
                        collect_comments=request.collect_comments,
                        download_media=request.download_media,
                    )
                )
                backend_sync = {"items": [], "agent_ingest": {"enabled": False, "items": []}}
                if settings.collector_sync_enabled:
                    backend_sync = sync_batch_to_backend(batch, self._home_feed_sync_request(platform, link, request, metadata), settings=settings)
                synced = len(backend_sync.get("items") or [])
                agent_ingest = backend_sync.get("agent_ingest") or {}
                agent_items = agent_ingest.get("items") or []
                agent_succeeded = sum(1 for item in agent_items if isinstance(item, dict) and item.get("status") == "SUCCESS")
                agent_failed = sum(1 for item in agent_items if isinstance(item, dict) and item.get("status") == "FAILED")
                counts["contentsCollected"] += len(batch.contents)
                counts["commentsCollected"] += len(batch.comments)
                counts["assetsCollected"] += len(batch.assets)
                counts["contentsSynced"] += synced
                counts["agentSucceeded"] += agent_succeeded
                counts["agentFailed"] += agent_failed
                skip_reason = _account_sync_skip_reason(batch, synced, settings)
                content_id = batch.contents[0].external_id if batch.contents else link
                content_item = {
                    "rank": rank,
                    "recommendationRank": rank,
                    "videoId": content_id,
                    "sourceUrl": link,
                    "status": "success" if synced > 0 else "collected_without_sync",
                    "counts": {
                        "contents": len(batch.contents),
                        "comments": len(batch.comments),
                        "assets": len(batch.assets),
                        "synced": synced,
                        "agentSucceeded": agent_succeeded,
                        "agentFailed": agent_failed,
                    },
                    "sync_skip_reason": skip_reason,
                    "skipSyncReason": skip_reason,
                    "backend_sync": backend_sync,
                }
                self._append_log(
                    run_id,
                    f"[HOME_FEED] item platform={platform.value} taskId={task_id} rank={rank} videoId={content_id} "
                    f"synced={synced} contents={len(batch.contents)} assets={len(batch.assets)}",
                )
            except Exception as exc:  # noqa: BLE001 - a failed rank must not be replaced by rank 6.
                counts["itemFailures"] += 1
                content_item = {
                    "rank": rank,
                    "recommendationRank": rank,
                    "sourceUrl": link,
                    "status": "failed",
                    "errorCode": _home_feed_error_code(str(exc)),
                    "reason": f"{type(exc).__name__}: {exc}",
                    "failedReason": f"{type(exc).__name__}: {exc}",
                }
                self._append_log(
                    run_id,
                    f"[HOME_FEED] item_failed platform={platform.value} taskId={task_id} rank={rank} url={link} reason={content_item['reason']}",
                )
            contents.append(content_item)
            self._update_job(
                run_id,
                {
                    "status": "RUNNING",
                    "currentPlatform": platform.value,
                    "currentLoginAccountId": login_account_id,
                    "currentRecommendationRank": rank,
                    "contentsCollected": counts["contentsCollected"],
                    "assetsCollected": counts["assetsCollected"],
                    "contentsSynced": counts["contentsSynced"],
                    "progressMessage": f"home feed platform={platform.value} rank={rank} synced={counts['contentsSynced']}",
                },
            )
        if not links:
            status = "success"
        elif counts["contentsSynced"] > 0 and counts.get("itemFailures", 0) > 0:
            status = "partial_success"
        elif counts["contentsSynced"] > 0:
            status = "success"
        elif counts.get("itemFailures", 0) > 0:
            status = "failed"
        else:
            status = "collected_without_sync"
        item = {
            "platform": platform.value,
            "loginAccountId": login_account_id,
            "status": status,
            "homeUrl": _home_feed_url(platform),
            "linksFound": len(links),
            "counts": counts,
            "contents": contents,
        }
        if status == "failed":
            first_failed = next((content for content in contents if content.get("status") == "failed"), {})
            item["errorCode"] = first_failed.get("errorCode") or "COLLECT_FAILED"
            item["reason"] = first_failed.get("reason") or "all recommended items failed"
            item["failedReason"] = first_failed.get("failedReason") or item["reason"]
        return item, counts

    def _home_feed_sync_request(self, platform: Platform, link: str, request: HomeFeedJobCreate, metadata: dict[str, Any]) -> CollectionRequest:
        return CollectionRequest(
            job_id=str(uuid4()),
            platform=platform,
            capability=Capability.CONTENT_DETAIL,
            target=CollectionTarget(
                platform=platform,
                target_type="CONTENT",
                target_value=link,
                source_url=link,
                capabilities=[Capability.CONTENT_DETAIL],
                metadata=metadata,
            ),
            limit=1,
            collect_comments=request.collect_comments,
            download_media=request.download_media,
        )

    def _normalize_account_target(self, account: AccountTargetCreate) -> AccountTargetCreate:
        effective_platform = self._effective_account_platform(account)
        normalized_account = _normalize_account_value(effective_platform, account.account)
        if effective_platform != Platform.YOUTUBE:
            url_account = _account_from_source_url(effective_platform, account.source_url)
            if url_account:
                normalized_account = url_account
        # The UI historically stores a human display name in ``account`` and
        # the durable YouTube handle in ``sourceUrl``.  Never send that
        # display name to an adapter as if it were a channel path: it leads to
        # a synthetic browser account ID which the ingestion backend rejects.
        if effective_platform == Platform.YOUTUBE and not (
            normalized_account.startswith("@") or normalized_account.startswith("UC") or normalized_account.startswith("http")
        ):
            source_handle = _youtube_handle_from_text(account.source_url)
            source_channel = re.search(r"(?:youtube\.com/channel/|\\b)(UC[A-Za-z0-9_-]{20,})\\b", account.source_url, flags=re.IGNORECASE)
            if source_handle:
                normalized_account = source_handle
            elif source_channel:
                normalized_account = source_channel.group(1)
        normalized_source_url = _normalize_account_source_url(effective_platform, normalized_account, account.source_url)
        return account.model_copy(update={"platform": effective_platform, "account": normalized_account, "source_url": normalized_source_url})

    def _effective_account_platform(self, account: AccountTargetCreate) -> Platform:
        # Startup recovery constructs this service before the legacy URL
        # helper declarations lower in this module have executed. Keep this
        # tiny classification self-contained so recovery cannot fail during
        # module import after a service restart.
        looks_like_facebook = bool(
            re.fullmatch(r"\d{8,}", account.account.strip())
            or re.search(r"(?:^|//)(?:www\.)?facebook\.com/", account.source_url.strip(), flags=re.IGNORECASE)
        )
        if account.platform == Platform.YOUTUBE and looks_like_facebook:
            return Platform.FACEBOOK
        return account.platform

    def _account_sync_request(self, account: AccountTargetCreate, request: AccountJobCreate, metadata: dict[str, Any] | None = None) -> CollectionRequest:
        return CollectionRequest(
            job_id=str(uuid4()),
            platform=account.platform,
            capability=Capability.ACCOUNT_TIMELINE,
            target=CollectionTarget(
                platform=account.platform,
                target_type="ACCOUNT",
                target_value=account.account,
                source_url=account.source_url,
                capabilities=[Capability.ACCOUNT_TIMELINE],
                metadata=dict(metadata or request.metadata or {}),
            ),
            limit=request.limit,
            collect_comments=request.collect_comments,
            download_media=request.download_media,
        )

    def _finish_keyword_job(self, run_id: str, result: dict[str, Any]) -> None:
        platform_results = result.get("platform_results") or []
        self._update_job(
            run_id,
            {
                "status": "SUCCESS" if result.get("status") == "success" else "FAILED",
                "finishedAt": self._now(),
                "keywordsTotal": sum(int(item.get("keywords_total") or 0) for item in platform_results),
                "keywordsCollected": sum(int(item.get("keywords_collected") or 0) for item in platform_results),
                "contentsSynced": int(result.get("contents_synced") or 0),
                "failedPlatform": result.get("failed_platform"),
                "summaryPath": str(self._run_dir(run_id) / "summary.json"),
            },
        )

    def _fail_job(self, run_id: str, reason: str, message: str, result: dict[str, Any] | None = None) -> None:
        result_payload = result or {}
        update = {
            "status": "FAILED",
            "finishedAt": self._now(),
            "failedReason": result_payload.get("failedReason") or f"{reason}: {message}",
            "summary": result_payload,
        }
        for key in ("stage", "failedCode", "failedAction", "retryable"):
            if key in result_payload:
                update[key] = result_payload[key]
        try:
            job = self.get_job(run_id)
        except (KeyError, TypeError, ValueError):
            job = {}
        if str(job.get("jobType") or "").upper() == "LIVE":
            update.update(
                {
                    "mediaStatus": "FAILED",
                    "mediaAvailable": False,
                    "playbackReady": False,
                }
            )
        self._update_job(run_id, update)

    def _discover_live_with_retry(self, run_id: str, adapter, collection_request: CollectionRequest, request: LiveJobCreate) -> CollectionBatch:
        """Resolve the current live room without trusting a cached video ID."""
        metadata = collection_request.target.metadata or {}
        try:
            attempts = int(metadata.get("liveResolveAttempts") or 3)
        except (TypeError, ValueError):
            attempts = 3
        attempts = max(1, min(attempts, 5))
        last_error: Exception | None = None
        for attempt in range(1, attempts + 1):
            try:
                return adapter.discover(collection_request)
            except Exception as exc:  # noqa: BLE001 - preserve platform-specific collector errors.
                last_error = exc
                if attempt >= attempts:
                    raise
                self._append_log(
                    run_id,
                    f"[LIVE] source resolve retry {attempt}/{attempts - 1}: {type(exc).__name__}: {exc}",
                )
                self._update_job(
                    run_id,
                    {
                        "status": "RUNNING",
                        "stage": "RECONNECTING",
                        "progressMessage": f"live source resolving current broadcast (retry {attempt}/{attempts - 1})",
                        "retryable": True,
                        "failedReason": f"temporary live source resolution failure: {exc}",
                    },
                )
                time.sleep(min(2 * attempt, 5))
        # The loop always returns or raises; keep a defensive error for static
        # type checkers and future adapter implementations.
        raise last_error or RuntimeError("live source resolution failed")

    def _report_upstream_task_status(
        self,
        request: LiveJobCreate,
        status: str,
        message: str,
        *,
        discovered_count: int = 0,
    ) -> None:
        """Best-effort lifecycle callback for the business task record."""
        metadata = request.metadata or {}
        task_id = str(
            metadata.get("upstreamTaskId")
            or metadata.get("taskId")
            or ""
        ).strip()
        if not task_id:
            return
        callback_base = str(
            metadata.get("callbackBaseUrl")
            or metadata.get("callback_base_url")
            or ""
        ).strip()
        try:
            settings = get_settings()
            client = CollectorBackendClient.from_settings(
                settings,
                base_url=callback_base or settings.collector_backend_base_url,
                timeout_seconds=min(int(settings.collector_media_timeout_seconds or 10), 15),
            )
            client.max_attempts = 1
            client.collector_task_status(
                task_id,
                {
                    "status": str(status).upper(),
                    "message": str(message or "")[:1000],
                    "discoveredCount": max(0, int(discovered_count or 0)),
                },
            )
        except Exception as exc:  # noqa: BLE001 - lifecycle callback must not stop collection.
            # The collector job file remains authoritative when the business
            # callback is temporarily unavailable; the next status poll or
            # retry can reconcile it without losing the media task.
            self._append_log(
                str(metadata.get("taskId") or task_id),
                f"[TASK_STATUS] callback {status} failed: {type(exc).__name__}: {exc}",
            )

    def _mark_running(self, run_id: str) -> None:
        self._update_job(run_id, {"status": "RUNNING", "startedAt": self._now()})

    def _cleanup_after_job(self, run_id: str) -> None:
        try:
            cleanup = cleanup_after_collection(settings=get_settings())
            self._append_log(
                run_id,
                "[CLEANUP] done "
                f"files={cleanup.get('filesRemoved', 0)} "
                f"bytes={cleanup.get('bytesRemoved', 0)} "
                f"dirs={cleanup.get('dirsRemoved', 0)} "
                f"gc={cleanup.get('gcCollected', 0)}",
            )
        except Exception as exc:  # noqa: BLE001 - cleanup must not flip a finished collection job.
            cleanup = {"status": "failed", "error": f"{type(exc).__name__}: {exc}"}
            self._append_log(run_id, f"[CLEANUP] failed reason={cleanup['error']}")
        self._update_job(run_id, {"cleanup": cleanup})

    def _initial_job(self, run_id: str, job_type: str, request: dict[str, Any]) -> dict[str, Any]:
        return {
            "runId": run_id,
            "jobType": job_type,
            "status": "PENDING",
            "stage": "QUEUED" if job_type == "LIVE" else "",
            "request": request,
            "createdAt": self._now(),
            "startedAt": None,
            "finishedAt": None,
            "keywordsTotal": 0,
            "keywordsCollected": 0,
            "accountsTotal": 0,
            "accountsProcessed": 0,
            "accountsCollected": 0,
            "contentsCollected": 0,
            "commentsCollected": 0,
            "assetsCollected": 0,
            "contentsSynced": 0,
            "agentSucceeded": 0,
            "agentFailed": 0,
            "failedPlatform": None,
            "failedReason": None,
            "failedCode": None,
            "failedAction": None,
            "retryable": False,
            "diagnostics": {},
            "lastHeartbeatAt": self._now(),
            "lastHeartbeatAckAt": None,
            "lastHeartbeatAttemptAt": None,
            "heartbeatDeliveryStatus": "NOT_STARTED",
            "heartbeatHealth": "UNKNOWN",
            "heartbeatConsecutiveFailures": 0,
            "heartbeatIntervalSeconds": 5,
            "updatedAt": self._now(),
            "summaryPath": str(self._run_dir(run_id) / "summary.json"),
            "logPath": str(self._log_path(run_id)),
            "lockedPlatforms": [],
            "signature": "",
            "cleanup": {},
        }

    def _active_job_for(
        self,
        signature: str,
        platforms: set[str],
        *,
        allow_live_overlap: bool = False,
        allow_batch_overlap: bool = False,
        allow_account_overlap: bool = False,
    ) -> dict[str, Any] | None:
        with self._lock:
            for job in self._jobs.values():
                if job.get("status") not in ACTIVE_JOB_STATUSES:
                    continue
                if job.get("signature") == signature:
                    duplicate = dict(job)
                    duplicate["deduplicated"] = True
                    return duplicate
                locked_platforms = set(job.get("lockedPlatforms") or [])
                if locked_platforms & platforms:
                    # Distinct live accounts on the same platform must be
                    # able to run concurrently. Their signatures were
                    # checked above for duplicate submissions; a platform
                    # lock here incorrectly rejected the second YouTube or
                    # TikTok account with 409 and left the UI at 接入中.
                    if allow_live_overlap or allow_batch_overlap:
                        continue
                    if allow_account_overlap and job.get("jobType") == "ACCOUNT":
                        continue
                    platform_text = ",".join(sorted(locked_platforms & platforms))
                    raise CollectorJobConflict(f"platform is already running: {platform_text}")
        return None

    def _wait_for_account_priority(self, run_id: str, platforms: list[Platform]) -> None:
        """Keep an admitted low-priority keyword job behind account work.

        Account batches may be pending on a global worker slot while another
        platform is downloading.  Rejecting a keyword job in that state loses
        replenishment demand; letting it take the platform lock first delays
        monitoring.  This durable wait gives us an actual low-priority queue.
        """
        wanted = {platform.value for platform in platforms}
        while True:
            active = self._active_account_job_for_platforms(wanted)
            if not active:
                return
            self._update_job(
                run_id,
                {
                    "status": "RUNNING",
                    "stage": "WAITING_FOR_ACCOUNT_PRIORITY",
                    "progressMessage": f"low-priority keyword queue waiting for account task {active}",
                },
            )
            time.sleep(3)

    def _active_account_job_for_platforms(self, platforms: set[str]) -> str:
        """Find an active account reservation for one of ``platforms``."""
        seen: set[str] = set()
        with self._lock:
            candidates = [dict(job) for job in self._jobs.values()]
        for job in candidates:
            seen.add(str(job.get("runId") or ""))
        if self.base_dir.exists():
            for path in self.base_dir.glob("account-*.json"):
                try:
                    job = json.loads(path.read_text(encoding="utf-8"))
                except (OSError, json.JSONDecodeError):
                    continue
                run_id = str(job.get("runId") or "")
                if run_id and run_id in seen:
                    continue
                seen.add(run_id)
                candidates.append(job)
        for job in candidates:
            if job.get("jobType") != "ACCOUNT" or job.get("status") not in ACTIVE_JOB_STATUSES:
                continue
            if set(job.get("lockedPlatforms") or []) & platforms:
                return str(job.get("runId") or "account task")
        return ""

    def _active_live_job_for_source(self, source_key: str) -> dict[str, Any] | None:
        """Find an active relay for the same platform account/source."""
        with self._lock:
            for job in self._jobs.values():
                if job.get("jobType") != "LIVE" or job.get("status") not in ACTIVE_JOB_STATUSES:
                    continue
                existing_key = str(job.get("liveSourceKey") or self._live_recovery_source_key(job))
                if existing_key == source_key:
                    duplicate = dict(job)
                    duplicate["deduplicated"] = True
                    return duplicate
        return None

    @contextmanager
    def _batch_platform_lock(self, run_id: str, platforms: list[Platform], *, job_label: str):
        """Serialize browser/downloader work without rejecting scheduled jobs.

        Locks are acquired in enum order to keep a multi-platform keyword
        refresh from deadlocking with single-platform account jobs.  The job
        remains inspectable as queued while it waits.
        """
        unique_platforms = sorted(set(platforms), key=lambda item: item.value)
        acquired: list[threading.Lock] = []
        try:
            for platform in unique_platforms:
                lock = self._batch_platform_locks[platform]
                if not lock.acquire(blocking=False):
                    self._update_job(
                        run_id,
                        {
                            "status": "PENDING",
                            "stage": "QUEUED",
                            "progressMessage": f"waiting for {platform.value.lower()} collector slot ({job_label})",
                        },
                    )
                    self._append_log(run_id, f"[QUEUE] waiting platform={platform.value} job={job_label}")
                    lock.acquire()
                acquired.append(lock)
            self._update_job(
                run_id,
                {
                    "status": "RUNNING",
                    "stage": "COLLECTING",
                    "progressMessage": f"collector slot acquired for {job_label}",
                },
            )
            yield
        finally:
            for lock in reversed(acquired):
                lock.release()

    def _job_signature(self, job_type: str, payload: dict[str, Any]) -> str:
        return json.dumps({"jobType": job_type, "payload": payload}, ensure_ascii=False, sort_keys=True, default=str)

    def _account_platform_circuit_reason(self, platform: Platform) -> str:
        entry = self._account_platform_circuit_until.get(platform)
        if not entry:
            return ""
        until, reason = entry
        if time.monotonic() < until:
            return reason
        self._account_platform_circuit_until.pop(platform, None)
        return ""

    def _wait_for_youtube_live_priority(self, run_id: str, request: AccountJobCreate) -> None:
        """Defer batch work before it can touch the sole YouTube Profile.

        Live playback has a long-lived authenticated connection.  Letting a
        scheduled account refresh issue metadata/detail requests through the
        same cookies changes the request pattern mid-broadcast and has caused
        YouTube to challenge the entire session.  Waiting happens before an
        account slot is acquired, so it cannot starve other platforms.
        """
        settings = get_settings()
        if not settings.youtube_defer_batch_while_live:
            return
        if not any(self._effective_account_platform(account) == Platform.YOUTUBE for account in request.accounts):
            return
        interval = max(5, min(int(settings.youtube_live_priority_wait_seconds or 15), 60))
        while True:
            live_run_id = self._active_live_run_for_platform(Platform.YOUTUBE, exclude_run_id=run_id)
            if not live_run_id:
                return
            self._update_job(
                run_id,
                {
                    "status": "PENDING",
                    "stage": "WAITING_FOR_LIVE_PRIORITY",
                    "progressMessage": f"waiting for active YouTube live task {live_run_id}; protecting shared login profile",
                },
            )
            time.sleep(interval)

    def _wait_for_queued_priority_account(
        self,
        run_id: str,
        request: AccountJobCreate,
        platform: Platform,
    ) -> None:
        """Yield a scheduled bulk row to a queued selected-account repair.

        This is intentionally limited to the scheduler-shaped bulk request.
        It does not add concurrency, does not interrupt an in-flight account,
        and only applies when the other task explicitly carries ``priority``.
        """
        if not _is_scheduled_account_bulk(request):
            return
        while self._has_queued_priority_account(run_id, platform):
            self._update_job(
                run_id,
                {
                    "status": "PENDING",
                    "stage": "WAITING_FOR_PRIORITY_ACCOUNT",
                    "progressMessage": f"yielding {platform.value.lower()} collector slot to a queued priority account",
                },
            )
            time.sleep(1)

    def _has_queued_priority_account(self, run_id: str, platform: Platform) -> bool:
        with self._lock:
            jobs = [dict(job) for job in self._jobs.values()]
        for job in jobs:
            if str(job.get("runId") or "") == run_id or job.get("jobType") != "ACCOUNT":
                continue
            if job.get("status") not in ACTIVE_JOB_STATUSES:
                continue
            try:
                queued_request = AccountJobCreate.model_validate(job.get("request") or {})
            except Exception:
                continue
            if any(
                account.priority and self._effective_account_platform(account) == platform
                for account in queued_request.accounts
            ):
                return True
        return False

    def _pace_scheduled_account_bulk(
        self,
        run_id: str,
        request: AccountJobCreate,
        *,
        account_index: int,
        accounts_total: int,
        settings,
    ) -> None:
        """Create breathing room between scheduler-owned account probes.

        This is deliberately applied only to the upstream bulk task shape,
        never an analyst's targeted account repair.  The pause is after a
        checkpoint, so a restart during it resumes from the next unfinished
        account rather than replaying downloads.
        """
        if not _is_scheduled_account_bulk(request) or account_index >= accounts_total:
            return
        chunk_size = max(1, int(getattr(settings, "collector_scheduled_bulk_chunk_size", 20) or 20))
        if account_index % chunk_size == 0:
            delay = max(0.0, float(getattr(settings, "collector_scheduled_bulk_chunk_pause_seconds", 15.0) or 0.0))
            reason = f"completed safe bulk chunk {account_index}/{accounts_total}; cooling down platform session"
        else:
            delay = max(0.0, float(getattr(settings, "collector_scheduled_bulk_account_pause_seconds", 1.0) or 0.0))
            reason = f"pacing scheduled bulk account {account_index}/{accounts_total}"
        if delay <= 0:
            return
        self._update_job(
            run_id,
            {
                "status": "RUNNING",
                "stage": "PACING",
                "progressMessage": f"{reason}; wait {delay:g}s",
            },
        )
        time.sleep(delay)

    def _active_live_run_for_platform(self, platform: Platform, *, exclude_run_id: str = "") -> str:
        with self._lock:
            for candidate_id, candidate in list(self._jobs.items()):
                if candidate_id == exclude_run_id:
                    continue
                if candidate.get("jobType") != "LIVE" or candidate.get("status") not in ACTIVE_JOB_STATUSES:
                    continue
                # A recovered live worker can finish while a stale in-memory
                # row survives a restart/error path.  Never let that ghost
                # block every YouTube priority probe; the persisted job is
                # the authoritative status for a cross-thread process.
                try:
                    persisted = json.loads(self._job_path(candidate_id).read_text(encoding="utf-8"))
                except (OSError, json.JSONDecodeError):
                    persisted = candidate
                if persisted.get("status") not in ACTIVE_JOB_STATUSES:
                    self._jobs[candidate_id] = persisted
                    continue
                request = candidate.get("request") or {}
                if str(request.get("platform") or "").upper() == platform.value:
                    return candidate_id
        return ""

    def _open_account_platform_circuit(self, platform: Platform, error: CollectorError) -> str:
        settings = get_settings()
        if platform in {Platform.FACEBOOK, Platform.INSTAGRAM}:
            # Checkpoint/login redirects are not per-account failures.  Keep a
            # blocked session out of the shared account slots long enough for
            # an operator or login watchdog to restore it.
            seconds = max(900, int(settings.youtube_auth_circuit_breaker_seconds or 0))
            label = f"{platform.value}_AUTH_REQUIRED"
        elif error.code == CollectorErrorCode.AUTH_EXPIRED:
            seconds = max(300, int(settings.youtube_auth_circuit_breaker_seconds or 0))
            label = "YOUTUBE_AUTH_REQUIRED"
        else:
            seconds = max(60, int(settings.youtube_cookie_pool_cooldown_seconds or 0))
            label = "YOUTUBE_RATE_LIMITED"
        reason = f"{label}: platform collection paused for {seconds}s after {error.code.value}"
        self._account_platform_circuit_until[platform] = (time.monotonic() + seconds, reason)
        return reason

    @staticmethod
    def _should_open_account_platform_circuit(platform: Platform, error: CollectorError) -> bool:
        """Open a circuit only for session-wide, credential-related faults."""
        if platform in {Platform.FACEBOOK, Platform.INSTAGRAM}:
            return error.code == CollectorErrorCode.AUTH_EXPIRED
        if platform == Platform.YOUTUBE:
            return error.code in {CollectorErrorCode.AUTH_EXPIRED, CollectorErrorCode.RATE_LIMITED}
        return False

    def _checkpoint_account_result(self, run_id: str, item: dict[str, Any]) -> None:
        """Persist a compact per-account checkpoint after every terminal attempt.

        A batch may be interrupted between accounts, but never needs to guess
        which successful account downloads can be skipped on restart.  Keep
        this compact: detailed backend responses stay in the final summary.
        """
        checkpoint = {
            "platform": item.get("platform"),
            "account": item.get("account"),
            "targetAccount": item.get("targetAccount"),
            "status": item.get("status"),
            "counts": dict(item.get("counts") or {}),
            "reason": item.get("reason") or item.get("failedReason") or "",
            "failureCode": item.get("failureCode") or "",
            "retryable": bool(item.get("retryable")),
            "checkpointedAt": self._now(),
        }
        try:
            job = self.get_job(run_id)
        except (KeyError, OSError, json.JSONDecodeError):
            return
        checkpoints = [entry for entry in (job.get("accountCheckpoints") or []) if isinstance(entry, dict)]
        key = (str(checkpoint.get("platform") or ""), str(checkpoint.get("targetAccount") or "").lower())
        checkpoints = [
            entry
            for entry in checkpoints
            if (str(entry.get("platform") or ""), str(entry.get("targetAccount") or "").lower()) != key
        ]
        checkpoints.append(checkpoint)
        self._update_job(run_id, {"accountCheckpoints": checkpoints, "lastAccountCheckpointAt": checkpoint["checkpointedAt"]})

    def _account_scope_signature(self, request: AccountJobCreate) -> str:
        """Return the deduplication identity of an account refresh.

        Scheduler task IDs and submission timestamps must not create another
        collector job for the exact same account set.  Other metadata is kept
        because it can change the actual collection behaviour.
        """
        payload = request.model_dump(by_alias=True, exclude={"run_in_background"})
        metadata = dict(payload.get("metadata") or {})
        for key in ACCOUNT_JOB_VOLATILE_METADATA_KEYS:
            metadata.pop(key, None)
        payload["metadata"] = metadata
        return self._job_signature("ACCOUNT_SCOPE", payload)

    def _account_scope_signature_from_job(self, job: dict[str, Any]) -> str:
        stored = str(job.get("accountScopeSignature") or "").strip()
        if stored:
            return stored
        try:
            return self._account_scope_signature(AccountJobCreate.model_validate(job.get("request") or {}))
        except Exception:
            return ""

    def _active_account_job_for_scope(self, scope_signature: str) -> dict[str, Any] | None:
        """Find an identical active account job, including one persisted on disk.

        Looking on disk is required after a service restart, before the
        reconciliation code has a chance to close or expose an older queued
        task.  It also prevents a new web worker from admitting a duplicate
        while another worker owns the same job directory.
        """
        seen_run_ids: set[str] = set()
        with self._lock:
            memory_jobs = [dict(job) for job in self._jobs.values()]
        candidates = memory_jobs
        seen_run_ids.update(str(job.get("runId") or "") for job in candidates)
        if self.base_dir.exists():
            for path in self.base_dir.glob("account-*.json"):
                try:
                    job = json.loads(path.read_text(encoding="utf-8"))
                except (OSError, json.JSONDecodeError):
                    continue
                run_id = str(job.get("runId") or "")
                if run_id and run_id in seen_run_ids:
                    continue
                seen_run_ids.add(run_id)
                candidates.append(job)
        for job in candidates:
            if job.get("jobType") != "ACCOUNT" or job.get("status") not in ACTIVE_JOB_STATUSES:
                continue
            if self._account_scope_signature_from_job(job) != scope_signature:
                continue
            duplicate = dict(job)
            duplicate["deduplicated"] = True
            return duplicate
        return None

    def _update_job(self, run_id: str, updates: dict[str, Any]) -> None:
        with self._lock:
            existing = self._jobs.get(run_id)
            if existing is None:
                path = self._job_path(run_id)
                existing = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {"runId": run_id}
            job = dict(existing)
            now = self._now()
            job.update(updates)
            job["updatedAt"] = now
            self._jobs[run_id] = job
        self._write_job_file(job)

    def _save_job(self, job: dict[str, Any]) -> None:
        with self._lock:
            self._jobs[job["runId"]] = dict(job)
        self._write_job_file(job)

    def _write_job_file(self, job: dict[str, Any]) -> None:
        self.base_dir.mkdir(parents=True, exist_ok=True)
        path = self._job_path(job["runId"])
        # Status is read by the API, the business backend and the UI while a
        # worker is updating it.  A direct write can expose an empty/truncated
        # JSON document for a few milliseconds, causing an intermittent 500
        # or a false "connecting" state.  Replace the completed temp file in
        # the same directory so readers always see either the old or new job.
        temporary = path.with_name(f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
        try:
            temporary.write_text(json.dumps(job, ensure_ascii=False, indent=2), encoding="utf-8")
            os.replace(temporary, path)
        finally:
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass

    def _append_log(self, run_id: str, line: str) -> None:
        log_path = self._log_path(run_id)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with log_path.open("a", encoding="utf-8") as handle:
            handle.write(f"{line}\n")

    def _log_writer(self, run_id: str):
        def write(line: str) -> None:
            self._append_log(run_id, line)

        return write

    def _job_path(self, run_id: str) -> Path:
        return self.base_dir / f"{run_id}.json"

    def _run_dir(self, run_id: str) -> Path:
        return self.pipeline_output_dir / run_id

    def _log_path(self, run_id: str) -> Path:
        return self.base_dir / f"{run_id}.log"

    def _new_run_id(self, prefix: str) -> str:
        return f"{prefix}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{uuid4().hex[:8]}"

    def _now(self) -> str:
        return datetime.now(timezone.utc).isoformat()


@router.post("/jobs/keywords")
def create_keyword_job(body: KeywordJobCreate) -> dict[str, Any]:
    try:
        return job_service.create_keyword_job(body)
    except CollectorJobConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/jobs/accounts")
def create_account_job(body: dict[str, Any]) -> dict[str, Any]:
    """Keep the pre-migration account endpoint compatible with LIVE work.

    The overseas business service currently posts ``jobType: LIVE`` to this URL.
    Routing that request before validating ``AccountJobCreate`` is essential: the
    legacy body contains one account plus live options, not an ordinary account
    backfill request.
    """
    try:
        job_type = str(body.get("jobType") or body.get("job_type") or "").strip().upper()
        capability = str(body.get("capability") or "").strip().upper()
        is_live = (
            job_type == "LIVE"
            or capability == Capability.LIVE_RESOLVE.value
            or bool(body.get("liveUrl") or body.get("live_url"))
            or _has_live_account_url(body)
            or _has_live_task_marker(body)
        )
        if is_live:
            request = LiveJobCreate.model_validate(_normalize_live_job_payload(body, legacy_account_job=True))
            _validate_realtime_live_account(request)
            return job_service.create_live_job(request)
        return job_service.create_account_job(AccountJobCreate.model_validate(body))
    except CollectorJobConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/jobs/accounts/{run_id}/stop")
def stop_account_job(run_id: str) -> dict[str, Any]:
    try:
        return job_service.stop_account_job(run_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="collector job not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/jobs/live", status_code=202)
def create_live_job(body: dict[str, Any]) -> dict[str, Any]:
    try:
        request = LiveJobCreate.model_validate(_normalize_live_job_payload(body))
        _validate_realtime_live_account(request)
        return job_service.create_live_job(request)
    except CollectorJobConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/jobs/home-feed", status_code=202)
def create_home_feed_job(body: HomeFeedJobCreate) -> dict[str, Any]:
    try:
        return job_service.create_home_feed_job(body)
    except CollectorJobConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/jobs")
def create_collector_job(body: dict[str, Any]) -> dict[str, Any]:
    job_type = str(body.get("jobType") or body.get("job_type") or "").strip().upper()
    if not job_type:
        capability = str(body.get("capability") or "").strip().upper()
        job_type = (
            "LIVE"
            if body.get("liveUrl") or body.get("live_url") or capability == Capability.LIVE_RESOLVE.value or _has_live_task_marker(body)
            else "HOME_FEED"
            if body.get("refreshHome")
            else "ACCOUNT"
            if body.get("accounts")
            else "KEYWORD"
        )
    try:
        if job_type == "KEYWORD":
            return job_service.create_keyword_job(KeywordJobCreate.model_validate(body))
        if job_type == "ACCOUNT":
            return job_service.create_account_job(AccountJobCreate.model_validate(body))
        if job_type == "HOME_FEED":
            return job_service.create_home_feed_job(HomeFeedJobCreate.model_validate(body))
        if job_type == "LIVE":
            request = LiveJobCreate.model_validate(_normalize_live_job_payload(body))
            _validate_realtime_live_account(request)
            return job_service.create_live_job(request)
    except CollectorJobConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    raise HTTPException(status_code=400, detail="jobType must be KEYWORD, ACCOUNT, HOME_FEED, or LIVE")


@router.get("/jobs/{run_id}")
def get_job(run_id: str) -> dict[str, Any]:
    try:
        return job_service.get_job(run_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="collector job not found") from exc


@router.post("/jobs/live/{run_id}/stop")
def stop_live_job(run_id: str) -> dict[str, Any]:
    try:
        return job_service.stop_live_job(run_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="collector job not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/jobs/{run_id}/logs")
def get_job_logs(run_id: str, tail: int = 200) -> dict[str, Any]:
    try:
        job_service.get_job(run_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="collector job not found") from exc
    return {"runId": run_id, "lines": job_service.tail_logs(run_id, tail=tail)}


@router.get("/platforms/health")
def collector_platform_health() -> dict[str, Any]:
    return {"items": platform_statuses()}

def _runtime_diagnostics(platform: Platform, settings) -> dict[str, Any]:
    if platform != Platform.YOUTUBE:
        return {"platform": platform.value}
    cookie_file = settings.youtube_cookie_file
    browser_profile = settings.youtube_browser_profile
    cookie_pool_count = _count_cookie_pool(settings.youtube_cookie_dir)
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


def _count_cookie_pool(cookie_dir: Path) -> int:
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


def _account_sync_skip_reason(batch: CollectionBatch, synced: int, settings) -> str:
    if synced > 0:
        return ""
    if not settings.collector_sync_enabled:
        return "collector sync disabled"
    if not batch.contents:
        return "no collectable content"
    if getattr(settings, "collector_metadata_only_sync_enabled", False):
        return "backend returned no synced items for collected metadata"
    if not batch.assets:
        return "metadata collected but media assets missing; backend sync requires video and thumbnail assets"
    content_ids = {content.external_id for content in batch.contents}
    video_ids = {asset.content_external_id for asset in batch.assets if asset.asset_type in {"VIDEO", "MEDIA"}}
    thumb_ids = {asset.content_external_id for asset in batch.assets if asset.asset_type == "THUMBNAIL"}
    missing_video = sorted(content_ids - video_ids)
    missing_thumb = sorted(content_ids - thumb_ids)
    if missing_video or missing_thumb:
        parts = []
        if missing_video:
            parts.append(f"missing video assets for {len(missing_video)} contents")
        if missing_thumb:
            parts.append(f"missing thumbnail assets for {len(missing_thumb)} contents")
        return "; ".join(parts)
    return "backend returned no synced items"


def _account_terminal_status(*, failed_count: int, zero_sync_count: int, accounts_total: int, contents_synced: int, contents_collected: int) -> str:
    if contents_synced > 0:
        return "PARTIAL_SUCCESS" if failed_count or zero_sync_count else "SUCCESS"
    if contents_collected > 0:
        return "PARTIAL_SUCCESS"
    if failed_count >= accounts_total:
        return "FAILED"
    return "FAILED" if failed_count else "SUCCESS"


def _home_feed_terminal_status(*, failed_platforms: int, platforms_total: int, contents_synced: int, contents_collected: int) -> str:
    if contents_synced > 0:
        return "PARTIAL_SUCCESS" if failed_platforms else "SUCCESS"
    if contents_collected > 0:
        return "PARTIAL_SUCCESS"
    if failed_platforms >= platforms_total:
        return "FAILED"
    return "SUCCESS"


def _home_feed_task_id(request: HomeFeedJobCreate) -> str:
    value = str((request.metadata or {}).get("taskId") or (request.metadata or {}).get("task_id") or "").strip()
    return value or f"home-feed-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{uuid4().hex[:8]}"


def _home_feed_url(platform: Platform) -> str:
    if platform == Platform.YOUTUBE:
        return "https://www.youtube.com/"
    if platform == Platform.TIKTOK:
        return "https://www.tiktok.com/foryou"
    if platform == Platform.FACEBOOK:
        return "https://www.facebook.com/watch/"
    if platform == Platform.INSTAGRAM:
        return "https://www.instagram.com/"
    if platform == Platform.X:
        return "https://x.com/home"
    return ""


def _home_feed_browser(settings, platform: Platform) -> str:
    return str(getattr(settings, f"{platform.value.lower()}_browser", "") or "chromium")


def _home_feed_cdp_url(settings, platform: Platform) -> str:
    return str(getattr(settings, f"{platform.value.lower()}_browser_cdp_url", "") or "").strip()


def _home_feed_browser_profile(settings, platform: Platform) -> Path | None:
    value = getattr(settings, f"{platform.value.lower()}_browser_profile", None)
    if not value:
        return None
    path = Path(value)
    if str(path).strip() in {"", "."} or not path.exists():
        return None
    return path


def _home_feed_login_account_id(settings, platform: Platform) -> str:
    profile = _home_feed_browser_profile(settings, platform)
    if profile:
        return f"{platform.value.lower()}-{profile.name}"
    cookie = getattr(settings, f"{platform.value.lower()}_cookie_file", None)
    if cookie:
        path = Path(cookie)
        if path.is_file():
            return f"{platform.value.lower()}-{path.stem}"
    return f"{platform.value.lower()}-unconfigured"


def _home_feed_error_code(reason: str) -> str:
    lowered = reason.lower()
    if "no_login_account" in lowered or "profile is not configured" in lowered:
        return "NO_LOGIN_ACCOUNT"
    if "login" in lowered or "sign in" in lowered or "cookie" in lowered or "auth" in lowered:
        return "LOGIN_EXPIRED"
    if "captcha" in lowered or "challenge" in lowered or "bot" in lowered:
        return "PLATFORM_CHALLENGE"
    if "timed out" in lowered or "timeout" in lowered or "connection" in lowered:
        return "NETWORK_TEMPORARY"
    return "COLLECT_FAILED"


def _home_feed_requires_login(platform: Platform, body_text: str, page_url: str) -> bool:
    text = f"{body_text}\n{page_url}".lower()
    if platform == Platform.YOUTUBE:
        return "sign in to like videos" in text or "accounts.google.com/servicelogin" in text
    if platform in {Platform.X, Platform.INSTAGRAM, Platform.TIKTOK, Platform.FACEBOOK}:
        return "log in" in text or "sign in" in text or "login" in text
    return False



def _account_display_name_from_target(account: AccountTargetCreate, normalized_value: str) -> str:
    value = str(account.display_name or "").strip()
    if not value or value == normalized_value:
        return ""
    return value

def _account_display_name_for_sync(platform: Platform, original_value: str, normalized_value: str) -> str:
    stripped = original_value.strip()
    if not stripped:
        return ""
    if platform == Platform.YOUTUBE:
        handle = _youtube_handle_from_text(stripped)
        if handle:
            display = stripped.replace(handle, "").strip()
            return display if display and display != normalized_value else ""
        if stripped.startswith("@"):
            return ""
        if re.search(r"https?://(?:www\.)?youtube\.com/", stripped, flags=re.IGNORECASE):
            return ""
        return stripped
    return ""

def _normalize_account_value(platform: Platform, value: str) -> str:
    stripped = value.strip()
    if platform != Platform.YOUTUBE:
        return stripped
    url_match = re.search(r"https?://(?:www\.)?youtube\.com/[^\s]+", stripped, flags=re.IGNORECASE)
    if url_match:
        return url_match.group(0).rstrip(".,;)")
    channel_match = re.search(r"\b(UC[A-Za-z0-9_-]{20,})\b", stripped)
    if channel_match:
        return channel_match.group(1)
    handle_match = re.search(r"(?<![\w.])@([A-Za-z0-9._-]{3,})\b", stripped)
    if handle_match:
        return f"@{handle_match.group(1)}"
    return stripped


def _account_from_source_url(platform: Platform, source_url: str) -> str:
    """Extract a stable account identifier from a persisted profile URL."""
    value = str(source_url or "").strip()
    if not value:
        return ""
    try:
        parsed = urlparse(value)
    except ValueError:
        return ""
    host = parsed.netloc.lower()
    parts = [part for part in parsed.path.split("/") if part]
    if platform == Platform.TIKTOK and "tiktok.com" in host:
        return f"@{parts[0]}" if parts and parts[0].startswith("@") else ""
    if platform == Platform.INSTAGRAM and "instagram.com" in host:
        if parts and parts[0].lower() not in {"p", "reel", "explore", "accounts", "direct"}:
            return f"@{parts[0].lstrip('@')}"
        return ""
    if platform == Platform.FACEBOOK and "facebook.com" in host:
        query_id = parse_qs(parsed.query).get("id", [""])[0]
        if query_id:
            return query_id
        return parts[0] if parts else ""
    if platform == Platform.X and ("x.com" in host or "twitter.com" in host):
        return parts[0] if parts else ""
    return ""


def _account_target_is_usable(account: AccountTargetCreate) -> bool:
    """Reject display names/placeholders that cannot be collected as URLs."""
    platform = account.platform
    value = str(account.account or "").strip()
    source = str(account.source_url or "").strip()
    if platform == Platform.YOUTUBE:
        # YouTube rows may intentionally store a display name plus a handle;
        # _normalize_account_target extracts the durable handle at runtime.
        return bool(value or source)
    from_url = _account_from_source_url(platform, source)
    if from_url:
        value = from_url
    if not value or "__starter__" in value.lower() or any(char.isspace() for char in value):
        return False
    if platform == Platform.X:
        return bool(re.fullmatch(r"@?[A-Za-z0-9_]{1,15}", value))
    if platform == Platform.FACEBOOK:
        return bool(re.fullmatch(r"[A-Za-z0-9._-]{2,120}", value))
    if platform in {Platform.TIKTOK, Platform.INSTAGRAM}:
        return bool(re.fullmatch(r"@?[^/?#]{2,80}", value))
    return True


def _normalize_account_source_url(platform: Platform, account: str, source_url: str) -> str:
    stripped = source_url.strip()
    if platform != Platform.YOUTUBE or not stripped:
        return stripped
    if account.startswith("@"):
        source_handle = _youtube_handle_from_text(stripped)
        if source_handle and source_handle.lower() != account.lower():
            return ""
    return _youtube_account_videos_url(stripped)


def _looks_like_facebook_account(account: str, source_url: str) -> bool:
    stripped = account.strip()
    if re.fullmatch(r"\d{8,}", stripped):
        return True
    return bool(re.search(r"(?:^|//)(?:www\.)?facebook\.com/", source_url.strip(), flags=re.IGNORECASE))



def _youtube_account_videos_url(source_url: str) -> str:
    if re.search(r"youtube\.com/(?:@[^/?#]+|channel/UC[A-Za-z0-9_-]+|c/[^/?#]+|user/[^/?#]+)/?$", source_url, flags=re.IGNORECASE):
        return source_url.rstrip("/") + "/videos"
    return source_url

def _youtube_handle_from_text(value: str) -> str:
    handle_match = re.search(r"(?<![\w.])@([A-Za-z0-9._-]{3,})\b", value)
    if handle_match:
        return f"@{handle_match.group(1)}"
    return ""


def _account_recency_cutoff(request: AccountJobCreate, metadata: dict[str, Any]) -> datetime | None:
    raw_months = request.recency_months
    if raw_months is None:
        raw_months = (
            metadata.get("account_recency_months")
            or metadata.get("accountRecencyMonths")
            or metadata.get("recency_months")
            or metadata.get("recencyMonths")
        )
    if raw_months is None:
        raw_days = request.recency_days
        if raw_days is None:
            raw_days = (
                metadata.get("account_recency_days")
                or metadata.get("accountRecencyDays")
                or metadata.get("recency_days")
                or metadata.get("recencyDays")
            )
        if raw_days is None:
            return None
        days = int(raw_days)
        if days <= 0:
            return None
        now = _account_recency_now(metadata)
        return now - timedelta(days=days)
    months = int(raw_months)
    if months <= 0:
        return None
    now = _account_recency_now(metadata)
    return now - timedelta(days=months * 30)


def _is_scheduled_account_bulk(request: AccountJobCreate) -> bool:
    """Identify the business scheduler's large, recurring account sweep.

    A user submitting two accounts from the console must retain the requested
    depth and comments.  The scheduler consistently supplies a task ID and
    `SHORT_VIDEO` task kind, which lets us apply a safer profile without
    weakening targeted repair jobs.
    """
    metadata = request.metadata or {}
    task_id = str(metadata.get("taskId") or metadata.get("task_id") or "").strip()
    task_kind = str(metadata.get("taskKind") or metadata.get("task_kind") or "").strip().upper()
    return len(request.accounts) > 1 and bool(task_id) and task_kind == "SHORT_VIDEO"


def _account_collection_profile(
    platform: Platform,
    request: AccountJobCreate,
    settings,
    *,
    account: AccountTargetCreate | None = None,
) -> dict[str, Any]:
    """Return effective per-account work without changing the public API.

    Bulk polling spends its budget discovering current videos.  It defers
    comments, which are the slowest and least reliable part of a fresh
    account sweep, to priority/new-video jobs.  Manual jobs retain the
    caller's exact settings.
    """
    # Priority is an explicit per-account instruction from the business
    # service.  It is not inferred from account age or platform, so turning
    # it on for one newly-added account cannot increase load for all others.
    if account and account.priority:
        metadata = {
            "collection_profile": "PRIORITY_TARGETED",
            "priority_account": True,
            "comments_deferred": False,
        }
        if platform == Platform.YOUTUBE:
            # The browser fallback is attached only to a selected/repair
            # account. Scheduled bulk jobs keep their low-load profile.
            metadata["youtube_browser_comment_fallback"] = True
            metadata["browser_comment_limit"] = min(50, max(1, int(request.metadata.get("max_comments") or 50)))
        return {
            "name": "PRIORITY_TARGETED",
            "mediaLimit": None,
            "collectComments": request.collect_comments,
            "metadata": metadata,
        }
    if _is_scheduled_account_bulk(request):
        media_limit = max(1, int(getattr(settings, "collector_scheduled_bulk_media_items_per_account", 3) or 3))
        metadata = {
            "collection_profile": "SCHEDULED_INCREMENTAL",
            "comments_deferred": True,
        }
        if platform == Platform.YOUTUBE:
            metadata["youtube_playlist_end"] = max(media_limit * 2, media_limit)
        return {
            "name": "SCHEDULED_INCREMENTAL",
            "mediaLimit": media_limit,
            "collectComments": bool(getattr(settings, "collector_scheduled_bulk_collect_comments", False)),
            "metadata": metadata,
        }
    return {
        "name": "REQUESTED",
        "mediaLimit": None,
        "collectComments": request.collect_comments,
        "metadata": {},
    }


def _account_collection_limit(request: AccountJobCreate, settings, profile: dict[str, Any] | None = None) -> int:
    """Apply the host-wide media-download ceiling without hiding metadata jobs."""
    requested = max(1, int(request.limit))
    if not request.download_media:
        return requested
    configured = max(1, int(getattr(settings, "collector_account_media_items_per_account", 10) or 10))
    profile_limit = (profile or {}).get("mediaLimit")
    if profile_limit is not None:
        configured = min(configured, max(1, int(profile_limit)))
    return min(requested, configured)


def _account_collection_metadata(
    platform: Platform,
    request: AccountJobCreate,
    collection_limit: int | None = None,
) -> dict[str, Any]:
    metadata = dict(request.metadata or {})
    if request.recency_months is not None:
        metadata.setdefault("account_recency_months", request.recency_months)
    elif request.recency_days is not None:
        metadata.setdefault("account_recency_days", request.recency_days)
    if platform == Platform.YOUTUBE and request.download_media:
        metadata.setdefault("youtube_try_uncached_details", True)
        effective_limit = collection_limit if collection_limit is not None else request.limit
        # Probe a few extra entries so an unavailable newest video does not
        # make the whole account look empty, without turning into a bulk job.
        metadata.setdefault("youtube_playlist_end", max(effective_limit * 2, effective_limit))
    return metadata


def _account_recency_now(metadata: dict[str, Any]) -> datetime:
    value = metadata.get("account_recency_now") or metadata.get("recency_now")
    if isinstance(value, datetime):
        return _ensure_aware_utc(value)
    if value:
        try:
            return _ensure_aware_utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
        except ValueError:
            pass
    return datetime.now(timezone.utc)


def _filter_account_batch_by_recency(batch: CollectionBatch, cutoff: datetime | None) -> tuple[CollectionBatch, dict[str, Any]]:
    if cutoff is None:
        return batch, {"enabled": False}
    normalized_cutoff = _ensure_aware_utc(cutoff)
    kept_contents = [
        content
        for content in batch.contents
        if content.published_at is None or _ensure_aware_utc(content.published_at) >= normalized_cutoff
    ]
    kept_content_ids = {content.external_id for content in kept_contents}
    kept_author_ids = {content.author_external_id for content in kept_contents if content.author_external_id}
    filtered = CollectionBatch(
        accounts=[account for account in batch.accounts if account.external_id in kept_author_ids],
        contents=kept_contents,
        comments=[comment for comment in batch.comments if comment.content_external_id in kept_content_ids],
        assets=[asset for asset in batch.assets if asset.content_external_id in kept_content_ids],
        next_cursor={"last_external_id": kept_contents[-1].external_id} if kept_contents else {},
        raw=dict(batch.raw or {}),
    )
    filter_info = {
        "enabled": True,
        "cutoff": normalized_cutoff.isoformat(),
        "inputContents": len(batch.contents),
        "outputContents": len(kept_contents),
        "filteredOld": len(batch.contents) - len(kept_contents),
        "missingPublishedAtKept": sum(1 for content in kept_contents if content.published_at is None),
    }
    filtered.raw["account_recency_filter"] = filter_info
    return filtered, filter_info


def _ensure_aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


# Construct the service only after every helper above is defined.  Startup
# reconciliation calls account normalization immediately, so constructing it
# beside the route declarations would make a restart order-dependent.
job_service = CollectorJobService(
    recover_live_jobs=os.getenv("MEDIA_MONITOR_RECOVER_LIVE_JOBS", "0").strip().lower() in {"1", "true", "yes"},
    # Importing this module from a diagnostic command or a test suite must
    # never close a live production batch. Only the managed API process opts
    # into startup reconciliation.
    reconcile_batch_jobs=os.getenv("MEDIA_MONITOR_RECONCILE_JOBS_ON_STARTUP", "0").strip().lower() in {"1", "true", "yes"},
    priority_watch_enabled=get_settings().priority_account_watch_enabled,
)
