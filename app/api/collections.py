from uuid import uuid4

from fastapi import APIRouter, HTTPException

from app.adapters.registry import get_adapter
from app.core.config import get_settings
from app.domain.enums import Capability
from app.domain.models import CollectionRequest, CollectionTarget, CollectorError
from app.ingestion.backend import sync_batch_to_backend
from app.ingestion.keyword_monitor import run_keyword_monitor
from app.scheduler.contracts import CollectionJobCreate, KeywordMonitorRunCreate

router = APIRouter(prefix="/collections", tags=["collections"])


@router.post("/keywords/run")
def run_keyword_monitor_collection(body: KeywordMonitorRunCreate) -> dict:
    try:
        return run_keyword_monitor(
            platforms=body.platforms,
            limit_per_keyword=body.limit_per_keyword,
            collect_comments=body.collect_comments,
            download_media=body.download_media,
            dry_run=body.dry_run,
            metadata=dict(body.metadata or {}),
        )
    except CollectorError as exc:
        raise HTTPException(
            status_code=502 if exc.retryable else 400,
            detail={
                "error_code": exc.code,
                "message": exc.message,
                "retryable": exc.retryable,
                "credential_action": exc.credential_action,
            },
        ) from exc


@router.post("/run")
def run_collection(body: CollectionJobCreate) -> dict:
    adapter = get_adapter(body.platform)
    if body.capability not in adapter.capabilities:
        raise HTTPException(
            status_code=400,
            detail=f"{body.platform} does not support {body.capability}",
        )

    metadata = dict(body.metadata or {})
    metadata["dry_run"] = body.dry_run
    metadata["timeout_seconds"] = body.timeout_seconds
    target = CollectionTarget(
        platform=body.platform,
        target_type=body.target_type,
        target_value=body.target_value,
        source_url=body.source_url,
        interval_seconds=metadata.get("interval_seconds", 3600),
        capabilities=[body.capability],
        metadata=metadata,
    )
    request = CollectionRequest(
        job_id=str(uuid4()),
        platform=body.platform,
        capability=body.capability,
        target=target,
        limit=body.limit,
        collect_comments=body.collect_comments,
        download_media=body.download_media,
        cursor={"dry_run": body.dry_run},
    )

    backend_sync = {"items": []}
    try:
        if body.capability == Capability.COMMENTS:
            batch = adapter.collect_comments(request)
        elif body.capability == Capability.PROFILE:
            batch = adapter.collect_profile(request)
        elif body.capability in {Capability.MEDIA_DOWNLOAD, Capability.LIVE_RESOLVE}:
            batch = adapter.resolve_media(request)
        else:
            batch = adapter.discover(request)
        settings = get_settings()
        if settings.collector_sync_enabled:
            backend_sync = sync_batch_to_backend(batch, request, settings=settings)
    except CollectorError as exc:
        raise HTTPException(
            status_code=502 if exc.retryable else 400,
            detail={
                "error_code": exc.code,
                "message": exc.message,
                "retryable": exc.retryable,
                "credential_action": exc.credential_action,
            },
        ) from exc

    agent_ingest = backend_sync.get("agent_ingest", {}) if isinstance(backend_sync, dict) else {}
    agent_items = agent_ingest.get("items", []) if isinstance(agent_ingest, dict) else []
    agent_success = sum(1 for item in agent_items if isinstance(item, dict) and item.get("status") == "SUCCESS")
    agent_failed = sum(1 for item in agent_items if isinstance(item, dict) and item.get("status") == "FAILED")

    return {
        "job_id": request.job_id,
        "platform": body.platform,
        "capability": body.capability,
        "dry_run": body.dry_run,
        "counts": {
            "accounts": len(batch.accounts),
            "contents": len(batch.contents),
            "comments": len(batch.comments),
            "assets": len(batch.assets),
        },
        "backend_sync": backend_sync,
        "agent_ingest": agent_ingest,
        "agent_counts": {
            "success": agent_success,
            "failed": agent_failed,
        },
        "batch": batch,
    }
