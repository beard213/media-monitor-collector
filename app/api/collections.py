from uuid import uuid4

from fastapi import APIRouter, HTTPException

from app.adapters.registry import get_adapter
from app.core.config import get_settings
from app.domain.enums import Capability
from app.domain.models import CollectionRequest, CollectionTarget, CollectorError
from app.ingestion.backend import sync_batch_to_backend
from app.scheduler.contracts import CollectionJobCreate

router = APIRouter(prefix="/collections", tags=["collections"])


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
        "batch": batch,
    }
