from __future__ import annotations

from collections.abc import Callable
import shutil
from typing import Any
from uuid import uuid4

from app.adapters.registry import build_registry
from app.core.config import get_settings
from app.domain.enums import Capability, CollectorErrorCode, CredentialAction, Platform
from app.domain.models import CollectionRequest, CollectionTarget, CollectorError
from app.ingestion.backend import ActiveKeyword, CollectorBackendClient, sync_batch_to_backend
from app.ingestion.keyword_database import KeywordDatabaseClient
from app.ingestion.keyword_recency import filter_keyword_batch_by_recency, keyword_recency_cutoff


def run_keyword_monitor(
    *,
    platforms: list[Platform],
    limit_per_keyword: int,
    collect_comments: bool,
    download_media: bool,
    dry_run: bool,
    metadata: dict,
    backend_client: CollectorBackendClient | None = None,
    keyword_database_client: KeywordDatabaseClient | None = None,
    adapters: dict[Platform, object] | None = None,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> dict:
    backend = backend_client or CollectorBackendClient.from_settings()
    registry = adapters or build_registry()
    requested_platforms = platforms or [Platform.YOUTUBE]
    keywords, keyword_source = _load_keywords(backend, requested_platforms, keyword_database_client=keyword_database_client)
    keywords = _filter_keywords_by_metadata(keywords, metadata)
    _emit_progress(
        progress_callback,
        {
            "event": "keywords_loaded",
            "platforms": [platform.value for platform in requested_platforms],
            "keyword_source": keyword_source,
            "keywords_total": len(keywords),
        },
    )
    collected = []
    skipped = []
    contents_synced = 0

    for keyword in keywords:
        target_platform = _keyword_platform(keyword, requested_platforms)
        if not target_platform:
            item = {"keyword_id": keyword.id, "platform": keyword.platform, "keyword": keyword.keyword, "reason": "unsupported_platform"}
            skipped.append(item)
            _emit_progress(progress_callback, {"event": "keyword_skipped", **item})
            continue
        adapter = registry.get(target_platform)
        if not adapter or Capability.SEARCH_KEYWORD not in getattr(adapter, "capabilities", set()):
            item = {"keyword_id": keyword.id, "platform": keyword.platform, "keyword": keyword.keyword, "reason": "unsupported_capability"}
            skipped.append(item)
            _emit_progress(progress_callback, {"event": "keyword_skipped", **item})
            continue

        _emit_progress(
            progress_callback,
            {"event": "keyword_started", "platform": target_platform.value, "keyword_id": keyword.id, "keyword": keyword.keyword},
        )
        request = _request_for_keyword(
            keyword=keyword,
            platform=target_platform,
            limit_per_keyword=limit_per_keyword,
            collect_comments=collect_comments,
            download_media=download_media,
            dry_run=dry_run,
            metadata=metadata,
        )
        try:
            _ensure_disk_space(metadata)
            batch = adapter.discover(request)
            batch, recency_filter = filter_keyword_batch_by_recency(batch, keyword_recency_cutoff(request.target.metadata))
            counts = {
                "accounts": len(batch.accounts),
                "contents": len(batch.contents),
                "comments": len(batch.comments),
                "assets": len(batch.assets),
            }
            _emit_progress(
                progress_callback,
                {
                    "event": "keyword_collected",
                    "platform": target_platform.value,
                    "keyword_id": keyword.id,
                    "keyword": keyword.keyword,
                    "counts": counts,
                    "recency_filter": recency_filter,
                },
            )
            backend_sync = {"items": []}
            if not dry_run:
                backend_sync = sync_batch_to_backend(batch, request, client=backend, settings=get_settings())
                synced_count = len(backend_sync.get("items") or [])
                contents_synced += synced_count
                _emit_progress(
                    progress_callback,
                    {
                        "event": "keyword_synced",
                        "platform": target_platform.value,
                        "keyword_id": keyword.id,
                        "keyword": keyword.keyword,
                        "contents_synced": synced_count,
                    },
                )
        except CollectorError as exc:
            item = {
                "keyword_id": keyword.id,
                "platform": target_platform.value,
                "keyword": keyword.keyword,
                "reason": exc.code.value,
                "message": exc.message,
                "retryable": exc.retryable,
            }
            skipped.append(item)
            _emit_progress(progress_callback, {"event": "keyword_skipped", **item})
            continue
        collected.append(
            {
                "keyword_id": keyword.id,
                "platform": target_platform.value,
                "keyword": keyword.keyword,
                "counts": counts,
                "recency_filter": recency_filter,
                "backend_sync": backend_sync,
            }
        )

    return {
        "platforms": [platform.value for platform in requested_platforms],
        "keyword_source": keyword_source,
        "keywords_total": len(keywords),
        "keywords_collected": len(collected),
        "contents_synced": contents_synced,
        "items": collected,
        "skipped": skipped,
    }


def _ensure_disk_space(metadata: dict[str, Any]) -> None:
    minimum_free_gb = float(metadata.get("min_free_disk_gb", 2))
    if minimum_free_gb <= 0:
        return
    settings = get_settings()
    free_bytes = shutil.disk_usage(settings.storage_root).free
    minimum_free_bytes = int(minimum_free_gb * 1024**3)
    if free_bytes < minimum_free_bytes:
        free_gb = free_bytes / 1024**3
        raise CollectorError(
            CollectorErrorCode.DISK_LOW,
            f"free disk space {free_gb:.2f} GB is below the {minimum_free_gb:.2f} GB guard",
            retryable=True,
            credential_action=CredentialAction.NONE,
        )


def _emit_progress(callback: Callable[[dict[str, Any]], None] | None, event: dict[str, Any]) -> None:
    if callback:
        callback(event)


def _load_keywords(
    backend: CollectorBackendClient,
    requested_platforms: list[Platform],
    *,
    keyword_database_client: KeywordDatabaseClient | None = None,
) -> tuple[list[ActiveKeyword], str]:
    try:
        return backend.active_keywords(platforms=requested_platforms), "backend_api"
    except CollectorError as exc:
        settings = get_settings()
        if not exc.retryable:
            raise
        if not keyword_database_client and not settings.keyword_db_fallback_enabled:
            raise
        database = keyword_database_client or KeywordDatabaseClient.from_settings(settings)
        return database.active_keywords(platforms=requested_platforms), "database_fallback"


def _filter_keywords_by_metadata(keywords: list[ActiveKeyword], metadata: dict[str, Any]) -> list[ActiveKeyword]:
    raw_ids = metadata.get("keyword_ids") or metadata.get("keywordIds")
    if not raw_ids:
        return keywords
    if isinstance(raw_ids, int):
        allowed_ids = {raw_ids}
    else:
        allowed_ids = {int(item) for item in raw_ids}
    return [keyword for keyword in keywords if keyword.id in allowed_ids]


def _request_for_keyword(
    *,
    keyword: ActiveKeyword,
    platform: Platform,
    limit_per_keyword: int,
    collect_comments: bool,
    download_media: bool,
    dry_run: bool,
    metadata: dict,
) -> CollectionRequest:
    request_metadata = dict(metadata or {})
    request_metadata.update(
        {
            "keyword_id": keyword.id,
            "keyword": keyword.keyword,
            "match_type": keyword.match_type,
            "risk_level": keyword.risk_level,
            "dry_run": dry_run,
        }
    )
    return CollectionRequest(
        job_id=str(uuid4()),
        platform=platform,
        capability=Capability.SEARCH_KEYWORD,
        target=CollectionTarget(
            platform=platform,
            target_type="KEYWORD",
            target_value=keyword.keyword,
            capabilities=[Capability.SEARCH_KEYWORD],
            metadata=request_metadata,
        ),
        limit=limit_per_keyword,
        collect_comments=collect_comments,
        download_media=download_media,
        cursor={"dry_run": dry_run},
    )


def _keyword_platform(keyword: ActiveKeyword, requested_platforms: list[Platform]) -> Platform | None:
    value = keyword.platform.strip().upper()
    if value == "TWITTER":
        value = Platform.X.value
    if value == "ALL":
        return requested_platforms[0] if requested_platforms else Platform.YOUTUBE
    try:
        platform = Platform(value)
    except ValueError:
        return None
    if platform not in requested_platforms:
        return None
    return platform
