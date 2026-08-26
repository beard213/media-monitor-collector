from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from app.domain.models import CollectionBatch

DEFAULT_KEYWORD_RECENCY_MONTHS = 3


def keyword_recency_cutoff(metadata: dict[str, Any]) -> datetime | None:
    raw_months = metadata.get("keyword_recency_months", DEFAULT_KEYWORD_RECENCY_MONTHS)
    months = int(raw_months)
    if months <= 0:
        return None
    now = _metadata_now(metadata)
    return now - timedelta(days=months * 30)


def filter_keyword_batch_by_recency(batch: CollectionBatch, cutoff: datetime | None) -> tuple[CollectionBatch, dict[str, Any]]:
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
        "months": DEFAULT_KEYWORD_RECENCY_MONTHS,
        "cutoff": normalized_cutoff.isoformat(),
        "input_contents": len(batch.contents),
        "output_contents": len(kept_contents),
        "filtered_old": len(batch.contents) - len(kept_contents),
        "missing_published_at_kept": sum(1 for content in kept_contents if content.published_at is None),
    }
    filtered.raw["keyword_recency_filter"] = filter_info
    return filtered, filter_info


def _metadata_now(metadata: dict[str, Any]) -> datetime:
    value = metadata.get("keyword_recency_now")
    if isinstance(value, datetime):
        return _ensure_aware_utc(value)
    if value:
        try:
            return _ensure_aware_utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
        except ValueError:
            pass
    return datetime.now(timezone.utc)


def _ensure_aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
