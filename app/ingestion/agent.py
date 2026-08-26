from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

import httpx

from app.core.config import Settings, get_settings
from app.domain.models import CollectionBatch, CollectionRequest, CollectedContent


_RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}


@dataclass
class AgentClient:
    ingest_url: str
    enabled: bool = True
    strategy: str = "rag_repair"
    timeout_seconds: float = 5
    max_attempts: int = 2
    retry_interval_seconds: float = 0.2
    http_client: httpx.Client | None = None

    @classmethod
    def from_settings(cls, settings: Settings | None = None) -> "AgentClient":
        resolved = settings or get_settings()
        base_url = str(getattr(resolved, "agent_base_url", "http://127.0.0.1:8088") or "").rstrip("/")
        ingest_url = str(getattr(resolved, "agent_ingest_url", "") or "").strip()
        if not ingest_url and base_url:
            ingest_url = f"{base_url}/api/agent/ingest"
        return cls(
            ingest_url=ingest_url,
            enabled=bool(getattr(resolved, "agent_ingest_enabled", True)),
            strategy=str(getattr(resolved, "agent_ingest_strategy", "rag_repair") or "rag_repair"),
            timeout_seconds=float(getattr(resolved, "agent_ingest_timeout_seconds", 5) or 5),
            max_attempts=max(1, int(getattr(resolved, "agent_ingest_max_attempts", 2) or 1)),
            retry_interval_seconds=float(getattr(resolved, "agent_ingest_retry_interval_seconds", 0.2) or 0),
        )

    def ingest(self, payload: dict[str, Any]) -> dict[str, Any]:
        if not self.enabled:
            return {"status": "SKIPPED", "reason": "disabled", "attempts": 0, "retryable": False}
        if not self.ingest_url:
            return {"status": "SKIPPED", "reason": "missing_agent_ingest_url", "attempts": 0, "retryable": False}

        outbound = dict(payload)
        outbound.setdefault("strategy", self.strategy)
        client = self.http_client or httpx.Client(timeout=self.timeout_seconds, trust_env=False)
        close_client = self.http_client is None
        try:
            return self._post_with_retry(client, outbound)
        finally:
            if close_client:
                client.close()

    def healthcheck(self) -> dict[str, Any]:
        if not self.enabled:
            return {"ok": True, "enabled": False, "reachable": False, "reason": "disabled"}
        if not self.ingest_url:
            return {"ok": False, "enabled": True, "reachable": False, "reason": "missing_agent_ingest_url"}
        base_url = self._base_url()
        candidates = [f"{base_url}/api/cases", f"{base_url}/health", f"{base_url}/docs"]
        client = self.http_client or httpx.Client(timeout=self.timeout_seconds, trust_env=False)
        close_client = self.http_client is None
        try:
            last_error = ""
            for url in candidates:
                try:
                    response = client.get(url)
                except httpx.HTTPError as exc:
                    last_error = str(exc)
                    continue
                if 200 <= response.status_code < 400:
                    return {"ok": True, "enabled": True, "reachable": True, "url": url, "status_code": response.status_code}
                last_error = f"HTTP {response.status_code}"
            return {"ok": False, "enabled": True, "reachable": False, "url": candidates[0], "error": last_error}
        finally:
            if close_client:
                client.close()

    def _post_with_retry(self, client: httpx.Client, payload: dict[str, Any]) -> dict[str, Any]:
        last_error = ""
        retryable = True
        for attempt in range(1, self.max_attempts + 1):
            try:
                response = client.post(self.ingest_url, headers={"Content-Type": "application/json"}, json=payload)
            except httpx.HTTPError as exc:
                last_error = str(exc)
                retryable = True
            else:
                if 200 <= response.status_code < 300:
                    data = _safe_json(response)
                    return {"status": "SUCCESS", "attempts": attempt, **data}
                last_error = response.text.strip() or response.reason_phrase
                retryable = response.status_code in _RETRYABLE_STATUS_CODES
                if not retryable:
                    return {
                        "status": "FAILED",
                        "attempts": attempt,
                        "retryable": False,
                        "status_code": response.status_code,
                        "error": last_error,
                    }
            if attempt < self.max_attempts and retryable and self.retry_interval_seconds > 0:
                time.sleep(self.retry_interval_seconds * attempt)
        return {"status": "FAILED", "attempts": self.max_attempts, "retryable": retryable, "error": last_error}

    def _base_url(self) -> str:
        if "/api/agent/ingest" in self.ingest_url:
            return self.ingest_url.split("/api/agent/ingest", 1)[0].rstrip("/")
        return self.ingest_url.rstrip("/")


def build_agent_payload(
    batch: CollectionBatch,
    content: CollectedContent,
    request: CollectionRequest,
    *,
    video: Any | None = None,
    thumbnail: Any | None = None,
    strategy: str = "rag_repair",
) -> dict[str, Any]:
    raw = content.raw or {}
    comments = [comment.text for comment in batch.comments if comment.content_external_id == content.external_id and comment.text]
    keyframes = _first_non_empty(
        raw.get("keyframes"),
        raw.get("keyframe_descriptions"),
        raw.get("frame_descriptions"),
    )
    if keyframes is None:
        keyframes = []

    minio_object_key = _object_value(video, "object_key")
    minio_url = minio_object_key or _object_value(video, "url") or _asset_value(batch, content.external_id, "VIDEO", "object_key")
    thumbnail_value = _object_value(thumbnail, "object_key") or _object_value(thumbnail, "url")
    account = next((item for item in batch.accounts if item.external_id == content.author_external_id), None)

    payload = {
        "data_source": request.platform.value.lower(),
        "video_id": content.external_id,
        "video_title": content.title or raw.get("title") or "",
        "content_url": content.url,
        "author_id": content.author_external_id,
        "author_name": (account.username or account.nickname or account.external_id) if account else content.author_external_id,
        "asr_text": str(_first_non_empty(raw.get("asr_text"), raw.get("asr"), raw.get("transcript"), raw.get("ocr_text"), content.description) or ""),
        "description": content.description,
        "comment_list": comments,
        "keyframes": keyframes,
        "minio_url": minio_url,
        "minio_object_key": minio_object_key or minio_url,
        "thumbnail_url": thumbnail_value,
        "strategy": strategy,
        "collector_job_id": request.job_id,
        "target_type": request.target.target_type,
        "target_value": request.target.target_value,
        "metadata": dict(request.target.metadata or {}),
    }
    if request.target.target_type == "KEYWORD":
        payload["keyword"] = str(request.target.metadata.get("keyword") or request.target.target_value)
        keyword_id = request.target.metadata.get("keyword_id") or request.target.metadata.get("keywordId")
        if keyword_id is not None:
            payload["keyword_id"] = keyword_id
    return payload


def _safe_json(response: httpx.Response) -> dict[str, Any]:
    try:
        data = response.json()
    except ValueError:
        return {"raw_response": response.text}
    return data if isinstance(data, dict) else {"raw_response": data}


def _first_non_empty(*values: Any) -> Any | None:
    for value in values:
        if value not in (None, "", [], {}):
            return value
    return None


def _object_value(obj: Any | None, field: str) -> str:
    if obj is None:
        return ""
    value = getattr(obj, field, "")
    return str(value or "")


def _asset_value(batch: CollectionBatch, content_id: str, asset_type: str, field: str) -> str:
    for asset in batch.assets:
        if asset.content_external_id != content_id:
            continue
        if asset.asset_type not in {asset_type, "MEDIA"}:
            continue
        value = getattr(asset, field, "")
        if value:
            return str(value)
    return ""
