from __future__ import annotations

import json
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, ConfigDict, Field, field_validator

from app.adapters.registry import get_adapter, platform_statuses
from app.core.config import get_settings
from app.domain.enums import Capability, Platform
from app.domain.models import CollectionRequest, CollectionTarget
from app.ingestion.backend import sync_batch_to_backend
from app.ingestion.runtime_cleanup import cleanup_after_collection
from app.pipeline.runner import PipelineOptions, run_pipeline

router = APIRouter(prefix="/collector", tags=["collector-jobs"])
ACTIVE_JOB_STATUSES = {"PENDING", "RUNNING"}


class CollectorJobConflict(Exception):
    pass


class _CamelModel(BaseModel):
    model_config = ConfigDict(populate_by_name=True)


class KeywordJobCreate(_CamelModel):
    platforms: list[Platform]
    keyword_ids: list[int] = Field(default_factory=list, alias="keywordIds")
    limit: int = Field(default=10, ge=1, le=10)
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

    @field_validator("platform", mode="before")
    @classmethod
    def _normalize_account_platform(cls, value):
        return _normalize_platform(value)


class AccountJobCreate(_CamelModel):
    accounts: list[AccountTargetCreate]
    limit: int = Field(default=10, ge=1, le=10)
    collect_comments: bool = Field(default=True, alias="collectComments")
    download_media: bool = Field(default=True, alias="downloadMedia")
    continue_on_error: bool = Field(default=True, alias="continueOnError")
    metadata: dict[str, Any] = Field(default_factory=dict)
    run_in_background: bool = Field(default=True, alias="runInBackground")


class CollectorJobService:
    def __init__(self, *, base_dir: Path = Path("data/collector-jobs"), pipeline_output_dir: Path = Path("data/pipeline-runs")) -> None:
        self.base_dir = base_dir
        self.pipeline_output_dir = pipeline_output_dir
        self._jobs: dict[str, dict[str, Any]] = {}
        self._lock = threading.Lock()

    def create_keyword_job(self, request: KeywordJobCreate) -> dict[str, Any]:
        run_id = self._new_run_id("keyword")
        job = self._initial_job(run_id, "KEYWORD", request.model_dump(by_alias=True))
        job["signature"] = self._job_signature("KEYWORD", request.model_dump(by_alias=True, exclude={"run_in_background"}))
        job["lockedPlatforms"] = sorted(platform.value for platform in request.platforms)
        existing = self._active_job_for(job["signature"], set(job["lockedPlatforms"]))
        if existing:
            return existing
        self._save_job(job)
        self._run_or_spawn(job, request.run_in_background, lambda: self._run_keyword_job(run_id, request))
        return self.get_job(run_id)

    def create_account_job(self, request: AccountJobCreate) -> dict[str, Any]:
        if not request.accounts:
            raise ValueError("at least one account is required")
        run_id = self._new_run_id("account")
        job = self._initial_job(run_id, "ACCOUNT", request.model_dump(by_alias=True))
        job["accountsTotal"] = len(request.accounts)
        job["signature"] = self._job_signature("ACCOUNT", request.model_dump(by_alias=True, exclude={"run_in_background"}))
        job["lockedPlatforms"] = sorted({account.platform.value for account in request.accounts})
        existing = self._active_job_for(job["signature"], set(job["lockedPlatforms"]))
        if existing:
            return existing
        self._save_job(job)
        self._run_or_spawn(job, request.run_in_background, lambda: self._run_account_job(run_id, request))
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

    def tail_logs(self, run_id: str, *, tail: int) -> list[str]:
        log_path = self._log_path(run_id)
        if not log_path.exists():
            return []
        lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
        return lines[-max(1, min(tail, 1000)) :]

    def _run_or_spawn(self, job: dict[str, Any], run_in_background: bool, target) -> None:
        if run_in_background:
            thread = threading.Thread(target=target, name=f"collector-job-{job['runId']}", daemon=False)
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
        try:
            self._mark_running(run_id)
            settings = get_settings()
            started = datetime.now(timezone.utc)
            items: list[dict[str, Any]] = []
            totals = {
                "accountsCollected": 0,
                "contentsCollected": 0,
                "commentsCollected": 0,
                "assetsCollected": 0,
                "contentsSynced": 0,
                "agentSucceeded": 0,
                "agentFailed": 0,
            }
            failed_reason = None
            for account in request.accounts:
                self._append_log(run_id, f"[ACCOUNT] start {account.platform.value.lower()} account={account.account}")
                try:
                    adapter = get_adapter(account.platform)
                    batch = adapter.discover(
                        CollectionRequest(
                            job_id=str(uuid4()),
                            platform=account.platform,
                            capability=Capability.ACCOUNT_TIMELINE,
                            target=CollectionTarget(
                                platform=account.platform,
                                target_type="ACCOUNT",
                                target_value=account.account,
                                source_url=account.source_url,
                                capabilities=[Capability.ACCOUNT_TIMELINE],
                                metadata=dict(request.metadata or {}),
                            ),
                            limit=request.limit,
                            collect_comments=request.collect_comments,
                            download_media=request.download_media,
                        )
                    )
                    backend_sync = {"items": []}
                    if settings.collector_sync_enabled:
                        backend_sync = sync_batch_to_backend(batch, self._account_sync_request(account, request), settings=settings)
                    synced = len(backend_sync.get("items") or [])
                    agent_ingest = backend_sync.get("agent_ingest") or {}
                    agent_items = agent_ingest.get("items") or []
                    agent_succeeded = sum(1 for item in agent_items if isinstance(item, dict) and item.get("status") == "SUCCESS")
                    agent_failed = sum(1 for item in agent_items if isinstance(item, dict) and item.get("status") == "FAILED")
                    totals["accountsCollected"] += 1
                    totals["contentsCollected"] += len(batch.contents)
                    totals["commentsCollected"] += len(batch.comments)
                    totals["assetsCollected"] += len(batch.assets)
                    totals["contentsSynced"] += synced
                    totals["agentSucceeded"] += agent_succeeded
                    totals["agentFailed"] += agent_failed
                    item = {
                        "platform": account.platform.value,
                        "account": account.account,
                        "status": "success",
                        "counts": {
                            "accounts": len(batch.accounts),
                            "contents": len(batch.contents),
                            "comments": len(batch.comments),
                            "assets": len(batch.assets),
                            "agentSucceeded": agent_succeeded,
                            "agentFailed": agent_failed,
                        },
                        "backend_sync": backend_sync,
                        "agent_ingest": agent_ingest,
                    }
                    self._append_log(run_id, f"[ACCOUNT] done {account.platform.value.lower()} account={account.account} synced={synced}")
                except Exception as exc:  # noqa: BLE001 - one account failure should be capturable.
                    failed_reason = f"{type(exc).__name__}: {exc}"
                    item = {"platform": account.platform.value, "account": account.account, "status": "failed", "reason": failed_reason}
                    self._append_log(run_id, f"[ACCOUNT] failed {account.platform.value.lower()} account={account.account} reason={failed_reason}")
                    if not request.continue_on_error:
                        items.append(item)
                        break
                items.append(item)
            summary = {
                "run_id": run_id,
                "status": "failed" if failed_reason else "success",
                "job_type": "ACCOUNT",
                "accounts_total": len(request.accounts),
                "accounts_collected": totals["accountsCollected"],
                "contents_collected": totals["contentsCollected"],
                "comments_collected": totals["commentsCollected"],
                "assets_collected": totals["assetsCollected"],
                "contents_synced": totals["contentsSynced"],
                "agent_succeeded": totals["agentSucceeded"],
                "agent_failed": totals["agentFailed"],
                "items": items,
                "elapsed_seconds": round((datetime.now(timezone.utc) - started).total_seconds(), 3),
            }
            self._run_dir(run_id).mkdir(parents=True, exist_ok=True)
            (self._run_dir(run_id) / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
            if failed_reason:
                self._fail_job(run_id, "ACCOUNT_FAILED", failed_reason, summary)
                return
            self._update_job(
                run_id,
                {
                    "status": "SUCCESS",
                    "finishedAt": self._now(),
                    "accountsTotal": len(request.accounts),
                    "accountsCollected": totals["accountsCollected"],
                    "contentsCollected": totals["contentsCollected"],
                    "commentsCollected": totals["commentsCollected"],
                    "assetsCollected": totals["assetsCollected"],
                    "contentsSynced": totals["contentsSynced"],
                    "agentSucceeded": totals["agentSucceeded"],
                    "agentFailed": totals["agentFailed"],
                    "summaryPath": str(self._run_dir(run_id) / "summary.json"),
                },
            )
        finally:
            self._cleanup_after_job(run_id)

    def _account_sync_request(self, account: AccountTargetCreate, request: AccountJobCreate) -> CollectionRequest:
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
                metadata=dict(request.metadata or {}),
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
        self._update_job(
            run_id,
            {
                "status": "FAILED",
                "finishedAt": self._now(),
                "failedReason": f"{reason}: {message}",
                "summary": result or {},
            },
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
            "request": request,
            "createdAt": self._now(),
            "startedAt": None,
            "finishedAt": None,
            "keywordsTotal": 0,
            "keywordsCollected": 0,
            "accountsTotal": 0,
            "accountsCollected": 0,
            "contentsCollected": 0,
            "commentsCollected": 0,
            "assetsCollected": 0,
            "contentsSynced": 0,
            "agentSucceeded": 0,
            "agentFailed": 0,
            "failedPlatform": None,
            "failedReason": None,
            "summaryPath": str(self._run_dir(run_id) / "summary.json"),
            "logPath": str(self._log_path(run_id)),
            "lockedPlatforms": [],
            "signature": "",
            "cleanup": {},
        }

    def _active_job_for(self, signature: str, platforms: set[str]) -> dict[str, Any] | None:
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
                    platform_text = ",".join(sorted(locked_platforms & platforms))
                    raise CollectorJobConflict(f"platform is already running: {platform_text}")
        return None

    def _job_signature(self, job_type: str, payload: dict[str, Any]) -> str:
        return json.dumps({"jobType": job_type, "payload": payload}, ensure_ascii=False, sort_keys=True, default=str)

    def _update_job(self, run_id: str, updates: dict[str, Any]) -> None:
        with self._lock:
            job = dict(self._jobs[run_id])
            job.update(updates)
            self._jobs[run_id] = job
        self._write_job_file(job)

    def _save_job(self, job: dict[str, Any]) -> None:
        with self._lock:
            self._jobs[job["runId"]] = dict(job)
        self._write_job_file(job)

    def _write_job_file(self, job: dict[str, Any]) -> None:
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self._job_path(job["runId"]).write_text(json.dumps(job, ensure_ascii=False, indent=2), encoding="utf-8")

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


job_service = CollectorJobService()


@router.post("/jobs/keywords")
def create_keyword_job(body: KeywordJobCreate) -> dict[str, Any]:
    try:
        return job_service.create_keyword_job(body)
    except CollectorJobConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/jobs/accounts")
def create_account_job(body: AccountJobCreate) -> dict[str, Any]:
    try:
        return job_service.create_account_job(body)
    except CollectorJobConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/jobs")
def create_collector_job(body: dict[str, Any]) -> dict[str, Any]:
    job_type = str(body.get("jobType") or body.get("job_type") or "").strip().upper()
    if not job_type:
        job_type = "ACCOUNT" if body.get("accounts") else "KEYWORD"
    try:
        if job_type == "KEYWORD":
            return job_service.create_keyword_job(KeywordJobCreate.model_validate(body))
        if job_type == "ACCOUNT":
            return job_service.create_account_job(AccountJobCreate.model_validate(body))
    except CollectorJobConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    raise HTTPException(status_code=400, detail="jobType must be KEYWORD or ACCOUNT")


@router.get("/jobs/{run_id}")
def get_job(run_id: str) -> dict[str, Any]:
    try:
        return job_service.get_job(run_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="collector job not found") from exc


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


def _normalize_platform(value):
    if isinstance(value, str):
        normalized = value.strip().upper()
        if normalized == "TWITTER":
            normalized = "X"
        return normalized
    return value
