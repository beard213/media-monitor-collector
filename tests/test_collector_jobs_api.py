from __future__ import annotations

import threading
import time

from fastapi.testclient import TestClient

from app.domain.enums import Platform
from app.domain.models import CollectedAccount, CollectedContent, CollectionBatch
from app.main import app


def _reset_job_service(collector_jobs, tmp_path):
    monkeypatch_target_base = tmp_path / "collector-jobs"
    collector_jobs.job_service.base_dir = monkeypatch_target_base
    collector_jobs.job_service.pipeline_output_dir = tmp_path / "pipeline-runs"
    with collector_jobs.job_service._lock:
        collector_jobs.job_service._jobs.clear()


def _wait_for_terminal_status(client: TestClient, run_id: str) -> None:
    for _ in range(100):
        status = client.get(f"/collector/jobs/{run_id}").json()["status"]
        if status in {"SUCCESS", "FAILED"}:
            return
        time.sleep(0.01)


def test_create_keyword_collector_job_runs_pipeline_and_exposes_status(tmp_path, monkeypatch):
    from app.api import collector_jobs
    cleanup_calls = []

    def fake_run_pipeline(options, *, output=print):
        output("[PIPELINE] fake keyword run")
        return {
            "run_id": options.run_id,
            "status": "success",
            "platforms": [platform.value for platform in options.platforms],
            "contents_synced": 2,
            "platform_results": [
                {
                    "platform": "INSTAGRAM",
                    "status": "success",
                    "keywords_total": 1,
                    "keywords_collected": 1,
                    "contents_synced": 2,
                    "skipped": [],
                }
            ],
        }

    def fake_cleanup_after_collection(*, settings=None):
        cleanup_calls.append(settings)
        return {"filesRemoved": 0, "bytesRemoved": 0, "dirsRemoved": 0, "gcCollected": 3}

    monkeypatch.setattr(collector_jobs, "run_pipeline", fake_run_pipeline)
    monkeypatch.setattr(collector_jobs, "cleanup_after_collection", fake_cleanup_after_collection)
    _reset_job_service(collector_jobs, tmp_path)
    client = TestClient(app)

    response = client.post(
        "/collector/jobs/keywords",
        json={
            "platforms": ["instagram"],
            "keywordIds": [31],
            "limit": 10,
            "collectComments": True,
            "downloadMedia": True,
            "recencyMonths": 0,
            "continueOnError": True,
            "metadata": {"instagram_search_candidate_limit": 80},
            "runInBackground": False,
        },
    )

    assert response.status_code == 200
    created = response.json()
    assert created["status"] == "SUCCESS"
    assert created["runId"]

    status = client.get(f"/collector/jobs/{created['runId']}").json()
    assert status["jobType"] == "KEYWORD"
    assert status["contentsSynced"] == 2
    assert status["status"] == "SUCCESS"
    assert status["cleanup"]["gcCollected"] == 3
    assert len(cleanup_calls) == 1

    logs = client.get(f"/collector/jobs/{created['runId']}/logs?tail=10").json()
    assert "[PIPELINE] fake keyword run" in logs["lines"]
    assert any(line.startswith("[CLEANUP] done") for line in logs["lines"])


def test_create_account_collector_job_collects_accounts_and_exposes_status(tmp_path, monkeypatch):
    from app.api import collector_jobs
    cleanup_calls = []

    video = tmp_path / "video.mp4"
    thumb = tmp_path / "thumb.jpg"
    video.write_bytes(b"video")
    thumb.write_bytes(b"thumb")

    class FakeAdapter:
        capabilities = set()

        def discover(self, request):
            return CollectionBatch(
                accounts=[CollectedAccount(platform=Platform.YOUTUBE, external_id="creator-1", username="@creator")],
                contents=[CollectedContent(platform=Platform.YOUTUBE, external_id="video-1", author_external_id="creator-1")],
            )

    def fake_get_adapter(platform):
        assert platform == Platform.YOUTUBE
        return FakeAdapter()

    def fake_sync_batch_to_backend(batch, request, *, settings):
        return {
            "items": [{"contentId": 1, "sourceVideoId": batch.contents[0].external_id, "status": "QUEUED"}],
            "agent_ingest": {
                "enabled": True,
                "items": [
                    {"video_id": batch.contents[0].external_id, "status": "SUCCESS"},
                    {"video_id": "video-failed", "status": "FAILED", "error": "timeout"},
                ],
            },
        }

    def fake_cleanup_after_collection(*, settings=None):
        cleanup_calls.append(settings)
        raise RuntimeError("cleanup disk temporarily unavailable")

    settings = collector_jobs.get_settings().model_copy(update={"collector_sync_enabled": True})
    monkeypatch.setattr(collector_jobs, "get_settings", lambda: settings)
    monkeypatch.setattr(collector_jobs, "get_adapter", fake_get_adapter)
    monkeypatch.setattr(collector_jobs, "sync_batch_to_backend", fake_sync_batch_to_backend)
    monkeypatch.setattr(collector_jobs, "cleanup_after_collection", fake_cleanup_after_collection)
    _reset_job_service(collector_jobs, tmp_path)
    client = TestClient(app)

    response = client.post(
        "/collector/jobs/accounts",
        json={
            "accounts": [{"platform": "youtube", "account": "@creator"}],
            "limit": 10,
            "collectComments": True,
            "downloadMedia": True,
            "continueOnError": True,
            "metadata": {"max_comments": 50, "min_free_disk_gb": 0},
            "runInBackground": False,
        },
    )

    assert response.status_code == 200
    created = response.json()
    assert created["status"] == "SUCCESS"

    status = client.get(f"/collector/jobs/{created['runId']}").json()
    assert status["jobType"] == "ACCOUNT"
    assert status["accountsTotal"] == 1
    assert status["accountsCollected"] == 1
    assert status["contentsSynced"] == 1
    assert status["agentSucceeded"] == 1
    assert status["agentFailed"] == 1
    assert status["cleanup"]["status"] == "failed"
    assert status["status"] == "SUCCESS"
    assert len(cleanup_calls) == 1


def test_generic_collector_job_endpoint_dispatches_by_job_type(tmp_path, monkeypatch):
    from app.api import collector_jobs

    def fake_run_pipeline(options, *, output=print):
        output("[PIPELINE] generic keyword run")
        return {"run_id": options.run_id, "status": "success", "contents_synced": 1, "platform_results": []}

    monkeypatch.setattr(collector_jobs, "run_pipeline", fake_run_pipeline)
    _reset_job_service(collector_jobs, tmp_path)
    client = TestClient(app)

    response = client.post(
        "/collector/jobs",
        json={
            "jobType": "KEYWORD",
            "platforms": ["x"],
            "limit": 10,
            "runInBackground": False,
        },
    )

    assert response.status_code == 200
    assert response.json()["jobType"] == "KEYWORD"


def test_duplicate_keyword_click_returns_existing_active_job(tmp_path, monkeypatch):
    from app.api import collector_jobs

    release = threading.Event()

    def fake_run_pipeline(options, *, output=print):
        output("[PIPELINE] waiting")
        release.wait(timeout=5)
        return {"run_id": options.run_id, "status": "success", "contents_synced": 0, "platform_results": []}

    monkeypatch.setattr(collector_jobs, "run_pipeline", fake_run_pipeline)
    _reset_job_service(collector_jobs, tmp_path)
    client = TestClient(app)
    payload = {
        "platforms": ["instagram"],
        "keywordIds": [31],
        "limit": 10,
        "collectComments": True,
        "downloadMedia": True,
        "recencyMonths": 0,
        "metadata": {"instagram_search_candidate_limit": 80},
    }

    first = client.post("/collector/jobs/keywords", json=payload).json()
    second = client.post("/collector/jobs/keywords", json=payload).json()
    release.set()
    _wait_for_terminal_status(client, first["runId"])

    assert first["runId"] == second["runId"]
    assert second["deduplicated"] is True


def test_platform_lock_rejects_different_active_job_on_same_platform(tmp_path, monkeypatch):
    from app.api import collector_jobs

    release = threading.Event()

    def fake_run_pipeline(options, *, output=print):
        output("[PIPELINE] waiting")
        release.wait(timeout=5)
        return {"run_id": options.run_id, "status": "success", "contents_synced": 0, "platform_results": []}

    monkeypatch.setattr(collector_jobs, "run_pipeline", fake_run_pipeline)
    _reset_job_service(collector_jobs, tmp_path)
    client = TestClient(app)

    first = client.post(
        "/collector/jobs/keywords",
        json={"platforms": ["instagram"], "keywordIds": [31], "limit": 10},
    )
    second = client.post(
        "/collector/jobs/keywords",
        json={"platforms": ["instagram"], "keywordIds": [32], "limit": 10},
    )
    release.set()
    _wait_for_terminal_status(client, first.json()["runId"])

    assert first.status_code == 200
    assert second.status_code == 409
    assert "platform is already running" in second.json()["detail"]
