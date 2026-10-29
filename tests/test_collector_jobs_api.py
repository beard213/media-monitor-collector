from __future__ import annotations

from datetime import datetime, timedelta, timezone
import threading
import time
from types import SimpleNamespace

from fastapi.testclient import TestClient

from app.domain.enums import CollectorErrorCode, Platform
from app.domain.models import CollectedAccount, CollectedContent, CollectionBatch, CollectorError, MediaAsset
from app.main import app


def _reset_job_service(collector_jobs, tmp_path):
    monkeypatch_target_base = tmp_path / "collector-jobs"
    collector_jobs.job_service.base_dir = monkeypatch_target_base
    collector_jobs.job_service.pipeline_output_dir = tmp_path / "pipeline-runs"
    collector_jobs.job_service._priority_watch_state_path = tmp_path / "runtime" / "priority-account-watch.json"
    with collector_jobs.job_service._lock:
        collector_jobs.job_service._jobs.clear()


def _wait_for_terminal_status(client: TestClient, run_id: str) -> None:
    for _ in range(100):
        status = client.get(f"/collector/jobs/{run_id}").json()["status"]
        if status in {"SUCCESS", "PARTIAL_SUCCESS", "FAILED"}:
            return
        time.sleep(0.01)


def test_priority_account_registration_is_persisted(tmp_path):
    from app.api.collector_jobs import AccountJobCreate, AccountTargetCreate, CollectorJobService

    service = CollectorJobService(
        base_dir=tmp_path / "collector-jobs",
        reconcile_batch_jobs=False,
        priority_watch_enabled=False,
    )
    request = AccountJobCreate(
        accounts=[
            AccountTargetCreate(
                platform=Platform.YOUTUBE,
                account="@wenzhaoofficial",
                sourceUrl="https://www.youtube.com/@wenzhaoofficial",
                priority=True,
            )
        ],
        metadata={"priorityPollIntervalSeconds": 300},
    )

    service._register_priority_accounts(request)

    entries = service._load_priority_watch_state()
    entry = entries["YOUTUBE:@wenzhaoofficial"]
    assert entry["intervalSeconds"] == 300
    assert entry["knownContentIds"] == []
    assert entry["enabled"] is True


def test_priority_registration_can_baseline_without_spawning_collection(tmp_path):
    from app.api.collector_jobs import AccountJobCreate, AccountTargetCreate, CollectorJobService

    service = CollectorJobService(
        base_dir=tmp_path / "collector-jobs",
        reconcile_batch_jobs=False,
        priority_watch_enabled=False,
    )
    result = service.create_account_job(
        AccountJobCreate(
            accounts=[AccountTargetCreate(platform=Platform.YOUTUBE, account="@creator", priority=True)],
            metadata={"priorityWatchRegisterOnly": True, "priorityWatchBaselineOnly": True},
        )
    )

    assert result["status"] == "SUCCESS"
    assert result["accountsRegistered"] == 1
    entry = service._load_priority_watch_state()["YOUTUBE:@creator"]
    assert entry["bootstrap"] is True


def test_scheduled_bulk_excludes_independently_watched_account(tmp_path):
    from app.api.collector_jobs import AccountTargetCreate, CollectorJobService

    service = CollectorJobService(base_dir=tmp_path / "collector-jobs", reconcile_batch_jobs=False)
    service._save_priority_watch_state(
        {
            "YOUTUBE:@priority": {
                "platform": "YOUTUBE",
                "account": "@priority",
                "enabled": True,
            }
        }
    )
    filtered, excluded = service._exclude_priority_watch_accounts(
        [
            AccountTargetCreate(platform=Platform.YOUTUBE, account="@priority"),
            AccountTargetCreate(platform=Platform.YOUTUBE, account="@ordinary"),
        ]
    )

    assert excluded == 1
    assert [item.account for item in filtered] == ["@ordinary"]


def test_stale_in_memory_live_job_does_not_block_priority_probe(tmp_path):
    from app.api.collector_jobs import CollectorJobService

    service = CollectorJobService(base_dir=tmp_path / "collector-jobs", reconcile_batch_jobs=False)
    run_id = "live-stale"
    service._jobs[run_id] = {
        "runId": run_id,
        "jobType": "LIVE",
        "status": "RUNNING",
        "request": {"platform": "YOUTUBE"},
    }
    service._job_path(run_id).parent.mkdir(parents=True, exist_ok=True)
    service._job_path(run_id).write_text(
        '{"runId":"live-stale","jobType":"LIVE","status":"SUCCESS","request":{"platform":"YOUTUBE"}}',
        encoding="utf-8",
    )

    assert service._active_live_run_for_platform(Platform.YOUTUBE) == ""


def test_priority_watch_rejects_youtube_channel_id_as_content_id():
    from app.api.collector_jobs import CollectorJobService

    assert CollectorJobService._priority_watch_content_id_valid(Platform.YOUTUBE, "UCtAIPjABiQD3qjlEl1T5VpA") is False
    assert CollectorJobService._priority_watch_content_id_valid(Platform.YOUTUBE, "dQw4w9WgXcQ") is True


def test_priority_watch_uses_youtube_cooldown_for_rate_limited_probe():
    from app.api.collector_jobs import CollectorJobService

    settings = SimpleNamespace(
        youtube_auth_circuit_breaker_seconds=3600,
        youtube_cookie_pool_cooldown_seconds=900,
    )
    error = CollectorError(CollectorErrorCode.RATE_LIMITED, "temporary rate limit", retryable=True)

    assert CollectorJobService._priority_watch_failure_delay(Platform.YOUTUBE, error, settings, 60) == 900


def test_priority_watch_backs_off_non_transient_probe_errors():
    from app.api.collector_jobs import CollectorJobService

    settings = SimpleNamespace(
        youtube_auth_circuit_breaker_seconds=3600,
        youtube_cookie_pool_cooldown_seconds=900,
    )
    error = CollectorError(CollectorErrorCode.ACCESS_DENIED, "account unavailable", retryable=False)

    assert CollectorJobService._priority_watch_failure_delay(Platform.FACEBOOK, error, settings, 60) == 900


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
            "metadata": {"max_comments": 50},
            "runInBackground": False,
        },
    )

    assert response.status_code == 200
    created = response.json()
    assert created["status"] == "SUCCESS"

    status = client.get(f"/collector/jobs/{created['runId']}").json()
    assert status["jobType"] == "ACCOUNT"
    assert status["accountsTotal"] == 1
    assert status["accountsProcessed"] == 1
    assert status["accountsCollected"] == 1
    assert status["contentsSynced"] == 1
    assert status["agentSucceeded"] == 1
    assert status["agentFailed"] == 1
    assert status["cleanup"]["status"] == "failed"
    assert status["status"] == "SUCCESS"
    assert len(cleanup_calls) == 1



def test_create_account_collector_job_background_start_returns_run_id_and_finishes(tmp_path, monkeypatch):
    from app.api import collector_jobs

    release = threading.Event()

    class FakeAdapter:
        capabilities = set()

        def discover(self, request):
            release.wait(timeout=5)
            return CollectionBatch(
                accounts=[CollectedAccount(platform=Platform.YOUTUBE, external_id="creator-1", username="@creator")],
                contents=[CollectedContent(platform=Platform.YOUTUBE, external_id="video-1", author_external_id="creator-1")],
            )

    monkeypatch.setattr(collector_jobs, "get_adapter", lambda platform: FakeAdapter())
    monkeypatch.setattr(
        collector_jobs,
        "sync_batch_to_backend",
        lambda batch, request, *, settings: {"items": [{"contentId": 1, "sourceVideoId": "video-1", "status": "QUEUED"}]},
    )
    monkeypatch.setattr(collector_jobs, "cleanup_after_collection", lambda *, settings=None: {})
    _reset_job_service(collector_jobs, tmp_path)
    client = TestClient(app)

    response = client.post(
        "/collector/jobs/accounts",
        json={
            "accounts": [{"platform": "youtube", "account": "@creator"}],
            "limit": 1,
            "metadata": {"callbackBaseUrl": "http://backend:18082/api/v1", "taskId": "front-click-1"},
        },
    )

    assert response.status_code == 200
    created = response.json()
    assert created["runId"]
    assert created["status"] in {"PENDING", "RUNNING"}

    release.set()
    _wait_for_terminal_status(client, created["runId"])
    status = client.get(f"/collector/jobs/{created['runId']}").json()
    assert status["status"] == "SUCCESS"
    assert status["contentsSynced"] == 1


def test_scheduled_youtube_bulk_uses_incremental_profile_and_defers_comments(tmp_path, monkeypatch):
    """A scheduler sweep must not turn every account into a 10-video comment crawl."""
    from app.api import collector_jobs

    seen_requests = []

    class FakeAdapter:
        capabilities = set()

        def discover(self, request):
            seen_requests.append(request)
            account = request.target.target_value
            return CollectionBatch(
                accounts=[CollectedAccount(platform=Platform.YOUTUBE, external_id=account, username=account)],
                contents=[CollectedContent(platform=Platform.YOUTUBE, external_id=f"video-{account}", author_external_id=account)],
            )

    settings = collector_jobs.get_settings()
    monkeypatch.setattr(settings, "collector_sync_enabled", True)
    monkeypatch.setattr(settings, "collector_scheduled_bulk_enabled", True)
    monkeypatch.setattr(settings, "collector_scheduled_bulk_media_items_per_account", 3)
    monkeypatch.setattr(settings, "collector_scheduled_bulk_account_pause_seconds", 0)
    monkeypatch.setattr(settings, "collector_scheduled_bulk_chunk_pause_seconds", 0)
    monkeypatch.setattr(collector_jobs, "get_adapter", lambda platform: FakeAdapter())
    monkeypatch.setattr(
        collector_jobs,
        "sync_batch_to_backend",
        lambda batch, request, *, settings: {"items": [{"sourceVideoId": batch.contents[0].external_id, "status": "QUEUED"}]},
    )
    monkeypatch.setattr(collector_jobs, "cleanup_after_collection", lambda *, settings=None: {})
    _reset_job_service(collector_jobs, tmp_path)
    client = TestClient(app)

    response = client.post(
        "/collector/jobs/accounts",
        json={
            "accounts": [
                {"platform": "youtube", "account": "@creatorone"},
                {"platform": "youtube", "account": "@creatortwo"},
            ],
            "limit": 10,
            "collectComments": True,
            "downloadMedia": True,
            "metadata": {"taskId": "scheduler-1", "taskKind": "SHORT_VIDEO"},
            "runInBackground": False,
        },
    )

    assert response.status_code == 200
    assert len(seen_requests) == 2
    assert all(request.limit == 3 for request in seen_requests)
    assert all(request.collect_comments is False for request in seen_requests)
    assert all(request.target.metadata["comments_deferred"] is True for request in seen_requests)
    assert response.json()["collectionProfile"] == "SCHEDULED_INCREMENTAL"


def test_scheduled_bulk_priority_account_runs_first_with_requested_comments(tmp_path, monkeypatch):
    """A newly-added priority account is not buried behind the bulk sweep."""
    from app.api import collector_jobs

    seen_requests = []

    class FakeAdapter:
        capabilities = set()

        def discover(self, request):
            seen_requests.append(request)
            account = request.target.target_value
            return CollectionBatch(
                accounts=[CollectedAccount(platform=Platform.X, external_id=account, username=account)],
                contents=[CollectedContent(platform=Platform.X, external_id=f"video-{account}", author_external_id=account)],
            )

    settings = collector_jobs.get_settings()
    monkeypatch.setattr(settings, "collector_sync_enabled", True)
    monkeypatch.setattr(settings, "collector_scheduled_bulk_enabled", True)
    monkeypatch.setattr(settings, "collector_scheduled_bulk_media_items_per_account", 3)
    monkeypatch.setattr(settings, "collector_scheduled_bulk_account_pause_seconds", 0)
    monkeypatch.setattr(settings, "collector_scheduled_bulk_chunk_pause_seconds", 0)
    monkeypatch.setattr(collector_jobs, "get_adapter", lambda platform: FakeAdapter())
    monkeypatch.setattr(
        collector_jobs,
        "sync_batch_to_backend",
        lambda batch, request, *, settings: {"items": [{"sourceVideoId": batch.contents[0].external_id, "status": "QUEUED"}]},
    )
    monkeypatch.setattr(collector_jobs, "cleanup_after_collection", lambda *, settings=None: {})
    _reset_job_service(collector_jobs, tmp_path)
    client = TestClient(app)

    response = client.post(
        "/collector/jobs/accounts",
        json={
            "accounts": [
                {"platform": "x", "account": "ordinary"},
                {"platform": "x", "account": "new_priority", "priority": True},
            ],
            "limit": 10,
            "collectComments": True,
            "downloadMedia": True,
            "metadata": {"taskId": "scheduler-priority-1", "taskKind": "SHORT_VIDEO"},
            "runInBackground": False,
        },
    )

    assert response.status_code == 200
    assert [item.target.target_value for item in seen_requests] == ["new_priority", "ordinary"]
    assert seen_requests[0].limit == 10
    assert seen_requests[0].collect_comments is True
    assert seen_requests[0].target.metadata["collection_profile"] == "PRIORITY_TARGETED"
    assert seen_requests[1].limit == 3
    assert seen_requests[1].collect_comments is False


def test_manual_single_account_is_not_blocked_by_bulk_platform_circuit(tmp_path, monkeypatch):
    """An operator retry must not inherit a scheduler batch's cooldown."""
    from app.api import collector_jobs

    calls = []

    class FakeAdapter:
        capabilities = set()

        def discover(self, request):
            calls.append(request.target.target_value)
            return CollectionBatch(
                accounts=[CollectedAccount(platform=Platform.YOUTUBE, external_id="creator-1", username="@creator")],
                contents=[CollectedContent(platform=Platform.YOUTUBE, external_id="video-1", author_external_id="creator-1")],
            )

    monkeypatch.setattr(collector_jobs, "get_adapter", lambda platform: FakeAdapter())
    monkeypatch.setattr(
        collector_jobs,
        "sync_batch_to_backend",
        lambda batch, request, *, settings: {"items": [{"sourceVideoId": "video-1", "status": "QUEUED"}]},
    )
    monkeypatch.setattr(collector_jobs, "cleanup_after_collection", lambda *, settings=None: {})
    _reset_job_service(collector_jobs, tmp_path)
    collector_jobs.job_service._account_platform_circuit_until.clear()
    collector_jobs.job_service._account_platform_circuit_until[Platform.YOUTUBE] = (
        time.monotonic() + 3600,
        "YOUTUBE_AUTH_REQUIRED: platform collection paused for 3600s after AUTH_EXPIRED",
    )
    client = TestClient(app)

    response = client.post(
        "/collector/jobs/accounts",
        json={
            "accounts": [{"platform": "youtube", "account": "@creator"}],
            "limit": 1,
            "runInBackground": False,
        },
    )

    assert response.status_code == 200
    assert response.json()["status"] == "SUCCESS"
    assert calls == ["@creator"]
    collector_jobs.job_service._account_platform_circuit_until.clear()


def test_bulk_accounts_skipped_by_circuit_is_failed_not_false_success(tmp_path, monkeypatch):
    from app.api import collector_jobs

    monkeypatch.setattr(collector_jobs, "cleanup_after_collection", lambda *, settings=None: {})
    _reset_job_service(collector_jobs, tmp_path)
    collector_jobs.job_service._account_platform_circuit_until.clear()
    collector_jobs.job_service._account_platform_circuit_until[Platform.YOUTUBE] = (
        time.monotonic() + 3600,
        "YOUTUBE_AUTH_REQUIRED: platform collection paused for 3600s after AUTH_EXPIRED",
    )
    client = TestClient(app)

    response = client.post(
        "/collector/jobs/accounts",
        json={
            "accounts": [
                {"platform": "youtube", "account": "@creator-one"},
                {"platform": "youtube", "account": "@creator-two"},
            ],
            "limit": 1,
            "runInBackground": False,
        },
    )

    assert response.status_code == 200
    assert response.json()["status"] == "FAILED"
    assert response.json()["failedAccounts"] == 2
    assert response.json()["failedReason"].startswith("ACCOUNT_FAILED: YOUTUBE_AUTH_REQUIRED")
    collector_jobs.job_service._account_platform_circuit_until.clear()

def test_account_collector_normalizes_youtube_display_name_and_filters_recency(tmp_path, monkeypatch):
    from app.api import collector_jobs

    captured = {}

    class FakeAdapter:
        capabilities = set()

        def discover(self, request):
            captured["target_value"] = request.target.target_value
            captured["source_url"] = request.target.source_url
            return CollectionBatch(
                accounts=[CollectedAccount(platform=Platform.YOUTUBE, external_id="creator-1", username="@wenzhaoofficial")],
                contents=[
                    CollectedContent(
                        platform=Platform.YOUTUBE,
                        external_id="recent-video",
                        author_external_id="creator-1",
                        published_at=datetime(2026, 8, 20, 0, 0, tzinfo=timezone.utc),
                    ),
                    CollectedContent(
                        platform=Platform.YOUTUBE,
                        external_id="old-video",
                        author_external_id="creator-1",
                        published_at=datetime(2026, 7, 20, 0, 0, tzinfo=timezone.utc),
                    ),
                ],
            )

    def fake_get_adapter(platform):
        assert platform == Platform.YOUTUBE
        return FakeAdapter()

    def fake_sync_batch_to_backend(batch, request, *, settings):
        captured["synced_ids"] = [content.external_id for content in batch.contents]
        captured["sync_target_value"] = request.target.target_value
        captured["sync_metadata"] = request.target.metadata
        return {"items": [{"sourceVideoId": content.external_id} for content in batch.contents]}

    monkeypatch.setattr(collector_jobs, "get_adapter", fake_get_adapter)
    monkeypatch.setattr(collector_jobs, "sync_batch_to_backend", fake_sync_batch_to_backend)
    monkeypatch.setattr(collector_jobs, "cleanup_after_collection", lambda *, settings=None: {})
    _reset_job_service(collector_jobs, tmp_path)
    client = TestClient(app)

    response = client.post(
        "/collector/jobs/accounts",
        json={
            "accounts": [
                {
                    "platform": "youtube",
                    "account": "文昭談古論今 -Wen Zhao Official @wenzhaoofficial",
                    "sourceUrl": "https://www.youtube.com/@wenzhaoofficia/videos",
                }
            ],
            "limit": 10,
            "collectComments": False,
            "downloadMedia": False,
            "recencyMonths": 1,
            "metadata": {"account_recency_now": "2026-09-08T00:00:00+00:00"},
            "runInBackground": False,
        },
    )

    assert response.status_code == 200
    status = response.json()
    assert status["status"] == "SUCCESS"
    assert captured["target_value"] == "@wenzhaoofficial"
    assert captured["source_url"] == ""
    assert captured["sync_target_value"] == "@wenzhaoofficial"
    assert captured["sync_metadata"]["account_display_name"] == "文昭談古論今 -Wen Zhao Official"
    assert captured["sync_metadata"]["account_handle"] == "@wenzhaoofficial"
    assert captured["synced_ids"] == ["recent-video"]
    assert status["summary"]["items"][0]["account"] == "文昭談古論今 -Wen Zhao Official"
    assert status["summary"]["items"][0]["targetAccount"] == "@wenzhaoofficial"
    assert status["contentsCollected"] == 1
    assert status["contentsSynced"] == 1


def test_account_collector_uses_youtube_handle_from_source_url_when_account_is_display_name(tmp_path, monkeypatch):
    from app.api import collector_jobs

    captured = {}

    class FakeAdapter:
        capabilities = set()

        def discover(self, request):
            captured["target"] = request.target.target_value
            captured["source_url"] = request.target.source_url
            return CollectionBatch(
                accounts=[CollectedAccount(platform=Platform.YOUTUBE, external_id="UC1234567890123456789012", username="@funtv8964")],
                contents=[CollectedContent(platform=Platform.YOUTUBE, external_id="video-1", author_external_id="UC1234567890123456789012")],
            )

    monkeypatch.setattr(collector_jobs, "get_adapter", lambda platform: FakeAdapter())
    monkeypatch.setattr(
        collector_jobs,
        "sync_batch_to_backend",
        lambda batch, request, *, settings: {"items": [{"sourceVideoId": "video-1", "status": "QUEUED"}]},
    )
    monkeypatch.setattr(collector_jobs, "cleanup_after_collection", lambda *, settings=None: {})
    _reset_job_service(collector_jobs, tmp_path)
    client = TestClient(app)

    response = client.post(
        "/collector/jobs/accounts",
        json={
            "accounts": [{
                "platform": "youtube",
                "account": "攝徒日記Fun TV",
                "sourceUrl": "https://www.youtube.com/@funtv8964",
            }],
            "limit": 1,
            "runInBackground": False,
        },
    )

    assert response.status_code == 200
    assert response.json()["status"] == "SUCCESS"
    assert captured == {
        "target": "@funtv8964",
        "source_url": "https://www.youtube.com/@funtv8964/videos",
    }



def test_account_collector_accepts_frontend_display_name_field(tmp_path, monkeypatch):
    from app.api import collector_jobs

    captured = {}

    class FakeAdapter:
        capabilities = set()

        def discover(self, request):
            captured["target_value"] = request.target.target_value
            video = tmp_path / "recent-video.mp4"
            thumbnail = tmp_path / "recent-video.jpg"
            video.write_bytes(b"video")
            thumbnail.write_bytes(b"thumb")
            return CollectionBatch(
                accounts=[CollectedAccount(platform=Platform.YOUTUBE, external_id="UC-dasheng", username="@dashengmedia", nickname="Dasheng Community")],
                contents=[CollectedContent(platform=Platform.YOUTUBE, external_id="recent-video", author_external_id="UC-dasheng")],
                assets=[
                    MediaAsset(platform=Platform.YOUTUBE, content_external_id="recent-video", asset_type="VIDEO", local_path=str(video)),
                    MediaAsset(platform=Platform.YOUTUBE, content_external_id="recent-video", asset_type="THUMBNAIL", local_path=str(thumbnail)),
                ],
            )

    def fake_sync_batch_to_backend(batch, request, *, settings):
        captured["sync_metadata"] = request.target.metadata
        captured["sync_target_value"] = request.target.target_value
        return {"items": [{"sourceVideoId": "recent-video", "status": "QUEUED"}]}

    monkeypatch.setattr(collector_jobs, "get_adapter", lambda platform: FakeAdapter())
    monkeypatch.setattr(collector_jobs, "sync_batch_to_backend", fake_sync_batch_to_backend)
    monkeypatch.setattr(collector_jobs, "cleanup_after_collection", lambda *, settings=None: {})
    _reset_job_service(collector_jobs, tmp_path)
    client = TestClient(app)

    response = client.post(
        "/collector/jobs/accounts",
        json={
            "accounts": [
                {
                    "platform": "youtube",
                    "account": "@dashengmedia",
                    "displayName": "大聲姐和朋友們",
                }
            ],
            "limit": 10,
            "collectComments": False,
            "downloadMedia": True,
            "runInBackground": False,
        },
    )

    assert response.status_code == 200
    status = response.json()
    assert status["status"] == "SUCCESS"
    assert captured["target_value"] == "@dashengmedia"
    assert captured["sync_target_value"] == "@dashengmedia"
    assert captured["sync_metadata"]["account_display_name"] == "大聲姐和朋友們"
    assert captured["sync_metadata"]["account_handle"] == "@dashengmedia"
    assert status["summary"]["items"][0]["account"] == "大聲姐和朋友們"
    assert status["summary"]["items"][0]["targetAccount"] == "@dashengmedia"

def test_account_collector_defaults_to_one_year_and_youtube_detail_download(tmp_path, monkeypatch):
    from app.api import collector_jobs

    captured = {}
    now = datetime(2026, 9, 9, 0, 0, tzinfo=timezone.utc)

    class FakeAdapter:
        capabilities = set()

        def discover(self, request):
            captured["metadata"] = request.target.metadata
            captured["limit"] = request.limit
            return CollectionBatch(
                accounts=[CollectedAccount(platform=Platform.YOUTUBE, external_id="creator-1", username="@creator")],
                contents=[
                    CollectedContent(
                        platform=Platform.YOUTUBE,
                        external_id="recent-video",
                        author_external_id="creator-1",
                        published_at=now - timedelta(days=6),
                    ),
                    CollectedContent(
                        platform=Platform.YOUTUBE,
                        external_id="old-video",
                        author_external_id="creator-1",
                        published_at=now - timedelta(days=366),
                    ),
                ],
            )

    def fake_sync_batch_to_backend(batch, request, *, settings):
        captured["synced_ids"] = [content.external_id for content in batch.contents]
        captured["sync_metadata"] = request.target.metadata
        return {"items": [{"sourceVideoId": content.external_id} for content in batch.contents]}

    monkeypatch.setattr(collector_jobs, "get_adapter", lambda platform: FakeAdapter())
    monkeypatch.setattr(collector_jobs, "sync_batch_to_backend", fake_sync_batch_to_backend)
    monkeypatch.setattr(collector_jobs, "cleanup_after_collection", lambda *, settings=None: {})
    _reset_job_service(collector_jobs, tmp_path)
    client = TestClient(app)

    response = client.post(
        "/collector/jobs/accounts",
        json={
            "accounts": [{"platform": "youtube", "account": "@creator"}],
            "metadata": {"account_recency_now": now.isoformat()},
            "runInBackground": False,
        },
    )

    assert response.status_code == 200
    status = response.json()
    assert captured["limit"] == 10
    assert captured["metadata"]["account_recency_days"] == 365
    assert captured["metadata"]["youtube_try_uncached_details"] is True
    assert captured["metadata"]["youtube_playlist_end"] == 20
    assert captured["synced_ids"] == ["recent-video"]
    assert captured["sync_metadata"]["account_recency_days"] == 365
    assert status["request"]["recencyDays"] == 365
    assert status["contentsCollected"] == 1
    assert status["contentsSynced"] == 1



def test_account_display_name_for_sync_keeps_youtube_plain_frontend_name():
    from app.api.collector_jobs import _account_display_name_for_sync

    assert (
        _account_display_name_for_sync(
            Platform.YOUTUBE,
            "文昭談古論今 -Wen Zhao Official",
            "文昭談古論今 -Wen Zhao Official",
        )
        == "文昭談古論今 -Wen Zhao Official"
    )
    assert _account_display_name_for_sync(Platform.YOUTUBE, "@wenzhaoofficial", "@wenzhaoofficial") == ""

def test_account_collector_normalizes_youtube_channel_source_to_videos_tab(tmp_path, monkeypatch):
    from app.api import collector_jobs

    captured = {}

    class FakeAdapter:
        capabilities = set()

        def discover(self, request):
            captured["source_url"] = request.target.source_url
            return CollectionBatch(
                accounts=[CollectedAccount(platform=Platform.YOUTUBE, external_id="creator-1", username="@creator")],
                contents=[CollectedContent(platform=Platform.YOUTUBE, external_id="video-1", author_external_id="creator-1")],
            )

    monkeypatch.setattr(collector_jobs, "get_adapter", lambda platform: FakeAdapter())
    monkeypatch.setattr(collector_jobs, "sync_batch_to_backend", lambda batch, request, *, settings: {"items": [{"sourceVideoId": "video-1"}]})
    monkeypatch.setattr(collector_jobs, "cleanup_after_collection", lambda *, settings=None: {})
    _reset_job_service(collector_jobs, tmp_path)
    client = TestClient(app)

    response = client.post(
        "/collector/jobs/accounts",
        json={
            "accounts": [{"platform": "youtube", "account": "@ChinainFocusNTD", "sourceUrl": "https://www.youtube.com/channel/UCBOqkAGTtzZVmKvY4SwdZ2g"}],
            "runInBackground": False,
        },
    )

    assert response.status_code == 200
    assert captured["source_url"].endswith("/videos")


def test_account_collector_routes_numeric_youtube_account_to_facebook(tmp_path, monkeypatch):
    from app.api import collector_jobs

    captured = {}

    class FakeAdapter:
        capabilities = set()

        def discover(self, request):
            captured["platform"] = request.platform
            captured["target_value"] = request.target.target_value
            return CollectionBatch(
                accounts=[CollectedAccount(platform=Platform.FACEBOOK, external_id="fb-user", username="fb-user")],
                contents=[CollectedContent(platform=Platform.FACEBOOK, external_id="video-1", author_external_id="fb-user")],
            )

    def fake_get_adapter(platform):
        captured["adapter_platform"] = platform
        return FakeAdapter()

    monkeypatch.setattr(collector_jobs, "get_adapter", fake_get_adapter)
    monkeypatch.setattr(collector_jobs, "sync_batch_to_backend", lambda batch, request, *, settings: {"items": [{"sourceVideoId": "video-1"}]})
    monkeypatch.setattr(collector_jobs, "cleanup_after_collection", lambda *, settings=None: {})
    _reset_job_service(collector_jobs, tmp_path)
    client = TestClient(app)

    response = client.post(
        "/collector/jobs/accounts",
        json={
            "accounts": [{"platform": "youtube", "account": "61590818619292", "sourceUrl": ""}],
            "runInBackground": False,
        },
    )

    assert response.status_code == 200
    status = response.json()
    assert captured["adapter_platform"] == Platform.FACEBOOK
    assert captured["platform"] == Platform.FACEBOOK
    assert captured["target_value"] == "61590818619292"
    assert status["summary"]["items"][0]["platform"] == "FACEBOOK"
    assert status["summary"]["items"][0]["targetAccount"] == "61590818619292"


def test_home_feed_job_refreshes_collects_and_syncs_ranked_items(tmp_path, monkeypatch):
    from app.api import collector_jobs

    captured = {"sync_requests": []}

    class FakeBrowserSearchCollector:
        def collect_with_context(self, **kwargs):
            captured["home_collect"] = kwargs
            return SimpleNamespace(links=["https://www.youtube.com/watch?v=video-1", "https://www.youtube.com/watch?v=video-2"], body_text="", page_url="https://www.youtube.com/")

    class FakeAdapter:
        capabilities = set()

        def discover(self, request):
            captured.setdefault("detail_requests", []).append(request)
            video_id = request.target.source_url.rsplit("=", 1)[-1]
            video = tmp_path / f"{video_id}.mp4"
            thumb = tmp_path / f"{video_id}.jpg"
            video.write_bytes(b"video")
            thumb.write_bytes(b"thumb")
            return CollectionBatch(
                accounts=[CollectedAccount(platform=Platform.YOUTUBE, external_id="author-1", username="@author")],
                contents=[CollectedContent(platform=Platform.YOUTUBE, external_id=video_id, author_external_id="author-1")],
                assets=[
                    MediaAsset(platform=Platform.YOUTUBE, content_external_id=video_id, asset_type="VIDEO", local_path=str(video)),
                    MediaAsset(platform=Platform.YOUTUBE, content_external_id=video_id, asset_type="THUMBNAIL", local_path=str(thumb)),
                ],
            )

    def fake_sync_batch_to_backend(batch, request, *, settings):
        captured["sync_requests"].append(request)
        return {"items": [{"contentId": len(captured["sync_requests"]), "sourceVideoId": batch.contents[0].external_id, "status": "QUEUED"}]}

    monkeypatch.setattr(collector_jobs, "BrowserSearchCollector", lambda: FakeBrowserSearchCollector())
    monkeypatch.setattr(collector_jobs, "get_adapter", lambda platform: FakeAdapter())
    monkeypatch.setattr(collector_jobs, "sync_batch_to_backend", fake_sync_batch_to_backend)
    monkeypatch.setattr(collector_jobs, "cleanup_after_collection", lambda *, settings=None: {})
    monkeypatch.setattr(collector_jobs, "_home_feed_browser_profile", lambda settings, platform: tmp_path)
    _reset_job_service(collector_jobs, tmp_path)
    client = TestClient(app)

    response = client.post(
        "/collector/jobs/home-feed",
        json={
            "platforms": ["youtube"],
            "limitPerPlatform": 5,
            "refreshHome": True,
            "collectComments": False,
            "downloadMedia": True,
            "continueOnError": True,
            "runInBackground": False,
            "metadata": {
                "taskId": "overseas-home-feed-test",
                "callbackBaseUrl": "http://backend:18082/api/v1",
            },
        },
    )

    assert response.status_code == 202
    status = response.json()
    assert status["jobType"] == "HOME_FEED"
    assert status["taskId"] == "overseas-home-feed-test"
    assert status["status"] == "SUCCESS"
    assert status["platformsTotal"] == 1
    assert status["platformsProcessed"] == 1
    assert status["contentsCollected"] == 2
    assert status["contentsSynced"] == 2
    assert captured["home_collect"]["url"] == "https://www.youtube.com/"
    assert captured["home_collect"]["refresh"] is True
    assert captured["home_collect"]["limit"] == 5
    assert len(captured["detail_requests"]) == 2
    assert captured["sync_requests"][0].target.metadata["collectionType"] == "HOME_FEED"
    assert captured["sync_requests"][0].target.metadata["homeFeedBatchId"] == "overseas-home-feed-test"
    assert captured["sync_requests"][0].target.metadata["recommendationRank"] == 1
    assert captured["sync_requests"][1].target.metadata["recommendationRank"] == 2



def test_home_feed_job_marks_partial_success_when_one_platform_fails(tmp_path, monkeypatch):
    from app.api import collector_jobs

    class FakeBrowserSearchCollector:
        def collect_with_context(self, **kwargs):
            if kwargs["platform"] == Platform.YOUTUBE:
                return SimpleNamespace(links=["https://www.youtube.com/watch?v=video-1"], body_text="", page_url="https://www.youtube.com/")
            return SimpleNamespace(links=[], body_text="Sign in", page_url="https://www.instagram.com/")

    class FakeAdapter:
        capabilities = set()

        def discover(self, request):
            video = tmp_path / "video-1.mp4"
            thumb = tmp_path / "video-1.jpg"
            video.write_bytes(b"video")
            thumb.write_bytes(b"thumb")
            return CollectionBatch(
                accounts=[CollectedAccount(platform=Platform.YOUTUBE, external_id="author-1", username="@author")],
                contents=[CollectedContent(platform=Platform.YOUTUBE, external_id="video-1", author_external_id="author-1")],
                assets=[
                    MediaAsset(platform=Platform.YOUTUBE, content_external_id="video-1", asset_type="VIDEO", local_path=str(video)),
                    MediaAsset(platform=Platform.YOUTUBE, content_external_id="video-1", asset_type="THUMBNAIL", local_path=str(thumb)),
                ],
            )

    monkeypatch.setattr(collector_jobs, "BrowserSearchCollector", lambda: FakeBrowserSearchCollector())
    monkeypatch.setattr(collector_jobs, "get_adapter", lambda platform: FakeAdapter())
    monkeypatch.setattr(collector_jobs, "sync_batch_to_backend", lambda batch, request, *, settings: {"items": [{"contentId": 1, "sourceVideoId": "video-1", "status": "QUEUED"}]})
    monkeypatch.setattr(collector_jobs, "cleanup_after_collection", lambda *, settings=None: {})
    monkeypatch.setattr(collector_jobs, "_home_feed_browser_profile", lambda settings, platform: tmp_path)
    _reset_job_service(collector_jobs, tmp_path)
    client = TestClient(app)

    response = client.post(
        "/collector/jobs/home-feed",
        json={
            "platforms": ["youtube", "instagram"],
            "limitPerPlatform": 1,
            "runInBackground": False,
            "metadata": {"taskId": "partial-home-feed"},
        },
    )

    assert response.status_code == 202
    status = response.json()
    assert status["status"] == "PARTIAL_SUCCESS"
    assert status["contentsSynced"] == 1
    assert status["failedPlatforms"] == 1
    assert status["summary"]["status"] == "partial_success"

def test_home_feed_job_is_idempotent_by_task_id(tmp_path, monkeypatch):
    from app.api import collector_jobs

    monkeypatch.setattr(collector_jobs, "BrowserSearchCollector", lambda: object())
    monkeypatch.setattr(collector_jobs, "cleanup_after_collection", lambda *, settings=None: {})
    monkeypatch.setattr(collector_jobs, "_home_feed_browser_profile", lambda settings, platform: tmp_path)
    _reset_job_service(collector_jobs, tmp_path)
    client = TestClient(app)
    payload = {
        "platforms": ["youtube"],
        "limitPerPlatform": 5,
        "runInBackground": False,
        "metadata": {"taskId": "same-home-feed-task"},
    }

    first = client.post("/collector/jobs/home-feed", json=payload)
    second = client.post("/collector/jobs/home-feed", json=payload)

    assert first.status_code == 202
    assert second.status_code == 202
    assert first.json()["runId"] == second.json()["runId"]
    assert second.json()["deduplicated"] is True


def test_home_feed_job_marks_youtube_sign_in_page_as_login_expired(tmp_path, monkeypatch):
    from app.api import collector_jobs

    class FakeBrowserSearchCollector:
        def collect_with_context(self, **kwargs):
            return SimpleNamespace(
                links=[],
                body_text="Sign in to like videos, comment, and subscribe.",
                page_url="https://www.youtube.com/",
            )

    monkeypatch.setattr(collector_jobs, "BrowserSearchCollector", lambda: FakeBrowserSearchCollector())
    monkeypatch.setattr(collector_jobs, "cleanup_after_collection", lambda *, settings=None: {})
    monkeypatch.setattr(collector_jobs, "_home_feed_browser_profile", lambda settings, platform: tmp_path)
    _reset_job_service(collector_jobs, tmp_path)
    client = TestClient(app)

    response = client.post(
        "/collector/jobs/home-feed",
        json={
            "platforms": ["youtube"],
            "limitPerPlatform": 5,
            "runInBackground": False,
            "metadata": {"taskId": "home-feed-login-expired"},
        },
    )

    assert response.status_code == 202
    status = response.json()
    assert status["status"] == "FAILED"
    assert status["failedPlatforms"] == 1
    assert status["summary"]["items"][0]["errorCode"] == "LOGIN_EXPIRED"



def test_create_live_collector_job_collects_syncs_and_exposes_status(tmp_path, monkeypatch):
    from app.api import collector_jobs

    video = tmp_path / "live.mp4"
    video.write_bytes(b"live-video")
    thumbnail = tmp_path / "live.jpg"
    thumbnail.write_bytes(b"live-thumb")
    captured = {}

    class FakeAdapter:
        def discover(self, request):
            captured["collection_request"] = request
            return CollectionBatch(
                accounts=[CollectedAccount(platform=Platform.YOUTUBE, external_id="UC-live", username="@live", nickname="Live Creator")],
                contents=[CollectedContent(platform=Platform.YOUTUBE, external_id="live-video-1", url=request.target.source_url, content_type="VIDEO", author_external_id="UC-live")],
                assets=[
                    MediaAsset(platform=Platform.YOUTUBE, content_external_id="live-video-1", asset_type="VIDEO", local_path=str(video)),
                    MediaAsset(platform=Platform.YOUTUBE, content_external_id="live-video-1", asset_type="THUMBNAIL", local_path=str(thumbnail)),
                ],
            )

    def fake_sync_batch_to_backend(batch, request, *, settings):
        captured["sync_request"] = request
        captured["content_type"] = batch.contents[0].content_type
        return {"items": [{"contentId": 5931, "sourceVideoId": "live-video-1", "status": "QUEUED", "mediaUrl": "http://minio/live.mp4"}]}

    monkeypatch.setattr(collector_jobs, "get_adapter", lambda platform: FakeAdapter())
    monkeypatch.setattr(collector_jobs, "sync_batch_to_backend", fake_sync_batch_to_backend)
    monkeypatch.setattr(collector_jobs, "cleanup_after_collection", lambda *, settings=None: {})
    _reset_job_service(collector_jobs, tmp_path)

    client = TestClient(app)
    created = client.post(
        "/collector/jobs/live",
        json={
            "platform": "youtube",
            "url": "https://www.youtube.com/watch?v=refmlUAQssM",
            "liveClipSeconds": 15,
            "runInBackground": False,
        },
    ).json()

    status = client.get(f"/collector/jobs/{created['runId']}").json()

    assert status["jobType"] == "LIVE"
    assert status["status"] == "SUCCESS"
    assert status["stage"] == "COMPLETED"
    assert status["lastHeartbeatAt"]
    assert status["updatedAt"]
    assert status["diagnostics"]["platform"] == "YOUTUBE"
    assert status["diagnostics"]["cookieSource"] in {"none", "cookie_file", "cookie_pool", "browser_profile"}
    assert status["contentsCollected"] == 1
    assert status["contentsSynced"] == 1
    assert status["summary"]["items"][0]["backend_sync"]["items"][0]["contentId"] == 5931
    assert captured["collection_request"].capability.value == "LIVE_RESOLVE"
    assert captured["collection_request"].target.metadata["collectorJobType"] == "LIVE"
    assert captured["collection_request"].target.metadata["collectionType"] == "USERNAME"
    assert captured["collection_request"].target.metadata["youtube_live_clip_seconds"] == 15
    assert captured["sync_request"].target.metadata["collectionType"] == "USERNAME"
    assert captured["content_type"] == "LIVE"



def test_live_job_exposes_collector_error_code(tmp_path, monkeypatch):
    from app.api import collector_jobs
    from app.domain.enums import CollectorErrorCode, CredentialAction
    from app.domain.models import CollectorError

    class FakeAdapter:
        def discover(self, request):
            raise CollectorError(
                CollectorErrorCode.AUTH_EXPIRED,
                "YouTube sign in required",
                retryable=False,
                credential_action=CredentialAction.REAUTH,
            )

    monkeypatch.setattr(collector_jobs, "get_adapter", lambda platform: FakeAdapter())
    monkeypatch.setattr(collector_jobs, "cleanup_after_collection", lambda *, settings=None: {})
    _reset_job_service(collector_jobs, tmp_path)

    client = TestClient(app)
    created = client.post(
        "/collector/jobs/live",
        json={
            "platform": "youtube",
            "url": "https://www.youtube.com/watch?v=login-required",
            "runInBackground": False,
        },
    ).json()

    status = client.get(f"/collector/jobs/{created['runId']}").json()
    assert status["status"] == "FAILED"
    assert status["stage"] == "FAILED"
    assert status["failedCode"] == "AUTH_EXPIRED"
    assert status["failedAction"] == "REAUTH"
    assert status["retryable"] is False
    assert "YouTube sign in required" in status["failedReason"]

def test_generic_collector_job_endpoint_dispatches_live_job(tmp_path, monkeypatch):
    from app.api import collector_jobs

    called = {}

    def fake_create_live_job(request):
        called["request"] = request
        return {"runId": "live-1", "jobType": "LIVE", "status": "PENDING"}

    monkeypatch.setattr(collector_jobs.job_service, "create_live_job", fake_create_live_job)
    _reset_job_service(collector_jobs, tmp_path)

    client = TestClient(app)
    response = client.post(
        "/collector/jobs",
        json={"jobType": "LIVE", "platform": "youtube", "sourceUrl": "https://www.youtube.com/watch?v=refmlUAQssM"},
    )

    assert response.status_code == 200
    assert response.json()["jobType"] == "LIVE"
    assert called["request"].url == "https://www.youtube.com/watch?v=refmlUAQssM"


def test_legacy_account_endpoint_dispatches_tiktok_live_job(tmp_path, monkeypatch):
    """The business backend has not migrated from /jobs/accounts yet."""
    from app.api import collector_jobs

    called = {}

    def fake_create_live_job(request):
        called["request"] = request
        return {"runId": "live-tiktok-1", "jobType": "LIVE", "status": "PENDING"}

    monkeypatch.setattr(collector_jobs.job_service, "create_live_job", fake_create_live_job)
    _reset_job_service(collector_jobs, tmp_path)
    client = TestClient(app)

    response = client.post(
        "/collector/jobs/accounts",
        json={
            "jobType": "LIVE",
            "accounts": [
                {
                    "platform": "tiktok",
                    "account": "@bigmouth556",
                    "sourceUrl": "https://www.tiktok.com/@bigmouth556",
                    "accountId": 4321,
                }
            ],
            "liveClipSeconds": 15,
            "runInBackground": True,
        },
    )

    assert response.status_code == 200
    assert response.json()["jobType"] == "LIVE"
    request = called["request"]
    assert request.platform == Platform.TIKTOK
    assert request.url == "https://www.tiktok.com/@bigmouth556/live"
    assert request.realtime is True
    assert request.live_clip_seconds == 15
    assert request.metadata["accountId"] == "4321"
    assert request.max_runtime_seconds == 86400


def test_account_endpoint_rejects_tiktok_live_url_without_downstream_account_id(tmp_path, monkeypatch):
    from app.api import collector_jobs

    called = {}

    def fake_create_live_job(request):
        called["request"] = request
        return {"runId": "live-tiktok-2", "jobType": "LIVE", "status": "PENDING"}

    monkeypatch.setattr(collector_jobs.job_service, "create_live_job", fake_create_live_job)
    _reset_job_service(collector_jobs, tmp_path)
    client = TestClient(app)

    # This is the shape submitted by the existing overseas UI: mode is ACCOUNT,
    # but sourceUrl is the actual TikTok Live room.
    response = client.post(
        "/collector/jobs/accounts",
        json={
            "accounts": [{"platform": "TIKTOK", "account": "Pai", "sourceUrl": "https://www.tiktok.com/@bigmouth556/live?enter_from_merge=others_homepage"}],
            "metadata": {"taskId": "overseas-account-1", "mode": "ACCOUNT"},
        },
    )

    assert response.status_code == 400
    assert "numeric accountId" in response.json()["detail"]
    assert "request" not in called


def test_live_endpoint_accepts_nested_account_contract(tmp_path, monkeypatch):
    from app.api import collector_jobs

    called = {}

    def fake_create_live_job(request):
        called["request"] = request
        return {"runId": "live-1", "jobType": "LIVE", "status": "PENDING"}

    monkeypatch.setattr(collector_jobs.job_service, "create_live_job", fake_create_live_job)
    _reset_job_service(collector_jobs, tmp_path)
    client = TestClient(app)

    response = client.post(
        "/collector/jobs/live",
        json={
            "taskId": "overseas-live-1",
            "platform": "TIKTOK",
            "account": {"accountId": 4321, "username": "bigmouth556", "sourceUrl": "https://www.tiktok.com/@bigmouth556"},
            "options": {"videoSegmentSeconds": 20, "collectMedia": True, "collectChat": True},
        },
    )

    assert response.status_code == 202
    request = called["request"]
    assert request.url == "https://www.tiktok.com/@bigmouth556/live"
    assert request.live_clip_seconds == 20
    assert request.collect_comments is True
    assert request.metadata["accountId"] == "4321"
    assert request.metadata["upstreamTaskId"] == "overseas-live-1"


def test_legacy_live_zero_clip_uses_bounded_segment_default(tmp_path, monkeypatch):
    from app.api import collector_jobs

    called = {}

    def fake_create_live_job(request):
        called["request"] = request
        return {"runId": "live-clip-1", "jobType": "LIVE", "status": "PENDING"}

    monkeypatch.setattr(collector_jobs.job_service, "create_live_job", fake_create_live_job)
    _reset_job_service(collector_jobs, tmp_path)
    client = TestClient(app)

    response = client.post(
        "/collector/jobs/accounts",
        json={
            "jobType": "LIVE",
            "liveClipSeconds": 0,
            "accounts": [{"platform": "TIKTOK", "account": "@bigmouth556", "sourceUrl": "https://www.tiktok.com/@bigmouth556/live", "accountId": 4321}],
        },
    )

    assert response.status_code == 200
    assert called["request"].live_clip_seconds == 15

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


def test_account_scheduler_deduplicates_volatile_task_metadata_across_persisted_jobs(tmp_path, monkeypatch):
    """A 15-minute scheduler tick must not start another identical batch."""
    from app.api import collector_jobs

    _reset_job_service(collector_jobs, tmp_path)
    first_request = collector_jobs.AccountJobCreate.model_validate(
        {
            "accounts": [{"platform": "youtube", "account": "@creator"}],
            "limit": 3,
            "metadata": {
                "taskId": "overseas-account-first",
                "requestedAt": "2026-09-29T08:55:00Z",
                "scope": "OVERSEAS",
            },
        }
    )
    persisted = collector_jobs.job_service._initial_job("account-existing", "ACCOUNT", first_request.model_dump(by_alias=True))
    persisted.update(
        {
            "status": "RUNNING",
            "accountsTotal": 1,
            "accountScopeSignature": collector_jobs.job_service._account_scope_signature(first_request),
        }
    )
    collector_jobs.job_service._write_job_file(persisted)

    # Simulate a post-restart process: only the persisted active job remains.
    with collector_jobs.job_service._lock:
        collector_jobs.job_service._jobs.clear()

    second_request = collector_jobs.AccountJobCreate.model_validate(
        {
            "accounts": [{"platform": "youtube", "account": "@creator"}],
            "limit": 3,
            "metadata": {
                "taskId": "overseas-account-second",
                "requestedAt": "2026-09-29T09:10:00Z",
                "scope": "OVERSEAS",
            },
        }
    )
    duplicate = collector_jobs.job_service.create_account_job(second_request)

    assert duplicate["runId"] == "account-existing"
    assert duplicate["deduplicated"] is True


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


def test_keyword_job_accepts_bounded_twenty_item_limit(tmp_path, monkeypatch):
    from app.api import collector_jobs

    monkeypatch.setattr(
        collector_jobs,
        "run_pipeline",
        lambda options, *, output=print: {
            "run_id": options.run_id,
            "status": "success",
            "contents_synced": 0,
            "platform_results": [],
        },
    )
    _reset_job_service(collector_jobs, tmp_path)
    client = TestClient(app)

    response = client.post(
        "/collector/jobs/keywords",
        json={"platforms": ["tiktok"], "keywordIds": [159], "limit": 20, "runInBackground": False},
    )

    assert response.status_code == 200
    assert response.json()["request"]["limit"] == 20


def test_stopping_queued_account_job_releases_its_platform_lock(tmp_path):
    from app.api import collector_jobs

    _reset_job_service(collector_jobs, tmp_path)
    job = collector_jobs.job_service._initial_job("account-pending", "ACCOUNT", {})
    job.update({"status": "PENDING", "stage": "QUEUED", "lockedPlatforms": ["TIKTOK"]})
    collector_jobs.job_service._save_job(job)

    stopped = collector_jobs.job_service.stop_account_job("account-pending")

    assert stopped["status"] == "CANCELLED"
    assert stopped["stage"] == "CANCELLED"


def test_collection_request_allows_keyword_depth_twenty():
    from app.domain.enums import Capability, Platform
    from app.domain.models import CollectionRequest, CollectionTarget

    request = CollectionRequest(
        job_id="keyword-depth-20",
        platform=Platform.TIKTOK,
        capability=Capability.SEARCH_KEYWORD,
        target=CollectionTarget(platform=Platform.TIKTOK, target_type="KEYWORD", target_value="CCP China"),
        limit=20,
    )

    assert request.limit == 20


def test_account_job_queues_behind_keyword_refresh_on_same_platform(tmp_path, monkeypatch):
    """The periodic keyword + account pair must not lose its account half."""
    from app.api import collector_jobs

    keyword_started = threading.Event()
    release_keyword = threading.Event()
    account_started = threading.Event()

    def fake_run_pipeline(options, *, output=print):
        keyword_started.set()
        release_keyword.wait(timeout=5)
        return {"run_id": options.run_id, "status": "success", "contents_synced": 0, "platform_results": []}

    class FakeAdapter:
        def discover(self, request):
            account_started.set()
            return CollectionBatch()

    monkeypatch.setattr(collector_jobs, "run_pipeline", fake_run_pipeline)
    monkeypatch.setattr(collector_jobs, "get_adapter", lambda platform: FakeAdapter())
    monkeypatch.setattr(collector_jobs, "cleanup_after_collection", lambda *, settings=None: {})
    _reset_job_service(collector_jobs, tmp_path)
    client = TestClient(app)

    keyword = client.post(
        "/collector/jobs/keywords",
        json={"platforms": ["youtube"], "keywordIds": [31], "limit": 1},
    )
    assert keyword_started.wait(timeout=1)
    account = client.post(
        "/collector/jobs/accounts",
        json={"accounts": [{"platform": "youtube", "account": "@creator"}], "limit": 1},
    )

    assert keyword.status_code == 200
    assert account.status_code == 200
    assert account.json()["status"] in {"PENDING", "RUNNING"}
    assert not account_started.wait(timeout=0.1)

    release_keyword.set()
    assert account_started.wait(timeout=2)
    _wait_for_terminal_status(client, keyword.json()["runId"])
    _wait_for_terminal_status(client, account.json()["runId"])


def test_live_job_can_run_while_account_refresh_uses_same_platform(tmp_path, monkeypatch):
    from app.api import collector_jobs

    release = threading.Event()

    def fake_account_job(run_id, request):
        collector_jobs.job_service._mark_running(run_id)
        release.wait(timeout=5)
        collector_jobs.job_service._update_job(run_id, {"status": "SUCCESS", "finishedAt": collector_jobs.job_service._now()})

    def fake_live_job(run_id, request):
        collector_jobs.job_service._mark_running(run_id)
        release.wait(timeout=5)
        collector_jobs.job_service._update_job(run_id, {"status": "SUCCESS", "finishedAt": collector_jobs.job_service._now()})

    monkeypatch.setattr(collector_jobs.job_service, "_run_account_job", fake_account_job)
    monkeypatch.setattr(collector_jobs.job_service, "_run_live_job", fake_live_job)
    _reset_job_service(collector_jobs, tmp_path)
    client = TestClient(app)

    account = client.post(
        "/collector/jobs/accounts",
        json={"accounts": [{"platform": "facebook", "account": "page-1"}]},
    )
    live = client.post(
        "/collector/jobs/live",
        json={
            "platform": "facebook",
            "url": "https://www.facebook.com/page-1/videos/123",
            "metadata": {"accountId": 342589},
        },
    )
    release.set()

    assert account.status_code in {200, 202}
    assert live.status_code == 202
    assert live.json()["jobType"] == "LIVE"


def test_restart_closes_interrupted_keyword_job_but_retains_progress(tmp_path):
    from app.api.collector_jobs import CollectorJobService

    service = CollectorJobService(base_dir=tmp_path / "collector-jobs")
    job = service._initial_job("keyword-interrupted", "KEYWORD", {"platforms": ["YOUTUBE"]})
    job.update({"status": "RUNNING", "contentsCollected": 2, "contentsSynced": 1})
    service._write_job_file(job)

    service._reconcile_interrupted_batch_jobs()
    recovered = service.get_job("keyword-interrupted")

    assert recovered["status"] == "PARTIAL_SUCCESS"
    assert recovered["stage"] == "INTERRUPTED"
    assert recovered["failedCode"] == "COLLECTOR_RESTARTED"


def test_restart_recovers_only_accounts_without_confirmed_sync(tmp_path, monkeypatch):
    from app.api.collector_jobs import AccountJobCreate, CollectorJobService

    service = CollectorJobService(base_dir=tmp_path / "collector-jobs", reconcile_batch_jobs=False)
    request = AccountJobCreate.model_validate(
        {
            "accounts": [
                {"platform": "youtube", "account": "@already-synced"},
                {"platform": "youtube", "account": "@needs-recovery"},
            ],
            "downloadMedia": True,
            "runInBackground": True,
        }
    )
    job = service._initial_job("account-interrupted", "ACCOUNT", request.model_dump(by_alias=True))
    original_scope = service._account_scope_signature(request)
    job.update(
        {
            "status": "RUNNING",
            "accountScopeSignature": original_scope,
            "accountCheckpoints": [
                {
                    "platform": "YOUTUBE",
                    "targetAccount": "@already-synced",
                    "status": "success",
                    "counts": {"synced": 1},
                }
            ],
        }
    )
    service._write_job_file(job)
    captured = {}

    def fake_create_account_job(recovery_request):
        captured["request"] = recovery_request
        return {"runId": "account-continuation"}

    monkeypatch.setattr(service, "create_account_job", fake_create_account_job)
    service._reconcile_interrupted_batch_jobs()

    assert [account.account for account in captured["request"].accounts] == ["@needs-recovery"]
    parent = service.get_job("account-interrupted")
    assert parent["stage"] == "RECOVERY_QUEUED"
    assert parent["recoveryRunId"] == "account-continuation"
    continuation = service.get_job("account-continuation")
    assert continuation["accountScopeSignature"] == original_scope
    assert continuation["recoveredFromRunId"] == "account-interrupted"
