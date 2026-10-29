from types import SimpleNamespace

from app.api.collector_jobs import AccountJobCreate, AccountTargetCreate, CollectorJobService
from app.domain.enums import Platform
from app.ingestion.hls_relay import HlsRelay


def test_youtube_account_work_is_deferred_while_live_is_active(monkeypatch, tmp_path):
    service = CollectorJobService(base_dir=tmp_path / "jobs", pipeline_output_dir=tmp_path / "runs")
    service._jobs["live-1"] = {"jobType": "LIVE", "status": "RUNNING", "request": {"platform": "YOUTUBE"}}
    request = AccountJobCreate(accounts=[AccountTargetCreate(platform=Platform.YOUTUBE, account="@demo")])
    updates = []

    monkeypatch.setattr("app.api.collector_jobs.get_settings", lambda: SimpleNamespace(youtube_defer_batch_while_live=True, youtube_live_priority_wait_seconds=5))
    monkeypatch.setattr(service, "_update_job", lambda run_id, patch: updates.append(patch))
    monkeypatch.setattr("app.api.collector_jobs.time.sleep", lambda _: service._jobs.__setitem__("live-1", {"jobType": "LIVE", "status": "COMPLETED", "request": {"platform": "YOUTUBE"}}))

    service._wait_for_youtube_live_priority("account-1", request)

    assert updates[0]["stage"] == "WAITING_FOR_LIVE_PRIORITY"


def test_relay_diagnostic_hides_signed_source_url(tmp_path):
    relay = HlsRelay(tmp_path, "https://example.invalid/private-signed-url")

    assert relay.diagnostic() == "ffmpeg_not_started"
