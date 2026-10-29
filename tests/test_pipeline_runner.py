import json
import time

import pytest

from app.domain.enums import Platform
from app.pipeline.runner import PipelineOptions, parse_platforms, run_pipeline


def test_parse_platforms_all_uses_default_order():
    assert parse_platforms("all") == [Platform.YOUTUBE, Platform.TIKTOK, Platform.FACEBOOK, Platform.X, Platform.INSTAGRAM]


def test_parse_platforms_accepts_comma_separated_values():
    assert parse_platforms("youtube,tiktok,x") == [Platform.YOUTUBE, Platform.TIKTOK, Platform.X]


def test_parse_platforms_rejects_unknown_platform():
    with pytest.raises(ValueError, match="unknown platform"):
        parse_platforms("youtube,unknown")


def test_run_pipeline_writes_summary_and_platform_results(tmp_path):
    calls = []
    lines = []

    def monitor_runner(**kwargs):
        platform = kwargs["platforms"][0]
        calls.append(platform)
        kwargs["progress_callback"](
            {"event": "keyword_started", "platform": platform.value, "keyword_id": 1, "keyword": f"{platform.value}-keyword"}
        )
        kwargs["progress_callback"](
            {
                "event": "keyword_collected",
                "platform": platform.value,
                "keyword_id": 1,
                "keyword": f"{platform.value}-keyword",
                "counts": {"accounts": 1, "contents": 2, "comments": 3, "assets": 4},
            }
        )
        kwargs["progress_callback"](
            {"event": "keyword_synced", "platform": platform.value, "keyword_id": 1, "keyword": f"{platform.value}-keyword", "contents_synced": 2}
        )
        return {
            "platforms": [platform.value],
            "keyword_source": "backend_api",
            "keywords_total": 1,
            "keywords_collected": 1,
            "contents_synced": 2,
            "items": [],
            "skipped": [],
        }

    result = run_pipeline(
        PipelineOptions(platforms=[Platform.YOUTUBE, Platform.X], output_dir=tmp_path, run_id="run-001"),
        monitor_runner=monitor_runner,
        output=lines.append,
    )

    assert calls == [Platform.YOUTUBE, Platform.X]
    assert result["status"] == "success"
    assert result["contents_synced"] == 4
    assert (tmp_path / "run-001" / "summary.json").exists()
    assert (tmp_path / "run-001" / "youtube.json").exists()
    assert (tmp_path / "run-001" / "x.json").exists()
    summary = json.loads((tmp_path / "run-001" / "summary.json").read_text(encoding="utf-8"))
    assert summary["status"] == "success"
    assert summary["platforms"] == ["YOUTUBE", "X"]
    assert any("[COLLECT] youtube keyword_id=1 contents=2 comments=3 assets=4 accounts=1" in line for line in lines)
    assert any("[SYNC] x keyword_id=1 contents_synced=2" in line for line in lines)


def test_run_pipeline_stops_on_failed_platform_by_default(tmp_path):
    calls = []

    def monitor_runner(**kwargs):
        platform = kwargs["platforms"][0]
        calls.append(platform)
        if platform == Platform.YOUTUBE:
            return {
                "platforms": [platform.value],
                "keywords_total": 1,
                "keywords_collected": 0,
                "contents_synced": 0,
                "items": [],
                "skipped": [{"keyword_id": 1, "platform": "YOUTUBE", "keyword": "bad", "reason": "ACCESS_DENIED"}],
            }
        return {
            "platforms": [platform.value],
            "keywords_total": 1,
            "keywords_collected": 1,
            "contents_synced": 1,
            "items": [],
            "skipped": [],
        }

    result = run_pipeline(
        PipelineOptions(platforms=[Platform.YOUTUBE, Platform.TIKTOK], output_dir=tmp_path, run_id="run-002"),
        monitor_runner=monitor_runner,
        output=lambda line: None,
    )

    assert calls == [Platform.YOUTUBE]
    assert result["status"] == "failed"
    assert result["failed_platform"] == "YOUTUBE"
    assert result["platform_results"][0]["status"] == "failed"


def test_run_pipeline_can_continue_after_failed_platform(tmp_path):
    calls = []

    def monitor_runner(**kwargs):
        platform = kwargs["platforms"][0]
        calls.append(platform)
        return {
            "platforms": [platform.value],
            "keywords_total": 1,
            "keywords_collected": 0 if platform == Platform.YOUTUBE else 1,
            "contents_synced": 0 if platform == Platform.YOUTUBE else 1,
            "items": [],
            "skipped": [{"keyword_id": 1, "platform": "YOUTUBE", "keyword": "bad", "reason": "ACCESS_DENIED"}]
            if platform == Platform.YOUTUBE
            else [],
        }

    result = run_pipeline(
        PipelineOptions(platforms=[Platform.YOUTUBE, Platform.TIKTOK], output_dir=tmp_path, run_id="run-003", continue_on_error=True),
        monitor_runner=monitor_runner,
        output=lambda line: None,
    )

    assert calls == [Platform.YOUTUBE, Platform.TIKTOK]
    assert result["status"] == "failed"
    assert result["contents_synced"] == 1
    assert [item["status"] for item in result["platform_results"]] == ["failed", "success"]


def test_run_pipeline_can_execute_platforms_in_parallel(tmp_path):
    started: list[Platform] = []
    finished: list[Platform] = []

    def monitor_runner(**kwargs):
        platform = kwargs["platforms"][0]
        started.append(platform)
        time.sleep(0.2)
        finished.append(platform)
        return {
            "platforms": [platform.value],
            "keywords_total": 1,
            "keywords_collected": 1,
            "contents_synced": 1,
            "items": [],
            "skipped": [],
        }

    started_at = time.monotonic()
    result = run_pipeline(
        PipelineOptions(
            platforms=[Platform.YOUTUBE, Platform.TIKTOK, Platform.FACEBOOK],
            output_dir=tmp_path,
            run_id="run-parallel",
            parallel_platforms=True,
        ),
        monitor_runner=monitor_runner,
        output=lambda line: None,
    )

    assert time.monotonic() - started_at < 0.45
    assert set(started) == {Platform.YOUTUBE, Platform.TIKTOK, Platform.FACEBOOK}
    assert set(finished) == {Platform.YOUTUBE, Platform.TIKTOK, Platform.FACEBOOK}
    assert result["status"] == "success"
    assert result["contents_synced"] == 3
    assert (tmp_path / "run-parallel" / "youtube.json").exists()
    assert (tmp_path / "run-parallel" / "tiktok.json").exists()
    assert (tmp_path / "run-parallel" / "facebook.json").exists()
