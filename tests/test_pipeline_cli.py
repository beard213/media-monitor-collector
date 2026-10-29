from pathlib import Path

from app.domain.enums import Platform
from app.pipeline import run as pipeline_cli


def test_cli_main_runs_with_defaults(monkeypatch, tmp_path):
    captured = {}

    def fake_run_pipeline(options):
        captured["options"] = options
        return {"status": "success"}

    monkeypatch.setattr(pipeline_cli, "run_pipeline", fake_run_pipeline)

    exit_code = pipeline_cli.main(["--output-dir", str(tmp_path)])

    assert exit_code == 0
    assert captured["options"].platforms == [Platform.YOUTUBE, Platform.TIKTOK, Platform.FACEBOOK, Platform.X, Platform.INSTAGRAM]
    assert captured["options"].limit_per_keyword == 10
    assert captured["options"].collect_comments is True
    assert captured["options"].download_media is True
    assert captured["options"].output_dir == tmp_path
    assert captured["options"].run_id


def test_cli_main_accepts_selected_platforms_and_metadata(monkeypatch, tmp_path):
    captured = {}

    def fake_run_pipeline(options):
        captured["options"] = options
        return {"status": "success"}

    monkeypatch.setattr(pipeline_cli, "run_pipeline", fake_run_pipeline)

    exit_code = pipeline_cli.main(
        [
            "--platform",
            "youtube,x",
            "--limit",
            "8",
            "--no-comments",
            "--no-download-media",
            "--dry-run",
            "--continue-on-error",
            "--output-dir",
            str(tmp_path),
            "--run-id",
            "manual-run",
            "--metadata-json",
            '{"browser_comment_limit": 20}',
        ]
    )

    options = captured["options"]
    assert exit_code == 0
    assert options.platforms == [Platform.YOUTUBE, Platform.X]
    assert options.limit_per_keyword == 8
    assert options.collect_comments is False
    assert options.download_media is False
    assert options.dry_run is True
    assert options.continue_on_error is True
    assert options.run_id == "manual-run"
    assert options.metadata == {"browser_comment_limit": 20}


def test_cli_main_returns_one_when_pipeline_fails(monkeypatch, tmp_path):
    def fake_run_pipeline(options):
        return {"status": "failed"}

    monkeypatch.setattr(pipeline_cli, "run_pipeline", fake_run_pipeline)

    assert pipeline_cli.main(["--output-dir", str(tmp_path)]) == 1


def test_cli_main_rejects_limit_above_ten(tmp_path):
    assert pipeline_cli.main(["--limit", "11", "--output-dir", str(Path(tmp_path))]) == 2


def test_cli_main_rejects_unknown_platform(tmp_path):
    assert pipeline_cli.main(["--platform", "youtube,unknown", "--output-dir", str(tmp_path)]) == 2


def test_cli_main_rejects_non_object_metadata(tmp_path):
    assert pipeline_cli.main(["--metadata-json", "[]", "--output-dir", str(tmp_path)]) == 2
