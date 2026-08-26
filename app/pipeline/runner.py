from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Callable
from uuid import uuid4

from app.domain.enums import Platform
from app.ingestion.keyword_monitor import run_keyword_monitor

DEFAULT_PLATFORM_ORDER = [Platform.YOUTUBE, Platform.TIKTOK, Platform.FACEBOOK, Platform.X, Platform.INSTAGRAM]


@dataclass(frozen=True)
class PipelineOptions:
    platforms: list[Platform] = field(default_factory=lambda: list(DEFAULT_PLATFORM_ORDER))
    limit_per_keyword: int = 10
    collect_comments: bool = True
    download_media: bool = True
    dry_run: bool = False
    continue_on_error: bool = False
    parallel_platforms: bool = False
    output_dir: Path = Path("data/pipeline-runs")
    run_id: str = field(default_factory=lambda: uuid4().hex)
    metadata: dict[str, Any] = field(default_factory=dict)


def parse_platforms(value: str) -> list[Platform]:
    raw_value = (value or "all").strip()
    if raw_value.lower() == "all":
        return list(DEFAULT_PLATFORM_ORDER)
    platforms: list[Platform] = []
    for item in raw_value.split(","):
        normalized = item.strip().upper()
        if not normalized:
            continue
        if normalized == "TWITTER":
            normalized = "X"
        try:
            platform = Platform(normalized)
        except ValueError as exc:
            raise ValueError(f"unknown platform: {item.strip()}") from exc
        platforms.append(platform)
    if not platforms:
        raise ValueError("at least one platform is required")
    return platforms


def run_pipeline(
    options: PipelineOptions,
    *,
    monitor_runner: Callable[..., dict[str, Any]] = run_keyword_monitor,
    output: Callable[[str], None] = print,
) -> dict[str, Any]:
    started_at = time.time()
    run_dir = options.output_dir / options.run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    selected = [platform.value for platform in options.platforms]
    output(f"[PIPELINE] run_id={options.run_id} platforms={','.join(selected)} limit={options.limit_per_keyword}")

    platform_results: list[dict[str, Any]] = []
    total_synced = 0
    failed_platform = None

    if options.parallel_platforms:
        output(f"[PIPELINE] mode=parallel platform_workers={len(options.platforms)}")
        platform_result_by_value: dict[str, dict[str, Any]] = {}
        with ThreadPoolExecutor(max_workers=max(1, len(options.platforms))) as executor:
            futures = {
                executor.submit(_run_platform, index, platform, options, monitor_runner, output, run_dir): platform
                for index, platform in enumerate(options.platforms, start=1)
            }
            for future in as_completed(futures):
                platform = futures[future]
                platform_summary = future.result()
                platform_result_by_value[platform.value] = platform_summary
                total_synced += int(platform_summary.get("contents_synced") or 0)
                if platform_summary["status"] == "failed" and failed_platform is None:
                    failed_platform = platform.value
        platform_results = [platform_result_by_value[platform.value] for platform in options.platforms if platform.value in platform_result_by_value]
        summary = {
            "run_id": options.run_id,
            "status": "failed" if failed_platform else "success",
            "platforms": selected,
            "contents_synced": total_synced,
            "failed_platform": failed_platform,
            "platform_results": platform_results,
            "elapsed_seconds": round(time.time() - started_at, 3),
            "output_dir": str(run_dir),
        }
        _write_json(run_dir / "summary.json", summary)
        output(f"[PIPELINE] done status={summary['status']} synced={total_synced} summary={run_dir / 'summary.json'}")
        return summary

    for index, platform in enumerate(options.platforms, start=1):
        platform_summary = _run_platform(index, platform, options, monitor_runner, output, run_dir)
        status = platform_summary["status"]
        synced = int(platform_summary.get("contents_synced") or 0)
        total_synced += synced
        platform_results.append(platform_summary)

        if status == "failed" and not options.continue_on_error:
            failed_platform = platform.value
            break
        if status == "failed" and failed_platform is None:
            failed_platform = platform.value

    summary = {
        "run_id": options.run_id,
        "status": "failed" if failed_platform else "success",
        "platforms": selected,
        "contents_synced": total_synced,
        "failed_platform": failed_platform,
        "platform_results": platform_results,
        "elapsed_seconds": round(time.time() - started_at, 3),
        "output_dir": str(run_dir),
    }
    _write_json(run_dir / "summary.json", summary)
    output(f"[PIPELINE] done status={summary['status']} synced={total_synced} summary={run_dir / 'summary.json'}")
    return summary


def _run_platform(
    index: int,
    platform: Platform,
    options: PipelineOptions,
    monitor_runner: Callable[..., dict[str, Any]],
    output: Callable[[str], None],
    run_dir: Path,
) -> dict[str, Any]:
    output(f"[PLATFORM] start {platform.value.lower()} {index}/{len(options.platforms)}")
    try:
        result = monitor_runner(
            platforms=[platform],
            limit_per_keyword=options.limit_per_keyword,
            collect_comments=options.collect_comments,
            download_media=options.download_media,
            dry_run=options.dry_run,
            metadata=dict(options.metadata),
            progress_callback=lambda event: _print_progress(event, output),
        )
    except Exception as exc:  # noqa: BLE001 - CLI pipeline must persist failures before exiting.
        result = {
            "platforms": [platform.value],
            "keywords_total": 0,
            "keywords_collected": 0,
            "contents_synced": 0,
            "items": [],
            "skipped": [{"platform": platform.value, "reason": type(exc).__name__, "message": str(exc)}],
        }
    _write_json(run_dir / f"{platform.value.lower()}.json", result)
    status = "failed" if _platform_failed(result) else "success"
    synced = int(result.get("contents_synced") or 0)
    platform_summary = {
        "platform": platform.value,
        "status": status,
        "keywords_total": int(result.get("keywords_total") or 0),
        "keywords_collected": int(result.get("keywords_collected") or 0),
        "contents_synced": synced,
        "skipped": result.get("skipped") or [],
    }
    output(
        f"[PLATFORM] done {platform.value.lower()} status={status} "
        f"keywords={platform_summary['keywords_collected']}/{platform_summary['keywords_total']} synced={synced}"
    )
    return platform_summary


def _platform_failed(result: dict[str, Any]) -> bool:
    if result.get("skipped"):
        return True
    total = int(result.get("keywords_total") or 0)
    collected = int(result.get("keywords_collected") or 0)
    return collected != total


def _print_progress(event: dict[str, Any], output: Callable[[str], None]) -> None:
    event_name = event.get("event")
    platform = str(event.get("platform") or "").lower()
    keyword_id = event.get("keyword_id")
    keyword = event.get("keyword")
    if event_name == "keywords_loaded":
        output(f"[KEYWORDS] source={event.get('keyword_source')} total={event.get('keywords_total')}")
    elif event_name == "keyword_started":
        output(f"[KEYWORD] start {platform} keyword_id={keyword_id} keyword={keyword}")
    elif event_name == "keyword_collected":
        counts = event.get("counts") or {}
        recency_filter = event.get("recency_filter") or {}
        recency_text = ""
        if recency_filter.get("enabled"):
            recency_text = (
                f" recency_filtered={recency_filter.get('filtered_old', 0)}"
                f" missing_time_kept={recency_filter.get('missing_published_at_kept', 0)}"
            )
        output(
            f"[COLLECT] {platform} keyword_id={keyword_id} "
            f"contents={counts.get('contents', 0)} comments={counts.get('comments', 0)} "
            f"assets={counts.get('assets', 0)} accounts={counts.get('accounts', 0)}{recency_text}"
        )
    elif event_name == "keyword_synced":
        output(f"[SYNC] {platform} keyword_id={keyword_id} contents_synced={event.get('contents_synced', 0)}")
    elif event_name == "keyword_skipped":
        output(f"[SKIP] {platform} keyword_id={keyword_id} keyword={keyword} reason={event.get('reason')}")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
