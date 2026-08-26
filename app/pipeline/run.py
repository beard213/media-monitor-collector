from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from app.pipeline.runner import PipelineOptions, parse_platforms, run_pipeline


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    try:
        args = parser.parse_args(argv)
        metadata = json.loads(args.metadata_json) if args.metadata_json else {}
        if not isinstance(metadata, dict):
            parser.error("--metadata-json must decode to a JSON object")
        option_values = {
            "platforms": parse_platforms(args.platform),
            "limit_per_keyword": args.limit,
            "collect_comments": args.collect_comments,
            "download_media": args.download_media,
            "dry_run": args.dry_run,
            "continue_on_error": args.continue_on_error,
            "parallel_platforms": args.parallel_platforms,
            "output_dir": Path(args.output_dir),
            "metadata": metadata,
        }
        if args.run_id:
            option_values["run_id"] = args.run_id
        options = PipelineOptions(**option_values)
    except SystemExit as exc:
        return int(exc.code or 0)
    except (json.JSONDecodeError, ValueError) as exc:
        print(f"{parser.prog}: error: {exc}", file=sys.stderr)
        return 2

    result = run_pipeline(options)
    return 0 if result.get("status") == "success" else 1


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the media monitor keyword collection pipeline.")
    parser.add_argument("--platform", default="all", help="all or comma-separated platforms: youtube,tiktok,facebook,x,instagram")
    parser.add_argument("--limit", type=_limit_value, default=10, help="maximum items per keyword, 1-10")
    parser.add_argument("--collect-comments", dest="collect_comments", action="store_true", default=True)
    parser.add_argument("--no-comments", dest="collect_comments", action="store_false")
    parser.add_argument("--download-media", dest="download_media", action="store_true", default=True)
    parser.add_argument("--no-download-media", dest="download_media", action="store_false")
    parser.add_argument("--dry-run", action="store_true", help="collect locally without downstream sync")
    parser.add_argument("--continue-on-error", action="store_true", help="continue later platforms after a failure")
    parser.add_argument("--parallel-platforms", action="store_true", help="collect selected platforms concurrently")
    parser.add_argument("--output-dir", default="data/pipeline-runs", help="directory for run result JSON files")
    parser.add_argument("--run-id", default=None, help="optional fixed run id")
    parser.add_argument("--metadata-json", default="", help="JSON object passed to collection metadata")
    return parser


def _limit_value(value: str) -> int:
    parsed = int(value)
    if parsed < 1 or parsed > 10:
        raise argparse.ArgumentTypeError("limit must be between 1 and 10")
    return parsed


if __name__ == "__main__":
    raise SystemExit(main())
