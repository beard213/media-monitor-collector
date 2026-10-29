#!/usr/bin/env bash
set -euo pipefail

# Expand X in bounded batches: 6 × (5 keywords × 20 results) = up to 600
# collection attempts.  A batch never starts while an X priority-account
# follow-up is waiting or running, so selected-account monitoring retains
# precedence over library expansion.

root_dir=/home/ubuntu/media-monitor
api_url=http://127.0.0.1:18083/collector/jobs/keywords
log_prefix='[x-expansion]'

batches=(
  '19,24,33,49,58'
  '98,107,116,133,143'
  '151,158,233,241,250'
  '259,268,278,287,296'
  '305,314,323,332,341'
  '1680,1771,1772,1773,1774'
)

priority_x_is_active() {
  python3 - "$root_dir/data/collector-jobs" <<'PY'
import json
import sys
from pathlib import Path

for path in Path(sys.argv[1]).glob("account-*.json"):
    try:
        job = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        continue
    if job.get("status") not in {"PENDING", "RUNNING"}:
        continue
    for account in (job.get("request") or {}).get("accounts") or []:
        if account.get("priority") and str(account.get("platform") or "").upper() in {"X", "TWITTER"}:
            raise SystemExit(0)
raise SystemExit(1)
PY
}

x_keyword_is_active() {
  python3 - "$root_dir/data/collector-jobs" <<'PY'
import json
import sys
from pathlib import Path

for path in Path(sys.argv[1]).glob("keyword-*.json"):
    try:
        job = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        continue
    if job.get("status") not in {"PENDING", "RUNNING"}:
        continue
    platforms = {str(item).upper() for item in (job.get("request") or {}).get("platforms") or []}
    if platforms & {"X", "TWITTER"}:
        raise SystemExit(0)
raise SystemExit(1)
PY
}

for index in "${!batches[@]}"; do
  ids="${batches[$index]}"
  while priority_x_is_active || x_keyword_is_active; do
    echo "$log_prefix batch=$((index + 1)) waiting_for_priority_or_existing_x_job"
    sleep 20
  done

  body=$(jq -nc --arg ids "$ids" '
    ($ids | split(",") | map(tonumber)) as $keyword_ids |
    {
      platforms: ["X"],
      keywordIds: $keyword_ids,
      limit: 20,
      collectComments: true,
      downloadMedia: true,
      recencyMonths: 6,
      parallelPlatforms: false,
      continueOnError: true,
      runInBackground: true,
      metadata: {
        collection_profile: "x-expansion-priority-safe",
        target_batch: ($keyword_ids | length * 20),
        min_free_disk_gb: 2
      }
    }')

  response=$(curl --fail --silent --show-error --request POST \
    --header 'Content-Type: application/json' \
    --data "$body" "$api_url")
  run_id=$(jq -r '.runId // empty' <<<"$response")
  if [[ -z "$run_id" ]]; then
    echo "$log_prefix batch=$((index + 1)) missing_run_id"
    exit 1
  fi
  echo "$log_prefix batch=$((index + 1)) run_id=$run_id submitted"

  job_file="$root_dir/data/collector-jobs/$run_id.json"
  while true; do
    state=$(jq -r '.status // empty' "$job_file" 2>/dev/null || true)
    case "$state" in
      PENDING|RUNNING|'') sleep 20 ;;
      *) echo "$log_prefix batch=$((index + 1)) run_id=$run_id terminal_state=$state"; break ;;
    esac
  done
done

echo "$log_prefix all_batches_submitted_and_finished"
