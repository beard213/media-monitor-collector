#!/usr/bin/env bash
set -euo pipefail

# YouTube is a capacity fallback, not a peer of the browser-backed low-volume
# lanes.  Instagram, X and TikTok always get first chance to replenish their
# own libraries.  When none of those lanes has a live, productive keyword job,
# this runner fills the otherwise idle YouTube slot with small bounded batches.

root_dir=/home/ubuntu/media-monitor
api_url=http://127.0.0.1:18083/collector/jobs/keywords
job_dir="$root_dir/data/collector-jobs"
log_prefix='[youtube-keyword-fallback]'
offset=0
batch_size=6

low_platforms_need_time() {
  "$root_dir/.venv/bin/python" - "$job_dir" <<'PY'
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

low_platforms = {"INSTAGRAM", "X", "TWITTER", "TIKTOK"}
now = datetime.now(timezone.utc)
for path in Path(sys.argv[1]).glob("keyword-*.json"):
    try:
        job = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        continue
    if job.get("status") not in {"PENDING", "RUNNING"}:
        continue
    platforms = {str(value).upper() for value in (job.get("request") or {}).get("platforms") or []}
    if not (platforms & low_platforms):
        continue
    # A low-platform job that already produced content remains preferred.
    if int(job.get("contentsSynced") or 0) > 0 or int(job.get("mediaSynced") or 0) > 0:
        raise SystemExit(0)
    # Give a fresh zero-output job a bounded chance.  A stale job with no
    # output must not keep the fallback slot idle forever.
    updated = str(job.get("updatedAt") or job.get("startedAt") or "")
    try:
        age = (now - datetime.fromisoformat(updated.replace("Z", "+00:00"))).total_seconds()
    except ValueError:
        age = 0
    if age < 20 * 60:
        raise SystemExit(0)
raise SystemExit(1)
PY
}

youtube_busy() {
  "$root_dir/.venv/bin/python" - "$job_dir" <<'PY'
import json
import sys
from pathlib import Path

for path in Path(sys.argv[1]).glob("*.json"):
    try:
        job = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        continue
    if job.get("status") != "RUNNING":
        continue
    request = job.get("request") or {}
    values = {str(request.get("platform") or "").upper()}
    values.update(str(value).upper() for value in request.get("platforms") or [])
    values.update(str(account.get("platform") or "").upper() for account in request.get("accounts") or [])
    if "YOUTUBE" in values:
        raise SystemExit(0)
raise SystemExit(1)
PY
}

keyword_ids() {
  "$root_dir/.venv/bin/python" - "$offset" "$batch_size" <<'PY'
from dotenv import dotenv_values
import pymysql
import sys

offset, limit = map(int, sys.argv[1:])
cfg = dotenv_values('/home/ubuntu/media-monitor/.env')
conn = pymysql.connect(
    host=cfg['KEYWORD_DB_HOST'], port=int(cfg['KEYWORD_DB_PORT']),
    user=cfg['KEYWORD_DB_USER'], password=cfg['KEYWORD_DB_PASSWORD'],
    database=cfg['KEYWORD_DB_NAME'], charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor, connect_timeout=10, read_timeout=30,
)
try:
    with conn.cursor() as cursor:
        cursor.execute("""SELECT id FROM monitor_keyword
            WHERE status='ACTIVE' AND platform IN ('YOUTUBE', 'ALL')
            ORDER BY (last_collected_at IS NOT NULL), last_collected_at ASC, id ASC
            LIMIT %s OFFSET %s""", (limit, offset))
        print(','.join(str(row['id']) for row in cursor.fetchall()))
finally:
    conn.close()
PY
}

while true; do
  if low_platforms_need_time; then
    echo "$log_prefix waiting_for_instagram_x_tiktok_output"
    sleep 60
    continue
  fi
  if youtube_busy; then
    echo "$log_prefix waiting_for_youtube_account_or_keyword"
    sleep 45
    continue
  fi
  ids=$(keyword_ids || true)
  if [[ -z "$ids" ]]; then
    offset=0
    sleep 60
    continue
  fi
  body=$(jq -nc --arg ids "$ids" '($ids|split(",")|map(tonumber)) as $keyword_ids | {
    platforms:["YOUTUBE"], keywordIds:$keyword_ids, limit:30,
    collectComments:true, downloadMedia:true, recencyMonths:6,
    parallelPlatforms:false, continueOnError:true, runInBackground:true,
    metadata:{collection_profile:"youtube-fallback-after-low-platform-no-output", lowPriorityQueue:true, min_free_disk_gb:2, browser_comment_limit:10, max_comments:10, max_height:480}
  }')
  if ! response=$(curl --fail --silent --show-error --request POST --header 'Content-Type: application/json' --data "$body" "$api_url"); then
    echo "$log_prefix submission_deferred"
    sleep 60
    continue
  fi
  run_id=$(jq -r '.runId // empty' <<<"$response")
  [[ -n "$run_id" ]] || { echo "$log_prefix submission_missing_run_id"; sleep 60; continue; }
  echo "$log_prefix run_id=$run_id keywords=$ids"
  while true; do
    state=$(jq -r '.status // empty' "$job_dir/$run_id.json" 2>/dev/null || true)
    case "$state" in PENDING|RUNNING|'') sleep 20 ;; *) break ;; esac
  done
  echo "$log_prefix run_id=$run_id terminal_state=$state"
  offset=$((offset + $(awk -F, '{print NF}' <<<"$ids")))
  sleep 30
done
