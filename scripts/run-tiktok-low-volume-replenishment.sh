#!/usr/bin/env bash
set -euo pipefail

# A dedicated low-volume platform lane.  TikTok has its own signed browser
# profile, so it can increase the smaller library without interfering with
# the active X/Instagram profiles.  Only one TikTok job may exist at once and
# any live relay gets absolute priority.

root_dir=/home/ubuntu/media-monitor
api_url=http://127.0.0.1:18083/collector/jobs/keywords
job_dir="$root_dir/data/collector-jobs"
log_prefix='[tiktok-low-volume-replenishment]'
offset=0
batch_size=6
cooldown_until=0

platform_busy() {
  "$root_dir/.venv/bin/python" - "$job_dir" <<'PY'
import json
import sys
from pathlib import Path
for path in Path(sys.argv[1]).glob("*.json"):
    try:
        job = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        continue
    request = job.get("request") or {}
    values = {str(request.get("platform") or "").upper()}
    values.update(str(item).upper() for item in request.get("platforms") or [])
    values.update(str(item.get("platform") or "").upper() for item in request.get("accounts") or [])
    if "TIKTOK" not in values:
        continue
    # A running job has the browser/profile.  Pending priority-account work
    # also wins; ordinary pending bulk work does not permanently starve this
    # independent low-priority keyword lane.
    if job.get("status") == "RUNNING":
        raise SystemExit(0)
    if job.get("status") == "PENDING" and job.get("jobType") == "ACCOUNT" and any(
        bool(account.get("priority")) and str(account.get("platform") or "").upper() == "TIKTOK"
        for account in request.get("accounts") or []
    ):
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
cfg=dotenv_values('/home/ubuntu/media-monitor/.env')
conn=pymysql.connect(host=cfg['KEYWORD_DB_HOST'],port=int(cfg['KEYWORD_DB_PORT']),user=cfg['KEYWORD_DB_USER'],password=cfg['KEYWORD_DB_PASSWORD'],database=cfg['KEYWORD_DB_NAME'],charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor,connect_timeout=10,read_timeout=30)
try:
    with conn.cursor() as cursor:
        cursor.execute("""SELECT id FROM monitor_keyword WHERE status='ACTIVE'
            AND platform IN ('TIKTOK','ALL')
            ORDER BY (last_collected_at IS NOT NULL), last_collected_at ASC, id ASC LIMIT %s OFFSET %s""", (limit, offset))
        print(','.join(str(row['id']) for row in cursor.fetchall()))
finally:
    conn.close()
PY
}

while true; do
  now=$(date +%s)
  if (( now < cooldown_until )); then
    echo "$log_prefix cooling_down_after_zero_output"
    sleep 60
    continue
  fi
  if platform_busy; then
    echo "$log_prefix waiting_for_tiktok_account_priority_or_running_job"
    sleep 45
    continue
  fi
  ids=$(keyword_ids || true)
  if [[ -z "$ids" ]]; then
    offset=0
    sleep 30
    continue
  fi
  body=$(jq -nc --arg ids "$ids" '($ids|split(",")|map(tonumber)) as $keyword_ids | {
    platforms:["TIKTOK"], keywordIds:$keyword_ids, limit:30,
    collectComments:true, downloadMedia:true, recencyMonths:6,
    parallelPlatforms:false, continueOnError:true, runInBackground:true,
    metadata:{collection_profile:"low-volume-tiktok-replenishment", lowPriorityQueue:true, min_free_disk_gb:2, browser_search_timeout_seconds:30, browser_comment_limit:10, max_comments:10, max_height:480}
  }')
  if response=$(curl --fail --silent --show-error --request POST --header 'Content-Type: application/json' --data "$body" "$api_url"); then
    run_id=$(jq -r '.runId // empty' <<<"$response")
    echo "$log_prefix run_id=$run_id keywords=$ids"
    while [[ -n "$run_id" ]]; do
      state=$(jq -r '.status // empty' "$job_dir/$run_id.json" 2>/dev/null || true)
      case "$state" in PENDING|RUNNING|'') sleep 20 ;; *) break ;; esac
    done
    synced=$(jq -r '.contentsSynced // 0' "$job_dir/$run_id.json" 2>/dev/null || echo 0)
    if [[ "$synced" =~ ^[0-9]+$ ]] && (( synced > 0 )); then
      offset=$((offset + $(awk -F, '{print NF}' <<<"$ids")))
    else
      cooldown_until=$(( $(date +%s) + 900 ))
      echo "$log_prefix run_id=$run_id zero_output cooldown_seconds=900"
    fi
  else
    echo "$log_prefix submission_deferred"
    sleep 45
  fi
  sleep 30
done
