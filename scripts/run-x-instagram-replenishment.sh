#!/usr/bin/env bash
set -euo pipefail

# Continuous, low-pressure video replenishment for X and Instagram.
# One bounded six-keyword (up to 180-video) batch runs at a time on this 8 GiB host.  The runner
# yields before every batch to any priority-account follow-up for that
# platform, so monitoring updates remain ahead of volume expansion.

root_dir=/home/ubuntu/media-monitor
api_url=http://127.0.0.1:18083/collector/jobs/keywords
job_dir="$root_dir/data/collector-jobs"
log_prefix='[x-instagram-replenishment]'
batch_size=6
declare -A offsets=( [X]=0 [INSTAGRAM]=0 )
declare -A cooldown_until=( [X]=0 [INSTAGRAM]=0 )

priority_is_active() {
  local platform="$1"
  python3 - "$job_dir" "$platform" <<'PY'
import json
import sys
from pathlib import Path

job_dir, platform = sys.argv[1:]
aliases = {"X", "TWITTER"} if platform == "X" else {platform}
for path in Path(job_dir).glob("account-*.json"):
    try:
        job = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        continue
    if job.get("status") not in {"PENDING", "RUNNING"}:
        continue
    for account in (job.get("request") or {}).get("accounts") or []:
        if account.get("priority") and str(account.get("platform") or "").upper() in aliases:
            raise SystemExit(0)
raise SystemExit(1)
PY
}

platform_keyword_job_is_active() {
  local platform="$1"
  python3 - "$job_dir" "$platform" <<'PY'
import json
import sys
from pathlib import Path

job_dir, platform = sys.argv[1:]
aliases = {"X", "TWITTER"} if platform == "X" else {platform}
for path in Path(job_dir).glob("keyword-*.json"):
    try:
        job = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        continue
    if job.get("status") not in {"PENDING", "RUNNING"}:
        continue
    platforms = {str(item).upper() for item in (job.get("request") or {}).get("platforms") or []}
    if platforms & aliases:
        raise SystemExit(0)
raise SystemExit(1)
PY
}

platform_account_job_is_running() {
  local platform="$1"
  python3 - "$job_dir" "$platform" <<'PY'
import json
import sys
from pathlib import Path

job_dir, platform = sys.argv[1:]
aliases = {"X", "TWITTER"} if platform == "X" else {platform}
for path in Path(job_dir).glob("account-*.json"):
    try:
        job = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        continue
    # Pending normal refreshes must not starve replenishment forever.  A
    # running account job owns the browser/profile and is the real conflict.
    if job.get("status") != "RUNNING":
        continue
    for account in (job.get("request") or {}).get("accounts") or []:
        if str(account.get("platform") or "").upper() in aliases:
            raise SystemExit(0)
raise SystemExit(1)
PY
}

keyword_ids() {
  local platform="$1"
  local offset="$2"
  "$root_dir/.venv/bin/python" - "$platform" "$offset" "$batch_size" <<'PY'
from dotenv import dotenv_values
import pymysql
import sys

platform, offset, limit = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
values = ("X", "TWITTER", "ALL") if platform == "X" else ("INSTAGRAM", "ALL")
cfg = dotenv_values("/home/ubuntu/media-monitor/.env")
marks = ",".join(["%s"] * len(values))
base_where = "status='ACTIVE' " + f"AND platform IN ({marks}) "
# Instagram has a dedicated, verified-volume topic lane.  General keyword
# inventory contains thousands of account names and niche terms that produce
# no Reels and repeatedly trigger empty tag pages.  Prefer this bounded lane;
# fall back only when it has not been configured.
if platform == "INSTAGRAM":
    sql = (
        "SELECT id FROM monitor_keyword WHERE " + base_where +
        "AND keyword_source='INSTAGRAM_VOLUME_SEED' ORDER BY id ASC LIMIT %s OFFSET %s"
    )
else:
    sql = "SELECT id FROM monitor_keyword WHERE " + base_where + "ORDER BY id ASC LIMIT %s OFFSET %s"
conn = pymysql.connect(
    host=cfg["KEYWORD_DB_HOST"], port=int(cfg["KEYWORD_DB_PORT"]),
    user=cfg["KEYWORD_DB_USER"], password=cfg["KEYWORD_DB_PASSWORD"],
    database=cfg["KEYWORD_DB_NAME"], charset="utf8mb4",
    cursorclass=pymysql.cursors.DictCursor, connect_timeout=10, read_timeout=30,
)
try:
    with conn.cursor() as cursor:
        if platform == "INSTAGRAM":
            cursor.execute(
                "SELECT COUNT(*) AS n FROM monitor_keyword WHERE " + base_where +
                "AND keyword_source='INSTAGRAM_VOLUME_SEED'", values,
            )
            has_volume_pool = bool((cursor.fetchone() or {}).get("n"))
        else:
            has_volume_pool = False
        cursor.execute(sql, (*values, limit, offset))
        rows = cursor.fetchall()
        # An exhausted configured volume pool must return empty so the shell
        # resets its offset and cycles the pool.  Falling through to the
        # generic inventory here silently reintroduced thousands of known
        # low-yield terms after the first three batches.
        if platform == "INSTAGRAM" and not rows and not has_volume_pool:
            fallback_sql = "SELECT id FROM monitor_keyword WHERE " + base_where + "ORDER BY id ASC LIMIT %s OFFSET %s"
            cursor.execute(fallback_sql, (*values, limit, offset))
            rows = cursor.fetchall()
        print(",".join(str(row["id"]) for row in rows))
finally:
    conn.close()
PY
}

disk_is_ready() {
  local available_gb
  available_gb=$(df -BG "$root_dir/data" | awk 'NR==2 {gsub("G", "", $4); print $4}')
  [[ "${available_gb:-0}" =~ ^[0-9]+$ ]] && (( available_gb >= 2 ))
}

run_batch() {
  local platform="$1"
  local ids="$2"
  local body response run_id job_file state synced
  body=$(jq -nc --arg platform "$platform" --arg ids "$ids" '
    ($ids | split(",") | map(tonumber)) as $keyword_ids |
    {
      platforms: [$platform], keywordIds: $keyword_ids, limit: 30,
      collectComments: true, downloadMedia: true, recencyMonths: 6,
      parallelPlatforms: false, continueOnError: true, runInBackground: true,
      metadata: {
        collection_profile: "continuous-x-instagram-video-replenishment",
        lowPriorityQueue: true,
        min_free_disk_gb: 2,
        # Comments are required for every newly collected replenishment
        # video. Ten is enough to populate the comment/author pipeline
        # without turning one keyword batch into hundreds of open pages.
        browser_comment_limit: 10,
        max_comments: 10,
        # A blocked tag page must not consume a full minute before the next
        # candidate is tried.  480p remains a real playable source file while
        # allowing the bounded Instagram lane to finish more useful terms.
        browser_search_timeout_seconds: 30,
        max_height: 480
      }
    }')
  if ! response=$(curl --fail --silent --show-error --request POST \
    --header 'Content-Type: application/json' --data "$body" "$api_url"); then
    echo "$log_prefix platform=$platform submission_deferred"
    return 1
  fi
  run_id=$(jq -r '.runId // empty' <<<"$response")
  [[ -n "$run_id" ]] || { echo "$log_prefix platform=$platform submission_missing_run_id"; return 1; }
  echo "$log_prefix platform=$platform run_id=$run_id submitted keywords=$ids"
  job_file="$job_dir/$run_id.json"
  while true; do
    state=$(jq -r '.status // empty' "$job_file" 2>/dev/null || true)
    case "$state" in
      PENDING|RUNNING|'') sleep 20 ;;
      *)
        echo "$log_prefix platform=$platform run_id=$run_id terminal_state=$state"
        # The pipeline reports FAILED when one keyword in a mixed batch is
        # empty, even if the preceding keywords already synced usable media.
        # Treat that as productive work; otherwise one empty term imposes a
        # 15-minute cooldown after every successful X batch.
        synced=$(jq -r '.contentsSynced // 0' "$job_file" 2>/dev/null || echo 0)
        if [[ "$synced" =~ ^[0-9]+$ ]] && (( synced > 0 )); then
          echo "$log_prefix platform=$platform run_id=$run_id productive_sync=$synced"
          return 0
        fi
        # A zero-result terminal state is a rendered/login/search failure in
        # practice, so retain the platform cooldown for it.
        case "$state" in
          SUCCESS|PARTIAL_SUCCESS)
            echo "$log_prefix platform=$platform run_id=$run_id zero_output"
            return 2
            ;;
          *) return 2 ;;
        esac
        ;;
    esac
  done
}

while true; do
  # X has a verified authenticated search session.  Give it the first turn in
  # each campaign pass so an Instagram login-wall retry cannot consume the
  # only active replenishment window; Instagram still follows immediately
  # whenever its session is usable.
  for platform in X INSTAGRAM; do
    # Never let a busy X priority lane block an idle Instagram lane (or vice
    # versa).  A platform that is busy is skipped for this pass and retried
    # after the other platform has had its chance.  This preserves one job per
    # platform while keeping both video sources advancing.
    now=$(date +%s)
    if (( now < ${cooldown_until[$platform]} )); then
      echo "$log_prefix platform=$platform cooling_down_after_failure"
      # Do not spin through the outer loop while a platform is cooling down:
      # it otherwise creates a tight log/CPU loop and starves the other lane.
      sleep 60
      continue
    fi
    if priority_is_active "$platform" || platform_account_job_is_running "$platform" || platform_keyword_job_is_active "$platform" || ! disk_is_ready; then
      echo "$log_prefix platform=$platform waiting_for_account_priority_or_capacity"
      continue
    fi
    ids=$(keyword_ids "$platform" "${offsets[$platform]}") || { sleep 60; continue; }
    if [[ -z "$ids" ]]; then
      offsets[$platform]=0
      ids=$(keyword_ids "$platform" 0) || { sleep 60; continue; }
    fi
    [[ -n "$ids" ]] || { echo "$log_prefix platform=$platform no_active_keywords"; sleep 300; continue; }
    count=$(awk -F, '{print NF}' <<<"$ids")
    if run_batch "$platform" "$ids"; then
      offsets[$platform]=$(( ${offsets[$platform]} + count ))
    else
      result_code=$?
      if (( result_code == 2 )); then
        cooldown_until[$platform]=$(( $(date +%s) + 900 ))
        echo "$log_prefix platform=$platform cooldown_seconds=900"
      fi
      # A 409 means this platform gained a priority or recovery job between
      # the preflight check and submission.  Give the other platform a turn
      # immediately; retry this one on the next pass.
      continue
    fi
    # A small transition delay avoids a sustained request burst when a batch
    # is empty or completes quickly due to deduplication.
    sleep 30
  done
done
