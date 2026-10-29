#!/usr/bin/env bash
set -euo pipefail

# One comment-only repair at a time.  This lane deliberately never downloads
# the video again and never shares a platform with a live, keyword, or account
# job.  It therefore improves historical coverage without stealing a browser
# profile from realtime monitoring or video expansion.

root_dir=/home/ubuntu/media-monitor
api_url=http://127.0.0.1:18083/collections/run
job_dir="$root_dir/data/collector-jobs"
state_file="$root_dir/data/runtime/comment-backfill-attempts.json"
platform_cooldown_file="$root_dir/data/runtime/comment-backfill-platform-cooldowns.json"
log_prefix='[comment-backfill]'

next_candidate() {
  "$root_dir/.venv/bin/python" - "$job_dir" "$state_file" "$platform_cooldown_file" <<'PY'
from datetime import datetime, timedelta, timezone
from dotenv import dotenv_values
import json
from pathlib import Path
import pymysql
import sys

job_dir, state_file, platform_cooldown_file = map(Path, sys.argv[1:])
now = datetime.now(timezone.utc)
try:
    attempts = json.loads(state_file.read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    attempts = {}
try:
    platform_cooldowns = json.loads(platform_cooldown_file.read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    platform_cooldowns = {}

busy = set()
for path in job_dir.glob("*.json"):
    try:
        job = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        continue
    if job.get("status") not in {"PENDING", "RUNNING"}:
        continue
    request = job.get("request") or {}
    platform = str(request.get("platform") or "").upper()
    if platform:
        busy.add("X" if platform == "TWITTER" else platform)
    for item in request.get("platforms") or []:
        value = str(item).upper()
        busy.add("X" if value == "TWITTER" else value)
    for account in request.get("accounts") or []:
        value = str(account.get("platform") or "").upper()
        if value:
            busy.add("X" if value == "TWITTER" else value)

cfg = dotenv_values("/home/ubuntu/media-monitor/.env")
conn = pymysql.connect(
    host=cfg["KEYWORD_DB_HOST"], port=int(cfg["KEYWORD_DB_PORT"]),
    user=cfg["KEYWORD_DB_USER"], password=cfg["KEYWORD_DB_PASSWORD"],
    database=cfg["KEYWORD_DB_NAME"], charset="utf8mb4",
    cursorclass=pymysql.cursors.DictCursor, connect_timeout=10, read_timeout=45,
)
try:
    with conn.cursor() as cursor:
        cursor.execute('''
            SELECT UPPER(m.platform) AS platform, m.video_id, m.source_url,
                   MIN(CASE UPPER(COALESCE(h.risk_level, ''))
                       WHEN 'HIGH' THEN 0 WHEN 'MEDIUM' THEN 1 WHEN 'LOW' THEN 2 ELSE 3 END) AS risk_rank,
                   MAX(m.collected_at) AS collected_at
            FROM media_content m
            LEFT JOIN harmful_content_ledger h ON h.video_id=m.video_id
            LEFT JOIN (SELECT DISTINCT video_id FROM content_comment) cc ON cc.video_id=m.video_id
            WHERE cc.video_id IS NULL
              AND UPPER(m.platform) IN ('YOUTUBE','FACEBOOK','INSTAGRAM','TIKTOK','X','TWITTER')
              AND COALESCE(m.comment_count, m.comment_cnt, 0) > 0
              AND m.source_url IS NOT NULL AND m.source_url <> ''
            GROUP BY UPPER(m.platform), m.video_id, m.source_url
            ORDER BY risk_rank ASC, collected_at DESC
            LIMIT 250
        ''')
        rows = cursor.fetchall()
finally:
    conn.close()

# Prefer risky candidates, but do useful work on an idle platform instead of
# waiting behind a live relay forever.  A failed/empty attempt cools down for
# 12 hours so one inaccessible page cannot spin indefinitely.
for row in rows:
    platform = "X" if row["platform"] == "TWITTER" else row["platform"]
    if platform in busy:
        continue
    try:
        until = datetime.fromisoformat(str((platform_cooldowns.get(platform) or {}).get("until") or "").replace("Z", "+00:00"))
    except ValueError:
        until = None
    if until and until.tzinfo is None:
        until = until.replace(tzinfo=timezone.utc)
    if until and until > now:
        continue
    previous = attempts.get(str(row["video_id"])) or {}
    try:
        attempted_at = datetime.fromisoformat(str(previous.get("attemptedAt") or "").replace("Z", "+00:00"))
    except ValueError:
        attempted_at = None
    if attempted_at and attempted_at.tzinfo is None:
        attempted_at = attempted_at.replace(tzinfo=timezone.utc)
    if attempted_at and now - attempted_at < timedelta(hours=12):
        continue
    print(json.dumps({"platform": platform, "video_id": row["video_id"], "source_url": row["source_url"], "risk_rank": row["risk_rank"]}, ensure_ascii=False))
    break
PY
}

record_attempt() {
  "$root_dir/.venv/bin/python" - "$state_file" "$platform_cooldown_file" "$1" "$2" "$3" "$4" <<'PY'
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sys

path, cooldown_path, platform, video_id, result, comments = sys.argv[1:]
target = Path(path)
try:
    payload = json.loads(target.read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    payload = {}
payload[str(video_id)] = {"attemptedAt": datetime.now(timezone.utc).isoformat(), "result": result, "comments": int(comments or 0)}
# The file is only a short cooldown cache, not a database of record.
if len(payload) > 5000:
    payload = dict(list(payload.items())[-3000:])
target.parent.mkdir(parents=True, exist_ok=True)
temporary = target.with_suffix('.tmp')
temporary.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
temporary.replace(target)

# A login wall/parser failure is platform-wide in practice.  Cool the
# platform for 15 minutes, then let the backfill lane use an idle platform
# that can actually return comments instead of issuing one Facebook failure
# every 30 seconds against different historical videos.
cooldown_target = Path(cooldown_path)
try:
    cooldowns = json.loads(cooldown_target.read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    cooldowns = {}
if result == "failed":
    cooldowns[platform] = {"until": (datetime.now(timezone.utc) + timedelta(minutes=15)).isoformat(), "reason": "comment_request_failed"}
else:
    cooldowns.pop(platform, None)
cooldown_target.parent.mkdir(parents=True, exist_ok=True)
cooldown_tmp = cooldown_target.with_suffix('.tmp')
cooldown_tmp.write_text(json.dumps(cooldowns, ensure_ascii=False), encoding="utf-8")
cooldown_tmp.replace(cooldown_target)
PY
}

while true; do
  row=$(next_candidate || true)
  if [[ -z "$row" ]]; then
    echo "$log_prefix waiting_for_idle_eligible_content"
    sleep 60
    continue
  fi
  platform=$(jq -r '.platform' <<<"$row")
  video_id=$(jq -r '.video_id' <<<"$row")
  source_url=$(jq -r '.source_url' <<<"$row")
  body=$(jq -nc --arg platform "$platform" --arg url "$source_url" '
    {
      platform: $platform, capability: "COMMENTS", target_type: "CONTENT",
      target_value: $url, source_url: $url, limit: 1,
      collect_comments: true, download_media: false, timeout_seconds: 300,
      metadata: {
        collection_profile: "stable-comment-backfill",
        browser_comment_limit: 20,
        browser_comment_timeout_seconds: 20
      }
    }')
  if response=$(curl --fail --silent --show-error --max-time 330 --request POST \
      --header 'Content-Type: application/json' --data "$body" "$api_url"); then
    comments=$(jq -r '.counts.comments // 0' <<<"$response")
    echo "$log_prefix platform=$platform video_id=$video_id comments=$comments"
    record_attempt "$platform" "$video_id" success "$comments"
  else
    echo "$log_prefix platform=$platform video_id=$video_id request_failed"
    record_attempt "$platform" "$video_id" failed 0
  fi
  sleep 30
done
