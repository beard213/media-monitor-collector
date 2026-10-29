#!/usr/bin/env bash
set -euo pipefail

# Repair comments only for content already assessed as harmful.  Risk level
# determines ordering, not eligibility: HIGH is repaired before MEDIUM and
# LOW, while ordinary content is never selected.
# Candidates must have an upstream comment count but no stored comment row.
# The current backend accepts comments through the content upsert contract, so
# each selected item is refreshed one at a time with its existing source URL.

root_dir=/home/ubuntu/media-monitor
job_dir="$root_dir/data/collector-jobs"
api_url=http://127.0.0.1:18083/collections/run
log_prefix='[harmful-comment-backfill]'

priority_running() {
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
    if job.get("status") != "RUNNING":
        continue
    for account in (job.get("request") or {}).get("accounts") or []:
        if account.get("priority") and str(account.get("platform") or "").upper() in aliases:
            raise SystemExit(0)
raise SystemExit(1)
PY
}

candidate_rows() {
  "$root_dir/.venv/bin/python" - <<'PY'
from dotenv import dotenv_values
import json
import pymysql

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
            SELECT UPPER(h.platform) AS platform, m.video_id, m.source_url,
                   MIN(CASE UPPER(h.risk_level) WHEN 'HIGH' THEN 0 WHEN 'MEDIUM' THEN 1 ELSE 2 END) AS risk_rank,
                   MAX(m.collected_at) AS last_collected_at
            FROM harmful_content_ledger h
            JOIN media_content m ON m.video_id=h.video_id
            LEFT JOIN (SELECT DISTINCT video_id FROM content_comment) cc ON cc.video_id=m.video_id
            WHERE UPPER(h.risk_level) IN ('HIGH', 'MEDIUM', 'LOW')
              AND cc.video_id IS NULL
              AND COALESCE(m.comment_count, m.comment_cnt, 0) > 0
              AND m.source_url IS NOT NULL AND m.source_url <> ''
            GROUP BY UPPER(h.platform), m.video_id, m.source_url
            ORDER BY risk_rank ASC, last_collected_at DESC
            LIMIT 1
        ''')
        for row in cursor.fetchall():
            print(json.dumps({
                "platform": row["platform"],
                "video_id": row["video_id"],
                "source_url": row["source_url"],
            }, ensure_ascii=False))
finally:
    conn.close()
PY
}

while IFS= read -r row; do
  [[ -n "$row" ]] || continue
  platform=$(jq -r '.platform' <<<"$row")
  video_id=$(jq -r '.video_id' <<<"$row")
  source_url=$(jq -r '.source_url' <<<"$row")
  while priority_running "$platform"; do
    echo "$log_prefix platform=$platform video_id=$video_id waiting_for_priority"
    sleep 20
  done
  body=$(jq -nc --arg platform "$platform" --arg url "$source_url" '
    {
      platform: $platform,
      capability: "COMMENTS",
      target_type: "CONTENT",
      target_value: $url,
      source_url: $url,
      limit: 1,
      collect_comments: true,
      download_media: true,
      timeout_seconds: 900,
      metadata: {
        collection_profile: "harmful-comment-backfill",
        browser_comment_limit: 10,
        browser_comment_timeout_seconds: 20
      }
    }')
  if response=$(curl --fail --silent --show-error --max-time 930 \
      --request POST --header 'Content-Type: application/json' --data "$body" "$api_url"); then
    comments=$(jq -r '.counts.comments // 0' <<<"$response")
    synced=$(jq -r '.backend_sync.metadataSynced // 0' <<<"$response")
    echo "$log_prefix platform=$platform video_id=$video_id comments=$comments synced=$synced"
  else
    echo "$log_prefix platform=$platform video_id=$video_id request_failed"
  fi
  # Pace browser/profile use and give priority-account work a chance to start.
  sleep 30
done < <(candidate_rows)

echo "$log_prefix candidate_pass_finished"
