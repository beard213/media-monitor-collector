#!/usr/bin/env bash
set -euo pipefail

# This deliberately waits for the currently running TikTok keyword task.  It
# never creates a second browser task while the first one owns the platform.
root_dir=/home/ubuntu/media-monitor
current_job="$root_dir/data/collector-jobs/keyword-20261022102152-bbae21db.json"
request_body="$root_dir/data/continuations/tiktok-expanded-20-request.json"

while true; do
  state=$(python3 - "$current_job" <<'PY'
import json
import sys
try:
    print(json.load(open(sys.argv[1], encoding='utf-8')).get('status') or '')
except Exception:
    print('')
PY
)
  case "$state" in
    RUNNING|PENDING|QUEUED)
      sleep 30
      ;;
    COMPLETED|FAILED|CANCELLED)
      break
      ;;
    *)
      sleep 30
      ;;
  esac
done

# Let the completed task release its platform lock before the next request.
sleep 15
while ! curl --fail --silent --show-error --request POST \
  --header 'Content-Type: application/json' \
  --data-binary "@$request_body" \
  http://127.0.0.1:18083/collector/jobs/keywords; do
  sleep 30
done
