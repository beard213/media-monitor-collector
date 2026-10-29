#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
START="$ROOT/scripts/start-login-browser.sh"

# Persistent Chromium sessions are useful for preserving login cookies, but
# keeping every platform resident consumes most of an 8 GB collector even
# when no collection can use it.  A profile remains on disk after Chromium is
# stopped, so keep a resident browser only while a collector is actually
# running.  PENDING tasks can sit behind priority work for hours; treating
# them as active was the source of accumulated idle renderer processes.
profile_has_active_job() {
  local platform="$1"
  python3 - "$ROOT/data/collector-jobs" "$platform" <<'PY'
import json
import sys
from pathlib import Path

jobs_dir = Path(sys.argv[1])
platform = sys.argv[2]
active = {"RUNNING"}
for path in jobs_dir.glob("*.json"):
    try:
        job = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        continue
    if job.get("status") not in active:
        continue
    request = job.get("request") or {}
    platforms = request.get("platforms") or job.get("platforms") or []
    accounts = request.get("accounts") or []
    if platform in platforms or any(account.get("platform") == platform for account in accounts if isinstance(account, dict)):
        raise SystemExit(0)
raise SystemExit(1)
PY
}

ensure_browser() {
  local instance="$1" platform="$2" display="$3" vnc="$4" novnc="$5" port="$6" profile="$7" url="$8" proxy="${9:-}"
  local pid_file="$ROOT/data/runtime/login-browser/$instance/chromium.pid"
  local pid=""
  if [[ -s "$pid_file" ]]; then
    pid="$(cat "$pid_file" 2>/dev/null || true)"
  fi
  # A browser can keep its CDP port alive while renderer children accumulate
  # after repeated page loads. A single runaway renderer is also a practical
  # leak signal even when the renderer count remains low.  Do not use the sum
  # of RSS here: Chromium shares many pages between processes, so that number
  # overstates actual RAM. Recycle only clearly bloated profiles; normal
  # sessions stay untouched and retain their on-disk login cookies.
  local renderer_count renderer_peak_kib page_count
  renderer_count="$(ps -eo args= | awk -v profile="$profile" 'index($0, profile) && /--type=renderer/{n++} END{print n+0}')"
  renderer_peak_kib="$(ps -eo rss=,args= | awk -v profile="$profile" 'index($0, profile) && /--type=renderer/ {if ($1 > max) max=$1} END{print max+0}')"
  # No task needs this profile right now.  Stop only its processes; its profile
  # directory (cookies and login state) is deliberately left intact.
  # A platform can be deliberately pre-warmed for a bounded replenishment
  # campaign.  The marker is operational state, not a default: without it we
  # retain the normal policy of stopping idle browsers on this 8 GB host.
  # It prevents the gap between keyword jobs from destroying a valid login
  # session, which otherwise makes every new task start as anonymous.
  if [[ ! -f "$ROOT/data/runtime/login-browser/$instance/keepalive" ]] && ! profile_has_active_job "$platform"; then
    pkill -TERM -f -- "--user-data-dir=$profile" 2>/dev/null || true
    sleep 1
    pkill -KILL -f -- "--user-data-dir=$profile" 2>/dev/null || true
    rm -f "$pid_file"
    return 0
  fi
  # Active collection legitimately uses a stable login page plus disposable
  # search/detail tabs.  Eight pages leaves headroom for that work without
  # recycling a browser in the middle of a collection.
  page_count="$(curl -fsS --max-time 2 "http://127.0.0.1:$port/json/list" 2>/dev/null | jq '[.[] | select(.type == "page")] | length' 2>/dev/null || echo 999)"
  if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null && curl -fsS --max-time 2 "http://127.0.0.1:$port/json/version" >/dev/null 2>&1 && (( renderer_count < 25 )) && (( renderer_peak_kib < 786432 )) && (( page_count <= 8 )); then
    return 0
  fi
  # Kill every process tied to this explicit profile, including orphaned
  # renderer children whose parent Chromium already disappeared.
  pkill -TERM -f -- "--user-data-dir=$profile" 2>/dev/null || true
  sleep 2
  pkill -KILL -f -- "--user-data-dir=$profile" 2>/dev/null || true
  if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
    kill "$pid" 2>/dev/null || true
  fi
  rm -f "$pid_file"
  LOGIN_INSTANCE="$instance" \
  LOGIN_DISPLAY_ID="$display" \
  LOGIN_VNC_PORT="$vnc" \
  LOGIN_NOVNC_PORT="$novnc" \
  LOGIN_REMOTE_DEBUGGING_PORT="$port" \
  LOGIN_BROWSER_PROXY="$proxy" \
    "$START" "$profile" "$url" >/dev/null 2>&1 || true
}

trap 'exit 0' INT TERM
while :; do
  ensure_browser facebook_cdp_9226 FACEBOOK 112 5914 16084 9226 \
    "$ROOT/data/storage/private/browser_profiles/facebook/facebook_01" \
    "https://www.facebook.com/"
  ensure_browser tiktok_cdp_19223 TIKTOK 113 5915 16085 19223 \
    "$ROOT/data/storage/private/browser_profiles/tiktok/tiktok_01" \
    "https://www.tiktok.com/" "socks5://127.0.0.1:17891"
  ensure_browser instagram_cdp_19224 INSTAGRAM 114 5916 16086 19224 \
    "$ROOT/data/storage/private/browser_profiles/instagram/instagram_login_02" \
    "https://www.instagram.com/"
  ensure_browser x_cdp_19225 X 115 5917 16087 19225 \
    "$ROOT/data/storage/private/browser_profiles/account_01" \
    "https://x.com/"
  sleep 20
done
