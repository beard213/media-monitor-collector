#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROFILE="${1:-$ROOT/data/storage/private/browser_profiles/account_01}"
START_URL="${2:-https://www.youtube.com/}"
DISPLAY_ID="${LOGIN_DISPLAY_ID:-99}"
VNC_PORT="${LOGIN_VNC_PORT:-5909}"
NOVNC_PORT="${LOGIN_NOVNC_PORT:-16080}"
REMOTE_DEBUGGING_PORT="${LOGIN_REMOTE_DEBUGGING_PORT:-}"
LOGIN_BROWSER_PROXY="${LOGIN_BROWSER_PROXY:-}"
INSTANCE_ID="${LOGIN_INSTANCE:-display_${DISPLAY_ID}}"
LOG_DIR="$ROOT/data/logs/login-browser/$INSTANCE_ID"
RUNTIME_DIR="$ROOT/data/runtime/login-browser/$INSTANCE_ID"
XAUTHORITY_FILE="${LOGIN_XAUTHORITY:-$RUNTIME_DIR/Xauthority}"

mkdir -p "$PROFILE" "$LOG_DIR" "$RUNTIME_DIR"
chmod 700 "$ROOT/data/storage/private" "$PROFILE"
touch "$XAUTHORITY_FILE"
chmod 600 "$XAUTHORITY_FILE"
if ! xauth -f "$XAUTHORITY_FILE" list ":$DISPLAY_ID" 2>/dev/null | grep -q .; then
  COOKIE="$(mcookie 2>/dev/null || openssl rand -hex 16)"
  xauth -f "$XAUTHORITY_FILE" add ":$DISPLAY_ID" . "$COOKIE" >/dev/null 2>&1 || true
fi

is_running() {
  local pid_file="$1"
  [ -s "$pid_file" ] || return 1
  local pid
  pid="$(cat "$pid_file" 2>/dev/null || true)"
  [ -n "$pid" ] || return 1
  ps -p "$pid" >/dev/null 2>&1
}

if is_running "$RUNTIME_DIR/xvfb.pid"; then
  echo "Xvfb :$DISPLAY_ID already running"
else
  setsid Xvfb ":$DISPLAY_ID" -screen 0 1366x768x24 -auth "$XAUTHORITY_FILE" >"$LOG_DIR/xvfb.log" 2>&1 < /dev/null &
  echo "$!" > "$RUNTIME_DIR/xvfb.pid"
fi

sleep 1

if is_running "$RUNTIME_DIR/x11vnc.pid"; then
  echo "x11vnc for :$DISPLAY_ID already running"
else
  setsid x11vnc \
    -display ":$DISPLAY_ID" \
    -auth "$XAUTHORITY_FILE" \
    -localhost \
    -rfbport "$VNC_PORT" \
    -forever \
    -shared \
    -nopw \
    >"$LOG_DIR/x11vnc.log" 2>&1 < /dev/null &
  echo "$!" > "$RUNTIME_DIR/x11vnc.pid"
fi

if is_running "$RUNTIME_DIR/novnc.pid"; then
  echo "noVNC websockify on $NOVNC_PORT already running"
else
  setsid websockify \
    --web=/usr/share/novnc \
    "127.0.0.1:$NOVNC_PORT" \
    "127.0.0.1:$VNC_PORT" \
    >"$LOG_DIR/novnc.log" 2>&1 < /dev/null &
  echo "$!" > "$RUNTIME_DIR/novnc.pid"
fi

if is_running "$RUNTIME_DIR/chromium.pid"; then
  echo "Chromium for $PROFILE already running"
else
  CHROMIUM_ARGS=(
    --user-data-dir="$PROFILE"
    --no-first-run
    --disable-dev-shm-usage
    --disable-gpu
    --window-size=1366,768
  )
  # Keep interactive authentication on the same egress as yt-dlp and the
  # HLS relay.  A Profile authenticated directly but collected through a
  # proxy is routinely challenged by YouTube as a different session.
  if [ -n "$LOGIN_BROWSER_PROXY" ]; then
    CHROMIUM_ARGS+=(--proxy-server="$LOGIN_BROWSER_PROXY")
  fi
  if [ -n "$REMOTE_DEBUGGING_PORT" ]; then
    CHROMIUM_ARGS+=(
      --remote-debugging-address=127.0.0.1
      --remote-debugging-port="$REMOTE_DEBUGGING_PORT"
    )
  fi
  DISPLAY=":$DISPLAY_ID" XAUTHORITY="$XAUTHORITY_FILE" setsid chromium-browser \
    "${CHROMIUM_ARGS[@]}" \
    "$START_URL" \
    >"$LOG_DIR/chromium.log" 2>&1 < /dev/null &
  echo "$!" > "$RUNTIME_DIR/chromium.pid"
fi

cat <<EOF
Login browser is running.

Profile:
$PROFILE

Use an SSH tunnel from your computer:
ssh -L $NOVNC_PORT:127.0.0.1:$NOVNC_PORT ubuntu@SERVER_IP

Then open:
http://127.0.0.1:$NOVNC_PORT/vnc.html

EOF
