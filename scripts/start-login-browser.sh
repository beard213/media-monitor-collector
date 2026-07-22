#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROFILE="${1:-$ROOT/data/storage/private/browser_profiles/account_01}"
START_URL="${2:-https://www.youtube.com/}"
DISPLAY_ID="${LOGIN_DISPLAY_ID:-99}"
VNC_PORT="${LOGIN_VNC_PORT:-5909}"
NOVNC_PORT="${LOGIN_NOVNC_PORT:-16080}"
LOG_DIR="$ROOT/data/logs/login-browser"
RUNTIME_DIR="$ROOT/data/runtime/login-browser"

mkdir -p "$PROFILE" "$LOG_DIR" "$RUNTIME_DIR"
chmod 700 "$ROOT/data/storage/private" "$PROFILE"

if pgrep -f "Xvfb :$DISPLAY_ID" >/dev/null; then
  echo "Xvfb :$DISPLAY_ID already running"
else
  setsid Xvfb ":$DISPLAY_ID" -screen 0 1366x768x24 >"$LOG_DIR/xvfb.log" 2>&1 < /dev/null &
  echo "$!" > "$RUNTIME_DIR/xvfb.pid"
fi

sleep 1

if pgrep -f "x11vnc .*:$DISPLAY_ID" >/dev/null; then
  echo "x11vnc for :$DISPLAY_ID already running"
else
  setsid x11vnc \
    -display ":$DISPLAY_ID" \
    -localhost \
    -rfbport "$VNC_PORT" \
    -forever \
    -shared \
    -nopw \
    >"$LOG_DIR/x11vnc.log" 2>&1 < /dev/null &
  echo "$!" > "$RUNTIME_DIR/x11vnc.pid"
fi

if pgrep -f "websockify .*${NOVNC_PORT} .*${VNC_PORT}" >/dev/null; then
  echo "noVNC websockify on $NOVNC_PORT already running"
else
  setsid websockify \
    --web=/usr/share/novnc \
    "127.0.0.1:$NOVNC_PORT" \
    "127.0.0.1:$VNC_PORT" \
    >"$LOG_DIR/novnc.log" 2>&1 < /dev/null &
  echo "$!" > "$RUNTIME_DIR/novnc.pid"
fi

if pgrep -f "chromium.*$PROFILE" >/dev/null; then
  echo "Chromium for $PROFILE already running"
else
  DISPLAY=":$DISPLAY_ID" setsid chromium-browser \
    --user-data-dir="$PROFILE" \
    --no-first-run \
    --disable-dev-shm-usage \
    --disable-gpu \
    --window-size=1366,768 \
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
