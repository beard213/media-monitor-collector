#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUNTIME_DIR="$ROOT/data/runtime/login-browser"

for name in chromium novnc x11vnc xvfb; do
  pid_file="$RUNTIME_DIR/$name.pid"
  if [[ -f "$pid_file" ]]; then
    pid="$(cat "$pid_file")"
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      kill "$pid" || true
    fi
    rm -f "$pid_file"
  fi
done

pkill -f "websockify .*127.0.0.1:16080" || true
pkill -f "x11vnc .*5909" || true
pkill -f "Xvfb :99" || true
