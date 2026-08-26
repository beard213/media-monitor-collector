#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SERVICE_NAME="${SERVICE_NAME:-media-monitor-api}"
UNIT_SOURCE="$ROOT/deploy/systemd/media-monitor-api.service"
UNIT_TARGET="/etc/systemd/system/${SERVICE_NAME}.service"

if [[ ! -f "$UNIT_SOURCE" ]]; then
  echo "unit file not found: $UNIT_SOURCE" >&2
  exit 1
fi

if [[ ! -x "$ROOT/.venv/bin/uvicorn" ]]; then
  echo "uvicorn not found: $ROOT/.venv/bin/uvicorn" >&2
  exit 1
fi

sudo cp "$UNIT_SOURCE" "$UNIT_TARGET"
sudo systemctl daemon-reload
sudo systemctl enable "$SERVICE_NAME"
sudo systemctl restart "$SERVICE_NAME"
sudo systemctl status "$SERVICE_NAME" --no-pager
