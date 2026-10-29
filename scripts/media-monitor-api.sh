#!/usr/bin/env bash
set -euo pipefail

SERVICE_NAME="${SERVICE_NAME:-media-monitor-api}"
PORT="${MEDIA_MONITOR_API_PORT:-18083}"

usage() {
  cat <<USAGE
Usage: $0 <install|start|stop|restart|status|logs|health|enable|disable>

Environment:
  SERVICE_NAME            systemd service name, default: media-monitor-api
  MEDIA_MONITOR_API_PORT  health check port, default: 18083
USAGE
}

cmd="${1:-}"
case "$cmd" in
  install)
    "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/install-api-service.sh"
    ;;
  start)
    sudo systemctl start "$SERVICE_NAME"
    ;;
  stop)
    sudo systemctl stop "$SERVICE_NAME"
    ;;
  restart)
    sudo systemctl restart "$SERVICE_NAME"
    ;;
  status)
    sudo systemctl status "$SERVICE_NAME" --no-pager
    ;;
  logs)
    sudo journalctl -u "$SERVICE_NAME" -f
    ;;
  health)
    curl -sS "http://127.0.0.1:${PORT}/health"
    echo
    ;;
  enable)
    sudo systemctl enable "$SERVICE_NAME"
    ;;
  disable)
    sudo systemctl disable "$SERVICE_NAME"
    ;;
  *)
    usage
    exit 2
    ;;
esac
