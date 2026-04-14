#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_FILE="${APP_FILE:-app.py}"
ADDRESS="${APP_ADDRESS:-0.0.0.0}"
PORT="${1:-${APP_PORT:-8080}}"

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

require_command python3

if (( PORT < 1024 )) && [[ "${EUID}" -ne 0 ]]; then
  echo "Re-running with sudo so the app can bind to port ${PORT}."
  exec sudo -E bash "$0" "$@"
fi

export APP_ADDRESS="${ADDRESS}"
export APP_PORT="${PORT}"
unset APP_SSL_CERT_FILE
unset APP_SSL_KEY_FILE

exec python3 "${ROOT_DIR}/${APP_FILE}"
