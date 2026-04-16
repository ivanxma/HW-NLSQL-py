#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVICE_NAME="${SERVICE_NAME:-hw-nlsql-https}"
SERVICE_TEMPLATE="${ROOT_DIR}/systemd/${SERVICE_NAME}.service.template"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
SERVICE_USER="${SERVICE_USER:-${SUDO_USER:-$(id -un)}}"
SERVICE_GROUP="${SERVICE_GROUP:-$(id -gn "${SERVICE_USER}")}"
APP_ADDRESS="${APP_ADDRESS:-0.0.0.0}"
APP_PORT="${APP_PORT:-443}"
APP_SSL_CN="${APP_SSL_CN:-$(hostname -f 2>/dev/null || hostname)}"

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

run_as_root() {
  if [[ "${EUID}" -ne 0 ]]; then
    exec sudo -E bash "$0" "$@"
  fi
}

install_os_packages() {
  if command -v apt-get >/dev/null 2>&1; then
    export DEBIAN_FRONTEND=noninteractive
    apt-get update
    apt-get install -y python3 python3-pip python3-venv openssl
    return
  fi

  if command -v dnf >/dev/null 2>&1; then
    dnf install -y python3 python3-pip openssl
    return
  fi

  echo "Unsupported package manager. Install python3, python3-pip, and openssl manually." >&2
  exit 1
}

install_python_requirements() {
  if python3 -m pip install -r "${ROOT_DIR}/requirements.txt"; then
    return
  fi

  echo "Retrying pip install with --break-system-packages."
  python3 -m pip install --break-system-packages -r "${ROOT_DIR}/requirements.txt"
}

prepare_runtime_files() {
  install -d -m 755 -o "${SERVICE_USER}" -g "${SERVICE_GROUP}" "${ROOT_DIR}/.certs"
  chown -R "${SERVICE_USER}:${SERVICE_GROUP}" "${ROOT_DIR}/.certs"

  if [[ ! -f "${ROOT_DIR}/profiles.json" ]]; then
    install -m 664 -o "${SERVICE_USER}" -g "${SERVICE_GROUP}" /dev/null "${ROOT_DIR}/profiles.json"
    printf '{\n  "profiles": []\n}\n' > "${ROOT_DIR}/profiles.json"
  fi

  chown "${SERVICE_USER}:${SERVICE_GROUP}" "${ROOT_DIR}/profiles.json"
}

install_systemd_service() {
  require_command systemctl

  if [[ ! -f "${SERVICE_TEMPLATE}" ]]; then
    echo "Missing service template: ${SERVICE_TEMPLATE}" >&2
    exit 1
  fi

  sed \
    -e "s|__ROOT_DIR__|${ROOT_DIR}|g" \
    -e "s|__SERVICE_USER__|${SERVICE_USER}|g" \
    -e "s|__SERVICE_GROUP__|${SERVICE_GROUP}|g" \
    -e "s|__APP_ADDRESS__|${APP_ADDRESS}|g" \
    -e "s|__APP_PORT__|${APP_PORT}|g" \
    -e "s|__APP_SSL_CN__|${APP_SSL_CN}|g" \
    "${SERVICE_TEMPLATE}" > "${SERVICE_FILE}"

  chmod 644 "${SERVICE_FILE}"
  systemctl daemon-reload
  systemctl enable --now "${SERVICE_NAME}.service"
}

main() {
  run_as_root "$@"
  install_os_packages
  require_command python3
  install_python_requirements
  prepare_runtime_files
  install_systemd_service

  echo
  echo "Setup complete."
  echo "Service: ${SERVICE_NAME}.service"
  echo "Check status with: sudo systemctl status ${SERVICE_NAME}.service"
  echo "View logs with: sudo journalctl -u ${SERVICE_NAME}.service -f"
}

main "$@"
