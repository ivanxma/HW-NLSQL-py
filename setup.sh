#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVICE_NAME="${SERVICE_NAME:-hw-nlsql-https}"
SERVICE_TEMPLATE="${ROOT_DIR}/systemd/${SERVICE_NAME}.service.template"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
SERVICE_USER="${SERVICE_USER:-${SUDO_USER:-$(id -un)}}"
SERVICE_GROUP="${SERVICE_GROUP:-$(id -gn "${SERVICE_USER}")}"
VENV_DIR="${APP_VENV_DIR:-${ROOT_DIR}/.venv}"
APP_ADDRESS="${APP_ADDRESS:-0.0.0.0}"
APP_PORT="${APP_PORT:-443}"
APP_SSL_CN="${APP_SSL_CN:-$(hostname -f 2>/dev/null || hostname)}"
SKIP_SYSTEMD="${SETUP_SKIP_SYSTEMD:-0}"
SYSTEMD_SUPPORTED=1
OS_ID=""
OS_VERSION_ID=""
OS_VERSION_MAJOR=""

systemd_available() {
  command -v systemctl >/dev/null 2>&1 && [[ -d /run/systemd/system ]]
}

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

detect_os() {
  if [[ ! -r /etc/os-release ]]; then
    echo "Cannot detect operating system because /etc/os-release is missing." >&2
    exit 1
  fi

  # shellcheck disable=SC1091
  . /etc/os-release

  OS_ID="${ID:-}"
  OS_VERSION_ID="${VERSION_ID:-}"
  OS_VERSION_MAJOR="${OS_VERSION_ID%%.*}"

  echo "Detected operating system: ${PRETTY_NAME:-${OS_ID} ${OS_VERSION_ID}}"
}

install_ubuntu_packages() {
  require_command apt-get
  export DEBIAN_FRONTEND=noninteractive
  apt-get update
  apt-get install -y python3 python3-venv python3-full openssl
}

install_ol8_packages() {
  require_command dnf
  dnf install -y python3 python3-pip python3-setuptools python3-wheel openssl
}

install_ol9_packages() {
  require_command dnf
  dnf install -y python3 python3-pip python3-setuptools python3-pip-wheel openssl
}

install_os_packages() {
  detect_os

  case "${OS_ID}" in
    ubuntu)
      install_ubuntu_packages
      ;;
    ol|oracle|oraclelinux)
      case "${OS_VERSION_MAJOR}" in
        8)
          install_ol8_packages
          ;;
        9)
          install_ol9_packages
          ;;
        *)
          echo "Unsupported Oracle Linux version: ${OS_VERSION_ID}. Expected Oracle Linux 8 or 9." >&2
          exit 1
          ;;
      esac
      ;;
    *)
      echo "Unsupported operating system: ${OS_ID} ${OS_VERSION_ID}. Expected Ubuntu, Oracle Linux 8, or Oracle Linux 9." >&2
      exit 1
      ;;
  esac
}

install_python_environment() {
  python3 -m venv "${VENV_DIR}"
  "${VENV_DIR}/bin/python" -m pip install --upgrade pip setuptools wheel
  "${VENV_DIR}/bin/python" -m pip install -r "${ROOT_DIR}/requirements.txt"
}

prepare_runtime_files() {
  chmod 755 "${ROOT_DIR}/start_https.sh"
  chmod 755 "${ROOT_DIR}/start_http.sh"
  install -d -m 755 -o "${SERVICE_USER}" -g "${SERVICE_GROUP}" "${ROOT_DIR}/.certs"
  chown -R "${SERVICE_USER}:${SERVICE_GROUP}" "${ROOT_DIR}/.certs"
  chown -R "${SERVICE_USER}:${SERVICE_GROUP}" "${VENV_DIR}"

  if [[ ! -f "${ROOT_DIR}/profiles.json" ]]; then
    install -m 664 -o "${SERVICE_USER}" -g "${SERVICE_GROUP}" /dev/null "${ROOT_DIR}/profiles.json"
    printf '{\n  "profiles": []\n}\n' > "${ROOT_DIR}/profiles.json"
  fi

  chown "${SERVICE_USER}:${SERVICE_GROUP}" "${ROOT_DIR}/profiles.json"
}

install_systemd_service() {
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

configure_ubuntu_firewall() {
  if ! command -v ufw >/dev/null 2>&1; then
    return
  fi

  if ufw status | grep -q "^Status: active"; then
    ufw allow "${APP_PORT}/tcp"
    echo "Opened port ${APP_PORT}/tcp in ufw."
  fi
}

configure_firewalld() {
  if ! command -v firewall-cmd >/dev/null 2>&1; then
    return
  fi

  if systemctl is-active --quiet firewalld; then
    if [[ "${APP_PORT}" == "443" ]]; then
      firewall-cmd --permanent --add-service=https
    else
      firewall-cmd --permanent --add-port="${APP_PORT}/tcp"
    fi
    firewall-cmd --reload
    echo "Opened port ${APP_PORT}/tcp in firewalld."
  fi
}

configure_selinux_port() {
  if ! command -v getenforce >/dev/null 2>&1; then
    return
  fi

  if [[ "$(getenforce)" == "Disabled" ]]; then
    return
  fi

  if [[ "${APP_PORT}" == "443" ]]; then
    echo "SELinux already permits HTTPS on port 443."
    return
  fi

  if ! command -v semanage >/dev/null 2>&1; then
    echo "SELinux is enabled but 'semanage' is not installed; port ${APP_PORT} may need manual labeling." >&2
    return
  fi

  if semanage port -l | grep -E '^http_port_t' | grep -qw "${APP_PORT}"; then
    return
  fi

  if semanage port -l | grep -qw "${APP_PORT}"; then
    semanage port -m -t http_port_t -p tcp "${APP_PORT}"
  else
    semanage port -a -t http_port_t -p tcp "${APP_PORT}"
  fi

  echo "Configured SELinux to allow HTTP/S traffic on port ${APP_PORT}."
}

configure_host_access() {
  case "${OS_ID}" in
    ubuntu)
      configure_ubuntu_firewall
      ;;
    ol|oracle|oraclelinux)
      configure_firewalld
      configure_selinux_port
      ;;
  esac
}

main() {
  run_as_root "$@"
  install_os_packages
  require_command python3
  install_python_environment
  prepare_runtime_files
  if [[ "${SKIP_SYSTEMD}" == "1" ]]; then
    echo
    echo "Setup complete in container mode."
    echo "Start the app with: /bin/bash ${ROOT_DIR}/start_https.sh"
    return
  fi

  if [[ "${SYSTEMD_SUPPORTED}" != "1" ]] || ! systemd_available; then
    echo
    echo "Setup complete."
    echo "A systemd service was not created in this environment."
    echo "Python dependencies were installed in: ${VENV_DIR}"
    echo "Start the app with: /bin/bash ${ROOT_DIR}/start_https.sh"
    return
  fi

  install_systemd_service
  configure_host_access

  echo
  echo "Setup complete."
  echo "Python dependencies were installed in: ${VENV_DIR}"
  echo "Service: ${SERVICE_NAME}.service"
  echo "Check status with: sudo systemctl status ${SERVICE_NAME}.service"
  echo "View logs with: sudo journalctl -u ${SERVICE_NAME}.service -f"
}

main "$@"
