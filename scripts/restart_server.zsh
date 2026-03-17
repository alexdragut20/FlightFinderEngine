#!/usr/bin/env zsh
set -euo pipefail

# FlightFinder Engine restart helper:
# - Kills any process listening on the configured port (default: 8000)
# - Kills any previous server.py process from this repo path
# - Kills orphaned Playwright driver + headless Chromium processes (optional)
# - Restarts the server with safe defaults (Playwright providers disabled)
#
# Usage:
#   ./scripts/restart_server.zsh
#   ./scripts/restart_server.zsh --port 8001
#   ./scripts/restart_server.zsh --allow-playwright
#   ./scripts/restart_server.zsh --foreground
#   ./scripts/restart_server.zsh --no-kill-playwright

SCRIPT_DIR="${0:A:h}"
PROJECT_ROOT="${SCRIPT_DIR:h}"

PORT="8000"
HOST="127.0.0.1"
ALLOW_PLAYWRIGHT="0"
FOREGROUND="0"
KILL_PLAYWRIGHT="1"
PID_FILE="logs/server.pid"

while (( $# > 0 )); do
  case "$1" in
    --port)
      PORT="${2:-}"
      shift 2
      ;;
    --host)
      HOST="${2:-}"
      shift 2
      ;;
    --allow-playwright)
      ALLOW_PLAYWRIGHT="1"
      shift 1
      ;;
    --foreground)
      FOREGROUND="1"
      shift 1
      ;;
    --no-kill-playwright)
      KILL_PLAYWRIGHT="0"
      shift 1
      ;;
    -h|--help)
      sed -n '1,60p' "$0"
      exit 0
      ;;
    *)
      echo "Unknown arg: $1" >&2
      exit 2
      ;;
  esac
done

if [[ -z "${PORT}" || "${PORT}" != <-> ]]; then
  echo "Invalid --port value: ${PORT}" >&2
  exit 2
fi

cd "${PROJECT_ROOT}"
mkdir -p logs

echo "== FlightFinder Engine restart =="
echo "Project: ${PROJECT_ROOT}"
echo "Host: ${HOST}"
echo "Port: ${PORT}"
echo "ALLOW_PLAYWRIGHT_PROVIDERS=${ALLOW_PLAYWRIGHT}"
echo "Logs: ${PROJECT_ROOT}/logs/server.out and ${PROJECT_ROOT}/logs/engine.log"
echo "PID file: ${PROJECT_ROOT}/${PID_FILE}"
echo

echo "== Stopping any previously recorded server PID =="
if [[ -f "${PID_FILE}" ]]; then
  old_pid="$(cat "${PID_FILE}" 2>/dev/null || true)"
  if [[ -n "${old_pid}" && "${old_pid}" == <-> ]]; then
    if kill -0 "${old_pid}" 2>/dev/null; then
      echo "Killing PID from ${PID_FILE}: ${old_pid}"
      kill "${old_pid}" 2>/dev/null || true
      sleep 1
      if kill -0 "${old_pid}" 2>/dev/null; then
        echo "Force killing PID: ${old_pid}"
        kill -9 "${old_pid}" 2>/dev/null || true
      fi
    else
      echo "PID ${old_pid} is not running."
    fi
  fi
  rm -f "${PID_FILE}" 2>/dev/null || true
else
  echo "No PID file found."
fi
echo

echo "== Stopping any listener on port ${PORT} =="
port_pids="$(lsof -tiTCP:${PORT} -sTCP:LISTEN 2>/dev/null | tr '\n' ' ' || true)"
if [[ -n "${port_pids}" ]]; then
  echo "Killing PIDs: ${port_pids}"
  kill ${=port_pids} 2>/dev/null || true
  sleep 1
  port_pids2="$(lsof -tiTCP:${PORT} -sTCP:LISTEN 2>/dev/null | tr '\n' ' ' || true)"
  if [[ -n "${port_pids2}" ]]; then
    echo "Force killing PIDs: ${port_pids2}"
    kill -9 ${=port_pids2} 2>/dev/null || true
  fi
else
  echo "No process is listening on port ${PORT}."
fi
echo

echo "== Stopping any server.py from this repo path =="
pkill -f "${PROJECT_ROOT}/server.py" 2>/dev/null || true
echo

if [[ "${KILL_PLAYWRIGHT}" == "1" ]]; then
  echo "== Killing orphaned Playwright processes =="
  pkill -f "playwright/driver/node" 2>/dev/null || true
  pkill -f "chrome-headless-shell" 2>/dev/null || true
  pkill -f "ms-playwright" 2>/dev/null || true
  echo
fi

echo "== Starting server =="
if [[ "${FOREGROUND}" == "1" ]]; then
  echo "Foreground mode (Ctrl+C to stop)."
  exec env HOST="${HOST}" PORT="${PORT}" ALLOW_PLAYWRIGHT_PROVIDERS="${ALLOW_PLAYWRIGHT}" python3 -u server.py
else
  # Background mode: write stdout/stderr to logs/server.out
  nohup env HOST="${HOST}" PORT="${PORT}" ALLOW_PLAYWRIGHT_PROVIDERS="${ALLOW_PLAYWRIGHT}" python3 -u server.py > logs/server.out 2>&1 &
  server_pid="$!"
  echo "${server_pid}" > "${PID_FILE}"
  disown || true

  # The server can take a few seconds to import/start; avoid false failures.
  max_wait_seconds=12
  waited=0
  while (( waited < max_wait_seconds )); do
    if lsof -iTCP:${PORT} -sTCP:LISTEN -n -P >/dev/null 2>&1; then
      echo "Server started (PID ${server_pid})."
      echo "Open: http://${HOST}:${PORT}"
      exit 0
    fi
    if ! kill -0 "${server_pid}" 2>/dev/null; then
      break
    fi
    sleep 1
    waited=$(( waited + 1 ))
  done

  echo "Server failed to bind to ${HOST}:${PORT} (waited ${max_wait_seconds}s)." >&2
  echo "Listener check:" >&2
  lsof -iTCP:${PORT} -sTCP:LISTEN -n -P 2>&1 | sed 's/^/  /' >&2 || true
  echo "Process status (PID ${server_pid}):" >&2
  ps -p "${server_pid}" -o pid=,command= 2>&1 | sed 's/^/  /' >&2 || true
  echo "Last 80 lines of logs/server.out:" >&2
  tail -n 80 logs/server.out 2>&1 | sed 's/^/  /' >&2 || true
  exit 1
fi
