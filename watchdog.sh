#!/usr/bin/env bash
# Yama Listener Watchdog (Layer 4)
# - Checks if state/heartbeat.json has not been updated for >= 3 minutes (180 seconds)
# - If missing or stale, runs `systemctl restart yama-listener` and logs the result to state/watchdog.log
# - If restart fails with "not found" (Unit yama-listener.service not found),
#   attempts bounded unit file recovery (with pre-checks, content/hash comparison, max attempts, and exponential backoff):
#   compares source and destination (skips copy if identical, otherwise installs yama-listener.service) ->
#   systemctl daemon-reload -> retries systemctl restart yama-listener.

set -uo pipefail

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STATE_DIR="${YAMA_STATE_DIR:-${BASE_DIR}/state}"
HEARTBEAT_PATH="${YAMA_HEARTBEAT_PATH:-${STATE_DIR}/heartbeat.json}"
LOG_PATH="${YAMA_WATCHDOG_LOG:-${STATE_DIR}/watchdog.log}"
THRESHOLD_SEC="${YAMA_WATCHDOG_THRESHOLD_SEC:-180}"
PYTHON_BIN="${YAMA_PYTHON_BIN:-${BASE_DIR}/venv/bin/python}"
RESTART_CMD="${YAMA_RESTART_CMD:-systemctl restart yama-listener}"

UNIT_SOURCE="${YAMA_UNIT_SOURCE:-${BASE_DIR}/yama-listener.service}"
UNIT_DEST="${YAMA_UNIT_DEST:-/etc/systemd/system/yama-listener.service}"
DAEMON_RELOAD_CMD="${YAMA_DAEMON_RELOAD_CMD:-systemctl daemon-reload}"
UNIT_INSTALL_CMD="${YAMA_UNIT_INSTALL_CMD:-}"
REINSTALL_STATE_PATH="${YAMA_UNIT_REINSTALL_STATE:-${YAMA_REINSTALL_STATE_PATH:-${STATE_DIR}/unit_reinstall_state.json}}"
REINSTALL_MAX="${YAMA_UNIT_REINSTALL_MAX:-3}"
# Default 60s (> 30s hook poll interval) so the 1st backoff window is not already
# elapsed when the next 30-second hook invocation runs.
REINSTALL_BACKOFF_BASE_SEC="${YAMA_UNIT_REINSTALL_BACKOFF_SEC:-60}"

if [[ ! -x "${PYTHON_BIN}" ]]; then
    PYTHON_BIN="python3"
fi

mkdir -p "${STATE_DIR}"

log_msg() {
    local ts
    ts="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
    echo "${ts} $*" >> "${LOG_PATH}"
}

reset_reinstall_state() {
    if [[ ! -f "${REINSTALL_STATE_PATH}" ]]; then
        return 0
    fi
    "${PYTHON_BIN}" - "${REINSTALL_STATE_PATH}" <<'PYEOF'
import json
import sys
import time
from pathlib import Path

state_path = Path(sys.argv[1])
try:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "attempts": 0,
        "last_attempt_ts": 0.0,
        "backoff_sec": 0.0,
        "next_allowed_ts": 0.0,
        "reset_at_ts": time.time(),
    }
    state_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
except Exception:
    pass
PYEOF
}

check_reinstall_gate() {
    "${PYTHON_BIN}" - "${REINSTALL_STATE_PATH}" "${REINSTALL_MAX}" <<'PYEOF'
import json
import math
import sys
import time
from pathlib import Path

state_path = Path(sys.argv[1])
max_attempts = int(sys.argv[2])
now = time.time()

attempts = 0
next_allowed_ts = 0.0

if state_path.exists():
    try:
        raw = state_path.read_text(encoding="utf-8").strip()
        if raw:
            try:
                data = json.loads(raw)
                if isinstance(data, int):
                    attempts = data
                elif isinstance(data, dict):
                    attempts = int(data.get("attempts", 0))
                    next_allowed_ts = float(data.get("next_allowed_ts", 0.0))
            except Exception:
                attempts = int(raw)
    except Exception:
        attempts = 0

if attempts >= max_attempts:
    print(f"MAX_REACHED:{attempts}:{max_attempts}")
elif next_allowed_ts > now:
    rem = int(math.ceil(next_allowed_ts - now))
    print(f"BACKOFF:{attempts}:{max_attempts}:{rem}")
else:
    print(f"ALLOWED:{attempts}:{max_attempts}")
PYEOF
}

record_reinstall_attempt() {
    "${PYTHON_BIN}" - "${REINSTALL_STATE_PATH}" "${REINSTALL_BACKOFF_BASE_SEC}" <<'PYEOF'
import json
import sys
import time
from pathlib import Path

state_path = Path(sys.argv[1])
base_backoff = float(sys.argv[2])
now = time.time()

attempts = 0
if state_path.exists():
    try:
        raw = state_path.read_text(encoding="utf-8").strip()
        if raw:
            try:
                data = json.loads(raw)
                if isinstance(data, int):
                    attempts = data
                elif isinstance(data, dict):
                    attempts = int(data.get("attempts", 0))
            except Exception:
                attempts = int(raw)
    except Exception:
        attempts = 0

new_attempts = attempts + 1
if base_backoff > 0:
    backoff_sec = base_backoff * (2 ** (new_attempts - 1))
else:
    backoff_sec = 0.0
next_allowed_ts = now + backoff_sec

state_path.parent.mkdir(parents=True, exist_ok=True)
payload = {
    "attempts": new_attempts,
    "last_attempt_ts": now,
    "backoff_sec": backoff_sec,
    "next_allowed_ts": next_allowed_ts,
}
state_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
print(f"{new_attempts}:{int(backoff_sec)}")
PYEOF
}

run_unit_install() {
    local dest_dir="$1"
    if [[ -n "${UNIT_INSTALL_CMD}" ]]; then
        ${UNIT_INSTALL_CMD} 2>&1
        return $?
    fi
    if [[ -w "${dest_dir}" ]]; then
        install -m 0644 "${UNIT_SOURCE}" "${UNIT_DEST}" 2>&1
        return $?
    else
        sudo -n install -m 0644 "${UNIT_SOURCE}" "${UNIT_DEST}" 2>&1
        return $?
    fi
}

reinstall_and_restart_unit() {
    local initial_rc="$1"
    local gate_status
    gate_status="$(check_reinstall_gate)"

    if [[ "${gate_status}" == MAX_REACHED:* ]]; then
        local cur_attempts max_attempts
        cur_attempts="$(echo "${gate_status}" | cut -d: -f2)"
        max_attempts="$(echo "${gate_status}" | cut -d: -f3)"
        log_msg "[ERROR] Unit reinstall skipped: max attempts reached (${cur_attempts}/${max_attempts}, state=${REINSTALL_STATE_PATH})"
        return "${initial_rc}"
    fi

    if [[ "${gate_status}" == BACKOFF:* ]]; then
        local cur_attempts max_attempts rem_sec
        cur_attempts="$(echo "${gate_status}" | cut -d: -f2)"
        max_attempts="$(echo "${gate_status}" | cut -d: -f3)"
        rem_sec="$(echo "${gate_status}" | cut -d: -f4)"
        log_msg "[WARN] Unit reinstall skipped: backoff active (${rem_sec}s remaining, attempts=${cur_attempts}/${max_attempts})"
        return "${initial_rc}"
    fi

    # Pre-check source unit file and destination directory before attempting reinstall
    if [[ ! -f "${UNIT_SOURCE}" || ! -r "${UNIT_SOURCE}" ]]; then
        log_msg "[ERROR] Unit reinstall aborted: source unit file missing or unreadable (${UNIT_SOURCE})"
        return "${initial_rc}"
    fi

    local dest_dir
    dest_dir="$(dirname "${UNIT_DEST}")"
    if [[ ! -d "${dest_dir}" ]]; then
        log_msg "[ERROR] Unit reinstall aborted: destination directory does not exist (${dest_dir})"
        return "${initial_rc}"
    fi

    local attempt_info attempt_num backoff_sec
    attempt_info="$(record_reinstall_attempt)"
    attempt_num="$(echo "${attempt_info}" | cut -d: -f1)"
    backoff_sec="$(echo "${attempt_info}" | cut -d: -f2)"

    local src_hash="" dest_hash=""
    if command -v sha256sum >/dev/null 2>&1; then
        src_hash="$(sha256sum "${UNIT_SOURCE}" 2>/dev/null | awk '{print $1}')"
        if [[ -f "${UNIT_DEST}" && -r "${UNIT_DEST}" ]]; then
            dest_hash="$(sha256sum "${UNIT_DEST}" 2>/dev/null | awk '{print $1}')"
        fi
    fi

    if [[ -f "${UNIT_DEST}" ]] && cmp -s "${UNIT_SOURCE}" "${UNIT_DEST}"; then
        log_msg "[INFO] Unit file already present and matches source (sha256=${src_hash:-identical}); skipping copy and running daemon-reload + restart (${attempt_num}/${REINSTALL_MAX}): ${UNIT_DEST}"
    else
        if [[ -f "${UNIT_DEST}" ]]; then
            log_msg "[WARN] Unit not found / hash mismatch detected (src_sha256=${src_hash:-unknown}, dest_sha256=${dest_hash:-unknown}). Attempting unit reinstall (${attempt_num}/${REINSTALL_MAX}, backoff=${backoff_sec}s): ${UNIT_SOURCE} -> ${UNIT_DEST}"
        else
            log_msg "[WARN] Unit not found detected (dest missing, src_sha256=${src_hash:-unknown}). Attempting unit reinstall (${attempt_num}/${REINSTALL_MAX}, backoff=${backoff_sec}s): ${UNIT_SOURCE} -> ${UNIT_DEST}"
        fi

        local install_out install_rc
        if install_out="$(run_unit_install "${dest_dir}")"; then
            log_msg "[INFO] Unit reinstall succeeded (${attempt_num}/${REINSTALL_MAX}): ${UNIT_SOURCE} -> ${UNIT_DEST} ${install_out}"
        else
            install_rc=$?
            log_msg "[ERROR] Unit reinstall failed (${attempt_num}/${REINSTALL_MAX}, exit=${install_rc}, check sudo/permissions): ${install_out}"
            return "${install_rc}"
        fi
    fi

    local reload_out reload_rc
    if reload_out="$(${DAEMON_RELOAD_CMD} 2>&1)"; then
        log_msg "[INFO] Daemon reload succeeded (${attempt_num}/${REINSTALL_MAX}): ${DAEMON_RELOAD_CMD} ${reload_out}"
    else
        reload_rc=$?
        log_msg "[ERROR] Daemon reload failed after unit reinstall (${attempt_num}/${REINSTALL_MAX}, exit=${reload_rc}): ${reload_out}"
        return "${reload_rc}"
    fi

    local retry_out retry_rc
    if retry_out="$(${RESTART_CMD} 2>&1)"; then
        log_msg "[INFO] Restart retry after unit reinstall succeeded (${attempt_num}/${REINSTALL_MAX}): ${RESTART_CMD} ${retry_out}"
        reset_reinstall_state
        return 0
    else
        retry_rc=$?
        log_msg "[ERROR] Restart retry after unit reinstall failed (${attempt_num}/${REINSTALL_MAX}, exit=${retry_rc}): ${retry_out}"
        return "${retry_rc}"
    fi
}

restart_listener() {
    local reason="$1"
    log_msg "[WARN] Heartbeat check failed (${reason}). Attempting: ${RESTART_CMD}"
    local output
    if output=$(${RESTART_CMD} 2>&1); then
        log_msg "[INFO] Restart succeeded: ${RESTART_CMD} ${output}"
        reset_reinstall_state
        return 0
    else
        local rc=$?
        log_msg "[ERROR] Restart failed (exit=${rc}): ${output}"
        if [[ "${output,,}" == *"not found"* ]]; then
            reinstall_and_restart_unit "${rc}"
            return $?
        fi
        return "${rc}"
    fi
}

if [[ ! -f "${HEARTBEAT_PATH}" ]]; then
    restart_listener "missing heartbeat file: ${HEARTBEAT_PATH}"
    exit $?
fi

CHECK_RESULT="$("${PYTHON_BIN}" - "${HEARTBEAT_PATH}" "${THRESHOLD_SEC}" <<'PYEOF'
import json
import os
import sys
import time

hb_path = sys.argv[1]
threshold = float(sys.argv[2])
now = time.time()

try:
    with open(hb_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    hb_ts = float(data.get("timestamp", 0))
except Exception:
    hb_ts = 0.0

if hb_ts <= 0:
    try:
        hb_ts = os.path.getmtime(hb_path)
    except Exception:
        print("STALE:unreadable_heartbeat")
        sys.exit(0)

age = now - hb_ts
if age >= threshold:
    print(f"STALE:age={int(age)}s>=threshold={int(threshold)}s")
else:
    print(f"OK:age={int(age)}s")
PYEOF
)"

if [[ "${CHECK_RESULT}" == STALE:* ]]; then
    restart_listener "${CHECK_RESULT}"
    exit $?
else
    log_msg "[OK] Heartbeat healthy (${CHECK_RESULT})"
    exit 0
fi
