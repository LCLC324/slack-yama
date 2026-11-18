#!/usr/bin/env bash
# Hook: poll for PENDING Slack events collected by the Yama listener.
# - Real run: invoke the project dispatcher (moves PENDING -> PROCESSING under
#   flock and writes state/payload.json), then wake the agent with the payload.
# - Dry run: only count PENDING events; never consume state.
set -euo pipefail
source "$HATCH_HOOK_RUNTIME"

PROJECT_DIR="/home/hatch/workspace/slack-yama"
DB_PATH="$PROJECT_DIR/state/slack_events.db"
PAYLOAD_PATH="$PROJECT_DIR/state/payload.json"
PYTHON_BIN="$PROJECT_DIR/venv/bin/python"

if [[ "${HATCH_HOOK_DRY_RUN:-0}" == "1" ]]; then
  count=0
  if [[ -f "$DB_PATH" ]]; then
    count=$("$PYTHON_BIN" - "$DB_PATH" <<'PYEOF'
import sqlite3
import sys

try:
    conn = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True)
    row = conn.execute(
        "SELECT COUNT(*) FROM events WHERE status = 'PENDING'"
    ).fetchone()
    print(row[0] if row else 0)
except Exception:
    print(0)
PYEOF
)
  fi
  if [[ "$count" -gt 0 ]]; then
    wake "dry-run: ${count} PENDING Slack event(s) waiting (state not consumed)" "{\"pending\": ${count}, \"dry_run\": true}"
  else
    silent "dry-run: no PENDING Slack events" "{\"pending\": 0, \"dry_run\": true}"
  fi
  exit 0
fi

# Self-heal: if the listener's systemd unit file has vanished (e.g. the VM
# was replaced; /etc does not persist) or differs from the project copy
# (hash mismatch), reinstall it from the project copy.
# Note: `systemctl enable --now` starts an inactive unit but does NOT restart an
# already-running service. To avoid conflicting with watchdog.sh's backoff/health
# management or failing in environments without restart privileges, we do not
# force a service restart here; instead, if an existing unit file was replaced
# (or the service was already active), we log that a restart is required for the
# updated unit definition to take effect.
UNIT_SOURCE="${YAMA_UNIT_SOURCE:-$PROJECT_DIR/yama-listener.service}"
UNIT_DEST="${YAMA_UNIT_DEST:-/etc/systemd/system/yama-listener.service}"
WATCHDOG_LOG="${YAMA_WATCHDOG_LOG:-$PROJECT_DIR/state/watchdog.log}"
SYSTEMCTL_BIN="${YAMA_SYSTEMCTL_BIN:-systemctl}"
if [[ -f "$UNIT_SOURCE" ]] && ! cmp -s "$UNIT_SOURCE" "$UNIT_DEST"; then
  unit_existed=0
  if [[ -f "$UNIT_DEST" ]]; then
    unit_existed=1
  fi
  service_was_active=0
  if "$SYSTEMCTL_BIN" is-active --quiet yama-listener.service 2>/dev/null; then
    service_was_active=1
  fi
  if cp "$UNIT_SOURCE" "$UNIT_DEST" 2>/dev/null; then
    "$SYSTEMCTL_BIN" daemon-reload 2>/dev/null || true
    "$SYSTEMCTL_BIN" enable --now yama-listener.service >/dev/null 2>&1 || true
    if [[ "$unit_existed" -eq 1 || "$service_was_active" -eq 1 ]]; then
      mkdir -p "$(dirname "$WATCHDOG_LOG")" 2>/dev/null || true
      echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") [WARN] Hook self-heal replaced unit file (${UNIT_SOURCE} -> ${UNIT_DEST}); service restart required (systemctl restart yama-listener.service) to apply updated unit definition" >> "$WATCHDOG_LOG" 2>/dev/null || true
      if declare -F log >/dev/null 2>&1; then
        log "unit file replaced; service restart required to apply updated unit definition" "{\"unit\": \"yama-listener.service\", \"restart_required\": true}" || true
      fi
    fi
  else
    mkdir -p "$(dirname "$WATCHDOG_LOG")" 2>/dev/null || true
    echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") [WARN] Hook self-heal could not replace unit file (${UNIT_SOURCE} -> ${UNIT_DEST}); check permissions, service restart required after update" >> "$WATCHDOG_LOG" 2>/dev/null || true
    if declare -F log >/dev/null 2>&1; then
      log "unit file self-heal copy failed; check permissions and restart service after update" "{\"unit\": \"yama-listener.service\", \"restart_required\": true}" || true
    fi
  fi
fi

# Backfill: recover any posts the listener missed (e.g. while it was down)
# by diffing Slack history against the local events DB. Best-effort.
"$PROJECT_DIR/venv/bin/python" "$PROJECT_DIR/backfill.py" >/dev/null 2>&1 || true

# Reconcile: periodically (at most once per hour) scan candidate threads for
# late-landed Yama replies and record them into the replies ledger. Best-effort.
RECONCILE_LAST_RUN_FILE="$PROJECT_DIR/state/reconcile_last_run"
now_epoch=$(date +%s)
last_reconcile=0
if [[ -f "$RECONCILE_LAST_RUN_FILE" ]]; then
  raw_last=$(cat "$RECONCILE_LAST_RUN_FILE" 2>/dev/null || echo 0)
  if [[ "$raw_last" =~ ^[0-9]+$ ]]; then
    last_reconcile="$raw_last"
  fi
fi
if (( now_epoch - last_reconcile >= 3600 )); then
  mkdir -p "$PROJECT_DIR/state" 2>/dev/null || true
  echo "$now_epoch" > "$RECONCILE_LAST_RUN_FILE" 2>/dev/null || true
  {
    echo "[$(date -Iseconds)] START reconcile-replies --days 7" >> "$PROJECT_DIR/state/reconcile_history.log"
    "$PROJECT_DIR/venv/bin/python" "$PROJECT_DIR/agent_tools.py" reconcile-replies --days 7 > "$PROJECT_DIR/state/reconcile_last_output.log" 2>&1
    echo "[$(date -Iseconds)] END reconcile-replies (exit: $?)" >> "$PROJECT_DIR/state/reconcile_history.log"
  } || true
fi

# Auto-join: periodically (at most once per 5 minutes) join public channels the
# owner is a member of (skipping channels Yama was removed from) and seed their
# recent top-level posts as PENDING. Best-effort; never affects dispatch.
{
  AUTO_JOIN_LAST_RUN_FILE="$PROJECT_DIR/state/auto_join_last_run"
  aj_now_epoch=$(date +%s)
  last_auto_join=0
  if [[ -f "$AUTO_JOIN_LAST_RUN_FILE" ]]; then
    raw_aj_last=$(cat "$AUTO_JOIN_LAST_RUN_FILE" 2>/dev/null || echo 0)
    if [[ "$raw_aj_last" =~ ^[0-9]+$ ]]; then
      last_auto_join="$raw_aj_last"
    fi
  fi
  if (( aj_now_epoch - last_auto_join >= 300 )); then
    mkdir -p "$PROJECT_DIR/state" 2>/dev/null || true
    echo "$aj_now_epoch" > "$AUTO_JOIN_LAST_RUN_FILE" 2>/dev/null || true
    "$PROJECT_DIR/venv/bin/python" "$PROJECT_DIR/auto_join.py" >> "$PROJECT_DIR/state/auto_join.log" 2>&1 || true
  fi
} || true

# Keep the listener's proxy credentials fresh: the egress proxy credentials
# rotate, and the systemd service can only read config/.env. This hook runs
# with the current environment, so sync the proxy lines into .env whenever
# they differ. The listener picks them up on its next (re)start.
ENV_FILE="${YAMA_ENV_FILE:-$PROJECT_DIR/config/.env}"
if [[ -f "$ENV_FILE" ]]; then
  tmp_env=$(mktemp "${ENV_FILE}.tmp.XXXXXX")
  chmod 600 "$tmp_env"
  grep -v -E '^(http_proxy|https_proxy|HTTP_PROXY|HTTPS_PROXY|all_proxy|ALL_PROXY|no_proxy|NO_PROXY)=' "$ENV_FILE" > "$tmp_env" || true
  for v in http_proxy https_proxy HTTP_PROXY HTTPS_PROXY all_proxy ALL_PROXY no_proxy NO_PROXY; do
    if [[ -n "${!v:-}" ]]; then
      echo "$v=${!v}" >> "$tmp_env"
    fi
  done
  if ! cmp -s "$tmp_env" "$ENV_FILE"; then
    chmod 600 "$tmp_env"
    mv -f "$tmp_env" "$ENV_FILE"
  fi
  rm -f "$tmp_env"
fi

# Best-effort listener watchdog: restarts the listener only when its
# heartbeat has been stale for more than 3 minutes. Silent when healthy.
"$PROJECT_DIR/watchdog.sh" >/dev/null 2>&1 || true

set +e
output=$("$PROJECT_DIR/dispatcher.sh" 2>/dev/null)
code=$?
set -e

case "$code" in
  0)
    payload=$(cat "$PAYLOAD_PATH")
    wake "New Slack events dispatched for Yama triage" "$payload"
    ;;
  1)
    silent "No PENDING Slack events" '{}'
    ;;
  2)
    silent "Dispatcher locked; previous batch still processing" '{}'
    ;;
  *)
    log "dispatcher error" "{\"exit_code\": ${code}}"
    silent "Dispatcher error (exit ${code})" '{}'
    ;;
esac
