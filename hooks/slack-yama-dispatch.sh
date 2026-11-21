#!/usr/bin/env bash
# Hook: poll for PENDING Slack events collected by the Yama listener.
# NOTE: the platform executes this file directly every 30s and bash reads it
# incrementally. Edit it only via tmp-file in this directory + bash -n + mv
# (never overwrite it in place: a half-written file could be executed).
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
# Guard: never execute a child script whose syntax is broken (e.g. mid-edit
# or a bad save). bash reads scripts incrementally, so a torn file could
# otherwise run half-old/half-new code. Skip this cycle and log it; the
# next 30s poll retries. (Python children fail fast on their own, so no guard.)
WD_OK=1
if bash -n "$PROJECT_DIR/watchdog.sh" 2>/dev/null; then
  "$PROJECT_DIR/watchdog.sh" >/dev/null 2>&1 || true
else
  mkdir -p "$(dirname "$WATCHDOG_LOG")" 2>/dev/null || true
  echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") [WARN] Watchdog skipped: watchdog.sh failed bash -n" >> "$WATCHDOG_LOG" 2>/dev/null || true
  WD_OK=0
fi

# Listener syntax check (same cycle, for the escalation clock below): the
# watchdog only py_compiles listener.py when a restart is attempted, so a
# broken save can sit unnoticed while the running listener still looks
# healthy. Check here every cycle; the result only feeds the escalation
# below and the log line (never blocks dispatch).
LISTENER_OK=1
if ! "$PYTHON_BIN" -m py_compile "$PROJECT_DIR/listener.py" 2>/dev/null; then
  LISTENER_OK=0
  mkdir -p "$(dirname "$WATCHDOG_LOG")" 2>/dev/null || true
  echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") [WARN] Listener skipped: listener.py failed py_compile" >> "$WATCHDOG_LOG" 2>/dev/null || true
fi

set +e
DISP_OK=1
if bash -n "$PROJECT_DIR/dispatcher.sh" 2>/dev/null; then
  output=$("$PROJECT_DIR/dispatcher.sh" 2>/dev/null)
  code=$?
else
  mkdir -p "$(dirname "$WATCHDOG_LOG")" 2>/dev/null || true
  echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") [WARN] Dispatcher skipped: dispatcher.sh failed bash -n" >> "$WATCHDOG_LOG" 2>/dev/null || true
  output="NO_EVENTS"
  code=1
  DISP_OK=0
fi
set -e

# --- syntax-guard escalation: begin (extracted verbatim by tests/run_tests.py test 64) ---
# Neiro's 10-minute split, mapped onto Yama's receivers: a target that keeps
# failing its syntax check is skipped quietly per cycle, but once it has been
# broken for >= SYNTAX_GUARD_WARN_SEC (default 600s) the hook wakes the
# parent agent once so the breakage is seen (it lands in the Slack-ops chat).
# Exit 2 is deliberately NOT used: Yama has no systemd-timer receiver for it,
# and hook `log` only prints to stderr, so `wake` is the only channel that
# actually reaches anyone. State lives in the hook runtime state
# (hook_state_get/set: atomic tmp+mv, dry-run safe) under "syntax_guard".
# Recovery wakes once too, but only if a warning was previously sent.
SYNTAX_GUARD_WARN_SEC="${YAMA_SYNTAX_GUARD_WARN_SEC:-600}"
_SG_WATCHDOG_LOG="${WATCHDOG_LOG:-${YAMA_WATCHDOG_LOG:-${PROJECT_DIR:-/home/hatch/workspace/slack-yama}/state/watchdog.log}}"
syntax_guard_now() {
  if [[ -n "${YAMA_SYNTAX_GUARD_NOW:-}" ]]; then printf '%s' "${YAMA_SYNTAX_GUARD_NOW}"; else date +%s; fi
}
syntax_guard_eval() {
  # $1 = target name, $2 = 1 if broken this cycle else 0.
  # Echoes "warn:<target>" when the warning is due, "recovered:<target>"
  # when a previously warned target is fixed, nothing otherwise.
  # Does not persist warned=true; caller calls syntax_guard_mark_warned only
  # after the warning wake succeeds. Best-effort by design: never fails the hook.
  local target="$1" broken="$2"
  local now state first_fail warned age new_state
  now="$(syntax_guard_now)"
  state="$(hook_state_get 2>/dev/null || echo '{}')"
  first_fail="$(printf '%s' "$state" | jq -r --arg t "$target" '.syntax_guard[$t].first_fail // 0' 2>/dev/null || echo 0)"
  warned="$(printf '%s' "$state" | jq -r --arg t "$target" '.syntax_guard[$t].warned // false' 2>/dev/null || echo false)"
  if [[ "$broken" == "1" ]]; then
    if [[ "$first_fail" == "0" ]]; then
      first_fail="$now"
      new_state="$(printf '%s' "$state" | jq --arg t "$target" --argjson ff "$first_fail" '.syntax_guard[$t] = {"first_fail": $ff, "warned": false}' 2>/dev/null || echo '')"
      [[ -n "$new_state" ]] && hook_state_set "$new_state" 2>/dev/null || true
    fi
    age=$((now - first_fail))
    if [[ "$warned" != "true" && "$age" -ge "$SYNTAX_GUARD_WARN_SEC" ]]; then
      echo "warn:${target}"
    fi
  else
    if [[ "$first_fail" != "0" ]]; then
      new_state="$(printf '%s' "$state" | jq --arg t "$target" 'del(.syntax_guard[$t])' 2>/dev/null || echo '')"
      [[ -n "$new_state" ]] && hook_state_set "$new_state" 2>/dev/null || true
      if [[ "$warned" == "true" ]]; then
        echo "recovered:${target}"
      fi
    fi
  fi
  return 0
}
syntax_guard_mark_warned() {
  # $1 = target name. Marks syntax_guard[target].warned = true while
  # preserving first_fail. Best-effort by design.
  local target="$1"
  local state first_fail new_state
  state="$(hook_state_get 2>/dev/null || echo '{}')"
  first_fail="$(printf '%s' "$state" | jq -r --arg t "$target" '.syntax_guard[$t].first_fail // 0' 2>/dev/null || echo 0)"
  if [[ ! "$first_fail" =~ ^[0-9]+$ || "$first_fail" == "0" ]]; then
    first_fail="$(syntax_guard_now)"
  fi
  new_state="$(printf '%s' "$state" | jq --arg t "$target" --argjson ff "$first_fail" '.syntax_guard[$t] = {"first_fail": $ff, "warned": true}' 2>/dev/null || echo '')"
  [[ -n "$new_state" ]] && hook_state_set "$new_state" 2>/dev/null || true
  return 0
}

# Evaluate the three syntax-guard targets and escalate if any has been broken
# for 10+ minutes (or report a recovery). A warning wake takes this cycle;
# pending events wait one 30s cycle, which is acceptable for a warning.
SYNTAX_WARN_TARGETS=()
SYNTAX_RECOVERED_TARGETS=()
for _sg in "watchdog.sh:${WD_OK:-1}" "dispatcher.sh:${DISP_OK:-1}" "listener.py:${LISTENER_OK:-1}"; do
  _sg_target="${_sg%%:*}"
  _sg_ok="${_sg##*:}"
  if [[ "$_sg_ok" == "0" ]]; then _sg_res="$(syntax_guard_eval "$_sg_target" 1 || true)"; else _sg_res="$(syntax_guard_eval "$_sg_target" 0 || true)"; fi
  case "$_sg_res" in
    warn:*) SYNTAX_WARN_TARGETS+=("${_sg_res#warn:}") ;;
    recovered:*) SYNTAX_RECOVERED_TARGETS+=("${_sg_res#recovered:}") ;;
  esac
done
if [[ "${#SYNTAX_WARN_TARGETS[@]}" -gt 0 ]]; then
  if _sg_targets_json="$(printf '%s\n' "${SYNTAX_WARN_TARGETS[@]}" | jq -R . | jq -s -c . 2>/dev/null)" \
    && _sg_first="$(printf '%s' "$(hook_state_get 2>/dev/null || echo '{}')" | jq -r --argjson targets "$_sg_targets_json" '[(.syntax_guard // {}) as $sg | $targets[] | $sg[.].first_fail | select(type == "number")] | min // 0' 2>/dev/null)" \
    && _sg_payload="$(jq -n -c \
      --argjson targets "$_sg_targets_json" \
      --argjson first "$_sg_first" \
      --argjson warn_sec "$SYNTAX_GUARD_WARN_SEC" \
      '{"syntax_guard_warning": true, "targets": $targets, "first_failed_at_epoch": $first, "warn_seconds": $warn_sec}' 2>/dev/null)"; then
    if wake "Syntax guard: target(s) broken for 10+ min" "$_sg_payload"; then
      for _sg_t in "${SYNTAX_WARN_TARGETS[@]}"; do
        syntax_guard_mark_warned "$_sg_t"
      done
    else
      mkdir -p "$(dirname "$_SG_WATCHDOG_LOG")" 2>/dev/null || true
      echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") [WARN] Syntax guard warning wake failed (targets=${SYNTAX_WARN_TARGETS[*]})" >> "$_SG_WATCHDOG_LOG" 2>/dev/null || true
    fi
  else
    mkdir -p "$(dirname "$_SG_WATCHDOG_LOG")" 2>/dev/null || true
    echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") [WARN] Syntax guard warning payload build failed (targets=${SYNTAX_WARN_TARGETS[*]})" >> "$_SG_WATCHDOG_LOG" 2>/dev/null || true
  fi
fi
if [[ "${#SYNTAX_RECOVERED_TARGETS[@]}" -gt 0 ]]; then
  if _sg_rec_targets_json="$(printf '%s\n' "${SYNTAX_RECOVERED_TARGETS[@]}" | jq -R . | jq -s -c . 2>/dev/null)" \
    && _sg_rec_payload="$(jq -n -c \
      --argjson targets "$_sg_rec_targets_json" \
      '{"syntax_guard_recovered": true, "targets": $targets}' 2>/dev/null)"; then
    if ! wake "Syntax guard: target(s) recovered" "$_sg_rec_payload"; then
      mkdir -p "$(dirname "$_SG_WATCHDOG_LOG")" 2>/dev/null || true
      echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") [WARN] Syntax guard recovery wake failed (targets=${SYNTAX_RECOVERED_TARGETS[*]})" >> "$_SG_WATCHDOG_LOG" 2>/dev/null || true
    fi
  else
    mkdir -p "$(dirname "$_SG_WATCHDOG_LOG")" 2>/dev/null || true
    echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") [WARN] Syntax guard recovery payload build failed (targets=${SYNTAX_RECOVERED_TARGETS[*]})" >> "$_SG_WATCHDOG_LOG" 2>/dev/null || true
  fi
fi
# --- syntax-guard escalation: end ---

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
