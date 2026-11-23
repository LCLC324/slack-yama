#!/usr/bin/env bash
# Yama Slack Event Dispatcher (Layer 2)
# NOTE: the dispatch hook executes this file every 30s and bash reads it
# incrementally. Edit it only via tmp-file + bash -n + mv in this directory.
# - Uses flock to prevent concurrent execution
# - Fetches up to 10 oldest PENDING events from SQLite, transitions them to PROCESSING,
#   and writes them to state/payload.json
# - Burst cap (2026-11-22): the batch also stops early once the summed event
#   weight (min(len(text), PAYLOAD_TEXT_LIMIT) + EVENT_WEIGHT_OVERHEAD per event)
#   would exceed BATCH_WEIGHT_BUDGET. The oldest event is always taken, and
#   order is never skipped, so a heavy event cannot starve. Deferred events stay
#   PENDING (picked up next cycle; ops_monitor's PENDING-stale alert covers
#   them if they linger) and are counted in payload.deferred.
#   Event text longer than PAYLOAD_TEXT_LIMIT is cut in the payload only (DB
#   keeps the full text) with text_truncated/text_length and a marker telling
#   the worker to run `agent_tools.py event-text --event-id <id>`.
# - Context bundling (2026-11-22): events carry context_key / context_ok /
#   context_gaps; contexts are inlined (small payloads) or in
#   state/payload_context.json (payload.context_file). See context_bundle.py.
# - Exit codes & stdout for hook/caller decision:
#     0 / "DISPATCHED:<N>" : N events transitioned to PROCESSING and written to state/payload.json
#     1 / "NO_EVENTS"      : No PENDING events found (do not wake agent)
#     2 / "LOCKED"         : Another dispatcher/session holds the flock lock (do not wake agent)

set -euo pipefail

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STATE_DIR="${YAMA_STATE_DIR:-${BASE_DIR}/state}"
DB_PATH="${YAMA_DB_PATH:-${STATE_DIR}/slack_events.db}"
PAYLOAD_PATH="${YAMA_PAYLOAD_PATH:-${STATE_DIR}/payload.json}"
LOCK_PATH="${YAMA_LOCK_PATH:-${STATE_DIR}/dispatcher.lock}"
MODE_FILE="${YAMA_MODE_FILE:-${BASE_DIR}/config/mode.txt}"
PYTHON_BIN="${YAMA_PYTHON_BIN:-${BASE_DIR}/venv/bin/python}"
# Threshold (in seconds) to distinguish active vs. abandoned PROCESSING events:
# 900s (15 minutes). Note (2026-11-23): the hook's script_timeout_secs (600s) limits
# the hook *scripts* (watchdog / dispatcher / backfill), NOT the woken worker (the LLM
# session); nothing here caps a worker's run time. 900s is simply well above the
# observed batch durations (p99 300s, max 930s once in 335 batches, 2026-11-21..23),
# so a slow in-flight batch is normally not preempted, while PROCESSING events
# stranded by a crash are reaped to PENDING. A worker that outlives 900s can still
# commit late; commit_event's terminal guard keeps such a late commit from undoing an
# already settled (DONE/IGNORED/FAILED) event, but it cannot tell two holders of the
# same PROCESSING row apart (that needs a claim token, stage 2).
PROCESSING_TIMEOUT_SEC="${YAMA_PROCESSING_TIMEOUT_SEC:-900}"
# Reaper re-queue cap (candidate 12): an event reaped this many times in total
# (reap_count, separate from retry_count) is moved to FAILED with
# error_message "reaped_too_many_times: ...". Each Reaper run that reaps or
# fails anything appends one aggregated alert line to the ops alert outbox,
# which the dispatch hook delivers to the ops chat via wake.
REAP_LIMIT="${YAMA_REAP_LIMIT:-3}"
OPS_ALERTS_PATH="${YAMA_OPS_ALERTS_PATH:-${STATE_DIR}/ops_alerts.jsonl}"
# Burst cap (see header). Defaults: 10 typical events (avg 363 chars measured)
# weigh ~13.6k and pass unchanged; ten 1,500-char posts are split 6 + 4.
PAYLOAD_TEXT_LIMIT="${YAMA_PAYLOAD_TEXT_LIMIT:-4000}"
EVENT_WEIGHT_OVERHEAD="${YAMA_EVENT_WEIGHT_OVERHEAD:-1000}"
BATCH_WEIGHT_BUDGET="${YAMA_BATCH_WEIGHT_BUDGET:-15000}"
# Context bundling (stall fix 2026-11-22, proposal 2): after the batch is moved to
# PROCESSING, context_bundle.py builds each thread's context from the local DB and
# writes it to state/payload_context.json; it is also inlined into payload.json
# when the whole payload stays within PAYLOAD_INLINE_MAX_BYTES (largest wake
# payload observed so far: ~15KB). Best-effort: any failure only marks events
# context_ok=false (the worker then reads Slack via `history`, as before).
PAYLOAD_INLINE_MAX_BYTES="${YAMA_PAYLOAD_INLINE_MAX_BYTES:-12000}"
CONTEXT_BUNDLE_ENABLED="${YAMA_CONTEXT_BUNDLE:-1}"

if [[ ! -x "${PYTHON_BIN}" ]]; then
    PYTHON_BIN="python3"
fi

mkdir -p "${STATE_DIR}"

# Acquire non-blocking exclusive lock on FD 9
exec 9>"${LOCK_PATH}"
if ! flock -n 9; then
    echo "LOCKED"
    exit 2
fi

# Optional hook for holding lock during concurrency tests
if [[ -n "${YAMA_HOLD_LOCK_SEC:-}" ]]; then
    sleep "${YAMA_HOLD_LOCK_SEC}"
fi

if [[ ! -f "${DB_PATH}" ]]; then
    echo "NO_EVENTS"
    exit 1
fi

"${PYTHON_BIN}" - "${DB_PATH}" "${PAYLOAD_PATH}" "${MODE_FILE}" "${PROCESSING_TIMEOUT_SEC}" "${REAP_LIMIT}" "${OPS_ALERTS_PATH}" "${PAYLOAD_TEXT_LIMIT}" "${EVENT_WEIGHT_OVERHEAD}" "${BATCH_WEIGHT_BUDGET}" "${BASE_DIR}" "${PAYLOAD_INLINE_MAX_BYTES}" "${CONTEXT_BUNDLE_ENABLED}" <<'PYEOF'
import fcntl
import json
import os
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

# Default 900s (15 minutes): well above observed batch durations (the worker itself
# has no enforced time limit; script_timeout_secs 600s applies to hook scripts only),
# to avoid concurrent dispatch / payload.json overwrite while a batch is still running,
# and to reap abandoned PROCESSING events back to PENDING after a crash.
DEFAULT_PROCESSING_TIMEOUT_SEC = 900

db_path = Path(sys.argv[1])
payload_path = Path(sys.argv[2])
mode_file = Path(sys.argv[3])
processing_timeout_sec = (
    int(sys.argv[4]) if len(sys.argv) > 4 else DEFAULT_PROCESSING_TIMEOUT_SEC
)
reap_limit = int(sys.argv[5]) if len(sys.argv) > 5 else 3
ops_alerts_path = Path(sys.argv[6]) if len(sys.argv) > 6 else db_path.parent / "ops_alerts.jsonl"
payload_text_limit = int(sys.argv[7]) if len(sys.argv) > 7 else 4000
event_weight_overhead = int(sys.argv[8]) if len(sys.argv) > 8 else 1000
batch_weight_budget = int(sys.argv[9]) if len(sys.argv) > 9 else 15000
base_dir = sys.argv[10] if len(sys.argv) > 10 else ""
payload_inline_max_bytes = int(sys.argv[11]) if len(sys.argv) > 11 else 12000
context_bundle_enabled = (sys.argv[12] if len(sys.argv) > 12 else "1") == "1"
BATCH_MAX_EVENTS = 10


def event_weight(row):
    text_len = len(row["text"] or "")
    if payload_text_limit > 0:
        text_len = min(text_len, payload_text_limit)
    return text_len + event_weight_overhead


def append_ops_alert(entry):
    """Append one JSON line to the ops alert outbox (same flock as ops_monitor.py)."""
    ops_alerts_path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(entry, ensure_ascii=False, sort_keys=True) + "\n"
    with open(ops_alerts_path, "a", encoding="utf-8") as f:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        try:
            f.write(line)
            f.flush()
            os.fsync(f.fileno())
        finally:
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)

mode = "shadow"
if mode_file.exists():
    raw_mode = mode_file.read_text(encoding="utf-8").strip()
    if raw_mode:
        mode = raw_mode


def parse_utc_timestamp(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=timezone.utc)
    raw = str(value).strip()
    if not raw:
        return None
    try:
        return datetime.fromtimestamp(float(raw), tz=timezone.utc)
    except ValueError:
        pass
    normalized = raw[:-1] + "+00:00" if raw.endswith(("Z", "z")) else raw
    try:
        dt = datetime.fromisoformat(normalized)
        return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)
    except ValueError:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f", "%Y/%m/%d %H:%M:%S"):
        try:
            return datetime.strptime(raw, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


# Unified 30s (30000ms) busy timeout matching listener.get_db_connection()
# so connect(timeout=30.0) is not silently downgraded by PRAGMA busy_timeout.
DB_BUSY_TIMEOUT_SEC = 30.0
DB_BUSY_TIMEOUT_MS = int(DB_BUSY_TIMEOUT_SEC * 1000)

conn = sqlite3.connect(str(db_path), timeout=DB_BUSY_TIMEOUT_SEC)
conn.row_factory = sqlite3.Row
conn.execute("PRAGMA journal_mode=WAL;")
conn.execute(f"PRAGMA busy_timeout={DB_BUSY_TIMEOUT_MS};")

try:
    conn.execute("BEGIN IMMEDIATE;")

    cols = {
        row["name"]
        for row in conn.execute("PRAGMA table_info(events);").fetchall()
    }
    if "next_retry_at" not in cols:
        conn.execute("ALTER TABLE events ADD COLUMN next_retry_at TIMESTAMP;")
    if "first_unverifiable_at" not in cols:
        conn.execute("ALTER TABLE events ADD COLUMN first_unverifiable_at TIMESTAMP;")
    if "reap_count" not in cols:
        conn.execute("ALTER TABLE events ADD COLUMN reap_count INTEGER NOT NULL DEFAULT 0;")

    now_utc = datetime.now(timezone.utc)
    proc_rows = conn.execute(
        """
        SELECT event_id, updated_at, created_at, reap_count
        FROM events
        WHERE status = 'PROCESSING'
        """
    ).fetchall()

    stale_procs = []
    active_proc_count = 0
    for pr in proc_rows:
        ts_dt = parse_utc_timestamp(pr["updated_at"]) or parse_utc_timestamp(pr["created_at"])
        if ts_dt is None or (now_utc - ts_dt).total_seconds() >= processing_timeout_sec:
            stale_procs.append(pr)
        else:
            active_proc_count += 1

    # Reaper: revert stale PROCESSING events (>= processing_timeout_sec) back to PENDING,
    # counting each reap in reap_count; once an event has been reaped reap_limit times
    # in total it is stopped as FAILED (reaped_too_many_times) instead of cycling forever.
    reaped_ids = []
    failed_ids = []
    for pr in stale_procs:
        new_reap_count = int(pr["reap_count"] or 0) + 1
        if new_reap_count >= reap_limit:
            conn.execute(
                """
                UPDATE events
                SET status = 'FAILED',
                    reap_count = ?,
                    next_retry_at = NULL,
                    error_message = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE event_id = ?
                """,
                (
                    new_reap_count,
                    (
                        f"reaped_too_many_times: PROCESSING exceeded {processing_timeout_sec}s "
                        f"and was reaped {new_reap_count} times (limit {reap_limit}); "
                        "stopped by the Reaper re-queue cap"
                    ),
                    pr["event_id"],
                ),
            )
            failed_ids.append(pr["event_id"])
        else:
            conn.execute(
                """
                UPDATE events
                SET status = 'PENDING',
                    reap_count = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE event_id = ?
                """,
                (new_reap_count, pr["event_id"]),
            )
            reaped_ids.append(pr["event_id"])

    def report_reaper():
        # Called only after the Reaper's changes are committed. One aggregated
        # line per Reaper run. A write failure must not change the dispatch
        # result, so it is surfaced on stderr, which the dispatch hook appends
        # to state/dispatcher_stderr.log and turns into an ops alert.
        if not reaped_ids and not failed_ids:
            return
        try:
            append_ops_alert(
                {
                    "kind": "reaper",
                    "at": datetime.now(timezone.utc).isoformat(),
                    "processing_timeout_sec": processing_timeout_sec,
                    "reap_limit": reap_limit,
                    "reaped_count": len(reaped_ids),
                    "reaped_event_ids": reaped_ids,
                    "failed_count": len(failed_ids),
                    "failed_event_ids": failed_ids,
                }
            )
        except Exception as alert_exc:
            print(
                f"REAPER_ALERT_WRITE_FAILED: {alert_exc} reaped={reaped_ids} failed={failed_ids}",
                file=sys.stderr,
            )

    # Guard: if any recent PROCESSING event (< processing_timeout_sec) is still in flight,
    # skip new dispatch so we do not overwrite state/payload.json or wake a second session.
    if active_proc_count > 0:
        conn.commit()
        report_reaper()
        print("LOCKED")
        sys.exit(2)

    # Wait-state design: events that failed with pre_post_check_unverifiable stay in
    # status='PENDING' with next_retry_at set in the future (without incrementing retry_count).
    # Keeping status='PENDING' and filtering out future next_retry_at here avoids introducing
    # a separate non-standard status that would need another daemon to transition back to PENDING:
    # while next_retry_at > now_utc the 30s dispatcher skips the event, and once the wait time
    # passes it automatically returns to normal dispatch.
    pending_rows = conn.execute(
        """
        SELECT event_id, channel_id, user_id, text, ts, thread_ts, is_bot,
               status, retry_count, error_message, next_retry_at, first_unverifiable_at,
               created_at, updated_at
        FROM events
        WHERE status = 'PENDING'
        ORDER BY CAST(ts AS REAL) ASC, created_at ASC, rowid ASC
        """
    ).fetchall()

    rows = []
    eligible_count = 0
    batch_weight = 0
    budget_stopped = False
    for r in pending_rows:
        retry_dt = parse_utc_timestamp(r["next_retry_at"])
        if retry_dt is not None and retry_dt > now_utc:
            continue
        eligible_count += 1
        if budget_stopped or len(rows) >= BATCH_MAX_EVENTS:
            continue
        w = event_weight(r)
        # Burst cap: keep FIFO order; stop (never skip ahead) once the next event
        # would overflow the budget. The first event is always taken.
        if rows and batch_weight_budget > 0 and batch_weight + w > batch_weight_budget:
            budget_stopped = True
            continue
        rows.append(r)
        batch_weight += w
    deferred_count = eligible_count - len(rows)

    if not rows:
        conn.commit()
        report_reaper()
        print("NO_EVENTS")
        sys.exit(1)

    event_ids = [r["event_id"] for r in rows]
    placeholders = ",".join("?" for _ in event_ids)
    conn.execute(
        f"""
        UPDATE events
        SET status = 'PROCESSING',
            next_retry_at = NULL,
            updated_at = CURRENT_TIMESTAMP
        WHERE event_id IN ({placeholders})
        """,
        event_ids,
    )
    conn.commit()
    report_reaper()

    events_data = []
    for r in rows:
        item = dict(r)
        item["status"] = "PROCESSING"
        item["next_retry_at"] = None
        full_text = item.get("text") or ""
        if payload_text_limit > 0 and len(full_text) > payload_text_limit:
            item["text"] = (
                f"{full_text[:payload_text_limit]}…［省略あり：全{len(full_text)}字中 先頭{payload_text_limit}字のみ。"
                f"全文は ./venv/bin/python agent_tools.py event-text --event-id {item['event_id']}］"
            )
            item["text_truncated"] = True
            item["text_length"] = len(full_text)
        events_data.append(item)

    # Context bundling (best-effort; never changes what was dispatched).
    contexts = {}
    if context_bundle_enabled:
        annotations = {}
        context_error = None
        try:
            if base_dir and base_dir not in sys.path:
                sys.path.insert(0, base_dir)
            import context_bundle
            contexts, annotations = context_bundle.build_batch_contexts(
                db_path, events_data, state_dir=db_path.parent
            )
        except Exception as ctx_exc:
            context_error = str(ctx_exc)[:300]
        for item in events_data:
            item.update(
                annotations.get(
                    item["event_id"],
                    {"context_ok": False, "context_gaps": ["context_build_error"]},
                )
            )
        if context_error:
            contexts = {"_error": {"complete": False, "gaps": ["context_build_error"], "error": context_error}}

    payload = {
        "dispatched_at": datetime.now(timezone.utc).isoformat(),
        "mode": mode,
        "count": len(events_data),
        "events": events_data,
        "batch_weight": batch_weight,
    }
    if contexts:
        context_path = payload_path.with_name("payload_context.json")
        try:
            context_doc = {
                "dispatched_at": payload["dispatched_at"],
                "event_ids": [e["event_id"] for e in events_data],
                "contexts": contexts,
            }
            ctx_tmp = context_path.with_suffix(".json.tmp")
            ctx_tmp.write_text(json.dumps(context_doc, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            ctx_tmp.replace(context_path)
            inline = dict(payload, contexts=contexts)
            if len(json.dumps(inline, ensure_ascii=False, separators=(",", ":")).encode("utf-8")) <= payload_inline_max_bytes:
                payload = inline
            else:
                payload["context_file"] = str(context_path)
        except Exception as ctx_write_exc:
            for item in events_data:
                item["context_ok"] = False
                item["context_gaps"] = ["context_build_error"]
            payload["context_error"] = str(ctx_write_exc)[:300]
    if deferred_count > 0:
        payload["deferred"] = {
            "count": deferred_count,
            "reason": "batch_weight_budget" if budget_stopped else "batch_max_events",
            "budget": batch_weight_budget,
            "note": f"未処理 {deferred_count} 件は PENDING のまま次回以降に回した（取りこぼしではない）",
        }

    payload_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = payload_path.with_suffix(".json.tmp")
    tmp_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    tmp_path.replace(payload_path)

    print(f"DISPATCHED:{len(events_data)}")
    sys.exit(0)
except SystemExit:
    raise
except Exception as exc:
    try:
        conn.rollback()
    except Exception:
        pass
    print(f"ERROR:{exc}", file=sys.stderr)
    sys.exit(3)
finally:
    conn.close()
PYEOF
