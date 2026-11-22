#!/usr/bin/env python3
"""
Local thread-context bundling for dispatched batches (stall fix 2026-11-22, proposal 2).

The dispatcher calls build_batch_contexts() right after it has moved a batch to
PROCESSING. For every thread in the batch it builds, from the local SQLite mirror
only (no Slack read), the same kind of context the worker would otherwise fetch
with `agent_tools.py history --thread-ts <key>`: the thread parent plus the newest
replies, in the compact history format.

Sources (merged by ts, first wins): events > mirror_messages > replies ledger;
message_edits supplies edited text / deletions.

A context is marked complete (context_ok=true on the event) only when nothing is
known to be missing. Otherwise the event gets context_ok=false with the reasons in
context_gaps, and the worker reads Slack (`history`) exactly as before:
  parent_missing                   thread parent not in the local DB
  own_post_mirror_inactive         the listener has not started mirroring own posts yet
  thread_predates_own_post_mirror  thread started before own-post mirroring and was
                                   not seeded (seed-mirror) afterwards
  listener_heartbeat_stale         listener heartbeat older than LISTENER_STALE_SECONDS
  backfill_failed / backfill_status_unknown / backfill_status_stale
  payload_context_budget           total bundled text budget exhausted
  retry_requires_api_history       (per event) retry / wait-state re-processing: the
                                   prompt requires reading the thread from Slack
  context_build_error              unexpected error (bundling is best-effort)

Safety reads (pre-post check, answered check, post-error recovery) are untouched:
they always read Slack inside `agent_tools.py reply`.
"""

import json
import os
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import agent_tools
from listener import MIRROR_SINCE_KEY, get_db_connection

CONTEXT_MESSAGE_LIMIT: int = 20
THREAD_TEXT_BUDGET: int = 15000
BATCH_TEXT_BUDGET: int = 40000
LISTENER_STALE_SECONDS: float = 180.0
BACKFILL_STATUS_MAX_AGE_SECONDS: float = 900.0
LAST_OWN_TEXT_LIMIT: int = 600


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    try:
        return int(raw) if raw else default
    except ValueError:
        return default


def _ts_key(ts: Any) -> float:
    try:
        return float(ts)
    except (TypeError, ValueError):
        return 0.0


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    return (
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
        ).fetchone()
        is not None
    )


def _column_exists(conn: sqlite3.Connection, table: str, col: str) -> bool:
    return any(r["name"] == col for r in conn.execute(f"PRAGMA table_info({table});").fetchall())


def load_thread_messages(
    conn: sqlite3.Connection,
    channel: str,
    thread_key: str,
    bot_user_id: str = agent_tools.YAMA_BOT_USER_ID,
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """All locally known messages of one thread (parent = thread_key), oldest first."""
    by_ts: Dict[str, Dict[str, Any]] = {}
    stats = {"edited": 0, "deleted": 0}

    for r in conn.execute(
        """
        SELECT user_id, text, ts, thread_ts, is_bot FROM events
        WHERE channel_id = ? AND (thread_ts = ? OR ts = ?)
        """,
        (channel, thread_key, thread_key),
    ).fetchall():
        by_ts.setdefault(
            str(r["ts"]),
            {
                "user": r["user_id"],
                "text": r["text"] or "",
                "ts": str(r["ts"]),
                "thread_ts": r["thread_ts"],
                "is_bot": int(r["is_bot"] or 0),
            },
        )

    if _table_exists(conn, "mirror_messages"):
        for r in conn.execute(
            """
            SELECT user_id, text, ts, thread_ts, is_bot FROM mirror_messages
            WHERE channel_id = ? AND (thread_ts = ? OR ts = ?)
            """,
            (channel, thread_key, thread_key),
        ).fetchall():
            by_ts.setdefault(
                str(r["ts"]),
                {
                    "user": r["user_id"] or bot_user_id,
                    "text": r["text"] or "",
                    "ts": str(r["ts"]),
                    "thread_ts": r["thread_ts"],
                    "is_bot": int(r["is_bot"] or 0),
                },
            )

    if _table_exists(conn, "replies"):
        text_col = "COALESCE(text, '')" if _column_exists(conn, "replies", "text") else "''"
        for r in conn.execute(
            f"""
            SELECT reply_ts, thread_ts, {text_col} AS text FROM replies
            WHERE channel_id = ? AND (thread_ts = ? OR reply_ts = ?)
            """,
            (channel, thread_key, thread_key),
        ).fetchall():
            by_ts.setdefault(
                str(r["reply_ts"]),
                {
                    "user": bot_user_id,
                    "text": r["text"] or "",
                    "ts": str(r["reply_ts"]),
                    "thread_ts": r["thread_ts"],
                    "is_bot": 1,
                },
            )

    edited_ts: Dict[str, bool] = {}
    if by_ts and _table_exists(conn, "message_edits"):
        placeholders = ",".join("?" for _ in by_ts)
        for r in conn.execute(
            f"SELECT ts, text, deleted FROM message_edits WHERE channel_id = ? AND ts IN ({placeholders})",
            [channel, *by_ts.keys()],
        ).fetchall():
            ts = str(r["ts"])
            if int(r["deleted"] or 0):
                by_ts.pop(ts, None)
                stats["deleted"] += 1
            elif r["text"] is not None:
                by_ts[ts]["text"] = r["text"]
                edited_ts[ts] = True
                stats["edited"] += 1

    msgs = sorted(by_ts.values(), key=lambda m: _ts_key(m["ts"]))
    for m in msgs:
        if edited_ts.get(m["ts"]):
            m["edited"] = True
    return msgs, stats


def mirror_health(
    state_dir: Path, now: Optional[float] = None
) -> List[str]:
    """Batch-wide reasons the local mirror may be behind Slack right now."""
    gaps: List[str] = []
    current = now if now is not None else time.time()
    hb_path = Path(os.environ.get("YAMA_HEARTBEAT_PATH", str(state_dir / "heartbeat.json")))
    try:
        hb = json.loads(hb_path.read_text(encoding="utf-8"))
        if current - float(hb.get("timestamp") or 0) > LISTENER_STALE_SECONDS:
            gaps.append("listener_heartbeat_stale")
    except Exception:
        gaps.append("listener_heartbeat_stale")
    try:
        st = json.loads((state_dir / "backfill_last_status.json").read_text(encoding="utf-8"))
        if not st.get("ok"):
            gaps.append("backfill_failed")
        elif current - float(st.get("at") or 0) > BACKFILL_STATUS_MAX_AGE_SECONDS:
            gaps.append("backfill_status_stale")
    except FileNotFoundError:
        gaps.append("backfill_status_unknown")
    except Exception:
        gaps.append("backfill_status_unknown")
    return gaps


def _own_post_coverage_gap(
    conn: sqlite3.Connection, channel: str, thread_key: str
) -> Optional[str]:
    if not _table_exists(conn, "mirror_meta"):
        return "own_post_mirror_inactive"
    row = conn.execute(
        "SELECT value FROM mirror_meta WHERE key = ?", (MIRROR_SINCE_KEY,)
    ).fetchone()
    if row is None:
        return "own_post_mirror_inactive"
    since = _ts_key(row["value"])
    if _ts_key(thread_key) >= since:
        return None
    if _table_exists(conn, "mirror_threads"):
        seeded = conn.execute(
            "SELECT seeded_at FROM mirror_threads WHERE channel_id = ? AND thread_ts = ?",
            (channel, thread_key),
        ).fetchone()
        if seeded is not None and _ts_key(seeded["seeded_at"]) >= since:
            return None
    return "thread_predates_own_post_mirror"


def build_thread_context(
    conn: sqlite3.Connection,
    channel: str,
    thread_key: str,
    limit: int = CONTEXT_MESSAGE_LIMIT,
    bot_user_id: str = agent_tools.YAMA_BOT_USER_ID,
) -> Dict[str, Any]:
    """Compact local context of one thread: parent + newest `limit` replies."""
    msgs, stats = load_thread_messages(conn, channel, thread_key, bot_user_id=bot_user_id)
    gaps: List[str] = []
    parent = next((m for m in msgs if str(m["ts"]) == str(thread_key)), None)
    replies = [m for m in msgs if m is not parent]
    if parent is None:
        gaps.append("parent_missing")
    cov = _own_post_coverage_gap(conn, channel, thread_key)
    if cov:
        gaps.append(cov)

    window = replies[-limit:] if limit > 0 else []
    shown: List[Dict[str, Any]] = []
    if parent is not None:
        p = dict(parent)
        p["reply_count"] = len(replies)
        shown.append(p)
    shown.extend(window)

    edited = {m["ts"] for m in msgs if m.get("edited")}
    saved_budget = agent_tools.HISTORY_TOTAL_TEXT_BUDGET
    try:
        agent_tools.HISTORY_TOTAL_TEXT_BUDGET = THREAD_TEXT_BUDGET
        ctx = agent_tools.compact_history_result(
            {
                "ok": True,
                "source": "local_db_bundle",
                "channel": channel,
                "thread_ts": thread_key,
                "messages": shown,
            }
        )
    finally:
        agent_tools.HISTORY_TOTAL_TEXT_BUDGET = saved_budget
    if parent is None and len(replies) > len(window):
        omitted = len(replies) - len(window)
        ctx["older_omitted"] = omitted
        ctx["note"] = "。".join(
            [n for n in [f"これ以前のスレッド返信 {omitted} 件は省略（直近の返信のみ表示）", ctx.get("note")] if n]
        )
    for m in ctx.get("messages") or []:
        if m.get("ts") in edited:
            m["edited"] = True
        if m.get("user") == bot_user_id:
            m["is_yama"] = True
    own = [m for m in msgs if m.get("user") == bot_user_id]
    if own:
        last = own[-1]
        ctx["last_own_message"] = {
            "ts": last["ts"],
            "text": agent_tools.truncate_text_for_worker(last.get("text"), LAST_OWN_TEXT_LIMIT)[0],
        }
    if stats["deleted"]:
        ctx["deleted_omitted"] = stats["deleted"]
    ctx["gaps"] = gaps
    ctx["complete"] = not gaps
    return ctx


def _event_requires_api(ev: Dict[str, Any], reap_count: int = 0) -> bool:
    """Retry / wait-state / reaped re-processing: read the thread from Slack
    (the prompt requires it for retries; a reaped batch may have posted already)."""
    try:
        if int(ev.get("retry_count") or 0) >= 1:
            return True
    except (TypeError, ValueError):
        return True
    if reap_count >= 1:
        return True
    return bool(ev.get("first_unverifiable_at") or ev.get("error_message"))


def _reap_count(conn: sqlite3.Connection, event_id: Any) -> int:
    try:
        row = conn.execute(
            "SELECT reap_count FROM events WHERE event_id = ?", (str(event_id),)
        ).fetchone()
        return int(row["reap_count"] or 0) if row else 0
    except Exception:
        return 1  # unknown -> treat as re-processing (read Slack)


def build_batch_contexts(
    db_path: Path,
    events: List[Dict[str, Any]],
    state_dir: Optional[Path] = None,
    now: Optional[float] = None,
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    """Returns (contexts by key, per-event annotations by event_id).
    Contexts whose thread is incomplete carry gaps only (no messages)."""
    state = Path(state_dir) if state_dir is not None else Path(db_path).parent
    limit = _env_int("YAMA_CONTEXT_BUNDLE_LIMIT", CONTEXT_MESSAGE_LIMIT)
    batch_budget = _env_int("YAMA_CONTEXT_BATCH_TEXT_BUDGET", BATCH_TEXT_BUDGET)
    global_gaps = mirror_health(state, now=now)
    contexts: Dict[str, Dict[str, Any]] = {}
    per_event: Dict[str, Dict[str, Any]] = {}
    used = 0
    with get_db_connection(db_path) as conn:
        for ev in events:
            channel = str(ev.get("channel_id") or "")
            thread_key = str(ev.get("thread_ts") or ev.get("ts") or "")
            key = f"{channel}:{thread_key}"
            if key not in contexts:
                try:
                    ctx = build_thread_context(conn, channel, thread_key, limit=limit)
                    ctx["gaps"] = list(global_gaps) + ctx["gaps"]
                    chars = int(ctx.get("text_chars") or 0)
                    if not ctx["gaps"] and used + chars > batch_budget:
                        ctx["gaps"].append("payload_context_budget")
                    if ctx["gaps"]:
                        ctx = {
                            "channel": channel,
                            "thread_ts": thread_key,
                            "complete": False,
                            "gaps": ctx["gaps"],
                        }
                    else:
                        ctx["complete"] = True
                        used += chars
                except Exception as exc:  # best-effort: never block dispatch
                    ctx = {
                        "channel": channel,
                        "thread_ts": thread_key,
                        "complete": False,
                        "gaps": ["context_build_error"],
                        "error": str(exc)[:300],
                    }
                contexts[key] = ctx
            ctx = contexts[key]
            gaps = list(ctx.get("gaps") or [])
            if _event_requires_api(ev, _reap_count(conn, ev.get("event_id"))):
                gaps.append("retry_requires_api_history")
            per_event[str(ev.get("event_id"))] = {
                "context_key": key,
                "context_ok": not gaps,
                "context_gaps": gaps,
            }
    return contexts, per_event
