#!/usr/bin/env python3
"""
Yama Slack Backfill Script
- Fetches recent messages (30 per page, up to HISTORY_MAX_PAGES=3 pages = ~90 via
  cursor paging) and thread replies (paged, when reply_count > 0) for target channels
  via Slack Web API. If more history remains after the page cap, the channel is
  marked truncated: logged to stderr and, edge-triggered (once when truncation
  starts, once when it clears), appended to the ops alert outbox
  (state/ops_alerts.jsonl) that the dispatch hook delivers to the ops chat.
- Filters messages with listener.should_ignore_event and persists missing messages
  via listener.save_event (using default channel_ts fallback event_id)
- Restricts scope to messages newer than (channel max ts in DB - 3600s),
  or last 24 hours if the channel has no events in DB
- Outputs {"inserted": N, "channels": {...}} to stdout and always exits 0
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple

from agent_tools import THREAD_REPLIES_MAX_PAGES, _call_with_retry, get_web_client
from listener import (
    DEFAULT_DB_PATH,
    DEFAULT_ENV_PATH,
    get_db_connection,
    init_db,
    load_env_file,
    record_message_edit,
    record_own_message,
    save_event,
    should_ignore_event,
)

# Target channels:
#   C_WELCOME: #slack-へようこそ
#   C_ALL: #all-muse取締役会
TARGET_CHANNELS: Tuple[str, ...] = (
    "C_WELCOME",
    "C_ALL",
)

HISTORY_LIMIT = 30
HISTORY_MAX_PAGES = 3
TRUNCATION_STATE_FILENAME = "backfill_truncation.json"
OPS_ALERTS_FILENAME = "ops_alerts.jsonl"
REPLIES_LIMIT = 30
MAX_TS_LOOKBACK_SEC = 3600.0
EMPTY_DB_LOOKBACK_SEC = 86400.0


def message_to_event(msg: Dict[str, Any], channel_id: str) -> Dict[str, Any]:
    """
    Convert a Slack Web API message object into an event dict suitable for
    listener.should_ignore_event and listener.save_event.
    Removes event_id and client_msg_id so save_event uses its default
    f"{channel_id}_{ts}" fallback as PRIMARY KEY.
    """
    event = dict(msg)
    event["type"] = msg.get("type", "message")
    event["channel"] = channel_id
    event.pop("event_id", None)
    event.pop("client_msg_id", None)
    if not event.get("user") and event.get("bot_id"):
        event["user"] = event["bot_id"]
    return event


def _get_channel_db_baseline(
    channel_id: str, db_path: Path
) -> Tuple[Optional[float], Set[str]]:
    """
    Return (max_ts, existing_non_fallback_ts_set) for channel_id in SQLite.
    - max_ts: maximum numeric ts stored for channel_id, or None if empty.
    - existing_non_fallback_ts_set: ts values already stored for channel_id under a
      Socket Mode envelope event_id (e.g. 'Ev...'), preventing duplicate insertion when
      the same message was already captured via Socket Mode.
      Messages stored with fallback event_id '{channel_id}_{ts}' are deduplicated
      directly by save_event via the event_id PRIMARY KEY.
    """
    with get_db_connection(db_path) as conn:
        row = conn.execute(
            "SELECT MAX(CAST(ts AS REAL)) AS max_ts FROM events WHERE channel_id = ?",
            (channel_id,),
        ).fetchone()
        max_ts = float(row["max_ts"]) if row and row["max_ts"] is not None else None

        fallback_prefix = f"{channel_id}_"
        existing_other_ids_ts = {
            r["ts"]
            for r in conn.execute(
                "SELECT ts, event_id FROM events WHERE channel_id = ?",
                (channel_id,),
            ).fetchall()
            if r["event_id"] != f"{fallback_prefix}{r['ts']}"
        }
    return max_ts, existing_other_ids_ts


def backfill_channel(
    channel_id: str,
    client: Any,
    bot_user_id: Optional[str],
    db_path: Path = DEFAULT_DB_PATH,
    ignored_user_ids: Optional[Set[str]] = None,
    now: Optional[float] = None,
    sleep_fn: Optional[Callable[[float], None]] = None,
    page_info: Optional[Dict[str, Any]] = None,
) -> int:
    """
    Fetch history and thread replies for a single channel, filter and insert missing
    messages into SQLite, and return the number of newly inserted rows.
    If page_info (a dict) is given, it is filled with {"pages": N, "truncated": bool,
    "oldest_fetched_ts": ...}; truncated=True means more history remained after
    HISTORY_MAX_PAGES pages.
    """
    current_now = now if now is not None else time.time()
    max_ts, existing_other_ids_ts = _get_channel_db_baseline(channel_id, db_path)
    if max_ts is not None:
        cutoff_ts = max_ts - MAX_TS_LOOKBACK_SEC
    else:
        cutoff_ts = current_now - EMPTY_DB_LOOKBACK_SEC

    history_messages: List[Dict[str, Any]] = []
    hist_cursor: Optional[str] = None
    pages_read = 0
    truncated = False
    for hist_page in range(1, HISTORY_MAX_PAGES + 1):
        hist_kwargs: Dict[str, Any] = {
            "channel": channel_id,
            "limit": HISTORY_LIMIT,
            "oldest": str(cutoff_ts),
        }
        if hist_cursor:
            hist_kwargs["cursor"] = hist_cursor
        hist_resp = _call_with_retry(
            lambda kw=hist_kwargs: client.conversations_history(**kw),
            sleep_fn=sleep_fn,
        )
        pages_read = hist_page
        history_messages.extend(list(hist_resp.get("messages", []) or []))
        hist_meta = hist_resp.get("response_metadata") or {}
        hist_next = (
            (hist_meta.get("next_cursor") or "").strip()
            if isinstance(hist_meta, dict)
            else ""
        )
        if not hist_next or hist_resp.get("has_more") is False:
            break
        if hist_page >= HISTORY_MAX_PAGES:
            truncated = True
            break
        hist_cursor = hist_next
    if page_info is not None:
        oldest_fetched: Optional[str] = None
        for m in history_messages:
            mts = m.get("ts") if isinstance(m, dict) else None
            try:
                if mts and (oldest_fetched is None or float(mts) < float(oldest_fetched)):
                    oldest_fetched = str(mts)
            except (TypeError, ValueError):
                continue
        page_info.update(
            {
                "pages": pages_read,
                "truncated": truncated,
                "oldest_fetched_ts": oldest_fetched,
                "cutoff_ts": f"{cutoff_ts:.6f}",
            }
        )

    collected_messages: List[Dict[str, Any]] = list(history_messages)
    reply_errors = 0

    for msg in history_messages:
        try:
            reply_count = int(msg.get("reply_count", 0) or 0)
        except (TypeError, ValueError):
            reply_count = 0
        parent_ts = msg.get("ts")
        if reply_count > 0 and parent_ts:
            try:
                cursor: Optional[str] = None
                for _page in range(1, THREAD_REPLIES_MAX_PAGES + 1):
                    replies_kwargs: Dict[str, Any] = {
                        "channel": channel_id,
                        "ts": parent_ts,
                        "oldest": str(cutoff_ts),
                        "limit": REPLIES_LIMIT,
                    }
                    if cursor:
                        replies_kwargs["cursor"] = cursor

                    # Review fix 2026-11-25 [mid-6] (candidate 29(b)): no TypeError
                    # fallback that silently drops oldest/cursor (it re-read page 1 up
                    # to THREAD_REPLIES_MAX_PAGES times and hid unrelated TypeErrors);
                    # same as agent_tools since candidate 28.
                    def _do_replies_call(kw: Dict[str, Any] = replies_kwargs) -> Any:
                        return client.conversations_replies(**kw)

                    replies_resp = _call_with_retry(
                        _do_replies_call,
                        sleep_fn=sleep_fn,
                    )
                    replies = list(replies_resp.get("messages", []) or [])
                    collected_messages.extend(replies)

                    meta = replies_resp.get("response_metadata") or {}
                    next_cursor = (
                        (meta.get("next_cursor") or "").strip()
                        if isinstance(meta, dict)
                        else ""
                    )
                    has_more = replies_resp.get("has_more")
                    if not next_cursor or has_more is False:
                        break
                    cursor = next_cursor
            except Exception as exc:
                reply_errors += 1
                print(
                    f"[backfill] Failed conversations_replies for channel={channel_id} ts={parent_ts}: {exc}",
                    file=sys.stderr,
                )

    def _ts_sort_key(m: Dict[str, Any]) -> float:
        try:
            return float(m.get("ts", 0))
        except (TypeError, ValueError):
            return 0.0

    collected_messages.sort(key=_ts_sort_key)
    if page_info is not None and reply_errors:
        page_info["reply_errors"] = reply_errors

    inserted_count = 0
    for msg in collected_messages:
        if not isinstance(msg, dict):
            continue
        _mirror_backfilled_message(msg, channel_id, bot_user_id, db_path)
        event = message_to_event(msg, channel_id)
        ignore, _reason = should_ignore_event(
            event,
            bot_user_id=bot_user_id,
            ignored_user_ids=ignored_user_ids,
        )
        if ignore:
            continue

        try:
            msg_ts = float(event["ts"])
        except (KeyError, TypeError, ValueError):
            continue

        if msg_ts <= cutoff_ts:
            continue

        if event["ts"] in existing_other_ids_ts:
            continue

        if save_event(event, db_path=db_path):
            inserted_count += 1

    return inserted_count


def _mirror_backfilled_message(
    msg: Dict[str, Any], channel_id: str, bot_user_id: Optional[str], db_path: Path
) -> None:
    """Keep the local thread mirror (mirror_messages / message_edits) complete for
    what backfill reads anyway: Yama's own posts (missed while the listener was
    down) and the current text of edited messages. Mirror only; never an event.
    Best-effort: a failure here never affects backfill."""
    try:
        ts = msg.get("ts")
        if not ts:
            return
        if bot_user_id and msg.get("user") == bot_user_id:
            record_own_message(
                channel_id, ts, msg.get("thread_ts"), msg.get("text"),
                user_id=bot_user_id, source="backfill", db_path=db_path,
            )
        if msg.get("edited"):
            record_message_edit(channel_id, ts, msg.get("text"), db_path=db_path)
    except Exception as exc:
        print(f"[backfill] mirror update failed (ignored): {exc}", file=sys.stderr)


def _ops_alerts_path(db_path: Path) -> Path:
    env_path = os.environ.get("YAMA_OPS_ALERTS_PATH", "").strip()
    return Path(env_path) if env_path else Path(db_path).parent / OPS_ALERTS_FILENAME


def _append_ops_alert(entry: Dict[str, Any], path: Path) -> None:
    import fcntl

    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(entry, ensure_ascii=False, sort_keys=True) + "\n"
    with open(path, "a", encoding="utf-8") as f:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        try:
            f.write(line)
            f.flush()
            os.fsync(f.fileno())
        finally:
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)


def update_truncation_alerts(
    page_infos: Dict[str, Dict[str, Any]],
    db_path: Path,
) -> List[Dict[str, Any]]:
    """Edge-triggered truncation alerts: append one outbox entry when a channel
    starts being truncated and one when it stops. Returns the entries written.
    The state file is only updated after the outbox write succeeds, so a failed
    write is retried on the next run."""
    state_path = Path(db_path).parent / TRUNCATION_STATE_FILENAME
    try:
        state = json.loads(state_path.read_text(encoding="utf-8"))
        if not isinstance(state, dict):
            state = {}
    except Exception:
        state = {}
    written: List[Dict[str, Any]] = []
    outbox = _ops_alerts_path(db_path)
    changed = False
    now_iso = datetime.now(timezone.utc).isoformat()
    for channel_id, info in page_infos.items():
        was = bool(state.get(channel_id, {}).get("truncated"))
        is_now = bool(info.get("truncated"))
        if is_now == was:
            continue
        if is_now:
            entry = {
                "kind": "backfill_truncated",
                "at": now_iso,
                "channel": channel_id,
                "pages": info.get("pages"),
                "max_pages": HISTORY_MAX_PAGES,
                "per_page": HISTORY_LIMIT,
                "oldest_fetched_ts": info.get("oldest_fetched_ts"),
                "cutoff_ts": info.get("cutoff_ts"),
            }
            print(
                f"[backfill] WARNING: history page cap reached for channel={channel_id} "
                f"({HISTORY_MAX_PAGES} pages x {HISTORY_LIMIT}); older posts in the window were not read",
                file=sys.stderr,
            )
        else:
            entry = {"kind": "backfill_truncation_recovered", "at": now_iso, "channel": channel_id}
            print(f"[backfill] history page cap cleared for channel={channel_id}", file=sys.stderr)
        try:
            _append_ops_alert(entry, outbox)
        except Exception as exc:
            print(
                f"[backfill] ERROR: could not write ops alert ({entry['kind']} channel={channel_id}): {exc}",
                file=sys.stderr,
            )
            continue
        written.append(entry)
        state[channel_id] = {"truncated": is_now, "changed_at": now_iso}
        changed = True
    if changed:
        tmp = state_path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        tmp.replace(state_path)
    return written


def run_backfill(
    db_path: Path = DEFAULT_DB_PATH,
    env_path: Path = DEFAULT_ENV_PATH,
    channels: Sequence[str] = TARGET_CHANNELS,
    client: Optional[Any] = None,
    now: Optional[float] = None,
    sleep_fn: Optional[Callable[[float], None]] = None,
) -> Dict[str, Any]:
    """
    Execute backfill across all target channels.
    Never raises: logs any errors to stderr and returns {"inserted": total, "channels": {...}}.
    """
    db_path = Path(db_path)
    env_path = Path(env_path)
    channels_map: Dict[str, int] = {ch: 0 for ch in channels}
    result: Dict[str, Any] = {
        "inserted": 0,
        "channels": channels_map,
        "truncated": [],
    }
    page_infos: Dict[str, Dict[str, Any]] = {}
    errors: List[str] = []

    try:
        load_env_file(env_path)
        init_db(db_path)

        active_client = client if client is not None else get_web_client(env_path)
        auth_resp = _call_with_retry(
            lambda: active_client.auth_test(),
            sleep_fn=sleep_fn,
        )
        bot_user_id = auth_resp.get("user_id") if auth_resp else None

        extra_ignored_str = os.environ.get("IGNORED_USER_IDS", "")
        ignored_user_ids: Set[str] = {
            u.strip() for u in extra_ignored_str.split(",") if u.strip()
        }

        total_inserted = 0
        for channel_id in channels:
            info: Dict[str, Any] = {}
            try:
                ch_inserted = backfill_channel(
                    channel_id=channel_id,
                    client=active_client,
                    bot_user_id=bot_user_id,
                    db_path=db_path,
                    ignored_user_ids=ignored_user_ids,
                    now=now,
                    sleep_fn=sleep_fn,
                    page_info=info,
                )
                page_infos[channel_id] = info
                if info.get("truncated"):
                    result["truncated"].append(channel_id)
                channels_map[channel_id] = ch_inserted
                total_inserted += ch_inserted
            except Exception as ch_exc:
                errors.append(f"channel {channel_id}: {ch_exc}")
                print(
                    f"[backfill] Error processing channel {channel_id}: {ch_exc}",
                    file=sys.stderr,
                )

        result["inserted"] = total_inserted
        try:
            update_truncation_alerts(page_infos, db_path)
        except Exception as trunc_exc:
            print(f"[backfill] ERROR: truncation alert update failed: {trunc_exc}", file=sys.stderr)
    except Exception as exc:
        errors.append(f"fatal: {exc}")
        print(f"[backfill] Fatal error: {exc}", file=sys.stderr)

    for ch_id, info in page_infos.items():
        if info.get("reply_errors"):
            errors.append(f"channel {ch_id}: {info['reply_errors']} thread read error(s)")
    write_backfill_status(db_path, ok=not errors, errors=errors)
    return result


BACKFILL_STATUS_FILENAME = "backfill_last_status.json"


def write_backfill_status(db_path: Path, ok: bool, errors: List[str]) -> None:
    """Record the outcome of the last backfill run (state/backfill_last_status.json).
    The dispatcher's context bundling reads it: if the last backfill failed, posts
    missed by the listener may be absent locally, so bundled context is marked
    incomplete and the worker reads Slack instead. Best-effort, atomic replace."""
    try:
        path = Path(db_path).parent / BACKFILL_STATUS_FILENAME
        tmp = path.with_suffix(".json.tmp")
        tmp.write_text(
            json.dumps(
                {"at": time.time(), "ok": bool(ok), "errors": [str(e)[:300] for e in errors[:10]]},
                ensure_ascii=False,
            )
            + "\n",
            encoding="utf-8",
        )
        tmp.replace(path)
    except Exception as exc:
        print(f"[backfill] could not write status file (ignored): {exc}", file=sys.stderr)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="backfill.py",
        description="Backfill missed Slack messages into SQLite events table",
    )
    parser.add_argument(
        "--db",
        default=os.environ.get("YAMA_DB_PATH", str(DEFAULT_DB_PATH)),
        help="Path to SQLite database (default: state/slack_events.db)",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    try:
        parser = build_parser()
        args = parser.parse_args(argv)
        summary = run_backfill(db_path=Path(args.db))
    except BaseException as exc:
        print(f"[backfill] Unexpected error: {exc}", file=sys.stderr)
        summary = {
            "inserted": 0,
            "channels": {ch: 0 for ch in TARGET_CHANNELS},
        }

    print(json.dumps(summary, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
