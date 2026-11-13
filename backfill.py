#!/usr/bin/env python3
"""
Yama Slack Backfill Script
- Fetches recent messages (latest 30) and thread replies (up to 30 when reply_count > 0)
  for target channels via Slack Web API
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
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple

from agent_tools import THREAD_REPLIES_MAX_PAGES, _call_with_retry, get_web_client
from listener import (
    DEFAULT_DB_PATH,
    DEFAULT_ENV_PATH,
    get_db_connection,
    init_db,
    load_env_file,
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
) -> int:
    """
    Fetch history and thread replies for a single channel, filter and insert missing
    messages into SQLite, and return the number of newly inserted rows.
    """
    current_now = now if now is not None else time.time()
    max_ts, existing_other_ids_ts = _get_channel_db_baseline(channel_id, db_path)
    if max_ts is not None:
        cutoff_ts = max_ts - MAX_TS_LOOKBACK_SEC
    else:
        cutoff_ts = current_now - EMPTY_DB_LOOKBACK_SEC

    hist_resp = _call_with_retry(
        lambda: client.conversations_history(
            channel=channel_id, limit=HISTORY_LIMIT, oldest=str(cutoff_ts)
        ),
        sleep_fn=sleep_fn,
    )
    history_messages: List[Dict[str, Any]] = list(hist_resp.get("messages", []) or [])

    collected_messages: List[Dict[str, Any]] = list(history_messages)

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

                    def _do_replies_call(kw: Dict[str, Any] = replies_kwargs) -> Any:
                        try:
                            return client.conversations_replies(**kw)
                        except TypeError:
                            fallback_kw = {
                                k: v for k, v in kw.items() if k in ("channel", "ts", "limit")
                            }
                            return client.conversations_replies(**fallback_kw)

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

    inserted_count = 0
    for msg in collected_messages:
        if not isinstance(msg, dict):
            continue
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
    }

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
            try:
                ch_inserted = backfill_channel(
                    channel_id=channel_id,
                    client=active_client,
                    bot_user_id=bot_user_id,
                    db_path=db_path,
                    ignored_user_ids=ignored_user_ids,
                    now=now,
                    sleep_fn=sleep_fn,
                )
                channels_map[channel_id] = ch_inserted
                total_inserted += ch_inserted
            except Exception as ch_exc:
                print(
                    f"[backfill] Error processing channel {channel_id}: {ch_exc}",
                    file=sys.stderr,
                )

        result["inserted"] = total_inserted
    except Exception as exc:
        print(f"[backfill] Fatal error: {exc}", file=sys.stderr)

    return result


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
