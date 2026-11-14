#!/usr/bin/env python3
"""
Yama Agent Tools CLI (Layer 3)
Subcommands for the awakened autonomous agent:
  - pending
  - history --channel X [--thread-ts Y] [--limit N]
  - reply --channel X --text-file F [--thread-ts Y] [--event-id E]
  - record-reply --event-id E --channel X --reply-ts T --text-file F [--thread-ts Y]
  - reconcile-replies [--days 7] [--dry-run]
  - prune-replies [--days 90] [--dry-run]
  - react --channel X --ts T --emoji eyes
  - commit --event-id E --status DONE|IGNORED|FAILED [--error MSG]
  - mode
"""

import argparse
import hashlib
import http.client
import json
import os
import re
import socket
import sys
import time
import urllib.error
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from zoneinfo import ZoneInfo

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

from listener import (
    DEFAULT_DB_PATH,
    DEFAULT_ENV_PATH,
    get_db_connection,
    init_db as _listener_init_db,
    load_env_file,
)

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_MODE_PATH = BASE_DIR / "config" / "mode.txt"
VALID_MODES = ("shadow", "mention", "full")
VALID_COMMIT_STATUSES = ("DONE", "IGNORED", "FAILED")
MAX_RETRIES = 3

TRANSIENT_CONNECTION_ERRORS: Tuple[type, ...] = (
    urllib.error.URLError,
    http.client.HTTPException,
    http.client.IncompleteRead,
    http.client.RemoteDisconnected,
    ConnectionError,
    TimeoutError,
    socket.timeout,
    OSError,
)
TRANSIENT_SLACK_ERROR_CODES: Set[str] = {
    "ratelimited",
    "rate_limited",
    "internal_error",
    "service_unavailable",
    "request_timeout",
    "fatal_error",
}
HISTORY_RETRY_MAX_ATTEMPTS: int = 5
HISTORY_RETRY_BACKOFFS: Tuple[float, ...] = (1.0, 2.0, 4.0, 8.0)
CHANNEL_HISTORY_BOUND_LIMIT: int = 50
THREAD_REPLIES_PAGE_LIMIT: int = 50
THREAD_REPLIES_MAX_PAGES: int = 20
THREAD_SCAN_MARGIN_SECONDS: int = 60
RECOVERY_WINDOW_BEFORE_SECONDS: float = 180.0
RECOVERY_WINDOW_AFTER_SECONDS: float = 300.0
REPLY_METADATA_EVENT_TYPE: str = "yama_reply"
DEFAULT_PRUNE_DAYS: int = 90
DEFAULT_RECONCILE_DAYS: int = 7
YAMA_BOT_USER_ID: str = "U_YAMA_BOT"
UNVERIFIABLE_ERROR_CODE: str = "pre_post_check_unverifiable"
# Wait 1 hour (3600s) before re-dispatching an event that failed with pre_post_check_unverifiable,
# so the 30-second dispatcher loop does not immediately re-wake the agent during a Slack API outage.
UNVERIFIABLE_RETRY_WAIT_SECONDS: int = 3600
# Give up and transition to terminal FAILED if history remains unreadable 24 hours (86400s)
# after the first pre_post_check_unverifiable failure.
UNVERIFIABLE_EXPIRE_SECONDS: int = 86400

try:
    ASIA_TOKYO_TZ: Any = ZoneInfo("Asia/Tokyo")
except Exception:
    ASIA_TOKYO_TZ = timezone(timedelta(hours=9))

DELAYED_RETRY_PREFIX_RE = re.compile(
    r"^(?:"
    r"遅れての再送（元の失敗時刻：\d{4}-\d{2}-\d{2} \d{2}:\d{2} JST）"
    r"|"
    r"（遅れての再送：最初の試行 \d{2}-\d{2} \d{2}:\d{2}）"
    r")(?:\r?\n|$)"
)
SKIPPED_ANSWERED_RESULT: str = "skipped_answered"
ANSWERED_MIN_TEXT_LENGTH: int = 10


def init_db(db_path: Path = DEFAULT_DB_PATH) -> None:
    """Initialize SQLite schema via listener.init_db and idempotently ensure
    events.intended_text and events.attempt_ts columns exist."""
    db_path = Path(db_path)
    _listener_init_db(db_path)
    with get_db_connection(db_path) as conn:
        cols = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(events);").fetchall()
        }
        if "intended_text" not in cols:
            conn.execute("ALTER TABLE events ADD COLUMN intended_text TEXT;")
        if "attempt_ts" not in cols:
            conn.execute("ALTER TABLE events ADD COLUMN attempt_ts TEXT;")
        conn.commit()


def get_web_client(env_path: Path = DEFAULT_ENV_PATH, no_retry: bool = False) -> WebClient:
    """Load SLACK_BOT_TOKEN from config/.env (or environment) and return a Slack WebClient."""
    load_env_file(env_path)
    bot_token = os.environ.get("SLACK_BOT_TOKEN", "").strip()
    if not bot_token:
        raise RuntimeError(f"SLACK_BOT_TOKEN is not set in environment or {env_path}")
    if no_retry:
        # Never auto-retry a POST whose outcome is unknown: a lost response
        # combined with an automatic retry posts the message twice.
        return WebClient(token=bot_token, retry_handlers=[])
    return WebClient(token=bot_token)


def get_processing_events(db_path: Path = DEFAULT_DB_PATH) -> List[Dict[str, Any]]:
    """Return all events currently in status='PROCESSING'."""
    db_path = Path(db_path)
    if not db_path.exists():
        return []
    init_db(db_path)
    with get_db_connection(db_path) as conn:
        rows = conn.execute(
            """
            SELECT event_id, channel_id, user_id, text, ts, thread_ts, is_bot,
                    status, retry_count, error_message, next_retry_at, first_unverifiable_at,
                    intended_text, attempt_ts, created_at, updated_at
            FROM events
            WHERE status = 'PROCESSING'
            ORDER BY CAST(ts AS REAL) ASC, created_at ASC, rowid ASC
            """
        ).fetchall()
        return [dict(r) for r in rows]


def commit_event(
    event_id: str,
    status: str,
    error_message: Optional[str] = None,
    db_path: Path = DEFAULT_DB_PATH,
    max_retries: int = MAX_RETRIES,
    now: Optional[Any] = None,
    retry_wait_seconds: Optional[int] = None,
    expire_seconds: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Commit the final or retry state of an event:
      - DONE / IGNORED: transitions status directly to DONE or IGNORED and clears wait timestamps.
      - FAILED (with pre_post_check_unverifiable): treated as an API wait rather than a post failure.
        Does NOT consume retry_count. Instead, keeps status='PENDING' and sets next_retry_at in the
        future (default 1 hour) so the existing 30-second dispatcher naturally skips it until the wait
        elapses and then re-dispatches it to the agent without needing a separate state-transition daemon.
        If >= 24 hours have elapsed since first_unverifiable_at and history is still unreadable,
        transitions permanently to FAILED (abandoned).
      - FAILED (other errors): increments retry_count; if retry_count < max_retries (3),
        reverts status to PENDING for retry; if retry_count >= max_retries (3),
        transitions status permanently to FAILED (DLQ).
    """
    status = status.upper()
    if status not in VALID_COMMIT_STATUSES:
        raise ValueError(f"Invalid status '{status}'. Must be one of {VALID_COMMIT_STATUSES}")

    if now is None:
        now_dt = datetime.now(timezone.utc)
    elif isinstance(now, (int, float)):
        now_dt = datetime.fromtimestamp(float(now), tz=timezone.utc)
    elif isinstance(now, datetime):
        now_dt = (
            now.replace(tzinfo=timezone.utc)
            if now.tzinfo is None
            else now.astimezone(timezone.utc)
        )
    elif isinstance(now, str):
        parsed_now = _parse_reply_created_at(now)
        if parsed_now is None:
            raise ValueError(f"Could not parse now timestamp: {now}")
        now_dt = parsed_now
    else:
        raise TypeError(f"Unsupported now type: {type(now)}")

    active_wait_sec = (
        int(retry_wait_seconds)
        if retry_wait_seconds is not None
        else int(os.environ.get("YAMA_UNVERIFIABLE_RETRY_WAIT_SEC", UNVERIFIABLE_RETRY_WAIT_SECONDS))
    )
    active_expire_sec = (
        int(expire_seconds)
        if expire_seconds is not None
        else int(os.environ.get("YAMA_UNVERIFIABLE_EXPIRE_SEC", UNVERIFIABLE_EXPIRE_SECONDS))
    )

    init_db(db_path)
    with get_db_connection(db_path) as conn:
        conn.execute("BEGIN IMMEDIATE;")
        row = conn.execute(
            """
            SELECT event_id, status, retry_count, error_message,
                   next_retry_at, first_unverifiable_at, created_at, updated_at
            FROM events
            WHERE event_id = ?
            """,
            (event_id,),
        ).fetchone()
        if not row:
            conn.rollback()
            raise LookupError(f"Event '{event_id}' not found in database")

        current_retries = int(row["retry_count"] or 0)

        if status == "FAILED":
            is_unverifiable = bool(
                error_message and UNVERIFIABLE_ERROR_CODE in error_message.lower()
            )
            if is_unverifiable:
                new_retries = current_retries
                first_dt = _parse_reply_created_at(row["first_unverifiable_at"])
                if (
                    first_dt is None
                    and row["error_message"]
                    and UNVERIFIABLE_ERROR_CODE in str(row["error_message"]).lower()
                ):
                    first_dt = _parse_reply_created_at(row["created_at"]) or _parse_reply_created_at(
                        row["updated_at"]
                    )
                if first_dt is None:
                    first_dt = now_dt
                first_unverifiable_str = first_dt.strftime("%Y-%m-%d %H:%M:%S")

                elapsed_sec = (now_dt - first_dt).total_seconds()
                if elapsed_sec >= active_expire_sec:
                    final_status = "FAILED"
                    next_retry_str: Optional[str] = None
                    if (
                        "abandoned" not in error_message.lower()
                        and "expired" not in error_message.lower()
                    ):
                        final_error = f"{error_message} (abandoned after 24h)"
                    else:
                        final_error = error_message
                else:
                    final_status = "PENDING"
                    next_retry_dt = now_dt + timedelta(seconds=active_wait_sec)
                    next_retry_str = next_retry_dt.strftime("%Y-%m-%d %H:%M:%S")
                    final_error = error_message

                conn.execute(
                    """
                    UPDATE events
                    SET status = ?,
                        retry_count = ?,
                        error_message = ?,
                        next_retry_at = ?,
                        first_unverifiable_at = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE event_id = ?
                    """,
                    (
                        final_status,
                        new_retries,
                        final_error,
                        next_retry_str,
                        first_unverifiable_str,
                        event_id,
                    ),
                )
            else:
                new_retries = current_retries + 1
                if new_retries < max_retries:
                    final_status = "PENDING"
                else:
                    final_status = "FAILED"
                conn.execute(
                    """
                    UPDATE events
                    SET status = ?,
                        retry_count = ?,
                        error_message = ?,
                        next_retry_at = NULL,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE event_id = ?
                    """,
                    (final_status, new_retries, error_message, event_id),
                )
        else:
            # Both DONE and IGNORED are terminal states: clear any stale error_message
            # from previous retry attempts unless an explicit error_message was provided.
            new_retries = current_retries
            final_status = status
            conn.execute(
                """
                UPDATE events
                SET status = ?,
                    error_message = ?,
                    next_retry_at = NULL,
                    first_unverifiable_at = NULL,
                    updated_at = CURRENT_TIMESTAMP
                WHERE event_id = ?
                """,
                (final_status, error_message, event_id),
            )
        conn.commit()

        updated = conn.execute(
            """
            SELECT event_id, channel_id, user_id, text, ts, thread_ts, is_bot,
                   status, retry_count, error_message, next_retry_at, first_unverifiable_at,
                   intended_text, attempt_ts, created_at, updated_at
            FROM events
            WHERE event_id = ?
            """,
            (event_id,),
        ).fetchone()
        return dict(updated)


def get_mode(mode_path: Path = DEFAULT_MODE_PATH) -> str:
    """Read current operation mode from config/mode.txt (shadow / mention / full)."""
    mode_path = Path(mode_path)
    if not mode_path.exists():
        return "shadow"
    content = mode_path.read_text(encoding="utf-8").strip().lower()
    return content if content else "shadow"


def cmd_pending(args: argparse.Namespace) -> int:
    events = get_processing_events(Path(args.db))
    print(json.dumps(events, ensure_ascii=False, indent=2))
    return 0


def _is_transient_slack_api_error(exc: SlackApiError) -> bool:
    """Return True if a SlackApiError represents a transient condition (HTTP 429 / 5xx
    or transient error code such as ratelimited, internal_error, service_unavailable, request_timeout)."""
    resp = getattr(exc, "response", None)
    if resp is not None:
        status_code = getattr(resp, "status_code", None)
        if status_code is None and isinstance(resp, dict):
            status_code = resp.get("status_code")
        if status_code is not None:
            try:
                code_int = int(status_code)
                if code_int == 429 or 500 <= code_int < 600:
                    return True
            except (TypeError, ValueError):
                pass

        err_code = resp.get("error") if hasattr(resp, "get") else None
        if isinstance(err_code, str) and err_code.strip().lower() in TRANSIENT_SLACK_ERROR_CODES:
            return True
    return False


def _get_retry_after_seconds(exc: SlackApiError) -> Optional[float]:
    """Extract non-negative Retry-After seconds from a SlackApiError response if present."""
    resp = getattr(exc, "response", None)
    if resp is None:
        return None
    headers = getattr(resp, "headers", None)
    if headers is None and isinstance(resp, dict):
        headers = resp.get("headers")
    if hasattr(headers, "items"):
        for k, v in headers.items():
            if str(k).strip().lower() == "retry-after" and v is not None:
                try:
                    val = float(v)
                    if val >= 0:
                        return val
                except (TypeError, ValueError):
                    pass
    if isinstance(resp, dict):
        for k in ("Retry-After", "retry-after", "retry_after"):
            if k in resp and resp[k] is not None:
                try:
                    val = float(resp[k])
                    if val >= 0:
                        return val
                except (TypeError, ValueError):
                    pass
    return None


def _call_with_retry(
    func: Callable[[], Any],
    max_attempts: int = HISTORY_RETRY_MAX_ATTEMPTS,
    backoffs: Tuple[float, ...] = HISTORY_RETRY_BACKOFFS,
    sleep_fn: Optional[Callable[[float], None]] = None,
) -> Any:
    """Call a read-only Slack API function with retry on transient connection errors
    and transient SlackApiError (HTTP 429 / 5xx, ratelimited, internal_error,
    service_unavailable, request_timeout). Respects Retry-After on 429.
    Permanent SlackApiError (e.g. channel_not_found) is raised immediately."""
    active_sleep = sleep_fn if sleep_fn is not None else time.sleep
    last_exc: Optional[BaseException] = None
    for attempt in range(max_attempts):
        try:
            return func()
        except SlackApiError as exc:
            if not _is_transient_slack_api_error(exc):
                raise
            last_exc = exc
            if attempt + 1 < max_attempts:
                retry_after = _get_retry_after_seconds(exc)
                if retry_after is not None:
                    delay = retry_after
                else:
                    delay = backoffs[attempt] if attempt < len(backoffs) else backoffs[-1]
                active_sleep(delay)
        except TRANSIENT_CONNECTION_ERRORS as exc:
            last_exc = exc
            if attempt + 1 < max_attempts:
                delay = backoffs[attempt] if attempt < len(backoffs) else backoffs[-1]
                active_sleep(delay)
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("Unreachable retry state")


def get_local_history_fallback(
    channel: str,
    thread_ts: Optional[str] = None,
    limit: int = 20,
    db_path: Path = DEFAULT_DB_PATH,
    bot_user_id: str = YAMA_BOT_USER_ID,
) -> List[Dict[str, Any]]:
    """Fetch history for channel (or thread) from local SQLite events and replies tables in chronological order."""
    db_path = Path(db_path)
    if not db_path.exists():
        return []
    _ensure_replies_table(db_path)
    with get_db_connection(db_path) as conn:
        if thread_ts:
            rows = conn.execute(
                """
                SELECT user_id, text, ts, thread_ts, is_bot
                FROM (
                    SELECT user_id, text, ts, thread_ts, is_bot, created_at, sort_id
                    FROM (
                        SELECT user_id, text, ts, thread_ts, is_bot, created_at, rowid AS sort_id
                        FROM events
                        WHERE channel_id = ? AND (thread_ts = ? OR ts = ?)
                        UNION ALL
                        SELECT ? AS user_id,
                               COALESCE(text, '') AS text,
                               reply_ts AS ts,
                               thread_ts,
                               1 AS is_bot,
                               created_at,
                               MIN(rowid) AS sort_id
                        FROM replies
                        WHERE channel_id = ? AND (thread_ts = ? OR reply_ts = ?)
                          AND NOT EXISTS (
                              SELECT 1 FROM events e
                              WHERE e.channel_id = replies.channel_id AND e.ts = replies.reply_ts
                          )
                        GROUP BY reply_ts
                    )
                    ORDER BY CAST(ts AS REAL) DESC, created_at DESC, sort_id DESC
                    LIMIT ?
                )
                ORDER BY CAST(ts AS REAL) ASC, created_at ASC, sort_id ASC
                """,
                (channel, thread_ts, thread_ts, bot_user_id, channel, thread_ts, thread_ts, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT user_id, text, ts, thread_ts, is_bot
                FROM (
                    SELECT user_id, text, ts, thread_ts, is_bot, created_at, sort_id
                    FROM (
                        SELECT user_id, text, ts, thread_ts, is_bot, created_at, rowid AS sort_id
                        FROM events
                        WHERE channel_id = ?
                        UNION ALL
                        SELECT ? AS user_id,
                               COALESCE(text, '') AS text,
                               reply_ts AS ts,
                               thread_ts,
                               1 AS is_bot,
                               created_at,
                               MIN(rowid) AS sort_id
                        FROM replies
                        WHERE channel_id = ?
                          AND NOT EXISTS (
                              SELECT 1 FROM events e
                              WHERE e.channel_id = replies.channel_id AND e.ts = replies.reply_ts
                          )
                        GROUP BY reply_ts
                    )
                    ORDER BY CAST(ts AS REAL) DESC, created_at DESC, sort_id DESC
                    LIMIT ?
                )
                ORDER BY CAST(ts AS REAL) ASC, created_at ASC, sort_id ASC
                """,
                (channel, bot_user_id, channel, limit),
            ).fetchall()

    messages: List[Dict[str, Any]] = []
    for r in rows:
        messages.append(
            {
                "user": r["user_id"],
                "text": r["text"],
                "ts": r["ts"],
                "thread_ts": r["thread_ts"],
                "is_bot": int(r["is_bot"] or 0),
            }
        )
    return messages


def fetch_history(
    channel: str,
    thread_ts: Optional[str] = None,
    limit: int = 20,
    db_path: Path = DEFAULT_DB_PATH,
    client: Optional[Any] = None,
    sleep_fn: Optional[Callable[[float], None]] = None,
) -> Dict[str, Any]:
    """Fetch conversation history from Slack API with up to 5 attempts on connection errors,
    falling back to local SQLite database if all attempts fail with connection errors."""
    try:
        active_client = client if client is not None else get_web_client()

        def _do_call() -> Any:
            if thread_ts:
                try:
                    return active_client.conversations_replies(
                        channel=channel,
                        ts=thread_ts,
                        limit=limit,
                        include_all_metadata=True,
                    )
                except TypeError:
                    return active_client.conversations_replies(
                        channel=channel,
                        ts=thread_ts,
                        limit=limit,
                    )
            try:
                return active_client.conversations_history(
                    channel=channel,
                    limit=limit,
                    include_all_metadata=True,
                )
            except TypeError:
                return active_client.conversations_history(
                    channel=channel,
                    limit=limit,
                )

        resp = _call_with_retry(
            _do_call,
            max_attempts=HISTORY_RETRY_MAX_ATTEMPTS,
            backoffs=HISTORY_RETRY_BACKOFFS,
            sleep_fn=sleep_fn,
        )
        messages = resp.get("messages", [])
        return {
            "ok": True,
            "source": "slack_api",
            "channel": channel,
            "thread_ts": thread_ts,
            "count": len(messages),
            "messages": messages,
        }
    except SlackApiError as exc:
        if not _is_transient_slack_api_error(exc):
            raise
        messages = get_local_history_fallback(
            channel=channel,
            thread_ts=thread_ts,
            limit=limit,
            db_path=db_path,
        )
        return {
            "ok": True,
            "source": "local_db_fallback",
            "channel": channel,
            "thread_ts": thread_ts,
            "count": len(messages),
            "messages": messages,
        }
    except TRANSIENT_CONNECTION_ERRORS:
        messages = get_local_history_fallback(
            channel=channel,
            thread_ts=thread_ts,
            limit=limit,
            db_path=db_path,
        )
        return {
            "ok": True,
            "source": "local_db_fallback",
            "channel": channel,
            "thread_ts": thread_ts,
            "count": len(messages),
            "messages": messages,
        }


def cmd_history(args: argparse.Namespace) -> int:
    try:
        output = fetch_history(
            channel=args.channel,
            thread_ts=args.thread_ts,
            limit=args.limit,
            db_path=Path(args.db),
        )
        print(json.dumps(output, ensure_ascii=False, indent=2))
        return 0
    except SlackApiError as exc:
        err_out = {
            "ok": False,
            "error": exc.response.get("error", str(exc)),
            "channel": args.channel,
        }
        print(json.dumps(err_out, ensure_ascii=False, indent=2), file=sys.stderr)
        return 1
    except Exception as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 1


def _ensure_replies_table(db_path: Path = DEFAULT_DB_PATH) -> None:
    """Ensure the replies table exists in SQLite for idempotent reply deduplication."""
    init_db(db_path)
    with get_db_connection(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS replies (
                event_id TEXT PRIMARY KEY,
                channel_id TEXT NOT NULL,
                thread_ts TEXT,
                reply_ts TEXT NOT NULL,
                text_hash TEXT NOT NULL,
                text TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        cols = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(replies);").fetchall()
        }
        if "text" not in cols:
            conn.execute("ALTER TABLE replies ADD COLUMN text TEXT;")
        conn.commit()


def get_recorded_reply(
    event_id: str,
    db_path: Path = DEFAULT_DB_PATH,
) -> Optional[Dict[str, Any]]:
    """Look up a previously recorded reply by event_id."""
    db_path = Path(db_path)
    if not db_path.exists():
        return None
    _ensure_replies_table(db_path)
    with get_db_connection(db_path) as conn:
        row = conn.execute(
            """
            SELECT event_id, channel_id, thread_ts, reply_ts, text_hash, created_at
            FROM replies
            WHERE event_id = ?
            """,
            (event_id,),
        ).fetchone()
        return dict(row) if row else None


def _find_own_ledger_reply(
    channel: str,
    text: str,
    thread_ts: Optional[str],
    db_path: Path = DEFAULT_DB_PATH,
) -> Optional[str]:
    """Look up Yama's past reply in the same thread (thread_ts) from the local replies ledger
    using normalized exact text match (_normalize_reconcile_text).
    Channel direct posts (thread_ts is None/empty) and rows without recorded text are excluded."""
    if not thread_ts or not str(thread_ts).strip():
        return None
    norm_target = _normalize_reconcile_text(text)
    if not norm_target:
        return None
    db_path = Path(db_path)
    if not db_path.exists():
        return None
    _ensure_replies_table(db_path)
    with get_db_connection(db_path) as conn:
        rows = conn.execute(
            """
            SELECT reply_ts, text
            FROM replies
            WHERE channel_id = ? AND thread_ts = ? AND text IS NOT NULL
            ORDER BY CAST(reply_ts AS REAL) ASC, created_at ASC, rowid ASC
            """,
            (channel, str(thread_ts)),
        ).fetchall()
    for row in rows:
        raw_row_text = row["text"]
        if raw_row_text is None:
            continue
        norm_row_text = _normalize_reconcile_text(raw_row_text)
        if not norm_row_text:
            continue
        if norm_row_text == norm_target:
            return str(row["reply_ts"])
    return None


def record_reply(
    event_id: str,
    channel_id: str,
    thread_ts: Optional[str],
    reply_ts: str,
    text: str,
    db_path: Path = DEFAULT_DB_PATH,
) -> None:
    """Record a successfully posted reply into the replies table."""
    db_path = Path(db_path)
    _ensure_replies_table(db_path)
    text_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
    with get_db_connection(db_path) as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO replies (
                event_id, channel_id, thread_ts, reply_ts, text_hash, text
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (event_id, channel_id, thread_ts, str(reply_ts), text_hash, text),
        )
        conn.commit()


def _parse_reply_created_at(value: Any) -> Optional[datetime]:
    """Parse a replies.created_at value into a UTC-aware datetime.
    Returns None if value is NULL/None, empty, or cannot be parsed as a valid timestamp."""
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if not isinstance(value, str):
        return None
    raw = value.strip()
    if not raw:
        return None
    if "-" in raw:
        try:
            normalized = raw[:-1] + "+00:00" if raw.endswith(("Z", "z")) else raw
            dt = datetime.fromisoformat(normalized)
            if dt.tzinfo is None:
                return dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except ValueError:
            pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f", "%Y/%m/%d %H:%M:%S"):
        try:
            dt = datetime.strptime(raw, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def prune_replies(
    days: int = DEFAULT_PRUNE_DAYS,
    dry_run: bool = False,
    db_path: Path = DEFAULT_DB_PATH,
    now: Optional[Any] = None,
) -> Dict[str, Any]:
    """Delete rows in the replies table whose created_at is older than (now - days).
    Rows with NULL or unparseable created_at are preserved."""
    if days < 0:
        raise ValueError(f"days must be non-negative, got {days}")
    db_path = Path(db_path)
    _ensure_replies_table(db_path)

    if now is None:
        now_dt = datetime.now(timezone.utc)
    elif isinstance(now, (int, float)):
        now_dt = datetime.fromtimestamp(float(now), tz=timezone.utc)
    elif isinstance(now, datetime):
        now_dt = (
            now.replace(tzinfo=timezone.utc)
            if now.tzinfo is None
            else now.astimezone(timezone.utc)
        )
    else:
        raise TypeError(f"Unsupported now type: {type(now)}")

    cutoff_dt = now_dt - timedelta(days=days)

    with get_db_connection(db_path) as conn:
        rows = conn.execute(
            "SELECT rowid, event_id, created_at FROM replies"
        ).fetchall()

        to_delete_rowids: List[int] = []
        for r in rows:
            parsed_dt = _parse_reply_created_at(r["created_at"])
            if parsed_dt is not None and parsed_dt < cutoff_dt:
                to_delete_rowids.append(int(r["rowid"]))

        if not dry_run and to_delete_rowids:
            conn.executemany(
                "DELETE FROM replies WHERE rowid = ?",
                [(rid,) for rid in to_delete_rowids],
            )
            conn.commit()

    count = len(to_delete_rowids)
    return {
        "ok": True,
        "dry_run": bool(dry_run),
        "days": int(days),
        "cutoff": cutoff_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "deleted_count": count,
    }


def cmd_prune_replies(args: argparse.Namespace) -> int:
    try:
        result = prune_replies(
            days=args.days,
            dry_run=bool(getattr(args, "dry_run", False)),
            db_path=Path(args.db),
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    except Exception as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 1


def _compute_thread_scan_oldest(
    thread_ts: str,
    event_ts: Optional[str] = None,
    margin_seconds: float = THREAD_SCAN_MARGIN_SECONDS,
) -> str:
    """Compute the oldest timestamp for scanning a thread:
    - If event_ts is provided and numeric, returns (event_ts - margin_seconds) formatted with 6 decimal places.
    - Otherwise (e.g. manual post without an event row), falls back to thread_ts for full-thread scan."""
    if event_ts is not None and str(event_ts).strip() != "":
        try:
            target_ts = float(event_ts)
            oldest_val = max(0.0, target_ts - float(margin_seconds))
            return f"{oldest_val:.6f}"
        except (TypeError, ValueError):
            pass
    return str(thread_ts)


def _is_ts_at_or_after(msg_ts: Any, oldest_ts: Any) -> bool:
    """Return True if msg_ts >= oldest_ts."""
    try:
        return float(msg_ts) >= float(oldest_ts)
    except (TypeError, ValueError):
        return str(msg_ts) >= str(oldest_ts)


def _is_ts_at_or_before(msg_ts: Any, latest_ts: Any) -> bool:
    """Return True if msg_ts <= latest_ts."""
    try:
        return float(msg_ts) <= float(latest_ts)
    except (TypeError, ValueError):
        return str(msg_ts) <= str(latest_ts)


def _parse_attempt_ts(value: Any) -> Optional[float]:
    """Parse an attempt timestamp (numeric epoch seconds, epoch string, or ISO/UTC datetime)
    into float epoch seconds. Returns None if value is None, empty, or unparseable."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, datetime):
        dt = (
            value.replace(tzinfo=timezone.utc)
            if value.tzinfo is None
            else value.astimezone(timezone.utc)
        )
        return dt.timestamp()
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        try:
            return float(raw)
        except ValueError:
            pass
        parsed_dt = _parse_reply_created_at(raw)
        if parsed_dt is not None:
            return parsed_dt.timestamp()
    return None


def _compute_recovery_window(
    attempt_ts: Any,
    before_seconds: float = RECOVERY_WINDOW_BEFORE_SECONDS,
    after_seconds: float = RECOVERY_WINDOW_AFTER_SECONDS,
) -> Optional[Tuple[str, str]]:
    """Compute (oldest, latest) Slack timestamps for the recovery window
    [attempt_ts - before_seconds, attempt_ts + after_seconds] (default: -3m to +5m).
    Returns None if attempt_ts is None or cannot be parsed."""
    epoch = _parse_attempt_ts(attempt_ts)
    if epoch is None:
        return None
    oldest_val = max(0.0, epoch - float(before_seconds))
    latest_val = max(0.0, epoch + float(after_seconds))
    return f"{oldest_val:.6f}", f"{latest_val:.6f}"


def _build_reply_metadata(
    event_id: Optional[str] = None,
    request_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Build Slack message metadata for chat.postMessage:
    event_type='yama_reply' and event_payload with event_id (when present)
    or request_id (UUID when event_id is absent)."""
    if event_id and str(event_id).strip():
        payload: Dict[str, Any] = {"event_id": str(event_id).strip()}
    else:
        req_id = (
            str(request_id).strip()
            if (request_id and str(request_id).strip())
            else str(uuid.uuid4())
        )
        payload = {"request_id": req_id}
    return {
        "event_type": REPLY_METADATA_EVENT_TYPE,
        "event_payload": payload,
    }


def _extract_reply_metadata_payload(msg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Extract event_payload dict from a Slack message's metadata if present
    and event_type is 'yama_reply' (or omitted)."""
    if not isinstance(msg, dict):
        return None
    meta = msg.get("metadata")
    if not isinstance(meta, dict):
        return None
    ev_type = meta.get("event_type")
    if ev_type is not None and str(ev_type).strip() != REPLY_METADATA_EVENT_TYPE:
        return None
    payload = meta.get("event_payload")
    if not isinstance(payload, dict):
        return None
    return payload


def _matches_reply_metadata(
    msg: Dict[str, Any],
    event_id: Optional[str] = None,
    request_id: Optional[str] = None,
) -> bool:
    """Return True if msg carries yama_reply metadata whose event_payload.event_id
    matches event_id or event_payload.request_id matches request_id."""
    payload = _extract_reply_metadata_payload(msg)
    if payload is None:
        return False
    if event_id and str(event_id).strip():
        meta_eid = payload.get("event_id")
        if meta_eid is not None and str(meta_eid).strip() == str(event_id).strip():
            return True
    if request_id and str(request_id).strip():
        meta_rid = payload.get("request_id")
        if meta_rid is not None and str(meta_rid).strip() == str(request_id).strip():
            return True
    return False


def _has_conflicting_reply_metadata(
    msg: Dict[str, Any],
    event_id: Optional[str] = None,
    request_id: Optional[str] = None,
) -> bool:
    """Return True if msg carries yama_reply metadata that explicitly identifies
    a DIFFERENT event_id or request_id than the target."""
    payload = _extract_reply_metadata_payload(msg)
    if payload is None:
        return False
    target_eid = str(event_id).strip() if (event_id and str(event_id).strip()) else None
    target_rid = str(request_id).strip() if (request_id and str(request_id).strip()) else None
    if target_eid is None and target_rid is None:
        return False
    meta_eid = payload.get("event_id")
    if meta_eid is not None and str(meta_eid).strip() != "":
        if target_eid is not None:
            return str(meta_eid).strip() != target_eid
        return True
    meta_rid = payload.get("request_id")
    if meta_rid is not None and str(meta_rid).strip() != "":
        if target_rid is not None:
            return str(meta_rid).strip() != target_rid
        return True
    return False


def _scan_thread_yama_messages(
    channel: str,
    parent_ts: str,
    client: Any,
    bot_user_ids: Set[str],
    sleep_fn: Optional[Callable[[float], None]] = None,
    event_ts: Optional[str] = None,
    oldest: Optional[str] = None,
    latest: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Scan a thread within [oldest, latest] (defaulting oldest to event_ts - 60s
    or parent_ts when oldest is not given) across up to THREAD_REPLIES_MAX_PAGES
    pages with include_all_metadata=True and return all messages posted by Yama
    within the scan window in chronological order (oldest first)."""
    scan_oldest = (
        str(oldest)
        if oldest is not None
        else _compute_thread_scan_oldest(parent_ts, event_ts=event_ts)
    )
    scan_latest = str(latest) if latest is not None else None
    yama_messages: List[Dict[str, Any]] = []
    seen_ts: Set[str] = set()
    cursor: Optional[str] = None

    for page in range(1, THREAD_REPLIES_MAX_PAGES + 1):
        replies_kwargs: Dict[str, Any] = {
            "channel": channel,
            "ts": str(parent_ts),
            "oldest": scan_oldest,
            "limit": THREAD_REPLIES_PAGE_LIMIT,
            "include_all_metadata": True,
        }
        if scan_latest is not None:
            replies_kwargs["latest"] = scan_latest
        if cursor:
            replies_kwargs["cursor"] = cursor

        def _do_call(kw: Dict[str, Any] = replies_kwargs) -> Any:
            try:
                return client.conversations_replies(**kw)
            except TypeError:
                fallback_kw = {
                    k: v for k, v in kw.items() if k in ("channel", "ts", "oldest", "limit", "cursor")
                }
                try:
                    return client.conversations_replies(**fallback_kw)
                except TypeError:
                    minimal_kw = {
                        k: v for k, v in kw.items() if k in ("channel", "ts", "limit")
                    }
                    return client.conversations_replies(**minimal_kw)

        resp = _call_with_retry(
            _do_call,
            max_attempts=HISTORY_RETRY_MAX_ATTEMPTS,
            backoffs=HISTORY_RETRY_BACKOFFS,
            sleep_fn=sleep_fn,
        )
        messages = resp.get("messages", []) or []
        for msg in messages:
            if not isinstance(msg, dict):
                continue
            if msg.get("user") not in bot_user_ids:
                continue
            msg_ts = msg.get("ts")
            if not msg_ts:
                continue
            if not _is_ts_at_or_after(msg_ts, scan_oldest):
                continue
            if scan_latest is not None and not _is_ts_at_or_before(msg_ts, scan_latest):
                continue
            msg_ts_str = str(msg_ts)
            if msg_ts_str in seen_ts:
                continue
            seen_ts.add(msg_ts_str)
            yama_messages.append(msg)

        meta = resp.get("response_metadata") or {}
        next_cursor = (
            (meta.get("next_cursor") or "").strip()
            if isinstance(meta, dict)
            else ""
        )
        has_more = resp.get("has_more")
        if not next_cursor or has_more is False:
            break
        if page >= THREAD_REPLIES_MAX_PAGES:
            break
        cursor = next_cursor

    def _ts_key(m: Dict[str, Any]) -> float:
        try:
            return float(m.get("ts", 0))
        except (TypeError, ValueError):
            return 0.0

    yama_messages.sort(key=_ts_key)
    return yama_messages


def _clear_event_error_message(
    event_id: str,
    db_path: Path = DEFAULT_DB_PATH,
) -> None:
    """Clear error_message to NULL for a reconciled DONE event."""
    db_path = Path(db_path)
    init_db(db_path)
    with get_db_connection(db_path) as conn:
        conn.execute(
            """
            UPDATE events
            SET error_message = NULL,
                updated_at = CURRENT_TIMESTAMP
            WHERE event_id = ? AND status = 'DONE'
            """,
            (event_id,),
        )
        conn.commit()


def _save_event_intended_text(
    event_id: str,
    intended_text: str,
    db_path: Path = DEFAULT_DB_PATH,
    attempt_ts: Optional[Any] = None,
) -> None:
    """Save the final post_text sent to chat.postMessage into events.intended_text
    and the attempt timestamp into events.attempt_ts when a reply post results in
    ambiguous_post_outcome."""
    if not event_id:
        return
    db_path = Path(db_path)
    init_db(db_path)
    parsed_attempt = _parse_attempt_ts(attempt_ts)
    attempt_ts_str = (
        f"{parsed_attempt:.6f}"
        if parsed_attempt is not None
        else f"{time.time():.6f}"
    )
    with get_db_connection(db_path) as conn:
        conn.execute(
            """
            UPDATE events
            SET intended_text = ?,
                attempt_ts = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE event_id = ?
            """,
            (intended_text, attempt_ts_str, event_id),
        )
        conn.commit()


def _normalize_reconcile_text(text: Any) -> str:
    """Normalize text for reconcile comparison:
    - unify CRLF (\\r\\n) to LF (\\n)
    - decode HTML escapes (&amp;, &lt;, &gt;)
    - strip leading/trailing whitespace
    """
    if text is None:
        return ""
    s = str(text).replace("\r\n", "\n")
    s = s.replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", "&")
    return s.strip()


def _select_matching_yama_message(
    yama_msgs: List[Dict[str, Any]],
    assigned_reply_ts: Set[str],
    cand_ts: str,
    intended_text: Optional[str] = None,
    event_id: Optional[str] = None,
    request_id: Optional[str] = None,
    oldest: Optional[str] = None,
    latest: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Select an unassigned Yama message within the scan window:
    Priority 1: Exact match on metadata.event_payload.event_id (or request_id).
    Priority 2: Legacy normalized text match against intended_text (for messages
                without conflicting metadata, strictly after cand_ts).
    Returns None if neither matches (no estimation fallback)."""
    # Priority 1: metadata match (event_id or request_id)
    if (event_id and str(event_id).strip()) or (request_id and str(request_id).strip()):
        for msg in yama_msgs:
            m_ts = str(msg.get("ts") or "")
            if not m_ts or m_ts in assigned_reply_ts:
                continue
            if oldest is not None and not _is_ts_at_or_after(m_ts, oldest):
                continue
            if latest is not None and not _is_ts_at_or_before(m_ts, latest):
                continue
            if _matches_reply_metadata(msg, event_id=event_id, request_id=request_id):
                return msg

    # Priority 2: normalized text match (backward compatibility for pre-metadata posts)
    norm_intended = _normalize_reconcile_text(intended_text)
    if not norm_intended:
        return None

    for msg in yama_msgs:
        m_ts = str(msg.get("ts") or "")
        if not m_ts or m_ts in assigned_reply_ts:
            continue
        if oldest is not None and not _is_ts_at_or_after(m_ts, oldest):
            continue
        if latest is not None and not _is_ts_at_or_before(m_ts, latest):
            continue
        try:
            is_after = float(m_ts) > float(cand_ts)
        except (TypeError, ValueError):
            is_after = m_ts > str(cand_ts)
        if not is_after:
            continue
        if _has_conflicting_reply_metadata(msg, event_id=event_id, request_id=request_id):
            continue
        if _normalize_reconcile_text(msg.get("text")) == norm_intended:
            return msg

    return None


def reconcile_replies(
    days: int = DEFAULT_RECONCILE_DAYS,
    dry_run: bool = False,
    db_path: Path = DEFAULT_DB_PATH,
    client: Optional[Any] = None,
    bot_user_id: str = YAMA_BOT_USER_ID,
    now: Optional[Any] = None,
    sleep_fn: Optional[Callable[[float], None]] = None,
) -> Dict[str, Any]:
    """Reconcile late-landed replies into the replies ledger for recent DONE/FAILED events
    whose error_message mentions 'reply' or 'ambiguous' (or pre_post_check_unverifiable) and that
    have no replies table entry, scanning [attempt_ts - 3m, attempt_ts + 5m] when attempt_ts is
    recorded (or from event_ts - 60s for legacy events without attempt_ts) and matching by:
      1. metadata.event_payload.event_id exact match
      2. normalized intended_text match (backward compatibility),
    and clear residual error_message on DONE events whose thread scan completes."""
    if days < 0:
        raise ValueError(f"days must be non-negative, got {days}")
    db_path = Path(db_path)
    _ensure_replies_table(db_path)

    if now is None:
        now_dt = datetime.now(timezone.utc)
    elif isinstance(now, (int, float)):
        now_dt = datetime.fromtimestamp(float(now), tz=timezone.utc)
    elif isinstance(now, datetime):
        now_dt = (
            now.replace(tzinfo=timezone.utc)
            if now.tzinfo is None
            else now.astimezone(timezone.utc)
        )
    else:
        raise TypeError(f"Unsupported now type: {type(now)}")

    cutoff_dt = now_dt - timedelta(days=days)

    with get_db_connection(db_path) as conn:
        assigned_reply_ts: Set[str] = {
            str(r["reply_ts"])
            for r in conn.execute(
                "SELECT reply_ts FROM replies WHERE reply_ts IS NOT NULL"
            ).fetchall()
        }
        existing_replies_by_event: Dict[str, str] = {
            str(r["event_id"]): str(r["reply_ts"])
            for r in conn.execute(
                "SELECT event_id, reply_ts FROM replies WHERE event_id IS NOT NULL AND reply_ts IS NOT NULL"
            ).fetchall()
        }
        raw_rows = conn.execute(
            """
            SELECT e.event_id, e.channel_id, e.user_id, e.text, e.ts, e.thread_ts,
                   e.status, e.error_message, e.intended_text, e.attempt_ts, e.created_at, e.updated_at
            FROM events e
            LEFT JOIN replies r ON e.event_id = r.event_id
            WHERE (
                e.status IN ('DONE', 'FAILED')
                OR (e.status = 'PENDING' AND LOWER(e.error_message) LIKE '%pre_post_check_unverifiable%')
            )
              AND r.event_id IS NULL
              AND e.error_message IS NOT NULL
              AND (
                  LOWER(e.error_message) LIKE '%reply%'
                  OR LOWER(e.error_message) LIKE '%ambiguous%'
                  OR LOWER(e.error_message) LIKE '%pre_post_check_unverifiable%'
              )
            ORDER BY CAST(e.ts AS REAL) ASC, e.created_at ASC, e.rowid ASC
            """
        ).fetchall()
        raw_done_error_rows = conn.execute(
            """
            SELECT e.event_id, e.channel_id, e.user_id, e.text, e.ts, e.thread_ts,
                   e.status, e.error_message, e.intended_text, e.attempt_ts, e.created_at, e.updated_at
            FROM events e
            WHERE e.status = 'DONE'
              AND e.error_message IS NOT NULL
            ORDER BY CAST(e.ts AS REAL) ASC, e.created_at ASC, e.rowid ASC
            """
        ).fetchall()

    def _is_within_window(row: Any) -> bool:
        parsed_dt = _parse_reply_created_at(row["created_at"]) or _parse_reply_created_at(
            row["updated_at"]
        )
        if parsed_dt is not None:
            return parsed_dt >= cutoff_dt
        return row["created_at"] is None and row["updated_at"] is None

    candidates: List[Dict[str, Any]] = [
        dict(r) for r in raw_rows if _is_within_window(r)
    ]
    done_error_candidates: List[Dict[str, Any]] = [
        dict(r) for r in raw_done_error_rows if _is_within_window(r)
    ]

    recorded: List[Dict[str, str]] = []
    not_found: List[str] = []
    done_error_cleared: Dict[str, List[str]] = {
        "with_reply": [],
        "without_reply": [],
    }
    error_details: List[Dict[str, str]] = []

    active_client: Optional[Any] = client
    bot_user_ids: Set[str] = {bot_user_id} if bot_user_id else {YAMA_BOT_USER_ID}
    auth_checked = False

    def _ensure_client_and_auth() -> Any:
        nonlocal active_client, auth_checked
        if active_client is None:
            active_client = get_web_client()
        if not auth_checked:
            auth_checked = True
            if hasattr(active_client, "auth_test"):
                try:
                    auth_resp = _call_with_retry(
                        lambda: active_client.auth_test(),
                        max_attempts=HISTORY_RETRY_MAX_ATTEMPTS,
                        backoffs=HISTORY_RETRY_BACKOFFS,
                        sleep_fn=sleep_fn,
                    )
                    if isinstance(auth_resp, dict) and auth_resp.get("user_id"):
                        bot_user_ids.add(str(auth_resp["user_id"]))
                except Exception:
                    pass
        return active_client

    phase1_handled_events: Dict[str, bool] = {}

    for cand in candidates:
        event_id = str(cand["event_id"])
        channel_id = str(cand["channel_id"])
        cand_ts = str(cand["ts"])
        parent_ts = str(cand["thread_ts"] if cand["thread_ts"] else cand["ts"])
        rec_win = _compute_recovery_window(cand.get("attempt_ts"))
        scan_oldest = rec_win[0] if rec_win is not None else None
        scan_latest = rec_win[1] if rec_win is not None else None

        try:
            cli = _ensure_client_and_auth()
            yama_msgs = _scan_thread_yama_messages(
                channel=channel_id,
                parent_ts=parent_ts,
                client=cli,
                bot_user_ids=bot_user_ids,
                sleep_fn=sleep_fn,
                event_ts=cand_ts,
                oldest=scan_oldest,
                latest=scan_latest,
            )

            matched_msg = _select_matching_yama_message(
                yama_msgs=yama_msgs,
                assigned_reply_ts=assigned_reply_ts,
                cand_ts=cand_ts,
                intended_text=cand.get("intended_text"),
                event_id=event_id,
                oldest=scan_oldest,
                latest=scan_latest,
            )

            if matched_msg is not None:
                matched_ts = str(matched_msg["ts"])
                matched_text = str(matched_msg.get("text") or "").strip()
                assigned_reply_ts.add(matched_ts)
                existing_replies_by_event[event_id] = matched_ts
                if not dry_run:
                    record_reply(
                        event_id=event_id,
                        channel_id=channel_id,
                        thread_ts=parent_ts,
                        reply_ts=matched_ts,
                        text=matched_text,
                        db_path=db_path,
                    )
                recorded.append({"event_id": event_id, "reply_ts": matched_ts})
                phase1_handled_events[event_id] = True
            else:
                not_found.append(event_id)
                phase1_handled_events[event_id] = False
        except Exception as exc:
            error_details.append({"event_id": event_id, "error": str(exc)})
            phase1_handled_events[event_id] = None  # type: ignore[assignment]

    for cand in done_error_candidates:
        event_id = str(cand["event_id"])
        channel_id = str(cand["channel_id"])
        cand_ts = str(cand["ts"])
        parent_ts = str(cand["thread_ts"] if cand["thread_ts"] else cand["ts"])

        if event_id in phase1_handled_events:
            outcome = phase1_handled_events[event_id]
            if outcome is True:
                if not dry_run:
                    _clear_event_error_message(event_id, db_path=db_path)
                done_error_cleared["with_reply"].append(event_id)
            elif outcome is False:
                if not dry_run:
                    _clear_event_error_message(event_id, db_path=db_path)
                done_error_cleared["without_reply"].append(event_id)
            continue

        rec_win = _compute_recovery_window(cand.get("attempt_ts"))
        scan_oldest = rec_win[0] if rec_win is not None else None
        scan_latest = rec_win[1] if rec_win is not None else None

        try:
            cli = _ensure_client_and_auth()
            yama_msgs = _scan_thread_yama_messages(
                channel=channel_id,
                parent_ts=parent_ts,
                client=cli,
                bot_user_ids=bot_user_ids,
                sleep_fn=sleep_fn,
                event_ts=cand_ts,
                oldest=scan_oldest,
                latest=scan_latest,
            )

            if event_id in existing_replies_by_event:
                rec_ts = existing_replies_by_event[event_id]
                reply_confirmed = any(str(m.get("ts")) == rec_ts for m in yama_msgs)
                if not dry_run:
                    _clear_event_error_message(event_id, db_path=db_path)
                if reply_confirmed:
                    done_error_cleared["with_reply"].append(event_id)
                else:
                    done_error_cleared["without_reply"].append(event_id)
            else:
                matched_msg = _select_matching_yama_message(
                    yama_msgs=yama_msgs,
                    assigned_reply_ts=assigned_reply_ts,
                    cand_ts=cand_ts,
                    intended_text=cand.get("intended_text"),
                    event_id=event_id,
                    oldest=scan_oldest,
                    latest=scan_latest,
                )

                if matched_msg is not None:
                    matched_ts = str(matched_msg["ts"])
                    matched_text = str(matched_msg.get("text") or "").strip()
                    assigned_reply_ts.add(matched_ts)
                    existing_replies_by_event[event_id] = matched_ts
                    if not dry_run:
                        record_reply(
                            event_id=event_id,
                            channel_id=channel_id,
                            thread_ts=parent_ts,
                            reply_ts=matched_ts,
                            text=matched_text,
                            db_path=db_path,
                        )
                        _clear_event_error_message(event_id, db_path=db_path)
                    recorded.append({"event_id": event_id, "reply_ts": matched_ts})
                    done_error_cleared["with_reply"].append(event_id)
                else:
                    if not dry_run:
                        _clear_event_error_message(event_id, db_path=db_path)
                    done_error_cleared["without_reply"].append(event_id)
        except Exception as exc:
            error_details.append({"event_id": event_id, "error": str(exc)})

    return {
        "ok": True,
        "dry_run": bool(dry_run),
        "days": int(days),
        "checked": len(candidates),
        "recorded": recorded,
        "not_found": not_found,
        "done_error_cleared": done_error_cleared,
        "errors": len(error_details),
        "error_details": error_details,
    }


def cmd_reconcile_replies(args: argparse.Namespace) -> int:
    try:
        result = reconcile_replies(
            days=args.days,
            dry_run=bool(getattr(args, "dry_run", False)),
            db_path=Path(args.db),
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    except Exception as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 1


def cmd_record_reply(args: argparse.Namespace) -> int:
    text_file = Path(args.text_file)
    if not text_file.exists():
        print(
            json.dumps({"ok": False, "error": f"text file not found: {text_file}"}),
            file=sys.stderr,
        )
        return 1

    text = text_file.read_text(encoding="utf-8").strip()
    if not text:
        print(
            json.dumps({"ok": False, "error": "text file is empty"}),
            file=sys.stderr,
        )
        return 1

    db_path = Path(getattr(args, "db", DEFAULT_DB_PATH))
    event_id = args.event_id
    thread_ts: Optional[str] = getattr(args, "thread_ts", None)

    try:
        existing = get_recorded_reply(event_id, db_path=db_path)
        if existing is not None:
            output = {
                "ok": True,
                "event_id": event_id,
                "channel": existing["channel_id"],
                "ts": existing["reply_ts"],
                "reply_ts": existing["reply_ts"],
                "thread_ts": existing["thread_ts"],
                "text_hash": existing["text_hash"],
                "already_recorded": True,
                "deduplicated": True,
            }
            print(json.dumps(output, ensure_ascii=False, indent=2))
            return 0

        record_reply(
            event_id=event_id,
            channel_id=args.channel,
            thread_ts=thread_ts,
            reply_ts=args.reply_ts,
            text=text,
            db_path=db_path,
        )
        recorded = get_recorded_reply(event_id, db_path=db_path)
        output = {
            "ok": True,
            "event_id": event_id,
            "channel": args.channel,
            "ts": str(args.reply_ts),
            "reply_ts": str(args.reply_ts),
            "thread_ts": thread_ts,
            "text_hash": recorded["text_hash"] if recorded else hashlib.sha256(text.encode("utf-8")).hexdigest(),
            "already_recorded": False,
            "deduplicated": False,
        }
        print(json.dumps(output, ensure_ascii=False, indent=2))
        return 0
    except Exception as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 1


def get_event_by_id(
    event_id: str,
    db_path: Path = DEFAULT_DB_PATH,
) -> Optional[Dict[str, Any]]:
    """Look up an event row by event_id from the events table."""
    if not event_id:
        return None
    db_path = Path(db_path)
    if not db_path.exists():
        return None
    init_db(db_path)
    with get_db_connection(db_path) as conn:
        row = conn.execute(
            """
            SELECT event_id, channel_id, user_id, text, ts, thread_ts, is_bot,
                   status, retry_count, error_message, next_retry_at, first_unverifiable_at,
                   intended_text, attempt_ts, created_at, updated_at
            FROM events
            WHERE event_id = ?
            """,
            (event_id,),
        ).fetchone()
        return dict(row) if row else None


def format_delayed_retry_prefix(first_unverifiable_at: Any) -> Optional[str]:
    """Format first_unverifiable_at in Asia/Tokyo as:
    遅れての再送（元の失敗時刻：{YYYY-MM-DD HH:MM} JST）
    Returns None if first_unverifiable_at is None, empty, or unparseable."""
    parsed_dt = _parse_reply_created_at(first_unverifiable_at)
    if parsed_dt is None and isinstance(first_unverifiable_at, (int, float)):
        try:
            parsed_dt = datetime.fromtimestamp(float(first_unverifiable_at), tz=timezone.utc)
        except (ValueError, OSError, OverflowError):
            parsed_dt = None
    elif parsed_dt is None and isinstance(first_unverifiable_at, str):
        raw = first_unverifiable_at.strip()
        if raw:
            try:
                parsed_dt = datetime.fromtimestamp(float(raw), tz=timezone.utc)
            except (ValueError, OSError, OverflowError):
                parsed_dt = None
    if parsed_dt is None:
        return None
    jst_dt = parsed_dt.astimezone(ASIA_TOKYO_TZ)
    return f"遅れての再送（元の失敗時刻：{jst_dt.strftime('%Y-%m-%d %H:%M')} JST）"


def apply_delayed_retry_prefix(
    text: str,
    first_unverifiable_at: Any,
) -> Tuple[str, bool, Optional[str]]:
    """Prepend the 1-line delayed retry mark to text if first_unverifiable_at is present
    and the text does not already start with the delayed retry mark.
    Returns (final_text, retry_mark_added, prefix_line)."""
    prefix_line = format_delayed_retry_prefix(first_unverifiable_at)
    if prefix_line is None:
        return text, False, None
    if text.startswith(prefix_line) or DELAYED_RETRY_PREFIX_RE.match(text):
        return text, False, prefix_line
    return f"{prefix_line}\n{text}", True, prefix_line


def _is_ts_after(msg_ts: Any, base_ts: Any) -> bool:
    """Return True if msg_ts is strictly after base_ts."""
    try:
        return float(msg_ts) > float(base_ts)
    except (TypeError, ValueError):
        return str(msg_ts) > str(base_ts)


def _is_bot_message(
    msg: Dict[str, Any],
    bot_ts_set: Optional[Set[str]] = None,
) -> bool:
    """Return True if a Slack message dict represents a bot message
    (bot_id, subtype=bot_message, is_bot, bot_profile, or recorded as is_bot=1 in DB)."""
    if msg.get("bot_id") is not None and str(msg.get("bot_id")).strip() != "":
        return True
    if msg.get("subtype") == "bot_message":
        return True
    if bool(msg.get("is_bot")):
        return True
    if msg.get("bot_profile") is not None:
        return True
    if bot_ts_set and str(msg.get("ts") or "") in bot_ts_set:
        return True
    return False


def _get_channel_bot_ts_set(
    channel: str,
    db_path: Path = DEFAULT_DB_PATH,
) -> Set[str]:
    """Return the set of message timestamps in channel recorded with is_bot=1 in local SQLite."""
    db_path = Path(db_path)
    if not db_path.exists():
        return set()
    try:
        with get_db_connection(db_path) as conn:
            rows = conn.execute(
                "SELECT ts FROM events WHERE channel_id = ? AND is_bot = 1",
                (channel,),
            ).fetchall()
            return {str(r["ts"]) for r in rows if r["ts"]}
    except Exception:
        return set()


def _find_answering_message_in_thread(
    messages: List[Dict[str, Any]],
    parent_ts: str,
    questioner_user_ids: Set[str],
    yama_user_ids: Set[str],
    bot_ts_set: Optional[Set[str]] = None,
    min_text_length: int = ANSWERED_MIN_TEXT_LENGTH,
) -> Optional[Dict[str, Any]]:
    """Conservative answered check over thread messages:
    Returns the first message strictly after parent_ts that satisfies ALL of:
      1. Poster is neither the original questioner nor Yama itself
      2. Not a bot message (excludes bot_id / subtype=bot_message / is_bot equivalents)
      3. Stripped body text length > min_text_length (10 chars)"""
    effective_questioners = set(questioner_user_ids)
    for msg in messages:
        if not isinstance(msg, dict):
            continue
        if str(msg.get("ts") or "") == str(parent_ts):
            parent_user = str(msg.get("user") or "").strip()
            if (
                parent_user
                and parent_user not in yama_user_ids
                and not _is_bot_message(msg, bot_ts_set)
            ):
                effective_questioners.add(parent_user)

    for msg in messages:
        if not isinstance(msg, dict):
            continue
        msg_ts = msg.get("ts")
        if not msg_ts or not _is_ts_after(msg_ts, parent_ts):
            continue
        subtype = msg.get("subtype")
        if subtype is not None and subtype not in ("file_share", "thread_broadcast"):
            continue
        if _is_bot_message(msg, bot_ts_set):
            continue
        msg_user = str(msg.get("user") or "").strip()
        if not msg_user:
            continue
        if msg_user in effective_questioners or msg_user in yama_user_ids:
            continue
        cleaned_text = str(msg.get("text") or "").strip()
        if len(cleaned_text) <= min_text_length:
            continue
        return msg
    return None


def _pre_post_check_thread_for_reply(
    channel: str,
    raw_text: str,
    post_text: str,
    thread_ts: str,
    client: Optional[Any] = None,
    bot_user_id: Optional[str] = None,
    is_delayed_retry: bool = False,
    questioner_user_id: Optional[str] = None,
    db_path: Path = DEFAULT_DB_PATH,
    max_attempts: int = HISTORY_RETRY_MAX_ATTEMPTS,
    backoffs: Tuple[float, ...] = HISTORY_RETRY_BACKOFFS,
    sleep_fn: Optional[Callable[[float], None]] = None,
    event_ts: Optional[str] = None,
    oldest: Optional[str] = None,
    event_id: Optional[str] = None,
) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
    """Scan a thread (starting from event_ts - 60s when event_ts is given, or thread_ts
    for full-thread scan, using pagination with per-page retry)
    to check:
      1. Whether Yama's own identical message (matching metadata event_id, raw_text, or post_text) already exists
      2. If is_delayed_retry is True, whether a qualifying answer from another human user exists
    Returns (existing_own_ts, answered_msg)."""
    active_client = client if client is not None else get_web_client()
    if not hasattr(active_client, "conversations_replies"):
        return None, None

    resolved_bot_uid = _resolve_bot_user_id(
        active_client,
        bot_user_id=bot_user_id,
        max_attempts=max_attempts,
        backoffs=backoffs,
        sleep_fn=sleep_fn,
    )
    yama_user_ids: Set[str] = {YAMA_BOT_USER_ID, resolved_bot_uid}
    target_texts: Set[str] = {raw_text.strip(), post_text.strip()}
    scan_oldest = (
        str(oldest)
        if oldest is not None
        else _compute_thread_scan_oldest(thread_ts, event_ts=event_ts)
    )

    thread_messages: List[Dict[str, Any]] = []
    cursor: Optional[str] = None

    for page in range(1, THREAD_REPLIES_MAX_PAGES + 1):
        replies_kwargs: Dict[str, Any] = {
            "channel": channel,
            "ts": thread_ts,
            "oldest": scan_oldest,
            "limit": THREAD_REPLIES_PAGE_LIMIT,
            "include_all_metadata": True,
        }
        if cursor:
            replies_kwargs["cursor"] = cursor

        def _do_page_call(kw: Dict[str, Any] = replies_kwargs) -> Any:
            try:
                return active_client.conversations_replies(**kw)
            except TypeError:
                no_meta_kw = {
                    k: v
                    for k, v in kw.items()
                    if k in ("channel", "ts", "oldest", "limit", "cursor")
                }
                try:
                    return active_client.conversations_replies(**no_meta_kw)
                except TypeError:
                    fallback_kw = {
                        k: v for k, v in kw.items() if k in ("channel", "ts", "oldest", "limit")
                    }
                    return active_client.conversations_replies(**fallback_kw)

        resp = _call_with_retry(
            _do_page_call,
            max_attempts=max_attempts,
            backoffs=backoffs,
            sleep_fn=sleep_fn,
        )
        messages = resp.get("messages", []) or []
        page_yama_msgs: List[Dict[str, Any]] = []
        for msg in messages:
            if not isinstance(msg, dict):
                continue
            msg_ts = msg.get("ts")
            if msg_ts and not _is_ts_at_or_after(msg_ts, scan_oldest):
                continue
            if msg.get("user") in yama_user_ids and msg_ts:
                page_yama_msgs.append(msg)
            thread_messages.append(msg)

        for ymsg in page_yama_msgs:
            if _matches_reply_metadata(ymsg, event_id=event_id):
                return str(ymsg["ts"]), None
        for ymsg in page_yama_msgs:
            if _has_conflicting_reply_metadata(ymsg, event_id=event_id):
                continue
            if (ymsg.get("text") or "").strip() in target_texts:
                return str(ymsg["ts"]), None

        meta = resp.get("response_metadata") or {}
        next_cursor = (
            (meta.get("next_cursor") or "").strip()
            if isinstance(meta, dict)
            else ""
        )
        has_more = resp.get("has_more")
        if not next_cursor or has_more is False:
            break
        if page >= THREAD_REPLIES_MAX_PAGES:
            print(
                json.dumps(
                    {
                        "ok": False,
                        "warning": "thread_replies_page_limit_reached",
                        "channel": channel,
                        "thread_ts": thread_ts,
                        "max_pages": THREAD_REPLIES_MAX_PAGES,
                    },
                    ensure_ascii=False,
                ),
                file=sys.stderr,
            )
            break
        cursor = next_cursor

    if not is_delayed_retry:
        return None, None

    questioner_uids: Set[str] = set()
    if questioner_user_id and str(questioner_user_id).strip():
        questioner_uids.add(str(questioner_user_id).strip())
    bot_ts_set = _get_channel_bot_ts_set(channel, db_path=db_path)

    answered_baseline_ts = (
        str(event_ts)
        if (event_ts is not None and str(event_ts).strip() != "")
        else str(thread_ts)
    )
    answered_msg = _find_answering_message_in_thread(
        messages=thread_messages,
        parent_ts=answered_baseline_ts,
        questioner_user_ids=questioner_uids,
        yama_user_ids=yama_user_ids,
        bot_ts_set=bot_ts_set,
        min_text_length=ANSWERED_MIN_TEXT_LENGTH,
    )
    return None, answered_msg


def cmd_reply(args: argparse.Namespace) -> int:
    text_file = Path(args.text_file)
    if not text_file.exists():
        print(
            json.dumps({"ok": False, "error": f"text file not found: {text_file}"}),
            file=sys.stderr,
        )
        return 1

    raw_text = text_file.read_text(encoding="utf-8").strip()
    if not raw_text:
        print(
            json.dumps({"ok": False, "error": "text file is empty"}),
            file=sys.stderr,
        )
        return 1

    event_id: Optional[str] = getattr(args, "event_id", None)
    db_path = Path(getattr(args, "db", DEFAULT_DB_PATH))

    event_row = get_event_by_id(event_id, db_path=db_path) if event_id else None
    event_ts: Optional[str] = (
        str(event_row["ts"])
        if (event_row is not None and event_row.get("ts") is not None)
        else None
    )
    first_unverifiable_at = (
        event_row.get("first_unverifiable_at") if event_row is not None else None
    )
    is_delayed_retry = format_delayed_retry_prefix(first_unverifiable_at) is not None
    post_text, retry_mark_added, retry_prefix_line = apply_delayed_retry_prefix(
        raw_text, first_unverifiable_at
    )

    if event_id:
        existing = get_recorded_reply(event_id, db_path=db_path)
        if existing is not None:
            output = {
                "ok": True,
                "channel": existing["channel_id"],
                "ts": existing["reply_ts"],
                "thread_ts": existing["thread_ts"],
                "event_id": event_id,
                "deduplicated": True,
                "delayed_retry": is_delayed_retry,
                "retry_mark_added": False,
            }
            print(json.dumps(output, ensure_ascii=False, indent=2))
            return 0

    if args.thread_ts:
        ledger_ts = _find_own_ledger_reply(
            channel=args.channel,
            text=post_text,
            thread_ts=args.thread_ts,
            db_path=db_path,
        )
        if ledger_ts:
            if event_id:
                record_reply(
                    event_id=event_id,
                    channel_id=args.channel,
                    thread_ts=args.thread_ts,
                    reply_ts=ledger_ts,
                    text=post_text,
                    db_path=db_path,
                )
            output = {
                "ok": True,
                "channel": args.channel,
                "ts": ledger_ts,
                "thread_ts": args.thread_ts,
                "deduplicated": True,
                "delayed_retry": is_delayed_retry,
                "retry_mark_added": False,
            }
            if event_id:
                output["event_id"] = event_id
            print(json.dumps(output, ensure_ascii=False, indent=2))
            return 0

    read_client: Optional[Any] = None
    answered_msg: Optional[Dict[str, Any]] = None
    try:
        read_client = get_web_client()
        if args.thread_ts and is_delayed_retry:
            existing_ts, answered_msg = _pre_post_check_thread_for_reply(
                channel=args.channel,
                raw_text=raw_text,
                post_text=post_text,
                thread_ts=args.thread_ts,
                client=read_client,
                is_delayed_retry=True,
                questioner_user_id=event_row.get("user_id") if event_row else None,
                db_path=db_path,
                max_attempts=HISTORY_RETRY_MAX_ATTEMPTS,
                backoffs=HISTORY_RETRY_BACKOFFS,
                event_ts=event_ts,
                event_id=event_id,
            )
        else:
            existing_ts = _find_own_recent_message(
                channel=args.channel,
                text=post_text,
                thread_ts=args.thread_ts,
                client=read_client,
                max_attempts=HISTORY_RETRY_MAX_ATTEMPTS,
                backoffs=HISTORY_RETRY_BACKOFFS,
                raise_on_error=True,
                event_ts=event_ts,
                event_id=event_id,
            )
    except Exception as pre_exc:
        if args.thread_ts:
            pre_err: Dict[str, Any] = {
                "ok": False,
                "error": "pre_post_check_unverifiable",
                "channel": args.channel,
                "thread_ts": args.thread_ts,
                "check_error": str(pre_exc),
            }
            if event_id:
                pre_err["event_id"] = event_id
                try:
                    draft_path = db_path.parent / f"reply-{event_id}.txt"
                    draft_path.parent.mkdir(parents=True, exist_ok=True)
                    if text_file.resolve() != draft_path.resolve():
                        draft_path.write_text(raw_text + "\n", encoding="utf-8")
                except Exception:
                    pass
            print(json.dumps(pre_err, ensure_ascii=False, indent=2), file=sys.stderr)
            return 1
        print(
            json.dumps(
                {
                    "ok": False,
                    "warning": "pre_post_channel_check_failed",
                    "channel": args.channel,
                    "thread_ts": args.thread_ts,
                    "error": str(pre_exc),
                },
                ensure_ascii=False,
            ),
            file=sys.stderr,
        )
        existing_ts = None
        answered_msg = None

    if existing_ts:
        if event_id:
            record_reply(
                event_id=event_id,
                channel_id=args.channel,
                thread_ts=args.thread_ts,
                reply_ts=existing_ts,
                text=post_text,
                db_path=db_path,
            )
        output = {
            "ok": True,
            "channel": args.channel,
            "ts": existing_ts,
            "thread_ts": args.thread_ts,
            "deduplicated": True,
            "delayed_retry": is_delayed_retry,
            "retry_mark_added": False,
        }
        if event_id:
            output["event_id"] = event_id
        print(json.dumps(output, ensure_ascii=False, indent=2))
        return 0

    if answered_msg is not None:
        answered_ts = str(answered_msg["ts"])
        answered_user = str(answered_msg.get("user") or "")
        final_event_status = "DONE"
        if event_id and event_row is not None:
            try:
                committed = commit_event(
                    event_id=event_id,
                    status="DONE",
                    db_path=db_path,
                )
                final_event_status = str(committed.get("status") or "DONE")
            except Exception:
                pass
        output = {
            "ok": True,
            "result": SKIPPED_ANSWERED_RESULT,
            "skipped": True,
            "channel": args.channel,
            "thread_ts": args.thread_ts,
            "answered_by_ts": answered_ts,
            "answered_ts": answered_ts,
            "answered_by_user": answered_user,
            "delayed_retry": True,
            "retry_mark_added": False,
            "deduplicated": False,
            "status": final_event_status,
        }
        if event_id:
            output["event_id"] = event_id
        print(json.dumps(output, ensure_ascii=False, indent=2))
        return 0

    request_id: Optional[str] = (
        None if (event_id is not None and str(event_id).strip() != "") else str(uuid.uuid4())
    )
    reply_metadata = _build_reply_metadata(event_id=event_id, request_id=request_id)
    attempt_epoch = time.time()

    try:
        client = get_web_client(no_retry=True)
        kwargs: Dict[str, Any] = {
            "channel": args.channel,
            "text": post_text,
            "metadata": reply_metadata,
        }
        if args.thread_ts:
            kwargs["thread_ts"] = args.thread_ts

        resp = client.chat_postMessage(**kwargs)
        posted_channel = resp.get("channel") or args.channel
        posted_ts = resp.get("ts")
        if event_id and posted_ts:
            record_reply(
                event_id=event_id,
                channel_id=posted_channel,
                thread_ts=args.thread_ts,
                reply_ts=posted_ts,
                text=post_text,
                db_path=db_path,
            )
        output = {
            "ok": True,
            "result": "posted",
            "channel": resp.get("channel"),
            "ts": posted_ts,
            "thread_ts": args.thread_ts,
            "message": resp.get("message", {}),
            "delayed_retry": is_delayed_retry,
            "retry_mark_added": retry_mark_added,
        }
        if is_delayed_retry and retry_prefix_line:
            output["retry_mark"] = retry_prefix_line
        if event_id:
            output["event_id"] = event_id
            output["deduplicated"] = False
        print(json.dumps(output, ensure_ascii=False, indent=2))
        return 0
    except SlackApiError as exc:
        err_out = {
            "ok": False,
            "error": exc.response.get("error", str(exc)),
            "channel": args.channel,
        }
        print(json.dumps(err_out, ensure_ascii=False, indent=2), file=sys.stderr)
        return 1
    except Exception as exc:
        # Connection-level failure: the post may or may not have landed.
        # Verify by reading back history within the attempt timestamp window
        # [attempt_ts - 3m, attempt_ts + 5m] with up to 5 attempts + backoff
        # before reporting failure, so the caller never blindly retries and double-posts.
        rec_oldest, rec_latest = _compute_recovery_window(attempt_epoch)
        recovery_error: Optional[BaseException] = None
        try:
            recovered = _find_own_recent_message(
                args.channel,
                post_text,
                args.thread_ts,
                client=read_client,
                max_attempts=HISTORY_RETRY_MAX_ATTEMPTS,
                backoffs=HISTORY_RETRY_BACKOFFS,
                raise_on_error=True,
                event_ts=event_ts,
                oldest=rec_oldest,
                latest=rec_latest,
                event_id=event_id,
                request_id=request_id,
            )
        except Exception as rec_exc:
            recovered = None
            recovery_error = rec_exc

        if recovered:
            if event_id:
                record_reply(
                    event_id=event_id,
                    channel_id=args.channel,
                    thread_ts=args.thread_ts,
                    reply_ts=recovered,
                    text=post_text,
                    db_path=db_path,
                )
            output = {
                "ok": True,
                "result": "posted",
                "channel": args.channel,
                "ts": recovered,
                "thread_ts": args.thread_ts,
                "recovered_after_ambiguous_error": True,
                "delayed_retry": is_delayed_retry,
                "retry_mark_added": retry_mark_added,
            }
            if is_delayed_retry and retry_prefix_line:
                output["retry_mark"] = retry_prefix_line
            if event_id:
                output["event_id"] = event_id
                output["deduplicated"] = False
            print(json.dumps(output, ensure_ascii=False, indent=2))
            return 0

        if event_id:
            try:
                _save_event_intended_text(
                    event_id=event_id,
                    intended_text=post_text,
                    db_path=db_path,
                    attempt_ts=attempt_epoch,
                )
            except Exception:
                pass

        ambiguous_err: Dict[str, Any] = {
            "ok": False,
            "error": "ambiguous_post_outcome",
            "channel": args.channel,
            "thread_ts": args.thread_ts,
            "post_error": str(exc),
        }
        if event_id:
            ambiguous_err["event_id"] = event_id
        if recovery_error is not None:
            ambiguous_err["recovery_error"] = str(recovery_error)
        print(json.dumps(ambiguous_err, ensure_ascii=False, indent=2), file=sys.stderr)
        return 1


def _resolve_bot_user_id(
    client: Any,
    bot_user_id: Optional[str] = None,
    max_attempts: int = HISTORY_RETRY_MAX_ATTEMPTS,
    backoffs: Tuple[float, ...] = HISTORY_RETRY_BACKOFFS,
    sleep_fn: Optional[Callable[[float], None]] = None,
) -> str:
    """Resolve Yama's bot_user_id, preferring explicit argument, environment variable,
    or value already cached on the client instance before calling auth_test() with retry."""
    if bot_user_id and str(bot_user_id).strip():
        return str(bot_user_id).strip()
    env_uid = (
        os.environ.get("YAMA_BOT_USER_ID", "").strip()
        or os.environ.get("SLACK_BOT_USER_ID", "").strip()
    )
    if env_uid:
        return env_uid
    cached_uid = getattr(client, "_cached_bot_user_id", None)
    if isinstance(cached_uid, str) and cached_uid.strip():
        return cached_uid.strip()

    resolved = YAMA_BOT_USER_ID
    if hasattr(client, "auth_test"):
        try:
            auth_resp = _call_with_retry(
                lambda: client.auth_test(),
                max_attempts=max_attempts,
                backoffs=backoffs,
                sleep_fn=sleep_fn,
            )
            if isinstance(auth_resp, dict) and auth_resp.get("user_id"):
                resolved = str(auth_resp["user_id"]).strip()
        except Exception:
            resolved = YAMA_BOT_USER_ID
    try:
        setattr(client, "_cached_bot_user_id", resolved)
    except Exception:
        pass
    return resolved


def _lookup_own_recent_message_once(
    channel: str,
    text: str,
    thread_ts: Optional[str],
    client: Optional[Any] = None,
    bot_user_id: Optional[str] = None,
    max_attempts: int = HISTORY_RETRY_MAX_ATTEMPTS,
    backoffs: Tuple[float, ...] = HISTORY_RETRY_BACKOFFS,
    sleep_fn: Optional[Callable[[float], None]] = None,
    event_ts: Optional[str] = None,
    oldest: Optional[str] = None,
    latest: Optional[str] = None,
    event_id: Optional[str] = None,
    request_id: Optional[str] = None,
) -> Optional[str]:
    """Look up our own message in the target thread or channel.
    Priority 1: exact match on metadata.event_payload.event_id (or request_id).
    Priority 2: legacy text match against messages without conflicting metadata.
    Applies _call_with_retry per individual API call so a transient failure on
    page N retries only page N rather than restarting from page 1."""
    active_client = client if client is not None else get_web_client()
    if thread_ts and not hasattr(active_client, "conversations_replies"):
        return None
    if not thread_ts and not hasattr(active_client, "conversations_history"):
        return None
    resolved_bot_uid = _resolve_bot_user_id(
        active_client,
        bot_user_id=bot_user_id,
        max_attempts=max_attempts,
        backoffs=backoffs,
        sleep_fn=sleep_fn,
    )
    normalized_target = _normalize_reconcile_text(text)

    if thread_ts:
        scan_oldest = (
            str(oldest)
            if oldest is not None
            else _compute_thread_scan_oldest(thread_ts, event_ts=event_ts)
        )
        scan_latest = (
            str(latest).strip()
            if (latest is not None and str(latest).strip() != "")
            else None
        )
        cursor: Optional[str] = None
        for page in range(1, THREAD_REPLIES_MAX_PAGES + 1):
            replies_kwargs: Dict[str, Any] = {
                "channel": channel,
                "ts": thread_ts,
                "oldest": scan_oldest,
                "limit": THREAD_REPLIES_PAGE_LIMIT,
                "include_all_metadata": True,
            }
            if scan_latest is not None:
                replies_kwargs["latest"] = scan_latest
            if cursor:
                replies_kwargs["cursor"] = cursor

            def _do_page_call(kw: Dict[str, Any] = replies_kwargs) -> Any:
                try:
                    return active_client.conversations_replies(**kw)
                except TypeError:
                    no_meta_kw = {
                        k: v
                        for k, v in kw.items()
                        if k in ("channel", "ts", "oldest", "limit", "cursor")
                    }
                    try:
                        return active_client.conversations_replies(**no_meta_kw)
                    except TypeError:
                        fallback_kw = {
                            k: v for k, v in kw.items() if k in ("channel", "ts", "oldest", "limit")
                        }
                        return active_client.conversations_replies(**fallback_kw)

            resp = _call_with_retry(
                _do_page_call,
                max_attempts=max_attempts,
                backoffs=backoffs,
                sleep_fn=sleep_fn,
            )
            messages = resp.get("messages", []) or []
            page_candidates: List[Dict[str, Any]] = []
            for msg in messages:
                if not isinstance(msg, dict):
                    continue
                if msg.get("user") != resolved_bot_uid:
                    continue
                msg_ts = msg.get("ts")
                if not msg_ts:
                    continue
                if not _is_ts_at_or_after(msg_ts, scan_oldest):
                    continue
                if scan_latest is not None and not _is_ts_at_or_before(msg_ts, scan_latest):
                    continue
                page_candidates.append(msg)

            # Priority 1: Metadata match (event_id or request_id)
            for msg in page_candidates:
                if _matches_reply_metadata(msg, event_id=event_id, request_id=request_id):
                    return str(msg["ts"])

            # Priority 2: Legacy text match (without conflicting metadata)
            for msg in page_candidates:
                if _has_conflicting_reply_metadata(msg, event_id=event_id, request_id=request_id):
                    continue
                if normalized_target and _normalize_reconcile_text(msg.get("text")) == normalized_target:
                    return str(msg["ts"])

            meta = resp.get("response_metadata") or {}
            next_cursor = (
                (meta.get("next_cursor") or "").strip()
                if isinstance(meta, dict)
                else ""
            )
            has_more = resp.get("has_more")
            if not next_cursor or has_more is False:
                break
            if page >= THREAD_REPLIES_MAX_PAGES:
                print(
                    json.dumps(
                        {
                            "ok": False,
                            "warning": "thread_replies_page_limit_reached",
                            "channel": channel,
                            "thread_ts": thread_ts,
                            "max_pages": THREAD_REPLIES_MAX_PAGES,
                        },
                        ensure_ascii=False,
                    ),
                    file=sys.stderr,
                )
                break
            cursor = next_cursor
        return None

    history_kwargs: Dict[str, Any] = {
        "channel": channel,
        "limit": CHANNEL_HISTORY_BOUND_LIMIT,
        "include_all_metadata": True,
    }
    scan_oldest_ch = (
        str(oldest).strip()
        if (oldest is not None and str(oldest).strip() != "")
        else None
    )
    scan_latest_ch = (
        str(latest).strip()
        if (latest is not None and str(latest).strip() != "")
        else None
    )
    if scan_oldest_ch is not None:
        history_kwargs["oldest"] = scan_oldest_ch
    if scan_latest_ch is not None:
        history_kwargs["latest"] = scan_latest_ch

    def _do_history_call(kw: Dict[str, Any] = history_kwargs) -> Any:
        try:
            return active_client.conversations_history(**kw)
        except TypeError:
            return active_client.conversations_history(
                channel=channel,
                limit=CHANNEL_HISTORY_BOUND_LIMIT,
            )

    resp = _call_with_retry(
        _do_history_call,
        max_attempts=max_attempts,
        backoffs=backoffs,
        sleep_fn=sleep_fn,
    )
    messages = resp.get("messages", []) or []
    ch_candidates: List[Dict[str, Any]] = []
    for msg in messages:
        if not isinstance(msg, dict):
            continue
        if msg.get("user") != resolved_bot_uid:
            continue
        msg_ts = msg.get("ts")
        if not msg_ts:
            continue
        if scan_oldest_ch is not None and not _is_ts_at_or_after(msg_ts, scan_oldest_ch):
            continue
        if scan_latest_ch is not None and not _is_ts_at_or_before(msg_ts, scan_latest_ch):
            continue
        ch_candidates.append(msg)

    for msg in ch_candidates:
        if _matches_reply_metadata(msg, event_id=event_id, request_id=request_id):
            return str(msg["ts"])

    for msg in ch_candidates:
        if _has_conflicting_reply_metadata(msg, event_id=event_id, request_id=request_id):
            continue
        if normalized_target and _normalize_reconcile_text(msg.get("text")) == normalized_target:
            return str(msg["ts"])
    return None


def _find_own_recent_message(
    channel: str,
    text: str,
    thread_ts: Optional[str],
    client: Optional[Any] = None,
    bot_user_id: Optional[str] = None,
    max_attempts: int = HISTORY_RETRY_MAX_ATTEMPTS,
    backoffs: Tuple[float, ...] = HISTORY_RETRY_BACKOFFS,
    sleep_fn: Optional[Callable[[float], None]] = None,
    raise_on_error: bool = False,
    event_ts: Optional[str] = None,
    oldest: Optional[str] = None,
    latest: Optional[str] = None,
    event_id: Optional[str] = None,
    request_id: Optional[str] = None,
) -> Optional[str]:
    """Return the ts of our own matching message in the target thread or channel, or None.
    Used for pre-post deduplication and post-error recovery confirmation,
    with up to max_attempts retries per API call."""
    try:
        return _lookup_own_recent_message_once(
            channel=channel,
            text=text,
            thread_ts=thread_ts,
            client=client,
            bot_user_id=bot_user_id,
            max_attempts=max_attempts,
            backoffs=backoffs,
            sleep_fn=sleep_fn,
            event_ts=event_ts,
            oldest=oldest,
            latest=latest,
            event_id=event_id,
            request_id=request_id,
        )
    except Exception:
        if raise_on_error:
            raise
        return None


def cmd_react(args: argparse.Namespace) -> int:
    emoji_name = args.emoji.strip(":")
    try:
        client = get_web_client()
        client.reactions_add(
            channel=args.channel,
            timestamp=args.ts,
            name=emoji_name,
        )
        print(
            json.dumps(
                {
                    "ok": True,
                    "channel": args.channel,
                    "ts": args.ts,
                    "emoji": emoji_name,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0
    except SlackApiError as exc:
        err_code = exc.response.get("error", str(exc))
        if err_code == "already_reacted":
            print(
                json.dumps(
                    {
                        "ok": True,
                        "already_reacted": True,
                        "channel": args.channel,
                        "ts": args.ts,
                        "emoji": emoji_name,
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            )
            return 0
        print(
            json.dumps({"ok": False, "error": err_code}, ensure_ascii=False, indent=2),
            file=sys.stderr,
        )
        return 1
    except Exception as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 1


def cmd_commit(args: argparse.Namespace) -> int:
    try:
        updated = commit_event(
            event_id=args.event_id,
            status=args.status,
            error_message=args.error,
            db_path=Path(args.db),
        )
        print(json.dumps({"ok": True, "event": updated}, ensure_ascii=False, indent=2))
        return 0
    except Exception as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 1


def cmd_mode(args: argparse.Namespace) -> int:
    mode = get_mode(Path(args.mode_file))
    print(mode)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="agent_tools.py",
        description="CLI tools for Yama Slack autonomous assistant",
    )
    parser.add_argument(
        "--db",
        default=os.environ.get("YAMA_DB_PATH", str(DEFAULT_DB_PATH)),
        help="Path to SQLite database (default: state/slack_events.db)",
    )
    parser.add_argument(
        "--mode-file",
        default=os.environ.get("YAMA_MODE_FILE", str(DEFAULT_MODE_PATH)),
        help="Path to mode file (default: config/mode.txt)",
    )

    subparsers = parser.add_subparsers(dest="subcommand", required=True)

    # pending
    p_pending = subparsers.add_parser("pending", help="Output PROCESSING events as JSON")
    p_pending.set_defaults(func=cmd_pending)

    # history
    p_history = subparsers.add_parser("history", help="Read channel or thread conversation history")
    p_history.add_argument("--channel", required=True, help="Slack channel ID")
    p_history.add_argument("--thread-ts", default=None, help="Thread parent timestamp (optional)")
    p_history.add_argument("--limit", type=int, default=20, help="Number of messages to fetch (default: 20)")
    p_history.set_defaults(func=cmd_history)

    # reply
    p_reply = subparsers.add_parser("reply", help="Post a message or reply as Yama bot")
    p_reply.add_argument("--channel", required=True, help="Slack channel ID")
    p_reply.add_argument("--text-file", required=True, help="Path to file containing message text")
    p_reply.add_argument("--thread-ts", default=None, help="Thread parent timestamp (optional)")
    p_reply.add_argument(
        "--event-id",
        default=None,
        help="Source event_id for idempotent reply deduplication (optional)",
    )
    p_reply.set_defaults(func=cmd_reply)

    # record-reply
    p_record_reply = subparsers.add_parser(
        "record-reply",
        help="Explicitly record an existing reply into the replies ledger without posting",
    )
    p_record_reply.add_argument("--event-id", required=True, help="Source event_id")
    p_record_reply.add_argument("--channel", required=True, help="Slack channel ID")
    p_record_reply.add_argument("--reply-ts", required=True, help="Timestamp of the existing reply on Slack")
    p_record_reply.add_argument("--text-file", required=True, help="Path to file containing reply text")
    p_record_reply.add_argument("--thread-ts", default=None, help="Thread parent timestamp (optional)")
    p_record_reply.add_argument(
        "--db",
        default=argparse.SUPPRESS,
        help=argparse.SUPPRESS,
    )
    p_record_reply.set_defaults(func=cmd_record_reply)

    # reconcile-replies
    p_reconcile = subparsers.add_parser(
        "reconcile-replies",
        help="Reconcile late-landed Yama replies into the replies ledger",
    )
    p_reconcile.add_argument(
        "--days",
        type=int,
        default=DEFAULT_RECONCILE_DAYS,
        help=f"Look back this many days for candidate events (default: {DEFAULT_RECONCILE_DAYS})",
    )
    p_reconcile.add_argument(
        "--dry-run",
        action="store_true",
        help="Report candidate replies that would be recorded without writing to DB",
    )
    p_reconcile.add_argument(
        "--db",
        default=argparse.SUPPRESS,
        help=argparse.SUPPRESS,
    )
    p_reconcile.set_defaults(func=cmd_reconcile_replies)

    # prune-replies
    p_prune = subparsers.add_parser(
        "prune-replies",
        help="Prune replies table entries older than N days (keeping NULL or unparseable timestamps)",
    )
    p_prune.add_argument(
        "--days",
        type=int,
        default=DEFAULT_PRUNE_DAYS,
        help=f"Delete replies older than this many days (default: {DEFAULT_PRUNE_DAYS})",
    )
    p_prune.add_argument(
        "--dry-run",
        action="store_true",
        help="Report how many rows would be deleted without deleting them",
    )
    p_prune.add_argument(
        "--db",
        default=argparse.SUPPRESS,
        help=argparse.SUPPRESS,
    )
    p_prune.set_defaults(func=cmd_prune_replies)

    # react
    p_react = subparsers.add_parser("react", help="Add an emoji reaction to a message")
    p_react.add_argument("--channel", required=True, help="Slack channel ID")
    p_react.add_argument("--ts", required=True, help="Target message timestamp")
    p_react.add_argument("--emoji", required=True, help="Emoji name (e.g. eyes, white_check_mark)")
    p_react.set_defaults(func=cmd_react)

    # commit
    p_commit = subparsers.add_parser("commit", help="Commit event status (DONE, IGNORED, FAILED)")
    p_commit.add_argument("--event-id", required=True, help="Target event_id")
    p_commit.add_argument(
        "--status",
        required=True,
        choices=["DONE", "IGNORED", "FAILED", "done", "ignored", "failed"],
        help="Target status: DONE | IGNORED | FAILED",
    )
    p_commit.add_argument("--error", default=None, help="Optional error message when FAILED")
    p_commit.set_defaults(func=cmd_commit)

    # mode
    p_mode = subparsers.add_parser("mode", help="Print current operating mode (shadow / mention / full)")
    p_mode.set_defaults(func=cmd_mode)

    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
