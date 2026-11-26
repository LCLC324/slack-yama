#!/usr/bin/env python3
"""
Yama Agent Tools CLI (Layer 3)
Subcommands for the awakened autonomous agent:
  - pending
  - history --channel X [--thread-ts Y] [--limit N]
  - event-text --event-id E   (print full stored text of an event)
  - reply --channel X --text-file F [--thread-ts Y] (--event-id E | --no-event-id)
      [--force --reason TEXT]   (exactly one of --event-id / --no-event-id is required;
      --event-id must exist in the events table and not be terminal without a recorded
      reply unless --force --reason; blocked in shadow mode)
  - record-reply --event-id E --channel X --reply-ts T --text-file F [--thread-ts Y]
  - reconcile-replies [--days 7] [--dry-run]
  - prune-replies [--days 90] [--dry-run]
  - triage-log (--json JSON | --file PATH)   (append one validated JSONL line to state/triage_log.jsonl)
  - prune-state-files [--days 7] [--dry-run]   (delete old DONE reply drafts and decisions-*.json)
  - react --channel X --ts T --emoji eyes   (blocked in shadow mode)
  - commit --event-id E --status DONE|IGNORED|FAILED [--error MSG] [--force --reason TEXT]
      (terminal guard: DONE/IGNORED same status = no-op, other status / FAILED = already_terminal)
  - commit-batch --file decisions.json   (several commits at once; all-or-nothing validation)
  - seed-mirror [--days 3] [--dry-run]   (one-off read-only fill of the local thread mirror)
  - requeue --event-id E [--force --reason TEXT]   (manual re-queue FAILED -> PENDING;
      resets reap_count only; refused with reply_already_landed when the replies ledger
      already has a reply for E, unless --force --reason)
  - mode
"""

import argparse
import hashlib
import http.client
import json
import os
import re
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from zoneinfo import ZoneInfo

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from slack_sdk.web.slack_response import SlackResponse

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
# Terminal guard (2026-11-23, candidate 19, stage 1). commit_event checks the row's
# current status inside its BEGIN IMMEDIATE transaction:
#   PENDING / PROCESSING -> anything : applied as before
#   DONE / IGNORED -> same status    : no-op success (row untouched, "noop": true)
#   DONE / IGNORED -> other status   : rejected (already_terminal), row untouched
#   FAILED -> anything               : rejected (requeue is the way back to PENDING)
#   terminal -> other + force/reason : applied; reason kept in error_message, ops alert
#   DONE / IGNORED -> same + force/reason : applied (reason text corrected; 2026-11-25)
#   terminal -> FAILED + force/reason     : stays FAILED, never PENDING (2026-11-25)
# Rejections and forced corrections each append one line to the ops alert outbox.
TERMINAL_STATUSES = ("DONE", "IGNORED", "FAILED")
COMMIT_REJECTED_CODE = "already_terminal"
OPS_ALERTS_FILENAME = "ops_alerts.jsonl"
# Candidate 27, alternative A: FAILED events with a landed reply already alerted once.
FAILED_LANDED_ALERTED_FILENAME = "failed_with_landed_reply_alerted.json"

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
# Burst cap (2026-11-22): bound how much text one `history` call hands the worker.
# Measured on the largest real thread (189 msgs): the raw Slack JSON for --limit 20
# was 67,980 bytes for only 7,662 chars of message text (blocks / bot_profile /
# attachments dominated), and --limit 200 was 658,266 bytes. The compact view keeps
# the judgement material (who / when / text / thread / metadata / link titles) and
# drops the render-only fields. Limits:
#   HISTORY_MAX_LIMIT          : --limit is clamped to this (Slack's per-call max is 1000)
#   HISTORY_MESSAGE_TEXT_LIMIT : per-message text cap (longest stored post so far: 2,121 chars)
#   HISTORY_TOTAL_TEXT_BUDGET  : total text cap per call; oldest non-parent messages are
#                                dropped first and counted in older_omitted
HISTORY_MAX_LIMIT: int = 50
HISTORY_MESSAGE_TEXT_LIMIT: int = 3000
HISTORY_TOTAL_TEXT_BUDGET: int = 40000
HISTORY_ATTACHMENT_TEXT_LIMIT: int = 300
# Delayed-retry answered check: read at most one page of this many replies from the
# scan start (event ts - 60s). If no qualifying answer is in that page and more
# replies remain, the check is incomplete and the resend is refused (fail-closed).
ANSWERED_CHECK_PAGE_LIMIT: int = 100
THREAD_SCAN_MARGIN_SECONDS: int = 60
RECOVERY_WINDOW_BEFORE_SECONDS: float = 180.0
RECOVERY_WINDOW_AFTER_SECONDS: float = 300.0
REPLY_METADATA_EVENT_TYPE: str = "yama_reply"
DEFAULT_PRUNE_DAYS: int = 90
DEFAULT_RECONCILE_DAYS: int = 7
YAMA_BOT_USER_ID: str = "U_YAMA_BOT"
UNVERIFIABLE_ERROR_CODE: str = "pre_post_check_unverifiable"
# Legacy fixed wait (1 hour). Used only when a fixed wait is explicitly requested
# (commit_event(retry_wait_seconds=...) or env YAMA_UNVERIFIABLE_RETRY_WAIT_SEC);
# otherwise the stepped ladder below applies.
UNVERIFIABLE_RETRY_WAIT_SECONDS: int = 3600
# Stall fix (2026-11-22, proposal 1): stepped wait before re-dispatching an event
# that failed with pre_post_check_unverifiable: 2 -> 5 -> 15 -> 30 -> 60 min, then
# 60 min each time until the 24h terminal. The step is derived from the time elapsed
# since first_unverifiable_at (no new column): elapsed < 2m -> 2m, < 7m -> 5m,
# < 22m -> 15m, < 52m -> 30m, else 60m. Every re-dispatch re-runs the full pre-post
# check (fail-closed unchanged). Override with env YAMA_UNVERIFIABLE_RETRY_LADDER_SEC
# (comma-separated seconds).
UNVERIFIABLE_RETRY_LADDER_SECONDS: Tuple[int, ...] = (120, 300, 900, 1800, 3600)
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
# Review fix 2026-11-25 [mid-1]: aligned with the hook definition ("an answer is
# any post with body text by someone other than the questioner and Yama, bots
# included"). 0 means: at least one non-whitespace character.
ANSWERED_MIN_TEXT_LENGTH: int = 0


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


CURL_READ_CONNECT_TIMEOUT_SECS: int = 5
CURL_READ_TIMEOUT_SECS: int = 15

# ---------------------------------------------------------------------------
# Read deadlines (stall fix 2026-11-22, proposal 3).
# Without a deadline one read could stack up to ~6 minutes: _call_with_retry
# (5 attempts, 1+2+4+8s backoff) x SDK ConnectionErrorRetryHandler (1 retry) x
# SDK timeout 30s, plus the curl fallback (15s) per attempt.
# A deadline is an absolute time.monotonic() value. While one is active
# (read_deadline() context), every read is bounded by it:
#   - _call_with_retry stops before an attempt / backoff that cannot finish in time,
#   - _CurlFallbackWebClient caps the SDK socket timeout and curl --max-time at the
#     remaining time and skips the SDK's own retry handler (retries are already
#     done by _call_with_retry),
# and SlackReadDeadlineExceeded (a TimeoutError, i.e. a transient connection
# error) is raised. Callers keep their existing failure semantics:
#   - context reads (history) fall back to the local DB (local_db_fallback),
#   - safety reads (pre-post check, answered check, post-error recovery) fail
#     closed exactly as before (pre_post_check_unverifiable / ambiguous_post_outcome).
# POST calls (chat_postMessage) are never bounded or retried by this mechanism.
# ---------------------------------------------------------------------------
CONTEXT_READ_DEADLINE_SECONDS: float = 20.0
# Safety reads: budget = min(SAFETY_READ_MAX_SECONDS,
#   time left until the Reaper may reclaim the event (PROCESSING_TIMEOUT_SECONDS
#   since dispatch, i.e. events.updated_at) - SAFETY_READ_RESERVE_SECONDS).
# Below SAFETY_READ_MIN_SECONDS a thread reply is refused without reading
# (pre_post_check_unverifiable; fail-closed, re-checked on the next dispatch).
SAFETY_READ_MAX_SECONDS: float = 180.0
SAFETY_READ_RESERVE_SECONDS: float = 60.0
SAFETY_READ_MIN_SECONDS: float = 5.0
PROCESSING_TIMEOUT_SECONDS: float = 900.0

_ACTIVE_READ_DEADLINE: Optional[float] = None


class SlackReadDeadlineExceeded(TimeoutError):
    """A read could not complete before the active read deadline."""


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _deadline_remaining(deadline: Optional[float] = None) -> Optional[float]:
    """Seconds left before `deadline` (or the active read deadline); None if unbounded."""
    dl = deadline if deadline is not None else _ACTIVE_READ_DEADLINE
    if dl is None:
        return None
    return dl - time.monotonic()


class read_deadline:
    """Context manager that bounds every Slack read inside it to `seconds` from now
    (nested deadlines keep the earlier one). seconds=None means no new bound."""

    def __init__(self, seconds: Optional[float]):
        self.seconds = seconds
        self._saved: Optional[float] = None

    def __enter__(self) -> "read_deadline":
        global _ACTIVE_READ_DEADLINE
        self._saved = _ACTIVE_READ_DEADLINE
        if self.seconds is not None:
            new_dl = time.monotonic() + max(0.0, float(self.seconds))
            if _ACTIVE_READ_DEADLINE is None or new_dl < _ACTIVE_READ_DEADLINE:
                _ACTIVE_READ_DEADLINE = new_dl
        return self

    def __exit__(self, *exc: Any) -> None:
        global _ACTIVE_READ_DEADLINE
        _ACTIVE_READ_DEADLINE = self._saved


def _parse_db_utc(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
        try:
            return datetime.strptime(raw, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)
    except ValueError:
        return None


def safety_read_budget_seconds(
    event_row: Optional[Dict[str, Any]],
    now: Optional[datetime] = None,
) -> float:
    """Time budget for one safety read, counted back from the time the Reaper may
    reclaim the event (events.updated_at is set when the dispatcher moves it to
    PROCESSING). Without an event row (no --event-id) only the cap applies."""
    cap = _env_float("YAMA_SAFETY_READ_MAX_SEC", SAFETY_READ_MAX_SECONDS)
    if event_row is None or str(event_row.get("status") or "") != "PROCESSING":
        return cap
    dispatched = _parse_db_utc(event_row.get("updated_at"))
    if dispatched is None:
        return cap
    now_dt = now or datetime.now(timezone.utc)
    timeout = _env_float("YAMA_PROCESSING_TIMEOUT_SEC", PROCESSING_TIMEOUT_SECONDS)
    reserve = _env_float("YAMA_SAFETY_READ_RESERVE_SEC", SAFETY_READ_RESERVE_SECONDS)
    left = timeout - (now_dt - dispatched).total_seconds() - reserve
    return max(0.0, min(cap, left))


def _curl_api_read(
    token: str,
    method: str,
    params: Dict[str, Any],
    max_time: Optional[float] = None,
) -> SlackResponse:
    """Perform one read-only Slack Web API call via a curl subprocess and return
    the parsed response as a SlackResponse. The curl config (including the
    Authorization header) is passed over stdin (`curl -K -`), never via argv,
    so the token does not appear in the process list. Raises RuntimeError on
    any curl-side failure (nonzero exit, unparsable output, or ok=false)."""
    if not token or '"' in token or "\n" in token or "\r" in token:
        raise RuntimeError("curl read aborted: token is not safe to pass via config")
    total_time = CURL_READ_TIMEOUT_SECS
    if max_time is not None:
        total_time = max(1, min(CURL_READ_TIMEOUT_SECS, int(max_time)))
    form: Dict[str, str] = {}
    for key, value in (params or {}).items():
        if value is None:
            continue
        if isinstance(value, bool):
            form[key] = "true" if value else "false"
        else:
            form[key] = str(value)
    config = "\n".join(
        [
            f'url = "https://slack.com/api/{method}"',
            f'header = "Authorization: Bearer {token}"',
            'header = "Content-Type: application/x-www-form-urlencoded"',
            f'data = "{urllib.parse.urlencode(form)}"',
            "silent",
            "show-error",
            f"connect-timeout = {min(CURL_READ_CONNECT_TIMEOUT_SECS, total_time)}",
            f"max-time = {total_time}",
            "",
        ]
    )
    proc = subprocess.run(
        ["curl", "-K", "-"],
        input=config,
        capture_output=True,
        text=True,
        timeout=total_time + 10,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"curl read failed for {method}: exit={proc.returncode}")
    try:
        data = json.loads(proc.stdout)
    except ValueError:
        raise RuntimeError(f"curl read failed for {method}: response is not JSON")
    if not isinstance(data, dict) or not data.get("ok"):
        err = data.get("error") if isinstance(data, dict) else "unknown"
        raise RuntimeError(f"curl read failed for {method}: slack error={err}")
    return SlackResponse(
        client=None,
        http_verb="POST",
        api_url=f"https://slack.com/api/{method}",
        req_args={},
        data=data,
        headers={},
        status_code=200,
    )


class _CurlFallbackWebClient(WebClient):
    """WebClient whose read calls (conversations_replies / conversations_history)
    fall back to a curl subprocess read when the SDK transport fails with a
    connection error (e.g. http.client.IncompleteRead during a bursty outage).
    If curl also fails, the original SDK exception is re-raised, so existing
    failure semantics (retry layers, local DB fallback, fail-closed pre-post
    checks) are unchanged: curl only ever adds successes. POST methods such as
    chat_postMessage are never overridden."""

    def _read_with_curl_fallback(
        self, method: str, super_fn: Callable[..., Any], kwargs: Dict[str, Any]
    ) -> Any:
        remaining = _deadline_remaining()
        if remaining is None:
            try:
                return super_fn(**kwargs)
            except TRANSIENT_CONNECTION_ERRORS as exc:
                try:
                    return _curl_api_read(self.token, method, kwargs)
                except Exception:
                    raise exc
        # Deadline-bounded read: cap the SDK socket timeout at the remaining time
        # and skip the SDK's own connection retry (_call_with_retry retries).
        if remaining <= 0:
            raise SlackReadDeadlineExceeded(f"{method}: read deadline reached before the call")
        saved_timeout = self.timeout
        saved_handlers = self.retry_handlers
        try:
            self.timeout = max(1.0, min(float(saved_timeout or 30), remaining))
            self.retry_handlers = []
            try:
                return super_fn(**kwargs)
            except TRANSIENT_CONNECTION_ERRORS as exc:
                left = _deadline_remaining()
                if left is None or left < 1.0:
                    raise exc
                try:
                    return _curl_api_read(self.token, method, kwargs, max_time=left)
                except Exception:
                    raise exc
        finally:
            self.timeout = saved_timeout
            self.retry_handlers = saved_handlers

    def conversations_replies(self, **kwargs: Any) -> Any:  # type: ignore[override]
        return self._read_with_curl_fallback(
            "conversations.replies", super().conversations_replies, kwargs
        )

    def conversations_history(self, **kwargs: Any) -> Any:  # type: ignore[override]
        return self._read_with_curl_fallback(
            "conversations.history", super().conversations_history, kwargs
        )


def get_web_client(env_path: Path = DEFAULT_ENV_PATH, no_retry: bool = False) -> WebClient:
    """Load SLACK_BOT_TOKEN from config/.env (or environment) and return a Slack WebClient."""
    load_env_file(env_path)
    bot_token = os.environ.get("SLACK_BOT_TOKEN", "").strip()
    if not bot_token:
        raise RuntimeError(f"SLACK_BOT_TOKEN is not set in environment or {env_path}")
    if no_retry:
        # Never auto-retry a POST whose outcome is unknown: a lost response
        # combined with an automatic retry posts the message twice.
        return _CurlFallbackWebClient(token=bot_token, retry_handlers=[])
    return _CurlFallbackWebClient(token=bot_token)


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


def _parse_retry_ladder(raw: Optional[str]) -> Tuple[int, ...]:
    """Parse a comma-separated ladder of positive seconds; invalid -> default ladder."""
    if raw is None or not str(raw).strip():
        return UNVERIFIABLE_RETRY_LADDER_SECONDS
    try:
        steps = tuple(int(float(p)) for p in str(raw).split(",") if p.strip())
    except ValueError:
        return UNVERIFIABLE_RETRY_LADDER_SECONDS
    if not steps or any(s <= 0 for s in steps):
        return UNVERIFIABLE_RETRY_LADDER_SECONDS
    return steps


def unverifiable_retry_wait_seconds(
    elapsed_sec: float, ladder: Optional[Tuple[int, ...]] = None
) -> int:
    """Stepped wait for the next re-dispatch of a pre_post_check_unverifiable event.
    Step i is chosen once elapsed_sec (time since first_unverifiable_at) has reached
    the sum of steps 0..i-1, so each re-dispatch moves one step up the ladder even
    when the dispatch itself is delayed by a poll cycle or a long batch."""
    steps = ladder or UNVERIFIABLE_RETRY_LADDER_SECONDS
    elapsed = max(0.0, float(elapsed_sec))
    cumulative = 0
    for i, step in enumerate(steps):
        if i == len(steps) - 1:
            return int(step)
        cumulative += step
        if elapsed < cumulative:
            return int(step)
    return int(steps[-1])


def _ops_alerts_path(db_path: Path) -> Path:
    """Ops alert outbox (same file the dispatch hook delivers; see ops_monitor.py)."""
    env_path = os.environ.get("YAMA_OPS_ALERTS_PATH", "").strip()
    return Path(env_path) if env_path else Path(db_path).parent / OPS_ALERTS_FILENAME


def _append_ops_alert(entry: Dict[str, Any], db_path: Path, quiet: bool = False) -> bool:
    """Append one JSON line to the ops alert outbox under an exclusive flock (same
    format as ops_monitor.outbox_append). Never raises: a failed write is reported on
    stderr so the caller's commit result is not affected. quiet=True skips that stderr
    line; the caller must then report the False return itself (requeue puts it in
    its single stderr JSON)."""
    import fcntl

    path = _ops_alerts_path(db_path)
    try:
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
        return True
    except Exception as exc:
        if quiet:
            return False
        print(
            f"[commit] ALERT_WRITE_FAILED ({entry.get('kind')}): {exc} entry={json.dumps(entry, ensure_ascii=False)[:500]}",
            file=sys.stderr,
        )
        return False


def classify_commit_transition(current_status: Any, requested_status: str, force: bool = False) -> str:
    """Terminal guard decision for commit_event: 'apply' | 'noop' | 'reject' | 'force'.
    See TERMINAL_STATUSES above for the table."""
    current = str(current_status or "").upper()
    requested = str(requested_status or "").upper()
    if current not in TERMINAL_STATUSES:
        return "apply"
    if force:
        # review fix 2026-11-25 [minor-3](b): force is checked before the same-state
        # no-op so a DONE -> DONE --force --reason can correct the reason text.
        return "force"
    if current in ("DONE", "IGNORED") and requested == current:
        return "noop"
    return "reject"


def _forced_error_message(
    previous_status: str, new_status: str, reason: str, error_message: Optional[str]
) -> str:
    marker = f"[forced {previous_status}->{new_status}] {reason.strip()}"
    return f"{error_message} {marker}" if error_message else marker


def _resolve_commit_params(
    status: str,
    force: bool,
    reason: Optional[str],
    now: Optional[Any],
    retry_wait_seconds: Optional[int],
    expire_seconds: Optional[int],
) -> Dict[str, Any]:
    """Validate the commit request and resolve clock / wait settings (no DB access)."""
    status = status.upper()
    if status not in VALID_COMMIT_STATUSES:
        raise ValueError(f"Invalid status '{status}'. Must be one of {VALID_COMMIT_STATUSES}")
    if force and not (reason and str(reason).strip()):
        raise ValueError("force requires a non-empty reason (--reason)")

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

    # Fixed wait only when explicitly requested (argument or env); otherwise the
    # stepped ladder (UNVERIFIABLE_RETRY_LADDER_SECONDS) decides per failure below.
    fixed_wait_sec: Optional[int] = None
    if retry_wait_seconds is not None:
        fixed_wait_sec = int(retry_wait_seconds)
    elif os.environ.get("YAMA_UNVERIFIABLE_RETRY_WAIT_SEC", "").strip():
        fixed_wait_sec = int(os.environ["YAMA_UNVERIFIABLE_RETRY_WAIT_SEC"].strip())
    wait_ladder = _parse_retry_ladder(os.environ.get("YAMA_UNVERIFIABLE_RETRY_LADDER_SEC"))
    active_expire_sec = (
        int(expire_seconds)
        if expire_seconds is not None
        else int(os.environ.get("YAMA_UNVERIFIABLE_EXPIRE_SEC", UNVERIFIABLE_EXPIRE_SECONDS))
    )
    return {
        "status": status,
        "now_dt": now_dt,
        "fixed_wait_sec": fixed_wait_sec,
        "wait_ladder": wait_ladder,
        "active_expire_sec": active_expire_sec,
    }


_COMMIT_ROW_COLUMNS = """event_id, channel_id, user_id, text, ts, thread_ts, is_bot,
                   status, retry_count, error_message, next_retry_at, first_unverifiable_at,
                   intended_text, attempt_ts, created_at, updated_at"""


def _commit_event_in_tx(
    conn: Any,
    event_id: str,
    params: Dict[str, Any],
    error_message: Optional[str],
    max_retries: int,
    force: bool,
    reason: Optional[str],
) -> Tuple[Dict[str, Any], str, str]:
    """Core of commit_event, run inside a transaction the caller has opened
    (BEGIN IMMEDIATE). Never begins, commits or rolls back by itself, and has no side
    effects outside the DB, so commit_batch can run several of these in one
    transaction (review fix 2026-11-25, [minor-3](d)).
    Returns (row_dict, decision, current_status). For decision 'noop' / 'reject'
    nothing was written and row_dict is the current row."""
    status = params["status"]
    now_dt = params["now_dt"]
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
        raise LookupError(f"Event '{event_id}' not found in database")

    # Terminal guard (stage 1): decide on the row read inside this transaction,
    # so a check here cannot race with another writer.
    current_status = str(row["status"] or "").upper()
    decision = classify_commit_transition(current_status, status, force=force)
    if decision in ("noop", "reject"):
        current_row = dict(
            conn.execute(
                f"SELECT {_COMMIT_ROW_COLUMNS} FROM events WHERE event_id = ?",
                (event_id,),
            ).fetchone()
        )
        current_row["noop"] = True
        if decision == "reject":
            current_row["rejected"] = COMMIT_REJECTED_CODE
            current_row["requested_status"] = status
        return current_row, decision, current_status
    if decision == "force":
        error_message = _forced_error_message(
            current_status, status, str(reason), error_message
        )

    current_retries = int(row["retry_count"] or 0)

    if decision == "force" and status == "FAILED":
        # Audit fix 2026-11-23 (#3), widened by review fix 2026-11-25 ([major-1],
        # candidate 29(a)): a forced commit to FAILED from any terminal state
        # (DONE / IGNORED / FAILED) settles as FAILED. It must not run the retry
        # branch below (retry_count < max_retries, or pre_post_check_unverifiable in
        # the reason, would flip it to PENDING and bypass the requeue ledger check).
        # Getting back to PENDING stays requeue-only. retry_count is unchanged.
        new_retries = current_retries
        final_status = "FAILED"
        conn.execute(
            """
            UPDATE events
            SET status = 'FAILED',
                error_message = ?,
                next_retry_at = NULL,
                updated_at = CURRENT_TIMESTAMP
            WHERE event_id = ?
            """,
            (error_message, event_id),
        )
    elif status == "FAILED":
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
            if elapsed_sec >= params["active_expire_sec"]:
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
                fixed_wait_sec = params["fixed_wait_sec"]
                active_wait_sec = (
                    fixed_wait_sec
                    if fixed_wait_sec is not None
                    else unverifiable_retry_wait_seconds(elapsed_sec, params["wait_ladder"])
                )
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
            # A failure for any other reason ends the unverifiable wait: clear
            # first_unverifiable_at so later retries are not treated as delayed
            # resends (candidate 19 remainder, review fix 2026-11-25 [minor-3](a)).
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
                    first_unverifiable_at = NULL,
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

    updated = conn.execute(
        f"SELECT {_COMMIT_ROW_COLUMNS} FROM events WHERE event_id = ?",
        (event_id,),
    ).fetchone()
    return dict(updated), decision, current_status


def _report_commit_outcome(
    result: Dict[str, Any],
    decision: str,
    current_status: str,
    event_id: str,
    status: str,
    error_message: Optional[str],
    reason: Optional[str],
    source: str,
    db_path: Path,
) -> None:
    """Side effects of a commit that has been applied (stderr line + ops alert).
    Called only after the transaction is committed."""
    if decision == "reject":
        print(
            f"[commit] WARN {COMMIT_REJECTED_CODE}: event {event_id} is {current_status}; "
            f"{status} commit from {source} ignored (row unchanged)",
            file=sys.stderr,
        )
        _append_ops_alert(
            {
                "kind": "commit_rejected",
                "reason": COMMIT_REJECTED_CODE,
                "event_id": event_id,
                "current_status": current_status,
                "requested_status": status,
                "requested_error": (error_message or "")[:300] or None,
                "source": source,
                "at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            },
            Path(db_path),
        )
    elif decision == "force":
        result["forced"] = True
        result["previous_status"] = current_status
        print(
            f"[commit] WARN forced: event {event_id} {current_status} -> {result.get('status')} "
            f"(requested {status}) from {source}: {str(reason).strip()}",
            file=sys.stderr,
        )
        _append_ops_alert(
            {
                "kind": "commit_forced",
                "event_id": event_id,
                "previous_status": current_status,
                "requested_status": status,
                "new_status": result.get("status"),
                "reason": str(reason).strip()[:300],
                "source": source,
                "at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            },
            Path(db_path),
        )


def commit_event(
    event_id: str,
    status: str,
    error_message: Optional[str] = None,
    db_path: Path = DEFAULT_DB_PATH,
    max_retries: int = MAX_RETRIES,
    now: Optional[Any] = None,
    retry_wait_seconds: Optional[int] = None,
    expire_seconds: Optional[int] = None,
    force: bool = False,
    reason: Optional[str] = None,
    source: str = "commit",
) -> Dict[str, Any]:
    """
    Terminal guard (checked first, inside the same BEGIN IMMEDIATE transaction):
      - current PENDING / PROCESSING: the rules below apply unchanged.
      - current DONE / IGNORED and the same status requested (without force): no-op
        success; the row is not touched and the returned dict has "noop": True.
      - current DONE / IGNORED and another status, or current FAILED (any status):
        rejected without raising; the row is not touched, the returned dict (the current
        row) has "noop": True and "rejected": "already_terminal", and one line goes to
        the ops alert outbox. FAILED goes back to PENDING only through requeue.
      - force=True with a non-empty reason: a rejected transition is applied anyway (for
        corrections such as IGNORED -> DONE). The reason is kept in error_message
        ("[forced OLD->NEW] reason") and one line goes to the ops alert outbox.
        A forced same-state DONE -> DONE / IGNORED -> IGNORED rewrites the reason text
        (review fix 2026-11-25 [minor-3](b)).
        A forced commit to FAILED from any terminal state (DONE / IGNORED / FAILED)
        settles as FAILED: the retry rules below are skipped (retry_count unchanged),
        so force is never a way around requeue (review fix 2026-11-25 [major-1]).

    Commit the final or retry state of an event:
      - DONE / IGNORED: transitions status directly to DONE or IGNORED and clears wait timestamps.
      - FAILED (with pre_post_check_unverifiable): treated as an API wait rather than a post failure.
        Does NOT consume retry_count. Instead, keeps status='PENDING' and sets next_retry_at in the
        future (stepped 2/5/15/30/60 min by time since the first failure) so the existing
        30-second dispatcher naturally skips it until the wait
        elapses and then re-dispatches it to the agent without needing a separate state-transition daemon.
        If >= 24 hours have elapsed since first_unverifiable_at and history is still unreadable,
        transitions permanently to FAILED (abandoned).
      - FAILED (other errors): increments retry_count and clears first_unverifiable_at;
        if retry_count < max_retries (3), reverts status to PENDING for retry; if
        retry_count >= max_retries (3), transitions status permanently to FAILED (DLQ).
    """
    params = _resolve_commit_params(status, force, reason, now, retry_wait_seconds, expire_seconds)
    status = params["status"]

    init_db(db_path)
    with get_db_connection(db_path) as conn:
        conn.execute("BEGIN IMMEDIATE;")
        try:
            result, decision, current_status = _commit_event_in_tx(
                conn, event_id, params, error_message, max_retries, force, reason
            )
        except Exception:
            conn.rollback()
            raise
        if decision in ("noop", "reject"):
            conn.rollback()
        else:
            conn.commit()
    _report_commit_outcome(
        result, decision, current_status, event_id, status, error_message, reason, source, Path(db_path)
    )
    return result


def get_mode(mode_path: Path = DEFAULT_MODE_PATH) -> str:
    """Read current operation mode from config/mode.txt (shadow / mention / full)."""
    mode_path = Path(mode_path)
    if not mode_path.exists():
        return "shadow"
    content = mode_path.read_text(encoding="utf-8").strip().lower()
    return content if content in VALID_MODES else "shadow"


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
    deadline: Optional[float] = None,
) -> Any:
    """Call a read-only Slack API function with retry on transient connection errors
    and transient SlackApiError (HTTP 429 / 5xx, ratelimited, internal_error,
    service_unavailable, request_timeout). Respects Retry-After on 429.
    Permanent SlackApiError (e.g. channel_not_found) is raised immediately.
    `deadline` (time.monotonic() value; default: the active read_deadline()) bounds
    the whole call: no attempt starts and no backoff is slept past it."""
    active_sleep = sleep_fn if sleep_fn is not None else time.sleep
    last_exc: Optional[BaseException] = None

    def _sleep_within_deadline(delay: float) -> None:
        # With an active read deadline, a backoff that would end at or past the
        # deadline is not taken: the read gives up now (SlackReadDeadlineExceeded,
        # a transient error, so each caller's existing fallback / fail-closed applies).
        remaining = _deadline_remaining(deadline)
        if remaining is not None and delay >= remaining:
            raise SlackReadDeadlineExceeded(
                f"read deadline: {remaining:.1f}s left, next backoff {delay:.1f}s"
            ) from last_exc
        active_sleep(delay)

    for attempt in range(max_attempts):
        remaining = _deadline_remaining(deadline)
        if remaining is not None and remaining <= 0:
            raise SlackReadDeadlineExceeded(
                f"read deadline reached after {attempt} attempt(s)"
            ) from last_exc
        try:
            return func()
        except SlackReadDeadlineExceeded:
            raise
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
                _sleep_within_deadline(delay)
        except TRANSIENT_CONNECTION_ERRORS as exc:
            last_exc = exc
            if attempt + 1 < max_attempts:
                delay = backoffs[attempt] if attempt < len(backoffs) else backoffs[-1]
                _sleep_within_deadline(delay)
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
    deadline_seconds: Optional[float] = None,
) -> Dict[str, Any]:
    """Fetch conversation history from Slack API with up to 5 attempts on connection errors,
    falling back to local SQLite database if all attempts fail with connection errors.
    deadline_seconds (context reads; cmd_history passes CONTEXT_READ_DEADLINE_SECONDS)
    bounds the whole read; when it runs out the result is the local DB fallback with
    fallback_reason="read_deadline"."""
    with read_deadline(deadline_seconds):
        return _fetch_history_unbounded(
            channel=channel,
            thread_ts=thread_ts,
            limit=limit,
            db_path=db_path,
            client=client,
            sleep_fn=sleep_fn,
        )


def _fetch_history_unbounded(
    channel: str,
    thread_ts: Optional[str],
    limit: int,
    db_path: Path,
    client: Optional[Any],
    sleep_fn: Optional[Callable[[float], None]],
) -> Dict[str, Any]:
    try:
        active_client = client if client is not None else get_web_client()

        # Candidate 28 (audit fix 2026-11-23): no TypeError fallback.
        def _do_call() -> Any:
            if thread_ts:
                return active_client.conversations_replies(
                    channel=channel,
                    ts=thread_ts,
                    limit=limit,
                    include_all_metadata=True,
                )
            return active_client.conversations_history(
                channel=channel,
                limit=limit,
                include_all_metadata=True,
            )

        resp = _call_with_retry(
            _do_call,
            max_attempts=HISTORY_RETRY_MAX_ATTEMPTS,
            backoffs=HISTORY_RETRY_BACKOFFS,
            sleep_fn=sleep_fn,
        )
        messages = resp.get("messages", [])
        result = {
            "ok": True,
            "source": "slack_api",
            "channel": channel,
            "thread_ts": thread_ts,
            "count": len(messages),
            "messages": messages,
        }
        try:
            if resp.get("has_more"):
                result["has_more"] = True
        except Exception:
            pass
        return result
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
    except TRANSIENT_CONNECTION_ERRORS as exc:
        messages = get_local_history_fallback(
            channel=channel,
            thread_ts=thread_ts,
            limit=limit,
            db_path=db_path,
        )
        out: Dict[str, Any] = {
            "ok": True,
            "source": "local_db_fallback",
            "channel": channel,
            "thread_ts": thread_ts,
            "count": len(messages),
            "messages": messages,
        }
        if isinstance(exc, SlackReadDeadlineExceeded):
            out["fallback_reason"] = "read_deadline"
        return out


def truncate_text_for_worker(text: Any, limit: int) -> Tuple[str, bool, int]:
    """Cap text at `limit` chars. Returns (text, truncated, original_length).
    A truncated text keeps its head and ends with an explicit omission marker so the
    worker never mistakes it for the whole post."""
    s = "" if text is None else str(text)
    n = len(s)
    if limit <= 0 or n <= limit:
        return s, False, n
    return f"{s[:limit]}…［省略あり：全{n}字中 先頭{limit}字のみ表示］", True, n


def _blocks_plain_text(blocks: Any) -> str:
    """Collect visible text from Slack blocks (rich_text / section / context).
    Used only when a message's top-level `text` is empty."""
    parts: List[str] = []

    def _walk(node: Any) -> None:
        if isinstance(node, dict):
            t = node.get("text")
            if isinstance(t, str) and node.get("type") in ("text", "mrkdwn", "plain_text"):
                parts.append(t)
            elif node.get("type") == "link" and isinstance(node.get("url"), str):
                parts.append(node.get("text") or node["url"])
            for v in node.values():
                if isinstance(v, (dict, list)):
                    _walk(v)
        elif isinstance(node, list):
            for v in node:
                _walk(v)

    _walk(blocks)
    return "".join(parts).strip()


_COMPACT_KEEP_KEYS: Tuple[str, ...] = (
    "user", "username", "bot_id", "subtype", "text", "ts", "thread_ts", "is_bot",
    "reply_count", "metadata",
)


def compact_history_message(msg: Dict[str, Any]) -> Dict[str, Any]:
    """Reduce one Slack message to the fields the worker judges with.
    Drops render-only fields (blocks, bot_profile icons, team, app_id, reply_users, ...).
    Keeps who/when/text/thread/metadata, the bot display name, link-unfurl titles
    (capped) and file names. Text is capped at HISTORY_MESSAGE_TEXT_LIMIT."""
    if not isinstance(msg, dict):
        return msg
    out: Dict[str, Any] = {k: msg[k] for k in _COMPACT_KEEP_KEYS if k in msg}
    if "is_bot" not in out and (msg.get("bot_id") or msg.get("subtype") == "bot_message"):
        out["is_bot"] = 1
    bp = msg.get("bot_profile")
    if isinstance(bp, dict) and bp.get("name"):
        out["bot_name"] = bp["name"]
    if not str(out.get("text") or "").strip() and msg.get("blocks"):
        from_blocks = _blocks_plain_text(msg.get("blocks"))
        if from_blocks:
            out["text"] = from_blocks
            out["text_from_blocks"] = True
    if "text" in out:
        t, truncated, n = truncate_text_for_worker(out["text"], HISTORY_MESSAGE_TEXT_LIMIT)
        out["text"] = t
        if truncated:
            out["text_truncated"] = True
            out["text_length"] = n
    atts = msg.get("attachments")
    if isinstance(atts, list) and atts:
        compact_atts: List[Dict[str, Any]] = []
        for a in atts:
            if not isinstance(a, dict):
                continue
            ca: Dict[str, Any] = {}
            if a.get("title"):
                ca["title"] = truncate_text_for_worker(a["title"], HISTORY_ATTACHMENT_TEXT_LIMIT)[0]
            body = a.get("text") or a.get("pretext") or ("" if a.get("title") else a.get("fallback"))
            if body:
                ca["text"] = truncate_text_for_worker(body, HISTORY_ATTACHMENT_TEXT_LIMIT)[0]
            url = a.get("from_url") or a.get("original_url") or a.get("title_link")
            if url:
                ca["url"] = url
            if ca:
                compact_atts.append(ca)
        if compact_atts:
            out["attachments"] = compact_atts
    files = msg.get("files")
    if isinstance(files, list) and files:
        out["files"] = [
            {k: f[k] for k in ("name", "title", "mimetype") if isinstance(f, dict) and f.get(k)}
            for f in files
            if isinstance(f, dict)
        ]
    return out


def compact_history_result(result: Dict[str, Any]) -> Dict[str, Any]:
    """Bound one history result for the worker.
    1) every message is compacted (compact_history_message);
    2) if the total text exceeds HISTORY_TOTAL_TEXT_BUDGET, the oldest messages are
       dropped first (the thread parent and the newest message are always kept);
    3) replies older than what was returned are counted in `older_omitted` and
       described in `note`, so the worker knows earlier context exists."""
    out = dict(result)
    messages = [compact_history_message(m) for m in (result.get("messages") or [])]
    thread_ts = result.get("thread_ts")

    parent: Optional[Dict[str, Any]] = None
    rest = messages
    if thread_ts and messages and str(messages[0].get("ts")) == str(thread_ts):
        parent, rest = messages[0], messages[1:]

    slack_omitted = 0
    if parent is not None:
        try:
            reply_count = int(parent.get("reply_count") or 0)
        except (TypeError, ValueError):
            reply_count = 0
        slack_omitted = max(0, reply_count - len(rest))

    def _tlen(m: Dict[str, Any]) -> int:
        return len(str(m.get("text") or ""))

    total = sum(_tlen(m) for m in messages)
    budget_omitted = 0
    while total > HISTORY_TOTAL_TEXT_BUDGET and len(rest) > 1:
        dropped = rest.pop(0)
        total -= _tlen(dropped)
        budget_omitted += 1

    messages = ([parent] if parent is not None else []) + rest
    out["messages"] = messages
    out["count"] = len(messages)
    out["text_chars"] = total
    truncated_count = sum(1 for m in messages if m.get("text_truncated"))
    if truncated_count:
        out["text_truncated_count"] = truncated_count

    older = slack_omitted + budget_omitted
    notes: List[str] = []
    if older > 0:
        out["older_omitted"] = older
        notes.append(f"これ以前のスレッド返信 {older} 件は省略（親投稿と直近の返信のみ表示）")
    elif not thread_ts and result.get("has_more"):
        notes.append("これ以前のチャンネル投稿あり（直近分のみ表示）")
    if budget_omitted:
        notes.append(f"うち {budget_omitted} 件は文量上限（合計{HISTORY_TOTAL_TEXT_BUDGET}字）による省略")
    if truncated_count:
        notes.append(f"長文 {truncated_count} 件は先頭{HISTORY_MESSAGE_TEXT_LIMIT}字で切り詰め（省略あり）")
    if notes:
        out["note"] = "。".join(notes)
    return out


def cmd_history(args: argparse.Namespace) -> int:
    try:
        requested = int(args.limit) if args.limit is not None else 20
        limit = max(1, min(requested, HISTORY_MAX_LIMIT))
        output = fetch_history(
            channel=args.channel,
            thread_ts=args.thread_ts,
            limit=limit,
            db_path=Path(args.db),
            deadline_seconds=_env_float(
                "YAMA_CONTEXT_READ_DEADLINE_SEC", CONTEXT_READ_DEADLINE_SECONDS
            ),
        )
        if not getattr(args, "raw", False):
            output = compact_history_result(output)
        if limit != requested:
            output["limit_clamped_from"] = requested
            output["limit"] = limit
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


TRIAGE_LOG_FILENAME = "triage_log.jsonl"
DEFAULT_STATE_FILE_RETENTION_DAYS = 7


def append_triage_log(entry: Any, db_path: Path = DEFAULT_DB_PATH) -> Dict[str, Any]:
    """Append one triage decision to state/triage_log.jsonl (review fix 2026-11-25
    [mid-3], candidate 32). The agents used to append ad hoc; a missing trailing
    newline glued the next record on as `}{` (4 of 764 lines were broken that way).
    Here: the entry must be a JSON object with a non-empty event_id; it is written as
    one compact line under an exclusive flock; if the file does not end with "\\n"
    (a torn earlier write) one is added first; then flush + fsync."""
    import fcntl

    if isinstance(entry, str):
        entry = json.loads(entry)
    if not isinstance(entry, dict):
        raise ValueError("triage-log entry must be a JSON object")
    if not str(entry.get("event_id") or "").strip():
        raise ValueError("triage-log entry needs a non-empty event_id")
    entry = dict(entry)
    entry.setdefault("logged_at", datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
    line = json.dumps(entry, ensure_ascii=False) + "\n"
    path = Path(db_path).parent / TRIAGE_LOG_FILENAME
    path.parent.mkdir(parents=True, exist_ok=True)
    repaired = False
    with open(path, "a+b") as f:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        try:
            f.seek(0, os.SEEK_END)
            if f.tell() > 0:
                f.seek(-1, os.SEEK_END)
                if f.read(1) != b"\n":
                    f.seek(0, os.SEEK_END)
                    f.write(b"\n")
                    repaired = True
            f.seek(0, os.SEEK_END)
            f.write(line.encode("utf-8"))
            f.flush()
            os.fsync(f.fileno())
        finally:
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)
    return {"ok": True, "path": str(path), "event_id": entry["event_id"], "repaired_missing_newline": repaired}


def cmd_triage_log(args: argparse.Namespace) -> int:
    try:
        if args.json is not None and args.file is not None:
            raise ValueError("use only one of --json / --file")
        if args.json is not None:
            raw = args.json
        elif args.file is not None:
            raw = Path(args.file).read_text(encoding="utf-8")
        else:
            raw = sys.stdin.read()
        result = append_triage_log(json.loads(raw), db_path=Path(args.db))
        print(json.dumps(result, ensure_ascii=False))
        return 0
    except Exception as exc:
        print(json.dumps({"ok": False, "error": f"{type(exc).__name__}: {exc}"}, ensure_ascii=False), file=sys.stderr)
        return 1


def prune_state_files(
    days: int = DEFAULT_STATE_FILE_RETENTION_DAYS,
    dry_run: bool = False,
    db_path: Path = DEFAULT_DB_PATH,
    now: Optional[float] = None,
) -> Dict[str, Any]:
    """Delete old working files in state/ (review fix 2026-11-25 [mid-7], candidate 23):
      - reply-<event_id>.txt: only when that event is DONE and was settled (updated_at)
        at least `days` ago, and the file itself was not touched for `days`.
        FAILED / PENDING / PROCESSING / IGNORED / unknown events keep their draft
        (FAILED drafts are evidence for reconcile and a later retry).
      - decisions-*.json (commit-batch inputs): file mtime older than `days`.
    Never touches anything else."""
    if days < 1:
        raise ValueError(f"days must be >= 1, got {days}")
    db_path = Path(db_path)
    state_dir = db_path.parent
    now_ts = time.time() if now is None else float(now)
    cutoff_ts = now_ts - days * 86400
    cutoff_dt = datetime.fromtimestamp(cutoff_ts, tz=timezone.utc)
    deleted_replies: List[str] = []
    deleted_decisions: List[str] = []
    kept_not_done = 0
    errors: List[str] = []

    reply_files = sorted(state_dir.glob("reply-*.txt"))
    statuses: Dict[str, Tuple[str, Any]] = {}
    if reply_files and db_path.exists():
        with get_db_connection(db_path) as conn:
            for f in reply_files:
                eid = f.name[len("reply-"):-len(".txt")]
                row = conn.execute(
                    "SELECT status, updated_at FROM events WHERE event_id = ?", (eid,)
                ).fetchone()
                if row is not None:
                    statuses[eid] = (str(row["status"] or "").upper(), row["updated_at"])
    for f in reply_files:
        eid = f.name[len("reply-"):-len(".txt")]
        status, updated_at = statuses.get(eid, ("", None))
        settled = _parse_reply_created_at(updated_at)
        try:
            old_file = f.stat().st_mtime < cutoff_ts
        except OSError:
            continue
        if status != "DONE" or settled is None or settled >= cutoff_dt or not old_file:
            if status != "DONE":
                kept_not_done += 1
            continue
        if not dry_run:
            try:
                f.unlink()
            except OSError as exc:
                errors.append(f"{f.name}: {exc}")
                continue
        deleted_replies.append(f.name)

    for f in sorted(state_dir.glob("decisions-*.json")):
        try:
            if f.stat().st_mtime >= cutoff_ts:
                continue
            if not dry_run:
                f.unlink()
        except OSError as exc:
            errors.append(f"{f.name}: {exc}")
            continue
        deleted_decisions.append(f.name)

    return {
        "ok": not errors,
        "dry_run": bool(dry_run),
        "days": int(days),
        "deleted_reply_drafts": len(deleted_replies),
        "deleted_decision_files": len(deleted_decisions),
        "kept_reply_drafts_not_done": kept_not_done,
        "errors": errors[:20],
    }


def cmd_prune_state_files(args: argparse.Namespace) -> int:
    try:
        result = prune_state_files(days=args.days, dry_run=bool(args.dry_run), db_path=Path(args.db))
        print(json.dumps(result, ensure_ascii=False))
        return 0 if result["ok"] else 1
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

        # No TypeError fallback (candidate 28): slack_sdk 3.46.0 declares
        # include_all_metadata on conversations_replies and the live API returns
        # metadata with it (checked 2026-11-23). Dropping the flag silently would
        # turn the metadata match into a text-only match, so a TypeError is a bug
        # and must surface instead of being swallowed.
        def _do_call(kw: Dict[str, Any] = replies_kwargs) -> Any:
            return client.conversations_replies(**kw)

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
            print(
                json.dumps(
                    {
                        "ok": False,
                        "warning": "thread_replies_page_limit_reached",
                        "channel": channel,
                        "thread_ts": parent_ts,
                        "max_pages": THREAD_REPLIES_MAX_PAGES,
                    },
                    ensure_ascii=False,
                ),
                file=sys.stderr,
            )
            raise RuntimeError(
                "thread_scan_truncated: thread replies exceed "
                f"{THREAD_REPLIES_MAX_PAGES} pages x {THREAD_REPLIES_PAGE_LIMIT} after the "
                "scan start; refusing to treat thread as fully scanned"
            )
        cursor = next_cursor

    def _ts_key(m: Dict[str, Any]) -> float:
        try:
            return float(m.get("ts", 0))
        except (TypeError, ValueError):
            return 0.0

    yama_messages.sort(key=_ts_key)
    return yama_messages


# Reconcile clears a DONE event's error_message only when it is a leftover of the
# reply / post path (reply, ambiguous, unverifiable and the transport errors that end
# up there). Judgement reasons written at commit time (e.g. "情報のない往復（ループ防止）",
# "既存返信あり（リトライ重複防止）") and forced-correction notes are kept (2026-11-23).
RECONCILE_CLEARABLE_ERROR_RE = re.compile(
    r"reply|ambiguous|unverifiable|incompleteread|max attempts|transient|socket"
    r"|timeout|timed out|connection|http|slack api|error|exception",
    re.IGNORECASE,
)
FORCED_COMMIT_MARKER_RE = re.compile(r"\[forced [A-Z]+->[A-Z]+\]")


def is_reconcile_clearable_error(message: Any) -> bool:
    """True if a DONE event's error_message is a reply/post-path leftover that
    reconcile may clear; False for judgement reasons and forced-correction notes."""
    if message is None:
        return False
    text = str(message)
    if not text.strip() or FORCED_COMMIT_MARKER_RE.search(text):
        return False
    return bool(RECONCILE_CLEARABLE_ERROR_RE.search(text))


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
) -> bool:
    """Save the final post_text sent to chat.postMessage into events.intended_text
    and the attempt timestamp into events.attempt_ts when a reply post results in
    ambiguous_post_outcome.

    Review fix 2026-11-25 [minor-3](c): a row that is already DONE / IGNORED is not
    touched (the settled row keeps its intended_text / attempt_ts / updated_at).
    FAILED rows are still updated so reconcile-replies can find a landed reply and
    failed_with_landed_reply can report it. Returns True when a row was updated."""
    if not event_id:
        return False
    db_path = Path(db_path)
    init_db(db_path)
    parsed_attempt = _parse_attempt_ts(attempt_ts)
    attempt_ts_str = (
        f"{parsed_attempt:.6f}"
        if parsed_attempt is not None
        else f"{time.time():.6f}"
    )
    with get_db_connection(db_path) as conn:
        cur = conn.execute(
            """
            UPDATE events
            SET intended_text = ?,
                attempt_ts = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE event_id = ?
              AND UPPER(COALESCE(status, '')) NOT IN ('DONE', 'IGNORED')
            """,
            (intended_text, attempt_ts_str, event_id),
        )
        conn.commit()
        return cur.rowcount > 0


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


def _failed_landed_alerted_path(db_path: Path) -> Path:
    """Which FAILED-with-landed-reply events were already alerted (one alert per event)."""
    return Path(db_path).parent / FAILED_LANDED_ALERTED_FILENAME


def detect_failed_with_landed_reply(
    db_path: Path = DEFAULT_DB_PATH,
    dry_run: bool = False,
    extra_event_ids: Optional[Set[str]] = None,
) -> Dict[str, Any]:
    """Candidate 27, alternative A (detection only; no status change).
    Finds events whose status is FAILED while the replies ledger already records a
    landed reply for the same event_id. Only FAILED is looked at, so PENDING /
    PROCESSING events (still being worked on) and DONE / IGNORED are never reported.
    The check reads the local DB only, so it is not limited to reconcile's day window
    (a FAILED event stays a double-reply risk for requeue however old it is).

    New findings go to the ops alert outbox as one line per run (kind
    "failed_with_landed_reply"). Already alerted event_ids are kept in a small state
    file beside the DB so the hourly run does not repeat them; an id is dropped from
    that file once the event is no longer FAILED-with-ledger, so a recurrence alerts
    again. dry_run reports what would be alerted without writing the outbox or the
    state file. extra_event_ids: FAILED events this run matched but (dry_run) did not
    write to the ledger; they are reported as if recorded."""
    db_path = Path(db_path)
    _ensure_replies_table(db_path)
    with get_db_connection(db_path) as conn:
        rows = [
            dict(r)
            for r in conn.execute(
                """
                SELECT e.event_id, e.channel_id, e.thread_ts, e.ts, e.error_message,
                       e.updated_at, r.reply_ts, r.created_at AS recorded_at
                FROM events e
                JOIN replies r ON r.event_id = e.event_id
                WHERE e.status = 'FAILED'
                ORDER BY CAST(e.ts AS REAL) ASC, e.rowid ASC
                """
            ).fetchall()
        ]
        if extra_event_ids:
            known = {r["event_id"] for r in rows}
            for eid in sorted(set(extra_event_ids) - known):
                ev = conn.execute(
                    "SELECT event_id, channel_id, thread_ts, ts, error_message, updated_at "
                    "FROM events WHERE event_id = ? AND status = 'FAILED'",
                    (eid,),
                ).fetchone()
                if ev is not None:
                    item = dict(ev)
                    item["reply_ts"] = None
                    item["recorded_at"] = None
                    item["pending_record"] = True
                    rows.append(item)

    found_ids = [str(r["event_id"]) for r in rows]
    state_path = _failed_landed_alerted_path(db_path)
    alerted_before: Dict[str, Any] = {}
    try:
        if state_path.exists():
            loaded = json.loads(state_path.read_text(encoding="utf-8") or "{}")
            if isinstance(loaded, dict):
                alerted_before = loaded
    except Exception:
        alerted_before = {}

    new_rows = [r for r in rows if str(r["event_id"]) not in alerted_before]
    result: Dict[str, Any] = {
        "found": found_ids,
        "new": [str(r["event_id"]) for r in new_rows],
        "alerted": False,
    }
    if dry_run:
        return result

    if new_rows:
        result["alerted"] = _append_ops_alert(
            {
                "kind": "failed_with_landed_reply",
                "source": "reconcile-replies",
                "count": len(new_rows),
                "event_ids": [str(r["event_id"]) for r in new_rows],
                "items": [
                    {
                        "event_id": str(r["event_id"]),
                        "channel": r.get("channel_id"),
                        "thread_ts": r.get("thread_ts"),
                        "reply_ts": r.get("reply_ts"),
                        "error": (str(r.get("error_message") or ""))[:200] or None,
                    }
                    for r in new_rows
                ],
                "note": "status unchanged (FAILED); do not requeue without checking the thread",
                "at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            },
            db_path,
        )
    # Keep ids still FAILED-with-ledger; drop the rest so a recurrence alerts again.
    # Ids from a failed alert write are not stored, so the next run retries them.
    next_state = {
        eid: alerted_before[eid] for eid in found_ids if eid in alerted_before
    }
    if new_rows and result["alerted"]:
        stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        for r in new_rows:
            next_state[str(r["event_id"])] = stamp
    if next_state != alerted_before:
        try:
            tmp_path = state_path.with_suffix(state_path.suffix + ".tmp")
            tmp_path.write_text(
                json.dumps(next_state, ensure_ascii=False, sort_keys=True), encoding="utf-8"
            )
            os.replace(tmp_path, state_path)
        except Exception as exc:
            print(
                f"[reconcile] WARN failed_with_landed_reply state write failed: {exc}",
                file=sys.stderr,
            )
    return result


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
    and clear residual error_message on DONE events whose thread scan completes.
    Finally, FAILED events that have a landed reply in the ledger are reported and
    alerted once (detect_failed_with_landed_reply); their status is left unchanged."""
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
    # Only reply/post-path leftovers are candidates; judgement reasons stay as written.
    done_error_candidates: List[Dict[str, Any]] = [
        dict(r)
        for r in raw_done_error_rows
        if _is_within_window(r) and is_reconcile_clearable_error(r["error_message"])
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

    # Candidate 27, alternative A: detect FAILED events that have a landed reply in the
    # ledger and alert once. Status is never changed here (terminal guard kept).
    failed_candidate_ids = {
        str(c["event_id"]) for c in candidates if str(c.get("status") or "").upper() == "FAILED"
    }
    dry_run_extra = (
        {r["event_id"] for r in recorded if r["event_id"] in failed_candidate_ids}
        if dry_run
        else None
    )
    try:
        failed_with_landed = detect_failed_with_landed_reply(
            db_path=db_path, dry_run=dry_run, extra_event_ids=dry_run_extra
        )
    except Exception as exc:
        failed_with_landed = {"error": str(exc)}
        error_details.append({"event_id": "*failed_with_landed_reply*", "error": str(exc)})

    return {
        "ok": True,
        "dry_run": bool(dry_run),
        "days": int(days),
        "checked": len(candidates),
        "recorded": recorded,
        "not_found": not_found,
        "done_error_cleared": done_error_cleared,
        "failed_with_landed_reply": failed_with_landed,
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


def cmd_event_text(args: argparse.Namespace) -> int:
    """Full text of one event. The dispatcher caps event text in payload.json
    (text_truncated / text_length); this is how the worker reads the rest."""
    row = get_event_by_id(args.event_id, db_path=Path(args.db))
    if row is None:
        print(json.dumps({"ok": False, "error": "event_not_found", "event_id": args.event_id},
                         ensure_ascii=False), file=sys.stderr)
        return 1
    text = row.get("text") or ""
    print(json.dumps({"ok": True, "event_id": args.event_id, "text_length": len(text), "text": text},
                     ensure_ascii=False, indent=2))
    return 0


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
    """Answered check over thread messages, matching the hook definition
    (slack-yama-dispatch.json: "a post with body text by someone other than the
    questioner and Yama, bots included"; review fix 2026-11-25 [mid-1]).
    Returns the first message strictly after parent_ts that satisfies ALL of:
      1. Poster (user or bot_id) is neither the original questioner nor Yama itself.
         Bot posts (including subtype="bot_message") count as answers (bot_ts_set
         is accepted for compatibility but no longer excludes anything).
      2. subtype is None, file_share, thread_broadcast, or bot_message
      3. Stripped body text length > min_text_length (default 0, i.e. at least
         one non-whitespace character)"""
    del bot_ts_set  # kept in the signature for callers; bots are not excluded
    effective_questioners = set(questioner_user_ids)
    for msg in messages:
        if not isinstance(msg, dict):
            continue
        if str(msg.get("ts") or "") == str(parent_ts):
            parent_user = str(msg.get("user") or msg.get("bot_id") or "").strip()
            # The poster of the parent is the questioner even when it is a bot.
            if parent_user and parent_user not in yama_user_ids:
                effective_questioners.add(parent_user)

    for msg in messages:
        if not isinstance(msg, dict):
            continue
        msg_ts = msg.get("ts")
        if not msg_ts or not _is_ts_after(msg_ts, parent_ts):
            continue
        subtype = msg.get("subtype")
        if subtype is not None and subtype not in ("file_share", "thread_broadcast", "bot_message"):
            continue
        msg_user = str(msg.get("user") or msg.get("bot_id") or "").strip()
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
      2. If is_delayed_retry is True, whether a qualifying answer from another user (including other bots) exists
    For delayed retries the scan is capped at a single page of ANSWERED_CHECK_PAGE_LIMIT
    replies; if more replies remain beyond that page and no qualifying answer was found,
    RuntimeError is raised so the caller fails closed (pre_post_check_unverifiable).
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
    single_page_cap = is_delayed_retry
    page_limit = ANSWERED_CHECK_PAGE_LIMIT if single_page_cap else THREAD_REPLIES_PAGE_LIMIT
    max_pages = 1 if single_page_cap else THREAD_REPLIES_MAX_PAGES
    scan_truncated = False

    for page in range(1, max_pages + 1):
        replies_kwargs: Dict[str, Any] = {
            "channel": channel,
            "ts": thread_ts,
            "oldest": scan_oldest,
            "limit": page_limit,
            "include_all_metadata": True,
        }
        if cursor:
            replies_kwargs["cursor"] = cursor

        # No TypeError fallback (candidate 28): see _scan_thread_yama_messages.
        def _do_page_call(kw: Dict[str, Any] = replies_kwargs) -> Any:
            return active_client.conversations_replies(**kw)

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
        if single_page_cap:
            scan_truncated = True
            break
        if page >= max_pages:
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
            raise RuntimeError(
                "thread_scan_truncated: thread replies exceed "
                f"{THREAD_REPLIES_MAX_PAGES} pages x {THREAD_REPLIES_PAGE_LIMIT} after the "
                "scan start and no own reply was found in the checked range; "
                "refusing to post (fail-closed)"
            )
        cursor = next_cursor

    if not is_delayed_retry:
        return None, None

    questioner_uids: Set[str] = set()
    if questioner_user_id and str(questioner_user_id).strip():
        questioner_uids.add(str(questioner_user_id).strip())

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
        min_text_length=ANSWERED_MIN_TEXT_LENGTH,
    )
    if answered_msg is not None:
        return None, answered_msg
    if scan_truncated:
        raise RuntimeError(
            "answered_check_incomplete: more replies exist beyond the first "
            f"{ANSWERED_CHECK_PAGE_LIMIT} after the scan start and no qualifying "
            "answer was found in the checked range; refusing the delayed resend (fail-closed)"
        )
    return None, None


def _reply_guard_alert(kind: str, fields: Dict[str, Any], db_path: Path) -> None:
    entry: Dict[str, Any] = {
        "kind": kind,
        "source": "reply",
        "at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    entry.update({k: v for k, v in fields.items() if v is not None})
    _append_ops_alert(entry, db_path)


def check_reply_event_id_choice(
    event_id_arg: Optional[str],
    no_event_id: bool,
    force: bool,
    reason: Optional[str],
    channel: Optional[str],
    db_path: Path,
) -> Tuple[Optional[str], Optional[Dict[str, Any]], bool]:
    """Event-id guard for reply (candidate 28). Returns (event_id, error, forced).

    Exactly one of --event-id <ID> / --no-event-id must be given (forgotten id guard).
    A given --event-id must exist in the events table (typo guard); --force --reason
    lets an unknown id through. --no-event-id posts carry no event_id at all, so they
    are never recorded in the replies ledger. A refusal returns an error dict (caller
    prints it and exits 1, nothing is read or posted) and leaves one ops alert line."""
    has_id_flag = event_id_arg is not None
    event_id = str(event_id_arg).strip() if has_id_flag else None

    def _refuse(code: str, message: str, **extra: Any) -> Tuple[None, Dict[str, Any], bool]:
        _reply_guard_alert(
            "reply_rejected",
            {"reason": code, "event_id": event_id or None, "channel": channel, **extra},
            db_path,
        )
        err: Dict[str, Any] = {"ok": False, "error": code, "message": message}
        if event_id:
            err["event_id"] = event_id
        for k, v in extra.items():
            if v is not None and k not in err:
                err[k] = v
        return None, err, False

    if has_id_flag and no_event_id:
        return _refuse(
            "event_id_choice_conflict",
            "--event-id and --no-event-id are mutually exclusive; give exactly one.",
        )
    if not has_id_flag and not no_event_id:
        return _refuse(
            "event_id_choice_missing",
            "reply requires --event-id <ID> (reply to an event) or --no-event-id "
            "(share / report post not tied to an event).",
        )
    if no_event_id:
        if force or reason is not None:
            return _refuse(
                "force_without_event_id",
                "--force / --reason only apply to --event-id; drop them for --no-event-id.",
            )
        return None, None, False
    if not event_id:
        return _refuse(
            "event_id_empty",
            "--event-id was given an empty value; pass the real event_id or use --no-event-id.",
        )
    if force and not (reason and str(reason).strip()):
        return _refuse(
            "force_requires_reason",
            "--force requires a non-empty --reason.",
        )
    event_row = get_event_by_id(event_id, db_path=db_path)
    if event_row is not None:
        cur_status = str(event_row.get("status") or "").upper()
        if (
            cur_status in TERMINAL_STATUSES
            and get_recorded_reply(event_id, db_path=db_path) is None
        ):
            if not force:
                return _refuse(
                    "event_already_terminal",
                    f"event_id {event_id} is already in terminal status {cur_status} "
                    "with no recorded reply. Nothing was posted. "
                    "To post anyway use --force --reason <text>.",
                    current_status=cur_status,
                )
            _reply_guard_alert(
                "reply_forced",
                {
                    "reason": str(reason).strip()[:300],
                    "event_id": event_id,
                    "channel": channel,
                    "forced_check": "event_already_terminal",
                    "current_status": cur_status,
                },
                db_path,
            )
            return event_id, None, True
        return event_id, None, False
    if not force:
        return _refuse(
            "unknown_event_id",
            f"event_id {event_id} is not in the events table (typo?). Nothing was posted. "
            "Check the id; to post anyway use --force --reason <text>.",
        )
    _reply_guard_alert(
        "reply_forced",
        {
            "reason": str(reason).strip()[:300],
            "event_id": event_id,
            "channel": channel,
            "forced_check": "unknown_event_id",
        },
        db_path,
    )
    return event_id, None, True


def cmd_reply(args: argparse.Namespace) -> int:
    db_path = Path(getattr(args, "db", DEFAULT_DB_PATH))
    mode_path = Path(getattr(args, "mode_file", DEFAULT_MODE_PATH))
    if get_mode(mode_path) == "shadow":
        _reply_guard_alert(
            "reply_rejected",
            {
                "reason": "shadow_mode_blocked",
                "event_id": getattr(args, "event_id", None),
                "channel": getattr(args, "channel", None),
            },
            db_path,
        )
        print(
            json.dumps(
                {
                    "ok": False,
                    "error": "shadow_mode_blocked",
                    "message": "Current mode is shadow; posting to Slack is disabled.",
                },
                ensure_ascii=False,
            ),
            file=sys.stderr,
        )
        return 1

    # A forced unknown/terminal event_id is recorded as a reply_forced ops alert
    # (stderr stays reserved for the single JSON error object callers parse).
    event_id, guard_err, _forced_unknown_event = check_reply_event_id_choice(
        event_id_arg=getattr(args, "event_id", None),
        no_event_id=bool(getattr(args, "no_event_id", False)),
        force=bool(getattr(args, "force", False)),
        reason=getattr(args, "reason", None),
        channel=getattr(args, "channel", None),
        db_path=db_path,
    )
    if guard_err is not None:
        print(json.dumps(guard_err, ensure_ascii=False), file=sys.stderr)
        return 1

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

    event_row = get_event_by_id(event_id, db_path=db_path) if event_id else None
    if event_id and event_row is not None and not args.thread_ts:
        expected_thread_ts = str(event_row.get("thread_ts") or event_row.get("ts") or "").strip()
        if expected_thread_ts:
            print(
                json.dumps(
                    {
                        "warning": "missing_thread_ts_for_event_reply",
                        "event_id": event_id,
                        "expected_thread_ts": expected_thread_ts,
                    },
                    ensure_ascii=False,
                ),
                file=sys.stderr,
            )
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
    # Safety-read deadline (stall fix proposal 3): counted back from the time the
    # Reaper may reclaim this event. Too little time left for a thread reply ->
    # refuse without reading (fail-closed, same pre_post_check_unverifiable path).
    # Channel posts keep their fail-open pre-check and get at least the minimum budget.
    safety_min = _env_float("YAMA_SAFETY_READ_MIN_SEC", SAFETY_READ_MIN_SECONDS)
    safety_budget = safety_read_budget_seconds(event_row)
    if not args.thread_ts:
        safety_budget = max(safety_budget, safety_min)
    try:
        if safety_budget < safety_min:
            raise SlackReadDeadlineExceeded(
                f"safety_read_deadline: only {safety_budget:.0f}s left before the "
                "Reaper may reclaim this event; refusing to post without the "
                "pre-post check (fail-closed)"
            )
        read_client = get_web_client()
        with read_deadline(safety_budget):
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
        commit_rejected: Optional[str] = None
        if event_id and event_row is not None:
            # The terminal guard may reject this commit (the event was already settled
            # elsewhere). Rejections stay non-fatal here, but the output reports the
            # event's actual status instead of assuming DONE.
            try:
                committed = commit_event(
                    event_id=event_id,
                    status="DONE",
                    db_path=db_path,
                    source="reply_skipped_answered",
                )
                final_event_status = str(committed.get("status") or final_event_status)
                if committed.get("rejected"):
                    commit_rejected = str(committed["rejected"])
            except Exception as commit_exc:
                print(
                    f"[reply] WARN skipped_answered commit failed for {event_id}: {commit_exc}",
                    file=sys.stderr,
                )
                try:
                    current = get_event_by_id(event_id, db_path=db_path)
                    if current and current.get("status"):
                        final_event_status = str(current["status"])
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
        if commit_rejected:
            output["commit_rejected"] = commit_rejected
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
        # Recovery is a safety read: bounded like the pre-post check (at least the
        # minimum budget). If it runs out the outcome stays ambiguous_post_outcome
        # (no resend), exactly as for any other recovery failure.
        rec_budget = max(safety_read_budget_seconds(event_row), safety_min)
        try:
            with read_deadline(rec_budget):
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

        intended_saved: Optional[bool] = None
        if event_id:
            try:
                intended_saved = _save_event_intended_text(
                    event_id=event_id,
                    intended_text=post_text,
                    db_path=db_path,
                    attempt_ts=attempt_epoch,
                )
            except Exception as save_exc:
                # review fix 2026-11-25 [minor-3](c): no silent discard. Without
                # intended_text, reconcile can only match by metadata, so say so.
                intended_saved = False
                print(
                    f"[reply] WARN intended_text_save_failed: event {event_id}: {save_exc}",
                    file=sys.stderr,
                )

        ambiguous_err: Dict[str, Any] = {
            "ok": False,
            "error": "ambiguous_post_outcome",
            "channel": args.channel,
            "thread_ts": args.thread_ts,
            "post_error": str(exc),
        }
        if event_id:
            ambiguous_err["event_id"] = event_id
            ambiguous_err["intended_text_saved"] = bool(intended_saved)
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
        # A Bot ID (B...) or a token (xox...) in the user-ID slot is a
        # configuration mistake: Slack user IDs start with U (or W on
        # Enterprise Grid). Refuse loudly instead of silently falling back
        # to another ID, whose intent we cannot know. The offending value
        # is never included in the message (it could be a token).
        upper_uid = env_uid.upper()
        if upper_uid.startswith("B") or upper_uid.startswith("XOX"):
            env_name = (
                "YAMA_BOT_USER_ID"
                if os.environ.get("YAMA_BOT_USER_ID", "").strip()
                else "SLACK_BOT_USER_ID"
            )
            raise ValueError(
                f"{env_name} に Bot ID またはトークンらしき値が設定されています。"
                "Bot ユーザーID（U始まり／W始まり）を設定してください。"
            )
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

            # Candidate 28 (audit fix 2026-11-23): no TypeError fallback. The
            # production SDK accepts include_all_metadata; silently dropping it
            # (or the oldest/latest/cursor window) would downgrade the metadata
            # match to a text match without anyone noticing.
            def _do_page_call(kw: Dict[str, Any] = replies_kwargs) -> Any:
                return active_client.conversations_replies(**kw)

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
                # Candidate 14 (option A, owner-delegated): a thread scan cut off
                # at the page cap cannot prove that no earlier own reply exists,
                # so fail closed like the delayed-resend path. cmd_reply turns
                # this into pre_post_check_unverifiable (no post; stepped wait, 24h
                # terminal); the post-error recovery path reports it as
                # recovery_error under ambiguous_post_outcome (no resend).
                raise RuntimeError(
                    "thread_scan_truncated: thread replies exceed "
                    f"{THREAD_REPLIES_MAX_PAGES} pages x {THREAD_REPLIES_PAGE_LIMIT} after the "
                    "scan start and no own reply was found in the checked range; "
                    "refusing to post (fail-closed)"
                )
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

    # Candidate 28 (audit fix 2026-11-23): no TypeError fallback (it also
    # dropped the oldest/latest window).
    def _do_history_call(kw: Dict[str, Any] = history_kwargs) -> Any:
        return active_client.conversations_history(**kw)

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
    mode_path = Path(getattr(args, "mode_file", DEFAULT_MODE_PATH))
    if get_mode(mode_path) == "shadow":
        print(
            json.dumps(
                {
                    "ok": False,
                    "error": "shadow_mode_blocked",
                    "message": "Current mode is shadow; reacting on Slack is disabled.",
                },
                ensure_ascii=False,
            ),
            file=sys.stderr,
        )
        return 1
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


def _rejection_message(event_id: str, current: Any, requested: Any) -> str:
    return (
        f"Event '{event_id}' is already {current}; {requested} was not applied "
        "(settled by another run). Do not retry; mention it in the report. "
        "Corrections: --force --reason <text>. FAILED -> retry: requeue."
    )


def cmd_commit(args: argparse.Namespace) -> int:
    try:
        updated = commit_event(
            event_id=args.event_id,
            status=args.status,
            error_message=args.error,
            db_path=Path(args.db),
            force=bool(getattr(args, "force", False)),
            reason=getattr(args, "reason", None),
            source="commit",
        )
        if updated.get("rejected"):
            out = {
                "ok": False,
                "error": updated["rejected"],
                "noop": True,
                "event_id": args.event_id,
                "current_status": updated.get("status"),
                "requested_status": updated.get("requested_status"),
                "message": _rejection_message(
                    args.event_id, updated.get("status"), updated.get("requested_status")
                ),
            }
            print(json.dumps(out, ensure_ascii=False), file=sys.stderr)
            return 1
        out_ok: Dict[str, Any] = {"ok": True, "event": updated}
        if updated.get("noop"):
            out_ok["noop"] = True
        if updated.get("forced"):
            out_ok["forced"] = True
        print(json.dumps(out_ok, ensure_ascii=False, indent=2))
        return 0
    except Exception as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 1


def _load_batch_decisions(path: Path) -> List[Dict[str, Any]]:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    if isinstance(data, dict) and isinstance(data.get("decisions"), list):
        data = data["decisions"]
    if not isinstance(data, list) or not data:
        raise ValueError("decisions file must be a non-empty JSON array (or {\"decisions\": [...]})")
    return data


def commit_batch(
    decisions: List[Dict[str, Any]],
    db_path: Path = DEFAULT_DB_PATH,
) -> Dict[str, Any]:
    """Commit several decisions in one call (stall fix proposal 5: decide all events
    first, commit the no-reply ones together, then spend the time on replies).
    Each item is {"event_id", "status": DONE|IGNORED|FAILED, "error"?, "force"?,
    "reason"?}. Validation is all-or-nothing: if any item is malformed, duplicated,
    unknown, or would be rejected by the terminal guard (DONE/IGNORED -> another
    status, FAILED -> anything, without force+reason), nothing is committed. A
    same-status item on DONE/IGNORED passes validation and becomes a no-op. Each valid
    item then goes through the commit_event core (same semantics as `commit`,
    including the unverifiable wait for FAILED), all in one transaction: the status
    is re-checked inside it, and if any item is rejected there (changed since
    validation) or fails, the whole batch is rolled back ("rolled_back": true)."""
    problems: List[str] = []
    seen: Set[str] = set()
    terminal_rejects: List[Dict[str, Any]] = []
    normalized: List[Tuple[str, str, Optional[str], bool, Optional[str]]] = []
    for i, item in enumerate(decisions):
        if not isinstance(item, dict):
            problems.append(f"item {i}: not an object")
            continue
        eid = str(item.get("event_id") or "").strip()
        status = str(item.get("status") or "").strip().upper()
        err = item.get("error")
        force = item.get("force") is True
        reason = item.get("reason")
        reason = None if reason is None else str(reason)
        if not eid:
            problems.append(f"item {i}: event_id missing")
            continue
        if status not in VALID_COMMIT_STATUSES:
            problems.append(f"item {i} ({eid}): invalid status {item.get('status')!r}")
            continue
        if eid in seen:
            problems.append(f"item {i} ({eid}): duplicated event_id")
            continue
        if force and not (reason and reason.strip()):
            problems.append(f"item {i} ({eid}): force requires a non-empty reason")
            continue
        current = get_event_by_id(eid, db_path=db_path)
        if current is None:
            problems.append(f"item {i} ({eid}): event not found")
            continue
        cur_status = str(current.get("status") or "").upper()
        if classify_commit_transition(cur_status, status, force=force) == "reject":
            problems.append(
                f"item {i} ({eid}): {COMMIT_REJECTED_CODE} (current {cur_status}, requested {status})"
            )
            terminal_rejects.append(
                {"event_id": eid, "current_status": cur_status, "requested_status": status}
            )
            continue
        seen.add(eid)
        normalized.append((eid, status, None if err is None else str(err), force, reason))
    if problems:
        if terminal_rejects:
            print(
                f"[commit-batch] WARN {COMMIT_REJECTED_CODE}: {len(terminal_rejects)} item(s) "
                "already terminal; nothing committed",
                file=sys.stderr,
            )
            _append_ops_alert(
                {
                    "kind": "commit_rejected",
                    "reason": COMMIT_REJECTED_CODE,
                    "source": "commit-batch-validation",
                    "batch_size": len(decisions),
                    "rejected": terminal_rejects[:50],
                    "at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                },
                Path(db_path),
            )
        out: Dict[str, Any] = {"ok": False, "committed": 0, "errors": problems}
        if terminal_rejects:
            out[COMMIT_REJECTED_CODE] = [r["event_id"] for r in terminal_rejects]
        return out

    # Write phase (review fix 2026-11-25 [minor-3](d)): all items in ONE
    # BEGIN IMMEDIATE transaction, so the batch is all-or-nothing on write too.
    # If any item is rejected by the in-transaction re-check (changed since
    # validation) or raises, everything is rolled back and nothing is committed.
    # Side effects (stderr / ops alerts) run only after the commit.
    try:
        prepared = [
            (eid, status, err, force, reason,
             _resolve_commit_params(status, force, reason, None, None, None))
            for eid, status, err, force, reason in normalized
        ]
    except Exception as exc:
        return {"ok": False, "committed": 0, "errors": [str(exc)]}

    results: List[Dict[str, Any]] = []
    outcomes: List[Tuple[Dict[str, Any], str, str, str, str, Optional[str], Optional[str]]] = []
    failures = 0
    noops = 0
    rolled_back = False
    init_db(db_path)
    with get_db_connection(db_path) as conn:
        conn.execute("BEGIN IMMEDIATE;")
        for eid, status, err, force, reason, params in prepared:
            try:
                updated, decision, cur = _commit_event_in_tx(
                    conn, eid, params, err, MAX_RETRIES, force, reason
                )
            except Exception as exc:
                failures += 1
                results.append({"event_id": eid, "ok": False, "error": str(exc)})
                continue
            outcomes.append((updated, decision, cur, eid, status, err, reason))
            if decision == "reject":
                failures += 1
                results.append(
                    {
                        "event_id": eid,
                        "ok": False,
                        "error": updated["rejected"],
                        "noop": True,
                        "status": updated.get("status"),
                    }
                )
                continue
            entry: Dict[str, Any] = {"event_id": eid, "ok": True, "status": updated.get("status")}
            if decision == "noop":
                entry["noop"] = True
                noops += 1
            if decision == "force":
                entry["forced"] = True
            results.append(entry)
        if failures:
            conn.rollback()
            rolled_back = True
        else:
            conn.commit()

    if rolled_back:
        # Report only the rejections (the rows really are terminal); forced or
        # applied items were rolled back, so they write no alert.
        for updated, decision, cur, eid, status, err, reason in outcomes:
            if decision == "reject":
                _report_commit_outcome(
                    updated, decision, cur, eid, status, err, reason, "commit-batch", Path(db_path)
                )
        for entry in results:
            if entry.get("ok"):
                entry["ok"] = False
                entry["rolled_back"] = True
                entry.pop("forced", None)
                entry.pop("noop", None)
        return {
            "ok": False,
            "committed": 0,
            "noop": 0,
            "failed": failures,
            "rolled_back": True,
            "results": results,
        }

    for updated, decision, cur, eid, status, err, reason in outcomes:
        _report_commit_outcome(
            updated, decision, cur, eid, status, err, reason, "commit-batch", Path(db_path)
        )
    return {
        "ok": True,
        "committed": len(results) - noops,
        "noop": noops,
        "failed": 0,
        "results": results,
    }


def cmd_commit_batch(args: argparse.Namespace) -> int:
    try:
        decisions = _load_batch_decisions(Path(args.file))
        out = commit_batch(decisions, db_path=Path(args.db))
    except Exception as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 1
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0 if out.get("ok") else 1


SEED_MIRROR_DEFAULT_DAYS: int = 3
SEED_MIRROR_THREAD_DEADLINE_SECONDS: float = 60.0


def seed_mirror(
    days: float = SEED_MIRROR_DEFAULT_DAYS,
    db_path: Path = DEFAULT_DB_PATH,
    client: Optional[Any] = None,
    dry_run: bool = False,
    pause_seconds: float = 1.0,
    max_threads: int = 100,
    sleep_fn: Optional[Callable[[float], None]] = None,
    now: Optional[float] = None,
) -> Dict[str, Any]:
    """One-off, read-only fill of the local thread mirror for threads that started
    before own-post mirroring (context bundling marks those incomplete). For each
    thread with events in the last `days`, read the whole thread
    (conversations.replies, paged), store the messages that are not in events
    (thread parents, Yama's own posts, ...) into mirror_messages, the current text
    of edited messages into message_edits, and mark the thread in mirror_threads.
    A thread whose read fails or hits the page cap is not marked (stays incomplete).
    Requires the listener's own-post mirror to be active first (mirror_meta), so no
    own post can fall between the seed read and the listener."""
    from listener import (
        MIRROR_SINCE_KEY,
        _init_mirror_tables,
        record_message_edit,
        record_mirror_message,
    )

    db_path = Path(db_path)
    init_db(db_path)
    active_sleep = sleep_fn if sleep_fn is not None else time.sleep
    current = now if now is not None else time.time()
    with get_db_connection(db_path) as conn:
        _init_mirror_tables(conn)
        conn.commit()
        since_row = conn.execute(
            "SELECT value FROM mirror_meta WHERE key = ?", (MIRROR_SINCE_KEY,)
        ).fetchone()
        threads = conn.execute(
            """
            SELECT channel_id, COALESCE(thread_ts, ts) AS thread_key,
                   MAX(CAST(ts AS REAL)) AS last_ts
            FROM events
            WHERE CAST(ts AS REAL) > ?
            GROUP BY channel_id, thread_key
            ORDER BY last_ts DESC
            LIMIT ?
            """,
            (current - float(days) * 86400.0, int(max_threads)),
        ).fetchall()
        threads = [(r["channel_id"], str(r["thread_key"])) for r in threads]
        event_ts: Dict[str, Set[str]] = {}
        for ch, _ in threads:
            if ch not in event_ts:
                event_ts[ch] = {
                    str(r["ts"])
                    for r in conn.execute(
                        "SELECT ts FROM events WHERE channel_id = ?", (ch,)
                    ).fetchall()
                }
    if since_row is None:
        return {
            "ok": False,
            "error": "own-post mirror is not active yet (restart the listener first)",
            "threads": len(threads),
        }
    summary: Dict[str, Any] = {
        "ok": True,
        "dry_run": dry_run,
        "threads": len(threads),
        "seeded": 0,
        "mirrored_messages": 0,
        "edits": 0,
        "failed": [],
        "truncated": [],
    }
    if dry_run:
        summary["thread_keys"] = [f"{c}:{t}" for c, t in threads]
        return summary
    active_client = client if client is not None else get_web_client()
    for idx, (channel, thread_key) in enumerate(threads):
        if idx and pause_seconds > 0:
            active_sleep(pause_seconds)
        messages: List[Dict[str, Any]] = []
        truncated = False
        try:
            with read_deadline(SEED_MIRROR_THREAD_DEADLINE_SECONDS):
                cursor: Optional[str] = None
                for page in range(1, THREAD_REPLIES_MAX_PAGES + 1):
                    kw: Dict[str, Any] = {
                        "channel": channel,
                        "ts": thread_key,
                        "limit": THREAD_REPLIES_PAGE_LIMIT,
                    }
                    if cursor:
                        kw["cursor"] = cursor
                    resp = _call_with_retry(
                        lambda kw=kw: active_client.conversations_replies(**kw),
                        sleep_fn=sleep_fn,
                    )
                    messages.extend(m for m in (resp.get("messages") or []) if isinstance(m, dict))
                    meta = resp.get("response_metadata") or {}
                    nxt = (meta.get("next_cursor") or "").strip() if isinstance(meta, dict) else ""
                    if not nxt or resp.get("has_more") is False:
                        break
                    if page >= THREAD_REPLIES_MAX_PAGES:
                        truncated = True
                        break
                    cursor = nxt
        except Exception as exc:
            summary["failed"].append({"thread": f"{channel}:{thread_key}", "error": str(exc)[:200]})
            continue
        if truncated:
            summary["truncated"].append(f"{channel}:{thread_key}")
            continue
        known = event_ts.get(channel, set())
        for m in messages:
            ts = str(m.get("ts") or "")
            if not ts:
                continue
            if m.get("edited"):
                record_message_edit(channel, ts, m.get("text"), db_path=db_path)
                summary["edits"] += 1
            if ts in known:
                continue
            record_mirror_message(
                channel,
                ts,
                m.get("thread_ts") or thread_key,
                m.get("user") or m.get("bot_id"),
                _is_bot_message(m) or m.get("user") == YAMA_BOT_USER_ID,
                m.get("text"),
                source="seed",
                db_path=db_path,
            )
            summary["mirrored_messages"] += 1
        with get_db_connection(db_path) as conn:
            conn.execute(
                """
                INSERT INTO mirror_threads (channel_id, thread_ts, seeded_at, message_count)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(channel_id, thread_ts) DO UPDATE SET
                    seeded_at = excluded.seeded_at, message_count = excluded.message_count
                """,
                (channel, thread_key, time.time(), len(messages)),
            )
            conn.commit()
        summary["seeded"] += 1
    summary["ok"] = not summary["failed"]
    return summary


def cmd_seed_mirror(args: argparse.Namespace) -> int:
    out = seed_mirror(
        days=float(args.days),
        db_path=Path(args.db),
        dry_run=bool(args.dry_run),
        pause_seconds=float(args.pause),
        max_threads=int(args.max_threads),
    )
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0 if out.get("ok") else 1


class RequeueBlocked(ValueError):
    """requeue refused because the replies ledger already holds a landed reply for the
    event (candidate 27, alternative A). Carries the ledger row for the CLI output."""

    def __init__(
        self, event_id: str, ledger_row: Dict[str, Any], alert_written: bool = True
    ) -> None:
        self.event_id = event_id
        self.ledger_row = dict(ledger_row)
        self.alert_written = bool(alert_written)
        super().__init__(
            f"Event '{event_id}' is FAILED but the replies ledger already records a landed "
            f"reply (channel={ledger_row.get('channel_id')}, reply_ts={ledger_row.get('reply_ts')}). "
            "Requeueing it would put an already answered event back in the queue. "
            "Check the thread first; if a requeue is still needed, run "
            "requeue --event-id <id> --force --reason <text>."
        )


def _forced_requeue_error_message(
    reply_ts: Any, reason: str, error_message: Optional[str]
) -> str:
    marker = f"[forced requeue: reply already landed ts={reply_ts}] {reason.strip()}"
    return f"{error_message} {marker}" if error_message else marker


def requeue_event(
    event_id: str,
    db_path: Path = DEFAULT_DB_PATH,
    force: bool = False,
    reason: Optional[str] = None,
) -> Dict[str, Any]:
    """Manually re-queue a FAILED event back to PENDING (operator action).
    This is the only path that resets reap_count to 0 (candidate 12). retry_count,
    error_message and first_unverifiable_at are deliberately left unchanged so the
    existing retry / 24h-unverifiable rules keep their history; next_retry_at is
    cleared so the event is dispatched on the next 30s cycle.

    Landed-reply guard (candidate 27, alternative A): if the replies ledger already has
    a row for this event_id, the requeue is refused (RequeueBlocked, row unchanged, one
    ops alert line "requeue_blocked"). force=True with a non-empty reason overrides it
    (same form as the commit terminal-guard correction): the reason is appended to
    error_message and one ops alert line "requeue_forced" is written. The ledger row is
    kept, so a later reply --event-id for this event is deduplicated, not re-posted.
    The status check and the ledger check run in the same BEGIN IMMEDIATE transaction."""
    if force and not (reason and str(reason).strip()):
        raise ValueError("force requires a non-empty reason (--reason)")
    init_db(db_path)
    _ensure_replies_table(Path(db_path))
    forced_reply_ts: Optional[str] = None
    with get_db_connection(db_path) as conn:
        conn.execute("BEGIN IMMEDIATE;")
        row = conn.execute(
            "SELECT status, error_message FROM events WHERE event_id = ?", (event_id,)
        ).fetchone()
        if not row:
            conn.rollback()
            raise LookupError(f"Event '{event_id}' not found in database")
        if str(row["status"]).upper() != "FAILED":
            conn.rollback()
            raise ValueError(
                f"Event '{event_id}' is {row['status']}, not FAILED; requeue only re-queues FAILED events"
            )
        ledger = conn.execute(
            "SELECT event_id, channel_id, thread_ts, reply_ts, created_at FROM replies WHERE event_id = ?",
            (event_id,),
        ).fetchone()
        new_error = row["error_message"]
        if ledger is not None:
            ledger_row = dict(ledger)
            if not force:
                conn.rollback()
                # Audit fix 2026-11-23 (#8): no separate "[requeue] WARN" line. The
                # same facts are in the requeue_blocked ops alert below and in the
                # single stderr JSON that cmd_requeue prints (stderr = one JSON).
                alert_ok = _append_ops_alert(
                    {
                        "kind": "requeue_blocked",
                        "reason": "reply_already_landed",
                        "event_id": event_id,
                        "current_status": "FAILED",
                        "requested_status": "PENDING",
                        "reply_ts": ledger_row.get("reply_ts"),
                        "channel": ledger_row.get("channel_id"),
                        "thread_ts": ledger_row.get("thread_ts"),
                        "source": "requeue",
                        "at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    },
                    Path(db_path),
                    quiet=True,
                )
                raise RequeueBlocked(event_id, ledger_row, alert_written=alert_ok)
            forced_reply_ts = str(ledger_row.get("reply_ts"))
            new_error = _forced_requeue_error_message(
                forced_reply_ts, str(reason), row["error_message"]
            )
        conn.execute(
            """
            UPDATE events
            SET status = 'PENDING',
                reap_count = 0,
                next_retry_at = NULL,
                error_message = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE event_id = ? AND status = 'FAILED'
            """,
            (new_error, event_id),
        )
        conn.commit()
        updated = conn.execute(
            """
            SELECT event_id, status, retry_count, reap_count, error_message,
                   next_retry_at, first_unverifiable_at, updated_at
            FROM events WHERE event_id = ?
            """,
            (event_id,),
        ).fetchone()
        result = dict(updated)
    if forced_reply_ts is not None:
        result["forced"] = True
        result["landed_reply_ts"] = forced_reply_ts
        _append_ops_alert(
            {
                "kind": "requeue_forced",
                "event_id": event_id,
                "previous_status": "FAILED",
                "requested_status": "PENDING",
                "new_status": result.get("status"),
                "reply_ts": forced_reply_ts,
                "reason": str(reason).strip()[:300],
                "source": "requeue",
                "at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            },
            Path(db_path),
        )
    return result


def cmd_requeue(args: argparse.Namespace) -> int:
    try:
        updated = requeue_event(
            args.event_id,
            db_path=Path(args.db),
            force=bool(getattr(args, "force", False)),
            reason=getattr(args, "reason", None),
        )
        print(json.dumps({"ok": True, "event": updated}, ensure_ascii=False, indent=2))
        return 0
    except RequeueBlocked as blocked:
        print(
            json.dumps(
                {
                    "ok": False,
                    "error": "reply_already_landed",
                    "event_id": blocked.event_id,
                    "current_status": "FAILED",
                    "landed_reply": blocked.ledger_row,
                    "message": str(blocked),
                    "alert_written": blocked.alert_written,
                },
                ensure_ascii=False,
            ),
            file=sys.stderr,
        )
        return 1
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
    p_history.add_argument(
        "--limit",
        type=int,
        default=20,
        help=f"Number of messages to fetch (default: 20, clamped to {HISTORY_MAX_LIMIT})",
    )
    p_history.add_argument(
        "--raw",
        action="store_true",
        help="Print the unmodified Slack messages (debug only; default is the bounded compact view)",
    )
    p_history.set_defaults(func=cmd_history)

    # event-text
    p_event_text = subparsers.add_parser(
        "event-text",
        help="Print the full stored text of one event (payload text may be truncated)",
    )
    p_event_text.add_argument("--event-id", required=True, help="Event ID")
    p_event_text.set_defaults(func=cmd_event_text)

    # reply
    p_reply = subparsers.add_parser("reply", help="Post a message or reply as Yama bot")
    p_reply.add_argument("--channel", required=True, help="Slack channel ID")
    p_reply.add_argument("--text-file", required=True, help="Path to file containing message text")
    p_reply.add_argument("--thread-ts", default=None, help="Thread parent timestamp (optional)")
    # Exactly one of --event-id / --no-event-id is required (checked in cmd_reply so
    # the refusal is a JSON error + exit 1 like every other reply error).
    p_reply.add_argument(
        "--event-id",
        default=None,
        help="Source event_id (must exist in the events table) for idempotent reply "
        "deduplication. Required unless --no-event-id is given.",
    )
    p_reply.add_argument(
        "--no-event-id",
        action="store_true",
        help="Post not tied to any event (share / report post). Not recorded in the "
        "replies ledger. Required unless --event-id is given.",
    )
    p_reply.add_argument(
        "--force",
        action="store_true",
        help="Post with an --event-id that is not in the events table; requires --reason",
    )
    p_reply.add_argument(
        "--reason", default=None, help="Why the unknown --event-id is forced (kept in the ops alert)"
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

    # triage-log (review fix 2026-11-25 [mid-3])
    p_tlog = subparsers.add_parser(
        "triage-log",
        help="Append one triage decision (JSON object with event_id) to state/triage_log.jsonl",
    )
    p_tlog.add_argument("--json", default=None, help="The entry as a JSON object string")
    p_tlog.add_argument("--file", default=None, help="Read the JSON object from this file (default: stdin)")
    p_tlog.add_argument("--db", default=argparse.SUPPRESS, help=argparse.SUPPRESS)
    p_tlog.set_defaults(func=cmd_triage_log)

    # prune-state-files (review fix 2026-11-25 [mid-7])
    p_psf = subparsers.add_parser(
        "prune-state-files",
        help="Delete reply-*.txt of events DONE for N days and decisions-*.json older than N days",
    )
    p_psf.add_argument("--days", type=int, default=DEFAULT_STATE_FILE_RETENTION_DAYS)
    p_psf.add_argument("--dry-run", action="store_true")
    p_psf.add_argument("--db", default=argparse.SUPPRESS, help=argparse.SUPPRESS)
    p_psf.set_defaults(func=cmd_prune_state_files)

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
    p_commit.add_argument(
        "--force",
        action="store_true",
        help="Correct an already terminal event (DONE/IGNORED/FAILED) to another status; requires --reason",
    )
    p_commit.add_argument(
        "--reason", default=None, help="Why the forced correction is made (kept in error_message)"
    )
    p_commit.set_defaults(func=cmd_commit)

    p_cbatch = subparsers.add_parser(
        "commit-batch",
        help="Commit several decisions at once from a JSON file "
        '([{"event_id": ..., "status": "IGNORED", "error": null}, ...])',
    )
    p_cbatch.add_argument("--file", required=True, help="JSON file with the decisions array")
    p_cbatch.set_defaults(func=cmd_commit_batch)

    p_seed = subparsers.add_parser(
        "seed-mirror",
        help="One-off read-only fill of the local thread mirror for recent threads",
    )
    p_seed.add_argument("--days", default=SEED_MIRROR_DEFAULT_DAYS, help="Threads with events in the last N days")
    p_seed.add_argument("--pause", default=1.0, help="Seconds to wait between threads")
    p_seed.add_argument("--max-threads", default=100, help="Upper bound on threads read")
    p_seed.add_argument("--dry-run", action="store_true", help="List target threads only")
    p_seed.set_defaults(func=cmd_seed_mirror)

    p_requeue = subparsers.add_parser(
        "requeue", help="Manually re-queue a FAILED event to PENDING (resets reap_count)"
    )
    p_requeue.add_argument("--event-id", required=True, help="Target event_id (must be FAILED)")
    p_requeue.add_argument(
        "--force",
        action="store_true",
        help="Requeue even though the replies ledger already records a landed reply; requires --reason",
    )
    p_requeue.add_argument(
        "--reason", default=None, help="Why the forced requeue is made (kept in error_message)"
    )
    p_requeue.set_defaults(func=cmd_requeue)

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
