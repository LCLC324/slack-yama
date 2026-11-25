#!/usr/bin/env python3
"""
Yama Slack Socket Mode Listener (Layer 1)
- Connects to Slack via Socket Mode using SLACK_APP_TOKEN and SLACK_BOT_TOKEN from config/.env
- Immediately ACKs incoming events
- Filters out self-messages, edits/deletes, and system events (self-messages,
  edits and deletes are mirrored into mirror_messages / message_edits for local
  thread context; they never become events). Other bots' posts are NOT filtered:
  bot_message is an allowed subtype and they are saved with is_bot=1.
- Persists valid user messages to SQLite (state/slack_events.db, WAL mode) with status='PENDING'
- Updates state/heartbeat.json every 30s and exits(1) if WebSocket is disconnected for >= 60s
- Exits(1) if no Socket Mode pong has been received for >= 90s even though the SDK
  still reports the socket as connected (frozen/half-open link: is_connected() only
  checks that a socket object exists, and the main-thread heartbeat stays fresh, so
  watchdog.sh cannot see this case). systemd Restart=always brings it back in ~10s.
- Sends sd_notify WATCHDOG=1 from the main loop every WATCHDOG_PING_INTERVAL_SEC when
  started by systemd with WatchdogSec (NOTIFY_SOCKET set; no-op otherwise). This is a
  backstop that does not depend on the 30s dispatch hook: if the main loop stalls and
  the hook (which runs watchdog.sh, 120s heartbeat threshold) is not running either,
  systemd restarts the listener. WatchdogSec is set above watchdog.sh's effective
  window so the two do not race on the same stall.
- Review fix 2026-11-25 (candidate 33, [mid-4]):
  * First-pong guard: a session that has been "connected" for >= 120s without a
    single pong is treated as frozen at connect time (the SDK's own stale check
    only starts after the first pong). To never restart-loop against a server
    that simply does not echo pings, such exits are capped at 2 in a row
    (state/first_pong_exits.json, reset by the first pong of any later session).
  * Event handler stall: if one Socket Mode request handler (ACK + DB save, run on
    the SDK's worker threads) has been running for >= 300s, the process exits(1)
    so systemd restarts it (receiver-side pongs keep working in that case, so the
    pong guard does not see it).
  * The heartbeat / WATCHDOG=1 cadence is scheduled on time.monotonic(), so a wall
    clock step cannot stall them. heartbeat.json keeps its wall-clock "timestamp"
    (watchdog.sh compares it with its own wall clock).
"""

import json
import logging
import os
import signal
import socket
import sqlite3
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Set, Tuple

from slack_sdk import WebClient
from slack_sdk.socket_mode import SocketModeClient
from slack_sdk.socket_mode.request import SocketModeRequest
from slack_sdk.socket_mode.response import SocketModeResponse

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_ENV_PATH = BASE_DIR / "config" / ".env"
DEFAULT_DB_PATH = BASE_DIR / "state" / "slack_events.db"
DEFAULT_HEARTBEAT_PATH = BASE_DIR / "state" / "heartbeat.json"

HEARTBEAT_INTERVAL_SEC = 30
MAX_DISCONNECT_SEC = 60
# No pong for this long => link considered frozen (SDK itself reconnects after
# 4 x ping_interval = 20s of silence, so 90s means its own recovery is not working).
MAX_PONG_SILENCE_SEC = 90
# sd_notify WATCHDOG=1 cadence (must be well below the unit's WatchdogSec=180).
WATCHDOG_PING_INTERVAL_SEC = 10
# First-pong guard (review fix 2026-11-25): connected this long without any pong in
# the current session => frozen at connect. Capped consecutive exits (no loop).
MAX_FIRST_PONG_WAIT_SEC = 120
MAX_FIRST_PONG_EXITS = 2
DEFAULT_FIRST_PONG_EXITS_PATH = BASE_DIR / "state" / "first_pong_exits.json"
# One request handler running this long => the event worker is stuck.
MAX_HANDLER_STALL_SEC = 300

# Subtypes allowed to pass the subtype gate (including bot_message from other bots);
# edits/deletes/joins/leaves (message_changed, message_deleted, channel_join, etc.) are ignored.
ALLOWED_SUBTYPES = {"file_share", "thread_broadcast", "bot_message"}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("yama-listener")


def load_env_file(env_path: Path = DEFAULT_ENV_PATH) -> Dict[str, str]:
    """Load KEY=VALUE pairs from config/.env into os.environ if not already set."""
    loaded: Dict[str, str] = {}
    if not env_path.exists():
        return loaded
    with env_path.open("r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[len("export ") :].strip()
            if "=" not in line:
                continue
            key, val = line.split("=", 1)
            key = key.strip()
            val = val.strip()
            if len(val) >= 2 and ((val[0] == val[-1] == '"') or (val[0] == val[-1] == "'")):
                val = val[1:-1]
            loaded[key] = val
            if key not in os.environ:
                os.environ[key] = val
    return loaded


DB_BUSY_TIMEOUT_SEC = 30.0
DB_BUSY_TIMEOUT_MS = int(DB_BUSY_TIMEOUT_SEC * 1000)


def get_db_connection(db_path: Path = DEFAULT_DB_PATH) -> sqlite3.Connection:
    """Open a SQLite connection with WAL mode and a unified 30s busy timeout.
    Note: sqlite3.connect(timeout=...) and PRAGMA busy_timeout configure the same
    underlying SQLite lock wait timeout; both are aligned to 30s (30000ms) so concurrent
    access across listener, backfill, dispatcher, and agent_tools does not prematurely
    fail with 'database is locked'."""
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), timeout=DB_BUSY_TIMEOUT_SEC)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute(f"PRAGMA busy_timeout={DB_BUSY_TIMEOUT_MS};")
    return conn


def init_db(db_path: Path = DEFAULT_DB_PATH) -> None:
    """Initialize the events and replies tables and index according to the design specification."""
    with get_db_connection(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS events (
                event_id TEXT PRIMARY KEY,
                channel_id TEXT NOT NULL,
                user_id TEXT NOT NULL,
                text TEXT NOT NULL,
                ts TEXT NOT NULL,
                thread_ts TEXT,
                is_bot INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'PENDING',
                retry_count INTEGER DEFAULT 0,
                error_message TEXT,
                next_retry_at TIMESTAMP,
                first_unverifiable_at TIMESTAMP,
                reap_count INTEGER NOT NULL DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE INDEX IF NOT EXISTS idx_status ON events(status);
            CREATE INDEX IF NOT EXISTS idx_channel_ts ON events(channel_id, ts);

            CREATE TABLE IF NOT EXISTS replies (
                event_id TEXT PRIMARY KEY,
                channel_id TEXT NOT NULL,
                thread_ts TEXT,
                reply_ts TEXT NOT NULL,
                text_hash TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        _init_mirror_tables(conn)
        cols = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(events);").fetchall()
        }
        if "is_bot" not in cols:
            conn.execute(
                "ALTER TABLE events ADD COLUMN is_bot INTEGER NOT NULL DEFAULT 0;"
            )
        if "next_retry_at" not in cols:
            conn.execute(
                "ALTER TABLE events ADD COLUMN next_retry_at TIMESTAMP;"
            )
        if "first_unverifiable_at" not in cols:
            conn.execute(
                "ALTER TABLE events ADD COLUMN first_unverifiable_at TIMESTAMP;"
            )
        # Reaper re-queue counter (candidate 12). Separate from retry_count:
        # only dispatcher.sh's Reaper increments it, only a manual requeue
        # (agent_tools.py requeue) resets it. Additive column, so older code
        # that does not know it keeps working.
        if "reap_count" not in cols:
            conn.execute(
                "ALTER TABLE events ADD COLUMN reap_count INTEGER NOT NULL DEFAULT 0;"
            )
        conn.commit()


# ---------------------------------------------------------------------------
# Local mirror for context bundling (stall fix 2026-11-22, proposal 2).
# These tables are NEVER a dispatch source and never feed reply decisions by
# themselves: they only complete the local copy of threads so the dispatcher can
# hand the worker the thread context without a Slack read. The events table and
# its filter (self posts / edits / deletes are not events) are unchanged.
#   mirror_messages : messages that are in a thread but not in events: Yama's own
#                     posts (the listener receives them and used to drop them as
#                     self_bot_message; posts without --event-id were nowhere) and
#                     thread parents / other posts filled in by `seed-mirror`.
#   message_edits   : latest edited text / deletion of any message in a mirrored
#                     channel (message_changed / message_deleted).
#   mirror_threads  : threads fully read once by `seed-mirror` (seeded_at epoch).
#   mirror_meta     : own_messages_since = epoch from which the listener records
#                     own posts. A thread that started earlier and was not seeded
#                     after that moment may miss own posts locally, so its bundled
#                     context is marked incomplete (the worker reads Slack).
# ---------------------------------------------------------------------------
MIRROR_SINCE_KEY = "own_messages_since"


def _init_mirror_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS mirror_messages (
            channel_id TEXT NOT NULL,
            ts TEXT NOT NULL,
            thread_ts TEXT,
            user_id TEXT,
            is_bot INTEGER NOT NULL DEFAULT 0,
            text TEXT NOT NULL DEFAULT '',
            source TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (channel_id, ts)
        );
        CREATE INDEX IF NOT EXISTS idx_mirror_thread ON mirror_messages(channel_id, thread_ts);
        CREATE TABLE IF NOT EXISTS message_edits (
            channel_id TEXT NOT NULL,
            ts TEXT NOT NULL,
            text TEXT,
            deleted INTEGER NOT NULL DEFAULT 0,
            edited_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (channel_id, ts)
        );
        CREATE TABLE IF NOT EXISTS mirror_threads (
            channel_id TEXT NOT NULL,
            thread_ts TEXT NOT NULL,
            seeded_at REAL NOT NULL,
            message_count INTEGER,
            PRIMARY KEY (channel_id, thread_ts)
        );
        CREATE TABLE IF NOT EXISTS mirror_meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        """
    )


def record_mirror_message(
    channel_id: str,
    ts: str,
    thread_ts: Optional[str],
    user_id: Optional[str],
    is_bot: bool,
    text: Optional[str],
    source: str = "listener",
    db_path: Path = DEFAULT_DB_PATH,
) -> None:
    """Upsert one non-event message (own post, thread parent, ...) into mirror_messages."""
    with get_db_connection(db_path) as conn:
        _init_mirror_tables(conn)
        conn.execute(
            """
            INSERT INTO mirror_messages (channel_id, ts, thread_ts, user_id, is_bot, text, source)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(channel_id, ts) DO UPDATE SET
                thread_ts = COALESCE(excluded.thread_ts, mirror_messages.thread_ts),
                user_id = COALESCE(excluded.user_id, mirror_messages.user_id),
                is_bot = excluded.is_bot,
                text = excluded.text
            """,
            (channel_id, str(ts), thread_ts, user_id, 1 if is_bot else 0, text or "", source),
        )
        conn.commit()


def record_own_message(
    channel_id: str,
    ts: str,
    thread_ts: Optional[str],
    text: Optional[str],
    user_id: Optional[str] = None,
    source: str = "listener",
    db_path: Path = DEFAULT_DB_PATH,
) -> None:
    """Mirror one of Yama's own posts (is_bot=1)."""
    record_mirror_message(
        channel_id, ts, thread_ts, user_id, True, text, source=source, db_path=db_path
    )


def record_message_edit(
    channel_id: str,
    ts: str,
    text: Optional[str],
    deleted: bool = False,
    db_path: Path = DEFAULT_DB_PATH,
) -> None:
    """Record the latest edited text (or deletion) of a message (mirror only)."""
    with get_db_connection(db_path) as conn:
        _init_mirror_tables(conn)
        conn.execute(
            """
            INSERT INTO message_edits (channel_id, ts, text, deleted, edited_at)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(channel_id, ts) DO UPDATE SET
                text = CASE WHEN excluded.deleted = 1 THEN message_edits.text ELSE excluded.text END,
                deleted = MAX(message_edits.deleted, excluded.deleted),
                edited_at = CURRENT_TIMESTAMP
            """,
            (channel_id, str(ts), text, 1 if deleted else 0),
        )
        conn.commit()


def mark_mirror_since(db_path: Path = DEFAULT_DB_PATH, now: Optional[float] = None) -> str:
    """Set mirror_meta.own_messages_since once (first listener start with mirroring)."""
    value = f"{(now if now is not None else time.time()):.6f}"
    with get_db_connection(db_path) as conn:
        _init_mirror_tables(conn)
        conn.execute(
            "INSERT OR IGNORE INTO mirror_meta (key, value) VALUES (?, ?)",
            (MIRROR_SINCE_KEY, value),
        )
        conn.commit()
        row = conn.execute(
            "SELECT value FROM mirror_meta WHERE key = ?", (MIRROR_SINCE_KEY,)
        ).fetchone()
        return str(row["value"]) if row else value


def mirror_side_event(
    event: Dict[str, Any],
    bot_user_id: Optional[str],
    db_path: Path = DEFAULT_DB_PATH,
) -> Optional[str]:
    """Mirror the messages the events table deliberately skips: Yama's own posts,
    edits and deletions. Returns what was mirrored ("own_message" / "edit" /
    "delete") or None. Never raises (mirroring must not affect event intake)."""
    try:
        if not isinstance(event, dict) or event.get("type", "message") != "message":
            return None
        channel = event.get("channel")
        if not channel:
            return None
        subtype = event.get("subtype")
        if subtype == "message_changed":
            msg = event.get("message") or {}
            ts = msg.get("ts")
            if not ts:
                return None
            if bot_user_id and msg.get("user") == bot_user_id:
                record_own_message(
                    channel, ts, msg.get("thread_ts"), msg.get("text"),
                    user_id=bot_user_id, db_path=db_path,
                )
            record_message_edit(channel, ts, msg.get("text"), db_path=db_path)
            return "edit"
        if subtype == "message_deleted":
            ts = event.get("deleted_ts") or (event.get("previous_message") or {}).get("ts")
            if not ts:
                return None
            record_message_edit(channel, ts, None, deleted=True, db_path=db_path)
            return "delete"
        if (
            bot_user_id
            and event.get("user") == bot_user_id
            and event.get("ts")
            and (subtype is None or subtype in ALLOWED_SUBTYPES)
        ):
            record_own_message(
                channel, event["ts"], event.get("thread_ts"), event.get("text"),
                user_id=bot_user_id, db_path=db_path,
            )
            return "own_message"
    except Exception as exc:
        logger.warning("mirror_side_event failed (ignored): %s", exc)
    return None


def is_bot_event(event: Dict[str, Any]) -> bool:
    """Return True if the event is considered a bot message."""
    return event.get("bot_id") is not None or event.get("subtype") == "bot_message"


def should_ignore_event(
    event: Dict[str, Any],
    bot_user_id: Optional[str] = None,
    ignored_user_ids: Optional[Set[str]] = None,
) -> Tuple[bool, str]:
    """
    Determine whether a Slack message event should be filtered out.
    Returns (ignore: bool, reason: str).
    """
    if not isinstance(event, dict):
        return True, "not_a_dict"

    event_type = event.get("type", "message")
    if event_type != "message":
        return True, f"unsupported_type:{event_type}"

    subtype = event.get("subtype")
    if subtype is not None and subtype not in ALLOWED_SUBTYPES:
        return True, f"ignored_subtype:{subtype}"

    user_id = event.get("user")
    if not user_id:
        return True, "missing_user"

    if bot_user_id and user_id == bot_user_id:
        return True, "self_bot_message"

    if ignored_user_ids and user_id in ignored_user_ids:
        return True, "ignored_user_id"

    if event.get("hidden") is True:
        return True, "hidden_event"

    text = event.get("text")
    if text is None:
        return True, "missing_text"

    if not event.get("channel") or not event.get("ts"):
        return True, "missing_channel_or_ts"

    return False, "ok"


def save_event(
    event: Dict[str, Any],
    event_id: Optional[str] = None,
    db_path: Path = DEFAULT_DB_PATH,
) -> bool:
    """
    Save a filtered event into SQLite with status='PENDING'.
    Deduplicates via event_id PRIMARY KEY (INSERT OR IGNORE) and checks
    existing (channel_id, ts) rows so backfill and Socket Mode do not
    double-insert the same message regardless of arrival order.
    Returns True if a new row was inserted, False if duplicate.
    """
    channel_id = event["channel"]
    user_id = event["user"]
    text = event.get("text", "")
    ts = event["ts"]
    thread_ts = event.get("thread_ts")
    is_bot = 1 if is_bot_event(event) else 0
    fallback_event_id = f"{channel_id}_{ts}"
    resolved_event_id = (
        event_id
        or event.get("event_id")
        or event.get("client_msg_id")
        or fallback_event_id
    )

    with get_db_connection(db_path) as conn:
        if resolved_event_id == fallback_event_id:
            existing = conn.execute(
                "SELECT 1 FROM events WHERE channel_id = ? AND ts = ? LIMIT 1",
                (channel_id, ts),
            ).fetchone()
        else:
            existing = conn.execute(
                "SELECT 1 FROM events WHERE channel_id = ? AND ts = ? AND event_id = ? LIMIT 1",
                (channel_id, ts, fallback_event_id),
            ).fetchone()
        if existing is not None:
            return False

        cursor = conn.execute(
            """
            INSERT OR IGNORE INTO events (
                event_id, channel_id, user_id, text, ts, thread_ts, is_bot, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 'PENDING')
            """,
            (resolved_event_id, channel_id, user_id, text, ts, thread_ts, is_bot),
        )
        conn.commit()
        return cursor.rowcount > 0


def handle_event_payload(
    payload: Dict[str, Any],
    bot_user_id: Optional[str] = None,
    db_path: Path = DEFAULT_DB_PATH,
    ignored_user_ids: Optional[Set[str]] = None,
) -> Tuple[bool, str]:
    """
    Validate and store an incoming Slack event payload (either outer envelope or raw event dict).
    Returns (saved: bool, reason: str).
    """
    if "event" in payload and isinstance(payload["event"], dict):
        event = payload["event"]
        envelope_event_id = payload.get("event_id")
    else:
        event = payload
        envelope_event_id = payload.get("event_id")

    ignore, reason = should_ignore_event(
        event, bot_user_id=bot_user_id, ignored_user_ids=ignored_user_ids
    )
    if ignore:
        # Not an event, but keep the local thread mirror complete (own posts,
        # edits, deletions). Best-effort; the return value is unchanged.
        mirror_side_event(event, bot_user_id=bot_user_id, db_path=db_path)
        return False, reason

    inserted = save_event(event, event_id=envelope_event_id, db_path=db_path)
    if not inserted:
        return False, "duplicate_event_id"
    return True, "saved"


def write_heartbeat(
    connected: bool,
    bot_user_id: Optional[str],
    heartbeat_path: Path = DEFAULT_HEARTBEAT_PATH,
    last_pong_age_sec: Optional[float] = None,
) -> None:
    """Atomically write heartbeat status to state/heartbeat.json.
    last_pong_age_sec (optional, observability only) is null until the
    current session has received its first pong."""
    heartbeat_path = Path(heartbeat_path)
    heartbeat_path.parent.mkdir(parents=True, exist_ok=True)
    now = time.time()
    data = {
        "timestamp": now,
        "iso_time": datetime.fromtimestamp(now, tz=timezone.utc).isoformat(),
        "pid": os.getpid(),
        "connected": bool(connected),
        "bot_user_id": bot_user_id,
        "last_pong_age_sec": (
            None if last_pong_age_sec is None else int(last_pong_age_sec)
        ),
    }
    tmp_path = heartbeat_path.with_suffix(".json.tmp")
    with tmp_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.write("\n")
    tmp_path.replace(heartbeat_path)


def check_connection_watchdog(
    is_connected: bool,
    disconnected_since: Optional[float],
    now: Optional[float] = None,
    max_disconnect_sec: float = MAX_DISCONNECT_SEC,
) -> Tuple[Optional[float], bool]:
    """
    Track WebSocket disconnection duration.
    Returns (updated_disconnected_since, should_exit).
    """
    if now is None:
        now = time.time()
    if is_connected:
        return None, False
    if disconnected_since is None:
        return now, False
    if (now - disconnected_since) >= max_disconnect_sec:
        return disconnected_since, True
    return disconnected_since, False


def get_pong_silence_sec(
    socket_client: Any,
    now: float,
) -> Optional[float]:
    """Seconds since the current Socket Mode session last received a pong
    (session.last_ping_pong_time, set by the SDK receiver thread on each pong).
    Returns None when there is no session or the session has not received any
    pong yet: only a link that *was* answering pongs and then went silent is
    treated as frozen, so a server that never echoes pings cannot cause a
    restart loop (that case keeps the previous behaviour)."""
    session = getattr(socket_client, "current_session", None)
    if session is None:
        return None
    last_pong = getattr(session, "last_ping_pong_time", None)
    if last_pong is None:
        return None
    try:
        return max(0.0, now - float(last_pong))
    except (TypeError, ValueError):
        return None


def should_exit_for_pong_silence(
    connected: bool,
    pong_silence: Optional[float],
    max_pong_silence_sec: float = MAX_PONG_SILENCE_SEC,
) -> bool:
    """True when the socket still reports connected but no pong has arrived for
    max_pong_silence_sec (frozen link). Unknown silence (None: no session yet, or
    no pong received yet in this session) never triggers an exit."""
    return bool(connected) and pong_silence is not None and pong_silence >= max_pong_silence_sec


def track_session_without_pong(
    socket_client: Any,
    mono_now: float,
    tracker: Dict[str, Any],
) -> Optional[float]:
    """Seconds (monotonic) the CURRENT session has existed without receiving any
    pong. None when there is no session or the session already got a pong.
    tracker keeps {"session": <object>, "since": <monotonic>} between calls and is
    reset whenever the SDK swaps in a new session object."""
    session = getattr(socket_client, "current_session", None)
    if session is None:
        tracker.clear()
        return None
    if getattr(session, "last_ping_pong_time", None) is not None:
        tracker.clear()
        return None
    if tracker.get("session") is not session:
        tracker["session"] = session
        tracker["since"] = mono_now
        return 0.0
    return max(0.0, mono_now - float(tracker.get("since", mono_now)))


def should_exit_for_no_first_pong(
    connected: bool,
    wait_sec: Optional[float],
    prior_exits: int,
    max_wait_sec: float = MAX_FIRST_PONG_WAIT_SEC,
    max_exits: int = MAX_FIRST_PONG_EXITS,
) -> bool:
    """True when the socket reports connected, the session has had no pong for
    max_wait_sec, and fewer than max_exits such exits happened in a row."""
    return (
        bool(connected)
        and wait_sec is not None
        and wait_sec >= max_wait_sec
        and int(prior_exits) < int(max_exits)
    )


def load_first_pong_exits(path: Optional[Path] = None) -> int:
    path = DEFAULT_FIRST_PONG_EXITS_PATH if path is None else path
    try:
        return max(0, int(json.loads(Path(path).read_text(encoding="utf-8")).get("count", 0)))
    except Exception:
        return 0


def save_first_pong_exits(count: int, path: Optional[Path] = None) -> None:
    path = Path(DEFAULT_FIRST_PONG_EXITS_PATH if path is None else path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(".json.tmp")
    tmp_path.write_text(json.dumps({"count": int(count), "at": time.time()}) + "\n", encoding="utf-8")
    tmp_path.replace(path)


class HandlerTracker:
    """Start times (monotonic) of Socket Mode request handlers in flight."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._started: Dict[int, float] = {}
        self._seq = 0

    def begin(self, mono_now: Optional[float] = None) -> int:
        with self._lock:
            self._seq += 1
            self._started[self._seq] = time.monotonic() if mono_now is None else mono_now
            return self._seq

    def end(self, token: int) -> None:
        with self._lock:
            self._started.pop(token, None)

    def oldest_age(self, mono_now: float) -> Optional[float]:
        with self._lock:
            if not self._started:
                return None
            return max(0.0, mono_now - min(self._started.values()))


def sd_notify(message: str, env: Optional[Dict[str, str]] = None) -> bool:
    """Send one sd_notify datagram (e.g. "WATCHDOG=1") to $NOTIFY_SOCKET.
    Returns False without doing anything when NOTIFY_SOCKET is unset (not run by
    systemd, or the unit has no WatchdogSec/NotifyAccess). Raises OSError on a
    send failure so the caller can log it. Abstract sockets ("@name") supported."""
    source = env if env is not None else os.environ
    addr = (source.get("NOTIFY_SOCKET") or "").strip()
    if not addr:
        return False
    if addr.startswith("@"):
        addr = "\0" + addr[1:]
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    try:
        sock.connect(addr)
        sock.sendall(message.encode("utf-8"))
    finally:
        sock.close()
    return True


def authenticate_bot(
    web_client: Any,
    sleep_fn: Optional[Any] = None,
) -> Dict[str, Any]:
    """Call web_client.auth_test() with retry on transient connection or Slack API errors."""
    from agent_tools import _call_with_retry

    return _call_with_retry(
        lambda: web_client.auth_test(),
        sleep_fn=sleep_fn,
    )


def main() -> None:
    load_env_file(DEFAULT_ENV_PATH)

    app_token = os.environ.get("SLACK_APP_TOKEN", "").strip()
    bot_token = os.environ.get("SLACK_BOT_TOKEN", "").strip()

    if not app_token or not bot_token:
        logger.error(
            "SLACK_APP_TOKEN and SLACK_BOT_TOKEN must be configured in %s",
            DEFAULT_ENV_PATH,
        )
        sys.exit(1)

    init_db(DEFAULT_DB_PATH)

    web_client = WebClient(token=bot_token)
    try:
        auth_resp = authenticate_bot(web_client)
        bot_user_id = auth_resp.get("user_id")
        logger.info(
            "Authenticated as bot_user_id=%s (team=%s, user=%s)",
            bot_user_id,
            auth_resp.get("team"),
            auth_resp.get("user"),
        )
    except Exception as exc:
        logger.error("Failed auth.test with SLACK_BOT_TOKEN: %s", exc)
        sys.exit(1)

    extra_ignored_str = os.environ.get("IGNORED_USER_IDS", "")
    ignored_user_ids: Set[str] = {
        u.strip() for u in extra_ignored_str.split(",") if u.strip()
    }

    socket_client = SocketModeClient(
        app_token=app_token,
        web_client=web_client,
        auto_reconnect_enabled=True,
    )

    handler_tracker = HandlerTracker()

    def on_socket_mode_request(client: SocketModeClient, req: SocketModeRequest) -> None:
        token = handler_tracker.begin()
        try:
            _handle_socket_mode_request(client, req)
        finally:
            handler_tracker.end(token)

    def _handle_socket_mode_request(client: SocketModeClient, req: SocketModeRequest) -> None:
        # 1. Immediately return ACK within 3 seconds
        try:
            client.send_socket_mode_response(
                SocketModeResponse(envelope_id=req.envelope_id)
            )
        except Exception as ack_err:
            logger.error("Failed to send ACK for envelope %s: %s", req.envelope_id, ack_err)

        # 2. Process Events API payload
        if req.type == "events_api":
            try:
                saved, reason = handle_event_payload(
                    req.payload,
                    bot_user_id=bot_user_id,
                    db_path=DEFAULT_DB_PATH,
                    ignored_user_ids=ignored_user_ids,
                )
                if saved:
                    ev = req.payload.get("event", {})
                    logger.info(
                        "Saved event_id=%s channel=%s user=%s ts=%s",
                        req.payload.get("event_id"),
                        ev.get("channel"),
                        ev.get("user"),
                        ev.get("ts"),
                    )
                else:
                    logger.debug("Ignored event (%s)", reason)
            except Exception as proc_err:
                logger.exception("Error processing event payload: %s", proc_err)

    socket_client.socket_mode_request_listeners.append(on_socket_mode_request)

    stop_event = threading.Event()

    def handle_signal(signum: int, _frame: Any) -> None:
        logger.info("Received signal %d, shutting down...", signum)
        stop_event.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    logger.info("Connecting to Slack Socket Mode...")
    socket_client.connect()
    try:
        since = mark_mirror_since(DEFAULT_DB_PATH)
        logger.info("Own-message mirror active since %s", since)
    except Exception as mirror_exc:
        logger.warning("Could not mark own-message mirror start: %s", mirror_exc)

    disconnected_since: Optional[float] = None
    # Monotonic schedule for heartbeat / WATCHDOG=1 (None = due now).
    last_heartbeat: Optional[float] = None
    last_watchdog_ping: Optional[float] = None
    watchdog_ping_error_logged = False
    first_pong_tracker: Dict[str, Any] = {}
    first_pong_exits = load_first_pong_exits()
    first_pong_cap_logged = False

    try:
        while not stop_event.is_set():
            now = time.time()
            mono = time.monotonic()
            try:
                connected = bool(socket_client.is_connected())
            except Exception:
                connected = False

            disconnected_since, should_exit = check_connection_watchdog(
                connected,
                disconnected_since,
                now=now,
                max_disconnect_sec=MAX_DISCONNECT_SEC,
            )

            if should_exit:
                write_heartbeat(
                    connected=False,
                    bot_user_id=bot_user_id,
                    heartbeat_path=DEFAULT_HEARTBEAT_PATH,
                )
                logger.error(
                    "WebSocket disconnected for >= %d seconds. Exiting(1) for systemd restart.",
                    MAX_DISCONNECT_SEC,
                )
                sys.exit(1)

            try:
                pong_silence = get_pong_silence_sec(socket_client, now)
            except Exception:
                pong_silence = None
            if should_exit_for_pong_silence(connected, pong_silence):
                write_heartbeat(
                    connected=False,
                    bot_user_id=bot_user_id,
                    heartbeat_path=DEFAULT_HEARTBEAT_PATH,
                )
                logger.error(
                    "No Socket Mode pong for %d seconds while the socket still looks connected "
                    "(frozen link). Exiting(1) for systemd restart.",
                    int(pong_silence),
                )
                # Skip socket_client.close(): on a frozen link it may block.
                logging.shutdown()
                os._exit(1)

            # First-pong guard (review fix 2026-11-25).
            if pong_silence is not None and first_pong_exits:
                first_pong_exits = 0
                try:
                    save_first_pong_exits(0)
                except Exception as fp_exc:
                    logger.warning("Could not reset first-pong exit counter: %s", fp_exc)
            try:
                no_pong_for = track_session_without_pong(socket_client, mono, first_pong_tracker)
            except Exception:
                no_pong_for = None
            if should_exit_for_no_first_pong(connected, no_pong_for, first_pong_exits):
                try:
                    save_first_pong_exits(first_pong_exits + 1)
                except Exception as fp_exc:
                    logger.warning("Could not save first-pong exit counter: %s", fp_exc)
                write_heartbeat(
                    connected=False,
                    bot_user_id=bot_user_id,
                    heartbeat_path=DEFAULT_HEARTBEAT_PATH,
                )
                logger.error(
                    "No Socket Mode pong at all for %d seconds since this session connected "
                    "(frozen at connect; exit %d/%d in a row). Exiting(1) for systemd restart.",
                    int(no_pong_for), first_pong_exits + 1, MAX_FIRST_PONG_EXITS,
                )
                logging.shutdown()
                os._exit(1)
            if (
                no_pong_for is not None
                and no_pong_for >= MAX_FIRST_PONG_WAIT_SEC
                and not first_pong_cap_logged
            ):
                logger.warning(
                    "No pong yet after %d seconds, but %d first-pong exits already happened in a "
                    "row; not exiting again (the server may not echo pings).",
                    int(no_pong_for), first_pong_exits,
                )
                first_pong_cap_logged = True

            stalled = handler_tracker.oldest_age(mono)
            if stalled is not None and stalled >= MAX_HANDLER_STALL_SEC:
                write_heartbeat(
                    connected=False,
                    bot_user_id=bot_user_id,
                    heartbeat_path=DEFAULT_HEARTBEAT_PATH,
                )
                logger.error(
                    "A Socket Mode request handler has been running for %d seconds (event worker "
                    "stuck). Exiting(1) for systemd restart.",
                    int(stalled),
                )
                logging.shutdown()
                os._exit(1)

            if last_heartbeat is None or (mono - last_heartbeat) >= HEARTBEAT_INTERVAL_SEC:
                write_heartbeat(
                    connected=connected,
                    bot_user_id=bot_user_id,
                    heartbeat_path=DEFAULT_HEARTBEAT_PATH,
                    last_pong_age_sec=pong_silence,
                )
                last_heartbeat = mono

            if last_watchdog_ping is None or (mono - last_watchdog_ping) >= WATCHDOG_PING_INTERVAL_SEC:
                last_watchdog_ping = mono
                try:
                    sd_notify("WATCHDOG=1")
                    watchdog_ping_error_logged = False
                except Exception as wd_exc:
                    if not watchdog_ping_error_logged:
                        logger.error("sd_notify WATCHDOG=1 failed: %s", wd_exc)
                        watchdog_ping_error_logged = True

            stop_event.wait(timeout=1.0)
    finally:
        try:
            socket_client.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
