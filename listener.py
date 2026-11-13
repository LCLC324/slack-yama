#!/usr/bin/env python3
"""
Yama Slack Socket Mode Listener (Layer 1)
- Connects to Slack via Socket Mode using SLACK_APP_TOKEN and SLACK_BOT_TOKEN from config/.env
- Immediately ACKs incoming events
- Filters out self-messages, other bots, edits/deletes, and system events
- Persists valid user messages to SQLite (state/slack_events.db, WAL mode) with status='PENDING'
- Updates state/heartbeat.json every 30s and exits(1) if WebSocket is disconnected for >= 60s
"""

import json
import logging
import os
import signal
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
        conn.commit()


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
        return False, reason

    inserted = save_event(event, event_id=envelope_event_id, db_path=db_path)
    if not inserted:
        return False, "duplicate_event_id"
    return True, "saved"


def write_heartbeat(
    connected: bool,
    bot_user_id: Optional[str],
    heartbeat_path: Path = DEFAULT_HEARTBEAT_PATH,
) -> None:
    """Atomically write heartbeat status to state/heartbeat.json."""
    heartbeat_path = Path(heartbeat_path)
    heartbeat_path.parent.mkdir(parents=True, exist_ok=True)
    now = time.time()
    data = {
        "timestamp": now,
        "iso_time": datetime.fromtimestamp(now, tz=timezone.utc).isoformat(),
        "pid": os.getpid(),
        "connected": bool(connected),
        "bot_user_id": bot_user_id,
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

    def on_socket_mode_request(client: SocketModeClient, req: SocketModeRequest) -> None:
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

    disconnected_since: Optional[float] = None
    last_heartbeat: float = 0.0

    try:
        while not stop_event.is_set():
            now = time.time()
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

            if (now - last_heartbeat) >= HEARTBEAT_INTERVAL_SEC:
                write_heartbeat(
                    connected=connected,
                    bot_user_id=bot_user_id,
                    heartbeat_path=DEFAULT_HEARTBEAT_PATH,
                )
                last_heartbeat = now

            stop_event.wait(timeout=1.0)
    finally:
        try:
            socket_client.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
