#!/usr/bin/env python3
"""
Yama ops monitor (called by the dispatch hook every 30s; not a daemon).

Subcommands:
  pending-check [--db PATH] [--threshold-sec N] [--now EPOCH]
      Report how long the oldest *human* PENDING event has been waiting to be
      dispatched. Prints one JSON object on stdout and exits 0; on any failure
      prints {"ok": false, "error": ...} and exits 2 (the hook surfaces this
      immediately, without a grace period).

      Counted ("human, unanswered"):
        status = 'PENDING' AND is_bot = 0
        AND user_id is not Yama itself (env YAMA_BOT_USER_ID / SLACK_BOT_USER_ID,
            heartbeat.json bot_user_id, agent_tools.YAMA_BOT_USER_ID)
        AND user_id is not in IGNORED_USER_IDS
        AND user_id is not a bot_id placeholder ("B...", set by backfill)
        AND user_id never appeared with is_bot = 1 in events (known bot users)
        AND next_retry_at is not in the future (deliberate 1h unverifiable wait)
      Age is measured from when the event last became dispatchable:
        max(created_at, updated_at, next_retry_at)
      so a FAILED->PENDING retry or a reaped event does not look instantly stale.

  outbox-take [--path PATH]
      Atomically read and clear the ops alert outbox (JSON lines appended by
      dispatcher.sh's Reaper and by backfill.py). Prints a JSON array.

  outbox-restore [--path PATH]
      Read a JSON array from stdin and append it back to the outbox (used by
      the hook when the alert wake could not be emitted).
"""

import argparse
import fcntl
import json
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_DB_PATH = BASE_DIR / "state" / "slack_events.db"
DEFAULT_HEARTBEAT_PATH = BASE_DIR / "state" / "heartbeat.json"
DEFAULT_OUTBOX_PATH = BASE_DIR / "state" / "ops_alerts.jsonl"
DEFAULT_PENDING_STALE_WARN_SEC = 600
DB_BUSY_TIMEOUT_SEC = 30.0


def _parse_db_ts(value: Any) -> Optional[float]:
    """Parse SQLite CURRENT_TIMESTAMP-style UTC strings (or epoch numbers) to epoch."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    raw = str(value).strip()
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError:
        pass
    normalized = raw[:-1] + "+00:00" if raw.endswith(("Z", "z")) else raw
    try:
        dt = datetime.fromisoformat(normalized)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except ValueError:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f", "%Y/%m/%d %H:%M:%S"):
        try:
            return datetime.strptime(raw, fmt).replace(tzinfo=timezone.utc).timestamp()
        except ValueError:
            continue
    return None


def _self_user_ids(heartbeat_path: Path) -> Set[str]:
    ids: Set[str] = set()
    for key in ("YAMA_BOT_USER_ID", "SLACK_BOT_USER_ID"):
        v = os.environ.get(key, "").strip()
        if v:
            ids.add(v)
    try:
        hb = json.loads(Path(heartbeat_path).read_text(encoding="utf-8"))
        v = str(hb.get("bot_user_id") or "").strip()
        if v:
            ids.add(v)
    except Exception:
        pass
    try:
        sys.path.insert(0, str(BASE_DIR))
        from agent_tools import YAMA_BOT_USER_ID  # type: ignore

        if YAMA_BOT_USER_ID:
            ids.add(str(YAMA_BOT_USER_ID))
    except Exception:
        pass
    return ids


def _ignored_user_ids() -> Set[str]:
    raw = os.environ.get("IGNORED_USER_IDS", "")
    return {u.strip() for u in raw.split(",") if u.strip()}


def pending_check(
    db_path: Path = DEFAULT_DB_PATH,
    threshold_sec: float = DEFAULT_PENDING_STALE_WARN_SEC,
    now: Optional[float] = None,
    heartbeat_path: Path = DEFAULT_HEARTBEAT_PATH,
    self_user_ids: Optional[Iterable[str]] = None,
) -> Dict[str, Any]:
    """Return the stale-PENDING report (raises on DB errors)."""
    now_epoch = float(now) if now is not None else time.time()
    db_path = Path(db_path)
    result: Dict[str, Any] = {
        "ok": True,
        "threshold_sec": int(threshold_sec),
        "human_pending": 0,
        "stale": False,
        "stale_count": 0,
        "oldest_event_id": None,
        "oldest_channel_id": None,
        "oldest_age_sec": 0,
        "excluded_bot_or_self": 0,
        "excluded_waiting": 0,
    }
    if not db_path.exists():
        return result

    self_ids = set(self_user_ids) if self_user_ids is not None else _self_user_ids(heartbeat_path)
    ignored = _ignored_user_ids()

    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=DB_BUSY_TIMEOUT_SEC)
    conn.row_factory = sqlite3.Row
    try:
        cols = {r["name"] for r in conn.execute("PRAGMA table_info(events);").fetchall()}
        if not cols:
            return result
        nra = "next_retry_at" if "next_retry_at" in cols else "NULL AS next_retry_at"
        known_bots = {
            str(r["user_id"])
            for r in conn.execute(
                "SELECT DISTINCT user_id FROM events WHERE is_bot = 1"
            ).fetchall()
            if r["user_id"]
        }
        rows = conn.execute(
            f"""
            SELECT event_id, channel_id, user_id, is_bot, created_at, updated_at, {nra}
            FROM events
            WHERE status = 'PENDING'
            """
        ).fetchall()
    finally:
        conn.close()

    oldest_age = -1.0
    for r in rows:
        uid = str(r["user_id"] or "")
        if (
            int(r["is_bot"] or 0) == 1
            or uid in self_ids
            or uid in ignored
            or uid in known_bots
            or uid.startswith("B")
        ):
            result["excluded_bot_or_self"] += 1
            continue
        retry_at = _parse_db_ts(r["next_retry_at"])
        if retry_at is not None and retry_at > now_epoch:
            result["excluded_waiting"] += 1
            continue
        result["human_pending"] += 1
        since_candidates = [
            t
            for t in (
                _parse_db_ts(r["created_at"]),
                _parse_db_ts(r["updated_at"]),
                retry_at,
            )
            if t is not None
        ]
        since = max(since_candidates) if since_candidates else now_epoch
        age = max(0.0, now_epoch - since)
        if age >= threshold_sec:
            result["stale_count"] += 1
        if age > oldest_age:
            oldest_age = age
            result["oldest_event_id"] = r["event_id"]
            result["oldest_channel_id"] = r["channel_id"]
    if oldest_age >= 0:
        result["oldest_age_sec"] = int(oldest_age)
    result["stale"] = result["stale_count"] > 0
    return result


def outbox_append(entry: Dict[str, Any], path: Path = DEFAULT_OUTBOX_PATH) -> None:
    """Append one alert entry (JSON line) to the outbox under an exclusive flock."""
    path = Path(path)
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


def outbox_take(path: Path = DEFAULT_OUTBOX_PATH) -> List[Dict[str, Any]]:
    """Read all entries and truncate the outbox (same flock as outbox_append).
    Unparseable lines are kept as {"kind": "unparseable_outbox_line", ...}."""
    path = Path(path)
    if not path.exists():
        return []
    entries: List[Dict[str, Any]] = []
    with open(path, "r+", encoding="utf-8") as f:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        try:
            for raw in f.read().splitlines():
                raw = raw.strip()
                if not raw:
                    continue
                try:
                    obj = json.loads(raw)
                    if not isinstance(obj, dict):
                        raise ValueError("not an object")
                    entries.append(obj)
                except Exception:
                    entries.append({"kind": "unparseable_outbox_line", "raw": raw[:500]})
            f.seek(0)
            f.truncate()
            f.flush()
            os.fsync(f.fileno())
        finally:
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)
    return entries


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="ops_monitor.py")
    sub = parser.add_subparsers(dest="cmd", required=True)
    p_pc = sub.add_parser("pending-check")
    p_pc.add_argument("--db", default=os.environ.get("YAMA_DB_PATH", str(DEFAULT_DB_PATH)))
    p_pc.add_argument(
        "--threshold-sec",
        type=float,
        default=float(os.environ.get("YAMA_PENDING_STALE_WARN_SEC", DEFAULT_PENDING_STALE_WARN_SEC)),
    )
    p_pc.add_argument("--now", type=float, default=None)
    p_pc.add_argument(
        "--heartbeat",
        default=os.environ.get("YAMA_HEARTBEAT_PATH", str(DEFAULT_HEARTBEAT_PATH)),
    )
    for name in ("outbox-take", "outbox-restore"):
        p = sub.add_parser(name)
        p.add_argument(
            "--path", default=os.environ.get("YAMA_OPS_ALERTS_PATH", str(DEFAULT_OUTBOX_PATH))
        )
    args = parser.parse_args(argv)

    try:
        if args.cmd == "pending-check":
            if os.environ.get("YAMA_PENDING_CHECK_FORCE_FAIL") == "1":
                raise RuntimeError("forced failure (test hook)")
            report = pending_check(
                db_path=Path(args.db),
                threshold_sec=args.threshold_sec,
                now=args.now,
                heartbeat_path=Path(args.heartbeat),
            )
            print(json.dumps(report, ensure_ascii=False, sort_keys=True))
            return 0
        if args.cmd == "outbox-take":
            print(json.dumps(outbox_take(Path(args.path)), ensure_ascii=False))
            return 0
        if args.cmd == "outbox-restore":
            data = json.loads(sys.stdin.read() or "[]")
            if not isinstance(data, list):
                raise ValueError("expected a JSON array on stdin")
            for entry in data:
                if isinstance(entry, dict):
                    outbox_append(entry, Path(args.path))
            return 0
    except Exception as exc:
        print(json.dumps({"ok": False, "error": f"{type(exc).__name__}: {exc}"}, ensure_ascii=False))
        return 2
    return 2


if __name__ == "__main__":
    sys.exit(main())
