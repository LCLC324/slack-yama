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
        AND next_retry_at is not in the future (deliberate unverifiable wait,
            stepped 2/5/15/30/60 min; agent_tools.UNVERIFIABLE_RETRY_WAIT_LADDER)
      Age is measured from when the event last became dispatchable:
        max(created_at, updated_at, next_retry_at)
      so a FAILED->PENDING retry or a reaped event does not look instantly stale.

  outbox-take [--path PATH]
      Atomically read and clear the ops alert outbox (JSON lines appended by
      dispatcher.sh's Reaper and by backfill.py). Prints a JSON array.

  outbox-restore [--path PATH]
      Read a JSON array from stdin and append it back to the outbox (used by
      the hook when the alert wake could not be emitted).

  outbox-append --kind KIND [--detail JSON_OBJECT] [--path PATH]
      Append one alert line {"kind", "at", ...detail} to the outbox under the
      same flock (review fix 2026-11-25 [mid-5]: lets the shell hooks report
      dispatcher.sh exit >= 3 and owner_dm_poll.py failures). Exits 2 on a bad
      --detail or a failed write.

Permanent archive (candidate 26, 2026-11-23):
  outbox-take is the single recording point. Every entry it takes out of the
  outbox is also appended to an append-only archive
  (YAMA_OPS_ALERTS_ARCHIVE_PATH, default: ops_alerts_archive.jsonl next to the
  outbox). Writers (commit guard, Reaper, backfill) do not write the archive
  themselves, so there is no double write.
  - Schema (aligned with Neiro/Yanayana mark_rejected.jsonl): at, key,
    attempted_status, current_status, reason, plus source, action
    ("rejected" | "forced" | "alert"), kind, archived_at, schema, entry (the
    outbox line as taken).
  - Bounded: when the next write would push the file past
    YAMA_OPS_ALERTS_ARCHIVE_MAX_BYTES (default 5 MiB), it is renamed to
    <archive>.1 (one generation, the previous .1 is replaced).
  - fail-open: a failed archive write never fails the take. Instead one
    {"kind": "archive_write_failed", ...} entry is added to the returned list
    so it is delivered with the same wake. Nothing is printed on stderr (the
    hook merges stderr into the JSON it parses).
  - Entries put back by outbox-restore (wake failed) carry "_archived": true so
    the next take does not archive them twice; take strips the marker.
    When the archive write itself failed, take returns those rows with an
    explicit "_archived": false; restore keeps an existing marker as is, so
    they are archived by the next take (audit fix 2026-11-23).
  - Timing: the hook's successful wake exits the hook, so take is the last
    point we control. A row is therefore "taken for delivery"; a failed wake is
    logged in watchdog.log and the row is restored, not re-archived.
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
ARCHIVE_FILENAME = "ops_alerts_archive.jsonl"
DEFAULT_ARCHIVE_MAX_BYTES = 5 * 1024 * 1024
ARCHIVE_SCHEMA_VERSION = 1
ARCHIVED_MARKER = "_archived"
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


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def default_archive_path(outbox_path: Path) -> Path:
    env_path = os.environ.get("YAMA_OPS_ALERTS_ARCHIVE_PATH", "").strip()
    return Path(env_path) if env_path else Path(outbox_path).parent / ARCHIVE_FILENAME


def _archive_max_bytes() -> int:
    try:
        value = int(os.environ.get("YAMA_OPS_ALERTS_ARCHIVE_MAX_BYTES", DEFAULT_ARCHIVE_MAX_BYTES))
        return value if value > 0 else DEFAULT_ARCHIVE_MAX_BYTES
    except ValueError:
        return DEFAULT_ARCHIVE_MAX_BYTES


def archive_record(entry: Dict[str, Any], archived_at: Optional[str] = None) -> Dict[str, Any]:
    """Map one outbox entry to the shared schema (mark_rejected.jsonl fields + source/action).
    action: "rejected" (commit / requeue / reply refused, row unchanged, nothing posted) | "forced" (correction
    or requeue with a reason via --force) | "alert" (every other ops alert)."""
    kind = str(entry.get("kind") or "unknown")
    if kind in ("commit_rejected", "requeue_blocked", "reply_rejected"):
        action = "rejected"
    elif kind in ("commit_forced", "requeue_forced", "reply_forced"):
        action = "forced"
    else:
        action = "alert"
    key = entry.get("event_id") or entry.get("channel")
    keys = None
    if not key and isinstance(entry.get("rejected"), list):
        # commit-batch validation: one outbox line for several events
        keys = [r.get("event_id") for r in entry["rejected"] if isinstance(r, dict) and r.get("event_id")]
    elif not key and isinstance(entry.get("event_ids"), list):
        # failed_with_landed_reply (reconcile): one outbox line for several events
        keys = [str(x) for x in entry["event_ids"] if x]
    # forced: the status the row had when the correction was attempted
    current = entry.get("previous_status") if action == "forced" else entry.get("current_status")
    now = archived_at or _utc_now_iso()
    record: Dict[str, Any] = {
        "schema": ARCHIVE_SCHEMA_VERSION,
        "at": entry.get("at") or now,
        "archived_at": now,
        "key": key,
        "attempted_status": entry.get("requested_status"),
        "current_status": current,
        "reason": entry.get("reason"),
        "source": entry.get("source"),
        "action": action,
        "kind": kind,
        "entry": entry,
    }
    if keys is not None:
        record["keys"] = keys
    if action == "forced":
        record["new_status"] = entry.get("new_status")
    return record


def archive_append(
    entries: List[Dict[str, Any]],
    archive_path: Path,
    max_bytes: Optional[int] = None,
) -> None:
    """Append archive records for `entries`, rotating to <archive>.1 first when the
    file would exceed max_bytes. Raises on failure (the caller decides fail-open)."""
    if not entries:
        return
    archive_path = Path(archive_path)
    limit = max_bytes if max_bytes and max_bytes > 0 else _archive_max_bytes()
    now = _utc_now_iso()
    data = "".join(
        json.dumps(archive_record(e, now), ensure_ascii=False, sort_keys=True) + "\n" for e in entries
    ).encode("utf-8")
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = archive_path.with_name(archive_path.name + ".lock")
    with open(lock_path, "a") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        try:
            try:
                size = archive_path.stat().st_size
            except FileNotFoundError:
                size = 0
            if size > 0 and size + len(data) > limit:
                os.replace(archive_path, archive_path.with_name(archive_path.name + ".1"))
            with open(archive_path, "ab") as f:
                f.write(data)
                f.flush()
                os.fsync(f.fileno())
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


def outbox_take(
    path: Path = DEFAULT_OUTBOX_PATH,
    archive_path: Optional[Path] = None,
    archive: bool = True,
) -> List[Dict[str, Any]]:
    """Read all entries and truncate the outbox (same flock as outbox_append).
    Unparseable lines are kept as {"kind": "unparseable_outbox_line", ...}.
    Taken entries are archived (see module docstring); an archive failure adds one
    archive_write_failed entry to the result instead of failing the take."""
    path = Path(path)
    entries = _outbox_take_raw(path)
    if not entries:
        return entries
    to_archive: List[Dict[str, Any]] = []
    for e in entries:
        if e.pop(ARCHIVED_MARKER, False) is not True:
            to_archive.append(e)
    if archive and to_archive:
        target = Path(archive_path) if archive_path else default_archive_path(path)
        try:
            if os.environ.get("YAMA_OPS_ARCHIVE_FORCE_FAIL") == "1":
                raise OSError("forced failure (test hook)")
            archive_append(to_archive, target)
        except Exception as exc:
            # Audit fix 2026-11-23 (#4): the rows that failed to archive carry an
            # explicit "_archived": false, so outbox-restore (wake failed) does
            # not mark them archived and the next take archives them.
            for e in to_archive:
                e[ARCHIVED_MARKER] = False
            entries.append(
                {
                    "kind": "archive_write_failed",
                    "at": _utc_now_iso(),
                    "archive_path": str(target),
                    "error": f"{type(exc).__name__}: {exc}"[:300],
                    "lost_count": len(to_archive),
                    "lost_kinds": sorted({str(e.get("kind") or "unknown") for e in to_archive}),
                }
            )
    return entries


def _outbox_take_raw(path: Path) -> List[Dict[str, Any]]:
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
    for name in ("outbox-take", "outbox-restore", "outbox-append"):
        p = sub.add_parser(name)
        p.add_argument(
            "--path", default=os.environ.get("YAMA_OPS_ALERTS_PATH", str(DEFAULT_OUTBOX_PATH))
        )
        if name == "outbox-append":
            p.add_argument("--kind", required=True)
            p.add_argument("--detail", default="{}")
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
                    # An explicit marker set by take is respected ("_archived": false =
                    # the archive write failed, so the next take must archive it).
                    # Without a marker the row was archived by the take that produced
                    # it; mark it so it is not archived twice (the archive_write_failed
                    # notice itself was never archived).
                    if ARCHIVED_MARKER not in entry and entry.get("kind") != "archive_write_failed":
                        entry = dict(entry, **{ARCHIVED_MARKER: True})
                    outbox_append(entry, Path(args.path))
            return 0
        if args.cmd == "outbox-append":
            detail = json.loads(args.detail or "{}")
            if not isinstance(detail, dict):
                raise ValueError("--detail must be a JSON object")
            kind = str(args.kind).strip()
            if not kind:
                raise ValueError("--kind must not be empty")
            entry = dict(detail)
            entry["kind"] = kind
            entry.setdefault("at", _utc_now_iso())
            outbox_append(entry, Path(args.path))
            return 0
    except Exception as exc:
        print(json.dumps({"ok": False, "error": f"{type(exc).__name__}: {exc}"}, ensure_ascii=False))
        return 2
    return 2


if __name__ == "__main__":
    sys.exit(main())
