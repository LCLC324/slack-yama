#!/usr/bin/env python3
"""
Yama Slack auto-join script
- Joins public channels where the owner (OWNER_USER_ID) is a member and Yama is not.
  No member-count requirement: a channel with only the owner is joined too.
- Opt-out: a channel Yama was a member of and is no longer (removed / left) is
  recorded in opted_out and never auto-joined again. Being manually re-invited
  (is_member becomes true) clears the opt-out.
- After a successful join, seeds recent top-level posts (last SEED_WINDOW_SECS,
  up to SEED_LIMIT) into the events DB as PENDING via the same path as backfill.py.
- State: state/auto_join_state.json {"known_member": [...], "opted_out": [...]}
  saved atomically (temp file + rename).
- Prints only "happenings" (joins, seed counts, opt-outs, errors). Silent otherwise.
- AUTO_JOIN_DRY_RUN=1: no join / seed / state save; prints the decisions only.
- Always exits 0 (callers treat this as best-effort).
"""

import json
import os
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple

from agent_tools import _call_with_retry, get_web_client
from backfill import message_to_event
from listener import (
    DEFAULT_DB_PATH,
    DEFAULT_ENV_PATH,
    get_db_connection,
    init_db,
    load_env_file,
    save_event,
    should_ignore_event,
)

OWNER_USER_ID = "U_OWNER"

SEED_WINDOW_SECS = 900
SEED_LIMIT = 20

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_STATE_PATH = BASE_DIR / "state" / "auto_join_state.json"

SEED_EXCLUDED_SUBTYPES = {
    "channel_join",
    "channel_leave",
    "message_changed",
    "message_deleted",
}

LIST_PAGE_LIMIT = 200
MEMBERS_PAGE_LIMIT = 200
MAX_PAGES = 50


def _log(message: str) -> None:
    print(f"[{datetime.now().astimezone().isoformat(timespec='seconds')}] [auto_join] {message}", flush=True)


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------


def empty_state() -> Dict[str, List[str]]:
    return {"known_member": [], "opted_out": []}


class StateLoadError(RuntimeError):
    """The state file and its .bak backup both exist but neither is readable.
    Auto-join must stop (fail-safe) instead of starting empty, because an empty
    start would forget opted_out and re-join channels Yama was removed from."""


def _bak_path(state_path: Path) -> Path:
    return state_path.with_name(state_path.name + ".bak")


def _parse_state(raw: Any) -> Optional[Dict[str, List[str]]]:
    """Sanity-check a parsed state payload. Valid only if it is a dict that has
    both keys (known_member / opted_out) and both values are lists. A count
    floor is deliberately NOT checked: opted_out legitimately returns to 0
    when a channel is manually re-invited."""
    if not isinstance(raw, dict):
        return None
    if "known_member" not in raw or "opted_out" not in raw:
        return None
    state = empty_state()
    for key in ("known_member", "opted_out"):
        vals = raw.get(key)
        if not isinstance(vals, list):
            return None
        state[key] = sorted({str(v) for v in vals if v})
    return state


def _read_state_file(path: Path) -> Optional[Dict[str, List[str]]]:
    """Read and sanity-check one state file. Returns None if missing/unreadable/invalid."""
    try:
        with path.open("r", encoding="utf-8") as f:
            raw = json.load(f)
    except (OSError, ValueError):
        return None
    return _parse_state(raw)


def load_state_with_status(
    state_path: Path = DEFAULT_STATE_PATH,
) -> Tuple[Dict[str, List[str]], str]:
    """Load state, falling back to the .bak backup. Returns (state, source) where
    source is "main", "bak", or "empty" (neither file exists: first run).
    Raises StateLoadError when at least one file exists but neither is readable."""
    state_path = Path(state_path)
    bak_path = _bak_path(state_path)
    if state_path.exists():
        state = _read_state_file(state_path)
        if state is not None:
            return state, "main"
    if bak_path.exists():
        state = _read_state_file(bak_path)
        if state is not None:
            return state, "bak"
    if not state_path.exists() and not bak_path.exists():
        return empty_state(), "empty"
    raise StateLoadError(
        f"state file and backup are both unreadable: {state_path} / {bak_path}"
    )


def load_state(state_path: Path = DEFAULT_STATE_PATH) -> Dict[str, List[str]]:
    state_path = Path(state_path)
    state, source = load_state_with_status(state_path)
    if source == "bak":
        # Restore immediately: write the recovered state back to the main file
        # (without rotating, so the good .bak is not overwritten by the broken main).
        save_state(state, state_path, _rotate=False)
    return state


def save_state(
    state: Dict[str, List[str]],
    state_path: Path = DEFAULT_STATE_PATH,
    _rotate: bool = True,
) -> None:
    """Atomically write the state file (temp file in the same dir + os.replace).

    Before replacing, the current main file is rotated to <name>.bak, but only
    when the current main file still passes the sanity check. A corrupted or
    shape-broken main file must never overwrite a good .bak."""
    state_path = Path(state_path)
    state_path.parent.mkdir(parents=True, exist_ok=True)
    data = {
        "known_member": sorted(set(state.get("known_member", []))),
        "opted_out": sorted(set(state.get("opted_out", []))),
    }
    if _rotate and state_path.exists() and _read_state_file(state_path) is not None:
        bak_path = _bak_path(state_path)
        fd_b, tmp_bak = tempfile.mkstemp(
            prefix=f"{bak_path.name}.tmp.", dir=str(state_path.parent)
        )
        try:
            with os.fdopen(fd_b, "wb") as out, state_path.open("rb") as src:
                out.write(src.read())
                out.flush()
                os.fsync(out.fileno())
            os.replace(tmp_bak, bak_path)
        except BaseException:
            try:
                os.unlink(tmp_bak)
            except OSError:
                pass
            raise
    fd, tmp_name = tempfile.mkstemp(
        prefix=f"{state_path.name}.tmp.", dir=str(state_path.parent)
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_name, state_path)
    except BaseException:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise


# ---------------------------------------------------------------------------
# Pure decision functions (unit-tested in tests/run_tests.py)
# ---------------------------------------------------------------------------


def update_membership_state(
    state: Dict[str, List[str]], channels: Iterable[Dict[str, Any]]
) -> Tuple[Dict[str, List[str]], List[str], List[str]]:
    """
    Apply observed membership to the state. Pure: does not mutate the input.
    - is_member true  -> add to known_member, remove from opted_out (manual re-invite)
    - is_member false and in known_member -> remove from known_member, add to opted_out
    Returns (new_state, newly_opted_out_ids, opt_out_cleared_ids).
    """
    known: Set[str] = set(state.get("known_member", []) or [])
    opted: Set[str] = set(state.get("opted_out", []) or [])
    newly_opted: List[str] = []
    cleared: List[str] = []

    for ch in channels:
        cid = ch.get("id")
        if not cid:
            continue
        if ch.get("is_member"):
            known.add(cid)
            if cid in opted:
                opted.discard(cid)
                cleared.append(cid)
        elif cid in known:
            known.discard(cid)
            if cid not in opted:
                opted.add(cid)
                newly_opted.append(cid)

    new_state = {"known_member": sorted(known), "opted_out": sorted(opted)}
    return new_state, newly_opted, cleared


def is_join_precandidate(channel: Dict[str, Any], opted_out: Iterable[str]) -> bool:
    """Checks that do not require the member list (used to avoid needless API calls)."""
    cid = channel.get("id")
    if not cid:
        return False
    if channel.get("is_archived"):
        return False
    if channel.get("is_member"):
        return False
    if cid in set(opted_out or []):
        return False
    return True


def should_join(
    channel: Dict[str, Any],
    opted_out: Iterable[str],
    member_ids: Iterable[str],
    owner_user_id: str = OWNER_USER_ID,
) -> bool:
    """
    Join iff: not archived, Yama not a member, not opted out, and the owner is a
    member. No member-count condition (owner alone is enough).
    """
    if not is_join_precandidate(channel, opted_out):
        return False
    return owner_user_id in set(member_ids or [])


# ---------------------------------------------------------------------------
# Slack helpers
# ---------------------------------------------------------------------------


def _next_cursor(resp: Any) -> str:
    meta = resp.get("response_metadata") or {}
    if not isinstance(meta, dict):
        return ""
    return (meta.get("next_cursor") or "").strip()


def list_public_channels(client: Any, sleep_fn: Optional[Callable[[float], None]] = None) -> List[Dict[str, Any]]:
    channels: List[Dict[str, Any]] = []
    cursor: Optional[str] = None
    for _page in range(MAX_PAGES):
        kwargs: Dict[str, Any] = {
            "types": "public_channel",
            "exclude_archived": True,
            "limit": LIST_PAGE_LIMIT,
        }
        if cursor:
            kwargs["cursor"] = cursor
        resp = _call_with_retry(lambda kw=kwargs: client.conversations_list(**kw), sleep_fn=sleep_fn)
        channels.extend(c for c in (resp.get("channels", []) or []) if isinstance(c, dict))
        cursor = _next_cursor(resp)
        if not cursor:
            break
    return channels


def channel_has_member(
    client: Any,
    channel_id: str,
    user_id: str,
    sleep_fn: Optional[Callable[[float], None]] = None,
) -> bool:
    cursor: Optional[str] = None
    for _page in range(MAX_PAGES):
        kwargs: Dict[str, Any] = {"channel": channel_id, "limit": MEMBERS_PAGE_LIMIT}
        if cursor:
            kwargs["cursor"] = cursor
        resp = _call_with_retry(lambda kw=kwargs: client.conversations_members(**kw), sleep_fn=sleep_fn)
        if user_id in (resp.get("members", []) or []):
            return True
        cursor = _next_cursor(resp)
        if not cursor:
            break
    return False


def _get_existing_channel_ts(channel_id: str, db_path: Path) -> Set[str]:
    """All ts already stored for channel_id (any event_id form), so seeding never
    duplicates a message captured via Socket Mode or backfill."""
    with get_db_connection(db_path) as conn:
        return {
            r["ts"]
            for r in conn.execute(
                "SELECT ts FROM events WHERE channel_id = ?", (channel_id,)
            ).fetchall()
        }


def select_seed_messages(
    messages: Iterable[Any],
    bot_user_id: Optional[str],
    existing_ts: Set[str],
    oldest_ts: float,
) -> List[Dict[str, Any]]:
    selected: List[Dict[str, Any]] = []
    for msg in messages:
        if not isinstance(msg, dict):
            continue
        ts = msg.get("ts")
        if not ts:
            continue
        thread_ts = msg.get("thread_ts")
        if thread_ts and thread_ts != ts:
            continue
        if msg.get("subtype") in SEED_EXCLUDED_SUBTYPES:
            continue
        if bot_user_id and msg.get("user") == bot_user_id:
            continue
        if ts in existing_ts:
            continue
        try:
            if float(ts) < oldest_ts:
                continue
        except (TypeError, ValueError):
            continue
        selected.append(msg)

    def _key(m: Dict[str, Any]) -> float:
        try:
            return float(m.get("ts", 0))
        except (TypeError, ValueError):
            return 0.0

    selected.sort(key=_key)
    return selected


def seed_channel(
    client: Any,
    channel_id: str,
    bot_user_id: Optional[str],
    db_path: Path,
    ignored_user_ids: Optional[Set[str]] = None,
    now: Optional[float] = None,
    sleep_fn: Optional[Callable[[float], None]] = None,
) -> int:
    current_now = now if now is not None else time.time()
    oldest_ts = current_now - SEED_WINDOW_SECS
    resp = _call_with_retry(
        lambda: client.conversations_history(
            channel=channel_id, limit=SEED_LIMIT, oldest=str(oldest_ts)
        ),
        sleep_fn=sleep_fn,
    )
    messages = list(resp.get("messages", []) or [])
    existing_ts = _get_existing_channel_ts(channel_id, db_path)

    inserted = 0
    for msg in select_seed_messages(messages, bot_user_id, existing_ts, oldest_ts):
        event = message_to_event(msg, channel_id)
        ignore, _reason = should_ignore_event(
            event, bot_user_id=bot_user_id, ignored_user_ids=ignored_user_ids
        )
        if ignore:
            continue
        if save_event(event, db_path=db_path):
            inserted += 1
    return inserted


# ---------------------------------------------------------------------------
# Main run
# ---------------------------------------------------------------------------


def run_auto_join(
    db_path: Path = DEFAULT_DB_PATH,
    env_path: Path = DEFAULT_ENV_PATH,
    state_path: Path = DEFAULT_STATE_PATH,
    client: Optional[Any] = None,
    dry_run: bool = False,
    now: Optional[float] = None,
    sleep_fn: Optional[Callable[[float], None]] = None,
) -> Dict[str, Any]:
    summary: Dict[str, Any] = {"joined": [], "would_join": [], "seeded": 0, "opted_out_added": [], "errors": 0}

    load_env_file(Path(env_path))
    active_client = client if client is not None else get_web_client(Path(env_path))

    try:
        loaded_state, load_source = load_state_with_status(state_path)
    except StateLoadError as exc:
        # Fail-safe: never start from an empty state when files exist but are
        # unreadable; that would forget opted_out and re-join removed channels.
        _log(f"error: {exc}; auto-join aborted")
        summary["errors"] += 1
        summary["state_error"] = True
        return summary
    if load_source == "bak":
        if dry_run:
            _log("dry-run: state main file unreadable; would restore from .bak backup")
        else:
            save_state(loaded_state, state_path, _rotate=False)
            _log("state restored from .bak backup (main file was unreadable)")

    channels = list_public_channels(active_client, sleep_fn=sleep_fn)
    names = {c.get("id"): c.get("name", "") for c in channels}

    state, newly_opted, cleared = update_membership_state(loaded_state, channels)
    for cid in newly_opted:
        prefix = "dry-run: would opt out" if dry_run else "opt-out added"
        _log(f"{prefix} #{names.get(cid, '')} ({cid}) (no longer a member)")
    for cid in cleared:
        prefix = "dry-run: would clear opt-out" if dry_run else "opt-out cleared"
        _log(f"{prefix} #{names.get(cid, '')} ({cid}) (member again)")
    summary["opted_out_added"] = newly_opted

    targets: List[Dict[str, Any]] = []
    for ch in channels:
        if not is_join_precandidate(ch, state["opted_out"]):
            continue
        cid = ch["id"]
        try:
            members = [OWNER_USER_ID] if channel_has_member(active_client, cid, OWNER_USER_ID, sleep_fn=sleep_fn) else []
        except Exception as exc:
            summary["errors"] += 1
            _log(f"error: conversations_members failed for #{ch.get('name', '')} ({cid}): {exc}")
            continue
        if should_join(ch, state["opted_out"], members):
            targets.append(ch)

    if dry_run:
        for ch in targets:
            _log(f"dry-run: would join #{ch.get('name', '')} ({ch['id']})")
            summary["would_join"].append(ch["id"])
        if not targets:
            _log("dry-run: no join targets")
        return summary

    state_dirty = state != loaded_state

    if targets:
        init_db(Path(db_path))
        auth_resp = _call_with_retry(lambda: active_client.auth_test(), sleep_fn=sleep_fn)
        bot_user_id = auth_resp.get("user_id") if auth_resp else None
        extra_ignored_str = os.environ.get("IGNORED_USER_IDS", "")
        ignored_user_ids: Set[str] = {u.strip() for u in extra_ignored_str.split(",") if u.strip()}

        for ch in targets:
            cid = ch["id"]
            cname = ch.get("name", "")
            try:
                active_client.conversations_join(channel=cid)
            except Exception as exc:
                summary["errors"] += 1
                _log(f"error: join failed #{cname} ({cid}): {exc}")
                continue
            summary["joined"].append(cid)
            if cid not in state["known_member"]:
                state["known_member"] = sorted(set(state["known_member"]) | {cid})
            state_dirty = True
            try:
                seeded = seed_channel(
                    active_client,
                    cid,
                    bot_user_id,
                    Path(db_path),
                    ignored_user_ids=ignored_user_ids,
                    now=now,
                    sleep_fn=sleep_fn,
                )
                summary["seeded"] += seeded
                _log(f"joined #{cname} ({cid}); seeded {seeded} event(s)")
            except Exception as exc:
                summary["errors"] += 1
                _log(f"joined #{cname} ({cid}); seed failed: {exc}")

    if state_dirty or not Path(state_path).exists():
        save_state(state, state_path)

    return summary


def main() -> int:
    try:
        dry_run = os.environ.get("AUTO_JOIN_DRY_RUN", "") == "1"
        db_path = Path(os.environ.get("YAMA_DB_PATH", str(DEFAULT_DB_PATH)))
        run_auto_join(db_path=db_path, dry_run=dry_run)
    except BaseException as exc:  # best-effort: never fail the caller
        try:
            _log(f"error: {type(exc).__name__}: {exc}")
        except Exception:
            pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
