#!/usr/bin/env python3
"""
Automated test suite for Yama Slack Autonomous Assistant.
Does NOT connect to real Slack; directly tests SQLite DB, filter functions,
dispatcher.sh (including flock concurrency), agent_tools.py state transitions, and watchdog.sh.
"""

import http.client
import io
import json
import os
import shutil
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import urllib.error
import uuid
from argparse import Namespace
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple
from unittest.mock import patch

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from slack_sdk.errors import SlackApiError  # noqa: E402

import agent_tools  # noqa: E402
import auto_join  # noqa: E402
import backfill  # noqa: E402
import owner_dm_poll  # noqa: E402
from agent_tools import (  # noqa: E402
    cmd_history,
    cmd_reply,
    commit_event,
    fetch_history,
    get_mode,
    get_processing_events,
    get_recorded_reply,
)
from backfill import TARGET_CHANNELS, run_backfill  # noqa: E402
from listener import (  # noqa: E402
    get_db_connection,
    handle_event_payload,
    init_db,
    save_event,
    write_heartbeat,
)

DISPATCHER_SH = BASE_DIR / "dispatcher.sh"
# Live hook script by default; YAMA_TEST_HOOK_SCRIPT points the hook tests at a
# staged copy before it is installed (stall fix 2026-11-22).
HOOK_SCRIPT_PATH = Path(
    os.environ.get("YAMA_TEST_HOOK_SCRIPT", "/home/hatch/hooks/scripts/slack-yama-dispatch.sh")
)
WATCHDOG_SH = BASE_DIR / "watchdog.sh"
AGENT_TOOLS_PY = BASE_DIR / "agent_tools.py"
PYTHON_BIN = BASE_DIR / "venv" / "bin" / "python"
if not PYTHON_BIN.exists():
    PYTHON_BIN = Path(sys.executable)


def count_events(db_path: Path, status: str = None) -> int:
    with get_db_connection(db_path) as conn:
        if status:
            row = conn.execute(
                "SELECT COUNT(*) AS cnt FROM events WHERE status = ?", (status,)
            ).fetchone()
        else:
            row = conn.execute("SELECT COUNT(*) AS cnt FROM events").fetchone()
        return int(row["cnt"])


def test_deduplication() -> str:
    """1. 同じ event_id を2回入れても1件になる（重複排除）"""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "slack_events.db"
        init_db(db_path)

        payload = {
            "event_id": "Ev_DUP_001",
            "event": {
                "type": "message",
                "channel": "C01234567",
                "user": "U_HUMAN_1",
                "text": "こんにちは Yama",
                "ts": "1700000001.000100",
            },
        }

        saved1, reason1 = handle_event_payload(payload, bot_user_id="U_YAMA_BOT", db_path=db_path)
        saved2, reason2 = handle_event_payload(payload, bot_user_id="U_YAMA_BOT", db_path=db_path)

        assert saved1 is True and reason1 == "saved", f"First insert failed: {saved1}, {reason1}"
        assert saved2 is False and reason2 == "duplicate_event_id", (
            f"Second insert should be rejected as duplicate: {saved2}, {reason2}"
        )
        total = count_events(db_path)
        assert total == 1, f"Expected 1 event in DB, got {total}"
        return "2回同一event_id投入 → 1回目保存(saved)・2回目破棄(duplicate_event_id)・DB件数=1件"


def test_filtering() -> str:
    """2. 他Bot発言はis_bot=1で保存し、自ボット発言・編集・削除イベントは除外する（フィルタ）"""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "slack_events.db"
        init_db(db_path)
        bot_uid = "U_YAMA_BOT"

        invalid_events = [
            (
                "self_bot_message",
                {
                    "event_id": "Ev_FILT_3",
                    "event": {
                        "type": "message",
                        "channel": "C01",
                        "user": bot_uid,
                        "bot_id": "B_YAMA",
                        "text": "Yama own message",
                        "ts": "1700000012.000100",
                    },
                },
            ),
            (
                "message_changed",
                {
                    "event_id": "Ev_FILT_4",
                    "event": {
                        "type": "message",
                        "subtype": "message_changed",
                        "channel": "C01",
                        "user": "U_HUMAN",
                        "text": "Edited message",
                        "ts": "1700000013.000100",
                    },
                },
            ),
            (
                "message_deleted",
                {
                    "event_id": "Ev_FILT_5",
                    "event": {
                        "type": "message",
                        "subtype": "message_deleted",
                        "channel": "C01",
                        "ts": "1700000014.000100",
                    },
                },
            ),
            (
                "channel_join",
                {
                    "event_id": "Ev_FILT_6",
                    "event": {
                        "type": "message",
                        "subtype": "channel_join",
                        "channel": "C01",
                        "user": "U_HUMAN",
                        "text": "joined channel",
                        "ts": "1700000015.000100",
                    },
                },
            ),
        ]

        for label, payload in invalid_events:
            saved, reason = handle_event_payload(payload, bot_user_id=bot_uid, db_path=db_path)
            assert saved is False, f"Filter failed to block {label} (reason={reason})"
            if label == "self_bot_message":
                assert reason == "self_bot_message", f"Expected self_bot_message, got {reason}"

        assert count_events(db_path) == 0, "Filtered events must not be stored in DB"

        # Confirm other bot messages ARE saved with is_bot=1
        other_bot_events = [
            {
                "event_id": "Ev_FILT_1",
                "event": {
                    "type": "message",
                    "channel": "C01",
                    "user": "U_NEIRO_BOT",
                    "bot_id": "B_NEIRO",
                    "text": "Bot message with bot_id",
                    "ts": "1700000010.000100",
                },
            },
            {
                "event_id": "Ev_FILT_2",
                "event": {
                    "type": "message",
                    "subtype": "bot_message",
                    "channel": "C01",
                    "user": "U_OTHER",
                    "text": "Bot message via subtype",
                    "ts": "1700000011.000100",
                },
            },
        ]
        for payload in other_bot_events:
            saved, reason = handle_event_payload(payload, bot_user_id=bot_uid, db_path=db_path)
            assert saved is True and reason == "saved", f"Other bot message was not saved: {reason}"

        # Confirm a normal human message IS saved with is_bot=0
        valid_payload = {
            "event_id": "Ev_VALID_1",
            "event": {
                "type": "message",
                "channel": "C01",
                "user": "U_HUMAN",
                "text": "通常の人間による投稿",
                "ts": "1700000020.000100",
            },
        }
        saved, reason = handle_event_payload(valid_payload, bot_user_id=bot_uid, db_path=db_path)
        assert saved is True and count_events(db_path) == 3

        with get_db_connection(db_path) as conn:
            rows = {
                r["event_id"]: r["is_bot"]
                for r in conn.execute("SELECT event_id, is_bot FROM events").fetchall()
            }
        assert rows["Ev_FILT_1"] == 1
        assert rows["Ev_FILT_2"] == 1
        assert rows["Ev_VALID_1"] == 0
        return "自ボット・編集・削除・システム通知を除外し、他Bot(bot_id/bot_message)はis_bot=1、通常投稿はis_bot=0で保存"


def test_dispatcher_transition_and_payload() -> str:
    """3. dispatcher が PENDING→PROCESSING にし、payload に内容が入る"""
    with tempfile.TemporaryDirectory() as tmp:
        state_dir = Path(tmp) / "state"
        state_dir.mkdir(parents=True, exist_ok=True)
        db_path = state_dir / "slack_events.db"
        payload_path = state_dir / "payload.json"
        lock_path = state_dir / "dispatcher.lock"
        mode_file = Path(tmp) / "mode.txt"
        mode_file.write_text("shadow\n", encoding="utf-8")

        init_db(db_path)

        env = os.environ.copy()
        env.update(
            {
                "YAMA_STATE_DIR": str(state_dir),
                "YAMA_DB_PATH": str(db_path),
                "YAMA_PAYLOAD_PATH": str(payload_path),
                "YAMA_LOCK_PATH": str(lock_path),
                "YAMA_MODE_FILE": str(mode_file),
                "YAMA_PYTHON_BIN": str(PYTHON_BIN),
            }
        )

        # Empty DB -> exit 1 / NO_EVENTS
        res_empty = subprocess.run(
            ["bash", str(DISPATCHER_SH)], env=env, capture_output=True, text=True
        )
        assert res_empty.returncode == 1 and res_empty.stdout.strip() == "NO_EVENTS", (
            f"Expected NO_EVENTS (exit 1), got rc={res_empty.returncode}, out={res_empty.stdout}"
        )

        # Insert 12 PENDING events (including 1 bot event at #2) to verify oldest-10 batching and is_bot propagation
        for i in range(1, 13):
            ev_body = {
                "type": "message",
                "channel": "C_BOARD",
                "user": f"U_{i:02d}",
                "text": f"Message #{i}",
                "ts": f"17000001{i:02d}.000000",
            }
            if i == 2:
                ev_body["bot_id"] = "B_NEIRO"
            handle_event_payload(
                {
                    "event_id": f"Ev_BATCH_{i:02d}",
                    "event": ev_body,
                },
                bot_user_id="U_YAMA_BOT",
                db_path=db_path,
            )

        res_disp = subprocess.run(
            ["bash", str(DISPATCHER_SH)], env=env, capture_output=True, text=True
        )
        assert res_disp.returncode == 0, f"Dispatcher failed: {res_disp.stderr}"
        assert res_disp.stdout.strip() == "DISPATCHED:10", (
            f"Expected DISPATCHED:10, got {res_disp.stdout.strip()}"
        )

        assert payload_path.exists(), "payload.json was not created"
        payload_data = json.loads(payload_path.read_text(encoding="utf-8"))
        assert payload_data["count"] == 10
        assert payload_data["mode"] == "shadow"
        assert len(payload_data["events"]) == 10
        assert payload_data["events"][0]["event_id"] == "Ev_BATCH_01"
        assert payload_data["events"][9]["event_id"] == "Ev_BATCH_10"
        assert all(ev["status"] == "PROCESSING" for ev in payload_data["events"])
        assert all("is_bot" in ev for ev in payload_data["events"])
        assert payload_data["events"][0]["is_bot"] == 0
        assert payload_data["events"][1]["is_bot"] == 1

        assert count_events(db_path, "PROCESSING") == 10
        assert count_events(db_path, "PENDING") == 2

        # Verify agent_tools.py pending subcommand returns the 10 PROCESSING events with is_bot
        proc_events = get_processing_events(db_path)
        assert len(proc_events) == 10
        assert all("is_bot" in ev for ev in proc_events)
        assert proc_events[0]["is_bot"] == 0
        assert proc_events[1]["is_bot"] == 1
        return "12件PENDING投入 → 古い順10件がPROCESSINGへ遷移・payload.json出力(is_bot=0/1含む)確認（残り2件PENDING維持、0件時はNO_EVENTS確認）"


def test_commit_transitions() -> str:
    """4. commit DONE / IGNORED / FAILED（リトライ3回未満は PENDING 戻し、以上で FAILED 確定）が正しく遷移する"""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "slack_events.db"
        init_db(db_path)

        for eid in ("Ev_DONE", "Ev_IGNORED", "Ev_RETRY"):
            handle_event_payload(
                {
                    "event_id": eid,
                    "event": {
                        "type": "message",
                        "channel": "C01",
                        "user": "U01",
                        "text": f"test {eid}",
                        "ts": "1700000200.000100",
                    },
                },
                db_path=db_path,
            )

        # Simulate dispatcher marking them PROCESSING
        with get_db_connection(db_path) as conn:
            conn.execute("UPDATE events SET status = 'PROCESSING'")
            conn.commit()

        # 1) DONE transition via CLI
        res_done = subprocess.run(
            [
                str(PYTHON_BIN),
                str(AGENT_TOOLS_PY),
                "--db",
                str(db_path),
                "commit",
                "--event-id",
                "Ev_DONE",
                "--status",
                "DONE",
            ],
            capture_output=True,
            text=True,
        )
        assert res_done.returncode == 0, f"CLI commit DONE failed: {res_done.stderr}"
        out_done = json.loads(res_done.stdout)["event"]
        assert out_done["status"] == "DONE" and out_done["retry_count"] == 0

        # 2) IGNORED transition
        ev_ignored = commit_event("Ev_IGNORED", "IGNORED", db_path=db_path)
        assert ev_ignored["status"] == "IGNORED" and ev_ignored["retry_count"] == 0

        # 3) FAILED retry 1 -> retry_count=1 (<3) -> PENDING
        ev_fail1 = commit_event("Ev_RETRY", "FAILED", error_message="err1", db_path=db_path)
        assert ev_fail1["retry_count"] == 1 and ev_fail1["status"] == "PENDING", (
            f"Retry 1 unexpected: {ev_fail1}"
        )

        # 4) FAILED retry 2 -> retry_count=2 (<3) -> PENDING
        ev_fail2 = commit_event("Ev_RETRY", "FAILED", error_message="err2", db_path=db_path)
        assert ev_fail2["retry_count"] == 2 and ev_fail2["status"] == "PENDING", (
            f"Retry 2 unexpected: {ev_fail2}"
        )

        # 5) FAILED retry 3 -> retry_count=3 (>=3) -> permanent FAILED (DLQ)
        ev_fail3 = commit_event("Ev_RETRY", "FAILED", error_message="err3_fatal", db_path=db_path)
        assert ev_fail3["retry_count"] == 3 and ev_fail3["status"] == "FAILED", (
            f"Retry 3 unexpected: {ev_fail3}"
        )
        assert ev_fail3["error_message"] == "err3_fatal"

        return "DONE/IGNORED即時確定、FAILED 1〜2回目はPENDING差戻し(retry_count=1,2)、3回目でFAILED確定(retry_count=3)"


def test_dispatcher_flock_exclusivity() -> str:
    """5. dispatcher の二重起動が flock で防がれる"""
    with tempfile.TemporaryDirectory() as tmp:
        state_dir = Path(tmp) / "state"
        state_dir.mkdir(parents=True, exist_ok=True)
        db_path = state_dir / "slack_events.db"
        payload_path = state_dir / "payload.json"
        lock_path = state_dir / "dispatcher.lock"

        init_db(db_path)
        handle_event_payload(
            {
                "event_id": "Ev_LOCK_1",
                "event": {
                    "type": "message",
                    "channel": "C01",
                    "user": "U01",
                    "text": "lock test",
                    "ts": "1700000300.000100",
                },
            },
            db_path=db_path,
        )

        env1 = os.environ.copy()
        env1.update(
            {
                "YAMA_STATE_DIR": str(state_dir),
                "YAMA_DB_PATH": str(db_path),
                "YAMA_PAYLOAD_PATH": str(payload_path),
                "YAMA_LOCK_PATH": str(lock_path),
                "YAMA_PYTHON_BIN": str(PYTHON_BIN),
                "YAMA_HOLD_LOCK_SEC": "2",
            }
        )

        env2 = env1.copy()
        env2.pop("YAMA_HOLD_LOCK_SEC", None)

        # Start first dispatcher which holds the lock for 2 seconds
        proc1 = subprocess.Popen(
            ["bash", str(DISPATCHER_SH)],
            env=env1,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        # Give proc1 a moment to acquire flock
        time.sleep(0.3)

        # Second dispatcher invoked while proc1 holds lock -> must immediately return exit 2 / LOCKED
        proc2 = subprocess.run(
            ["bash", str(DISPATCHER_SH)],
            env=env2,
            capture_output=True,
            text=True,
        )
        out1, err1 = proc1.communicate(timeout=5)

        assert proc2.returncode == 2 and proc2.stdout.strip() == "LOCKED", (
            f"Second dispatcher was not blocked by flock: rc={proc2.returncode}, out={proc2.stdout}"
        )
        assert proc1.returncode == 0 and out1.strip() == "DISPATCHED:1", (
            f"First dispatcher failed: rc={proc1.returncode}, out={out1}, err={err1}"
        )
        return "先行dispatcher実行中(flock保持)の後発起動がLOCKED(exit 2)で即座にスキップされることを確認"


def test_watchdog_and_mode() -> str:
    """補足検証: watchdog.sh の3分閾値判定・ログ記録および mode 取得"""
    with tempfile.TemporaryDirectory() as tmp:
        state_dir = Path(tmp) / "state"
        state_dir.mkdir(parents=True, exist_ok=True)
        hb_path = state_dir / "heartbeat.json"
        log_path = state_dir / "watchdog.log"

        # 1) Fresh heartbeat -> no restart
        write_heartbeat(connected=True, bot_user_id="U_YAMA_BOT", heartbeat_path=hb_path)
        env = os.environ.copy()
        env.update(
            {
                "YAMA_STATE_DIR": str(state_dir),
                "YAMA_HEARTBEAT_PATH": str(hb_path),
                "YAMA_WATCHDOG_LOG": str(log_path),
                "YAMA_PYTHON_BIN": str(PYTHON_BIN),
                "YAMA_RESTART_CMD": "echo MOCK_RESTART_EXECUTED",
            }
        )
        res_ok = subprocess.run(
            ["bash", str(WATCHDOG_SH)], env=env, capture_output=True, text=True
        )
        assert res_ok.returncode == 0
        log_text = log_path.read_text(encoding="utf-8")
        assert "[OK] Heartbeat healthy" in log_text

        # 2) Stale heartbeat (4 minutes old >= 180s) -> triggers restart and logs
        stale_data = {
            "timestamp": time.time() - 240,
            "connected": True,
            "bot_user_id": "U_YAMA_BOT",
        }
        hb_path.write_text(json.dumps(stale_data), encoding="utf-8")
        res_stale = subprocess.run(
            ["bash", str(WATCHDOG_SH)], env=env, capture_output=True, text=True
        )
        assert res_stale.returncode == 0
        log_text = log_path.read_text(encoding="utf-8")
        assert "MOCK_RESTART_EXECUTED" in log_text and "STALE:" in log_text

        # 3) Verify config/mode.txt returns a valid mode (phase-dependent)
        assert get_mode() in ("shadow", "mention", "full")
        return "heartbeat正常時スキップ・3分超停止時の再起動コマンド実行＆watchdog.log出力・mode.txt(有効値)読込を確認"


def test_history_retry_recovery() -> str:
    """7. history: 1回目の接続系例外の後に成功する状況で、リトライにより source=slack_api で返る"""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "slack_events.db"
        init_db(db_path)

        sleeps: List[float] = []
        calls = {"count": 0}

        class FlakyClient:
            def conversations_history(self, channel: str, limit: int = 20, include_all_metadata: bool = False) -> Dict[str, Any]:
                calls["count"] += 1
                if calls["count"] == 1:
                    raise http.client.IncompleteRead(b"partial")
                return {
                    "ok": True,
                    "messages": [
                        {"user": "U_API_1", "text": "hello from api", "ts": "1700000500.000100"}
                    ],
                }

            def conversations_replies(self, channel: str, ts: str, limit: int = 20, include_all_metadata: bool = False) -> Dict[str, Any]:
                calls["count"] += 1
                if calls["count"] < 3:
                    raise urllib.error.URLError("Remote end closed connection")
                return {
                    "ok": True,
                    "messages": [
                        {
                            "user": "U_API_2",
                            "text": "thread reply from api",
                            "ts": "1700000501.000100",
                            "thread_ts": ts,
                        }
                    ],
                }

        # Case 1: 1st call fails (IncompleteRead), 2nd succeeds via cmd_history
        stdout_buf = io.StringIO()
        stderr_buf = io.StringIO()
        args = Namespace(channel="C_RETRY", thread_ts=None, limit=20, db=str(db_path))
        with patch.object(agent_tools, "get_web_client", return_value=FlakyClient()), patch.object(
            agent_tools.time, "sleep", side_effect=lambda s: sleeps.append(s)
        ), redirect_stdout(stdout_buf), redirect_stderr(stderr_buf):
            rc = cmd_history(args)

        assert rc == 0, f"Expected rc=0, got {rc}, stderr={stderr_buf.getvalue()}"
        assert stderr_buf.getvalue() == "", f"Expected empty stderr, got {stderr_buf.getvalue()}"
        out = json.loads(stdout_buf.getvalue())
        assert out["ok"] is True
        assert out["source"] == "slack_api"
        assert out["count"] == 1
        assert out["messages"][0]["text"] == "hello from api"
        assert calls["count"] == 2
        assert sleeps == [1.0], f"Expected [1.0] backoff, got {sleeps}"

        # Case 2: 1st and 2nd calls fail (URLError), 3rd succeeds (thread_ts specified)
        calls["count"] = 0
        sleeps.clear()
        res2 = fetch_history(
            channel="C_RETRY",
            thread_ts="1700000500.000100",
            limit=10,
            db_path=db_path,
            client=FlakyClient(),
            sleep_fn=lambda s: sleeps.append(s),
        )
        assert res2["ok"] is True
        assert res2["source"] == "slack_api"
        assert res2["count"] == 1
        assert res2["messages"][0]["text"] == "thread reply from api"
        assert calls["count"] == 3
        assert sleeps == [1.0, 2.0], f"Expected [1.0, 2.0] backoffs, got {sleeps}"

        # Case 3: SlackApiError is NOT retried and immediately fails
        api_calls = {"count": 0}

        class ApiErrorClient:
            def conversations_history(self, channel: str, limit: int = 20, include_all_metadata: bool = False) -> Dict[str, Any]:
                api_calls["count"] += 1
                raise SlackApiError(
                    message="channel_not_found",
                    response={"ok": False, "error": "channel_not_found"},
                )

        stdout_err = io.StringIO()
        stderr_err = io.StringIO()
        with patch.object(agent_tools, "get_web_client", return_value=ApiErrorClient()), redirect_stdout(
            stdout_err
        ), redirect_stderr(stderr_err):
            rc_err = cmd_history(args)
        assert rc_err == 1
        assert api_calls["count"] == 1, f"SlackApiError must not retry, called {api_calls['count']} times"
        err_json = json.loads(stderr_err.getvalue())
        assert err_json["ok"] is False and err_json["error"] == "channel_not_found"

        return "1回目(IncompleteRead)・1〜2回目(URLError)失敗後にリトライ(1s→2s)で回復しsource=slack_api返却、SlackApiErrorはリトライせず即時エラーを確認"


def test_history_local_db_fallback() -> str:
    """8. history: 接続系例外が3回続いた場合、ローカルDBの内容を source=local_db_fallback で時系列返却する"""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "slack_events.db"
        init_db(db_path)

        # Insert events out of ts order and across channels/threads to verify filtering and chronological ordering
        events_to_insert = [
            {
                "event_id": "Ev_FB_2",
                "event": {
                    "type": "message",
                    "channel": "C_FALLBACK",
                    "user": "U_BOT_OTHER",
                    "bot_id": "B_OTHER",
                    "text": "2番目のメッセージ(Bot)",
                    "ts": "1700000602.000000",
                    "thread_ts": "1700000601.000000",
                },
            },
            {
                "event_id": "Ev_FB_1",
                "event": {
                    "type": "message",
                    "channel": "C_FALLBACK",
                    "user": "U_HUMAN_1",
                    "text": "1番目のメッセージ(親)",
                    "ts": "1700000601.000000",
                },
            },
            {
                "event_id": "Ev_FB_3",
                "event": {
                    "type": "message",
                    "channel": "C_FALLBACK",
                    "user": "U_HUMAN_2",
                    "text": "3番目のメッセージ(別スレッド外)",
                    "ts": "1700000603.000000",
                },
            },
            {
                "event_id": "Ev_FB_OTHER_CH",
                "event": {
                    "type": "message",
                    "channel": "C_OTHER",
                    "user": "U_HUMAN_9",
                    "text": "別チャンネル",
                    "ts": "1700000604.000000",
                },
            },
        ]
        for payload in events_to_insert:
            saved, _ = handle_event_payload(payload, bot_user_id="U_YAMA_BOT", db_path=db_path)
            assert saved is True

        calls = {"count": 0}
        sleeps: List[float] = []

        class AlwaysDownClient:
            def conversations_history(self, channel: str, limit: int = 20, include_all_metadata: bool = False) -> Dict[str, Any]:
                calls["count"] += 1
                raise socket.timeout("timed out")

            def conversations_replies(self, channel: str, ts: str, limit: int = 20, include_all_metadata: bool = False) -> Dict[str, Any]:
                calls["count"] += 1
                raise ConnectionResetError("Remote end closed connection without response")

        # 1) Channel history fallback via cmd_history
        stdout_buf = io.StringIO()
        stderr_buf = io.StringIO()
        args_ch = Namespace(channel="C_FALLBACK", thread_ts=None, limit=20, db=str(db_path))
        with patch.object(agent_tools, "get_web_client", return_value=AlwaysDownClient()), patch.object(
            agent_tools.time, "sleep", side_effect=lambda s: sleeps.append(s)
        ), redirect_stdout(stdout_buf), redirect_stderr(stderr_buf):
            rc = cmd_history(args_ch)

        assert rc == 0, f"Expected rc=0 on fallback, got {rc}"
        assert stderr_buf.getvalue() == "", f"Expected no stderr output on fallback, got: {stderr_buf.getvalue()}"
        assert calls["count"] == 5, f"Expected 5 attempts before fallback, got {calls['count']}"
        assert sleeps == [1.0, 2.0, 4.0, 8.0], f"Expected backoffs [1.0, 2.0, 4.0, 8.0], got {sleeps}"

        out_ch = json.loads(stdout_buf.getvalue())
        assert out_ch["ok"] is True
        assert out_ch["source"] == "local_db_fallback"
        assert out_ch["channel"] == "C_FALLBACK"
        assert out_ch["count"] == 3
        msgs = out_ch["messages"]
        assert [m["ts"] for m in msgs] == [
            "1700000601.000000",
            "1700000602.000000",
            "1700000603.000000",
        ], f"Messages not in chronological order: {msgs}"
        for m in msgs:
            assert set(m.keys()) == {"user", "text", "ts", "thread_ts", "is_bot"}
        assert msgs[0]["user"] == "U_HUMAN_1" and msgs[0]["is_bot"] == 0 and msgs[0]["thread_ts"] is None
        assert msgs[1]["user"] == "U_BOT_OTHER" and msgs[1]["is_bot"] == 1 and msgs[1]["thread_ts"] == "1700000601.000000"

        # 2) Thread history fallback (thread_ts specified -> returns parent + thread reply in chronological order)
        stdout_th = io.StringIO()
        stderr_th = io.StringIO()
        args_th = Namespace(
            channel="C_FALLBACK",
            thread_ts="1700000601.000000",
            limit=20,
            db=str(db_path),
        )
        with patch.object(agent_tools, "get_web_client", return_value=AlwaysDownClient()), patch.object(
            agent_tools.time, "sleep", side_effect=lambda s: None
        ), redirect_stdout(stdout_th), redirect_stderr(stderr_th):
            rc_th = cmd_history(args_th)

        assert rc_th == 0 and stderr_th.getvalue() == ""
        out_th = json.loads(stdout_th.getvalue())
        assert out_th["ok"] is True
        assert out_th["source"] == "local_db_fallback"
        assert out_th["thread_ts"] == "1700000601.000000"
        assert out_th["count"] == 2
        assert [m["ts"] for m in out_th["messages"]] == [
            "1700000601.000000",
            "1700000602.000000",
        ]

        # 3) Empty channel fallback -> returns ok=True, source=local_db_fallback, messages=[]
        stdout_empty = io.StringIO()
        stderr_empty = io.StringIO()
        args_empty = Namespace(channel="C_NO_EVENTS", thread_ts=None, limit=20, db=str(db_path))
        with patch.object(agent_tools, "get_web_client", return_value=AlwaysDownClient()), patch.object(
            agent_tools.time, "sleep", side_effect=lambda s: None
        ), redirect_stdout(stdout_empty), redirect_stderr(stderr_empty):
            rc_empty = cmd_history(args_empty)

        assert rc_empty == 0 and stderr_empty.getvalue() == ""
        out_empty = json.loads(stdout_empty.getvalue())
        assert out_empty["ok"] is True
        assert out_empty["source"] == "local_db_fallback"
        assert out_empty["count"] == 0
        assert out_empty["messages"] == []

        return "3回接続失敗後にローカルDBから時系列取得(user/text/ts/thread_ts/is_bot・スレッド絞込・該当なし時は空配列)・source=local_db_fallback・stderr出力なしを確認"


def test_backfill() -> str:
    """9. backfill: (a)未保存投稿の投入とis_bot付与・スレッド返信取得、(b)既存投稿の二重投入防止・時刻閾値制御、(c)自Bot投稿・編集イベント除外"""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "slack_events.db"
        init_db(db_path)

        ch_welcome = "C_WELCOME"
        ch_board = "C_ALL"
        assert TARGET_CHANNELS == (ch_welcome, ch_board)

        bot_uid = "U_YAMA_BOT"
        base_now = 1700100000.0

        # Pre-populate ch_welcome with 2 existing events:
        # 1) Saved via save_event default fallback (channel_ts) at ts = 1700099000.000100 (this is max_ts for ch_welcome)
        save_event(
            {
                "type": "message",
                "channel": ch_welcome,
                "user": "U_EXISTING_1",
                "text": "Already in DB with channel_ts event_id",
                "ts": "1700099000.000100",
            },
            db_path=db_path,
        )
        # 2) Saved via Socket Mode handle_event_payload (Ev_... event_id) at ts = 1700098500.000100
        handle_event_payload(
            {
                "event_id": "Ev_SOCKET_EXISTING",
                "event": {
                    "type": "message",
                    "channel": ch_welcome,
                    "user": "U_EXISTING_2",
                    "text": "Already in DB with Socket Mode Ev_ id",
                    "ts": "1700098500.000100",
                },
            },
            bot_user_id=bot_uid,
            db_path=db_path,
        )
        assert count_events(db_path) == 2

        # For ch_welcome, max_ts in DB is 1700099000.000100 -> cutoff is 1700099000.000100 - 3600 = 1700095400.000100
        # For ch_board, DB is empty -> cutoff is base_now - 86400 = 1700013600.0

        class MockBackfillClient:
            def auth_test(self) -> Dict[str, Any]:
                return {"ok": True, "user_id": bot_uid}

            def conversations_history(self, channel: str, limit: int = 30, **kwargs: Any) -> Dict[str, Any]:
                assert limit == 30, f"Expected limit=30, got {limit}"
                if channel == ch_welcome:
                    return {
                        "ok": True,
                        "messages": [
                            # (b) Already in DB (channel_ts primary key)
                            {
                                "type": "message",
                                "user": "U_EXISTING_1",
                                "text": "Already in DB with channel_ts event_id",
                                "ts": "1700099000.000100",
                                "client_msg_id": "CLIENT-MSG-IGNORE-ME",
                            },
                            # (b) Already in DB (Socket Mode Ev_ id)
                            {
                                "type": "message",
                                "user": "U_EXISTING_2",
                                "text": "Already in DB with Socket Mode Ev_ id",
                                "ts": "1700098500.000100",
                            },
                            # (a) Missing human message with reply_count > 0 -> should insert with is_bot=0 and trigger conversations_replies
                            {
                                "type": "message",
                                "user": "U_HUMAN_NEW",
                                "text": "Missing human parent message",
                                "ts": "1700099100.000100",
                                "reply_count": 2,
                                "client_msg_id": "CLIENT-MSG-HUMAN-NEW",
                            },
                            # (a) Missing other-bot message -> should insert with is_bot=1
                            {
                                "type": "message",
                                "user": "U_OTHER_BOT",
                                "bot_id": "B_OTHER_BOT",
                                "text": "Missing question from another bot",
                                "ts": "1700099200.000100",
                            },
                            # (c) Self-bot message -> must be ignored
                            {
                                "type": "message",
                                "user": bot_uid,
                                "bot_id": "B_YAMA",
                                "text": "Yama own reply",
                                "ts": "1700099300.000100",
                            },
                            # (c) Edit event (subtype=message_changed) -> must be ignored
                            {
                                "type": "message",
                                "subtype": "message_changed",
                                "user": "U_HUMAN_NEW",
                                "text": "Edited content",
                                "ts": "1700099400.000100",
                            },
                            # Older than max_ts - 3600 -> must be ignored
                            {
                                "type": "message",
                                "user": "U_OLD",
                                "text": "Too old for ch_welcome (>1h before max_ts)",
                                "ts": "1700095000.000100",
                            },
                        ],
                    }
                if channel == ch_board:
                    return {
                        "ok": True,
                        "messages": [
                            # (a) Missing bot_message via subtype in empty channel (within 24h) -> is_bot=1
                            {
                                "type": "message",
                                "subtype": "bot_message",
                                "user": "U_BOARD_BOT",
                                "text": "Board bot update",
                                "ts": "1700090000.000100",
                            },
                            # Older than 24h in empty channel -> must be ignored
                            {
                                "type": "message",
                                "user": "U_BOARD_OLD",
                                "text": "Older than 24h in empty channel",
                                "ts": "1700000000.000100",
                            },
                        ],
                    }
                raise AssertionError(f"Unexpected channel: {channel}")

            def conversations_replies(self, channel: str, ts: str, limit: int = 30) -> Dict[str, Any]:
                assert channel == ch_welcome and ts == "1700099100.000100" and limit == 30
                return {
                    "ok": True,
                    "messages": [
                        # Parent message (also in conversations_history; must not be duplicated)
                        {
                            "type": "message",
                            "user": "U_HUMAN_NEW",
                            "text": "Missing human parent message",
                            "ts": "1700099100.000100",
                            "thread_ts": "1700099100.000100",
                        },
                        # (a) Missing thread reply by another bot -> should insert with is_bot=1
                        {
                            "type": "message",
                            "user": "U_OTHER_BOT",
                            "bot_id": "B_OTHER_BOT",
                            "text": "Thread reply from other bot",
                            "ts": "1700099150.000100",
                            "thread_ts": "1700099100.000100",
                        },
                        # (c) Self-bot thread reply -> must be ignored
                        {
                            "type": "message",
                            "user": bot_uid,
                            "text": "Self thread reply",
                            "ts": "1700099160.000100",
                            "thread_ts": "1700099100.000100",
                        },
                    ],
                }

        # 1st backfill run via CLI main() to verify stdout JSON and exit code 0
        stdout_buf = io.StringIO()
        stderr_buf = io.StringIO()
        with patch.object(backfill, "get_web_client", return_value=MockBackfillClient()), patch.object(
            backfill.time, "time", return_value=base_now
        ), redirect_stdout(stdout_buf), redirect_stderr(stderr_buf):
            rc = backfill.main(["--db", str(db_path)])

        assert rc == 0, f"Expected exit 0, got {rc}"
        assert stderr_buf.getvalue() == "", f"Unexpected stderr: {stderr_buf.getvalue()}"
        summary1 = json.loads(stdout_buf.getvalue())
        assert summary1["inserted"] == 4, f"Expected 4 inserted events, got {summary1}"
        assert summary1["channels"] == {
            ch_welcome: 3,  # human parent + thread bot reply + channel bot post
            ch_board: 1,    # board bot post
        }

        # Check DB contents: 2 pre-existing + 4 newly inserted = 6 total
        assert count_events(db_path) == 6
        with get_db_connection(db_path) as conn:
            rows = {
                r["event_id"]: dict(r)
                for r in conn.execute("SELECT * FROM events").fetchall()
            }

        # Verify event_id uses channel_ts fallback and is_bot is properly set
        ev_human = rows[f"{ch_welcome}_1700099100.000100"]
        assert ev_human["user_id"] == "U_HUMAN_NEW"
        assert ev_human["is_bot"] == 0
        assert ev_human["status"] == "PENDING"

        ev_thread_bot = rows[f"{ch_welcome}_1700099150.000100"]
        assert ev_thread_bot["user_id"] == "U_OTHER_BOT"
        assert ev_thread_bot["is_bot"] == 1
        assert ev_thread_bot["thread_ts"] == "1700099100.000100"

        ev_ch_bot = rows[f"{ch_welcome}_1700099200.000100"]
        assert ev_ch_bot["user_id"] == "U_OTHER_BOT"
        assert ev_ch_bot["is_bot"] == 1

        ev_board_bot = rows[f"{ch_board}_1700090000.000100"]
        assert ev_board_bot["user_id"] == "U_BOARD_BOT"
        assert ev_board_bot["is_bot"] == 1

        # Verify self-bot, edit event, and out-of-window events were NOT inserted
        assert f"{ch_welcome}_1700099300.000100" not in rows
        assert f"{ch_welcome}_1700099400.000100" not in rows
        assert f"{ch_welcome}_1700095000.000100" not in rows
        assert f"{ch_board}_1700000000.000100" not in rows

        # (b) 2nd backfill run with same data -> 0 inserted (no duplicate insertion)
        summary2 = run_backfill(
            db_path=db_path,
            client=MockBackfillClient(),
            now=base_now,
        )
        assert summary2["inserted"] == 0, f"Expected 0 on 2nd run, got {summary2}"
        assert summary2["channels"] == {ch_welcome: 0, ch_board: 0}
        assert count_events(db_path) == 6

        # Verify partial channel error isolation: 1st channel fails, 2nd channel succeeds, exit 0 + stderr logged
        class PartialFailClient(MockBackfillClient):
            def conversations_history(self, channel: str, limit: int = 30, **kwargs: Any) -> Dict[str, Any]:
                if channel == ch_welcome:
                    raise SlackApiError(
                        message="channel_not_found",
                        response={"ok": False, "error": "channel_not_found"},
                    )
                return {
                    "ok": True,
                    "messages": [
                        {
                            "type": "message",
                            "user": "U_BOARD_2",
                            "text": "New message when welcome fails",
                            "ts": "1700095000.000100",
                        }
                    ],
                }

        stdout_pf = io.StringIO()
        stderr_pf = io.StringIO()
        with patch.object(backfill, "get_web_client", return_value=PartialFailClient()), patch.object(
            backfill.time, "time", return_value=base_now
        ), redirect_stdout(stdout_pf), redirect_stderr(stderr_pf):
            rc_pf = backfill.main(["--db", str(db_path)])

        assert rc_pf == 0
        summary_pf = json.loads(stdout_pf.getvalue())
        assert summary_pf["inserted"] == 1
        assert summary_pf["channels"] == {ch_welcome: 0, ch_board: 1}
        assert ch_welcome in stderr_pf.getvalue()
        assert count_events(db_path) == 7

        return "(a)未保存投稿(人間is_bot=0・他Bot/スレッド返信is_bot=1)の補完投入、(b)既存投稿の二重投入防止・時間窓フィルタ、(c)自Bot投稿・編集イベント除外を確認"


def test_reply_idempotency() -> str:
    """10. reply: 同じ event_id で2回呼んでも実投稿は1回だけ行われ、2回目は deduplicated=true で記録済み reply_ts を返す（--event-id 無しは従来通り毎回投稿）"""
    import hashlib

    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "slack_events.db"
        init_db(db_path)

        text_file = Path(tmp) / "reply_msg.txt"
        reply_text = "Yanayanaさん、ご質問ありがとうございます。Yamaの回答です。"
        text_file.write_text(reply_text + "\n", encoding="utf-8")

        post_calls: List[Dict[str, Any]] = []

        class MockReplyClient:
            def chat_postMessage(self, **kwargs: Any) -> Dict[str, Any]:
                post_calls.append(kwargs)
                call_idx = len(post_calls)
                return {
                    "ok": True,
                    "channel": kwargs["channel"],
                    "ts": f"1794472780.00000{call_idx}",
                    "message": {"text": kwargs["text"], "ts": f"1794472780.00000{call_idx}"},
                }

        # 1) First call with --event-id Ev0CH0PMRGF8 -> posts to Slack and records in replies table
        stdout1 = io.StringIO()
        stderr1 = io.StringIO()
        with patch.object(agent_tools, "get_web_client", return_value=MockReplyClient()), redirect_stdout(
            stdout1
        ), redirect_stderr(stderr1):
            rc1 = agent_tools.main(
                [
                    "--db",
                    str(db_path),
                    "reply",
                    "--channel",
                    "C_ALL",
                    "--text-file",
                    str(text_file),
                    "--thread-ts",
                    "1794472708.249819",
                    "--event-id",
                    "Ev0CH0PMRGF8",
                    "--force",
                    "--reason",
                    "test fixture: events row intentionally absent (pre-candidate-28 call shape)",
                ]
            )

        assert rc1 == 0, f"1st reply failed: rc={rc1}, stderr={stderr1.getvalue()}"
        assert len(post_calls) == 1, f"Expected 1 chat_postMessage call, got {len(post_calls)}"
        out1 = json.loads(stdout1.getvalue())
        assert out1["ok"] is True
        assert out1["ts"] == "1794472780.000001"
        assert out1["thread_ts"] == "1794472708.249819"
        assert out1.get("deduplicated") is False

        # Verify DB row in replies table
        rec = get_recorded_reply("Ev0CH0PMRGF8", db_path=db_path)
        assert rec is not None, "Expected reply record in replies table"
        assert rec["event_id"] == "Ev0CH0PMRGF8"
        assert rec["channel_id"] == "C_ALL"
        assert rec["thread_ts"] == "1794472708.249819"
        assert rec["reply_ts"] == "1794472780.000001"
        assert rec["text_hash"] == hashlib.sha256(reply_text.encode("utf-8")).hexdigest()
        assert rec["created_at"] is not None

        # 2) Second call with the SAME --event-id Ev0CH0PMRGF8 -> must NOT call chat_postMessage,
        #    returns recorded reply_ts and deduplicated=True
        stdout2 = io.StringIO()
        stderr2 = io.StringIO()
        with patch.object(agent_tools, "get_web_client", return_value=MockReplyClient()), redirect_stdout(
            stdout2
        ), redirect_stderr(stderr2):
            rc2 = agent_tools.main(
                [
                    "--db",
                    str(db_path),
                    "reply",
                    "--channel",
                    "C_ALL",
                    "--text-file",
                    str(text_file),
                    "--thread-ts",
                    "1794472708.249819",
                    "--event-id",
                    "Ev0CH0PMRGF8",
                    "--force",
                    "--reason",
                    "test fixture: events row intentionally absent (pre-candidate-28 call shape)",
                ]
            )

        assert rc2 == 0, f"2nd reply failed: rc={rc2}, stderr={stderr2.getvalue()}"
        assert len(post_calls) == 1, (
            f"Duplicate reply was posted! Expected still 1 chat_postMessage call, got {len(post_calls)}"
        )
        out2 = json.loads(stdout2.getvalue())
        assert out2["ok"] is True
        assert out2["deduplicated"] is True
        assert out2["ts"] == "1794472780.000001"
        assert out2["channel"] == "C_ALL"
        assert out2["thread_ts"] == "1794472708.249819"

        # 3) --no-event-id call (no event, candidate 28 form of the old id-less call) -> posts normally every time
        with get_db_connection(db_path) as conn:
            conn.execute("DELETE FROM replies")
            conn.commit()
        stdout3 = io.StringIO()
        stderr3 = io.StringIO()
        with patch.object(agent_tools, "get_web_client", return_value=MockReplyClient()), redirect_stdout(
            stdout3
        ), redirect_stderr(stderr3):
            rc3 = agent_tools.main(
                [
                    "--db",
                    str(db_path),
                    "reply",
                    "--channel",
                    "C_ALL",
                    "--text-file",
                    str(text_file),
                    "--thread-ts",
                    "1794472708.249819",
                    "--no-event-id",
                ]
            )

        assert rc3 == 0
        assert len(post_calls) == 2, f"Expected --no-event-id call to post (total 2), got {len(post_calls)}"
        out3 = json.loads(stdout3.getvalue())
        assert out3["ok"] is True
        assert out3["ts"] == "1794472780.000002"
        assert "deduplicated" not in out3

        return "同一event_idの2回呼び出しで実投稿1回のみ・2回目はdeduplicated=trueと記録済みreply_ts返却・repliesテーブル保存・--event-id無しの後方互換動作を確認"


def test_reply_lost_response_recovery() -> str:
    """11. reply: 投稿はSlackへ到達したが応答が接続例外で失われ、回復確認の履歴取得も1回目は失敗・2回目で同一文面が見つかるケースで実投稿が1回だけになり、3回とも履歴取得失敗時は ambiguous_post_outcome を返す"""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "slack_events.db"
        init_db(db_path)

        text_file = Path(tmp) / "reply_lost_resp.txt"
        reply_text = "Neiroさん、ご提案ありがとうございます。Yamaからの返信（1561文字相当テスト）です。"
        text_file.write_text(reply_text + "\n", encoding="utf-8")

        base_now = 1794476170.0
        bot_uid = "U_YAMA_BOT"
        thread_ts = "1794475963.152409"
        landed_ts = "1794476165.978039"

        post_calls: List[Dict[str, Any]] = []
        replies_calls: List[Dict[str, Any]] = []
        sleeps: List[float] = []
        post_landed = {"value": False}
        recovery_attempt = {"count": 0}

        class LostResponseMockClient:
            def auth_test(self) -> Dict[str, Any]:
                return {"ok": True, "user_id": bot_uid}

            def chat_postMessage(self, **kwargs: Any) -> Dict[str, Any]:
                post_calls.append(kwargs)
                # Message lands on Slack server, but response read fails with IncompleteRead
                post_landed["value"] = True
                raise http.client.IncompleteRead(b"partial-response", 128)

            def conversations_replies(
                self,
                channel: str,
                ts: str,
                oldest: Optional[str] = None,
                limit: int = 50,
                cursor: Optional[str] = None,
                **kwargs: Any,
            ) -> Dict[str, Any]:
                replies_calls.append(
                    {
                        "channel": channel,
                        "ts": ts,
                        "oldest": oldest,
                        "limit": limit,
                        "cursor": cursor,
                        **kwargs,
                    }
                )
                if not post_landed["value"]:
                    assert oldest == thread_ts, (
                        f"pre-post oldest parameter (thread_ts={thread_ts}) must be passed, got {oldest}"
                    )
                    # Pre-post check before chat_postMessage: reply has not been posted yet
                    return {
                        "ok": True,
                        "messages": [
                            {"user": "U_NEIRO", "text": "Parent post", "ts": thread_ts}
                        ],
                    }

                expected_rec_oldest = f"{base_now - 180.0:.6f}"
                expected_rec_latest = f"{base_now + 300.0:.6f}"
                assert oldest == expected_rec_oldest, (
                    f"recovery oldest must be attempt_ts - 180s ({expected_rec_oldest}), got {oldest}"
                )
                assert kwargs.get("latest") == expected_rec_latest, (
                    f"recovery latest must be attempt_ts + 300s ({expected_rec_latest}), got {kwargs.get('latest')}"
                )
                assert kwargs.get("include_all_metadata") is True, (
                    f"include_all_metadata=True must be passed, got {kwargs}"
                )

                # Post-exception recovery check: 1st attempt fails with transient error, 2nd succeeds
                recovery_attempt["count"] += 1
                if recovery_attempt["count"] == 1:
                    raise http.client.IncompleteRead(b"transient-history-drop", 64)

                return {
                    "ok": True,
                    "messages": [
                        {"user": "U_NEIRO", "text": "Parent post", "ts": thread_ts},
                        {
                            "user": bot_uid,
                            "text": reply_text,
                            "ts": landed_ts,
                            "thread_ts": thread_ts,
                        },
                    ],
                }

        stdout1 = io.StringIO()
        stderr1 = io.StringIO()
        with patch.object(
            agent_tools, "get_web_client", return_value=LostResponseMockClient()
        ), patch.object(agent_tools.time, "time", return_value=base_now), patch.object(
            agent_tools.time, "sleep", side_effect=lambda s: sleeps.append(s)
        ), redirect_stdout(
            stdout1
        ), redirect_stderr(
            stderr1
        ):
            rc1 = agent_tools.main(
                [
                    "--db",
                    str(db_path),
                    "reply",
                    "--channel",
                    "C_ALL",
                    "--text-file",
                    str(text_file),
                    "--thread-ts",
                    thread_ts,
                    "--event-id",
                    "Ev0CHXK8PDLY",
                    "--force",
                    "--reason",
                    "test fixture: events row intentionally absent (pre-candidate-28 call shape)",
                ]
            )

        assert rc1 == 0, f"Expected rc=0 after recovery on 2nd history attempt, got {rc1}, stderr={stderr1.getvalue()}"
        assert len(post_calls) == 1, f"Expected exactly 1 chat_postMessage call, got {len(post_calls)}"
        assert recovery_attempt["count"] == 2, f"Expected 2 recovery history attempts, got {recovery_attempt['count']}"
        assert sleeps == [1.0], f"Expected 1s backoff sleep between 1st and 2nd recovery attempt, got {sleeps}"

        out1 = json.loads(stdout1.getvalue())
        assert out1["ok"] is True
        assert out1["ts"] == landed_ts
        assert out1["recovered_after_ambiguous_error"] is True
        assert out1["event_id"] == "Ev0CHXK8PDLY"

        # Verify DB recorded the recovered reply
        rec = get_recorded_reply("Ev0CHXK8PDLY", db_path=db_path)
        assert rec is not None and rec["reply_ts"] == landed_ts

        # Re-invoking reply for the same event must not post again (total post_calls remains 1)
        stdout2 = io.StringIO()
        stderr2 = io.StringIO()
        with patch.object(
            agent_tools, "get_web_client", return_value=LostResponseMockClient()
        ), redirect_stdout(stdout2), redirect_stderr(stderr2):
            rc2 = agent_tools.main(
                [
                    "--db",
                    str(db_path),
                    "reply",
                    "--channel",
                    "C_ALL",
                    "--text-file",
                    str(text_file),
                    "--thread-ts",
                    thread_ts,
                    "--event-id",
                    "Ev0CHXK8PDLY",
                    "--force",
                    "--reason",
                    "test fixture: events row intentionally absent (pre-candidate-28 call shape)",
                ]
            )
        assert rc2 == 0
        assert len(post_calls) == 1, f"Must still be 1 real post, got {len(post_calls)}"
        out2 = json.loads(stdout2.getvalue())
        assert out2["deduplicated"] is True and out2["ts"] == landed_ts

        # Verify unrecoverable case: all 3 recovery attempts fail -> error="ambiguous_post_outcome" and exit 1
        with get_db_connection(db_path) as conn:
            conn.execute("DELETE FROM replies")
            conn.commit()
        unrec_sleeps: List[float] = []
        unrec_history_attempts = {"count": 0}
        unrec_posted = {"value": False}

        class AmbiguousOutcomeMockClient:
            def auth_test(self) -> Dict[str, Any]:
                return {"ok": True, "user_id": bot_uid}

            def chat_postMessage(self, **kwargs: Any) -> Dict[str, Any]:
                unrec_posted["value"] = True
                raise urllib.error.URLError("connection reset during response")

            def conversations_replies(
                self,
                channel: str,
                ts: str,
                oldest: Optional[str] = None,
                limit: int = 50,
                include_all_metadata: bool = False,
                latest: Optional[str] = None,
                cursor: Optional[str] = None,
            ) -> Dict[str, Any]:
                if not unrec_posted["value"]:
                    return {"ok": True, "messages": []}
                unrec_history_attempts["count"] += 1
                raise urllib.error.URLError("history read also down")

        stdout3 = io.StringIO()
        stderr3 = io.StringIO()
        with patch.object(
            agent_tools, "get_web_client", return_value=AmbiguousOutcomeMockClient()
        ), patch.object(agent_tools.time, "time", return_value=base_now), patch.object(
            agent_tools.time, "sleep", side_effect=lambda s: unrec_sleeps.append(s)
        ), redirect_stdout(
            stdout3
        ), redirect_stderr(
            stderr3
        ):
            rc3 = agent_tools.main(
                [
                    "--db",
                    str(db_path),
                    "reply",
                    "--channel",
                    "C_ALL",
                    "--text-file",
                    str(text_file),
                    "--thread-ts",
                    thread_ts,
                    "--event-id",
                    "Ev_AMBIGUOUS",
                    "--force",
                    "--reason",
                    "test fixture: events row intentionally absent (pre-candidate-28 call shape)",
                ]
            )

        assert rc3 == 1, f"Expected exit code 1 on ambiguous outcome, got {rc3}"
        assert unrec_history_attempts["count"] == 5, (
            f"Expected 5 recovery attempts, got {unrec_history_attempts['count']}"
        )
        assert unrec_sleeps == [1.0, 2.0, 4.0, 8.0], f"Expected backoffs [1.0, 2.0, 4.0, 8.0], got {unrec_sleeps}"
        err3 = json.loads(stderr3.getvalue())
        assert err3["ok"] is False
        assert err3["error"] == "ambiguous_post_outcome"

        return "応答ロスト後の回復確認リトライ(1回目失敗→2回目成功)で実投稿1回のみ＆DB記録・5回全失敗時はambiguous_post_outcome出力を確認"


def test_reply_pre_post_thread_dedup() -> str:
    """12. reply: 投稿前に同一文面が既にスレッドにある場合（oldest=親ts・スレッド全体走査）、repliesテーブル未記録や--event-id無しでも再投稿せず deduplicated=true で正常終了する"""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "slack_events.db"
        init_db(db_path)

        text_file = Path(tmp) / "reply_pre_dedup.txt"
        reply_text = "スレッド内に既に投稿済みのYamaの同一文面メッセージです。"
        text_file.write_text(reply_text + "\n", encoding="utf-8")

        base_now = 1794476200.0
        bot_uid = "U_YAMA_BOT"
        thread_ts = "1794475963.152409"
        existing_slack_ts = "1794476165.978039"

        post_calls: List[Dict[str, Any]] = []
        oldest_seen: List[Optional[str]] = []

        class PreExistingThreadReplyClient:
            def auth_test(self) -> Dict[str, Any]:
                return {"ok": True, "user_id": bot_uid}

            def conversations_replies(
                self,
                channel: str,
                ts: str,
                oldest: Optional[str] = None,
                limit: int = 50,
                cursor: Optional[str] = None,
                include_all_metadata: bool = False,
            ) -> Dict[str, Any]:
                oldest_seen.append(oldest)
                assert limit == 50, f"Expected limit=50, got {limit}"
                return {
                    "ok": True,
                    "messages": [
                        {"user": "U_NEIRO", "text": "Question in thread", "ts": thread_ts},
                        {
                            "user": bot_uid,
                            "text": reply_text,
                            "ts": existing_slack_ts,
                            "thread_ts": thread_ts,
                        },
                    ],
                }

            def chat_postMessage(self, **kwargs: Any) -> Dict[str, Any]:
                post_calls.append(kwargs)
                raise AssertionError("chat_postMessage must NOT be called when identical reply exists in thread")

        # 1) With --event-id (not yet in replies table) -> detects in thread, records in DB, returns deduplicated=true without posting
        assert get_recorded_reply("Ev_PRE_DEDUP_1", db_path=db_path) is None
        stdout1 = io.StringIO()
        stderr1 = io.StringIO()
        with patch.object(
            agent_tools, "get_web_client", return_value=PreExistingThreadReplyClient()
        ), patch.object(agent_tools.time, "time", return_value=base_now), redirect_stdout(
            stdout1
        ), redirect_stderr(
            stderr1
        ):
            rc1 = agent_tools.main(
                [
                    "--db",
                    str(db_path),
                    "reply",
                    "--channel",
                    "C_ALL",
                    "--text-file",
                    str(text_file),
                    "--thread-ts",
                    thread_ts,
                    "--event-id",
                    "Ev_PRE_DEDUP_1",
                    "--force",
                    "--reason",
                    "test fixture: events row intentionally absent (pre-candidate-28 call shape)",
                ]
            )

        assert rc1 == 0, f"Expected rc=0, got {rc1}, stderr={stderr1.getvalue()}"
        assert len(post_calls) == 0, f"Expected 0 chat_postMessage calls, got {len(post_calls)}"
        assert len(oldest_seen) == 1 and oldest_seen[0] == thread_ts, (
            f"Expected oldest={thread_ts}, got {oldest_seen}"
        )

        out1 = json.loads(stdout1.getvalue())
        assert out1["ok"] is True
        assert out1["deduplicated"] is True
        assert out1["ts"] == existing_slack_ts
        assert out1["event_id"] == "Ev_PRE_DEDUP_1"

        rec = get_recorded_reply("Ev_PRE_DEDUP_1", db_path=db_path)
        assert rec is not None and rec["reply_ts"] == existing_slack_ts

        # 2) With --no-event-id (with --thread-ts) -> also detects in thread and skips posting with deduplicated=true
        stdout2 = io.StringIO()
        stderr2 = io.StringIO()
        with patch.object(
            agent_tools, "get_web_client", return_value=PreExistingThreadReplyClient()
        ), patch.object(agent_tools.time, "time", return_value=base_now), redirect_stdout(
            stdout2
        ), redirect_stderr(
            stderr2
        ):
            rc2 = agent_tools.main(
                [
                    "--db",
                    str(db_path),
                    "reply",
                    "--channel",
                    "C_ALL",
                    "--text-file",
                    str(text_file),
                    "--thread-ts",
                    thread_ts,
                    "--no-event-id",
                ]
            )

        assert rc2 == 0
        assert len(post_calls) == 0, f"Expected 0 chat_postMessage calls without event_id, got {len(post_calls)}"
        out2 = json.loads(stdout2.getvalue())
        assert out2["ok"] is True
        assert out2["deduplicated"] is True
        assert out2["ts"] == existing_slack_ts

        return "投稿前のスレッド履歴確認(oldest=親ts・スレッド全体走査)によりevent_id有無にかかわらず同一文面の再投稿を阻止しDB記録＆deduplicated=true返却を確認"


def test_reply_pre_post_channel_bounded_dedup() -> str:
    """13. reply: チャンネル投稿(thread_ts無し)で直近50件の履歴内に15分より古い同一文面の自Bot投稿がある場合、chat_postMessageを呼ばず deduplicated=true になる（時間窓なし・limit=50・fail-open・回復確認も同様）"""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "slack_events.db"
        init_db(db_path)

        text_file = Path(tmp) / "reply_channel_bounded.txt"
        channel_text = "うちもこの bounded チェックをチャンネル投稿に取り入れます。取り入れたらまた共有します。"
        text_file.write_text(channel_text + "\n", encoding="utf-8")

        base_now = 1794480000.0
        # 1 hour (3600s > 900s) older than base_now, proving no 15-min cutoff is applied on channel posts
        old_channel_post_ts = f"{base_now - 3600.0:.6f}"
        bot_uid = "U_YAMA_BOT"
        channel_id = "C_ALL"

        post_calls: List[Dict[str, Any]] = []
        history_calls: List[Dict[str, Any]] = []

        class BoundedChannelHistoryClient:
            def auth_test(self) -> Dict[str, Any]:
                return {"ok": True, "user_id": bot_uid}

            def conversations_history(self, **kwargs: Any) -> Dict[str, Any]:
                history_calls.append(kwargs)
                assert kwargs.get("channel") == channel_id
                assert kwargs.get("limit") == 50, f"Expected limit=50, got {kwargs.get('limit')}"
                assert "oldest" not in kwargs, f"Channel bounded check must NOT pass oldest: {kwargs}"
                return {
                    "ok": True,
                    "messages": [
                        {"user": "U_OTHER_1", "text": "Recent post", "ts": f"{base_now - 60.0:.6f}"},
                        {
                            "user": bot_uid,
                            "text": channel_text,
                            "ts": old_channel_post_ts,
                        },
                    ],
                }

            def chat_postMessage(self, **kwargs: Any) -> Dict[str, Any]:
                post_calls.append(kwargs)
                raise AssertionError(
                    "chat_postMessage must NOT be called when identical channel post exists in latest 50 messages"
                )

        # 1) Channel post with --event-id (not in replies table) and existing message >15m old in latest 50
        assert get_recorded_reply("Ev_CH_BOUND_1", db_path=db_path) is None
        stdout1 = io.StringIO()
        stderr1 = io.StringIO()
        with patch.object(
            agent_tools, "get_web_client", return_value=BoundedChannelHistoryClient()
        ), patch.object(agent_tools.time, "time", return_value=base_now), redirect_stdout(
            stdout1
        ), redirect_stderr(
            stderr1
        ):
            rc1 = agent_tools.main(
                [
                    "--db",
                    str(db_path),
                    "reply",
                    "--channel",
                    channel_id,
                    "--text-file",
                    str(text_file),
                    "--event-id",
                    "Ev_CH_BOUND_1",
                    "--force",
                    "--reason",
                    "test fixture: events row intentionally absent (pre-candidate-28 call shape)",
                ]
            )

        assert rc1 == 0, f"Expected rc=0, got {rc1}, stderr={stderr1.getvalue()}"
        assert len(post_calls) == 0, f"Expected 0 chat_postMessage calls, got {len(post_calls)}"
        assert len(history_calls) == 1
        out1 = json.loads(stdout1.getvalue())
        assert out1["ok"] is True
        assert out1["deduplicated"] is True
        assert out1["channel"] == channel_id
        assert out1["ts"] == old_channel_post_ts
        assert out1["thread_ts"] is None
        assert out1["event_id"] == "Ev_CH_BOUND_1"

        rec = get_recorded_reply("Ev_CH_BOUND_1", db_path=db_path)
        assert rec is not None
        assert rec["reply_ts"] == old_channel_post_ts
        assert rec["thread_ts"] is None

        # 2) Channel post WITHOUT --event-id -> also bounded-checked against latest 50 messages
        stdout2 = io.StringIO()
        stderr2 = io.StringIO()
        with patch.object(
            agent_tools, "get_web_client", return_value=BoundedChannelHistoryClient()
        ), patch.object(agent_tools.time, "time", return_value=base_now), redirect_stdout(
            stdout2
        ), redirect_stderr(
            stderr2
        ):
            rc2 = agent_tools.main(
                [
                    "--db",
                    str(db_path),
                    "reply",
                    "--channel",
                    channel_id,
                    "--text-file",
                    str(text_file),
                    "--no-event-id",
                ]
            )

        assert rc2 == 0
        assert len(post_calls) == 0
        assert len(history_calls) == 2
        out2 = json.loads(stdout2.getvalue())
        assert out2["ok"] is True
        assert out2["deduplicated"] is True
        assert out2["ts"] == old_channel_post_ts

        # 3) Fail-open check: if conversations_history fails before posting, logs warning to stderr and posts normally
        fail_open_posts: List[Dict[str, Any]] = []

        class FailOpenChannelClient:
            def auth_test(self) -> Dict[str, Any]:
                return {"ok": True, "user_id": bot_uid}

            def conversations_history(self, **kwargs: Any) -> Dict[str, Any]:
                raise urllib.error.URLError("temporary history read error")

            def chat_postMessage(self, **kwargs: Any) -> Dict[str, Any]:
                fail_open_posts.append(kwargs)
                return {
                    "ok": True,
                    "channel": kwargs["channel"],
                    "ts": f"{base_now + 1.0:.6f}",
                    "message": {"text": kwargs["text"], "ts": f"{base_now + 1.0:.6f}"},
                }

        stdout3 = io.StringIO()
        stderr3 = io.StringIO()
        with patch.object(
            agent_tools, "get_web_client", return_value=FailOpenChannelClient()
        ), patch.object(
            agent_tools.time, "sleep", side_effect=lambda s: None
        ), redirect_stdout(stdout3), redirect_stderr(stderr3):
            rc3 = agent_tools.main(
                [
                    "--db",
                    str(db_path),
                    "reply",
                    "--channel",
                    channel_id,
                    "--text-file",
                    str(text_file),
                    "--event-id",
                    "Ev_CH_FAIL_OPEN",
                    "--force",
                    "--reason",
                    "test fixture: events row intentionally absent (pre-candidate-28 call shape)",
                ]
            )

        assert rc3 == 0
        assert len(fail_open_posts) == 1
        warn3 = json.loads(stderr3.getvalue())
        assert warn3["warning"] == "pre_post_channel_check_failed"
        out3 = json.loads(stdout3.getvalue())
        assert out3["ok"] is True and out3["deduplicated"] is False

        return "チャンネル投稿の投稿前boundedチェック(直近50件・時間窓なし)で15分超の同一文面も再投稿阻止＆DB記録・履歴失敗時fail-open継続を確認"


def test_reply_thread_fullscan_and_pagination() -> str:
    """14. reply: スレッド返信で15分より古い同一文面の検出、2ページ以上にまたがるスレッドでの2ページ目検出（投稿前＆応答ロスト回復確認）、および最大20ページ上限打ち切り警告を確認"""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "slack_events.db"
        init_db(db_path)

        text_file = Path(tmp) / "reply_thread_fullscan.txt"
        reply_text = "うちもスレッド全体への拡大を検討します。取り込めたらまた共有します。"
        text_file.write_text(reply_text + "\n", encoding="utf-8")

        base_now = 1794500000.0
        bot_uid = "U_YAMA_BOT"
        channel_id = "C_ALL"
        thread_ts = "1794475963.152409"
        # Over 3 hours (> 900s) older than base_now
        old_reply_ts = f"{base_now - 10800.0:.6f}"
        page2_reply_ts = f"{base_now - 3600.0:.6f}"

        # Case 1: Identical message in thread is >15 minutes old (3 hours old) -> detected without posting
        old_calls: List[Dict[str, Any]] = []
        old_posts: List[Dict[str, Any]] = []

        class OldThreadReplyClient:
            def auth_test(self) -> Dict[str, Any]:
                return {"ok": True, "user_id": bot_uid}

            def conversations_replies(self, **kwargs: Any) -> Dict[str, Any]:
                old_calls.append(kwargs)
                assert kwargs.get("channel") == channel_id
                assert kwargs.get("ts") == thread_ts
                assert kwargs.get("oldest") == thread_ts
                assert kwargs.get("limit") == 50
                assert "cursor" not in kwargs
                return {
                    "ok": True,
                    "messages": [
                        {"user": "U_PARENT", "text": "Parent post", "ts": thread_ts},
                        {
                            "user": bot_uid,
                            "text": reply_text,
                            "ts": old_reply_ts,
                            "thread_ts": thread_ts,
                        },
                    ],
                    "has_more": False,
                    "response_metadata": {"next_cursor": ""},
                }

            def chat_postMessage(self, **kwargs: Any) -> Dict[str, Any]:
                old_posts.append(kwargs)
                raise AssertionError("chat_postMessage must NOT be called when >15m old identical reply exists in thread")

        stdout1 = io.StringIO()
        stderr1 = io.StringIO()
        with patch.object(
            agent_tools, "get_web_client", return_value=OldThreadReplyClient()
        ), patch.object(agent_tools.time, "time", return_value=base_now), redirect_stdout(
            stdout1
        ), redirect_stderr(
            stderr1
        ):
            rc1 = agent_tools.main(
                [
                    "--db",
                    str(db_path),
                    "reply",
                    "--channel",
                    channel_id,
                    "--text-file",
                    str(text_file),
                    "--thread-ts",
                    thread_ts,
                    "--event-id",
                    "Ev_THREAD_OLD_1",
                    "--force",
                    "--reason",
                    "test fixture: events row intentionally absent (pre-candidate-28 call shape)",
                ]
            )

        assert rc1 == 0, f"Expected rc=0, got {rc1}, stderr={stderr1.getvalue()}"
        assert len(old_posts) == 0
        assert len(old_calls) == 1
        out1 = json.loads(stdout1.getvalue())
        assert out1["ok"] is True
        assert out1["deduplicated"] is True
        assert out1["ts"] == old_reply_ts
        rec1 = get_recorded_reply("Ev_THREAD_OLD_1", db_path=db_path)
        assert rec1 is not None and rec1["reply_ts"] == old_reply_ts

        # Case 2: Multi-page thread (2 pages) where identical message is on page 2 (pre-post check)
        with get_db_connection(db_path) as conn:
            conn.execute("DELETE FROM replies")
            conn.commit()
        page_calls: List[Dict[str, Any]] = []
        page_posts: List[Dict[str, Any]] = []

        class PaginatedThreadReplyClient:
            def auth_test(self) -> Dict[str, Any]:
                return {"ok": True, "user_id": bot_uid}

            def conversations_replies(self, **kwargs: Any) -> Dict[str, Any]:
                page_calls.append(kwargs)
                assert kwargs.get("channel") == channel_id
                assert kwargs.get("ts") == thread_ts
                assert kwargs.get("oldest") == thread_ts
                assert kwargs.get("limit") == 50
                cursor = kwargs.get("cursor")
                if not cursor:
                    # Page 1: 50 messages from other users, has_more=True, next_cursor="cursor_page_2"
                    msgs_p1 = [
                        {
                            "user": "U_OTHER",
                            "text": f"Other reply #{i}",
                            "ts": f"{float(thread_ts) + i:.6f}",
                            "thread_ts": thread_ts,
                        }
                        for i in range(50)
                    ]
                    return {
                        "ok": True,
                        "messages": msgs_p1,
                        "has_more": True,
                        "response_metadata": {"next_cursor": "cursor_page_2"},
                    }
                assert cursor == "cursor_page_2", f"Unexpected cursor: {cursor}"
                return {
                    "ok": True,
                    "messages": [
                        {
                            "user": "U_OTHER",
                            "text": "Other reply #50",
                            "ts": f"{float(thread_ts) + 50:.6f}",
                            "thread_ts": thread_ts,
                        },
                        {
                            "user": bot_uid,
                            "text": reply_text,
                            "ts": page2_reply_ts,
                            "thread_ts": thread_ts,
                        },
                    ],
                    "has_more": False,
                    "response_metadata": {"next_cursor": ""},
                }

            def chat_postMessage(self, **kwargs: Any) -> Dict[str, Any]:
                page_posts.append(kwargs)
                raise AssertionError("chat_postMessage must NOT be called when identical reply is found on page 2")

        stdout2 = io.StringIO()
        stderr2 = io.StringIO()
        with patch.object(
            agent_tools, "get_web_client", return_value=PaginatedThreadReplyClient()
        ), patch.object(agent_tools.time, "time", return_value=base_now), redirect_stdout(
            stdout2
        ), redirect_stderr(
            stderr2
        ):
            rc2 = agent_tools.main(
                [
                    "--db",
                    str(db_path),
                    "reply",
                    "--channel",
                    channel_id,
                    "--text-file",
                    str(text_file),
                    "--thread-ts",
                    thread_ts,
                    "--event-id",
                    "Ev_THREAD_PAGE2_1",
                    "--force",
                    "--reason",
                    "test fixture: events row intentionally absent (pre-candidate-28 call shape)",
                ]
            )

        assert rc2 == 0, f"Expected rc=0, got {rc2}, stderr={stderr2.getvalue()}"
        assert len(page_posts) == 0
        assert len(page_calls) == 2, f"Expected 2 pages fetched, got {len(page_calls)}"
        assert "cursor" not in page_calls[0]
        assert page_calls[1].get("cursor") == "cursor_page_2"
        out2 = json.loads(stdout2.getvalue())
        assert out2["ok"] is True
        assert out2["deduplicated"] is True
        assert out2["ts"] == page2_reply_ts
        rec2 = get_recorded_reply("Ev_THREAD_PAGE2_1", db_path=db_path)
        assert rec2 is not None and rec2["reply_ts"] == page2_reply_ts

        # Case 3: Lost response recovery (_find_own_recent_message) also scans across pages (page 2)
        with get_db_connection(db_path) as conn:
            conn.execute("DELETE FROM replies")
            conn.commit()
        rec_landed = {"value": False}
        rec_posts: List[Dict[str, Any]] = []
        rec_landed_ts = f"{base_now - 2.0:.6f}"

        class PaginatedLostResponseClient:
            def auth_test(self) -> Dict[str, Any]:
                return {"ok": True, "user_id": bot_uid}

            def chat_postMessage(self, **kwargs: Any) -> Dict[str, Any]:
                rec_posts.append(kwargs)
                rec_landed["value"] = True
                raise http.client.IncompleteRead(b"lost-after-landing-on-page2", 64)

            def conversations_replies(self, **kwargs: Any) -> Dict[str, Any]:
                cursor = kwargs.get("cursor")
                if not cursor:
                    return {
                        "ok": True,
                        "messages": [{"user": "U_OTHER", "text": "Page 1 msg", "ts": thread_ts}],
                        "has_more": True,
                        "response_metadata": {"next_cursor": "cursor_rec_p2"},
                    }
                assert cursor == "cursor_rec_p2"
                msgs_p2 = [{"user": "U_OTHER", "text": "Page 2 msg", "ts": f"{float(thread_ts) + 10:.6f}"}]
                if rec_landed["value"]:
                    msgs_p2.append(
                        {
                            "user": bot_uid,
                            "text": reply_text,
                            "ts": rec_landed_ts,
                            "thread_ts": thread_ts,
                        }
                    )
                return {
                    "ok": True,
                    "messages": msgs_p2,
                    "has_more": False,
                    "response_metadata": {"next_cursor": ""},
                }

        stdout3 = io.StringIO()
        stderr3 = io.StringIO()
        with patch.object(
            agent_tools, "get_web_client", return_value=PaginatedLostResponseClient()
        ), patch.object(agent_tools.time, "time", return_value=base_now), redirect_stdout(
            stdout3
        ), redirect_stderr(
            stderr3
        ):
            rc3 = agent_tools.main(
                [
                    "--db",
                    str(db_path),
                    "reply",
                    "--channel",
                    channel_id,
                    "--text-file",
                    str(text_file),
                    "--thread-ts",
                    thread_ts,
                    "--event-id",
                    "Ev_THREAD_REC_PAGE2",
                    "--force",
                    "--reason",
                    "test fixture: events row intentionally absent (pre-candidate-28 call shape)",
                ]
            )

        assert rc3 == 0, f"Expected rc=0 on paginated recovery, got {rc3}, stderr={stderr3.getvalue()}"
        assert len(rec_posts) == 1
        out3 = json.loads(stdout3.getvalue())
        assert out3["ok"] is True
        assert out3["recovered_after_ambiguous_error"] is True
        assert out3["ts"] == rec_landed_ts

        # Case 4 (candidate 14, option A): page cap at 20 pages -> stops after 20 calls, logs the
        # warning, then fails closed: no post, pre_post_check_unverifiable (thread_scan_truncated), draft kept
        with get_db_connection(db_path) as conn:
            conn.execute("DELETE FROM replies")
            conn.commit()
        cap_calls: List[Dict[str, Any]] = []
        cap_posts: List[Dict[str, Any]] = []

        class HugeThreadClient:
            def auth_test(self) -> Dict[str, Any]:
                return {"ok": True, "user_id": bot_uid}

            def conversations_replies(self, **kwargs: Any) -> Dict[str, Any]:
                cap_calls.append(kwargs)
                next_idx = len(cap_calls) + 1
                return {
                    "ok": True,
                    "messages": [
                        {
                            "user": "U_OTHER",
                            "text": f"Unrelated message on page {len(cap_calls)}",
                            "ts": f"{float(thread_ts) + len(cap_calls):.6f}",
                        }
                    ],
                    "has_more": True,
                    "response_metadata": {"next_cursor": f"cursor_page_{next_idx}"},
                }

            def chat_postMessage(self, **kwargs: Any) -> Dict[str, Any]:
                cap_posts.append(kwargs)
                return {
                    "ok": True,
                    "channel": kwargs["channel"],
                    "ts": f"{base_now + 5.0:.6f}",
                    "message": {"text": kwargs["text"], "ts": f"{base_now + 5.0:.6f}"},
                }

        stdout4 = io.StringIO()
        stderr4 = io.StringIO()
        with patch.object(
            agent_tools, "get_web_client", return_value=HugeThreadClient()
        ), redirect_stdout(stdout4), redirect_stderr(stderr4):
            rc4 = agent_tools.main(
                [
                    "--db",
                    str(db_path),
                    "reply",
                    "--channel",
                    channel_id,
                    "--text-file",
                    str(text_file),
                    "--thread-ts",
                    thread_ts,
                    "--event-id",
                    "Ev_THREAD_CAP20",
                    "--force",
                    "--reason",
                    "test fixture: events row intentionally absent (pre-candidate-28 call shape)",
                ]
            )

        assert rc4 == 1, f"Expected rc=1 (fail-closed) after 20-page cap, got {rc4}"
        assert len(cap_calls) == 20, f"Expected cap at 20 pages (no whole-scan retry), got {len(cap_calls)}"
        assert len(cap_posts) == 0, f"Truncated scan must not post, got {len(cap_posts)} post(s)"
        err_text4 = stderr4.getvalue()
        first_line4, _, rest4 = err_text4.partition("\n")
        warn4 = json.loads(first_line4)
        assert warn4["warning"] == "thread_replies_page_limit_reached"
        assert warn4["max_pages"] == 20
        err4 = json.loads(rest4)
        assert err4["ok"] is False
        assert err4["error"] == "pre_post_check_unverifiable", err4
        assert "thread_scan_truncated" in err4["check_error"], err4
        assert err4["event_id"] == "Ev_THREAD_CAP20"
        draft4 = db_path.parent / "reply-Ev_THREAD_CAP20.txt"
        assert draft4.exists() and draft4.read_text(encoding="utf-8").strip() == reply_text, (
            "draft must be kept for the 1h-wait re-dispatch"
        )

        # Case 5: a thread that ends exactly at the 20th page (no next cursor) is not truncated -> posts
        end_calls: List[Dict[str, Any]] = []
        end_posts: List[Dict[str, Any]] = []

        class TwentyPageThreadClient(HugeThreadClient):
            def conversations_replies(self, **kwargs: Any) -> Dict[str, Any]:
                end_calls.append(kwargs)
                last = len(end_calls) >= 20
                return {
                    "ok": True,
                    "messages": [
                        {
                            "user": "U_OTHER",
                            "text": f"Unrelated message on page {len(end_calls)}",
                            "ts": f"{float(thread_ts) + len(end_calls):.6f}",
                        }
                    ],
                    "has_more": not last,
                    "response_metadata": {"next_cursor": "" if last else f"c{len(end_calls) + 1}"},
                }

            def chat_postMessage(self, **kwargs: Any) -> Dict[str, Any]:
                end_posts.append(kwargs)
                return {
                    "ok": True,
                    "channel": kwargs["channel"],
                    "ts": f"{base_now + 6.0:.6f}",
                    "message": {"text": kwargs["text"], "ts": f"{base_now + 6.0:.6f}"},
                }

        with get_db_connection(db_path) as conn:
            conn.execute("DELETE FROM replies")
            conn.commit()
        stdout5 = io.StringIO()
        stderr5 = io.StringIO()
        with patch.object(
            agent_tools, "get_web_client", return_value=TwentyPageThreadClient()
        ), redirect_stdout(stdout5), redirect_stderr(stderr5):
            rc5 = agent_tools.main(
                [
                    "--db",
                    str(db_path),
                    "reply",
                    "--channel",
                    channel_id,
                    "--text-file",
                    str(text_file),
                    "--thread-ts",
                    thread_ts,
                    "--event-id",
                    "Ev_THREAD_END20",
                    "--force",
                    "--reason",
                    "test fixture: events row intentionally absent (pre-candidate-28 call shape)",
                ]
            )
        assert rc5 == 0, f"Expected rc=0 when the thread ends on page 20, got {rc5}, stderr={stderr5.getvalue()}"
        assert len(end_calls) == 20 and len(end_posts) == 1
        assert "thread_replies_page_limit_reached" not in stderr5.getvalue()

        return "15分超(3時間前)のスレッド内同一文面検出・2ページ目(cursorページング)の同一文面検出(投稿前＆応答ロスト回復)・20ページ上限打ち切りで警告＋送信せずpre_post_check_unverifiable(thread_scan_truncated)＆下書き保持・ちょうど20ページで終わるスレッドは送信を確認"


def test_prune_replies() -> str:
    """15. Replies 台帳90日prune: 91日超は削除・89日は保持・NULLおよび解釈不能タイムスタンプは保持・--dry-runは削除せず予定件数のみ返却"""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "slack_events.db"
        init_db(db_path)

        now_dt = datetime.now(timezone.utc)
        ts_91d_ago = (now_dt - timedelta(days=91)).strftime("%Y-%m-%d %H:%M:%S")
        ts_89d_ago = (now_dt - timedelta(days=89)).strftime("%Y-%m-%d %H:%M:%S")

        with get_db_connection(db_path) as conn:
            conn.executemany(
                """
                INSERT INTO replies (event_id, channel_id, thread_ts, reply_ts, text_hash, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                [
                    ("Ev_PRUNE_91D", "C_PRUNE", None, "1700000001.000000", "hash_91d", ts_91d_ago),
                    ("Ev_KEEP_89D", "C_PRUNE", None, "1700000002.000000", "hash_89d", ts_89d_ago),
                    ("Ev_KEEP_NULL", "C_PRUNE", None, "1700000003.000000", "hash_null", None),
                    ("Ev_KEEP_BAD_TS", "C_PRUNE", None, "1700000004.000000", "hash_bad", "not-a-timestamp"),
                ],
            )
            conn.commit()

        # 1) --dry-run: does not delete any rows, returns scheduled deletion count (1)
        stdout_dry = io.StringIO()
        stderr_dry = io.StringIO()
        with redirect_stdout(stdout_dry), redirect_stderr(stderr_dry):
            rc_dry = agent_tools.main(
                [
                    "--db",
                    str(db_path),
                    "prune-replies",
                    "--days",
                    "90",
                    "--dry-run",
                ]
            )

        assert rc_dry == 0, f"Expected rc=0 for --dry-run, got {rc_dry}, stderr={stderr_dry.getvalue()}"
        out_dry = json.loads(stdout_dry.getvalue())
        assert out_dry["ok"] is True
        assert out_dry["dry_run"] is True
        assert out_dry["days"] == 90
        assert out_dry["deleted_count"] == 1

        with get_db_connection(db_path) as conn:
            remaining_after_dry = {
                r["event_id"] for r in conn.execute("SELECT event_id FROM replies").fetchall()
            }
        assert remaining_after_dry == {
            "Ev_PRUNE_91D",
            "Ev_KEEP_89D",
            "Ev_KEEP_NULL",
            "Ev_KEEP_BAD_TS",
        }, f"--dry-run must not delete rows, got {remaining_after_dry}"

        # 2) Actual prune (default --days 90): deletes only Ev_PRUNE_91D, keeps 89d / NULL / unparseable
        stdout_run = io.StringIO()
        stderr_run = io.StringIO()
        with redirect_stdout(stdout_run), redirect_stderr(stderr_run):
            rc_run = agent_tools.main(
                [
                    "--db",
                    str(db_path),
                    "prune-replies",
                ]
            )

        assert rc_run == 0, f"Expected rc=0 for prune-replies, got {rc_run}, stderr={stderr_run.getvalue()}"
        out_run = json.loads(stdout_run.getvalue())
        assert out_run["ok"] is True
        assert out_run["dry_run"] is False
        assert out_run["days"] == 90
        assert out_run["deleted_count"] == 1

        with get_db_connection(db_path) as conn:
            remaining_after_prune = {
                r["event_id"] for r in conn.execute("SELECT event_id FROM replies").fetchall()
            }
        assert remaining_after_prune == {
            "Ev_KEEP_89D",
            "Ev_KEEP_NULL",
            "Ev_KEEP_BAD_TS",
        }, f"Unexpected remaining rows after prune: {remaining_after_prune}"
        assert get_recorded_reply("Ev_PRUNE_91D", db_path=db_path) is None
        assert get_recorded_reply("Ev_KEEP_89D", db_path=db_path) is not None
        assert get_recorded_reply("Ev_KEEP_NULL", db_path=db_path) is not None
        assert get_recorded_reply("Ev_KEEP_BAD_TS", db_path=db_path) is not None

        return "91日超の行を削除(1件)・89日/NULL/解釈不能文字列の3件を保持・--dry-run時は削除せず予定件数(1件)とdry_run=true返却を確認"


def test_watchdog_unit_recovery() -> str:
    """16. Watchdog ユニット復旧: Unit not found時の再配置・daemon-reload・restart再試行成功、他エラー時スキップ、コピー元欠落時中断、試行上限・backoff・カウンタリセットを確認"""
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        state_dir = tmp_path / "state"
        state_dir.mkdir(parents=True, exist_ok=True)
        hb_path = state_dir / "heartbeat.json"
        log_path = state_dir / "watchdog.log"
        reinstall_state_path = state_dir / "unit_reinstall_state.json"

        # Stale heartbeat (4 minutes old >= 180s)
        hb_path.write_text(
            json.dumps(
                {
                    "timestamp": time.time() - 240,
                    "connected": False,
                    "bot_user_id": "U_YAMA_BOT",
                }
            ),
            encoding="utf-8",
        )

        src_unit = tmp_path / "yama-listener.service.src"
        src_unit.write_text("[Unit]\nDescription=Mock Yama Unit\n", encoding="utf-8")
        dest_dir = tmp_path / "systemd_mock"
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest_unit = dest_dir / "yama-listener.service"

        trace_file = tmp_path / "calls.log"
        mock_restart_script = tmp_path / "mock_restart.sh"
        mock_reload_script = tmp_path / "mock_reload.sh"
        mock_install_script = tmp_path / "mock_install.sh"

        # Case 1: Restart fails with "Unit yama-listener.service not found" when dest_unit is missing;
        #         mock_install copies src -> dest; mock_reload logs reload; 2nd restart succeeds.
        mock_restart_script.write_text(
            f"""#!/usr/bin/env bash
echo "RESTART" >> "{trace_file}"
if [[ ! -f "{dest_unit}" ]]; then
    echo "Failed to restart yama-listener.service: Unit yama-listener.service not found." >&2
    exit 5
fi
echo "MOCK_RESTART_OK"
exit 0
""",
            encoding="utf-8",
        )
        mock_restart_script.chmod(0o755)

        mock_install_script.write_text(
            f"""#!/usr/bin/env bash
echo "INSTALL" >> "{trace_file}"
cp "{src_unit}" "{dest_unit}"
echo "MOCK_INSTALL_OK"
""",
            encoding="utf-8",
        )
        mock_install_script.chmod(0o755)

        mock_reload_script.write_text(
            f"""#!/usr/bin/env bash
echo "DAEMON_RELOAD" >> "{trace_file}"
echo "MOCK_RELOAD_OK"
""",
            encoding="utf-8",
        )
        mock_reload_script.chmod(0o755)

        base_env = os.environ.copy()
        base_env.update(
            {
                "YAMA_STATE_DIR": str(state_dir),
                "YAMA_HEARTBEAT_PATH": str(hb_path),
                "YAMA_WATCHDOG_LOG": str(log_path),
                "YAMA_PYTHON_BIN": str(PYTHON_BIN),
                "YAMA_RESTART_CMD": str(mock_restart_script),
                "YAMA_UNIT_SOURCE": str(src_unit),
                "YAMA_UNIT_DEST": str(dest_unit),
                "YAMA_UNIT_INSTALL_CMD": str(mock_install_script),
                "YAMA_DAEMON_RELOAD_CMD": str(mock_reload_script),
                "YAMA_UNIT_REINSTALL_STATE": str(reinstall_state_path),
                "YAMA_UNIT_REINSTALL_MAX": "3",
                "YAMA_UNIT_REINSTALL_BACKOFF_SEC": "0",
            }
        )

        res1 = subprocess.run(
            ["bash", str(WATCHDOG_SH)], env=base_env, capture_output=True, text=True
        )
        assert res1.returncode == 0, f"Case 1 expected rc=0, got {res1.returncode}, log={log_path.read_text(encoding='utf-8')}"
        assert dest_unit.exists(), "Unit file should have been installed to dest_unit"
        calls1 = trace_file.read_text(encoding="utf-8").strip().splitlines()
        assert calls1 == ["RESTART", "INSTALL", "DAEMON_RELOAD", "RESTART"], (
            f"Expected [RESTART, INSTALL, DAEMON_RELOAD, RESTART], got {calls1}"
        )
        log1 = log_path.read_text(encoding="utf-8")
        assert "Unit reinstall succeeded" in log1
        assert "Daemon reload succeeded" in log1
        assert "Restart retry after unit reinstall succeeded" in log1
        state1 = json.loads(reinstall_state_path.read_text(encoding="utf-8"))
        assert state1["attempts"] == 0, f"Counter should reset to 0 after recovery, got {state1}"

        # Case 2: Restart fails with non-"not found" error -> reinstall is NOT called
        trace_file.write_text("", encoding="utf-8")
        log_path.write_text("", encoding="utf-8")
        mock_other_fail = tmp_path / "mock_other_fail.sh"
        mock_other_fail.write_text(
            f"""#!/usr/bin/env bash
echo "RESTART_OTHER_FAIL" >> "{trace_file}"
echo "Job for yama-listener.service failed because the control process exited with error code." >&2
exit 1
""",
            encoding="utf-8",
        )
        mock_other_fail.chmod(0o755)
        env2 = base_env.copy()
        env2["YAMA_RESTART_CMD"] = str(mock_other_fail)

        res2 = subprocess.run(
            ["bash", str(WATCHDOG_SH)], env=env2, capture_output=True, text=True
        )
        assert res2.returncode == 1
        calls2 = trace_file.read_text(encoding="utf-8").strip().splitlines()
        assert calls2 == ["RESTART_OTHER_FAIL"], (
            f"Non-'not found' failure must not trigger INSTALL or DAEMON_RELOAD, got {calls2}"
        )

        # Case 3: Source unit file missing -> reinstall is NOT attempted and logged
        dest_unit.unlink(missing_ok=True)
        trace_file.write_text("", encoding="utf-8")
        log_path.write_text("", encoding="utf-8")
        env3 = base_env.copy()
        env3["YAMA_UNIT_SOURCE"] = str(tmp_path / "nonexistent-yama-listener.service")

        res3 = subprocess.run(
            ["bash", str(WATCHDOG_SH)], env=env3, capture_output=True, text=True
        )
        assert res3.returncode == 5
        calls3 = trace_file.read_text(encoding="utf-8").strip().splitlines()
        assert calls3 == ["RESTART"], f"Missing source file must not call INSTALL, got {calls3}"
        log3 = log_path.read_text(encoding="utf-8")
        assert "source unit file missing or unreadable" in log3

        # Case 4: Reinstall fails repeatedly (e.g. sudo/permission denied) up to max (3),
        #         4th call does NOT attempt reinstall. Also verify backoff blocks immediate retry.
        trace_file.write_text("", encoding="utf-8")
        log_path.write_text("", encoding="utf-8")
        reinstall_state_path.unlink(missing_ok=True)

        mock_install_fail = tmp_path / "mock_install_fail.sh"
        mock_install_fail.write_text(
            f"""#!/usr/bin/env bash
echo "INSTALL_FAIL" >> "{trace_file}"
echo "sudo: a password is required" >&2
exit 1
""",
            encoding="utf-8",
        )
        mock_install_fail.chmod(0o755)

        # 4a) First attempt with backoff=60s fails -> immediate 2nd call is skipped due to backoff
        env4_backoff = base_env.copy()
        env4_backoff["YAMA_UNIT_INSTALL_CMD"] = str(mock_install_fail)
        env4_backoff["YAMA_UNIT_REINSTALL_BACKOFF_SEC"] = "60"

        res4_1 = subprocess.run(
            ["bash", str(WATCHDOG_SH)], env=env4_backoff, capture_output=True, text=True
        )
        assert res4_1.returncode == 1
        res4_backoff = subprocess.run(
            ["bash", str(WATCHDOG_SH)], env=env4_backoff, capture_output=True, text=True
        )
        assert res4_backoff.returncode == 5
        calls4_backoff = trace_file.read_text(encoding="utf-8").strip().splitlines()
        assert calls4_backoff == ["RESTART", "INSTALL_FAIL", "RESTART"], (
            f"Backoff should block immediate 2nd INSTALL_FAIL, got {calls4_backoff}"
        )
        assert "backoff active" in log_path.read_text(encoding="utf-8")

        # 4b) Reach max attempts (3) -> 4th attempt is skipped with max attempts reached
        trace_file.write_text("", encoding="utf-8")
        log_path.write_text("", encoding="utf-8")
        reinstall_state_path.unlink(missing_ok=True)
        env4_max = base_env.copy()
        env4_max["YAMA_UNIT_INSTALL_CMD"] = str(mock_install_fail)
        env4_max["YAMA_UNIT_REINSTALL_BACKOFF_SEC"] = "0"

        for i in range(3):
            r = subprocess.run(
                ["bash", str(WATCHDOG_SH)], env=env4_max, capture_output=True, text=True
            )
            assert r.returncode == 1, f"Attempt {i+1} should return install failure rc=1"

        state_after_3 = json.loads(reinstall_state_path.read_text(encoding="utf-8"))
        assert state_after_3["attempts"] == 3

        # 4th call: already at max (3/3) -> must NOT call mock_install_fail
        trace_file.write_text("", encoding="utf-8")
        res4_4 = subprocess.run(
            ["bash", str(WATCHDOG_SH)], env=env4_max, capture_output=True, text=True
        )
        assert res4_4.returncode == 5
        calls4_4 = trace_file.read_text(encoding="utf-8").strip().splitlines()
        assert calls4_4 == ["RESTART"], (
            f"After max attempts (3) reached, INSTALL_FAIL must not be called, got {calls4_4}"
        )
        log4 = log_path.read_text(encoding="utf-8")
        assert "Unit reinstall skipped: max attempts reached (3/3" in log4

        # 4c) When unit exists normally and initial restart succeeds, counter resets to 0
        dest_unit.write_text("[Unit]\nDescription=Restored\n", encoding="utf-8")
        res4_reset = subprocess.run(
            ["bash", str(WATCHDOG_SH)], env=env4_max, capture_output=True, text=True
        )
        assert res4_reset.returncode == 0
        state_after_reset = json.loads(reinstall_state_path.read_text(encoding="utf-8"))
        assert state_after_reset["attempts"] == 0, (
            f"Normal restart success should reset attempts to 0, got {state_after_reset}"
        )

        # Case 5: Destination exists with DIFFERENT content -> INSTALL (copy) IS called, followed by DAEMON_RELOAD and RESTART
        reloaded_marker = tmp_path / "daemon_reloaded.flag"
        mock_restart_reload_aware = tmp_path / "mock_restart_reload_aware.sh"
        mock_restart_reload_aware.write_text(
            f"""#!/usr/bin/env bash
echo "RESTART" >> "{trace_file}"
if [[ ! -f "{dest_unit}" || ! -f "{reloaded_marker}" ]]; then
    echo "Failed to restart yama-listener.service: Unit yama-listener.service not found." >&2
    exit 5
fi
echo "MOCK_RESTART_OK"
exit 0
""",
            encoding="utf-8",
        )
        mock_restart_reload_aware.chmod(0o755)

        mock_reload_marker_script = tmp_path / "mock_reload_marker.sh"
        mock_reload_marker_script.write_text(
            f"""#!/usr/bin/env bash
echo "DAEMON_RELOAD" >> "{trace_file}"
touch "{reloaded_marker}"
echo "MOCK_RELOAD_OK"
""",
            encoding="utf-8",
        )
        mock_reload_marker_script.chmod(0o755)

        env5 = base_env.copy()
        env5["YAMA_RESTART_CMD"] = str(mock_restart_reload_aware)
        env5["YAMA_DAEMON_RELOAD_CMD"] = str(mock_reload_marker_script)

        dest_unit.write_text("[Unit]\nDescription=Outdated Or Corrupted Unit\n", encoding="utf-8")
        reloaded_marker.unlink(missing_ok=True)
        trace_file.write_text("", encoding="utf-8")
        log_path.write_text("", encoding="utf-8")
        reinstall_state_path.unlink(missing_ok=True)
        # 独立ケースとして、再起動クールダウンの記録 (last_restart.json) もリセットする
        (state_dir / "last_restart.json").unlink(missing_ok=True)

        res5 = subprocess.run(
            ["bash", str(WATCHDOG_SH)], env=env5, capture_output=True, text=True
        )
        assert res5.returncode == 0, f"Case 5 expected rc=0, got {res5.returncode}, log={log_path.read_text(encoding='utf-8')}"
        calls5 = trace_file.read_text(encoding="utf-8").strip().splitlines()
        assert calls5 == ["RESTART", "INSTALL", "DAEMON_RELOAD", "RESTART"], (
            f"Expected [RESTART, INSTALL, DAEMON_RELOAD, RESTART] on hash mismatch, got {calls5}"
        )
        assert dest_unit.read_text(encoding="utf-8") == src_unit.read_text(encoding="utf-8"), (
            "Destination unit content should be overwritten with source unit content"
        )
        log5 = log_path.read_text(encoding="utf-8")
        assert "hash mismatch" in log5 and "Unit reinstall succeeded" in log5
        state5 = json.loads(reinstall_state_path.read_text(encoding="utf-8"))
        assert state5["attempts"] == 0

        # Case 6: Destination exists with IDENTICAL content -> INSTALL (copy) is SKIPPED, only DAEMON_RELOAD and RESTART run
        dest_unit.write_text(src_unit.read_text(encoding="utf-8"), encoding="utf-8")
        reloaded_marker.unlink(missing_ok=True)
        trace_file.write_text("", encoding="utf-8")
        log_path.write_text("", encoding="utf-8")
        reinstall_state_path.write_text(
            json.dumps({"attempts": 2, "last_attempt_ts": 0.0, "backoff_sec": 0.0, "next_allowed_ts": 0.0}),
            encoding="utf-8",
        )

        res6 = subprocess.run(
            ["bash", str(WATCHDOG_SH)], env=env5, capture_output=True, text=True
        )
        assert res6.returncode == 0, f"Case 6 expected rc=0, got {res6.returncode}, log={log_path.read_text(encoding='utf-8')}"
        calls6 = trace_file.read_text(encoding="utf-8").strip().splitlines()
        assert calls6 == ["RESTART", "DAEMON_RELOAD", "RESTART"], (
            f"Expected [RESTART, DAEMON_RELOAD, RESTART] (no INSTALL) when content is identical, got {calls6}"
        )
        log6 = log_path.read_text(encoding="utf-8")
        assert "skipping copy" in log6
        assert "Unit reinstall succeeded" not in log6
        assert "Daemon reload succeeded" in log6
        assert "Restart retry after unit reinstall succeeded" in log6
        state6 = json.loads(reinstall_state_path.read_text(encoding="utf-8"))
        assert state6["attempts"] == 0, f"Counter should reset to 0 even when copy is skipped, got {state6}"

        # Verify sudoers example file exists and is scoped to yama-listener
        sudoers_example = BASE_DIR / "deploy" / "sudoers-yama-watchdog.example"
        assert sudoers_example.exists(), "deploy/sudoers-yama-watchdog.example must exist"
        sudoers_text = sudoers_example.read_text(encoding="utf-8")
        assert "yama-listener.service" in sudoers_text and "daemon-reload" in sudoers_text

        return "Unit not found時の再配置(欠落・内容不一致時コピー実行、同一内容時コピースキップ)・daemon-reload・restart再試行成功とカウンタリセット、別エラー時・コピー元欠落時の非実行、backoff抑止と上限3回到達後の再配置停止を確認"


def test_record_reply_and_done_error_clear() -> str:
    """17. record-reply & DONE時errorクリア: record-replyで台帳記録・2回目はalready_recorded=trueで1行維持・後のreply --event-idはdeduplicated=trueで実投稿なし・FAILED後のDONE commitでerror_messageがNULLにクリア"""
    import hashlib

    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "slack_events.db"
        init_db(db_path)

        event_id = "Ev0CGMTJ3KBR"
        channel_id = "C_ALL"
        thread_ts = "1794476000.100000"
        landed_reply_ts = "1794476395.000500"
        reply_text = "遅れて着地した返信のテスト本文です。"
        expected_hash = hashlib.sha256(reply_text.encode("utf-8")).hexdigest()

        text_file = Path(tmp) / "late_landed_reply.txt"
        text_file.write_text(reply_text + "\n", encoding="utf-8")

        # Insert event and mark FAILED once with error_message
        handle_event_payload(
            {
                "event_id": event_id,
                "event": {
                    "type": "message",
                    "channel": channel_id,
                    "user": "U_NEIRO",
                    "text": "テストイベント",
                    "ts": thread_ts,
                },
            },
            db_path=db_path,
        )
        ev_failed = commit_event(
            event_id,
            "FAILED",
            error_message="reply not posted, no replies-table record",
            db_path=db_path,
        )
        assert ev_failed["status"] == "PENDING"
        assert ev_failed["retry_count"] == 1
        assert ev_failed["error_message"] == "reply not posted, no replies-table record"

        # 1) First record-reply call -> records row in replies table
        stdout1 = io.StringIO()
        stderr1 = io.StringIO()
        with redirect_stdout(stdout1), redirect_stderr(stderr1):
            rc1 = agent_tools.main(
                [
                    "--db",
                    str(db_path),
                    "record-reply",
                    "--event-id",
                    event_id,
                    "--channel",
                    channel_id,
                    "--reply-ts",
                    landed_reply_ts,
                    "--text-file",
                    str(text_file),
                    "--thread-ts",
                    thread_ts,
                ]
            )
        assert rc1 == 0, f"First record-reply failed: rc={rc1}, stderr={stderr1.getvalue()}"
        out1 = json.loads(stdout1.getvalue())
        assert out1["ok"] is True
        assert out1["event_id"] == event_id
        assert out1["reply_ts"] == landed_reply_ts
        assert out1["already_recorded"] is False
        assert out1["text_hash"] == expected_hash

        rec = get_recorded_reply(event_id, db_path=db_path)
        assert rec is not None
        assert rec["channel_id"] == channel_id
        assert rec["thread_ts"] == thread_ts
        assert rec["reply_ts"] == landed_reply_ts
        assert rec["text_hash"] == expected_hash

        # 2) Second record-reply call with same event_id (even with different reply-ts) -> no-op, stays 1 row
        stdout2 = io.StringIO()
        stderr2 = io.StringIO()
        with redirect_stdout(stdout2), redirect_stderr(stderr2):
            rc2 = agent_tools.main(
                [
                    "--db",
                    str(db_path),
                    "record-reply",
                    "--event-id",
                    event_id,
                    "--channel",
                    channel_id,
                    "--reply-ts",
                    "1794499999.999999",
                    "--text-file",
                    str(text_file),
                    "--thread-ts",
                    thread_ts,
                ]
            )
        assert rc2 == 0, f"Second record-reply failed: rc={rc2}, stderr={stderr2.getvalue()}"
        out2 = json.loads(stdout2.getvalue())
        assert out2["ok"] is True
        assert out2["already_recorded"] is True
        assert out2["deduplicated"] is True
        assert out2["reply_ts"] == landed_reply_ts

        with get_db_connection(db_path) as conn:
            row_count = conn.execute(
                "SELECT COUNT(*) AS cnt FROM replies WHERE event_id = ?",
                (event_id,),
            ).fetchone()["cnt"]
        assert int(row_count) == 1, f"Expected 1 row in replies table, got {row_count}"

        # 3) Subsequent reply --event-id -> deduplicated: true from ledger without calling Slack API
        class ForbiddenSlackClient:
            def auth_test(self) -> Dict[str, Any]:
                raise AssertionError("Slack API must not be called when event_id is in replies table")

            def chat_postMessage(self, **kwargs: Any) -> Dict[str, Any]:
                raise AssertionError("chat_postMessage must not be called when event_id is in replies table")

        stdout3 = io.StringIO()
        stderr3 = io.StringIO()
        with patch.object(
            agent_tools, "get_web_client", return_value=ForbiddenSlackClient()
        ), redirect_stdout(stdout3), redirect_stderr(stderr3):
            rc3 = agent_tools.main(
                [
                    "--db",
                    str(db_path),
                    "reply",
                    "--channel",
                    channel_id,
                    "--text-file",
                    str(text_file),
                    "--thread-ts",
                    thread_ts,
                    "--event-id",
                    event_id,
                ]
            )
        assert rc3 == 0, f"reply after record-reply failed: rc={rc3}, stderr={stderr3.getvalue()}"
        out3 = json.loads(stdout3.getvalue())
        assert out3["ok"] is True
        assert out3["deduplicated"] is True
        assert out3["ts"] == landed_reply_ts
        assert out3["event_id"] == event_id

        # 4) Commit DONE without --error -> clears prior error_message to None (NULL)
        stdout4 = io.StringIO()
        stderr4 = io.StringIO()
        with redirect_stdout(stdout4), redirect_stderr(stderr4):
            rc4 = agent_tools.main(
                [
                    "--db",
                    str(db_path),
                    "commit",
                    "--event-id",
                    event_id,
                    "--status",
                    "DONE",
                ]
            )
        assert rc4 == 0, f"commit DONE failed: rc={rc4}, stderr={stderr4.getvalue()}"
        out4 = json.loads(stdout4.getvalue())["event"]
        assert out4["status"] == "DONE"
        assert out4["error_message"] is None, f"Expected error_message=None on DONE, got {out4['error_message']}"

        return "record-replyでreplies記録＆同一event_id再実行時はalready_recorded=trueで1行維持、後のreply --event-idはdeduplicated=trueで実投稿なし、FAILED後のDONE commitでerror_messageがNULLクリアされることを確認"


def test_watchdog_unit_hash_comparison() -> str:
    """18. Watchdog 再配置ハッシュ照合: 配置先と同一内容時はコピースキップ(daemon-reload+restartのみ)・異なる内容時はコピー実行・配置先欠落時はコピー実行・上限/backoff/カウンタリセット維持"""
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        state_dir = tmp_path / "state"
        state_dir.mkdir(parents=True, exist_ok=True)
        hb_path = state_dir / "heartbeat.json"
        log_path = state_dir / "watchdog.log"
        reinstall_state_path = state_dir / "unit_reinstall_state.json"

        hb_path.write_text(
            json.dumps(
                {
                    "timestamp": time.time() - 300,
                    "connected": False,
                    "bot_user_id": "U_YAMA_BOT",
                }
            ),
            encoding="utf-8",
        )

        src_content = "[Unit]\nDescription=Yama Slack Socket Mode Listener\n[Service]\nExecStart=/usr/bin/python3 listener.py\n"
        src_unit = tmp_path / "yama-listener.service.src"
        src_unit.write_text(src_content, encoding="utf-8")

        dest_dir = tmp_path / "systemd_mock"
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest_unit = dest_dir / "yama-listener.service"

        trace_file = tmp_path / "calls.log"
        reloaded_marker = tmp_path / "reloaded.flag"

        mock_restart_script = tmp_path / "mock_restart.sh"
        mock_restart_script.write_text(
            f"""#!/usr/bin/env bash
echo "RESTART" >> "{trace_file}"
if [[ ! -f "{dest_unit}" || ! -f "{reloaded_marker}" ]]; then
    echo "Failed to restart yama-listener.service: Unit yama-listener.service not found." >&2
    exit 5
fi
echo "MOCK_RESTART_OK"
exit 0
""",
            encoding="utf-8",
        )
        mock_restart_script.chmod(0o755)

        mock_install_script = tmp_path / "mock_install.sh"
        mock_install_script.write_text(
            f"""#!/usr/bin/env bash
echo "INSTALL" >> "{trace_file}"
cp "{src_unit}" "{dest_unit}"
echo "MOCK_INSTALL_OK"
""",
            encoding="utf-8",
        )
        mock_install_script.chmod(0o755)

        mock_reload_script = tmp_path / "mock_reload.sh"
        mock_reload_script.write_text(
            f"""#!/usr/bin/env bash
echo "DAEMON_RELOAD" >> "{trace_file}"
touch "{reloaded_marker}"
echo "MOCK_RELOAD_OK"
""",
            encoding="utf-8",
        )
        mock_reload_script.chmod(0o755)

        env = os.environ.copy()
        env.update(
            {
                "YAMA_STATE_DIR": str(state_dir),
                "YAMA_HEARTBEAT_PATH": str(hb_path),
                "YAMA_WATCHDOG_LOG": str(log_path),
                "YAMA_PYTHON_BIN": str(PYTHON_BIN),
                "YAMA_RESTART_CMD": str(mock_restart_script),
                "YAMA_UNIT_SOURCE": str(src_unit),
                "YAMA_UNIT_DEST": str(dest_unit),
                "YAMA_UNIT_INSTALL_CMD": str(mock_install_script),
                "YAMA_DAEMON_RELOAD_CMD": str(mock_reload_script),
                "YAMA_UNIT_REINSTALL_STATE": str(reinstall_state_path),
                "YAMA_UNIT_REINSTALL_MAX": "3",
                "YAMA_UNIT_REINSTALL_BACKOFF_SEC": "0",
            }
        )

        # 1) 配置先に同一内容が既にある -> コピーコマンド(INSTALL)は呼ばれず、DAEMON_RELOAD と RESTART だけが呼ばれ、カウンタもリセットされる
        dest_unit.write_text(src_content, encoding="utf-8")
        reloaded_marker.unlink(missing_ok=True)
        trace_file.write_text("", encoding="utf-8")
        log_path.write_text("", encoding="utf-8")
        reinstall_state_path.write_text(
            json.dumps({"attempts": 1, "last_attempt_ts": 0.0, "backoff_sec": 0.0, "next_allowed_ts": 0.0}),
            encoding="utf-8",
        )

        res_same = subprocess.run(
            ["bash", str(WATCHDOG_SH)], env=env, capture_output=True, text=True
        )
        assert res_same.returncode == 0
        calls_same = trace_file.read_text(encoding="utf-8").strip().splitlines()
        assert calls_same == ["RESTART", "DAEMON_RELOAD", "RESTART"], (
            f"Identical content should skip INSTALL: got {calls_same}"
        )
        log_same = log_path.read_text(encoding="utf-8")
        assert "skipping copy" in log_same
        assert json.loads(reinstall_state_path.read_text(encoding="utf-8"))["attempts"] == 0

        # 2) 配置先に異なる内容がある -> コピーコマンド(INSTALL)が呼ばれ、内容が上書きされ、DAEMON_RELOAD -> RESTART が成功する
        dest_unit.write_text("[Unit]\nDescription=Tampered Or Old Unit\n", encoding="utf-8")
        reloaded_marker.unlink(missing_ok=True)
        trace_file.write_text("", encoding="utf-8")
        log_path.write_text("", encoding="utf-8")

        res_diff = subprocess.run(
            ["bash", str(WATCHDOG_SH)], env=env, capture_output=True, text=True
        )
        assert res_diff.returncode == 0
        calls_diff = trace_file.read_text(encoding="utf-8").strip().splitlines()
        assert calls_diff == ["RESTART", "INSTALL", "DAEMON_RELOAD", "RESTART"], (
            f"Different content should call INSTALL: got {calls_diff}"
        )
        assert dest_unit.read_text(encoding="utf-8") == src_content
        log_diff = log_path.read_text(encoding="utf-8")
        assert "hash mismatch" in log_diff and "Unit reinstall succeeded" in log_diff
        assert json.loads(reinstall_state_path.read_text(encoding="utf-8"))["attempts"] == 0

        # 3) 配置先が無い -> コピーコマンド(INSTALL)が呼ばれる（既存動作の回帰確認）
        dest_unit.unlink(missing_ok=True)
        reloaded_marker.unlink(missing_ok=True)
        trace_file.write_text("", encoding="utf-8")
        log_path.write_text("", encoding="utf-8")

        res_missing = subprocess.run(
            ["bash", str(WATCHDOG_SH)], env=env, capture_output=True, text=True
        )
        assert res_missing.returncode == 0
        calls_missing = trace_file.read_text(encoding="utf-8").strip().splitlines()
        assert calls_missing == ["RESTART", "INSTALL", "DAEMON_RELOAD", "RESTART"], (
            f"Missing destination should call INSTALL: got {calls_missing}"
        )
        assert dest_unit.exists() and dest_unit.read_text(encoding="utf-8") == src_content
        assert json.loads(reinstall_state_path.read_text(encoding="utf-8"))["attempts"] == 0

        return "同一内容配置済み時はINSTALLスキップ(DAEMON_RELOAD+RESTARTのみ実行＆ログ記録＆カウンタリセット)、異なる内容時はINSTALL実行＆上書き、配置先欠落時はINSTALL実行を確認"


def test_reconcile_replies() -> str:
    """19. Reconcile-replies 遅延着地返信の定期照合: 候補抽出(DONE/FAILED & reply/ambiguous & 未記録 & 直近N日)・スレッド走査と台帳記録・未発見時スキップ・記録済み除外・同一スレッド複数候補の古い順割当・--dry-run・API失敗時継続を確認"""
    import hashlib

    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "slack_events.db"
        agent_tools.init_db(db_path)

        bot_uid = "U_YAMA_BOT"
        channel_id = "C_ALL"

        # 1) Candidate 1: DONE with error_message containing 'reply not posted', no replies record, parent event (thread_ts is None -> uses ts)
        ev_cand_done = "Ev0CGMTJ3KBR"
        th_1 = "1794476000.100000"
        reply_ts_1 = "1794476395.285939"
        reply_text_1 = "遅れて着地したYamaの返信1です。"
        expected_hash_1 = hashlib.sha256(reply_text_1.encode("utf-8")).hexdigest()

        # 2) Candidate 2: FAILED with 'reply not posted', thread has NO Yama post -> not_found, not recorded
        ev_cand_no_yama = "Ev_NO_YAMA_POST"
        th_2 = "1794477000.100000"

        # 3) Already recorded event: DONE with 'reply not posted', but already in replies table -> NOT a candidate
        ev_already_recorded = "Ev_ALREADY_IN_REPLIES"
        th_3 = "1794478000.100000"
        already_reply_ts = "1794478050.000100"

        # 4) Two candidates sharing the same thread (th_4):
        #    ev_shared_old (ts=1794479001) and ev_shared_new (ts=1794479005, FAILED with 'ambiguous_post_outcome')
        #    Thread th_4 has one already-recorded reply_ts (for another event) and two unrecorded Yama replies:
        #    reply_ts_4a (older) and reply_ts_4b (newer).
        #    ev_shared_old must get reply_ts_4a, and ev_shared_new must get reply_ts_4b without duplicate assignment.
        th_4 = "1794479000.100000"
        ev_shared_pre = "Ev_SHARED_PRE_RECORDED"
        reply_ts_4_pre = "1794479000.500000"
        ev_shared_old = "Ev_SHARED_OLD"
        ev_shared_new = "Ev_SHARED_NEW"
        reply_ts_4a = "1794479010.000100"
        reply_text_4a = "共有スレッドの古い方の返信"
        reply_ts_4b = "1794479020.000200"
        reply_text_4b = "共有スレッドの新しい方の返信"

        # 5) Non-candidates that must be ignored:
        #    - DONE with unrelated error_message
        #    - DONE with error_message=None
        #    - PENDING with 'reply not posted'
        #    - Old event (> 7 days ago)
        now_dt = datetime.now(timezone.utc)
        recent_ts_str = (now_dt - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
        old_ts_str = (now_dt - timedelta(days=10)).strftime("%Y-%m-%d %H:%M:%S")

        with get_db_connection(db_path) as conn:
            events_rows = [
                (ev_cand_done, channel_id, "U_USER1", "Q1", th_1, None, "DONE", "reply not posted, no replies-table record", reply_text_1, recent_ts_str),
                (ev_cand_no_yama, channel_id, "U_USER2", "Q2", th_2, None, "FAILED", "reply not posted", "未着地の送信文面", recent_ts_str),
                (ev_already_recorded, channel_id, "U_USER3", "Q3", th_3, None, "FAILED", "reply not posted", "既に台帳記録済みの返信", recent_ts_str),
                # Insert ev_shared_new BEFORE ev_shared_old in row order to verify sorting by event ts (oldest first)
                (ev_shared_new, channel_id, "U_USER4", "Q4b", "1794479005.000000", th_4, "FAILED", "ambiguous_post_outcome", reply_text_4b, recent_ts_str),
                (ev_shared_old, channel_id, "U_USER4", "Q4a", "1794479001.000000", th_4, "DONE", "reply not posted", reply_text_4a, recent_ts_str),
                ("Ev_UNRELATED_ERR", channel_id, "U_USER5", "Q5", "1794480000.000000", None, "FAILED", "rate_limited", None, recent_ts_str),
                ("Ev_NO_ERR", channel_id, "U_USER6", "Q6", "1794481000.000000", None, "DONE", None, None, recent_ts_str),
                ("Ev_PENDING_REPLY", channel_id, "U_USER7", "Q7", "1794482000.000000", None, "PENDING", "reply not posted", None, recent_ts_str),
                ("Ev_TOO_OLD_10D", channel_id, "U_USER8", "Q8", "1794470000.000000", None, "DONE", "reply not posted", None, old_ts_str),
            ]
            conn.executemany(
                """
                INSERT INTO events (
                    event_id, channel_id, user_id, text, ts, thread_ts, status, error_message, intended_text, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [(e[0], e[1], e[2], e[3], e[4], e[5], e[6], e[7], e[8], e[9], e[9]) for e in events_rows],
            )
            conn.commit()

        # Pre-record ev_already_recorded and ev_shared_pre in replies table
        agent_tools.record_reply(
            event_id=ev_already_recorded,
            channel_id=channel_id,
            thread_ts=th_3,
            reply_ts=already_reply_ts,
            text="既に台帳記録済みの返信",
            db_path=db_path,
        )
        agent_tools.record_reply(
            event_id=ev_shared_pre,
            channel_id=channel_id,
            thread_ts=th_4,
            reply_ts=reply_ts_4_pre,
            text="共有スレッドの既存記録済み返信",
            db_path=db_path,
        )

        queried_threads: List[str] = []

        class ReconcileMockClient:
            def auth_test(self) -> Dict[str, Any]:
                return {"ok": True, "user_id": bot_uid}

            def conversations_replies(self, **kwargs: Any) -> Dict[str, Any]:
                ts = kwargs.get("ts")
                queried_threads.append(str(ts))
                if ts == th_1:
                    return {
                        "ok": True,
                        "messages": [
                            {"user": "U_USER1", "text": "Q1", "ts": th_1},
                            {"user": bot_uid, "text": reply_text_1, "ts": reply_ts_1, "thread_ts": th_1},
                        ],
                        "has_more": False,
                    }
                if ts == th_2:
                    return {
                        "ok": True,
                        "messages": [
                            {"user": "U_USER2", "text": "Q2", "ts": th_2},
                            {"user": "U_OTHER_HUMAN", "text": "Human comment", "ts": "1794477010.000100", "thread_ts": th_2},
                        ],
                        "has_more": False,
                    }
                if ts == th_4:
                    return {
                        "ok": True,
                        "messages": [
                            {"user": "U_USER4", "text": "Thread parent", "ts": th_4},
                            {"user": bot_uid, "text": "既存記録済み", "ts": reply_ts_4_pre, "thread_ts": th_4},
                            {"user": bot_uid, "text": reply_text_4b, "ts": reply_ts_4b, "thread_ts": th_4},
                            {"user": bot_uid, "text": reply_text_4a, "ts": reply_ts_4a, "thread_ts": th_4},
                        ],
                        "has_more": False,
                    }
                raise AssertionError(f"Unexpected thread_ts queried: {ts}")

        # Step A: --dry-run -> does NOT write to replies table, but returns planned recordings
        stdout_dry = io.StringIO()
        stderr_dry = io.StringIO()
        with patch.object(
            agent_tools, "get_web_client", return_value=ReconcileMockClient()
        ), redirect_stdout(stdout_dry), redirect_stderr(stderr_dry):
            rc_dry = agent_tools.main(
                [
                    "--db",
                    str(db_path),
                    "reconcile-replies",
                    "--days",
                    "7",
                    "--dry-run",
                ]
            )

        assert rc_dry == 0, f"reconcile-replies --dry-run failed: rc={rc_dry}, stderr={stderr_dry.getvalue()}"
        out_dry = json.loads(stdout_dry.getvalue())
        assert out_dry["ok"] is True
        assert out_dry["dry_run"] is True
        assert out_dry["checked"] == 4, f"Expected 4 candidates checked, got {out_dry}"
        assert out_dry["recorded"] == [
            {"event_id": ev_cand_done, "reply_ts": reply_ts_1},
            {"event_id": ev_shared_old, "reply_ts": reply_ts_4a},
            {"event_id": ev_shared_new, "reply_ts": reply_ts_4b},
        ], f"Unexpected dry-run recorded list: {out_dry['recorded']}"
        assert out_dry["not_found"] == [ev_cand_no_yama], f"Unexpected not_found: {out_dry['not_found']}"
        assert th_3 not in queried_threads, "Already-recorded event must not be queried as a candidate"

        # Confirm DB was untouched during --dry-run
        assert get_recorded_reply(ev_cand_done, db_path=db_path) is None
        assert get_recorded_reply(ev_shared_old, db_path=db_path) is None
        assert get_recorded_reply(ev_shared_new, db_path=db_path) is None
        assert get_recorded_reply(ev_cand_no_yama, db_path=db_path) is None

        # Step B: Real reconcile run -> writes found replies to replies ledger
        queried_threads.clear()
        stdout_run = io.StringIO()
        stderr_run = io.StringIO()
        with patch.object(
            agent_tools, "get_web_client", return_value=ReconcileMockClient()
        ), redirect_stdout(stdout_run), redirect_stderr(stderr_run):
            rc_run = agent_tools.main(
                [
                    "--db",
                    str(db_path),
                    "reconcile-replies",
                    "--days",
                    "7",
                ]
            )

        assert rc_run == 0, f"reconcile-replies failed: rc={rc_run}, stderr={stderr_run.getvalue()}"
        out_run = json.loads(stdout_run.getvalue())
        assert out_run["ok"] is True
        assert out_run["dry_run"] is False
        assert out_run["checked"] == 4
        assert out_run["recorded"] == [
            {"event_id": ev_cand_done, "reply_ts": reply_ts_1},
            {"event_id": ev_shared_old, "reply_ts": reply_ts_4a},
            {"event_id": ev_shared_new, "reply_ts": reply_ts_4b},
        ]
        assert out_run["not_found"] == [ev_cand_no_yama]
        assert out_run["errors"] == 0

        rec1 = get_recorded_reply(ev_cand_done, db_path=db_path)
        assert rec1 is not None
        assert rec1["reply_ts"] == reply_ts_1
        assert rec1["thread_ts"] == th_1
        assert rec1["text_hash"] == expected_hash_1

        rec_old = get_recorded_reply(ev_shared_old, db_path=db_path)
        rec_new = get_recorded_reply(ev_shared_new, db_path=db_path)
        assert rec_old is not None and rec_old["reply_ts"] == reply_ts_4a
        assert rec_new is not None and rec_new["reply_ts"] == reply_ts_4b
        assert get_recorded_reply(ev_cand_no_yama, db_path=db_path) is None

        # Step C: Running reconcile again -> the 3 newly recorded events are no longer candidates, only ev_cand_no_yama remains
        stdout_again = io.StringIO()
        with patch.object(
            agent_tools, "get_web_client", return_value=ReconcileMockClient()
        ), redirect_stdout(stdout_again):
            rc_again = agent_tools.main(["--db", str(db_path), "reconcile-replies"])
        assert rc_again == 0
        out_again = json.loads(stdout_again.getvalue())
        assert out_again["checked"] == 1
        assert out_again["recorded"] == []
        assert out_again["not_found"] == [ev_cand_no_yama]

        # Step D: Slack API error on one candidate does not abort processing of subsequent candidates
        ev_api_err = "Ev_API_ERR"
        th_err = "1794477500.100000"
        ev_after_err = "Ev_AFTER_ERR"
        th_after = "1794485000.100000"
        reply_ts_after = "1794485010.000100"
        with get_db_connection(db_path) as conn:
            conn.executemany(
                """
                INSERT INTO events (
                    event_id, channel_id, user_id, text, ts, thread_ts, status, error_message, intended_text, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (ev_api_err, channel_id, "U_USER9", "Q_err", th_err, None, "DONE", "reply not posted", "エラー時の文面", recent_ts_str, recent_ts_str),
                    (ev_after_err, channel_id, "U_USER9", "Q_ok", th_after, None, "DONE", "reply not posted", "エラー後の正常記録", recent_ts_str, recent_ts_str),
                ],
            )
            conn.commit()

        class PartialErrorClient:
            def conversations_replies(self, **kwargs: Any) -> Dict[str, Any]:
                ts = kwargs.get("ts")
                if ts == th_err:
                    raise SlackApiError(
                        message="channel_not_found",
                        response={"ok": False, "error": "channel_not_found"},
                    )
                if ts == th_after:
                    return {
                        "ok": True,
                        "messages": [
                            {"user": bot_uid, "text": "エラー後の正常記録", "ts": reply_ts_after, "thread_ts": th_after},
                        ],
                        "has_more": False,
                    }
                return {"ok": True, "messages": [], "has_more": False}

        stdout_err = io.StringIO()
        with patch.object(
            agent_tools, "get_web_client", return_value=PartialErrorClient()
        ), redirect_stdout(stdout_err):
            rc_err = agent_tools.main(["--db", str(db_path), "reconcile-replies"])
        assert rc_err == 0
        out_err = json.loads(stdout_err.getvalue())
        assert out_err["checked"] == 3  # ev_cand_no_yama, ev_api_err, ev_after_err
        assert out_err["errors"] == 1
        assert out_err["recorded"] == [{"event_id": ev_after_err, "reply_ts": reply_ts_after}]
        assert out_err["not_found"] == [ev_cand_no_yama]
        assert get_recorded_reply(ev_after_err, db_path=db_path) is not None

        return "候補抽出・Yama投稿の台帳記録(text_hash計算)・未発見スキップ・記録済み候補除外・同一スレッド古い順割当・--dry-run・APIエラー時の継続処理を確認"


def test_history_retry_5_attempts_and_backoff() -> str:
    """20. (a) 履歴取得リトライ強化: 接続系例外で3回失敗した後に4回目で成功するケースの回復(1→2→4sバックオフ)、最大5回試行(1→2→4→8s)、および全経路(fetch_history/投稿前チェック/回復確認/reconcile)での適用を確認"""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "slack_events.db"
        init_db(db_path)

        bot_uid = "U_YAMA_BOT"
        channel_id = "C_ALL"
        thread_ts = "1794491580.000100"

        # 1) fetch_history: fails 3 times with IncompleteRead, RemoteDisconnected, URLError -> succeeds on 4th attempt
        fh_sleeps: List[float] = []
        fh_calls = {"count": 0}
        transient_sequence = [
            http.client.IncompleteRead(b"partial-1", 32),
            http.client.RemoteDisconnected("Remote end closed connection without response"),
            urllib.error.URLError("temporary network unreachable"),
        ]

        class SucceedOn4thHistoryClient:
            def conversations_history(self, channel: str, limit: int = 20, include_all_metadata: bool = False) -> Dict[str, Any]:
                idx = fh_calls["count"]
                fh_calls["count"] += 1
                if idx < 3:
                    raise transient_sequence[idx]
                return {
                    "ok": True,
                    "messages": [
                        {"user": "U_HUMAN", "text": "4th attempt succeeded", "ts": "1794491581.000100"}
                    ],
                }

        res_fh = fetch_history(
            channel=channel_id,
            limit=20,
            db_path=db_path,
            client=SucceedOn4thHistoryClient(),
            sleep_fn=lambda s: fh_sleeps.append(s),
        )
        assert res_fh["ok"] is True
        assert res_fh["source"] == "slack_api"
        assert res_fh["count"] == 1
        assert res_fh["messages"][0]["text"] == "4th attempt succeeded"
        assert fh_calls["count"] == 4, f"Expected 4 attempts (3 failures + 1 success), got {fh_calls['count']}"
        assert fh_sleeps == [1.0, 2.0, 4.0], f"Expected exponential backoffs [1.0, 2.0, 4.0], got {fh_sleeps}"

        # 2) Pre-post thread check in cmd_reply: fails 3 times -> succeeds on 4th attempt and detects existing reply (deduplicated=True)
        text_file = Path(tmp) / "share_reconcile.txt"
        share_text = "定期照合（reconcile-replies）の初回実行結果を共有します。"
        text_file.write_text(share_text + "\n", encoding="utf-8")

        pre_sleeps: List[float] = []
        pre_calls = {"count": 0}
        existing_reply_ts = "1794491587.025199"

        class SucceedOn4thPrePostClient:
            def auth_test(self) -> Dict[str, Any]:
                return {"ok": True, "user_id": bot_uid}

            def conversations_replies(self, **kwargs: Any) -> Dict[str, Any]:
                idx = pre_calls["count"]
                pre_calls["count"] += 1
                if idx < 3:
                    raise transient_sequence[idx]
                return {
                    "ok": True,
                    "messages": [
                        {"user": "U_PARENT", "text": "Parent", "ts": thread_ts},
                        {"user": bot_uid, "text": share_text, "ts": existing_reply_ts, "thread_ts": thread_ts},
                    ],
                }

            def chat_postMessage(self, **kwargs: Any) -> Dict[str, Any]:
                raise AssertionError("chat_postMessage must NOT be called when 4th attempt finds existing reply")

        stdout_pre = io.StringIO()
        stderr_pre = io.StringIO()
        with patch.object(
            agent_tools, "get_web_client", return_value=SucceedOn4thPrePostClient()
        ), patch.object(
            agent_tools.time, "sleep", side_effect=lambda s: pre_sleeps.append(s)
        ), redirect_stdout(stdout_pre), redirect_stderr(stderr_pre):
            rc_pre = agent_tools.main(
                [
                    "--db",
                    str(db_path),
                    "reply",
                    "--channel",
                    channel_id,
                    "--text-file",
                    str(text_file),
                    "--thread-ts",
                    thread_ts,
                    "--event-id",
                    "share-reconcile-20261112",
                    "--force",
                    "--reason",
                    "test fixture: events row intentionally absent (pre-candidate-28 call shape)",
                ]
            )

        assert rc_pre == 0, f"Expected rc=0, got {rc_pre}, stderr={stderr_pre.getvalue()}"
        assert pre_calls["count"] == 4
        assert pre_sleeps == [1.0, 2.0, 4.0]
        out_pre = json.loads(stdout_pre.getvalue())
        assert out_pre["ok"] is True
        assert out_pre["deduplicated"] is True
        assert out_pre["ts"] == existing_reply_ts

        # 3) reconcile_replies: thread scan fails 3 times -> succeeds on 4th attempt
        now_dt = datetime.now(timezone.utc)
        recent_ts_str = (now_dt - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
        agent_tools.init_db(db_path)
        with get_db_connection(db_path) as conn:
            conn.execute(
                """
                INSERT INTO events (
                    event_id, channel_id, user_id, text, ts, thread_ts, status, error_message, intended_text, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "Ev_REC_4TH",
                    channel_id,
                    "U_HUMAN",
                    "Parent",
                    thread_ts,
                    None,
                    "FAILED",
                    "ambiguous_post_outcome",
                    "Recovered on 4th attempt",
                    recent_ts_str,
                    recent_ts_str,
                ),
            )
            conn.commit()

        rec_sleeps: List[float] = []
        rec_calls = {"count": 0}
        rec_reply_ts = "1794491599.000100"

        class SucceedOn4thReconcileClient:
            def auth_test(self) -> Dict[str, Any]:
                return {"ok": True, "user_id": bot_uid}

            def conversations_replies(self, **kwargs: Any) -> Dict[str, Any]:
                idx = rec_calls["count"]
                rec_calls["count"] += 1
                if idx < 3:
                    raise transient_sequence[idx]
                return {
                    "ok": True,
                    "messages": [
                        {"user": bot_uid, "text": "Recovered on 4th attempt", "ts": rec_reply_ts, "thread_ts": thread_ts},
                    ],
                    "has_more": False,
                }

        res_rec = agent_tools.reconcile_replies(
            days=7,
            dry_run=False,
            db_path=db_path,
            client=SucceedOn4thReconcileClient(),
            sleep_fn=lambda s: rec_sleeps.append(s),
        )
        assert res_rec["ok"] is True
        assert res_rec["errors"] == 0
        assert res_rec["recorded"] == [{"event_id": "Ev_REC_4TH", "reply_ts": rec_reply_ts}]
        assert rec_calls["count"] == 4
        assert rec_sleeps == [1.0, 2.0, 4.0]

        return "接続系例外3回失敗後の4回目成功(1s→2s→4s)をfetch_history・投稿前チェック・reconcile-repliesの全経路で確認、最大5回試行(1s→2s→4s→8s)を確認"


def test_pre_post_check_fail_closed_thread_and_fail_open_channel() -> str:
    """21. (b) 送信前チェックの fail-closed 化: スレッド返信で履歴取得が全試行失敗時は実投稿せず pre_post_check_unverifiable を返し、チャンネル投稿で全試行失敗時は従来通り投稿される（fail-open回帰）ことを確認"""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "slack_events.db"
        init_db(db_path)

        bot_uid = "U_YAMA_BOT"
        channel_id = "C_ALL"
        thread_ts = "1794491580.000100"

        text_file = Path(tmp) / "reply_fail_closed.txt"
        reply_text = "スレッド返信の送信前チェック fail-closed テスト文面です。"
        text_file.write_text(reply_text + "\n", encoding="utf-8")

        # 1) Thread reply (--thread-ts specified): all 5 history attempts fail -> must NOT post, returns pre_post_check_unverifiable (exit 1)
        thread_history_calls = {"count": 0}
        thread_post_calls: List[Dict[str, Any]] = []
        thread_sleeps: List[float] = []

        class ThreadPreCheckDownClient:
            def auth_test(self) -> Dict[str, Any]:
                return {"ok": True, "user_id": bot_uid}

            def conversations_replies(self, **kwargs: Any) -> Dict[str, Any]:
                thread_history_calls["count"] += 1
                raise http.client.IncompleteRead(b"thread-history-dropped", 64)

            def chat_postMessage(self, **kwargs: Any) -> Dict[str, Any]:
                thread_post_calls.append(kwargs)
                raise AssertionError("chat_postMessage must NOT be called when thread pre-post check fails")

        stdout_th = io.StringIO()
        stderr_th = io.StringIO()
        with patch.object(
            agent_tools, "get_web_client", return_value=ThreadPreCheckDownClient()
        ), patch.object(
            agent_tools.time, "sleep", side_effect=lambda s: thread_sleeps.append(s)
        ), redirect_stdout(stdout_th), redirect_stderr(stderr_th):
            rc_th = agent_tools.main(
                [
                    "--db",
                    str(db_path),
                    "reply",
                    "--channel",
                    channel_id,
                    "--text-file",
                    str(text_file),
                    "--thread-ts",
                    thread_ts,
                    "--event-id",
                    "Ev_THREAD_FAIL_CLOSED",
                    "--force",
                    "--reason",
                    "test fixture: events row intentionally absent (pre-candidate-28 call shape)",
                ]
            )

        assert rc_th == 1, f"Expected exit code 1 when thread pre-post check is unverifiable, got {rc_th}"
        assert len(thread_post_calls) == 0, f"Expected 0 posts on fail-closed thread check, got {len(thread_post_calls)}"
        assert thread_history_calls["count"] == 5, (
            f"Expected 5 history attempts before failing closed, got {thread_history_calls['count']}"
        )
        assert thread_sleeps == [1.0, 2.0, 4.0, 8.0], (
            f"Expected backoffs [1.0, 2.0, 4.0, 8.0], got {thread_sleeps}"
        )
        assert stdout_th.getvalue() == "", f"Expected empty stdout on error, got {stdout_th.getvalue()}"
        err_th = json.loads(stderr_th.getvalue())
        assert err_th["ok"] is False
        assert err_th["error"] == "pre_post_check_unverifiable"
        assert err_th["channel"] == channel_id
        assert err_th["thread_ts"] == thread_ts
        assert err_th["event_id"] == "Ev_THREAD_FAIL_CLOSED"
        assert get_recorded_reply("Ev_THREAD_FAIL_CLOSED", db_path=db_path) is None

        # 2) Channel post (no --thread-ts): all 5 bounded check history attempts fail -> logs warning and posts normally (fail-open)
        ch_history_calls = {"count": 0}
        ch_post_calls: List[Dict[str, Any]] = []
        ch_sleeps: List[float] = []
        posted_ch_ts = "1794492000.000100"

        class ChannelPreCheckDownClient:
            def auth_test(self) -> Dict[str, Any]:
                return {"ok": True, "user_id": bot_uid}

            def conversations_history(self, **kwargs: Any) -> Dict[str, Any]:
                ch_history_calls["count"] += 1
                raise urllib.error.URLError("channel history temporarily unreachable")

            def chat_postMessage(self, **kwargs: Any) -> Dict[str, Any]:
                ch_post_calls.append(kwargs)
                return {
                    "ok": True,
                    "channel": kwargs["channel"],
                    "ts": posted_ch_ts,
                    "message": {"text": kwargs["text"], "ts": posted_ch_ts},
                }

        stdout_ch = io.StringIO()
        stderr_ch = io.StringIO()
        with patch.object(
            agent_tools, "get_web_client", return_value=ChannelPreCheckDownClient()
        ), patch.object(
            agent_tools.time, "sleep", side_effect=lambda s: ch_sleeps.append(s)
        ), redirect_stdout(stdout_ch), redirect_stderr(stderr_ch):
            rc_ch = agent_tools.main(
                [
                    "--db",
                    str(db_path),
                    "reply",
                    "--channel",
                    channel_id,
                    "--text-file",
                    str(text_file),
                    "--event-id",
                    "Ev_CHANNEL_FAIL_OPEN",
                    "--force",
                    "--reason",
                    "test fixture: events row intentionally absent (pre-candidate-28 call shape)",
                ]
            )

        assert rc_ch == 0, f"Expected exit code 0 on channel fail-open, got {rc_ch}, stderr={stderr_ch.getvalue()}"
        assert ch_history_calls["count"] == 5, f"Expected 5 history attempts on channel check, got {ch_history_calls['count']}"
        assert ch_sleeps == [1.0, 2.0, 4.0, 8.0]
        assert len(ch_post_calls) == 1, f"Expected 1 post on channel fail-open, got {len(ch_post_calls)}"
        warn_ch = json.loads(stderr_ch.getvalue())
        assert warn_ch["warning"] == "pre_post_channel_check_failed"
        out_ch = json.loads(stdout_ch.getvalue())
        assert out_ch["ok"] is True
        assert out_ch["ts"] == posted_ch_ts
        assert out_ch["deduplicated"] is False
        rec_ch = get_recorded_reply("Ev_CHANNEL_FAIL_OPEN", db_path=db_path)
        assert rec_ch is not None and rec_ch["reply_ts"] == posted_ch_ts

        return "スレッド返信は投稿前履歴取得5回全失敗時に投稿せずpre_post_check_unverifiable(exit 1)で停止、チャンネル投稿は5回全失敗時も警告出力のうえ従来通り投稿(fail-open)を確認"


def test_reconcile_done_residual_errors() -> str:
    """22. (c) DONE残存エラーの網羅的定期照合: DONE＋error_message残存は返信あり→replies記録＆errorクリア(with_reply)、返信なし→errorクリア(without_reply)、スレッド走査エラー→error維持＆errors計上、--dry-run確認"""
    import hashlib

    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "slack_events.db"
        init_db(db_path)

        bot_uid = "U_YAMA_BOT"
        channel_id = "C_ALL"
        now_dt = datetime.now(timezone.utc)
        recent_ts_str = (now_dt - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")

        # Case 1: DONE + error_message ('IncompleteRead', not matching %reply%/%ambiguous%) + unrecorded in replies + Yama reply in thread
        # -> records reply, clears error_message, listed in done_error_cleared["with_reply"]
        ev_done_unrec = "Ev_DONE_ERR_UNREC"
        th_unrec = "1794493000.100000"
        reply_ts_unrec = "1794493010.000100"
        reply_text_unrec = "Yamaの着地済み返信（IncompleteRead残存エラー解消テスト）"
        expected_hash_unrec = hashlib.sha256(reply_text_unrec.encode("utf-8")).hexdigest()

        # Case 2: DONE + error_message ('reply not posted, no replies-table record') + already recorded in replies + Yama reply in thread
        # -> clears error_message, listed in done_error_cleared["with_reply"]
        ev_done_alr = "Ev_DONE_ERR_ALREADY_REC"
        th_alr = "1794494000.100000"
        reply_ts_alr = "1794494010.000200"
        reply_text_alr = "Yamaの台帳記録済みかつerror_message残存イベントの返信"

        # Case 3: DONE + error_message ('max attempts exceeded' / 'pre_post_check_unverifiable') + NO Yama reply in thread
        # -> clears error_message even without reply, listed in done_error_cleared["without_reply"]
        ev_done_no_reply = "Ev_DONE_ERR_NO_REPLY"
        th_no_reply = "1794495000.100000"

        # Case 4: DONE + error_message ('reply not posted', in Phase 1 & Phase 2) + NO Yama reply in thread
        # -> Phase 1 not_found, Phase 2 clears error_message and lists in done_error_cleared["without_reply"]
        ev_done_phase1_no_reply = "Ev_DONE_PHASE1_NO_REPLY"
        th_phase1_no_reply = "1794495500.100000"

        # Case 5: DONE + error_message ('max attempts exceeded') + thread scan fails with history fetch error
        # -> error_message is NOT cleared (preserved for next reconcile), counted in errors / error_details
        ev_done_scan_err = "Ev_DONE_ERR_SCAN_FAIL"
        th_scan_err = "1794496000.100000"

        agent_tools.init_db(db_path)
        with get_db_connection(db_path) as conn:
            conn.executemany(
                """
                INSERT INTO events (
                    event_id, channel_id, user_id, text, ts, thread_ts, status, error_message, intended_text, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (ev_done_unrec, channel_id, "U_U1", "Q_unrec", th_unrec, None, "DONE", "IncompleteRead(64 bytes read)", reply_text_unrec, recent_ts_str, recent_ts_str),
                    (ev_done_alr, channel_id, "U_U2", "Q_alr", th_alr, None, "DONE", "reply not posted, no replies-table record", None, recent_ts_str, recent_ts_str),
                    (ev_done_no_reply, channel_id, "U_U3", "Q_norep", th_no_reply, None, "DONE", "pre_post_check_unverifiable", None, recent_ts_str, recent_ts_str),
                    (ev_done_phase1_no_reply, channel_id, "U_U4", "Q_p1_norep", th_phase1_no_reply, None, "DONE", "reply not posted", None, recent_ts_str, recent_ts_str),
                    (ev_done_scan_err, channel_id, "U_U5", "Q_scan_err", th_scan_err, None, "DONE", "max attempts exceeded", None, recent_ts_str, recent_ts_str),
                ],
            )
            conn.commit()

        agent_tools.record_reply(
            event_id=ev_done_alr,
            channel_id=channel_id,
            thread_ts=th_alr,
            reply_ts=reply_ts_alr,
            text=reply_text_alr,
            db_path=db_path,
        )

        class DoneErrorReconcileClient:
            def auth_test(self) -> Dict[str, Any]:
                return {"ok": True, "user_id": bot_uid}

            def conversations_replies(self, **kwargs: Any) -> Dict[str, Any]:
                ts = kwargs.get("ts")
                if ts == th_unrec:
                    return {
                        "ok": True,
                        "messages": [
                            {"user": "U_U1", "text": "Q_unrec", "ts": th_unrec},
                            {"user": bot_uid, "text": reply_text_unrec, "ts": reply_ts_unrec, "thread_ts": th_unrec},
                        ],
                        "has_more": False,
                    }
                if ts == th_alr:
                    return {
                        "ok": True,
                        "messages": [
                            {"user": "U_U2", "text": "Q_alr", "ts": th_alr},
                            {"user": bot_uid, "text": reply_text_alr, "ts": reply_ts_alr, "thread_ts": th_alr},
                        ],
                        "has_more": False,
                    }
                if ts in (th_no_reply, th_phase1_no_reply):
                    return {
                        "ok": True,
                        "messages": [
                            {"user": "U_U3", "text": "Q_norep", "ts": ts},
                        ],
                        "has_more": False,
                    }
                if ts == th_scan_err:
                    raise SlackApiError(
                        message="internal_error",
                        response={"ok": False, "error": "internal_error"},
                    )
                raise AssertionError(f"Unexpected thread_ts: {ts}")

        # 1) --dry-run: reports planned done_error_cleared (with_reply / without_reply) and errors without modifying DB
        stdout_dry = io.StringIO()
        stderr_dry = io.StringIO()
        with patch.object(
            agent_tools, "get_web_client", return_value=DoneErrorReconcileClient()
        ), patch.object(
            agent_tools.time, "sleep", side_effect=lambda s: None
        ), redirect_stdout(stdout_dry), redirect_stderr(stderr_dry):
            rc_dry = agent_tools.main(
                [
                    "--db",
                    str(db_path),
                    "reconcile-replies",
                    "--days",
                    "7",
                    "--dry-run",
                ]
            )

        assert rc_dry == 0, f"dry-run failed: rc={rc_dry}, stderr={stderr_dry.getvalue()}"
        out_dry = json.loads(stdout_dry.getvalue())
        assert out_dry["ok"] is True
        assert out_dry["dry_run"] is True
        assert "done_error_unresolved" not in out_dry
        assert out_dry["done_error_cleared"] == {
            "with_reply": [ev_done_unrec, ev_done_alr],
            "without_reply": [ev_done_no_reply, ev_done_phase1_no_reply],
        }
        assert out_dry["errors"] == 1
        assert len(out_dry["error_details"]) == 1
        assert out_dry["error_details"][0]["event_id"] == ev_done_scan_err
        assert get_recorded_reply(ev_done_unrec, db_path=db_path) is None

        with get_db_connection(db_path) as conn:
            errs_after_dry = {
                r["event_id"]: r["error_message"]
                for r in conn.execute("SELECT event_id, error_message FROM events").fetchall()
            }
        assert errs_after_dry[ev_done_unrec] == "IncompleteRead(64 bytes read)"
        assert errs_after_dry[ev_done_alr] == "reply not posted, no replies-table record"
        assert errs_after_dry[ev_done_no_reply] == "pre_post_check_unverifiable"
        assert errs_after_dry[ev_done_phase1_no_reply] == "reply not posted"
        assert errs_after_dry[ev_done_scan_err] == "max attempts exceeded"

        # 2) Real run: records unrecorded reply, clears error_message on both with_reply and without_reply, keeps error on scan failure
        stdout_run = io.StringIO()
        stderr_run = io.StringIO()
        with patch.object(
            agent_tools, "get_web_client", return_value=DoneErrorReconcileClient()
        ), patch.object(
            agent_tools.time, "sleep", side_effect=lambda s: None
        ), redirect_stdout(stdout_run), redirect_stderr(stderr_run):
            rc_run = agent_tools.main(
                [
                    "--db",
                    str(db_path),
                    "reconcile-replies",
                    "--days",
                    "7",
                ]
            )

        assert rc_run == 0, f"reconcile-replies failed: rc={rc_run}, stderr={stderr_run.getvalue()}"
        out_run = json.loads(stdout_run.getvalue())
        assert out_run["ok"] is True
        assert out_run["dry_run"] is False
        assert "done_error_unresolved" not in out_run
        assert out_run["done_error_cleared"] == {
            "with_reply": [ev_done_unrec, ev_done_alr],
            "without_reply": [ev_done_no_reply, ev_done_phase1_no_reply],
        }
        assert out_run["errors"] == 1
        assert len(out_run["error_details"]) == 1
        assert out_run["error_details"][0]["event_id"] == ev_done_scan_err

        # Check replies table for ev_done_unrec (recorded) and without_reply events (not recorded)
        rec_unrec = get_recorded_reply(ev_done_unrec, db_path=db_path)
        assert rec_unrec is not None
        assert rec_unrec["reply_ts"] == reply_ts_unrec
        assert rec_unrec["thread_ts"] == th_unrec
        assert rec_unrec["text_hash"] == expected_hash_unrec
        assert get_recorded_reply(ev_done_no_reply, db_path=db_path) is None
        assert get_recorded_reply(ev_done_phase1_no_reply, db_path=db_path) is None

        # Check events table error_message values: cleared for with_reply & without_reply, preserved for scan error
        with get_db_connection(db_path) as conn:
            errs_after_run = {
                r["event_id"]: r["error_message"]
                for r in conn.execute("SELECT event_id, error_message FROM events").fetchall()
            }
        assert errs_after_run[ev_done_unrec] is None, f"Expected None, got {errs_after_run[ev_done_unrec]}"
        assert errs_after_run[ev_done_alr] is None, f"Expected None, got {errs_after_run[ev_done_alr]}"
        assert errs_after_run[ev_done_no_reply] is None, f"Expected None, got {errs_after_run[ev_done_no_reply]}"
        assert errs_after_run[ev_done_phase1_no_reply] is None, f"Expected None, got {errs_after_run[ev_done_phase1_no_reply]}"
        assert errs_after_run[ev_done_scan_err] == "max attempts exceeded", (
            f"Expected error_message preserved on scan error, got {errs_after_run[ev_done_scan_err]}"
        )

        return "DONE＋error_message残存イベントのスレッド走査で返信あり時はreplies記録＆errorクリア(with_reply)、返信なし時もerrorクリア(without_reply)、走査エラー時はerror維持＆errors計上、--dry-run動作を確認"


def test_dispatcher_processing_guard_and_reaper() -> str:
    """23. (監査指摘1) dispatcher.sh の PROCESSING 競合防止と Reaper: 新しい PROCESSING がある間は新規ディスパッチをスキップして payload.json を保護し、15分(900秒)超の PROCESSING は PENDING に戻して再ディスパッチする"""
    with tempfile.TemporaryDirectory() as tmp:
        state_dir = Path(tmp) / "state"
        state_dir.mkdir(parents=True, exist_ok=True)
        db_path = state_dir / "slack_events.db"
        payload_path = state_dir / "payload.json"
        lock_path = state_dir / "dispatcher.lock"
        mode_file = Path(tmp) / "mode.txt"
        mode_file.write_text("full\n", encoding="utf-8")

        init_db(db_path)

        env = os.environ.copy()
        env.update(
            {
                "YAMA_STATE_DIR": str(state_dir),
                "YAMA_DB_PATH": str(db_path),
                "YAMA_PAYLOAD_PATH": str(payload_path),
                "YAMA_LOCK_PATH": str(lock_path),
                "YAMA_MODE_FILE": str(mode_file),
                "YAMA_PYTHON_BIN": str(PYTHON_BIN),
            }
        )

        # 1) Insert Event 1 and dispatch it -> transitions to PROCESSING, writes payload.json
        handle_event_payload(
            {
                "event_id": "Ev_PROC_1",
                "event": {
                    "type": "message",
                    "channel": "C_BOARD",
                    "user": "U_01",
                    "text": "First batch message",
                    "ts": "1700200001.000000",
                },
            },
            bot_user_id="U_YAMA_BOT",
            db_path=db_path,
        )

        res1 = subprocess.run(
            ["bash", str(DISPATCHER_SH)], env=env, capture_output=True, text=True
        )
        assert res1.returncode == 0 and res1.stdout.strip() == "DISPATCHED:1"
        payload_before = payload_path.read_text(encoding="utf-8")
        assert json.loads(payload_before)["events"][0]["event_id"] == "Ev_PROC_1"

        # 2) Insert Event 2 (PENDING) while Event 1 is still fresh PROCESSING (updated_at is current)
        handle_event_payload(
            {
                "event_id": "Ev_PROC_2",
                "event": {
                    "type": "message",
                    "channel": "C_BOARD",
                    "user": "U_02",
                    "text": "Second batch message while first is still running",
                    "ts": "1700200002.000000",
                },
            },
            bot_user_id="U_YAMA_BOT",
            db_path=db_path,
        )

        # Calling dispatcher now must skip dispatch (LOCKED / exit 2) and NOT overwrite payload.json
        res2 = subprocess.run(
            ["bash", str(DISPATCHER_SH)], env=env, capture_output=True, text=True
        )
        assert res2.returncode == 2 and res2.stdout.strip() == "LOCKED", (
            f"Expected LOCKED (exit 2) while active PROCESSING exists, got rc={res2.returncode}, out={res2.stdout}"
        )
        assert payload_path.read_text(encoding="utf-8") == payload_before, (
            "payload.json must NOT be overwritten while previous batch is still PROCESSING"
        )
        assert count_events(db_path, "PROCESSING") == 1
        assert count_events(db_path, "PENDING") == 1

        # 3) Age Event 1's updated_at beyond 15 minutes (e.g., 16 minutes = 960s ago) -> Reaper must revert it to PENDING
        #    and then dispatch both Ev_PROC_1 and Ev_PROC_2 in chronological order
        stale_ts_str = (datetime.now(timezone.utc) - timedelta(seconds=960)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        with get_db_connection(db_path) as conn:
            conn.execute(
                "UPDATE events SET updated_at = ? WHERE event_id = 'Ev_PROC_1'",
                (stale_ts_str,),
            )
            conn.commit()

        res3 = subprocess.run(
            ["bash", str(DISPATCHER_SH)], env=env, capture_output=True, text=True
        )
        assert res3.returncode == 0 and res3.stdout.strip() == "DISPATCHED:2", (
            f"Expected stale PROCESSING to be reaped and re-dispatched (DISPATCHED:2), got rc={res3.returncode}, out={res3.stdout}"
        )
        payload_after = json.loads(payload_path.read_text(encoding="utf-8"))
        assert [e["event_id"] for e in payload_after["events"]] == ["Ev_PROC_1", "Ev_PROC_2"]
        assert count_events(db_path, "PROCESSING") == 2
        assert count_events(db_path, "PENDING") == 0

        return "updated_atが15分以内のPROCESSING存在時はLOCKED(exit 2)でスキップしpayload.json上書きを防止、15分超のPROCESSINGはPENDINGへreapして再ディスパッチされることを確認"


def test_reconcile_replies_no_false_linking_and_ts_order() -> str:
    """24. (監査指摘2) reconcile_replies の誤紐付け防止と時刻前後チェック: 未割当返信がない場合の yama_msgs[0] 強制割当廃止(without_reply化)・reply_confirmed の恒真条件解消・イベント ts より前の Yama 投稿の誤紐付け防止を確認"""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "slack_events.db"
        init_db(db_path)

        bot_uid = "U_YAMA_BOT"
        channel_id = "C_ALL"
        now_dt = datetime.now(timezone.utc)
        recent_ts_str = (now_dt - timedelta(hours=2)).strftime("%Y-%m-%d %H:%M:%S")

        # Thread 1: Event A (replied & recorded in replies) and Event B (DONE without reply, has residual error_message)
        # Reconcile must NOT assign Event A's reply_ts (yama_msgs[0]) to Event B, and must put Event B into without_reply.
        th_1 = "1794500000.100000"
        ev_a = "Ev_TH1_A_REPLIED"
        ev_b = "Ev_TH1_B_NO_REPLY_ERR"
        reply_ts_a = "1794500010.000100"
        ev_b_ts = "1794500020.000200"

        # Thread 2: Child event (cand["ts"] > thread_ts) where an unrecorded Yama message exists BEFORE cand["ts"].
        # Reconcile (both Phase 1 and Phase 2) must NOT match a Yama message with msg["ts"] <= cand["ts"].
        th_2 = "1794510000.100000"
        older_unrecorded_yama_ts = "1794510005.000100"
        ev_child_p1 = "Ev_TH2_CHILD_PHASE1"
        ev_child_p1_ts = "1794510050.000200"
        ev_child_p2 = "Ev_TH2_CHILD_PHASE2"
        ev_child_p2_ts = "1794510060.000300"

        # Thread 3: Event C has a recorded reply_ts in replies table that does NOT exist in the thread,
        # while another Yama message exists in the thread. reply_confirmed must be False -> without_reply.
        th_3 = "1794520000.100000"
        ev_c = "Ev_TH3_MISSING_REC_TS"
        ghost_rec_ts = "1794520099.999999"
        other_yama_ts = "1794520010.000100"

        agent_tools.init_db(db_path)
        with get_db_connection(db_path) as conn:
            conn.executemany(
                """
                INSERT INTO events (
                    event_id, channel_id, user_id, text, ts, thread_ts, status, error_message, intended_text, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (ev_a, channel_id, "U_1", "Parent A", th_1, None, "DONE", None, None, recent_ts_str, recent_ts_str),
                    (ev_b, channel_id, "U_2", "Followup B (no reply needed)", ev_b_ts, th_1, "DONE", "IncompleteRead(32 bytes read)", "Event Aへの正常な返信", recent_ts_str, recent_ts_str),
                    (ev_child_p1, channel_id, "U_3", "Child P1", ev_child_p1_ts, th_2, "FAILED", "ambiguous_post_outcome", "子イベント発生前の古いYama発言", recent_ts_str, recent_ts_str),
                    (ev_child_p2, channel_id, "U_4", "Child P2", ev_child_p2_ts, th_2, "DONE", "IncompleteRead", "子イベント発生前の古いYama発言", recent_ts_str, recent_ts_str),
                    (ev_c, channel_id, "U_5", "Event C", th_3, None, "DONE", "residual error", None, recent_ts_str, recent_ts_str),
                ],
            )
            conn.commit()

        # Pre-record Event A's reply and Event C's non-existent reply_ts
        agent_tools.record_reply(
            event_id=ev_a,
            channel_id=channel_id,
            thread_ts=th_1,
            reply_ts=reply_ts_a,
            text="Event Aへの正常な返信",
            db_path=db_path,
        )
        agent_tools.record_reply(
            event_id=ev_c,
            channel_id=channel_id,
            thread_ts=th_3,
            reply_ts=ghost_rec_ts,
            text="実在しないreply_tsの記録",
            db_path=db_path,
        )

        class Audit2MockClient:
            def auth_test(self) -> Dict[str, Any]:
                return {"ok": True, "user_id": bot_uid}

            def conversations_replies(self, **kwargs: Any) -> Dict[str, Any]:
                ts = kwargs.get("ts")
                if ts == th_1:
                    return {
                        "ok": True,
                        "messages": [
                            {"user": "U_1", "text": "Parent A", "ts": th_1},
                            {"user": bot_uid, "text": "Event Aへの正常な返信", "ts": reply_ts_a, "thread_ts": th_1},
                            {"user": "U_2", "text": "Followup B", "ts": ev_b_ts, "thread_ts": th_1},
                        ],
                        "has_more": False,
                    }
                if ts == th_2:
                    return {
                        "ok": True,
                        "messages": [
                            {"user": "U_3", "text": "Thread 2 parent", "ts": th_2},
                            {"user": bot_uid, "text": "子イベント発生前の古いYama発言", "ts": older_unrecorded_yama_ts, "thread_ts": th_2},
                            {"user": "U_3", "text": "Child P1", "ts": ev_child_p1_ts, "thread_ts": th_2},
                            {"user": "U_4", "text": "Child P2", "ts": ev_child_p2_ts, "thread_ts": th_2},
                        ],
                        "has_more": False,
                    }
                if ts == th_3:
                    return {
                        "ok": True,
                        "messages": [
                            {"user": "U_5", "text": "Event C", "ts": th_3},
                            {"user": bot_uid, "text": "別イベントのYama返信", "ts": other_yama_ts, "thread_ts": th_3},
                        ],
                        "has_more": False,
                    }
                raise AssertionError(f"Unexpected thread_ts: {ts}")

        res = agent_tools.reconcile_replies(
            days=7,
            dry_run=False,
            db_path=db_path,
            client=Audit2MockClient(),
        )
        assert res["ok"] is True
        assert res["recorded"] == [], f"Expected no false reply recordings, got {res['recorded']}"
        assert res["not_found"] == [ev_child_p1], f"Expected {ev_child_p1} in not_found, got {res['not_found']}"
        assert res["done_error_cleared"]["with_reply"] == [], (
            f"Expected empty with_reply, got {res['done_error_cleared']['with_reply']}"
        )
        assert set(res["done_error_cleared"]["without_reply"]) == {ev_b, ev_child_p2, ev_c}, (
            f"Expected {ev_b}, {ev_child_p2}, {ev_c} in without_reply, got {res['done_error_cleared']['without_reply']}"
        )

        # Confirm Event B and child events did NOT get recorded in replies table
        assert get_recorded_reply(ev_b, db_path=db_path) is None
        assert get_recorded_reply(ev_child_p1, db_path=db_path) is None
        assert get_recorded_reply(ev_child_p2, db_path=db_path) is None

        return "同一スレッドの返信なしDONEイベント(B)へのyama_msgs[0]誤割当阻止(without_reply分類)・イベントtsより前のYama発言の誤紐付け阻止・記録reply_ts不在時のwithout_reply判定を確認"


def test_history_local_fallback_includes_self_replies() -> str:
    """25. (監査指摘3) ローカルフォールバック履歴への自Bot返信マージ: API全試行失敗時の local_db_fallback に replies テーブルの該当チャンネル/スレッドの Yama 返信が時系列マージされることを確認"""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "slack_events.db"
        init_db(db_path)

        ch_id = "C_HIST_MERGE"
        th_1 = "1700300001.000000"
        th_2 = "1700300010.000000"

        # 1) Save human parent message and follow-up in events table (listener filters out Yama's own messages)
        for ev_id, text, ts, thread_ts in [
            ("Ev_HM_1", "親メッセージ（人間）", th_1, None),
            ("Ev_HM_3", "人間の追加質問", "1700300003.000000", th_1),
            ("Ev_HM_OTHER_TH", "別スレッドの親メッセージ", th_2, None),
        ]:
            saved, _ = handle_event_payload(
                {
                    "event_id": ev_id,
                    "event": {
                        "type": "message",
                        "channel": ch_id,
                        "user": "U_HUMAN",
                        "text": text,
                        "ts": ts,
                        **({"thread_ts": thread_ts} if thread_ts else {}),
                    },
                },
                bot_user_id=agent_tools.YAMA_BOT_USER_ID,
                db_path=db_path,
            )
            assert saved is True

        # 2) Record Yama's own reply in replies table for th_1 (ts between Ev_HM_1 and Ev_HM_3) and for th_2
        yama_reply_ts_1 = "1700300002.000000"
        yama_reply_text_1 = "Yamaの1回目のスレッド返信です。"
        agent_tools.record_reply(
            event_id="Ev_HM_1",
            channel_id=ch_id,
            thread_ts=th_1,
            reply_ts=yama_reply_ts_1,
            text=yama_reply_text_1,
            db_path=db_path,
        )
        agent_tools.record_reply(
            event_id="Ev_HM_OTHER_TH",
            channel_id=ch_id,
            thread_ts=th_2,
            reply_ts="1700300011.000000",
            text="別スレッドへのYama返信",
            db_path=db_path,
        )

        class AlwaysFailHistoryClient:
            def conversations_replies(self, **kwargs: Any) -> Dict[str, Any]:
                raise http.client.IncompleteRead(b"offline", 16)

            def conversations_history(self, **kwargs: Any) -> Dict[str, Any]:
                raise http.client.IncompleteRead(b"offline", 16)

        # 3) Thread fallback history for th_1 -> must include Yama's reply in chronological order
        res_th = fetch_history(
            channel=ch_id,
            thread_ts=th_1,
            limit=20,
            db_path=db_path,
            client=AlwaysFailHistoryClient(),
            sleep_fn=lambda s: None,
        )
        assert res_th["ok"] is True
        assert res_th["source"] == "local_db_fallback"
        assert res_th["count"] == 3, f"Expected 3 messages (2 events + 1 Yama reply), got {res_th}"
        assert [m["ts"] for m in res_th["messages"]] == [
            th_1,
            yama_reply_ts_1,
            "1700300003.000000",
        ]
        yama_msg = res_th["messages"][1]
        assert yama_msg["user"] == agent_tools.YAMA_BOT_USER_ID
        assert yama_msg["text"] == yama_reply_text_1
        assert yama_msg["ts"] == yama_reply_ts_1
        assert yama_msg["thread_ts"] == th_1
        assert yama_msg["is_bot"] == 1

        # 4) Channel fallback history for ch_id -> must include all 3 events + 2 Yama replies in chronological order
        res_ch = fetch_history(
            channel=ch_id,
            thread_ts=None,
            limit=20,
            db_path=db_path,
            client=AlwaysFailHistoryClient(),
            sleep_fn=lambda s: None,
        )
        assert res_ch["ok"] is True
        assert res_ch["source"] == "local_db_fallback"
        assert res_ch["count"] == 5
        assert [m["ts"] for m in res_ch["messages"]] == [
            th_1,
            yama_reply_ts_1,
            "1700300003.000000",
            th_2,
            "1700300011.000000",
        ]

        return "API全試行失敗時のlocal_db_fallbackでrepliesテーブルのYama返信(user=U_YAMA_BOT, is_bot=1, text, reply_ts)がスレッド指定・チャンネル指定の双方で時系列マージされることを確認"


def test_dlq_unverifiable_wait_dispatch_and_expiration() -> str:
    """26. (監査指摘4・DLQ改訂) pre_post_check_unverifiable は retry_count を消費せず next_retry_at で待機し、待ち時間中は dispatcher に抽出されず、経過後に再抽出され、最初の失敗から24時間経過で終端(FAILED)する"""
    with tempfile.TemporaryDirectory() as tmp:
        state_dir = Path(tmp) / "state"
        state_dir.mkdir(parents=True, exist_ok=True)
        db_path = state_dir / "slack_events.db"
        payload_path = state_dir / "payload.json"
        lock_path = state_dir / "dispatcher.lock"
        mode_file = Path(tmp) / "mode.txt"
        mode_file.write_text("full\n", encoding="utf-8")

        init_db(db_path)

        env = os.environ.copy()
        env.update(
            {
                "YAMA_STATE_DIR": str(state_dir),
                "YAMA_DB_PATH": str(db_path),
                "YAMA_PAYLOAD_PATH": str(payload_path),
                "YAMA_LOCK_PATH": str(lock_path),
                "YAMA_MODE_FILE": str(mode_file),
                "YAMA_PYTHON_BIN": str(PYTHON_BIN),
            }
        )

        ev_id = "Ev_UNVERIFIABLE_1"
        handle_event_payload(
            {
                "event_id": ev_id,
                "event": {
                    "type": "message",
                    "channel": "C_WELCOME",
                    "user": "U_YANAYANA",
                    "text": "Yanayana implementation share",
                    "ts": "1794494089.354149",
                    "thread_ts": "1794494000.000100",
                },
            },
            bot_user_id="U_YAMA_BOT",
            db_path=db_path,
        )

        # Dispatch initially -> status becomes PROCESSING
        res_init = subprocess.run(
            ["bash", str(DISPATCHER_SH)], env=env, capture_output=True, text=True
        )
        assert res_init.returncode == 0 and res_init.stdout.strip() == "DISPATCHED:1"

        # 1) Verify cmd_reply failing with pre_post_check_unverifiable saves draft to state/reply-<event_id>.txt
        ext_draft = Path(tmp) / "temp_draft.txt"
        draft_content = "Yanayanaさん、共有ありがとうございます。"
        ext_draft.write_text(draft_content + "\n", encoding="utf-8")

        class UnverifiableReplyClient:
            def auth_test(self) -> Dict[str, Any]:
                return {"ok": True, "user_id": agent_tools.YAMA_BOT_USER_ID}

            def conversations_replies(self, **kwargs: Any) -> Dict[str, Any]:
                raise http.client.IncompleteRead(b"drop", 16)

        with patch.object(
            agent_tools, "get_web_client", return_value=UnverifiableReplyClient()
        ), patch.object(
            agent_tools.time, "sleep", side_effect=lambda s: None
        ), redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            rc_rep = agent_tools.main(
                [
                    "--db",
                    str(db_path),
                    "reply",
                    "--channel",
                    "C_WELCOME",
                    "--text-file",
                    str(ext_draft),
                    "--thread-ts",
                    "1794494000.000100",
                    "--event-id",
                    ev_id,
                ]
            )
        assert rc_rep == 1
        expected_state_draft = state_dir / f"reply-{ev_id}.txt"
        assert expected_state_draft.exists(), "Draft file state/reply-<event_id>.txt should be preserved"
        assert expected_state_draft.read_text(encoding="utf-8").strip() == draft_content

        # 2) Commit FAILED with pre_post_check_unverifiable 4 times (> MAX_RETRIES=3):
        #    retry_count must remain 0, status must stay PENDING, first_unverifiable_at fixed at t0,
        #    and next_retry_at set in the future.
        t0 = datetime.now(timezone.utc).replace(microsecond=0)
        for i in range(4):
            cur_now = t0 + timedelta(minutes=10 * i)
            ev_state = commit_event(
                ev_id,
                "FAILED",
                error_message="pre_post_check_unverifiable",
                db_path=db_path,
                now=cur_now,
                retry_wait_seconds=3600,
            )
            assert ev_state["status"] == "PENDING", f"Iteration {i}: expected PENDING, got {ev_state['status']}"
            assert ev_state["retry_count"] == 0, f"Iteration {i}: retry_count must not increment, got {ev_state['retry_count']}"
            assert ev_state["first_unverifiable_at"] == t0.strftime("%Y-%m-%d %H:%M:%S")
            expected_next = (cur_now + timedelta(seconds=3600)).strftime("%Y-%m-%d %H:%M:%S")
            assert ev_state["next_retry_at"] == expected_next

        # 3) While next_retry_at is in the future, dispatcher.sh must NOT extract the event (NO_EVENTS / exit 1)
        res_wait = subprocess.run(
            ["bash", str(DISPATCHER_SH)], env=env, capture_output=True, text=True
        )
        assert res_wait.returncode == 1 and res_wait.stdout.strip() == "NO_EVENTS", (
            f"Expected NO_EVENTS during wait window, got rc={res_wait.returncode}, out={res_wait.stdout}"
        )
        assert count_events(db_path, "PENDING") == 1
        assert count_events(db_path, "PROCESSING") == 0

        # 4) Once wait time has elapsed (next_retry_at <= now_utc), dispatcher.sh extracts the event -> PROCESSING
        past_retry_str = (datetime.now(timezone.utc) - timedelta(seconds=10)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        with get_db_connection(db_path) as conn:
            conn.execute(
                "UPDATE events SET next_retry_at = ? WHERE event_id = ?",
                (past_retry_str, ev_id),
            )
            conn.commit()

        res_ready = subprocess.run(
            ["bash", str(DISPATCHER_SH)], env=env, capture_output=True, text=True
        )
        assert res_ready.returncode == 0 and res_ready.stdout.strip() == "DISPATCHED:1", (
            f"Expected DISPATCHED:1 after wait elapsed, got rc={res_ready.returncode}, out={res_ready.stdout}"
        )
        dispatched_payload = json.loads(payload_path.read_text(encoding="utf-8"))
        assert dispatched_payload["events"][0]["event_id"] == ev_id
        assert dispatched_payload["events"][0]["status"] == "PROCESSING"
        assert dispatched_payload["events"][0]["next_retry_at"] is None

        # 5) At 23h59m after first_unverifiable_at (< 24h), another unverifiable failure still keeps it PENDING
        ev_before_24h = commit_event(
            ev_id,
            "FAILED",
            error_message="pre_post_check_unverifiable",
            db_path=db_path,
            now=t0 + timedelta(hours=23, minutes=59),
            retry_wait_seconds=3600,
        )
        assert ev_before_24h["status"] == "PENDING"
        assert ev_before_24h["retry_count"] == 0
        assert ev_before_24h["next_retry_at"] is not None

        # 6) At >= 24h after first_unverifiable_at (e.g. 24h01m), unverifiable failure terminates as FAILED (abandoned)
        ev_expired = commit_event(
            ev_id,
            "FAILED",
            error_message="pre_post_check_unverifiable",
            db_path=db_path,
            now=t0 + timedelta(hours=24, minutes=1),
            retry_wait_seconds=3600,
        )
        assert ev_expired["status"] == "FAILED", f"Expected terminal FAILED after 24h, got {ev_expired}"
        assert ev_expired["retry_count"] == 0
        assert ev_expired["next_retry_at"] is None
        assert "pre_post_check_unverifiable" in ev_expired["error_message"]
        assert "abandoned after 24h" in ev_expired["error_message"]

        # Confirm terminal FAILED event is never dispatched again
        res_after_exp = subprocess.run(
            ["bash", str(DISPATCHER_SH)], env=env, capture_output=True, text=True
        )
        assert res_after_exp.returncode == 1 and res_after_exp.stdout.strip() == "NO_EVENTS"

        return "pre_post_check_unverifiableでretry_count非消費(4回連続でも0維持)・待ち時間中のdispatcher抽出抑止(NO_EVENTS)・待ち時間経過後の再抽出(DISPATCHED:1)・24時間経過後の終端(FAILED確定)を確認"


def test_reconcile_replies_picks_up_unverifiable() -> str:
    """27. (監査指摘4・DLQ改訂) reconcile_replies の候補抽出が pre_post_check_unverifiable（待ち状態 PENDING および FAILED/DONE）を拾い、着地済み返信を replies 台帳へ回収できることを確認"""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "slack_events.db"
        init_db(db_path)

        bot_uid = "U_YAMA_BOT"
        channel_id = "C_WELCOME"
        now_dt = datetime.now(timezone.utc)
        recent_ts_str = (now_dt - timedelta(hours=2)).strftime("%Y-%m-%d %H:%M:%S")
        future_retry_str = (now_dt + timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")

        # Case 1: PENDING event waiting on next_retry_at with error_message='pre_post_check_unverifiable'
        #         where a Yama reply exists in the thread -> picked up and recorded in replies
        ev_pending_unv = "Ev_REC_PENDING_UNV"
        th_1 = "1794600000.100000"
        reply_ts_1 = "1794600010.000100"
        reply_text_1 = "待ち状態イベントに対する既存のYama返信"

        # Case 2: FAILED (24h abandoned) event with error_message='pre_post_check_unverifiable (abandoned after 24h)'
        #         where a Yama reply exists in the thread -> picked up and recorded in replies
        ev_failed_unv = "Ev_REC_FAILED_UNV"
        th_2 = "1794601000.100000"
        reply_ts_2 = "1794601015.000200"
        reply_text_2 = "終端イベントに対する遅延着地Yama返信"

        # Case 3: PENDING event waiting on next_retry_at with error_message='pre_post_check_unverifiable'
        #         where NO Yama reply exists yet -> picked up as candidate and reported in not_found
        ev_pending_no_reply = "Ev_REC_PENDING_UNV_NONE"
        th_3 = "1794602000.100000"

        agent_tools.init_db(db_path)
        with get_db_connection(db_path) as conn:
            conn.executemany(
                """
                INSERT INTO events (
                    event_id, channel_id, user_id, text, ts, thread_ts,
                    status, retry_count, error_message, next_retry_at, first_unverifiable_at,
                    intended_text, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        ev_pending_unv,
                        channel_id,
                        "U_1",
                        "Q1",
                        th_1,
                        None,
                        "PENDING",
                        0,
                        "pre_post_check_unverifiable",
                        future_retry_str,
                        recent_ts_str,
                        reply_text_1,
                        recent_ts_str,
                        recent_ts_str,
                    ),
                    (
                        ev_failed_unv,
                        channel_id,
                        "U_2",
                        "Q2",
                        th_2,
                        None,
                        "FAILED",
                        0,
                        "pre_post_check_unverifiable (abandoned after 24h)",
                        None,
                        recent_ts_str,
                        reply_text_2,
                        recent_ts_str,
                        recent_ts_str,
                    ),
                    (
                        ev_pending_no_reply,
                        channel_id,
                        "U_3",
                        "Q3",
                        th_3,
                        None,
                        "PENDING",
                        0,
                        "pre_post_check_unverifiable",
                        future_retry_str,
                        recent_ts_str,
                        "未着地の送信予定文面",
                        recent_ts_str,
                        recent_ts_str,
                    ),
                ],
            )
            conn.commit()

        class UnverifiableReconcileClient:
            def auth_test(self) -> Dict[str, Any]:
                return {"ok": True, "user_id": bot_uid}

            def conversations_replies(self, **kwargs: Any) -> Dict[str, Any]:
                ts = kwargs.get("ts")
                if ts == th_1:
                    return {
                        "ok": True,
                        "messages": [
                            {"user": "U_1", "text": "Q1", "ts": th_1},
                            {"user": bot_uid, "text": reply_text_1, "ts": reply_ts_1, "thread_ts": th_1},
                        ],
                        "has_more": False,
                    }
                if ts == th_2:
                    return {
                        "ok": True,
                        "messages": [
                            {"user": "U_2", "text": "Q2", "ts": th_2},
                            {"user": bot_uid, "text": reply_text_2, "ts": reply_ts_2, "thread_ts": th_2},
                        ],
                        "has_more": False,
                    }
                if ts == th_3:
                    return {
                        "ok": True,
                        "messages": [
                            {"user": "U_3", "text": "Q3", "ts": th_3},
                        ],
                        "has_more": False,
                    }
                raise AssertionError(f"Unexpected thread_ts: {ts}")

        res = agent_tools.reconcile_replies(
            days=7,
            dry_run=False,
            db_path=db_path,
            client=UnverifiableReconcileClient(),
        )
        assert res["ok"] is True
        assert res["checked"] == 3, f"Expected 3 unverifiable candidates checked, got {res}"
        assert res["recorded"] == [
            {"event_id": ev_pending_unv, "reply_ts": reply_ts_1},
            {"event_id": ev_failed_unv, "reply_ts": reply_ts_2},
        ]
        assert res["not_found"] == [ev_pending_no_reply]

        rec_1 = get_recorded_reply(ev_pending_unv, db_path=db_path)
        rec_2 = get_recorded_reply(ev_failed_unv, db_path=db_path)
        assert rec_1 is not None and rec_1["reply_ts"] == reply_ts_1
        assert rec_2 is not None and rec_2["reply_ts"] == reply_ts_2
        assert get_recorded_reply(ev_pending_no_reply, db_path=db_path) is None

        return "reconcile_repliesがpre_post_check_unverifiableのイベント(PENDING待ち状態・FAILED終端状態)を候補抽出し、スレッド内のYama返信をreplies台帳へ回収・未返信はnot_found分類することを確認"


def test_pre_post_check_per_page_retry_and_bot_id_reuse() -> str:
    """28. (監査指摘5) 送信前チェックのAPIコール単位リトライとbot_user_id再利用: 複数ページ走査の2・3ページ目で単発IncompleteReadが起きても1ページ目からやり直さず当該ページのみリトライで完走し、bot_user_idも再利用されることを確認"""
    # This test exercises the auth_test path + its cache, so the env override
    # (config/.env has YAMA_BOT_USER_ID since 2026-11-22) must be absent here.
    saved_uid_env = {k: os.environ.pop(k) for k in ("YAMA_BOT_USER_ID", "SLACK_BOT_USER_ID") if k in os.environ}
    try:
        return _test_pre_post_check_per_page_retry_and_bot_id_reuse_body()
    finally:
        os.environ.update(saved_uid_env)


def _test_pre_post_check_per_page_retry_and_bot_id_reuse_body() -> str:
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "slack_events.db"
        init_db(db_path)

        bot_uid = "U_YAMA_BOT"
        channel_id = "C_ALL"
        thread_ts = "1794700000.100000"
        target_reply_ts = "1794700099.000300"
        target_text = "3ページ目に存在するYamaの返信テキスト"

        auth_calls = {"count": 0}
        page_attempts: Dict[str, int] = {"page1": 0, "page2": 0, "page3": 0}
        sleeps: List[float] = []

        class MultiPageFlakyClient:
            def auth_test(self) -> Dict[str, Any]:
                auth_calls["count"] += 1
                return {"ok": True, "user_id": bot_uid}

            def conversations_replies(self, **kwargs: Any) -> Dict[str, Any]:
                cursor = kwargs.get("cursor")
                if not cursor:
                    page_attempts["page1"] += 1
                    return {
                        "ok": True,
                        "messages": [
                            {"user": "U_OTHER", "text": "p1 msg", "ts": thread_ts},
                        ],
                        "has_more": True,
                        "response_metadata": {"next_cursor": "cursor_page_2"},
                    }
                if cursor == "cursor_page_2":
                    page_attempts["page2"] += 1
                    if page_attempts["page2"] == 1:
                        raise http.client.IncompleteRead(b"flaky-page-2", 32)
                    return {
                        "ok": True,
                        "messages": [
                            {"user": "U_OTHER", "text": "p2 msg", "ts": "1794700050.000200"},
                        ],
                        "has_more": True,
                        "response_metadata": {"next_cursor": "cursor_page_3"},
                    }
                if cursor == "cursor_page_3":
                    page_attempts["page3"] += 1
                    if page_attempts["page3"] == 1:
                        raise http.client.IncompleteRead(b"flaky-page-3", 32)
                    return {
                        "ok": True,
                        "messages": [
                            {
                                "user": bot_uid,
                                "text": target_text,
                                "ts": target_reply_ts,
                                "thread_ts": thread_ts,
                            }
                        ],
                        "has_more": False,
                        "response_metadata": {"next_cursor": ""},
                    }
                raise AssertionError(f"Unexpected cursor: {cursor}")

        client = MultiPageFlakyClient()
        found_ts = agent_tools._find_own_recent_message(
            channel=channel_id,
            text=target_text,
            thread_ts=thread_ts,
            client=client,
            sleep_fn=lambda s: sleeps.append(s),
            raise_on_error=True,
        )
        assert found_ts == target_reply_ts, f"Expected {target_reply_ts}, got {found_ts}"
        assert page_attempts == {"page1": 1, "page2": 2, "page3": 2}, (
            f"Expected page1 called once and page2/page3 retried in-place (1, 2, 2), got {page_attempts}"
        )
        assert sleeps == [1.0, 1.0], f"Expected 1.0s sleep for page2 and 1.0s sleep for page3, got {sleeps}"
        assert auth_calls["count"] == 1, f"Expected auth_test called once, got {auth_calls['count']}"

        # Calling again with the same client reuses cached bot_user_id without calling auth_test again
        page_attempts["page1"] = 0
        page_attempts["page2"] = 1  # no failure on 2nd run
        page_attempts["page3"] = 1
        found_ts_2 = agent_tools._find_own_recent_message(
            channel=channel_id,
            text=target_text,
            thread_ts=thread_ts,
            client=client,
            sleep_fn=lambda s: sleeps.append(s),
            raise_on_error=True,
        )
        assert found_ts_2 == target_reply_ts
        assert auth_calls["count"] == 1, f"Expected cached bot_user_id reuse (still 1 auth_test), got {auth_calls['count']}"

        # Passing bot_user_id explicitly or via environment variable avoids calling auth_test at all
        fresh_client = MultiPageFlakyClient()
        page_attempts["page2"] = 1
        page_attempts["page3"] = 1
        found_ts_3 = agent_tools._find_own_recent_message(
            channel=channel_id,
            text=target_text,
            thread_ts=thread_ts,
            client=fresh_client,
            bot_user_id=bot_uid,
            raise_on_error=True,
        )
        assert found_ts_3 == target_reply_ts
        assert auth_calls["count"] == 1, "Explicit bot_user_id should not call auth_test"

        return "3ページ走査の2・3ページ目で単発IncompleteRead発生時に1ページ目へ戻らず当該ページのみリトライ(呼び出し回数1,2,2)で完走・bot_user_idキャッシュ＆引数再利用を確認"


def test_backfill_live_dedup_and_thread_pagination() -> str:
    """29. (監査指摘6) backfill と live(Socket Mode) の双方向重複排除および30件超スレッド返信のページング回収を確認"""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "slack_events.db"
        init_db(db_path)

        bot_uid = "U_YAMA_BOT"
        ch_id = "C_WELCOME"
        base_now = 1794800000.0

        # 1) Path A: backfill saves "{channel}_{ts}" first, then listener receives "Ev..." for same (channel, ts)
        msg_ts_a = "1794799000.000100"
        bf_event_a = backfill.message_to_event(
            {
                "type": "message",
                "user": "U_HUMAN_A",
                "text": "Backfill arrived first",
                "ts": msg_ts_a,
            },
            ch_id,
        )
        inserted_bf_a = save_event(bf_event_a, db_path=db_path)
        assert inserted_bf_a is True
        assert count_events(db_path) == 1

        saved_live_a, reason_live_a = handle_event_payload(
            {
                "event_id": "Ev_LIVE_AFTER_BACKFILL",
                "event": {
                    "type": "message",
                    "channel": ch_id,
                    "user": "U_HUMAN_A",
                    "text": "Backfill arrived first",
                    "ts": msg_ts_a,
                },
            },
            bot_user_id=bot_uid,
            db_path=db_path,
        )
        assert saved_live_a is False and reason_live_a == "duplicate_event_id", (
            f"Expected duplicate rejection when backfill row already exists, got {saved_live_a}, {reason_live_a}"
        )
        assert count_events(db_path) == 1

        # 2) Path B: listener saves "Ev..." first (e.g. after baseline read), then backfill calls save_event with "{channel}_{ts}"
        msg_ts_b = "1794799010.000200"
        saved_live_b, _ = handle_event_payload(
            {
                "event_id": "Ev_LIVE_FIRST_B",
                "event": {
                    "type": "message",
                    "channel": ch_id,
                    "user": "U_HUMAN_B",
                    "text": "Live arrived first",
                    "ts": msg_ts_b,
                },
            },
            bot_user_id=bot_uid,
            db_path=db_path,
        )
        assert saved_live_b is True
        assert count_events(db_path) == 2

        bf_event_b = backfill.message_to_event(
            {
                "type": "message",
                "user": "U_HUMAN_B",
                "text": "Live arrived first",
                "ts": msg_ts_b,
            },
            ch_id,
        )
        inserted_bf_b = save_event(bf_event_b, db_path=db_path)
        assert inserted_bf_b is False, "Backfill save_event must not insert duplicate when Ev... row already exists"
        assert count_events(db_path) == 2

        # 3) Thread with >30 replies (35 replies across 2 pages): verify oldest and cursor pagination recover replies 31..35
        parent_ts = "1794799100.000000"
        replies_calls: List[Dict[str, Any]] = []

        page1_replies = [
            {
                "type": "message",
                "user": "U_PARENT",
                "text": "Thread parent with 35 replies",
                "ts": parent_ts,
                "thread_ts": parent_ts,
            }
        ] + [
            {
                "type": "message",
                "user": "U_REPLIER",
                "text": f"Reply #{i}",
                "ts": f"{1794799100 + i}.000100",
                "thread_ts": parent_ts,
            }
            for i in range(1, 30)
        ]
        page2_replies = [
            {
                "type": "message",
                "user": "U_REPLIER",
                "text": f"Reply #{i}",
                "ts": f"{1794799100 + i}.000100",
                "thread_ts": parent_ts,
            }
            for i in range(30, 36)
        ]

        class PaginatedBackfillClient:
            def auth_test(self) -> Dict[str, Any]:
                return {"ok": True, "user_id": bot_uid}

            def conversations_history(self, channel: str, limit: int = 30, **kwargs: Any) -> Dict[str, Any]:
                if channel == ch_id:
                    return {
                        "ok": True,
                        "messages": [
                            {
                                "type": "message",
                                "user": "U_PARENT",
                                "text": "Thread parent with 35 replies",
                                "ts": parent_ts,
                                "reply_count": 35,
                            }
                        ],
                    }
                return {"ok": True, "messages": []}

            def conversations_replies(self, **kwargs: Any) -> Dict[str, Any]:
                replies_calls.append(kwargs)
                assert kwargs.get("channel") == ch_id
                assert kwargs.get("ts") == parent_ts
                assert "oldest" in kwargs, f"Expected oldest in conversations_replies kwargs: {kwargs}"
                cursor = kwargs.get("cursor")
                if not cursor:
                    return {
                        "ok": True,
                        "messages": page1_replies,
                        "has_more": True,
                        "response_metadata": {"next_cursor": "bf_cursor_page_2"},
                    }
                assert cursor == "bf_cursor_page_2"
                return {
                    "ok": True,
                    "messages": page2_replies,
                    "has_more": False,
                    "response_metadata": {"next_cursor": ""},
                }

        summary = run_backfill(
            db_path=db_path,
            channels=(ch_id,),
            client=PaginatedBackfillClient(),
            now=base_now,
        )
        # 1 parent + 35 replies = 36 newly inserted events
        assert summary["inserted"] == 36, f"Expected 36 inserted (parent + 35 replies), got {summary}"
        assert len(replies_calls) == 2, f"Expected 2 pages of conversations_replies, got {len(replies_calls)}"
        assert replies_calls[0].get("cursor") is None
        assert replies_calls[1].get("cursor") == "bf_cursor_page_2"

        # Verify 35th reply is in DB
        with get_db_connection(db_path) as conn:
            r35 = conn.execute(
                "SELECT text FROM events WHERE channel_id = ? AND ts = ?",
                (ch_id, f"{1794799100 + 35}.000100"),
            ).fetchone()
        assert r35 is not None and r35["text"] == "Reply #35"

        return "backfill先着→live後着およびlive先着→backfill後着の双方向で重複行を防止し、返信35件スレッドでoldest+cursorページングにより31件目以降も回収されることを確認"


def test_hook_env_atomic_replace() -> str:
    """30. (監査指摘7) フック slack-yama-dispatch.sh の .env 更新が同一ディレクトリ内一時ファイル＋chmod 600＋mv アトミック置換になっており、空ファイル競合が起きないことを確認"""
    hook_script = HOOK_SCRIPT_PATH
    assert hook_script.exists(), f"Hook script not found: {hook_script}"
    script_text = hook_script.read_text(encoding="utf-8")

    assert 'cat "$tmp_env" > "$ENV_FILE"' not in script_text, (
        "Non-atomic in-place truncation (cat $tmp_env > $ENV_FILE) must be removed"
    )
    assert 'mktemp "${ENV_FILE}.tmp.XXXXXX"' in script_text, (
        "Temporary file must be created in the same directory as ENV_FILE"
    )
    assert 'mv -f "$tmp_env" "$ENV_FILE"' in script_text, (
        "ENV_FILE must be replaced atomically via mv -f"
    )

    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        mock_env_file = tmp_dir / ".env"
        initial_content = "SLACK_BOT_TOKEN=xoxb-dummy-token-for-test\nSLACK_APP_TOKEN=xapp-dummy-token\nhttp_proxy=http://old-proxy:3128\n"
        mock_env_file.write_text(initial_content, encoding="utf-8")
        mock_env_file.chmod(0o600)
        old_inode = mock_env_file.stat().st_ino

        # Extract and run the exact ENV_FILE block against mock_env_file with new proxy env
        test_sh = tmp_dir / "test_env_sync.sh"
        test_sh.write_text(
            f"""#!/usr/bin/env bash
set -euo pipefail
PROJECT_DIR="{tmp_dir}"
ENV_FILE="${{YAMA_ENV_FILE:-$PROJECT_DIR/config/.env}}"
if [[ -f "$ENV_FILE" ]]; then
  tmp_env=$(mktemp "${{ENV_FILE}}.tmp.XXXXXX")
  chmod 600 "$tmp_env"
  grep -v -E '^(http_proxy|https_proxy|HTTP_PROXY|HTTPS_PROXY|all_proxy|ALL_PROXY|no_proxy|NO_PROXY)=' "$ENV_FILE" > "$tmp_env" || true
  for v in http_proxy https_proxy HTTP_PROXY HTTPS_PROXY all_proxy ALL_PROXY no_proxy NO_PROXY; do
    if [[ -n "${{!v:-}}" ]]; then
      echo "$v=${{!v}}" >> "$tmp_env"
    fi
  done
  if ! cmp -s "$tmp_env" "$ENV_FILE"; then
    chmod 600 "$tmp_env"
    mv -f "$tmp_env" "$ENV_FILE"
  fi
  rm -f "$tmp_env"
fi
""",
            encoding="utf-8",
        )
        test_sh.chmod(0o755)

        env = os.environ.copy()
        env["YAMA_ENV_FILE"] = str(mock_env_file)
        env["http_proxy"] = "http://rotated-proxy:8080"
        env["https_proxy"] = "http://rotated-proxy:8080"

        res = subprocess.run(["bash", str(test_sh)], env=env, capture_output=True, text=True)
        assert res.returncode == 0, f"Atomic env update failed: {res.stderr}"

        new_stat = mock_env_file.stat()
        assert new_stat.st_ino != old_inode, "Expected atomic rename (mv) to replace inode rather than truncating in-place"
        assert (new_stat.st_mode & 0o777) == 0o600, f"Expected mode 0600, got {oct(new_stat.st_mode & 0o777)}"
        updated_text = mock_env_file.read_text(encoding="utf-8")
        assert "SLACK_BOT_TOKEN=xoxb-dummy-token-for-test" in updated_text
        assert "http_proxy=http://rotated-proxy:8080" in updated_text
        assert "http://old-proxy:3128" not in updated_text
        assert list(tmp_dir.glob(".env.tmp.*")) == [], "Temporary files must be cleaned up"

    return "slack-yama-dispatch.shが同一ディレクトリ内一時ファイル＋chmod 600＋mv -fでアトミック置換(inode入替・権限0600維持・一時ファイル残存なし)することを確認"


def test_transient_slack_api_error_retry_and_listener_auth_retry() -> str:
    """31. (監査指摘8) 一時的SlackApiError(429 Retry-After / 5xx / service_unavailable等)のリトライとローカルフォールバック、恒常的エラー即時失敗、およびlistener起動時auth_testリトライを確認"""
    import listener

    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "slack_events.db"
        init_db(db_path)

        ch_id = "C_ERR_CLASS"
        handle_event_payload(
            {
                "event_id": "Ev_ERR_FB_1",
                "event": {
                    "type": "message",
                    "channel": ch_id,
                    "user": "U_HUMAN",
                    "text": "Fallback message on 503",
                    "ts": "1794900001.000100",
                },
            },
            bot_user_id="U_YAMA_BOT",
            db_path=db_path,
        )

        # 1) _call_with_retry: 503 service_unavailable + 429 ratelimited (with Retry-After=3) -> retries and succeeds
        sleeps: List[float] = []
        calls = {"count": 0}

        def flaky_api_call() -> Dict[str, Any]:
            calls["count"] += 1
            if calls["count"] == 1:
                raise SlackApiError(
                    message="service_unavailable",
                    response={"ok": False, "error": "service_unavailable", "status_code": 503},
                )
            if calls["count"] == 2:
                raise SlackApiError(
                    message="ratelimited",
                    response={
                        "ok": False,
                        "error": "ratelimited",
                        "status_code": 429,
                        "headers": {"Retry-After": "3"},
                    },
                )
            return {"ok": True, "value": "recovered"}

        res_retry = agent_tools._call_with_retry(
            flaky_api_call,
            sleep_fn=lambda s: sleeps.append(s),
        )
        assert res_retry == {"ok": True, "value": "recovered"}
        assert calls["count"] == 3
        assert sleeps == [1.0, 3.0], f"Expected 1.0s backoff for 503 and 3.0s Retry-After for 429, got {sleeps}"

        # 2) Permanent SlackApiError (channel_not_found) -> fails immediately without retry
        perm_calls = {"count": 0}
        perm_sleeps: List[float] = []

        def perm_error_call() -> Dict[str, Any]:
            perm_calls["count"] += 1
            raise SlackApiError(
                message="channel_not_found",
                response={"ok": False, "error": "channel_not_found"},
            )

        try:
            agent_tools._call_with_retry(perm_error_call, sleep_fn=lambda s: perm_sleeps.append(s))
            raise AssertionError("Expected SlackApiError for channel_not_found")
        except SlackApiError as exc:
            assert exc.response.get("error") == "channel_not_found"
        assert perm_calls["count"] == 1
        assert perm_sleeps == []

        # 3) fetch_history: persistent transient SlackApiError (503 service_unavailable) across all 5 attempts -> falls back to local DB
        fb_calls = {"count": 0}
        fb_sleeps: List[float] = []

        class Always503Client:
            def conversations_history(self, **kwargs: Any) -> Dict[str, Any]:
                fb_calls["count"] += 1
                raise SlackApiError(
                    message="service_unavailable",
                    response={"ok": False, "error": "service_unavailable", "status_code": 503},
                )

        res_fb = fetch_history(
            channel=ch_id,
            limit=20,
            db_path=db_path,
            client=Always503Client(),
            sleep_fn=lambda s: fb_sleeps.append(s),
        )
        assert fb_calls["count"] == 5
        assert fb_sleeps == [1.0, 2.0, 4.0, 8.0]
        assert res_fb["ok"] is True
        assert res_fb["source"] == "local_db_fallback"
        assert res_fb["count"] == 1
        assert res_fb["messages"][0]["text"] == "Fallback message on 503"

        # 4) listener.authenticate_bot: retries auth_test on IncompleteRead and transient SlackApiError
        auth_calls = {"count": 0}
        auth_sleeps: List[float] = []

        class FlakyAuthClient:
            def auth_test(self) -> Dict[str, Any]:
                auth_calls["count"] += 1
                if auth_calls["count"] == 1:
                    raise http.client.IncompleteRead(b"auth-drop", 16)
                if auth_calls["count"] == 2:
                    raise SlackApiError(
                        message="internal_error",
                        response={"ok": False, "error": "internal_error", "status_code": 500},
                    )
                return {"ok": True, "user_id": "U_YAMA_BOT", "team": "T_MUSE", "user": "yama"}

        auth_resp = listener.authenticate_bot(
            FlakyAuthClient(),
            sleep_fn=lambda s: auth_sleeps.append(s),
        )
        assert auth_resp["user_id"] == "U_YAMA_BOT"
        assert auth_calls["count"] == 3
        assert auth_sleeps == [1.0, 2.0]

        return "503/internal_error/429(Retry-After=3s尊重)のリトライ回復・503連続失敗時のlocal_db_fallback移行・channel_not_found即時失敗・listener起動時auth_testリトライ回復を確認"


def test_batch_d_ignored_error_clear_watchdog_and_busy_timeout() -> str:
    """32. (監査指摘9・10) commit IGNORED時のerror_messageクリア、watchdogデフォルト60秒backoff、フック側ユニット入れ替え時の再起動要否ログ記録、busy_timeout統一、未使用定数削除を確認"""
    import listener

    assert not hasattr(agent_tools, "RECENT_MESSAGE_WINDOW_SECONDS"), (
        "Unused constant RECENT_MESSAGE_WINDOW_SECONDS should be removed from agent_tools"
    )

    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        db_path = tmp_dir / "slack_events.db"
        init_db(db_path)

        # 1) Verify unified busy_timeout (30000ms = 30s) in listener.get_db_connection() and dispatcher.sh
        with get_db_connection(db_path) as conn:
            bt_row = conn.execute("PRAGMA busy_timeout;").fetchone()
            assert int(bt_row[0]) == 30000, f"Expected PRAGMA busy_timeout=30000, got {bt_row[0]}"
        assert listener.DB_BUSY_TIMEOUT_SEC == 30.0 and listener.DB_BUSY_TIMEOUT_MS == 30000
        disp_text = DISPATCHER_SH.read_text(encoding="utf-8")
        assert "DB_BUSY_TIMEOUT_SEC = 30.0" in disp_text and "PRAGMA busy_timeout={DB_BUSY_TIMEOUT_MS};" in disp_text

        # 2) commit_event with IGNORED clears residual error_message (both Python API and CLI)
        handle_event_payload(
            {
                "event_id": "Ev_IGN_CLEAR_1",
                "event": {
                    "type": "message",
                    "channel": "C_IGN",
                    "user": "U_HUMAN",
                    "text": "Retry then ignore",
                    "ts": "1795000001.000100",
                },
            },
            bot_user_id="U_YAMA_BOT",
            db_path=db_path,
        )
        # 1st attempt fails -> status='PENDING', retry_count=1, error_message='transient failure'
        fail_ev = commit_event(
            "Ev_IGN_CLEAR_1",
            "FAILED",
            error_message="transient failure on 1st attempt",
            db_path=db_path,
        )
        assert fail_ev["status"] == "PENDING"
        assert fail_ev["retry_count"] == 1
        assert fail_ev["error_message"] == "transient failure on 1st attempt"

        # 2nd attempt decides IGNORED without error_message -> error_message must be cleared to None
        ign_ev = commit_event("Ev_IGN_CLEAR_1", "IGNORED", db_path=db_path)
        assert ign_ev["status"] == "IGNORED"
        assert ign_ev["retry_count"] == 1
        assert ign_ev["error_message"] is None, (
            f"Expected error_message=None on IGNORED commit, got {ign_ev['error_message']!r}"
        )
        assert ign_ev["next_retry_at"] is None
        assert ign_ev["first_unverifiable_at"] is None

        # Also test via CLI subcommand `commit --status IGNORED` after an unverifiable wait error
        handle_event_payload(
            {
                "event_id": "Ev_IGN_CLEAR_CLI",
                "event": {
                    "type": "message",
                    "channel": "C_IGN",
                    "user": "U_HUMAN",
                    "text": "CLI ignore after unverifiable",
                    "ts": "1795000002.000100",
                },
            },
            bot_user_id="U_YAMA_BOT",
            db_path=db_path,
        )
        commit_event(
            "Ev_IGN_CLEAR_CLI",
            "FAILED",
            error_message="pre_post_check_unverifiable",
            db_path=db_path,
        )
        res_cli = subprocess.run(
            [
                str(PYTHON_BIN),
                str(AGENT_TOOLS_PY),
                "--db",
                str(db_path),
                "commit",
                "--event-id",
                "Ev_IGN_CLEAR_CLI",
                "--status",
                "IGNORED",
            ],
            capture_output=True,
            text=True,
        )
        assert res_cli.returncode == 0, f"CLI commit IGNORED failed: {res_cli.stderr}"
        cli_ev = json.loads(res_cli.stdout)["event"]
        assert cli_ev["status"] == "IGNORED"
        assert cli_ev["error_message"] is None
        assert cli_ev["next_retry_at"] is None
        assert cli_ev["first_unverifiable_at"] is None

        # 3) watchdog.sh default REINSTALL_BACKOFF_BASE_SEC is 60s when YAMA_UNIT_REINSTALL_BACKOFF_SEC is unset
        wd_state_dir = tmp_dir / "wd_state"
        wd_state_dir.mkdir(parents=True, exist_ok=True)
        hb_path = wd_state_dir / "heartbeat.json"
        wd_log = wd_state_dir / "watchdog.log"
        reinstall_state = wd_state_dir / "unit_reinstall_state.json"
        hb_path.write_text(json.dumps({"timestamp": time.time() - 300, "connected": False}), encoding="utf-8")

        src_unit = tmp_dir / "yama-listener.service.src"
        src_unit.write_text("[Unit]\nDescription=New Unit\n", encoding="utf-8")
        dest_dir = tmp_dir / "systemd_dir"
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest_unit = dest_dir / "yama-listener.service"

        mock_restart_nf = tmp_dir / "mock_restart_nf.sh"
        mock_restart_nf.write_text(
            "#!/usr/bin/env bash\necho 'Unit yama-listener.service not found.' >&2\nexit 5\n",
            encoding="utf-8",
        )
        mock_restart_nf.chmod(0o755)
        mock_install_fail = tmp_dir / "mock_install_fail.sh"
        mock_install_fail.write_text(
            "#!/usr/bin/env bash\necho 'permission denied' >&2\nexit 1\n",
            encoding="utf-8",
        )
        mock_install_fail.chmod(0o755)

        wd_env = os.environ.copy()
        wd_env.pop("YAMA_UNIT_REINSTALL_BACKOFF_SEC", None)
        wd_env.update(
            {
                "YAMA_STATE_DIR": str(wd_state_dir),
                "YAMA_HEARTBEAT_PATH": str(hb_path),
                "YAMA_WATCHDOG_LOG": str(wd_log),
                "YAMA_PYTHON_BIN": str(PYTHON_BIN),
                "YAMA_RESTART_CMD": str(mock_restart_nf),
                "YAMA_UNIT_SOURCE": str(src_unit),
                "YAMA_UNIT_DEST": str(dest_unit),
                "YAMA_UNIT_INSTALL_CMD": str(mock_install_fail),
                "YAMA_UNIT_REINSTALL_STATE": str(reinstall_state),
            }
        )
        res_wd1 = subprocess.run(["bash", str(WATCHDOG_SH)], env=wd_env, capture_output=True, text=True)
        assert res_wd1.returncode == 1
        st_data = json.loads(reinstall_state.read_text(encoding="utf-8"))
        assert st_data["attempts"] == 1
        assert float(st_data["backoff_sec"]) == 60.0, (
            f"Expected default initial backoff_sec=60.0, got {st_data['backoff_sec']}"
        )

        # Simulate 30s later (next hook run): still within 60s backoff window -> skipped
        st_data["last_attempt_ts"] = time.time() - 30.0
        st_data["next_allowed_ts"] = time.time() + 30.0
        reinstall_state.write_text(json.dumps(st_data), encoding="utf-8")
        res_wd2 = subprocess.run(["bash", str(WATCHDOG_SH)], env=wd_env, capture_output=True, text=True)
        assert res_wd2.returncode == 5
        assert "Unit reinstall skipped: backoff active" in wd_log.read_text(encoding="utf-8")

        # 4) Hook self-heal logs restart-required warning when replacing an existing unit file
        hook_script = HOOK_SCRIPT_PATH
        hook_text = hook_script.read_text(encoding="utf-8")
        assert "service restart required" in hook_text

        hook_unit_dest = dest_dir / "hook-yama-listener.service"
        hook_unit_dest.write_text("[Unit]\nDescription=Old Unit\n", encoding="utf-8")
        hook_wd_log = wd_state_dir / "hook_watchdog.log"
        mock_systemctl = tmp_dir / "mock_systemctl.sh"
        mock_systemctl.write_text(
            "#!/usr/bin/env bash\nexit 0\n",
            encoding="utf-8",
        )
        mock_systemctl.chmod(0o755)

        self_heal_test_sh = tmp_dir / "test_hook_self_heal.sh"
        self_heal_test_sh.write_text(
            f"""#!/usr/bin/env bash
set -euo pipefail
PROJECT_DIR="{tmp_dir}"
UNIT_SOURCE="{src_unit}"
UNIT_DEST="{hook_unit_dest}"
WATCHDOG_LOG="{hook_wd_log}"
SYSTEMCTL_BIN="{mock_systemctl}"
if [[ -f "$UNIT_SOURCE" ]] && ! cmp -s "$UNIT_SOURCE" "$UNIT_DEST"; then
  unit_existed=0
  if [[ -f "$UNIT_DEST" ]]; then
    unit_existed=1
  fi
  service_was_active=0
  if "$SYSTEMCTL_BIN" is-active --quiet yama-listener.service 2>/dev/null; then
    service_was_active=1
  fi
  if cp "$UNIT_SOURCE" "$UNIT_DEST" 2>/dev/null; then
    "$SYSTEMCTL_BIN" daemon-reload 2>/dev/null || true
    "$SYSTEMCTL_BIN" enable --now yama-listener.service >/dev/null 2>&1 || true
    if [[ "$unit_existed" -eq 1 || "$service_was_active" -eq 1 ]]; then
      mkdir -p "$(dirname "$WATCHDOG_LOG")" 2>/dev/null || true
      echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") [WARN] Hook self-heal replaced unit file (${{UNIT_SOURCE}} -> ${{UNIT_DEST}}); service restart required (systemctl restart yama-listener.service) to apply updated unit definition" >> "$WATCHDOG_LOG" 2>/dev/null || true
    fi
  fi
fi
""",
            encoding="utf-8",
        )
        self_heal_test_sh.chmod(0o755)
        res_sh = subprocess.run(["bash", str(self_heal_test_sh)], capture_output=True, text=True)
        assert res_sh.returncode == 0
        assert hook_unit_dest.read_text(encoding="utf-8") == src_unit.read_text(encoding="utf-8")
        assert "service restart required" in hook_wd_log.read_text(encoding="utf-8")

        return "commit IGNORED時のerror_messageクリア(API/CLI)・watchdogデフォルト初回60秒backoff(30秒後スキップ)・フック側ユニット更新時の再起動要否ログ記録・busy_timeout=30000ms統一・未使用定数削除を確認"


def test_delayed_retry_mark_prefix() -> str:
    """33. 遅延再送の印付加: 遅延再送(first_unverifiable_at記録あり)で新形式ヘッダーが1行だけ前置される／通常返信には付かない／既に新形式・旧形式の印がある場合は二重付加しない"""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "slack_events.db"
        init_db(db_path)

        bot_uid = "U_YAMA_BOT"
        channel_id = "C_WELCOME"
        thread_ts = "1795100000.000100"
        questioner_uid = "U_ASKER_1"

        # 2026-11-13 23:05:00 UTC == 2026-11-14 08:05 Asia/Tokyo
        first_unv_utc = "2026-11-13 23:05:00"
        expected_mark = "遅れての再送（元の失敗時刻：2026-11-14 08:05 JST）"
        legacy_mark = "（遅れての再送：最初の試行 11-14 08:05）"
        body_text = "ご質問の件、設定ファイルのタイムアウト値を見直すことで解決します。"

        ev_delayed = "Ev_DELAYED_MARK_1"
        ev_normal = "Ev_NORMAL_NO_MARK_1"
        ev_already_marked = "Ev_DELAYED_ALREADY_MARKED_1"
        ev_legacy_marked = "Ev_DELAYED_LEGACY_MARKED_1"

        with get_db_connection(db_path) as conn:
            conn.executemany(
                """
                INSERT INTO events (
                    event_id, channel_id, user_id, text, ts, thread_ts,
                    status, retry_count, error_message, next_retry_at, first_unverifiable_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        ev_delayed,
                        channel_id,
                        questioner_uid,
                        "質問1",
                        thread_ts,
                        None,
                        "PROCESSING",
                        0,
                        "pre_post_check_unverifiable",
                        None,
                        first_unv_utc,
                    ),
                    (
                        ev_normal,
                        channel_id,
                        questioner_uid,
                        "通常質問",
                        "1795100100.000100",
                        None,
                        "PROCESSING",
                        0,
                        None,
                        None,
                        None,
                    ),
                    (
                        ev_already_marked,
                        channel_id,
                        questioner_uid,
                        "質問2",
                        "1795100200.000100",
                        None,
                        "PROCESSING",
                        0,
                        "pre_post_check_unverifiable",
                        None,
                        first_unv_utc,
                    ),
                    (
                        ev_legacy_marked,
                        channel_id,
                        questioner_uid,
                        "質問3",
                        "1795100300.000100",
                        None,
                        "PROCESSING",
                        0,
                        "pre_post_check_unverifiable",
                        None,
                        first_unv_utc,
                    ),
                ],
            )
            conn.commit()

        post_calls: List[Dict[str, Any]] = []

        class MarkTestClient:
            def auth_test(self) -> Dict[str, Any]:
                return {"ok": True, "user_id": bot_uid}

            def conversations_replies(self, **kwargs: Any) -> Dict[str, Any]:
                ts = kwargs.get("ts")
                return {
                    "ok": True,
                    "messages": [
                        {"user": questioner_uid, "text": "親投稿", "ts": ts},
                    ],
                    "has_more": False,
                }

            def chat_postMessage(self, **kwargs: Any) -> Dict[str, Any]:
                post_calls.append(kwargs)
                posted_ts = f"1795100900.00000{len(post_calls)}"
                return {
                    "ok": True,
                    "channel": kwargs["channel"],
                    "ts": posted_ts,
                    "message": {"text": kwargs["text"], "ts": posted_ts},
                }

        # 1) Delayed retry -> prepends exactly 1 line of expected_mark (new unified format)
        text_file_1 = Path(tmp) / "reply_delayed.txt"
        text_file_1.write_text(body_text + "\n", encoding="utf-8")

        stdout1 = io.StringIO()
        stderr1 = io.StringIO()
        with patch.object(
            agent_tools, "get_web_client", return_value=MarkTestClient()
        ), redirect_stdout(stdout1), redirect_stderr(stderr1):
            rc1 = agent_tools.main(
                [
                    "--db",
                    str(db_path),
                    "reply",
                    "--channel",
                    channel_id,
                    "--text-file",
                    str(text_file_1),
                    "--thread-ts",
                    thread_ts,
                    "--event-id",
                    ev_delayed,
                ]
            )
        assert rc1 == 0, f"Delayed retry reply failed: rc={rc1}, stderr={stderr1.getvalue()}"
        assert len(post_calls) == 1
        posted_text_1 = post_calls[0]["text"]
        assert posted_text_1 == f"{expected_mark}\n{body_text}", (
            f"Expected 1-line retry mark prefix, got: {posted_text_1!r}"
        )
        assert posted_text_1.count("遅れての再送") == 1
        out1 = json.loads(stdout1.getvalue())
        assert out1["ok"] is True
        assert out1["delayed_retry"] is True
        assert out1["retry_mark_added"] is True
        assert out1["retry_mark"] == expected_mark

        # 2) Normal reply (first_unverifiable_at is NULL) -> no mark added
        stdout2 = io.StringIO()
        stderr2 = io.StringIO()
        with patch.object(
            agent_tools, "get_web_client", return_value=MarkTestClient()
        ), redirect_stdout(stdout2), redirect_stderr(stderr2):
            rc2 = agent_tools.main(
                [
                    "--db",
                    str(db_path),
                    "reply",
                    "--channel",
                    channel_id,
                    "--text-file",
                    str(text_file_1),
                    "--thread-ts",
                    "1795100100.000100",
                    "--event-id",
                    ev_normal,
                ]
            )
        assert rc2 == 0
        assert len(post_calls) == 2
        posted_text_2 = post_calls[1]["text"]
        assert posted_text_2 == body_text, f"Normal reply must not have retry mark, got: {posted_text_2!r}"
        assert "遅れての再送" not in posted_text_2
        out2 = json.loads(stdout2.getvalue())
        assert out2["ok"] is True
        assert out2["delayed_retry"] is False
        assert out2["retry_mark_added"] is False
        assert "retry_mark" not in out2

        # 3) Delayed retry when input text ALREADY starts with the new mark -> does NOT double-prepend
        text_file_3 = Path(tmp) / "reply_already_marked.txt"
        text_file_3.write_text(f"{expected_mark}\n{body_text}\n", encoding="utf-8")

        stdout3 = io.StringIO()
        stderr3 = io.StringIO()
        with patch.object(
            agent_tools, "get_web_client", return_value=MarkTestClient()
        ), redirect_stdout(stdout3), redirect_stderr(stderr3):
            rc3 = agent_tools.main(
                [
                    "--db",
                    str(db_path),
                    "reply",
                    "--channel",
                    channel_id,
                    "--text-file",
                    str(text_file_3),
                    "--thread-ts",
                    "1795100200.000100",
                    "--event-id",
                    ev_already_marked,
                ]
            )
        assert rc3 == 0
        assert len(post_calls) == 3
        posted_text_3 = post_calls[2]["text"]
        assert posted_text_3 == f"{expected_mark}\n{body_text}", (
            f"Mark must not be duplicated when new format already present, got: {posted_text_3!r}"
        )
        assert posted_text_3.count("遅れての再送") == 1
        out3 = json.loads(stdout3.getvalue())
        assert out3["ok"] is True
        assert out3["delayed_retry"] is True
        assert out3["retry_mark_added"] is False

        # 4) Delayed retry when input text ALREADY starts with the legacy mark -> does NOT prepend new mark
        text_file_4 = Path(tmp) / "reply_legacy_marked.txt"
        text_file_4.write_text(f"{legacy_mark}\n{body_text}\n", encoding="utf-8")

        stdout4 = io.StringIO()
        stderr4 = io.StringIO()
        with patch.object(
            agent_tools, "get_web_client", return_value=MarkTestClient()
        ), redirect_stdout(stdout4), redirect_stderr(stderr4):
            rc4 = agent_tools.main(
                [
                    "--db",
                    str(db_path),
                    "reply",
                    "--channel",
                    channel_id,
                    "--text-file",
                    str(text_file_4),
                    "--thread-ts",
                    "1795100300.000100",
                    "--event-id",
                    ev_legacy_marked,
                ]
            )
        assert rc4 == 0
        assert len(post_calls) == 4
        posted_text_4 = post_calls[3]["text"]
        assert posted_text_4 == f"{legacy_mark}\n{body_text}", (
            f"Mark must not be duplicated when legacy format already present, got: {posted_text_4!r}"
        )
        assert posted_text_4.count("遅れての再送") == 1
        out4 = json.loads(stdout4.getvalue())
        assert out4["ok"] is True
        assert out4["delayed_retry"] is True
        assert out4["retry_mark_added"] is False

        return "遅延再送でAsia/Tokyo整形の新形式ヘッダー1行前置(retry_mark_added=true)・通常返信は印なし(retry_mark_added=false)・新旧両形式の既存ヘッダーあり時は二重付加防止を確認"


def test_delayed_retry_skipped_answered() -> str:
    """34. 答え済み判定（送信抑止）: 遅延再送でスレッドに条件を満たす他者の投稿（質問者・Yama以外、非ボット、10文字超）が親より後にあれば送信せず skipped_answered を返し、判定tsを含めDONEへ遷移する"""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "slack_events.db"
        init_db(db_path)

        bot_uid = "U_YAMA_BOT"
        channel_id = "C_ALL"
        thread_ts = "1795200000.000100"
        questioner_uid = "U_QUESTIONER"
        answerer_uid = "U_OTHER_HUMAN"
        answer_ts = "1795200045.000200"
        answer_text = "そのエラーは config/.env の権限設定を確認すると直りますよ。"  # > 10 chars

        ev_id = "Ev_DELAYED_ANSWERED_1"
        with get_db_connection(db_path) as conn:
            conn.execute(
                """
                INSERT INTO events (
                    event_id, channel_id, user_id, text, ts, thread_ts,
                    status, retry_count, error_message, next_retry_at, first_unverifiable_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ev_id,
                    channel_id,
                    questioner_uid,
                    "エラーの原因が分かりません",
                    thread_ts,
                    None,
                    "PROCESSING",
                    0,
                    "pre_post_check_unverifiable",
                    None,
                    "2026-11-13 22:00:00",
                ),
            )
            conn.commit()

        text_file = Path(tmp) / "reply_to_skip.txt"
        text_file.write_text("Yamaからの遅延回答です。\n", encoding="utf-8")

        post_calls: List[Dict[str, Any]] = []

        class AnsweredThreadClient:
            def auth_test(self) -> Dict[str, Any]:
                return {"ok": True, "user_id": bot_uid}

            def conversations_replies(self, **kwargs: Any) -> Dict[str, Any]:
                return {
                    "ok": True,
                    "messages": [
                        {"user": questioner_uid, "text": "エラーの原因が分かりません", "ts": thread_ts},
                        {
                            "user": answerer_uid,
                            "text": answer_text,
                            "ts": answer_ts,
                            "thread_ts": thread_ts,
                        },
                    ],
                    "has_more": False,
                }

            def chat_postMessage(self, **kwargs: Any) -> Dict[str, Any]:
                post_calls.append(kwargs)
                raise AssertionError("chat_postMessage must NOT be called when thread is already answered")

        stdout_buf = io.StringIO()
        stderr_buf = io.StringIO()
        with patch.object(
            agent_tools, "get_web_client", return_value=AnsweredThreadClient()
        ), redirect_stdout(stdout_buf), redirect_stderr(stderr_buf):
            rc = agent_tools.main(
                [
                    "--db",
                    str(db_path),
                    "reply",
                    "--channel",
                    channel_id,
                    "--text-file",
                    str(text_file),
                    "--thread-ts",
                    thread_ts,
                    "--event-id",
                    ev_id,
                ]
            )

        assert rc == 0, f"Expected rc=0 on skipped_answered, got {rc}, stderr={stderr_buf.getvalue()}"
        assert len(post_calls) == 0, "Must not post when thread is already answered"

        out = json.loads(stdout_buf.getvalue())
        assert out["ok"] is True
        assert out["result"] == "skipped_answered"
        assert out["skipped"] is True
        assert out["answered_by_ts"] == answer_ts
        assert out["answered_ts"] == answer_ts
        assert out["answered_by_user"] == answerer_uid
        assert out["delayed_retry"] is True
        assert out["retry_mark_added"] is False
        assert out["status"] == "DONE"

        # Event in DB is transitioned to DONE with wait/error fields cleared, and no replies row recorded
        ev_after = agent_tools.get_event_by_id(ev_id, db_path=db_path)
        assert ev_after is not None
        assert ev_after["status"] == "DONE"
        assert ev_after["error_message"] is None
        assert ev_after["next_retry_at"] is None
        assert ev_after["first_unverifiable_at"] is None
        assert get_recorded_reply(ev_id, db_path=db_path) is None

        # Subsequent explicit `commit --status DONE` is also idempotent and keeps status=DONE
        ev_committed = commit_event(ev_id, "DONE", db_path=db_path)
        assert ev_committed["status"] == "DONE"
        assert ev_committed["error_message"] is None

        return "条件を満たす他者投稿(10文字超・非Bot・質問者/Yama以外)がある遅延再送で送信抑止(result=skipped_answered)・判定ts出力・イベントDONE遷移を確認"


def test_delayed_retry_not_answered_conservative_cases() -> str:
    """35. 答え済みにならない保守判定ケース: ボットの投稿のみ／短文（10文字以下）のみ／質問者本人の追記のみ／Yama自身の投稿のみの場合は答え済みと判定せず印を付けて通常送信する"""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "slack_events.db"
        init_db(db_path)

        bot_uid = "U_YAMA_BOT"
        channel_id = "C_ALL"
        questioner_uid = "U_QUESTIONER"
        other_human_uid = "U_OTHER_HUMAN"
        first_unv_utc = "2026-11-13 23:30:00"  # -> 2026-11-14 08:30 JST
        expected_mark = "遅れての再送（元の失敗時刻：2026-11-14 08:30 JST）"
        reply_body = "Yamaからの遅延回答本文です。"

        text_file = Path(tmp) / "reply_conservative.txt"
        text_file.write_text(reply_body + "\n", encoding="utf-8")

        cases = [
            (
                "bot_only",
                "Ev_DELAYED_BOT_ONLY",
                "1795300100.000100",
                [
                    {"user": questioner_uid, "text": "親の質問投稿です", "ts": "1795300100.000100"},
                    {
                        "user": "U_NEIRO_BOT",
                        "bot_id": "B_NEIRO",
                        "text": "これはボットによる10文字を超える長い自動応答メッセージです。",
                        "ts": "1795300110.000200",
                        "thread_ts": "1795300100.000100",
                    },
                    {
                        "user": "U_SUBTYPE_BOT",
                        "subtype": "bot_message",
                        "text": "こちらもsubtype=bot_messageによる10文字超の投稿です。",
                        "ts": "1795300120.000300",
                        "thread_ts": "1795300100.000100",
                    },
                    {
                        "user": "U_IS_BOT_FLAG",
                        "is_bot": 1,
                        "text": "こちらもis_bot=1フラグ付きの10文字を超える投稿です。",
                        "ts": "1795300130.000400",
                        "thread_ts": "1795300100.000100",
                    },
                ],
            ),
            (
                "short_text_only",
                "Ev_DELAYED_SHORT_ONLY",
                "1795300200.000100",
                [
                    {"user": questioner_uid, "text": "親の質問投稿です", "ts": "1795300200.000100"},
                    {
                        "user": other_human_uid,
                        "text": "  1234567890  ",  # stripped length == 10 (<= 10)
                        "ts": "1795300210.000200",
                        "thread_ts": "1795300200.000100",
                    },
                    {
                        "user": other_human_uid,
                        "text": "たしかに！",  # stripped length == 5 (<= 10)
                        "ts": "1795300220.000300",
                        "thread_ts": "1795300200.000100",
                    },
                ],
            ),
            (
                "questioner_self_followup_only",
                "Ev_DELAYED_SELF_ONLY",
                "1795300300.000100",
                [
                    {"user": questioner_uid, "text": "親の質問投稿です", "ts": "1795300300.000100"},
                    {
                        "user": questioner_uid,
                        "text": "質問者本人による追記です。エラーログの詳細も貼っておきます。",
                        "ts": "1795300310.000200",
                        "thread_ts": "1795300300.000100",
                    },
                ],
            ),
            (
                "yama_own_post_only",
                "Ev_DELAYED_YAMA_ONLY",
                "1795300400.000100",
                [
                    {"user": questioner_uid, "text": "親の質問投稿です", "ts": "1795300400.000100"},
                    {
                        "user": bot_uid,
                        "text": "Yama自身の別文面による先行投稿（10文字を超えるテキスト）です。",
                        "ts": "1795300410.000200",
                        "thread_ts": "1795300400.000100",
                    },
                ],
            ),
        ]

        with get_db_connection(db_path) as conn:
            for _, ev_id, th_ts, _ in cases:
                conn.execute(
                    """
                    INSERT INTO events (
                        event_id, channel_id, user_id, text, ts, thread_ts,
                        status, retry_count, error_message, next_retry_at, first_unverifiable_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        ev_id,
                        channel_id,
                        questioner_uid,
                        "親の質問投稿です",
                        th_ts,
                        None,
                        "PROCESSING",
                        0,
                        "pre_post_check_unverifiable",
                        None,
                        first_unv_utc,
                    ),
                )
            conn.commit()

        threads_map = {th_ts: msgs for _, _, th_ts, msgs in cases}
        post_calls: List[Dict[str, Any]] = []

        class ConservativeCasesClient:
            def auth_test(self) -> Dict[str, Any]:
                return {"ok": True, "user_id": bot_uid}

            def conversations_replies(self, **kwargs: Any) -> Dict[str, Any]:
                ts = kwargs.get("ts")
                return {
                    "ok": True,
                    "messages": threads_map[ts],
                    "has_more": False,
                }

            def chat_postMessage(self, **kwargs: Any) -> Dict[str, Any]:
                post_calls.append(kwargs)
                posted_ts = f"1795300900.00000{len(post_calls)}"
                return {
                    "ok": True,
                    "channel": kwargs["channel"],
                    "ts": posted_ts,
                    "message": {"text": kwargs["text"], "ts": posted_ts},
                }

        for idx, (label, ev_id, th_ts, _) in enumerate(cases, start=1):
            stdout_buf = io.StringIO()
            stderr_buf = io.StringIO()
            with patch.object(
                agent_tools, "get_web_client", return_value=ConservativeCasesClient()
            ), redirect_stdout(stdout_buf), redirect_stderr(stderr_buf):
                rc = agent_tools.main(
                    [
                        "--db",
                        str(db_path),
                        "reply",
                        "--channel",
                        channel_id,
                        "--text-file",
                        str(text_file),
                        "--thread-ts",
                        th_ts,
                        "--event-id",
                        ev_id,
                    ]
                )
            assert rc == 0, f"Case {label} failed: rc={rc}, stderr={stderr_buf.getvalue()}"
            assert len(post_calls) == idx, f"Case {label} should have posted, got {len(post_calls)} posts"
            assert post_calls[-1]["text"] == f"{expected_mark}\n{reply_body}"
            out = json.loads(stdout_buf.getvalue())
            assert out["ok"] is True
            assert out["result"] == "posted", f"Case {label} expected result='posted', got {out}"
            assert out["delayed_retry"] is True
            assert out["retry_mark_added"] is True

        return "ボット投稿のみ(bot_id/bot_message/is_bot)・短文(空白除去後10文字以下)のみ・質問者本人の追記のみ・Yama自身の投稿のみの全4ケースで誤沈黙せず印付き送信されることを確認"


def test_delayed_retry_history_failure_unverifiable() -> str:
    """36. 遅延再送の履歴取得失敗時: 新しい失敗モードを作らず従来どおり fail-closed（pre_post_check_unverifiable）経路になることを確認"""
    with tempfile.TemporaryDirectory() as tmp:
        state_dir = Path(tmp) / "state"
        state_dir.mkdir(parents=True, exist_ok=True)
        db_path = state_dir / "slack_events.db"
        init_db(db_path)

        bot_uid = "U_YAMA_BOT"
        channel_id = "C_ALL"
        thread_ts = "1795400000.000100"
        ev_id = "Ev_DELAYED_HIST_FAIL_1"
        first_unv_utc = "2026-11-13 23:00:00"

        with get_db_connection(db_path) as conn:
            conn.execute(
                """
                INSERT INTO events (
                    event_id, channel_id, user_id, text, ts, thread_ts,
                    status, retry_count, error_message, next_retry_at, first_unverifiable_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ev_id,
                    channel_id,
                    "U_ASKER",
                    "履歴失敗テスト質問",
                    thread_ts,
                    None,
                    "PROCESSING",
                    0,
                    "pre_post_check_unverifiable",
                    None,
                    first_unv_utc,
                ),
            )
            conn.commit()

        text_file = Path(tmp) / "delayed_reply_draft.txt"
        raw_body = "履歴失敗時の遅延再送テスト本文です。"
        text_file.write_text(raw_body + "\n", encoding="utf-8")

        history_calls = {"count": 0}
        sleeps: List[float] = []

        class FailingHistoryDelayedClient:
            def auth_test(self) -> Dict[str, Any]:
                return {"ok": True, "user_id": bot_uid}

            def conversations_replies(self, **kwargs: Any) -> Dict[str, Any]:
                history_calls["count"] += 1
                raise http.client.IncompleteRead(b"delayed-retry-history-down", 32)

            def chat_postMessage(self, **kwargs: Any) -> Dict[str, Any]:
                raise AssertionError("chat_postMessage must NOT be called when history read fails on delayed retry")

        stdout_buf = io.StringIO()
        stderr_buf = io.StringIO()
        with patch.object(
            agent_tools, "get_web_client", return_value=FailingHistoryDelayedClient()
        ), patch.object(
            agent_tools.time, "sleep", side_effect=lambda s: sleeps.append(s)
        ), redirect_stdout(stdout_buf), redirect_stderr(stderr_buf):
            rc = agent_tools.main(
                [
                    "--db",
                    str(db_path),
                    "reply",
                    "--channel",
                    channel_id,
                    "--text-file",
                    str(text_file),
                    "--thread-ts",
                    thread_ts,
                    "--event-id",
                    ev_id,
                ]
            )

        assert rc == 1, f"Expected exit code 1 on history failure, got {rc}"
        assert history_calls["count"] == 5, f"Expected 5 history attempts, got {history_calls['count']}"
        assert sleeps == [1.0, 2.0, 4.0, 8.0]
        assert stdout_buf.getvalue() == ""
        err_out = json.loads(stderr_buf.getvalue())
        assert err_out["ok"] is False
        assert err_out["error"] == "pre_post_check_unverifiable"
        assert err_out["channel"] == channel_id
        assert err_out["thread_ts"] == thread_ts
        assert err_out["event_id"] == ev_id

        # Draft file saved in state/ must remain raw_body (not polluted with prefix)
        saved_draft = state_dir / f"reply-{ev_id}.txt"
        assert saved_draft.exists()
        assert saved_draft.read_text(encoding="utf-8").strip() == raw_body

        # Committing FAILED with pre_post_check_unverifiable preserves first_unverifiable_at and retry_count=0
        ev_state = commit_event(
            ev_id,
            "FAILED",
            error_message="pre_post_check_unverifiable",
            db_path=db_path,
            now="2026-11-14 01:00:00",
            retry_wait_seconds=3600,
        )
        assert ev_state["status"] == "PENDING"
        assert ev_state["retry_count"] == 0
        assert ev_state["first_unverifiable_at"] == first_unv_utc
        assert ev_state["next_retry_at"] == "2026-11-14 02:00:00"

        return "遅延再送時のスレッド履歴取得5回全失敗で送信せず既存のpre_post_check_unverifiable(exit 1)を返し、下書き本文とfirst_unverifiable_atが維持されることを確認"


def test_delayed_retry_legacy_and_new_header_dedup() -> str:
    """37. 再送ヘッダー3者統一形＆移行期二重付加防止: 新形式生成・新旧両形式の正規表現判定（別時刻・CRLF含む）で二重付加しないことを確認"""
    # 1) format_delayed_retry_prefix formats in Asia/Tokyo with YYYY-MM-DD HH:MM JST
    # 2026-11-13 14:00:00 UTC == 2026-11-13 23:00 Asia/Tokyo
    prefix = agent_tools.format_delayed_retry_prefix("2026-11-13 14:00:00")
    assert prefix == "遅れての再送（元の失敗時刻：2026-11-13 23:00 JST）", (
        f"Unexpected formatted prefix: {prefix!r}"
    )

    body = "テスト本文です。"

    # 2) apply_delayed_retry_prefix: prepends 1 line when no header exists
    out_added, added_flag, p_line = agent_tools.apply_delayed_retry_prefix(
        body, "2026-11-13 14:00:00"
    )
    assert added_flag is True
    assert p_line == "遅れての再送（元の失敗時刻：2026-11-13 23:00 JST）"
    assert out_added == f"遅れての再送（元の失敗時刻：2026-11-13 23:00 JST）\n{body}"

    # 3) apply_delayed_retry_prefix: does NOT add when starting with new format (same or different timestamp, LF or CRLF)
    for new_existing in (
        f"遅れての再送（元の失敗時刻：2026-11-13 23:00 JST）\n{body}",
        f"遅れての再送（元の失敗時刻：2026-11-12 09:15 JST）\n{body}",
        f"遅れての再送（元の失敗時刻：2026-11-12 09:15 JST）\r\n{body}",
        "遅れての再送（元の失敗時刻：2026-11-13 23:00 JST）",
    ):
        assert agent_tools.DELAYED_RETRY_PREFIX_RE.match(new_existing) is not None, (
            f"Regex should match new format: {new_existing!r}"
        )
        res_text, res_added, _ = agent_tools.apply_delayed_retry_prefix(
            new_existing, "2026-11-13 14:00:00"
        )
        assert res_added is False, f"Should not add mark to {new_existing!r}"
        assert res_text == new_existing

    # 4) apply_delayed_retry_prefix: does NOT add when starting with legacy format (same or different timestamp, LF or CRLF)
    for legacy_existing in (
        f"（遅れての再送：最初の試行 11-13 23:00）\n{body}",
        f"（遅れての再送：最初の試行 11-12 09:15）\n{body}",
        f"（遅れての再送：最初の試行 11-12 09:15）\r\n{body}",
        "（遅れての再送：最初の試行 11-13 23:00）",
    ):
        assert agent_tools.DELAYED_RETRY_PREFIX_RE.match(legacy_existing) is not None, (
            f"Regex should match legacy format: {legacy_existing!r}"
        )
        res_text, res_added, _ = agent_tools.apply_delayed_retry_prefix(
            legacy_existing, "2026-11-13 14:00:00"
        )
        assert res_added is False, f"Should not add mark to {legacy_existing!r}"
        assert res_text == legacy_existing

    # 5) Normal text mentioning "遅れての再送" in the middle is NOT matched as a header and gets the header prepended on delayed retry
    normal_body_with_word = "先ほどの遅れての再送についての補足です。"
    assert agent_tools.DELAYED_RETRY_PREFIX_RE.match(normal_body_with_word) is None
    res_norm, res_norm_added, _ = agent_tools.apply_delayed_retry_prefix(
        normal_body_with_word, "2026-11-13 14:00:00"
    )
    assert res_norm_added is True
    assert res_norm == f"遅れての再送（元の失敗時刻：2026-11-13 23:00 JST）\n{normal_body_with_word}"

    return "新形式（遅れての再送（元の失敗時刻：YYYY-MM-DD HH:MM JST））整形・新旧2形式の正規表現判定（別時刻・CRLF対応）による二重付加防止を確認"


def test_ambiguous_saves_intended_text() -> str:
    """38. ambiguous時の送信文面(intended_text)保存: ambiguous_post_outcome時のみchat.postMessageへ渡した最終文字列(再送ヘッダー含む)をevents.intended_textへ保存し、通常の成功・失敗・即時回復では保存しない＆既存DBへのALTER TABLE移行が冪等であることを確認"""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "slack_events.db"
        # Initialize with listener.init_db first (simulating an existing DB before migration)
        init_db(db_path)
        # Verify agent_tools.init_db idempotently adds intended_text column
        agent_tools.init_db(db_path)
        agent_tools.init_db(db_path)

        with get_db_connection(db_path) as conn:
            cols = {
                row["name"]
                for row in conn.execute("PRAGMA table_info(events);").fetchall()
            }
        assert "intended_text" in cols, f"intended_text column missing from events: {cols}"
        assert "attempt_ts" in cols, f"attempt_ts column missing from events: {cols}"

        bot_uid = "U_YAMA_BOT"
        channel_id = "C_ALL"
        thread_ts = "1796000000.000100"
        attempt_now_1 = 1796000190.25
        attempt_now_2 = 1796000195.5
        first_unv_utc = "2026-11-13 23:05:00"  # -> 2026-11-14 08:05 JST
        expected_mark = "遅れての再送（元の失敗時刻：2026-11-14 08:05 JST）"

        ev_ambig_normal = "Ev_AMBIG_NORMAL_1"
        ev_ambig_delayed = "Ev_AMBIG_DELAYED_1"
        ev_success = "Ev_POST_SUCCESS_1"
        ev_api_fail = "Ev_POST_API_FAIL_1"
        ev_recovered = "Ev_POST_RECOVERED_1"

        with get_db_connection(db_path) as conn:
            conn.executemany(
                """
                INSERT INTO events (
                    event_id, channel_id, user_id, text, ts, thread_ts,
                    status, retry_count, error_message, next_retry_at, first_unverifiable_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (ev_ambig_normal, channel_id, "U_ASKER", "Q1", "1796000001.000100", thread_ts, "PROCESSING", 0, None, None, None),
                    (ev_ambig_delayed, channel_id, "U_ASKER", "Q2", "1796000002.000100", thread_ts, "PROCESSING", 0, "pre_post_check_unverifiable", None, first_unv_utc),
                    (ev_success, channel_id, "U_ASKER", "Q3", "1796000003.000100", thread_ts, "PROCESSING", 0, None, None, None),
                    (ev_api_fail, channel_id, "U_ASKER", "Q4", "1796000004.000100", thread_ts, "PROCESSING", 0, None, None, None),
                    (ev_recovered, channel_id, "U_ASKER", "Q5", "1796000005.000100", thread_ts, "PROCESSING", 0, None, None, None),
                ],
            )
            conn.commit()

        raw_body = "ambiguous保存テストの本文です（A & B < C > D）。"
        text_file = Path(tmp) / "reply_ambig.txt"
        text_file.write_text(raw_body + "\n", encoding="utf-8")

        # 1) Normal ambiguous_post_outcome -> intended_text and attempt_ts saved
        class AmbiguousClient:
            def auth_test(self) -> Dict[str, Any]:
                return {"ok": True, "user_id": bot_uid}

            def conversations_replies(self, **kwargs: Any) -> Dict[str, Any]:
                return {
                    "ok": True,
                    "messages": [{"user": "U_ASKER", "text": "parent", "ts": thread_ts}],
                    "has_more": False,
                }

            def chat_postMessage(self, **kwargs: Any) -> Dict[str, Any]:
                raise http.client.RemoteDisconnected("Lost connection after POST")

        stdout1, stderr1 = io.StringIO(), io.StringIO()
        with patch.object(agent_tools, "get_web_client", return_value=AmbiguousClient()), patch.object(
            agent_tools.time, "time", return_value=attempt_now_1
        ), patch.object(
            agent_tools.time, "sleep", side_effect=lambda s: None
        ), redirect_stdout(stdout1), redirect_stderr(stderr1):
            rc1 = agent_tools.main(
                [
                    "--db", str(db_path),
                    "reply",
                    "--channel", channel_id,
                    "--text-file", str(text_file),
                    "--thread-ts", thread_ts,
                    "--event-id", ev_ambig_normal,
                ]
            )
        assert rc1 == 1
        err1 = json.loads(stderr1.getvalue())
        assert err1["error"] == "ambiguous_post_outcome"
        row_ambig_normal = agent_tools.get_event_by_id(ev_ambig_normal, db_path=db_path)
        assert row_ambig_normal is not None
        assert row_ambig_normal["intended_text"] == raw_body, (
            f"Expected intended_text={raw_body!r}, got {row_ambig_normal['intended_text']!r}"
        )
        assert row_ambig_normal["attempt_ts"] == f"{attempt_now_1:.6f}", (
            f"Expected attempt_ts={attempt_now_1:.6f}, got {row_ambig_normal['attempt_ts']!r}"
        )

        # 2) Delayed retry ambiguous_post_outcome -> intended_text saved WITH delayed retry header prepended + attempt_ts
        stdout2, stderr2 = io.StringIO(), io.StringIO()
        with patch.object(agent_tools, "get_web_client", return_value=AmbiguousClient()), patch.object(
            agent_tools.time, "time", return_value=attempt_now_2
        ), patch.object(
            agent_tools.time, "sleep", side_effect=lambda s: None
        ), redirect_stdout(stdout2), redirect_stderr(stderr2):
            rc2 = agent_tools.main(
                [
                    "--db", str(db_path),
                    "reply",
                    "--channel", channel_id,
                    "--text-file", str(text_file),
                    "--thread-ts", thread_ts,
                    "--event-id", ev_ambig_delayed,
                ]
            )
        assert rc2 == 1
        err2 = json.loads(stderr2.getvalue())
        assert err2["error"] == "ambiguous_post_outcome"
        row_ambig_delayed = agent_tools.get_event_by_id(ev_ambig_delayed, db_path=db_path)
        assert row_ambig_delayed is not None
        expected_final_delayed_text = f"{expected_mark}\n{raw_body}"
        assert row_ambig_delayed["intended_text"] == expected_final_delayed_text, (
            f"Expected intended_text with retry header {expected_final_delayed_text!r}, got {row_ambig_delayed['intended_text']!r}"
        )
        assert row_ambig_delayed["attempt_ts"] == f"{attempt_now_2:.6f}", (
            f"Expected attempt_ts={attempt_now_2:.6f}, got {row_ambig_delayed['attempt_ts']!r}"
        )

        # 3) Normal success -> intended_text and attempt_ts remain None
        class SuccessClient:
            def auth_test(self) -> Dict[str, Any]:
                return {"ok": True, "user_id": bot_uid}

            def conversations_replies(self, **kwargs: Any) -> Dict[str, Any]:
                return {"ok": True, "messages": [], "has_more": False}

            def chat_postMessage(self, **kwargs: Any) -> Dict[str, Any]:
                return {"ok": True, "channel": channel_id, "ts": "1796000099.000100", "message": {"text": kwargs["text"]}}

        stdout3, stderr3 = io.StringIO(), io.StringIO()
        with patch.object(agent_tools, "get_web_client", return_value=SuccessClient()), redirect_stdout(
            stdout3
        ), redirect_stderr(stderr3):
            rc3 = agent_tools.main(
                [
                    "--db", str(db_path),
                    "reply",
                    "--channel", channel_id,
                    "--text-file", str(text_file),
                    "--thread-ts", thread_ts,
                    "--event-id", ev_success,
                ]
            )
        assert rc3 == 0
        row_success = agent_tools.get_event_by_id(ev_success, db_path=db_path)
        assert row_success is not None and row_success["intended_text"] is None and row_success["attempt_ts"] is None

        # 4) SlackApiError failure -> intended_text and attempt_ts remain None
        with get_db_connection(db_path) as conn:
            conn.execute("DELETE FROM replies")
            conn.commit()
        class ApiFailClient:
            def auth_test(self) -> Dict[str, Any]:
                return {"ok": True, "user_id": bot_uid}

            def conversations_replies(self, **kwargs: Any) -> Dict[str, Any]:
                return {"ok": True, "messages": [], "has_more": False}

            def chat_postMessage(self, **kwargs: Any) -> Dict[str, Any]:
                raise SlackApiError(message="channel_not_found", response={"ok": False, "error": "channel_not_found"})

        stdout4, stderr4 = io.StringIO(), io.StringIO()
        with patch.object(agent_tools, "get_web_client", return_value=ApiFailClient()), redirect_stdout(
            stdout4
        ), redirect_stderr(stderr4):
            rc4 = agent_tools.main(
                [
                    "--db", str(db_path),
                    "reply",
                    "--channel", channel_id,
                    "--text-file", str(text_file),
                    "--thread-ts", thread_ts,
                    "--event-id", ev_api_fail,
                ]
            )
        assert rc4 == 1
        row_api_fail = agent_tools.get_event_by_id(ev_api_fail, db_path=db_path)
        assert row_api_fail is not None and row_api_fail["intended_text"] is None and row_api_fail["attempt_ts"] is None

        # 5) Connection error on post, but immediately recovered by _find_own_recent_message -> intended_text and attempt_ts remain None
        reply_calls = {"count": 0}

        class ImmediateRecoveryClient:
            def auth_test(self) -> Dict[str, Any]:
                return {"ok": True, "user_id": bot_uid}

            def conversations_replies(self, **kwargs: Any) -> Dict[str, Any]:
                reply_calls["count"] += 1
                if reply_calls["count"] == 1:
                    return {"ok": True, "messages": [], "has_more": False}
                return {
                    "ok": True,
                    "messages": [{"user": bot_uid, "text": raw_body, "ts": "1796000199.000100", "thread_ts": thread_ts}],
                    "has_more": False,
                }

            def chat_postMessage(self, **kwargs: Any) -> Dict[str, Any]:
                raise http.client.RemoteDisconnected("Lost connection but message landed")

        stdout5, stderr5 = io.StringIO(), io.StringIO()
        with patch.object(agent_tools, "get_web_client", return_value=ImmediateRecoveryClient()), patch.object(
            agent_tools.time, "time", return_value=attempt_now_2
        ), redirect_stdout(
            stdout5
        ), redirect_stderr(stderr5):
            rc5 = agent_tools.main(
                [
                    "--db", str(db_path),
                    "reply",
                    "--channel", channel_id,
                    "--text-file", str(text_file),
                    "--thread-ts", thread_ts,
                    "--event-id", ev_recovered,
                ]
            )
        assert rc5 == 0
        row_recovered = agent_tools.get_event_by_id(ev_recovered, db_path=db_path)
        assert row_recovered is not None and row_recovered["intended_text"] is None and row_recovered["attempt_ts"] is None

        return "ambiguous発生時のみ最終送信文字列(遅延再送ヘッダー付き含む)をevents.intended_textへ保存し、通常成功・API失敗・即時回復ではNULL維持、ALTER TABLE移行の冪等性を確認"


def test_reconcile_intended_text_matching_and_fallback() -> str:
    """39. reconcileのintended_text文面一致＆正規化＆不一致時未記録（推定フォールバック廃止）: (1)スレッド内Yama投稿2件で遅い方がintended_textと一致→一致側を割当、(2)HTMLエスケープ(&amp;/&lt;/&gt;)・CRLF・前後空白違いでも正規化一致、(3)不一致時は最古未割当へフォールバックせず未記録(phase1はnot_found、phase2はerror消去のみwithout_reply)を確認"""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "slack_events.db"
        init_db(db_path)
        agent_tools.init_db(db_path)

        bot_uid = "U_YAMA_BOT"
        channel_id = "C_ALL"
        now_dt = datetime.now(timezone.utc)
        recent_ts_str = (now_dt - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")

        # Thread 1 (phase1): 2 Yama posts after event; earlier is different text, later matches intended_text
        th_1 = "1796100100.000100"
        ev_p1_match = "Ev_P1_LATER_MATCH"
        ts_p1_early_other = "1796100110.000100"
        ts_p1_late_match = "1796100120.000200"
        intended_p1 = "質問Aへの回答です。設定ファイルを確認してください。"

        # Thread 2 (phase1): HTML escape (&amp; &lt; &gt;), CRLF (\r\n), and leading/trailing whitespace differences
        th_2 = "1796100200.000100"
        ev_p1_escape = "Ev_P1_ESCAPE_NORM"
        ts_p1_esc_early = "1796100210.000100"
        ts_p1_esc_match = "1796100220.000200"
        intended_p2 = "  条件: a < b && c > d\r\n2行目のテキスト  "
        slack_escaped_p2 = "\n条件: a &lt; b &amp;&amp; c &gt; d\n2行目のテキスト\t"

        # Thread 3 (phase1): intended_text is present, but neither Yama post matches -> must NOT record (not_found)
        th_3 = "1796100300.000100"
        ev_p1_mismatch = "Ev_P1_MISMATCH_NOT_RECORDED"
        ts_p1_fb_early = "1796100310.000100"
        ts_p1_fb_late = "1796100320.000200"
        intended_p3 = "どの投稿とも一致しない送信予定文面"

        # Thread 4 (phase2 - DONE with residual error_message not matching phase1 filter):
        # 2 Yama posts after event; earlier is different, later matches intended_text (with HTML escape)
        th_4 = "1796100400.000100"
        ev_p2_match = "Ev_P2_LATER_MATCH"
        ts_p2_early_other = "1796100410.000100"
        ts_p2_late_match = "1796100420.000200"
        intended_p4 = "Phase2の回答: X & Y < Z > W"
        slack_escaped_p4 = "Phase2の回答: X &amp; Y &lt; Z &gt; W"

        # Thread 5 (phase2 - DONE with residual error_message, non-matching intended_text -> NOT recorded, without_reply)
        th_5 = "1796100500.000100"
        ev_p2_mismatch = "Ev_P2_MISMATCH_NOT_RECORDED"
        ts_p2_fb_early = "1796100510.000100"
        ts_p2_fb_late = "1796100520.000200"
        intended_p5 = "Phase2で一致しない文面"

        with get_db_connection(db_path) as conn:
            conn.executemany(
                """
                INSERT INTO events (
                    event_id, channel_id, user_id, text, ts, thread_ts,
                    status, error_message, intended_text, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (ev_p1_match, channel_id, "U_1", "Q1", th_1, None, "FAILED", "ambiguous_post_outcome", intended_p1, recent_ts_str, recent_ts_str),
                    (ev_p1_escape, channel_id, "U_2", "Q2", th_2, None, "FAILED", "ambiguous_post_outcome", intended_p2, recent_ts_str, recent_ts_str),
                    (ev_p1_mismatch, channel_id, "U_3", "Q3", th_3, None, "FAILED", "ambiguous_post_outcome", intended_p3, recent_ts_str, recent_ts_str),
                    (ev_p2_match, channel_id, "U_4", "Q4", th_4, None, "DONE", "transient_socket_warning", intended_p4, recent_ts_str, recent_ts_str),
                    (ev_p2_mismatch, channel_id, "U_5", "Q5", th_5, None, "DONE", "transient_socket_warning", intended_p5, recent_ts_str, recent_ts_str),
                ],
            )
            conn.commit()

        threads_data = {
            th_1: [
                {"user": "U_1", "text": "Q1", "ts": th_1},
                {"user": bot_uid, "text": "別イベントへの先行返信（早い方）", "ts": ts_p1_early_other, "thread_ts": th_1},
                {"user": bot_uid, "text": intended_p1, "ts": ts_p1_late_match, "thread_ts": th_1},
            ],
            th_2: [
                {"user": "U_2", "text": "Q2", "ts": th_2},
                {"user": bot_uid, "text": "無関係な早い投稿", "ts": ts_p1_esc_early, "thread_ts": th_2},
                {"user": bot_uid, "text": slack_escaped_p2, "ts": ts_p1_esc_match, "thread_ts": th_2},
            ],
            th_3: [
                {"user": "U_3", "text": "Q3", "ts": th_3},
                {"user": bot_uid, "text": "別件の早い方の返信", "ts": ts_p1_fb_early, "thread_ts": th_3},
                {"user": bot_uid, "text": "別の遅い返信", "ts": ts_p1_fb_late, "thread_ts": th_3},
            ],
            th_4: [
                {"user": "U_4", "text": "Q4", "ts": th_4},
                {"user": bot_uid, "text": "Phase2の別投稿（早い方）", "ts": ts_p2_early_other, "thread_ts": th_4},
                {"user": bot_uid, "text": slack_escaped_p4, "ts": ts_p2_late_match, "thread_ts": th_4},
            ],
            th_5: [
                {"user": "U_5", "text": "Q5", "ts": th_5},
                {"user": bot_uid, "text": "Phase2別件の早い方", "ts": ts_p2_fb_early, "thread_ts": th_5},
                {"user": bot_uid, "text": "Phase2別件の遅い方", "ts": ts_p2_fb_late, "thread_ts": th_5},
            ],
        }

        class ReconcileMatchClient:
            def auth_test(self) -> Dict[str, Any]:
                return {"ok": True, "user_id": bot_uid}

            def conversations_replies(self, **kwargs: Any) -> Dict[str, Any]:
                ts = str(kwargs.get("ts"))
                return {
                    "ok": True,
                    "messages": threads_data.get(ts, []),
                    "has_more": False,
                }

        res = agent_tools.reconcile_replies(
            days=7,
            dry_run=False,
            db_path=db_path,
            client=ReconcileMatchClient(),
            now=now_dt,
        )
        assert res["ok"] is True
        assert res["errors"] == 0

        rec_map = {item["event_id"]: item["reply_ts"] for item in res["recorded"]}
        # (1) Phase 1: picks later matching post instead of earlier non-matching post
        assert rec_map[ev_p1_match] == ts_p1_late_match, (
            f"Expected {ts_p1_late_match} (matching text), got {rec_map.get(ev_p1_match)}"
        )
        # (2) Phase 1: matches despite HTML entities (&amp; &lt; &gt;), CRLF, and surrounding whitespace
        assert rec_map[ev_p1_escape] == ts_p1_esc_match, (
            f"Expected {ts_p1_esc_match} (normalized escape match), got {rec_map.get(ev_p1_escape)}"
        )
        # (3) Phase 1: does NOT fall back when no text matches -> left unrecorded and listed in not_found
        assert ev_p1_mismatch not in rec_map, (
            f"Mismatched phase1 event must not be recorded, got {rec_map.get(ev_p1_mismatch)}"
        )
        assert res["not_found"] == [ev_p1_mismatch], (
            f"Expected [{ev_p1_mismatch}] in not_found, got {res['not_found']}"
        )
        # (4) Phase 2: picks later matching post (with HTML escape normalization) and clears DONE error_message as with_reply
        assert rec_map[ev_p2_match] == ts_p2_late_match, (
            f"Expected {ts_p2_late_match} in phase2, got {rec_map.get(ev_p2_match)}"
        )
        # (5) Phase 2: does NOT fall back when no text matches -> clears DONE error_message as without_reply, not recorded
        assert ev_p2_mismatch not in rec_map, (
            f"Mismatched phase2 event must not be recorded, got {rec_map.get(ev_p2_mismatch)}"
        )
        assert res["done_error_cleared"]["with_reply"] == [ev_p2_match], (
            f"Expected only [{ev_p2_match}] in with_reply, got {res['done_error_cleared']['with_reply']}"
        )
        assert res["done_error_cleared"]["without_reply"] == [ev_p2_mismatch], (
            f"Expected [{ev_p2_mismatch}] in without_reply, got {res['done_error_cleared']['without_reply']}"
        )

        # Verify replies table records match only the text-matched events
        for eid, expected_ts in (
            (ev_p1_match, ts_p1_late_match),
            (ev_p1_escape, ts_p1_esc_match),
            (ev_p2_match, ts_p2_late_match),
        ):
            r_row = get_recorded_reply(eid, db_path=db_path)
            assert r_row is not None and r_row["reply_ts"] == expected_ts

        assert get_recorded_reply(ev_p1_mismatch, db_path=db_path) is None
        assert get_recorded_reply(ev_p2_mismatch, db_path=db_path) is None

        # Verify DONE error_message was cleared for both phase2 DONE events
        for eid in (ev_p2_match, ev_p2_mismatch):
            ev_row = agent_tools.get_event_by_id(eid, db_path=db_path)
            assert ev_row is not None and ev_row["error_message"] is None

        return "スレッド内複数Yama投稿でintended_text一致側(遅い投稿)を割当・HTMLエスケープ(&amp;/&lt;/&gt;)やCRLF/前後空白の正規化一致・不一致時は推定割当せず未記録(phase1=not_found, phase2=without_reply)を確認"


def test_reconcile_no_intended_text_never_estimated() -> str:
    """40. intended_textがないイベントの推定割当廃止: intended_textがNULL/空文字のイベントは未割当Yama投稿が後に存在してもreplies台帳に記録せず(phase1=not_found、phase2 DONE=without_replyでエラー消去)、台帳記録済みDONEイベントのts完全一致確認(with_reply)は維持されることを確認"""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "slack_events.db"
        init_db(db_path)
        agent_tools.init_db(db_path)

        bot_uid = "U_YAMA_BOT"
        channel_id = "C_ALL"
        now_dt = datetime.now(timezone.utc)
        recent_ts_str = (now_dt - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")

        # 1) Phase 1 FAILED candidate with intended_text=None, unassigned Yama post exists after event -> not_found, not recorded
        th_1 = "1796200100.000100"
        ev_p1_null = "Ev_P1_NO_INTENDED_NULL"
        ts_yama_1 = "1796200110.000100"

        # 2) Phase 1 DONE candidate with intended_text="   " (whitespace only), unassigned Yama post exists -> not_found, not recorded, cleared as without_reply
        th_2 = "1796200200.000100"
        ev_p1_done_empty = "Ev_P1_DONE_NO_INTENDED_EMPTY"
        ts_yama_2 = "1796200210.000100"

        # 3) Phase 2 DONE candidate (non-phase1 error_message) with intended_text=None, unassigned Yama post exists -> not recorded, cleared as without_reply
        th_3 = "1796200300.000100"
        ev_p2_done_null = "Ev_P2_DONE_NO_INTENDED_NULL"
        ts_yama_3 = "1796200310.000100"

        # 4) Phase 2 DONE candidate already recorded in replies table with intended_text=None -> confirmed via ts match -> cleared as with_reply
        th_4 = "1796200400.000100"
        ev_p2_recorded_ts = "Ev_P2_RECORDED_TS_CONFIRM"
        ts_yama_4 = "1796200410.000100"

        with get_db_connection(db_path) as conn:
            conn.executemany(
                """
                INSERT INTO events (
                    event_id, channel_id, user_id, text, ts, thread_ts,
                    status, error_message, intended_text, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (ev_p1_null, channel_id, "U_1", "Q1", th_1, None, "FAILED", "ambiguous_post_outcome", None, recent_ts_str, recent_ts_str),
                    (ev_p1_done_empty, channel_id, "U_2", "Q2", th_2, None, "DONE", "reply not posted", "   \n  ", recent_ts_str, recent_ts_str),
                    (ev_p2_done_null, channel_id, "U_3", "Q3", th_3, None, "DONE", "IncompleteRead(16 bytes read)", None, recent_ts_str, recent_ts_str),
                    (ev_p2_recorded_ts, channel_id, "U_4", "Q4", th_4, None, "DONE", "reply not posted", None, recent_ts_str, recent_ts_str),
                ],
            )
            conn.commit()

        agent_tools.record_reply(
            event_id=ev_p2_recorded_ts,
            channel_id=channel_id,
            thread_ts=th_4,
            reply_ts=ts_yama_4,
            text="既に台帳記録済みのYama返信",
            db_path=db_path,
        )

        threads_data = {
            th_1: [
                {"user": "U_1", "text": "Q1", "ts": th_1},
                {"user": bot_uid, "text": "未割当のYama返信1", "ts": ts_yama_1, "thread_ts": th_1},
            ],
            th_2: [
                {"user": "U_2", "text": "Q2", "ts": th_2},
                {"user": bot_uid, "text": "未割当のYama返信2", "ts": ts_yama_2, "thread_ts": th_2},
            ],
            th_3: [
                {"user": "U_3", "text": "Q3", "ts": th_3},
                {"user": bot_uid, "text": "未割当のYama返信3", "ts": ts_yama_3, "thread_ts": th_3},
            ],
            th_4: [
                {"user": "U_4", "text": "Q4", "ts": th_4},
                {"user": bot_uid, "text": "既に台帳記録済みのYama返信", "ts": ts_yama_4, "thread_ts": th_4},
            ],
        }

        class NoEstimateClient:
            def auth_test(self) -> Dict[str, Any]:
                return {"ok": True, "user_id": bot_uid}

            def conversations_replies(self, **kwargs: Any) -> Dict[str, Any]:
                ts = str(kwargs.get("ts"))
                return {
                    "ok": True,
                    "messages": threads_data.get(ts, []),
                    "has_more": False,
                }

        res = agent_tools.reconcile_replies(
            days=7,
            dry_run=False,
            db_path=db_path,
            client=NoEstimateClient(),
            now=now_dt,
        )
        assert res["ok"] is True
        assert res["errors"] == 0
        assert res["checked"] == 2  # ev_p1_null, ev_p1_done_empty
        assert res["recorded"] == [], f"Expected no estimated recordings, got {res['recorded']}"
        assert res["not_found"] == [ev_p1_null, ev_p1_done_empty], (
            f"Expected [{ev_p1_null}, {ev_p1_done_empty}] in not_found, got {res['not_found']}"
        )
        assert res["done_error_cleared"]["with_reply"] == [ev_p2_recorded_ts], (
            f"Expected only ts-confirmed [{ev_p2_recorded_ts}] in with_reply, got {res['done_error_cleared']['with_reply']}"
        )
        assert res["done_error_cleared"]["without_reply"] == [ev_p1_done_empty, ev_p2_done_null], (
            f"Expected [{ev_p1_done_empty}, {ev_p2_done_null}] in without_reply, got {res['done_error_cleared']['without_reply']}"
        )

        # Confirm none of the events without intended_text were recorded into replies table
        assert get_recorded_reply(ev_p1_null, db_path=db_path) is None
        assert get_recorded_reply(ev_p1_done_empty, db_path=db_path) is None
        assert get_recorded_reply(ev_p2_done_null, db_path=db_path) is None
        # Pre-recorded event stays intact
        rec_4 = get_recorded_reply(ev_p2_recorded_ts, db_path=db_path)
        assert rec_4 is not None and rec_4["reply_ts"] == ts_yama_4

        # Confirm DONE events had their error_message cleared, while FAILED event kept its error_message
        for eid in (ev_p1_done_empty, ev_p2_done_null, ev_p2_recorded_ts):
            row = agent_tools.get_event_by_id(eid, db_path=db_path)
            assert row is not None and row["error_message"] is None
        row_failed = agent_tools.get_event_by_id(ev_p1_null, db_path=db_path)
        assert row_failed is not None and row_failed["error_message"] == "ambiguous_post_outcome"

        return "intended_textなし(NULL/空白のみ)のイベントは未割当Yama投稿が後にあっても推定記録せず(not_found/without_reply)、台帳記録済みDONEイベントのts完全一致確認(with_reply)は維持されることを確認"


def test_windowed_thread_dedup_after_event_ts() -> str:
    """41. 対象イベント以降のスレッド読み取り（重複検出・reconcile起点）: イベント ts より後にある自分の同一文面を oldest=event_ts-60s で検出してスキップし、reconcile も同じ oldest 起点を渡すことを確認"""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "slack_events.db"
        init_db(db_path)
        agent_tools.init_db(db_path)

        bot_uid = "U_YAMA_BOT"
        channel_id = "C_ALL"
        thread_ts = "1797000000.000100"
        event_ts = "1797005000.000200"
        expected_oldest = f"{float(event_ts) - 60.0:.6f}"  # 1797004940.000200
        own_after_ts = "1797005015.000300"
        reply_text = "対象イベントの後に着地済みのYama返信です。"

        ev_normal = "Ev_WIN_AFTER_NORMAL"
        ev_delayed = "Ev_WIN_AFTER_DELAYED"
        ev_reconcile = "Ev_WIN_AFTER_RECONCILE"
        reconcile_event_ts = "1797008000.000500"
        expected_reconcile_oldest = f"{float(reconcile_event_ts) - 60.0:.6f}"
        reconcile_reply_ts = "1797008020.000600"
        reconcile_text = "reconcile対象のYama返信です。"
        now_dt = datetime.now(timezone.utc)
        recent_str = (now_dt - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
        # PROCESSING rows are "just dispatched" (updated_at = now): the safety-read
        # deadline counts back from the Reaper horizon (dispatch + 900s).
        dispatched_str = now_dt.strftime("%Y-%m-%d %H:%M:%S")

        with get_db_connection(db_path) as conn:
            conn.executemany(
                """
                INSERT INTO events (
                    event_id, channel_id, user_id, text, ts, thread_ts,
                    status, error_message, first_unverifiable_at, intended_text, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (ev_normal, channel_id, "U_ASKER", "質問1", event_ts, thread_ts, "PROCESSING", None, None, None, recent_str, dispatched_str),
                    (ev_delayed, channel_id, "U_ASKER", "質問2", event_ts, thread_ts, "PROCESSING", "pre_post_check_unverifiable", "2026-11-13 23:00:00", None, recent_str, dispatched_str),
                    (ev_reconcile, channel_id, "U_ASKER", "質問3", reconcile_event_ts, thread_ts, "FAILED", "ambiguous_post_outcome", None, reconcile_text, recent_str, recent_str),
                ],
            )
            conn.commit()

        text_file = Path(tmp) / "reply_win_after.txt"
        text_file.write_text(reply_text + "\n", encoding="utf-8")

        seen_oldest: List[Optional[str]] = []

        class WindowAfterClient:
            def auth_test(self) -> Dict[str, Any]:
                return {"ok": True, "user_id": bot_uid}

            def conversations_replies(self, **kwargs: Any) -> Dict[str, Any]:
                seen_oldest.append(kwargs.get("oldest"))
                if kwargs.get("oldest") == expected_reconcile_oldest:
                    return {
                        "ok": True,
                        "messages": [
                            {"user": "U_ASKER", "text": "親", "ts": thread_ts},
                            {"user": bot_uid, "text": reconcile_text, "ts": reconcile_reply_ts, "thread_ts": thread_ts},
                        ],
                        "has_more": False,
                    }
                return {
                    "ok": True,
                    "messages": [
                        {"user": "U_ASKER", "text": "親", "ts": thread_ts},
                        {"user": "U_ASKER", "text": "質問", "ts": event_ts, "thread_ts": thread_ts},
                        {"user": bot_uid, "text": reply_text, "ts": own_after_ts, "thread_ts": thread_ts},
                    ],
                    "has_more": False,
                }

            def chat_postMessage(self, **kwargs: Any) -> Dict[str, Any]:
                raise AssertionError("chat_postMessage must NOT be called when own message after event_ts is found")

        # 1) Normal reply with event_id -> uses oldest = event_ts - 60, detects own message after event_ts, skips
        stdout1, stderr1 = io.StringIO(), io.StringIO()
        with patch.object(agent_tools, "get_web_client", return_value=WindowAfterClient()), redirect_stdout(
            stdout1
        ), redirect_stderr(stderr1):
            rc1 = agent_tools.main(
                [
                    "--db", str(db_path),
                    "reply",
                    "--channel", channel_id,
                    "--text-file", str(text_file),
                    "--thread-ts", thread_ts,
                    "--event-id", ev_normal,
                ]
            )
        assert rc1 == 0, f"Expected rc=0, got {rc1}, stderr={stderr1.getvalue()}"
        assert seen_oldest == [expected_oldest], f"Expected oldest={expected_oldest}, got {seen_oldest}"
        out1 = json.loads(stdout1.getvalue())
        assert out1["ok"] is True and out1["deduplicated"] is True and out1["ts"] == own_after_ts
        assert get_recorded_reply(ev_normal, db_path=db_path) is not None

        # 2) Delayed retry with event_id -> also uses oldest = event_ts - 60, detects own message after event_ts, skips
        seen_oldest.clear()
        stdout2, stderr2 = io.StringIO(), io.StringIO()
        with patch.object(agent_tools, "get_web_client", return_value=WindowAfterClient()), redirect_stdout(
            stdout2
        ), redirect_stderr(stderr2):
            rc2 = agent_tools.main(
                [
                    "--db", str(db_path),
                    "reply",
                    "--channel", channel_id,
                    "--text-file", str(text_file),
                    "--thread-ts", thread_ts,
                    "--event-id", ev_delayed,
                ]
            )
        assert rc2 == 0, f"Expected rc=0, got {rc2}, stderr={stderr2.getvalue()}"
        assert seen_oldest == [expected_oldest], f"Expected oldest={expected_oldest} on delayed retry, got {seen_oldest}"
        out2 = json.loads(stdout2.getvalue())
        assert out2["ok"] is True and out2["deduplicated"] is True and out2["ts"] == own_after_ts

        # 3) reconcile_replies -> also scans from oldest = cand_ts - 60
        seen_oldest.clear()
        res_rec = agent_tools.reconcile_replies(
            days=7,
            dry_run=False,
            db_path=db_path,
            client=WindowAfterClient(),
            now=now_dt,
        )
        assert res_rec["ok"] is True
        assert seen_oldest == [expected_reconcile_oldest], (
            f"Expected reconcile oldest={expected_reconcile_oldest}, got {seen_oldest}"
        )
        assert res_rec["recorded"] == [{"event_id": ev_reconcile, "reply_ts": reconcile_reply_ts}]

        return "イベントtsより後の自Bot同一文面をoldest=event_ts-60sで検出・スキップ(通常/遅延再送)し、reconcileもoldest=event_ts-60sで走査することを確認"


def test_windowed_thread_ignores_past_event_template() -> str:
    """42. 過去イベントへの定型文の誤スキップ防止: イベント ts より前（60秒マージン外）にある自分の同一文面は重複とみなさず通常投稿し、event_idなし手動投稿は従来どおり全件走査を維持することを確認"""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "slack_events.db"
        init_db(db_path)
        agent_tools.init_db(db_path)

        bot_uid = "U_YAMA_BOT"
        channel_id = "C_ALL"
        thread_ts = "1797100000.000100"
        past_yama_ts = "1797100100.000200"  # Past template reply 4900s before current event
        current_event_ts = "1797105000.000300"
        expected_oldest = f"{float(current_event_ts) - 60.0:.6f}"  # 1797104940.000300
        template_text = "了解しました。確認して折り返します。"

        ev_normal = "Ev_PAST_TEMPLATE_NORMAL"
        ev_delayed = "Ev_PAST_TEMPLATE_DELAYED"
        first_unv_utc = "2026-11-13 23:00:00"  # -> 2026-11-14 08:00 JST

        with get_db_connection(db_path) as conn:
            conn.executemany(
                """
                INSERT INTO events (
                    event_id, channel_id, user_id, text, ts, thread_ts,
                    status, error_message, first_unverifiable_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (ev_normal, channel_id, "U_ASKER", "新しい依頼1", current_event_ts, thread_ts, "PROCESSING", None, None),
                    (ev_delayed, channel_id, "U_ASKER", "新しい依頼2", current_event_ts, thread_ts, "PROCESSING", "pre_post_check_unverifiable", first_unv_utc),
                ],
            )
            conn.commit()

        text_file = Path(tmp) / "reply_template.txt"
        text_file.write_text(template_text + "\n", encoding="utf-8")

        seen_oldest: List[Optional[str]] = []
        post_calls: List[Dict[str, Any]] = []

        class PastTemplateThreadClient:
            def auth_test(self) -> Dict[str, Any]:
                return {"ok": True, "user_id": bot_uid}

            def conversations_replies(self, **kwargs: Any) -> Dict[str, Any]:
                seen_oldest.append(kwargs.get("oldest"))
                # Even if the API/mock returns the old message alongside current messages,
                # or filters by oldest, messages before (event_ts - 60) must not be treated as duplicates
                return {
                    "ok": True,
                    "messages": [
                        {"user": "U_ASKER", "text": "スレッド親", "ts": thread_ts},
                        {"user": bot_uid, "text": template_text, "ts": past_yama_ts, "thread_ts": thread_ts},
                        {"user": "U_ASKER", "text": "新しい依頼", "ts": current_event_ts, "thread_ts": thread_ts},
                    ],
                    "has_more": False,
                }

            def chat_postMessage(self, **kwargs: Any) -> Dict[str, Any]:
                post_calls.append(kwargs)
                posted_ts = f"1797105050.00000{len(post_calls)}"
                return {
                    "ok": True,
                    "channel": kwargs["channel"],
                    "ts": posted_ts,
                    "message": {"text": kwargs["text"], "ts": posted_ts},
                }

        # 1) Normal reply for current_event_ts -> does NOT falsely deduplicate against past_yama_ts, posts normally
        stdout1, stderr1 = io.StringIO(), io.StringIO()
        with patch.object(agent_tools, "get_web_client", return_value=PastTemplateThreadClient()), redirect_stdout(
            stdout1
        ), redirect_stderr(stderr1):
            rc1 = agent_tools.main(
                [
                    "--db", str(db_path),
                    "reply",
                    "--channel", channel_id,
                    "--text-file", str(text_file),
                    "--thread-ts", thread_ts,
                    "--event-id", ev_normal,
                ]
            )
        assert rc1 == 0, f"Expected rc=0, got {rc1}, stderr={stderr1.getvalue()}"
        assert seen_oldest == [expected_oldest], f"Expected oldest={expected_oldest}, got {seen_oldest}"
        assert len(post_calls) == 1, "Past template reply before window must NOT suppress new post"
        out1 = json.loads(stdout1.getvalue())
        assert out1["ok"] is True and out1["result"] == "posted" and out1["deduplicated"] is False

        # 2) Delayed retry for current_event_ts -> also does NOT falsely deduplicate against past_yama_ts, posts with mark
        seen_oldest.clear()
        stdout2, stderr2 = io.StringIO(), io.StringIO()
        with patch.object(agent_tools, "get_web_client", return_value=PastTemplateThreadClient()), redirect_stdout(
            stdout2
        ), redirect_stderr(stderr2):
            rc2 = agent_tools.main(
                [
                    "--db", str(db_path),
                    "reply",
                    "--channel", channel_id,
                    "--text-file", str(text_file),
                    "--thread-ts", thread_ts,
                    "--event-id", ev_delayed,
                ]
            )
        assert rc2 == 0, f"Expected rc=0, got {rc2}, stderr={stderr2.getvalue()}"
        assert seen_oldest == [expected_oldest]
        assert len(post_calls) == 2, "Past template reply before window must NOT suppress delayed retry post"
        out2 = json.loads(stdout2.getvalue())
        assert out2["ok"] is True and out2["result"] == "posted" and out2["deduplicated"] is False

        # 3) Manual post WITHOUT event_id -> still scans full thread (oldest=thread_ts) and detects past_yama_ts
        with get_db_connection(db_path) as conn:
            conn.execute("DELETE FROM replies")
            conn.commit()
        seen_oldest.clear()
        stdout3, stderr3 = io.StringIO(), io.StringIO()
        with patch.object(agent_tools, "get_web_client", return_value=PastTemplateThreadClient()), redirect_stdout(
            stdout3
        ), redirect_stderr(stderr3):
            rc3 = agent_tools.main(
                [
                    "--db", str(db_path),
                    "reply",
                    "--channel", channel_id,
                    "--text-file", str(text_file),
                    "--thread-ts", thread_ts,
                    "--no-event-id",
                ]
            )
        assert rc3 == 0
        assert seen_oldest == [thread_ts], f"Manual post without event_id must scan from thread_ts, got {seen_oldest}"
        assert len(post_calls) == 2, "Manual post should deduplicate against full thread scan"
        out3 = json.loads(stdout3.getvalue())
        assert out3["ok"] is True and out3["deduplicated"] is True and out3["ts"] == past_yama_ts

        return "イベントtsより前(マージン外)の過去定型文では誤スキップせず正常投稿し、event_idなし手動投稿は従来どおりoldest=親ts全件走査を維持することを確認"


def test_windowed_thread_dedup_within_margin_before_event_ts() -> str:
    """43. 安全マージン内の重複検出: イベント ts の数秒前（60秒マージン内）にある自分の同一文面投稿も検出してスキップすることを確認"""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "slack_events.db"
        init_db(db_path)
        agent_tools.init_db(db_path)

        bot_uid = "U_YAMA_BOT"
        channel_id = "C_ALL"
        thread_ts = "1797200000.000100"
        event_ts = "1797203000.000200"
        expected_oldest = f"{float(event_ts) - 60.0:.6f}"  # 1797202940.000200
        # 5 seconds before event_ts (well within the 60s safety margin)
        margin_own_ts = f"{float(event_ts) - 5.0:.6f}"  # 1797202995.000200
        reply_text = "イベントtsの5秒前（60秒マージン内）にあるYamaの投稿です。"

        ev_normal = "Ev_MARGIN_NORMAL"
        ev_delayed = "Ev_MARGIN_DELAYED"

        with get_db_connection(db_path) as conn:
            conn.executemany(
                """
                INSERT INTO events (
                    event_id, channel_id, user_id, text, ts, thread_ts,
                    status, error_message, first_unverifiable_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (ev_normal, channel_id, "U_ASKER", "質問A", event_ts, thread_ts, "PROCESSING", None, None),
                    (ev_delayed, channel_id, "U_ASKER", "質問B", event_ts, thread_ts, "PROCESSING", "pre_post_check_unverifiable", "2026-11-13 23:00:00"),
                ],
            )
            conn.commit()

        text_file = Path(tmp) / "reply_margin.txt"
        text_file.write_text(reply_text + "\n", encoding="utf-8")

        seen_oldest: List[Optional[str]] = []

        class MarginThreadClient:
            def auth_test(self) -> Dict[str, Any]:
                return {"ok": True, "user_id": bot_uid}

            def conversations_replies(self, **kwargs: Any) -> Dict[str, Any]:
                seen_oldest.append(kwargs.get("oldest"))
                return {
                    "ok": True,
                    "messages": [
                        {"user": bot_uid, "text": reply_text, "ts": margin_own_ts, "thread_ts": thread_ts},
                        {"user": "U_ASKER", "text": "質問", "ts": event_ts, "thread_ts": thread_ts},
                    ],
                    "has_more": False,
                }

            def chat_postMessage(self, **kwargs: Any) -> Dict[str, Any]:
                raise AssertionError("chat_postMessage must NOT be called when own post exists within 60s margin")

        # 1) Normal reply -> detects own post at event_ts - 5s and skips
        stdout1, stderr1 = io.StringIO(), io.StringIO()
        with patch.object(agent_tools, "get_web_client", return_value=MarginThreadClient()), redirect_stdout(
            stdout1
        ), redirect_stderr(stderr1):
            rc1 = agent_tools.main(
                [
                    "--db", str(db_path),
                    "reply",
                    "--channel", channel_id,
                    "--text-file", str(text_file),
                    "--thread-ts", thread_ts,
                    "--event-id", ev_normal,
                ]
            )
        assert rc1 == 0, f"Expected rc=0, got {rc1}, stderr={stderr1.getvalue()}"
        assert seen_oldest == [expected_oldest]
        out1 = json.loads(stdout1.getvalue())
        assert out1["ok"] is True and out1["deduplicated"] is True and out1["ts"] == margin_own_ts
        rec1 = get_recorded_reply(ev_normal, db_path=db_path)
        assert rec1 is not None and rec1["reply_ts"] == margin_own_ts

        # 2) Delayed retry -> also detects own post at event_ts - 5s and skips
        seen_oldest.clear()
        stdout2, stderr2 = io.StringIO(), io.StringIO()
        with patch.object(agent_tools, "get_web_client", return_value=MarginThreadClient()), redirect_stdout(
            stdout2
        ), redirect_stderr(stderr2):
            rc2 = agent_tools.main(
                [
                    "--db", str(db_path),
                    "reply",
                    "--channel", channel_id,
                    "--text-file", str(text_file),
                    "--thread-ts", thread_ts,
                    "--event-id", ev_delayed,
                ]
            )
        assert rc2 == 0, f"Expected rc=0, got {rc2}, stderr={stderr2.getvalue()}"
        assert seen_oldest == [expected_oldest]
        out2 = json.loads(stdout2.getvalue())
        assert out2["ok"] is True and out2["deduplicated"] is True and out2["ts"] == margin_own_ts

        return "イベントtsの5秒前(60秒安全マージン内)にある自Bot同一文面を通常返信・遅延再送の双方で検出しスキップ(deduplicated=true)することを確認"


def test_windowed_thread_answered_after_event_ts() -> str:
    """44. 絞った読み取り範囲での答え済み判定: スレッド内の対象イベント ts より後の他者投稿で答え済み判定が従来どおり働き、イベント ts より前の他者投稿では誤抑止しないことを確認"""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "slack_events.db"
        init_db(db_path)
        agent_tools.init_db(db_path)

        bot_uid = "U_YAMA_BOT"
        channel_id = "C_ALL"
        thread_ts = "1797300000.000100"
        questioner_uid = "U_QUESTIONER"
        answerer_uid = "U_ANSWERER"
        first_unv_utc = "2026-11-13 22:00:00"

        # Case 1: Child event in a grown thread; another user answers AFTER event_ts -> skipped_answered
        ev_answered = "Ev_WIN_ANS_AFTER"
        event_ts_1 = "1797305000.000200"
        expected_oldest_1 = f"{float(event_ts_1) - 60.0:.6f}"
        answer_after_ts = "1797305030.000300"
        answer_text = "その設定は config/mode.txt を書き換えることで反映されますよ。"

        # Case 2: Child event in a grown thread; another user's long comment exists ONLY BEFORE event_ts -> posts normally
        ev_only_before = "Ev_WIN_ANS_BEFORE_ONLY"
        event_ts_2 = "1797309000.000200"
        expected_oldest_2 = f"{float(event_ts_2) - 60.0:.6f}"
        comment_before_ts = f"{float(event_ts_2) - 10.0:.6f}"  # inside 60s margin, but before event_ts_2

        with get_db_connection(db_path) as conn:
            conn.executemany(
                """
                INSERT INTO events (
                    event_id, channel_id, user_id, text, ts, thread_ts,
                    status, retry_count, error_message, first_unverifiable_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (ev_answered, channel_id, questioner_uid, "スレッド内の追加質問1", event_ts_1, thread_ts, "PROCESSING", 0, "pre_post_check_unverifiable", first_unv_utc),
                    (ev_only_before, channel_id, questioner_uid, "スレッド内の追加質問2", event_ts_2, thread_ts, "PROCESSING", 0, "pre_post_check_unverifiable", first_unv_utc),
                ],
            )
            conn.commit()

        text_file = Path(tmp) / "reply_win_ans.txt"
        text_file.write_text("Yamaからの遅延回答です。\n", encoding="utf-8")

        seen_oldest: List[Optional[str]] = []
        post_calls: List[Dict[str, Any]] = []

        class WindowAnsweredClient:
            def auth_test(self) -> Dict[str, Any]:
                return {"ok": True, "user_id": bot_uid}

            def conversations_replies(self, **kwargs: Any) -> Dict[str, Any]:
                oldest = kwargs.get("oldest")
                seen_oldest.append(oldest)
                if oldest == expected_oldest_1:
                    return {
                        "ok": True,
                        "messages": [
                            {"user": questioner_uid, "text": "スレッド内の追加質問1", "ts": event_ts_1, "thread_ts": thread_ts},
                            {"user": answerer_uid, "text": answer_text, "ts": answer_after_ts, "thread_ts": thread_ts},
                        ],
                        "has_more": False,
                    }
                if oldest == expected_oldest_2:
                    return {
                        "ok": True,
                        "messages": [
                            {"user": answerer_uid, "text": "前の話題に対する10文字以上の他者コメントです。", "ts": comment_before_ts, "thread_ts": thread_ts},
                            {"user": questioner_uid, "text": "スレッド内の追加質問2", "ts": event_ts_2, "thread_ts": thread_ts},
                        ],
                        "has_more": False,
                    }
                raise AssertionError(f"Unexpected oldest: {oldest}")

            def chat_postMessage(self, **kwargs: Any) -> Dict[str, Any]:
                post_calls.append(kwargs)
                return {
                    "ok": True,
                    "channel": kwargs["channel"],
                    "ts": "1797309050.000900",
                    "message": {"text": kwargs["text"], "ts": "1797309050.000900"},
                }

        # 1) Answer after event_ts_1 -> skipped_answered and DONE
        stdout1, stderr1 = io.StringIO(), io.StringIO()
        with patch.object(agent_tools, "get_web_client", return_value=WindowAnsweredClient()), redirect_stdout(
            stdout1
        ), redirect_stderr(stderr1):
            rc1 = agent_tools.main(
                [
                    "--db", str(db_path),
                    "reply",
                    "--channel", channel_id,
                    "--text-file", str(text_file),
                    "--thread-ts", thread_ts,
                    "--event-id", ev_answered,
                ]
            )
        assert rc1 == 0, f"Expected rc=0, got {rc1}, stderr={stderr1.getvalue()}"
        assert seen_oldest == [expected_oldest_1]
        assert len(post_calls) == 0
        out1 = json.loads(stdout1.getvalue())
        assert out1["ok"] is True
        assert out1["result"] == "skipped_answered"
        assert out1["answered_by_ts"] == answer_after_ts
        assert out1["answered_by_user"] == answerer_uid
        assert out1["status"] == "DONE"
        ev1_row = agent_tools.get_event_by_id(ev_answered, db_path=db_path)
        assert ev1_row is not None and ev1_row["status"] == "DONE"

        # 2) Other user's comment only before event_ts_2 -> NOT skipped, posts normally
        seen_oldest.clear()
        stdout2, stderr2 = io.StringIO(), io.StringIO()
        with patch.object(agent_tools, "get_web_client", return_value=WindowAnsweredClient()), redirect_stdout(
            stdout2
        ), redirect_stderr(stderr2):
            rc2 = agent_tools.main(
                [
                    "--db", str(db_path),
                    "reply",
                    "--channel", channel_id,
                    "--text-file", str(text_file),
                    "--thread-ts", thread_ts,
                    "--event-id", ev_only_before,
                ]
            )
        assert rc2 == 0, f"Expected rc=0, got {rc2}, stderr={stderr2.getvalue()}"
        assert seen_oldest == [expected_oldest_2]
        assert len(post_calls) == 1
        out2 = json.loads(stdout2.getvalue())
        assert out2["ok"] is True and out2["result"] == "posted"

        return "oldest=event_ts-60sの窓読み取りでイベントtsより後の他者回答によりskipped_answered＆DONE遷移し、イベントts前の他者発言では誤抑止しないことを確認"


def test_windowed_thread_failure_unverifiable() -> str:
    """45. 絞った読み取り失敗時の fail-closed 維持: イベント ts 起点（oldest=event_ts-60s）の読み取りが5回リトライ後も失敗した場合は投稿せず pre_post_check_unverifiable になることを確認"""
    with tempfile.TemporaryDirectory() as tmp:
        state_dir = Path(tmp) / "state"
        state_dir.mkdir(parents=True, exist_ok=True)
        db_path = state_dir / "slack_events.db"
        init_db(db_path)
        agent_tools.init_db(db_path)

        bot_uid = "U_YAMA_BOT"
        channel_id = "C_ALL"
        thread_ts = "1797400000.000100"
        event_ts = "1797405000.000200"
        expected_oldest = f"{float(event_ts) - 60.0:.6f}"
        ev_id = "Ev_WIN_FAIL_CLOSED"

        with get_db_connection(db_path) as conn:
            conn.execute(
                """
                INSERT INTO events (
                    event_id, channel_id, user_id, text, ts, thread_ts, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (ev_id, channel_id, "U_ASKER", "質問テキスト", event_ts, thread_ts, "PROCESSING"),
            )
            conn.commit()

        text_file = Path(tmp) / "reply_win_fail.txt"
        text_file.write_text("fail-closed検証用の返信文面です。\n", encoding="utf-8")

        seen_oldest: List[Optional[str]] = []
        sleeps: List[float] = []

        class FailingWindowClient:
            def auth_test(self) -> Dict[str, Any]:
                return {"ok": True, "user_id": bot_uid}

            def conversations_replies(self, **kwargs: Any) -> Dict[str, Any]:
                seen_oldest.append(kwargs.get("oldest"))
                raise http.client.IncompleteRead(b"window-read-dropped", 32)

            def chat_postMessage(self, **kwargs: Any) -> Dict[str, Any]:
                raise AssertionError("chat_postMessage must NOT be called when windowed thread read fails")

        stdout_buf, stderr_buf = io.StringIO(), io.StringIO()
        with patch.object(
            agent_tools, "get_web_client", return_value=FailingWindowClient()
        ), patch.object(
            agent_tools.time, "sleep", side_effect=lambda s: sleeps.append(s)
        ), redirect_stdout(stdout_buf), redirect_stderr(stderr_buf):
            rc = agent_tools.main(
                [
                    "--db", str(db_path),
                    "reply",
                    "--channel", channel_id,
                    "--text-file", str(text_file),
                    "--thread-ts", thread_ts,
                    "--event-id", ev_id,
                ]
            )

        assert rc == 1, f"Expected exit code 1 on fail-closed, got {rc}"
        assert seen_oldest == [expected_oldest] * 5, (
            f"Expected 5 attempts with oldest={expected_oldest}, got {seen_oldest}"
        )
        assert sleeps == [1.0, 2.0, 4.0, 8.0]
        assert stdout_buf.getvalue() == ""
        err_out = json.loads(stderr_buf.getvalue())
        assert err_out["ok"] is False
        assert err_out["error"] == "pre_post_check_unverifiable"
        assert err_out["event_id"] == ev_id
        assert get_recorded_reply(ev_id, db_path=db_path) is None

        return "oldest=event_ts-60sに絞ったスレッド読み取りが5回試行(1→2→4→8s)すべて失敗した場合に投稿せずpre_post_check_unverifiable(exit 1)となることを確認"


def test_reply_metadata_attachment_and_include_all_metadata() -> str:
    """46. 投稿時の metadata 付与と履歴読み取り時の include_all_metadata=True 指定:
    (1) event_id あり投稿で metadata={"event_type": "yama_reply", "event_payload": {"event_id": ...}} が付与される、
    (2) event_id なし手動投稿で metadata={"event_type": "yama_reply", "event_payload": {"request_id": "<UUID>"}} が付与される、
    (3) 投稿前チェック（通常/遅延再送/チャンネル）・回復確認・reconcile 走査すべてで include_all_metadata=True が渡されることを確認"""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "slack_events.db"
        init_db(db_path)
        agent_tools.init_db(db_path)

        bot_uid = "U_YAMA_BOT"
        channel_id = "C_ALL"
        thread_ts = "1798000000.000100"
        ev_with_id = "Ev_META_ATTACH_1"
        ev_delayed_id = "Ev_META_DELAYED_1"
        ev_rec_id = "Ev_META_RECONCILE_1"
        now_dt = datetime.now(timezone.utc)
        recent_ts_str = (now_dt - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
        dispatched_ts_str = now_dt.strftime("%Y-%m-%d %H:%M:%S")  # PROCESSING rows: just dispatched

        with get_db_connection(db_path) as conn:
            conn.executemany(
                """
                INSERT INTO events (
                    event_id, channel_id, user_id, text, ts, thread_ts,
                    status, retry_count, error_message, first_unverifiable_at,
                    intended_text, attempt_ts, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        ev_with_id,
                        channel_id,
                        "U_ASKER",
                        "質問1",
                        "1798000010.000100",
                        thread_ts,
                        "PROCESSING",
                        0,
                        None,
                        None,
                        None,
                        None,
                        recent_ts_str,
                        dispatched_ts_str,
                    ),
                    (
                        ev_delayed_id,
                        channel_id,
                        "U_ASKER",
                        "質問2",
                        "1798000020.000100",
                        thread_ts,
                        "PROCESSING",
                        0,
                        "pre_post_check_unverifiable",
                        "2026-11-14 01:00:00",
                        None,
                        None,
                        recent_ts_str,
                        dispatched_ts_str,
                    ),
                    (
                        ev_rec_id,
                        channel_id,
                        "U_ASKER",
                        "質問3",
                        "1798000030.000100",
                        thread_ts,
                        "FAILED",
                        3,
                        "ambiguous_post_outcome",
                        None,
                        "reconcile対象テキスト",
                        "1798000100.000000",
                        recent_ts_str,
                        recent_ts_str,
                    ),
                ],
            )
            conn.commit()

        text_file = Path(tmp) / "reply_meta.txt"
        text_file.write_text("メタデータ検証用の返信テキストです。\n", encoding="utf-8")

        post_calls: List[Dict[str, Any]] = []
        replies_calls: List[Dict[str, Any]] = []
        history_calls: List[Dict[str, Any]] = []

        class MetadataInspectClient:
            def auth_test(self) -> Dict[str, Any]:
                return {"ok": True, "user_id": bot_uid}

            def conversations_replies(self, **kwargs: Any) -> Dict[str, Any]:
                replies_calls.append(kwargs)
                return {
                    "ok": True,
                    "messages": [{"user": "U_ASKER", "text": "親投稿", "ts": thread_ts}],
                    "has_more": False,
                }

            def conversations_history(self, **kwargs: Any) -> Dict[str, Any]:
                history_calls.append(kwargs)
                return {
                    "ok": True,
                    "messages": [{"user": "U_ASKER", "text": "チャンネル投稿", "ts": thread_ts}],
                    "has_more": False,
                }

            def chat_postMessage(self, **kwargs: Any) -> Dict[str, Any]:
                post_calls.append(kwargs)
                return {
                    "ok": True,
                    "channel": kwargs["channel"],
                    "ts": f"{1798000050.0 + len(post_calls):.6f}",
                    "message": {
                        "text": kwargs["text"],
                        "ts": f"{1798000050.0 + len(post_calls):.6f}",
                        "metadata": kwargs.get("metadata"),
                    },
                }

        client = MetadataInspectClient()

        # (1) Thread reply WITH event_id -> metadata has event_type="yama_reply" and event_payload={"event_id": ev_with_id}
        stdout1, stderr1 = io.StringIO(), io.StringIO()
        with patch.object(agent_tools, "get_web_client", return_value=client), redirect_stdout(
            stdout1
        ), redirect_stderr(stderr1):
            rc1 = agent_tools.main(
                [
                    "--db", str(db_path),
                    "reply",
                    "--channel", channel_id,
                    "--text-file", str(text_file),
                    "--thread-ts", thread_ts,
                    "--event-id", ev_with_id,
                ]
            )
        assert rc1 == 0, f"Expected rc=0, got {rc1}, stderr={stderr1.getvalue()}"
        assert len(post_calls) == 1
        meta1 = post_calls[0].get("metadata")
        assert meta1 == {
            "event_type": "yama_reply",
            "event_payload": {"event_id": ev_with_id},
        }, f"Unexpected metadata for event_id post: {meta1}"
        assert len(replies_calls) == 1
        assert replies_calls[0].get("include_all_metadata") is True, (
            f"Pre-post thread check must specify include_all_metadata=True: {replies_calls[0]}"
        )

        # (2) Manual channel post WITHOUT event_id -> metadata has event_type="yama_reply" and request_id UUID
        stdout2, stderr2 = io.StringIO(), io.StringIO()
        with patch.object(agent_tools, "get_web_client", return_value=client), redirect_stdout(
            stdout2
        ), redirect_stderr(stderr2):
            rc2 = agent_tools.main(
                [
                    "--db", str(db_path),
                    "reply",
                    "--channel", channel_id,
                    "--text-file", str(text_file),
                    "--no-event-id",
                ]
            )
        assert rc2 == 0
        assert len(post_calls) == 2
        meta2 = post_calls[1].get("metadata")
        assert isinstance(meta2, dict), f"Expected metadata dict on manual post, got {meta2}"
        assert meta2.get("event_type") == "yama_reply"
        payload2 = meta2.get("event_payload")
        assert isinstance(payload2, dict) and "request_id" in payload2 and "event_id" not in payload2, (
            f"Manual post metadata must contain request_id (and no event_id): {meta2}"
        )
        parsed_uuid = uuid.UUID(str(payload2["request_id"]))
        assert str(parsed_uuid) == payload2["request_id"]
        assert len(history_calls) == 1
        assert history_calls[0].get("include_all_metadata") is True, (
            f"Pre-post channel check must specify include_all_metadata=True: {history_calls[0]}"
        )

        # (3) Delayed retry pre-post check (_pre_post_check_thread_for_reply) -> include_all_metadata=True
        stdout3, stderr3 = io.StringIO(), io.StringIO()
        with patch.object(agent_tools, "get_web_client", return_value=client), redirect_stdout(
            stdout3
        ), redirect_stderr(stderr3):
            rc3 = agent_tools.main(
                [
                    "--db", str(db_path),
                    "reply",
                    "--channel", channel_id,
                    "--text-file", str(text_file),
                    "--thread-ts", thread_ts,
                    "--event-id", ev_delayed_id,
                ]
            )
        assert rc3 == 0
        assert len(replies_calls) == 2
        assert replies_calls[1].get("include_all_metadata") is True, (
            f"Delayed retry pre-post check must specify include_all_metadata=True: {replies_calls[1]}"
        )

        # (4) Reconcile scan (_scan_thread_yama_messages) -> include_all_metadata=True
        rec_summary = agent_tools.reconcile_replies(
            db_path=db_path,
            client=client,
            bot_user_id=bot_uid,
        )
        assert rec_summary["ok"] is True
        assert len(replies_calls) == 3
        assert replies_calls[2].get("include_all_metadata") is True, (
            f"reconcile_replies thread scan must specify include_all_metadata=True: {replies_calls[2]}"
        )

        return "chat.postMessageへのmetadata(event_type=yama_reply + event_id/request_id)付与と全履歴読み取り経路でのinclude_all_metadata=True指定を確認"


def test_recovery_and_reconcile_by_metadata_match() -> str:
    """47. メタデータ一致による着地回収（文面整形差異時＆優先順位1位）:
    (1) 応答ロスト後の回復確認で、Slack側の文面整形により送信文面と不一致でも metadata.event_payload.event_id 一致で回収できる、
    (2) event_idなし手動投稿の応答ロストでも metadata.event_payload.request_id 一致で回収できる、
    (3) reconcile（Phase 1 / Phase 2）で、文面が整形変化していても metadata 一致で回収でき、かつ同一スレッド内の文面一致のみの別投稿より metadata 一致が最優先されることを確認"""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "slack_events.db"
        init_db(db_path)
        agent_tools.init_db(db_path)

        bot_uid = "U_YAMA_BOT"
        channel_id = "C_ALL"
        thread_ts = "1798100000.000100"
        base_now = 1798100200.0
        ev_recover = "Ev_META_RECOVER_1"

        with get_db_connection(db_path) as conn:
            conn.execute(
                """
                INSERT INTO events (
                    event_id, channel_id, user_id, text, ts, thread_ts, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (ev_recover, channel_id, "U_ASKER", "URL教えて", "1798100100.000100", thread_ts, "PROCESSING"),
            )
            conn.commit()

        raw_text = "詳細は https://example.com/docs を参照してください。"
        # Slack reformats URLs into <https://example.com/docs|example.com/docs>, so text match alone would fail
        slack_reformatted_text = "詳細は <https://example.com/docs|example.com/docs> を参照してください。"
        text_file = Path(tmp) / "reply_reformat.txt"
        text_file.write_text(raw_text + "\n", encoding="utf-8")

        landed_meta_ts = f"{base_now + 1.5:.6f}"
        post_landed = {"value": False}
        captured_metadata: List[Dict[str, Any]] = []

        class ReformattedMetadataRecoveryClient:
            def auth_test(self) -> Dict[str, Any]:
                return {"ok": True, "user_id": bot_uid}

            def chat_postMessage(self, **kwargs: Any) -> Dict[str, Any]:
                captured_metadata.append(kwargs.get("metadata"))
                post_landed["value"] = True
                raise http.client.IncompleteRead(b"dropped-after-landing", 64)

            def conversations_replies(self, **kwargs: Any) -> Dict[str, Any]:
                assert kwargs.get("include_all_metadata") is True
                if not post_landed["value"]:
                    return {
                        "ok": True,
                        "messages": [{"user": "U_ASKER", "text": "URL教えて", "ts": thread_ts}],
                        "has_more": False,
                    }
                return {
                    "ok": True,
                    "messages": [
                        {"user": "U_ASKER", "text": "URL教えて", "ts": thread_ts},
                        {
                            "user": bot_uid,
                            "text": slack_reformatted_text,
                            "ts": landed_meta_ts,
                            "thread_ts": thread_ts,
                            "metadata": {
                                "event_type": "yama_reply",
                                "event_payload": {"event_id": ev_recover},
                            },
                        },
                    ],
                    "has_more": False,
                }

        stdout1, stderr1 = io.StringIO(), io.StringIO()
        with patch.object(
            agent_tools, "get_web_client", return_value=ReformattedMetadataRecoveryClient()
        ), patch.object(agent_tools.time, "time", return_value=base_now), redirect_stdout(
            stdout1
        ), redirect_stderr(stderr1):
            rc1 = agent_tools.main(
                [
                    "--db", str(db_path),
                    "reply",
                    "--channel", channel_id,
                    "--text-file", str(text_file),
                    "--thread-ts", thread_ts,
                    "--event-id", ev_recover,
                ]
            )

        assert rc1 == 0, f"Expected rc=0 via metadata recovery, got {rc1}, stderr={stderr1.getvalue()}"
        out1 = json.loads(stdout1.getvalue())
        assert out1["ok"] is True
        assert out1["recovered_after_ambiguous_error"] is True
        assert out1["ts"] == landed_meta_ts
        rec1 = get_recorded_reply(ev_recover, db_path=db_path)
        assert rec1 is not None and rec1["reply_ts"] == landed_meta_ts

        # (2) Manual post (no event_id) lost response recovery via request_id in metadata
        with get_db_connection(db_path) as conn:
            conn.execute("DELETE FROM replies")
            conn.commit()
        manual_landed = {"value": False, "request_id": ""}
        manual_landed_ts = f"{base_now + 3.0:.6f}"

        class ManualRequestIdRecoveryClient:
            def auth_test(self) -> Dict[str, Any]:
                return {"ok": True, "user_id": bot_uid}

            def chat_postMessage(self, **kwargs: Any) -> Dict[str, Any]:
                meta = kwargs.get("metadata") or {}
                manual_landed["request_id"] = (meta.get("event_payload") or {}).get("request_id", "")
                manual_landed["value"] = True
                raise http.client.RemoteDisconnected("manual-post-disconnected")

            def conversations_replies(self, **kwargs: Any) -> Dict[str, Any]:
                if not manual_landed["value"]:
                    return {"ok": True, "messages": [], "has_more": False}
                return {
                    "ok": True,
                    "messages": [
                        {
                            "user": bot_uid,
                            "text": slack_reformatted_text,
                            "ts": manual_landed_ts,
                            "thread_ts": thread_ts,
                            "metadata": {
                                "event_type": "yama_reply",
                                "event_payload": {"request_id": manual_landed["request_id"]},
                            },
                        }
                    ],
                    "has_more": False,
                }

        stdout2, stderr2 = io.StringIO(), io.StringIO()
        with patch.object(
            agent_tools, "get_web_client", return_value=ManualRequestIdRecoveryClient()
        ), patch.object(agent_tools.time, "time", return_value=base_now), redirect_stdout(
            stdout2
        ), redirect_stderr(stderr2):
            rc2 = agent_tools.main(
                [
                    "--db", str(db_path),
                    "reply",
                    "--channel", channel_id,
                    "--text-file", str(text_file),
                    "--thread-ts", thread_ts,
                    "--no-event-id",
                ]
            )
        assert rc2 == 0, f"Expected rc=0 via request_id metadata recovery, got {rc2}"
        out2 = json.loads(stdout2.getvalue())
        assert out2["ok"] is True
        assert out2["recovered_after_ambiguous_error"] is True
        assert out2["ts"] == manual_landed_ts

        # (3) Reconcile (Phase 1 & Phase 2): metadata match succeeds despite text change AND takes Priority 1 over text match
        now_dt = datetime.now(timezone.utc)
        recent_ts_str = (now_dt - timedelta(minutes=30)).strftime("%Y-%m-%d %H:%M:%S")
        th_rec_p1 = "1798200000.000100"
        th_rec_p2 = "1798300000.000100"
        ev_rec_p1 = "Ev_RECONCILE_META_P1"
        ev_rec_p2 = "Ev_RECONCILE_META_P2"
        attempt_p1 = 1798200100.0
        attempt_p2 = 1798300100.0
        ts_p1_text_only = f"{attempt_p1 + 5.0:.6f}"
        ts_p1_meta_match = f"{attempt_p1 + 15.0:.6f}"
        ts_p2_meta_match = f"{attempt_p2 + 10.0:.6f}"

        with get_db_connection(db_path) as conn:
            conn.executemany(
                """
                INSERT INTO events (
                    event_id, channel_id, user_id, text, ts, thread_ts,
                    status, retry_count, error_message, intended_text, attempt_ts,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        ev_rec_p1,
                        channel_id,
                        "U_ASKER",
                        "質問P1",
                        "1798200050.000100",
                        th_rec_p1,
                        "FAILED",
                        3,
                        "ambiguous_post_outcome",
                        "定型文：確認して折り返します。",
                        f"{attempt_p1:.6f}",
                        recent_ts_str,
                        recent_ts_str,
                    ),
                    (
                        ev_rec_p2,
                        channel_id,
                        "U_ASKER",
                        "質問P2",
                        "1798300050.000100",
                        th_rec_p2,
                        "DONE",
                        1,
                        "residual_timeout_error",
                        "元の送信テキスト（リンク https://example.org あり）",
                        f"{attempt_p2:.6f}",
                        recent_ts_str,
                        recent_ts_str,
                    ),
                ],
            )
            conn.commit()

        class ReconcileMetadataPriorityClient:
            def auth_test(self) -> Dict[str, Any]:
                return {"ok": True, "user_id": bot_uid}

            def conversations_replies(self, **kwargs: Any) -> Dict[str, Any]:
                assert kwargs.get("include_all_metadata") is True
                ts = kwargs.get("ts")
                if ts == th_rec_p1:
                    return {
                        "ok": True,
                        "messages": [
                            # Earlier message has exact matching text, but no metadata (or from another context)
                            {
                                "user": bot_uid,
                                "text": "定型文：確認して折り返します。",
                                "ts": ts_p1_text_only,
                                "thread_ts": th_rec_p1,
                            },
                            # Later message has reformatted text, but exact metadata match for ev_rec_p1 -> Priority 1!
                            {
                                "user": bot_uid,
                                "text": "定型文：確認して折り返します！（整形後）",
                                "ts": ts_p1_meta_match,
                                "thread_ts": th_rec_p1,
                                "metadata": {
                                    "event_type": "yama_reply",
                                    "event_payload": {"event_id": ev_rec_p1},
                                },
                            },
                        ],
                        "has_more": False,
                    }
                if ts == th_rec_p2:
                    return {
                        "ok": True,
                        "messages": [
                            {
                                "user": bot_uid,
                                "text": "元の送信テキスト（リンク <https://example.org> あり）",
                                "ts": ts_p2_meta_match,
                                "thread_ts": th_rec_p2,
                                "metadata": {
                                    "event_type": "yama_reply",
                                    "event_payload": {"event_id": ev_rec_p2},
                                },
                            }
                        ],
                        "has_more": False,
                    }
                return {"ok": True, "messages": [], "has_more": False}

        summary = agent_tools.reconcile_replies(
            db_path=db_path,
            client=ReconcileMetadataPriorityClient(),
            bot_user_id=bot_uid,
        )
        assert summary["ok"] is True
        assert len(summary["recorded"]) == 2
        assert summary["done_error_cleared"]["with_reply"] == [ev_rec_p2]

        rec_p1 = get_recorded_reply(ev_rec_p1, db_path=db_path)
        assert rec_p1 is not None
        assert rec_p1["reply_ts"] == ts_p1_meta_match, (
            f"Priority 1 metadata match ({ts_p1_meta_match}) must win over text-only match ({ts_p1_text_only}), got {rec_p1['reply_ts']}"
        )

        rec_p2 = get_recorded_reply(ev_rec_p2, db_path=db_path)
        assert rec_p2 is not None
        assert rec_p2["reply_ts"] == ts_p2_meta_match

        return "応答ロスト回復およびreconcile(Phase1/2)で文面整形差異があってもmetadata(event_id/request_id)一致で回収でき、文面一致より優先されることを確認"


def test_recovery_and_reconcile_attempt_window_boundaries() -> str:
    """48. 試行時刻の窓（前3分〜後5分）の境界制御:
    (1) 応答ロスト後の回復確認で [attempt_ts - 180s, attempt_ts + 300s] が oldest/latest に指定され、窓の外（-181s / +301s）にある同一メタデータの投稿は回収対象にしない、
    (2) reconcile でもイベントの attempt_ts の前後（前3分〜後5分）で走査し、窓の外にある同一メタデータの投稿は回収せず、窓の境界内（-180s / +300s）は回収できることを確認"""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "slack_events.db"
        init_db(db_path)
        agent_tools.init_db(db_path)

        bot_uid = "U_YAMA_BOT"
        channel_id = "C_ALL"
        thread_ts = "1798400000.000100"
        attempt_now = 1798401000.0
        expected_oldest = f"{attempt_now - 180.0:.6f}"
        expected_latest = f"{attempt_now + 300.0:.6f}"
        ev_out_of_win = "Ev_WIN_OUTSIDE_1"

        with get_db_connection(db_path) as conn:
            conn.execute(
                """
                INSERT INTO events (
                    event_id, channel_id, user_id, text, ts, thread_ts, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (ev_out_of_win, channel_id, "U_ASKER", "窓境界テスト", "1798400500.000100", thread_ts, "PROCESSING"),
            )
            conn.commit()

        reply_body = "試行時刻窓テストの本文です。"
        text_file = Path(tmp) / "reply_win_bound.txt"
        text_file.write_text(reply_body + "\n", encoding="utf-8")

        post_attempted = {"value": False}
        recovery_calls: List[Dict[str, Any]] = []

        # Messages outside the [attempt_now - 180s, attempt_now + 300s] window with identical metadata & text
        ts_before_win = f"{attempt_now - 180.001:.6f}"
        ts_after_win = f"{attempt_now + 300.001:.6f}"

        class OutsideWindowRecoveryClient:
            def auth_test(self) -> Dict[str, Any]:
                return {"ok": True, "user_id": bot_uid}

            def chat_postMessage(self, **kwargs: Any) -> Dict[str, Any]:
                post_attempted["value"] = True
                raise http.client.IncompleteRead(b"dropped", 32)

            def conversations_replies(self, **kwargs: Any) -> Dict[str, Any]:
                if not post_attempted["value"]:
                    return {"ok": True, "messages": [], "has_more": False}
                recovery_calls.append(kwargs)
                return {
                    "ok": True,
                    "messages": [
                        {
                            "user": bot_uid,
                            "text": reply_body,
                            "ts": ts_before_win,
                            "thread_ts": thread_ts,
                            "metadata": {
                                "event_type": "yama_reply",
                                "event_payload": {"event_id": ev_out_of_win},
                            },
                        },
                        {
                            "user": bot_uid,
                            "text": reply_body,
                            "ts": ts_after_win,
                            "thread_ts": thread_ts,
                            "metadata": {
                                "event_type": "yama_reply",
                                "event_payload": {"event_id": ev_out_of_win},
                            },
                        },
                    ],
                    "has_more": False,
                }

        stdout1, stderr1 = io.StringIO(), io.StringIO()
        with patch.object(
            agent_tools, "get_web_client", return_value=OutsideWindowRecoveryClient()
        ), patch.object(agent_tools.time, "time", return_value=attempt_now), patch.object(
            agent_tools.time, "sleep", side_effect=lambda s: None
        ), redirect_stdout(
            stdout1
        ), redirect_stderr(stderr1):
            rc1 = agent_tools.main(
                [
                    "--db", str(db_path),
                    "reply",
                    "--channel", channel_id,
                    "--text-file", str(text_file),
                    "--thread-ts", thread_ts,
                    "--event-id", ev_out_of_win,
                ]
            )

        assert rc1 == 1, f"Expected rc=1 when matching metadata is outside window, got {rc1}"
        assert len(recovery_calls) == 1
        assert recovery_calls[0].get("oldest") == expected_oldest
        assert recovery_calls[0].get("latest") == expected_latest
        assert get_recorded_reply(ev_out_of_win, db_path=db_path) is None
        err1 = json.loads(stderr1.getvalue())
        assert err1["error"] == "ambiguous_post_outcome"

        # Verify attempt_ts was recorded on ev_out_of_win
        ev_row = agent_tools.get_event_by_id(ev_out_of_win, db_path=db_path)
        assert ev_row is not None and ev_row["attempt_ts"] == f"{attempt_now:.6f}"

        # (2) Reconcile window test:
        #   - ev_out_of_win has messages only at attempt_now - 180.001s and attempt_now + 300.001s -> must NOT be reconciled
        #   - ev_in_win has a message right on the +300.0s boundary -> MUST be reconciled
        th_in_win = "1798500000.000100"
        ev_in_win = "Ev_WIN_INSIDE_BOUNDARY"
        attempt_in = 1798501000.0
        ts_on_upper_boundary = f"{attempt_in + 300.0:.6f}"
        now_dt = datetime.now(timezone.utc)
        recent_ts_str = (now_dt - timedelta(minutes=20)).strftime("%Y-%m-%d %H:%M:%S")

        with get_db_connection(db_path) as conn:
            conn.execute(
                "UPDATE events SET status = 'FAILED', error_message = 'ambiguous_post_outcome', created_at = ?, updated_at = ? WHERE event_id = ?",
                (recent_ts_str, recent_ts_str, ev_out_of_win),
            )
            conn.execute(
                """
                INSERT INTO events (
                    event_id, channel_id, user_id, text, ts, thread_ts,
                    status, retry_count, error_message, intended_text, attempt_ts,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ev_in_win,
                    channel_id,
                    "U_ASKER",
                    "境界内テスト",
                    "1798500500.000100",
                    th_in_win,
                    "FAILED",
                    3,
                    "ambiguous_post_outcome",
                    reply_body,
                    f"{attempt_in:.6f}",
                    recent_ts_str,
                    recent_ts_str,
                ),
            )
            conn.commit()

        reconcile_ReplyCalls: List[Dict[str, Any]] = []

        class ReconcileWindowClient:
            def auth_test(self) -> Dict[str, Any]:
                return {"ok": True, "user_id": bot_uid}

            def conversations_replies(self, **kwargs: Any) -> Dict[str, Any]:
                reconcile_ReplyCalls.append(kwargs)
                ts = kwargs.get("ts")
                if ts == thread_ts:
                    assert kwargs.get("oldest") == expected_oldest
                    assert kwargs.get("latest") == expected_latest
                    return {
                        "ok": True,
                        "messages": [
                            {
                                "user": bot_uid,
                                "text": reply_body,
                                "ts": ts_before_win,
                                "thread_ts": thread_ts,
                                "metadata": {
                                    "event_type": "yama_reply",
                                    "event_payload": {"event_id": ev_out_of_win},
                                },
                            },
                            {
                                "user": bot_uid,
                                "text": reply_body,
                                "ts": ts_after_win,
                                "thread_ts": thread_ts,
                                "metadata": {
                                    "event_type": "yama_reply",
                                    "event_payload": {"event_id": ev_out_of_win},
                                },
                            },
                        ],
                        "has_more": False,
                    }
                if ts == th_in_win:
                    assert kwargs.get("oldest") == f"{attempt_in - 180.0:.6f}"
                    assert kwargs.get("latest") == f"{attempt_in + 300.0:.6f}"
                    return {
                        "ok": True,
                        "messages": [
                            {
                                "user": bot_uid,
                                "text": reply_body,
                                "ts": ts_on_upper_boundary,
                                "thread_ts": th_in_win,
                                "metadata": {
                                    "event_type": "yama_reply",
                                    "event_payload": {"event_id": ev_in_win},
                                },
                            }
                        ],
                        "has_more": False,
                    }
                return {"ok": True, "messages": [], "has_more": False}

        summary = agent_tools.reconcile_replies(
            db_path=db_path,
            client=ReconcileWindowClient(),
            bot_user_id=bot_uid,
        )
        assert summary["ok"] is True
        assert summary["recorded"] == [{"event_id": ev_in_win, "reply_ts": ts_on_upper_boundary}]
        assert summary["not_found"] == [ev_out_of_win]
        assert get_recorded_reply(ev_out_of_win, db_path=db_path) is None, (
            "Post outside [attempt_ts - 3m, attempt_ts + 5m] must NOT be recovered even with matching metadata"
        )
        rec_in = get_recorded_reply(ev_in_win, db_path=db_path)
        assert rec_in is not None and rec_in["reply_ts"] == ts_on_upper_boundary

        return "試行時刻の窓(前3分〜後5分)でoldest/latestが指定され、窓外の同一metadata投稿は回収せず窓内(境界含む)のみ回収することを確認"


def test_legacy_post_without_metadata_text_fallback() -> str:
    """49. メタデータのない旧投稿の後方互換（第2優先：文面一致）と別イベントメタデータ衝突除外:
    (1) metadata を持たない旧投稿でも intended_text と文面一致すれば従来どおり回収できる、
    (2) 別 event_id の metadata が付いた同一文面（定型文など）は文面一致フォールバックで誤回収しない、
    (3) metadata がなく文面も不一致の投稿は推定回収しないことを確認"""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "slack_events.db"
        init_db(db_path)
        agent_tools.init_db(db_path)

        bot_uid = "U_YAMA_BOT"
        channel_id = "C_ALL"
        now_dt = datetime.now(timezone.utc)
        recent_ts_str = (now_dt - timedelta(minutes=25)).strftime("%Y-%m-%d %H:%M:%S")

        th_legacy_ok = "1798600000.000100"
        th_conflict_meta = "1798700000.000100"
        ev_legacy_ok = "Ev_LEGACY_TEXT_OK"
        ev_conflict_meta = "Ev_CONFLICT_META_SKIP"
        attempt_legacy = 1798600100.0
        attempt_conflict = 1798700100.0
        ts_legacy_reply = f"{attempt_legacy + 12.0:.6f}"
        ts_other_event_reply = f"{attempt_conflict + 8.0:.6f}"
        template_text = "了解しました。担当にて確認します。"

        with get_db_connection(db_path) as conn:
            conn.executemany(
                """
                INSERT INTO events (
                    event_id, channel_id, user_id, text, ts, thread_ts,
                    status, retry_count, error_message, intended_text, attempt_ts,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        ev_legacy_ok,
                        channel_id,
                        "U_ASKER",
                        "旧投稿テスト",
                        "1798600050.000100",
                        th_legacy_ok,
                        "FAILED",
                        3,
                        "ambiguous_post_outcome",
                        template_text,
                        f"{attempt_legacy:.6f}",
                        recent_ts_str,
                        recent_ts_str,
                    ),
                    (
                        ev_conflict_meta,
                        channel_id,
                        "U_ASKER",
                        "別イベント定型文衝突テスト",
                        "1798700050.000100",
                        th_conflict_meta,
                        "FAILED",
                        3,
                        "ambiguous_post_outcome",
                        template_text,
                        f"{attempt_conflict:.6f}",
                        recent_ts_str,
                        recent_ts_str,
                    ),
                ],
            )
            conn.commit()

        class LegacyAndConflictClient:
            def auth_test(self) -> Dict[str, Any]:
                return {"ok": True, "user_id": bot_uid}

            def conversations_replies(self, **kwargs: Any) -> Dict[str, Any]:
                ts = kwargs.get("ts")
                if ts == th_legacy_ok:
                    # Legacy message without metadata -> recovered by Priority 2 text match
                    return {
                        "ok": True,
                        "messages": [
                            {
                                "user": bot_uid,
                                "text": template_text,
                                "ts": ts_legacy_reply,
                                "thread_ts": th_legacy_ok,
                            }
                        ],
                        "has_more": False,
                    }
                if ts == th_conflict_meta:
                    # Identical template text within the window, but metadata belongs to ANOTHER event_id
                    return {
                        "ok": True,
                        "messages": [
                            {
                                "user": bot_uid,
                                "text": template_text,
                                "ts": ts_other_event_reply,
                                "thread_ts": th_conflict_meta,
                                "metadata": {
                                    "event_type": "yama_reply",
                                    "event_payload": {"event_id": "Ev_DIFFERENT_EVENT_999"},
                                },
                            }
                        ],
                        "has_more": False,
                    }
                return {"ok": True, "messages": [], "has_more": False}

        summary = agent_tools.reconcile_replies(
            db_path=db_path,
            client=LegacyAndConflictClient(),
            bot_user_id=bot_uid,
        )
        assert summary["ok"] is True
        assert summary["recorded"] == [{"event_id": ev_legacy_ok, "reply_ts": ts_legacy_reply}]
        assert summary["not_found"] == [ev_conflict_meta]

        rec_legacy = get_recorded_reply(ev_legacy_ok, db_path=db_path)
        assert rec_legacy is not None and rec_legacy["reply_ts"] == ts_legacy_reply

        rec_conflict = get_recorded_reply(ev_conflict_meta, db_path=db_path)
        assert rec_conflict is None, (
            "Message carrying metadata for a different event_id must NOT be matched by text fallback"
        )

        return "metadataなしの旧投稿は従来どおり文面一致(第2優先)で回収でき、別event_idのmetadataを持つ同一文面は誤回収しないことを確認"


def test_pre_post_thread_ledger_parroting_prevention() -> str:
    """50. 投稿前チェックの台帳（replies）照合による同一スレッド内 parroting 防止:
    (1) 同一スレッドで台帳の過去投稿と文面一致 → 送らない（deduplicated=True・台帳記録）、
    (2) 別スレッドで台帳の過去投稿と文面一致 → 通常どおり送る、
    (3) 正規化の差異だけの一致（前後空白・\\r\\n→\\n・&amp;/&lt;/&gt;デコード） → 送らない、
    (4) 台帳に文面が記録されていない（NULL/空白のみ）過去投稿 → 照合対象外として通常どおり送る、
    (5) チャンネル直投稿（thread_tsなし）は対象外として送ることを確認"""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "slack_events.db"
        init_db(db_path)
        agent_tools.init_db(db_path)

        bot_uid = "U_YAMA_BOT"
        channel_id = "C_ALL"

        thread_same = "1799000000.000100"
        thread_other = "1799100000.000100"
        thread_norm = "1799200000.000100"
        thread_no_text = "1799300000.000100"

        past_same_reply_ts = "1799000010.000100"
        past_norm_reply_ts = "1799200020.000200"
        past_null_reply_ts = "1799300010.000100"
        past_blank_reply_ts = "1799300020.000200"

        same_text = "了解しました。担当にてログを確認します。"
        norm_ledger_text = "  条件チェック: A &amp; B &lt; C &gt; D\r\n2行目の確認結果です。  \r\n"
        norm_input_text = "条件チェック: A & B < C > D\n2行目の確認結果です。"
        no_text_input = "台帳に文面が未記録のスレッドへの新規返信です。"

        # Record past replies in the ledger (outside the 60s Slack window of new events)
        agent_tools.record_reply(
            event_id="Ev50_PAST_SAME_1",
            channel_id=channel_id,
            thread_ts=thread_same,
            reply_ts=past_same_reply_ts,
            text=same_text,
            db_path=db_path,
        )
        agent_tools.record_reply(
            event_id="Ev50_PAST_NORM_1",
            channel_id=channel_id,
            thread_ts=thread_norm,
            reply_ts=past_norm_reply_ts,
            text=norm_ledger_text,
            db_path=db_path,
        )
        with get_db_connection(db_path) as conn:
            conn.executemany(
                """
                INSERT INTO replies (event_id, channel_id, thread_ts, reply_ts, text_hash, text)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                [
                    ("Ev50_PAST_NULL_TEXT", channel_id, thread_no_text, past_null_reply_ts, "hash_null", None),
                    ("Ev50_PAST_BLANK_TEXT", channel_id, thread_no_text, past_blank_reply_ts, "hash_blank", "   \r\n  "),
                    ("Ev50_PAST_CHANNEL_DIRECT", channel_id, None, "1799000005.000100", "hash_ch", same_text),
                ],
            )
            conn.executemany(
                """
                INSERT INTO events (event_id, channel_id, user_id, text, ts, thread_ts, status)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    ("Ev50_NEW_SAME", channel_id, "U_ASKER", "再確認です", "1799003600.000100", thread_same, "PROCESSING"),
                    ("Ev50_NEW_OTHER", channel_id, "U_ASKER", "別スレッド質問", "1799103600.000100", thread_other, "PROCESSING"),
                    ("Ev50_NEW_NORM", channel_id, "U_ASKER", "条件の再確認", "1799203600.000100", thread_norm, "PROCESSING"),
                    ("Ev50_NEW_NOTEXT", channel_id, "U_ASKER", "文面なし台帳スレッド", "1799303600.000100", thread_no_text, "PROCESSING"),
                ],
            )
            conn.commit()

        post_calls: List[Dict[str, Any]] = []

        class LedgerParrotTestClient:
            def auth_test(self) -> Dict[str, Any]:
                return {"ok": True, "user_id": bot_uid}

            def conversations_replies(self, **kwargs: Any) -> Dict[str, Any]:
                # 60s window in Slack contains no past Yama reply
                return {
                    "ok": True,
                    "messages": [
                        {"user": "U_ASKER", "text": "trigger message", "ts": kwargs.get("oldest") or kwargs.get("ts")}
                    ],
                    "has_more": False,
                }

            def conversations_history(self, **kwargs: Any) -> Dict[str, Any]:
                return {"ok": True, "messages": [], "has_more": False}

            def chat_postMessage(self, **kwargs: Any) -> Dict[str, Any]:
                post_calls.append(kwargs)
                posted_ts = f"179990000{len(post_calls)}.000100"
                return {
                    "ok": True,
                    "channel": kwargs["channel"],
                    "ts": posted_ts,
                    "message": {"text": kwargs["text"], "ts": posted_ts},
                }

        client = LedgerParrotTestClient()

        # (1) Same thread + matching text in ledger -> do NOT send (parroting prevented)
        file_same = Path(tmp) / "reply_same.txt"
        file_same.write_text(same_text + "\n", encoding="utf-8")
        stdout1, stderr1 = io.StringIO(), io.StringIO()
        with patch.object(agent_tools, "get_web_client", return_value=client), redirect_stdout(
            stdout1
        ), redirect_stderr(stderr1):
            rc1 = agent_tools.main(
                [
                    "--db", str(db_path),
                    "reply",
                    "--channel", channel_id,
                    "--text-file", str(file_same),
                    "--thread-ts", thread_same,
                    "--event-id", "Ev50_NEW_SAME",
                ]
            )
        assert rc1 == 0, f"Expected rc=0, got {rc1}, stderr={stderr1.getvalue()}"
        assert len(post_calls) == 0, f"Same-thread parroting must NOT post, got {len(post_calls)} posts"
        out1 = json.loads(stdout1.getvalue())
        assert out1["ok"] is True
        assert out1["deduplicated"] is True
        assert out1["ts"] == past_same_reply_ts
        assert out1["thread_ts"] == thread_same
        assert out1["event_id"] == "Ev50_NEW_SAME"
        rec_same = get_recorded_reply("Ev50_NEW_SAME", db_path=db_path)
        assert rec_same is not None and rec_same["reply_ts"] == past_same_reply_ts

        # (2) Different thread + matching text in ledger -> send normally
        stdout2, stderr2 = io.StringIO(), io.StringIO()
        with patch.object(agent_tools, "get_web_client", return_value=client), redirect_stdout(
            stdout2
        ), redirect_stderr(stderr2):
            rc2 = agent_tools.main(
                [
                    "--db", str(db_path),
                    "reply",
                    "--channel", channel_id,
                    "--text-file", str(file_same),
                    "--thread-ts", thread_other,
                    "--event-id", "Ev50_NEW_OTHER",
                ]
            )
        assert rc2 == 0, f"Expected rc=0, got {rc2}, stderr={stderr2.getvalue()}"
        assert len(post_calls) == 1, f"Different thread with same text must post normally, got {len(post_calls)}"
        out2 = json.loads(stdout2.getvalue())
        assert out2["ok"] is True
        assert out2["result"] == "posted"
        assert out2["deduplicated"] is False
        assert out2["ts"] == "1799900001.000100"

        # (3) Same thread + normalization differences only (whitespace, \r\n->\n, &amp;/&lt;/&gt;) -> do NOT send
        file_norm = Path(tmp) / "reply_norm.txt"
        file_norm.write_text(norm_input_text, encoding="utf-8")
        stdout3, stderr3 = io.StringIO(), io.StringIO()
        with patch.object(agent_tools, "get_web_client", return_value=client), redirect_stdout(
            stdout3
        ), redirect_stderr(stderr3):
            rc3 = agent_tools.main(
                [
                    "--db", str(db_path),
                    "reply",
                    "--channel", channel_id,
                    "--text-file", str(file_norm),
                    "--thread-ts", thread_norm,
                    "--event-id", "Ev50_NEW_NORM",
                ]
            )
        assert rc3 == 0, f"Expected rc=0, got {rc3}, stderr={stderr3.getvalue()}"
        assert len(post_calls) == 1, "Normalized text match in same thread must NOT post"
        out3 = json.loads(stdout3.getvalue())
        assert out3["ok"] is True
        assert out3["deduplicated"] is True
        assert out3["ts"] == past_norm_reply_ts
        assert out3["thread_ts"] == thread_norm
        assert out3["event_id"] == "Ev50_NEW_NORM"
        rec_norm = get_recorded_reply("Ev50_NEW_NORM", db_path=db_path)
        assert rec_norm is not None and rec_norm["reply_ts"] == past_norm_reply_ts

        # (4) Same thread + past ledger rows have no recorded text (NULL / whitespace-only) -> send normally
        file_no_text = Path(tmp) / "reply_no_text.txt"
        file_no_text.write_text(no_text_input + "\n", encoding="utf-8")
        stdout4, stderr4 = io.StringIO(), io.StringIO()
        with patch.object(agent_tools, "get_web_client", return_value=client), redirect_stdout(
            stdout4
        ), redirect_stderr(stderr4):
            rc4 = agent_tools.main(
                [
                    "--db", str(db_path),
                    "reply",
                    "--channel", channel_id,
                    "--text-file", str(file_no_text),
                    "--thread-ts", thread_no_text,
                    "--event-id", "Ev50_NEW_NOTEXT",
                ]
            )
        assert rc4 == 0, f"Expected rc=0, got {rc4}, stderr={stderr4.getvalue()}"
        assert len(post_calls) == 2, "Unrecorded text (NULL/blank) in ledger must NOT suppress posting"
        out4 = json.loads(stdout4.getvalue())
        assert out4["ok"] is True
        assert out4["result"] == "posted"
        assert out4["deduplicated"] is False
        assert out4["ts"] == "1799900002.000100"

        # (5) Channel direct post (no --thread-ts) is out of scope for ledger check -> send normally
        stdout5, stderr5 = io.StringIO(), io.StringIO()
        with patch.object(agent_tools, "get_web_client", return_value=client), redirect_stdout(
            stdout5
        ), redirect_stderr(stderr5):
            rc5 = agent_tools.main(
                [
                    "--db", str(db_path),
                    "reply",
                    "--channel", channel_id,
                    "--text-file", str(file_same),
                    "--event-id", "Ev50_NEW_CHANNEL_DIRECT",
                    "--force",
                    "--reason",
                    "test fixture: events row intentionally absent (pre-candidate-28 call shape)",
                ]
            )
        assert rc5 == 0, f"Expected rc=0, got {rc5}, stderr={stderr5.getvalue()}"
        assert len(post_calls) == 3, "Channel direct post must not be suppressed by thread ledger check"
        out5 = json.loads(stdout5.getvalue())
        assert out5["ok"] is True
        assert out5["result"] == "posted"
        assert out5["deduplicated"] is False

        return "同一スレッド一致は送信抑止(deduplicated=True)・別スレッド一致は送信・正規化差異(空白/CRLF/HTMLエスケープ)のみの一致は送信抑止・台帳文面なし(NULL/空白)およびチャンネル直投稿は送信されることを確認"


def test_delayed_retry_answered_check_single_page_cap() -> str:
    """51. 遅延再送の答え済み確認1ページ上限: 起点(event_ts-60s)から最大100件を1ページだけ読み、(A)ページ内に条件を満たす回答があれば後続があっても skipped_answered、(B)回答がなく後続もなければ送信、(C)回答がなく後続がある場合は確認未完として pre_post_check_unverifiable で送信しない(fail-closed)。いずれも conversations_replies の読み取りは1回だけ"""
    bot_uid = "U_YAMA_BOT"
    channel_id = "C_ALL"
    thread_ts = "1795200000.000100"
    questioner_uid = "U_QUESTIONER"
    answerer_uid = "U_OTHER_HUMAN"
    answer_ts = "1795200045.000200"
    answer_text = "そのエラーは config/.env の権限設定を確認すると直りますよ。"
    filler_ts = "1795200030.000150"

    def run_case(messages: List[Dict[str, Any]], has_more: bool, next_cursor: Any) -> Any:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "slack_events.db"
            init_db(db_path)
            ev_id = "Ev_DELAYED_PAGECAP_1"
            with get_db_connection(db_path) as conn:
                conn.execute(
                    """
                    INSERT INTO events (
                        event_id, channel_id, user_id, text, ts, thread_ts,
                        status, retry_count, error_message, next_retry_at, first_unverifiable_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        ev_id,
                        channel_id,
                        questioner_uid,
                        "エラーの原因が分かりません",
                        thread_ts,
                        None,
                        "PROCESSING",
                        0,
                        "pre_post_check_unverifiable",
                        None,
                        "2026-11-14 09:00:00",
                    ),
                )
                conn.commit()
            text_file = Path(tmp) / "reply.txt"
            text_file.write_text("Yamaからの遅延回答です。\n", encoding="utf-8")
            reply_calls: List[Dict[str, Any]] = []
            post_calls: List[Dict[str, Any]] = []

            class PagedClient:
                def auth_test(self) -> Dict[str, Any]:
                    return {"ok": True, "user_id": bot_uid}

                def conversations_replies(self, **kwargs: Any) -> Dict[str, Any]:
                    reply_calls.append(kwargs)
                    resp: Dict[str, Any] = {
                        "ok": True,
                        "messages": messages,
                        "has_more": has_more,
                    }
                    if next_cursor:
                        resp["response_metadata"] = {"next_cursor": next_cursor}
                    return resp

                def chat_postMessage(self, **kwargs: Any) -> Dict[str, Any]:
                    post_calls.append(kwargs)
                    return {"ok": True, "channel": channel_id, "ts": "1795203600.000300"}

            stdout_buf = io.StringIO()
            stderr_buf = io.StringIO()
            with patch.object(
                agent_tools, "get_web_client", return_value=PagedClient()
            ), redirect_stdout(stdout_buf), redirect_stderr(stderr_buf):
                rc = agent_tools.main(
                    [
                        "--db",
                        str(db_path),
                        "reply",
                        "--channel",
                        channel_id,
                        "--text-file",
                        str(text_file),
                        "--thread-ts",
                        thread_ts,
                        "--event-id",
                        ev_id,
                    ]
                )
            return rc, stdout_buf.getvalue(), stderr_buf.getvalue(), reply_calls, post_calls

    parent_msg = {"user": questioner_uid, "text": "エラーの原因が分かりません", "ts": thread_ts}
    answer_msg = {"user": answerer_uid, "text": answer_text, "ts": answer_ts, "thread_ts": thread_ts}
    filler_msg = {"user": answerer_uid, "text": "あ", "ts": filler_ts, "thread_ts": thread_ts}

    # (A) Answer inside the single page, more replies remain -> skipped_answered, exactly one read with limit=100
    rc_a, out_a, err_a, calls_a, posts_a = run_case([parent_msg, answer_msg], True, "cursor-2")
    assert rc_a == 0, f"A: expected rc=0, got {rc_a}, stderr={err_a}"
    assert len(posts_a) == 0, "A: must not post when answered"
    assert len(calls_a) == 1 and calls_a[0].get("limit") == 100, f"A: expected exactly one read with limit=100, got {calls_a}"
    out_a_json = json.loads(out_a)
    assert out_a_json["result"] == "skipped_answered", f"A: unexpected output {out_a_json}"

    # (B) No qualifying answer and no more replies -> resend proceeds
    rc_b, out_b, err_b, calls_b, posts_b = run_case([parent_msg, filler_msg], False, None)
    assert rc_b == 0, f"B: expected rc=0, got {rc_b}, stderr={err_b}"
    assert len(posts_b) == 1, "B: resend must be posted when not answered and scan complete"
    assert calls_b[0].get("limit") == 100, f"B: expected limit=100, got {calls_b[0]}"
    out_b_json = json.loads(out_b)
    assert out_b_json["ok"] is True and out_b_json["result"] == "posted", f"B: unexpected output {out_b_json}"

    # (C) No qualifying answer but more replies remain -> fail-closed unverifiable, no post, exactly one read
    rc_c, out_c, err_c, calls_c, posts_c = run_case([parent_msg, filler_msg], True, "cursor-2")
    assert rc_c == 1, f"C: expected rc=1, got {rc_c}, stdout={out_c}"
    assert len(posts_c) == 0, "C: must not post when the answered check is incomplete"
    assert len(calls_c) == 1 and calls_c[0].get("limit") == 100, f"C: expected exactly one read with limit=100, got {calls_c}"
    err_c_json = json.loads(err_c)
    assert err_c_json["error"] == "pre_post_check_unverifiable", f"C: unexpected error {err_c_json}"
    assert "answered_check_incomplete" in str(err_c_json.get("check_error") or ""), f"C: check_error must name the incomplete answered check: {err_c_json}"

    return "遅延再送の答え済み確認は起点から1ページ(最大100件)のみ読み、ページ内回答で抑止・後続なしで送信・後続あり未完はfail-closed(pre_post_check_unverifiable)になることを確認"


def test_reply_post_no_retry_on_timeout_and_server_error() -> str:
    """52. reply: chat.postMessage は再送しない。TimeoutError直撃時は1回のみPOST→回復確認の履歴読み取り→未着地ならambiguous_post_outcome＋intended_text保存、HTTP 500のSlackApiError時は1回のみPOST→回復確認に進まず即失敗"""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "slack_events.db"
        init_db(db_path)
        agent_tools.init_db(db_path)

        bot_uid = "U_YAMA_BOT"
        channel_id = "C_ALL"
        thread_ts = "1797000000.000100"
        base_now = 1797000190.5
        ev_timeout = "Ev_POST_TIMEOUT_1"
        ev_500 = "Ev_POST_HTTP500_1"

        with get_db_connection(db_path) as conn:
            conn.executemany(
                """
                INSERT INTO events (
                    event_id, channel_id, user_id, text, ts, thread_ts,
                    status, retry_count, error_message, next_retry_at, first_unverifiable_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (ev_timeout, channel_id, "U_ASKER", "Q1", "1797000001.000100", thread_ts, "PROCESSING", 0, None, None, None),
                    (ev_500, channel_id, "U_ASKER", "Q2", "1797000002.000100", thread_ts, "PROCESSING", 0, None, None, None),
                ],
            )
            conn.commit()

        reply_text = "再送しない設計の確認テスト本文です。"
        text_file = Path(tmp) / "reply_no_retry.txt"
        text_file.write_text(reply_text + "\n", encoding="utf-8")

        # --- Case A: TimeoutError hits chat_postMessage directly ---
        a_post_calls: List[Dict[str, Any]] = []
        a_replies_pre = {"count": 0}
        a_replies_recovery = {"count": 0}
        a_posted = {"value": False}

        class TimeoutPostClient:
            def auth_test(self) -> Dict[str, Any]:
                return {"ok": True, "user_id": bot_uid}

            def chat_postMessage(self, **kwargs: Any) -> Dict[str, Any]:
                a_post_calls.append(kwargs)
                a_posted["value"] = True
                raise TimeoutError("The read operation timed out")

            def conversations_replies(self, **kwargs: Any) -> Dict[str, Any]:
                if a_posted["value"]:
                    a_replies_recovery["count"] += 1
                else:
                    a_replies_pre["count"] += 1
                # Landing is never found: only the parent message exists
                return {
                    "ok": True,
                    "messages": [{"user": "U_ASKER", "text": "parent", "ts": thread_ts}],
                    "has_more": False,
                }

        stdout_a, stderr_a = io.StringIO(), io.StringIO()
        with patch.object(agent_tools, "get_web_client", return_value=TimeoutPostClient()), patch.object(
            agent_tools.time, "time", return_value=base_now
        ), patch.object(
            agent_tools.time, "sleep", side_effect=lambda s: None
        ), redirect_stdout(stdout_a), redirect_stderr(stderr_a):
            rc_a = agent_tools.main(
                [
                    "--db", str(db_path),
                    "reply",
                    "--channel", channel_id,
                    "--text-file", str(text_file),
                    "--thread-ts", thread_ts,
                    "--event-id", ev_timeout,
                ]
            )

        assert rc_a == 1, f"Case A: expected rc=1, got {rc_a}, stderr={stderr_a.getvalue()}"
        assert len(a_post_calls) == 1, f"Case A: expected exactly 1 chat_postMessage call, got {len(a_post_calls)}"
        assert a_replies_recovery["count"] >= 1, (
            f"Case A: recovery history read must be attempted after TimeoutError, got {a_replies_recovery['count']}"
        )
        err_a = json.loads(stderr_a.getvalue())
        assert err_a["ok"] is False
        assert err_a["error"] == "ambiguous_post_outcome", f"Case A: expected ambiguous_post_outcome, got {err_a}"
        row_a = agent_tools.get_event_by_id(ev_timeout, db_path=db_path)
        assert row_a is not None
        assert row_a["intended_text"] == reply_text, (
            f"Case A: expected intended_text={reply_text!r}, got {row_a['intended_text']!r}"
        )
        assert get_recorded_reply(ev_timeout, db_path=db_path) is None

        # --- Case B: HTTP 500 SlackApiError returned on chat_postMessage ---
        b_post_calls: List[Dict[str, Any]] = []
        b_replies_recovery = {"count": 0}
        b_posted = {"value": False}

        class ServerErrorPostClient:
            def auth_test(self) -> Dict[str, Any]:
                return {"ok": True, "user_id": bot_uid}

            def chat_postMessage(self, **kwargs: Any) -> Dict[str, Any]:
                b_post_calls.append(kwargs)
                b_posted["value"] = True
                raise SlackApiError(
                    message="internal_error",
                    response={"ok": False, "error": "internal_error", "status_code": 500},
                )

            def conversations_replies(self, **kwargs: Any) -> Dict[str, Any]:
                if b_posted["value"]:
                    b_replies_recovery["count"] += 1
                return {
                    "ok": True,
                    "messages": [{"user": "U_ASKER", "text": "parent", "ts": thread_ts}],
                    "has_more": False,
                }

        stdout_b, stderr_b = io.StringIO(), io.StringIO()
        with patch.object(agent_tools, "get_web_client", return_value=ServerErrorPostClient()), patch.object(
            agent_tools.time, "time", return_value=base_now
        ), patch.object(
            agent_tools.time, "sleep", side_effect=lambda s: None
        ), redirect_stdout(stdout_b), redirect_stderr(stderr_b):
            rc_b = agent_tools.main(
                [
                    "--db", str(db_path),
                    "reply",
                    "--channel", channel_id,
                    "--text-file", str(text_file),
                    "--thread-ts", thread_ts,
                    "--event-id", ev_500,
                ]
            )

        assert rc_b == 1, f"Case B: expected rc=1, got {rc_b}, stderr={stderr_b.getvalue()}"
        assert len(b_post_calls) == 1, f"Case B: expected exactly 1 chat_postMessage call, got {len(b_post_calls)}"
        assert b_replies_recovery["count"] == 0, (
            f"Case B: SlackApiError must fail immediately without recovery history read, got {b_replies_recovery['count']}"
        )
        err_b = json.loads(stderr_b.getvalue())
        assert err_b["ok"] is False
        assert err_b["error"] == "internal_error", f"Case B: expected error=internal_error, got {err_b}"
        row_b = agent_tools.get_event_by_id(ev_500, db_path=db_path)
        assert row_b is not None and row_b["intended_text"] is None

        return "TimeoutError直撃時はPOST1回のみ・回復確認履歴読み取り実施・未着地でambiguous_post_outcome＋intended_text保存、HTTP 500 SlackApiError時はPOST1回のみ・回復確認なしで即失敗(error=internal_error)を確認"


def test_auto_join_should_join_decision() -> str:
    """53. auto_join 参加判定: オーナー在籍＆未参加なら参加(オーナー1人でも)・オーナー不在/opted_out/参加済み/アーカイブは不参加"""
    owner = auto_join.OWNER_USER_ID
    assert owner == "U_OWNER"
    ch = {"id": "C_NEW", "name": "new", "is_member": False, "is_archived": False}

    # オーナーがメンバーで未参加 → 入る（オーナー1人だけでも入る）
    assert auto_join.should_join(ch, [], [owner]) is True, "owner-only channel must be joined"
    assert auto_join.should_join(ch, [], [owner, "U_OTHER"]) is True

    # オーナーがメンバーでない → 入らない
    assert auto_join.should_join(ch, [], ["U_OTHER", "U_OTHER2"]) is False
    assert auto_join.should_join(ch, [], []) is False

    # opted_out に含まれる → オーナーがメンバーでも入らない
    assert auto_join.should_join(ch, ["C_NEW"], [owner]) is False

    # 既に参加済み → 入らない
    joined = dict(ch, is_member=True)
    assert auto_join.should_join(joined, [], [owner]) is False

    # アーカイブ済み（防御的確認）→ 入らない
    archived = dict(ch, is_archived=True)
    assert auto_join.should_join(archived, [], [owner]) is False

    return "オーナー在籍＆未参加で参加(1人でも)・オーナー不在/opted_out/参加済み/アーカイブで不参加を確認"


def test_auto_join_state_transitions() -> str:
    """54. auto_join 状態遷移: 参加済み→未参加でopted_outへ移動・手動再参加でopted_out解除・未知の未参加chは変化なし・入力非破壊"""
    state0 = {"known_member": [], "opted_out": []}

    # 参加済みを観測 → known_member に入る
    s1, opted1, cleared1 = auto_join.update_membership_state(
        state0,
        [
            {"id": "C_A", "is_member": True},
            {"id": "C_B", "is_member": False},
        ],
    )
    assert s1 == {"known_member": ["C_A"], "opted_out": []}, s1
    assert opted1 == [] and cleared1 == []
    assert state0 == {"known_member": [], "opted_out": []}, "input state must not be mutated"

    # 参加済みだった C_A が未参加になった → opted_out に移る（C_B は known でないので変化なし）
    s2, opted2, cleared2 = auto_join.update_membership_state(
        s1,
        [
            {"id": "C_A", "is_member": False},
            {"id": "C_B", "is_member": False},
        ],
    )
    assert s2 == {"known_member": [], "opted_out": ["C_A"]}, s2
    assert opted2 == ["C_A"] and cleared2 == []

    # opted_out になった C_A は、オーナーがいても参加対象外
    ch_a = {"id": "C_A", "name": "a", "is_member": False, "is_archived": False}
    assert auto_join.should_join(ch_a, s2["opted_out"], [auto_join.OWNER_USER_ID]) is False

    # 同じ観測が続いても opted_out は重複しない
    s2b, opted2b, _ = auto_join.update_membership_state(s2, [{"id": "C_A", "is_member": False}])
    assert s2b == s2 and opted2b == []

    # 手動で再び参加済み → opted_out から外れ known_member に戻る
    s3, opted3, cleared3 = auto_join.update_membership_state(
        s2, [{"id": "C_A", "is_member": True}]
    )
    assert s3 == {"known_member": ["C_A"], "opted_out": []}, s3
    assert opted3 == [] and cleared3 == ["C_A"]

    # 状態ファイルの原子的保存＆読み戻し
    with tempfile.TemporaryDirectory() as tmp:
        state_path = Path(tmp) / "state" / "auto_join_state.json"
        assert auto_join.load_state(state_path) == {"known_member": [], "opted_out": []}
        auto_join.save_state(s2, state_path)
        assert auto_join.load_state(state_path) == s2
        leftovers = [p.name for p in state_path.parent.iterdir() if p.name != state_path.name]
        assert leftovers == [], f"temp files must not remain: {leftovers}"

    return "参加済み→未参加でopted_out移動・重複なし・手動再参加で解除・入力非破壊・状態ファイル原子的保存/読み戻しを確認"


def test_auto_join_run_with_mock_client() -> str:
    """55. auto_join 実行: 参加＆シード(トップレベルのみ・join/自分/既存ts/窓外除外でPENDING保存)・join失敗でも継続・ドライランは副作用なし"""
    owner = auto_join.OWNER_USER_ID
    bot_uid = "U_YAMA_BOT"
    base_now = 1797100000.0

    class MockAutoJoinClient:
        def __init__(self) -> None:
            self.join_calls: List[str] = []
            self.history_calls: List[Dict[str, Any]] = []

        def conversations_list(self, **kwargs: Any) -> Dict[str, Any]:
            assert kwargs.get("types") == "public_channel"
            assert kwargs.get("exclude_archived") is True
            return {
                "ok": True,
                "channels": [
                    {"id": "C_SOLO", "name": "solo", "is_member": False, "is_archived": False},
                    {"id": "C_NOOWNER", "name": "noowner", "is_member": False, "is_archived": False},
                    {"id": "C_DENIED", "name": "denied", "is_member": False, "is_archived": False},
                    {"id": "C_IN", "name": "in", "is_member": True, "is_archived": False},
                    {"id": "C_KICKED", "name": "kicked", "is_member": False, "is_archived": False},
                ],
                "response_metadata": {"next_cursor": ""},
            }

        def conversations_members(self, **kwargs: Any) -> Dict[str, Any]:
            members = {
                "C_SOLO": [owner],
                "C_NOOWNER": ["U_OTHER"],
                "C_DENIED": [owner, "U_OTHER"],
                "C_KICKED": [owner],
            }[kwargs["channel"]]
            return {"ok": True, "members": members, "response_metadata": {"next_cursor": ""}}

        def conversations_join(self, **kwargs: Any) -> Dict[str, Any]:
            self.join_calls.append(kwargs["channel"])
            if kwargs["channel"] == "C_DENIED":
                raise RuntimeError("method_not_supported_for_channel_type")
            return {"ok": True}

        def auth_test(self) -> Dict[str, Any]:
            return {"ok": True, "user_id": bot_uid}

        def conversations_history(self, **kwargs: Any) -> Dict[str, Any]:
            self.history_calls.append(dict(kwargs))
            assert float(kwargs["oldest"]) == base_now - auto_join.SEED_WINDOW_SECS
            assert kwargs["limit"] == auto_join.SEED_LIMIT
            return {
                "ok": True,
                "messages": [
                    {"type": "message", "subtype": "channel_join", "user": bot_uid, "text": "joined", "ts": "1797099990.000100"},
                    {"type": "message", "user": bot_uid, "text": "yama self", "ts": "1797099980.000100"},
                    {"type": "message", "user": owner, "text": "reply in thread", "ts": "1797099970.000100", "thread_ts": "1797099900.000100"},
                    {"type": "message", "user": owner, "text": "parent", "ts": "1797099900.000100", "thread_ts": "1797099900.000100", "reply_count": 1},
                    {"type": "message", "user": owner, "text": "already stored", "ts": "1797099800.000100"},
                    {"type": "message", "user": owner, "text": "first post", "ts": "1797099700.000100", "client_msg_id": "abc"},
                    {"type": "message", "user": owner, "text": "too old", "ts": "1797098000.000100"},
                ],
            }

    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "slack_events.db"
        state_path = Path(tmp) / "auto_join_state.json"
        env_path = Path(tmp) / ".env"
        init_db(db_path)
        with get_db_connection(db_path) as conn:
            conn.execute(
                "INSERT INTO events (event_id, channel_id, user_id, text, ts, status) VALUES (?, ?, ?, ?, ?, ?)",
                ("Ev_EXISTING", "C_SOLO", owner, "already stored", "1797099800.000100", "DONE"),
            )
            conn.commit()
        auto_join.save_state({"known_member": ["C_KICKED"], "opted_out": []}, state_path)

        # Dry run: no join / seed / state save
        dry_client = MockAutoJoinClient()
        out_dry = io.StringIO()
        with redirect_stdout(out_dry):
            dry = auto_join.run_auto_join(
                db_path=db_path, env_path=env_path, state_path=state_path,
                client=dry_client, dry_run=True, now=base_now, sleep_fn=lambda _s: None,
            )
        assert dry_client.join_calls == [] and dry_client.history_calls == [], "dry-run must not join/seed"
        assert sorted(dry["would_join"]) == ["C_DENIED", "C_SOLO"], dry
        assert "would join #solo (C_SOLO)" in out_dry.getvalue(), out_dry.getvalue()
        assert auto_join.load_state(state_path) == {"known_member": ["C_KICKED"], "opted_out": []}, "dry-run must not save state"
        assert count_events(db_path) == 1

        # Real run with mock client
        client = MockAutoJoinClient()
        out = io.StringIO()
        with redirect_stdout(out):
            summary = auto_join.run_auto_join(
                db_path=db_path, env_path=env_path, state_path=state_path,
                client=client, dry_run=False, now=base_now, sleep_fn=lambda _s: None,
            )
        log = out.getvalue()
        assert sorted(client.join_calls) == ["C_DENIED", "C_SOLO"], client.join_calls
        assert "C_KICKED" not in client.join_calls and "C_NOOWNER" not in client.join_calls
        assert summary["joined"] == ["C_SOLO"], summary
        assert summary["errors"] == 1 and "join failed #denied" in log, log
        assert summary["seeded"] == 2, summary
        assert "opt-out added #kicked (C_KICKED)" in log, log

        with get_db_connection(db_path) as conn:
            rows = conn.execute(
                "SELECT event_id, text, status FROM events WHERE channel_id = 'C_SOLO' AND status = 'PENDING' ORDER BY ts"
            ).fetchall()
        texts = [r["text"] for r in rows]
        assert texts == ["first post", "parent"], texts
        assert all(r["event_id"].startswith("C_SOLO_") for r in rows), [r["event_id"] for r in rows]

        st = auto_join.load_state(state_path)
        assert st == {"known_member": ["C_IN", "C_SOLO"], "opted_out": ["C_KICKED"]}, st

        # 何も起きない回は無出力（C_DENIED の失敗は残るので、ここでは参加対象なしの状況を作る）
        class QuietClient(MockAutoJoinClient):
            def conversations_list(self, **kwargs: Any) -> Dict[str, Any]:
                return {"ok": True, "channels": [
                    {"id": "C_IN", "name": "in", "is_member": True, "is_archived": False},
                    {"id": "C_SOLO", "name": "solo", "is_member": True, "is_archived": False},
                    {"id": "C_KICKED", "name": "kicked", "is_member": False, "is_archived": False},
                ]}

        out_quiet = io.StringIO()
        with redirect_stdout(out_quiet):
            auto_join.run_auto_join(
                db_path=db_path, env_path=env_path, state_path=state_path,
                client=QuietClient(), dry_run=False, now=base_now, sleep_fn=lambda _s: None,
            )
        assert out_quiet.getvalue() == "", out_quiet.getvalue()

    return "ドライランは副作用なし・オーナー1人chへ参加＆シード2件(PENDING/fallback event_id)・join失敗で継続・外されたchはopted_out・平穏回は無出力を確認"


def test_auto_join_state_bak_rotation_and_restore() -> str:
    """56. auto_join .bak: 保存時に正常な本体を.bakへ退避・本体破損時は.bakから復旧し本体を即修復・形の壊れた本体(空dict)も.bakへfallback"""
    with tempfile.TemporaryDirectory() as tmp:
        state_path = Path(tmp) / "state" / "auto_join_state.json"
        bak_path = auto_join._bak_path(state_path)
        s1 = {"known_member": ["C_A"], "opted_out": ["C_X"]}
        s2 = {"known_member": ["C_A", "C_B"], "opted_out": ["C_X"]}

        auto_join.save_state(s1, state_path)
        assert not bak_path.exists(), "first save must not create .bak"
        auto_join.save_state(s2, state_path)
        assert auto_join._read_state_file(bak_path) == s1, ".bak must hold previous good state"
        assert auto_join.load_state(state_path) == s2

        # Corrupt the main file -> load falls back to .bak and repairs main at once
        state_path.write_text("this is not json {{{", encoding="utf-8")
        assert auto_join.load_state(state_path) == s1
        assert auto_join._read_state_file(state_path) == s1, "main must be repaired from .bak on load"
        assert auto_join._read_state_file(bak_path) == s1, ".bak must stay intact during restore"

        # Shape-broken main (valid JSON, missing keys) is also invalid -> .bak fallback
        state_path.write_text("{}", encoding="utf-8")
        state, source = auto_join.load_state_with_status(state_path)
        assert source == "bak" and state == s1, (source, state)

    return ".bak退避(正常時のみ)・破損時の.bak復旧＆本体即修復・空dict化した本体のfallbackを確認"


def test_auto_join_state_both_corrupt_failsafe() -> str:
    """57. auto_join .bak fail-safe: 本体・.bak両方読めない場合は空開始せずStateLoadErrorで自動参加を止める・初回(両方なし)は空開始"""
    with tempfile.TemporaryDirectory() as tmp:
        state_path = Path(tmp) / "auto_join_state.json"
        bak_path = auto_join._bak_path(state_path)

        # First run: neither file exists -> empty state is fine
        assert auto_join.load_state(state_path) == {"known_member": [], "opted_out": []}

        state_path.write_text("garbage", encoding="utf-8")
        bak_path.write_text("also garbage", encoding="utf-8")
        try:
            auto_join.load_state(state_path)
            raise AssertionError("load_state must raise StateLoadError when both files are unreadable")
        except auto_join.StateLoadError:
            pass

        # run_auto_join aborts before listing/joining anything
        class GuardClient:
            def __init__(self) -> None:
                self.list_calls = 0

            def conversations_list(self, **kwargs: Any) -> Dict[str, Any]:
                self.list_calls += 1
                return {"ok": True, "channels": [], "response_metadata": {"next_cursor": ""}}

        client = GuardClient()
        out = io.StringIO()
        with redirect_stdout(out):
            summary = auto_join.run_auto_join(
                db_path=Path(tmp) / "db.sqlite", env_path=Path(tmp) / ".env",
                state_path=state_path, client=client, dry_run=False,
                now=1797100000.0, sleep_fn=lambda _s: None,
            )
        assert summary.get("state_error") is True and summary["errors"] >= 1, summary
        assert summary["joined"] == [] and client.list_calls == 0, summary
        assert "auto-join aborted" in out.getvalue(), out.getvalue()

    return "両方破損時は空開始せず停止(StateLoadError・list未実行・state_error記録)・初回は空開始を確認"


def test_auto_join_state_bak_not_overwritten_by_corrupt_main() -> str:
    """58. auto_join .bak ガード: 壊れた本体を保存しても良い.bakを上書きしない(04d9ade型の穴の再現防止)"""
    with tempfile.TemporaryDirectory() as tmp:
        state_path = Path(tmp) / "auto_join_state.json"
        bak_path = auto_join._bak_path(state_path)
        s1 = {"known_member": ["C_A"], "opted_out": ["C_X"]}
        s2 = {"known_member": ["C_A", "C_B"], "opted_out": ["C_X"]}
        s3 = {"known_member": ["C_A", "C_B", "C_C"], "opted_out": ["C_X"]}

        auto_join.save_state(s1, state_path)
        auto_join.save_state(s2, state_path)
        assert auto_join._read_state_file(bak_path) == s1

        # Main breaks, then a save happens: rotation must be skipped, .bak preserved
        state_path.write_text("\x00\x01 broken", encoding="utf-8")
        auto_join.save_state(s3, state_path)
        assert auto_join._read_state_file(bak_path) == s1, "corrupt main must not overwrite good .bak"
        assert auto_join.load_state(state_path) == s3

    return "破損した本体の保存で.bakが潰れないこと・保存自体は新しい状態で完了することを確認"


def test_auto_join_last_status_and_transient_retries() -> str:
    """65. auto_join statusファイル＆transient_retries: 実行ごとにlast_statusを上書き・吸収されたリトライだけを数え最終失敗は二重計上しない・ドライランは書かない・状態エラー時もerrorで記録"""
    owner = auto_join.OWNER_USER_ID
    base_now = 1797200000.0

    class FlakyClient:
        def __init__(self) -> None:
            self.list_calls = 0
            self.member_calls: Dict[str, int] = {}

        def conversations_list(self, **kwargs: Any) -> Dict[str, Any]:
            self.list_calls += 1
            if self.list_calls == 1:
                raise ConnectionResetError("closed once")
            return {"ok": True, "channels": [
                {"id": "C_IN", "name": "in", "is_member": True, "is_archived": False},
                {"id": "C_CHECK", "name": "check", "is_member": False, "is_archived": False},
                {"id": "C_DEAD", "name": "dead", "is_member": False, "is_archived": False},
            ], "response_metadata": {"next_cursor": ""}}

        def conversations_members(self, **kwargs: Any) -> Dict[str, Any]:
            cid = kwargs["channel"]
            self.member_calls[cid] = self.member_calls.get(cid, 0) + 1
            if cid == "C_DEAD":
                raise ConnectionResetError("always down")
            if cid == "C_CHECK" and self.member_calls[cid] <= 2:
                raise ConnectionResetError("flaky members")
            return {"ok": True, "members": ["U_OTHER"], "response_metadata": {"next_cursor": ""}}

    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "slack_events.db"
        state_path = Path(tmp) / "auto_join_state.json"
        env_path = Path(tmp) / ".env"
        status_path = auto_join.status_path_for(state_path)
        init_db(db_path)

        # list: 1 retry absorbed; members(C_CHECK): 2 retries absorbed; members(C_DEAD): always fails -> errors only
        summary = auto_join.run_auto_join(
            db_path=db_path, env_path=env_path, state_path=state_path,
            client=FlakyClient(), dry_run=False, now=base_now, sleep_fn=lambda _s: None,
        )
        assert summary["checked_channels"] == 3, summary
        assert summary["errors"] == 1, summary
        assert summary["transient_retries"] == 3, summary
        assert status_path.exists(), "status file must be written on a real run"
        st = json.loads(status_path.read_text(encoding="utf-8"))
        assert st["status"] == "error" and st["errors"] == 1, st
        assert st["checked_channels"] == 3 and st["transient_retries"] == 3, st
        assert st["joined"] == 0 and st["opted_out_added"] == 0, st
        assert st["finished_at"], st

        # Clean run overwrites and resets transient_retries to 0
        class CleanClient:
            def conversations_list(self, **kwargs: Any) -> Dict[str, Any]:
                return {"ok": True, "channels": [
                    {"id": "C_IN", "name": "in", "is_member": True, "is_archived": False},
                ], "response_metadata": {"next_cursor": ""}}

        summary2 = auto_join.run_auto_join(
            db_path=db_path, env_path=env_path, state_path=state_path,
            client=CleanClient(), dry_run=False, now=base_now, sleep_fn=lambda _s: None,
        )
        assert summary2["transient_retries"] == 0 and summary2["errors"] == 0, summary2
        st2 = json.loads(status_path.read_text(encoding="utf-8"))
        assert st2["status"] == "success" and st2["transient_retries"] == 0 and st2["checked_channels"] == 1, st2

        # Dry run must not overwrite the status file
        before = status_path.read_text(encoding="utf-8")
        auto_join.run_auto_join(
            db_path=db_path, env_path=env_path, state_path=state_path,
            client=CleanClient(), dry_run=True, now=base_now, sleep_fn=lambda _s: None,
        )
        assert status_path.read_text(encoding="utf-8") == before, "dry-run must not write status"

        # State load error: status=error is recorded and the run aborts
        state_path.write_text("{broken", encoding="utf-8")
        auto_join._bak_path(state_path).write_text("{also broken", encoding="utf-8")
        summary3 = auto_join.run_auto_join(
            db_path=db_path, env_path=env_path, state_path=state_path,
            client=CleanClient(), dry_run=False, now=base_now, sleep_fn=lambda _s: None,
        )
        assert summary3.get("state_error") is True and summary3["errors"] == 1, summary3
        st3 = json.loads(status_path.read_text(encoding="utf-8"))
        assert st3["status"] == "error" and st3["errors"] == 1 and st3["checked_channels"] == 0, st3

    return "statusファイルの毎回上書き・吸収リトライ3件の計上(最終失敗はerrorsのみ)・正常回での0リセット・ドライラン無書込・状態エラー時のerror記録を確認"


def test_curl_read_fallback() -> str:
    """59. 読み取りcurlフォールバック: SDK接続エラー時のみcurlで読み直し・curlも失敗したら元の例外が伝播・正常時はcurl未使用・POST経路は非オーバーライド"""
    from slack_sdk import WebClient

    client = agent_tools._CurlFallbackWebClient(token="xoxb-dummy-token-for-test")
    assert isinstance(client, WebClient)
    # POST (chat_postMessage) must not be overridden by the subclass
    assert "chat_postMessage" not in agent_tools._CurlFallbackWebClient.__dict__

    def sdk_boom(self: Any, **kwargs: Any) -> Any:
        raise http.client.IncompleteRead(b"partial")

    curl_calls: List[Tuple[str, str]] = []

    def curl_ok(token: str, method: str, params: Dict[str, Any]) -> Dict[str, Any]:
        curl_calls.append((token, method))
        return {"ok": True, "messages": [{"ts": "1795006461.907139", "text": "hi"}]}

    with patch.object(WebClient, "conversations_replies", sdk_boom), patch.object(
        agent_tools, "_curl_api_read", curl_ok
    ):
        resp = client.conversations_replies(channel="C_X", ts="1795006461.907139", limit=5)
        assert resp.get("ok") is True and resp["messages"][0]["text"] == "hi"
        assert curl_calls == [("xoxb-dummy-token-for-test", "conversations.replies")], curl_calls

    def curl_fail(token: str, method: str, params: Dict[str, Any]) -> Any:
        raise RuntimeError("curl failed")

    with patch.object(WebClient, "conversations_replies", sdk_boom), patch.object(
        agent_tools, "_curl_api_read", curl_fail
    ):
        try:
            client.conversations_replies(channel="C_X", ts="1795006461.907139")
            raise AssertionError("original IncompleteRead must propagate when curl also fails")
        except http.client.IncompleteRead:
            pass

    def sdk_ok(self: Any, **kwargs: Any) -> Dict[str, Any]:
        return {"ok": True, "messages": []}

    curl_calls.clear()
    with patch.object(WebClient, "conversations_history", sdk_ok), patch.object(
        agent_tools, "_curl_api_read", curl_ok
    ):
        resp2 = client.conversations_history(channel="C_X", limit=3)
        assert resp2.get("ok") is True
        assert curl_calls == [], "curl must not be used when the SDK read succeeds"

    # _curl_api_read itself: token goes via stdin config (never argv), booleans
    # serialize as true/false, None params are dropped, result is a SlackResponse
    captured: Dict[str, Any] = {}

    class FakeProc:
        returncode = 0
        stdout = '{"ok": true, "messages": [{"ts": "1.5"}]}'
        stderr = ""

    def fake_run(argv: Any, **kwargs: Any) -> Any:
        captured["argv"] = argv
        captured["input"] = kwargs.get("input", "")
        return FakeProc()

    with patch.object(agent_tools.subprocess, "run", fake_run):
        resp3 = agent_tools._curl_api_read(
            "xoxb-dummy-token-for-test",
            "conversations.replies",
            {"channel": "C_X", "ts": "1.5", "include_all_metadata": True, "cursor": None},
        )
    assert captured["argv"] == ["curl", "-K", "-"], captured["argv"]
    assert "xoxb-dummy-token-for-test" not in " ".join(captured["argv"])
    assert "Authorization: Bearer xoxb-dummy-token-for-test" in captured["input"]
    assert "include_all_metadata=true" in captured["input"], captured["input"]
    assert "cursor" not in captured["input"], captured["input"]
    assert resp3.get("ok") is True and resp3["messages"][0]["ts"] == "1.5"
    assert resp3.data["ok"] is True and resp3.status_code == 200

    return "SDK接続エラー時のみcurl発動・curl失敗時は元の例外を維持・正常時はcurl未使用・POST非対象・トークンは標準入力経由・SlackResponseで返却を確認"


def test_watchdog_threshold_recheck_cooldown() -> str:
    """60. Watchdog 120秒しきい値＆再確認＆クールダウン (150秒停止で再起動・同一heartbeatへの再起動は60秒抑止・再確認中に回復すれば再起動しない)"""
    import threading

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        state_dir = tmp_path / "state"
        state_dir.mkdir(parents=True, exist_ok=True)
        hb_path = state_dir / "heartbeat.json"
        log_path = state_dir / "watchdog.log"
        trace_file = tmp_path / "restart_calls.log"
        mock_restart = tmp_path / "mock_restart.sh"
        mock_restart.write_text(
            f"#!/usr/bin/env bash\necho RESTART >> \"{trace_file}\"\nexit 0\n",
            encoding="utf-8",
        )
        mock_restart.chmod(0o755)

        def run_wd(extra_env: Dict[str, str]) -> Any:
            env = os.environ.copy()
            env.update(
                {
                    "YAMA_STATE_DIR": str(state_dir),
                    "YAMA_HEARTBEAT_PATH": str(hb_path),
                    "YAMA_WATCHDOG_LOG": str(log_path),
                    "YAMA_PYTHON_BIN": str(PYTHON_BIN),
                    "YAMA_RESTART_CMD": str(mock_restart),
                    "YAMA_WATCHDOG_RECHECK_SEC": "0",
                }
            )
            env.update(extra_env)
            return subprocess.run(
                ["bash", str(WATCHDOG_SH)], env=env, capture_output=True, text=True
            )

        # 1) 150s stale: below the old 180s threshold but >= new 120s -> restart fires
        stale_ts = time.time() - 150
        hb_path.write_text(
            json.dumps({"timestamp": stale_ts, "connected": True, "bot_user_id": "U_YAMA_BOT"}),
            encoding="utf-8",
        )
        res1 = run_wd({})
        assert res1.returncode == 0, res1.stderr
        assert trace_file.read_text(encoding="utf-8").count("RESTART") == 1
        assert "threshold=120s" in log_path.read_text(encoding="utf-8")

        # 2) Same unchanged heartbeat immediately after: cooldown suppresses re-restart
        res2 = run_wd({})
        assert res2.returncode == 0, res2.stderr
        assert trace_file.read_text(encoding="utf-8").count("RESTART") == 1, "cooldown must suppress the second restart"
        assert "suppressed by cooldown" in log_path.read_text(encoding="utf-8")

        # 3) Recheck: heartbeat recovers during the recheck wait -> no restart
        state2 = tmp_path / "state2"
        state2.mkdir(parents=True, exist_ok=True)
        hb2 = state2 / "heartbeat.json"
        log2 = state2 / "watchdog.log"
        hb2.write_text(
            json.dumps({"timestamp": time.time() - 200, "connected": False, "bot_user_id": "U_YAMA_BOT"}),
            encoding="utf-8",
        )

        def recover() -> None:
            time.sleep(1)
            hb2.write_text(
                json.dumps({"timestamp": time.time(), "connected": True, "bot_user_id": "U_YAMA_BOT"}),
                encoding="utf-8",
            )

        timer = threading.Timer(0.5, recover)
        timer.start()
        try:
            env = os.environ.copy()
            env.update(
                {
                    "YAMA_STATE_DIR": str(state2),
                    "YAMA_HEARTBEAT_PATH": str(hb2),
                    "YAMA_WATCHDOG_LOG": str(log2),
                    "YAMA_PYTHON_BIN": str(PYTHON_BIN),
                    "YAMA_RESTART_CMD": str(mock_restart),
                    "YAMA_WATCHDOG_RECHECK_SEC": "3",
                }
            )
            res3 = subprocess.run(
                ["bash", str(WATCHDOG_SH)], env=env, capture_output=True, text=True
            )
        finally:
            timer.join()
        assert res3.returncode == 0, res3.stderr
        assert trace_file.read_text(encoding="utf-8").count("RESTART") == 1, "recheck recovery must not restart"
        assert "recovered on recheck" in log2.read_text(encoding="utf-8")

    return "120秒しきい値で150秒停止を検知・同一heartbeatへの再起動を60秒抑止・再確認中の回復で再起動回避を確認"


def test_watchdog_restart_py_compile_gate() -> str:
    """61. Watchdog 再起動前 py_compile ゲート (壊れた listener.py では再起動を見送り・直った後に再起動する)"""
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        state_dir = tmp_path / "state"
        state_dir.mkdir(parents=True, exist_ok=True)
        hb_path = state_dir / "heartbeat.json"
        log_path = state_dir / "watchdog.log"
        trace_file = tmp_path / "restart_calls.log"
        mock_restart = tmp_path / "mock_restart.sh"
        mock_restart.write_text(
            f"#!/usr/bin/env bash\necho RESTART >> \"{trace_file}\"\nexit 0\n",
            encoding="utf-8",
        )
        mock_restart.chmod(0o755)
        broken_listener = tmp_path / "broken_listener.py"
        broken_listener.write_text("def broken(:\n", encoding="utf-8")
        good_listener = tmp_path / "good_listener.py"
        good_listener.write_text("x = 1\n", encoding="utf-8")

        def run_wd(listener_path: Path) -> Any:
            env = os.environ.copy()
            env.update(
                {
                    "YAMA_STATE_DIR": str(state_dir),
                    "YAMA_HEARTBEAT_PATH": str(hb_path),
                    "YAMA_WATCHDOG_LOG": str(log_path),
                    "YAMA_PYTHON_BIN": str(PYTHON_BIN),
                    "YAMA_RESTART_CMD": str(mock_restart),
                    "YAMA_WATCHDOG_RECHECK_SEC": "0",
                    "YAMA_LISTENER_PATH": str(listener_path),
                }
            )
            return subprocess.run(
                ["bash", str(WATCHDOG_SH)], env=env, capture_output=True, text=True
            )

        stale_ts = time.time() - 150
        hb_path.write_text(
            json.dumps({"timestamp": stale_ts, "connected": True, "bot_user_id": "U_YAMA_BOT"}),
            encoding="utf-8",
        )
        # 1) listener.py が壊れている (編集中) 場合は再起動を見送る
        res1 = run_wd(broken_listener)
        assert res1.returncode == 1, res1.stderr
        assert not trace_file.exists() or trace_file.read_text(encoding="utf-8").count("RESTART") == 0
        log_text = log_path.read_text(encoding="utf-8")
        assert "Restart skipped" in log_text and "py_compile" in log_text

        # 2) listener.py が直れば次周期で再起動する (見送り時に再起動マーカーは残らない)
        res2 = run_wd(good_listener)
        assert res2.returncode == 0, res2.stderr
        assert trace_file.read_text(encoding="utf-8").count("RESTART") == 1

    return "壊れたlistener.pyでは再起動を見送りログに記録・直った後に再起動することを確認"


def test_hook_bash_n_guards() -> str:
    """62. フックの bash -n ガード (dispatcher.sh / watchdog.sh が構文エラーの回は実行を飛ばしてログに残す)"""
    hook_script = HOOK_SCRIPT_PATH
    assert hook_script.exists(), f"Hook script not found: {hook_script}"
    script_text = hook_script.read_text(encoding="utf-8")
    assert 'bash -n "$PROJECT_DIR/dispatcher.sh"' in script_text
    assert 'bash -n "$PROJECT_DIR/watchdog.sh"' in script_text
    assert "Dispatcher skipped: dispatcher.sh failed bash -n" in script_text
    assert "Watchdog skipped: watchdog.sh failed bash -n" in script_text

    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        proj = tmp_dir / "proj"
        proj.mkdir()
        log_path = tmp_dir / "watchdog.log"
        trace_file = tmp_dir / "trace.log"
        # Guard blocks below mirror the hook script verbatim (same commands,
        # same skip-and-log behaviour); the static assertions above tie them
        # to the live hook text.
        guard_sh = tmp_dir / "guard.sh"
        guard_sh.write_text(
            """#!/usr/bin/env bash
set -uo pipefail
PROJECT_DIR="$1"
WATCHDOG_LOG="$2"
TRACE_FILE="$3"
if bash -n "$PROJECT_DIR/watchdog.sh" 2>/dev/null; then
  "$PROJECT_DIR/watchdog.sh" >/dev/null 2>&1 || true
else
  mkdir -p "$(dirname "$WATCHDOG_LOG")" 2>/dev/null || true
  echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") [WARN] Watchdog skipped: watchdog.sh failed bash -n" >> "$WATCHDOG_LOG" 2>/dev/null || true
fi
set +e
if bash -n "$PROJECT_DIR/dispatcher.sh" 2>/dev/null; then
  output=$("$PROJECT_DIR/dispatcher.sh" 2>/dev/null)
  code=$?
else
  mkdir -p "$(dirname "$WATCHDOG_LOG")" 2>/dev/null || true
  echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") [WARN] Dispatcher skipped: dispatcher.sh failed bash -n" >> "$WATCHDOG_LOG" 2>/dev/null || true
  output="NO_EVENTS"
  code=1
fi
set -e
echo "code=${code} output=${output}"
""",
            encoding="utf-8",
        )
        good_sh = f"#!/usr/bin/env bash\necho RAN >> \"{trace_file}\"\n"
        broken_sh = "#!/usr/bin/env bash\nif true; then\n"

        def write_mock(name: str, content: str) -> None:
            p = proj / name
            p.write_text(content, encoding="utf-8")
            p.chmod(0o755)

        # Case A: broken dispatcher, good watchdog -> dispatch skipped, watchdog runs
        write_mock("dispatcher.sh", broken_sh)
        write_mock("watchdog.sh", good_sh.replace("RAN", "WATCHDOG-RAN"))
        res_a = subprocess.run(["bash", str(guard_sh), str(proj), str(log_path), str(trace_file)],
                               capture_output=True, text=True)
        assert res_a.returncode == 0, res_a.stderr
        assert "code=1 output=NO_EVENTS" in res_a.stdout
        trace = trace_file.read_text(encoding="utf-8") if trace_file.exists() else ""
        assert "WATCHDOG-RAN" in trace and "DISPATCH-RAN" not in trace
        assert "Dispatcher skipped: dispatcher.sh failed bash -n" in log_path.read_text(encoding="utf-8")
        # Case B: good dispatcher, broken watchdog -> watchdog skipped, dispatch runs
        write_mock("dispatcher.sh", good_sh.replace("RAN", "DISPATCH-RAN"))
        write_mock("watchdog.sh", broken_sh)
        res_b = subprocess.run(["bash", str(guard_sh), str(proj), str(log_path), str(trace_file)],
                               capture_output=True, text=True)
        assert res_b.returncode == 0, res_b.stderr
        assert "code=0" in res_b.stdout
        trace = trace_file.read_text(encoding="utf-8")
        assert trace.count("DISPATCH-RAN") == 1
        assert "Watchdog skipped: watchdog.sh failed bash -n" in log_path.read_text(encoding="utf-8")

    return "壊れた子スクリプトの回だけ実行を飛ばし、直った方は実行され、飛ばした理由がログに残ることを確認"


def test_resolve_bot_user_id_env_validation() -> str:
    """63. Bot ユーザーIDの環境変数上書きの形状チェック (B始まり・xox始まりは例外で弾く、U/W始まりは採用、空・未設定は auth_test 経由へ)"""

    class _FakeClient:
        def __init__(self, user_id=None, fail_auth=False):
            self._auth_user_id = user_id
            self._fail_auth = fail_auth
            self.auth_calls = 0

        def auth_test(self):
            self.auth_calls += 1
            if self._fail_auth:
                raise RuntimeError("auth_test failed")
            return {"ok": True, "user_id": self._auth_user_id}

    def resolve(env, client, bot_user_id=None):
        saved = {k: os.environ.get(k) for k in ("YAMA_BOT_USER_ID", "SLACK_BOT_USER_ID")}
        try:
            for k in ("YAMA_BOT_USER_ID", "SLACK_BOT_USER_ID"):
                os.environ.pop(k, None)
            os.environ.update(env)
            return agent_tools._resolve_bot_user_id(
                client, bot_user_id=bot_user_id,
                max_attempts=1, backoffs=(0,), sleep_fn=lambda s: None,
            )
        finally:
            for k in ("YAMA_BOT_USER_ID", "SLACK_BOT_USER_ID"):
                os.environ.pop(k, None)
            for k, v in saved.items():
                if v is not None:
                    os.environ[k] = v

    # 正常系：U始まり・W始まり（Enterprise Grid）の環境変数はそのまま採用され、auth_test は呼ばれない
    client = _FakeClient(user_id="U9999999999")
    assert resolve({"YAMA_BOT_USER_ID": "U1111111111"}, client) == "U1111111111"
    assert client.auth_calls == 0
    assert resolve({"SLACK_BOT_USER_ID": "W2222222222"}, client) == "W2222222222"
    assert client.auth_calls == 0

    # 異常系：B始まり（大文字・小文字）は例外で弾く。値はエラーメッセージに出ない
    for bad in ("B3333333333", "b3333333333"):
        try:
            resolve({"YAMA_BOT_USER_ID": bad}, _FakeClient(user_id="U9999999999"))
        except ValueError as exc:
            assert bad not in str(exc), "offending value must not leak into the error"
            assert "YAMA_BOT_USER_ID" in str(exc)
        else:
            raise AssertionError(f"B-prefixed env value was not rejected: {bad}")

    # 異常系：トークン誤代入（xoxb-始まり）も例外で弾く。トークン値はメッセージに出ない
    # リテラルを置くと公開前スキャン（Slackトークン形）に引っかかるため部品から組み立てる
    token_like = "xox" + "b-" + "1234567890-abcdefghij"
    token_prefix = "xox" + "b-" + "1234567890"
    try:
        resolve({"SLACK_BOT_USER_ID": token_like}, _FakeClient(user_id="U9999999999"))
    except ValueError as exc:
        assert token_prefix not in str(exc), "token must not leak into the error"
        assert "SLACK_BOT_USER_ID" in str(exc)
    else:
        raise AssertionError("token-like env value was not rejected")

    # 未設定・空・空白のみ：auth_test の値へ進む
    assert resolve({}, _FakeClient(user_id="U9999999999")) == "U9999999999"
    assert resolve({"YAMA_BOT_USER_ID": ""}, _FakeClient(user_id="U9999999999")) == "U9999999999"
    assert resolve({"YAMA_BOT_USER_ID": "   "}, _FakeClient(user_id="U9999999999")) == "U9999999999"

    # auth_test 失敗時：既定の定数へフォールバックする
    assert resolve({}, _FakeClient(fail_auth=True)) == agent_tools.YAMA_BOT_USER_ID

    # 明示引数は環境変数より優先され、チェック対象は環境変数の受け取り口のみ
    assert resolve(
        {"YAMA_BOT_USER_ID": "B3333333333"},
        _FakeClient(user_id="U9999999999"),
        bot_user_id="U4444444444",
    ) == "U4444444444"

    return "B始まり・xox始まりの環境変数を例外で弾き（値は漏らさず）、U/W始まりは採用、空・未設定は auth_test 経由、明示引数は環境変数より優先することを確認"


def test_hook_syntax_guard_escalation() -> str:
    """64. 構文ガードの10分エスカレーション＆警告wake (jq組立てJSON検証・wake失敗時はwarned未保存で再試行＆後段処理続行・成功時のみwarned保存・10分未満静観・復旧・対象独立・破損状態耐性)"""
    hook_script = HOOK_SCRIPT_PATH
    assert hook_script.exists(), f"Hook script not found: {hook_script}"
    script_text = hook_script.read_text(encoding="utf-8")
    for needle in (
        "syntax_guard_eval",
        "syntax_guard_mark_warned",
        "SYNTAX_GUARD_WARN_SEC",
        '"syntax_guard_warning"',
        '"syntax_guard_recovered"',
        'wake "Syntax guard: target(s) broken for 10+ min"',
        'wake "Syntax guard: target(s) recovered"',
        "hook_state_get",
        "hook_state_set",
        "jq -n",
    ):
        assert needle in script_text, f"missing in live hook: {needle}"
    # Extract the entire syntax-guard escalation block (eval + mark_warned +
    # wake dispatch) verbatim from the live hook so the test executes the real
    # shipped logic including wake invocation and error handling.
    begin = script_text.index("# --- syntax-guard escalation: begin")
    end = script_text.index("# --- syntax-guard escalation: end ---")
    guard_src = script_text[begin:end]
    assert 'wake "Syntax guard: target(s) broken for 10+ min"' in guard_src, (
        "warning wake dispatch must be inside begin..end markers"
    )
    assert 'wake "Syntax guard: target(s) recovered"' in guard_src, (
        "recovery wake dispatch must be inside begin..end markers"
    )

    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        state_file = tmp_dir / "hook_state.json"
        wake_log = tmp_dir / "wake_calls.jsonl"
        watchdog_log = tmp_dir / "watchdog.log"
        after_log = tmp_dir / "after_guard.log"
        stub = tmp_dir / "guard_block_stub.sh"
        stub.write_text(
            "#!/usr/bin/env bash\n"
            "set -euo pipefail\n"
            'STATE_FILE="$1"\n'
            'WAKE_LOG="$2"\n'
            'WATCHDOG_LOG="$3"\n'
            'AFTER_LOG="$4"\n'
            'WD_OK="$5"\n'
            'DISP_OK="$6"\n'
            'LISTENER_OK="$7"\n'
            "hook_state_get() {\n"
            '  if [[ -f "$STATE_FILE" ]]; then\n'
            '    jq -ce \'select(type == "object")\' <"$STATE_FILE" 2>/dev/null || printf \'{}\'\n'
            "  else\n"
            "    printf '{}'\n"
            "  fi\n"
            "}\n"
            "hook_state_set() {\n"
            '  local value="$1" tmp\n'
            '  value="$(printf \'%s\' "$value" | jq -ce \'select(type == "object")\')" || return 1\n'
            '  tmp="${STATE_FILE}.tmp.$$"\n'
            '  printf \'%s\\n\' "$value" >"$tmp" || return 1\n'
            '  mv -f "$tmp" "$STATE_FILE"\n'
            "}\n"
            "wake() {\n"
            '  local title="$1" payload="$2"\n'
            '  jq -n -c --arg t "$title" --arg p "$payload" \'{"title": $t, "payload": $p}\' >>"$WAKE_LOG"\n'
            '  if [[ "${STUB_WAKE_FAIL:-0}" == "1" ]]; then\n'
            "    return 1\n"
            "  fi\n"
            "  return 0\n"
            "}\n"
            + guard_src
            + '\necho "AFTER_GUARD_OK" >>"$AFTER_LOG"\n',
            encoding="utf-8",
        )

        def run_cycle(
            now: int,
            *,
            wd_ok: int = 1,
            disp_ok: int = 1,
            listener_ok: int = 1,
            wake_fail: int = 0,
        ) -> Tuple[List[Dict[str, str]], bool]:
            before_wakes = (
                len(wake_log.read_text(encoding="utf-8").splitlines())
                if wake_log.exists()
                else 0
            )
            before_after = (
                len(after_log.read_text(encoding="utf-8").splitlines())
                if after_log.exists()
                else 0
            )
            env = os.environ.copy()
            env["YAMA_SYNTAX_GUARD_NOW"] = str(now)
            env["STUB_WAKE_FAIL"] = str(wake_fail)
            res = subprocess.run(
                [
                    "bash",
                    str(stub),
                    str(state_file),
                    str(wake_log),
                    str(watchdog_log),
                    str(after_log),
                    str(wd_ok),
                    str(disp_ok),
                    str(listener_ok),
                ],
                env=env,
                capture_output=True,
                text=True,
            )
            assert res.returncode == 0, f"guard block failed (rc={res.returncode}): {res.stderr}"
            lines = (
                wake_log.read_text(encoding="utf-8").splitlines()
                if wake_log.exists()
                else []
            )
            new_wakes = [json.loads(line) for line in lines[before_wakes:]]
            after_lines = (
                after_log.read_text(encoding="utf-8").splitlines()
                if after_log.exists()
                else []
            )
            downstream_ran = (
                len(after_lines) == before_after + 1
                and after_lines[-1] == "AFTER_GUARD_OK"
            )
            return new_wakes, downstream_ran

        def read_state() -> Any:
            if not state_file.exists():
                return {}
            return json.loads(state_file.read_text(encoding="utf-8"))

        def jq_assert(payload_str: str, expr: str) -> None:
            chk = subprocess.run(
                ["jq", "-e", expr],
                input=payload_str,
                capture_output=True,
                text=True,
            )
            assert chk.returncode == 0, (
                f"jq validation failed for payload={payload_str!r} expr={expr!r}: {chk.stderr}"
            )

        # (e) 10分未満は静か：初回失敗を記録するだけ
        wakes, ran = run_cycle(1000, disp_ok=0)
        assert wakes == [] and ran is True
        st = read_state()["syntax_guard"]["dispatcher.sh"]
        assert st["first_fail"] == 1000 and st["warned"] is False
        # 599秒経過でもまだ警告なし
        wakes, ran = run_cycle(1599, disp_ok=0)
        assert wakes == [] and ran is True
        assert read_state()["syntax_guard"]["dispatcher.sh"]["warned"] is False

        # (a)(b)(d) ちょうど600秒で警告 wake 試行、wake 失敗時：
        #  - 第2引数は jq 検証可能な妥当 JSON で型も正しい
        #  - warned は保存されない
        #  - watchdog.log に失敗記録が書かれる
        #  - ブロック後段のダミー処理が実行される
        wakes, ran = run_cycle(1600, disp_ok=0, wake_fail=1)
        assert len(wakes) == 1, f"expected 1 wake attempt, got {wakes}"
        assert ran is True, "downstream step must still execute after warning wake failure"
        assert wakes[0]["title"] == "Syntax guard: target(s) broken for 10+ min"
        payload_str = wakes[0]["payload"]
        jq_assert(
            payload_str,
            '.syntax_guard_warning == true '
            'and (.targets | type == "array") and (.targets == ["dispatcher.sh"]) '
            'and (.first_failed_at_epoch | type == "number") and (.first_failed_at_epoch == 1000) '
            'and (.warn_seconds | type == "number") and (.warn_seconds == 600)',
        )
        parsed = json.loads(payload_str)
        assert parsed["syntax_guard_warning"] is True
        assert parsed["targets"] == ["dispatcher.sh"]
        assert type(parsed["first_failed_at_epoch"]) is int and parsed["first_failed_at_epoch"] == 1000
        assert type(parsed["warn_seconds"]) is int and parsed["warn_seconds"] == 600
        st_after_fail = read_state()["syntax_guard"]["dispatcher.sh"]
        assert st_after_fail["warned"] is False and st_after_fail["first_fail"] == 1000, st_after_fail
        assert watchdog_log.exists() and "Syntax guard warning wake failed" in watchdog_log.read_text(encoding="utf-8")

        # (b)(c) 次の実行で再度 wake が試みられ、成功した場合のみ warned=true が保存される
        wakes, ran = run_cycle(1630, disp_ok=0, wake_fail=0)
        assert len(wakes) == 1 and ran is True, wakes
        assert wakes[0]["title"] == "Syntax guard: target(s) broken for 10+ min"
        jq_assert(
            wakes[0]["payload"],
            '.syntax_guard_warning == true and .targets == ["dispatcher.sh"] '
            'and (.first_failed_at_epoch | type == "number") and .first_failed_at_epoch == 1000 '
            'and (.warn_seconds | type == "number") and .warn_seconds == 600',
        )
        st_after_ok = read_state()["syntax_guard"]["dispatcher.sh"]
        assert st_after_ok["warned"] is True and st_after_ok["first_fail"] == 1000, st_after_ok

        # (c) 壊れたままの2回目実行では wake しない
        wakes, ran = run_cycle(1900, disp_ok=0, wake_fail=0)
        assert wakes == [] and ran is True

        # (e) 直ったら復旧 wake を1回だけ送り、状態から消える
        wakes, ran = run_cycle(2000, disp_ok=1, wake_fail=0)
        assert len(wakes) == 1 and ran is True
        assert wakes[0]["title"] == "Syntax guard: target(s) recovered"
        jq_assert(
            wakes[0]["payload"],
            '.syntax_guard_recovered == true and (.targets | type == "array") and .targets == ["dispatcher.sh"]',
        )
        assert "dispatcher.sh" not in read_state().get("syntax_guard", {})
        wakes, ran = run_cycle(2100, disp_ok=1, wake_fail=0)
        assert wakes == [] and ran is True

        # (e) 対象ごとに独立：watchdog.sh は自分の600秒で警告、復旧 wake 失敗でも後段続行
        wakes, ran = run_cycle(3000, wd_ok=0)
        assert wakes == [] and ran is True
        wakes, ran = run_cycle(3599, wd_ok=0)
        assert wakes == [] and ran is True
        wakes, ran = run_cycle(3600, wd_ok=0)
        assert len(wakes) == 1 and ran is True
        jq_assert(
            wakes[0]["payload"],
            '.syntax_guard_warning == true and .targets == ["watchdog.sh"] '
            'and .first_failed_at_epoch == 3000 and .warn_seconds == 600',
        )
        assert read_state()["syntax_guard"]["watchdog.sh"]["warned"] is True
        wakes, ran = run_cycle(3700, wd_ok=1, wake_fail=1)
        assert len(wakes) == 1 and ran is True
        assert wakes[0]["title"] == "Syntax guard: target(s) recovered"
        assert "watchdog.sh" not in read_state().get("syntax_guard", {})
        assert "Syntax guard recovery wake failed" in watchdog_log.read_text(encoding="utf-8")

        # (e) 破損した状態ファイルでも落ちず、時計を仕切り直す
        state_file.write_text("{not json", encoding="utf-8")
        wakes, ran = run_cycle(5000, listener_ok=0)
        assert wakes == [] and ran is True
        st_l = read_state()["syntax_guard"]["listener.py"]
        assert st_l["first_fail"] == 5000 and st_l["warned"] is False

    return "begin〜end全体を実行し、jq検証済みJSONペイロード・wake失敗時のwarned未保存＆再試行＆後段処理続行・成功時のみwarned保存＆重複抑止・復旧・対象独立・破損状態耐性を確認"


def test_owner_dm_poll_proxy_ts_and_auto_streak() -> str:
    """66. DM代行 auto_streak・proxy_ts・排他制御 (代行3連続で1→2→3到達しsend拒否・本人投稿で0リセット・resolve --reply-ts予備経路・50件上限切捨て・ts文字列完全一致)"""
    uid = "U_NEIRO"  # Neiro
    owner = owner_dm_poll.OWNER_ID

    with tempfile.TemporaryDirectory() as tmp:
        state_path = Path(tmp) / "slack-owner-dm.json"
        lock_path = Path(str(state_path) + ".lock")

        # Backward-compat initial state WITHOUT "proxy_ts" key
        init_data = {
            "convs": {
                "U_YANAYANA": {"top_ts": "1795000000.000000", "threads": {}, "handled": [], "auto_streak": 0},
                uid: {"top_ts": "1795000000.000000", "threads": {}, "handled": [], "auto_streak": 0},
                "U_ATA": {"top_ts": "1795000000.000000", "threads": {}, "handled": [], "auto_streak": 0},
            },
            "pending": [],
            "last_wake_at": 0,
        }
        state_path.write_text(json.dumps(init_data), encoding="utf-8")

        dm_feeds: Dict[str, List[Dict[str, Any]]] = {
            "U_YANAYANA": [],
            uid: [],
            "U_ATA": [],
        }
        send_calls: List[Tuple[str, str, Optional[str]]] = []
        send_responses: List[Optional[str]] = []

        def fake_read_dm(user_id: str, limit: int = 6) -> List[Dict[str, Any]]:
            return list(dm_feeds.get(user_id, []))

        def fake_send_dm(user_id: str, message: str, thread_ts: Optional[str] = None) -> Optional[str]:
            send_calls.append((user_id, message, thread_ts))
            return send_responses.pop(0)

        def read_st() -> Dict[str, Any]:
            return json.loads(state_path.read_text(encoding="utf-8"))

        def call_resolve(item_id: str, action: str, reply_ts: Optional[str] = None) -> Dict[str, Any]:
            buf = io.StringIO()
            with redirect_stdout(buf):
                owner_dm_poll.resolve(item_id, action, reply_ts=reply_ts)
            return json.loads(buf.getvalue())

        with patch.object(owner_dm_poll, "STATE_PATH", str(state_path)), patch.object(
            owner_dm_poll, "read_dm", fake_read_dm
        ), patch.object(owner_dm_poll, "send_dm", fake_send_dm):
            # (a) 代行返信3回連続: send -> resolve(replied) -> poll で streak が 1 -> 2 -> 3 と上がり戻らない
            # Round 1: result.content[0].text 内 JSON の "ts" 形式
            dm_feeds[uid] = [{"author": uid, "ts": "1795000001.100000", "thread_latest": None}]
            p1 = owner_dm_poll.poll()
            assert lock_path.exists(), "state_lock must create STATE_PATH + '.lock'"
            assert p1["wake"] is True and len(p1["pending"]) == 1
            item1 = p1["pending"][0]["id"]

            send_responses.append(
                json.dumps({
                    "result": {
                        "content": [{"type": "text", "text": json.dumps({"ok": True, "ts": "1795000002.100000"})}]
                    }
                })
            )
            s1 = owner_dm_poll.send(uid, "返信1")
            assert s1 == {"ok": True, "ts": "1795000002.100000"}, s1
            assert read_st()["convs"][uid]["auto_streak"] == 0, "send must not increment auto_streak"
            assert "1795000002.100000" in read_st()["convs"][uid]["proxy_ts"]

            r1 = call_resolve(item1, "replied")
            assert r1["ok"] is True and r1["auto_streak"] == 1

            # Poll sees proxy reply + new counterpart msg -> streak stays 1
            dm_feeds[uid] = [
                {"author": uid, "ts": "1795000003.100000", "thread_latest": None},
                {"author": owner, "ts": "1795000002.100000", "thread_latest": None},
            ]
            p2 = owner_dm_poll.poll()
            assert read_st()["convs"][uid]["auto_streak"] == 1
            assert len(p2["pending"]) == 1
            item2 = p2["pending"][0]["id"]

            # Round 2: "Message TS: ..." 表記 + thread_ts 指定
            send_responses.append(
                json.dumps({
                    "result": {
                        "content": [{"type": "text", "text": "Message sent!\nMessage TS: 1795000004.200000"}]
                    }
                })
            )
            s2 = owner_dm_poll.send(uid, "返信2", thread_ts="1795000003.100000")
            assert s2 == {"ok": True, "ts": "1795000004.200000"}, s2
            assert send_calls[-1] == (uid, "返信2", "1795000003.100000")
            r2 = call_resolve(item2, "replied")
            assert r2["ok"] is True and r2["auto_streak"] == 2

            # Poll sees proxy reply + new counterpart msg -> streak stays 2
            dm_feeds[uid] = [
                {"author": uid, "ts": "1795000005.100000", "thread_latest": None},
                {"author": owner, "ts": "1795000004.200000", "thread_latest": None},
            ]
            p3 = owner_dm_poll.poll()
            assert read_st()["convs"][uid]["auto_streak"] == 2
            item3 = p3["pending"][0]["id"]

            # ts_not_found check (while streak < 3)
            send_responses.append(
                json.dumps({"result": {"content": [{"type": "text", "text": "sent without ts"}]}})
            )
            s_missing = owner_dm_poll.send(uid, "ts欠落テスト")
            assert s_missing["ok"] is False and s_missing["error"] == "ts_not_found", s_missing
            assert s_missing.get("message"), "ts_not_found should include explanation message"

            # Round 3 -> streak reaches 3
            send_responses.append(
                json.dumps({
                    "result": {
                        "content": [{"type": "text", "text": json.dumps({"ts": "1795000006.300000"})}]
                    }
                })
            )
            s3 = owner_dm_poll.send(uid, "返信3")
            assert s3 == {"ok": True, "ts": "1795000006.300000"}, s3
            r3 = call_resolve(item3, "replied")
            assert r3["ok"] is True and r3["auto_streak"] == 3

            # Poll sees proxy reply only -> streak stays 3
            dm_feeds[uid] = [{"author": owner, "ts": "1795000006.300000", "thread_latest": None}]
            owner_dm_poll.poll()
            assert read_st()["convs"][uid]["auto_streak"] == 3

            # Attempt 4 at streak == 3 -> rejected with auto_streak_limit without calling send_dm
            calls_before = len(send_calls)
            s4 = owner_dm_poll.send(uid, "4回目は拒否")
            assert s4 == {"ok": False, "error": "auto_streak_limit"}, s4
            assert len(send_calls) == calls_before, "send_dm must not be called when auto_streak >= 3"

            # Verify CLI exits with code 1 on auto_streak_limit
            cli_chk = subprocess.run(
                [
                    str(PYTHON_BIN),
                    "-c",
                    (
                        "import sys, json, owner_dm_poll; "
                        f"owner_dm_poll.STATE_PATH = {str(state_path)!r}; "
                        "res = owner_dm_poll.send('U_NEIRO', 'blocked'); "
                        "print(json.dumps(res)); "
                        "sys.exit(0 if res.get('ok') else 1)"
                    ),
                ],
                cwd=str(BASE_DIR),
                capture_output=True,
                text=True,
            )
            assert cli_chk.returncode == 1
            assert json.loads(cli_chk.stdout) == {"ok": False, "error": "auto_streak_limit"}

            # (b) オーナー本人の実投稿（proxy_ts に無い ts）が水位より後にあれば（最新1件目でなくても）streak が 0 に戻る
            dm_feeds[uid] = [
                {"author": uid, "ts": "1795000008.000000", "thread_latest": None},
                {"author": owner, "ts": "1795000007.500000", "thread_latest": None},  # real owner post
                {"author": owner, "ts": "1795000006.300000", "thread_latest": None},
            ]
            p_reset = owner_dm_poll.poll()
            assert read_st()["convs"][uid]["auto_streak"] == 0, "real owner post must reset auto_streak to 0"
            assert len(p_reset["pending"]) == 1
            item_after_reset = p_reset["pending"][0]["id"]

            # (c) resolve --reply-ts の予備経路でも proxy_ts に記録され、次回 poll でリセットされない
            r_fb = call_resolve(item_after_reset, "replied", reply_ts="1795000009.400000")
            assert r_fb["ok"] is True and r_fb["auto_streak"] == 1
            assert "1795000009.400000" in read_st()["convs"][uid]["proxy_ts"]
            dm_feeds[uid] = [{"author": owner, "ts": "1795000009.400000", "thread_latest": None}]
            owner_dm_poll.poll()
            assert read_st()["convs"][uid]["auto_streak"] == 1, (
                "reply_ts recorded via resolve --reply-ts must prevent streak reset"
            )

            # (e) ts は文字列のまま完全一致で扱われる（末尾ゼロ落ちなし）
            send_responses.append(
                json.dumps({
                    "result": {
                        "content": [{"type": "text", "text": json.dumps({"ts": "1795000100.100000"})}]
                    }
                })
            )
            s_zero = owner_dm_poll.send(uid, "末尾ゼロ保持確認")
            assert s_zero == {"ok": True, "ts": "1795000100.100000"}
            assert "1795000100.100000" in read_st()["convs"][uid]["proxy_ts"]
            # Exact string match -> does NOT reset streak
            dm_feeds[uid] = [{"author": owner, "ts": "1795000100.100000", "thread_latest": None}]
            owner_dm_poll.poll()
            assert read_st()["convs"][uid]["auto_streak"] == 1
            # Numerically equal string without trailing zeros -> must NOT match, so resets streak!
            st_mod = read_st()
            st_mod["convs"][uid]["top_ts"] = "1795000099.000000"
            state_path.write_text(json.dumps(st_mod), encoding="utf-8")
            dm_feeds[uid] = [{"author": owner, "ts": "1795000100.1", "thread_latest": None}]
            owner_dm_poll.poll()
            assert read_st()["convs"][uid]["auto_streak"] == 0, (
                "proxy_ts matching must be exact string equality without float normalization"
            )

            # (d) proxy_ts が50件を超えたら古いものから落ちる
            st_cap = read_st()
            st_cap["convs"][uid]["auto_streak"] = 0
            st_cap["convs"][uid]["proxy_ts"] = []
            state_path.write_text(json.dumps(st_cap), encoding="utf-8")
            for i in range(55):
                ts_val = f"1795001{i:03d}.100000"
                send_responses.append(
                    json.dumps({
                        "result": {
                            "content": [{"type": "text", "text": json.dumps({"ts": ts_val})}]
                        }
                    })
                )
                res_i = owner_dm_poll.send(uid, f"msg-{i}")
                assert res_i == {"ok": True, "ts": ts_val}
            final_proxy_ts = read_st()["convs"][uid]["proxy_ts"]
            assert len(final_proxy_ts) == 50, f"expected 50 proxy_ts entries, got {len(final_proxy_ts)}"
            assert final_proxy_ts[0] == "1795001005.100000"
            assert final_proxy_ts[-1] == "1795001054.100000"
            assert "1795001000.100000" not in final_proxy_ts

    return "代行3連続でstreak 1→2→3＆auto_streak_limit拒否・本人実投稿(非最新含む)で0リセット・resolve --reply-ts記録・50件上限切捨て・ts末尾ゼロ文字列完全一致・flockロック生成を確認"


def _ops_insert_event(
    db_path: Path,
    event_id: str,
    *,
    user_id: str,
    is_bot: int = 0,
    status: str = "PENDING",
    age_sec: int = 0,
    next_retry_in_sec: Optional[int] = None,
) -> None:
    with get_db_connection(db_path) as conn:
        conn.execute(
            """
            INSERT INTO events (event_id, channel_id, user_id, ts, text, status, is_bot,
                                created_at, updated_at, next_retry_at)
            VALUES (?, 'C_OPS', ?, ?, 'ops test', ?, ?,
                    datetime('now', ?), datetime('now', ?),
                    CASE WHEN ? IS NULL THEN NULL ELSE datetime('now', ?) END)
            """,
            (
                event_id,
                user_id,
                f"{1700300000 + len(event_id)}.{uuid.uuid4().int % 1000000:06d}",
                status,
                is_bot,
                f"-{age_sec} seconds",
                f"-{age_sec} seconds",
                next_retry_in_sec,
                f"+{next_retry_in_sec or 0} seconds",
            ),
        )
        conn.commit()


def test_ops_pending_stale_check_and_outbox() -> str:
    """67. PENDING滞留チェック(候補1/11) ops_monitor.py: 人間の未回答だけを数える(自分・他Bot・既知Bot・B始まり・IGNORED・1時間待ち中を除外)・600秒しきい値・失敗時exit 2・outboxの取り出し/戻し"""
    import ops_monitor  # noqa: E402

    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        db_path = tmp_dir / "slack_events.db"
        init_db(db_path)
        hb = tmp_dir / "heartbeat.json"
        hb.write_text(json.dumps({"bot_user_id": "U_SELF_HB"}), encoding="utf-8")

        # DBなしは ok・stale=false
        r0 = ops_monitor.pending_check(db_path=tmp_dir / "missing.db", self_user_ids=[])
        assert r0["ok"] is True and r0["stale"] is False and r0["human_pending"] == 0

        _ops_insert_event(db_path, "Ev_H_FRESH", user_id="U_HUMAN_A", age_sec=120)
        r1 = ops_monitor.pending_check(db_path=db_path, threshold_sec=600, heartbeat_path=hb, self_user_ids=["U_SELF"])
        assert r1["stale"] is False and r1["human_pending"] == 1 and r1["oldest_event_id"] == "Ev_H_FRESH"

        # 自分・他Bot(is_bot=1)・既知Botの is_bot=0 行・B始まり・IGNORED_USER_IDS・待ち中は数えない
        _ops_insert_event(db_path, "Ev_SELF_OLD", user_id="U_SELF", age_sec=5000)
        _ops_insert_event(db_path, "Ev_BOT_OLD", user_id="U_OTHER_BOT", is_bot=1, age_sec=5000)
        _ops_insert_event(db_path, "Ev_KNOWN_BOT_OLD", user_id="U_OTHER_BOT", is_bot=0, age_sec=5000)
        _ops_insert_event(db_path, "Ev_BID_OLD", user_id="B0BOTID", age_sec=5000)
        _ops_insert_event(db_path, "Ev_IGN_OLD", user_id="U_IGNORED", age_sec=5000)
        _ops_insert_event(db_path, "Ev_WAIT_OLD", user_id="U_HUMAN_B", age_sec=5000, next_retry_in_sec=1800)
        _ops_insert_event(db_path, "Ev_DONE_OLD", user_id="U_HUMAN_C", status="DONE", age_sec=5000)
        with patch.dict(os.environ, {"IGNORED_USER_IDS": "U_IGNORED"}):
            r2 = ops_monitor.pending_check(db_path=db_path, threshold_sec=600, heartbeat_path=hb, self_user_ids=["U_SELF"])
        assert r2["stale"] is False, r2
        assert r2["human_pending"] == 1 and r2["excluded_bot_or_self"] == 5 and r2["excluded_waiting"] == 1, r2

        # heartbeat の bot_user_id も自分として除外される(self_user_ids 未指定時)
        _ops_insert_event(db_path, "Ev_SELF_HB_OLD", user_id="U_SELF_HB", age_sec=5000)
        with patch.dict(os.environ, {"IGNORED_USER_IDS": "U_IGNORED", "YAMA_BOT_USER_ID": "U_SELF"}):
            r3 = ops_monitor.pending_check(db_path=db_path, threshold_sec=600, heartbeat_path=hb)
        assert r3["stale"] is False and r3["excluded_bot_or_self"] == 6, r3

        # 人間の投稿が600秒を超えたら stale
        _ops_insert_event(db_path, "Ev_H_OLD", user_id="U_HUMAN_D", age_sec=650)
        with patch.dict(os.environ, {"IGNORED_USER_IDS": "U_IGNORED", "YAMA_BOT_USER_ID": "U_SELF"}):
            r4 = ops_monitor.pending_check(db_path=db_path, threshold_sec=600, heartbeat_path=hb)
        assert r4["stale"] is True and r4["stale_count"] == 1, r4
        assert r4["oldest_event_id"] == "Ev_H_OLD" and 640 <= r4["oldest_age_sec"] <= 700, r4

        # CLI: 正常は exit 0 の JSON、失敗は exit 2 の {"ok": false}
        env = os.environ.copy()
        env.update({"YAMA_HEARTBEAT_PATH": str(hb), "YAMA_BOT_USER_ID": "U_SELF", "IGNORED_USER_IDS": "U_IGNORED"})
        cli = subprocess.run(
            [str(PYTHON_BIN), str(BASE_DIR / "ops_monitor.py"), "pending-check", "--db", str(db_path)],
            env=env, capture_output=True, text=True,
        )
        assert cli.returncode == 0 and json.loads(cli.stdout)["stale"] is True, cli
        env_fail = dict(env, YAMA_PENDING_CHECK_FORCE_FAIL="1")
        cli_f = subprocess.run(
            [str(PYTHON_BIN), str(BASE_DIR / "ops_monitor.py"), "pending-check", "--db", str(db_path)],
            env=env_fail, capture_output=True, text=True,
        )
        assert cli_f.returncode == 2 and json.loads(cli_f.stdout)["ok"] is False, cli_f
        bad_db = tmp_dir / "not_a_db.db"
        bad_db.write_text("garbage" * 100, encoding="utf-8")
        cli_b = subprocess.run(
            [str(PYTHON_BIN), str(BASE_DIR / "ops_monitor.py"), "pending-check", "--db", str(bad_db)],
            env=env, capture_output=True, text=True,
        )
        assert cli_b.returncode == 2 and json.loads(cli_b.stdout)["ok"] is False, cli_b

        # outbox: append -> take(空になる) -> restore(戻る)・壊れた行は unparseable として残す
        outbox = tmp_dir / "ops_alerts.jsonl"
        ops_monitor.outbox_append({"kind": "reaper", "reaped_count": 1}, outbox)
        with open(outbox, "a", encoding="utf-8") as f:
            f.write("{broken\n")
        taken = ops_monitor.outbox_take(outbox)
        assert [e["kind"] for e in taken] == ["reaper", "unparseable_outbox_line"], taken
        assert outbox.read_text(encoding="utf-8") == ""
        assert ops_monitor.outbox_take(outbox) == []
        rs = subprocess.run(
            [str(PYTHON_BIN), str(BASE_DIR / "ops_monitor.py"), "outbox-restore", "--path", str(outbox)],
            input=json.dumps(taken[:1]), capture_output=True, text=True,
        )
        assert rs.returncode == 0
        assert [e["kind"] for e in ops_monitor.outbox_take(outbox)] == ["reaper"]

    return "自分・他Bot・既知Bot・B始まり・IGNORED・1時間待ち中を除外し人間の未回答だけを計数・600秒超でstale・CLIの失敗はexit 2・outboxの取り出し/戻し/壊れ行保持を確認"


def test_hook_ops_alert_block_edge_wake() -> str:
    """68. フックの運用警告ブロック (ライブフックのbegin〜endを実行: PENDING滞留のエッジ通知・復旧1回・outbox配送・wake失敗時の状態ロールバック＆outbox復元・監視失敗の即時表面化・ディスパッチ回は送らない) と構文ガードのwake=exit前提での重複抑止"""
    hook_script = HOOK_SCRIPT_PATH
    script_text = hook_script.read_text(encoding="utf-8")
    assert "ops_monitor.py" in script_text and "pending-check" in script_text
    begin = script_text.index("# --- ops alerts: begin")
    end = script_text.index("# --- ops alerts: end ---")
    ops_src = script_text[begin:end]
    sg_begin = script_text.index("# --- syntax-guard escalation: begin")
    sg_end = script_text.index("# --- syntax-guard escalation: end ---")
    sg_src = script_text[sg_begin:sg_end]
    # 運用警告ブロックは構文ガードの後・ディスパッチ結果の wake の前
    assert sg_end < begin < script_text.index('wake "New Slack events dispatched for Yama triage"')

    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        db_path = tmp_dir / "slack_events.db"
        init_db(db_path)
        state_file = tmp_dir / "hook_state.json"
        wake_log = tmp_dir / "wakes.jsonl"
        watchdog_log = tmp_dir / "watchdog.log"
        outbox = tmp_dir / "ops_alerts.jsonl"
        hb = tmp_dir / "heartbeat.json"
        hb.write_text(json.dumps({"bot_user_id": "U_SELF"}), encoding="utf-8")
        prelude = (
            "#!/usr/bin/env bash\n"
            "set -euo pipefail\n"
            'STATE_FILE="$1"\nWAKE_LOG="$2"\n'
            f'PROJECT_DIR="{BASE_DIR}"\n'
            f'PYTHON_BIN="{PYTHON_BIN}"\n'
            f'DB_PATH="{db_path}"\n'
            f'WATCHDOG_LOG="{watchdog_log}"\n'
            'code="${STUB_CODE:-1}"\n'
            "hook_state_get() { if [[ -f \"$STATE_FILE\" ]]; then jq -ce 'select(type == \"object\")' <\"$STATE_FILE\" 2>/dev/null || printf '{}'; else printf '{}'; fi; }\n"
            "hook_state_set() { local v; v=\"$(printf '%s' \"$1\" | jq -ce 'select(type == \"object\")')\" || return 1; printf '%s\\n' \"$v\" >\"$STATE_FILE\"; }\n"
            # 実ランタイムと同じく、成功した wake はフックを終了させる
            "wake() { jq -n -c --arg t \"$1\" --arg p \"$2\" '{\"title\": $t, \"payload\": $p}' >>\"$WAKE_LOG\"; "
            "if [[ \"${STUB_WAKE_FAIL:-0}\" == \"1\" ]]; then return 1; fi; echo WAKE_EXIT; exit 0; }\n"
        )
        ops_stub = tmp_dir / "ops_stub.sh"
        ops_stub.write_text(prelude + ops_src + '\necho "AFTER_OPS_OK"\n', encoding="utf-8")
        sg_stub = tmp_dir / "sg_stub.sh"
        sg_stub.write_text(
            prelude + 'WD_OK="${STUB_WD_OK:-1}"\nDISP_OK=1\nLISTENER_OK=1\n' + sg_src + '\necho "AFTER_SG_OK"\n',
            encoding="utf-8",
        )

        base_env = os.environ.copy()
        base_env.update(
            {
                "YAMA_OPS_ALERTS_PATH": str(outbox),
                "YAMA_HEARTBEAT_PATH": str(hb),
                "YAMA_BOT_USER_ID": "U_SELF",
                "IGNORED_USER_IDS": "",
            }
        )
        base_env.pop("YAMA_PENDING_CHECK_FORCE_FAIL", None)

        def run(stub: Path, **extra: str) -> Tuple[List[Dict[str, Any]], str]:
            before = len(wake_log.read_text(encoding="utf-8").splitlines()) if wake_log.exists() else 0
            env = dict(base_env, **extra)
            res = subprocess.run(["bash", str(stub), str(state_file), str(wake_log)], env=env, capture_output=True, text=True)
            assert res.returncode == 0, f"stub failed rc={res.returncode}: {res.stderr}"
            lines = wake_log.read_text(encoding="utf-8").splitlines() if wake_log.exists() else []
            return [json.loads(x) for x in lines[before:]], res.stdout

        def ops_state() -> Dict[str, Any]:
            if not state_file.exists():
                return {}
            return json.loads(state_file.read_text(encoding="utf-8")).get("ops_monitor", {})

        # (a) 過渡ノイズ: 5分の人間PENDING・古いBot/自分のPENDINGでは何も送らない
        _ops_insert_event(db_path, "Ev_HK_FRESH", user_id="U_HUMAN", age_sec=300)
        _ops_insert_event(db_path, "Ev_HK_BOT", user_id="U_BOT", is_bot=1, age_sec=4000)
        _ops_insert_event(db_path, "Ev_HK_SELF", user_id="U_SELF", age_sec=4000)
        wakes, out = run(ops_stub)
        assert wakes == [] and "AFTER_OPS_OK" in out

        # (b) 600秒超の人間PENDING -> 1回だけ wake(payload は jq で検証できる JSON)
        _ops_insert_event(db_path, "Ev_HK_OLD", user_id="U_HUMAN", age_sec=700)
        wakes, out = run(ops_stub)
        assert len(wakes) == 1 and "WAKE_EXIT" in out and "AFTER_OPS_OK" not in out, (wakes, out)
        assert wakes[0]["title"] == "Ops alert: pending_stale"
        p = json.loads(wakes[0]["payload"])
        assert p["ops_alert"] is True and p["kinds"] == ["pending_stale"]
        assert p["alerts"][0]["oldest_event_id"] == "Ev_HK_OLD" and p["alerts"][0]["stale_count"] == 1
        assert "Slack には投稿せず" in p["note"]
        assert ops_state() == {"pending_stale_warned": True, "monitor_failed_warned": False}
        # 続いている間は送らない
        wakes, out = run(ops_stub)
        assert wakes == [] and "AFTER_OPS_OK" in out

        # (c) ディスパッチした回(code 0)は outbox も状態も触らない
        backfill._append_ops_alert({"kind": "reaper", "reaped_count": 1, "reaped_event_ids": ["Ev_X"],
                                    "failed_count": 0, "failed_event_ids": []}, outbox)
        wakes, out = run(ops_stub, STUB_CODE="0")
        assert wakes == [] and "AFTER_OPS_OK" in out
        assert outbox.read_text(encoding="utf-8").strip() != ""

        # (d) outbox 配送: wake 失敗時は outbox を戻し状態も変えない -> 次回成功で配送され outbox は空
        wakes, out = run(ops_stub, STUB_WAKE_FAIL="1")
        assert len(wakes) == 1 and "AFTER_OPS_OK" in out
        assert json.loads(wakes[0]["payload"])["kinds"] == ["reaper"]
        assert json.loads(outbox.read_text(encoding="utf-8").strip())["reaped_event_ids"] == ["Ev_X"]
        assert "Ops alert: wake failed" in watchdog_log.read_text(encoding="utf-8")
        wakes, out = run(ops_stub)
        assert len(wakes) == 1 and wakes[0]["title"] == "Ops alert: reaper"
        assert outbox.read_text(encoding="utf-8") == ""

        # (e) 復旧したら1回だけ通知。wake 失敗時は warned を戻して次回再送
        with get_db_connection(db_path) as conn:
            conn.execute("UPDATE events SET status = 'DONE' WHERE event_id IN ('Ev_HK_OLD', 'Ev_HK_FRESH')")
            conn.commit()
        wakes, out = run(ops_stub, STUB_WAKE_FAIL="1")
        assert len(wakes) == 1 and wakes[0]["title"] == "Ops alert: pending_stale_recovered"
        assert ops_state()["pending_stale_warned"] is True, "rollback after failed wake"
        wakes, out = run(ops_stub)
        assert len(wakes) == 1 and wakes[0]["title"] == "Ops alert: pending_stale_recovered"
        assert ops_state()["pending_stale_warned"] is False
        wakes, out = run(ops_stub)
        assert wakes == []

        # (f) 監視処理自体の失敗は猶予なしで即 wake(1回)・ログには毎回・直ったら復旧1回
        wakes, out = run(ops_stub, YAMA_PENDING_CHECK_FORCE_FAIL="1")
        assert len(wakes) == 1 and wakes[0]["title"] == "Ops alert: ops_monitor_failed"
        det = json.loads(wakes[0]["payload"])["alerts"][0]["details"][0]
        assert det["step"] == "pending-check" and det["exit_code"] == 2
        wakes, out = run(ops_stub, YAMA_PENDING_CHECK_FORCE_FAIL="1")
        assert wakes == [] and "AFTER_OPS_OK" in out
        assert watchdog_log.read_text(encoding="utf-8").count("ops_monitor failed") == 2
        wakes, out = run(ops_stub)
        assert len(wakes) == 1 and wakes[0]["title"] == "Ops alert: ops_monitor_recovered"

        # (g) 警告書き込み失敗(dispatcher/backfill の stderr 由来)は毎回そのまま転送
        extra = json.dumps([{"kind": "alert_write_failed", "source": "dispatcher.sh", "stderr": "REAPER_ALERT_WRITE_FAILED: x"}])
        wakes, out = run(ops_stub, OPS_EXTRA_ALERTS=extra)
        assert len(wakes) == 1 and wakes[0]["title"] == "Ops alert: alert_write_failed"

        # (h) 構文ガード: 実ランタイム同様 wake で終了しても、警告は1回だけ(以前は wake 後の warned 保存が走らず毎回警告)
        sg_env = {"YAMA_SYNTAX_GUARD_NOW": "1000", "STUB_WD_OK": "0"}
        wakes, out = run(sg_stub, **sg_env)
        assert wakes == [] and "AFTER_SG_OK" in out
        wakes, out = run(sg_stub, **dict(sg_env, YAMA_SYNTAX_GUARD_NOW="1600"))
        assert len(wakes) == 1 and "WAKE_EXIT" in out
        wakes, out = run(sg_stub, **dict(sg_env, YAMA_SYNTAX_GUARD_NOW="1630"))
        assert wakes == [] and "AFTER_SG_OK" in out, "syntax guard must not re-warn every cycle"
        # ディスパッチした回(code 0)は構文ガードを評価せず、ペイロードの wake を奪わない
        wakes, out = run(sg_stub, **dict(sg_env, YAMA_SYNTAX_GUARD_NOW="1700", STUB_WD_OK="1", STUB_CODE="0"))
        assert wakes == [] and "AFTER_SG_OK" in out
        wakes, out = run(sg_stub, **dict(sg_env, YAMA_SYNTAX_GUARD_NOW="1730", STUB_WD_OK="1"))
        assert len(wakes) == 1 and wakes[0]["title"] == "Syntax guard: target(s) recovered"

    return "ライブフックの運用警告ブロックを実行し、過渡ノイズ無視・600秒超で1回wake・継続中は無通知・ディスパッチ回は保留・wake失敗時のoutbox復元＆状態ロールバック・復旧1回・監視失敗の即時1回＆毎回ログ・書込失敗の転送を確認。構文ガードもwake=exit前提で重複警告なし・ディスパッチ回は評価しないことを確認"


def test_reaper_reap_count_cap_and_requeue() -> str:
    """69. Reaper差し戻し上限(候補12): reap_count列の後方互換追加・差し戻しごとに+1・3回目でFAILED(reaped_too_many_times)・retry_count不変・実行ごとの集約アラート・書込失敗はstderr・手動requeueだけがreap_countを0に戻す"""
    with tempfile.TemporaryDirectory() as tmp:
        state_dir = Path(tmp) / "state"
        state_dir.mkdir(parents=True, exist_ok=True)
        db_path = state_dir / "slack_events.db"
        payload_path = state_dir / "payload.json"
        mode_file = Path(tmp) / "mode.txt"
        mode_file.write_text("full\n", encoding="utf-8")
        outbox = state_dir / "ops_alerts.jsonl"

        # 旧スキーマ(reap_count 列なし)の既存DBを用意 -> dispatcher が列を追加する
        conn = sqlite3.connect(str(db_path))
        conn.execute(
            """
            CREATE TABLE events (
                event_id TEXT PRIMARY KEY, channel_id TEXT, user_id TEXT, ts TEXT, thread_ts TEXT,
                text TEXT, status TEXT NOT NULL DEFAULT 'PENDING', is_bot INTEGER NOT NULL DEFAULT 0,
                retry_count INTEGER NOT NULL DEFAULT 0, error_message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            "INSERT INTO events (event_id, channel_id, user_id, ts, text, status, retry_count) "
            "VALUES ('Ev_REAP_A', 'C_BOARD', 'U_01', '1700400001.000000', 'a', 'PENDING', 1)"
        )
        conn.execute(
            "INSERT INTO events (event_id, channel_id, user_id, ts, text, status) "
            "VALUES ('Ev_REAP_B', 'C_BOARD', 'U_02', '1700400002.000000', 'b', 'PENDING')"
        )
        conn.commit()
        conn.close()

        env = os.environ.copy()
        env.update(
            {
                "YAMA_STATE_DIR": str(state_dir),
                "YAMA_DB_PATH": str(db_path),
                "YAMA_PAYLOAD_PATH": str(payload_path),
                "YAMA_LOCK_PATH": str(state_dir / "dispatcher.lock"),
                "YAMA_MODE_FILE": str(mode_file),
                "YAMA_PYTHON_BIN": str(PYTHON_BIN),
                "YAMA_OPS_ALERTS_PATH": str(outbox),
            }
        )
        env.pop("YAMA_REAP_LIMIT", None)

        def dispatch() -> subprocess.CompletedProcess:
            return subprocess.run(["bash", str(DISPATCHER_SH)], env=env, capture_output=True, text=True)

        def age(ids: List[str]) -> None:
            stale = (datetime.now(timezone.utc) - timedelta(seconds=960)).strftime("%Y-%m-%d %H:%M:%S")
            with get_db_connection(db_path) as c:
                for eid in ids:
                    c.execute("UPDATE events SET updated_at = ? WHERE event_id = ?", (stale, eid))
                c.commit()

        def rows() -> Dict[str, Dict[str, Any]]:
            with get_db_connection(db_path) as c:
                return {
                    r["event_id"]: dict(r)
                    for r in c.execute("SELECT event_id, status, reap_count, retry_count, error_message FROM events")
                }

        r = dispatch()
        assert r.returncode == 0 and r.stdout.strip() == "DISPATCHED:2", r
        assert {v["reap_count"] for v in rows().values()} == {0}, "column added with default 0"
        assert not outbox.exists() or outbox.read_text(encoding="utf-8") == "", "no alert when nothing reaped"

        # 1回目・2回目の差し戻し: PENDING に戻り(同じ回で再ディスパッチ) reap_count が 1, 2
        for n in (1, 2):
            age(["Ev_REAP_A", "Ev_REAP_B"])
            r = dispatch()
            assert r.returncode == 0 and r.stdout.strip() == "DISPATCHED:2", r
            st = rows()
            assert st["Ev_REAP_A"]["reap_count"] == n and st["Ev_REAP_B"]["reap_count"] == n, st
            assert st["Ev_REAP_A"]["retry_count"] == 1 and st["Ev_REAP_B"]["retry_count"] == 0, "retry_count untouched"
        alerts = [json.loads(x) for x in outbox.read_text(encoding="utf-8").splitlines()]
        assert len(alerts) == 2, "one aggregated line per Reaper run"
        assert alerts[0]["kind"] == "reaper" and alerts[0]["reaped_count"] == 2
        assert sorted(alerts[0]["reaped_event_ids"]) == ["Ev_REAP_A", "Ev_REAP_B"] and alerts[0]["failed_event_ids"] == []

        # 3回目: 累計3回に達したので FAILED(reaped_too_many_times) で止める
        age(["Ev_REAP_A", "Ev_REAP_B"])
        r = dispatch()
        assert r.returncode == 1 and r.stdout.strip() == "NO_EVENTS", r
        st = rows()
        for eid in ("Ev_REAP_A", "Ev_REAP_B"):
            assert st[eid]["status"] == "FAILED" and st[eid]["reap_count"] == 3, st[eid]
            assert st[eid]["error_message"].startswith("reaped_too_many_times"), st[eid]
        assert st["Ev_REAP_A"]["retry_count"] == 1
        alerts = [json.loads(x) for x in outbox.read_text(encoding="utf-8").splitlines()]
        assert len(alerts) == 3 and alerts[2]["reaped_count"] == 0 and alerts[2]["failed_count"] == 2
        assert sorted(alerts[2]["failed_event_ids"]) == ["Ev_REAP_A", "Ev_REAP_B"]

        # FAILED のままなら再ディスパッチされない
        r = dispatch()
        assert r.stdout.strip() == "NO_EVENTS"

        # 手動 requeue: FAILED -> PENDING、reap_count だけ 0 に戻す(retry_count は維持)
        out = io.StringIO()
        with redirect_stdout(out):
            rc = agent_tools.main(["--db", str(db_path), "requeue", "--event-id", "Ev_REAP_A"])
        assert rc == 0, out.getvalue()
        st = rows()
        assert st["Ev_REAP_A"]["status"] == "PENDING" and st["Ev_REAP_A"]["reap_count"] == 0
        assert st["Ev_REAP_A"]["retry_count"] == 1
        # FAILED 以外の requeue・存在しない event_id は拒否
        err = io.StringIO()
        with redirect_stderr(err):
            assert agent_tools.main(["--db", str(db_path), "requeue", "--event-id", "Ev_REAP_A"]) == 1
            assert agent_tools.main(["--db", str(db_path), "requeue", "--event-id", "Ev_NOPE"]) == 1
        # 通常の FAILED リトライ経路(commit)は reap_count を変えない
        r = dispatch()
        assert r.stdout.strip() == "DISPATCHED:1"
        commit_event("Ev_REAP_A", "FAILED", error_message="agent error", db_path=db_path)
        assert rows()["Ev_REAP_A"]["reap_count"] == 0

        # アラート書き込み失敗: 差し戻し自体は行い、stderr に REAPER_ALERT_WRITE_FAILED、終了コードは変えない
        with get_db_connection(db_path) as c:
            c.execute("UPDATE events SET status = 'PROCESSING' WHERE event_id = 'Ev_REAP_A'")
            c.commit()
        age(["Ev_REAP_A"])
        bad_env = dict(env, YAMA_OPS_ALERTS_PATH=str(state_dir))  # ディレクトリなので開けない
        r = subprocess.run(["bash", str(DISPATCHER_SH)], env=bad_env, capture_output=True, text=True)
        assert r.returncode == 0 and r.stdout.strip() == "DISPATCHED:1", r
        assert "REAPER_ALERT_WRITE_FAILED" in r.stderr and "Ev_REAP_A" in r.stderr
        assert rows()["Ev_REAP_A"]["reap_count"] == 1

        # 上限は YAMA_REAP_LIMIT で変えられる(既定3)
        age(["Ev_REAP_A"])
        r = subprocess.run(["bash", str(DISPATCHER_SH)], env=dict(env, YAMA_REAP_LIMIT="2"), capture_output=True, text=True)
        assert rows()["Ev_REAP_A"]["status"] == "FAILED" and rows()["Ev_REAP_A"]["reap_count"] == 2

    return "旧DBへのreap_count列追加・差し戻しごとの+1・累計3回でFAILED(reaped_too_many_times)・retry_count不変・実行ごと1行の集約アラート(件数＋ID一覧)・書込失敗時もディスパッチ継続＆stderr表面化・手動requeueのみreap_count=0・commit経路は不変を確認"


def test_backfill_history_paging_and_truncation_alert() -> str:
    """70. backfill履歴ページング(候補13): 30件x最大3ページをcursorで読む・続きがあるのに打ち切ったら警告(stderr＋outboxにエッジで1回、解消時1回)・2ページで終われば警告なし・3ページ目の投稿も取り込む"""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "slack_events.db"
        init_db(db_path)
        outbox = Path(tmp) / "ops_alerts.jsonl"
        ch_welcome, ch_board = TARGET_CHANNELS
        base_now = 1700500000.0
        calls: List[Dict[str, Any]] = []
        pages_by_channel = {ch_welcome: 5, ch_board: 2}

        class PagingClient:
            def auth_test(self) -> Dict[str, Any]:
                return {"ok": True, "user_id": "U_YAMA_BOT"}

            def conversations_history(self, **kwargs: Any) -> Dict[str, Any]:
                calls.append(kwargs)
                ch = kwargs["channel"]
                assert kwargs["limit"] == 30
                page = int((kwargs.get("cursor") or "p1")[1:])
                total = pages_by_channel[ch]
                msgs = [
                    {
                        "type": "message",
                        "user": "U_HUMAN",
                        "text": f"{ch} page {page} msg {i}",
                        "ts": f"{base_now - page * 100 - i:.6f}",
                    }
                    for i in range(2)
                ]
                more = page < total
                return {
                    "ok": True,
                    "messages": msgs,
                    "has_more": more,
                    "response_metadata": {"next_cursor": f"p{page + 1}" if more else ""},
                }

            def conversations_replies(self, **kwargs: Any) -> Dict[str, Any]:
                return {"ok": True, "messages": [], "has_more": False}

        env_path = Path(tmp) / "missing.env"
        err = io.StringIO()
        with patch.dict(os.environ, {"YAMA_OPS_ALERTS_PATH": str(outbox)}), redirect_stderr(err):
            res = run_backfill(db_path=db_path, env_path=env_path, client=PagingClient(), now=base_now, sleep_fn=lambda s: None)
        wel_calls = [c for c in calls if c["channel"] == ch_welcome]
        brd_calls = [c for c in calls if c["channel"] == ch_board]
        assert len(wel_calls) == 3, f"capped at 3 pages, got {len(wel_calls)}"
        assert [c.get("cursor") for c in wel_calls] == [None, "p2", "p3"]
        assert len({c["oldest"] for c in wel_calls}) == 1, "same oldest window on every page"
        assert len(brd_calls) == 2, "stops when no next cursor"
        assert res["truncated"] == [ch_welcome], res
        assert res["channels"][ch_welcome] == 6 and res["channels"][ch_board] == 4, res
        assert "history page cap reached" in err.getvalue()
        alerts = [json.loads(x) for x in outbox.read_text(encoding="utf-8").splitlines()]
        assert len(alerts) == 1 and alerts[0]["kind"] == "backfill_truncated" and alerts[0]["channel"] == ch_welcome
        assert alerts[0]["max_pages"] == 3 and alerts[0]["per_page"] == 30 and alerts[0]["pages"] == 3

        # 打ち切りが続いている間は outbox に追加しない(エッジ)
        with patch.dict(os.environ, {"YAMA_OPS_ALERTS_PATH": str(outbox)}), redirect_stderr(io.StringIO()):
            run_backfill(db_path=db_path, env_path=env_path, client=PagingClient(), now=base_now, sleep_fn=lambda s: None)
        assert len(outbox.read_text(encoding="utf-8").splitlines()) == 1

        # 解消したら1回だけ recovered
        pages_by_channel[ch_welcome] = 1
        with patch.dict(os.environ, {"YAMA_OPS_ALERTS_PATH": str(outbox)}), redirect_stderr(io.StringIO()):
            res3 = run_backfill(db_path=db_path, env_path=env_path, client=PagingClient(), now=base_now, sleep_fn=lambda s: None)
        assert res3["truncated"] == []
        alerts = [json.loads(x) for x in outbox.read_text(encoding="utf-8").splitlines()]
        assert [a["kind"] for a in alerts] == ["backfill_truncated", "backfill_truncation_recovered"]

        # outbox に書けないときはエッジ状態を進めず、次回に再試行する
        pages_by_channel[ch_welcome] = 4
        bad = Path(tmp) / "as_dir"
        bad.mkdir()
        err5 = io.StringIO()
        with patch.dict(os.environ, {"YAMA_OPS_ALERTS_PATH": str(bad)}), redirect_stderr(err5):
            run_backfill(db_path=db_path, env_path=env_path, client=PagingClient(), now=base_now, sleep_fn=lambda s: None)
        assert "could not write ops alert" in err5.getvalue()
        with patch.dict(os.environ, {"YAMA_OPS_ALERTS_PATH": str(outbox)}), redirect_stderr(io.StringIO()):
            run_backfill(db_path=db_path, env_path=env_path, client=PagingClient(), now=base_now, sleep_fn=lambda s: None)
        alerts = [json.loads(x) for x in outbox.read_text(encoding="utf-8").splitlines()]
        assert [a["kind"] for a in alerts][-1] == "backfill_truncated"

    return "3ページ(cursor p2,p3・同一oldest)で打ち切り＆3ページ目まで取り込み・2ページで自然終了は警告なし・打ち切り警告はstderr＋outboxにエッジで1回・解消時1回・outbox書込失敗時は状態を進めず次回再試行を確認"


def test_listener_freeze_guards_and_sd_notify() -> str:
    """71. リスナー凍結対策(項目5): pong途絶判定(接続中かつ90秒以上のみ・pong未受信はNone扱いで終了しない)・sd_notify WATCHDOG=1 の送信(NOTIFY_SOCKET未設定はno-op・抽象ソケット対応)・ユニットの WatchdogSec/NotifyAccess"""
    import socket as _socket

    import listener

    class Sess:
        def __init__(self, t: Optional[float]) -> None:
            self.last_ping_pong_time = t

    class Cli:
        def __init__(self, sess: Any) -> None:
            self.current_session = sess

    now = 1700600000.0
    assert listener.get_pong_silence_sec(Cli(None), now) is None
    assert listener.get_pong_silence_sec(Cli(Sess(None)), now) is None
    assert listener.get_pong_silence_sec(Cli(Sess(now - 95)), now) == 95.0
    assert listener.get_pong_silence_sec(Cli(Sess(now + 5)), now) == 0.0
    assert listener.get_pong_silence_sec(Cli(Sess("bad")), now) is None

    assert listener.MAX_PONG_SILENCE_SEC == 90
    assert listener.should_exit_for_pong_silence(True, 90.0) is True
    assert listener.should_exit_for_pong_silence(True, 89.9) is False
    assert listener.should_exit_for_pong_silence(True, None) is False, "no pong yet must not exit"
    assert listener.should_exit_for_pong_silence(False, 500.0) is False, "disconnect path is the 60s rule"

    # sd_notify: 未設定は False で何もしない
    assert listener.sd_notify("WATCHDOG=1", env={}) is False
    with tempfile.TemporaryDirectory() as tmp:
        sock_path = str(Path(tmp) / "notify.sock")
        srv = _socket.socket(_socket.AF_UNIX, _socket.SOCK_DGRAM)
        srv.bind(sock_path)
        srv.settimeout(2)
        try:
            assert listener.sd_notify("WATCHDOG=1", env={"NOTIFY_SOCKET": sock_path}) is True
            assert srv.recv(64) == b"WATCHDOG=1"
        finally:
            srv.close()
        # 送信先が無いときは OSError(呼び出し側がログに出す)
        try:
            listener.sd_notify("WATCHDOG=1", env={"NOTIFY_SOCKET": sock_path + ".missing"})
            raise AssertionError("expected OSError")
        except OSError:
            pass
    abstract = f"@yama-test-{uuid.uuid4().hex}"
    srv2 = _socket.socket(_socket.AF_UNIX, _socket.SOCK_DGRAM)
    srv2.bind("\0" + abstract[1:])
    srv2.settimeout(2)
    try:
        assert listener.sd_notify("WATCHDOG=1", env={"NOTIFY_SOCKET": abstract}) is True
        assert srv2.recv(64) == b"WATCHDOG=1"
    finally:
        srv2.close()

    # ping 間隔は WatchdogSec より十分短く、WatchdogSec は watchdog.sh の窓(120秒+再確認+30秒周期)より長い
    unit = (BASE_DIR / "yama-listener.service").read_text(encoding="utf-8")
    kv = dict(
        line.split("=", 1) for line in unit.splitlines() if "=" in line and not line.startswith("#")
    )
    assert kv.get("NotifyAccess") == "main", "Type=simple needs NotifyAccess=main for WATCHDOG=1"
    wd_sec = int(kv["WatchdogSec"])
    assert wd_sec >= 180 and listener.WATCHDOG_PING_INTERVAL_SEC * 3 <= wd_sec
    assert kv.get("Restart") == "always"
    src = (BASE_DIR / "listener.py").read_text(encoding="utf-8")
    assert 'sd_notify("WATCHDOG=1")' in src and "should_exit_for_pong_silence(connected, pong_silence)" in src

    return "pong途絶は接続中かつ90秒以上だけで終了・pong未受信や切断中は対象外・sd_notifyの送信/未設定no-op/抽象ソケット/送信失敗OSError・ユニットのWatchdogSec(>=180)とNotifyAccess=mainを確認"


def test_history_compact_view_and_text_bounds() -> str:
    """72. 長文バースト対策: history の文量上限 (コンパクト表示・1投稿の切り詰め・合計上限で古い順に省略・これ以前ありの明記・--limit上限・--raw)"""
    parent_ts = "1700009000.000000"
    calls: List[Dict[str, Any]] = []

    def _bot_msg(i: int, text: str) -> Dict[str, Any]:
        return {
            "type": "message",
            "user": f"U_{i:03d}",
            "bot_id": "B_NEIRO",
            "app_id": "A_X",
            "team": "T_X",
            "text": text,
            "ts": f"17000090{i:02d}.000100",
            "thread_ts": parent_ts,
            "blocks": [{"type": "rich_text", "elements": [{"type": "rich_text_section",
                                                           "elements": [{"type": "text", "text": text}]}]}],
            "bot_profile": {"name": "Neiro", "icons": {"image_72": "https://x/" + "i" * 200}},
            "attachments": [{"title": "GitHub - repo", "text": "d" * 1000,
                             "from_url": "https://github.com/x/y", "service_icon": "https://x/icon.png"}],
            "metadata": {"event_type": "neiro_reply", "event_payload": {"event_id": f"Ev{i}"}},
        }

    class ThreadClient:
        def __init__(self, messages: List[Dict[str, Any]]) -> None:
            self.messages = messages

        def conversations_replies(self, **kw: Any) -> Dict[str, Any]:
            calls.append(kw)
            return {"ok": True, "messages": self.messages, "has_more": True}

        def conversations_history(self, **kw: Any) -> Dict[str, Any]:
            calls.append(kw)
            return {"ok": True, "messages": self.messages, "has_more": True}

    parent = {"type": "message", "user": "U_OWNER", "text": "親投稿", "ts": parent_ts,
              "thread_ts": parent_ts, "reply_count": 120, "reply_users": ["U_1"] * 50,
              "blocks": [{"type": "rich_text", "elements": []}]}

    def run(args: Namespace) -> Dict[str, Any]:
        buf = io.StringIO()
        with redirect_stdout(buf):
            rc = cmd_history(args)
        assert rc == 0
        return json.loads(buf.getvalue())

    with tempfile.TemporaryDirectory() as tmp:
        db = str(Path(tmp) / "x.db")

        # 1) Compact view: render-only fields dropped, judgement fields kept, size shrinks.
        replies = [_bot_msg(i, f"返信{i} " + "あ" * 200) for i in range(1, 21)]
        client = ThreadClient([parent] + replies)
        args = Namespace(channel="C_T", thread_ts=parent_ts, limit=20, db=db)
        with patch.object(agent_tools, "get_web_client", return_value=client):
            out = run(args)
            raw = run(Namespace(channel="C_T", thread_ts=parent_ts, limit=20, db=db, raw=True))
        m = out["messages"][1]
        for k in ("blocks", "bot_profile", "app_id", "team"):
            assert k not in m, f"{k} must be dropped: {m.keys()}"
        assert m["user"] == "U_001" and m["bot_id"] == "B_NEIRO" and m["is_bot"] == 1
        assert m["bot_name"] == "Neiro" and m["metadata"]["event_type"] == "neiro_reply"
        assert m["text"] == replies[0]["text"] and m["thread_ts"] == parent_ts
        att = m["attachments"][0]
        assert att["title"] == "GitHub - repo" and att["url"] == "https://github.com/x/y"
        assert att["text"].startswith("d" * 300) and "省略あり" in att["text"] and "service_icon" not in att
        assert "reply_users" not in out["messages"][0] and out["messages"][0]["reply_count"] == 120
        assert "blocks" in raw["messages"][1], "--raw must keep the unmodified messages"
        compact_bytes = len(json.dumps(out, ensure_ascii=False, indent=2))
        raw_bytes = len(json.dumps(raw, ensure_ascii=False, indent=2))
        assert compact_bytes * 2 < raw_bytes, (compact_bytes, raw_bytes)
        # "これ以前あり": 120 replies in thread, 20 returned -> 100 older omitted.
        assert out["older_omitted"] == 100 and "これ以前のスレッド返信 100 件は省略" in out["note"]

        # 2) Per-message cap: one huge post is cut with an explicit marker.
        huge = "い" * (agent_tools.HISTORY_MESSAGE_TEXT_LIMIT + 5000)
        client2 = ThreadClient([parent, _bot_msg(1, huge), _bot_msg(2, "短い返信")])
        with patch.object(agent_tools, "get_web_client", return_value=client2):
            out2 = run(Namespace(channel="C_T", thread_ts=parent_ts, limit=20, db=db))
        hm = out2["messages"][1]
        assert hm["text_truncated"] is True and hm["text_length"] == len(huge)
        assert hm["text"].startswith("い" * agent_tools.HISTORY_MESSAGE_TEXT_LIMIT)
        assert f"全{len(huge)}字中 先頭{agent_tools.HISTORY_MESSAGE_TEXT_LIMIT}字" in hm["text"]
        assert out2["text_truncated_count"] == 1 and "切り詰め" in out2["note"]
        assert out2["messages"][2]["text"] == "短い返信" and "text_truncated" not in out2["messages"][2]

        # 3) Total budget: oldest replies dropped first; parent and newest always kept.
        n_long = 30
        long_replies = [_bot_msg(i, f"R{i:02d}" + "う" * 2500) for i in range(1, n_long + 1)]
        client3 = ThreadClient([dict(parent, reply_count=n_long)] + long_replies)
        with patch.object(agent_tools, "get_web_client", return_value=client3):
            out3 = run(Namespace(channel="C_T", thread_ts=parent_ts, limit=50, db=db))
        assert out3["text_chars"] <= agent_tools.HISTORY_TOTAL_TEXT_BUDGET
        assert out3["messages"][0]["ts"] == parent_ts
        assert out3["messages"][-1]["text"].startswith(f"R{n_long:02d}")
        kept = out3["count"] - 1
        assert out3["older_omitted"] == n_long - kept and kept < n_long
        assert out3["messages"][1]["text"].startswith(f"R{n_long - kept + 1:02d}"), "must drop oldest first"
        assert "文量上限" in out3["note"]

        # 4) --limit is clamped to HISTORY_MAX_LIMIT (and the clamp is reported).
        calls.clear()
        with patch.object(agent_tools, "get_web_client", return_value=ThreadClient([parent])):
            out4 = run(Namespace(channel="C_T", thread_ts=parent_ts, limit=500, db=db))
        assert calls[-1]["limit"] == agent_tools.HISTORY_MAX_LIMIT
        assert out4["limit_clamped_from"] == 500 and out4["limit"] == agent_tools.HISTORY_MAX_LIMIT

        # 5) Channel history: has_more -> "これ以前あり" note; empty text falls back to blocks.
        blk = {"type": "message", "user": "U_A", "text": "", "ts": "1700009100.000000",
               "blocks": [{"type": "section", "text": {"type": "mrkdwn", "text": "ブロックだけの本文"}}]}
        with patch.object(agent_tools, "get_web_client", return_value=ThreadClient([blk])):
            out5 = run(Namespace(channel="C_T", thread_ts=None, limit=20, db=db))
        assert out5["messages"][0]["text"] == "ブロックだけの本文" and out5["messages"][0]["text_from_blocks"]
        assert "これ以前のチャンネル投稿あり" in out5["note"] and "older_omitted" not in out5

    return (
        f"blocks/bot_profile等を除去し判断材料(user/bot名/本文/metadata/リンク題)は維持・サイズ{raw_bytes}→{compact_bytes}字、"
        f"1投稿{agent_tools.HISTORY_MESSAGE_TEXT_LIMIT}字で省略明記、合計{agent_tools.HISTORY_TOTAL_TEXT_BUDGET}字超は古い順に省略(親と最新は維持)、"
        f"これ以前あり件数の明記、--limit {agent_tools.HISTORY_MAX_LIMIT}上限、--raw、blocksのみ本文の救済を確認"
    )


def test_dispatcher_batch_weight_budget_and_payload_text_cap() -> str:
    """73. 長文バースト対策: dispatcher の重さ予算 (重い回は件数を減らし残りはPENDINGで次回・順序維持・先頭は必ず取る・payload本文の切り詰めとevent-textで全文)"""
    with tempfile.TemporaryDirectory() as tmp:
        state_dir = Path(tmp) / "state"
        state_dir.mkdir(parents=True, exist_ok=True)
        db_path = state_dir / "slack_events.db"
        payload_path = state_dir / "payload.json"
        mode_file = Path(tmp) / "mode.txt"
        mode_file.write_text("shadow\n", encoding="utf-8")
        init_db(db_path)
        env = os.environ.copy()
        for k in ("YAMA_PAYLOAD_TEXT_LIMIT", "YAMA_EVENT_WEIGHT_OVERHEAD", "YAMA_BATCH_WEIGHT_BUDGET"):
            env.pop(k, None)
        env.update({
            "YAMA_STATE_DIR": str(state_dir),
            "YAMA_DB_PATH": str(db_path),
            "YAMA_PAYLOAD_PATH": str(payload_path),
            "YAMA_LOCK_PATH": str(state_dir / "dispatcher.lock"),
            "YAMA_MODE_FILE": str(mode_file),
            "YAMA_PYTHON_BIN": str(PYTHON_BIN),
            "YAMA_OPS_ALERTS_PATH": str(state_dir / "ops_alerts.jsonl"),
        })

        def add(i: int, text: str) -> None:
            handle_event_payload(
                {"event_id": f"Ev_W_{i:02d}",
                 "event": {"type": "message", "channel": "C_BOARD", "user": f"U_{i:02d}",
                           "text": text, "ts": f"17000002{i:02d}.000000"}},
                bot_user_id="U_YAMA_BOT", db_path=db_path,
            )

        def dispatch() -> Dict[str, Any]:
            res = subprocess.run(["bash", str(DISPATCHER_SH)], env=env, capture_output=True, text=True)
            assert res.returncode == 0, f"rc={res.returncode} out={res.stdout} err={res.stderr}"
            return json.loads(payload_path.read_text(encoding="utf-8"))

        def finish(payload: Dict[str, Any]) -> None:
            for ev in payload["events"]:
                commit_event(ev["event_id"], "DONE", db_path=db_path)

        # 1) Ten 1,500-char posts (weight 2,500 each, budget 15,000) -> 6 now, 4 deferred (PENDING), FIFO.
        for i in range(1, 11):
            add(i, f"#{i:02d} " + "え" * 1496)
        p1 = dispatch()
        ids1 = [e["event_id"] for e in p1["events"]]
        assert ids1 == [f"Ev_W_{i:02d}" for i in range(1, 7)], ids1
        assert p1["count"] == 6 and p1["batch_weight"] == 6 * 2500
        assert p1["deferred"]["count"] == 4 and p1["deferred"]["reason"] == "batch_weight_budget"
        assert count_events(db_path, "PROCESSING") == 6 and count_events(db_path, "PENDING") == 4
        finish(p1)
        p2 = dispatch()
        assert [e["event_id"] for e in p2["events"]] == [f"Ev_W_{i:02d}" for i in range(7, 11)]
        assert "deferred" not in p2, "nothing left -> no deferred block"
        finish(p2)
        assert count_events(db_path, "PENDING") == 0 and count_events(db_path, "DONE") == 10

        # 2) A single huge post is always taken first (no starvation), and its payload text is capped;
        #    the next (small) post waits for the following cycle rather than jumping ahead.
        #    (Default budget: capped weight 5,000 + 1,006 fits, so both go together.)
        huge = "お" * 20000
        add(20, huge)
        add(21, "短い質問です")
        env["YAMA_BATCH_WEIGHT_BUDGET"] = "3000"  # below the huge event's own weight
        p3 = dispatch()
        env.pop("YAMA_BATCH_WEIGHT_BUDGET")
        assert [e["event_id"] for e in p3["events"]] == ["Ev_W_20"]
        ev = p3["events"][0]
        assert ev["text_truncated"] is True and ev["text_length"] == 20000
        assert ev["text"].startswith("お" * 4000) and len(ev["text"]) < 4200
        assert "event-text --event-id Ev_W_20" in ev["text"]
        assert p3["deferred"]["count"] == 1
        payload_bytes = len(payload_path.read_bytes())
        # Full text remains in the DB and is readable via event-text.
        res_t = subprocess.run(
            [str(PYTHON_BIN), str(AGENT_TOOLS_PY), "--db", str(db_path), "event-text", "--event-id", "Ev_W_20"],
            capture_output=True, text=True,
        )
        assert res_t.returncode == 0, res_t.stderr
        full = json.loads(res_t.stdout)
        assert full["text"] == huge and full["text_length"] == 20000
        res_nf = subprocess.run(
            [str(PYTHON_BIN), str(AGENT_TOOLS_PY), "--db", str(db_path), "event-text", "--event-id", "Ev_NOPE"],
            capture_output=True, text=True,
        )
        assert res_nf.returncode == 1 and "event_not_found" in res_nf.stderr
        finish(p3)
        p4 = dispatch()
        assert [e["event_id"] for e in p4["events"]] == ["Ev_W_21"] and "text_truncated" not in p4["events"][0]
        finish(p4)

        # 3) Typical light batch (12 short posts) is unchanged: 10 + 2 (reason batch_max_events).
        for i in range(30, 42):
            add(i, f"普通の投稿 {i}")
        p5 = dispatch()
        assert p5["count"] == 10 and p5["deferred"] == {**p5["deferred"], "count": 2, "reason": "batch_max_events"}

        # 4) Budget is tunable via env (0 disables the weight cap).
        finish(p5)
        finish(dispatch())
        for i in range(50, 55):
            add(i, "か" * 3000)
        env["YAMA_BATCH_WEIGHT_BUDGET"] = "0"
        p6 = dispatch()
        assert p6["count"] == 5 and "deferred" not in p6

    return (
        f"1,500字x10件→6件+残4件PENDING(次回4件・FIFO)、2万字1件は予算超でも先頭で必ず取り後続は次回・payload本文を4,000字に切詰め"
        f"(payload {payload_bytes}B・event-textで全文20,000字取得・未存在はexit 1)、軽い12件は従来通り10+2、予算0で無効化を確認"
    )


# ---------------------------------------------------------------------------
# Stall fix (2026-11-22): stepped unverifiable wait, read deadlines, local
# thread mirror + context bundling, commit-batch, seed-mirror, hook ordering.
# ---------------------------------------------------------------------------


def _write_mirror_health(state_dir: Path, hb_age: float = 0.0, backfill_ok: Optional[bool] = True) -> None:
    now = time.time()
    (state_dir / "heartbeat.json").write_text(
        json.dumps({"timestamp": now - hb_age, "connected": True}), encoding="utf-8"
    )
    st = state_dir / "backfill_last_status.json"
    if backfill_ok is None:
        if st.exists():
            st.unlink()
    else:
        st.write_text(json.dumps({"at": now, "ok": backfill_ok, "errors": []}), encoding="utf-8")


def test_unverifiable_stepped_wait() -> str:
    """74. 投稿前チェック不能の待ちの段階化 (2→5→15→30→60分・first_unverifiable_at基準・retry_count非消費・24h終端不変・固定値指定は従来どおり)"""
    f = agent_tools.unverifiable_retry_wait_seconds
    assert [f(0), f(119), f(120), f(419), f(420), f(1319), f(1320), f(3119), f(3120), f(80000)] == [
        120, 120, 300, 300, 900, 900, 1800, 1800, 3600, 3600]
    assert agent_tools._parse_retry_ladder("60, 600") == (60, 600)
    assert agent_tools._parse_retry_ladder("x,1") == agent_tools.UNVERIFIABLE_RETRY_LADDER_SECONDS
    assert agent_tools._parse_retry_ladder("0,5") == agent_tools.UNVERIFIABLE_RETRY_LADDER_SECONDS

    with tempfile.TemporaryDirectory() as tmp, patch.dict(os.environ, {}, clear=False):
        os.environ.pop("YAMA_UNVERIFIABLE_RETRY_WAIT_SEC", None)
        os.environ.pop("YAMA_UNVERIFIABLE_RETRY_LADDER_SEC", None)
        db_path = Path(tmp) / "state" / "slack_events.db"
        init_db(db_path)
        with get_db_connection(db_path) as conn:
            conn.execute(
                "INSERT INTO events (event_id, channel_id, user_id, text, ts, thread_ts, status) "
                "VALUES ('Ev_LADDER', 'C1', 'U1', 'q', '1797000000.000100', '1797000000.000100', 'PROCESSING')"
            )
            conn.commit()
        t0 = datetime(2026, 11, 22, 0, 0, 0, tzinfo=timezone.utc)
        waits = []
        # Each re-dispatch happens a bit after next_retry_at (poll cycle + batch time).
        offsets = [0, 150, 480, 1400, 3200, 6900]
        for off in offsets:
            now_dt = t0 + timedelta(seconds=off)
            row = commit_event("Ev_LADDER", "FAILED", error_message="pre_post_check_unverifiable",
                               db_path=db_path, now=now_dt)
            assert row["status"] == "PENDING" and int(row["retry_count"]) == 0, row
            assert row["first_unverifiable_at"] == "2026-11-22 00:00:00", row
            nxt = datetime.strptime(row["next_retry_at"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            waits.append(int((nxt - now_dt).total_seconds()))
        assert waits == [120, 300, 900, 1800, 3600, 3600], waits
        row = commit_event("Ev_LADDER", "FAILED", error_message="pre_post_check_unverifiable",
                           db_path=db_path, now=t0 + timedelta(hours=24, seconds=1))
        assert row["status"] == "FAILED" and "abandoned after 24h" in row["error_message"], row
        # Explicit fixed wait (argument or env) keeps the old behaviour.
        with get_db_connection(db_path) as conn:
            conn.execute("UPDATE events SET status='PROCESSING', first_unverifiable_at=NULL, error_message=NULL")
            conn.commit()
        row = commit_event("Ev_LADDER", "FAILED", error_message="pre_post_check_unverifiable",
                           db_path=db_path, now=t0, retry_wait_seconds=3600)
        assert row["next_retry_at"] == "2026-11-22 01:00:00", row
        with get_db_connection(db_path) as conn:
            conn.execute("UPDATE events SET status='PROCESSING', first_unverifiable_at=NULL, error_message=NULL")
            conn.commit()
        with patch.dict(os.environ, {"YAMA_UNVERIFIABLE_RETRY_WAIT_SEC": "600"}):
            row = commit_event("Ev_LADDER", "FAILED", error_message="pre_post_check_unverifiable",
                               db_path=db_path, now=t0)
        assert row["next_retry_at"] == "2026-11-22 00:10:00", row
        with get_db_connection(db_path) as conn:
            conn.execute("UPDATE events SET status='PROCESSING', first_unverifiable_at=NULL, error_message=NULL")
            conn.commit()
        with patch.dict(os.environ, {"YAMA_UNVERIFIABLE_RETRY_LADDER_SEC": "60,600"}):
            r1 = commit_event("Ev_LADDER", "FAILED", error_message="pre_post_check_unverifiable",
                              db_path=db_path, now=t0)
            r2 = commit_event("Ev_LADDER", "FAILED", error_message="pre_post_check_unverifiable",
                              db_path=db_path, now=t0 + timedelta(seconds=70))
        assert r1["next_retry_at"] == "2026-11-22 00:01:00" and r2["next_retry_at"] == "2026-11-22 00:11:10", (r1, r2)
    return "待ち時間が初回からの経過で2→5→15→30→60→60分と段階化(再ディスパッチの遅れがあっても1段ずつ)・retry_count非消費・24h超FAILED終端は不変・固定値指定(引数/環境変数)と段階の上書きを確認"


class _FakeClock:
    def __init__(self) -> None:
        self.t = 1000.0
        self.slept: List[float] = []

    def monotonic(self) -> float:
        return self.t

    def sleep(self, d: float) -> None:
        self.slept.append(d)
        self.t += d


def test_read_deadlines_context_and_safety() -> str:
    """75. 通信の締切 (文脈用20秒でローカルへ・再試行の待ちを締切内に制限・SDKのtimeout/再試行とcurlを残り時間で制限・安全用は残り時間から逆算しfail-closed)"""
    clock = _FakeClock()
    calls = {"n": 0}

    def always_fail() -> Any:
        calls["n"] += 1
        clock.t += 3.0  # each attempt itself takes 3s
        raise urllib.error.URLError("down")

    # 1) _call_with_retry stops before a backoff that would cross the deadline.
    with patch.object(agent_tools.time, "monotonic", clock.monotonic):
        with agent_tools.read_deadline(10.0):
            try:
                agent_tools._call_with_retry(always_fail, sleep_fn=clock.sleep)
                raise AssertionError("expected SlackReadDeadlineExceeded")
            except agent_tools.SlackReadDeadlineExceeded as exc:
                assert isinstance(exc, TimeoutError)
        assert clock.t - 1000.0 <= 10.0 + 3.0, clock.t
        assert calls["n"] == 3 and clock.slept == [1.0, 2.0], (calls, clock.slept)
        # No deadline -> unchanged 5 attempts.
        calls["n"] = 0
        clock.slept.clear()
        try:
            agent_tools._call_with_retry(always_fail, sleep_fn=clock.sleep)
        except urllib.error.URLError:
            pass
        assert calls["n"] == 5 and clock.slept == [1.0, 2.0, 4.0, 8.0], clock.slept
        # Nested deadlines keep the earlier one and restore on exit.
        with agent_tools.read_deadline(5.0):
            d1 = agent_tools._ACTIVE_READ_DEADLINE
            with agent_tools.read_deadline(50.0):
                assert agent_tools._ACTIVE_READ_DEADLINE == d1
            with agent_tools.read_deadline(1.0):
                assert agent_tools._ACTIVE_READ_DEADLINE < d1
            assert agent_tools._ACTIVE_READ_DEADLINE == d1
        assert agent_tools._ACTIVE_READ_DEADLINE is None

    # 2) Context read: deadline -> local DB fallback with fallback_reason.
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "state" / "slack_events.db"
        init_db(db_path)
        handle_event_payload(
            {"event_id": "Ev_DL_1", "event": {"type": "message", "channel": "C_DL", "user": "U_A",
                                               "text": "ローカルにある投稿", "ts": "1797100000.000100"}},
            bot_user_id="U_YAMA", db_path=db_path,
        )

        class SlowClient:
            def conversations_replies(self, **kw: Any) -> Any:
                clock.t += 15.0
                raise urllib.error.URLError("slow")

        clock.t = 5000.0
        clock.slept.clear()
        with patch.object(agent_tools.time, "monotonic", clock.monotonic):
            res = fetch_history("C_DL", thread_ts="1797100000.000100", limit=20, db_path=db_path,
                                client=SlowClient(), sleep_fn=clock.sleep, deadline_seconds=20.0)
        assert res["source"] == "local_db_fallback" and res.get("fallback_reason") == "read_deadline", res
        assert res["messages"][0]["text"] == "ローカルにある投稿"
        assert clock.t - 5000.0 <= 31.0, clock.t
        assert agent_tools.CONTEXT_READ_DEADLINE_SECONDS == 20.0

    # 3) _CurlFallbackWebClient: socket timeout / SDK retry handlers / curl max-time bounded.
    seen: Dict[str, Any] = {}
    client = agent_tools._CurlFallbackWebClient(token="xoxb-dummy-token-for-test")
    orig_timeout, orig_handlers = client.timeout, list(client.retry_handlers)

    def fake_sdk_replies(self: Any, **kw: Any) -> Any:
        seen["timeout"] = self.timeout
        seen["handlers"] = list(self.retry_handlers)
        raise urllib.error.URLError("sdk down")

    def fake_curl(token: str, method: str, params: Dict[str, Any], max_time: Optional[float] = None) -> Any:
        seen["max_time"] = max_time
        raise RuntimeError("curl down")

    with patch.object(agent_tools.WebClient, "conversations_replies", fake_sdk_replies), \
            patch.object(agent_tools, "_curl_api_read", fake_curl):
        with agent_tools.read_deadline(7.0):
            try:
                client.conversations_replies(channel="C", ts="1.0")
            except urllib.error.URLError:
                pass
        assert seen["timeout"] <= 7.0 and seen["handlers"] == [] and seen["max_time"] <= 7.0, seen
        assert client.timeout == orig_timeout and len(client.retry_handlers) == len(orig_handlers)
        seen.clear()
        try:
            client.conversations_replies(channel="C", ts="1.0")
        except urllib.error.URLError:
            pass
        assert seen["timeout"] == orig_timeout and len(seen["handlers"]) == len(orig_handlers)
        assert seen["max_time"] is None, seen

    # 4) Safety budget counted back from the Reaper horizon.
    now_dt = datetime(2026, 11, 22, 12, 0, 0, tzinfo=timezone.utc)

    def row(age: float, status: str = "PROCESSING") -> Dict[str, Any]:
        return {"status": status, "updated_at": (now_dt - timedelta(seconds=age)).strftime("%Y-%m-%d %H:%M:%S")}

    b = agent_tools.safety_read_budget_seconds
    assert b(row(0), now=now_dt) == 180.0
    assert b(row(700), now=now_dt) == 140.0
    assert b(row(850), now=now_dt) == 0.0
    assert b(row(5000, "PENDING"), now=now_dt) == 180.0 and b(None, now=now_dt) == 180.0

    # 5) cmd_reply: too little time left -> pre_post_check_unverifiable without any read or post.
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "state" / "slack_events.db"
        init_db(db_path)
        old = (datetime.now(timezone.utc) - timedelta(seconds=860)).strftime("%Y-%m-%d %H:%M:%S")
        fresh = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        with get_db_connection(db_path) as conn:
            conn.executemany(
                "INSERT INTO events (event_id, channel_id, user_id, text, ts, thread_ts, status, created_at, updated_at) "
                "VALUES (?, 'C_S', 'U_A', 'q', ?, '1797200000.000100', 'PROCESSING', ?, ?)",
                [("Ev_SAFE_OLD", "1797200001.000100", old, old), ("Ev_SAFE_NEW", "1797200002.000100", fresh, fresh)],
            )
            conn.commit()
        text_file = Path(tmp) / "r.txt"
        text_file.write_text("返信本文です。\n", encoding="utf-8")
        reads = {"n": 0, "posts": 0}

        class Client:
            timeout = 30
            retry_handlers: List[Any] = []

            def auth_test(self) -> Dict[str, Any]:
                return {"ok": True, "user_id": "U_YAMA_BOT"}

            def conversations_replies(self, **kw: Any) -> Dict[str, Any]:
                reads["n"] += 1
                return {"ok": True, "messages": [], "has_more": False}

            def chat_postMessage(self, **kw: Any) -> Dict[str, Any]:
                reads["posts"] += 1
                return {"ok": True, "channel": kw["channel"], "ts": "1797200099.000100"}

        def run(eid: str) -> Tuple[int, str, str]:
            args = Namespace(channel="C_S", text_file=str(text_file), thread_ts="1797200000.000100",
                             event_id=eid, db=str(db_path))
            out, err = io.StringIO(), io.StringIO()
            with patch.object(agent_tools, "get_web_client", lambda *a, **k: Client()), \
                    redirect_stdout(out), redirect_stderr(err):
                rc = cmd_reply(args)
            return rc, out.getvalue(), err.getvalue()

        rc, _, err = run("Ev_SAFE_OLD")
        assert rc == 1 and "pre_post_check_unverifiable" in err and "safety_read_deadline" in err, err
        assert reads == {"n": 0, "posts": 0}, reads
        rc, out, _ = run("Ev_SAFE_NEW")
        assert rc == 0 and reads["n"] >= 1 and reads["posts"] == 1, (rc, reads, out)
    return "締切内で再試行を打ち切り(待ちを跨がない)・締切なしは従来の5回・文脈用は締切でlocal_db_fallback(fallback_reason)・SDKのtimeout/再試行ハンドラとcurlのmax-timeを残り時間で制限し呼出後に復元・安全用予算はReaper回収時刻から逆算(余裕なしは読まず送らずunverifiable)を確認"


def test_listener_mirror_own_posts_and_edits() -> str:
    """76. ミラーの穴埋め (自分の投稿はevents外のmirror_messagesへ・編集/削除をmessage_editsへ・eventsの除外は不変・backfillも自分の投稿と編集を記録)"""
    import listener as listener_mod
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "state" / "slack_events.db"
        init_db(db_path)
        saved, reason = handle_event_payload(
            {"event_id": "Ev_OWN", "event": {"type": "message", "channel": "C_M", "user": "U_YAMA", "bot_id": "B_Y",
                                              "text": "Yama の共有投稿", "ts": "1797300001.000100",
                                              "thread_ts": "1797300000.000100"}},
            bot_user_id="U_YAMA", db_path=db_path,
        )
        assert (saved, reason) == (False, "self_bot_message")
        saved, reason = handle_event_payload(
            {"event": {"type": "message", "subtype": "message_changed", "channel": "C_M", "hidden": True,
                       "message": {"user": "U_OTHER", "text": "編集後の本文", "ts": "1797300002.000100"}}},
            bot_user_id="U_YAMA", db_path=db_path,
        )
        assert saved is False and reason.startswith("ignored_subtype")
        handle_event_payload(
            {"event": {"type": "message", "subtype": "message_changed", "channel": "C_M",
                       "message": {"user": "U_YAMA", "text": "Yama 編集後", "ts": "1797300001.000100",
                                   "thread_ts": "1797300000.000100"}}},
            bot_user_id="U_YAMA", db_path=db_path,
        )
        handle_event_payload(
            {"event": {"type": "message", "subtype": "message_deleted", "channel": "C_M",
                       "deleted_ts": "1797300003.000100"}},
            bot_user_id="U_YAMA", db_path=db_path,
        )
        with get_db_connection(db_path) as conn:
            assert conn.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 0
            own = conn.execute("SELECT * FROM mirror_messages WHERE ts='1797300001.000100'").fetchone()
            assert own["user_id"] == "U_YAMA" and own["is_bot"] == 1 and own["text"] == "Yama 編集後"
            assert own["thread_ts"] == "1797300000.000100"
            ed = {r["ts"]: dict(r) for r in conn.execute("SELECT * FROM message_edits").fetchall()}
            assert ed["1797300002.000100"]["text"] == "編集後の本文" and ed["1797300002.000100"]["deleted"] == 0
            assert ed["1797300003.000100"]["deleted"] == 1
        s1 = listener_mod.mark_mirror_since(db_path, now=100.0)
        s2 = listener_mod.mark_mirror_since(db_path, now=200.0)
        assert s1 == s2 == "100.000000"
        # A mirror failure never breaks intake.
        with patch.object(listener_mod, "record_own_message", side_effect=RuntimeError("disk")):
            assert listener_mod.mirror_side_event(
                {"type": "message", "channel": "C", "user": "U_YAMA", "ts": "1.0", "text": "x"}, "U_YAMA",
                db_path=db_path) is None

        # backfill: own posts and edited messages it reads are mirrored too.
        class BfClient:
            def auth_test(self) -> Dict[str, Any]:
                return {"ok": True, "user_id": "U_YAMA"}

            def conversations_history(self, **kw: Any) -> Dict[str, Any]:
                if kw["channel"] != "C_B":
                    return {"ok": True, "messages": [], "has_more": False}
                return {"ok": True, "has_more": False, "messages": [
                    {"type": "message", "user": "U_YAMA", "bot_id": "B_Y", "text": "停止中のYama投稿", "ts": f"{time.time() - 30:.6f}"},
                    {"type": "message", "user": "U_P", "text": "編集された人間の投稿", "ts": f"{time.time() - 20:.6f}", "edited": {"ts": "1"}},
                ]}

            def conversations_replies(self, **kw: Any) -> Dict[str, Any]:
                return {"ok": True, "messages": [], "has_more": False}

        env_file = Path(tmp) / ".env"
        env_file.write_text("", encoding="utf-8")
        res = run_backfill(db_path=db_path, env_path=env_file, channels=("C_B",), client=BfClient(), sleep_fn=lambda s: None)
        assert res["inserted"] == 1, res
        with get_db_connection(db_path) as conn:
            m = conn.execute("SELECT * FROM mirror_messages WHERE channel_id='C_B'").fetchall()
            assert len(m) == 1 and m[0]["text"] == "停止中のYama投稿" and m[0]["source"] == "backfill"
            assert conn.execute("SELECT COUNT(*) FROM message_edits WHERE channel_id='C_B'").fetchone()[0] == 1
            assert conn.execute("SELECT COUNT(*) FROM events WHERE channel_id='C_B' AND user_id='U_YAMA'").fetchone()[0] == 0
        st = json.loads((db_path.parent / "backfill_last_status.json").read_text(encoding="utf-8"))
        assert st["ok"] is True

        class BadClient(BfClient):
            def conversations_history(self, **kw: Any) -> Dict[str, Any]:
                raise SlackApiError("boom", {"ok": False, "error": "channel_not_found"})

        run_backfill(db_path=db_path, env_path=env_file, channels=("C_B",), client=BadClient(), sleep_fn=lambda s: None)
        st = json.loads((db_path.parent / "backfill_last_status.json").read_text(encoding="utf-8"))
        assert st["ok"] is False and st["errors"], st
    return "自分の投稿はeventsに入れずmirror_messagesへ(編集も反映)・他者の編集と削除をmessage_editsへ・mirror_meta開始時刻は初回のみ・ミラー失敗は取り込みに影響なし・backfillが停止中の自分の投稿と編集を記録しbackfill_last_status.jsonに成否を残すことを確認"


def _seed_context_db(db_path: Path, since: float) -> None:
    import listener as listener_mod
    init_db(db_path)
    listener_mod.mark_mirror_since(db_path, now=since)
    thread = "1797400000.000100"
    with get_db_connection(db_path) as conn:
        rows = [("Ev_P", "C_X", "U_OWNER", "親の問いかけ", thread, None, 0, "DONE")]
        for i in range(1, 26):
            rows.append((f"Ev_R{i:02d}", "C_X", "U_NEIRO", f"返信{i:02d}", f"17974000{i:02d}.000100", thread, 1, "DONE"))
        conn.executemany(
            "INSERT INTO events (event_id, channel_id, user_id, text, ts, thread_ts, is_bot, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        conn.commit()
    listener_mod.record_own_message("C_X", "1797400026.500000", thread, "Yama の共有（台帳外）", user_id=agent_tools.YAMA_BOT_USER_ID, db_path=db_path)
    agent_tools.record_reply(event_id="Ev_R25", channel_id="C_X", thread_ts=thread, reply_ts="1797400027.000100",
                             text="Yama の返信（台帳）", db_path=db_path)
    listener_mod.record_message_edit("C_X", "1797400024.000100", "返信24（編集後）", db_path=db_path)
    listener_mod.record_message_edit("C_X", "1797400023.000100", None, deleted=True, db_path=db_path)


def test_context_bundle_local_thread_context() -> str:
    """77. 文脈のローカル同梱 (events+mirror+台帳の統合・親＋直近20件・編集反映/削除除外・直前のYama発言・欠けがあればcontext_ok=falseで理由)"""
    import context_bundle
    import listener as listener_mod
    with tempfile.TemporaryDirectory() as tmp:
        state = Path(tmp) / "state"
        state.mkdir()
        db_path = state / "slack_events.db"
        _seed_context_db(db_path, since=1797300000.0)
        _write_mirror_health(state)
        ev = {"event_id": "Ev_NEW", "channel_id": "C_X", "ts": "1797400028.000100",
              "thread_ts": "1797400000.000100", "retry_count": 0}
        ctxs, ann = context_bundle.build_batch_contexts(db_path, [ev], state_dir=state)
        a = ann["Ev_NEW"]
        assert a == {"context_key": "C_X:1797400000.000100", "context_ok": True, "context_gaps": []}, a
        c = ctxs[a["context_key"]]
        msgs = c["messages"]
        assert c["complete"] is True and c["source"] == "local_db_bundle"
        assert msgs[0]["ts"] == "1797400000.000100" and msgs[0]["text"] == "親の問いかけ"
        assert len(msgs) == 21, len(msgs)
        texts = [m["text"] for m in msgs]
        assert "返信23" not in texts and c.get("deleted_omitted") == 1
        assert "返信24（編集後）" in texts and any(m.get("edited") for m in msgs)
        assert texts[-2:] == ["Yama の共有（台帳外）", "Yama の返信（台帳）"], texts[-3:]
        assert msgs[-1].get("is_yama") is True
        assert c["last_own_message"]["text"] == "Yama の返信（台帳）"
        assert c["older_omitted"] == 26 - 20 and "省略" in c["note"]

        # Same thread twice in one batch -> one context, shared.
        ev2 = dict(ev, event_id="Ev_NEW2", ts="1797400029.000100")
        ctxs, ann = context_bundle.build_batch_contexts(db_path, [ev, ev2], state_dir=state)
        assert len(ctxs) == 1 and ann["Ev_NEW2"]["context_key"] == ann["Ev_NEW"]["context_key"]

        # Retry / wait-state event -> per-event gap (thread context itself still complete).
        ev_r = dict(ev, event_id="Ev_RETRY", retry_count=1)
        ev_w = dict(ev, event_id="Ev_WAIT", first_unverifiable_at="2026-11-22 00:00:00")
        _, ann = context_bundle.build_batch_contexts(db_path, [ev_r, ev_w], state_dir=state)
        assert ann["Ev_RETRY"]["context_ok"] is False and ann["Ev_RETRY"]["context_gaps"] == ["retry_requires_api_history"]
        assert ann["Ev_WAIT"]["context_ok"] is False

        # Parent missing.
        ev_np = {"event_id": "Ev_NP", "channel_id": "C_X", "ts": "1797500001.000100", "thread_ts": "1797500000.000100"}
        with get_db_connection(db_path) as conn:
            conn.execute("INSERT INTO events (event_id, channel_id, user_id, text, ts, thread_ts, status) VALUES "
                         "('Ev_NP', 'C_X', 'U_N', 'x', '1797500001.000100', '1797500000.000100', 'PROCESSING')")
            conn.commit()
        ctxs, ann = context_bundle.build_batch_contexts(db_path, [ev_np], state_dir=state)
        assert ann["Ev_NP"]["context_gaps"] == ["parent_missing"] and "messages" not in ctxs["C_X:1797500000.000100"]

        # Top-level post (no thread_ts): its own ts is the parent -> complete.
        ev_top = {"event_id": "Ev_NP", "channel_id": "C_X", "ts": "1797500001.000100", "thread_ts": None}
        _, ann = context_bundle.build_batch_contexts(db_path, [ev_top], state_dir=state)
        assert ann["Ev_NP"]["context_ok"] is True, ann

        # Thread older than the own-post mirror start -> incomplete until seeded after it.
        with get_db_connection(db_path) as conn:
            conn.execute("UPDATE mirror_meta SET value='1797450000.000000'")
            conn.commit()
        _, ann = context_bundle.build_batch_contexts(db_path, [ev], state_dir=state)
        assert ann["Ev_NEW"]["context_gaps"] == ["thread_predates_own_post_mirror"], ann
        with get_db_connection(db_path) as conn:
            conn.execute("INSERT INTO mirror_threads (channel_id, thread_ts, seeded_at) VALUES ('C_X', '1797400000.000100', 1797440000.0)")
            conn.commit()
        _, ann = context_bundle.build_batch_contexts(db_path, [ev], state_dir=state)
        assert ann["Ev_NEW"]["context_ok"] is False, "seeded before the mirror start does not count"
        with get_db_connection(db_path) as conn:
            conn.execute("UPDATE mirror_threads SET seeded_at = 1797460000.0")
            conn.commit()
        _, ann = context_bundle.build_batch_contexts(db_path, [ev], state_dir=state)
        assert ann["Ev_NEW"]["context_ok"] is True, ann
        with get_db_connection(db_path) as conn:
            conn.execute("DELETE FROM mirror_meta")
            conn.commit()
        _, ann = context_bundle.build_batch_contexts(db_path, [ev], state_dir=state)
        assert ann["Ev_NEW"]["context_gaps"] == ["own_post_mirror_inactive"], ann
        listener_mod.mark_mirror_since(db_path, now=1797300000.0)

        # Batch-wide health gaps.
        _write_mirror_health(state, hb_age=600)
        _, ann = context_bundle.build_batch_contexts(db_path, [ev], state_dir=state)
        assert "listener_heartbeat_stale" in ann["Ev_NEW"]["context_gaps"]
        _write_mirror_health(state, backfill_ok=False)
        _, ann = context_bundle.build_batch_contexts(db_path, [ev], state_dir=state)
        assert ann["Ev_NEW"]["context_gaps"] == ["backfill_failed"]
        _write_mirror_health(state, backfill_ok=None)
        _, ann = context_bundle.build_batch_contexts(db_path, [ev], state_dir=state)
        assert ann["Ev_NEW"]["context_gaps"] == ["backfill_status_unknown"]
        _write_mirror_health(state)

        # Batch text budget.
        with patch.dict(os.environ, {"YAMA_CONTEXT_BATCH_TEXT_BUDGET": "50"}):
            _, ann = context_bundle.build_batch_contexts(db_path, [ev], state_dir=state)
        assert ann["Ev_NEW"]["context_gaps"] == ["payload_context_budget"], ann
    return "events・mirror・台帳を統合し親＋直近20件(26件中6件省略を明記)・編集後本文/削除除外・台帳外の自分の投稿と直前のYama発言を含む・同一スレッドは1回だけ・再処理イベント/親欠落/ミラー開始前の未シードスレッド/listener停止/backfill失敗・不明/予算超過はcontext_ok=falseで理由付き、を確認"


def test_dispatcher_context_bundle_payload() -> str:
    """78. dispatcher の文脈同梱 (PROCESSING確定後に作成・小さい時はpayloadに内包/大きい時はpayload_context.json参照・失敗しても配信は不変・無効化スイッチ)"""
    with tempfile.TemporaryDirectory() as tmp:
        state_dir = Path(tmp) / "state"
        state_dir.mkdir(parents=True, exist_ok=True)
        db_path = state_dir / "slack_events.db"
        payload_path = state_dir / "payload.json"
        mode_file = Path(tmp) / "mode.txt"
        mode_file.write_text("full\n", encoding="utf-8")
        _seed_context_db(db_path, since=1797300000.0)
        _write_mirror_health(state_dir)
        env = os.environ.copy()
        for k in ("YAMA_PAYLOAD_INLINE_MAX_BYTES", "YAMA_CONTEXT_BUNDLE", "YAMA_HEARTBEAT_PATH"):
            env.pop(k, None)
        env.update({
            "YAMA_STATE_DIR": str(state_dir), "YAMA_DB_PATH": str(db_path),
            "YAMA_PAYLOAD_PATH": str(payload_path), "YAMA_LOCK_PATH": str(state_dir / "dispatcher.lock"),
            "YAMA_MODE_FILE": str(mode_file), "YAMA_PYTHON_BIN": str(PYTHON_BIN),
            "YAMA_OPS_ALERTS_PATH": str(state_dir / "ops_alerts.jsonl"),
        })

        def add(eid: str, ts: str, thread: Optional[str], text: str = "新しい投稿") -> None:
            ev = {"type": "message", "channel": "C_X", "user": "U_NEIRO", "bot_id": "B_N", "text": text, "ts": ts}
            if thread:
                ev["thread_ts"] = thread
            handle_event_payload({"event_id": eid, "event": ev}, bot_user_id="U_YAMA", db_path=db_path)

        def dispatch(extra: Optional[Dict[str, str]] = None, script: Path = DISPATCHER_SH) -> Dict[str, Any]:
            e = dict(env, **(extra or {}))
            res = subprocess.run(["bash", str(script)], env=e, capture_output=True, text=True)
            assert res.returncode == 0, f"rc={res.returncode} out={res.stdout} err={res.stderr}"
            return json.loads(payload_path.read_text(encoding="utf-8"))

        def finish(p: Dict[str, Any]) -> None:
            for ev in p["events"]:
                commit_event(ev["event_id"], "DONE", db_path=db_path)

        # 1) Small batch -> inlined.
        add("Ev_D1", "1797400030.000100", "1797400000.000100")
        p = dispatch({"YAMA_PAYLOAD_INLINE_MAX_BYTES": "200000"})
        e = p["events"][0]
        assert e["context_ok"] is True and e["context_key"] == "C_X:1797400000.000100", e
        assert p["contexts"][e["context_key"]]["messages"][0]["text"] == "親の問いかけ"
        assert "context_file" not in p
        doc = json.loads((state_dir / "payload_context.json").read_text(encoding="utf-8"))
        assert doc["event_ids"] == ["Ev_D1"] and e["context_key"] in doc["contexts"]
        finish(p)

        # 2) Default inline limit (12,000 bytes) exceeded -> pointer to the file only.
        add("Ev_D2", "1797400031.000100", "1797400000.000100", text="長め" * 300)
        with get_db_connection(db_path) as conn:
            conn.execute("UPDATE events SET text = ? WHERE event_id LIKE 'Ev_R%'", ("長い返信本文。" * 120,))
            conn.commit()
        p = dispatch()
        assert "contexts" not in p and p["context_file"] == str(state_dir / "payload_context.json"), list(p)
        assert p["events"][0]["context_ok"] is True
        assert len(payload_path.read_bytes()) < 12000 + 4000
        finish(p)

        # 3) Bundling failure -> dispatch unchanged, events marked context_ok=false.
        alt = Path(tmp) / "alt"
        alt.mkdir()
        shutil.copy(DISPATCHER_SH, alt / "dispatcher.sh")
        (alt / "context_bundle.py").write_text("raise RuntimeError('broken bundle')\n", encoding="utf-8")
        add("Ev_D3", "1797400032.000100", "1797400000.000100")
        p = dispatch(script=alt / "dispatcher.sh")
        assert [x["event_id"] for x in p["events"]] == ["Ev_D3"] and count_events(db_path, "PROCESSING") == 1
        assert p["events"][0]["context_ok"] is False and p["events"][0]["context_gaps"] == ["context_build_error"]
        finish(p)

        # 4) Disabled -> payload exactly as before (no context fields).
        add("Ev_D4", "1797400033.000100", "1797400000.000100")
        p = dispatch({"YAMA_CONTEXT_BUNDLE": "0"})
        assert "context_ok" not in p["events"][0] and "contexts" not in p and "context_file" not in p
        finish(p)
    return "PROCESSING確定後に文脈を作りpayloadへ内包(小)・12,000バイト超はpayload_context.jsonを参照(context_file)・同梱処理が壊れても配信件数と状態は不変でcontext_ok=false・YAMA_CONTEXT_BUNDLE=0で従来のpayloadを確認"


def test_commit_batch() -> str:
    """79. commit-batch (返さない件の一括commit・検証は全件一括で1件でも不正なら何もしない・各件はcommitと同じ扱い)"""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "state" / "slack_events.db"
        init_db(db_path)
        with get_db_connection(db_path) as conn:
            conn.executemany(
                "INSERT INTO events (event_id, channel_id, user_id, text, ts, status, error_message) VALUES (?, 'C', 'U', 't', ?, 'PROCESSING', ?)",
                [("Ev_B1", "1.1", None), ("Ev_B2", "1.2", "old error"), ("Ev_B3", "1.3", None), ("Ev_B4", "1.4", None)],
            )
            conn.commit()
        f = Path(tmp) / "decisions.json"

        def run(obj: Any) -> Tuple[int, Dict[str, Any]]:
            f.write_text(json.dumps(obj, ensure_ascii=False), encoding="utf-8")
            out, err = io.StringIO(), io.StringIO()
            with redirect_stdout(out), redirect_stderr(err):
                rc = agent_tools.main(["--db", str(db_path), "commit-batch", "--file", str(f)])
            txt = out.getvalue() or err.getvalue()
            return rc, json.loads(txt)

        def status(eid: str) -> str:
            with get_db_connection(db_path) as conn:
                return conn.execute("SELECT status FROM events WHERE event_id=?", (eid,)).fetchone()[0]

        rc, res = run([{"event_id": "Ev_B1", "status": "IGNORED"}, {"event_id": "Ev_NOPE", "status": "DONE"},
                       {"event_id": "Ev_B2", "status": "maybe"}, {"event_id": "Ev_B1", "status": "DONE"}])
        assert rc == 1 and res["committed"] == 0 and len(res["errors"]) == 3, res
        assert status("Ev_B1") == "PROCESSING", "nothing committed on validation error"
        rc, res = run({"decisions": [
            {"event_id": "Ev_B1", "status": "ignored", "error": None},
            {"event_id": "Ev_B2", "status": "DONE"},
            {"event_id": "Ev_B3", "status": "FAILED", "error": "pre_post_check_unverifiable"},
        ]})
        assert rc == 0 and res["ok"] and res["committed"] == 3, res
        assert [status(x) for x in ("Ev_B1", "Ev_B2", "Ev_B3", "Ev_B4")] == ["IGNORED", "DONE", "PENDING", "PROCESSING"]
        with get_db_connection(db_path) as conn:
            r2 = conn.execute("SELECT error_message FROM events WHERE event_id='Ev_B2'").fetchone()[0]
            r3 = conn.execute("SELECT retry_count, next_retry_at FROM events WHERE event_id='Ev_B3'").fetchone()
        assert r2 is None and r3[0] == 0 and r3[1] is not None
        rc, res = run([])
        assert rc == 1
    return "不正・重複・未知のIDが1件でもあれば何もcommitしない・正常時は各件をcommit_eventで確定(DONEでerror消去・unverifiableは待ち状態)・対象外の件は不変を確認"


def test_seed_mirror() -> str:
    """80. seed-mirror (ミラー開始前のスレッドを一度だけ全件読みして親投稿と台帳外の自分の投稿を補完・ページ上限/失敗は未シードのまま・開始前は拒否)"""
    import context_bundle
    import listener as listener_mod
    with tempfile.TemporaryDirectory() as tmp:
        state = Path(tmp) / "state"
        state.mkdir()
        db_path = state / "slack_events.db"
        init_db(db_path)
        _write_mirror_health(state)
        now = time.time()
        t_ok = f"{now - 7200:.6f}"
        t_fail = f"{now - 3600:.6f}"
        with get_db_connection(db_path) as conn:
            conn.executemany(
                "INSERT INTO events (event_id, channel_id, user_id, text, ts, thread_ts, is_bot, status) VALUES (?, 'C_S', 'U_N', ?, ?, ?, 1, 'DONE')",
                [("Ev_S1", "返信A", f"{float(t_ok) + 10:.6f}", t_ok), ("Ev_S2", "返信B", f"{float(t_fail) + 10:.6f}", t_fail)],
            )
            conn.commit()

        class SeedClient:
            def __init__(self) -> None:
                self.calls: List[Dict[str, Any]] = []

            def conversations_replies(self, **kw: Any) -> Dict[str, Any]:
                self.calls.append(kw)
                if kw["ts"] == t_fail:
                    raise SlackApiError("x", {"ok": False, "error": "thread_not_found"})
                if not kw.get("cursor"):
                    return {"ok": True, "has_more": True, "response_metadata": {"next_cursor": "p2"}, "messages": [
                        {"user": "U_OWNER", "text": "親の問いかけ（DB外）", "ts": t_ok, "reply_count": 2},
                        {"user": "U_N", "bot_id": "B_N", "text": "返信A", "ts": f"{float(t_ok) + 10:.6f}", "thread_ts": t_ok},
                    ]}
                return {"ok": True, "has_more": False, "messages": [
                    {"user": agent_tools.YAMA_BOT_USER_ID, "bot_id": "B_Y", "text": "Yama の報告（台帳外）",
                     "ts": f"{float(t_ok) + 20:.6f}", "thread_ts": t_ok, "edited": {"ts": "1"}},
                ]}

        res = agent_tools.seed_mirror(days=1, db_path=db_path, client=SeedClient(), sleep_fn=lambda s: None)
        assert res["ok"] is False and "not active" in res["error"], res
        listener_mod.mark_mirror_since(db_path, now=now - 60)
        dry = agent_tools.seed_mirror(days=1, db_path=db_path, client=SeedClient(), dry_run=True)
        assert dry["threads"] == 2 and len(dry["thread_keys"]) == 2
        client = SeedClient()
        res = agent_tools.seed_mirror(days=1, db_path=db_path, client=client, sleep_fn=lambda s: None)
        assert res["seeded"] == 1 and res["mirrored_messages"] == 2 and res["edits"] == 1, res
        assert len(res["failed"]) == 1 and res["ok"] is False
        assert all("oldest" not in c for c in client.calls)
        ev = {"event_id": "Ev_NEXT", "channel_id": "C_S", "ts": f"{now:.6f}", "thread_ts": t_ok}
        ctxs, ann = context_bundle.build_batch_contexts(db_path, [ev], state_dir=state)
        assert ann["Ev_NEXT"]["context_ok"] is True, ann
        texts = [m["text"] for m in ctxs[f"C_S:{t_ok}"]["messages"]]
        assert texts == ["親の問いかけ（DB外）", "返信A", "Yama の報告（台帳外）"], texts
        ev2 = {"event_id": "Ev_NEXT2", "channel_id": "C_S", "ts": f"{now:.6f}", "thread_ts": t_fail}
        _, ann = context_bundle.build_batch_contexts(db_path, [ev2], state_dir=state)
        assert ann["Ev_NEXT2"]["context_ok"] is False
        with get_db_connection(db_path) as conn:
            assert conn.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 2, "seed never creates events"
    return "ミラー開始前は拒否・dry-runは対象一覧のみ・全ページ読みで親投稿と台帳外の自分の投稿を補完しスレッドをシード済みに(以後context_ok=true)・読み取り失敗のスレッドは未シードのまま・eventsは増えないことを確認"


def test_hook_supplementary_jobs_after_dispatch() -> str:
    """81. フックの補完処理の順序 (backfill/reconcile/auto_joinはdispatcherの後・バッチを起こす周期は飛ばしてすぐwake・連続時もbackfillは最低5分に1回)"""
    assert HOOK_SCRIPT_PATH.exists(), HOOK_SCRIPT_PATH
    runtime = Path("/home/hatch/hooks/runtime/hatch_hook_runtime.sh")
    assert runtime.exists()
    text = HOOK_SCRIPT_PATH.read_text(encoding="utf-8")
    i_disp = text.index('output=$("$PROJECT_DIR/dispatcher.sh"')
    for marker in ('"$PROJECT_DIR/backfill.py"', "reconcile-replies --days 7 >", '"$PROJECT_DIR/auto_join.py"'):
        assert text.index(marker) > i_disp, f"{marker} must run after the dispatcher"
    with tempfile.TemporaryDirectory() as tmp:
        proj = Path(tmp) / "proj"
        (proj / "state").mkdir(parents=True)
        (proj / "config").mkdir()
        (proj / "venv").symlink_to(BASE_DIR / "venv")
        order = Path(tmp) / "order.log"
        stub_py = (
            "import sys, time\n"
            "open(sys.argv[1], 'a').write(sys.argv[2] + '\\n')\n"
        )
        (Path(tmp) / "rec.py").write_text(stub_py, encoding="utf-8")
        rec = f'"{PYTHON_BIN}" "{tmp}/rec.py" "{order}"'
        (proj / "backfill.py").write_text(
            f"import subprocess\nsubprocess.run('{rec} backfill', shell=True)\n", encoding="utf-8")
        (proj / "auto_join.py").write_text(
            f"import subprocess\nsubprocess.run('{rec} auto_join', shell=True)\n", encoding="utf-8")
        (proj / "agent_tools.py").write_text(
            f"import subprocess, sys\nsubprocess.run('{rec} ' + sys.argv[1], shell=True)\n", encoding="utf-8")
        (proj / "listener.py").write_text("x = 1\n", encoding="utf-8")
        (proj / "watchdog.sh").write_text(f"#!/usr/bin/env bash\n{rec} watchdog\n", encoding="utf-8")
        (proj / "dispatcher.sh").write_text(
            "#!/usr/bin/env bash\n"
            f"{rec} dispatcher\n"
            'code="${STUB_DISPATCH_CODE:-1}"\n'
            'if [[ "$code" == "0" ]]; then echo \'{"count": 1, "events": [{"event_id": "Ev_H"}]}\' > "$(dirname "$0")/state/payload.json"; echo DISPATCHED:1; else echo NO_EVENTS; fi\n'
            'exit "$code"\n', encoding="utf-8")
        for f in ("watchdog.sh", "dispatcher.sh"):
            (proj / f).chmod(0o755)
        ops = Path(tmp) / "ops.py"
        ops.write_text("import json\nprint(json.dumps({'ok': True, 'stale': False, 'human_pending': 0, 'oldest_age_sec': 0}))\n", encoding="utf-8")
        hook_state = Path(tmp) / "hook_state"
        hook_state.mkdir()
        env = {k: v for k, v in os.environ.items() if not k.startswith(("YAMA_", "HATCH_HOOK"))}
        env.update({
            "HATCH_HOOK_RUNTIME": str(runtime), "HATCH_HOOK_STATE_DIR": str(hook_state),
            "HATCH_HOOK_ID": "test-hook", "YAMA_PROJECT_DIR": str(proj),
            "YAMA_UNIT_SOURCE": str(proj / "no-such.service"), "YAMA_ENV_FILE": str(proj / "config" / ".env"),
            "YAMA_WATCHDOG_LOG": str(proj / "state" / "watchdog.log"), "YAMA_SYSTEMCTL_BIN": "true",
            "YAMA_OPS_MONITOR_PY": str(ops), "YAMA_OPS_ALERTS_PATH": str(proj / "state" / "ops_alerts.jsonl"),
        })

        def cycle(code: int, backfill_age: Optional[int]) -> Tuple[List[str], str]:
            if order.exists():
                order.unlink()
            bf = proj / "state" / "backfill_last_run"
            if backfill_age is None:
                if bf.exists():
                    bf.unlink()
            else:
                bf.write_text(str(int(time.time()) - backfill_age), encoding="utf-8")
            for f in ("reconcile_last_run", "auto_join_last_run"):
                p = proj / "state" / f
                if p.exists():
                    p.unlink()
            res = subprocess.run(["bash", str(HOOK_SCRIPT_PATH)], env=dict(env, STUB_DISPATCH_CODE=str(code)),
                                 capture_output=True, text=True, timeout=60)
            assert res.returncode == 0, res.stderr
            lines = order.read_text(encoding="utf-8").split() if order.exists() else []
            return lines, res.stdout

        # Dispatch cycle, backfill ran recently -> straight to wake; nothing supplementary.
        lines, out = cycle(0, backfill_age=10)
        assert lines == ["watchdog", "dispatcher"], lines
        assert '"decision":"wake"' in out and "Ev_H" in out, out
        # Dispatch cycle, backfill overdue (>= 300s) -> insurance backfill after the dispatcher, then wake.
        lines, out = cycle(0, backfill_age=301)
        assert lines == ["watchdog", "dispatcher", "backfill"], lines
        assert '"decision":"wake"' in out
        # Idle cycle -> dispatcher first, then backfill, reconcile, auto_join.
        lines, out = cycle(1, backfill_age=10)
        assert lines == ["watchdog", "dispatcher", "backfill", "reconcile-replies", "auto_join"], lines
        assert '"decision":"silent"' in out
        # Locked cycle (batch in flight) -> same as idle.
        lines, out = cycle(2, backfill_age=None)
        assert lines[:3] == ["watchdog", "dispatcher", "backfill"], lines
        # Dry run never runs the supplementary jobs.
        if order.exists():
            order.unlink()
        res = subprocess.run(["bash", str(HOOK_SCRIPT_PATH)], env=dict(env, HATCH_HOOK_DRY_RUN="1"),
                             capture_output=True, text=True, timeout=60)
        assert res.returncode == 0 and "dry-run" in res.stdout and not order.exists(), res.stdout
    return "補完処理はすべてdispatcherの後・バッチを起こす周期はwatchdog→dispatcher→即wake(補完なし)・backfillが300秒以上空いた時だけ保険で実行してからwake・待機/ロック周期はdispatcher→backfill→reconcile→auto_join・dry-runは補完なしを、実フック＋実ランタイムで確認"


def test_commit_terminal_guard() -> str:
    """82. commit_event の終端ガード（第1段、候補19）: 遷移表の各行（PENDING/PROCESSING は従来どおり・DONE/IGNORED の同一状態は no-op・別状態と FAILED は already_terminal で拒否し行は不変＋運用アラート1行・--force --reason は訂正として許可＋運用アラート）、commit-batch の検証での拒否（1件でも不正なら何もしない・同一状態は no-op で通す）、FAILED→requeue→PENDING→PROCESSING→DONE、cmd_reply の内部 commit が拒否されたときの status 表示、reconcile が判定理由を消さないこと"""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "state" / "slack_events.db"
        init_db(db_path)
        outbox = db_path.parent / "ops_alerts.jsonl"
        saved_env = os.environ.pop("YAMA_OPS_ALERTS_PATH", None)
        try:
            rows = [
                # event_id, status, retry_count, error_message
                ("Ev_G_PEND", "PENDING", 0, None),
                ("Ev_G_PROC", "PROCESSING", 0, None),
                ("Ev_G_DONE", "DONE", 0, "情報のない往復（ループ防止）"),
                ("Ev_G_IGN", "IGNORED", 0, "Neiro宛の相槌"),
                ("Ev_G_FAIL", "FAILED", 3, "ambiguous_post_outcome"),
                ("Ev_G_REAPED", "FAILED", 0, "reaped_too_many_times: reaped 3 times"),
                ("Ev_G_FIX", "IGNORED", 0, "誤判定"),
            ]
            with get_db_connection(db_path) as conn:
                for i, (eid, st, rc_, err) in enumerate(rows):
                    conn.execute(
                        "INSERT INTO events (event_id, channel_id, user_id, text, ts, status, retry_count, error_message, "
                        "created_at, updated_at) VALUES (?, 'C', 'U', 't', ?, ?, ?, ?, '2026-11-23 00:00:00', '2026-11-23 00:00:00')",
                        (eid, f"1795400000.{i:06d}", st, rc_, err),
                    )
                conn.commit()

            def snap(eid: str) -> Tuple[Any, ...]:
                with get_db_connection(db_path) as conn:
                    r = conn.execute(
                        "SELECT status, retry_count, error_message, next_retry_at, first_unverifiable_at, updated_at "
                        "FROM events WHERE event_id=?",
                        (eid,),
                    ).fetchone()
                    return tuple(r)

            def alerts() -> List[Dict[str, Any]]:
                if not outbox.exists():
                    return []
                return [json.loads(x) for x in outbox.read_text(encoding="utf-8").splitlines() if x.strip()]

            def cli(argv: List[str]) -> Tuple[int, Dict[str, Any]]:
                out, err = io.StringIO(), io.StringIO()
                with redirect_stdout(out), redirect_stderr(err):
                    rc = agent_tools.main(["--db", str(db_path)] + argv)
                txt = out.getvalue().strip() or err.getvalue().strip().splitlines()[-1]
                return rc, json.loads(txt)

            # (1) PENDING / PROCESSING -> anything: unchanged behaviour
            r = agent_tools.commit_event("Ev_G_PEND", "FAILED", error_message="boom", db_path=db_path)
            assert r["status"] == "PENDING" and r["retry_count"] == 1 and not r.get("noop"), r
            r = agent_tools.commit_event("Ev_G_PROC", "DONE", db_path=db_path)
            assert r["status"] == "DONE" and not r.get("noop"), r
            assert alerts() == [], "normal transitions write no alert"

            # (2) DONE -> DONE / IGNORED -> IGNORED: no-op success, row untouched
            before = snap("Ev_G_DONE")
            r = agent_tools.commit_event("Ev_G_DONE", "DONE", db_path=db_path)
            assert r["noop"] is True and not r.get("rejected") and r["status"] == "DONE", r
            assert snap("Ev_G_DONE") == before, "no-op must not touch the row (error_message / updated_at kept)"
            rc, out = cli(["commit", "--event-id", "Ev_G_IGN", "--status", "IGNORED"])
            assert rc == 0 and out["ok"] is True and out["noop"] is True, out
            assert alerts() == [], "no-op writes no alert"

            # (3) DONE / IGNORED -> another status: rejected (already_terminal), row untouched, 1 alert line
            for eid, req in (("Ev_G_DONE", "FAILED"), ("Ev_G_DONE", "IGNORED"), ("Ev_G_IGN", "DONE")):
                before = snap(eid)
                r = agent_tools.commit_event(eid, req, error_message="late", db_path=db_path)
                assert r["noop"] is True and r["rejected"] == "already_terminal", r
                assert r["status"] == before[0] and snap(eid) == before, (eid, req)
            assert [a["kind"] for a in alerts()] == ["commit_rejected"] * 3
            assert alerts()[0]["event_id"] == "Ev_G_DONE" and alerts()[0]["current_status"] == "DONE"
            assert alerts()[0]["requested_status"] == "FAILED" and alerts()[0]["source"] == "commit"
            rc, out = cli(["commit", "--event-id", "Ev_G_DONE", "--status", "FAILED", "--error", "x"])
            assert rc == 1 and out["ok"] is False and out["error"] == "already_terminal", out
            assert out["current_status"] == "DONE" and out["requested_status"] == "FAILED" and out["noop"] is True
            assert len(alerts()) == 4

            # (4) FAILED -> anything: rejected (incl. FAILED -> FAILED and the Reaper cap not bypassed)
            for eid, req in (("Ev_G_FAIL", "DONE"), ("Ev_G_FAIL", "FAILED"), ("Ev_G_FAIL", "IGNORED"),
                             ("Ev_G_REAPED", "FAILED")):
                before = snap(eid)
                r = agent_tools.commit_event(eid, req, db_path=db_path)
                assert r.get("rejected") == "already_terminal" and snap(eid) == before, (eid, req, r)
            assert snap("Ev_G_REAPED")[0] == "FAILED", "reaped_too_many_times must not be unblocked by a FAILED commit"
            assert len(alerts()) == 8

            # (5) --force --reason: allowed correction, reason kept, alert commit_forced
            rc, out = cli(["commit", "--event-id", "Ev_G_FIX", "--status", "DONE", "--force"])
            assert rc == 1 and "reason" in out["error"], out
            assert snap("Ev_G_FIX")[0] == "IGNORED"
            try:
                agent_tools.commit_event("Ev_G_FIX", "DONE", force=True, reason="  ", db_path=db_path)
                raise AssertionError("blank reason must be refused")
            except ValueError:
                pass
            rc, out = cli(["commit", "--event-id", "Ev_G_FIX", "--status", "DONE", "--force",
                           "--reason", "情報のない往復（ループ防止）への訂正"])
            assert rc == 0 and out["ok"] and out["forced"] is True, out
            ev = out["event"]
            assert ev["status"] == "DONE" and ev["previous_status"] == "IGNORED"
            assert ev["error_message"] == "[forced IGNORED->DONE] 情報のない往復（ループ防止）への訂正", ev
            last = alerts()[-1]
            assert last["kind"] == "commit_forced" and last["previous_status"] == "IGNORED" and last["new_status"] == "DONE"
            assert len(alerts()) == 9

            # (6) commit-batch: validation rejects a terminal->other item (nothing committed), same status = no-op
            with get_db_connection(db_path) as conn:
                conn.executemany(
                    "INSERT INTO events (event_id, channel_id, user_id, text, ts, status) VALUES (?, 'C', 'U', 't', ?, ?)",
                    [("Ev_GB1", "1795400100.1", "PROCESSING"), ("Ev_GB2", "1795400100.2", "PROCESSING"),
                     ("Ev_GB3", "1795400100.3", "IGNORED")],
                )
                conn.commit()
            f = Path(tmp) / "decisions.json"
            f.write_text(json.dumps([
                {"event_id": "Ev_GB1", "status": "IGNORED"},
                {"event_id": "Ev_G_DONE", "status": "FAILED", "error": "late"},
                {"event_id": "Ev_GB3", "status": "IGNORED"},
            ]), encoding="utf-8")
            rc, out = cli(["commit-batch", "--file", str(f)])
            assert rc == 1 and out["committed"] == 0 and out["already_terminal"] == ["Ev_G_DONE"], out
            assert any("already_terminal" in e for e in out["errors"]), out
            assert snap("Ev_GB1")[0] == "PROCESSING", "nothing committed when one item is already terminal"
            last = alerts()[-1]
            assert last["kind"] == "commit_rejected" and last["source"] == "commit-batch-validation"
            assert last["rejected"] == [{"event_id": "Ev_G_DONE", "current_status": "DONE", "requested_status": "FAILED"}]
            assert len(alerts()) == 10
            f.write_text(json.dumps([
                {"event_id": "Ev_GB1", "status": "IGNORED"},
                {"event_id": "Ev_GB3", "status": "IGNORED"},
                {"event_id": "Ev_GB2", "status": "DONE"},
            ]), encoding="utf-8")
            rc, out = cli(["commit-batch", "--file", str(f)])
            assert rc == 0 and out["ok"] and out["committed"] == 2 and out["noop"] == 1, out
            assert [x.get("noop", False) for x in out["results"]] == [False, True, False]
            assert [snap(x)[0] for x in ("Ev_GB1", "Ev_GB2", "Ev_GB3")] == ["IGNORED", "DONE", "IGNORED"]
            # force in commit-batch: needs a reason, then corrects
            f.write_text(json.dumps([{"event_id": "Ev_GB3", "status": "DONE", "force": True}]), encoding="utf-8")
            rc, out = cli(["commit-batch", "--file", str(f)])
            assert rc == 1 and out["committed"] == 0 and snap("Ev_GB3")[0] == "IGNORED", out
            f.write_text(json.dumps([{"event_id": "Ev_GB3", "status": "DONE", "force": True,
                                      "reason": "11-23型の訂正"}]), encoding="utf-8")
            rc, out = cli(["commit-batch", "--file", str(f)])
            assert rc == 0 and out["results"][0]["forced"] is True and snap("Ev_GB3")[0] == "DONE", out
            assert alerts()[-1]["kind"] == "commit_forced" and alerts()[-1]["source"] == "commit-batch"
            # race between validation and write: commit_event re-checks and reports already_terminal
            real_get = agent_tools.get_event_by_id

            def stale_get(eid: str, db_path: Path = db_path) -> Optional[Dict[str, Any]]:
                row = real_get(eid, db_path=db_path)
                if row is not None and eid == "Ev_G_DONE":
                    row = dict(row, status="PROCESSING")
                return row

            with patch.object(agent_tools, "get_event_by_id", side_effect=stale_get):
                res = agent_tools.commit_batch([{"event_id": "Ev_G_DONE", "status": "FAILED"}], db_path=db_path)
            assert res["ok"] is False and res["results"][0]["error"] == "already_terminal", res
            assert snap("Ev_G_DONE")[0] == "DONE"

            # (7) FAILED -> requeue -> PENDING -> (dispatcher) PROCESSING -> DONE
            rq = agent_tools.requeue_event("Ev_G_FAIL", db_path=db_path)
            assert rq["status"] == "PENDING"
            with get_db_connection(db_path) as conn:
                conn.execute("UPDATE events SET status='PROCESSING' WHERE event_id='Ev_G_FAIL'")
                conn.commit()
            r = agent_tools.commit_event("Ev_G_FAIL", "DONE", db_path=db_path)
            assert r["status"] == "DONE" and not r.get("noop") and r["error_message"] is None, r

            # (8) cmd_reply skipped_answered: the internal commit is rejected (settled elsewhere
            #     meanwhile) -> output status is the real one, not "DONE"
            channel_id, thread_ts = "C_ALL", "1795400200.000100"
            with get_db_connection(db_path) as conn:
                conn.execute(
                    "INSERT INTO events (event_id, channel_id, user_id, text, ts, status, retry_count, error_message, "
                    "first_unverifiable_at) VALUES ('Ev_G_REPLY', ?, 'U_Q', '原因が分かりません', ?, 'PROCESSING', 0, "
                    "'pre_post_check_unverifiable', '2026-11-23 00:00:00')",
                    (channel_id, thread_ts),
                )
                conn.commit()
            text_file = Path(tmp) / "reply.txt"
            text_file.write_text("Yamaからの遅延回答です。\n", encoding="utf-8")

            class SettledMeanwhileClient:
                def auth_test(self) -> Dict[str, Any]:
                    return {"ok": True, "user_id": "U_YAMA_BOT"}

                def conversations_replies(self, **kwargs: Any) -> Dict[str, Any]:
                    with get_db_connection(db_path) as conn:
                        conn.execute("UPDATE events SET status='IGNORED' WHERE event_id='Ev_G_REPLY'")
                        conn.commit()
                    return {"ok": True, "has_more": False, "messages": [
                        {"user": "U_Q", "text": "原因が分かりません", "ts": thread_ts},
                        {"user": "U_OTHER", "text": "config/.env の権限設定を確認すると直りますよ。",
                         "ts": "1795400245.000200", "thread_ts": thread_ts},
                    ]}

                def chat_postMessage(self, **kwargs: Any) -> Dict[str, Any]:
                    raise AssertionError("must not post")

            out_buf, err_buf = io.StringIO(), io.StringIO()
            with patch.object(agent_tools, "get_web_client", return_value=SettledMeanwhileClient()), \
                    redirect_stdout(out_buf), redirect_stderr(err_buf):
                rc = agent_tools.main(["--db", str(db_path), "reply", "--channel", channel_id, "--text-file",
                                       str(text_file), "--thread-ts", thread_ts, "--event-id", "Ev_G_REPLY"])
            assert rc == 0, err_buf.getvalue()
            out = json.loads(out_buf.getvalue())
            assert out["result"] == "skipped_answered" and out["status"] == "IGNORED", out
            assert out["commit_rejected"] == "already_terminal"
            assert alerts()[-1]["kind"] == "commit_rejected" and alerts()[-1]["source"] == "reply_skipped_answered"
            assert snap("Ev_G_REPLY")[0] == "IGNORED"

            # (9) reconcile: only reply/post-path leftovers on DONE are cleared; judgement reasons stay
            assert agent_tools.is_reconcile_clearable_error("reply not posted, no replies-table record")
            assert agent_tools.is_reconcile_clearable_error("pre_post_check_unverifiable")
            assert agent_tools.is_reconcile_clearable_error("IncompleteRead(64 bytes read)")
            assert not agent_tools.is_reconcile_clearable_error("情報のない往復（ループ防止）")
            assert not agent_tools.is_reconcile_clearable_error("既存返信あり（リトライ重複防止）")
            assert not agent_tools.is_reconcile_clearable_error("[forced IGNORED->DONE] reply は不要と判断")
            recent = (datetime.now(timezone.utc) - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
            with get_db_connection(db_path) as conn:
                conn.execute("DELETE FROM events")
                conn.execute(
                    "INSERT INTO events (event_id, channel_id, user_id, text, ts, status, error_message, created_at, updated_at) "
                    "VALUES ('Ev_G_REASON', 'C', 'U', 't', '1795400300.000001', 'DONE', '情報のない往復（ループ防止）', ?, ?)",
                    (recent, recent),
                )
                conn.commit()

            class NoCallClient:
                def __getattr__(self, name: str) -> Any:
                    raise AssertionError(f"reconcile must not scan a judgement-reason event ({name})")

            res = agent_tools.reconcile_replies(db_path=db_path, client=NoCallClient(), sleep_fn=lambda s: None)
            assert res["ok"] is True, res
            with get_db_connection(db_path) as conn:
                kept = conn.execute("SELECT error_message FROM events WHERE event_id='Ev_G_REASON'").fetchone()[0]
            assert kept == "情報のない往復（ループ防止）", kept
        finally:
            if saved_env is not None:
                os.environ["YAMA_OPS_ALERTS_PATH"] = saved_env
    return ("PENDING/PROCESSING従来どおり・DONE/IGNORED同一状態はno-op(行不変・アラートなし)・別状態とFAILED(→FAILED含む・reaped上限の迂回なし)は"
            "already_terminalで拒否(行不変・exit 1・アラート1行)・--forceは理由必須で訂正(error_messageに理由・commit_forced)・"
            "commit-batchは検証で拒否(何もcommitせず・集約アラート)/同一状態はno-op/force対応/検証後の競合も再確認・"
            "FAILED→requeue→PROCESSING→DONE・cmd_replyの内部commit拒否時は実際のstatus表示・reconcileは判定理由を消さない、を確認")


def test_failed_with_landed_reply_detect_and_requeue_guard() -> str:
    """84. 候補27 代替案A: reconcile は状態を変えず「台帳に着地記録があるのに FAILED」を検知して運用アラートへ1行（同じ件は繰り返さない・FAILED でなくなれば記録から外す・dry-run は書かない・窓の外の古い FAILED も対象＝Ev0CH5CWKHM0 型・PENDING/PROCESSING/DONE は対象外）、手動 requeue は台帳に着地ありならブロック（行は不変＋requeue_blocked 1行）、--force --reason でだけ通す（理由を error_message に残し requeue_forced 1行・台帳は残るので reply は重複排除）"""
    import ops_monitor

    saved_env = os.environ.pop("YAMA_OPS_ALERTS_PATH", None)
    try:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "state" / "slack_events.db"
            init_db(db_path)
            outbox = db_path.parent / "ops_alerts.jsonl"
            alerted_state = db_path.parent / agent_tools.FAILED_LANDED_ALERTED_FILENAME
            now = datetime(2026, 11, 23, 12, 0, 0, tzinfo=timezone.utc)
            th = "1795400000.000000"
            events = [
                # event_id, status, error_message, ts, created_at, ledger reply_ts (None = no ledger)
                # 実例 Ev0CH5CWKHM0 と同じ形: 11-12 作成（reconcile の7日窓の外）・FAILED・台帳あり
                ("Ev_L_OLD", "FAILED",
                 "pre_post_check_unverifiable: Slack API IncompleteRead（投稿前のスレッド確認が失敗、投稿は未実施。次回再送）",
                 "1794492249.625919", "2026-11-12 14:04:09", "1794492548.415999"),
                ("Ev_L_PROC", "PROCESSING", None, "1795400001.000000", "2026-11-23 00:00:00", "1795400101.000000"),
                ("Ev_L_PEND", "PENDING", None, "1795400002.000000", "2026-11-23 00:00:00", "1795400102.000000"),
                ("Ev_L_DONE", "DONE", None, "1795400003.000000", "2026-11-23 00:00:00", "1795400103.000000"),
                ("Ev_L_NOLEDGER", "FAILED", "agent error", "1795400004.000000", "2026-11-23 00:00:00", None),
                # 窓の中・FAILED・台帳なし。スレッドに metadata 付きの着地があり、この run で台帳に記録される
                ("Ev_L_SCAN", "FAILED", "ambiguous_post_outcome", "1795400005.000000", "2026-11-23 00:00:00", None),
            ]
            with get_db_connection(db_path) as conn:
                for eid, st, err, ts, created, _ in events:
                    conn.execute(
                        "INSERT INTO events (event_id, channel_id, user_id, text, ts, thread_ts, status, retry_count, "
                        "error_message, created_at, updated_at) VALUES (?, 'C_L', 'U_Q', 't', ?, ?, ?, 3, ?, ?, ?)",
                        (eid, ts, th, st, err, created, created),
                    )
                conn.commit()
            for eid, _, _, _, _, rts in events:
                if rts:
                    agent_tools.record_reply(event_id=eid, channel_id="C_L", thread_ts=th, reply_ts=rts,
                                 text=f"reply for {eid}", db_path=db_path)

            scan_ts = "1795400105.000000"

            class LandedClient:
                def __init__(self) -> None:
                    self.calls = 0

                def auth_test(self) -> Dict[str, Any]:
                    return {"ok": True, "user_id": "U_YAMA"}

                def conversations_replies(self, **kw: Any) -> Dict[str, Any]:
                    self.calls += 1
                    return {
                        "ok": True,
                        "messages": [
                            {"user": "U_Q", "text": "質問", "ts": th},
                            {"user": "U_YAMA", "text": "Yama の返信", "ts": scan_ts, "thread_ts": th,
                             "metadata": {"event_type": "yama_reply", "event_payload": {"event_id": "Ev_L_SCAN"}}},
                        ],
                        "has_more": False,
                    }

            def alerts() -> List[Dict[str, Any]]:
                if not outbox.exists():
                    return []
                return [json.loads(x) for x in outbox.read_text(encoding="utf-8").splitlines() if x.strip()]

            def status_of(eid: str) -> Tuple[Any, ...]:
                with get_db_connection(db_path) as conn:
                    r = conn.execute(
                        "SELECT status, retry_count, reap_count, error_message, next_retry_at FROM events WHERE event_id=?",
                        (eid,),
                    ).fetchone()
                    return tuple(r)

            before_all = {e[0]: status_of(e[0]) for e in events}

            # (1) dry-run: 検知は報告するが outbox・状態ファイル・台帳・状態は書かない
            res = agent_tools.reconcile_replies(days=7, dry_run=True, db_path=db_path, client=LandedClient(),
                                                bot_user_id="U_YAMA", now=now)
            fw = res["failed_with_landed_reply"]
            assert sorted(fw["found"]) == ["Ev_L_OLD", "Ev_L_SCAN"], fw
            assert fw["alerted"] is False and alerts() == [] and not alerted_state.exists(), fw
            assert get_recorded_reply("Ev_L_SCAN", db_path=db_path) is None

            # (2) 本番 run: Ev_L_SCAN を台帳に記録 → 同じ run の検知に入る。窓の外の Ev_L_OLD も入る。
            #     PROCESSING / PENDING / DONE（台帳あり）と FAILED（台帳なし）は対象外。状態は一切変えない
            res = agent_tools.reconcile_replies(days=7, dry_run=False, db_path=db_path, client=LandedClient(),
                                                bot_user_id="U_YAMA", now=now)
            assert res["recorded"] == [{"event_id": "Ev_L_SCAN", "reply_ts": scan_ts}], res["recorded"]
            fw = res["failed_with_landed_reply"]
            assert fw["found"] == ["Ev_L_OLD", "Ev_L_SCAN"] and fw["new"] == ["Ev_L_OLD", "Ev_L_SCAN"], fw
            assert fw["alerted"] is True
            a = alerts()
            assert len(a) == 1 and a[0]["kind"] == "failed_with_landed_reply" and a[0]["count"] == 2, a
            assert a[0]["event_ids"] == ["Ev_L_OLD", "Ev_L_SCAN"], a
            assert {i["event_id"]: i["reply_ts"] for i in a[0]["items"]} == {
                "Ev_L_OLD": "1794492548.415999", "Ev_L_SCAN": scan_ts}, a
            for eid, *_ in events:
                assert status_of(eid) == before_all[eid], f"reconcile must not change {eid}"

            # (3) 次の run（毎時）では同じ件を繰り返さない
            res = agent_tools.reconcile_replies(days=7, dry_run=False, db_path=db_path, client=LandedClient(),
                                                bot_user_id="U_YAMA", now=now)
            assert res["failed_with_landed_reply"]["new"] == [] and len(alerts()) == 1, res

            # (4) requeue: 台帳に着地あり → ブロック（行は不変・明確なエラー・requeue_blocked 1行）
            before = status_of("Ev_L_OLD")
            out, err = io.StringIO(), io.StringIO()
            with redirect_stdout(out), redirect_stderr(err):
                rc = agent_tools.main(["--db", str(db_path), "requeue", "--event-id", "Ev_L_OLD"])
            assert rc == 1, out.getvalue()
            body = json.loads(err.getvalue().strip().splitlines()[-1])
            assert body["error"] == "reply_already_landed" and body["landed_reply"]["reply_ts"] == "1794492548.415999", body
            assert "--force --reason" in body["message"], body
            assert status_of("Ev_L_OLD") == before
            a = alerts()
            assert len(a) == 2 and a[1]["kind"] == "requeue_blocked" and a[1]["event_id"] == "Ev_L_OLD", a
            assert ops_monitor.archive_record(a[1])["action"] == "rejected"

            # --force だけ（理由なし）は通さない
            with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                assert agent_tools.main(["--db", str(db_path), "requeue", "--event-id", "Ev_L_OLD", "--force"]) == 1
                assert agent_tools.main(["--db", str(db_path), "requeue", "--event-id", "Ev_L_OLD",
                                         "--force", "--reason", "   "]) == 1
            assert status_of("Ev_L_OLD") == before and len(alerts()) == 2

            # (5) PROCESSING / PENDING は従来どおり「FAILED でない」で拒否（台帳ブロックの対象外）
            for eid in ("Ev_L_PROC", "Ev_L_PEND"):
                err = io.StringIO()
                with redirect_stdout(io.StringIO()), redirect_stderr(err):
                    assert agent_tools.main(["--db", str(db_path), "requeue", "--event-id", eid]) == 1
                assert "not FAILED" in err.getvalue() and "reply_already_landed" not in err.getvalue(), err.getvalue()
                assert status_of(eid) == before_all[eid]

            # (6) 台帳なしの FAILED は従来どおり通る（アラートも出ない）
            r = agent_tools.requeue_event("Ev_L_NOLEDGER", db_path=db_path)
            assert r["status"] == "PENDING" and r["error_message"] == "agent error" and not r.get("forced"), r
            assert len(alerts()) == 2

            # (7) --force --reason で通す: 理由を error_message に残し requeue_forced 1行。台帳は残す
            out = io.StringIO()
            with redirect_stdout(out), redirect_stderr(io.StringIO()):
                rc = agent_tools.main(["--db", str(db_path), "requeue", "--event-id", "Ev_L_OLD",
                                       "--force", "--reason", "スレッドで返信の欠落を確認したため"])
            assert rc == 0, out.getvalue()
            ev = json.loads(out.getvalue())["event"]
            assert ev["status"] == "PENDING" and ev["reap_count"] == 0 and ev["retry_count"] == 3, ev
            assert ev["forced"] is True and ev["landed_reply_ts"] == "1794492548.415999", ev
            assert ev["error_message"].startswith("pre_post_check_unverifiable") and \
                "[forced requeue: reply already landed ts=1794492548.415999] スレッドで返信の欠落を確認したため" in ev["error_message"], ev
            a = alerts()
            assert len(a) == 3 and a[2]["kind"] == "requeue_forced" and a[2]["new_status"] == "PENDING", a
            assert ops_monitor.archive_record(a[2])["action"] == "forced"
            assert get_recorded_reply("Ev_L_OLD", db_path=db_path) is not None

            # (8) FAILED でなくなった件は既通知の記録から外れ、再び FAILED になれば再通知される
            res = agent_tools.reconcile_replies(days=7, dry_run=False, db_path=db_path, client=LandedClient(),
                                                bot_user_id="U_YAMA", now=now)
            assert res["failed_with_landed_reply"]["found"] == ["Ev_L_SCAN"], res["failed_with_landed_reply"]
            state = json.loads(alerted_state.read_text(encoding="utf-8"))
            assert sorted(state) == ["Ev_L_SCAN"], state
            with get_db_connection(db_path) as conn:
                conn.execute("UPDATE events SET status='FAILED' WHERE event_id='Ev_L_OLD'")
                conn.commit()
            res = agent_tools.reconcile_replies(days=7, dry_run=False, db_path=db_path, client=LandedClient(),
                                                bot_user_id="U_YAMA", now=now)
            assert res["failed_with_landed_reply"]["new"] == ["Ev_L_OLD"], res["failed_with_landed_reply"]
            assert alerts()[-1]["kind"] == "failed_with_landed_reply" and alerts()[-1]["event_ids"] == ["Ev_L_OLD"]

            # (9) アラート書き込みに失敗した run は既通知に入れず、次の run で再試行する
            with get_db_connection(db_path) as conn:
                conn.execute("UPDATE events SET status='DONE' WHERE event_id='Ev_L_SCAN'")
                conn.commit()
            agent_tools.detect_failed_with_landed_reply(db_path=db_path)  # Ev_L_SCAN を記録から外す
            with get_db_connection(db_path) as conn:
                conn.execute("UPDATE events SET status='FAILED' WHERE event_id='Ev_L_SCAN'")
                conn.commit()
            os.environ["YAMA_OPS_ALERTS_PATH"] = str(db_path.parent)  # ディレクトリなので開けない
            try:
                with redirect_stderr(io.StringIO()):
                    r1 = agent_tools.detect_failed_with_landed_reply(db_path=db_path)
            finally:
                os.environ.pop("YAMA_OPS_ALERTS_PATH", None)
            assert r1["new"] == ["Ev_L_SCAN"] and r1["alerted"] is False, r1
            r2 = agent_tools.detect_failed_with_landed_reply(db_path=db_path)
            assert r2["new"] == ["Ev_L_SCAN"] and r2["alerted"] is True, r2
    finally:
        if saved_env is not None:
            os.environ["YAMA_OPS_ALERTS_PATH"] = saved_env

    return ("dry-runは書かない・台帳に着地ありのFAILEDだけ検知(窓外の古い実例型も・同じrunで記録した分も)・PROCESSING/PENDING/DONE/台帳なしは対象外・状態は不変・"
            "同じ件は再通知しない・FAILEDを外れたら記録から外し再発で再通知・書込失敗は次runで再試行・requeueは台帳ありでブロック(行不変＋requeue_blocked)・"
            "--force単独/空理由は不可・--force --reasonで通過(理由をerror_messageに・requeue_forced・台帳は保持)・PROCESSING/PENDINGは従来の拒否")


def test_ops_alert_archive_on_take() -> str:
    """83. 運用アラートの永続ログ（候補26）: outbox-take の時点で取り出した行を ops_alerts_archive.jsonl に追記（単一経路・書き手側は二重書きしない）・mark_rejected と揃えたスキーマ（at/key/attempted_status/current_status/reason＋source/action）・rejected と forced の区別・サイズ上限で1世代ローテーション・書き込み失敗でも take は成功し archive_write_failed を1行返す（stderr は汚さない）・restore した行は二重にアーカイブしない"""
    import ops_monitor

    saved_env = {k: os.environ.pop(k, None) for k in (
        "YAMA_OPS_ALERTS_PATH", "YAMA_OPS_ALERTS_ARCHIVE_PATH",
        "YAMA_OPS_ALERTS_ARCHIVE_MAX_BYTES", "YAMA_OPS_ARCHIVE_FORCE_FAIL")}
    try:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "state" / "slack_events.db"
            init_db(db_path)
            outbox = db_path.parent / "ops_alerts.jsonl"
            archive = db_path.parent / "ops_alerts_archive.jsonl"

            def arch_rows(p: Path = archive) -> List[Dict[str, Any]]:
                if not p.exists():
                    return []
                return [json.loads(x) for x in p.read_text(encoding="utf-8").splitlines() if x.strip()]

            with get_db_connection(db_path) as conn:
                for i, (eid, st) in enumerate((("Ev_A_DONE", "DONE"), ("Ev_A_IGN", "IGNORED"))):
                    conn.execute(
                        "INSERT INTO events (event_id, channel_id, user_id, text, ts, status, retry_count, "
                        "created_at, updated_at) VALUES (?, 'C', 'U', 't', ?, ?, 0, '2026-11-23 00:00:00', '2026-11-23 00:00:00')",
                        (eid, f"1795500000.{i:06d}", st),
                    )
                conn.commit()

            # (1) 本物の書き手（commit 終端ガード）→ outbox → take → archive。書き手は archive に書かない
            with redirect_stderr(io.StringIO()):
                agent_tools.commit_event("Ev_A_DONE", "FAILED", error_message="late", db_path=db_path)
                agent_tools.commit_event("Ev_A_IGN", "DONE", db_path=db_path, force=True, reason="誤判定の訂正")
            ops_monitor.outbox_append({"kind": "reaper", "reaped_count": 2, "at": "2026-11-23T00:00:00Z"}, outbox)
            ops_monitor.outbox_append({"kind": "commit_rejected", "reason": "already_terminal",
                                       "source": "commit-batch-validation", "batch_size": 2,
                                       "rejected": [{"event_id": "Ev_X1"}, {"event_id": "Ev_X2"}]}, outbox)
            assert not archive.exists(), "書き手（ガード）からは archive に書かない（二重書きなし）"
            taken = ops_monitor.outbox_take(outbox)
            assert [e["kind"] for e in taken] == ["commit_rejected", "commit_forced", "reaper", "commit_rejected"], taken
            assert outbox.read_text(encoding="utf-8") == ""
            rows = arch_rows()
            assert len(rows) == 4, rows
            shared = {"at", "key", "attempted_status", "current_status", "reason", "source", "action"}
            for r in rows:
                assert shared <= set(r), r
                assert r["schema"] == 1 and r["archived_at"] and isinstance(r["entry"], dict)
            rej, forced, reaper, batch = rows
            assert rej["action"] == "rejected" and rej["key"] == "Ev_A_DONE"
            assert rej["attempted_status"] == "FAILED" and rej["current_status"] == "DONE"
            assert rej["reason"] == "already_terminal" and rej["source"] == "commit"
            assert forced["action"] == "forced" and forced["key"] == "Ev_A_IGN"
            assert forced["attempted_status"] == "DONE" and forced["current_status"] == "IGNORED"
            assert forced["new_status"] == "DONE" and forced["reason"] == "誤判定の訂正"
            assert reaper["action"] == "alert" and reaper["key"] is None and reaper["at"] == "2026-11-23T00:00:00Z"
            assert reaper["entry"]["reaped_count"] == 2
            assert batch["action"] == "rejected" and batch["keys"] == ["Ev_X1", "Ev_X2"]
            assert ops_monitor.outbox_take(outbox) == [] and len(arch_rows()) == 4, "空の take は何も足さない"

            # (2) restore（wake 失敗）した行は次の take で二重にアーカイブしない・印は配送内容に出さない
            rs = subprocess.run(
                [str(PYTHON_BIN), str(BASE_DIR / "ops_monitor.py"), "outbox-restore", "--path", str(outbox)],
                input=json.dumps(taken[2:3]), capture_output=True, text=True,
            )
            assert rs.returncode == 0, rs
            ops_monitor.outbox_append({"kind": "backfill_truncated", "channel": "C_NEW"}, outbox)
            again = ops_monitor.outbox_take(outbox)
            assert [e["kind"] for e in again] == ["reaper", "backfill_truncated"], again
            assert all(ops_monitor.ARCHIVED_MARKER not in e for e in again), again
            rows = arch_rows()
            assert len(rows) == 5 and rows[-1]["kind"] == "backfill_truncated" and rows[-1]["key"] == "C_NEW", rows

            # (3) ローテーション: 上限超えで .1 へ（1世代のみ・古い .1 は置き換え）・本体は上限内
            rot_dir = Path(tmp) / "rot"
            rot_out = rot_dir / "ops_alerts.jsonl"
            rot_arch = rot_dir / "ops_alerts_archive.jsonl"
            os.environ["YAMA_OPS_ALERTS_ARCHIVE_MAX_BYTES"] = "1500"
            for n in range(12):
                ops_monitor.outbox_append({"kind": "reaper", "n": n, "pad": "x" * 200}, rot_out)
                ops_monitor.outbox_take(rot_out)
            gen1 = rot_arch.with_name(rot_arch.name + ".1")
            assert gen1.exists() and rot_arch.exists()
            assert rot_arch.stat().st_size <= 1500 and gen1.stat().st_size <= 1500
            assert sorted(p.name for p in rot_dir.iterdir() if "archive" in p.name and not p.name.endswith(".lock")) == [
                "ops_alerts_archive.jsonl", "ops_alerts_archive.jsonl.1"], "世代は1つだけ"
            ns = [r["entry"]["n"] for r in arch_rows(gen1) + arch_rows(rot_arch)]
            assert ns == sorted(ns) and ns[-1] == 11 and ns[0] > 0, ns  # 最古は捨てられ、最新は残る
            os.environ.pop("YAMA_OPS_ALERTS_ARCHIVE_MAX_BYTES")

            # (4) 書き込み失敗: take は成功（outbox は空・元の行も配送される）＋archive_write_failed を1行
            bad_dir = Path(tmp) / "bad"
            bad_out = bad_dir / "ops_alerts.jsonl"
            ops_monitor.outbox_append({"kind": "commit_forced", "event_id": "Ev_F"}, bad_out)
            blocked = bad_dir / "blocked"
            blocked.write_text("not a dir", encoding="utf-8")
            res = ops_monitor.outbox_take(bad_out, archive_path=blocked / "archive.jsonl")
            assert [e["kind"] for e in res] == ["commit_forced", "archive_write_failed"], res
            assert res[1]["lost_count"] == 1 and res[1]["lost_kinds"] == ["commit_forced"] and res[1]["error"]
            assert bad_out.read_text(encoding="utf-8") == ""
            # CLI 経由（フックの呼び方）: exit 0・stdout は JSON 配列・stderr は空（フックは 2>&1 で読む）
            ops_monitor.outbox_append({"kind": "reaper"}, bad_out)
            env = dict(os.environ, YAMA_OPS_ARCHIVE_FORCE_FAIL="1")
            cp = subprocess.run(
                [str(PYTHON_BIN), str(BASE_DIR / "ops_monitor.py"), "outbox-take", "--path", str(bad_out)],
                env=env, capture_output=True, text=True,
            )
            assert cp.returncode == 0 and cp.stderr == "", cp
            out = json.loads(cp.stdout)
            assert [e["kind"] for e in out] == ["reaper", "archive_write_failed"], out
            # wake 失敗で restore された失敗通知は、次の成功した take でアーカイブされる
            subprocess.run(
                [str(PYTHON_BIN), str(BASE_DIR / "ops_monitor.py"), "outbox-restore", "--path", str(bad_out)],
                input=json.dumps(out), capture_output=True, text=True, check=True,
            )
            out2 = ops_monitor.outbox_take(bad_out)
            assert [e["kind"] for e in out2] == ["reaper", "archive_write_failed"], out2
            # 監査指摘4の修正（2026-11-23）: アーカイブに失敗した reaper 行も、次の成功した take でアーカイブされる
            # （旧 assert は ["archive_write_failed"] だけで、失敗行が「アーカイブ済み」扱いで抜け落ちる挙動を固定していた）
            assert [r["kind"] for r in arch_rows(bad_dir / "ops_alerts_archive.jsonl")] == ["reaper", "archive_write_failed"]
    finally:
        for k, v in saved_env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    return "take時に4行(rejected/forced/alert/バッチrejected)をアーカイブ・ガードは書かない・共通スキーマ＋source/action・forcedはcurrent=訂正前/new_status・restore行の二重記録なし＆印は配送に出ない・1500B上限で.1へ1世代ローテーション・書込失敗でもtake成功＋archive_write_failed(CLIはexit 0・stderr空)・失敗通知は次回アーカイブ、を確認"


def test_reply_event_id_guard_and_metadata_read_no_fallback() -> str:
    """85. 候補28 event-id 付け忘れ・打ち間違い防止: reply は --event-id <ID> と --no-event-id の二択必須
    （どちらも無い・両方ある・空の --event-id は非0で何も読まず投稿しない＋reply_rejected 1行）、--event-id は
    events に実在しなければ unknown_event_id で拒否、--force は --reason 必須、--force --reason でだけ通す
    （reply_forced 1行・台帳には指定 ID で記録）、--no-event-id の投稿は台帳に記録されない（metadata は
    request_id）、--no-event-id に --force は不可。読み取り側: 本番 slack_sdk は include_all_metadata を宣言して
    おり、_pre_post_check_thread_for_reply / _scan_thread_yama_messages は TypeError を握りつぶして
    include_all_metadata なしで呼び直さない（1回だけ呼んで例外を上げる・遅延再送の reply は fail-closed）"""
    import inspect
    import ops_monitor
    from slack_sdk import WebClient as RealWebClient

    saved_env = os.environ.pop("YAMA_OPS_ALERTS_PATH", None)
    try:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "state" / "slack_events.db"
            init_db(db_path)
            outbox = db_path.parent / "ops_alerts.jsonl"
            channel_id, thread_ts = "C_ALL", "1795450000.000100"
            with get_db_connection(db_path) as conn:
                conn.execute(
                    "INSERT INTO events (event_id, channel_id, user_id, text, ts, thread_ts, status) "
                    "VALUES ('Ev_R28_REAL', ?, 'U_Q', '質問です', '1795450001.000200', ?, 'PROCESSING')",
                    (channel_id, thread_ts),
                )
                conn.commit()
            text_file = Path(tmp) / "reply.txt"
            text_file.write_text("候補28のテスト返信です。\n", encoding="utf-8")

            def alerts() -> List[Dict[str, Any]]:
                if not outbox.exists():
                    return []
                return [json.loads(x) for x in outbox.read_text(encoding="utf-8").splitlines() if x.strip()]

            def ledger_rows() -> List[Tuple[Any, ...]]:
                with get_db_connection(db_path) as conn:
                    return [tuple(r) for r in conn.execute(
                        "SELECT event_id, channel_id, thread_ts, reply_ts FROM replies ORDER BY reply_ts"
                    ).fetchall()]

            class PostClient:
                def __init__(self) -> None:
                    self.posts: List[Dict[str, Any]] = []
                    self.reads = 0

                def auth_test(self) -> Dict[str, Any]:
                    return {"ok": True, "user_id": "U_YAMA_BOT"}

                def conversations_replies(self, **kwargs: Any) -> Dict[str, Any]:
                    self.reads += 1
                    return {"ok": True, "messages": [], "has_more": False}

                def conversations_history(self, **kwargs: Any) -> Dict[str, Any]:
                    self.reads += 1
                    return {"ok": True, "messages": [], "has_more": False}

                def chat_postMessage(self, **kwargs: Any) -> Dict[str, Any]:
                    self.posts.append(kwargs)
                    ts = f"1795450100.00000{len(self.posts)}"
                    return {"ok": True, "channel": kwargs["channel"], "ts": ts,
                            "message": {"text": kwargs["text"], "ts": ts}}

            def run(extra: List[str], thread: bool = True) -> Tuple[int, str, str, PostClient]:
                client = PostClient()
                argv = ["--db", str(db_path), "reply", "--channel", channel_id, "--text-file", str(text_file)]
                if thread:
                    argv += ["--thread-ts", thread_ts]
                argv += extra
                out, err = io.StringIO(), io.StringIO()
                with patch.object(agent_tools, "get_web_client", return_value=client), \
                        patch.object(agent_tools.time, "sleep", return_value=None), \
                        redirect_stdout(out), redirect_stderr(err):
                    rc = agent_tools.main(argv)
                return rc, out.getvalue(), err.getvalue(), client

            def assert_refused(extra: List[str], code: str, n_alerts: int) -> None:
                rc, out, err, client = run(extra)
                assert rc == 1, f"{extra}: expected exit 1, got {rc} (stdout={out})"
                assert out == "", out
                e = json.loads(err)
                assert e["ok"] is False and e["error"] == code, e
                assert client.posts == [] and client.reads == 0, "a refused reply must not read or post"
                a = alerts()
                assert len(a) == n_alerts, a
                assert a[-1]["kind"] == "reply_rejected" and a[-1]["reason"] == code and a[-1]["source"] == "reply", a[-1]
                assert ledger_rows() == []

            # (1) neither --event-id nor --no-event-id -> refused (the forgotten-id guard itself)
            assert_refused([], "event_id_choice_missing", 1)
            # (2) both given / empty --event-id -> refused
            assert_refused(["--event-id", "Ev_R28_REAL", "--no-event-id"], "event_id_choice_conflict", 2)
            assert_refused(["--event-id", ""], "event_id_empty", 3)
            assert_refused(["--event-id", "   "], "event_id_empty", 4)
            # (3) unknown --event-id (typo) -> refused; --force alone / empty reason -> refused
            assert_refused(["--event-id", "Ev_R28_TYPO"], "unknown_event_id", 5)
            assert alerts()[-1]["event_id"] == "Ev_R28_TYPO"
            assert_refused(["--event-id", "Ev_R28_TYPO", "--force"], "force_requires_reason", 6)
            assert_refused(["--event-id", "Ev_R28_TYPO", "--force", "--reason", "  "], "force_requires_reason", 7)
            # --no-event-id with --force / --reason -> refused (force only applies to --event-id)
            assert_refused(["--no-event-id", "--force", "--reason", "x"], "force_without_event_id", 8)
            assert_refused(["--no-event-id", "--reason", "x"], "force_without_event_id", 9)

            # (4) --force --reason lets the unknown id through: posts once, reply_forced alert, ledger keeps the id
            rc, out, err, client = run(["--event-id", "Ev_R28_TYPO", "--force", "--reason", "手動投稿の記録用"])
            assert rc == 0, err
            assert len(client.posts) == 1
            assert client.posts[0]["metadata"]["event_payload"] == {"event_id": "Ev_R28_TYPO"}
            a = alerts()
            assert len(a) == 10 and a[-1]["kind"] == "reply_forced", a[-1]
            assert a[-1]["event_id"] == "Ev_R28_TYPO" and a[-1]["reason"] == "手動投稿の記録用"
            assert a[-1]["forced_check"] == "unknown_event_id"
            assert [r[0] for r in ledger_rows()] == ["Ev_R28_TYPO"]

            # (5) existing --event-id: no --force needed, no alert, recorded in the ledger
            text_file.write_text("候補28のテスト返信（実在イベント）です。\n", encoding="utf-8")
            rc, out, err, client = run(["--event-id", "Ev_R28_REAL"])
            assert rc == 0, err
            assert len(client.posts) == 1 and json.loads(out)["event_id"] == "Ev_R28_REAL"
            assert len(alerts()) == 10, "a valid --event-id leaves no ops alert"
            assert sorted(r[0] for r in ledger_rows()) == ["Ev_R28_REAL", "Ev_R28_TYPO"]
            # --force with an existing id is harmless (no reply_forced line)
            text_file.write_text("候補28のテスト返信（実在イベント・force）です。\n", encoding="utf-8")
            with get_db_connection(db_path) as conn:
                conn.execute("DELETE FROM replies WHERE event_id='Ev_R28_REAL'")
                conn.commit()
            rc, out, err, client = run(["--event-id", "Ev_R28_REAL", "--force", "--reason", "念のため"])
            assert rc == 0 and len(client.posts) == 1, err
            assert len(alerts()) == 10

            # (6) --no-event-id share posts (thread and channel): posted, never recorded in the ledger
            before = ledger_rows()
            for thread in (True, False):
                text_file.write_text(f"共有の投稿です（thread={thread}）。\n", encoding="utf-8")
                rc, out, err, client = run(["--no-event-id"], thread=thread)
                assert rc == 0, err
                assert len(client.posts) == 1
                payload = client.posts[0]["metadata"]["event_payload"]
                assert "event_id" not in payload and payload.get("request_id"), payload
                assert "event_id" not in json.loads(out)
                assert ledger_rows() == before, "--no-event-id must not write the replies ledger"
            assert len(alerts()) == 10

            # (7) archive classification of the new kinds (rejected / forced, like commit/requeue)
            recs = {e["kind"]: ops_monitor.archive_record(e) for e in alerts()}
            assert recs["reply_rejected"]["action"] == "rejected"
            assert recs["reply_forced"]["action"] == "forced"
            assert recs["reply_forced"]["key"] == "Ev_R28_TYPO"

            # (8) read side: the production SDK declares include_all_metadata ...
            for name in ("conversations_replies", "conversations_history"):
                assert "include_all_metadata" in inspect.signature(getattr(RealWebClient, name)).parameters, name

            # ... so a TypeError is a bug: no silent retry without the flag (text-only match)
            class StrictOldClient:
                def __init__(self) -> None:
                    self.calls: List[Dict[str, Any]] = []
                    self.posts: List[Dict[str, Any]] = []

                def auth_test(self) -> Dict[str, Any]:
                    return {"ok": True, "user_id": "U_YAMA_BOT"}

                def conversations_replies(self, channel: str, ts: str, oldest: Optional[str] = None,
                                          limit: int = 20, cursor: Optional[str] = None,
                                          latest: Optional[str] = None) -> Dict[str, Any]:
                    raise AssertionError("must not be reached without include_all_metadata")

                def chat_postMessage(self, **kwargs: Any) -> Dict[str, Any]:
                    self.posts.append(kwargs)
                    raise AssertionError("must not post")

            def recording(client: "StrictOldClient") -> Callable[..., Any]:
                original = StrictOldClient.conversations_replies

                def _call(**kwargs: Any) -> Any:
                    client.calls.append(kwargs)
                    return original(client, **kwargs)
                return _call

            c1 = StrictOldClient()
            c1.conversations_replies = recording(c1)  # type: ignore[assignment]
            try:
                agent_tools._pre_post_check_thread_for_reply(
                    channel=channel_id, raw_text="x", post_text="x", thread_ts=thread_ts,
                    client=c1, bot_user_id="U_YAMA_BOT", db_path=db_path, sleep_fn=lambda s: None,
                )
                raise AssertionError("expected TypeError from _pre_post_check_thread_for_reply")
            except TypeError:
                pass
            assert len(c1.calls) == 1 and c1.calls[0]["include_all_metadata"] is True, c1.calls

            c2 = StrictOldClient()
            c2.conversations_replies = recording(c2)  # type: ignore[assignment]
            try:
                agent_tools._scan_thread_yama_messages(
                    channel=channel_id, parent_ts=thread_ts, client=c2, bot_user_ids={"U_YAMA_BOT"},
                    sleep_fn=lambda s: None,
                )
                raise AssertionError("expected TypeError from _scan_thread_yama_messages")
            except TypeError:
                pass
            assert len(c2.calls) == 1 and c2.calls[0]["include_all_metadata"] is True, c2.calls

            # delayed-retry reply through cmd_reply: the TypeError fails closed (nothing posted)
            with get_db_connection(db_path) as conn:
                conn.execute(
                    "INSERT INTO events (event_id, channel_id, user_id, text, ts, thread_ts, status, "
                    "first_unverifiable_at) VALUES ('Ev_R28_DELAYED', ?, 'U_Q', '質問', '1795450002.000300', ?, "
                    "'PROCESSING', ?)",
                    (channel_id, thread_ts,
                     (datetime.now(timezone.utc) - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")),
                )
                conn.commit()
            text_file.write_text("遅延再送の返信です。\n", encoding="utf-8")
            c3 = StrictOldClient()
            c3.conversations_replies = recording(c3)  # type: ignore[assignment]
            err = io.StringIO()
            with patch.object(agent_tools, "get_web_client", return_value=c3), \
                    patch.object(agent_tools.time, "sleep", return_value=None), \
                    redirect_stdout(io.StringIO()), redirect_stderr(err):
                rc = agent_tools.main(["--db", str(db_path), "reply", "--channel", channel_id, "--text-file",
                                       str(text_file), "--thread-ts", thread_ts, "--event-id", "Ev_R28_DELAYED"])
            assert rc == 1, err.getvalue()
            e = json.loads(err.getvalue())
            assert e["error"] == "pre_post_check_unverifiable", e
            assert c3.posts == [] and len(c3.calls) == 1, (c3.posts, c3.calls)
    finally:
        if saved_env is not None:
            os.environ["YAMA_OPS_ALERTS_PATH"] = saved_env

    return ("二択なし/両方/空ID/実在しないID/--force単独/空理由/--no-event-idに--forceは非0で読まず投稿せずreply_rejected・"
            "--force --reasonでだけ通過(reply_forced・台帳に記録)・実在IDはアラートなしで記録・--no-event-idは台帳に記録せず"
            "metadataはrequest_id・新kindはrejected/forcedに分類・本番SDKはinclude_all_metadataを宣言・2関数はTypeErrorで"
            "呼び直さず1回で例外・遅延再送はfail-closedを確認")


def test_audit_fix_2026_11_23() -> str:
    """86. 監査（2026-11-23 夜）の指摘の修正: (1) commit --force で FAILED→FAILED は FAILED のまま（retry_count 不変・PENDING に戻らない・requeue のブロックは引き続き効く。PENDING/PROCESSING からの FAILED は従来どおりリトライ）、(2) アーカイブに失敗した行は take が _archived:false を明示し、restore はその印を尊重して次の take でアーカイブされる（印のない行は従来どおり true を付ける）、(3) failed_with_landed_reply の event_ids が archive の keys に入る、(4) _lookup_own_recent_message_once と _fetch_history_unbounded は TypeError で呼び直さない（1回で例外）、(5) requeue 拒否時の stderr は JSON 1行"""
    import inspect
    import ops_monitor

    saved_env = {k: os.environ.pop(k, None) for k in (
        "YAMA_OPS_ALERTS_PATH", "YAMA_OPS_ALERTS_ARCHIVE_PATH",
        "YAMA_OPS_ALERTS_ARCHIVE_MAX_BYTES", "YAMA_OPS_ARCHIVE_FORCE_FAIL")}
    try:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "state" / "slack_events.db"
            init_db(db_path)
            outbox = db_path.parent / "ops_alerts.jsonl"
            th = "1795600000.000000"

            def alerts() -> List[Dict[str, Any]]:
                if not outbox.exists():
                    return []
                return [json.loads(x) for x in outbox.read_text(encoding="utf-8").splitlines() if x.strip()]

            def row(eid: str) -> Dict[str, Any]:
                with get_db_connection(db_path) as conn:
                    return dict(conn.execute(
                        "SELECT status, retry_count, reap_count, error_message, next_retry_at FROM events "
                        "WHERE event_id = ?", (eid,)).fetchone())

            with get_db_connection(db_path) as conn:
                # 監査 exp1 と同じ形: Reaper が FAILED にした（retry_count=0, reap_count=3）＋台帳に着地あり
                conn.execute(
                    "INSERT INTO events (event_id, channel_id, user_id, text, ts, thread_ts, status, retry_count, "
                    "reap_count, error_message, created_at, updated_at) VALUES ('Ev_AF_FAIL', 'C_AF', 'U_Q', 't', "
                    "'1795600001.000000', ?, 'FAILED', 0, 3, 'x', '2026-11-23 00:00:00', '2026-11-23 00:00:00')",
                    (th,),
                )
                conn.execute(
                    "INSERT INTO events (event_id, channel_id, user_id, text, ts, status, retry_count, "
                    "created_at, updated_at) VALUES ('Ev_AF_PROC', 'C_AF', 'U_Q', 't', '1795600002.000000', "
                    "'PROCESSING', 0, '2026-11-23 00:00:00', '2026-11-23 00:00:00')"
                )
                conn.commit()
            agent_tools.record_reply(event_id="Ev_AF_FAIL", channel_id="C_AF", thread_ts=th,
                                     reply_ts="1795600100.000000", text="landed", db_path=db_path)

            # (1) force FAILED->FAILED: stays FAILED, retry_count unchanged, reason kept, commit_forced
            with redirect_stderr(io.StringIO()):
                res = agent_tools.commit_event("Ev_AF_FAIL", "FAILED", error_message="audit probe",
                                               db_path=db_path, force=True, reason="理由の訂正")
            r = row("Ev_AF_FAIL")
            assert res["status"] == "FAILED" and r["status"] == "FAILED", (res, r)
            assert r["retry_count"] == 0 and r["reap_count"] == 3 and r["next_retry_at"] is None, r
            assert "[forced FAILED->FAILED] 理由の訂正" in r["error_message"], r
            a = alerts()
            assert a[-1]["kind"] == "commit_forced" and a[-1]["new_status"] == "FAILED", a
            # unverifiable error_message must not turn it into a PENDING wait either
            with redirect_stderr(io.StringIO()):
                agent_tools.commit_event("Ev_AF_FAIL", "FAILED", error_message="pre_post_check_unverifiable: x",
                                         db_path=db_path, force=True, reason="再訂正")
            r = row("Ev_AF_FAIL")
            assert r["status"] == "FAILED" and r["retry_count"] == 0 and r["next_retry_at"] is None, r
            # CLI 経由も同じ
            out, err = io.StringIO(), io.StringIO()
            with redirect_stdout(out), redirect_stderr(err):
                rc = agent_tools.main(["--db", str(db_path), "commit", "--event-id", "Ev_AF_FAIL", "--status",
                                       "FAILED", "--force", "--reason", "CLI での訂正"])
            assert rc == 0 and row("Ev_AF_FAIL")["status"] == "FAILED", (rc, out.getvalue(), err.getvalue())
            # the only way back is requeue, and the ledger still blocks it
            n_before = len(alerts())
            out, err = io.StringIO(), io.StringIO()
            with redirect_stdout(out), redirect_stderr(err):
                rc = agent_tools.main(["--db", str(db_path), "requeue", "--event-id", "Ev_AF_FAIL"])
            assert rc == 1 and row("Ev_AF_FAIL")["status"] == "FAILED"
            # (5) stderr is exactly one JSON line (no separate WARN line); the facts are in the alert
            lines = [x for x in err.getvalue().splitlines() if x.strip()]
            assert len(lines) == 1, err.getvalue()
            body = json.loads(lines[0])
            assert body["error"] == "reply_already_landed" and body["alert_written"] is True, body
            a = alerts()
            assert len(a) == n_before + 1 and a[-1]["kind"] == "requeue_blocked", a
            assert a[-1]["event_id"] == "Ev_AF_FAIL" and a[-1]["reply_ts"] == "1795600100.000000", a[-1]
            # non-terminal FAILED commits keep the retry rule (PROCESSING -> FAILED -> PENDING, retry_count 1)
            with redirect_stderr(io.StringIO()):
                agent_tools.commit_event("Ev_AF_PROC", "FAILED", error_message="agent error", db_path=db_path)
            r = row("Ev_AF_PROC")
            assert r["status"] == "PENDING" and r["retry_count"] == 1, r
            # alert write failure on a blocked requeue: still one JSON line, alert_written false
            blockfile = Path(tmp) / "blockfile"
            blockfile.write_text("not a dir", encoding="utf-8")
            with patch.object(agent_tools, "_ops_alerts_path", return_value=blockfile / "ops_alerts.jsonl"):
                out, err = io.StringIO(), io.StringIO()
                with redirect_stdout(out), redirect_stderr(err):
                    rc = agent_tools.main(["--db", str(db_path), "requeue", "--event-id", "Ev_AF_FAIL"])
            lines = [x for x in err.getvalue().splitlines() if x.strip()]
            assert rc == 1 and len(lines) == 1 and json.loads(lines[0])["alert_written"] is False, err.getvalue()

            # (2) archive failure -> restore -> next take archives the failed rows (audit exp2)
            af_dir = Path(tmp) / "arch"
            af_out = af_dir / "ops_alerts.jsonl"
            af_arch = af_dir / "ops_alerts_archive.jsonl"

            def arch_kinds() -> List[str]:
                if not af_arch.exists():
                    return []
                return [json.loads(x)["kind"] for x in af_arch.read_text(encoding="utf-8").splitlines() if x.strip()]

            ops_monitor.outbox_append({"kind": "commit_rejected", "event_id": "Ev_AF_1"}, af_out)
            env_fail = dict(os.environ, YAMA_OPS_ARCHIVE_FORCE_FAIL="1")
            cp = subprocess.run([str(PYTHON_BIN), str(BASE_DIR / "ops_monitor.py"), "outbox-take", "--path",
                                 str(af_out)], env=env_fail, capture_output=True, text=True)
            assert cp.returncode == 0 and cp.stderr == "", cp
            taken = json.loads(cp.stdout)
            assert [e["kind"] for e in taken] == ["commit_rejected", "archive_write_failed"], taken
            assert taken[0][ops_monitor.ARCHIVED_MARKER] is False, "失敗行には _archived:false を明示する"
            assert ops_monitor.ARCHIVED_MARKER not in taken[1], taken[1]
            # wake failed -> hook restores the same JSON; restore keeps the explicit false
            subprocess.run([str(PYTHON_BIN), str(BASE_DIR / "ops_monitor.py"), "outbox-restore", "--path",
                            str(af_out)], input=json.dumps(taken), capture_output=True, text=True, check=True)
            restored = [json.loads(x) for x in af_out.read_text(encoding="utf-8").splitlines() if x.strip()]
            assert restored[0][ops_monitor.ARCHIVED_MARKER] is False, restored
            assert ops_monitor.ARCHIVED_MARKER not in restored[1], restored
            # the second take fails too (two failures in a row): still not marked archived
            cp = subprocess.run([str(PYTHON_BIN), str(BASE_DIR / "ops_monitor.py"), "outbox-take", "--path",
                                 str(af_out)], env=env_fail, capture_output=True, text=True)
            taken2 = json.loads(cp.stdout)
            assert [e["kind"] for e in taken2] == ["commit_rejected", "archive_write_failed", "archive_write_failed"]
            assert taken2[0][ops_monitor.ARCHIVED_MARKER] is False and taken2[1][ops_monitor.ARCHIVED_MARKER] is False
            subprocess.run([str(PYTHON_BIN), str(BASE_DIR / "ops_monitor.py"), "outbox-restore", "--path",
                            str(af_out)], input=json.dumps(taken2), capture_output=True, text=True, check=True)
            # the next (successful) take archives every row once, and delivers them without the marker
            ok_take = ops_monitor.outbox_take(af_out)
            assert [e["kind"] for e in ok_take] == ["commit_rejected", "archive_write_failed", "archive_write_failed"]
            assert all(ops_monitor.ARCHIVED_MARKER not in e for e in ok_take), ok_take
            assert arch_kinds() == ["commit_rejected", "archive_write_failed", "archive_write_failed"], arch_kinds()
            # rows from a successful take (no marker) are still marked true by restore: no double archive
            subprocess.run([str(PYTHON_BIN), str(BASE_DIR / "ops_monitor.py"), "outbox-restore", "--path",
                            str(af_out)], input=json.dumps(ok_take[:1]), capture_output=True, text=True, check=True)
            assert ops_monitor.outbox_take(af_out)[0]["kind"] == "commit_rejected"
            assert len(arch_kinds()) == 3, arch_kinds()

            # (3) failed_with_landed_reply: keys from event_ids
            rec = ops_monitor.archive_record({"kind": "failed_with_landed_reply", "count": 2,
                                              "event_ids": ["Ev0CH5CWKHM0", "Ev_B"], "items": []})
            assert rec["key"] is None and rec["keys"] == ["Ev0CH5CWKHM0", "Ev_B"], rec
            assert rec["action"] == "alert"
            # single-event rows and commit-batch rows keep their previous shape
            assert "keys" not in ops_monitor.archive_record({"kind": "reaper", "event_id": "Ev_S"})
            assert ops_monitor.archive_record({"kind": "commit_rejected", "rejected": [{"event_id": "Ev_X"}]})["keys"] == ["Ev_X"]

            # (4) no TypeError fallback in _lookup_own_recent_message_once / _fetch_history_unbounded
            class StrictOldClient:
                def __init__(self) -> None:
                    self.replies_calls: List[Dict[str, Any]] = []
                    self.history_calls: List[Dict[str, Any]] = []

                def auth_test(self) -> Dict[str, Any]:
                    return {"ok": True, "user_id": "U_YAMA_BOT"}

                def conversations_replies(self, **kw: Any) -> Dict[str, Any]:
                    self.replies_calls.append(kw)
                    if "include_all_metadata" in kw:
                        raise TypeError("conversations_replies() got an unexpected keyword argument 'include_all_metadata'")
                    raise AssertionError("fallback call without include_all_metadata must not happen")

                def conversations_history(self, **kw: Any) -> Dict[str, Any]:
                    self.history_calls.append(kw)
                    if "include_all_metadata" in kw:
                        raise TypeError("conversations_history() got an unexpected keyword argument 'include_all_metadata'")
                    raise AssertionError("fallback call without include_all_metadata must not happen")

            for thread in (th, None):
                c = StrictOldClient()
                try:
                    agent_tools._lookup_own_recent_message_once(
                        channel="C_AF", text="x", thread_ts=thread, client=c, bot_user_id="U_YAMA_BOT",
                        sleep_fn=lambda s: None, oldest="1795600000.000000", latest="1795609999.000000",
                        event_id="Ev_AF_FAIL",
                    )
                    raise AssertionError("expected TypeError from _lookup_own_recent_message_once")
                except TypeError:
                    pass
                calls = c.replies_calls if thread else c.history_calls
                assert len(calls) == 1 and calls[0]["include_all_metadata"] is True, calls
                assert calls[0]["oldest"] == "1795600000.000000" and calls[0]["latest"] == "1795609999.000000", calls
                assert (c.history_calls if thread else c.replies_calls) == []

                c = StrictOldClient()
                try:
                    agent_tools._fetch_history_unbounded(
                        channel="C_AF", thread_ts=thread, limit=5, db_path=db_path, client=c,
                        sleep_fn=lambda s: None)
                    raise AssertionError("expected TypeError from _fetch_history_unbounded")
                except TypeError:
                    pass
                calls = c.replies_calls if thread else c.history_calls
                assert len(calls) == 1 and calls[0]["include_all_metadata"] is True, calls
            assert "except TypeError" not in inspect.getsource(agent_tools._lookup_own_recent_message_once)
            assert "except TypeError" not in inspect.getsource(agent_tools._fetch_history_unbounded)
    finally:
        for k, v in saved_env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    return ("force FAILED->FAILEDはFAILEDのまま(retry_count不変・unverifiableでもPENDINGにならない・CLIも同じ)・requeueは台帳で"
            "引き続きブロック・非終端からのFAILEDは従来どおりPENDING・requeue拒否のstderrはJSON1行(alert_written)・"
            "アーカイブ失敗行は_archived:falseでrestore後も保持され2連続失敗後の成功takeで全行1回ずつアーカイブ・"
            "failed_with_landed_replyのkeys・2関数はTypeErrorで呼び直さず1回で例外、を確認")


def main() -> int:
    tests: List[Tuple[str, Callable[[], str]]] = [
        ("1. 重複排除 (同一 event_id 2回投入で1件維持)", test_deduplication),
        ("2. 除外フィルタ (他Botはis_bot=1で保存・自ボット・編集・削除イベント除外)", test_filtering),
        ("3. ディスパッチャ遷移 (PENDING→PROCESSING & payload.json 生成)", test_dispatcher_transition_and_payload),
        ("4. コミット状態遷移 (DONE / IGNORED / FAILED リトライ3回制御)", test_commit_transitions),
        ("5. 二重起動防止 (dispatcher.sh の flock 排他ロック)", test_dispatcher_flock_exclusivity),
        ("6. Watchdog & モード確認 (3分停止検知・ログ出力・mode.txt)", test_watchdog_and_mode),
        ("7. History リトライ回復 (接続系例外からのバックオフリトライ & SlackApiError即時失敗)", test_history_retry_recovery),
        ("8. History ローカルDBフォールバック (3回失敗時のSQLite時系列代替・空メッセージ対応)", test_history_local_db_fallback),
        ("9. Backfill 取りこぼし補完 (未保存投入&is_bot・二重投入防止・自Bot/編集イベント除外)", test_backfill),
        ("10. Reply 冪等化 (同一 event_id で2回呼んでも実投稿1回・deduplicated=true 返却・後方互換)", test_reply_idempotency),
        ("11. Reply 応答ロスト回復 (投稿成功+応答例外・回復履歴1回目失敗→2回目発見で実投稿1回・3回失敗時ambiguous_post_outcome)", test_reply_lost_response_recovery),
        ("12. Reply 投稿前スレッド重複防止 (スレッドに同一文面が既にあれば投稿せずdeduplicated=true・oldest=親ts全体走査)", test_reply_pre_post_thread_dedup),
        ("13. Reply チャンネル投稿bounded重複防止 (直近50件・時間窓なしで15分超の同一文面も再投稿阻止・fail-open)", test_reply_pre_post_channel_bounded_dedup),
        ("14. Reply スレッド全体走査＆ページング (15分超の同一文面検出・2ページ目検出・20ページ上限打ち切りはfail-closedで送信せず)", test_reply_thread_fullscan_and_pagination),
        ("15. Replies 台帳90日prune (91日超削除・89日保持・NULL/解釈不能タイムスタンプ保持・--dry-run確認)", test_prune_replies),
        ("16. Watchdog ユニット復旧 (Unit not found再配置・他失敗除外・コピー元確認・上限3回＆backoff制御)", test_watchdog_unit_recovery),
        ("17. Record-reply 台帳記録＆DONE時errorクリア (record-reply冪等記録・reply連動dedup・DONE時error_message消去)", test_record_reply_and_done_error_clear),
        ("18. Watchdog 再配置ハッシュ照合 (同一内容コピースキップ・不一致コピー実行・欠落時コピー実行)", test_watchdog_unit_hash_comparison),
        ("19. Reconcile-replies 遅延着地返信の定期照合 (候補抽出・スレッド走査台帳記録・未発見スキップ・記録済み除外・--dry-run)", test_reconcile_replies),
        ("20. History リトライ強化 (最大5回試行・1→2→4→8s指数バックオフ・3回失敗後4回目成功・全経路適用)", test_history_retry_5_attempts_and_backoff),
        ("21. Reply 送信前チェック fail-closed化 (スレッド返信全試行失敗時pre_post_check_unverifiable・チャンネル投稿fail-open維持)", test_pre_post_check_fail_closed_thread_and_fail_open_channel),
        ("22. Reconcile DONE残存エラー網羅照合 (返信あり時with_reply記録＆クリア・返信なし時without_replyクリア・走査エラー時維持＆errors計上・--dry-run)", test_reconcile_done_residual_errors),
        ("23. Dispatcher PROCESSING競合防止＆Reaper (15分以内のPROCESSING存在時LOCKEDスキップ・15分超PENDING差戻し)", test_dispatcher_processing_guard_and_reaper),
        ("24. Reconcile 誤紐付け防止＆時刻順序チェック (yama_msgs[0]強制割当廃止・過去Yama発言除外・不在reply_ts確認)", test_reconcile_replies_no_false_linking_and_ts_order),
        ("25. History ローカルフォールバック自Bot返信マージ (repliesテーブルのチャンネル/スレッド返信の時系列統合)", test_history_local_fallback_includes_self_replies),
        ("26. DLQ改訂 unverifiable待機・再抽出・24時間終端 (retry_count非消費・待ち時間中NO_EVENTS・経過後DISPATCHED・24h超FAILED終端)", test_dlq_unverifiable_wait_dispatch_and_expiration),
        ("27. Reconcile unverifiable候補抽出 (pre_post_check_unverifiableのPENDING/FAILED候補抽出とreplies台帳回収)", test_reconcile_replies_picks_up_unverifiable),
        ("28. 送信前チェックAPIコール単位リトライ＆bot_user_id再利用 (複数ページ走査の途中IncompleteRead時の当該ページ再試行・auth_testキャッシュ)", test_pre_post_check_per_page_retry_and_bot_id_reuse),
        ("29. Backfill/Live双方向重複排除＆30件超スレッド返信ページング (save_event重複防止・oldest+cursorページング回収)", test_backfill_live_dedup_and_thread_pagination),
        ("30. Hook .envアトミック書き換え (同一ディレクトリ一時ファイル・chmod 600・mv置換)", test_hook_env_atomic_replace),
        ("31. エラー分類適正化＆listener auth_testリトライ (429 Retry-After/5xxリトライ・503時local_db_fallback・恒常エラー即時失敗)", test_transient_slack_api_error_retry_and_listener_auth_retry),
        ("32. バッチD検証 (commit IGNORED時errorクリア・watchdog初回60s backoff・フック自己修復ログ・busy_timeout統一・未使用定数削除)", test_batch_d_ignored_error_clear_watchdog_and_busy_timeout),
        ("33. 遅延再送の印付加 (Asia/Tokyo整形1行前置・通常返信不付加・新旧両形式の二重付加防止)", test_delayed_retry_mark_prefix),
        ("34. 遅延再送の答え済み送信抑止 (他者10文字超回答でskipped_answered・判定ts出力・DONE遷移)", test_delayed_retry_skipped_answered),
        ("35. 遅延再送の答え済み非該当ケース (ボットのみ・10文字以下短文のみ・質問者追記のみ・Yama投稿のみ)", test_delayed_retry_not_answered_conservative_cases),
        ("36. 遅延再送の履歴取得失敗時 fail-closed (既存pre_post_check_unverifiable経路維持)", test_delayed_retry_history_failure_unverifiable),
        ("37. 再送ヘッダー3者統一形＆移行期二重付加防止 (新形式生成・新旧両形式の正規表現判定)", test_delayed_retry_legacy_and_new_header_dedup),
        ("38. Ambiguous 送信文面保存 (ambiguous時のみintended_text保存・再送ヘッダー込み最終文字列・ALTER TABLE冪等移行)", test_ambiguous_saves_intended_text),
        ("39. Reconcile 文面一致＆正規化＆不一致時未記録 (遅い一致投稿の割当・エスケープ/改行/空白正規化・不一致時フォールバック廃止)", test_reconcile_intended_text_matching_and_fallback),
        ("40. Reconcile intended_textなし時の推定割当廃止 (NULL/空白時は未記録＆without_reply・記録済みts確認はwith_reply維持)", test_reconcile_no_intended_text_never_estimated),
        ("41. 対象イベント以降のスレッド重複検出＆reconcile起点 (oldest=event_ts-60s・イベント後の同一文面スキップ)", test_windowed_thread_dedup_after_event_ts),
        ("42. 過去イベント定型文の誤スキップ防止＆手動投稿全件走査維持 (イベント前の同一文面は非重複・event_idなしは全件走査)", test_windowed_thread_ignores_past_event_template),
        ("43. 安全マージン内の重複検出 (イベントtsの数秒前・60秒マージン内の自Bot投稿を検出してスキップ)", test_windowed_thread_dedup_within_margin_before_event_ts),
        ("44. 対象イベント以降の答え済み判定 (イベントtsより後の他者回答でskipped_answered・前の発言は非該当)", test_windowed_thread_answered_after_event_ts),
        ("45. 絞ったスレッド読み取り失敗時のfail-closed維持 (oldest=event_ts-60sで5回失敗時pre_post_check_unverifiable)", test_windowed_thread_failure_unverifiable),
        ("46. 投稿メタデータ付与＆include_all_metadata指定 (yama_reply+event_id/request_id・全読み取り経路)", test_reply_metadata_attachment_and_include_all_metadata),
        ("47. メタデータ一致での着地回収＆優先順位1位 (文面整形変化時の回復・reconcile・文面一致より優先)", test_recovery_and_reconcile_by_metadata_match),
        ("48. 試行時刻の窓(前3分〜後5分)による回収範囲制御 (窓外の同一metadata除外・窓境界内回収)", test_recovery_and_reconcile_attempt_window_boundaries),
        ("49. 旧投稿(metadataなし)の文面一致回収維持＆別イベントmetadata衝突防止", test_legacy_post_without_metadata_text_fallback),
        ("50. 投稿前チェックの台帳照合によるparroting防止 (同一スレッド一致抑止・別スレッド送信・正規化一致抑止・文面なし送信)", test_pre_post_thread_ledger_parroting_prevention),
        ("51. 遅延再送の答え済み確認1ページ上限＆fail-closed (ページ内回答で抑止・後続なしで送信・後続ありは未完として送信せず)", test_delayed_retry_answered_check_single_page_cap),
        ("52. 送信の再送なし確認 (TimeoutError直撃でPOST1回・回復確認→ambiguous_post_outcome＋intended_text保存／HTTP 500 SlackApiErrorでPOST1回・回復確認なし即失敗)", test_reply_post_no_retry_on_timeout_and_server_error),
        ("53. auto_join 参加判定 (オーナー在籍＆未参加で参加・1人でも参加・オーナー不在/opted_out/参加済みは不参加)", test_auto_join_should_join_decision),
        ("54. auto_join 状態遷移 (参加済み→未参加でopted_out移動・手動再参加で解除・状態ファイル原子的保存)", test_auto_join_state_transitions),
        ("55. auto_join 実行 (ドライラン副作用なし・参加＆シードPENDING保存・join失敗で継続・平穏回は無出力)", test_auto_join_run_with_mock_client),
        ("56. auto_join .bak退避＆復旧 (正常時のみ退避・破損時は.bak復旧＆本体即修復・空dict化もfallback)", test_auto_join_state_bak_rotation_and_restore),
        ("57. auto_join .bak fail-safe (本体・.bak両方破損時は空開始せず自動参加を停止・初回は空開始)", test_auto_join_state_both_corrupt_failsafe),
        ("58. auto_join .bak上書きガード (壊れた本体の保存で良い.bakを潰さない)", test_auto_join_state_bak_not_overwritten_by_corrupt_main),
        ("59. 読み取りcurlフォールバック (SDK接続エラー時のみ発動・失敗時は元の例外維持・POST非対象・SlackResponse返却)", test_curl_read_fallback),
        ("60. Watchdog 120秒しきい値＆再確認＆クールダウン (150秒停止で再起動・同一heartbeat再起動抑止・再確認中の回復で回避)", test_watchdog_threshold_recheck_cooldown),
        ("61. Watchdog 再起動前 py_compile ゲート (壊れた listener.py では再起動を見送り・直った後に再起動する)", test_watchdog_restart_py_compile_gate),
        ("62. フックの bash -n ガード (dispatcher.sh / watchdog.sh が構文エラーの回は実行を飛ばしてログに残す)", test_hook_bash_n_guards),
        ("63. Bot ユーザーIDの環境変数上書きの形状チェック (B始まり・xox始まりは例外で弾く、U/W始まりは採用、空・未設定は auth_test 経由へ)", test_resolve_bot_user_id_env_validation),
        ("64. 構文ガードの10分エスカレーション＆警告wake (jq組立てJSON検証・wake失敗時はwarned未保存＆後段続行・成功時のみwarned保存・復旧・対象独立・破損耐性)", test_hook_syntax_guard_escalation),
        ("65. auto_join statusファイル＆transient_retries (毎回上書き・吸収リトライのみ計上・最終失敗は二重計上なし・ドライラン無書込・状態エラー時error記録)", test_auto_join_last_status_and_transient_retries),
        ("66. DM代行 auto_streak・proxy_ts・排他制御 (代行3連続で1→2→3＆send拒否・本人投稿で0リセット・resolve --reply-ts・50件上限・ts文字列完全一致)", test_owner_dm_poll_proxy_ts_and_auto_streak),
        ("67. PENDING滞留チェック ops_monitor.py (人間の未回答だけ計数・自分/他Bot/既知Bot/B始まり/IGNORED/1時間待ち除外・600秒しきい値・失敗exit 2・outbox取出/復元)", test_ops_pending_stale_check_and_outbox),
        ("68. フック運用警告ブロック (PENDING滞留エッジ通知・復旧1回・outbox配送・wake失敗時ロールバック＆復元・監視失敗の即時通知・ディスパッチ回は保留・構文ガード重複警告なし)", test_hook_ops_alert_block_edge_wake),
        ("69. Reaper差し戻し上限 (reap_count後方互換追加・3回目でFAILED reaped_too_many_times・retry_count不変・実行ごと集約アラート・手動requeueのみ0リセット)", test_reaper_reap_count_cap_and_requeue),
        ("70. Backfill履歴ページング (30件x最大3ページ・打ち切り警告をstderr＋outboxにエッジ通知・解消時1回・書込失敗時は次回再試行)", test_backfill_history_paging_and_truncation_alert),
        ("71. リスナー凍結対策 (pong途絶90秒判定・sd_notify WATCHDOG=1・ユニットWatchdogSec/NotifyAccess)", test_listener_freeze_guards_and_sd_notify),
        ("72. 長文バースト対策: history の文量上限 (コンパクト表示・1投稿3,000字・合計40,000字・これ以前あり明記・--limit 50上限・--raw)", test_history_compact_view_and_text_bounds),
        ("73. 長文バースト対策: dispatcher の重さ予算 (重い回は件数を減らし残りPENDING・FIFO・先頭必取・payload本文4,000字＋event-text)", test_dispatcher_batch_weight_budget_and_payload_text_cap),
        ("74. 滞留対策: 投稿前チェック不能の待ちの段階化 (2→5→15→30→60分・retry_count非消費・24h終端不変・固定値指定の互換)", test_unverifiable_stepped_wait),
        ("75. 滞留対策: 通信の締切 (文脈用20秒でローカルへ・再試行/SDK/curlを残り時間で制限・安全用は残り時間から逆算しfail-closed)", test_read_deadlines_context_and_safety),
        ("76. 滞留対策: ミラーの穴埋め (自分の投稿/編集/削除をevents外に記録・backfillも記録・成否ファイル)", test_listener_mirror_own_posts_and_edits),
        ("77. 滞留対策: 文脈のローカル同梱 (親＋直近20件・編集/削除・直前のYama発言・欠けはcontext_ok=false)", test_context_bundle_local_thread_context),
        ("78. 滞留対策: dispatcher の文脈同梱 (内包/ファイル参照・失敗しても配信不変・無効化スイッチ)", test_dispatcher_context_bundle_payload),
        ("79. 滞留対策: commit-batch (一括commit・全件一括検証)", test_commit_batch),
        ("80. 滞留対策: seed-mirror (ミラー開始前スレッドの一度きりの補完)", test_seed_mirror),
        ("81. 滞留対策: フックの補完処理をdispatcherの後へ (バッチ周期は即wake・backfill最低5分に1回)", test_hook_supplementary_jobs_after_dispatch),
        ("82. commit 終端ガード 第1段 (同一状態no-op・別状態/FAILEDはalready_terminal拒否・--force訂正・commit-batch検証・requeue経由DONE・reply表示・reconcile判定理由保持)", test_commit_terminal_guard),
        ("83. 運用アラートの永続ログ (take時にarchiveへ追記・単一経路・rejected/forced区別・サイズ上限で1世代ローテーション・書込失敗でもtake継続＋archive_write_failed・restore行の二重記録なし)", test_ops_alert_archive_on_take),
        ("84. 候補27 代替案A (台帳に着地ありのFAILEDを検知して1行通知・状態は不変・PROCESSING等は対象外・requeueは台帳ありでブロック・--force --reasonで通過)", test_failed_with_landed_reply_detect_and_requeue_guard),
        ("85. 候補28 event-id 付け忘れ・打ち間違い防止 (reply の二択必須・実在確認と--force --reason・--no-event-idは台帳に記録しない・読み取りのinclude_all_metadataフォールバック削除)", test_reply_event_id_guard_and_metadata_read_no_fallback),
        ("86. 監査（2026-11-23 夜）の修正 (force FAILED->FAILEDはFAILEDのまま・requeue拒否のstderrはJSON1行・アーカイブ失敗行に印を付けない・failed_with_landed_replyのkeys・2関数のTypeErrorフォールバック削除)", test_audit_fix_2026_11_23),
    ]

    print("=" * 72)
    print(" Yama Slack Autonomous Assistant - Automated Test Suite")
    print("=" * 72)

    passed = 0
    failed = 0

    for name, fn in tests:
        try:
            detail = fn()
            print(f"[PASS] {name}")
            print(f"       -> {detail}")
            passed += 1
        except Exception as exc:
            print(f"[FAIL] {name}")
            print(f"       -> Error: {exc}")
            failed += 1

    print("-" * 72)
    print(f"Summary: {passed}/{len(tests)} passed, {failed} failed.")
    print("=" * 72)
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
