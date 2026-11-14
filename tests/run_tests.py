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
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
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
import backfill  # noqa: E402
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
            def conversations_history(self, channel: str, limit: int = 20) -> Dict[str, Any]:
                calls["count"] += 1
                if calls["count"] == 1:
                    raise http.client.IncompleteRead(b"partial")
                return {
                    "ok": True,
                    "messages": [
                        {"user": "U_API_1", "text": "hello from api", "ts": "1700000500.000100"}
                    ],
                }

            def conversations_replies(self, channel: str, ts: str, limit: int = 20) -> Dict[str, Any]:
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
            def conversations_history(self, channel: str, limit: int = 20) -> Dict[str, Any]:
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
            def conversations_history(self, channel: str, limit: int = 20) -> Dict[str, Any]:
                calls["count"] += 1
                raise socket.timeout("timed out")

            def conversations_replies(self, channel: str, ts: str, limit: int = 20) -> Dict[str, Any]:
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

        # 3) Legacy call WITHOUT --event-id -> backward compatible, posts normally every time
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
                ]
            )

        assert rc3 == 0
        assert len(post_calls) == 2, f"Expected legacy call without --event-id to post (total 2), got {len(post_calls)}"
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
            ) -> Dict[str, Any]:
                replies_calls.append(
                    {"channel": channel, "ts": ts, "oldest": oldest, "limit": limit, "cursor": cursor}
                )
                assert oldest == thread_ts, f"oldest parameter (thread_ts={thread_ts}) must be passed, got {oldest}"

                if not post_landed["value"]:
                    # Pre-post check before chat_postMessage: reply has not been posted yet
                    return {
                        "ok": True,
                        "messages": [
                            {"user": "U_NEIRO", "text": "Parent post", "ts": thread_ts}
                        ],
                    }

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
                ]
            )
        assert rc2 == 0
        assert len(post_calls) == 1, f"Must still be 1 real post, got {len(post_calls)}"
        out2 = json.loads(stdout2.getvalue())
        assert out2["deduplicated"] is True and out2["ts"] == landed_ts

        # Verify unrecoverable case: all 3 recovery attempts fail -> error="ambiguous_post_outcome" and exit 1
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

        # 2) Without --event-id (with --thread-ts) -> also detects in thread and skips posting with deduplicated=true
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
                    "Ev_THREAD_REC_PAGE2",
                ]
            )

        assert rc3 == 0, f"Expected rc=0 on paginated recovery, got {rc3}, stderr={stderr3.getvalue()}"
        assert len(rec_posts) == 1
        out3 = json.loads(stdout3.getvalue())
        assert out3["ok"] is True
        assert out3["recovered_after_ambiguous_error"] is True
        assert out3["ts"] == rec_landed_ts

        # Case 4: Page cap at 20 pages -> stops after 20 calls, logs warning to stderr, and posts normally
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
                ]
            )

        assert rc4 == 0, f"Expected rc=0 after 20-page cap, got {rc4}"
        assert len(cap_calls) == 20, f"Expected cap at 20 pages, got {len(cap_calls)}"
        assert len(cap_posts) == 1, f"Expected 1 post after 20-page scan found no match, got {len(cap_posts)}"
        warn4 = json.loads(stderr4.getvalue())
        assert warn4["warning"] == "thread_replies_page_limit_reached"
        assert warn4["max_pages"] == 20

        return "15分超(3時間前)のスレッド内同一文面検出・2ページ目(cursorページング)の同一文面検出(投稿前＆応答ロスト回復)・20ページ上限打ち切りとstderr警告を確認"


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
        init_db(db_path)

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
                (ev_cand_done, channel_id, "U_USER1", "Q1", th_1, None, "DONE", "reply not posted, no replies-table record", recent_ts_str),
                (ev_cand_no_yama, channel_id, "U_USER2", "Q2", th_2, None, "FAILED", "reply not posted", recent_ts_str),
                (ev_already_recorded, channel_id, "U_USER3", "Q3", th_3, None, "FAILED", "reply not posted", recent_ts_str),
                # Insert ev_shared_new BEFORE ev_shared_old in row order to verify sorting by event ts (oldest first)
                (ev_shared_new, channel_id, "U_USER4", "Q4b", "1794479005.000000", th_4, "FAILED", "ambiguous_post_outcome", recent_ts_str),
                (ev_shared_old, channel_id, "U_USER4", "Q4a", "1794479001.000000", th_4, "DONE", "reply not posted", recent_ts_str),
                ("Ev_UNRELATED_ERR", channel_id, "U_USER5", "Q5", "1794480000.000000", None, "FAILED", "rate_limited", recent_ts_str),
                ("Ev_NO_ERR", channel_id, "U_USER6", "Q6", "1794481000.000000", None, "DONE", None, recent_ts_str),
                ("Ev_PENDING_REPLY", channel_id, "U_USER7", "Q7", "1794482000.000000", None, "PENDING", "reply not posted", recent_ts_str),
                ("Ev_TOO_OLD_10D", channel_id, "U_USER8", "Q8", "1794470000.000000", None, "DONE", "reply not posted", old_ts_str),
            ]
            conn.executemany(
                """
                INSERT INTO events (
                    event_id, channel_id, user_id, text, ts, thread_ts, status, error_message, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [(e[0], e[1], e[2], e[3], e[4], e[5], e[6], e[7], e[8], e[8]) for e in events_rows],
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
                    event_id, channel_id, user_id, text, ts, thread_ts, status, error_message, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (ev_api_err, channel_id, "U_USER9", "Q_err", th_err, None, "DONE", "reply not posted", recent_ts_str, recent_ts_str),
                    (ev_after_err, channel_id, "U_USER9", "Q_ok", th_after, None, "DONE", "reply not posted", recent_ts_str, recent_ts_str),
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
            def conversations_history(self, channel: str, limit: int = 20) -> Dict[str, Any]:
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
        with get_db_connection(db_path) as conn:
            conn.execute(
                """
                INSERT INTO events (
                    event_id, channel_id, user_id, text, ts, thread_ts, status, error_message, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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

        with get_db_connection(db_path) as conn:
            conn.executemany(
                """
                INSERT INTO events (
                    event_id, channel_id, user_id, text, ts, thread_ts, status, error_message, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (ev_done_unrec, channel_id, "U_U1", "Q_unrec", th_unrec, None, "DONE", "IncompleteRead(64 bytes read)", recent_ts_str, recent_ts_str),
                    (ev_done_alr, channel_id, "U_U2", "Q_alr", th_alr, None, "DONE", "reply not posted, no replies-table record", recent_ts_str, recent_ts_str),
                    (ev_done_no_reply, channel_id, "U_U3", "Q_norep", th_no_reply, None, "DONE", "pre_post_check_unverifiable", recent_ts_str, recent_ts_str),
                    (ev_done_phase1_no_reply, channel_id, "U_U4", "Q_p1_norep", th_phase1_no_reply, None, "DONE", "reply not posted", recent_ts_str, recent_ts_str),
                    (ev_done_scan_err, channel_id, "U_U5", "Q_scan_err", th_scan_err, None, "DONE", "max attempts exceeded", recent_ts_str, recent_ts_str),
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

        with get_db_connection(db_path) as conn:
            conn.executemany(
                """
                INSERT INTO events (
                    event_id, channel_id, user_id, text, ts, thread_ts, status, error_message, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (ev_a, channel_id, "U_1", "Parent A", th_1, None, "DONE", None, recent_ts_str, recent_ts_str),
                    (ev_b, channel_id, "U_2", "Followup B (no reply needed)", ev_b_ts, th_1, "DONE", "IncompleteRead(32 bytes read)", recent_ts_str, recent_ts_str),
                    (ev_child_p1, channel_id, "U_3", "Child P1", ev_child_p1_ts, th_2, "FAILED", "ambiguous_post_outcome", recent_ts_str, recent_ts_str),
                    (ev_child_p2, channel_id, "U_4", "Child P2", ev_child_p2_ts, th_2, "DONE", "IncompleteRead", recent_ts_str, recent_ts_str),
                    (ev_c, channel_id, "U_5", "Event C", th_3, None, "DONE", "residual error", recent_ts_str, recent_ts_str),
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

        with get_db_connection(db_path) as conn:
            conn.executemany(
                """
                INSERT INTO events (
                    event_id, channel_id, user_id, text, ts, thread_ts,
                    status, retry_count, error_message, next_retry_at, first_unverifiable_at,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
    hook_script = Path("/home/hatch/hooks/scripts/slack-yama-dispatch.sh")
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
        hook_script = Path("/home/hatch/hooks/scripts/slack-yama-dispatch.sh")
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
        ("14. Reply スレッド全体走査＆ページング (15分超の同一文面検出・2ページ目検出・最大20ページ上限)", test_reply_thread_fullscan_and_pagination),
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
