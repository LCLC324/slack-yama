# 候補34 実装レポート：watchdog のフックからの独立（systemd タイマー駆動）

- 実施日: 2026-11-27
- 作業ディレクトリ: `/home/hatch/workspace/slack-yama`
- 退避ディレクトリ: `/home/hatch/workspace/slack-yama/bak-2026-11-27-watchdog-timer/`（`watchdog.sh`, `IMPROVEMENTS.md`, `run_tests.py` を変更前に退避）

## 1. 実装内容

### 1.1 systemd ユニット新設（`/etc/systemd/system/`）

先行例 `home-assistant-ensure.service` / `home-assistant-ensure.timer` を参照し、以下の2ユニットを配置（パーミッション `0644`）したうえで `systemctl daemon-reload` および `systemctl enable --now yama-watchdog.timer` を実行した。

#### `/etc/systemd/system/yama-watchdog.service`
```ini
[Unit]
Description=Yama Slack Listener Watchdog

[Service]
Type=oneshot
WorkingDirectory=/home/hatch/workspace/slack-yama
ExecStart=/home/hatch/workspace/slack-yama/watchdog.sh
StandardOutput=null
StandardError=journal
```

#### `/etc/systemd/system/yama-watchdog.timer`
```ini
[Unit]
Description=Run Yama Slack Listener Watchdog every minute

[Timer]
OnBootSec=30s
OnUnitInactiveSec=1min
AccuracySec=10s

[Install]
WantedBy=default.target
```

`systemctl enable --now yama-watchdog.timer` により `/etc/systemd/system/default.target.wants/yama-watchdog.timer` のシンボリックリンクが作成され、即座に有効化・稼働した。

### 1.2 `watchdog.sh` の同時実行対策（`flock` 排他）

- 変更前の `watchdog.sh` に排他機構が無かったため、冒頭（`mkdir -p "${STATE_DIR}"` 直後）に FD 9 を使った非ブロッキング排他ロックを追加した。
  - ロックファイル: `LOCK_PATH="${YAMA_WATCHDOG_LOCK_PATH:-${STATE_DIR}/watchdog.lock}"`
  - `exec 9>"${LOCK_PATH}"` / `if ! flock -n 9; then exit 0; fi`
  - ロック取得不可時はログを出さず `exit 0` で静かに終了する。
- 編集はファイル冒頭の注意書きに従い、同一ディレクトリの一時ファイル（`watchdog.sh.tmp`）作成 → `bash -n` 構文検証 → パーミッション維持（`chmod --reference`）→ `mv -f` による原子置換で実施した。
- 試行期間のため、フック側（`~/hooks/scripts/slack-yama-dispatch.sh`）からの `watchdog.sh` 呼び出しは変更せず維持した。
- `listener.py`、`dispatcher.sh`、`agent_tools.py` は変更していない。

## 2. 検証結果と証拠

### 2.1 `systemctl list-timers --all`（NEXT / LAST の進行）

タイマー有効化（14:16:07 JST）から約2分45秒後の 14:18:52 JST 時点で、`yama-watchdog.timer` の `LAST` / `NEXT` が `home-assistant-ensure.timer` と同様に毎分進んでいることを確認した。

```text
NEXT                        LEFT LAST                         PASSED UNIT                         ACTIVATES
Fri 2026-11-27 14:19:14 JST  22s Fri 2026-11-27 14:18:14 JST 38s ago yama-watchdog.timer          yama-watchdog.service
Fri 2026-11-27 14:19:14 JST  22s Fri 2026-11-27 14:18:14 JST 38s ago home-assistant-ensure.timer  home-assistant-ensure.service
```

### 2.2 `journalctl -u yama-watchdog.service`（複数回のタイマー起動）

2分強の観測で、タイマー由来で `yama-watchdog.service` が3回連続（14:16:07、14:17:08、14:18:14 JST）正常起動・正常終了していることを確認した。

```text
Nov 27 14:16:07 htch-runtime systemd[1]: Starting yama-watchdog.service - Yama Slack Listener Watchdog...
Nov 27 14:16:07 htch-runtime systemd[1]: yama-watchdog.service: Deactivated successfully.
Nov 27 14:16:07 htch-runtime systemd[1]: Finished yama-watchdog.service - Yama Slack Listener Watchdog.
Nov 27 14:17:08 htch-runtime systemd[1]: Starting yama-watchdog.service - Yama Slack Listener Watchdog...
Nov 27 14:17:09 htch-runtime systemd[1]: yama-watchdog.service: Deactivated successfully.
Nov 27 14:17:09 htch-runtime systemd[1]: Finished yama-watchdog.service - Yama Slack Listener Watchdog.
Nov 27 14:18:14 htch-runtime systemd[1]: Starting yama-watchdog.service - Yama Slack Listener Watchdog...
Nov 27 14:18:14 htch-runtime systemd[1]: yama-watchdog.service: Deactivated successfully.
Nov 27 14:18:14 htch-runtime systemd[1]: Finished yama-watchdog.service - Yama Slack Listener Watchdog.
```

### 2.3 リスナー健全性・`watchdog.log`・フックとの並走確認

- `yama-listener.service`: 14:13:16 JST 起動の同一プロセス（Main PID `1225`）のまま `active (running)` を維持。`state/heartbeat.json` も最新（`2026-11-27T05:18:48Z`, `connected: true`, `last_pong_age_sec: 0`）。
- `state/watchdog.log`: 14:13:21 JST（`05:13:21Z`）の起動時記録以降、二重再起動・エラー等の異常ログは一切発生していない。
- `state/watchdog.lock`: 作成・更新を確認（`-rw-r--r-- 1 root nogroup 0 Nov 27 14:18 state/watchdog.lock`）。
- フックとの並走時間帯（14:16〜14:18 JST）の実行記録:
  - ディスパッチフック（`~/hooks/logs/slack-yama-dispatch.jsonl`）: `14:16:12`, `14:16:42`, `14:17:12`, `14:17:42`, `14:18:12`（所要約2.9秒）, `14:18:42` JST に正常実行。
  - タイマー（`14:18:14 JST`）とフック（`14:18:12〜14:18:15 JST`）の近接・重複時間帯でも異常・二重再起動なし。
- なお、`flock` による再起動の二重抑止そのものは、テスト94でロック保持中の無言 `exit 0` および stale heartbeat＋`RECHECK_SEC=2` での2プロセス同時起動（`RESTART_CMD` 実行回数＝1回のみ）として決定論的に検証した。

## 3. テスト結果

`tests/run_tests.py` にテスト94（`test_watchdog_flock_exclusivity`）を追加し、フルスイートを実行した。

- 実行コマンド: `cd /home/hatch/workspace/slack-yama && ./venv/bin/python tests/run_tests.py`
- 結果: **94/94 passed, 0 failed.**（既存93件すべて回帰なし＋新規1件通過）

```text
[PASS] 94. 候補34 Watchdog flock 排他制御 (ロック保持中は無言exit 0・同時実行でも再起動は1回だけ)
       -> ロック保持中のwatchdog起動は無言exit 0＆再起動なし、RECHECK中の同時起動2本でも再起動実行は1回のみを確認
------------------------------------------------------------------------
Summary: 94/94 passed, 0 failed.
========================================================================
```

## 4. ホスト再起動後の再確認と重要な発見（14:29〜14:37 JST）

14:29 JST 頃のホスト再起動（`boot_id` 変化: `9a834af1` → `13975121`）後に状態を再点検したところ、以下が実測された：

- **ワークスペース側（永続）**: `watchdog.sh`、`tests/run_tests.py`、`IMPROVEMENTS.md`、`agy_watchdog_timer_2026-11-27.md`、`bak-2026-11-27-watchdog-timer/` はすべて残存。
- **`/etc/systemd/system/` 側（再起動で揮発）**: `yama-watchdog.service`・`yama-watchdog.timer` および `default.target.wants` リンクは消失していた。一方、`yama-listener.service` はフック側の自己修復（`~/workspace/slack-yama/yama-listener.service` からの再配置）により 14:29 JST に自動復帰していた。

これを受け、`/etc/systemd/system/yama-watchdog.service` と `yama-watchdog.timer` を再配置し、`systemctl daemon-reload && systemctl enable --now yama-watchdog.timer` を再実行して約2分間の再検証を実施した。

### 4.1 再起動後の `systemctl list-timers --all`（14:37:30 JST）
```text
NEXT                        LEFT LAST                         PASSED UNIT                         ACTIVATES
Fri 2026-11-27 14:37:43 JST  12s Fri 2026-11-27 14:36:43 JST 48s ago home-assistant-ensure.timer  home-assistant-ensure.service
Fri 2026-11-27 14:38:20 JST  49s Fri 2026-11-27 14:37:20 JST 10s ago yama-watchdog.timer          yama-watchdog.service
```

### 4.2 再起動後の `journalctl -u yama-watchdog.service`
```text
Nov 27 14:35:12 htch-runtime systemd[1]: Starting yama-watchdog.service - Yama Slack Listener Watchdog...
Nov 27 14:35:12 htch-runtime systemd[1]: yama-watchdog.service: Deactivated successfully.
Nov 27 14:35:12 htch-runtime systemd[1]: Finished yama-watchdog.service - Yama Slack Listener Watchdog.
Nov 27 14:36:20 htch-runtime systemd[1]: Starting yama-watchdog.service - Yama Slack Listener Watchdog...
Nov 27 14:36:20 htch-runtime systemd[1]: yama-watchdog.service: Deactivated successfully.
Nov 27 14:36:20 htch-runtime systemd[1]: Finished yama-watchdog.service - Yama Slack Listener Watchdog.
Nov 27 14:37:20 htch-runtime systemd[1]: Starting yama-watchdog.service - Yama Slack Listener Watchdog...
Nov 27 14:37:20 htch-runtime systemd[1]: yama-watchdog.service: Deactivated successfully.
Nov 27 14:37:20 htch-runtime systemd[1]: Finished yama-watchdog.service - Yama Slack Listener Watchdog.
```

- その間、`yama-listener.service` は 14:29:15 JST 起動の同一 PID（`2090`）で `active (running)` を維持、`state/heartbeat.json` 健全（`connected: true`, `last_pong_age_sec: 5`）、`state/watchdog.log` に異常・二重再起動なし、ディスパッチフック（`14:34:33`〜`14:37:03 JST`）も正常並走を確認した。

## 5. 残した課題・今後の検討事項

1. **【重要・実測済み】ホスト再起動時の `/etc/systemd/system/` 揮発に対する自己修復**: 本日のホスト再起動（14:29 JST）で `/etc/systemd/system/` に配置した `yama-watchdog.service` / `yama-watchdog.timer` が消失することが実測された（`yama-listener.service` はワークスペース正本からの自己修復で復帰）。本ブリーフの制約上 `~/hooks/scripts/slack-yama-dispatch.sh` は変更不可のため未対応だが、次の再起動でもタイマーを維持するには、`yama-watchdog.service` / `yama-watchdog.timer` の正本をワークスペース内に置き、フック（または `watchdog.sh`）から未配置時に `install` + `daemon-reload` + `enable --now` で自己修復する仕組みを追加する必要がある。
2. **フック側呼び出しの整理（試行期間後）**: 現在は試行期間として `~/hooks/scripts/slack-yama-dispatch.sh` からの `watchdog.sh` 呼び出しを残している。上記1の自己修復を入れたうえで、タイマーの安定稼働を確認してからフック側の呼び出しを外すか（または二重化を続けるか）を判断する。
3. **ホスト凍結時の制約（前提どおり対象外）**: ホスト自体が凍結（サスペンド）している間は systemd タイマーも停止する。本対策の適用範囲は「フック単体が停止した場合のフェイルセーフ」である。
