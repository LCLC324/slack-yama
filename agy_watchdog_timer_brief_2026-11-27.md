# 候補34 実装ブリーフ：watchdog のフックからの独立（systemd タイマー駆動）

作業ディレクトリ: ~/workspace/slack-yama
前提: IMPROVEMENTS.md の候補34（提案済み、agyレビュー済み）を読んでから着手すること。判断は「実施」で確定済み。

## 背景（実測済みの事実）

- watchdog.sh は現在、30秒ごとのフック（slack-yama-dispatch）内でのみ実行される。フックが止まるとリスナー停止の検知・再起動も止まる（2026-11-26 実測：フック間隔 最大57分、リスナー再起動3回ともフック復帰直後に発火）。
- 先行例 home-assistant-ensure.timer は /etc/systemd/system/ のシステムタイマーで、WantedBy=default.target、OnBootSec=30s、OnUnitInactiveSec=1min、AccuracySec=10s。実際に毎分発火している（systemctl list-timers で確認済み）。
- yama-listener.service も /etc/systemd/system/ のシステムサービス。
- systemctl --user はこの環境ではバスに繋がらない。user タイマーは使わず、システムタイマー形で作ること。

## やること

1. **ユニット新設**（/etc/systemd/system/ に配置）:
   - `yama-watchdog.service`: oneshot で /home/hatch/workspace/slack-yama/watchdog.sh を実行する。home-assistant-ensure.service の形を参考にする（先に `systemctl cat home-assistant-ensure.service` で中身を確認すること）。
   - `yama-watchdog.timer`: OnBootSec=30s、OnUnitInactiveSec=1min、AccuracySec=10s、WantedBy=default.target（home-assistant-ensure.timer と同形）。
   - `systemctl daemon-reload` のうえで `systemctl enable --now yama-watchdog.timer`。
2. **watchdog.sh の同時実行対策**: フック駆動（30秒）とタイマー駆動（60秒）が重なっても安全なようにする。既に排他機構があるか先に確認し、無ければ flock による排他を入れる（例：スクリプト冒頭でロックファイルへ `flock -n` を取り、取れなければ何もせず exit 0）。watchdog.sh の編集は必ず「一時ファイル → bash -n → mv」の手順で行うこと（実行中スクリプトの直接編集は禁止。ファイル冒頭の注意書き参照）。
3. **フック側の呼び出しは残す**（試行期間）。~/hooks/scripts/slack-yama-dispatch.sh は変更しない。
4. **検証**（証拠をレポートに残すこと）:
   - `systemctl list-timers --all` で yama-watchdog.timer の NEXT/LAST が進むこと。
   - 約2分待って、タイマー由来で yama-watchdog.service が複数回起動したことを `journalctl -u yama-watchdog.service` で確認する。
   - その間、リスナー（yama-listener.service）が健全なままであること、watchdog.log に二重再起動や異常が出ていないこと。
   - フック駆動とタイマー駆動が重なる時間帯でも、再起動が二重に走らないこと（flock の効果をログで確認）。
5. **テスト**: `cd ~/workspace/slack-yama && ./venv/bin/python tests/run_tests.py` を実行し、全件通過を確認する（現行ベースラインは 93/93。弱めて通すことは禁止）。flock 等の新規挙動に対応するテストを自然に追加できる場合は追加してよい。
6. **IMPROVEMENTS.md 更新**: 候補34の状態を「対応済み（2026-11-27、タイマー独立を実装）」に更新し、実装内容を1〜2行追記する。
7. **成果物**: ~/workspace/slack-yama/agy_watchdog_timer_2026-11-27.md にレポートを書く（作ったユニットの全文、検証の証拠＝list-timers と journalctl の抜粋、テスト結果、残した課題）。

## 制約

- 変更前のファイルは ~/workspace/slack-yama/bak-2026-11-27-watchdog-timer/ に退避してから編集する。
- Slack への投稿と GitHub push は行わない（Muse が別途行う）。
- listener.py、dispatcher.sh、agent_tools.py の挙動は変更しない。対象は watchdog.sh の排他追加と systemd ユニットの新設だけ。
- ホスト凍結時はタイマーも止まる、という留保は前提として受け入れる（対策範囲外）。
- 詰まったら無理に進めず、状況をレポートに書いて終えること。
