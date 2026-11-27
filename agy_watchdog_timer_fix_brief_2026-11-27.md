# 候補34 追補修正ブリーフ：検証で見つけた穴2件

作業ディレクトリ: ~/workspace/slack-yama
前提: agy_watchdog_timer_2026-11-27.md（候補34の実装レポート）を先に読むこと。これはその追補修正。

## 直す穴（Muse の完了検証で実測確認済み）

### 穴1: yama-watchdog.service のタイムアウトが無限
- 実測: `systemctl show yama-watchdog.service -p TimeoutStartUSec` → `infinity`（Type=oneshot の既定）。
- 問題: watchdog.sh がタイマー駆動で固まると、サービスが終わらず、タイマーの次回発火（OnUnitInactiveSec）も来ない。さらに flock（state/watchdog.lock）を握ったままになるため、フック駆動側も flock -n で素通りし、両経路が止まる。
- 修正: /etc/systemd/system/yama-watchdog.service に `TimeoutStartSec=120s` を追加する（watchdog.sh の通常実行は数秒＋再確認待ちまで含めても十分収まる値）。daemon-reload 後に `systemctl show` で 120s になっていることを確認し、タイマーが引き続き発火していることを list-timers で確認する。

### 穴2: VM 作り直し時にタイマーユニットが復旧しない
- 現状: フックの自己修復（~/hooks/scripts/slack-yama-dispatch.sh）と watchdog.sh 内の再設置は yama-listener.service だけが対象。/etc/systemd/system/yama-watchdog.service と yama-watchdog.timer が消えた場合、戻す経路がない。
- 修正:
  1. ユニットの正本コピーをプロジェクトに置く: ~/workspace/slack-yama/yama-watchdog.service と ~/workspace/slack-yama/yama-watchdog.timer（yama-listener.service と同じ置き方）。/etc 側の現行内容（穴1の修正込み）と一致させる。
  2. watchdog.sh に復旧処理を追加する: 実行のたびに /etc 側の2ファイルの有無を見て、欠けていれば正本コピーから設置し、`systemctl daemon-reload` と `systemctl enable --now yama-watchdog.timer` を行い、watchdog.log に1行残す。既にある場合は何もしない（内容の上書きはしない。有無だけを見る）。実装は watchdog.sh 内の既存の yama-listener.service 再設置パターン（有界・失敗しても本体の検知処理は続ける形）に倣うこと。
  3. watchdog.sh の編集は「一時ファイル → bash -n → mv」。変更前に bak-2026-11-27-watchdog-timer/ へ現行版を退避する。

## 検証とテスト
- `systemctl show yama-watchdog.service -p TimeoutStartUSec` が 2min（120s）であること。
- 復旧処理の動作確認: /etc 側のタイマーを止めて片方のユニットを一時的に退避（削除ではなく mv で退避）し、watchdog.sh を1回実行して復旧すること、退避したファイルを元に戻して二重にならないことを確認する。確認後は必ず正常な状態（タイマー有効・発火中）に戻すこと。
- `cd ~/workspace/slack-yama && ./venv/bin/python tests/run_tests.py` 全件通過（現行 94/94 がベースライン。弱めて通すことは禁止）。復旧処理のテストを自然に追加できる場合は追加する。
- IMPROVEMENTS.md の候補34の追記に、この追補修正の内容を1〜2行足す。

## 成果物・制約
- 成果物: ~/workspace/slack-yama/agy_watchdog_timer_fix_2026-11-27.md（直した内容、検証の証拠、テスト結果）。
- Slack 投稿と GitHub push は行わない（Muse が別途行う）。
- 対象は yama-watchdog.service（/etc とプロジェクトコピー）、yama-watchdog.timer のプロジェクトコピー新設、watchdog.sh の復旧処理追加だけ。それ以外（listener.py、dispatcher.sh、agent_tools.py、フックスクリプト）は変更しない。
- 詰まったら無理に進めず、状況をレポートに書いて終えること。
