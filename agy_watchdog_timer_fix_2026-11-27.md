# 候補34 追補修正レポート：検証で見つけた穴2件

- 実施日: 2026-11-27
- 実装: agy Neon（ブリーフ `agy_watchdog_timer_fix_brief_2026-11-27.md`）。実装は完了したが、レポート作成前にセッションが中断されたため、本レポートは検証担当の Muse が実測に基づいて作成した。
- 検証: Muse（TimeoutStartUSec の読み込み値、復旧処理の実地試験、テスト再実行）。

## 直した穴

### 穴1: yama-watchdog.service のタイムアウトが無限
- 実測（修正前）: `systemctl show yama-watchdog.service -p TimeoutStartUSec` → `infinity`（Type=oneshot の既定）。
- 問題: watchdog.sh がタイマー駆動で固まるとサービスが終わらず、タイマーの次回発火も来ない。flock（state/watchdog.lock）を握ったままになるためフック駆動側も素通りし、両経路が止まる。
- 修正: `/etc/systemd/system/yama-watchdog.service` に `TimeoutStartSec=120s` を追加。
- 検証: daemon-reload 後に `TimeoutStartUSec=2min` と読み込まれることを確認。タイマーは引き続き毎分発火。

### 穴2: VM 作り直し時にタイマーユニットが復旧しない
- 実例: 2026-11-27 14:29 JST のホスト再起動で /etc/systemd/system/ のユニットが揮発した（番頭の rescue が再配置し、14:35-14:37 の3回発火を再検証）。
- 修正:
  1. ユニットの正本コピーをプロジェクトに配置: `~/workspace/slack-yama/yama-watchdog.service` / `yama-watchdog.timer`（yama-listener.service と同じ置き方、/etc 側と一致）。
  2. `watchdog.sh` に復旧処理を追加: 実行のたびに /etc 側2ファイルの有無を見て、欠けていれば正本から設置し、`systemctl daemon-reload` と `systemctl enable --now yama-watchdog.timer` を実行、watchdog.log に1行記録（上限つき、失敗しても本体の検知処理は継続）。
- 検証（Muse が実地試験）: /etc 側の `yama-watchdog.timer` を退避させて watchdog.sh を1回実行 → ファイルが復旧し、タイマーが active に戻り、watchdog.log に「Watchdog timer unit(s) restored (yama-watchdog.timer; 1/3): daemon-reload and enable --now succeeded」が記録された。

## テスト
- `./venv/bin/python tests/run_tests.py` → **95/95 passed, 0 failed**（94 の flock テストに加え、復旧処理のテスト95を追加。Muse が再実行して確認）。

## 残した課題
- ホスト自体が凍結した場合はタイマーも止まる（候補34の前提どおり、対策範囲外）。
- タイマーが長期的に止まったことを検知する仕組みは未整備（初期発火と今回の復旧確認まで）。
