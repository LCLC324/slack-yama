# 実装継続依頼：Slack自律応答アシスタント「Yama」（前回の続き）

## 状況
前回の実装作業が途中で中断された。仕様は同フォルダの `agy_impl_prompt.md`、設計は `agy_design_output.md`。両方を読んでから始めること。

## 既に出来ているもの（検証済み）
- `listener.py`（構文OK）、`agent_tools.py`（構文OK）、`dispatcher.sh`（構文OK）
- `config/.env.example`、`config/mode.txt`（shadow）
- `venv/`（slack-sdk インポート可）

まず既存の3ファイルを仕様と照らして中身を確認し、不足や不整合があれば直すこと。問題なければそのまま使う。

## 残りの実装（今回の作業）
1. **`watchdog.sh`**：`state/heartbeat.json` が3分以上更新されていなければ `systemctl restart yama-listener` を試み、結果を `state/watchdog.log` に残す
2. **`yama-listener.service`**：systemdユニット（Restart=always、RestartSec=10、WorkingDirectory=~/workspace/slack-yama、ExecStart は venv の python で listener.py、EnvironmentFile=config/.env）。ファイルを作るだけでインストール・有効化はしない
3. **`tests/run_tests.py`**：模擬イベントで次を自動検証し、結果を標準出力にまとめる（実Slackには接続しない。DBとフィルタ関数を直接叩く）
   - 同じ event_id を2回入れても1件になる（重複排除）
   - Bot発言・自ボット発言・編集イベントが保存されない（フィルタ）
   - dispatcher が PENDING→PROCESSING にし、payload に内容が入る
   - commit DONE / IGNORED / FAILED（リトライ3回未満は PENDING 戻し、以上で FAILED 確定）が正しく遷移する
   - dispatcher の二重起動が flock で防がれる
4. **`README.md`**：構成、ファイル一覧、セットアップ手順（人間の作業：Slackアプリ作成→Socket Mode有効化→xapp発行→スコープ付与（channels:history, channels:read, channels:join, groups:history, groups:read, im:history, im:read, mpim:history, mpim:read, chat:write, users:read, reactions:write）→イベント購読 message.channels/groups/im/mpim→インストール→xoxb取得→config/.env に記入→ユニット有効化）、モード切替方法（config/mode.txt：shadow/mention/full）、運用手順

## 制約（前回と同じ）
- トークンの実値を書かない。`.env` 本体は作らない
- systemd の有効化・起動、cron登録、フック登録はしない
- **テストを実行し、失敗したら直して再実行し、全件パスさせるところまでやること**
- 完了したら、作ったファイルの一覧とテスト結果の要約を最終出力に書く
