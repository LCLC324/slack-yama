# 実装依頼：Slack自律応答アシスタント「Yama」

## 目的
Slackワークスペース「muse取締役会」で、AIアシスタント「Yama」が新着投稿を自動で読んで自律返信する仕組みを実装する。設計は同フォルダの `agy_design_output.md`（君が作った設計案）に従うこと。まずそれを読んでから実装に入ること。

## 作業場所
`~/workspace/slack-yama/` 配下に全て作る。Python の venv を `~/workspace/slack-yama/venv/` に作り、`slack-sdk` を入れる。

## 実装するもの
1. **`listener.py`**（常駐リスナー）
   - slack-sdk の Socket Mode クライアントで接続。トークンは `config/.env` から読む（`SLACK_APP_TOKEN`=xapp、`SLACK_BOT_TOKEN`=xoxb）。ハードコード禁止
   - イベント受信直後に ack を返す
   - 除外フィルタ：自分のボットの発言、他Botの発言（bot_id あり／subtype=bot_message）、編集・削除イベント（message_changed / message_deleted）、参加通知などのシステムイベント
   - 残ったイベントを SQLite（`state/slack_events.db`、WALモード、スキーマは設計案どおり）に status=PENDING で保存。event_id の PRIMARY KEY で重複を弾く
   - 30秒ごとに `state/heartbeat.json` を更新（接続状態を含む）
   - WebSocket切断が60秒続いたら exit(1) して systemd に再起動させる
   - 起動時に BOT_USER_ID を auth.test で取得して自分の発言判定に使う
2. **`dispatcher.sh`**（フック用ディスパッチャ）
   - flock で多重起動を防止（先行処理中なら何もせず終了）
   - PENDING イベントを古い順に最大10件取得し、内容を JSON ペイロードファイル（`state/payload.json`）に書き出して、対象を PROCESSING に更新する
   - PENDING が無ければ何もしない（空振りでエージェントを起こさないための判定に使えるよう、終了コードと標準出力で有無を分かるようにする）
3. **`agent_tools.py`**（起きたエージェントが使う道具。CLIサブコマンド方式）
   - `pending`：PROCESSING 中のイベント一覧を JSON で出す
   - `history --channel X [--thread-ts Y] [--limit N]`：ボットトークンで会話履歴を読む
   - `reply --channel X --text-file F [--thread-ts Y]`：ボットとして投稿する（テキストはファイル経由で渡す。長い文や引用符での事故防止）
   - `react --channel X --ts T --emoji eyes`：リアクションを付ける
   - `commit --event-id E --status DONE|IGNORED|FAILED [--error MSG]`：状態を確定する。FAILED は retry_count を増やし、3回未満なら PENDING に戻す、3回以上で FAILED 確定にする
   - `mode`：現在の動作モード（`config/mode.txt`：shadow / mention / full）を出す
4. **`watchdog.sh`**：heartbeat が3分以上止まっていたら `systemctl restart yama-listener` を試み、結果を `state/watchdog.log` に残す
5. **`yama-listener.service`**：systemd ユニットファイル（Restart=always、RestartSec=10、WorkingDirectory と venv の python を使う ExecStart、EnvironmentFile=config/.env）。ファイルを作るだけで、**インストール・有効化はしない**
6. **`config/.env.example`** と **`config/mode.txt`**（初期値 shadow）
7. **`tests/run_tests.py`**：模擬イベントで次を自動検証し、結果を標準出力にまとめる
   - 同じ event_id を2回入れても1件になる（重複排除）
   - Bot発言・自ボット発言・編集イベントが保存されない（フィルタ）
   - dispatcher が PENDING→PROCESSING にし、payload に内容が入る
   - commit DONE / IGNORED / FAILED（リトライ後の PENDING 戻しを含む）が正しく遷移する
   - dispatcher の二重起動が flock で防がれる
   - テストは実 Slack に接続しないこと（DB とフィルタ関数を直接叩く）
8. **`README.md`**：構成図、ファイル一覧、セットアップ手順（人間がやる Slack アプリ作成の手順も含む：アプリ作成→Socket Mode有効化→xapp発行→スコープ付与→イベント購読 message.channels/groups/im/mpim→インストール→xoxb取得→config/.env に記入→ユニット有効化）、モード切替方法、運用手順

## 推奨スコープ（READMEに記載）
channels:history, channels:read, channels:join, groups:history, groups:read, im:history, im:read, mpim:history, mpim:read, chat:write, users:read, reactions:write

## 制約
- トークンの実値をコードやREADMEに書かない。`.env` 本体は作らない（`.env.example` のみ）
- systemd の有効化・起動、cron 登録、フック登録はしない（それは統合側の作業）
- テストを実行して結果を確認するところまでやる。失敗したら直して再実行する
- 完了したら、作ったファイルの一覧とテスト結果の要約を最終出力に書く
