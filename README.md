# Slack自律応答アシスタント「Yama」

Slackワークスペース「muse取締役会」において、新着メッセージをリアルタイムに監視し、自律的にTriage（返信要否判定）・返信・リアクションを行うアシスタント「Yama」のバックエンド基盤です。

---

## 1. アーキテクチャ構成

```
[ Slack Platform (Socket Mode WebSocket / Web API) ]
       │  ▲
       │  │ (SLACK_APP_TOKEN: xapp-... / SLACK_BOT_TOKEN: xoxb-...)
       ▼  │
┌─────────────────────────────────────────────────────────────┐
│ [Layer 1] 常駐リスナー (listener.py)                         │
│  - Socket Mode で常時接続し、イベント受信直後に即時 ACK 返却  │
│  - 除外フィルタ (自ボット / 編集・削除 / 参加通知等。他Botは除外しない) │
│  - SQLite (state/slack_events.db, WAL) に PENDING で保存     │
│  - 30秒ごとに state/heartbeat.json 更新 / 60秒切断で exit(1) │
└─────────────────────────────────────────────────────────────┘
       │ (event_id PRIMARY KEY による重複排除)
       ▼
┌─────────────────────────────────────────────────────────────┐
│ [Storage] state/slack_events.db (SQLite WAL)                │
│  - Status: PENDING -> PROCESSING -> DONE / IGNORED / FAILED │
└─────────────────────────────────────────────────────────────┘
       │
       │ 定期ポーリング (flock による多重起動防止)
       ▼
┌─────────────────────────────────────────────────────────────┐
│ [Layer 2] ディスパッチャ (dispatcher.sh)                     │
│  - state/dispatcher.lock で排他制御 (先行動作中なら LOCKED)  │
│  - PENDING を古い順に最大10件取得し PROCESSING に一括更新    │
│  - state/payload.json に書き出してエージェント起動判定へ渡す │
└─────────────────────────────────────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────────────────────────────┐
│ [Layer 3] エージェント用 CLI ツール (agent_tools.py)         │
│  - pending     : PROCESSING 中のイベント一覧を JSON 出力     │
│  - history     : チャンネル / スレッドの会話履歴を取得 (最大5回) │
│  - event-text  : 切り詰められたイベントの全文を取得           │
│  - react       : メッセージに絵文字リアクション (👀 / ✅ 等) │
│  - reply       : ファイル経由で安全に返信 (回復確認最大5回)  │
│  - commit / commit-batch : DONE / IGNORED / FAILED 確定      │
│  - requeue     : FAILED イベントの手動再投入 (台帳ガード付)  │
│  - triage-log  : 判定ログを検証＋排他で JSONL 追記           │
│  - prune-state-files / prune-replies : 下書き・台帳の掃除    │
│  - seed-mirror : ローカルスレッドミラーの初期補完            │
│  - mode        : 現在の動作モード (shadow / mention / full)  │
└─────────────────────────────────────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────────────────────────────┐
│ [Layer 4] 死活監視 (yama-listener.service & watchdog.sh)    │
│  - systemd (Restart=always, RestartSec=10)                  │
│  - watchdog.sh: 30秒ごとのイベントフック内で実行、           │
│    heartbeat が120秒以上停止で systemctl restart              │
└─────────────────────────────────────────────────────────────┘
```

---

## 2. ファイル一覧

| ファイルパス | 役割 |
| :--- | :--- |
| `listener.py` | Socket Mode 常駐リスナー（ACK返却・フィルタリング・SQLite保存・Heartbeat出力） |
| `dispatcher.sh` | フック用ディスパッチャ（`flock` 排他制御・PENDING→PROCESSING遷移・`state/payload.json` 生成） |
| `agent_tools.py` | エージェント用CLIツール（`pending`, `history`, `event-text`, `reply`（送信前チェック・応答ロスト回復確認最大5回）, `record-reply`, `reconcile-replies`, `prune-replies`, `triage-log`, `prune-state-files`, `react`, `commit`, `commit-batch`, `requeue`, `seed-mirror`, `mode`） |
| `backfill.py` | リスナー停止中等の取りこぼし補完（Slack 履歴とローカル DB を突き合わせて未保存投稿を PENDING 登録） |
| `ops_monitor.py` | 運用警告ツール（人間 PENDING 滞留検知・`ops_alerts.jsonl` outbox の追記・取り出し・復元・永久アーカイブ） |
| `auto_join.py` | 公開チャンネル自動参加（オーナー参加済みチャンネルへ参加し直近投稿を初期取り込み、opt-out 管理） |
| `context_bundle.py` | ディスパッチ時のローカル文脈同梱（DB からスレッド文脈を構築し `payload.json` / `payload_context.json` へ出力） |
| `watchdog.sh` | 死活監視スクリプト（30秒ごとのイベントフック `~/hooks/scripts/slack-yama-dispatch.sh` 内で毎回実行。`state/heartbeat.json` が120秒以上未更新なら（再起動前に5秒おいて再確認し、再起動成功後は同一heartbeatへの再起動を60秒抑止） `systemctl restart yama-listener` を実行し `state/watchdog.log` に記録） |
| `yama-listener.service` | systemd ユニット定義ファイル（インストール・有効化はセットアップ手順参照） |
| `config/.env.example` | Slack トークン設定ファイルのテンプレート |
| `config/mode.txt` | 動作モード設定ファイル（初期値: `shadow`） |
| `tests/run_tests.py` | 自動検証テストスイート（重複排除・フィルタ・ディスパッチ・状態遷移・排他ロック等） |
| `venv/` | Python 仮想環境（`slack-sdk` インストール済み） |
| `state/` | 実行時データ格納ディレクトリ（`slack_events.db`, `heartbeat.json`, `payload.json`, `watchdog.log`） |

---

## 3. セットアップ手順（人間側の作業）

### Step 1: Slack アプリの作成
1. [Slack API: Applications](https://api.slack.com/apps) にアクセスし、**Create New App** → **From scratch** を選択します。
2. App Name に `Yama`、対象ワークスペースに `muse取締役会` を選択して作成します。

### Step 2: Socket Mode の有効化と App-Level Token (`xapp-`) の発行
1. 左メニューの **Socket Mode** を開き、**Enable Socket Mode** を ON にします。
2. トークン名（例: `yama-socket-token`）を入力し、`connections:write` スコープを付与して **Generate** をクリックします。
3. 発行された `xapp-1-...` トークンを控えておきます（`SLACK_APP_TOKEN` に使用）。

### Step 3: Bot Token Scopes（推奨スコープ）の付与
左メニューの **OAuth & Permissions** → **Scopes** → **Bot Token Scopes** に以下の **12個のスコープ** をすべて追加します。

- `channels:history` （パブリックチャンネルの履歴取得）
- `channels:read` （パブリックチャンネル一覧・情報取得）
- `channels:join` （パブリックチャンネルへの参加）
- `groups:history` （プライベートチャンネルの履歴取得）
- `groups:read` （プライベートチャンネル情報取得）
- `im:history` （DMの履歴取得）
- `im:read` （DM情報取得）
- `mpim:history` （グループDMの履歴取得）
- `mpim:read` （グループDM情報取得）
- `chat:write` （メッセージ投稿・返信）
- `users:read` （ユーザー名・プロファイル解決）
- `reactions:write` （絵文字リアクション付与）

### Step 4: Event Subscriptions（イベント購読）の設定
1. 左メニューの **Event Subscriptions** を開き、**Enable Events** を ON にします。
2. **Subscribe to bot events** を展開し、以下の **4つのメッセージイベント** を追加して **Save Changes** をクリックします。
   - `message.channels`
   - `message.groups`
   - `message.im`
   - `message.mpim`

### Step 5: ワークスペースへのインストールと Bot Token (`xoxb-`) の取得
1. 左メニューの **Install App** を開き、**Install to Workspace**（または Reinstall）を実行して許可します。
2. 発行された **Bot User OAuth Token**（`xoxb-...`）を控えておきます（`SLACK_BOT_TOKEN` に使用）。
3. Slack 上で Yama を監視対象のチャンネルに招待（`/invite @Yama`）します。

### Step 6: `config/.env` の作成とトークン記入
テンプレートをコピーして `config/.env` を作成し、取得したトークンを記入します。

```bash
cd ~/workspace/slack-yama
cp config/.env.example config/.env
chmod 600 config/.env
vi config/.env
```

`config/.env` の記述内容：
```ini
SLACK_APP_TOKEN=xapp-1-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
SLACK_BOT_TOKEN=xoxb-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
```

### Step 7: systemd ユニットの有効化・起動およびイベントフック連携
```bash
# systemd ユニットの配置と有効化
sudo cp ~/workspace/slack-yama/yama-listener.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now yama-listener.service

# ステータスと Heartbeat の確認
sudo systemctl status yama-listener.service
cat ~/workspace/slack-yama/state/heartbeat.json
```

Watchdog（`watchdog.sh`）の駆動方式：
- 現行運用では cron ではなく、30秒ごとに走るプラットフォームのイベントフック（`~/hooks/scripts/slack-yama-dispatch.sh`）内で毎回自動実行されます（同フック内でユニット自己修復・プロキシ環境変数同期・`dispatcher.sh` 呼び出しを行い、backfill・1時間ごとの `reconcile-replies`・5分ごとの auto_join は 2026-11-22 以降 dispatcher の後ろで、バッチを起こさない周期にだけ実行されます。backfill は連続バッチ中も最低5分に1回）。

---

## 4. 動作モード切替方法（`config/mode.txt`）

`config/mode.txt` の文字列を書き換えることで、再起動なしに Yama の動作モードを切り替えられます。

| モード値 | 挙動 | 用途 |
| :--- | :--- | :--- |
| `shadow` | **ログ収集・Triage検証のみ**。Slack への返信は一切行わず、判断結果の検証のみ行う | 初期導入時（Phase 1） |
| `mention` | **明示的な `@Yama` メンションまたは DM のみ**自律返信を行う | 限定テスト運用（Phase 2） |
| `full` | **全投稿 Triage モード**。メンション・名指し・全体への質問や依頼にも自律参加して返信する | 本番運用（Phase 3〜） |

切替コマンド例：
```bash
# shadow モード（初期値）
echo "shadow" > ~/workspace/slack-yama/config/mode.txt

# mention モードへ切替
echo "mention" > ~/workspace/slack-yama/config/mode.txt

# full モードへ切替
echo "full" > ~/workspace/slack-yama/config/mode.txt

# 現在のモードを確認
~/workspace/slack-yama/venv/bin/python ~/workspace/slack-yama/agent_tools.py mode
```

---

## 5. 運用手順・CLIリファレンス

### 5.1 ディスパッチャの実行（フックからの呼び出し）
```bash
~/workspace/slack-yama/dispatcher.sh
```
- **終了コード `0` (`DISPATCHED:<件数>`)**：`PENDING` イベントを最大10件 `PROCESSING` に更新し、`state/payload.json` に書き出しました。エージェントを起動してください。
- **終了コード `1` (`NO_EVENTS`)**：未処理イベントはありません（エージェント起動不要）。
- **終了コード `2` (`LOCKED`)**：先行セッションがロック保持中です（エージェント起動不要）。

### 5.2 エージェント用ツール（`agent_tools.py`）の使い方
```bash
# 1. 現在 PROCESSING 中のイベント一覧を JSON で取得
./venv/bin/python agent_tools.py pending

# 2. 現在の動作モードを確認
./venv/bin/python agent_tools.py mode

# 3. 処理開始リアクション (👀) を付与
./venv/bin/python agent_tools.py react --channel C01234567 --ts "1700000001.000100" --emoji eyes

# 4. チャンネルまたはスレッドの直近履歴を取得
./venv/bin/python agent_tools.py history --channel C01234567 --limit 20
./venv/bin/python agent_tools.py history --channel C01234567 --thread-ts "1700000001.000100" --limit 20

# 5. 返信メッセージをファイル経由で送信
cat << 'EOF' > /tmp/yama_reply.txt
お疲れ様です、Yamaです。確認いたしました。
EOF
# reply は --event-id <ID> と --no-event-id のどちらか一方が必須（どちらも無いと非0終了）
# イベントへの返信: --event-id（events テーブルに無い ID は拒否。--force --reason <理由> のときだけ通す）
./venv/bin/python agent_tools.py reply --channel C01234567 --thread-ts "1700000001.000100" --text-file /tmp/yama_reply.txt --event-id Ev01234567
# イベントに紐づかない共有・報告の投稿: --no-event-id（replies 台帳には記録されない）
./venv/bin/python agent_tools.py reply --channel C01234567 --text-file /tmp/yama_share.txt --no-event-id

# 6. 処理結果のコミット
# 返信完了時
./venv/bin/python agent_tools.py commit --event-id Ev01234567 --status DONE
# スルー（返信不要）判断時
./venv/bin/python agent_tools.py commit --event-id Ev01234567 --status IGNORED
# エラー発生時（retry_count < 3 なら PENDING に自動差戻し、3回以上で FAILED 確定）
./venv/bin/python agent_tools.py commit --event-id Ev01234567 --status FAILED --error "Rate limited"
```

### 5.3 テストの実行
実 Slack に接続せず、SQLite・フィルタ・ディスパッチャ・flock 排他制御・状態遷移を自動検証します。
```bash
~/workspace/slack-yama/venv/bin/python ~/workspace/slack-yama/tests/run_tests.py
```
