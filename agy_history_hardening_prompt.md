# 変更依頼: agent_tools.py の履歴取得を堅牢化する（リトライ＋ローカルDBフォールバック）

作業ディレクトリ: ~/workspace/slack-yama/
対象: agent_tools.py, tests/run_tests.py（listener.py / dispatcher.sh は変更しない）

## 背景
Slack API への履歴取得（conversations_replies / conversations_history）が、接続の瞬断（IncompleteRead、Remote end closed、URLError 等）で時々失敗する。実測では 5 回連続成功することもあり、失敗はバースト的に起こる。読み取り系は冪等なのでリトライが有効。またリスナーが受信したイベントはローカル SQLite（state/slack_events.db）に全て残っているため、API が落ちていてもローカルで代替できる。

## 変更内容

### 1. 読み取り系 API コールのリトライ
- `history` サブコマンドが使う Slack API 呼び出し（conversations_replies / conversations_history）を、接続系例外（urllib.error.URLError、http.client.IncompleteRead、ConnectionError、TimeoutError、socket.timeout 等）に対して最大3回までリトライする（間隔 1秒 → 2秒 のバックオフ）。
- SlackApiError（API が返した正式なエラー。例: channel_not_found）はリトライせず即座に失敗として扱う。
- 投稿系（reply）は既存の「自動リトライしない＋着地確認」の仕様を変えない。react も変更しない。

### 2. ローカル DB フォールバック
- リトライしても履歴取得に失敗した場合、state/slack_events.db の events テーブルから該当チャンネル（thread_ts 指定時はそのスレッド）のイベントを時系列で返して正常終了する。
- 出力 JSON には `"source": "local_db_fallback"` を含め、呼び出し側が API でないことに気づけるようにする（API 成功時は `"source": "slack_api"`）。
- ローカルに該当イベントが無い場合は空の messages で返す（エラーにしない）。フォールバックである旨は stderr ではなく出力 JSON の source フィールドで示す。
- フォールバックの各メッセージは、API 形式に寄せて user / text / ts / thread_ts / is_bot を含める。

### 3. テスト（tests/run_tests.py）
- 既存の全テストが引き続き PASS すること。
- 新しいテストを追加する: (a) Slack API 呼び出しが接続系例外を投げる状況を再現し、history がローカル DB の内容を source=local_db_fallback で返すこと。(b) 1回目の接続系例外の後に成功する状況で、リトライにより slack_api として結果が返ること。再現は WebClient の差し替えやモックで実装してよい（実 API には接続しない）。
- テスト総数は増えてよい。全 PASS を確認してから完了を報告する。

## 制約
- トークンや .env の中身を表示・出力しない。
- systemd サービスの再起動はしない（デプロイは別途行う）。
- 既存のサブコマンド（pending / reply / react / commit / mode）の挙動と出力形式は、history の source フィールド追加以外は変えない。
