# 依頼：replies 台帳の90日 prune を実装してください

対象プロジェクト：~/workspace/slack-yama（Yama Slack ボット）

## 背景
agent_tools.py の replies テーブル（返信冪等化の台帳）は以下のスキーマです：

```sql
CREATE TABLE replies (
    event_id TEXT PRIMARY KEY,
    channel_id TEXT NOT NULL,
    thread_ts TEXT,
    reply_ts TEXT NOT NULL,
    text_hash TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

台帳が無限に増えないよう、Yanayana の運用に倣って「90日より古い行を削除する prune」を追加します。Yama は Slack で「90日＋タイムスタンプが読めないものは残す設計ごと取り入れる」と Yanayana に伝えたので、その設計通りに実装してください。

## 実装内容
1. agent_tools.py に prune 用のサブコマンドを追加してください（例：`prune-replies`）。引数は `--days 90`（既定90）と `--dry-run` を受け付け、DB パスは既存の他のコマンドと同じ扱いにしてください。
2. 削除条件：created_at が「現在時刻 − days 日」より古い行を削除する。
3. **created_at が NULL、またはタイムスタンプとして解釈できない行は削除しない**（読めないものは残す）。
4. 出力は JSON で、削除件数（dry-run 時は削除予定件数）と dry-run かどうかが分かる形にしてください。
5. 既存の reply 経路・他のサブコマンド・テスト対象の既存挙動は変更しないでください。

## テスト
tests/run_tests.py に新規テスト15を追加してください：
- 91日より古い行は削除される
- 89日の行は残る
- created_at が NULL の行は残る
- created_at が解釈できない文字列の行は残る
- --dry-run では削除されず、予定件数だけ返る

既存テスト1〜14もすべて通過させてください（合計15件）。

## 注意
- 実際の state/slack_events.db に対して prune を実行しないでください（テストは一時 DB で行うこと）。
- config/.env の内容は表示・複製しないでください。
