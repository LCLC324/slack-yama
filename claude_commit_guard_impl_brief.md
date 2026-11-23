# 実装依頼：commit_event に終端ガード（第1段）を入れる

調査レポート ~/workspace/slack-yama/claude_commit_guard_investigation_2026-11-23.md の §4-2 の設計で実装してください。agy のレビュー（~/workspace/slack-yama/agy_commit_guard_out.md）も読みました。採用する方針は次の通りです。

## 採用する設計（終端ガード、第1段）
- `commit_event` の `BEGIN IMMEDIATE` 内、行を SELECT した直後に1か所だけ追加する。commit・commit-batch・cmd_reply の内部 commit すべてに効かせる
- 遷移表：
  - PENDING／PROCESSING → 何でも：従来どおり
  - DONE／IGNORED → 同じ状態：no-op の成功（行は変えず、`"noop": true` を返す）
  - DONE／IGNORED → 別の状態：拒否（`already_terminal`、exit 1）
  - FAILED → 何でも：拒否。戻すのは requeue（→PENDING→再処理）が正規の経路
  - 終端 → 別の状態＋`--force --reason <文>`：許可。11-23 型の訂正用。reason を error_message に残し、運用アラートの outbox に1行書く
- commit-batch の全件一括の検証フェーズにも状態チェックを足す（終端への別状態は「1件でも不正なら何もしない」側に入れる。同一状態は no-op として通す）
- cmd_reply の内部 commit（:2653）の拒否は今も握りつぶされるが、出力の `status` が実際の値でなく "DONE" になる点だけ直す（現在の行を読んで返す）
- 拒否された commit は例外で落とさず no-op とし、警告ログ（運用アラートへの1行）を必ず出すこと
- フックのプロンプトに1行足す：「commit が `already_terminal` を返したら、それは他の処理で確定済みということ。再試行せず報告に1行含める」

## 採らないもの
- 「PROCESSING からのみ許可」の厳格版は採らない（調査で 76/81 となり、本命の競合も防げないため）
- 第2段の claim トークンは今回やらない（並行ワーカーの実在が未確認のため）

## ついでに直す小さな点（レポート §4-4）
- dispatcher.sh の「ワーカーは600秒で打ち切られる」前提のコメントを正しい前提に直す
- reconcile が DONE の error_message を一律に消して判定理由まで消す点を直す（消すのは reply／ambiguous／unverifiable 系の文言に限る）

## 制約
- 既存テストは全件通すこと（`./venv/bin/python tests/run_tests.py`）。新しく要るテスト：遷移表の各行（拒否・no-op・`--force`・commit-batch の検証での拒否・FAILED から requeue を経て DONE になる流れ）
- IMPROVEMENTS.md に候補19（commit_event が現在の状態を確認しない）を「対応済み」として更新すること
- 変更前の原本は bak-2026-11-23-commitguard/ に退避すること
- 成果物として実装レポートを ~/workspace/slack-yama/claude_commit_guard_impl_2026-11-23.md に書くこと
- Slack への投稿、GitHub への push はしない（Muse が別途行う）

## 注意
- Neiro 側のチェックが期待している test_slack_event_state_done_guard.py というファイル名があるが、こちら側には存在しない。テストは既存の tests/run_tests.py の構成に沿って追加し、そのファイル名に合わせるために無理な作りにはしないこと
