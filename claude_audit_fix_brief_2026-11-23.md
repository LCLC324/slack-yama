# 実装依頼：監査（2026-11-23 夜）の指摘の修正

監査レポート ~/workspace/slack-yama/claude_audit_2026-11-23_night.md の指摘のうち、次のものを直してください。重大1件（自己レビュー cron の文面）は Muse が cron.update で対応済みなので対象外です。

## 直すもの
1. **metadata フォールバックの残り**：`_lookup_own_recent_message_once`（agent_tools.py:3297 付近）に残っている TypeError 時のフォールバック2か所（conversations_replies と conversations_history）を削除する。あわせて `_fetch_history_unbounded` の同じ形のフォールバックも削除する。既存テストの偽クライアント（厳密なシグネチャのもの）は、この経路を通るために必要なら手直ししてよい（テストの意図と assert は変えない）。本番 SDK が include_all_metadata に対応していることは監査で確認済み
2. **commit --force の抜け道**：`commit --status FAILED --force --reason` を使うと、台帳の確認なしで FAILED→PENDING に戻せる（requeue ブロックの抜け道）。監査の対応案どおり、force で FAILED→FAILED を指定したときはリトライ処理を通さず FAILED のままにする。docstring の「FAILED goes back to PENDING only through requeue」と実挙動を一致させること
3. **アーカイブの印付け**：アーカイブに失敗した行に「アーカイブ済み」の印が付いて二度とアーカイブされなくなる。take の時点で、アーカイブに失敗した行には印を付けない（または _archived: false を明示する）ようにし、restore は既存の印を尊重すること
4. **アーカイブの keys**：`failed_with_landed_reply` の行で key が null になり keys も入らない。event_ids があれば keys に入れること
5. **フックの note の説明**：~/hooks/scripts/slack-yama-dispatch.sh の運用警告の note に、failed_with_landed_reply／requeue_blocked／requeue_forced の説明を追加する。ワークスペース側のコピー（hooks/slack-yama-dispatch.sh）も同期すること
6. **requeue 拒否時の stderr**：requeue を拒否したとき stderr が「WARN 行＋JSON」の2行になる。JSON 1行に統一すること（警告の内容は運用アラート側に残っているはずなので、そちらで代替できるか確認すること）

## 記録だけするもの（修正しない）
- 指摘5（1回の take が大きいと上限を超えて書かれる）：IMPROVEMENTS.md の候補26に「保持量は最大でおよそ10 MiB＋1回分」と正確な表現で追記する
- 指摘9（--no-event-id の選び間違いは防げない）：候補28に「設計上の残余」として追記する
- 指摘2の対応にあわせて、候補28の状態の記述を実態に合わせる（フォールバック削除が全経路で完了した形に直す）

## 制約
- 既存テストは全件通すこと（`./venv/bin/python tests/run_tests.py`）。新しく要るテスト：commit --force で FAILED→FAILED が FAILED のままになること、アーカイブ失敗行に印が付かないこと、failed_with_landed_reply の keys が入ること、_lookup_own_recent_message_once でフォールバックが消えたこと
- 変更前の原本は bak-2026-11-23-auditfix/ に退避すること
- 成果物として実装レポートを ~/workspace/slack-yama/claude_audit_fix_2026-11-23.md に書くこと
- Slack への投稿、GitHub への push はしない（Muse が別途行う）
