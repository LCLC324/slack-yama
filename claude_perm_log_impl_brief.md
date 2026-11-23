# 実装依頼：運用アラートの配送済み行を追記専用の永続ログへアーカイブする

調査・設計は Muse と agy で済ませた。実装だけを依頼する。agy のレビュー（~/workspace/slack-yama/agy_perm_log_out.md）を必ず読むこと。

## 採用する設計（agy の「条件付きで持つ」）
- 配送用 outbox（state/ops_alerts.jsonl）の `take` 側で、配送済みの行を追記専用の永続ログへアーカイブする。**ガードから2箇所に書く二重書きはしない**。単一の記録経路にする
- アーカイブ先：state/ops_alerts_archive.jsonl（新規）。書く範囲は outbox 全般をそのままアーカイブする（却下・強制訂正に限定しない。outbox の行はすべて運用アラートとして残す価値があるため）
- 上限を最初から入れる：サイズ上限＋1世代ローテーション（または最大行数維持）。際限ない蓄積にしないこと
- スキーマ：Neiro・Yanayana の mark_rejected.jsonl と項目を揃える（at・key・attempted_status・current_status・reason に加え、source・action を入れる）。rejected（却下）と forced（理由付き訂正）を明示的に区別するフィールドを入れること。3者で揃えるのはスキーマであってファイル構成ではない
- fail-open＋失敗の検知：アーカイブの書き込み失敗で本処理は止めない。書き込みに失敗したときだけ標準エラー／実行ログ、または outbox にエラーを1行流すこと
- 前提の確認：配送先（Slack の運用チャット）の履歴だけでは、検索性・保持期間・機械集計の面で不足がある。今日の調査でも状態の履歴が無く、間接指標でしか「戻った形跡」を判定できなかった。永続ログはその穴を埋める

## 制約
- 既存テストは全件通すこと（`./venv/bin/python tests/run_tests.py`）。新しく要るテスト：take 時にアーカイブされること、ローテーションが働くこと、書き込み失敗時に本処理が止まらずエラーが出ること、rejected／forced の区別
- IMPROVEMENTS.md に候補として記録すること（候補番号は既存の最大＋1。内容：永続ログのアーカイブ化）
- 変更前の原本は bak-2026-11-23-permlog/ に退避すること
- 成果物として実装レポートを ~/workspace/slack-yama/claude_perm_log_impl_2026-11-23.md に書くこと
- Slack への投稿、GitHub への push はしない（Muse が別途行う）
