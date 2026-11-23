# 実装依頼：heal-replied は「検知＋requeue ブロック」で止める（代替案A）

候補27（reconcile への heal-replied 回復パス）について、agy のレビュー（~/workspace/slack-yama/agy_heal_recovery_out.md）を受けて、Muse が次の方針を決めた。実装だけを依頼する。

## 採用する方針（代替案A）
- reconcile に FAILED→DONE の状態変更権限は与えない。reconcile は従来通り「台帳記録のみ」に徹する（終端ガードを完全に維持する。実例はまだ1件のため、状態遷移の例外は増やさない）
- 代わりに入れる2点：
  1. **検知＋通知**：reconcile が「台帳に着地記録があるのにイベントが FAILED」の組み合わせを見つけたら、運用アラート（outbox）へ1行出す
  2. **requeue のブロック／警告**：手動 requeue で、対象イベントの event_id が replies 台帳に着地記録として既にある場合、通常はブロックする（明確なメッセージつき）。理由付きの明示フラグ（--force --reason、終端ガードの訂正と同じ形）でだけ通す

## あわせて調査すること（実装前に確認）
- Yama の送信（cmd_reply）が metadata の event_id をどこまで確実に付けているか。欠けやすい箇所があれば、レポートに書くこと（今回の修正範囲には含めない。metadata の付与・取得側の修正は別途判断する）
- 既存の実例 Ev0CH5CWKHM0（着地は確認できたのに FAILED のまま）が、検知の対象になることを確認すること

## 制約
- 既存テストは全件通すこと（`./venv/bin/python tests/run_tests.py`）。新しく要るテスト：検知が働くこと、requeue のブロックと --force での通過、processing 中のものは対象外であること
- IMPROVEMENTS.md の候補27を「対応済み（代替案Aで実施。FAILED→DONE の回復パスは実例が増えるまで見送り）」として更新すること
- 変更前の原本は bak-2026-11-23-healrec/ に退避すること
- 成果物として実装レポートを ~/workspace/slack-yama/claude_heal_recovery_altA_2026-11-23.md に書くこと
- Slack への投稿、GitHub への push はしない（Muse が別途行う）
