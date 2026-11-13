# 実装依頼（復旧）：監査バッチDの未完了分

作業場所は ~/workspace/slack-yama/ です。前回のバッチD実行が途中で中断され、完了報告がありません。現在のコードは一部変更済み（agent_tools.py、listener.py、watchdog.sh、hooks/scripts/slack-yama-dispatch.sh、tests/run_tests.py が変更されている）で、テストは 31/31 通過の状態です。

まず agy_batch_d_prompt.md と agy_full_audit_out.md の指摘9・10を読み、次に各項目について「現在のコードで実施済みか」を実物で確認してください。実施済みの項目は変更せず、未実施の項目だけを完了させてください。

## 対象項目（バッチD）
9. watchdog.sh の再配置バックオフ初回値を60秒にする／フック側ユニット自己修復で、入れ替え後に再起動が要る場合にその旨をログへ残す。
10. 次の4点：
   - agent_tools.py の未使用定数 RECENT_MESSAGE_WINDOW_SECONDS の削除（現状まだ残っています）
   - IMPROVEMENTS.md と README.md の古い記述の修正：応答ロスト回復確認の回数（3回→5回）、ウォッチドッグの駆動方式（cron ではなく30秒フック内）
   - commit_event で IGNORED 確定時も error_message をクリアする（＋この振る舞いの新規テスト）
   - listener.py と dispatcher.sh の busy_timeout 二重指定の統一
記録の整理：
   - IMPROVEMENTS.md の候補3・候補5の状態を「実装済み（2026-11-13、バッチC／バッチB）」に更新（現状まだ未更新です）
   - IMPROVEMENTS.md 末尾に、監査指摘1〜10とバッチA〜Dの対応関係を短く追記

## 制約
- 既存テスト（./venv/bin/python tests/run_tests.py）が全て通り続けること。
- config/.env の中身は読まない・変更しない。
- 完了後、各項目について「今回実施／実施済みで変更なし」の別と、テスト結果を簡潔に報告すること。
