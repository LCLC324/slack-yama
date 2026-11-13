# 実装依頼：監査バッチD（低重要度2件＋記録の整理）

作業場所は ~/workspace/slack-yama/ です。まず agy_full_audit_out.md の指摘9・10を読み、対象コード・文書を確認してから実装してください。バッチA・B・Cは完了済み（テスト31/31通過）で、その変更と競合しないようにしてください。

## 実装内容

### 9. ウォッチドッグまわりの不整合（監査指摘9）
- watchdog.sh の再配置バックオフ初回値（REINSTALL_BACKOFF_BASE_SEC、現行デフォルト30秒）を、フック間隔30秒より大きい値（60秒）にする。
- フック側（~/hooks/scripts/slack-yama-dispatch.sh）のユニット自己修復：ユニットファイルを入れ替えたのにサービスが古い定義のまま動き続ける問題について、watchdog.sh 側の実装と矛盾しない範囲で、入れ替え後に再起動が要る場合はその旨をログに残す（権限の都合で再起動できない環境では、無理に実行せずログで分かるようにする）。

### 10. 文書・コードの食い違いと死んだコード（監査指摘10）
- agent_tools.py の未使用定数 RECENT_MESSAGE_WINDOW_SECONDS を削除する（本当に未使用か全体を検索してから）。
- IMPROVEMENTS.md と README.md の古い記述を現行実装に合わせる：応答ロスト回復確認の回数（3回→実装どおり5回）、ウォッチドッグの駆動方式（cron ではなく30秒フック内で実行）。
- commit_event：IGNORED で確定したイベントも DONE と同様に古い error_message をクリアする。
- listener.py と dispatcher.sh の busy_timeout 二重指定（connect の timeout と PRAGMA）を、意図が分かる形に統一する（値を変える場合は理由をコメントに残す）。

### 記録の整理
- IMPROVEMENTS.md の候補3（live+backfill 重複排除）と候補5（DLQ 再送）は、バッチC・Bで実装済みになったため、状態を「実装済み（2026-11-13、バッチC／バッチB）」に更新する。
- 今回の監査（agy_full_audit_out.md の指摘1〜10）とバッチA〜Dの対応関係を、IMPROVEMENTS.md の末尾に短く追記する。

## 制約
- 既存テスト（./venv/bin/python tests/run_tests.py、31件）が全て通り続けること。振る舞いを変える箇所（IGNORED のエラークリア等）には新規テストを追加すること。
- config/.env の中身は読まない・変更しない。
- 変更は上記の範囲に限定する。
- 完了後、変更ファイル・変更の要旨・テスト結果を簡潔に報告すること。
