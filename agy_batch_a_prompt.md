# 実装依頼：監査バッチA（高重要度3件）

作業場所は ~/workspace/slack-yama/ です。まず agy_full_audit_out.md の指摘1・2・3を読み、対象コード（agent_tools.py、dispatcher.sh）を確認してから実装してください。

## 実装内容

### 1. PROCESSING の置き去り解消とディスパッチ競合の防止（監査指摘1）
- dispatcher.sh で新しいイベントを PROCESSING に遷移させる前に:
  - updated_at が新しい（処理中とみなせる）PROCESSING イベントが存在する場合は、新規ディスパッチをスキップする（既存の payload を上書きしない）。
  - 一定時間（目安15分。フック側の処理上限600秒より十分長く）を超えた PROCESSING イベントは PENDING に戻す（reaper）。
- 閾値は定数として明示し、理由をコメントに残すこと。

### 2. reconcile_replies の誤紐付け修正（監査指摘2）
- agent_tools.py の reconcile_replies 内:
  - 未割当の返信がない場合に yama_msgs[0] を強制的に選ぶフォールバックを削除する（その場合は without_reply として扱い、エラークリアのみ）。
  - reply_confirmed の判定から `or bool(yama_msgs)` を削除する（記録された reply_ts がスレッド内に実在する場合のみ True）。
  - 返信候補のマッチングに「対象イベントの ts より後の投稿であること」の条件を加える。

### 3. ローカルフォールバック履歴に自ペ返信を含める（監査指摘3）
- agent_tools.py の fetch_history / get_local_history_fallback:
  - ローカル DB フォールバックの結果に、replies テーブルの該当チャンネル（スレッド指定時はそのスレッド）の記録をマージし、Yama 自身の返信も履歴として見えるようにする。
  - 結果の source が "local_db_fallback" であることは従来通り明示する。

## 制約
- 既存テスト（./venv/bin/python tests/run_tests.py、22件）が全て通り続けること。各指摘に対応する新規テストを追加すること（監査出力の「追加すべきテスト」を参考に）。
- フック定義（~/hooks/）は変更しない。config/.env の中身は読まない・変更しない。
- 変更は上記3件の範囲に限定する。他の監査指摘（4〜10）には手を付けない。
- 完了後、変更ファイル・変更の要旨・テスト結果（総件数と全件通過か）を簡潔に報告すること。
