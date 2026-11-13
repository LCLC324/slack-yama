# 実装依頼：監査バッチC（中重要度4件）

作業場所は ~/workspace/slack-yama/ です。まず agy_full_audit_out.md の指摘5・6・7・8を読み、対象コードを確認してから実装してください。バッチA・Bは完了済み（テスト27/27通過）で、その変更と競合しないようにしてください。

## 実装内容

### 5. 送信前チェックの再試行粒度（監査指摘5）
- agent_tools.py の _lookup_own_recent_message_once / _find_own_recent_message：現在は auth_test ＋ 最大20ページ全体を1つのリトライ単位にしているため、途中の1回の IncompleteRead で最初からやり直しになる。
- _scan_thread_yama_messages と同様に、APIコール単位で _call_with_retry を適用する形に揃える。bot_user_id の取得も毎回の auth_test に依存しすぎないよう、取得済みの値を再利用する（環境変数や引数で渡せる場合はそれを優先）。

### 6. backfill の重複排除と取りこぼし（監査指摘6）
- listener.py の save_event：挿入前に (channel_id, ts) の既存行を確認し、backfill 由来の行が既にある場合は二重に INSERT しない（可能なら events(channel_id, ts) の UNIQUE インデックスも検討し、既存DBへの移行が安全な場合のみ追加）。
- backfill.py の conversations_replies：oldest とカーソルページングを使い、返信30件超のスレッドでも新しい返信を回収できるようにする。

### 7. .env のアトミック書き換え（監査指摘7）
- ~/hooks/scripts/slack-yama-dispatch.sh の .env 更新を、同一ディレクトリ内の一時ファイルに書いて chmod 600 してから mv で置換する形に変更する（読み手が空ファイルを掴む競合の解消）。

### 8. エラー分類の適正化（監査指摘8）
- agent_tools.py の _call_with_retry：SlackApiError のうち一時的なもの（HTTP 429 / 5xx、エラーコード ratelimited / internal_error / service_unavailable / request_timeout 等）はリトライ対象にする。429 の場合は Retry-After を尊重する。恒常的エラー（channel_not_found 等）は従来通り即時失敗。
- fetch_history：一時的な SlackApiError の場合はローカルフォールバックへ進む。
- listener.py 起動時の auth_test にも再試行を付ける。

## 制約
- 既存テスト（./venv/bin/python tests/run_tests.py、27件）が全て通り続けること。各指摘に対応する新規テストを追加すること（監査出力の「追加すべきテスト」を参考に）。
- config/.env の中身は読まない・変更しない（書き換え方式の変更のみ）。
- 変更は上記4件の範囲に限定する。監査指摘9・10には手を付けない。
- 完了後、変更ファイル・変更の要旨・テスト結果を簡潔に報告すること。
