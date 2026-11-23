# 調査依頼：commit_event の done ガード不在は実際に起きうるか

コードベース（~/workspace/slack-yama/）を実際に読んで調査してください。今回は調査のみで、コードの変更はしないでください。

## 調べること
1. `agent_tools.py` の `commit_event`（および commit-batch）の実装：現在の状態を確認せずに上書きしているか。どの遷移が可能か（DONE→FAILED、DONE→PENDING、FAILED→DONE など）を実装から列挙する
2. 遅れた commit が現実に発生しうる経路の棚卸し：Reaper の差し戻し（PROCESSING 15分）、ワーカーのタイムアウト（600秒）と commit の競合、遅延再送、reconcile の回収、手動 requeue。それぞれ「起きうる／起きない」と根拠
3. 過去の実データ（state/slack_events.db）に、DONE から戻った形跡や不整合（updated_at の逆行、replies 台帳と events の食い違い）がないか
4. ガードを入れる場合の設計案：どの遷移を禁止するか、手動 requeue と reconcile をどう通すか、既存テストへの影響

## 成果物
- 調査結果を ~/workspace/slack-yama/claude_commit_guard_investigation_2026-11-23.md に書く
- 標準出力には結論の要約（起きうるか、実害の形跡、設計案の骨子）だけを短く返す
- コードの変更、Slack への投稿、GitHub への push はしない
