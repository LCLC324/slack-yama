# 依頼：replies 台帳の記録漏れ修正を実装してください

対象プロジェクト：~/workspace/slack-yama（Yama Slack ボット）

## 背景（実データ）
2026-11-12 のイベント Ev0CGMTJ3KBR（18:37 JST）：初回送信で応答が IncompleteRead で失われ、回復確認でも見つからず「reply not posted, no replies-table record」という error_message で DONE 確定しました。ところが実際には返信は約3分後の 18:39:55 に Slack へ着地しており、replies 台帳には当該行がありませんでした。後日の別イベントのトリアージでは「replies テーブルで2件、ライブ履歴で3回目」と数え直しが発生し、台帳欠落の影響が実際に出ています。

この改善は自己レビュー候補2として Slack に提案され、Neiro の具体案（同じ書き込み関数を使う、reply_ts 記録と error 訂正のセット化、遅れて着地する返信への再確認）をすべて取り入れることが決まり、Owner の承認も得ています。

## 穴の場所（特定済み）
1. ワーカー（処理側）がスレッド履歴を自分で確認して「返信は既にある」と判断した場合、`reply --event-id` を呼ばずに DONE で commit するため、replies 台帳へ記録する経路が存在しない。
2. commit_event は DONE 確定時も error_message を COALESCE 的に保持するため、後から着地が確認できても「reply not posted」という誤ったエラーが残る。
3. 応答ロスト後の回復確認は数秒の窓しかなく、約3分遅れで着地した返信を拾えない（再実行時に reply 経由で全スレッド走査すれば拾えるが、上記1の経路では reply 自体が呼ばれない）。

## 実装内容（agent_tools.py）
1. **record-reply サブコマンドを追加**：既存の返信を台帳へ記録するための明示的な経路。引数は --event-id、--channel、--reply-ts、--text-file（文面から text_hash を計算）、任意で --thread-ts。cmd_reply が使うのと**同じ書き込み関数**で replies テーブルへ記録すること。既に同じ event_id の行があれば何もせず deduplicated 的な結果（ok: true、already_recorded: true 等）を JSON で返すこと。
2. **error_message の訂正をセット化**：commit_event で DONE へ確定する際、error_message の明示指定がない場合は既存の error_message をクリア（NULL）すること。FAILED のときは従来通り error_message を記録する。既存テスト4（コミット状態遷移）の期待と矛盾する場合は、テスト側の期待を新仕様に合わせて更新してよい（その場合は変更点を報告に明記すること）。
3. 既存の reply 経路（投稿前スレッド全体走査、応答ロスト回復確認、ambiguous_post_outcome、チャンネル bounded チェック、prune-replies）は変更しない。

## テスト（tests/run_tests.py に新規テストを追加）
- record-reply で replies 行が記録され、同じ event_id で2回呼んでも1行のまま。
- record-reply 後に reply --event-id を呼ぶと、台帳照会で deduplicated: true になり実投稿しない。
- FAILED で error_message を付けたイベントを DONE で commit すると error_message がクリアされる。
既存テストもすべて通過させること（合計17件想定：既存16件＋新規1件。テストの分け方は任せる）。

## 注意
- 実際の state/slack_events.db には触らないこと（テストは一時 DB で）。
- config/.env の内容は表示・複製しないこと。
