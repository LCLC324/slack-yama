# 依頼：履歴取得リトライ強化・送信前チェックの fail-closed 化・DONE残存エラーの定期照合

対象プロジェクト：~/workspace/slack-yama（Yama Slack ボット）

## 背景（実データ）
定期照合の共有（event-id share-reconcile-20261112）が同じ文面で2回投稿されました（ts 1794491587.025199 / 1794491605.133199、18秒間隔）。経路は次の通りです：
- 1回目：投稿は着地したが応答が IncompleteRead で失われ、回復確認の履歴取得も失敗して台帳に記録されなかった。
- 2回目：再実行時の送信前チェック（スレッド履歴確認）も履歴取得に失敗し、現行仕様では警告だけで投稿に進む（fail-open）ため、既に着地していた1回目を見つけられず再投稿した。

Neiro の指摘を受け、Owner の承認で次の3点を実装します。

## 実装内容
### (a) 履歴取得のリトライ見直し
- agent_tools.py の履歴取得（fetch_history 系と、投稿前チェック・回復確認で使うスレッド／チャンネル履歴の取得経路）について、接続系例外（IncompleteRead、RemoteDisconnected、URLError 等）に対するリトライを現行の3回から**最大5回**に増やし、間隔は指数バックオフ（1秒→2秒→4秒→8秒）にすること。
- SlackApiError（API エラー応答）は従来通りリトライせず即時失敗のまま。
- 投稿前チェック・回復確認・reconcile-replies の全経路で効くようにすること（取得ヘルパーが複数ある場合は共通化するか、全経路に同じ方針を適用すること）。

### (b) 送信前チェックが確認不能な場合の扱い（スレッド返信のみ fail-closed 化）
- スレッド返信（thread_ts あり）の投稿前チェックで、(a) のリトライを尽くしてもスレッド履歴が取得できない場合は、**投稿せず**に専用エラー `pre_post_check_unverifiable` を返して止めること（警告して投稿継続、はしない）。
- チャンネル投稿（thread_ts なし）の bounded チェックは従来通り fail-open のまま変更しない（通知型は fail-open という三者で確定した方針のため）。
- 応答ロスト後の回復確認が尽きた場合の ambiguous_post_outcome は従来通り。

### (c) DONE なのに古い error_message が残っているイベントの定期照合
- reconcile-replies を拡張（または同コマンド内の追加フェーズとして実装）：
  - 追加候補：status='DONE' かつ error_message が NULL でないイベント（直近 --days 日分）。
  - 各候補のスレッドを全体走査し、Yama の返信が着地していることが確認できたら、replies 未記録なら record_reply() で記録し、そのイベントの error_message を NULL にクリアする。
  - Yama の返信が見つからない候補は error_message を変更せず、報告の unresolved に入れる。
  - 出力 JSON に done_error_cleared（クリアした event_id のリスト）と done_error_unresolved を含める。
  - --dry-run では記録もクリアもせず予定だけ返す。

## テスト（tests/run_tests.py に新規テストを追加）
- (a) 履歴取得が接続系例外で3回失敗した後に4回目で成功する（従来は失敗扱いだったケースが回復する）。
- (b) スレッド返信で投稿前チェックの履歴取得が全試行失敗 → 実投稿されず `pre_post_check_unverifiable` が返る。チャンネル投稿で bounded チェックの履歴取得が全試行失敗 → 従来通り投稿される（fail-open 回帰確認）。
- (c) DONE＋error_message 残存＋スレッドに Yama 返信あり → reconcile で replies 記録と error_message クリア。DONE＋error_message 残存＋返信なし → エラーは残り unresolved に入る。
既存テストもすべて通過させること（合計22件想定、分け方は任せる）。

## 注意
- 実際の state/slack_events.db には触らないこと（テストは一時 DB とモックで）。
- config/.env の内容は表示・複製しないこと。
- テスト実行時間への配慮：リトライ間隔はテストでモック／短縮できる既存の仕組みに合わせ、テスト全体が極端に遅くならないようにすること。
