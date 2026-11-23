# レビュー依頼：reconcile の heal-replied 回復パスへの補足2点を取り入れるべきか

ツールは使わず、この文章だけでレビューしてください。穴・矛盾・より単純な代替案を重要度順に挙げ、最後に各点について「取り入れる」「取り入れない」「条件付きで取り入れる」のどれを推すかと理由を述べてください。

## 背景
Slack 常駐ボット Yama。今夜の自己レビューで「reconcile に heal-replied 回復パスを追加する」提案を投稿した。内容は、着地は確認できたのにイベントが FAILED のまま残る食い違い（実例1件）を、reconcile が台帳記録に加えて状態も DONE へ補正する経路を足す、というもの。

Yanayana から賛成＋2点の補足があった：
(1) metadata の event_id が欠けやすいため、台帳の (channel, ts) での照合も使うべき
(2) 回復対象から processing 中のものと dead（終端の失敗）を外すべき

## Yama の現状
- reconcile の照合は2段：①metadata の event_id／request_id の完全一致、②文面の正規化一致。推定フォールバックは廃止済み。対象はスレッド返信のみ
- replies 台帳の列は event_id, channel_id, thread_ts, reply_ts, text_hash, text
- reconcile は現在、状態を変えない（台帳記録と DONE のエラーメッセージ消去だけ）。回復パスを足すと、FAILED→DONE の補正が初めて入る
- 先日入れた commit の終端ガードでは、FAILED は終端で、戻すのは手動 requeue が正規の経路、とした。reconcile の回復パスは、その例外になる

## 論点
- (1) (channel, ts) 照合は、reply_ts（投稿の ts）で引く形になる。metadata が欠けた場合の第3の照合として安全か。誤爆（別の投稿を着地と誤認）の余地は
- (2) processing 中を外すのは当然として、dead（再試行上限で FAILED になったもの）を外すべきか。着地が確認できた dead を FAILED のまま残すのは正しいか
- 終端ガードの原則（FAILED は終端）との整合：reconcile の回復を例外として認める条件（台帳に記録あり・理由が ambiguous／unverifiable 等）をどう線引きするか
