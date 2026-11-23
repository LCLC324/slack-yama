agy レビュー（2026-11-23、Gemini 4 Neon Medium）の要旨：
- 補足(1) 台帳の (channel, ts) 照合を追加する：取り入れない。reply_ts 基準では未記録の着地を拾う照合として成立せず、台帳に既にあるなら event_id で直接引けるため冗長。thread_ts を指しているなら誤爆の温床になる。metadata 欠落が課題なら、照合追加ではなく metadata の付与・取得側を修正すべき
- 補足(2) 回復対象から processing 中と dead を外す：条件付きで取り入れる（processing 除外のみ採用、dead 一律除外は不採用）。processing 中の除外は必須で、加えて原子的な条件付き更新（WHERE status = 'FAILED' 等）が必須。dead を一律除外すると「着地済みの dead が FAILED のまま残り、手動 requeue で二重返信を招く」問題が解決しない。除外条件は「dead かどうか」ではなく回復許可条件に置き換えるべき
- 終端ガードの例外として heal-replied を認める線引き：条件付きで取り入れる。ただし実例1件なら代替案Aの「requeue ブロック＋検知」で止めるのも有力。認める場合の必須条件：(1) 強照合のみ許可（metadata の event_id 完全一致、または台帳にその event_id が既記録。文面正規化一致だけでは FAILED→DONE にしない）。(2) 失敗理由の限定（送信結果不明 ambiguous／unverifiable／タイムアウト等、または送信後の後処理失敗に限定。送信前エラーの FAILED は対象外）。(3) 原子性と監査証跡（WHERE status = 'FAILED' 等の条件付き更新で競合を防ぎ、「reconcile により FAILED→DONE へ補正した」履歴を残す）
- 代替案A（終端ガードを完全に維持）：reconcile は従来通り「台帳記録のみ」に徹し、状態は変えない。代わりに「台帳に着地記録があるのに FAILED のイベント」を検知して通知し、手動 requeue 側で「台帳に既に着地あり」ならブロック／警告する。これなら状態遷移の例外を増やさず、二重返信も防げる
- 代替案B（発生源の修正）：着地後に FAILED になった根本原因（送信成功後の DB 更新失敗、タイムアウト時の結果不明ハンドリング、metadata 欠落）を先に塞ぐ
- processing 除外だけでは不十分：reconcile が FAILED を読んでから DONE に更新するまでの間に手動 requeue が走ると競合する。フィルタだけでなく条件付き更新が必須
