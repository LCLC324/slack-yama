agy レビュー（2026-11-23、Gemini 4 Neon Medium）の要旨：
- 結論：「条件付きで入れる」を推す
- 理由：Reaper の差し戻し経路がある以上、現在状態を確認しない無条件 UPDATE は明確な設計上の欠陥。実害は今なくても、タイムアウト・再起動・将来の並列化が絡んだ瞬間に静かなデータ不整合を起こす。SQLite の WHERE 句1つで防げるため、放置する理由がない
- ただし「DONE だけ弾くガード」は不十分。PROCESSING／PENDING の上書きが残り、requeue／reconcile との責務混在も悪化する
- 導入の3条件：(1) ブラックリスト（!= 'DONE'）ではなくホワイトリスト（status = 'PROCESSING'、可能なら attempt または lease_token の一致）でガードする。(2) requeue／reconcile を commit_event とは別経路（別の許可遷移条件）に分離する。(3) ガードに弾かれた場合（rowcount == 0）は例外で落とさず no-op とし、必ず警告ログ（可能ならメトリクス）を出して観測可能にする
