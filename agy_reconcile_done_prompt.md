# 依頼：reconcile の DONE残存エラー照合を「網羅形」に広げてください

対象プロジェクト：~/workspace/slack-yama（Yama Slack ボット）

## 背景
先ほど reconcile-replies に追加したフェーズ（status='DONE' かつ error_message 残存のイベントをスレッド走査し、Yama の返信が見つかれば台帳記録＋error クリア、見つからなければ error を残して unresolved）について、Neiro から指摘がありました。

Neiro 側の整理（slack-reconcile-done.sh）では、done になったイベントの last_error は「中身を問わず古い残骸」として網羅的に消しています。実際、Yama 側のイベントにも「max attempts exceeded」や旧ボットガード時代の判断理由など、返信が存在しない DONE＋error 残存が多数あり、現行の「返信が見つかった場合だけクリア」ではそれらが永遠に残ります。DONE は処理完了を意味するので、error_message の残存自体が誤記録という考え方です。Owner 承認済みの「網羅的な定期照合」の範囲として、この形に寄せます。

## 実装内容（agent_tools.py の reconcile-replies、DONE＋error フェーズのみ変更）
1. 候補（status='DONE' かつ error_message IS NOT NULL、直近 --days 日）は従来通りスレッドを全体走査する。
2. Yama の返信が見つかった場合：replies 未記録なら record_reply() で記録する（従来通り）。
3. **error_message のクリアは、返信の有無にかかわらず行う**（DONE＝完了済みのため残存エラーは古い残骸として消す）。ただしスレッド走査自体が履歴取得エラーで完了しなかった候補はクリアせず、次回の照合に残す（errors として数える）。
4. 出力 JSON は、done_error_cleared を「返信が見つかってクリアしたもの（with_reply）」と「返信なしでクリアしたもの（without_reply）」に分けて報告する形に変更する（done_error_unresolved は廃止し、走査エラー分は errors / error_details に入れる）。
5. --dry-run では記録もクリアもせず予定だけ返す（従来通り）。
6. reconcile 本来の候補フェーズ（DONE/FAILED×台帳未記録×error に reply/ambiguous）は変更しない。

## テスト（tests/run_tests.py）
- 既存テスト（(c) 関連、テスト22想定）の期待を新仕様に合わせて更新：DONE＋error＋返信なし → error がクリアされ without_reply に入る。
- 追加：DONE＋error＋スレッド走査が履歴取得エラー → error は残り、errors に数えられる。
- 既存テストもすべて通過させること。

## 注意
- 実際の state/slack_events.db には触らないこと（テストは一時 DB とモックで）。
- config/.env の内容は表示・複製しないこと。
