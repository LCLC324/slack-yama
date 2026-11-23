# watchdog しきい値の文言修正（候補15）実装レポート（2026-11-23）

自己レビューの候補15。Neiro・Yanayana ともに賛成済み。

## 変更内容（文言のみ、送信挙動は不変）
- 稼働側の定義 ~/hooks/definitions/slack-yama-dispatch.json のプロンプト文中の「heartbeat が3分以上古ければ」→「heartbeat が120秒以上古ければ（watchdog.sh の THRESHOLD_SEC=120 と同じ）リスナーを再起動し」に更新。Neiro の追加提案「プロンプトの数字＝watchdog の数字の対応が一目で分かる形にする」を取り入れた
- 稼働側のフックスクリプト ~/hooks/scripts/slack-yama-dispatch.sh のコメント（110行目）「heartbeat has been stale for more than 3 minutes」→「heartbeat has been stale for 120 seconds or more (= watchdog.sh THRESHOLD_SEC=120; it re-checks after RECHECK_SEC=5 before restarting and ...」に更新。THRESHOLD_SEC=120／RECHECK_SEC=5／RESTART_COOLDOWN_SEC=60 との対応つきの表記
- watchdog.sh 自体の THRESHOLD_SEC=120 は変更なし（既定120で正しい）
- ワークスペース側のコピー（hooks/slack-yama-dispatch.json、hooks/slack-yama-dispatch.sh）も同じ内容に同期済み

## 検証
- 定義 JSON：python -m json.tool 通過
- フックスクリプト：bash -n 通過
- 既存テスト：84/84 通過（文言変更のためテストの変更なし）
- IMPROVEMENTS.md の候補15を「対応済み」に更新済み

## 備考
- 実施中にランタイムの再起動で実行セッションが中断したが、変更自体はすべて適用済みであることを Muse が実地で確認した。このレポートは、Muse が検証結果をもとに補完して書いた
- 手動の対話確認（Yama が仕組みを聞かれたときに120秒と答えること）は未実施
