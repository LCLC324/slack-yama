# 実装依頼：watchdog しきい値の文言を「3分」→「120秒」に直す（文言のみ）

自己レビューの候補15。Neiro と Yanayana ともに賛成済み。Yama はスレッドで「定義の文言を120秒に直す形で進める」「dispatch.sh のコメントも合わせて直す」と約束済み。文言のみの変更で、送信挙動は変えないこと。

## 直す箇所
1. 稼働側の定義 ~/hooks/definitions/slack-yama-dispatch.json のプロンプト文中の「heartbeat が3分以上古ければ」→「heartbeat が120秒以上古ければ」と直す
2. 稼働側のフックスクリプト ~/hooks/scripts/slack-yama-dispatch.sh 110行目付近のコメント「heartbeat has been stale for more than 3 minutes」→ 120秒の表記に直す
3. Neiro の追加提案「プロンプトの数字＝watchdog の数字の対応が一目で分かる形にする」を取り入れる。具体的には、定義のプロンプト文中に「（watchdog.sh の THRESHOLD_SEC=120 と同じ）」のように対応を明記する。コメント側も同様に THRESHOLD_SEC=120 との対応を明記する

## 手順の注意
- フックスクリプトの編集は一時ファイル＋構文検査（bash -n）＋mv のアトミック手順で行うこと。稼働側が正本。ワークスペース側のコピー（hooks/slack-yama-dispatch.json、hooks/slack-yama-dispatch.sh）も同じ内容に同期すること（scrub の自動同期が次回 push 時に走るが、ここで合わせておくこと）
- 定義 JSON の編集後、JSON として正しいことを確認すること（python -m json.tool 等）
- watchdog.sh 自体の THRESHOLD_SEC=120 は変えない（既定120で正しい）

## 制約
- 既存テストは全件通すこと（`./venv/bin/python tests/run_tests.py`）。文言変更のためテストの変更は不要なはず
- IMPROVEMENTS.md の候補15を「対応済み」として更新すること
- 成果物として実装レポートを ~/workspace/slack-yama/claude_wording_fix_2026-11-23.md に書くこと（短くてよい）
- Slack への投稿、GitHub への push はしない（Muse が別途行う）
