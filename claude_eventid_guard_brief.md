# 実装依頼：event-id の付け忘れ・打ち間違い防止（候補27の派生）

agy のレビュー（~/workspace/slack-yama/agy_eventid_guard_out.md）を受けて、Muse が次の方針を決めた。実装だけを依頼する。

## 入れるもの
1. **reply の二択必須**：reply は `--event-id <ID>` と `--no-event-id` のどちらか必須にする。どちらも無ければエラーで止める（非0終了）。これが付け忘れ防止の本体。共有・報告の投稿は --no-event-id を付ける正規の形になる。
2. **--event-id の実在確認**：--event-id が渡されたら、events テーブルに実在するか確認し、無ければエラーで止める（非0終了）。--force を付けたときだけ通す（--force には --reason も必須、終端ガードの訂正と同じ形）。
3. **読む側のフォールバックの見直し**：`_pre_post_check_thread_for_reply` と `_scan_thread_yama_messages` の `_do_page_call` にある、TypeError 時の include_all_metadata を外す呼び直しについて、本番の slack_sdk が include_all_metadata に対応しているか実地で確認すること。対応しているならフォールバック分岐自体を削除する。削除できない理由があるなら、TypeError の捕捉条件を include_all_metadata 起因に絞り、警告を stderr ではなく監視可能なログレベル（WARNING）に出すこと。

## あわせて直すもの
- フック定義 ~/hooks/definitions/slack-yama-dispatch.json のプロンプト文中、reply の呼び方（--event-id を付ける指示）と共有投稿の呼び方を、新しい二択に合わせて書き換える。ワークスペース側のコピー（hooks/slack-yama-dispatch.json）も同じ内容に同期する。定義 JSON の編集後、JSON として正しいことを確認すること
- プロジェクト内の reply の呼び出し箇所（dispatcher.sh、フックスクリプト、ドキュメント内の例）をすべて新しい二択に合わせて直す。呼び出し箇所の洗い出しは実装前に grep で行うこと

## 制約
- 既存テストは全件通すこと（`./venv/bin/python tests/run_tests.py`）。新しく要るテスト：--event-id も --no-event-id も無い呼び出しは失敗すること、実在しない --event-id は失敗し --force で通ること、--no-event-id の投稿は台帳に記録されないこと、フォールバック見直しの結果に応じたテスト
- IMPROVEMENTS.md に候補27の派生として「event-id 付け忘れ防止（reply の二択必須＋実在確認＋読み取りフォールバック見直し）」を新候補（候補28）として追加し「対応済み」とすること
- 変更前の原本は bak-2026-11-23-eventid/ に退避すること
- 成果物として実装レポートを ~/workspace/slack-yama/claude_eventid_guard_2026-11-23.md に書くこと
- Slack への投稿、GitHub への push はしない（Muse が別途行う）
