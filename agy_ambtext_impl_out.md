実装およびテストの追加が完了しました。

### 変更内容

1. **送信文面（`intended_text`）の保存**（[agent_tools.py](file:///home/hatch/workspace/slack-yama/agent_tools.py)）
   - [init_db](file:///home/hatch/workspace/slack-yama/agent_tools.py#L98-L110) にて `PRAGMA table_info(events)` を確認し、未作成の場合のみ `ALTER TABLE events ADD COLUMN intended_text TEXT;` を実行して既存DBへ冪等に移行する処理を追加しました。
   - [_save_event_intended_text](file:///home/hatch/workspace/slack-yama/agent_tools.py#L878-L896) を追加し、[cmd_reply](file:///home/hatch/workspace/slack-yama/agent_tools.py#L1828-L1836) で `ambiguous_post_outcome` になった場合のみ、実際に `chat.postMessage` へ渡した最終文字列 `post_text`（遅延再送ヘッダー付与後）を `events.intended_text` に保存します（通常成功・API失敗・即時回復時は保存しません）。

2. **reconcile の照合ロジック**（[agent_tools.py](file:///home/hatch/workspace/slack-yama/agent_tools.py)）
   - [_normalize_reconcile_text](file:///home/hatch/workspace/slack-yama/agent_tools.py#L899-L909) にて、改行コード統一（`\r\n` → `\n`）、HTMLエスケープデコード（`&lt;` / `&gt;` / `&amp;`）、前後空白のトリムを行います。
   - [_select_matching_yama_message](file:///home/hatch/workspace/slack-yama/agent_tools.py#L912-L942) を追加し、対象イベントに `intended_text` がある場合はスレッド内の対象イベント後の未割当 Yama 投稿の中から正規化文面が一致するものを最優先で選択し、一致しない場合（または `intended_text` がない場合）は従来どおり最も早い未割当投稿へフォールバックします。
   - [reconcile_replies](file:///home/hatch/workspace/slack-yama/agent_tools.py#L945-L1200) の phase1（[L1083-L1088](file:///home/hatch/workspace/slack-yama/agent_tools.py#L1083-L1088)）および phase2（[L1150-L1155](file:///home/hatch/workspace/slack-yama/agent_tools.py#L1150-L1155)）の両方の照合ループに適用しました。

3. **テスト追加**（[tests/run_tests.py](file:///home/hatch/workspace/slack-yama/tests/run_tests.py)）
   - [test_ambiguous_saves_intended_text](file:///home/hatch/workspace/slack-yama/tests/run_tests.py#L5535-L5731): ambiguous 発生時の `intended_text` 保存（通常および遅延再送ヘッダー付き）、通常成功・API失敗・即時回復での非保存、`ALTER TABLE` 移行の冪等性を検証。
   - [test_reconcile_intended_text_matching_and_fallback](file:///home/hatch/workspace/slack-yama/tests/run_tests.py#L5734-L5867): スレッド内Yama投稿2件で遅い方が `intended_text` と一致する場合の優先割当、HTMLエスケープ・CRLF・前後空白違いでの正規化一致、不一致時の最古未割当投稿へのフォールバック（phase1 / phase2 両方）を検証。

### テスト実行結果

`./venv/bin/python tests/run_tests.py` を実行し、既存テストを含む全 **39件（39/39 passed, 0 failed）** の通過を確認しました。
