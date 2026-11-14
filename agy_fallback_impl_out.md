`reconcile-replies` の推定フォールバックを廃止し、正規化文面（`intended_text`）が一致する未割当投稿のみを台帳（`replies`）に回収するよう更新しました。

### 変更内容
1. **[agent_tools.py](file:///home/hatch/workspace/slack-yama/agent_tools.py)**
   - [`_select_matching_yama_message`](file:///home/hatch/workspace/slack-yama/agent_tools.py#L925-L953):
     - 「対象イベントより後にある最も早い未割当投稿（`unassigned_after[0]`）」への推定フォールバックを完全削除しました。
     - `intended_text` が存在しない（`NULL` または空白のみ）場合は即座に `None` を返し、推定割当を行わないようにしました。
     - `intended_text` がある場合も、正規化文面（CRLF→LF、`&amp;`/`&lt;`/`&gt;` デコード、前後空白除去）が一致する未割当の自ボット投稿のみを返し、一致がなければ `None`（未解決のまま残す）を返すようにしました。
   - [`reconcile_replies`](file:///home/hatch/workspace/slack-yama/agent_tools.py#L956-L1206):
     - Phase 1・Phase 2 ともに `_select_matching_yama_message` の文面一致のみで `replies` 台帳に記録します。
     - Phase 2 の DONE 残存エラー消去では、既に `replies` 台帳に記録済みの `reply_ts` がスレッド内で確認できた場合、または今回 `intended_text` の正規化一致で台帳記録できた場合のみ `done_error_cleared["with_reply"]` とし、文面不一致や `intended_text` なしの場合は推定で記録せず `done_error_cleared["without_reply"]` としてエラー文言のみ消去します。

2. **[tests/run_tests.py](file:///home/hatch/workspace/slack-yama/tests/run_tests.py)**
   - [`test_reconcile_intended_text_matching_and_fallback` (Test 39)](file:///home/hatch/workspace/slack-yama/tests/run_tests.py#L5756-L5909): 文面一致による選択・HTMLエスケープ等の正規化一致を維持しつつ、文面不一致時は最古未割当へフォールバックせず未記録（Phase 1 は `not_found`、Phase 2 は `without_reply` でエラー消去のみ）となる検証に更新しました。
   - [`test_reconcile_no_intended_text_never_estimated` (Test 40)](file:///home/hatch/workspace/slack-yama/tests/run_tests.py#L5912-L6019): `intended_text` がない（`NULL`／空白のみ）イベントが未割当の自ボット投稿が後にあっても推定で記録されないこと（Phase 1・Phase 2 両方）、および台帳記録済み DONE イベントの `ts` 完全一致確認（`with_reply`）が維持されることのテストを追加しました。
   - 既存の reconcile 関連テスト（Test 19, 20, 22, 24, 27）も `intended_text` 照合仕様に合わせて更新しました。

### テスト実行結果
全 40 件のテストを実行し、すべてパスしました（**40/40 passed, 0 failed**）。
