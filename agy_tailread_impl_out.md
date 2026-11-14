投稿前チェックおよび `reconcile-replies` におけるスレッド読み取り範囲を「対象イベントの `ts − 60秒` 以降」に限定する実装とテストの追加を完了しました。

### 変更内容

1. **スレッド読み取り起点（`oldest`）の限定**（[agent_tools.py](file:///home/hatch/workspace/slack-yama/agent_tools.py)）
   - [THREAD_SCAN_MARGIN_SECONDS](file:///home/hatch/workspace/slack-yama/agent_tools.py#L71)（`60` 秒）および [_compute_thread_scan_oldest](file:///home/hatch/workspace/slack-yama/agent_tools.py#L793-L809)、[_is_ts_at_or_after](file:///home/hatch/workspace/slack-yama/agent_tools.py#L812-L817) を追加しました。
   - **投稿前チェック・応答ロスト回復確認**（[cmd_reply](file:///home/hatch/workspace/slack-yama/agent_tools.py#L1626-L1698)、[_pre_post_check_thread_for_reply](file:///home/hatch/workspace/slack-yama/agent_tools.py#L1485-L1623)、[_lookup_own_recent_message_once](file:///home/hatch/workspace/slack-yama/agent_tools.py#L1951-L2056)、[_find_own_recent_message](file:///home/hatch/workspace/slack-yama/agent_tools.py#L2059-L2096)）：
     - `event_id` があり `events` テーブルから対象イベントの `ts` が引ける場合、`conversations.replies` の `oldest` を `event_ts - 60秒` に設定し、窓内のメッセージのみを対象に重複判定・答え済み判定（`event_ts` より後の他者回答）を行います。
     - `event_id` がない手動投稿（または `events` テーブルに起点イベントが存在しない共有ポスト）は、従来どおり `oldest=thread_ts` のスレッド全件走査を維持しています。
     - 窓内のページネーション（最大20ページ×50件）、ページ単位のリトライ（最大5回・指数バックオフ `1→2→4→8s`）、および読み取り失敗時の fail-closed（`pre_post_check_unverifiable`）を維持しています。
   - **reconcile のスレッド走査**（[_scan_thread_yama_messages](file:///home/hatch/workspace/slack-yama/agent_tools.py#L820-L902)、[reconcile_replies](file:///home/hatch/workspace/slack-yama/agent_tools.py#L1126-L1200)）：
     - Phase 1（未記録候補の照合）および Phase 2（`DONE` 残存エラー候補の照合）ともに、候補イベントの `cand_ts` を渡して `oldest = cand_ts - 60秒` を起点に走査するよう更新しました。

2. **テストの追加**（[tests/run_tests.py](file:///home/hatch/workspace/slack-yama/tests/run_tests.py#L6053-L6638)）
   - **Test 41**（[test_windowed_thread_dedup_after_event_ts](file:///home/hatch/workspace/slack-yama/tests/run_tests.py#L6053-L6186)）：イベント `ts` より後にある自分の同一文面を `oldest = event_ts - 60s` で検出してスキップすること、および `reconcile_replies` も同じ `oldest` 起点を使うことを確認
   - **Test 42**（[test_windowed_thread_ignores_past_event_template](file:///home/hatch/workspace/slack-yama/tests/run_tests.py#L6189-L6315)）：イベント `ts` より前（60秒マージン外）にある過去の別イベントへの同一文面（定型文）は重複とみなさず投稿すること、および `event_id` なしの手動投稿は全件走査を維持することを確認
   - **Test 43**（[test_windowed_thread_dedup_within_margin_before_event_ts](file:///home/hatch/workspace/slack-yama/tests/run_tests.py#L6318-L6412)）：イベント `ts` の数秒前（60秒安全マージン内）にある自分の同一文面投稿も検出してスキップすることを確認
   - **Test 44**（[test_windowed_thread_answered_after_event_ts](file:///home/hatch/workspace/slack-yama/tests/run_tests.py#L6415-L6543)）：答え済み判定がイベント `ts` より後の他者投稿で従来どおり働き、イベント `ts` 前の他者投稿では誤抑止しないことを確認
   - **Test 45**（[test_windowed_thread_failure_unverifiable](file:///home/hatch/workspace/slack-yama/tests/run_tests.py#L6546-L6622)）：絞った読み取りが5回リトライしても失敗した場合は `pre_post_check_unverifiable`（fail-closed）になることを確認

### テスト実行結果

`./venv/bin/python tests/run_tests.py` を実行し、既存40件＋新規5件の**全45件すべて PASS（45/45 passed, 0 failed）**であることを確認しました。
