遅延再送時の印付加および保守的な答え済み判定のコード固定化を実装し、全テストの通過を確認しました。

---

### 1. 変更ファイルと diff の要約

- [agent_tools.py](file:///home/hatch/workspace/slack-yama/agent_tools.py)
  - **対象イベント判定 (`get_event_by_id`, `format_delayed_retry_prefix`, `apply_delayed_retry_prefix`)** ([agent_tools.py:76-85](file:///home/hatch/workspace/slack-yama/agent_tools.py#L76-L85), [agent_tools.py:1202-1255](file:///home/hatch/workspace/slack-yama/agent_tools.py#L1202-L1255)):
    - `events` テーブルの対象 `event_id` に `first_unverifiable_at` が記録されている場合のみ「遅延再送 (`delayed_retry=True`)」と判定し、通常返信には印も答え済み判定も適用しない。
    - 遅延再送時は `first_unverifiable_at` を `Asia/Tokyo` で整形した1行 `（遅れての再送：最初の試行 {MM-DD HH:MM}）` を本文先頭に前置する。本文が既に同じ印（`DELAYED_RETRY_PREFIX_RE`）で始まっている場合は二重付加しない。
    - 結果JSONに `delayed_retry`（bool）、`retry_mark_added`（bool）、`retry_mark`（印文字列、遅延再送時）を出力する。
  - **保守的な答え済み判定 (`_is_bot_message`, `_find_answering_message_in_thread`, `_pre_post_check_thread_for_reply`)** ([agent_tools.py:1258-1420](file:///home/hatch/workspace/slack-yama/agent_tools.py#L1258-L1420)):
    - 既存のスレッド全体走査（`oldest=thread_ts`、ページ単位リトライ・最大20ページ）を再利用し、遅延再送時にスレッド親（`thread_ts`）より後の投稿を検査する。
    - ①投稿者が元の投稿者（質問者）でも Yama 自身でもない、②ボット投稿ではない（`bot_id` / `subtype="bot_message"` / `is_bot` / `bot_profile` / DB `is_bot=1` を除外）、③前後空白を除いた本文が10文字を超える（`len > 10`）のすべてを満たす投稿が1件でもあれば答え済みと判定する。
  - **`cmd_reply` 送信経路への統合** ([agent_tools.py:1423-1638](file:///home/hatch/workspace/slack-yama/agent_tools.py#L1423-L1638)):
    - 答え済みと判定された場合は `chat_postMessage` を呼ばず、`commit_event(event_id, "DONE")` によりイベントを `DONE`（`error_message` / `next_retry_at` / `first_unverifiable_at` クリア）へ遷移させ、`result="skipped_answered"` および判定に使った投稿の `ts`（`answered_by_ts` / `answered_ts`）を含めて JSON 出力（exit 0）する。
    - 履歴取得が全試行失敗した場合は、従来どおり `pre_post_check_unverifiable`（fail-closed、exit 1）経路に従う。

- [tests/run_tests.py](file:///home/hatch/workspace/slack-yama/tests/run_tests.py)
  - テスト33〜36（計4件）を新規追加 ([tests/run_tests.py:4804-5424](file:///home/hatch/workspace/slack-yama/tests/run_tests.py#L4804-L5424))。

---

### 2. 新規テスト名と全テストの結果

**新規追加テスト:**
1. `test_delayed_retry_mark_prefix` (`33. 遅延再送の印付加 (Asia/Tokyo整形1行前置・通常返信不付加・二重付加防止)`)
2. `test_delayed_retry_skipped_answered` (`34. 遅延再送の答え済み送信抑止 (他者10文字超回答でskipped_answered・判定ts出力・DONE遷移)`)
3. `test_delayed_retry_not_answered_conservative_cases` (`35. 遅延再送の答え済み非該当ケース (ボットのみ・10文字以下短文のみ・質問者追記のみ・Yama投稿のみ)`)
4. `test_delayed_retry_history_failure_unverifiable` (`36. 遅延再送の履歴取得失敗時 fail-closed (既存pre_post_check_unverifiable経路維持)`)

**全テスト実行結果:**
- **36/36 passed** (`0 failed`)
