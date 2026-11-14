案2（投稿メタデータ）＋案4（試行時刻の窓での回収）の実装およびテスト追加を完了しました。

## 変更概要

### 1. [agent_tools.py](file:///home/hatch/workspace/slack-yama/agent_tools.py)
- **投稿メタデータ付与（案2）**
  - `_build_reply_metadata` を追加し、`cmd_reply` での `chat.postMessage` 呼び出しに `metadata={"event_type": "yama_reply", "event_payload": ...}` を付与するようにしました。
  - `event_id` がある場合は `{"event_id": event_id}`、手動投稿など `event_id` がない場合は `{"request_id": "<UUID4>"}` を `event_payload` に格納します。
  - スレッド・チャンネル履歴の読み取り箇所（投稿前チェック `_pre_post_check_thread_for_reply` / `_lookup_own_recent_message_once`、投稿後の回復確認 `_find_own_recent_message`、`reconcile_replies` の `_scan_thread_yama_messages`、および `fetch_history`）すべてで `include_all_metadata=True` を指定するようにしました。
- **試行時刻の記録と前後窓での回収・照合優先順位（案4）**
  - `init_db` に冪等な `ALTER TABLE events ADD COLUMN attempt_ts TEXT;` 移行を追加し、`ambiguous_post_outcome` 発生時に `_save_event_intended_text` で送信文面（`intended_text`）とともに試行時刻（`attempt_ts`）を記録するようにしました。
  - 投稿後の回復確認（`_find_own_recent_message`）および `reconcile_replies`（`attempt_ts` 記録ありのイベント）の走査範囲を `_compute_recovery_window` により **試行時刻の前3分〜後5分**（`[attempt_ts - 180s, attempt_ts + 300s]`）に絞りました。
  - 照合の優先順位を刷新しました（`_select_matching_yama_message` / `_lookup_own_recent_message_once`）：
    1. **第1優先**：`metadata.event_type == "yama_reply"` の `event_payload.event_id`（または `request_id`）完全一致（Slack 側で文面整形が入っても回収可能）
    2. **第2優先**：従来の正規化文面一致（メタデータ導入前の旧投稿に対する後方互換。別イベントの `yama_reply` メタデータを持つ定型文は除外し、不一致時の推定フォールバックは行わない）
  - 投稿前チェックおよび答え済み判定の範囲・条件（`oldest = event_ts - 60s` または `thread_ts`、fail-closed 維持）は変更していません。

### 2. [tests/run_tests.py](file:///home/hatch/workspace/slack-yama/tests/run_tests.py)
- 既存テスト（11, 14, 38）のモック時刻および `attempt_ts` 保存検証を更新しました。
- 新規テスト 4件（46〜49）を追加しました：
  - **46 (`test_reply_metadata_attachment_and_include_all_metadata`)**：`event_id` あり／なし投稿時の `metadata`（`event_type="yama_reply"` + `event_id` / `request_id`）付与と、各読み取り経路での `include_all_metadata=True` 指定の確認
  - **47 (`test_recovery_and_reconcile_by_metadata_match`)**：Slack による文面整形で本文が変わっていてもメタデータ一致（`event_id` / `request_id`）で回復確認・`reconcile_replies`（Phase 1 / Phase 2）ともに回収でき、文面一致より優先されることの確認
  - **48 (`test_recovery_and_reconcile_attempt_window_boundaries`)**：試行時刻の窓（前3分〜後5分）の境界検証（窓外の同一メタデータ投稿は回収せず、窓境界内の投稿は回収すること）
  - **49 (`test_legacy_post_without_metadata_text_fallback`)**：メタデータのない旧投稿の文面一致回収維持と、別 `event_id` メタデータを持つ同一文面（定型文）の誤回収防止の確認

## テスト実行結果
- 全件実行結果：**49 / 49 件 PASS（失敗 0 件）**
