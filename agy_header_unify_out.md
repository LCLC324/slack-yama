再送ヘッダーの3者統一形への変更および移行期の二重付加防止の実装・テストが完了しました。

### 変更ファイルと diff の要約

1. **[agent_tools.py](file:///home/hatch/workspace/slack-yama/agent_tools.py)**
   - **二重付加防止の正規表現 ([L81-L87](file:///home/hatch/workspace/slack-yama/agent_tools.py#L81-L87))**:
     `DELAYED_RETRY_PREFIX_RE` を新旧2形式の両方に明示対応するよう更新。
     ```diff
      DELAYED_RETRY_PREFIX_RE = re.compile(
     -    r"^（遅れての再送：最初の試行 \d{2}-\d{2} \d{2}:\d{2}）(?:\r?\n|$)"
     +    r"^(?:"
     +    r"遅れての再送（元の失敗時刻：\d{4}-\d{2}-\d{2} \d{2}:\d{2} JST）"
     +    r"|"
     +    r"（遅れての再送：最初の試行 \d{2}-\d{2} \d{2}:\d{2}）"
     +    r")(?:\r?\n|$)"
      )
     ```
   - **ヘッダー整形関数 ([L1231-L1255](file:///home/hatch/workspace/slack-yama/agent_tools.py#L1231-L1255))**:
     `format_delayed_retry_prefix()` の出力形式を `Asia/Tokyo` の `%Y-%m-%d %H:%M` を用いた3者統一形に変更。
     ```diff
          jst_dt = parsed_dt.astimezone(ASIA_TOKYO_TZ)
     -    return f"（遅れての再送：最初の試行 {jst_dt.strftime('%m-%d %H:%M')}）"
     +    return f"遅れての再送（元の失敗時刻：{jst_dt.strftime('%Y-%m-%d %H:%M')} JST）"
     ```

2. **[run_tests.py](file:///home/hatch/workspace/slack-yama/tests/run_tests.py)**
   - 既存の印関連テストを新形式に更新し、旧形式・新形式双方の二重付加防止テストケースを追加。

---

### 新規・更新テスト名とテスト結果

- **更新テスト**:
  - `33. test_delayed_retry_mark_prefix` ([run_tests.py#L4804-L5058](file:///home/hatch/workspace/slack-yama/tests/run_tests.py#L4804-L5058))
    - 遅延再送で新形式ヘッダー `遅れての再送（元の失敗時刻：YYYY-MM-DD HH:MM JST）` が1行だけ前置されること
    - 通常返信には付かないこと
    - 本文先頭に新形式ヘッダーが既にある場合は追加されないこと
    - 本文先頭に旧形式ヘッダー `（遅れての再送：最初の試行 MM-DD HH:MM）` が既にある場合も追加されないこと
  - `35. test_delayed_retry_not_answered_conservative_cases` ([run_tests.py#L5180-L5357](file:///home/hatch/workspace/slack-yama/tests/run_tests.py#L5180-L5357))
    - 期待される再送ヘッダー文言を新形式に更新
- **新規テスト**:
  - `37. test_delayed_retry_legacy_and_new_header_dedup` ([run_tests.py#L5472-L5530](file:///home/hatch/workspace/slack-yama/tests/run_tests.py#L5472-L5530))
    - `format_delayed_retry_prefix` / `apply_delayed_retry_prefix` / `DELAYED_RETRY_PREFIX_RE` における新形式生成、および新旧2形式（別時刻・LF/CRLF・単一行含む）での二重付加防止、本文途中に「遅れての再送」を含む通常文では正常にヘッダーが前置されることの単体検証

- **全テスト結果**: **37/37 passed** (`Summary: 37/37 passed, 0 failed.`)
