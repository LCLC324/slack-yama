root agent idle; waiting for 1 background task(s) to finish
テストを実行中です。完了次第、結果を確認して報告します。
重大欠陥2件の修正およびテスト更新・追加を完了し、全66件のテストが通過しました。

### 1. 変更ファイル一覧と行の目安

1. [slack-yama-dispatch.sh](file:///home/hatch/hooks/scripts/slack-yama-dispatch.sh#L191-L303)（L191〜L303）
   - 同一ディレクトリ一時ファイル → `bash -n` 検査 → `os.replace` のアトミック手順で置換（権限 `0755` 維持）。
   - `syntax_guard_eval`（L206〜L238）から `warned=true` の保存を分離し、期限到達時は `"warn:<target>"` の返却のみに変更。
   - `syntax_guard_mark_warned`（L239〜L252）を新設し、`first_fail` を保持したまま `warned=true` を保存。
   - 警告・復旧の `wake` ディスパッチ処理（L254〜L302）を `# --- syntax-guard escalation: begin`〜`# --- syntax-guard escalation: end ---` マーカー内へ移動。
   - 警告・復旧ペイロードを `jq -n -c --argjson` で組み立て、`wake` および `jq` を `if` 条件内で実行（`|| true` なし）。成功時のみ対象全件に `syntax_guard_mark_warned` を呼び、失敗時は `state/watchdog.log` へ1行記録して後段処理を続行。
2. [owner_dm_poll.py](file:///home/hatch/workspace/slack-yama/owner_dm_poll.py#L6-L359)（L6〜L359）
   - `state_lock()`（L43〜L54）を追加し、`STATE_PATH + ".lock"` を `fcntl.flock(LOCK_EX)` で排他ロック。`poll` / `resolve` / `send`（および `init_state`）の読み込み→変更→保存をロック内で実施。`save_state` は既存の `tmp + os.replace` を維持。
   - 会話状態に `proxy_ts`（直近50件上限・文字列リスト、キー未存在時は空リストの後方互換）を追加（`_record_proxy_ts`: L57〜L63）。
   - `extract_sent_ts`（L78〜L127）・`send_dm`（L130〜L147）・`send`（L150〜L180）および `send` サブコマンド（L342〜L359）を追加。`auto_streak >= 3` で `{"ok": false, "error": "auto_streak_limit"}`＋終了コード1、`result.content[0].text` 内の JSON `"ts"` および `"Message TS: ..."` の両方から ts を抽出して `proxy_ts` へ保存、抽出不可時は送信済み可能性を明記した `{"ok": false, "error": "ts_not_found", "message": ...}`＋終了コード1、`auto_streak` 加算なし。
   - `resolve`（L284〜L309, L341, L354）に `--reply-ts` を追加し、`action == "replied"` 時に `proxy_ts` へ記録。
   - `poll` のリセット判定（L250〜L257）を前回水位（`prev_top`）より新しい全メッセージ走査に変更し、`OWNER_ID` かつ `proxy_ts` に含まれない ts が1件でもあれば `auto_streak = 0` にリセット（水位前進・pending 検出ロジックは不変）。
3. [slack-owner-dm-dispatch.json](file:///home/hatch/hooks/definitions/slack-owner-dm-dispatch.json#L6)（L6 `prompt` の手順4）
   - 手順4の送信方法のみを `owner_dm_poll.py send`（thread 時は `--thread-ts`、`auto_streak_limit` 時は `resolve skipped`、不可時のみ従来コネクタ送信＋`resolve --reply-ts`）へ更新。他の手順・文言・JSON 構造は不変（一時ファイル＋JSON検証＋アトミック置換）。
4. [run_tests.py](file:///home/hatch/workspace/slack-yama/tests/run_tests.py#L8841-L9294)（L34, L8841〜L9294, L9373〜L9375）
   - **テスト64（更新）** `test_hook_syntax_guard_escalation`（[run_tests.py](file:///home/hatch/workspace/slack-yama/tests/run_tests.py#L8841-L9055)）：begin〜end の全ブロックを `set -euo pipefail`＋スタブ化環境で実行し、`jq -e` による警告/復旧ペイロード JSON と型検証、`wake` 失敗時の `warned` 未保存・再試行・`watchdog.log` 記録・ブロック後段のダミー処理続行、成功時のみの `warned=true` 保存と重複抑止、10分未満静観・復旧・対象独立・破損状態耐性を検証。
   - **テスト66（新設）** `test_owner_dm_poll_proxy_ts_and_auto_streak`（[run_tests.py](file:///home/hatch/workspace/slack-yama/tests/run_tests.py#L9058-L9294)）：`owner_dm_poll` を import し `read_dm` / `send_dm` をスタブ化して、代行3連続（1→2→3・`auto_streak_limit` 拒否と終了コード1）、`ts_not_found`、水位より後のオーナー本人投稿（最新1件目以外を含む）による0リセット、`resolve --reply-ts` 予備経路、`proxy_ts` 50件上限切捨て、末尾ゼロ付き ts の文字列完全一致、`proxy_ts` 欠落状態の後方互換、`.lock` ファイル生成を検証。

### 2. テスト実行結果

- 構文・JSON 検査：`bash -n /home/hatch/hooks/scripts/slack-yama-dispatch.sh`、`jq -e`、`py_compile`（`owner_dm_poll.py`, `tests/run_tests.py`）すべて成功（exit 0）。
- `./venv/bin/python tests/run_tests.py`：**66/66 passed, 0 failed**（既存65件＋新設1件、全件通過）。

### 3. 判断に迷った点

- **修正1の `first_failed_at_epoch` 算出対象**：既存の `[.syntax_guard[] | .first_fail] | min // 0` は既に警告済みの別ターゲットの `first_fail` も含める形だったため、今回警告対象となる `$targets` に絞って最小値を取る形（`[(.syntax_guard // {}) as $sg | $targets[] | $sg[.].first_fail | select(type == "number")] | min // 0`）にしました。単一・同時複数ターゲット時は従来と同一結果で、別ターゲットが後から遅れて10分到達した際も実態と一致します。
- **修正2の `send` における `state_lock()` の保持範囲**：Slack CLI 送信中も排他ロックを保持する設計にしました。送信前にロックを解放すると「Slack 送信完了〜`proxy_ts` 保存」の隙間に定期 `poll` が割り込んでオーナー本人発言と誤認し `auto_streak` を0に戻すレースが起き得るため、チェック→送信→`proxy_ts` 保存までを同一ロック内で完結させています。
- **修正2の `slack-owner-dm-dispatch.json` 手順4の既存箇条書き**：手順4にあった「送信の直前に会話をもう一度読み、Owner 本人が新着より後に発言していたら送信をやめて resolve skipped にする」は仕様の「他の手順・文言は変えない」に従いそのまま残しています。
