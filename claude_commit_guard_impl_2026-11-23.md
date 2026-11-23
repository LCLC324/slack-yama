# commit_event の終端ガード（第1段）実装レポート（2026-11-23）

依頼：調査レポート `claude_commit_guard_investigation_2026-11-23.md` §4-2 の第1段を実装する。あわせて §4-4 の小さな点を2つ直す。
結果：**テストは 82/82 通過**（既存の81件はすべて無変更で通過、新規1件）。Slack への投稿と GitHub への push はしていない。

---

## 0. 要点
- `commit_event` の `BEGIN IMMEDIATE` の中で、行を SELECT した直後に状態を1回だけ判定するようにした（agent_tools.py:611〜）。commit・commit-batch・cmd_reply の内部 commit は、すべてここを通る。
- 拒否しても例外にはしない。行は変えず、現在の行に `"noop": true, "rejected": "already_terminal"` を付けて返し、stderr に警告を出し、運用アラートの outbox（`state/ops_alerts.jsonl`）に1行書く。CLI の `commit` はこのとき exit 1 になる。
- **すぐに効く点に注意**：フックはワークスペースの `agent_tools.py` を直接実行するので、配布の手順はなく、次の周期から新しい動作になる。フックのプロンプト（`~/hooks/definitions/slack-yama-dispatch.json`）と運用アラートの説明文（`~/hooks/scripts/slack-yama-dispatch.sh`）も稼働側を直接書き換え、ワークスペースの `hooks/` に同期した（`cmp` で一致を確認済み）。

## 1. 遷移表（実装）

| 現在 | commit | 結果 | 戻り値／CLI |
|---|---|---|---|
| PENDING／PROCESSING | 何でも | 従来どおり（リトライ・unverifiable の待ちも同じ） | 従来どおり |
| DONE／IGNORED | 同じ状態 | **no-op**。行は変えない（error_message・updated_at もそのまま）。アラートなし | `"noop": true`、exit 0 |
| DONE／IGNORED | 別の状態 | **拒否**。行は変えない。運用アラート `commit_rejected` を1行 | `"rejected": "already_terminal"`、exit 1 |
| FAILED | 何でも（FAILED→FAILED を含む） | **拒否**（同上）。Reaper の上限で止めた `reaped_too_many_times` を FAILED commit で外すこともできなくなった | 同上 |
| 終端 | 別の状態＋`--force --reason <文>` | **許可**。通常の commit と同じ規則で書き込む。error_message は `[forced 旧->新] 理由`（`--error` があれば `<error> [forced 旧->新] 理由`）。運用アラート `commit_forced` を1行 | `"forced": true`、`previous_status` |

- 判定は `classify_commit_transition()`（agent_tools.py:499）の1か所にまとめ、commit-batch の検証でも同じ関数を使う。
- `--force` に理由が無い（空白だけを含む）場合は ValueError（CLI は exit 1）になり、何も書かない。
- 同じ状態への commit は、`--force` を付けても no-op のまま（理由を書き換えるための経路にはしていない）。
- `--force` で FAILED を指定した場合は、通常の FAILED の規則（retry_count が残っていれば PENDING）に従う。訂正の主な用途は 11-23 型の IGNORED→DONE。

## 2. 変更点

### agent_tools.py
- 定数 `TERMINAL_STATUSES`・`COMMIT_REJECTED_CODE="already_terminal"`（:63〜）。
- `_ops_alerts_path()`／`_append_ops_alert()`（:466〜）：backfill.py と ops_monitor.py と同じ形式で、flock を取って JSON を1行追記する。宛先は `YAMA_OPS_ALERTS_PATH`、未指定なら DB と同じディレクトリの `ops_alerts.jsonl`。書き込みに失敗しても例外は出さず、stderr に `ALERT_WRITE_FAILED` を出す（commit の結果は変えない）。
- `commit_event()`（:518）：引数 `force`、`reason`、`source` を追加（既定値は従来の動作と同じ）。判定はトランザクション内で読んだ行に対して行うので、判定と書き込みの間で競合しない。
- `cmd_commit()`（:3378）：`--force`／`--reason` を追加。拒否のときは stderr に `{"ok": false, "error": "already_terminal", "noop": true, "current_status", "requested_status", "message"}` を出して exit 1。no-op のときは `"noop": true` を付けて exit 0。
- `commit_batch()`（:3424）：
  - 各件に `"force"`／`"reason"` を指定できるようにした（`force` は true のときだけ有効で、理由が要る）。
  - 全件一括の検証で現在の状態も見るようにした。拒否される件が1件でもあれば何も commit せず、`errors` に加えて `"already_terminal": [event_id...]` を返し、運用アラートを1行（`source: commit-batch-validation`、対象一覧つき）書く。
  - 同じ状態の件は検証を通し、書き込みの段で no-op になる（結果に `"noop": true`、集計に `noop` 件数）。`committed` は実際に書いた件数で、no-op は数えない。
  - 検証と書き込みの間に状態が変わった場合は、commit_event 側の再確認で `already_terminal` として失敗扱い（exit 1）になる。
- `cmd_reply` の「他者が回答済み」経路（:2837 付近）：拒否されても今までどおり投稿は止めて exit 0 で返すが、出力の `status` は commit_event が返す実際の値（拒否のときは現在の行の値）になる。拒否のときは `"commit_rejected": "already_terminal"` を付ける。例外のときは stderr に警告を出し、`get_event_by_id` で読み直した値を返す。
- reconcile（§4-4 の2点目）：`is_reconcile_clearable_error()`（:1806）を追加し、DONE の error_message を消す候補を「返信・投稿経路の技術的エラー（reply／ambiguous／unverifiable と、そこに付く IncompleteRead・max attempts・transient・socket・timeout・connection・http・Slack API・error・exception）」に限った（:2029）。判定理由（「情報のない往復（ループ防止）」など）と `[forced ...]` の訂正の注記は候補に入らないので、消えず、スレッドの走査もしない。
  - 既存テストの残存エラーの文言（`IncompleteRead(...)`・`max attempts exceeded`・`transient_socket_warning`・`residual error` 等）は、すべて消す側に入る。依頼文の「reply／ambiguous／unverifiable 系」をそのまま字句で絞ると既存テスト22など（`IncompleteRead(64 bytes read)`・`max attempts exceeded` が消えることを確かめている）が壊れるため、投稿経路の技術的エラーまで含めた。
  - 実データで確認（読み取り専用）：現在 DONE で error_message が残っている4件はすべて「情報のない往復（ループ防止）」で、新しい規則では残る側。IGNORED の理由と decisions ファイルの理由の計21件で、消す側に誤判定されるものは0件。

### dispatcher.sh（§4-4 の1点目）
- 「hook の script_timeout_secs（600s）より十分長い 900s」という2か所のコメントを直した。600s はフックのスクリプトの上限で、起こされたワーカー（LLM のセッション）には打ち切りが無いこと、900s は実測のバッチ所要時間（p99 300s、最大 930s）に対する余裕であること、遅れた commit は終端ガードで確定済みの件を戻せなくなったが、同じ PROCESSING の保持者どうしは区別できない（第2段）こと、を書いた。コードの変更はない（`bash -n` 通過）。

### フック（稼働側を直接編集し、ワークスペースに同期）
- `slack-yama-dispatch.json` のプロンプト、「処理に失敗したイベントは FAILED で commit する。」の直後に1文追加：
  > commit（commit-batch を含む）が `already_terminal` を返したら、それは他の処理で確定済みということ。その件は再試行せず報告に1行含める（commit-batch では `already_terminal` の件だけを外して残りを再実行する。同じ状態の commit は `noop` の成功で、確定済みの件の訂正だけは `--force --reason <理由>` を使う）。
  - 括弧の中は依頼の1文への補足。commit-batch は1件でも拒否されると全件が止まるので、残りの件の進め方を書いておかないとワーカーがバッチ全体を諦めるおそれがあると判断した。不要なら括弧ごと消してかまわない。
- `slack-yama-dispatch.sh` の運用アラートの wake 用 note に、`commit_rejected` と `commit_forced` の説明を足した（ops 側で種類の意味が分かるように）。`bash -n` 通過。テスト68（稼働中のフックのブロックを実行するテスト）も通過。
- 注意：AGENTS.md には hooks.dry_run での検証の記録があるが、この環境ではそのツールを呼べないため、ドライランはしていない。JSON の構文チェック（`json.loads`）とテスト68で代わりに確かめた。

### tests/run_tests.py
- 新規テスト **82. `test_commit_terminal_guard`**（既存の番号付きの並びの末尾に追加）：
  1. PENDING／PROCESSING は従来どおり（FAILED→PENDING のリトライ、DONE の確定）。アラートなし
  2. DONE→DONE・IGNORED→IGNORED は no-op（行全体が不変、CLI は exit 0 で `noop`）。アラートなし
  3. DONE→FAILED／IGNORED、IGNORED→DONE は拒否（行は不変、CLI は exit 1 で `already_terminal`、アラート1行ずつ、内容の確認）
  4. FAILED→DONE／FAILED／IGNORED は拒否。`reaped_too_many_times` を FAILED commit で外せないこと
  5. `--force` は理由が無いと拒否（空白だけの理由も拒否）、理由があれば IGNORED→DONE に訂正（error_message の書式、`commit_forced` アラート）
  6. commit-batch：終端の件が1件あれば何も commit しない（`already_terminal` 一覧、集約アラート）／同じ状態の件は no-op として通る／force は理由が要る・理由があれば訂正できる／検証の後に状態が変わっても commit_event が再確認して拒否する
  7. FAILED →requeue→ PENDING →（dispatcher 相当）PROCESSING → DONE が通ること
  8. cmd_reply の「他者が回答済み」経路で、確認中に他の処理が IGNORED に確定させた場合、出力の `status` が "IGNORED" になり、`commit_rejected` が付くこと
  9. reconcile が判定理由の残った DONE を走査せず、error_message を消さないこと（Slack のクライアントに一切触れないことも確認）。消す側／残す側の判定の単体確認
- 依頼の注意どおり、Neiro 側が期待している `test_slack_event_state_done_guard.py` という別ファイルは作っていない。既存の `tests/run_tests.py` の並びに沿って追加した。

### IMPROVEMENTS.md
- 候補19を「対応済み（2026-11-23）」に更新し、調査の要旨と、範囲外として残したもの（下の §4）を明記した。

## 3. 確認したこと
- 変更前：`./venv/bin/python tests/run_tests.py` → 81/81。
- 変更後（ガード・CLI・batch・reply・reconcile・フックの変更を入れた状態で、既存のテストだけ）→ 81/81。調査時の試作と同じく、既存のテストは無変更で通った。
- 新規テストを足した後 → **82/82**。
- 本番 DB（`state/slack_events.db`）は読み取り専用で照会しただけで、書き込みはしていない。テストはすべて一時ディレクトリの DB とアウトボックスを使う（テストの間は `YAMA_OPS_ALERTS_PATH` を外して元に戻す）。

## 4. やらなかったこと・残っていること
- **第2段の claim トークン**：依頼どおりやっていない。本命の競合（A の遅れた commit が、B が保持している PROCESSING を上書きする）と、遅れた reply による二重投稿は、第1段では防げない。
- **候補19の後半**（unverifiable 以外の FAILED で first_unverifiable_at を消す）：今回の依頼に含まれないため手を付けていない。IMPROVEMENTS.md にもその旨を書いた。
- **reconcile による「着地を確認できた FAILED」の DONE 化**：今までどおり status は変えない。将来やるときは commit を流用せず、専用の関数にする（調査レポート §4-2 のとおり）。
- **requeue `--allow-terminal`**：DONE／IGNORED のやり直しの実例がまだ無いので、足していない。今は `--force` で訂正できる。
- **運用アラートの量**：拒否のたびに1行書かれ、フックがまとめて ops に通知する。拒否は今まで記録上0件だったので、通知が煩くなることはまず無いと見ているが、頻発したら集約を検討する。

## 5. 退避と切り戻し
- 変更前の原本：`bak-2026-11-23-commitguard/`（agent_tools.py、dispatcher.sh、IMPROVEMENTS.md、run_tests.py、slack-yama-dispatch.json、slack-yama-dispatch.sh）。
- 切り戻すとき：`agent_tools.py`・`dispatcher.sh`・`tests/run_tests.py` をワークスペースに戻し、2つのフックファイルを稼働側（`~/hooks/definitions/`、`~/hooks/scripts/`）とワークスペースの `hooks/` の両方に戻す。DB のスキーマは変えていないので、データの移行も戻しも要らない。
