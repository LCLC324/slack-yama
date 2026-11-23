# 実装レポート：監査（2026-11-23 夜）の指摘の修正

- 日付: 2026-11-23（23:15〜 JST）
- 実装: Claude Code（Opus 5.5）
- 元の監査: `claude_audit_2026-11-23_night.md`
- 対象外: 重大1件（自己レビュー cron の文面）。Muse が cron.update で対応済み
- 変更前の原本: `bak-2026-11-23-auditfix/`（agent_tools.py・ops_monitor.py・IMPROVEMENTS.md・tests/run_tests.py・hooks/slack-yama-dispatch.sh（ワークスペース側）・hooks/slack-yama-dispatch.sh.live（稼働側 ~/hooks/scripts/ の原本））
- Slack への投稿と GitHub への push はしていない。本番の state には書いていない（テスト後も `state/ops_alerts.jsonl` は0バイト、`ops_alerts_archive.jsonl` は1327バイトで、どちらも mtime 22:30 のまま）

## 結論

- テストは **86/86 で通過した**（`./venv/bin/python tests/run_tests.py`、所要1分55秒）。既存85件はすべて通過し、新しいテスト86を足した
- 直したのは6件（監査の指摘2・3・4・6・7・8）。記録だけしたのは3件（指摘5・9と、候補28の状態の記述）
- 新しいテスト86は、旧コード（`bak-2026-11-23-auditfix/` の agent_tools.py／ops_monitor.py）では落ちることを確かめた（逆向きの確認。§3）

## 1. 直したもの

### 1.1 metadata フォールバックの残り（指摘2）— agent_tools.py
- `_lookup_own_recent_message_once`:
  - conversations_replies：TypeError のとき、include_all_metadata を外して呼び直し、さらに latest／cursor も外して呼び直していた2段の分岐を削除した
  - conversations_history：TypeError のとき、channel と limit だけで呼び直していた（oldest／latest の窓も外れていた）分岐を削除した
- `_fetch_history_unbounded`：conversations_replies と conversations_history の、同じ形の分岐を両方削除した
- これで agent_tools.py には、TypeError で Slack API を呼び直す分岐は残っていない（`except TypeError` は0件。残っている `except (TypeError, ValueError)` は、数値や時刻の変換の分だけ）
- 旧コードの挙動（逆向きの確認で実測した）：厳密な偽クライアントで、スレッドは2回呼ばれ、2回目は `{'channel','ts','oldest','limit'}` だけになっていた（metadata と latest が外れる）。チャンネルは2回目が `{'channel','limit'}` だけになり、時間窓が外れていた
- 既存テストの偽クライアントの手直し（シグネチャだけ。テストの意図と assert は変えていない）:
  - テスト7・8・20：`conversations_history(self, channel, limit=20)`／`conversations_replies(self, channel, ts, limit=20)` の6か所に、`include_all_metadata: bool = False` を足した
  - テスト11（AmbiguousOutcomeMockClient）：`include_all_metadata`・`latest`・`cursor` を足した。回復確認は `latest` も渡すので、旧コードではフォールバックがそれを外して、この偽クライアントに合わせていた
  - テスト12（PreExistingThreadReplyClient）：`include_all_metadata` を足した

### 1.2 commit --force の抜け道（指摘3）— agent_tools.py `commit_event`
- force で FAILED→FAILED を指定したときは、リトライ処理（retry_count < 3 なら PENDING に戻す処理、unverifiable の待ち）を通さないようにした。UPDATE は `status='FAILED'`、error_message は `[forced FAILED->FAILED] 理由` 付き、next_retry_at は NULL。retry_count と reap_count は変えない
- `commit_forced` の運用アラートは従来どおり1行出る（`new_status` は FAILED）
- docstring に「forced FAILED -> FAILED は error_message を書き換えるだけで、FAILED のまま。force は requeue の回り道にならない」を足した。既存の「FAILED goes back to PENDING only through requeue」と実挙動が一致した
- 非終端（PENDING／PROCESSING）からの FAILED は、従来どおりリトライ処理を通る（テスト86で確認した）
- 範囲の注記：今回の変更は、依頼どおり FAILED→FAILED だけ。DONE／IGNORED から `--force` で FAILED を指定したときは、今もリトライ処理を通るので、retry_count が小さければ PENDING になる。台帳は残るので、二重返信にはならない（再ディスパッチ後の reply --event-id は重複排除される）。ただ「FAILED を指定したのに PENDING になる」という点は同じなので、必要なら別の候補にしてほしい

### 1.3 アーカイブの印付け（指摘4）— ops_monitor.py
- take：アーカイブの書き込みに失敗したとき、その回にアーカイブしようとした行に `"_archived": false` を明示して返すようにした（成功したときは今までどおり印を付けずに返すので、配送の内容は変わらない）
- restore：行に `_archived` キーがあれば、その値をそのまま使う。キーが無ければ、今までどおり true を付ける（`archive_write_failed` の通知は除く）
- 結果：アーカイブ失敗→wake 失敗→restore→（もう一度失敗しても）→次の成功した take で、失敗した行も通知もすべて1回ずつアーカイブされる。成功した take の行を restore したときは、今までどおり二重にアーカイブしない
- 注意：失敗したときだけ、配送する JSON に `_archived: false` が出る。フックは take の出力をそのまま restore に渡すので、この形にした（依頼の「`_archived: false` を明示する」の方）
- テスト83の (4) の最後の assert を1か所変えた：旧 `== ["archive_write_failed"]` → 新 `== ["reaper", "archive_write_failed"]`。旧 assert は、監査が指摘した不具合の挙動（失敗した reaper 行がアーカイブされない）をそのまま固定していたため。変更の理由はテストのコメントに書いた。テスト83のほかの assert は変えていない

### 1.4 アーカイブの keys（指摘6）— ops_monitor.py `archive_record`
- key が無く、`event_ids` がリストの行（`failed_with_landed_reply`）は、`keys` に event_ids を入れるようにした。key は今までどおり null
- 1件の行（event_id あり）と commit-batch の行（`rejected` から keys を作る）の形は変えていない
- 本番のアーカイブにすでにある1行目（13:29:24Z、`key: None`）は書き換えていない。元の行は `entry.event_ids` に残っている

### 1.5 フックの note の説明（指摘7）
- `~/hooks/scripts/slack-yama-dispatch.sh` の運用警告の note に、次の3つの説明を足した
  - `failed_with_landed_reply`＝返信の着地が台帳にあるのに FAILED のイベントを reconcile が検知（状態は変えていない。requeue しないこと）
  - `requeue_blocked`＝台帳に返信の着地があるため requeue を拒否（行は不変）
  - `requeue_forced`＝台帳に着地があるイベントを --force --reason で requeue（台帳は残るので reply は重複排除される）
- ワークスペース側の `hooks/slack-yama-dispatch.sh` へ cp -p で同期した（cmp で一致を確認）。`bash -n` は通った。フックの運用警告ブロックのテスト（68）も通過した
- 未実施：`hooks.dry_run`（Muse の道具なので、こちらからは実行していない）

### 1.6 requeue 拒否時の stderr（指摘8）— agent_tools.py
- `requeue_event` の `[requeue] WARN reply_already_landed: ...` の行を削除した。stderr は `cmd_requeue` が出す JSON 1行だけになった
- 消した WARN 行の内容（event_id、reply_ts、拒否したこと）は、運用アラートの `requeue_blocked` の行（event_id・reply_ts・channel・thread_ts・reason・current_status・requested_status）にすべて入っていて、JSON 本体（`landed_reply`・`message`）にもある。そのため運用アラートで代替できると判断した
- 残る穴：アラートの書き込みが失敗したとき、`_append_ops_alert` は自分で `[commit] ALERT_WRITE_FAILED` の行を stderr に出していたので、その場合はまた2行になっていた。そこで `_append_ops_alert` に `quiet` 引数を足した。requeue はそれを使い、結果を JSON の `"alert_written": true/false` に入れる。ほかの呼び出し元の挙動は変えていない
- 既存のテスト84は `splitlines()[-1]` で JSON を読んでいたので、変更なしで通る

## 2. 記録だけしたもの（IMPROVEMENTS.md）

- 候補26：リスク (2) を「保持量は最大でおよそ 10 MiB＋1回分」に直した。理由（ローテーションの判定は追記前のサイズで行うので、ファイルが空なら1回の take が上限を超えていてもそのまま書く）と、監査の実測値（上限300バイトに2778バイト）も書いた。あわせて、§1.3・§1.4 の監査対応を1行足した
- 候補27：§1.2・§1.5・§1.6 の監査対応を1行足した
- 候補28：
  - 状態：「2関数のみ」ではなく、フォールバックの削除が全経路（`_lookup_own_recent_message_once` と `_fetch_history_unbounded` を含む）で完了した、という形に直した
  - 「設計上の残余」として指摘9を足した：二択必須は選び間違い（`--no-event-id` の誤用）を防げない。`--event-id` の実在確認は、イベントの状態やチャンネルとの対応までは見ていない。`record-reply` は実在確認をしていない

## 3. テスト

- 新しいテスト86（`test_audit_fix_2026_11_23`）で確かめたこと:
  1. 監査の exp1 と同じ形（FAILED・retry_count=0・reap_count=3・台帳に着地あり）に force FAILED→FAILED → FAILED のまま、retry_count と reap_count は変わらない、理由が error_message に残る、`commit_forced` の new_status が FAILED。unverifiable の文言でも PENDING の待ちにならない。CLI でも同じ。そのあと requeue は台帳で引き続きブロックされる。PROCESSING→FAILED は従来どおり PENDING（retry_count 1）
  2. requeue 拒否の stderr が JSON 1行で、`alert_written` が true。アラートの書き込みに失敗させたときも1行で、`alert_written` が false
  3. 監査の exp2 と同じ流れ（アーカイブ失敗→restore→もう一度失敗→restore→成功）で、失敗した行に `_archived: false` が付いて保たれ、最後の成功した take で全行が1回ずつアーカイブされる。配送の内容に印は出ない。成功した take の行を restore しても、二重にアーカイブされない
  4. `failed_with_landed_reply` の keys に event_ids が入る。1件の行と commit-batch の行は、形が変わらない
  5. `_lookup_own_recent_message_once`（スレッド／チャンネル）と `_fetch_history_unbounded`（スレッド／チャンネル）が、厳密な偽クライアントで1回だけ（include_all_metadata=True、oldest／latest 付きで）呼ばれて TypeError になり、呼び直さない
- 逆向きの確認（旧コードで落ちること）:
  - 旧 agent_tools.py＋旧 ops_monitor.py：(1) で落ちた。status が PENDING、retry_count が 1 になった（監査の exp1 を再現）
  - 新 agent_tools.py＋旧 ops_monitor.py：(3) で落ちた（`_archived` が付かない）
  - 旧 agent_tools.py を直接呼んだ：両関数ともフォールバックで2回目を呼んでいた（§1.1）。旧 `archive_record` の keys は None だった
- 全件：86/86 通過

## 4. 気づいたが触っていないもの

- `backfill.py:198-205` にも、TypeError で conversations_replies を呼び直す分岐がある。これは include_all_metadata の分ではないが、呼び直すときに oldest と cursor が外れる（窓とページングが外れる）。依頼の範囲外なので、変更していない
- §1.2 の注記：DONE／IGNORED から force で FAILED を指定すると、PENDING になりうる
- 公開用コピー（`~/workspace/slack-yama-publish/`）には反映していない。push のときに Muse が公開フローで同期する前提
