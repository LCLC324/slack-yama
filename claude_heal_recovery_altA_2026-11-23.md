# 実装レポート：候補27 を代替案Aで実施（検知＋requeue ブロック）

日付: 2026-11-23　実装: Claude Code　依頼: `claude_heal_recovery_altA_brief.md`（Muse 決定、agy レビュー `agy_heal_recovery_out.md` を受けて）

## 結論
- reconcile には状態を変える権限を与えなかった。終端ガードには手を入れていない（FAILED→DONE の経路は追加していない）。
- 追加したのは2点。(1) reconcile が「台帳に着地記録があるのに FAILED」のイベントを見つけたら、運用アラートへ1行出す。(2) 手動 requeue は、台帳に着地記録があれば拒否する。`--force --reason` のときだけ通す。
- テストは全84件通過（既存83件＋新規のテスト84）。変更前の原本は `bak-2026-11-23-healrec/` にある。
- 実 DB のコピーで dry-run を実行した結果、実例 **Ev0CH5CWKHM0 が検知される**ことを確認した（`found: ['Ev0CH5CWKHM0']`）。
- Slack への投稿と GitHub への push はしていない。稼働中のフック（~/hooks/scripts）にも触っていない。

## 1. 実装前の調査

### 1-1. 実例 Ev0CH5CWKHM0 が検知の対象になるか
実 DB は読み取り専用で開いた。

| 項目 | 値 |
|---|---|
| events.status | FAILED（retry_count 3） |
| error_message | `pre_post_check_unverifiable: Slack API IncompleteRead（投稿前のスレッド確認が失敗、投稿は未実施。次回再送）` |
| created_at / updated_at | 2026-11-12 14:04:09 / 2026-11-12 14:27:11 |
| attempt_ts | なし |
| replies 台帳 | あり。reply_ts 1794492548.415999、記録は 2026-11-13 14:52:44（翌日の reconcile が拾ったものとみられる） |

- 実 DB で FAILED かつ台帳ありの組み合わせは、この1件だけだった。
- **注意点**：このイベントの作成日は 11-12 で、reconcile の既定の窓（7日）から外れている。窓で絞る作りにすると、この実例は検知できない。そこで、検知は窓を使わずローカル DB 全体の FAILED×台帳 を見る形にした。Slack API を呼ばない SQL の JOIN 1本なので、負荷は小さい。
- 実 DB のコピーで `detect_failed_with_landed_reply(dry_run=True)` を実行した結果は `{'found': ['Ev0CH5CWKHM0'], 'new': ['Ev0CH5CWKHM0'], 'alerted': False}`。本番に入れると、**最初の毎時 reconcile で、この1件について運用アラートが1回出る**。これは想定どおりの動き。
- 補足：error_message には「投稿は未実施」とあるのに、実際には 1794492548（イベントの約5分後）に着地している。FAILED の文言と実態が食い違っている。発生源（代替案B）の手がかりになるかもしれないが、今回は調べていない。

### 1-2. cmd_reply は metadata の event_id をどこまで確実に付けているか（今回は修正していない）
コード上の事実：
1. **`--event-id` は任意の引数で、欠けやすさの最大の要因**。argparse の既定は None。付け忘れると metadata は `request_id`（UUID）だけになり、event_id は入らない。台帳への記録（record_reply）も `if event_id` の中でしか行われない。その結果、台帳にも metadata にも痕跡が残らず、今回の検知と requeue ブロックの両方から見えなくなる。「必ず付けること」はフック定義のプロンプト（mention／full の手順）に書いてあるだけで、コード側では強制していない。
2. `--event-id` を渡した場合は、実際に投稿する経路（chat_postMessage）が1か所しかなく、そこで必ず `metadata.event_payload.event_id` が付く。ただし event_id が events テーブルに実在するかは確かめていないので、打ち間違えた ID もそのまま付く。
3. 投稿しない経路（台帳の文面一致による重複排除、事前チェックで既存の返信が見つかった場合）は、metadata は付かない（投稿していないため）が、台帳には記録する。この経路は問題ない。
4. **読む側の弱点**：`_pre_post_check_thread_for_reply` と `_scan_thread_yama_messages` の `_do_page_call` は、TypeError のときに `include_all_metadata` を外して呼び直す。その場合、metadata が見えなくなり、黙って文面一致に落ちる。実際の slack_sdk では起きにくく、テスト用のモックとの互換のための処理だが、そうなっても警告は出ない。
5. Slack 側が metadata を黙って捨てたかどうかは、ローカルでは確かめられない。ミラー（mirror_messages）に metadata の列がないため。
6. 他の投稿経路：Python 側に chat_postMessage があるのは cmd_reply だけで、owner_dm_poll.py と auto_join.py にはない。

実データ（ミラーが始まった 2026-11-22 11:23 以降）：
- Yama の投稿としてミラーにあるのは426件。そのうち台帳に対応する行がないのは45件（スレッド返信34件）。
- 45件の本文を見ると、すべて「持ち帰って検討した結果の共有」「自己レビューの提案」「実装完了の報告」といった、イベント返信の流れの外で（`--event-id` なしで）出した投稿だった。イベントへの返信で `--event-id` が抜けた例は、見た範囲では見当たらなかった。
- ただし、この45件のように後から出した回答は、元のイベントとは紐付かない。今回の検知の対象外になる。

**別途判断してほしい候補**（今回の範囲外）：(a) 手順上イベントへの返信であるべき場面で `--event-id` が無いときに、警告するか拒否するか。(b) `--event-id` が events テーブルに無いときに警告するか。(c) metadata を外して呼び直したときに stderr へ警告を出すか。

## 2. 変更内容

### 2-1. 検知と通知（`agent_tools.py`：`detect_failed_with_landed_reply`、`reconcile_replies` の最後から呼ぶ）
- 条件：`events.status = 'FAILED'` かつ、同じ event_id の行が replies にあること。対象は FAILED だけなので、**PENDING／PROCESSING（処理中）と DONE／IGNORED は対象にならない**。状態は一切変えない。
- 同じ run の前半（フェーズ1）で台帳に新しく記録した FAILED のイベントも、同じ run で検知に入る。これは Ev0CH5CWKHM0 が生まれたのと同じ経路にあたる。dry-run では台帳に書かないので、その分は `extra_event_ids` として「記録した扱い」で報告する。
- 通知：新しく見つかった分を、run ごとに運用アラートへ1行出す（kind `failed_with_landed_reply`。event_ids、items（channel／thread_ts／reply_ts／error の先頭200字）、note を含む）。
- 繰り返し防止：通知済みの ID を `state/failed_with_landed_reply_alerted.json` に持ち、毎時の run で同じ件を再通知しない。FAILED×台帳 の組み合わせでなくなった ID（requeue された等）は記録から外す。同じ件がまた FAILED になれば、もう一度通知する。アラートの書き込みに失敗した run の分は記録しないので、次の run で再び通知を試みる。
- dry-run の間は、outbox にも状態ファイルにも書かない。
- reconcile の出力に `failed_with_landed_reply: {found, new, alerted}` を追加した。検知そのものが例外になっても reconcile は止めず、`error_details` に1件入れる。

### 2-2. requeue のブロック（`requeue_event`／`cmd_requeue`／parser）
- 状態の確認と台帳の確認は、同じ `BEGIN IMMEDIATE` トランザクションの中で行う。UPDATE にも `AND status = 'FAILED'` を付けた。
- 台帳に着地記録がある場合は拒否する：終了コード1、stderr に `{"ok": false, "error": "reply_already_landed", "landed_reply": {...}, "message": "...--force --reason..."}` を出す。行は変えない。運用アラートに `requeue_blocked` を1行出す。
- `--force --reason <理由>` の場合だけ通す（commit の終端ガードの訂正と同じ形）。理由が空なら拒否する。error_message の末尾に `[forced requeue: reply already landed ts=<reply_ts>] <理由>` を足し、運用アラートに `requeue_forced` を1行出す。出力の event には `forced: true` と `landed_reply_ts` が付く。
- **台帳は消さない**。そのため、強制 requeue の後に再ディスパッチされて `reply --event-id` が走っても、`get_recorded_reply` による重複排除で投稿はされない。二重返信は起きない。本当に送り直したい場合は台帳の行を別に消す必要があるが、その手段はわざと用意していない。
- 台帳にないものと、FAILED 以外（PROCESSING／PENDING 等）の扱いは従来どおり。
- `ops_monitor.archive_record`：`requeue_blocked` を action `rejected`、`requeue_forced` を action `forced` として永続ログに分類するようにした（commit の場合と揃えた）。

### 2-3. 変更したファイル
- `agent_tools.py`：定数 `FAILED_LANDED_ALERTED_FILENAME`、`_failed_landed_alerted_path`、`detect_failed_with_landed_reply`、reconcile の最後での呼び出しと出力、`RequeueBlocked`、`_forced_requeue_error_message`、`requeue_event`（force／reason）、`cmd_requeue`、requeue の parser（`--force`／`--reason`）、モジュール冒頭の使い方の説明。
- `ops_monitor.py`：`archive_record` での action の分類。
- `tests/run_tests.py`：テスト84 `test_failed_with_landed_reply_detect_and_requeue_guard` を追加。
- `IMPROVEMENTS.md`：候補27 を「対応済み（代替案Aで実施。FAILED→DONE の回復パスは実例が増えるまで見送り）」に更新。
- フック（`~/hooks/scripts/slack-yama-dispatch.sh` とフック定義）は変更していない。reconcile の呼び出しはそのままで、新しいアラートは既存の outbox 配送で届く。

## 3. テスト
`./venv/bin/python tests/run_tests.py` → **Summary: 84/84 passed, 0 failed**（変更前の基準は 83/83）。

テスト84で確かめていること：
1. dry-run では検知を報告するだけで、outbox・状態ファイル・台帳には書かない。
2. 本番の run で、窓の外の古い FAILED×台帳（Ev0CH5CWKHM0 と同じ形）と、同じ run で台帳に記録した FAILED の両方を検知し、1行で通知する。**PROCESSING／PENDING／DONE（台帳あり）と、台帳のない FAILED は対象外**。全イベントの状態は変わらない。
3. 次の run では同じ件を再通知しない。
4. requeue：台帳に着地があれば `reply_already_landed` でブロックする。行は変わらず、`requeue_blocked` が1行出て、archive の分類は rejected。`--force` だけの場合と、理由が空白の場合は通らない。
5. PROCESSING／PENDING の requeue は従来どおり「not FAILED」で拒否する（台帳ブロックの対象外）。
6. 台帳のない FAILED は従来どおり通り、アラートも出ない。
7. `--force --reason` で通る。理由が error_message に残り、`requeue_forced` が1行出て、archive の分類は forced。台帳は残る。
8. FAILED でなくなった件は通知済みの記録から外れ、再び FAILED になると再通知される。
9. アラートの書き込みに失敗した run の分は記録されず、次の run で通知される。

## 4. 残っていることと判断材料
- FAILED→DONE の回復パスは見送り。実例が増えたら、agy の必須条件（強照合のみ、失敗理由の限定、条件付き更新と監査証跡）で改めて検討する。
- 検知されたイベントの片付けは手作業になる。スレッドで着地を確認できたら、`commit --event-id <id> --status DONE --force --reason <理由>` で訂正する。DONE になれば次の run で通知済みの記録からも外れる。
- metadata の付与と取得の側（§1-2 の a〜c）は別途判断。
- 発生源（代替案B）として、「投稿は未実施」の FAILED が実際には着地していた食い違いが Ev0CH5CWKHM0 で見えている。
