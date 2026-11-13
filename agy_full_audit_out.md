ご提示いただいたコード・設定・ドキュメント一式について、指定の7観点および前提条件に基づき監査を実施しました。

---

## 指摘事項一覧（重要度順）

### 1. 【高】`dispatcher.sh` のロック解放タイミングと `PROCESSING` 滞留（Reaper 不在・`payload.json` 上書き競合）
- **重要度**: 高
- **場所**: `dispatcher.sh` 全体、`hooks/scripts/slack-yama-dispatch.sh`（85〜99行目）、`agent_tools.py`（`get_processing_events` / `commit_event`）
- **問題**:
  1. `dispatcher.sh` の `flock -n 9` は **`dispatcher.sh` のプロセスが終了した瞬間に解放**されます（実行時間は1秒未満）。一方、`slack-yama-dispatch.sh` の95行目では `2) silent "Dispatcher locked; previous batch still processing"` と記述されており、「エージェントが前バッチを処理中はロックされている」という前提で書かれていますが、実際にはエージェントの処理中（最大600秒）はロックが保持されていません。
  2. そのため、エージェントが1バッチ目（特にリトライ等で30秒以上かかるケース）を処理している最中に次の30秒フックが走ると、2バッチ目の `PENDING` イベントが `PROCESSING` に遷移し、**`state/payload.json` が上書き**されます。
  3. さらに、エージェントのタイムアウト（`script_timeout_secs: 600`）やクラッシュ等で `commit` が呼ばれなかった場合、**`PROCESSING` 状態のまま取り残されたイベントを `PENDING` に戻す仕組み（Reaper）がどこにも存在しません**。`backfill.py` も `INSERT OR IGNORE` のため、一度 `PROCESSING` で固まったイベントは永久に未処理のまま放置されます。
- **起きうるシナリオ**:
  - Slack API の `IncompleteRead` が多発し、5回リトライ（1+2+4+8=15秒）が複数回走って1バッチの処理が30秒を超えた際、後続フックが別イベントをディスパッチして多重起動・`payload.json` 上書きを起こす。
  - エージェントが途中で異常終了・タイムアウトした際、該当イベントが `PROCESSING` のまま永久にロストする（または次回起動時に `agent_tools.py pending` を呼ばない限り気づけない）。
- **修正の方向性**:
  - `dispatcher.sh` 内で、`status = 'PROCESSING'` かつ `updated_at` が一定時間（例：10分＝`script_timeout_secs` 相当）以内のイベントが存在する場合は新規ディスパッチをスキップする（実質的な処理中ロック）。
  - `status = 'PROCESSING'` のまま一定時間（例：15分）を超過したイベントを自動的に `PENDING`（または `retry_count` 加算）へ戻す Reaper 処理を `dispatcher.sh` またはフック冒頭に追加する。
- **追加すべきテスト**:
  - `PROCESSING` 状態のイベントが一定時間経過後に再度ディスパッチ対象（`PENDING`）へ戻ることを確認するテスト。
  - `PROCESSING` 中の新しいイベントがある間は次のバッチがディスパッチされないことを確認するテスト。

---

### 2. 【高】`reconcile_replies` における別イベント返信の誤紐付け（台帳汚染）と時刻前後チェックの欠如
- **重要度**: 高
- **場所**: `agent_tools.py` `reconcile_replies()`（421〜523行目）
- **問題**:
  1. **Phase 2（494〜509行目）のフォールバック不具合**:
     ```python
     matched_msg = None
     for msg in yama_msgs:
         m_ts = str(msg["ts"])
         if m_ts not in assigned_reply_ts:
             matched_msg = msg
             break
     if matched_msg is None and yama_msgs:
         matched_msg = yama_msgs[0]
     ```
     同一スレッド内の全ての Yama 返信が既に別イベントに紐付いている（`assigned_reply_ts` に含まれている）場合でも、`if matched_msg is None and yama_msgs:` によって **スレッド先頭の返信 `yama_msgs[0]` を強制的に選択し、別の `event_id` に対して同じ `reply_ts` を `record_reply()` で重複登録**してしまいます。さらに `without_reply` ではなく `with_reply` に分類されます。
  2. **483行目の恒真条件**:
     `reply_confirmed = any(str(m.get("ts")) == rec_ts for m in yama_msgs) or bool(yama_msgs)`
     後半に `or bool(yama_msgs)` があるため、記録された `rec_ts` がスレッド内に存在しなくても、同スレッドに別の Yama の発言が1件でもあれば `reply_confirmed = True` になります。
  3. **Phase 1 / Phase 2 共通の時刻順序チェック欠如**:
     `_scan_thread_yama_messages` はスレッド親 `parent_ts` から走査します。対象イベント `cand` がスレッド内の返信（`cand["ts"] > parent_ts`）である場合、**`cand["ts"]` より前（イベント発生前）に投稿された Yama の過去返信**も `yama_msgs` に含まれます。過去の返信が台帳未記録（または90日 prune 後）だった場合、イベント発生前の Yama の発言を「このイベントへの返信」として誤って台帳登録してしまいます。
- **起きうるシナリオ**:
  - 同一スレッドでイベントAに Yama が返信（台帳記録済み）。その後、同スレッドのイベントBが1回目 `IncompleteRead` で失敗後に2回目で「返信不要（相槌・ループ防止等）」として返信せず `DONE` になった場合（`commit_event` で `--error` を渡すかエラーが残った場合）、Phase 2 がイベントAの返信 `yama_msgs[0]` をイベントBの返信として台帳に二重登録する。
- **修正の方向性**:
  - 494〜495行目の `if matched_msg is None and yama_msgs: matched_msg = yama_msgs[0]` を削除する（未割当の返信がなければ `without_reply` としてエラークリアのみ行う）。
  - 483行目の `or bool(yama_msgs)` を削除する。
  - 返信候補のマッチング条件に `float(msg["ts"]) > float(cand["ts"])`（対象イベントより後の投稿であること）を追加する。
- **追加すべきテスト**:
  - 同一スレッド内に「返信済みイベントA」と「返信なしで DONE になった残存エラー付きイベントB」があるとき、`reconcile_replies` がイベントAの `reply_ts` をイベントBに誤割当せず、イベントBを `without_reply` に入れるテスト。
  - スレッド内の子イベント（`cand["ts"] > thread_ts`）より前に投稿された未記録の Yama メッセージを誤って紐付けないテスト。

---

### 3. 【高】リトライ時の履歴確認（`history`）が接続エラー時にローカル DB へフォールバックし、Yama 自身の返信が必ず欠落する
- **重要度**: 高
- **場所**: `agent_tools.py` `fetch_history()` / `get_local_history_fallback()`（215〜317行目）、`listener.py` `should_ignore_event()`（155〜156行目）、`hooks/definitions/slack-yama-dispatch.json`（手順1）
- **問題**:
  - フック指示書では「`retry_count` が 1 以上のイベントでは、返信前に必ず `agent_tools.py history` でスレッド履歴を確認し、Yama（`U_YAMA_BOT`）の返信が既にあれば投稿せず `record-reply` して `DONE` にする」と定めています。
  - しかし、`agent_tools.py history`（`fetch_history`）は Slack API が5回接続失敗すると **`"ok": True, "source": "local_db_fallback"`** としてローカル SQLite の `events` テーブルの履歴を返します。
  - ところが、`listener.py`（および `backfill.py`）の `should_ignore_event()` は `if bot_user_id and user_id == bot_user_id: return True, "self_bot_message"` により **Yama 自身の投稿を一切 `events` テーブルに保存しません**。
  - さらに、リトライ時にエージェント（LLM）が新しく生成した返信文案は前回試行時と1文字でも異なれば、`cmd_reply` 内の `_lookup_own_recent_message_once`（全文一致 `msg.get("text", "").strip() != normalized_target`）でも検出できません。
- **起きうるシナリオ**:
  1. 1回目の `reply` で応答ロストし、直後の回復確認も `IncompleteRead` で失敗して `ambiguous_post_outcome`（実際には Slack に着地済み）となる。
  2. 2回目の実行（`retry_count=1`）でエージェントが指示通り `agent_tools.py history` を呼ぶが、5回失敗して `local_db_fallback` が返る。
  3. `local_db_fallback` には構造上 Yama の返信が絶対に存在しないため、エージェントは「まだ返信していない」と判断し、新しく文案を作って `reply` を実行する。
  4. `reply` 実行時点で API が回復していると、前回と文面が微妙に異なるため `cmd_reply` の全文一致チェックもすり抜け、**二重投稿が発生**する。
- **修正の方向性**:
  - `get_local_history_fallback()` で `replies` テーブルも UNION / マージして返すか、または `history` が `local_db_fallback` になった場合は「Yama の自己返信が含まれない不完全な履歴である」ことを警告フィールド等で明示し、`retry_count >= 1` の着地確認には使わせない（fail-closed にする）。
  - 加えて、`cmd_reply` の送信前スレッドチェックにおいて、同一 `event_id` のリトライ時は「全文一致」だけでなく「対象イベントの `ts` 以降に未割当の Yama の返信が既に存在するか」もチェック対象にすることを検討する。
- **追加すべきテスト**:
  - `retry_count >= 1` かつ前回返信が Slack 上に着地済み（`replies` 未記録）の状態で、`history` がローカルフォールバックした際に二重投稿へ進まないことの確認テスト。

---

### 4. 【高】承認済み DLQ 改訂設計と既存コード（`dispatcher.sh` / `reconcile_replies` / `commit_event`）の不整合
- **重要度**: 高
- **場所**: `agent_tools.py` `commit_event()`（90〜164行目）、`reconcile_replies()`（381〜393行目）、`dispatcher.sh`（57〜65行目）
- **問題**:
  前提条件にある承認済み設計「履歴が読めない失敗（`pre_post_check_unverifiable`）は `retry_count` に数えず、毎時の `reconcile` で処理し、24時間で諦める方式」を既存コードに載せる際、以下の3箇所が既存実装と衝突します。
  1. **`dispatcher.sh` の30秒ポーリングとの競合**:
     `commit_event` で `retry_count` を増やさずに `status = 'PENDING'` に戻すと、毎時の `reconcile` を待たずに **30秒後の `dispatcher.sh` が即座に再取得**してしまい、API 障害中に30秒間隔で最大24時間エージェントを無限に wake し続けます。かといって `status = 'FAILED'` にすると、現行の仕組みでは終端状態（DLQ）扱いになります。
  2. **`reconcile_replies` の候補抽出 SQL が `pre_post_check_unverifiable` を除外している**:
     `reconcile_replies()` の388〜391行目の WHERE 句は以下となっています。
     ```sql
     AND (
         LOWER(e.error_message) LIKE '%reply%'
         OR LOWER(e.error_message) LIKE '%ambiguous%'
     )
     ```
     送信前履歴チェック失敗時のエラー文字列は `"pre_post_check_unverifiable"` であり、`reply` も `ambiguous` も含まないため、**現行の `reconcile_replies` のクエリでは候補として1件もヒットしません**。
  3. **下書きファイルのパス管理の不在**:
     `cmd_reply` は `--text-file` を受け取るだけで、失敗時に下書きテキストやファイルパスを DB（`events` テーブル等）に保存していません。毎時の `reconcile`（非 LLM の `slack-yama-dispatch.sh` 内コマンド実行）で再送する場合、どのファイルをどのイベントに送るかの紐付けが DB 上にありません。
- **起きうるシナリオ**:
  - `retry_count` を据え置きにして `PENDING` に戻す実装を入れると、30秒ごとに同じイベントがディスパッチされ続け、クォータや API 枠を枯渇させる。
  - `FAILED` に入れて毎時の `reconcile-replies` に任せる実装にすると、WHERE 句の `LIKE '%reply%' OR LIKE '%ambiguous%'` に弾かれて処理されない。
- **修正の方向性**:
  - 30秒ディスパッチから隔離するための状態（例：`DEFERRED` / `RETRY_WAIT`、または `next_retry_at` カラム）を設けるか、`FAILED` のうち `error_message LIKE '%pre_post_check_unverifiable%'` を `reconcile` 対象に含める。
  - 下書き本文（または定型パス `state/reply-<event_id>.txt`）を DB または規約で確実に保持する。
- **追加すべきテスト**:
  - `pre_post_check_unverifiable` で失敗したイベントが30秒後の `dispatcher.sh` では再取得されず、1時間後の `reconcile` で拾われること、および24時間経過後に諦められることのテスト。

---

### 5. 【中】`_lookup_own_recent_message_once` のリトライ粒度が粗く、長大スレッドで `IncompleteRead` 時に全ページ再取得になる
- **重要度**: 中
- **場所**: `agent_tools.py` `_lookup_own_recent_message_once()`（696〜776行目）および `_find_own_recent_message()`（779〜806行目）
- **問題**:
  - `_scan_thread_yama_messages()`（358〜399行目）では、ページングの **各ページ取得（`conversations_replies` 1回）ごと** に `_call_with_retry()` で5回リトライしています。
  - 一方、送信前チェックおよび応答ロスト回復確認で使われる `_lookup_own_recent_message_once()` は、**`auth_test()` ＋ 最大20ページのループ全体** を1つの関数として `_find_own_recent_message()` の `_call_with_retry()` で包んでいます。
  - さらに、`_lookup_own_recent_message_once()` は毎回先頭で `active_client.auth_test()` を呼んでおり（710行目）、この `auth_test()` と最大20回の `conversations_replies` のうち **最後の1回でも `IncompleteRead` が起きると、1ページ目と `auth_test()` から全てやり直し**になります。
- **起きうるシナリオ**:
  - `IncompleteRead` が頻発する当該環境において、数ページ以上にわたるスレッドへ返信する際、1試行あたり複数回の API コールが全て連続成功しなければならず、5回リトライしても高確率で `pre_post_check_unverifiable` または `ambiguous_post_outcome` に陥る。
- **修正の方向性**:
  - `_lookup_own_recent_message_once()` 内の API 呼び出しも `_scan_thread_yama_messages()` と同様に1コール単位で `_call_with_retry()` を適用する（または `_scan_thread_yama_messages()` に共通化する）。
  - `bot_user_id` の取得も毎回 `auth_test()` を呼ばずキャッシュまたはフォールバックする。
- **追加すべきテスト**:
  - 複数ページのスレッド走査で、2ページ目・3ページ目で単発の `IncompleteRead` が発生しても、1ページ目からやり直さずそのページのリトライだけで完走できることを確認するテスト。

---

### 6. 【中】候補3（live + backfill 重複配送）のコード上の発生経路と `backfill.py` のスレッド返信取りこぼし
- **重要度**: 中
- **場所**: `backfill.py` `_get_channel_db_baseline()`（60〜86行目）、`backfill_channel()`（114〜165行目）、`listener.py` `save_event()`（177〜207行目）
- **問題**:
  1. **重複配送が起きる2つのコード経路（候補3の裏付け）**:
     - **経路A（backfill 先着 → live 後着）**: `backfill.py` が先にメッセージを取得し、フォールバックID `{channel_id}_{ts}` で `events` に INSERT する。直後に Socket Mode（`listener.py`）が遅延または再接続で同じメッセージを受信すると、`listener.py` の `save_event()` は `event_id = Ev...`（エンベロープID）を主キーとして `INSERT OR IGNORE` するため、**既存の `{channel_id}_{ts}` の存在を一切チェックせず2件目として INSERT してしまう**。
     - **経路B（baseline 取得後の競合）**: `backfill_channel()` は冒頭で一度だけ `_get_channel_db_baseline()` を呼んで `existing_other_ids_ts` をメモリに取得し、その後に `conversations_history` と各スレッドの `conversations_replies` を順次 API 呼び出しします。この API 呼び出し中（数秒〜十数秒）に到着した新しい投稿は、すでに `listener.py` が `Ev...` で保存済みであっても `existing_other_ids_ts` に含まれないため、`backfill.py` が `{channel_id}_{ts}` でもう1件 INSERT します。
     - なお、`IMPROVEMENTS.md` 候補3の提案には「既存イベントが終端状態でない場合は backfill 側を残す等の条件付きにする」とありますが、既存が `PENDING` / `PROCESSING`（非終端）のときに backfill 側を残すと、まさに今起きている二重ディスパッチそのものになります。1つのイベント行自体が最大3回のリトライ機構を持っているため、`(channel_id, ts)` 単位で一意にすれば十分です。
  2. **スレッド返信が30件を超えると新しい返信を backfill できない**:
     - `backfill_channel()`（122〜126行目）の `conversations_replies(channel=channel_id, ts=pts, limit=REPLIES_LIMIT)` は `oldest` も `cursor` も指定していません。Slack の `conversations_replies` は古い順（親投稿＋最初の29件）から返すため、**30件以上の返信があるスレッドでは31件目以降の新しい返信が一切 backfill されません**。
- **起きうるシナリオ**:
  - フック実行のタイミングと投稿タイミングが重なると、`Ev...` と `{channel_id}_{ts}` の2レコードが生成され、2回トリアージされる。
  - 活発なスレッド（返信30件超）でリスナー再起動が重なると、最新の返信が backfill でも拾われず完全に取りこぼされる。
- **修正の方向性**:
  - `listener.py` の `save_event()` 側でも、挿入前に `SELECT 1 FROM events WHERE channel_id = ? AND ts = ?` を確認する（または `events(channel_id, ts)` に UNIQUE インデックスを張る）。
  - `backfill.py` の `conversations_replies` でも `oldest=str(cutoff_ts)` とカーソルページングを行う。
- **追加すべきテスト**:
  - 先に `backfill.py` が `{channel}_{ts}` を保存した後に `listener.py` が同一 `(channel, ts)` の `Ev...` ペイロードを受信した際、重複行が作られないことを確認するテスト。
  - 30件超の返信があるスレッドで、31件目以降の新しい返信が `backfill.py` で回収されるテスト。

---

### 7. 【中】フック `slack-yama-dispatch.sh` における `.env` 非アトミック書き換えによるトークン消失レース
- **重要度**: 中
- **場所**: `hooks/scripts/slack-yama-dispatch.sh`（69〜81行目）
- **問題**:
  ```bash
  if ! cmp -s "$tmp_env" "$ENV_FILE"; then
    cat "$tmp_env" > "$ENV_FILE"
    chmod 600 "$ENV_FILE"
  fi
  ```
  - 30秒ごとに実行されるフック内で、`cat "$tmp_env" > "$ENV_FILE"` により `config/.env` をインプレースで truncate（0バイト化）してから書き込んでいます。
  - 同時に別プロセスで動作しているエージェント（`agent_tools.py`）や再起動直後の `listener.py` が `load_env_file()` で `config/.env` を開いた瞬間がこの truncate と重なると、`SLACK_BOT_TOKEN` が空になり `RuntimeError("SLACK_BOT_TOKEN is not set...")` で失敗します。
- **起きうるシナリオ**:
  - プロキシ認証情報のローテーションタイミングで、稼働中のエージェントが `agent_tools.py reply` や `commit` のために `.env` を読み込み、空ファイルを掴んで異常終了する。
- **修正の方向性**:
  - 同一ファイルシステム内（`$PROJECT_DIR/config/.env.tmp.$$`）に一時ファイルを作成し、`chmod 600` を設定した上で `mv -f "$tmp_env" "$ENV_FILE"` でアトミックに置換する。
- **追加すべきテスト**:
  - `.env` 更新処理が `mv` によるアトミック置換になっており、読み取り側が途中状態（空ファイル）を読まないことの確認。

---

### 8. 【中】エラー分類：Slack API の一時的エラー（HTTP 429 / 5xx）が `SlackApiError` として即時失敗扱いになる、および `listener.py` 起動時の `auth_test()` リトライ欠如
- **重要度**: 中
- **場所**:
  1. `agent_tools.py` `_call_with_retry()`（192〜212行目）、`fetch_history()`（298行目）
  2. `listener.py` `main()`（275〜287行目）
- **問題**:
  1. `_call_with_retry()` は `except SlackApiError: raise` として全ての `SlackApiError` をリトライ対象外にしています。しかし Slack API がレート制限（HTTP 429 `ratelimited`）や一時的サーバ障害（HTTP 500 `internal_error`, 502/503/504 `service_unavailable` 等）を返した場合も `SlackApiError` が送出されます。さらに `fetch_history()` では `SlackApiError` の場合は `get_local_history_fallback()` にもフォールバックせず即座にエラー終了します。
  2. `listener.py` 起動時の `web_client.auth_test()`（277行目）には `_call_with_retry()` が適用されていません。`IncompleteRead` が頻発する環境では、起動時の `auth_test()` が1回失敗しただけで `sys.exit(1)` します。このとき `watchdog.sh` の `systemctl restart yama-listener` はプロセス起動時点で成功（exit 0）とみなして `reset_reinstall_state` を呼んでしまいますが、直後に `listener.py` が `auth_test()` 失敗で落ちる場合があります。
- **起きうるシナリオ**:
  - Slack 側の一時的な 503 や 429 発生時に、リトライもローカル DB フォールバックも行われず即座に履歴取得・送信前チェックが失敗する。
  - リスナー再起動時に `auth_test()` が `IncompleteRead` で失敗し、systemd の Restart ループに入る。
- **修正の方向性**:
  - `SlackApiError` のうち HTTP ステータス 429 / 5xx またはエラーコード `ratelimited`, `internal_error`, `service_unavailable`, `request_timeout` は一時的エラーとしてバックオフリトライ（429 は `Retry-After` 考慮）の対象にする。
  - `listener.py` の `auth_test()` にも `_call_with_retry()` を適用する。
- **追加すべきテスト**:
  - `SlackApiError`（503 / `service_unavailable`）発生時にリトライされる一方、`channel_not_found` 等の恒常的エラーは即時失敗することを確認するテスト。

---

### 9. 【低】フック・ウォッチドッグのユニット自己修復とバックオフ設定の不整合
- **重要度**: 低
- **場所**: `hooks/scripts/slack-yama-dispatch.sh`（40〜45行目）、`watchdog.sh`（29行目、117〜120行目）
- **問題**:
  1. `slack-yama-dispatch.sh`（40〜45行目）ではユニットファイルが異なる場合に `cp` と `systemctl enable --now` を実行していますが、すでに古い定義でサービスが起動中の場合、`enable --now` は **再起動（`restart`）を行わない** ため変更が反映されません。また `watchdog.sh` と異なり `sudo -n` を使っていません。
  2. `watchdog.sh` の初回バックオフ `REINSTALL_BACKOFF_BASE_SEC` はデフォルト `30` 秒（`30 * 2^(1-1) = 30`秒）ですが、フックの実行間隔も `30` 秒（`poll_interval_secs: 30`）であるため、1回目の失敗後の次回フック実行時にはすでに30秒が経過しており、**1回目のバックオフが実質的に機能しません**。
- **修正の方向性**:
  - ユニット修復ロジックを `watchdog.sh` 側に一本化するか、フック側でも `systemctl restart` を使う。
  - `REINSTALL_BACKOFF_BASE_SEC` をフック間隔より大きい値（例：60秒以上）に設定する。
- **追加すべきテスト**:
  - 30秒周期の呼び出しにおいて、1回目の再配置失敗直後の30秒後呼び出しがバックオフでスキップされることを確認するテスト。

---

### 10. 【低】ドキュメント・プロンプト・コード間の食い違いおよび死んだコード
- **重要度**: 低
- **場所**:
  1. `hooks/definitions/slack-yama-dispatch.json`（システム構成 vs 手順1）
  2. `agent_tools.py`（48行目、662行目、150〜161行目）
  3. `IMPROVEMENTS.md`（15行目、70行目）
- **問題**:
  1. **Bot往復上限に関するプロンプト内の自己矛盾**:
     `slack-yama-dispatch.json` の前半（システム構成）には「`Bot同士は人間の投稿を挟まず3往復まで。`」と残っている一方、後半（手順1）には「`相手がBotでも、回数の上限は設けない。`」と書かれており、同一プロンプト内で指示が矛盾しています。
  2. **未使用の定数（死んだコード）**:
     `agent_tools.py` 48行目の `RECENT_MESSAGE_WINDOW_SECONDS: int = 900` はどこからも参照されていません。
  3. **応答ロスト回復確認のリトライ回数表記の食い違い**:
     `IMPROVEMENTS.md`（15行目）および監査前提では「応答ロスト時の回復確認（最大3回）」と記載されていますが、`agent_tools.py` 662行目の実装では `max_attempts=HISTORY_RETRY_MAX_ATTEMPTS`（**5回**）が渡されています。
  4. **ウォッチドッグの起動方式に関する記述の食い違い**:
     `IMPROVEMENTS.md` 70行目では「Yama のウォッチドッグは cron 駆動（README: */5 * * * *）」と書かれていますが、`slack-yama-dispatch.json` および `slack-yama-dispatch.sh` 83行目では30秒ごとのイベントフック内で毎回 `watchdog.sh` が実行されています。
  5. **`commit --status IGNORED` 時の古い `error_message` 残存**:
     `agent_tools.py` `commit_event()`（150〜161行目）で、`DONE` の場合は `error_message = ?`（指定がなければ `NULL` にクリア）ですが、`IGNORED` の場合は `error_message = COALESCE(?, error_message)` となっており、1回目 `FAILED` → 2回目 `IGNORED` となったイベントに古いエラーメッセージが残り続けます。
  6. **`get_db_connection()` のタイムアウト設定の二重指定**:
     `listener.py` 78〜82行目で `sqlite3.connect(str(db_path), timeout=30.0)`（＝busy_timeout 30秒）を指定した直後に `PRAGMA busy_timeout=10000;` で10秒に引き下げています（`dispatcher.sh` 51〜54行目も同様）。
- **修正の方向性**:
  - `slack-yama-dispatch.json` 前半の「3往復まで」の記述を改定後の仕様に統一する。
  - `RECENT_MESSAGE_WINDOW_SECONDS` の削除、ドキュメントのリトライ回数表記（3回→5回）とウォッチドッグ駆動方式の記述統一、`IGNORED` 確定時の `error_message` クリアを行う。

---

## 確認したが問題なかった主な点

- **`cmd_reply` における POST 自動リトライの抑止**: `get_web_client(no_retry=True)` で `retry_handlers=[]` を明示しており、`slack_sdk` 内部の自動リトライによる二重送信は確実に防がれています。
- **スレッド返信の送信前チェックの fail-closed 動作**: `args.thread_ts` がある場合は `_find_own_recent_message(..., raise_on_error=True)` の例外時に `pre_post_check_unverifiable`（exit 1）で停止し、安易に POST へ進まない構造になっています。
- **`prune_replies` の安全性**: 日時が `NULL` またはパース不能な行は削除対象から除外されており、不正データによる誤削除が起きない実装になっています。
- **`write_heartbeat` および `dispatcher.sh` の `payload.json` 書き出しのアトミック性**: いずれも `.json.tmp` に書き出してから `replace()` でリネームしており、読み取り側が書きかけの JSON を読む問題は発生しません。
- **Socket Mode リスナーの即時 ACK と自己発言・編集イベント除外**: 3秒以内の ACK 返却が最優先で行われ、`should_ignore_event` により自身の投稿や `message_changed` / `message_deleted` による無限ループは適切に遮断されています。
