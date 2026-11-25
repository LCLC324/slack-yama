# Slack ボット Yama：Neon 点検（2026-11-25）指摘の修正報告

- 作業日: 2026-11-25 23:56 〜 2026-11-26 01:50 JST（Claude Code）
- 依頼: `claude_review_fix_brief_2026-11-25.md`／点検レポート: `slack_review_2026-11-25.md`
- Slack 投稿・GitHub push は行っていない

## 経過の注記（中断と再開）
- 実装の大半は最初のセッション（23:56〜01:34 JST）で終えた。そのセッションは報告書を書く前に途切れた。`state/watchdog.log` に 01:36 JST の VM 再起動（boot_id が変わった記録）が残っており、中断はこの再起動によるものとみられる（断定はしない）。
- 再開したセッションでは、退避した原本との差分を項目ごとに照合した。そのうえで残りの作業を済ませた：項目10の状態行の書き換え、テスト88の ID 形状リテラルの除去、CLI の実地確認、全テスト、この報告書。
- 原本の退避先: `bak-2026-11-25-reviewfix/`（変更前の `agent_tools.py`・`auto_join.py`・`backfill.py`・`dispatcher.sh`・`listener.py`・`ops_monitor.py`・`owner_dm_poll.py`・`watchdog.sh`・`yama-listener.service`・`IMPROVEMENTS.md`・`tests/run_tests.py`、稼働側フック定義 `hooks-definitions/`、稼働側フックスクリプト `hooks-scripts/`、ワークスペース側コピー `ws-hooks/`）。`dispatcher.sh` と `yama-listener.service` は退避しただけで、変更していない。

## テスト結果
- `./venv/bin/python tests/run_tests.py` → **90/90 passed, 0 failed**（既存86件＋新規4件。87: commit 周り、88: DM 代行、89: 通知と掃除、90: リスナーとウォッチドッグ）
- 構文確認: `bash -n`（`slack-yama-dispatch.sh`・`slack-owner-dm.sh`・`watchdog.sh`）OK。`python3 -m json.tool`（稼働側フック定義2本・ワークスペース側コピー）OK。
- 同期: 稼働側 `~/hooks/definitions/slack-yama-dispatch.json`・`~/hooks/scripts/slack-yama-dispatch.sh` とワークスペース側 `hooks/` は `cmp` で一致。`slack-owner-dm-dispatch.json`・`slack-owner-dm.sh` はワークスペース側にコピーがない（従来どおり）。
- 実地確認（一時ディレクトリ宛て、本番データには書いていない）:
  - `triage-log`: 末尾改行のないファイルに追記すると改行を補って1行1件になる（`repaired_missing_newline: true`）。`event_id` が無ければ exit 1。
  - `ops_monitor.py outbox-append`: 1行追記を確認。
  - `prune-state-files --dry-run`（本番 state）: 削除候補0件（下記の本番実行の後なので）。
- テストの ID: 新規テスト88で使っていた DM 相手の ID 形状リテラル1件を、`owner_dm_poll.CONVS` から取り出す形に変えた。ほかの新規テストのダミーは `U_Q`・`Ev_RF_B1` など、ID の形をしていない既存の慣例に従っている。

## 直した項目

### 1. [重大-1] `commit --status FAILED --force` で DONE／IGNORED が PENDING に戻る（候補29(a)）
- `agent_tools.py` の分岐を `decision == "force" and status == "FAILED"` に広げた。これで終端状態（DONE/IGNORED/FAILED）からの強制 FAILED は、理由が `pre_post_check_unverifiable` を含んでいても `FAILED` のまま確定する。`retry_count` は変えず、`next_retry_at` は消す。FAILED→FAILED（error_message の訂正のみ）の扱いは従来どおり。
- テスト87: DONE／IGNORED に `--force --reason` で FAILED を commit すると FAILED のままで、PENDING にならないこと（unverifiable の理由でも、CLI 経由でも）を確認。

### 2. [重大-2] DM 代行の二重投稿経路
- (a) `slack-owner-dm-dispatch.json` の手順4に次を明記した。
  - `ts_not_found`／`send_timeout` のときは絶対に再送しない（`send` の再実行もコネクタ送信もしない）。会話を読み直して着地を確かめ、着地していれば `resolve replied --reply-ts`、確かめられなければ `resolve hold` にして報告する。
  - `send_failed` のときも、再送の前に会話を読み直す。
  - コネクタ送信を使ってよいのは、`send` コマンド自体が起動できないときだけ。
- (b) `owner_dm_poll.py`: `auto_streak` の加算を `_count_proxy_reply()` の一か所にまとめた。
  - `send` が加算し、`resolve --reply-ts` は同じ ts を二重に数えない（`proxy_ts` で重複を除く）。
  - `resolve` の前に `send` を繰り返しても、上限で止まる。
  - タイムアウト（新設の `SEND_TIMEOUT`）と `ts_not_found` は「着地したかもしれない」ので、安全側で1つ数え、`unconfirmed_sends` として覚えておく。後から ts が分かっても、着地した投稿が poll で見えても、二重には数えない。
  - CLI の非0終了（`send_failed`）は数えない。
- テスト88で裏付けた。

### 3. [中-1] 「答え済み」定義の統一（フック定義を正とした）
- `_find_answering_message_in_thread`: Bot 投稿の除外をやめた（`bot_ts_set` 引数は呼び出し元との互換のために残し、何も除外しない）。
- `ANSWERED_MIN_TEXT_LENGTH = 0` にした（空白を除いて1文字以上あれば数える）。元の投稿者が Bot でも質問者として扱う。subtype の許可条件（None/file_share/thread_broadcast）は変えていない。フック定義の文面も変えていない。
- テスト87で確認：Bot の回答と短文（「OK」）は答え済みに数え、空白だけの投稿と Yama 自身の投稿は数えない。

### 4. [中-2] フック定義「システム構成」の同期
- `slack-yama-dispatch.json` の「システム構成」に次を追記した。
  - リスナーの自己終了の条件（切断60秒、pong 無応答90秒、初回 pong が来ない120秒＝連続2回まで、イベント処理の詰まり300秒）
  - `WatchdogSec=180` と10秒ごとの `WATCHDOG=1`
  - 返信の照合（`reconcile-replies` 毎時、`failed_with_landed_reply`）と同じ枠での掃除
  - 自動参加（`auto_join.py`）
  - 運用警告（`ops_monitor.py`：PENDING 滞留600秒、継続1回＋復旧1回、段階待ちの件は除外、受け箱とアーカイブ）
  - 構文ガードの10分エスカレーション
- 「ウォッチドッグ」の説明のずれを直した。
  - プロキシ認証は毎周期 `config/.env` へ同期され、リスナーには次の（再）起動から効く。
  - ユニットは、消えている場合に加えて内容が違う場合も入れ直す。ただし稼働中のサービスは再起動しない。
  - 5秒後の再確認と60秒のクールダウン、正常時は記録しないこと、boot_id の記録も書き足した。

### 5. [中-3] `triage_log.jsonl` 追記の CLI 化（候補32）
- `agent_tools.py triage-log --json '<obj>'`（または `--file`、標準入力）を追加した。中身は次のとおり。
  - JSON オブジェクトであることと、空でない `event_id` を検証する
  - `flock` で排他する
  - 追記前にファイル末尾が `\n` でなければ補う
  - 1行のコンパクトな JSON で書き、`flush`＋`fsync` する
  - `logged_at` を自動で付ける
- フック定義の指示をこのコマンド経由に変えた。「全モード共通、full モードでも記録する」と明記し、シェルやファイル追記で直接書くことは禁止した。
- 既存の破損4行（35・116・675・721行目）は直していない。過去の記録なので、読む側が `raw_decode` で分割できる。今後の追記で増えないことが今回の目的。

### 6. [中-4] 30分無言停止の隙間（候補33）— 検証可能な最小限
- (a) 初回 pong 前の凍結: `listener.py` に初回 pong ガードを追加した。
  - 接続中の同じセッションで、120秒（単調時計）たっても1回も pong が来なければ終了する（exit 1 → systemd が再起動）。
  - 再起動ループを防ぐため、この理由の終了は**連続2回まで**とした（`state/first_pong_exits.json`）。それを超えると警告ログを1回出すだけで終了しない。どれかのセッションで pong が1回来れば0に戻す。
  - 既存の `get_pong_silence_sec`／`should_exit_for_pong_silence` は変えていない。新設の `track_session_without_pong`／`should_exit_for_no_first_pong` で扱う。
- (b) 壁時計と単調時計: `heartbeat.json` の `timestamp` は**壁時計 epoch のまま**とした。理由は、`watchdog.sh` が自分の壁時計と引き算して鮮度を判定しているためで、単調時計にすると比べられなくなる（VM の再起動で単調時計は0に戻り、プロセスをまたいで比べる意味もない）。かわりに heartbeat の書き込みと `WATCHDOG=1` の**送る間隔の判定**を `time.monotonic()` に変えた。壁時計が巻き戻っても、送信が止まらない。
- (c) 受信スレッドだけが詰まる件: 最小限の対策を入れた。Socket Mode のリクエストハンドラ（ACK＋DB 保存）の開始時刻を `HandlerTracker` に記録し、1件が300秒以上終わらなければ終了する（exit 1）。
  - 残る隙間: SDK の受信ループ自体が、ハンドラを呼ばずに止まるケースは検知できない。この場合も pong は届き続けるので、pong ガードでは見えない。
  - 今後の案: 「イベントの最終受信時刻」での判定。ただし、イベントが来ない静かな時間帯と区別できないので、今回は入れていない。
- 停止当時（2026-11-25 17:27–17:57 JST）の基盤の事情は確認できていない。今回の変更がその停止の原因に当たるかどうかは断定しない。
- 稼働確認: リスナーは 2026-11-26 01:37:55 JST（VM 再起動後）に新しいコードで起動した。heartbeat は `connected: true`、`last_pong_age_sec` は数秒で、初回 pong ガードによる終了は記録されていない。
- テスト90で確認：120秒で検知して終了する／連続2回で打ち止めになりループしない／pong で0に戻る／ハンドラが300秒詰まると終了する／heartbeat の壁時計 timestamp を維持し、`main()` ループで `WATCHDOG=1` が続く。

### 7. [中-5] フック内の重要な失敗の無言破棄
- 共通の書き口として `ops_monitor.py outbox-append --kind K --detail JSON` を追加した（既存の `outbox_append` と同じ flock を使う）。
- `slack-yama-dispatch.sh`: `dispatcher.sh` の exit 3 以上を `dispatcher_error`（stderr の末尾500字つき）として `ops_alerts.jsonl` に書く。同じ周期の運用警告ブロックで配送する。続いている間は10分に1回だけ再通知し、治った最初の周期で `dispatcher_error_recovered` を1行書く（印のファイル: `state/dispatcher_error_alerted`）。運用警告の説明文（note）に新しい kind を足した。
- `slack-owner-dm.sh`: `2>/dev/null || echo '{}'` の握りつぶしをやめた。`owner_dm_poll.py poll` の非0終了は `owner_dm_poll_failed`（終了コード・stderr 末尾・stdout 先頭つき）として書く（10分抑止、復旧1回）。dry-run のときは書かない。警告が書けなかったときは `watchdog.log` に WARN を1行残す。
- `auto_join.py`: `StateLoadError` のとき、1エピソードにつき1行 `auto_join_state_error` を書く。5分ごとの再実行では重ねない（印のファイル）。次に正常に読めたら `auto_join_state_error_recovered` を1行書く。dry-run では書かない。
- テスト89で確認。

### 8. [中-6] `backfill.py` の `TypeError` フォールバック削除（候補29(b)）
- `try/except TypeError` を削除し、`client.conversations_replies(**kw)` を直接呼ぶようにした。既存テストの偽クライアントは `oldest`／`cursor` を受け取れたので、偽クライアントの調整は要らなかった（テスト90で削除を確認）。

### 9. [中-7] ログ・下書きの蓄積（候補23）
- `watchdog.sh`: 正常時の `[OK]` 行をやめた。`watchdog.log` の最後の `[OK]` 行は 2026-11-26 01:30:33 JST。
- `agent_tools.py prune-state-files [--days 7] [--dry-run]` を追加し、`reconcile-replies` と同じ毎時の枠に組み込んだ（結果は `state/reconcile_history.log` に1行）。削除するのは次の2種類だけ。
  - `reply-<event_id>.txt`: そのイベントが DONE で、確定（`updated_at`）から7日以上たち、ファイル自体も7日以上更新されていないもの。FAILED／PENDING／PROCESSING／IGNORED／DB にないイベントの下書きは消さない。
  - `decisions-*.json`: 更新から7日以上たったもの。
- **本番ですでに1回動いた**（2026-11-26 01:39 JST）：DONE から7日を過ぎた下書き318件を削除し、`decisions-*.json` の削除は0件（全件7日以内）。DONE 以外の下書き7件は残っている。現在の残りは `reply-*.txt` 473件、`decisions-*.json` 89件。削除したファイルは元に戻せない。消したのは依頼の条件どおり DONE 確定済みの分だけ。

### 10. [軽微-1] `IMPROVEMENTS.md` 候補14の記述修正
- 「状態」行を「対応済み（(A) fail-closed）」に書き換えた。「問題」行には「記録当時。現行は fail-closed」と注記し、実装の現状とテスト14を書いた訂正行を足した。
- 残る不整合（`_pre_post_check_thread_for_reply` の `single_page_cap=False` 分岐に fail-open の `break` が残っていること）は、訂正行に書いておいた。直してはいない（今回の対象外で、実害もない）。

### 11. [軽微-2] コメント・docstring の食い違い（5件）
1. `listener.py` の docstring: 他の Bot の投稿は除外せず、`is_bot=1` で保存する、と直した。
2. `ops_monitor.py` の docstring と `slack-yama-dispatch.sh` のコメント: 「1h wait」を段階待ち（2/5/15/30/60分）に直した。
3. `watchdog.sh` の冒頭コメント: 「警告は未実装」と読める文を消した。運用警告は `ops_monitor.py`（600秒）と構文ガードが出す、と書いた。
4. `slack-yama-dispatch.sh` の「Silent when healthy」: 標準出力も `watchdog.log` も、正常時は何も出さない（2026-11-25 以降）と書いた。
5. `owner_dm_poll.py` の docstring: `send` は上限を検査し、加算もする（加算の箇所は `_count_proxy_reply` だけ）と直した。

### 12. [軽微-3] 終端ガード周辺
- (a) `pre_post_check_unverifiable` 以外の理由での FAILED（リトライで PENDING／上限で FAILED）では、`first_unverifiable_at` を NULL に戻すようにした。
- (b) `classify_commit_transition` で `force` を同一状態の `noop` より先に判定するようにした。`DONE→DONE --force --reason` で理由文言を訂正できる。force なしは従来どおり no-op。
- (c) `_save_event_intended_text` は DONE／IGNORED の行を書き換えないようにした（FAILED と PROCESSING は従来どおり保存する。reconcile と `failed_with_landed_reply` が使うため）。`cmd_reply` 側の `except Exception: pass` はやめた。stderr に `intended_text_save_failed` の WARN を出し、エラーの JSON に `intended_text_saved` を載せる。
- (d) `commit_batch` を書き込みまで含めて1トランザクションにした。`commit_event` の本体を `_commit_event_in_tx`（DB 内の処理だけ）と `_report_commit_outcome`（stderr と運用警告）に分け、バッチでは全件を1つの `BEGIN IMMEDIATE` で処理する。途中で拒否や例外が出たら全件ロールバックする（`rolled_back: true`、`committed: 0`）。運用警告はコミットの後にだけ出す。ロールバックしたときは、実際に終端だった行の拒否だけを報告する。
- テスト87で確認。

### 13. [軽微-4] `owner_dm_poll.py` のスレッド返信ループと `auto_streak` のリセット漏れ
- `poll()`: スレッドの `thread_latest` が代行の送信 ts と同じ秒（`thread_latest` は1秒単位）なら、自分の返信とみなす。pending は立てず、透かし（既読位置）だけ進める。
- リセット: `poll()` が見える `read_dm` の結果では、スレッド返信の投稿者が分からない。そこで `resolve --owner-active` を新設した。ワーカーがスレッド内（またはトップレベル）でオーナー本人の後続の発言を見たら、これで `auto_streak` を0に戻す。フック定義の手順にも書いた。トップレベルでの自動リセットは従来どおりで、未確認の送信（ts 不明）がトップレベルに着地した場合は、本人の発言と誤ってリセットしない。
- 制約: スレッド内の本人返信の自動検知は、ワーカーの判断（`--owner-active`）に頼る。`poll()` だけで完結させるには、スレッドごとに `conversations.replies` を読む必要があり、呼び出しが増えるので今回は見送った。

### 14. 候補33の宿題：`boot_id` の自動記録
- `watchdog.sh`: リスナーを再起動するたびに、`[INFO] Boot identity (listener restart): boot_id=<sha256 先頭8文字> booted=<uptime -s>` を `state/watchdog.log` に追記する。あわせて初回と、ブートが変わったとき（`state/last_boot_id` と比べる）にも1回記録する。生の boot_id は書かない。Slack には投稿しない。
- 実際に、初回の記録（01:31 JST）と、VM 再起動後の「boot changed」の記録（01:37 JST）が残っている。

## 直さなかった項目（理由つき）
- **[中-8] Reaper 差し戻し後の並行ワーカー競合（claim トークン第2段）**: 実測の頻度が極めて低い（335バッチ中、900秒超は1回）ので、依頼どおり見送った。残るリスク：900秒を超えたワーカーと、差し戻し後のワーカーが同時に `reply` の送信前チェックを通ると、二重投稿がありうる。また旧ワーカーが遅れて `commit` すると、新しいワーカーの PROCESSING 行を上書きしうる。
- **[軽微-5] 候補16・17・18・20・21、遅延再送ヘッダーの二重付与**: 既存バックログの裏取りだけ行い、今回は実装していない（依頼どおり）。各件の内容は点検レポートの [軽微-5] のとおりで、`IMPROVEMENTS.md` の各候補にも記録が残っている。
- **点検レポート [中-5] のうち、依頼の対象外だったもの**: 次の3点は手を入れていない。
  - `watchdog.sh` の失敗（`>/dev/null 2>&1 || true`）
  - `reconcile-replies` の非0終了と `backfill.py` の取得失敗の通知
  - ディスパッチが成功した周期に `ops_monitor` をスキップすること
- **`triage_log.jsonl` の既存の破損4行**: 過去の記録なので書き換えていない（項目5を参照）。
- **`state/` のほかのログのローテーション**（`triage_log.jsonl`・`auto_join.log`・`reconcile_history.log`）: 依頼の範囲（`[OK]` 行、`reply-*.txt`、`decisions-*.json`）の外なので、手を入れていない。
- **`IMPROVEMENTS.md` の候補23・29・32・33の状態欄**: 今回の修正で状態が変わったが、状態欄は書き換えていない（修正の対象は候補14の記述だけだったため）。台帳の更新は Muse 側でお願いしたい。

## 稼働への反映について
- フック（`slack-yama-dispatch.sh`・`slack-owner-dm.sh`）、`watchdog.sh`、`agent_tools.py`、`ops_monitor.py`、`auto_join.py`、`backfill.py`、`owner_dm_poll.py` は、毎周期呼ばれるので、すでに新しいコードで動いている。
- `listener.py` は 01:37:55 JST の起動から新しいコードで動いている（変更は 01:33）。
- フック定義の文面は、次に起動するワーカーから効く。
