# Slack ボット Yama 設計・コード点検レポート（2026-11-25）

- 点検日: 2026-11-25
- 対象: `~/workspace/slack-yama/` 配下の実装一式、`IMPROVEMENTS.md`、フック定義（`~/hooks/definitions/slack-yama-dispatch.json`、`~/hooks/definitions/slack-owner-dm-dispatch.json`）、フックスクリプト（`~/hooks/scripts/slack-yama-dispatch.sh`、`~/hooks/scripts/slack-owner-dm.sh`）、`yama-listener.service`
- テスト実行結果: `./venv/bin/python tests/run_tests.py` → **86/86 PASS, 0 failed**（フィクスチャ実行のみ、本番非接触）
- 制約遵守: コード・設定・テストの変更なし、Slack 投稿・本番 API 実行なし

---

## 重大

### [重大-1] `commit --status FAILED --force --reason` で DONE／IGNORED が PENDING に戻る（終端ガード＋requeue ガードの抜け道）
- **既存／新規**: 既存カバー済み（`IMPROVEMENTS.md` 候補29(a) の裏取り）
- **根拠**:
  - `agent_tools.py:666-692`: `decision == "force"` の場合、line 667 で `error_message` を `_forced_error_message(...)` に書き換えた後、line 673 は `if decision == "force" and current_status == "FAILED" and status == "FAILED":` のみを特別扱いしている。
  - そのため `current_status` が `DONE` または `IGNORED` のイベントに対して `commit --status FAILED --force --reason <理由>` を実行すると、line 692 の `elif status == "FAILED":` に入る。
  - 通常リトライ分岐（line 754-756）では `retry_count`（通常 0）が `max_retries`（3）未満のため `final_status = "PENDING"` になる。さらに `--reason` や `--error` に `pre_post_check_unverifiable` を含む場合は line 693-723 に入り、同じく `PENDING`（`next_retry_at` 付き）になる。
  - `agent_tools.py:553-555`（docstring）は「`FAILED -> FAILED` はリトライ分岐をスキップし、`force` が `requeue` の迂回路にならない」と述べているが、`DONE/IGNORED -> FAILED` は `PENDING` へ遷移するため、`requeue` の replies 台帳チェック（`agent_tools.py:4015-4043`）を迂回して再ディスパッチされる。
- **影響**: 確定済みイベントを `FAILED` に訂正しようとした操作が意図せず `PENDING` への再投入になり、再処理が発生する（ただし `replies` 台帳が残っている場合は `reply --event-id` の重複排除が二重投稿を防ぐ）。
- **最小修正**: `agent_tools.py:673` の条件を `if decision == "force" and status == "FAILED":` に広げ、終端状態からの `--force --status FAILED` はすべてリトライ分岐を通さず `FAILED` のまま確定させる。

### [重大-2] `owner_dm_poll.py send` の失敗（`ts_not_found`・タイムアウト）＋フック定義のフォールバック指示による二重投稿経路
- **既存／新規**: 新規（前提「送信経路は1か所のはず」の裏取り結果を含む）
- **根拠**:
  - **前提の裏取り（送信経路は1か所ではない）**:
    1. `agent_tools.py:3132`: `client.chat_postMessage(**kwargs)`（Yama 本体の返信・共有）
    2. `owner_dm_poll.py:140`: `/opt/hatch/bin/slack call-tool --name slack_send_message`（オーナー DM 代行の `send` サブコマンド）
    3. `~/hooks/definitions/slack-owner-dm-dispatch.json:6` 手順4: 「`send` が使えない場合のみ従来のコネクタ送信を使い、その場合は送信 ts を `resolve` の `--reply-ts` に必ず渡す」（エージェントによる `slack_send_message` 直接呼び出しを許容）
    - （参考: 別用途のフック `~/hooks/scripts/eeg-watchdog.sh:817,846` および `parser-carbon-watchdog.sh:123` も `slack_send_message` を呼ぶ。）
  - **二重投稿経路**: `owner_dm_poll.py:171-177` では、`send_dm()` が exit 0 で完了しても応答から `ts` を抽出できない場合、`proxy_ts` を記録せず `{"ok": false, "error": "ts_not_found", "message": "送信自体は完了している可能性がありますが..."}` を返して exit 1（line 358-359）となる。同様に `send_dm()` は `timeout=50` でタイムアウトすると `None` → `send_failed`（line 143-144, 161-162）を返す。
  - しかし `~/hooks/definitions/slack-owner-dm-dispatch.json:6` 手順4 は `auto_streak_limit` 時の送信中止しか書いておらず、`ts_not_found`／`send_failed` 時の「再送禁止・会話再読による着地確認」が未指示。エージェントが「`send` が使えない」と判断して従来のコネクタ送信へフォールバックすると二重投稿になる。
  - 加えて `owner_dm_poll.py:150-180` の `send()` は `auto_streak >= 3` を検査するものの `auto_streak` を加算せず（加算は `resolve --action replied` のみ、`owner_dm_poll.py:303-305`）、`resolve` 前に `send` が再実行されると連続返信上限ガードが効かない。
- **最小修正**: `slack-owner-dm-dispatch.json` 手順4 に「`ts_not_found` やタイムアウト時は再送せず、会話を読み直して着地を確認する」旨を明記し、可能なら `owner_dm_poll.py send` 側でも `auto_streak` の扱いを見直す。

---

## 中

### [中-1] 「答え済み」の定義がフック定義（Bot 含む・本文あり）と実装（人間のみ・11字以上）で食い違っている
- **既存／新規**: 既存メモあり・未番号（`IMPROVEMENTS.md:375-376`「未投稿（定義の明確化）」の裏取り）
- **根拠**:
  - フック定義（`~/hooks/definitions/slack-yama-dispatch.json:6`）:
    > 「待ち状態のイベントを再処理する際、元の投稿への返信が既に他者（Bot含む）から付いていて答え済みであれば、再送せずに理由（他者が回答済み等）を記録して `DONE` で commit して終えること。「答え済み」の判定は、元の投稿より後に、質問者と自分（Yama）以外による本文のある投稿が1件でもある場合とする。」
  - 実装（`agent_tools.py:151, 2574-2620` `_find_answering_message_in_thread`、呼び出し元 `agent_tools.py:2759-2766`）:
    - line 2609-2610: `if _is_bot_message(msg, bot_ts_set): continue`（Bot 投稿を除外）
    - line 151, 2617-2618: `ANSWERED_MIN_TEXT_LENGTH = 10` かつ `if len(cleaned_text) <= min_text_length: continue`（11文字以上を要求。短い肯定「承知しました」等は除外）
    - line 2606-2608: `subtype` は `None`, `file_share`, `thread_broadcast` のみ許可
- **影響**: ワークスペース投稿の99%超が Bot 投稿（`IMPROVEMENTS.md:411` 実測）である環境下、エージェントが手順どおり事前 `history` で判断する限りはフック定義側（広い条件）が先に効くが、エージェントが「未回答」と判断して `reply` を呼んだ場合や自動遅延再送ゲートとして機能する場面では、Bot の回答があっても実装側は答え済みとみなさず再送する。文字数条件（10字超）もフック定義に書かれていない。
- **最小修正**: どちらを正とするかオーナー合意のうえ、フック定義と `_find_answering_message_in_thread`（および `ANSWERED_MIN_TEXT_LENGTH` の扱い）を一致させる。

### [中-2] フック定義「システム構成」に `ops_monitor.py` ほか実装済み機能が載っていない／事実とずれている
- **既存／新規**: 一部既存（`IMPROVEMENTS.md:131` 候補11 追記の裏取り）＋新規の発見を含む
- **根拠**:
  - フック定義（`~/hooks/definitions/slack-yama-dispatch.json:6`）の「システム構成（自分の仕組みについて質問されたら、この事実に基づいて答える）」には「受信」「ウォッチドッグ」「取りこぼし補完」「判定・返信」の4項目しかなく、以下の稼働中コンポーネントが記載されていない：
    1. `ops_monitor.py`（PENDING 滞留警告：人間 PENDING しきい値 600 秒・継続1回＋復旧1回のエッジ通知、outbox 配送と `ops_alerts_archive.jsonl` 永続アーカイブ）— `~/hooks/scripts/slack-yama-dispatch.sh:372-483`、`ops_monitor.py:78, 140-379`（実際に 2026-11-24 に Yama が「まだ実装しておらず」と誤答する事故が発生、`IMPROVEMENTS.md:131`）。
    2. `auto_join.py`（5分間隔での公開チャンネル自動参加＋opt-out 管理＋直近15分シード）— `slack-yama-dispatch.sh:223-241`、`auto_join.py`。
    3. `reconcile-replies`（毎時照合＋`failed_with_landed_reply` 検知）— `slack-yama-dispatch.sh:202-221`。
    4. リスナーの自己終了・バックストップ（切断60秒・pong 無応答90秒での自己終了、`WatchdogSec=180`＋10秒ごとの `sd_notify`）— `listener.py:47-52, 717-773`、`yama-listener.service:18-19`。
    5. 構文ガードの10分エスカレーション（`watchdog.sh`/`dispatcher.sh` の `bash -n`、`listener.py` の `py_compile`）— `slack-yama-dispatch.sh:117-162, 245-370`。
  - さらに記載済みの「ウォッチドッグ」説明にも実装との軽微なずれがある：
    - 「heartbeat が120秒以上古ければ…リスナーを再起動し、プロキシ認証もその場で更新する」とあるが、実際のプロキシ認証同期（`slack-yama-dispatch.sh:92-107`）は毎30秒周期で `watchdog.sh` の前に毎回実行され、反映は次回再起動時（コメント line 91）。
    - 「ユニットファイル自体が消えていたら…入れ直す」とあるが、実際はハッシュ不一致でも入れ替える（`slack-yama-dispatch.sh:56-82`）。ただし稼働中サービスの再起動は行わずログ出力のみ。
- **最小修正**: `slack-yama-dispatch.json` の「システム構成」に `ops_monitor.py`（PENDING 滞留警告 600s 等）、`auto_join.py`、`reconcile-replies`、`WatchdogSec=180`/pong 監視、およびプロキシ認証同期のタイミングを簡潔に追記する。

### [中-3] `state/triage_log.jsonl` の壊れた JSON 行は「途中切断」ではなく改行欠落による複数 JSON 連結（`}{`）
- **既存／新規**: 既存候補32の裏取りおよび**原因の訂正**
- **根拠**:
  - `IMPROVEMENTS.md:323-325`（候補32）は、2026-11-24 の2件（675行目 `Ev0CM2GFLNQ0`、721行目 `Ev0CLA36K02Z`）を「行の途中で切れている」「JSON として閉じていない」と記録している。
  - 実ファイル `state/triage_log.jsonl`（全764行）を `json.loads` と `json.JSONDecoder.raw_decode` で全行検証した結果、不正行は計4行（35, 116, 675, 721行目）で、**全4行ともエラーは `JSONDecodeError: Extra data` であり、1行の中に完全な JSON オブジェクトが2〜3個、改行なしで連結（`}{`）されている**ことを確認した：
    - 35行目（710文字 / 1,334バイト）: `Ev0CH3CLFBPG` と `C_WELCOME_1794492249.625919` の **2件**が連結（col 564 で `}{`）
    - 116行目（2,159文字 / 4,329バイト）: `Ev0CHKJ8A4TV` と `Ev0CHA9ZPL5V` の **2件**が連結（col 972 で `}{`）
    - 675行目（2,355文字 / 4,798バイト）: `Ev0CM2GFLNQ0`、`Ev0CL2SDQ813`、`Ev0CL206KLSF` の **3件**が連結（col 597 で `}{`）。※候補32の「4,798字」は UTF-8 バイト数と一致。
    - 721行目（1,311文字 / 2,347バイト）: `Ev0CLA36K02Z` と `Ev0CL95A82BV` の **2件**が連結（col 554 で `}{`）。※候補32の「2,347字」も UTF-8 バイト数と一致。
  - ワークスペース内の `.py`/`.sh` および `~/hooks/` を `grep` しても `triage_log` への書き込み実装は存在しない。書き込み経路はフック定義（`slack-yama-dispatch.json:6`）の指示を受けたワーカーエージェント自身によるアドホックな追記（シェルやファイル追記ツール）である。
  - なおフック定義では `shadow` と `mention`（非返信分）のみ `triage_log.jsonl` への追記を指示しており、`full` モードでは追記指示がないにもかかわらず、実際には `mode: "full"` の行が大量に書かれている（定義と運用の食い違い）。
- **なぜ起きるか**: エージェントが都度コマンドやファイル追記を組み立てるため、末尾に `\n` を付けずに追記した直後の次回の追記が `}{` で結合する（例：116行目・721行目は前半が空白なしコンパクト JSON、後半が空白あり JSON で書き方が変わっている）。また `flock` もない。
- **一番小さい直し方**: `agent_tools.py` に `triage-log` サブコマンド（JSON 検証＋`flock`＋末尾改行保証、さらに追記前にファイル末尾が `\n` でなければ `\n` を補う＋`flush`/`fsync`）を数十行で追加し、`slack-yama-dispatch.json` の指示をそのコマンド経由に変更する（あわせて `full` モードでの記録要否を明記する）。

### [中-4] 2026-11-25 17:27–17:57 JST の30分無言停止に対するコード・設定上の隙間
- **既存／新規**: 既存候補33の裏取り＋コード上の構造的隙間の整理
- **根拠**:
  - `state/watchdog.log` の該当時間帯を確認：`2026-11-25T08:27:48Z [OK] Heartbeat healthy (OK:age=24s)` の次が `2026-11-25T08:58:01Z [WARN] Heartbeat check failed (STALE:age=1806s>=threshold=120s:hb_ts=1795595274.5081716)` → `08:58:04Z` に `systemctl restart yama-listener` で復旧。
  - 現行ブートでの `systemctl show yama-listener.service` は `Type=simple`, `Restart=always`, `NotifyAccess=main`, `WatchdogUSec=3min`, `WatchdogTimestamp=Wed 2026-11-25 23:42:43 JST`（更新中）であり、`yama-listener.service` と `/etc/systemd/system/yama-listener.service` に差分なし。ただし停止当時（21:19 JST の再起動前のブート）の systemd 状態・基盤側の事情は **未確認**。
  - **コードとフックスクリプトから読み取れる設計上の隙間（5点）**:
    1. **ウォッチドッグのフック単一依存**: `watchdog.sh` はプラットフォームの30秒フック（`slack-yama-dispatch.sh:118-119`）からしか呼ばれない。フックが30分（当日は最大101分）止まれば、`watchdog.sh` も完全に無言で止まる。
    2. **壁時計（`CLOCK_REALTIME`）と単調時計（`CLOCK_MONOTONIC`）の非対称性**: `listener.py:508, 711, 755, 764` は `heartbeat.json` の記録および `HEARTBEAT_INTERVAL_SEC`／`WATCHDOG_PING_INTERVAL_SEC` の経過判定にすべて `time.time()`（壁時計）を使っている。一方 systemd の `WatchdogSec=180` は `CLOCK_MONOTONIC` を使うため、VM／コンテナの一時停止（suspend/freeze）が起きた場合、復帰時に `WatchdogSec` は 0 秒経過とみなして発火しないのに対し、`watchdog.sh` は壁時計差 `age=1806s` を即座に検出する（また壁時計が巻き戻ると `now - last_heartbeat` や `now - last_watchdog_ping` が負になりループ更新が止まる）。
    3. **ユニット更新後の未再起動の隙間**: `slack-yama-dispatch.sh:46-74` は `/etc/systemd/system/yama-listener.service` を差し替えても `systemctl enable --now` のみで稼働中サービスを再起動しない（しかも line 65 の `cp` は `sudo` なし）。ユニット定義に `WatchdogSec=180` を足した後に再起動されないまま動いていた場合、旧プロセスには `NOTIFY_SOCKET`／ウォッチドッグが有効化されない（11-25 当時の該当有無は **未確認**）。
    4. **初回 pong 前のフリーズ無検知**: `listener.py:547-577`（`get_pong_silence_sec` / `should_exit_for_pong_silence`）は、そのセッションで1回も pong を受信していない（`last_ping_pong_time is None`）場合 `None` → `False` を返す。接続直後にリンクが凍結しても自己終了せず、メインループは `heartbeat.json` と `sd_notify("WATCHDOG=1")` を打ち続ける。
    5. **メインループとイベント受信スレッドの分離**: `sd_notify("WATCHDOG=1")` と `write_heartbeat` はメインループ（`listener.py:710-774`）が打ち、イベント受信・DB 保存は SDK の受信スレッド（`on_socket_mode_request`, `listener.py:653-684`）が担う。受信スレッドだけが例外・詰まりで機能不全になっても、`is_connected()` が真のままなら `WATCHDOG=1` は止まらない。

### [中-5] フック内の重要な失敗（`dispatcher.sh` 異常終了・`watchdog.sh` 失敗・`auto_join` 状態破損・`owner_dm_poll` 失敗）が無言で消える
- **既存／新規**: 新規（一部、候補24 の拡張を含む）
- **根拠**:
  - `~/hooks/scripts/slack-yama-dispatch.sh:497-500`: `dispatcher.sh` が例外（`exit 3`, `dispatcher.sh:444`）で失敗しても、`log`（stderr のみ）と `silent "Dispatcher error (exit ${code})"` を返すだけで、`wake` も `ops_alerts.jsonl` への記録もしない。人間の PENDING が 600 秒滞留するまで（Bot 投稿のみなら永久に）気づけない。
  - `~/hooks/scripts/slack-yama-dispatch.sh:118-119`: `"$PROJECT_DIR/watchdog.sh" >/dev/null 2>&1 || true` により、リスナー再起動失敗やユニット再配置の上限到達（`MAX_REACHED`, `watchdog.sh:194`）がすべて `watchdog.log` に埋もれ、運用通知に出ない。
  - `~/hooks/scripts/slack-yama-dispatch.sh:218-220, 239`: `reconcile-replies` の非0終了、`auto_join.py` の `StateLoadError`（本体・`.bak` 両方破損による自動参加停止、`auto_join.py:520-526`）や API エラーはログ／status ファイルに書かれるだけで通知経路がない。`backfill.py` のチャンネル取得失敗・`auth_test` 失敗も `slack-yama-dispatch.sh:195-198` の stderr フィルタ（アラート書き込み失敗のみ対象）を素通りする。
  - `~/hooks/scripts/slack-owner-dm.sh:28, 30, 48-52`: `owner_dm_poll.py poll` が例外終了（状態 JSON 破損等）しても `2>/dev/null || echo '{}'` で握りつぶし、`silent "no new DM messages" "{}"` として無言終了する。加えて `owner_dm_poll.py:203-218` の正規表現パース全滅も `"0 msgs"` 扱いになる（候補24）。
  - `~/hooks/scripts/slack-yama-dispatch.sh:317, 404`: ディスパッチ成功周期（`code == 0`）は `syntax-guard` と `ops_monitor`（`pending-check`／`outbox-take`）をスキップするため、バーストが連続すると監視とアラート配送が遅れる。
- **最小修正**: 少なくとも `dispatcher.sh` の `exit >= 3` と `slack-owner-dm.sh` の `owner_dm_poll.py` 非0終了は `ops_alerts.jsonl` または `wake` へ流し、`auto_join.py` の `StateLoadError` も `ops_alerts.jsonl` に1行書く。

### [中-6] `backfill.py:198-205` に `TypeError` 時の引数脱落フォールバックが残っている
- **既存／新規**: 既存カバー済み（`IMPROVEMENTS.md` 候補29(b) の裏取り）
- **根拠**:
  - `backfill.py:188-223` のスレッド返信ページングループ内、`backfill.py:198-205`:
    ```python
    def _do_replies_call(kw: Dict[str, Any] = replies_kwargs) -> Any:
        try:
            return client.conversations_replies(**kw)
        except TypeError:
            fallback_kw = {
                k: v for k, v in kw.items() if k in ("channel", "ts", "limit")
            }
            return client.conversations_replies(**fallback_kw)
    ```
  - 候補28で `agent_tools.py` の全4関数からは同様の `TypeError` フォールバックを削除済みだが（`IMPROVEMENTS.md:298`）、`backfill.py` にだけ残存している。
  - `TypeError` 発生時に `oldest` と `cursor` を黙って落とすため、ページングループ中で `has_more=True` かつ `next_cursor` があると **同じ1ページ目を最大 `THREAD_REPLIES_MAX_PAGES`（20）回繰り返し取得**する動作になり、かつ無関係な `TypeError` バグも握りつぶす。
- **最小修正**: `backfill.py:198-205` の `try/except TypeError` を削除し、直接 `client.conversations_replies(**kw)` を呼ぶ（テストの偽クライアントが `oldest`/`cursor` を受け取るよう必要なら合わせて調整）。

### [中-7] ログ・下書き・バッチ判定 JSON が際限なく蓄積している
- **既存／新規**: 既存候補23の裏取り＋新規の対象ファイル発見（`decisions-*.json` 等）
- **根拠**:
  - 2026-11-25 23:43 JST 時点の実測（`state/` 内合計 1,012 ファイル）：
    - `state/watchdog.log`: **2,111,732 バイト（約 2.1 MB、36,983 行）**。原因は `watchdog.sh:423` が毎30秒 `log_msg "[OK] Heartbeat healthy (${CHECK_RESULT})"` を追記し続けるため（1日約 2,880 行・約 158 KB 増）。
    - `state/reply-*.txt`: **791 ファイル（726,053 バイト）**。削除処理がどこにもない。
    - **新規確認**: `state/decisions-*.json` も **89 ファイル（16,072 バイト）** 蓄積している（`commit-batch` 用にワーカーが作成し、削除処理なし）。
    - その他 `state/triage_log.jsonl`（861 KB）、`state/auto_join.log`（82 KB）、`state/reconcile_history.log`（33 KB）にもローテーションがない（ローテーション実装済みは `ops_alerts_archive.jsonl` の5 MiB・1世代のみ、`ops_monitor.py:330-331`）。
- **最小修正**: (1) `watchdog.sh:423` の `[OK]` 行を出力しない（異常・再確認復旧・再起動時のみ記録）、(2) 定期ジョブ（`reconcile-replies` や `prune-replies` 相当）で `DONE` 確定から7日経過した `reply-*.txt` と7日経過した `decisions-*.json` を削除する（`FAILED`／`PENDING` の下書きは削除対象外）。

### [中-8] 900秒 Reaper 差し戻し後の並行ワーカー競合（二重投稿・PROCESSING 上書き）
- **既存／新規**: 既存候補19の設計残余（第2段 claim トークン未実施）の裏取り
- **根拠**:
  - `dispatcher.sh:34-43` のコメント自身に明記のとおり、ワーカーの実行時間には上限がなく、900秒（`PROCESSING_TIMEOUT_SEC`）を超えると Reaper が `PENDING` に差し戻し（`dispatcher.sh:216-256`）、次周期で別ワーカーを起動する。
  - `agent_tools.py:2912-3143`（`cmd_reply`）にはイベント単位のプロセス間ロックがないため、旧ワーカーと新ワーカーが同時に送信前チェックを通過すると TOCTOU で二重投稿しうる。また `commit_event`（`agent_tools.py:514-515`）は `PROCESSING` からの遷移を常に通すため、旧ワーカーが遅れて `commit` すると新ワーカーの `PROCESSING` 行を上書きする。
- **補足**: 実測での発生頻度は極めて低い（335バッチ中900秒超は1回、`dispatcher.sh:38`）ため、直近の優先度は中〜低。

---

## 軽微

### [軽微-1] `IMPROVEMENTS.md` 候補14の記述と現行コード・テスト14の食い違い
- **既存／新規**: 新規（文書・台帳とコードの食い違い）
- **根拠**:
  - `IMPROVEMENTS.md:155-163`（候補14）は「状態: 提案済み」「現行は 20 ページ上限打ち切りで None を返し fail-open で投稿に進む」「推奨は (B) 現行維持」と書いている。
  - しかし実際のコード `agent_tools.py:3434-3445`（`_lookup_own_recent_message_once`）には `# Candidate 14 (option A, owner-delegated)` として `raise RuntimeError("thread_scan_truncated: ...")` の **fail-closed が既に実装済み**であり、`tests/run_tests.py` テスト14も「20ページ上限打ち切りは fail-closed で送信せず」で PASS している。
  - 一方で `agent_tools.py:2729-2743`（`_pre_post_check_thread_for_reply` の `single_page_cap=False` 分岐）には旧来の fail-open の `break` が残っている（`cmd_reply` からは `is_delayed_retry=True` でしか呼ばれないため実害はないが、関数単体では不整合）。

### [軽微-2] コメント・docstring と実装の食い違い（5件）
- **既存／新規**: 新規（一部、既存候補21・候補18と関連）
- **根拠**:
  1. `listener.py:6` docstring: `Filters out self-messages, other bots, edits/deletes, and system events` とあるが、実装は `ALLOWED_SUBTYPES` に `bot_message` を含み（line 56）、他 Bot 投稿を除外せず `is_bot=1` で保存する（line 370-410, 430）。
  2. `ops_monitor.py:19` docstring & `~/hooks/scripts/slack-yama-dispatch.sh:380` コメント: `deliberate 1h unverifiable wait` / `deliberate 1h waits` とあるが、実装は 2→5→15→30→60分の段階待ち（`agent_tools.py:133, 453-469`）。
  3. `watchdog.sh:6-7` コメント: `the escalation/warning threshold is restart + 60s = 180s, recorded in IMPROVEMENTS.md - the warning itself is not implemented here` — `watchdog.sh` 内に heartbeat stale の 180s 警告が無いのは事実だが、PENDING 滞留警告自体は `ops_monitor.py`（600s）で実装済みであり誤読を招く。
  4. `~/hooks/scripts/slack-yama-dispatch.sh:112` コメント: `# Silent when healthy.` とあるが、`watchdog.sh:423` は正常時も毎30秒 `watchdog.log` に追記している（標準出力のみ無音）。
  5. `owner_dm_poll.py:11` docstring: `send - send a proxy DM reply (enforcing auto_streak < 3 and recording proxy_ts)` とあるが、`send()` は `auto_streak` を検査するだけで加算しない（加算は `resolve` 時のみ、`owner_dm_poll.py:158, 304`）。

### [軽微-3] 終端ガード周辺の細かい抜け・不整合（4件）
- **既存／新規**: (a) 既存候補19の残余、(b)(c)(d) 新規
- **根拠**:
  - (a) `agent_tools.py:759-770`: `pre_post_check_unverifiable` 以外の理由で `FAILED`（リトライ→`PENDING`）にしたとき `first_unverifiable_at` がクリアされず、以後の再試行も遅延再送扱い（`is_delayed_retry=True`）に入り続ける（候補19の未実施分）。
  - (b) `agent_tools.py:516-517`: `classify_commit_transition` は `DONE/IGNORED` への同一状態リクエストを `force` より先に `noop` と判定するため、`DONE` 済みイベントの `error_message`（理由文言）を `--status DONE --force --reason` で訂正しようとしても無言で `noop` になり反映されない。
  - (c) `agent_tools.py:1872`: `_save_event_intended_text` は `WHERE event_id = ?` で `status` を確認せず更新するため、既に終端のイベントに対して `reply` が `ambiguous_post_outcome` になると終端行の `intended_text` / `attempt_ts` / `updated_at` が書き換わる。また `cmd_reply:3229-3237` では `_save_event_intended_text` の例外が `except Exception: pass` で無言破棄される。
  - (d) `agent_tools.py:3659-3765`: `commit_batch` は検証フェーズのみ all-or-nothing であり、書き込みは1件ずつ別トランザクションで `commit_event` を呼ぶため、検証後の並行更新等で途中失敗した場合は部分コミットになりうる。

### [軽微-4] `owner_dm_poll.py` のスレッド返信ループ検知漏れと `auto_streak` リセット漏れ
- **既存／新規**: 新規
- **根拠**:
  - `owner_dm_poll.py:259-268`: `poll()` は `m["thread_latest"] > seen` だけでスレッド pending を生成し、最新返信の投稿者がオーナー本人／代行送信（`proxy_ts`）かを判定しない。そのため代行がスレッド返信するたびに次周期で自分自身の返信に反応して pending が1件立ち、ワーカーが読んで `resolve skipped` する無駄な wake が発生する。
  - `owner_dm_poll.py:251-257`: `auto_streak = 0` のリセット判定はトップレベルメッセージ `msgs` のみを走査しているため、オーナー本人が DM スレッド内で返信しても `auto_streak` がリセットされない。

### [軽微-5] その他の既知未決候補の裏取り（コード上で残存を確認）
- **候補16**（`agent_tools.py:3161-3168`）: `chat_postMessage` の `SlackApiError`（5xx/`fatal_error` 含む）は着地確認なしで即時 exit 1。チャンネル直投稿は fail-open のため再投稿リスクが残る。
- **候補17**（`agent_tools.py:1383-1420, 2928-2957`）: `_find_own_ledger_reply` は別 `event_id` の同文返信を `deduplicated: true` で黙って抑止し、元 `event_id` や抑止理由を出さない。
- **候補18**（`agent_tools.py:2661, 2714`）: `_pre_post_check_thread_for_reply` の文面照合が `.strip()` のみで `_normalize_reconcile_text` を使っていない。
- **候補20**（`listener.py:134, 439-462`）: `events(channel_id, ts)` は非ユニークインデックスで、`save_event` の SELECT→INSERT がトランザクション外。
- **候補21**（`agent_tools.py:56, 825-831`、`dispatcher.sh:141-145`）: `VALID_MODES` が未使用で、`mode.txt` の typo を検証していない。
- **遅延再送ヘッダーの二重付与リスク**（`IMPROVEMENTS.md:398` 残件関連、`slack-yama-dispatch.json:6` vs `agent_tools.py:143-149, 2513-2525`）: フック定義は「返信文の先頭に『遅れての再送』と元の失敗時刻を1行添えること」と自由文で指示しているが、`apply_delayed_retry_prefix` は厳密な正規表現 `DELAYED_RETRY_PREFIX_RE` に一致しない限り自動で1行目を前置するため、エージェントの表記揺れでヘッダーが2重に付く。

---

## 点検焦点との対応表

| 点検焦点 | 該当指摘 | 状態 |
|---|---|---|
| 1. 「答え済み」の定義の食い違い | [中-1] | 裏取り済み（Bot除外＋11字以上） |
| 1. システム構成に `ops_monitor.py` なし | [中-2] | 裏取り済み（他に `auto_join` 等も未記載） |
| 1. その他の決まりと実装の食い違い | [重大-2], [中-2], [中-3], [軽微-5] | 新規確認（DMフォールバック、`full` モードの `triage_log`、プロキシ同期、遅延再送ヘッダー） |
| 2. 候補29 (a) force FAILED→PENDING / (b) backfill TypeError | [重大-1], [中-6] | 両件ともコード上で100%裏取り済み |
| 2. 候補32 `triage_log.jsonl` 破損 | [中-3] | **原因特定・訂正**（途中切断ではなく改行欠落による `}{` 連結） |
| 2. 候補23 ログ・下書きの蓄積 | [中-7] | 実測裏取り済み（`watchdog.log` 2.1MB、`reply-*.txt` 791件、新たに `decisions-*.json` 89件も確認） |
| 2. 候補33 30分無言停止と `WatchdogSec=180` | [中-4] | 設計上の隙間5点を特定（停止当時の基盤事情は未確認） |
| 3. 二重投稿経路（送信1か所前提の裏取り） | [重大-2], [中-8], [軽微-5] | 送信経路は最低2〜3系統存在。DM `ts_not_found` フォールバックが新規リスク |
| 3. 終端ガードの抜け道 | [重大-1], [中-8], [軽微-3] | `DONE/IGNORED` の `--force FAILED` 等を確認 |
| 3. 失敗が無言で消える経路 | [中-5], [軽微-3] | `dispatcher.sh` exit 3、`watchdog.sh`、`owner_dm_poll` 失敗等の無言破棄を確認 |
| 3. コメント・docstring・台帳の食い違い | [軽微-1], [軽微-2] | 候補14の実装済み未更新、`listener.py:6`、`ops_monitor.py:19` 等を確認 |

---

## 直す価値の高い順（まとめ）

1. **`commit_event` の `DONE/IGNORED` + `--force --status FAILED` が `PENDING` に戻るバグ修正（[重大-1] / 候補29(a)）**
   - 1行の条件修正（`agent_tools.py:673`）で終端ガードと `requeue` 台帳ガードの明確な抜け道を塞げる。費用対効果が最も高い。
2. **`triage_log.jsonl` 追記の CLI 化（[中-3] / 候補32）**
   - 原因が「エージェントのアドホック追記による末尾改行欠落（`}{` 連結）」と確定した。`agent_tools.py triage-log` の追加だけで再発を根絶でき、影響範囲も小さい。
3. **フック定義の同期（[中-1], [中-2], [重大-2]）**
   - `slack-yama-dispatch.json` への `ops_monitor.py` 等の追記（誤答の再発防止）、「答え済み」定義の統一、`slack-owner-dm-dispatch.json` への `ts_not_found`／タイムアウト時再送禁止の明記。文言修正中心で二重投稿・誤報告を防げる。
4. **`watchdog.log` の `[OK]` 行削減＋古い `reply-*.txt` / `decisions-*.json` の掃除（[中-7] / 候補23）**
   - `watchdog.sh:423` の1行変更だけで日次約 158 KB の肥大化が止まる。下書き・判定ファイルも放置すると `state/` が数千ファイルへ膨らむため早めの掃除が有効。
5. **無言失敗の通知導線追加（[中-5]）＋`backfill.py:198` フォールバック削除（[中-6] / 候補29(b)）**
   - `dispatcher.sh` の `exit 3` や `slack-owner-dm.sh` の `owner_dm_poll.py` 失敗が黙って消える穴を塞ぎ、`backfill.py` の古い `TypeError` フォールバックを他経路と揃えて削除する。
6. **リスナー監視の補強（[中-4] / 候補33）と台帳・コメントの整理（[軽微-1], [軽微-2]）**
   - `listener.py` の経過時間判定を `time.monotonic()` に揃える・初回 pong 前フリーズの扱いを検討する。また `IMPROVEMENTS.md` 候補14や古い docstring を現状に合わせておくと、将来の誤判断を防げる。
