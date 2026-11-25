# 実装依頼：Slack ボット Yama の Neon 点検指摘の修正（2026-11-25）

点検レポート `~/workspace/slack-yama/slack_review_2026-11-25.md`（Neon、2026-11-25）の指摘を修正する実装です。点検役が指摘した根拠（ファイル名・行番号）はすべて Muse が実地で裏取り済みです。**修正するのは下記の項目だけ**です。Slack への投稿と GitHub push は Muse が別途行うので、あなたは行わないでください。

## 絶対の制約

- 変更前に変更対象ファイルの原本を `~/workspace/slack-yama/bak-2026-11-25-reviewfix/` へ退避すること。
- テストは `./venv/bin/python tests/run_tests.py` で全件通過させること（現行 86 件＋新規テスト）。
- 個人情報（Slack の実ID・ワークスペースドメイン・実名・メール等）をレポートやコードに書かないこと。テストのダミーIDは既存の慣例（非ID形状リテラル）に従うこと。
- 作業ディレクトリは `~/workspace/slack-yama/`。フック定義は稼働側 `~/hooks/definitions/` が正本で、ワークスペース側コピー `~/workspace/slack-yama/hooks/` へも同期すること。
- 成果物：`~/workspace/slack-yama/claude_review_fix_2026-11-25.md` に、直した項目・直さなかった項目（理由つき）・テスト結果を書くこと。

## 修正項目（優先度順）

### 1. [重大-1] `commit --status FAILED --force` で DONE／IGNORED が PENDING に戻る（候補29(a)）
- `agent_tools.py:673` の条件を `if decision == "force" and status == "FAILED":` に広げ、終端状態からの `--force --status FAILED` はすべてリトライ分岐を通さず `FAILED` のまま確定させる。`FAILED -> FAILED` の既存の扱い（error_message の訂正のみ）は維持する。
- テスト追加：DONE イベントに `commit --status FAILED --force --reason` したとき `FAILED` のままで `PENDING` にならないこと。

### 2. [重大-2] DM 代行の二重投稿経路
- (a) `~/hooks/definitions/slack-owner-dm-dispatch.json` 手順4に「`ts_not_found` やタイムアウト時は再送せず、会話を読み直して着地を確認する」旨を明記する。
- (b) `owner_dm_poll.py send()` の `auto_streak` の扱いを見直す。現状は `send()` が検査するだけで加算せず（加算は `resolve --action replied` のみ）、`resolve` 前に `send` が再実行されると上限ガードが効かない。加算の責任箇所を一か所に統一し、二重加算にならない形にすること。テストで裏付けること。

### 3. [中-1] 「答え済み」定義の統一（フック定義を正とする）
- フック定義の文面（Bot を含む・本文のある投稿が1件でもあれば答え済み）に実装を合わせる。`_find_answering_message_in_thread` の Bot 除外をやめ、本文は空白除き1文字以上あれば数える。`ANSWERED_MIN_TEXT_LENGTH`（10）は削除するか 0 にする。subtype の許可条件は現行のまま。テストを更新する。

### 4. [中-2] フック定義「システム構成」の同期
- `~/hooks/definitions/slack-yama-dispatch.json` の「システム構成」に、稼働中の `ops_monitor.py`（PENDING 滞留警告 600s・継続1回＋復旧1回）、`auto_join.py`、`reconcile-replies`、リスナーの自己終了・`WatchdogSec=180`・pong 監視、構文ガードの10分エスカレーションを簡潔に追記する。「ウォッチドッグ」の説明のずれ（プロキシ認証同期のタイミング、ユニット差し替え時の再起動なし）も直す。

### 5. [中-3] `triage_log.jsonl` 追記の CLI 化（候補32）
- 原因は確定済み：エージェントのアドホック追記で末尾改行が欠け、次回追記と `}{` で連結する（破損行は全764行中4行、すべて `Extra data`）。
- `agent_tools.py` に `triage-log` サブコマンドを追加する（JSON 検証＋`flock`＋末尾改行保証、追記前にファイル末尾が `\n` でなければ補う＋`flush`/`fsync`）。フック定義の指示をそのコマンド経由に変更し、`full` モードでの記録要否も明記する。

### 6. [中-4] 30分無言停止の隙間（候補33）— 検証可能な最小限だけ
- (a) 初回 pong 前フリーズ：`listener.py` の `get_pong_silence_sec`／`should_exit_for_pong_silence` が、1回も pong を受信していない場合に `None` → `False` を返す。安全な範囲で、接続直後の凍結を検知できる形にすること（再起動ループにならないよう注意）。
- (b) 壁時計と単調時計の非対称性：`heartbeat.json` の形式（壁時計 epoch）は `watchdog.sh` との互換性のため変更しない。変更しない場合はその理由をレポートに明記する。
- (c) 受信スレッドだけが詰まっても `WATCHDOG=1` が止まらない点は、検証可能な最小限の対策があれば入れ、なければ未対応として理由をレポートに書く。
- 停止当時の基盤事情は未確認のため、原因の断定はしないこと。

### 7. [中-5] フック内の重要な失敗の無言破棄
- `slack-yama-dispatch.sh` で `dispatcher.sh` の `exit >= 3` を `ops_alerts.jsonl` へ流す。`slack-owner-dm.sh` で `owner_dm_poll.py` の非0終了を `ops_alerts.jsonl` へ流す（握りつぶしの `2>/dev/null || echo '{}'` をやめる）。`auto_join.py` の `StateLoadError` も `ops_alerts.jsonl` に1行書く。

### 8. [中-6] `backfill.py:198-205` の `TypeError` フォールバック削除（候補29(b)）
- `try/except TypeError` を削除し、直接 `client.conversations_replies(**kw)` を呼ぶ。テストの偽クライアントが `oldest`／`cursor` を受け取るよう必要なら合わせて調整する。

### 9. [中-7] ログ・下書きの蓄積（候補23）
- `watchdog.sh:423` の正常時 `[OK]` 行の毎30秒追記をやめる（異常・復旧・再起動時のみ記録）。
- `DONE` 確定から7日経過した `reply-*.txt` と7日経過した `decisions-*.json` を削除する掃除を、既存の定期処理（`reconcile-replies` 相当）に組み込む。`FAILED`／`PENDING` の下書きは削除対象外。

### 10. [軽微-1] `IMPROVEMENTS.md` 候補14の記述修正
- 候補14は「現行維持（B）推奨」と書かれているが、実装は fail-closed（A案）が既に入っている。記述を現状に合わせて直す。

### 11. [軽微-2] コメント・docstring と実装の食い違い（5件）
- 点検レポート [軽微-2] の1〜5件を、それぞれ実装に合わせて直す（`listener.py:6`、`ops_monitor.py:19` と `slack-yama-dispatch.sh:380`、`watchdog.sh:6-7`、`slack-yama-dispatch.sh:112`、`owner_dm_poll.py:11`）。

### 12. [軽微-3] 終端ガード周辺の細かい抜け
- (a) `pre_post_check_unverifiable` 以外の理由で `FAILED` にしたとき `first_unverifiable_at` が残る問題（`agent_tools.py:759-770`）。
- (b) `DONE/IGNORED` への同一状態リクエストが `force` より先に `noop` になり、理由文言の訂正が反映されない問題（`agent_tools.py:516-517`）。
- (c) `_save_event_intended_text` が終端行を書き換える問題（`agent_tools.py:1872`、例外の無言破棄を含む）。
- (d) `commit_batch` の部分コミット可能性（`agent_tools.py:3659-3765`）は、直せるなら直し、直せないなら設計上の残余としてレポートに明記する。

### 13. [軽微-4] `owner_dm_poll.py` のスレッド返信ループ検知漏れと `auto_streak` リセット漏れ
- `poll()` が代行自身のスレッド返信に反応して pending を立てる無駄を防ぐ（最新返信の投稿者がオーナー本人／代行送信かを判定する）。
- `auto_streak` のリセット判定をトップレベルだけでなくスレッド内のオーナー返信も見る形にする。

### 14. 候補33の宿題：`boot_id` の自動記録
- `watchdog.sh` のリスナー再起動検知時に、`boot_id`（`/proc/sys/kernel/random/boot_id` の sha256 先頭8文字）と起動日時（`uptime -s`）を `state/watchdog.log` へ自動追記する。3者で「同じ VM か」を比べるための記録。Slack への投稿はしない。

## 実施しない項目（記録のみ）
- [中-8] Reaper 差し戻し後の並行ワーカー競合（claim トークン第2段）：実測頻度が極めて低く（335バッチ中1回）、今回は見送り。レポートに「未実施・理由」を書く。
- [軽微-5] 候補16・17・18・20・21 と遅延再送ヘッダー二重付与：既存バックログの裏取りのみで、今回は実装しない。レポートに「未実施・理由」を書く。

## 進め方
- 項目ごとに実装→テスト実行を繰り返し、最後に全テストを通すこと。
- 5時間利用上限に当たったら、そこで止めて、どこまで終わったかをレポートに書いて終えること（無理に続けない）。
- フック定義（JSON）を編集したら `python3 -m json.tool` で構文確認をすること。ワークスペース側コピーへの同期も忘れないこと。
