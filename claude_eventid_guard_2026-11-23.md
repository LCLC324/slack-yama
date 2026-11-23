# 実装レポート：event-id の付け忘れ・打ち間違い防止（候補28、候補27の派生）

- 日付: 2026-11-23
- 依頼: `claude_eventid_guard_brief.md`（agy レビュー `agy_eventid_guard_out.md` を受けた Muse の決定）
- 実装: Claude Code（Opus 5.5）
- 結果: 依頼の3項目をすべて実装した。テストは 85/85 件通過（既存84件と新規テスト85）
- Slack への投稿、GitHub への push はしていない。Slack への接続は、読み取り（conversations.replies／history）の実地確認だけ。

## 0. Muse に先に見てほしいこと（要対応1件）

**今夜の自己レビュー cron は、このままだと投稿で失敗する。** 呼び出し側の文面を直す必要がある。

- 場所: `~/workspace/goals/slack-muse/crons/daily/slack-yama-nightly-self-review__daily@21:47:00_user_current.md` の28行目
- 現在の文面: `./venv/bin/python agent_tools.py reply --channel C_WELCOME --text-file <file>`
- この呼び出しは `--event-id` も `--no-event-id` も付けていないので、今後は `event_id_choice_missing` で非0終了し、何も投稿されない。
- 直し方: 末尾に ` --no-event-id` を足す。
- 私が直さなかった理由: このファイルはプロジェクトの外にある Muse の目標／cron の定義で、ファイル名（`_user_current`）から見て、cron システム側が生成した写しの可能性がある。直接書き換えても実際の cron に反映されるか分からないため、Muse の手順で直してほしい。

## 1. 呼び出し箇所の洗い出し（実装前に grep で実施）

`agent_tools.py reply`、`--event-id`、`reply` を、プロジェクト全体・`~/hooks`・`~/MEMORY.md`・`~/workspace` の cron 定義・目標まで grep した。

| 場所 | 内容 | 対応 |
|---|---|---|
| `~/hooks/definitions/slack-yama-dispatch.json`（＋ワークスペースの `hooks/slack-yama-dispatch.json`） | プロンプトの mention 行と full 行の reply の呼び方 | 書き換えた（§3.4） |
| `README.md` 205行目 | reply の例（`--event-id` なし） | 書き換えた |
| `agent_tools.py` 冒頭の docstring | サブコマンドの書式 | 書き換えた |
| `dispatcher.sh` | reply の呼び出しはない（`event-text --event-id` の案内だけ） | 変更なし |
| `hooks/slack-yama-dispatch.sh`（稼働側も同じ） | reply の呼び出しはない | 運用警告の説明に新しい kind を追記（§3.3） |
| `context_bundle.py` 29行目 | コメントで reply に触れているだけ | 変更なし |
| `tests/run_tests.py` | reply の呼び出し58か所 | 必要な23か所を直した（§4） |
| 自己レビュー cron（プロジェクト外） | `--event-id` なしの共有投稿 | **未対応。§0 を参照** |
| `state/` の下書き・共有文、`agy_*`／`claude_*` の過去の記録 | 文章の中で触れているだけ | 変更なし（過去の記録のため） |

## 2. 読み取り側フォールバックの実地確認（項目3）

結論: **本番の slack_sdk は include_all_metadata に対応している。フォールバック分岐は2関数とも削除した。**

確認したこと（2026-11-23、すべて読み取りのみ）:
1. 本番 venv（`yama-listener.service` の ExecStart と同じ `venv/bin/python`）の slack_sdk は **3.46.0**。`WebClient.conversations_replies` と `conversations_history` のシグネチャに `include_all_metadata: Optional[bool] = None` がある。どちらも `**kwargs` も受けるので、SDK から include_all_metadata が原因の TypeError が出ることはない。
2. 実 API: 本番の `get_web_client()`（`_CurlFallbackWebClient`）で、スレッド `C_WELCOME / 1795438575.469479` に `conversations_replies(..., include_all_metadata=True)` を呼んだ。`ok=true` で20件が返り、全件に metadata（`yama_reply`／`neiro_reply`／`yanayana_reply` と event_id／request_id）が付いていた。`conversations_history(..., include_all_metadata=True)` も `ok=true` だった。
3. curl の経路: `_curl_api_read` に同じ引数を渡しても `ok=true` で、metadata 付きの20件が返った（フラグは `include_all_metadata=true` としてフォームで送られる）。

変更:
- `_scan_thread_yama_messages` と `_pre_post_check_thread_for_reply` の `_do_page_call` から、TypeError を捕まえて include_all_metadata を外し、さらに引数を減らして呼び直す二段のフォールバックを削除した。今は `conversations_replies(**kw)` を1回呼ぶだけ。
- TypeError は `_call_with_retry` が再試行の対象にしないので、そのまま上がる。その先の扱い:
  - 遅延再送の reply（`_pre_post_check_thread_for_reply`）は、cmd_reply の既存の `except Exception` によって `pre_post_check_unverifiable` になる（fail-closed で、投稿しない）。テスト85で確認済み。
  - reconcile（`_scan_thread_yama_messages`）は、既存のスレッド単位のエラー処理でエラーとして数えられる。
  - どちらの場合も、黙って文面照合に落ちることはなくなった。

範囲外として残したもの（参考）: 同じ形の TypeError フォールバックが、あと2関数に残っている。`_fetch_history_unbounded`（history コマンド）と `_lookup_own_recent_message_once`（通常の reply の送信前チェックと回復確認。replies と history の両方）。依頼は上の2関数に限っていたので触っていない。SDK の事情は同じなので消すこともできるが、既存テストの偽クライアントの一部（`conversations_replies(self, channel, ts, limit=20)` のような厳密なシグネチャのもの）がこの経路を通るため、消すなら偽クライアントの手直しも一緒に要る。次の候補にするかどうかは Muse の判断に任せる。

## 3. 変更内容

### 3.1 reply の二択必須と実在確認（項目1・2）。`agent_tools.py`

- 引数に `--no-event-id`、`--force`、`--reason` を追加した。判定は新しい関数 `check_reply_event_id_choice` で行い、cmd_reply の先頭で、テキストファイルの確認や Slack の読み取りより前に呼ぶ。
- 判定の順序と結果（拒否はすべて exit 1。stderr に JSON を1つ出す。Slack の読み取り・投稿は0回。運用アラートに `reply_rejected` を1行出す）:

| 呼び出し | 結果（error） |
|---|---|
| `--event-id` と `--no-event-id` の両方 | `event_id_choice_conflict` |
| どちらも無い | `event_id_choice_missing`（付け忘れ防止の本体） |
| `--no-event-id` に `--force` または `--reason` | `force_without_event_id` |
| `--event-id ""`（空白だけを含む） | `event_id_empty` |
| `--force` で理由が無い、または空 | `force_requires_reason` |
| events に無い `--event-id` | `unknown_event_id` |
| events に無い `--event-id` に `--force --reason <理由>` | 通る。`reply_forced` を1行出す（reason・event_id・channel・`forced_check: unknown_event_id`）。台帳にはその ID で記録される |
| events にある `--event-id` | 通る（従来と同じ。アラートなし。`--force` を付けても何も起きない） |
| `--no-event-id` | 通る。event_id は持たないので、replies 台帳には記録されない。metadata は従来の ID なし投稿と同じく request_id（UUID）になる |

- argparse の `required=True` の相互排他グループは使わなかった。exit 2 と usage 文でなく、他の reply エラーと同じ「JSON＋exit 1」にそろえ、`agent_tools.main()` を直接呼ぶ側にも SystemExit を飛ばさないため。
- `--force` で通したときは、stderr に警告を出さない。stderr は「エラー時の JSON 1つ」を呼び出し側（テストを含む）が解析する前提なので、記録は運用アラートの `reply_forced` に一本化した。
- `--event-id` の値は前後の空白を削って使う。

### 3.2 運用アラートの分類。`ops_monitor.py`

- `archive_record` で、`reply_rejected` を action `rejected` に、`reply_forced` を action `forced` に分類する（commit／requeue と同じ扱い）。永続ログ（`ops_alerts_archive.jsonl`）でも、却下と訂正を区別して追える。

### 3.3 フックスクリプトの運用警告の説明。`~/hooks/scripts/slack-yama-dispatch.sh`（ワークスペースの `hooks/slack-yama-dispatch.sh` と同じ内容）

- 運用警告の note に、`reply_rejected`（event-id ガードで拒否、何も投稿していない）と `reply_forced`（events に無い ID を `--force --reason` で投稿）の説明を足した。変わったのは文言だけ。`bash -n` は通り、稼働側とワークスペース側は `diff` で一致している。

### 3.4 フック定義のプロンプト。`~/hooks/definitions/slack-yama-dispatch.json`（ワークスペースの `hooks/slack-yama-dispatch.json` と同じ内容）

- mention 行:
  - イベントへの返信では必ず `--event-id <event_id>` を付けること。
  - reply は二択が必須で、`event_id_choice_missing`／`unknown_event_id` で拒否されると何も投稿されないこと。
  - 拒否されたら payload の event_id を確かめて呼び直すこと。
  - `--force --reason` は、理由がはっきりしている場合だけ使うこと。
- 共有・報告の投稿の呼び方として、`reply --channel <channel_id> --text-file <file> [--thread-ts <ts>] --no-event-id` を新しく明記した（台帳には記録されないこと、イベントへの返信には使わないことも書いた）。
- full 行: 「必ず `--event-id <event_id>`。共有・報告の投稿だけが `--no-event-id`」。
- 確認したこと: 両方のファイルが `python3 -m json.tool` を通った。両者は `diff` で一致している。prompt 以外のキー（version・id・enabled・script_path・presentation_locale・created_at_ms・updated_at_ms ほか）は変更前と同じ。整形も元の形式（indent=2・ensure_ascii=False・末尾改行）のまま。
- 未実施: フック基盤の検証（`hooks.dry_run`）は Muse 側の道具なので、私は実行していない。必要なら Muse 側で走らせてほしい。

### 3.5 README.md

- reply の例を、`--event-id` 付きの返信と `--no-event-id` の共有投稿の2つに分けた。二択が必須であることも注記した。

### 3.6 IMPROVEMENTS.md

- 候補27の後に「候補28: event-id 付け忘れ防止（reply の二択必須＋実在確認＋読み取りフォールバック見直し）（候補27の派生）」を追加した。状態は「対応済み」（問題・根拠・提案・リスク・テスト計画・状態・実施内容・日付）。

## 4. テスト

- 実行: `./venv/bin/python tests/run_tests.py` → **Summary: 85/85 passed, 0 failed.**（変更前の基準は 84/84）
- 新規のテスト85 `test_reply_event_id_guard_and_metadata_read_no_fallback` で確認していること:
  1. 二択なし・両方・空 ID（空文字と空白だけ）・実在しない ID・`--force` 単独・空の理由・`--no-event-id` に `--force`／`--reason`。いずれも exit 1、stdout は空、error コードは期待どおり、読み取り0・投稿0、`reply_rejected` が1行ずつ出る、台帳は空のまま。
  2. 実在しない ID でも `--force --reason` なら1回投稿され、metadata の event_id はその ID、`reply_forced` が1行出て、台帳にその ID で記録される。
  3. 実在する ID ならアラートなしで記録される。`--force` を付けても `reply_forced` は出ない。
  4. `--no-event-id` の投稿は、スレッドでもチャンネルでも台帳に記録されない。metadata は request_id で、出力に event_id は含まれない。
  5. 新しい kind がアーカイブで rejected／forced に分類される。
  6. 本番 SDK の `conversations_replies`／`conversations_history` のシグネチャに include_all_metadata がある（SDK を下げたときに気づくための回帰ガード）。
  7. include_all_metadata を受け付けない厳密な偽クライアントに対して、`_pre_post_check_thread_for_reply` と `_scan_thread_yama_messages` は1回だけ呼んで TypeError を上げる。include_all_metadata なしでの呼び直しはしない。遅延再送の reply は `pre_post_check_unverifiable` で投稿しない。
- 逆向きの確認（このテストが変更前のコードでは落ちること）: 変更前の `_scan_thread_yama_messages` を同じ偽クライアントで呼ぶと、例外にならず、include_all_metadata なしで2回目を呼んでいた。テスト85の (7) は変更前のコードでは失敗する。
- 既存テストの手直し（テストの意図は変えていない）:
  - events に行の無い ID で reply を呼んでいた17か所に、`"--force", "--reason", "test fixture: events row intentionally absent (pre-candidate-28 call shape)"` を足した。対象はテスト10・11・12・13・14・20・21・46。足した17か所は、ガードを計測するラッパーで実際に `--force` の経路を通ることを確かめた。行を足す方式にしなかったのは、events に行があると oldest の窓や安全読み取りの予算が変わり、テストの対象そのものが変わってしまうため。
  - ID なしで呼んでいた6か所に `--no-event-id` を足した（テスト10・12・13・42・46・47）。
  - 機械的に足した `--force` のうち9か所は、イベントが実在していて不要だったので外した（テスト42・46・47・50）。
  - テスト10・12のコメント3か所を新しい呼び方に合わせた。
- CLI でも実際に確かめた（一時 DB と一時 outbox を使い、本番の state には触れていない）: 二択なし、実在しない ID、`--force` 単独、`--no-event-id --force --reason` が、いずれも rc=1 と期待どおりの JSON になり、アラートが4行出た。`reply --help` に新しい引数が出ることも確認した。

## 5. 運用上の注意

- **擬似 ID は今後は拒否される。** 本番 DB の台帳 745件のうち8件は、events に無い `share-*-20261112` という ID だった（2026-11-12 の共有投稿で、重複排除のために付けた擬似 ID）。同じ使い方をすると `unknown_event_id` になる。共有投稿は `--no-event-id` を使う。重複の確認は、従来の ID なし投稿と同じく送信前の文面照合で行われ、台帳による重複排除は無い。擬似 ID で台帳に残したい場合だけ `--force --reason` を使う。11-13 以降、この使い方は0件。
- 拒否が起きると、`reply_rejected` が運用アラートとして次の周期でワーカーに届く（Slack には投稿せず、報告するだけの扱い）。ワーカーが二択を付け忘れたり ID を打ち間違えたりすると、Muse の報告に出てくる。
- 再起動は不要。agent_tools.py は呼び出しのたびに起動し、ops_monitor.py もフックの周期ごとに起動する。listener.py はどちらも import していない。

## 6. 変更したファイルと退避先

原本は `bak-2026-11-23-eventid/` に退避した。

| 変更したファイル | 退避先 |
|---|---|
| `agent_tools.py` | `bak-2026-11-23-eventid/agent_tools.py` |
| `ops_monitor.py` | `bak-2026-11-23-eventid/ops_monitor.py` |
| `tests/run_tests.py` | `bak-2026-11-23-eventid/run_tests.py` |
| `README.md` | `bak-2026-11-23-eventid/README.md` |
| `IMPROVEMENTS.md` | `bak-2026-11-23-eventid/IMPROVEMENTS.md` |
| `hooks/slack-yama-dispatch.json`（ワークスペース） | `bak-2026-11-23-eventid/slack-yama-dispatch.json.workspace` |
| `~/hooks/definitions/slack-yama-dispatch.json`（稼働側） | `bak-2026-11-23-eventid/hooks-definitions/slack-yama-dispatch.json` |
| `hooks/slack-yama-dispatch.sh`（ワークスペース） | `bak-2026-11-23-eventid/slack-yama-dispatch.sh.workspace` |
| `~/hooks/scripts/slack-yama-dispatch.sh`（稼働側） | `bak-2026-11-23-eventid/hooks-scripts/slack-yama-dispatch.sh` |

新しく作ったファイル: このレポート（`claude_eventid_guard_2026-11-23.md`）。
