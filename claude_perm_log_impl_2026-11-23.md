# 実装レポート：運用アラートの配送済み行を永続ログへアーカイブ（候補26）

日付: 2026-11-23　実装: Claude Code（Opus 5.5）　依頼: claude_perm_log_impl_brief.md

## 0. 前提の確認（重要）
- **agy のレビュー（agy_perm_log_out.md）はワークスペースに存在しなかった。** あるのは依頼文の agy_perm_log_prompt.md だけで、~ 配下を検索しても出力ファイルは見つからなかった。このため、ブリーフに書かれた「採用する設計（agy の『条件付きで持つ』）」を設計の正本として実装した。agy の本文で追加の条件が出ていた場合は、それと照合する必要がある（下記「残課題」）。
- 結果：既存82件＋新規1件で **83/83 通過**（`./venv/bin/python tests/run_tests.py`）。変更前のベースラインも 82/82 だった。

## 1. 変更したファイル
| ファイル | 変更内容 |
|---|---|
| ops_monitor.py | `outbox_take` にアーカイブを追加（単一の記録経路）。`archive_record`（スキーマ変換）と `archive_append`（追記＋ローテーション）を新設。`outbox-restore` は戻した行に `_archived: true` の印を付ける。モジュール docstring に仕様を記載 |
| ~/hooks/scripts/slack-yama-dispatch.sh（稼働側＝正本） | 運用警告ペイロードの note に `archive_write_failed` の説明を1つ追加（文言のみ。ロジックは変更なし）。bash -n 確認済み。ワークスペースの hooks/ のコピーも同じ内容に同期した |
| tests/run_tests.py | テスト83 `test_ops_alert_archive_on_take` を追加 |
| IMPROVEMENTS.md | 候補26（永続ログのアーカイブ化、状態：実装済み）を追加。既存の最大は候補25 |

書き手側（agent_tools.py の commit ガード、dispatcher.sh の Reaper、backfill.py）は**変更していない**。ガードから2箇所に書く二重書きはしていない。

退避：変更前の原本は bak-2026-11-23-permlog/ に置いた（ops_monitor.py・IMPROVEMENTS.md・run_tests.py・slack-yama-dispatch.sh）。

## 2. 設計の実装内容
### 記録の経路
- `outbox_take` が outbox を読み、空にしたあと（flock を外してから）、取り出した行をアーカイブに追記する。outbox に入るものはすべてアーカイブされる（却下・訂正に限定しない）。
- 置き場所：`YAMA_OPS_ALERTS_ARCHIVE_PATH`。未設定なら outbox と同じディレクトリの `ops_alerts_archive.jsonl`（本番では state/ops_alerts_archive.jsonl）。テストの一時 outbox では一時ディレクトリに書かれ、本番の state は汚れない（実行後に確認済み）。
- 排他：アーカイブ専用のロックファイル `ops_alerts_archive.jsonl.lock` の flock の中で、サイズ確認・ローテーション・追記を行う。書き込みは fsync まで行う。

### 時点についての注意（「配送済み」の意味）
フックの `wake` は、成功するとフックそのものを終了させる。このため、配送が成功した後に処理を挟む場所が無い。記録は **take 時点（配送に取り出した時）** に行う。
- wake が失敗した場合、フックはこれまでどおり outbox-restore で行を戻し、watchdog.log に記録する。戻した行には `_archived: true` を付け、次の take では二重にアーカイブしない。印は take の出力から取り除くので、配送内容には出ない。
- つまりアーカイブの1行は「配送に取り出された」記録で、配送の失敗は watchdog.log 側に残る。

### スキーマ（1行＝outbox の1行）
```json
{"schema":1, "at":"…", "archived_at":"…", "key":"Ev…|channel|null",
 "attempted_status":"…", "current_status":"…", "reason":"…", "source":"…",
 "action":"rejected|forced|alert", "kind":"commit_rejected|…", "entry":{…outbox の行そのまま…}}
```
- mark_rejected.jsonl と共通の項目は at・key・attempted_status・current_status・reason。これに source と action を加えた。揃えたのはスキーマで、ファイル構成は揃えていない。
- **action で却下と訂正を区別する**：`commit_rejected` → `rejected`、`commit_forced` → `forced`、それ以外（reaper・backfill_truncated・pending_stale など）→ `alert`。
- forced の行の `current_status` は訂正前の状態（previous_status）とし、訂正後の状態は `new_status` に入れる。rejected の行は「試みた状態」と「その時点の状態（変わらず）」で、意味を mark_rejected に合わせた。
- commit-batch の検証による却下は1行に複数のイベントが入るため、`key` は null とし、`keys` にイベントID の一覧を入れる。
- `at` は元の行の at を使う（無ければ archived_at）。元の行は `entry` にそのまま残すので、情報は失われない。

### 上限
- `YAMA_OPS_ALERTS_ARCHIVE_MAX_BYTES`（既定 5 MiB）。次の書き込みで上限を超える場合、先に `<archive>.1` へ rename してから新しいファイルに書く（1世代のみで、古い .1 は置き換える）。保持量は最大でおよそ 10 MiB。
- 規模の見積もり：1行はおよそ 0.5〜1 KB なので、5 MiB は数千件にあたる。現在の発生頻度（却下0件、Reaper・backfill は稀）なら長期間もつ。

### fail-open＋失敗の検知
- アーカイブの書き込みで例外が出ても、take は成功させる（outbox は空にし、元の行もそのまま配送する）。
- 代わりに `{"kind":"archive_write_failed","archive_path","error","lost_count","lost_kinds","at"}` を **take の戻り値に1行足し**、同じ wake で運用チャットへ届ける（ブリーフの「outbox にエラーを1行流す」に相当）。
- 標準エラーには出さない。フックは `outbox-take … 2>&1` の出力を JSON として解釈するため、stderr に書くと JSON の解釈が壊れ、取り出し済みの行を失う恐れがあった。この点はコードとテストの両方で確認している。
- 失敗通知の行そのものはその時点ではアーカイブされないが、wake 失敗で restore された場合は印を付けずに戻すので、次に成功した take でアーカイブされる。

## 3. テスト83の内容
1. 本物の書き手（`commit_event` の却下と --force 訂正）→ outbox → take → アーカイブ。ガードは archive に直接書かないこと、4行（rejected・forced・alert・バッチ却下）の共通項目と action、forced の current_status と new_status、バッチの keys、空の take では何も足さないこと。
2. restore した行を二重にアーカイブしないこと。`_archived` の印が配送内容に出ないこと。
3. 上限 1500 バイトで12回 take し、`.1` が1世代だけでき、両方とも上限内に収まること。最古の行は捨てられ、最新の行は残ること。
4. 書き込み失敗（親パスがファイル／`YAMA_OPS_ARCHIVE_FORCE_FAIL=1`）のとき、take が成功し、outbox が空になり、`archive_write_failed` が1行出ること。CLI は exit 0 で、stdout は JSON 配列、stderr は空。restore した失敗通知が、次の take でアーカイブされること。

## 4. 運用への影響
- ops_monitor.py は稼働中のフックが PROJECT_DIR から直接呼ぶため、**次のフック周期から本番で有効になる**。state/ops_alerts_archive.jsonl は、最初に outbox が空でない周期で作られる（実装時点で本番の outbox は 0 バイト、アーカイブは未作成）。
- 公開フロー：scrub_publish.py は明示した許可リスト方式で、state/ は公開対象外。新しい .py ファイルも追加していないので、コード分類のチェックにも影響しない。

## 5. 残課題・確認事項
- agy_perm_log_out.md が見つからないため、agy の本文に追加の条件（例：上限値の具体値、行数での維持の指定）があったかは確認できていない。見つかれば照合してほしい。
- アーカイブを読む道具（集計・検索用の CLI）は作っていない。現状は `jq` で足りる。例：`jq -c 'select(.action=="rejected")' state/ops_alerts_archive.jsonl`
- Slack への共有と GitHub への push はしていない（依頼どおり、Muse が別途行う）。
