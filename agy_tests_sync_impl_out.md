[scrub_publish.py](file:///home/hatch/workspace/scrub_publish.py) に未分類コードファイル検出機能を追加しました。仕様外の既存動作（フック同期、コピー、置換、スキャンゲート、`--selftest`）および他ファイルは変更していません。

### 変更箇所
- [scrub_publish.py (L79-L82)](file:///home/hatch/workspace/scrub_publish.py#L79-L82): 定数 `NEVER_PUBLISH` を追加（初期エントリ `"owner_dm_poll.py"`）。
- [scrub_publish.py (L165-L216)](file:///home/hatch/workspace/scrub_publish.py#L165-L216):
  - `collect_code_files()`: `SRC` 直下と `SRC/tests/` 直下の `*.py`（ドットファイル除外、実ファイルのみ）を相対パス表記で収集。
  - `check_code_classification(paths)`:
    - `NEVER_PUBLISH` と公開対象の重複（矛盾）→ `stderr` に出力し `exit 1`
    - `NEVER_PUBLISH` の実ファイル不在（stale エントリ）→ `stderr` に警告を出して続行
    - 公開対象にも `NEVER_PUBLISH` にも無い検出 `.py` → 未分類ファイル一覧と対処を促すメッセージを `stderr` に出力し `exit 1`
- [scrub_publish.py (L246)](file:///home/hatch/workspace/scrub_publish.py#L246): `main()` 内で `published_paths()` 取得直後・コピー処理前に `check_code_classification(paths)` を呼び出し。

### 確認実行の結果

1. **現在のツリーでの実行 (`python3 /home/hatch/workspace/scrub_publish.py`)**
   - **未分類チェック**: 通過（公開対象 `agent_tools.py`, `auto_join.py`, `backfill.py`, `listener.py`, `tests/run_tests.py`＋`NEVER_PUBLISH` の `owner_dm_poll.py` で矛盾・未分類・stale なし）。75 ファイルのコピー・17 件の置換まで完了。
   - **スキャンゲート（既存機能）**: 以下の通り **`exit 1`（スキャンNG）** で停止しました。
     ```text
     スキャンNG: 公開対象ファイルから機密・個人情報パターンが検出されました:
       tests/run_tests.py: [実SlackID] U_ATA
       tests/run_tests.py: [実SlackID] U_ATA
     ```
     [run_tests.py (L9095-L9105)](file:///home/hatch/workspace/slack-yama/tests/run_tests.py#L9095-L9105) に置換表外の Slack ID `U_ATA` が含まれているためです（仕様に従い `tests/run_tests.py` や置換表・スキャンゲートは変更していません）。

2. **`SRC` 直下にダミー `.py` を置いた状態での実行**
   - `/home/hatch/workspace/slack-yama/_tmp_dummy_unclassified.py` を一時作成して実行したところ、コピー処理に進まず **`exit 1`** で停止しました。
     ```text
     エラー: 未分類のコードファイルが検出されました:
       _tmp_dummy_unclassified.py
     公開対象（同期記録）に追加するか、scrub_publish.py の NEVER_PUBLISH に理由つきで登録してください。
     ```
   - 実行後、ダミーファイルは削除し不在を確認済みです。

3. **補足確認**
   - `python3 /home/hatch/workspace/scrub_publish.py --selftest`: 全 9 ケース PASS（`exit 0`）。
   - インメモリ検証で `NEVER_PUBLISH` と公開対象の矛盾時に `exit 1`、stale エントリ時に警告表示＋処理続行となることも確認済みです。
