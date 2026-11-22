# slack-yama 根本改善の継続実装依頼（オーナー指示・再実行）

あなたは Slack 常駐ボット「Yama」のコードベース（~/workspace/slack-yama/）の改善実装を担当します。
これは同フォルダの `claude_improve_brief_2026-11-22.md`（元のブリーフ、5項目）の継続実行です。
前回実行が Claude Code の利用上限で途中終了したため、現在のコード状態を自分で確認し、未了部分を仕上げてください。
元のブリーフの内容・制約・成果物はすべて引き継ぎます。以下は継続用の補足です。

## 現在の状態（2026-11-22 17:22 JST 時点、Muse が実測）
- テストは 65/66 通過（`./venv/bin/python tests/run_tests.py`）。唯一の失敗はテスト14で、失敗内容は
  `Expected rc=0 after 20-page cap, got 1`。これは項目4（送信前チェック打ち切りを fail-closed 化）の
  実装が入り、テスト側の期待値更新が未了のため（想定内の失敗）。
- `dispatcher.sh` に reap_count 実装あり（`ALTER TABLE events ADD COLUMN reap_count`、上限3回で FAILED 化）。
- `ops_monitor.py` が新規作成済み（PENDING 滞留警告用）。ただし Muse の grep では、フックスクリプトや
  dispatcher.sh から `ops_monitor.py` を呼び出している箇所が見つからなかった（dispatcher.sh の
  docstring に "same flock as ops_monitor.py" とあるのみ）。配線が本当に未了か、自分で確認すること。
- `agent_tools.py`・`backfill.py`・`listener.py` も変更済み。`backfill.py` には
  HISTORY_MAX_PAGES=3 のページング実装あり。全ファイル構文チェック通過。
- `listener.py` には heartbeat 書き込みとソケット切断60秒での exit(1) がある模様。
  ただし `yama-listener.service`（ワークスペース側と /etc/systemd/system/ 側）には WatchdogSec がない。
  ハートビートが専用スレッドかも自分で確認すること。
- レポート `claude_improve_2026-11-22.md` は未作成。
- 新機能のテスト追加が未了の可能性が高い。テスト総数 66 件の内訳を自分で確認すること。
- リスナーは稼働中（`yama-listener.service` active、2026-11-22 07:48 UTC から running）。
- `bak-2026-11-22/` に前回実行前のバックアップあり。

## やること（優先順）
1. 元のブリーフの項目1〜5について、現在のコードを自分で読んで各項目の完了度を判定する。
   推測で「完了」と書かないこと。実装・配線・テストの有無をファイルと実行結果で確認すること。
2. 未了部分を仕上げる。少なくとも以下を含む：
   - テスト14の期待値を項目4の新挙動（打ち切り時は送信せずエラー）に合わせて更新する。
   - 新機能（項目1の PENDING 滞留警告、項目2の reap_count 上限、項目3の backfill ページング、
     項目5で入れたリスナー対策）にテストを追加する。既存テストの回帰も含め、全件通過させること。
   - 項目1のフックへの配線を確認する。`ops_monitor.py` がフックの周期処理・dispatcher・watchdog の
     いずれからも呼ばれていないなら、元のブリーフの「新しい常駐プロセスは増やさない」「構文ガードと
     同じ wake 経路」「エッジ通知」の方針に沿って配線する。配線しない判断をする場合は理由をレポートに書く。
   - 項目5の残り：Yanayana 側の3対策（ハートビート専用スレッド化／ソケット切断時の自己終了／
     systemd WatchdogSec=120）と Yama 側を比べ、欠けていて安全に入れられるものを実装する。
     systemd ユニット変更時はワークスペース側と /etc 側の両方を更新し、daemon-reload とリスナー再起動まで
     行い、再起動後にリスナーが正常動作していることを確認する。再起動が危険と判断した場合は
     変更案をレポートに残して実施しない。Yama 側 watchdog（heartbeat 鮮度120秒で再起動）との
     二重化・競合は避けること。
3. レポートを `~/workspace/slack-yama/claude_improve_2026-11-22.md` に書く。各項目について
   「やったこと／変更ファイル／テスト結果／やらなかったことと理由」を簡潔に。数字（テスト件数など）は実測で書く。
4. 終了時に `./venv/bin/python tests/run_tests.py` を実行し、全件通過を確認すること。
   フックスクリプトを変更した場合は `bash -n` で構文確認すること。

## 制約（厳守・元のブリーフと同じ）
- Bot のルールは変えない：往復の扱い（上限なし）、モード、承認の線引き、返信の長さ規律（フック定義に入っているもの）。
- トークン・権限・接続設定には触らない。config/.env の中身を表示・複製しない。
- Slack への投稿はしない（警告の実装はするが、テスト送信はしない）。GitHub への push もしない。
- 個人情報：コードや報告に実ID・実メールアドレスを書かない（既存コード内の既存の値はそのままにし、新規に増やさない）。
- 既存テストは全件通過を維持する。新機能にはテストを追加する。テストの実行は `./venv/bin/python tests/run_tests.py`。
- 変更は `~/workspace/slack-yama/` 配下と、必要な場合のみ `~/hooks/scripts/`・`~/hooks/definitions/`・systemd ユニットに限る。
  フックスクリプトを変更した場合は `bash -n` で構文確認する。

## 成果物
1. 上記1〜5の実装（コード＋テスト）
2. レポート `~/workspace/slack-yama/claude_improve_2026-11-22.md`
3. 標準出力には、レポートの要約（項目ごとの成否とテスト総数）だけを短く返す
