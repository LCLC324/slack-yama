# 点検依頼：Slack ボット Yama のコードと決まりの食い違い探し（agy Neon）

あなたは設計・コードの点検役です。対象は Slack ワークスペース「muse取締役会」で常駐するボット Yama の実装一式です。

## 絶対の制約

- **コード・設定・テストは一切変更しない**。書き込むのは成果物のレビューファイル1つだけ。
- Slack への投稿や、本番ワークスペースに対するコマンド実行（agent_tools.py の reply／history 等）はしない。
- テストの実行は可：`./venv/bin/python tests/run_tests.py`（フィクスチャで動き、本番には触れない）。
- 成果物：`~/workspace/slack-yama/slack_review_2026-11-25.md` に日本語で書く。

## 読む対象（~/workspace/slack-yama/ 配下）

- agent_tools.py（中核。返信・照合・commit・requeue 等）
- listener.py、dispatcher.sh、watchdog.sh、backfill.py、ops_monitor.py、context_bundle.py、owner_dm_poll.py、auto_join.py
- tests/run_tests.py
- IMPROVEMENTS.md（改善候補の台帳。候補番号つき）
- フック定義：~/hooks/definitions/slack-yama-dispatch.json と ~/hooks/definitions/slack-owner-dm-dispatch.json（ボットへの指示文＝「決まり」の正本）
- フックスクリプト：~/hooks/scripts/slack-yama-dispatch.sh

## 点検の焦点

### 1. 決まり（フック定義の指示文）と実装の食い違い

疑い済みの2件を裏取りし、同じ種類の食い違いを他にも探すこと。

- 「答え済み」の定義：フック定義の文面（Bot を含む読み方）と、実装 `_find_answering_message_in_thread`（人間の投稿しか数えない）がずれている、という未決事項がある。
- フック定義の「システム構成」の説明に、PENDING 滞留警告の ops_monitor.py が載っていない（2026-11-22 実装済み）。

### 2. 既知の未決候補の裏取り（IMPROVEMENTS.md の候補番号と対応づけること）

- 候補29：(a) DONE／IGNORED のイベントに `commit --status FAILED --force --reason` を使うと PENDING に戻ることがある。(b) backfill.py:198 付近に、TypeError 時に oldest と cursor を外して呼び直すフォールバックが残っている。
- 候補32：state/triage_log.jsonl に壊れた JSON 行が再発している（2026-11-24 に2件、行の途中で切れている）。書き込み経路が特定できていない（ワークスペース内に .py／.sh の書き手は見つかっていない。フック定義は「判定を triage_log.jsonl に1行ずつ追記する」とディスパッチ役のエージェントに指示している）。書き込みの仕組み上、なぜ途中で切れるのか、どう直すのが一番小さいか。
- 候補23：ログと下書きの際限ない蓄積。state/watchdog.log は約2MB（1日約158KB増）、state/reply-*.txt は776件（1日約87件増）。
- 候補33：2026-11-25 17:27〜17:57 JST にリスナーとフックが約30分無言で止まった（その後再起動で復旧、実害なし）。リスナーには systemd の WatchdogSec=180 と sd_notify がある。それでも30分無言になった設計上の隙間はどこか。コードとフックスクリプトから読み取れる範囲でよい（当時の基盤側の事情は分からなくてよい）。

### 3. 新しい穴

- 二重投稿が起きうる経路（送信経路は1か所のはず、という前提の裏取りを含む）
- 終端ガード（DONE／IGNORED／FAILED を終端とし、訂正は --force --reason のみ）の抜け道
- 失敗が無言で消える経路（警告が出ない、台帳に残らない、次回に持ち越されない）
- コメント・docstring の主張とコードの食い違い

## 成果物の形

- 指摘は重要度順（重大／中／軽微）に並べる。
- 各指摘に、根拠（ファイル名と行番号、該当する決まりの文面）を必ず付ける。推測で書かない。裏が取れないものは「未確認」と明記する。
- 各指摘に、IMPROVEMENTS.md の既存候補でカバー済みか（候補番号）、新規かを書く。
- 最後に「直す価値の高い順」の短いまとめを付ける。
- 虚偽や誇張をしない。レポートの主張とコードが一致しているかを、後から別の担当が全件検証する。
