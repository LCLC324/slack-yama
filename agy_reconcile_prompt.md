# 依頼：遅れて着地した返信の定期照合（reconcile）を実装してください

対象プロジェクト：~/workspace/slack-yama（Yama Slack ボット）

## 背景
record-reply コマンドで「履歴で見つけた既存返信を replies 台帳へ記録する」経路はできましたが、拾うタイミングはイベントの再実行時に限られます。再実行がしばらく起きないと、遅れて着地した返信（例：Ev0CGMTJ3KBR、約3分遅れで着地し台帳未記録のまま DONE になった件）が台帳に残りません。Neiro の懸念に応え、Owner の承認を得て「定期照合」を追加します。

## 実装内容
### 1. agent_tools.py に `reconcile-replies` サブコマンドを追加
- 引数：--days（既定7）、--dry-run、DB パスは既存コマンドと同じ扱い。
- 候補の抽出：events テーブルから、status が DONE または FAILED で、event_id が replies テーブルに無く、error_message に 'reply' または 'ambiguous' を含む行を、直近 --days 日分から抽出する。
- 各候補について、そのイベントのスレッド（thread_ts があればその値、無ければイベント自身の ts を親とする）を既存の履歴取得の仕組みで全体走査し、Yama 自身（U_YAMA_BOT）の投稿を探す。
- 見つけた Yama 投稿のうち、replies テーブルに reply_ts が記録されていない最も古いものを、そのイベントの返信として record_reply()（cmd_reply / record-reply と同じ書き込み関数）で台帳へ記録する。text_hash は見つけた投稿の文面から計算する。複数の候補が同じスレッドを共有する場合はイベントの古い順に処理し、同じ reply_ts を二重に割り当てない。
- Yama 投稿が見つからなかった候補は何もしない（記録しない）。
- 出力は JSON：checked（候補数）、recorded（記録した event_id と reply_ts のリスト）、not_found（見つからなかった event_id のリスト）を含める。--dry-run では記録せず、記録予定のリストを返す。
- Slack API の履歴取得が失敗した候補はエラーとして数え、処理全体は止めない。

### 2. 定期実行の組み込み
~/hooks/scripts/slack-yama-dispatch.sh に、backfill の近くへ reconcile の呼び出しを追加してください：
- state/reconcile_last_run（プロジェクトの state 配下）に前回実行時刻を記録し、前回から1時間以上空いている場合だけ実行する（毎回は実行しない）。
- 実行は best-effort（`|| true`）で、失敗してもフック全体を止めない。
- 呼び出しは `./venv/bin/python agent_tools.py reconcile-replies --days 7` 相当（スクリプト内の既存の呼び出し方に合わせる）。

## テスト（tests/run_tests.py に新規テストを追加）
Slack クライアントをモックして：
- 候補イベント（DONE・error_message に 'reply not posted' を含み replies 未記録）のスレッドに Yama 投稿がある → reconcile で replies に記録される。
- Yama 投稿が無い候補 → 記録されない。
- replies 記録済みのイベント → 候補にならない。
- --dry-run → 記録されないが予定リストに載る。
既存テストもすべて通過させること。

## 注意
- 実際の state/slack_events.db に対して reconcile を実行しないこと（テストは一時 DB とモックで）。
- config/.env の内容は表示・複製しないこと。
