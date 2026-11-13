# 変更依頼: 取りこぼし補完（backfill）の追加

作業ディレクトリ: ~/workspace/slack-yama/
新規: backfill.py、変更: tests/run_tests.py（listener.py / dispatcher.sh / agent_tools.py の既存挙動は変えない）

## 背景
常駐リスナー（Socket Mode）が接続の異常に気づけないまま投稿を取りこぼす事故が実際に起きた（他Botの質問投稿が DB に入らなかった）。対策として、30秒ごとのディスパッチ実行前に、Slack API の履歴から「DB に無い投稿」を補完投入する。

## backfill.py の仕様
- ボットトークンは config/.env から読む（agent_tools.py の get_web_client を再利用してよい）。
- 対象チャンネル: C_WELCOME（#slack-へようこそ）, C_ALL（#all-muse取締役会）。定数として定義。
- 各チャンネルで conversations_history（latest 30件）を取得。reply_count > 0 の親投稿には conversations_replies（最大30件）も取得する。
- 取得した各メッセージをイベント形式に変換し、listener.should_ignore_event（bot_user_id は auth_test で取得）でフィルタし、通ったものだけ listener.save_event で保存する（event_id は save_event の既定フォールバック `channel_ts` を使う。重複は PRIMARY KEY で自然に排除される）。
- 効率と安全のため、対象は「そのチャンネルの DB 内最大 ts − 3600秒」より新しいメッセージのみ。それより古いものは投入しない。DB が空のチャンネルでは直近24時間分のみ。
- 自Botの投稿は should_ignore_event で除外される（従来通り）。
- API 呼び出しは agent_tools.py のリトライ helper（_call_with_retry）が使えるなら再利用する。全体として、一部チャンネルの取得に失敗しても他のチャンネルは処理し、最後に JSON で {"inserted": 投入件数, "channels": {...}} を stdout に出して exit 0 で終わる（フック側を止めないため、致命的な例外でも握りつぶして exit 0。エラー内容は stderr へ）。
- 引数なしで実行でき、所要は数秒以内に収まること。

## テスト（tests/run_tests.py）
- 既存の全テスト（8件）が引き続き PASS すること。
- 新規テスト: WebClient をモックし、(a) DB に無い投稿が backfill で投入され is_bot が正しく付く、(b) 既に DB にある投稿は二重投入されない、(c) 自Botの投稿と編集イベントは投入されない、ことを確認する。
- 全 PASS を確認してから完了を報告する。

## 制約
- トークンや .env の中身を表示・出力しない。
- systemd サービスの再起動はしない。
