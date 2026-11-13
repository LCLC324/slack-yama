# 依頼：Slack自律応答アシスタント「Yama」の堅牢な設計案

## 背景
Slackワークスペース「muse取締役会」（your-workspace.slack.com）に、持ち主（Owner）のAIアシスタント「Yama」（Muse）を常駐させ、**新着投稿を自動で読んで、内容に応じて自律的に返信する**仕組みを作りたい。

参考になる実働中の実装（別ユーザーのAIアシスタント「Neiro」の構成、本人が公開チャンネルで説明した内容）がある。この構成を土台に、**弱点を直したより精密・堅牢な設計案**を作ってほしい。実装はまだしない。設計案のみ。

## 参考実装（Neiro）の構成
- Slackアプリ（ボット）を作成し、ボットトークン（xoxb）で認証。送受信はサードパーティOSSの slackcli（shaharia-lab/slackcli v0.19.1、MIT）経由
- スコープ11個：channels:history, channels:join, channels:read, chat:write, files:write, groups:history, groups:read, users:read, im:read, im:history, im:write
- Socket Mode＋Event Subscriptions（message.channels/groups/im/mpim）を有効化
- Pythonリスナー（listener.py）がSocket Mode（WebSocket）で常駐接続し、新着イベントを `events/inbox.jsonl` に追記。自分のボットの発言と編集・削除イベントは inbox に入れない。Socket Mode の ack を返して再送防止
- リスナーは systemd サービス（enabled、Restart=always、RestartSec=10）。30秒ごとに heartbeat.json を更新（各ワークスペースの connected/disconnected）。5分間隔のウォッチドッグがサービスの死活を確認し、止まっていれば再起動、ユニットファイルが消えていれば入れ直す
- 処理済み管理：`state/slack-inbox.offset` に処理済み行数を保存。30秒ごとのフックが inbox.jsonl の総行数と比べ、未処理行だけを新イベントとして取り出す。ファイルが短くなっていたらオフセットを0にリセット
- フック：クラウドPC上のポーリングスクリプト（poll_interval_secs: 30）。新着があればイベント内容をペイロードにしてエージェントを起こす
- エージェントは起きるたびに新規セッション。文脈は (a) ペイロード、(b) 保存されたメモリ、(c) slackcli でスレッド・チャンネル履歴を読み直す、で再構築
- 返信判断：メンション時のみではなく全投稿が対象。自分宛の質問・挨拶・雑談とタスク依頼に返信。特定の人宛の話や持ち主本人の投稿への相づちには返さない。大きな判断や曖昧な内容は、勝手に進めず返信で持ち主に確認。対応内容は後で持ち主にまとめて報告

## 参考実装の判明している弱点（本人が認めているもの＋私の指摘）
1. **取りこぼし**：フックがイベントを渡す「前」にオフセットを進める方式なので、処理中
...[truncated 2085 chars]