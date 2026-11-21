# コード点検のお願い（読み取り専用。コードは一切変更しないこと）

Slack 常駐ボット「Yama」のコードベース全体を点検してください。あなたは外部の監査役です。実装者（Muse）が自分のコードを過大評価していないか、第三者の目で確認するのが目的です。

## 対象（すべて読むこと）
プロジェクト: ~/workspace/slack-yama/
- agent_tools.py（中核：投稿・履歴読み・重複防止・照合・curlフォールバック）
- listener.py（イベント受信）
- dispatcher.sh / watchdog.sh（処理の駆動・再起動）
- backfill.py（取りこぼし補完）
- auto_join.py（チャンネル自動参加＋statusファイル）
- owner_dm_poll.py（オーナーDM代行の検知）
- tests/run_tests.py（テスト65件）
- IMPROVEMENTS.md（実装済み・見送り・候補の記録。主張と実装の食い違いを探す材料）
稼働側フック:
- ~/hooks/scripts/slack-yama-dispatch.sh
- ~/hooks/scripts/slack-owner-dm.sh
- ~/hooks/definitions/slack-yama-dispatch.json
- ~/hooks/definitions/slack-owner-dm-dispatch.json

## 点検の観点
1. バグ・競合条件・例外処理の穴（特に送信経路：二重投稿が起きうる経路が残っていないか）
2. 重複防止の仕組み（event_id 冪等化、投稿前チェック、台帳照合、遅延再送の答え済み判定）に抜けがないか
3. 個人情報・秘密情報の漏洩経路（ログ出力、例外メッセージ、状態ファイルへの書き込み等にトークンや実IDが残らないか）
4. IMPROVEMENTS.md に「実装済み」と書かれている内容と、実際のコードの食い違い
5. テスト65件が担保している範囲と、担保されていない重要経路
6. 障害時の振る舞い（fail-closed / fail-open の使い分けが意図どおりか、無限リトライや詰まりがないか）

## 出力
レポートを ~/workspace/slack-yama/claude_audit_2026-11-21.md に書いてください。形式：
- 冒頭に総評（3〜5行）
- 指摘は重要度順（重大／中／軽微）に、各指摘へ「場所（ファイルと関数・行の目安）／問題／根拠（コードの該当箇所）／直すならどう直すか」を書く
- 食い違いがなければ「なし」と明記する。推測だけで重大にしないこと。コードで確認できたことだけを書く
- 良い点（設計として効いているところ）も最後に短く書く

レポートを書き終えたら、標準出力には総評と重大指摘の件数だけを短く返してください。
