# 変更依頼: 他Botの投稿も受信する（is_bot フラグ付き）

作業ディレクトリ: ~/workspace/slack-yama/
対象: listener.py, dispatcher.sh, agent_tools.py, tests/run_tests.py

## 背景と目的
現状 listener.py は bot_id を持つ投稿を全て除外しているため、他のBot（Neiro、Yanayana等）の発言を一切受け取れず、Bot同士の会話が成立しない。方針を変更し、**他のBotの投稿は is_bot=1 のフラグ付きで受信**する。自Botの投稿は今まで通り除外する（自分への反応ループ防止）。Bot同士の応答抑制（メンション時のみ返信、スレッド往復回数の上限）は上位レイヤー（トリアージ側）で既に担保する設計なので、今回の変更範囲は受信・保存・受け渡しまで。

## 変更内容

### 1. listener.py — should_ignore_event
- Bot判定: `event.get("bot_id") is not None` または `subtype == "bot_message"` のいずれかでBot投稿とみなす。
- Bot投稿かつ `user == bot_user_id`（自Bot）→ ignore（理由 self_bot_message）。
- Bot投稿かつ他Bot → Botであることを理由には ignore しない。他のチェック（user必須、text必須、channel/ts必須、hidden除外、編集・削除・参加等のsubtype除外）は従来通り適用する。
- subtypeゲート: "bot_message" は subtype チェックで落とさないこと。ただし message_changed / message_deleted / channel_join / channel_leave 等はBot投稿でも従来通り除外。
- ignored_user_ids の扱いは従来通り。

### 2. listener.py — DBスキーマと保存
- events テーブルに `is_bot INTEGER NOT NULL DEFAULT 0` を追加。CREATE TABLE 文にも含め、既存DBに対しては init_db 内で PRAGMA table_info を確認して無ければ ALTER TABLE で追加（マイグレーション、冪等）。
- save_event: is_bot を算出（Bot判定と同じ条件で 1/0）して INSERT に含める。

### 3. dispatcher.sh
- payload.json に書き出すイベントの SELECT と dict に is_bot を含める。

### 4. agent_tools.py
- イベント行を返す SELECT（pending / get_processing_events 等）すべてに is_bot を含める。

### 5. tests/run_tests.py
- テスト2（除外フィルタ）の期待値を新仕様に更新: 他Botの投稿は保存され is_bot=1、自Botの投稿は除外、message_changed/message_deleted は引き続き除外。その他の既存アサーションは維持。
- テスト3（ディスパッチャ）: payload のイベントに is_bot キーが含まれることを確認するアサーションを追加（Botイベントを1件混ぜて is_bot=1 で渡ることまで確認できるとなお良い）。
- テスト6の mode アサーションは現行の「有効値のいずれか」を維持すること（shadow固定に戻さない）。
- 全6テストが PASS すること。

## 制約
- event_id による重複排除、heartbeat、watchdog、commit の状態遷移など、他の挙動は変えない。
- トークンや .env の中身を表示・出力しない。
- 変更後に `~/workspace/slack-yama/venv/bin/python tests/run_tests.py` を実行し、全PASSを確認してから完了を報告する。
- systemd サービスの再起動はしない（デプロイは別途行う）。
