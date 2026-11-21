# 実装依頼：監査で見つかった重大欠陥2件の修正

あなたは実装担当です。以下の仕様どおりに修正し、テストを全件通過させてください。仕様にない変更はしないこと。終わったら、変更ファイル一覧・テスト結果・判断に迷った点を報告してください。

作業場所: ~/workspace/slack-yama（テスト実行は ./venv/bin/python tests/run_tests.py）

## 修正1：構文ガードの警告 wake（稼働側フック ~/hooks/scripts/slack-yama-dispatch.sh）

現状の欠陥：警告 wake の第2引数（JSON文字列）で二重引用符がエスケープされておらず不正な文字列が渡る。set -e のため wake 失敗でフック全体が落ちる。warned フラグは wake の前に保存されるため、失敗すると二度と警告が出ない。テスト64の抽出範囲は eval 関数までで、wake 呼び出し部分が範囲外だったのが見逃しの原因。

仕様：
1. 編集は必ず「同じディレクトリに一時ファイルを作成 → bash -n で検査 → mv で置換」のアトミック手順で行うこと（フックは30秒ごとに直接実行され、bash が逐次読みするため、上書き編集は厳禁。ファイル冒頭の NOTE コメント参照）。
2. syntax_guard_eval の役割を変更する：警告期限が来たら "warn:<target>" を返すが、warned=true は保存しない（first_fail の記録は従来どおり eval 内でよい）。
3. 新関数 syntax_guard_mark_warned(target)：状態の syntax_guard[target].warned を true にして保存する（first_fail は保持）。
4. 警告・復旧の wake ディスパッチ部分を、テスト抽出マーカー「# --- syntax-guard escalation: begin」から「# --- syntax-guard escalation: end ---」の範囲内に収める（現在は end マーカーの後ろにあるので範囲内へ移す）。
5. ペイロードは jq -n --arg / --argjson で組み立てる（手書きエスケープ禁止）。targets は JSON 配列、first_failed_at_epoch と warn_seconds は数値。
6. wake は if 条件の中で呼ぶ（|| true は付けない）。成功したら対象全件に mark_warned する。失敗したら state/watchdog.log へ1行記録し、処理を続行する（後段のイベント処理を止めない）。ペイロード組み立て（jq）の失敗も同じ扱いにする。
7. 復旧 wake も同様に jq -n で組み立て、失敗しても続行する（復旧側に保存するフラグはない）。
8. テスト64を更新する：抽出範囲（begin〜end のブロック全体）を、wake と hook_state_get/set をスタブ化した環境で実際に実行し、次を検証する。
   a. 警告時に wake へ渡る第2引数が jq で検証可能な妥当な JSON で、targets・数値フィールドの型が正しい。
   b. wake が失敗した場合、warned は保存されず、次の実行で再度 wake が試みられる。
   c. wake が成功した場合のみ warned が保存され、2回目の実行では wake しない。
   d. 警告 wake の失敗後も、ブロック後段の処理（テスト側で置いたダミーの後続処理）が実行される。
   e. 既存の検証（10分未満では鳴らない、復旧で状態が消える等）は維持する。

## 修正2：DM 代行の auto_streak（~/workspace/slack-yama/owner_dm_poll.py と DM フック定義）

現状の欠陥：代行返信はオーナー名義で投稿されるため、次回 poll が「本人が発言した」と誤認して auto_streak を 0 に戻し、安全弁（3回で停止）が永遠に働かない。あわせて poll と resolve が同じ状態ファイルを無排他で読み書きしている。

仕様：
1. 状態ファイルの排他制御：STATE_PATH + ".lock" を fcntl.flock で排他ロックし、poll / resolve / send の「読み込み→変更→保存」をロック内で行う。save_state は従来どおり tmp + os.replace。
2. 会話ごとの状態に proxy_ts（代行が送信したメッセージ ts の文字列リスト、直近50件で切り捨て）を追加する。既存の状態ファイルにキーが無い場合は空リストとして扱う（後方互換）。
3. 新サブコマンド send：`send --user-id <相手ID> --message <本文> [--thread-ts <ts>]`
   - 送信前に、その会話の auto_streak が 3 以上なら送信せず、{"ok": false, "error": "auto_streak_limit"} を出力して終了コード 1（安全弁をコード側で強制する）。
   - 送信は既存の read_dm と同じ Slack CLI 方式：/opt/hatch/bin/slack call-tool --name slack_send_message --arguments-json '{"channel_id": "<user_id>", "message": "<本文>"}'（thread 時は "thread_ts" を追加）。
   - 送信成功時、結果 JSON からメッセージ ts を抽出する（result.content[0].text 内の JSON の "ts" フィールド、または "Message TS: ..." 表記の両方に対応する）。
   - 抽出した ts を、その会話の proxy_ts に追加して保存し、{"ok": true, "ts": "<ts>"} を出力する。ts が抽出できない場合は {"ok": false, "error": "ts_not_found"} で終了コード 1（送信自体は済んでいる可能性があるため、エラー文にその旨を含める）。
   - auto_streak の加算は send では行わない（従来どおり resolve の役割）。
4. resolve の拡張：任意引数 --reply-ts を追加。action="replied" で --reply-ts があれば、その ts を proxy_ts に追加する（send を使わずコネクタで送った場合の予備経路）。streak の +1 は従来どおり。
5. poll のリセット判定の修正：最新1件だけではなく、前回水位（top_ts）より新しい全メッセージを走査し、「オーナー名義かつ proxy_ts に含まれない ts」が1件でもあれば auto_streak を 0 に戻す。proxy_ts に含まれるものだけなら戻さない。水位の前進や pending 検出の既存動作は変えない。
6. フック定義 ~/hooks/definitions/slack-owner-dm-dispatch.json の prompt を更新する：手順4の送信方法を「python3 ~/workspace/slack-yama/owner_dm_poll.py send --user-id <相手ID> --message <本文>（thread 時は --thread-ts <parent_ts>）を使う。send が auto_streak_limit で失敗したら送信せず resolve skipped にする。send が使えない場合のみ従来のコネクタ送信を使い、その場合は送信 ts を resolve の --reply-ts に必ず渡す」に書き換える。他の手順・文言は変えない。JSON として妥当な形を保つこと。
7. テストを新設する（tests/run_tests.py に追加し、総件数が増えること）：owner_dm_poll を import し、read_dm と送信関数をスタブ化して検証する。
   a. 代行返信が3回続くケース：send→resolve(replied) を繰り返し、途中の poll で streak が戻らず、1→2→3 と上がる。3 に達したら send が auto_streak_limit で拒否される。
   b. オーナー本人の実投稿（proxy_ts に無い ts）が水位より後にあれば、poll で streak が 0 に戻る。
   c. resolve --reply-ts の予備経路でも proxy_ts に記録され、リセットされない。
   d. proxy_ts が50件を超えたら古いものから落ちる。
   e. ts は文字列のまま完全一致で扱われる（数値化による末尾ゼロ落ちがない）。

## 制約
- 仕様外の挙動変更をしない。とくに修正1では構文ガード以外のフック処理、修正2では pending 検出・水位・handled の既存動作を変えない。
- テストは全件（既存65件＋追加分）通過させること。失敗したら原因を直してから報告する。
- config/.env の中身は読まない・出力しない。
- 報告には、変更したファイルと行の目安、追加・変更したテストの番号と内容、実行したテストの結果（件数）を含めること。
