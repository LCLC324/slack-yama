# 依頼：ウォッチドッグのユニット復旧を実装してください

対象プロジェクト：~/workspace/slack-yama（Yama Slack ボット）

## 背景（実データ）
2026-11-12 17:00–17:26 JST、リスナー停止中に watchdog が `systemctl restart yama-listener` を53回試行し、全て `Unit yama-listener.service not found (exit=5)` で失敗しました。ユニットファイル自体が消えている場合、現行 watchdog.sh は restart を繰り返すだけで復旧できず、外部から直すまで約30分止まりました。

この改善は自己レビュー候補1として Slack に提案され、Neiro の具体案（失敗種別での分岐＋ファイル有無の先行確認、再配置後の確認と試行上限＋backoff、対象ユニット限定の sudoers）をすべて取り入れることが決まり、Owner の承認も得ています。

## 現行 watchdog.sh の要点
- heartbeat が3分以上 stale なら `systemctl restart yama-listener`（環境変数 YAMA_RESTART_CMD で差し替え可能）を実行し、結果を state/watchdog.log に記録する。
- テストでは YAMA_STATE_DIR / YAMA_HEARTBEAT_PATH / YAMA_WATCHDOG_LOG / YAMA_RESTART_CMD 等の環境変数でモック化している（tests/run_tests.py のテスト6）。

## 実装内容（watchdog.sh）
1. **失敗種別での分岐**：restart が失敗し、出力に "not found"（Unit ... not found）が含まれる場合のみ、ユニット再配置フローに入る。それ以外の失敗では再配置しない。
2. **ファイル有無の先行確認**：再配置の前に、コピー元（ワークスペース内の yama-listener.service、環境変数 YAMA_UNIT_SOURCE で差し替え可能）と配置先ディレクトリの状態を確認する。コピー元が無ければ再配置を試みず、その旨をログに残して終了する。
3. **再配置**：コピー元を /etc/systemd/system/yama-listener.service（配置先は環境変数 YAMA_UNIT_DEST で差し替え可能）へコピーし、daemon-reload（環境変数 YAMA_DAEMON_RELOAD_CMD で差し替え可能、既定 `systemctl daemon-reload`）してから restart を再試行する。コピーコマンドも環境変数 YAMA_UNIT_INSTALL_CMD で差し替え可能にし、テストでモックできるようにすること。
4. **再配置後の確認**：restart 再試行の成否をログに残す。
5. **試行上限＋backoff**：再配置の試行回数を状態ファイル（state 配下、環境変数で差し替え可能）に記録し、上限（既定3回、環境変数 YAMA_UNIT_REINSTALL_MAX で差し替え可能）に達したら再配置をやめてログに残す。ユニットが正常に存在して restart が成功したらカウンタをリセットする。短時間に連続実行されても上限を超えて再配置を繰り返さないこと。
6. **sudoers**：再配置に sudo が必要な場合に備え、対象ユニット限定の sudoers スニペット例をプロジェクト内にファイルとして追加してください（例：deploy/sudoers-yama-watchdog.example）。内容は yama-listener.service に関する install／daemon-reload／restart のみに限定したものにし、実際に /etc/sudoers.d へは配置しない（例示ファイルのみ）。
7. sudo が使えない環境では、再配置が失敗したことを明確にログへ残し、上限管理の対象にすること（無限リトライしない）。

## テスト（tests/run_tests.py に新規テスト16を追加）
- restart が "Unit not found" で失敗 → コピー元あり → 再配置（モック）が呼ばれ、daemon-reload（モック）後に restart 再試行が成功する。
- restart が "not found" 以外の理由で失敗 → 再配置は呼ばれない。
- コピー元ファイルが無い → 再配置を試みず、ログに残る。
- 再配置が上限回数失敗済み → 再配置を試みない。
既存テスト1〜15（特にテスト6）もすべて通過させること（合計16件）。

## 注意
- 実際の systemctl や /etc/systemd/system には触らないこと（テストは全てモックと一時ディレクトリで）。
- config/.env の内容は表示・複製しないこと。
