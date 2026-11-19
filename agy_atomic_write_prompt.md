# agy レビュー依頼：編集中のファイルに定期実行が当たる罠（Yama側）

あなたは設計レビュー役です。ツールを使わず、文章だけで答えてください。
コードは書かないでください。結論と理由、採用すべき範囲の判断材料をください。

## 背景（Neiro側の事故、2026-11-19）
- Neiro の同期チェック用スクリプト slack-ids.sh を編集中、5分間隔のタイマーが
  書きかけのファイルを読み込み、途中で切れた行で syntax error（exit 2）。
- 原因はファイル欠落ではなく、書き込み中に読み取りが当たったこと。
- Neiro の教訓案：書き込みをアトミックにするか、チェック側で bash -n を先に見るか。
- Yanayana の共有：同社ではフック定義・スクリプトの保存は「一時ファイルへ書いて
  からリネームする」原子化ルールにしている。書き手側の原子化と読み手側の検査は
  両輪だという整理。Neiro も原子化を採用する方向。

## Yama側の実情（確認済み）
Yama には Neiro のような同期チェックの仕組み自体はない。ただし定期的に
ファイルを実行・読み込む経路があり、同じ罠があり得る。書き手は Muse（と
Muse から委任された agy）で、ファイルツールによる直接の上書きが主。
書き込みがアトミックかどうかは保証されていないものとして扱う。

定期実行される読み取り・実行の経路（頻度と読み方）：
1. ~/hooks/scripts/slack-yama-dispatch.sh — 基盤が30秒ごとに直接実行する。
   bash はスクリプトを逐次読みながら実行するため、編集中に当たると
   途中まで古い内容・途中から新しい内容の混ざった実行になり得る。
   このファイル自体を外から事前検査する仕組みはない（基盤が直接実行する）。
   シンボリックリンクにはできない（基盤の検証で弾かれる）。稼働側が正本。
2. ~/workspace/slack-yama/dispatcher.sh — 上記フックから30秒ごとに実行される
   bash スクリプト（中身は heredoc の Python が本体）。同じく逐次読み。
3. ~/workspace/slack-yama/watchdog.sh — 上記フックから30秒ごとに実行される
   bash スクリプト（heredoc の Python を含む）。同じく逐次読み。
4. ~/workspace/slack-yama/backfill.py — フックから30秒ごとに venv の
   python で実行（失敗しても無視される best-effort）。Python は起動時に
   ファイル全体をコンパイルするため、書きかけに当たると SyntaxError で
   その回だけ失敗し、次回は直っている。
5. ~/workspace/slack-yama/auto_join.py — フックから5分ゲートで同様に実行。
6. agent_tools.py — フックから1時間ゲートで reconcile 用に実行されるほか、
   ディスパッチの処理役（別エージェント）が都度実行する。起動時コンパイル。
7. ~/workspace/slack-yama/listener.py — systemd サービス yama-listener が
   起動時に1回だけ読む常駐プロセス（Restart=always、RestartSec=10）。
   実行中の編集は動いているプロセスに影響しない。再起動（watchdog による
   heartbeat 枯渇時の再起動、またはクラッシュ時）が編集中に当たると
   SyntaxError で起動に失敗するが、systemd が10秒ごとに再試行するため
   編集が終われば自己復旧する。
8. ~/hooks/scripts/slack-owner-dm.sh — 60秒ごとに実行され、中で
   owner_dm_poll.py を system python3 で実行する。失敗は握りつぶされ
   その回は wake しない（次回に自己復旧）。
9. ~/workspace/slack-yama/yama-listener.service — systemd が
   daemon-reload・再起動時に読む。フックの自己修復と watchdog の再
   インストール時に cp／install で上書きされる（非アトミック）が、
   読まれるのは再起動・リロードのタイミングのみ。
10. 既にアトミックな箇所：state/payload.json（tmp＋置換）、config/.env の
    プロキシ同期（tmp＋mv）、state/auto_join_state.json（tmp＋置換）。
    scrub_publish.py のフック同期は shutil.copyfile（非アトミック）だが、
    コピー先はワークスペース内の公開用コピーで、定期実行の読み手はいない。

## 論点
1. 書き込み側のアトミック化（tmp へ書いて検証してから mv）だけで足りるか。
   対象ファイルは 1〜3、7、8 のスクリプト類と、4〜6 の Python 類。
   ただし書き手は人間ではなくエージェント（Muse／agy）で、習慣としての
   ルールだけだと将来の別セッションが直接上書きする抜け道が残る。
2. 読み取り側の構文チェック（bash -n／python のコンパイルチェック）を
   先に見るガードは要るか。要るとすればどこまでか。候補：
   (a) slack-yama-dispatch.sh の中で、実行前に dispatcher.sh・watchdog.sh
       へ bash -n、backfill.py・auto_join.py へ py_compile をかけて、
       失敗したらその回の実行を飛ばす。
   (b) watchdog.sh の再起動直前に listener.py へ py_compile をかけ、
       失敗したら再起動を見送って次回に回す（再起動ループの空回り防止）。
   (c) slack-owner-dm.sh の中で owner_dm_poll.py へ py_compile をかける。
   (d) agent_tools.py は都度の実行で失敗が FAILED として再試行される
       仕組みがあるため、ガードは不要ではないか。
   なお (a) のガード自体はフックスクリプト内に書くため、フックスクリプト
   自身の編集中の罠はガードでは塞げない（書き手側の原子化が要る）。
   また bash -n は heredoc 内の Python は検査しない。
3. ほかに、軽く済ませる範囲で見落としがないか。たとえば
   yama-listener.service の cp／install の非アトミック性や、
   scrub_publish.py のフック同期 copyfile の非アトミック性は、
   読まれるタイミングを考えると対応不要か。

## 制約
- 設計方針は「重い処理を避け、軽くサクサク」。過剰な仕組みは入れない。
- 1回のポーリング（30秒）あたりの追加コストは小さいこと。
- 失敗時の挙動は fail-safe であること。壊れたファイルを無理に実行する
  より、その回を飛ばして次回に回す方がよい。ただし、恒久的に壊れた
  ファイルがある場合に、ガードが無言で飛ばし続けて気づけない形は避けたい。

## 求める答え
- 論点1〜3それぞれへの賛否と理由。
- 採用すべき最小の組み合わせ（何を・どこに・なぜ）。
- 採用しないものの理由（過剰・効果なし・副作用のどれか）。
- 実装上の注意点（bash -n や py_compile の落とし穴、競合条件の残りなど）。
