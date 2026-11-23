# commit_event の done ガード不在は実際に起きうるか（調査、2026-11-23）

方針：調査のみ。リポジトリのコードは変更していない。根拠は実コードの読解と、手元データ（`state/slack_events.db` の読み取り専用照会、`~/hooks/logs/slack-yama-dispatch.jsonl`、`state/triage_log.jsonl`、`state/decisions-*.json`）。
テストへの影響だけは、`/tmp` に作った使い捨てのコピーへ試作ガードを当てて実測した（本体は無変更。コピーは削除済み）。Slack への通信はしていない。

---

## 0. 結論

1. **実装上、commit は現在の状態を一切見ずに上書きする。** DONE→PENDING／DONE→FAILED／IGNORED→DONE／FAILED→DONE、さらに Reaper の上限で止めた FAILED→PENDING まで、すべて通る（§1）。
2. **「遅れた commit」の前提条件は実際に起きた。** 2026-11-22 08:27 に出したバッチが15分を超え、08:43 に Reaper が5件を差し戻し、同じトランザクションで再ディスパッチした（§2-1）。ただし、その回の最終 commit は1回の連続した書き込みだけで、**遅れた上書きの痕跡はない。**
3. **実データ上、DONE から戻った形跡はゼロ**（§3）。見つかった食い違いは3種類で、どれもガード不在が原因ではない：(a) 意図的な IGNORED→DONE の訂正（11-23、ワーカー自身による正当な上書き）、(b) reconcile が着地を確認した FAILED イベント1件（events は FAILED のまま）、(c) DONE 確定の3時間後に、その event_id 付きで返信が投稿された1件。
4. **推奨：条件付きで入れる。** 第1段として「終端状態（DONE／IGNORED／FAILED）からの別状態への commit を拒否、同一状態は no-op、訂正は明示フラグ」を commit_event の1か所に入れる。試作では**既存テスト 81/81 がそのまま通った。** 「PROCESSING からのみ許可」の厳格版は 76/81（5件失敗）で、しかも本命の競合（A の遅れた commit が、B が持っている PROCESSING を上書きする）は防げない。本命を防ぐには第2段の claim トークンが要る（§4）。

---

## 1. commit_event／commit-batch の実装と、可能な遷移

### 1-1. 実装の事実
- `commit_event`（agent_tools.py:454-640）は `BEGIN IMMEDIATE` の中で行を SELECT する（:517）が、使うのは `retry_count`・`first_unverifiable_at`・`error_message`・`created_at`/`updated_at` だけで、**`status` は読んでいるのに判定に使っていない。** 拒否するのは「event_id が無い」（LookupError, :526）と「status の値が不正」（:479）だけ。
- UPDATE はすべて `WHERE event_id = ?` だけで、`AND status = 'PROCESSING'` のような条件が無い（:574, :600, :617）。
- `commit_batch`（:3200-3251）の検証は「形式・重複・存在」だけで、状態は見ない。各件は `commit_event` をそのまま呼び、**1件ずつ別トランザクション**（検証は全件一括だが、書き込みは全件一括ではない）。
- 内部からの呼び出しがもう1つある：`cmd_reply` の「他者が回答済み」経路が `commit_event(DONE)` を呼ぶ（:2653）。例外は `except: pass` で握りつぶし、出力の `status` は既定値 "DONE" のまま返る（:2650, :2658-2660）。

### 1-2. 実装から導いた遷移表（現在の状態 → commit の内容 → 結果）
「任意」は PENDING／PROCESSING／DONE／IGNORED／FAILED のどれでも同じ、の意味。

| 現在 | commit | 結果 | 備考 |
|---|---|---|---|
| 任意 | DONE | **DONE** | error_message を引数値（無ければ NULL）で上書き、next_retry_at／first_unverifiable_at を消去、retry_count は維持 |
| 任意 | IGNORED | **IGNORED** | 同上 |
| 任意（retry_count 0〜1） | FAILED（通常） | **PENDING**（retry_count+1） | DONE→PENDING、IGNORED→PENDING が起きる。再ディスパッチされる |
| 任意（retry_count ≥2） | FAILED（通常） | **FAILED**（retry_count+1） | DONE→FAILED。DLQ 済み FAILED にもう一度来ると retry_count 4, 5… と増える |
| 任意（初回から24h未満） | FAILED（unverifiable） | **PENDING**＋next_retry_at | DONE→PENDING（待ち）。retry_count は消費しない |
| 任意（初回から24h以上） | FAILED（unverifiable） | **FAILED**（abandoned） | |
| FAILED（`reaped_too_many_times`、retry_count 0） | FAILED（通常） | **PENDING** | Reaper の上限停止を、requeue を通らずに外せてしまう。reap_count は残るので、次に差し戻されると即 FAILED |
| PENDING（未ディスパッチ） | DONE／IGNORED | 終端 | 一度も処理されずに閉じられる |
| PROCESSING（別バッチが保持中） | 何でも | 上書き | 現在の保持者を区別する手段が無い |

まとめ：**DONE→FAILED、DONE→PENDING、FAILED→DONE、IGNORED→DONE、FAILED→PENDING のどれも実装上は可能。**

### 1-3. events.status に書き込む他の経路（参考。どれも状態を確認している）
| 経路 | 遷移 | 状態の確認 |
|---|---|---|
| dispatcher（dispatcher.sh） | PENDING→PROCESSING | `BEGIN IMMEDIATE` 内で `status='PENDING'` を選んだ行だけ |
| Reaper（dispatcher.sh） | PROCESSING(≥900s)→PENDING／FAILED | 同じトランザクション内で `status='PROCESSING'` を選んだ行だけ |
| requeue（:3431） | FAILED→PENDING | FAILED 以外は拒否（:3446） |
| reconcile（:1765） | **status は変えない** | replies に記録し、DONE の error_message を消すだけ（`WHERE status='DONE'`, :1657） |
| listener／backfill | 新規挿入のみ | `INSERT OR IGNORE`（既存行は触らない） |

状態を確認せずに書いているのは commit_event だけ。

---

## 2. 遅れた commit が実際に起きうる経路

### 2-0. まず前提の訂正：「ワーカーのタイムアウト600秒」は存在しない
`script_timeout_secs: 600`（~/hooks/definitions/slack-yama-dispatch.json）は**フックのスクリプト**（watchdog・dispatcher・backfill など）の上限で、wake で起こされる**ワーカー（LLM のセッション）の上限ではない**。dispatcher.sh のコメント（「hook's script_timeout_secs (600s) より十分長い 900s」）もこの2つを混同しているように読める。ワーカーには、こちらで強制する打ち切りが無い。したがって **「ワーカーは15分以内に必ず終わる」という保証は無い。**

### 2-1. Reaper の差し戻し（PROCESSING 15分）→ **起きうる（前提条件は実際に発生した）**
フックログ（11-21 03:16〜11-23 08:31、約53時間、バッチ335回）を再構成した結果：
- 11-22 08:27:58 バッチA（5件：Ev0CL0NG3M8R ほか）をディスパッチ。
- 08:43:21 Reaper が A の5件を差し戻し（reap_count=1）、**同じトランザクションで**別の5件と合わせて**バッチB（10件）として再ディスパッチ**し、wake した。08:43:46 に reaper の運用アラート。
- 08:48:12 Yama の返信が1件（「まとめて返します」、B にしか無い Ev0CKHF19M1D の内容にも答えている）。
- 08:48:45〜48 **10件すべての commit が連続して1回だけ**入り、triage_log も10件とも 17:49:18 JST の同時刻。
- バッチ所要時間の分布：p50 60秒、p90 150秒、p99 300秒、最大 930秒（この1回）。≥900秒は335回中1回、Reaper アラートも全期間でこの1回だけ。

読み取れること：
- 「A のワーカーがまだ動いている間に B が起こされる」状況は実際に作られた。これが遅れた commit の前提条件。
- 今回は、最後に commit したのが B を知っている1つのワーカーで、A 由来の遅れた書き込みは無かった（あれば updated_at が 08:48:48 より後になる）。A のワーカーが途中で終わったのか、B の wake が同じ side_chat の会話に合流したのかは、手元の記録では判別できない。**同じ side_chat に2つのワーカーが並行して走りうるかどうかはプラットフォーム側の挙動で、未確認。**
- 競合が起きた場合の形：B が DONE を確定 → A が遅れて FAILED（例：A 自身の reply が ambiguous）→ DONE が PENDING に戻る → 3回目の処理。逆に B が FAILED（再試行待ち）→ A が遅れて DONE → 再試行が消える。もう1つ、A の遅れた commit が「B が保持中の PROCESSING」を直接上書きする形もある。

### 2-2. ワーカーの長時間化と commit の競合 → **起きうる（2-1 と同じ経路）**
上記のとおり、ワーカー側に打ち切りが無いので、遅いワーカーは15分を過ぎても commit しうる。それを止める仕組みは無い。安全用の読み取り期限（`safety_read_budget_seconds`：Reaper の締切から逆算して投稿前チェックを打ち切る、:250-268）は **reply の前**にだけ効き、commit には効かない。また、A の手元にある updated_at が古いままなら残り時間は0と計算され、A の reply は fail-closed で拒否される。この点は二重投稿をある程度防いでいる。ただし判定に使うのは `--event-id` で読み直した現在の行なので、B が再ディスパッチして updated_at を更新した後なら、**A の reply も新しい予算で通ってしまう。**

### 2-3. 遅延再送（pre_post_check_unverifiable の待ち）→ **単独では起きない**
FAILED(unverifiable) の commit は、イベントを `PENDING＋next_retry_at` に置いてワーカーが終わる。待ちが明けると dispatcher が再ディスパッチし、別のワーカーが最初から処理する。commit した側はもう居ないので、遅れて届く commit は生まれない。triage_log の「FAILED → 約1時間後 DONE」の11組（11-14〜11-19）は、すべてこの正規の流れ（DB 上は PENDING→PROCESSING→DONE）だった。
ただし、待ちの最中（PENDING）に誰かが commit すると、待ちが消える。これは §1-2 の「未ディスパッチの PENDING への commit」と同じ形。

### 2-4. reconcile の回収 → **起きない（status に触らない）**
reconcile は replies 台帳への記録と、DONE の error_message の消去だけを行い、commit_event を呼ばない。したがって、ガードを入れても影響を受けない。
副作用として「着地は確認できたが events は FAILED のまま」という食い違いが残る（実例 Ev0CH5CWKHM0、§3）。これは設計どおりの動き。

### 2-5. 手動 requeue → **起きない（requeue 自体は守られている）**
requeue は FAILED 以外を拒否する。requeue の後は通常の再ディスパッチになるだけ。
注意点：通常の FAILED commit を使えば、requeue を通らずに FAILED→PENDING にできてしまう（§1-2）。

### 2-6. 表に無かった経路（棚卸しで見つかったもの）
| 経路 | 判定 | 根拠 |
|---|---|---|
| **意図的な訂正の上書き** | **実際に起きた（正当）** | 11-23 11:44 JST、`decisions-20261123-1144.json` で IGNORED にした2件（Ev0CLWRV5JKS, Ev0CL665SRT6）を、`decisions-20261123-1144-correct.json` で DONE（理由「情報のない往復（ループ防止）」）に上書きしている。ガードはこれを壊してはいけない |
| 古い decisions ファイルの再実行・event_id の取り違え | 起きうる（人為ミス） | state/ に decisions-*.json が30件近く残っている。commit-batch は状態を見ないので、再実行すると確定済みの件を黙って上書きする |
| 終端後の reply | 実際に起きた（commit の上書きではない） | Ev0CHRRPQ5C1：11-14 04:28 に「答え済みのため再送せず」で DONE。07:44 にその event_id 付きで返信が投稿され、台帳に記録された。cmd_reply はイベントの状態を見ない |

---

## 3. 実データの照合（state/slack_events.db、読み取り専用、2026-11-23 08:31 時点）

前提：**状態の履歴テーブルは無い**ので、「戻った」ことは直接は観測できない。以下の間接指標で見た。

| 指標 | 結果 |
|---|---|
| 件数 | events 1,556（DONE 949／IGNORED 599／FAILED 5／PENDING 2／PROCESSING 1）、replies 673 |
| updated_at < created_at（逆行） | **0件** |
| updated_at が未来 | **0件** |
| replies があるのに status≠DONE | **1件**：Ev0CH5CWKHM0（FAILED、retry 3）。11-12 に ambiguous_post_outcome で FAILED、11-13 に reconcile が着地を確認して台帳に記録。status は FAILED のまま（§2-4 の設計どおり） |
| IGNORED なのに replies あり | 0件 |
| DONE なのに error_message が残っている | 0件（reconcile が消している） |
| replies 記録が events 最終更新より後 | 2件：上記の Ev0CH5CWKHM0 と、Ev0CHRRPQ5C1（§2-6、終端後の reply） |
| reap_count>0 | 5件（すべて 11-22 08:43 の1回）。どれも DONE／IGNORED で正常に終端している |
| FAILED が PENDING に戻った形跡（retry_count>0 で DONE） | 30件。すべて 11-12〜11-18 の正規のリトライ。うち FAILED 終端の5件は 11-12 の旧仕様（unverifiable が retry_count を消費していた頃） |
| replies 中で event に紐付かないもの | 8件（`share-*-20261112`：共有投稿用の擬似 event_id。仕様） |
| 1つの reply_ts を複数の event が共有 | 35組（「まとめて返信」の仕様。食い違いではない） |
| triage_log で同じ event に複数行 | 12件。すべて「待ち → 再処理」か、記録の重複。DONE→別状態の流れは無い |
| decisions ファイルで同じ event を2回 commit | 2件（§2-6 の正当な訂正） |

**結論：DONE（や IGNORED）から意図せず戻った形跡は見つからない。** 唯一の終端の上書きは、意図的な訂正だった。

---

## 4. ガードの設計案

### 4-1. 方針：2段に分ける
本命の競合（A の遅れた commit が、B の PROCESSING や B の確定を上書きする）は、**状態だけでは見分けられない**。A から見ても B から見ても、対象は「PROCESSING」だから。状態のガードで防げるのは「確定後の上書き」までで、それより先を防ぐには「誰が保持しているか」の印が要る。

### 4-2. 第1段（推奨・小さい）：終端ガード
`commit_event` の `BEGIN IMMEDIATE` の中、行を SELECT した直後（:526 の後）に1か所だけ追加する。commit・commit-batch・cmd_reply の内部 commit のすべてに効く。

| 現在 | commit | 扱い |
|---|---|---|
| PENDING／PROCESSING | 何でも | 従来どおり |
| DONE／IGNORED | 同じ状態 | **no-op の成功**（行は変えず、`"noop": true` を返す。冪等性のテストと、人がやり直したときのため） |
| DONE／IGNORED | 別の状態 | **拒否**（`already_terminal`、exit 1） |
| FAILED | 何でも | **拒否**。戻すのは requeue（→PENDING→再処理）が正規の経路 |
| 終端 | 別の状態＋`--force --reason <文>` | **許可**。11-23 型の訂正用。reason を error_message に残し、運用アラートの outbox に1行書く |

- **FAILED も終端として扱う理由**：FAILED→PENDING（通常の FAILED commit）が Reaper の上限停止と requeue を迂回してしまうため。FAILED→DONE が正当になるのは「後から着地が確認できた」場合だけで、それは reconcile の領分（今は status を変えない設計）か、人の `--force` で行う。
- **手動 requeue**：変更しない（もともと FAILED だけを受け付けるので、ガードと矛盾しない）。DONE／IGNORED をやり直したい場合のために requeue `--allow-terminal` を足すかどうかは、必要が出てから決めればよい。今のところ実例は無い。
- **reconcile**：commit_event を通らないので影響なし。将来「着地を確認できた FAILED を DONE にする」場合は、commit を流用せず、条件（台帳に記録あり・FAILED・理由が ambiguous／unverifiable）を明示した専用の関数にする。
- **commit-batch**：全件一括の検証フェーズにも状態チェックを足す（終端への別状態は「1件でも不正なら何もしない」側に入れる。同一状態は no-op として通す）。検証と書き込みの間の競合は、commit_event 側で必ず再確認されるので、二重に守られる。
- **cmd_reply の内部 commit**（:2653）：拒否は今も握りつぶされる。出力の `status` が実際の値でなく "DONE" になる点だけ直す（現在の行を読んで返す）。
- **指示文（フックのプロンプト）**：「commit が `already_terminal` を返したら、それは他の処理で確定済みということ。再試行せず報告に1行含める」を1行足す。

**既存テストへの影響（実測）**：使い捨てコピーに上の表と同じ判定（`--force` 以外）を当てたところ、**81/81 がそのまま通過**した。同一状態の no-op にしたので、DONE→DONE の冪等性テスト（run_tests.py:5312 付近）も通る。新しく要るテストは、遷移表の各行（拒否・no-op・`--force`・commit-batch の検証での拒否・FAILED から requeue を経て DONE になる流れ）。

参考：「PROCESSING からのみ許可」の厳格版は **76/81** で、テスト 4・17・26・32・74 が失敗した。これらは PENDING の行に続けて commit してリトライや待ちを確かめているので、テストの作りを大きく直す必要がある。それでいて本命の競合は防げないので、採らない。

### 4-3. 第2段（並行ワーカーが実在すると確認できたら）：claim トークン
- dispatcher が PENDING→PROCESSING にするときに `claim_id`（uuid）列を書き、payload の各イベントに入れる。Reaper の差し戻しでは claim_id を消す。
- commit／commit-batch（各件に `claim_id`）と、**reply** にも `--claim` を足す。DB の claim_id と一致しなければ `stale_claim` で拒否する。これで A の遅れた commit も遅れた reply も、B の保持中には通らない。**実害として大きいのは二重投稿のほうなので、reply に効くことが第2段の主な価値。**
- `--claim` が無い呼び出し（手動・既存テスト）は第1段の扱いのままにして、後方互換を保つ。
- 着手の条件：同じ side_chat で2つのワーカーが並行しうるか（11-22 のような状況で、A のセッションがまだ動いていたか）をプラットフォーム側で確認すること。実例は53時間で1回、最終の書き込みは1回だけだった。今すぐ入れる根拠は弱い。

### 4-4. ついでに見つけた小さな点（今回の範囲外）
- dispatcher.sh の「600s より長い 900s」というコメントの前提（ワーカーが600秒で打ち切られる）は実装と合わない（§2-0）。
- reconcile は DONE の error_message を一律に消すので、「情報のない往復（ループ防止）」のような**判定理由**まで消える（11-23 の訂正2件で、11:44 の commit から約43分後に消えていた）。理由を残したいなら、消すのは reply／ambiguous／unverifiable 系の文言に限る。
- 状態の履歴テーブルが無いので、今回のような「戻った形跡」の調査は間接指標に頼るしかない。第1段で `--force` と拒否を ops アラートに残すようにすれば、少なくともそこは追える。
