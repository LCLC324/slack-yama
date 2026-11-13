# 実装依頼：監査バッチB（DLQ改訂の実装）

作業場所は ~/workspace/slack-yama/ です。まず agy_full_audit_out.md の指摘4と、agy_dlq_design_review_out.md（設計レビューの対案）を読み、対象コード（agent_tools.py、dispatcher.sh、hooks/definitions/slack-yama-dispatch.json）を確認してから実装してください。バッチAは完了済み（テスト25/25通過）で、その変更と競合しないようにしてください。

## 実装する設計（Owner 承認済みの改訂案）
「履歴が読めなくて送信前に止まった失敗（pre_post_check_unverifiable）」は、投稿の失敗ではなく API 待ちとして扱う。

1. pre_post_check_unverifiable で失敗したイベントは、DLQ 行きの retry_count を消費しない。
2. ただし30秒ごとの dispatcher に即座に再取得されないよう、再試行までの待ち時間を持たせる（例：events に next_retry_at を追加し、dispatcher の抽出条件で未来のものを除外する。方式は既存コードと最も素直に合うものを選び、理由をコメントに残すこと）。
3. 待ち時間が過ぎたら通常のディスパッチに戻り、処理役が履歴を確認してから返信する。その際、元の投稿への返信が既に他者（Bot含む）から付いている場合は、再送せずに理由を記録して終える。
4. 最初の失敗から24時間を過ぎても読めない場合は、諦めとして終端させる（状態と記録の残し方は既存の流儀に合わせる）。
5. 監査指摘4の不整合を解消すること:
   - reconcile_replies の候補抽出（error_message の LIKE 条件）が pre_post_check_unverifiable を拾えるようにする（新しい待ち状態の設計に合わせて、必要な範囲で）。
   - 下書き本文は state/reply-<event_id>.txt の決まったパスに残る前提を、フック指示側でも明示する。

## フック指示の追随
- hooks/definitions/slack-yama-dispatch.json の手順文を、新しい commit の振る舞いに合わせて最小限だけ更新する（待ち状態のイベントを再処理するときは、返信前にスレッド履歴で「他者が既に答えていないか」を確認し、答え済みなら投稿せず終える、という趣旨を、既存のリトライ時履歴確認の段落に統合する形で）。JSON として正しいことを確認すること。

## 制約
- 既存テスト（./venv/bin/python tests/run_tests.py、25件）が全て通り続けること。新規テストを追加すること（最低限：unverifiable で retry_count が増えない／待ち時間中は dispatcher に抽出されない／待ち時間経過後に抽出される／24時間経過で終端する／reconcile が unverifiable を拾える）。
- config/.env の中身は読まない・変更しない。
- 変更は DLQ 改訂の範囲に限定する。他の監査指摘（5〜10）には手を付けない。
- 完了後、変更ファイル・設計判断（待ち状態の方式とその理由）・テスト結果を簡潔に報告すること。
