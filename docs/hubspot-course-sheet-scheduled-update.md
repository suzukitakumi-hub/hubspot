# HubSpot Course Sheet Scheduled Update

## 推奨構成

サーバーを持たない前提では、GitHub Actions の `schedule` を使って更新する。

- 実行時刻: 毎週火曜・金曜 08:00 JST
- GitHub cron: `0 23 * * 1,4`
- 更新内容: 当月分を再取得し、同一メールIDは上書き、新規メールは追加。その後、全掲載メールのHubSpot標準フォーム送信数を再集計
- 対象講座: CIA / CISA / CFE / IFRS / USCPA / MBA / AAIA
- 実行後: ライブシート監査とフォーム照合監査を実行し、issue が残ればジョブを失敗させる

## フォーム送信数の定義

- 対象: HubSpotマーケティングメール経由のHubSpot標準フォーム送信
- 照合キー: フォーム送信時のpage URLにある `_hsmi`。これがない場合は、`utm_source=hs_email` または `utm_medium=email` を伴う `utm_content`
- 集計期間: メール送信日時の5分前から30日後まで。HubSpot側の時刻差を吸収するため、送信直前5分を許容する
- 重複排除: フォームGUIDと`conversionId`の組み合わせ
- 表示: 送信後30日未満は「集計中（送信後30日未満）」、30日経過後は確定
- 非対象: USCPA/MBAのHubSpot Meetings予約。標準フォーム送信ではなく、メール別に確定できないため加算しない
- イベント系: フォームの予約送信数であり、実際の出席者数ではない

フォーム名だけでは新しいフォームを自動加算しない。レビュー済みGUIDは`config/hubspot_form_cv_registry.json`で管理し、掲載メールIDをpage URLに持つ未登録フォームが見つかった場合は本番更新を止める。

Meetingsはメール名や講座名では判定しない。メール詳細の`content.flexAreas`から参照される有効なウィジェットだけを調べ、ボタンURLが登録済みのUSCPA/MBA予約LPと完全一致した場合に注記する。さらに予約LPが所定のHubSpot Meetingsを埋め込んでいることを毎回確認する。未登録のカウンセリングURLやLP内容の変更を検出した場合は本番更新を止める。

APIが成功応答のまま空データを返すケースも考慮し、定期処理では最小走査件数・最小集計件数と、送信後30日を経過した代表メール3件の確定値を検算する。いずれかを下回る、または確定値が変わった場合はN/O列を更新しない。

旧メール実績処理はN/O列を所有しない。既存メールは直前のフォーム値をメールID単位で保持し、新規メールのN/Oはフォーム照合が成功するまで空欄にする。そのため、フォームAPIや監査が失敗してもGA4値へ戻らない。

## 必要な GitHub Secrets

Repository settings の `Secrets and variables` から以下を登録する。

| Secret | 内容 |
| --- | --- |
| `HUBSPOT_PAT` | HubSpot Private App Token |
| `GOOGLE_SERVICE_ACCOUNT_JSON_CONTENT` | GoogleサービスアカウントJSONの中身全体 |
| `GA4_PROPERTY_ID` | GA4 property ID。現状は `249786227` |
| `HUBSPOT_COURSE_SPREADSHEET_ID` | 転記先スプレッドシートID |

## 手動実行

GitHub Actions の `HubSpot course sheet update` から `Run workflow` を押す。

- `months` 未指定: 現在月を更新
- `months` 指定例: `2026-04 2026-05`
- `skip_promote=true`: live反映せず、staging/validation/auditと非表示のフォーム監査タブ更新だけ実行

## ログ確認

Actions の実行結果に `hubspot-course-sheet-update-logs` が artifact として残る。

- `logs/course_sheet_updates/*.log`
- `logs/form_cv_updates/*.json`
- `hubspot_course_sheet_validation_*.json`
- `hubspot_course_sheet_live_audit_*.json`
- `ga4_hubspot_cv_map_*_manifest.json`
- `ga4_hubspot_cv_map_*_unmapped_keys.csv`

## ChatGPT / Claude Code との使い分け

ChatGPT Tasks や Codex Automations、Claude Code Routines は「定期的にAIへ作業を依頼する」用途には使えるが、この更新は認証情報を使って本番シートを書き換える定型ジョブなので、GitHub Actions の方が適している。

理由:

- Secrets 管理が標準である
- 実行ログが残る
- cron が明確
- 失敗時にジョブとして検知できる
- Pythonスクリプトをそのまま再現実行できる
