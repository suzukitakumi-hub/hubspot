# HubSpot Course Sheet Scheduled Update

## 推奨構成

サーバーを持たない前提では、GitHub Actions の `schedule` を使って更新する。

- 実行時刻: 毎週火曜・金曜 08:00 JST
- GitHub cron: `0 23 * * 1,4`
- 更新内容: 当月分を再取得し、同一メールIDは上書き、新規メールは追加
- 実行後: ライブシート監査を実行し、issue が残ればジョブを失敗させる

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
- `skip_promote=true`: live反映せず、staging/validation/auditだけ実行

## ログ確認

Actions の実行結果に `hubspot-course-sheet-update-logs` が artifact として残る。

- `logs/course_sheet_updates/*.log`
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
