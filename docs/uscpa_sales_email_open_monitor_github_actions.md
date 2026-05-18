# USCPA過去リード再行動通知 GitHub Actions移行

## 目的

Windowsスケジューラー依存を外し、GitHub Actionsで `uscpa_sales_email_open_monitor.py` を15分ごとに実行する。

## 実行内容

- HubSpot CRMメール活動から開封済みメールを検索
- `noreply` 送信元を除外
- `info@abitus.co.jp` またはCPA営業担当メールから送信されたメールのみ対象
- HubSpotコンタクトの直近Web閲覧から `uscpa` を含むページ閲覧を検索
- 過去リードリスト `6567` のコンタクトのみ対象
- `sales_staff_cpa` が対象CPA営業担当の場合のみ通知
- Slack Botで担当者をメンション
- Slack通知の開封メール件名にはHubSpotのCRMメール活動レコードURLを付ける
- Slack通知のWeb閲覧には直近閲覧ページURLを付ける
- Google Sheetsへ担当者別タブで追記
- 同一通知キーの二重通知と、同一コンタクト10日以内の再通知を抑止

## GitHub Secrets

以下をリポジトリの Actions secrets に設定する。

- `HUBSPOT_PAT`
- `SLACK_BOT_TOKEN`
- `USCPA_SLACK_CHANNEL_ID`
- `USCPA_SHEET_SPREADSHEET_ID`
- `GOOGLE_SERVICE_ACCOUNT_JSON_CONTENT`

一時的にGitHub Actionsの成功/失敗メールを送る場合は、以下も設定する。

- `SMTP_HOST`
- `SMTP_PORT`
- `SMTP_USERNAME`
- `SMTP_PASSWORD`
- `SMTP_FROM`

`GOOGLE_SERVICE_ACCOUNT_JSON_CONTENT` はサービスアカウントJSONファイルの中身全体を入れる。

メール通知を有効化する場合は、リポジトリ変数 `USCPA_RUN_EMAIL_NOTIFY` を `true` にする。
現在のワークフローでは検証用に `2026-05-19T15:00:00Z` 以降は送らない。

## ワークフロー

- ファイル: `.github/workflows/uscpa-sales-email-open-monitor.yml`
- 定期実行: 15分ごと
- cron: `*/15 * * * *`
- 手動実行: `workflow_dispatch`

## テスト手順

1. GitHub Secretsを設定する。
2. Actionsから `USCPA sales email open monitor` を手動実行する。
3. まず `apply=false` でドライランする。
4. 成功後、`apply=true` かつ `max_updates=1` で本番相当テストをする。
5. Slack通知、HubSpotプロパティ更新、Sheets追記を確認する。
6. 問題なければWindowsタスク `USCPA Sales Email Open Slack Monitor` を停止する。

## 注意

GitHub Secretsには実トークンを保存するため、リポジトリの管理者・権限設計を確認してから投入する。
