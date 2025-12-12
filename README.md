# Monthly Account Summary

## PyInstaller での EXE 化
PyInstaller 実行時は、設定ファイルとサービスアカウント JSON をバンドルする必要があります。リポジトリ直下で以下のように実行してください（Windows の `--add-data` はセミコロン区切り、macOS/Linux はコロン区切り）。

### Windows (PowerShell)
PowerShell ではバックスラッシュによる行継続が効かないため、バッククォート（`）で行継続するか、1 行で実行してください。

```powershell
pyinstaller --noconfirm --onefile --name monthly_account_summary `
  --add-data "config.yaml;." `
  --add-data "app/credentials.json;." `
  app/main.py
```

### macOS / Linux（bash/zsh）

```bash
pyinstaller --noconfirm --onefile --name monthly_account_summary \
  --add-data "config.yaml:." \
  --add-data "app/credentials.json:." \
  app/main.py
```

- `config.yaml` と `credentials.json` を exe と同じディレクトリに配置する形でバンドルします。
- サービスアカウントは `GOOGLE_APPLICATION_CREDENTIALS` / `GOOGLE_APPLICATION_CREDENTIALS_JSON` 環境変数でも解決できますが、exe 同梱が最も簡単です。
- ビルド後は `dist/monthly_account_summary.exe` を実行してください。

## Slack 通知
- 実行開始と処理完了時に Slack Webhook へ通知を送る場合は、以下のいずれかで URL を指定してください。
  - 環境変数 `SLACK_WEBHOOK_URL`
  - `config.yaml` の `runtime.slack_webhook_url`
- 通知メッセージには対象ファイル数や挿入件数、各ファイルのステータスが含まれます。
