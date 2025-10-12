# Yumenoi Accounting CSV → PostgreSQL (Shared Drive)

## セットアップ
1. 依存ライブラリをインストール
   ```bash
   pip install -r requirements.txt
   ```

2. サービスアカウント鍵を安全な場所に置き、プロジェクト直下の `.env` にフルパスを書く
   ```env
   GOOGLE_APPLICATION_CREDENTIALS=C:\path\to\service_account.json
   ```

3. `config.yaml` は本番値を埋め済みです（フォルダID、DB接続、列名マッピング）。
   実行：
   ```bash
   python -m app.main
   ```

## 仕様
- 共有ドライブ配下の「会計システムデータ/データ格納」(folder_id) からCSVを取得
- 3行2列目を facility_name、4行4列目の和暦を year_month(YYYY-MM-01) へ
- 先頭4行を削除し、直後1行をヘッダーとして採用
- 日本語列名 → DB列名にマッピング
- DBは既存テーブルに INSERT（UPSERTオフ）。投入後は「過去データ保存」へ移動
