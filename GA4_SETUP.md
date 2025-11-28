# GA4 API 設定手順

## 1. `.env`ファイルの作成

1. プロジェクトルート（`C:\Dev\analyzeap`）に `.env` ファイルを作成します
2. `.env.template` ファイルを開いて内容をコピー
3. `.env` ファイルに貼り付けて、実際の認証情報を入力してください

**重要**: `.env` ファイルは Git にコミットしないでください（既に `.gitignore` に含まれています）

---

## 2. Google Cloud Console での設定

### 2.1 プロジェクトの作成（既にある場合はスキップ）

1. [Google Cloud Console](https://console.cloud.google.com/) にアクセス
2. プロジェクトを作成（または既存のプロジェクトを選択）

### 2.2 Google Analytics Data API の有効化

1. 左メニューから「APIとサービス」>「ライブラリ」を選択
2. 「Google Analytics Data API」を検索
3. 「有効にする」をクリック

### 2.3 サービスアカウントの作成

1. 左メニューから「IAMと管理」>「サービスアカウント」を選択
2. 「サービスアカウントを作成」をクリック
3. サービスアカウント名を入力（例: `ga4-api-access`）
4. 「作成して続行」をクリック
5. ロールは「編集者」または「閲覧者」を選択（必要最小限の権限を推奨）
6. 「完了」をクリック

### 2.4 認証情報JSONのダウンロード

1. 作成したサービスアカウントをクリック
2. 「キー」タブを選択
3. 「キーを追加」>「新しいキーを作成」を選択
4. キーのタイプで「JSON」を選択
5. 「作成」をクリック（JSONファイルがダウンロードされます）

### 2.5 JSONを1行に圧縮

ダウンロードしたJSONファイルを開いて、改行を削除して1行にします。

**方法1: オンラインツールを使用**
- [JSON Minifier](https://jsonformatter.org/json-minify) などのツールを使用

**方法2: PowerShellで圧縮**
```powershell
$json = Get-Content path/to/your-credentials.json -Raw | ConvertFrom-Json | ConvertTo-Json -Compress
$json | Out-File -Encoding utf8 credentials-oneline.json
```

**方法3: 手動で**
- JSONファイルを開いて、すべての改行と不要なスペースを削除

### 2.6 `.env`ファイルに設定

圧縮したJSONを `.env` ファイルの `GA4_CREDENTIALS_JSON=` の後に貼り付けます。

**例:**
```
GA4_CREDENTIALS_JSON={"type":"service_account","project_id":"your-project-123","private_key_id":"abc123...","private_key":"-----BEGIN PRIVATE KEY-----\nMIIEvQ...\n-----END PRIVATE KEY-----\n",...}
```

**重要**: JSON内にダブルクォート（`"`）が含まれているため、`.env`ファイルでは全体をダブルクォートで囲む必要はありません。

---

## 3. GA4プロパティIDの取得

### 3.1 Google Analytics 4 でプロパティIDを確認

1. [Google Analytics](https://analytics.google.com/) にアクセス
2. 対象のプロパティを選択
3. 左下の「管理」（歯車アイコン）をクリック
4. 「プロパティ設定」を選択
5. 「プロパティID」をコピー（数字のみ、例: `123456789`）

### 3.2 サービスアカウントに権限を付与

1. Google Analytics の管理画面で「プロパティへのアクセス管理」を選択
2. 「+」ボタンをクリック
3. サービスアカウントのメールアドレス（JSONファイルの `client_email` に記載）を入力
4. 権限を「閲覧者」に設定
5. 「追加」をクリック

### 3.3 `.env`ファイルにプロパティIDを設定

各ブランドのプロパティIDを `.env` ファイルに設定します。

**例:**
```
GA4_PROPERTY_RADY=123456789
GA4_PROPERTY_CHERIMI=987654321
GA4_PROPERTY_MICHELLMACARON=456789123
GA4_PROPERTY_RADYCHARM=789123456
```

---

## 4. 設定の確認

### 4.1 `.env`ファイルの確認

`.env` ファイルが以下のようになっているか確認してください：

```
SECRET_KEY=dev-secret-key-change-in-production
GA4_CREDENTIALS_JSON={"type":"service_account",...}
GA4_PROPERTY_RADY=123456789
GA4_PROPERTY_CHERIMI=987654321
GA4_PROPERTY_MICHELLMACARON=456789123
GA4_PROPERTY_RADYCHARM=789123456
```

### 4.2 アプリケーションの再起動

`.env` ファイルを保存したら、アプリケーションを再起動してください：

```powershell
# 実行中のアプリを停止（Ctrl+C）
# 再度起動
python app.py
```

### 4.3 動作確認

1. ブラウザで `http://localhost:8080/upload` にアクセス
2. 「🔗 GA4 API 自動取得」セクションが表示されていれば成功です
3. 「📅 前日データを取得」または「📆 週次データを取得」ボタンをクリックしてテスト

---

## トラブルシューティング

### GA4 APIの項目が表示されない

- `.env` ファイルが正しい場所（プロジェクトルート）にあるか確認
- `.env` ファイルの内容が正しいか確認（JSONが1行になっているか）
- アプリケーションを再起動したか確認

### エラー: "GA4_CREDENTIALS_JSON not set"

- `.env` ファイルに `GA4_CREDENTIALS_JSON=` が設定されているか確認
- JSONが正しく1行に圧縮されているか確認

### エラー: "GA4 property ID not set for brand: xxx"

- 該当ブランドのプロパティIDが設定されているか確認
- プロパティIDが数字のみか確認（余分な文字が入っていないか）

### エラー: "Permission denied" または "403 Forbidden"

- サービスアカウントにGA4プロパティへのアクセス権限が付与されているか確認
- Google Analytics Data API が有効になっているか確認

---

## 参考リンク

- [Google Analytics Data API ドキュメント](https://developers.google.com/analytics/devguides/reporting/data/v1)
- [サービスアカウントの作成](https://cloud.google.com/iam/docs/service-accounts)
- [GA4 プロパティIDの確認方法](https://support.google.com/analytics/answer/9304153)

