# GA4 API 設定 - ステップバイステップガイド

## 📋 全体の流れ

1. Google Cloud Consoleでログイン
2. プロジェクトの確認/作成
3. Google Analytics Data APIの有効化
4. サービスアカウントの作成
5. 認証情報JSONのダウンロード
6. JSONを1行に圧縮
7. GA4プロパティIDの取得
8. .envファイルに設定
9. アプリの再起動

---

## ステップ1: Google Cloud Consoleにログイン

1. ブラウザで https://console.cloud.google.com/ を開く
2. Googleアカウントでログイン

---

## ステップ2: プロジェクトの確認/作成

### 既存のプロジェクトがある場合
1. 画面上部のプロジェクト選択ドロップダウンをクリック
2. 使用するプロジェクトを選択

### 新しいプロジェクトを作成する場合
1. 画面上部の「プロジェクトを選択」をクリック
2. 「新しいプロジェクト」をクリック
3. プロジェクト名を入力（例: `analyzeap-ga4`）
4. 「作成」をクリック
5. プロジェクトが作成されるまで待つ（数秒）

**重要**: プロジェクトIDをメモしておいてください

---

## ステップ3: Google Analytics Data APIの有効化

1. 左メニューから「**APIとサービス**」>「**ライブラリ**」をクリック
2. 検索ボックスに「**Google Analytics Data API**」と入力
3. 「**Google Analytics Data API**」をクリック
4. 「**有効にする**」ボタンをクリック
5. 有効化が完了するまで待つ（数秒）

---

## ステップ4: サービスアカウントの作成

1. 左メニューから「**IAMと管理**」>「**サービスアカウント**」をクリック
2. 上部の「**+ サービスアカウントを作成**」をクリック
3. **サービスアカウントの詳細**:
   - **サービスアカウント名**: `ga4-api-access`（任意の名前）
   - **サービスアカウントID**: 自動生成されます（そのままでOK）
   - **説明**: `GA4 API access for analyzeap`（任意）
4. 「**作成して続行**」をクリック
5. **ロールの付与**（オプション）:
   - 「ロールを選択」ドロップダウンから「**閲覧者**」を選択
   - またはスキップしてもOK（後でGA4側で権限を付与します）
6. 「**続行**」をクリック
7. 「**完了**」をクリック

---

## ステップ5: 認証情報JSONのダウンロード

1. 作成したサービスアカウント（`ga4-api-access`など）をクリック
2. 「**キー**」タブをクリック
3. 「**キーを追加**」>「**新しいキーを作成**」をクリック
4. **キーのタイプ**で「**JSON**」を選択
5. 「**作成**」をクリック
   - ⚠️ **重要**: JSONファイルが自動的にダウンロードされます
   - このファイルは**一度しかダウンロードできません**。安全な場所に保存してください

---

## ステップ6: JSONを1行に圧縮

ダウンロードしたJSONファイル（例: `analyzeap-ga4-xxxxx.json`）を1行に圧縮します。

### 方法A: PowerShellで圧縮（推奨）

```powershell
# JSONファイルのパスを指定
$jsonPath = "C:\Users\YourName\Downloads\analyzeap-ga4-xxxxx.json"

# JSONを読み込んで圧縮
$json = Get-Content $jsonPath -Raw | ConvertFrom-Json | ConvertTo-Json -Compress

# 結果を表示（これをコピーして.envファイルに貼り付け）
$json
```

### 方法B: オンラインツールを使用

1. https://jsonformatter.org/json-minify を開く
2. ダウンロードしたJSONファイルの内容をコピー
3. 左側のテキストエリアに貼り付け
4. 「Minify」ボタンをクリック
5. 右側に表示された1行のJSONをコピー

### 方法C: 手動で

1. JSONファイルをメモ帳で開く
2. すべての改行を削除（Ctrl+Hで「改行」を「なし」に置換）
3. 不要なスペースを削除

---

## ステップ7: GA4プロパティIDの取得

1. https://analytics.google.com/ にアクセス
2. 対象のGA4プロパティを選択
3. 左下の「**管理**」（歯車アイコン）をクリック
4. 「**プロパティ設定**」をクリック
5. 「**プロパティID**」をコピー（数字のみ、例: `123456789`）

**各ブランドのプロパティIDを取得**:
- RADY: `GA4_PROPERTY_RADY=`
- CHERIMI: `GA4_PROPERTY_CHERIMI=`
- MICHELLMACARON: `GA4_PROPERTY_MICHELLMACARON=`
- RADYCHARM: `GA4_PROPERTY_RADYCHARM=`

---

## ステップ8: サービスアカウントにGA4アクセス権限を付与

1. Google Analyticsの管理画面で「**プロパティへのアクセス管理**」をクリック
2. 「**+**」ボタンをクリック
3. サービスアカウントのメールアドレスを入力
   - メールアドレスは、ダウンロードしたJSONファイルの `client_email` に記載されています
   - 例: `ga4-api-access@analyzeap-ga4.iam.gserviceaccount.com`
4. 権限を「**閲覧者**」に設定
5. 「**追加**」をクリック

**各ブランドのプロパティに対して、この手順を繰り返してください**

---

## ステップ9: .envファイルに設定

1. `C:\Dev\analyzeap\.env` ファイルを開く
2. 以下のように設定:

```
SECRET_KEY=dev-secret-key-change-in-production

GA4_CREDENTIALS_JSON={"type":"service_account","project_id":"...","private_key":"..."}

GA4_PROPERTY_RADY=123456789
GA4_PROPERTY_CHERIMI=987654321
GA4_PROPERTY_MICHELLMACARON=456789123
GA4_PROPERTY_RADYCHARM=789123456
```

**重要**:
- `GA4_CREDENTIALS_JSON=` の後に、圧縮したJSONを**そのまま**貼り付け（ダブルクォートで囲まない）
- プロパティIDは数字のみ（余分な文字は入れない）

3. ファイルを保存

---

## ステップ10: アプリの再起動と確認

1. 実行中のアプリを停止（Ctrl+C）
2. 再度起動:
   ```powershell
   python app.py
   ```
3. ブラウザで `http://localhost:8080/upload` にアクセス
4. 「**🔗 GA4 API 自動取得**」セクションが表示されていれば成功！

---

## トラブルシューティング

### GA4 APIの項目が表示されない
- `.env`ファイルが正しい場所にあるか確認
- JSONが正しく1行に圧縮されているか確認
- プロパティIDが正しく設定されているか確認
- アプリを再起動したか確認

### エラー: "Permission denied"
- サービスアカウントにGA4プロパティへのアクセス権限が付与されているか確認
- 各ブランドのプロパティに対して権限を付与したか確認

### エラー: "API not enabled"
- Google Analytics Data APIが有効になっているか確認
- 正しいプロジェクトを選択しているか確認

---

## よくある質問

**Q: サービスアカウントのJSONファイルを紛失した**
A: 新しいキーを作成する必要があります。サービスアカウントの「キー」タブから「キーを追加」>「新しいキーを作成」で再作成できます。

**Q: プロパティIDがわからない**
A: Google Analyticsの管理画面 > プロパティ設定で確認できます。数字のみです（例: `123456789`）。

**Q: JSONを1行に圧縮するのが面倒**
A: PowerShellのコマンドを使うと簡単です（ステップ6の方法Aを参照）。

