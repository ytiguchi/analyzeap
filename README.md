# 商品分析ダッシュボード

GA4売上データと商品マスタ（在庫・画像）を突き合わせて、ブランド別に「在庫過多×低売上」などを分析するWebアプリです。

## 機能

- 📦 **商品マスタCSV読み込み**: 在庫数、カラー、サイズ、商品画像URLなど
- 📊 **GA4売上CSV読み込み**: 閲覧数、カート追加、購入数、売上など（ブランド別対応）
- 🔗 **SKU IDで自動突合**: 両データをマージして分析
- 🏷️ **ブランド別サマリー**: 各ブランドの売上・在庫状況を一覧表示
- 🔥 **PVランキング**: 閲覧数TOP商品を画像付きで表示
- 🚨 **問題商品の検出**: 在庫過多×低売上の商品を自動抽出
- 💡 **機会損失の検出**: 閲覧多×在庫切れの商品を自動抽出
- 🖼️ **商品画像付きレポート**: 視覚的にわかりやすい分析結果
- ☁️ **Cloudflare R2連携**: 商品マスタをクラウドストレージで共有
- 📅 **分析期間の自動検出**: GA4 CSVから日次/週次を自動判定

## デプロイ（Railway）

### 1. Railwayプロジェクト作成
1. [Railway](https://railway.app/) にログイン
2. 「New Project」→「Deploy from GitHub repo」
3. このリポジトリを選択

### 2. 環境変数設定
Railway の Variables タブで以下を設定：

```
SECRET_KEY=your-random-secret-key

# Cloudflare R2（オプション）
R2_ACCOUNT_ID=your_account_id
R2_ACCESS_KEY_ID=your_access_key_id  
R2_SECRET_ACCESS_KEY=your_secret_access_key
R2_BUCKET_NAME=analyzeap-data
```

### 3. デプロイ
自動でデプロイされます。Settings → Domains でURLを確認。

---

## ローカル開発

## セットアップ

```bash
cd /Users/iguchiyuuta/Dev/analyzeap

# 仮想環境作成（推奨）
python3 -m venv venv
source venv/bin/activate

# 依存パッケージインストール
pip install -r requirements.txt

# アプリ起動
python app.py
```

ブラウザで http://localhost:5000 を開く

## 使い方

### 1. CSVファイルの準備

#### 商品マスタCSV
以下のカラムを含むCSV（Shift-JIS/UTF-8対応）:
- `SKU商品ID` - GA4のItem IDと突合するキー
- `ブランド名`
- `商品名`
- `カラー名`, `カラータグ`
- `サイズ名`
- `販売価格`
- `WEB在庫`, `調整在庫`, `見込み在庫`
- `商品画像URL`
- `公開ステータス`, `販売ステータス`

#### GA4売上CSV
GA4から「Eコマース購入」レポートをCSVエクスポート:
- `Item name`
- `Item ID` - 商品マスタのSKU商品IDと一致
- `Items viewed`
- `Items added to cart`
- `Items purchased`
- `Item revenue`

### 2. データアップロード

1. 「データ更新」ページを開く
2. 商品マスタCSVをアップロード
3. GA4売上CSVをアップロード
4. 自動で突合・分析が実行される

### 3. 分析結果の確認

- **ダッシュボード**: ブランド別サマリー
- **ブランド詳細**: 問題商品・機会損失・売上TOPを画像付きで表示

## 分析ロジック

### 🚨 在庫過多×低売上（問題商品）
- 在庫が上位30%（多い）
- かつ売上が下位30%（少ない）

→ 値下げ・販促・在庫処分の検討対象

### 💡 機会損失
- 閲覧数が上位30%（人気）
- かつ在庫が5個以下
- かつ購入率が5%未満

→ 在庫補充・再入荷の検討対象

## ディレクトリ構成

```
analyzeap/
├── app.py              # Flaskアプリ本体
├── requirements.txt    # 依存パッケージ
├── README.md
├── uploads/            # アップロードCSV保存先
└── templates/
    ├── base.html       # 共通レイアウト
    ├── index.html      # ダッシュボード
    ├── upload.html     # アップロード画面
    └── brand_detail.html # ブランド詳細
```

## 毎日の運用フロー

1. GA4から当日の売上CSVをエクスポート
2. 商品マスタCSV（在庫更新済み）を用意
3. アプリにアップロード
4. 分析結果を確認・レポート作成

## 今後の拡張案

- [ ] 日次データの蓄積・トレンド分析
- [ ] レポートのPDF/Excel出力
- [ ] Slack/メール通知（問題商品発生時）
- [ ] GA4 API連携（自動データ取得）

