"""
商品在庫 × GA4売上 分析アプリ
- 商品マスタCSV（在庫・色・画像など）とGA4売上CSVを突き合わせ
- ブランド別に「在庫過多×低売上」などの商品を分析
- 商品画像付きでレポート表示
"""

import os
import re
from datetime import datetime

# .envファイルから環境変数を読み込み（ローカル開発用）
from dotenv import load_dotenv
load_dotenv()

from flask import Flask, render_template, request, redirect, url_for, flash, jsonify, session
from functools import wraps
import pandas as pd
from werkzeug.utils import secure_filename

# シンプル認証
SITE_PASSWORD = os.environ.get('SITE_PASSWORD', '898989')

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get('logged_in'):
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

# R2ストレージ連携
try:
    from storage import download_product_master, upload_product_master, get_product_master_info, is_r2_enabled, save_ga4_data, get_latest_ga4_data
except ImportError:
    is_r2_enabled = lambda: False
    download_product_master = None
    save_ga4_data = None
    get_latest_ga4_data = None
    upload_product_master = None
    get_product_master_info = None

# GA4 API連携
try:
    from ga4_api import (
        is_ga4_configured, get_configured_brands, 
        fetch_yesterday_data, fetch_weekly_data, fetch_all_brands_data
    )
except ImportError:
    is_ga4_configured = lambda: False
    get_configured_brands = lambda: []
    fetch_yesterday_data = None
    fetch_weekly_data = None
    fetch_all_brands_data = None

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'dev-secret-key-change-in-production')
app.config['UPLOAD_FOLDER'] = os.path.join(os.path.dirname(__file__), 'uploads')
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB

# アップロードフォルダ作成
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# グローバルデータストア（本番ではDBを使用）
data_store = {
    'product_master': None,
    'product_master_info': None,  # R2からの情報
    'ga_sales': {},  # ブランド別に保存 {'rady': {'data': df, 'period': {...}}, ...}
    'ga_sales_previous': {},  # 前期間データ（比較用）
    'merged_data': None,
    'merged_data_previous': None  # 前期間のマージデータ
}

# 登録済みブランド一覧
BRANDS = ['rady', 'cherimi', 'michellmacaron', 'solni']


def process_product_master_df(df):
    """商品マスタDataFrameを処理"""
    # 必要なカラムを抽出・リネーム
    col_map = {
        'SKU商品ID': 'sku_id',
        '商品ID（型単位）': 'product_class_id',
        'ブランド名': 'brand',
        '商品名': 'product_name',
        'カラー名': 'color_name',
        'カラータグ': 'color_tag',
        'サイズ名': 'size',
        '販売価格': 'price',
        'WEB在庫': 'web_stock',
        '調整在庫': 'adjust_stock',
        '見込み在庫': 'expected_stock',
        '商品ページURL': 'product_url',
        '商品画像URL': 'image_url',
        '公開ステータス': 'publish_status',
        '販売ステータス': 'sales_status',
    }
    # 存在するカラムのみリネーム
    existing_cols = {k: v for k, v in col_map.items() if k in df.columns}
    df = df.rename(columns=existing_cols)
    
    # 在庫合計を計算
    stock_cols = ['web_stock', 'adjust_stock', 'expected_stock']
    for col in stock_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
    
    df['total_stock'] = df[['web_stock', 'adjust_stock', 'expected_stock']].sum(axis=1) if all(c in df.columns for c in stock_cols) else 0
    
    return df


def load_product_master(filepath):
    """商品マスタCSVを読み込み（cp932/utf-8対応）"""
    for enc in ['cp932', 'utf-8', 'utf-8-sig']:
        try:
            df = pd.read_csv(filepath, encoding=enc)
            return process_product_master_df(df)
        except Exception:
            continue
    raise ValueError("CSVの読み込みに失敗しました")


def parse_ga_period(lines):
    """GA4 CSVのヘッダーから期間情報を抽出"""
    period = {
        'start_date': None,
        'end_date': None,
        'property': None,
        'days': 0,
        'period_type': 'unknown'  # daily, weekly, monthly, custom
    }
    
    for line in lines[:15]:  # 最初の15行だけチェック
        line = line.strip()
        
        # Start date: 20251127 形式
        if 'Start date:' in line:
            match = re.search(r'Start date:\s*(\d{8})', line)
            if match:
                date_str = match.group(1)
                try:
                    period['start_date'] = datetime.strptime(date_str, '%Y%m%d')
                except:
                    pass
        
        # End date: 20251128 形式
        if 'End date:' in line:
            match = re.search(r'End date:\s*(\d{8})', line)
            if match:
                date_str = match.group(1)
                try:
                    period['end_date'] = datetime.strptime(date_str, '%Y%m%d')
                except:
                    pass
        
        # Property名
        if 'Property:' in line:
            match = re.search(r'Property:\s*(.+)', line)
            if match:
                period['property'] = match.group(1).strip()
    
    # 日数計算と期間タイプ判定
    if period['start_date'] and period['end_date']:
        delta = period['end_date'] - period['start_date']
        period['days'] = delta.days + 1  # 両端含む
        
        if period['days'] == 1:
            period['period_type'] = 'daily'
        elif period['days'] == 7:
            period['period_type'] = 'weekly'
        elif 28 <= period['days'] <= 31:
            period['period_type'] = 'monthly'
        else:
            period['period_type'] = 'custom'
    
    return period


def load_ga_sales(filepath):
    """GA4売上CSVを読み込み、期間情報も返す"""
    for enc in ['utf-8', 'utf-8-sig', 'cp932']:
        try:
            # ヘッダー行をスキップ（GA4エクスポート形式対応）
            with open(filepath, 'r', encoding=enc) as f:
                lines = f.readlines()
            
            # 期間情報を抽出
            period = parse_ga_period(lines)
            
            # データ開始行を探す（#コメント行をスキップ）
            header_idx = 0
            for i, line in enumerate(lines):
                # #で始まるコメント行をスキップ
                if line.strip().startswith('#'):
                    continue
                # Item nameまたはItem IDを含むヘッダー行を探す
                if 'Item name' in line or 'Item ID' in line:
                    header_idx = i
                    break
            
            df = pd.read_csv(filepath, encoding=enc, skiprows=header_idx)
            
            # カラム名を正規化
            col_map = {
                'Item name': 'item_name',
                'Item ID': 'sku_id',
                'Items viewed': 'views',
                'Items added to cart': 'add_to_cart',
                'Items purchased': 'purchases',
                'Item revenue': 'revenue',
            }
            existing_cols = {k: v for k, v in col_map.items() if k in df.columns}
            df = df.rename(columns=existing_cols)
            
            # sku_idが存在するか確認
            if 'sku_id' not in df.columns:
                raise ValueError("Item ID column not found")
            
            # 数値変換
            for col in ['views', 'add_to_cart', 'purchases', 'revenue']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
            return {'data': df, 'period': period}
        except Exception as e:
            continue
    raise ValueError("GA4 CSVの読み込みに失敗しました")


def get_analysis_period():
    """現在の分析期間情報を取得"""
    ga_dict = data_store['ga_sales']
    if not ga_dict:
        return None
    
    # 全ブランドの期間を集約
    all_periods = []
    for brand, ga_info in ga_dict.items():
        if ga_info and 'period' in ga_info:
            period = ga_info['period']
            period['brand'] = brand
            all_periods.append(period)
    
    if not all_periods:
        return None
    
    # 全体の期間を計算
    start_dates = [p['start_date'] for p in all_periods if p['start_date']]
    end_dates = [p['end_date'] for p in all_periods if p['end_date']]
    
    overall = {
        'brands': all_periods,
        'min_start': min(start_dates) if start_dates else None,
        'max_end': max(end_dates) if end_dates else None,
    }
    
    if overall['min_start'] and overall['max_end']:
        delta = overall['max_end'] - overall['min_start']
        overall['total_days'] = delta.days + 1
    else:
        overall['total_days'] = 0
    
    return overall


def merge_and_analyze():
    """商品マスタとGA売上を突き合わせて分析"""
    pm = data_store['product_master']
    ga_dict = data_store['ga_sales']
    
    if pm is None or not ga_dict:
        return None
    
    # 全ブランドのGAデータを結合
    ga_list = [info['data'] for info in ga_dict.values() if info and 'data' in info and len(info['data']) > 0]
    if not ga_list:
        return None
    
    ga = pd.concat(ga_list, ignore_index=True)
    
    # 同じSKUが複数ブランドにある場合は合算
    ga = ga.groupby('sku_id').agg({
        'item_name': 'first',
        'views': 'sum',
        'add_to_cart': 'sum',
        'purchases': 'sum',
        'revenue': 'sum',
    }).reset_index()
    
    # SKU IDで結合
    merged = pm.merge(ga, on='sku_id', how='left')
    
    # 商品ページURLを自動生成（空の場合）
    def generate_product_url(row):
        if pd.notna(row.get('product_url')) and str(row.get('product_url')).strip():
            return row['product_url']
        
        brand_raw = str(row.get('brand', '')).lower().replace(' ', '').replace('_', '')
        brand_slug = ''
        if 'rady' in brand_raw:
            brand_slug = 'rady'
        elif 'cherimi' in brand_raw:
            brand_slug = 'cherimi'
        elif 'michell' in brand_raw or 'macaron' in brand_raw:
            brand_slug = 'michellmacaron'
        elif 'solni' in brand_raw:
            brand_slug = 'solni'
        
        if brand_slug and row.get('sku_id'):
            return f"https://mycolor.jp/{brand_slug}/item/{row['sku_id']}"
        return ''
    
    merged['product_url'] = merged.apply(generate_product_url, axis=1)
    
    # 欠損値を0埋め
    for col in ['views', 'add_to_cart', 'purchases', 'revenue']:
        if col in merged.columns:
            merged[col] = merged[col].fillna(0)
    
    # 分析指標を追加
    # CVR（閲覧→購入率）
    merged['cvr'] = merged.apply(
        lambda x: (x['purchases'] / x['views'] * 100) if x['views'] > 0 else 0, axis=1
    )
    
    # カート追加率
    merged['cart_rate'] = merged.apply(
        lambda x: (x['add_to_cart'] / x['views'] * 100) if x['views'] > 0 else 0, axis=1
    )
    
    # 在庫効率スコア（売上÷在庫、高いほど効率的）
    merged['stock_efficiency'] = merged.apply(
        lambda x: x['revenue'] / x['total_stock'] if x['total_stock'] > 0 else 0, axis=1
    )
    
    # 問題フラグ: 在庫多い × 売上少ない
    stock_threshold = merged['total_stock'].quantile(0.7)  # 上位30%の在庫
    revenue_threshold = merged['revenue'].quantile(0.3)    # 下位30%の売上
    
    merged['is_problem'] = (merged['total_stock'] >= stock_threshold) & (merged['revenue'] <= revenue_threshold)
    
    # 機会損失フラグ: 閲覧多い × 在庫少ない × 購入少ない
    views_threshold = merged['views'].quantile(0.7)
    merged['is_opportunity'] = (merged['views'] >= views_threshold) & (merged['total_stock'] <= 5) & (merged['purchases'] < merged['views'] * 0.05)
    
    # Regalectを除外
    merged = merged[merged['brand'] != 'Regalect']
    
    # 前期間データとの比較（デルタ計算）
    ga_prev_dict = data_store.get('ga_sales_previous', {})
    if ga_prev_dict:
        ga_prev_list = [info['data'] for info in ga_prev_dict.values() if info and 'data' in info and len(info['data']) > 0]
        if ga_prev_list:
            ga_prev = pd.concat(ga_prev_list, ignore_index=True)
            ga_prev = ga_prev.groupby('sku_id').agg({
                'views': 'sum',
                'add_to_cart': 'sum',
                'purchases': 'sum',
                'revenue': 'sum',
            }).reset_index()
            ga_prev.columns = ['sku_id', 'prev_views', 'prev_add_to_cart', 'prev_purchases', 'prev_revenue']
            
            # 前期間データをマージ
            merged = merged.merge(ga_prev, on='sku_id', how='left')
            
            # デルタ計算
            for col in ['views', 'add_to_cart', 'purchases', 'revenue']:
                prev_col = f'prev_{col}'
                delta_col = f'delta_{col}'
                pct_col = f'delta_{col}_pct'
                
                merged[prev_col] = merged[prev_col].fillna(0)
                merged[delta_col] = merged[col] - merged[prev_col]
                merged[pct_col] = merged.apply(
                    lambda x: ((x[col] - x[prev_col]) / x[prev_col] * 100) if x[prev_col] > 0 else (100 if x[col] > 0 else 0),
                    axis=1
                )
            
            # CVRのデルタ
            merged['prev_cvr'] = merged.apply(
                lambda x: (x['prev_purchases'] / x['prev_views'] * 100) if x['prev_views'] > 0 else 0, axis=1
            )
            merged['delta_cvr'] = merged['cvr'] - merged['prev_cvr']
            
            print(f"✅ Calculated deltas for {len(merged)} items")
    
    data_store['merged_data'] = merged
    return merged


def get_brand_summary():
    """ブランド別サマリーを取得"""
    df = data_store['merged_data']
    if df is None:
        return None
    
    # Regalectを除外
    df = df[df['brand'] != 'Regalect']
    
    summary = df.groupby('brand').agg({
        'sku_id': 'count',
        'total_stock': 'sum',
        'views': 'sum',
        'add_to_cart': 'sum',
        'purchases': 'sum',
        'revenue': 'sum',
        'is_problem': 'sum',
        'is_opportunity': 'sum',
    }).reset_index()
    
    summary.columns = ['brand', 'sku_count', 'total_stock', 'total_views', 
                       'total_cart', 'total_purchases', 'total_revenue',
                       'problem_count', 'opportunity_count']
    
    # CVR計算
    summary['overall_cvr'] = summary.apply(
        lambda x: (x['total_purchases'] / x['total_views'] * 100) if x['total_views'] > 0 else 0, axis=1
    )
    
    return summary.to_dict('records')


def get_problem_products(brand=None, limit=50):
    """問題商品（在庫過多×低売上）を取得"""
    df = data_store['merged_data']
    if df is None:
        return []
    
    filtered = df[df['is_problem'] == True].copy()
    if brand and brand != 'all':
        filtered = filtered[filtered['brand'] == brand]
    
    filtered = filtered.sort_values('total_stock', ascending=False).head(limit)
    return filtered.to_dict('records')


def get_opportunity_products(brand=None, limit=50):
    """機会損失商品（閲覧多×在庫切れ）を取得"""
    df = data_store['merged_data']
    if df is None:
        return []
    
    filtered = df[df['is_opportunity'] == True].copy()
    if brand and brand != 'all':
        filtered = filtered[filtered['brand'] == brand]
    
    filtered = filtered.sort_values('views', ascending=False).head(limit)
    return filtered.to_dict('records')


def get_top_performers(brand=None, limit=30):
    """売上上位商品を取得（カラー/サイズ別）"""
    df = data_store['merged_data']
    if df is None:
        return []
    
    filtered = df.copy()
    if brand and brand != 'all':
        filtered = filtered[filtered['brand'] == brand]
    
    filtered = filtered.sort_values('revenue', ascending=False).head(limit)
    return filtered.to_dict('records')


def get_pv_ranking(brand=None, limit=50):
    """PV（閲覧数）ランキングを取得（商品名でグループ化、SKU詳細付き）"""
    df = data_store['merged_data']
    if df is None:
        return []
    
    # ブランドフィルタ
    if brand and brand != 'all':
        df = df[df['brand'] == brand].copy()
    else:
        df = df.copy()
    
    # 商品名（product_class_id）でグループ化して集計
    if 'product_class_id' not in df.columns:
        return []
    
    # 閲覧数が0より大きい商品のみ（グループ集計前）
    products_with_views = df[df['views'] > 0]['product_class_id'].unique()
    
    grouped = df.groupby('product_class_id').agg({
        'brand': 'first',
        'product_name': 'first',
        'image_url': 'first',
        'product_url': 'first',
        'views': 'first',  # GA4のPVは商品名レベルで同じ値
        'add_to_cart': 'sum',
        'purchases': 'sum',
        'revenue': 'sum',
        'total_stock': 'sum',
    }).reset_index()
    
    # PVがある商品のみ
    grouped = grouped[grouped['product_class_id'].isin(products_with_views)]
    
    # CVR（PVに対する購入率）= 購入数 / PV * 100
    grouped['cvr'] = (grouped['purchases'].astype(float) / grouped['views'].astype(float) * 100).fillna(0)
    grouped['purchase_rate'] = grouped['cvr']
    
    grouped = grouped.sort_values('views', ascending=False).head(limit)
    
    # SKU詳細を追加（元のdfから取得）
    result = []
    for _, row in grouped.iterrows():
        product = row.to_dict()
        # CVRを確実にfloatで保持
        product['cvr'] = float(product.get('cvr', 0) or 0)
        
        # このproduct_class_idに属する全SKUを取得（元データから）
        skus = df[df['product_class_id'] == row['product_class_id']].copy()
        
        if len(skus) > 0:
            # 各SKUのCVRを計算
            skus['cvr'] = skus.apply(
                lambda x: (float(x['purchases']) / float(x['views']) * 100) if float(x['views']) > 0 else 0.0, axis=1
            )
            
            # 購入数の多い順にソート（優れている順）
            skus = skus.sort_values('purchases', ascending=False)
            
            # SKUデータを辞書リストに変換（デルタ情報も含む）
            sku_list = []
            for _, s in skus.iterrows():
                sku_data = {
                    'color_name': str(s.get('color_name', '') or ''),
                    'color_tag': str(s.get('color_tag', '#888') or '#888'),
                    'size': str(s.get('size', '') or ''),
                    'views': int(s.get('views', 0) or 0),
                    'add_to_cart': int(s.get('add_to_cart', 0) or 0),
                    'purchases': int(s.get('purchases', 0) or 0),
                    'cvr': float(s.get('cvr', 0) or 0),
                    'total_stock': int(s.get('total_stock', 0) or 0),
                    # デルタ情報
                    'delta_purchases': int(s.get('delta_purchases', 0) or 0),
                    'delta_purchases_pct': float(s.get('delta_purchases_pct', 0) or 0),
                    'delta_add_to_cart': int(s.get('delta_add_to_cart', 0) or 0),
                    'delta_cvr': float(s.get('delta_cvr', 0) or 0),
                    'prev_purchases': int(s.get('prev_purchases', 0) or 0),
                }
                sku_list.append(sku_data)
            product['skus'] = sku_list
        else:
            product['skus'] = []
        
        result.append(product)
    
    return result


def get_pv_ranking_by_brand(limit_per_brand=30):
    """ブランド別PVランキングを取得"""
    df = data_store['merged_data']
    if df is None:
        return {}
    
    brands = df['brand'].dropna().unique().tolist()
    result = {}
    
    for brand in brands:
        result[brand] = get_pv_ranking(brand=brand, limit=limit_per_brand)
    
    return result


def get_grouped_products(df, sort_by='views', limit=50):
    """商品を型番（product_class_id）でグルーピング"""
    if df is None or len(df) == 0:
        return []
    
    # product_class_idでグループ化して集計
    grouped = df.groupby('product_class_id').agg({
        'sku_id': 'count',  # SKU数
        'brand': 'first',
        'product_name': 'first',
        'image_url': 'first',
        'product_url': 'first',
        'total_stock': 'sum',
        'views': 'sum',
        'add_to_cart': 'sum',
        'purchases': 'sum',
        'revenue': 'sum',
    }).reset_index()
    
    grouped.columns = ['product_class_id', 'sku_count', 'brand', 'product_name', 
                       'image_url', 'product_url', 'total_stock', 'views', 
                       'add_to_cart', 'purchases', 'revenue']
    
    # CVR計算
    grouped['cvr'] = grouped.apply(
        lambda x: (x['purchases'] / x['views'] * 100) if x['views'] > 0 else 0, axis=1
    )
    
    # ソートして上位を取得
    grouped = grouped.sort_values(sort_by, ascending=False).head(limit)
    
    # 各グループのSKU詳細を取得
    result = []
    for _, row in grouped.iterrows():
        product = row.to_dict()
        # このproduct_class_idに属するSKUを取得
        skus = df[df['product_class_id'] == row['product_class_id']].sort_values(sort_by, ascending=False)
        product['skus'] = skus.to_dict('records')
        result.append(product)
    
    return result


def get_top_performers_grouped(brand=None, limit=20):
    """売上上位商品を取得（グループ化）"""
    df = data_store['merged_data']
    if df is None:
        return []
    
    filtered = df.copy()
    if brand and brand != 'all':
        filtered = filtered[filtered['brand'] == brand]
    
    if 'product_class_id' in filtered.columns:
        return get_grouped_products(filtered, 'revenue', limit)
    
    filtered = filtered.sort_values('revenue', ascending=False).head(limit)
    return filtered.to_dict('records')


@app.route('/login', methods=['GET', 'POST'])
def login():
    """ログイン画面"""
    if request.method == 'POST':
        password = request.form.get('password', '')
        if password == SITE_PASSWORD:
            session['logged_in'] = True
            return redirect(url_for('index'))
        else:
            flash('パスワードが違います', 'error')
    return render_template('login.html')


@app.route('/logout')
def logout():
    """ログアウト"""
    session.pop('logged_in', None)
    return redirect(url_for('login'))


@app.route('/')
@login_required
def index():
    """メインダッシュボード"""
    has_data = data_store['merged_data'] is not None
    brands = []
    summary = None
    pv_ranking = []
    analysis_period = None
    
    if has_data:
        brands = data_store['merged_data']['brand'].dropna().unique().tolist()
        summary = get_brand_summary()
        pv_ranking_by_brand = get_pv_ranking_by_brand(limit_per_brand=30)
        analysis_period = get_analysis_period()
    
    return render_template('index.html', 
                         has_data=has_data, 
                         brands=brands,
                         summary=summary,
                         pv_ranking_by_brand=pv_ranking_by_brand,
                         analysis_period=analysis_period)


@app.route('/upload', methods=['GET', 'POST'])
@login_required
def upload():
    """CSVアップロード画面"""
    if request.method == 'POST':
        # 商品マスタ
        if 'product_csv' in request.files:
            file = request.files['product_csv']
            if file.filename:
                filename = secure_filename(file.filename)
                filepath = os.path.join(app.config['UPLOAD_FOLDER'], 'product_master.csv')
                file.save(filepath)
                try:
                    data_store['product_master'] = load_product_master(filepath)
                    # R2にもアップロード（設定されている場合）
                    if is_r2_enabled():
                        upload_product_master(filepath)
                        flash(f'商品マスタを読み込み＆R2に保存しました（{len(data_store["product_master"])}件）', 'success')
                    else:
                        flash(f'商品マスタを読み込みました（{len(data_store["product_master"])}件）', 'success')
                except Exception as e:
                    flash(f'商品マスタの読み込みエラー: {str(e)}', 'error')
        
        # ブランド別GA売上
        for brand in BRANDS:
            field_name = f'ga_csv_{brand}'
            if field_name in request.files:
                file = request.files[field_name]
                if file.filename:
                    filename = secure_filename(file.filename)
                    filepath = os.path.join(app.config['UPLOAD_FOLDER'], f'ga_sales_{brand}.csv')
                    file.save(filepath)
                    try:
                        ga_result = load_ga_sales(filepath)
                        data_store['ga_sales'][brand] = ga_result
                        period = ga_result['period']
                        period_str = ""
                        if period['start_date'] and period['end_date']:
                            period_str = f"（{period['start_date'].strftime('%m/%d')}〜{period['end_date'].strftime('%m/%d')}）"
                        flash(f'{brand.upper()} GA売上を読み込みました（{len(ga_result["data"])}件）{period_str}', 'success')
                    except Exception as e:
                        flash(f'{brand.upper()} GA売上の読み込みエラー: {str(e)}', 'error')
        
        # 商品マスタとGAデータが揃ったら分析実行
        if data_store['product_master'] is not None and data_store['ga_sales']:
            merge_and_analyze()
            flash('データの突合・分析が完了しました！', 'success')
            return redirect(url_for('index'))
    
    # 現在のGA読み込み状況（期間情報含む）
    ga_status = {}
    for brand in BRANDS:
        if brand in data_store['ga_sales'] and data_store['ga_sales'][brand]:
            ga_info = data_store['ga_sales'][brand]
            ga_status[brand] = {
                'loaded': True,
                'count': len(ga_info['data']) if 'data' in ga_info else 0,
                'period': ga_info.get('period', {})
            }
        else:
            ga_status[brand] = {'loaded': False, 'count': 0, 'period': {}}
    
    # R2の商品マスタ情報
    r2_product_info = None
    if is_r2_enabled():
        r2_product_info = get_product_master_info()
    
    return render_template('upload.html',
                         has_product=data_store['product_master'] is not None,
                         ga_status=ga_status,
                         brands=BRANDS,
                         r2_enabled=is_r2_enabled(),
                         r2_product_info=r2_product_info,
                         ga4_api_enabled=is_ga4_configured())


@app.route('/sync-r2', methods=['POST'])
@login_required
def sync_r2():
    """R2から商品マスタを同期"""
    if not is_r2_enabled():
        flash('R2ストレージが設定されていません', 'error')
        return redirect(url_for('upload'))
    
    try:
        df = download_product_master()
        if df is not None:
            data_store['product_master'] = process_product_master_df(df)
            flash(f'R2から商品マスタを同期しました（{len(data_store["product_master"])}件）', 'success')
            
            # GAデータがあれば再分析
            if data_store['ga_sales']:
                merge_and_analyze()
        else:
            flash('R2に商品マスタが見つかりません', 'error')
    except Exception as e:
        flash(f'R2同期エラー: {str(e)}', 'error')
    
    return redirect(url_for('upload'))


@app.route('/fetch-ga4', methods=['POST'])
@login_required
def fetch_ga4():
    """GA4 APIからデータを取得（前期間データも同時取得）"""
    if not is_ga4_configured():
        flash('GA4 APIが設定されていません', 'error')
        return redirect(url_for('upload'))
    
    period_type = request.form.get('period_type', 'yesterday')  # 'yesterday' or 'weekly'
    
    try:
        # 現在期間のデータを取得
        results = fetch_all_brands_data(period_type)
        
        if not results:
            flash('GA4からデータを取得できませんでした', 'error')
            return redirect(url_for('upload'))
        
        # 取得したデータをdata_storeに保存 & R2にも保存
        for brand, result in results.items():
            data_store['ga_sales'][brand] = result
            period = result['period']
            period_str = f"（{period['start_date'].strftime('%m/%d')}〜{period['end_date'].strftime('%m/%d')}）"
            flash(f'{brand.upper()} GA4データを取得しました（{len(result["data"])}件）{period_str}', 'success')
            
            # R2に保存
            if save_ga4_data and is_r2_enabled():
                start_str = period['start_date'].strftime('%Y%m%d')
                end_str = period['end_date'].strftime('%Y%m%d')
                save_ga4_data(brand, result['data'], start_str, end_str)
        
        # 前期間データも取得（比較用）
        if period_type == 'yesterday':
            # 前日データなら前々日も取得
            from ga4_api import fetch_day_before_yesterday_data
            for brand in results.keys():
                prev_result = fetch_day_before_yesterday_data(brand)
                if prev_result:
                    data_store['ga_sales_previous'][brand] = prev_result
                    print(f"✅ Fetched previous day data for {brand}: {len(prev_result['data'])} items")
        
        # 商品マスタがあれば分析実行
        if data_store['product_master'] is not None:
            merge_and_analyze()
            flash('データの突合・分析が完了しました！', 'success')
            return redirect(url_for('index'))
        
    except Exception as e:
        flash(f'GA4データ取得エラー: {str(e)}', 'error')
    
    return redirect(url_for('upload'))


@app.route('/brand/<brand_name>')
@login_required
def brand_detail(brand_name):
    """ブランド別詳細"""
    if data_store['merged_data'] is None:
        return redirect(url_for('upload'))
    
    brand = brand_name if brand_name != 'all' else None
    
    problem_products = get_problem_products(brand)
    opportunity_products = get_opportunity_products(brand)
    top_products = get_top_performers(brand)
    pv_ranking = get_pv_ranking(brand, limit=50)
    
    # ブランド統計
    df = data_store['merged_data']
    if brand:
        brand_df = df[df['brand'] == brand]
    else:
        brand_df = df
    
    stats = {
        'total_sku': len(brand_df),
        'total_stock': int(brand_df['total_stock'].sum()),
        'total_revenue': float(brand_df['revenue'].sum()),
        'total_views': int(brand_df['views'].sum()),
        'avg_cvr': float(brand_df['cvr'].mean()),
        'problem_count': int(brand_df['is_problem'].sum()),
        'opportunity_count': int(brand_df['is_opportunity'].sum()),
    }
    
    brands = df['brand'].dropna().unique().tolist()
    analysis_period = get_analysis_period()
    
    return render_template('brand_detail.html',
                         brand_name=brand_name,
                         stats=stats,
                         problem_products=problem_products,
                         opportunity_products=opportunity_products,
                         top_products=top_products,
                         pv_ranking=pv_ranking,
                         brands=brands,
                         analysis_period=analysis_period)


@app.route('/api/products')
def api_products():
    """商品データAPI"""
    if data_store['merged_data'] is None:
        return jsonify([])
    
    brand = request.args.get('brand', 'all')
    category = request.args.get('category', 'all')  # problem, opportunity, top, pv
    limit = int(request.args.get('limit', 50))
    
    if category == 'problem':
        products = get_problem_products(brand, limit)
    elif category == 'opportunity':
        products = get_opportunity_products(brand, limit)
    elif category == 'pv':
        products = get_pv_ranking(brand, limit)
    else:
        products = get_top_performers(brand, limit)
    
    return jsonify(products)


def init_from_r2():
    """起動時にR2から最新の商品マスタを読み込む"""
    if not is_r2_enabled():
        print("⚠️ R2 is not enabled. Set R2 environment variables to enable.")
        return False
    
    try:
        print("☁️ Loading product master from R2...")
        df = download_product_master()
        if df is not None and len(df) > 0:
            data_store['product_master'] = process_product_master_df(df)
            data_store['product_master_info'] = get_product_master_info()
            print(f"✅ Loaded {len(data_store['product_master'])} products from R2")
            
            # GA4データも読み込み
            if get_latest_ga4_data:
                print("☁️ Loading GA4 data from R2...")
                for brand in BRANDS:
                    ga4_data = get_latest_ga4_data(brand)
                    if ga4_data:
                        from datetime import datetime
                        start_date = datetime.strptime(ga4_data['start_date'], '%Y%m%d') if ga4_data['start_date'] else None
                        end_date = datetime.strptime(ga4_data['end_date'], '%Y%m%d') if ga4_data['end_date'] else None
                        data_store['ga_sales'][brand] = {
                            'data': ga4_data['df'],
                            'period': {
                                'start_date': start_date,
                                'end_date': end_date,
                                'period_type': 'daily' if start_date == end_date else 'custom'
                            }
                        }
                        print(f"  ✅ {brand}: {len(ga4_data['df'])} rows")
                
                # 商品マスタとGA4データがあれば分析実行
                if any(data_store['ga_sales'].values()):
                    merge_and_analyze()
                    print("✅ Auto-merged data on startup")
            
            return True
        else:
            print("⚠️ No product master found in R2")
            return False
    except Exception as e:
        print(f"❌ Error loading from R2: {e}")
        return False


# アプリ起動時にR2から読み込み
with app.app_context():
    init_from_r2()


if __name__ == '__main__':
    # 開発時はdebug=Trueだが、環境変数が消える問題があるのでuse_reloader=Falseに
    app.run(debug=True, port=8080, host='0.0.0.0', use_reloader=False)
