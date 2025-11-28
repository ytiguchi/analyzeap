"""
商品在庫 × GA4売上 分析アプリ
- 商品マスタCSV（在庫・色・画像など）とGA4売上CSVを突き合わせ
- ブランド別に「在庫過多×低売上」などの商品を分析
- 商品画像付きでレポート表示
"""

import os
import re
from datetime import datetime
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
import pandas as pd
from werkzeug.utils import secure_filename

# R2ストレージ連携
try:
    from storage import download_product_master, upload_product_master, get_product_master_info, is_r2_enabled
except ImportError:
    is_r2_enabled = lambda: False
    download_product_master = None
    upload_product_master = None
    get_product_master_info = None

# GA4 API連携
try:
    from ga4_api import (
        is_ga4_configured, get_configured_brands, 
        fetch_yesterday_data, fetch_weekly_data, fetch_all_brands_data
    )
    GA4_API_ENABLED = is_ga4_configured()
except ImportError:
    GA4_API_ENABLED = False

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
    'merged_data': None
}

# 登録済みブランド一覧
BRANDS = ['rady', 'cherimi', 'michellmacaron', 'radycharm']


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
    
    data_store['merged_data'] = merged
    return merged


def get_brand_summary():
    """ブランド別サマリーを取得"""
    df = data_store['merged_data']
    if df is None:
        return None
    
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


def get_top_performers(brand=None, limit=20):
    """売上上位商品を取得"""
    df = data_store['merged_data']
    if df is None:
        return []
    
    filtered = df.copy()
    if brand and brand != 'all':
        filtered = filtered[filtered['brand'] == brand]
    
    filtered = filtered.sort_values('revenue', ascending=False).head(limit)
    return filtered.to_dict('records')


def get_pv_ranking(brand=None, limit=50, grouped=True):
    """PV（閲覧数）ランキングを取得"""
    df = data_store['merged_data']
    if df is None:
        return []
    
    filtered = df.copy()
    if brand and brand != 'all':
        filtered = filtered[filtered['brand'] == brand]
    
    # 閲覧数が0より大きいものだけ
    filtered = filtered[filtered['views'] > 0]
    
    if grouped and 'product_class_id' in filtered.columns:
        return get_grouped_products(filtered, 'views', limit)
    
    filtered = filtered.sort_values('views', ascending=False).head(limit)
    return filtered.to_dict('records')


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


@app.route('/')
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
        pv_ranking = get_pv_ranking(limit=10)  # TOP10
        analysis_period = get_analysis_period()
    
    return render_template('index.html', 
                         has_data=has_data, 
                         brands=brands,
                         summary=summary,
                         pv_ranking=pv_ranking,
                         analysis_period=analysis_period)


@app.route('/upload', methods=['GET', 'POST'])
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
                         ga4_api_enabled=GA4_API_ENABLED)


@app.route('/sync-r2', methods=['POST'])
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
def fetch_ga4():
    """GA4 APIからデータを取得"""
    if not GA4_API_ENABLED:
        flash('GA4 APIが設定されていません', 'error')
        return redirect(url_for('upload'))
    
    period_type = request.form.get('period_type', 'yesterday')  # 'yesterday' or 'weekly'
    
    try:
        results = fetch_all_brands_data(period_type)
        
        if not results:
            flash('GA4からデータを取得できませんでした', 'error')
            return redirect(url_for('upload'))
        
        # 取得したデータをdata_storeに保存
        for brand, result in results.items():
            data_store['ga_sales'][brand] = result
            period = result['period']
            period_str = f"（{period['start_date'].strftime('%m/%d')}〜{period['end_date'].strftime('%m/%d')}）"
            flash(f'{brand.upper()} GA4データを取得しました（{len(result["data"])}件）{period_str}', 'success')
        
        # 商品マスタがあれば分析実行
        if data_store['product_master'] is not None:
            merge_and_analyze()
            flash('データの突合・分析が完了しました！', 'success')
            return redirect(url_for('index'))
        
    except Exception as e:
        flash(f'GA4データ取得エラー: {str(e)}', 'error')
    
    return redirect(url_for('upload'))


@app.route('/brand/<brand_name>')
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
    app.run(debug=True, port=5050, use_reloader=False)
