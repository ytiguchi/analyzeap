"""
Cloudflare R2 ストレージ連携
- 商品マスタCSVの自動読み込み
- S3互換APIを使用
"""

import os
import boto3
from botocore.config import Config
import pandas as pd
from io import StringIO, BytesIO


def get_r2_config():
    """R2設定を取得（呼び出し時に環境変数を読む）"""
    return {
        'endpoint_url': os.environ.get('R2_ENDPOINT_URL', ''),
        'access_key_id': os.environ.get('R2_ACCESS_KEY_ID', ''),
        'secret_access_key': os.environ.get('R2_SECRET_ACCESS_KEY', ''),
        'bucket_name': os.environ.get('R2_BUCKET_NAME', 'analyzeap-data'),
        'product_master_key': os.environ.get('R2_PRODUCT_MASTER_KEY', 'product_master.csv'),
    }


def get_r2_client():
    """R2クライアントを取得"""
    config = get_r2_config()
    
    if not all([config['endpoint_url'], config['access_key_id'], config['secret_access_key']]):
        print(f"R2 config missing: ENDPOINT={bool(config['endpoint_url'])}, KEY={bool(config['access_key_id'])}, SECRET={bool(config['secret_access_key'])}")
        return None
    
    return boto3.client(
        's3',
        endpoint_url=config['endpoint_url'],
        aws_access_key_id=config['access_key_id'],
        aws_secret_access_key=config['secret_access_key'],
        config=Config(signature_version='s3v4'),
        region_name='auto'
    )


def is_r2_enabled():
    """R2が有効かどうかを確認"""
    config = get_r2_config()
    return all([config['endpoint_url'], config['access_key_id'], config['secret_access_key']])


def find_latest_csv():
    """R2バケット内の最新の商品マスタCSVファイルを見つける（ga4_data/は除外）"""
    client = get_r2_client()
    if client is None:
        return None
    
    config = get_r2_config()
    
    try:
        response = client.list_objects_v2(Bucket=config['bucket_name'])
        csv_files = []
        for obj in response.get('Contents', []):
            key = obj['Key']
            # ga4_data/ と periods/ フォルダは除外（商品マスタのみ対象）
            if key.endswith('.csv') and not key.startswith('ga4_data/') and not key.startswith('periods/') and not key.startswith('config/'):
                csv_files.append({
                    'key': key,
                    'last_modified': obj['LastModified'],
                    'size': obj['Size']
                })
        
        if not csv_files:
            print("[ERROR] No product master CSV files found in R2")
            return None
        
        # 最新のファイルを取得
        latest = max(csv_files, key=lambda x: x['last_modified'])
        print(f"[OK] Found product master CSV: {latest['key']}")
        return latest
    except Exception as e:
        print(f"[ERROR] Error finding latest CSV: {e}")
        return None


def download_product_master():
    """R2から商品マスタCSVをダウンロード（最新のCSVを自動検出）"""
    client = get_r2_client()
    if client is None:
        print("R2 credentials not configured, skipping download")
        return None
    
    config = get_r2_config()
    
    # 最新のCSVファイルを探す
    latest = find_latest_csv()
    file_key = latest['key'] if latest else config['product_master_key']
    
    try:
        print(f"[INFO] Downloading from R2: {config['bucket_name']}/{file_key}")
        response = client.get_object(Bucket=config['bucket_name'], Key=file_key)
        content = response['Body'].read()
        
        # エンコーディング検出して読み込み
        for enc in ['utf-8', 'utf-8-sig', 'cp932']:
            try:
                csv_str = content.decode(enc)
                df = pd.read_csv(StringIO(csv_str))
                print(f"[OK] Downloaded product master from R2: {len(df)} rows")
                return df
            except Exception as e:
                print(f"  Encoding {enc} failed: {e}")
                continue
        
        print("[ERROR] All encodings failed")
        return None
    except Exception as e:
        print(f"[ERROR] Error downloading from R2: {e}")
        return None


def upload_product_master(filepath):
    """商品マスタCSVをR2にアップロード"""
    client = get_r2_client()
    if client is None:
        print("R2 credentials not configured, skipping upload")
        return False
    
    config = get_r2_config()
    
    try:
        with open(filepath, 'rb') as f:
            client.put_object(
                Bucket=config['bucket_name'],
                Key=config['product_master_key'],
                Body=f,
                ContentType='text/csv'
            )
        print(f"[OK] Uploaded product master to R2: {config['product_master_key']}")
        return True
    except Exception as e:
        print(f"[ERROR] Error uploading to R2: {e}")
        return False


def list_r2_files():
    """R2バケット内のファイル一覧を取得"""
    client = get_r2_client()
    if client is None:
        return []
    
    config = get_r2_config()
    
    try:
        response = client.list_objects_v2(Bucket=config['bucket_name'])
        files = []
        for obj in response.get('Contents', []):
            files.append({
                'key': obj['Key'],
                'size': obj['Size'],
                'last_modified': obj['LastModified']
            })
        return files
    except Exception as e:
        print(f"Error listing R2 files: {e}")
        return []


def save_ga4_data(brand, df, start_date, end_date):
    """GA4データをR2に保存"""
    client = get_r2_client()
    if client is None:
        print("R2 credentials not configured, skipping GA4 data save")
        return False
    
    config = get_r2_config()
    
    try:
        # ファイル名: ga4_data/brand_YYYYMMDD_YYYYMMDD.csv
        filename = f"ga4_data/{brand}_{start_date}_{end_date}.csv"
        
        # DataFrameをCSVに変換
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        
        client.put_object(
            Bucket=config['bucket_name'],
            Key=filename,
            Body=csv_buffer.getvalue().encode('utf-8'),
            ContentType='text/csv'
        )
        print(f"[OK] Saved GA4 data to R2: {filename} ({len(df)} rows)")
        return True
    except Exception as e:
        print(f"[ERROR] Error saving GA4 data to R2: {e}")
        return False


def save_period_data(period_type, brand, df, start_date, end_date, is_previous=False):
    """期間タイプ別にGA4データをR2に保存"""
    client = get_r2_client()
    if client is None:
        return False
    
    config = get_r2_config()
    
    try:
        # ファイル名: periods/period_type/brand.csv または periods/period_type/brand_prev.csv
        suffix = "_prev" if is_previous else ""
        filename = f"periods/{period_type}/{brand}{suffix}.csv"
        
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        
        # メタデータとして日付を保存
        metadata = {
            'start_date': start_date,
            'end_date': end_date
        }
        
        client.put_object(
            Bucket=config['bucket_name'],
            Key=filename,
            Body=csv_buffer.getvalue().encode('utf-8'),
            ContentType='text/csv',
            Metadata=metadata
        )
        print(f"[OK] Saved period data: {filename}")
        return True
    except Exception as e:
        print(f"[ERROR] Error saving period data: {e}")
        return False


def load_period_data(period_type, brand, is_previous=False):
    """期間タイプ別にGA4データをR2から読み込み"""
    client = get_r2_client()
    if client is None:
        return None
    
    config = get_r2_config()
    
    try:
        suffix = "_prev" if is_previous else ""
        filename = f"periods/{period_type}/{brand}{suffix}.csv"
        
        response = client.get_object(Bucket=config['bucket_name'], Key=filename)
        content = response['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(content))
        
        # 必須カラムの検証
        required_cols = ['sku_id', 'views', 'add_to_cart', 'purchases', 'revenue']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            print(f"[WARN] Period data {period_type}/{brand} missing columns: {missing_cols}, skipping")
            return None
        
        # メタデータから日付を取得
        metadata = response.get('Metadata', {})
        start_date = metadata.get('start_date')
        end_date = metadata.get('end_date')
        
        return {
            'df': df,
            'start_date': start_date,
            'end_date': end_date
        }
    except client.exceptions.NoSuchKey:
        return None
    except Exception as e:
        print(f"[ERROR] Error loading period data {period_type}/{brand}: {e}")
        return None


def get_available_periods():
    """R2に保存されている期間タイプの一覧を取得"""
    client = get_r2_client()
    if client is None:
        return []
    
    config = get_r2_config()
    
    try:
        response = client.list_objects_v2(
            Bucket=config['bucket_name'],
            Prefix="periods/",
            Delimiter="/"
        )
        
        periods = []
        for prefix in response.get('CommonPrefixes', []):
            period = prefix['Prefix'].replace('periods/', '').rstrip('/')
            if period in ['yesterday', '3days', 'weekly']:
                periods.append(period)
        
        return periods
    except Exception as e:
        print(f"[ERROR] Error getting available periods: {e}")
        return []


def get_latest_ga4_data(brand):
    """R2から最新のGA4データを取得"""
    client = get_r2_client()
    if client is None:
        return None
    
    config = get_r2_config()
    
    try:
        # ga4_data/ フォルダ内のファイルを検索
        response = client.list_objects_v2(
            Bucket=config['bucket_name'],
            Prefix=f"ga4_data/{brand}_"
        )
        
        files = []
        for obj in response.get('Contents', []):
            files.append({
                'key': obj['Key'],
                'last_modified': obj['LastModified']
            })
        
        if not files:
            return None
        
        # 最新のファイルを取得
        latest = max(files, key=lambda x: x['last_modified'])
        
        response = client.get_object(Bucket=config['bucket_name'], Key=latest['key'])
        content = response['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(content))
        
        # ファイル名から日付を抽出
        filename = latest['key'].split('/')[-1]  # brand_YYYYMMDD_YYYYMMDD.csv
        parts = filename.replace('.csv', '').split('_')
        start_date = parts[-2] if len(parts) >= 3 else None
        end_date = parts[-1] if len(parts) >= 3 else None
        
        print(f"[OK] Loaded GA4 data from R2: {brand} ({len(df)} rows)")
        return {
            'df': df,
            'start_date': start_date,
            'end_date': end_date,
            'last_modified': latest['last_modified']
        }
    except Exception as e:
        print(f"[ERROR] Error loading GA4 data from R2: {e}")
        return None


def save_passwords(passwords_dict):
    """パスワード設定をR2に保存（JSONファイル）"""
    import json
    client = get_r2_client()
    if client is None:
        print("R2 credentials not configured, skipping password save")
        return False
    
    config = get_r2_config()
    
    try:
        json_str = json.dumps(passwords_dict, ensure_ascii=False)
        client.put_object(
            Bucket=config['bucket_name'],
            Key='config/passwords.json',
            Body=json_str.encode('utf-8'),
            ContentType='application/json'
        )
        print("[OK] Saved passwords to R2")
        return True
    except Exception as e:
        print(f"[ERROR] Error saving passwords to R2: {e}")
        return False


def load_passwords():
    """R2からパスワード設定を読み込み"""
    import json
    from botocore.exceptions import ClientError
    
    client = get_r2_client()
    if client is None:
        return None
    
    config = get_r2_config()
    
    try:
        response = client.get_object(Bucket=config['bucket_name'], Key='config/passwords.json')
        content = response['Body'].read().decode('utf-8')
        passwords = json.loads(content)
        print("[OK] Loaded passwords from R2")
        return passwords
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', '')
        if error_code == 'NoSuchKey':
            print("[INFO] No passwords.json found in R2, will use defaults")
            return None
        print(f"[WARN] Error loading passwords from R2: {e}")
        return None
    except Exception as e:
        print(f"[WARN] Error loading passwords from R2: {e}")
        return None


def get_product_master_info():
    """商品マスタの情報を取得（最新のCSVを自動検出）"""
    client = get_r2_client()
    if client is None:
        print("[ERROR] get_product_master_info: R2 client is None")
        return {'exists': False}
    
    config = get_r2_config()
    
    # 最新のCSVを探す
    latest = find_latest_csv()
    if latest:
        return {
            'key': latest['key'],
            'size': latest['size'],
            'last_modified': latest['last_modified'],
            'exists': True
        }
    
    # フォールバック: 固定ファイル名で探す
    try:
        print(f"📂 Checking R2: {config['bucket_name']}/{config['product_master_key']}")
        response = client.head_object(Bucket=config['bucket_name'], Key=config['product_master_key'])
        info = {
            'key': config['product_master_key'],
            'size': response['ContentLength'],
            'last_modified': response['LastModified'],
            'exists': True
        }
        print(f"[OK] Found: {info}")
        return info
    except Exception as e:
        print(f"[ERROR] get_product_master_info error: {e}")
        return {'exists': False}
