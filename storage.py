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

# R2設定（環境変数から取得）
R2_ENDPOINT_URL = os.environ.get('R2_ENDPOINT_URL', '')
R2_ACCESS_KEY_ID = os.environ.get('R2_ACCESS_KEY_ID', '')
R2_SECRET_ACCESS_KEY = os.environ.get('R2_SECRET_ACCESS_KEY', '')
R2_BUCKET_NAME = os.environ.get('R2_BUCKET_NAME', 'analyzeap-data')
R2_PRODUCT_MASTER_KEY = os.environ.get('R2_PRODUCT_MASTER_KEY', 'product_master.csv')


def get_r2_client():
    """R2クライアントを取得"""
    if not all([R2_ENDPOINT_URL, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY]):
        print(f"R2 config missing: ENDPOINT={bool(R2_ENDPOINT_URL)}, KEY={bool(R2_ACCESS_KEY_ID)}, SECRET={bool(R2_SECRET_ACCESS_KEY)}")
        return None
    
    return boto3.client(
        's3',
        endpoint_url=R2_ENDPOINT_URL,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        config=Config(signature_version='s3v4'),
        region_name='auto'
    )


def download_product_master():
    """R2から商品マスタCSVをダウンロード（最新のCSVを自動検出）"""
    client = get_r2_client()
    if client is None:
        print("R2 credentials not configured, skipping download")
        return None
    
    # 最新のCSVファイルを探す
    latest = find_latest_csv()
    file_key = latest['key'] if latest else R2_PRODUCT_MASTER_KEY
    
    try:
        print(f"📥 Downloading from R2: {R2_BUCKET_NAME}/{file_key}")
        response = client.get_object(Bucket=R2_BUCKET_NAME, Key=file_key)
        content = response['Body'].read()
        
        # エンコーディング検出して読み込み
        for enc in ['utf-8', 'utf-8-sig', 'cp932']:
            try:
                csv_str = content.decode(enc)
                df = pd.read_csv(StringIO(csv_str))
                print(f"✅ Downloaded product master from R2: {len(df)} rows (encoding: {enc})")
                return df
            except Exception as e:
                print(f"  Encoding {enc} failed: {e}")
                continue
        
        print("❌ All encodings failed")
        return None
    except Exception as e:
        print(f"❌ Error downloading from R2: {e}")
        return None


def upload_product_master(filepath):
    """商品マスタCSVをR2にアップロード"""
    client = get_r2_client()
    if client is None:
        print("R2 credentials not configured, skipping upload")
        return False
    
    try:
        with open(filepath, 'rb') as f:
            client.put_object(
                Bucket=R2_BUCKET_NAME,
                Key=R2_PRODUCT_MASTER_KEY,
                Body=f,
                ContentType='text/csv'
            )
        print(f"✅ Uploaded product master to R2: {R2_PRODUCT_MASTER_KEY}")
        return True
    except Exception as e:
        print(f"❌ Error uploading to R2: {e}")
        return False


def list_r2_files():
    """R2バケット内のファイル一覧を取得"""
    client = get_r2_client()
    if client is None:
        return []
    
    try:
        response = client.list_objects_v2(Bucket=R2_BUCKET_NAME)
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


def find_latest_csv():
    """R2バケット内の最新のCSVファイルを見つける"""
    client = get_r2_client()
    if client is None:
        return None
    
    try:
        response = client.list_objects_v2(Bucket=R2_BUCKET_NAME)
        csv_files = []
        for obj in response.get('Contents', []):
            key = obj['Key']
            if key.endswith('.csv'):
                csv_files.append({
                    'key': key,
                    'last_modified': obj['LastModified'],
                    'size': obj['Size']
                })
        
        if not csv_files:
            print("❌ No CSV files found in R2")
            return None
        
        # 最新のファイルを取得
        latest = max(csv_files, key=lambda x: x['last_modified'])
        print(f"✅ Found latest CSV: {latest['key']} (modified: {latest['last_modified']})")
        return latest
    except Exception as e:
        print(f"❌ Error finding latest CSV: {e}")
        return None


def get_product_master_info():
    """商品マスタの情報を取得（最新のCSVを自動検出）"""
    client = get_r2_client()
    if client is None:
        print("❌ get_product_master_info: R2 client is None")
        return {'exists': False}
    
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
        print(f"📂 Checking R2: {R2_BUCKET_NAME}/{R2_PRODUCT_MASTER_KEY}")
        response = client.head_object(Bucket=R2_BUCKET_NAME, Key=R2_PRODUCT_MASTER_KEY)
        info = {
            'key': R2_PRODUCT_MASTER_KEY,
            'size': response['ContentLength'],
            'last_modified': response['LastModified'],
            'exists': True
        }
        print(f"✅ Found: {info}")
        return info
    except Exception as e:
        print(f"❌ get_product_master_info error: {e}")
        return {'exists': False}
