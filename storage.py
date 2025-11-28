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
R2_ACCOUNT_ID = os.environ.get('R2_ACCOUNT_ID', '')
R2_ACCESS_KEY_ID = os.environ.get('R2_ACCESS_KEY_ID', '')
R2_SECRET_ACCESS_KEY = os.environ.get('R2_SECRET_ACCESS_KEY', '')
R2_BUCKET_NAME = os.environ.get('R2_BUCKET_NAME', 'analyzeap-data')

# 商品マスタのファイル名
PRODUCT_MASTER_KEY = 'product_list.csv'


def get_r2_client():
    """R2クライアントを取得"""
    if not all([R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY]):
        return None
    
    return boto3.client(
        's3',
        endpoint_url=f'https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com',
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        config=Config(signature_version='s3v4'),
        region_name='auto'
    )


def download_product_master():
    """R2から商品マスタCSVをダウンロード"""
    client = get_r2_client()
    if client is None:
        print("R2 credentials not configured, skipping download")
        return None
    
    try:
        response = client.get_object(Bucket=R2_BUCKET_NAME, Key=PRODUCT_MASTER_KEY)
        content = response['Body'].read()
        
        # エンコーディング検出して読み込み
        for enc in ['cp932', 'utf-8', 'utf-8-sig']:
            try:
                csv_str = content.decode(enc)
                df = pd.read_csv(StringIO(csv_str))
                print(f"Downloaded product master from R2: {len(df)} rows")
                return df
            except:
                continue
        
        return None
    except client.exceptions.NoSuchKey:
        print(f"Product master not found in R2: {PRODUCT_MASTER_KEY}")
        return None
    except Exception as e:
        print(f"Error downloading from R2: {e}")
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
                Key=PRODUCT_MASTER_KEY,
                Body=f,
                ContentType='text/csv'
            )
        print(f"Uploaded product master to R2: {PRODUCT_MASTER_KEY}")
        return True
    except Exception as e:
        print(f"Error uploading to R2: {e}")
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


def get_product_master_info():
    """商品マスタの情報を取得"""
    client = get_r2_client()
    if client is None:
        return None
    
    try:
        response = client.head_object(Bucket=R2_BUCKET_NAME, Key=PRODUCT_MASTER_KEY)
        return {
            'size': response['ContentLength'],
            'last_modified': response['LastModified'],
            'exists': True
        }
    except:
        return {'exists': False}

