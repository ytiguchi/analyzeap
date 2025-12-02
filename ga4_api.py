"""
GA4 API 連携
- Google Analytics Data API を使用してEコマースデータを取得
- 前日分析・週次分析に対応
"""

import os
import json
import time
from datetime import datetime, timedelta
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    RunReportRequest,
    Dimension,
    Metric,
    DateRange,
)
from google.oauth2 import service_account
import pandas as pd


def retry_with_backoff(func, max_retries=3, initial_delay=1):
    """リトライ処理（指数バックオフ）"""
    def wrapper(*args, **kwargs):
        delay = initial_delay
        last_exception = None
        for attempt in range(max_retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                last_exception = e
                if attempt < max_retries - 1:
                    print(f"[RETRY] Attempt {attempt + 1} failed: {e}. Retrying in {delay}s...")
                    time.sleep(delay)
                    delay *= 2  # 指数バックオフ
        print(f"[ERROR] All {max_retries} attempts failed: {last_exception}")
        return None
    return wrapper


def get_ga4_config():
    """GA4設定を取得（呼び出し時に環境変数を読む）"""
    return {
        'credentials_json': os.environ.get('GA4_CREDENTIALS_JSON', ''),
        'properties': {
            'rady': os.environ.get('GA4_PROPERTY_RADY', ''),
            'cherimi': os.environ.get('GA4_PROPERTY_CHERIMI', ''),
            'michellmacaron': os.environ.get('GA4_PROPERTY_MICHELLMACARON', ''),
            'solni': os.environ.get('GA4_PROPERTY_SOLNI', ''),
        }
    }


def is_ga4_configured() -> bool:
    """GA4 APIが設定されているかチェック"""
    config = get_ga4_config()
    if not config['credentials_json']:
        return False
    
    # 少なくとも1つのプロパティIDが設定されているか
    return any(config['properties'].values())


def get_configured_brands() -> list:
    """設定済みのブランド一覧を取得"""
    config = get_ga4_config()
    return [brand for brand, prop_id in config['properties'].items() if prop_id]


def get_ga4_client():
    """GA4 APIクライアントを取得"""
    config = get_ga4_config()
    
    if not config['credentials_json']:
        print("[ERROR] GA4_CREDENTIALS_JSON not set")
        return None
    
    try:
        credentials_info = json.loads(config['credentials_json'])
        credentials = service_account.Credentials.from_service_account_info(
            credentials_info,
            scopes=['https://www.googleapis.com/auth/analytics.readonly']
        )
        client = BetaAnalyticsDataClient(credentials=credentials)
        return client
    except Exception as e:
        print(f"[ERROR] Error creating GA4 client: {e}")
        return None


def _fetch_ecommerce_data_impl(client, property_id: str, start_date: str, end_date: str, brand: str) -> pd.DataFrame:
    """GA4データ取得の実装（リトライ対象）"""
    request = RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=[
            Dimension(name="itemId"),
            Dimension(name="itemName"),
        ],
        metrics=[
            Metric(name="itemsViewed"),
            Metric(name="itemsAddedToCart"),
            Metric(name="itemsPurchased"),
            Metric(name="itemRevenue"),
        ],
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
    )
    
    response = client.run_report(request)
    
    # レスポンスをDataFrameに変換
    rows = []
    for row in response.rows:
        rows.append({
            'sku_id': row.dimension_values[0].value,
            'item_name': row.dimension_values[1].value,
            'views': int(row.metric_values[0].value),
            'add_to_cart': int(row.metric_values[1].value),
            'purchases': int(row.metric_values[2].value),
            'revenue': float(row.metric_values[3].value),
        })
    
    # 空の場合でも必要なカラムを持つDataFrameを返す
    if not rows:
        print(f"[WARN] No data returned from GA4 for {brand}")
        df = pd.DataFrame(columns=['sku_id', 'item_name', 'views', 'add_to_cart', 'purchases', 'revenue'])
    else:
        df = pd.DataFrame(rows)
    
    print(f"[OK] Fetched {len(df)} items from GA4 for {brand}")
    return df


def fetch_ecommerce_data(brand: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    GA4からEコマースデータを取得（リトライ付き）
    
    Args:
        brand: ブランド名 (rady, cherimi, michellmacaron, radycharm)
        start_date: 開始日 (YYYY-MM-DD)
        end_date: 終了日 (YYYY-MM-DD)
    
    Returns:
        DataFrame with columns: sku_id, item_name, views, add_to_cart, purchases, revenue
    """
    client = get_ga4_client()
    if client is None:
        return None
    
    config = get_ga4_config()
    property_id = config['properties'].get(brand, '')
    
    if not property_id:
        print(f"[ERROR] GA4 property ID not set for brand: {brand}")
        return None
    
    # リトライ付きで実行
    fetch_with_retry = retry_with_backoff(
        lambda: _fetch_ecommerce_data_impl(client, property_id, start_date, end_date, brand),
        max_retries=3,
        initial_delay=2
    )
    
    try:
        return fetch_with_retry()
    except Exception as e:
        print(f"[ERROR] Error fetching GA4 data for {brand}: {e}")
        return None


def fetch_yesterday_data(brand: str) -> dict:
    """前日のデータを取得"""
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    df = fetch_ecommerce_data(brand, yesterday, yesterday)
    
    if df is not None:
        return {
            'data': df,
            'period': {
                'start_date': datetime.strptime(yesterday, '%Y-%m-%d'),
                'end_date': datetime.strptime(yesterday, '%Y-%m-%d'),
                'days': 1,
                'period_type': 'daily'
            }
        }
    return None


def fetch_day_before_yesterday_data(brand: str) -> dict:
    """前々日のデータを取得"""
    day_before = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
    df = fetch_ecommerce_data(brand, day_before, day_before)
    
    if df is not None:
        return {
            'data': df,
            'period': {
                'start_date': datetime.strptime(day_before, '%Y-%m-%d'),
                'end_date': datetime.strptime(day_before, '%Y-%m-%d'),
                'days': 1,
                'period_type': 'daily'
            }
        }
    return None


def fetch_comparison_data(brand: str) -> dict:
    """前日と前々日の比較データを取得"""
    yesterday = fetch_yesterday_data(brand)
    day_before = fetch_day_before_yesterday_data(brand)
    
    if yesterday is None:
        return None
    
    result = {
        'current': yesterday,
        'previous': day_before,
        'has_comparison': day_before is not None
    }
    
    return result


def fetch_3days_data(brand: str) -> dict:
    """直近3日間のデータを取得"""
    end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d')
    df = fetch_ecommerce_data(brand, start_date, end_date)
    
    if df is not None:
        return {
            'data': df,
            'period': {
                'start_date': datetime.strptime(start_date, '%Y-%m-%d'),
                'end_date': datetime.strptime(end_date, '%Y-%m-%d'),
                'days': 3,
                'period_type': '3days'
            }
        }
    return None


def fetch_previous_3days_data(brand: str) -> dict:
    """前の3日間のデータを取得（4日前〜6日前）"""
    end_date = (datetime.now() - timedelta(days=4)).strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=6)).strftime('%Y-%m-%d')
    df = fetch_ecommerce_data(brand, start_date, end_date)
    
    if df is not None:
        return {
            'data': df,
            'period': {
                'start_date': datetime.strptime(start_date, '%Y-%m-%d'),
                'end_date': datetime.strptime(end_date, '%Y-%m-%d'),
                'days': 3,
                'period_type': '3days'
            }
        }
    return None


def fetch_previous_weekly_data(brand: str) -> dict:
    """前週のデータを取得（8日前〜14日前）"""
    end_date = (datetime.now() - timedelta(days=8)).strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=14)).strftime('%Y-%m-%d')
    df = fetch_ecommerce_data(brand, start_date, end_date)
    
    if df is not None:
        return {
            'data': df,
            'period': {
                'start_date': datetime.strptime(start_date, '%Y-%m-%d'),
                'end_date': datetime.strptime(end_date, '%Y-%m-%d'),
                'days': 7,
                'period_type': 'weekly'
            }
        }
    return None


def fetch_weekly_data(brand: str) -> dict:
    """過去7日間のデータを取得"""
    end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    df = fetch_ecommerce_data(brand, start_date, end_date)
    
    if df is not None:
        return {
            'data': df,
            'period': {
                'start_date': datetime.strptime(start_date, '%Y-%m-%d'),
                'end_date': datetime.strptime(end_date, '%Y-%m-%d'),
                'days': 7,
                'period_type': 'weekly'
            }
        }
    return None


def fetch_custom_data(brand: str, start_date: str, end_date: str) -> dict:
    """カスタム期間のデータを取得"""
    df = fetch_ecommerce_data(brand, start_date, end_date)
    
    if df is not None:
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        days = (end_dt - start_dt).days + 1
        
        return {
            'data': df,
            'period': {
                'start_date': start_dt,
                'end_date': end_dt,
                'days': days,
                'period_type': 'weekly' if days == 7 else 'daily' if days == 1 else 'custom'
            }
        }
    return None


def fetch_all_brands_data(period_type: str = 'weekly') -> dict:
    """
    全ブランドのデータを取得
    
    Args:
        period_type: 'yesterday', 'weekly', or '3days'
    
    Returns:
        dict: {brand: {'data': df, 'period': {...}}, ...}
    """
    config = get_ga4_config()
    results = {}
    
    for brand, prop_id in config['properties'].items():
        if not prop_id:
            print(f"[WARN] Skipping {brand} - no property ID configured")
            continue
        
        if period_type == 'yesterday':
            result = fetch_yesterday_data(brand)
        elif period_type == '3days':
            result = fetch_3days_data(brand)
        else:  # weekly
            result = fetch_weekly_data(brand)
        
        if result is not None:
            results[brand] = result
    
    return results


def fetch_channel_data(brand: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    チャネル別のトラフィック・売上データを取得（詳細ソース含む）
    """
    client = get_ga4_client()
    if client is None:
        return None
    
    config = get_ga4_config()
    property_id = config['properties'].get(brand, '')
    
    if not property_id:
        print(f"[ERROR] GA4 property ID not set for brand: {brand}")
        return None
    
    try:
        # チャネルグループ + 詳細ソースを取得
        request = RunReportRequest(
            property=f"properties/{property_id}",
            dimensions=[
                Dimension(name="sessionDefaultChannelGroup"),
                Dimension(name="sessionSource"),
            ],
            metrics=[
                Metric(name="sessions"),
                Metric(name="activeUsers"),
                Metric(name="ecommercePurchases"),
                Metric(name="purchaseRevenue"),
            ],
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        )
        
        response = client.run_report(request)
        
        rows = []
        for row in response.rows:
            rows.append({
                'channel': row.dimension_values[0].value,
                'source': row.dimension_values[1].value,
                'sessions': int(row.metric_values[0].value),
                'users': int(row.metric_values[1].value),
                'purchases': int(row.metric_values[2].value),
                'revenue': float(row.metric_values[3].value),
            })
        
        df = pd.DataFrame(rows)
        print(f"[OK] Fetched channel data for {brand}: {len(df)} sources")
        return df
    
    except Exception as e:
        print(f"[ERROR] Error fetching channel data for {brand}: {e}")
        return None


# チャネル名の日本語マッピング
CHANNEL_NAME_MAP = {
    'Organic Search': '🔍 自然検索（Google等）',
    'Direct': '🔗 ダイレクト（直接アクセス）',
    'Organic Social': '📱 SNS（自然流入）',
    'Paid Social': '💰 SNS広告',
    'Referral': '🔀 参照サイト',
    'Email': '📧 メール',
    'Paid Search': '💎 検索広告（リスティング）',
    'Display': '🖼️ ディスプレイ広告',
    'Affiliates': '🤝 アフィリエイト',
    'Unassigned': '❓ 未分類',
    'Cross-network': '🌐 クロスネットワーク',
    'Video': '🎬 動画広告',
    'Audio': '🎵 音声広告',
    'SMS': '💬 SMS',
    'Mobile Push Notifications': '📲 プッシュ通知',
}

# 詳細ソースの日本語マッピング
SOURCE_NAME_MAP = {
    'google': 'Google',
    'instagram': 'Instagram',
    'facebook': 'Facebook',
    'twitter': 'Twitter/X',
    't.co': 'Twitter/X',
    'tiktok': 'TikTok',
    'youtube': 'YouTube',
    'yahoo': 'Yahoo!',
    'bing': 'Bing',
    'line': 'LINE',
    'pinterest': 'Pinterest',
    'note': 'note',
    '(direct)': '直接アクセス',
}


def translate_channel_name(channel: str) -> str:
    """チャネル名を日本語に変換"""
    return CHANNEL_NAME_MAP.get(channel, f'📡 {channel}')


def fetch_campaign_data(brand: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    キャンペーン別のトラフィック・売上データを取得
    """
    client = get_ga4_client()
    if client is None:
        return None
    
    config = get_ga4_config()
    property_id = config['properties'].get(brand, '')
    
    if not property_id:
        return None
    
    try:
        request = RunReportRequest(
            property=f"properties/{property_id}",
            dimensions=[
                Dimension(name="sessionCampaignName"),
                Dimension(name="sessionSource"),
                Dimension(name="sessionMedium"),
            ],
            metrics=[
                Metric(name="sessions"),
                Metric(name="activeUsers"),
                Metric(name="ecommercePurchases"),
                Metric(name="purchaseRevenue"),
            ],
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        )
        
        response = client.run_report(request)
        
        rows = []
        for row in response.rows:
            rows.append({
                'campaign': row.dimension_values[0].value,
                'source': row.dimension_values[1].value,
                'medium': row.dimension_values[2].value,
                'sessions': int(row.metric_values[0].value),
                'users': int(row.metric_values[1].value),
                'purchases': int(row.metric_values[2].value),
                'revenue': float(row.metric_values[3].value),
            })
        
        df = pd.DataFrame(rows)
        print(f"[OK] Fetched campaign data for {brand}: {len(df)} campaigns")
        return df
    
    except Exception as e:
        print(f"[ERROR] Error fetching campaign data for {brand}: {e}")
        return None


# 広告タイプの分類パターン
AD_TYPE_PATTERNS = {
    'pmax': {
        'name': '🚀 Performance Max',
        'patterns': ['pmax', 'performance max', 'performance_max', 'p-max'],
    },
    'meta': {
        'name': '📘 Meta広告（FB/IG）',
        'patterns': ['facebook', 'instagram', 'meta', 'fb_', 'ig_'],
        'sources': ['facebook', 'instagram', 'fb', 'ig'],
    },
    'line': {
        'name': '💚 LINE広告',
        'patterns': ['line_ads', 'line広告', 'lineads'],
        'sources': ['line'],
        'mediums': ['cpc', 'cpm', 'paid'],
    },
    'google_search': {
        'name': '🔍 Google検索広告',
        'patterns': ['google_search', 'gsa_', 'search_'],
        'sources': ['google'],
        'mediums': ['cpc', 'ppc'],
    },
    'google_display': {
        'name': '🖼️ Googleディスプレイ',
        'patterns': ['gdn_', 'display_', 'gdn'],
        'sources': ['google'],
        'mediums': ['display', 'banner'],
    },
    'yahoo': {
        'name': '🔴 Yahoo!広告',
        'patterns': ['yahoo_', 'yda_', 'yss_'],
        'sources': ['yahoo'],
        'mediums': ['cpc', 'cpm'],
    },
    'tiktok': {
        'name': '🎵 TikTok広告',
        'patterns': ['tiktok_', 'tiktok'],
        'sources': ['tiktok'],
        'mediums': ['cpc', 'cpm', 'paid'],
    },
    'affiliate': {
        'name': '🤝 アフィリエイト',
        'patterns': ['affiliate', 'aff_', 'a8', 'valuecommerce', 'accesstrade'],
    },
}


def classify_ad_type(campaign: str, source: str, medium: str) -> str:
    """キャンペーン/ソース/メディウムから広告タイプを分類"""
    campaign_lower = campaign.lower() if campaign else ''
    source_lower = source.lower() if source else ''
    medium_lower = medium.lower() if medium else ''
    
    for ad_type, config in AD_TYPE_PATTERNS.items():
        # キャンペーン名でマッチ
        for pattern in config.get('patterns', []):
            if pattern in campaign_lower:
                return ad_type
        
        # ソース+メディウムでマッチ
        if 'sources' in config:
            for src_pattern in config['sources']:
                if src_pattern in source_lower:
                    # メディウムもチェック（有料広告のみ）
                    if 'mediums' in config:
                        for med_pattern in config['mediums']:
                            if med_pattern in medium_lower:
                                return ad_type
                    elif medium_lower in ['cpc', 'cpm', 'paid', 'display', 'banner', 'video']:
                        return ad_type
    
    # 有料広告っぽいけど分類できない
    if medium_lower in ['cpc', 'cpm', 'paid']:
        return 'other_paid'
    
    return None  # 広告ではない


def fetch_all_brands_campaign_data(period_type: str = 'weekly') -> dict:
    """全ブランドのキャンペーンデータを取得"""
    config = get_ga4_config()
    results = {}
    
    # 期間計算
    if period_type == 'yesterday':
        start_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        end_date = start_date
        prev_start = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
        prev_end = prev_start
    elif period_type == '3days':
        end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d')
        prev_end = (datetime.now() - timedelta(days=4)).strftime('%Y-%m-%d')
        prev_start = (datetime.now() - timedelta(days=6)).strftime('%Y-%m-%d')
    else:  # weekly
        end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        prev_end = (datetime.now() - timedelta(days=8)).strftime('%Y-%m-%d')
        prev_start = (datetime.now() - timedelta(days=14)).strftime('%Y-%m-%d')
    
    for brand in config['properties'].keys():
        try:
            df = fetch_campaign_data(brand, start_date, end_date)
            prev_df = fetch_campaign_data(brand, prev_start, prev_end)
            
            if df is not None:
                results[brand] = {
                    'current': df,
                    'previous': prev_df
                }
        except Exception as e:
            print(f"[ERROR] Campaign data for {brand}: {e}")
    
    return results


def translate_source_name(source: str) -> str:
    """ソース名をわかりやすく変換"""
    source_lower = source.lower()
    for key, name in SOURCE_NAME_MAP.items():
        if key in source_lower:
            return name
    return source


def fetch_all_brands_channel_data(period_type: str = 'weekly') -> dict:
    """全ブランドのチャネルデータを取得（前期間も含む）"""
    config = get_ga4_config()
    results = {}
    
    # 期間計算
    if period_type == 'yesterday':
        start_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        end_date = start_date
        prev_start = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
        prev_end = prev_start
    elif period_type == '3days':
        start_date = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d')
        end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        prev_start = (datetime.now() - timedelta(days=6)).strftime('%Y-%m-%d')
        prev_end = (datetime.now() - timedelta(days=4)).strftime('%Y-%m-%d')
    else:  # weekly
        start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        prev_start = (datetime.now() - timedelta(days=14)).strftime('%Y-%m-%d')
        prev_end = (datetime.now() - timedelta(days=8)).strftime('%Y-%m-%d')
    
    for brand, prop_id in config['properties'].items():
        if not prop_id:
            continue
        
        # 現在期間
        df = fetch_channel_data(brand, start_date, end_date)
        # 前期間
        prev_df = fetch_channel_data(brand, prev_start, prev_end)
        
        if df is not None:
            results[brand] = {
                'current': df,
                'previous': prev_df,
                'period': {
                    'start': start_date,
                    'end': end_date,
                    'prev_start': prev_start,
                    'prev_end': prev_end
                }
            }
    
    return results
