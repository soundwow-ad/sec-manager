import streamlit as st
import pandas as pd
import sqlite3
import os
import hashlib
from datetime import datetime, timedelta, date

from services_cue_parser import (
    parse_cueapp_excel as service_parse_cueapp_excel,
    parse_cue_excel_for_table1 as service_parse_cue_excel_for_table1,
    parse_excel_daily_ads as service_parse_excel_daily_ads,
)
from services_table_builders import (
    build_daily_inventory_and_metrics as service_build_daily_inventory_and_metrics,
    build_table1_from_cue_excel as service_build_table1_from_cue_excel,
    build_table1_from_segments as service_build_table1_from_segments,
    build_table2_details_by_company as service_build_table2_details_by_company,
    build_table2_summary_by_company as service_build_table2_summary_by_company,
    build_table3_monthly_control as service_build_table3_monthly_control,
    build_excel_table1_view as service_build_excel_table1_view,
)
from services_summary_viz import (
    build_annual_seconds_summary as service_build_annual_seconds_summary,
    build_visualization_summary_excel as service_build_visualization_summary_excel,
    build_visualization_summary_pdf as service_build_visualization_summary_pdf,
)
from services_segments import (
    build_ad_flight_segments as service_build_ad_flight_segments,
    explode_segments_to_daily as service_explode_segments_to_daily,
)
from services_utils import (
    df_to_excel_bytes as _df_to_excel_bytes,
    sanitize_dataframe_for_display as _sanitize_dataframe_for_display,
    styler_one_decimal as _styler_one_decimal_impl,
    seconds_to_spot_label as _seconds_to_spot_label_impl,
    normalize_date as _normalize_date_impl,
    SECONDS_USAGE_TYPES,
    normalize_seconds_type as _normalize_seconds_type_impl,
)
from config_ragic import RAGIC_FIELDS, RAGIC_SUBTABLE_FIELDS


# ==========================================
# 1. 設定區 (Configuration)
# ==========================================

# 平台店數對照表 (Store Count Logic)
# ⚠️ 重要：請根據實際情況修正這些數值
# 以下為「平台名稱」直接對應；未列在此處者改由 REGION_STORE_COUNTS 依區域對應
STORE_COUNTS = {
    "新鮮視全省": 3124,
    "新鮮視北北基": 1127,
    "新鮮視中彰投": 528,
    "全家廣播": 4200,
    # 若找不到對應，改依區域查 REGION_STORE_COUNTS，再無則預設為 1
}

# 區域店數對照（對齊獎金表；新鮮視/企頻 等皆依「區域」取店數）
REGION_STORE_COUNTS = {
    "全省": 3124,
    "北北基": 1127,
    "桃竹苗": 616,
    "中彰投": 528,
    "高高屏": 405,
    "雲嘉南": 365,
    "宜花東": 83,
}

# 平台產能設定 (每日營業時間，單位：小時)
# 用於計算每日最大可容納秒數
PLATFORM_CAPACITY = {
    "新鮮視全省": 18,      # 每日營業 18 小時
    "新鮮視北北基": 18,
    "新鮮視中彰投": 18,
    "全家廣播": 18,
    # 預設值：18 小時
}

DB_FILE = "inventory_data.db"

# ==========================================
# 2. 核心邏輯區 (Core Logic)
# ==========================================

def get_db_connection():
    """取得資料庫連線"""
    conn = sqlite3.connect(DB_FILE)
    return conn


# --- 表3 與整頁重跑加速：依 DB 修改時間快取讀取與重計算 ---
@st.cache_data(ttl=120)
def _load_orders_cached(db_mtime):
    """依 DB 檔案修改時間快取 orders 讀取，DB 更新後自動失效。"""
    from services_cache import load_orders_cached

    return load_orders_cached(get_db_connection=get_db_connection, db_mtime=db_mtime)


@st.cache_data(ttl=120)
def _load_segments_cached(db_mtime):
    """依 DB 檔案修改時間快取 ad_flight_segments 讀取，DB 更新後自動失效。"""
    from services_cache import load_segments_cached

    return load_segments_cached(get_db_connection=get_db_connection, db_mtime=db_mtime)


@st.cache_data(ttl=120)
def _explode_segments_to_daily_cached(df_segments):
    """快取 explode_segments_to_daily，相同 segments 不重算。"""
    from services_cache import explode_segments_to_daily_cached

    return explode_segments_to_daily_cached(explode_segments_to_daily=explode_segments_to_daily, df_segments=df_segments)


@st.cache_data(ttl=120)
def _build_table3_monthly_control_cached(db_mtime, year, month, monthly_capacity_tuple):
    """快取表3 建表結果，以 db_mtime+年月+容量為鍵，不 hash 大 DataFrame，換月才約 1 秒內。"""
    from services_cache import build_table3_monthly_control_cached

    return build_table3_monthly_control_cached(
        load_segments_cached_fn=_load_segments_cached,
        explode_segments_to_daily_cached_fn=_explode_segments_to_daily_cached,
        build_table3_monthly_control=build_table3_monthly_control,
        db_mtime=db_mtime,
        year=year,
        month=month,
        monthly_capacity_tuple=monthly_capacity_tuple,
    )

def init_db():
    from services_db import init_db as _init_db

    return _init_db(
        get_db_connection=get_db_connection,
        hash_password=_hash_password,
    )


def _sync_sheets_if_enabled(only_tables=None, skip_if_unchanged=True):
    """若已設定 Google Sheet 後端，將目前 DB 同步至試算表。回傳錯誤列表（空表示成功）。"""
    try:
        from sheets_backend import is_sheets_enabled, sync_db_to_sheets
        if is_sheets_enabled():
            return sync_db_to_sheets(
                get_db_connection,
                only_tables=only_tables,
                skip_if_unchanged=skip_if_unchanged,
            )
    except Exception:
        pass
    return []


# --- 登入與權限管理 ---
ROLES = ["行政主管", "業務", "總經理"]
SALT = "secmanager_2026"

def _hash_password(password):
    return hashlib.sha256((SALT + password).encode()).hexdigest()

def auth_verify(username, password):
    """驗證帳密，成功回傳 dict {username, role}，失敗回傳 None"""
    from services_auth import auth_verify as _auth_verify

    return _auth_verify(
        get_db_connection=get_db_connection,
        hash_password=_hash_password,
        username=username.strip(),
        password=password,
    )

def auth_list_users():
    """列出所有帳號（不含密碼）"""
    from services_auth import auth_list_users as _auth_list_users

    return _auth_list_users(get_db_connection=get_db_connection)

def auth_create_user(username, password, role):
    """新增帳號，回傳 (success: bool, message: str)"""
    from services_auth import auth_create_user as _auth_create_user

    u = str(username).strip()
    if role not in ROLES:
        return False, "無效的權限"
    ok, msg = _auth_create_user(
        get_db_connection=get_db_connection,
        hash_password=_hash_password,
        username=u,
        password=password,
        role=role,
    )
    if ok:
        _sync_sheets_if_enabled()
    return ok, msg

def auth_delete_user(username):
    """刪除帳號"""
    from services_auth import auth_delete_user as _auth_delete_user

    _auth_delete_user(get_db_connection=get_db_connection, username=str(username).strip())
    _sync_sheets_if_enabled()

def auth_change_password(username, new_password):
    """變更密碼"""
    if not new_password:
        return False, "密碼不可為空"
    from services_auth import auth_change_password as _auth_change_password

    _auth_change_password(
        get_db_connection=get_db_connection,
        hash_password=_hash_password,
        username=str(username).strip(),
        new_password=new_password,
    )
    _sync_sheets_if_enabled()
    return True

def get_platform_monthly_purchase(media_platform, year, month):
    """取得某媒體某年某月的購買秒數與購買價格，回傳 (purchased_seconds, purchase_price) 或 None"""
    from services_platform import get_platform_monthly_purchase as _get_platform_monthly_purchase

    return _get_platform_monthly_purchase(
        get_db_connection=get_db_connection,
        media_platform=media_platform,
        year=year,
        month=month,
    )

def set_platform_monthly_purchase(media_platform, year, month, purchased_seconds, purchase_price):
    """設定某媒體某年某月的購買秒數與購買價格；並同步更新 platform_monthly_capacity（每日可用 = 購買秒數/當月天數）"""
    from services_platform import set_platform_monthly_purchase as _set_platform_monthly_purchase

    _set_platform_monthly_purchase(
        get_db_connection=get_db_connection,
        sync_sheets_if_enabled=_sync_sheets_if_enabled,
        media_platform=media_platform,
        year=year,
        month=month,
        purchased_seconds=purchased_seconds,
        purchase_price=purchase_price,
    )

def load_platform_monthly_purchase_for_year(media_platform, year):
    """載入某媒體某年 1~12 月的購買資料，回傳 dict: month -> (purchased_seconds, purchase_price)"""
    from services_platform import load_platform_monthly_purchase_for_year as _load_platform_monthly_purchase_for_year

    return _load_platform_monthly_purchase_for_year(
        get_db_connection=get_db_connection,
        media_platform=media_platform,
        year=year,
    )

def load_platform_monthly_purchase_all_media_for_year(year):
    """載入某年所有媒體 1~12 月購買資料，回傳 dict: media_platform -> { month -> (purchased_seconds, purchase_price) }"""
    from services_platform import load_platform_monthly_purchase_all_media_for_year as _load_platform_monthly_purchase_all_media_for_year

    return _load_platform_monthly_purchase_all_media_for_year(
        get_db_connection=get_db_connection,
        year=year,
    )


def load_platform_settings():
    """從資料庫載入平台設定（優先使用資料庫中的設定）"""
    from services_platform import load_platform_settings as _load_platform_settings

    return _load_platform_settings(get_db_connection=get_db_connection)

def get_platform_monthly_capacity(media_platform, year, month):
    """取得某媒體、某年某月的「當月每日可用秒數」（向全家/家樂福等購買的每日秒數），無設定則回傳 None"""
    from services_platform import get_platform_monthly_capacity as _get_platform_monthly_capacity

    return _get_platform_monthly_capacity(
        get_db_connection=get_db_connection,
        media_platform=media_platform,
        year=year,
        month=month,
    )

def set_platform_monthly_capacity(media_platform, year, month, daily_available_seconds):
    """設定某媒體、某年某月的當月每日可用秒數"""
    from services_platform import set_platform_monthly_capacity as _set_platform_monthly_capacity

    _set_platform_monthly_capacity(
        get_db_connection=get_db_connection,
        sync_sheets_if_enabled=_sync_sheets_if_enabled,
        media_platform=media_platform,
        year=year,
        month=month,
        daily_available_seconds=daily_available_seconds,
    )

def load_platform_monthly_capacity_for(year, month):
    """載入某年某月所有媒體的每日可用秒數設定，回傳 dict: media_platform -> daily_available_seconds"""
    from services_platform import load_platform_monthly_capacity_for as _load_platform_monthly_capacity_for

    return _load_platform_monthly_capacity_for(
        get_db_connection=get_db_connection,
        year=year,
        month=month,
    )

def save_platform_settings(platform, store_count, daily_hours):
    """儲存平台設定到資料庫"""
    from services_platform import save_platform_settings as _save_platform_settings

    _save_platform_settings(
        get_db_connection=get_db_connection,
        sync_sheets_if_enabled=_sync_sheets_if_enabled,
        platform=platform,
        store_count=store_count,
        daily_hours=daily_hours,
    )

def parse_platform_region(raw_platform):
    """
    將原始平台名稱拆解為 (platform, channel, region)
    例如：'新鮮視全省' → ('全家', '新鮮視', '全省')
    """
    from services_media_platform import parse_platform_region as _parse_platform_region

    return _parse_platform_region(raw_platform)

# 表一「媒體平台」顯示名稱：全家廣播(企頻)、全家新鮮視、家樂福超市、家樂福量販店
MEDIA_PLATFORM_OPTIONS = ['全家廣播(企頻)', '全家新鮮視', '家樂福超市', '家樂福量販店']

def get_media_platform_display(platform, channel, raw_platform=''):
    """
    依 platform / channel / 原始平台名稱 回傳表一用「媒體平台」顯示名稱。
    回傳值為 MEDIA_PLATFORM_OPTIONS 之一或 '其他'。
    """
    from services_media_platform import get_media_platform_display as _get_media_platform_display

    return _get_media_platform_display(platform, channel, raw_platform)


def should_multiply_store_count(media_platform: str) -> bool:
    """
    使用秒數計算規則（重要）：
    - 全家廣播(企頻)、全家新鮮視：使用店秒 = 檔次 × 秒數 × 店數
    - 其他（如 家樂福超市/量販店、診所/門診等）：使用秒數 = 檔次 × 秒數（不乘店數）

    注意：系統內部仍沿用欄名「使用店秒」，但在不乘店數的平台其意義等同「使用秒數」。
    """
    mp = (media_platform or '').strip()
    return mp in ('全家廣播(企頻)', '全家新鮮視')

def get_store_count(platform, custom_settings=None):
    """取得平台店數（優先使用自訂設定，其次平台鍵，再依區域對照，最後預設 1）"""
    if custom_settings and platform in custom_settings:
        return custom_settings[platform]['store_count']
    if platform in STORE_COUNTS:
        return STORE_COUNTS[platform]
    # 依區域對照：新鮮視/企頻 等「平台名含區域」皆可由此取得店數
    try:
        _, _, region = parse_platform_region(platform)
        if region and region != '未知' and region in REGION_STORE_COUNTS:
            return REGION_STORE_COUNTS[region]
    except Exception:
        pass
    # 家樂福超市/家樂福量販店 等未列在 STORE_COUNTS 時，fallback 至家樂福
    if platform and '家樂福' in str(platform):
        return STORE_COUNTS.get('家樂福', 1)
    return 1

def get_daily_capacity(platform, custom_settings=None):
    """計算平台每日最大容量（店數 × 每日小時數 × 3600秒）"""
    store_count = get_store_count(platform, custom_settings)
    
    # 取得每日營業小時數
    if custom_settings and platform in custom_settings:
        daily_hours = custom_settings[platform]['daily_hours']
    else:
        daily_hours = PLATFORM_CAPACITY.get(platform, 18)
    
    # 計算每日最大秒數容量
    return store_count * daily_hours * 3600


def df_to_excel_bytes(df, sheet_name="Sheet1"):
    """將 DataFrame 轉為 Excel (.xlsx) 的 bytes。"""
    return _df_to_excel_bytes(df, sheet_name=sheet_name)


def sanitize_dataframe_for_display(df):
    """清理 DataFrame 供 st.dataframe 顯示（修復 PyArrow 錯誤）。"""
    return _sanitize_dataframe_for_display(df)


def _styler_one_decimal(df):
    """各分頁表格用：數值欄位顯示最多小數一位、千分位。"""
    return _styler_one_decimal_impl(df)


def _display_monthly_table_split(df, month_cols, style_func=None, height=None, key_prefix=""):
    """
    將包含 12 個月欄位的表格拆分成上下半年兩個表格顯示，避免左右滑動。
    將 12 個月分成 2 組：上半年（1-6月）、下半年（7-12月），垂直排列顯示。
    
    參數:
        df: DataFrame，必須包含 month_cols 中的欄位
        month_cols: 月份欄位列表，例如 ['1月', '2月', ..., '12月']
        style_func: 可選的樣式函數，接受 DataFrame 並回傳 Styler
        height: 可選的表格高度
        key_prefix: 用於生成唯一 key 的前綴
    """
    if df.empty or not month_cols:
        return
    
    # 將 12 個月分成 2 組：上半年和下半年
    groups = [
        (month_cols[0:6], "上半年（1月～6月）"),   # 1-6月
        (month_cols[6:12], "下半年（7月～12月）"),  # 7-12月
    ]
    
    # 取得非月份欄位（例如「項目」欄位）
    non_month_cols = [c for c in df.columns if c not in month_cols]
    
    # 垂直排列顯示兩個表格
    for idx, (group_months, label) in enumerate(groups):
        # 選取該組的欄位
        display_cols = non_month_cols + group_months
        df_subset = df[[c for c in display_cols if c in df.columns]].copy()
        
        if df_subset.empty:
            continue
        
        # 顯示標題
        st.markdown(f"**{label}**")
        
        # 套用樣式
        if style_func:
            styled_df = style_func(df_subset)
        else:
            styled_df = df_subset.style
        
        # 顯示表格（一個一列，垂直排列）
        st.dataframe(
            styled_df,
            use_container_width=True,
            height=height,
            key=f"{key_prefix}_split_{idx}"
        )


# CUE/Excel 解析已下沉至 services_cue_parser.py

def build_table1_from_cue_excel(cue_data_list, custom_settings=None):
    return service_build_table1_from_cue_excel(
        cue_data_list=cue_data_list,
        custom_settings=custom_settings,
        parse_platform_region_fn=parse_platform_region,
        get_media_platform_display_fn=get_media_platform_display,
        get_store_count_fn=get_store_count,
        should_multiply_store_count_fn=should_multiply_store_count,
    )

# 常數（訂單 CRUD、匯入與總結表用，對齊 Cue 表規格）
MOCK_REGIONS = ["北區", "桃竹苗", "中區", "雲嘉南", "高屏", "東區", "全省"]
MOCK_PLATFORM_RAW = [
    "新鮮視全省", "新鮮視北北基", "新鮮視中彰投", "新鮮視桃竹苗", "新鮮視雲嘉南", "新鮮視高高屏", "新鮮視宜花東",
    "企頻全省", "企頻北北基", "企頻中彰投", "企頻桃竹苗", "企頻雲嘉南", "企頻高高屏", "企頻宜花東",
    "全家廣播", "全家廣播北北基", "全家廣播中彰投", "全家廣播桃竹苗",
    "家樂福", "家樂福全省", "家樂福超市", "家樂福量販店",
]
MOCK_SECONDS = [5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60]
MOCK_CLIENTS = ["統一企業", "富邦投信", "國泰人壽", "台灣大哥大", "遠傳電信", "中華電信", "可口可樂", "味全", "桂格", "黑松", "義美", "光泉", "大潤發", "全聯", "家樂福", "PChome", "momo", "玉山銀行", "中信金", "台新金"]
MOCK_PRODUCTS = ["春節檔期", "中秋促銷", "年貨大街", "週年慶", "品牌形象", "新品上市", "促銷活動", "認簽專案", "聯播方案", "區域方案", "30秒廣告", "15秒廣告", "10秒廣告"]
MOCK_SALES = ["王小明", "李小華", "張小美", "陳小傑", "林小芳", "黃小偉", "劉小玲"]
# 公司別：東吳、聲活、鉑霖（三家分公司）
MOCK_COMPANY = ["東吳", "聲活", "鉑霖"]


def _normalize_seconds_type(val):
    """將秒數用途正規化為 SECONDS_USAGE_TYPES 其一。"""
    return _normalize_seconds_type_impl(val)


def import_ragic_to_orders_by_date_range(
    ragic_url: str,
    api_key: str,
    date_from: date,
    date_to: date,
    date_field: str = "建立日期",
    replace_existing: bool = False,
    max_fetch: int = 5000,
):
    from services_ragic_import import import_ragic_to_orders_by_date_range_service

    return import_ragic_to_orders_by_date_range_service(
        ragic_url=ragic_url,
        api_key=api_key,
        date_from=date_from,
        date_to=date_to,
        date_field=date_field,
        replace_existing=replace_existing,
        max_fetch=max_fetch,
        ragic_fields=RAGIC_FIELDS,
        parse_cue_excel_for_table1=parse_cue_excel_for_table1,
        get_db_connection=get_db_connection,
        init_db=init_db,
        build_ad_flight_segments=build_ad_flight_segments,
        load_platform_settings=load_platform_settings,
        compute_and_save_split_amount_for_contract=_compute_and_save_split_amount_for_contract,
        sync_sheets_if_enabled=_sync_sheets_if_enabled,
        normalize_date=_normalize_date,
    )


def get_ragic_import_logs(limit=1000):
    """讀取 Ragic 匯入紀錄（最新在前）。"""
    from services_ragic_import import get_ragic_import_logs_service

    return get_ragic_import_logs_service(
        limit=limit,
        init_db=init_db,
        get_db_connection=get_db_connection,
    )


def _normalize_date(val):
    """將 2026/1/1、2026-01-01 等轉成 YYYY-MM-DD。"""
    return _normalize_date_impl(val)


def import_google_sheet_to_orders(url_or_id, replace_existing=True):
    """
    從 Google 試算表（表1結構）匯入至 orders，並建立 ad_flight_segments。
    url_or_id: 試算表完整網址或 Sheet ID。
    replace_existing: True 則先清空 orders 再匯入；False 則追加。
    回傳 (success: bool, message: str)
    """
    from services_google_import import import_google_sheet_to_orders_service

    return import_google_sheet_to_orders_service(
        url_or_id=url_or_id,
        replace_existing=replace_existing,
        normalize_seconds_type=_normalize_seconds_type,
        init_db=init_db,
        get_db_connection=get_db_connection,
        load_platform_settings=load_platform_settings,
        build_ad_flight_segments=build_ad_flight_segments,
        compute_and_save_split_amount_for_contract=_compute_and_save_split_amount_for_contract,
        sync_sheets_if_enabled=_sync_sheets_if_enabled,
    )


def build_ad_flight_segments(df_orders, custom_settings=None, write_to_db=True, sync_sheets=True):
    return service_build_ad_flight_segments(
        df_orders=df_orders,
        custom_settings=custom_settings,
        write_to_db=write_to_db,
        sync_sheets=sync_sheets,
        parse_platform_region_fn=parse_platform_region,
        get_media_platform_display_fn=get_media_platform_display,
        get_store_count_fn=get_store_count,
        should_multiply_store_count_fn=should_multiply_store_count,
        normalize_seconds_type_fn=_normalize_seconds_type,
        get_db_connection_fn=get_db_connection,
        sync_sheets_if_enabled_fn=_sync_sheets_if_enabled,
    )

def _resolve_media_platform_for_daily(seg):
    from services_segments import resolve_media_platform_for_daily

    return resolve_media_platform_for_daily(seg, get_media_platform_display_fn=get_media_platform_display)

def explode_segments_to_daily(df_segments):
    return service_explode_segments_to_daily(
        df_segments=df_segments,
        get_media_platform_display_fn=get_media_platform_display,
        normalize_seconds_type_fn=_normalize_seconds_type,
    )

def build_table2_summary_by_company(df_segments, df_daily, df_orders, media_platform=None):
    return service_build_table2_summary_by_company(
        df_segments=df_segments,
        df_daily=df_daily,
        df_orders=df_orders,
        get_media_platform_display_fn=get_media_platform_display,
        media_platform=media_platform,
    )

def build_table2_details_by_company(df_segments, df_daily, df_orders):
    return service_build_table2_details_by_company(
        df_segments=df_segments,
        df_daily=df_daily,
        df_orders=df_orders,
    )

def build_table3_monthly_control(df_daily, df_segments, custom_settings=None, year=None, month=None, monthly_capacity=None):
    return service_build_table3_monthly_control(
        df_daily=df_daily,
        df_segments=df_segments,
        media_platform_options=MEDIA_PLATFORM_OPTIONS,
        get_media_platform_display_fn=get_media_platform_display,
        year=year,
        month=month,
        monthly_capacity=monthly_capacity,
    )


# 年度使用秒數總表：實體對應（企頻、新鮮視、家樂福、診所）
ANNUAL_SUMMARY_ENTITY_LABELS = ['企頻', '新鮮視', '家樂福', '診所']
ANNUAL_SUMMARY_MEDIA_MAP = {
    '企頻': ['全家廣播(企頻)'],
    '新鮮視': ['全家新鮮視'],
    '家樂福': ['家樂福超市', '家樂福量販店'],
    '診所': [],  # 無對應平台則顯示 0
}

# ========== 實驗分頁：依時間的庫存警示與分析 ==========
# 【核心假設】當月秒數若未使用於月底結算視為 100% 浪費（不可逆）；秒數價值隨接近月底而衰減；目標為最小化月底浪費；爆量仍監控但屬次要。
EMERGENCY_DAYS = 7  # T0 緊急期天數（today ~ today+N 為唯一可補救窗口）
TIME_WEIGHT = {"past": 1.0, "emergency": 0.9, "buffer": 0.3}
TARGET_USAGE = 0.8
TOLERANCE = 0.2
SAFE_LIMIT = 0.95
OVER_BUFFER = 0.1
# 「約 X 檔全省 15 秒」換算：1 檔 = 15 秒 × 全省店數（店秒）
SECONDS_PER_SPOT_15S = 15 * 4200  # 全省約 4200 店


def build_daily_inventory_and_metrics(df_daily, year, month, today, emergency_days=EMERGENCY_DAYS, monthly_capacity_loader=None, media_platform=None):
    if not monthly_capacity_loader:
        monthly_capacity_loader = lambda mp, yr, mo: get_platform_monthly_capacity(mp, yr, mo)
    return service_build_daily_inventory_and_metrics(
        df_daily=df_daily,
        year=year,
        month=month,
        today=today,
        emergency_days=emergency_days,
        monthly_capacity_loader=monthly_capacity_loader,
        media_platform_options=MEDIA_PLATFORM_OPTIONS,
        time_weight=TIME_WEIGHT,
        target_usage=TARGET_USAGE,
        tolerance=TOLERANCE,
        safe_limit=SAFE_LIMIT,
        over_buffer=OVER_BUFFER,
        media_platform=media_platform,
    )


def _seconds_to_spot_label(seconds, sec_per_spot=SECONDS_PER_SPOT_15S, short=False):
    """轉譯為「約 X 檔全省 15 秒」；short=True 為「約 X 檔(15秒)」。"""
    return _seconds_to_spot_label_impl(seconds, sec_per_spot=sec_per_spot, short=short)


# ========== ROI 分頁：依現有資料計算投報率（不寫入資料庫）==========
# 成本：來自「媒體秒數與採購」分頁（購買價格）
# 實收：來自表1 訂單（依各媒體使用秒數比例拆分，或使用拆分金額）
# ROI = (實收 - 購買成本) / 購買成本
SYSTEM_MEDIA_COST_PER_SECOND = {}


def _compute_and_save_split_amount_for_contract(contract_key):
    from services_roi import compute_and_save_split_amount_for_contract

    return compute_and_save_split_amount_for_contract(
        contract_key=contract_key,
        get_db_connection=get_db_connection,
        sync_sheets_if_enabled=_sync_sheets_if_enabled,
    )


def get_revenue_per_media_by_period(period_type, year, month=None):
    from services_roi import get_revenue_per_media_by_period as _get_revenue_per_media_by_period

    return _get_revenue_per_media_by_period(
        period_type=period_type,
        year=year,
        month=month,
        get_db_connection=get_db_connection,
    )


def get_cost_per_media_by_period(period_type, year, month=None):
    from services_roi import get_cost_per_media_by_period as _get_cost_per_media_by_period

    return _get_cost_per_media_by_period(
        period_type=period_type,
        year=year,
        month=month,
        get_db_connection=get_db_connection,
    )


def _get_roi_all_period_date_range():
    """
    取得「累計至今」的實際統計日期範圍。
    回傳 (start_str, end_str) 如 ("2024/1/1", "2026/12/31")，無資料則回傳 (None, None)
    """
    from services_roi import get_roi_all_period_date_range

    return get_roi_all_period_date_range(
        get_db_connection=get_db_connection,
    )


def _calculate_roi_by_period(period_type, year, month, period_label):
    """
    依時間維度計算各媒體 ROI。
    period_type: 'month' | 'quarter' | 'year' | 'all'
    period_label: 顯示用標籤，如 "2026年1月"、"2026 Q1"、"2026年"、"累計至今"
    回傳 list of dict
    """
    from services_roi import calculate_roi_by_period

    return calculate_roi_by_period(
        period_type=period_type,
        year=year,
        month=month,
        period_label=period_label,
        media_platform_options=MEDIA_PLATFORM_OPTIONS,
        get_revenue_per_media_by_period=get_revenue_per_media_by_period,
        get_cost_per_media_by_period=get_cost_per_media_by_period,
    )


def build_annual_seconds_summary(df_daily, year, monthly_capacity_loader=None):
    return service_build_annual_seconds_summary(
        df_daily=df_daily,
        year=year,
        monthly_capacity_loader=monthly_capacity_loader,
        annual_summary_entity_labels=ANNUAL_SUMMARY_ENTITY_LABELS,
        annual_summary_media_map=ANNUAL_SUMMARY_MEDIA_MAP,
        seconds_usage_types=SECONDS_USAGE_TYPES,
    )


def _build_visualization_summary_excel(annual_viz, summary_year):
    return service_build_visualization_summary_excel(
        annual_viz=annual_viz,
        summary_year=summary_year,
        annual_summary_entity_labels=ANNUAL_SUMMARY_ENTITY_LABELS,
    )

def _build_visualization_summary_pdf(annual_viz, summary_year):
    return service_build_visualization_summary_pdf(
        annual_viz=annual_viz,
        summary_year=summary_year,
        annual_summary_entity_labels=ANNUAL_SUMMARY_ENTITY_LABELS,
    )

def _build_table1_from_segments(df_segments: pd.DataFrame, custom_settings=None, df_orders_info=None) -> pd.DataFrame:
    return service_build_table1_from_segments(
        df_segments=df_segments,
        custom_settings=custom_settings,
        df_orders_info=df_orders_info,
        get_db_connection_fn=get_db_connection,
        get_media_platform_display_fn=get_media_platform_display,
    )

def build_excel_table1_view(df_orders: pd.DataFrame, custom_settings=None, use_segments=True, df_segments=None) -> pd.DataFrame:
    return service_build_excel_table1_view(
        df_orders=df_orders,
        custom_settings=custom_settings,
        use_segments=use_segments,
        df_segments=df_segments,
        build_table1_from_segments_fn=_build_table1_from_segments,
        get_db_connection_fn=get_db_connection,
        parse_platform_region_fn=parse_platform_region,
        get_media_platform_display_fn=get_media_platform_display,
        get_store_count_fn=get_store_count,
    )

# ==========================================
# 3. 介面呈現區 (Streamlit UI)
# ==========================================

st.set_page_config(layout="wide", page_title="秒數控管系統", page_icon="📊")

@st.fragment
def _render_tab3():
    from ui_tab_table3 import render_table3_tab

    render_table3_tab(
        db_mtime=st.session_state.get("_db_mtime", 0),
        load_segments_cached=_load_segments_cached,
        explode_segments_to_daily_cached=_explode_segments_to_daily_cached,
        load_platform_monthly_capacity_for=load_platform_monthly_capacity_for,
        build_table3_monthly_control_cached=_build_table3_monthly_control_cached,
        media_platform_options=MEDIA_PLATFORM_OPTIONS,
        styler_one_decimal=_styler_one_decimal,
    )


from app_shell import run_app_shell
from ui_main_tabs import render_main_tabs

parse_cueapp_excel = service_parse_cueapp_excel
parse_excel_daily_ads = service_parse_excel_daily_ads
parse_cue_excel_for_table1 = service_parse_cue_excel_for_table1

run_app_shell(
    init_db=init_db,
    get_db_connection=get_db_connection,
    auth_verify=auth_verify,
    auth_change_password=auth_change_password,
    auth_list_users=auth_list_users,
    auth_create_user=auth_create_user,
    auth_delete_user=auth_delete_user,
    sync_sheets_if_enabled=_sync_sheets_if_enabled,
    import_google_sheet_to_orders=import_google_sheet_to_orders,
    import_ragic_to_orders_by_date_range=import_ragic_to_orders_by_date_range,
    load_platform_settings=load_platform_settings,
    load_orders_cached=_load_orders_cached,
    load_segments_cached=_load_segments_cached,
    explode_segments_to_daily_cached=_explode_segments_to_daily_cached,
    build_ad_flight_segments=build_ad_flight_segments,
    render_tab3=_render_tab3,
    render_main_tabs=render_main_tabs,
    roles=ROLES,
    db_file=DB_FILE,
    platform_capacity=PLATFORM_CAPACITY,
    get_store_count=get_store_count,
    save_platform_settings=save_platform_settings,
    media_platform_options=MEDIA_PLATFORM_OPTIONS,
    annual_summary_entity_labels=ANNUAL_SUMMARY_ENTITY_LABELS,
    emergency_days=EMERGENCY_DAYS,
    seconds_per_spot_15s=SECONDS_PER_SPOT_15S,
    ragic_fields=RAGIC_FIELDS,
    ragic_subtable_fields=RAGIC_SUBTABLE_FIELDS,
    compute_split_for_contract=_compute_and_save_split_amount_for_contract,
    styler_one_decimal=_styler_one_decimal,
    df_to_excel_bytes=df_to_excel_bytes,
    build_excel_table1_view=build_excel_table1_view,
    build_table2_summary_by_company=build_table2_summary_by_company,
    build_table2_details_by_company=build_table2_details_by_company,
    get_platform_monthly_capacity=get_platform_monthly_capacity,
    build_annual_seconds_summary=build_annual_seconds_summary,
    display_monthly_table_split=_display_monthly_table_split,
    build_visualization_summary_pdf=_build_visualization_summary_pdf,
    build_visualization_summary_excel=_build_visualization_summary_excel,
    load_platform_monthly_purchase_all_media_for_year=load_platform_monthly_purchase_all_media_for_year,
    set_platform_monthly_purchase=set_platform_monthly_purchase,
    get_ragic_import_logs=get_ragic_import_logs,
    parse_cue_excel_for_table1=parse_cue_excel_for_table1,
    build_daily_inventory_and_metrics=build_daily_inventory_and_metrics,
    seconds_to_spot_label=_seconds_to_spot_label,
    calculate_roi_by_period=_calculate_roi_by_period,
    get_roi_all_period_date_range=_get_roi_all_period_date_range,
    mock_platform_raw=MOCK_PLATFORM_RAW,
    mock_sales=MOCK_SALES,
    mock_company=MOCK_COMPANY,
    mock_seconds=MOCK_SECONDS,
    seconds_usage_types=SECONDS_USAGE_TYPES,
)
