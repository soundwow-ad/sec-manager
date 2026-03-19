# -*- coding: utf-8 -*-
"""Google Sheet 匯入服務層。"""

from __future__ import annotations

import io
import re
from datetime import datetime
from typing import Callable

import pandas as pd
import requests


def normalize_date(val) -> str:
    if pd.isna(val) or val == "" or val == "nan":
        return ""
    val = str(val).strip()
    if not val:
        return ""
    try:
        dt = pd.to_datetime(val, errors="coerce")
        if pd.isna(dt):
            return ""
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return ""


def extract_google_sheet_id(url_or_id):
    s = (url_or_id or "").strip()
    if not s:
        return None
    if "/" not in s and len(s) > 20:
        return s
    m = re.search(r"/d/([a-zA-Z0-9_-]{40,})", s)
    return m.group(1) if m else None


def fetch_google_sheet_as_dataframe(sheet_id, gid=0):
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        content = r.content
        if content.startswith(b"\xef\xbb\xbf"):
            content = content[3:]
        df_raw = pd.read_csv(io.BytesIO(content), encoding="utf-8", header=None, dtype=str)
    except Exception as e:
        return None, str(e)
    if df_raw.empty or len(df_raw) < 2:
        return None, "試算表為空或列數不足"
    header_row = None
    for i in range(min(10, len(df_raw))):
        row_str = " ".join(df_raw.iloc[i].astype(str).fillna(""))
        if "平台" in row_str and ("起始日" in row_str or "終止日" in row_str):
            header_row = i
            break
    if header_row is None:
        return None, "找不到表1結構的表頭列（需含：平台、起始日/終止日）"
    df = pd.read_csv(io.BytesIO(content), encoding="utf-8", header=header_row, dtype=str)
    df = df.dropna(how="all", axis=1).dropna(how="all", axis=0)
    df.columns = [str(c).strip() for c in df.columns]
    return df, None


def sheet_row_to_order(row, row_index, col_map, normalize_seconds_type: Callable[[str], str]):
    def get(k, default=""):
        key = col_map.get(k, k)
        if key not in row.index:
            return default
        v = row.get(key, default)
        return "" if pd.isna(v) or v == "nan" else str(v).strip()

    platform = get("platform") or get("平台")
    if not platform:
        return None
    start_date = normalize_date(get("start_date") or get("起始日"))
    end_date = normalize_date(get("end_date") or get("終止日"))
    if not start_date or not end_date:
        return None
    try:
        seconds = int(float(get("seconds") or get("秒數") or 0))
    except (ValueError, TypeError):
        seconds = 0
    try:
        spots = int(float(get("spots") or get("每天總檔次") or get("委刊總檔數") or get("委刋總檔數") or 0))
    except (ValueError, TypeError):
        spots = 0
    try:
        amount_net = float(get("amount_net") or get("實收金額") or 0)
    except (ValueError, TypeError):
        amount_net = 0
    client = get("client") or get("HYUNDAI_CUSTIN") or get("客戶")
    product = get("product") or get("素材")
    sales = get("sales") or get("業務")
    company = get("company") or get("公司")
    contract_id = get("contract_id") or get("合約編號")
    seconds_type = normalize_seconds_type(get("seconds_type") or get("秒數用途") or "銷售秒數")
    try:
        project_amount_net = float(get("project_amount_net") or get("專案實收金額") or 0)
    except (ValueError, TypeError):
        project_amount_net = 0
    if project_amount_net <= 0:
        project_amount_net = None
    updated_at = get("updated_at") or get("提交日") or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if not updated_at or updated_at == "":
        updated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    else:
        updated_at = normalize_date(updated_at)
        if not updated_at:
            updated_at = datetime.now().strftime("%Y-%m-%d")
        updated_at = updated_at + " 00:00:00" if len(updated_at) == 10 else updated_at
    order_id = f"gs_{row_index}_{contract_id or row_index}_{platform}_{start_date}".replace(" ", "_")[:200]
    return (
        order_id,
        platform,
        client or "",
        product or "",
        sales or "",
        company or "",
        start_date,
        end_date,
        seconds,
        spots,
        amount_net,
        updated_at,
        contract_id or None,
        seconds_type or "銷售秒數",
        project_amount_net,
    )


def import_google_sheet_to_orders_service(
    *,
    url_or_id: str,
    replace_existing: bool,
    normalize_seconds_type: Callable[[str], str],
    init_db: Callable[[], None],
    get_db_connection: Callable[[], object],
    load_platform_settings: Callable[[], dict],
    build_ad_flight_segments: Callable[..., object],
    compute_and_save_split_amount_for_contract: Callable[[str], None],
    sync_sheets_if_enabled: Callable[..., None],
) -> tuple[bool, str]:
    sheet_id = extract_google_sheet_id(url_or_id)
    if not sheet_id:
        return False, "請輸入有效的 Google 試算表網址或 ID"
    df, err = fetch_google_sheet_as_dataframe(sheet_id)
    if err:
        return False, f"無法讀取試算表：{err}"

    col_map = {
        "platform": "平台",
        "company": "公司",
        "sales": "業務",
        "contract_id": "合約編號",
        "client": "HYUNDAI_CUSTIN",
        "product": "素材",
        "start_date": "起始日",
        "end_date": "終止日",
        "seconds": "秒數",
        "spots": "每天總檔次",
        "amount_net": "實收金額",
        "seconds_type": "秒數用途",
        "updated_at": "提交日",
        "客戶": "HYUNDAI_CUSTIN",
        "委刊總檔數": "委刊總檔數",
        "委刋總檔數": "委刋總檔數",
        "project_amount_net": "專案實收金額",
        "專案實收金額": "專案實收金額",
    }
    orders = []
    for i, (_, row) in enumerate(df.iterrows()):
        t = sheet_row_to_order(row, i, col_map, normalize_seconds_type)
        if t is not None:
            orders.append(t)
    if not orders:
        return False, "沒有可匯入的資料列（需有平台、起始日、終止日且為有效日期）"

    init_db()
    conn = get_db_connection()
    c = conn.cursor()
    try:
        if replace_existing:
            c.execute("DELETE FROM orders")
        for t in orders:
            project_val = t[14] if len(t) > 14 else None
            c.execute(
                """
                INSERT OR REPLACE INTO orders
                (id, platform, client, product, sales, company, start_date, end_date, seconds, spots, amount_net, updated_at, contract_id, seconds_type, project_amount_net, split_amount)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
                (*t[:14], project_val, None),
            )
        conn.commit()
        conn.close()
        conn_read = get_db_connection()
        df_orders = pd.read_sql("SELECT * FROM orders", conn_read)
        conn_read.close()
        custom_settings = load_platform_settings()
        build_ad_flight_segments(df_orders, custom_settings, write_to_db=True, sync_sheets=False)
        contracts_with_project = (
            df_orders.loc[
                df_orders["project_amount_net"].notna() & (pd.to_numeric(df_orders["project_amount_net"], errors="coerce") > 0),
                "contract_id",
            ]
            .dropna()
            .unique()
        )
        for cid in contracts_with_project:
            if cid:
                compute_and_save_split_amount_for_contract(str(cid))
        sync_sheets_if_enabled(only_tables=["Orders", "Segments"], skip_if_unchanged=True)
        return True, f"已匯入 {len(orders)} 筆（表1結構）；若有專案實收金額已自動計算拆分金額）"
    except Exception as e:
        conn.rollback()
        conn.close()
        return False, str(e)

