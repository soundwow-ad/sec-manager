# -*- coding: utf-8 -*-
"""快取讀取服務。"""

from __future__ import annotations

import pandas as pd


def load_orders_cached(*, get_db_connection, db_mtime):
    conn = get_db_connection()
    df = pd.read_sql("SELECT * FROM orders", conn)
    conn.close()
    return df


def load_segments_cached(*, get_db_connection, db_mtime):
    conn = get_db_connection()
    try:
        df = pd.read_sql("SELECT * FROM ad_flight_segments", conn)
    except Exception:
        df = pd.DataFrame()
    conn.close()
    return df


def explode_segments_to_daily_cached(*, explode_segments_to_daily, df_segments):
    if df_segments.empty:
        return pd.DataFrame()
    return explode_segments_to_daily(df_segments)


def build_table3_monthly_control_cached(
    *,
    load_segments_cached_fn,
    explode_segments_to_daily_cached_fn,
    build_table3_monthly_control,
    db_mtime,
    year,
    month,
    monthly_capacity_tuple,
):
    df_seg = load_segments_cached_fn(db_mtime)
    df_daily = explode_segments_to_daily_cached_fn(df_seg) if not df_seg.empty else pd.DataFrame()
    if df_daily.empty or df_seg.empty:
        return {}
    cap = dict(monthly_capacity_tuple) if monthly_capacity_tuple else None
    return build_table3_monthly_control(df_daily, df_seg, None, year, month, cap)

