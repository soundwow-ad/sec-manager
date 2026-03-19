# -*- coding: utf-8 -*-
"""主畫面資料懶載入組裝。"""

from __future__ import annotations

import os
import time
import pandas as pd
import streamlit as st

from services_utils import log_timing

def load_runtime_data(
    *,
    db_file: str,
    load_platform_settings,
    load_orders_cached,
    load_segments_cached,
    explode_segments_to_daily_cached,
    build_ad_flight_segments,
):
    t_total = time.perf_counter()
    db_mtime = os.path.getmtime(db_file) if os.path.exists(db_file) else 0
    st.session_state["_db_mtime"] = db_mtime
    tab_hint = st.session_state.get("main_tab", "🧾 Ragic匯入紀錄")

    tabs_need_orders = {
        "📋 表1-資料",
        "📅 表2-秒數明細",
        "📉 總結表圖表",
        "📊 分公司×媒體 每月秒數",
        "📋 媒體秒數與採購",
        "📊 ROI",
        "🧪 實驗分頁",
    }
    tabs_need_segments = {
        "📅 表2-秒數明細",
        "📉 總結表圖表",
        "📊 分公司×媒體 每月秒數",
        "📊 ROI",
        "🧪 實驗分頁",
    }
    tabs_need_daily = {
        "📉 總結表圖表",
        "📊 分公司×媒體 每月秒數",
        "📊 ROI",
        "🧪 實驗分頁",
    }

    if tab_hint in tabs_need_orders:
        t0 = time.perf_counter()
        df_orders = load_orders_cached(db_mtime)
        log_timing(
            "runtime.load_orders_cached",
            time.perf_counter() - t0,
            tab_hint=tab_hint,
            db_mtime=db_mtime,
            orders_rows=len(df_orders) if isinstance(df_orders, pd.DataFrame) else 0,
        )
    else:
        df_orders = pd.DataFrame()
    if tab_hint in tabs_need_orders and df_orders.empty:
        st.warning("📭 資料庫為空，請由左側匯入試算表或新增訂單。")

    t0 = time.perf_counter()
    custom_settings = load_platform_settings()
    log_timing("runtime.load_platform_settings", time.perf_counter() - t0)

    df_seg_main = pd.DataFrame()
    df_daily = pd.DataFrame()
    if tab_hint in tabs_need_segments:
        t0 = time.perf_counter()
        df_seg_main = load_segments_cached(db_mtime)
        log_timing(
            "runtime.load_segments_cached",
            time.perf_counter() - t0,
            tab_hint=tab_hint,
            db_mtime=db_mtime,
            seg_rows=len(df_seg_main) if isinstance(df_seg_main, pd.DataFrame) else 0,
        )
        if tab_hint in tabs_need_daily:
            t0 = time.perf_counter()
            df_daily = explode_segments_to_daily_cached(df_seg_main) if not df_seg_main.empty else pd.DataFrame()
            log_timing(
                "runtime.explode_segments_to_daily_cached",
                time.perf_counter() - t0,
                tab_hint=tab_hint,
                daily_rows=len(df_daily) if isinstance(df_daily, pd.DataFrame) else 0,
            )
        if df_seg_main.empty and not df_orders.empty:
            with st.spinner("正在建立檔次段..."):
                t_build = time.perf_counter()
                build_ad_flight_segments(df_orders, custom_settings, write_to_db=True, sync_sheets=False)
                db_mtime = os.path.getmtime(db_file) if os.path.exists(db_file) else db_mtime
                st.session_state["_db_mtime"] = db_mtime
                t0 = time.perf_counter()
                df_seg_main = load_segments_cached(db_mtime)
                log_timing("runtime.build_ad_flight_segments", time.perf_counter() - t_build, db_mtime=db_mtime)
                log_timing("runtime.load_segments_cached_after_build", time.perf_counter() - t0, db_mtime=db_mtime)
                if tab_hint in tabs_need_daily:
                    t0 = time.perf_counter()
                    df_daily = explode_segments_to_daily_cached(df_seg_main) if not df_seg_main.empty else pd.DataFrame()
                    log_timing("runtime.explode_segments_to_daily_cached_after_build", time.perf_counter() - t0, db_mtime=db_mtime)

    log_timing(
        "runtime.load_runtime_data.total",
        time.perf_counter() - t_total,
        tab_hint=tab_hint,
        db_mtime=db_mtime,
    )
    return {
        "db_mtime": db_mtime,
        "custom_settings": custom_settings,
        "df_orders": df_orders,
        "df_seg_main": df_seg_main,
        "df_daily": df_daily,
    }

