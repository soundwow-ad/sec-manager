# -*- coding: utf-8 -*-
"""表1 分頁 UI 模組。"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Callable, Sequence

import numpy as np
import pandas as pd
import streamlit as st
import time

from services_utils import log_timing

from ui_order_crud import render_order_crud_panel


def render_table1_tab(
    *,
    db_mtime: int,
    df_orders: pd.DataFrame,
    df_seg_main: pd.DataFrame,
    custom_settings: dict,
    role: str,
    media_platform_options: Sequence[str],
    build_excel_table1_view: Callable[..., pd.DataFrame],
    styler_one_decimal: Callable[[pd.DataFrame], object],
    df_to_excel_bytes: Callable[..., bytes],
    get_db_connection: Callable[[], object],
    load_platform_settings: Callable[[], dict],
    build_ad_flight_segments: Callable[..., pd.DataFrame],
    compute_split_for_contract: Callable[[str], None],
    sync_sheets_if_enabled: Callable[..., None],
    mock_platform_raw: Sequence[str],
    mock_sales: Sequence[str],
    mock_company: Sequence[str],
    mock_seconds: Sequence[int],
    seconds_usage_types: Sequence[str],
) -> None:
    st.markdown("### 📋 表1－資料（訂單主表）")
    st.caption("此表對應 Excel：秒數管理表 → 表1-資料，為行政與業務對帳用之訂單主表。")

    # 表1大表若依賴 segments 會牽涉到大量資料與運算；訂單逐筆管理其實可不依賴 segments。
    # 預設採「不使用 segments」以確保啟動/切換分頁速度；需要更完整細節再手動切換。
    use_segments = st.checkbox(
        "使用檔次段（Segments，較慢）",
        value=False,
        key="table1_use_segments",
    )

    table1_default_index = 0  # 為了速度：預設先不產生日/日期欄位（更接近 1s 體感）
    view_mode = st.radio(
        "顯示模式",
        ["精簡", "行政", "完整"],
        format_func=lambda x: {
            "精簡": "① 精簡（業務：合約/客戶/平台/秒數/檔次/起訖/使用秒數）",
            "行政": "② 行政（+ 日期欄位、店數、委刊總檔數）",
            "完整": "③ 完整（全部欄位）",
        }[x],
        index=table1_default_index,
        horizontal=True,
        key="table1_view_mode",
    )
    include_daily_columns = view_mode != "精簡"

    cache_key = (db_mtime, use_segments, view_mode)

    if st.session_state.get("_table1_cache_key") == cache_key and "_table1_cache" in st.session_state:
        df_table1 = st.session_state["_table1_cache"]
    else:
        t0 = time.perf_counter()
        df_table1 = build_excel_table1_view(
            df_orders,
            custom_settings,
            use_segments=use_segments,
            df_segments=df_seg_main,
            include_daily_columns=include_daily_columns,
        )
        log_timing(
            "ui_table1.build_excel_table1_view",
            time.perf_counter() - t0,
            db_mtime=db_mtime,
            use_segments=use_segments,
            view_mode=view_mode,
            rows=len(df_table1) if isinstance(df_table1, pd.DataFrame) else 0,
            cols=len(df_table1.columns) if isinstance(df_table1, pd.DataFrame) else 0,
        )
        st.session_state["_table1_cache"] = df_table1
        st.session_state["_table1_cache_key"] = cache_key

    if df_table1.empty:
        st.warning("📭 尚無訂單資料")
        st.stop()

    if "實收金額" in df_table1.columns and "合約編號" in df_table1.columns:
        amount_display_mode = st.radio(
            "實收金額顯示",
            options=["依訂單列（每列顯示該筆訂單金額）", "依合約合併（每合約只顯示一筆總額於第一列）"],
            index=0,
            horizontal=True,
            key="table1_amount_display_mode",
        )
        if "依合約合併" in amount_display_mode:
            contract_total = df_table1.groupby("合約編號")["實收金額"].transform("sum")
            first_in_contract = ~df_table1.duplicated("合約編號", keep="first")
            df_table1 = df_table1.copy()
            df_table1["實收金額"] = np.where(first_in_contract, contract_total, 0)
            df_table1["除佣實收"] = df_table1["實收金額"]

    if "媒體平台" in df_table1.columns:
        st.markdown("#### 📺 媒體平台切換")
        platform_options = ["全部"] + [p for p in media_platform_options if p in df_table1["媒體平台"].unique().tolist()]
        if len(platform_options) == 1:
            platform_options = ["全部"] + list(media_platform_options)
        selected_platform = st.radio("選擇要顯示的媒體平台", options=platform_options, horizontal=True, key="table1_media_platform_filter")
        if selected_platform != "全部":
            df_table1 = df_table1[df_table1["媒體平台"] == selected_platform]
            if df_table1.empty:
                st.info(f"📭 媒體平台「{selected_platform}」目前沒有資料")
                st.stop()
    elif "平台分類" in df_table1.columns:
        st.markdown("#### 📺 平台篩選")
        platform_categories = ["全部", "全家新鮮視", "全家廣播", "家樂福", "診所", "其他"]
        selected_platform = st.radio("選擇要顯示的平台", options=platform_categories, horizontal=True, key="table1_platform_filter")
        if selected_platform != "全部":
            df_table1 = df_table1[df_table1["平台分類"] == selected_platform]
            if df_table1.empty:
                st.info(f"📭 平台「{selected_platform}」目前沒有資料")
                st.stop()

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("訂單筆數", len(df_table1))
    with col2:
        st.metric("客戶數", df_table1["客戶"].nunique() if "客戶" in df_table1.columns else (df_table1["HYUNDAI_CUSTIN"].nunique() if "HYUNDAI_CUSTIN" in df_table1.columns else 0))
    with col3:
        if "媒體平台" in df_table1.columns:
            st.metric("媒體平台數", df_table1["媒體平台"].nunique())
        elif "平台分類" in df_table1.columns:
            st.metric("平台數", df_table1["平台分類"].nunique())
        else:
            st.metric("平台數", df_table1["平台"].nunique() if "平台" in df_table1.columns else 0)
    with col4:
        total_amount = df_table1["實收金額"].sum() if "實收金額" in df_table1.columns else 0
        st.metric("實收金額總計", f"{total_amount:,}")

    with st.expander("🔍 篩選條件", expanded=False):
        c1, c2, c3 = st.columns(3)
        with c1:
            sel_company = st.selectbox("公司", ["全部"] + sorted(df_table1["公司"].unique().tolist())) if "公司" in df_table1.columns else "全部"
        with c2:
            sel_sales = st.selectbox("業務", ["全部"] + sorted(df_table1["業務"].unique().tolist())) if "業務" in df_table1.columns else "全部"
        with c3:
            client_col_filter = "客戶" if "客戶" in df_table1.columns else "HYUNDAI_CUSTIN"
            sel_client = (
                st.selectbox("客戶", ["全部"] + sorted(df_table1[client_col_filter].dropna().unique().astype(str).tolist()))
                if client_col_filter in df_table1.columns
                else "全部"
            )

    df_filtered = df_table1.copy()
    if sel_company != "全部" and "公司" in df_filtered.columns:
        df_filtered = df_filtered[df_filtered["公司"] == sel_company]
    if sel_sales != "全部" and "業務" in df_filtered.columns:
        df_filtered = df_filtered[df_filtered["業務"] == sel_sales]
    client_col_filter = "客戶" if "客戶" in df_table1.columns else "HYUNDAI_CUSTIN"
    if sel_client != "全部" and client_col_filter in df_filtered.columns:
        df_filtered = df_filtered[df_filtered[client_col_filter].astype(str) == sel_client]

    client_col = "客戶" if "客戶" in df_filtered.columns else "HYUNDAI_CUSTIN"
    cols_simple = [c for c in ["業務", "合約編號", client_col, "媒體平台", "秒數", "每天總檔次", "起始日", "終止日", "使用總秒數"] if c in df_filtered.columns]
    date_cols_t1 = [c for c in df_filtered.columns if re.match(r"^\d{1,2}/\d{1,2}\([一二三四五六日]\)$", str(c))]
    cols_admin = cols_simple + [c for c in ["店數", "委刊總檔數"] if c in df_filtered.columns] + date_cols_t1
    if view_mode == "精簡":
        show_cols = cols_simple
    elif view_mode == "行政":
        show_cols = [c for c in cols_admin if c in df_filtered.columns]
    else:
        show_cols = list(df_filtered.columns)
    df_display = df_filtered[[c for c in show_cols if c in df_filtered.columns]]

    st.markdown("#### 📊 表1-資料（可橫向滾動查看完整欄位）")
    st.dataframe(styler_one_decimal(df_display), use_container_width=True, height=650)
    st.info(
        "💡 **提示**：此表格較寬，請使用橫向滾動查看完整內容。\n"
        "- 每日24小時檔次分配欄位（6-23, 0-1點）目前為預留，未來可從 CUE 表取得詳細資料\n"
        "- 月份欄位顯示該月每天的檔次數\n"
        "- 星期序列顯示走期內每天的星期標記"
    )

    st.markdown("#### 📥 下載資料")
    st.info("💡 **提示**：建議下載 Excel 格式以避免編碼問題。下載內容依目前顯示模式（精簡/行政/完整）。")
    c1, c2 = st.columns(2)
    with c1:
        excel_bytes = df_to_excel_bytes(df_display, sheet_name="表1-資料")
        st.download_button(
            label="📥 下載 Excel（推薦）",
            data=excel_bytes,
            file_name=f"表1_資料_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    with c2:
        csv = df_display.to_csv(index=False, encoding="utf-8-sig")
        st.download_button(
            label="📥 下載 CSV（備選）",
            data=csv,
            file_name=f"表1_資料_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv; charset=utf-8",
        )

    if st.checkbox("顯示訂單逐筆管理（較慢）", value=False, key="table1_show_crud"):
        render_order_crud_panel(
            get_db_connection=get_db_connection,
            load_platform_settings=load_platform_settings,
            build_ad_flight_segments=build_ad_flight_segments,
            compute_split_for_contract=compute_split_for_contract,
            sync_sheets_if_enabled=sync_sheets_if_enabled,
            styler_one_decimal=styler_one_decimal,
            mock_platform_raw=mock_platform_raw,
            mock_sales=mock_sales,
            mock_company=mock_company,
            mock_seconds=mock_seconds,
            seconds_usage_types=seconds_usage_types,
        )
    else:
        st.caption("尚未開啟訂單逐筆管理：可提升表1顯示速度。")

