# -*- coding: utf-8 -*-
"""表2 分頁 UI 模組。"""

from __future__ import annotations

from datetime import datetime
from io import BytesIO
from typing import Callable

import pandas as pd
import streamlit as st


def render_table2_tab(
    *,
    db_mtime: float,
    df_daily: pd.DataFrame,
    df_orders: pd.DataFrame,
    load_segments_cached: Callable[[float], pd.DataFrame],
    explode_segments_to_daily_cached_by_db_mtime: Callable[[int], pd.DataFrame],
    build_table2_summary_by_company: Callable[..., pd.DataFrame],
    build_table2_details_by_company: Callable[..., dict],
    styler_one_decimal: Callable[[pd.DataFrame], object],
) -> None:
    st.markdown("### 📅 表2－秒數明細（對齊 Excel 表2）")
    st.caption("依公司統計總覽、依業務統計明細（平台／合約／客戶／每日使用店秒），含小計。")

    df_seg_t2 = load_segments_cached(db_mtime)
    if df_seg_t2.empty:
        st.warning("📭 尚無檔次段資料，請先匯入或新增訂單。")
        return

    df_daily_t2 = df_daily if df_daily is not None and not df_daily.empty else pd.DataFrame()
    if df_daily_t2.empty:
        st.info("載入每日使用店秒中（表2需要）。")
        with st.spinner("正在計算每日資料..."):
            df_daily_t2 = explode_segments_to_daily_cached_by_db_mtime(int(db_mtime))

    if df_daily_t2.empty:
        st.warning("📭 尚無每日資料，請先匯入或新增訂單。")
        return

    # 先快顯：只算「依公司彙總」。明細改成按公司選擇後再計算。
    if st.session_state.get("_table2_cache_key") == db_mtime and "_table2_summary_fresh" in st.session_state and "_table2_summary_qi" in st.session_state:
        summary_t2_fresh = st.session_state.get("_table2_summary_fresh", pd.DataFrame())
        summary_t2_qi = st.session_state.get("_table2_summary_qi", pd.DataFrame())
    else:
        with st.spinner("彙總表2（依公司）中..."):
            summary_t2_fresh = build_table2_summary_by_company(df_seg_t2, df_daily_t2, df_orders, media_platform="全家新鮮視")
            summary_t2_qi = build_table2_summary_by_company(df_seg_t2, df_daily_t2, df_orders, media_platform="全家廣播(企頻)")
        st.session_state["_table2_summary_fresh"] = summary_t2_fresh
        st.session_state["_table2_summary_qi"] = summary_t2_qi
        st.session_state["_table2_cache_key"] = db_mtime

    details_t2_selected_df = pd.DataFrame()
    selected_company = None

    def _render_summary_table(summary_df, label):
        if summary_df.empty:
            st.info(f"尚無{label}公司彙總資料")
            return
        col_company = summary_df[["公司"]].copy()
        col_rest = summary_df.drop(columns=["公司"])
        tbl_h = min(400, 80 + len(summary_df) * 38)
        c_left, c_right = st.columns([0.5, 7])
        with c_left:
            st.dataframe(styler_one_decimal(col_company), use_container_width=True, height=tbl_h, hide_index=True)
        with c_right:
            st.dataframe(styler_one_decimal(col_rest), use_container_width=True, height=tbl_h, hide_index=True)

    st.markdown("#### 依公司統計（新鮮視）")
    _render_summary_table(summary_t2_fresh, "新鮮視")
    st.markdown("#### 依公司統計（企頻）")
    _render_summary_table(summary_t2_qi, "企頻")
    if not summary_t2_fresh.empty or not summary_t2_qi.empty:
        st.caption("(使用店秒) = 每天檔數 × 秒數 × 店數")

    st.markdown("#### 依業務統計明細")
    companies = sorted([c for c in df_seg_t2["company"].dropna().unique().tolist() if str(c).strip()])
    if not companies:
        st.info("尚無依公司明細資料")
    else:
        selected_company = st.selectbox("選擇公司查看明細", options=companies, key="table2_company_select")
        details_cache_key = f"_table2_details_{db_mtime}_{str(selected_company)}"
        if details_cache_key not in st.session_state:
            with st.spinner(f"計算 {selected_company} 明細中..."):
                details_dict = build_table2_details_by_company(
                    df_seg_t2,
                    df_daily_t2,
                    df_orders,
                    companies_to_include=[selected_company],
                )
                st.session_state[details_cache_key] = details_dict.get(selected_company, pd.DataFrame())
        details_t2_selected_df = st.session_state.get(details_cache_key, pd.DataFrame())

        if not details_t2_selected_df.empty:
            st.dataframe(
                styler_one_decimal(details_t2_selected_df),
                use_container_width=True,
                height=min(500, 90 + len(details_t2_selected_df) * 30),
                hide_index=True,
            )
        else:
            st.info("該公司尚無明細資料")

    st.markdown("#### 📥 下載表2")
    try:
        buf = BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as w:
            if not summary_t2_fresh.empty:
                summary_t2_fresh.to_excel(w, sheet_name="依公司統計-新鮮視", index=False)
            if not summary_t2_qi.empty:
                summary_t2_qi.to_excel(w, sheet_name="依公司統計-企頻", index=False)
            if selected_company and not details_t2_selected_df.empty:
                sheet_name = str(selected_company)[:31]
                details_t2_selected_df.to_excel(w, sheet_name=sheet_name, index=False)
        buf.seek(0)
        st.download_button(
            label="📥 下載表2 Excel（含依公司統計與選中公司明細）",
            data=buf.getvalue(),
            file_name=f"表2_秒數明細_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    except Exception as e:
        st.caption(f"下載 Excel 時發生錯誤：{e}")

