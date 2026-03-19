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
    build_table2_summary_by_company: Callable[..., pd.DataFrame],
    build_table2_details_by_company: Callable[..., dict],
    styler_one_decimal: Callable[[pd.DataFrame], object],
) -> None:
    st.markdown("### 📅 表2－秒數明細（對齊 Excel 表2）")
    st.caption("依公司統計總覽、依業務統計明細（平台／合約／客戶／每日使用店秒），含小計。")

    df_seg_t2 = load_segments_cached(db_mtime)
    if df_seg_t2.empty or df_daily.empty:
        st.warning("📭 尚無檔次段或每日資料，請先匯入或新增資料。")
        return

    # 換月份時不重算：表2 依 _db_mtime 快取於 session_state
    if st.session_state.get("_table2_cache_key") == db_mtime and "_table2_summary" in st.session_state and "_table2_details" in st.session_state:
        details_t2 = st.session_state["_table2_details"]
        if "_table2_summary_fresh" not in st.session_state or "_table2_summary_qi" not in st.session_state:
            st.session_state["_table2_summary_fresh"] = build_table2_summary_by_company(df_seg_t2, df_daily, df_orders, media_platform="全家新鮮視")
            st.session_state["_table2_summary_qi"] = build_table2_summary_by_company(df_seg_t2, df_daily, df_orders, media_platform="全家廣播(企頻)")
    else:
        summary_t2 = build_table2_summary_by_company(df_seg_t2, df_daily, df_orders)
        details_t2 = build_table2_details_by_company(df_seg_t2, df_daily, df_orders)
        summary_t2_fresh = build_table2_summary_by_company(df_seg_t2, df_daily, df_orders, media_platform="全家新鮮視")
        summary_t2_qi = build_table2_summary_by_company(df_seg_t2, df_daily, df_orders, media_platform="全家廣播(企頻)")
        st.session_state["_table2_summary"] = summary_t2
        st.session_state["_table2_summary_fresh"] = summary_t2_fresh
        st.session_state["_table2_summary_qi"] = summary_t2_qi
        st.session_state["_table2_details"] = details_t2
        st.session_state["_table2_cache_key"] = db_mtime

    summary_t2_fresh = st.session_state.get("_table2_summary_fresh", pd.DataFrame())
    summary_t2_qi = st.session_state.get("_table2_summary_qi", pd.DataFrame())
    details_t2 = st.session_state.get("_table2_details", {})

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
    if details_t2:
        for company_name, detail_df in details_t2.items():
            with st.expander(f"**{company_name}**", expanded=True):
                st.dataframe(styler_one_decimal(detail_df), use_container_width=True, height=min(400, 80 + len(detail_df) * 38), hide_index=True)
    else:
        st.info("尚無依公司明細資料")

    st.markdown("#### 📥 下載表2")
    try:
        buf = BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as w:
            if not summary_t2_fresh.empty:
                summary_t2_fresh.to_excel(w, sheet_name="依公司統計-新鮮視", index=False)
            if not summary_t2_qi.empty:
                summary_t2_qi.to_excel(w, sheet_name="依公司統計-企頻", index=False)
            for company_name, detail_df in details_t2.items():
                sheet_name = str(company_name)[:31]
                detail_df.to_excel(w, sheet_name=sheet_name, index=False)
        buf.seek(0)
        st.download_button(
            label="📥 下載表2 Excel（含依公司統計與各公司明細）",
            data=buf.getvalue(),
            file_name=f"表2_秒數明細_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    except Exception as e:
        st.caption(f"下載 Excel 時發生錯誤：{e}")

