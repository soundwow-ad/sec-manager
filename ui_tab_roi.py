# -*- coding: utf-8 -*-
"""ROI 分頁 UI 模組。"""

from __future__ import annotations

from datetime import datetime
from typing import Callable, Sequence

import pandas as pd
import streamlit as st


def render_roi_tab(
    *,
    calculate_roi_by_period: Callable[[str, int, int, str], Sequence[dict]],
    get_roi_all_period_date_range: Callable[[], tuple],
    styler_one_decimal: Callable[[pd.DataFrame], object],
) -> None:
    st.markdown("### 📊 ROI 投報分析")
    st.caption("依現有採購與訂單資料計算各媒體之投報率，支援多時間維度檢視。")

    with st.expander("📖 資料來源說明", expanded=False):
        st.markdown(
            """
| 項目 | 來源 |
|------|------|
| **購買成本** | 「📋 媒體秒數與採購」分頁的購買價格，依選定時間維度彙總 |
| **實收金額** | 表1 訂單；依檔次段日期與選定區間重疊者計算，同一合約多媒體時依秒數比例或拆分金額分配 |
| **ROI** | (實收 - 購買成本) ÷ 購買成本 |
"""
        )

    roi_time_dim = st.radio(
        "時間維度",
        options=["month", "quarter", "year", "all"],
        format_func=lambda x: {"month": "📅 單月", "quarter": "📊 單季", "year": "📆 單年", "all": "🔄 累計至今"}[x],
        horizontal=True,
        key="roi_time_dim",
    )
    roi_year = datetime.now().year
    roi_month = 1
    roi_quarter = 1
    if roi_time_dim == "month":
        c1, c2 = st.columns(2)
        with c1:
            roi_year = st.number_input("參考年度", min_value=2020, max_value=2030, value=datetime.now().year, key="roi_year")
        with c2:
            roi_month = st.number_input("參考月份", min_value=1, max_value=12, value=datetime.now().month, key="roi_month")
        period_label = f"{roi_year}年{roi_month}月"
    elif roi_time_dim == "quarter":
        c1, c2 = st.columns(2)
        with c1:
            roi_year = st.number_input("參考年度", min_value=2020, max_value=2030, value=datetime.now().year, key="roi_year")
        with c2:
            roi_quarter = st.selectbox(
                "參考季度",
                options=[1, 2, 3, 4],
                format_func=lambda x: f"Q{x}（{'1-3月' if x == 1 else '4-6月' if x == 2 else '7-9月' if x == 3 else '10-12月'}）",
                key="roi_quarter",
            )
        roi_month = (roi_quarter - 1) * 3 + 1
        period_label = f"{roi_year} Q{roi_quarter}"
    elif roi_time_dim == "year":
        roi_year = st.number_input("參考年度", min_value=2020, max_value=2030, value=datetime.now().year, key="roi_year")
        period_label = f"{roi_year}年"
    else:
        st.caption("將彙總所有採購與訂單資料，無需選擇年度或月份。")
        period_label = "累計至今"

    roi_rows = calculate_roi_by_period(roi_time_dim, roi_year, roi_month if roi_time_dim in ("month", "quarter") else 1, period_label)
    if not roi_rows:
        st.warning("尚無採購資料或該區間無資料。請至「📋 媒體秒數與採購」分頁輸入購買秒數與購買價格。")
        return

    roi_df = pd.DataFrame(roi_rows)
    display_label = period_label
    if period_label == "累計至今":
        range_start, range_end = get_roi_all_period_date_range()
        if range_start and range_end:
            display_label = f"累計至今（{range_start} ～ {range_end}）"
    st.markdown(f"#### 媒體別 ROI 表 — {display_label}")

    try:
        import altair as alt

        roi_chart_df = roi_df.copy()
        roi_chart_df["ROI色彩"] = roi_chart_df["ROI（投報率）"].apply(lambda x: "正報酬" if x >= 0 else "負報酬")
        chart_roi = alt.Chart(roi_chart_df).mark_bar(size=38).encode(
            x=alt.X("媒體:N", title="媒體", sort="-y"),
            y=alt.Y("ROI（投報率）:Q", title="ROI（投報率）", axis=alt.Axis(format="%")),
            color=alt.Color("ROI色彩:N", scale=alt.Scale(domain=["正報酬", "負報酬"], range=["#27ae60", "#e74c3c"]), legend=None),
            tooltip=[
                alt.Tooltip("媒體:N", title="媒體"),
                alt.Tooltip("ROI（投報率）:Q", format=".2%", title="ROI"),
                alt.Tooltip("實收金額（元）:Q", format=",.0f"),
                alt.Tooltip("購買成本（元）:Q", format=",.0f"),
            ],
        ).properties(width=700, height=350).configure_axisY(format="%")
        st.altair_chart(chart_roi, use_container_width=True)
    except Exception:
        st.bar_chart(roi_df.set_index("媒體")["ROI（投報率）"])

    st.dataframe(styler_one_decimal(roi_df.drop(columns=["時間區間"], errors="ignore")), use_container_width=True, height=min(200, 60 + len(roi_rows) * 38))

    st.markdown("---")
    st.markdown("#### 🔀 多維度比較")
    st.caption("一次檢視「當月、當季、當年、累計」四種維度的 ROI，快速掌握各媒體在不同時間尺度下的表現。")
    if st.checkbox("顯示多維度比較表", value=False, key="roi_multi_compare"):
        all_rows = []
        for pt, pl in [("month", f"{roi_year}年{roi_month}月"), ("quarter", f"{roi_year} Q{(roi_month-1)//3+1}"), ("year", f"{roi_year}年"), ("all", "累計至今")]:
            rows = calculate_roi_by_period(pt, roi_year, roi_month if pt in ("month", "quarter") else 1, pl)
            for row in rows:
                row["時間區間"] = pl
                all_rows.append(row)
        if all_rows:
            multi_df = pd.DataFrame(all_rows)
            pivot_roi = multi_df.pivot_table(index="媒體", columns="時間區間", values="ROI（投報率）", aggfunc="first")
            order_cols = [f"{roi_year}年{roi_month}月", f"{roi_year} Q{(roi_month-1)//3+1}", f"{roi_year}年", "累計至今"]
            pivot_roi = pivot_roi.reindex(columns=[c for c in order_cols if c in pivot_roi.columns])
            st.dataframe(pivot_roi.style.format("{:.2%}"), use_container_width=True)

