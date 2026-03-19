# -*- coding: utf-8 -*-
"""分公司×媒體 每月秒數分頁 UI 模組。"""

from __future__ import annotations

from datetime import datetime
from typing import Callable

import numpy as np
import pandas as pd
import streamlit as st


def render_branch_media_tab(
    *,
    df_daily: pd.DataFrame,
    styler_one_decimal: Callable[[pd.DataFrame], object],
) -> None:
    st.markdown("### 📊 分公司 × 媒體平台 使用總秒數")
    st.caption("多種圖表回答不同決策問題：結構、總量、誰用最多、是否失衡、趨勢。")
    if df_daily.empty or "使用店秒" not in df_daily.columns or "公司" not in df_daily.columns or "媒體平台" not in df_daily.columns:
        st.warning("📭 尚無每日資料或缺少「公司」「媒體平台」「使用店秒」欄位，請先匯入或新增資料。")
        return

    df_v = df_daily.copy()
    df_v["日期"] = pd.to_datetime(df_v["日期"], errors="coerce")
    df_v = df_v.dropna(subset=["日期"])
    df_v["年"] = df_v["日期"].dt.year
    df_v["月"] = df_v["日期"].dt.month
    agg = df_v.groupby(["年", "月", "公司", "媒體平台"], dropna=False)["使用店秒"].sum().reset_index()
    years_avail = sorted(agg["年"].dropna().unique().astype(int).tolist()) if not agg.empty else [datetime.now().year]
    viz_year = st.number_input("年度", min_value=2020, max_value=2030, value=years_avail[0] if years_avail else datetime.now().year, key="viz_branch_media_year")
    agg_y = agg[agg["年"] == viz_year]
    companies_avail = sorted(agg_y["公司"].dropna().unique().tolist()) if not agg_y.empty else []
    companies_avail = [c for c in companies_avail if c]
    media_avail = sorted(agg_y["媒體平台"].dropna().unique().tolist()) if not agg_y.empty else []
    media_avail = [m for m in media_avail if m]

    time_scope = st.radio("時間範圍", options=["全年合計", "指定月份"], horizontal=True, key="viz_branch_scope")
    if time_scope == "指定月份":
        month_choice = st.selectbox("選擇月份", options=list(range(1, 13)), format_func=lambda x: f"{x}月", key="viz_branch_month")
        agg_scope = agg_y[agg_y["月"] == month_choice]
    else:
        agg_scope = agg_y

    if not agg_scope.empty and companies_avail:
        pivot_t = agg_scope.pivot_table(index="公司", columns="媒體平台", values="使用店秒", aggfunc="sum").reindex(companies_avail).fillna(0)
        pivot_t = pivot_t.reindex(columns=media_avail, fill_value=0) if media_avail else pivot_t
        total_scope = pivot_t.sum().sum()
    else:
        pivot_t = pd.DataFrame()
        total_scope = 0

    if pivot_t.empty or total_scope <= 0:
        st.caption("尚無分公司或媒體資料，或該時間範圍無使用資料，請先匯入或新增資料。")
        return

    st.markdown("---")
    st.markdown("#### ① 各分公司 × 媒體平台 — 總秒數堆疊圖")
    st.caption("每根長條為一分公司，各段為各媒體使用總秒數（堆疊為實際秒數，非占比）。")
    try:
        import altair as alt

        cols = pivot_t.reset_index().columns.tolist()
        melt_t = pivot_t.reset_index().melt(id_vars=[cols[0]], var_name="媒體", value_name="秒數").rename(columns={cols[0]: "分公司"})
        chart1 = alt.Chart(melt_t).mark_bar(size=38).encode(
            x=alt.X("分公司:N", title="分公司"),
            y=alt.Y("秒數:Q", title="秒數"),
            color=alt.Color("媒體:N", title="媒體"),
            tooltip=["分公司", "媒體", alt.Tooltip("秒數:Q", format=",.0f")],
        ).properties(width=700, height=400)
        st.altair_chart(chart1, use_container_width=True)
    except ImportError:
        st.bar_chart(pivot_t)
    st.dataframe(styler_one_decimal(pivot_t.reset_index()), use_container_width=True, height=min(220, 80 + len(pivot_t) * 36))

    st.markdown("---")
    st.markdown("#### ② 分公司 × 平台 使用秒數（先分公司、再分平台，同平台同色）")
    st.caption("X 軸依序為 分公司-平台（東吳-企頻、東吳-新鮮視…）；同一平台顏色一致，方便比較不同分公司。")
    bar_labels = [f"{co}-{mp}" for co in companies_avail for mp in media_avail]
    df_bars = pd.DataFrame(0.0, index=bar_labels, columns=media_avail)
    for co in companies_avail:
        for mp in media_avail:
            df_bars.loc[f"{co}-{mp}", mp] = float(pivot_t.loc[co, mp]) if co in pivot_t.index and mp in pivot_t.columns else 0.0
    try:
        import altair as alt

        melt_bars = df_bars.reset_index().melt(id_vars=["index"], var_name="媒體", value_name="秒數").rename(columns={"index": "分公司-平台"})
        if not melt_bars.empty:
            chart2 = alt.Chart(melt_bars).mark_bar(size=38).encode(
                x=alt.X("分公司-平台:N", title="分公司-平台", sort=bar_labels),
                y=alt.Y("秒數:Q", title="秒數"),
                color=alt.Color("媒體:N", title="媒體"),
                tooltip=["分公司-平台", "媒體", alt.Tooltip("秒數:Q", format=",.0f")],
            ).properties(width=700, height=400)
            st.altair_chart(chart2, use_container_width=True)
        else:
            st.bar_chart(df_bars)
    except ImportError:
        st.bar_chart(df_bars)
    st.dataframe(styler_one_decimal(pivot_t.reset_index()), use_container_width=True, height=min(220, 80 + len(pivot_t) * 36))

    st.markdown("---")
    st.markdown("#### ③ 某媒體「誰用最多」— 媒體 × 分公司矩陣表 / heatmap")
    st.caption("列＝媒體平台、欄＝分公司；顏色越深表示該媒體在該分公司用量越高（可看出單一媒體誰用最多）。")
    pivot_media_company = pivot_t.T.astype(float)

    def _heatmap_row_style(row):
        mn, mx = row.min(), row.max()
        if mx <= mn or pd.isna(mx):
            return [""] * len(row)
        out = []
        for v in row:
            if not isinstance(v, (int, float)) or pd.isna(v) or v <= 0:
                out.append("")
                continue
            r = (v - mn) / (mx - mn)
            R = 255
            G = int(255 - 138 * r)
            B = int(240 - 133 * r)
            out.append(f"background-color: rgb({R},{max(0,G)},{max(0,B)})")
        return out

    heatmap_styled = pivot_media_company.style.apply(_heatmap_row_style, axis=1).format("{:,.0f}")
    st.dataframe(heatmap_styled, use_container_width=True, height=min(320, 100 + len(pivot_media_company) * 38))

    st.markdown("---")
    st.markdown("#### ④ 資源是否失衡 — 占比 + 警示色")
    st.caption("各分公司內各媒體占比；🔴 單一媒體佔該分公司 ≥50% 可能過度集中、🟡 30–50%、🟢 較分散。")
    row_sum_ = pivot_t.sum(axis=1)
    pct_t = pivot_t.div(row_sum_.replace(0, np.nan), axis=0).fillna(0) * 100

    def _cell_balance_style(v):
        if not isinstance(v, (int, float)) or pd.isna(v):
            return ""
        if v >= 50:
            return "background-color: #ff6b6b; color: white"
        if v >= 30:
            return "background-color: #ffd93d"
        if v > 0:
            return "background-color: #90EE90"
        return ""

    pct_display = pct_t.reset_index()

    def _balance_color(row):
        return ["" if c == "公司" else _cell_balance_style(row.get(c)) for c in pct_display.columns]

    st.dataframe(
        pct_display.style.format({c: "{:,.1f}%" for c in media_avail if c in pct_display.columns}).apply(_balance_color, axis=1),
        use_container_width=True,
        height=min(280, 80 + len(pct_display) * 36),
    )

    st.markdown("---")
    st.markdown("#### ⑤ 年度 vs 月份趨勢 — 小 multiples 折線圖")
    st.caption("各分公司在 1～12 月、各媒體使用秒數的變化（每區塊一分公司）。")
    if not agg_y.empty and companies_avail and media_avail:
        n_cols = min(3, len(companies_avail))
        for i in range(0, len(companies_avail), n_cols):
            cols = st.columns(n_cols)
            for j in range(n_cols):
                idx = i + j
                if idx >= len(companies_avail):
                    break
                co = companies_avail[idx]
                with cols[j]:
                    agg_co = agg_y[agg_y["公司"] == co].pivot_table(index="月", columns="媒體平台", values="使用店秒", aggfunc="sum").reindex(range(1, 13)).fillna(0)
                    agg_co.index = [f"{int(m)}月" for m in agg_co.index]
                    if not agg_co.empty and agg_co.sum().sum() > 0:
                        st.caption(f"**{co}**")
                        st.line_chart(agg_co)
                    else:
                        st.caption(f"**{co}**（無資料）")

    st.markdown("---")
    st.markdown("#### ⑥ 全年趨勢合併圖（所有分公司-平台 一次看）")
    st.caption("圖⑤ 的折線合併成一張圖，每條線為一個「分公司-平台」；顏色採易辨識配置。")
    if not agg_y.empty and companies_avail and media_avail:
        series_order = [f"{co}-{mp}" for co in companies_avail for mp in media_avail]
        long_rows = []
        for _, r in agg_y.iterrows():
            key = f"{r['公司']}-{r['媒體平台']}"
            if key in series_order:
                long_rows.append({"月": f"{int(r['月'])}月", "分公司-平台": key, "使用秒數": float(r["使用店秒"])})
        if long_rows:
            df_lines = pd.DataFrame(long_rows)
            pivot_lines = df_lines.pivot_table(index="月", columns="分公司-平台", values="使用秒數", aggfunc="sum").reindex([f"{m}月" for m in range(1, 13)], fill_value=0).fillna(0)
            for c in series_order:
                if c not in pivot_lines.columns:
                    pivot_lines[c] = 0
            pivot_lines = pivot_lines[[c for c in series_order if c in pivot_lines.columns]]
            if not pivot_lines.empty and pivot_lines.sum().sum() > 0:
                try:
                    import altair as alt
                    import colorsys

                    palette = []
                    n_c, n_m = len(companies_avail), len(media_avail)
                    for i in range(n_c):
                        hue = (i / max(1, n_c)) * 0.82
                        for j in range(n_m):
                            lightness = 0.38 + 0.4 * (j / max(1, n_m))
                            r, g, b = colorsys.hls_to_rgb(hue, lightness, 0.75)
                            palette.append("#{:02x}{:02x}{:02x}".format(int(r * 255), int(g * 255), int(b * 255)))
                    source = pivot_lines.reset_index().melt(id_vars=["月"], var_name="分公司-平台", value_name="使用秒數")
                    month_order = [f"{m}月" for m in range(1, 13)]
                    source["月序"] = source["月"].map(lambda x: month_order.index(x) if x in month_order else 0)
                    lines = (
                        alt.Chart(source)
                        .mark_line(strokeWidth=2.5, point=alt.OverlayMarkDef(size=50, filled=True))
                        .encode(
                            x=alt.X("月:O", title="月份", sort=month_order),
                            y=alt.Y("使用秒數:Q", title="使用秒數"),
                            color=alt.Color("分公司-平台:N", legend=alt.Legend(title="分公司-平台"), scale=alt.Scale(range=palette)),
                            order="月序",
                        )
                        .properties(width=700, height=400)
                    )
                    st.altair_chart(lines, use_container_width=True)
                except ImportError:
                    st.line_chart(pivot_lines)
                    st.caption("（安裝 altair 可顯示自訂易辨識顏色：pip install altair）")
            else:
                st.caption("該年度無使用資料")
        else:
            st.caption("該年度無使用資料")
    else:
        st.caption("尚無分公司或媒體資料。")

