# -*- coding: utf-8 -*-
"""總結表圖表分頁 UI 模組。"""

from __future__ import annotations

from datetime import datetime
from io import BytesIO
from typing import Callable, Sequence

import pandas as pd
import streamlit as st


def render_summary_viz_tab(
    *,
    df_daily: pd.DataFrame,
    annual_summary_entity_labels: Sequence[str],
    get_platform_monthly_capacity: Callable[[str, int, int], float],
    build_annual_seconds_summary: Callable[..., dict],
    display_monthly_table_split: Callable[..., None],
    build_visualization_summary_pdf: Callable[..., bytes],
    build_visualization_summary_excel: Callable[..., bytes],
) -> None:
    st.markdown("### 📉 總結表視覺化")
    st.caption("圖表與數字表格一併呈現：① 各媒體平台使用率 ② 各秒數類型使用比例；下方為對應的總結表數字。")

    summary_year_viz = datetime.now().year
    if not df_daily.empty and "日期" in df_daily.columns:
        df_daily_viz = df_daily.copy()
        df_daily_viz["日期"] = pd.to_datetime(df_daily_viz["日期"], errors="coerce")
        valid = df_daily_viz["日期"].dropna()
        if len(valid) > 0:
            summary_year_viz = int(valid.min().year)
    summary_year_viz = st.number_input("年度", min_value=2020, max_value=2030, value=summary_year_viz, key="summary_year_viz")

    if df_daily.empty or "使用店秒" not in df_daily.columns:
        st.warning("📭 尚無每日資料，請先匯入或新增資料。")
        return

    def _monthly_cap_viz(mp, y, m):
        return get_platform_monthly_capacity(mp, y, m)

    annual_viz = build_annual_seconds_summary(df_daily, summary_year_viz, monthly_capacity_loader=_monthly_cap_viz)
    if not annual_viz:
        st.warning("📭 尚無每日資料或媒體平台欄位，請先匯入或新增資料。")
        return

    month_cols = [f"{m}月" for m in range(1, 13)]

    def _style_pct_viz(val):
        if not isinstance(val, (int, float)) or pd.isna(val):
            return ""
        if val >= 100:
            return "background-color: #ff6b6b; color: white"
        if val >= 70:
            return "background-color: #ffd93d"
        if val >= 50:
            return "background-color: #6bcf7f"
        return ""

    st.markdown("#### ① 各媒體平台使用率隨時間變化趨勢")
    if annual_viz.get("top_usage_df") is not None and not annual_viz["top_usage_df"].empty:
        top_df = annual_viz["top_usage_df"].copy()
        top_df["媒體平台"] = top_df["項目"].str.replace("使用率", "", regex=False)
        chart_df_platform = top_df.set_index("媒體平台")[month_cols].T
        chart_df_platform.index.name = "月份"
        try:
            import altair as alt

            chart_df_platform_melted = chart_df_platform.reset_index().melt(id_vars="月份", var_name="媒體平台", value_name="使用率")
            line_chart = alt.Chart(chart_df_platform_melted).mark_line(point=True).encode(
                x=alt.X("月份:O", title="月份"),
                y=alt.Y("使用率:Q", title="使用率 (%)", axis=alt.Axis(format=".1f")),
                color=alt.Color("媒體平台:N", title="媒體平台"),
                tooltip=["月份", "媒體平台", alt.Tooltip("使用率:Q", format=".1f", title="使用率 (%)")],
            ).properties(width=700, height=400)
            chart_df_platform_melted["使用率標籤"] = chart_df_platform_melted["使用率"].apply(lambda x: f"{x:.1f}%" if pd.notna(x) else "")
            text_chart = alt.Chart(chart_df_platform_melted).mark_text(align="center", baseline="bottom", dy=-8, fontSize=10).encode(
                x=alt.X("月份:O", title="月份"),
                y=alt.Y("使用率:Q", title="使用率 (%)", axis=alt.Axis(format=".1f")),
                text=alt.Text("使用率標籤:N"),
                color=alt.Color("媒體平台:N", legend=None),
            )
            chart = (line_chart + text_chart).properties(width=700, height=400)
            st.altair_chart(chart, use_container_width=True)
        except ImportError:
            st.line_chart(chart_df_platform)

        st.markdown("**對應數字表：年度使用率（各實體 × 1月~12月）**")
        st.caption("🟢 50%+　🟡 70%+　🔴 100%+；若某媒體整年皆為 0%，請至「媒體秒數與採購」為該媒體設定該年各月每日可用秒數（例如全家廣播(企頻)）。")
        top_tbl = annual_viz["top_usage_df"].copy()
        month_cols_viz = [c for c in top_tbl.columns if c != "項目"]

        def _style_top_table(df_subset):
            sub_month_cols = [c for c in month_cols_viz if c in df_subset.columns]
            return df_subset.style.format({c: "{:.1f}%" for c in sub_month_cols}).apply(
                lambda row: [_style_pct_viz(row.get(c)) for c in df_subset.columns], axis=1
            )

        display_monthly_table_split(top_tbl, month_cols_viz, style_func=_style_top_table, height=180, key_prefix="top_usage")
    else:
        st.info("尚無各媒體平台使用率資料（請於「媒體秒數與採購」分頁為各媒體設定當月每日可用秒數，例如「全家廣播(企頻)」1～3 月，使用率才會顯示）。")

    st.markdown("#### ② 各秒數類型使用比例隨時間變化趨勢")
    by_type_agg = None
    for ent in annual_summary_entity_labels:
        block = annual_viz.get("entities", {}).get(ent)
        if not block or block.get("by_type_df") is None:
            continue
        bt = block["by_type_df"].set_index("項目")[month_cols]
        by_type_agg = bt.copy() if by_type_agg is None else (by_type_agg + bt)

    if by_type_agg is not None and not by_type_agg.empty:
        monthly_total = by_type_agg.sum(axis=0)
        proportion = by_type_agg.copy()
        for c in month_cols:
            if monthly_total.get(c, 0) and monthly_total[c] > 0:
                proportion[c] = by_type_agg[c] / monthly_total[c] * 100
            else:
                proportion[c] = 0
        for col in proportion.columns:
            monthly_sum = proportion[col].sum()
            if monthly_sum > 0 and abs(monthly_sum - 100) > 0.01:
                proportion[col] = proportion[col] / monthly_sum * 100
        chart_df_type = proportion.T
        chart_df_type.index.name = "月份"

        try:
            import altair as alt

            chart_df_type_melted = chart_df_type.reset_index().melt(id_vars="月份", var_name="秒數類型", value_name="比例")
            chart_df_type_melted["比例"] = pd.to_numeric(chart_df_type_melted["比例"], errors="coerce").fillna(0).clip(lower=0)
            all_types = chart_df_type_melted["秒數類型"].unique()
            all_months = chart_df_type_melted["月份"].unique()
            complete_data = []
            for month in all_months:
                for sec_type in all_types:
                    existing = chart_df_type_melted[(chart_df_type_melted["月份"] == month) & (chart_df_type_melted["秒數類型"] == sec_type)]
                    complete_data.append({"月份": month, "秒數類型": sec_type, "比例": 0} if existing.empty else existing.iloc[0].to_dict())
            chart_df_type_melted = pd.DataFrame(complete_data)
            chart_df_type_melted["比例"] = chart_df_type_melted.groupby("月份")["比例"].transform(lambda x: (x / x.sum() * 100) if x.sum() > 0 else 0)
            chart_df_type_melted["比例標籤"] = chart_df_type_melted.apply(
                lambda row: f"{row['比例']:.1f}%" if pd.notna(row["比例"]) and row["比例"] > 2 else "",
                axis=1,
            )
            chart_df_type_melted_sorted = chart_df_type_melted.sort_values(["月份", "秒數類型"]).reset_index(drop=True)
            chart_df_type_melted_sorted["累積起始"] = chart_df_type_melted_sorted.groupby("月份")["比例"].transform(lambda x: x.shift(1).fillna(0).cumsum())
            chart_df_type_melted_sorted["段中間位置"] = chart_df_type_melted_sorted["累積起始"] + chart_df_type_melted_sorted["比例"] / 2

            bar_chart = alt.Chart(chart_df_type_melted_sorted).mark_bar(size=38).encode(
                x=alt.X("月份:O", title="月份"),
                y=alt.Y("比例:Q", title="比例 (%)", axis=alt.Axis(format=".1f"), stack=True, scale=alt.Scale(domain=[0, 100])),
                color=alt.Color(
                    "秒數類型:N",
                    title="秒數類型",
                    sort=alt.SortField("秒數類型", order="ascending"),
                    legend=alt.Legend(title="秒數類型", orient="right", titleFontSize=12, labelFontSize=10),
                ),
                order=alt.Order("秒數類型:O", sort="ascending"),
                tooltip=["月份", "秒數類型", alt.Tooltip("比例:Q", format=".1f", title="比例 (%)")],
            ).properties(width=700, height=400)
            text_chart = alt.Chart(chart_df_type_melted_sorted[chart_df_type_melted_sorted["比例標籤"] != ""]).mark_text(
                align="center", baseline="middle", fontSize=10, fontWeight="bold", fill="white"
            ).encode(
                x=alt.X("月份:O", title="月份"),
                y=alt.Y("段中間位置:Q", title="比例 (%)", axis=alt.Axis(format=".1f"), scale=alt.Scale(domain=[0, 100])),
                text=alt.Text("比例標籤:N"),
                color=alt.Color("秒數類型:N", legend=None),
            )
            st.altair_chart((bar_chart + text_chart).properties(width=700, height=400), use_container_width=True)
        except ImportError:
            st.bar_chart(chart_df_type)
    else:
        st.info("尚無各秒數類型使用資料。")

    st.markdown("---")
    st.markdown("#### 📊 總結表數字")
    entity_tabs = st.tabs([f"📍 {ent}" for ent in annual_summary_entity_labels])
    for idx, ent in enumerate(annual_summary_entity_labels):
        with entity_tabs[idx]:
            block = annual_viz.get("entities", {}).get(ent)
            if not block:
                st.info(f"尚無 {ent} 的資料")
                continue
            st.markdown(f"**{summary_year_viz} {ent}**")
            st.caption(f"平均每月店秒：{block['avg_monthly_seconds']:,.0f}" if block["avg_monthly_seconds"] else f"{ent} 當月每日可用秒數請於表3 設定。")

            rate_row = block.get("usage_rate_row", {})
            if rate_row and any(c.endswith("月") for c in rate_row.keys()):
                rate_data = {c: rate_row.get(c, 0) for c in month_cols if c in rate_row}
                if rate_data and any(v > 0 for v in rate_data.values()):
                    df_rate = pd.DataFrame([rate_data], index=[f"{ent}使用率"])
                    st.markdown(f"**{ent} 使用率趨勢（1月～12月）**")
                    try:
                        import altair as alt

                        df_rate_melted = df_rate.T.reset_index()
                        df_rate_melted.columns = ["月份", "使用率"]
                        chart = alt.Chart(df_rate_melted).mark_line(point=True).encode(
                            x=alt.X("月份:O", title="月份"),
                            y=alt.Y("使用率:Q", title="使用率 (%)", axis=alt.Axis(format=".1f")),
                            tooltip=["月份", alt.Tooltip("使用率:Q", format=".1f", title="使用率 (%)")],
                        ).properties(width=700, height=300)
                        st.altair_chart(chart, use_container_width=True)
                    except ImportError:
                        st.line_chart(df_rate.T)

            used_row = block.get("used_row", {})
            unused_row = block.get("unused_row", {})
            if used_row and unused_row:
                used_data = {c: used_row.get(c, 0) for c in month_cols if c in used_row}
                unused_data = {c: unused_row.get(c, 0) for c in month_cols if c in unused_row}
                if used_data or unused_data:
                    df_usage = pd.DataFrame({"使用秒數": [used_data.get(c, 0) for c in month_cols], "未使用秒數": [unused_data.get(c, 0) for c in month_cols]}, index=month_cols)
                    st.markdown(f"**{ent} 使用/未使用秒數（1月～12月）**")
                    st.area_chart(df_usage)

            bt = block["by_type_df"]
            if not bt.empty and "項目" in bt.columns:
                bt_chart = bt.set_index("項目")[month_cols].T
                if not bt_chart.empty and bt_chart.sum().sum() > 0:
                    st.markdown(f"**{ent} 秒數用途分列趨勢（1月～12月）**")
                    st.area_chart(bt_chart)

            bt_month_cols = [c for c in bt.columns if c != "項目"]

            def _style_by_type_table(df_subset):
                sub_month_cols = [c for c in bt_month_cols if c in df_subset.columns]
                return df_subset.style.format({c: "{:,.1f}" for c in sub_month_cols}) if sub_month_cols else df_subset.style

            st.markdown(f"**{ent} 秒數用途分列（1月～12月）**")
            display_monthly_table_split(bt, bt_month_cols, style_func=_style_by_type_table, height=220, key_prefix=f"by_type_{ent}")

            summary_table = pd.DataFrame([block["used_row"], block["unused_row"], block["usage_rate_row"]])
            sum_month_cols = [c for c in summary_table.columns if c.endswith("月")]

            def _style_summary_table(df_subset):
                sub_month_cols = [c for c in sum_month_cols if c in df_subset.columns]
                df_display = df_subset.copy()
                original_values = {}
                for idx2, row in df_display.iterrows():
                    row_name = str(row.get("項目", ""))
                    original_values[idx2] = {}
                    if row_name.endswith("使用率"):
                        for col in sub_month_cols:
                            if col in df_display.columns:
                                val = row[col]
                                original_values[idx2][col] = val
                                if isinstance(val, (int, float)) and not pd.isna(val):
                                    df_display.at[idx2, col] = f"{val:.1f}%"
                    else:
                        for col in sub_month_cols:
                            if col in df_display.columns:
                                val = row[col]
                                original_values[idx2][col] = val
                                if isinstance(val, (int, float)) and not pd.isna(val):
                                    df_display.at[idx2, col] = f"{val:,.1f}"

                def _apply_color(row):
                    row_name = str(row.get("項目", ""))
                    idx3 = row.name
                    colors = []
                    for c in df_subset.columns:
                        if row_name.endswith("使用率") and c.endswith("月"):
                            orig_val = original_values.get(idx3, {}).get(c, row.get(c))
                            colors.append(_style_pct_viz(orig_val))
                        else:
                            colors.append("")
                    return colors

                return df_display.style.apply(_apply_color, axis=1)

            st.markdown(f"**{ent} 使用/未使用/使用率（1月～12月）**")
            display_monthly_table_split(summary_table, sum_month_cols, style_func=_style_summary_table, height=140, key_prefix=f"summary_{ent}")

    st.markdown("---")
    st.markdown("#### 📥 下載報告")
    col_pdf, col_excel = st.columns(2)
    with col_pdf:
        pdf_bytes = build_visualization_summary_pdf(annual_viz, summary_year_viz)
        if pdf_bytes:
            st.download_button(
                label="📥 下載 PDF",
                data=pdf_bytes,
                file_name=f"總結表視覺化_{summary_year_viz}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf",
                mime="application/pdf",
                key="download_viz_pdf",
                use_container_width=True,
            )
        else:
            st.caption("PDF 生成失敗（可能缺少中文字型支援）")
    with col_excel:
        excel_bytes = build_visualization_summary_excel(annual_viz, summary_year_viz)
        if excel_bytes:
            st.download_button(
                label="📥 下載 Excel",
                data=excel_bytes,
                file_name=f"總結表視覺化_{summary_year_viz}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="download_viz_excel",
                use_container_width=True,
            )
        else:
            st.caption("Excel 生成失敗")

    st.markdown("#### 📥 下載年度總結（Excel）")
    try:
        buf = BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as w:
            if annual_viz.get("top_usage_df") is not None and not annual_viz["top_usage_df"].empty:
                annual_viz["top_usage_df"].to_excel(w, sheet_name="年度使用率", index=False)
            for ent in annual_summary_entity_labels:
                block = annual_viz.get("entities", {}).get(ent)
                if block:
                    block["by_type_df"].to_excel(w, sheet_name=f"{ent}_秒數用途", index=False)
                    pd.DataFrame([block["used_row"], block["unused_row"], block["usage_rate_row"]]).to_excel(w, sheet_name=f"{ent}_使用未使用率", index=False)
        buf.seek(0)
        st.download_button(
            label="📥 下載年度使用秒數總表 Excel",
            data=buf.getvalue(),
            file_name=f"年度使用秒數總表_{summary_year_viz}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="dl_annual_summary_viz",
        )
    except Exception as e:
        st.caption(f"下載 Excel 時發生錯誤：{e}")

