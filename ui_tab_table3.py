# -*- coding: utf-8 -*-
"""表3 每日庫存分頁 UI 模組。"""

from __future__ import annotations

from datetime import datetime
from typing import Callable, Mapping, Sequence

import numpy as np
import pandas as pd
import streamlit as st


def render_table3_tab(
    *,
    db_mtime: int,
    load_segments_cached: Callable[[int], pd.DataFrame],
    explode_segments_to_daily_cached: Callable[[pd.DataFrame], pd.DataFrame],
    load_platform_monthly_capacity_for: Callable[[int, int], Mapping[str, int]],
    build_table3_monthly_control_cached: Callable[[int, int, int, tuple], Mapping[str, pd.DataFrame]],
    media_platform_options: Sequence[str],
    styler_one_decimal: Callable[[pd.DataFrame], object],
) -> None:
    """表3 內容：fragment 重跑時只跑此函數。"""
    _db = db_mtime
    df_seg_t3 = load_segments_cached(_db)
    df_daily_t3 = explode_segments_to_daily_cached(df_seg_t3) if not df_seg_t3.empty else pd.DataFrame()

    st.markdown("### 📊 每月秒數控管表（對齊 Excel 表3）")
    st.caption("依媒體平台區分：執行秒、可用秒數、使用率、可排日（綠 50%+／黃 70%+／紅 100%+）。可選年份月份。每日可用秒數請至「媒體秒數與採購」分頁設定。")

    default_year = datetime.now().year
    default_month = datetime.now().month
    if not df_daily_t3.empty and "日期" in df_daily_t3.columns:
        df_daily_t3["日期"] = pd.to_datetime(df_daily_t3["日期"], errors="coerce")
        valid = df_daily_t3["日期"].dropna()
        if len(valid) > 0:
            default_year = int(valid.min().year)
            default_month = int(valid.min().month)
    sel_year = st.number_input("年份", min_value=2020, max_value=2030, value=default_year, key="table3_year")
    sel_month = st.number_input("月份", min_value=1, max_value=12, value=default_month, key="table3_month")

    if df_daily_t3.empty or df_seg_t3.empty:
        st.warning("📭 尚無每日或檔次段資料，請先匯入或新增資料。")
    elif "媒體平台" not in df_daily_t3.columns:
        st.warning("📭 每日資料尚無媒體平台欄位，請重新匯入或檢查資料。")
    else:
        monthly_cap = load_platform_monthly_capacity_for(sel_year, sel_month)
        cap_tuple = tuple(sorted(monthly_cap.items())) if monthly_cap else ()
        table3_data = build_table3_monthly_control_cached(_db, sel_year, sel_month, cap_tuple)
        if not table3_data:
            st.info("該年該月尚無媒體平台資料可顯示。")
        else:
            st.markdown("#### 📺 媒體平台")
            options_mp = ["全部"] + [p for p in media_platform_options if p in table3_data]
            if len(options_mp) == 1:
                options_mp = ["全部"] + list(table3_data.keys())
            sel_mp = st.radio("選擇媒體平台", options=options_mp, horizontal=True, key="table3_media_filter")

            def _util_color(u):
                if u >= 100:
                    return "background-color: #ff6b6b; color: white"
                if u >= 70:
                    return "background-color: #ffd93d"
                return "background-color: #6bcf7f"

            to_show = list(table3_data.keys()) if sel_mp == "全部" else [sel_mp]
            for mp in to_show:
                if mp not in table3_data:
                    continue
                df_t3 = table3_data[mp].copy()
                date_cols_t3 = [c for c in df_t3.columns if c not in ("授權", "項目", "秒數", "%")]
                row_util_vals = table3_data[mp].iloc[3]
                util_vals = [row_util_vals.get(c) for c in date_cols_t3 if isinstance(row_util_vals.get(c), (int, float)) and pd.notna(row_util_vals.get(c))]
                n_red = sum(1 for u in util_vals if u >= 100)
                n_yellow = sum(1 for u in util_vals if 70 <= u < 100)
                n_green = sum(1 for u in util_vals if u < 70)
                try:
                    _mu = table3_data[mp].iloc[0].get("%")
                    month_util = float(_mu) if _mu is not None and pd.notna(_mu) else (sum(util_vals) / len(util_vals) if util_vals else 0)
                except (TypeError, KeyError, ValueError):
                    month_util = sum(util_vals) / len(util_vals) if util_vals else 0
                util_label = f"{round(float(month_util), 1)}%" if isinstance(month_util, (int, float)) else "—"
                if isinstance(month_util, (int, float)) and month_util >= 100:
                    util_status = "🔴 已滿"
                    suggestion = "建議：避免再加全省案，僅可補區域。"
                elif isinstance(month_util, (int, float)) and month_util >= 70:
                    util_status = "⚠️ 偏高"
                    suggestion = "建議：注意檔期集中，可考慮分散排程。"
                else:
                    util_status = "✅ 尚可"
                    suggestion = "建議：可排新案，留意熱門日期。"
                st.markdown(f"**{mp}**")
                st.markdown(f"📌 **{sel_year}/{sel_month} {mp}**  ")
                st.markdown(f"- 本月使用率：**{util_label}**（{util_status}）  ")
                st.markdown(f"- 🔴 紅色天數：{n_red} 天　🟡 黃色天數：{n_yellow} 天　🟢 綠色天數：{n_green} 天  ")
                st.markdown(f"- {suggestion}")
                st.markdown("")

                for col in date_cols_t3:
                    val = df_t3.at[3, col]
                    if isinstance(val, (int, float)) and pd.notna(val):
                        df_t3.at[3, col] = f"{round(float(val), 1)}%"
                orig_row4 = table3_data[mp].iloc[3].copy()
                fixed_cols_t3 = ["授權", "項目", "秒數", "%"]
                chunk_size = 6
                date_chunks = [date_cols_t3[i : i + chunk_size] for i in range(0, len(date_cols_t3), chunk_size)]

                def _style_chunk(row, chunk_dates):
                    out = [""] * len(row)
                    if row.name != 3:
                        return out
                    for i, c in enumerate(row.index):
                        if c in chunk_dates:
                            orig = orig_row4.get(c)
                            if isinstance(orig, (int, float)) and pd.notna(orig):
                                out[i] = _util_color(orig)
                    return out

                st.caption("🟢 綠 &lt;70%　🟡 黃 70%+　🔴 紅 100%+")
                for chunk in date_chunks:
                    sub = df_t3[fixed_cols_t3 + chunk]
                    st.caption(f"**{chunk[0]} ～ {chunk[-1]}**")
                    num_cols_sub = sub.select_dtypes(include=[np.number]).columns.tolist()
                    fmt_sub = {c: "{:,.1f}" for c in num_cols_sub} if num_cols_sub else {}
                    st.dataframe(sub.style.format(fmt_sub).apply(lambda row: _style_chunk(row, chunk), axis=1), use_container_width=True)

                if not df_daily_t3.empty and "媒體平台" in df_daily_t3.columns and "日期" in df_daily_t3.columns:
                    df_daily_t3["日期"] = pd.to_datetime(df_daily_t3["日期"], errors="coerce")
                    month_dates = [d for d in df_daily_t3["日期"].dropna().unique() if d.year == sel_year and d.month == sel_month]
                    month_dates = sorted(month_dates)
                    if month_dates:
                        date_options = ["— 選擇日期查看當日明細 —"] + [f"{d.month}/{d.day}" for d in month_dates]
                        sel_date_str = st.selectbox(f"選擇日期（{mp}）", date_options, key=f"table3_sel_date_{mp}_{sel_year}_{sel_month}")
                        if sel_date_str != "— 選擇日期查看當日明細 —":
                            try:
                                parts = sel_date_str.split("/")
                                day = int(parts[1])
                                target_d = pd.Timestamp(sel_year, sel_month, day)
                                dd = df_daily_t3[(df_daily_t3["媒體平台"] == mp) & (df_daily_t3["日期"].dt.normalize() == target_d)]
                                if not dd.empty:
                                    show_cols = [c for c in ["日期", "媒體平台", "公司", "業務", "客戶", "產品", "使用店秒", "秒數", "檔次"] if c in dd.columns]
                                    dd_show = dd[show_cols] if show_cols else dd
                                    st.dataframe(styler_one_decimal(dd_show), use_container_width=True, height=min(200, 80 + len(dd) * 38))
                                else:
                                    st.caption("該日無使用紀錄")
                            except Exception:
                                pass
                st.markdown("---")

            st.markdown("#### 🎨 可排日顏色說明")
            c1, c2, c3 = st.columns(3)
            with c1:
                st.markdown("🟢 **綠**：使用率 &lt; 70%")
            with c2:
                st.markdown("🟡 **黃**：70% ≤ 使用率 &lt; 100%")
            with c3:
                st.markdown("🔴 **紅**：使用率 ≥ 100%")

