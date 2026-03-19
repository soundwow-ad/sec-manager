# -*- coding: utf-8 -*-
"""實驗分頁 UI 模組。"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Callable, Sequence

import pandas as pd
import streamlit as st


def render_experiment_tab(
    *,
    df_daily: pd.DataFrame,
    media_platform_options: Sequence[str],
    emergency_days_default: int,
    seconds_per_spot_15s: int,
    get_platform_monthly_capacity: Callable[[str, int, int], float],
    build_daily_inventory_and_metrics: Callable[..., tuple],
    seconds_to_spot_label: Callable[..., str],
    styler_one_decimal: Callable[[pd.DataFrame], object],
) -> None:
    st.session_state["main_tab"] = "🧪 實驗分頁"
    st.markdown("### 🧪 依時間的庫存警示與分析（實驗）")
    with st.expander("📌 系統前提（核心假設）", expanded=True):
        st.markdown(
            """
- **當月秒數若未使用，於月底結算時視為 100% 浪費（不可逆）**
- 秒數的價值會隨時間接近月底而**快速衰減**
- 系統目標：**最小化月底浪費**（不是避免爆量）
- 爆量仍需監控，但屬次要風險
        """
        )

    today = datetime.now().date()
    exp_year = st.number_input("年度", min_value=2020, max_value=2030, value=today.year, key="exp_year")
    exp_month = st.number_input("月份", min_value=1, max_value=12, value=today.month, key="exp_month")
    emergency_days = st.slider("緊急期天數（T0 可補救窗口）", min_value=3, max_value=14, value=emergency_days_default, key="exp_emergency_days")

    exp_scope_options = ["全媒體合計"] + list(media_platform_options)
    exp_scope = st.selectbox("**分析對象**（本頁所有指標與圖表皆依此對象計算）", exp_scope_options, key="exp_scope")
    exp_media_filter = None if exp_scope == "全媒體合計" else exp_scope

    if df_daily.empty or "使用店秒" not in df_daily.columns:
        st.warning("📭 尚無每日資料，請先匯入或新增資料。")
        return

    def _cap_loader(mp, y, mo):
        return get_platform_monthly_capacity(mp, y, mo)

    daily_inv, metrics = build_daily_inventory_and_metrics(
        df_daily,
        exp_year,
        exp_month,
        today,
        emergency_days=emergency_days,
        monthly_capacity_loader=_cap_loader,
        media_platform=exp_media_filter,
    )
    month_cap = metrics["month_total_capacity"] or 1
    past_wasted_pct = round(metrics["past_wasted_seconds"] / month_cap * 100, 1) if month_cap else 0

    st.markdown("---")
    st.info(f"📌 **目前顯示對象：{exp_scope}** — 以下浪費總覽、時間軸、救援壓力與戰略判斷皆為此對象。")
    st.markdown("#### 🔝 本月浪費總覽")
    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("本月已成浪費率（過去日）", f"{past_wasted_pct}%")
    with c2:
        st.metric("已浪費秒數（Past）", seconds_to_spot_label(metrics["past_wasted_seconds"], short=True))
    with c3:
        st.metric("尚可救援秒數（Emergency）", seconds_to_spot_label(metrics["emergency_unused_seconds"], short=True))
    st.caption(f"尚可救援 = **{seconds_to_spot_label(metrics['emergency_unused_seconds'], short=True)}** 未賣（全省 15 秒檔）")

    st.markdown("#### 🧠 時間軸與未售庫存視覺化")
    past_sec = daily_inv[daily_inv["time_bucket"] == "past"]["unused_seconds"].sum()
    em_sec = daily_inv[daily_inv["time_bucket"] == "emergency"]["unused_seconds"].sum()
    buf_sec = daily_inv[daily_inv["time_bucket"] == "buffer"]["unused_seconds"].sum()
    total_unused = past_sec + em_sec + buf_sec or 1
    p_past = past_sec / total_unused * 100
    p_em = em_sec / total_unused * 100
    p_buf = buf_sec / total_unused * 100

    ndays = len(daily_inv)
    timeline_rows = []
    date_order_list = []
    for _, row in daily_inv.iterrows():
        d = row["date"]
        date_label = f"{d.month}/{d.day}"
        date_order_list.append(date_label)
        bucket = row["time_bucket"]
        label = "過去（浪費）" if bucket == "past" else ("緊急期（可救援）" if bucket == "emergency" else "緩衝期")
        timeline_rows.append({"日期": date_label, "bucket": bucket, "label": label})
    df_timeline = pd.DataFrame(timeline_rows)
    try:
        import altair as alt

        today_label = f"{today.month}/{today.day}" if (today.year == exp_year and today.month == exp_month) else None
        domain_bucket = ["past", "emergency", "buffer"]
        range_bucket = ["#c0392b", "#e67e22", "#27ae60"]
        chart_timeline = alt.Chart(df_timeline).mark_rect().encode(
            x=alt.X("日期:N", title="日期（月/日）", sort=date_order_list, axis=alt.Axis(labelFontSize=9, titleFontSize=10)),
            y=alt.value(0),
            y2=alt.value(60),
            color=alt.Color("bucket:N", title="區段", scale=alt.Scale(domain=domain_bucket, range=range_bucket), legend=alt.Legend(title="時間區段", labelFontSize=9, titleFontSize=10)),
            tooltip=[alt.Tooltip("日期:N", title="日期"), alt.Tooltip("label:N", title="說明")],
        ).properties(height=80, width=700, title=f"本月時間軸（{exp_month}月・左=月初 → 右=月底）")
        if today_label and today_label in date_order_list:
            rule_today = alt.Chart(pd.DataFrame([{"日期": today_label}])).mark_rule(color="white", strokeWidth=3).encode(x="日期:N")
            chart_timeline = alt.layer(chart_timeline, rule_today)
        st.altair_chart(chart_timeline, use_container_width=True)
        st.caption("🔴 今日為白線｜紅=已過（浪費）｜橙=緊急期（可救援）｜綠=緩衝期")
    except Exception:
        st.markdown(f"[🟥 Past 未售 {int(past_sec):,} 秒 ] [ 🟧 Emergency {int(em_sec):,} 秒 ] [ 🟩 Buffer {int(buf_sec):,} 秒 ]")
        st.caption("Past：已成浪費｜Emergency：可救援｜Buffer：可等待調度")

    try:
        import altair as alt

        df_bar = pd.DataFrame(
            [
                {"label_short": "過去浪費", "segment": "過去浪費（不可逆）", "秒數": int(past_sec), "pct": p_past, "order": 1},
                {"label_short": "緊急期可救援", "segment": "緊急期未售（可救援）", "秒數": int(em_sec), "pct": p_em, "order": 2},
                {"label_short": "緩衝期未售", "segment": "緩衝期未售", "秒數": int(buf_sec), "pct": p_buf, "order": 3},
            ]
        )
        df_bar = df_bar[df_bar["秒數"] > 0]
        if not df_bar.empty:
            label_order = ["過去浪費", "緊急期可救援", "緩衝期未售"]
            seg_range = ["#c0392b", "#e67e22", "#27ae60"]
            chart_bar = alt.Chart(df_bar).mark_bar(size=36).encode(
                x=alt.X("pct:Q", title="佔未售比例（%）", scale=alt.Scale(domain=[0, 100])),
                y=alt.Y("label_short:N", title="", sort=label_order, axis=alt.Axis(labelLimit=0, labelPadding=10)),
                color=alt.Color("label_short:N", scale=alt.Scale(domain=label_order, range=seg_range), legend=None),
                tooltip=[alt.Tooltip("segment:N", title="區段"), alt.Tooltip("秒數:Q", title="未售店秒", format=","), alt.Tooltip("pct:Q", title="比例%", format=".1f")],
            ).properties(height=180, title="未售庫存結構（可救援比例愈高愈需行動）").configure_axis(labelFontSize=12, labelLimit=0)
            st.altair_chart(chart_bar, use_container_width=True)
    except Exception:
        pass

    st.markdown("#### ⏱ 救援壓力與緊迫程度")
    rem = metrics["remaining_days"]
    req = metrics["required_daily_seconds"]
    emergency_total_days = min(emergency_days, len([d for d in daily_inv["date"] if d >= today and d <= today + timedelta(days=emergency_days)])) or emergency_days
    rem_ratio = rem / emergency_total_days if emergency_total_days else 0
    daily_cap = month_cap / ndays if ndays else 1
    req_vs_cap = min(1.0, (req / daily_cap)) if daily_cap else 0

    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("剩餘緊急期天數", f"{rem} 天")
        st.progress(rem_ratio, text="緊急期剩餘時間" if rem_ratio <= 0.5 else "尚有緩衝")
    with c2:
        st.metric("尚未售出（緊急期內）", seconds_to_spot_label(metrics["emergency_unused_seconds"], short=True))
        emergency_unused_ratio = metrics["emergency_unused_seconds"] / month_cap if month_cap else 0
        st.progress(min(1.0, emergency_unused_ratio), text="緊急期未售佔本月容量")
    with c3:
        st.metric("每日需賣", seconds_to_spot_label(req, short=True))
        st.progress(req_vs_cap, text="每日需賣 vs 日容量（愈滿愈吃緊）")
    st.markdown("**每日需賣：** ≈ **{:,}** 店秒/日（{}）".format(int(req), seconds_to_spot_label(req, short=True)))

    emergency_unused_sec = metrics["emergency_unused_seconds"]
    total_sellable_label = seconds_to_spot_label(emergency_unused_sec, short=True)
    daily_target_label = seconds_to_spot_label(req, short=True) if rem else "—"
    st.markdown("#### 📅 剩餘日子可賣多少")
    if rem > 0 and emergency_unused_sec > 0:
        try:
            import altair as alt

            emergency_dates = metrics.get("emergency_dates") or []
            target_per_day = req
            rows = []
            for d in sorted(emergency_dates):
                date_label = f"{d.month}/{d.day}" if hasattr(d, "month") else str(d)
                rows.append({"日期": date_label, "date_sort": d, "需賣": int(target_per_day), "需賣檔": round(target_per_day / seconds_per_spot_15s, 1)})
            if not rows:
                date_order = [today + timedelta(days=i) for i in range(rem)]
                rows = [{"日期": f"{d.month}/{d.day}", "date_sort": d, "需賣": int(target_per_day), "需賣檔": round(target_per_day / seconds_per_spot_15s, 1)} for d in date_order]
            df_days = pd.DataFrame(rows)
            date_order_str = df_days["日期"].tolist()
            chart_days = alt.Chart(df_days).mark_bar(color="#e67e22").encode(
                x=alt.X("日期:N", title="日期", sort=date_order_str),
                y=alt.Y("需賣:Q", title="店秒"),
                tooltip=[
                    alt.Tooltip("日期:N", title="日期"),
                    alt.Tooltip("需賣:Q", title="當日建議需賣(店秒)", format=","),
                    alt.Tooltip("需賣檔:Q", title="約檔(15秒)", format=".1f"),
                ],
            ).properties(height=220, title=f"依日期・每日建議需賣 ≈ {daily_target_label}")
            st.altair_chart(chart_days, use_container_width=True)
        except Exception:
            pass
        st.markdown(f"**未來 {rem} 天**內可售總量 = **{total_sellable_label}**｜每日目標 ≈ **{daily_target_label}**（與下方行動建議一致）")
    else:
        st.caption("緊急期內無剩餘天數或無未售量，無需補救目標。")

    st.markdown("#### 🚦 即時戰略判斷")
    state = metrics["strategy_state"]
    state_label = {"SELL": "強推補檔", "HOLD": "限制接案", "NORMAL": "正常銷售", "ANOMALY": "檢查假設"}[state]
    state_color = {"SELL": "#e74c3c", "HOLD": "#f39c12", "NORMAL": "#27ae60", "ANOMALY": "#9b59b6"}
    state_bg = state_color.get(state, "#95a5a6")
    st.markdown(
        f'<div style="background:{state_bg};color:white;padding:12px 20px;border-radius:8px;font-size:1.1em;margin:8px 0;">'
        f"🎯 當前戰略：<strong>{state}</strong> — {state_label}"
        f"</div>",
        unsafe_allow_html=True,
    )
    under_high = metrics["under_risk"] >= 0.5
    over_high = metrics["over_risk"] >= 0.5
    time_pressure_high = rem <= 3 and metrics["emergency_unused_seconds"] > 0
    risk_waste = min(1.0, metrics["under_risk"])
    risk_over = min(1.0, metrics["over_risk"])
    risk_time = 1.0 if time_pressure_high else (0.5 if rem <= 5 and metrics["emergency_unused_seconds"] > 0 else 0.0)
    r1, r2, r3 = st.columns(3)
    with r1:
        st.caption("浪費風險（未達目標使用率）")
        st.progress(risk_waste)
        st.caption("高" if under_high else "低")
    with r2:
        st.caption("爆量風險（超過安全上限）")
        st.progress(risk_over)
        st.caption("高" if over_high else "低")
    with r3:
        st.caption("時間壓力（緊急期內未售）")
        st.progress(risk_time)
        st.caption("高" if time_pressure_high else "中/低")

    st.markdown("#### 📌 行動建議")
    if state == "SELL":
        suggestions = ["15 秒短檔（區域）優先推", "舊客戶補檔／加購", "包量促銷或限時方案"]
    elif state == "HOLD":
        suggestions = ["暫緩新案接單", "以既有訂單消化為主", "觀察明日使用率再決定"]
    elif state == "ANOMALY":
        suggestions = ["檢查資料與假設是否正確", "確認容量設定與實際排程", "必要時人工覆核"]
    else:
        suggestions = ["維持正常銷售節奏", "留意緊急期內未售秒數", "可排日可彈性接案"]
    for i, s in enumerate(suggestions, 1):
        st.markdown(f"{i}. {s}")

    if metrics["emergency_unused_seconds"] > 0 and rem > 0 and rem <= 10:
        st.info(f"💬 **本月進入關鍵救援期，未來 {rem} 天為唯一補救窗口。**")
    st.caption("TWWI（時間加權浪費指數）= " + str(round(metrics["twwi"], 1)))

    with st.expander("📋 日粒度事實表（daily_inventory）", expanded=False):
        st.dataframe(styler_one_decimal(daily_inv), use_container_width=True, height=400)

