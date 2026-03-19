# -*- coding: utf-8 -*-
"""媒體秒數與採購分頁 UI 模組。"""

from __future__ import annotations

from datetime import datetime
from typing import Callable, Sequence

import streamlit as st


def render_media_purchase_tab(
    *,
    media_platform_options: Sequence[str],
    load_platform_monthly_purchase_all_media_for_year: Callable[[int], dict],
    set_platform_monthly_purchase: Callable[[str, int, int, int, float], None],
) -> None:
    st.markdown("### 📋 媒體秒數與採購")
    st.caption("輸入各媒體平台「一年 12 個月」的購買秒數與購買價格；儲存後會同步更新表3 的當月每日可用秒數，並供 ROI 分頁計算成本。")
    purchase_year = st.number_input("年度", min_value=2020, max_value=2030, value=datetime.now().year, key="purchase_year")
    existing = load_platform_monthly_purchase_all_media_for_year(purchase_year)

    for mp in media_platform_options:
        st.markdown(f"#### {mp}")
        data = existing.get(mp, {})
        cols = st.columns(12)
        inputs_sec = {}
        inputs_price = {}
        for m in range(1, 13):
            with cols[m - 1]:
                st.markdown(f"**{m}月**")
                sec, pr = data.get(m, (0, 0.0))
                key_sec = f"purchase_sec_{mp}_{m}"
                key_price = f"purchase_price_{mp}_{m}"
                default_sec = int(sec) if sec else 0
                default_price = float(pr) if pr else 0.0
                inputs_sec[m] = st.number_input(
                    "購買秒數",
                    min_value=0,
                    value=default_sec,
                    step=5000,
                    key=key_sec,
                )
                inputs_price[m] = st.number_input(
                    "購買價格（元）",
                    min_value=0.0,
                    value=default_price,
                    step=1000.0,
                    format="%.0f",
                    key=key_price,
                )
        if st.button(f"儲存 {mp}", key=f"save_purchase_{mp}"):
            for m in range(1, 13):
                set_platform_monthly_purchase(mp, purchase_year, m, inputs_sec[m], inputs_price[m])
            st.success(f"已儲存 {mp} {purchase_year} 年 1~12 月資料（並已同步表3 每日可用秒數）。")
            st.rerun()

    st.markdown("---")
    st.caption("儲存後，ROI 分頁將依「購買價格 ÷ 購買秒數」計算成本並產生投報率。")

