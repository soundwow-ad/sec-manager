# -*- coding: utf-8 -*-
"""側欄：Ragic 區間匯入 UI。"""

from __future__ import annotations

from datetime import date, timedelta
import time
from typing import Callable

import streamlit as st


def render_sidebar_ragic_import(
    *,
    import_ragic_to_orders_by_date_range: Callable[..., tuple[bool, str, str]],
) -> None:
    with st.sidebar.expander("📥 匯入 Ragic（日期區間）", expanded=False):
        st.caption("可依 Ragic 指定日期欄位篩選區間，匯入該期間所有可解析的 CUE Excel。")
        ragic_url_default = "https://ap13.ragic.com/soundwow/forms12/17"
        ragic_import_url = st.text_input(
            "Ragic 表單網址",
            value=ragic_url_default,
            key="ragic_import_url",
            placeholder="https://ap13.ragic.com/soundwow/forms12/17",
        )
        # 方便測試：若 secrets 未配置，先帶入暫時預設 key（之後可移除）
        api_default = "MEwyTEExWHJQamRDalZ6N0hzQ2syZlBHNUNJeWhwZFBrM3BMM2tDRWd4aGIvZ1JxWTlYaGkyM0RoRmo1ZExHaA=="
        try:
            api_default = (
                st.secrets.get("RAGIC_API_KEY")
                or st.secrets.get("ragic", {}).get("api_key")
                or st.secrets.get("RAGIC", {}).get("api_key")
                or api_default
            )
        except Exception:
            pass
        ragic_import_api_key = st.text_input("Ragic API Key", value=api_default, type="password", key="ragic_import_api_key")
        ragic_date_field = st.selectbox(
            "日期欄位",
            options=["建立日期", "執行開始日期", "執行結束日期"],
            index=0,
            key="ragic_import_date_field",
        )
        d1, d2 = st.columns(2)
        with d1:
            ragic_date_from = st.date_input("起日", value=date.today() - timedelta(days=30), key="ragic_import_date_from")
        with d2:
            ragic_date_to = st.date_input("迄日", value=date.today(), key="ragic_import_date_to")
        ragic_replace = st.checkbox("匯入時取代現有資料", value=False, key="ragic_import_replace")
        if st.button("📥 匯入 Ragic 區間資料", key="btn_ragic_import_range"):
            if ragic_date_from > ragic_date_to:
                st.error("日期區間錯誤：起日不可大於迄日")
            elif not (ragic_import_url or "").strip():
                st.error("請輸入 Ragic 表單網址")
            elif not (ragic_import_api_key or "").strip():
                st.error("請輸入 Ragic API Key")
            else:
                with st.spinner("正在從 Ragic 匯入資料（抓取、下載 Excel、解析、寫入）..."):
                    ok, msg, batch_id = import_ragic_to_orders_by_date_range(
                        ragic_url=ragic_import_url.strip(),
                        api_key=ragic_import_api_key.strip(),
                        date_from=ragic_date_from,
                        date_to=ragic_date_to,
                        date_field=ragic_date_field,
                        replace_existing=ragic_replace,
                    )
                    st.session_state["_ragic_last_batch_id"] = batch_id
                    if ok:
                        st.success(msg)
                        time.sleep(0.3)
                        st.rerun()
                    else:
                        st.error(msg)

