# -*- coding: utf-8 -*-
"""側欄：管理工具與平台設定。"""

from __future__ import annotations

import os
import time
from typing import Callable

import pandas as pd
import streamlit as st


def render_sidebar_admin(
    *,
    get_db_connection: Callable[[], object],
    db_file: str,
    get_store_count: Callable[[str, dict], int],
    load_platform_settings: Callable[[], dict],
    platform_capacity: dict,
    save_platform_settings: Callable[[str, int, int], None],
) -> None:
    st.sidebar.markdown("---")
    if st.sidebar.button("🧨 重置資料庫（刪除並重建）", help="⚠️ 警告：這會刪除所有現有資料"):
        try:
            conn = get_db_connection()
            conn.close()
            if os.path.exists(db_file):
                os.remove(db_file)
                st.sidebar.success("✅ 已刪除資料庫，將重新初始化")
                time.sleep(1)
                st.rerun()
            else:
                st.sidebar.info("資料庫檔案不存在，無需刪除")
        except Exception as e:
            st.sidebar.error(f"❌ 刪除失敗: {e}")

    st.sidebar.markdown("---")
    st.sidebar.markdown("### 📝 平台設定")
    with st.sidebar.expander("設定平台店數與營業時間"):
        conn = get_db_connection()
        platforms = pd.read_sql("SELECT DISTINCT platform FROM orders", conn)
        conn.close()

        if not platforms.empty:
            custom_settings = load_platform_settings()
            sel_platform = st.selectbox("選擇平台", platforms["platform"].tolist())
            current_store = get_store_count(sel_platform, custom_settings)
            current_hours = platform_capacity.get(sel_platform, 18)
            if custom_settings and sel_platform in custom_settings:
                current_hours = custom_settings[sel_platform]["daily_hours"]

            new_store = st.number_input("店數", min_value=1, value=int(current_store), step=1)
            new_hours = st.number_input("每日營業小時數", min_value=1, max_value=24, value=int(current_hours), step=1)

            if st.button("💾 儲存設定"):
                save_platform_settings(sel_platform, new_store, new_hours)
                st.success("設定已儲存！")
                st.rerun()
        else:
            st.info("請先新增訂單或匯入資料")

