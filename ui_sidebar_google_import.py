# -*- coding: utf-8 -*-
"""側欄：Google 試算表匯入。"""

from __future__ import annotations

import time
import streamlit as st


def render_sidebar_google_import(*, import_google_sheet_to_orders):
    st.sidebar.markdown("### 📊 資料來源")
    with st.sidebar.expander("📥 匯入 Google 試算表（表1結構）", expanded=False):
        st.caption("貼上試算表網址或 ID，結構需含：平台、起始日、終止日、秒數、每天總檔次、客戶(HYUNDAI_CUSTIN)、素材、業務、公司、合約編號、實收金額、秒數用途等。")
        gs_url = st.text_input(
            "試算表網址或 ID",
            value="https://docs.google.com/spreadsheets/d/1x2cboM_xmB7nl9aA12O633BzmvPNyJnZoqPipOQhVY4/edit?usp=sharing",
            placeholder="https://docs.google.com/spreadsheets/d/xxx/edit 或 貼上 ID",
            key="gs_import_url",
        )
        gs_replace = st.checkbox("匯入時取代現有資料", value=True, key="gs_replace")
        if st.button("📥 匯入（表1結構）", key="gs_import_btn"):
            if not (gs_url or "").strip():
                st.warning("請輸入試算表網址或 ID")
            else:
                with st.spinner("正在讀取試算表並匯入..."):
                    success, msg = import_google_sheet_to_orders(gs_url.strip(), replace_existing=gs_replace)
                    if success:
                        st.success(msg)
                        time.sleep(0.5)
                        st.rerun()
                    else:
                        st.error(msg)

