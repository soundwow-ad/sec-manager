# -*- coding: utf-8 -*-
"""側欄：Ragic 區間匯入 UI。"""

from __future__ import annotations

from datetime import date, timedelta
import time
from typing import Callable
import re

import streamlit as st


def render_sidebar_ragic_import(
    *,
    import_ragic_to_orders_by_date_range: Callable[..., tuple[bool, str, str, str]],
) -> None:
    last = st.session_state.get("_ragic_import_last_summary")
    if isinstance(last, dict):
        ok = bool(last.get("ok"))
        msg = str(last.get("msg", ""))
        batch_id = str(last.get("batch_id", ""))
        elapsed = float(last.get("elapsed_sec", 0) or 0)
        if ok:
            st.sidebar.success(msg or "Ragic 區間匯入完成")
        else:
            st.sidebar.error(msg or "Ragic 區間匯入失敗")
        if batch_id:
            st.sidebar.caption(f"Batch: `{batch_id}`")
        if elapsed > 0:
            st.sidebar.caption(f"耗時：約 {elapsed:.1f} 秒")
        m = re.search(r"新增\s*(\d+).*更新\s*(\d+).*略過\s*(\d+)", msg)
        if m:
            c1, c2, c3 = st.sidebar.columns(3)
            c1.metric("新增", m.group(1))
            c2.metric("更新", m.group(2))
            c3.metric("略過", m.group(3))

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
        # Streamlit widget key 一旦存在，value 不會覆蓋；這裡在空值時主動補預設。
        if not str(st.session_state.get("ragic_import_api_key", "")).strip():
            st.session_state["ragic_import_api_key"] = str(api_default).strip()
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
        st.caption("匯入策略：不清空舊資料，僅新增/更新有變動的列。")
        if st.button("📥 匯入 Ragic 區間資料", key="btn_ragic_import_range"):
            if ragic_date_from > ragic_date_to:
                st.error("日期區間錯誤：起日不可大於迄日")
            elif not (ragic_import_url or "").strip():
                st.error("請輸入 Ragic 表單網址")
            elif not (ragic_import_api_key or "").strip():
                st.error("請輸入 Ragic API Key")
            else:
                p = st.sidebar.progress(0, text="匯入進度：準備中")
                status_box = st.sidebar.empty()
                t0 = time.perf_counter()

                def _on_progress(evt: dict) -> None:
                    stage = str(evt.get("stage", "") or "")
                    msg_txt = str(evt.get("message", "") or "")
                    if stage == "fetch_page":
                        p.progress(12, text=f"匯入進度：{msg_txt}")
                    elif stage == "filter_done":
                        p.progress(20, text=f"匯入進度：{msg_txt}")
                    elif stage == "entry_start":
                        idx = int(evt.get("entry_index", 1) or 1)
                        total = max(int(evt.get("entry_total", 1) or 1), 1)
                        frac = idx / total
                        pct = int(20 + frac * 60)  # 20%~80%
                        p.progress(min(80, max(20, pct)), text=f"匯入進度：{msg_txt}")
                    elif stage in ("file_download_start", "file_parse_start", "file_parse_done"):
                        idx = int(evt.get("entry_index", 1) or 1)
                        total = max(int(evt.get("entry_total", 1) or 1), 1)
                        base = 20 + int((idx - 1) / total * 60)
                        p.progress(min(80, max(20, base)), text=f"匯入進度：{msg_txt}")
                    elif stage == "db_write_start":
                        p.progress(85, text=f"匯入進度：{msg_txt}")
                    elif stage == "segments_built":
                        p.progress(92, text=f"匯入進度：{msg_txt}")
                    elif stage == "done":
                        p.progress(100, text="匯入進度：完成")
                    if msg_txt:
                        status_box.caption(f"目前動作：{msg_txt}")

                with st.spinner("正在從 Ragic 匯入資料（抓取、下載 Excel、解析、寫入）..."):
                    p.progress(10, text="匯入進度：抓取 Ragic 列表")
                    ok, msg, batch_id, detail_report = import_ragic_to_orders_by_date_range(
                        ragic_url=ragic_import_url.strip(),
                        api_key=ragic_import_api_key.strip(),
                        date_from=ragic_date_from,
                        date_to=ragic_date_to,
                        date_field=ragic_date_field,
                        replace_existing=False,
                        progress_cb=_on_progress,
                    )
                    p.progress(100, text="匯入進度：完成")
                    elapsed = time.perf_counter() - t0
                    st.session_state["_ragic_last_batch_id"] = batch_id
                    st.session_state["_ragic_import_last_summary"] = {
                        "ok": ok,
                        "msg": msg,
                        "batch_id": batch_id,
                        "elapsed_sec": elapsed,
                    }
                    if ok:
                        st.success(msg)
                        time.sleep(0.3)
                        st.rerun()
                    else:
                        st.error(msg)

