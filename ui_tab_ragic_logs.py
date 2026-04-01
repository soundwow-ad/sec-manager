# -*- coding: utf-8 -*-
"""Ragic 匯入紀錄分頁 UI 模組。"""

from __future__ import annotations

from datetime import datetime
from typing import Callable

import streamlit as st


def render_ragic_logs_tab(
    *,
    get_ragic_import_logs: Callable[..., object],
    styler_one_decimal: Callable[[object], object],
) -> None:
    st.markdown("### 🧾 Ragic 匯入紀錄")
    st.caption("顯示每次 Ragic 區間匯入的詳細成功/失敗紀錄（抓取、下載、解析、寫入）。")
    last_batch = st.session_state.get("_ragic_last_batch_id")
    if last_batch:
        st.info(f"最近一次批次：`{last_batch}`")

    logs = get_ragic_import_logs(limit=3000)
    if logs.empty:
        st.info("目前尚無 Ragic 匯入紀錄。")
        return

    c1, c2, c3 = st.columns(3)
    with c1:
        batch_opts = ["全部"] + sorted(logs["batch_id"].dropna().astype(str).unique().tolist())
        sel_batch = st.selectbox("批次", batch_opts, index=0, key="ragic_log_batch")
    with c2:
        status_opts = ["全部"] + sorted(logs["status"].dropna().astype(str).unique().tolist())
        sel_status = st.selectbox("狀態", status_opts, index=0, key="ragic_log_status")
    with c3:
        phase_opts = ["全部"] + sorted(logs["phase"].dropna().astype(str).unique().tolist())
        sel_phase = st.selectbox("階段", phase_opts, index=0, key="ragic_log_phase")

    f = logs.copy()
    if sel_batch != "全部":
        f = f[f["batch_id"].astype(str) == sel_batch]
    if sel_status != "全部":
        f = f[f["status"].astype(str) == sel_status]
    if sel_phase != "全部":
        f = f[f["phase"].astype(str) == sel_phase]

    st.dataframe(styler_one_decimal(f), use_container_width=True, height=520, hide_index=True)
    if not f.empty:
        st.markdown("#### 🔎 紀錄詳情")
        f_view = f.reset_index(drop=True).copy()
        options = list(range(len(f_view)))

        def _label(i: int) -> str:
            r = f_view.iloc[i]
            return (
                f"{r.get('created_at', '')} | "
                f"batch={r.get('batch_id', '')} | "
                f"status={r.get('status', '')} | "
                f"phase={r.get('phase', '')}"
            )

        idx = st.selectbox("選擇一筆紀錄", options=options, format_func=_label, key="ragic_log_detail_idx")
        row = f_view.iloc[int(idx)]
        st.caption(
            f"batch_id=`{row.get('batch_id', '')}` | status=`{row.get('status', '')}` | "
            f"phase=`{row.get('phase', '')}` | ragic_id=`{row.get('ragic_id', '')}`"
        )
        msg = str(row.get("message", "") or "")
        if msg:
            st.code(msg, language="text")
        else:
            st.info("此筆無 message 內容。")

        with st.expander("顯示同批次完整時間序", expanded=False):
            batch_id = str(row.get("batch_id", "") or "")
            if batch_id:
                b = logs[logs["batch_id"].astype(str) == batch_id].copy()
                st.dataframe(styler_one_decimal(b), use_container_width=True, height=300, hide_index=True)
            else:
                st.info("此筆沒有 batch_id。")
    st.download_button(
        "📥 下載匯入紀錄 CSV",
        data=f.to_csv(index=False, encoding="utf-8-sig"),
        file_name=f"ragic_import_logs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv; charset=utf-8",
        key="dl_ragic_import_logs_csv",
    )

