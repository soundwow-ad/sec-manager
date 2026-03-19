# -*- coding: utf-8 -*-
"""表1 分頁 UI 模組。"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Callable, Sequence

import numpy as np
import pandas as pd
import streamlit as st
import time

from services_utils import log_timing

from ui_order_crud import render_order_crud_panel


def render_table1_tab(
    *,
    db_mtime: int,
    df_orders: pd.DataFrame,
    df_seg_main: pd.DataFrame,
    custom_settings: dict,
    role: str,
    media_platform_options: Sequence[str],
    build_excel_table1_view: Callable[..., pd.DataFrame],
    styler_one_decimal: Callable[[pd.DataFrame], object],
    df_to_excel_bytes: Callable[..., bytes],
    get_db_connection: Callable[[], object],
    load_platform_settings: Callable[[], dict],
    build_ad_flight_segments: Callable[..., pd.DataFrame],
    compute_split_for_contract: Callable[[str], None],
    sync_sheets_if_enabled: Callable[..., None],
    mock_platform_raw: Sequence[str],
    mock_sales: Sequence[str],
    mock_company: Sequence[str],
    mock_seconds: Sequence[int],
    seconds_usage_types: Sequence[str],
) -> None:
    st.markdown("### 📋 表1－資料（訂單主表）")
    st.caption("此表對應 Excel：秒數管理表 → 表1-資料，為行政與業務對帳用之訂單主表。")

    # 商業定義：表1本來就是要以 Segments（排程/檔次段）為主體口徑。
    # 為避免 UI 語意錯置，移除切換勾選框，直接永遠使用 Segments。
    use_segments = True

    table1_default_index = 0  # 為了速度：預設先不產生日/日期欄位（更接近 1s 體感）
    view_mode = st.radio(
        "顯示模式",
        ["精簡", "行政", "完整"],
        format_func=lambda x: {
            "精簡": "① 精簡（業務：合約/客戶/平台/秒數/檔次/起訖/使用秒數）",
            "行政": "② 行政（+ 日期欄位、店數、委刊總檔數）",
            "完整": "③ 完整（全部欄位）",
        }[x],
        index=table1_default_index,
        horizontal=True,
        key="table1_view_mode",
    )
    include_daily_columns = view_mode != "精簡"

    cache_key = (db_mtime, use_segments, view_mode)

    if st.session_state.get("_table1_cache_key") == cache_key and "_table1_cache" in st.session_state:
        df_table1 = st.session_state["_table1_cache"]
    else:
        t0 = time.perf_counter()
        df_table1 = build_excel_table1_view(
            df_orders,
            custom_settings,
            use_segments=use_segments,
            df_segments=df_seg_main,
            include_daily_columns=include_daily_columns,
        )
        log_timing(
            "ui_table1.build_excel_table1_view",
            time.perf_counter() - t0,
            db_mtime=db_mtime,
            use_segments=use_segments,
            view_mode=view_mode,
            rows=len(df_table1) if isinstance(df_table1, pd.DataFrame) else 0,
            cols=len(df_table1.columns) if isinstance(df_table1, pd.DataFrame) else 0,
        )
        st.session_state["_table1_cache"] = df_table1
        st.session_state["_table1_cache_key"] = cache_key

    if df_table1.empty:
        st.warning("📭 尚無訂單資料")
        st.stop()

    if "實收金額" in df_table1.columns and "合約編號" in df_table1.columns:
        amount_display_mode = st.radio(
            "實收金額顯示",
            options=["依訂單列（每列顯示該筆訂單金額）", "依合約合併（每合約只顯示一筆總額於第一列）"],
            index=0,
            horizontal=True,
            key="table1_amount_display_mode",
        )
        if "依合約合併" in amount_display_mode:
            contract_total = df_table1.groupby("合約編號")["實收金額"].transform("sum")
            first_in_contract = ~df_table1.duplicated("合約編號", keep="first")
            df_table1 = df_table1.copy()
            df_table1["實收金額"] = np.where(first_in_contract, contract_total, 0)
            df_table1["除佣實收"] = df_table1["實收金額"]

    if "媒體平台" in df_table1.columns:
        st.markdown("#### 📺 媒體平台切換")
        platform_options = ["全部"] + [p for p in media_platform_options if p in df_table1["媒體平台"].unique().tolist()]
        if len(platform_options) == 1:
            platform_options = ["全部"] + list(media_platform_options)
        selected_platform = st.radio("選擇要顯示的媒體平台", options=platform_options, horizontal=True, key="table1_media_platform_filter")
        if selected_platform != "全部":
            df_table1 = df_table1[df_table1["媒體平台"] == selected_platform]
            if df_table1.empty:
                st.info(f"📭 媒體平台「{selected_platform}」目前沒有資料")
                st.stop()
    elif "平台分類" in df_table1.columns:
        st.markdown("#### 📺 平台篩選")
        platform_categories = ["全部", "全家新鮮視", "全家廣播", "家樂福", "診所", "其他"]
        selected_platform = st.radio("選擇要顯示的平台", options=platform_categories, horizontal=True, key="table1_platform_filter")
        if selected_platform != "全部":
            df_table1 = df_table1[df_table1["平台分類"] == selected_platform]
            if df_table1.empty:
                st.info(f"📭 平台「{selected_platform}」目前沒有資料")
                st.stop()

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        orders_count = len(df_orders)
        shown_rows = len(df_table1)
        st.metric("檔次段(segments)數", shown_rows)
        st.caption(f"來源 orders 筆數={orders_count}")
    with col2:
        st.metric("客戶數", df_table1["客戶"].nunique() if "客戶" in df_table1.columns else (df_table1["HYUNDAI_CUSTIN"].nunique() if "HYUNDAI_CUSTIN" in df_table1.columns else 0))
    with col3:
        if "媒體平台" in df_table1.columns:
            st.metric("媒體平台數", df_table1["媒體平台"].nunique())
        elif "平台分類" in df_table1.columns:
            st.metric("平台數", df_table1["平台分類"].nunique())
        else:
            st.metric("平台數", df_table1["平台"].nunique() if "平台" in df_table1.columns else 0)
    with col4:
        total_amount = df_table1["實收金額"].sum() if "實收金額" in df_table1.columns else 0
        st.metric("實收金額總計", f"{total_amount:,}")

    # 以商業口徑：Table1 永遠用 Segments 為主體，所以提供基於 segments 的秒數用途編輯。

    st.markdown("#### 🧩 Segments 秒數用途快速編輯（優先）")
    with st.expander("🔧 顯示／編輯尚未填寫 seconds_type 的 Segments", expanded=False):
        if df_seg_main is None or df_seg_main.empty:
            st.info("目前 segments 為空，無可編輯資料。")
        else:
            df_seg_editor = df_seg_main.copy()
            # 可能欄位不齊時的保護
            for col in ["segment_id", "seconds_type", "company", "sales", "client", "platform", "channel", "region"]:
                if col not in df_seg_editor.columns:
                    df_seg_editor[col] = ""

            only_missing = st.checkbox("只顯示尚未填寫 seconds_type", value=True, key="seg_missing_only")
            kw = st.text_input("關鍵字（segment_id / 公司 / 客戶 / 平台）", value="", key="seg_edit_kw").strip().lower()

            if only_missing:
                # 嚴謹口徑：尚未填寫 = seconds_type 為空/NULL（不使用「不在清單」推斷，避免誤判）
                seg_type_str = df_seg_editor["seconds_type"].fillna("").astype(str).str.strip()
                df_seg_editor = df_seg_editor[seg_type_str == ""]

            if kw:
                df_seg_editor = df_seg_editor[
                    df_seg_editor["segment_id"].astype(str).str.lower().str.contains(kw)
                    | df_seg_editor["company"].astype(str).str.lower().str.contains(kw)
                    | df_seg_editor["client"].astype(str).str.lower().str.contains(kw)
                    | df_seg_editor["platform"].astype(str).str.lower().str.contains(kw)
                ]

            st.caption(f"可編輯 segments 筆數：{len(df_seg_editor)}")
            show_df = df_seg_editor.head(200).copy()
            if not show_df.empty:
                show_df["segment_id_short"] = show_df["segment_id"].astype(str).str[:8]
                show_cols = [c for c in ["segment_id_short", "segment_id", "seconds_type", "company", "sales", "client", "platform", "region", "start_date", "end_date"] if c in show_df.columns]
                st.dataframe(
                    show_df[show_cols],
                    use_container_width=True,
                    hide_index=True,
                    height=320,
                )

            # 套用編輯：用 selectbox 選 segment_id（比整張 data_editor 更穩、也更快）
            if not df_seg_editor.empty:
                seg_id_options = df_seg_editor["segment_id"].astype(str).tolist()
                seg_id_selected = st.selectbox("選擇要編輯的 segment_id", seg_id_options, key="seg_edit_segment_id")
                current_stype = ""
                try:
                    cur_row = df_seg_editor[df_seg_editor["segment_id"].astype(str) == str(seg_id_selected)]
                    if not cur_row.empty:
                        current_stype = str(cur_row.iloc[0].get("seconds_type", "") or "")
                except Exception:
                    current_stype = ""

                new_seconds_type = st.selectbox(
                    "新的秒數用途(seconds_type)",
                    options=list(seconds_usage_types),
                    index=list(seconds_usage_types).index(current_stype) if current_stype in seconds_usage_types else 0,
                    key="seg_edit_new_seconds_type",
                )

                auto_sync = st.checkbox("套用後立即同步 Google Sheet", value=True, key="seg_edit_auto_sync")
                if st.button("套用並同步", type="primary", disabled=not seg_id_selected, key="seg_edit_apply_sync"):
                    now_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    conn_upd = get_db_connection()
                    try:
                        conn_upd.execute(
                            "UPDATE ad_flight_segments SET seconds_type=?, updated_at=? WHERE segment_id=?",
                            (new_seconds_type, now_ts, seg_id_selected),
                        )
                        conn_upd.commit()
                    except Exception as e:
                        conn_upd.rollback()
                        conn_upd.close()
                        st.error(f"Segments seconds_type 更新失敗：{e}")
                        st.stop()
                    conn_upd.close()

                    if auto_sync:
                        errs = sync_sheets_if_enabled(only_tables=["Segments"], skip_if_unchanged=False)
                        if errs:
                            st.error("Google Sheet 同步失敗：" + "; ".join(errs[:5]))
                    if "_table1_cache_key" in st.session_state:
                        del st.session_state["_table1_cache_key"]
                    st.success("✅ 已更新 segments 的 seconds_type。")
                    st.rerun()

    with st.expander("🔍 篩選條件", expanded=False):
        c1, c2, c3 = st.columns(3)
        with c1:
            sel_company = st.selectbox("公司", ["全部"] + sorted(df_table1["公司"].unique().tolist())) if "公司" in df_table1.columns else "全部"
        with c2:
            sel_sales = st.selectbox("業務", ["全部"] + sorted(df_table1["業務"].unique().tolist())) if "業務" in df_table1.columns else "全部"
        with c3:
            client_col_filter = "客戶" if "客戶" in df_table1.columns else "HYUNDAI_CUSTIN"
            sel_client = (
                st.selectbox("客戶", ["全部"] + sorted(df_table1[client_col_filter].dropna().unique().astype(str).tolist()))
                if client_col_filter in df_table1.columns
                else "全部"
            )

    df_filtered = df_table1.copy()
    if sel_company != "全部" and "公司" in df_filtered.columns:
        df_filtered = df_filtered[df_filtered["公司"] == sel_company]
    if sel_sales != "全部" and "業務" in df_filtered.columns:
        df_filtered = df_filtered[df_filtered["業務"] == sel_sales]
    client_col_filter = "客戶" if "客戶" in df_table1.columns else "HYUNDAI_CUSTIN"
    if sel_client != "全部" and client_col_filter in df_filtered.columns:
        df_filtered = df_filtered[df_filtered[client_col_filter].astype(str) == sel_client]

    client_col = "客戶" if "客戶" in df_filtered.columns else "HYUNDAI_CUSTIN"
    cols_simple = [c for c in ["業務", "合約編號", client_col, "媒體平台", "秒數", "每天總檔次", "起始日", "終止日", "使用總秒數"] if c in df_filtered.columns]
    date_cols_t1 = [c for c in df_filtered.columns if re.match(r"^\d{1,2}/\d{1,2}\([一二三四五六日]\)$", str(c))]
    cols_admin = cols_simple + [c for c in ["店數", "委刊總檔數"] if c in df_filtered.columns] + date_cols_t1
    if view_mode == "精簡":
        show_cols = cols_simple
    elif view_mode == "行政":
        show_cols = [c for c in cols_admin if c in df_filtered.columns]
    else:
        show_cols = list(df_filtered.columns)
    df_display = df_filtered[[c for c in show_cols if c in df_filtered.columns]]

    st.markdown("#### 📊 表1-資料（可橫向滾動查看完整欄位）")
    st.dataframe(styler_one_decimal(df_display), use_container_width=True, height=650)
    st.info(
        "💡 **提示**：此表格較寬，請使用橫向滾動查看完整內容。\n"
        "- 每日24小時檔次分配欄位（6-23, 0-1點）目前為預留，未來可從 CUE 表取得詳細資料\n"
        "- 月份欄位顯示該月每天的檔次數\n"
        "- 星期序列顯示走期內每天的星期標記"
    )

    st.markdown("#### 📥 下載資料")
    st.info("💡 **提示**：建議下載 Excel 格式以避免編碼問題。下載內容依目前顯示模式（精簡/行政/完整）。")
    c1, c2 = st.columns(2)
    with c1:
        excel_bytes = df_to_excel_bytes(df_display, sheet_name="表1-資料")
        st.download_button(
            label="📥 下載 Excel（推薦）",
            data=excel_bytes,
            file_name=f"表1_資料_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    with c2:
        csv = df_display.to_csv(index=False, encoding="utf-8-sig")
        st.download_button(
            label="📥 下載 CSV（備選）",
            data=csv,
            file_name=f"表1_資料_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv; charset=utf-8",
        )

    # 去掉 checkbox：避免「不勾什麼都不會出現」的體驗問題
    render_order_crud_panel(
        get_db_connection=get_db_connection,
        load_platform_settings=load_platform_settings,
        build_ad_flight_segments=build_ad_flight_segments,
        compute_split_for_contract=compute_split_for_contract,
        sync_sheets_if_enabled=sync_sheets_if_enabled,
        styler_one_decimal=styler_one_decimal,
        mock_platform_raw=mock_platform_raw,
        mock_sales=mock_sales,
        mock_company=mock_company,
        mock_seconds=mock_seconds,
        seconds_usage_types=seconds_usage_types,
    )

