# -*- coding: utf-8 -*-
"""主畫面分頁導覽與分發。"""

from __future__ import annotations

from typing import Callable, Sequence

import streamlit as st


TAB_OPTIONS = ["📋 表1-資料", "📅 表2-秒數明細", "📊 表3-每日庫存", "📉 總結表圖表", "📊 分公司×媒體 每月秒數", "📋 媒體秒數與採購", "📊 ROI", "🧾 Ragic匯入紀錄", "🧪 Ragic抓取測試", "🧪 實驗分頁"]
TAB_OPTIONS_BY_ROLE = {
    "行政主管": TAB_OPTIONS,
    "業務": ["📋 表1-資料", "📊 表3-每日庫存"],
    "總經理": ["📉 總結表圖表", "📊 表3-每日庫存", "📅 表2-秒數明細", "📊 分公司×媒體 每月秒數", "📊 ROI", "🧾 Ragic匯入紀錄", "🧪 實驗分頁"],
}


def render_main_tabs(
    *,
    role: str,
    db_mtime: int,
    df_orders,
    df_seg_main,
    df_daily,
    custom_settings: dict,
    media_platform_options: Sequence[str],
    annual_summary_entity_labels: Sequence[str],
    emergency_days: int,
    seconds_per_spot_15s: int,
    ragic_fields: dict,
    ragic_subtable_fields: dict,
    # callbacks
    render_tab3: Callable[[], None],
    get_db_connection: Callable[[], object],
    load_platform_settings: Callable[[], dict],
    build_ad_flight_segments: Callable[..., object],
    compute_split_for_contract: Callable[[str], None],
    sync_sheets_if_enabled: Callable[..., None],
    styler_one_decimal: Callable[..., object],
    df_to_excel_bytes: Callable[..., bytes],
    build_excel_table1_view: Callable[..., object],
    load_segments_cached: Callable[..., object],
    build_table2_summary_by_company: Callable[..., object],
    build_table2_details_by_company: Callable[..., object],
    get_platform_monthly_capacity: Callable[..., object],
    build_annual_seconds_summary: Callable[..., object],
    display_monthly_table_split: Callable[..., object],
    build_visualization_summary_pdf: Callable[..., bytes],
    build_visualization_summary_excel: Callable[..., bytes],
    load_platform_monthly_purchase_all_media_for_year: Callable[..., object],
    set_platform_monthly_purchase: Callable[..., object],
    get_ragic_import_logs: Callable[..., object],
    parse_cue_excel_for_table1: Callable[..., object],
    build_daily_inventory_and_metrics: Callable[..., object],
    seconds_to_spot_label: Callable[..., str],
    calculate_roi_by_period: Callable[..., object],
    get_roi_all_period_date_range: Callable[..., object],
    # constants
    mock_platform_raw,
    mock_sales,
    mock_company,
    mock_seconds,
    seconds_usage_types,
) -> None:
    role_label = {"行政主管": "🗂 行政主管", "業務": "🧑‍💼 業務", "總經理": "👔 總經理"}.get(role, role)
    st.markdown(f"#### 目前身份：{role_label}")
    tab_options_for_role = TAB_OPTIONS_BY_ROLE.get(role, TAB_OPTIONS)
    default_tab = "🧾 Ragic匯入紀錄" if "🧾 Ragic匯入紀錄" in tab_options_for_role else tab_options_for_role[0]
    current_tab = st.session_state.get("main_tab", default_tab)
    if current_tab not in tab_options_for_role:
        st.session_state["main_tab"] = default_tab

    st.markdown("---")
    st.markdown("### 選擇分頁")
    selected_tab = st.session_state.get("main_tab", tab_options_for_role[0])
    tab_cols = st.columns(len(tab_options_for_role))
    for i, tab in enumerate(tab_options_for_role):
        with tab_cols[i]:
            is_selected = selected_tab == tab
            if st.button(tab, key=f"tab_btn_{i}", type="primary" if is_selected else "secondary", use_container_width=True):
                st.session_state["main_tab"] = tab
                st.rerun()
    st.markdown("---")

    if selected_tab == "📋 表1-資料":
        from ui_tab_table1 import render_table1_tab

        render_table1_tab(
            db_mtime=db_mtime,
            df_orders=df_orders,
            df_seg_main=df_seg_main,
            custom_settings=custom_settings,
            role=role,
            media_platform_options=media_platform_options,
            build_excel_table1_view=build_excel_table1_view,
            styler_one_decimal=styler_one_decimal,
            df_to_excel_bytes=df_to_excel_bytes,
            get_db_connection=get_db_connection,
            load_platform_settings=load_platform_settings,
            build_ad_flight_segments=build_ad_flight_segments,
            compute_split_for_contract=compute_split_for_contract,
            sync_sheets_if_enabled=sync_sheets_if_enabled,
            mock_platform_raw=mock_platform_raw,
            mock_sales=mock_sales,
            mock_company=mock_company,
            mock_seconds=mock_seconds,
            seconds_usage_types=seconds_usage_types,
        )
    elif selected_tab == "📅 表2-秒數明細":
        from ui_tab_table2 import render_table2_tab

        render_table2_tab(
            db_mtime=db_mtime,
            df_daily=df_daily,
            df_orders=df_orders,
            load_segments_cached=load_segments_cached,
            build_table2_summary_by_company=build_table2_summary_by_company,
            build_table2_details_by_company=build_table2_details_by_company,
            styler_one_decimal=styler_one_decimal,
        )
    elif selected_tab == "📊 表3-每日庫存":
        render_tab3()
    elif selected_tab == "📉 總結表圖表":
        from ui_tab_summary_viz import render_summary_viz_tab

        render_summary_viz_tab(
            df_daily=df_daily,
            annual_summary_entity_labels=annual_summary_entity_labels,
            get_platform_monthly_capacity=get_platform_monthly_capacity,
            build_annual_seconds_summary=build_annual_seconds_summary,
            display_monthly_table_split=display_monthly_table_split,
            build_visualization_summary_pdf=build_visualization_summary_pdf,
            build_visualization_summary_excel=build_visualization_summary_excel,
        )
    elif selected_tab == "📊 分公司×媒體 每月秒數":
        from ui_tab_branch_media import render_branch_media_tab

        render_branch_media_tab(df_daily=df_daily, styler_one_decimal=styler_one_decimal)
    elif selected_tab == "📋 媒體秒數與採購":
        from ui_tab_media_purchase import render_media_purchase_tab

        render_media_purchase_tab(
            media_platform_options=media_platform_options,
            load_platform_monthly_purchase_all_media_for_year=load_platform_monthly_purchase_all_media_for_year,
            set_platform_monthly_purchase=set_platform_monthly_purchase,
        )
    elif selected_tab == "🧾 Ragic匯入紀錄":
        from ui_tab_ragic_logs import render_ragic_logs_tab

        render_ragic_logs_tab(get_ragic_import_logs=get_ragic_import_logs, styler_one_decimal=styler_one_decimal)
    elif selected_tab == "🧪 Ragic抓取測試":
        from ui_tab_ragic_test_entry import render_ragic_test_entry

        render_ragic_test_entry(
            ragic_fields=ragic_fields,
            ragic_subtable_fields=ragic_subtable_fields if ragic_subtable_fields else {},
            parse_cue_excel_for_table1=parse_cue_excel_for_table1,
        )
    elif selected_tab == "🧪 實驗分頁":
        from ui_tab_experiment import render_experiment_tab

        render_experiment_tab(
            df_daily=df_daily,
            media_platform_options=media_platform_options,
            emergency_days_default=emergency_days,
            seconds_per_spot_15s=seconds_per_spot_15s,
            get_platform_monthly_capacity=get_platform_monthly_capacity,
            build_daily_inventory_and_metrics=build_daily_inventory_and_metrics,
            seconds_to_spot_label=seconds_to_spot_label,
            styler_one_decimal=styler_one_decimal,
        )
    elif selected_tab == "📊 ROI":
        from ui_tab_roi import render_roi_tab

        render_roi_tab(
            calculate_roi_by_period=calculate_roi_by_period,
            get_roi_all_period_date_range=get_roi_all_period_date_range,
            styler_one_decimal=styler_one_decimal,
        )

