# -*- coding: utf-8 -*-
"""App UI 殼層：登入、側欄、資料載入與分頁掛載。"""

from __future__ import annotations

import time

import streamlit as st

from services_utils import log_timing


def run_app_shell(
    *,
    init_db,
    get_db_connection,
    auth_verify,
    auth_change_password,
    auth_list_users,
    auth_create_user,
    auth_delete_user,
    sync_sheets_if_enabled,
    import_google_sheet_to_orders,
    import_ragic_to_orders_by_date_range,
    import_ragic_single_entry_to_orders,
    load_platform_settings,
    load_orders_cached,
    load_segments_cached,
    explode_segments_to_daily_cached,
    explode_segments_to_daily_cached_by_db_mtime,
    build_ad_flight_segments,
    render_tab3,
    render_main_tabs,
    # data/config
    roles,
    db_file,
    platform_capacity,
    get_store_count,
    save_platform_settings,
    media_platform_options,
    annual_summary_entity_labels,
    emergency_days,
    seconds_per_spot_15s,
    ragic_fields,
    ragic_subtable_fields,
    # business callbacks for tabs
    compute_split_for_contract,
    styler_one_decimal,
    df_to_excel_bytes,
    build_excel_table1_view,
    build_table2_summary_by_company,
    build_table2_details_by_company,
    get_platform_monthly_capacity,
    build_annual_seconds_summary,
    display_monthly_table_split,
    build_visualization_summary_pdf,
    build_visualization_summary_excel,
    load_platform_monthly_purchase_all_media_for_year,
    set_platform_monthly_purchase,
    get_ragic_import_logs,
    parse_cue_excel_for_table1,
    build_daily_inventory_and_metrics,
    seconds_to_spot_label,
    calculate_roi_by_period,
    get_roi_all_period_date_range,
    mock_platform_raw,
    mock_sales,
    mock_company,
    mock_seconds,
    seconds_usage_types,
) -> None:
    init_db()

    if "user" not in st.session_state or st.session_state.get("user") is None:
        st.markdown("### 🔐 登入")
        st.caption("請輸入帳號與密碼。（測試用：已預填行政主管 admin / admin123）")
        with st.form("login_form"):
            login_user = st.text_input("帳號", value="admin", placeholder="username")
            login_pass = st.text_input("密碼", type="password", value="admin123", placeholder="password")
            if st.form_submit_button("登入"):
                u = auth_verify(login_user, login_pass)
                if u:
                    st.session_state["user"] = u
                    st.success("登入成功")
                    st.rerun()
                else:
                    st.error("帳號或密碼錯誤")
        st.stop()

    user = st.session_state["user"]
    role = user["role"]

    from ui_sidebar_account import render_sidebar_account
    from ui_sidebar_google_import import render_sidebar_google_import
    from ui_sidebar_ragic_import import render_sidebar_ragic_import
    from ui_sidebar_admin import render_sidebar_admin
    from app_runtime_data import load_runtime_data

    render_sidebar_account(
        user=user,
        role=role,
        roles=roles,
        auth_verify=auth_verify,
        auth_change_password=auth_change_password,
        auth_list_users=auth_list_users,
        auth_create_user=auth_create_user,
        auth_delete_user=auth_delete_user,
    )
    render_sidebar_google_import(import_google_sheet_to_orders=import_google_sheet_to_orders)
    render_sidebar_ragic_import(import_ragic_to_orders_by_date_range=import_ragic_to_orders_by_date_range)
    render_sidebar_admin(
        get_db_connection=get_db_connection,
        init_db=init_db,
        db_file=db_file,
        get_store_count=get_store_count,
        load_platform_settings=load_platform_settings,
        platform_capacity=platform_capacity,
        save_platform_settings=save_platform_settings,
        sync_sheets_if_enabled=sync_sheets_if_enabled,
    )

    t0 = time.perf_counter()
    runtime = load_runtime_data(
        db_file=db_file,
        load_platform_settings=load_platform_settings,
        load_orders_cached=load_orders_cached,
        load_segments_cached=load_segments_cached,
        explode_segments_to_daily_cached=explode_segments_to_daily_cached,
        build_ad_flight_segments=build_ad_flight_segments,
    )
    log_timing("app_shell.load_runtime_data_total", time.perf_counter() - t0, db_file=db_file)
    db_mtime = runtime["db_mtime"]
    custom_settings = runtime["custom_settings"]
    df_orders = runtime["df_orders"]
    df_seg_main = runtime["df_seg_main"]
    df_daily = runtime["df_daily"]

    render_main_tabs(
        role=role,
        db_mtime=db_mtime,
        df_orders=df_orders,
        df_seg_main=df_seg_main,
        df_daily=df_daily,
        custom_settings=custom_settings,
        media_platform_options=media_platform_options,
        annual_summary_entity_labels=annual_summary_entity_labels,
        emergency_days=emergency_days,
        seconds_per_spot_15s=seconds_per_spot_15s,
        ragic_fields=ragic_fields,
        ragic_subtable_fields=ragic_subtable_fields,
        import_ragic_single_entry_to_orders=import_ragic_single_entry_to_orders,
        render_tab3=render_tab3,
        get_db_connection=get_db_connection,
        load_platform_settings=load_platform_settings,
        build_ad_flight_segments=build_ad_flight_segments,
        compute_split_for_contract=compute_split_for_contract,
        sync_sheets_if_enabled=sync_sheets_if_enabled,
        styler_one_decimal=styler_one_decimal,
        df_to_excel_bytes=df_to_excel_bytes,
        build_excel_table1_view=build_excel_table1_view,
        load_segments_cached=load_segments_cached,
        explode_segments_to_daily_cached_by_db_mtime=explode_segments_to_daily_cached_by_db_mtime,
        build_table2_summary_by_company=build_table2_summary_by_company,
        build_table2_details_by_company=build_table2_details_by_company,
        get_platform_monthly_capacity=get_platform_monthly_capacity,
        build_annual_seconds_summary=build_annual_seconds_summary,
        display_monthly_table_split=display_monthly_table_split,
        build_visualization_summary_pdf=build_visualization_summary_pdf,
        build_visualization_summary_excel=build_visualization_summary_excel,
        load_platform_monthly_purchase_all_media_for_year=load_platform_monthly_purchase_all_media_for_year,
        set_platform_monthly_purchase=set_platform_monthly_purchase,
        get_ragic_import_logs=get_ragic_import_logs,
        parse_cue_excel_for_table1=parse_cue_excel_for_table1,
        build_daily_inventory_and_metrics=build_daily_inventory_and_metrics,
        seconds_to_spot_label=seconds_to_spot_label,
        calculate_roi_by_period=calculate_roi_by_period,
        get_roi_all_period_date_range=get_roi_all_period_date_range,
        mock_platform_raw=mock_platform_raw,
        mock_sales=mock_sales,
        mock_company=mock_company,
        mock_seconds=mock_seconds,
        seconds_usage_types=seconds_usage_types,
    )

