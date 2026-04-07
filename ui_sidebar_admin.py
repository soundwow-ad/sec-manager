# -*- coding: utf-8 -*-
"""側欄：管理工具與平台設定。"""

from __future__ import annotations

from typing import Callable

import pandas as pd
import streamlit as st


def render_sidebar_admin(
    *,
    get_db_connection: Callable[[], object],
    init_db: Callable[[], None],
    db_file: str,
    get_store_count: Callable[[str, dict], int],
    load_platform_settings: Callable[[], dict],
    platform_capacity: dict,
    save_platform_settings: Callable[[str, int, int], None],
    sync_sheets_if_enabled: Callable[..., object],
) -> None:
    st.sidebar.markdown("---")
    with st.sidebar.expander("☁️ Google Sheet 資料庫（簡易）", expanded=False):
        st.caption("只保留兩個核心動作：寫入（覆蓋 Sheet）與讀取（覆蓋本地 DB）。")
        sync_business_only = st.checkbox("僅同步業務資料（不含 Users）", value=True, key="gs_sync_business_only")
        try:
            from sheets_backend import (
                is_sheets_enabled,
                get_sheets_url,
                sync_db_to_sheets,
                load_all_from_sheets_into_db,
            )

            if not is_sheets_enabled():
                st.warning("Google Sheet 未啟用或設定不完整。")
            else:
                sheets_url = get_sheets_url()
                if sheets_url:
                    st.link_button("🔗 開啟 Google Sheet", sheets_url, use_container_width=True)

                if st.button("⬆️ 存入 Google Sheet（完全覆蓋）", key="btn_export_db_to_sheets"):
                    with st.spinner("正在寫入 Google Sheet..."):
                        only_tables = ["Orders", "Segments", "PlatformSettings", "Capacity", "Purchase"] if sync_business_only else None
                        errs = sync_db_to_sheets(
                            get_db_connection,
                            only_tables=only_tables,
                            skip_if_unchanged=False,
                        )
                    if errs:
                        st.error("寫入失敗：" + "; ".join(errs[:5]))
                    else:
                        if sync_business_only:
                            st.success("已將業務資料寫入 Google Sheet（Users 未同步）。")
                        else:
                            st.success("已將目前程式資料完整寫入 Google Sheet。")

                if st.button("⬇️ 讀取 Google Sheet（覆蓋本地資料）", key="btn_import_sheets_to_db"):
                    users_backup_df = pd.DataFrame()
                    if sync_business_only:
                        try:
                            conn_u = get_db_connection()
                            users_backup_df = pd.read_sql("SELECT id, username, password_hash, role, created_at FROM users", conn_u)
                            conn_u.close()
                        except Exception:
                            users_backup_df = pd.DataFrame()
                    with st.spinner("正在從 Google Sheet 載入資料..."):
                        errs = load_all_from_sheets_into_db(get_db_connection, init_db)
                    if sync_business_only:
                        # 還原 users：保留讀取前本地 users（避免被 Sheet 覆蓋）
                        if not users_backup_df.empty:
                            try:
                                conn_w = get_db_connection()
                                conn_w.execute("DELETE FROM users")
                                for _, r in users_backup_df.iterrows():
                                    conn_w.execute(
                                        "INSERT OR REPLACE INTO users (id, username, password_hash, role, created_at) VALUES (?,?,?,?,?)",
                                        (r.get("id"), r.get("username"), r.get("password_hash"), r.get("role"), r.get("created_at")),
                                    )
                                conn_w.commit()
                                conn_w.close()
                            except Exception as e:
                                errs = (errs or []) + [f"Users 還原失敗: {e}"]
                    if errs:
                        st.error("讀取失敗：" + "; ".join(errs[:5]))
                    else:
                        if sync_business_only:
                            st.success("已用 Google Sheet 覆蓋業務資料（Users 保留本地）。")
                        else:
                            st.success("已用 Google Sheet 資料覆蓋本地資料。")
                        st.rerun()
        except Exception as e:
            st.error(f"Google Sheet 功能載入失敗：{e}")

    if st.sidebar.button("🧨 重置資料庫（清空資料，保留 Users）", help="⚠️ 警告：會清空主要業務資料，保留帳號權限"):
        try:
            init_db()
            conn = get_db_connection()
            try:
                c = conn.cursor()
                c.execute("DELETE FROM orders")
                c.execute("DELETE FROM ad_flight_segments")
                c.execute("DELETE FROM platform_settings")
                c.execute("DELETE FROM platform_monthly_capacity")
                c.execute("DELETE FROM platform_monthly_purchase")
                c.execute("DELETE FROM ragic_import_logs")
                # 保留 users（帳號權限不動）
                conn.commit()
            finally:
                conn.close()

            st.sidebar.success("✅ 已清空資料庫資料（Users 保留）")
        except Exception as e:
            st.sidebar.error(f"❌ 刪除失敗: {e}")

    st.sidebar.markdown("---")
    st.sidebar.markdown("### 📝 平台設定")
    st.sidebar.info(
        "平台店數與基礎播出時段已改為自動讀取外部設定表：\n"
        "https://docs.google.com/spreadsheets/d/1g36WdYPLQgWk20VkPN7cOmyTDAl3Lp8vFd_v4ptmRec/edit?usp=sharing\n"
        "（Pricing / Stores 分頁）"
    )
    with st.sidebar.expander("🔎 設定來源檢查", expanded=False):
        try:
            from services_platform import get_external_settings_status

            st_status = get_external_settings_status()
            if st_status.get("ok"):
                st.success("外部設定表讀取成功")
            else:
                st.error("外部設定表讀取失敗或欄位不完整")
            st.caption(f"檢查時間：{st_status.get('checked_at', '')}")
            st.caption(f"Pricing 筆數：{st_status.get('pricing_rows', 0)}")
            st.caption(f"Stores 筆數：{st_status.get('stores_rows', 0)}")
            if st_status.get("missing_pricing_columns"):
                st.caption(f"Pricing 缺欄：{', '.join(st_status.get('missing_pricing_columns', []))}")
            if st_status.get("missing_stores_columns"):
                st.caption(f"Stores 缺欄：{', '.join(st_status.get('missing_stores_columns', []))}")
            if st_status.get("error"):
                st.caption(f"錯誤訊息：{st_status.get('error')}")
        except Exception as e:
            st.error(f"設定來源檢查失敗：{e}")

