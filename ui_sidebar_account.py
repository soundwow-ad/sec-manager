# -*- coding: utf-8 -*-
"""側欄：帳號、權限、同步狀態。"""

from __future__ import annotations

import streamlit as st


def render_sidebar_account(
    *,
    user: dict,
    role: str,
    roles: list[str],
    sync_sheets_if_enabled,
    auth_verify,
    auth_change_password,
    auth_list_users,
    auth_create_user,
    auth_delete_user,
) -> None:
    st.sidebar.title("⚙️ 控制台")
    st.sidebar.caption(f"👤 {user['username']}（{role}）")

    try:
        from sheets_backend import is_sheets_enabled, get_sheets_status

        sheets_on = is_sheets_enabled()
        sheets_status, sheets_reason = get_sheets_status()
    except Exception:
        sheets_on = False
        sheets_status, sheets_reason = "disabled", "無法載入設定"
    if sheets_on:
        load_errs = st.session_state.get("_sheets_load_errors")
        if load_errs:
            st.sidebar.warning("📄 啟動時自 Google Sheet 還原有誤（前幾筆）：" + "; ".join(load_errs[:2]))
        else:
            st.sidebar.caption("📄 資料已同步至 Google Sheet")
        if st.sidebar.button("🔄 立即同步至 Google Sheet", key="btn_sheets_sync"):
            errs = sync_sheets_if_enabled()
            if errs:
                st.session_state["_sheets_last_sync"] = ("error", "同步失敗: " + "; ".join(errs[:3]))
            else:
                st.session_state["_sheets_last_sync"] = ("success", "已同步至 Google Sheet")
        sync_status = st.session_state.get("_sheets_last_sync")
        if sync_status:
            kind, msg = sync_status
            if kind == "error":
                st.sidebar.error(msg)
            else:
                st.sidebar.success(msg)
    else:
        st.sidebar.caption("📄 未設定 Google Sheet（可於 .streamlit/secrets.toml 或 Cloud Secrets 設定）")
        if sheets_reason:
            st.sidebar.caption(f"原因：{sheets_reason}")

    if st.sidebar.button("🚪 登出", key="btn_logout"):
        del st.session_state["user"]
        st.rerun()

    with st.sidebar.expander("🔑 變更密碼", expanded=False):
        cur_p = st.text_input("目前密碼", type="password", key="chpwd_current")
        new_p1 = st.text_input("新密碼", type="password", key="chpwd_new1")
        new_p2 = st.text_input("確認新密碼", type="password", key="chpwd_new2")
        if st.button("💾 變更密碼", key="chpwd_btn"):
            u = auth_verify(user["username"], cur_p)
            if not u:
                st.error("目前密碼錯誤")
            elif not new_p1 or new_p1 != new_p2:
                st.error("新密碼不一致或為空")
            else:
                auth_change_password(user["username"], new_p1)
                st.success("已變更，請重新登入")
                del st.session_state["user"]
                st.rerun()

    if role == "行政主管":
        with st.sidebar.expander("👥 帳號管理", expanded=False):
            df_users = auth_list_users()
            st.dataframe(df_users[["username", "role"]], use_container_width=True, hide_index=True)
            st.caption("新增帳號")
            new_u = st.text_input("帳號", key="am_new_username", placeholder="username")
            new_p = st.text_input("密碼", type="password", key="am_new_password", placeholder="password")
            new_r = st.selectbox("權限", roles, key="am_new_role")
            if st.button("➕ 新增", key="am_btn_add"):
                ok, msg = auth_create_user(new_u, new_p, new_r)
                if ok:
                    st.success("已新增")
                    st.rerun()
                else:
                    st.error(msg)
            st.caption("刪除帳號")
            del_u = st.selectbox("選擇要刪除的帳號", df_users["username"].tolist(), key="am_del_user")
            if st.button("🗑️ 刪除", key="am_btn_del"):
                if del_u == user["username"]:
                    st.error("無法刪除目前登入的帳號")
                else:
                    auth_delete_user(del_u)
                    st.success("已刪除")
                    st.rerun()

