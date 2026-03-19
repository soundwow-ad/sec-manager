# -*- coding: utf-8 -*-
"""
表1 訂單逐筆管理 UI（新增 / 編輯 / 刪除）
將大段 UI/CRUD 邏輯自 ragic_inventory.py 抽離，降低主檔複雜度。
"""

from __future__ import annotations

from datetime import datetime
from typing import Callable, Sequence

import pandas as pd
import streamlit as st


def render_order_crud_panel(
    *,
    get_db_connection: Callable[[], object],
    load_platform_settings: Callable[[], dict],
    build_ad_flight_segments: Callable[..., pd.DataFrame],
    compute_split_for_contract: Callable[[str], None],
    sync_sheets_if_enabled: Callable[..., list[str]],
    styler_one_decimal: Callable[[pd.DataFrame], object],
    mock_platform_raw: Sequence[str],
    mock_sales: Sequence[str],
    mock_company: Sequence[str],
    mock_seconds: Sequence[int],
    seconds_usage_types: Sequence[str],
) -> None:
    st.markdown("---")
    st.markdown("#### 📝 訂單逐筆管理（新增／編輯／刪除）")
    st.caption("新增一筆：於下方表單填寫後儲存。每列可點「編輯」修改或「刪除」移除該筆訂單；變更後會自動重建檔次段。")

    # 置頂：先顯示 orders 總筆數，避免為了看總數一直往下捲動
    try:
        conn_total = get_db_connection()
        total_all = int(pd.read_sql("SELECT COUNT(1) AS n FROM orders", conn_total).iloc[0]["n"])
        conn_total.close()
        st.markdown(f"**📊 訂單總筆數：{total_all:,} 筆**")
    except Exception:
        st.markdown("**📊 訂單總筆數：-**")

    def _idx(lst, val, default=0):
        try:
            return lst.index(val) if val in lst else default
        except (ValueError, TypeError):
            return default

    # 新增一筆
    with st.expander("➕ 新增一筆訂單（填寫欄位後儲存）", expanded=False):
        c1, c2 = st.columns(2)
        with c1:
            new_id = st.text_input("訂單 ID（唯一）", key="crud_new_id", placeholder="例如 mock_2026_c001_01")
            new_contract_id = st.text_input("所屬合約編號（選填）", key="crud_new_contract_id", placeholder="同合約多列填相同值")
            new_platform = st.selectbox("平台", list(mock_platform_raw), key="crud_new_platform")
            new_client = st.text_input("客戶", key="crud_new_client", value="")
            new_product = st.text_input("產品名稱", key="crud_new_product", value="")
            new_sales = st.selectbox("業務", list(mock_sales), key="crud_new_sales")
            new_company = st.selectbox("公司", list(mock_company), key="crud_new_company")
            new_seconds_type = st.selectbox("秒數用途", list(seconds_usage_types), key="crud_new_seconds_type")
        with c2:
            new_start = st.date_input("開始日", value=datetime(2026, 1, 1), key="crud_new_start")
            new_end = st.date_input("結束日", value=datetime(2026, 1, 31), key="crud_new_end")
            new_seconds = st.selectbox("秒數", list(mock_seconds), key="crud_new_seconds")
            new_spots = st.number_input("檔次", min_value=2, value=10, step=2, key="crud_new_spots")
            new_amount = st.number_input("實收金額（未稅）", min_value=0, value=100000, step=10000, key="crud_new_amount")
            new_project_amount = st.number_input(
                "專案實收金額（同專案填同一數字，選填）",
                min_value=0,
                value=0,
                step=10000,
                key="crud_new_project_amount",
                help="同一合約編號多筆時填一次總額即可，系統會依使用秒數比例計算「拆分金額」",
            )
            new_split_amount = st.number_input(
                "拆分金額（選填，或由專案實收自動計算）",
                min_value=0,
                value=0,
                step=10000,
                key="crud_new_split_amount",
                help="ROI 等計算使用此欄；有填專案實收時儲存後會自動依比例計算",
            )
        if st.button("💾 儲存新增", key="crud_btn_add"):
            if not new_id or not new_client or not new_product:
                st.error("請填寫訂單 ID、客戶、產品名稱")
            else:
                conn_chk = get_db_connection()
                exists = conn_chk.execute("SELECT 1 FROM orders WHERE id=? LIMIT 1", (new_id,)).fetchone() is not None
                conn_chk.close()
                if exists:
                    st.error(f"訂單 ID「{new_id}」已存在")
                else:
                    conn_ins = get_db_connection()
                    try:
                        contract_id_val = (new_contract_id or "").strip() or None
                        project_val = float(new_project_amount) if new_project_amount else None
                        split_val = float(new_split_amount) if new_split_amount else None
                        conn_ins.execute(
                            """
                            INSERT INTO orders (id, platform, client, product, sales, company, start_date, end_date, seconds, spots, amount_net, updated_at, contract_id, seconds_type, project_amount_net, split_amount)
                            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                            """,
                            (
                                new_id,
                                new_platform,
                                new_client,
                                new_product,
                                new_sales,
                                new_company,
                                new_start.strftime("%Y-%m-%d"),
                                new_end.strftime("%Y-%m-%d"),
                                int(new_seconds),
                                int(new_spots),
                                float(new_amount),
                                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                contract_id_val,
                                new_seconds_type,
                                project_val,
                                split_val,
                            ),
                        )
                        conn_ins.commit()
                        df_after = pd.read_sql("SELECT * FROM orders", conn_ins)
                        conn_ins.close()
                        build_ad_flight_segments(df_after, load_platform_settings(), write_to_db=True)
                        if project_val and project_val > 0 and contract_id_val:
                            compute_split_for_contract(contract_id_val)
                        sync_sheets_if_enabled(only_tables=["Orders", "Segments"], skip_if_unchanged=True)
                        st.success("✅ 已新增一筆")
                        if "_table1_cache_key" in st.session_state:
                            del st.session_state["_table1_cache_key"]
                        st.rerun()
                    except Exception as e:
                        conn_ins.rollback()
                        conn_ins.close()
                        st.error(f"新增失敗: {e}")

    # 編輯表單（僅在點選某列「編輯」時顯示）
    crud_edit_id = st.session_state.get("crud_edit_id")
    if crud_edit_id:
        conn_edit = get_db_connection()
        edit_match = pd.read_sql("SELECT * FROM orders WHERE id=? LIMIT 1", conn_edit, params=(crud_edit_id,))
        conn_edit.close()
        if not edit_match.empty:
            selected_row = edit_match.iloc[0]
            with st.expander("✏️ 編輯此筆訂單", expanded=True):
                col_edit_a, col_edit_b = st.columns(2)
                with col_edit_a:
                    edit_contract_id = st.text_input(
                        "所屬合約編號（選填）",
                        value=(str(selected_row.get("contract_id")) if (pd.notna(selected_row.get("contract_id")) and selected_row.get("contract_id")) else ""),
                        key="crud_edit_contract_id",
                    )
                    edit_platform = st.selectbox("平台", list(mock_platform_raw), index=_idx(list(mock_platform_raw), selected_row.get("platform")), key="crud_edit_platform")
                    edit_client = st.text_input("客戶", value=selected_row.get("client", "") or "", key="crud_edit_client")
                    edit_product = st.text_input("產品名稱", value=selected_row.get("product", "") or "", key="crud_edit_product")
                    edit_sales = st.selectbox("業務", list(mock_sales), index=_idx(list(mock_sales), selected_row.get("sales")), key="crud_edit_sales")
                    edit_company = st.selectbox("公司", list(mock_company), index=_idx(list(mock_company), selected_row.get("company")), key="crud_edit_company")
                    edit_seconds_type = st.selectbox(
                        "秒數用途",
                        list(seconds_usage_types),
                        index=_idx(list(seconds_usage_types), selected_row.get("seconds_type") or ""),
                        key="crud_edit_seconds_type",
                    )
                with col_edit_b:
                    try:
                        _start_val = pd.to_datetime(selected_row["start_date"], errors="coerce")
                        edit_start_val = _start_val.date() if pd.notna(_start_val) else datetime(2026, 1, 1).date()
                    except Exception:
                        edit_start_val = datetime(2026, 1, 1).date()
                    try:
                        _end_val = pd.to_datetime(selected_row["end_date"], errors="coerce")
                        edit_end_val = _end_val.date() if pd.notna(_end_val) else datetime(2026, 1, 31).date()
                    except Exception:
                        edit_end_val = datetime(2026, 1, 31).date()
                    edit_start = st.date_input("開始日", value=edit_start_val, key="crud_edit_start")
                    edit_end = st.date_input("結束日", value=edit_end_val, key="crud_edit_end")
                    edit_seconds = st.number_input("秒數", min_value=5, max_value=60, value=int(selected_row["seconds"]), key="crud_edit_seconds")
                    edit_spots = st.number_input("檔次", min_value=2, value=int(selected_row["spots"]), step=2, key="crud_edit_spots")
                    edit_amount = st.number_input("實收金額（未稅）", min_value=0, value=int(selected_row["amount_net"]), step=10000, key="crud_edit_amount")
                    _proj = selected_row.get("project_amount_net")
                    edit_project_amount = st.number_input("專案實收金額（同專案填同一數字，選填）", min_value=0, value=int(_proj) if pd.notna(_proj) and _proj else 0, step=10000, key="crud_edit_project_amount")
                    _split = selected_row.get("split_amount")
                    edit_split_amount = st.number_input("拆分金額（選填，或由專案實收自動計算）", min_value=0, value=int(_split) if pd.notna(_split) and _split else 0, step=10000, key="crud_edit_split_amount")
                col_save, col_cancel, _ = st.columns([1, 1, 2])
                with col_save:
                    if st.button("💾 儲存編輯", key="crud_btn_edit"):
                        conn_up = get_db_connection()
                        try:
                            edit_contract_id_val = (edit_contract_id or "").strip() or None
                            project_val = float(edit_project_amount) if edit_project_amount else None
                            split_val = float(edit_split_amount) if edit_split_amount else None
                            conn_up.execute(
                                """
                                UPDATE orders SET platform=?, client=?, product=?, sales=?, company=?, start_date=?, end_date=?, seconds=?, spots=?, amount_net=?, updated_at=?, contract_id=?, seconds_type=?, project_amount_net=?, split_amount=?
                                WHERE id=?
                                """,
                                (
                                    edit_platform,
                                    edit_client,
                                    edit_product,
                                    edit_sales,
                                    edit_company,
                                    edit_start.strftime("%Y-%m-%d"),
                                    edit_end.strftime("%Y-%m-%d"),
                                    int(edit_seconds),
                                    int(edit_spots),
                                    float(edit_amount),
                                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                    edit_contract_id_val,
                                    edit_seconds_type,
                                    project_val,
                                    split_val,
                                    selected_row["id"],
                                ),
                            )
                            conn_up.commit()
                            df_after = pd.read_sql("SELECT * FROM orders", conn_up)
                            conn_up.close()
                            build_ad_flight_segments(df_after, load_platform_settings(), write_to_db=True)
                            if project_val and project_val > 0 and edit_contract_id_val:
                                compute_split_for_contract(edit_contract_id_val)
                            sync_sheets_if_enabled(only_tables=["Orders", "Segments"], skip_if_unchanged=True)
                            if "crud_edit_id" in st.session_state:
                                del st.session_state["crud_edit_id"]
                            if "_table1_cache_key" in st.session_state:
                                del st.session_state["_table1_cache_key"]
                            st.success("✅ 已更新")
                            st.rerun()
                        except Exception as e:
                            conn_up.rollback()
                            conn_up.close()
                            st.error(f"更新失敗: {e}")
                with col_cancel:
                    if st.button("取消", key="crud_btn_cancel"):
                        if "crud_edit_id" in st.session_state:
                            del st.session_state["crud_edit_id"]
                        st.rerun()

    # 訂單清單（SQL 搜尋 + SQL 分頁）
    st.markdown("**訂單清單（高效模式）**")
    st.caption("使用搜尋與分頁瀏覽；只對選取的一筆做「編輯／刪除」，避免全表渲染。")

    q1, q2, q3 = st.columns([2, 1, 1])
    with q1:
        crud_kw = st.text_input("搜尋（ID / 合約 / 客戶 / 產品 / 平台）", key="crud_kw", placeholder="輸入關鍵字")
    with q2:
        page_size = st.selectbox("每頁筆數", options=[20, 50, 100, 200, 500], index=1, key="crud_page_size")
    with q3:
        sort_desc = st.checkbox("最新在前", value=True, key="crud_sort_desc")

    conn_list = get_db_connection()
    where_sql = ""
    params = []
    if crud_kw and str(crud_kw).strip():
        kw_like = f"%{str(crud_kw).strip()}%"
        where_sql = "WHERE id LIKE ? OR IFNULL(contract_id,'') LIKE ? OR IFNULL(client,'') LIKE ? OR IFNULL(product,'') LIKE ? OR IFNULL(platform,'') LIKE ?"
        params = [kw_like, kw_like, kw_like, kw_like, kw_like]

    total_rows = int(pd.read_sql(f"SELECT COUNT(1) AS n FROM orders {where_sql}", conn_list, params=params).iloc[0]["n"])
    if total_rows <= 0:
        conn_list.close()
        st.info("📭 尚無訂單資料，請於上方「新增一筆訂單」填寫後儲存。")
    else:
        if crud_kw and str(crud_kw).strip():
            st.markdown(f"**📊 符合搜尋條件：{total_rows:,} 筆**")
        total_pages = max(1, (total_rows + int(page_size) - 1) // int(page_size))
        p1, p2 = st.columns([1, 3])
        with p1:
            page_no = st.number_input("頁碼", min_value=1, max_value=total_pages, value=1, step=1, key="crud_page_no")
        with p2:
            st.caption(f"共 {total_rows} 筆，{total_pages} 頁")

        offset_val = (int(page_no) - 1) * int(page_size)
        order_by = "updated_at DESC, id DESC" if sort_desc else "updated_at ASC, id ASC"
        sql_page = f"""
            SELECT id, contract_id, platform, client, product, start_date, end_date, seconds, spots, seconds_type, amount_net, updated_at
            FROM orders
            {where_sql}
            ORDER BY {order_by}
            LIMIT ? OFFSET ?
        """
        df_page = pd.read_sql(sql_page, conn_list, params=[*params, int(page_size), int(offset_val)])
        conn_list.close()

        # 顯示秒數用途以供快速編輯（不影響原本「編輯所選」流程）
        if "seconds_type" not in df_page.columns:
            # 舊資料庫可能尚未填滿 seconds_type；補上欄位避免 UI crash
            df_page["seconds_type"] = ""

        st.dataframe(styler_one_decimal(df_page), use_container_width=True, height=360, hide_index=True)

        options = df_page["id"].astype(str).tolist() if not df_page.empty else []

        sel_id = st.selectbox("選取一筆訂單 ID", options=options, key="crud_select_id")
        op1, op2 = st.columns(2)
        with op1:
            if st.button("✏️ 編輯所選", key="crud_btn_edit_selected", disabled=(not sel_id)):
                st.session_state["crud_edit_id"] = sel_id
                st.rerun()
        with op2:
            if st.button("🗑️ 刪除所選", key="crud_btn_del_selected", type="primary", disabled=(not sel_id)):
                conn_del = get_db_connection()
                try:
                    conn_del.execute("DELETE FROM orders WHERE id=?", (sel_id,))
                    conn_del.commit()
                    df_after = pd.read_sql("SELECT * FROM orders", conn_del)
                    conn_del.close()
                    build_ad_flight_segments(df_after, load_platform_settings(), write_to_db=True, sync_sheets=False)
                    sync_sheets_if_enabled(only_tables=["Orders", "Segments"], skip_if_unchanged=True)
                    if "crud_edit_id" in st.session_state and st.session_state.get("crud_edit_id") == sel_id:
                        del st.session_state["crud_edit_id"]
                    if "_table1_cache_key" in st.session_state:
                        del st.session_state["_table1_cache_key"]
                    st.success("✅ 已刪除")
                    st.rerun()
                except Exception as e:
                    conn_del.rollback()
                    conn_del.close()
                    st.error(f"刪除失敗: {e}")

