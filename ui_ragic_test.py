# -*- coding: utf-8 -*-
"""
Ragic 抓取資料測試：搜尋單一案子 → 完整 Ragic 欄位展示 + CUE 解析成表1 + Excel/PDF 下載
"""
from __future__ import annotations

import io
import json
from datetime import datetime
from typing import Any, Callable

import pandas as pd
import streamlit as st

from ragic_client import (
    download_file,
    extract_entries,
    get_json,
    make_listing_url,
    make_single_record_url,
    parse_file_tokens,
    parse_sheet_url,
)


def _normalize_cell(v: Any) -> str:
    if v is None:
        return ""
    try:
        if isinstance(v, float) and pd.isna(v):
            return ""
    except Exception:
        pass
    if isinstance(v, (list, tuple)):
        return ", ".join([_normalize_cell(x) for x in v if _normalize_cell(x)])
    if isinstance(v, dict):
        return json.dumps(v, ensure_ascii=False)[:200]
    return str(v).strip()


def _flatten_entry(entry: dict, rev_id_to_name: dict[str, str]) -> list[dict]:
    """將一筆 Ragic entry 攤平成「欄位ID / 欄位名稱 / 值」列表（含巢狀）。"""
    rows = []

    def walk(obj, prefix=""):
        if obj is None:
            return
        if isinstance(obj, dict):
            for k, v in obj.items():
                label = rev_id_to_name.get(str(k), k) if rev_id_to_name else k
                if v is not None and not isinstance(v, (dict, list)):
                    rows.append({"欄位ID": k, "欄位名稱": label, "值": _normalize_cell(v)})
                elif isinstance(v, (dict, list)):
                    if isinstance(v, dict) and v:
                        walk(v, prefix=f"{prefix}{k}.")
                    elif isinstance(v, list):
                        for i, item in enumerate(v):
                            if isinstance(item, dict):
                                walk(item, prefix=f"{prefix}{k}[{i}].")
                            else:
                                rows.append({"欄位ID": f"{k}[{i}]", "欄位名稱": f"{label}[{i}]", "值": _normalize_cell(item)})
            return
        if isinstance(obj, list):
            for i, item in enumerate(obj):
                walk(item, prefix=f"{prefix}[{i}].")

    walk(entry)
    return rows


def _entry_to_order_info(entry: dict, ragic_fields: dict[str, str]) -> dict:
    """從 Ragic entry 抽出 parse_cue_excel_for_table1 用的 order_info。"""
    def g(name: str) -> str:
        fid = ragic_fields.get(name)
        if fid and fid in entry and entry.get(fid) not in (None, ""):
            return _normalize_cell(entry.get(fid))
        return _normalize_cell(entry.get(name, ""))

    return {
        "client": g("客戶"),
        "product": g("產品名稱"),
        "sales": g("業務(開發客戶)"),
        "company": g("公司"),
        "order_id": g("訂檔單號"),
        "amount_net": 0,
    }


def _deep_collect_excel_tokens(val: Any) -> list[str]:
    out: list[str] = []
    if val is None:
        return out
    if isinstance(val, str):
        s = val.strip()
        if "@" in s and s.lower().endswith((".xlsx", ".xls")):
            out.append(s)
        return out
    if isinstance(val, (list, tuple)):
        for x in val:
            out.extend(_deep_collect_excel_tokens(x))
        return out
    if isinstance(val, dict):
        for x in val.values():
            out.extend(_deep_collect_excel_tokens(x))
        return out
    return out


def _create_pdf_bytes(ragic_rows: list[dict], table1_df: pd.DataFrame, title: str) -> bytes:
    """用 reportlab 產生 PDF（Ragic 欄位表 + 表1）。"""
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import cm
    from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4, rightMargin=1.5*cm, leftMargin=1.5*cm, topMargin=1.5*cm, bottomMargin=1.5*cm)
    styles = getSampleStyleSheet()
    story = []

    story.append(Paragraph(title, styles["Title"]))
    story.append(Spacer(1, 12))

    # Ragic 完整欄位
    story.append(Paragraph("一、Ragic 完整欄位", styles["Heading2"]))
    story.append(Spacer(1, 6))
    if ragic_rows:
        df_ragic = pd.DataFrame(ragic_rows)
        data_ragic = [df_ragic.columns.tolist()] + df_ragic.values.tolist()
        t_ragic = Table(data_ragic, colWidths=[3*cm, 4*cm, 8*cm])
        t_ragic.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 8),
            ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ]))
        story.append(t_ragic)
    else:
        story.append(Paragraph("（無欄位資料）", styles["Normal"]))
    story.append(Spacer(1, 16))

    # 表1 解析明細
    story.append(Paragraph("二、表1 解析明細（CUE Excel → 表1 最詳細列表）", styles["Heading2"]))
    story.append(Spacer(1, 6))
    if not table1_df.empty:
        # 表1 欄位多，橫向縮小字體與欄寬
        data_t1 = [table1_df.columns.tolist()] + table1_df.head(100).fillna("").astype(str).values.tolist()
        ncol = len(table1_df.columns)
        colw = [2*cm] * min(8, ncol) + [1.2*cm] * max(0, ncol - 8)
        t_t1 = Table(data_t1, colWidths=colw[:ncol])
        t_t1.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 6),
            ("GRID", (0, 0), (-1, -1), 0.3, colors.grey),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ]))
        story.append(t_t1)
        if len(table1_df) > 100:
            story.append(Paragraph(f"（僅顯示前 100 列，共 {len(table1_df)} 列；完整請下載 Excel）", styles["Normal"]))
    else:
        story.append(Paragraph("（無 CUE Excel 或解析無結果）", styles["Normal"]))

    doc.build(story)
    buf.seek(0)
    return buf.read()


def render_ragic_test_tab(
    *,
    ragic_fields: dict[str, str],
    parse_cue_excel_for_table1: Callable[[bytes, Any], list[dict]],
    build_table1_from_cue_excel: Callable[..., pd.DataFrame] | None = None,
    load_platform_settings: Callable[[], dict] | None = None,
) -> None:
    st.markdown("### 🧪 Ragic 抓取資料測試")
    st.caption("搜尋單一案子（訂檔單號或 Ragic ID），檢視完整 Ragic 欄位、CUE 解析成表1、並下載 Excel / PDF。")

    default_url = "https://ap13.ragic.com/soundwow/forms12/17"
    ragic_url = st.text_input("訂檔表單網址", value=default_url, help="Ragic 表單 URL")
    api_key = ""
    try:
        api_key = (st.secrets.get("RAGIC_API_KEY") or "").strip()
    except Exception:
        pass
    api_key_input = st.text_input("Ragic API Key", value=api_key, type="password", help="可放在 .streamlit/secrets.toml 的 RAGIC_API_KEY")

    # 搜尋介面
    search_query = st.text_input(
        "🔍 搜尋案子",
        placeholder="輸入訂檔單號 或 Ragic 記錄 ID（數字）",
        key="ragic_search_query",
    )
    use_fts = st.checkbox("以關鍵字搜尋列表（訂檔單號／客戶／產品／平台）", value=False, help="若未勾選且輸入為數字，則直接以 Ragic ID 取得單筆")

    ref = parse_sheet_url(ragic_url)
    rev_id_to_name = {v: k for k, v in ragic_fields.items()} if ragic_fields else {}

    entry = None
    if st.button("搜尋", type="primary", key="ragic_search_btn") and (search_query or "").strip():
        key = (search_query or "").strip()
        api_key_use = api_key_input or api_key
        if not api_key_use:
            st.error("請輸入 Ragic API Key。")
            st.stop()

        # 若為數字且未勾選 fts，先試單筆 API
        if not use_fts and key.isdigit():
            single_url = make_single_record_url(ref, int(key))
            payload, err = get_json(single_url, api_key_use, timeout=60)
            if err:
                st.warning(f"單筆取得失敗：{err}，改以列表搜尋。")
            elif isinstance(payload, dict) and str(payload.get("status", "")).upper() == "ERROR":
                st.warning("Ragic 回傳錯誤，改以列表搜尋。")
            else:
                if str(key) in payload and isinstance(payload.get(str(key)), dict):
                    entry = payload[str(key)]
                    entry["_ragicId"] = int(key)
                elif isinstance(payload, dict) and not any(str(k).isdigit() for k in payload.keys()):
                    entry = payload
                    entry["_ragicId"] = int(key)
                if entry:
                    st.session_state["_ragic_last_entry"] = entry
                    st.success(f"已取得 Ragic ID = {key}")
                    st.rerun()

        # 列表 + fts 或單筆失敗
        if entry is None:
            fts_param = key if use_fts or not key.isdigit() else ""
            list_url = make_listing_url(ref, limit=50, offset=0, subtables0=False, fts=fts_param)
            payload, err = get_json(list_url, api_key_use, timeout=60)
            if err:
                st.error(f"抓取失敗：{err}")
                st.stop()
            if isinstance(payload, dict) and str(payload.get("status", "")).upper() == "ERROR":
                st.error("Ragic 回傳 status=ERROR（API Key/權限或 URL 可能有誤）。")
                st.stop()
            entries = extract_entries(payload)
            if not entries:
                st.warning("沒有符合的資料。")
                st.stop()
            # 關鍵字篩選（本機）
            if key and not key.isdigit():
                kw = key.lower()
                filtered = [e for e in entries if kw in str(e.get("訂檔單號") or e.get(ragic_fields.get("訂檔單號", "")) or "").lower()
                    or kw in str(e.get("客戶") or e.get(ragic_fields.get("客戶", "")) or "").lower()
                    or kw in str(e.get("產品名稱") or e.get(ragic_fields.get("產品名稱", "")) or "").lower()
                    or kw in str(e.get("平台") or e.get(ragic_fields.get("平台", "")) or "").lower()]
                if filtered:
                    entries = filtered
            if len(entries) == 1:
                entry = entries[0]
                st.session_state["_ragic_last_entry"] = entry
                st.success("找到 1 筆，已載入。")
                st.rerun()
            else:
                st.session_state["_ragic_search_results"] = entries
                st.session_state["_ragic_search_key"] = key
                st.info(f"找到 {len(entries)} 筆，請選擇一筆檢視。")
                st.rerun()

    # 若剛搜尋有多筆，顯示選擇
    if "_ragic_search_results" in st.session_state and st.session_state.get("_ragic_search_results"):
        results = st.session_state["_ragic_search_results"]
        options = []
        for e in results:
            rid = e.get("_ragicId", "")
            no = e.get("訂檔單號") or e.get(ragic_fields.get("訂檔單號", "")) or ""
            client = e.get("客戶") or e.get(ragic_fields.get("客戶", "")) or ""
            options.append(f"{rid} | {no} | {client}")
        idx = st.selectbox("選擇一筆案子", range(len(options)), format_func=lambda i: options[i], key="ragic_select_result")
        if st.button("載入所選", key="ragic_load_selected"):
            st.session_state["_ragic_last_entry"] = results[idx]
            del st.session_state["_ragic_search_results"]
            st.rerun()

    # 顯示已載入的單筆
    entry = st.session_state.get("_ragic_last_entry")
    if not entry or not isinstance(entry, dict):
        st.info("請在上方輸入訂檔單號或 Ragic ID，按「搜尋」取得一筆案子。")
        return

    api_key_use = api_key_input or api_key
    if not api_key_use:
        st.warning("未設定 API Key，無法下載 CUE Excel；僅顯示已抓到的 Ragic 欄位。")

    rid = entry.get("_ragicId", "")
    st.markdown("---")
    st.markdown(f"#### 📌 案子：_ragicId = {rid}")

    # 一、Ragic 完整欄位（超詳盡）
    st.markdown("##### 一、Ragic 完整欄位（所有抓到的欄位）")
    ragic_rows = _flatten_entry(entry, rev_id_to_name)
    if ragic_rows:
        df_ragic = pd.DataFrame(ragic_rows)
        with st.expander("展開 Ragic 完整欄位表", expanded=True):
            st.dataframe(df_ragic, use_container_width=True, hide_index=True)
    else:
        # 簡易 key-value
        simple = [{"欄位": k, "值": _normalize_cell(v)} for k, v in entry.items()]
        st.dataframe(pd.DataFrame(simple), use_container_width=True, hide_index=True)

    # 二、CUE Excel 解析成表1
    st.markdown("##### 二、CUE Excel 解析為表1（最詳細列表）")
    cue_fid = ragic_fields.get("訂檔CUE表")
    cue_val = entry.get(cue_fid) if cue_fid and cue_fid in entry else entry.get("訂檔CUE表")
    cue_tokens = parse_file_tokens(cue_val)
    excel_tokens = [t for t in cue_tokens if str(t).lower().endswith((".xlsx", ".xls"))]
    if not excel_tokens:
        excel_tokens = _deep_collect_excel_tokens(entry)
    order_info = _entry_to_order_info(entry, ragic_fields)
    custom_settings = load_platform_settings() if load_platform_settings else None
    build_table1 = build_table1_from_cue_excel if build_table1_from_cue_excel else None

    all_table1_dfs: list[pd.DataFrame] = []
    for i, tok in enumerate(excel_tokens, start=1):
        with st.expander(f"CUE 檔案 {i}：{tok[:50]}...", expanded=(i == 1)):
            if not api_key_use:
                st.caption("請設定 API Key 以下載並解析。")
                continue
            content, derr = download_file(ref, tok, api_key_use, timeout=120)
            if derr or not content:
                st.error(f"下載失敗：{derr}")
                continue
            cue_units = parse_cue_excel_for_table1(content, order_info=order_info)
            if not cue_units:
                st.warning("此檔案未解析出每日檔次（可能非 CUE 版型）。")
                continue
            st.caption(f"解析出 {len(cue_units)} 個廣告單位。")
            if build_table1:
                df_t1 = build_table1(cue_units, custom_settings=custom_settings)
                if not df_t1.empty:
                    all_table1_dfs.append(df_t1)
                    st.dataframe(df_t1, use_container_width=True, hide_index=True)
            else:
                df_simple = pd.DataFrame([
                    {
                        "platform": u.get("platform"),
                        "region": u.get("region"),
                        "seconds": u.get("seconds"),
                        "start_date": u.get("start_date"),
                        "end_date": u.get("end_date"),
                        "days": u.get("days"),
                        "total_spots": u.get("total_spots"),
                        "source_sheet": u.get("source_sheet"),
                    }
                    for u in cue_units
                ])
                st.dataframe(df_simple, use_container_width=True, hide_index=True)

    # 合併表1（多檔時）
    if len(all_table1_dfs) > 1:
        st.markdown("##### 表1 合併結果（所有 CUE 檔案）")
        df_combined = pd.concat(all_table1_dfs, ignore_index=True)
        st.dataframe(df_combined, use_container_width=True, hide_index=True)
        all_table1_dfs = [df_combined]
    elif len(all_table1_dfs) == 1:
        df_combined = all_table1_dfs[0]
    else:
        df_combined = pd.DataFrame()

    # 下載 Excel
    st.markdown("##### 📥 下載")
    if not ragic_rows:
        ragic_rows = [{"欄位ID": k, "欄位名稱": k, "值": _normalize_cell(v)} for k, v in entry.items()]
    df_ragic_export = pd.DataFrame(ragic_rows)

    excel_buf = io.BytesIO()
    with pd.ExcelWriter(excel_buf, engine="openpyxl") as w:
        df_ragic_export.to_excel(w, sheet_name="Ragic完整欄位", index=False)
        if not df_combined.empty:
            df_combined.to_excel(w, sheet_name="表1-解析明細", index=False)
    excel_buf.seek(0)
    st.download_button(
        "📥 下載 Excel（Ragic 欄位 + 表1 解析明細）",
        data=excel_buf.getvalue(),
        file_name=f"ragic_case_{rid}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="dl_ragic_excel",
    )

    # 下載 PDF
    try:
        pdf_bytes = _create_pdf_bytes(ragic_rows, df_combined, f"Ragic 案子 {rid} 詳盡解析")
        st.download_button(
            "📥 下載 PDF（Ragic 欄位 + 表1 摘要）",
            data=pdf_bytes,
            file_name=f"ragic_case_{rid}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf",
            mime="application/pdf",
            key="dl_ragic_pdf",
        )
    except Exception as e:
        st.caption(f"PDF 產生失敗：{e}（請確認已安裝 reportlab）")

    if st.button("清除目前案子", key="ragic_clear_entry"):
        if "_ragic_last_entry" in st.session_state:
            del st.session_state["_ragic_last_entry"]
        if "_ragic_search_results" in st.session_state:
            del st.session_state["_ragic_search_results"]
        st.rerun()
