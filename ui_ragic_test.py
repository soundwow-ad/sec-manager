# -*- coding: utf-8 -*-
"""
Ragic 抓取資料測試：搜尋單一案子 → 完整 Ragic 欄位展示 + CUE 解析成表1 + Excel/PDF 下載
表1 呈現與主程式「📋 表1-資料」完整權限一致：同欄位順序、缺值顯示「無法判斷」/「抓不到」。
"""
from __future__ import annotations

import io
import json
import re
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
from services_ragic_import import _ragic_material_display_string


# 與主程式 表1-資料 完整欄位順序一致（不含動態日期欄）
TABLE1_BASE_COLUMNS = [
    "業務", "主管", "合約編號", "公司", "實收金額", "除佣實收", "專案實收金額", "拆分金額",
    "製作成本", "獎金%", "核定獎金", "加發獎金", "業務基金", "協力基金", "秒數用途", "提交日",
    "客戶名稱", "秒數", "素材", "起始日", "終止日", "走期天數", "區域", "媒體平台",
]
TABLE1_HOUR_COLUMNS = [str(h) for h in [6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 0, 1]]
TABLE1_STAT_COLUMNS = ["每天總檔次", "委刊總檔數", "總秒數", "店數", "使用總秒數"]
PLACEHOLDER_MISSING = "無法判斷"
PLACEHOLDER_NOT_AVAILABLE = "抓不到"


def _ensure_full_table1_columns(df: pd.DataFrame, placeholder: str = PLACEHOLDER_MISSING) -> pd.DataFrame:
    """補齊表1 全部欄位，缺的欄位填 placeholder；日期欄（月/日(星期)）依 df 現有或補空。"""
    if df.empty:
        return df
    # 動態日期欄：符合 1/1(四) 這種格式的欄位
    date_cols = [c for c in df.columns if isinstance(c, str) and re.match(r"^\d{1,2}/\d{1,2}\([一二三四五六日]\)$", c)]
    date_cols_sorted = sorted(date_cols, key=lambda x: (int(x.split("/")[0]), int(x.split("/")[1].split("(")[0])))
    all_fixed = TABLE1_BASE_COLUMNS + TABLE1_HOUR_COLUMNS + TABLE1_STAT_COLUMNS + date_cols_sorted
    for col in all_fixed:
        if col not in df.columns:
            df[col] = placeholder
    # 其他 df 有但不在 list 的欄位放最後
    other = [c for c in df.columns if c not in all_fixed]
    order = [c for c in all_fixed if c in df.columns] + other
    return df[[c for c in order if c in df.columns]]


def _styler_one_decimal_ragic(df: pd.DataFrame):
    """表1 數值欄位格式（與主程式 _styler_one_decimal 一致）。"""
    if df is None or df.empty:
        return df.style if hasattr(df, "style") else df
    try:
        num_cols = df.select_dtypes(include=["number"]).columns.tolist()
        if not num_cols:
            return df.style
        return df.style.format({c: "{:,.1f}" for c in num_cols})
    except Exception:
        return df.style if hasattr(df, "style") else df


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


def _ensure_seconds_mgmt_rows(
    rows: list[dict],
    *,
    entry: dict,
    ragic_fields: dict[str, str],
) -> list[dict]:
    """強制補上秒數管理相關欄位（即使原始回傳未攤平到也要顯示）。"""
    out = list(rows or [])
    existed_ids = {str(r.get("欄位ID", "")).strip() for r in out}
    existed_names = {str(r.get("欄位名稱", "")).strip() for r in out}

    def _latest_seconds_type_from_note() -> str:
        note_text = _normalize_cell(
            _get_ragic_value_by_keys(
                entry,
                str(ragic_fields.get("秒數管理(備註)", "") or "").strip(),
                "秒數管理(備註)",
            )
        )
        if not note_text:
            return ""
        m = re.findall(r"seconds_type\s*更新為[「\"]([^」\"\n]+)[」\"]", note_text)
        return str(m[-1]).strip() if m else ""

    latest_from_note = _latest_seconds_type_from_note()

    def add_if_missing(field_name: str) -> None:
        fid = str(ragic_fields.get(field_name, "") or "").strip()
        val = _normalize_cell(_get_ragic_value_by_keys(entry, fid, field_name))
        if field_name == "秒數用途" and (not val) and latest_from_note:
            val = latest_from_note
        # 允許空值列出，避免使用者誤判「沒有這欄」
        id_key = fid or field_name
        if id_key in existed_ids or field_name in existed_names:
            return
        out.append({"欄位ID": id_key, "欄位名稱": field_name, "值": val if val else "（空白）"})
        existed_ids.add(id_key)
        existed_names.add(field_name)

    for name in ("秒數管理", "秒數管理(備註)", "秒數用途"):
        add_if_missing(name)
    return out


def _entry_to_order_info(entry: dict, ragic_fields: dict[str, str]) -> dict:
    """從 Ragic entry 抽出 parse_cue_excel_for_table1 用的 order_info。"""
    def g(name: str) -> str:
        fid = ragic_fields.get(name)
        if fid and fid in entry and entry.get(fid) not in (None, ""):
            return _normalize_cell(entry.get(fid))
        return _normalize_cell(entry.get(name, ""))

    # 素材欄：僅子表「廣告篇名」（與正式 Ragic 匯入一致）；缺則空白，不以主表產品名等替代
    mat = _ragic_material_display_string(entry, ragic_fields)
    product = mat

    return {
        "client": g("客戶"),
        "product": product,
        "sales": g("業務(開發客戶)"),
        "company": g("公司"),
        "order_id": g("訂檔單號"),
        "amount_net": 0,
    }


def _get_ragic_value_by_keys(entry: dict, *keys: str) -> Any:
    """依序用 key 從 entry 取值，第一個非空即回傳（支援 Ragic 回傳 ID 或中文欄位名）。"""
    if not entry or not isinstance(entry, dict):
        return None
    for k in keys:
        if not k:
            continue
        v = entry.get(k)
        if v is not None and v != "":
            return v
    return None


def _extract_ragic_value(entry: Any, field_id: str, chinese_names: list[str] | None = None) -> Any:
    """從 entry 取單一欄位值（主表 key 可能為 field_id 或中文名；子表可能為 list）。"""
    if entry is None or not isinstance(entry, dict):
        return None
    keys_to_try = [field_id]
    if chinese_names:
        keys_to_try.extend(chinese_names)
    v = _get_ragic_value_by_keys(entry, *keys_to_try)
    if v is None:
        return None
    if isinstance(v, list):
        if not v:
            return None
        first = v[0]
        if isinstance(first, (int, float)) and not isinstance(first, bool):
            try:
                return sum(float(x) for x in v if x is not None and str(x).replace(".", "").replace("-", "").replace(" ", "").replace("e", "").replace("E", "").isdigit())
            except (TypeError, ValueError):
                return _normalize_cell(first)
        if isinstance(first, dict):
            # 子表列為 dict：每筆用 field_id 或 chinese_names 取值，數字則加總
            nums = []
            try_keys = [field_id]
            if chinese_names:
                try_keys.extend(chinese_names)
            for row in v:
                if not isinstance(row, dict):
                    continue
                cell = _get_ragic_value_by_keys(row, *try_keys)
                if cell is None or cell == "":
                    continue
                try:
                    nums.append(float(cell))
                except (TypeError, ValueError):
                    pass
            if nums:
                return int(sum(nums)) if all(x == int(x) for x in nums) else sum(nums)
            return _normalize_cell(_get_ragic_value_by_keys(first, *try_keys))
        return _normalize_cell(first)
    return v


# Ragic 常回傳中文欄位名，表1 對應的多種可能鍵名
RAGIC_REVENUE_KEYS = ["收入_實收金額總計(未稅)", "實收金額總計(未稅)", "實收金額(未稅)"]
RAGIC_NET_KEYS = ["收入_除價買收總計(未稅)", "除佣實收總計(未稅)", "除佣實收(未稅)"]
RAGIC_COST_KEYS = ["收入_製作成本x金額(未稅)", "收入_成本", "製作成本", "成本"]


def _extract_ragic_from_any_subtable(entry: dict, field_id: str, chinese_names: list[str] | None = None) -> Any:
    """Ragic 子表可能在任意 key 下，遍歷 entry 找 list of dict，用 field_id 或中文欄位名加總。"""
    if not entry or not isinstance(entry, dict):
        return None
    try_keys = [field_id]
    if chinese_names:
        try_keys.extend(chinese_names)
    for val in entry.values():
        if not isinstance(val, list) or not val:
            continue
        first = val[0]
        if not isinstance(first, dict):
            continue
        if _get_ragic_value_by_keys(first, *try_keys) is None:
            continue
        nums = []
        for row in val:
            if not isinstance(row, dict):
                continue
            cell = _get_ragic_value_by_keys(row, *try_keys)
            if cell is None or cell == "":
                continue
            try:
                nums.append(float(cell))
            except (TypeError, ValueError):
                pass
        if nums:
            return int(sum(nums)) if all(x == int(x) for x in nums) else sum(nums)
    return None


def _entry_to_table1_ragic_overrides(entry: dict, ragic_fields: dict[str, str]) -> dict[str, Any]:
    """
    從 Ragic entry 解析可對應到表1 的欄位，回傳 {表1欄位名: 值}。
    用於覆寫「無法判斷」，能從 Ragic 抓到的就填進去。
    """
    def g(name: str) -> str:
        fid = ragic_fields.get(name)
        if fid and fid in entry and entry.get(fid) not in (None, ""):
            return _normalize_cell(entry.get(fid))
        return _normalize_cell(entry.get(name, ""))

    overrides: dict[str, Any] = {}

    # 主管 ← 業務主管
    v = g("業務主管")
    if v:
        overrides["主管"] = v

    # 提交日 ← 建立日期 或 修改日期
    v = g("建立日期") or g("修改日期")
    if v:
        overrides["提交日"] = v

    # 實收金額 / 專案實收金額 ← 主表「實收金額總計(未稅)」或子表「收入_實收金額總計(未稅)」「實收金額(未稅)」
    fid_revenue = ragic_fields.get("收入_實收金額總計(未稅)")
    rev = _get_ragic_value_by_keys(entry, *([fid_revenue] if fid_revenue else []), "實收金額總計(未稅)", "收入_實收金額總計(未稅)")
    if rev is None or rev == "":
        rev = _extract_ragic_value(entry, fid_revenue or "", RAGIC_REVENUE_KEYS)
    if rev is None or rev == "":
        rev = _extract_ragic_from_any_subtable(entry, fid_revenue or "", ["實收金額(未稅)", "實收金額總計(未稅)"])
    if rev is not None and rev != "":
        try:
            overrides["實收金額"] = int(float(rev))
            overrides["除佣實收"] = int(float(rev))
            overrides["專案實收金額"] = int(float(rev))
        except (TypeError, ValueError):
            overrides["實收金額"] = rev
            overrides["專案實收金額"] = rev

    # 除佣實收（若上面未取到，單獨試「除佣實收總計(未稅)」）
    if "除佣實收" not in overrides:
        fid_net = ragic_fields.get("收入_除價買收總計(未稅)")
        net = _get_ragic_value_by_keys(entry, *([fid_net] if fid_net else []), "除佣實收總計(未稅)", "除佣實收(未稅)")
        if net is None or net == "":
            net = _extract_ragic_from_any_subtable(entry, fid_net or "", ["除佣實收(未稅)", "除佣實收總計(未稅)"])
        if net is not None and net != "":
            try:
                overrides["除佣實收"] = int(float(net))
            except (TypeError, ValueError):
                overrides["除佣實收"] = net

    # 拆分金額不從 Ragic 帶入，一律由 _apply_split_amount_by_spots 依「委刊總檔數」比例計算

    # 製作成本 ← 主表「製作成本」或子表 收入_製作成本 / 收入_成本 / 成本
    cost = _get_ragic_value_by_keys(entry, "製作成本", "成本")
    if cost is None or cost == "":
        for name in ("收入_製作成本x金額(未稅)", "收入_成本"):
            fid = ragic_fields.get(name)
            if fid:
                cost = _extract_ragic_value(entry, fid, RAGIC_COST_KEYS)
                if cost is None or cost == "":
                    cost = _extract_ragic_from_any_subtable(entry, fid, ["製作成本", "成本"])
                if cost is not None and cost != "":
                    break
    if cost is not None and cost != "":
        try:
            overrides["製作成本"] = int(float(cost)) if isinstance(cost, (int, float)) else cost
        except (TypeError, ValueError):
            overrides["製作成本"] = cost

    # 獎金% ← 退佣%+現折% / 退佣% / 現折%（主表常為中文鍵）
    for name in ("退佣%+現折%", "退佣%", "現折%"):
        pct = _get_ragic_value_by_keys(entry, *([ragic_fields.get(name)] if ragic_fields.get(name) else []), name)
        if pct is not None and pct != "":
            overrides["獎金%"] = _normalize_cell(pct)
            break

    # 秒數用途：優先主欄位；若主欄位空白，回退解析秒數管理(備註)最新紀錄
    stype = _normalize_cell(
        _get_ragic_value_by_keys(
            entry,
            ragic_fields.get("秒數用途", ""),
            "秒數用途",
            "seconds_type",
        )
    )
    if not stype:
        note_text = _normalize_cell(_get_ragic_value_by_keys(entry, ragic_fields.get("秒數管理(備註)", ""), "秒數管理(備註)"))
        if note_text:
            ms = re.findall(r"seconds_type\s*更新為[「\"]([^」\"\n]+)[」\"]", note_text)
            if ms:
                stype = str(ms[-1]).strip()
    if stype:
        overrides["秒數用途"] = stype

    return overrides


def _to_float_or_none(v: Any) -> float | None:
    """安全轉成 float；無法轉換回傳 None。"""
    if v is None:
        return None
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return float(v)
    s = str(v).strip().replace(",", "")
    if not s or s in (PLACEHOLDER_MISSING, PLACEHOLDER_NOT_AVAILABLE):
        return None
    try:
        return float(s)
    except (TypeError, ValueError):
        return None


def _apply_split_amount_by_spots(df: pd.DataFrame) -> pd.DataFrame:
    """
    依「同一專案(合約編號)的委刊總檔數比例」計算拆分金額。
    - 專案總額優先取：專案實收金額，其次實收金額，再其次除佣實收
    - 各列拆分金額為整數，且加總精準等於專案總額
    """
    if df is None or df.empty:
        return df
    if "合約編號" not in df.columns or "委刊總檔數" not in df.columns:
        return df

    out = df.copy()
    if "拆分金額" not in out.columns:
        out["拆分金額"] = PLACEHOLDER_MISSING
    else:
        # pandas 2.x + pyarrow 字串欄位不接受直接寫入 int，先轉 object 避免 TypeError
        out["拆分金額"] = out["拆分金額"].astype(object)

    for contract_id, idxs in out.groupby("合約編號").groups.items():
        idx_list = list(idxs)
        if not idx_list:
            continue

        # 取此專案總額（第一個可解析值）
        project_total = None
        for amount_col in ("專案實收金額", "實收金額", "除佣實收"):
            if amount_col not in out.columns:
                continue
            for i in idx_list:
                val = _to_float_or_none(out.at[i, amount_col])
                if val is not None:
                    project_total = val
                    break
            if project_total is not None:
                break

        # 沒有金額可拆則保留原值
        if project_total is None:
            continue

        # 檔次權重
        spots = []
        for i in idx_list:
            sp = _to_float_or_none(out.at[i, "委刊總檔數"]) or 0.0
            spots.append(max(0.0, sp))
        total_spots = sum(spots)
        if total_spots <= 0:
            continue

        raw_alloc = [project_total * (sp / total_spots) for sp in spots]
        floors = [int(x) for x in raw_alloc]
        remainder = int(round(project_total)) - sum(floors)

        # 把剩餘金額分配給小數部分最大的列，確保加總正確
        frac_order = sorted(
            range(len(raw_alloc)),
            key=lambda k: (raw_alloc[k] - floors[k]),
            reverse=True,
        )
        alloc = floors[:]
        for k in frac_order[: max(0, remainder)]:
            alloc[k] += 1

        for local_i, row_i in enumerate(idx_list):
            out.at[row_i, "拆分金額"] = alloc[local_i]

    return out


# 常見附檔副檔名（Ragic 附檔 token 常為 ...@xxx.副檔名）
FILE_EXTENSIONS = (".xlsx", ".xls", ".pdf", ".jpg", ".jpeg", ".png", ".doc", ".docx", ".csv")


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


def _deep_collect_all_file_tokens(val: Any) -> list[str]:
    """從 entry 遞迴收集所有附檔 token（Excel、PDF、圖片等）。"""
    out: list[str] = []
    if val is None:
        return out
    if isinstance(val, str):
        s = val.strip()
        if "@" in s and s.lower().endswith(FILE_EXTENSIONS):
            out.append(s)
        return out
    if isinstance(val, (list, tuple)):
        for x in val:
            out.extend(_deep_collect_all_file_tokens(x))
        return out
    if isinstance(val, dict):
        for x in val.values():
            out.extend(_deep_collect_all_file_tokens(x))
        return out
    return out


def _suggest_filename(token: str, index: int) -> str:
    """從 Ragic token 推測下載檔名（token 常含 @ 後接檔名）。"""
    s = (token or "").strip()
    if "@" in s:
        part = s.split("@")[-1].strip()
        if part and "." in part:
            return part
    ext = ".bin"
    for e in FILE_EXTENSIONS:
        if s.lower().endswith(e):
            ext = e
            break
    return f"附檔_{index + 1}{ext}"


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
    import_ragic_single_entry_to_orders: Callable[..., object] | None = None,
    **kwargs: Any,
) -> None:
    """kwargs 可含 build_table1_from_cue_excel, load_platform_settings（相容舊版僅傳 2 參數）。"""
    build_table1_from_cue_excel = kwargs.get("build_table1_from_cue_excel")
    load_platform_settings = kwargs.get("load_platform_settings")
    st.markdown("### 🧪 Ragic 抓取資料測試")
    st.caption("搜尋單一案子（訂檔單號或 Ragic ID），檢視完整 Ragic 欄位、CUE 解析成表1、並下載 Excel / PDF。")

    pend_single = st.session_state.pop("_ragic_single_import_detail_pending", None)
    if pend_single:
        with st.expander("上次單筆匯入詳情（Ragic 秒數管理回寫）", expanded=True):
            st.text(pend_single)

    default_url = "https://ap13.ragic.com/soundwow/forms12/17"
    ragic_url = st.text_input("訂檔表單網址", value=default_url, help="Ragic 表單 URL")
    # 方便測試：若 secrets 沒配置，先帶入暫時預設 key（之後可再移除）
    api_key = "MEwyTEExWHJQamRDalZ6N0hzQ2syZlBHNUNJeWhwZFBrM3BMM2tDRWd4aGIvZ1JxWTlYaGkyM0RoRmo1ZExHaA=="
    try:
        # 相容兩種 secrets 寫法：
        # 1) RAGIC_API_KEY = "..."
        # 2) [ragic] api_key = "..."
        api_key = (
            st.secrets.get("RAGIC_API_KEY")
            or st.secrets.get("ragic", {}).get("api_key")
            or st.secrets.get("RAGIC", {}).get("api_key")
            or api_key
        ).strip()
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
    # 讀檔時固定顯示秒數管理兩欄（即使空值也顯示）
    sec_flag = _normalize_cell(_get_ragic_value_by_keys(entry, ragic_fields.get("秒數管理", ""), "秒數管理"))
    sec_note = _normalize_cell(_get_ragic_value_by_keys(entry, ragic_fields.get("秒數管理(備註)", ""), "秒數管理(備註)"))
    with st.expander("📝 秒數管理欄位（Ragic）", expanded=True):
        st.text(f"秒數管理：{sec_flag}")
        st.text("秒數管理(備註)：")
        st.text(sec_note if sec_note else "（空白）")

    if import_ragic_single_entry_to_orders:
        st.caption("可將這筆單一 Ragic 案子匯入至 `orders` / `segments`（無法產生 segment 的列會被略過並寫入匯入紀錄）。")
        st.caption("匯入策略：不清空舊資料，僅新增/更新有變動的列。")
        api_key_use = api_key_input or api_key
        if st.button("📥 匯入此單筆到資料庫", type="primary", key="ragic_import_single_btn") and (str(ragic_url or "").strip()):
            if not api_key_use or not str(api_key_use).strip():
                st.error("請輸入 Ragic API Key。")
                st.stop()
            with st.spinner("正在匯入（抓取附檔、解析 CUE、寫入 orders/segments）..."):
                ok, msg, batch_id, detail_report = import_ragic_single_entry_to_orders(
                    ragic_url=ragic_url.strip(),
                    api_key=str(api_key_use).strip(),
                    ragic_id=rid,
                    replace_existing=False,
                )
            if detail_report and str(detail_report).strip():
                st.session_state["_ragic_single_import_detail_pending"] = detail_report
            if ok:
                st.success(msg)
                st.session_state["_ragic_last_batch_id"] = batch_id
                st.rerun()
            else:
                st.error(msg)
                if detail_report and str(detail_report).strip():
                    with st.expander("匯入詳情（含寫入 Ragic 秒數管理／備註結果）", expanded=True):
                        st.text(detail_report)

    # 一、Ragic 完整欄位（超詳盡）
    st.markdown("##### 一、Ragic 完整欄位（所有抓到的欄位）")
    ragic_rows = _flatten_entry(entry, rev_id_to_name)
    ragic_rows = _ensure_seconds_mgmt_rows(ragic_rows, entry=entry, ragic_fields=ragic_fields)
    if ragic_rows:
        df_ragic = pd.DataFrame(ragic_rows)
        with st.expander("展開 Ragic 完整欄位表", expanded=True):
            st.dataframe(df_ragic, use_container_width=True, hide_index=True)
    else:
        # 簡易 key-value
        simple = [{"欄位": k, "值": _normalize_cell(v)} for k, v in entry.items()]
        st.dataframe(pd.DataFrame(simple), use_container_width=True, hide_index=True)

    # 二、CUE Excel 解析成表1（與主程式 表1-資料 完整權限同欄位、缺值顯示無法判斷/抓不到）
    st.markdown("##### 二、CUE Excel 解析為表1（與 📋 表1-資料 同欄位，缺值顯示「無法判斷」/「抓不到」）")
    st.caption("若解析失敗或想對照表頭：請展開下方各 CUE 檔，在成功下載後可使用「下載此 CUE Excel 到本機」另存，再以 Excel 開啟除錯。")
    cue_fid = ragic_fields.get("訂檔CUE表")
    cue_val = entry.get(cue_fid) if cue_fid and cue_fid in entry else entry.get("訂檔CUE表")
    cue_tokens = parse_file_tokens(cue_val)
    excel_tokens = [t for t in cue_tokens if str(t).lower().endswith((".xlsx", ".xls"))]
    if not excel_tokens:
        excel_tokens = _deep_collect_excel_tokens(entry)
    order_info = _entry_to_order_info(entry, ragic_fields)
    ragic_overrides = _entry_to_table1_ragic_overrides(entry, ragic_fields)
    custom_settings = load_platform_settings() if load_platform_settings else None
    build_table1 = build_table1_from_cue_excel if build_table1_from_cue_excel else None

    all_table1_dfs: list[pd.DataFrame] = []
    if not excel_tokens:
        st.info("未偵測到 CUE 的 .xlsx / .xls 附件（請確認 Ragic「訂檔CUE表」欄位）。")
    for i, tok in enumerate(excel_tokens, start=1):
        with st.expander(f"CUE 檔案 {i}：{tok[:50]}...", expanded=(i == 1)):
            if not api_key_use:
                st.caption("請設定 API Key 以下載並解析。")
                continue
            content, derr = download_file(ref, tok, api_key_use, timeout=120)
            if derr or not content:
                st.error(f"下載失敗：{derr}")
                continue
            _cue_ext = ".xlsx" if str(tok).lower().endswith(".xlsx") else (".xls" if str(tok).lower().endswith(".xls") else ".xlsx")
            _cue_mime = (
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                if _cue_ext == ".xlsx"
                else "application/vnd.ms-excel"
            )
            st.download_button(
                label="⬇️ 下載此 CUE Excel 到本機（除錯用）",
                data=content,
                file_name=f"CUE_Ragic{rid}_檔{i}{_cue_ext}",
                mime=_cue_mime,
                key=f"ragic_test_dl_cue_{str(rid)}_{i}",
                help="與上方解析使用同一份下載內容；可另存後檢查工作表結構與表頭。",
            )
            cue_units = parse_cue_excel_for_table1(content, order_info=order_info)
            if not cue_units:
                st.warning("此檔案未解析出每日檔次（可能非 CUE 版型）。")
                continue
            st.caption(f"解析出 {len(cue_units)} 個廣告單位。")
            if build_table1:
                df_t1 = build_table1(cue_units, custom_settings=custom_settings)
                if not df_t1.empty:
                    all_table1_dfs.append(df_t1)
            else:
                # 無 build_table1 時仍組出表1 結構，能從 Ragic 解析的欄位用 ragic_overrides
                rows = []
                for u in cue_units:
                    daily_spots = u.get("daily_spots") or []
                    total_spots = u.get("total_spots") or sum(daily_spots)
                    sec = int(u.get("seconds") or 0)
                    days = int(u.get("days") or len(daily_spots))
                    row = {
                        "業務": order_info.get("sales") or PLACEHOLDER_NOT_AVAILABLE,
                        "主管": ragic_overrides.get("主管") or PLACEHOLDER_MISSING,
                        "合約編號": order_info.get("order_id") or PLACEHOLDER_NOT_AVAILABLE,
                        "公司": order_info.get("company") or PLACEHOLDER_NOT_AVAILABLE,
                        "實收金額": ragic_overrides.get("實收金額") if "實收金額" in ragic_overrides else PLACEHOLDER_MISSING,
                        "除佣實收": ragic_overrides.get("除佣實收") if "除佣實收" in ragic_overrides else PLACEHOLDER_MISSING,
                        "專案實收金額": ragic_overrides.get("專案實收金額") if "專案實收金額" in ragic_overrides else PLACEHOLDER_MISSING,
                        "拆分金額": ragic_overrides.get("拆分金額") if "拆分金額" in ragic_overrides else PLACEHOLDER_MISSING,
                        "製作成本": ragic_overrides.get("製作成本") or PLACEHOLDER_MISSING,
                        "獎金%": ragic_overrides.get("獎金%") or PLACEHOLDER_MISSING,
                        "核定獎金": PLACEHOLDER_MISSING,
                        "加發獎金": PLACEHOLDER_MISSING,
                        "業務基金": PLACEHOLDER_MISSING,
                        "協力基金": PLACEHOLDER_MISSING,
                        # 無法可靠判斷秒數用途時，維持空值（避免硬推銷售秒數）
                        "秒數用途": ragic_overrides.get("秒數用途", ""),
                        "提交日": ragic_overrides.get("提交日") or PLACEHOLDER_MISSING,
                        "客戶名稱": order_info.get("client") or PLACEHOLDER_NOT_AVAILABLE,
                        "秒數": sec,
                        "素材": order_info.get("product") or PLACEHOLDER_NOT_AVAILABLE,
                        "起始日": u.get("start_date") or "",
                        "終止日": u.get("end_date") or "",
                        "走期天數": days,
                        "區域": u.get("region") or "未知",
                        "媒體平台": u.get("platform") or PLACEHOLDER_MISSING,
                        "每天總檔次": daily_spots[0] if daily_spots else 0,
                        "委刊總檔數": total_spots,
                        "總秒數": total_spots * sec,
                        "店數": PLACEHOLDER_MISSING,
                        "使用總秒數": (total_spots * sec) if sec else 0,
                    }
                    for h in TABLE1_HOUR_COLUMNS:
                        row[h] = ""
                    rows.append(row)
                if rows:
                    df_fallback = pd.DataFrame(rows)
                    # 日期欄：從 cue_units 收集
                    all_dates = set()
                    for u in cue_units:
                        for d in u.get("dates") or []:
                            try:
                                all_dates.add(pd.to_datetime(d))
                            except Exception:
                                pass
                    if all_dates:
                        weekday_map = {0: "一", 1: "二", 2: "三", 3: "四", 4: "五", 5: "六", 6: "日"}
                        for d in sorted(all_dates):
                            key = f"{d.month}/{d.day}({weekday_map[d.weekday()]})"
                            if key not in df_fallback.columns:
                                df_fallback[key] = ""
                        for idx, u in enumerate(cue_units):
                            dts = u.get("dates") or []
                            dss = u.get("daily_spots") or []
                            for j, (dt, sp) in enumerate(zip(dts, dss)):
                                try:
                                    dd = pd.to_datetime(dt)
                                    key = f"{dd.month}/{dd.day}({weekday_map[dd.weekday()]})"
                                    if key in df_fallback.columns and idx < len(df_fallback):
                                        df_fallback.loc[df_fallback.index[idx], key] = sp
                                except Exception:
                                    pass
                    all_table1_dfs.append(df_fallback)

    # 合併表1（多檔時）並補齊欄位、與主程式表1 同呈現
    if len(all_table1_dfs) > 1:
        df_combined = pd.concat(all_table1_dfs, ignore_index=True)
    elif len(all_table1_dfs) == 1:
        df_combined = all_table1_dfs[0].copy()
    else:
        df_combined = pd.DataFrame()

    if not df_combined.empty:
        df_combined = _ensure_full_table1_columns(df_combined, placeholder=PLACEHOLDER_MISSING)
        # 用 Ragic 欄位覆寫可解析的欄位（主管、實收金額、製作成本、獎金%、提交日等）
        for col, val in ragic_overrides.items():
            if col in df_combined.columns and val not in (None, ""):
                df_combined[col] = val
        # 拆分金額：依同專案委刊總檔數比例分配（符合專案金額拆分邏輯）
        df_combined = _apply_split_amount_by_spots(df_combined)
        # 與 表1-資料 最大權限相同：可橫向滾動、完整欄位
        st.markdown("#### 📊 表1-資料（可橫向滾動查看完整欄位）")
        st.dataframe(
            _styler_one_decimal_ragic(df_combined),
            use_container_width=True,
            height=650,
        )
        st.info(
            "💡 **提示**：此表格與主程式「📋 表1-資料」完整權限相同欄位。"
            " 抓不到或無法判斷的資料已顯示為「無法判斷」或「抓不到」。請使用橫向滾動查看完整內容。"
        )

    # ---------- 下載 Ragic 上的附檔（原始 Excel / PDF 等）----------
    st.markdown("##### 📥 下載 Ragic 附檔（原始檔案）")
    _raw_tokens = _deep_collect_all_file_tokens(entry)
    seen: set[str] = set()
    all_file_tokens = [t for t in _raw_tokens if t not in seen and not seen.add(t)]
    if not all_file_tokens:
        st.caption("此筆 Ragic 沒有偵測到附檔（訂檔CUE表或其他欄位）。")
    elif not api_key_use:
        st.caption("請設定 Ragic API Key 才能下載附檔。")
    else:
        cache_key = f"_ragic_attach_cache_{rid}"
        if cache_key not in st.session_state:
            st.session_state[cache_key] = {}
        cache = st.session_state[cache_key]

        if st.button("🔄 載入附檔列表（從 Ragic 下載）", key="ragic_load_attachments"):
            prog = st.progress(0)
            for i, tok in enumerate(all_file_tokens):
                content, err = download_file(ref, tok, api_key_use, timeout=120)
                cache[tok] = (content, err)
                prog.progress((i + 1) / len(all_file_tokens))
            st.rerun()

        if cache:
            for i, tok in enumerate(all_file_tokens):
                content, err = cache.get(tok, (None, None))
                fname = _suggest_filename(tok, i)
                if err or content is None:
                    st.caption(f"附檔 {i + 1} `{fname}`：{err or '下載失敗'}")
                else:
                    mime = "application/octet-stream"
                    if fname.lower().endswith(".pdf"):
                        mime = "application/pdf"
                    elif fname.lower().endswith((".xlsx", ".xls")):
                        mime = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" if fname.lower().endswith(".xlsx") else "application/vnd.ms-excel"
                    st.download_button(
                        f"📎 下載附檔 {i + 1}：{fname}",
                        data=content,
                        file_name=fname,
                        mime=mime,
                        key=f"dl_ragic_attach_{rid}_{i}",
                    )

    # ---------- 下載本頁報告（Ragic 欄位＋表1 解析產生的 Excel/PDF）----------
    st.markdown("##### 📥 下載本頁報告（Excel / PDF）")
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
        "📥 下載本頁報告 Excel（Ragic 欄位 + 表1 解析明細）",
        data=excel_buf.getvalue(),
        file_name=f"ragic_case_{rid}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="dl_ragic_excel",
    )

    try:
        pdf_bytes = _create_pdf_bytes(ragic_rows, df_combined, f"Ragic 案子 {rid} 詳盡解析")
        st.download_button(
            "📥 下載本頁報告 PDF（Ragic 欄位 + 表1 摘要）",
            data=pdf_bytes,
            file_name=f"ragic_case_{rid}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf",
            mime="application/pdf",
            key="dl_ragic_pdf",
        )
    except Exception as e:
        st.caption(f"本頁報告 PDF 產生失敗：{e}（請確認已安裝 reportlab）")

    if st.button("清除目前案子", key="ragic_clear_entry"):
        if "_ragic_last_entry" in st.session_state:
            del st.session_state["_ragic_last_entry"]
        if "_ragic_search_results" in st.session_state:
            del st.session_state["_ragic_search_results"]
        try:
            del st.session_state[f"_ragic_attach_cache_{rid}"]
        except KeyError:
            pass
        st.rerun()
