# -*- coding: utf-8 -*-
"""Ragic 區間匯入服務層。"""

from __future__ import annotations

from datetime import date, datetime
import hashlib
import uuid
from typing import Callable

import pandas as pd

SECONDS_MGMT_REMARK_MAX = 60000


def _log_ragic_import(
    *,
    get_db_connection: Callable[[], object],
    batch_id: str,
    status: str,
    phase: str,
    ragic_id=None,
    order_no=None,
    file_token=None,
    imported_orders=0,
    message="",
):
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute(
            """
            INSERT INTO ragic_import_logs
            (batch_id, status, phase, ragic_id, order_no, file_token, imported_orders, message)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(batch_id),
                str(status),
                str(phase),
                str(ragic_id) if ragic_id is not None else None,
                str(order_no) if order_no is not None else None,
                str(file_token) if file_token is not None else None,
                int(imported_orders or 0),
                str(message or ""),
            ),
        )
        conn.commit()
        conn.close()
    except Exception:
        pass


def _make_ragic_order_id(
    *,
    ragic_id: str,
    order_no: str,
    file_token: str,
    unit_idx: int,
    platform: str,
    client: str,
    product: str,
    sales: str,
    company: str,
    start_date: str,
    end_date: str,
    seconds: int,
    spots: int,
) -> str:
    """用穩定鍵產生 order_id，避免每次匯入都新建不同 id。"""
    stable_key = "|".join(
        map(
            str,
            [
                ragic_id or "",
                order_no or "",
                file_token or "",
                unit_idx,
                platform or "",
                client or "",
                product or "",
                sales or "",
                company or "",
                start_date or "",
                end_date or "",
                int(seconds or 0),
                int(spots or 0),
            ],
        )
    )
    digest = hashlib.sha1(stable_key.encode("utf-8")).hexdigest()[:14]
    return f"ragic_{ragic_id}_{digest}"


def _order_match_key(
    *,
    platform: str,
    client: str,
    product: str,
    sales: str,
    company: str,
    start_date: str,
    end_date: str,
    seconds: int,
    spots: int,
    contract_id: str,
) -> tuple:
    return (
        str(platform or "").strip(),
        str(client or "").strip(),
        str(product or "").strip(),
        str(sales or "").strip(),
        str(company or "").strip(),
        str(start_date or "").strip(),
        str(end_date or "").strip(),
        int(seconds or 0),
        int(spots or 0),
        str(contract_id or "").strip(),
    )


def _load_existing_order_id_map(get_db_connection: Callable[[], object]) -> dict[tuple, str]:
    mapping: dict[tuple, str] = {}
    conn = None
    try:
        conn = get_db_connection()
        df_old = pd.read_sql(
            """
            SELECT id, platform, client, product, sales, company, start_date, end_date, seconds, spots, contract_id
            FROM orders
            """,
            conn,
        )
        for _, r in df_old.iterrows():
            key = _order_match_key(
                platform=r.get("platform", ""),
                client=r.get("client", ""),
                product=r.get("product", ""),
                sales=r.get("sales", ""),
                company=r.get("company", ""),
                start_date=r.get("start_date", ""),
                end_date=r.get("end_date", ""),
                seconds=r.get("seconds", 0),
                spots=r.get("spots", 0),
                contract_id=r.get("contract_id", ""),
            )
            oid = str(r.get("id") or "").strip()
            if oid:
                mapping[key] = oid
    except Exception:
        pass
    finally:
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass
    return mapping


def _norm_text(v) -> str:
    return str(v or "").strip()


def _norm_num(v) -> float:
    try:
        return float(v or 0)
    except Exception:
        return 0.0


def _signature_from_existing_row(r: dict, effective_seconds_type: str) -> tuple:
    return (
        _norm_text(r.get("platform", "")),
        _norm_text(r.get("client", "")),
        _norm_text(r.get("product", "")),
        _norm_text(r.get("sales", "")),
        _norm_text(r.get("company", "")),
        _norm_text(r.get("start_date", "")),
        _norm_text(r.get("end_date", "")),
        int(_norm_num(r.get("seconds", 0))),
        int(_norm_num(r.get("spots", 0))),
        _norm_num(r.get("amount_net", 0)),
        _norm_text(r.get("contract_id", "")),
        _norm_text(effective_seconds_type),
        _norm_num(r.get("project_amount_net", 0)),
        _norm_num(r.get("split_amount", 0)),
    )


def _signature_from_tuple(t: tuple, effective_seconds_type: str) -> tuple:
    project_val = t[14] if len(t) > 14 else None
    split_val = t[15] if len(t) > 15 else None
    return (
        _norm_text(t[1]),
        _norm_text(t[2]),
        _norm_text(t[3]),
        _norm_text(t[4]),
        _norm_text(t[5]),
        _norm_text(t[6]),
        _norm_text(t[7]),
        int(_norm_num(t[8])),
        int(_norm_num(t[9])),
        _norm_num(t[10]),
        _norm_text(t[12]),
        _norm_text(effective_seconds_type),
        _norm_num(project_val),
        _norm_num(split_val),
    )


def _ragic_get_field(entry: dict, name: str, ragic_fields: dict):
    fid = ragic_fields.get(name)
    if fid and isinstance(entry, dict) and entry.get(fid) not in (None, ""):
        return entry.get(fid)
    if isinstance(entry, dict):
        return entry.get(name)
    return None


def _collect_excel_tokens_from_entry(entry: dict) -> list[str]:
    out: list[str] = []

    def walk(v):
        if v is None:
            return
        if isinstance(v, str):
            s = v.strip()
            if "@" in s and s.lower().endswith((".xlsx", ".xls")):
                out.append(s)
            return
        if isinstance(v, (list, tuple)):
            for x in v:
                walk(x)
            return
        if isinstance(v, dict):
            for x in v.values():
                walk(x)
            return

    walk(entry)
    seen: set[str] = set()
    return [t for t in out if t not in seen and not seen.add(t)]


def _ragic_extract_project_amount(entry: dict) -> float | None:
    candidates = ["實收金額總計(未稅)", "收入_實收金額總計(未稅)", "除佣實收總計(未稅)", "收入_除價買收總計(未稅)"]
    for k in candidates:
        v = entry.get(k)
        if v not in (None, ""):
            try:
                return float(v)
            except (TypeError, ValueError):
                pass
    for v in entry.values():
        if not isinstance(v, list):
            continue
        for row in v:
            if not isinstance(row, dict):
                continue
            for k in ("實收金額(未稅)", "除佣實收(未稅)", "實收金額總計(未稅)"):
                rv = row.get(k)
                if rv not in (None, ""):
                    try:
                        return float(rv)
                    except (TypeError, ValueError):
                        pass
    return None


def _truncate_seconds_remark(s: str, max_len: int = SECONDS_MGMT_REMARK_MAX) -> str:
    if len(s) <= max_len:
        return s
    return s[: max_len - 24] + "\n...[內容過長已截斷]"


def _format_unit_daily_detail(u: dict) -> str:
    dates = u.get("dates") or []
    ds = u.get("daily_spots") or []
    if dates and ds and len(dates) == len(ds):
        parts = [f"{d}:{sp}檔" for d, sp in zip(dates, ds)]
        return "；".join(parts)
    if ds:
        head = ds[:20]
        more = f" …(共{len(ds)}日)" if len(ds) > 20 else ""
        return "每日檔次=" + ",".join(str(x) for x in head) + more
    return "(無每日檔次陣列；total_spots=%s days=%s)" % (u.get("total_spots"), u.get("days"))


def _seconds_mgmt_yes_no(state: dict) -> str:
    if state.get("issues"):
        return "No"
    if state.get("skipped_summaries"):
        return "No"
    if not state.get("imported_summaries"):
        return "No"
    return "Yes"


def _compose_seconds_mgmt_remark(*, state: dict, batch_id: str, seconds_type_notes: list[str] | None = None) -> str:
    lines = [
        "=== 秒數管理／匯入報告 ===",
        f"時間：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"Batch：{batch_id}",
        f"訂檔單號：{state.get('order_no', '')}",
        f"RagicId：{state.get('ragic_id', '')}",
        "",
        f"【秒數管理】{_seconds_mgmt_yes_no(state)}（有任一檔案失敗、解析疑慮或略過列 → No）",
        "",
    ]
    if state.get("imported_summaries"):
        lines.append("【已寫入本系統 orders 的檔次摘要】")
        lines.extend(state["imported_summaries"])
        lines.append("")
    if state.get("uploaded_rows_detail"):
        lines.append("【已通過入庫條件列（完整欄位快照）】")
        lines.extend(state["uploaded_rows_detail"][:200])
        if len(state["uploaded_rows_detail"]) > 200:
            lines.append(f"... 其餘 {len(state['uploaded_rows_detail']) - 200} 列略")
        lines.append("")
    if state.get("skipped_summaries"):
        lines.append("【解析到但未入庫的列（疑慮／條件不符）】")
        lines.extend(state["skipped_summaries"])
        lines.append("")
    if state.get("issues"):
        lines.append("【檔案或整體流程問題】")
        lines.extend(state["issues"])
        lines.append("")
    if state.get("cue_excel_layout_sections"):
        lines.append(
            "【CUE Excel 版面摘錄（pandas 讀入、無表頭；列／欄皆 0-based；tab 分隔；供與解析診斷對照）】"
        )
        lines.extend(state["cue_excel_layout_sections"])
        lines.append("")
    if state.get("cue_structural_reports"):
        lines.append(
            "【CUE 結構化判讀（版型、主表頭列、各欄標題、秒數／日期區間、有意義列與卡住點；含模擬表節錄）】"
        )
        lines.extend(state["cue_structural_reports"])
        lines.append("")
    if state.get("file_logs"):
        lines.append("【各 CUE 檔處理】")
        for fl in state["file_logs"]:
            tok = fl.get("token_short", "")
            if fl.get("download_err"):
                lines.append(f"- 檔#{fl.get('file_index')} {tok} | 下載失敗：{fl.get('download_err')}")
            elif fl.get("parse_err"):
                lines.append(f"- 檔#{fl.get('file_index')} {tok} | 解析例外：{fl.get('parse_err')}")
            else:
                lines.append(
                    f"- 檔#{fl.get('file_index')} {tok} | units={fl.get('n_units')} | 入庫列數={fl.get('imported')}"
                )
        lines.append("")
    notes = seconds_type_notes or state.get("seconds_type_notes") or []
    if notes:
        lines.append("【秒數用途（資料庫合併／保留）】")
        lines.extend(notes[:40])
        if len(notes) > 40:
            lines.append(f"... 其餘 {len(notes) - 40} 筆略")
        lines.append("")
    return "\n".join(lines).strip()


def _push_seconds_mgmt_to_ragic(
    *,
    ref,
    api_key: str,
    ragic_fields: dict,
    entry_outcomes: dict[str, dict],
    batch_id: str,
    extra_seconds_notes: dict[str, list[str]] | None = None,
) -> str:
    from ragic_client import post_update_entry_fields

    fid_flag = ragic_fields.get("秒數管理")
    fid_note = ragic_fields.get("秒數管理(備註)")
    if not fid_flag or not fid_note:
        return "（config 未設定 秒數管理／秒數管理(備註) 流水號，已略過 Ragic 回寫）\n"

    extra = extra_seconds_notes or {}
    report_lines: list[str] = ["", "—— Ragic 秒數管理欄位回寫 ——"]
    for rid, state in sorted(entry_outcomes.items(), key=lambda x: x[0]):
        st = dict(state)
        merged_notes = list(st.get("seconds_type_notes") or []) + list(extra.get(rid, []))
        flag = _seconds_mgmt_yes_no(st)
        remark = _compose_seconds_mgmt_remark(state=st, batch_id=batch_id, seconds_type_notes=merged_notes)
        remark = _truncate_seconds_remark(remark)
        ok, err = post_update_entry_fields(ref, rid, {str(fid_flag): flag, str(fid_note): remark}, api_key)
        if ok:
            report_lines.append(f"RagicId {rid}：已寫入 秒數管理={flag}，備註長度={len(remark)} 字元")
            report_lines.append("──────── 以下為寫入 Ragic「秒數管理(備註)」的全文（與表單欄位內容相同）────────")
            report_lines.append(remark)
            report_lines.append("")
        else:
            report_lines.append(f"RagicId {rid}：回寫失敗 — {err}")
            report_lines.append("──────── 以下為本次擬寫入之備註全文（API 失敗時 Ragic 可能未更新）────────")
            report_lines.append(remark)
            report_lines.append("")
    return "\n".join(report_lines).strip() + "\n"


def _ragic_entry_collect_order_rows(
    entry: dict,
    ref,
    api_key: str,
    *,
    ragic_fields: dict,
    parse_cue_excel_for_table1: Callable[..., list],
    normalize_date: Callable[[str], str],
    existing_order_id_map: dict,
    get_db_connection: Callable[[], object],
    batch_id: str,
    max_files: int | None = None,
) -> tuple[list[tuple[str, tuple]], dict]:
    from ragic_client import parse_file_tokens, download_file
    from services_media_platform import parse_platform_region as _parse_platform_region

    ragic_id = entry.get("_ragicId")
    rid_s = str(ragic_id)
    order_no = _ragic_get_field(entry, "訂檔單號", ragic_fields) or f"ragic_{ragic_id}"
    order_no = str(order_no)
    order_info = {
        "client": str(_ragic_get_field(entry, "客戶", ragic_fields) or ""),
        "product": str(_ragic_get_field(entry, "產品名稱", ragic_fields) or ""),
        "sales": str(_ragic_get_field(entry, "業務(開發客戶)", ragic_fields) or ""),
        "company": str(_ragic_get_field(entry, "公司", ragic_fields) or ""),
        "order_id": str(order_no),
        "amount_net": 0,
    }
    project_amount = _ragic_extract_project_amount(entry)

    state: dict = {
        "ragic_id": rid_s,
        "order_no": order_no,
        "issues": [],
        "file_logs": [],
        "imported_summaries": [],
        "uploaded_rows_detail": [],
        "skipped_summaries": [],
        "seconds_type_notes": [],
        "cue_excel_layout_sections": [],
        "cue_structural_reports": [],
    }

    rows_out: list[tuple[str, tuple]] = []

    cue_val = _ragic_get_field(entry, "訂檔CUE表", ragic_fields)
    tokens = parse_file_tokens(cue_val) if cue_val not in (None, "") else []
    excel_tokens = [t for t in tokens if str(t).lower().endswith((".xlsx", ".xls"))]
    if not excel_tokens:
        excel_tokens = _collect_excel_tokens_from_entry(entry)
    if max_files is not None:
        excel_tokens = excel_tokens[: int(max_files)]

    if not excel_tokens:
        state["issues"].append("無 CUE Excel 附件可下載")
        _log_ragic_import(
            get_db_connection=get_db_connection,
            batch_id=batch_id,
            status="failed",
            phase="download",
            ragic_id=ragic_id,
            order_no=order_no,
            message="無 CUE Excel 附件",
        )
        return rows_out, state

    for file_i, token in enumerate(excel_tokens, start=1):
        tok_short = str(token) if len(str(token)) <= 52 else str(token)[:50] + "…"
        flog = {
            "file_index": file_i,
            "token_short": tok_short,
            "download_err": None,
            "parse_err": None,
            "n_units": 0,
            "imported": 0,
        }
        content, derr = download_file(ref, token, api_key, timeout=180)
        if derr or not content:
            flog["download_err"] = str(derr)
            state["file_logs"].append(flog)
            state["issues"].append(f"檔案#{file_i} 下載失敗：{derr}")
            _log_ragic_import(
                get_db_connection=get_db_connection,
                batch_id=batch_id,
                status="failed",
                phase="download",
                ragic_id=ragic_id,
                order_no=order_no,
                file_token=token,
                message=f"下載失敗：{derr}",
            )
            continue
        cue_parse_diag: list[str] = []
        cue_layout_sec: list[str] = []
        cue_struct_sec: list[str] = []
        try:
            cue_units = parse_cue_excel_for_table1(
                content,
                order_info=order_info,
                cue_parse_diagnostics=cue_parse_diag,
                cue_layout_sections=cue_layout_sec,
                cue_structural_reports=cue_struct_sec,
            )
        except Exception as e:
            cue_units = []
            flog["parse_err"] = str(e)
            flog["n_units"] = 0
            state["file_logs"].append(flog)
            state["issues"].append(f"檔案#{file_i} 解析例外：{e}")
            _log_ragic_import(
                get_db_connection=get_db_connection,
                batch_id=batch_id,
                status="failed",
                phase="parse",
                ragic_id=ragic_id,
                order_no=order_no,
                file_token=token,
                message=f"解析例外：{e}",
            )
            continue

        flog["n_units"] = len(cue_units)
        flog["cue_parse_diagnostics"] = cue_parse_diag
        flog["cue_layout_sections"] = cue_layout_sec
        if cue_layout_sec:
            state["cue_excel_layout_sections"].append(
                f"──── 檔案#{file_i}（{flog.get('token_short', '')}）────\n"
                + "\n\n".join(cue_layout_sec)
            )
        if cue_struct_sec:
            state["cue_structural_reports"].append(
                f"──── 檔案#{file_i}（{flog.get('token_short', '')}）────\n"
                + "\n\n".join(cue_struct_sec)
            )
        if not cue_units:
            state["file_logs"].append(flog)
            state["issues"].append(f"檔案#{file_i} 解析不到可用 ad_unit")
            if cue_parse_diag:
                cap = 15
                tail = f" …（共 {len(cue_parse_diag)} 條）" if len(cue_parse_diag) > cap else ""
                state["issues"].append(
                    f"檔案#{file_i} 解析診斷：" + "；".join(cue_parse_diag[:cap]) + tail
                )
            _log_ragic_import(
                get_db_connection=get_db_connection,
                batch_id=batch_id,
                status="failed",
                phase="parse",
                ragic_id=ragic_id,
                order_no=order_no,
                file_token=token,
                message="解析不到可用 ad_unit",
            )
            continue

        rows_before = len(rows_out)
        for i, u in enumerate(cue_units):
            daily_spots = u.get("daily_spots") or []
            days = int(u.get("days") or len(daily_spots) or 1)
            total_spots = int(u.get("total_spots") or (sum(daily_spots) if daily_spots else 0))
            spots = int(round(total_spots / max(days, 1))) if total_spots > 0 else int(daily_spots[0] if daily_spots else 0)
            start_date = str(u.get("start_date") or _ragic_get_field(entry, "執行開始日期", ragic_fields) or "")
            end_date = str(u.get("end_date") or _ragic_get_field(entry, "執行結束日期", ragic_fields) or "")
            seconds = int(u.get("seconds") or 0)
            platform = str(u.get("platform") or _ragic_get_field(entry, "平台", ragic_fields) or "")

            skip_reason = None
            if seconds <= 0 or spots <= 0:
                skip_reason = f"秒數或檔次無效（秒數={seconds}，代表檔次={spots}）"
            elif not platform or not start_date or not end_date:
                skip_reason = "缺少平台或起迄日期"
            else:
                parsed_platform, _, _ = _parse_platform_region(platform)
                if parsed_platform not in ["全家", "家樂福"]:
                    skip_reason = f"平台無法產生 segment（媒體={parsed_platform}，需為全家／家樂福）"

            start_date_norm = normalize_date(start_date) or start_date if start_date else ""
            end_date_norm = normalize_date(end_date) or end_date if end_date else ""
            if skip_reason is None:
                s_date = pd.to_datetime(start_date_norm, errors="coerce")
                e_date = pd.to_datetime(end_date_norm, errors="coerce")
                if pd.isna(s_date) or pd.isna(e_date):
                    skip_reason = f"起迄日期無法解析（{start_date_norm} ~ {end_date_norm}）"

            if skip_reason:
                state["skipped_summaries"].append(
                    f"檔#{file_i} unit#{i + 1} | 平台={platform} | {skip_reason} | {_format_unit_daily_detail(u)}"
                )
                continue

            match_key = _order_match_key(
                platform=platform,
                client=order_info["client"],
                product=order_info["product"],
                sales=order_info["sales"],
                company=order_info["company"],
                start_date=start_date_norm,
                end_date=end_date_norm,
                seconds=seconds,
                spots=spots,
                contract_id=str(order_no),
            )
            order_id = existing_order_id_map.get(match_key) or _make_ragic_order_id(
                ragic_id=str(ragic_id),
                order_no=str(order_no),
                file_token=str(token),
                unit_idx=i,
                platform=platform,
                client=order_info["client"],
                product=order_info["product"],
                sales=order_info["sales"],
                company=order_info["company"],
                start_date=start_date_norm,
                end_date=end_date_norm,
                seconds=seconds,
                spots=spots,
            )
            rows_out.append(
                (
                    rid_s,
                    (
                        order_id,
                        platform,
                        order_info["client"],
                        order_info["product"],
                        order_info["sales"],
                        order_info["company"],
                        start_date_norm,
                        end_date_norm,
                        seconds,
                        spots,
                        0,
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        str(order_no),
                        "",
                        project_amount if project_amount and project_amount > 0 else None,
                        None,
                    ),
                )
            )
            detail_row = {
                "業務": order_info.get("sales", ""),
                "主管": str(_ragic_get_field(entry, "主管", ragic_fields) or ""),
                "合約編號": str(order_no),
                "公司": order_info.get("company", ""),
                "實收金額": project_amount if project_amount and project_amount > 0 else "",
                "除佣實收": project_amount if project_amount and project_amount > 0 else "",
                "專案實收金額": project_amount if project_amount and project_amount > 0 else "",
                "拆分金額": "待拆分計算",
                "製作成本": str(_ragic_get_field(entry, "製作成本", ragic_fields) or ""),
                "獎金%": str(_ragic_get_field(entry, "獎金%", ragic_fields) or ""),
                "核定獎金": str(_ragic_get_field(entry, "核定獎金", ragic_fields) or ""),
                "加發獎金": str(_ragic_get_field(entry, "加發獎金", ragic_fields) or ""),
                "業務基金": str(_ragic_get_field(entry, "業務基金", ragic_fields) or ""),
                "協力基金": str(_ragic_get_field(entry, "協力基金", ragic_fields) or ""),
                "秒數用途": "",
                "提交日": str(_ragic_get_field(entry, "建立日期", ragic_fields) or ""),
                "HYUNDAI_CUSTIN": str(_ragic_get_field(entry, "客戶", ragic_fields) or ""),
                "秒數": seconds,
                "素材": str(u.get("ad_name") or ""),
                "起始日": start_date_norm,
                "終止日": end_date_norm,
                "走期天數": days,
                "區域": str(u.get("region") or ""),
                "媒體平台": platform,
            }
            col_order = [
                "業務", "主管", "合約編號", "公司", "實收金額", "除佣實收", "專案實收金額", "拆分金額",
                "製作成本", "獎金%", "核定獎金", "加發獎金", "業務基金", "協力基金", "秒數用途", "提交日",
                "HYUNDAI_CUSTIN", "秒數", "素材", "起始日", "終止日", "走期天數", "區域", "媒體平台",
            ]
            row_text = " | ".join(f"{k}={detail_row.get(k, '')}" for k in col_order)
            state["uploaded_rows_detail"].append(f"檔#{file_i} unit#{i + 1} | {row_text}")
            state["imported_summaries"].append(
                f"order_id={order_id} | 平台={platform} | {seconds}秒 | 代表檔次≈{spots}/日 | 走期={start_date_norm}~{end_date_norm} | {_format_unit_daily_detail(u)} | sheet={u.get('source_sheet', '')!s}"
            )

        imported_now = len(rows_out) - rows_before
        flog["imported"] = imported_now
        state["file_logs"].append(flog)
        if imported_now <= 0:
            state["issues"].append(
                f"檔案#{file_i} 解析出 {len(cue_units)} 個 unit，但無任何列通過入庫條件（秒數／平台／日期等）"
            )
            _log_ragic_import(
                get_db_connection=get_db_connection,
                batch_id=batch_id,
                status="failed",
                phase="parse",
                ragic_id=ragic_id,
                order_no=order_no,
                file_token=token,
                message="解析有結果但無有效列",
            )
        else:
            _log_ragic_import(
                get_db_connection=get_db_connection,
                batch_id=batch_id,
                status="success",
                phase="parse",
                ragic_id=ragic_id,
                order_no=order_no,
                file_token=token,
                imported_orders=imported_now,
                message="解析成功",
            )

    return rows_out, state


def import_ragic_to_orders_by_date_range_service(
    *,
    ragic_url: str,
    api_key: str,
    date_from: date,
    date_to: date,
    date_field: str = "建立日期",
    replace_existing: bool = False,
    max_fetch: int = 5000,
    ragic_fields: dict,
    parse_cue_excel_for_table1: Callable[..., list],
    get_db_connection: Callable[[], object],
    init_db: Callable[[], None],
    build_ad_flight_segments: Callable[..., object],
    load_platform_settings: Callable[[], dict],
    compute_and_save_split_amount_for_contract: Callable[[str], None],
    sync_sheets_if_enabled: Callable[..., None],
    normalize_date: Callable[[str], str],
) -> tuple[bool, str, str, str]:
    def ragic_to_date(v):
        if v is None:
            return None
        try:
            d = pd.to_datetime(str(v).strip(), errors="coerce")
            if pd.isna(d):
                return None
            return d.date()
        except Exception:
            return None

    def entry_in_date_range(entry: dict, from_d: date, to_d: date, field_name="建立日期") -> bool:
        d = ragic_to_date(_ragic_get_field(entry, field_name, ragic_fields))
        if d is None:
            return False
        if from_d and d < from_d:
            return False
        if to_d and d > to_d:
            return False
        return True

    batch_id = f"ragic_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    _log_ragic_import(get_db_connection=get_db_connection, batch_id=batch_id, status="info", phase="summary", message=f"開始匯入：{date_field} {date_from}~{date_to}")

    if not ragic_url or not str(ragic_url).strip():
        _log_ragic_import(get_db_connection=get_db_connection, batch_id=batch_id, status="failed", phase="fetch", message="Ragic URL 空白")
        return False, "Ragic URL 不可為空", batch_id, ""
    if not api_key or not str(api_key).strip():
        _log_ragic_import(get_db_connection=get_db_connection, batch_id=batch_id, status="failed", phase="fetch", message="API Key 空白")
        return False, "Ragic API Key 不可為空", batch_id, ""

    try:
        from ragic_client import parse_sheet_url, make_listing_url, get_json, extract_entries
    except Exception as e:
        _log_ragic_import(get_db_connection=get_db_connection, batch_id=batch_id, status="failed", phase="fetch", message=f"ragic_client 載入失敗：{e}")
        return False, f"無法載入 ragic_client：{e}", batch_id, ""

    ref = parse_sheet_url(ragic_url)
    limit = 200
    all_entries = []
    for offset in range(0, max_fetch, limit):
        url = make_listing_url(ref, limit=limit, offset=offset, subtables0=False, fts="")
        payload, err = get_json(url, api_key, timeout=60)
        if err:
            _log_ragic_import(get_db_connection=get_db_connection, batch_id=batch_id, status="failed", phase="fetch", message=f"offset={offset} 抓取失敗：{err}")
            return False, f"抓取 Ragic 失敗（offset={offset}）：{err}", batch_id, ""
        entries = extract_entries(payload)
        if not entries:
            break
        all_entries.extend(entries)
        if len(entries) < limit:
            break

    _log_ragic_import(get_db_connection=get_db_connection, batch_id=batch_id, status="info", phase="fetch", imported_orders=len(all_entries), message=f"已抓取 entries={len(all_entries)}")
    if not all_entries:
        return False, "Ragic 無資料可匯入", batch_id, ""

    filtered = [e for e in all_entries if entry_in_date_range(e, date_from, date_to, field_name=date_field)]
    _log_ragic_import(get_db_connection=get_db_connection, batch_id=batch_id, status="info", phase="filter", imported_orders=len(filtered), message=f"日期篩選後 entries={len(filtered)}")
    if not filtered:
        return False, "指定日期區間內無資料", batch_id, ""

    existing_order_id_map = _load_existing_order_id_map(get_db_connection)
    entry_outcomes: dict[str, dict] = {}
    staged_rows: list[tuple[str, tuple]] = []
    for entry in filtered:
        chunk, state = _ragic_entry_collect_order_rows(
            entry,
            ref,
            api_key,
            ragic_fields=ragic_fields,
            parse_cue_excel_for_table1=parse_cue_excel_for_table1,
            normalize_date=normalize_date,
            existing_order_id_map=existing_order_id_map,
            get_db_connection=get_db_connection,
            batch_id=batch_id,
            max_files=None,
        )
        entry_outcomes[state["ragic_id"]] = state
        staged_rows.extend(chunk)

    total_skipped_units = sum(len(s.get("skipped_summaries") or []) for s in entry_outcomes.values())
    total_import_summaries = sum(len(s.get("imported_summaries") or []) for s in entry_outcomes.values())
    _log_ragic_import(
        get_db_connection=get_db_connection,
        batch_id=batch_id,
        status="info",
        phase="segment_filter",
        message=(
            f"ragic_entries={len(entry_outcomes)} staged_order_rows={len(staged_rows)} "
            f"import_unit_summaries={total_import_summaries} skipped_unit_lines={total_skipped_units}"
        ),
    )

    order_rows = [pair[1] for pair in staged_rows]

    if not order_rows:
        _log_ragic_import(get_db_connection=get_db_connection, batch_id=batch_id, status="failed", phase="summary", message="無可匯入訂單")
        push_detail = _push_seconds_mgmt_to_ragic(
            ref=ref,
            api_key=api_key,
            ragic_fields=ragic_fields,
            entry_outcomes=entry_outcomes,
            batch_id=batch_id,
        )
        return False, "日期區間有資料，但無可匯入的 CUE 解析結果", batch_id, push_detail

    init_db()
    conn = get_db_connection()
    c = conn.cursor()
    try:
        existing_rows: dict[str, dict] = {}
        df_existing = pd.read_sql(
            """
            SELECT id, platform, client, product, sales, company, start_date, end_date, seconds, spots, amount_net, contract_id, seconds_type, project_amount_net, split_amount
            FROM orders
            """,
            conn,
        )
        if not df_existing.empty:
            for _, rr in df_existing.iterrows():
                oid = _norm_text(rr.get("id", ""))
                if oid:
                    existing_rows[oid] = rr.to_dict()

        inserted_count = 0
        updated_count = 0
        skipped_count = 0
        for rid, t in staged_rows:
            oid = _norm_text(t[0])
            old_row = existing_rows.get(oid)
            old_seconds_type = _norm_text((old_row or {}).get("seconds_type", ""))
            incoming_seconds_type = _norm_text(t[13] if len(t) > 13 else "")
            effective_seconds_type = old_seconds_type if incoming_seconds_type == "" else incoming_seconds_type
            if old_row is None:
                inserted_count += 1
            else:
                old_sig = _signature_from_existing_row(old_row, effective_seconds_type)
                new_sig = _signature_from_tuple(t, effective_seconds_type)
                if old_sig == new_sig:
                    skipped_count += 1
                else:
                    updated_count += 1
            if old_row is not None and incoming_seconds_type == "" and old_seconds_type:
                st_e = entry_outcomes.get(rid)
                if st_e is not None:
                    st_e.setdefault("seconds_type_notes", [])
                    st_e["seconds_type_notes"].append(
                        f"order_id={oid}：匯入未帶秒數用途，沿用資料庫「{old_seconds_type}」"
                    )
            if old_row is not None and incoming_seconds_type != "" and old_seconds_type != incoming_seconds_type:
                st_e = entry_outcomes.get(rid)
                if st_e is not None:
                    st_e.setdefault("seconds_type_notes", [])
                    st_e["seconds_type_notes"].append(
                        f"order_id={oid}：秒數用途將由「{old_seconds_type}」更新為「{incoming_seconds_type}」（本次匯入帶入）"
                    )

        c.executemany(
            """
            INSERT INTO orders
            (id, platform, client, product, sales, company, start_date, end_date, seconds, spots, amount_net, updated_at, contract_id, seconds_type, project_amount_net, split_amount)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(id) DO UPDATE SET
                platform=excluded.platform,
                client=excluded.client,
                product=excluded.product,
                sales=excluded.sales,
                company=excluded.company,
                start_date=excluded.start_date,
                end_date=excluded.end_date,
                seconds=excluded.seconds,
                spots=excluded.spots,
                amount_net=excluded.amount_net,
                updated_at=excluded.updated_at,
                contract_id=excluded.contract_id,
                seconds_type=CASE
                    WHEN excluded.seconds_type IS NULL OR TRIM(excluded.seconds_type) = '' THEN orders.seconds_type
                    ELSE excluded.seconds_type
                END,
                project_amount_net=excluded.project_amount_net,
                split_amount=excluded.split_amount
            WHERE
                COALESCE(orders.platform, '') != COALESCE(excluded.platform, '')
                OR COALESCE(orders.client, '') != COALESCE(excluded.client, '')
                OR COALESCE(orders.product, '') != COALESCE(excluded.product, '')
                OR COALESCE(orders.sales, '') != COALESCE(excluded.sales, '')
                OR COALESCE(orders.company, '') != COALESCE(excluded.company, '')
                OR COALESCE(orders.start_date, '') != COALESCE(excluded.start_date, '')
                OR COALESCE(orders.end_date, '') != COALESCE(excluded.end_date, '')
                OR COALESCE(orders.seconds, 0) != COALESCE(excluded.seconds, 0)
                OR COALESCE(orders.spots, 0) != COALESCE(excluded.spots, 0)
                OR COALESCE(orders.amount_net, 0) != COALESCE(excluded.amount_net, 0)
                OR COALESCE(orders.contract_id, '') != COALESCE(excluded.contract_id, '')
                OR COALESCE(orders.seconds_type, '') != COALESCE(
                    CASE
                        WHEN excluded.seconds_type IS NULL OR TRIM(excluded.seconds_type) = '' THEN orders.seconds_type
                        ELSE excluded.seconds_type
                    END,
                    ''
                )
                OR COALESCE(orders.project_amount_net, 0) != COALESCE(excluded.project_amount_net, 0)
                OR COALESCE(orders.split_amount, 0) != COALESCE(excluded.split_amount, 0)
            """,
            order_rows,
        )
        conn.commit()
        conn.close()
        conn_read = get_db_connection()
        df_orders = pd.read_sql("SELECT * FROM orders", conn_read)
        conn_read.close()
        build_ad_flight_segments(df_orders, load_platform_settings(), write_to_db=True, sync_sheets=False)
        contracts_with_project = (
            df_orders.loc[
                df_orders["project_amount_net"].notna() & (pd.to_numeric(df_orders["project_amount_net"], errors="coerce") > 0),
                "contract_id",
            ]
            .dropna()
            .unique()
        )
        for cid in contracts_with_project:
            if cid:
                compute_and_save_split_amount_for_contract(str(cid))
        sync_sheets_if_enabled(only_tables=["Orders", "Segments"], skip_if_unchanged=True)
        _log_ragic_import(
            get_db_connection=get_db_connection,
            batch_id=batch_id,
            status="success",
            phase="insert",
            imported_orders=len(order_rows),
            message=(
                f"匯入完成：entries={len(filtered)} rows={len(order_rows)} "
                f"inserted={inserted_count} updated={updated_count} skipped={skipped_count}"
            ),
        )
        push_detail = _push_seconds_mgmt_to_ragic(
            ref=ref,
            api_key=api_key,
            ragic_fields=ragic_fields,
            entry_outcomes=entry_outcomes,
            batch_id=batch_id,
        )
        summary_lines = [
            f"【本機 DB】寫入 {len(order_rows)} 筆 orders；新增 {inserted_count}、更新 {updated_count}、略過（無變更）{skipped_count}。",
            f"【區間內 Ragic 筆數】{len(filtered)}；各筆解析／疑慮摘要已寫入 Ragic「秒數管理」「秒數管理(備註)」。",
            "",
            "【上傳至 orders 的欄位摘要（每筆）】",
        ]
        for rid, t in staged_rows:
            summary_lines.append(
                f"  RagicId={rid} | id={t[0]} | 平台={t[1]} | 秒數={t[8]} | 檔次={t[9]} | 走期={t[6]}~{t[7]} | contract_id={t[12]}"
            )
        if state.get("uploaded_rows_detail"):
            summary_lines.append("")
            summary_lines.append("【上傳至 orders 的完整欄位快照（每筆）】")
            summary_lines.extend(state["uploaded_rows_detail"][:200])
            if len(state["uploaded_rows_detail"]) > 200:
                summary_lines.append(f"... 其餘 {len(state['uploaded_rows_detail']) - 200} 列略")
        summary_lines.append("")
        detail_report = "\n".join(summary_lines) + push_detail
        return (
            True,
            (
                f"Ragic 匯入完成：{len(order_rows)} 筆（來源 entries={len(filtered)}，"
                f"新增 {inserted_count}、更新 {updated_count}、略過 {skipped_count}）。詳見第四段回傳／介面展開。"
            ),
            batch_id,
            detail_report,
        )
    except Exception as e:
        conn.rollback()
        conn.close()
        _log_ragic_import(get_db_connection=get_db_connection, batch_id=batch_id, status="failed", phase="insert", imported_orders=len(order_rows), message=f"寫入失敗：{e}")
        return False, f"寫入資料庫失敗：{e}", batch_id, ""


def import_ragic_single_entry_to_orders_service(
    *,
    ragic_url: str,
    api_key: str,
    ragic_id: str | int,
    replace_existing: bool,
    max_files_per_entry: int = 20,
    ragic_fields: dict,
    parse_cue_excel_for_table1: Callable[..., list],
    get_db_connection: Callable[[], object],
    init_db: Callable[[], None],
    build_ad_flight_segments: Callable[..., object],
    load_platform_settings: Callable[[], dict],
    compute_and_save_split_amount_for_contract: Callable[[str], None],
    sync_sheets_if_enabled: Callable[..., None],
    normalize_date: Callable[[str], str],
) -> tuple[bool, str, str, str]:
    batch_id = f"ragic_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    _log_ragic_import(get_db_connection=get_db_connection, batch_id=batch_id, status="info", phase="summary", message=f"開始匯入：單筆 ragic_id={ragic_id}")

    if not ragic_url or not str(ragic_url).strip():
        _log_ragic_import(get_db_connection=get_db_connection, batch_id=batch_id, status="failed", phase="fetch", message="Ragic URL 空白")
        return False, "Ragic URL 不可為空", batch_id, ""
    if not api_key or not str(api_key).strip():
        _log_ragic_import(get_db_connection=get_db_connection, batch_id=batch_id, status="failed", phase="fetch", message="Ragic API Key 空白")
        return False, "Ragic API Key 不可為空", batch_id, ""

    try:
        from ragic_client import parse_sheet_url, make_single_record_url, get_json
    except Exception as e:
        _log_ragic_import(get_db_connection=get_db_connection, batch_id=batch_id, status="failed", phase="fetch", message=f"ragic_client 載入失敗：{e}")
        return False, f"無法載入 ragic_client：{e}", batch_id, ""

    try:
        ref = parse_sheet_url(ragic_url)
        single_url = make_single_record_url(ref, ragic_id)
        payload, err = get_json(single_url, api_key, timeout=60)
        if err:
            _log_ragic_import(get_db_connection=get_db_connection, batch_id=batch_id, status="failed", phase="fetch", ragic_id=str(ragic_id), message=f"抓取失敗：{err}")
            return False, f"抓取 Ragic 失敗：{err}", batch_id, ""
        if isinstance(payload, dict) and str(payload.get("status", "")).upper() == "ERROR":
            _log_ragic_import(get_db_connection=get_db_connection, batch_id=batch_id, status="failed", phase="fetch", ragic_id=str(ragic_id), message=f"Ragic 回傳 status=ERROR：{payload.get('message','')}")
            return False, "Ragic 回傳錯誤（status=ERROR）。", batch_id, ""

        entry = None
        if isinstance(payload, dict):
            rid = str(ragic_id)
            if rid in payload and isinstance(payload.get(rid), dict):
                entry = payload.get(rid)
                entry["_ragicId"] = int(ragic_id) if str(ragic_id).isdigit() else ragic_id
            else:
                # 有些情況 payload 可能已是 entry dict
                entry = payload
                entry["_ragicId"] = int(ragic_id) if str(ragic_id).isdigit() else ragic_id

        if not isinstance(entry, dict) or not entry:
            _log_ragic_import(get_db_connection=get_db_connection, batch_id=batch_id, status="failed", phase="fetch", ragic_id=str(ragic_id), message="未取得可用 entry")
            return False, "未取得可用的 Ragic entry。", batch_id, ""
    except Exception as e:
        _log_ragic_import(get_db_connection=get_db_connection, batch_id=batch_id, status="failed", phase="fetch", ragic_id=str(ragic_id), message=f"抓取例外：{e}")
        return False, f"抓取例外：{e}", batch_id, ""

    ragic_id_str = str(ragic_id)
    existing_order_id_map = _load_existing_order_id_map(get_db_connection)
    staged_rows, state = _ragic_entry_collect_order_rows(
        entry,
        ref,
        api_key,
        ragic_fields=ragic_fields,
        parse_cue_excel_for_table1=parse_cue_excel_for_table1,
        normalize_date=normalize_date,
        existing_order_id_map=existing_order_id_map,
        get_db_connection=get_db_connection,
        batch_id=batch_id,
        max_files=max_files_per_entry,
    )
    entry_outcomes = {state["ragic_id"]: state}
    order_rows = [p[1] for p in staged_rows]

    _log_ragic_import(
        get_db_connection=get_db_connection,
        batch_id=batch_id,
        status="info",
        phase="segment_filter",
        message=(
            f"staged_order_rows={len(staged_rows)} "
            f"import_summaries={len(state.get('imported_summaries') or [])} "
            f"skipped_lines={len(state.get('skipped_summaries') or [])}"
        ),
    )

    if not order_rows:
        _log_ragic_import(get_db_connection=get_db_connection, batch_id=batch_id, status="failed", phase="summary", message="無可匯入訂單（皆未通過可產生 segment 條件）")
        push_detail = _push_seconds_mgmt_to_ragic(
            ref=ref,
            api_key=api_key,
            ragic_fields=ragic_fields,
            entry_outcomes=entry_outcomes,
            batch_id=batch_id,
        )
        return False, "此筆 Ragic 沒有可匯入的有效資料（皆未通過可產生 segment 條件）。", batch_id, push_detail

    init_db()
    conn = get_db_connection()
    c = conn.cursor()
    try:
        existing_rows: dict[str, dict] = {}
        df_existing = pd.read_sql(
            """
            SELECT id, platform, client, product, sales, company, start_date, end_date, seconds, spots, amount_net, contract_id, seconds_type, project_amount_net, split_amount
            FROM orders
            """,
            conn,
        )
        if not df_existing.empty:
            for _, rr in df_existing.iterrows():
                oid = _norm_text(rr.get("id", ""))
                if oid:
                    existing_rows[oid] = rr.to_dict()

        inserted_count = 0
        updated_count = 0
        skipped_count = 0
        for rid, t in staged_rows:
            oid = _norm_text(t[0])
            old_row = existing_rows.get(oid)
            old_seconds_type = _norm_text((old_row or {}).get("seconds_type", ""))
            incoming_seconds_type = _norm_text(t[13] if len(t) > 13 else "")
            effective_seconds_type = old_seconds_type if incoming_seconds_type == "" else incoming_seconds_type
            if old_row is None:
                inserted_count += 1
            else:
                old_sig = _signature_from_existing_row(old_row, effective_seconds_type)
                new_sig = _signature_from_tuple(t, effective_seconds_type)
                if old_sig == new_sig:
                    skipped_count += 1
                else:
                    updated_count += 1
            if old_row is not None and incoming_seconds_type == "" and old_seconds_type:
                st_e = entry_outcomes.get(rid)
                if st_e is not None:
                    st_e.setdefault("seconds_type_notes", [])
                    st_e["seconds_type_notes"].append(
                        f"order_id={oid}：匯入未帶秒數用途，沿用資料庫「{old_seconds_type}」"
                    )
            if old_row is not None and incoming_seconds_type != "" and old_seconds_type != incoming_seconds_type:
                st_e = entry_outcomes.get(rid)
                if st_e is not None:
                    st_e.setdefault("seconds_type_notes", [])
                    st_e["seconds_type_notes"].append(
                        f"order_id={oid}：秒數用途將由「{old_seconds_type}」更新為「{incoming_seconds_type}」（本次匯入帶入）"
                    )

        c.executemany(
            """
            INSERT INTO orders
            (id, platform, client, product, sales, company, start_date, end_date, seconds, spots, amount_net, updated_at, contract_id, seconds_type, project_amount_net, split_amount)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(id) DO UPDATE SET
                platform=excluded.platform,
                client=excluded.client,
                product=excluded.product,
                sales=excluded.sales,
                company=excluded.company,
                start_date=excluded.start_date,
                end_date=excluded.end_date,
                seconds=excluded.seconds,
                spots=excluded.spots,
                amount_net=excluded.amount_net,
                updated_at=excluded.updated_at,
                contract_id=excluded.contract_id,
                seconds_type=CASE
                    WHEN excluded.seconds_type IS NULL OR TRIM(excluded.seconds_type) = '' THEN orders.seconds_type
                    ELSE excluded.seconds_type
                END,
                project_amount_net=excluded.project_amount_net,
                split_amount=excluded.split_amount
            WHERE
                COALESCE(orders.platform, '') != COALESCE(excluded.platform, '')
                OR COALESCE(orders.client, '') != COALESCE(excluded.client, '')
                OR COALESCE(orders.product, '') != COALESCE(excluded.product, '')
                OR COALESCE(orders.sales, '') != COALESCE(excluded.sales, '')
                OR COALESCE(orders.company, '') != COALESCE(excluded.company, '')
                OR COALESCE(orders.start_date, '') != COALESCE(excluded.start_date, '')
                OR COALESCE(orders.end_date, '') != COALESCE(excluded.end_date, '')
                OR COALESCE(orders.seconds, 0) != COALESCE(excluded.seconds, 0)
                OR COALESCE(orders.spots, 0) != COALESCE(excluded.spots, 0)
                OR COALESCE(orders.amount_net, 0) != COALESCE(excluded.amount_net, 0)
                OR COALESCE(orders.contract_id, '') != COALESCE(excluded.contract_id, '')
                OR COALESCE(orders.seconds_type, '') != COALESCE(
                    CASE
                        WHEN excluded.seconds_type IS NULL OR TRIM(excluded.seconds_type) = '' THEN orders.seconds_type
                        ELSE excluded.seconds_type
                    END,
                    ''
                )
                OR COALESCE(orders.project_amount_net, 0) != COALESCE(excluded.project_amount_net, 0)
                OR COALESCE(orders.split_amount, 0) != COALESCE(excluded.split_amount, 0)
            """,
            order_rows,
        )
        conn.commit()
        conn.close()
        conn_read = get_db_connection()
        df_orders = pd.read_sql("SELECT * FROM orders", conn_read)
        conn_read.close()

        build_ad_flight_segments(df_orders, load_platform_settings(), write_to_db=True, sync_sheets=False)

        contracts_with_project = (
            df_orders.loc[
                df_orders["project_amount_net"].notna() & (pd.to_numeric(df_orders["project_amount_net"], errors="coerce") > 0),
                "contract_id",
            ]
            .dropna()
            .unique()
        )
        for cid in contracts_with_project:
            if cid:
                compute_and_save_split_amount_for_contract(str(cid))
        sync_sheets_if_enabled(only_tables=["Orders", "Segments"], skip_if_unchanged=True)
        _log_ragic_import(
            get_db_connection=get_db_connection,
            batch_id=batch_id,
            status="success",
            phase="insert",
            imported_orders=len(order_rows),
            message=(
                f"單筆匯入完成：rows={len(order_rows)} "
                f"inserted={inserted_count} updated={updated_count} skipped={skipped_count}"
            ),
        )
        push_detail = _push_seconds_mgmt_to_ragic(
            ref=ref,
            api_key=api_key,
            ragic_fields=ragic_fields,
            entry_outcomes=entry_outcomes,
            batch_id=batch_id,
        )
        summary_lines = [
            f"【本機 DB】寫入 {len(order_rows)} 筆 orders；新增 {inserted_count}、更新 {updated_count}、略過（無變更）{skipped_count}。",
            f"【RagicId】{ragic_id_str}；解析／疑慮摘要已寫入「秒數管理」「秒數管理(備註)」。",
            "",
            "【上傳至 orders 的欄位摘要（每筆）】",
        ]
        for rid, t in staged_rows:
            summary_lines.append(
                f"  RagicId={rid} | id={t[0]} | 平台={t[1]} | 秒數={t[8]} | 檔次={t[9]} | 走期={t[6]}~{t[7]} | contract_id={t[12]}"
            )
        if state.get("uploaded_rows_detail"):
            summary_lines.append("")
            summary_lines.append("【上傳至 orders 的完整欄位快照（每筆）】")
            summary_lines.extend(state["uploaded_rows_detail"][:200])
            if len(state["uploaded_rows_detail"]) > 200:
                summary_lines.append(f"... 其餘 {len(state['uploaded_rows_detail']) - 200} 列略")
        summary_lines.append("")
        detail_report = "\n".join(summary_lines) + push_detail
        return (
            True,
            (
                f"Ragic 單筆匯入完成：{len(order_rows)} 筆（"
                f"新增 {inserted_count}、更新 {updated_count}、略過 {skipped_count}）。詳見第四段回傳／介面展開。"
            ),
            batch_id,
            detail_report,
        )
    except Exception as e:
        conn.rollback()
        conn.close()
        _log_ragic_import(get_db_connection=get_db_connection, batch_id=batch_id, status="failed", phase="insert", imported_orders=len(order_rows), message=f"寫入失敗：{e}")
        return False, f"寫入資料庫失敗：{e}", batch_id, ""


def get_ragic_import_logs_service(
    *,
    limit: int,
    init_db: Callable[[], None],
    get_db_connection: Callable[[], object],
) -> pd.DataFrame:
    init_db()
    conn = get_db_connection()
    try:
        df = pd.read_sql("SELECT * FROM ragic_import_logs ORDER BY id DESC LIMIT ?", conn, params=(int(limit),))
    except Exception:
        df = pd.DataFrame()
    conn.close()
    return df

