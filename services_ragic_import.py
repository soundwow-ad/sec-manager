# -*- coding: utf-8 -*-
"""Ragic 區間匯入服務層。"""

from __future__ import annotations

from datetime import date, datetime
import uuid
from typing import Callable

import pandas as pd


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
) -> tuple[bool, str, str]:
    def ragic_get_field(entry: dict, name: str):
        fid = ragic_fields.get(name)
        if fid and isinstance(entry, dict) and entry.get(fid) not in (None, ""):
            return entry.get(fid)
        if isinstance(entry, dict):
            return entry.get(name)
        return None

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
        d = ragic_to_date(ragic_get_field(entry, field_name))
        if d is None:
            return False
        if from_d and d < from_d:
            return False
        if to_d and d > to_d:
            return False
        return True

    def collect_excel_tokens_from_entry(entry: dict) -> list[str]:
        out = []

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
        seen = set()
        return [t for t in out if t not in seen and not seen.add(t)]

    def ragic_extract_project_amount(entry: dict) -> float | None:
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

    batch_id = f"ragic_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    _log_ragic_import(get_db_connection=get_db_connection, batch_id=batch_id, status="info", phase="summary", message=f"開始匯入：{date_field} {date_from}~{date_to}")

    if not ragic_url or not str(ragic_url).strip():
        _log_ragic_import(get_db_connection=get_db_connection, batch_id=batch_id, status="failed", phase="fetch", message="Ragic URL 空白")
        return False, "Ragic URL 不可為空", batch_id
    if not api_key or not str(api_key).strip():
        _log_ragic_import(get_db_connection=get_db_connection, batch_id=batch_id, status="failed", phase="fetch", message="API Key 空白")
        return False, "Ragic API Key 不可為空", batch_id

    try:
        from ragic_client import parse_sheet_url, make_listing_url, get_json, extract_entries, parse_file_tokens, download_file
    except Exception as e:
        _log_ragic_import(get_db_connection=get_db_connection, batch_id=batch_id, status="failed", phase="fetch", message=f"ragic_client 載入失敗：{e}")
        return False, f"無法載入 ragic_client：{e}", batch_id

    ref = parse_sheet_url(ragic_url)
    limit = 200
    all_entries = []
    for offset in range(0, max_fetch, limit):
        url = make_listing_url(ref, limit=limit, offset=offset, subtables0=False, fts="")
        payload, err = get_json(url, api_key, timeout=60)
        if err:
            _log_ragic_import(get_db_connection=get_db_connection, batch_id=batch_id, status="failed", phase="fetch", message=f"offset={offset} 抓取失敗：{err}")
            return False, f"抓取 Ragic 失敗（offset={offset}）：{err}", batch_id
        entries = extract_entries(payload)
        if not entries:
            break
        all_entries.extend(entries)
        if len(entries) < limit:
            break

    _log_ragic_import(get_db_connection=get_db_connection, batch_id=batch_id, status="info", phase="fetch", imported_orders=len(all_entries), message=f"已抓取 entries={len(all_entries)}")
    if not all_entries:
        return False, "Ragic 無資料可匯入", batch_id

    filtered = [e for e in all_entries if entry_in_date_range(e, date_from, date_to, field_name=date_field)]
    _log_ragic_import(get_db_connection=get_db_connection, batch_id=batch_id, status="info", phase="filter", imported_orders=len(filtered), message=f"日期篩選後 entries={len(filtered)}")
    if not filtered:
        return False, "指定日期區間內無資料", batch_id

    order_rows = []
    parsed_files = 0
    for entry in filtered:
        ragic_id = entry.get("_ragicId")
        order_no = ragic_get_field(entry, "訂檔單號") or f"ragic_{ragic_id}"
        order_info = {
            "client": str(ragic_get_field(entry, "客戶") or ""),
            "product": str(ragic_get_field(entry, "產品名稱") or ""),
            "sales": str(ragic_get_field(entry, "業務(開發客戶)") or ""),
            "company": str(ragic_get_field(entry, "公司") or ""),
            "order_id": str(order_no),
            "amount_net": 0,
        }
        project_amount = ragic_extract_project_amount(entry)
        cue_val = ragic_get_field(entry, "訂檔CUE表")
        tokens = parse_file_tokens(cue_val) if cue_val not in (None, "") else []
        excel_tokens = [t for t in tokens if str(t).lower().endswith((".xlsx", ".xls"))]
        if not excel_tokens:
            excel_tokens = collect_excel_tokens_from_entry(entry)
        if not excel_tokens:
            _log_ragic_import(get_db_connection=get_db_connection, batch_id=batch_id, status="failed", phase="download", ragic_id=ragic_id, order_no=order_no, message="無 CUE Excel 附件")
            continue

        for file_i, token in enumerate(excel_tokens):
            content, derr = download_file(ref, token, api_key, timeout=180)
            if derr or not content:
                _log_ragic_import(get_db_connection=get_db_connection, batch_id=batch_id, status="failed", phase="download", ragic_id=ragic_id, order_no=order_no, file_token=token, message=f"下載失敗：{derr}")
                continue
            try:
                cue_units = parse_cue_excel_for_table1(content, order_info=order_info)
            except Exception as e:
                cue_units = []
                _log_ragic_import(get_db_connection=get_db_connection, batch_id=batch_id, status="failed", phase="parse", ragic_id=ragic_id, order_no=order_no, file_token=token, message=f"解析例外：{e}")
            if not cue_units:
                _log_ragic_import(get_db_connection=get_db_connection, batch_id=batch_id, status="failed", phase="parse", ragic_id=ragic_id, order_no=order_no, file_token=token, message="解析不到可用 ad_unit")
                continue

            rows_before = len(order_rows)
            for i, u in enumerate(cue_units):
                daily_spots = u.get("daily_spots") or []
                days = int(u.get("days") or len(daily_spots) or 1)
                total_spots = int(u.get("total_spots") or (sum(daily_spots) if daily_spots else 0))
                spots = int(round(total_spots / max(days, 1))) if total_spots > 0 else int(daily_spots[0] if daily_spots else 0)
                order_id = f"ragic_{ragic_id}_{file_i}_{i}_{uuid.uuid4().hex[:6]}"
                start_date = str(u.get("start_date") or ragic_get_field(entry, "執行開始日期") or "")
                end_date = str(u.get("end_date") or ragic_get_field(entry, "執行結束日期") or "")
                seconds = int(u.get("seconds") or 0)
                platform = str(u.get("platform") or ragic_get_field(entry, "平台") or "")
                if not platform or not start_date or not end_date or seconds <= 0 or spots <= 0:
                    continue
                order_rows.append(
                    (
                        order_id,
                        platform,
                        order_info["client"],
                        order_info["product"],
                        order_info["sales"],
                        order_info["company"],
                        normalize_date(start_date) or start_date,
                        normalize_date(end_date) or end_date,
                        seconds,
                        spots,
                        0,
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        str(order_no),
                        "銷售秒數",
                        project_amount if project_amount and project_amount > 0 else None,
                        None,
                    )
                )
            imported_now = len(order_rows) - rows_before
            if imported_now > 0:
                parsed_files += 1
                _log_ragic_import(get_db_connection=get_db_connection, batch_id=batch_id, status="success", phase="parse", ragic_id=ragic_id, order_no=order_no, file_token=token, imported_orders=imported_now, message="解析成功")
            else:
                _log_ragic_import(get_db_connection=get_db_connection, batch_id=batch_id, status="failed", phase="parse", ragic_id=ragic_id, order_no=order_no, file_token=token, message="解析有結果但無有效列")

    if not order_rows:
        _log_ragic_import(get_db_connection=get_db_connection, batch_id=batch_id, status="failed", phase="summary", message="無可匯入訂單")
        return False, "日期區間有資料，但無可匯入的 CUE 解析結果", batch_id

    init_db()
    conn = get_db_connection()
    c = conn.cursor()
    try:
        if replace_existing:
            c.execute("DELETE FROM orders")
        c.executemany(
            """
            INSERT OR REPLACE INTO orders
            (id, platform, client, product, sales, company, start_date, end_date, seconds, spots, amount_net, updated_at, contract_id, seconds_type, project_amount_net, split_amount)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
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
            message=f"匯入完成：entries={len(filtered)} files={parsed_files} rows={len(order_rows)}",
        )
        return True, f"Ragic 匯入完成：{len(order_rows)} 筆（來源 entries={len(filtered)}，成功檔案={parsed_files}）", batch_id
    except Exception as e:
        conn.rollback()
        conn.close()
        _log_ragic_import(get_db_connection=get_db_connection, batch_id=batch_id, status="failed", phase="insert", imported_orders=len(order_rows), message=f"寫入失敗：{e}")
        return False, f"寫入資料庫失敗：{e}", batch_id


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

