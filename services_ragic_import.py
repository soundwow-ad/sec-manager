# -*- coding: utf-8 -*-
"""Ragic 區間匯入服務層。"""

from __future__ import annotations

from datetime import date, datetime
import hashlib
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

    existing_order_id_map = _load_existing_order_id_map(get_db_connection)
    order_rows = []
    parsed_files = 0
    cue_unit_candidates = 0
    skipped = {
        "invalid_seconds_or_spots": 0,
        "missing_platform_or_dates": 0,
        "invalid_platform_after_parse": 0,
        "invalid_dates_after_parse": 0,
        "parse_exception": 0,
    }
    from services_media_platform import parse_platform_region as _parse_platform_region
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
                cue_unit_candidates += 1
                daily_spots = u.get("daily_spots") or []
                days = int(u.get("days") or len(daily_spots) or 1)
                total_spots = int(u.get("total_spots") or (sum(daily_spots) if daily_spots else 0))
                spots = int(round(total_spots / max(days, 1))) if total_spots > 0 else int(daily_spots[0] if daily_spots else 0)
                start_date = str(u.get("start_date") or ragic_get_field(entry, "執行開始日期") or "")
                end_date = str(u.get("end_date") or ragic_get_field(entry, "執行結束日期") or "")
                seconds = int(u.get("seconds") or 0)
                platform = str(u.get("platform") or ragic_get_field(entry, "平台") or "")
                if seconds <= 0 or spots <= 0:
                    skipped["invalid_seconds_or_spots"] += 1
                    continue
                if not platform or not start_date or not end_date:
                    skipped["missing_platform_or_dates"] += 1
                    continue

                # 預先套用與 build_ad_flight_segments 相同的「可產生 segment 條件」
                # 1) platform 經 parse 後必須是全家/家樂福
                parsed_platform, _, _ = _parse_platform_region(platform)
                if parsed_platform not in ["全家", "家樂福"]:
                    skipped["invalid_platform_after_parse"] += 1
                    continue
                start_date_norm = normalize_date(start_date) or start_date
                end_date_norm = normalize_date(end_date) or end_date
                s_date = pd.to_datetime(start_date_norm, errors="coerce")
                e_date = pd.to_datetime(end_date_norm, errors="coerce")
                if pd.isna(s_date) or pd.isna(e_date):
                    skipped["invalid_dates_after_parse"] += 1
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
                order_rows.append(
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
                    "",  # 目前 ragic 匯入無法可靠判斷秒數用途：保留空值
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

    # 把不可產生 segment 的列寫到匯入紀錄，方便你除錯
    _log_ragic_import(
        get_db_connection=get_db_connection,
        batch_id=batch_id,
        status="info",
        phase="segment_filter",
        message=(
            f"cue_units_candidates={cue_unit_candidates} "
            f"skipped_invalid_seconds_or_spots={skipped['invalid_seconds_or_spots']} "
            f"skipped_missing_platform_or_dates={skipped['missing_platform_or_dates']} "
            f"skipped_invalid_platform_after_parse={skipped['invalid_platform_after_parse']} "
            f"skipped_invalid_dates_after_parse={skipped['invalid_dates_after_parse']}"
        ),
    )

    if not order_rows:
        _log_ragic_import(get_db_connection=get_db_connection, batch_id=batch_id, status="failed", phase="summary", message="無可匯入訂單")
        return False, "日期區間有資料，但無可匯入的 CUE 解析結果", batch_id

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
        for t in order_rows:
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
                f"匯入完成：entries={len(filtered)} files={parsed_files} rows={len(order_rows)} "
                f"inserted={inserted_count} updated={updated_count} skipped={skipped_count}"
            ),
        )
        return (
            True,
            (
                f"Ragic 匯入完成：{len(order_rows)} 筆（來源 entries={len(filtered)}，成功檔案={parsed_files}，"
                f"新增 {inserted_count}、更新 {updated_count}、略過 {skipped_count}）"
            ),
            batch_id,
        )
    except Exception as e:
        conn.rollback()
        conn.close()
        _log_ragic_import(get_db_connection=get_db_connection, batch_id=batch_id, status="failed", phase="insert", imported_orders=len(order_rows), message=f"寫入失敗：{e}")
        return False, f"寫入資料庫失敗：{e}", batch_id


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
) -> tuple[bool, str, str]:
    def ragic_get_field(entry: dict, name: str):
        fid = ragic_fields.get(name)
        if fid and isinstance(entry, dict) and entry.get(fid) not in (None, ""):
            return entry.get(fid)
        if isinstance(entry, dict):
            return entry.get(name)
        return None

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

    def collect_excel_tokens_from_entry(entry: dict) -> list[str]:
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
        seen = set()
        return [t for t in out if t not in seen and not seen.add(t)]

    batch_id = f"ragic_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    _log_ragic_import(get_db_connection=get_db_connection, batch_id=batch_id, status="info", phase="summary", message=f"開始匯入：單筆 ragic_id={ragic_id}")

    if not ragic_url or not str(ragic_url).strip():
        _log_ragic_import(get_db_connection=get_db_connection, batch_id=batch_id, status="failed", phase="fetch", message="Ragic URL 空白")
        return False, "Ragic URL 不可為空", batch_id
    if not api_key or not str(api_key).strip():
        _log_ragic_import(get_db_connection=get_db_connection, batch_id=batch_id, status="failed", phase="fetch", message="Ragic API Key 空白")
        return False, "Ragic API Key 不可為空", batch_id

    try:
        from ragic_client import parse_sheet_url, make_single_record_url, get_json, parse_file_tokens, download_file
    except Exception as e:
        _log_ragic_import(get_db_connection=get_db_connection, batch_id=batch_id, status="failed", phase="fetch", message=f"ragic_client 載入失敗：{e}")
        return False, f"無法載入 ragic_client：{e}", batch_id

    from services_media_platform import parse_platform_region as _parse_platform_region

    try:
        ref = parse_sheet_url(ragic_url)
        single_url = make_single_record_url(ref, ragic_id)
        payload, err = get_json(single_url, api_key, timeout=60)
        if err:
            _log_ragic_import(get_db_connection=get_db_connection, batch_id=batch_id, status="failed", phase="fetch", ragic_id=str(ragic_id), message=f"抓取失敗：{err}")
            return False, f"抓取 Ragic 失敗：{err}", batch_id
        if isinstance(payload, dict) and str(payload.get("status", "")).upper() == "ERROR":
            _log_ragic_import(get_db_connection=get_db_connection, batch_id=batch_id, status="failed", phase="fetch", ragic_id=str(ragic_id), message=f"Ragic 回傳 status=ERROR：{payload.get('message','')}")
            return False, "Ragic 回傳錯誤（status=ERROR）。", batch_id

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
            return False, "未取得可用的 Ragic entry。", batch_id
    except Exception as e:
        _log_ragic_import(get_db_connection=get_db_connection, batch_id=batch_id, status="failed", phase="fetch", ragic_id=str(ragic_id), message=f"抓取例外：{e}")
        return False, f"抓取例外：{e}", batch_id

    ragic_id_str = str(ragic_id)
    order_no = ragic_get_field(entry, "訂檔單號") or f"ragic_{ragic_id_str}"
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
    excel_tokens = excel_tokens[: max_files_per_entry]
    if not excel_tokens:
        _log_ragic_import(get_db_connection=get_db_connection, batch_id=batch_id, status="failed", phase="download", ragic_id=ragic_id_str, order_no=str(order_no), message="無可下載的 CUE Excel 附件")
        return False, "此筆 Ragic 沒有可下載/可解析的 CUE Excel 附件。", batch_id

    existing_order_id_map = _load_existing_order_id_map(get_db_connection)
    order_rows: list[tuple] = []
    parsed_files = 0
    cue_unit_candidates = 0
    skipped = {
        "invalid_seconds_or_spots": 0,
        "missing_platform_or_dates": 0,
        "invalid_platform_after_parse": 0,
        "invalid_dates_after_parse": 0,
    }

    for file_i, token in enumerate(excel_tokens):
        content, derr = download_file(ref, token, api_key, timeout=180)
        if derr or not content:
            _log_ragic_import(get_db_connection=get_db_connection, batch_id=batch_id, status="failed", phase="download", ragic_id=ragic_id_str, order_no=str(order_no), file_token=token, message=f"下載失敗：{derr}")
            continue
        try:
            cue_units = parse_cue_excel_for_table1(content, order_info=order_info)
        except Exception as e:
            cue_units = []
            _log_ragic_import(get_db_connection=get_db_connection, batch_id=batch_id, status="failed", phase="parse", ragic_id=ragic_id_str, order_no=str(order_no), file_token=token, message=f"解析例外：{e}")
        if not cue_units:
            _log_ragic_import(get_db_connection=get_db_connection, batch_id=batch_id, status="failed", phase="parse", ragic_id=ragic_id_str, order_no=str(order_no), file_token=token, message="解析不到可用 ad_unit")
            continue

        rows_before = len(order_rows)
        for i, u in enumerate(cue_units):
            cue_unit_candidates += 1
            daily_spots = u.get("daily_spots") or []
            days = int(u.get("days") or len(daily_spots) or 1)
            total_spots = int(u.get("total_spots") or (sum(daily_spots) if daily_spots else 0))
            spots = int(round(total_spots / max(days, 1))) if total_spots > 0 else int(daily_spots[0] if daily_spots else 0)
            start_date_raw = str(u.get("start_date") or ragic_get_field(entry, "執行開始日期") or "")
            end_date_raw = str(u.get("end_date") or ragic_get_field(entry, "執行結束日期") or "")
            seconds = int(u.get("seconds") or 0)
            platform_raw = str(u.get("platform") or ragic_get_field(entry, "平台") or "")

            if seconds <= 0 or spots <= 0:
                skipped["invalid_seconds_or_spots"] += 1
                continue
            if not platform_raw or not start_date_raw or not end_date_raw:
                skipped["missing_platform_or_dates"] += 1
                continue

            parsed_platform, _, _ = _parse_platform_region(platform_raw)
            if parsed_platform not in ["全家", "家樂福"]:
                skipped["invalid_platform_after_parse"] += 1
                continue

            start_date_norm = normalize_date(start_date_raw) or start_date_raw
            end_date_norm = normalize_date(end_date_raw) or end_date_raw
            s_date = pd.to_datetime(start_date_norm, errors="coerce")
            e_date = pd.to_datetime(end_date_norm, errors="coerce")
            if pd.isna(s_date) or pd.isna(e_date):
                skipped["invalid_dates_after_parse"] += 1
                continue
            match_key = _order_match_key(
                platform=platform_raw,
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
                ragic_id=str(ragic_id_str),
                order_no=str(order_no),
                file_token=str(token),
                unit_idx=i,
                platform=platform_raw,
                client=order_info["client"],
                product=order_info["product"],
                sales=order_info["sales"],
                company=order_info["company"],
                start_date=start_date_norm,
                end_date=end_date_norm,
                seconds=seconds,
                spots=spots,
            )

            order_rows.append(
                    (
                    order_id,
                    platform_raw,
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
                        "",  # 目前 ragic 匯入無法可靠判斷秒數用途：保留空值，避免硬推語意
                    project_amount if project_amount and project_amount > 0 else None,
                    None,
                )
            )

        imported_now = len(order_rows) - rows_before
        if imported_now > 0:
            parsed_files += 1
            _log_ragic_import(get_db_connection=get_db_connection, batch_id=batch_id, status="success", phase="parse", ragic_id=ragic_id_str, order_no=str(order_no), file_token=token, imported_orders=imported_now, message="解析成功")
        else:
            _log_ragic_import(get_db_connection=get_db_connection, batch_id=batch_id, status="failed", phase="parse", ragic_id=ragic_id_str, order_no=str(order_no), file_token=token, message="解析有結果但無有效列")

    _log_ragic_import(
        get_db_connection=get_db_connection,
        batch_id=batch_id,
        status="info",
        phase="segment_filter",
        message=(
            f"cue_units_candidates={cue_unit_candidates} "
            f"skipped_invalid_seconds_or_spots={skipped['invalid_seconds_or_spots']} "
            f"skipped_missing_platform_or_dates={skipped['missing_platform_or_dates']} "
            f"skipped_invalid_platform_after_parse={skipped['invalid_platform_after_parse']} "
            f"skipped_invalid_dates_after_parse={skipped['invalid_dates_after_parse']}"
        ),
    )

    if not order_rows:
        _log_ragic_import(get_db_connection=get_db_connection, batch_id=batch_id, status="failed", phase="summary", message="無可匯入訂單（皆未通過可產生 segment 條件）")
        return False, "此筆 Ragic 沒有可匯入的有效資料（皆未通過可產生 segment 條件）。", batch_id

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
        for t in order_rows:
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
                f"單筆匯入完成：files={parsed_files} rows={len(order_rows)} "
                f"inserted={inserted_count} updated={updated_count} skipped={skipped_count}"
            ),
        )
        return (
            True,
            (
                f"Ragic 單筆匯入完成：{len(order_rows)} 筆（成功檔案={parsed_files}，"
                f"新增 {inserted_count}、更新 {updated_count}、略過 {skipped_count}）"
            ),
            batch_id,
        )
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

