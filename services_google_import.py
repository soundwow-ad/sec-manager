# -*- coding: utf-8 -*-
"""Google Sheet 匯入服務層。"""

from __future__ import annotations

import io
import re
from datetime import datetime
from typing import Callable

import pandas as pd
import requests


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
    split_val = None
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


def normalize_date(val) -> str:
    if pd.isna(val) or val == "" or val == "nan":
        return ""
    val = str(val).strip()
    if not val:
        return ""
    try:
        dt = pd.to_datetime(val, errors="coerce")
        if pd.isna(dt):
            return ""
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return ""


def extract_google_sheet_id(url_or_id):
    s = (url_or_id or "").strip()
    if not s:
        return None
    if "/" not in s and len(s) > 20:
        return s
    m = re.search(r"/d/([a-zA-Z0-9_-]{40,})", s)
    return m.group(1) if m else None


def fetch_google_sheet_as_dataframe(sheet_id, gid=0):
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        content = r.content
        if content.startswith(b"\xef\xbb\xbf"):
            content = content[3:]
        df_raw = pd.read_csv(io.BytesIO(content), encoding="utf-8", header=None, dtype=str)
    except Exception as e:
        return None, str(e)
    if df_raw.empty or len(df_raw) < 2:
        return None, "試算表為空或列數不足"
    header_row = None
    for i in range(min(10, len(df_raw))):
        row_str = " ".join(df_raw.iloc[i].astype(str).fillna(""))
        if "平台" in row_str and ("起始日" in row_str or "終止日" in row_str):
            header_row = i
            break
    if header_row is None:
        return None, "找不到表1結構的表頭列（需含：平台、起始日/終止日）"
    df = pd.read_csv(io.BytesIO(content), encoding="utf-8", header=header_row, dtype=str)
    df = df.dropna(how="all", axis=1).dropna(how="all", axis=0)
    df.columns = [str(c).strip() for c in df.columns]
    return df, None


def sheet_row_to_order(row, row_index, col_map, normalize_seconds_type: Callable[[str], str]):
    def get(k, default=""):
        keys = [col_map.get(k, k)]
        if k == "client":
            keys = ["客戶名稱", "HYUNDAI_CUSTIN", "客戶", col_map.get(k, k)]
        for key in keys:
            if not key or key not in row.index:
                continue
            v = row.get(key, default)
            if pd.isna(v) or v == "nan":
                continue
            s = str(v).strip()
            if s:
                return s
        return default

    platform = get("platform") or get("平台")
    if not platform:
        return None
    start_date = normalize_date(get("start_date") or get("起始日"))
    end_date = normalize_date(get("end_date") or get("終止日"))
    if not start_date or not end_date:
        return None
    try:
        seconds = int(float(get("seconds") or get("秒數") or 0))
    except (ValueError, TypeError):
        seconds = 0
    try:
        spots = int(float(get("spots") or get("每天總檔次") or get("委刊總檔數") or get("委刋總檔數") or 0))
    except (ValueError, TypeError):
        spots = 0
    try:
        amount_net = float(get("amount_net") or get("實收金額") or 0)
    except (ValueError, TypeError):
        amount_net = 0
    client = get("client")
    product = get("product") or get("素材")
    sales = get("sales") or get("業務")
    company = get("company") or get("公司")
    contract_id = get("contract_id") or get("合約編號")
    # 嚴謹口徑：若試算表沒有填秒數用途，就保留空值（不要硬推銷售秒數）
    seconds_type = normalize_seconds_type(get("seconds_type") or get("秒數用途") or "")
    try:
        project_amount_net = float(get("project_amount_net") or get("專案實收金額") or 0)
    except (ValueError, TypeError):
        project_amount_net = 0
    if project_amount_net <= 0:
        project_amount_net = None
    updated_at = get("updated_at") or get("提交日") or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if not updated_at or updated_at == "":
        updated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    else:
        updated_at = normalize_date(updated_at)
        if not updated_at:
            updated_at = datetime.now().strftime("%Y-%m-%d")
        updated_at = updated_at + " 00:00:00" if len(updated_at) == 10 else updated_at
    # 重要：避免同一筆訂單在 Google Sheet 出現重複列時，因 `row_index`
    # 造成 orders.id 不同而無法合併（INSERT OR REPLACE 也合併不起來）。
    # 這個 stable_key 用到「定義該筆訂單」的核心欄位，讓重複列能覆蓋同一筆 orders。
    import hashlib

    stable_key = "|".join(
        map(
            str,
            [
                contract_id or "",
                platform or "",
                client or "",
                product or "",
                sales or "",
                company or "",
                start_date or "",
                end_date or "",
                seconds,
                spots,
                seconds_type or "",
            ],
        )
    )
    digest = hashlib.sha1(stable_key.encode("utf-8")).hexdigest()[:12]
    order_id = f"gs_{contract_id or 'na'}_{start_date}_{end_date}_{digest}".replace(" ", "_")[:200]

    # 預先套用與 build_ad_flight_segments 相同的「可產生 segment 條件」
    # - seconds/spots 需 > 0
    # - platform 經 parse 後必須是全家/家樂福
    # - start/end 需可解析成日期
    if seconds <= 0 or spots <= 0:
        return None
    try:
        from services_media_platform import parse_platform_region as _parse_platform_region

        parsed_platform, _, _ = _parse_platform_region(platform)
        if parsed_platform not in ["全家", "家樂福"]:
            return None
        if pd.isna(pd.to_datetime(start_date, errors="coerce")) or pd.isna(pd.to_datetime(end_date, errors="coerce")):
            return None
    except Exception:
        return None
    return (
        order_id,
        platform,
        client or "",
        product or "",
        sales or "",
        company or "",
        start_date,
        end_date,
        seconds,
        spots,
        amount_net,
        updated_at,
        contract_id or None,
        seconds_type or "",
        project_amount_net,
    )


def import_google_sheet_to_orders_service(
    *,
    url_or_id: str,
    replace_existing: bool,
    normalize_seconds_type: Callable[[str], str],
    merge_orders_by_contract_id: bool = False,
    init_db: Callable[[], None],
    get_db_connection: Callable[[], object],
    load_platform_settings: Callable[[], dict],
    build_ad_flight_segments: Callable[..., object],
    compute_and_save_split_amount_for_contract: Callable[[str], None],
    sync_sheets_if_enabled: Callable[..., None],
) -> tuple[bool, str]:
    sheet_id = extract_google_sheet_id(url_or_id)
    if not sheet_id:
        return False, "請輸入有效的 Google 試算表網址或 ID"
    df, err = fetch_google_sheet_as_dataframe(sheet_id)
    if err:
        return False, f"無法讀取試算表：{err}"

    if "合約編號" not in df.columns:
        return False, "試算表缺少「合約編號」欄位，無法匯入（此欄位必填）。"

    contract_series = df["合約編號"]
    missing_contract_mask = contract_series.isna() | contract_series.astype(str).str.strip().eq("")
    if missing_contract_mask.any():
        bad_rows = (df.index[missing_contract_mask] + 2).tolist()
        sample_rows = ", ".join(str(x) for x in bad_rows[:10])
        more = f" 等 {len(bad_rows)} 列" if len(bad_rows) > 10 else ""
        return False, f"試算表有列缺少「合約編號」（例如第 {sample_rows} 列{more}），無法匯入。"

    col_map = {
        "platform": "平台",
        "company": "公司",
        "sales": "業務",
        "contract_id": "合約編號",
        "client": "客戶名稱",
        "product": "素材",
        "start_date": "起始日",
        "end_date": "終止日",
        "seconds": "秒數",
        "spots": "每天總檔次",
        "amount_net": "實收金額",
        "seconds_type": "秒數用途",
        "updated_at": "提交日",
        "客戶": "客戶名稱",
        "HYUNDAI_CUSTIN": "客戶名稱",
        "委刊總檔數": "委刊總檔數",
        "委刋總檔數": "委刋總檔數",
        "project_amount_net": "專案實收金額",
        "專案實收金額": "專案實收金額",
    }
    orders = []
    for i, (_, row) in enumerate(df.iterrows()):
        t = sheet_row_to_order(row, i, col_map, normalize_seconds_type)
        if t is not None:
            orders.append(t)
    if not orders:
        return False, "沒有可匯入的資料列（需有平台、起始日、終止日且為有效日期）"

    # 預留設計：若未來 Google Sheet 可提供 contract_id，
    # 才啟用「同合約內容相同才合併去重」。
    # 目標是：避免重複列造成 orders/segments 放大，同時不破壞不同切分（start/end/spots/seconds 等不同）。
    if merge_orders_by_contract_id:
        # orders tuple 結構：
        # (0)id,(1)platform,(2)client,(3)product,(4)sales,(5)company,(6)start_date,(7)end_date,(8)seconds,(9)spots,
        # (10)amount_net,(11)updated_at,(12)contract_id,(13)seconds_type,(14)project_amount_net
        by_contract: dict[str, list[tuple]] = {}
        for t in orders:
            cid = t[12]
            cid_key = str(cid).strip() if cid not in (None, "") else ""
            if not cid_key:
                continue
            by_contract.setdefault(cid_key, []).append(t)

        if by_contract:
            keep_ids = set()
            # 對每個 contract_id：若所有欄位除 id/updated_at 外都相同，則合併（保留第一筆）
            # 若內容不同，因為切分可能不同，就不要合併，以免缺少應產生的 segment。
            for cid_key, rows in by_contract.items():
                if len(rows) <= 1:
                    continue
                # 以第一筆為基準
                base = rows[0]
                def content_sig(x: tuple) -> tuple:
                    return (
                        x[1], x[2], x[3], x[4], x[5],
                        x[6], x[7], x[8], x[9],
                        x[10], x[12], x[13], x[14],
                    )
                sig = content_sig(base)
                if all(content_sig(x) == sig for x in rows):
                    keep_ids.add(str(base[0]))
                else:
                    # 不合併：保留原本所有 rows（因為切分不同）
                    for x in rows:
                        keep_ids.add(str(x[0]))

            # 對於沒有 contract_id 的 rows，照原樣保留
            new_orders = []
            for t in orders:
                if str(t[0]) in keep_ids:
                    new_orders.append(t)
                elif t[12] in (None, ""):
                    new_orders.append(t)
            orders = new_orders

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

        # 商業規則：不清空重建；僅在資料有變動時更新。
        # 且若匯入列 seconds_type 為空，保留既有 seconds_type（避免覆蓋人工修正）。
        for t in orders:
            project_val = t[14] if len(t) > 14 else None
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

            c.execute(
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
                (*t[:14], project_val, None),
            )
        conn.commit()
        conn.close()
        conn_read = get_db_connection()
        df_orders = pd.read_sql("SELECT * FROM orders", conn_read)
        conn_read.close()
        custom_settings = load_platform_settings()
        build_ad_flight_segments(df_orders, custom_settings, write_to_db=True, sync_sheets=False)
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
        return True, (
            f"已處理 {len(orders)} 筆（新增 {inserted_count}、更新 {updated_count}、略過 {skipped_count}）；"
            "若有專案實收金額已自動計算拆分金額。"
        )
    except Exception as e:
        conn.rollback()
        conn.close()
        return False, str(e)

