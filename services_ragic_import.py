# -*- coding: utf-8 -*-
"""Ragic 區間匯入服務層。"""

from __future__ import annotations

from datetime import date, datetime
import hashlib
import json
import re
import uuid
from typing import Callable

import pandas as pd

SECONDS_MGMT_REMARK_MAX = 60000
HOUR_COLUMNS = [6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 0, 1]
HOUR_PRIORITY = [8, 16, 20, 12, 10, 14, 18, 22, 7, 9, 11, 13, 15, 17, 19, 21, 23, 6]
PEAK_WINDOW_CAPS = [
    ({7, 8}, 4),    # 7~9（7~8 + 8~9）兩小時合計上限 4
    ({11, 12}, 4),  # 11~13 兩小時合計上限 4
    ({17, 18}, 4),  # 17~19 兩小時合計上限 4
]
NO_SCHEDULE_HOURS = {0, 1}


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
    region: str = "",
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
                region or "",
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
    region: str = "",
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
        str(region or "").strip(),
    )


def _load_existing_order_id_map(get_db_connection: Callable[[], object]) -> dict[tuple, str]:
    mapping: dict[tuple, str] = {}
    conn = None
    try:
        conn = get_db_connection()
        df_old = pd.read_sql(
            """
            SELECT id, platform, client, product, sales, company, start_date, end_date, seconds, spots, contract_id, region
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
                region=r.get("region", ""),
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


def _ensure_orders_hourly_schedule_column(get_db_connection: Callable[[], object]) -> None:
    """保底 migration：舊 DB 若缺排程/時段欄位，匯入前自動補欄。"""
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        cols = [r[1] for r in c.execute("PRAGMA table_info(orders)").fetchall()]
        if "hourly_schedule_json" not in cols:
            c.execute("ALTER TABLE orders ADD COLUMN hourly_schedule_json TEXT")
            conn.commit()
        if "play_time_window" not in cols:
            c.execute("ALTER TABLE orders ADD COLUMN play_time_window TEXT")
            conn.commit()
        if "special_time_window" not in cols:
            c.execute("ALTER TABLE orders ADD COLUMN special_time_window INTEGER")
            conn.commit()
    except Exception:
        pass
    finally:
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass


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
        _norm_text(r.get("hourly_schedule_json", "")),
        _norm_text(r.get("play_time_window", "")),
        int(_norm_num(r.get("special_time_window", 0))),
        _norm_text(r.get("region", "")),
    )


def _signature_from_tuple(t: tuple, effective_seconds_type: str) -> tuple:
    project_val = t[14] if len(t) > 14 else None
    split_val = t[15] if len(t) > 15 else None
    schedule_val = t[16] if len(t) > 16 else ""
    play_window_val = t[17] if len(t) > 17 else ""
    special_window_val = t[18] if len(t) > 18 else 0
    region_val = t[19] if len(t) > 19 else ""
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
        _norm_text(schedule_val),
        _norm_text(play_window_val),
        int(_norm_num(special_window_val)),
        _norm_text(region_val),
    )


def _normalize_allowed_hours(hours: list[int] | None) -> list[int]:
    out: list[int] = []
    for h in list(hours or []):
        try:
            hh = int(h)
        except Exception:
            continue
        if 0 <= hh <= 23 and hh not in NO_SCHEDULE_HOURS and hh not in out:
            out.append(hh)
    return out


def _is_hour_schedule_target(platform_text: str) -> bool:
    s = str(platform_text or "")
    return ("全家" in s and "廣播" in s) or ("新鮮視" in s)


def _default_allowed_hours_for_platform(platform_text: str) -> list[int]:
    s = str(platform_text or "")
    if ("全家" in s and "廣播" in s) or ("企頻" in s):
        return list(range(7, 23))  # 07-23
    if "新鮮視" in s:
        return list(range(7, 23))  # 07-23（基礎設定表）
    return []


def _window_label_from_hours(hours: list[int]) -> str:
    hh = sorted(set(int(x) for x in hours if 0 <= int(x) <= 23))
    if not hh:
        return ""
    return f"{min(hh):02d}-{(max(hh) + 1):02d}"


def _effective_window_for_unit(u: dict, platform_text: str) -> tuple[list[int], str, int]:
    parsed = _normalize_allowed_hours(u.get("allowed_hours"))
    default = _default_allowed_hours_for_platform(platform_text)
    if not parsed:
        return default, "", 0
    if default and parsed == default:
        return default, "", 0
    return parsed, _window_label_from_hours(parsed), 1


def _hour_priority_for_allowed(allowed_hours: list[int]) -> list[int]:
    allowed = _normalize_allowed_hours(allowed_hours)
    if not allowed:
        allowed = [h for h in HOUR_COLUMNS if h not in NO_SCHEDULE_HOURS]
    ordered: list[int] = []
    for h in HOUR_PRIORITY:
        if h in allowed and h not in ordered:
            ordered.append(h)
    for h in allowed:
        if h not in ordered:
            ordered.append(h)
    return ordered


def _allocate_hourly_schedule(
    *,
    spots_per_day: int,
    dates: list[str],
    allowed_hours: list[int],
    contract_day_hour_usage: dict[str, dict[int, int]],
) -> dict[str, int]:
    if int(spots_per_day or 0) <= 0:
        return {}
    allowed = _hour_priority_for_allowed(allowed_hours)
    if not allowed:
        return {}
    per_row: dict[int, int] = {h: 0 for h in allowed}
    prio_idx = {h: i for i, h in enumerate(allowed)}

    def _blocked_by_peak_window(hh: int) -> bool:
        for win_hours, cap in PEAK_WINDOW_CAPS:
            if hh not in win_hours:
                continue
            for d in dates:
                day_use = contract_day_hour_usage.get(str(d), {})
                win_used = 0
                for wh in win_hours:
                    win_used += int(day_use.get(wh, 0)) + int(per_row.get(wh, 0))
                # 本次若把 hh 再加 1，是否超過該窗口上限
                if win_used + 1 > int(cap):
                    return True
        return False

    remain = int(spots_per_day)
    safety = 0
    while remain > 0 and safety < 2000:
        safety += 1
        # 候選時段：先過濾「特殊時段上限」後，按同 CUE 現有使用量最少優先，
        # 同分時才回到既定優先序，避免前幾個素材吃光熱門時段。
        candidates: list[tuple[int, int, int]] = []
        for h in allowed:
            if _blocked_by_peak_window(h):
                continue
            usage_score = 0
            for d in dates:
                usage_score += int(contract_day_hour_usage.get(str(d), {}).get(h, 0)) + int(per_row.get(h, 0))
            candidates.append((usage_score, prio_idx.get(h, 999), h))
        if candidates:
            candidates.sort(key=lambda x: (x[0], x[1]))
            h_pick = candidates[0][2]
            per_row[h_pick] = per_row.get(h_pick, 0) + 1
            remain -= 1
            continue
        # 特殊區間受限時，退而求其次塞到其他時段（仍依優先序）
        peak_hours_flat = set().union(*[wh for wh, _ in PEAK_WINDOW_CAPS])
        non_peak = [h for h in allowed if h not in peak_hours_flat]
        if non_peak:
            best_h = min(non_peak, key=lambda x: (per_row.get(x, 0), prio_idx.get(x, 999)))
        else:
            best_h = min(allowed, key=lambda x: (per_row.get(x, 0), prio_idx.get(x, 999)))
        per_row[best_h] = per_row.get(best_h, 0) + 1
        remain -= 1
    for d in dates:
        day_use = contract_day_hour_usage.setdefault(str(d), {})
        for h, n in per_row.items():
            if n > 0:
                day_use[h] = int(day_use.get(h, 0)) + int(n)
    return {str(h): int(per_row.get(h, 0)) for h in HOUR_COLUMNS if per_row.get(h, 0) > 0}


def _ragic_get_field(entry: dict, name: str, ragic_fields: dict):
    fid = ragic_fields.get(name)
    if fid and isinstance(entry, dict) and entry.get(fid) not in (None, ""):
        return entry.get(fid)
    if isinstance(entry, dict):
        return entry.get(name)
    return None


def _parse_seconds_from_material_title(text: str) -> int | None:
    """從素材／廣告篇名擷取秒數（如 30秒、[30]）。"""
    if not text:
        return None
    s = str(text).strip()
    for pat in (
        r"(\d+)\s*秒",
        r"\[(\d+)\]",
        r"【(\d+)】",
        r"\((\d+)\)\s*秒",
    ):
        m = re.search(pat, s)
        if m:
            try:
                n = int(m.group(1))
                if 5 <= n <= 180:
                    return n
            except (ValueError, TypeError):
                pass
    return None


def _try_parse_json_array_of_objects(s: str) -> list | None:
    """Ragic 偶將子表序列化成 JSON 字串。"""
    t = str(s).strip()
    if len(t) < 2 or t[0] != "[":
        return None
    try:
        j = json.loads(t)
    except Exception:
        return None
    return j if isinstance(j, list) else None


def _material_title_from_subtable_row(row: dict, fid_article_name: str | None) -> str:
    """單一子表列：僅採用「廣告篇名」（或 config 指定之篇名欄位 ID）；缺則空白，不以其他欄位替代。"""
    if not isinstance(row, dict):
        return ""

    def _ok(v) -> bool:
        if v is None:
            return False
        s = str(v).strip()
        return bool(s) and s.lower() != "nan"

    if _ok(row.get("廣告篇名")):
        return str(row.get("廣告篇名")).strip()
    if fid_article_name and _ok(row.get(str(fid_article_name))):
        return str(row.get(str(fid_article_name))).strip()
    return ""


def _collect_ragic_lists_of_dicts(node: object, bucket: list[list], seen_ids: set[int]) -> None:
    """遞迴掃描 Ragic entry（含巢狀子表），收集「含至少一筆 dict 列」的 list（Ragic 偶有空列／非 dict 占位）。"""
    if isinstance(node, dict):
        for v in node.values():
            if isinstance(v, str):
                parsed = _try_parse_json_array_of_objects(v)
                if isinstance(parsed, list):
                    _collect_ragic_lists_of_dicts(parsed, bucket, seen_ids)
                    continue
            _collect_ragic_lists_of_dicts(v, bucket, seen_ids)
        return
    if isinstance(node, list):
        if node and any(isinstance(x, dict) for x in node):
            lid = id(node)
            if lid not in seen_ids:
                seen_ids.add(lid)
                bucket.append(node)
        for x in node:
            if isinstance(x, (dict, list)):
                _collect_ragic_lists_of_dicts(x, bucket, seen_ids)


def _ragic_subtable_list_smells_material(lst: list) -> bool:
    """辨識「訂檔素材」子表，避免誤用收入等其他子表（結構特徵；素材字串仍只取廣告篇名）。"""
    if not lst:
        return False
    hint_keys = frozenset({"廣告篇名", "廣告檔名", "素材音檔", "素材檔名", "1015381", "1015380", "1015382"})
    for r in lst:
        if not isinstance(r, dict):
            continue
        ks = {str(k) for k in r.keys()}
        if ks & hint_keys:
            return True
    return False


def _embedded_material_dict_rows(entry: dict) -> list[dict]:
    """
    連結子列／部分 API 會把素材放在「數字 key 下的獨立 dict」（非 list），
    與主表 dict 區隔：不採用根物件本身。
    """
    if not isinstance(entry, dict):
        return []
    root_id = id(entry)
    found: list[dict] = []
    seen: set[int] = set()

    def walk(o: object) -> None:
        if isinstance(o, dict):
            if id(o) != root_id:
                ks = set(o.keys())
                gv = str(o.get("廣告篇名") or "").strip()
                if "廣告篇名" in ks and gv:
                    oid = id(o)
                    if oid not in seen:
                        seen.add(oid)
                        found.append(o)
            for v in o.values():
                if isinstance(v, str):
                    parsed = _try_parse_json_array_of_objects(v)
                    if isinstance(parsed, list):
                        walk(parsed)
                        continue
                walk(v)
        elif isinstance(o, list):
            for x in o:
                walk(x)

    walk(entry)
    return found


def _extract_ragic_material_filename_rows(
    entry: dict,
    fid_subtable: str | None,
    *,
    fid_article_name: str | None = None,
) -> list[tuple[str, int | None]]:
    """Ragic 素材子表：回傳 (廣告篇名, 從篇名解析的秒數或 None)；篇名缺則空字串，不以檔名等替代。"""
    out: list[tuple[str, int | None]] = []
    if not entry or not isinstance(entry, dict):
        return out

    def _parse_list(val: list) -> list[tuple[str, int | None]]:
        acc: list[tuple[str, int | None]] = []
        for row in val:
            if not isinstance(row, dict):
                continue
            title = _material_title_from_subtable_row(row, fid_article_name)
            ps = _parse_seconds_from_material_title(title) if title else None
            acc.append((title, ps))
        return acc

    if fid_subtable:
        direct = entry.get(str(fid_subtable))
        if isinstance(direct, str):
            parsed = _try_parse_json_array_of_objects(direct)
            if isinstance(parsed, list):
                direct = parsed
        if isinstance(direct, list) and direct:
            dict_rows = [x for x in direct if isinstance(x, dict)]
            if dict_rows:
                tmp = _parse_list(direct)
                if any(str(t).strip() for t, _ in tmp):
                    return tmp

    all_lists: list[list] = []
    _collect_ragic_lists_of_dicts(entry, all_lists, set())

    material_lists = [L for L in all_lists if _ragic_subtable_list_smells_material(L)]
    if not material_lists:
        material_lists = list(all_lists)

    for val in material_lists:
        if not val:
            continue
        dict_rows = [x for x in val if isinstance(x, dict)]
        if not dict_rows:
            continue
        sample_hit = any(
            str(_material_title_from_subtable_row(r, fid_article_name)).strip() for r in dict_rows
        )
        if not sample_hit:
            continue
        for row in dict_rows:
            title = _material_title_from_subtable_row(row, fid_article_name)
            ps = _parse_seconds_from_material_title(title) if title else None
            out.append((title, ps))

    if not any(str(t).strip() for t, _ in out):
        for d in _embedded_material_dict_rows(entry):
            title = _material_title_from_subtable_row(d, fid_article_name)
            if str(title).strip():
                ps = _parse_seconds_from_material_title(title) if title else None
                out.append((title, ps))
    return out


def _ragic_material_display_string(entry: dict, ragic_fields: dict | None) -> str:
    """
    供 Ragic 測試頁／order_info：從子表彙整「廣告篇名」（去重、分號連接）。
    ragic_fields 須含「素材_廣告檔名」流水號；可選「廣告篇名」流水號（API 僅回欄位 ID 時）。
    """
    st = dict(ragic_fields or {})
    fid = st.get("素材_廣告檔名") or st.get("廣告檔名")
    fid_art = st.get("廣告篇名")
    rows = _extract_ragic_material_filename_rows(
        entry,
        str(fid) if fid else None,
        fid_article_name=str(fid_art) if fid_art else None,
    )
    titles: list[str] = []
    seen: set[str] = set()
    for t, _ in rows:
        s = str(t).strip()
        if s and s not in seen:
            seen.add(s)
            titles.append(s)
    return "；".join(titles)


def _material_titles_for_unit_seconds(rows: list[tuple[str, int | None]], unit_seconds: int) -> list[str]:
    """依 CUE 單位秒數，篩選應拆分的素材篇名；無篇名時回傳單一空白。"""
    typed = [(str(t).strip(), ps) for t, ps in rows]
    non_empty = [(t, ps) for t, ps in typed if t]
    if not non_empty:
        return [""]
    exact = [t for t, ps in non_empty if ps == unit_seconds]
    if exact:
        return exact
    wild = [t for t, ps in non_empty if ps is None]
    if wild:
        return wild
    return [""]


def _fair_daily_spot_allocations(daily_totals: list[int], n: int) -> list[list[int]]:
    """
    將每日總檔次公平拆給 n 支素材：每日合計不變、全走期各素材總檔次最多差 1，
    且逐日依「尚欠目標」分配，使後續依檔次變化切段時列數盡量少。
    """
    if n <= 0:
        return []
    if n == 1:
        return [[int(x) for x in daily_totals]]
    total_sum = sum(int(x) for x in daily_totals)
    target_per = [total_sum // n + (1 if i < total_sum % n else 0) for i in range(n)]
    cur = [0] * n
    alloc_by_day: list[list[int]] = []
    for T in daily_totals:
        T = int(T)
        base = T // n
        rem = T % n
        alloc = [base] * n
        deficit = [(target_per[i] - cur[i] - base, -i) for i in range(n)]
        deficit.sort(reverse=True)
        for k in range(rem):
            idx = -deficit[k][1]
            alloc[idx] += 1
        cur = [cur[i] + alloc[i] for i in range(n)]
        alloc_by_day.append(alloc)
    return alloc_by_day


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


def _extract_segments_seconds_type_blocks(note_text: str) -> list[str]:
    """保留既有備註中以【Segments 秒數用途更新紀錄】開頭的附加區塊。"""
    txt = str(note_text or "")
    marker = "【Segments 秒數用途更新紀錄】"
    if marker not in txt:
        return []
    blocks: list[str] = []
    parts = txt.split(marker)
    for p in parts[1:]:
        b = (marker + p).strip()
        if b:
            blocks.append(b)
    return blocks


def _extract_latest_segments_seconds_type_block(note_text: str) -> str:
    """只取最後一段 Segments 秒數用途更新紀錄。"""
    blocks = _extract_segments_seconds_type_blocks(note_text)
    return blocks[-1] if blocks else ""


def _remove_segments_seconds_type_blocks(note_text: str) -> str:
    """
    移除備註中所有【Segments 秒數用途更新紀錄】區塊，保留其他內容。
    """
    txt = str(note_text or "")
    marker = "【Segments 秒數用途更新紀錄】"
    if marker not in txt:
        return txt.strip()
    head = txt.split(marker)[0].rstrip()
    return head.strip()


def _extract_latest_seconds_type_from_note(note_text: str) -> str:
    """
    從「秒數管理(備註)」中擷取最後一次 Segments 更新紀錄的 seconds_type。
    匹配格式：- seconds_type 更新為「銷售秒數」；...
    """
    txt = str(note_text or "")
    if not txt:
        return ""
    try:
        import re

        matches = re.findall(r"seconds_type\s*更新為[「\"]([^」\"\n]+)[」\"]", txt)
        if matches:
            return str(matches[-1]).strip()
    except Exception:
        return ""
    return ""


def _push_seconds_mgmt_to_ragic(
    *,
    ref,
    api_key: str,
    ragic_fields: dict,
    entry_outcomes: dict[str, dict],
    batch_id: str,
    extra_seconds_notes: dict[str, list[str]] | None = None,
    prefetched_entries: dict[str, dict] | None = None,
) -> str:
    from ragic_client import post_update_entry_fields, make_single_record_url, get_json

    fid_flag = ragic_fields.get("秒數管理")
    fid_note = ragic_fields.get("秒數管理(備註)")
    if not fid_flag or not fid_note:
        return "（config 未設定 秒數管理／秒數管理(備註) 流水號，已略過 Ragic 回寫）\n"

    prefetch = prefetched_entries or {}
    extra = extra_seconds_notes or {}
    report_lines: list[str] = ["", "—— Ragic 秒數管理欄位回寫 ——"]
    for rid, state in sorted(entry_outcomes.items(), key=lambda x: x[0]):
        st = dict(state)
        merged_notes = list(st.get("seconds_type_notes") or []) + list(extra.get(rid, []))
        flag = _seconds_mgmt_yes_no(st)
        remark = _compose_seconds_mgmt_remark(state=st, batch_id=batch_id, seconds_type_notes=merged_notes)
        # 保留舊備註中人工更新過的「Segments 秒數用途更新紀錄」
        old_note = ""
        rid_key = str(rid)
        pref = prefetch.get(rid_key)
        if isinstance(pref, dict):
            try:
                old_note = str(
                    _ragic_get_field(pref, "秒數管理(備註)", ragic_fields)
                    or pref.get(str(fid_note))
                    or ""
                )
            except Exception:
                old_note = ""
        else:
            try:
                one_url = make_single_record_url(ref, rid)
                payload, err = get_json(one_url, api_key, timeout=60)
                if not err and isinstance(payload, dict):
                    entry_obj = payload.get(str(rid)) if isinstance(payload.get(str(rid)), dict) else payload
                    if isinstance(entry_obj, dict):
                        old_note = str(
                            _ragic_get_field(entry_obj, "秒數管理(備註)", ragic_fields)
                            or entry_obj.get(str(fid_note))
                            or ""
                        )
            except Exception:
                old_note = ""
        # 只保留最後一段 Segments 秒數用途更新紀錄，避免備註無限膨脹。
        latest_block = _extract_latest_segments_seconds_type_block(old_note)
        if latest_block:
            remark = (remark + "\n\n" + latest_block).strip()
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
    ragic_subtable_fields: dict | None = None,
    parse_cue_excel_for_table1: Callable[..., list],
    normalize_date: Callable[[str], str],
    existing_order_id_map: dict,
    get_db_connection: Callable[[], object],
    batch_id: str,
    max_files: int | None = None,
    progress_cb: Callable[[dict], None] | None = None,
    entry_index: int | None = None,
    entry_total: int | None = None,
    submit_date_display: str = "",
    submit_at_sql: str = "",
) -> tuple[list[tuple[str, tuple]], dict]:
    from ragic_client import parse_file_tokens, download_file
    from services_cue_parser import _split_by_spots_change
    from services_media_platform import parse_platform_region as _parse_platform_region

    ragic_id = entry.get("_ragicId")
    rid_s = str(ragic_id)
    order_no = _ragic_get_field(entry, "訂檔單號", ragic_fields) or f"ragic_{ragic_id}"
    order_no = str(order_no)
    contract_id = str(_ragic_get_field(entry, "CUE", ragic_fields) or "").strip()
    merged_for_material = dict(ragic_fields or {})
    if ragic_subtable_fields:
        merged_for_material.update(ragic_subtable_fields)
    mat_for_merge = _ragic_material_display_string(entry, merged_for_material)
    product_merge = mat_for_merge
    order_info = {
        "client": str(_ragic_get_field(entry, "客戶", ragic_fields) or ""),
        "product": product_merge,
        "sales": str(_ragic_get_field(entry, "業務(開發客戶)", ragic_fields) or ""),
        "company": str(_ragic_get_field(entry, "公司", ragic_fields) or ""),
        "order_id": contract_id,
        "amount_net": 0,
    }
    # Ragic 為秒數用途單一真實來源：匯入時直接採用此值。
    ragic_seconds_type = str(
        _ragic_get_field(entry, "秒數用途", ragic_fields)
        or _ragic_get_field(entry, "seconds_type", ragic_fields)
        or entry.get("秒數用途")
        or entry.get("seconds_type")
        or ""
    ).strip()
    # 若主欄位未填，回退使用「秒數管理(備註)」最後一次更新紀錄，避免再次匯入時遺失用途。
    if not ragic_seconds_type:
        note_text = str(
            _ragic_get_field(entry, "秒數管理(備註)", ragic_fields)
            or entry.get("秒數管理(備註)")
            or ""
        )
        ragic_seconds_type = _extract_latest_seconds_type_from_note(note_text)
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

    if not contract_id:
        state["issues"].append("缺少 CUE 欄位（合約編號；流水號 1015336），無法匯入")
        _log_ragic_import(
            get_db_connection=get_db_connection,
            batch_id=batch_id,
            status="failed",
            phase="summary",
            ragic_id=ragic_id,
            order_no=order_no,
            message="缺少 CUE 欄位（合約編號；流水號 1015336），無法匯入",
        )
        return [], state

    rows_out: list[tuple[str, tuple]] = []
    contract_day_hour_usage: dict[str, dict[int, int]] = {}

    st_sub = dict(ragic_subtable_fields or {})
    fid_material = st_sub.get("素材_廣告檔名") or st_sub.get("廣告檔名")
    fid_article = st_sub.get("廣告篇名")
    material_rows = _extract_ragic_material_filename_rows(
        entry,
        str(fid_material) if fid_material else None,
        fid_article_name=str(fid_article) if fid_article else None,
    )
    submit_disp = (submit_date_display or "").strip() or f"{datetime.now().month}/{datetime.now().day}"
    updated_at_sql = (submit_at_sql or "").strip() or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    client_for_detail = str(_ragic_get_field(entry, "客戶", ragic_fields) or "")

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
        if progress_cb:
            progress_cb(
                {
                    "stage": "file_download_start",
                    "ragic_id": rid_s,
                    "entry_index": entry_index,
                    "entry_total": entry_total,
                    "file_index": file_i,
                    "file_total": len(excel_tokens),
                    "token": tok_short,
                    "message": f"RagicId {rid_s} 檔案 {file_i}/{len(excel_tokens)}：下載中",
                }
            )
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
        if progress_cb:
            progress_cb(
                {
                    "stage": "file_parse_start",
                    "ragic_id": rid_s,
                    "entry_index": entry_index,
                    "entry_total": entry_total,
                    "file_index": file_i,
                    "file_total": len(excel_tokens),
                    "token": tok_short,
                    "message": f"RagicId {rid_s} 檔案 {file_i}/{len(excel_tokens)}：解析中",
                }
            )
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
            dates_list = list(u.get("dates") or [])
            daily_spots = list(u.get("daily_spots") or [])
            if dates_list and daily_spots and len(dates_list) != len(daily_spots):
                m = min(len(dates_list), len(daily_spots))
                dates_list = dates_list[:m]
                daily_spots = daily_spots[:m]
            if (not dates_list) and daily_spots and u.get("start_date") and u.get("end_date"):
                try:
                    dr = pd.date_range(u.get("start_date"), u.get("end_date"), freq="D")
                    dates_list = [d.strftime("%Y-%m-%d") for d in dr][: len(daily_spots)]
                except Exception:
                    pass

            cue_company = (u.get("cue_sheet_company") or "").strip()
            cue_sales = (u.get("cue_sheet_sales") or "").strip()
            eff_company = cue_company or order_info["company"]
            eff_sales = cue_sales or order_info["sales"]

            seconds = int(u.get("seconds") or 0)
            platform = str(u.get("platform") or _ragic_get_field(entry, "平台", ragic_fields) or "")
            region = str(u.get("region") or "").strip()
            platform_for_order = platform
            if region and region != "未知" and (region not in platform_for_order):
                platform_for_order = f"{platform_for_order}-{region}"

            u_start = str(u.get("start_date") or _ragic_get_field(entry, "執行開始日期", ragic_fields) or "")
            u_end = str(u.get("end_date") or _ragic_get_field(entry, "執行結束日期", ragic_fields) or "")

            skip_reason = None
            if seconds <= 0:
                skip_reason = f"秒數無效（秒數={seconds}）"
            elif not daily_spots:
                skip_reason = "無每日檔次資料"
            elif not platform or not u_start or not u_end:
                skip_reason = "缺少平台或起迄日期"
            else:
                parsed_platform, _, _ = _parse_platform_region(platform_for_order)
                if parsed_platform not in ["全家", "家樂福"]:
                    skip_reason = f"平台無法產生 segment（媒體={parsed_platform}，需為全家／家樂福）"

            u_start_norm = normalize_date(u_start) or u_start if u_start else ""
            u_end_norm = normalize_date(u_end) or u_end if u_end else ""
            if skip_reason is None:
                s_date = pd.to_datetime(u_start_norm, errors="coerce")
                e_date = pd.to_datetime(u_end_norm, errors="coerce")
                if pd.isna(s_date) or pd.isna(e_date):
                    skip_reason = f"起迄日期無法解析（{u_start_norm} ~ {u_end_norm}）"

            if skip_reason:
                state["skipped_summaries"].append(
                    f"檔#{file_i} unit#{i + 1} | 平台={platform} | {skip_reason} | {_format_unit_daily_detail(u)}"
                )
                continue

            if not dates_list or len(dates_list) != len(daily_spots):
                state["skipped_summaries"].append(
                    f"檔#{file_i} unit#{i + 1} | 平台={platform} | 逐日日期與檔次長度不符（無法拆分素材） | {_format_unit_daily_detail(u)}"
                )
                continue

            mat_titles = _material_titles_for_unit_seconds(material_rows, seconds)
            n_mat = len(mat_titles)
            daily_totals = [int(x) for x in daily_spots]
            allocs = _fair_daily_spot_allocations(daily_totals, n_mat)

            for mi, prod_name in enumerate(mat_titles):
                per_mat_daily = [allocs[d][mi] for d in range(len(allocs))]
                if sum(per_mat_daily) <= 0:
                    continue
                split_groups = _split_by_spots_change(per_mat_daily, dates_list, dates_list[0], dates_list[-1])
                if not split_groups:
                    continue
                for gi, group in enumerate(split_groups):
                    g_days = int(group.get("days") or 0)
                    ds_list = group.get("daily_spots_list") or []
                    total_spots_g = int(sum(ds_list)) if ds_list else 0
                    spots = (
                        int(round(total_spots_g / max(g_days, 1)))
                        if total_spots_g > 0
                        else int(ds_list[0] if ds_list else 0)
                    )
                    if spots <= 0 or g_days <= 0:
                        continue
                    start_date = str(group.get("start_date") or u_start)
                    end_date = str(group.get("end_date") or u_end)
                    start_date_norm = normalize_date(start_date) or start_date if start_date else ""
                    end_date_norm = normalize_date(end_date) or end_date if end_date else ""
                    s_date = pd.to_datetime(start_date_norm, errors="coerce")
                    e_date = pd.to_datetime(end_date_norm, errors="coerce")
                    if pd.isna(s_date) or pd.isna(e_date):
                        continue

                    unit_idx = i * 1000 + mi * 20 + gi
                    match_key = _order_match_key(
                        platform=platform_for_order,
                        client=order_info["client"],
                        product=str(prod_name),
                        sales=eff_sales,
                        company=eff_company,
                        start_date=start_date_norm,
                        end_date=end_date_norm,
                        seconds=seconds,
                        spots=spots,
                        contract_id=contract_id,
                        region=region,
                    )
                    order_id = existing_order_id_map.get(match_key) or _make_ragic_order_id(
                        ragic_id=str(ragic_id),
                        order_no=str(order_no),
                        file_token=str(token),
                        unit_idx=unit_idx,
                        platform=platform_for_order,
                        client=order_info["client"],
                        product=str(prod_name),
                        sales=eff_sales,
                        company=eff_company,
                        start_date=start_date_norm,
                        end_date=end_date_norm,
                        seconds=seconds,
                        spots=spots,
                        region=region,
                    )
                    group_dates = [str(x) for x in (group.get("dates") or [])]
                    schedule_json = ""
                    play_time_window = ""
                    special_time_window = 0
                    if _is_hour_schedule_target(platform_for_order):
                        allowed_hours, play_time_window, special_time_window = _effective_window_for_unit(u, platform_for_order)
                        schedule_map = _allocate_hourly_schedule(
                            spots_per_day=int(spots),
                            dates=group_dates,
                            allowed_hours=allowed_hours,
                            contract_day_hour_usage=contract_day_hour_usage,
                        )
                        if schedule_map:
                            schedule_json = json.dumps(schedule_map, ensure_ascii=False, separators=(",", ":"))

                    rows_out.append(
                        (
                            rid_s,
                            (
                                order_id,
                                platform_for_order,
                                order_info["client"],
                                str(prod_name),
                                eff_sales,
                                eff_company,
                                start_date_norm,
                                end_date_norm,
                                seconds,
                                spots,
                                0,
                                updated_at_sql,
                                contract_id,
                                ragic_seconds_type,
                                project_amount if project_amount and project_amount > 0 else None,
                                None,
                                schedule_json,
                                play_time_window,
                                special_time_window,
                                region,
                            ),
                        )
                    )
                    detail_row = {
                        "業務": eff_sales,
                        "主管": str(_ragic_get_field(entry, "主管", ragic_fields) or ""),
                        "合約編號": contract_id,
                        "公司": eff_company,
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
                        "秒數用途": ragic_seconds_type,
                        "提交日": submit_disp,
                        "客戶名稱": client_for_detail,
                        "秒數": seconds,
                        "素材": str(prod_name),
                        "起始日": start_date_norm,
                        "終止日": end_date_norm,
                        "走期天數": g_days,
                        "區域": region,
                        "媒體平台": platform_for_order,
                        "播出時段": play_time_window,
                        "特殊時段": "是" if int(special_time_window or 0) == 1 else "否",
                        "時段排程": schedule_json,
                    }
                    col_order = [
                        "業務", "主管", "合約編號", "公司", "實收金額", "除佣實收", "專案實收金額", "拆分金額",
                        "製作成本", "獎金%", "核定獎金", "加發獎金", "業務基金", "協力基金", "秒數用途", "提交日",
                        "客戶名稱", "秒數", "素材", "起始日", "終止日", "走期天數", "區域", "媒體平台", "播出時段", "特殊時段",
                    ]
                    row_text = " | ".join(f"{k}={detail_row.get(k, '')}" for k in col_order)
                    state["uploaded_rows_detail"].append(
                        f"檔#{file_i} unit#{i + 1} mat#{mi + 1}/{n_mat} seg#{gi + 1} | {row_text}"
                    )
                    ds_seg = group.get("dates") or []
                    ds_part = [f"{d}:{sp}檔" for d, sp in zip(ds_seg, ds_list)] if ds_seg and ds_list else []
                    daily_detail = "；".join(ds_part) if ds_part else str(ds_list)
                    special_window_part = (
                        f"特殊播出時段={play_time_window} | " if int(special_time_window or 0) == 1 and play_time_window else ""
                    )
                    state["imported_summaries"].append(
                        f"order_id={order_id} | 平台={platform_for_order} | 素材={prod_name!s} | {seconds}秒 | "
                        f"代表檔次≈{spots}/日 | 秒數用途={ragic_seconds_type or '（空白）'} | 走期={start_date_norm}~{end_date_norm} | "
                        f"{special_window_part}時段排程={schedule_json or '（未排）'} | "
                        f"{daily_detail} | sheet={u.get('source_sheet', '')!s}"
                    )

        imported_now = len(rows_out) - rows_before
        if progress_cb:
            progress_cb(
                {
                    "stage": "file_parse_done",
                    "ragic_id": rid_s,
                    "entry_index": entry_index,
                    "entry_total": entry_total,
                    "file_index": file_i,
                    "file_total": len(excel_tokens),
                    "token": tok_short,
                    "imported_rows": imported_now,
                    "message": f"RagicId {rid_s} 檔案 {file_i}/{len(excel_tokens)}：完成（入庫列 {imported_now}）",
                }
            )
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
    ragic_subtable_fields: dict | None = None,
    parse_cue_excel_for_table1: Callable[..., list],
    get_db_connection: Callable[[], object],
    init_db: Callable[[], None],
    build_ad_flight_segments: Callable[..., object],
    load_platform_settings: Callable[[], dict],
    compute_and_save_split_amount_for_contract: Callable[[str], None],
    sync_sheets_if_enabled: Callable[..., None],
    normalize_date: Callable[[str], str],
    progress_cb: Callable[[dict], None] | None = None,
) -> tuple[bool, str, str, str]:
    def _emit(stage: str, message: str, **extra):
        if progress_cb:
            payload = {"stage": stage, "message": message}
            payload.update(extra)
            try:
                progress_cb(payload)
            except Exception:
                pass

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
    _emit("start", "開始匯入：初始化")
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
        _emit("fetch_page", f"抓取 Ragic 列表 offset={offset}", offset=offset)
        url = make_listing_url(ref, limit=limit, offset=offset, subtables0=False, fts="")
        payload, err = get_json(url, api_key, timeout=60)
        if err:
            _log_ragic_import(get_db_connection=get_db_connection, batch_id=batch_id, status="failed", phase="fetch", message=f"offset={offset} 抓取失敗：{err}")
            return False, f"抓取 Ragic 失敗（offset={offset}）：{err}", batch_id, ""
        if isinstance(payload, dict) and str(payload.get("status", "")).upper() == "ERROR":
            msg = str(payload.get("message", "") or "Ragic status=ERROR")
            _log_ragic_import(get_db_connection=get_db_connection, batch_id=batch_id, status="failed", phase="fetch", message=f"offset={offset} Ragic 回傳錯誤：{msg}")
            return False, f"Ragic 回傳錯誤（offset={offset}）：{msg}", batch_id, ""
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
    _emit("filter_done", f"日期篩選完成：{len(filtered)} 筆", filtered_count=len(filtered))
    _log_ragic_import(get_db_connection=get_db_connection, batch_id=batch_id, status="info", phase="filter", imported_orders=len(filtered), message=f"日期篩選後 entries={len(filtered)}")
    if not filtered:
        return False, "指定日期區間內無資料", batch_id, ""

    _now = datetime.now()
    submit_date_display = f"{_now.month}/{_now.day}"
    submit_at_sql = datetime(_now.year, _now.month, _now.day, 12, 0, 0).strftime("%Y-%m-%d %H:%M:%S")

    existing_order_id_map = _load_existing_order_id_map(get_db_connection)
    entry_outcomes: dict[str, dict] = {}
    staged_rows: list[tuple[str, tuple]] = []
    for idx, entry in enumerate(filtered, start=1):
        rid = str(entry.get("_ragicId") or "")
        _emit("entry_start", f"處理第 {idx}/{len(filtered)} 筆 Ragic（ID={rid}）", entry_index=idx, entry_total=len(filtered), ragic_id=rid)
        chunk, state = _ragic_entry_collect_order_rows(
            entry,
            ref,
            api_key,
            ragic_fields=ragic_fields,
            ragic_subtable_fields=ragic_subtable_fields,
            parse_cue_excel_for_table1=parse_cue_excel_for_table1,
            normalize_date=normalize_date,
            existing_order_id_map=existing_order_id_map,
            get_db_connection=get_db_connection,
            batch_id=batch_id,
            max_files=None,
            progress_cb=progress_cb,
            entry_index=idx,
            entry_total=len(filtered),
            submit_date_display=submit_date_display,
            submit_at_sql=submit_at_sql,
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

    _emit("db_write_start", f"開始寫入本地資料庫：{len(order_rows)} 筆")
    init_db()
    _ensure_orders_hourly_schedule_column(get_db_connection)
    conn = get_db_connection()
    c = conn.cursor()
    try:
        if replace_existing:
            c.execute("DELETE FROM orders")
            _log_ragic_import(
                get_db_connection=get_db_connection,
                batch_id=batch_id,
                status="info",
                phase="replace",
                message="replace_existing=True：已清空 orders 舊資料",
            )
        else:
            contract_ids_in_batch = sorted({str(t[12]).strip() for _, t in staged_rows if len(t) > 12 and str(t[12]).strip()})
            if contract_ids_in_batch:
                placeholders = ",".join(["?"] * len(contract_ids_in_batch))
                c.execute(f"DELETE FROM orders WHERE contract_id IN ({placeholders})", contract_ids_in_batch)
                _log_ragic_import(
                    get_db_connection=get_db_connection,
                    batch_id=batch_id,
                    status="info",
                    phase="replace",
                    message=f"已清除同訂檔單號舊資料：{', '.join(contract_ids_in_batch)}",
                )

        existing_rows: dict[str, dict] = {}
        try:
            df_existing = pd.read_sql(
                """
            SELECT id, platform, client, product, sales, company, start_date, end_date, seconds, spots, amount_net, contract_id, seconds_type, project_amount_net, split_amount, hourly_schedule_json, play_time_window, special_time_window, region
                FROM orders
                """,
                conn,
            )
        except Exception:
            df_existing = pd.read_sql(
                """
                SELECT id, platform, client, product, sales, company, start_date, end_date, seconds, spots, amount_net, contract_id, seconds_type, project_amount_net, split_amount, region
                FROM orders
                """,
                conn,
            )
            df_existing["hourly_schedule_json"] = ""
            df_existing["play_time_window"] = ""
            df_existing["special_time_window"] = 0
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
            incoming_seconds_type = _norm_text(t[13] if len(t) > 13 else "")
            effective_seconds_type = incoming_seconds_type
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
            (id, platform, client, product, sales, company, start_date, end_date, seconds, spots, amount_net, updated_at, contract_id, seconds_type, project_amount_net, split_amount, hourly_schedule_json, play_time_window, special_time_window, region)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
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
                seconds_type=excluded.seconds_type,
                project_amount_net=excluded.project_amount_net,
                split_amount=excluded.split_amount,
                hourly_schedule_json=excluded.hourly_schedule_json,
                play_time_window=excluded.play_time_window,
                special_time_window=excluded.special_time_window,
                region=excluded.region
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
                OR COALESCE(orders.seconds_type, '') != COALESCE(excluded.seconds_type, '')
                OR COALESCE(orders.project_amount_net, 0) != COALESCE(excluded.project_amount_net, 0)
                OR COALESCE(orders.split_amount, 0) != COALESCE(excluded.split_amount, 0)
                OR COALESCE(orders.hourly_schedule_json, '') != COALESCE(excluded.hourly_schedule_json, '')
                OR COALESCE(orders.play_time_window, '') != COALESCE(excluded.play_time_window, '')
                OR COALESCE(orders.special_time_window, 0) != COALESCE(excluded.special_time_window, 0)
                OR COALESCE(orders.region, '') != COALESCE(excluded.region, '')
            """,
            order_rows,
        )
        conn.commit()
        conn.close()
        conn_read = get_db_connection()
        df_orders = pd.read_sql("SELECT * FROM orders", conn_read)
        conn_read.close()
        build_ad_flight_segments(df_orders, load_platform_settings(), write_to_db=True, sync_sheets=False)
        _emit("segments_built", "已完成 segments 重建")
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
        # Ragic 匯入後強制即時同步，避免重啟時本機/雲端快照不一致。
        sync_sheets_if_enabled(only_tables=["Orders", "Segments"], skip_if_unchanged=False)
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
        uploaded_rows_detail_all: list[str] = []
        for st in entry_outcomes.values():
            uploaded_rows_detail_all.extend(list(st.get("uploaded_rows_detail") or []))
        if uploaded_rows_detail_all:
            summary_lines.append("")
            summary_lines.append("【上傳至 orders 的完整欄位快照（每筆）】")
            summary_lines.extend(uploaded_rows_detail_all[:200])
            if len(uploaded_rows_detail_all) > 200:
                summary_lines.append(f"... 其餘 {len(uploaded_rows_detail_all) - 200} 列略")
        summary_lines.append("")
        detail_report = "\n".join(summary_lines) + push_detail
        _log_ragic_import(
            get_db_connection=get_db_connection,
            batch_id=batch_id,
            status="info",
            phase="detail",
            imported_orders=len(order_rows),
            message=detail_report,
        )
        _emit("done", "匯入完成")
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
    ragic_subtable_fields: dict | None = None,
    parse_cue_excel_for_table1: Callable[..., list],
    get_db_connection: Callable[[], object],
    init_db: Callable[[], None],
    build_ad_flight_segments: Callable[..., object],
    load_platform_settings: Callable[[], dict],
    compute_and_save_split_amount_for_contract: Callable[[str], None],
    sync_sheets_if_enabled: Callable[..., None],
    normalize_date: Callable[[str], str],
    progress_cb: Callable[[dict], None] | None = None,
) -> tuple[bool, str, str, str]:
    batch_id = f"ragic_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    _log_ragic_import(get_db_connection=get_db_connection, batch_id=batch_id, status="info", phase="summary", message=f"開始匯入：單筆 ragic_id={ragic_id}")
    _now = datetime.now()
    submit_date_display = f"{_now.month}/{_now.day}"
    submit_at_sql = datetime(_now.year, _now.month, _now.day, 12, 0, 0).strftime("%Y-%m-%d %H:%M:%S")

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
    prefetch_for_push = {str(entry.get("_ragicId", ragic_id)): entry}
    if progress_cb:
        progress_cb(
            {
                "stage": "cue_collect",
                "message": (
                    f"RagicId {ragic_id_str}：下載 CUE 附檔並解析 Excel（檔案大或網路慢時可能需一至數分鐘，並非當機）…"
                ),
            }
        )
    existing_order_id_map = _load_existing_order_id_map(get_db_connection)
    staged_rows, state = _ragic_entry_collect_order_rows(
        entry,
        ref,
        api_key,
        ragic_fields=ragic_fields,
        ragic_subtable_fields=ragic_subtable_fields,
        parse_cue_excel_for_table1=parse_cue_excel_for_table1,
        normalize_date=normalize_date,
        existing_order_id_map=existing_order_id_map,
        get_db_connection=get_db_connection,
        batch_id=batch_id,
        max_files=max_files_per_entry,
        submit_date_display=submit_date_display,
        submit_at_sql=submit_at_sql,
        progress_cb=progress_cb,
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
        if progress_cb:
            progress_cb(
                {
                    "stage": "ragic_push_seconds",
                    "message": f"RagicId {ragic_id_str}：寫回「秒數管理／備註」至 Ragic（約數十秒內應完成）…",
                }
            )
        push_detail = _push_seconds_mgmt_to_ragic(
            ref=ref,
            api_key=api_key,
            ragic_fields=ragic_fields,
            entry_outcomes=entry_outcomes,
            batch_id=batch_id,
            prefetched_entries=prefetch_for_push,
        )
        return False, "此筆 Ragic 沒有可匯入的有效資料（皆未通過可產生 segment 條件）。", batch_id, push_detail

    init_db()
    _ensure_orders_hourly_schedule_column(get_db_connection)
    conn = get_db_connection()
    c = conn.cursor()
    try:
        if replace_existing:
            c.execute("DELETE FROM orders")
            _log_ragic_import(
                get_db_connection=get_db_connection,
                batch_id=batch_id,
                status="info",
                phase="replace",
                ragic_id=ragic_id_str,
                message="replace_existing=True：已清空 orders 舊資料",
            )
        else:
            contract_ids_in_batch = sorted({str(t[12]).strip() for _, t in staged_rows if len(t) > 12 and str(t[12]).strip()})
            if contract_ids_in_batch:
                placeholders = ",".join(["?"] * len(contract_ids_in_batch))
                c.execute(f"DELETE FROM orders WHERE contract_id IN ({placeholders})", contract_ids_in_batch)
                _log_ragic_import(
                    get_db_connection=get_db_connection,
                    batch_id=batch_id,
                    status="info",
                    phase="replace",
                    ragic_id=ragic_id_str,
                    message=f"已清除同訂檔單號舊資料：{', '.join(contract_ids_in_batch)}",
                )

        existing_rows: dict[str, dict] = {}
        try:
            df_existing = pd.read_sql(
                """
            SELECT id, platform, client, product, sales, company, start_date, end_date, seconds, spots, amount_net, contract_id, seconds_type, project_amount_net, split_amount, hourly_schedule_json, play_time_window, special_time_window, region
                FROM orders
                """,
                conn,
            )
        except Exception:
            df_existing = pd.read_sql(
                """
                SELECT id, platform, client, product, sales, company, start_date, end_date, seconds, spots, amount_net, contract_id, seconds_type, project_amount_net, split_amount, region
                FROM orders
                """,
                conn,
            )
            df_existing["hourly_schedule_json"] = ""
            df_existing["play_time_window"] = ""
            df_existing["special_time_window"] = 0
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
            incoming_seconds_type = _norm_text(t[13] if len(t) > 13 else "")
            effective_seconds_type = incoming_seconds_type
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
            (id, platform, client, product, sales, company, start_date, end_date, seconds, spots, amount_net, updated_at, contract_id, seconds_type, project_amount_net, split_amount, hourly_schedule_json, play_time_window, special_time_window, region)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
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
                seconds_type=excluded.seconds_type,
                project_amount_net=excluded.project_amount_net,
                split_amount=excluded.split_amount,
                hourly_schedule_json=excluded.hourly_schedule_json,
                play_time_window=excluded.play_time_window,
                special_time_window=excluded.special_time_window,
                region=excluded.region
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
                OR COALESCE(orders.seconds_type, '') != COALESCE(excluded.seconds_type, '')
                OR COALESCE(orders.project_amount_net, 0) != COALESCE(excluded.project_amount_net, 0)
                OR COALESCE(orders.split_amount, 0) != COALESCE(excluded.split_amount, 0)
                OR COALESCE(orders.hourly_schedule_json, '') != COALESCE(excluded.hourly_schedule_json, '')
                OR COALESCE(orders.play_time_window, '') != COALESCE(excluded.play_time_window, '')
                OR COALESCE(orders.special_time_window, 0) != COALESCE(excluded.special_time_window, 0)
                OR COALESCE(orders.region, '') != COALESCE(excluded.region, '')
            """,
            order_rows,
        )
        conn.commit()
        conn.close()
        if progress_cb:
            progress_cb(
                {
                    "stage": "segments",
                    "message": f"RagicId {ragic_id_str}：重算 segments／拆帳（orders 多時會稍久）…",
                }
            )
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
        # Ragic 匯入後強制即時同步，避免重啟時本機/雲端快照不一致。
        if progress_cb:
            progress_cb(
                {
                    "stage": "sync_sheets",
                    "message": f"RagicId {ragic_id_str}：同步 Google 試算表（網路慢時可能超過一分鐘）…",
                }
            )
        sync_sheets_if_enabled(only_tables=["Orders", "Segments"], skip_if_unchanged=False)
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
        if progress_cb:
            progress_cb(
                {
                    "stage": "ragic_push_seconds",
                    "message": f"RagicId {ragic_id_str}：寫回「秒數管理／備註」至 Ragic…",
                }
            )
        push_detail = _push_seconds_mgmt_to_ragic(
            ref=ref,
            api_key=api_key,
            ragic_fields=ragic_fields,
            entry_outcomes=entry_outcomes,
            batch_id=batch_id,
            prefetched_entries=prefetch_for_push,
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
        _log_ragic_import(
            get_db_connection=get_db_connection,
            batch_id=batch_id,
            status="info",
            phase="detail",
            ragic_id=ragic_id_str,
            imported_orders=len(order_rows),
            message=detail_report,
        )
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


def append_seconds_type_notes_to_ragic_by_contract_service(
    *,
    ragic_url: str,
    api_key: str,
    ragic_fields: dict,
    notes_by_contract: dict[str, list[str]],
    max_fetch: int = 5000,
) -> tuple[int, list[str]]:
    """將 seconds_type 更新紀錄附加到對應合約的 Ragic「秒數管理(備註)」欄位。"""
    msgs: list[str] = []
    if not notes_by_contract:
        return 0, msgs
    if not ragic_url or not str(ragic_url).strip() or not api_key or not str(api_key).strip():
        return 0, ["未提供 Ragic URL/API Key，略過回寫秒數管理備註。"]

    from ragic_client import parse_sheet_url, make_listing_url, get_json, extract_entries, post_update_entry_fields

    fid_note = ragic_fields.get("秒數管理(備註)")
    if not fid_note:
        return 0, ["ragic_fields 未設定「秒數管理(備註)」欄位 id，略過回寫。"]

    contracts = {str(k).strip() for k in notes_by_contract.keys() if str(k).strip()}
    if not contracts:
        return 0, msgs

    ref = parse_sheet_url(ragic_url)
    all_entries: list[dict] = []
    limit = 200
    for offset in range(0, max_fetch, limit):
        url = make_listing_url(ref, limit=limit, offset=offset, subtables0=False, fts="")
        payload, err = get_json(url, api_key, timeout=60)
        if err:
            msgs.append(f"抓取 Ragic 清單失敗（offset={offset}）：{err}")
            break
        entries = extract_entries(payload)
        if not entries:
            break
        all_entries.extend(entries)
        if len(entries) < limit:
            break

    touched = 0
    failed = 0
    now_s = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    matched_contracts: set[str] = set()
    matched_entries = 0

    contract_to_entries: dict[str, list[dict]] = {}
    for entry in all_entries:
        cid = str(_ragic_get_field(entry, "CUE", ragic_fields) or "").strip()
        if cid:
            contract_to_entries.setdefault(cid, []).append(entry)

    missing_contracts = sorted([c for c in contracts if c not in contract_to_entries])
    for cid in missing_contracts:
        msgs.append(f"合約 {cid}：在 Ragic 清單中找不到可回寫的記錄。")

    for entry in all_entries:
        rid = str(entry.get("_ragicId") or "").strip()
        if not rid:
            continue
        contract_id = str(_ragic_get_field(entry, "CUE", ragic_fields) or "").strip()
        if contract_id not in contracts:
            continue
        matched_contracts.add(contract_id)
        matched_entries += 1
        old_note = str(_ragic_get_field(entry, "秒數管理(備註)", ragic_fields) or "")
        lines = [str(x) for x in (notes_by_contract.get(contract_id) or []) if str(x).strip()]
        if not lines:
            continue
        append_block = "\n".join(
            [
                "【Segments 秒數用途更新紀錄】",
                f"時間：{now_s}",
                f"合約編號：{contract_id}",
                *lines,
            ]
        )
        base_note = _remove_segments_seconds_type_blocks(old_note)
        new_note = ((base_note.rstrip() + "\n\n" + append_block) if base_note.strip() else append_block).strip()
        new_note = _truncate_seconds_remark(new_note)
        ok, err = post_update_entry_fields(ref, rid, {str(fid_note): new_note}, api_key)
        if ok:
            touched += 1
            msgs.append(f"RagicId {rid} 已附加 seconds_type 更新紀錄。")
        else:
            failed += 1
            msgs.append(f"RagicId {rid} 回寫失敗：{err}")
    summary = (
        f"回寫摘要：目標合約 {len(contracts)}、命中合約 {len(matched_contracts)}、"
        f"命中記錄 {matched_entries}、成功 {touched}、失敗 {failed}、未命中合約 {len(missing_contracts)}"
    )
    msgs.insert(0, summary)
    return touched, msgs

