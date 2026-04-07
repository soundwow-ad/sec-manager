# -*- coding: utf-8 -*-
"""平台設定、採購與容量服務層。"""

from __future__ import annotations

import calendar
import re
import pandas as pd

SETTINGS_SHEET_ID = "1g36WdYPLQgWk20VkPN7cOmyTDAl3Lp8vFd_v4ptmRec"
SETTINGS_SHEET_URL = f"https://docs.google.com/spreadsheets/d/{SETTINGS_SHEET_ID}/edit?usp=sharing"


def _sheet_csv_url(sheet_name: str) -> str:
    return f"https://docs.google.com/spreadsheets/d/{SETTINGS_SHEET_ID}/gviz/tq?tqx=out:csv&sheet={sheet_name}"


def get_external_settings_status() -> dict:
    """
    回傳外部設定表讀取狀態，供 UI 顯示健康檢查。
    """
    out = {
        "ok": False,
        "sheet_url": SETTINGS_SHEET_URL,
        "checked_at": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
        "pricing_rows": 0,
        "stores_rows": 0,
        "pricing_columns": [],
        "stores_columns": [],
        "missing_pricing_columns": [],
        "missing_stores_columns": [],
        "error": "",
    }
    try:
        df_pr = pd.read_csv(_sheet_csv_url("Pricing"), dtype=str).fillna("")
        df_st = pd.read_csv(_sheet_csv_url("Stores"), dtype=str).fillna("")
        out["pricing_rows"] = int(len(df_pr))
        out["stores_rows"] = int(len(df_st))
        out["pricing_columns"] = [str(c).strip() for c in df_pr.columns.tolist()]
        out["stores_columns"] = [str(c).strip() for c in df_st.columns.tolist()]
        need_pr = {"Media", "Region", "Day_Part"}
        need_st = {"Key", "Count"}
        miss_pr = sorted(list(need_pr - set(out["pricing_columns"])))
        miss_st = sorted(list(need_st - set(out["stores_columns"])))
        out["missing_pricing_columns"] = miss_pr
        out["missing_stores_columns"] = miss_st
        out["ok"] = (len(miss_pr) == 0 and len(miss_st) == 0)
        if not out["ok"]:
            out["error"] = (
                f"欄位缺失：Pricing 缺 {', '.join(miss_pr) if miss_pr else '無'}；"
                f"Stores 缺 {', '.join(miss_st) if miss_st else '無'}"
            )
    except Exception as e:
        out["error"] = str(e)
        out["ok"] = False
    return out


def _norm_region_name(region: str) -> str:
    s = str(region or "").strip()
    if not s:
        return ""
    mapping = {
        "北區": "北北基",
        "北北基": "北北基",
        "中區": "中彰投",
        "中彰投": "中彰投",
        "高屏": "高高屏",
        "高高屏": "高高屏",
        "東區": "宜花東",
        "宜花東": "宜花東",
        "全台": "全省",
    }
    s = s.replace("台", "臺") if s == "全台" else s
    return mapping.get(s, s)


def _norm_media_name(media: str) -> str:
    s = str(media or "").strip()
    if "新鮮視" in s:
        return "新鮮視"
    if "家樂福" in s:
        return "家樂福"
    if "廣播" in s or "企頻" in s or "全家" in s:
        return "全家廣播"
    return s


def _parse_day_part_to_hours(day_part: str) -> int:
    txt = str(day_part or "").strip()
    m = re.search(r"(\d{1,2})[:：]?\d{0,2}\s*[-~～到至]\s*(\d{1,2})[:：]?\d{0,2}", txt)
    if not m:
        return 18
    st = int(m.group(1))
    ed = int(m.group(2))
    if ed <= st:
        ed += 24
    return max(1, min(24, ed - st))


def get_platform_monthly_purchase(*, get_db_connection, media_platform, year, month):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        "SELECT purchased_seconds, purchase_price FROM platform_monthly_purchase WHERE media_platform=? AND year=? AND month=?",
        (media_platform, int(year), int(month)),
    )
    row = c.fetchone()
    conn.close()
    return row if row is not None else None


def set_platform_monthly_purchase(
    *,
    get_db_connection,
    sync_sheets_if_enabled,
    media_platform,
    year,
    month,
    purchased_seconds,
    purchase_price,
):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        """
        INSERT OR REPLACE INTO platform_monthly_purchase (media_platform, year, month, purchased_seconds, purchase_price)
        VALUES (?, ?, ?, ?, ?)
    """,
        (media_platform, int(year), int(month), int(purchased_seconds), float(purchase_price)),
    )
    ndays = calendar.monthrange(int(year), int(month))[1]
    daily_seconds = int(purchased_seconds) // ndays if ndays else 0
    c.execute(
        """
        INSERT OR REPLACE INTO platform_monthly_capacity (media_platform, year, month, daily_available_seconds)
        VALUES (?, ?, ?, ?)
    """,
        (media_platform, int(year), int(month), daily_seconds),
    )
    conn.commit()
    conn.close()
    sync_sheets_if_enabled()


def load_platform_monthly_purchase_for_year(*, get_db_connection, media_platform, year):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        "SELECT month, purchased_seconds, purchase_price FROM platform_monthly_purchase WHERE media_platform=? AND year=?",
        (media_platform, int(year)),
    )
    out = {row[0]: (row[1], row[2]) for row in c.fetchall()}
    conn.close()
    return out


def load_platform_monthly_purchase_all_media_for_year(*, get_db_connection, year):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        "SELECT media_platform, month, purchased_seconds, purchase_price FROM platform_monthly_purchase WHERE year=?",
        (int(year),),
    )
    out = {}
    for row in c.fetchall():
        mp, mo, sec, pr = row[0], row[1], row[2], row[3]
        if mp not in out:
            out[mp] = {}
        out[mp][mo] = (sec, pr)
    conn.close()
    return out


def load_platform_settings(*, get_db_connection):
    """
    平台設定改由外部 Google Sheet 即時讀取：
    - Pricing：Media/Region/Day_Part
    - Stores：Key/Count
    """
    _ = get_db_connection  # 相容既有呼叫簽名
    settings: dict[str, dict] = {}
    try:
        df_pr = pd.read_csv(_sheet_csv_url("Pricing"), dtype=str).fillna("")
        df_st = pd.read_csv(_sheet_csv_url("Stores"), dtype=str).fillna("")
    except Exception:
        return settings

    stores_default_by_region: dict[str, int] = {}
    stores_media_region: dict[tuple[str, str], int] = {}
    for _, r in df_st.iterrows():
        key = str(r.get("Key", "")).strip()
        try:
            cnt = int(float(str(r.get("Count", "")).strip() or "0"))
        except Exception:
            cnt = 0
        if not key or cnt <= 0:
            continue
        if "_" in key:
            m, rg = key.split("_", 1)
            media = _norm_media_name(m)
            region = _norm_region_name(rg)
            stores_media_region[(media, region)] = cnt
        else:
            region = _norm_region_name(key)
            stores_default_by_region[region] = cnt

    for _, r in df_pr.iterrows():
        media = _norm_media_name(r.get("Media", ""))
        region = _norm_region_name(r.get("Region", ""))
        day_part = str(r.get("Day_Part", "")).strip()
        daily_hours = _parse_day_part_to_hours(day_part)
        store_count = stores_media_region.get((media, region), stores_default_by_region.get(region, 1))

        keys = set()
        if media == "全家廣播":
            keys.update({f"全家廣播-{region}", f"全家廣播{region}", f"全家廣播(企頻)-{region}", "全家廣播"})
        elif media == "新鮮視":
            keys.update({f"新鮮視-{region}", f"新鮮視{region}", f"全家新鮮視-{region}", "全家新鮮視", "新鮮視"})
        elif media == "家樂福":
            keys.update({f"家樂福-{region}", f"家樂福{region}", "家樂福"})
        else:
            keys.update({f"{media}-{region}", media})
        keys.add(f"REGION:{region}")
        for k in keys:
            settings[k] = {"store_count": int(store_count), "daily_hours": int(daily_hours), "play_window": day_part}
    return settings


def get_platform_monthly_capacity(*, get_db_connection, media_platform, year, month):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        "SELECT daily_available_seconds FROM platform_monthly_capacity WHERE media_platform=? AND year=? AND month=?",
        (media_platform, int(year), int(month)),
    )
    row = c.fetchone()
    conn.close()
    return row[0] if row is not None else None


def set_platform_monthly_capacity(*, get_db_connection, sync_sheets_if_enabled, media_platform, year, month, daily_available_seconds):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        """
        INSERT OR REPLACE INTO platform_monthly_capacity (media_platform, year, month, daily_available_seconds)
        VALUES (?, ?, ?, ?)
    """,
        (media_platform, int(year), int(month), int(daily_available_seconds)),
    )
    conn.commit()
    conn.close()
    sync_sheets_if_enabled()


def load_platform_monthly_capacity_for(*, get_db_connection, year, month):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        "SELECT media_platform, daily_available_seconds FROM platform_monthly_capacity WHERE year=? AND month=?",
        (int(year), int(month)),
    )
    out = {row[0]: row[1] for row in c.fetchall()}
    conn.close()
    return out


def save_platform_settings(*, get_db_connection, sync_sheets_if_enabled, platform, store_count, daily_hours):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        """
        INSERT OR REPLACE INTO platform_settings (platform, store_count, daily_hours)
        VALUES (?, ?, ?)
    """,
        (platform, store_count, daily_hours),
    )
    conn.commit()
    conn.close()
    sync_sheets_if_enabled()

