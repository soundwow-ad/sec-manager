# -*- coding: utf-8 -*-
"""ROI 服務層：統一計算與日期範圍查詢。"""

from __future__ import annotations

import calendar
from typing import Callable, Sequence, Tuple

import pandas as pd


def get_roi_all_period_date_range(
    *,
    get_db_connection: Callable[[], object],
) -> Tuple[str | None, str | None]:
    """取得「累計至今」的實際統計日期範圍。"""
    try:
        conn = get_db_connection()
        df_pur = pd.read_sql("SELECT year, month FROM platform_monthly_purchase", conn)
        df_seg = pd.read_sql("SELECT start_date, end_date FROM ad_flight_segments WHERE start_date IS NOT NULL AND end_date IS NOT NULL", conn)
        conn.close()
        dates = []
        if not df_pur.empty:
            for _, r in df_pur.iterrows():
                try:
                    y = int(r["year"])
                    m = int(r["month"])
                    dates.append(pd.Timestamp(y, m, 1))
                    _, nd = calendar.monthrange(y, m)
                    dates.append(pd.Timestamp(y, m, nd))
                except Exception:
                    pass
        if not df_seg.empty:
            df_seg["start_date"] = pd.to_datetime(df_seg["start_date"], errors="coerce")
            df_seg["end_date"] = pd.to_datetime(df_seg["end_date"], errors="coerce")
            dates.extend(df_seg["start_date"].dropna().tolist())
            dates.extend(df_seg["end_date"].dropna().tolist())
        if not dates:
            return None, None
        start_d = min(dates)
        end_d = max(dates)
        return start_d.strftime("%Y/%m/%d"), end_d.strftime("%Y/%m/%d")
    except Exception:
        return None, None


def calculate_roi_by_period(
    *,
    period_type: str,
    year: int,
    month: int,
    period_label: str,
    media_platform_options: Sequence[str],
    get_revenue_per_media_by_period: Callable[[str, int, int], dict],
    get_cost_per_media_by_period: Callable[[str, int, int], dict],
) -> list[dict]:
    """依時間維度計算各媒體 ROI。"""
    revenue_per_media = get_revenue_per_media_by_period(period_type, year, month)
    cost_per_media = get_cost_per_media_by_period(period_type, year, month)
    media_set = set(media_platform_options)
    media_set.update(revenue_per_media.keys())
    media_set.update(cost_per_media.keys())
    rows = []
    for mp in sorted(media_set):
        cost_row = cost_per_media.get(mp)
        if cost_row is None or not cost_row[0] or cost_row[0] <= 0:
            continue
        purchased_sec, purchase_cost = cost_row[0], cost_row[1]
        revenue = int(revenue_per_media.get(mp, 0) or 0)
        roi = ((revenue - purchase_cost) / purchase_cost) if purchase_cost > 0 else 0
        rows.append(
            {
                "媒體": mp,
                "時間區間": period_label,
                "購買秒數": int(purchased_sec),
                "購買成本（元）": round(purchase_cost, 0),
                "實收金額（元）": revenue,
                "ROI（投報率）": round(roi, 2),
            }
        )
    return rows


def compute_and_save_split_amount_for_contract(
    *,
    contract_key: str,
    get_db_connection: Callable[[], object],
    sync_sheets_if_enabled: Callable[..., object],
):
    if not contract_key:
        return
    try:
        conn = get_db_connection()
        df_ord = pd.read_sql(
            "SELECT id, contract_id, project_amount_net FROM orders WHERE contract_id = ? OR id = ?",
            conn,
            params=(str(contract_key), str(contract_key)),
        )
        if df_ord.empty:
            conn.close()
            return
        project_amt = pd.to_numeric(df_ord["project_amount_net"].iloc[0], errors="coerce")
        if pd.isna(project_amt) or project_amt <= 0:
            conn.close()
            return
        order_ids = df_ord["id"].tolist()
        placeholders = ",".join(["?"] * len(order_ids))
        df_seg = pd.read_sql(
            f"SELECT source_order_id, total_store_seconds FROM ad_flight_segments WHERE source_order_id IN ({placeholders})",
            conn,
            params=order_ids,
        )
        conn.close()
        if df_seg.empty:
            return
        order_seconds = df_seg.groupby("source_order_id")["total_store_seconds"].sum().to_dict()
        total_sec = sum(order_seconds.values()) or 1
        conn = get_db_connection()
        for oid in order_ids:
            sec = order_seconds.get(oid, 0) or 0
            split_val = project_amt * (sec / total_sec)
            conn.execute("UPDATE orders SET split_amount = ? WHERE id = ?", (round(split_val, 2), oid))
        conn.commit()
        conn.close()
        sync_sheets_if_enabled()
    except Exception:
        try:
            conn.close()
        except Exception:
            pass


def get_revenue_per_media_by_period(
    *,
    period_type: str,
    year: int,
    month: int | None,
    get_db_connection: Callable[[], object],
) -> dict:
    try:
        conn = get_db_connection()
        df_ord = pd.read_sql("SELECT id, contract_id, amount_net, split_amount FROM orders", conn)
        df_seg = pd.read_sql(
            "SELECT source_order_id, media_platform, total_store_seconds, start_date, end_date FROM ad_flight_segments "
            "WHERE media_platform IS NOT NULL AND total_store_seconds IS NOT NULL",
            conn,
        )
        conn.close()
    except Exception:
        return {}
    if df_ord.empty or df_seg.empty:
        return {}
    df_seg["start_date"] = pd.to_datetime(df_seg["start_date"], errors="coerce")
    df_seg["end_date"] = pd.to_datetime(df_seg["end_date"], errors="coerce")
    df_seg = df_seg.dropna(subset=["start_date", "end_date"])
    if period_type == "month" and month is not None:
        _, ndays = calendar.monthrange(int(year), int(month))
        period_start = pd.Timestamp(year, month, 1)
        period_end = pd.Timestamp(year, month, ndays)
    elif period_type == "quarter" and month is not None:
        q = (int(month) - 1) // 3 + 1
        start_m = (q - 1) * 3 + 1
        end_m = q * 3
        period_start = pd.Timestamp(year, start_m, 1)
        _, ndays = calendar.monthrange(year, end_m)
        period_end = pd.Timestamp(year, end_m, ndays)
    elif period_type == "year":
        period_start = pd.Timestamp(year, 1, 1)
        period_end = pd.Timestamp(year, 12, 31)
    else:
        period_start = pd.Timestamp(2000, 1, 1)
        period_end = pd.Timestamp(2100, 12, 31)
    df_seg = df_seg[(df_seg["start_date"] <= period_end) & (df_seg["end_date"] >= period_start)]
    if df_seg.empty:
        return {}
    df_seg = df_seg.merge(df_ord, left_on="source_order_id", right_on="id", how="left")
    df_seg["split_amount"] = pd.to_numeric(df_seg["split_amount"], errors="coerce").fillna(0)
    if (df_seg["split_amount"] > 0).any():
        rev_by_media = df_seg.groupby("media_platform")["split_amount"].sum()
        return {k: int(round(v)) for k, v in rev_by_media.items() if v and v > 0}
    df_seg["contract_key"] = df_seg["contract_id"].fillna(df_seg["source_order_id"])
    df_seg["amount_net"] = pd.to_numeric(df_seg["amount_net"], errors="coerce").fillna(0)
    contract_total = df_ord.copy()
    contract_total["contract_key"] = contract_total["contract_id"].fillna(contract_total["id"])
    contract_total["amount_net"] = pd.to_numeric(contract_total["amount_net"], errors="coerce").fillna(0)
    contract_total = contract_total.groupby("contract_key")["amount_net"].sum().to_dict()
    seg_seconds = df_seg.groupby(["contract_key", "media_platform"])["total_store_seconds"].sum().reset_index()
    contract_seconds = df_seg.groupby("contract_key")["total_store_seconds"].sum().to_dict()
    revenue_per_media = {}
    for (contract_key, media_platform), grp in seg_seconds.groupby(["contract_key", "media_platform"]):
        media_sec = int(grp["total_store_seconds"].sum())
        total_sec = contract_seconds.get(contract_key, 0) or 1
        rev = contract_total.get(contract_key, 0)
        revenue_per_media[media_platform] = revenue_per_media.get(media_platform, 0) + rev * (media_sec / total_sec)
    return {k: int(round(v)) for k, v in revenue_per_media.items()}


def get_cost_per_media_by_period(
    *,
    period_type: str,
    year: int,
    month: int | None,
    get_db_connection: Callable[[], object],
) -> dict:
    try:
        conn = get_db_connection()
        if period_type == "all":
            df = pd.read_sql("SELECT media_platform, purchased_seconds, purchase_price FROM platform_monthly_purchase", conn)
        else:
            df = pd.read_sql(
                "SELECT media_platform, year, month, purchased_seconds, purchase_price FROM platform_monthly_purchase WHERE year=?",
                conn,
                params=(int(year),),
            )
        conn.close()
    except Exception:
        return {}
    if df.empty:
        return {}
    df["purchased_seconds"] = pd.to_numeric(df["purchased_seconds"], errors="coerce").fillna(0)
    df["purchase_price"] = pd.to_numeric(df["purchase_price"], errors="coerce").fillna(0)
    if period_type == "month" and month is not None:
        df = df[df["month"] == int(month)]
    elif period_type == "quarter" and month is not None:
        q = (int(month) - 1) // 3 + 1
        start_m, end_m = (q - 1) * 3 + 1, q * 3
        df = df[(df["month"] >= start_m) & (df["month"] <= end_m)]
    elif period_type in ("year", "all"):
        pass
    else:
        return {}
    out = df.groupby("media_platform").agg({"purchased_seconds": "sum", "purchase_price": "sum"}).to_dict("index")
    return {k: (int(v["purchased_seconds"]), float(v["purchase_price"])) for k, v in out.items()}

