import uuid
from datetime import datetime

import pandas as pd


def build_ad_flight_segments(
    df_orders,
    custom_settings=None,
    write_to_db=True,
    sync_sheets=True,
    parse_platform_region_fn=None,
    get_media_platform_display_fn=None,
    get_store_count_fn=None,
    should_multiply_store_count_fn=None,
    normalize_seconds_type_fn=None,
    get_db_connection_fn=None,
    sync_sheets_if_enabled_fn=None,
):
    if df_orders.empty:
        return pd.DataFrame()
    segments = []
    for order_id, group in df_orders.groupby("id"):
        group = group.sort_values("start_date")
        for _, row in group.iterrows():
            try:
                if pd.isna(row["seconds"]) or row["seconds"] <= 0:
                    continue
                if pd.isna(row["spots"]) or row["spots"] <= 0:
                    continue
                platform, channel, region = parse_platform_region_fn(row["platform"])
                media_platform = get_media_platform_display_fn(platform, channel, row.get("platform", ""))
                if platform not in ["全家", "家樂福"]:
                    continue
                s_date = pd.to_datetime(row["start_date"], errors="coerce")
                e_date = pd.to_datetime(row["end_date"], errors="coerce")
                if pd.isna(s_date) or pd.isna(e_date):
                    continue
                store_count = get_store_count_fn(row["platform"], custom_settings) if should_multiply_store_count_fn(media_platform) else 1
                days = (e_date - s_date).days + 1
                total_spots = row["spots"] * days
                total_store_seconds = row["seconds"] * total_spots * store_count
                segments.append(
                    {
                        "segment_id": str(uuid.uuid4()),
                        "source_order_id": order_id,
                        "platform": platform,
                        "channel": channel,
                        "region": region,
                        "media_platform": media_platform,
                        "company": row.get("company", ""),
                        "sales": row.get("sales", ""),
                        "client": row.get("client", ""),
                        "product": row.get("product", ""),
                        "seconds": int(row["seconds"]),
                        "spots": int(row["spots"]),
                        "start_date": s_date.date(),
                        "end_date": e_date.date(),
                        "duration_days": days,
                        "store_count": store_count,
                        "total_spots": total_spots,
                        "total_store_seconds": total_store_seconds,
                        "seconds_type": normalize_seconds_type_fn(row.get("seconds_type")),
                        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    }
                )
            except Exception:
                continue
    df_segments = pd.DataFrame(segments)
    if write_to_db and not df_segments.empty:
        conn = get_db_connection_fn()
        c = conn.cursor()
        try:
            c.execute("BEGIN TRANSACTION")
            c.execute("DELETE FROM ad_flight_segments")
            c.executemany(
                """
                INSERT INTO ad_flight_segments
                (segment_id, source_order_id, platform, channel, region, media_platform, company, sales,
                 client, product, seconds, spots, start_date, end_date, duration_days,
                 store_count, total_spots, total_store_seconds, seconds_type, created_at, updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
                [
                    (
                        seg["segment_id"],
                        seg["source_order_id"],
                        seg["platform"],
                        seg["channel"],
                        seg["region"],
                        seg.get("media_platform", ""),
                        seg["company"],
                        seg["sales"],
                        seg["client"],
                        seg["product"],
                        seg["seconds"],
                        seg["spots"],
                        seg["start_date"],
                        seg["end_date"],
                        seg["duration_days"],
                        seg["store_count"],
                        seg["total_spots"],
                        seg["total_store_seconds"],
                        seg["seconds_type"],
                        seg["created_at"],
                        seg["updated_at"],
                    )
                    for seg in segments
                ],
            )
            conn.commit()
            conn.close()
            if sync_sheets:
                sync_sheets_if_enabled_fn(only_tables=["Segments"], skip_if_unchanged=True)
        except Exception:
            conn.rollback()
            conn.close()
    return df_segments


def resolve_media_platform_for_daily(seg, get_media_platform_display_fn=None):
    mp = seg.get("media_platform")
    if mp is not None and str(mp).strip() and not (isinstance(mp, float) and pd.isna(mp)):
        return str(mp).strip()
    return get_media_platform_display_fn(
        seg.get("platform") if pd.notna(seg.get("platform")) else "",
        seg.get("channel") if pd.notna(seg.get("channel")) else "",
        "",
    )


def explode_segments_to_daily(df_segments, get_media_platform_display_fn=None, normalize_seconds_type_fn=None):
    daily_records = []
    for _, seg in df_segments.iterrows():
        try:
            s_date = pd.to_datetime(seg["start_date"])
            e_date = pd.to_datetime(seg["end_date"])
            date_range = pd.date_range(s_date, e_date, inclusive="both")
            for d in date_range:
                platform_display = f"{seg['platform']}-{seg['channel']}"
                if seg["region"] != "未知":
                    platform_display = f"{seg['platform']}-{seg['channel']}-{seg['region']}"
                daily_records.append(
                    {
                        "日期": d,
                        "平台": platform_display,
                        "媒體平台": resolve_media_platform_for_daily(seg, get_media_platform_display_fn=get_media_platform_display_fn),
                        "秒數用途": normalize_seconds_type_fn(seg.get("seconds_type")),
                        "公司": seg["company"],
                        "業務": seg["sales"],
                        "客戶": seg["client"],
                        "產品": seg["product"],
                        "使用店秒": seg["seconds"] * seg["spots"] * seg["store_count"],
                        "原始秒數": seg["seconds"] * seg["spots"],
                        "秒數": seg["seconds"],
                        "檔次": seg["spots"],
                        "segment_id": seg["segment_id"],
                        "訂單ID": seg["source_order_id"],
                    }
                )
        except Exception:
            continue
    return pd.DataFrame(daily_records)
