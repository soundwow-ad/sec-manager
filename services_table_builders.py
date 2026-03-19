import pandas as pd
import numpy as np
import calendar
from datetime import datetime, timedelta


def build_table1_from_cue_excel(
    cue_data_list,
    custom_settings=None,
    parse_platform_region_fn=None,
    get_media_platform_display_fn=None,
    get_store_count_fn=None,
    should_multiply_store_count_fn=None,
):
    if not cue_data_list:
        return pd.DataFrame()

    result_rows = []
    for ad_unit in cue_data_list:
        platform_display = ad_unit.get("platform", "未知")
        try:
            p, ch, _ = parse_platform_region_fn(platform_display)
            mp = get_media_platform_display_fn(p, ch, platform_display)
        except Exception:
            mp = "其他"
        store_count = get_store_count_fn(platform_display, custom_settings) if should_multiply_store_count_fn(mp) else 1

        daily_spots = ad_unit.get("daily_spots", [])
        days = ad_unit.get("days", len(daily_spots))
        total_spots = ad_unit.get("total_spots", sum(daily_spots))
        seconds = ad_unit.get("seconds", 0)
        total_seconds = total_spots * seconds
        total_store_seconds = total_seconds * store_count

        base_row = {
            "業務": ad_unit.get("sales", ""),
            "主管": "",
            "合約編號": ad_unit.get("order_id", ""),
            "實收金額": int(ad_unit.get("amount_net", 0) or 0),
            "除佣實收": int(ad_unit.get("amount_net", 0) or 0),
            "製作成本": "",
            "獎金%": "",
            "核定獎金": "",
            "加發獎金": "",
            "業務基金": "",
            "協力基金": "",
            "秒數用途": "銷售秒數",
            "提交日": "",
            "HYUNDAI_CUSTIN": ad_unit.get("client", ""),
            "秒數": seconds,
            "素材": ad_unit.get("product", ""),
            "起始日": ad_unit.get("start_date", ""),
            "終止日": ad_unit.get("end_date", ""),
            "走期天數": days,
            "區域": ad_unit.get("region", "未知"),
            "平台": platform_display,
            "平台分類": ad_unit.get("platform_category", "其他"),
            "媒體平台": get_media_platform_display_fn(*parse_platform_region_fn(platform_display), platform_display),
        }

        for hour in [6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 0, 1]:
            base_row[str(hour)] = ""

        base_row["每天總檔次"] = daily_spots[0] if daily_spots else 0
        base_row["委刊總檔數"] = total_spots
        base_row["總秒數"] = total_seconds
        base_row["店數"] = store_count
        base_row["使用總秒數"] = total_store_seconds
        result_rows.append(base_row)

    df_table1 = pd.DataFrame(result_rows)
    all_dates = set()
    for ad_unit in cue_data_list:
        dates = ad_unit.get("dates", [])
        all_dates.update([pd.to_datetime(d) for d in dates if d])

    if all_dates:
        sorted_dates = sorted(all_dates)
        weekday_map = {0: "一", 1: "二", 2: "三", 3: "四", 4: "五", 5: "六", 6: "日"}
        date_column_names = []
        for d in sorted_dates:
            date_key = f"{d.month}/{d.day}({weekday_map[d.weekday()]})"
            if date_key not in date_column_names:
                date_column_names.append(date_key)
        for date_key in date_column_names:
            df_table1[date_key] = ""
        for idx, ad_unit in enumerate(cue_data_list):
            dates = ad_unit.get("dates", [])
            daily_spots = ad_unit.get("daily_spots", [])
            for date_str, spots in zip(dates, daily_spots):
                try:
                    d = pd.to_datetime(date_str)
                    date_key = f"{d.month}/{d.day}({weekday_map[d.weekday()]})"
                    if date_key in df_table1.columns:
                        df_table1.loc[idx, date_key] = spots
                except Exception:
                    pass
    return df_table1


def segment_platform_display(seg):
    platform = seg.get("platform", "")
    channel = seg.get("channel", "")
    region = seg.get("region", "未知")
    media = seg.get("media_platform", "")
    if platform == "全家":
        if region and region != "未知":
            return (channel or "全家") + region
        if channel == "企頻":
            return "企頻"
        if channel == "新鮮視":
            return "新鮮視"
        return media or (channel or "全家")
    if platform == "家樂福":
        return media or "家樂福"
    return media or f"{platform}-{channel}"


def build_table2_summary_by_company(df_segments, df_daily, df_orders, get_media_platform_display_fn, media_platform=None):
    if df_segments.empty or df_daily.empty:
        return pd.DataFrame()

    def _resolve_mp(r):
        return r.get("media_platform") or get_media_platform_display_fn(r.get("platform"), r.get("channel"), r.get("platform", ""))

    if media_platform:
        df_segments = df_segments[df_segments.apply(_resolve_mp, axis=1) == media_platform].copy()
        if "媒體平台" in df_daily.columns:
            df_daily = df_daily[df_daily["媒體平台"] == media_platform].copy()
        if df_segments.empty or df_daily.empty:
            return pd.DataFrame()
    companies = df_segments["company"].dropna().unique()
    companies = [c for c in companies if c]
    if not companies:
        return pd.DataFrame()
    try:
        df_ord = df_orders[["id", "amount_net", "contract_id"]].drop_duplicates(subset=["id"])
    except Exception:
        df_ord = df_orders[["id", "amount_net"]].copy().drop_duplicates(subset=["id"])
        df_ord["contract_id"] = None
    seg_ord = df_segments[["source_order_id", "company", "total_spots", "total_store_seconds"]].merge(df_ord, left_on="source_order_id", right_on="id", how="left")
    seg_ord["_contract_key"] = seg_ord.get("contract_id").fillna(seg_ord["source_order_id"])
    by_company = seg_ord.groupby("company").agg(total_spots=("total_spots", "sum"), total_store_seconds=("total_store_seconds", "sum")).reset_index()

    def _sum_amt_unique_contract(g):
        return g.drop_duplicates("_contract_key")["amount_net"].sum()

    amt_by_co = seg_ord.groupby("company").apply(_sum_amt_unique_contract).reindex(companies).fillna(0)
    by_company["實收金額"] = by_company["company"].map(amt_by_co).fillna(0).astype(int)
    by_company["除佣實收"] = by_company["實收金額"]
    by_company["委刊總檔數"] = by_company["total_spots"].fillna(0).astype(int)
    by_company["使用總秒數"] = by_company["total_store_seconds"].fillna(0).astype(int)

    if "日期" not in df_daily.columns or "使用店秒" not in df_daily.columns or "公司" not in df_daily.columns:
        date_cols = []
    else:
        daily_agg = df_daily.groupby(["公司", "日期"])["使用店秒"].sum().reset_index()
        daily_agg["日期"] = pd.to_datetime(daily_agg["日期"])
        all_dates = sorted(daily_agg["日期"].dropna().unique())
        weekday_map = {0: "一", 1: "二", 2: "三", 3: "四", 4: "五", 5: "六", 6: "日"}
        date_cols = [f"{d.month}/{d.day}({weekday_map[d.weekday()]})" for d in all_dates]
        pivot_daily = daily_agg.pivot(index="公司", columns="日期", values="使用店秒").reindex(companies).fillna(0)
        for d in all_dates:
            key = f"{d.month}/{d.day}({weekday_map[d.weekday()]})"
            if d in pivot_daily.columns:
                by_company[key] = pivot_daily.loc[by_company["company"], d].fillna(0).astype(int).values
            else:
                by_company[key] = 0
    base_cols = ["公司", "實收金額", "除佣實收", "委刊總檔數", "使用總秒數"]
    out = by_company[["company", "實收金額", "除佣實收", "委刊總檔數", "使用總秒數"]].copy()
    out.columns = base_cols
    for c in date_cols:
        if c in by_company.columns:
            out[c] = by_company[c].fillna(0).astype(int)
    subtotal = {"公司": "小計", "實收金額": out["實收金額"].sum(), "除佣實收": out["除佣實收"].sum(), "委刊總檔數": out["委刊總檔數"].sum(), "使用總秒數": out["使用總秒數"].sum()}
    for c in date_cols:
        subtotal[c] = out[c].sum() if c in out.columns else 0
    out = pd.concat([out, pd.DataFrame([subtotal])], ignore_index=True)
    return out


def build_table2_details_by_company(df_segments, df_daily, df_orders):
    if df_segments.empty:
        return {}
    try:
        df_ord = df_orders[["id", "amount_net", "updated_at", "contract_id"]].drop_duplicates(subset=["id"])
    except Exception:
        df_ord = df_orders[["id", "amount_net", "updated_at"]].drop_duplicates(subset=["id"])
        df_ord["contract_id"] = None
    df_ord["提交日"] = pd.to_datetime(df_ord["updated_at"], errors="coerce").dt.strftime("%Y/%m/%d")
    seg = df_segments.merge(df_ord, left_on="source_order_id", right_on="id", how="left")
    seg["合約編號"] = seg.get("contract_id").fillna(seg["source_order_id"])
    seg["平台顯示"] = seg.apply(segment_platform_display, axis=1)
    result = {}
    daily_pivot = pd.DataFrame()
    if not df_daily.empty and "segment_id" in df_daily.columns and "日期" in df_daily.columns:
        _piv = df_daily.groupby(["segment_id", "日期"])["使用店秒"].sum().unstack(fill_value=0)
        weekday_map = {0: "一", 1: "二", 2: "三", 3: "四", 4: "五", 5: "六", 6: "日"}
        _piv.columns = [f"{c.month}/{c.day}({weekday_map.get(c.weekday(), '')})" if hasattr(c, "month") else str(c) for c in _piv.columns]
        daily_pivot = _piv
    for company in seg["company"].dropna().unique():
        if not company:
            continue
        s = seg[seg["company"] == company].copy()
        s = s.rename(columns={"client": "客戶名稱", "total_spots": "委刊總檔數", "total_store_seconds": "使用總秒數"})
        detail = pd.DataFrame(
            {
                "公司": s["company"].values,
                "平台": s["平台顯示"].values,
                "業務": s["sales"].values,
                "合約編號": s["合約編號"].astype(str).values,
                "實收金額": s["amount_net"].fillna(0).astype(int).values,
                "除佣實收": s["amount_net"].fillna(0).astype(int).values,
                "提交日": s["提交日"].fillna("").values,
                "客戶名稱": s["客戶名稱"].fillna("").values,
                "秒數": s["seconds"].fillna(0).astype(int).values,
                "委刊總檔數": s["委刊總檔數"].fillna(0).astype(int).values,
                "使用總秒數": s["使用總秒數"].fillna(0).astype(int).values,
            }
        )
        if not daily_pivot.empty and "segment_id" in s.columns:
            for col in daily_pivot.columns:
                detail[col] = 0
            for i, seg_id in enumerate(s["segment_id"].values):
                if seg_id in daily_pivot.index:
                    row_vals = daily_pivot.loc[seg_id]
                    for col in daily_pivot.columns:
                        if col in detail.columns:
                            detail.iloc[i, detail.columns.get_loc(col)] = int(row_vals.get(col, 0))
        sub = {
            "公司": company,
            "平台": "",
            "業務": "",
            "合約編號": "小計",
            "實收金額": detail["實收金額"].sum(),
            "除佣實收": detail["除佣實收"].sum(),
            "提交日": "",
            "客戶名稱": "",
            "秒數": "",
            "委刊總檔數": detail["委刊總檔數"].sum(),
            "使用總秒數": detail["使用總秒數"].sum(),
        }
        if not daily_pivot.empty:
            for col in daily_pivot.columns:
                sub[col] = detail[col].sum() if col in detail.columns else 0
        detail = pd.concat([detail, pd.DataFrame([sub])], ignore_index=True)
        result[company] = detail
    return result


def build_table1_from_segments(
    df_segments: pd.DataFrame,
    custom_settings=None,
    df_orders_info=None,
    get_db_connection_fn=None,
    get_media_platform_display_fn=None,
    include_daily_columns: bool = True,
) -> pd.DataFrame:
    if df_segments.empty:
        return pd.DataFrame()

    df = df_segments.copy()
    df["start_date"] = pd.to_datetime(df["start_date"], errors="coerce")
    df["end_date"] = pd.to_datetime(df["end_date"], errors="coerce")
    df["走期天數"] = df["duration_days"]
    df["區域"] = df["region"]
    df["店數"] = df["store_count"]
    df["每天總檔次"] = df["spots"]
    df["委刊總檔數"] = df["total_spots"]
    df["總秒數"] = df["委刊總檔數"] * df["seconds"]
    df["使用總秒數"] = df["total_store_seconds"]

    if df_orders_info is not None and not df_orders_info.empty:
        df = df.merge(df_orders_info, left_on="source_order_id", right_on="id", how="left", suffixes=("", "_order"))
        df["提交日"] = pd.to_datetime(df["updated_at"], errors="coerce").dt.strftime("%Y/%m/%d")
        df["提交日"] = df["提交日"].fillna("")
    else:
        conn = get_db_connection_fn()
        try:
            try:
                df_orders_info = pd.read_sql("SELECT id, updated_at, contract_id FROM orders", conn)
            except Exception:
                df_orders_info = pd.read_sql("SELECT id, updated_at FROM orders", conn)
                df_orders_info["contract_id"] = None
            conn.close()
            df = df.merge(df_orders_info, left_on="source_order_id", right_on="id", how="left", suffixes=("", "_order"))
            df["提交日"] = pd.to_datetime(df["updated_at"], errors="coerce").dt.strftime("%Y/%m/%d")
            df["提交日"] = df["提交日"].fillna("")
        except Exception:
            df["提交日"] = ""
            if "contract_id" not in df.columns:
                df["contract_id"] = None

    date_column_names = []
    weekday_map = {0: "一", 1: "二", 2: "三", 3: "四", 4: "五", 5: "六", 6: "日"}
    if include_daily_columns:
        all_dates = set()
        for _, row in df.iterrows():
            if pd.notna(row["start_date"]) and pd.notna(row["end_date"]):
                all_dates.update(pd.date_range(row["start_date"], row["end_date"], freq="D"))
        if all_dates:
            for d in sorted(all_dates):
                date_key = f"{d.month}/{d.day}({weekday_map[d.weekday()]})"
                if date_key not in date_column_names:
                    date_column_names.append(date_key)

    result_rows = []
    for idx, row in df.iterrows():
        _contract_id = row.get("contract_id")
        _display_contract = (_contract_id if (pd.notna(_contract_id) and _contract_id) else row.get("source_order_id", ""))
        base_row = {
            "_source_order_id": row.get("source_order_id"),
            "業務": row.get("sales", ""),
            "主管": "",
            "合約編號": _display_contract,
            "公司": row.get("company", ""),
            "實收金額": 0,
            "除佣實收": 0,
            "製作成本": "",
            "獎金%": "",
            "核定獎金": "",
            "加發獎金": "",
            "業務基金": "",
            "協力基金": "",
            "秒數用途": ("銷售秒數" if (row.get("seconds_type") or "") == "銷售" else (row.get("seconds_type") or "銷售秒數")),
            "提交日": df.loc[idx, "提交日"],
            "HYUNDAI_CUSTIN": row.get("client", ""),
            "秒數": int(row.get("seconds", 0) or 0),
            "素材": row.get("product", ""),
            "起始日": row["start_date"].strftime("%Y/%m/%d") if pd.notna(row["start_date"]) else "",
            "終止日": row["end_date"].strftime("%Y/%m/%d") if pd.notna(row["end_date"]) else "",
            "走期天數": int(df.loc[idx, "走期天數"]),
            "區域": df.loc[idx, "區域"],
            "媒體平台": row.get("media_platform") or get_media_platform_display_fn(row.get("platform"), row.get("channel"), ""),
        }
        for hour in [6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 0, 1]:
            base_row[str(hour)] = ""
        base_row["每天總檔次"] = int(df.loc[idx, "每天總檔次"])
        base_row["委刊總檔數"] = int(df.loc[idx, "委刊總檔數"])
        base_row["總秒數"] = int(df.loc[idx, "總秒數"])
        base_row["店數"] = int(df.loc[idx, "店數"])
        base_row["使用總秒數"] = int(df.loc[idx, "使用總秒數"])
        if include_daily_columns:
            for date_key in date_column_names:
                base_row[date_key] = ""
            if pd.notna(row["start_date"]) and pd.notna(row["end_date"]):
                date_range = pd.date_range(row["start_date"], row["end_date"], freq="D")
                daily_spots = df.loc[idx, "每天總檔次"]
                for d in date_range:
                    date_key = f"{d.month}/{d.day}({weekday_map[d.weekday()]})"
                    if date_key in date_column_names:
                        base_row[date_key] = daily_spots
        result_rows.append(base_row)

    df_excel = pd.DataFrame(result_rows)
    conn = get_db_connection_fn()
    try:
        df_orders_amount = pd.read_sql("SELECT id, amount_net, project_amount_net, split_amount FROM orders", conn)
        conn.close()
        df_excel = df_excel.merge(df_orders_amount, left_on="_source_order_id", right_on="id", how="left", suffixes=("", "_order"))
        df_excel["實收金額"] = df_excel["amount_net"].fillna(0).astype(int)
        df_excel["除佣實收"] = df_excel["amount_net"].fillna(0).astype(int)
        df_excel["專案實收金額"] = pd.to_numeric(df_excel["project_amount_net"], errors="coerce").fillna(0)
        df_excel["拆分金額"] = pd.to_numeric(df_excel["split_amount"], errors="coerce").fillna(0)
        df_excel = df_excel.drop(columns=["id", "amount_net", "project_amount_net", "split_amount", "_source_order_id"], errors="ignore")
    except Exception:
        try:
            conn = get_db_connection_fn()
            df_orders_amount = pd.read_sql("SELECT id, amount_net FROM orders", conn)
            conn.close()
            df_excel = df_excel.merge(df_orders_amount, left_on="_source_order_id", right_on="id", how="left")
            df_excel["實收金額"] = df_excel["amount_net"].fillna(0).astype(int)
            df_excel["除佣實收"] = df_excel["amount_net"].fillna(0).astype(int)
            df_excel = df_excel.drop(columns=["id", "amount_net", "_source_order_id"], errors="ignore")
        except Exception:
            df_excel = df_excel.drop(columns=["_source_order_id"], errors="ignore")
        if "專案實收金額" not in df_excel.columns:
            df_excel["專案實收金額"] = 0
        if "拆分金額" not in df_excel.columns:
            df_excel["拆分金額"] = 0

    base_columns = ["業務", "主管", "合約編號", "公司", "實收金額", "除佣實收", "專案實收金額", "拆分金額", "製作成本", "獎金%", "核定獎金", "加發獎金", "業務基金", "協力基金", "秒數用途", "提交日", "HYUNDAI_CUSTIN", "秒數", "素材", "起始日", "終止日", "走期天數", "區域", "媒體平台"]
    hour_columns = [str(h) for h in [6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 0, 1]]
    stat_columns = ["每天總檔次", "委刊總檔數", "總秒數", "店數", "使用總秒數"]
    all_columns = base_columns + hour_columns + stat_columns + date_column_names
    existing_columns = [col for col in all_columns if col in df_excel.columns]
    other_columns = [col for col in df_excel.columns if col not in existing_columns]
    df_excel = df_excel[existing_columns + other_columns]
    sort_cols = [c for c in ["業務", "合約編號", "起始日"] if c in df_excel.columns]
    if sort_cols:
        df_excel = df_excel.sort_values(by=sort_cols, ascending=[True] * len(sort_cols), na_position="last")
    return df_excel.reset_index(drop=True)


def build_excel_table1_view(
    df_orders: pd.DataFrame,
    custom_settings=None,
    use_segments=True,
    df_segments=None,
    build_table1_from_segments_fn=None,
    get_db_connection_fn=None,
    parse_platform_region_fn=None,
    get_media_platform_display_fn=None,
    get_store_count_fn=None,
    include_daily_columns: bool = True,
) -> pd.DataFrame:
    if use_segments:
        if df_segments is not None and not df_segments.empty:
            cols = ["id", "updated_at", "contract_id"] if "contract_id" in df_orders.columns else ["id", "updated_at"]
            info = df_orders[cols].copy() if all(c in df_orders.columns for c in cols) else df_orders[["id", "updated_at"]].copy()
            if "contract_id" not in info.columns:
                info["contract_id"] = None
            return build_table1_from_segments_fn(
                df_segments,
                custom_settings,
                df_orders_info=info,
                include_daily_columns=include_daily_columns,
            )
        conn = get_db_connection_fn()
        try:
            df_seg = pd.read_sql("SELECT * FROM ad_flight_segments", conn)
            conn.close()
            if not df_seg.empty:
                return build_table1_from_segments_fn(
                    df_seg,
                    custom_settings,
                    include_daily_columns=include_daily_columns,
                )
        except Exception:
            conn.close()

    if df_orders.empty:
        return pd.DataFrame()
    df = df_orders.copy()
    df["start_date"] = pd.to_datetime(df["start_date"], errors="coerce")
    df["end_date"] = pd.to_datetime(df["end_date"], errors="coerce")
    df["走期天數"] = (df["end_date"] - df["start_date"]).dt.days + 1
    df["走期天數"] = df["走期天數"].fillna(0).astype(int)

    def extract_region(p):
        if pd.isna(p):
            return ""
        p_str = str(p)
        for r in ["全省", "北北基", "桃竹苗", "中彰投", "高高屏", "雲嘉南", "宜花東"]:
            if r in p_str:
                return r
        return ""

    df["區域"] = df["platform"].apply(extract_region)

    def _media_platform(r):
        platform, channel, _ = parse_platform_region_fn(r["platform"])
        return get_media_platform_display_fn(platform, channel, r.get("platform", ""))

    df["媒體平台"] = df.apply(_media_platform, axis=1)
    df["店數"] = df["platform"].apply(lambda p: get_store_count_fn(p, custom_settings))
    df["每天總檔次"] = df["spots"].fillna(0).astype(int)
    df["委刊總檔數"] = df["每天總檔次"] * df["走期天數"]
    df["總秒數"] = df["委刊總檔數"] * df["seconds"].fillna(0).astype(int)
    df["使用總秒數"] = df["總秒數"] * df["店數"]
    df["提交日"] = pd.to_datetime(df["updated_at"], errors="coerce").dt.strftime("%Y/%m/%d")
    df["提交日"] = df["提交日"].fillna("")

    date_column_names = []
    weekday_map = {0: "一", 1: "二", 2: "三", 3: "四", 4: "五", 5: "六", 6: "日"}
    if include_daily_columns:
        all_dates = set()
        for _, row in df.iterrows():
            if pd.notna(row["start_date"]) and pd.notna(row["end_date"]):
                all_dates.update(pd.date_range(row["start_date"], row["end_date"], freq="D"))
        if all_dates:
            for d in sorted(all_dates):
                date_key = f"{d.month}/{d.day}({weekday_map[d.weekday()]})"
                if date_key not in date_column_names:
                    date_column_names.append(date_key)

    result_rows = []
    for idx, row in df.iterrows():
        _cid = row.get("contract_id")
        _display_contract = (_cid if (pd.notna(_cid) and _cid) else row.get("id", ""))
        base_row = {
            "業務": row.get("sales", ""),
            "主管": "",
            "合約編號": _display_contract,
            "公司": row.get("company", ""),
            "實收金額": int(row.get("amount_net", 0) or 0),
            "除佣實收": int(row.get("amount_net", 0) or 0),
            "製作成本": "",
            "獎金%": "",
            "核定獎金": "",
            "加發獎金": "",
            "業務基金": "",
            "協力基金": "",
            "秒數用途": row.get("seconds_type") or "銷售秒數",
            "提交日": df.loc[idx, "提交日"],
            "HYUNDAI_CUSTIN": row.get("client", ""),
            "秒數": int(row.get("seconds", 0) or 0),
            "素材": row.get("product", ""),
            "起始日": row["start_date"].strftime("%Y/%m/%d") if pd.notna(row["start_date"]) else "",
            "終止日": row["end_date"].strftime("%Y/%m/%d") if pd.notna(row["end_date"]) else "",
            "走期天數": df.loc[idx, "走期天數"],
            "區域": df.loc[idx, "區域"],
            "媒體平台": df.loc[idx, "媒體平台"] if "媒體平台" in df.columns else "",
        }
        for hour in [6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 0, 1]:
            base_row[str(hour)] = ""
        base_row["每天總檔次"] = df.loc[idx, "每天總檔次"]
        base_row["委刊總檔數"] = df.loc[idx, "委刊總檔數"]
        base_row["總秒數"] = df.loc[idx, "總秒數"]
        base_row["店數"] = df.loc[idx, "店數"]
        base_row["使用總秒數"] = df.loc[idx, "使用總秒數"]
        if include_daily_columns:
            for date_key in date_column_names:
                base_row[date_key] = ""
            if pd.notna(row["start_date"]) and pd.notna(row["end_date"]):
                date_range = pd.date_range(row["start_date"], row["end_date"], freq="D")
                daily_spots = df.loc[idx, "每天總檔次"]
                for d in date_range:
                    date_key = f"{d.month}/{d.day}({weekday_map[d.weekday()]})"
                    if date_key in date_column_names:
                        base_row[date_key] = daily_spots
        result_rows.append(base_row)

    df_excel = pd.DataFrame(result_rows)
    base_columns = ["業務", "主管", "合約編號", "公司", "實收金額", "除佣實收", "製作成本", "獎金%", "核定獎金", "加發獎金", "業務基金", "協力基金", "秒數用途", "提交日", "HYUNDAI_CUSTIN", "秒數", "素材", "起始日", "終止日", "走期天數", "區域", "媒體平台"]
    hour_columns = [str(h) for h in [6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 0, 1]]
    stat_columns = ["每天總檔次", "委刊總檔數", "總秒數", "店數", "使用總秒數"]
    all_columns = base_columns + hour_columns + stat_columns + date_column_names
    existing_columns = [col for col in all_columns if col in df_excel.columns]
    other_columns = [col for col in df_excel.columns if col not in existing_columns]
    df_excel = df_excel[existing_columns + other_columns]
    sort_cols = [c for c in ["業務", "合約編號", "起始日"] if c in df_excel.columns]
    if sort_cols:
        df_excel = df_excel.sort_values(by=sort_cols, ascending=[True] * len(sort_cols), na_position="last")
    return df_excel.reset_index(drop=True)


def build_table3_monthly_control(
    df_daily,
    df_segments,
    media_platform_options,
    get_media_platform_display_fn,
    year=None,
    month=None,
    monthly_capacity=None,
):
    if df_daily.empty or df_segments.empty:
        return {}
    if "媒體平台" not in df_daily.columns:
        return {}
    daily_hours_default = 18
    monthly_capacity = monthly_capacity or {}
    df_daily = df_daily.copy()
    df_daily["日期"] = pd.to_datetime(df_daily["日期"], errors="coerce")
    df_segments = df_segments.copy()
    df_segments["start_date"] = pd.to_datetime(df_segments["start_date"], errors="coerce")
    df_segments["end_date"] = pd.to_datetime(df_segments["end_date"], errors="coerce")
    all_dates = sorted(df_daily["日期"].dropna().unique())
    if year is not None and month is not None:
        y, m = int(year), int(month)
        all_dates = [d for d in all_dates if d.year == y and d.month == m]
        if not all_dates:
            ndays = calendar.monthrange(y, m)[1]
            all_dates = [pd.Timestamp(year=y, month=m, day=day) for day in range(1, ndays + 1)]
    if not all_dates:
        return {}
    media_platforms = [p for p in media_platform_options if p in df_daily["媒體平台"].unique()]
    if not media_platforms:
        media_platforms = df_daily["媒體平台"].dropna().unique().tolist()
    result = {}

    def _resolve_media_platform(r):
        return r.get("media_platform") or get_media_platform_display_fn(r.get("platform"), r.get("channel"), r.get("platform", ""))

    for mp in media_platforms:
        dd = df_daily[df_daily["媒體平台"] == mp]
        seg_mp = df_segments[df_segments.apply(lambda r: _resolve_media_platform(r) == mp, axis=1)].copy()
        used_by_date = dd.groupby("日期")["使用店秒"].sum().reindex(all_dates).fillna(0)
        set_cap = monthly_capacity.get(mp)
        if set_cap is not None and set_cap > 0:
            cap_series = pd.Series([int(set_cap)] * len(all_dates), index=all_dates)
        else:
            starts = np.array(seg_mp["start_date"].values, dtype=np.datetime64)
            ends = np.array(seg_mp["end_date"].values, dtype=np.datetime64)
            scs = (seg_mp["store_count"].fillna(0).astype(int) * daily_hours_default * 3600).values
            cap_list = []
            for d in all_dates:
                d64 = np.datetime64(d)
                mask = (starts <= d64) & (d64 <= ends)
                cap_list.append(np.sum(scs[mask]))
            cap_series = pd.Series(cap_list, index=all_dates)
        used_by_date = used_by_date.reindex(all_dates).fillna(0)
        util_series = (used_by_date / cap_series.replace(0, 1)).fillna(0) * 100
        weekday_cn = ["一", "二", "三", "四", "五", "六", "日"]
        date_cols = [f"{d.month}/{d.day}({weekday_cn[d.weekday()]})" for d in all_dates]
        total_used = used_by_date.sum()
        total_cap = cap_series.sum()
        pct_used = round(total_used / (total_cap or 1) * 100, 1)
        pct_unused = round((total_cap - total_used) / (total_cap or 1) * 100, 1)
        row_used = {"授權": "總經理", "項目": "執行秒", "秒數": int(total_used), "%": f"{pct_used:.1f}"}
        row_cap = {"授權": "總經理", "項目": "可用秒數", "秒數": int(total_cap), "%": f"{pct_unused:.1f}"}
        row_util = {"授權": "總經理", "項目": "使用率", "秒數": "", "%": "100.0"}
        row_color = {"授權": "業務", "項目": "可排日", "秒數": "", "%": ""}
        for i in range(len(all_dates)):
            row_used[date_cols[i]] = int(used_by_date.iloc[i]) if i < len(used_by_date) else 0
            row_cap[date_cols[i]] = int(cap_series.iloc[i]) if i < len(cap_series) else 0
            u = util_series.iloc[i] if i < len(util_series) else 0
            row_util[date_cols[i]] = f"{round(float(u), 1)}%" if pd.notna(u) else "0%"
            row_color[date_cols[i]] = float(u) if pd.notna(u) else 0
        result[mp] = pd.DataFrame([row_used, row_cap, row_util, row_color])
    return result


def build_daily_inventory_and_metrics(
    df_daily,
    year,
    month,
    today,
    emergency_days,
    monthly_capacity_loader,
    media_platform_options,
    time_weight,
    target_usage,
    tolerance,
    safe_limit,
    over_buffer,
    media_platform=None,
):
    y, m = int(year), int(month)
    ndays = calendar.monthrange(y, m)[1]
    month_dates = [datetime(y, m, d).date() for d in range(1, ndays + 1)]
    daily_cap_total = 0
    platforms_to_sum = [media_platform] if media_platform else media_platform_options
    for mp in platforms_to_sum:
        cap = monthly_capacity_loader(mp, y, m)
        if cap is not None and cap > 0:
            daily_cap_total += int(cap)
    df = df_daily.copy()
    if media_platform and "媒體平台" in df.columns:
        df = df[df["媒體平台"] == media_platform]
    if df.empty or "日期" not in df.columns or "使用店秒" not in df.columns:
        used_by_date = {d: 0 for d in month_dates}
    else:
        df["日期"] = pd.to_datetime(df["日期"], errors="coerce").dt.date
        used_by_date = df.groupby("日期")["使用店秒"].sum().to_dict()
    last_day = datetime(y, m, ndays).date()
    rows = []
    for d in month_dates:
        cap = daily_cap_total
        used = int(used_by_date.get(d, 0))
        unused = max(0, cap - used) if cap else 0
        usage_rate = (used / cap) if cap else 0
        days_to_end = (last_day - d).days
        if d < today:
            bucket = "past"
        elif d <= today + timedelta(days=emergency_days) and d >= today:
            bucket = "emergency"
        else:
            bucket = "buffer"
        rows.append({"date": d, "total_capacity_seconds": cap, "used_seconds": used, "unused_seconds": unused, "usage_rate": usage_rate, "days_to_month_end": days_to_end, "time_bucket": bucket})
    daily_inventory = pd.DataFrame(rows)
    past_wasted = int(daily_inventory[daily_inventory["time_bucket"] == "past"]["unused_seconds"].sum())
    emergency_df = daily_inventory[daily_inventory["time_bucket"] == "emergency"]
    emergency_unused = int(emergency_df["unused_seconds"].sum())
    twwi = sum(row["unused_seconds"] * time_weight.get(row["time_bucket"], 0.3) for _, row in daily_inventory.iterrows())
    remaining_days = max(0, len(emergency_df))
    required_daily_seconds = (emergency_unused / remaining_days) if remaining_days else 0
    month_usage_rate = daily_inventory["used_seconds"].sum() / (daily_inventory["total_capacity_seconds"].sum() or 1)
    under_risk = max(0, target_usage - month_usage_rate) / tolerance if tolerance else 0
    over_risk = max(0, month_usage_rate - safe_limit) / over_buffer if over_buffer else 0
    under_high = under_risk >= 0.5
    over_high = over_risk >= 0.5
    if under_high and over_high:
        strategy_state = "ANOMALY"
    elif under_high and not over_high:
        strategy_state = "SELL"
    elif not under_high and over_high:
        strategy_state = "HOLD"
    else:
        strategy_state = "NORMAL"
    metrics = {
        "past_wasted_seconds": past_wasted,
        "emergency_unused_seconds": emergency_unused,
        "twwi": twwi,
        "remaining_days": remaining_days,
        "required_daily_seconds": required_daily_seconds,
        "under_risk": under_risk,
        "over_risk": over_risk,
        "strategy_state": strategy_state,
        "month_usage_rate": month_usage_rate,
        "month_total_capacity": daily_inventory["total_capacity_seconds"].sum(),
        "month_total_used": daily_inventory["used_seconds"].sum(),
        "emergency_dates": emergency_df["date"].tolist(),
        "past_dates": daily_inventory[daily_inventory["time_bucket"] == "past"]["date"].tolist(),
        "buffer_dates": daily_inventory[daily_inventory["time_bucket"] == "buffer"]["date"].tolist(),
    }
    return daily_inventory, metrics
