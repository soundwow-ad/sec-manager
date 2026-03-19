import hashlib
import io
import re
from collections import Counter
from datetime import date, datetime

import pandas as pd


# ================= Cueapp Excel 專用解析（東吳／聲活／鉑霖三種格式）=================
def _parse_cueapp_period_dongwu(row_b5_value):
    """從東吳格式 B5 儲存格解析 Period : YYYY. MM. DD - YYYY. MM. DD"""
    if pd.isna(row_b5_value):
        return None, None
    s = str(row_b5_value).strip()
    if hasattr(row_b5_value, "date"):
        return row_b5_value.date(), row_b5_value.date()
    m = re.search(r"(\d{4})\s*[.\-/]\s*(\d{1,2})\s*[.\-/]\s*(\d{1,2})\s*[-~－]\s*(\d{4})\s*[.\-/]\s*(\d{1,2})\s*[.\-/]\s*(\d{1,2})", s)
    if m:
        try:
            start = datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
            end = datetime(int(m.group(4)), int(m.group(5)), int(m.group(6)))
            return start.date(), end.date()
        except (ValueError, TypeError):
            pass
    return None, None


def _parse_cueapp_period_shenghuo_bolin(df, search_rows=(3, 4, 5)):
    """從聲活/鉑霖格式中找「執行期間：YYYY.MM.DD - YYYY.MM.DD」"""
    for ri in search_rows:
        if ri >= len(df):
            continue
        row_text = df.iloc[ri].fillna("").astype(str).str.cat(sep=" ")
        m = re.search(r"執行期間[：:]\s*(\d{4})[.\-/](\d{1,2})[.\-/](\d{1,2})\s*[-~－]\s*(\d{4})[.\-/](\d{1,2})[.\-/](\d{1,2})", row_text)
        if m:
            try:
                start = datetime(int(m.group(1)), int(m.group(2)), int(m.group(3))).date()
                end = datetime(int(m.group(4)), int(m.group(5)), int(m.group(6))).date()
                return start, end
            except (ValueError, TypeError):
                pass
    return None, None


def _cell_val(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    if hasattr(v, "date"):
        return v.date() if hasattr(v, "date") else v
    return v


def _safe_spots(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return 0
    try:
        n = int(round(float(val)))
        return n if 0 <= n <= 10000 else 0
    except (ValueError, TypeError):
        return 0


def _extract_seconds_from_cell(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return 0
    s = str(val).strip()
    m = re.search(r"(\d+)\s*秒", s)
    if m:
        try:
            sec = int(m.group(1))
            if 5 <= sec <= 120:
                return sec
        except ValueError:
            pass
    return 0


def parse_cueapp_excel(file_content):
    result = []
    try:
        excel_file = io.BytesIO(file_content)
        xls = pd.ExcelFile(excel_file, engine="openpyxl")
    except Exception:
        return []

    for sheet_name in xls.sheet_names:
        try:
            excel_file.seek(0)
            df = pd.read_excel(excel_file, sheet_name=sheet_name, header=None, engine="openpyxl")
            if df.empty or len(df) < 9:
                continue
            row0_text = df.iloc[0].fillna("").astype(str).str.cat(sep=" ")
            fmt = None
            if "Media Schedule" in row0_text or (len(df.columns) > 0 and str(df.iloc[0, 0]).strip() == "Media Schedule"):
                fmt = "dongwu"
            elif "聲活數位" in row0_text:
                fmt = "shenghuo"
            elif "鉑霖行動行銷" in row0_text or "鉑霖" in row0_text:
                fmt = "bolin"

            if fmt is None:
                if re.match(r"^\d+月$", str(sheet_name).strip()):
                    b5 = df.iloc[4, 1] if df.shape[1] > 1 else None
                    start, end = _parse_cueapp_period_dongwu(b5)
                    if start and end:
                        fmt = "dongwu"
                if fmt is None:
                    continue

            start_date, end_date = None, None
            date_start_col = None
            eff_days = None
            header_row_idx = None

            def _find_schedule_header_row(_df: pd.DataFrame):
                def _row_text(i: int) -> str:
                    try:
                        return _df.iloc[i].fillna("").astype(str).str.cat(sep=" ")
                    except Exception:
                        return ""

                for i in range(min(40, len(_df))):
                    t = _row_text(i)
                    if ("頻道" in t and "播出地區" in t and "秒數" in t) or ("Station" in t and "Location" in t and ("Size" in t or "秒數" in t)):
                        return i
                return None

            def _parse_day_cell(v):
                v = _cell_val(v)
                if isinstance(v, (datetime, date)):
                    try:
                        return int(v.day)
                    except Exception:
                        return None
                try:
                    import numbers as _numbers

                    is_num = isinstance(v, (_numbers.Integral, _numbers.Real))
                except Exception:
                    is_num = isinstance(v, (int, float))
                if is_num and not pd.isna(v):
                    try:
                        n = int(round(float(v)))
                        return n if 1 <= n <= 31 else None
                    except Exception:
                        return None
                if isinstance(v, str):
                    s = v.strip()
                    if s.isdigit():
                        n = int(s)
                        return n if 1 <= n <= 31 else None
                return None

            def _infer_year_from_df(_df: pd.DataFrame):
                try:
                    for i in range(min(25, len(_df))):
                        for j in range(min(15, _df.shape[1])):
                            s = str(_df.iloc[i, j]) if _df.iloc[i, j] is not None else ""
                            m = re.search(r"(20\d{2})", s)
                            if m:
                                y = int(m.group(1))
                                if 2000 <= y <= 2100:
                                    return y
                except Exception:
                    pass
                return None

            def _infer_month_for_col(_df: pd.DataFrame, header_i: int, col_j: int):
                for i in range(max(0, header_i - 6), header_i):
                    try:
                        s = str(_df.iloc[i, col_j]).strip()
                        m = re.search(r"(\d{1,2})\s*月", s)
                        if m:
                            mm = int(m.group(1))
                            if 1 <= mm <= 12:
                                return mm
                    except Exception:
                        continue
                for j in range(col_j, -1, -1):
                    try:
                        s = str(_df.iloc[header_i - 1, j]).strip()
                        m = re.search(r"(\d{1,2})\s*月", s)
                        if m:
                            mm = int(m.group(1))
                            if 1 <= mm <= 12:
                                return mm
                    except Exception:
                        continue
                return None

            if fmt == "dongwu":
                b5 = df.iloc[4, 1] if df.shape[1] > 1 else None
                start_date, end_date = _parse_cueapp_period_dongwu(b5)
                if start_date and end_date:
                    date_start_col = 7
                    header_row_idx = 6
                    for c in range(df.shape[1] - 1, date_start_col - 1, -1):
                        try:
                            val = str(df.iloc[header_row_idx, c]).strip() + str(df.iloc[header_row_idx + 1, c]).strip()
                            if "檔次" in val:
                                eff_days = c - date_start_col
                                break
                        except IndexError:
                            continue
                    if eff_days is None:
                        eff_days = max(0, df.shape[1] - date_start_col - 1)
                else:
                    header_row_idx = _find_schedule_header_row(df)
                    if header_row_idx is None:
                        continue
                    sec_col = None
                    for j in range(min(25, df.shape[1])):
                        s = str(df.iloc[header_row_idx, j]).strip()
                        if ("秒數" in s) or (s.lower() == "size") or ("size" in s.lower()):
                            sec_col = j
                            break
                    if sec_col is None:
                        continue
                    date_start_col = sec_col + 1
                    day_cols = []
                    for j in range(date_start_col, min(df.shape[1], date_start_col + 80)):
                        d = _parse_day_cell(df.iloc[header_row_idx, j])
                        if d is None:
                            if day_cols:
                                break
                            continue
                        day_cols.append((j, d))
                    if not day_cols:
                        continue
                    eff_days = len(day_cols)
                    year = _infer_year_from_df(df) or datetime.now().year
                    months = []
                    last_day = None
                    last_month = None
                    for j, d in day_cols:
                        mm = _infer_month_for_col(df, header_row_idx, j)
                        if mm is None:
                            mm = last_month if last_month is not None else 1
                        if last_day is not None and d < last_day and (mm == last_month):
                            mm = 1 if last_month == 12 else (last_month + 1)
                        months.append(mm)
                        last_day = d
                        last_month = mm
                    dates2 = []
                    for (_, d), mm in zip(day_cols, months):
                        try:
                            dates2.append(date(int(year), int(mm), int(d)))
                        except Exception:
                            pass
                    if not dates2:
                        continue
                    start_date = min(dates2)
                    end_date = max(dates2)
            else:
                start_date, end_date = _parse_cueapp_period_shenghuo_bolin(df)
                header_row_idx = _find_schedule_header_row(df)
                if header_row_idx is None:
                    continue
                sec_col = None
                for j in range(min(25, df.shape[1])):
                    s = str(df.iloc[header_row_idx, j]).strip()
                    if ("秒數" in s) or (s.lower() == "size") or ("size" in s.lower()):
                        sec_col = j
                        break
                if sec_col is None:
                    continue
                date_start_col = sec_col + 1
                day_cols = []
                for j in range(date_start_col, min(df.shape[1], date_start_col + 80)):
                    d = _parse_day_cell(df.iloc[header_row_idx, j])
                    if d is None:
                        if day_cols:
                            break
                        continue
                    day_cols.append((j, d))
                if not day_cols:
                    continue
                eff_days = len(day_cols)

                year = _infer_year_from_df(df) or (start_date.year if start_date else None)
                if year is None:
                    year = datetime.now().year
                months = []
                last_day = None
                last_month = None
                base_month = start_date.month if start_date else None
                for j, d in day_cols:
                    mm = _infer_month_for_col(df, header_row_idx, j) or base_month
                    if mm is None:
                        if last_month is None:
                            mm = 1
                        else:
                            mm = last_month
                    if last_day is not None and d < last_day and (mm == last_month):
                        mm = 1 if last_month == 12 else (last_month + 1)
                    months.append(mm)
                    last_day = d
                    last_month = mm

                dates = []
                for (_, d), mm in zip(day_cols, months):
                    try:
                        dates.append(date(year, int(mm), int(d)))
                    except Exception:
                        dates.append(None)
                dates = [dt for dt in dates if dt is not None]
                if not dates:
                    continue
                start_date = start_date or min(dates)
                end_date = end_date or max(dates)

            if eff_days is None or eff_days <= 0:
                continue
            dates_str = None
            if fmt != "dongwu" and header_row_idx is not None and date_start_col is not None:
                try:
                    day_cols2 = []
                    for j in range(date_start_col, min(df.shape[1], date_start_col + 80)):
                        d = _parse_day_cell(df.iloc[header_row_idx, j])
                        if d is None:
                            if day_cols2:
                                break
                            continue
                        day_cols2.append((j, d))
                    if day_cols2:
                        year2 = _infer_year_from_df(df) or (start_date.year if start_date else datetime.now().year)
                        months2 = []
                        last_day2 = None
                        last_month2 = start_date.month if start_date else None
                        for j, d in day_cols2:
                            mm = _infer_month_for_col(df, header_row_idx, j) or last_month2
                            if mm is None:
                                mm = 1
                            if last_day2 is not None and d < last_day2 and (mm == last_month2):
                                mm = 1 if last_month2 == 12 else (last_month2 + 1)
                            months2.append(mm)
                            last_day2 = d
                            last_month2 = mm
                        dates2 = []
                        for (_, d), mm in zip(day_cols2, months2):
                            try:
                                dates2.append(date(int(year2), int(mm), int(d)))
                            except Exception:
                                pass
                        if dates2:
                            dates_str = [dt.strftime("%Y-%m-%d") for dt in dates2]
                            eff_days = len(dates_str)
                except Exception:
                    dates_str = None
            if not dates_str:
                date_list = pd.date_range(start_date, end_date, freq="D")
                if len(date_list) != eff_days:
                    date_list = date_list[:eff_days]
                dates_str = [d.strftime("%Y-%m-%d") for d in date_list]

            data_start_row = header_row_idx + 1
            platform_info = _extract_platform_from_sheet(df, sheet_name)
            seconds_info = _extract_seconds_from_sheet(df, sheet_name)
            default_seconds = seconds_info.get("seconds", 0)

            for r in range(data_start_row, min(data_start_row + 200, len(df))):
                row = df.iloc[r]
                try:
                    try:
                        if date_start_col is not None and date_start_col < len(row):
                            day_marker = str(row.iloc[date_start_col]).strip()
                            if day_marker in ("一", "二", "三", "四", "五", "六", "日"):
                                continue
                    except Exception:
                        pass
                    e_val = row.iloc[4] if len(row) > 4 else None
                    e_str = str(e_val).strip() if e_val is not None else ""
                    if "Total" in e_str or "total" in e_str or e_str == "Total":
                        break
                    first_cell = str(row.iloc[0]).strip() if len(row) > 0 else ""
                    if not first_cell or first_cell == "nan":
                        continue
                    region_cell = row.iloc[1] if len(row) > 1 else ""
                    region = str(region_cell).strip() if region_cell is not None and str(region_cell) != "nan" else platform_info.get("region", "全省")
                    sec_cell = None
                    try:
                        if fmt != "dongwu" and date_start_col is not None and date_start_col >= 1:
                            sec_cell = row.iloc[date_start_col - 1]
                        else:
                            sec_cell = row.iloc[4] if len(row) > 4 else None
                    except Exception:
                        sec_cell = row.iloc[4] if len(row) > 4 else None
                    sec = _extract_seconds_from_cell(sec_cell)
                    if sec <= 0:
                        sec = default_seconds
                    daily_spots = []
                    for c in range(date_start_col, date_start_col + min(eff_days, len(dates_str))):
                        if c < len(row):
                            daily_spots.append(_safe_spots(row.iloc[c]))
                        else:
                            daily_spots.append(0)
                    if len(daily_spots) < len(dates_str):
                        daily_spots.extend([0] * (len(dates_str) - len(daily_spots)))
                    daily_spots = daily_spots[: len(dates_str)]
                    if len([s for s in daily_spots if s > 0]) < 1:
                        continue
                    split_groups = _split_by_spots_change(daily_spots, dates_str, dates_str[0] if dates_str else None, dates_str[-1] if dates_str else None)
                    for group in split_groups:
                        ad_unit = {
                            "platform": platform_info.get("platform", "未知"),
                            "platform_category": platform_info.get("category", "其他"),
                            "seconds": sec,
                            "region": region,
                            "ad_name": first_cell,
                            "daily_spots": group.get("daily_spots_list", [group["daily_spots"]] * group["days"]),
                            "dates": group.get("dates", []),
                            "start_date": group.get("start_date", ""),
                            "end_date": group.get("end_date", ""),
                            "total_spots": sum(group.get("daily_spots_list", [])),
                            "days": group.get("days", 0),
                            "source_sheet": sheet_name,
                            "source_row": r,
                            "split_reason": group.get("split_reason", "none"),
                            "split_groups": [group],
                        }
                        if ad_unit["total_spots"] == 0:
                            ad_unit["total_spots"] = sum(ad_unit["daily_spots"])
                        result.append(ad_unit)
                except (IndexError, KeyError, ValueError, TypeError):
                    continue
        except Exception:
            continue

    try:
        excel_file.close()
    except Exception:
        pass
    return result


SECONDS_BLACKLIST = {5, 10, 15, 20, 30, 40, 60}
YEAR_BLACKLIST = {114, 115, 116, 2025, 2026}


def safe_int_v29(v, target=None):
    try:
        f = float(v)
        if abs(f - round(f)) > 1e-3:
            return None
        f = int(round(f))

        if target and f != target:
            if f in SECONDS_BLACKLIST:
                return None
            if f in YEAR_BLACKLIST:
                return None

        if 0 < f <= 50000:
            return f
    except Exception:
        return None
    return None


def is_noise_row_v29(text):
    noise = ["元", "$", "含稅", "未稅", "VAT", "COST", "PRICE", "報價", "金額", "製作費", "費用", "日期", "結案", "發票"]
    return any(x in text for x in noise)


def is_store_count_row_v29(text, nums):
    keywords = ["門市", "店數", "間門市", "約", "覆蓋", "店家", "家數"]
    if any(k in text for k in keywords):
        if len(nums) <= 2 and max(nums) > 100:
            return True
    return False


def semantic_bonus_v29(text):
    bonus = 0
    if any(x in text for x in ["全家", "家樂福", "區域", "北", "中", "南", "通路", "RADIO", "VISION", "廣播", "店舖"]):
        bonus += 3
    if any(x in text for x in ["每日", "明細", "LIST"]):
        bonus -= 2
    return bonus


def extract_row_signatures_v29(df, sheet_name, target=None):
    rows = []
    for idx in range(len(df)):
        row = df.iloc[idx]
        nums = [safe_int_v29(v, target) for v in row if safe_int_v29(v, target) is not None]
        if len(nums) < 1:
            continue

        text = row.astype(str).str.cat(sep=" ").upper()
        if is_noise_row_v29(text):
            continue
        if is_store_count_row_v29(text, nums):
            continue

        if len(nums) > 2:
            big_nums = [n for n in nums if n > 1000]
            small_nums = [n for n in nums if n <= 200]
            if big_nums and small_nums and target and target not in big_nums:
                nums = small_nums

        unit_val = None
        if len(nums) >= 2:
            c = Counter(nums)
            most_common, count = c.most_common(1)[0]
            if count >= 3 or count / len(nums) > 0.3:
                if target and most_common in SECONDS_BLACKLIST and most_common != target:
                    pass
                elif target and most_common in YEAR_BLACKLIST and most_common != target:
                    pass
                else:
                    unit_val = most_common

        level = "L3"
        if len(nums) == 1:
            level = "L1"
        else:
            max_n = max(nums)
            if max_n >= sum(nums) * 0.4:
                level = "L2"

        rows.append(
            {
                "sheet": sheet_name,
                "row_idx": idx,
                "sum": sum(nums),
                "nums": nums,
                "unit_val": unit_val,
                "count": len(nums),
                "text": text,
                "bonus": semantic_bonus_v29(text),
                "level": level,
                "raw_row": row.tolist(),
            }
        )
    return rows


def parse_excel_daily_ads(file_content, target_spots=None):
    file_hash = hashlib.md5(file_content).hexdigest()
    result = {
        "file_hash": file_hash,
        "file_name": "",
        "ai_interpretations": [],
        "raw_data": {},
        "error": None,
    }

    try:
        excel_file = io.BytesIO(file_content)
        excel_file.seek(0)
        xls = pd.ExcelFile(excel_file, engine="openpyxl")
        result["file_name"] = "cue_file.xlsx"

        for sheet_name in xls.sheet_names:
            try:
                df = pd.read_excel(excel_file, sheet_name=sheet_name, header=None, engine="openpyxl")
                result["raw_data"][sheet_name] = df.to_dict("records")
                row_signatures = extract_row_signatures_v29(df, sheet_name, target_spots)

                for sig in row_signatures:
                    if sig["unit_val"] and 1 <= sig["unit_val"] <= 1000:
                        interpretation = {
                            "sheet": sheet_name,
                            "row_idx": sig["row_idx"],
                            "col_idx": -1,
                            "date": "",
                            "ad_name": "",
                            "spots": sig["unit_val"],
                            "seconds": None,
                            "confidence": "medium" if sig["bonus"] > 0 else "low",
                            "rule_used": "unit_val_extraction_v29",
                            "reason": f"Row {sig['row_idx']+1}: 發現重複數值 {sig['unit_val']} (出現 {sig['count']} 次), level={sig['level']}, bonus={sig['bonus']}",
                            "raw_value": sig["unit_val"],
                            "raw_row": sig["raw_row"],
                        }
                        result["ai_interpretations"].append(interpretation)

                    if target_spots and sig["sum"] > 0:
                        diff_ratio = abs(sig["sum"] - target_spots) / target_spots if target_spots > 0 else 1
                        if diff_ratio < 0.1:
                            interpretation = {
                                "sheet": sheet_name,
                                "row_idx": sig["row_idx"],
                                "col_idx": -1,
                                "date": "",
                                "ad_name": "",
                                "spots": sig["sum"],
                                "seconds": None,
                                "confidence": "high" if diff_ratio < 0.05 else "medium",
                                "rule_used": "sum_match_target_v29",
                                "reason": f"Row {sig['row_idx']+1}: 總和 {sig['sum']} 接近目標 {target_spots} (誤差 {diff_ratio*100:.1f}%)",
                                "raw_value": sig["sum"],
                                "raw_row": sig["raw_row"],
                            }
                            result["ai_interpretations"].append(interpretation)

            except Exception as e:
                result["error"] = f"處理工作表 '{sheet_name}' 時發生錯誤: {str(e)}"

        excel_file.close()
        return result

    except Exception as e:
        result["error"] = f"讀取 Excel 檔案失敗: {str(e)}"
        return result


def parse_cue_excel_for_table1(file_content, order_info=None):
    result = []
    try:
        result = parse_cueapp_excel(file_content)
        if result:
            if order_info:
                for ad_unit in result:
                    ad_unit.update(
                        {
                            "client": order_info.get("client", ""),
                            "product": order_info.get("product", ""),
                            "sales": order_info.get("sales", ""),
                            "company": order_info.get("company", ""),
                            "order_id": order_info.get("order_id", ""),
                            "amount_net": order_info.get("amount_net", 0),
                        }
                    )
            return result

        excel_file = io.BytesIO(file_content)
        excel_file.seek(0)
        xls = pd.ExcelFile(excel_file, engine="openpyxl")

        for sheet_name in xls.sheet_names:
            try:
                df = pd.read_excel(excel_file, sheet_name=sheet_name, header=None, engine="openpyxl")
                df = df.loc[:, ~df.isna().all()]
                sheet_date_range = _parse_sheet_date_range(sheet_name)
                platform_info = _extract_platform_from_sheet(df, sheet_name)
                seconds_info = _extract_seconds_from_sheet(df, sheet_name)
                daily_spots_rows = _extract_daily_spots_rows(df, sheet_name, sheet_date_range)

                for spots_row in daily_spots_rows:
                    split_groups = _split_by_spots_change(
                        spots_row["daily_spots"],
                        spots_row["dates"],
                        spots_row.get("start_date"),
                        spots_row.get("end_date"),
                    )
                    for group in split_groups:
                        ad_unit = {
                            "platform": platform_info.get("platform", "未知"),
                            "platform_category": platform_info.get("category", "其他"),
                            "seconds": seconds_info.get("seconds", 0),
                            "region": platform_info.get("region", "未知"),
                            "ad_name": spots_row.get("ad_name", ""),
                            "daily_spots": group["daily_spots_list"] if "daily_spots_list" in group else [group["daily_spots"]] * group["days"],
                            "dates": group["dates"],
                            "start_date": group["start_date"],
                            "end_date": group["end_date"],
                            "total_spots": sum(group["daily_spots_list"]) if "daily_spots_list" in group else group["daily_spots"] * group["days"],
                            "days": group["days"],
                            "source_sheet": sheet_name,
                            "source_row": spots_row.get("row_idx", -1),
                            "split_reason": group.get("split_reason", "none"),
                            "split_groups": [group],
                        }
                        if order_info:
                            ad_unit.update(
                                {
                                    "client": order_info.get("client", ""),
                                    "product": order_info.get("product", ""),
                                    "sales": order_info.get("sales", ""),
                                    "company": order_info.get("company", ""),
                                    "order_id": order_info.get("order_id", ""),
                                    "amount_net": order_info.get("amount_net", 0),
                                }
                            )
                        result.append(ad_unit)

            except Exception as e:
                print(f"處理工作表 '{sheet_name}' 時發生錯誤: {str(e)}")
                continue

        excel_file.close()
        return result

    except Exception as e:
        print(f"讀取 Excel 檔案失敗: {str(e)}")
        return result


def _parse_sheet_date_range(sheet_name):
    patterns = [
        r"(\d{2})(\d{2})-(\d{2})(\d{2})",
        r"(\d{2})/(\d{2})-(\d{2})/(\d{2})",
    ]
    for pattern in patterns:
        m = re.search(pattern, sheet_name)
        if m and len(m.groups()) == 4:
            current_year = datetime.now().year
            start_month = int(m.group(1))
            start_day = int(m.group(2))
            end_month = int(m.group(3))
            end_day = int(m.group(4))
            try:
                start_date = datetime(current_year, start_month, start_day)
                end_date = datetime(current_year, end_month, end_day)
                return {"start": start_date.strftime("%Y-%m-%d"), "end": end_date.strftime("%Y-%m-%d")}
            except Exception:
                pass
    return None


def _extract_platform_from_sheet(df, sheet_name):
    platform_keywords = {
        "全家新鮮視": ["新鮮視", "VISION", "全家便利商店店鋪"],
        "全家廣播": ["全家廣播", "企頻", "RADIO", "企業頻道", "【全台全家共", "全家便利商店店鋪廣播"],
        "家樂福": ["家樂福", "CARREFOUR", "量販通路", "量販店", "超市"],
        "診所": ["診所", "CLINIC", "醫療", "醫院"],
    }
    region_keywords = ["全省", "北北基", "中彰投", "桃竹苗", "高高屏", "雲嘉南", "宜花東"]
    for idx in range(min(30, len(df))):
        row_text = " ".join(df.iloc[idx].astype(str).tolist())
        row_text_upper = row_text.upper()
        platform_found = None
        for platform in ["全家廣播", "全家新鮮視", "家樂福", "診所"]:
            keywords = platform_keywords.get(platform, [])
            if any(kw in row_text_upper or kw in row_text for kw in keywords):
                platform_found = platform
                break
        if platform_found:
            region = "全省"
            for r in region_keywords:
                if r in row_text:
                    region = r
                    break
            return {"platform": platform_found, "category": platform_found, "region": region}
    return {"platform": "未知", "category": "其他", "region": "未知"}


def _extract_seconds_from_sheet(df, sheet_name):
    for idx in range(min(20, len(df))):
        row_text = " ".join(df.iloc[idx].astype(str).tolist())
        patterns = [r"(\d+)\s*秒", r"(\d+)\s*\"", r"廣告秒數[：:]\s*(\d+)", r"秒數[：:]\s*(\d+)"]
        for pattern in patterns:
            m = re.search(pattern, row_text)
            if m:
                try:
                    seconds = int(m.group(1))
                    if 5 <= seconds <= 120:
                        return {"seconds": seconds}
                except Exception:
                    pass
    return {"seconds": 0}


def _extract_daily_spots_rows(df, sheet_name, date_range=None):
    result = []
    date_header_row_idx = None
    date_columns = []

    for idx in range(min(30, len(df))):
        row = df.iloc[idx]
        nums = []
        for col_idx, val in enumerate(row):
            try:
                num = int(float(val))
                if 1 <= num <= 31:
                    nums.append((col_idx, num))
            except Exception:
                pass
        if len(nums) >= 5:
            date_header_row_idx = idx
            date_columns = [col_idx for col_idx, _ in nums]
            break

    if date_header_row_idx is None or not date_columns:
        return result

    dates = []
    if date_range:
        start_date = pd.to_datetime(date_range["start"])
        end_date = pd.to_datetime(date_range["end"])
        date_list = pd.date_range(start_date, end_date, freq="D")
        dates = [d.strftime("%Y-%m-%d") for d in date_list]
    else:
        return result

    for idx in range(date_header_row_idx + 1, min(date_header_row_idx + 50, len(df))):
        row = df.iloc[idx]
        daily_spots = []
        for col_idx in date_columns[: len(dates)]:
            try:
                val = row.iloc[col_idx]
                if pd.notna(val):
                    spots = int(float(val))
                    if 0 <= spots <= 1000:
                        daily_spots.append(spots)
                    else:
                        daily_spots.append(0)
                else:
                    daily_spots.append(0)
            except Exception:
                daily_spots.append(0)

        if len([s for s in daily_spots if s > 0]) >= 3:
            ad_name = ""
            try:
                first_col = str(row.iloc[0]).strip()
                if first_col and first_col != "nan":
                    ad_name = first_col
            except Exception:
                pass
            result.append(
                {
                    "row_idx": idx,
                    "ad_name": ad_name,
                    "daily_spots": daily_spots,
                    "dates": dates[: len(daily_spots)],
                    "start_date": dates[0] if dates else "",
                    "end_date": dates[len(daily_spots) - 1] if dates and len(daily_spots) > 0 else "",
                }
            )
    return result


def _split_by_spots_change(daily_spots, dates, start_date=None, end_date=None):
    if not daily_spots or not dates:
        return []
    groups = []
    current_group = {"daily_spots": daily_spots[0], "daily_spots_list": [daily_spots[0]], "dates": [dates[0]], "start_date": dates[0]}
    for i in range(1, len(daily_spots)):
        if daily_spots[i] != current_group["daily_spots"]:
            current_group["end_date"] = dates[i - 1]
            current_group["days"] = len(current_group["daily_spots_list"])
            current_group["split_reason"] = "daily_spots_change"
            groups.append(current_group)
            current_group = {"daily_spots": daily_spots[i], "daily_spots_list": [daily_spots[i]], "dates": [dates[i]], "start_date": dates[i]}
        else:
            current_group["daily_spots_list"].append(daily_spots[i])
            current_group["dates"].append(dates[i])
    if current_group:
        current_group["end_date"] = dates[-1]
        current_group["days"] = len(current_group["daily_spots_list"])
        current_group["split_reason"] = "daily_spots_change" if len(groups) > 0 else "none"
        groups.append(current_group)
    return groups
