import hashlib
import io
import re
from collections import Counter
from datetime import date, datetime

import pandas as pd

# 播出時段樣式 07:00-23:00（欄位驗證／列推斷共用）
_TIME_RANGE_RE = re.compile(r"\d{1,2}\s*:\s*\d{2}\s*[-~－至到]\s*\d{1,2}\s*:\s*\d{2}")


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


def _parse_cueapp_period_shenghuo_bolin(df, search_rows=None):
    """從聲活/鉑霖格式中找「執行期間：YYYY.MM.DD - YYYY.MM.DD」（V2 可能上移／下移，多列掃描）"""
    if search_rows is None:
        search_rows = range(0, min(28, len(df)))
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


def _cueapp_top_block_text(df: pd.DataFrame, max_rows: int = 14) -> str:
    """合併表頭區多列文字，辨識公司名／版型（避免 V2 首列空白或標題下移）。"""
    parts = []
    for ri in range(min(max_rows, len(df))):
        try:
            parts.append(df.iloc[ri].fillna("").astype(str).str.cat(sep=" "))
        except Exception:
            continue
    return " ".join(parts)


def _row_text_df(df: pd.DataFrame, i: int) -> str:
    try:
        return df.iloc[i].fillna("").astype(str).str.cat(sep=" ")
    except Exception:
        return ""


def _find_cueapp_schedule_header_row(df: pd.DataFrame) -> int | None:
    """
    找出排程表 anchor 列（右側為「幾日」數字之列）。
    左側頻道／秒數常跨兩列合併（秒數在第二列）；須合併 (i, i+1) 判斷關鍵字，再以數字日期欄數選 i 或 i+1。
    """
    n = len(df)
    for i in range(min(50, n)):
        single = _row_text_df(df, i)
        if not single or len(single.strip()) < 4:
            continue
        pair = single + (" " + _row_text_df(df, i + 1) if i + 1 < n else "")
        if _schedule_header_text_matches(pair):
            if i + 1 < n:
                si = _row_day_header_count(df, i)
                sj = _row_day_header_count(df, i + 1)
                return i if si >= sj else i + 1
            return i
        if _schedule_header_text_matches(single):
            return i
    return None


def _cell_val(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    if hasattr(v, "date"):
        return v.date() if hasattr(v, "date") else v
    return v


def _parse_cueapp_day_header_cell(v):
    """表頭「幾日」欄位：1–31 或 datetime；週幾（一～日）回傳 None。"""
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


def _row_day_header_count(df: pd.DataFrame, row_i: int) -> int:
    if row_i < 0 or row_i >= len(df):
        return 0
    n = 0
    for j in range(min(45, df.shape[1])):
        if _parse_cueapp_day_header_cell(df.iloc[row_i, j]) is not None:
            n += 1
    return n


def _schedule_header_text_matches(t: str) -> bool:
    if not t or len(t.strip()) < 4:
        return False
    t_flat = t.replace("\n", "")
    # 表頭或合併列常為「15秒廣告」：有「秒」但無「秒數」二字，需一併視為秒數欄
    has_sec = (
        "秒數" in t
        or bool(re.search(r"\d+\s*秒", t_flat))
        or "Size" in t
        or ("size" in t.lower() and "station" in t.lower())
        or "秒數規格" in t_flat
    )
    ch_sched = (
        "頻道" in t
        and has_sec
        and (
            "播出地區" in t
            or "播出區域" in t
            or "播出店數" in t
            or ("播出" in t and "地區" in t)
            or ("播出" in t and "區域" in t)
        )
    )
    en_sched = "Station" in t and "Location" in t and ("Size" in t or "秒數" in t)
    ch_loose = "頻道" in t and ("秒數" in t_flat or bool(re.search(r"\d+\s*秒", t_flat)))
    en_loose = ("Station" in t or "Channel" in t) and ("Size" in t or "秒數" in t)
    return bool(ch_sched or en_sched or ch_loose or en_loose)


def _find_cueapp_sec_col(df: pd.DataFrame, header_row_idx: int, row_span: int = 2) -> int | None:
    """秒數／Size 欄可能在合併表頭的第二列，需掃描 header_row_idx 起連續列。"""
    for dr in range(row_span):
        ri = header_row_idx + dr
        if ri >= len(df):
            break
        for j in range(min(25, df.shape[1])):
            s = str(df.iloc[ri, j]).strip()
            if ("秒數" in s) or bool(re.search(r"\d+\s*秒", s)) or (s.lower() == "size") or ("size" in s.lower()):
                return j
    return None


def _find_first_day_column_streak(
    df: pd.DataFrame, r: int, min_col: int = 2, max_col: int = 50
) -> int | None:
    """
    在數字日期列上找第一組連續 >=3 欄的「1–31」起點 index。
    用於與秒數欄幾何對齊，避免 _find_cueapp_sec_col 先命中表頭區較早的「15秒廣告」。
    """
    if r < 0 or r >= len(df):
        return None
    streak = 0
    start_j: int | None = None
    best_start: int | None = None
    best_len = 0
    for j in range(min_col, min(max_col, df.shape[1])):
        if _parse_cueapp_day_header_cell(df.iloc[r, j]) is not None:
            if streak == 0:
                start_j = j
            streak += 1
        else:
            if streak >= 3 and streak > best_len and start_j is not None:
                best_len = streak
                best_start = start_j
            streak = 0
            start_j = None
    if streak >= 3 and streak > best_len and start_j is not None:
        best_start = start_j
    return best_start


def _cue_header_seconds_like(df: pd.DataFrame, base_row: int, j: int, row_span: int = 2) -> bool:
    """表頭鄰列該欄是否像秒數／規格（鬆匹配）。"""
    if j < 0:
        return False
    for dr in range(row_span):
        ri = base_row + dr
        if ri >= len(df):
            break
        s = str(df.iloc[ri, j]).replace("\n", " ").strip().lower()
        if "秒" in s or "size" in s or "規格" in s:
            return True
    return False


_WEEKDAYS_CN = frozenset("一二三四五六日")


def _cell_is_weekday_cn(v) -> bool:
    s = str(v).strip() if v is not None else ""
    return len(s) == 1 and s in _WEEKDAYS_CN


def _row_month_hint_from_text(df: pd.DataFrame, r: int, max_col: int = 40) -> int | None:
    """從單列文字中找「N月」作為月份提示。"""
    if r < 0 or r >= len(df):
        return None
    for j in range(min(max_col, df.shape[1])):
        s = str(df.iloc[r, j]).replace("\n", " ").strip()
        m = re.search(r"(\d{1,2})\s*月", s)
        if m:
            mm = int(m.group(1))
            if 1 <= mm <= 12:
                return mm
    return None


def _infer_month_neighborhood(df: pd.DataFrame, anchor_row: int) -> int | None:
    """錨列上下數列內找「N月」等月份提示（不依賴單一欄位）。"""
    for dr in range(-4, 3):
        ri = anchor_row + dr
        mm = _row_month_hint_from_text(df, ri)
        if mm is not None:
            return mm
    return None


def _count_numeric_day_headers_in_row(df: pd.DataFrame, r: int, j0: int, j1: int) -> int:
    if r < 0 or r >= len(df):
        return 0
    n = 0
    for j in range(j0, min(j1, df.shape[1])):
        if _parse_cueapp_day_header_cell(df.iloc[r, j]) is not None:
            n += 1
    return n


def _pick_numeric_day_header_row(
    df: pd.DataFrame,
    anchor_row: int,
    date_start_col: int,
    diagnostics: list | None,
    min_days: int = 3,
) -> tuple[int, list[str]]:
    """
    在錨列上下多列中，選「日期數字（1–31）」最密集之列，避免把「4月」列或週幾列當成日期錨點。
    """
    notes: list[str] = []
    j1 = min(df.shape[1], date_start_col + 80)
    best_r, best_n = anchor_row, -1
    for r in range(max(0, anchor_row - 3), min(len(df), anchor_row + 4)):
        cnt = _count_numeric_day_headers_in_row(df, r, date_start_col, j1)
        if cnt > best_n:
            best_n, best_r = cnt, r
    if best_n < min_days:
        msg = (
            f"日期表頭：錨列上下未找到足夠的「幾日」數字欄（最多 {best_n} 欄，需要至少 {min_days}），"
            f"可能為合併異常或日期與週幾列位置與預期不同。"
        )
        notes.append(msg)
        if diagnostics is not None:
            diagnostics.append(msg)
        return anchor_row, notes
    if best_r != anchor_row:
        msg = f"日期表頭：以第 {best_r + 1} 列（0-based={best_r}）為「幾日」數字列，而非原錨列 {anchor_row + 1}。"
        notes.append(msg)
        if diagnostics is not None:
            diagnostics.append(msg)
    return best_r, notes


def _row_looks_like_weekday_subheader(df: pd.DataFrame, r: int, date_start_col: int, n_slots: int) -> bool:
    """整段日期欄多為週幾單字 → 週幾子表頭列。"""
    if r < 0 or r >= len(df):
        return False
    row = df.iloc[r]
    wd, filled = 0, 0
    for c in range(date_start_col, min(len(row), date_start_col + n_slots)):
        s = str(row.iloc[c]).strip()
        if not s or s.lower() == "nan":
            continue
        filled += 1
        if _cell_is_weekday_cn(row.iloc[c]):
            wd += 1
    return filled >= 3 and wd >= filled * 0.55


def _row_looks_like_month_banner_row(df: pd.DataFrame, r: int, date_start_col: int, n_slots: int) -> bool:
    """日期區僅見「N月」類、幾乎無數字日期 → 月份列。"""
    if r < 0 or r >= len(df):
        return False
    row = df.iloc[r]
    month_cells, num_days = 0, 0
    for c in range(date_start_col, min(len(row), date_start_col + n_slots)):
        s = str(row.iloc[c]).replace("\n", " ").strip()
        if not s or s.lower() == "nan":
            continue
        if re.search(r"\d{1,2}\s*月", s):
            month_cells += 1
        if _parse_cueapp_day_header_cell(df.iloc[r, c]) is not None:
            num_days += 1
    return month_cells >= 1 and num_days <= 1


def _find_ch_schedule_data_start_row(
    df: pd.DataFrame,
    numeric_day_row: int,
    date_start_col: int,
    n_date_cols: int,
    diagnostics: list | None,
) -> int:
    """
    自數字日期列下一列起掃描：略過週幾列、略過單純「N月」列，再找第一列像資料列者。
    仍找不到則回退 numeric_day_row+2（舊版慣例）。
    """
    r = numeric_day_row + 1
    limit = min(numeric_day_row + 8, len(df))
    skipped: list[str] = []
    while r < limit:
        if _row_looks_like_month_banner_row(df, r, date_start_col, n_date_cols):
            skipped.append(f"列{r+1}視為月份列")
            r += 1
            continue
        if _row_looks_like_weekday_subheader(df, r, date_start_col, n_date_cols):
            skipped.append(f"列{r+1}視為週幾子表頭")
            r += 1
            continue
        row = df.iloc[r]
        spot_sum = sum(
            _safe_spots(row.iloc[c])
            for c in range(date_start_col, min(len(row), date_start_col + n_date_cols))
        )
        if spot_sum > 0:
            msg = f"資料起始列：第 {r + 1} 列（略過：{'; '.join(skipped) if skipped else '無'}）。"
            if diagnostics is not None:
                diagnostics.append(msg)
            return r
        r += 1
    fallback = numeric_day_row + 2
    msg = f"資料起始列：未在緊鄰列找到檔次數字，回退為第 {fallback + 1} 列（numeric_row+2）。略過：{'; '.join(skipped) if skipped else '無'}。"
    if diagnostics is not None:
        diagnostics.append(msg)
    return min(fallback, len(df) - 1) if len(df) > 0 else 0


def _validate_left_block_against_samples(
    df: pd.DataFrame,
    sec_col: int,
    date_start_col: int,
    data_start_row: int,
    n_date_cols: int,
    diagnostics: list | None,
    max_samples: int = 10,
) -> None:
    """
    在表頭下方抽樣含檔次之列，比對左側欄位是否像「頻道／地區／店數／時段／秒數」語意；差異大則寫入診斷，不中止解析。
    """
    if diagnostics is None:
        return
    region_kw = ("雲嘉南", "高屏", "北北基", "桃竹苗", "中彰投", "宜花東", "全省", "高高屏")
    samples: list[int] = []
    for r in range(data_start_row, min(data_start_row + 40, len(df))):
        row = df.iloc[r]
        if str(row.iloc[date_start_col]).strip() in _WEEKDAYS_CN if date_start_col < len(row) else False:
            continue
        spots = sum(
            _safe_spots(row.iloc[c])
            for c in range(date_start_col, min(len(row), date_start_col + n_date_cols))
        )
        if spots <= 0:
            continue
        tsec = str(row.iloc[sec_col]).replace("\n", " ") if sec_col < len(row) else ""
        if "total" in tsec.lower():
            continue
        samples.append(r)
        if len(samples) >= max_samples:
            break
    if not samples:
        diagnostics.append("欄位驗證：表頭下方短區間內未取到含檔次之資料列樣本，無法驗證左側欄位（仍繼續解析）。")
        return
    reg_hit = 0
    time_hit = 0
    sec_hit = 0
    ch_hit = 0
    for r in samples:
        row = df.iloc[r]
        if df.shape[1] > 1:
            b = str(row.iloc[1]).strip()
            if any(k in b for k in region_kw) or (len(b) <= 8 and any(x in b for x in ("區", "屏", "基", "南", "北", "中"))):
                reg_hit += 1
        rt = row.fillna("").astype(str).str.cat(sep=" ")[:200]
        if _TIME_RANGE_RE.search(rt) or ("07:" in rt and "23:" in rt):
            time_hit += 1
        if sec_col < len(row):
            sc = str(row.iloc[sec_col]).replace("\n", " ")
            if re.search(r"\d+\s*秒", sc) or "秒" in sc:
                sec_hit += 1
        if df.shape[1] > 0:
            a = str(row.iloc[0]).strip()
            if a and a.lower() != "nan" and len(a) >= 2:
                if any(k in a for k in ("全家", "廣播", "新鮮", "家樂福", "FM", "店鋪")):
                    ch_hit += 1
    n = len(samples)
    if reg_hit < max(1, int(n * 0.25)):
        diagnostics.append(
            f"欄位驗證（警告）：播出地區欄樣本命中率 {reg_hit}/{n} 偏低，若地區錯請檢查是否多插欄或合併格位移。"
        )
    if time_hit < max(1, int(n * 0.15)):
        diagnostics.append(
            f"欄位驗證（提示）：播出時段格式樣本命中率 {time_hit}/{n}，若表無時段欄可忽略。"
        )
    if sec_hit < max(1, int(n * 0.25)):
        diagnostics.append(
            f"欄位驗證（警告）：秒數欄（索引 {sec_col}）樣本命中率 {sec_hit}/{n}，若秒數錯請檢查秒數欄是否位移。"
        )
    if ch_hit < max(1, int(n * 0.2)):
        diagnostics.append(
            f"欄位驗證（提示）：頻道欄樣本命中率 {ch_hit}/{n}（合併儲存格下列常空白，可能偏低屬正常）。"
        )


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


def parse_cueapp_excel(file_content, diagnostics_out: list | None = None):
    result = []
    try:
        excel_file = io.BytesIO(file_content)
        xls = pd.ExcelFile(excel_file, engine="openpyxl")
    except Exception:
        return []

    for sheet_name in xls.sheet_names:
        try:
            def _diag(msg: str) -> None:
                if diagnostics_out is not None:
                    diagnostics_out.append(f"[{sheet_name}] {msg}")

            excel_file.seek(0)
            df = pd.read_excel(excel_file, sheet_name=sheet_name, header=None, engine="openpyxl")
            if df.empty or len(df) < 9:
                _diag("略過：工作表為空或列數 < 9。")
                continue
            top_block = _cueapp_top_block_text(df, max_rows=22)
            row0_text = _row_text_df(df, 0)
            fmt = None
            # 順序重要：鉑霖標題含「Mobi Media Schedule」，不可先判成東吳 Media Schedule
            if "聲活數位" in top_block:
                fmt = "shenghuo"
            elif (
                "鉑霖行動行銷" in top_block
                or "Mobi Media Schedule" in top_block
                or ("鉑霖" in top_block and "排程表" in top_block)
                or ("鉑霖" in top_block and "媒體計劃" in top_block)
                or ("媒體計劃" in top_block and "Mobi" in top_block)
            ):
                fmt = "bolin"
            elif "Media Schedule" in top_block or (len(df.columns) > 0 and str(df.iloc[0, 0]).strip() == "Media Schedule"):
                fmt = "dongwu"

            if fmt is None:
                if re.match(r"^\d+月$", str(sheet_name).strip()):
                    b5 = df.iloc[4, 1] if df.shape[1] > 1 else None
                    start, end = _parse_cueapp_period_dongwu(b5)
                    if start and end:
                        fmt = "dongwu"
                if fmt is None:
                    _diag("略過：無法判定為東吳／聲活／鉑霖（頂部關鍵字與工作表名皆不符）。")
                    continue

            start_date, end_date = None, None
            date_start_col = None
            eff_days = None
            header_row_idx = None
            sec_col: int | None = None
            schedule_day_row: int | None = None
            data_start_row: int | None = None

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
                    schedule_day_row = header_row_idx
                    data_start_row = header_row_idx + 2
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
                    header_row_idx = _find_cueapp_schedule_header_row(df)
                    if header_row_idx is None:
                        _diag("東吳：找不到排程表頭列。")
                        continue
                    sec_col = _find_cueapp_sec_col(df, header_row_idx)
                    if sec_col is None:
                        _diag("東吳：找不到秒數／Size 欄。")
                        continue
                    hdr_join = _row_text_df(df, header_row_idx)
                    # 東吳英文表固定 A~G（0~6）為 Station…Package-cost，日期自第 8 欄（index 7）起
                    if "Station" in hdr_join and "Location" in hdr_join:
                        date_start_col = 7
                    else:
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
                        _diag("東吳：表頭右側未找到連續「幾日」數字欄。")
                        continue
                    eff_days = len(day_cols)
                    schedule_day_row = header_row_idx
                    data_start_row = header_row_idx + 2
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
                        _diag("東吳：無法由表頭日期組出有效西曆日期。")
                        continue
                    start_date = min(dates2)
                    end_date = max(dates2)
            else:
                start_date, end_date = _parse_cueapp_period_shenghuo_bolin(df)
                header_row_idx = _find_cueapp_schedule_header_row(df)
                if header_row_idx is None:
                    _diag("聲活／鉑霖：找不到排程表頭列（頻道、秒數線索、播出地區等）。")
                    continue
                sec_guess = _find_cueapp_sec_col(df, header_row_idx)
                if sec_guess is None:
                    _diag("聲活／鉑霖：找不到秒數／Size 欄（含 15秒廣告 等形式）。")
                    continue
                # 自第 3 欄起掃「幾日」數字，避免誤用表頭上方「廣告規格／15秒」附近的欄位
                schedule_day_row, pick_notes = _pick_numeric_day_header_row(df, header_row_idx, 3, None)
                for note in pick_notes:
                    _diag(note)
                first_day_j = _find_first_day_column_streak(df, schedule_day_row, min_col=2, max_col=min(55, df.shape[1]))
                if first_day_j is not None:
                    sec_col = first_day_j - 1
                    date_start_col = first_day_j
                    if not _cue_header_seconds_like(df, schedule_day_row, sec_col):
                        _diag(
                            f"欄位幾何：日期數字區起於欄索引 {first_day_j}（左鄰 {sec_col} 表頭未含秒數／規格關鍵字），"
                            "仍採左鄰為秒數欄；若檔次仍為 0 請檢查合併格。"
                        )
                    elif sec_guess != sec_col:
                        _diag(
                            f"秒數欄校正：表頭先掃到索引 {sec_guess}，依「幾日」連續區左界改為 {sec_col}（避免誤用較早的 15 秒欄）。"
                        )
                else:
                    sec_col = sec_guess
                    date_start_col = sec_guess + 1
                    _diag("未在數字日期列偵測到連續「幾日」區塊，退回初次秒數掃描決定日期起欄。")
                flex_month = _infer_month_neighborhood(df, schedule_day_row)
                if flex_month is not None:
                    _diag(f"月份提示：表頭鄰近掃到「{flex_month} 月」（與執行期間併用於拼日期）。")

                day_cols = []
                for j in range(date_start_col, min(df.shape[1], date_start_col + 80)):
                    d = _parse_day_cell(df.iloc[schedule_day_row, j])
                    if d is None:
                        if day_cols:
                            break
                        continue
                    day_cols.append((j, d))
                if not day_cols:
                    _diag(
                        f"在「幾日」數字列（第 {schedule_day_row + 1} 列）右側未找到連續日期欄（1–31）；"
                        "常見原因：日期與週幾分行、上方另有「N月」列、或合併格導致空白。"
                    )
                    continue
                eff_days = len(day_cols)

                year = _infer_year_from_df(df) or (start_date.year if start_date else None)
                if year is None:
                    year = datetime.now().year
                months = []
                last_day = None
                last_month = None
                base_month = (start_date.month if start_date else None) or flex_month
                if base_month is None:
                    _diag("未由執行期間或「N月」列取得月份，將依欄位上方逐欄推斷或沿用上一欄月份；跨月請人工核對。")
                for j, d in day_cols:
                    mm = _infer_month_for_col(df, schedule_day_row, j) or base_month
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
                    _diag("聲活／鉑霖：無法由「幾日」與月份組出有效西曆日期。")
                    continue
                start_date = start_date or min(dates)
                end_date = end_date or max(dates)

            if eff_days is None or eff_days <= 0:
                _diag("有效檔期天數為 0，略過本分頁。")
                continue
            dates_str = None
            date_header_row = schedule_day_row if schedule_day_row is not None else header_row_idx
            if fmt != "dongwu" and date_header_row is not None and date_start_col is not None:
                try:
                    day_cols2 = []
                    for j in range(date_start_col, min(df.shape[1], date_start_col + 80)):
                        d = _parse_day_cell(df.iloc[date_header_row, j])
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
                            mm = _infer_month_for_col(df, date_header_row, j) or last_month2
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
                except Exception as e:
                    dates_str = None
                    _diag(f"重建逐日日期字串時發生例外（已改以執行期間填補）：{e}")
            if not dates_str:
                date_list = pd.date_range(start_date, end_date, freq="D")
                if len(date_list) != eff_days:
                    date_list = date_list[:eff_days]
                dates_str = [d.strftime("%Y-%m-%d") for d in date_list]
                if fmt in ("bolin", "shenghuo"):
                    _diag(
                        f"逐日日期改由執行期間連續展開（{eff_days} 天）；若與表頭「幾日」不完全對齊，請以表頭為準人工核對。"
                    )

            if fmt in ("bolin", "shenghuo") and schedule_day_row is not None and date_start_col is not None and sec_col is not None:
                data_start_row = _find_ch_schedule_data_start_row(
                    df, schedule_day_row, date_start_col, eff_days, diagnostics_out
                )
                _validate_left_block_against_samples(
                    df, sec_col, date_start_col, data_start_row, eff_days, diagnostics_out
                )
            elif data_start_row is None:
                data_start_row = (header_row_idx if header_row_idx is not None else 0) + 2

            platform_info = _extract_platform_from_sheet(df, sheet_name)
            seconds_info = _extract_seconds_from_sheet(df, sheet_name)
            default_seconds = seconds_info.get("seconds", 0)
            # 鉑霖／聲活：A 欄「頻道」直向合併，pandas 僅第一列有字、下列為 NaN，需沿用上一列頻道名才能讀到第二區（如高屏）
            last_merged_channel = ""

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
                    total_chk_col = sec_col if fmt in ("bolin", "shenghuo") and sec_col is not None else 4
                    e_val = row.iloc[total_chk_col] if len(row) > total_chk_col else None
                    e_str = str(e_val).strip() if e_val is not None else ""
                    if "Total" in e_str or "total" in e_str or e_str == "Total":
                        break
                    raw_a = row.iloc[0] if len(row) > 0 else None
                    first_cell = (
                        str(raw_a).strip()
                        if raw_a is not None and not (isinstance(raw_a, float) and pd.isna(raw_a)) and str(raw_a).strip().lower() != "nan"
                        else ""
                    )
                    if first_cell:
                        last_merged_channel = first_cell
                    elif fmt in ("bolin", "shenghuo") and last_merged_channel:
                        first_cell = last_merged_channel
                    if not first_cell:
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


# --- 彈性掃描：版型（東吳／聲活／鉑霖）、表頭欄位對應、檔次列推斷、診斷報告 -----------------

# 邏輯欄位 → 各版型可能出現的標題關鍵字（子字串比對）
CUE_HEADER_FIELD_ALIASES: dict[str, list[str]] = {
    "channel": ["頻道", "Station", "Channel", "通道"],
    "region": ["播出地區", "播出區域", "播放地區", "播放區域", "Location", "地區"],
    "store_count": ["播出店數", "店數", "門市數", "Stores"],
    "time_window": ["播出時間", "時段", "播放時間", "Time", "Day-part"],
    "seconds_spec": ["秒數規格", "秒數", "Size", "廣告秒數"],
    "slots_total": ["檔次", "總檔次", "Spots", "Total Spots"],
    "list_price": ["定價", "List", "牌價", "市價", "List Price"],
    "project_price": ["專案價", "專案價格", "成交價", "優惠價", "Project", "Package"],
    "cost_remarks": ["製作", "VAT", "稅", "小計", "合計", "備註"],
}


def detect_cue_vendor_from_sheet_block(
    top_block: str, sheet_name: str = "", first_cell_a1: str | None = None
) -> tuple[str | None, list[str]]:
    """
    與 parse_cueapp_excel 相同優先序：聲活 → 鉑霖（含 Mobi）→ 東吳；避免鉑霖誤判東吳。
    回傳 (shenghuo|bolin|dongwu|None, 人類可讀說明)。
    """
    notes: list[str] = []
    sn = str(sheet_name).strip()
    a1 = str(first_cell_a1).strip() if first_cell_a1 is not None else ""
    if "聲活數位" in top_block:
        notes.append("命中：聲活數位")
        return "shenghuo", notes
    if (
        "鉑霖行動行銷" in top_block
        or "Mobi Media Schedule" in top_block
        or ("鉑霖" in top_block and "排程表" in top_block)
        or ("鉑霖" in top_block and "媒體計劃" in top_block)
        or ("媒體計劃" in top_block and "Mobi" in top_block)
    ):
        notes.append("命中：鉑霖／Mobi 媒體排程表")
        return "bolin", notes
    if "Media Schedule" in top_block or a1 == "Media Schedule":
        notes.append("命中：Media Schedule（東吳系）")
        return "dongwu", notes
    if re.match(r"^\d+月$", sn):
        notes.append(f"工作表名「{sn}」常見於東吳月份表，需搭配 B5 期間解析")
        return None, notes
    notes.append("頂部區塊無法唯一對應東吳／聲活／鉑霖")
    return None, notes


def quick_scan_cue_workbook(file_content: bytes, max_rows_per_sheet: int = 45) -> dict:
    """
    快速：各工作表僅讀前 max_rows_per_sheet 列，合併文字後做版型線索與多分頁提示。
    """
    issues: list[str] = []
    sheets_out: list[dict] = []
    try:
        bio = io.BytesIO(file_content)
        xls = pd.ExcelFile(bio, engine="openpyxl")
    except Exception as e:
        return {"ok": False, "error": str(e), "sheet_names": [], "sheet_count": 0, "per_sheet": [], "issues": [f"無法開啟 Excel：{e}"]}

    names = list(xls.sheet_names)
    if len(names) > 1:
        issues.append(f"此檔含 {len(names)} 個工作表：{', '.join(names[:8])}{'…' if len(names) > 8 else ''}；解析時會逐表嘗試。")

    for name in names:
        try:
            bio.seek(0)
            df = pd.read_excel(bio, sheet_name=name, header=None, engine="openpyxl", nrows=max_rows_per_sheet)
            top = _cueapp_top_block_text(df, max_rows=min(22, len(df)))
            a1 = str(df.iloc[0, 0]).strip() if df.shape[1] > 0 else ""
            vendor, vnotes = detect_cue_vendor_from_sheet_block(top, name, first_cell_a1=a1)
            sheets_out.append(
                {
                    "sheet_name": name,
                    "rows_scanned": len(df),
                    "vendor_guess": vendor,
                    "vendor_notes": vnotes,
                    "text_preview": (top[:280] + "…") if len(top) > 280 else top,
                }
            )
        except Exception as e:
            sheets_out.append({"sheet_name": name, "error": str(e), "vendor_guess": None})

    try:
        xls.close()
    except Exception:
        pass

    return {
        "ok": True,
        "sheet_names": names,
        "sheet_count": len(names),
        "per_sheet": sheets_out,
        "issues": issues,
    }


def map_cue_header_fields(df: pd.DataFrame, header_anchor_row: int, row_span: int = 2) -> dict[str, list[int]]:
    """在表頭 anchor 列起 row_span 列內，掃描欄位標題對應到邏輯欄位（回傳欄 index 列表）。"""
    out: dict[str, list[int]] = {k: [] for k in CUE_HEADER_FIELD_ALIASES}
    for dr in range(row_span):
        ri = header_anchor_row + dr
        if ri < 0 or ri >= len(df):
            continue
        for j in range(min(45, df.shape[1])):
            raw = df.iloc[ri, j]
            if raw is None or (isinstance(raw, float) and pd.isna(raw)):
                continue
            cell = str(raw).replace("\n", " ").strip()
            if not cell or cell.lower() == "nan":
                continue
            cell_lower = cell.lower()
            for field, aliases in CUE_HEADER_FIELD_ALIASES.items():
                for alias in aliases:
                    if alias.isascii():
                        hit = alias.lower() in cell_lower
                    else:
                        hit = alias in cell
                    if hit:
                        if j not in out[field]:
                            out[field].append(j)
                        break
    return out


_REGION_TOKENS = frozenset(
    ["全省", "北北基", "桃竹苗", "中彰投", "雲嘉南", "高高屏", "高屏", "宜花東", "北區", "中區", "南區"]
)


def _classify_one_body_row(
    row: pd.Series,
    date_start_col: int,
    num_day_cols: int,
    fmt: str | None,
    last_channel: str,
) -> tuple[str, str, str]:
    """
    回傳 (status, channel_effective, reason_zh)
    status: data_candidate | skipped | stop_table | header_leak
    """
    n = len(row)
    if date_start_col is not None and date_start_col < n:
        dm = str(row.iloc[date_start_col]).strip()
        if dm in ("一", "二", "三", "四", "五", "六", "日"):
            return "header_leak", last_channel, "日期欄為週幾字樣，視為表頭延伸列"

    e_val = row.iloc[4] if n > 4 else None
    e_str = str(e_val).strip() if e_val is not None else ""
    if "Total" in e_str or e_str.lower() == "total":
        return "stop_table", last_channel, "播出時間／摘要欄出現 Total，視為表尾合計列"

    raw_a = row.iloc[0] if n > 0 else None
    first = (
        str(raw_a).strip()
        if raw_a is not None and not (isinstance(raw_a, float) and pd.isna(raw_a)) and str(raw_a).strip().lower() != "nan"
        else ""
    )
    ch_eff = first if first else last_channel
    if fmt in ("bolin", "shenghuo") and not first and last_channel:
        ch_eff = last_channel
    if not ch_eff:
        return "skipped", last_channel, "頻道欄空白且無合併沿用"

    rt = row.iloc[1] if n > 1 else None
    rtxt = str(rt).strip() if rt is not None and str(rt).lower() != "nan" else ""
    row_join = row.fillna("").astype(str).str.cat(sep=" ")[:500]
    region_hint = any(t in row_join for t in _REGION_TOKENS) or (rtxt and len(rtxt) <= 12 and not rtxt.isdigit())
    time_hint = bool(_TIME_RANGE_RE.search(row_join)) or ("07:" in row_join and "23:" in row_join)

    spots = 0
    end_c = min(n, date_start_col + max(1, num_day_cols))
    if date_start_col is not None and date_start_col >= 0:
        for c in range(date_start_col, end_c):
            if _safe_spots(row.iloc[c]) > 0:
                spots += 1

    if spots < 1:
        noise = any(x in row_join for x in ["元", "$", "VAT", "製作費", "發票", "統一編號", "乙方"])
        if noise:
            return "skipped", ch_eff if first else last_channel, "疑似報價／備註列（金額、稅、製作費等）"
        return "skipped", ch_eff if first else last_channel, "日期區間無正檔次數字"

    reasons = []
    if region_hint:
        reasons.append("含地區或區域名稱")
    if time_hint:
        reasons.append("含時段格式")
    reasons.append(f"日期欄有 {spots} 天檔次>0")
    return "data_candidate", (first or last_channel), "；".join(reasons)


def analyze_cue_schedule_body_rows(
    df: pd.DataFrame,
    fmt: str | None,
    header_row_idx: int,
    date_start_col: int | None,
    eff_day_columns: int,
) -> list[dict]:
    """表頭下方逐列標註是否像「檔次／排程資料列」（僅推斷，與 parse 邏輯對齊）。"""
    if header_row_idx is None or date_start_col is None or eff_day_columns <= 0:
        return []
    data_start = header_row_idx + 2
    last_ch = ""
    rows_out: list[dict] = []
    for r in range(data_start, min(data_start + 200, len(df))):
        row = df.iloc[r]
        st, ch_next, why = _classify_one_body_row(row, date_start_col, eff_day_columns, fmt, last_ch)
        if st == "data_candidate" and row.iloc[0] is not None and not (isinstance(row.iloc[0], float) and pd.isna(row.iloc[0])):
            s0 = str(row.iloc[0]).strip()
            if s0 and s0.lower() != "nan":
                last_ch = s0
        elif st == "data_candidate" and fmt in ("bolin", "shenghuo") and last_ch:
            last_ch = last_ch
        rows_out.append(
            {
                "row_1based": r + 1,
                "row_index": r,
                "status": st,
                "channel_effective": ch_next,
                "reason": why,
            }
        )
        if st == "stop_table":
            break
    return rows_out


def parse_cueapp_excel_with_report(file_content: bytes) -> dict:
    """
    拆解 CUE 並附診斷：多分頁、版型、表頭欄位對應、檔次列推斷。
    回傳鍵：ad_units, ad_unit_count, workbook_scan, sheets, issues, warnings
    """
    warnings: list[str] = []
    issues: list[str] = []

    try:
        workbook_scan = quick_scan_cue_workbook(file_content)
        if not workbook_scan.get("ok"):
            issues.extend(workbook_scan.get("issues") or [])
        else:
            issues.extend(workbook_scan.get("issues") or [])
    except Exception as e:
        workbook_scan = {"ok": False, "error": str(e)}
        warnings.append(f"快速掃描例外：{e}")

    parse_diagnostics: list[str] = []
    ad_units = parse_cueapp_excel(file_content, diagnostics_out=parse_diagnostics)

    sheets_report: list[dict] = []
    try:
        bio = io.BytesIO(file_content)
        xls = pd.ExcelFile(bio, engine="openpyxl")
        for sheet_name in xls.sheet_names:
            entry: dict = {"sheet_name": sheet_name}
            try:
                bio.seek(0)
                df = pd.read_excel(bio, sheet_name=sheet_name, header=None, engine="openpyxl")
                if df.empty or len(df) < 9:
                    entry["skipped"] = True
                    entry["reason"] = "工作表為空或列數過少（<9）"
                    sheets_report.append(entry)
                    continue

                top = _cueapp_top_block_text(df, max_rows=22)
                a1 = str(df.iloc[0, 0]).strip() if df.shape[1] > 0 else ""
                vendor, vnotes = detect_cue_vendor_from_sheet_block(top, sheet_name, first_cell_a1=a1)
                entry["vendor"] = vendor
                entry["vendor_detection_notes"] = vnotes

                if vendor is None:
                    entry["issues"] = ["無法由頂部區塊判定東吳／聲活／鉑霖；本分頁可能被略過。"]
                    sheets_report.append(entry)
                    continue

                hi = _find_cueapp_schedule_header_row(df)
                entry["header_anchor_row_0based"] = hi
                if hi is None:
                    entry["issues"] = ["找不到排程表頭列（頻道、秒數／規格、播出地區等線索）。"]
                    sheets_report.append(entry)
                    continue

                entry["header_field_map"] = map_cue_header_fields(df, hi, row_span=2)
                sec_col = _find_cueapp_sec_col(df, hi)
                entry["seconds_column_index"] = sec_col
                if sec_col is None:
                    entry["issues"] = ["找不到秒數／Size 欄位索引。"]
                    sheets_report.append(entry)
                    continue

                date_start_col = sec_col + 1
                if vendor == "dongwu":
                    hdr_join = _row_text_df(df, hi)
                    if "Station" in hdr_join and "Location" in hdr_join:
                        date_start_col = 7

                eff = 0
                for j in range(date_start_col, min(df.shape[1], date_start_col + 80)):
                    if _parse_cueapp_day_header_cell(df.iloc[hi, j]) is not None:
                        eff += 1
                    elif eff > 0:
                        break
                entry["date_start_column_index"] = date_start_col
                entry["day_columns_detected"] = eff

                body = analyze_cue_schedule_body_rows(df, vendor, hi, date_start_col, eff)
                entry["body_row_diagnostics"] = body
                n_data = sum(1 for x in body if x.get("status") == "data_candidate")
                n_skip = sum(1 for x in body if x.get("status") == "skipped")
                entry["body_summary"] = {"data_candidate_rows": n_data, "skipped_rows": n_skip, "other": len(body) - n_data - n_skip}

                units_here = [u for u in ad_units if u.get("source_sheet") == sheet_name]
                entry["ad_units_from_sheet"] = len(units_here)
                if n_data > 0 and len(units_here) == 0:
                    entry["warnings"] = [
                        "推斷有檔次資料列但未產生 ad_unit，可能為平台／日期／合併儲存格等進階問題。"
                    ]
            except Exception as e:
                entry["error"] = str(e)
                warnings.append(f"分頁「{sheet_name}」診斷失敗：{e}")
            sheets_report.append(entry)

        try:
            xls.close()
        except Exception:
            pass
    except Exception as e:
        warnings.append(f"產生逐頁診斷失敗：{e}")

    if not ad_units:
        issues.append("未產生任何 ad_unit；請檢視 workbook_scan 與 sheets[*].issues／body_row_diagnostics。")

    return {
        "ad_units": ad_units,
        "ad_unit_count": len(ad_units),
        "workbook_scan": workbook_scan,
        "sheets": sheets_report,
        "parse_diagnostics": parse_diagnostics,
        "issues": issues,
        "warnings": warnings,
    }


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


def parse_cue_excel_for_table1(file_content, order_info=None, cue_parse_diagnostics: list | None = None):
    result = []
    try:
        result = parse_cueapp_excel(file_content, diagnostics_out=cue_parse_diagnostics)
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
