# -*- coding: utf-8 -*-
"""共用工具：Excel 輸出、DataFrame 清理、樣式、日期正規化、秒數標籤。"""

import io
import logging
import os
import time
import numpy as np
import pandas as pd


_TIMING_ENABLED = os.environ.get("SEC_MANAGER_TIMING", "1") == "1"


def log_timing(step: str, elapsed_s: float, **meta) -> None:
    """
    寫入 Streamlit/console logs 用的計時資訊（預設啟用）。
    可用環境變數 `SEC_MANAGER_TIMING=0` 關閉。
    """
    if not _TIMING_ENABLED:
        return
    meta_items = [(k, v) for k, v in meta.items() if v is not None]
    meta_s = " ".join([f"{k}={v}" for k, v in meta_items])
    logger = logging.getLogger("secmanager.timing")
    logger.info(f"[timing] {step} took {elapsed_s:.3f}s{(' ' + meta_s) if meta_s else ''}")


def df_to_excel_bytes(df, sheet_name="Sheet1"):
    """將 DataFrame 轉為 Excel (.xlsx) 的 bytes。"""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    output.seek(0)
    return output.getvalue()


def sanitize_dataframe_for_display(df):
    """清理 DataFrame，將複雜類型轉為字串，以便 PyArrow 序列化（修復 st.dataframe 錯誤）。"""
    df = df.copy()
    for col in df.columns:
        if df[col].dtype == "object":
            def safe_convert(x):
                if x is None or (pd.notna(x) is False):
                    return ""
                try:
                    return str(x)
                except (TypeError, ValueError):
                    return ""
            df[col] = df[col].apply(safe_convert)
    return df


def styler_one_decimal(df):
    """數值欄位顯示最多小數一位，超過三位數加千分位；大表時回傳原 DataFrame 避免 Styler 卡頓。"""
    if df is None:
        return None
    if df.empty:
        return df.style
    num_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    try:
        if df.shape[0] * df.shape[1] > 120000:
            return df
    except Exception:
        pass
    if not num_cols:
        return df.style
    return df.style.format({c: "{:,.1f}" for c in num_cols})


def seconds_to_spot_label(seconds, sec_per_spot, short=False):
    """轉譯為「約 X 檔全省 15 秒」；short=True 為「約 X 檔(15秒)」。"""
    if sec_per_spot <= 0:
        return f"{int(seconds):,} 店秒"
    n = round(seconds / sec_per_spot)
    return f"約 {n} 檔(15秒)" if short else f"約 {n} 檔全省 15 秒"


def normalize_date(val):
    """將 2026/1/1、2026-01-01 等轉成 YYYY-MM-DD。"""
    if pd.isna(val) or val == "" or str(val).strip() == "nan":
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


# 秒數用途類型（總結表／訂單 CRUD 用）
SECONDS_USAGE_TYPES = ["銷售秒數", "交換秒數", "贈送秒數", "補檔秒數", "賀歲秒數", "公益秒數"]
SECONDS_TYPE_ALIASES = {
    "銷售": "銷售秒數", "交換": "交換秒數", "贈送": "贈送秒數",
    "補檔": "補檔秒數", "賀歲": "賀歲秒數", "公益": "公益秒數",
}


def normalize_seconds_type(val):
    """將秒數用途正規化為 SECONDS_USAGE_TYPES 其一。"""
    if not val or (isinstance(val, float) and pd.isna(val)):
        return "銷售秒數"
    s = str(val).strip()
    if s in SECONDS_USAGE_TYPES:
        return s
    return SECONDS_TYPE_ALIASES.get(s, "銷售秒數")
