# -*- coding: utf-8 -*-
"""
Google Sheet 後端：將系統資料同步至 Google 試算表，供免費版 Streamlit 重啟後還原。
可透過 .streamlit/secrets.toml 或環境變數設定；未來可改為公司用 Sheet。
"""
from __future__ import annotations

import io
import json
import math
import os
import hashlib
import re
from typing import Any
import pandas as pd


def _sheet_cell_json_safe(val: Any) -> Any:
    """Google Sheets API 以 JSON 傳值，不可含 float nan/inf。"""
    if val is None:
        return ""
    if isinstance(val, bool):
        return val
    if isinstance(val, str):
        return val
    try:
        if val is pd.NA:
            return ""
        if pd.isna(val):
            return ""
    except (TypeError, ValueError):
        pass
    try:
        x = float(val)
        if math.isnan(x) or math.isinf(x):
            return ""
        if x == int(x):
            return int(x)
        return x
    except (TypeError, ValueError, OverflowError):
        return str(val)


def _sanitize_sheet_matrix(values: list[list[Any]]) -> list[list[Any]]:
    return [[_sheet_cell_json_safe(c) for c in row] for row in values]

# 各工作表名稱（與 DB 表對應）
WS_ORDERS = "Orders"
WS_SEGMENTS = "Segments"
WS_PLATFORM_SETTINGS = "PlatformSettings"
WS_CAPACITY = "Capacity"
WS_PURCHASE = "Purchase"
WS_USERS = "Users"
WS_T1_TEMPLATE_ORDERS = "表1樣式_Orders"
WS_T1_TEMPLATE_SEGMENTS = "表1樣式_Segments"

ALL_WORKHEET_NAMES = [
    WS_ORDERS,
    WS_SEGMENTS,
    WS_PLATFORM_SETTINGS,
    WS_CAPACITY,
    WS_PURCHASE,
    WS_USERS,
    WS_T1_TEMPLATE_ORDERS,
    WS_T1_TEMPLATE_SEGMENTS,
]

TEMPLATE_SHEET_ID = "1x2cboM_xmB7nl9aA12O633BzmvPNyJnZoqPipOQhVY4"

# 試算表與檔案存取權限（與 stockanalysis 一致，避免權限不足）
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
]

# 供同步失敗時顯示 _client() 無法連線的具體原因
_last_client_error: str | None = None
_last_table_signatures: dict[str, str] = {}


def get_last_client_error() -> str | None:
    """回傳最近一次 _client() 失敗的原因（若無則 None）。"""
    return _last_client_error


def _get_sheet_config() -> dict[str, Any]:
    """從 Streamlit secrets 或環境變數取得設定。"""
    try:
        import streamlit as st
        secrets = getattr(st, "secrets", None)
        if not secrets:
            raise ValueError("no secrets")
        # 支援 google_sheet 或 Google_Sheet（Cloud 有時會改 key 名稱）
        gs = getattr(secrets, "google_sheet", None) or getattr(secrets, "Google_Sheet", None)
        if gs is None and hasattr(secrets, "get"):
            gs = secrets.get("google_sheet") or secrets.get("Google_Sheet")
        if gs is None:
            raise ValueError("no google_sheet section")
        # Cloud 的 secrets 可能是 AttributeDict 等，不一定是 dict，需相容 .get 與屬性
        def _get(obj: Any, *keys: str) -> Any:
            for k in keys:
                if obj is None:
                    return None
                if isinstance(obj, dict):
                    v = obj.get(k)
                else:
                    v = getattr(obj, k, None)
                if v is not None and v != "":
                    return v
            return None
        sid = _get(gs, "sheet_id", "sheet_id_") or os.environ.get("GOOGLE_SHEET_ID")
        if sid or os.environ.get("GOOGLE_SHEET_ID"):
            try:
                out = dict(gs)
            except (TypeError, ValueError):
                out = {k: _get(gs, k) for k in ("sheet_id", "sheet_id_", "enabled", "client_email", "private_key", "credentials", "credentials_json", "project_id", "private_key_id", "client_id")}
                out = {k: v for k, v in out.items() if v is not None}
            if not out.get("sheet_id") and os.environ.get("GOOGLE_SHEET_ID"):
                out["sheet_id"] = os.environ["GOOGLE_SHEET_ID"]
            elif sid and not out.get("sheet_id"):
                out["sheet_id"] = sid.strip() if isinstance(sid, str) else str(sid)
            return out
    except Exception:
        pass
    out = {}
    if os.environ.get("GOOGLE_SHEET_ID"):
        out["sheet_id"] = os.environ["GOOGLE_SHEET_ID"]
    if os.environ.get("GOOGLE_SHEET_CREDENTIALS"):
        try:
            out["credentials"] = json.loads(os.environ["GOOGLE_SHEET_CREDENTIALS"])
        except Exception:
            pass
    return out


def _get_credentials():
    """取得 Google API 憑證（服務帳戶）。"""
    try:
        import streamlit as st
        raw_gs = getattr(st.secrets, "google_sheet", None) or getattr(st.secrets, "Google_Sheet", None)
        if raw_gs is None and hasattr(st, "secrets") and st.secrets and hasattr(st.secrets, "get"):
            raw_gs = st.secrets.get("google_sheet") or st.secrets.get("Google_Sheet")
        if raw_gs is None:
            gs = {}
        else:
            try:
                gs = dict(raw_gs)
            except (TypeError, ValueError):
                gs = {k: getattr(raw_gs, k, None) for k in ("credentials", "credentials_json", "credentials_b64", "client_email", "private_key", "project_id", "private_key_id", "client_id") if getattr(raw_gs, k, None) is not None}
    except Exception:
        gs = {}
    cred_dict = gs.get("credentials")
    if isinstance(cred_dict, dict):
        from google.oauth2 import service_account
        return service_account.Credentials.from_service_account_info(cred_dict, scopes=SCOPES)
    raw = gs.get("credentials_json") or gs.get("credentials_b64") or os.environ.get("GOOGLE_SHEET_CREDENTIALS") or os.environ.get("GOOGLE_SHEET_CREDENTIALS_B64")
    if raw:
        if isinstance(raw, str):
            s = raw.strip()
            if s.startswith("{"):
                try:
                    cred_dict = json.loads(s)
                except json.JSONDecodeError:
                    return None
            else:
                try:
                    import base64
                    decoded = base64.b64decode(s).decode("utf-8")
                    cred_dict = json.loads(decoded)
                except Exception:
                    return None
        else:
            cred_dict = raw
        from google.oauth2 import service_account
        return service_account.Credentials.from_service_account_info(cred_dict, scopes=SCOPES)
    # 個別欄位（方便在 TOML 裡填；Cloud 多行 private_key 易出錯，建議改用 credentials_json）
    client_email = (gs.get("client_email") or os.environ.get("GOOGLE_SHEET_CLIENT_EMAIL") or "").strip()
    raw_key = gs.get("private_key") or os.environ.get("GOOGLE_SHEET_PRIVATE_KEY") or ""
    if isinstance(raw_key, str):
        private_key = raw_key.strip().replace("\\n", "\n")
    else:
        private_key = ""
    if client_email and private_key and "BEGIN PRIVATE KEY" in private_key:
        from google.oauth2 import service_account
        return service_account.Credentials.from_service_account_info({
            "type": "service_account",
            "project_id": gs.get("project_id") or "secmanager",
            "private_key_id": gs.get("private_key_id") or "",
            "private_key": private_key,
            "client_email": client_email,
            "client_id": gs.get("client_id") or "",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": "",
        }, scopes=SCOPES)
    return None


def is_sheets_enabled() -> bool:
    """是否已設定並啟用 Google Sheet 後端。"""
    return get_sheets_status()[0] == "ok"


def get_sheets_status() -> tuple[str, str | None]:
    """
    回傳 ("ok", None) 表示已啟用；否則 ("disabled", "原因")。
    供 UI 顯示未設定時的原因（例如：未填 sheet_id、憑證無法載入）。
    """
    config = _get_sheet_config()
    sheet_id = (config.get("sheet_id") or config.get("sheet_id_") or "").strip()
    if not sheet_id:
        return "disabled", "未填 sheet_id（請在 Secrets 設定 [google_sheet] 的 sheet_id）"
    if config.get("enabled") is False:
        return "disabled", "已關閉（enabled = false）"
    creds = _get_credentials()
    if creds is None:
        return "disabled", "憑證無法載入（請檢查 client_email / private_key，或改用 credentials_json 單行 JSON）"
    return "ok", None


def get_sheets_url() -> str | None:
    """回傳目前 DB 綁定的 Google Sheet 網址（未設定時回傳 None）。"""
    sid = _get_sheet_id()
    if not sid:
        return None
    return f"https://docs.google.com/spreadsheets/d/{sid}/edit#gid=0"


def get_effective_sheet_id() -> str | None:
    """回傳目前設定實際使用的 sheet_id（供 UI 診斷）。"""
    return _get_sheet_id()


def _get_sheet_id() -> str | None:
    config = _get_sheet_config()
    sid = config.get("sheet_id") or config.get("sheet_id_")
    return str(sid).strip() if sid else None


def _client():
    """取得 gspread 客戶端，失敗回傳 None，並設定 _last_client_error 供 UI 顯示原因。"""
    global _last_client_error
    _last_client_error = None
    if not is_sheets_enabled():
        _last_client_error = "Google Sheet 未啟用或設定不完整"
        return None
    creds = _get_credentials()
    sheet_id = _get_sheet_id()
    if not creds:
        _last_client_error = "無法載入憑證（請檢查 Secrets 的 client_email / private_key 或 credentials_json）"
        return None
    if not sheet_id:
        _last_client_error = "未填 sheet_id（請在 Secrets 的 [google_sheet] 填寫 sheet_id）"
        return None
    try:
        import gspread
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(sheet_id)
        return sh
    except Exception as e:
        err = str(e).strip()
        if "403" in err or "permission" in err.lower() or "權限" in err or "does not have permission" in err.lower():
            _last_client_error = "試算表未分享給服務帳戶：請在 Google 試算表按「共用」，加入 Secrets 裡的 client_email（例如 xxx@xxx.iam.gserviceaccount.com）為編輯者。"
        elif "404" in err or "not found" in err.lower():
            _last_client_error = "找不到試算表：請確認 sheet_id 是否正確，且試算表已分享給服務帳戶。"
        else:
            _last_client_error = err or "無法連線至 Google Sheet"
        return None


def _ensure_worksheets(sh) -> bool:
    """確保試算表內有所有需要的工作表，沒有則建立。"""
    if sh is None:
        return False
    try:
        existing = [ws.title for ws in sh.worksheets()]
        for name in ALL_WORKHEET_NAMES:
            if name not in existing:
                sh.add_worksheet(title=name, rows=1000, cols=30)
        return True
    except Exception:
        return False


def _df_to_values(df: pd.DataFrame) -> list[list[Any]]:
    """DataFrame 轉成 gspread 可寫入的 [header, row1, row2, ...]。"""
    if df is None or df.empty:
        return []
    df = df.fillna("")
    return [df.columns.tolist()] + df.astype(str).values.tolist()


def _update_worksheet_with_clear_when_empty(ws, df: pd.DataFrame, fallback_header: list[str]) -> None:
    """
    寫入工作表；若 df 為空，仍會把既有資料列覆蓋清空，避免殘留舊資料。
    """
    if df is None or df.empty:
        header = list(getattr(df, "columns", [])) if df is not None else list(fallback_header)
        if not header:
            header = list(fallback_header)
        # 清空時避免先讀全表（get_all_values）以降低 Read quota 壓力。
        ws.clear()
        ws.update([header] if header else [[""]], value_input_option="USER_ENTERED")
        return

    ws.update(_df_to_values(df), value_input_option="USER_ENTERED")


def _records_to_df(records: list[dict]) -> pd.DataFrame:
    if not records:
        return pd.DataFrame()
    return pd.DataFrame(records)


def _extract_template_headers_from_google_sheet() -> list[str]:
    """
    從指定模板 Google Sheet 擷取「表頭列」。
    目標：確保輸出欄位順序/名稱 100% 對齊模板。
    """
    url = f"https://docs.google.com/spreadsheets/d/{TEMPLATE_SHEET_ID}/export?format=csv&gid=0"
    try:
        df_raw = pd.read_csv(url, header=None, dtype=str, keep_default_na=False)
    except Exception:
        return []
    if df_raw is None or df_raw.empty:
        return []
    header_row = None
    for i in range(min(12, len(df_raw))):
        row_vals = [str(v).strip() for v in df_raw.iloc[i].tolist()]
        row_txt = " ".join(row_vals)
        if "平台" in row_txt and "起始日" in row_txt and "終止日" in row_txt and "每天總檔次" in row_txt:
            header_row = i
            break
    if header_row is None:
        return []
    headers = [str(v).strip() for v in df_raw.iloc[header_row].tolist()]
    headers = [h if h else f"欄位_{idx+1}" for idx, h in enumerate(headers)]
    return headers


def _extract_template_layout_from_google_sheet() -> tuple[list[list[str]], list[str]]:
    """
    擷取模板的三列抬頭：
    - 月份分區列
    - 月日列
    - 欄位列（含星期欄）
    """
    url = f"https://docs.google.com/spreadsheets/d/{TEMPLATE_SHEET_ID}/export?format=csv&gid=0"
    try:
        df_raw = pd.read_csv(url, header=None, dtype=str, keep_default_na=False)
    except Exception:
        return [], []
    if df_raw is None or df_raw.empty:
        return [], []

    header_row = None
    for i in range(min(12, len(df_raw))):
        row_vals = [str(v).strip() for v in df_raw.iloc[i].tolist()]
        row_txt = " ".join(row_vals)
        if "平台" in row_txt and "起始日" in row_txt and "終止日" in row_txt and "每天總檔次" in row_txt:
            header_row = i
            break
    if header_row is None:
        return [], []

    row_month = df_raw.iloc[max(0, header_row - 2)].astype(str).tolist()
    row_day = df_raw.iloc[max(0, header_row - 1)].astype(str).tolist()
    row_header = [str(v).strip() for v in df_raw.iloc[header_row].tolist()]
    if not row_header:
        return [], []
    row_header = [h if h else f"欄位_{idx+1}" for idx, h in enumerate(row_header)]
    ncols = len(row_header)
    row_month = (row_month + [""] * ncols)[:ncols]
    row_day = (row_day + [""] * ncols)[:ncols]
    return [row_month, row_day, row_header], row_header


def _days_between(start_date: Any, end_date: Any) -> int | None:
    try:
        s = pd.to_datetime(start_date, errors="coerce")
        e = pd.to_datetime(end_date, errors="coerce")
        if pd.isna(s) or pd.isna(e):
            return None
        return int((e - s).days) + 1
    except Exception:
        return None


def _build_template_sheet_df(df_src: pd.DataFrame, headers: list[str], source_type: str) -> pd.DataFrame:
    if df_src is None:
        df_src = pd.DataFrame()
    if not headers:
        return pd.DataFrame()
    out_rows: list[dict[str, Any]] = []
    for _, r in df_src.iterrows():
        row = {h: "" for h in headers}
        platform = r.get("platform", "")
        company = r.get("company", "")
        sales = r.get("sales", "")
        client = r.get("client", "")
        product = r.get("product", "")
        start_date = r.get("start_date", "")
        end_date = r.get("end_date", "")
        seconds = r.get("seconds", "")
        spots = r.get("spots", "")
        amount_net = r.get("amount_net", "")
        updated_at = r.get("updated_at", "")
        contract_id = r.get("contract_id", "")
        seconds_type = r.get("seconds_type", "")
        duration_days = _days_between(start_date, end_date)
        total_spots = ""
        total_seconds = ""
        if source_type == "segments":
            total_spots = r.get("total_spots", "")
            total_seconds = r.get("total_store_seconds", "")
        row.update(
            {
                "平台": platform,
                "公司": company,
                "業務": sales,
                "秒數用途": seconds_type,
                "提交日": updated_at,
                "HYUNDAI_CUSTIN": client,
                "秒數": seconds,
                "素材": product,
                "起始日": start_date,
                "終止日": end_date,
                "走期天數": duration_days if duration_days is not None else "",
                "每天總檔次": spots,
                "委刋總檔數": total_spots,
                "總秒數": total_seconds,
                "合約編號": contract_id,
                "實收金額": amount_net,
                "除佣實收": amount_net,
            }
        )
        out_rows.append(row)
    return pd.DataFrame(out_rows, columns=headers)


def _build_template_sheet_rows(df_src: pd.DataFrame, headers: list[str], source_type: str) -> list[list[Any]]:
    """
    依模板欄位建立資料列（不含抬頭）。
    """
    if df_src is None:
        df_src = pd.DataFrame()
    if not headers:
        return []
    idx = {h: i for i, h in enumerate(headers)}

    def put(row_vals: list[Any], key: str, val: Any) -> None:
        if key in idx:
            row_vals[idx[key]] = _sheet_cell_json_safe(val)

    rows_out: list[list[Any]] = []
    for _, r in df_src.iterrows():
        row_vals: list[Any] = [""] * len(headers)
        platform = r.get("platform", "")
        company = r.get("company", "")
        sales = r.get("sales", "")
        client = r.get("client", "")
        product = r.get("product", "")
        start_date = r.get("start_date", "")
        end_date = r.get("end_date", "")
        seconds = r.get("seconds", "")
        spots = r.get("spots", "")
        amount_net = r.get("amount_net", "")
        updated_at = r.get("updated_at", "")
        contract_id = r.get("contract_id", "")
        seconds_type = r.get("seconds_type", "")
        duration_days = _days_between(start_date, end_date)
        total_spots = ""
        total_seconds = ""
        store_count = ""
        if source_type == "segments":
            total_spots = r.get("total_spots", "")
            total_seconds = r.get("total_store_seconds", "")
            store_count = r.get("store_count", "")

        put(row_vals, "平台", platform)
        put(row_vals, "公司", company)
        put(row_vals, "業務", sales)
        put(row_vals, "秒數用途", seconds_type)
        put(row_vals, "提交日", updated_at)
        put(row_vals, "HYUNDAI_CUSTIN", client)
        put(row_vals, "秒數", seconds)
        put(row_vals, "素材", product)
        put(row_vals, "起始日", start_date)
        put(row_vals, "終止日", end_date)
        put(row_vals, "走期天數", duration_days if duration_days is not None else "")
        put(row_vals, "每天總檔次", spots)
        put(row_vals, "委刋總檔數", total_spots)
        put(row_vals, "委刊總檔數", total_spots)
        put(row_vals, "總秒數", total_seconds)
        put(row_vals, "店數", store_count)
        put(row_vals, "合約編號", contract_id)
        put(row_vals, "實收金額", amount_net)
        put(row_vals, "除佣實收", amount_net)
        rows_out.append(row_vals)
    return rows_out


def _write_template_style_tabs(
    *,
    sh,
    df_orders: pd.DataFrame,
    df_segments: pd.DataFrame,
) -> str | None:
    layout_rows, headers = _extract_template_layout_from_google_sheet()
    if not headers or len(layout_rows) < 3:
        return "無法從模板試算表抓到表頭，未建立表1樣式分頁"

    ws_o = sh.worksheet(WS_T1_TEMPLATE_ORDERS)
    ws_s = sh.worksheet(WS_T1_TEMPLATE_SEGMENTS)

    rows_o = _build_template_sheet_rows(df_orders, headers, source_type="orders")
    rows_s = _build_template_sheet_rows(df_segments, headers, source_type="segments")
    values_o = _sanitize_sheet_matrix(layout_rows + rows_o)
    values_s = _sanitize_sheet_matrix(layout_rows + rows_s)

    ws_o.clear()
    ws_s.clear()
    ws_o.update(values_o, value_input_option="USER_ENTERED")
    ws_s.update(values_s, value_input_option="USER_ENTERED")
    return None


def _table_signature(df: pd.DataFrame) -> str:
    """
    建立 DataFrame 簽章，用於判斷資料是否變更。
    目標是避免每次都把整張工作表重寫，降低同步延遲。
    """
    try:
        if df is None or df.empty:
            return "empty"
        # 轉成穩定字串後做 hash（4000 列等級開銷遠低於網路全量同步）
        payload = df.fillna("").astype(str).to_csv(index=False).encode("utf-8")
        return hashlib.md5(payload).hexdigest()
    except Exception:
        # fallback：至少保留形狀資訊
        try:
            return f"shape:{df.shape[0]}x{df.shape[1]}"
        except Exception:
            return "unknown"


def load_orders_from_sheets() -> pd.DataFrame:
    try:
        sh = _client()
        if not sh:
            return pd.DataFrame()
        ws = sh.worksheet(WS_ORDERS)
        rec = ws.get_all_records()
        return _records_to_df(rec)
    except Exception:
        return pd.DataFrame()


def load_segments_from_sheets() -> pd.DataFrame:
    try:
        sh = _client()
        if not sh:
            return pd.DataFrame()
        ws = sh.worksheet(WS_SEGMENTS)
        rec = ws.get_all_records()
        return _records_to_df(rec)
    except Exception:
        return pd.DataFrame()


def load_platform_settings_from_sheets() -> pd.DataFrame:
    try:
        sh = _client()
        if not sh:
            return pd.DataFrame()
        ws = sh.worksheet(WS_PLATFORM_SETTINGS)
        rec = ws.get_all_records()
        return _records_to_df(rec)
    except Exception:
        return pd.DataFrame()


def load_capacity_from_sheets() -> pd.DataFrame:
    try:
        sh = _client()
        if not sh:
            return pd.DataFrame()
        ws = sh.worksheet(WS_CAPACITY)
        rec = ws.get_all_records()
        return _records_to_df(rec)
    except Exception:
        return pd.DataFrame()


def load_purchase_from_sheets() -> pd.DataFrame:
    try:
        sh = _client()
        if not sh:
            return pd.DataFrame()
        ws = sh.worksheet(WS_PURCHASE)
        rec = ws.get_all_records()
        return _records_to_df(rec)
    except Exception:
        return pd.DataFrame()


def load_users_from_sheets() -> pd.DataFrame:
    try:
        sh = _client()
        if not sh:
            return pd.DataFrame()
        ws = sh.worksheet(WS_USERS)
        rec = ws.get_all_records()
        return _records_to_df(rec)
    except Exception:
        return pd.DataFrame()


def write_orders_to_sheets(df: pd.DataFrame) -> str | None:
    """寫入 orders 到 Google Sheet，成功回傳 None，失敗回傳錯誤訊息。"""
    try:
        sh = _client()
        if not sh:
            return "未設定或未啟用 Google Sheet"
        _ensure_worksheets(sh)
        ws = sh.worksheet(WS_ORDERS)
        # 嚴謹處理：有些權限/保護情況下 `ws.clear()` 會失敗。
        # 這裡改用「覆蓋式清空」：讀出目前值的形狀，再用空字串覆蓋整個區塊。
        if df is None or df.empty:
            header = (list(getattr(df, "columns", [])) if df is not None else [])
            existing = ws.get_all_values() or []
            if not existing:
                ws.update([header] if header else [[""]], value_input_option="USER_ENTERED")
                return None
            ncols = max(len(existing[0]), len(header))
            header_padded = header + [""] * (ncols - len(header))
            blank_row = [""] * ncols
            new_values = [header_padded] + [blank_row] * (max(0, len(existing) - 1))
            ws.update(new_values, value_input_option="USER_ENTERED")
            return None

        vals = _df_to_values(df)
        ws.update(vals, value_input_option="USER_ENTERED")
        return None
    except Exception as e:
        return str(e)


def write_segments_to_sheets(df: pd.DataFrame) -> str | None:
    try:
        sh = _client()
        if not sh:
            return "未設定或未啟用 Google Sheet"
        _ensure_worksheets(sh)
        ws = sh.worksheet(WS_SEGMENTS)
        # 同 orders：改用覆蓋式清空，避免 `ws.clear()` 失敗造成不同步。
        if df is None or df.empty:
            header = (list(getattr(df, "columns", [])) if df is not None else [])
            existing = ws.get_all_values() or []
            if not existing:
                ws.update([header] if header else [[""]], value_input_option="USER_ENTERED")
                return None
            ncols = max(len(existing[0]), len(header))
            header_padded = header + [""] * (ncols - len(header))
            blank_row = [""] * ncols
            new_values = [header_padded] + [blank_row] * (max(0, len(existing) - 1))
            ws.update(new_values, value_input_option="USER_ENTERED")
            return None

        vals = _df_to_values(df)
        ws.update(vals, value_input_option="USER_ENTERED")
        return None
    except Exception as e:
        return str(e)


def _col_to_a1(col_idx_1based: int) -> str:
    """1-based column index -> A1 column label."""
    n = int(col_idx_1based)
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def update_segments_seconds_type_rows(
    updates: list[tuple[str, str, str]] | list[tuple[str, str]],
) -> list[str]:
    """
    精準更新 Segments 工作表中指定 segment_id 的 seconds_type（與 updated_at）。
    updates:
      - (segment_id, seconds_type)
      - (segment_id, seconds_type, updated_at)
    回傳錯誤列表；空表示成功。
    """
    errors: list[str] = []
    if not updates:
        return errors
    try:
        sh = _client()
        if not sh:
            return ["未設定或未啟用 Google Sheet"]
        _ensure_worksheets(sh)
        ws = sh.worksheet(WS_SEGMENTS)
        values = ws.get_all_values() or []
        if not values:
            return ["Segments 工作表為空，無法逐列更新"]
        header = [str(h).strip() for h in values[0]]
        if "segment_id" not in header or "seconds_type" not in header:
            return ["Segments 工作表缺少必要欄位（segment_id / seconds_type）"]

        col_seg = header.index("segment_id") + 1
        col_stype = header.index("seconds_type") + 1
        col_updated = (header.index("updated_at") + 1) if "updated_at" in header else None

        row_by_seg: dict[str, int] = {}
        for ridx, row in enumerate(values[1:], start=2):
            seg_val = str(row[col_seg - 1]).strip() if len(row) >= col_seg else ""
            if seg_val and seg_val not in row_by_seg:
                row_by_seg[seg_val] = ridx

        payload = []
        missing_ids = []
        for item in updates:
            if len(item) == 2:
                seg_id, new_stype = item  # type: ignore[misc]
                upd_ts = ""
            else:
                seg_id, new_stype, upd_ts = item  # type: ignore[misc]
            seg_id_s = str(seg_id).strip()
            if not seg_id_s:
                continue
            row_idx = row_by_seg.get(seg_id_s)
            if row_idx is None:
                missing_ids.append(seg_id_s)
                continue
            stype_a1 = f"{_col_to_a1(col_stype)}{row_idx}"
            payload.append({"range": stype_a1, "values": [[str(new_stype or "")]]})
            if col_updated and str(upd_ts or "").strip():
                upd_a1 = f"{_col_to_a1(col_updated)}{row_idx}"
                payload.append({"range": upd_a1, "values": [[str(upd_ts)]]})

        if missing_ids:
            errors.append(f"Segments 找不到 {len(missing_ids)} 筆 segment_id（前5筆）：{', '.join(missing_ids[:5])}")
        if payload:
            ws.batch_update(payload, value_input_option="USER_ENTERED")
        elif not errors:
            errors.append("沒有可更新的 segment_id")
    except Exception as e:
        errors.append(str(e))
    return errors


def _norm_date_text(v: str) -> str:
    try:
        dt = pd.to_datetime(str(v).strip(), errors="coerce")
        if pd.isna(dt):
            return ""
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return ""


def update_source_sheet_seconds_type(
    *,
    source_sheet_id: str,
    updates: list[dict[str, Any]],
) -> list[str]:
    """
    回寫「匯入來源表」的秒數用途。
    updates 每筆需包含：
      platform, company, sales, client, product, start_date, end_date, seconds, spots, seconds_type
    可選：
      region, contract_id（若來源表有這兩欄會一併納入精準匹配）
    回傳錯誤列表；空表示成功。
    """
    errors: list[str] = []
    if not source_sheet_id or not updates:
        return errors
    try:
        sh = _client()
        if not sh:
            return ["未設定或未啟用 Google Sheet"]
        ws = sh.open_by_key(source_sheet_id).sheet1
        values = ws.get_all_values() or []
        if len(values) < 2:
            return ["來源表內容不足，無法回寫"]

        # 前 10 列找表頭（沿用匯入邏輯）
        header_row_idx = None
        for i in range(min(10, len(values))):
            row_text = " ".join([str(x) for x in values[i]])
            if "平台" in row_text and ("起始日" in row_text or "終止日" in row_text):
                header_row_idx = i
                break
        if header_row_idx is None:
            return ["來源表找不到表頭列（需含：平台、起始日/終止日）"]

        header = [str(h).strip() for h in values[header_row_idx]]
        req_cols = {
            "platform": "平台",
            "company": "公司",
            "sales": "業務",
            "client": "HYUNDAI_CUSTIN",
            "product": "素材",
            "start_date": "起始日",
            "end_date": "終止日",
            "seconds": "秒數",
            "spots": "每天總檔次",
            "seconds_type": "秒數用途",
        }
        col_idx = {}
        for k, h in req_cols.items():
            if h not in header:
                if k == "spots" and "委刊總檔數" in header:
                    col_idx[k] = header.index("委刊總檔數")
                    continue
                return [f"來源表缺少欄位：{h}"]
            col_idx[k] = header.index(h)
        # 可選精準匹配欄位（存在才使用）
        opt_col_region = header.index("區域") if "區域" in header else None
        opt_col_contract = header.index("合約編號") if "合約編號" in header else None

        # 建立待更新鍵值（同一鍵可對多列，全部更新）
        update_map: dict[tuple, str] = {}
        for u in updates:
            key = (
                str(u.get("platform", "")).strip(),
                str(u.get("company", "")).strip(),
                str(u.get("sales", "")).strip(),
                str(u.get("client", "")).strip(),
                str(u.get("product", "")).strip(),
                _norm_date_text(u.get("start_date", "")),
                _norm_date_text(u.get("end_date", "")),
                str(int(float(u.get("seconds", 0) or 0))),
                str(int(float(u.get("spots", 0) or 0))),
                (str(u.get("region", "")).strip() if opt_col_region is not None else ""),
                (str(u.get("contract_id", "")).strip() if opt_col_contract is not None else ""),
            )
            update_map[key] = str(u.get("seconds_type", "") or "")

        payload = []
        matched = 0
        for ridx in range(header_row_idx + 1, len(values)):
            row = values[ridx]
            key = (
                str(row[col_idx["platform"]]).strip() if len(row) > col_idx["platform"] else "",
                str(row[col_idx["company"]]).strip() if len(row) > col_idx["company"] else "",
                str(row[col_idx["sales"]]).strip() if len(row) > col_idx["sales"] else "",
                str(row[col_idx["client"]]).strip() if len(row) > col_idx["client"] else "",
                str(row[col_idx["product"]]).strip() if len(row) > col_idx["product"] else "",
                _norm_date_text(row[col_idx["start_date"]]) if len(row) > col_idx["start_date"] else "",
                _norm_date_text(row[col_idx["end_date"]]) if len(row) > col_idx["end_date"] else "",
                str(int(float((row[col_idx["seconds"]] if len(row) > col_idx["seconds"] else "0") or 0))),
                str(int(float((row[col_idx["spots"]] if len(row) > col_idx["spots"] else "0") or 0))),
                (
                    str(row[opt_col_region]).strip()
                    if opt_col_region is not None and len(row) > opt_col_region
                    else ""
                ),
                (
                    str(row[opt_col_contract]).strip()
                    if opt_col_contract is not None and len(row) > opt_col_contract
                    else ""
                ),
            )
            if key in update_map:
                a1 = f"{_col_to_a1(col_idx['seconds_type'] + 1)}{ridx + 1}"
                payload.append({"range": a1, "values": [[update_map[key]]]})
                matched += 1

        if payload:
            ws.batch_update(payload, value_input_option="USER_ENTERED")
        else:
            errors.append("來源表找不到可匹配列（未回寫任何秒數用途）")
        if matched < len(update_map):
            errors.append(f"來源表僅匹配到 {matched} 列，預期至少 {len(update_map)} 列。")
    except Exception as e:
        errors.append(str(e))
    return errors


def write_platform_settings_to_sheets(df: pd.DataFrame) -> str | None:
    try:
        sh = _client()
        if not sh:
            return "未設定或未啟用 Google Sheet"
        _ensure_worksheets(sh)
        ws = sh.worksheet(WS_PLATFORM_SETTINGS)
        _update_worksheet_with_clear_when_empty(ws, df, ["platform", "store_count", "daily_hours"])
        return None
    except Exception as e:
        return str(e)


def write_capacity_to_sheets(df: pd.DataFrame) -> str | None:
    try:
        sh = _client()
        if not sh:
            return "未設定或未啟用 Google Sheet"
        _ensure_worksheets(sh)
        ws = sh.worksheet(WS_CAPACITY)
        _update_worksheet_with_clear_when_empty(
            ws,
            df,
            ["media_platform", "year", "month", "daily_available_seconds"],
        )
        return None
    except Exception as e:
        return str(e)


def write_purchase_to_sheets(df: pd.DataFrame) -> str | None:
    try:
        sh = _client()
        if not sh:
            return "未設定或未啟用 Google Sheet"
        _ensure_worksheets(sh)
        ws = sh.worksheet(WS_PURCHASE)
        _update_worksheet_with_clear_when_empty(
            ws,
            df,
            ["media_platform", "year", "month", "purchased_seconds", "purchase_price"],
        )
        return None
    except Exception as e:
        return str(e)


def write_users_to_sheets(df: pd.DataFrame) -> str | None:
    try:
        sh = _client()
        if not sh:
            return "未設定或未啟用 Google Sheet"
        _ensure_worksheets(sh)
        ws = sh.worksheet(WS_USERS)
        _update_worksheet_with_clear_when_empty(
            ws,
            df,
            ["id", "username", "password_hash", "role", "created_at"],
        )
        return None
    except Exception as e:
        return str(e)


def sync_db_to_sheets(
    get_db_connection,
    only_tables: list[str] | None = None,
    skip_if_unchanged: bool = True,
) -> list[str]:
    """
    將目前 SQLite 內所有表同步到 Google Sheet。
    使用 get_db_connection() 取得連線並讀取各表。
    回傳錯誤列表，空表示全部成功。
    """
    errors = []
    conn = get_db_connection()
    try:
        table_jobs = [
            (WS_ORDERS, lambda: pd.read_sql("SELECT * FROM orders", conn), write_orders_to_sheets),
            (WS_SEGMENTS, lambda: pd.read_sql("SELECT * FROM ad_flight_segments", conn), write_segments_to_sheets),
            (WS_PLATFORM_SETTINGS, lambda: pd.read_sql("SELECT * FROM platform_settings", conn), write_platform_settings_to_sheets),
            (WS_CAPACITY, lambda: pd.read_sql("SELECT * FROM platform_monthly_capacity", conn), write_capacity_to_sheets),
            (WS_PURCHASE, lambda: pd.read_sql("SELECT * FROM platform_monthly_purchase", conn), write_purchase_to_sheets),
            (WS_USERS, lambda: pd.read_sql("SELECT id, username, password_hash, role, created_at FROM users", conn), write_users_to_sheets),
        ]
        latest_df_orders = pd.DataFrame()
        latest_df_segments = pd.DataFrame()
        allow = set(only_tables) if only_tables else None
        for name, loader, writer in table_jobs:
            if allow is not None and name not in allow:
                continue
            try:
                df = loader()
                if name == WS_ORDERS:
                    latest_df_orders = df.copy()
                elif name == WS_SEGMENTS:
                    latest_df_segments = df.copy()
                if skip_if_unchanged:
                    sig = _table_signature(df)
                    old = _last_table_signatures.get(name)
                    if old == sig:
                        continue
                err = writer(df)
                if err:
                    errors.append(f"{name}: {err}")
                else:
                    if skip_if_unchanged:
                        _last_table_signatures[name] = _table_signature(df)
            except Exception as e:
                errors.append(f"{name}: {e}")

        # 額外輸出兩個「表1樣式」分頁（欄位對齊指定模板）。
        # 只要本次有同步業務資料（Orders/Segments）就嘗試建立。
        if allow is None or WS_ORDERS in allow or WS_SEGMENTS in allow:
            try:
                sh = _client()
                if sh:
                    _ensure_worksheets(sh)
                    err = _write_template_style_tabs(
                        sh=sh,
                        df_orders=latest_df_orders,
                        df_segments=latest_df_segments,
                    )
                    if err:
                        errors.append(f"表1樣式分頁: {err}")
                else:
                    errors.append("表1樣式分頁: 未設定或未啟用 Google Sheet")
            except Exception as e:
                errors.append(f"表1樣式分頁: {e}")
    finally:
        conn.close()
    # 若有錯誤且 _client() 有留下具體原因，放在第一則讓使用者看到
    reason = get_last_client_error()
    if errors and reason:
        errors.insert(0, reason)
    return errors


def clear_business_tables_in_sheets(*, keep_users: bool = True, verify_after_clear: bool = False) -> list[str]:
    """
    直接清空 Google Sheet 的業務資料分頁（不依賴 DB 內容）。
    預設保留 Users 分頁不動。
    """
    errors: list[str] = []
    table_jobs: list[tuple[str, list[str], Any]] = [
        (
            WS_ORDERS,
            [
                "id",
                "platform",
                "client",
                "product",
                "sales",
                "company",
                "start_date",
                "end_date",
                "seconds",
                "spots",
                "amount_net",
                "updated_at",
                "contract_id",
                "seconds_type",
                "project_amount_net",
                "split_amount",
                "region",
            ],
            write_orders_to_sheets,
        ),
        (
            WS_SEGMENTS,
            [
                "segment_id",
                "source_order_id",
                "platform",
                "channel",
                "region",
                "media_platform",
                "company",
                "sales",
                "client",
                "product",
                "seconds",
                "spots",
                "start_date",
                "end_date",
                "duration_days",
                "store_count",
                "total_spots",
                "total_store_seconds",
                "seconds_type",
                "created_at",
                "updated_at",
            ],
            write_segments_to_sheets,
        ),
        (WS_PLATFORM_SETTINGS, ["platform", "store_count", "daily_hours"], write_platform_settings_to_sheets),
        (WS_CAPACITY, ["media_platform", "year", "month", "daily_available_seconds"], write_capacity_to_sheets),
        (WS_PURCHASE, ["media_platform", "year", "month", "purchased_seconds", "purchase_price"], write_purchase_to_sheets),
    ]
    if not keep_users:
        table_jobs.append((WS_USERS, ["id", "username", "password_hash", "role", "created_at"], write_users_to_sheets))

    for name, cols, writer in table_jobs:
        try:
            err = writer(pd.DataFrame(columns=cols))
            if err:
                errors.append(f"{name}: {err}")
        except Exception as e:
            errors.append(f"{name}: {e}")

    # 可選回讀驗證（預設關閉，避免觸發 Read requests quota）
    if verify_after_clear:
        try:
            sh = _client()
            if not sh:
                errors.append("驗證失敗：無法連線 Google Sheet")
            else:
                for name, _, _ in table_jobs:
                    try:
                        ws = sh.worksheet(name)
                        values = ws.get_all_values() or []
                        body_rows = values[1:] if len(values) > 1 else []
                        has_non_empty = any(any(str(cell).strip() for cell in row) for row in body_rows)
                        if has_non_empty:
                            errors.append(f"{name}: 驗證未通過（仍有資料列）")
                    except Exception as e:
                        errors.append(f"{name}: 驗證讀取失敗: {e}")
        except Exception as e:
            errors.append(f"驗證例外: {e}")

    reason = get_last_client_error()
    if errors and reason:
        errors.insert(0, reason)
    return errors


def clear_business_tables_in_sheets_with_report(
    *,
    keep_users: bool = True,
    verify_after_clear: bool = False,
) -> tuple[list[str], list[str]]:
    """
    清空業務分頁並回傳詳細步驟訊息（供 UI 顯示進度/報告）。
    """
    errors: list[str] = []
    reports: list[str] = []
    table_jobs: list[tuple[str, list[str], Any]] = [
        (
            WS_ORDERS,
            [
                "id",
                "platform",
                "client",
                "product",
                "sales",
                "company",
                "start_date",
                "end_date",
                "seconds",
                "spots",
                "amount_net",
                "updated_at",
                "contract_id",
                "seconds_type",
                "project_amount_net",
                "split_amount",
                "region",
            ],
            write_orders_to_sheets,
        ),
        (
            WS_SEGMENTS,
            [
                "segment_id",
                "source_order_id",
                "platform",
                "channel",
                "region",
                "media_platform",
                "company",
                "sales",
                "client",
                "product",
                "seconds",
                "spots",
                "start_date",
                "end_date",
                "duration_days",
                "store_count",
                "total_spots",
                "total_store_seconds",
                "seconds_type",
                "created_at",
                "updated_at",
            ],
            write_segments_to_sheets,
        ),
        (WS_PLATFORM_SETTINGS, ["platform", "store_count", "daily_hours"], write_platform_settings_to_sheets),
        (WS_CAPACITY, ["media_platform", "year", "month", "daily_available_seconds"], write_capacity_to_sheets),
        (WS_PURCHASE, ["media_platform", "year", "month", "purchased_seconds", "purchase_price"], write_purchase_to_sheets),
    ]
    if not keep_users:
        table_jobs.append((WS_USERS, ["id", "username", "password_hash", "role", "created_at"], write_users_to_sheets))

    for name, cols, writer in table_jobs:
        reports.append(f"開始清空 `{name}`")
        try:
            err = writer(pd.DataFrame(columns=cols))
            if err:
                msg = f"{name}: {err}"
                errors.append(msg)
                reports.append(f"失敗：{msg}")
            else:
                reports.append(f"完成：`{name}`")
        except Exception as e:
            msg = f"{name}: {e}"
            errors.append(msg)
            reports.append(f"例外：{msg}")

    # 預設做輕量驗證：只讀各表第 2 列（資料首列）是否為空，避免高配額讀取。
    if verify_after_clear:
        reports.append("開始回讀驗證（檢查第2列）")
        try:
            sh = _client()
            if not sh:
                msg = "驗證失敗：無法連線 Google Sheet"
                errors.append(msg)
                reports.append(msg)
            else:
                for name, cols, _ in table_jobs:
                    try:
                        ws = sh.worksheet(name)
                        end_col = _col_to_a1(max(1, len(cols)))
                        row2 = ws.get(f"A2:{end_col}2") or []
                        has_non_empty = any(any(str(cell).strip() for cell in row) for row in row2)
                        if has_non_empty:
                            msg = f"{name}: 驗證未通過（第2列仍有資料）"
                            errors.append(msg)
                            reports.append(msg)
                        else:
                            reports.append(f"驗證通過：`{name}`")
                    except Exception as e:
                        msg = f"{name}: 驗證讀取失敗: {e}"
                        errors.append(msg)
                        reports.append(msg)
        except Exception as e:
            msg = f"驗證例外: {e}"
            errors.append(msg)
            reports.append(msg)

    reason = get_last_client_error()
    if errors and reason:
        errors.insert(0, reason)
        reports.insert(0, f"客戶端錯誤：{reason}")
    return errors, reports


def run_sheets_healthcheck() -> tuple[bool, str]:
    """
    Google Sheet 連線與寫入健康檢查。
    會在 _HealthCheck 工作表寫入/回讀 A1，成功回傳 (True, message)。
    """
    try:
        sh = _client()
        if not sh:
            return False, get_last_client_error() or "無法連線 Google Sheet"
        try:
            ws = sh.worksheet("_HealthCheck")
        except Exception:
            ws = sh.add_worksheet(title="_HealthCheck", rows=20, cols=5)
        payload = f"ok:{pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}"
        ws.update("A1", [[payload]], value_input_option="USER_ENTERED")
        read_back = ws.acell("A1").value
        if str(read_back or "").strip() != payload:
            return False, f"回讀不一致：寫入={payload}，讀取={read_back}"
        return True, f"健康檢查成功（A1={payload}）"
    except Exception as e:
        return False, str(e)


def load_all_from_sheets_into_db(get_db_connection, init_db) -> list[str]:
    """
    從 Google Sheet 讀取所有表並寫入 SQLite（覆蓋本地）。
    先呼叫 init_db() 確保表存在，再依序清空並寫入。
    回傳錯誤列表。
    """
    if not is_sheets_enabled():
        return ["未啟用或未設定 Google Sheet"]
    init_db()
    errors = []
    conn = get_db_connection()

    def run_sql(*args):
        c = conn.cursor()
        if len(args) == 1:
            c.execute(args[0])
        else:
            c.execute(args[0], args[1])
        conn.commit()

    try:
        # Orders
        df = load_orders_from_sheets()
        if not df.empty and "id" in df.columns:
            try:
                run_sql("DELETE FROM orders")
            except Exception:
                pass
            for _, row in df.iterrows():
                try:
                    conn.execute("""
                        INSERT OR REPLACE INTO orders (id, platform, client, product, sales, company, start_date, end_date, seconds, spots, amount_net, updated_at, contract_id, seconds_type, project_amount_net, split_amount)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """, (
                        row.get("id"), row.get("platform"), row.get("client"), row.get("product"), row.get("sales"), row.get("company"),
                        row.get("start_date"), row.get("end_date"),
                        _int(row.get("seconds")), _int(row.get("spots")), _float(row.get("amount_net")), row.get("updated_at"),
                        row.get("contract_id"), row.get("seconds_type"), _float(row.get("project_amount_net")), _float(row.get("split_amount"))
                    ))
                    conn.commit()
                except Exception as e:
                    errors.append(f"Orders row {row.get('id')}: {e}")
        # Segments
        df_seg = load_segments_from_sheets()
        if not df_seg.empty and "segment_id" in df_seg.columns:
            try:
                run_sql("DELETE FROM ad_flight_segments")
            except Exception:
                pass
            for _, row in df_seg.iterrows():
                try:
                    conn.execute("""
                        INSERT OR REPLACE INTO ad_flight_segments
                        (segment_id, source_order_id, platform, channel, region, media_platform, company, sales, client, product, seconds, spots, start_date, end_date, duration_days, store_count, total_spots, total_store_seconds, seconds_type, created_at, updated_at)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """, (
                        row.get("segment_id"), row.get("source_order_id"), row.get("platform"), row.get("channel"), row.get("region"),
                        row.get("media_platform"), row.get("company"), row.get("sales"), row.get("client"), row.get("product"),
                        _int(row.get("seconds")), _int(row.get("spots")), row.get("start_date"), row.get("end_date"),
                        _int(row.get("duration_days")), _int(row.get("store_count")), _int(row.get("total_spots")), _int(row.get("total_store_seconds")),
                        row.get("seconds_type"), row.get("created_at"), row.get("updated_at")
                    ))
                    conn.commit()
                except Exception as e:
                    errors.append(f"Segments {row.get('segment_id')}: {e}")
        # Platform settings
        df_ps = load_platform_settings_from_sheets()
        if not df_ps.empty and "platform" in df_ps.columns:
            try:
                run_sql("DELETE FROM platform_settings")
            except Exception:
                pass
            for _, row in df_ps.iterrows():
                try:
                    run_sql("INSERT OR REPLACE INTO platform_settings (platform, store_count, daily_hours) VALUES (?,?,?)",
                            (row.get("platform"), _int(row.get("store_count")), _int(row.get("daily_hours"))))
                except Exception as e:
                    errors.append(f"PlatformSettings: {e}")
        # Capacity
        df_cap = load_capacity_from_sheets()
        if not df_cap.empty and "media_platform" in df_cap.columns:
            try:
                run_sql("DELETE FROM platform_monthly_capacity")
            except Exception:
                pass
            for _, row in df_cap.iterrows():
                try:
                    run_sql("""INSERT OR REPLACE INTO platform_monthly_capacity (media_platform, year, month, daily_available_seconds) VALUES (?,?,?,?)""",
                            (row.get("media_platform"), _int(row.get("year")), _int(row.get("month")), _int(row.get("daily_available_seconds"))))
                except Exception as e:
                    errors.append(f"Capacity: {e}")
        # Purchase
        df_pr = load_purchase_from_sheets()
        if not df_pr.empty and "media_platform" in df_pr.columns:
            try:
                run_sql("DELETE FROM platform_monthly_purchase")
            except Exception:
                pass
            for _, row in df_pr.iterrows():
                try:
                    run_sql("""INSERT OR REPLACE INTO platform_monthly_purchase (media_platform, year, month, purchased_seconds, purchase_price) VALUES (?,?,?,?,?)""",
                            (row.get("media_platform"), _int(row.get("year")), _int(row.get("month")), _int(row.get("purchased_seconds")), _float(row.get("purchase_price"))))
                except Exception as e:
                    errors.append(f"Purchase: {e}")
        # Users（選填：若 Sheet 有且不為空可還原，否則跳過保留本地）
        df_usr = load_users_from_sheets()
        if not df_usr.empty and "username" in df_usr.columns:
            try:
                run_sql("DELETE FROM users")
            except Exception:
                pass
            for _, row in df_usr.iterrows():
                try:
                    run_sql("INSERT OR REPLACE INTO users (id, username, password_hash, role, created_at) VALUES (?,?,?,?,?)",
                            (_int(row.get("id")) or None, row.get("username"), row.get("password_hash"), row.get("role"), row.get("created_at")))
                except Exception as e:
                    errors.append(f"Users: {e}")
    finally:
        conn.close()
    return errors


def _int(v):
    if v is None or (isinstance(v, float) and pd.isna(v)) or v == "":
        return None
    try:
        return int(float(v))
    except (ValueError, TypeError):
        return None


def _float(v):
    if v is None or (isinstance(v, float) and pd.isna(v)) or v == "":
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None
