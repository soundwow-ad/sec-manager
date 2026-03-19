# -*- coding: utf-8 -*-
"""
Google Sheet 後端：將系統資料同步至 Google 試算表，供免費版 Streamlit 重啟後還原。
可透過 .streamlit/secrets.toml 或環境變數設定；未來可改為公司用 Sheet。
"""
from __future__ import annotations

import io
import json
import os
from typing import Any

import pandas as pd


# 各工作表名稱（與 DB 表對應）
WS_ORDERS = "Orders"
WS_SEGMENTS = "Segments"
WS_PLATFORM_SETTINGS = "PlatformSettings"
WS_CAPACITY = "Capacity"
WS_PURCHASE = "Purchase"
WS_USERS = "Users"

ALL_WORKHEET_NAMES = [WS_ORDERS, WS_SEGMENTS, WS_PLATFORM_SETTINGS, WS_CAPACITY, WS_PURCHASE, WS_USERS]


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
                gs = {k: getattr(raw_gs, k, None) for k in ("credentials", "credentials_json", "client_email", "private_key", "project_id", "private_key_id", "client_id") if getattr(raw_gs, k, None) is not None}
    except Exception:
        gs = {}
    cred_dict = gs.get("credentials")
    if isinstance(cred_dict, dict):
        from google.oauth2 import service_account
        return service_account.Credentials.from_service_account_info(cred_dict)
    raw = gs.get("credentials_json") or os.environ.get("GOOGLE_SHEET_CREDENTIALS")
    if raw:
        if isinstance(raw, str):
            try:
                cred_dict = json.loads(raw)
            except Exception:
                return None
        else:
            cred_dict = raw
        from google.oauth2 import service_account
        return service_account.Credentials.from_service_account_info(cred_dict)
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
        })
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


def _get_sheet_id() -> str | None:
    config = _get_sheet_config()
    sid = config.get("sheet_id") or config.get("sheet_id_")
    return str(sid).strip() if sid else None


def _client():
    """取得 gspread 客戶端，失敗回傳 None。"""
    if not is_sheets_enabled():
        return None
    creds = _get_credentials()
    sheet_id = _get_sheet_id()
    if not creds or not sheet_id:
        return None
    try:
        import gspread
        gc = gspread.authorize(creds)
        return gc.open_by_key(sheet_id)
    except Exception:
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


def _records_to_df(records: list[dict]) -> pd.DataFrame:
    if not records:
        return pd.DataFrame()
    return pd.DataFrame(records)


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
        vals = _df_to_values(df)
        if not vals:
            ws.clear()
            return None
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
        vals = _df_to_values(df)
        if not vals:
            ws.clear()
            return None
        ws.update(vals, value_input_option="USER_ENTERED")
        return None
    except Exception as e:
        return str(e)


def write_platform_settings_to_sheets(df: pd.DataFrame) -> str | None:
    try:
        sh = _client()
        if not sh:
            return "未設定或未啟用 Google Sheet"
        _ensure_worksheets(sh)
        ws = sh.worksheet(WS_PLATFORM_SETTINGS)
        vals = _df_to_values(df)
        ws.update(vals if vals else [["platform", "store_count", "daily_hours"]], value_input_option="USER_ENTERED")
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
        vals = _df_to_values(df)
        ws.update(vals if vals else [["media_platform", "year", "month", "daily_available_seconds"]], value_input_option="USER_ENTERED")
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
        vals = _df_to_values(df)
        ws.update(vals if vals else [["media_platform", "year", "month", "purchased_seconds", "purchase_price"]], value_input_option="USER_ENTERED")
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
        vals = _df_to_values(df)
        ws.update(vals if vals else [["id", "username", "password_hash", "role", "created_at"]], value_input_option="USER_ENTERED")
        return None
    except Exception as e:
        return str(e)


def sync_db_to_sheets(get_db_connection) -> list[str]:
    """
    將目前 SQLite 內所有表同步到 Google Sheet。
    使用 get_db_connection() 取得連線並讀取各表。
    回傳錯誤列表，空表示全部成功。
    """
    errors = []
    conn = get_db_connection()
    try:
        for name, loader, writer in [
            (WS_ORDERS, lambda: pd.read_sql("SELECT * FROM orders", conn), write_orders_to_sheets),
            (WS_SEGMENTS, lambda: pd.read_sql("SELECT * FROM ad_flight_segments", conn), write_segments_to_sheets),
            (WS_PLATFORM_SETTINGS, lambda: pd.read_sql("SELECT * FROM platform_settings", conn), write_platform_settings_to_sheets),
            (WS_CAPACITY, lambda: pd.read_sql("SELECT * FROM platform_monthly_capacity", conn), write_capacity_to_sheets),
            (WS_PURCHASE, lambda: pd.read_sql("SELECT * FROM platform_monthly_purchase", conn), write_purchase_to_sheets),
            (WS_USERS, lambda: pd.read_sql("SELECT id, username, password_hash, role, created_at FROM users", conn), write_users_to_sheets),
        ]:
            try:
                df = loader()
                err = writer(df)
                if err:
                    errors.append(f"{name}: {err}")
            except Exception as e:
                errors.append(f"{name}: {e}")
    finally:
        conn.close()
    return errors


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
