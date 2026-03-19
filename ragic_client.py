from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

import requests


@dataclass(frozen=True)
class RagicSheetRef:
    host: str
    account: str
    tab_folder: str
    sheet_index: str


def parse_sheet_url(url: str) -> RagicSheetRef:
    """解析 `https://ap13.ragic.com/soundwow/forms12/17?...` -> (host, account, tab, sheet)."""
    try:
        from urllib.parse import urlparse

        u = urlparse((url or "").strip())
        host = u.netloc or "ap13.ragic.com"
        parts = [p for p in (u.path or "").split("/") if p]
        if len(parts) >= 3:
            return RagicSheetRef(host=host, account=parts[0], tab_folder=parts[1], sheet_index=parts[2])
    except Exception:
        pass
    return RagicSheetRef(host="ap13.ragic.com", account="soundwow", tab_folder="forms12", sheet_index="17")


def auth_headers(api_key: str) -> dict[str, str]:
    return {"Authorization": f"Basic {(api_key or '').strip()}"}


def make_listing_url(ref: RagicSheetRef, *, limit: int, offset: int, subtables0: bool, fts: str = "") -> str:
    """
    參考實務（fptest）：用 query string 直接組 `?api&v=3&...`
    注意：Ragic 的 api 旗標在部分環境下需要是 `?api`（無等號），不能用 `api=`。
    """
    base = f"https://{ref.host.strip().strip('/')}/{ref.account.strip().strip('/')}/{ref.tab_folder.strip().strip('/')}/{str(ref.sheet_index).strip().strip('/')}"
    from urllib.parse import quote

    parts = [f"{base}?api", "v=3", f"limit={int(limit)}", f"offset={int(offset)}"]
    if subtables0:
        parts.append("subtables=0")
    if fts:
        parts.append("fts=" + quote(fts))
    return "&".join(parts)


def make_single_record_url(ref: RagicSheetRef, record_id: str | int) -> str:
    base = f"https://{ref.host.strip().strip('/')}/{ref.account.strip().strip('/')}/{ref.tab_folder.strip().strip('/')}/{str(ref.sheet_index).strip().strip('/')}"
    return f"{base}/{str(record_id).strip()}?api&v=3"


def get_json(url: str, api_key: str, *, timeout: int = 60) -> tuple[dict[str, Any] | None, str | None]:
    try:
        r = requests.get(url, headers=auth_headers(api_key), timeout=timeout)
        ct = (r.headers.get("content-type") or "").lower()
        status = r.status_code
        text_snip = (r.text or "")[:600]
        if status >= 400:
            return None, f"http {status} {r.reason} body(head600)={text_snip}"
        # 某些情境 content-type 不是 json，但 body 仍是 json 字串
        try:
            data = r.json()
        except Exception:
            return None, f"non-json response http {status} ct={ct} body(head600)={text_snip}"
        if isinstance(data, dict):
            return data, None
        return None, f"json is not dict: {type(data).__name__}"
    except Exception as e:
        return None, str(e)


def extract_entries(payload: dict[str, Any] | None) -> list[dict[str, Any]]:
    """
    listing payload: { "<rid>": { ...fields... }, ... }
    部分 listing 不會帶 `_ragicId`，這裡一律補上。
    """
    if not isinstance(payload, dict):
        return []
    out: list[dict[str, Any]] = []
    for k, entry in payload.items():
        if not isinstance(entry, dict):
            continue
        if not entry.get("_ragicId"):
            try:
                entry["_ragicId"] = int(k) if str(k).isdigit() else k
            except Exception:
                entry["_ragicId"] = k
        out.append(entry)
    return out


def parse_file_tokens(v: Any) -> list[str]:
    """Ragic 檔案欄位可能是單一字串或多檔字串（換行/逗號分隔）。"""
    if v is None:
        return []
    try:
        import pandas as pd

        if isinstance(v, float) and pd.isna(v):
            return []
    except Exception:
        pass

    if isinstance(v, (list, tuple)):
        out: list[str] = []
        for x in v:
            out.extend(parse_file_tokens(x))
        return [t for t in out if t]

    s = str(v).strip()
    if not s or s.lower() == "nan":
        return []
    import re

    parts = re.split(r"[\n,]+", s)
    return [p.strip() for p in parts if p and p.strip()]


def download_file(ref: RagicSheetRef, file_token: str, api_key: str, *, timeout: int = 120) -> tuple[bytes | None, str | None]:
    """
    下載 Ragic 檔案欄位內容：
    `https://<host>/sims/file.jsp?a=<account>&f=<token>`
    """
    try:
        from urllib.parse import quote

        token = str(file_token).strip()
        if not token:
            return None, "empty token"
        url = f"https://{ref.host.strip().strip('/')}/sims/file.jsp?a={ref.account.strip()}&f={quote(token)}"
        r = requests.get(url, headers=auth_headers(api_key), timeout=timeout)
        r.raise_for_status()
        return r.content, None
    except Exception as e:
        return None, str(e)


def now_hms() -> str:
    return datetime.now().strftime("%H:%M:%S")

