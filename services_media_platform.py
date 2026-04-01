# -*- coding: utf-8 -*-
"""媒體平台解析服務。"""

from __future__ import annotations

import pandas as pd


def parse_platform_region(raw_platform):
    if not raw_platform or pd.isna(raw_platform):
        return "其他", "其他", "未知"
    raw_platform = str(raw_platform)
    if "宜花束" in raw_platform:
        raw_platform = raw_platform.replace("宜花束", "宜花東")
    # 同義字正規化：部分檔案用「高屏」，系統區域主鍵用「高高屏」
    if "高高屏" not in raw_platform and "高屏" in raw_platform:
        raw_platform = raw_platform.replace("高屏", "高高屏")
    if "新鮮視" in raw_platform:
        platform = "全家"
        channel = "新鮮視"
    # 業務用語：南頻／北頻＝企頻區域別，與「企頻」同等視為全家廣播(企頻)
    elif (
        "企頻" in raw_platform
        or "南頻" in raw_platform
        or "北頻" in raw_platform
        or ("廣播" in raw_platform and "全家" in raw_platform)
    ):
        platform = "全家"
        channel = "企頻"
    elif "家樂福" in raw_platform:
        platform = "家樂福"
        channel = "廣播"
    elif raw_platform.strip() in ("企頻", "RADIO", "企業頻道", "全家廣播"):
        platform = "全家"
        channel = "企頻"
    else:
        platform = "其他"
        channel = "其他"
    region = "未知"
    for r in ["全省", "北北基", "中彰投", "桃竹苗", "高高屏", "雲嘉南", "宜花東"]:
        if r in raw_platform:
            region = r
            break
    return platform, channel, region


def get_media_platform_display(platform, channel, raw_platform=""):
    raw = str(raw_platform or "")
    if platform == "全家" and channel == "企頻":
        return "全家廣播(企頻)"
    if platform == "全家" and channel == "新鮮視":
        return "全家新鮮視"
    if platform == "家樂福":
        return "家樂福量販店" if "量販" in raw else "家樂福超市"
    return "其他"

