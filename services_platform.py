# -*- coding: utf-8 -*-
"""平台設定、採購與容量服務層。"""

from __future__ import annotations

import calendar


def get_platform_monthly_purchase(*, get_db_connection, media_platform, year, month):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        "SELECT purchased_seconds, purchase_price FROM platform_monthly_purchase WHERE media_platform=? AND year=? AND month=?",
        (media_platform, int(year), int(month)),
    )
    row = c.fetchone()
    conn.close()
    return row if row is not None else None


def set_platform_monthly_purchase(
    *,
    get_db_connection,
    sync_sheets_if_enabled,
    media_platform,
    year,
    month,
    purchased_seconds,
    purchase_price,
):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        """
        INSERT OR REPLACE INTO platform_monthly_purchase (media_platform, year, month, purchased_seconds, purchase_price)
        VALUES (?, ?, ?, ?, ?)
    """,
        (media_platform, int(year), int(month), int(purchased_seconds), float(purchase_price)),
    )
    ndays = calendar.monthrange(int(year), int(month))[1]
    daily_seconds = int(purchased_seconds) // ndays if ndays else 0
    c.execute(
        """
        INSERT OR REPLACE INTO platform_monthly_capacity (media_platform, year, month, daily_available_seconds)
        VALUES (?, ?, ?, ?)
    """,
        (media_platform, int(year), int(month), daily_seconds),
    )
    conn.commit()
    conn.close()
    sync_sheets_if_enabled()


def load_platform_monthly_purchase_for_year(*, get_db_connection, media_platform, year):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        "SELECT month, purchased_seconds, purchase_price FROM platform_monthly_purchase WHERE media_platform=? AND year=?",
        (media_platform, int(year)),
    )
    out = {row[0]: (row[1], row[2]) for row in c.fetchall()}
    conn.close()
    return out


def load_platform_monthly_purchase_all_media_for_year(*, get_db_connection, year):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        "SELECT media_platform, month, purchased_seconds, purchase_price FROM platform_monthly_purchase WHERE year=?",
        (int(year),),
    )
    out = {}
    for row in c.fetchall():
        mp, mo, sec, pr = row[0], row[1], row[2], row[3]
        if mp not in out:
            out[mp] = {}
        out[mp][mo] = (sec, pr)
    conn.close()
    return out


def load_platform_settings(*, get_db_connection):
    conn = get_db_connection()
    c = conn.cursor()
    settings = {}
    for row in c.execute("SELECT platform, store_count, daily_hours FROM platform_settings"):
        settings[row[0]] = {"store_count": row[1], "daily_hours": row[2]}
    conn.close()
    return settings


def get_platform_monthly_capacity(*, get_db_connection, media_platform, year, month):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        "SELECT daily_available_seconds FROM platform_monthly_capacity WHERE media_platform=? AND year=? AND month=?",
        (media_platform, int(year), int(month)),
    )
    row = c.fetchone()
    conn.close()
    return row[0] if row is not None else None


def set_platform_monthly_capacity(*, get_db_connection, sync_sheets_if_enabled, media_platform, year, month, daily_available_seconds):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        """
        INSERT OR REPLACE INTO platform_monthly_capacity (media_platform, year, month, daily_available_seconds)
        VALUES (?, ?, ?, ?)
    """,
        (media_platform, int(year), int(month), int(daily_available_seconds)),
    )
    conn.commit()
    conn.close()
    sync_sheets_if_enabled()


def load_platform_monthly_capacity_for(*, get_db_connection, year, month):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        "SELECT media_platform, daily_available_seconds FROM platform_monthly_capacity WHERE year=? AND month=?",
        (int(year), int(month)),
    )
    out = {row[0]: row[1] for row in c.fetchall()}
    conn.close()
    return out


def save_platform_settings(*, get_db_connection, sync_sheets_if_enabled, platform, store_count, daily_hours):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        """
        INSERT OR REPLACE INTO platform_settings (platform, store_count, daily_hours)
        VALUES (?, ?, ?)
    """,
        (platform, store_count, daily_hours),
    )
    conn.commit()
    conn.close()
    sync_sheets_if_enabled()

