# -*- coding: utf-8 -*-
"""認證與使用者管理服務層。"""

from __future__ import annotations

import pandas as pd


def auth_verify(*, get_db_connection, hash_password, username: str, password: str):
    conn = get_db_connection()
    try:
        df = pd.read_sql("SELECT username, role, password_hash FROM users WHERE username=?", conn, params=(username,))
    finally:
        conn.close()
    if df.empty:
        return None
    if str(df.iloc[0]["password_hash"]) == hash_password(password):
        return {"username": str(df.iloc[0]["username"]), "role": str(df.iloc[0]["role"])}
    return None


def auth_list_users(*, get_db_connection):
    conn = get_db_connection()
    try:
        return pd.read_sql("SELECT username, role, created_at FROM users ORDER BY username", conn)
    finally:
        conn.close()


def auth_create_user(*, get_db_connection, hash_password, username: str, password: str, role: str):
    username = (username or "").strip()
    if not username or not password:
        return False, "帳號與密碼不可空白"
    conn = get_db_connection()
    c = conn.cursor()
    try:
        c.execute("SELECT 1 FROM users WHERE username=?", (username,))
        if c.fetchone():
            conn.close()
            return False, "帳號已存在"
        c.execute(
            "INSERT INTO users (username, role, password_hash, created_at) VALUES (?,?,?,CURRENT_TIMESTAMP)",
            (username, role, hash_password(password)),
        )
        conn.commit()
        conn.close()
        return True, "建立成功"
    except Exception as e:
        conn.rollback()
        conn.close()
        return False, str(e)


def auth_delete_user(*, get_db_connection, username: str):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("DELETE FROM users WHERE username=?", (username,))
    conn.commit()
    conn.close()


def auth_change_password(*, get_db_connection, hash_password, username: str, new_password: str):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("UPDATE users SET password_hash=? WHERE username=?", (hash_password(new_password), username))
    conn.commit()
    conn.close()

