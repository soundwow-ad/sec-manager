# -*- coding: utf-8 -*-
"""Ragic 素材篇名抽取：首列非 dict、JSON 子表、連結列獨立 dict（如 _ragicId 214）。"""

from __future__ import annotations

import json

from services_ragic_import import (
    _extract_ragic_material_filename_rows,
    _ragic_material_display_string,
)


def test_subtable_first_row_not_dict_still_parses():
    entry = {
        "sub": [
            None,
            {"廣告篇名": "0227-0312 正修科技大學-A.申請入學篇.15秒"},
        ]
    }
    rows = _extract_ragic_material_filename_rows(entry, None)
    titles = [str(t).strip() for t, _ in rows if str(t).strip()]
    assert titles
    assert "正修科技大學" in titles[0]


def test_subtable_as_json_string_with_field_id():
    payload = [{"廣告篇名": "篇名A 15秒"}]
    entry = {"1015381": json.dumps(payload, ensure_ascii=False)}
    rows = _extract_ragic_material_filename_rows(entry, "1015381")
    assert any("篇名A" in str(t) for t, _ in rows)


def test_article_name_only_via_config_field_id():
    """API 若僅以流水號表示廣告篇名欄，可於 ragic_fields 設定「廣告篇名」→ 欄位 ID。"""
    entry = {"1015381": [{"1015999": "流水號鍵篇名 30秒"}]}
    rows = _extract_ragic_material_filename_rows(
        entry, "1015381", fid_article_name="1015999"
    )
    assert any("流水號鍵篇名" in str(t) for t, _ in rows)


def test_linked_child_record_nested_dict_not_list():
    """模擬主表 212 下掛連結列 214：素材在子 dict，非 list。"""
    entry = {
        "_ragicId": 212,
        "1015349": "飛立速有限公司",
        "214": {
            "_ragicId": 214,
            "_parentRagicId": 212,
            "廣告篇名": "0227-0312 正修科大（第一波 申請入學）",
        },
    }
    mat = _ragic_material_display_string(entry, {"素材_廣告檔名": "1015381"})
    assert "飛立速" not in mat
    assert "正修科大" in mat


def test_without_article_name_no_fallback_from_other_cells():
    """無「廣告篇名」時不從其他儲存格猜篇名，素材列應為空。"""
    entry = {
        "st": [
            {"1": "noise", "2": "0227 宣傳 [15] 版"},
        ]
    }
    rows = _extract_ragic_material_filename_rows(entry, None)
    assert not any(str(t).strip() for t, _ in rows)
