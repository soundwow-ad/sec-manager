# -*- coding: utf-8 -*-
"""Ragic 抓取測試分頁入口封裝。"""

from __future__ import annotations

from typing import Callable, Dict


def render_ragic_test_entry(
    *,
    ragic_fields: Dict,
    ragic_subtable_fields: Dict,
    parse_cue_excel_for_table1: Callable[..., object],
) -> None:
    from ui_ragic_test import render_ragic_test_tab

    merged = {**ragic_fields}
    if ragic_subtable_fields:
        merged.update(ragic_subtable_fields)
    render_ragic_test_tab(
        ragic_fields=merged,
        parse_cue_excel_for_table1=parse_cue_excel_for_table1,
    )

