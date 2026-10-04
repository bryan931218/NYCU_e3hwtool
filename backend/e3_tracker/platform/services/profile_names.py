"""Recognize academic-unit labels accidentally stored as personal names."""

import re


def is_academic_unit_name(value: str) -> bool:
    return bool(re.fullmatch(
        r"[\u3400-\u9fff]{1,20}(?:學系|科系|研究所|學院|學程|學部|中心|系)",
        str(value or "").strip(),
    ))
