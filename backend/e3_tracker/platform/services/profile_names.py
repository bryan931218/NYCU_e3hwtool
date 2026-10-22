"""Recognize academic-unit labels accidentally stored as personal names."""

import re


def is_academic_unit_name(value: str) -> bool:
    # E3 also uses shortened degree-program labels without a trailing 系/學程,
    # such as 學士後電子與光子, alongside the person's name in the same heading.
    return bool(re.fullmatch(
        r"(?:[\u3400-\u9fff]{1,20}(?:學系|科系|研究所|學院|學程|學部|中心|系)"
        r"|學士後[\u3400-\u9fff]{2,20})",
        str(value or "").strip(),
    ))
