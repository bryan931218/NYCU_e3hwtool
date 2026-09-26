from __future__ import annotations

import re
from typing import Any


_PARENT_TOPIC_PATTERNS = (
    (r"\bdeap\b", "Deap"),
    (r"\bsmmh\b", "SMMH"),
    (r"\b(?:min[ -]?max\s+)?heap\b|最大堆|最小堆|極值堆|二元堆", "Heap"),
    (r"\bavl\b", "AVL 樹"),
    (r"\b(?:threaded\s+b\.?t\.?|threaded\s+binary\s+tree)\b|線索二元樹|線索樹", "線索二元樹"),
    (r"\b(?:extended\s+b\.?t\.?|extended\s+binary\s+tree)\b|延伸二元樹", "延伸二元樹"),
    (r"\bhuffman\b|\bwepl\b|霍夫曼", "Huffman 與 WEPL"),
    (r"\b(?:m[ -]?way(?:\s+search\s+tree)?|b[ -]?tree)\b|多路搜尋樹|B樹", "多路搜尋樹"),
    (r"\b(?:bst|binary\s+search\s+tree)\b|二元搜尋樹", "二元搜尋樹"),
    (r"\bsvd\b|奇異值分解", "奇異值分解"),
    (r"特徵值|特徵向量|eigenvalue|eigenvector", "特徵值與特徵向量"),
    (r"行列式|determinant", "行列式"),
    (r"線性映射|線性轉換|linear\s+(?:map|transformation)", "線性映射"),
    (r"生成樹|spanning\s+tree", "生成樹"),
    (r"最短路徑|shortest\s+path|dijkstra|bellman[ -]?ford", "最短路徑"),
    (r"動態規劃|dynamic\s+programming", "動態規劃"),
    (r"行程|程序|process|thread|執行緒", "行程與執行緒"),
    (r"deadlock|死結", "死結"),
    (r"虛擬記憶體|virtual\s+memory|page\s+replacement|分頁", "虛擬記憶體"),
    (r"cache|快取", "快取記憶體"),
    (r"pipeline|管線", "處理器管線"),
)

_TOPIC_DETAIL_MARKERS = re.compile(
    r"\s*(?:的)?(?:定義|性質|操作|建構|建立|調整|插入|刪除|搜尋|遍歷|範例|"
    r"例題|例子|證明|計算|流程|步驟|方法|應用|演算法|概念|結構|比較|設計|"
    r"標記|總覽|延伸|分析).*$",
    re.IGNORECASE,
)


def coarse_study_topic(topic: Any, concept: Any = "") -> str:
    """Return a stable parent topic for compact note-library presentation."""

    original = " ".join(str(topic or "").split()).strip()
    concept_text = " ".join(str(concept or "").split()).strip()
    searchable = f"{original} {concept_text}".strip()
    for pattern, label in _PARENT_TOPIC_PATTERNS:
        if re.search(pattern, searchable, flags=re.IGNORECASE):
            return label

    candidate = re.split(r"[：:｜|]", original, maxsplit=1)[0].strip()
    candidate = _TOPIC_DETAIL_MARKERS.sub("", candidate).strip(" -–—_、，；;()（）")
    if 2 <= len(candidate) <= 24:
        return candidate
    return original[:48] or concept_text[:48] or "重點整理"
