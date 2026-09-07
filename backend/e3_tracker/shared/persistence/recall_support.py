"""Recall scheduling defaults and search scoring helpers."""
import re
import unicodedata
from difflib import SequenceMatcher
from typing import Any, Dict
from fsrs import Scheduler as FSRSScheduler
from ..source_localization import canonicalize_source_text, literal_source_evidence

RECALL_DAILY_CAPACITY = 18

RECALL_FSRS_SCHEDULER = FSRSScheduler(
    desired_retention=0.88,
    learning_steps=(),
    relearning_steps=(),
    maximum_interval=180,
    enable_fuzzing=False,
)

_RECALL_SEARCH_QUESTION_PHRASES = (
    "請問",
    "我想找",
    "我想查",
    "幫我找",
    "告訴我",
    "是什麼",
    "為什麼",
    "怎麼算",
    "怎麼做",
    "怎麼用",
    "怎麼",
    "什麼意思",
    "什麼",
    "如何",
    "哪一頁",
    "哪裡",
    "相關內容",
)

def _recall_search_compact(value: Any) -> str:
    normalized = unicodedata.normalize("NFKC", str(value or "")).casefold()
    return re.sub(r"[^0-9a-z\u3400-\u9fff]+", "", normalized)

def _recall_search_query_core(value: Any) -> str:
    compact = _recall_search_compact(value)
    for phrase in _RECALL_SEARCH_QUESTION_PHRASES:
        compact = compact.replace(_recall_search_compact(phrase), "")
    return compact or _recall_search_compact(value)

def _recall_search_bigrams(value: str) -> set[str]:
    if len(value) <= 1:
        return {value} if value else set()
    return {value[index:index + 2] for index in range(len(value) - 1)}

def _recall_search_similarity(query: str, value: Any) -> float:
    query_core = _recall_search_query_core(query)
    candidate = _recall_search_compact(value)
    if not query_core or not candidate:
        return 0.0
    score = 0.0
    if query_core in candidate:
        score += 125.0 + min(30.0, len(query_core) * 2.0)
    query_bigrams = _recall_search_bigrams(query_core)
    if query_bigrams:
        candidate_bigrams = _recall_search_bigrams(candidate)
        coverage = len(query_bigrams & candidate_bigrams) / len(query_bigrams)
        score += coverage * 74.0
    normalized_query = unicodedata.normalize("NFKC", str(query or "")).casefold()
    terms = re.findall(r"[a-z0-9]{2,}|[\u3400-\u9fff]{2,}", normalized_query)
    for term in terms:
        compact_term = _recall_search_compact(term)
        if compact_term and compact_term in candidate:
            score += min(24.0, 8.0 + len(compact_term) * 2.0)
    if len(query_core) >= 3:
        segments = [
            _recall_search_compact(segment)
            for segment in re.split(r"[\n\r，。；：、！？,.!?;:]+", str(value or ""))
        ]
        ratios = [
            SequenceMatcher(None, query_core, segment[:max(24, len(query_core) * 4)]).ratio()
            for segment in segments
            if segment
        ]
        if ratios:
            score += max(ratios) * 34.0
    return score

def _recall_search_formula_signature(value: Any) -> str:
    normalized = unicodedata.normalize("NFKC", str(value or "")).casefold()
    normalized = (
        normalized.replace("−", "-")
        .replace("×", "*")
        .replace("÷", "/")
        .replace("→", "->")
        .replace("⇒", "=>")
        .replace("≤", "<=")
        .replace("≥", ">=")
        .replace("≠", "!=")
    )
    normalized = re.sub(
        r"\\(?:operatorname|mathrm|mathbf|mathbb|mathcal)\s*\{([^{}]+)\}",
        r"\1",
        normalized,
    )
    normalized = re.sub(r"\^\{([^{}]+)\}", r"^\1", normalized)
    normalized = re.sub(r"_\{([^{}]+)\}", r"_\1", normalized)
    normalized = re.sub(r"\\(?:left|right|quad|qquad|[,;!])", "", normalized)
    normalized = normalized.replace("{", "").replace("}", "")
    return re.sub(r"[^0-9a-z\u3400-\u9fff+\-*/=<>!^_()\[\]|.]+", "", normalized)

def _recall_search_formula_similarity(query: Any, value: Any) -> float:
    query_signature = _recall_search_formula_signature(query)
    candidate_signature = _recall_search_formula_signature(value)
    if len(query_signature) < 2 or len(candidate_signature) < 2:
        return 0.0
    score = 0.0
    if query_signature in candidate_signature:
        score += 95.0 + min(28.0, len(query_signature) * 1.5)
    query_bigrams = _recall_search_bigrams(query_signature)
    if query_bigrams:
        candidate_bigrams = _recall_search_bigrams(candidate_signature)
        score += len(query_bigrams & candidate_bigrams) / len(query_bigrams) * 56.0
    return score

def _recall_search_contains_formula(value: Any) -> bool:
    text_value = str(value or "")
    return bool(
        re.search(r"\$[^$]+\$|\\[\[(].+?\\[\])]", text_value, flags=re.DOTALL)
        or re.search(r"[A-Za-z0-9)\]}]\s*(?:=|≠|≤|≥|<|>|\^|_)\s*[A-Za-z0-9({\[]", text_value)
        or re.search(r"\\(?:frac|sum|prod|int|sqrt|det|ker|rank|dim|lambda|alpha|beta)\b", text_value)
    )

def _recall_search_excerpt(value: Any, query: str, *, limit: int = 210) -> str:
    text_value = " ".join(str(value or "").split()).strip()
    if len(text_value) <= limit:
        return text_value
    normalized_query = unicodedata.normalize("NFKC", str(query or "")).casefold()
    terms = sorted(
        re.findall(r"[a-z0-9]{2,}|[\u3400-\u9fff]{2,}", normalized_query),
        key=len,
        reverse=True,
    )
    folded_text = unicodedata.normalize("NFKC", text_value).casefold()
    match_index = -1
    for term in terms:
        match_index = folded_text.find(term)
        if match_index >= 0:
            break
    if match_index < 0:
        match_index = 0
    start = max(0, match_index - limit // 3)
    end = min(len(text_value), start + limit)
    start = max(0, end - limit)
    return f"{'…' if start else ''}{text_value[start:end].strip()}{'…' if end < len(text_value) else ''}"

def _recall_search_resolved_page(
    evidence: Any,
    canonical_pages: Dict[int, str],
    preferred_image_index: int,
) -> int:
    """Correct an obviously stale page id without request-time fuzzy localization."""
    canonical_evidence = canonicalize_source_text(literal_source_evidence(evidence))
    if len(canonical_evidence) < 3:
        return preferred_image_index
    exact_matches = [
        image_index
        for image_index, canonical_page in canonical_pages.items()
        if canonical_evidence in canonical_page
    ]
    return exact_matches[0] if len(exact_matches) == 1 else preferred_image_index
