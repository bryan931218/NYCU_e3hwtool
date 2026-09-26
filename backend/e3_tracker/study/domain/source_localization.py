import re
import unicodedata
from collections import Counter
from difflib import SequenceMatcher
from typing import Any, Dict, List, Optional, Tuple

from PIL import Image, ImageChops, ImageFilter, ImageOps


SOURCE_BBOX_VERSION = 15
SOURCE_PAGE_INDEX_VERSION = 14

_META_SOURCE_SUBJECT = re.compile(
    r"(?:本頁|頁面|頁中|圖中|上方|下方|左側|右側|原稿|手寫(?:筆記)?|黑板(?:照片)?|"
    r"圖片|圖示|畫面|截圖|版面)"
)
_META_SOURCE_VERB = re.compile(
    r"(?:可見|顯示|列出|寫有|包含|呈現|示意|說明|整理|總結|標示|畫出|以.+表示)"
)


def literal_source_evidence(value: Any) -> str:
    """Return only evidence that can reasonably be found as literal image text."""
    text = " ".join(str(value or "").split()).strip()
    if not text:
        return ""

    def remove_meta_parenthetical(match: re.Match[str]) -> str:
        content = match.group(0)
        return "" if _META_SOURCE_SUBJECT.search(content) and _META_SOURCE_VERB.search(content) else content

    for _ in range(2):
        text = re.sub(r"[（(][^（）()]{0,360}[）)]", remove_meta_parenthetical, text)
    text = " ".join(text.split()).strip(" ，,；;。")
    if (
        _META_SOURCE_SUBJECT.search(text[:28])
        and _META_SOURCE_VERB.search(text[:100])
        and not re.search(r"[：:]\s*[\w\\\[\]{|]", text)
    ):
        return ""
    return text if len(canonicalize_source_text(text)) >= 3 else ""


def canonicalize_source_text(value: Any) -> str:
    """Normalize OCR/LaTeX text while preserving formula semantics."""
    text = unicodedata.normalize("NFKC", str(value or "")).casefold()
    text = re.sub(r"\\(?:begin|end)\s*\{[^{}]+\}", "", text)
    command_replacements = {
        "alpha": "α",
        "beta": "β",
        "gamma": "γ",
        "delta": "δ",
        "theta": "θ",
        "lambda": "λ",
        "mu": "μ",
        "pi": "π",
        "infty": "∞",
        "rightarrow": "→",
        "longrightarrow": "→",
        "leftarrow": "←",
        "longleftarrow": "←",
        "leftrightarrow": "↔",
        "longleftrightarrow": "↔",
        "mapsto": "↦",
        "iff": "↔",
        "implies": "→",
        "to": "→",
        "in": "∈",
        "notin": "∉",
        "subset": "⊂",
        "subseteq": "⊆",
        "supset": "⊃",
        "supseteq": "⊇",
        "cup": "∪",
        "cap": "∩",
        "oplus": "⊕",
        "neq": "≠",
        "ne": "≠",
        "leq": "≤",
        "le": "≤",
        "geq": "≥",
        "ge": "≥",
        "cdot": "*",
        "times": "*",
        "pm": "±",
        "perp": "⊥",
        "mid": "|",
        "vert": "|",
        "lvert": "|",
        "rvert": "|",
        "parallel": "∥",
        "land": "∧",
        "wedge": "∧",
        "lor": "∨",
        "vee": "∨",
        "equiv": "≡",
        "approx": "≈",
        "ldots": "…",
        "cdots": "…",
        "dots": "…",
    }
    formatting_commands = {
        "operatorname",
        "mathbb",
        "mathcal",
        "mathrm",
        "mathbf",
        "mathsf",
        "mathtt",
        "text",
        "left",
        "right",
        "quad",
        "qquad",
        "displaystyle",
        "overline",
        "underline",
        "bar",
        "vec",
        "hat",
        "tilde",
    }

    def replace_command(match: re.Match[str]) -> str:
        command = match.group(1).casefold()
        if command in command_replacements:
            return command_replacements[command]
        if command in formatting_commands:
            return ""
        return command

    text = re.sub(r"\\([a-zA-Z]+)", replace_command, text)
    text = text.translate(
        str.maketrans(
            {
                "−": "-",
                "–": "-",
                "—": "-",
                "×": "*",
                "⋅": "*",
                "／": "/",
                "＝": "=",
                "＜": "<",
                "＞": ">",
                "⇒": "→",
                "⇐": "←",
                "⇔": "↔",
            }
        )
    )
    semantic_symbols = set("=<>≤≥≠∈∉⊂⊆⊃⊇∪∩⊕⊥∥∧∨≡≈→←↔↦+-*/^|&±∞…")
    return "".join(
        character
        for character in text
        if character.isalnum() or character in semantic_symbols
    )


def resolve_source_evidence_page(
    evidence: Any,
    pages: List[Dict[str, Any]],
    *,
    preferred_image_index: Optional[int] = None,
    context: Any = "",
) -> Optional[Dict[str, Any]]:
    """Resolve literal evidence across every page without trusting a model page id.

    Exact normalized text is the primary gate. Repeated evidence is accepted only
    when the card context clearly distinguishes one page; otherwise no page is
    returned so a common formula cannot silently point at an unrelated image.
    """
    canonical_evidence = canonicalize_source_text(literal_source_evidence(evidence))
    canonical_context = canonicalize_source_text(context)
    if len(canonical_evidence) < 3:
        return None

    ranked: List[Dict[str, Any]] = []
    canonical_pages: Dict[int, str] = {}
    for page in pages:
        if not isinstance(page, dict):
            continue
        try:
            image_index = int(page.get("image_index") or 0)
        except (TypeError, ValueError):
            continue
        canonical_page = canonicalize_source_text(page.get("transcription"))
        if image_index <= 0 or not canonical_page:
            continue
        canonical_pages[image_index] = canonical_page
        exact_count = canonical_page.count(canonical_evidence)
        evidence_metrics = _match_metrics(canonical_evidence, canonical_page)
        context_metrics = (
            _match_metrics(canonical_context, canonical_page)
            if len(canonical_context) >= 4
            else _match_metrics("", "")
        )
        context_score = max(
            float(context_metrics.get("score") or 0.0),
            float(context_metrics.get("coverage") or 0.0) * 0.82,
            float(context_metrics.get("formula_coverage") or 0.0) * 0.70
            if context_metrics.get("formula_token_count")
            else 0.0,
        )
        evidence_score = (
            1.0
            if exact_count
            else max(
                float(evidence_metrics.get("score") or 0.0),
                float(evidence_metrics.get("formula_coverage") or 0.0) * 0.80
                if evidence_metrics.get("formula_token_count")
                else 0.0,
            )
        )
        ranked.append(
            {
                "image_index": image_index,
                "exact_count": exact_count,
                "evidence_score": evidence_score,
                "context_score": context_score,
                "coverage": float(evidence_metrics.get("coverage") or 0.0),
                "boundary_coverage": float(
                    evidence_metrics.get("boundary_coverage") or 0.0
                ),
                "longest_ratio": float(evidence_metrics.get("longest_ratio") or 0.0),
                "formula_coverage": float(
                    evidence_metrics.get("formula_coverage") or 0.0
                ),
                "formula_token_count": int(
                    evidence_metrics.get("formula_token_count") or 0
                ),
            }
        )
    if not ranked:
        return None

    exact_matches = [item for item in ranked if item["exact_count"] > 0]
    if len(exact_matches) == 1:
        result = dict(exact_matches[0])
        result.update(match_kind="unique_exact", match_margin=1.0, page_verified=True)
        return result
    if len(exact_matches) > 1:
        exact_matches.sort(
            key=lambda item: (
                float(item["context_score"]),
                1 if item["image_index"] == preferred_image_index else 0,
            ),
            reverse=True,
        )
        best = exact_matches[0]
        second = exact_matches[1]
        margin = float(best["context_score"]) - float(second["context_score"])
        if float(best["context_score"]) >= 0.34 and margin >= 0.08:
            result = dict(best)
            result.update(
                match_kind="context_disambiguated_exact",
                match_margin=round(margin, 4),
                page_verified=True,
            )
            return result
        return None

    ranked.sort(
        key=lambda item: (
            float(item["evidence_score"]),
            float(item["context_score"]),
        ),
        reverse=True,
    )
    best = ranked[0]
    second_score = float(ranked[1]["evidence_score"]) if len(ranked) > 1 else 0.0
    margin = float(best["evidence_score"]) - second_score
    formula_supported = (
        not best["formula_token_count"] or float(best["formula_coverage"]) >= 0.82
    )
    if (
        float(best["evidence_score"]) >= 0.78
        and float(best["coverage"]) >= 0.86
        and float(best["boundary_coverage"]) >= 0.76
        and float(best["longest_ratio"]) >= 0.62
        and margin >= 0.14
        and formula_supported
    ):
        result = dict(best)
        result.update(
            match_kind="unique_fuzzy",
            match_margin=round(margin, 4),
            page_verified=True,
        )
        return result

    literal_evidence = literal_source_evidence(evidence)
    raw_fragments = re.split(
        r"(?:[。！？；;\n]|\b(?:Pf|Ex|Note)\.?|[∴⇒])+",
        literal_evidence,
        flags=re.IGNORECASE,
    )
    fragments: List[str] = []
    for raw_fragment in raw_fragments:
        fragment = canonicalize_source_text(raw_fragment)
        if len(fragment) < 8 or fragment in fragments:
            continue
        fragments.append(fragment)
    if len(fragments) >= 2:
        total_fragment_length = sum(len(fragment) for fragment in fragments)
        fragment_ranked: List[Dict[str, Any]] = []
        for image_index, canonical_page in canonical_pages.items():
            supported_count = 0
            supported_length = 0
            weighted_quality = 0.0
            for fragment in fragments:
                metrics = _match_metrics(fragment, canonical_page)
                formula_supported = (
                    not metrics.get("formula_token_count")
                    or float(metrics.get("formula_coverage") or 0.0) >= 0.72
                )
                quality = max(
                    float(metrics.get("score") or 0.0),
                    float(metrics.get("formula_coverage") or 0.0) * 0.78
                    if metrics.get("formula_token_count")
                    else 0.0,
                )
                if (
                    quality < 0.76
                    or float(metrics.get("coverage") or 0.0) < 0.82
                    or float(metrics.get("longest_ratio") or 0.0) < 0.40
                    or not formula_supported
                ):
                    continue
                supported_count += 1
                supported_length += len(fragment)
                weighted_quality += quality * len(fragment)
            supported_ratio = supported_length / max(1, total_fragment_length)
            fragment_score = weighted_quality / max(1, supported_length)
            fragment_ranked.append(
                {
                    "image_index": image_index,
                    "fragment_score": fragment_score,
                    "fragment_count": supported_count,
                    "fragment_coverage": supported_ratio,
                }
            )
        fragment_ranked.sort(
            key=lambda item: (
                int(item["fragment_count"]),
                float(item["fragment_coverage"]),
                float(item["fragment_score"]),
            ),
            reverse=True,
        )
        fragment_best = fragment_ranked[0]
        fragment_second = fragment_ranked[1] if len(fragment_ranked) > 1 else None
        best_strength = (
            float(fragment_best["fragment_score"])
            * float(fragment_best["fragment_coverage"])
        )
        second_strength = (
            float(fragment_second["fragment_score"])
            * float(fragment_second["fragment_coverage"])
            if fragment_second
            else 0.0
        )
        fragment_margin = best_strength - second_strength
        if (
            int(fragment_best["fragment_count"]) >= 2
            and float(fragment_best["fragment_coverage"]) >= 0.50
            and float(fragment_best["fragment_score"]) >= 0.80
            and fragment_margin >= 0.16
        ):
            matched = next(
                item
                for item in ranked
                if item["image_index"] == fragment_best["image_index"]
            )
            result = dict(matched)
            result.update(
                match_kind="fragment_consensus",
                match_margin=round(fragment_margin, 4),
                fragment_count=int(fragment_best["fragment_count"]),
                fragment_coverage=round(
                    float(fragment_best["fragment_coverage"]), 4
                ),
                page_verified=True,
            )
            return result
    return None


def _match_metrics(evidence: str, candidate: str) -> Dict[str, float]:
    if not evidence or not candidate:
        return {
            "score": 0.0,
            "coverage": 0.0,
            "precision": 0.0,
            "start_coverage": 0.0,
            "end_coverage": 0.0,
            "boundary_coverage": 0.0,
            "tail_coverage": 0.0,
            "longest_ratio": 0.0,
            "formula_coverage": 0.0,
            "formula_precision": 0.0,
            "formula_token_count": 0.0,
            "uniqueness": 0.0,
        }
    anchor_length = min(64, max(10, round(len(evidence) * 0.34)))
    start_anchor = evidence[:anchor_length]
    end_anchor = evidence[-anchor_length:]
    matcher = SequenceMatcher(None, evidence, candidate, autojunk=False)
    blocks = [block for block in matcher.get_matching_blocks() if block.size]
    matched = sum(block.size for block in blocks)
    longest = max((block.size for block in blocks), default=0)
    first_match = min((block.a for block in blocks), default=len(evidence))
    last_match = max((block.a + block.size for block in blocks), default=0)
    start_matcher = SequenceMatcher(None, start_anchor, candidate, autojunk=False)
    end_matcher = SequenceMatcher(None, end_anchor, candidate, autojunk=False)
    start_coverage = sum(
        block.size for block in start_matcher.get_matching_blocks() if block.size
    ) / max(1, len(start_anchor))
    end_coverage = sum(
        block.size for block in end_matcher.get_matching_blocks() if block.size
    ) / max(1, len(end_anchor))
    coverage = matched / max(1, len(evidence))
    precision = matched / max(1, len(candidate))
    boundary_position = (
        max(0.0, 1.0 - first_match / max(1, anchor_length))
        + max(0.0, 1.0 - (len(evidence) - last_match) / max(1, anchor_length))
    ) / 2
    boundary_coverage = min(start_coverage, end_coverage)
    score = (
        coverage * 0.54
        + boundary_coverage * 0.22
        + ((start_coverage + end_coverage) / 2) * 0.08
        + boundary_position * 0.08
        + precision * 0.06
        + min(0.02, longest / 400)
    )
    formula_metrics = _formula_match_metrics(evidence, candidate)
    return {
        "score": score,
        "coverage": coverage,
        "precision": precision,
        "start_coverage": start_coverage,
        "end_coverage": end_coverage,
        "boundary_coverage": boundary_coverage,
        "tail_coverage": last_match / max(1, len(evidence)),
        "longest_ratio": longest / max(1, len(evidence)),
        **formula_metrics,
    }


_FORMULA_TOKEN_RE = re.compile(
    r"(?:rank|det|dim|ker|span|nullity|trace|sin|cos|tan|log|ln|exp)"
    r"|(?:[a-zα-ω]+\d*)|(?:\d+(?:\.\d+)?)|[=<>≤≥≠∈∉⊂⊆⊃⊇∪∩⊕⊥∥∧∨≡≈→←↔↦+*/^|&±∞]",
    re.IGNORECASE,
)
_FORMULA_RELATIONS = set("=<>≤≥≠∈∉⊂⊆⊃⊇∪∩⊕⊥∥∧∨≡≈→←↔↦+*/^|&±∞")


def _formula_tokens(value: Any) -> List[str]:
    canonical = canonicalize_source_text(value)
    tokens = [match.group(0) for match in _FORMULA_TOKEN_RE.finditer(canonical)]
    if not any(token in _FORMULA_RELATIONS for token in tokens):
        return []
    return tokens


def _formula_match_metrics(evidence: str, candidate: str) -> Dict[str, float]:
    evidence_tokens = _formula_tokens(evidence)
    candidate_tokens = _formula_tokens(candidate)
    if not evidence_tokens:
        return {
            "formula_coverage": 1.0,
            "formula_precision": 1.0,
            "formula_token_count": 0.0,
        }
    evidence_counts = Counter(evidence_tokens)
    candidate_counts = Counter(candidate_tokens)
    matched = sum((evidence_counts & candidate_counts).values())
    relation_evidence = Counter(token for token in evidence_tokens if token in _FORMULA_RELATIONS)
    relation_candidate = Counter(token for token in candidate_tokens if token in _FORMULA_RELATIONS)
    relation_matched = sum((relation_evidence & relation_candidate).values())
    relation_coverage = relation_matched / max(1, sum(relation_evidence.values()))
    token_coverage = matched / max(1, len(evidence_tokens))
    token_precision = matched / max(1, len(candidate_tokens))
    return {
        "formula_coverage": token_coverage * 0.62 + relation_coverage * 0.38,
        "formula_precision": token_precision,
        "formula_token_count": float(len(evidence_tokens)),
    }


def _source_lines_are_spatially_compatible(
    anchor: Dict[str, Any],
    candidate: Dict[str, Any],
) -> bool:
    if not all(key in anchor and key in candidate for key in ("left", "right", "top", "bottom")):
        return True
    try:
        anchor_left = float(anchor["left"])
        anchor_right = float(anchor["right"])
        candidate_left = float(candidate["left"])
        candidate_right = float(candidate["right"])
        anchor_bottom = float(anchor["bottom"])
        candidate_top = float(candidate["top"])
    except (TypeError, ValueError):
        return True
    anchor_width = max(1.0, anchor_right - anchor_left)
    candidate_width = max(1.0, candidate_right - candidate_left)
    overlap = max(0.0, min(anchor_right, candidate_right) - max(anchor_left, candidate_left))
    overlap_ratio = overlap / min(anchor_width, candidate_width)
    center_gap = abs(
        (anchor_left + anchor_right) / 2 - (candidate_left + candidate_right) / 2
    )
    vertical_gap = candidate_top - anchor_bottom
    return (
        vertical_gap <= 135
        and (
            overlap_ratio >= 0.18
            or center_gap <= max(90.0, min(anchor_width, candidate_width) * 0.62)
            or anchor_width >= 720
            or candidate_width >= 720
        )
    )


def _candidate_source_line_paths(
    prepared_lines: List[Tuple[Dict[str, Any], str]],
    maximum_span: int,
) -> List[List[Tuple[Dict[str, Any], str]]]:
    paths: List[List[Tuple[Dict[str, Any], str]]] = []
    for start, first in enumerate(prepared_lines):
        path = [first]
        paths.append(list(path))
        previous = first[0]
        for current in prepared_lines[start + 1 :]:
            if len(path) >= maximum_span:
                break
            if not _source_lines_are_spatially_compatible(previous, current[0]):
                continue
            path.append(current)
            paths.append(list(path))
            previous = current[0]
    return paths


def match_source_evidence_to_lines(
    evidence: str,
    lines: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], Dict[str, float]]:
    canonical_evidence = canonicalize_source_text(evidence)
    prepared_lines = [
        (line, canonicalize_source_text(line.get("text")))
        for line in lines
        if isinstance(line, dict)
    ]
    prepared_lines = [(line, text) for line, text in prepared_lines if text]
    empty_metrics = _match_metrics("", "")
    if len(canonical_evidence) < 3 or not prepared_lines:
        return [], empty_metrics

    maximum_span = min(
        len(prepared_lines),
        max(5, min(28, len(canonical_evidence) // 7 + 6)),
    )
    ranked: List[Tuple[float, List[Tuple[Dict[str, Any], str]], Dict[str, float]]] = []
    for path in _candidate_source_line_paths(prepared_lines, maximum_span):
        combined = "".join(text for _line, text in path)
        if len(combined) > len(canonical_evidence) * 2.25 + 48:
            continue
        metrics = _match_metrics(canonical_evidence, combined)
        formula_weight = 0.18 if metrics["formula_token_count"] else 0.0
        candidate_score = (
            metrics["score"] * (1.0 - formula_weight)
            + metrics["formula_coverage"] * formula_weight
            - max(0, len(path) - 1) * 0.0008
        )
        ranked.append((candidate_score, path, metrics))

    if not ranked:
        return [], empty_metrics
    ranked.sort(key=lambda item: item[0], reverse=True)
    best_score, best_path, metrics = ranked[0]
    if best_score < 0.46:
        return [], metrics

    best_ids = {id(line) for line, _text in best_path}
    second_score = max(
        (
            score
            for score, path, _candidate_metrics in ranked[1:]
            if not best_ids.intersection(id(line) for line, _text in path)
        ),
        default=0.0,
    )
    metrics = dict(metrics)
    metrics["uniqueness"] = max(0.0, min(1.0, (best_score - second_score) / 0.16))
    metrics["candidate_score"] = best_score
    metrics["second_candidate_score"] = second_score
    return [line for line, _text in best_path], metrics


def match_source_evidence_to_sections(
    evidence: str,
    sections: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], Dict[str, float]]:
    """Match evidence inside whole note blocks without a candidate-length cap."""
    canonical_evidence = canonicalize_source_text(evidence)
    empty_metrics = _match_metrics("", "")
    if len(canonical_evidence) < 3:
        return [], empty_metrics
    ranked: List[Tuple[float, Dict[str, Any], Dict[str, float]]] = []
    for section in sections:
        if not isinstance(section, dict):
            continue
        canonical_section = canonicalize_source_text(section.get("text"))
        if not canonical_section:
            continue
        metrics = _match_metrics(canonical_evidence, canonical_section)
        formula_weight = 0.20 if metrics["formula_token_count"] else 0.0
        candidate_score = (
            metrics["score"] * (1.0 - formula_weight)
            + metrics["formula_coverage"] * formula_weight
        )
        ranked.append((candidate_score, section, metrics))
    if not ranked:
        return [], empty_metrics
    ranked.sort(key=lambda item: item[0], reverse=True)
    best_score, best_section, metrics = ranked[0]
    if best_score < 0.42:
        return [], metrics
    second_score = ranked[1][0] if len(ranked) > 1 else 0.0
    metrics = dict(metrics)
    metrics["uniqueness"] = max(0.0, min(1.0, (best_score - second_score) / 0.16))
    metrics["candidate_score"] = best_score
    metrics["second_candidate_score"] = second_score
    return [best_section], metrics


def match_source_evidence_via_page_alignment(
    evidence: str,
    page_transcription: str,
    sections: List[Dict[str, Any]],
    *,
    expected_source_span: Optional[Tuple[int, int]] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, float]]:
    """Map literal page evidence to a separator-bounded OCR section.

    Section OCR often writes matrices and handwritten symbols differently from
    the full-page transcription. Aligning the complete reading order first lets
    surrounding text and section order recover the correct block without using
    the target text to choose image coordinates.
    """
    canonical_evidence = canonicalize_source_text(evidence)
    canonical_page = canonicalize_source_text(page_transcription)
    empty_metrics = {
        **_match_metrics("", ""),
        "alignment_score": 0.0,
        "alignment_evidence_coverage": 0.0,
        "alignment_interval_coverage": 0.0,
        "alignment_context_coverage": 0.0,
        "alignment_section_coverage": 0.0,
        "alignment_page_coverage": 0.0,
        "alignment_uniqueness": 0.0,
        "alignment_mapped_chars": 0.0,
        "alignment_expected_span_agreement": 0.0,
        "alignment_source_start": 0.0,
        "alignment_source_end": 0.0,
        "alignment_section_source_start": 0.0,
        "alignment_section_source_end": 0.0,
    }
    if len(canonical_evidence) < 3 or not canonical_page:
        return [], empty_metrics

    prepared_sections: List[Tuple[Dict[str, Any], str, int, int]] = []
    combined_parts: List[str] = []
    combined_length = 0
    for section in sections:
        if not isinstance(section, dict):
            continue
        canonical_section = canonicalize_source_text(section.get("text"))
        if not canonical_section:
            continue
        start = combined_length
        combined_parts.append(canonical_section)
        combined_length += len(canonical_section)
        prepared_sections.append((section, canonical_section, start, combined_length))
    if not prepared_sections:
        return [], empty_metrics
    canonical_combined = "".join(combined_parts)

    occurrences: List[int] = []
    search_start = 0
    while search_start <= len(canonical_page) - len(canonical_evidence):
        occurrence = canonical_page.find(canonical_evidence, search_start)
        if occurrence < 0:
            break
        occurrences.append(occurrence)
        search_start = occurrence + max(1, len(canonical_evidence) // 3)
    if not occurrences:
        return [], empty_metrics

    matcher = SequenceMatcher(None, canonical_page, canonical_combined, autojunk=False)
    section_pieces: List[List[Tuple[int, int, int]]] = [
        [] for _section in prepared_sections
    ]
    total_mapped_chars = 0
    for block in matcher.get_matching_blocks():
        if not block.size:
            continue
        total_mapped_chars += block.size
        combined_start = block.b
        combined_end = block.b + block.size
        for section_index, (_section, _text, section_start, section_end) in enumerate(
            prepared_sections
        ):
            overlap_start = max(combined_start, section_start)
            overlap_end = min(combined_end, section_end)
            if overlap_end <= overlap_start:
                continue
            page_start = block.a + overlap_start - combined_start
            page_end = page_start + overlap_end - overlap_start
            section_pieces[section_index].append(
                (page_start, page_end, overlap_end - overlap_start)
            )

    page_alignment_coverage = total_mapped_chars / max(1, len(canonical_combined))
    ranked: List[Tuple[float, int, int, Dict[str, float]]] = []
    evidence_length = len(canonical_evidence)
    context_padding = max(48, min(220, evidence_length * 2))
    for occurrence in occurrences:
        evidence_start = occurrence
        evidence_end = occurrence + evidence_length
        context_start = max(0, evidence_start - context_padding)
        context_end = min(len(canonical_page), evidence_end + context_padding)
        context_length = max(
            1,
            (evidence_start - context_start) + (context_end - evidence_end),
        )
        evidence_midpoint = (evidence_start + evidence_end) / 2
        for section_index, (_section, section_text, _start, _end) in enumerate(
            prepared_sections
        ):
            pieces = section_pieces[section_index]
            if not pieces:
                continue
            evidence_support = sum(
                max(0, min(page_end, evidence_end) - max(page_start, evidence_start))
                for page_start, page_end, _size in pieces
            )
            context_support = sum(
                max(0, min(page_end, evidence_start) - max(page_start, context_start))
                + max(0, min(page_end, context_end) - max(page_start, evidence_end))
                for page_start, page_end, _size in pieces
            )
            mapped_chars = sum(size for _page_start, _page_end, size in pieces)
            section_source_start = min(page_start for page_start, _page_end, _size in pieces)
            section_source_end = max(page_end for _page_start, page_end, _size in pieces)
            interval_overlap = max(
                0,
                min(section_source_end, evidence_end)
                - max(section_source_start, evidence_start),
            )
            interval_coverage = interval_overlap / max(1, evidence_length)
            if section_source_start <= evidence_midpoint <= section_source_end:
                proximity = 1.0
            else:
                distance = min(
                    abs(evidence_midpoint - section_source_start),
                    abs(evidence_midpoint - section_source_end),
                )
                proximity = max(0.0, 1.0 - distance / max(80, context_padding * 1.5))
            evidence_coverage = min(1.0, evidence_support / max(1, evidence_length))
            context_coverage = min(1.0, context_support / context_length)
            section_coverage = min(1.0, mapped_chars / max(1, len(section_text)))
            direct_metrics = _match_metrics(canonical_evidence, section_text)
            direct_component = max(
                float(direct_metrics.get("score") or 0.0),
                float(direct_metrics.get("formula_coverage") or 0.0) * 0.82
                if direct_metrics.get("formula_token_count")
                else 0.0,
            )
            expected_agreement = 1.0
            if expected_source_span is not None:
                expected_start, expected_end = expected_source_span
                expected_length = max(1, expected_end - expected_start)
                expected_agreement = max(
                    0,
                    min(section_source_end, expected_end)
                    - max(section_source_start, expected_start),
                ) / expected_length
            alignment_score = (
                evidence_coverage * 0.34
                + interval_coverage * 0.20
                + context_coverage * 0.14
                + section_coverage * 0.12
                + direct_component * 0.10
                + proximity * 0.06
                + expected_agreement * 0.04
            )
            metrics = {
                **direct_metrics,
                "alignment_score": alignment_score,
                "alignment_evidence_coverage": evidence_coverage,
                "alignment_interval_coverage": interval_coverage,
                "alignment_context_coverage": context_coverage,
                "alignment_section_coverage": section_coverage,
                "alignment_page_coverage": page_alignment_coverage,
                "alignment_mapped_chars": float(evidence_support + context_support),
                "alignment_expected_span_agreement": expected_agreement,
                "alignment_source_start": float(evidence_start),
                "alignment_source_end": float(evidence_end),
                "alignment_section_source_start": float(section_source_start),
                "alignment_section_source_end": float(section_source_end),
            }
            ranked.append((alignment_score, occurrence, section_index, metrics))

    if not ranked:
        return [], empty_metrics
    ranked.sort(key=lambda item: item[0], reverse=True)
    best_score, best_occurrence, best_section_index, best_metrics = ranked[0]
    second_score = max(
        (
            score
            for score, occurrence, section_index, _metrics in ranked[1:]
            if occurrence != best_occurrence or section_index != best_section_index
        ),
        default=0.0,
    )
    best_metrics = dict(best_metrics)
    best_metrics["alignment_uniqueness"] = max(
        0.0,
        min(1.0, (best_score - second_score) / 0.14),
    )
    best_metrics["alignment_second_score"] = second_score
    best_metrics["uniqueness"] = best_metrics["alignment_uniqueness"]
    best_metrics["candidate_score"] = max(
        float(best_metrics.get("candidate_score") or 0.0),
        best_score,
    )
    return [prepared_sections[best_section_index][0]], best_metrics


def source_page_alignment_match_is_candidate(
    evidence: str,
    metrics: Dict[str, float],
) -> bool:
    evidence_length = len(canonicalize_source_text(evidence))
    if evidence_length < 4:
        return False
    evidence_coverage = float(metrics.get("alignment_evidence_coverage") or 0.0)
    context_coverage = float(metrics.get("alignment_context_coverage") or 0.0)
    mapped_chars = float(metrics.get("alignment_mapped_chars") or 0.0)
    formula_token_count = float(metrics.get("formula_token_count") or 0.0)
    short_formula_supported = (
        evidence_length > 28
        or formula_token_count == 0.0
        or (
            evidence_coverage >= 0.34
            and float(metrics.get("formula_coverage") or 0.0) >= 0.42
        )
    )
    return (
        float(metrics.get("alignment_score") or 0.0) >= 0.50
        and float(metrics.get("alignment_interval_coverage") or 0.0) >= 0.60
        and float(metrics.get("alignment_section_coverage") or 0.0) >= 0.20
        and float(metrics.get("alignment_page_coverage") or 0.0) >= 0.22
        and (evidence_coverage >= 0.16 or context_coverage >= 0.18)
        and mapped_chars >= min(12.0, max(5.0, evidence_length * 0.10))
        and float(metrics.get("alignment_uniqueness") or 0.0) >= 0.20
        and short_formula_supported
    )


def source_page_alignment_match_is_verified(
    evidence: str,
    metrics: Dict[str, float],
) -> bool:
    evidence_length = len(canonicalize_source_text(evidence))
    if evidence_length < 4:
        return False
    evidence_coverage = float(metrics.get("alignment_evidence_coverage") or 0.0)
    context_coverage = float(metrics.get("alignment_context_coverage") or 0.0)
    formula_token_count = float(metrics.get("formula_token_count") or 0.0)
    short_formula_supported = (
        evidence_length > 28
        or formula_token_count == 0.0
        or (
            evidence_coverage >= 0.38
            and float(metrics.get("formula_coverage") or 0.0) >= 0.46
        )
    )
    return (
        float(metrics.get("alignment_score") or 0.0) >= 0.52
        and float(metrics.get("alignment_interval_coverage") or 0.0) >= 0.64
        and float(metrics.get("alignment_section_coverage") or 0.0) >= 0.28
        and float(metrics.get("alignment_page_coverage") or 0.0) >= 0.30
        and (evidence_coverage >= 0.18 or context_coverage >= 0.22)
        and float(metrics.get("alignment_mapped_chars") or 0.0)
        >= min(14.0, max(6.0, evidence_length * 0.12))
        and float(metrics.get("alignment_uniqueness") or 0.0) >= 0.20
        and float(metrics.get("alignment_expected_span_agreement") or 0.0) >= 0.64
        and short_formula_supported
    )


def source_line_match_is_verified(evidence: str, metrics: Dict[str, float]) -> bool:
    evidence_length = len(canonicalize_source_text(evidence))
    formula_token_count = float(metrics.get("formula_token_count") or 0.0)
    if evidence_length < 4 or (evidence_length < 8 and formula_token_count < 3):
        return False
    if evidence_length <= 18:
        minimums = (0.66, 0.84, 0.78, 0.24, 0.28)
    elif evidence_length <= 45:
        minimums = (0.58, 0.72, 0.64, 0.18, 0.18)
    else:
        minimums = (0.52, 0.62, 0.56, 0.10, 0.10)
    score, coverage, boundary, precision, longest_ratio = minimums
    formula_verified = (
        formula_token_count == 0.0
        or (
            float(metrics.get("formula_coverage") or 0.0) >= 0.76
            and float(metrics.get("formula_precision") or 0.0) >= 0.25
        )
    )
    uniqueness = float(metrics.get("uniqueness") or 0.0)
    uniqueness_verified = "candidate_score" not in metrics or uniqueness >= 0.24
    return (
        float(metrics.get("score") or 0.0) >= score
        and float(metrics.get("coverage") or 0.0) >= coverage
        and float(metrics.get("boundary_coverage") or 0.0) >= boundary
        and float(metrics.get("precision") or 0.0) >= precision
        and float(metrics.get("longest_ratio") or 0.0) >= longest_ratio
        and float(metrics.get("tail_coverage") or 0.0) >= 0.88
        and formula_verified
        and uniqueness_verified
    )


def source_line_match_is_candidate(evidence: str, metrics: Dict[str, float]) -> bool:
    """Use a broad gate before the independent blind-crop verification."""
    evidence_length = len(canonicalize_source_text(evidence))
    formula_token_count = float(metrics.get("formula_token_count") or 0.0)
    if evidence_length < 4 or (evidence_length < 8 and formula_token_count < 3):
        return False
    formula_verified = (
        formula_token_count == 0.0
        or (
            float(metrics.get("formula_coverage") or 0.0) >= 0.68
            and float(metrics.get("formula_precision") or 0.0) >= 0.24
        )
    )
    return (
        float(metrics.get("candidate_score") or metrics.get("score") or 0.0) >= 0.48
        and float(metrics.get("coverage") or 0.0) >= (0.56 if evidence_length > 45 else 0.64)
        and float(metrics.get("longest_ratio") or 0.0) >= 0.12
        and float(metrics.get("uniqueness") or 0.0) >= 0.24
        and formula_verified
    )


def source_section_match_is_candidate(evidence: str, metrics: Dict[str, float]) -> bool:
    """Select one separator-bounded note block without penalizing its context."""
    evidence_length = len(canonicalize_source_text(evidence))
    formula_token_count = float(metrics.get("formula_token_count") or 0.0)
    if evidence_length < 4 or (evidence_length < 8 and formula_token_count < 3):
        return False
    formula_coverage = float(metrics.get("formula_coverage") or 0.0)
    formula_verified = formula_token_count == 0.0 or formula_coverage >= 0.72 or (
        formula_coverage >= 0.56
        and float(metrics.get("coverage") or 0.0) >= 0.86
        and float(metrics.get("boundary_coverage") or 0.0) >= 0.82
    )
    return (
        float(metrics.get("candidate_score") or metrics.get("score") or 0.0) >= 0.50
        and float(metrics.get("coverage") or 0.0) >= 0.62
        and float(metrics.get("boundary_coverage") or 0.0) >= 0.50
        and float(metrics.get("longest_ratio") or 0.0) >= 0.12
        and float(metrics.get("uniqueness") or 0.0) >= 0.20
        and formula_verified
    )


def source_section_match_is_verified(evidence: str, metrics: Dict[str, float]) -> bool:
    evidence_length = len(canonicalize_source_text(evidence))
    formula_token_count = float(metrics.get("formula_token_count") or 0.0)
    if evidence_length < 4 or (evidence_length < 8 and formula_token_count < 3):
        return False
    formula_coverage = float(metrics.get("formula_coverage") or 0.0)
    formula_verified = formula_token_count == 0.0 or formula_coverage >= 0.76 or (
        formula_coverage >= 0.58
        and float(metrics.get("coverage") or 0.0) >= 0.68
        and float(metrics.get("boundary_coverage") or 0.0) >= 0.78
        and float(metrics.get("longest_ratio") or 0.0) >= 0.22
    )
    return (
        float(metrics.get("score") or 0.0) >= 0.52
        and float(metrics.get("coverage") or 0.0) >= 0.62
        and float(metrics.get("boundary_coverage") or 0.0) >= 0.56
        and float(metrics.get("tail_coverage") or 0.0) >= 0.88
        and float(metrics.get("longest_ratio") or 0.0) >= 0.12
        and formula_verified
    )


def _merged_index_ranges(indices: List[int], maximum_gap: int) -> List[Tuple[int, int]]:
    if not indices:
        return []
    ranges: List[Tuple[int, int]] = []
    start = indices[0]
    previous = indices[0]
    for current in indices[1:]:
        if current - previous > maximum_gap:
            ranges.append((start, previous + 1))
            start = current
        previous = current
    ranges.append((start, previous + 1))
    return ranges


def _source_layout_masks(source: Image.Image) -> Tuple[Image.Image, Image.Image]:
    """Return a normalized page and a mask of ink darker than its local background."""
    working = ImageOps.exif_transpose(source).convert("L")
    target_width = min(600, working.width)
    if working.width != target_width:
        target_height = max(1, round(working.height * target_width / working.width))
        working = working.resize((target_width, target_height), Image.Resampling.LANCZOS)
    background = working.filter(ImageFilter.GaussianBlur(radius=max(9.0, working.width / 38)))
    local_darkness = ImageChops.subtract(background, working)
    mask = local_darkness.point(lambda value: 255 if value >= 16 else 0)
    return working, mask


def estimate_source_page_content_bounds(source: Image.Image) -> Dict[str, int]:
    """Estimate the union of visible note content, including embedded dark images."""
    working, local_mask = _source_layout_masks(source)
    width, height = working.size
    gray_pixels = working.load()
    local_pixels = local_mask.load()
    active_rows: List[int] = []
    row_minimum = max(4, round(width * 0.006))
    row_run_minimum = max(3, round(width * 0.006))
    for y in range(height):
        active_count = 0
        current_run = 0
        longest_run = 0
        for x in range(width):
            if local_pixels[x, y] or gray_pixels[x, y] <= 145:
                active_count += 1
                current_run += 1
                longest_run = max(longest_run, current_run)
            else:
                current_run = 0
        if active_count >= row_minimum and (
            longest_run >= row_run_minimum or active_count >= width * 0.08
        ):
            active_rows.append(y)
    row_ranges = [
        bounds
        for bounds in _merged_index_ranges(active_rows, max(1, round(height * 0.003)))
        if bounds[1] - bounds[0] >= max(4, round(height * 0.005))
    ]
    if not row_ranges:
        return {"left": 25, "top": 25, "right": 975, "bottom": 975}
    pixel_top = row_ranges[0][0]
    pixel_bottom = row_ranges[-1][1]

    active_columns: List[int] = []
    column_minimum = max(3, round((pixel_bottom - pixel_top) * 0.004))
    for x in range(width):
        active_count = sum(
            1
            for y in range(pixel_top, pixel_bottom)
            if local_pixels[x, y] or gray_pixels[x, y] <= 145
        )
        if active_count >= column_minimum:
            active_columns.append(x)
    column_ranges = [
        bounds
        for bounds in _merged_index_ranges(active_columns, max(2, round(width * 0.004)))
        if bounds[1] - bounds[0] >= max(3, round(width * 0.005))
    ]
    if column_ranges:
        pixel_left = column_ranges[0][0]
        pixel_right = column_ranges[-1][1]
    else:
        pixel_left, pixel_right = 0, width

    horizontal_padding = max(7, round(width * 0.014))
    vertical_padding = max(6, round(height * 0.010))
    return {
        "left": max(0, round((pixel_left - horizontal_padding) * 1000 / width)),
        "top": max(0, round((pixel_top - vertical_padding) * 1000 / height)),
        "right": min(1000, round((pixel_right + horizontal_padding) * 1000 / width)),
        "bottom": min(1000, round((pixel_bottom + vertical_padding) * 1000 / height)),
    }


def detect_source_horizontal_separator_candidates(
    source: Image.Image,
) -> List[Dict[str, Any]]:
    """Find exact y positions of dashed or solid horizontal line candidates."""
    working, local_mask = _source_layout_masks(source)
    width, height = working.size
    scan_left, scan_right, scan_width = 0, width, width
    mask_pixels = local_mask.load()
    row_candidates: List[Dict[str, Any]] = []
    band_radius = max(1, round(height * 0.0025))
    minimum_run = max(4, round(scan_width * 0.008))
    merge_gap = max(2, round(scan_width * 0.005))
    for y in range(band_radius, height - band_radius):
        active = [
            any(mask_pixels[x, scan_y] for scan_y in range(y - band_radius, y + band_radius + 1))
            for x in range(scan_left, scan_right)
        ]
        runs: List[Tuple[int, int]] = []
        x = 0
        while x < scan_width:
            while x < scan_width and not active[x]:
                x += 1
            start = x
            while x < scan_width and active[x]:
                x += 1
            if x - start >= minimum_run:
                runs.append((start, x))
        merged_runs: List[Tuple[int, int]] = []
        for left, right in runs:
            if merged_runs and left - merged_runs[-1][1] <= merge_gap:
                merged_runs[-1] = (merged_runs[-1][0], right)
            else:
                merged_runs.append((left, right))
        runs = [run for run in merged_runs if run[1] - run[0] >= minimum_run]
        if not runs:
            continue
        coverage = sum(right - left for left, right in runs) / scan_width
        span = (runs[-1][1] - runs[0][0]) / scan_width
        longest = max(right - left for left, right in runs) / scan_width
        if span < 0.32 or (coverage < 0.055 and longest < 0.28):
            continue
        if len(runs) < 4 and longest < 0.28:
            continue
        score = (
            coverage * 0.42
            + span * 0.28
            + min(1.0, len(runs) / 12) * 0.22
            + min(1.0, len(runs) / 6) * 0.08
        )
        row_candidates.append(
            {
                "pixel_y": y,
                "score": score,
                "coverage": coverage,
                "span": span,
                "run_count": len(runs),
                "longest_run": longest,
            }
        )

    clusters: List[List[Dict[str, Any]]] = []
    cluster_gap = max(2, round(height * 0.003))
    for candidate in row_candidates:
        if (
            clusters
            and int(candidate["pixel_y"]) - int(clusters[-1][-1]["pixel_y"]) <= cluster_gap
        ):
            clusters[-1].append(candidate)
        else:
            clusters.append([candidate])

    candidates: List[Dict[str, Any]] = []
    for cluster in clusters:
        cluster_height = int(cluster[-1]["pixel_y"]) - int(cluster[0]["pixel_y"]) + 1
        if cluster_height * 1000 / height > 34:
            continue
        best = max(cluster, key=lambda item: float(item["score"]))
        pixel_y = int(best["pixel_y"])
        context_gap = max(4, round(height * 0.009))
        context_span = max(context_gap + 2, round(height * 0.022))
        context_rows = [
            *range(max(0, pixel_y - context_span), max(0, pixel_y - context_gap)),
            *range(min(height, pixel_y + context_gap), min(height, pixel_y + context_span)),
        ]
        context_density = (
            sum(
                1
                for context_y in context_rows
                for x in range(scan_left, scan_right)
                if mask_pixels[x, context_y]
            )
            / max(1, len(context_rows) * scan_width)
        )
        if (
            float(best["span"]) < 0.43
            and (
                context_density > 0.04
                or cluster_height * 1000 / height > 12
                or float(best["coverage"]) < 0.12
            )
        ):
            continue
        full_context_density = (
            sum(
                1
                for context_y in context_rows
                for x in range(width)
                if mask_pixels[x, context_y]
            )
            / max(1, len(context_rows) * width)
        )
        normalized_thickness = max(1, round(cluster_height * 1000 / height))
        full_width_ratio = scan_width / width
        full_coverage = float(best["coverage"]) * full_width_ratio
        full_span = float(best["span"]) * full_width_ratio
        full_longest = float(best["longest_run"]) * full_width_ratio
        full_score = (
            full_coverage * 0.42
            + full_span * 0.28
            + min(1.0, int(best["run_count"]) / 12) * 0.22
            + min(1.0, int(best["run_count"]) / 6) * 0.08
        )
        separator_likelihood = (
            full_score
            + full_coverage * 0.20
            - full_context_density * 1.60
            - normalized_thickness * 0.003
        )
        candidates.append(
            {
                "y": round(pixel_y * 1000 / height),
                "score": round(float(best["score"]), 4),
                "coverage": round(float(best["coverage"]), 4),
                "span": round(float(best["span"]), 4),
                "run_count": int(best["run_count"]),
                "thickness": normalized_thickness,
                "context_density": round(context_density, 4),
                "full_coverage": round(full_coverage, 4),
                "full_span": round(full_span, 4),
                "full_longest_run": round(full_longest, 4),
                "separator_likelihood": round(separator_likelihood, 4),
            }
        )
    if len(candidates) > 30:
        candidates = sorted(candidates, key=lambda item: float(item["score"]), reverse=True)[:30]
        candidates.sort(key=lambda item: int(item["y"]))
    for index, candidate in enumerate(candidates, start=1):
        candidate["separator_id"] = f"S{index:02d}"
    return candidates


def build_source_page_geometry(source: Image.Image) -> List[Dict[str, Any]]:
    """Detect actual ink-line regions without asking a model for coordinates."""
    working = ImageOps.exif_transpose(source).convert("RGB")
    max_side = max(working.size)
    if max_side > 1800:
        scale = 1800 / max_side
        working = working.resize(
            (max(1, round(working.width * scale)), max(1, round(working.height * scale))),
            Image.Resampling.LANCZOS,
        )
    width, height = working.size
    if width < 80 or height < 80:
        return []

    gray = ImageOps.grayscale(working)
    background = gray.filter(ImageFilter.GaussianBlur(radius=max(4.0, min(width, height) / 52)))
    contrast = ImageChops.difference(background, gray)
    # Camera noise and screen moire create weak ink across nearly every row. A
    # stronger local threshold preserves handwriting while removing that texture.
    mask = contrast.point(lambda value: 255 if value >= 28 else 0).filter(ImageFilter.MaxFilter(3))

    content_top = 0
    color_scan = working
    if width > 420:
        scan_scale = 420 / width
        color_scan = working.resize((420, max(1, round(height * scan_scale))), Image.Resampling.BOX)
    pixels = color_scan.load()
    blue_rows: List[int] = []
    for y in range(max(1, round(color_scan.height * 0.30))):
        blue_count = 0
        for x in range(color_scan.width):
            red, green, blue = pixels[x, y]
            if blue >= red + 22 and blue >= green + 10 and blue >= 68:
                blue_count += 1
        if blue_count / color_scan.width >= 0.16:
            blue_rows.append(y)
    if blue_rows:
        content_top = min(
            height,
            round((max(blue_rows) + 8) * height / color_scan.height + height * 0.035),
        )

    dilation_radius = max(10, round(width * 0.018))
    dilated = mask
    for offset in range(-dilation_radius, dilation_radius + 1, max(3, dilation_radius // 5)):
        shifted = mask.transform(
            mask.size,
            Image.Transform.AFFINE,
            (1, 0, -offset, 0, 1, 0),
            resample=Image.Resampling.NEAREST,
            fillcolor=0,
        )
        dilated = ImageChops.lighter(dilated, shifted)
    dilated = dilated.filter(ImageFilter.MaxFilter(3))

    parent: List[int] = []

    def new_label() -> int:
        label = len(parent)
        parent.append(label)
        return label

    def find(label: int) -> int:
        while parent[label] != label:
            parent[label] = parent[parent[label]]
            label = parent[label]
        return label

    def union(first: int, second: int) -> int:
        first_root = find(first)
        second_root = find(second)
        if first_root != second_root:
            parent[second_root] = first_root
        return first_root

    pixel_access = dilated.load()
    all_runs: List[Tuple[int, int, int, int]] = []
    previous_runs: List[Tuple[int, int, int]] = []
    for y in range(content_top, height):
        row_runs: List[Tuple[int, int, int]] = []
        x = 0
        while x < width:
            while x < width and not pixel_access[x, y]:
                x += 1
            start = x
            while x < width and pixel_access[x, y]:
                x += 1
            if x - start < 2:
                continue
            overlapping = [
                label
                for previous_left, previous_right, label in previous_runs
                if start <= previous_right + 1 and x >= previous_left - 1
            ]
            label = find(overlapping[0]) if overlapping else new_label()
            for other in overlapping[1:]:
                label = union(label, other)
            row_runs.append((start, x, label))
            all_runs.append((y, start, x, label))
        previous_runs = row_runs

    component_boxes: Dict[int, List[int]] = {}
    for y, left, right, label in all_runs:
        root = find(label)
        box = component_boxes.setdefault(root, [left, y, right, y + 1])
        box[0] = min(box[0], left)
        box[1] = min(box[1], y)
        box[2] = max(box[2], right)
        box[3] = max(box[3], y + 1)

    def actual_ink_box(box: List[int]) -> Optional[List[int]]:
        crop = mask.crop(tuple(box))
        rows = list(crop.resize((1, crop.height), Image.Resampling.BOX).getdata())
        columns = list(crop.resize((crop.width, 1), Image.Resampling.BOX).getdata())
        active_rows = [index for index, value in enumerate(rows) if value >= 3]
        active_columns = [index for index, value in enumerate(columns) if value >= 3]
        if not active_rows or not active_columns:
            return None
        return [
            box[0] + active_columns[0],
            box[1] + active_rows[0],
            box[0] + active_columns[-1] + 1,
            box[1] + active_rows[-1] + 1,
        ]

    boxes: List[List[int]] = []
    for component_box in component_boxes.values():
        box = actual_ink_box(component_box)
        if box is None:
            continue
        box_width = box[2] - box[0]
        box_height = box[3] - box[1]
        normalized_width = box_width * 1000 / width
        normalized_height = box_height * 1000 / height
        if normalized_width < 18 or normalized_height < 6:
            continue
        if normalized_height > normalized_width * 1.5 and normalized_width < 150:
            continue
        if normalized_width >= 45 and normalized_height <= 16:
            continue
        if normalized_width >= 760 and normalized_height <= 28:
            continue
        if box[1] >= height * 0.94:
            continue
        average_luminance = int(
            ImageOps.grayscale(working.crop(tuple(box))).resize((1, 1), Image.Resampling.BOX).getpixel((0, 0))
        )
        if average_luminance < 105 and (box[0] < width * 0.12 or normalized_width > 300):
            continue
        if box[1] > height * 0.88 and average_luminance < 205:
            continue
        boxes.append(box)

    def split_large_component(box: List[int]) -> List[List[int]]:
        box_width = box[2] - box[0]
        box_height = box[3] - box[1]
        if box_height * 1000 / height <= 82:
            return [box]
        crop = mask.crop(tuple(box))
        row_values = list(crop.resize((1, crop.height), Image.Resampling.BOX).getdata())
        positive_rows = sorted(value for value in row_values if value > 0)
        row_threshold = max(
            18,
            min(
                58,
                (
                    positive_rows[min(len(positive_rows) - 1, round(len(positive_rows) * 0.55))]
                    if positive_rows
                    else 0
                )
                + 5,
            ),
        )
        active_rows = [index for index, value in enumerate(row_values) if value >= row_threshold]
        row_ranges = _merged_index_ranges(active_rows, max(2, round(height * 0.0022)))
        pieces: List[List[int]] = []
        for local_top, local_bottom in row_ranges:
            if local_bottom - local_top < max(4, round(height * 0.003)):
                continue
            strip = crop.crop((0, local_top, crop.width, local_bottom))
            column_values = list(strip.resize((strip.width, 1), Image.Resampling.BOX).getdata())
            positive_columns = sorted(value for value in column_values if value > 0)
            column_threshold = max(
                5,
                min(
                    28,
                    (
                        positive_columns[
                            min(len(positive_columns) - 1, round(len(positive_columns) * 0.30))
                        ]
                        if positive_columns
                        else 0
                    )
                    + 2,
                ),
            )
            active_columns = [
                index for index, value in enumerate(column_values)
                if value >= column_threshold
            ]
            column_ranges = _merged_index_ranges(
                active_columns,
                max(18, round(width * 0.037)),
            )
            for local_left, local_right in column_ranges:
                piece_width = local_right - local_left
                piece_height = local_bottom - local_top
                if piece_width < max(9, round(width * 0.012)):
                    continue
                normalized_piece_width = piece_width * 1000 / width
                normalized_piece_height = piece_height * 1000 / height
                if normalized_piece_width >= 340 and normalized_piece_height <= 14:
                    continue
                pieces.append(
                    [
                        box[0] + local_left,
                        box[1] + local_top,
                        box[0] + local_right,
                        box[1] + local_bottom,
                    ]
                )
        return pieces if len(pieces) >= 2 else [box]

    boxes = [piece for box in boxes for piece in split_large_component(box)]

    # Merge fragments that share the same physical row, while keeping real
    # left/right columns separate when the whitespace gap is substantial.
    changed = True
    while changed:
        changed = False
        boxes.sort(key=lambda box: (box[1], box[0]))
        merged: List[List[int]] = []
        while boxes:
            current = boxes.pop(0)
            match_index = None
            for index, candidate in enumerate(boxes):
                overlap = max(0, min(current[3], candidate[3]) - max(current[1], candidate[1]))
                overlap_ratio = overlap / max(1, min(current[3] - current[1], candidate[3] - candidate[1]))
                horizontal_gap = max(0, max(current[0], candidate[0]) - min(current[2], candidate[2]))
                if overlap_ratio >= 0.48 and horizontal_gap <= max(28, round(width * 0.042)):
                    match_index = index
                    break
            if match_index is not None:
                candidate = boxes.pop(match_index)
                current = [
                    min(current[0], candidate[0]),
                    min(current[1], candidate[1]),
                    max(current[2], candidate[2]),
                    max(current[3], candidate[3]),
                ]
                boxes.insert(0, current)
                changed = True
            else:
                merged.append(current)
        boxes = merged

    regions: List[Dict[str, Any]] = []
    for left, top, right, bottom in boxes:
        region_width = right - left
        region_height = bottom - top
        normalized_width = region_width * 1000 / width
        normalized_height = region_height * 1000 / height
        average_luminance = int(
            ImageOps.grayscale(working.crop((left, top, right, bottom)))
            .resize((1, 1), Image.Resampling.BOX)
            .getpixel((0, 0))
        )
        if normalized_width >= 45 and normalized_height <= 16:
            continue
        if normalized_height > normalized_width * 1.5 and normalized_width < 150:
            continue
        if right <= width * 0.11 and average_luminance < 185:
            continue
        if top >= height * 0.88 and average_luminance < 205:
            continue
        if normalized_width > 300 and normalized_height > 120 and average_luminance < 130:
            continue
        crop = mask.crop((left, top, right, bottom))
        density = sum(1 for value in crop.getdata() if value) / max(1, region_width * region_height)
        stability = max(0.0, min(1.0, density / 0.22))
        regions.append(
            {
                "line_id": len(regions) + 1,
                "left": max(0, round((left - max(3, width * 0.006)) * 1000 / width)),
                "top": max(0, round((top - max(3, height * 0.003)) * 1000 / height)),
                "right": min(1000, round((right + max(3, width * 0.006)) * 1000 / width)),
                "bottom": min(1000, round((bottom + max(3, height * 0.003)) * 1000 / height)),
                "geometry_confidence": round(0.68 + stability * 0.26, 4),
                "text": "",
            }
        )
    regions.sort(key=lambda item: (item["top"], item["left"], item["bottom"]))
    for line_id, region in enumerate(regions, start=1):
        region["line_id"] = line_id
    return regions


def source_bbox_from_lines(lines: List[Dict[str, Any]]) -> Optional[Dict[str, int]]:
    if not lines:
        return None
    try:
        left = min(int(line["left"]) for line in lines)
        top = min(int(line["top"]) for line in lines)
        right = max(int(line["right"]) for line in lines)
        bottom = max(int(line["bottom"]) for line in lines)
    except (KeyError, TypeError, ValueError):
        return None
    # Sections are already bounded by the user's solid/dashed separators. Keep
    # the highlight slightly outside those bounds so its stroke never covers ink.
    vertical_padding = 15
    horizontal_padding = 18
    return {
        "left": max(0, left - horizontal_padding),
        "top": max(0, top - vertical_padding),
        "right": min(1000, right + horizontal_padding),
        "bottom": min(1000, bottom + vertical_padding),
    }


def assign_transcription_to_source_sections(
    transcription: Any,
    sections: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Map isolated page text to visual sections in contiguous reading order."""
    prepared_sections = [dict(section) for section in sections if isinstance(section, dict)]
    raw_lines = [
        line.strip()
        for line in str(transcription or "").splitlines()
        if line.strip()
    ]
    if not prepared_sections or not raw_lines:
        return []
    prepared_sections.sort(
        key=lambda section: (
            int(section.get("top") or 0),
            int(section.get("left") or 0),
        )
    )
    section_count = min(len(prepared_sections), len(raw_lines))
    prepared_sections = prepared_sections[:section_count]
    if section_count == 1:
        prepared_sections[0].update(
            text="\n".join(raw_lines),
            ocr_confidence=72,
            transcription_fallback=True,
            transcription_isolated=True,
        )
        return prepared_sections

    line_weights = [
        max(6, len(canonicalize_source_text(line)))
        for line in raw_lines
    ]
    cumulative_weights: List[int] = []
    running_weight = 0
    for weight in line_weights:
        running_weight += weight
        cumulative_weights.append(running_weight)
    total_weight = max(1, running_weight)
    heights = [
        max(1, int(section.get("bottom") or 0) - int(section.get("top") or 0))
        for section in prepared_sections
    ]
    total_height = max(1, sum(heights))

    split_indices: List[int] = []
    previous_split = 0
    cumulative_height = 0
    for section_index in range(section_count - 1):
        cumulative_height += heights[section_index]
        target_ratio = cumulative_height / total_height
        minimum_split = previous_split + 1
        maximum_split = len(raw_lines) - (section_count - section_index - 1)
        split_index = min(
            range(minimum_split, maximum_split + 1),
            key=lambda candidate: abs(
                cumulative_weights[candidate - 1] / total_weight - target_ratio
            ),
        )
        split_indices.append(split_index)
        previous_split = split_index

    assigned: List[Dict[str, Any]] = []
    line_start = 0
    for section, line_end in zip(
        prepared_sections,
        [*split_indices, len(raw_lines)],
    ):
        section_text = "\n".join(raw_lines[line_start:line_end]).strip()
        line_start = line_end
        if not section_text:
            continue
        section.update(
            text=section_text,
            ocr_confidence=72,
            transcription_fallback=True,
            transcription_isolated=True,
        )
        assigned.append(section)
    return assigned


def estimated_source_line_count(evidence: Any) -> int:
    text = str(evidence or "")
    canonical_length = len(canonicalize_source_text(text))
    numbered_items = len(re.findall(r"(?:^|\s)[(（]\s*\d+\s*[)）]", text))
    latex_rows = max(
        (match.group(0).count(r"\\") + 1 for match in re.finditer(r"\\begin\{[^{}]*matrix\}.*?\\end\{[^{}]*matrix\}", text, re.DOTALL)),
        default=1,
    )
    text_lines = max(1, len([line for line in text.splitlines() if line.strip()]))
    return max(
        1,
        numbered_items,
        latex_rows,
        text_lines,
        (canonical_length + 21) // 22,
    )


def source_bbox_span_is_plausible(evidence: Any, bbox: Dict[str, Any]) -> bool:
    try:
        height = int(bbox.get("bottom")) - int(bbox.get("top"))
    except (TypeError, ValueError, AttributeError):
        return False
    line_count = estimated_source_line_count(evidence)
    maximum_height = min(820, 125 + max(0, line_count - 1) * 80)
    return 8 <= height <= maximum_height


def collapse_source_refs_by_image(
    source_refs: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Keep one display source per card and image, preferring a verified bbox."""
    collapsed: List[Dict[str, Any]] = []
    positions: Dict[int, int] = {}

    def display_score(source_ref: Dict[str, Any]) -> Tuple[int, int, int]:
        bbox = source_ref.get("bbox")
        confidence = 0
        if isinstance(bbox, dict):
            try:
                confidence = int(bbox.get("confidence") or 0)
            except (TypeError, ValueError):
                confidence = 0
        return (
            1 if isinstance(bbox, dict) else 0,
            confidence,
            1 if source_ref.get("locatable") else 0,
        )

    for source_ref in source_refs:
        if not isinstance(source_ref, dict):
            continue
        try:
            image_index = int(source_ref.get("image_index") or 0)
        except (TypeError, ValueError):
            continue
        if image_index <= 0:
            continue
        position = positions.get(image_index)
        candidate = dict(source_ref)
        if position is None:
            positions[image_index] = len(collapsed)
            collapsed.append(candidate)
            continue
        if display_score(candidate) > display_score(collapsed[position]):
            collapsed[position] = candidate
    return collapsed


def validated_source_bbox(
    value: Any,
    *,
    require_text_verified: bool = False,
    expected_image_index: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    if not isinstance(value, dict):
        return None
    try:
        bbox: Dict[str, Any] = {
            "left": int(value.get("left")),
            "top": int(value.get("top")),
            "right": int(value.get("right")),
            "bottom": int(value.get("bottom")),
            "confidence": int(value.get("confidence")),
            "version": max(1, int(value.get("version") or 1)),
        }
    except (TypeError, ValueError):
        return None
    width = bbox["right"] - bbox["left"]
    height = bbox["bottom"] - bbox["top"]
    if bbox["confidence"] < 60:
        return None
    if not (
        0 <= bbox["left"] < bbox["right"] <= 1000
        and 0 <= bbox["top"] < bbox["bottom"] <= 1000
    ):
        return None
    is_current_section = (
        bbox["version"] >= SOURCE_BBOX_VERSION
        and str(value.get("localization_method") or "")
        in {
            "section_ocr_rag",
            "section_ocr_alignment",
            "section_ocr_reanchored",
            "section_transcription_fallback",
        }
    )
    if width < 12 or height < 8 or width > 960 or height > (960 if is_current_section else 850):
        return None

    if bbox["version"] >= SOURCE_BBOX_VERSION:
        try:
            bbox.update(
                {
                    "text_verified": bool(value.get("text_verified")),
                    "match_score": round(float(value.get("match_score")), 4),
                    "match_coverage": round(float(value.get("match_coverage")), 4),
                    "boundary_coverage": round(float(value.get("boundary_coverage")), 4),
                    "evidence_length": int(value.get("evidence_length") or 0),
                    "expected_lines": int(value.get("expected_lines") or 0),
                    "span_verified": bool(value.get("span_verified")),
                    "crop_verified": bool(value.get("crop_verified")),
                    "transcription_fallback_verified": bool(
                        value.get("transcription_fallback_verified")
                    ),
                    "transcription_isolated": bool(
                        value.get("transcription_isolated")
                    ),
                    "crop_match_score": round(float(value.get("crop_match_score")), 4),
                    "crop_match_coverage": round(float(value.get("crop_match_coverage")), 4),
                    "crop_boundary_coverage": round(
                        float(value.get("crop_boundary_coverage")), 4
                    ),
                    "crop_match_precision": round(
                        float(value.get("crop_match_precision")), 4
                    ),
                    "geometry_verified": bool(value.get("geometry_verified")),
                    "page_verified": bool(value.get("page_verified")),
                    "source_image_index": int(value.get("source_image_index") or 0),
                    "page_match_kind": str(value.get("page_match_kind") or "")[:48],
                    "page_match_margin": round(
                        float(value.get("page_match_margin") or 0.0), 4
                    ),
                    "formula_coverage": round(float(value.get("formula_coverage") or 0.0), 4),
                    "formula_token_count": int(value.get("formula_token_count") or 0),
                    "uniqueness": round(float(value.get("uniqueness") or 0.0), 4),
                    "segmentation_stability": round(
                        float(value.get("segmentation_stability") or 0.0), 4
                    ),
                    "localization_method": str(value.get("localization_method") or ""),
                    "anchor_verified": bool(value.get("anchor_verified")),
                    "localization_anchor": str(
                        value.get("localization_anchor") or ""
                    )[:420],
                    "alignment_verified": bool(value.get("alignment_verified")),
                    "alignment_score": round(float(value.get("alignment_score") or 0.0), 4),
                    "alignment_evidence_coverage": round(
                        float(value.get("alignment_evidence_coverage") or 0.0), 4
                    ),
                    "alignment_interval_coverage": round(
                        float(value.get("alignment_interval_coverage") or 0.0), 4
                    ),
                    "alignment_context_coverage": round(
                        float(value.get("alignment_context_coverage") or 0.0), 4
                    ),
                    "alignment_section_coverage": round(
                        float(value.get("alignment_section_coverage") or 0.0), 4
                    ),
                    "alignment_page_coverage": round(
                        float(value.get("alignment_page_coverage") or 0.0), 4
                    ),
                    "alignment_expected_span_agreement": round(
                        float(value.get("alignment_expected_span_agreement") or 0.0), 4
                    ),
                    "crop_alignment_score": round(
                        float(value.get("crop_alignment_score") or 0.0), 4
                    ),
                    "crop_alignment_evidence_coverage": round(
                        float(value.get("crop_alignment_evidence_coverage") or 0.0), 4
                    ),
                    "crop_alignment_interval_coverage": round(
                        float(value.get("crop_alignment_interval_coverage") or 0.0), 4
                    ),
                    "crop_alignment_context_coverage": round(
                        float(value.get("crop_alignment_context_coverage") or 0.0), 4
                    ),
                    "crop_alignment_section_coverage": round(
                        float(value.get("crop_alignment_section_coverage") or 0.0), 4
                    ),
                    "crop_alignment_page_coverage": round(
                        float(value.get("crop_alignment_page_coverage") or 0.0), 4
                    ),
                    "crop_alignment_expected_span_agreement": round(
                        float(value.get("crop_alignment_expected_span_agreement") or 0.0), 4
                    ),
                }
            )
        except (TypeError, ValueError):
            return None
        is_direct_section = bbox["localization_method"] in {
            "section_ocr_rag",
            "section_ocr_reanchored",
        }
        is_alignment_section = bbox["localization_method"] == "section_ocr_alignment"
        is_transcription_fallback = (
            bbox["localization_method"] == "section_transcription_fallback"
        )
        is_reanchored_section = (
            bbox["localization_method"] == "section_ocr_reanchored"
        )
        common_rejected = (
            not bbox["text_verified"]
            or bbox["evidence_length"] < 4
            or (bbox["evidence_length"] < 8 and bbox["formula_token_count"] < 3)
            or bbox["expected_lines"] < 1
            or not bbox["span_verified"]
            or (not bbox["crop_verified"] and not is_transcription_fallback)
            or not bbox["geometry_verified"]
            or not bbox["page_verified"]
            or bbox["source_image_index"] <= 0
            or (
                expected_image_index is not None
                and bbox["source_image_index"] != int(expected_image_index)
            )
            or bbox["segmentation_stability"] < 0.62
            or (
                is_transcription_fallback
                and (
                    not bbox["transcription_fallback_verified"]
                    or not bbox["transcription_isolated"]
                    or bbox["confidence"] < 72
                    or bbox["page_match_kind"]
                    not in {
                        "unique_exact",
                        "unique_fuzzy",
                        "fragment_consensus",
                        "context_disambiguated_exact",
                    }
                )
            )
            or not (
                is_direct_section
                or is_alignment_section
                or is_transcription_fallback
            )
        )
        if is_alignment_section:
            alignment_support = max(
                bbox["alignment_evidence_coverage"],
                bbox["alignment_context_coverage"],
            )
            crop_alignment_support = max(
                bbox["crop_alignment_evidence_coverage"],
                bbox["crop_alignment_context_coverage"],
            )
            short_formula_rejected = (
                bbox["formula_token_count"] > 0
                and bbox["evidence_length"] <= 28
                and (
                    bbox["formula_coverage"] < 0.42
                    or bbox["alignment_evidence_coverage"] < 0.34
                    or bbox["crop_alignment_evidence_coverage"] < 0.38
                )
            )
            alignment_rejected = (
                not bbox["alignment_verified"]
                or bbox["confidence"] < 72
                or bbox["alignment_score"] < 0.50
                or bbox["alignment_interval_coverage"] < 0.60
                or bbox["alignment_section_coverage"] < 0.20
                or bbox["alignment_page_coverage"] < 0.22
                or bbox["alignment_expected_span_agreement"] < 0.64
                or bbox["crop_alignment_score"] < 0.52
                or bbox["crop_alignment_interval_coverage"] < 0.64
                or bbox["crop_alignment_section_coverage"] < 0.28
                or bbox["crop_alignment_page_coverage"] < 0.30
                or bbox["crop_alignment_expected_span_agreement"] < 0.64
                or alignment_support < 0.18
                or crop_alignment_support < 0.22
                or bbox["uniqueness"] < 0.20
                or short_formula_rejected
            )
            if common_rejected or alignment_rejected:
                return None
        else:
            direct_rejected = (
                bbox["match_score"] < 0.52
                or bbox["match_coverage"] < 0.62
                or bbox["boundary_coverage"] < 0.56
                or bbox["crop_match_score"] < 0.52
                or bbox["crop_match_coverage"] < 0.62
                or bbox["crop_boundary_coverage"] < 0.56
                or bbox["crop_match_precision"] < 0.01
                or bbox["uniqueness"] < 0.24
                or (
                    bbox["formula_token_count"] > 0
                    and bbox["formula_coverage"] < 0.76
                    and not (
                        bbox["formula_coverage"] >= 0.58
                        and bbox["match_coverage"] >= 0.68
                        and bbox["boundary_coverage"] >= 0.78
                        and bbox["crop_match_coverage"] >= 0.68
                        and bbox["crop_boundary_coverage"] >= 0.78
                    )
                )
                or (
                    is_reanchored_section
                    and (
                        not bbox["anchor_verified"]
                        or len(
                            canonicalize_source_text(
                                bbox["localization_anchor"]
                            )
                        )
                        < 8
                        or bbox["confidence"] < 78
                    )
                )
            )
            if common_rejected or direct_rejected:
                return None
        if height > 960:
            return None
    elif require_text_verified:
        return None
    return bbox
