"""Note validation; dependencies are bound per application."""

import math
import re
from difflib import SequenceMatcher
from typing import Any, Dict, List, Optional, Set, Tuple


def build_note_validation(*,
    _canonical_study_source_match_text,
    _is_recall_concept_eligible,
    _literal_study_source_evidence,
    _normalize_study_concept_title,
    _normalize_study_math_markup,
    _resolve_study_source_page,
    _strip_study_process_narration,
    _study_has_invalid_negation_counterexample,
    _study_text_quality_issue,
    app,
):
    def _study_source_coverage_items(source_pages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        example_heading_pattern = re.compile(
            r"(?:^|(?<=[\n。；;]))\s*(?:"
            r"例題|範例|例子|算例|反例|練習題|練習|題目|問題|作業題|示範題|案例"
            r"|(?:worked\s+)?example|ex\.?|exercise|problem|question"
            r")\s*(?:[：:.)、-]|\d{0,3})",
            flags=re.IGNORECASE,
        )

        def is_example_block(value: str) -> bool:
            normalized = " ".join(value.split()).strip()
            if example_heading_pattern.search(normalized):
                return True
            # A question-shaped block is an example only when it also has a
            # concrete request or a solution marker. This avoids promoting
            # ordinary explanatory sentences containing "for example".
            has_request = bool(
                re.search(
                    r"(?:求出|求解|計算|判斷|證明|找出|解出|求其|是否|試證|solve|show\s+that|find|calculate|determine|prove)",
                    normalized,
                    flags=re.IGNORECASE,
                )
            )
            has_solution = bool(
                re.search(r"(?:解答|解：|解:|答案|solution|answer)", normalized, flags=re.IGNORECASE)
            )
            return has_request and (has_solution or bool(re.search(r"[?？=→≤≥]", normalized)))

        def split_blocks(value: str) -> List[str]:
            paragraphs = [part for part in re.split(r"\n\s*\n+", value) if part.strip()]
            result: List[str] = []
            for paragraph in paragraphs:
                # Notes frequently place several examples one after another
                # without a blank line. Split at explicit example headings,
                # while leaving numbered equations and normal prose intact.
                matches = list(example_heading_pattern.finditer(paragraph))
                if len(matches) <= 1:
                    result.append(paragraph)
                    continue
                starts = [match.start() for match in matches]
                if starts[0] > 0 and paragraph[: starts[0]].strip():
                    result.append(paragraph[: starts[0]])
                for index, start in enumerate(starts):
                    end = starts[index + 1] if index + 1 < len(starts) else len(paragraph)
                    result.append(paragraph[start:end])
            return result

        items: List[Dict[str, Any]] = []
        for page in source_pages:
            try:
                image_index = int(page.get("image_index") or 0)
            except (TypeError, ValueError):
                continue
            block_index = 0
            for block in split_blocks(str(page.get("transcription") or "")):
                compact = " ".join(block.split()).strip()
                clear_text = re.sub(r"〔[^〕]*〕", "", compact).strip(" -—_")
                if "〔無法推定〕" in compact:
                    continue
                example_block = is_example_block(clear_text)
                has_structure = bool(
                    re.search(
                        r"(?:=|≠|⇔|→|≤|≥|∈|∉|\\(?:frac|sum|prod|int|to|in|oplus)|"
                        r"\b(?:if|then|rank|det|Ex\.)\b|定義|條件|方法|性質|結論|證明|範例|例題)",
                        clear_text,
                        flags=re.IGNORECASE,
                    )
                )
                if len(clear_text) < 8 or (len(clear_text) < 20 and not example_block) or (
                    len(clear_text) < 34 and not has_structure and not example_block
                ):
                    continue
                block_index += 1
                items.append(
                    {
                        "id": f"p{image_index}b{block_index}",
                        "image_index": image_index,
                        "text": compact,
                        "priority": "required" if has_structure or example_block else "supporting",
                        "content_type": "example" if example_block else "concept",
                        "is_example": example_block,
                    }
                )
            for visual_region in page.get("visual_regions") or []:
                if not isinstance(visual_region, dict):
                    continue
                region_id = str(visual_region.get("region_id") or "").strip()
                visual_text = "；".join(
                    part
                    for part in (
                        str(visual_region.get("title") or "").strip(),
                        str(visual_region.get("description") or "").strip(),
                        str(visual_region.get("visible_text") or "").strip(),
                    )
                    if part
                )
                if not region_id or not visual_text:
                    continue
                items.append(
                    {
                        "id": region_id,
                        "image_index": image_index,
                        "text": visual_text[:1200],
                        "priority": "required",
                        "content_type": "visual",
                        "is_example": False,
                        "visual_region_id": region_id,
                    }
                )
        return items

    def _study_source_page_coverage_plan(source_pages: List[Dict[str, Any]]) -> Dict[str, Any]:
        page_scores: Dict[int, int] = {}
        example_counts: Dict[int, int] = {}
        for page in source_pages:
            try:
                image_index = int(page.get("image_index") or 0)
            except (TypeError, ValueError):
                continue
            meaningful_blocks: List[str] = []
            page_items = [
                item
                for item in _study_source_coverage_items([page])
                if int(item.get("image_index") or 0) == image_index
            ]
            for item in page_items:
                block = str(item.get("text") or "")
                compact = " ".join(block.split()).strip()
                clear_text = re.sub(r"〔[^〕]*〕", "", compact).strip()
                if len(clear_text) >= 20:
                    meaningful_blocks.append(clear_text)
            if not meaningful_blocks:
                continue
            clear_length = sum(len(block) for block in meaningful_blocks)
            page_scores[image_index] = max(len(meaningful_blocks), math.ceil(clear_length / 220))
            example_counts[image_index] = sum(1 for item in page_items if item.get("is_example"))
        if not page_scores:
            return {"target_cards": 1, "page_quotas": {}}

        target_cards = max(
            len(page_scores),
            math.ceil(sum(page_scores.values()) / 2),
            sum(example_counts.values()),
        )
        page_quotas = {
            image_index: max(1, example_counts.get(image_index, 0))
            for image_index in page_scores
        }
        target_cards = max(target_cards, sum(page_quotas.values()))
        while sum(page_quotas.values()) < target_cards:
            image_index = max(
                page_scores,
                key=lambda index: (
                    page_scores[index] / (page_quotas[index] + 1),
                    page_scores[index],
                    -index,
                ),
            )
            page_quotas[image_index] += 1
        return {"target_cards": target_cards, "page_quotas": page_quotas}

    def _study_coverage_evidence_matches(evidence: Any, item_text: Any) -> bool:
        evidence_text = _canonical_study_source_match_text(evidence)
        coverage_text = _canonical_study_source_match_text(item_text)
        if not evidence_text or not coverage_text:
            return False
        if evidence_text in coverage_text or coverage_text in evidence_text:
            return True
        match = SequenceMatcher(None, evidence_text, coverage_text, autojunk=False).find_longest_match()
        return match.size >= 18 and match.size / max(1, min(len(evidence_text), len(coverage_text))) >= 0.72

    def _enrich_study_card_coverage_ids(payload: Any, source_pages: List[Dict[str, Any]]) -> None:
        if not isinstance(payload, dict) or not isinstance(payload.get("key_concepts"), list):
            return
        coverage_items = _study_source_coverage_items(source_pages)
        for concept in payload["key_concepts"]:
            if not isinstance(concept, dict):
                continue
            matched_ids: List[str] = []
            for source_ref in concept.get("source_refs") or []:
                if not isinstance(source_ref, dict):
                    continue
                try:
                    image_index = int(source_ref.get("image_index") or 0)
                except (TypeError, ValueError):
                    continue
                evidence = source_ref.get("evidence") or ""
                for item in coverage_items:
                    if (
                        item["image_index"] == image_index
                        and item["id"] not in matched_ids
                        and _study_coverage_evidence_matches(evidence, item["text"])
                    ):
                        matched_ids.append(item["id"])
            for visual_ref in concept.get("visual_refs") or []:
                if not isinstance(visual_ref, dict):
                    continue
                region_id = str(visual_ref.get("region_id") or "").strip()
                if region_id and any(
                    item["id"] == region_id
                    and item.get("visual_region_id") == region_id
                    for item in coverage_items
                ):
                    matched_ids.append(region_id)
            concept["coverage_ids"] = list(dict.fromkeys(matched_ids))[:8]

    def _study_recall_coverage_gaps(payload: Any, source_pages: List[Dict[str, Any]]) -> Dict[str, Any]:
        if not isinstance(payload, dict) or not isinstance(payload.get("key_concepts"), list):
            return {"page_quotas": {"payload": "invalid"}, "coverage_items": ["all"], "example_items": ["all"]}
        page_texts = {
            int(page.get("image_index") or 0): " ".join(str(page.get("transcription") or "").split())
            for page in source_pages
            if isinstance(page, dict)
        }
        cards_by_page: Dict[int, int] = {image_index: 0 for image_index in page_texts}
        for concept in payload["key_concepts"]:
            if not isinstance(concept, dict):
                continue
            valid_pages: Set[int] = set()
            for source_ref in concept.get("source_refs") or []:
                if not isinstance(source_ref, dict):
                    continue
                try:
                    image_index = int(source_ref.get("image_index") or 0)
                except (TypeError, ValueError):
                    continue
                evidence = " ".join(str(source_ref.get("evidence") or "").split()).strip()
                if evidence and evidence in page_texts.get(image_index, ""):
                    valid_pages.add(image_index)
            for visual_ref in concept.get("visual_refs") or []:
                if not isinstance(visual_ref, dict):
                    continue
                try:
                    image_index = int(visual_ref.get("image_index") or 0)
                except (TypeError, ValueError):
                    continue
                if image_index in page_texts and visual_ref.get("region_id"):
                    valid_pages.add(image_index)
            for image_index in valid_pages:
                cards_by_page[image_index] = cards_by_page.get(image_index, 0) + 1
        coverage_plan = _study_source_page_coverage_plan(source_pages)
        missing_page_quotas = {
            image_index: {
                "current": cards_by_page.get(image_index, 0),
                "required": quota,
            }
            for image_index, quota in coverage_plan["page_quotas"].items()
            if cards_by_page.get(image_index, 0) < quota
        }
        coverage_items = {item["id"]: item for item in _study_source_coverage_items(source_pages)}
        covered_ids: Set[str] = set()
        example_covered_ids: Set[str] = set()
        for concept in payload["key_concepts"]:
            if not isinstance(concept, dict):
                continue
            source_evidence = [
                (
                    int(source_ref.get("image_index") or 0),
                    " ".join(str(source_ref.get("evidence") or "").split()).strip(),
                )
                for source_ref in concept.get("source_refs") or []
                if isinstance(source_ref, dict)
            ]
            visual_region_ids = {
                str(visual_ref.get("region_id") or "").strip()
                for visual_ref in concept.get("visual_refs") or []
                if isinstance(visual_ref, dict)
            }
            for coverage_id in concept.get("coverage_ids") or []:
                item = coverage_items.get(str(coverage_id))
                if not item:
                    continue
                if item.get("visual_region_id"):
                    if item["visual_region_id"] in visual_region_ids:
                        covered_ids.add(str(coverage_id))
                    continue
                if any(
                    image_index == item["image_index"]
                    and _study_coverage_evidence_matches(evidence, item["text"])
                    for image_index, evidence in source_evidence
                ):
                    covered_ids.add(str(coverage_id))
                    if item.get("is_example") and concept.get("card_type") == "example":
                        example_covered_ids.add(str(coverage_id))
            # Some model responses omit coverage_ids even when source_refs are
            # valid. Resolve example coverage directly from the evidence so a
            # missing example cannot pass as an ordinary concept card.
            if concept.get("card_type") == "example":
                for item_id, item in coverage_items.items():
                    if not item.get("is_example") or item_id in example_covered_ids:
                        continue
                    if any(
                        image_index == item["image_index"]
                        and _study_coverage_evidence_matches(evidence, item["text"])
                        for image_index, evidence in source_evidence
                    ):
                        example_covered_ids.add(item_id)
                        covered_ids.add(item_id)
        example_items = {
            item_id: item
            for item_id, item in coverage_items.items()
            if item.get("is_example")
        }
        return {
            "page_quotas": missing_page_quotas,
            "coverage_items": sorted(set(coverage_items) - covered_ids),
            "example_items": [
                {
                    "id": item_id,
                    "image_index": item["image_index"],
                    "text": str(item["text"])[:600],
                }
                for item_id, item in sorted(example_items.items())
                if item_id not in example_covered_ids
            ],
        }

    def _study_recall_page_coverage_met(payload: Any, source_pages: List[Dict[str, Any]]) -> bool:
        gaps = _study_recall_coverage_gaps(payload, source_pages)
        return not gaps["page_quotas"] and not gaps["coverage_items"] and not gaps.get("example_items")

    def _study_recall_coverage_metrics(
        payload: Any,
        source_pages: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        gaps = _study_recall_coverage_gaps(payload, source_pages)
        coverage_items = _study_source_coverage_items(source_pages)
        missing_ids = set(gaps["coverage_items"])
        required_items = [item for item in coverage_items if item.get("priority") == "required"]
        supporting_items = [item for item in coverage_items if item.get("priority") != "required"]
        example_items = [item for item in coverage_items if item.get("is_example")]
        required_covered = sum(item["id"] not in missing_ids for item in required_items)
        supporting_covered = sum(item["id"] not in missing_ids for item in supporting_items)
        example_missing = len(gaps.get("example_items") or [])
        page_plan = _study_source_page_coverage_plan(source_pages)
        planned_pages = set(page_plan["page_quotas"])
        missing_pages = {
            int(image_index)
            for image_index, values in gaps["page_quotas"].items()
            if isinstance(values, dict) and int(values.get("current") or 0) <= 0
        }
        represented_pages = len(planned_pages - missing_pages)
        required_ratio = required_covered / len(required_items) if required_items else 1.0
        supporting_ratio = supporting_covered / len(supporting_items) if supporting_items else 1.0
        page_ratio = represented_pages / len(planned_pages) if planned_pages else 1.0
        overall_ratio = (
            (len(coverage_items) - len(missing_ids)) / len(coverage_items)
            if coverage_items
            else 1.0
        )
        quality_score = required_ratio * 0.65 + page_ratio * 0.25 + supporting_ratio * 0.10
        return {
            "quality_score": round(quality_score, 4),
            "example_ratio": round(
                ((len(example_items) - example_missing) / len(example_items))
                if example_items else 1.0,
                4,
            ),
            "required_ratio": round(required_ratio, 4),
            "supporting_ratio": round(supporting_ratio, 4),
            "page_ratio": round(page_ratio, 4),
            "overall_ratio": round(overall_ratio, 4),
            "required_total": len(required_items),
            "required_missing": len(required_items) - required_covered,
            "example_total": len(example_items),
            "example_missing": example_missing,
            "supporting_total": len(supporting_items),
            "supporting_missing": len(supporting_items) - supporting_covered,
            "planned_pages": len(planned_pages),
            "missing_pages": sorted(missing_pages),
            "gaps": gaps,
        }

    def _study_recall_coverage_needs_repair(
        payload: Any,
        source_pages: List[Dict[str, Any]],
    ) -> bool:
        metrics = _study_recall_coverage_metrics(payload, source_pages)
        return bool(
            metrics["example_ratio"] < 1.0
            or metrics["required_ratio"] < 0.90
            or metrics["page_ratio"] < 0.80
            or metrics["quality_score"] < 0.82
        )

    def _validate_recall_output(payload: Any, source_pages: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        def normalize_math_markup(value: str) -> str:
            return _normalize_study_math_markup(value)

        def strip_process_narration(value: str) -> str:
            return _strip_study_process_narration(value)

        def has_transpose_dimension_conflict(value: str) -> bool:
            if not ("A^T" in value or "A^{T}" in value or "轉置" in value):
                return False
            match = re.search(
                r"M_\{([^{}\s]+)[×x]([^{}\s]+)\}\s*(?:→|\\to)\s*M_\{([^{}\s]+)[×x]([^{}\s]+)\}",
                value,
            )
            if not match:
                return False
            source_rows, source_columns, target_rows, target_columns = match.groups()
            return target_rows != source_columns or target_columns != source_rows

        def mapping_signatures(value: str) -> Set[str]:
            canonical = str(value or "")
            canonical = re.sub(r"\\mathbb\{([A-Za-z])\}", r"\1", canonical)
            canonical = canonical.replace("\\mathbb R", "R").replace("ℝ", "R").replace("ℂ", "C")
            canonical = canonical.replace("\\times", "×").replace("\\to", "→").replace("->", "→")
            canonical = re.sub(r"(?<=[A-Za-z0-9])\s*x\s*(?=[A-Za-z0-9])", "×", canonical)
            canonical = re.sub(r"[{}\s_]", "", canonical)
            space = r"(?:[A-Z][A-Za-z0-9]*(?:\^[A-Za-z0-9]+(?:×[A-Za-z0-9]+)?)?(?:×[A-Za-z0-9]+)?)"
            return set(re.findall(rf"{space}→{space}", canonical))

        def has_mapping_signature_conflict(value: str, source_refs: List[Dict[str, Any]]) -> bool:
            card_signatures = mapping_signatures(value)
            if not card_signatures:
                return False
            source_signatures = mapping_signatures(" ".join(ref["evidence"] for ref in source_refs))
            if not source_signatures:
                return False
            return not card_signatures.issubset(source_signatures)

        def has_invalid_negation_counterexample(value: str) -> bool:
            return _study_has_invalid_negation_counterexample(value)

        def has_matrix_product_dimension_conflict(value: str) -> bool:
            canonical = str(value or "")
            canonical = canonical.replace("\\times", "×").replace("\\(", "").replace("\\)", "")
            canonical = canonical.replace("\\[", "").replace("\\]", "")
            dimensions: Dict[str, Tuple[str, str]] = {}
            for name, rows, columns in re.findall(
                r"\b([A-Z])_?\{?([A-Za-z0-9]+)×([A-Za-z0-9]+)\}?",
                canonical,
            ):
                dimensions[name] = (rows, columns)
            for name, rows, columns in re.findall(
                r"\b([A-Z])\b\s*(?:為|is|:)\s*\{?([A-Za-z0-9]+)×([A-Za-z0-9]+)\}?",
                canonical,
                flags=re.IGNORECASE,
            ):
                dimensions[name.upper()] = (rows, columns)
            for left, right, rows, columns in re.findall(
                r"\(?([A-Z])([A-Z])\)?_?\{?([A-Za-z0-9]+)×([A-Za-z0-9]+)\}?",
                canonical,
            ):
                left_dimensions = dimensions.get(left)
                right_dimensions = dimensions.get(right)
                if not left_dimensions or not right_dimensions:
                    continue
                if left_dimensions[1] != right_dimensions[0]:
                    return True
                if (rows, columns) != (left_dimensions[0], right_dimensions[1]):
                    return True
            return False

        if not isinstance(payload, dict):
            return None
        summary = strip_process_narration(str(payload.get("summary") or ""))
        detected_topic = str(payload.get("detected_topic") or "").strip()
        detected_topic = re.sub(r"[（(][^）)]*(?:修正|校正|審核)[^）)]*[）)]", "", detected_topic).strip()
        raw_concepts = payload.get("key_concepts")
        if (
            not summary
            or _study_text_quality_issue(summary, max_length=1200)
            or not detected_topic
            or _study_text_quality_issue(detected_topic, max_length=120)
            or not isinstance(raw_concepts, list)
        ):
            return None
        page_transcriptions = {
            int(page.get("image_index") or 0): " ".join(str(page.get("transcription") or "").split())
            for page in source_pages
            if isinstance(page, dict)
        }
        visual_regions_by_id = {
            str(region.get("region_id") or ""): region
            for page in source_pages
            if isinstance(page, dict)
            for region in page.get("visual_regions") or []
            if isinstance(region, dict) and str(region.get("region_id") or "")
        }
        valid_coverage_ids = {item["id"] for item in _study_source_coverage_items(source_pages)}
        prepared_concepts: List[Dict[str, Any]] = []
        correction_records: List[Dict[str, Any]] = []
        rejected_corrupted_content = False
        for item in raw_concepts:
            if not isinstance(item, dict):
                continue
            concept = str(item.get("concept") or "").strip()
            concept = re.sub(r"[（(][^）)]*(?:修正|校正|審核)[^）)]*[）)]", "", concept).strip()
            recall_cue = normalize_math_markup(strip_process_narration(str(item.get("recall_cue") or "").strip()))
            core_summary = normalize_math_markup(strip_process_narration(str(item.get("core_summary") or "").strip()))
            explanation = normalize_math_markup(strip_process_narration(str(item.get("explanation") or "").strip()))
            card_type = "example" if item.get("card_type") == "example" else "concept"
            content_kind = str(item.get("content_kind") or "").strip().lower()
            if content_kind not in {
                "definition",
                "concept",
                "procedure",
                "comparison",
                "formula",
                "code",
                "example",
                "fact",
                "visual",
            }:
                content_kind = "example" if card_type == "example" else "concept"
            example_problem = normalize_math_markup(strip_process_narration(str(item.get("example_problem") or "").strip()))
            example_method = normalize_math_markup(strip_process_narration(str(item.get("example_method") or "").strip()))
            simple_example = normalize_math_markup(strip_process_narration(str(item.get("simple_example") or "").strip()))
            memory_hint = normalize_math_markup(strip_process_narration(str(item.get("memory_hint") or "").strip()))
            common_confusion = normalize_math_markup(strip_process_narration(str(item.get("common_confusion") or "").strip()))
            reasoning_steps = [
                normalize_math_markup(strip_process_narration(str(step or "").strip()))
                for step in (item.get("reasoning_steps") or [])[:8]
                if str(step or "").strip()
            ] if isinstance(item.get("reasoning_steps"), list) else []
            topic = _normalize_study_concept_title(item.get("topic"), detected_topic)
            concept = _normalize_study_concept_title(concept, topic or detected_topic)
            quality_issues = (
                _study_text_quality_issue(concept, max_length=120),
                _study_text_quality_issue(recall_cue, max_length=180),
                _study_text_quality_issue(core_summary, max_length=320),
                _study_text_quality_issue(explanation, max_length=900),
                _study_text_quality_issue(example_problem, max_length=420) if example_problem else None,
                _study_text_quality_issue(example_method, max_length=340) if example_method else None,
                _study_text_quality_issue(simple_example, max_length=420) if simple_example else None,
                _study_text_quality_issue(memory_hint, max_length=240) if memory_hint else None,
                _study_text_quality_issue(common_confusion, max_length=240) if common_confusion else None,
                *(_study_text_quality_issue(step, max_length=220) for step in reasoning_steps),
                _study_text_quality_issue(topic, max_length=80),
            )
            source_bound_card_text = " ".join(
                [
                    core_summary,
                    explanation,
                    example_problem,
                    example_method,
                    common_confusion,
                    *reasoning_steps,
                ]
            )
            if any(quality_issues):
                rejected_corrupted_content = True
                continue
            if card_type == "example" and example_problem and not example_method:
                # Keep a clearly detected source example even when the model
                # did not find a worked solution. Never invent a method just
                # to satisfy the card schema.
                example_method = "來源未提供完整解法"
                source_bound_card_text = " ".join(
                    [core_summary, explanation, example_problem, example_method, *reasoning_steps]
                )
            if card_type == "example" and not example_problem:
                continue
            if card_type != "example":
                example_problem = ""
                example_method = ""
            else:
                simple_example = ""
            related_concepts = item.get("related_concepts")
            search_keywords = [
                " ".join(str(value or "").split()).strip()[:40]
                for value in (item.get("search_keywords") or [])[:8]
                if str(value or "").strip()
            ] if isinstance(item.get("search_keywords"), list) else []
            source_refs: List[Dict[str, Any]] = []
            for source_ref in item.get("source_refs") or []:
                if not isinstance(source_ref, dict):
                    continue
                try:
                    image_index = int(source_ref.get("image_index") or 0)
                except (TypeError, ValueError):
                    continue
                evidence = _literal_study_source_evidence(source_ref.get("evidence"))
                page_resolution = _resolve_study_source_page(
                    evidence,
                    source_pages,
                    preferred_image_index=image_index,
                    context=" ".join(
                        (
                            concept,
                            topic,
                            recall_cue,
                            core_summary,
                            source_bound_card_text,
                        )
                    ),
                )
                if not page_resolution:
                    continue
                image_index = int(page_resolution["image_index"])
                source_refs.append({"image_index": image_index, "evidence": evidence[:240]})
            visual_refs: List[Dict[str, Any]] = []
            for visual_ref in item.get("visual_refs") or []:
                if not isinstance(visual_ref, dict):
                    continue
                region_id = str(visual_ref.get("region_id") or "").strip()
                region = visual_regions_by_id.get(region_id)
                if region is None or any(
                    existing["region_id"] == region_id for existing in visual_refs
                ):
                    continue
                visual_refs.append(
                    {
                        "region_id": region_id,
                        "image_index": int(region.get("image_index") or 0),
                        "region_type": str(region.get("region_type") or "other"),
                        "title": str(region.get("title") or "")[:100],
                        "description": str(region.get("description") or "")[:700],
                        "visible_text": str(region.get("visible_text") or "")[:800],
                        "bbox": region.get("bbox"),
                        "nodes": region.get("nodes") or [],
                        "edges": region.get("edges") or [],
                        "confidence": str(region.get("confidence") or "medium"),
                        "render_mode": str(region.get("render_mode") or "crop"),
                    }
                )
            correction = item.get("correction") if isinstance(item.get("correction"), dict) else {}
            correction_applied = bool(correction.get("applied"))
            correction_original = " ".join(str(correction.get("original") or "").split()).strip()
            correction_corrected = " ".join(str(correction.get("corrected") or "").split()).strip()
            correction_reason = " ".join(str(correction.get("reason") or "").split()).strip()
            coverage_ids = [
                str(value).strip()
                for value in (item.get("coverage_ids") or [])
                if str(value).strip() in valid_coverage_ids
            ] if isinstance(item.get("coverage_ids"), list) else []
            keyword_corpus = _canonical_study_source_match_text(
                " ".join(
                    [
                        concept,
                        topic,
                        recall_cue,
                        source_bound_card_text,
                        *(source_ref["evidence"] for source_ref in source_refs),
                        *(
                            " ".join(
                                (
                                    visual_ref["title"],
                                    visual_ref["description"],
                                    visual_ref["visible_text"],
                                )
                            )
                            for visual_ref in visual_refs
                        ),
                    ]
                )
            )
            search_keywords = [
                keyword
                for keyword in search_keywords
                if len(_canonical_study_source_match_text(keyword)) >= 2
                and _canonical_study_source_match_text(keyword) in keyword_corpus
            ]
            if (
                concept
                and explanation
                and (source_refs or visual_refs)
                and not has_transpose_dimension_conflict(source_bound_card_text)
                and not has_mapping_signature_conflict(source_bound_card_text, source_refs)
                and not has_invalid_negation_counterexample(source_bound_card_text)
                and not has_matrix_product_dimension_conflict(source_bound_card_text)
                and _is_recall_concept_eligible(
                {"concept": concept, "explanation": explanation, "memory_hint": memory_hint}
                )
            ):
                prepared = {
                    "concept": concept[:80],
                    "recall_cue": recall_cue,
                    "core_summary": core_summary,
                    "explanation": explanation,
                    "card_type": card_type,
                    "content_kind": content_kind,
                    "example_problem": example_problem,
                    "example_method": example_method,
                    "simple_example": simple_example,
                    "reasoning_steps": reasoning_steps,
                    "common_confusion": common_confusion,
                    "memory_hint": memory_hint,
                    "topic": topic[:48] or detected_topic[:48],
                    "note_topic": detected_topic[:80],
                    "source_refs": source_refs[:4],
                    "visual_refs": visual_refs[:6],
                    "coverage_ids": list(dict.fromkeys(coverage_ids))[:8],
                    "related_concepts": [
                        str(value).strip()[:80]
                        for value in related_concepts[:4]
                        if str(value).strip()
                    ] if isinstance(related_concepts, list) else [],
                    "search_keywords": list(dict.fromkeys(search_keywords))[:8],
                }
                prepared_concepts.append(prepared)
                if correction_applied and correction_original and correction_corrected and correction_reason:
                    correction_records.append(
                        {
                            "concept": prepared["concept"],
                            "original": correction_original[:240],
                            "corrected": correction_corrected[:240],
                            "reason": correction_reason[:300],
                            "image_index": (
                                source_refs[0]["image_index"]
                                if source_refs
                                else visual_refs[0]["image_index"]
                            ),
                        }
                    )
        if rejected_corrupted_content:
            app.logger.warning(
                "Discarded one or more corrupted study cards before final validation; kept=%s",
                len(prepared_concepts),
            )
        if not prepared_concepts:
            return None
        prepared_payload = {"key_concepts": prepared_concepts}
        if not _study_recall_page_coverage_met(prepared_payload, source_pages):
            app.logger.warning(
                "Final study-card coverage has non-blocking gaps after filtering: %s",
                _study_recall_coverage_metrics(prepared_payload, source_pages),
            )
        title_lookup = {item["concept"].casefold(): item["concept"] for item in prepared_concepts}
        concepts_by_title = {item["concept"]: item for item in prepared_concepts}
        for item in prepared_concepts:
            normalized_related: List[str] = []
            for related in item["related_concepts"]:
                matched = title_lookup.get(related.casefold())
                if matched and matched != item["concept"] and matched not in normalized_related:
                    normalized_related.append(matched)
            item["related_concepts"] = normalized_related[:4]
        for item in prepared_concepts:
            for related in list(item["related_concepts"]):
                target = concepts_by_title.get(related)
                if target is not None and item["concept"] not in target["related_concepts"]:
                    target["related_concepts"] = (target["related_concepts"] + [item["concept"]])[:4]
        return {
            "detected_topic": detected_topic[:80],
            "summary": summary,
            "key_concepts": prepared_concepts,
            "correction_records": correction_records,
        }

    return (
        _study_source_coverage_items,
        _study_source_page_coverage_plan,
        _study_coverage_evidence_matches,
        _enrich_study_card_coverage_ids,
        _study_recall_coverage_gaps,
        _study_recall_page_coverage_met,
        _study_recall_coverage_metrics,
        _study_recall_coverage_needs_repair,
        _validate_recall_output,
    )
