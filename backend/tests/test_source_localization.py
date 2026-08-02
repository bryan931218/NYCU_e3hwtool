import unittest

from PIL import Image, ImageDraw, ImageFont

from e3_tracker.shared.source_localization import (
    SOURCE_BBOX_VERSION,
    assign_transcription_to_source_sections,
    build_source_page_geometry,
    canonicalize_source_text,
    collapse_source_refs_by_image,
    detect_source_horizontal_separator_candidates,
    estimated_source_line_count,
    literal_source_evidence,
    match_source_evidence_to_lines,
    match_source_evidence_to_sections,
    match_source_evidence_via_page_alignment,
    resolve_source_evidence_page,
    source_bbox_from_lines,
    source_line_match_is_verified,
    source_line_match_is_candidate,
    source_page_alignment_match_is_candidate,
    source_page_alignment_match_is_verified,
    source_section_match_is_candidate,
    source_section_match_is_verified,
    source_bbox_span_is_plausible,
    validated_source_bbox,
)


class SourceLocalizationTests(unittest.TestCase):
    def test_canonicalization_preserves_formula_relations(self) -> None:
        canonical = canonicalize_source_text(
            r"\(T:V \to W,\ x \in V,\ U \subseteq V,\ A \neq B\)"
        )
        self.assertIn("→", canonical)
        self.assertIn("∈", canonical)
        self.assertIn("⊆", canonical)
        self.assertIn("≠", canonical)
        self.assertEqual(
            canonicalize_source_text(r"[A\mid b]"),
            canonicalize_source_text("[A|b]"),
        )
        self.assertEqual(
            canonicalize_source_text(r"A\Longleftrightarrow B"),
            canonicalize_source_text("A⇔B"),
        )

    def test_visual_page_description_is_not_locatable(self) -> None:
        self.assertEqual(
            literal_source_evidence("（頁中列出一個矩陣例子示意證明步驟）"),
            "",
        )
        self.assertEqual(
            literal_source_evidence("右側原稿以列交換示意證明"),
            "",
        )

    def test_meta_parenthetical_is_removed_from_literal_evidence(self) -> None:
        self.assertEqual(
            literal_source_evidence("rank(A)=2（下方有數值例子示意）"),
            "rank(A)=2",
        )

    def test_exact_formula_span_is_selected_and_verified(self) -> None:
        lines = [
            {"line_id": 1, "text": r"rank(A)=2", "confidence": 90},
            {"line_id": 2, "text": r"rank(A)=rank([A\mid b])", "confidence": 91},
            {"line_id": 3, "text": r"\Rightarrow Ax=b 有解", "confidence": 92},
            {"line_id": 4, "text": r"rank(A)\neq rank([A\mid b])", "confidence": 93},
        ]
        evidence = r"rank(A)=rank([A\mid b]) \Rightarrow Ax=b 有解"
        selected, metrics = match_source_evidence_to_lines(evidence, lines)
        self.assertEqual([line["line_id"] for line in selected], [2, 3])
        self.assertTrue(source_line_match_is_verified(evidence, metrics))

    def test_shared_keyword_does_not_verify_wrong_formula(self) -> None:
        lines = [
            {"line_id": 1, "text": r"rank(A)=2", "confidence": 90},
            {"line_id": 2, "text": r"rank(A)\leq \min(m,n)", "confidence": 90},
        ]
        evidence = r"rank(A)=rank([A\mid b]) \Rightarrow Ax=b 有唯一解"
        _selected, metrics = match_source_evidence_to_lines(evidence, lines)
        self.assertFalse(source_line_match_is_verified(evidence, metrics))

    def test_only_text_verified_v6_bbox_is_trusted_for_display(self) -> None:
        old_bbox = {
            "left": 100,
            "top": 100,
            "right": 400,
            "bottom": 300,
            "confidence": 90,
            "version": 5,
        }
        self.assertIsNotNone(validated_source_bbox(old_bbox))
        self.assertIsNone(validated_source_bbox(old_bbox, require_text_verified=True))

        stale_verified_bbox = {
            **old_bbox,
            "version": 6,
            "text_verified": True,
            "match_score": 0.95,
            "match_coverage": 0.95,
            "boundary_coverage": 0.95,
            "evidence_length": 38,
        }
        self.assertIsNone(
            validated_source_bbox(stale_verified_bbox, require_text_verified=True)
        )
        stale_verified_bbox["version"] = 8
        self.assertIsNone(
            validated_source_bbox(stale_verified_bbox, require_text_verified=True)
        )

        verified_bbox = {
            **old_bbox,
            "version": SOURCE_BBOX_VERSION,
            "text_verified": True,
            "match_score": 0.83,
            "match_coverage": 0.91,
            "boundary_coverage": 0.84,
            "evidence_length": 38,
            "expected_lines": 2,
            "span_verified": True,
            "crop_verified": True,
            "crop_match_score": 0.84,
            "crop_match_coverage": 0.92,
            "crop_boundary_coverage": 0.85,
            "crop_match_precision": 0.88,
            "geometry_verified": True,
            "page_verified": True,
            "source_image_index": 1,
            "page_match_kind": "unique_exact",
            "page_match_margin": 1.0,
            "formula_coverage": 0.92,
            "formula_token_count": 4,
            "uniqueness": 0.88,
            "segmentation_stability": 0.90,
            "localization_method": "section_ocr_rag",
        }
        self.assertIsNotNone(
            validated_source_bbox(
                verified_bbox,
                require_text_verified=True,
                expected_image_index=1,
            )
        )
        self.assertIsNone(
            validated_source_bbox(
                verified_bbox,
                require_text_verified=True,
                expected_image_index=2,
            )
        )
        representation_variant = {
            **verified_bbox,
            "formula_coverage": 0.60,
            "match_coverage": 0.90,
            "boundary_coverage": 0.88,
            "crop_match_coverage": 0.91,
            "crop_boundary_coverage": 0.89,
        }
        self.assertIsNotNone(
            validated_source_bbox(representation_variant, require_text_verified=True)
        )
        transcription_fallback = {
            **verified_bbox,
            "confidence": 76,
            "crop_verified": False,
            "transcription_fallback_verified": True,
            "transcription_isolated": True,
            "localization_method": "section_transcription_fallback",
        }
        self.assertIsNotNone(
            validated_source_bbox(
                transcription_fallback,
                require_text_verified=True,
                expected_image_index=1,
            )
        )
        transcription_fallback["transcription_isolated"] = False
        self.assertIsNone(
            validated_source_bbox(
                transcription_fallback,
                require_text_verified=True,
            )
        )

    def test_unverified_v6_bbox_is_rejected(self) -> None:
        bbox = {
            "left": 100,
            "top": 100,
            "right": 400,
            "bottom": 300,
            "confidence": 90,
            "version": SOURCE_BBOX_VERSION,
            "text_verified": True,
            "match_score": 0.82,
            "match_coverage": 0.41,
            "boundary_coverage": 0.80,
            "evidence_length": 38,
            "coordinate_agreement": 0.88,
            "expected_lines": 2,
            "span_verified": True,
            "crop_verified": True,
            "crop_match_score": 0.84,
            "crop_match_coverage": 0.92,
            "crop_boundary_coverage": 0.85,
            "crop_match_precision": 0.88,
        }
        self.assertIsNone(validated_source_bbox(bbox))

    def test_alignment_bbox_requires_same_page_span_in_blind_crop(self) -> None:
        bbox = {
            "left": 40,
            "top": 100,
            "right": 940,
            "bottom": 360,
            "confidence": 82,
            "version": SOURCE_BBOX_VERSION,
            "text_verified": True,
            "match_score": 0.44,
            "match_coverage": 0.46,
            "boundary_coverage": 0.42,
            "evidence_length": 82,
            "expected_lines": 4,
            "span_verified": True,
            "crop_verified": True,
            "crop_match_score": 0.45,
            "crop_match_coverage": 0.47,
            "crop_boundary_coverage": 0.43,
            "crop_match_precision": 0.10,
            "geometry_verified": True,
            "page_verified": True,
            "source_image_index": 1,
            "page_match_kind": "unique_exact",
            "page_match_margin": 1.0,
            "formula_coverage": 0.55,
            "formula_token_count": 12,
            "uniqueness": 0.76,
            "segmentation_stability": 0.88,
            "localization_method": "section_ocr_alignment",
            "alignment_verified": True,
            "alignment_score": 0.69,
            "alignment_evidence_coverage": 0.42,
            "alignment_interval_coverage": 1.0,
            "alignment_context_coverage": 0.58,
            "alignment_section_coverage": 0.74,
            "alignment_page_coverage": 0.66,
            "alignment_expected_span_agreement": 1.0,
            "crop_alignment_score": 0.67,
            "crop_alignment_evidence_coverage": 0.40,
            "crop_alignment_interval_coverage": 1.0,
            "crop_alignment_context_coverage": 0.55,
            "crop_alignment_section_coverage": 0.70,
            "crop_alignment_page_coverage": 0.64,
            "crop_alignment_expected_span_agreement": 1.0,
        }
        self.assertIsNotNone(
            validated_source_bbox(bbox, require_text_verified=True)
        )
        bbox["crop_alignment_expected_span_agreement"] = 0.22
        self.assertIsNone(
            validated_source_bbox(bbox, require_text_verified=True)
        )

    def test_reanchored_bbox_requires_a_verified_literal_anchor(self) -> None:
        bbox = {
            "left": 40,
            "top": 100,
            "right": 940,
            "bottom": 360,
            "confidence": 84,
            "version": SOURCE_BBOX_VERSION,
            "text_verified": True,
            "match_score": 0.88,
            "match_coverage": 0.92,
            "boundary_coverage": 0.86,
            "evidence_length": 36,
            "expected_lines": 2,
            "span_verified": True,
            "crop_verified": True,
            "crop_match_score": 0.87,
            "crop_match_coverage": 0.91,
            "crop_boundary_coverage": 0.84,
            "crop_match_precision": 0.08,
            "geometry_verified": True,
            "page_verified": True,
            "source_image_index": 1,
            "page_match_kind": "unique_exact",
            "page_match_margin": 1.0,
            "formula_coverage": 0.94,
            "formula_token_count": 5,
            "uniqueness": 0.82,
            "segmentation_stability": 0.90,
            "localization_method": "section_ocr_reanchored",
            "anchor_verified": True,
            "localization_anchor": r"rank(A)=n，所以 Ax=0 只有零解",
        }
        self.assertIsNotNone(
            validated_source_bbox(bbox, require_text_verified=True)
        )
        bbox["anchor_verified"] = False
        self.assertIsNone(
            validated_source_bbox(bbox, require_text_verified=True)
        )

    def test_current_bbox_requires_blind_crop_verification(self) -> None:
        bbox = {
            "left": 100,
            "top": 100,
            "right": 400,
            "bottom": 180,
            "confidence": 84,
            "version": SOURCE_BBOX_VERSION,
            "text_verified": True,
            "match_score": 0.90,
            "match_coverage": 0.94,
            "boundary_coverage": 0.88,
            "evidence_length": 38,
            "coordinate_agreement": 0.91,
            "expected_lines": 1,
            "span_verified": True,
            "crop_verified": False,
            "crop_match_score": 0.0,
            "crop_match_coverage": 0.0,
            "crop_boundary_coverage": 0.0,
            "crop_match_precision": 0.0,
        }
        self.assertIsNone(validated_source_bbox(bbox, require_text_verified=True))

    def test_short_bbox_rejects_crop_with_too_much_adjacent_text(self) -> None:
        bbox = {
            "left": 100,
            "top": 100,
            "right": 400,
            "bottom": 180,
            "confidence": 84,
            "version": SOURCE_BBOX_VERSION,
            "text_verified": True,
            "match_score": 0.90,
            "match_coverage": 1.0,
            "boundary_coverage": 1.0,
            "evidence_length": 14,
            "coordinate_agreement": 0.91,
            "expected_lines": 1,
            "span_verified": True,
            "crop_verified": True,
            "crop_match_score": 0.92,
            "crop_match_coverage": 1.0,
            "crop_boundary_coverage": 1.0,
            "crop_match_precision": 0.24,
        }
        self.assertIsNone(validated_source_bbox(bbox, require_text_verified=True))

    def test_bbox_height_must_fit_evidence_line_count(self) -> None:
        self.assertEqual(estimated_source_line_count("rank(A)=n"), 1)
        self.assertFalse(
            source_bbox_span_is_plausible(
                "rank(A)=n",
                {"top": 100, "bottom": 300},
            )
        )
        numbered = "(1) A 可逆 (2) Ax=0 只有零解 (3) rank(A)=n"
        self.assertGreaterEqual(estimated_source_line_count(numbered), 3)
        self.assertTrue(
            source_bbox_span_is_plausible(
                numbered,
                {"top": 100, "bottom": 360},
            )
        )

    def test_source_bbox_expands_outside_section_content(self) -> None:
        bbox = source_bbox_from_lines(
            [{"left": 100, "top": 200, "right": 900, "bottom": 420}]
        )
        self.assertEqual(
            bbox,
            {"left": 82, "top": 185, "right": 918, "bottom": 435},
        )

    def test_isolated_transcription_is_assigned_to_sections_in_order(self) -> None:
        sections = [
            {"line_id": 1, "left": 20, "top": 0, "right": 980, "bottom": 300},
            {"line_id": 2, "left": 20, "top": 310, "right": 980, "bottom": 650},
            {"line_id": 3, "left": 20, "top": 660, "right": 980, "bottom": 1000},
        ]
        assigned = assign_transcription_to_source_sections(
            "第一區定義\n第一區公式\n第二區例題\n第二區步驟\n第三區結論\n第三區補充",
            sections,
        )
        self.assertEqual(len(assigned), 3)
        self.assertIn("第一區", assigned[0]["text"])
        self.assertIn("第二區", assigned[1]["text"])
        self.assertIn("第三區", assigned[2]["text"])
        self.assertTrue(all(item["transcription_isolated"] for item in assigned))

    def test_unique_literal_evidence_repairs_wrong_page_assignment(self) -> None:
        pages = [
            {
                "image_index": 1,
                "transcription": r"可逆矩陣滿足 \(AA^{-1}=I\)，且解唯一。",
            },
            {
                "image_index": 2,
                "transcription": r"秩與零空間：\(n=rank(A)+nullity(A)\)。",
            },
        ]
        resolved = resolve_source_evidence_page(
            r"\(AA^{-1}=I\)",
            pages,
            preferred_image_index=2,
            context="可逆矩陣與唯一解",
        )
        self.assertIsNotNone(resolved)
        self.assertEqual(resolved["image_index"], 1)
        self.assertEqual(resolved["match_kind"], "unique_exact")

    def test_repeated_formula_without_distinguishing_context_is_rejected(self) -> None:
        pages = [
            {"image_index": 1, "transcription": r"定義一：\(Ax=b\)。"},
            {"image_index": 2, "transcription": r"例題二：\(Ax=b\)。"},
        ]
        self.assertIsNone(
            resolve_source_evidence_page(
                r"\(Ax=b\)",
                pages,
                preferred_image_index=2,
            )
        )

    def test_repeated_formula_uses_card_context_to_choose_page(self) -> None:
        pages = [
            {
                "image_index": 1,
                "transcription": r"線性方程組的係數矩陣與增廣矩陣，\(Ax=b\)。",
            },
            {
                "image_index": 2,
                "transcription": r"最小平方法與投影矩陣，法方程式寫成 \(Ax=b\)。",
            },
        ]
        resolved = resolve_source_evidence_page(
            r"\(Ax=b\)",
            pages,
            preferred_image_index=1,
            context="最小平方法 投影矩陣 法方程式",
        )
        self.assertIsNotNone(resolved)
        self.assertEqual(resolved["image_index"], 2)

    def test_multiple_literal_fragments_override_a_polluted_page_hint(self) -> None:
        pages = [
            {
                "image_index": 1,
                "transcription": "秩的重要性質：rank(A)=rank(A^T)。",
            },
            {
                "image_index": 2,
                "transcription": (
                    "若 rank(A)=1，則存在 x,y 使 A=xy^T。"
                    "已知 rank(A)=dim(RS(A))=1，因此 A 有某一非零列，"
                    "其他列為此列之倍數。"
                ),
            },
            {
                "image_index": 3,
                "transcription": "若 AB=0，則 rank(A)+rank(B) 不超過中間維度。",
            },
        ]
        resolved = resolve_source_evidence_page(
            (
                "Ex. A_{4×5}, rank=1 則存在 x,y 使 A=xy^T。"
                "Pf. 已知 rank(A)=dim(RS(A))=1；A 有某一非零列，"
                "且其他列為此列之倍數。"
            ),
            pages,
            preferred_image_index=1,
        )
        self.assertIsNotNone(resolved)
        self.assertEqual(resolved["image_index"], 2)
        self.assertEqual(resolved["match_kind"], "fragment_consensus")

    def test_fragments_on_different_pages_do_not_form_false_consensus(self) -> None:
        pages = [
            {"image_index": 1, "transcription": "已知 rank(A)=1。"},
            {"image_index": 2, "transcription": "其他列為某一非零列的倍數。"},
        ]
        self.assertIsNone(
            resolve_source_evidence_page(
                "Pf. 已知 rank(A)=1；其他列為某一非零列的倍數。",
                pages,
                preferred_image_index=1,
            )
        )

    def test_source_display_keeps_one_ref_per_image_and_prefers_bbox(self) -> None:
        collapsed = collapse_source_refs_by_image(
            [
                {"image_index": 1, "evidence": "first", "bbox": None},
                {
                    "image_index": 1,
                    "evidence": "located",
                    "bbox": {"confidence": 82},
                },
                {"image_index": 2, "evidence": "other", "bbox": None},
            ]
        )
        self.assertEqual([item["image_index"] for item in collapsed], [1, 2])
        self.assertEqual(collapsed[0]["evidence"], "located")

    def test_repeated_formula_is_not_treated_as_unique_location(self) -> None:
        lines = [
            {"line_id": 1, "left": 100, "top": 100, "right": 420, "bottom": 150, "text": r"rank(A)=n"},
            {"line_id": 2, "left": 100, "top": 500, "right": 420, "bottom": 550, "text": r"rank(A)=n"},
        ]
        _selected, metrics = match_source_evidence_to_lines(r"rank(A)=n", lines)
        self.assertLess(metrics["uniqueness"], 0.24)
        self.assertFalse(source_line_match_is_candidate(r"rank(A)=n", metrics))

    def test_long_section_context_does_not_hide_literal_formula(self) -> None:
        sections = [
            {
                "line_id": 1,
                "text": (
                    "座標變換的完整說明與例題。"
                    r"對任意 v，皆有 [v]_{\beta'}=P[v]_{\beta}。"
                    "接著列出矩陣計算與複習提醒。"
                ),
            },
            {"line_id": 2, "text": r"另一區塊只有 rank(A)=n。"},
        ]
        evidence = r"[v]_{\beta'}=P[v]_{\beta}"
        selected, metrics = match_source_evidence_to_sections(evidence, sections)
        self.assertEqual([section["line_id"] for section in selected], [1])
        self.assertTrue(source_section_match_is_candidate(evidence, metrics))
        self.assertTrue(source_section_match_is_verified(evidence, metrics))

    def test_repeated_formula_in_two_sections_is_not_unique(self) -> None:
        sections = [
            {"line_id": 1, "text": r"定理一：rank(A)=n"},
            {"line_id": 2, "text": r"例題再次使用 rank(A)=n"},
        ]
        evidence = r"rank(A)=n"
        _selected, metrics = match_source_evidence_to_sections(evidence, sections)
        self.assertLess(metrics["uniqueness"], 0.20)
        self.assertFalse(source_section_match_is_candidate(evidence, metrics))

    def test_page_alignment_recovers_formula_ocr_representation_change(self) -> None:
        page = (
            "秩的定義 rank(A) 是列梯陣的非零列個數。"
            r"例 A=\begin{bmatrix}1&2&3\\2&4&6\\0&0&1\end{bmatrix}"
            r"\rightarrow\begin{bmatrix}1&2&3\\0&0&1\\0&0&0\end{bmatrix}"
            r"\Rightarrow rank(A)=2。"
            r"性質 rank(A)\leq\min(m,n)。"
        )
        evidence = (
            r"A=\begin{bmatrix}1&2&3\\2&4&6\\0&0&1\end{bmatrix}"
            r"\rightarrow\begin{bmatrix}1&2&3\\0&0&1\\0&0&0\end{bmatrix}"
            r"\Rightarrow rank(A)=2"
        )
        sections = [
            {
                "line_id": 1,
                "text": (
                    "秩的定義 rank(A) 是列梯陣的非零列個數。"
                    "例 A=[1 2 3; 2 4 6; 0 0 1] → "
                    "[1 2 3; 0 0 1; 0 0 0]，所以秩為 2。"
                ),
            },
            {"line_id": 2, "text": r"性質 rank(A)\leq\min(m,n)。"},
        ]
        selected, metrics = match_source_evidence_via_page_alignment(
            evidence,
            page,
            sections,
        )
        self.assertEqual([section["line_id"] for section in selected], [1])
        self.assertTrue(source_page_alignment_match_is_candidate(evidence, metrics))

        crop_selected, crop_metrics = match_source_evidence_via_page_alignment(
            evidence,
            page,
            [{"line_id": 1, "text": sections[0]["text"]}],
            expected_source_span=(
                int(metrics["alignment_source_start"]),
                int(metrics["alignment_source_end"]),
            ),
        )
        self.assertEqual([section["line_id"] for section in crop_selected], [1])
        self.assertTrue(source_page_alignment_match_is_verified(evidence, crop_metrics))

    def test_page_alignment_rejects_repeated_ambiguous_formula(self) -> None:
        page = r"第一區 rank(A)=n。第二區再次寫 rank(A)=n。"
        sections = [
            {"line_id": 1, "text": r"第一區 rank(A)=n。"},
            {"line_id": 2, "text": r"第二區再次寫 rank(A)=n。"},
        ]
        evidence = r"rank(A)=n"
        _selected, metrics = match_source_evidence_via_page_alignment(
            evidence,
            page,
            sections,
        )
        self.assertLess(metrics["alignment_uniqueness"], 0.20)
        self.assertFalse(source_page_alignment_match_is_candidate(evidence, metrics))

    def test_separator_detector_returns_dashed_line_pixel_position(self) -> None:
        image = Image.new("RGB", (800, 1000), "white")
        draw = ImageDraw.Draw(image)
        try:
            font = ImageFont.truetype("C:/Windows/Fonts/arial.ttf", 32)
        except OSError:
            font = ImageFont.load_default()
        draw.text((60, 120), "rank(A)=n", fill="black", font=font)
        for left in range(35, 765, 55):
            draw.line((left, 410, left + 30, 410), fill="black", width=4)
        draw.text((60, 520), "Ax=b", fill="black", font=font)
        candidates = detect_source_horizontal_separator_candidates(image)
        separator = min(candidates, key=lambda item: abs(int(item["y"]) - 410))
        self.assertLessEqual(abs(int(separator["y"]) - 410), 8)
        self.assertGreater(float(separator["separator_likelihood"]), 0.30)

    def test_two_column_match_skips_unrelated_neighbor_column(self) -> None:
        lines = [
            {"line_id": 1, "left": 80, "top": 100, "right": 430, "bottom": 145, "text": "線性條件"},
            {"line_id": 2, "left": 560, "top": 105, "right": 900, "bottom": 150, "text": "右欄例題"},
            {"line_id": 3, "left": 80, "top": 160, "right": 430, "bottom": 205, "text": r"T(u+v)=T(u)+T(v)"},
            {"line_id": 4, "left": 560, "top": 165, "right": 900, "bottom": 210, "text": "另一個答案"},
        ]
        selected, _metrics = match_source_evidence_to_lines(
            r"線性條件 T(u+v)=T(u)+T(v)",
            lines,
        )
        self.assertEqual([line["line_id"] for line in selected], [1, 3])

    def test_geometry_uses_ink_rows_and_ignores_toolbar_and_rule(self) -> None:
        image = Image.new("RGB", (800, 1000), "white")
        draw = ImageDraw.Draw(image)
        draw.rectangle((0, 0, 800, 90), fill=(20, 88, 176))
        try:
            font = ImageFont.truetype("C:/Windows/Fonts/arial.ttf", 34)
        except OSError:
            font = ImageFont.load_default()
        draw.text((90, 160), "rank(A)=n", fill="black", font=font)
        draw.text((90, 235), "Ax=0", fill="black", font=font)
        for left in range(50, 750, 70):
            draw.line((left, 330, left + 32, 330), fill="black", width=3)
        geometry = build_source_page_geometry(image)
        self.assertGreaterEqual(len(geometry), 2)
        self.assertTrue(all(line["top"] >= 90 for line in geometry))
        self.assertTrue(any(130 <= line["top"] <= 210 for line in geometry))
        self.assertFalse(any(line["right"] - line["left"] > 700 and line["bottom"] - line["top"] < 20 for line in geometry))


if __name__ == "__main__":
    unittest.main()
