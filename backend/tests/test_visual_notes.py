import io
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from PIL import Image, ImageDraw

from e3_tracker.api.web import _offset_study_note_batch_analysis, create_app
from e3_tracker.shared.study_note_composer import (
    StudyNoteToolAccumulator,
    StudyNoteToolError,
    build_study_note_tools,
)
from e3_tracker.shared.visual_notes import (
    merge_visual_regions,
    normalize_visual_regions,
    render_visual_region_svg,
    visual_region_crop_box,
)


def _tree_region():
    return {
        "region_id": "p1v1",
        "image_index": 1,
        "region_type": "tree",
        "title": "Binary tree <root>",
        "description": "A points to B and C.",
        "visible_text": "A B C",
        "bbox": {"left": 100, "top": 200, "right": 900, "bottom": 800},
        "nodes": [
            {"id": "a", "label": "A", "x": 500, "y": 180},
            {"id": "b", "label": "B", "x": 300, "y": 680},
            {"id": "c", "label": "C", "x": 700, "y": 680},
        ],
        "edges": [
            {"from": "a", "to": "b", "label": "left"},
            {"from": "a", "to": "c", "label": "right"},
        ],
        "confidence": "high",
        "render_mode": "svg",
    }


class VisualNoteHelperTests(unittest.TestCase):
    def test_batch_offsets_keep_visual_regions_refs_and_coverage_in_sync(self):
        analysis = {
            "source_transcription": [
                {"image_index": 1, "visual_regions": [_tree_region()]}
            ],
            "key_concepts": [
                {
                    "source_refs": [{"image_index": 1, "evidence": "A B C"}],
                    "visual_refs": [{"region_id": "p1v1", "image_index": 1}],
                    "coverage_ids": ["p1b2", "p1v1"],
                }
            ],
        }

        _offset_study_note_batch_analysis(analysis, 8)

        page = analysis["source_transcription"][0]
        card = analysis["key_concepts"][0]
        self.assertEqual(page["image_index"], 9)
        self.assertEqual(page["visual_regions"][0]["region_id"], "p9v1")
        self.assertEqual(card["visual_refs"][0]["region_id"], "p9v1")
        self.assertEqual(card["coverage_ids"], ["p9b2", "p9v1"])

    def test_normalizes_visual_regions_and_rejects_invalid_boxes(self):
        valid = _tree_region()
        invalid = {**valid, "bbox": {"left": 600, "top": 100, "right": 500, "bottom": 200}}

        regions = normalize_visual_regions([valid, invalid], image_index=2)

        self.assertEqual(len(regions), 1)
        self.assertEqual(regions[0]["region_id"], "p2v1")
        self.assertEqual(regions[0]["render_mode"], "svg")
        self.assertEqual(len(regions[0]["edges"]), 2)

    def test_merges_overlapping_audits_without_duplicate_visuals(self):
        initial = _tree_region()
        audited = {
            **_tree_region(),
            "description": "A is the root; B and C are children with visible labels.",
            "bbox": {"left": 105, "top": 195, "right": 905, "bottom": 805},
        }

        regions = merge_visual_regions([[initial], [audited]], image_index=1)

        self.assertEqual(len(regions), 1)
        self.assertEqual(regions[0]["region_id"], "p1v1")
        self.assertIn("children", regions[0]["description"])

    def test_crop_box_uses_normalized_coordinates_without_overflow(self):
        box = visual_region_crop_box(
            _tree_region(), image_width=2000, image_height=1000, padding_ratio=0
        )
        edge_box = visual_region_crop_box(
            {"bbox": {"left": 0, "top": 0, "right": 1000, "bottom": 1000}},
            image_width=640,
            image_height=480,
        )

        self.assertEqual(box, (200, 200, 1800, 800))
        self.assertEqual(edge_box, (0, 0, 640, 480))

    def test_svg_redraw_is_deterministic_and_escapes_model_text(self):
        svg = render_visual_region_svg(_tree_region())

        self.assertIsNotNone(svg)
        self.assertIn("Binary tree &lt;root&gt;", svg)
        self.assertNotIn("<root>", svg)
        self.assertEqual(svg, render_visual_region_svg(_tree_region()))
        self.assertIsNone(render_visual_region_svg({**_tree_region(), "render_mode": "crop"}))


class VisualNoteComposerTests(unittest.TestCase):
    def _accumulator(self):
        return StudyNoteToolAccumulator(
            source_pages=[
                {
                    "image_index": 1,
                    "transcription": "",
                    "visual_regions": [_tree_region()],
                }
            ],
            valid_coverage_ids=("p1v1",),
            required_coverage_ids=("p1v1",),
            coverage_items=(
                {
                    "id": "p1v1",
                    "image_index": 1,
                    "text": "Binary tree A B C",
                    "content_type": "visual",
                    "visual_region_id": "p1v1",
                },
            ),
        )

    def test_enabled_tool_schema_allows_a_pure_visual_block(self):
        tools = build_study_note_tools(max_image_index=2, enable_visual_refs=True)
        block_schema = next(tool for tool in tools if tool["name"] == "add_note_block")[
            "parameters"
        ]

        self.assertIn("visual_refs", block_schema["required"])
        self.assertEqual(block_schema["properties"]["sources"]["minItems"], 0)

    def test_disabled_tool_schema_is_identical_to_the_text_only_contract(self):
        tools = build_study_note_tools(max_image_index=2, enable_visual_refs=False)
        block_schema = next(tool for tool in tools if tool["name"] == "add_note_block")[
            "parameters"
        ]

        self.assertNotIn("visual_refs", block_schema["properties"])
        self.assertEqual(block_schema["properties"]["sources"]["minItems"], 1)

    def test_pure_visual_block_covers_the_exact_region(self):
        accumulator = self._accumulator()
        accumulator.execute(
            "set_note_overview",
            {"detected_topic": "Binary tree", "summary": "Tree structure."},
        )
        accumulator.execute(
            "add_note_block",
            {
                "block_id": "tree-structure",
                "block_type": "visual",
                "title": "Binary tree structure",
                "topic": "Tree traversal",
                "recall_cue": "How are the nodes connected?",
                "key_point": "A has children B and C.",
                "explanation": "The arrows connect root A to B and C.",
                "details": ["A -> B", "A -> C"],
                "example": None,
                "pitfall": None,
                "memory_hint": None,
                "keywords": ["tree", "root"],
                "sources": [],
                "visual_refs": [{"region_id": "p1v1"}],
                "coverage_ids": ["p1v1"],
                "correction": {
                    "applied": False,
                    "original": None,
                    "corrected": None,
                    "reason": None,
                },
            },
        )
        accumulator.execute("finish_note", {"complete": True, "review_note": None})

        card = accumulator.to_legacy_payload()["key_concepts"][0]
        self.assertEqual(card["content_kind"], "visual")
        self.assertEqual(card["source_refs"], [])
        self.assertEqual(card["visual_refs"], [{"region_id": "p1v1", "image_index": 1}])

    def test_visual_coverage_cannot_be_claimed_without_its_region(self):
        accumulator = self._accumulator()
        accumulator.execute(
            "set_note_overview",
            {"detected_topic": "Binary tree", "summary": "Tree structure."},
        )
        with self.assertRaisesRegex(StudyNoteToolError, "literal or visual"):
            accumulator.execute(
                "add_note_block",
                {
                    "block_id": "tree-structure",
                    "block_type": "visual",
                    "title": "Binary tree structure",
                    "topic": "Tree traversal",
                    "recall_cue": None,
                    "key_point": "A has children B and C.",
                    "explanation": "The arrows connect root A to B and C.",
                    "details": [],
                    "example": None,
                    "pitfall": None,
                    "memory_hint": None,
                    "keywords": [],
                    "sources": [],
                    "visual_refs": [{"region_id": "p1v99"}],
                    "coverage_ids": ["p1v1"],
                    "correction": {
                        "applied": False,
                        "original": None,
                        "corrected": None,
                        "reason": None,
                    },
                },
            )


class VisualNoteRouteTests(unittest.TestCase):
    def test_crop_redraw_and_card_render_use_authoritative_region(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            with patch.dict(
                os.environ,
                {
                    "E3_CACHE_DIR": temp_dir,
                    "E3_DATABASE_URL": "",
                    "E3_SESSION_COOKIE_SECURE": "0",
                    "E3_VISUAL_NOTE_PIPELINE": "1",
                },
            ):
                app = create_app()
            storage = app.extensions["e3_storage"]
            try:
                session_id = storage.create_study_recall_session(
                    study_date="2026-08-25",
                    subject="資料結構",
                    title="Tree note",
                    image_filenames=["tree.png"],
                    summary="Tree summary",
                    source_transcription=[
                        {
                            "image_index": 1,
                            "transcription": "A B C",
                            "visual_regions": [_tree_region()],
                        }
                    ],
                    key_concepts=[
                        {
                            "concept": "Binary tree structure",
                            "topic": "Tree traversal",
                            "content_kind": "visual",
                            "card_type": "concept",
                            "recall_cue": "How are A, B and C connected?",
                            "core_summary": "A has children B and C.",
                            "explanation": "The diagram connects A to B and C.",
                            "visual_refs": [_tree_region()],
                            "source_refs": [],
                        }
                    ],
                )
                image_dir = Path(temp_dir) / "study_note_images" / str(session_id)
                image_dir.mkdir(parents=True)
                image = Image.new("RGB", (1000, 800), "white")
                draw = ImageDraw.Draw(image)
                draw.rectangle((100, 160, 900, 640), outline="#cc3344", width=4)
                draw.line((500, 250, 300, 530), fill="#222222", width=6)
                draw.line((500, 250, 700, 530), fill="#222222", width=6)
                image.save(image_dir / "tree.png")

                token = "visual-note-session"
                storage.save_web_session(token, "test-admin")
                anonymous_response = app.test_client().get(
                    f"/admin/study-recall/{session_id}/visual/p1v1/crop"
                )
                client = app.test_client()
                with client.session_transaction() as browser_session:
                    browser_session["username"] = "test-admin"
                    browser_session["session_token"] = token
                    browser_session["is_admin"] = True

                crop_response = client.get(
                    f"/admin/study-recall/{session_id}/visual/p1v1/crop"
                )
                redraw_response = client.get(
                    f"/admin/study-recall/{session_id}/visual/p1v1/redraw"
                )
                page_response = client.get(
                    "/admin/study-recall", query_string={"session_id": session_id}
                )
                search_response = client.get(
                    "/admin/study-recall/search", query_string={"q": "left child B"}
                )

                self.assertEqual(anonymous_response.status_code, 302)
                self.assertIn("/login", anonymous_response.headers["Location"])
                self.assertEqual(crop_response.status_code, 200)
                self.assertEqual(crop_response.mimetype, "image/jpeg")
                with Image.open(io.BytesIO(crop_response.data)) as cropped:
                    self.assertEqual(cropped.size, (810, 488))
                self.assertEqual(redraw_response.status_code, 200)
                self.assertEqual(redraw_response.mimetype, "image/svg+xml")
                self.assertIn("default-src 'none'", redraw_response.headers["Content-Security-Policy"])
                page = page_response.get_data(as_text=True)
                self.assertEqual(page_response.status_code, 200)
                self.assertIn('aria-label="筆記圖像與圖表"', page)
                self.assertIn(f"/admin/study-recall/{session_id}/visual/p1v1/crop", page)
                self.assertIn("結構重繪", page)
                search_payload = search_response.get_json()
                self.assertEqual(search_response.status_code, 200)
                self.assertTrue(search_payload["results"])
                self.assertEqual(search_payload["results"][0]["session_id"], session_id)
            finally:
                storage._engine.dispose()

    def test_feature_flag_can_disable_the_new_pipeline(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            with patch.dict(
                os.environ,
                {
                    "E3_CACHE_DIR": temp_dir,
                    "E3_DATABASE_URL": "",
                    "E3_SESSION_COOKIE_SECURE": "0",
                    "E3_VISUAL_NOTE_PIPELINE": "0",
                },
            ):
                app = create_app()
            try:
                self.assertFalse(app.extensions["visual_note_pipeline_enabled"])
            finally:
                app.extensions["e3_storage"]._engine.dispose()


if __name__ == "__main__":
    unittest.main()
