import unittest
import json
import os
import tempfile
from unittest.mock import Mock, patch

from e3_tracker.api.web import create_app
from e3_tracker.shared.study_note_composer import (
    CONTENT_KINDS,
    StudyNoteToolAccumulator,
    StudyNoteToolError,
    build_study_note_tools,
    run_study_note_tool_conversation,
)


class StudyNoteComposerTests(unittest.TestCase):
    SUBJECT_CASES = (
        (
            "線性代數",
            "formula",
            "矩陣乘法條件",
            "若 A 為 m×n，B 為 n×p，則 AB 為 m×p。",
        ),
        (
            "離散數學",
            "definition",
            "單射定義",
            "不同輸入會得到不同輸出。",
        ),
        (
            "資料結構",
            "code",
            "堆疊 push",
            "push 將元素加入堆疊頂端。",
        ),
        (
            "作業系統",
            "comparison",
            "行程與執行緒",
            "同一行程內的執行緒共享位址空間。",
        ),
        (
            "計算機組織",
            "procedure",
            "指令週期",
            "指令依序經過擷取、解碼與執行。",
        ),
        (
            "演算法",
            "procedure",
            "廣度優先搜尋",
            "使用佇列逐層拜訪相鄰節點。",
        ),
    )

    def _build_accumulator(
        self,
        transcription,
        coverage_ids=("p1b1",),
        required_coverage_ids=(),
        coverage_items=(),
    ):
        return StudyNoteToolAccumulator(
            source_pages=[
                {
                    "image_index": 1,
                    "transcription": transcription,
                    "uncertain_fragments": [],
                }
            ],
            valid_coverage_ids=coverage_ids,
            required_coverage_ids=required_coverage_ids,
            coverage_items=coverage_items,
        )

    def _add_block(self, accumulator, *, block_type, title, evidence, **overrides):
        arguments = {
            "block_id": overrides.pop("block_id", "block-1"),
            "block_type": block_type,
            "title": title,
            "topic": overrides.pop("topic", title),
            "recall_cue": overrides.pop("recall_cue", None),
            "key_point": overrides.pop("key_point", evidence),
            "explanation": overrides.pop("explanation", evidence),
            "details": overrides.pop("details", []),
            "example": overrides.pop("example", None),
            "pitfall": overrides.pop("pitfall", None),
            "memory_hint": overrides.pop("memory_hint", None),
            "keywords": overrides.pop("keywords", [title]),
            "sources": overrides.pop(
                "sources", [{"image_index": 1, "evidence": evidence}]
            ),
            "coverage_ids": overrides.pop("coverage_ids", ["p1b1"]),
            "correction": overrides.pop(
                "correction",
                {"applied": False, "original": None, "corrected": None, "reason": None},
            ),
        }
        arguments.update(overrides)
        return accumulator.execute("add_note_block", arguments)

    def test_every_subject_uses_the_same_composer_without_a_forced_example(self):
        for subject, content_kind, title, evidence in self.SUBJECT_CASES:
            with self.subTest(subject=subject):
                accumulator = self._build_accumulator(evidence)
                accumulator.execute(
                    "set_note_overview",
                    {"detected_topic": title, "summary": evidence},
                )
                self._add_block(
                    accumulator,
                    block_type=content_kind,
                    title=title,
                    evidence=evidence,
                )
                accumulator.execute(
                    "finish_note", {"complete": True, "review_note": None}
                )

                payload = accumulator.to_legacy_payload()
                self.assertEqual(payload["organization_mode"], "tool_composer_v1")
                self.assertEqual(len(payload["key_concepts"]), 1)
                card = payload["key_concepts"][0]
                self.assertEqual(card["content_kind"], content_kind)
                self.assertEqual(card["card_type"], "concept")
                self.assertEqual(card["simple_example"], "")
                self.assertEqual(card["source_refs"][0]["evidence"], evidence)

    def test_code_block_preserves_language_and_indentation(self):
        code = (
            "```cpp\n"
            "for (int i = 0; i < n; ++i) {\n"
            "    Node* next = nodes[i]->next;\n"
            "    cout << next->value;\n"
            "}\n"
            "```"
        )
        accumulator = self._build_accumulator(code)
        accumulator.execute(
            "set_note_overview",
            {"detected_topic": "鏈結串列走訪", "summary": "依序走訪每個節點。"},
        )
        self._add_block(
            accumulator,
            block_type="code",
            title="鏈結串列走訪",
            evidence=code,
            key_point="沿著 `next` 逐一走訪節點。",
            explanation=code,
        )
        accumulator.execute("finish_note", {"complete": True, "review_note": None})

        card = accumulator.to_legacy_payload()["key_concepts"][0]

        self.assertEqual(card["content_kind"], "code")
        self.assertEqual(card["explanation"], code)
        self.assertIn("    Node* next", card["explanation"])

    def test_web_validator_keeps_non_math_blocks_without_forced_examples(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            with patch.dict(
                os.environ,
                {
                    "E3_CACHE_DIR": temp_dir,
                    "E3_DATABASE_URL": "",
                    "E3_SESSION_COOKIE_SECURE": "0",
                },
            ):
                app = create_app()
            try:
                validator = app.extensions["study_note_output_validator"]
                for subject, content_kind, title, evidence in self.SUBJECT_CASES:
                    with self.subTest(subject=subject):
                        payload = {
                            "detected_topic": title,
                            "summary": f"{title}的核心筆記整理，內容完全依照來源建立。",
                            "key_concepts": [
                                {
                                    "concept": title,
                                    "content_kind": content_kind,
                                    "card_type": "concept",
                                    "recall_cue": f"如何說明{title}？",
                                    "core_summary": evidence,
                                    "explanation": evidence,
                                    "simple_example": "",
                                    "example_problem": "",
                                    "example_method": "",
                                    "reasoning_steps": [],
                                    "common_confusion": "",
                                    "memory_hint": "",
                                    "topic": title,
                                    "related_concepts": [],
                                    "search_keywords": [title],
                                    "source_refs": [
                                        {"image_index": 1, "evidence": evidence}
                                    ],
                                    "coverage_ids": [],
                                    "correction": {
                                        "applied": False,
                                        "original": "",
                                        "corrected": "",
                                        "reason": "",
                                    },
                                }
                            ],
                        }
                        validated = validator(
                            payload,
                            [{"image_index": 1, "transcription": evidence}],
                        )

                        self.assertIsNotNone(validated)
                        card = validated["key_concepts"][0]
                        self.assertEqual(card["content_kind"], content_kind)
                        self.assertEqual(card["simple_example"], "")
            finally:
                app.extensions["e3_storage"]._engine.dispose()

    def test_web_composer_sends_strict_tools_and_processes_responses_calls(self):
        evidence = "指令依序經過擷取、解碼與執行。"
        with tempfile.TemporaryDirectory() as temp_dir:
            with patch.dict(
                os.environ,
                {
                    "E3_CACHE_DIR": temp_dir,
                    "E3_DATABASE_URL": "",
                    "E3_SESSION_COOKIE_SECURE": "0",
                    "OPENAI_API_KEY": "test-key",
                },
            ):
                app = create_app()
            responses = [
                {
                    "output": [
                        {
                            "type": "function_call",
                            "call_id": "overview",
                            "name": "set_note_overview",
                            "arguments": json.dumps(
                                {"detected_topic": "指令週期", "summary": evidence},
                                ensure_ascii=False,
                            ),
                        },
                        {
                            "type": "function_call",
                            "call_id": "block",
                            "name": "add_note_block",
                            "arguments": json.dumps(
                                {
                                    "block_id": "instruction-cycle",
                                    "block_type": "procedure",
                                    "title": "指令週期",
                                    "topic": "處理器流程",
                                    "recall_cue": None,
                                    "key_point": evidence,
                                    "explanation": evidence,
                                    "details": ["擷取", "解碼", "執行"],
                                    "example": None,
                                    "pitfall": None,
                                    "memory_hint": None,
                                    "keywords": ["擷取", "解碼", "執行"],
                                    "sources": [
                                        {"image_index": 1, "evidence": evidence}
                                    ],
                                    "coverage_ids": ["p1b1"],
                                    "correction": {
                                        "applied": False,
                                        "original": None,
                                        "corrected": None,
                                        "reason": None,
                                    },
                                },
                                ensure_ascii=False,
                            ),
                        },
                    ]
                },
                {
                    "output": [
                        {
                            "type": "function_call",
                            "call_id": "finish",
                            "name": "finish_note",
                            "arguments": '{"complete":true,"review_note":null}',
                        }
                    ]
                },
            ]
            posted_bodies = []

            def fake_post(*_args, **kwargs):
                posted_bodies.append(kwargs["json"])
                response = Mock()
                response.raise_for_status.return_value = None
                response.json.return_value = responses[len(posted_bodies) - 1]
                return response

            try:
                with patch("e3_tracker.api.web.requests.post", side_effect=fake_post):
                    payload = app.extensions["study_note_tool_composer"](
                        subject="計算機組織",
                        source_pages=[
                            {
                                "image_index": 1,
                                "transcription": evidence,
                                "uncertain_fragments": [],
                            }
                        ],
                        coverage_checklist=[
                            {
                                "id": "p1b1",
                                "image_index": 1,
                                "text": evidence,
                                "priority": "required",
                                "content_type": "concept",
                                "is_example": False,
                            }
                        ],
                        allow_corrections=False,
                    )

                self.assertEqual(payload["key_concepts"][0]["content_kind"], "procedure")
                self.assertEqual(len(posted_bodies), 2)
                first_body = posted_bodies[0]
                self.assertEqual(first_body["tool_choice"], "required")
                self.assertTrue(first_body["parallel_tool_calls"])
                self.assertTrue(all(tool["strict"] for tool in first_body["tools"]))
                second_input = posted_bodies[1]["input"]
                self.assertTrue(
                    any(
                        item.get("type") == "function_call_output"
                        and item.get("call_id") == "block"
                        for item in second_input
                    )
                )
            finally:
                app.extensions["e3_storage"]._engine.dispose()

    def test_example_block_keeps_problem_method_and_steps_separate(self):
        transcription = "例題：輸入陣列 [3,1,2]，使用 insertion sort 排序。依序插入元素。"
        accumulator = self._build_accumulator(transcription)
        accumulator.execute(
            "set_note_overview",
            {"detected_topic": "插入排序", "summary": "使用插入排序處理例題。"},
        )
        self._add_block(
            accumulator,
            block_type="example",
            title="插入排序例題",
            evidence=transcription,
            key_point="逐步維持已排序前綴。",
            explanation="把目前元素插入前方已排序區間。",
            details=["從第二個元素開始", "向前尋找插入位置"],
            example="輸入陣列 [3,1,2]，使用 insertion sort 排序。",
        )
        accumulator.execute("finish_note", {"complete": True, "review_note": None})

        card = accumulator.to_legacy_payload()["key_concepts"][0]
        self.assertEqual(card["card_type"], "example")
        self.assertIn("[3,1,2]", card["example_problem"])
        self.assertEqual(card["example_method"], "把目前元素插入前方已排序區間。")
        self.assertEqual(len(card["reasoning_steps"]), 2)
        self.assertEqual(card["simple_example"], "")

    def test_literal_source_evidence_is_required(self):
        accumulator = self._build_accumulator("CPU 依序執行 fetch、decode、execute。")
        accumulator.execute(
            "set_note_overview",
            {"detected_topic": "指令週期", "summary": "指令分階段執行。"},
        )
        with self.assertRaisesRegex(StudyNoteToolError, "literal source evidence"):
            self._add_block(
                accumulator,
                block_type="procedure",
                title="指令週期",
                evidence="來源中不存在的摘要",
            )

    def test_example_type_requires_a_concrete_source_problem(self):
        accumulator = self._build_accumulator("例題：計算 T(n)=T(n/2)+1。")
        accumulator.execute(
            "set_note_overview",
            {"detected_topic": "遞迴式", "summary": "分析遞迴式。"},
        )
        with self.assertRaisesRegex(StudyNoteToolError, "requires a concrete"):
            self._add_block(
                accumulator,
                block_type="example",
                title="遞迴式例題",
                evidence="例題：計算 T(n)=T(n/2)+1。",
            )

    def test_links_may_reference_blocks_added_later(self):
        transcription = "虛擬記憶體使用頁面。TLB 快取近期頁表項目。"
        accumulator = self._build_accumulator(transcription)
        accumulator.execute(
            "set_note_overview",
            {"detected_topic": "虛擬記憶體", "summary": transcription},
        )
        self._add_block(
            accumulator,
            block_type="concept",
            title="虛擬記憶體",
            evidence="虛擬記憶體使用頁面。",
            block_id="vm",
        )
        accumulator.execute(
            "link_note_blocks",
            {"source_block_id": "vm", "related_block_ids": ["tlb"]},
        )
        self._add_block(
            accumulator,
            block_type="fact",
            title="TLB",
            evidence="TLB 快取近期頁表項目。",
            block_id="tlb",
        )
        accumulator.execute("finish_note", {"complete": True, "review_note": None})

        cards = accumulator.to_legacy_payload()["key_concepts"]
        self.assertEqual(cards[0]["related_concepts"], ["TLB"])

    def test_invalid_coverage_ids_are_not_trusted(self):
        evidence = "Quick sort 以 pivot 分割區間。"
        accumulator = self._build_accumulator(evidence, coverage_ids=("valid",))
        accumulator.execute(
            "set_note_overview",
            {"detected_topic": "Quick sort", "summary": evidence},
        )
        self._add_block(
            accumulator,
            block_type="procedure",
            title="Quick sort",
            evidence=evidence,
            coverage_ids=["invalid", "valid"],
        )
        accumulator.execute("finish_note", {"complete": True, "review_note": None})

        self.assertEqual(
            accumulator.to_legacy_payload()["key_concepts"][0]["coverage_ids"],
            ["valid"],
        )

    def test_required_coverage_must_match_evidence_before_finish(self):
        transcription = "行程有獨立位址空間。執行緒共享所屬行程的位址空間。"
        accumulator = self._build_accumulator(
            transcription,
            coverage_ids=("process", "thread"),
            required_coverage_ids=("process", "thread"),
            coverage_items=(
                {"id": "process", "image_index": 1, "text": "行程有獨立位址空間。"},
                {
                    "id": "thread",
                    "image_index": 1,
                    "text": "執行緒共享所屬行程的位址空間。",
                },
            ),
        )
        accumulator.execute(
            "set_note_overview",
            {"detected_topic": "行程與執行緒", "summary": transcription},
        )
        self._add_block(
            accumulator,
            block_type="comparison",
            title="行程",
            evidence="行程有獨立位址空間。",
            coverage_ids=["process", "thread"],
        )
        with self.assertRaisesRegex(StudyNoteToolError, "thread"):
            accumulator.execute(
                "finish_note", {"complete": True, "review_note": None}
            )

        self._add_block(
            accumulator,
            block_type="comparison",
            title="執行緒",
            evidence="執行緒共享所屬行程的位址空間。",
            block_id="thread-block",
            coverage_ids=["thread"],
        )
        accumulator.execute("finish_note", {"complete": True, "review_note": None})

        cards = accumulator.to_legacy_payload()["key_concepts"]
        self.assertEqual(cards[0]["coverage_ids"], ["process"])
        self.assertEqual(cards[1]["coverage_ids"], ["thread"])

    def test_finish_is_deferred_when_a_parallel_sibling_call_fails(self):
        evidence = "Queue 採用 FIFO。"
        accumulator = self._build_accumulator(evidence)
        responses = [
            {
                "output": [
                    {
                        "type": "function_call",
                        "call_id": "overview",
                        "name": "set_note_overview",
                        "arguments": json.dumps(
                            {"detected_topic": "Queue", "summary": evidence},
                            ensure_ascii=False,
                        ),
                    },
                    {
                        "type": "function_call",
                        "call_id": "broken",
                        "name": "add_note_block",
                        "arguments": "{broken",
                    },
                    {
                        "type": "function_call",
                        "call_id": "premature-finish",
                        "name": "finish_note",
                        "arguments": '{"complete":true,"review_note":null}',
                    },
                ]
            },
            {
                "output": [
                    {
                        "type": "function_call",
                        "call_id": "block",
                        "name": "add_note_block",
                        "arguments": json.dumps(
                            {
                                "block_id": "queue",
                                "block_type": "definition",
                                "title": "Queue",
                                "topic": "FIFO",
                                "recall_cue": None,
                                "key_point": evidence,
                                "explanation": evidence,
                                "details": [],
                                "example": None,
                                "pitfall": None,
                                "memory_hint": None,
                                "keywords": ["Queue", "FIFO"],
                                "sources": [{"image_index": 1, "evidence": evidence}],
                                "coverage_ids": ["p1b1"],
                                "correction": {
                                    "applied": False,
                                    "original": None,
                                    "corrected": None,
                                    "reason": None,
                                },
                            },
                            ensure_ascii=False,
                        ),
                    },
                    {
                        "type": "function_call",
                        "call_id": "finish",
                        "name": "finish_note",
                        "arguments": '{"complete":true,"review_note":null}',
                    },
                ]
            },
        ]
        observed = []

        def request_round(conversation, round_index):
            if round_index == 1:
                observed.extend(
                    json.loads(item["output"])
                    for item in conversation
                    if item.get("type") == "function_call_output"
                    and item.get("call_id") in {"broken", "premature-finish"}
                )
            return responses[round_index]

        result = run_study_note_tool_conversation(
            initial_input=[{"role": "user", "content": "compose"}],
            accumulator=accumulator,
            request_round=request_round,
        )

        self.assertEqual([item["ok"] for item in observed], [False, False])
        self.assertEqual(result["key_concepts"][0]["concept"], "Queue")

    def test_round_limit_keeps_complete_validated_blocks(self):
        evidence = "Queue 採用 FIFO。"
        accumulator = self._build_accumulator(
            evidence,
            required_coverage_ids=("p1b1",),
            coverage_items=(
                {"id": "p1b1", "image_index": 1, "text": evidence},
            ),
        )
        response = {
            "output": [
                {
                    "type": "function_call",
                    "call_id": "overview",
                    "name": "set_note_overview",
                    "arguments": json.dumps(
                        {"detected_topic": "Queue", "summary": evidence},
                        ensure_ascii=False,
                    ),
                },
                {
                    "type": "function_call",
                    "call_id": "block",
                    "name": "add_note_block",
                    "arguments": json.dumps(
                        {
                            "block_id": "queue",
                            "block_type": "definition",
                            "title": "Queue",
                            "topic": "FIFO",
                            "recall_cue": None,
                            "key_point": evidence,
                            "explanation": evidence,
                            "details": [],
                            "example": None,
                            "pitfall": None,
                            "memory_hint": None,
                            "keywords": ["Queue", "FIFO"],
                            "sources": [{"image_index": 1, "evidence": evidence}],
                            "coverage_ids": ["p1b1"],
                            "correction": {
                                "applied": False,
                                "original": None,
                                "corrected": None,
                                "reason": None,
                            },
                        },
                        ensure_ascii=False,
                    ),
                },
                {
                    "type": "function_call",
                    "call_id": "broken-optional-sibling",
                    "name": "add_note_block",
                    "arguments": "{broken",
                },
                {
                    "type": "function_call",
                    "call_id": "finish",
                    "name": "finish_note",
                    "arguments": '{"complete":true,"review_note":null}',
                },
            ]
        }

        result = run_study_note_tool_conversation(
            initial_input=[{"role": "user", "content": "compose"}],
            accumulator=accumulator,
            request_round=lambda _conversation, _round_index: response,
            max_rounds=1,
        )

        self.assertEqual(len(result["key_concepts"]), 1)
        self.assertEqual(result["key_concepts"][0]["coverage_ids"], ["p1b1"])

    def test_tool_schemas_are_strict_and_cover_all_content_kinds(self):
        tools = build_study_note_tools(max_image_index=6)
        self.assertEqual(
            {tool["name"] for tool in tools},
            {"set_note_overview", "add_note_block", "link_note_blocks", "finish_note"},
        )
        for tool in tools:
            self.assertTrue(tool["strict"])
            parameters = tool["parameters"]
            self.assertFalse(parameters["additionalProperties"])
            self.assertEqual(set(parameters["required"]), set(parameters["properties"]))
        add_tool = next(tool for tool in tools if tool["name"] == "add_note_block")
        self.assertEqual(
            set(add_tool["parameters"]["properties"]["block_type"]["enum"]),
            set(CONTENT_KINDS),
        )

    def test_finish_rejects_an_empty_note_and_duplicate_blocks(self):
        accumulator = self._build_accumulator("Queue 採用 FIFO。")
        with self.assertRaisesRegex(StudyNoteToolError, "overview"):
            accumulator.execute("finish_note", {"complete": True, "review_note": None})
        accumulator.execute(
            "set_note_overview",
            {"detected_topic": "Queue", "summary": "Queue 採用 FIFO。"},
        )
        self._add_block(
            accumulator,
            block_type="definition",
            title="Queue",
            evidence="Queue 採用 FIFO。",
        )
        with self.assertRaisesRegex(StudyNoteToolError, "duplicate block_id"):
            self._add_block(
                accumulator,
                block_type="definition",
                title="Queue",
                evidence="Queue 採用 FIFO。",
            )

    def test_tool_conversation_preserves_reasoning_and_matches_call_ids(self):
        evidence = "Process 包含程式碼、資料與執行狀態。"
        accumulator = self._build_accumulator(evidence)
        seen_conversations = []
        responses = [
            {
                "output": [
                    {"type": "reasoning", "id": "reason-1", "summary": []},
                    {
                        "type": "function_call",
                        "call_id": "call-overview",
                        "name": "set_note_overview",
                        "arguments": json.dumps(
                            {"detected_topic": "Process", "summary": evidence},
                            ensure_ascii=False,
                        ),
                    },
                    {
                        "type": "function_call",
                        "call_id": "call-block",
                        "name": "add_note_block",
                        "arguments": json.dumps(
                            {
                                "block_id": "process",
                                "block_type": "definition",
                                "title": "Process",
                                "topic": "行程結構",
                                "recall_cue": None,
                                "key_point": evidence,
                                "explanation": evidence,
                                "details": [],
                                "example": None,
                                "pitfall": None,
                                "memory_hint": None,
                                "keywords": ["Process"],
                                "sources": [{"image_index": 1, "evidence": evidence}],
                                "coverage_ids": ["p1b1"],
                                "correction": {
                                    "applied": False,
                                    "original": None,
                                    "corrected": None,
                                    "reason": None,
                                },
                            },
                            ensure_ascii=False,
                        ),
                    },
                ]
            },
            {
                "output": [
                    {
                        "type": "function_call",
                        "call_id": "call-finish",
                        "name": "finish_note",
                        "arguments": '{"complete":true,"review_note":null}',
                    }
                ]
            },
        ]

        def request_round(conversation, round_index):
            seen_conversations.append(json.loads(json.dumps(conversation)))
            return responses[round_index]

        result = run_study_note_tool_conversation(
            initial_input=[{"role": "user", "content": "compose"}],
            accumulator=accumulator,
            request_round=request_round,
        )

        self.assertEqual(result["key_concepts"][0]["content_kind"], "definition")
        second_input = seen_conversations[1]
        self.assertTrue(any(item.get("type") == "reasoning" for item in second_input))
        outputs = {
            item["call_id"]: json.loads(item["output"])
            for item in second_input
            if item.get("type") == "function_call_output"
        }
        self.assertTrue(outputs["call-overview"]["ok"])
        self.assertTrue(outputs["call-block"]["ok"])

    def test_tool_conversation_returns_errors_so_the_model_can_retry(self):
        evidence = "Queue 採用 FIFO。"
        accumulator = self._build_accumulator(evidence)
        responses = [
            {
                "output": [
                    {
                        "type": "function_call",
                        "call_id": "bad",
                        "name": "add_note_block",
                        "arguments": "{broken",
                    }
                ]
            },
            {
                "output": [
                    {
                        "type": "function_call",
                        "call_id": "overview",
                        "name": "set_note_overview",
                        "arguments": json.dumps(
                            {"detected_topic": "Queue", "summary": evidence},
                            ensure_ascii=False,
                        ),
                    },
                    {
                        "type": "function_call",
                        "call_id": "block",
                        "name": "add_note_block",
                        "arguments": json.dumps(
                            {
                                "block_id": "queue",
                                "block_type": "definition",
                                "title": "Queue",
                                "topic": "FIFO",
                                "recall_cue": None,
                                "key_point": evidence,
                                "explanation": evidence,
                                "details": [],
                                "example": None,
                                "pitfall": None,
                                "memory_hint": None,
                                "keywords": ["Queue", "FIFO"],
                                "sources": [{"image_index": 1, "evidence": evidence}],
                                "coverage_ids": ["p1b1"],
                                "correction": {
                                    "applied": False,
                                    "original": None,
                                    "corrected": None,
                                    "reason": None,
                                },
                            },
                            ensure_ascii=False,
                        ),
                    },
                ]
            },
            {
                "output": [
                    {
                        "type": "function_call",
                        "call_id": "finish",
                        "name": "finish_note",
                        "arguments": '{"complete":true,"review_note":null}',
                    }
                ]
            },
        ]
        observed_error = []

        def request_round(conversation, round_index):
            if round_index == 1:
                error_output = next(
                    item for item in conversation if item.get("call_id") == "bad" and item.get("type") == "function_call_output"
                )
                observed_error.append(json.loads(error_output["output"])["ok"])
            return responses[round_index]

        result = run_study_note_tool_conversation(
            initial_input=[{"role": "user", "content": "compose"}],
            accumulator=accumulator,
            request_round=request_round,
        )

        self.assertEqual(observed_error, [False])
        self.assertEqual(result["key_concepts"][0]["concept"], "Queue")


if __name__ == "__main__":
    unittest.main()
