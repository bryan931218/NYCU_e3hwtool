"""Subject-neutral tool composer for OpenAI study-note organization.

The model chooses which semantic blocks a note needs instead of filling one
large, discipline-specific JSON object.  This module deliberately contains no
OpenAI or Flask code so tool execution and validation stay deterministic and
unit-testable.
"""

from __future__ import annotations

import copy
import json
import re
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Mapping, Sequence, Set

from .study_math import protect_markdown_code, restore_markdown_code


CONTENT_KINDS = (
    "definition",
    "concept",
    "procedure",
    "comparison",
    "formula",
    "code",
    "example",
    "fact",
)


class StudyNoteToolError(ValueError):
    """Raised when a model tool call cannot be safely applied."""


def _strict_object(properties: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": list(properties),
        "properties": properties,
    }


def build_study_note_tools(*, max_image_index: int) -> List[Dict[str, Any]]:
    """Return strict Responses API function tools for one note batch."""

    source_schema = _strict_object(
        {
            "image_index": {
                "type": "integer",
                "minimum": 1,
                "maximum": max(1, int(max_image_index)),
            },
            "evidence": {"type": "string", "minLength": 1, "maxLength": 500},
        }
    )
    correction_schema = _strict_object(
        {
            "applied": {"type": "boolean"},
            "original": {"type": ["string", "null"], "maxLength": 300},
            "corrected": {"type": ["string", "null"], "maxLength": 300},
            "reason": {"type": ["string", "null"], "maxLength": 360},
        }
    )
    return [
        {
            "type": "function",
            "name": "set_note_overview",
            "description": (
                "Set the precise topic and short overview for this uploaded note. "
                "Call once before adding blocks."
            ),
            "strict": True,
            "parameters": _strict_object(
                {
                    "detected_topic": {"type": "string", "minLength": 1, "maxLength": 80},
                    "summary": {"type": "string", "minLength": 1, "maxLength": 1200},
                }
            ),
        },
        {
            "type": "function",
            "name": "add_note_block",
            "description": (
                "Add one independently reviewable note block. Choose the semantic "
                "block type that best matches the source; optional sections may be null."
            ),
            "strict": True,
            "parameters": _strict_object(
                {
                    "block_id": {
                        "type": "string",
                        "pattern": "^[A-Za-z0-9_-]{1,48}$",
                    },
                    "block_type": {"type": "string", "enum": list(CONTENT_KINDS)},
                    "title": {"type": "string", "minLength": 1, "maxLength": 100},
                    "topic": {"type": "string", "minLength": 1, "maxLength": 80},
                    "recall_cue": {"type": ["string", "null"], "maxLength": 180},
                    "key_point": {"type": "string", "minLength": 1, "maxLength": 320},
                    "explanation": {"type": "string", "minLength": 1, "maxLength": 900},
                    "details": {
                        "type": "array",
                        "maxItems": 8,
                        "items": {"type": "string", "minLength": 1, "maxLength": 220},
                    },
                    "example": {"type": ["string", "null"], "maxLength": 420},
                    "pitfall": {"type": ["string", "null"], "maxLength": 240},
                    "memory_hint": {"type": ["string", "null"], "maxLength": 240},
                    "keywords": {
                        "type": "array",
                        "maxItems": 10,
                        "items": {"type": "string", "minLength": 1, "maxLength": 60},
                    },
                    "sources": {
                        "type": "array",
                        "minItems": 1,
                        "maxItems": 6,
                        "items": source_schema,
                    },
                    "coverage_ids": {
                        "type": "array",
                        "maxItems": 12,
                        "items": {"type": "string", "minLength": 1, "maxLength": 24},
                    },
                    "correction": correction_schema,
                }
            ),
        },
        {
            "type": "function",
            "name": "link_note_blocks",
            "description": (
                "Declare strong prerequisite, comparison, or derivation links between "
                "blocks. Skip weak same-chapter associations."
            ),
            "strict": True,
            "parameters": _strict_object(
                {
                    "source_block_id": {
                        "type": "string",
                        "pattern": "^[A-Za-z0-9_-]{1,48}$",
                    },
                    "related_block_ids": {
                        "type": "array",
                        "minItems": 1,
                        "maxItems": 4,
                        "items": {
                            "type": "string",
                            "pattern": "^[A-Za-z0-9_-]{1,48}$",
                        },
                    },
                }
            ),
        },
        {
            "type": "function",
            "name": "finish_note",
            "description": (
                "Finish only after every required source item has been represented by "
                "an appropriate block."
            ),
            "strict": True,
            "parameters": _strict_object(
                {
                    "complete": {"type": "boolean", "enum": [True]},
                    "review_note": {"type": ["string", "null"], "maxLength": 300},
                }
            ),
        },
    ]


def _clean_text(value: Any, *, limit: int, required: bool = False) -> str:
    text = " ".join(str(value or "").split()).strip()
    if required and not text:
        raise StudyNoteToolError("required text is empty")
    return text[:limit]


def _clean_multiline_text(value: Any, *, limit: int, required: bool = False) -> str:
    text = str(value or "").strip()
    text, protected_code = protect_markdown_code(text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = restore_markdown_code(text, protected_code)
    if required and not text:
        raise StudyNoteToolError("required text is empty")
    return text[:limit]


def _canonical_source_text(value: Any) -> str:
    return re.sub(r"\s+", "", str(value or "")).casefold()


@dataclass
class StudyNoteToolAccumulator:
    """Validate model tool calls and convert them to the existing card format."""

    source_pages: Sequence[Mapping[str, Any]]
    valid_coverage_ids: Iterable[str] = field(default_factory=tuple)
    required_coverage_ids: Iterable[str] = field(default_factory=tuple)
    coverage_items: Sequence[Mapping[str, Any]] = field(default_factory=tuple)
    max_blocks: int = 80
    detected_topic: str = ""
    summary: str = ""
    blocks: List[Dict[str, Any]] = field(default_factory=list)
    pending_links: Dict[str, List[str]] = field(default_factory=dict)
    finished: bool = False

    def __post_init__(self) -> None:
        self._page_text = {
            int(page.get("image_index") or 0): str(page.get("transcription") or "")
            for page in self.source_pages
            if isinstance(page, Mapping)
        }
        self._valid_coverage_ids: Set[str] = {
            str(value).strip() for value in self.valid_coverage_ids if str(value).strip()
        }
        self._required_coverage_ids: Set[str] = {
            str(value).strip()
            for value in self.required_coverage_ids
            if str(value).strip()
        }
        self._coverage_items: Dict[str, Dict[str, Any]] = {}
        for item in self.coverage_items:
            if not isinstance(item, Mapping):
                continue
            coverage_id = str(item.get("id") or "").strip()
            try:
                image_index = int(item.get("image_index") or 0)
            except (TypeError, ValueError):
                continue
            if coverage_id and image_index > 0 and str(item.get("text") or "").strip():
                self._coverage_items[coverage_id] = {
                    "image_index": image_index,
                    "text": str(item.get("text") or ""),
                }
        if self._coverage_items:
            self._valid_coverage_ids.update(self._coverage_items)
        self._required_coverage_ids.intersection_update(self._valid_coverage_ids)
        self._block_ids: Set[str] = set()

    def execute(self, tool_name: str, arguments: Mapping[str, Any]) -> Dict[str, Any]:
        if self.finished:
            raise StudyNoteToolError("note has already been finished")
        if not isinstance(arguments, Mapping):
            raise StudyNoteToolError("tool arguments must be an object")
        if tool_name == "set_note_overview":
            return self._set_overview(arguments)
        if tool_name == "add_note_block":
            return self._add_block(arguments)
        if tool_name == "link_note_blocks":
            return self._link_blocks(arguments)
        if tool_name == "finish_note":
            return self._finish(arguments)
        raise StudyNoteToolError(f"unknown tool: {tool_name}")

    def _set_overview(self, arguments: Mapping[str, Any]) -> Dict[str, Any]:
        self.detected_topic = _clean_text(
            arguments.get("detected_topic"), limit=80, required=True
        )
        self.summary = _clean_multiline_text(
            arguments.get("summary"), limit=1200, required=True
        )
        return {"ok": True, "topic": self.detected_topic}

    def _add_block(self, arguments: Mapping[str, Any]) -> Dict[str, Any]:
        if len(self.blocks) >= max(1, int(self.max_blocks)):
            raise StudyNoteToolError("too many note blocks")
        block_id = _clean_text(arguments.get("block_id"), limit=48, required=True)
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,48}", block_id):
            raise StudyNoteToolError("invalid block_id")
        if block_id in self._block_ids:
            raise StudyNoteToolError(f"duplicate block_id: {block_id}")
        block_type = str(arguments.get("block_type") or "").strip()
        if block_type not in CONTENT_KINDS:
            raise StudyNoteToolError("invalid block_type")

        sources: List[Dict[str, Any]] = []
        seen_sources: Set[tuple[int, str]] = set()
        for item in arguments.get("sources") or []:
            if not isinstance(item, Mapping):
                continue
            try:
                image_index = int(item.get("image_index") or 0)
            except (TypeError, ValueError):
                continue
            evidence = _clean_multiline_text(item.get("evidence"), limit=500)
            page_text = self._page_text.get(image_index, "")
            if (
                not evidence
                or not page_text
                or _canonical_source_text(evidence) not in _canonical_source_text(page_text)
            ):
                continue
            key = (image_index, _canonical_source_text(evidence))
            if key in seen_sources:
                continue
            seen_sources.add(key)
            sources.append({"image_index": image_index, "evidence": evidence})
        if not sources:
            raise StudyNoteToolError("block has no literal source evidence")

        example = _clean_multiline_text(arguments.get("example"), limit=420)
        if block_type == "example" and not example:
            raise StudyNoteToolError("example block requires a concrete source problem")
        correction_raw = arguments.get("correction")
        correction_raw = correction_raw if isinstance(correction_raw, Mapping) else {}
        correction_applied = bool(correction_raw.get("applied"))
        correction = {
            "applied": correction_applied,
            "original": _clean_multiline_text(correction_raw.get("original"), limit=300),
            "corrected": _clean_multiline_text(correction_raw.get("corrected"), limit=300),
            "reason": _clean_multiline_text(correction_raw.get("reason"), limit=360),
        }
        if not correction_applied:
            correction.update({"original": "", "corrected": "", "reason": ""})

        coverage_ids = []
        for value in arguments.get("coverage_ids") or []:
            coverage_id = _clean_text(value, limit=24)
            if not coverage_id or coverage_id in coverage_ids:
                continue
            if self._valid_coverage_ids and coverage_id not in self._valid_coverage_ids:
                continue
            coverage_item = self._coverage_items.get(coverage_id)
            if coverage_item:
                coverage_text = _canonical_source_text(coverage_item["text"])
                coverage_page = int(coverage_item["image_index"] or 0)
                matches_evidence = any(
                    int(source["image_index"]) == coverage_page
                    and (
                        _canonical_source_text(source["evidence"]) in coverage_text
                        or coverage_text in _canonical_source_text(source["evidence"])
                    )
                    for source in sources
                )
                if not matches_evidence:
                    continue
            coverage_ids.append(coverage_id)

        details = [
            _clean_multiline_text(value, limit=220)
            for value in (arguments.get("details") or [])[:8]
        ]
        details = [value for value in details if value]
        keywords = [
            _clean_text(value, limit=60)
            for value in (arguments.get("keywords") or [])[:10]
        ]
        keywords = list(dict.fromkeys(value for value in keywords if value))
        block = {
            "block_id": block_id,
            "content_kind": block_type,
            "title": _clean_text(arguments.get("title"), limit=100, required=True),
            "topic": _clean_text(arguments.get("topic"), limit=80, required=True),
            "recall_cue": _clean_multiline_text(arguments.get("recall_cue"), limit=180),
            "key_point": _clean_multiline_text(
                arguments.get("key_point"), limit=320, required=True
            ),
            "explanation": _clean_multiline_text(
                arguments.get("explanation"), limit=900, required=True
            ),
            "details": details,
            "example": example,
            "pitfall": _clean_multiline_text(arguments.get("pitfall"), limit=240),
            "memory_hint": _clean_multiline_text(
                arguments.get("memory_hint"), limit=240
            ),
            "keywords": keywords,
            "source_refs": sources[:6],
            "coverage_ids": coverage_ids[:12],
            "correction": correction,
        }
        self.blocks.append(block)
        self._block_ids.add(block_id)
        return {"ok": True, "block_id": block_id, "block_count": len(self.blocks)}

    def _link_blocks(self, arguments: Mapping[str, Any]) -> Dict[str, Any]:
        source_id = _clean_text(arguments.get("source_block_id"), limit=48, required=True)
        related = [
            _clean_text(value, limit=48)
            for value in (arguments.get("related_block_ids") or [])[:4]
        ]
        related = list(dict.fromkeys(value for value in related if value and value != source_id))
        if not related:
            raise StudyNoteToolError("link has no related block ids")
        self.pending_links[source_id] = related
        return {"ok": True, "source_block_id": source_id, "related_count": len(related)}

    def _finish(self, arguments: Mapping[str, Any]) -> Dict[str, Any]:
        if arguments.get("complete") is not True:
            raise StudyNoteToolError("finish_note.complete must be true")
        if not self.detected_topic or not self.summary:
            raise StudyNoteToolError("note overview is missing")
        if not self.blocks:
            raise StudyNoteToolError("note has no blocks")
        covered_ids = {
            coverage_id
            for block in self.blocks
            for coverage_id in block.get("coverage_ids") or []
        }
        missing_required = sorted(self._required_coverage_ids - covered_ids)
        if missing_required:
            raise StudyNoteToolError(
                "required source items are still missing: "
                + ", ".join(missing_required[:20])
            )
        unknown_links = {
            block_id
            for source_id, related_ids in self.pending_links.items()
            for block_id in (source_id, *related_ids)
            if block_id not in self._block_ids
        }
        if unknown_links:
            raise StudyNoteToolError(
                "linked blocks do not exist: " + ", ".join(sorted(unknown_links))
            )
        self.finished = True
        return {"ok": True, "block_count": len(self.blocks), "finished": True}

    def to_legacy_payload(self) -> Dict[str, Any]:
        if not self.finished:
            raise StudyNoteToolError("note is not finished")
        titles = {block["block_id"]: block["title"] for block in self.blocks}
        cards: List[Dict[str, Any]] = []
        for block in self.blocks:
            content_kind = block["content_kind"]
            is_example = content_kind == "example"
            cue = block["recall_cue"] or "、".join(block["keywords"][:4]) or block["title"]
            related_titles = [
                titles[related_id]
                for related_id in self.pending_links.get(block["block_id"], [])
                if related_id in titles and related_id != block["block_id"]
            ]
            cards.append(
                {
                    "concept": block["title"],
                    "content_kind": content_kind,
                    "card_type": "example" if is_example else "concept",
                    "recall_cue": cue,
                    "core_summary": block["key_point"],
                    "explanation": block["explanation"],
                    "simple_example": "" if is_example else block["example"],
                    "example_problem": block["example"] if is_example else "",
                    "example_method": block["explanation"] if is_example else "",
                    "reasoning_steps": block["details"],
                    "common_confusion": block["pitfall"],
                    "memory_hint": block["memory_hint"],
                    "topic": block["topic"],
                    "related_concepts": related_titles[:4],
                    "search_keywords": block["keywords"],
                    "source_refs": block["source_refs"],
                    "coverage_ids": block["coverage_ids"],
                    "correction": block["correction"],
                }
            )
        return {
            "detected_topic": self.detected_topic,
            "summary": self.summary,
            "key_concepts": cards,
            "organization_mode": "tool_composer_v1",
        }


def run_study_note_tool_conversation(
    *,
    initial_input: Sequence[Mapping[str, Any]],
    accumulator: StudyNoteToolAccumulator,
    request_round: Callable[[List[Dict[str, Any]], int], Mapping[str, Any]],
    max_rounds: int = 24,
) -> Dict[str, Any]:
    """Run a Responses API function-call loop through an injected requester.

    The requester owns HTTP/model settings.  This runner owns the stateful tool
    protocol, preserves reasoning output items, and returns deterministic cards.
    """

    conversation: List[Dict[str, Any]] = copy.deepcopy(list(initial_input))
    for round_index in range(max(1, int(max_rounds))):
        response_payload = request_round(conversation, round_index)
        output_items = response_payload.get("output")
        if not isinstance(output_items, list) or not output_items:
            raise StudyNoteToolError("tool composer returned no output items")
        # Responses reasoning models require all output items to be passed back,
        # not only the function_call items.
        conversation.extend(copy.deepcopy(output_items))
        tool_outputs: List[Dict[str, Any]] = []
        function_call_count = 0
        had_tool_error = False
        function_calls = [
            item
            for item in output_items
            if isinstance(item, Mapping) and item.get("type") == "function_call"
        ]
        # A parallel batch may contain both content calls and finish_note. Apply
        # finish last so a malformed sibling call cannot silently disappear.
        function_calls.sort(key=lambda item: item.get("name") == "finish_note")
        for item in function_calls:
            if not isinstance(item, Mapping) or item.get("type") != "function_call":
                continue
            function_call_count += 1
            call_id = str(item.get("call_id") or "").strip()
            tool_name = str(item.get("name") or "").strip()
            if not call_id:
                raise StudyNoteToolError("function call is missing call_id")
            raw_arguments = item.get("arguments")
            try:
                if tool_name == "finish_note" and had_tool_error:
                    raise StudyNoteToolError(
                        "finish_note deferred until earlier tool errors are fixed"
                    )
                arguments = (
                    json.loads(raw_arguments)
                    if isinstance(raw_arguments, str)
                    else raw_arguments
                )
                result = accumulator.execute(tool_name, arguments)
                output = {"ok": True, "result": result}
            except (json.JSONDecodeError, StudyNoteToolError, TypeError, ValueError) as exc:
                had_tool_error = True
                output = {"ok": False, "error": str(exc)[:500]}
            tool_outputs.append(
                {
                    "type": "function_call_output",
                    "call_id": call_id,
                    "output": json.dumps(output, ensure_ascii=False, separators=(",", ":")),
                }
            )
        if not function_call_count:
            raise StudyNoteToolError("tool composer did not call a function")
        conversation.extend(tool_outputs)
        if accumulator.finished:
            return accumulator.to_legacy_payload()
    raise StudyNoteToolError("tool composer exceeded the maximum number of rounds")
