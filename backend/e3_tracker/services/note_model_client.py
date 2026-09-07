"""Note model client; dependencies are bound per application."""

import json
import math
import re
from typing import Any, Dict, List, Optional, Set, Tuple
import requests
from ..shared.config import normalize_openai_reasoning_effort
from ..shared.study_note_composer import StudyNoteToolAccumulator, build_study_note_tools, run_study_note_tool_conversation


def build_note_model_client(*,
    _STUDY_LATEX_COMMANDS,
    _raise_if_study_upload_cancelled,
    _set_study_upload_job,
    _study_upload_retry_wait,
    app,
    openai_api_key,
    openai_model,
    study_upload_context,
    visual_note_pipeline_enabled,
):
    def _extract_openai_text(payload: Dict[str, Any]) -> str:
        direct = payload.get("output_text")
        if isinstance(direct, str) and direct.strip():
            return direct.strip()
        for item in payload.get("output") or []:
            if not isinstance(item, dict):
                continue
            for content in item.get("content") or []:
                if isinstance(content, dict) and isinstance(content.get("text"), str):
                    return content["text"].strip()
        return ""

    def _repair_openai_latex_json_escapes(raw: str) -> str:
        """Protect LaTeX commands before JSON turns their prefixes into controls."""
        text = str(raw or "")
        repaired: List[str] = []
        in_string = False
        index = 0
        while index < len(text):
            char = text[index]
            if char == '"':
                in_string = not in_string
                repaired.append(char)
                index += 1
                continue
            if not in_string or char != "\\" or index + 1 >= len(text):
                repaired.append(char)
                index += 1
                continue
            following = text[index + 1]
            if following == "\\":
                repaired.append(text[index:index + 2])
                index += 2
                continue
            if following in {'"', "/"}:
                repaired.append(text[index:index + 2])
                index += 2
                continue
            if following == "u" and re.fullmatch(r"[0-9A-Fa-f]{4}", text[index + 2:index + 6]):
                repaired.append(text[index:index + 6])
                index += 6
                continue
            command_match = re.match(r"[A-Za-z]+", text[index + 1:])
            command = command_match.group(0) if command_match else ""
            is_latex_command = command in _STUDY_LATEX_COMMANDS
            is_latex_symbol = following in "()[]{}|,;!:%#&_"
            if is_latex_command or is_latex_symbol:
                repaired.append("\\\\")
                index += 1
                continue
            repaired.append(char)
            index += 1
        return "".join(repaired)

    def _repair_study_decoded_text(value: Any) -> str:
        text = str(value or "")
        corruption_markers = ("Ã", "Â", "â€", "ðŸ", "ï»¿", "锟斤拷")
        if any(marker in text for marker in corruption_markers) or any(0x80 <= ord(char) <= 0x9F for char in text):
            candidates = [text]
            for encoding in ("latin1", "cp1252"):
                try:
                    candidates.append(text.encode(encoding).decode("utf-8"))
                except (UnicodeEncodeError, UnicodeDecodeError):
                    continue

            def corruption_score(candidate: str) -> int:
                return (
                    sum(candidate.count(marker) for marker in corruption_markers) * 8
                    + candidate.count("\ufffd") * 12
                    + sum(1 for char in candidate if 0x80 <= ord(char) <= 0x9F) * 4
                )

            text = min(candidates, key=corruption_score)
        text = text.replace("\x08", "\\b").replace("\x0c", "\\f")
        text = re.sub(r"\t(?=[A-Za-z])", r"\\t", text)
        text = re.sub(r"\r(?=[A-Za-z])", r"\\r", text)
        text = re.sub(
            r"\n(?=(?:abla|e(?:q|xists)?|ot|u\b|atural|i\b|oindent|orm|ewline)(?:\b|[_^{}]))",
            r"\\n",
            text,
        )
        return text

    def _study_latex_markup_issue(value: Any) -> Optional[str]:
        text = str(value or "")
        delimiter_patterns = (
            (r"(?<!\\)\\\(", r"(?<!\\)\\\)"),
            (r"(?<!\\)\\\[", r"(?<!\\)\\\]"),
        )
        for opening, closing in delimiter_patterns:
            if len(re.findall(opening, text)) != len(re.findall(closing, text)):
                return "unbalanced_math_delimiter"
        math_spans = re.findall(
            r"(?<!\\)\\\[(.*?)(?<!\\)\\\]|(?<!\\)\\\((.*?)(?<!\\)\\\)",
            text,
            flags=re.DOTALL,
        )
        for display_body, inline_body in math_spans:
            body = display_body or inline_body
            if "\\(" in body or "\\)" in body or "\\[" in body or "\\]" in body:
                return "nested_math_delimiter"
            prose_free_body = re.sub(
                r"\\text\{(?:[^{}]|\{[^{}]*\})*\}",
                "",
                body,
            )
            if re.search(r"[\u3400-\u9fff]", prose_free_body):
                return "chinese_inside_math"
            brace_depth = 0
            for match in re.finditer(r"(?<!\\)[{}]", body):
                brace_depth += 1 if match.group(0) == "{" else -1
                if brace_depth < 0:
                    return "unbalanced_math_brace"
            if brace_depth:
                return "unbalanced_math_brace"
            environments = re.findall(r"\\begin\{([^{}]+)\}", body)
            closed_environments = re.findall(r"\\end\{([^{}]+)\}", body)
            if environments != closed_environments:
                return "unbalanced_math_environment"
            if re.search(r"\\(?:b|f|n|r|t)(?![A-Za-z])", body) or body.rstrip().endswith("\\"):
                return "broken_latex_command"
        return None

    def _openai_error_details(response: Any) -> Tuple[str, str, str]:
        if response is None:
            return "", "", ""
        try:
            payload = response.json()
        except (TypeError, ValueError, requests.RequestException):
            return "", "", ""
        error = payload.get("error") if isinstance(payload, dict) else None
        if not isinstance(error, dict):
            return "", "", ""
        return (
            str(error.get("code") or "").strip().lower(),
            str(error.get("type") or "").strip().lower(),
            str(error.get("message") or "").strip(),
        )

    def _is_openai_quota_error(code: str, error_type: str, message: str) -> bool:
        quota_codes = {
            "billing_hard_limit_reached",
            "billing_not_active",
            "insufficient_quota",
            "usage_limit_reached",
        }
        normalized_code = str(code or "").strip().lower()
        normalized_type = str(error_type or "").strip().lower()
        normalized_message = str(message or "").strip().lower()
        return (
            normalized_code in quota_codes
            or normalized_type in quota_codes
            or "current quota" in normalized_message
            or "billing" in normalized_message
            or "run out of credits" in normalized_message
        )

    def _request_openai_response(
        *,
        name: str,
        request_body: Dict[str, Any],
        timeout: int,
    ) -> Dict[str, Any]:
        """Send one Responses API request with the shared retry policy."""

        response = None
        for attempt in range(6):
            _raise_if_study_upload_cancelled()
            try:
                response = requests.post(
                    "https://api.openai.com/v1/responses",
                    headers={
                        "Authorization": f"Bearer {openai_api_key}",
                        "Content-Type": "application/json",
                    },
                    json=request_body,
                    timeout=timeout,
                )
                response.raise_for_status()
                _raise_if_study_upload_cancelled()
                break
            except requests.RequestException as exc:
                error_response = getattr(exc, "response", None)
                status_code = getattr(error_response, "status_code", None)
                error_code, error_type, error_message = _openai_error_details(error_response)
                quota_exhausted = status_code == 429 and _is_openai_quota_error(
                    error_code,
                    error_type,
                    error_message,
                )
                setattr(exc, "openai_error_code", error_code)
                setattr(exc, "openai_error_type", error_type)
                setattr(exc, "openai_error_message", error_message)
                retryable = (
                    status_code is None
                    or (status_code == 429 and not quota_exhausted)
                    or (status_code is not None and status_code >= 500)
                )
                retry_limit = 6 if status_code == 429 else 4
                if not retryable or attempt + 1 >= retry_limit:
                    raise
                retry_after = 0.0
                if error_response is not None:
                    try:
                        retry_after = float(error_response.headers.get("Retry-After") or 0)
                    except (TypeError, ValueError):
                        retry_after = 0.0
                retry_delay = min(
                    90.0,
                    max(
                        retry_after,
                        5.0 * (2 ** attempt)
                        if status_code == 429
                        else 2.0 * (attempt + 1),
                    ),
                )
                job_id = getattr(study_upload_context, "job_id", None)
                if job_id and status_code == 429:
                    _set_study_upload_job(
                        job_id,
                        message=(
                            f"AI 服務目前忙碌，{math.ceil(retry_delay)} 秒後自動重試"
                            f"（第 {attempt + 1}／{retry_limit - 1} 次）。"
                        ),
                    )
                app.logger.warning(
                    "Retrying OpenAI request %s after status=%s in %.1fs (attempt %s/%s)",
                    name,
                    status_code,
                    retry_delay,
                    attempt + 1,
                    retry_limit - 1,
                )
                _study_upload_retry_wait(retry_delay)
        if response is None:
            raise requests.RequestException("OpenAI request did not return a response")
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError("OpenAI response payload is not an object")
        return payload

    def _call_openai_json(
        *,
        name: str,
        schema: Dict[str, Any],
        content: List[Dict[str, Any]],
        timeout: int = 120,
        reasoning_effort: Optional[str] = None,
        max_output_tokens: int = 12000,
        repair_simple_location_json: bool = False,
        json_retry_attempts: int = 1,
    ) -> Dict[str, Any]:
        _raise_if_study_upload_cancelled()

        def repair_strings(value: Any) -> Any:
            if isinstance(value, str):
                return _repair_study_decoded_text(value)
            if isinstance(value, list):
                return [repair_strings(item) for item in value]
            if isinstance(value, dict):
                return {key: repair_strings(item) for key, item in value.items()}
            return value

        request_body = {
            "model": openai_model,
            "store": False,
            "input": [{"role": "user", "content": content}],
            "text": {"format": {"type": "json_schema", "name": name, "strict": True, "schema": schema}},
            "max_output_tokens": max_output_tokens,
        }
        effective_reasoning_effort = normalize_openai_reasoning_effort(
            openai_model,
            reasoning_effort,
        )
        if effective_reasoning_effort:
            request_body["reasoning"] = {"effort": effective_reasoning_effort}
        response_payload = _request_openai_response(
            name=name,
            request_body=request_body,
            timeout=timeout,
        )
        output_text = _extract_openai_text(response_payload)
        if not output_text:
            incomplete = response_payload.get("incomplete_details") or {}
            raise ValueError(
                f"OpenAI returned no output text (status={response_payload.get('status')}, "
                f"reason={incomplete.get('reason')})"
            )
        protected_output_text = _repair_openai_latex_json_escapes(output_text)
        try:
            parsed = json.loads(protected_output_text)
        except json.JSONDecodeError as parse_error:
            incomplete = response_payload.get("incomplete_details") or {}
            app.logger.warning(
                "Invalid JSON for %s (status=%s, reason=%s, chars=%s, error=%s)",
                name,
                response_payload.get("status"),
                incomplete.get("reason"),
                len(output_text),
                parse_error,
            )
            if not repair_simple_location_json and json_retry_attempts > 0:
                retry_content = list(content) + [
                    {
                        "type": "input_text",
                        "text": (
                            "上一個回應的 JSON 字串未完整結束或格式損壞，因此未被接收。"
                            "請重新從頭輸出一份完整、合法且符合 schema 的 JSON；不要省略結尾，"
                            "不要輸出 Markdown 或任何 JSON 以外的文字。"
                        ),
                    }
                ]
                return _call_openai_json(
                    name=name,
                    schema=schema,
                    content=retry_content,
                    timeout=max(timeout, 360),
                    reasoning_effort=reasoning_effort,
                    max_output_tokens=min(24000, max_output_tokens + 4000),
                    repair_simple_location_json=repair_simple_location_json,
                    json_retry_attempts=json_retry_attempts - 1,
                )
            if not repair_simple_location_json:
                raise
            location_fields = (
                "location_id|found|left|top|right|bottom|start_x|start_y|end_x|end_y|confidence"
            )
            repaired_output = re.sub(
                rf"(true|false|-?\d+|\}}|\])\s*(?=\"(?:{location_fields})\"\s*:)",
                r"\1,",
                protected_output_text,
            )
            repaired_output = re.sub(r"}\s*{", "},{", repaired_output)
            repaired_output = re.sub(r",\s*([}\]])", r"\1", repaired_output)
            try:
                parsed = json.loads(repaired_output)
            except json.JSONDecodeError:
                if json_retry_attempts > 0:
                    return _call_openai_json(
                        name=name,
                        schema=schema,
                        content=content,
                        timeout=max(timeout, 360),
                        reasoning_effort=reasoning_effort,
                        max_output_tokens=min(24000, max_output_tokens + 4000),
                        repair_simple_location_json=repair_simple_location_json,
                        json_retry_attempts=json_retry_attempts - 1,
                    )
                app.logger.warning(
                    "Invalid simple location JSON for %s (status=%s, reason=%s, chars=%s)",
                    name,
                    response_payload.get("status"),
                    incomplete.get("reason"),
                    len(output_text),
                )
                raise
        if not isinstance(parsed, dict):
            raise ValueError("OpenAI did not return a JSON object")
        return repair_strings(parsed)

    def _call_openai_study_note_tools(
        *,
        subject: str,
        source_pages: List[Dict[str, Any]],
        coverage_checklist: List[Dict[str, Any]],
        allow_corrections: bool,
        max_rounds: int = 12,
    ) -> Dict[str, Any]:
        """Let the model compose a note through semantic function tools."""

        valid_coverage_ids = {
            str(item.get("id") or "").strip()
            for item in coverage_checklist
            if isinstance(item, dict) and str(item.get("id") or "").strip()
        }
        required_coverage_ids = {
            str(item.get("id") or "").strip()
            for item in coverage_checklist
            if isinstance(item, dict)
            and item.get("priority") == "required"
            and str(item.get("id") or "").strip()
        }
        # A page containing only explanatory prose still needs representation.
        # Requiring its first source item prevents an entire non-mathematical
        # page from disappearing merely because it has no formula/example cue.
        represented_pages: Set[int] = set()
        for item in coverage_checklist:
            if not isinstance(item, dict) or item.get("priority") != "required":
                continue
            try:
                required_image_index = int(item.get("image_index") or 0)
            except (TypeError, ValueError):
                continue
            if required_image_index > 0:
                represented_pages.add(required_image_index)
        for item in coverage_checklist:
            if not isinstance(item, dict):
                continue
            try:
                image_index = int(item.get("image_index") or 0)
            except (TypeError, ValueError):
                continue
            coverage_id = str(item.get("id") or "").strip()
            if image_index > 0 and coverage_id and image_index not in represented_pages:
                required_coverage_ids.add(coverage_id)
                represented_pages.add(image_index)
        accumulator = StudyNoteToolAccumulator(
            source_pages=source_pages,
            valid_coverage_ids=valid_coverage_ids,
            required_coverage_ids=required_coverage_ids,
            coverage_items=coverage_checklist,
        )
        prompt = (
            f"你是「{subject}」筆記的忠實內容架構師。你可使用工具自由決定區塊數量、順序與類型，"
            "不要把所有內容硬塞成同一種卡片。只根據 source_pages；禁止補充課本知識、外部例子、"
            "來源未寫出的因果或定義。先呼叫 set_note_overview，再依內容呼叫 add_note_block，最後才呼叫 "
            "finish_note。可在同一輪呼叫多個 add_note_block。\n"
            "block_type 選擇規則：definition 用於明確定義；concept 用於核心觀念；procedure 用於流程、"
            "演算法步驟、系統執行階段或操作順序；comparison 用於來源明確列出的差異；formula 用於公式、"
            "遞迴式、複雜度或硬體關係；code 用於程式碼、虛擬碼、指令或資料結構操作；example 只用於來源"
            "確實存在的例題／案例／練習；fact 用於不適合前述類型的具體事實。六科全部使用相同規則。\n"
            + (
                "visual 用於主要知識由樹、流程、圖表或示意圖表達的區塊。\n"
                if visual_note_pipeline_enabled
                else ""
            )
            +
            "每個區塊只處理一個可獨立複習的目標。key_point 寫最重要結論，explanation 寫來源已有的脈絡；"
            "details 只在來源真的有步驟、條目、組成或比較項目時使用。example 是可選欄位；definition、"
            "concept、procedure、comparison、formula、code、fact 沒有來源例子時必須傳 null，不得為了填欄位"
            "創造數字、情境或程式。example 區塊則必須在 example 放完整可作答題設或完整案例。pitfall 與 "
            "memory_hint 也只有來源明寫時才填，否則傳 null。\n"
            "所有公式使用 KaTeX 可渲染的 \\( ... \\) 或 \\[ ... \\]。程式碼不是數學式：code 區塊的"
            "完整程式、函式、類別、虛擬碼或連續兩行以上操作，必須放在帶語言名稱的 Markdown fenced "
            "code block，例如 ```cpp、```python 或 ```pseudocode；未知語言用 ```text。保留自然換行、"
            "縮排、大小寫、括號、分號、陣列索引、指標、運算子與註解。行內識別字或短指令使用單反引號。"
            "title 與 topic 必須是實際細分內容，不能直接等於六科科目名稱，也不能使用『其他』或"
            "『綜合重點』。recall_cue 可為 null；系統會安全產生後備提示。\n"
            "每個 sources.evidence 都必須逐字複製對應 transcription 中連續出現的原文；工具會拒絕改寫、"
            "摘要、錯頁或不存在的 evidence。coverage_checklist 中 is_example=true 的項目必須各自建立 "
            "example 區塊，其餘 required 項目必須被至少一個區塊覆蓋。coverage_ids 只能填實際涵蓋的 id。\n"
            + (
                "source_pages.visual_regions 是獨立的視覺來源。只要區塊使用某張圖的內容，就必須在 visual_refs"
                " 填入對應 region_id；純視覺區塊可讓 sources 為空，但不可讓 sources 與 visual_refs 同時為空。"
                "content_type=visual 的 required coverage id 必須由引用同一 region_id 的卡片覆蓋。不得只把圖"
                "描述成文字後丟掉 visual_refs，也不得捏造圖中沒有的節點、箭頭或趨勢。\n"
                if visual_note_pipeline_enabled
                else ""
            )
            + (
                "只可修正能由來源中的公式、標準定義或直接計算毫無歧義確認的錯誤；修正時 correction 四欄"
                "完整填寫，否則 applied=false 且其他欄傳 null。"
                if allow_corrections
                else "不得修正內容；correction.applied=false，其他欄一律傳 null。"
            )
            + "\n若工具回傳錯誤，依錯誤修正該次呼叫，不要放棄整份筆記。finish_note 前自行確認每個重要"
            "定義、流程、公式、程式碼、比較、例題與結論都已涵蓋。\n\n"
            "source_pages="
            + json.dumps(source_pages, ensure_ascii=False, separators=(",", ":"))
            + "\ncoverage_checklist="
            + json.dumps(coverage_checklist, ensure_ascii=False, separators=(",", ":"))
        )
        initial_input: List[Dict[str, Any]] = [
            {
                "role": "user",
                "content": [{"type": "input_text", "text": prompt}],
            }
        ]
        tools = build_study_note_tools(
            max_image_index=len(source_pages),
            enable_visual_refs=visual_note_pipeline_enabled,
        )

        def request_round(
            conversation: List[Dict[str, Any]],
            round_index: int,
        ) -> Dict[str, Any]:
            _raise_if_study_upload_cancelled()
            request_body: Dict[str, Any] = {
                "model": openai_model,
                "store": False,
                "input": conversation,
                "tools": tools,
                "tool_choice": "required",
                "parallel_tool_calls": True,
                "max_output_tokens": 12000,
            }
            effective_reasoning_effort = normalize_openai_reasoning_effort(
                openai_model,
                "medium",
            )
            if effective_reasoning_effort:
                request_body["reasoning"] = {"effort": effective_reasoning_effort}
            return _request_openai_response(
                name=f"study_note_tool_composer_{round_index + 1}",
                request_body=request_body,
                timeout=300,
            )

        return run_study_note_tool_conversation(
            initial_input=initial_input,
            accumulator=accumulator,
            request_round=request_round,
            max_rounds=max_rounds,
        )

    return (
        _extract_openai_text,
        _repair_openai_latex_json_escapes,
        _repair_study_decoded_text,
        _study_latex_markup_issue,
        _openai_error_details,
        _is_openai_quota_error,
        _request_openai_response,
        _call_openai_json,
        _call_openai_study_note_tools,
    )
