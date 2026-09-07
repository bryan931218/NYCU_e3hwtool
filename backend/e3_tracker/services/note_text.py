"""Note text; dependencies are bound per application."""

import re
from typing import Any, Dict, List, Optional, Tuple
from ..shared.study_math import (
    is_pure_math_expression,
    protect_markdown_code,
    repair_math_delimiters,
    restore_markdown_code,
    wrap_bare_math_candidate,
)


def build_note_text(*,
    _repair_study_decoded_text,
    _study_latex_markup_issue,
):
    def _study_text_quality_issue(value: Any, *, max_length: int) -> Optional[str]:
        text = str(value or "")
        if not text.strip():
            return "empty"
        if len(text) > max_length:
            return "too_long"
        if any(
            (ord(char) < 32 and char != "\n")
            or 0x7F <= ord(char) <= 0x9F
            or 0xE000 <= ord(char) <= 0xF8FF
            for char in text
        ):
            return "control_character"
        if "\ufffd" in text or any(marker in text for marker in ("Ã", "Â", "â€", "ðŸ", "锟斤拷", "ï»¿")):
            return "encoding_corruption"
        math_validation_text, _protected_code = protect_markdown_code(text)
        if re.search(r"\\(?:\(|\[)\s*(?:\.{3}|\\cdots|null|undefined|[?？])\s*\\(?:\)|\])", math_validation_text, re.IGNORECASE):
            return "formula_placeholder"
        if re.search(r"\\\(\s*\\\)|\\\[\s*\\\]", math_validation_text):
            return "empty_formula"
        latex_issue = _study_latex_markup_issue(math_validation_text)
        if latex_issue:
            return latex_issue

        compact_lines = [line.strip() for line in text.splitlines() if line.strip()]
        if compact_lines:
            line_counts: Dict[str, int] = {}
            for line in compact_lines:
                line_counts[line] = line_counts.get(line, 0) + 1
            most_repeated = max(line_counts.values())
            if most_repeated >= 10 and most_repeated / len(compact_lines) >= 0.45:
                return "repeated_lines"

        inline_atoms = [
            match.strip()
            for match in re.findall(r"\\\((.{1,48}?)\\\)", math_validation_text, flags=re.DOTALL)
        ]
        if inline_atoms:
            atom_counts: Dict[str, int] = {}
            for atom in inline_atoms:
                atom_counts[atom] = atom_counts.get(atom, 0) + 1
            if max(atom_counts.values()) >= 12 and max(atom_counts.values()) / len(inline_atoms) >= 0.45:
                return "repeated_formula"
        return None

    def _study_relation_association_signature(
        association: Any,
        *,
        source_title: Any = "",
        target_title: Any = "",
    ) -> str:
        """Return a stable content key so templated relation copy is not repeated."""
        normalized = _normalize_study_math_markup(association).casefold()
        for title in (source_title, target_title):
            title_text = _normalize_study_math_markup(title).casefold().strip()
            if title_text:
                normalized = normalized.replace(title_text, "{card}")
        return re.sub(r"[\s，。；：、！？,.!?()（）\[\]{}]", "", normalized)

    def _study_relation_association_issue(
        association: Any,
        *,
        source_title: Any = "",
        target_title: Any = "",
    ) -> Optional[str]:
        issue = _study_text_quality_issue(association, max_length=240)
        if issue:
            return issue
        signature = _study_relation_association_signature(
            association,
            source_title=source_title,
            target_title=target_title,
        )
        generic_signatures = (
            "這兩張卡屬於同一份筆記中的直接相關觀念可一起對照複習",
            "這兩張卡適合一起複習",
            "兩張卡適合一起複習",
            "兩者相關可一起複習",
        )
        if not signature or any(generic in signature for generic in generic_signatures):
            return "generic_relation"
        return None

    def _study_has_invalid_negation_counterexample(value: Any) -> bool:
        canonical = str(value or "")

        def matrix_to_vector(match: re.Match[str]) -> str:
            body = match.group(1)
            body = re.sub(r"\\\\(?:\[[^\]]*\])?", ";", body)
            body = body.replace("&", ",")
            return "[" + body + "]"

        canonical = re.sub(
            r"\\begin\{(?:p|b|v|V|B)?matrix\}(.*?)\\end\{(?:p|b|v|V|B)?matrix\}",
            matrix_to_vector,
            canonical,
            flags=re.DOTALL,
        )
        canonical = canonical.replace("\\left", "").replace("\\right", "")
        canonical = canonical.replace("\\(", "").replace("\\)", "")
        pattern = re.compile(
            r"[A-Za-z][A-Za-z0-9_]*\(\s*\[([+\-\d\s,;]+)\]\s*\)\s*"
            r"(?:≠|!=|\\ne)\s*-\s*"
            r"[A-Za-z][A-Za-z0-9_]*\(\s*\[([+\-\d\s,;]+)\]\s*\)"
        )
        for match in pattern.finditer(canonical):
            try:
                left = [int(part) for part in re.split(r"[,;\s]+", match.group(1).strip()) if part]
                right = [int(part) for part in re.split(r"[,;\s]+", match.group(2).strip()) if part]
            except ValueError:
                continue
            if len(left) != len(right) or any(
                left_value != -right_value
                for left_value, right_value in zip(left, right)
            ):
                return True
        return False

    def _normalize_study_math_markup(value: Any) -> str:
        """Make model formula output renderable while leaving ordinary prose alone."""
        text = str(value or "").replace("\r\n", "\n").replace("\r", "\n")
        text, protected_code = protect_markdown_code(text)
        text = _repair_study_decoded_text(text).replace("\t", " ")
        text = re.sub(r"\\n(?=\s|\\[\[(])", "\n", text)
        # Older model output was sometimes JSON-escaped twice, leaving literal
        # ``\\(`` and ``\\beta`` in the browser. Collapse one extra escape only
        # when it precedes a KaTeX delimiter or command.
        structured_environments: List[str] = []
        text = re.sub(
            r"\\\\(?=(?:begin|end)\{(?:matrix|bmatrix|pmatrix|smallmatrix|vmatrix|Vmatrix|array|aligned|cases)\})",
            r"\\",
            text,
        )

        def protect_structured_environment(match: re.Match[str]) -> str:
            structured_environments.append(match.group(0))
            return f"E3LATEXENVPLACEHOLDER{len(structured_environments) - 1}END"

        structured_pattern = re.compile(
            r"\\begin\{(matrix|bmatrix|pmatrix|smallmatrix|vmatrix|Vmatrix|array|aligned|cases)\}"
            r".*?\\end\{\1\}",
            flags=re.DOTALL,
        )
        text = structured_pattern.sub(protect_structured_environment, text)
        for _ in range(3):
            repaired = re.sub(r"\\\\(?=[()A-Za-z])", r"\\", text)
            repaired = re.sub(
                r"\\\\(?=\[(?!\s*-?\d+(?:\.\d+)?(?:pt|em|ex|px|mm|cm|in)\s*\]))",
                r"\\",
                repaired,
            )
            repaired = re.sub(r"\\\\(?=\])", r"\\", repaired)
            if repaired == text:
                break
            text = repaired
        for environment_index, environment_text in enumerate(structured_environments):
            text = text.replace(
                f"E3LATEXENVPLACEHOLDER{environment_index}END",
                environment_text,
            )
        text = re.sub(r"\\begin\{(equation\*?|align\*?|gather\*?)\}", r"\\[", text)
        text = re.sub(r"\\end\{(equation\*?|align\*?|gather\*?)\}", r"\\]", text)
        text = re.sub(r"\$\$(.+?)\$\$", lambda match: "\\[" + match.group(1).strip() + "\\]", text, flags=re.DOTALL)
        text = re.sub(r"(?<!\$)\$(?!\$)([^$\n]+?)(?<!\$)\$(?!\$)", lambda match: "\\(" + match.group(1).strip() + "\\)", text)

        # Structured output can still contain duplicated delimiters or a bare formula
        # embedded in Chinese prose. Repair markup only; never rewrite note content.
        for _ in range(3):
            text = re.sub(r"(?<!\\)\\\[\s*(?<!\\)\\\[", r"\\[", text)
            text = re.sub(r"(?<!\\)\\\]\s*(?<!\\)\\\]", r"\\]", text)
            text = re.sub(r"(?<!\\)\\\(\s*(?<!\\)\\\(", r"\\(", text)
            text = re.sub(r"(?<!\\)\\\)\s*(?<!\\)\\\)", r"\\)", text)
        text = repair_math_delimiters(text)

        math_signal = re.compile(r"(?:=|≠|!=|⇔|→|↔|≤|≥|∈|∉|\\(?:ne|to|le|ge|in|notin|frac|sum|prod|int|begin\{(?:matrix|bmatrix|pmatrix|smallmatrix|vmatrix|Vmatrix|array|aligned|cases)\})|[A-Za-z]\s*\([^\n)]*\)|[A-Za-z]\s*[A-Za-z0-9]*[_^])")
        normalized_lines: List[str] = []
        in_display_math = False
        for line in text.split("\n"):
            stripped = line.strip()
            display_open_count = len(re.findall(r"(?<!\\)\\\[", stripped))
            display_close_count = len(re.findall(r"(?<!\\)\\\]", stripped))
            if (
                not stripped
                or "\\(" in stripped
                or "\\[" in stripped
                or in_display_math
            ):
                normalized_lines.append(line)
                in_display_math = max(
                    0,
                    int(in_display_math) + display_open_count - display_close_count,
                ) > 0
                continue
            if math_signal.search(stripped):
                cjk_count = len(re.findall(r"[\u3400-\u9fff]", stripped))
                if cjk_count == 0 and is_pure_math_expression(stripped):
                    normalized_lines.append(
                        wrap_bare_math_candidate(stripped, display_if_pure=True)
                    )
                    continue
                prefix_match = re.match(r"^(.*?[：:]\s*)(.+)$", stripped)
                if (
                    prefix_match
                    and len(re.findall(r"[\u3400-\u9fff]", prefix_match.group(2))) <= 2
                    and is_pure_math_expression(prefix_match.group(2).strip())
                ):
                    normalized_lines.append(
                        prefix_match.group(1)
                        + wrap_bare_math_candidate(prefix_match.group(2))
                    )
                    continue
            normalized_lines.append(line)
        normalized = "\n".join(normalized_lines).strip()

        protected_math = re.compile(
            r"((?<!\\)\\\[.*?(?<!\\)\\\]|(?<!\\)\\\(.*?(?<!\\)\\\))",
            re.DOTALL,
        )
        bare_candidate = re.compile(r"[A-Za-z0-9\\{}\[\]()`'_^=+\-*/<>|,:;. \t×≠⇔→↔≤≥∈∉]+")
        binary_math = re.compile(
            r"(?:\b[A-Za-z]\b|\d+|[)\]}])\s*[+\-*/]\s*(?:\b[A-Za-z]\b|\d+|[(\[{\\])"
        )
        standalone_symbol = re.compile(r"(?:[A-Za-z]|[A-Z]{2,4})(?:_\{?[^\s}]+\}?|\^\{?[^\s}]+\}?)?")

        def wrap_candidate(match: re.Match[str]) -> str:
            raw = match.group(0)
            body = raw.strip()
            if not body:
                return raw
            looks_like_math = bool(
                math_signal.search(body)
                or binary_math.search(body)
                or standalone_symbol.fullmatch(body)
            )
            if not looks_like_math:
                return raw
            return wrap_bare_math_candidate(raw)

        chunks = protected_math.split(normalized)
        def normalize_plain_chunk(chunk: str) -> str:
            environments: List[str] = []

            def protect_environment(match: re.Match[str]) -> str:
                environments.append(match.group(0))
                return f"E3LATEXBAREENVPH{len(environments) - 1}END"

            repaired = structured_pattern.sub(protect_environment, chunk)
            repaired = bare_candidate.sub(wrap_candidate, repaired)
            for environment_index, environment_text in enumerate(environments):
                repaired = repaired.replace(
                    f"E3LATEXBAREENVPH{environment_index}END",
                    f"\\({environment_text}\\)",
                )
            return repaired

        normalized = "".join(
            chunk if index % 2 else normalize_plain_chunk(chunk)
            for index, chunk in enumerate(chunks)
        )

        def keep_cjk_out_of_math(match: re.Match[str]) -> str:
            opener = match.group(1)
            body = match.group(2)
            closer = "\\)" if opener == "\\(" else "\\]"
            protected_text: List[str] = []

            def protect_text(command_match: re.Match[str]) -> str:
                protected_text.append(command_match.group(0))
                return f"E3LATEXTEXTPH{len(protected_text) - 1}END"

            repaired_body = re.sub(
                r"\\text\{(?:[^{}]|\{[^{}]*\})*\}",
                protect_text,
                body,
            )
            repaired_body = re.sub(
                r"[\u3400-\u9fff]+",
                lambda cjk: f"\\text{{{cjk.group(0)}}}",
                repaired_body,
            )
            for text_index, protected in enumerate(protected_text):
                repaired_body = repaired_body.replace(
                    f"E3LATEXTEXTPH{text_index}END",
                    protected,
                )
            return f"{opener}{repaired_body}{closer}"

        normalized = re.sub(
            r"(\\\(|\\\[)(.*?)(?:\\\)|\\\])",
            keep_cjk_out_of_math,
            normalized,
            flags=re.DOTALL,
        )
        normalized = re.sub(r"\n{3,}", "\n\n", normalized).strip()
        return restore_markdown_code(normalized, protected_code)

    def _normalize_study_library_answer_markup(value: Any) -> str:
        """Repair common model Markdown/LaTeX drift in notebook-wide answers."""
        text = str(value or "").replace("\r\n", "\n").replace("\r", "\n")

        # Some model responses drop the backslashes from display delimiters and
        # emit a line containing only ``[`` / ``]``. Convert only blocks that
        # contain an unmistakable mathematical signal so ordinary prose lists
        # are never reinterpreted as formulas.
        lines = text.split("\n")
        repaired_lines: List[str] = []
        line_index = 0
        display_signal = re.compile(
            r"(?:\\[A-Za-z]+|[=^_]|\\begin\{|\\end\{|[A-Za-z]\s*[A-Za-z0-9]*\s*[<>+*/-])"
        )
        while line_index < len(lines):
            if lines[line_index].strip() != "[":
                repaired_lines.append(lines[line_index])
                line_index += 1
                continue
            closing_index = next(
                (
                    candidate
                    for candidate in range(line_index + 1, min(len(lines), line_index + 36))
                    if lines[candidate].strip() == "]"
                ),
                None,
            )
            if closing_index is None:
                repaired_lines.append(lines[line_index])
                line_index += 1
                continue
            display_body = "\n".join(lines[line_index + 1 : closing_index]).strip()
            if not display_body or not display_signal.search(display_body):
                repaired_lines.append(lines[line_index])
                line_index += 1
                continue
            repaired_lines.extend((r"\[", display_body, r"\]"))
            line_index = closing_index + 1
        text = "\n".join(repaired_lines)

        # Markdown-oriented models often escape subscripts even inside LaTeX.
        text = re.sub(r"\\_(?=[A-Za-z0-9{])", "_", text)

        def repair_matrix_rows(match: re.Match[str]) -> str:
            environment = match.group(1)
            body = match.group(2)
            # Decode duplicated command escapes inside a protected matrix, but
            # retain exactly two slashes for a matrix row boundary.
            body = re.sub(r"\\{2,}(?=[A-Za-z])", r"\\", body)
            body = re.sub(r"(?:\\\\){2,}(?=\s*-?\d)", r"\\\\", body)
            # A single slash followed by a number is never a valid TeX command
            # in these matrix environments; it is a collapsed row separator.
            body = re.sub(r"(?<!\\)\\(?=\s*-?\d)", r"\\\\", body)
            return f"\\begin{{{environment}}}{body}\\end{{{environment}}}"

        text = re.sub(
            r"\\begin\{(matrix|bmatrix|pmatrix|smallmatrix|vmatrix|Vmatrix|array)\}"
            r"(.*?)\\end\{\1\}",
            repair_matrix_rows,
            text,
            flags=re.DOTALL,
        )

        # Convert plain ASCII parentheses back into inline math delimiters when
        # the whole balanced group is mathematical. This handles output such as
        # ``(A^T A)`` and ``(\sigma_i>0)`` without touching Chinese prose.
        stack: List[int] = []
        pairs: List[Tuple[int, int]] = []
        for character_index, character in enumerate(text):
            if character == "(" and (character_index == 0 or text[character_index - 1] != "\\"):
                stack.append(character_index)
            elif character == ")" and (character_index == 0 or text[character_index - 1] != "\\") and stack:
                pairs.append((stack.pop(), character_index))

        math_group = re.compile(
            r"^(?:[A-Za-z]|\\[A-Za-z]+)(?:[A-Za-z0-9\\{}\[\].,'\s_^=<>+*/!-])*$"
        )
        candidates: List[Tuple[int, int]] = []
        for start, end in sorted(pairs, key=lambda pair: (pair[0], -pair[1])):
            body = text[start + 1 : end].strip()
            cjk_count = len(re.findall(r"[\u3400-\u9fff]", body))
            looks_mathematical = bool(
                cjk_count <= 2
                and (
                    re.search(r"\\[A-Za-z]+|[=^_<>]", body)
                    or math_group.fullmatch(body)
                )
            )
            if not looks_mathematical:
                continue
            if any(parent_start <= start and end <= parent_end for parent_start, parent_end in candidates):
                continue
            candidates.append((start, end))
        delimiter_positions = {
            position: replacement
            for start, end in candidates
            for position, replacement in ((start, r"\("), (end, r"\)"))
        }
        if delimiter_positions:
            text = "".join(
                delimiter_positions.get(index, character)
                for index, character in enumerate(text)
            )

        # Turn recognizable section labels into real Markdown headings. This is
        # deliberately conservative and only targets the answer vocabulary.
        section_title = re.compile(
            r"^(步驟總覽|核心結論|重要公式(?:與變數意義)?|公式表(?:格)?|"
            r"常見陷阱(?:與判斷線索)?|考場判斷|範例核對|"
            r"建立考試可直接套用的步驟|解題步驟|觀念比較|常考題型)"
            r"(?:（[^\n]{1,80}）)?[：:]?$"
        )
        heading_lines = []
        for line in text.split("\n"):
            stripped = line.strip()
            if not stripped.startswith("#") and section_title.fullmatch(stripped):
                heading_lines.append(f"## {stripped.rstrip('：:')}")
            else:
                heading_lines.append(line)
        structured_lines: List[str] = []
        in_step_section = False
        for line in heading_lines:
            stripped = line.strip()
            if stripped.startswith("## "):
                in_step_section = bool(
                    re.match(
                        r"## (?:步驟總覽|建立考試可直接套用的步驟|解題步驟)",
                        stripped,
                    )
                )
            if in_step_section and re.match(r"^\d+[.)、]\s+", stripped):
                structured_lines.append(f"### {stripped}")
            else:
                structured_lines.append(line)
        text = "\n".join(structured_lines)
        text = re.sub(
            r"\n*如需，我可以[^\n]*(?:\n[^\n]*)?\s*$",
            "",
            text,
        ).strip()

        # Flatten nested delimiters before the shared delimiter balancer sees
        # them. Otherwise it correctly closes the outer block early and loses
        # grouping parentheses from malformed model output.
        text = re.sub(
            r"(?<!\\)\\\[(.*?)(?<!\\)\\\]",
            lambda match: "\\["
            + re.sub(
                r"(?<!\\)\\\((.*?)(?<!\\)\\\)",
                lambda nested_match: f"({nested_match.group(1)})",
                match.group(1),
                flags=re.DOTALL,
            )
            + "\\]",
            text,
            flags=re.DOTALL,
        )
        text = re.sub(
            r"(?<!\\)\\\((.*?)(?<!\\)\\\)",
            lambda match: "\\("
            + re.sub(r"(?<!\\)\\[\[\]]", "", match.group(1))
            + "\\)",
            text,
            flags=re.DOTALL,
        )
        text = _normalize_study_math_markup(text)

        # Library answers occasionally arrive with one logical equation split
        # into several adjacent math fragments, for example
        # ``\[\det\]\(A\)\(=\det\)\(B,\quad \operatorname{tr}\)...``.
        # KaTeX correctly renders the delimited fragments, but commands left
        # between them remain visible as raw text.  Rejoin math-only lines and
        # wrap any remaining TeX island in mixed prose without changing the
        # surrounding Chinese explanation.
        math_span = re.compile(
            r"((?<!\\)\\\[.*?(?<!\\)\\\]|(?<!\\)\\\(.*?(?<!\\)\\\))",
            re.DOTALL,
        )
        delimiter = re.compile(r"(?<!\\)\\[()\[\]]")
        raw_tex_command = re.compile(
            r"\\(?:operatorname|mathrm|mathbf|mathbb|mathcal|text|frac|sqrt|"
            r"det|ker|rank|dim|tr|quad|qquad|neq|ne|leq|geq|lambda|sigma|"
            r"Delta|begin|end)\b"
        )
        prose_boundary = re.compile(r"([\u3400-\u9fff]+)")

        def plain_math_islands(value: str) -> str:
            pieces = prose_boundary.split(value)
            repaired: List[str] = []
            for piece in pieces:
                if not piece or prose_boundary.fullmatch(piece) or not raw_tex_command.search(piece):
                    repaired.append(piece)
                    continue
                citation_start = piece.find("[", raw_tex_command.search(piece).start())
                suffix = piece[citation_start:] if citation_start >= 0 else ""
                candidate = piece[:citation_start] if citation_start >= 0 else piece
                leading_match = re.match(r"^[\s，。；：、（）!?！？]*", candidate)
                trailing_match = re.search(r"[\s，。；：、（）!?！？]*$", candidate)
                leading = leading_match.group(0) if leading_match else ""
                trailing = trailing_match.group(0) if trailing_match else ""
                end = len(candidate) - len(trailing) if trailing else len(candidate)
                body = candidate[len(leading) : end]
                if body:
                    repaired.append(f"{leading}\\({body}\\){trailing}{suffix}")
                else:
                    repaired.append(piece)
            return "".join(repaired)

        def flatten_display_math(match: re.Match[str]) -> str:
            body = re.sub(
                r"(?<!\\)\\([()])",
                lambda delimiter_match: delimiter_match.group(1),
                match.group(1),
            ).strip()
            return f"\\[{body}\\]"

        text = re.sub(
            r"(?<!\\)\\\[(.*?)(?<!\\)\\\]",
            flatten_display_math,
            text,
            flags=re.DOTALL,
        )

        repaired_answer_lines: List[str] = []
        for line in text.split("\n"):
            stripped = line.strip()
            math_parts = math_span.findall(line)
            outside_math = "".join(math_span.split(line)[::2])
            is_standalone_equation = bool(
                stripped
                and "|" not in stripped
                and not re.match(r"^(?:#{1,4}|[-*]|\d+[.)])\s+", stripped)
                and len(math_parts) >= 2
                and not re.search(r"[\u3400-\u9fff]", stripped)
                and raw_tex_command.search(delimiter.sub("", stripped))
            )
            if is_standalone_equation:
                body = delimiter.sub("", stripped).strip()
                repaired_answer_lines.append(f"\\[{body}\\]")
                continue
            if raw_tex_command.search(outside_math):
                chunks = math_span.split(line)
                line = "".join(
                    chunk if index % 2 else plain_math_islands(chunk)
                    for index, chunk in enumerate(chunks)
                )
            repaired_answer_lines.append(line)
        text = "\n".join(repaired_answer_lines).strip()

        # Remove delimiter pairs accidentally nested inside another formula.
        # Delimiters mark Markdown math; once inside a formula they are invalid
        # LaTeX and make KaTeX expose the source in red.
        delimiter_token = re.compile(r"(?<!\\)\\([()\[\]])")
        matching_closer = {"(": ")", "[": "]"}
        flattened: List[str] = []
        cursor = 0
        outer: Optional[str] = None
        nested: List[str] = []
        for match in delimiter_token.finditer(text):
            flattened.append(text[cursor : match.start()])
            marker = match.group(1)
            if outer is None:
                flattened.append(match.group(0))
                if marker in matching_closer:
                    outer = marker
                    nested = []
            elif marker in matching_closer:
                nested.append(marker)
                if marker == "(":
                    flattened.append("(")
            elif nested and matching_closer[nested[-1]] == marker:
                if nested[-1] == "(":
                    flattened.append(")")
                nested.pop()
            elif not nested and matching_closer[outer] == marker:
                flattened.append(match.group(0))
                outer = None
            # Any mismatched/nested math delimiter is discarded here.
            cursor = match.end()
        flattened.append(text[cursor:])
        text = "".join(flattened)

        def sanitize_formula_body(body: str) -> str:
            repaired = body.strip()
            # A frequent model typo is ``\left\frac`` / ``\right^``. KaTeX
            # requires an actual delimiter after both commands.
            repaired = re.sub(
                r"\\left(?=\s*\\(?:frac|sqrt|sum|prod|int|operatorname|[A-Za-z]+))",
                r"\\left(",
                repaired,
            )
            repaired = re.sub(
                r"\\right(?=\s*(?:[_^]|[,.;:]|$))",
                r"\\right)",
                repaired,
            )
            return repaired

        text = re.sub(
            r"(?<!\\)\\\[(.*?)(?<!\\)\\\]",
            lambda match: f"\\[{sanitize_formula_body(match.group(1))}\\]",
            text,
            flags=re.DOTALL,
        )
        text = re.sub(
            r"(?<!\\)\\\((.*?)(?<!\\)\\\)",
            lambda match: f"\\({sanitize_formula_body(match.group(1))}\\)",
            text,
            flags=re.DOTALL,
        )
        return text.strip()

    def _strip_study_process_narration(value: Any) -> str:
        """Remove model workflow narration without rewriting study content."""
        text = _normalize_study_math_markup(value)
        leading_patterns = (
            r"^(?:保留(?:原始)?來源內容並(?:已)?修正為可直接使用的筆記)",
            r"^(?:(?:本|此|原始)?筆記(?:給出|註明|記載|紀載|整理|說明|指出))",
            r"^(?:(?:根據|依據|依照|參考)(?:原始)?(?:筆記|來源)(?:內容)?)",
            r"^(?:以下(?:是|為)(?:整理後|修正後)?(?:的)?(?:筆記|重點|內容))",
        )
        changed = True
        while changed:
            changed = False
            for pattern in leading_patterns:
                repaired = re.sub(pattern + r"[：:，,。\s]*", "", text, count=1)
                if repaired != text:
                    text = repaired.strip()
                    changed = True
        text = re.sub(
            r"[，,；;]\s*其(?:公式|矩陣|定義|內容)?(?:皆|均|分別)?如(?:原始)?"
            r"(?:來源|原文|筆記)(?:中)?(?:所)?(?:列|示|載|述)[。.]?",
            "。",
            text,
        )
        text = re.sub(
            r"(?:如|詳見)(?:原始)?(?:來源|原文|筆記)(?:中)?(?:所)?(?:列|示|載|述)",
            "",
            text,
        )
        text = re.sub(r"。{2,}", "。", text)
        return re.sub(r"\s+([，。；：])", r"\1", text).strip(" \t\n：:，,")

    def _normalize_study_concept_title(value: Any, fallback: Any = "") -> str:
        """Keep titles compact while preserving formulas required by the card."""

        def clean(candidate: Any) -> str:
            text = _repair_study_decoded_text(candidate).replace("\n", " ").strip()
            text = re.sub(
                r"\s*[（(][^（）()]{0,12}(?:例題|範例|例證|概念|定義|性質|方法)\s*[）)]?\s*$",
                "",
                text,
                flags=re.IGNORECASE,
            )
            text = re.sub(r"\s+", " ", text)
            text = text.strip(" \t:：,，;；/|·。-－—_")
            return _normalize_study_math_markup(text) if text else ""

        return clean(value) or clean(fallback) or "重點觀念"

    return (
        _study_text_quality_issue,
        _study_relation_association_signature,
        _study_relation_association_issue,
        _study_has_invalid_negation_counterexample,
        _normalize_study_math_markup,
        _normalize_study_library_answer_markup,
        _strip_study_process_narration,
        _normalize_study_concept_title,
    )
