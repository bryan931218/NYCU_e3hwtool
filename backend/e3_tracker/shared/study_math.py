import re
from typing import Optional, Tuple


_RELATION_PATTERN = re.compile(r"(?:!=|<=|>=|≠|≤|≥|⇔|↔|→|∈|∉|(?<![<>=!])=(?!=))")
_TOKEN_PATTERN = re.compile(r"\S+")
_MATH_WORDS = {
    "arg",
    "cos",
    "det",
    "dim",
    "exp",
    "gcd",
    "hom",
    "iff",
    "im",
    "ker",
    "lim",
    "ln",
    "log",
    "max",
    "min",
    "mod",
    "null",
    "rank",
    "sin",
    "span",
    "sup",
    "tan",
    "tr",
}
_ATOMIC_FORMULA_PATTERN = re.compile(
    r"(?<![A-Za-z0-9\\])"
    r"(?:[A-Za-z]|[A-Z]{2,8}|\\[A-Za-z]+(?:\{[^{}\n]*\})?)"
    r"(?:\s*[_^]\s*(?:\{[^{}\n]+\}|[A-Za-z0-9+\-]+))+"
    r"(?![A-Za-z0-9])"
)
_FUNCTION_FORMULA_PATTERN = re.compile(
    r"(?<![A-Za-z0-9])"
    r"(?:rank|dim|ker|span|det|tr|lim|log|sin|cos|tan|O)"
    r"\s*\([^()\n]{1,100}\)"
)
_STRUCTURED_ENVIRONMENT_PATTERN = re.compile(
    r"\\begin\{(matrix|bmatrix|pmatrix|smallmatrix|vmatrix|Vmatrix|array|aligned|cases)\}"
    r".*?\\end\{\1\}",
    flags=re.DOTALL,
)
_MATH_DELIMITER_PATTERN = re.compile(r"(?<!\\)\\([\(\)\[\]])")


def _trim_token(token: str) -> str:
    return token.strip(" \t\r\n,，.。;；:：!?！？")


def _token_is_math(token: str) -> bool:
    prepared = _trim_token(token)
    if not prepared:
        return False
    lowered = prepared.casefold()
    if lowered in _MATH_WORDS:
        return True
    if prepared in {"+", "-", "*", "/", "=", "!=", "<=", ">=", "≠", "≤", "≥", "⇔", "↔", "→", "∈", "∉"}:
        return True
    if re.fullmatch(r"-?\d+(?:\.\d+)?", prepared):
        return True
    if re.fullmatch(r"[A-Za-z]", prepared) or re.fullmatch(r"[A-Z]{2,8}", prepared):
        return True
    if "\\" in prepared and re.search(r"\\[A-Za-z]+", prepared):
        return True
    if re.search(r"[\[\]{}()_^=<>+\-*/|']", prepared) and re.search(r"[A-Za-z0-9]", prepared):
        return True
    return False


def bare_equation_span(value: str) -> Optional[Tuple[int, int]]:
    """Return the formula span around a bare relation without absorbing prose."""
    text = str(value or "")
    relation = _RELATION_PATTERN.search(text)
    if not relation:
        return None
    tokens = list(_TOKEN_PATTERN.finditer(text))
    if not tokens:
        return None
    relation_token_index = next(
        (
            index
            for index, token in enumerate(tokens)
            if token.start() <= relation.start() < token.end()
        ),
        None,
    )
    if relation_token_index is None:
        return None

    start_index = relation_token_index
    while start_index > 0 and _token_is_math(tokens[start_index - 1].group(0)):
        start_index -= 1
    end_index = relation_token_index
    while end_index + 1 < len(tokens) and _token_is_math(tokens[end_index + 1].group(0)):
        end_index += 1

    start = tokens[start_index].start()
    end = tokens[end_index].end()
    while start < relation.start() and text[start] in " \t,，.。;；:：!?！？":
        start += 1
    while end > relation.end() and text[end - 1] in " \t,，.。;；:：!?！？":
        end -= 1
    candidate = text[start:end].strip()
    if (
        not candidate
        or not _RELATION_PATTERN.search(candidate)
        or not re.search(r"[A-Za-z0-9\\\[\](){}]", candidate)
    ):
        return None
    return start, end


def is_pure_math_expression(value: str) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    if _STRUCTURED_ENVIRONMENT_PATTERN.fullmatch(text):
        return True
    span = bare_equation_span(text)
    if span and not text[:span[0]].strip() and not text[span[1]:].strip(" \t,，.。;；"):
        return True
    return bool(
        _ATOMIC_FORMULA_PATTERN.fullmatch(text)
        or _FUNCTION_FORMULA_PATTERN.fullmatch(text)
        or (
            re.search(r"\\(?:frac|sum|prod|int|sqrt|lim)\b", text)
            and not re.search(r"[\u3400-\u9fff]", text)
        )
    )


def repair_math_delimiters(value: str) -> str:
    """Repair unbalanced KaTeX delimiters without changing formula contents."""
    text = str(value or "")
    pieces = []
    cursor = 0
    active_opener: Optional[str] = None
    closing_for = {"(": r"\)", "[": r"\]"}
    for match in _MATH_DELIMITER_PATTERN.finditer(text):
        pieces.append(text[cursor:match.start()])
        marker = match.group(1)
        if marker in {"(", "["}:
            if active_opener is not None:
                pieces.append(closing_for[active_opener])
            pieces.append(match.group(0))
            active_opener = marker
        elif active_opener is not None:
            pieces.append(closing_for[active_opener])
            active_opener = None
        cursor = match.end()
    pieces.append(text[cursor:])
    if active_opener is not None:
        pieces.append(closing_for[active_opener])
    return "".join(pieces)


def wrap_bare_math_candidate(value: str, *, display_if_pure: bool = False) -> str:
    """Wrap only the mathematical part of a delimiter-free candidate."""
    raw = str(value or "")
    if not raw.strip():
        return raw
    leading = raw[: len(raw) - len(raw.lstrip())]
    trailing = raw[len(raw.rstrip()):]
    body = raw.strip()
    if is_pure_math_expression(body):
        math_body = body.rstrip(",，.。;；")
        sentence_suffix = body[len(math_body):]
        opener, closer = ("\\[", "\\]") if display_if_pure else ("\\(", "\\)")
        return f"{leading}{opener}{math_body}{closer}{sentence_suffix}{trailing}"

    span = bare_equation_span(body)
    if span:
        start, end = span
        return (
            f"{leading}{body[:start]}\\({body[start:end]}\\)"
            f"{body[end:]}{trailing}"
        )

    protected_environments = []

    def protect_environment(match: re.Match[str]) -> str:
        protected_environments.append(match.group(0))
        return f"E3MATHENVPH{len(protected_environments) - 1}END"

    body_without_environments = _STRUCTURED_ENVIRONMENT_PATTERN.sub(
        protect_environment,
        body,
    )
    def wrap_atomic(match: re.Match[str]) -> str:
        return f"\\({match.group(0).strip()}\\)"

    protected_functions = []

    def protect_function(match: re.Match[str]) -> str:
        protected_functions.append(match.group(0))
        return f"E3MATHFUNCTIONPH{len(protected_functions) - 1}END"

    repaired = _FUNCTION_FORMULA_PATTERN.sub(
        protect_function,
        body_without_environments,
    )
    repaired = _ATOMIC_FORMULA_PATTERN.sub(wrap_atomic, repaired)
    for index, function in enumerate(protected_functions):
        repaired = repaired.replace(
            f"E3MATHFUNCTIONPH{index}END",
            f"\\({function}\\)",
        )
    for index, environment in enumerate(protected_environments):
        repaired = repaired.replace(
            f"E3MATHENVPH{index}END",
            f"\\({environment}\\)",
        )
    return f"{leading}{repaired}{trailing}"
