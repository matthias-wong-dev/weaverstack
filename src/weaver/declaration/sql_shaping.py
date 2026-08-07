"""Offset-exact T-SQL text transforms for shape-only query materialisation.

A Warehouse table's inferred build runs its query in *shape-only* form: every
``SELECT`` is guarded to return its columns and no rows, and the final result is
diverted into a temp table whose metadata the generated script then reads. Those
two rewrites — :func:`insert_where_one_eq_zero` and :func:`insert_select_into` —
work over a flattened, offset-carrying token stream rather than by string
munging, so nested queries, CTEs, set operations and existing ``WHERE`` clauses
are handled correctly.

**Everything here is T-SQL's.** The keyword sets, the ``GO`` boundary, the
``SELECT INTO`` placement and the shape-only guard are all this dialect's, and
they stay here for that reason. What is *not* dialect-specific — flattening text
into offset-carrying tokens, and finding where one statement ends — lives in
:mod:`weaver.sql_statements`, because Spark SQL needs the same answer and a
second splitter would be a second set of bugs about string literals.

Ported from the proven ``weaver_runtime.dbrep.sql.wrangle`` reference
implementation; only the dependency-finding and CTAS helpers (which weaverstack
covers elsewhere) were dropped. :func:`render_sql_template` fills the T-SQL DDL
templates in ``ses/templates``.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
from string import Template

from sqlparse import tokens as T

from ..sql_statements import SqlToken as _FlatToken
from ..sql_statements import flatten_with_offsets as _flatten_with_offsets


SQL_TEMPLATE_DIR = Path(__file__).resolve().parent / "templates"


_BOUNDARY_KEYWORDS = {
    "GO",
    "GROUP",
    "HAVING",
    "ORDER",
    "UNION",
    "EXCEPT",
    "INTERSECT",
    "OPTION",
    "FOR",
}

_STATEMENT_START_KEYWORDS = {
    "ALTER",
    "CREATE",
    "DECLARE",
    "DELETE",
    "DROP",
    "EXEC",
    "EXECUTE",
    "IF",
    "INSERT",
    "MERGE",
    "PRINT",
    "RAISERROR",
    "RETURN",
    "SELECT",
    "SET",
    "THROW",
    "TRUNCATE",
    "UPDATE",
    "USE",
    "WAITFOR",
    "WHILE",
}


@dataclass(frozen=True)
class _Replacement:
    start: int
    end: int
    text: str


@dataclass(frozen=True)
class _QuerySpan:
    start: int
    end: int
    select_index: int


def insert_where_one_eq_zero(sql_text: str) -> str:
    """Insert ``WHERE 1=0`` into every SELECT in a T-SQL string.

    If a SELECT already has a WHERE clause, its existing condition is wrapped in
    parentheses and combined with ``AND 1=0``.
    """

    replacements = _collect_replacements(sql_text)
    if not replacements:
        return sql_text

    result = sql_text
    for replacement in sorted(replacements, key=lambda item: item.start, reverse=True):
        result = (
            result[: replacement.start] + replacement.text + result[replacement.end :]
        )
    return result


def insert_select_into(sql_text: str, table_name: str) -> str:
    """Insert ``INTO <table_name>`` into the last standalone SELECT query."""

    query_span = _find_last_standalone_query(sql_text)
    if query_span is None:
        return sql_text

    tokens = _flatten_with_offsets(sql_text)
    insert_at = _find_select_into_insert_position(tokens, query_span)
    insert_text = _select_into_text(sql_text, insert_at, table_name)
    return f"{sql_text[:insert_at]}{insert_text}{sql_text[insert_at:]}"


def get_sql_template(template_name: str) -> str:
    """Fetch a SQL template from ``source/sql_templates``."""

    template_path = _sql_template_path(template_name)
    return template_path.read_text(encoding="utf-8")


def render_sql_template(template_name: str, **values: object) -> str:
    """Fetch and populate a SQL template with ``string.Template`` values."""

    template = Template(get_sql_template(template_name))
    return template.substitute({key: str(value) for key, value in values.items()})


def _sql_template_path(template_name: str) -> Path:
    normalised_name = template_name if template_name.endswith(".sql") else f"{template_name}.sql"
    candidate = (SQL_TEMPLATE_DIR / normalised_name).resolve()
    template_root = SQL_TEMPLATE_DIR.resolve()
    if template_root not in candidate.parents:
        raise ValueError("template_name must stay within the SQL template directory")
    if not candidate.is_file():
        raise FileNotFoundError(f"SQL template not found: {template_name}")
    return candidate


def _collect_replacements(sql_text: str) -> list[_Replacement]:
    tokens = _flatten_with_offsets(sql_text)
    replacements: list[_Replacement] = []
    covered_ranges: list[tuple[int, int]] = []

    for index, token in enumerate(tokens):
        if not _is_select(token):
            continue

        if _is_covered(token.start, covered_ranges):
            continue

        replacement = _replacement_for_select(sql_text, tokens, index)
        if replacement is None:
            continue

        replacements = [
            item
            for item in replacements
            if not (replacement.start <= item.start and item.end <= replacement.end)
        ]
        if replacement.start != replacement.end:
            covered_ranges.append((replacement.start, replacement.end))
        replacements.append(replacement)

    return replacements


def _find_last_standalone_query(sql_text: str) -> _QuerySpan | None:
    tokens = _flatten_with_offsets(sql_text)
    spans: list[_QuerySpan] = []

    for index, token in enumerate(tokens):
        if token.depth != 0:
            continue

        if _is_select(token) and _is_standalone_select_start(tokens, index):
            spans.append(_QuerySpan(token.start, _find_query_end(tokens, index), index))
            continue

        if _keyword_head(token) == "WITH" and _is_statement_boundary_before(
            tokens, index
        ):
            select_index = _find_cte_body_select(tokens, index)
            if select_index is not None:
                spans.append(
                    _QuerySpan(
                        token.start, _find_query_end(tokens, select_index), select_index
                    )
                )

    if not spans:
        return None

    return max(spans, key=lambda item: item.start)


def _find_select_into_insert_position(
    tokens: list[_FlatToken], query_span: _QuerySpan
) -> int:
    select_token = tokens[query_span.select_index]

    for index in range(query_span.select_index + 1, len(tokens)):
        token = tokens[index]
        if token.start >= query_span.end:
            break
        if token.depth != select_token.depth:
            continue
        if _keyword_head(token) == "FROM":
            return _end_before_trivia(tokens, query_span.select_index + 1, index)
        if _is_select_into_boundary(token):
            return _end_before_trivia(tokens, query_span.select_index + 1, index)

    end_index = _token_index_at_or_after(tokens, query_span.end)
    return _end_before_trivia(tokens, query_span.select_index + 1, end_index)


def _select_into_text(sql_text: str, insert_at: int, table_name: str) -> str:
    if insert_at > 0 and sql_text[insert_at - 1] == "\n":
        return f"into {table_name}\n"
    next_non_space = insert_at
    while next_non_space < len(sql_text) and sql_text[next_non_space] in " \t\r\n":
        if sql_text[next_non_space] == "\n":
            return f"\ninto {table_name}"
        next_non_space += 1
    return f" into {table_name}"


def _is_select_into_boundary(token: _FlatToken) -> bool:
    keyword = _keyword_head(token)
    return token.value == ";" or keyword in {
        "WHERE",
        "GROUP",
        "HAVING",
        "ORDER",
        "UNION",
        "EXCEPT",
        "INTERSECT",
        "OPTION",
        "FOR",
        "GO",
    }


def _token_index_at_or_after(tokens: list[_FlatToken], position: int) -> int:
    for index, token in enumerate(tokens):
        if token.start >= position:
            return index
    return len(tokens)


def _find_cte_body_select(tokens: list[_FlatToken], with_index: int) -> int | None:
    for index in range(with_index + 1, len(tokens)):
        token = tokens[index]
        if token.depth != 0:
            continue
        if _is_select(token):
            return index
        if token.value == ";" or _keyword_head(token) == "GO":
            return None
        if _is_statement_starter(token) and _keyword_head(token) not in {"WITH", "SELECT"}:
            return None
    return None


def _find_query_end(tokens: list[_FlatToken], select_index: int) -> int:
    select_token = tokens[select_index]

    for index in range(select_index + 1, len(tokens)):
        token = tokens[index]
        if token.depth < select_token.depth:
            return token.start
        if token.depth != select_token.depth:
            continue
        if token.value == ";" or _keyword_head(token) == "GO":
            return token.end if token.value == ";" else _end_before_trivia(tokens, select_index, index)
        if _is_statement_starter(token) and not _is_set_operator_select(tokens, index):
            return _end_before_trivia(tokens, select_index, index)

    return _end_before_trivia(tokens, select_index, len(tokens))


def _is_standalone_select_start(tokens: list[_FlatToken], index: int) -> bool:
    if _is_set_operator_select(tokens, index):
        return False
    if _has_top_level_with_since_boundary(tokens, index):
        return False
    if _is_statement_boundary_before(tokens, index):
        return True
    if not _starts_new_line(tokens, index):
        return False

    starter = _last_statement_starter_since_boundary(tokens, index)
    if starter is None:
        return True
    if _keyword_head(tokens[starter]) == "INSERT":
        return _has_top_level_select_between(tokens, starter + 1, index)
    return True


def _is_set_operator_select(tokens: list[_FlatToken], index: int) -> bool:
    previous = _previous_significant_index(tokens, index)
    if previous is None:
        return False

    previous_keyword = _keyword_head(tokens[previous])
    if previous_keyword in {"UNION", "EXCEPT", "INTERSECT"}:
        return True
    if previous_keyword == "ALL":
        before_all = _previous_significant_index(tokens, previous)
        return before_all is not None and _keyword_head(tokens[before_all]) in {
            "UNION",
            "EXCEPT",
            "INTERSECT",
        }
    return False


def _is_statement_boundary_before(tokens: list[_FlatToken], index: int) -> bool:
    previous = _previous_significant_index(tokens, index)
    if previous is None:
        return True

    previous_token = tokens[previous]
    return previous_token.value == ";" or _keyword_head(previous_token) == "GO"


def _starts_new_line(tokens: list[_FlatToken], index: int) -> bool:
    for previous in range(index - 1, -1, -1):
        token = tokens[previous]
        if token.depth != tokens[index].depth:
            continue
        if "\n" in token.value:
            return True
        if not _is_trivia(token):
            return False
    return True


def _last_statement_starter_since_boundary(
    tokens: list[_FlatToken], index: int
) -> int | None:
    for previous in range(index - 1, -1, -1):
        token = tokens[previous]
        if token.depth != tokens[index].depth or _is_trivia(token):
            continue
        if token.value == ";" or _keyword_head(token) == "GO":
            return None
        if _is_statement_starter(token):
            return previous
    return None


def _has_top_level_select_between(
    tokens: list[_FlatToken], start: int, end: int
) -> bool:
    return any(token.depth == 0 and _is_select(token) for token in tokens[start:end])


def _has_top_level_with_since_boundary(tokens: list[_FlatToken], index: int) -> bool:
    for previous in range(index - 1, -1, -1):
        token = tokens[previous]
        if token.depth != tokens[index].depth or _is_trivia(token):
            continue
        if token.value == ";" or _keyword_head(token) == "GO":
            return False
        if _keyword_head(token) == "WITH":
            return True
    return False


def _replacement_for_select(
    sql_text: str, tokens: list[_FlatToken], select_index: int
) -> _Replacement | None:
    select_token = tokens[select_index]
    scope_end_index = _find_scope_end(tokens, select_index)
    where_index = _find_where(tokens, select_index + 1, scope_end_index, select_token.depth)

    if where_index is None:
        insert_at = _find_insert_position(
            tokens, select_index + 1, scope_end_index, select_token.depth
        )
        return _Replacement(insert_at, insert_at, " where 1=0")

    condition_start = tokens[where_index].end
    condition_end = _find_condition_end(
        tokens, where_index + 1, scope_end_index, select_token.depth
    )
    condition = sql_text[condition_start:condition_end].strip()
    transformed_condition = insert_where_one_eq_zero(condition) if condition else condition
    return _Replacement(
        condition_start,
        condition_end,
        f" ({transformed_condition}) and 1=0",
    )


def _find_scope_end(tokens: list[_FlatToken], select_index: int) -> int:
    select_token = tokens[select_index]

    for index in range(select_index + 1, len(tokens)):
        token = tokens[index]
        if token.depth < select_token.depth:
            return index
        if token.depth == select_token.depth and _is_scope_terminator(token):
            return index

    return len(tokens)


def _find_where(
    tokens: list[_FlatToken], start: int, end: int, depth: int
) -> int | None:
    for index in range(start, end):
        token = tokens[index]
        if token.depth == depth and token.normalized == "WHERE":
            return index
    return None


def _find_insert_position(
    tokens: list[_FlatToken], start: int, end: int, depth: int
) -> int:
    for index in range(start, end):
        token = tokens[index]
        if token.depth == depth and _is_boundary(token):
            return _end_before_trivia(tokens, start, index)

    if end < len(tokens):
        return _end_before_trivia(tokens, start, end)
    if tokens:
        return _end_before_trivia(tokens, start, len(tokens))
    return 0


def _find_condition_end(
    tokens: list[_FlatToken], start: int, end: int, depth: int
) -> int:
    for index in range(start, end):
        token = tokens[index]
        if token.depth == depth and _is_boundary(token):
            return _end_before_trivia(tokens, start, index)

    if end < len(tokens):
        return _end_before_trivia(tokens, start, end)
    if tokens:
        return _end_before_trivia(tokens, start, len(tokens))
    return 0


def _end_before_trivia(tokens: list[_FlatToken], start: int, end: int) -> int:
    index = end - 1
    while index >= start and tokens[index].ttype in T.Whitespace:
        index -= 1
    if index >= start:
        return tokens[index].end
    return tokens[start].start if start < len(tokens) else 0


def _is_select(token: _FlatToken) -> bool:
    return token.ttype is T.DML and token.normalized == "SELECT"


def _is_boundary(token: _FlatToken) -> bool:
    return token.value == ";" or _keyword_head(token) in _BOUNDARY_KEYWORDS


def _is_scope_terminator(token: _FlatToken) -> bool:
    if token.value == ";":
        return True

    keyword = _keyword_head(token)
    return keyword in {"GO", "UNION", "EXCEPT", "INTERSECT"} or (
        _is_statement_starter(token) and keyword != "SELECT"
    ) or _is_select(token)


def _is_statement_starter(token: _FlatToken) -> bool:
    return _keyword_head(token) in _STATEMENT_START_KEYWORDS


def _keyword_head(token: _FlatToken) -> str:
    parts = token.normalized.split(maxsplit=1)
    return parts[0] if parts else ""


def _previous_significant_index(
    tokens: list[_FlatToken], index: int, depth: int = 0
) -> int | None:
    for previous in range(index - 1, -1, -1):
        token = tokens[previous]
        if token.depth != depth or _is_trivia(token):
            continue
        return previous
    return None

def _is_trivia(token: _FlatToken) -> bool:
    return token.ttype in T.Whitespace or token.ttype in T.Comment


def _is_covered(position: int, ranges: list[tuple[int, int]]) -> bool:
    return any(start <= position < end for start, end in ranges)


def split_trailing_query(sql_text: str) -> tuple[str, str]:
    """Split a body into everything before its final query, and that query.

    A Weaver object's body is not always one statement. An author may set a
    temporary view up first and select from it, and that shape is supported —
    the repository's own fixtures use it. So a load cannot wrap the whole body
    in a subquery to stage it: only the *last standalone query* produces the
    rows, and whatever precedes it has to run first, on its own.

    This is the same span the T-SQL build uses to place its ``INTO``
    (:func:`insert_select_into`), reused rather than re-derived so build and
    load agree about which part of a body is the query.

    Returns ``(preamble, query)``. ``preamble`` is empty for the ordinary
    single-statement body.
    """

    span = _find_last_standalone_query(sql_text)
    if span is None:
        return "", sql_text.strip()
    preamble = sql_text[: span.start].strip().rstrip(";").strip()
    query = sql_text[span.start : span.end].strip().rstrip(";").strip()
    return preamble, query
