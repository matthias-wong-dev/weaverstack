"""Static dependency extraction — the names a source file refers to.

Extraction only. Nothing here decides whether a name resolves: a two-part name
may be an object in this repository, a shortcut declared elsewhere, or a typo,
and telling those apart needs the external-dependency configuration supplied at
build. This module's whole job is to report, accurately, what the file says.

**Python** declares a dependency by importing the other object's module. The
marker is structural — one ``__`` in an absolute import name::

    from Sales__Order import Sales__Order    ->  Sales.Order
    from weaver import Table                 ->  not a reference
    from ._helpers.dates import parse        ->  not a reference

**SQL** declares them by relation position — after ``from``, ``join``,
``apply`` or ``using``. Names are returned with their delimiters removed and
their part count intact:

===================================  ==========================================
``Schema.Object``                    two parts — Weaver's namespace
``Catalogue.Schema.Object``          three parts — a physical thing, named by
                                     the author
``Server.Catalogue.Schema.Object``   four parts — likewise
===================================  ==========================================

Single-part names are never relations. A CTE, a temp view, a temp table and a
table alias are all single-part, so requiring two parts excludes every one of
them without tracking scope.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from .metadata import ObjectId

_TOKENS = None


def _tokens():
    global _TOKENS
    if _TOKENS is None:
        from sqlparse import tokens as _t

        _TOKENS = _t
    return _TOKENS


@dataclass(frozen=True)
class RelationReference:
    """One name a source file refers to, with its parts as written."""

    parts: tuple[str, ...]

    @property
    def object_id(self) -> ObjectId | None:
        """The two-part identity, or None for a physically-qualified name."""

        if len(self.parts) != 2:
            return None
        return ObjectId(schema=self.parts[0], object=self.parts[1])

    @property
    def is_qualified(self) -> bool:
        """True when the author named a physical target rather than an object."""

        return len(self.parts) > 2

    def __str__(self) -> str:
        return ".".join(self.parts)


# --- Python -----------------------------------------------------------------


def extract_python_references(imported_modules: tuple[str, ...]) -> tuple[RelationReference, ...]:
    """Object references among a module's absolute imports.

    Structural: exactly one ``__``, with both sides present and neither
    beginning with an underscore. ``weaver`` has no ``__`` and is not a
    reference; a helper reached as ``_helpers.dates`` contributes its package
    name, which likewise is not one.
    """

    references: list[RelationReference] = []
    seen: set[tuple[str, ...]] = set()
    for name in imported_modules:
        if name.startswith("_"):
            continue
        parts = name.split("__")
        if len(parts) != 2:
            continue
        if not all(part and not part.startswith("_") for part in parts):
            continue
        key = tuple(parts)
        if key not in seen:
            seen.add(key)
            references.append(RelationReference(parts=key))
    return tuple(references)


# --- SQL --------------------------------------------------------------------

_FROM_BOUNDARY_KEYWORDS = {
    "FOR",
    "GO",
    "GROUP",
    "HAVING",
    "OPTION",
    "ORDER",
    "UNION",
    "EXCEPT",
    "INTERSECT",
    "WHERE",
    "LATERAL",
    "PIVOT",
    "UNPIVOT",
    "WINDOW",
    "QUALIFY",
    "CLUSTER",
    "DISTRIBUTE",
    "SORT",
    "LIMIT",
}

#: ``trim(chars from value)`` is not a relation position.
_FROM_FUNCTIONS = {"TRIM", "SUBSTRING", "EXTRACT", "OVERLAY", "POSITION"}

_STATEMENT_START_KEYWORDS = {
    "ALTER",
    "CREATE",
    "DELETE",
    "DROP",
    "INSERT",
    "MERGE",
    "SELECT",
    "SET",
    "TRUNCATE",
    "UPDATE",
    "USE",
}

#: Spark reads a path as ``delta.`abfss://…```. The prefix is a format, not a
#: schema, so the pair is not an object reference.
_PATH_FORMATS = {"delta", "parquet", "csv", "json", "orc", "avro", "text", "binaryfile"}


@dataclass(frozen=True)
class _FlatToken:
    value: str
    normalized: str
    ttype: object
    start: int
    depth: int


def extract_sql_references(sql_text: str) -> tuple[RelationReference, ...]:
    """Ordered, de-duplicated relation references from a SQL body."""

    from sqlparse.exceptions import SQLParseError

    try:
        tokens = _flatten(sql_text)
    except (SQLParseError, RecursionError):
        return _fallback(sql_text)

    references: list[RelationReference] = []
    seen: set[tuple[str, ...]] = set()

    for index, token in enumerate(tokens):
        if not _is_keyword(token):
            continue
        head = _keyword_head(token)
        words = set(token.normalized.split())
        if head == "FROM":
            if _enclosing_function(tokens, index) in _FROM_FUNCTIONS:
                continue
            for parts in _from_relations(sql_text, tokens, index):
                _add(references, seen, parts)
        elif head in {"APPLY", "USING"} or "JOIN" in words or "APPLY" in words:
            following = _next_significant(tokens, index + 1)
            if following is not None:
                parts = _parse_name(sql_text, tokens[following].start)
                if parts is not None:
                    _add(references, seen, parts)
        elif head in {"MERGE", "INSERT", "UPDATE", "DELETE"}:
            # A DML target is a relation too. Weaver does not restrict what an
            # author writes; it only has to read it accurately.
            parts = _dml_target(sql_text, tokens, index)
            if parts is not None:
                _add(references, seen, parts)
        elif head in {"CROSS", "OUTER"}:
            # sqlparse keywords `cross` but not `apply`, so `cross apply Schema.Fn(…)`
            # arrives as two tokens and the relation sits after the second.
            following = _next_significant(tokens, index + 1)
            if following is not None and tokens[following].value.lower() == "apply":
                after = _next_significant(tokens, following + 1)
                if after is not None:
                    parts = _parse_name(sql_text, tokens[after].start)
                    if parts is not None:
                        _add(references, seen, parts)

    return tuple(references)


def _dml_target(
    sql_text: str, tokens: list[_FlatToken], index: int
) -> tuple[str, ...] | None:
    """The relation a DML statement writes to.

    ``insert into``, ``merge into`` and ``delete from`` may arrive as one
    keyword token or two, depending on the dialect and on sqlparse, so an
    intervening ``into``/``from`` is skipped when present.
    """

    following = _next_significant(tokens, index + 1)
    if following is None:
        return None
    if tokens[following].normalized.strip() in {"INTO", "FROM"}:
        following = _next_significant(tokens, following + 1)
        if following is None:
            return None
    return _parse_name(sql_text, tokens[following].start)


def _fallback(sql_text: str) -> tuple[RelationReference, ...]:
    """Scanner for bodies sqlparse cannot tokenise."""

    references: list[RelationReference] = []
    seen: set[tuple[str, ...]] = set()
    keyword = re.compile(r"\b(from|join|apply|using)\b", flags=re.IGNORECASE)
    for match in keyword.finditer(sql_text):
        parts = _parse_name(sql_text, match.end())
        if parts is not None:
            _add(references, seen, parts)
    return tuple(references)


def _add(
    references: list[RelationReference],
    seen: set[tuple[str, ...]],
    parts: tuple[str, ...],
) -> None:
    if parts in seen:
        return
    seen.add(parts)
    references.append(RelationReference(parts=parts))


def _from_relations(
    sql_text: str, tokens: list[_FlatToken], from_index: int
) -> list[tuple[str, ...]]:
    """Every relation in one ``from`` list, including comma-separated ones."""

    depth = tokens[from_index].depth
    first = _next_significant(tokens, from_index + 1)
    if first is None:
        return []

    relations: list[tuple[str, ...]] = []
    parts = _parse_name(sql_text, tokens[first].start)
    if parts is not None:
        relations.append(parts)

    for index in range(first + 1, len(tokens)):
        token = tokens[index]
        if token.depth < depth:
            break
        if token.depth != depth:
            continue
        if _is_from_boundary(token):
            break
        if token.value != ",":
            continue
        following = _next_significant(tokens, index + 1)
        if following is None or tokens[following].depth != depth:
            continue
        parts = _parse_name(sql_text, tokens[following].start)
        if parts is not None:
            relations.append(parts)

    return relations


def _is_from_boundary(token: _FlatToken) -> bool:
    if token.value == ";":
        return True
    if not _is_keyword(token):
        return False
    head = _keyword_head(token)
    if head in _FROM_BOUNDARY_KEYWORDS:
        return True
    return head in _STATEMENT_START_KEYWORDS and head != "SELECT"


def _parse_name(sql_text: str, start: int) -> tuple[str, ...] | None:
    position = _skip_space(sql_text, start)
    parts: list[str] = []

    while position < len(sql_text):
        parsed = _parse_identifier_part(sql_text, position)
        if parsed is None:
            break
        part, position = parsed
        parts.append(part)
        position = _skip_space(sql_text, position)
        if position >= len(sql_text) or sql_text[position] != ".":
            break
        position = _skip_space(sql_text, position + 1)
        if len(parts) >= 4:
            break

    if len(parts) < 2 or len(parts) > 4:
        return None
    if any(not part or part.startswith(("#", "@")) for part in parts):
        return None
    if len(parts) == 2 and parts[0].lower() in _PATH_FORMATS:
        # delta.`abfss://…` — a format and a path, not schema and object.
        return None
    return tuple(parts)


def _parse_identifier_part(sql_text: str, start: int) -> tuple[str, int] | None:
    if start >= len(sql_text):
        return None
    character = sql_text[start]
    if character == "[":
        return _parse_delimited(sql_text, start, "]")
    if character == '"':
        return _parse_delimited(sql_text, start, '"')
    if character == "`":
        return _parse_delimited(sql_text, start, "`")
    match = re.match(r"[A-Za-z_@#][A-Za-z0-9_@$#]*", sql_text[start:])
    if not match:
        return None
    return match.group(0), start + match.end()


def _parse_delimited(sql_text: str, start: int, closer: str) -> tuple[str, int] | None:
    position = start + 1
    characters: list[str] = []
    while position < len(sql_text):
        character = sql_text[position]
        if character == closer:
            if position + 1 < len(sql_text) and sql_text[position + 1] == closer:
                characters.append(closer)
                position += 2
                continue
            return "".join(characters), position + 1
        characters.append(character)
        position += 1
    return None


def _skip_space(sql_text: str, start: int) -> int:
    position = start
    while position < len(sql_text) and sql_text[position] in " \t\r\n":
        position += 1
    return position


def _flatten(sql_text: str) -> list[_FlatToken]:
    import sqlparse

    flat: list[_FlatToken] = []
    offset = 0
    depth = 0
    for statement in sqlparse.parse(sql_text):
        for token in statement.flatten():
            value = token.value
            token_depth = depth
            if value == ")":
                depth = max(0, depth - 1)
                token_depth = depth
            flat.append(
                _FlatToken(
                    value=value,
                    normalized=token.normalized.upper(),
                    ttype=token.ttype,
                    start=offset,
                    depth=token_depth,
                )
            )
            offset += len(value)
            if value == "(":
                depth += 1
    return flat


def _next_significant(tokens: list[_FlatToken], index: int) -> int | None:
    for candidate in range(index, len(tokens)):
        if not _is_trivia(tokens[candidate]):
            return candidate
    return None


def _previous_significant(tokens: list[_FlatToken], index: int) -> int | None:
    for candidate in range(index - 1, -1, -1):
        if not _is_trivia(tokens[candidate]):
            return candidate
    return None


def _enclosing_function(tokens: list[_FlatToken], index: int) -> str | None:
    """The function keyword of the parenthesis enclosing ``index``, if any."""

    depth = 0
    for candidate in range(index - 1, -1, -1):
        value = tokens[candidate].value
        if value == ")":
            depth += 1
        elif value == "(":
            if depth == 0:
                previous = _previous_significant(tokens, candidate)
                if previous is None:
                    return None
                return _keyword_head(tokens[previous])
            depth -= 1
    return None


def _is_trivia(token: _FlatToken) -> bool:
    tokens = _tokens()
    return token.ttype in tokens.Whitespace or token.ttype in tokens.Comment


def _is_keyword(token: _FlatToken) -> bool:
    return token.ttype in _tokens().Keyword


def _keyword_head(token: _FlatToken) -> str:
    parts = token.normalized.split(maxsplit=1)
    return parts[0] if parts else ""
