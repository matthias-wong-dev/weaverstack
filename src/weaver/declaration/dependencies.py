"""Extract dependency references from Python and SQL source.

Resolution and policy checks occur outside this module.
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
    """A relation name, preserving the parts as written."""

    parts: tuple[str, ...]
    #: True when the name is immediately followed by ``(``, so a table-valued
    #: function call, not a managed object. ``cross apply Sales.SplitLines(…)``
    #: reads like a two-part relation but resolves to a function, so strict
    #: two-part validation exempts it the way it exempts CTEs and temp tables.
    call: bool = False

    @property
    def object_id(self) -> ObjectId | None:
        """The two-part identity; calls and qualified names have none."""

        if len(self.parts) != 2 or self.call:
            return None
        return ObjectId(schema=self.parts[0], object=self.parts[1])

    @property
    def is_qualified(self) -> bool:
        return len(self.parts) > 2

    def __str__(self) -> str:
        return ".".join(self.parts)


@dataclass(frozen=True)
class PythonImport:
    """A Python import with its relative level preserved."""

    module: str | None
    level: int = 0
    names: tuple[str, ...] = ()

    def __str__(self) -> str:
        prefix = "." * self.level
        return prefix + (self.module or "")


def extract_python_references(
    imported_modules: tuple[str, ...],
) -> tuple[RelationReference, ...]:
    """Find ``Schema__Object`` references in absolute imports.

    Both parts must be present and public. Other imports are not object
    references.
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


@dataclass(frozen=True)
class LocatedReference:
    """A relation reference and its exact source span."""

    reference: RelationReference
    start: int
    end: int


def extract_sql_references(sql_text: str) -> tuple[RelationReference, ...]:
    """Return relation references in source order, without duplicates."""

    references: list[RelationReference] = []
    seen: set[tuple[str, ...]] = set()
    for located in locate_sql_references(sql_text):
        if located.reference.parts in seen:
            continue
        seen.add(located.reference.parts)
        references.append(located.reference)
    return tuple(references)


def rewrite_sql_references(sql_text: str, rewrite) -> str:
    """Replace the relation references selected by ``rewrite``.

    ``rewrite`` receives a :class:`RelationReference` and returns the text to put
    in its place, or None to leave it exactly as written. Everything else in the
    body is untouched: whitespace, comments, casing, the author's own delimiters.
    Replacements run last-first so source offsets remain valid.
    """

    replacements = []
    for located in locate_sql_references(sql_text):
        replacement = rewrite(located.reference)
        if replacement is not None:
            replacements.append((located.start, located.end, replacement))

    for start, end, replacement in sorted(replacements, reverse=True):
        sql_text = sql_text[:start] + replacement + sql_text[end:]
    return sql_text


def address_managed_references(sql_text: str, destination) -> str:
    """Qualify managed two-part references for ``destination``.

    Physical names and table-valued function calls remain as authored.
    """

    def rewrite(reference):
        object_id = reference.object_id
        if object_id is None:
            return None
        return destination.qualify(object_id.schema, object_id.object)

    return rewrite_sql_references(sql_text, rewrite)


def locate_sql_references(sql_text: str) -> tuple[LocatedReference, ...]:
    """Locate every occurrence of each relation reference in source order."""

    from sqlparse.exceptions import SQLParseError

    try:
        tokens = _flatten(sql_text)
    except (SQLParseError, RecursionError):
        return _fallback(sql_text)

    references: list[LocatedReference] = []
    seen: set[int] = set()

    for index, token in enumerate(tokens):
        if not _is_keyword(token):
            continue
        head = _keyword_head(token)
        words = set(token.normalized.split())
        if head == "FROM":
            if _enclosing_function(tokens, index) in _FROM_FUNCTIONS:
                continue
            for reference in _from_relations(sql_text, tokens, index):
                _add(references, seen, reference)
        elif head in {"APPLY", "USING"} or "JOIN" in words or "APPLY" in words:
            following = _next_significant(tokens, index + 1)
            if following is not None:
                reference = _relation_at(sql_text, tokens[following].start)
                if reference is not None:
                    _add(references, seen, reference)
        elif head in {"MERGE", "INSERT", "UPDATE", "DELETE"}:
            # A DML target is a relation too. Weaver does not restrict what an
            # author writes; it only has to read it accurately.
            reference = _dml_target(sql_text, tokens, index)
            if reference is not None:
                _add(references, seen, reference)
        elif head in {"CROSS", "OUTER"}:
            # sqlparse keywords `cross` but not `apply`, so `cross apply Schema.Fn(…)`
            # arrives as two tokens and the relation sits after the second.
            following = _next_significant(tokens, index + 1)
            if following is not None and tokens[following].value.lower() == "apply":
                after = _next_significant(tokens, following + 1)
                if after is not None:
                    reference = _relation_at(sql_text, tokens[after].start)
                    if reference is not None:
                        _add(references, seen, reference)

    return tuple(references)


def _dml_target(
    sql_text: str, tokens: list[_FlatToken], index: int
) -> LocatedReference | None:
    """Find a DML target across one- or two-token keyword forms."""

    following = _next_significant(tokens, index + 1)
    if following is None:
        return None
    if tokens[following].normalized.strip() in {"INTO", "FROM"}:
        following = _next_significant(tokens, following + 1)
        if following is None:
            return None
    return _relation_at(sql_text, tokens[following].start)


def _fallback(sql_text: str) -> tuple[LocatedReference, ...]:
    references: list[LocatedReference] = []
    seen: set[int] = set()
    keyword = re.compile(r"\b(from|join|apply|using)\b", flags=re.IGNORECASE)
    for match in keyword.finditer(sql_text):
        located = _relation_at(sql_text, match.end())
        if located is not None:
            _add(references, seen, located)
    return tuple(references)


def _add(
    references: list[LocatedReference],
    seen: set[int],
    located: LocatedReference,
) -> None:
    """Record each source position once, even when two rules find it."""

    if located.start in seen:
        return
    seen.add(located.start)
    references.append(located)


def _from_relations(
    sql_text: str, tokens: list[_FlatToken], from_index: int
) -> list[LocatedReference]:

    depth = tokens[from_index].depth
    first = _next_significant(tokens, from_index + 1)
    if first is None:
        return []

    relations: list[LocatedReference] = []
    reference = _relation_at(sql_text, tokens[first].start)
    if reference is not None:
        relations.append(reference)

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
        reference = _relation_at(sql_text, tokens[following].start)
        if reference is not None:
            relations.append(reference)

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


def _relation_at(sql_text: str, start: int) -> LocatedReference | None:
    parsed = _parse_name(sql_text, start)
    if parsed is None:
        return None
    parts, begin, position = parsed
    # A ``(`` directly abutting the name is a call, ``Sales.SplitLines(…)``.
    # A table hint is ``… with (nolock)``, where a keyword and a space intervene,
    # so requiring the paren to abut avoids mistaking a hinted table for one.
    call = position < len(sql_text) and sql_text[position] == "("
    return LocatedReference(
        reference=RelationReference(parts=parts, call=call), start=begin, end=position
    )


def _parse_name(sql_text: str, start: int) -> tuple[tuple[str, ...], int, int] | None:
    """Parse a replaceable name span, excluding trailing whitespace.

    The exact end distinguishes an abutting function call from a spaced table
    hint.
    """

    position = _skip_space(sql_text, start)
    begin = position
    parts: list[str] = []
    end = position

    while position < len(sql_text):
        parsed = _parse_identifier_part(sql_text, position)
        if parsed is None:
            break
        part, position = parsed
        parts.append(part)
        end = position
        after_space = _skip_space(sql_text, position)
        if after_space >= len(sql_text) or sql_text[after_space] != ".":
            break
        position = _skip_space(sql_text, after_space + 1)
        if len(parts) >= 4:
            break

    if len(parts) < 2 or len(parts) > 4:
        return None
    if any(not part or part.startswith(("#", "@")) for part in parts):
        return None
    if len(parts) == 2 and parts[0].lower() in _PATH_FORMATS:
        # delta.`abfss://…` is a format and a path, not schema and object.
        return None
    return tuple(parts), begin, end


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
