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
    """One name a source file refers to, with its parts as written."""

    parts: tuple[str, ...]
    #: True when the name is immediately followed by ``(`` — a table-valued
    #: function call, not a managed object. ``cross apply Sales.SplitLines(…)``
    #: reads like a two-part relation but resolves to a function, so strict
    #: two-part validation exempts it the way it exempts CTEs and temp tables.
    call: bool = False

    @property
    def object_id(self) -> ObjectId | None:
        """The two-part identity, or None for a call or a qualified name.

        A function call is not a repository object, so it yields no object
        identity even though it has two parts.
        """

        if len(self.parts) != 2 or self.call:
            return None
        return ObjectId(schema=self.parts[0], object=self.parts[1])

    @property
    def is_qualified(self) -> bool:
        """True when the author named a physical target rather than an object."""

        return len(self.parts) > 2

    def __str__(self) -> str:
        return ".".join(self.parts)


@dataclass(frozen=True)
class PythonImport:
    """One Python import with its relative level preserved for item resolution."""

    module: str | None
    level: int = 0
    names: tuple[str, ...] = ()

    def __str__(self) -> str:
        prefix = "." * self.level
        return prefix + (self.module or "")


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


@dataclass(frozen=True)
class LocatedReference:
    """One relation reference, and where in the text it was written.

    Extraction reports what a file says; a *span* additionally lets a caller
    rewrite it. Build needs that: a two-part name in a view body resolves through
    whatever catalogue the session is currently pointed at, so the planner
    replaces each managed reference with a name that says which Lakehouse it
    means (see :mod:`weaver.spark.tokens`).
    """

    reference: RelationReference
    start: int
    end: int


def extract_sql_references(sql_text: str) -> tuple[RelationReference, ...]:
    """Ordered, de-duplicated relation references from a SQL body."""

    references: list[RelationReference] = []
    seen: set[tuple[str, ...]] = set()
    for located in locate_sql_references(sql_text):
        if located.reference.parts in seen:
            continue
        seen.add(located.reference.parts)
        references.append(located.reference)
    return tuple(references)


def rewrite_sql_references(sql_text: str, rewrite) -> str:
    """One body with each relation reference the caller claims replaced.

    ``rewrite`` receives a :class:`RelationReference` and returns the text to put
    in its place, or None to leave it exactly as written. Everything else in the
    body — whitespace, comments, casing, the author's own delimiters — is
    untouched, because a build must not quietly reformat a query it is going to
    freeze and execute.

    Replacements are applied last-first so an earlier span's offsets stay valid.
    """

    replacements = []
    for located in locate_sql_references(sql_text):
        replacement = rewrite(located.reference)
        if replacement is not None:
            replacements.append((located.start, located.end, replacement))

    for start, end, replacement in sorted(replacements, reverse=True):
        sql_text = sql_text[:start] + replacement + sql_text[end:]
    return sql_text


def locate_sql_references(sql_text: str) -> tuple[LocatedReference, ...]:
    """Every relation reference in a SQL body, in order, with its span.

    Not de-duplicated: a name written three times is three places to rewrite.
    """

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
    return _relation_at(sql_text, tokens[following].start)


def _fallback(sql_text: str) -> tuple[LocatedReference, ...]:
    """Scanner for bodies sqlparse cannot tokenise."""

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
    """Record one occurrence, by position.

    Position, not name: extraction de-duplicates by name afterwards, but a
    rewrite needs every place the name was written — and two rules can reach the
    same place, which is the one duplicate to drop here.
    """

    if located.start in seen:
        return
    seen.add(located.start)
    references.append(located)


def _from_relations(
    sql_text: str, tokens: list[_FlatToken], from_index: int
) -> list[LocatedReference]:
    """Every relation in one ``from`` list, including comma-separated ones."""

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
    """A relation reference at ``start``, tagged if it is a function call."""

    parsed = _parse_name(sql_text, start)
    if parsed is None:
        return None
    parts, begin, position = parsed
    # A ``(`` directly abutting the name is a call, ``Sales.SplitLines(…)``.
    # A table hint is ``… with (nolock)`` — a keyword and a space intervene —
    # so requiring the paren to abut avoids mistaking a hinted table for one.
    call = position < len(sql_text) and sql_text[position] == "("
    return LocatedReference(
        reference=RelationReference(parts=parts, call=call), start=begin, end=position
    )


def _parse_name(sql_text: str, start: int) -> tuple[tuple[str, ...], int, int] | None:
    """The parts of a relation name, where it begins, and where it ends.

    The end offset is where the name stops — before any trailing whitespace — so
    a caller can tell an abutting ``(`` (a function call) from a spaced one. The
    begin offset is what makes the name replaceable.
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
        # delta.`abfss://…` — a format and a path, not schema and object.
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
