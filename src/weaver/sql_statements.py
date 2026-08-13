"""Split SQL text into top-level statements without interpreting a dialect.

The parser identifies statement boundaries while Spark and Warehouse validate
statement semantics.
"""

from __future__ import annotations

from dataclasses import dataclass

import sqlparse
from sqlparse import tokens as T


@dataclass(frozen=True)
class SqlToken:
    """One flattened token, carrying where it was and how deeply nested."""

    value: str
    normalized: str
    ttype: object
    start: int
    end: int
    depth: int


@dataclass(frozen=True)
class SqlStatement:
    """One top-level statement, as it was written.

    ``text`` excludes the terminating semicolon, since what an engine executes
    is the statement rather than the separator. ``terminated`` records whether
    there was one, so a caller can enforce a termination rule without
    re-splitting.
    """

    text: str
    start: int
    end: int
    terminated: bool

    @property
    def keyword(self) -> str:
        """The leading significant keyword, upper-cased, or ``""``.

        Leading comments and whitespace are skipped, so a statement introduced
        by a comment classifies by what it does.
        """

        return first_keyword(self.text)


def parse_statements(sql_text: str) -> tuple[SqlStatement, ...]:
    """Every top-level statement in ``sql_text``, in source order.

    Comment-only and empty runs are not statements and are dropped — including a
    trailing comment after the final semicolon, which must not read as an
    unterminated statement.
    """

    text = sql_text or ""
    statements: list[SqlStatement] = []
    start = 0
    for token in flatten_with_offsets(text):
        if token.depth == 0 and token.value.strip() == ";":
            _append(statements, text, start, token.start, terminated=True)
            start = token.end
    _append(statements, text, start, len(text), terminated=False)
    return tuple(statements)


def split_statements(sql_text: str) -> tuple[str, ...]:
    """One body's top-level statements as plain text, separators removed."""

    return tuple(statement.text for statement in parse_statements(sql_text))


def unterminated(sql_text: str) -> SqlStatement | None:
    """The statement that ran to the end of the text without a semicolon.

    There can only be one, and it is always the last: everything before it was
    ended by the separator that started the next.
    """

    statements = parse_statements(sql_text)
    if statements and not statements[-1].terminated:
        return statements[-1]
    return None


def strip_terminator(sql_text: str) -> str:
    """The text with one trailing statement terminator removed, if it has one.

    For callers embedding an authored body into something that runs it as one
    unit — a shape-only build instruction, a view definition — where the
    separator would be a syntax error.
    """

    stripped = (sql_text or "").strip()
    return stripped[:-1].strip() if stripped.endswith(";") else stripped


def first_keyword(sql_text: str) -> str:
    """The first significant word of a statement, upper-cased.

    ``""`` when there is nothing but whitespace and comments. A leading ``(`` is
    returned as itself, so a caller can recognise a parenthesised query.
    """

    for token in flatten_with_offsets(sql_text):
        if _is_trivia(token):
            continue
        head = token.value.strip()
        if not head:
            continue
        if head.startswith("("):
            return "("
        return head.split()[0].upper()
    return ""


def flatten_with_offsets(sql_text: str) -> list[SqlToken]:
    """Every token in ``sql_text``, flattened, with exact offsets and depth.

    Offsets are into the original string, so a caller slices the source back out
    rather than reassembling it from tokens — which keeps an authored program
    byte-identical through parsing.
    """

    flat: list[SqlToken] = []
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
                SqlToken(
                    value=value,
                    normalized=token.normalized.upper(),
                    ttype=token.ttype,
                    start=offset,
                    end=offset + len(value),
                    depth=token_depth,
                )
            )
            offset += len(value)

            if value == "(":
                depth += 1

    return flat


def is_only_trivia(sql_text: str) -> bool:
    """Whether this text is entirely whitespace and comments."""

    return all(_is_trivia(token) for token in flatten_with_offsets(sql_text))


def _append(
    statements: list[SqlStatement],
    text: str,
    start: int,
    end: int,
    *,
    terminated: bool,
) -> None:
    piece = text[start:end]
    if not piece.strip() or is_only_trivia(piece):
        return
    leading = len(piece) - len(piece.lstrip())
    trailing = len(piece) - len(piece.rstrip())
    statements.append(
        SqlStatement(
            text=piece.strip(),
            start=start + leading,
            end=end - trailing,
            terminated=terminated,
        )
    )


def _is_trivia(token: SqlToken) -> bool:
    return token.ttype in T.Whitespace or token.ttype in T.Comment


__all__ = [
    "SqlStatement",
    "SqlToken",
    "first_keyword",
    "flatten_with_offsets",
    "is_only_trivia",
    "parse_statements",
    "split_statements",
    "strip_terminator",
    "unterminated",
]
