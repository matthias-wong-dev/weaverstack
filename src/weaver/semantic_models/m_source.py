"""Recognise an M expression that reads one SQL relation.

Recognition is deliberately narrow: a navigation to one `Schema`/`Item`, reached
through plain step aliases and followed only by standard-library table shaping.
Anything else, including a second source, a native query or an unknown
function, is not recognised, and nothing here raises on unfamiliar M.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*")
_NUMBER = re.compile(r"(?:0[xX][0-9a-fA-F]+|[0-9]*\.?[0-9]+(?:[eE][+-]?[0-9]+)?)")
_PUNCTUATION = ("...", "=>", "<>", "<=", ">=", "..", "??")

# Standard-library namespaces that shape a table already read. Data access
# (Sql, Value, Odbc, Web and the like) is deliberately absent.
_SHAPING = frozenset(
    {
        "Table",
        "List",
        "Record",
        "Text",
        "Number",
        "Date",
        "DateTime",
        "DateTimeZone",
        "Duration",
        "Time",
        "Logical",
        "Splitter",
        "Combiner",
        "Comparer",
        "Replacer",
        "Order",
        "JoinKind",
        "JoinSide",
        "MissingField",
        "Occurrence",
        "QuoteStyle",
        "RoundingMode",
        "ExtraValues",
        "Culture",
        "Int8",
        "Int16",
        "Int32",
        "Int64",
        "Single",
        "Double",
        "Decimal",
        "Currency",
        "Percentage",
        "Byte",
        "Character",
    }
)
_KEYWORDS = frozenset(
    {
        "each",
        "_",
        "type",
        "true",
        "false",
        "null",
        "and",
        "or",
        "not",
        "if",
        "then",
        "else",
        "as",
        "is",
        "meta",
        "nullable",
        "optional",
        "any",
        "anynonnull",
        "none",
        "binary",
        "date",
        "datetime",
        "datetimezone",
        "duration",
        "function",
        "list",
        "logical",
        "number",
        "record",
        "table",
        "text",
        "time",
    }
)


@dataclass(frozen=True)
class Token:
    kind: str  # name, quoted, string, number, intrinsic or punctuation
    text: str
    start: int
    end: int

    @property
    def value(self):
        """A string literal's or quoted name's text, unescaped."""

        if self.kind == "quoted":
            return _unescape(self.text[2:-1])
        if self.kind == "string":
            return _unescape(self.text[1:-1])
        return self.text


def _unescape(text):
    text = text.replace('""', '"')
    return re.sub(
        r"#\((#|lf|cr|tab)\)",
        lambda m: {"#": "#", "lf": "\n", "cr": "\r", "tab": "\t"}[m[1]],
        text,
    )


def _quoted_end(text, index):
    """The index after a `"` literal starting at `index`, or the text's end."""

    index += 1
    while index < len(text):
        if text.startswith('""', index):
            index += 2
        elif text[index] == '"':
            return index + 1
        else:
            index += 1
    return len(text)


def tokens(text):
    """M tokens without whitespace or comments; unknown characters stand alone."""

    found = []
    index = 0
    while index < len(text):
        char = text[index]
        if char.isspace():
            index += 1
        elif text.startswith("//", index):
            end = text.find("\n", index)
            index = len(text) if end < 0 else end
        elif text.startswith("/*", index):
            end = text.find("*/", index + 2)
            index = len(text) if end < 0 else end + 2
        elif char == '"':
            end = _quoted_end(text, index)
            found.append(Token("string", text[index:end], index, end))
            index = end
        elif text.startswith('#"', index):
            end = _quoted_end(text, index + 1)
            found.append(Token("quoted", text[index:end], index, end))
            index = end
        elif char == "#" and (match := _NAME.match(text, index + 1)):
            found.append(Token("intrinsic", "#" + match[0], index, match.end()))
            index = match.end()
        elif match := _NAME.match(text, index):
            found.append(Token("name", match[0], index, match.end()))
            index = match.end()
        elif (match := _NUMBER.match(text, index)) and match[0]:
            found.append(Token("number", match[0], index, match.end()))
            index = match.end()
        else:
            symbol = next((p for p in _PUNCTUATION if text.startswith(p, index)), char)
            found.append(Token("punctuation", symbol, index, index + len(symbol)))
            index += len(symbol)
    return found


def _is(token, text):
    return token.kind in {"punctuation", "name"} and token.text == text


def _identifier(token):
    return token.kind in {"name", "quoted"} and token.text not in _KEYWORDS


def _split(items, separator, *, stop=None):
    """Split at top-level separators, ending at a top-level `stop` name."""

    parts, current, depth = [], [], 0
    for index, token in enumerate(items):
        if token.kind == "punctuation" and token.text in "([{":
            depth += 1
        elif token.kind == "punctuation" and token.text in ")]}":
            depth -= 1
        elif depth == 0 and stop and _is(token, stop):
            parts.append(current)
            return parts, items[index + 1 :]
        elif depth == 0 and _is(token, separator):
            parts.append(current)
            current = []
            continue
        current.append(token)
    parts.append(current)
    return parts, None


@dataclass(frozen=True)
class Relation:
    """The navigation an expression performs, and the root it navigates."""

    root: str | None
    schema: str
    object: str
    root_tokens: tuple[Token, ...] = ()


def _navigation(items):
    """`base{[Schema="s", Item="o"]}[Data]` as (base tokens, schema, object)."""

    if len(items) < 14 or not (
        [t.text for t in items[-4:]] == ["}", "[", "Data", "]"]
        and [t.text for t in items[-14:-12]] == ["{", "["]
        and items[-5].text == "]"
    ):
        return None
    fields = {}
    for field in _split(items[-12:-5], ",")[0]:
        if (
            len(field) != 3
            or field[0].kind != "name"
            or not _is(field[1], "=")
            or field[2].kind != "string"
        ):
            return None
        fields[field[0].text] = field[2].value
    if set(fields) != {"Schema", "Item"}:
        return None
    return items[:-14], fields["Schema"], fields["Item"]


def _shaping(items, steps):
    """The step a standard-library table function shapes, if that is all it reads."""

    if (
        len(items) < 4
        or items[0].kind != "name"
        or items[0].text.split(".", 1)[0] != "Table"
        or not _is(items[1], "(")
        or not _is(items[-1], ")")
    ):
        return None
    arguments, _ = _split(items[2:-1], ",")
    first = arguments[0]
    if len(first) != 1 or first[0].value not in steps:
        return None
    for argument in arguments[1:]:
        if not _pure(argument):
            return None
    return first[0].value


def _pure(items):
    """Whether tokens name nothing but record fields, keywords and shaping functions."""

    # Each open bracket: None, or for a record whether a field name comes next.
    brackets = []
    for token in items:
        if token.kind == "punctuation":
            if token.text in "([{":
                brackets.append(True if token.text == "[" else None)
            elif token.text in ")]}" and brackets:
                brackets.pop()
            elif token.text in {",", "="} and brackets and brackets[-1] is not None:
                brackets[-1] = token.text == ","
            continue
        if brackets and brackets[-1] is True:
            continue
        if token.kind == "quoted":
            return False
        if token.kind == "name" and token.text not in _KEYWORDS:
            namespace, dot, _ = token.text.partition(".")
            if not dot or namespace not in _SHAPING:
                return False
    return True


def relation(text):
    """The one SQL relation an M expression reads, or None when that is unclear."""

    if not isinstance(text, str):
        return None
    items = tokens(text)
    if items and _is(items[0], "let"):
        steps = {}
        bindings, rest = _split(items[1:], ",", stop="in")
        if rest is None or len(rest) != 1 or not _identifier(rest[0]):
            return None
        for binding in bindings:
            if (
                len(binding) < 3
                or not _identifier(binding[0])
                or not _is(binding[1], "=")
            ):
                return None
            if binding[0].value in steps:
                return None
            steps[binding[0].value] = binding[2:]
        current, seen = rest[0].value, set()
        while True:
            if current in seen or current not in steps:
                return None
            seen.add(current)
            body = steps[current]
            found = _navigation(body)
            if found is not None:
                break
            if len(body) == 1 and _identifier(body[0]):
                current = body[0].value
                continue
            current = _shaping(body, steps)
            if current is None:
                return None
        base, schema, name = found
        while len(base) == 1 and _identifier(base[0]) and base[0].value in steps:
            if base[0].value in seen:
                return None
            seen.add(base[0].value)
            base = steps[base[0].value]
    else:
        found = _navigation(items)
        if found is None:
            return None
        base, schema, name = found
    root = base[0].value if len(base) == 1 and _identifier(base[0]) else None
    return Relation(root, schema, name, tuple(base))


def sql_database(items):
    """The server and database tokens of `Sql.Database("s", "d"[, options])`."""

    if len(items) < 6 or not (
        items[0].kind == "name"
        and items[0].text == "Sql.Database"
        and _is(items[1], "(")
        and _is(items[-1], ")")
    ):
        return None
    arguments, _ = _split(items[2:-1], ",")
    if (
        len(arguments) not in {2, 3}
        or any(len(a) != 1 or a[0].kind != "string" for a in arguments[:2])
        or (
            len(arguments) == 3
            and not (
                arguments[2]
                and _is(arguments[2][0], "[")
                and _is(arguments[2][-1], "]")
            )
        )
    ):
        return None
    return arguments[0][0], arguments[1][0]
