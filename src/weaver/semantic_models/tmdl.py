"""TMDL source spans for edits to named objects and properties."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from urllib.parse import quote

from ..errors import ConfigError

_NAME = r"(?:'(?:[^']|'')*'|[^\s=:'\"]+)"
_OBJECT = re.compile(
    rf"(?P<ref>ref\s+)?(?P<kind>[A-Za-z][A-Za-z0-9]*)"
    rf"(?:\s+(?P<name>{_NAME}))?(?:\s*=\s*(?P<value>.*))?\Z",
    re.IGNORECASE,
)
_NAMED = frozenset(
    {
        "database",
        "model",
        "table",
        "column",
        "measure",
        "partition",
        "relationship",
        "expression",
        "role",
        "tablepermission",
        "hierarchy",
        "level",
        "annotation",
        # `changedProperty = IsHidden` is an object, though it reads like an
        # expression property; properties must precede it.
        "changedproperty",
    }
)


def object_name(value):
    return value[1:-1].replace("''", "'") if value.startswith("'") else value


def quote_name(value):
    return "'" + value.replace("'", "''") + "'"


def indent_width(line):
    prefix = line[: len(line) - len(line.lstrip(" \t"))]
    return len(prefix.expandtabs(4))


def text_value(value):
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, int):
        return str(value)
    if not isinstance(value, str) or "\n" in value or "\r" in value:
        raise ConfigError("TMDL scalar properties require a single-line value")
    if value != value.strip() or '"' in value or not value:
        return '"' + value.replace('"', '""') + '"'
    return value


@dataclass
class Span:
    header: int
    end: int
    indent: int
    prefix: str
    text: str
    kind: str = ""
    name: str = ""
    value: str | None = None
    reference: bool = False
    expression_end: int = 0
    description_start: int = 0
    parent: Span | None = None
    children: list[Span] = field(default_factory=list)

    @property
    def path(self):
        parent = self.parent.path if self.parent is not None else ()
        if self.kind in {"model", "database"}:
            return parent
        return parent + ((self.kind or "opaque", self.name),)


class Document:
    def __init__(self, path, content):
        self.path = path
        self.lines = content.decode("utf-8").splitlines(keepends=True)
        self.newline = (
            "\r\n" if any(line.endswith("\r\n") for line in self.lines) else "\n"
        )
        self.spans = []
        stack = []
        description_start = None
        index = 0
        while index < len(self.lines):
            line = self.lines[index]
            lexical = line.lstrip("\ufeff") if index == 0 else line
            text = lexical.strip()
            if not text or text.startswith("//"):
                if text.startswith("///"):
                    description_start = (
                        index if description_start is None else description_start
                    )
                elif not text:
                    description_start = None
                index += 1
                continue
            indent = indent_width(lexical)
            start = index if description_start is None else description_start
            description_start = None
            while stack and stack[-1].indent >= indent:
                stack.pop().end = start
            match = _OBJECT.fullmatch(text)
            named = match and (match["name"] or match["kind"].lower() in _NAMED)
            kind = match["kind"].lower() if named else ""
            name = object_name(match["name"]) if named and match["name"] else ""
            value = match["value"] if named else None
            if not named:
                name = re.split(r"\s*[:=]\s*", text, maxsplit=1)[0]
            node = Span(
                index,
                len(self.lines),
                indent,
                lexical[: len(lexical) - len(lexical.lstrip(" \t"))],
                text,
                kind,
                name,
                value,
                bool(match and match["ref"]),
                index + 1,
                start,
                stack[-1] if stack else None,
            )
            if node.parent is not None:
                node.parent.children.append(node)
            self.spans.append(node)
            stack.append(node)
            index += 1
            assignment = re.match(r"[^:=]*=\s*(.*)\Z", text)
            if assignment and kind != "partition":
                value = assignment[1]
                if value == "```":
                    while (
                        index < len(self.lines) and self.lines[index].strip() != "```"
                    ):
                        index += 1
                    # Opaque invalid input remains Fabric's responsibility until edited.
                    if index < len(self.lines):
                        index += 1
                elif not value:
                    threshold = indent + 4 if kind else indent
                    while index < len(self.lines):
                        next_line = self.lines[index]
                        if next_line.strip() and indent_width(next_line) <= threshold:
                            break
                        index += 1
                node.expression_end = index

    def child_prefix(self, node):
        for child in node.children:
            if child.prefix.startswith(node.prefix) and child.indent > node.indent:
                return child.prefix
        return node.prefix + self.indent_unit()

    def indent_unit(self):
        """Four spaces only for a file already indented with spaces; else a tab."""

        indents = [line[: len(line) - len(line.lstrip())] for line in self.lines]
        spaces = any(i.strip("\r\n") for i in indents) and not any(
            "\t" in i for i in indents
        )
        return "    " if spaces else "\t"

    def replace(self, start, end, content):
        lines = list(self.lines)
        lines[start:end] = [content] if content else []
        return "".join(lines).encode("utf-8")

    def remove(self, ranges):
        """Delete line ranges, merging overlaps; return None for an emptied file."""

        merged = []
        for start, end in sorted(ranges):
            if merged and start <= merged[-1][1]:
                merged[-1] = (merged[-1][0], max(end, merged[-1][1]))
            else:
                merged.append((start, end))
        lines = list(self.lines)
        for start, end in reversed(merged):
            del lines[start:end]
        text = "".join(lines)
        return text.encode("utf-8") if text.lstrip("\ufeff").strip() else None

    @property
    def index(self):
        try:
            return self._index
        except AttributeError:
            self._index = {}
            for span in self.spans:
                self._index.setdefault(folded(span.path), []).append(span)
            return self._index

    def property_insertion(self, node):
        """New properties follow the last plain property, before any child.

        Fabric reads a block property such as ``dataAccessOptions`` as a child,
        and refuses a plain property after one.
        """

        end = node.expression_end
        for child in node.children:
            if child.kind or (child.children and not re.search(r"[:=]", child.text)):
                break
            end = content_end(child)
        return end


def folded(path):
    return tuple((kind.casefold(), name.casefold()) for kind, name in path)


def content_end(span):
    return max([span.expression_end, *(content_end(c) for c in span.children)])


def root_file(kind, name):
    """The file a new root-level object of this kind is written to."""

    if kind.casefold() in {"table", "role", "culture", "perspective"}:
        return f"definition/{kind}s/{quote(name, safe='')}.tmdl"
    return {
        "relationship": "definition/relationships.tmdl",
        "expression": "definition/expressions.tmdl",
        "datasource": "definition/dataSources.tmdl",
        "function": "definition/functions.tmdl",
    }.get(kind.casefold())


class PackageEditor:
    def __init__(self, parts):
        self.parts = dict(parts)
        self._parsed = {}
        # Edits made through live objects, in order: ("set", path, key, value),
        # ("unset", path, key), ("add", path) and ("remove", path).
        self.journal = []

    def documents(self):
        for filename in sorted(self.parts):
            if filename.startswith("definition/") and filename.endswith(".tmdl"):
                content = self.parts[filename]
                cached = self._parsed.get(filename)
                # A write always stores new bytes, so identity detects a stale parse.
                if cached is None or cached[0] is not content:
                    cached = (content, Document(filename, content))
                    self._parsed[filename] = cached
                yield cached[1]

    def locations(self, path):
        wanted = folded(path)
        found = []
        for document in self.documents():
            for span in document.index.get(wanted, ()):
                if (
                    span.kind == "model"
                    if not path
                    else (bool(span.kind) or path[-1][0] == "opaque")
                ):
                    if not span.reference or span.children:
                        found.append((document, span))
        return found

    def children(self, path, kind):
        """Names of declared child objects of one kind, in definition order."""

        wanted = folded(path)
        kind = kind.casefold()
        names = {}
        for document in self.documents():
            for key, spans in document.index.items():
                if not key or key[:-1] != wanted or key[-1][0] != kind:
                    continue
                for span in spans:
                    if span.kind and (not span.reference or span.children):
                        names.setdefault(key[-1][1], span.name)
        return list(names.values())

    def remove(self, path):
        """Delete every declaration of an object, including bare references."""

        wanted = folded(path)
        for document in list(self.documents()):
            spans = document.index.get(wanted, ())
            if not spans:
                continue
            content = document.remove((s.description_start, s.end) for s in spans)
            if content is None:
                del self.parts[document.path]
            else:
                self.parts[document.path] = content

    def remove_property(self, path, key):
        for document, child in self._property_spans(path, key):
            self.parts[document.path] = document.replace(
                child.header, content_end(child), ""
            )
            return

    def _property_spans(self, path, key):
        matches = [
            (document, child)
            for document, node in self.locations(path)
            for child in node.children
            if not child.kind and child.name.casefold() == key.casefold()
        ]
        if len(matches) > 1:
            raise ConfigError(
                f"TMDL {path!r}/{key}: property is declared more than once"
            )
        return matches

    def add(self, path, kind, value):
        from .render import object_file, render_object

        if len(path) == 1 and kind in {"table", "role", "expression", "relationship"}:
            filename = object_file(kind, value["name"])
            previous = self.parts.get(filename, b"")
            self.parts[filename] = (
                previous
                + (b"\n" if previous else b"")
                + render_object(kind, value).encode("utf-8")
            )
            return
        parents = self.locations(path[:-1])
        if not parents:
            raise ConfigError(f"TMDL parent {path[:-1]!r} was not found")
        document, parent = parents[0]
        prefix = document.child_prefix(parent)
        unit = prefix[len(parent.prefix) :]
        text = document.newline + render_object(
            kind, value, prefix=prefix, unit=unit, newline=document.newline
        )
        self.parts[document.path] = document.replace(parent.end, parent.end, text)

    def expression(self, path, key, value):
        from .render import expression_lines

        locations = self.locations(path)
        defaults = {
            "measure": "expression",
            "column": "expression",
            "expression": "expression",
            "annotation": "value",
            "tablepermission": "filterExpression",
        }
        candidates = []
        for document, node in locations:
            if defaults.get(node.kind) == key:
                candidates.append((document, node, node))
            else:
                candidates.extend(
                    (document, node, c)
                    for c in node.children
                    if not c.kind and c.name == key
                )
        if len(candidates) > 1 or not locations:
            raise ConfigError(f"TMDL {path!r}/{key}: ambiguous or missing object")
        if candidates:
            document, parent, node = candidates[0]
            prefix = node.prefix
            if node.kind:
                match = _OBJECT.fullmatch(node.text)
                header = (
                    node.text[: match.start("value")].rsplit("=", 1)[0].rstrip()
                    if match and node.value is not None
                    else node.text
                )
            else:
                header = key
            start, end = node.header, node.expression_end
            unit = document.child_prefix(node)[len(node.prefix) :]
        else:
            document, parent = locations[0]
            prefix = document.child_prefix(parent)
            unit = prefix[len(parent.prefix) :]
            header = key
            start = end = document.property_insertion(parent)
        text = expression_lines(header, value, prefix, unit, document.newline)
        self.parts[document.path] = document.replace(start, end, text)

    def source(self, path, value):
        locations = self.locations(path)
        if len(locations) != 1:
            raise ConfigError(f"TMDL {path!r}: expected one partition source")
        document, partition = locations[0]
        source_type = value.get("type", partition.value)
        if source_type != partition.value:
            line = document.lines[partition.header]
            header = partition.text.rsplit("=", 1)[0].rstrip()
            self.parts[document.path] = document.replace(
                partition.header,
                partition.header + 1,
                partition.prefix
                + header
                + " = "
                + source_type
                + ("\r\n" if line.endswith("\r\n") else "\n"),
            )
        if source_type in {"m", "calculated"}:
            if "expression" in value:
                self.expression(path, "source", value["expression"])
        elif source_type == "entity":
            child_path = path + (("opaque", "source"),)
            if not self.locations(child_path):
                document, partition = self.locations(path)[0]
                prefix = document.child_prefix(partition)
                self.parts[document.path] = document.replace(
                    partition.expression_end,
                    partition.expression_end,
                    prefix + "source" + document.newline,
                )
            for key, member in value.items():
                if key != "type":
                    self.property(child_path, key, member)
        else:
            raise ConfigError(
                f"TMDL partition source type {source_type!r} is unsupported"
            )

    def property(self, path, key, value):
        locations = self.locations(path)
        if not locations:
            raise ConfigError(f"TMDL object {path!r} was not found")
        matches = []
        for document, node in locations:
            if key == "description":
                if node.description_start != node.header:
                    matches.append((document, node, None))
            else:
                for child in node.children:
                    if not child.kind and child.name.casefold() == key.casefold():
                        matches.append((document, node, child))
        if len(matches) > 1:
            raise ConfigError(
                f"TMDL {path!r}/{key}: property is declared more than once"
            )
        document, node, child = matches[0] if matches else (*locations[0], None)
        if key == "description":
            content = "".join(
                node.prefix + "/// " + line + document.newline
                for line in value.split("\n")
            )
            start, end = node.description_start, node.header
        else:
            prefix = child.prefix if child else document.child_prefix(node)
            content = f"{prefix}{key}: {text_value(value)}{document.newline}"
            start, end = (
                (child.header, child.header + 1)
                if child
                else (document.property_insertion(node),) * 2
            )
        self.parts[document.path] = document.replace(start, end, content)
