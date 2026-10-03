"""TMDL source spans for edits to named objects and properties."""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from ..errors import ConfigError

_NAME = r"(?:'(?:[^']|'')*'|[^\s=:.]+)"
_OBJECT = re.compile(
    rf"(?P<ref>ref\s+)?(?P<kind>database|model|table|column|measure|partition|"
    rf"relationship|expression|role|tablePermission|hierarchy|level|annotation)"
    rf"(?:\s+(?P<name>{_NAME}))?(?:\s*=\s*(?P<value>.*))?\Z",
    re.IGNORECASE,
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
            kind = match["kind"].lower() if match else ""
            name = object_name(match["name"]) if match and match["name"] else ""
            value = match["value"] if match else None
            if not match:
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
        return node.prefix + (
            "\t"
            if any(
                "\t" in line[: len(line) - len(line.lstrip())] for line in self.lines
            )
            else "    "
        )

    def replace(self, start, end, content):
        self.lines[start:end] = [content] if content else []
        return "".join(self.lines).encode("utf-8")


class PackageEditor:
    def __init__(self, parts):
        self.parts = dict(parts)

    def locations(self, path):
        wanted = tuple((k.lower(), n.casefold()) for k, n in path)
        found = []
        for filename, content in sorted(self.parts.items()):
            if not filename.startswith("definition/") or not filename.endswith(".tmdl"):
                continue
            document = Document(filename, content)
            for span in document.spans:
                actual = tuple((k, n.casefold()) for k, n in span.path)
                if actual == wanted and (
                    span.kind == "model"
                    if not path
                    else (bool(span.kind) or path[-1][0] == "opaque")
                ):
                    if not span.reference or span.children:
                        found.append((document, span))
        return found

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
            start = end = parent.expression_end
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
                else (node.expression_end, node.expression_end)
            )
        self.parts[document.path] = document.replace(start, end, content)
