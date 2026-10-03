"""Merge partial native declarations by editing their addressed TMDL spans."""

from dataclasses import dataclass, replace
from urllib.parse import quote

from ..errors import ConfigError
from .compiler import _merge, escape, leaf_properties
from .extension_expectations import requested_fragment
from .tmdl import Document, PackageEditor

_PATCHABLE = frozenset(
    {
        "model",
        "table",
        "column",
        "measure",
        "partition",
        "expression",
        "relationship",
        "role",
        "tablepermission",
        "hierarchy",
        "level",
        "annotation",
    }
)
_COLLECTIONS = {
    "table": "tables",
    "column": "columns",
    "measure": "measures",
    "partition": "partitions",
    "expression": "expressions",
    "relationship": "relationships",
    "role": "roles",
    "tablepermission": "tablePermissions",
    "hierarchy": "hierarchies",
    "level": "levels",
    "annotation": "annotations",
}


@dataclass(frozen=True)
class ExtendedPackage:
    parts: dict[str, bytes]
    requested: dict
    provenance: dict
    owned: tuple[str, ...]


def _error(document, node, message):
    raise ConfigError(f"{document.path}:{node.header + 1}: {message}")


def _validate(document):
    seen = set()
    for node in document.spans:
        if node.parent is None:
            if node.indent:
                _error(document, node, "root object indentation must be zero")
            if not node.kind or not node.name:
                _error(document, node, "expected a named TMDL object declaration")
            if node.kind == "database":
                _error(
                    document, node, "extensions address model content, not the database"
                )
        elif node.indent != node.parent.indent + 4:
            _error(document, node, "child indentation must be one tab or four spaces")
        if node.kind and not node.name:
            _error(document, node, "object declaration requires a name")
        identity = tuple((k.casefold(), n.casefold()) for k, n in node.path)
        if identity in seen:
            _error(document, node, f"duplicate declaration {node.text!r}")
        seen.add(identity)
        if not node.kind and (
            node.name.startswith(".") or any(c.isspace() for c in node.name)
        ):
            _error(document, node, "malformed native property declaration")
        assignment = node.text.partition("=")
        if assignment[1] and assignment[2].strip() == "```":
            if document.lines[node.expression_end - 1].strip() != "```":
                _error(document, node, "unterminated expression fence")


def _text(document, node, prefix, newline, *, start=None, end=None):
    lines = document.lines[
        node.description_start if start is None else start : node.end
        if end is None
        else end
    ]
    result = []
    for line in lines:
        value = line.rstrip("\r\n")
        if value.startswith(node.prefix):
            value = value[len(node.prefix) :]
        result.append(prefix + value + newline if value else newline)
    return "".join(result)


def _filename(node):
    if node.kind in {"table", "role", "culture", "perspective"}:
        return f"definition/{node.kind}s/{quote(node.name, safe='')}.tmdl"
    return {
        "relationship": "definition/relationships.tmdl",
        "expression": "definition/expressions.tmdl",
        "datasource": "definition/dataSources.tmdl",
        "function": "definition/functions.tmdl",
    }.get(node.kind, "definition/model.tmdl")


def _pointer(path):
    if any(kind not in _COLLECTIONS for kind, _ in path):
        return None
    return "/model" + "".join(
        f"/{_COLLECTIONS[kind]}/{escape(name)}" for kind, name in path
    )


class ExtensionEditor:
    def __init__(self, parts):
        self.package = PackageEditor(parts)
        self.owned = set()

    def _locate(self, document, node):
        found = self.package.locations(node.path)
        if len(found) > 1:
            _error(document, node, f"{node.kind} {node.name!r} is ambiguous")
        return found[0] if found else None

    def _add(self, document, node):
        if node.parent is None or (
            node.parent.kind == "model" and node.kind != "annotation"
        ):
            filename = _filename(node)
            previous = self.package.parts.get(filename, b"")
            newline = "\r\n" if b"\r\n" in previous else "\n"
            content = _text(document, node, "", newline)
            self.package.parts[filename] = (
                previous + (newline.encode() if previous else b"") + content.encode()
            )
        else:
            parents = self.package.locations(node.parent.path)
            if len(parents) != 1:
                _error(
                    document,
                    node,
                    f"parent {node.parent.name!r} is missing or ambiguous",
                )
            target, parent = parents[0]
            content = target.newline + _text(
                document, node, target.child_prefix(parent), target.newline
            )
            self.package.parts[target.path] = target.replace(
                parent.end, parent.end, content
            )
        pointer = _pointer(node.path)
        if pointer and node.kind in _PATCHABLE:
            self.owned.add(pointer)

    def merge(self, document, node):
        located = self._locate(document, node)
        if not located:
            if node.reference:
                _error(document, node, f"ref {node.kind} {node.name!r} not found")
            if node.kind == "model":
                _error(document, node, "model not found in the base package")
            self._add(document, node)
            return
        target, current = located
        description = node.description_start < node.header
        if node.kind not in _PATCHABLE and node.kind:
            if (
                node.reference
                and not node.children
                and not description
                and node.value is None
            ):
                return
            _error(
                document,
                node,
                f"existing {node.kind} {node.name!r}: cannot safely patch this type; new non-colliding native objects are supported",
            )
        if node.kind == "model" and node.name.casefold() != current.name.casefold():
            _error(document, node, f"model {node.name!r} not found")
        if not node.kind:
            self._property(document, node, target, current)
            return
        if description:
            content = _text(
                document,
                node,
                current.prefix,
                target.newline,
                start=node.description_start,
                end=node.header,
            )
            self.package.parts[target.path] = target.replace(
                current.description_start, current.header, content
            )
        if node.value is not None:
            target, current = self._locate(document, node)
            content = _text(
                document,
                node,
                current.prefix,
                target.newline,
                start=node.header,
                end=node.expression_end,
            )
            lines = content.splitlines(keepends=True)
            lines[0] = (
                current.prefix
                + current.text.partition("=")[0].rstrip()
                + " ="
                + node.text.partition("=")[2]
                + target.newline
            )
            self.package.parts[target.path] = target.replace(
                current.header, current.expression_end, "".join(lines)
            )
        for child in node.children:
            if child.kind:
                self.merge(document, child)
            else:
                self._merge_property(document, child)

    def _merge_property(self, document, node):
        located = self._locate(document, node)
        if located:
            self._property(document, node, *located)
            return
        parents = self.package.locations(node.parent.path)
        if len(parents) != 1:
            _error(
                document, node, f"parent {node.parent.name!r} is missing or ambiguous"
            )
        target, parent = parents[0]
        content = _text(document, node, target.child_prefix(parent), target.newline)
        self.package.parts[target.path] = target.replace(
            parent.expression_end, parent.expression_end, content
        )

    def _property(self, document, node, target, current):
        # Block properties merge recursively; an explicitly supplied expression replaces its body.
        if node.children and current.children and "=" not in node.text:
            for child in node.children:
                if child.kind:
                    self.merge(document, child)
                else:
                    self._merge_property(document, child)
        else:
            content = _text(document, node, current.prefix, target.newline)
            self.package.parts[target.path] = target.replace(
                current.description_start, current.end, content
            )


def merge_extensions(parts, fragments):
    editor = ExtensionEditor(parts)
    requested = {}
    provenance = {}
    for content, origin in fragments:
        try:
            document = Document(origin, content)
        except UnicodeDecodeError as exc:
            raise ConfigError(f"{origin}:1: TMDL must use UTF-8") from exc
        _validate(document)
        patch = requested_fragment(document)
        for node in document.spans:
            if node.parent is None:
                editor.merge(document, node)
        requested = _merge(requested, patch)
        for path in leaf_properties({"model": patch}) if patch else ():
            provenance[path] = {"source": origin, "reason": "extension"}
    return ExtendedPackage(
        editor.package.parts, requested, provenance, tuple(sorted(editor.owned))
    )


def apply_extensions(contribution, name, fragments):
    from .render import empty_parts

    result = merge_extensions(contribution.parts or empty_parts(name), fragments)
    owned = set(contribution.owned) | set(result.owned)
    requested = contribution.requested
    if not contribution.parts:
        owned.add("/model")
        requested = {
            "culture": "en-US",
            "defaultPowerBIDataSourceVersion": "powerBI_V3",
            **requested,
        }
    return replace(
        contribution,
        parts=result.parts,
        requested=_merge(requested, result.requested),
        provenance={**contribution.provenance, **result.provenance},
        owned=tuple(sorted(owned)),
    )
