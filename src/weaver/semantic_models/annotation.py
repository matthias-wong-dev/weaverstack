"""Annotations that run Python over a semantic model's native TMDL.

``annotation DWG.HideIntegerColumns = true`` runs the `Annotation` subclass named
``DWG__HideIntegerColumns``: ``__`` in a class name is ``.`` in TMDL. Weaver's
own annotations hold the ``Weaver`` namespace. A project adds its own in
``SemanticModel/annotations/<ClassName>.py``, one class per file, and they apply
to every semantic model in the project. Once a project defines a namespace, an
undefined annotation in it is an error; other annotations stay native.

Project annotation files are trusted Build-time code: Build executes them while
it compiles the semantic definition. Each file is self-contained apart from
installed packages such as ``weaver``.
"""

from __future__ import annotations

import re
import sys
import types
import uuid
from dataclasses import dataclass, field, replace
from typing import Mapping, NoReturn

from ..errors import ConfigError
from .compiler import _COMMON, _SCHEMAS, _merge, escape, leaf_properties
from .extension_expectations import requested_object
from .fragments import expression_text, scalar
from .objects import TmdlObject
from .tmdl import Document, PackageEditor, folded, quote_name

DIRECTORY = "SemanticModel/annotations"
_SEGMENT = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9_]*[A-Za-z0-9])?\Z")
_KINDS = {key.casefold(): key for key in _SCHEMAS}
_COLLECTION_OF = {
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


def annotation_name(cls) -> str | None:
    """The TMDL name a class answers to, or None when it is not namespaced."""

    segments = cls.__name__.split("__")
    if len(segments) < 2 or not all(_SEGMENT.match(s) for s in segments):
        return None
    return ".".join(segments)


class Annotation:
    """One semantic model annotation, named by its class.

    Set ``scopes`` to the TMDL object kinds the annotation may be declared on,
    and implement `apply`. Build creates one instance per declaration and calls
    ``apply(target)`` with the annotated object, whose native properties read
    and write in place, for example ``column.isHidden = True``.
    """

    scopes: frozenset[str] = frozenset()
    phase = "post_schema"

    _text: str
    _location: str
    _compilation: _Compilation

    def apply(self, target: TmdlObject) -> None:
        raise NotImplementedError

    @property
    def value(self) -> str:
        """The declared value, without surrounding space or double quotes."""

        return _unquote(self._text)

    def lines(self) -> list[str]:
        """Each non-blank line of the value, unquoted."""

        return [_unquote(line) for line in self._text.splitlines() if line.strip()]

    def boolean(self) -> bool:
        if self.value not in {"true", "false"}:
            self.error("requires a boolean true or false")
        return self.value == "true"

    def error(self, message: str) -> NoReturn:
        """Fail Build with this declaration's file and line."""

        raise ConfigError(f"{self._location}: {message}")


def _unquote(text):
    value = text.strip()
    return scalar(value) if value.startswith('"') else value


@dataclass(frozen=True)
class AnnotationRegistry:
    """The annotation classes one repository's Build dispatches to."""

    classes: Mapping[str, type[Annotation]]
    sources: Mapping[str, bytes] = field(default_factory=dict)

    @property
    def namespaces(self):
        return {name.split(".", 1)[0].casefold() for name in self.classes}

    def dispatch(self, document, node):
        namespace, dot, _ = node.name.partition(".")
        if not dot or namespace.casefold() not in self.namespaces:
            return None
        cls = self.classes.get(node.name)
        if cls is None:
            raise ConfigError(
                f"{document.path}:{node.header + 1}: {node.name}: "
                f"unknown {namespace} annotation"
            )
        if node.parent is None or node.parent.kind not in {
            scope.casefold() for scope in cls.scopes
        }:
            raise ConfigError(
                f"{document.path}:{node.header + 1}: {node.name}: valid only at "
                f"{' or '.join(sorted(cls.scopes))} scope"
            )
        return cls


def builtin_registry() -> AnnotationRegistry:
    from .builtin_annotations import BUILTIN_ANNOTATIONS

    return AnnotationRegistry(
        {annotation_name(cls): cls for cls in BUILTIN_ANNOTATIONS}
    )


def discover_annotations(root, store, paths) -> AnnotationRegistry:
    """Load the project's annotation classes beside Weaver's own."""

    builtin = builtin_registry()
    classes = dict(builtin.classes)
    sources = {}
    prefix = DIRECTORY + "/"
    for path in sorted(p for p in paths if p.startswith(prefix)):
        relative = path[len(prefix) :]
        if "__pycache__" in relative.split("/"):
            continue
        if "/" in relative or not relative.endswith(".py"):
            raise ConfigError(
                f"{path}: {DIRECTORY}/ holds one <Namespace>__<Name>.py file per "
                "annotation"
            )
        stem = relative[: -len(".py")]
        sources[path] = store.read(root.join(*path.split("/")))
        cls = _load(path, stem, sources[path])
        name = annotation_name(cls)
        if name is None:
            raise ConfigError(
                f"{path}: name the class <Namespace>__<Name>, for example "
                "DWG__HideIntegerColumns"
            )
        namespace = name.split(".", 1)[0]
        if namespace.casefold() in builtin.namespaces:
            raise ConfigError(
                f"{path}: the {namespace} namespace is reserved for Weaver's "
                "annotations; choose another"
            )
        clash = next((n for n in classes if n.casefold() == name.casefold()), None)
        if clash is not None:
            raise ConfigError(f"{path}: {name} differs from {clash} only by case")
        scopes = cls.scopes
        if (
            isinstance(scopes, str)
            or not scopes
            or not all(isinstance(s, str) and s for s in scopes)
        ):
            raise ConfigError(
                f"{path}: set {stem}.scopes to the TMDL object kinds it annotates, "
                'for example {"table"}'
            )
        if cls.apply is Annotation.apply:
            raise ConfigError(f"{path}: {stem} must implement apply(self, target)")
        if cls.phase not in ("schema", "post_schema"):
            raise ConfigError(f"{path}: {stem}.phase must be 'schema' or 'post_schema'")
        classes[name] = cls
    return AnnotationRegistry(classes, sources)


def _load(path, stem, source):
    # The module is registered only while its body runs, which dataclasses and
    # typing need; afterwards nothing global refers to the project's classes.
    name = f"weaver._semantic_annotations.m{uuid.uuid4().hex}"
    module = types.ModuleType(name)
    module.__file__ = path
    sys.modules[name] = module
    try:
        exec(compile(source, path, "exec"), module.__dict__)
    except ConfigError:
        raise
    except Exception as exc:
        raise ConfigError(f"{path}: {type(exc).__name__}: {exc}") from exc
    finally:
        sys.modules.pop(name, None)
    defined = [
        value
        for value in vars(module).values()
        if isinstance(value, type)
        and issubclass(value, Annotation)
        and value.__module__ == name
    ]
    if len(defined) != 1:
        found = ", ".join(sorted(c.__name__ for c in defined)) or "none"
        raise ConfigError(
            f"{path}: define exactly one Annotation subclass, named {stem}; "
            f"found {found}"
        )
    if defined[0].__name__ != stem:
        raise ConfigError(
            f"{path}: rename class {defined[0].__name__} to {stem}, the file's name"
        )
    return defined[0]


def _pointer(path):
    if any(kind.casefold() not in _COLLECTION_OF for kind, _ in path):
        return None
    return "/model" + "".join(
        f"/{_COLLECTION_OF[kind.casefold()]}/{escape(name)}" for kind, name in path
    )


def _nest(path, value):
    for kind, name in reversed(path):
        if kind == "opaque":
            value = {name: value}
            continue
        collection = _COLLECTION_OF.get(kind.casefold())
        if collection is None:
            return None
        value = {collection: [{"name": name, **value}]}
    return value


def _schema(path):
    schema = "model"
    for kind, name in path:
        if kind == "opaque":
            schema = {**_COMMON, **_SCHEMAS.get(schema, {})}.get(name)
            if not isinstance(schema, str):
                return None
        else:
            schema = _KINDS.get(kind.casefold())
            if schema is None:
                return None
    return schema


def _expected(path, key, value):
    """The typed readback expectation for one edit, when Weaver checks it."""

    schema = _schema(path)
    if schema is None:
        return None
    if schema == "partition" and key == "sourceType":
        return _nest(path, {"source": {"type": value}})
    expected = {**_COMMON, **_SCHEMAS[schema]}.get(key)
    if isinstance(expected, type) and isinstance(value, expected):
        if expected is int and isinstance(value, bool):
            return None
        return _nest(path, {key: value})
    if isinstance(expected, str) and isinstance(value, str):
        if "expression" in _SCHEMAS.get(expected, {}):
            return _nest(path, {key: {"expression": value}})
    return None


def _without(requested, path):
    kind, name = path[0]
    if kind == "opaque":
        result = dict(requested)
        if len(path) == 1:
            result.pop(name, None)
        elif name in result:
            result[name] = _without(result[name], path[1:])
        return result
    collection = _COLLECTION_OF.get(kind.casefold())
    if collection not in requested:
        return requested
    result = dict(requested)
    members = []
    for value in requested[collection]:
        if value["name"].casefold() != name.casefold():
            members.append(value)
        elif len(path) > 1:
            members.append(_without(value, path[1:]))
    if members:
        result[collection] = members
    else:
        del result[collection]
    return result


class _Compilation:
    """One contribution's annotation run: the editor and what its edits imply."""

    def __init__(self, contribution, registry):
        self.registry = registry
        self.editor = PackageEditor(contribution.parts)
        self.contribution = contribution
        self.requested = contribution.requested
        self.owned = set(contribution.owned)
        self.provenance = dict(contribution.provenance)
        self.absent = list(contribution.absent)
        self.source_references = dict(contribution.source_references)
        self.source_bindings = dict(contribution.source_bindings)
        self._seen = 0

    def removed(self, path):
        """Forget a removed object and expect its absence on readback."""

        self.requested = _without(self.requested, path)
        pointer = _pointer(path)
        if pointer is not None:
            self.provenance = {
                key: value
                for key, value in self.provenance.items()
                if key != pointer and not key.startswith(pointer + "/")
            }
            self.owned = {
                o
                for o in self.owned
                if o != pointer and not o.startswith(pointer + "/")
            }
            if folded(path) not in {folded(p) for p in self.absent}:
                self.absent.append(tuple(path))
        if len(path) == 1 and path[0][0].casefold() == "table":
            name = path[0][1].casefold()
            for mapping in (self.source_references, self.source_bindings):
                for key in [k for k in mapping if k.casefold() == name]:
                    del mapping[key]

    def _settle(self, document, node):
        """Turn one annotation's edits into readback expectations and provenance."""

        origin = self.provenance.get(
            (_pointer(node.parent.path) or "")
            + f"/annotations/{escape(node.name)}/value",
            {},
        ).get("source", document.path)
        events = self.editor.journal[self._seen :]
        self._seen = len(self.editor.journal)
        added = []
        for event in events:
            if event[0] == "set":
                patch = _expected(event[1], event[2], event[3])
                if patch is not None:
                    self.requested = _merge(self.requested, patch)
                    for leaf in leaf_properties({"model": patch}):
                        self.provenance[leaf] = {"source": origin, "reason": node.name}
            elif event[0] == "unset":
                self.requested = _without(
                    self.requested, event[1] + (("opaque", event[2]),)
                )
                pointer = _pointer(event[1])
                if pointer is not None:
                    self.provenance.pop(pointer + "/" + escape(event[2]), None)
            elif event[0] == "add":
                added.append(event[1])
            elif event[0] == "remove":
                self.removed(event[1])
        for path in added:
            located = self.editor.locations(path)
            pointer = _pointer(path)
            if not located or pointer is None or _schema(path) is None:
                continue
            value = requested_object(*located[0], _schema(path))
            self.requested = _merge(
                self.requested,
                _nest(path, {k: v for k, v in value.items() if k != "name"}),
            )
            self.owned.add(pointer)
        if self.editor.locations(node.parent.path):
            expected = _nest(
                node.parent.path, {"annotations": [requested_object(document, node)]}
            )
            if expected is not None:
                self.requested = _merge(self.requested, expected)

    def run(self, phase):
        declared = [
            (document, node)
            for name in self.editor.parts
            if name.startswith("definition/") and name.endswith(".tmdl")
            for document in [Document(name, self.editor.parts[name])]
            for node in document.spans
            if node.kind == "annotation"
        ]
        dispatched = set()
        for document, node in declared:
            cls = self.registry.dispatch(document, node)
            if cls is None or cls.phase != phase:
                continue
            dispatched.add(node.name)
            if not self.editor.locations(node.parent.path):
                continue
            annotation = object.__new__(cls)
            annotation._text = expression_text(document, node)
            annotation._location = f"{document.path}:{node.header + 1}: {node.name}"
            annotation._compilation = self
            annotation.apply(TmdlObject(self.editor, node.parent.path))
            self._settle(document, node)
            if phase == "post_schema":
                references = prepare_annotations(
                    replace(self.contribution, parts=self.editor.parts), self.registry
                ).source_references
                if any(
                    self.source_references.get(name) != reference
                    for name, reference in references.items()
                ):
                    annotation.error(
                        "post_schema introduces a source dependency; declare phase = 'schema'"
                    )
        _quote_names(self.editor, dispatched)
        return replace(
            self.contribution,
            parts=self.editor.parts,
            requested=self.requested,
            owned=tuple(sorted(self.owned)),
            provenance=self.provenance,
            absent=tuple(sorted(self.absent)),
            source_references=self.source_references,
            source_bindings=self.source_bindings,
            annotations=self.registry,
            compilation=self if phase == "schema" else None,
        )


def _quote_names(editor, names):
    """Quote dispatched annotation names, which Fabric keeps only when quoted."""

    for document in list(editor.documents()):
        lines = list(document.lines)
        for node in document.spans:
            if node.kind != "annotation" or node.name not in names:
                continue
            line = lines[node.header]
            match = re.match(r"\s*annotation\s+(?P<name>'(?:[^']|'')*'|[^\s=]+)", line)
            if match and not match["name"].startswith("'"):
                start, end = match.span("name")
                lines[node.header] = line[:start] + quote_name(node.name) + line[end:]
        content = "".join(lines).encode("utf-8")
        if content != editor.parts[document.path]:
            editor.parts[document.path] = content


def prepare_annotations(contribution, registry=None):
    """Validate annotations and collect sources without executing handlers."""

    registry = registry or contribution.annotations or builtin_registry()
    references = {}
    editor = PackageEditor(contribution.parts)
    for document in editor.documents():
        for node in document.spans:
            if node.kind != "annotation":
                continue
            cls = registry.dispatch(document, node)
            if cls is None:
                continue
            if node.name == "Weaver.Source":
                annotation = object.__new__(cls)
                annotation._text = expression_text(document, node)
                annotation._location = f"{document.path}:{node.header + 1}: {node.name}"
                references[node.parent.name] = annotation.reference
    return replace(
        contribution,
        parts=editor.parts,
        source_references=references,
        annotations=registry,
    )


def begin_annotations(contribution, registry=None):
    if contribution.compilation is not None:
        return contribution
    registry = registry or contribution.annotations or builtin_registry()
    compilation = _Compilation(contribution, registry)
    for path in contribution.absent:
        compilation.editor.remove(path)
        compilation.removed(path)
    result = prepare_annotations(compilation.run("schema"), registry)
    compilation.editor.parts = dict(result.parts)
    compilation.source_references = dict(result.source_references)
    return result


def apply_annotations(contribution, registry=None):
    """Execute schema then post-schema annotations over one editor."""

    contribution = begin_annotations(contribution, registry)
    return contribution.compilation.run("post_schema")
