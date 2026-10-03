"""Weaver annotations in native TMDL.

Weaver.Source: table; Warehouse/<item>/<schema>.<object> or
Lakehouse/<item>/Tables/<schema>.<object>; generate managed source content.
Weaver.AutoHideColumns: model or table; newline-separated glob patterns; hide matching columns.
Weaver.AutoHideForeignKeys: model; boolean; hide native many-side columns.
Weaver.MeasureTable: table; true; generate the INFO.VIEW.MEASURES() partition.
Weaver.Switch: measure; qualified measure references; generate value and format switches.
Weaver.Exclude: table or column; boolean; remove the annotated native object.
"""

import re
from dataclasses import dataclass, replace
from fnmatch import fnmatchcase
from typing import Callable, NoReturn

from ..errors import ConfigError
from .annotation_exclusion import exclude_paths
from .compiler import _merge
from .extension_expectations import requested_object
from .fragments import expression_text, scalar
from .patching import _patch_object, _pointer
from .references import source_identity
from .tmdl import Document, PackageEditor, object_name, quote_name


@dataclass(frozen=True)
class AnnotationDefinition:
    scopes: frozenset[str]
    grammar: str
    handler: Callable


def _error(document, node, message) -> NoReturn:
    raise ConfigError(f"{document.path}:{node.header + 1}: {node.name}: {message}")


def _source(contribution, document, node):
    reference = scalar(expression_text(document, node).strip())
    if reference.startswith("Lakehouse/") and (
        len(reference.split("/")) != 4 or reference.split("/")[2] != "Tables"
    ):
        _error(document, node, "use Lakehouse/<item>/Tables/<schema>.<object>")
    try:
        source_identity(reference)
    except ConfigError as exc:
        _error(document, node, str(exc))
    pointer = _pointer(node.parent.path)
    origin = contribution.provenance.get(
        pointer + "/annotations/Weaver.Source/value", {}
    ).get("source", document.path)
    return replace(
        contribution,
        provenance={
            **contribution.provenance,
            pointer + "/source": {
                "source": origin,
                "reason": "Weaver.Source",
                "reference": reference,
            },
        },
        source_references={
            **contribution.source_references,
            node.parent.name: reference,
        },
    )


def _patch(contribution, changes):
    editor = PackageEditor(contribution.parts)
    owned = set(contribution.owned)
    _patch_object(editor, (), "model", changes, owned)
    return replace(
        contribution,
        parts=editor.parts,
        requested=_merge(contribution.requested, changes),
        owned=tuple(sorted(owned)),
    )


def _hide_columns(contribution, document, annotation):
    patterns = [
        scalar(line.strip())
        for line in expression_text(document, annotation).splitlines()
        if line.strip()
    ]
    if not patterns or any(not pattern for pattern in patterns):
        _error(document, annotation, "requires at least one glob pattern")
    tables = []
    for data_path, data in contribution.parts.items():
        if not data_path.endswith(".tmdl"):
            continue
        current = Document(data_path, data)
        for table in current.spans:
            if table.kind != "table" or table.reference:
                continue
            if (
                annotation.parent.kind == "table"
                and table.name != annotation.parent.name
            ):
                continue
            columns = [
                {"name": c.name, "isHidden": True}
                for c in table.children
                if c.kind == "column"
                and any(fnmatchcase(c.name, pattern) for pattern in patterns)
            ]
            if columns:
                tables.append({"name": table.name, "columns": columns})
    return _patch(contribution, {"tables": tables}) if tables else contribution


def _boolean(document, node):
    value = scalar(expression_text(document, node).strip())
    if value not in {"true", "false"}:
        _error(document, node, "requires a boolean true or false")
    return value == "true"


def _hide_foreign_keys(contribution, document, annotation):
    if not _boolean(document, annotation):
        return contribution
    editor = PackageEditor(contribution.parts)
    tables = {}
    for path, data in contribution.parts.items():
        if not path.endswith(".tmdl"):
            continue
        current = Document(path, data)
        for node in current.spans:
            if node.kind != "relationship" or node.reference:
                continue
            relationship = requested_object(current, node)
            for side, default in (("from", "many"), ("to", "one")):
                if relationship.get(side + "Cardinality", default) != "many":
                    continue
                table = relationship.get(side + "Table")
                column = relationship.get(side + "Column")
                if not table or not column:
                    _error(current, node, "cannot address the many-side column")
                if not editor.locations((("table", table), ("column", column))):
                    continue
                tables.setdefault(table, set()).add(column)
    changes = {
        "tables": [
            {
                "name": table,
                "columns": [
                    {"name": column, "isHidden": True} for column in sorted(columns)
                ],
            }
            for table, columns in sorted(tables.items())
        ]
    }
    return _patch(contribution, changes) if tables else contribution


def _measure_table(contribution, document, annotation):
    if not _boolean(document, annotation):
        _error(document, annotation, "requires true")
    name = annotation.parent.name
    partitions = [
        requested_object(document, n)
        for n in annotation.parent.children
        if n.kind == "partition"
    ]
    if partitions and (
        len(partitions) != 1
        or partitions[0].get("name") != name
        or partitions[0].get("source")
        != {"type": "calculated", "expression": "INFO.VIEW.MEASURES()"}
    ):
        _error(
            document,
            annotation,
            "cannot replace an authored partition; use a bare table",
        )
    return _patch(
        contribution,
        {
            "tables": [
                {
                    "name": name,
                    "partitions": [
                        {
                            "name": name,
                            "mode": "import",
                            "source": {
                                "type": "calculated",
                                "expression": "INFO.VIEW.MEASURES()",
                            },
                        }
                    ],
                }
            ]
        },
    )


def _dax_string(value):
    return '"' + value.replace('"', '""') + '"'


def _switch(contribution, document, annotation):
    measures = {}
    selectors = []
    for path, data in contribution.parts.items():
        if not path.endswith(".tmdl"):
            continue
        current = Document(path, data)
        for node in current.spans:
            if node.kind == "measure" and node.parent and node.parent.kind == "table":
                measures[(node.parent.name.casefold(), node.name.casefold())] = (
                    node.parent.name,
                    requested_object(current, node),
                )
            if node.kind == "annotation" and node.name == "Weaver.MeasureTable":
                selectors.append(node.parent.name)
    if len(selectors) != 1:
        _error(document, annotation, "requires one Weaver.MeasureTable selector")
    values, formats = [], []
    labels = set()
    references = [
        line.strip()
        for line in expression_text(document, annotation).splitlines()
        if line.strip()
    ]
    if not references:
        _error(document, annotation, "requires at least one measure reference")
    for reference in references:
        match = re.fullmatch(r"(.+)\[((?:[^\]]|\]\])+)\]", reference.strip())
        if match:
            key = (
                object_name(match[1].strip()).casefold(),
                match[2].replace("]]", "]").casefold(),
            )
            matches = [measures[key]] if key in measures else []
        else:
            name = reference.strip()
            if name.startswith("[") and name.endswith("]"):
                name = name[1:-1].replace("]]", "]")
            matches = [
                value for key, value in measures.items() if key[1] == name.casefold()
            ]
        if len(matches) != 1:
            _error(
                document,
                annotation,
                f"{'ambiguous' if matches else 'missing'} measure {reference!r}",
            )
        table, measure = matches[0]
        name = measure["name"]
        if name.casefold() in labels:
            _error(document, annotation, f"ambiguous selector label {name!r}")
        labels.add(name.casefold())
        if any(a["name"] == "Weaver.Switch" for a in measure.get("annotations", [])):
            _error(
                document,
                annotation,
                f"switch measure reference {reference!r} is recursive or unsupported",
            )
        values.append(
            f"    {_dax_string(name)}, {quote_name(table)}[{name.replace(']', ']]')}]"
        )
        dynamic = measure.get("formatStringDefinition", {}).get("expression")
        if dynamic:
            code = re.sub(
                r'//[^\n]*|--[^\n]*|/\*.*?\*/|"(?:[^"]|"")*"', "", dynamic, flags=re.S
            )
            if re.search(
                r"\b(?:SELECTEDMEASURE(?:NAME|FORMATSTRING)?|ISSELECTEDMEASURE)\s*\(",
                code,
                re.I,
            ):
                _error(
                    document,
                    annotation,
                    f"dynamic format for {reference!r} depends on measure context",
                )
        formats.append(
            f"    {_dax_string(name)}, {dynamic if dynamic else _dax_string(measure.get('formatString', ''))}"
        )
    selector = f"SWITCH(\n    SELECTEDVALUE({quote_name(selectors[0])}[Name]),\n"
    value = {
        "name": annotation.parent.name,
        "expression": selector + ",\n".join(values) + "\n)",
        "formatStringDefinition": {
            "expression": selector + ",\n".join(formats) + "\n)"
        },
    }
    return _patch(
        contribution,
        {"tables": [{"name": annotation.parent.parent.name, "measures": [value]}]},
    )


def _exclude(contribution, document, annotation):
    if not _boolean(document, annotation):
        return contribution
    return exclude_paths(contribution, (*contribution.absent, annotation.parent.path))


SUPPORTED_ANNOTATIONS = {
    "Weaver.Exclude": AnnotationDefinition(
        frozenset({"table", "column"}), "boolean", _exclude
    ),
    "Weaver.Switch": AnnotationDefinition(
        frozenset({"measure"}), "qualified measure references", _switch
    ),
    "Weaver.MeasureTable": AnnotationDefinition(
        frozenset({"table"}), "true", _measure_table
    ),
    "Weaver.AutoHideForeignKeys": AnnotationDefinition(
        frozenset({"model"}), "boolean", _hide_foreign_keys
    ),
    "Weaver.AutoHideColumns": AnnotationDefinition(
        frozenset({"model", "table"}), "newline-separated glob patterns", _hide_columns
    ),
    "Weaver.Source": AnnotationDefinition(
        frozenset({"table"}),
        "Warehouse/<item>/<schema>.<object> or Lakehouse/<item>/Tables/<schema>.<object>",
        _source,
    ),
}


def apply_annotations(contribution):
    contribution = exclude_paths(contribution, contribution.absent)
    for path, data in contribution.parts.items():
        if not path.startswith("definition/") or not path.endswith(".tmdl"):
            continue
        document = Document(path, data)
        for node in document.spans:
            if node.kind != "annotation" or not node.name.casefold().startswith(
                "weaver."
            ):
                continue
            definition = SUPPORTED_ANNOTATIONS.get(node.name)
            if definition is None:
                _error(document, node, "unknown Weaver annotation")
            if node.parent is None or node.parent.kind not in definition.scopes:
                _error(
                    document,
                    node,
                    f"valid only at {' or '.join(sorted(definition.scopes))} scope",
                )
            if not PackageEditor(contribution.parts).locations(node.parent.path):
                continue
            contribution = definition.handler(contribution, document, node)
            if PackageEditor(contribution.parts).locations(node.parent.path):
                expected = {"annotations": [requested_object(document, node)]}
                for kind, name in reversed(node.parent.path):
                    collection = {
                        "table": "tables",
                        "column": "columns",
                        "measure": "measures",
                    }[kind]
                    expected = {collection: [{"name": name, **expected}]}
                contribution = replace(
                    contribution, requested=_merge(contribution.requested, expected)
                )
    return contribution
