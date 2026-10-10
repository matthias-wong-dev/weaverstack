"""Compile resolved logical sources into native semantic definitions."""

import copy
import re
from dataclasses import replace

from ..declaration.metadata import (
    AUDIT_COLUMNS,
    SPARK_SQL,
    SQL,
    audit_column_name,
    signature_column_name,
)
from ..errors import BuildError
from .compiler import _NAMED_COLLECTIONS, _merge, escape, leaf_properties
from .fragments import source_context, source_table
from .patching import _patch_object
from .references import source_identity


def m_string(value):
    return (
        '"'
        + value.replace("#", "#(#)")
        .replace('"', '""')
        .replace("\r", "#(cr)")
        .replace("\n", "#(lf)")
        .replace("\t", "#(tab)")
        + '"'
    )


def _m_identifier(name):
    if re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", name):
        return name
    return "#" + m_string(name)


def semantic_type(value, reference, column):
    name = str(value or "").lower().split("(")[0].strip()
    for native, types in {
        "int64": {
            "bigint",
            "int",
            "integer",
            "smallint",
            "tinyint",
            "long",
            "short",
            "byte",
        },
        "double": {"decimal", "numeric", "float", "real", "double"},
        "decimal": {"money", "smallmoney"},
        "string": {
            "varchar",
            "nvarchar",
            "char",
            "nchar",
            "text",
            "ntext",
            "string",
            "uniqueidentifier",
        },
        "boolean": {"bit", "boolean", "bool"},
        "dateTime": {"date", "datetime", "datetime2", "smalldatetime", "timestamp"},
    }.items():
        if name in types:
            return native
    raise BuildError(
        f"Semantic source {reference}: column {column!r} has unsupported type {value!r}. Author a supported source projection."
    )


def _bind_partition(model, table, source):
    """Give a table with no authored partition one reading its managed source."""

    expression_name = str(source_identity(source["reference"]).item)
    expressions = model.setdefault("expressions", [])
    existing = next(
        (e for e in expressions if e["name"].casefold() == expression_name.casefold()),
        None,
    )
    if existing is not None:
        expression_name = existing["name"]
    expression = {
        "name": expression_name,
        "kind": "m",
        "expression": f"Sql.Database({m_string(source['server'])}, {m_string(source['database'])})",
    }
    if existing is None:
        expressions.append(expression)
    else:
        existing.update(expression)
    if model.get("defaultMode") == "import":
        # An Import model reads the shared source through M navigation.
        table["partitions"] = [
            {
                "name": table["name"],
                "mode": "import",
                "source": {
                    "type": "m",
                    "expression": (
                        "let\n"
                        f"    Source = {_m_identifier(expression_name)},\n"
                        f"    Data = Source{{[Schema={m_string(source['schema'])},"
                        f"Item={m_string(source['object'])}]}}[Data]\n"
                        "in\n    Data"
                    ),
                },
            }
        ]
        return "import"
    table["partitions"] = [
        {
            "name": table["name"],
            "mode": "directLake",
            "source": {
                "type": "entity",
                "schemaName": source["schema"],
                "entityName": source["object"],
                "expressionSource": expression_name,
            },
        }
    ]
    return "directLake"


def _changes(before, after, key=""):
    if isinstance(before, dict) and isinstance(after, dict):
        return {
            k: _changes(before.get(k), v, k)
            for k, v in after.items()
            if k not in before or before[k] != v
        }
    if (
        key in _NAMED_COLLECTIONS
        and isinstance(before, list)
        and isinstance(after, list)
    ):
        old = {v["name"].casefold(): v for v in before}
        return [
            {**_changes(old.get(v["name"].casefold()), v), "name": v["name"]}
            for v in after
            if old.get(v["name"].casefold()) != v
        ]
    return copy.deepcopy(after)


def _housekeeping(reference):
    """The audit and signature columns Weaver adds to a source's rows."""

    language = (
        SQL if source_identity(reference).item.item_type == "Warehouse" else SPARK_SQL
    )
    return {
        name.casefold()
        for name in (
            *(audit_column_name(c, language) for c in AUDIT_COLUMNS),
            signature_column_name(language),
        )
    }


def _source_columns(table, authored, source, reference):
    """Every source column, refined by the authored column it names.

    An authored column matches by ``sourceColumn``, else by name. One that matches
    nothing must be calculated. Weaver's housekeeping columns appear only when an
    authored column names them.
    """

    by_source = {str(c.get("sourceColumn", c["name"])).casefold(): c for c in authored}
    housekeeping = _housekeeping(reference)
    columns = []
    for c in source["source_columns"]:
        name = c["column_name"]
        if name.casefold() in housekeeping and name.casefold() not in by_source:
            continue
        column = by_source.pop(name.casefold(), {"name": name})
        column.setdefault("sourceColumn", name)
        if "dataType" not in column:
            column["dataType"] = semantic_type(c["data_type"], reference, name)
        columns.append(column)
    for column in by_source.values():
        if not column.get("expression"):
            raise BuildError(
                f"tables/{table}/columns/{column['name']}: {reference} has no column "
                f"{column.get('sourceColumn', column['name'])}"
            )
        columns.append(column)
    return columns


def bind_semantic_sources(repository, observed, selected):
    from .annotation import begin_annotations

    contributions = dict(repository.semantic_models)
    for item, contribution in repository.semantic_models.items():
        if item not in selected:
            continue
        contribution = begin_annotations(contribution)
        compilation = contribution.compilation
        editor = compilation.editor
        editor.parts = dict(contribution.parts)
        compilation.contribution = contribution
        requested = copy.deepcopy(contribution.requested)
        owned = set(contribution.owned)
        provenance = copy.deepcopy(contribution.provenance)
        bindings = dict(contribution.source_bindings)
        for table_name, reference in contribution.source_references.items():
            table = source_table(editor.parts, table_name)
            before_table = copy.deepcopy(table)
            context = source_context(editor.parts, table)
            before_context = copy.deepcopy(context)
            generated = not table.get("partitions")
            if reference not in observed and generated:
                raise BuildError(
                    f"Semantic source {reference}: source metadata was not read before Build planning"
                )
            source = copy.deepcopy(observed.get(reference, {"reference": reference}))
            before = leaf_properties({"model": {"tables": [table]}})
            mode = (
                _bind_partition(context, table, source)
                if generated
                else table["partitions"][0].get(
                    "mode", context.get("defaultMode", "import")
                )
            )
            if generated:
                table["columns"] = _source_columns(
                    table["name"], table.get("columns", []), source, reference
                )
                table["columns"] = [
                    column
                    for column in table["columns"]
                    if (("table", table["name"]), ("column", column["name"]))
                    not in contribution.absent
                ]

            if source.get("description"):
                table.setdefault("description", source["description"])
            for column in table.get("columns", []):
                notes = {
                    name.casefold(): note
                    for name, note in source.get("column_notes", {}).items()
                }
                note = notes.get(
                    str(column.get("sourceColumn", column["name"])).casefold()
                )
                if note:
                    column.setdefault("description", note)
            patch = _changes(before_context, context)
            change = _changes(before_table, table)
            if change:
                patch["tables"] = [{**change, "name": table["name"]}]
            _patch_object(editor, (), "model", patch, owned)
            requested = _merge(requested, patch)
            source["mode"] = mode
            # Authored partitions establish logical provenance only; Weaver
            # verifies SQL access when it creates or rebinds a partition.
            source["access"] = "sql" if generated else None
            bindings[table["name"]] = source
            origin = contribution.provenance.get(
                f"/model/tables/{escape(table['name'])}/source", {"source": reference}
            )
            for path, value in leaf_properties({"model": {"tables": [table]}}).items():
                if path not in before or before[path] != value:
                    provenance[path] = {
                        **origin,
                        "reason": "Weaver.Source",
                        "reference": reference,
                    }
        compilation.requested = requested
        compilation.owned = owned
        compilation.provenance = provenance
        compilation.source_bindings = bindings
        contributions[item] = compilation.run("post_schema")
    return replace(repository, semantic_models=contributions)


def begin_semantic_sources(repository, selected):
    from .annotation import begin_annotations

    return replace(
        repository,
        semantic_models={
            item: begin_annotations(contribution) if item in selected else contribution
            for item, contribution in repository.semantic_models.items()
        },
    )
