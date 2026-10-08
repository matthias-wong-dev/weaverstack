"""Compile resolved logical sources into native semantic definitions."""

import copy
import re
from dataclasses import replace

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


_M_TEXT = r'"(?:[^"]|"")*"'
_M_ID = r'(?:[A-Za-z_][A-Za-z_0-9]*|#"(?:[^"]|"")*")'
_SQL_DATABASE = rf"Sql\.Database\(\s*(?P<server>{_M_TEXT})\s*,\s*(?P<database>{_M_TEXT})\s*(?:,\s*\[\s*CreateNavigationProperties\s*=\s*(?:true|false)\s*\]\s*)?\)"
_NAVIGATION = rf"\{{\s*\[\s*Schema\s*=\s*(?P<schema>{_M_TEXT})\s*,\s*Item\s*=\s*(?P<object>{_M_TEXT})\s*\]\s*\}}\s*\[Data\]"
_SQL_RELATIONS = tuple(
    re.compile(pattern, re.DOTALL)
    for pattern in (
        rf"\s*{_SQL_DATABASE}\s*{_NAVIGATION}\s*",
        rf"\s*let\s+(?P<source>{_M_ID})\s*=\s*{_SQL_DATABASE}\s*,\s*(?P<table>{_M_ID})\s*=\s*(?P=source)\s*{_NAVIGATION}\s+in\s+(?P=table)\s*",
    )
)


def _rebind_m(expression, source, table):
    match = next(
        (m for pattern in _SQL_RELATIONS if (m := pattern.fullmatch(expression))), None
    )
    if match is None:
        raise BuildError(
            f"tables/{table}/source: unsupported M source. Use a Sql.Database connection followed by one Schema/Item navigation without transforms."
        )
    for key in sorted(
        ("server", "database", "schema", "object"),
        key=lambda k: match.start(k),
        reverse=True,
    ):
        start, end = match.span(key)
        expression = expression[:start] + m_string(source[key]) + expression[end:]
    return expression


def _bind_partition(model, table, source):
    partitions = table.get("partitions", [])
    expression = None
    if partitions:
        if len(partitions) != 1:
            raise BuildError(
                f"tables/{table['name']}/source: rebinding multiple partitions is unsupported"
            )
        partition = partitions[0]
        mode = partition.get("mode", model.get("defaultMode", "import"))
        native_source = partition.get("source", {})
        if native_source.get("type") == "m" and mode in {
            "import",
            "directQuery",
            "dual",
        }:
            native_source["expression"] = _rebind_m(
                native_source.get("expression", ""), source, table["name"]
            )
            return mode
        if native_source.get("type") != "entity" or mode != "directLake":
            raise BuildError(
                f"tables/{table['name']}/source: unsupported authored partition form"
            )
        original = next(
            (
                e
                for e in model.get("expressions", [])
                if e["name"] == native_source.get("expressionSource")
            ),
            {},
        )
        match = re.fullmatch(
            rf"\s*{_SQL_DATABASE}\s*", original.get("expression", ""), re.DOTALL
        )
        if original.get("kind") != "m" or match is None:
            raise BuildError(
                f"tables/{table['name']}/source: unsupported Direct Lake expression. Use a shared Sql.Database expression for SQL endpoint rebinding."
            )
        expression = copy.deepcopy(original)
        for key in ("database", "server"):
            start, end = match.span(key)
            expression["expression"] = (
                expression["expression"][:start]
                + m_string(source[key])
                + expression["expression"][end:]
            )
    expression_name = str(source_identity(source["reference"]).item)
    expressions = model.setdefault("expressions", [])
    existing = next(
        (e for e in expressions if e["name"].casefold() == expression_name.casefold()),
        None,
    )
    if existing is not None:
        expression_name = existing["name"]
    expression = expression or {
        "name": expression_name,
        "kind": "m",
        "expression": f"Sql.Database({m_string(source['server'])}, {m_string(source['database'])})",
    }
    expression["name"] = expression_name
    if existing is None:
        expressions.append(expression)
    else:
        existing.update(expression)
    if not partitions and model.get("defaultMode") == "import":
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
    partition = (
        partitions[0]
        if partitions
        else {
            "name": table["name"],
            "mode": "directLake",
        }
    )
    partition["source"] = {
        **partition.get("source", {}),
        "type": "entity",
        "schemaName": source["schema"],
        "entityName": source["object"],
        "expressionSource": expression_name,
    }
    table["partitions"] = [partition]
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


def _source_columns(table, authored, source, reference):
    """Every source column, refined by the authored column it names.

    An authored column matches by ``sourceColumn``, else by name. One that matches
    nothing must be calculated.
    """

    by_source = {str(c.get("sourceColumn", c["name"])).casefold(): c for c in authored}
    columns = []
    for c in source["source_columns"]:
        name = c["column_name"]
        column = by_source.pop(name.casefold(), {"name": name})
        column.setdefault("sourceColumn", name)
        column.setdefault("dataType", semantic_type(c["data_type"], reference, name))
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
            if source.get("description"):
                table.setdefault("description", source["description"])
            for column in table.get("columns", []):
                note = source.get("column_notes", {}).get(
                    column.get("sourceColumn", column["name"])
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
            source["access"] = "sql"
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
