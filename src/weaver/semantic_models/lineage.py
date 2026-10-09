"""Observable shared-source relations and their existing Weaver identities."""

import re

from ..catalogue.claims import catalogue_columns
from ..declaration.metadata import TABLE, VIEW
from ..declaration.model import WeaverItemId
from .fragments import source_table
from .tmdl import Document

_M_ID = r'(?:#"(?:[^"]|"")*"|[A-Za-z_][A-Za-z0-9_]*)'
_M_TEXT = r'"(?:[^"]|"")*"'
_NAVIGATION = rf"\{{\s*\[\s*Schema\s*=\s*(?P<schema>{_M_TEXT})\s*,\s*Item\s*=\s*(?P<object>{_M_TEXT})\s*\]\s*\}}\s*\[Data\]"
_DIRECT = re.compile(rf"\s*(?P<expression>{_M_ID})\s*{_NAVIGATION}\s*", re.DOTALL)
_LET = re.compile(
    rf"\s*let\s+(?P<root>{_M_ID})\s*=\s*(?P<expression>{_M_ID})\s*,\s*(?P<result>{_M_ID})\s*=\s*(?P=root)\s*{_NAVIGATION}\s+in\s+(?P=result)\s*",
    re.DOTALL,
)


def _m_value(value):
    if value.startswith('#"'):
        value = value[1:]
    if value.startswith('"'):
        value = value[1:-1].replace('""', '"')
    return re.sub(
        r"#\((#|lf|cr|tab)\)",
        lambda m: {"#": "#", "lf": "\n", "cr": "\r", "tab": "\t"}[m[1]],
        value,
    )


def source_relation(source):
    if source.get("type") == "entity":
        return (
            source.get("expressionSource"),
            source.get("schemaName", "dbo"),
            source.get("entityName"),
        )
    if source.get("type") != "m":
        return None
    text = source.get("expression", "")
    text = "\n".join(text) if isinstance(text, list) else text
    match = _DIRECT.fullmatch(text) or _LET.fullmatch(text)
    return (
        tuple(_m_value(match[key]) for key in ("expression", "schema", "object"))
        if match
        else None
    )


def managed_relations(repository, catalogue, bindings, source):
    aliases = {WeaverItemId.parse(item) for item in source["logical_items"]}
    relations = {}
    for identity, registered in catalogue.registered.items():
        if (
            identity.item in aliases
            and identity.item not in bindings.by_item
            and registered.object_type in {"table", "view"}
        ):
            schema, name = catalogue_columns(identity)
            relations[str(identity)] = {
                "schema": schema.removeprefix("Tables/"),
                "object": name,
                "reference": str(identity),
                "object_type": registered.object_type,
            }
    for identity, document in repository.source_documents.items():
        if (
            identity.item in aliases
            and identity.item in bindings.by_item
            and document.kind in {TABLE, VIEW}
        ):
            schema, name = catalogue_columns(identity)
            relations[str(identity)] = {
                "schema": schema.removeprefix("Tables/"),
                "object": name,
                "reference": str(identity),
                "object_type": document.kind.lower(),
            }
    return list(relations.values())


def source_bindings(consumers, expressions):
    result = {}
    for table, partition in consumers:
        relation = source_relation(partition.get("source", {}))
        if not relation or relation[0] not in expressions:
            continue
        expression, schema, name = relation
        source = expressions[expression]
        matches = [
            r
            for r in source["relations"]
            if r["schema"] == schema and r["object"] == name
        ]
        binding = {
            key: value
            for key, value in source.items()
            if key not in {"relations", "logical_items", "source_columns"}
        }
        binding.update(
            expression=expression,
            schema=schema,
            object=name,
            mode=partition.get("mode"),
            access=source["connector"],
        )
        if len(matches) == 1:
            binding.update(matches[0])
        entries = result.setdefault(table, [])
        if binding not in entries:
            entries.append(binding)
    return {
        table: values[0] if len(values) == 1 else {"sources": values}
        for table, values in result.items()
    }


def tmdl_bindings(parts, expressions):
    names = {
        node.name
        for path, content in parts.items()
        if path.endswith(".tmdl")
        for node in Document(path, content).spans
        if node.kind == "table" and len(node.path) == 1
    }
    return source_bindings(
        (
            (name, partition)
            for name in sorted(names)
            for partition in source_table(parts, name).get("partitions", ())
        ),
        expressions,
    )


def observed_bindings(model, expressions):
    return source_bindings(
        (
            (table["name"], partition)
            for table in model["model"].get("tables", ())
            for partition in table.get("partitions", ())
        ),
        expressions,
    )


def verify_lineage(contribution, model):
    from ..errors import InstallError

    if not contribution.expression_sources:
        return
    actual = observed_bindings(model, contribution.expression_sources)
    planned = set(
        dependency_references(
            {},
            {
                name: value
                for name, value in contribution.source_bindings.items()
                if name not in contribution.source_references
            },
        )
    )
    missing = planned - set(dependency_references({}, actual))
    if missing:
        raise InstallError(
            f"Semantic readback does not confirm mapped table lineage: {sorted(missing)}"
        )


def dependency_references(references, bindings):
    result = set(references.items())
    for table, value in bindings.items():
        for source in value.get("sources", [value]):
            if reference := source.get("reference"):
                result.add((table, reference))
    return tuple(sorted(result))


#: Partition sources that compute their rows rather than read them.
_COMPUTED = frozenset({"calculated", "calculationgroup"})


def _traced(source, expressions):
    relation = source_relation(source)
    if not relation or relation[0] not in expressions:
        return False
    _, schema, name = relation
    matches = [
        r
        for r in expressions[relation[0]].get("relations", ())
        if r["schema"] == schema and r["object"] == name
    ]
    return len(matches) == 1


def untraced_tables(contribution):
    """Tables without Weaver.Source that read data from an untraced partition."""

    declared = {name.casefold() for name in contribution.source_references}
    return tuple(
        name
        for name in contribution.table_names
        if name.casefold() not in declared
        and any(
            str(partition["source"].get("type")).casefold() not in _COMPUTED
            and not _traced(partition["source"], contribution.expression_sources)
            for partition in source_table(contribution.parts, name).get(
                "partitions", ()
            )
        )
    )


def untraced_warning(item, contribution):
    tables = untraced_tables(contribution)
    if not tables:
        return None
    noun = "table" if len(tables) == 1 else "tables"
    return (
        f"{item}: {noun} {', '.join(tables)} read data that is not traced to a "
        "managed Table or View. Add Weaver.Source to name each table's source"
    )
