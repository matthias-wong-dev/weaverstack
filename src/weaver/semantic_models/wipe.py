"""Native definitions for resetting one semantic model."""

import copy
import hashlib
import json

from ..errors import CommandError, InstallError
from .definition import decode_model, decode_parts, encode_parts
from .deployed import canonical_model
from .render import empty_parts
from .tmdl import PackageEditor


def reset_definition(
    name, observed, *, parts=None, preserve_data_source=False, connections=()
):
    editor = PackageEditor(empty_parts(name))
    model = observed["model"]
    culture = model.get("culture")
    if culture:
        editor.property((), "culture", culture)
    retained = []
    expected = {"culture": culture, "tables": [], "expressions": []}
    source = _preserved_partition(model, connections) if preserve_data_source else None
    if source is not None:
        table, partition = source
        retained = [partition["source"]["expressionSource"]]
        native = PackageEditor(parts)
        expressions = [
            _fragment(native, (("expression", expression["name"]),))
            for expression in model.get("expressions", [])
        ]
        editor.parts["definition/expressions.tmdl"] = b"".join(expressions)
        fragment = _fragment(
            native, (("table", table["name"]), ("partition", partition["name"]))
        )
        lines = fragment.decode().splitlines(keepends=True)
        header = next(
            index
            for index, line in enumerate(lines)
            if line.lstrip().startswith("partition ")
        )
        lines[header] = "partition 'Source' = entity\n"
        editor.parts["definition/tables/__WeaverSource.tmdl"] = (
            "table '__WeaverSource'\n\tisHidden: true\n"
            + "".join("\t" + line for line in lines)
        ).encode()
        expected["tables"] = [
            {
                "name": "__WeaverSource",
                "isHidden": True,
                "partitions": [{**copy.deepcopy(partition), "name": "Source"}],
            }
        ]
        expected["expressions"] = copy.deepcopy(model.get("expressions", []))
    return {
        "definition": encode_parts(editor.parts),
        "preserve_data_source": preserve_data_source,
        "retained_sources": retained,
        "expected": expected,
        "connection_signature": connection_signature(connections if retained else ()),
    }


def _fragment(editor, path):
    found = editor.locations(path)
    if len(found) != 1:
        raise CommandError(f"Source TMDL must contain exactly one {path!r}")
    document, span = found[0]
    return "".join(
        line[len(span.prefix) :] if line.startswith(span.prefix) else line
        for line in document.lines[span.description_start : span.end]
    ).encode()


def _preserved_partition(model, connections):
    partitions = [
        (table, partition)
        for table in model.get("tables", [])
        for partition in table.get("partitions", [])
        if partition.get("source", {}).get("type") != "calculated"
    ]
    if (
        not partitions
        and not connections
        and not model.get("expressions")
        and not model.get("dataSources")
    ):
        return None
    qualified = (
        len(connections) == 1
        and connections[0].get("connectivityType") == "Automatic"
        and not connections[0].get("id")
        and not connections[0].get("gatewayId")
        and connections[0].get("connectionDetails", {}).get("type") == "SQL"
        and partitions
        and not model.get("dataSources")
        and all(
            partition.get("mode") == "directLake"
            and partition.get("source", {}).get("type") == "entity"
            for _, partition in partitions
        )
    )
    expressions = {
        partition.get("source", {}).get("expressionSource")
        for _, partition in partitions
    }
    if (
        not qualified
        or len(expressions) != 1
        or not all(isinstance(name, str) and name for name in expressions)
    ):
        raise CommandError(
            "--preserve-data-source supports one Automatic SQL source with Direct Lake entity partitions"
        )
    names = [expression["name"] for expression in model.get("expressions", [])]
    if len(names) != len(set(names)) or not expressions <= set(names):
        raise CommandError("The preserved source expression is missing or duplicated")
    return partitions[0]


def connection_signature(connections):
    values = sorted(
        json.dumps(value, sort_keys=True, separators=(",", ":"))
        for value in connections
    )
    return hashlib.sha256(json.dumps(values).encode()).hexdigest()


def prepare_reset(client, name, *, preserve_data_source):
    observed = decode_model(client.get_definition())
    connections = client.get_connections()
    parts = (
        decode_parts({**client.get_definition(format="TMDL"), "format": "TMDL"})
        if preserve_data_source
        else None
    )
    spec = reset_definition(
        name,
        observed,
        parts=parts,
        preserve_data_source=preserve_data_source,
        connections=connections,
    )
    spec.update(workspace_id=client.workspace_id, model_id=client.model_id)
    spec["before_definition"] = _digest(canonical_model(observed))
    spec["before_connection"] = connection_signature(connections)
    spec["removed"] = [
        f"{kind}/{value['name']}"
        for kind in (
            "tables",
            "relationships",
            "roles",
            "perspectives",
            "cultures",
            "expressions",
            "dataSources",
            "functions",
        )
        for value in observed["model"].get(kind, [])
        if kind != "expressions" or not preserve_data_source
    ]
    return spec


def _digest(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def verify_prepared(spec, observed, connections):
    if (
        _digest(canonical_model(observed)) != spec["before_definition"]
        or connection_signature(connections) != spec["before_connection"]
    ):
        raise CommandError(
            "Semantic model or connection changed after wipe preparation; plan the wipe again"
        )


def verify_reset(spec, observed, connections):
    model = observed["model"]
    expected = spec["expected"]
    if model.get("culture") != expected["culture"]:
        raise InstallError("Semantic wipe changed the model culture")
    tables = model.get("tables", [])
    if len(tables) != len(expected["tables"]):
        raise InstallError("Semantic wipe readback retains unexpected tables")
    for table, wanted in zip(tables, expected["tables"]):
        if table.get("name") != wanted["name"] or table.get("isHidden") is not True:
            raise InstallError(
                "Semantic wipe readback differs from the hidden source table"
            )
        if table.get("columns") or table.get("measures"):
            raise InstallError("Semantic wipe readback retains table content")
        if canonical_model(
            {"model": {"tables": [{"partitions": table.get("partitions", [])}]}}
        ) != canonical_model(
            {"model": {"tables": [{"partitions": wanted["partitions"]}]}}
        ):
            raise InstallError("Semantic wipe readback changed the source partition")
    if canonical_model(
        {"model": {"expressions": model.get("expressions", [])}}
    ) != canonical_model({"model": {"expressions": expected["expressions"]}}):
        raise InstallError("Semantic wipe readback changed the source expressions")
    if connection_signature(connections) != spec["connection_signature"]:
        raise InstallError("Semantic wipe readback changed the connection")
