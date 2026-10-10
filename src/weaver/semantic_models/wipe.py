"""Native definitions for resetting one semantic model."""

import copy
import hashlib
import json

from ..errors import CommandError, InstallError
from ..fabric.semantic_model import is_sql_type
from .definition import decode_model, decode_parts, encode_parts
from .deployed import comparable, object_identity, verify_requested
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
        retained = [
            partition["source"].get("expressionSource")
            or connections[0]["connectionDetails"]["path"]
        ]
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
        lines[header] = f"partition 'Source' = {partition['source']['type']}\n"
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
    if _explicit_import(model, partitions, connections):
        return partitions[0]
    qualified = (
        len(connections) == 1
        and connections[0].get("connectivityType") == "Automatic"
        and not connections[0].get("id")
        and not connections[0].get("gatewayId")
        and is_sql_type(connections[0].get("connectionDetails", {}).get("type"))
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
            "--preserve-data-source supports one Automatic SQL Direct Lake source or one ShareableCloud SQL Import source with literal navigation"
        )
    names = [expression["name"] for expression in model.get("expressions", [])]
    if len(names) != len(set(names)) or not expressions <= set(names):
        raise CommandError("The preserved source expression is missing or duplicated")
    return partitions[0]


def _explicit_import(model, partitions, connections):
    from .m_source import relation, sql_database

    if (
        len(connections) != 1
        or connections[0].get("connectivityType") != "ShareableCloud"
        or not connections[0].get("id")
        or not is_sql_type(connections[0].get("connectionDetails", {}).get("type"))
        or not partitions
        or model.get("dataSources")
        or model.get("expressions")
    ):
        return False
    path = connections[0]["connectionDetails"].get("path")
    if not isinstance(path, str) or not path:
        return False
    for _, partition in partitions:
        source = partition.get("source", {})
        if partition.get("mode") != "import" or source.get("type") != "m":
            return False
        expression = source.get("expression", "")
        if isinstance(expression, list):
            expression = "\n".join(expression)
        if not isinstance(expression, str):
            return False
        found = relation(expression)
        database = found and sql_database(list(found.root_tokens))
        if not database:
            return False
        values = [token.value for token in database]
        if any(not value or "#" in value or ";" in value for value in values):
            return False
        if ";".join(values).casefold() != path.casefold():
            return False
    return True


def connection_signature(connections):
    """A digest of what binds a model to its sources, and nothing else Fabric lists."""

    values = sorted(
        json.dumps(_binding(value), sort_keys=True, separators=(",", ":"))
        for value in connections
    )
    return hashlib.sha256(json.dumps(values).encode()).hexdigest()


def _binding(connection):
    details = connection.get("connectionDetails") or {}
    path = details.get("path")
    return {
        "connectivityType": connection.get("connectivityType"),
        "id": connection.get("id"),
        "gatewayId": connection.get("gatewayId"),
        "type": details.get("type"),
        "path": path.casefold() if isinstance(path, str) else path,
    }


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
    spec["removed"] = [
        f"{kind}/{value['name']}"
        for kind in _REMOVED
        for value in observed["model"].get(kind, [])
        if kind != "expressions" or not preserve_data_source
    ]
    spec["before"] = _dependencies(observed, connections, preserve_data_source)
    return spec


_REMOVED = (
    "tables",
    "relationships",
    "roles",
    "perspectives",
    "cultures",
    "expressions",
    "dataSources",
    "functions",
)


def _dependencies(observed, connections, preserve_data_source):
    """A digest of what a prepared wipe writes back or reports removing.

    Names compare as Fabric may recase or quote them, and preserved M or DAX
    by layout, so service normalisation is not drift.
    """

    model = observed["model"]
    value = {
        "culture": _culture(model.get("culture")),
        "removed": sorted(
            [kind, object_identity(member.get("name") or "")]
            for kind in _REMOVED
            for member in model.get(kind, [])
            if isinstance(member, dict)
        ),
    }
    if preserve_data_source:
        value["partitions"] = sorted(
            (
                [
                    object_identity(table.get("name") or ""),
                    object_identity(partition.get("name") or ""),
                    comparable({key: partition.get(key) for key in ("mode", "source")}),
                ]
                for table in model.get("tables", [])
                for partition in table.get("partitions", [])
            ),
            key=lambda each: each[:2],
        )
        value["expressions"] = sorted(
            (
                [
                    object_identity(expression.get("name") or ""),
                    comparable(
                        {key: expression.get(key) for key in ("kind", "expression")}
                    ),
                ]
                for expression in model.get("expressions", [])
            ),
            key=lambda each: each[0],
        )
        value["connection"] = connection_signature(connections)
    return _digest(value)


def _digest(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def verify_prepared(spec, observed, connections):
    if spec["before"] != _dependencies(
        observed, connections, spec["preserve_data_source"]
    ):
        raise CommandError(
            "Semantic model or connection changed after wipe preparation; plan the wipe again"
        )


def verify_reset(spec, observed, connections):
    """Raise unless the reset model is the empty shell Weaver wrote.

    Returns the paths of preserved source content Fabric rewrote.
    """

    model = observed["model"]
    expected = spec["expected"]
    if _culture(model.get("culture")) != _culture(expected["culture"]):
        raise InstallError("Semantic wipe changed the model culture")
    tables = model.get("tables", [])
    if len(tables) != len(expected["tables"]):
        raise InstallError("Semantic wipe readback retains unexpected tables")
    for table, wanted in zip(tables, expected["tables"]):
        if (
            object_identity(table.get("name")) != object_identity(wanted["name"])
            or table.get("isHidden") is not True
        ):
            raise InstallError(
                "Semantic wipe readback differs from the hidden source table"
            )
        if table.get("columns") or table.get("measures"):
            raise InstallError("Semantic wipe readback retains table content")
    expressions = model.get("expressions", [])
    if _names(expressions) != _names(expected["expressions"]):
        raise InstallError("Semantic wipe readback changed the source expressions")
    for table, wanted in zip(tables, expected["tables"]):
        if _names(table.get("partitions", [])) != _names(wanted["partitions"]):
            raise InstallError("Semantic wipe readback changed the source partition")
    try:
        differences = verify_requested(
            {
                "tables": [
                    {"name": wanted["name"], "partitions": wanted["partitions"]}
                    for wanted in expected["tables"]
                ],
                "expressions": expected["expressions"],
            },
            {
                "model": {
                    "tables": [
                        {
                            "name": table["name"],
                            "partitions": table.get("partitions", []),
                        }
                        for table in tables
                    ],
                    "expressions": expressions,
                }
            },
        )
    except InstallError as exc:
        raise InstallError(
            f"Semantic wipe readback changed the source partition or expressions: {exc}"
        ) from exc
    if connection_signature(connections) != spec["connection_signature"]:
        raise InstallError("Semantic wipe readback changed the connection")
    return differences


def _culture(value):
    return value.casefold() if isinstance(value, str) else value


def _names(members):
    return sorted(
        object_identity(member.get("name") or "")
        for member in members
        if isinstance(member, dict)
    )
