"""Shared M source expressions selected for environment substitution."""

from collections.abc import Mapping
from dataclasses import replace

from ..declaration.model import WeaverItemId
from ..errors import ConfigError, IdentityError
from .binding import m_string
from .compiler import _merge
from .fragments import expression_text, needs_source_columns, source_table
from .references import source_identity
from .tmdl import Document, PackageEditor


def source_mappings(values):
    if values is None:
        return {}
    if isinstance(values, Mapping):
        pairs = list(values.items())
    else:
        pairs = []
        for value in [values] if isinstance(values, str) else values:
            name, separator, target = value.partition("=")
            if not separator:
                raise ConfigError(
                    "data-source requires Expression=Warehouse/Name or Expression=Lakehouse/Name"
                )
            pairs.append((name, target))
    result = {}
    for name, target in pairs:
        if not isinstance(name, str) or not name or name != name.strip():
            raise ConfigError(
                "data-source expression name must be non-empty without surrounding whitespace"
            )
        item = WeaverItemId.parse(target)
        if str(item) != target or item.item_type not in {"Warehouse", "Lakehouse"}:
            raise ConfigError(
                f"data-source target must be Warehouse/Name or Lakehouse/Name, got {target!r}"
            )
        if name in result:
            raise ConfigError(f"Duplicate data-source mapping: {name}")
        result[name] = target
    return result


def expression_names(parts):
    return {
        node.name
        for filename, content in parts.items()
        if filename.startswith("definition/") and filename.endswith(".tmdl")
        for node in Document(filename, content).spans
        if node.kind == "expression" and node.path == (("expression", node.name),)
    }


def _physical(target, bindings, workspace):
    """A mapped logical item resolves as its build binding or configured target."""

    item = WeaverItemId.parse(target)
    binding = bindings.by_item.get(item)
    if binding is not None:
        return f"{binding.target.physical_kind}/{binding.target.item.name}"
    if item in workspace.targets:
        return f"{item.item_type}/{workspace.targets[item].physical}"
    return target


def configure_sources(repository, mappings, bindings, workspace):
    explicit = source_mappings(mappings)
    mappings = {**workspace.data_sources, **explicit}
    models = dict(repository.semantic_models)
    matched = set()
    for item, contribution in models.items():
        if item not in bindings.by_item:
            continue
        existing = expression_names(contribution.parts)
        generated = {
            str(source_identity(ref).item)
            for table, ref in contribution.source_references.items()
            if needs_source_columns(source_table(contribution.parts, table))
        }
        names = existing | generated
        selected = {}
        for name in sorted(names):
            try:
                logical = WeaverItemId.parse(name)
            except (IdentityError, ValueError):
                logical = None
            if logical is not None and logical.item_type not in {
                "Warehouse",
                "Lakehouse",
            }:
                logical = None
            if name in mappings:
                target = _physical(mappings[name], bindings, workspace)
            elif logical is not None:
                binding = bindings.by_item.get(logical)
                if binding is not None:
                    target = (
                        f"{binding.target.physical_kind}/{binding.target.item.name}"
                    )
                elif logical in workspace.targets:
                    target = (
                        f"{logical.item_type}/{workspace.targets[logical].physical}"
                    )
                elif name in generated and name not in existing:
                    continue
                else:
                    target = str(logical)
            else:
                continue
            selected[name] = {"target": target}
            if name not in existing:
                selected[name]["generated"] = True
        matched.update(selected)
        models[item] = replace(contribution, expression_sources=selected)
    if missing := set(explicit) - matched:
        raise ConfigError(
            "No selected semantic model has shared expression(s): "
            + ", ".join(sorted(missing))
        )
    return replace(repository, semantic_models=models)


def read_expression_sources(repository, bindings, *, session, workspace):
    sources = {}
    for item, contribution in repository.semantic_models.items():
        if item not in bindings.by_item:
            continue
        for name, source in contribution.expression_sources.items():
            target = WeaverItemId.parse(source["target"])
            locations = PackageEditor(contribution.parts).locations(
                (("expression", name),)
            )
            authored = next(((d, n) for d, n in locations if n.value is not None), None)
            sql = (
                source.get("generated", False)
                or target.item_type == "Warehouse"
                or (
                    authored is not None
                    and "Sql.Database" in m_code(expression_text(*authored))
                )
            )
            if sql:
                observed = session.semantic_source(
                    target.item_name,
                    item_type=target.item_type,
                    schema="",
                    name="",
                    include_columns=False,
                    workspace=workspace,
                )
            else:
                resolved = session.resolve_item(
                    target.item_name, item_type=target.item_type, workspace=workspace
                )
                observed = {
                    "item_type": resolved.type,
                    "item_name": resolved.name,
                    "item_id": resolved.id,
                    "workspace_id": resolved.workspace_id,
                }
            sources.setdefault(str(item), {})[name] = {
                **source,
                **observed,
                "connector": "sql" if sql else "lakehouse",
            }
    return sources


def m_code(text):
    """Mask M strings and comments when identifying an opted-in connector."""
    output = []
    index = 0
    while index < len(text):
        if text.startswith("//", index):
            end = text.find("\n", index)
            index = len(text) if end < 0 else end
        elif text.startswith("/*", index):
            index += 2
            depth = 1
            while index < len(text) and depth:
                if text.startswith("/*", index):
                    depth += 1
                    index += 2
                elif text.startswith("*/", index):
                    depth -= 1
                    index += 2
                else:
                    index += 1
        elif text[index] == '"':
            index += 1
            while index < len(text):
                if text.startswith('""', index):
                    index += 2
                elif text[index] == '"':
                    index += 1
                    break
                else:
                    index += 1
        else:
            output.append(text[index])
            index += 1
    return "".join(output)


def _sql_database(authored, server, database):
    """`Sql.Database` for server and database, keeping an authored options record."""

    from .m_source import sql_database, tokens

    authored = authored.strip()
    found = sql_database(tokens(authored))
    if found is None:
        return f"Sql.Database({m_string(server)}, {m_string(database)})"
    old_server, old_database = found
    return (
        authored[: old_server.start]
        + m_string(server)
        + authored[old_server.end : old_database.start]
        + m_string(database)
        + authored[old_database.end :]
    )


def bind_expression_sources(repository, sources):
    models = dict(repository.semantic_models)
    for item, observed in sources.items():
        identity = WeaverItemId.parse(item)
        contribution = models[identity]
        editor = PackageEditor(contribution.parts)
        requested = contribution.requested
        for name, source in observed.items():
            if source.get("generated"):
                continue
            if source["connector"] == "sql":
                authored = next(
                    (
                        expression_text(d, n)
                        for d, n in editor.locations((("expression", name),))
                        if n.value is not None
                    ),
                    "",
                )
                expression = _sql_database(
                    authored, source["server"], source["database"]
                )
            else:
                expression = (
                    "let\n    Source = Lakehouse.Contents([]),\n"
                    f"    Workspace = Source{{[workspaceId={m_string(source['workspace_id'])}]}}[Data],\n"
                    f"    Lakehouse = Workspace{{[lakehouseId={m_string(source['item_id'])}]}}[Data]\n"
                    "in\n    Lakehouse"
                )
            editor.expression((("expression", name),), "expression", expression)
            requested = _merge(
                requested, {"expressions": [{"name": name, "expression": expression}]}
            )
        from .lineage import tmdl_bindings

        bindings = {
            **contribution.source_bindings,
            **tmdl_bindings(editor.parts, observed),
        }
        models[identity] = replace(
            contribution,
            parts=editor.parts,
            requested=requested,
            expression_sources=observed,
            source_bindings=bindings,
        )
    return replace(repository, semantic_models=models)
