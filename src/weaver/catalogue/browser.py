"""Project the installed graph into per-item rows a report can walk.

Each item's rows come from :func:`weaver.installed.item_dag` over that item's
own desired rows, so they never depend on another item's state. A read of an
object in another item is an edge to that object's node ID, whether or not the
object is installed. A read the graph does not resolve is an External edge to
the reference as its author wrote it.
"""

from __future__ import annotations

import hashlib
import json
from types import MappingProxyType

from ..declaration.model import (
    ARTIFACT_SHAPE,
    MODEL_SHAPE,
    WeaverItemId,
    WeaverSchemaId,
)
from .builtin import BUILTIN_ITEM
from .claims import catalogue_columns
from .render import Row
from .state import Catalogue
from .tables import (
    BROWSER_EDGE,
    BROWSER_NODE,
    BROWSER_TABLES,
    CATALOGUE_SCHEMA,
    EDGE_DEPENDENCY,
    EDGE_EXTERNAL,
    EDGE_SHORTCUT,
    EDGE_VALIDATION,
    FOLDER_DICTIONARY,
    ROLE_SHORTCUT,
    SEMANTIC_MODEL_TABLE,
    SIGNATURE,
    TABLE_DICTIONARY,
)

#: The byte width of ``search_text``.
SEARCH_TEXT_BYTES = 4000


def with_browser_rows(
    catalogue: Catalogue, repository, *, current: Catalogue | None = None, warn=None
) -> Catalogue:
    """Add each item's BrowserNode and BrowserEdge rows to desired state.

    The projection is advisory. An item whose rows cannot be projected keeps
    its ``current`` rows, so publication leaves them alone, and ``warn`` says so.
    """

    rows = {}
    for item, tables in catalogue.rows.items():
        merged = dict(tables)
        try:
            projected = browser_rows(
                _with_semantic_sources(item, tables, repository), item
            )
        except Exception as exc:
            kept = current.rows.get(item, {}) if current is not None else {}
            projected = {
                table.name: tuple(kept.get(table.name, ())) for table in BROWSER_TABLES
            }
            if warn is not None:
                warn(
                    f"Catalogue Browser graph for {item} was not updated: "
                    f"{_reason(exc)}. The Build continued."
                )
        merged.update(projected)
        rows[item] = MappingProxyType(merged)
    return Catalogue(rows=MappingProxyType(rows))


def _reason(exc: Exception) -> str:
    return (str(exc).strip() or type(exc).__name__).rstrip(".")


def _with_semantic_sources(item, tables, repository) -> Catalogue:
    """An item's rows, with a model's source tables as Build deploys them.

    Fabric's copy of the model is read only after deployment, so desired state
    has no SemanticModelTable rows yet. The graph reads their source mode and
    access, which come from the authored definition.
    """

    contribution = repository.semantic_models.get(item)
    if contribution is None or tables.get(SEMANTIC_MODEL_TABLE.name):
        return Catalogue({item: tables})
    from .semantic import project_semantic_model

    requested = {
        "model": {"tables": [{"name": name} for name in contribution.table_names]}
    }
    projected = project_semantic_model(item, contribution, deployed=requested)
    return Catalogue(
        {
            item: {
                **tables,
                SEMANTIC_MODEL_TABLE.name: projected[SEMANTIC_MODEL_TABLE.name],
            }
        }
    )


def browser_rows(
    catalogue: Catalogue, item: WeaverItemId
) -> dict[str, tuple[Row, ...]]:
    """BrowserNode and BrowserEdge rows for ``item``, from its rows alone."""

    from ..installed import item_dag

    dag = item_dag(catalogue, item)
    tables = catalogue.rows.get(item, {})
    descriptions = _descriptions(tables)
    nodes = tuple(
        _node_row(item, node, descriptions) for node in dag.nodes if node.item == item
    )
    validations = {node.node_id for node in dag.nodes if node.is_validation}

    through: dict[tuple[str, str, str], set[str]] = {}
    for edge in dag.edges:
        downstream = str(edge.downstream)
        if edge.is_shortcut:
            kind = EDGE_SHORTCUT
        elif downstream in validations:
            kind = EDGE_VALIDATION
        else:
            kind = EDGE_DEPENDENCY
        passes = through.setdefault((downstream, str(edge.upstream), kind), set())
        if edge.through is not None:
            passes.add(str(edge.through))
    for references in (dag.external_references, dag.unresolved_references):
        for consumer, written in references.items():
            for reference in written:
                through.setdefault((str(consumer), reference, EDGE_EXTERNAL), set())

    edges = tuple(
        _signed(
            {
                "item_type": item.item_type,
                "item_name": item.item_name,
                "downstream_node_id": downstream,
                "upstream_node_id": upstream,
                "edge_kind": kind,
                # Two shortcuts to one source can both be read; one is named.
                "through_node_id": min(passes) if passes else None,
            }
        )
        for (downstream, upstream, kind), passes in sorted(through.items())
    )
    return {BROWSER_NODE.name: nodes, BROWSER_EDGE.name: edges}


def _descriptions(tables) -> dict[tuple[str, str], str]:
    found = {}
    for table in (TABLE_DICTIONARY, FOLDER_DICTIONARY):
        for row in tables.get(table.name, ()):
            if row.get("description"):
                key = (str(row.get("schema_name")), str(row.get("object_name")))
                found[key] = str(row["description"])
    return found


def _node_row(item: WeaverItemId, node, descriptions) -> Row:
    schema_name, object_name = catalogue_columns(node.identity)
    if node.is_validation:
        description = node.description
    elif node.role == ROLE_SHORTCUT:
        description = None
    else:
        description = descriptions.get((schema_name, object_name))
    label = _label(node.identity)
    item_label = str(item)
    return _signed(
        {
            "item_type": item.item_type,
            "item_name": item.item_name,
            "schema_name": schema_name,
            "object_name": object_name,
            "node_id": node.node_id,
            "node_kind": _kind(node),
            "label": label,
            "item_label": item_label,
            "description": description,
            "search_text": _search_text(label, item_label, description),
            "is_internal": _is_internal(node.identity),
        }
    )


def _kind(node) -> str:
    if node.is_validation or node.role == ROLE_SHORTCUT:
        return node.role
    return node.object_type


def _label(identity) -> str:
    if isinstance(identity, WeaverSchemaId):
        return identity.schema
    if identity.shape in {MODEL_SHAPE, ARTIFACT_SHAPE}:
        # A model or Report root is known by its item.
        return identity.item.item_name
    return identity.object_id.qualified


def _is_internal(identity) -> bool:
    """The catalogue item, the Catalogue Browser over it, and the ``_`` surface
    the catalogue presents in every item."""

    from ..catalogue_browser import BROWSER_ITEMS

    if identity.item == BUILTIN_ITEM or identity.item in BROWSER_ITEMS:
        return True
    schema = (
        identity.schema
        if isinstance(identity, WeaverSchemaId)
        else identity.object_id.schema
    )
    return schema == CATALOGUE_SCHEMA


def _search_text(*parts) -> str:
    text = " ".join(part for part in parts if part).lower()
    return text.encode("utf-8")[:SEARCH_TEXT_BYTES].decode("utf-8", errors="ignore")


def _signed(row: dict) -> Row:
    content = json.dumps(row, sort_keys=True, ensure_ascii=False)
    return {**row, SIGNATURE: hashlib.sha256(content.encode("utf-8")).hexdigest()}
