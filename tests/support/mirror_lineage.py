"""Lineage as a Mirror commits it, for tests of the Build that follows."""

from __future__ import annotations

from weaver.catalogue.lineage_rows import installed_workspaces, lineage_rows, node_key
from weaver.catalogue.state import Catalogue
from weaver.catalogue.tables import EDGE_EXTERNAL, LINEAGE_EDGE, LINEAGE_NODE, MIRROR


def as_mirror_commits(catalogue: Catalogue, *, source_workspace: str) -> Catalogue:
    """``catalogue``, with lineage forked from a source in ``source_workspace``.

    The source projected each item where nothing was borrowed. Mirror then
    re-keys every endpoint by this catalogue's installations and marks what
    ``_.Mirror`` records, in place (``lineage_key_statements``,
    ``lineage_statement``), so signatures stay the source's.
    """

    here = installed_workspaces(catalogue)

    def owner(node_id):
        for item in catalogue.rows:
            if node_id == str(item) or node_id.startswith(f"{item}/"):
                return item
        return None

    def keyed(node_id):
        return None if node_id is None else node_key(node_id, here.get(owner(node_id)))

    rows = {}
    for item, tables in catalogue.rows.items():
        declared = {
            name: found for name, found in tables.items() if name != MIRROR.name
        }
        source = lineage_rows(
            Catalogue({item: declared}),
            item,
            workspaces=dict.fromkeys(catalogue.rows, source_workspace),
        )
        borrowed = {
            (row["schema_name"], row["object_name"])
            for row in tables.get(MIRROR.name, ())
        }
        rows[item] = {
            **tables,
            LINEAGE_NODE.name: tuple(
                {
                    **row,
                    "node_key": keyed(row["node_id"]),
                    **(
                        {"is_mirrored": True, "can_load": False}
                        if (row["schema_name"], row["object_name"]) in borrowed
                        else {}
                    ),
                }
                for row in source[LINEAGE_NODE.name]
            ),
            LINEAGE_EDGE.name: tuple(
                {
                    **row,
                    "downstream_node_key": keyed(row["downstream_node_id"]),
                    "upstream_node_key": (
                        row["upstream_node_key"]
                        if row["edge_kind"] == EDGE_EXTERNAL
                        else keyed(row["upstream_node_id"])
                    ),
                    "through_node_key": keyed(row["through_node_id"]),
                }
                for row in source[LINEAGE_EDGE.name]
            ),
        }
    return Catalogue(rows=rows, materialised=catalogue.materialised)
