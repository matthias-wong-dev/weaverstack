"""Target projection — the maximal coherent subgraph for the bound targets.

Synthetic graphs only: a node id's ``kind:`` head decides which physical
binding it needs, so these tests exercise the projection algorithm without a
repository or Spark. They pin the two shapes the plan calls out — a Lakehouse
chain ending in a Warehouse leaf, and a Delta node stranded above a Warehouse
producer — plus determinism, closure, and the empty-projection contract.
"""

from __future__ import annotations

import pytest

from weaver.build_bundle.models import OMIT_DEPENDS_ON_OMITTED, OMIT_TARGET_UNBOUND
from weaver.build_bundle.planner import project
from weaver.build_bundle.targets import LAKEHOUSE_TARGET, WAREHOUSE_TARGET
from weaver.errors import BuildError
from weaver.ses.graph import Graph

LAKEHOUSE_ONLY = frozenset({LAKEHOUSE_TARGET})
BOTH = frozenset({LAKEHOUSE_TARGET, WAREHOUSE_TARGET})

# folder:Raw.CustomerCsv -> delta:DWG.Customer -> sql:Reporting.CustomerReport
CHAIN = Graph(
    ["folder:Raw.CustomerCsv", "delta:DWG.Customer", "sql:Reporting.CustomerReport"],
    [
        ("folder:Raw.CustomerCsv", "delta:DWG.Customer"),
        ("delta:DWG.Customer", "sql:Reporting.CustomerReport"),
    ],
)


def _omitted(projection):
    return {node.node_id: node.reason for node in projection.omitted}


# --- Lakehouse-only projection ----------------------------------------------


def test_lakehouse_only_retains_folder_and_delta_omits_warehouse():
    projection = project(CHAIN, bound_target_kinds=LAKEHOUSE_ONLY)

    assert set(projection.retained) == {"folder:Raw.CustomerCsv", "delta:DWG.Customer"}
    assert _omitted(projection) == {"sql:Reporting.CustomerReport": OMIT_TARGET_UNBOUND}


def test_projected_graph_has_no_dangling_edge():
    projection = project(CHAIN, bound_target_kinds=LAKEHOUSE_ONLY)

    retained = set(projection.retained)
    for edge in projection.graph.edges:
        assert edge.upstream in retained and edge.downstream in retained


def test_warehouse_binding_retains_the_whole_chain():
    projection = project(CHAIN, bound_target_kinds=BOTH)

    assert set(projection.retained) == set(CHAIN.nodes)
    assert projection.omitted == ()


# --- transitive removal ------------------------------------------------------


def test_delta_above_a_warehouse_producer_is_transitively_omitted():
    # sql:W.A -> delta:L.B : the Delta node loses its only producer.
    graph = Graph(["sql:W.A", "delta:L.B"], [("sql:W.A", "delta:L.B")])

    projection = project(graph, bound_target_kinds=LAKEHOUSE_ONLY)

    assert projection.is_empty
    assert _omitted(projection) == {
        "sql:W.A": OMIT_TARGET_UNBOUND,
        "delta:L.B": OMIT_DEPENDS_ON_OMITTED,
    }


def test_a_deep_lakehouse_branch_survives_a_sibling_warehouse_branch():
    # folder:R.F -> delta:D.T ; and independently sql:W.A -> delta:D.U
    graph = Graph(
        ["folder:R.F", "delta:D.T", "sql:W.A", "delta:D.U"],
        [("folder:R.F", "delta:D.T"), ("sql:W.A", "delta:D.U")],
    )

    projection = project(graph, bound_target_kinds=LAKEHOUSE_ONLY)

    assert set(projection.retained) == {"folder:R.F", "delta:D.T"}
    assert _omitted(projection) == {
        "sql:W.A": OMIT_TARGET_UNBOUND,
        "delta:D.U": OMIT_DEPENDS_ON_OMITTED,
    }


# --- determinism and the empty contract -------------------------------------


def test_projection_is_deterministic():
    first = project(CHAIN, bound_target_kinds=LAKEHOUSE_ONLY)
    second = project(CHAIN, bound_target_kinds=LAKEHOUSE_ONLY)

    assert first.retained == second.retained
    assert first.omitted == second.omitted
    assert first.graph.layers() == second.graph.layers()


def test_no_binding_yields_an_empty_but_valid_projection():
    projection = project(CHAIN, bound_target_kinds=frozenset())

    assert projection.is_empty
    assert set(_omitted(projection)) == set(CHAIN.nodes)
    assert all(reason == OMIT_TARGET_UNBOUND for reason in _omitted(projection).values())


def test_unrecognised_target_kind_is_rejected():
    graph = Graph(["mystery:X.Y"], [])

    with pytest.raises(BuildError, match="unrecognised target kind"):
        project(graph, bound_target_kinds=LAKEHOUSE_ONLY)
