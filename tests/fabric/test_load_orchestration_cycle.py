"""Load orchestration across the one boundary only Fabric has.

Composition over three Lakehouse dispatch kinds is decided without a tenant.
What needs one is the crossing, because only a real Lakehouse has a SQL
analytics endpoint to cross:

.. code-block:: text

    load Producer DWG.Customer      a Delta table, loaded by a deployed module
      → refresh Producer endpoint   the metadata Fabric syncs behind the write
      → load Consumer Rpt.CustomerReport   a generated Warehouse procedure

That is the whole subject. A Warehouse reads a Lakehouse table through its SQL
analytics endpoint, and the endpoint lags the Delta mutation, so a consumer that
ran before the refresh would read the previous shape, and often does. The barrier
is a node in the graph precisely so it can be ordered, inspected and asserted
rather than hidden inside dispatch.

**`weaver.load(...)` is called, not a seam beneath it.** Load runs where the data
is, so this runs inside the session on the installed wheel, which is also the
one thing the local twin has to substitute for, since its shared Spark session
cannot survive the public entry acquiring its own.

This is not the first sight of any defect in a primitive, an individual DAG edge
case, fault-tolerant propagation, or a task-log field contract. Those are owned
below it.
"""

from __future__ import annotations

import pytest
from support.build_envs import LOAD_ORCHESTRATION_WAREHOUSE_FIXTURE
from support.weaver_test import weaver_test

pytestmark = pytest.mark.parametrize(
    "weaver_repo_fixture",
    [LOAD_ORCHESTRATION_WAREHOUSE_FIXTURE],
    indirect=True,
)

CUSTOMER = "DWG.Customer"
REPORT = "Rpt.CustomerReport"
SEED = "DWG.Seed"


#: Both runs in one round trip, and the physical evidence with them. A Livy call
#: is an architectural decision here, not an implementation detail: the dry run
#: and the real run are two moments of one estate, and asking about them
#: separately would be two claims about two instants.
BODY = """
import weaver
from weaver.sessions.host import use_or_create_session

requested = ["Lakehouse/{lakehouse}", "Warehouse/{warehouse}"]

with use_or_create_session(None, workspace=workspace) as session:
    dry = weaver.load(
        requested, dry_run=True, fault_tolerant=False, session=session
    )
    real = weaver.load(
        requested, dry_run=False, fault_tolerant=False, session=session
    )

# Session close is the evidence durability barrier.
with use_or_create_session(None, workspace=workspace) as session:
    from weaver.catalogue.connection import catalogue_connection

    rows = catalogue_connection(session, workspace).rows(
        "select [Task type], [Target type], [Target name], [Schema name], "
        "[Object name], [Result] from [_].[Log] "
        "where [Workflow ID] = N'" + str(real.workflow_id) + "'"
    )
    log = [dict(row) for row in rows]

emit({{
    "dry": dry.to_mapping(),
    "real": real.to_mapping(),
    "log": log,
    "workflow_id": real.workflow_id,
}})
"""


@pytest.fixture(scope="module")
def orchestrated(fabric_mixed_estate):
    """One installed mixed estate, loaded once: dry, then for real."""

    env = fabric_mixed_estate.env
    seen = env.run_python(
        BODY.format(lakehouse=env.target.name, warehouse=env.warehouse.item.name),
        label="orchestrate the installed load graph",
    )
    return env, seen


def node_names(report) -> list[str]:
    """The order, with the physical target stripped so a name is readable."""

    return [node_id.split(":", 1)[1] for node_id in report["order"]]


def by_node(report) -> dict:
    return {node["node_id"]: node for node in report["nodes"]}


# --- the plan, resolved and not executed --------------------------------------


@weaver_test(hosted=True)
def test_the_requested_targets_resolve_to_the_installed_physical_graph(orchestrated):
    """Catalogue in, physical graph out: with the repository playing no part."""

    env, seen = orchestrated
    dry = seen["dry"]

    assert dry["status"] == "succeeded", dry["messages"]
    assert node_names(dry) == [
        f"Lakehouse/{env.target.name}/{SEED}",
        f"Lakehouse/{env.target.name}/{CUSTOMER}",
        f"Lakehouse/{env.target.name}",
        f"Warehouse/{env.warehouse.item.name}/{REPORT}",
    ]


@weaver_test(hosted=True)
def test_exactly_one_endpoint_refresh_stands_between_the_two_sides(orchestrated):
    env, seen = orchestrated
    dry = seen["dry"]

    refresh = f"refresh:Lakehouse/{env.target.name}"
    customer = f"load:Lakehouse/{env.target.name}/{CUSTOMER}"
    report = f"load:Warehouse/{env.warehouse.item.name}/{REPORT}"

    refreshes = [
        node for node in dry["nodes"] if node["primitive_kind"] == "endpoint_refresh"
    ]
    assert [node["node_id"] for node in refreshes] == [refresh]
    # The crossing is physical: the consumer waits on the barrier, and the
    # barrier waits on the Delta load that fills what it publishes.
    assert [refresh, report] in [list(edge) for edge in dry["edges"]] or (
        [refresh, report] in dry["edges"]
    )
    assert [customer, refresh] in [list(edge) for edge in dry["edges"]]


@weaver_test(hosted=True)
def test_every_node_resolves_to_its_exact_installed_primitive(orchestrated):
    env, seen = orchestrated
    nodes = by_node(seen["dry"])
    lakehouse, warehouse = env.target.name, env.warehouse.item.name

    kinds = {node_id: node["primitive_kind"] for node_id, node in nodes.items()}
    assert kinds == {
        f"load:Lakehouse/{lakehouse}/{SEED}": "python_folder",
        f"load:Lakehouse/{lakehouse}/{CUSTOMER}": "python_table",
        f"refresh:Lakehouse/{lakehouse}": "endpoint_refresh",
        f"load:Warehouse/{warehouse}/{REPORT}": "warehouse_procedure",
    }
    assert nodes[f"load:Warehouse/{warehouse}/{REPORT}"]["dispatch_location"] == (
        f"Warehouse/{warehouse}/[_].[Load {REPORT}]"
    )
    # The deployed modules are addressed through the Lakehouse that owns them,
    # never through a notebook's attachment. Named logically: a dry run says
    # what it intends to reach without reaching a workspace to resolve it, so
    # the Lakehouse is in the name rather than in an absolute URL.
    for node_id in (
        f"load:Lakehouse/{lakehouse}/{SEED}",
        f"load:Lakehouse/{lakehouse}/{CUSTOMER}",
    ):
        location = nodes[node_id]["dispatch_location"]
        assert location.startswith(f"Lakehouse/{lakehouse}/")
        assert "/_/Load/" in location
    assert all(node["status"] == "validated" for node in nodes.values())
    assert not any(node["executed"] for node in nodes.values())
    assert seen["dry"]["workflow_id"] is None


# --- and then executed --------------------------------------------------------


@weaver_test(hosted=True)
def test_the_executed_graph_is_the_one_the_dry_run_planned(orchestrated):
    _env, seen = orchestrated

    assert seen["real"]["order"] == seen["dry"]["order"]
    assert seen["real"]["edges"] == seen["dry"]["edges"]
    assert {
        node["node_id"]: node["dispatch_location"] for node in seen["real"]["nodes"]
    } == {node["node_id"]: node["dispatch_location"] for node in seen["dry"]["nodes"]}


@weaver_test(hosted=True)
def test_every_step_ran_in_topological_order_through_its_own_primitive(orchestrated):
    _env, seen = orchestrated
    real = seen["real"]

    assert real["status"] == "succeeded", real["messages"]
    assert all(node["executed"] for node in real["nodes"])
    assert not any(
        node["status"] in ("blocked", "skipped", "failed") for node in real["nodes"]
    )
    # Executed in the planned order, not reported in it.
    started = [
        node["started_at"]
        for node in sorted(
            real["nodes"], key=lambda node: real["order"].index(node["node_id"])
        )
    ]
    assert started == sorted(started)


@weaver_test(hosted=True, resources={"tds"})
def test_the_warehouse_sees_the_delta_rows_the_lakehouse_load_wrote(
    orchestrated, warehouse_session
):
    """The whole point of the barrier, asserted through the target itself."""

    env, _seen = orchestrated

    rows = env.warehouse.executor.query(
        "select count(*) as n from [Rpt].[CustomerReport]"
    )
    assert rows[0]["n"] == 3
    names = env.warehouse.executor.query(
        "select CustomerName from [Rpt].[CustomerReport] order by CustomerId"
    )
    assert [row["CustomerName"] for row in names] == ["Ada", "Grace", "Katherine"]


@weaver_test(hosted=True)
def test_the_upstream_delta_rows_exist_and_the_folder_materialised(orchestrated):
    env, seen = orchestrated
    real = by_node(seen["real"])

    customer = real[f"load:Lakehouse/{env.target.name}/{CUSTOMER}"]
    assert customer["rows"]["rows_inserted"] == 3
    assert env.store.exists(
        env.resolver.files_root(env.target).join("DWG", "Seed", "customers.csv")
    )


# --- the evidence it left behind ----------------------------------------------


@weaver_test(hosted=True)
def test_the_real_run_wrote_one_coherent_workflow_into_the_log(orchestrated):
    """One row per settled node, correlated by the run's own Workflow ID.

    No plan row and no completion row: a workflow is its rows.
    """

    _env, seen = orchestrated
    written = seen["log"]

    assert seen["workflow_id"]
    assert all(row["Task type"] == "load" for row in written)
    # Three loads and the endpoint refresh between them, and nothing else.
    assert len(written) == 4
    assert sum(row["Object name"] is None for row in written) == 1
    assert {row["Result"] for row in written} == {"Succeeded"}
    # Physical identity, which is what a log records.
    assert {row["Target type"] for row in written} == {"Lakehouse", "Warehouse"}
