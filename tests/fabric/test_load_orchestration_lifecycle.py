"""Load orchestration across the one boundary only Fabric has.

Its local twin (``tests/spark/test_local_load_orchestration_lifecycle.py``) proves the
composition over three Lakehouse dispatch kinds. What it cannot prove is the
crossing, because the emulator has no SQL analytics endpoint to cross:

.. code-block:: text

    load Producer DWG.Customer      a Delta table, loaded by a deployed module
      → refresh Producer endpoint   the metadata Fabric syncs *behind* the write
      → load Consumer Rpt.CustomerReport   a generated Warehouse procedure

That is the whole subject. A Warehouse reads a Lakehouse table through its SQL
analytics endpoint, and the endpoint lags the Delta mutation — so a consumer that
ran before the refresh would read the previous shape, and often does. The barrier
is a node in the graph precisely so it can be ordered, inspected and asserted
rather than hidden inside dispatch.

**`weaver.load(...)` is called, not a seam beneath it.** Load runs where the data
is, so this runs inside the session on the installed wheel — which is also the
one thing the local twin has to substitute for, since its shared Spark session
cannot survive the public entry acquiring its own.

This is not the first sight of any defect in a primitive, an individual DAG edge
case, fault-tolerant propagation, or a task-log field contract. Those are owned
below it.
"""

from __future__ import annotations

import pytest
from support.build_envs import LOAD_ORCHESTRATION_WAREHOUSE_FIXTURE

pytestmark = [
    pytest.mark.fabric,
    pytest.mark.hosted,
    pytest.mark.parametrize(
        "weaver_repo_fixture",
        [LOAD_ORCHESTRATION_WAREHOUSE_FIXTURE],
        indirect=True,
    ),
]

CUSTOMER = "DWG.Customer"
REPORT = "Rpt.CustomerReport"
SEED = "DWG.Seed"


#: Both runs in one round trip, and the physical evidence with them. A Livy call
#: is an architectural decision here, not an implementation detail: the dry run
#: and the real run are two moments of one estate, and asking about them
#: separately would be two claims about two instants.
BODY = '''
import weaver
from weaver.locations import Location
from weaver.resolution import store_for

requested = ["Lakehouse/{lakehouse}", "Warehouse/{warehouse}"]

dry = weaver.load(requested, dry_run=True, fault_tolerant=False)
real = weaver.load(requested, dry_run=False, fault_tolerant=False)

store = store_for(workspace)
log = sorted(
    entry.location.value.rsplit("/", 1)[-1]
    for entry in store.list(Location(real.task_log))
    if not entry.is_directory
)

emit({{
    "dry": dry.to_mapping(),
    "real": real.to_mapping(),
    "log": log,
    "task_log": real.task_log,
}})
'''


@pytest.fixture(scope="module")
def orchestrated(fabric_mixed_estate):
    """One installed mixed estate, loaded once — dry, then for real."""

    env = fabric_mixed_estate.env
    seen = env.run_python(
        BODY.format(
            lakehouse=env.target.name, warehouse=env.warehouse.item.name
        ),
        label="orchestrate the installed load graph",
    )
    return env, seen


def node_names(report) -> list[str]:
    """The order, with the physical target stripped so a name is readable."""

    return [node_id.split(":", 1)[1] for node_id in report["order"]]


def by_node(report) -> dict:
    return {node["node_id"]: node for node in report["nodes"]}


# --- the plan, resolved and not executed --------------------------------------


def test_the_requested_targets_resolve_to_the_installed_physical_graph(orchestrated):
    """Catalogue in, physical graph out — with the repository playing no part."""

    env, seen = orchestrated
    dry = seen["dry"]

    assert dry["status"] == "succeeded", dry["messages"]
    assert node_names(dry) == [
        f"Lakehouse/{env.target.name}/{SEED}",
        f"Lakehouse/{env.target.name}/{CUSTOMER}",
        f"Lakehouse/{env.target.name}",
        f"Warehouse/{env.warehouse.item.name}/{REPORT}",
    ]


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
    # The deployed modules are addressed through the Lakehouse's own OneLake
    # root, never through a notebook's attachment.
    for node_id in (f"load:Lakehouse/{lakehouse}/{SEED}", f"load:Lakehouse/{lakehouse}/{CUSTOMER}"):
        location = nodes[node_id]["dispatch_location"]
        assert location.startswith("abfss://")
        assert "/Files/_/Load/" in location
    assert all(node["status"] == "validated" for node in nodes.values())
    assert not any(node["executed"] for node in nodes.values())
    assert seen["dry"]["task_log"] is None


# --- and then executed --------------------------------------------------------


def test_the_executed_graph_is_the_one_the_dry_run_planned(orchestrated):
    _env, seen = orchestrated

    assert seen["real"]["order"] == seen["dry"]["order"]
    assert seen["real"]["edges"] == seen["dry"]["edges"]
    assert {
        node["node_id"]: node["dispatch_location"] for node in seen["real"]["nodes"]
    } == {
        node["node_id"]: node["dispatch_location"] for node in seen["dry"]["nodes"]
    }


def test_every_step_ran_in_topological_order_through_its_own_primitive(orchestrated):
    _env, seen = orchestrated
    real = seen["real"]

    assert real["status"] == "succeeded", real["messages"]
    assert all(node["executed"] for node in real["nodes"])
    assert not any(
        node["status"] in ("blocked", "skipped", "failed") for node in real["nodes"]
    )
    # Executed in the planned order, not merely reported in it.
    started = [
        node["started_at"]
        for node in sorted(
            real["nodes"], key=lambda node: real["order"].index(node["node_id"])
        )
    ]
    assert started == sorted(started)


def test_the_warehouse_sees_the_delta_rows_the_lakehouse_load_wrote(orchestrated):
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


def test_the_upstream_delta_rows_exist_and_the_folder_materialised(orchestrated):
    env, seen = orchestrated
    real = by_node(seen["real"])

    customer = real[f"load:Lakehouse/{env.target.name}/{CUSTOMER}"]
    assert customer["rows"]["rows_inserted"] == 3
    assert env.store.exists(
        env.resolver.files_root(env.target).join("DWG", "Seed", "customers.csv")
    )


# --- the evidence it left behind ----------------------------------------------


def test_the_real_task_wrote_one_coherent_log_under_the_declared_folder(orchestrated):
    env, seen = orchestrated

    assert f"/{env.weaver.name}/Files/_/Log/task_date=" in seen["task_log"]
    written = seen["log"]
    assert "plan.json" in written
    assert sum("_refresh_" in name for name in written) == 1
    assert sum("_load_" in name for name in written) == 3
    assert sum("_complete_" in name for name in written) == 1
