"""The load orchestration composition, over a real installed estate.

Small on purpose. Its job is to prove that the seams *compose* — catalogue read,
reverse Registry binding, dependency resolution, primitive resolution, dry-run
validation, sequential dispatch, result normalisation and real task logging —
and it is narrow enough that a failure names the orchestration layer rather than
an unrelated lifecycle transition. It is not the first sight of any defect in a
primitive, a DAG edge case, fault-tolerant propagation or a task-log field
contract; those are all owned below it.

Three objects and one chain:

.. code-block:: text

    Sales.Seed (folder)  →  Sales.Customer (python)  →  Sales.Named (spark sql)

which is the fewest that can carry three dispatch kinds and a real dependency
order. The alias crossing, the endpoint-refresh barrier and the Warehouse
procedure need a Fabric Lakehouse and Warehouse, so they are proven in
``tests/fabric/test_load_orchestration_lifecycle.py`` and, at the pure layer, in
``tests/targeted/test_load_dag_binding.py``.

The public ``weaver.load(...)`` is not called here, and only for a harness
reason: it opens and stops its own Spark session, and this suite shares one. The
composition beneath it — :func:`weaver.load.run_load` over a
:class:`weaver.load.LoadSession` — is entered at the same point, with the
session's Spark and store supplied rather than acquired.
"""

from __future__ import annotations

import json

import pytest
from support.build_envs import LOAD_ORCHESTRATION_FIXTURE

from weaver.catalogue.builtin import LOG_FOLDER
from weaver.load import LoadSession, run_load
from weaver.load_plan import PYTHON_FOLDER, PYTHON_TABLE, SPARK_SQL_FILE, PhysicalTargetRef
from weaver.load_report import TASK_SUCCEEDED, VALIDATED
from weaver.store import FilesystemStore
from weaver.targets import FolderTarget, ItemRef
from weaver.task_logging import COMPLETE_STEP, PLAN_FILE

pytestmark = [
    pytest.mark.spark,
    pytest.mark.parametrize(
        "weaver_repo_fixture", [LOAD_ORCHESTRATION_FIXTURE], indirect=True
    ),
]

TARGET = PhysicalTargetRef("lakehouse", "Sales_LH")

SEED = "load:Lakehouse/Sales_LH/Sales.Seed"
CUSTOMER = "load:Lakehouse/Sales_LH/Sales.Customer"
NAMED = "load:Lakehouse/Sales_LH/Sales.Named"


@pytest.fixture(scope="module")
def estate(local_lakehouse_estate):
    """The installed estate, built once — building it is not the subject."""

    return local_lakehouse_estate


def session(estate) -> LoadSession:
    env = estate.env
    return LoadSession(
        env.workspace, (TARGET,), spark=env.generate_spark, store=env.store
    )


def log_root(estate):
    env = estate.env
    return env.resolver.folder_object(
        FolderTarget(lakehouse=env.weaver), "_", LOG_FOLDER
    )


def rows(estate, table: str) -> list[dict]:
    return [dict(row) for row in estate.env.query(f"SELECT * FROM {table}")]


# --- dry run ------------------------------------------------------------------


@pytest.fixture(scope="module")
def dry(estate):
    return run_load(session(estate), requested=(TARGET,), dry_run=True)


def test_a_complete_load_plan_locates_every_primitive_without_executing_it(dry, estate):
    """The whole path, stopping where a target would be touched."""

    env = estate.env
    files_root = env.resolver.files_root(env.target).value

    assert dry.order == (SEED, CUSTOMER, NAMED)
    assert dry.edges == ((CUSTOMER, NAMED), (SEED, CUSTOMER))
    assert {node.node_id: node.primitive_kind for node in dry.nodes} == {
        SEED: PYTHON_FOLDER,
        CUSTOMER: PYTHON_TABLE,
        NAMED: SPARK_SQL_FILE,
    }
    assert {node.node_id: node.dispatch_location for node in dry.nodes} == {
        SEED: f"{files_root}/_/Load/Files/Sales__Seed.py",
        CUSTOMER: f"{files_root}/_/Load/Sales__Customer.py",
        NAMED: f"{files_root}/_/Load/Sales.Named.sql",
    }
    assert all(node.status == VALIDATED for node in dry.nodes)
    assert all(not node.executed for node in dry.nodes)


def test_the_dry_run_changes_no_data_and_writes_no_task_log(dry, estate):
    """The folder exists — the build installed it. Nothing is *in* it."""

    store = estate.env.store
    folder = log_root(estate)

    assert dry.task_log is None
    assert store.exists(folder)
    assert store.list(folder) == []
    # A build creates structure; only a load puts rows in it, and none has run.
    assert rows(estate, "{{object:Sales.Customer}}") == []


# --- real execution -----------------------------------------------------------


@pytest.fixture(scope="module")
def real(estate, dry):
    """The same target scope, executed. Ordered after the dry run deliberately."""

    return run_load(session(estate), requested=(TARGET,), dry_run=False)


def test_the_executed_graph_is_the_one_the_dry_run_planned(real, dry):
    assert real.order == dry.order
    assert real.edges == dry.edges
    assert {node.node_id: node.dispatch_location for node in real.nodes} == {
        node.node_id: node.dispatch_location for node in dry.nodes
    }


def test_every_step_dispatched_through_its_intended_primitive_kind(real):
    assert [(node.node_id, node.primitive_kind) for node in real.nodes] == [
        (SEED, PYTHON_FOLDER),
        (CUSTOMER, PYTHON_TABLE),
        (NAMED, SPARK_SQL_FILE),
    ]
    assert all(node.executed for node in real.nodes)


def test_the_run_succeeded_with_nothing_blocked_or_skipped(real):
    assert real.status == TASK_SUCCEEDED
    assert {node.status for node in real.nodes} == {"succeeded"}


def test_each_completed_step_returned_a_normalised_result(real):
    for node in real.nodes:
        assert node.result is not None
        assert node.result.succeeded
        assert node.result.rows_rejected == 0


def test_the_upstream_load_ran_before_the_table_that_reads_it(real):
    """Ordering is the claim; the counts are what prove it actually held."""

    by_node = real.by_node
    assert by_node[CUSTOMER].result.rows_inserted == 3
    assert by_node[NAMED].result.rows_inserted == 3


def test_the_downstream_result_reflects_the_upstream_loaded_data(real, estate):
    customers = {row["Customer name"] for row in rows(estate, "{{object:Sales.Customer}}")}
    named = {row["Customer name"] for row in rows(estate, "{{object:Sales.Named}}")}

    assert customers == {"Ada", "Grace", "Katherine"}
    assert named == {"ADA", "GRACE", "KATHERINE"}


def test_the_folder_materialised_the_files_its_object_staged(real, estate):
    env = estate.env
    folder = env.resolver.folder_object(
        FolderTarget(lakehouse=env.target), "Sales", "Seed"
    )

    assert {entry.name for entry in env.store.list(folder)} >= {"customers.csv"}


# --- the task log -------------------------------------------------------------


def written(estate, real) -> dict[str, dict]:
    store = FilesystemStore()
    from weaver.locations import Location

    root = Location(real.task_log)
    return {
        entry.name: json.loads(store.read(entry.location).decode("utf-8"))
        for entry in store.list(root)
        if not entry.is_directory
    }


def test_the_real_task_creates_one_folder_under_the_declared_log_folder(real, estate):
    root = log_root(estate).value

    assert real.task_log.startswith(root + "/task_date=")
    assert real.task_id and real.task_id in real.task_log


def test_the_plan_records_the_same_graph_the_report_returned(real, estate):
    plan = written(estate, real)[PLAN_FILE]

    assert plan["order"] == list(real.order)
    assert plan["edges"] == [list(edge) for edge in real.edges]
    assert {node["node_id"]: node["dispatch_location"] for node in plan["nodes"]} == {
        node.node_id: node.dispatch_location for node in real.nodes
    }
    assert plan["mode"] == "execute"
    assert plan["weaver_version"]


def test_one_immutable_step_result_exists_per_executed_step(real, estate):
    files = written(estate, real)
    steps = {
        name: payload
        for name, payload in files.items()
        if "_load_" in name or "_refresh_" in name
    }

    assert len(steps) == len(real.nodes)
    assert {payload["node_id"] for payload in steps.values()} == {
        node.node_id for node in real.nodes
    }
    assert all(payload["executed"] for payload in steps.values())


def test_exactly_one_completion_file_reconciles_with_the_returned_report(real, estate):
    files = written(estate, real)
    completions = [
        payload for name, payload in files.items() if f"_{COMPLETE_STEP}_" in name
    ]

    (completion,) = completions
    assert completion["final_status"] == real.status
    assert completion["planned_steps"] == len(real.nodes)
    assert completion["executed_steps"] == len(real.nodes)
    assert completion["failed_steps"] == 0
    assert completion["blocked_steps"] == 0
    assert completion["rows"]["rows_inserted"] == sum(
        node.result.rows_inserted for node in real.nodes
    )
