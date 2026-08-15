"""Can the orchestrator locate what it would dispatch?

That is the whole question, and it is deliberately not *does the primitive
work*. Each of the four primitives is independently runnable and has its own
suite; what is claimed here is that orchestration can find the installed one and
says so plainly when it cannot.

Physical state arrives as a `TargetInventory` — the same object a build reads
before planning — so an estate missing a procedure, a file or a table is a
one-line edit rather than a wiped Warehouse.
"""

from __future__ import annotations

from dataclasses import replace

import pytest
from factories import (
    installed_catalogue,
    installed_inventories,
    load_estate,
    load_estate_bindings,
)

from weaver.load_plan import PhysicalTargetRef
from weaver.load_report import (
    BLOCKED,
    DISPATCH_LOCATION_MISSING,
    INVALID,
    TARGET_MISSING,
    VALIDATED,
)
from weaver.run import Runner, RunRequest, RunState

RAW = PhysicalTargetRef("lakehouse", "Raw_LH")
REPORTING = PhysicalTargetRef("warehouse", "Reporting_WH")

ORDER = "load:Lakehouse/Raw_LH/Sales.Order"
DAILY = "load:Lakehouse/Raw_LH/Sales.Daily"
EXPORT = "load:Lakehouse/Raw_LH/Sales.Export"
REFRESH = "refresh:Lakehouse/Raw_LH"
SUMMARY = "load:Warehouse/Reporting_WH/Sales.Summary"


class Resolutions:
    """Every node of the graph, resolved against one observed estate."""

    def __init__(self, runner) -> None:
        self.runner = runner
        self.by_id = {
            node.node_id: runner.resolve(node) for node in runner.graph.order()
        }

    def dry_run(self):
        return self.runner.run(dispatch=None)


@pytest.fixture
def estate(tmp_path):
    """The canonical estate, its catalogue and its physical inventories."""

    repository = load_estate(tmp_path)
    bindings = load_estate_bindings()
    return (
        installed_catalogue(repository, bindings),
        installed_inventories(repository, bindings),
    )


def _runner(estate, *, inventories=None, can_refresh=True, dry_run=False):
    catalogue, observed = estate
    return Runner(
        RunState(
            catalogue=catalogue,
            target_inventories=observed if inventories is None else inventories,
        ),
        RunRequest.load((RAW, REPORTING), dry_run=dry_run),
        can_refresh=can_refresh,
    )


def resolve(estate, *, inventories=None, can_refresh=True):
    """Resolution, asked of the snapshot and of nothing else."""

    return Resolutions(
        _runner(estate, inventories=inventories, can_refresh=can_refresh)
    )


def without(inventories, target: str, **fields):
    return {**inventories, target: replace(inventories[target], **fields)}


# --- what each kind resolves to ----------------------------------------------


def test_load_resolution_finds_a_warehouse_procedure(estate):
    resolved = resolve(estate).by_id[SUMMARY]

    assert resolved.dispatch_location == (
        "Warehouse/Reporting_WH/[_].[Load Sales.Summary]"
    )
    assert resolved.primitive_present
    assert resolved.valid


def test_load_resolution_finds_a_sql_authored_tables_deployed_module(estate):
    """Resolved exactly as a Python-authored table is, because it is one.

    A Spark SQL table is compiled into a ``SparkSqlTable`` module, so what
    resolution locates is a module and a class — and nothing downstream can tell
    which language the table was written in.
    """

    resolved = resolve(estate).by_id[DAILY]

    assert resolved.dispatch_location == "Lakehouse/Raw_LH/_/Load/Sales__Daily.py"
    assert resolved.primitive_present
    assert resolved.expected_class == "Sales__Daily"


def test_load_resolution_finds_a_python_table_module_and_class(estate):
    resolved = resolve(estate).by_id[ORDER]

    assert resolved.dispatch_location == "Lakehouse/Raw_LH/_/Load/Sales__Order.py"
    assert resolved.expected_class == "Sales__Order"


def test_load_resolution_finds_a_python_folder_module_and_class(estate):
    resolved = resolve(estate).by_id[EXPORT]

    assert resolved.dispatch_location == (
        "Lakehouse/Raw_LH/_/Load/Files/Sales__Export.py"
    )
    assert resolved.expected_class == "Sales__Export"


def test_load_resolution_resolves_an_endpoint_refresh_target(estate):
    """A host that has an endpoint. Whether it does is the host's answer, not
    something resolution discovers by reaching for one."""

    resolved = resolve(estate, can_refresh=True).by_id[REFRESH]

    assert resolved.dispatch_location == "Lakehouse/Raw_LH/sql_endpoint"
    assert resolved.primitive_present
    assert not resolved.unsupported


def test_load_resolution_reports_a_refresh_the_host_cannot_perform(estate):
    """An emulator has no endpoint. That is an absence, not a fault."""

    resolved = resolve(estate, can_refresh=False).by_id[REFRESH]

    assert resolved.unsupported
    assert resolved.valid, "a warning, not an error: nothing here is wrong"
    assert [message.message for message in resolved.messages] == [
        "SQL endpoint refresh is unsupported in this environment; "
        f"{REFRESH} will be skipped"
    ]


# --- what it reports when the estate is not there -----------------------------


def test_load_resolution_reports_a_missing_procedure(estate):
    catalogue, inventories = estate
    resolved = resolve(
        (catalogue, without(inventories, "Warehouse/Reporting_WH", procedures=())),
    ).by_id[SUMMARY]

    assert not resolved.primitive_present
    assert not resolved.valid
    assert [message.code for message in resolved.messages] == [
        DISPATCH_LOCATION_MISSING
    ]


def test_load_resolution_reports_a_missing_file(estate):
    catalogue, inventories = estate
    trimmed = without(
        inventories,
        "Lakehouse/Raw_LH",
        files=tuple(
            name
            for name in inventories["Lakehouse/Raw_LH"].files
            if not name.endswith("Sales__Daily.py")
        ),
    )
    resolved = resolve((catalogue, trimmed)).by_id[DAILY]

    assert not resolved.primitive_present
    assert [message.code for message in resolved.messages] == [
        DISPATCH_LOCATION_MISSING
    ]


def test_load_resolution_reports_a_missing_module(estate):
    catalogue, inventories = estate
    trimmed = without(
        inventories,
        "Lakehouse/Raw_LH",
        files=tuple(
            name
            for name in inventories["Lakehouse/Raw_LH"].files
            if not name.endswith("Sales__Order.py")
        ),
    )
    resolved = resolve((catalogue, trimmed)).by_id[ORDER]

    assert not resolved.primitive_present
    assert resolved.dispatch_location == "Lakehouse/Raw_LH/_/Load/Sales__Order.py"


def test_load_resolution_reports_a_missing_target_table(estate):
    catalogue, inventories = estate
    trimmed = without(inventories, "Warehouse/Reporting_WH", tables=())
    resolved = resolve((catalogue, trimmed)).by_id[SUMMARY]

    assert [message.code for message in resolved.messages] == [TARGET_MISSING]


def test_load_resolution_reports_a_missing_target(estate):
    catalogue, inventories = estate
    without_warehouse = {
        key: value
        for key, value in inventories.items()
        if key != "Warehouse/Reporting_WH"
    }
    resolved = resolve((catalogue, without_warehouse)).by_id[SUMMARY]

    assert not resolved.target_present
    assert [message.code for message in resolved.messages] == [TARGET_MISSING]


# --- and what that does to everything downstream ------------------------------


def test_load_resolution_blocks_descendants_of_invalid_nodes(estate):
    catalogue, inventories = estate
    trimmed = without(
        inventories,
        "Lakehouse/Raw_LH",
        files=tuple(
            name
            for name in inventories["Lakehouse/Raw_LH"].files
            if not name.endswith("Sales__Order.py")
        ),
    )
    result = _runner((catalogue, trimmed), dry_run=True).run()
    reports = result.by_node

    assert reports[ORDER].status == INVALID
    # Everything that reads what Sales.Order fills, directly or through the
    # barrier that publishes it.
    assert reports[DAILY].status == BLOCKED
    assert reports[REFRESH].status == BLOCKED
    assert reports[SUMMARY].status == BLOCKED
    # And nothing unrelated is caught up in it.
    assert reports[EXPORT].status == VALIDATED
    assert all(not report.executed for report in reports.values())
