"""Dispatch addresses derived from the installed catalogue."""

from __future__ import annotations

import pytest
from factories import installed_catalogue, load_estate, load_estate_bindings
from support.weaver_test import weaver_test

from weaver.run import Runner, RunRequest, RunState
from weaver.targets import PhysicalTargetRef

RAW = PhysicalTargetRef("lakehouse", "Raw_LH")
REPORTING = PhysicalTargetRef("warehouse", "Reporting_WH")

ORDER = "load:Lakehouse/Raw_LH/Sales.Order"
DAILY = "load:Lakehouse/Raw_LH/Sales.Daily"
EXPORT = "load:Lakehouse/Raw_LH/Sales.Export"
REFRESH = "refresh:Lakehouse/Raw_LH"
SUMMARY = "load:Warehouse/Reporting_WH/Sales.Summary"


@pytest.fixture
def catalogue(tmp_path):
    repository = load_estate(tmp_path)
    return installed_catalogue(repository, load_estate_bindings())


def _resolved(catalogue, *, can_refresh=True):
    runner = Runner(
        RunState(catalogue=catalogue),
        RunRequest.load((RAW, REPORTING)),
        can_refresh=can_refresh,
    )
    return {node.node_id: runner.resolve(node) for node in runner.graph.order()}


@weaver_test()
def test_warehouse_procedure_address_comes_from_the_catalogue_graph(catalogue):
    resolved = _resolved(catalogue)[SUMMARY]

    assert resolved.dispatch_location == (
        "Warehouse/Reporting_WH/[_].[Load Sales.Summary]"
    )
    assert resolved.valid


@weaver_test()
def test_python_table_address_and_class_come_from_the_catalogue_graph(catalogue):
    resolved = _resolved(catalogue)[ORDER]

    assert resolved.dispatch_location == "Lakehouse/Raw_LH/_/Load/Sales__Order.py"
    assert resolved.expected_class == "Sales__Order"
    assert resolved.valid


@weaver_test()
def test_compiled_sql_table_uses_its_deployed_python_address(catalogue):
    resolved = _resolved(catalogue)[DAILY]

    assert resolved.dispatch_location == "Lakehouse/Raw_LH/_/Load/Sales__Daily.py"
    assert resolved.expected_class == "Sales__Daily"


@weaver_test()
def test_python_folder_address_comes_from_the_catalogue_graph(catalogue):
    resolved = _resolved(catalogue)[EXPORT]

    assert resolved.dispatch_location == (
        "Lakehouse/Raw_LH/_/Load/Files/Sales__Export.py"
    )
    assert resolved.expected_class == "Sales__Export"


@weaver_test()
def test_endpoint_refresh_address_requires_no_inventory(catalogue):
    resolved = _resolved(catalogue)[REFRESH]

    assert resolved.dispatch_location == "Lakehouse/Raw_LH/sql_endpoint"
    assert not resolved.unsupported


@weaver_test()
def test_endpoint_refresh_is_skipped_when_the_host_cannot_do_it(catalogue):
    resolved = _resolved(catalogue, can_refresh=False)[REFRESH]

    assert resolved.unsupported
    assert resolved.valid
    assert [message.message for message in resolved.messages] == [
        "SQL endpoint refresh is unsupported in this environment; "
        f"{REFRESH} will be skipped"
    ]


@weaver_test()
def test_resolution_exposes_no_physical_presence_claims(catalogue):
    resolved = _resolved(catalogue)[ORDER]

    assert not hasattr(resolved, "target_present")
    assert not hasattr(resolved, "primitive_present")
