"""Validation through a real build, into a real catalogue and back out.

The pure-Python tests prove the projection. This proves the round trip: an item
declaring a Test and an Assumption builds, its logical declarations land in
`_.TestDictionary`, its dependencies land in `_.Dependency` under the *logical*
validation ID, and nothing is certified in `_.Registry` under a validation ID
because nothing is materialised there.
"""

from __future__ import annotations

import pytest

from weaver.build_bundle import (
    InstallationEnvironment,
    ItemBinding,
    ItemBindings,
    LakehouseBinding,
    build_uploaded_item_repository,
    effective_item_bindings,
)
from weaver.catalogue.tables import DEPENDENCY, REGISTRY, TEST_DICTIONARY
from weaver.declaration.model import WeaverItemId
from weaver.locations import Location
from weaver.spark import SparkCatalogue

pytestmark = pytest.mark.spark

ITEM = "Lakehouse/Sales"

SCHEMA = "Schema ID: Sales\nDescription: Sales objects.\n"

ORDER = '''\
"""
Table ID: Sales.Order

Description: One row per order.

Lineage: A source system.

Primary key: Id

Schema:
  Id: string
"""
from weaver import Table


class Sales__Order(Table):
    def read(self):
        return [], []
'''

RECONCILE = '''\
"""
Test ID: Sales.OrdersReconcile

Description: Orders reconcile to the independently derived expected relation.

Primary key: Id
"""
from Sales__Order import Sales__Order

from weaver import Test


class Sales__OrdersReconcile(Test):
    def expected(self):
        return self.spark.createDataFrame([], "Id string")

    def actual(self):
        return Sales__Order(self).dataframe()
'''

HAVE_CUSTOMERS = '''\
"""
Assumption ID: Sales.OrdersHaveCustomers

Description: Every order carries a customer.
"""
from Sales__Order import Sales__Order

from weaver import Assumption


class Sales__OrdersHaveCustomers(Assumption):
    def read(self):
        return Sales__Order(self).dataframe().where("Id is null")
'''


def _failures(report) -> str:
    return "\n".join(
        f"{action.action_id}: {action.error_type}: {action.error_message}"
        for sequence in report.sequences
        for action in sequence.actions
        if action.status == "failed"
    )


def _write(root, relative: str, text: str) -> None:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _build(root, lakehouses, spark):
    selected = ItemBindings(
        (ItemBinding(WeaverItemId.parse(ITEM), LakehouseBinding(lakehouses.target)),)
    )
    result = build_uploaded_item_repository(
        Location(str(root)),
        bindings=effective_item_bindings(
            selected, weaver_lakehouse=lakehouses.weaver.name
        ),
        environment=InstallationEnvironment(
            store=lakehouses.store,
            resolver=lakehouses.resolver,
            spark=spark,
            workspace=lakehouses.workspace,
        ),
        control_lakehouse=LakehouseBinding(lakehouses.weaver),
    )
    assert result.report.status == "succeeded", _failures(result.report)
    return result


def _estate(tmp_path):
    root = tmp_path / "Estate"
    _write(root, "Lakehouse/Sales/schemas/Sales.yml", SCHEMA)
    _write(root, "Lakehouse/Sales/Sales__Order.py", ORDER)
    _write(root, "Lakehouse/Sales/tests/Sales__OrdersReconcile.py", RECONCILE)
    _write(root, "Lakehouse/Sales/assumptions/Sales__OrdersHaveCustomers.py", HAVE_CUSTOMERS)
    return root


@pytest.fixture
def built(tmp_path, lakehouses, spark, weaver_catalogue):
    """One item with a table, a Test and an Assumption, built for real."""

    target = SparkCatalogue(
        spark, lakehouses.resolver.spark_destination(lakehouses.target)
    )
    try:
        _build(_estate(tmp_path), lakehouses, spark)
        yield weaver_catalogue
    finally:
        spark.sql(f"DROP SCHEMA IF EXISTS {target.qualified_schema('Sales')} CASCADE")


def _rows(catalogue, table, columns: str):
    frame = catalogue.spark.table(catalogue.qualify("_", table.name)).where(
        "item_type = 'Lakehouse' AND item_name = 'Sales'"
    )
    return [tuple(row) for row in frame.selectExpr(*columns.split(", ")).collect()]


def test_the_declarations_land_in_the_test_dictionary(built):
    rows = sorted(_rows(built, TEST_DICTIONARY, "object_name, test_type, primary_key"))

    assert rows == [
        ("OrdersHaveCustomers", "assumption", None),
        ("OrdersReconcile", "test", "Id"),
    ]


def test_the_description_is_recorded(built):
    rows = dict(_rows(built, TEST_DICTIONARY, "object_name, description"))

    assert rows["OrdersHaveCustomers"] == "Every order carries a customer."


def test_nothing_is_certified_under_a_validation_id(built):
    certified = {name for (name,) in _rows(built, REGISTRY, "object_name")}

    assert "OrdersReconcile" not in certified
    assert "OrdersHaveCustomers" not in certified
    assert "Order" in certified


def test_the_dependency_belongs_to_the_logical_validation(built):
    edges = set(_rows(built, DEPENDENCY, "object_name, dependency_name"))

    assert ("OrdersReconcile", "Sales__Order") in edges
    assert ("OrdersHaveCustomers", "Sales__Order") in edges


def test_the_installed_roles_are_read_back_as_they_were_written(built, lakehouses):
    """Registry roles survive the round trip through a real Delta table."""

    from weaver.catalogue.state import read_catalogue_state

    catalogue = read_catalogue_state(built, [WeaverItemId.parse(ITEM)])
    roles = {
        identity.object_id.object: document.object_role
        for identity, document in catalogue.registered.items()
    }

    assert roles["Order"] == "data"
    assert all(role in {"data", "load"} for role in roles.values())


def test_a_deleted_test_loses_its_dictionary_row(tmp_path, lakehouses, spark, weaver_catalogue):
    """A validation that stops being declared stops being described."""

    target = SparkCatalogue(
        spark, lakehouses.resolver.spark_destination(lakehouses.target)
    )
    try:
        root = _estate(tmp_path)
        _build(root, lakehouses, spark)
        assert {name for (name,) in _rows(weaver_catalogue, TEST_DICTIONARY, "object_name")} == {
            "OrdersReconcile",
            "OrdersHaveCustomers",
        }

        (root / "Lakehouse/Sales/tests/Sales__OrdersReconcile.py").unlink()
        _build(root, lakehouses, spark)

        assert {name for (name,) in _rows(weaver_catalogue, TEST_DICTIONARY, "object_name")} == {
            "OrdersHaveCustomers"
        }
        assert ("OrdersReconcile", "Sales__Order") not in set(
            _rows(weaver_catalogue, DEPENDENCY, "object_name, dependency_name")
        )
    finally:
        spark.sql(f"DROP SCHEMA IF EXISTS {target.qualified_schema('Sales')} CASCADE")
