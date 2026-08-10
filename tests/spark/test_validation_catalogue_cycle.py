"""Validation through a real build, into a real catalogue and back out.

The pure-Python tests prove the projection. This proves the round trip: an item
declaring a Test and an Assumption builds, its logical declarations land in
`_.TestDictionary`, its dependencies land in `_.Dependency` under the *logical*
validation ID, and nothing is certified in `_.Registry` under a validation ID
because nothing is materialised there.
"""

from __future__ import annotations

import pytest
from support.sessions import given_session

from weaver.build_bundle import (
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
        session=given_session(
                workspace=lakehouses.workspace,
                store=lakehouses.store,
                resolver=lakehouses.resolver,
                spark=spark,
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


@pytest.fixture(scope="module")
def built(tmp_path_factory, shared_lakehouses, spark, shared_weaver_catalogue):
    """One item with a table, a Test and an Assumption, built for real.

    Built once: the claims that read this only read it. The three below that
    change what is declared and build again keep an estate of their own, and
    can, because the shared pair of Lakehouses is named apart from the per-test
    pair.
    """

    lakehouses = shared_lakehouses
    target = SparkCatalogue(
        spark, lakehouses.resolver.spark_destination(lakehouses.target)
    )
    try:
        _build(_estate(tmp_path_factory.mktemp("declared")), lakehouses, spark)
        yield shared_weaver_catalogue
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
    assert roles["Sales__OrdersReconcile.py"] == "test"
    assert roles["Sales__OrdersHaveCustomers.py"] == "assumption"
    assert roles["Sales__Order.py"] == "load"


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


# --- the artefacts a build installs -------------------------------------------


def test_the_runtime_artefacts_are_certified_with_their_roles(built):
    """The physical primitives, under names of their own and roles of their own."""

    rows = {
        (name, role)
        for name, role in _rows(built, REGISTRY, "object_name, object_role")
    }

    assert ("Sales__OrdersReconcile.py", "test") in rows
    assert ("Sales__OrdersHaveCustomers.py", "assumption") in rows
    assert ("Sales__Order.py", "load") in rows
    assert ("Order", "data") in rows


def test_a_validation_module_lands_under_the_runtime_root(built):
    """Beneath the import root, so its dependency imports still resolve."""

    schemas = {
        schema
        for (schema,) in _rows(built, REGISTRY, "schema_name")
        if schema.startswith("_/Load")
    }

    assert "_/Load/tests" in schemas
    assert "_/Load/assumptions" in schemas


def test_the_module_is_actually_written_where_it_was_certified(built, shared_lakehouses):
    lakehouses = shared_lakehouses
    root = lakehouses.resolver.files_root(lakehouses.target)
    module = root / "_" / "Load" / "tests" / "Sales__OrdersReconcile.py"

    assert lakehouses.store.exists(module)
    assert b"class Sales__OrdersReconcile(Test)" in lakehouses.store.read(module)


def test_a_deleted_validation_is_pruned_from_the_estate(
    tmp_path, lakehouses, spark, weaver_catalogue
):
    """The ordinary prune, because it is the ordinary lifecycle."""

    target = SparkCatalogue(
        spark, lakehouses.resolver.spark_destination(lakehouses.target)
    )
    root = lakehouses.resolver.files_root(lakehouses.target)
    module = root / "_" / "Load" / "tests" / "Sales__OrdersReconcile.py"
    try:
        estate = _estate(tmp_path)
        _build(estate, lakehouses, spark)
        assert lakehouses.store.exists(module)

        (estate / "Lakehouse/Sales/tests/Sales__OrdersReconcile.py").unlink()
        _build(estate, lakehouses, spark)

        assert not lakehouses.store.exists(module)
        assert "Sales__OrdersReconcile.py" not in {
            name for (name,) in _rows(weaver_catalogue, REGISTRY, "object_name")
        }
    finally:
        spark.sql(f"DROP SCHEMA IF EXISTS {target.qualified_schema('Sales')} CASCADE")


def test_an_unchanged_validation_is_not_reinstalled(
    tmp_path, lakehouses, spark, weaver_catalogue
):
    """Incremental selection reads the signature, and it did not change."""

    target = SparkCatalogue(
        spark, lakehouses.resolver.spark_destination(lakehouses.target)
    )
    try:
        estate = _estate(tmp_path)
        _build(estate, lakehouses, spark)
        second = _build(estate, lakehouses, spark)

        installed = [
            action.action_id
            for sequence in second.report.sequences
            for action in sequence.actions
            if "OrdersReconcile" in action.action_id
        ]

        assert installed == []
    finally:
        spark.sql(f"DROP SCHEMA IF EXISTS {target.qualified_schema('Sales')} CASCADE")
