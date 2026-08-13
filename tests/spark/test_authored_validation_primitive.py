"""An authored Test and Assumption, run the way a notebook runs one.

These are *primitive* tests: each imports a deployed module, constructs the
class with a session and calls ``read()``. No repository is parsed, no catalogue
is opened, no orchestrator runs and no task log is written — which is the claim
of §18 of the design, not merely how the test is arranged.

The modules are written to disk and imported for the reason the load primitives
are: a class declared in this file would read *this* module's docstring as its
contract, and a validation carrying its own metadata is the property under test.
"""

from __future__ import annotations

import sys
import textwrap

import pytest

from weaver import lakehouse_for
from weaver.errors import LoadError, ValidationError
from weaver.targets import DeltaTarget, ItemRef

pytestmark = pytest.mark.spark

TARGET = "Sales_LH"

ORDERS_MODULE = '''\
"""
Table ID: Sales.Orders

Description: One row per order.

Lineage: The sales system.

Primary key: OrderId

Schema:
  OrderId: int
  Amount: int
"""
from weaver import Table


class Sales__Orders(Table):
    def read(self):
        return self.dataframe(), None
'''

RECONCILE_MODULE = '''\
"""
Test ID: Sales.OrdersReconcile

Description: Orders reconcile to the independently derived expected relation.

Primary key: OrderId
"""
from Sales__Orders import Sales__Orders

from weaver import Test


class Sales__OrdersReconcile(Test):
    expected_rows = []

    def expected(self):
        return self.spark.createDataFrame(self.expected_rows, "OrderId int, Amount int")

    def actual(self):
        return Sales__Orders(self).dataframe()
'''

UNKEYED_MODULE = '''\
"""
Test ID: Sales.OrdersPresent

Description: Orders match the expected relation, row for row.
"""
from Sales__Orders import Sales__Orders

from weaver import Test


class Sales__OrdersPresent(Test):
    expected_rows = []

    def expected(self):
        return self.spark.createDataFrame(self.expected_rows, "OrderId int, Amount int")

    def actual(self):
        return Sales__Orders(self).dataframe()
'''

UP_TO_DATE_MODULE = '''\
"""
Assumption ID: Sales.OrdersPositive

Description: Every order carries a positive amount.
"""
from Sales__Orders import Sales__Orders

from weaver import Assumption


class Sales__OrdersPositive(Assumption):
    def read(self):
        return Sales__Orders(self).dataframe().where("Amount <= 0")
'''

MODULES = {
    "Sales__Orders": ORDERS_MODULE,
    "Sales__OrdersReconcile": RECONCILE_MODULE,
    "Sales__OrdersPresent": UNKEYED_MODULE,
    "Sales__OrdersPositive": UP_TO_DATE_MODULE,
}


@pytest.fixture
def deployed(tmp_path, monkeypatch):
    """The modules where a deployed runtime tree would put them.

    Validation modules sit under the same runtime root as the objects they read,
    which is what lets ``from Sales__Orders import Sales__Orders`` resolve.
    """

    root = tmp_path / "deployed"
    root.mkdir()
    for name, source in MODULES.items():
        (root / f"{name}.py").write_text(source, encoding="utf-8")
    monkeypatch.syspath_prepend(str(root))
    for name in MODULES:
        sys.modules.pop(name, None)
    yield root
    for name in MODULES:
        sys.modules.pop(name, None)


@pytest.fixture
def lakehouse(lakehouses):
    return lakehouse_for(lakehouses.resolver, ItemRef(TARGET))


@pytest.fixture
def orders(spark, lakehouses):
    """Real Delta rows where the resolver says Sales.Orders lives."""

    def write(rows):
        path = lakehouses.resolver.delta_table(
            DeltaTarget.parse(TARGET), "Sales", "Orders"
        ).value
        spark.createDataFrame(rows, "OrderId int, Amount int").write.format(
            "delta"
        ).mode("overwrite").save(path)

    return write


def _class(name: str):
    import importlib

    return getattr(importlib.import_module(name), name)


# --- Test -------------------------------------------------------------------


def test_a_passing_test_returns_no_rows(spark, deployed, lakehouse, registered_orders):
    registered_orders([(1, 100), (2, 200)])
    reconcile = _class("Sales__OrdersReconcile")
    reconcile.expected_rows = [(1, 100), (2, 200)]

    failures = reconcile(spark, lakehouse=lakehouse).read()

    assert failures.count() == 0


def test_a_failing_test_returns_both_sides(spark, deployed, lakehouse, registered_orders):
    registered_orders([(1, 110), (3, 300)])
    reconcile = _class("Sales__OrdersReconcile")
    reconcile.expected_rows = [(1, 100), (2, 200)]

    failures = reconcile(spark, lakehouse=lakehouse).read()

    rows = sorted(
        (row["_weaver_side"], row["OrderId"], row["Amount"]) for row in failures.collect()
    )
    assert rows == [
        ("actual", 1, 110),
        ("actual", 3, 300),
        ("expected", 1, 100),
        ("expected", 2, 200),
    ]


def test_the_declared_key_pairs_the_two_sides(spark, deployed, lakehouse, registered_orders):
    registered_orders([(1, 110)])
    reconcile = _class("Sales__OrdersReconcile")
    reconcile.expected_rows = [(1, 100)]

    failures = reconcile(spark, lakehouse=lakehouse).read()

    keys = {row["_weaver_sk"] for row in failures.collect()}
    assert failures.count() == 2 and len(keys) == 1


def test_a_test_without_a_key_pairs_nothing(spark, deployed, lakehouse, registered_orders):
    registered_orders([(1, 110)])
    present = _class("Sales__OrdersPresent")
    present.expected_rows = [(1, 100)]

    failures = present(spark, lakehouse=lakehouse).read()

    keys = [row["_weaver_sk"] for row in failures.collect()]
    assert len(keys) == 2 and len(set(keys)) == 2


def test_a_dependency_is_constructed_from_the_test(spark, deployed, lakehouse, orders):
    """``Sales__Orders(self)`` inherits the session and the resolved Lakehouse."""

    orders([(1, 100)])
    reconcile = _class("Sales__OrdersReconcile")(spark, lakehouse=lakehouse)

    from Sales__Orders import Sales__Orders  # noqa: PLC0415 - the authored import

    dependency = Sales__Orders(reconcile)
    assert dependency.spark is reconcile.spark
    assert dependency.lakehouse == reconcile.lakehouse


def test_a_broken_test_contract_raises_rather_than_passing(
    spark, deployed, lakehouse, orders
):
    """A Test nobody could run must not report as a Test that found nothing."""

    orders([(1, 100), (1, 200)])
    reconcile = _class("Sales__OrdersReconcile")
    reconcile.expected_rows = [(1, 100)]

    with pytest.raises(ValidationError, match="repeats"):
        reconcile(spark, lakehouse=lakehouse).read()


def test_a_test_may_not_author_its_own_read():
    with pytest.raises(LoadError, match="which a Test may not"):
        from weaver import Test

        class Sales__Sneaky(Test):
            def read(self):
                return None


# --- Assumption -------------------------------------------------------------


def test_an_assumption_holding_returns_no_rows(spark, deployed, lakehouse, registered_orders):
    registered_orders([(1, 100), (2, 200)])

    violations = _class("Sales__OrdersPositive")(spark, lakehouse=lakehouse).read()

    assert violations.count() == 0


def test_an_assumption_returns_its_violations(spark, deployed, lakehouse, registered_orders):
    registered_orders([(1, 100), (2, -5), (3, 0)])

    violations = _class("Sales__OrdersPositive")(spark, lakehouse=lakehouse).read()

    assert sorted(row["OrderId"] for row in violations.collect()) == [2, 3]
    assert "_weaver_sk" not in violations.columns


def test_an_unimplemented_assumption_says_what_to_write(spark, lakehouse):
    from weaver import Assumption

    class Sales__Unwritten(Assumption):
        pass

    with pytest.raises(NotImplementedError, match="must implement read"):
        Sales__Unwritten(spark, lakehouse=lakehouse).read()


def test_an_unimplemented_test_says_what_to_write(spark, lakehouse):
    from weaver import Test

    class Sales__Unwritten(Test):
        pass

    with pytest.raises(NotImplementedError, match="must implement expected"):
        Sales__Unwritten(spark, lakehouse=lakehouse).read()


def test_the_module_docstring_is_the_contract(spark, deployed, lakehouse, orders):
    """The declared key comes from the module, not from an argument."""

    orders([(1, 110)])
    reconcile = _class("Sales__OrdersReconcile")
    reconcile.expected_rows = [(1, 100)]

    document = reconcile(spark, lakehouse=lakehouse)._document()

    assert document.kind == "Test"
    assert document.primary_key == ("OrderId",)
    assert textwrap.dedent(RECONCILE_MODULE).startswith('"""')


# --- compiled from Spark SQL ------------------------------------------------
#
# The same claims as the authored classes above, made against modules Weaver
# generated. What matters is that the compiled form reaches the *same*
# comparison: a Test authored in Python and a Test authored in SQL must agree
# about what passing means, or "the Sales tests pass" says nothing.

SQL_TEST_SOURCE = """/*
Test ID: Sales.OrdersMatch

Description: Orders match the independently derived expected relation.

Primary key: OrderId
*/
create or replace temporary view expected_orders as
select * from values (1, 100), (2, 200) as t(OrderId, Amount);

select OrderId, Amount from expected_orders;

select OrderId, Amount from Sales.Orders;
"""

SQL_ASSUMPTION_SOURCE = """/*
Assumption ID: Sales.OrdersArePositive

Description: Every order carries a positive amount.
*/
select OrderId, Amount from Sales.Orders where Amount <= 0;
"""


@pytest.fixture
def registered_orders(spark, lakehouses, lakehouse, orders):
    """Sales.Orders in the metastore, which is how SQL addresses it.

    A Python validation reaches its dependency through the object module it
    imports, which addresses by path; a SQL one names the table, so it has to be
    registered. Both are real arrangements — this is the one a Spark SQL program
    meets, and it is what a build leaves behind.
    """

    schema = lakehouse.destination.qualified_schema("Sales")
    spark.sql(
        f"CREATE SCHEMA IF NOT EXISTS {schema} "
        f"LOCATION '{lakehouse.destination.schema_location('Sales')}'"
    )
    path = lakehouses.resolver.delta_table(
        DeltaTarget.parse(TARGET), "Sales", "Orders"
    ).value

    def write(rows):
        orders(rows)
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {lakehouse.qualify('Sales', 'Orders')} "
            f"USING delta LOCATION '{path}'"
        )

    try:
        yield write
    finally:
        spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")


@pytest.fixture
def compiled(tmp_path, monkeypatch, lakehouses):
    """The generated modules, written and addressed as an install would."""

    from weaver.declaration import read_source_document
    from weaver.declaration.model import LAKEHOUSE
    from weaver.declaration.validation import generate_validation
    from weaver.spark import tokens

    root = tmp_path / "compiled"
    root.mkdir()
    destination = lakehouses.resolver.spark_destination(lakehouses.target)
    sources = {
        "Lakehouse/Sales/tests/Sales.OrdersMatch.sql": SQL_TEST_SOURCE,
        "Lakehouse/Sales/assumptions/Sales.OrdersArePositive.sql": SQL_ASSUMPTION_SOURCE,
    }
    names = []
    for path, source in sources.items():
        document = read_source_document(path, source.encode("utf-8"), LAKEHOUSE)
        payload = generate_validation(document).payload.decode("utf-8")
        # The installer resolves object tokens as it writes the file down.
        name = document.object_id.qualified.replace(".", "__")
        (root / f"{name}.py").write_text(
            tokens.expand(payload, destination), encoding="utf-8"
        )
        names.append(name)

    monkeypatch.syspath_prepend(str(root))
    for name in names:
        sys.modules.pop(name, None)
    yield root
    for name in names:
        sys.modules.pop(name, None)


def test_a_compiled_test_passes_against_matching_data(
    spark, compiled, lakehouse, registered_orders
):
    registered_orders([(1, 100), (2, 200)])

    failures = _class("Sales__OrdersMatch")(spark, lakehouse=lakehouse).read()

    assert failures.count() == 0


def test_a_compiled_test_reaches_the_common_comparison(
    spark, compiled, lakehouse, registered_orders
):
    """Same diagnostic columns, same pairing, same counting as an authored one."""

    registered_orders([(1, 110), (3, 300)])

    failures = _class("Sales__OrdersMatch")(spark, lakehouse=lakehouse).read()

    assert failures.columns == ["_weaver_side", "_weaver_sk", "OrderId", "Amount"]
    collected = failures.collect()
    assert sorted(
        (row["_weaver_side"], row["OrderId"], row["Amount"]) for row in collected
    ) == [
        ("actual", 1, 110),
        ("actual", 3, 300),
        ("expected", 1, 100),
        ("expected", 2, 200),
    ]
    assert len({row["_weaver_sk"] for row in collected if row["OrderId"] == 1}) == 1


def test_a_compiled_test_produces_both_sides_from_one_pass(
    spark, compiled, lakehouse, registered_orders
):
    """One program, one execution — not one per side."""

    registered_orders([(1, 100), (2, 200)])
    test = _class("Sales__OrdersMatch")(spark, lakehouse=lakehouse)

    expected, actual = test._sides()

    assert sorted(row["OrderId"] for row in expected.collect()) == [1, 2]
    assert sorted(row["OrderId"] for row in actual.collect()) == [1, 2]


def test_a_compiled_test_reads_its_key_from_its_own_docstring(
    spark, compiled, lakehouse
):
    document = _class("Sales__OrdersMatch")(spark, lakehouse=lakehouse)._document()

    assert document.kind == "Test"
    assert document.primary_key == ("OrderId",)


def test_a_compiled_assumption_returns_its_violations(
    spark, compiled, lakehouse, registered_orders
):
    registered_orders([(1, 100), (2, -5), (3, 0)])

    violations = _class("Sales__OrdersArePositive")(spark, lakehouse=lakehouse).read()

    assert sorted(row["OrderId"] for row in violations.collect()) == [2, 3]


def test_a_compiled_assumption_holding_returns_no_rows(
    spark, compiled, lakehouse, registered_orders
):
    registered_orders([(1, 100)])

    violations = _class("Sales__OrdersArePositive")(spark, lakehouse=lakehouse).read()

    assert violations.count() == 0


def test_a_compiled_test_still_refuses_to_author_read():
    from weaver import SparkSqlTest

    with pytest.raises(LoadError, match="which a Test may not"):

        class Sales__Sneaky(SparkSqlTest):
            def read(self):
                return None


def test_a_compiled_validation_with_no_program_says_so(spark, lakehouse):
    from weaver import SparkSqlAssumption

    class Sales__Empty(SparkSqlAssumption):
        pass

    with pytest.raises(ValidationError, match="carries no program"):
        Sales__Empty(spark, lakehouse=lakehouse).read()
