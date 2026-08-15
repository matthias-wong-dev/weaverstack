"""Compiling a ``.sql`` validation into the module a build deploys.

A Spark SQL Test and a Spark SQL table compile to the same shape of file — the
authored header as the docstring, the authored SQL under ``SQL``, a Weaver base
supplying everything else — and differ in the base class and one word on the
marker line. These assert that the difference is exactly that, because a
validation acquiring a compilation path of its own is how the estate ends up
with two ideas of what a Test is.

Nothing here needs Spark: the module is text until someone runs it.
"""

from __future__ import annotations

import ast

import pytest

from weaver.declaration import read_source_document
from weaver.declaration.model import LAKEHOUSE
from weaver.declaration.spark_sql_module import GENERATED_MODULE_MARKER
from weaver.declaration.validation import (
    SPARK_VALIDATION_VERSION,
    generate_validation,
    has_generated_validation,
)
from weaver.spark import FabricSparkTarget

SALES = FabricSparkTarget(workspace="Demo", lakehouse="Sales_LH")

TEST_SOURCE = """/*
Test ID: Sales.OrdersReconcile

Description: Orders reconcile to the independent aggregation.

Primary key: OrderId
*/
create or replace temporary view expected_orders as
select OrderId, sum(Amount) as Amount from Sales.OrderLine group by OrderId;

select OrderId, Amount from expected_orders;

select OrderId, Amount from Sales.Order;
"""

ASSUMPTION_SOURCE = """/*
Assumption ID: Sales.OrdersHaveCustomers

Description: Every order carries a customer.
*/
select OrderId from Sales.Order where CustomerId is null;
"""


def _document(source: str, path: str):
    return read_source_document(path, source.encode("utf-8"), LAKEHOUSE)


def _module(source: str, path: str) -> str:
    return generate_validation(
        _document(source, path), destination=SALES
    ).payload.decode("utf-8")


@pytest.fixture
def test_module():
    return _module(TEST_SOURCE, "Lakehouse/Sales/tests/Sales.OrdersReconcile.sql")


@pytest.fixture
def assumption_module():
    return _module(
        ASSUMPTION_SOURCE,
        "Lakehouse/Sales/assumptions/Sales.OrdersHaveCustomers.sql",
    )


# --- what the module is ------------------------------------------------------


def test_the_module_is_importable_python(test_module, assumption_module):
    assert ast.parse(test_module)
    assert ast.parse(assumption_module)


def test_a_test_subclasses_the_generated_test_base(test_module):
    assert "from weaver import SparkSqlTest" in test_module
    assert "class Sales__OrdersReconcile(SparkSqlTest):" in test_module


def test_an_assumption_subclasses_the_generated_assumption_base(assumption_module):
    assert "from weaver import SparkSqlAssumption" in assumption_module
    assert "class Sales__OrdersHaveCustomers(SparkSqlAssumption):" in assumption_module


def test_the_marker_says_which_kind_was_generated(test_module, assumption_module):
    """The installer needs "generated"; a reader opening the file wants more."""

    assert test_module.splitlines()[0].startswith(f"{GENERATED_MODULE_MARKER} test —")
    assert assumption_module.splitlines()[0].startswith(
        f"{GENERATED_MODULE_MARKER} assumption —"
    )


def test_the_marker_still_identifies_a_generated_module(test_module):
    """What the load-file installer keys on to expand object tokens."""

    assert test_module.lstrip().startswith(GENERATED_MODULE_MARKER)


# --- what travels ------------------------------------------------------------


def test_the_authored_header_becomes_the_contract(test_module):
    """The docstring is what the primitive parses at run time."""

    document = ast.parse(test_module)
    docstring = ast.get_docstring(document, clean=False)

    assert "Test ID: Sales.OrdersReconcile" in docstring
    assert "Primary key: OrderId" in docstring


def test_the_authored_program_travels_whole(test_module):
    namespace: dict = {}
    exec(compile(ast.parse(test_module), "<module>", "exec"), namespace)  # noqa: S102

    assert "create or replace temporary view expected_orders" in namespace["SQL"]
    assert namespace["SQL"].count("select") == 3


def test_managed_references_name_the_lakehouse_they_read(test_module):
    """Addressed when the bundle is generated, like every other payload."""

    program = test_module.split('"""', 2)[2]

    assert "`Demo`.`Sales_LH`.`Sales`.`Order`" in program
    assert "`Demo`.`Sales_LH`.`Sales`.`OrderLine`" in program
    assert "from Sales.Order" not in program


# --- which declarations are compiled ----------------------------------------


def test_a_sql_validation_is_compiled(test_module):
    assert has_generated_validation(
        _document(TEST_SOURCE, "Lakehouse/Sales/tests/Sales.OrdersReconcile.sql")
    )


def test_a_python_validation_is_deployed_verbatim():
    """The module a developer wrote is already the primitive."""

    source = '''"""
Test ID: Sales.OrdersReconcile

Description: Orders reconcile.
"""
from weaver import Test


class Sales__OrdersReconcile(Test):
    def expected(self):
        return None

    def actual(self):
        return None
'''
    document = read_source_document(
        "Lakehouse/Sales/tests/Sales__OrdersReconcile.py",
        source.encode("utf-8"),
        LAKEHOUSE,
    )

    assert not has_generated_validation(document)


def test_the_generator_version_salts_the_artefact(test_module):
    generated = generate_validation(
        _document(TEST_SOURCE, "Lakehouse/Sales/tests/Sales.OrdersReconcile.sql"),
        destination=SALES,
    )

    assert generated.template_version == SPARK_VALIDATION_VERSION
    assert generated.object_type == "file"
    assert generated.extension == ".py"
