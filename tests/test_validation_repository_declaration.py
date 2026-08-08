"""Validation as a repository declaration: where it lives and what it may be.

A Test and an Assumption are authored beneath the item that owns them, in
``tests/`` and ``assumptions/``, and are read by the same machinery that reads
objects. What this proves is that they are *not* objects: they carry the item's
ordinary ``Schema.Object`` identity, they resolve dependencies the ordinary way,
and they are held apart from the documents an item materialises.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from weaver.declaration import parse_item_repository
from weaver.declaration.model import WeaverDocumentId
from weaver.errors import DiscoveryError, MetadataError
from weaver.locations import Location


def _write(root: Path, relative: str, text: str) -> None:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _schema(name: str) -> str:
    return f"Schema ID: {name}\nDescription: {name} objects.\n"


def _table(object_id: str) -> str:
    class_name = object_id.replace(".", "__")
    return f'''\
"""
Table ID: {object_id}
Description: A declared table.
Lineage: A source system.
Primary key: Id
Schema:
  Id: string
"""
from weaver import Table

class {class_name}(Table):
    def read(self):
        return [], []
'''


def _python_test(object_id: str, *, reads: str = "Sales.Order") -> str:
    class_name = object_id.replace(".", "__")
    dependency = reads.replace(".", "__")
    return f'''\
"""
Test ID: {object_id}

Description: The materialised rows match the independent calculation.

Primary key: Id
"""
from {dependency} import {dependency}

from weaver import Test

raise RuntimeError("static discovery must never execute this module")

class {class_name}(Test):
    def expected(self):
        return self.spark.createDataFrame([], "Id string")

    def actual(self):
        return {dependency}(self).dataframe()
'''


def _python_assumption(object_id: str, *, reads: str = "Sales.Order") -> str:
    class_name = object_id.replace(".", "__")
    dependency = reads.replace(".", "__")
    return f'''\
"""
Assumption ID: {object_id}

Description: Every row carries a customer.
"""
from {dependency} import {dependency}

from weaver import Assumption

class {class_name}(Assumption):
    def read(self):
        return {dependency}(self).dataframe().where("Id is null")
'''


def _tsql_test(object_id: str) -> str:
    return f'''\
/*
Test ID: {object_id}

Description: The materialised summary matches the independent aggregation.

Primary key: Id
*/
select Id from Sales.Order;

select Id from Sales.Order;
'''


def _tsql_assumption(object_id: str) -> str:
    return f'''\
/*
Assumption ID: {object_id}

Description: Every order carries a customer.
*/
select Id from Sales.Order where CustomerId is null;
'''


@pytest.fixture
def lakehouse(tmp_path):
    """A Lakehouse item with one table, ready for validation to be added."""

    _write(tmp_path, "Lakehouse/Sales/schemas/Sales.yml", _schema("Sales"))
    _write(tmp_path, "Lakehouse/Sales/Sales__Order.py", _table("Sales.Order"))
    return tmp_path


@pytest.fixture
def warehouse(tmp_path):
    _write(tmp_path, "Warehouse/Reporting/schemas/Sales.yml", _schema("Sales"))
    _write(
        tmp_path,
        "Warehouse/Reporting/Sales.Order.sql",
        "/*\nTable ID: Sales.Order\nDescription: Orders.\nLineage: A source.\n*/\n"
        "select 1 as Id;\n",
    )
    return tmp_path


def parse(root: Path):
    return parse_item_repository(Location(str(root)))


def item(repository, name: str):
    return next(item for item in repository.items if item.identity.item_name == name)


# --- where validation lives -------------------------------------------------


def test_a_lakehouse_item_accepts_tests_and_assumptions(lakehouse):
    _write(
        lakehouse,
        "Lakehouse/Sales/tests/Sales__OrdersReconcile.py",
        _python_test("Sales.OrdersReconcile"),
    )
    _write(
        lakehouse,
        "Lakehouse/Sales/assumptions/Sales__OrdersHaveCustomers.py",
        _python_assumption("Sales.OrdersHaveCustomers"),
    )

    sales = item(parse(lakehouse), "Sales")

    assert [str(identity) for identity in sales.validations] == [
        "Lakehouse/Sales/Sales.OrdersHaveCustomers",
        "Lakehouse/Sales/Sales.OrdersReconcile",
    ]


def test_a_warehouse_item_accepts_sql_validation(warehouse):
    _write(
        warehouse,
        "Warehouse/Reporting/tests/Sales.OrderReconciliation.sql",
        _tsql_test("Sales.OrderReconciliation"),
    )
    _write(
        warehouse,
        "Warehouse/Reporting/assumptions/Sales.OrdersHaveCustomers.sql",
        _tsql_assumption("Sales.OrdersHaveCustomers"),
    )

    reporting = item(parse(warehouse), "Reporting")

    assert len(reporting.validations) == 2


def test_validation_is_not_among_the_documents_an_item_materialises(lakehouse):
    """The distinction the whole repository model turns on."""

    _write(
        lakehouse,
        "Lakehouse/Sales/tests/Sales__OrdersReconcile.py",
        _python_test("Sales.OrdersReconcile"),
    )

    sales = item(parse(lakehouse), "Sales")

    assert "Lakehouse/Sales/Sales.OrdersReconcile" not in [
        str(identity) for identity in sales.documents
    ]
    assert [str(identity) for identity in sales.validations] == [
        "Lakehouse/Sales/Sales.OrdersReconcile"
    ]
    assert set(sales.declarations) == set(sales.documents) | set(sales.validations)


def test_a_validation_may_not_sit_deeper(lakehouse):
    _write(
        lakehouse,
        "Lakehouse/Sales/tests/nested/Sales__OrdersReconcile.py",
        _python_test("Sales.OrdersReconcile"),
    )

    with pytest.raises(DiscoveryError, match="authored subdirectories"):
        parse(lakehouse)


def test_the_directory_names_the_kind(lakehouse):
    """An Assumption in tests/ is refused, and told where it belongs."""

    _write(
        lakehouse,
        "Lakehouse/Sales/tests/Sales__OrdersHaveCustomers.py",
        _python_assumption("Sales.OrdersHaveCustomers"),
    )

    with pytest.raises(DiscoveryError, match="Move it to assumptions/"):
        parse(lakehouse)


def test_a_test_in_assumptions_is_refused(lakehouse):
    _write(
        lakehouse,
        "Lakehouse/Sales/assumptions/Sales__OrdersReconcile.py",
        _python_test("Sales.OrdersReconcile"),
    )

    with pytest.raises(DiscoveryError, match="Move it to tests/"):
        parse(lakehouse)


def test_python_validation_belongs_to_a_lakehouse(warehouse):
    """It runs through Spark, so a Warehouse writes its validation in SQL."""

    _write(
        warehouse,
        "Warehouse/Reporting/tests/Sales__OrderReconciliation.py",
        _python_test("Sales.OrderReconciliation"),
    )

    with pytest.raises(DiscoveryError, match="belongs to a Lakehouse item"):
        parse(warehouse)


def test_a_validation_schema_must_be_declared_by_the_item(lakehouse):
    _write(
        lakehouse,
        "Lakehouse/Sales/tests/Finance__OrdersReconcile.py",
        _python_test("Finance.OrdersReconcile"),
    )

    with pytest.raises(DiscoveryError, match="'Finance' is not declared"):
        parse(lakehouse)


def test_the_filename_and_the_declared_id_must_agree(lakehouse):
    _write(
        lakehouse,
        "Lakehouse/Sales/tests/Sales__Elsewhere.py",
        _python_test("Sales.OrdersReconcile"),
    )

    with pytest.raises(DiscoveryError, match="they must agree"):
        parse(lakehouse)


# --- one namespace ----------------------------------------------------------


def test_a_test_may_not_take_an_objects_logical_id(lakehouse):
    """Both are the item's Schema.Object, so both cannot be Sales.Order."""

    _write(
        lakehouse,
        "Lakehouse/Sales/tests/Sales__Order.py",
        _python_test("Sales.Order", reads="Sales.Order"),
    )

    with pytest.raises(DiscoveryError, match="declared twice"):
        parse(lakehouse)


def test_a_test_and_an_assumption_may_not_share_a_logical_id(lakehouse):
    _write(
        lakehouse,
        "Lakehouse/Sales/tests/Sales__Reconciles.py",
        _python_test("Sales.Reconciles"),
    )
    _write(
        lakehouse,
        "Lakehouse/Sales/assumptions/Sales__Reconciles.py",
        _python_assumption("Sales.Reconciles"),
    )

    with pytest.raises(DiscoveryError, match="declared twice"):
        parse(lakehouse)


def test_the_same_validation_id_in_two_items_is_ordinary(tmp_path):
    """Identity is item-qualified, so two items may each test Sales.Reconciles."""

    for name in ("Sales", "Inventory"):
        _write(tmp_path, f"Lakehouse/{name}/schemas/Sales.yml", _schema("Sales"))
        _write(tmp_path, f"Lakehouse/{name}/Sales__Order.py", _table("Sales.Order"))
        _write(
            tmp_path,
            f"Lakehouse/{name}/tests/Sales__Reconciles.py",
            _python_test("Sales.Reconciles"),
        )

    repository = parse(tmp_path)

    assert len(item(repository, "Sales").validations) == 1
    assert len(item(repository, "Inventory").validations) == 1


# --- dependencies -----------------------------------------------------------


def test_a_python_import_is_a_validation_dependency(lakehouse):
    """The ordinary AST machinery, with nothing added for validation."""

    _write(
        lakehouse,
        "Lakehouse/Sales/tests/Sales__OrdersReconcile.py",
        _python_test("Sales.OrdersReconcile"),
    )

    repository = parse(lakehouse)
    consumer = WeaverDocumentId.parse("Lakehouse/Sales/Sales.OrdersReconcile")
    producers = [
        str(edge.producer)
        for edge in repository.dependency_edges
        if edge.consumer == consumer and edge.producer is not None
    ]

    assert producers == ["Lakehouse/Sales/Sales.Order"]


def test_a_sql_reference_is_a_validation_dependency(warehouse):
    _write(
        warehouse,
        "Warehouse/Reporting/tests/Sales.OrderReconciliation.sql",
        _tsql_test("Sales.OrderReconciliation"),
    )

    repository = parse(warehouse)
    consumer = WeaverDocumentId.parse("Warehouse/Reporting/Sales.OrderReconciliation")
    producers = [
        str(edge.producer)
        for edge in repository.dependency_edges
        if edge.consumer == consumer and edge.producer is not None
    ]

    assert producers == ["Warehouse/Reporting/Sales.Order"]


def _validation_edges(repository, consumer_text: str):
    consumer = WeaverDocumentId.parse(consumer_text)
    return sorted(
        edge.reference
        for edge in repository.dependency_edges
        if edge.consumer == consumer
    )


def test_a_declaration_supplements_inference_rather_than_replacing_it(lakehouse):
    """The rule a validation uses, and the opposite of the one an object uses.

    A validation reads what it reads: the edges exist to run it after the data
    it inspects is ready, so naming one more adds to what was found. An object's
    declared graph is its build order, so there a declaration replaces.
    """

    _write(lakehouse, "Lakehouse/Sales/Sales__Customer.py", _table("Sales.Customer"))
    source = _python_test("Sales.OrdersReconcile").replace(
        "Primary key: Id", "Primary key: Id\n\nDependencies:\n  - Sales.Customer"
    )
    _write(lakehouse, "Lakehouse/Sales/tests/Sales__OrdersReconcile.py", source)

    repository = parse(lakehouse)

    # The import was inferred; the header added the one it could not reach.
    assert _validation_edges(repository, "Lakehouse/Sales/Sales.OrdersReconcile") == [
        "Sales.Customer",
        "Sales__Order",
    ]


def test_declaring_nothing_leaves_the_inferred_graph_intact(lakehouse):
    """`Dependencies: []` suppresses discovery on an object. Not here."""

    source = _python_test("Sales.OrdersReconcile").replace(
        "Primary key: Id", "Primary key: Id\n\nDependencies: []"
    )
    _write(lakehouse, "Lakehouse/Sales/tests/Sales__OrdersReconcile.py", source)

    repository = parse(lakehouse)

    assert _validation_edges(repository, "Lakehouse/Sales/Sales.OrdersReconcile") == [
        "Sales__Order"
    ]


def test_declaring_what_was_already_inferred_is_still_one_edge(lakehouse):
    """One dependency named twice, in two spellings, is one row."""

    source = _python_test("Sales.OrdersReconcile").replace(
        "Primary key: Id", "Primary key: Id\n\nDependencies:\n  - Sales.Order"
    )
    _write(lakehouse, "Lakehouse/Sales/tests/Sales__OrdersReconcile.py", source)

    repository = parse(lakehouse)

    assert _validation_edges(repository, "Lakehouse/Sales/Sales.OrdersReconcile") == [
        "Sales__Order"
    ]


def test_a_spark_sql_validation_infers_its_references(tmp_path):
    """No `Dependencies:` header, and the graph is still right."""

    _write(tmp_path, "Lakehouse/Sales/schemas/Sales.yml", _schema("Sales"))
    _write(tmp_path, "Lakehouse/Sales/Sales__Order.py", _table("Sales.Order"))
    _write(
        tmp_path,
        "Lakehouse/Sales/tests/Sales.OrdersReconcile.sql",
        """/*
Test ID: Sales.OrdersReconcile

Description: The summary matches the independent aggregation.
*/
select Id from Sales.Order;

select Id from Sales.Order;
""",
    )

    repository = parse(tmp_path)

    assert _validation_edges(repository, "Lakehouse/Sales/Sales.OrdersReconcile") == [
        "Sales.Order"
    ]


def test_a_changed_test_changes_its_item_signature(lakehouse):
    _write(
        lakehouse,
        "Lakehouse/Sales/tests/Sales__OrdersReconcile.py",
        _python_test("Sales.OrdersReconcile"),
    )
    before = item(parse(lakehouse), "Sales").signature

    _write(
        lakehouse,
        "Lakehouse/Sales/tests/Sales__OrdersReconcile.py",
        _python_test("Sales.OrdersReconcile").replace(
            "the independent calculation", "the independently derived relation"
        ),
    )

    assert item(parse(lakehouse), "Sales").signature != before


# --- the Python structural contract -----------------------------------------


def test_a_test_must_define_expected_and_actual(lakehouse):
    source = _python_test("Sales.OrdersReconcile").replace(
        "    def actual(self):\n        return Sales__Order(self).dataframe()\n", ""
    )
    _write(lakehouse, "Lakehouse/Sales/tests/Sales__OrdersReconcile.py", source)

    with pytest.raises(DiscoveryError, match="must implement actual"):
        parse(lakehouse)


def test_a_test_may_not_author_read(lakehouse):
    source = _python_test("Sales.OrdersReconcile") + """
    def read(self):
        return None
"""
    _write(lakehouse, "Lakehouse/Sales/tests/Sales__OrdersReconcile.py", source)

    with pytest.raises(DiscoveryError, match="must not define read"):
        parse(lakehouse)


def test_an_assumption_must_define_read(lakehouse):
    source = _python_assumption("Sales.OrdersHaveCustomers").replace(
        "def read(self):", "def rows(self):"
    )
    _write(lakehouse, "Lakehouse/Sales/assumptions/Sales__OrdersHaveCustomers.py", source)

    with pytest.raises(DiscoveryError, match="must implement read"):
        parse(lakehouse)


def test_a_test_must_inherit_test(lakehouse):
    source = _python_test("Sales.OrdersReconcile").replace(
        "class Sales__OrdersReconcile(Test)", "class Sales__OrdersReconcile(Assumption)"
    ).replace("from weaver import Test", "from weaver import Assumption")
    _write(lakehouse, "Lakehouse/Sales/tests/Sales__OrdersReconcile.py", source)

    with pytest.raises(DiscoveryError, match="must inherit Test"):
        parse(lakehouse)


# --- the SQL structural contract --------------------------------------------


def test_a_sql_test_is_exactly_two_result_queries(warehouse):
    _write(
        warehouse,
        "Warehouse/Reporting/tests/Sales.OrderReconciliation.sql",
        _tsql_test("Sales.OrderReconciliation") + "\nselect 1 as Id;\n",
    )

    with pytest.raises(DiscoveryError, match="must produce exactly 2 result set"):
        parse(warehouse)


def test_a_sql_assumption_is_exactly_one_result_query(warehouse):
    _write(
        warehouse,
        "Warehouse/Reporting/assumptions/Sales.OrdersHaveCustomers.sql",
        _tsql_assumption("Sales.OrdersHaveCustomers") + "\nselect 1 as Id;\n",
    )

    with pytest.raises(DiscoveryError, match="must produce exactly 1 result set"):
        parse(warehouse)


def test_setup_statements_precede_the_contract_queries(warehouse):
    _write(
        warehouse,
        "Warehouse/Reporting/tests/Sales.OrderReconciliation.sql",
        _tsql_test("Sales.OrderReconciliation").replace(
            "select Id from Sales.Order;",
            "select Id into #expected from Sales.Order;\n\nselect Id from #expected;",
            1,
        ),
    )

    reporting = item(parse(warehouse), "Reporting")

    assert len(reporting.validations) == 1


def test_dynamic_setup_is_not_a_reason_to_refuse_a_validation(warehouse):
    """The contract is about the queries Weaver can see, not about EXEC."""

    _write(
        warehouse,
        "Warehouse/Reporting/tests/Sales.OrderReconciliation.sql",
        _tsql_test("Sales.OrderReconciliation").replace(
            "select Id from Sales.Order;",
            "exec sp_executesql N'select 1';\n\nselect Id from Sales.Order;",
            1,
        ),
    )

    reporting = item(parse(warehouse), "Reporting")

    assert len(reporting.validations) == 1


def test_data_metadata_on_a_validation_is_refused_by_the_repository(lakehouse):
    source = _python_test("Sales.OrdersReconcile").replace(
        "Primary key: Id", "Primary key: Id\n\nLineage: A source system."
    )
    _write(lakehouse, "Lakehouse/Sales/tests/Sales__OrdersReconcile.py", source)

    with pytest.raises(MetadataError, match="unknown metadata key"):
        parse(lakehouse)
