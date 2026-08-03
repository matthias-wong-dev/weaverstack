"""``Table.load()`` and ``Folder.load()`` against real Delta files.

These are *primitive* tests: each constructs the authored object directly and
calls ``load()``. No repository is parsed, no catalogue is read, no bundle is
built and no planner or orchestrator runs — which is the claim, not merely the
arrangement. If any of that were needed, ``load()`` would not be a primitive.

The object modules are written to disk and imported, rather than declared in
this file, because that is how a load actually meets one. A class defined here
would read *this* module's docstring as its contract, and the property under
test is precisely that a deployed module carries its own.
"""

from __future__ import annotations

import importlib
import sys
import textwrap

import pytest

from weaver import DeltaTarget, ItemRef, lakehouse_for

pytestmark = pytest.mark.spark

TARGET = "Sales_LH"

CUSTOMER_MODULE = '''\
"""
Table ID: Sales.Customer

Description: One row per customer.

Lineage: The sales system.

Primary key: Customer id

Schema:
  Customer id: string
  Customer name: string
"""
from weaver import Table


class Sales__Customer(Table):
    rows = []
    deletes = []

    def read(self):
        frame = self.spark.createDataFrame(
            self.rows, "`Customer id` string, `Customer name` string"
        )
        deletes = self.spark.createDataFrame(
            self.deletes, "`Customer id` string"
        )
        return frame, deletes
'''

EXPORT_MODULE = '''\
"""
Folder ID: Sales.Export

Description: Customer extracts.

Lineage: Sales.Customer

File key: "*.csv"

Incremental: false
"""
from pathlib import Path

from weaver import Folder


class Sales__Export(Folder):
    files = {}

    def read(self):
        staging = Path(self.staging_folder())
        staging.mkdir(parents=True, exist_ok=True)
        for name, text in self.files.items():
            (staging / name).write_text(text, encoding="utf-8")
        return str(staging), []
'''


@pytest.fixture
def deployed(tmp_path, monkeypatch):
    """Write the object modules where a deployed runtime tree would put them."""

    root = tmp_path / "deployed"
    root.mkdir()
    (root / "Sales__Customer.py").write_text(CUSTOMER_MODULE, encoding="utf-8")
    (root / "Sales__Export.py").write_text(EXPORT_MODULE, encoding="utf-8")
    monkeypatch.syspath_prepend(str(root))
    for name in ("Sales__Customer", "Sales__Export"):
        sys.modules.pop(name, None)
    yield root
    for name in ("Sales__Customer", "Sales__Export"):
        sys.modules.pop(name, None)


@pytest.fixture
def lakehouse(lakehouses):
    return lakehouse_for(lakehouses.resolver, ItemRef(TARGET))


@pytest.fixture
def customer(spark, lakehouses, lakehouse, deployed):
    """The built target, plus the authored class that loads into it.

    The table is created first because a load writes into a built table — the
    same order the real system uses, where build and load are separate phases.

    The schema is dropped afterwards. The Spark session is session-scoped while
    each test gets its own `tmp_path`, so a registered schema outlives the files
    it pointed at — a schema is not a cache, and nothing clears it for us.
    """

    # With a LOCATION, as the real schema action issues. Without one Spark pins
    # the schema to its own warehouse directory, so the table would outlive the
    # test's tmp_path and the next run would meet the last run's files.
    schema = lakehouse.destination.qualified_schema("Sales")
    location = lakehouse.destination.schema_location("Sales")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema} LOCATION '{location}'")
    spark.sql(
        f"CREATE TABLE {lakehouse.qualify('Sales', 'Customer')} (\n"
        "  `Customer id` string NOT NULL,\n"
        "  `Customer name` string,\n"
        "  `row_insert_datetime` timestamp NOT NULL,\n"
        "  `row_update_datetime` timestamp NOT NULL,\n"
        "  `row_delete_datetime` timestamp NOT NULL\n"
        ") USING delta TBLPROPERTIES ('delta.columnMapping.mode' = 'name')"
    )
    module = importlib.import_module("Sales__Customer")
    yield module.Sales__Customer
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")


def _load(cls, spark, lakehouse, rows, *, deletes=(), fault_tolerant=False):
    cls.rows = list(rows)
    cls.deletes = list(deletes)
    return cls(spark, lakehouse=lakehouse).load(fault_tolerant=fault_tolerant)


def _contents(spark, lakehouse):
    frame = spark.sql(
        f"SELECT `Customer id`, `Customer name` "
        f"FROM {lakehouse.qualify('Sales', 'Customer')} ORDER BY 1"
    )
    return [(row["Customer id"], row["Customer name"]) for row in frame.collect()]


# --- the ordinary path -------------------------------------------------------


def test_a_table_load_inserts_the_rows_its_object_proposed(spark, lakehouse, customer):
    result = _load(customer, spark, lakehouse, [("c1", "One"), ("c2", "Two")])

    assert result.succeeded is True
    assert (result.rows_read, result.rows_inserted) == (2, 2)
    assert _contents(spark, lakehouse) == [("c1", "One"), ("c2", "Two")]


def test_a_second_load_updates_only_what_changed(spark, lakehouse, customer):
    _load(customer, spark, lakehouse, [("c1", "One"), ("c2", "Two")])

    result = _load(customer, spark, lakehouse, [("c1", "One"), ("c2", "Changed")])

    assert (result.rows_inserted, result.rows_updated) == (0, 1)
    assert _contents(spark, lakehouse) == [("c1", "One"), ("c2", "Changed")]


def test_an_unchanged_row_keeps_its_original_update_time(spark, lakehouse, customer):
    """Otherwise "when did this row last change" means "when was this loaded"."""

    _load(customer, spark, lakehouse, [("c1", "One"), ("c2", "Two")])
    _load(customer, spark, lakehouse, [("c1", "One"), ("c2", "Changed")])

    frame = spark.sql(
        f"SELECT `Customer id`, "
        f"`row_insert_datetime` = `row_update_datetime` AS untouched "
        f"FROM {lakehouse.qualify('Sales', 'Customer')} ORDER BY 1"
    ).collect()

    assert [(row["Customer id"], row["untouched"]) for row in frame] == [
        ("c1", True),
        ("c2", False),
    ]


def test_a_non_incremental_load_deletes_rows_the_source_stopped_producing(
    spark, lakehouse, customer
):
    _load(customer, spark, lakehouse, [("c1", "One"), ("c2", "Two")])

    result = _load(customer, spark, lakehouse, [("c1", "One")])

    assert result.rows_deleted == 1
    assert _contents(spark, lakehouse) == [("c1", "One")]


def test_an_explicit_delete_retires_a_row_the_source_still_produces(
    spark, lakehouse, customer
):
    """An explicit delete is a statement, not an inference.

    The object names the row as gone, so it goes — which is how an incremental
    source reports a deletion it actually observed.
    """

    _load(customer, spark, lakehouse, [("c1", "One"), ("c2", "Two")])

    result = _load(
        customer, spark, lakehouse, [("c1", "One"), ("c2", "Two")], deletes=[("c2",)]
    )

    assert result.rows_deleted == 1
    assert _contents(spark, lakehouse) == [("c1", "One")]


# --- rejection and fault tolerance -------------------------------------------


REJECTABLE = [("c1", "One"), (None, "NoKey"), ("   ", "Blank"), ("c4", "A"), ("c4", "B")]


def test_an_intolerant_load_with_rejects_leaves_the_target_untouched(
    spark, lakehouse, customer
):
    result = _load(customer, spark, lakehouse, REJECTABLE, fault_tolerant=False)

    assert result.succeeded is False
    assert result.rows_rejected == 3
    assert (result.rows_inserted, result.rows_updated, result.rows_deleted) == (0, 0, 0)
    assert _contents(spark, lakehouse) == []


def test_a_tolerant_load_writes_the_valid_rows_and_still_reports_failure(
    spark, lakehouse, customer
):
    """The rows did not arrive, so the run is not a success.

    Tolerating rejects changes what is written, never what is reported — a
    caller that only checked for an exception would otherwise call this clean.
    """

    result = _load(customer, spark, lakehouse, REJECTABLE, fault_tolerant=True)

    assert result.succeeded is False
    assert result.rows_rejected == 3
    assert result.rows_inserted == 2
    assert _contents(spark, lakehouse) == [("c1", "One"), ("c4", "A")]


def test_the_rejected_rows_are_kept_with_their_reason(spark, lakehouse, customer):
    """A count says something went wrong and nothing about what."""

    _load(customer, spark, lakehouse, REJECTABLE, fault_tolerant=True)

    rejects = spark.sql(
        f"SELECT `Customer id`, `Rejection reason` "
        f"FROM {lakehouse.qualify('Sales', 'Customer_Reject')}"
    ).collect()

    assert {row["Rejection reason"] for row in rejects} == {
        "null primary key",
        "duplicate primary key",
    }


# --- the contract comes from the module --------------------------------------


def test_a_metadata_edit_is_visible_after_reload(spark, lakehouse, customer, deployed):
    """The notebook loop: edit the docstring, reload, load again.

    Nothing revalidates the module against the repository it came from, which is
    the operator's risk to take and exactly what makes the loop usable.
    """

    _load(customer, spark, lakehouse, [("c1", "One"), ("c2", "Two")])

    path = deployed / "Sales__Customer.py"
    path.write_text(
        CUSTOMER_MODULE.replace(
            "Primary key: Customer id", "Primary key: Customer id\n\nIncremental: true"
        ),
        encoding="utf-8",
    )
    module = importlib.reload(importlib.import_module("Sales__Customer"))

    result = _load(module.Sales__Customer, spark, lakehouse, [("c1", "One")])

    # Incremental now, so absence is not a retirement and c2 survives.
    assert result.rows_deleted == 0
    assert _contents(spark, lakehouse) == [("c1", "One"), ("c2", "Two")]


def test_a_table_load_needs_no_repository_or_catalogue(spark, lakehouse, customer):
    """Stated as a test because it is the design, not an implementation detail."""

    import weaver.catalogue.state
    import weaver.declaration.repository

    def refuse(*args, **kwargs):  # pragma: no cover - the point is not reaching it
        raise AssertionError("a load primitive reached for repository state")

    original = weaver.declaration.repository.parse_item_repository
    weaver.declaration.repository.parse_item_repository = refuse
    try:
        result = _load(customer, spark, lakehouse, [("c1", "One")])
    finally:
        weaver.declaration.repository.parse_item_repository = original

    assert result.succeeded is True


# --- folders -----------------------------------------------------------------


@pytest.fixture
def export(spark, lakehouse, deployed):
    module = importlib.import_module("Sales__Export")
    return module.Sales__Export


def _load_folder(cls, spark, lakehouse, files, **kwargs):
    cls.files = dict(files)
    return cls(spark, lakehouse=lakehouse).load(**kwargs)


def _folder_contents(lakehouse):
    from pathlib import Path

    root = Path(lakehouse.folder_path("Sales", "Export"))
    if not root.exists():
        return {}
    return {
        path.name: path.read_text(encoding="utf-8")
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def test_a_folder_load_publishes_what_it_staged(spark, lakehouse, export):
    result = _load_folder(export, spark, lakehouse, {"a.csv": "1", "b.csv": "2"})

    assert result.succeeded is True
    assert (result.rows_read, result.rows_inserted) == (2, 2)
    assert _folder_contents(lakehouse) == {"a.csv": "1", "b.csv": "2"}


def test_a_folder_load_replaces_and_counts_what_changed(spark, lakehouse, export):
    _load_folder(export, spark, lakehouse, {"a.csv": "1", "b.csv": "2"})

    result = _load_folder(export, spark, lakehouse, {"a.csv": "1", "b.csv": "changed"})

    # `a.csv` is byte-identical, so it is neither inserted nor updated.
    assert (result.rows_inserted, result.rows_updated) == (0, 1)
    assert _folder_contents(lakehouse) == {"a.csv": "1", "b.csv": "changed"}


def test_a_non_incremental_folder_removes_what_it_stopped_producing(
    spark, lakehouse, export
):
    _load_folder(export, spark, lakehouse, {"a.csv": "1", "b.csv": "2"})

    result = _load_folder(export, spark, lakehouse, {"a.csv": "1"})

    assert result.rows_deleted == 1
    assert _folder_contents(lakehouse) == {"a.csv": "1"}


def test_a_file_the_key_does_not_claim_survives_replacement(spark, lakehouse, export):
    """The file key is what makes replacement safe.

    Only files the key matches are Weaver's to manage, so a replacement is not
    entitled to remove anything else that happens to be in the destination.
    """

    from pathlib import Path

    _load_folder(export, spark, lakehouse, {"a.csv": "1"})
    unmanaged = Path(lakehouse.folder_path("Sales", "Export")) / "README.md"
    unmanaged.write_text("not Weaver's", encoding="utf-8")

    _load_folder(export, spark, lakehouse, {"a.csv": "1"})

    assert unmanaged.exists()
