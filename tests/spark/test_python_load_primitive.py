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

from weaver.targets import DeltaTarget, ItemRef
from weaver import lakehouse_for
from weaver.errors import LoadError
from weaver.runtime.load_contract import (
    REASON_BLANK_PK,
    REASON_DUPLICATE_PK,
    REJECTION_REASON,
)

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

UNKEYED_MODULE = '''\
"""
Table ID: Sales.Snapshot

Description: A table with no key, replaced whole on every load.

Lineage: The sales system.

Schema:
  Customer id: string
  Customer name: string
"""
from weaver import Table


class Sales__Snapshot(Table):
    rows = []
    fail = False

    def read(self):
        frame = self.spark.createDataFrame(
            self.rows, "`Customer id` string, `Customer name` string"
        )
        if self.fail:
            # Fails while staging is materialised, which is before the target is
            # touched — the ordering the physical staging table exists to give.
            frame = frame.selectExpr("`Customer id`", "no_such_column AS `Customer name`")
        return frame, None
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
    (root / "Sales__Snapshot.py").write_text(UNKEYED_MODULE, encoding="utf-8")
    (root / "Sales__Export.py").write_text(EXPORT_MODULE, encoding="utf-8")
    monkeypatch.syspath_prepend(str(root))
    for name in ("Sales__Customer", "Sales__Export", "Sales__Snapshot"):
        sys.modules.pop(name, None)
    yield root
    for name in ("Sales__Customer", "Sales__Export", "Sales__Snapshot"):
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


@pytest.fixture
def incremental(spark, lakehouse, customer, deployed):
    """The same table declared incremental, where explicit deletes are the driver."""

    import importlib

    (deployed / "Sales__Customer.py").write_text(
        CUSTOMER_MODULE.replace(
            "Primary key: Customer id", "Primary key: Customer id\n\nIncremental: true"
        ),
        encoding="utf-8",
    )
    return importlib.reload(importlib.import_module("Sales__Customer")).Sales__Customer


@pytest.fixture
def unkeyed(spark, lakehouse, customer, deployed):
    """An unkeyed table beside the keyed one, sharing its schema fixture."""

    import importlib

    spark.sql(
        f"CREATE TABLE {lakehouse.qualify('Sales', 'Snapshot')} (\n"
        "  `Customer id` string,\n"
        "  `Customer name` string,\n"
        "  `row_insert_datetime` timestamp NOT NULL,\n"
        "  `row_update_datetime` timestamp NOT NULL,\n"
        "  `row_delete_datetime` timestamp NOT NULL\n"
        ") USING delta TBLPROPERTIES ('delta.columnMapping.mode' = 'name')"
    )
    module = importlib.import_module("Sales__Snapshot")
    module.Sales__Snapshot.fail = False
    return module.Sales__Snapshot


def _load(cls, spark, lakehouse, rows, *, deletes=(), fault_tolerant=False, keep=False):
    """Run one load. ``keep`` stages an extra rejectable row so the artefacts
    survive, for the tests whose subject is what a load left behind."""

    cls.rows = list(rows) + ([(None, "keeps the artefacts")] if keep else [])
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


def test_an_incremental_load_deletes_only_what_it_was_told_to(
    spark, lakehouse, incremental
):
    """Absence proves nothing to an incremental source, so it must *state*.

    `c2` is missing from this run's rows and survives; `c3` is named for
    deletion and goes.
    """

    _load(incremental, spark, lakehouse,
          [("c1", "One"), ("c2", "Two"), ("c3", "Three")])

    result = _load(incremental, spark, lakehouse, [("c1", "One")], deletes=[("c3",)])

    assert result.rows_deleted == 1
    assert _contents(spark, lakehouse) == [("c1", "One"), ("c2", "Two")]


def test_a_staged_key_may_still_be_explicitly_deleted(spark, lakehouse, incremental):
    """The delete runs after the upsert, so the later statement wins.

    No conflict rule is needed, and over-deleting is the recoverable direction.
    """

    _load(incremental, spark, lakehouse, [("c1", "One"), ("c2", "Two")])

    result = _load(
        incremental, spark, lakehouse, [("c1", "One"), ("c2", "Two")], deletes=[("c2",)]
    )

    assert result.rows_deleted == 1
    assert _contents(spark, lakehouse) == [("c1", "One")]


def test_a_non_incremental_load_refuses_explicit_deletes(spark, lakehouse, customer):
    """The source is the whole truth, so absence already retires a row.

    An explicit list would be a second, quieter answer to a question the
    reconciliation has already answered — so it is refused rather than ignored,
    as a non-incremental folder's explicit deletes already are.
    """

    _load(customer, spark, lakehouse, [("c1", "One"), ("c2", "Two")])

    with pytest.raises(LoadError, match="cannot name explicit deletes"):
        _load(customer, spark, lakehouse, [("c1", "One")], deletes=[("c2",)])


def test_a_delete_for_a_row_that_was_not_there_is_not_a_deletion(
    spark, lakehouse, incremental
):
    """Reported from the target's own cardinality, so what is counted is what
    actually left rather than what was asked for."""

    _load(incremental, spark, lakehouse, [("c1", "One")])

    result = _load(incremental, spark, lakehouse, [("c1", "One")], deletes=[("gone",)])

    assert result.rows_deleted == 0


# --- rejection and fault tolerance -------------------------------------------


REJECTABLE = [("c1", "One"), (None, "NoKey"), ("   ", "Blank"), ("c4", "A"), ("c4", "B")]


def test_an_intolerant_load_with_rejects_raises_and_leaves_the_target_untouched(
    spark, lakehouse, customer
):
    """`fault_tolerant` decides how a failure surfaces: raised, or returned.

    The error carries the result, so a caller that catches it can still report
    how many rows were read and how many refused.
    """

    with pytest.raises(LoadError) as raised:
        _load(customer, spark, lakehouse, REJECTABLE, fault_tolerant=False)

    assert raised.value.result.rows_rejected == 3
    assert raised.value.result.succeeded is False
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
        f"SELECT `Customer id`, `{REJECTION_REASON}` "
        f"FROM {lakehouse.qualify('Sales', 'Customer_Reject')}"
    ).collect()

    assert {row[REJECTION_REASON] for row in rejects} == {
        REASON_BLANK_PK,
        REASON_DUPLICATE_PK,
    }


# --- the reconciliation lifecycle, not just its outcome ----------------------


def _tables(spark, lakehouse):
    """Which of the load's artefacts exist right now."""

    rows = spark.sql(
        f"SHOW TABLES IN {lakehouse.destination.qualified_schema('Sales')}"
    ).collect()
    return {row["tableName"] for row in rows}


def test_the_intermediate_artefacts_are_real_delta_tables(spark, lakehouse, customer):
    """Physical, as they are in the Warehouse and the generated Spark program.

    A run that refused rows is one someone will want to look at, and a temporary
    view vanishes with the session — which is exactly when they look.
    """

    _load(customer, spark, lakehouse, REJECTABLE, fault_tolerant=True)

    present = _tables(spark, lakehouse)

    assert {"customer_staging", "customer_upsert", "customer_reject"} <= {
        name.lower() for name in present
    }


def test_staging_holds_what_read_produced_before_anything_was_rejected(
    spark, lakehouse, customer
):
    """`read()[0]` is staging, not an upsert set.

    It has not been validated and nothing has been classified — which is why the
    rejected rows were in it and the reject table is derived *from* it.
    """

    _load(customer, spark, lakehouse, REJECTABLE, fault_tolerant=True)

    reject_count = spark.sql(
        f"SELECT count(*) AS n FROM {lakehouse.qualify('Sales', 'Customer_Reject')}"
    ).collect()[0]["n"]

    assert reject_count == 3


def test_the_upsert_set_records_what_weaver_decided_to_change(
    spark, lakehouse, customer
):
    """Inspectable afterwards: which rows were new, and which merely changed."""

    _load(customer, spark, lakehouse, [("c1", "One"), ("c2", "Two")])
    _load(customer, spark, lakehouse, [("c1", "One"), ("c2", "Changed"), ("c3", "New")],
          fault_tolerant=True, keep=True)

    rows = spark.sql(
        f"SELECT `Customer id`, `_Is new row` "
        f"FROM {lakehouse.qualify('Sales', 'Customer_Upsert')} ORDER BY 1"
    ).collect()

    # c1 is unchanged, so it is not in the change set at all.
    assert [(r["Customer id"], r["_Is new row"]) for r in rows] == [
        ("c2", 0),
        ("c3", 1),
    ]


def test_a_clean_run_clears_the_artefacts_it_no_longer_needs(
    spark, lakehouse, customer
):
    _load(customer, spark, lakehouse, [("c1", "One")])

    present = {name.lower() for name in _tables(spark, lakehouse)}

    assert "customer" in present
    assert not {"customer_staging", "customer_upsert", "customer_reject"} & present


def test_a_clean_run_clears_the_previous_run_s_rejects(spark, lakehouse, customer):
    """Otherwise a stale reject table reads as evidence about the run that just
    succeeded."""

    _load(customer, spark, lakehouse, REJECTABLE, fault_tolerant=True)
    assert "customer_reject" in {name.lower() for name in _tables(spark, lakehouse)}

    _load(customer, spark, lakehouse, [("c1", "One")])

    assert "customer_reject" not in {name.lower() for name in _tables(spark, lakehouse)}


def test_a_full_replacement_materialises_staging_before_emptying_the_target(
    spark, lakehouse, deployed, unkeyed
):
    """Clearing the target and *then* evaluating the source would leave nothing
    behind when the source fails."""

    _load(unkeyed, spark, lakehouse, [("c1", "One"), ("c2", "Two")])

    unkeyed.fail = True
    with pytest.raises(Exception):
        _load(unkeyed, spark, lakehouse, [("c3", "Three")])

    # The target still holds the previous load: the failure happened while
    # staging was being materialised, before the delete.
    rows = spark.sql(
        f"SELECT `Customer id`, `Customer name` "
        f"FROM {lakehouse.qualify('Sales', 'Snapshot')} ORDER BY 1"
    ).collect()
    assert [(r["Customer id"], r["Customer name"]) for r in rows] == [
        ("c1", "One"),
        ("c2", "Two"),
    ]


# --- stability thresholds ----------------------------------------------------


GUARDED_MODULE = CUSTOMER_MODULE.replace(
    "Primary key: Customer id",
    "Primary key: Customer id\n\nDelete percentage threshold: 5"
    "\n\nUpdate percentage threshold: 20\n\nStability row threshold: 10",
)


@pytest.fixture
def guarded(spark, lakehouse, customer, deployed):
    """The same table, declaring thresholds a small fixture can actually trip.

    The row threshold is 10 rather than the default million, because the guard's
    subject is the percentages and a test cannot afford a million rows to reach
    them.
    """

    import importlib

    (deployed / "Sales__Customer.py").write_text(GUARDED_MODULE, encoding="utf-8")
    return importlib.reload(importlib.import_module("Sales__Customer")).Sales__Customer


TWENTY = [(f"c{n}", f"Name {n}") for n in range(20)]


def test_a_load_below_the_row_threshold_is_never_guarded(spark, lakehouse, customer):
    """On a small table one row is a large percentage, and tripping on that
    would teach everyone to disable the guard."""

    _load(customer, spark, lakehouse, [("c1", "One"), ("c2", "Two")])

    result = _load(customer, spark, lakehouse, [("c1", "One")])

    assert result.succeeded is True
    assert result.rows_deleted == 1


def test_too_many_deletes_refuses_and_leaves_the_target_untouched(
    spark, lakehouse, guarded
):
    """A source that broke overnight produces a load Weaver would otherwise
    carry out faithfully."""

    _load(guarded, spark, lakehouse, TWENTY)

    with pytest.raises(LoadError, match="over the 5% threshold") as raised:
        _load(guarded, spark, lakehouse, TWENTY[:5])

    assert raised.value.result.succeeded is False
    assert len(_contents(spark, lakehouse)) == 20


def test_too_many_updates_refuses(spark, lakehouse, guarded):
    _load(guarded, spark, lakehouse, TWENTY)

    changed = [(key, f"changed {key}") for key, _ in TWENTY]
    with pytest.raises(LoadError, match="over the 20% threshold"):
        _load(guarded, spark, lakehouse, changed)

    # Untouched: the names are the originals, not the changed ones.
    assert sorted(_contents(spark, lakehouse)) == sorted(TWENTY)


def test_tolerating_a_breach_changes_only_how_it_is_reported(
    spark, lakehouse, guarded
):
    """A breach never writes.

    Tolerating exactly the change the threshold was declared to prevent would
    defeat the guard, so `fault_tolerant` decides only whether the refusal is
    raised or returned. Permitting it is what `ignore_stability_threshold` is
    for.
    """

    _load(guarded, spark, lakehouse, TWENTY)

    result = _load(guarded, spark, lakehouse, TWENTY[:5], fault_tolerant=True)

    assert result.succeeded is False
    assert result.rows_deleted == 0
    assert len(_contents(spark, lakehouse)) == 20


def test_the_threshold_can_be_waived_for_one_run(spark, lakehouse, guarded):
    """For the case where a very large change is the correct answer."""

    _load(guarded, spark, lakehouse, TWENTY)

    guarded.rows = TWENTY[:5]
    guarded.deletes = []
    result = guarded(spark, lakehouse=lakehouse).load(ignore_stability_threshold=True)

    assert result.succeeded is True
    assert result.rows_deleted == 15


def test_an_unkeyed_load_is_exempt(spark, lakehouse, unkeyed):
    """It replaces every row by definition, so a delete threshold would trip on
    every run — which is the declaration working, not a symptom."""

    _load(unkeyed, spark, lakehouse, [(f"c{n}", f"Name {n}") for n in range(20)])

    result = _load(unkeyed, spark, lakehouse, [("c1", "One")])

    assert result.succeeded is True


def test_the_threshold_counts_the_selected_delete_driver(
    spark, lakehouse, customer, deployed
):
    """One driver, counted once.

    An incremental load's deletes are what it named; adding an absence count on
    top would guard against work the load was never going to do.
    """

    import importlib

    (deployed / "Sales__Customer.py").write_text(
        GUARDED_MODULE.replace(
            "Primary key: Customer id", "Primary key: Customer id\n\nIncremental: true"
        ),
        encoding="utf-8",
    )
    cls = importlib.reload(importlib.import_module("Sales__Customer")).Sales__Customer

    _load(cls, spark, lakehouse, TWENTY)

    # Fifteen rows are absent from this run — but the source is incremental, so
    # absence deletes nothing and only the one named key counts toward the guard.
    result = _load(cls, spark, lakehouse, TWENTY[:5], deletes=[("c0",)])

    assert result.succeeded is True
    assert result.rows_deleted == 1


def test_reported_deletions_are_reconciled_from_cardinality(
    spark, lakehouse, customer
):
    """before + inserted - after, so what is reported is what actually left."""

    _load(customer, spark, lakehouse, [("c1", "One"), ("c2", "Two"), ("c3", "Three")])

    result = _load(customer, spark, lakehouse, [("c1", "One"), ("c4", "Four")])

    assert (result.rows_inserted, result.rows_deleted) == (1, 2)
    assert _contents(spark, lakehouse) == [("c1", "One"), ("c4", "Four")]


def test_an_empty_target_is_never_guarded(spark, lakehouse, guarded):
    """A first load has no proportion to be a percentage of."""

    result = _load(guarded, spark, lakehouse, TWENTY)

    assert result.succeeded is True
    assert result.rows_inserted == 20


def test_a_tolerated_rejection_loads_the_valid_rows_and_reports_failure(
    spark, lakehouse, customer
):
    """The other half of what fault_tolerant governs: suppress, not permit."""

    result = _load(customer, spark, lakehouse, REJECTABLE, fault_tolerant=True)

    assert result.succeeded is False
    assert result.rows_rejected == 3
    assert result.rows_inserted == 2


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


def test_a_nested_file_is_not_claimed_by_a_single_segment_key(spark, lakehouse, export):
    """`*.csv` claims `a.csv` and not `archive/old.csv`.

    The file key is matched segment by segment, so `*` stops at a directory
    boundary. Matching it against the whole path instead would quietly claim
    every nested file — and then delete it on the next replacement.
    """

    from pathlib import Path

    _load_folder(export, spark, lakehouse, {"a.csv": "1"})
    nested = Path(lakehouse.folder_path("Sales", "Export")) / "archive" / "old.csv"
    nested.parent.mkdir(parents=True, exist_ok=True)
    nested.write_text("kept", encoding="utf-8")

    _load_folder(export, spark, lakehouse, {"a.csv": "1"})

    assert nested.exists()


def test_a_staged_file_the_key_does_not_claim_is_rejected(spark, lakehouse, export):
    """Publishing a folder while silently dropping part of it is not a success."""

    result = _load_folder(
        export, spark, lakehouse, {"a.csv": "1", "notes.md": "x"}, fault_tolerant=True
    )

    assert result.succeeded is False
    assert result.rows_rejected == 1
    assert _folder_contents(lakehouse) == {"a.csv": "1"}


def test_an_intolerant_folder_load_with_rejects_raises_and_publishes_nothing(
    spark, lakehouse, export
):
    with pytest.raises(LoadError) as raised:
        _load_folder(export, spark, lakehouse, {"a.csv": "1", "notes.md": "x"})

    assert raised.value.result.rows_rejected == 1
    assert _folder_contents(lakehouse) == {}


def test_staging_is_reissued_so_a_run_never_republishes_the_last_one(
    spark, lakehouse, export
):
    """Staging belongs to the object, and a run starts from nothing it did not
    itself produce — otherwise a replacement never retires anything."""

    _load_folder(export, spark, lakehouse, {"a.csv": "1", "b.csv": "2"})

    result = _load_folder(export, spark, lakehouse, {"a.csv": "1"})

    assert result.rows_read == 1
    assert _folder_contents(lakehouse) == {"a.csv": "1"}


def test_a_non_incremental_folder_may_not_name_explicit_deletes(
    spark, lakehouse, export, deployed
):
    """It is replaced whole, so absence from staging is what retires a file."""

    import importlib

    path = deployed / "Sales__Export.py"
    path.write_text(
        EXPORT_MODULE.replace("return str(staging), []", "return str(staging), ['a.csv']"),
        encoding="utf-8",
    )
    module = importlib.reload(importlib.import_module("Sales__Export"))

    with pytest.raises(LoadError, match="cannot name explicit deletes"):
        _load_folder(module.Sales__Export, spark, lakehouse, {"b.csv": "1"})


def test_a_delete_that_escapes_the_folder_is_refused(spark, lakehouse, export, deployed):
    """A load may only touch its own destination."""

    import importlib

    path = deployed / "Sales__Export.py"
    path.write_text(
        EXPORT_MODULE.replace("Incremental: false", "Incremental: true").replace(
            "return str(staging), []", "return str(staging), ['../../escape.csv']"
        ),
        encoding="utf-8",
    )
    module = importlib.reload(importlib.import_module("Sales__Export"))

    with pytest.raises(LoadError, match="traverse out of the folder"):
        _load_folder(module.Sales__Export, spark, lakehouse, {"a.csv": "1"})


def test_a_folder_that_returns_someone_elses_directory_is_refused(
    spark, lakehouse, deployed, tmp_path
):
    """Weaver publishes the tree it issued and validated, not one it was handed."""

    import importlib

    path = deployed / "Sales__Export.py"
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    path.write_text(
        EXPORT_MODULE.replace(
            "return str(staging), []", f"return {str(elsewhere)!r}, []"
        ),
        encoding="utf-8",
    )
    module = importlib.reload(importlib.import_module("Sales__Export"))

    with pytest.raises(LoadError, match="not the staging folder Weaver issued"):
        _load_folder(module.Sales__Export, spark, lakehouse, {})
