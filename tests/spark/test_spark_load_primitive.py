"""The generated Spark SQL load program, executed against real Delta files.

A *primitive* test: the program is generated from a document, addressed for a
destination, and run. No repository is parsed, no bundle is built and no
installer runs — the claim is that the generated program loads correctly by
itself, which is what makes everything downstream free to just install it.

The outcomes asserted here are deliberately the same ones
``tests/fabric/test_warehouse_load_primitive.py`` asserts against Fabric. Two
engines, one set of load semantics: if these two files ever disagree, the
semantics have diverged and the plan's promise of common behaviour is broken.
"""

from __future__ import annotations

import pytest

from weaver import ItemRef, lakehouse_for
from weaver.declaration import read_source_document
from weaver.declaration.model import LAKEHOUSE
from weaver.runtime.load_contract import (
    REASON_BLANK_PK,
    REASON_DUPLICATE_PK,
    REJECTION_REASON,
)
from weaver.runtime.spark_load import run_load_program
from weaver.build_bundle.executors.spark_case import exact_identifier_case
from weaver.spark import SparkCatalogue

pytestmark = pytest.mark.spark

TARGET = "Sales_LH"

SOURCE = """/*
Table ID: Sales.Customer

Description: Customers.

Lineage: $Sales.Raw

Dependencies: []

Primary key: Customer id

Schema:
  Customer id: string
  Customer name: string
*/
select `Customer id`, `Customer name` from Sales.Raw
"""


@pytest.fixture
def lakehouse(lakehouses):
    return lakehouse_for(lakehouses.resolver, ItemRef(TARGET))


@pytest.fixture
def estate(spark, lakehouse):
    """A built target and a source table, as build leaves them for a load.

    The schema carries a LOCATION so its tables live under this test's own
    `tmp_path`; without one Spark pins them to its warehouse directory and the
    next run meets the last run's files.
    """

    destination = lakehouse.destination
    schema = destination.qualified_schema("Sales")
    mapping = "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')"
    # In the same exact-case scope the real executors use. Weaver identities are
    # case-exact and this destination preserves them, so a table created outside
    # that scope lands lowercased and the generated program cannot find it.
    with exact_identifier_case(
        spark, enabled=destination.preserve_table_identifier_case
    ):
        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {schema} "
            f"LOCATION '{destination.schema_location('Sales')}'"
        )
        spark.sql(
            f"CREATE TABLE {destination.qualify('Sales', 'Raw')} "
            f"(`Customer id` string, `Customer name` string) USING delta {mapping}"
        )
        spark.sql(
            f"CREATE TABLE {destination.qualify('Sales', 'Customer')} (\n"
            "  `Customer id` string NOT NULL,\n"
            "  `Customer name` string,\n"
            "  `row_insert_datetime` timestamp NOT NULL,\n"
            "  `row_update_datetime` timestamp NOT NULL,\n"
            "  `row_delete_datetime` timestamp NOT NULL\n"
            f") USING delta {mapping}"
        )
        yield destination
        spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")


@pytest.fixture
def program(spark, estate):
    """The generated program, addressed for this destination as install does."""

    document = read_source_document(
        "Sales.Customer.sql", SOURCE.encode("utf-8"), LAKEHOUSE
    )
    payload = document.create_load().payload.decode("utf-8")
    return SparkCatalogue(spark, estate).expand(payload)


def _source_rows(spark, estate, rows):
    spark.sql(f"DELETE FROM {estate.qualify('Sales', 'Raw')}")
    if rows:
        values = ", ".join(
            "(" + ", ".join("NULL" if v is None else f"'{v}'" for v in row) + ")"
            for row in rows
        )
        spark.sql(f"INSERT INTO {estate.qualify('Sales', 'Raw')} VALUES {values}")


def _contents(spark, estate):
    frame = spark.sql(
        f"SELECT `Customer id`, `Customer name` "
        f"FROM {estate.qualify('Sales', 'Customer')} ORDER BY 1"
    )
    return [(row["Customer id"], row["Customer name"]) for row in frame.collect()]


CLEAN = [("c1", "One"), ("c2", "Two")]
REJECTABLE = CLEAN + [(None, "NoKey"), ("   ", "Blank"), ("c4", "A"), ("c4", "B")]


def test_the_program_inserts_the_rows_its_query_produced(spark, estate, program):
    _source_rows(spark, estate, CLEAN)

    result = run_load_program(spark, program)

    assert result.succeeded is True
    assert (result.rows_read, result.rows_inserted) == (2, 2)
    assert _contents(spark, estate) == CLEAN


def test_a_second_run_updates_only_what_changed(spark, estate, program):
    _source_rows(spark, estate, CLEAN)
    run_load_program(spark, program)

    _source_rows(spark, estate, [("c1", "One"), ("c2", "Changed")])
    result = run_load_program(spark, program)

    assert (result.rows_inserted, result.rows_updated) == (0, 1)
    assert _contents(spark, estate) == [("c1", "One"), ("c2", "Changed")]


def test_a_non_incremental_run_deletes_rows_the_source_stopped_producing(
    spark, estate, program
):
    _source_rows(spark, estate, CLEAN)
    run_load_program(spark, program)

    _source_rows(spark, estate, [("c1", "One")])
    result = run_load_program(spark, program)

    assert result.rows_deleted == 1
    assert _contents(spark, estate) == [("c1", "One")]


def test_an_intolerant_run_with_rejects_leaves_the_target_untouched(
    spark, estate, program
):
    """The case that must never write, and the reason the delete is a merge.

    An empty valid set makes every write a no-op; expressing the delete as
    `NOT MATCHED BY SOURCE` would instead have matched every target row.
    """

    _source_rows(spark, estate, CLEAN)
    run_load_program(spark, program)

    _source_rows(spark, estate, REJECTABLE)
    result = run_load_program(spark, program, fault_tolerant=False)

    assert result.succeeded is False
    assert result.rows_rejected == 3
    assert (result.rows_inserted, result.rows_updated, result.rows_deleted) == (0, 0, 0)
    assert _contents(spark, estate) == CLEAN


def test_a_tolerant_run_loads_valid_rows_and_still_reports_failure(
    spark, estate, program
):
    _source_rows(spark, estate, REJECTABLE)

    result = run_load_program(spark, program, fault_tolerant=True)

    assert result.succeeded is False
    assert result.rows_rejected == 3
    assert result.rows_inserted == 3
    assert _contents(spark, estate) == [("c1", "One"), ("c2", "Two"), ("c4", "A")]


def test_the_rejected_rows_are_kept_with_their_reason(spark, estate, program):
    _source_rows(spark, estate, REJECTABLE)
    run_load_program(spark, program, fault_tolerant=True)

    rejects = spark.sql(
        f"SELECT * FROM {estate.qualify('Sales', 'Customer_Reject')}"
    ).collect()

    assert {row[REJECTION_REASON] for row in rejects} == {
        REASON_BLANK_PK,
        REASON_DUPLICATE_PK,
    }
    # The author's columns and the reason — not Weaver's internal rank column.
    assert set(rejects[0].asDict()) == {
        "Customer id",
        "Customer name",
        REJECTION_REASON,
    }


def test_an_unchanged_row_keeps_its_original_update_time(spark, estate, program):
    _source_rows(spark, estate, CLEAN)
    run_load_program(spark, program)
    _source_rows(spark, estate, [("c1", "One"), ("c2", "Changed")])
    run_load_program(spark, program)

    rows = spark.sql(
        f"SELECT `Customer id`, "
        f"`row_insert_datetime` = `row_update_datetime` AS untouched "
        f"FROM {estate.qualify('Sales', 'Customer')} ORDER BY 1"
    ).collect()

    assert [(row["Customer id"], row["untouched"]) for row in rows] == [
        ("c1", True),
        ("c2", False),
    ]
