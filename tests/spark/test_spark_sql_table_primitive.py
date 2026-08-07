"""A Spark-SQL-authored table, loaded through the ordinary ``Table.load()``.

The claim these make is *inheritance*, not reimplementation: the module here is
what a build installs for a ``.sql`` table — a class carrying its program — and
what it does when loaded is what a Python-authored table does, because it is the
same ``load()``. So what is asserted is the seam: setup statements really do run
before the queries and in the same session, the first query really does become
staging, the second really does drive deletion, and the resulting table is what
the ordinary load would have produced.

Everything else a load does — rejection, thresholds, audit columns, counts — is
proved once against Python-authored tables in
``test_python_load_primitive.py`` and is deliberately not repeated. If it needed
repeating, ``SparkSqlTable`` would not be delegating.

The module is written to disk and imported, like every other deployed primitive,
because a class defined in this file would read *this* module's docstring as its
contract.
"""

from __future__ import annotations

import importlib
import sys

import pytest

from weaver import lakehouse_for
from weaver.errors import LoadError
from weaver.targets import ItemRef

pytestmark = pytest.mark.spark

TARGET = "Sales_LH"

#: One query: the ordinary shape. A temporary view first, to prove setup runs in
#: the same session the query is resolved in.
SUMMARY_MODULE = '''\
"""
Table ID: Sales.Summary

Description: Customers with their totals.

Lineage: The sales system.

Dependencies: []

Primary key: Customer id

Schema:
  Customer id: string
  Total amount: bigint
"""
from weaver import SparkSqlTable


SQL = r"""
create or replace temporary view live as
select * from source_rows where `Total amount` > 0;

select `Customer id`, `Total amount` from live;
"""


class Sales__Summary(SparkSqlTable):
    sql = SQL
'''

#: Two queries: staging, then the keys to retire. Incremental, because only an
#: incremental table may state its deletes rather than imply them.
INCREMENTAL_MODULE = '''\
"""
Table ID: Sales.Summary

Description: Customers with their totals, loaded incrementally.

Lineage: The sales system.

Dependencies: []

Primary key: Customer id

Incremental: true

Schema:
  Customer id: string
  Total amount: bigint
"""
from weaver import SparkSqlTable


SQL = r"""
select `Customer id`, `Total amount` from source_rows;

select `Customer id` from retired_keys;
"""


class Sales__Summary(SparkSqlTable):
    sql = SQL
'''

#: A delete query returning more than the key — refused before anything is
#: written, because a delete is applied by joining on the key alone.
WIDE_DELETE_MODULE = INCREMENTAL_MODULE.replace(
    "select `Customer id` from retired_keys;",
    "select `Customer id`, 1 as `Total amount` from retired_keys;",
)


@pytest.fixture
def deployed(tmp_path, monkeypatch):
    """Where a deployed runtime tree would hold the generated module."""

    root = tmp_path / "deployed"
    root.mkdir()
    monkeypatch.syspath_prepend(str(root))
    sys.modules.pop("Sales__Summary", None)
    yield root
    sys.modules.pop("Sales__Summary", None)


def _install(deployed, source: str):
    (deployed / "Sales__Summary.py").write_text(source, encoding="utf-8")
    sys.modules.pop("Sales__Summary", None)
    return importlib.import_module("Sales__Summary").Sales__Summary


@pytest.fixture
def lakehouse(lakehouses):
    return lakehouse_for(lakehouses.resolver, ItemRef(TARGET))


@pytest.fixture
def summary(spark, lakehouse):
    """The built target the load writes into, dropped afterwards.

    A load writes into a table a build already made, which is the order the real
    system uses and the reason this is a fixture rather than part of the load.
    """

    schema = lakehouse.destination.qualified_schema("Sales")
    location = lakehouse.destination.schema_location("Sales")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema} LOCATION '{location}'")
    spark.sql(
        f"CREATE TABLE {lakehouse.qualify('Sales', 'Summary')} (\n"
        "  `Customer id` string NOT NULL,\n"
        "  `Total amount` bigint,\n"
        "  `row_insert_datetime` timestamp NOT NULL,\n"
        "  `row_update_datetime` timestamp NOT NULL,\n"
        "  `row_delete_datetime` timestamp NOT NULL\n"
        ") USING delta TBLPROPERTIES ('delta.columnMapping.mode' = 'name')"
    )
    yield lakehouse.qualify("Sales", "Summary")
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")


def _source(spark, rows):
    spark.createDataFrame(
        rows, "`Customer id` string, `Total amount` bigint"
    ).createOrReplaceTempView("source_rows")


def _retired(spark, keys):
    spark.createDataFrame(
        [(key,) for key in keys], "`Customer id` string"
    ).createOrReplaceTempView("retired_keys")


def _contents(spark, target):
    return {
        (row["Customer id"], row["Total amount"])
        for row in spark.sql(
            f"SELECT `Customer id`, `Total amount` FROM {target} "
            "WHERE `row_delete_datetime` > current_timestamp()"
        ).collect()
    }


# --- one query ----------------------------------------------------------------


def test_a_one_query_program_stages_what_its_final_query_returns(
    spark, lakehouse, summary, deployed
):
    _source(spark, [("C1", 10), ("C2", 20), ("C3", 0)])
    cls = _install(deployed, SUMMARY_MODULE)

    result = cls(spark, lakehouse=lakehouse).load()

    assert result.succeeded
    # C3 is filtered out by the temporary view, which proves the setup statement
    # ran in the session the query was resolved in.
    assert _contents(spark, summary) == {("C1", 10), ("C2", 20)}
    assert (result.rows_read, result.rows_inserted) == (2, 2)


def test_read_returns_the_staging_frame_and_no_delete_claim(
    spark, lakehouse, summary, deployed
):
    _source(spark, [("C1", 10)])
    cls = _install(deployed, SUMMARY_MODULE)

    staging, deletes = cls(spark, lakehouse=lakehouse).read()

    assert sorted(staging.columns) == ["Customer id", "Total amount"]
    assert deletes is None


def test_a_second_load_updates_rather_than_duplicates(
    spark, lakehouse, summary, deployed
):
    cls = _install(deployed, SUMMARY_MODULE)
    _source(spark, [("C1", 10), ("C2", 20)])
    cls(spark, lakehouse=lakehouse).load()

    _source(spark, [("C1", 99), ("C2", 20)])
    result = cls(spark, lakehouse=lakehouse).load()

    assert _contents(spark, summary) == {("C1", 99), ("C2", 20)}
    assert (result.rows_inserted, result.rows_updated) == (0, 1)


def test_a_non_incremental_program_retires_what_it_stopped_staging(
    spark, lakehouse, summary, deployed
):
    cls = _install(deployed, SUMMARY_MODULE)
    _source(spark, [("C1", 10), ("C2", 20)])
    cls(spark, lakehouse=lakehouse).load()

    _source(spark, [("C1", 10)])
    result = cls(spark, lakehouse=lakehouse).load()

    assert _contents(spark, summary) == {("C1", 10)}
    assert result.rows_deleted == 1


# --- two queries --------------------------------------------------------------


def test_a_two_query_program_deletes_exactly_the_keys_its_second_query_named(
    spark, lakehouse, summary, deployed
):
    cls = _install(deployed, INCREMENTAL_MODULE)
    _source(spark, [("C1", 10), ("C2", 20), ("C3", 30)])
    _retired(spark, [])
    cls(spark, lakehouse=lakehouse).load()

    _source(spark, [("C1", 11)])
    _retired(spark, ["C3"])
    result = cls(spark, lakehouse=lakehouse).load()

    # C3 went because it was named; C2 stayed although it was absent, which is
    # what Incremental means.
    assert _contents(spark, summary) == {("C1", 11), ("C2", 20)}
    assert result.rows_deleted == 1


def test_read_returns_both_frames_when_the_program_has_two_queries(
    spark, lakehouse, summary, deployed
):
    cls = _install(deployed, INCREMENTAL_MODULE)
    _source(spark, [("C1", 10)])
    _retired(spark, ["C9"])

    staging, deletes = cls(spark, lakehouse=lakehouse).read()

    assert sorted(staging.columns) == ["Customer id", "Total amount"]
    assert deletes.columns == ["Customer id"]


def test_a_delete_query_wider_than_the_key_is_refused_with_the_target_untouched(
    spark, lakehouse, summary, deployed
):
    cls = _install(deployed, INCREMENTAL_MODULE)
    _source(spark, [("C1", 10)])
    _retired(spark, [])
    cls(spark, lakehouse=lakehouse).load()

    wide = _install(deployed, WIDE_DELETE_MODULE)
    _source(spark, [("C1", 99)])
    _retired(spark, ["C1"])

    with pytest.raises(LoadError, match="exactly the primary key"):
        wide(spark, lakehouse=lakehouse).load()

    assert _contents(spark, summary) == {("C1", 10)}
