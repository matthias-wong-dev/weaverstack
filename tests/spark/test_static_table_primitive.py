"""``Static: true`` on a table, against a real Delta target.

Two authored forms reach one implementation — a Spark-SQL-authored table *is* a
Python table by the time it loads — so both are proved here, and proving them
separately is the point: the claim is that they behave identically, and a single
test could not have said so.

What each asserts is the same three things, and the third is the one that needs
a counter rather than an assertion about outcomes:

.. code-block:: text

    an empty target      loads normally
    a populated target   returns a successful no-op
    and the source       was never executed

"Nothing happened" is only convincing if something would have been observable
had it happened, which is why the sources here count their own invocations and
the SQL one reads a view that is emptied between runs.
"""

from __future__ import annotations

import importlib
import sys

import pytest

from weaver import lakehouse_for
from weaver.targets import ItemRef

pytestmark = pytest.mark.spark

TARGET = "Sales_LH"

#: A Python-authored static table. ``read()`` records that it ran, which is how
#: "the source was not executed" becomes an assertion rather than an inference.
PYTHON_MODULE = '''\
"""
Table ID: Sales.Country

Description: The country reference list.

Lineage: Seeded once.

Primary key: Code

Static: true

Schema:
  Code: string
  Name: string
"""
from weaver import Table


class Sales__Country(Table):
    reads = 0
    rows = []

    def read(self):
        type(self).reads += 1
        return self.spark.createDataFrame(
            type(self).rows, "`Code` string, `Name` string"
        ), None
'''

#: The same declaration authored in SQL, compiled as a build would compile it.
SQL_MODULE = '''\
# Weaver generated load — Sales.Country, from Sales.Country.sql
"""
Table ID: Sales.Country

Description: The country reference list.

Lineage: Seeded once.

Dependencies: []

Primary key: Code

Static: true

Schema:
  Code: string
  Name: string
"""

from weaver import SparkSqlTable


SQL = """\\
select `Code`, `Name` from country_source;
"""


class Sales__Country(SparkSqlTable):
    sql = SQL
'''


@pytest.fixture
def deployed(tmp_path, monkeypatch):
    root = tmp_path / "deployed"
    root.mkdir()
    monkeypatch.syspath_prepend(str(root))
    sys.modules.pop("Sales__Country", None)
    yield root
    sys.modules.pop("Sales__Country", None)


def _install(deployed, source: str):
    (deployed / "Sales__Country.py").write_text(source, encoding="utf-8")
    sys.modules.pop("Sales__Country", None)
    return importlib.import_module("Sales__Country").Sales__Country


@pytest.fixture
def lakehouse(lakehouses):
    return lakehouse_for(lakehouses.resolver, ItemRef(TARGET))


@pytest.fixture
def country(spark, lakehouse):
    """The built target a static load seeds, dropped afterwards."""

    schema = lakehouse.destination.qualified_schema("Sales")
    location = lakehouse.destination.schema_location("Sales")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema} LOCATION '{location}'")
    spark.sql(
        f"CREATE TABLE {lakehouse.qualify('Sales', 'Country')} (\n"
        "  `Code` string NOT NULL,\n"
        "  `Name` string,\n"
        "  `row_insert_datetime` timestamp NOT NULL,\n"
        "  `row_update_datetime` timestamp NOT NULL,\n"
        "  `row_delete_datetime` timestamp NOT NULL\n"
        ") USING delta TBLPROPERTIES ('delta.columnMapping.mode' = 'name')"
    )
    yield lakehouse.qualify("Sales", "Country")
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")


def _contents(spark, target):
    return {
        (row["Code"], row["Name"])
        for row in spark.sql(f"SELECT `Code`, `Name` FROM {target}").collect()
    }


def _source(spark, rows):
    spark.createDataFrame(rows, "`Code` string, `Name` string").createOrReplaceTempView(
        "country_source"
    )


# --- a Python-authored static table -------------------------------------------


def test_a_static_python_table_seeds_an_empty_target(spark, lakehouse, country, deployed):
    cls = _install(deployed, PYTHON_MODULE)
    cls.reads = 0
    cls.rows = [("GB", "United Kingdom"), ("NZ", "New Zealand")]

    result = cls(spark, lakehouse=lakehouse).load()

    assert result.succeeded
    assert result.rows_inserted == 2
    assert _contents(spark, country) == {("GB", "United Kingdom"), ("NZ", "New Zealand")}
    assert cls.reads == 1


def test_a_second_load_of_a_static_python_table_is_a_successful_no_op(
    spark, lakehouse, country, deployed
):
    cls = _install(deployed, PYTHON_MODULE)
    cls.reads = 0
    cls.rows = [("GB", "United Kingdom")]
    cls(spark, lakehouse=lakehouse).load()

    cls.rows = [("XX", "Somewhere else")]
    result = cls(spark, lakehouse=lakehouse).load()

    assert result.succeeded
    assert (
        result.rows_read,
        result.rows_inserted,
        result.rows_updated,
        result.rows_deleted,
        result.rows_rejected,
    ) == (0, 0, 0, 0, 0)
    # The source was not consulted the second time, so the changed rows never
    # reached staging — which is what a no-op has to mean.
    assert cls.reads == 1
    assert _contents(spark, country) == {("GB", "United Kingdom")}


def test_a_non_static_python_table_reloads_normally(
    spark, lakehouse, country, deployed
):
    cls = _install(deployed, PYTHON_MODULE.replace("Static: true\n\n", ""))
    cls.reads = 0
    cls.rows = [("GB", "United Kingdom")]
    cls(spark, lakehouse=lakehouse).load()

    cls.rows = [("GB", "Britain")]
    result = cls(spark, lakehouse=lakehouse).load()

    assert cls.reads == 2
    assert result.rows_updated == 1
    assert _contents(spark, country) == {("GB", "Britain")}


# --- a Spark-SQL-authored static table ----------------------------------------


def test_a_static_sql_table_seeds_an_empty_target(spark, lakehouse, country, deployed):
    cls = _install(deployed, SQL_MODULE)
    _source(spark, [("GB", "United Kingdom"), ("NZ", "New Zealand")])

    result = cls(spark, lakehouse=lakehouse).load()

    assert result.succeeded
    assert result.rows_inserted == 2
    assert _contents(spark, country) == {("GB", "United Kingdom"), ("NZ", "New Zealand")}


def test_a_second_load_of_a_static_sql_table_is_a_successful_no_op(
    spark, lakehouse, country, deployed
):
    """The source view is *removed* between the runs.

    A second load that executed the program would fail on a missing relation, so
    succeeding is itself evidence that nothing ran — stronger than counting,
    because the SQL cannot count for itself.
    """

    cls = _install(deployed, SQL_MODULE)
    _source(spark, [("GB", "United Kingdom")])
    cls(spark, lakehouse=lakehouse).load()

    spark.catalog.dropTempView("country_source")
    result = cls(spark, lakehouse=lakehouse).load()

    assert result.succeeded
    assert (result.rows_read, result.rows_inserted) == (0, 0)
    assert _contents(spark, country) == {("GB", "United Kingdom")}


def test_the_two_authored_forms_agree_about_what_static_means(
    spark, lakehouse, country, deployed
):
    """The claim the whole conversion rests on.

    A SQL-authored table and a Python-authored one share ``Table.load()``, so
    behaviour that lives there cannot differ between them. Asserting it keeps
    that true rather than merely intended.
    """

    python = _install(deployed, PYTHON_MODULE)
    python.reads = 0
    python.rows = [("GB", "United Kingdom")]
    first = python(spark, lakehouse=lakehouse).load()
    second = python(spark, lakehouse=lakehouse).load()

    spark.sql(f"DELETE FROM {country}")

    sql = _install(deployed, SQL_MODULE)
    _source(spark, [("GB", "United Kingdom")])
    third = sql(spark, lakehouse=lakehouse).load()
    fourth = sql(spark, lakehouse=lakehouse).load()

    assert (first.rows_inserted, second.rows_inserted) == (1, 0)
    assert (third.rows_inserted, fourth.rows_inserted) == (1, 0)
    assert second.as_row() == fourth.as_row()
