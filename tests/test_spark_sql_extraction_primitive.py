"""``SparkSqlTable.read()`` — running an authored program and returning a pair.

The extraction half of a SQL-authored table, proved without Spark. What is
asserted here is what the primitive *does with* a session — which statements it
submits, in what order, and what it hands back — and a recording double answers
that exactly. Whether Spark then executes the SQL correctly is Spark's claim,
made in ``tests/fabric/test_spark_table_lakehouse_boundary.py``.

The load itself is deliberately absent. A SQL-authored table reaches
``load_table`` through the ordinary ``Table.load()``, so rejection, thresholds
and counts are proved once, against Python-authored tables, and inherited.
"""

from __future__ import annotations

import pytest
from support.weaver_test import weaver_test

from weaver.declaration.metadata import ObjectId
from weaver.errors import LoadError
from weaver.runtime.load_contract import LoadContract
from weaver.runtime.spark_sql_table import read_spark_sql

KEY = ("Customer id",)


class _Frame:
    """What a session hands back: something with columns, and its own identity."""

    def __init__(self, statement: str, columns: tuple[str, ...] = KEY) -> None:
        self.statement = statement
        self.columns = list(columns)


class _Session:
    """A Spark session double that records what it was asked to run."""

    def __init__(self, columns: dict[str, tuple[str, ...]] | None = None) -> None:
        self.submitted: list[str] = []
        self._columns = columns or {}

    def sql(self, statement: str) -> _Frame:
        self.submitted.append(statement)
        for marker, columns in self._columns.items():
            if marker in statement:
                return _Frame(statement, columns)
        return _Frame(statement)


def _contract(*, primary_key=KEY, incremental: bool = False) -> LoadContract:
    return LoadContract(
        object_id=ObjectId(schema="Sales", object="Summary"),
        primary_key=primary_key,
        incremental=incremental,
    )


# --- what reaches the session -------------------------------------------------


@weaver_test()
def test_every_statement_is_submitted_once_in_source_order():
    session = _Session()

    read_spark_sql(
        session,
        sql=(
            "create or replace temporary view recent as select 1 as x;\n"
            "cache table recent;\n"
            "select `Customer id` from recent;"
        ),
        contract=_contract(),
    )

    assert session.submitted == [
        "create or replace temporary view recent as select 1 as x",
        "cache table recent",
        "select `Customer id` from recent",
    ]


@weaver_test()
def test_statements_are_submitted_without_their_separators():
    session = _Session()

    read_spark_sql(session, sql="select `Customer id` from t;", contract=_contract())

    assert session.submitted == ["select `Customer id` from t"]


# --- what comes back ----------------------------------------------------------


@weaver_test()
def test_one_query_stages_and_claims_no_deletes():
    session = _Session()

    staging, deletes = read_spark_sql(
        session,
        sql="create or replace temporary view v as select 1;\nselect * from v;",
        contract=_contract(),
    )

    assert staging.statement == "select * from v"
    # None rather than an empty frame: the program made no claim about deletes,
    # and load_table already knows what that means for both kinds of table.
    assert deletes is None


@weaver_test()
def test_two_queries_stage_and_name_the_keys_to_delete():
    session = _Session()

    staging, deletes = read_spark_sql(
        session,
        sql="select * from source;\nselect `Customer id` from gone;",
        contract=_contract(incremental=True),
    )

    assert staging.statement == "select * from source"
    assert deletes.statement == "select `Customer id` from gone"


@weaver_test()
def test_a_setup_statement_between_the_queries_does_not_become_one():
    session = _Session()

    staging, deletes = read_spark_sql(
        session,
        sql=(
            "select * from source;\n"
            "create or replace temporary view gone as select 1;\n"
            "select `Customer id` from gone;"
        ),
        contract=_contract(incremental=True),
    )

    assert staging.statement == "select * from source"
    assert deletes.statement == "select `Customer id` from gone"


# --- what is refused ----------------------------------------------------------


@weaver_test()
def test_a_primitive_with_no_program_is_refused_by_name():
    with pytest.raises(LoadError, match="carries no program"):
        read_spark_sql(_Session(), sql="", contract=_contract())


@weaver_test()
def test_an_unterminated_program_is_refused_before_anything_runs():
    session = _Session()

    with pytest.raises(LoadError, match="must end with ';'"):
        read_spark_sql(session, sql="select 1 as x", contract=_contract())

    assert session.submitted == []


@weaver_test()
def test_a_program_that_produces_nothing_is_refused_before_anything_runs():
    session = _Session()

    with pytest.raises(LoadError, match="must end in a query"):
        read_spark_sql(
            session,
            sql="create or replace temporary view v as select 1;",
            contract=_contract(),
        )

    assert session.submitted == []


@weaver_test()
def test_a_non_incremental_table_may_not_name_explicit_deletes():
    with pytest.raises(LoadError, match="non-incremental table cannot name"):
        read_spark_sql(
            _Session(),
            sql="select * from source;\nselect `Customer id` from gone;",
            contract=_contract(incremental=False),
        )


@weaver_test()
def test_a_delete_query_returning_more_than_the_key_is_refused():
    session = _Session(columns={"gone": ("Customer id", "Amount")})

    with pytest.raises(LoadError, match="exactly the primary key"):
        read_spark_sql(
            session,
            sql="select * from source;\nselect * from gone;",
            contract=_contract(incremental=True),
        )


@weaver_test()
def test_a_delete_query_missing_part_of_the_key_is_refused():
    session = _Session(columns={"gone": ("Customer id",)})

    with pytest.raises(LoadError, match="exactly the primary key"):
        read_spark_sql(
            session,
            sql="select * from source;\nselect * from gone;",
            contract=_contract(
                primary_key=("Customer id", "Order id"), incremental=True
            ),
        )


@weaver_test()
def test_a_delete_query_naming_the_key_in_another_order_is_accepted():
    session = _Session(columns={"gone": ("Order id", "Customer id")})

    _staging, deletes = read_spark_sql(
        session,
        sql="select * from source;\nselect * from gone;",
        contract=_contract(primary_key=("Customer id", "Order id"), incremental=True),
    )

    assert deletes.columns == ["Order id", "Customer id"]
