"""What an authored Spark SQL table's body means, and what it may not mean.

A Spark SQL table is installed as an importable primitive whose ``read()``
returns ``(staging, deletes)``, so the body's shape *is* that pair: one query is
staging, two are staging and the keys to delete. These are the rules that make
one body legible as the other, and they are asserted at both ends — the parse
that refuses a build, and the contract check the deployed primitive repeats.

Pure Python throughout. Nothing here needs Spark, because nothing here parses
Spark SQL: statement boundaries are lexical and classification is by what a
statement leads with. Whether each statement is *valid* is Spark's answer, given
when it runs.
"""

from __future__ import annotations

import pytest
from support.weaver_test import weaver_test

from weaver.declaration import read_source_document
from weaver.declaration.model import LAKEHOUSE
from weaver.declaration.spark_sql_program import (
    parse_spark_sql_program,
    validate_query_contract,
)
from weaver.errors import DiscoveryError, LoadError

TABLE = """/*
Table ID: Sales.Summary

Description: Order totals by customer.

Lineage: The sales system.

Dependencies: []

Primary key: Customer id
{extra}
Schema:
  Customer id: string
  Total amount: decimal(18,2)
*/
{body}"""


def _document(body: str, *, extra: str = "") -> object:
    source = TABLE.format(body=body, extra=f"\n{extra}\n" if extra else "\n")
    return read_source_document("Sales.Summary.sql", source.encode("utf-8"), LAKEHOUSE)


def _program(body: str):
    return parse_spark_sql_program(body, what="Sales.Summary", error=LoadError)


def _validate(body: str, *, primary_key=("Customer id",), incremental=False) -> None:
    validate_query_contract(
        _program(body),
        what="Sales.Summary",
        primary_key=primary_key,
        incremental=incremental,
        error=LoadError,
    )


# --- setup and query ----------------------------------------------------------


@weaver_test()
def test_a_select_produces_a_result():
    program = _program("select 1 as x;")

    assert [one.produces_result for one in program.statements] == [True]


@weaver_test()
def test_a_cte_produces_a_result_although_it_does_not_start_with_select():
    program = _program("with c as (select 1 as x) select * from c;")

    assert program.queries and program.queries[0].produces_result


@weaver_test()
def test_a_create_view_as_select_is_setup_although_it_contains_a_select():
    program = _program("create or replace temporary view v as select 1 as x;")

    assert not program.queries
    assert len(program.setup) == 1


@pytest.mark.parametrize(
    "statement",
    [
        "drop view if exists v",
        "cache table t",
        "uncache table t",
        "set spark.sql.shuffle.partitions = 8",
        "insert into t values (1)",
        "refresh table t",
    ],
)
@weaver_test()
def test_ordinary_preamble_statements_are_setup(statement):
    program = _program(f"{statement};\nselect 1 as x;")

    assert [one.produces_result for one in program.statements] == [False, True]


@weaver_test()
def test_setup_and_queries_keep_the_order_they_were_written_in():
    program = _program(
        "create or replace temporary view v as select 1 as x;\n"
        "select * from v;\n"
        "select x from v where false;\n"
    )

    assert [one.sql.split()[0] for one in program.statements] == [
        "create",
        "select",
        "select",
    ]
    assert len(program.queries) == 2


# --- termination --------------------------------------------------------------


@weaver_test()
def test_an_unterminated_final_statement_is_refused():
    with pytest.raises(LoadError, match="must end with ';'"):
        _program("select 1 as x")


@weaver_test()
def test_an_unterminated_statement_is_refused_when_the_repository_is_parsed():
    with pytest.raises(DiscoveryError, match="must end with ';'"):
        _document("select `Customer id`, `Total amount` from Sales.Order")


@weaver_test()
def test_a_terminated_body_parses_as_a_repository_document():
    document = _document("select `Customer id`, `Total amount` from Sales.Order;")

    assert document.document.qualified == "Sales.Summary"


# --- the query-count contract -------------------------------------------------


@weaver_test()
def test_a_body_that_produces_no_rows_is_not_a_table():
    with pytest.raises(LoadError, match="must end in a query"):
        _validate("create or replace temporary view v as select 1 as x;")


@weaver_test()
def test_one_query_is_the_staging_rows():
    _validate("select 1 as x;")


@weaver_test()
def test_two_queries_are_staging_and_the_keys_to_delete():
    _validate(
        "select 1 as x;\nselect 2 as `Customer id`;",
        incremental=True,
    )


@weaver_test()
def test_three_queries_are_ambiguous_and_refused():
    with pytest.raises(LoadError, match="3 statements produce results"):
        _validate(
            "select 1 as x;\nselect 2 as x;\nselect 3 as x;",
            incremental=True,
        )


@weaver_test()
def test_a_delete_query_needs_a_primary_key_to_name_rows_by():
    with pytest.raises(LoadError, match="needs a primary key"):
        _validate(
            "select 1 as x;\nselect 2 as x;",
            primary_key=(),
            incremental=True,
        )


@weaver_test()
def test_a_non_incremental_table_cannot_name_explicit_deletes():
    with pytest.raises(LoadError, match="non-incremental table cannot name"):
        _validate("select 1 as x;\nselect 2 as x;", incremental=False)


@weaver_test()
def test_a_second_query_is_refused_when_the_repository_is_parsed():
    with pytest.raises(DiscoveryError, match="non-incremental table cannot name"):
        _document(
            "select `Customer id`, `Total amount` from Sales.Order;\n"
            "select `Customer id` from Sales.Cancelled;"
        )


@weaver_test()
def test_an_incremental_table_may_declare_its_deletes_in_the_repository():
    document = _document(
        "select `Customer id`, `Total amount` from Sales.Order;\n"
        "select `Customer id` from Sales.Cancelled;",
        extra="Incremental: true",
    )

    assert document.document.is_incremental


# --- what the rule is not -----------------------------------------------------


@weaver_test()
def test_a_setup_statement_between_the_two_queries_is_allowed():
    _validate(
        "select 1 as x;\n"
        "create or replace temporary view gone as select 2 as `Customer id`;\n"
        "select * from gone;",
        incremental=True,
    )


@weaver_test()
def test_a_comment_before_a_query_does_not_make_it_setup():
    program = _program("-- the staging rows\nselect 1 as x;")

    assert len(program.queries) == 1
