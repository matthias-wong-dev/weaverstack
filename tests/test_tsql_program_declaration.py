"""What an authored Warehouse T-SQL body means, and what it may not mean.

The Warehouse counterpart of ``test_spark_sql_program_declaration.py``. A
Warehouse table's body is read once and that reading serves both ends — the
build materialises the staging query in shape-only form, the load stages it and
deletes by whatever comes next — so these are the rules that decide which
``SELECT`` is which.

The hard part is not the contract but the recognition. T-SQL does not require
statement terminators, so "where does one statement end" cannot be answered by
splitting on ``;``: a ``SELECT`` beginning a query has to be told apart from one
that is the tail of an ``INSERT``, a branch of a ``UNION``, the body of a
``WITH``, a subquery inside a predicate, or the working half of a
``SELECT … INTO``. Most of what follows is that distinction.

Pure Python throughout. Nothing here needs Fabric, because nothing here parses
T-SQL: whether each statement is *valid* is the Warehouse's answer, given when
it runs. That the generated artefacts then execute is proved in
``tests/fabric/test_warehouse_load_primitive.py``.
"""

from __future__ import annotations

import pytest

from weaver.declaration import read_source_document
from weaver.declaration.model import WAREHOUSE
from weaver.declaration.tsql_program import (
    parse_tsql_program,
    validate_query_contract,
)
from weaver.errors import DiscoveryError, LoadError

TABLE = """/*
Table ID: Sales.Customer

Description: Customers.

Lineage: The sales system.

Dependencies: []

Primary key: Customer id
{extra}*/
{body}"""


def _document(body: str, *, extra: str = ""):
    source = TABLE.format(body=body, extra=f"{extra}\n" if extra else "")
    return read_source_document("Sales.Customer.sql", source.encode("utf-8"), WAREHOUSE)


def _program(body: str):
    return parse_tsql_program(body, what="Sales.Customer", error=LoadError)


def _validate(body: str, *, primary_key=("Customer id",), incremental=True) -> None:
    validate_query_contract(
        _program(body),
        what="Sales.Customer",
        primary_key=primary_key,
        incremental=incremental,
        error=LoadError,
    )


def _staging(body: str) -> str:
    return _program(body).staging.sql


# --- what counts as one query -------------------------------------------------


def test_a_select_is_the_query():
    program = _program("select [Customer id] from [Sales].[Customer]")

    assert [one.produces_result for one in program.statements] == [True]
    assert program.staging.sql == "select [Customer id] from [Sales].[Customer]"


def test_a_cte_is_one_query_including_its_with():
    body = """with recent as (
    select [Customer id] from [Sales].[Order] where [Order date] > '2026-01-01'
)
select r.[Customer id] from recent as r"""

    program = _program(body)

    assert len(program.queries) == 1
    assert program.staging.sql.startswith("with recent as (")
    assert program.staging.sql.endswith("from recent as r")


def test_a_nested_select_does_not_make_a_second_query():
    body = """select c.[Customer id]
from [Sales].[Customer] as c
where c.[Customer id] in (select o.[Customer id] from [Sales].[Order] as o)"""

    assert len(_program(body).queries) == 1


def test_a_union_is_one_query():
    body = """select [Customer id] from [Sales].[Customer]
union all
select [Customer id] from [Sales].[Prospect]"""

    program = _program(body)

    assert len(program.queries) == 1
    assert "union all" in program.staging.sql


def test_a_subquery_in_from_does_not_make_a_second_query():
    body = """select s.[Customer id]
from (select [Customer id] from [Sales].[Customer]) as s"""

    assert len(_program(body).queries) == 1


# --- what is setup instead ----------------------------------------------------


def test_select_into_a_temp_table_is_setup():
    """It names its own destination, so its rows never come back to Weaver."""

    body = """select [Customer id]
into #Working
from [Sales].[Customer];

select [Customer id] from #Working"""

    program = _program(body)

    assert [one.produces_result for one in program.statements] == [False, True]
    assert program.staging.sql == "select [Customer id] from #Working"


def test_insert_select_is_setup():
    body = """insert into #Working ([Customer id])
select [Customer id] from [Sales].[Customer];

select [Customer id] from #Working"""

    program = _program(body)

    assert [one.produces_result for one in program.statements] == [False, True]


def test_declare_set_update_and_delete_are_setup():
    body = """declare @cutoff date;
set @cutoff = '2026-01-01';
update #Working set [Customer name] = trim([Customer name]);
delete from #Working where [Customer id] is null;

select [Customer id] from #Working"""

    program = _program(body)

    assert [one.produces_result for one in program.statements] == [
        False,
        False,
        False,
        False,
        True,
    ]
    assert len(program.setup) == 4


def test_setup_keeps_the_order_it_was_written_in():
    """Setup written between two queries was written there deliberately."""

    body = """declare @cutoff date;

select [Customer id] from [Sales].[Customer] where [Changed] > @cutoff;

select [Customer id] into #Retired from [Sales].[Retirement];

select [Customer id] from #Retired"""

    program = _program(body)

    assert [one.produces_result for one in program.statements] == [
        False,
        True,
        False,
        True,
    ]


def test_comments_do_not_change_classification():
    """A ``select`` inside a comment is prose, and a comment is not a statement."""

    body = """-- This comment mentions select * from [Nope]
/* and so does this: select 1 */
select [Customer id] from [Sales].[Customer]

-- and a trailing one"""

    program = _program(body)

    assert len(program.queries) == 1
    assert program.setup == ()
    assert program.staging.sql.startswith("select [Customer id]")


def test_a_comment_between_two_queries_is_not_a_statement():
    body = """select [Customer id] from #Working;

-- the rows this load retires
select [Customer id] from #Retired"""

    program = _program(body)

    assert [one.produces_result for one in program.statements] == [True, True]


def test_a_statement_run_without_terminators_still_finds_its_query():
    """T-SQL does not require ';', so the recognition cannot depend on one."""

    body = """declare @cutoff date = '2026-01-01'
set @cutoff = dateadd(day, -7, @cutoff)
select [Customer id] from [Sales].[Customer] where [Changed] >= @cutoff"""

    program = _program(body)

    assert len(program.queries) == 1
    assert program.staging.sql.startswith("select [Customer id]")


# --- dynamic SQL --------------------------------------------------------------


def test_dynamic_sql_is_allowed_as_setup():
    body = """declare @sql nvarchar(max);

set @sql = N'insert into #Working select [Customer id] from [Sales].[Customer]';
exec sp_executesql @sql;

select [Customer id] from #Working"""

    program = _program(body)

    assert [one.produces_result for one in program.statements] == [
        False,
        False,
        False,
        True,
    ]
    assert program.staging.sql == "select [Customer id] from #Working"


def test_exec_is_allowed_as_setup():
    body = """exec [Sales].[RefreshWorking];

select [Customer id] from #Working"""

    assert len(_program(body).queries) == 1


def test_a_result_hidden_inside_dynamic_sql_is_not_a_query():
    """Weaver does not read the text inside EXEC, so it cannot stage it."""

    body = "exec sp_executesql N'select [Customer id] from [Sales].[Customer]';"

    program = _program(body)

    assert program.queries == ()
    assert program.staging is None
    with pytest.raises(LoadError, match="visible SELECT"):
        _validate(body)


def test_dynamic_setup_can_precede_two_visible_queries():
    body = """exec sp_executesql N'insert into #Working select 1';

select [Customer id] from #Working;

select [Customer id] from #Retired"""

    program = _program(body)

    assert len(program.queries) == 2
    assert program.deletes.sql == "select [Customer id] from #Retired"


# --- the query contract -------------------------------------------------------


def test_one_query_stages_and_deletes_nothing():
    program = _program("select [Customer id] from [Sales].[Customer]")

    assert program.staging is not None
    assert program.deletes is None


def test_two_queries_are_staging_then_deletes():
    body = """select [Customer id], [Customer name] from #Working;

select [Customer id] from #Retired"""

    program = _program(body)

    assert program.staging.sql == "select [Customer id], [Customer name] from #Working"
    assert program.deletes.sql == "select [Customer id] from #Retired"


def test_three_queries_are_refused():
    body = """select 1 as [Customer id];

select 2 as [Customer id];

select 3 as [Customer id]"""

    with pytest.raises(LoadError, match="3 statements produce results"):
        _validate(body)


def test_no_visible_query_is_refused():
    with pytest.raises(LoadError, match="visible SELECT"):
        _validate("select [Customer id] into #Working from [Sales].[Customer];")


def test_a_second_query_needs_a_primary_key():
    body = "select 1 as a;\n\nselect 1 as a;"

    with pytest.raises(LoadError, match="needs a primary key"):
        _validate(body, primary_key=())


def test_a_second_query_needs_incremental():
    body = "select 1 as a;\n\nselect 1 as a;"

    with pytest.raises(LoadError, match="non-incremental table cannot name"):
        _validate(body, incremental=False)


def test_one_query_needs_neither():
    _validate("select 1 as a;", primary_key=(), incremental=False)


# --- GO -----------------------------------------------------------------------


def test_go_is_refused():
    """The load runs the body inside a procedure, where GO cannot appear."""

    body = """select [Customer id] from [Sales].[Customer]
GO
select [Customer id] from #Retired"""

    with pytest.raises(LoadError, match="batch separator"):
        _program(body)


def test_a_column_called_go_is_not_a_batch_separator():
    program = _program("select [go], t.go from [Sales].[Customer] as t")

    assert len(program.queries) == 1


# --- the same rules, at repository parse --------------------------------------


def test_the_repository_accepts_setup_and_one_query():
    document = _document(
        """select [Customer id]
into #Working
from [Sales].[Customer];

select [Customer id] from #Working"""
    )

    assert document.document.kind == "Table"


def test_the_repository_accepts_two_queries_for_an_incremental_keyed_table():
    document = _document(
        """select [Customer id] from #Working;

select [Customer id] from #Retired""",
        extra="Incremental: true\n",
    )

    assert document.document.is_incremental


def test_the_repository_refuses_a_second_query_without_incremental():
    with pytest.raises(DiscoveryError, match="non-incremental table cannot name"):
        _document(
            """select [Customer id] from #Working;

select [Customer id] from #Retired"""
        )


def test_the_repository_refuses_three_queries():
    with pytest.raises(DiscoveryError, match="produce results"):
        _document(
            """select 1 as [Customer id];

select 2 as [Customer id];

select 3 as [Customer id]""",
            extra="Incremental: true\n",
        )


def test_the_repository_refuses_a_body_with_no_visible_query():
    with pytest.raises(DiscoveryError, match="visible SELECT"):
        _document("exec sp_executesql N'select [Customer id] from [Sales].[Customer]';")


def test_the_repository_accepts_dynamic_setup_before_a_visible_query():
    """Unknowable result-set analysis is not a reason to refuse a body."""

    document = _document(
        """declare @sql nvarchar(max);
set @sql = N'insert into #Working select 1';
exec sp_executesql @sql;

select [Customer id] from #Working"""
    )

    assert document.document.kind == "Table"
