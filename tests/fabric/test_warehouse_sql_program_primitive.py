"""Authored Warehouse T-SQL, as a program, executed against a real Warehouse.

``test_tsql_program_declaration.py`` establishes what a body *means* — which
``SELECT`` is staging, which names the keys to retire, what is setup. It proves
none of it runs, because it deliberately parses no T-SQL: the Warehouse is the
only authority on that.

So this is the other half, and it is deliberately small. Three seams, each one a
place where an authoring shape and a generated artefact meet and could plausibly
disagree with the engine:

.. code-block:: text

    a complex staging query    CTE, join, nested predicate — built and loaded
    authored setup             SELECT INTO #Working, then a query over it
    two result queries         staging, and the keys an incremental load retires

The last is the new capability and gets the most attention, because "absence
does not delete, an explicit key does" is a claim about two behaviours at once:
the row that stays and the row that goes. Both are asserted in the same load.

Everything comes from ``create_ddl()`` and ``create_load()``. Building the table
by hand would prove the procedure works against a table Weaver does not generate.
"""

from __future__ import annotations

import pytest

from weaver.declaration import read_source_document
from weaver.declaration.model import WAREHOUSE
from weaver.runtime import LoadResult

pytestmark = [pytest.mark.fabric, pytest.mark.remote]

SCHEMA = "DWG"

#: A staging query with something to go wrong in it: a CTE, a join, a nested
#: predicate and a final SELECT that is none of those. The build has to guard
#: every one of those SELECTs and divert only the outermost.
COMPLEX = f"""/*
Table ID: {SCHEMA}.ProgramComplex

Description: Customers with their order totals.

Lineage: The sales system.

Primary key: Customer id
*/
with active as (
    select c.[Customer id], c.[Customer name]
    from [{SCHEMA}].[ProgramCustomer] as c
    where c.[Active] = 1
)
select a.[Customer id]
     , a.[Customer name]
     , sum(o.[Amount]) as [Total]
from active as a
join [{SCHEMA}].[ProgramOrder] as o on o.[Customer id] = a.[Customer id]
where o.[Amount] > (
    select min(x.[Amount]) - 1 from [{SCHEMA}].[ProgramOrder] as x
)
group by a.[Customer id], a.[Customer name]
"""

#: Setup that names its own destination, and a query over what it produced. The
#: working table has to exist by the time the second statement runs, in the
#: build and in the load, which are two different executions of the same body.
SETUP_THEN_QUERY = f"""/*
Table ID: {SCHEMA}.ProgramWorking

Description: Customers, staged through a working table.

Lineage: The sales system.

Primary key: Customer id
*/
select c.[Customer id], c.[Customer name]
into #Working
from [{SCHEMA}].[ProgramCustomer] as c
where c.[Active] = 1;

select w.[Customer id], w.[Customer name]
from #Working as w
"""

#: The two-query contract. The staging query is a window — an incremental
#: source, so what it omits is not retired — and the second query is the only
#: thing that can retire anything.
TWO_QUERY = f"""/*
Table ID: {SCHEMA}.ProgramRetire

Description: Customers, with retirement stated explicitly.

Lineage: The sales system.

Primary key: Customer id

Incremental: true
*/
select c.[Customer id], c.[Customer name]
from [{SCHEMA}].[ProgramCustomer] as c
where c.[Active] = 1;

select r.[Customer id]
from [{SCHEMA}].[ProgramRetirement] as r
"""


def _document(name: str, source: str):
    return read_source_document(
        f"{SCHEMA}.{name}.sql", source.encode("utf-8"), WAREHOUSE
    )


def _drop_object(executor, name: str) -> None:
    executor.execute_script(
        f"drop procedure if exists [_].[Load {SCHEMA}.{name}];\n"
        + "\n".join(
            f"if object_id(N'{SCHEMA}.{name}{suffix}', N'U') is not null "
            f"drop table [{SCHEMA}].[{name}{suffix}];"
            for suffix in ("_Reject", "_Upsert", "_Delete", "_Staging", "")
        )
    )


@pytest.fixture(scope="module")
def warehouse(clean_disposable_warehouse):
    """The schemas and source tables every case here reads from.

    Module-scoped and shared: each test installs its own object and empties the
    sources it uses, so one wipe serves all of them.
    """

    executor = clean_disposable_warehouse.executor
    executor.execute_script(
        f"if schema_id(N'{SCHEMA}') is null exec('create schema [{SCHEMA}]');"
        "if schema_id(N'_') is null exec('create schema [_]');"
    )
    for table, columns in (
        (
            "ProgramCustomer",
            "[Customer id] varchar(50) null, [Customer name] varchar(200) null, "
            "[Active] int null",
        ),
        ("ProgramOrder", "[Customer id] varchar(50) null, [Amount] int null"),
        ("ProgramRetirement", "[Customer id] varchar(50) null"),
    ):
        executor.execute_script(
            f"if object_id(N'{SCHEMA}.{table}', N'U') is not null "
            f"drop table [{SCHEMA}].[{table}];\n"
            f"create table [{SCHEMA}].[{table}] ({columns});"
        )
    yield executor
    for table in ("ProgramCustomer", "ProgramOrder", "ProgramRetirement"):
        executor.execute_script(
            f"if object_id(N'{SCHEMA}.{table}', N'U') is not null "
            f"drop table [{SCHEMA}].[{table}];"
        )


def _install(executor, name: str, source: str):
    document = _document(name, source)
    _drop_object(executor, name)
    executor.execute_script(document.create_ddl().content)
    executor.execute_script(document.create_load().payload.decode("utf-8"))
    return document


def _rows(executor, table: str, columns: str, values: list[tuple]) -> None:
    executor.execute_script(f"delete from [{SCHEMA}].[{table}];")
    if not values:
        return
    literals = ", ".join(
        "(" + ", ".join("null" if v is None else _literal(v) for v in row) + ")"
        for row in values
    )
    executor.execute_script(
        f"insert into [{SCHEMA}].[{table}] ({columns}) values {literals};"
    )


def _literal(value) -> str:
    return str(value) if isinstance(value, int) else "'" + str(value).replace("'", "''") + "'"


def _load(executor, name: str) -> LoadResult:
    rows = executor.query(f"exec [_].[Load {SCHEMA}.{name}] @fault_tolerant = 0;")
    return LoadResult.from_row(rows[0])


def _contents(executor, name: str, columns: str = "[Customer id], [Customer name]"):
    rows = executor.query(
        f"select {columns} from [{SCHEMA}].[{name}] order by [Customer id];"
    )
    return [tuple(row[column.strip(" []")] for column in columns.split(",")) for row in rows]


# --- a complex staging query ---------------------------------------------------


def test_a_cte_join_and_nested_predicate_build_and_load(warehouse):
    """The gnarly shape, end to end: shape-only build, then a real load.

    The build has to guard the CTE's SELECT, the outer SELECT and the one inside
    the WHERE, and divert only the last of those into its shape table — while
    still producing a table the load's generated procedure can fill.
    """

    _install(warehouse, "ProgramComplex", COMPLEX)
    _rows(
        warehouse,
        "ProgramCustomer",
        "[Customer id], [Customer name], [Active]",
        [("c1", "One", 1), ("c2", "Two", 1), ("c3", "Gone", 0)],
    )
    _rows(
        warehouse,
        "ProgramOrder",
        "[Customer id], [Amount]",
        [("c1", 10), ("c1", 5), ("c2", 20), ("c3", 100)],
    )

    result = _load(warehouse, "ProgramComplex")

    assert result.succeeded is True
    assert (result.rows_read, result.rows_inserted) == (2, 2)
    assert _contents(
        warehouse, "ProgramComplex", "[Customer id], [Customer name], [Total]"
    ) == [("c1", "One", 15), ("c2", "Two", 20)]

    _drop_object(warehouse, "ProgramComplex")


def test_the_build_infers_the_columns_the_staging_query_produces(warehouse):
    """Including the aggregate, which exists only in the outer SELECT."""

    _install(warehouse, "ProgramComplex", COMPLEX)

    rows = warehouse.query(
        "select name from sys.columns "
        f"where [object_id] = object_id(N'{SCHEMA}.ProgramComplex') "
        "order by column_id;"
    )

    assert [str(row["name"]) for row in rows][:3] == [
        "Customer id",
        "Customer name",
        "Total",
    ]

    _drop_object(warehouse, "ProgramComplex")


# --- authored setup ------------------------------------------------------------


def test_setup_runs_and_the_query_over_it_becomes_staging(warehouse):
    """``SELECT INTO #Working`` is working, not a result — in both executions."""

    _install(warehouse, "ProgramWorking", SETUP_THEN_QUERY)
    _rows(
        warehouse,
        "ProgramCustomer",
        "[Customer id], [Customer name], [Active]",
        [("c1", "One", 1), ("c2", "Two", 1), ("c3", "Gone", 0)],
    )

    result = _load(warehouse, "ProgramWorking")

    assert result.succeeded is True
    assert (result.rows_read, result.rows_inserted) == (2, 2)
    assert _contents(warehouse, "ProgramWorking") == [("c1", "One"), ("c2", "Two")]

    _drop_object(warehouse, "ProgramWorking")


# --- two result queries --------------------------------------------------------


@pytest.fixture
def retiring(warehouse):
    """The two-query table, loaded once with three customers and none retired."""

    _install(warehouse, "ProgramRetire", TWO_QUERY)
    _rows(
        warehouse,
        "ProgramCustomer",
        "[Customer id], [Customer name], [Active]",
        [("c1", "One", 1), ("c2", "Two", 1), ("c3", "Three", 1)],
    )
    _rows(warehouse, "ProgramRetirement", "[Customer id]", [])

    first = _load(warehouse, "ProgramRetire")
    assert first.rows_inserted == 3, first.error_message

    yield warehouse
    _drop_object(warehouse, "ProgramRetire")


def test_absence_does_not_delete_but_a_named_key_does(retiring):
    """The whole contract, in one load.

    The staging query returns only c1, so c2 is absent — and absence from an
    incremental window is not a retirement, so c2 stays. c3 is absent *and*
    named by the delete query, and that is what removes it.
    """

    _rows(
        retiring,
        "ProgramCustomer",
        "[Customer id], [Customer name], [Active]",
        [("c1", "Renamed", 1), ("c2", "Two", 0), ("c3", "Three", 0)],
    )
    _rows(retiring, "ProgramRetirement", "[Customer id]", [("c3",)])

    result = _load(retiring, "ProgramRetire")

    assert result.succeeded is True, result.error_message
    assert (result.rows_read, result.rows_updated, result.rows_deleted) == (1, 1, 1)
    assert _contents(retiring, "ProgramRetire") == [("c1", "Renamed"), ("c2", "Two")]


def test_a_claim_for_a_key_that_is_not_there_deletes_nothing(retiring):
    """A delete is a report of what happened, not of what was asked for."""

    _rows(
        retiring,
        "ProgramCustomer",
        "[Customer id], [Customer name], [Active]",
        [("c1", "One", 1)],
    )
    _rows(retiring, "ProgramRetirement", "[Customer id]", [("c99",)])

    result = _load(retiring, "ProgramRetire")

    assert result.succeeded is True, result.error_message
    assert result.rows_deleted == 0
    assert _contents(retiring, "ProgramRetire") == [
        ("c1", "One"),
        ("c2", "Two"),
        ("c3", "Three"),
    ]


def test_a_key_claimed_twice_is_one_deletion(retiring):
    """The claim is normalised before it is counted or applied."""

    _rows(
        retiring,
        "ProgramCustomer",
        "[Customer id], [Customer name], [Active]",
        [("c1", "One", 1)],
    )
    _rows(
        retiring,
        "ProgramRetirement",
        "[Customer id]",
        [("c3",), ("c3",), (None,), ("   ",)],
    )

    result = _load(retiring, "ProgramRetire")

    assert result.succeeded is True, result.error_message
    assert result.rows_deleted == 1
    assert _contents(retiring, "ProgramRetire") == [("c1", "One"), ("c2", "Two")]


def test_the_delete_working_table_is_cleaned_up_after_a_clean_run(retiring):
    """It is working, and a run that rejected nothing leaves no evidence."""

    _rows(retiring, "ProgramRetirement", "[Customer id]", [("c3",)])
    _load(retiring, "ProgramRetire")

    rows = retiring.query(
        f"select object_id(N'{SCHEMA}.ProgramRetire_Delete', N'U') as remaining;"
    )

    assert rows[0]["remaining"] is None
