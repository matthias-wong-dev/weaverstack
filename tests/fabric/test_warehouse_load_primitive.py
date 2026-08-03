"""The generated Warehouse load procedure, executed against a real Fabric Warehouse.

A *primitive* test: the table is built from ``create_ddl()``, the procedure is
installed from ``create_load()``, and the procedure is then executed directly.
No bundle is planned, no installer runs and no orchestrator exists — the claim
is that ``exec [_].[Load S.N]`` loads correctly on its own.

Fabric is the only place several of these can be answered. Whether the engine
accepts an identity column, whether it accepts the generated procedure at all,
and what it does with a two-phase installer reading ``sys.columns`` are its
answers, not ours — which is why the semantics are established here rather than
inferred from a local approximation.

The outcomes match ``tests/spark/test_spark_load_primitive.py`` deliberately.
Two engines, one set of load semantics; if the two files disagree, the semantics
have diverged.
"""

from __future__ import annotations

import pytest

from weaver.declaration import read_source_document
from weaver.declaration.model import WAREHOUSE
from weaver.runtime import LoadResult

pytestmark = pytest.mark.fabric

SCHEMA = "DWG"

SOURCE = f"""/*
Table ID: {SCHEMA}.LoadCustomer

Description: Customers.

Lineage: The sales system.

Primary key: Customer id

Identity: Customer key

Schema:
  Customer id: varchar(50)
  Customer name: varchar(200)
*/
select [Customer id], [Customer name] from [{SCHEMA}].[LoadRaw]
"""

CLEAN = [("c1", "One"), ("c2", "Two")]
REJECTABLE = CLEAN + [(None, "NoKey"), ("   ", "Blank"), ("c4", "A"), ("c4", "B")]


@pytest.fixture(scope="module")
def document():
    return read_source_document(
        f"{SCHEMA}.LoadCustomer.sql", SOURCE.encode("utf-8"), WAREHOUSE
    )


@pytest.fixture
def estate(clean_disposable_warehouse, document):
    """The built table and its installed load, from the generators themselves.

    Both come from `create_ddl()` and `create_load()` rather than from
    hand-written SQL: a fixture that built the table by hand would prove the
    procedure works against a table Weaver does not actually generate.
    """

    executor = clean_disposable_warehouse.executor
    executor.execute_script(
        f"if schema_id(N'{SCHEMA}') is null exec('create schema [{SCHEMA}]');"
        "if schema_id(N'_') is null exec('create schema [_]');"
    )
    _drop(executor)
    executor.execute_script(
        f"create table [{SCHEMA}].[LoadRaw] "
        "([Customer id] varchar(50) null, [Customer name] varchar(200) null);"
    )
    executor.execute_script(document.create_ddl().content)
    executor.execute_script(document.create_load().payload.decode("utf-8"))
    yield executor
    _drop(executor)


def _drop(executor) -> None:
    executor.execute_script(
        f"drop procedure if exists [_].[Load {SCHEMA}.LoadCustomer];\n"
        + "\n".join(
            f"if object_id(N'{SCHEMA}.LoadCustomer{suffix}', N'U') is not null "
            f"drop table [{SCHEMA}].[LoadCustomer{suffix}];"
            for suffix in ("_Reject", "_Upsert", "_Staging", "")
        )
        + f"\nif object_id(N'{SCHEMA}.LoadRaw', N'U') is not null "
        f"drop table [{SCHEMA}].[LoadRaw];"
    )


def _source_rows(executor, rows) -> None:
    executor.execute_script(f"delete from [{SCHEMA}].[LoadRaw];")
    if rows:
        values = ", ".join(
            "(" + ", ".join("null" if v is None else f"'{v}'" for v in row) + ")"
            for row in rows
        )
        executor.execute_script(
            f"insert into [{SCHEMA}].[LoadRaw] "
            f"([Customer id], [Customer name]) values {values};"
        )


def _load(executor, *, fault_tolerant: bool) -> LoadResult:
    rows = executor.query(
        f"exec [_].[Load {SCHEMA}.LoadCustomer] "
        f"@fault_tolerant = {1 if fault_tolerant else 0};"
    )
    return LoadResult.from_row(rows[0])


def _contents(executor):
    rows = executor.query(
        f"select [Customer id], [Customer name] from [{SCHEMA}].[LoadCustomer] "
        "order by [Customer id];"
    )
    return [(row["Customer id"], row["Customer name"]) for row in rows]


# --- what only Fabric can answer ---------------------------------------------


def test_the_generated_procedure_installs_and_is_callable(estate):
    """The two-phase installer ran: it read sys.columns and created the procedure."""

    rows = estate.query(
        f"select name from sys.procedures where name = N'Load {SCHEMA}.LoadCustomer';"
    )

    assert [str(row["name"]) for row in rows] == [f"Load {SCHEMA}.LoadCustomer"]


def test_the_load_generates_identities_without_being_given_them(estate):
    """The engine assigns them, so the load never names the column.

    The values are Fabric's to choose — not 1, 2, 3, and not consecutive — so
    what is asserted is that every row got a distinct one.
    """

    _source_rows(estate, CLEAN)
    _load(estate, fault_tolerant=False)

    rows = estate.query(
        f"select count(*) as n, count(distinct [Customer key]) as distinct_keys "
        f"from [{SCHEMA}].[LoadCustomer];"
    )

    assert rows[0]["n"] == 2
    assert rows[0]["distinct_keys"] == 2


# --- the load semantics ------------------------------------------------------


def test_the_load_inserts_the_rows_its_query_produced(estate):
    _source_rows(estate, CLEAN)

    result = _load(estate, fault_tolerant=False)

    assert result.succeeded is True
    assert (result.rows_read, result.rows_inserted) == (2, 2)
    assert _contents(estate) == CLEAN


def test_a_second_run_updates_only_what_changed(estate):
    _source_rows(estate, CLEAN)
    _load(estate, fault_tolerant=False)

    _source_rows(estate, [("c1", "One"), ("c2", "Changed")])
    result = _load(estate, fault_tolerant=False)

    assert (result.rows_inserted, result.rows_updated) == (0, 1)
    assert _contents(estate) == [("c1", "One"), ("c2", "Changed")]


def test_an_unchanged_row_keeps_its_original_update_time(estate):
    _source_rows(estate, CLEAN)
    _load(estate, fault_tolerant=False)
    _source_rows(estate, [("c1", "One"), ("c2", "Changed")])
    _load(estate, fault_tolerant=False)

    rows = estate.query(
        f"select [Customer id], case when [Row insert datetime] = "
        f"[Row update datetime] then 1 else 0 end as untouched "
        f"from [{SCHEMA}].[LoadCustomer] order by [Customer id];"
    )

    assert [(row["Customer id"], row["untouched"]) for row in rows] == [
        ("c1", 1),
        ("c2", 0),
    ]


def test_a_non_incremental_run_deletes_rows_the_source_stopped_producing(estate):
    _source_rows(estate, CLEAN)
    _load(estate, fault_tolerant=False)

    _source_rows(estate, [("c1", "One")])
    result = _load(estate, fault_tolerant=False)

    assert result.rows_deleted == 1
    assert _contents(estate) == [("c1", "One")]


# --- rejection and fault tolerance -------------------------------------------


def test_an_intolerant_run_with_rejects_leaves_the_target_untouched(estate):
    _source_rows(estate, CLEAN)
    _load(estate, fault_tolerant=False)

    _source_rows(estate, REJECTABLE)
    result = _load(estate, fault_tolerant=False)

    assert result.succeeded is False
    assert result.rows_rejected == 3
    assert (result.rows_inserted, result.rows_updated, result.rows_deleted) == (0, 0, 0)
    assert _contents(estate) == CLEAN


def test_a_tolerant_run_loads_the_valid_rows_and_still_reports_failure(estate):
    """Tolerating rejects changes what is written, never what is reported."""

    _source_rows(estate, REJECTABLE)

    result = _load(estate, fault_tolerant=True)

    assert result.succeeded is False
    assert result.rows_rejected == 3
    assert result.rows_inserted == 3
    assert _contents(estate) == [("c1", "One"), ("c2", "Two"), ("c4", "A")]


def test_the_rejected_rows_survive_with_their_reason(estate):
    """A count says something went wrong and nothing about what."""

    _source_rows(estate, REJECTABLE)
    _load(estate, fault_tolerant=False)

    rows = estate.query(
        f"select distinct [Rejection reason] from [{SCHEMA}].[LoadCustomer_Reject];"
    )

    assert {str(row["Rejection reason"]) for row in rows} == {
        "null primary key",
        "duplicate primary key",
    }


def test_a_clean_run_tidies_its_intermediate_tables_away(estate):
    """They are evidence, and a run that rejected nothing produced none."""

    _source_rows(estate, CLEAN)
    result = _load(estate, fault_tolerant=False)

    leftover = estate.query(
        f"select count(*) as n from sys.tables "
        f"where schema_id = schema_id(N'{SCHEMA}') "
        f"and name like 'LoadCustomer[_]%';"
    )

    assert result.succeeded is True
    assert leftover[0]["n"] == 0
