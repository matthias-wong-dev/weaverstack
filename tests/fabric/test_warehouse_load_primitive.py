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

The outcomes match what a Lakehouse load produces, deliberately: two engines,
one set of load semantics. If they disagree, the semantics have diverged.

**One estate, and one execution per sequence.** Every round trip to a Warehouse
costs seconds, so what this file is careful about is not how many assertions it
makes but how many times it makes the engine do something. Two things follow,
and both are visible in the shape below.

The table and its procedure are installed once for the module rather than per
test. Installing them is not a claim any test here makes — it is the premise all
of them share — and a two-phase procedure install is one of the more expensive
things in the suite.

And a *sequence* runs once, whatever number of claims are about it. "A second
run updates only what changed" and "an unchanged row keeps its original update
time" are two questions about one load-then-load-again; asking the Warehouse to
do it twice does not make either answer better. So each sequence below runs its
loads, **captures everything its claims need at the moment it finishes**, and
hands back a snapshot. Capturing rather than leaving the tests to query later is
what makes the sharing safe: the sequences share one table, so a snapshot taken
afterwards would describe whichever sequence ran last.

The ordinary path goes further and runs as a chain — seed, update, shrink —
because each of those steps *is* the next one's starting state. Rejection keeps
its own sequences, because refusing and tolerating are a different subject and
neither follows from the other.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any

import pytest
from support.weaver_test import weaver_test

from weaver.declaration import read_source_document
from weaver.declaration.model import WAREHOUSE
from weaver.declaration.tsql_load import RESULT_PARAMETERS
from weaver.runtime import LoadResult
from weaver.runtime.load_contract import (
    REASON_BLANK_PK,
    REASON_DUPLICATE_PK,
    REJECTION_REASON,
)

SCHEMA = "DWG"

#: The ordinary object, and the static one. Separate names, because both estates
#: live for the whole module and a shared name would mean whichever was built
#: second dropped the other out from under its own tests.
OBJECT = "LoadCustomer"
STATIC_OBJECT = "LoadStatic"


def _source(object_name: str, *, static: bool = False) -> str:
    static_line = "\nStatic: true\n" if static else ""
    return f"""/*
Table ID: {SCHEMA}.{object_name}

Description: Customers.

Lineage: The sales system.

Primary key: Customer id
{static_line}
Identity: Customer key

Schema:
  Customer id: varchar(50)
  Customer name: varchar(200)
*/
select [Customer id], [Customer name] from [{SCHEMA}].[{object_name}Raw]
"""


CLEAN = [("c1", "One"), ("c2", "Two")]
CHANGED = [("c1", "One"), ("c2", "Changed")]
SHRUNK = [("c1", "One")]
REJECTABLE = CLEAN + [(None, "NoKey"), ("   ", "Blank"), ("c4", "A"), ("c4", "B")]


# --- the estate ---------------------------------------------------------------


@dataclass(frozen=True)
class Estate:
    """One installed table and procedure, and the executor that reaches them."""

    executor: Any
    object_name: str

    @property
    def raw(self) -> str:
        return f"{self.object_name}Raw"


def _install(executor, object_name: str, *, static: bool) -> Estate:
    document = read_source_document(
        f"{SCHEMA}.{object_name}.sql",
        _source(object_name, static=static).encode("utf-8"),
        WAREHOUSE,
    )
    executor.execute_script(
        f"if schema_id(N'{SCHEMA}') is null exec('create schema [{SCHEMA}]');"
        "if schema_id(N'_') is null exec('create schema [_]');"
    )
    estate = Estate(executor, object_name)
    _drop(estate)
    executor.execute_script(
        f"create table [{SCHEMA}].[{estate.raw}] "
        "([Customer id] varchar(50) null, [Customer name] varchar(200) null);"
    )
    executor.execute_script(document.create_ddl().content)
    executor.execute_script(document.create_load().payload.decode("utf-8"))
    return estate


@pytest.fixture(scope="module")
def estate(clean_disposable_warehouse):
    """The built table and its installed load, from the generators themselves.

    Both come from `create_ddl()` and `create_load()` rather than from
    hand-written SQL: a fixture that built the table by hand would prove the
    procedure works against a table Weaver does not actually generate.
    """

    built = _install(clean_disposable_warehouse.executor, OBJECT, static=False)
    yield built
    _drop(built)


@pytest.fixture(scope="module")
def static_estate(clean_disposable_warehouse):
    """The same table declared static, under a name of its own."""

    built = _install(clean_disposable_warehouse.executor, STATIC_OBJECT, static=True)
    yield built
    _drop(built)


def _drop(estate: Estate) -> None:
    name = estate.object_name
    estate.executor.execute_script(
        f"drop procedure if exists [_].[Load {SCHEMA}.{name}];\n"
        + "\n".join(
            f"if object_id(N'{SCHEMA}.{name}{suffix}', N'U') is not null "
            f"drop table [{SCHEMA}].[{name}{suffix}];"
            for suffix in ("_Reject", "_Upsert", "_Staging", "")
        )
        + f"\nif object_id(N'{SCHEMA}.{estate.raw}', N'U') is not null "
        f"drop table [{SCHEMA}].[{estate.raw}];"
    )


def _reset(estate: Estate) -> None:
    """Empty the target and its evidence, without rebuilding either.

    A sequence has to start from a known state, and dropping and recreating the
    table and procedure would be the obvious way to get one — and would put the
    module's most expensive statement back into every sequence. Deleting rows is
    the same starting state for every claim here, none of which is about a table
    that has never existed.
    """

    name = estate.object_name
    estate.executor.execute_script(
        f"delete from [{SCHEMA}].[{name}];\n"
        f"delete from [{SCHEMA}].[{estate.raw}];\n"
        + "\n".join(
            f"if object_id(N'{SCHEMA}.{name}{suffix}', N'U') is not null "
            f"drop table [{SCHEMA}].[{name}{suffix}];"
            for suffix in ("_Reject", "_Upsert", "_Staging")
        )
    )


def _source_rows(estate: Estate, rows) -> None:
    executor = estate.executor
    executor.execute_script(f"delete from [{SCHEMA}].[{estate.raw}];")
    if rows:
        values = ", ".join(
            "(" + ", ".join("null" if v is None else f"'{v}'" for v in row) + ")"
            for row in rows
        )
        executor.execute_script(
            f"insert into [{SCHEMA}].[{estate.raw}] "
            f"([Customer id], [Customer name]) values {values};"
        )


def _load(estate: Estate, *, fault_tolerant: bool) -> LoadResult:
    return LoadResult.from_row(
        estate.executor.call_procedure(
            f"[_].[Load {SCHEMA}.{estate.object_name}]",
            inputs=(("fault_tolerant", 1 if fault_tolerant else 0),),
            outputs=RESULT_PARAMETERS,
        )
    )


def _contents(estate: Estate):
    rows = estate.executor.query(
        f"select [Customer id], [Customer name] from [{SCHEMA}].[{estate.object_name}] "
        "order by [Customer id];"
    )
    return [(row["Customer id"], row["Customer name"]) for row in rows]


def _leftovers(estate: Estate) -> int:
    rows = estate.executor.query(
        f"select count(*) as n from sys.tables "
        f"where schema_id = schema_id(N'{SCHEMA}') "
        f"and name like '{estate.object_name}[_]%';"
    )
    return rows[0]["n"]


@dataclass(frozen=True)
class Ran:
    """What one sequence produced, read while it was still the estate's state."""

    result: LoadResult
    contents: list = field(default_factory=list)
    extra: dict = field(default_factory=dict)


# --- what only Fabric can answer ---------------------------------------------


@pytest.fixture(scope="module")
def ordinary(estate):
    """The ordinary life of a loaded table: seed it, change it, shrink it.

    One chain rather than three, because each step's *base* is the step before
    it. Run separately, the update case would first have to re-seed and the
    shrink case would have to re-seed and update again — three loads bought to
    reach states two earlier loads had already produced.

    Every step captures what its own claims need before the next one runs, so
    each claim still reads the estate at the moment it is about. What the chain
    costs is that a broken step takes the later ones with it; what it buys is
    three loads instead of five, and on a Warehouse that is a minute.
    """

    _reset(estate)

    _source_rows(estate, CLEAN)
    seeded = _load(estate, fault_tolerant=False)
    identities = estate.executor.query(
        f"select count(*) as n, count(distinct [Customer key]) as distinct_keys "
        f"from [{SCHEMA}].[{estate.object_name}];"
    )[0]
    procedures = estate.executor.query(
        f"select name from sys.procedures where name = N'Load {SCHEMA}.{OBJECT}';"
    )
    first = Ran(
        result=seeded,
        contents=_contents(estate),
        extra={
            "rows": identities["n"],
            "distinct_keys": identities["distinct_keys"],
            "leftovers": _leftovers(estate),
            "procedures": [str(row["name"]) for row in procedures],
        },
    )

    _source_rows(estate, CHANGED)
    updated = _load(estate, fault_tolerant=False)
    audit = estate.executor.query(
        f"select [Customer id], case when [Row insert datetime] = "
        f"[Row update datetime] then 1 else 0 end as untouched "
        f"from [{SCHEMA}].[{estate.object_name}] order by [Customer id];"
    )
    second = Ran(
        result=updated,
        contents=_contents(estate),
        extra={"audit": [(row["Customer id"], row["untouched"]) for row in audit]},
    )

    _source_rows(estate, SHRUNK)
    shrunk = _load(estate, fault_tolerant=False)
    third = Ran(result=shrunk, contents=_contents(estate))

    return SimpleNamespace(seeded=first, updated=second, shrunk=third)


@weaver_test(remote=True)
def test_the_generated_procedure_installs_and_is_callable(ordinary):
    """The two-phase installer ran: it read sys.columns and created the procedure."""

    assert ordinary.seeded.extra["procedures"] == [f"Load {SCHEMA}.{OBJECT}"]


@weaver_test(remote=True)
def test_the_load_generates_identities_without_being_given_them(ordinary):
    """The engine assigns them, so the load never names the column.

    The values are Fabric's to choose — not 1, 2, 3, and not consecutive — so
    what is asserted is that every row got a distinct one.
    """

    assert ordinary.seeded.extra["rows"] == 2
    assert ordinary.seeded.extra["distinct_keys"] == 2


# --- the load semantics ------------------------------------------------------


@weaver_test(remote=True)
def test_the_load_inserts_the_rows_its_query_produced(ordinary):
    seeded = ordinary.seeded

    assert seeded.result.succeeded is True
    assert (seeded.result.rows_read, seeded.result.rows_inserted) == (2, 2)
    assert seeded.contents == CLEAN


@weaver_test(remote=True)
def test_a_clean_run_tidies_its_intermediate_tables_away(ordinary):
    """They are evidence, and a run that rejected nothing produced none."""

    assert ordinary.seeded.result.succeeded is True
    assert ordinary.seeded.extra["leftovers"] == 0


@weaver_test(remote=True)
def test_a_second_run_updates_only_what_changed(ordinary):
    updated = ordinary.updated

    assert (updated.result.rows_inserted, updated.result.rows_updated) == (0, 1)
    assert updated.contents == CHANGED


@weaver_test(remote=True)
def test_an_unchanged_row_keeps_its_original_update_time(ordinary):
    assert ordinary.updated.extra["audit"] == [("c1", 1), ("c2", 0)]


@weaver_test(remote=True)
def test_a_non_incremental_run_deletes_rows_the_source_stopped_producing(ordinary):
    """The source stopped producing c2, so the target stops holding it."""

    assert ordinary.shrunk.result.rows_deleted == 1
    assert ordinary.shrunk.contents == SHRUNK


# --- rejection and fault tolerance -------------------------------------------


@pytest.fixture(scope="module")
def refused(estate):
    """A clean load, then an intolerant one over a source that rejects."""

    _reset(estate)
    _source_rows(estate, CLEAN)
    _load(estate, fault_tolerant=False)

    _source_rows(estate, REJECTABLE)
    with pytest.raises(Exception, match="rows were rejected") as raised:
        _load(estate, fault_tolerant=False)
    return Ran(result=None, contents=_contents(estate), extra={"raised": raised.value})


@weaver_test(remote=True)
def test_an_intolerant_run_with_rejects_raises_and_leaves_the_target_untouched(refused):
    """`exec [_].[Load S.N]` fails the way `.load()` does.

    The procedure throws rather than returning a row saying `succeeded = 0`, so
    a caller does not have to special-case which primitive it is driving.
    """

    assert "rows were rejected" in str(refused.extra["raised"])
    assert refused.contents == CLEAN


@pytest.fixture(scope="module")
def tolerated(estate):
    """One tolerant load over a rejecting source, and the evidence it kept."""

    _reset(estate)
    _source_rows(estate, REJECTABLE)
    result = _load(estate, fault_tolerant=True)

    # Read here because the reject table is this run's evidence, and the next
    # sequence's `_reset` takes it away.
    reasons = estate.executor.query(
        f"select distinct [{REJECTION_REASON}] from "
        f"[{SCHEMA}].[{estate.object_name}_Reject];"
    )
    return Ran(
        result=result,
        contents=_contents(estate),
        extra={"reasons": {str(row[REJECTION_REASON]) for row in reasons}},
    )


@weaver_test(remote=True)
def test_a_tolerant_run_loads_the_valid_rows_and_still_reports_failure(tolerated):
    """Tolerating rejects changes what is written, never what is reported."""

    assert tolerated.result.succeeded is False
    assert tolerated.result.rows_rejected == 3
    assert tolerated.result.rows_inserted == 3
    assert tolerated.contents == [("c1", "One"), ("c2", "Two"), ("c4", "A")]


@weaver_test(remote=True)
def test_the_rejected_rows_survive_with_their_reason(tolerated):
    """A count says something went wrong and nothing about what."""

    # Tolerated, not refused — and that distinction is a finding rather than a
    # convenience. Fabric's DDL is transactional, so the `throw` an intolerant
    # run raises rolls the batch back and takes the reject table with it. On the
    # Warehouse the evidence therefore survives only the tolerant path, which
    # sits awkwardly beside the plan's "preserve the rejection evidence" and is
    # reported rather than worked around here.
    assert tolerated.extra["reasons"] == {REASON_BLANK_PK, REASON_DUPLICATE_PK}


# --- static -------------------------------------------------------------------
#
# The fourth authored form, and the only one whose static behaviour is written in
# SQL rather than in Python. The gate is *inside* the procedure — see
# `weaver.declaration.tsql_load._static_gate` — so it has to be executed to be
# proved, and only a Warehouse can execute it. What the generator emits is
# asserted cheaply in `tests/test_static_load_declaration.py`; this is the half
# that needs an engine.


@pytest.fixture(scope="module")
def static_run(static_estate):
    """A static load into an empty target, then a second over a changed source."""

    _reset(static_estate)
    _source_rows(static_estate, CLEAN)
    seed = _load(static_estate, fault_tolerant=False)
    seeded_contents = _contents(static_estate)

    _source_rows(static_estate, [("c9", "Different")])
    again = _load(static_estate, fault_tolerant=False)
    return Ran(
        result=again,
        contents=_contents(static_estate),
        extra={
            "seed": seed,
            "seeded_contents": seeded_contents,
            "leftovers": _leftovers(static_estate),
        },
    )


@weaver_test(remote=True)
def test_a_static_warehouse_load_seeds_an_empty_target(static_run):
    seed = static_run.extra["seed"]

    assert seed.succeeded is True
    assert seed.rows_inserted == 2
    assert static_run.extra["seeded_contents"] == CLEAN


@weaver_test(remote=True)
def test_a_second_static_warehouse_load_is_a_successful_no_op(static_run):
    """The source is changed between the runs and the target does not move.

    Which is the whole claim: the procedure returned without reading its query,
    so what the source now says never reached staging.
    """

    result = static_run.result

    assert result.succeeded is True
    assert (
        result.rows_read,
        result.rows_inserted,
        result.rows_updated,
        result.rows_deleted,
        result.rows_rejected,
    ) == (0, 0, 0, 0, 0)
    assert static_run.contents == CLEAN


@weaver_test(remote=True)
def test_a_static_no_op_leaves_no_intermediate_tables_behind(static_run):
    """It returned before staging, so there was never anything to tidy."""

    assert static_run.extra["leftovers"] == 0
