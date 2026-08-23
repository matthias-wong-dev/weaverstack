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
from sql_support import (
    PROCEDURE_ITEM,
    forget_bookmark,
    forget_runtime_state,
    install_runtime_references,
)
from support.weaver_test import weaver_test

from weaver.declaration import read_source_document
from weaver.declaration.metadata import ObjectId
from weaver.declaration.model import WAREHOUSE, WeaverItemId
from weaver.declaration.tsql_entry import generate_load_entry
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


#: The logical item the installed procedures belong to. A load procedure is keyed
#: by it — its bookmark row carries the Registry's four-part identity — so it is
#: named here rather than left to a default.
ITEM = WeaverItemId(*PROCEDURE_ITEM)


def _install(executor, object_name: str, catalogue: str, *, static: bool) -> Estate:
    document = read_source_document(
        f"{SCHEMA}.{object_name}.sql",
        _source(object_name, static=static).encode("utf-8"),
        WAREHOUSE,
    )
    executor.execute_script(
        f"if schema_id(N'{SCHEMA}') is null exec('create schema [{SCHEMA}]');"
        "if schema_id(N'_') is null exec('create schema [_]');"
    )
    install_runtime_references(executor, catalogue)
    estate = Estate(executor, object_name)
    _drop(estate)
    executor.execute_script(
        f"create table [{SCHEMA}].[{estate.raw}] "
        "([Customer id] varchar(50) null, [Customer name] varchar(200) null);"
    )
    executor.execute_script(document.create_ddl().content)
    executor.execute_script(document.create_load(item=ITEM).payload.decode("utf-8"))
    # And the entry point over it, because the object's own procedure records
    # nothing: `exec _.[Load]` is what runs it by hand and writes the record.
    executor.execute_script(generate_load_entry(ITEM, [ObjectId(SCHEMA, object_name)]))
    return estate


@pytest.fixture(scope="module")
def estate(clean_disposable_warehouse, fabric_workspace, fabric_initialise_catalogue):
    """The built table and its installed load, from the generators themselves.

    Both come from `create_ddl()` and `create_load()` rather than from
    hand-written SQL: a fixture that built the table by hand would prove the
    procedure works against a table Weaver does not actually generate.

    The catalogue is built because a procedure reads and writes its own
    bookmark: `_.Bookmark` has to be there for the reference to resolve, and
    another module's wipe may have taken the whole `_` schema with it.
    """

    fabric_initialise_catalogue()
    built = _install(
        clean_disposable_warehouse.executor,
        OBJECT,
        fabric_workspace.catalogue_item.name,
        static=False,
    )
    yield built
    _drop(built)


@pytest.fixture(scope="module")
def static_estate(
    clean_disposable_warehouse, fabric_workspace, fabric_initialise_catalogue
):
    """The same table declared static, under a name of its own."""

    fabric_initialise_catalogue()
    built = _install(
        clean_disposable_warehouse.executor,
        STATIC_OBJECT,
        fabric_workspace.catalogue_item.name,
        static=True,
    )
    yield built
    _drop(built)


def _drop(estate: Estate) -> None:
    name = estate.object_name
    estate.executor.execute_script(
        f"drop procedure if exists [_].[Load {SCHEMA}.{name}];\n"
        + "\n".join(
            f"if object_id(N'{SCHEMA}.{name}{suffix}', N'U') is not null "
            f"drop table [{SCHEMA}].[{name}{suffix}];"
            for suffix in ("_Reject", "_Upsert", "_Delete", "_Staging", "")
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
        + forget_runtime_state(SCHEMA, name)
        + "\n".join(
            f"if object_id(N'{SCHEMA}.{name}{suffix}', N'U') is not null "
            f"drop table [{SCHEMA}].[{name}{suffix}];"
            for suffix in ("_Reject", "_Upsert", "_Delete", "_Staging")
        )
    )


def _bookmark(estate) -> object:
    """This object's bookmark, as the catalogue holds it, or None."""

    rows = estate.executor.query(
        "select [Bookmark datetime] as at from [_].[Bookmark] "
        f"where [Item type] = N'{ITEM.item_type}' "
        f"and [Item name] = N'{ITEM.item_name}' "
        f"and [Schema name] = N'{SCHEMA}' "
        f"and [Object name] = N'{estate.object_name}';"
    )
    return rows[0]["at"] if rows else None


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
    """The object's own procedure, which is what an orchestrated run calls."""

    return LoadResult.from_row(
        estate.executor.call_procedure(
            f"[_].[Load {SCHEMA}.{estate.object_name}]",
            inputs=(("fault_tolerant", 1 if fault_tolerant else 0),),
            outputs=RESULT_PARAMETERS,
        )
    )


def _standalone(estate: Estate, *, fault_tolerant: bool = False) -> None:
    """``exec _.[Load]``, which is what a person calls and what records.

    It reports through the catalogue rather than through output parameters: the
    row it wrote is the answer, and reading that back is the claim.
    """

    estate.executor.execute_script(
        f"exec [_].[Load] @object_name = N'{SCHEMA}.{estate.object_name}'"
        f", @fault_tolerant = {1 if fault_tolerant else 0};"
    )


def _status(estate: Estate) -> dict | None:
    """This object's row in ``_.LoadStatus``, as the catalogue holds it."""

    rows = estate.executor.query(
        "select [Result] as result, [Duration milliseconds] as duration "
        "from [_].[LoadStatus] " + _identity_predicate(estate)
    )
    return dict(rows[0]) if rows else None


def _statistics(estate: Estate) -> list:
    rows = estate.executor.query(
        "select [Rows read] as read, [Rows inserted] as inserted, "
        "[Is reload] as reload, [Is static skip] as skip "
        "from [_].[LoadStatistic] " + _identity_predicate(estate)
    )
    return [dict(row) for row in rows]


def _log(estate: Estate) -> list:
    rows = estate.executor.query(
        "select [Task type] as task, [Result] as result, [Target name] as target "
        "from [_].[Log] " + _identity_predicate(estate)
    )
    return [dict(row) for row in rows]


def _identity_predicate(estate: Estate) -> str:
    return (
        f"where [Item type] = N'{ITEM.item_type}' "
        f"and [Item name] = N'{ITEM.item_name}' "
        f"and [Schema name] = N'{SCHEMA}' "
        f"and [Object name] = N'{estate.object_name}'"
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


def _ordinary(estate):
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


@weaver_test(remote=True, resources={"tds"})
def test_the_ordinary_load_lifecycle(estate):
    """Install, seed, update and shrink through the generated procedure."""

    ordinary = _ordinary(estate)
    seeded = ordinary.seeded

    assert seeded.extra["procedures"] == [f"Load {SCHEMA}.{OBJECT}"]
    assert seeded.extra["rows"] == 2
    assert seeded.extra["distinct_keys"] == 2
    assert seeded.result.succeeded is True
    assert (seeded.result.rows_read, seeded.result.rows_inserted) == (2, 2)
    assert seeded.contents == CLEAN
    assert ordinary.seeded.result.succeeded is True
    assert ordinary.seeded.extra["leftovers"] == 0
    updated = ordinary.updated
    assert (updated.result.rows_inserted, updated.result.rows_updated) == (0, 1)
    assert updated.contents == CHANGED
    assert ordinary.updated.extra["audit"] == [("c1", 1), ("c2", 0)]
    assert ordinary.shrunk.result.rows_deleted == 1
    assert ordinary.shrunk.contents == SHRUNK


# --- rejection and fault tolerance -------------------------------------------


def _refused(estate):
    """A clean load, then an intolerant one over a source that rejects."""

    _reset(estate)
    _source_rows(estate, CLEAN)
    _load(estate, fault_tolerant=False)

    _source_rows(estate, REJECTABLE)
    with pytest.raises(Exception, match="rows were rejected") as raised:
        _load(estate, fault_tolerant=False)
    return Ran(result=None, contents=_contents(estate), extra={"raised": raised.value})


@weaver_test(remote=True, resources={"tds"})
def test_an_intolerant_run_with_rejects_raises_and_leaves_the_target_untouched(estate):
    """`exec [_].[Load S.N]` fails the way `.load()` does.

    The procedure throws rather than returning a row saying `succeeded = 0`, so
    a caller does not have to special-case which primitive it is driving.
    """

    refused = _refused(estate)
    assert "rows were rejected" in str(refused.extra["raised"])
    assert refused.contents == CLEAN


def _tolerated(estate):
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


@weaver_test(remote=True, resources={"tds"})
def test_a_tolerant_run_preserves_valid_rows_and_rejection_evidence(estate):
    """Tolerating rejects changes what is written, never what is reported."""

    tolerated = _tolerated(estate)
    assert tolerated.result.succeeded is False
    assert tolerated.result.rows_rejected == 3
    assert tolerated.result.rows_inserted == 3
    assert tolerated.contents == [("c1", "One"), ("c2", "Two"), ("c4", "A")]
    assert tolerated.extra["reasons"] == {REASON_BLANK_PK, REASON_DUPLICATE_PK}


# --- static -------------------------------------------------------------------
#
# The fourth authored form, and the only one whose static behaviour is written in
# SQL rather than in Python. The gate is *inside* the procedure — see
# `weaver.declaration.tsql_load._static_gate` — so it has to be executed to be
# proved, and only a Warehouse can execute it. What the generator emits is
# asserted cheaply in `tests/test_static_load_declaration.py`; this is the half
# that needs an engine.


def _static_run(static_estate):
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
            "bookmark": _bookmark(static_estate),
        },
    )


@weaver_test(remote=True, resources={"tds"})
def test_the_objects_own_procedure_records_nothing(estate):
    """It is an execution primitive: whoever called it owns the record.

    It still reports the instant it began, because that is what a caller
    advances the bookmark to.
    """

    _reset(estate)
    _source_rows(estate, CLEAN)

    result = _load(estate, fault_tolerant=False)

    assert result.succeeded is True
    assert result.bookmark_datetime is not None
    assert _bookmark(estate) is None
    assert _status(estate) is None
    assert _log(estate) == []


@weaver_test(remote=True, resources={"tds"})
def test_the_entry_point_records_a_clean_load_through_the_views(estate):
    """``exec _.[Load]`` writes the whole operational record.

    In every Warehouse but the one the catalogue lives in these tables are views
    over the catalogue's own, so this is also the claim that an insert and an
    update through such a view reach the table behind it. Fabric refuses a plain
    INSERT there and accepts a MERGE's, which is why every write is one.
    """

    _reset(estate)
    _source_rows(estate, CLEAN)

    _standalone(estate)

    assert _bookmark(estate) is not None
    assert _status(estate)["result"] == "Succeeded"
    (logged,) = _log(estate)
    assert logged["task"] == "load"
    assert logged["result"] == "Succeeded"
    # The Warehouse the procedure ran in, taken from the connection rather than
    # baked into the generated statement.
    assert logged["target"]
    (statistic,) = _statistics(estate)
    assert statistic["read"] == len(CLEAN)
    assert statistic["reload"] is False
    assert statistic["skip"] is False


@weaver_test(remote=True, resources={"tds"})
def test_a_second_clean_load_moves_the_bookmark_on(estate):
    """The row is updated in place, which is the half an insert cannot prove."""

    _reset(estate)
    _source_rows(estate, CLEAN)
    _standalone(estate)
    first = _bookmark(estate)

    _source_rows(estate, CHANGED)
    _standalone(estate)
    second = _bookmark(estate)

    assert first is not None and second is not None
    assert second > first
    # And the status is one row per object, updated rather than accumulated,
    # while the statistics accumulate.
    assert _status(estate)["result"] == "Succeeded"
    assert len(_statistics(estate)) == 2


@weaver_test(remote=True, resources={"tds"})
def test_a_load_that_rejected_rows_is_rejected_and_keeps_its_bookmark(estate):
    """It has not read its window, whether or not it was told to tolerate them."""

    _reset(estate)
    _source_rows(estate, CLEAN)
    _standalone(estate)
    clean = _bookmark(estate)

    _source_rows(estate, REJECTABLE)
    _standalone(estate, fault_tolerant=True)

    assert _bookmark(estate) == clean
    assert _status(estate)["result"] == "Rejected"


@weaver_test(remote=True, resources={"tds"})
def test_a_refusal_the_entry_point_caught_is_failed_and_still_recorded(estate):
    """An intolerant refusal throws inside the wrapper and is recorded there.

    Weaver's own refusal ran under Weaver's control and produced an unacceptable
    result, so it is Failed rather than Error — and it leaves a row, because a
    refusal is an outcome and not a reason to leave no record.
    """

    _reset(estate)
    _source_rows(estate, REJECTABLE)

    _standalone(estate, fault_tolerant=False)

    assert _status(estate)["result"] == "Failed"
    assert _bookmark(estate) is None


@weaver_test(remote=True, resources={"tds"})
def test_the_entry_point_refuses_an_object_this_warehouse_does_not_load(estate):
    """Reporting a row for it would put an object in the record the estate lacks."""

    from weaver.sql.errors import SqlError

    with pytest.raises((SqlError, Exception), match="is not a loadable object"):
        estate.executor.execute_script(
            "exec [_].[Load] @object_name = N'Sales.NotAThing';"
        )


@weaver_test(remote=True, resources={"tds"})
def test_the_static_warehouse_load_seeds_once_and_then_is_a_no_op(static_estate):
    static_run = _static_run(static_estate)
    seed = static_run.extra["seed"]

    assert seed.succeeded is True
    assert seed.rows_inserted == 2
    assert static_run.extra["seeded_contents"] == CLEAN
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
    assert static_run.extra["leftovers"] == 0
    # What closed the gate the second time: the bookmark the seed recorded.
    assert static_run.extra["bookmark"] is not None
    assert result.bookmark_datetime is None


# --- declared constraints, executed ------------------------------------------
#
# Nullability and uniqueness are declarations, and what they mean is what the
# engine does with the generated procedure. Two more estates, because the two
# subjects are genuinely different: one is about refusing incoming rows and
# recovering, the other about refusing to write at all.
#
# One test per sequence, as above, with every claim about that sequence in it.
# The sequence is the expensive part and the claims are free.

#: Declared nullable and unique, non-incremental: the recoverable refusals.
CONSTRAINED_OBJECT = "LoadConstrained"

#: Declared unique and incremental: the one refusal that is not recoverable.
MERGE_OBJECT = "LoadMerge"

WIDE_COLUMNS = ("Customer id", "Customer name", "Email", "Region id", "External ref")

WIDE_RAW_DDL = (
    "[Customer id] varchar(50) null, [Customer name] varchar(200) null, "
    "[Email] varchar(100) null, [Region id] int null, [External ref] varchar(30) null"
)


def _wide_source(object_name: str, *, incremental: bool) -> str:
    """One object declaring a key, a required column and two unique keys.

    The second unique key is composite, and ``Email`` is left nullable, so the
    same declaration covers a null that does not claim a value and a tuple that
    does.
    """

    body = (
        "select [Customer id], [Customer name], [Email], [Region id], [External ref] "
        f"from [{SCHEMA}].[{object_name}Raw]"
    )
    if incremental:
        body += f";\n\nselect [Customer id] from [{SCHEMA}].[{object_name}Retire]"
    policy = "\nIncremental: true\n" if incremental else ""
    return f"""/*
Table ID: {SCHEMA}.{object_name}

Description: Customers.

Lineage: The sales system.

Primary key: Customer id

Not null:
  - Customer name

Unique keys:
  - Email
  - Region id, External ref
{policy}
Schema:
  Customer id: varchar(50)
  Customer name: varchar(200)
  Email: varchar(100)
  Region id: int
  External ref: varchar(30)
*/
{body}
"""


@dataclass(frozen=True)
class WideEstate(Estate):
    """A wide estate, and whether it also has a table of keys to retire."""

    retires: bool = False

    @property
    def retire(self) -> str:
        return f"{self.object_name}Retire"


def _install_wide(
    executor, object_name: str, catalogue: str, *, incremental: bool
) -> WideEstate:
    document = read_source_document(
        f"{SCHEMA}.{object_name}.sql",
        _wide_source(object_name, incremental=incremental).encode("utf-8"),
        WAREHOUSE,
    )
    executor.execute_script(
        f"if schema_id(N'{SCHEMA}') is null exec('create schema [{SCHEMA}]');"
        "if schema_id(N'_') is null exec('create schema [_]');"
    )
    install_runtime_references(executor, catalogue)
    estate = WideEstate(executor, object_name, retires=incremental)
    _drop_wide(estate)
    executor.execute_script(f"create table [{SCHEMA}].[{estate.raw}] ({WIDE_RAW_DDL});")
    if incremental:
        executor.execute_script(
            f"create table [{SCHEMA}].[{estate.retire}] "
            "([Customer id] varchar(50) null);"
        )
    executor.execute_script(document.create_ddl().content)
    executor.execute_script(document.create_load(item=ITEM).payload.decode("utf-8"))
    return estate


def _drop_wide(estate: WideEstate) -> None:
    name = estate.object_name
    statements = [f"drop procedure if exists [_].[Load {SCHEMA}.{name}];"]
    statements += [
        f"if object_id(N'{SCHEMA}.{name}{suffix}', N'U') is not null "
        f"drop table [{SCHEMA}].[{name}{suffix}];"
        for suffix in ("_Reject", "_Upsert", "_Delete", "_Staging", "", "Raw", "Retire")
    ]
    estate.executor.execute_script("\n".join(statements))


def _reset_wide(estate: WideEstate) -> None:
    name = estate.object_name
    statements = [
        f"delete from [{SCHEMA}].[{name}];",
        f"delete from [{SCHEMA}].[{estate.raw}];",
        forget_bookmark(SCHEMA, name),
    ]
    if estate.retires:
        statements.append(f"delete from [{SCHEMA}].[{estate.retire}];")
    statements += [
        f"if object_id(N'{SCHEMA}.{name}{suffix}', N'U') is not null "
        f"drop table [{SCHEMA}].[{name}{suffix}];"
        for suffix in ("_Reject", "_Upsert", "_Delete", "_Staging")
    ]
    estate.executor.execute_script("\n".join(statements))


def _literal(value) -> str:
    if value is None:
        return "null"
    if isinstance(value, int):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def _wide_rows(estate: WideEstate, rows, *, retire=()) -> None:
    columns = ", ".join(f"[{column}]" for column in WIDE_COLUMNS)
    statements = [f"delete from [{SCHEMA}].[{estate.raw}];"]
    if estate.retires:
        statements.append(f"delete from [{SCHEMA}].[{estate.retire}];")
    if rows:
        values = ", ".join(
            "(" + ", ".join(_literal(value) for value in row) + ")" for row in rows
        )
        statements.append(
            f"insert into [{SCHEMA}].[{estate.raw}] ({columns}) values {values};"
        )
    if retire:
        keys = ", ".join(f"({_literal(key)})" for key in retire)
        statements.append(
            f"insert into [{SCHEMA}].[{estate.retire}] ([Customer id]) values {keys};"
        )
    estate.executor.execute_script("\n".join(statements))


def _wide_contents(estate: WideEstate):
    rows = estate.executor.query(
        f"select [Customer id], [Customer name], [Email], [Region id], "
        f"[External ref] from [{SCHEMA}].[{estate.object_name}] "
        "order by [Customer id];"
    )
    return [tuple(row[column] for column in WIDE_COLUMNS) for row in rows]


def _by_key(rows) -> dict:
    return {row[0]: row for row in rows}


def _signatures(estate: WideEstate) -> dict:
    rows = estate.executor.query(
        f"select [Customer id], [Row signature] from "
        f"[{SCHEMA}].[{estate.object_name}] order by [Customer id];"
    )
    return {str(row["Customer id"]): bytes(row["Row signature"]) for row in rows}


def _reject_reasons(estate: WideEstate) -> dict:
    rows = estate.executor.query(
        f"select [Customer id], [{REJECTION_REASON}] from "
        f"[{SCHEMA}].[{estate.object_name}_Reject];"
    )
    return {
        (None if row["Customer id"] is None else str(row["Customer id"])): str(
            row[REJECTION_REASON]
        )
        for row in rows
    }


@pytest.fixture(scope="module")
def constrained_estate(
    clean_disposable_warehouse, fabric_workspace, fabric_initialise_catalogue
):
    fabric_initialise_catalogue()
    built = _install_wide(
        clean_disposable_warehouse.executor,
        CONSTRAINED_OBJECT,
        fabric_workspace.catalogue_item.name,
        incremental=False,
    )
    yield built
    _drop_wide(built)


@pytest.fixture(scope="module")
def merge_estate(
    clean_disposable_warehouse, fabric_workspace, fabric_initialise_catalogue
):
    fabric_initialise_catalogue()
    built = _install_wide(
        clean_disposable_warehouse.executor,
        MERGE_OBJECT,
        fabric_workspace.catalogue_item.name,
        incremental=True,
    )
    yield built
    _drop_wide(built)


# --- recoverable refusals -----------------------------------------------------

#: Every recoverable refusal the declaration can produce, and the rows that
#: survive them. One load, because they are discovered in one statement and the
#: claim worth making is which rows came out the other side.
REFUSABLE = [
    ("c1", "One", "a@x.test", 10, "A"),  # clean
    (None, "NoKey", "b@x.test", 10, "B"),  # the key is not a key
    ("c3", None, "c@x.test", 10, "C"),  # a declared not-null column left empty
    ("c4", "Four", "d@x.test", 10, "D"),  # clean, and duplicated below
    ("c4", "FourAgain", "e@x.test", 10, "E"),  # one of the two c4 rows goes
    ("c6", "Six", "a@x.test", 10, "F"),  # claims c1's Email
    ("c7", "Seven", "g@x.test", 10, "A"),  # claims c1's Region id + External ref
    ("c8", "Eight", None, 10, "H"),  # Email is nullable
    ("c9", "Nine", None, 10, "I"),  # so two nulls are not a collision
]

#: What survives every refusal above.
SURVIVING_KEYS = ["c1", "c4", "c8", "c9"]


def _constrained_run(estate):
    """One tolerant load over every refusal, then two more over clean sources.

    Chained, because the second load's subject is the state the first left: an
    unchanged source must write nothing, and a changed one must write exactly what
    changed. Re-seeding between them would buy a load to reach a state the
    previous load had already produced.
    """

    _reset_wide(estate)

    _wide_rows(estate, REFUSABLE)
    refused = _load(estate, fault_tolerant=True)
    first = Ran(
        result=refused,
        contents=_wide_contents(estate),
        extra={
            "reasons": _reject_reasons(estate),
            "signatures": _signatures(estate),
        },
    )

    # The accepted rows, restaged exactly as they were loaded. An unchanged source
    # must produce no work at all, which is what the stored signature is for.
    accepted = first.contents
    _wide_rows(estate, accepted)
    unchanged = _load(estate, fault_tolerant=False)
    second = Ran(
        result=unchanged,
        contents=_wide_contents(estate),
        extra={"signatures": _signatures(estate), "leftovers": _leftovers(estate)},
    )

    changed = [
        (row[0], "Renamed" if row[0] == "c1" else row[1], *row[2:]) for row in accepted
    ]
    _wide_rows(estate, changed)
    updated = _load(estate, fault_tolerant=False)
    third = Ran(
        result=updated,
        contents=_wide_contents(estate),
        extra={"signatures": _signatures(estate)},
    )

    return SimpleNamespace(refused=first, unchanged=second, updated=third)


@weaver_test(remote=True, resources={"tds"})
def test_declared_constraints_refuse_rows_and_the_survivors_load(constrained_estate):
    """Each declared refusal, the rows it leaves, and what a signature then buys.

    Which row of a duplicate group survives is arbitrary and the declaration does
    not order them, so what is asserted is that one did and that the result is
    valid under every declared key.
    """

    run = _constrained_run(constrained_estate)
    refused, unchanged, updated = run.refused, run.unchanged, run.updated

    # One row, one reason. A row wrong twice over is still one row refused.
    assert refused.extra["reasons"] == {
        None: REASON_BLANK_PK,
        "c3": "null_column: Customer name",
        "c4": REASON_DUPLICATE_PK,
        "c6": "duplicate_unique_key: Email",
        "c7": "duplicate_unique_key: Region id, External ref",
    }
    assert refused.result.rows_rejected == 5
    assert refused.result.rows_inserted == 4
    assert [row[0] for row in refused.contents] == SURVIVING_KEYS

    # The target is valid under both declared keys, and a null claims neither.
    emails = [row[2] for row in refused.contents if row[2] is not None]
    tuples = [(row[3], row[4]) for row in refused.contents]
    assert len(emails) == len(set(emails))
    assert len(tuples) == len(set(tuples))
    assert [row[0] for row in refused.contents if row[2] is None] == ["c8", "c9"]

    # Every loaded row carries a signature of its own.
    signatures = refused.extra["signatures"]
    assert sorted(signatures) == SURVIVING_KEYS
    assert all(signatures.values())
    assert len(set(signatures.values())) == len(signatures)

    # An unchanged source is one equality test per row, and no work.
    assert unchanged.result.succeeded is True
    assert (
        unchanged.result.rows_inserted,
        unchanged.result.rows_updated,
        unchanged.result.rows_deleted,
        unchanged.result.rows_rejected,
    ) == (0, 0, 0, 0)
    assert unchanged.extra["signatures"] == signatures
    assert unchanged.extra["leftovers"] == 0

    # A changed row is updated, and its signature moves with it. Nobody else's does.
    after = updated.extra["signatures"]
    assert (updated.result.rows_updated, updated.result.rows_inserted) == (1, 0)
    assert _by_key(updated.contents)["c1"][1] == "Renamed"
    assert after["c1"] != signatures["c1"]
    assert {key: after[key] for key in after if key != "c1"} == {
        key: signatures[key] for key in signatures if key != "c1"
    }


# --- the refusal that is not recoverable --------------------------------------
#
# An incremental load changes part of a target it cannot see the rest of, so the
# rows it proposes may each be fine while the state they would leave is not. That
# is not a row to reject: it is a load not to run.

#: The target these sequences start from, reached by an incremental load because
#: every other state here is too.
SEED = [
    ("c1", "One", "a@x.test", 10, "A"),
    ("c2", "Two", "b@x.test", 10, "B"),
    ("c3", "Three", "c@x.test", 10, "C"),
    ("c4", "Four", "d@x.test", 10, "D"),
    ("c5", "Five", "e@x.test", 10, "E"),
]


def _seed_merge(estate):
    _reset_wide(estate)
    _wide_rows(estate, SEED)
    return _load(estate, fault_tolerant=False)


@weaver_test(remote=True, resources={"tds"})
def test_a_holder_gives_up_a_unique_value_by_leaving_or_by_moving(merge_estate):
    """The three proposals a holder really does free its value for.

    A two-way swap, a holder moving its own composite tuple, and a claim on a
    value whose holder this same load retires. All three describe a valid final
    state, and a check that only asked "is this value held?" would refuse every
    one of them.
    """

    _seed_merge(merge_estate)
    _wide_rows(
        merge_estate,
        [
            ("c1", "One", "b@x.test", 10, "A"),  # swaps Email with c2
            ("c2", "Two", "a@x.test", 10, "B"),  # the other half of the swap
            ("c3", "Three", "c@x.test", 10, "Z"),  # moves its own composite tuple
            ("c4", "Four", "d@x.test", 10, "E"),  # claims c5's tuple; c5 is retired
        ],
        retire=["c5"],
    )
    result = _load(merge_estate, fault_tolerant=False)
    contents = _by_key(_wide_contents(merge_estate))

    assert result.succeeded is True
    assert result.rows_deleted == 1
    assert contents["c1"][2] == "b@x.test"
    assert contents["c2"][2] == "a@x.test"
    assert (contents["c3"][3], contents["c3"][4]) == (10, "Z")
    assert (contents["c4"][3], contents["c4"][4]) == (10, "E")
    assert "c5" not in contents


@weaver_test(remote=True, resources={"tds"})
def test_a_key_the_source_still_produces_is_not_retired(merge_estate):
    """The claim gives it up, and the row is loaded as an ordinary update.

    c2 is claimed and staged changed; c3 is claimed and staged unchanged. Neither
    is deleted, c2 is updated, and c3 is left alone — so its insert and update
    times both survive, which deleting and re-inserting would not have preserved.
    """

    _seed_merge(merge_estate)
    before = merge_estate.executor.query(
        f"select [Customer id], [Row insert datetime] as inserted, "
        f"[Row update datetime] as updated from [{SCHEMA}].[{MERGE_OBJECT}] "
        "order by [Customer id];"
    )
    stamps = {
        str(row["Customer id"]): (row["inserted"], row["updated"]) for row in before
    }

    _wide_rows(
        merge_estate,
        [
            ("c2", "Renamed", "b@x.test", 10, "B"),  # claimed, and changed
            ("c3", "Three", "c@x.test", 10, "C"),  # claimed, and unchanged
        ],
        retire=["c2", "c3", "c4"],  # c4 is claimed and not staged, so it goes
    )
    result = _load(merge_estate, fault_tolerant=False)
    contents = _by_key(_wide_contents(merge_estate))
    after = merge_estate.executor.query(
        f"select [Customer id], [Row insert datetime] as inserted, "
        f"[Row update datetime] as updated from [{SCHEMA}].[{MERGE_OBJECT}] "
        "order by [Customer id];"
    )
    now = {str(row["Customer id"]): (row["inserted"], row["updated"]) for row in after}

    assert result.succeeded is True
    assert (result.rows_deleted, result.rows_inserted, result.rows_updated) == (1, 0, 1)
    assert "c4" not in contents
    assert contents["c2"][1] == "Renamed"
    assert contents["c3"][1] == "Three"
    # The changed row keeps the time it was inserted; the unchanged row is untouched.
    assert now["c2"][0] == stamps["c2"][0]
    assert now["c3"] == stamps["c3"]


@weaver_test(remote=True, resources={"tds"})
def test_a_holder_moving_to_a_null_frees_its_value(merge_estate):
    """A null claims nothing, so a holder that takes one has given the value up.

    The case a plain inequality would get wrong: comparing the holder's proposed
    value with its current one answers unknown when the proposal is null, and the
    claim would be refused for a value nobody holds any more.
    """

    _seed_merge(merge_estate)
    _wide_rows(
        merge_estate,
        [
            ("c1", "One", "c@x.test", 10, "A"),  # claims c3's Email
            ("c3", "Three", None, 10, "C"),  # c3 takes a null instead
        ],
    )
    result = _load(merge_estate, fault_tolerant=False)
    contents = _by_key(_wide_contents(merge_estate))

    assert result.succeeded is True
    assert contents["c1"][2] == "c@x.test"
    assert contents["c3"][2] is None


#: Proposals that do not describe a valid target. Each is run over the same
#: seeded state, which an abort leaves untouched — so nothing has to be re-seeded
#: between them, and that fact is itself one of the claims.
CONFLICTS = {
    # A value nobody is giving up. c2's rename is valid on its own and must not be
    # applied anyway: the load either describes a valid target or does not run.
    "untouched holder": [
        ("c1", "One", "c@x.test", 10, "A"),
        ("c2", "Renamed", "b@x.test", 10, "B"),
    ],
    # The same question, asked of the composite key.
    "composite holder untouched": [
        ("c1", "One", "a@x.test", 10, "B"),
    ],
}

# A holder that is in the upsert set while keeping the value a claimant wants is
# not among these, and cannot be: both rows would then carry that value in
# staging, which incoming uniqueness refuses before the merge check is reached.
# The generated predicate still has to distinguish the two, because that is what
# lets a genuine swap or move through — asserted in
# ``tests/targeted/test_load_representation.py``.


@weaver_test(remote=True, resources={"tds"})
def test_a_proposal_that_would_leave_a_key_conflicted_stops_the_load(merge_estate):
    """And leaves the target exactly as it was, including the valid changes.

    Fatal whatever ``fault_tolerant`` says: that governs recoverable problems with
    incoming rows, and a target that is not valid under its own declaration is not
    one of those.
    """

    _seed_merge(merge_estate)
    seeded = _wide_contents(merge_estate)
    refusals = {}

    for label, rows in CONFLICTS.items():
        _wide_rows(merge_estate, rows)
        with pytest.raises(Exception) as raised:
            _load(merge_estate, fault_tolerant=False)
        refusals[label] = str(raised.value)

    _wide_rows(merge_estate, CONFLICTS["untouched holder"])
    with pytest.raises(Exception) as tolerated:
        _load(merge_estate, fault_tolerant=True)
    refusals["tolerated"] = str(tolerated.value)

    assert set(refusals) == {*CONFLICTS, "tolerated"}
    for label, message in refusals.items():
        assert "declared unique key" in message, label
    assert _wide_contents(merge_estate) == seeded
    assert _by_key(seeded)["c2"][1] == "Two"
