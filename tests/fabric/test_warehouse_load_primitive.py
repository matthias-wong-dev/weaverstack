"""The generated Warehouse load procedure, executed against a real Fabric Warehouse.

A primitive test: the table is built from ``create_ddl()``, the procedure is
installed from ``create_load()``, and the procedure is then executed directly.
No bundle is planned, no installer runs and no orchestrator exists, the claim
is that ``exec [_].[Load S.N]`` loads correctly on its own.

Fabric is the only place several of these can be answered. Whether the engine
accepts an identity column, whether it accepts the generated procedure at all,
and what it does with a two-phase installer reading ``sys.columns`` are its
answers, not ours, which is why the semantics are established here rather than
inferred from a local approximation.

The outcomes match what a Lakehouse load produces: two engines,
one set of load semantics. If they disagree, the semantics have diverged.

**One estate, and one execution per sequence.** Every round trip to a Warehouse
costs seconds, so what this file is careful about is not how many assertions it
makes but how many times it makes the engine do something. Two things follow,
and both are visible in the shape below.

The table and its procedure are installed once for the module rather than per
test. Installing them is not a claim any test here makes. It is the premise all
of them share, and a two-phase procedure install is one of the more expensive
things in the suite.

And a sequence runs once, whatever number of claims are about it. "A second
run updates only what changed" and "an unchanged row keeps its original update
time" are two questions about one load-then-load-again; asking the Warehouse to
do it twice does not make either answer better. So each sequence below runs its
loads, **captures everything its claims need at the moment it finishes**, and
hands back a snapshot. Capturing rather than leaving the tests to query later is
what makes the sharing safe: the sequences share one table, so a snapshot taken
afterwards would describe whichever sequence ran last.

The ordinary path goes further and runs as a chain, seed, update, shrink,
because each of those steps is the next one's starting state. Rejection keeps
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
    WORKING_TABLES,
    drop_load_script,
    drop_tables,
    entry_point_script,
    forget_installations,
    forget_runtime_state,
    literal,
    prepare_hand_installed,
)
from support.weaver_test import weaver_test

from weaver.catalogue.tables import BOOKMARK_SENTINEL
from weaver.declaration import read_source_document
from weaver.declaration.model import WAREHOUSE, WeaverItemId
from weaver.declaration.tsql_load import (
    PROCEDURE_RESULT_PARAMETERS,
    logical_result_row,
)
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
#: The same table under thresholds a two-row change can breach.
STRICT_OBJECT = "LoadStrict"


def _source(object_name: str, *, static: bool = False, strict: bool = False) -> str:
    static_line = "\nStatic: true\n" if static else ""
    # Low enough that removing one row of two breaches, so a refusal needs no
    # large fixture to provoke.
    strict_lines = (
        "\nDelete percentage threshold: 1\n\nStability row threshold: 1\n"
        if strict
        else ""
    )
    return f"""/*
Table ID: {SCHEMA}.{object_name}

Description: Customers.

Lineage: The sales system.

Primary key: Customer id
{static_line}{strict_lines}
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
#: by it, its bookmark row carries the Registry's four-part identity, so it is
#: named here rather than left to a default.
ITEM = WeaverItemId(*PROCEDURE_ITEM)


def _install(
    executor, object_name: str, catalogue: str, *, static: bool, strict: bool = False
) -> Estate:
    document = read_source_document(
        f"{SCHEMA}.{object_name}.sql",
        _source(object_name, static=static, strict=strict).encode("utf-8"),
        WAREHOUSE,
    )
    prepare_hand_installed(executor, SCHEMA, catalogue)
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
    executor.execute_script(entry_point_script("Load"))
    return estate


def _module_estate(
    install, drop, warehouse, workspace, initialise_catalogue, object_name, **options
):
    """Install one estate for a module, and remove it and its Installation row after."""

    initialise_catalogue()
    built = install(
        warehouse.executor, object_name, workspace.catalogue_item.name, **options
    )
    yield built
    drop(built)
    # Only at teardown: `drop` also runs during setup, and the Installation row
    # this estate needs is written before it.
    forget_installations(built.executor)


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

    yield from _module_estate(
        _install,
        _drop,
        clean_disposable_warehouse,
        fabric_workspace,
        fabric_initialise_catalogue,
        OBJECT,
        static=False,
    )


@pytest.fixture(scope="module")
def static_estate(
    clean_disposable_warehouse, fabric_workspace, fabric_initialise_catalogue
):
    """The same table declared static, under a name of its own."""

    yield from _module_estate(
        _install,
        _drop,
        clean_disposable_warehouse,
        fabric_workspace,
        fabric_initialise_catalogue,
        STATIC_OBJECT,
        static=True,
    )


@pytest.fixture(scope="module")
def strict_estate(
    clean_disposable_warehouse, fabric_workspace, fabric_initialise_catalogue
):
    """The same table under thresholds a two-row change can breach."""

    yield from _module_estate(
        _install,
        _drop,
        clean_disposable_warehouse,
        fabric_workspace,
        fabric_initialise_catalogue,
        STRICT_OBJECT,
        static=False,
        strict=True,
    )


def _drop(estate: Estate) -> None:
    estate.executor.execute_script(
        drop_load_script(SCHEMA, estate.object_name, also=("Raw",))
    )


def _reset(estate: Estate, rows=()) -> None:
    """Empty the target and its evidence, without rebuilding either, and seed
    the source with ``rows`` in the same batch.

    A sequence has to start from a known state, and dropping and recreating the
    table and procedure would be the obvious way to get one, and would put the
    module's most expensive statement back into every sequence. Deleting rows is
    the same starting state for every claim here, none of which is about a table
    that has never existed.
    """

    name = estate.object_name
    estate.executor.execute_script(
        f"delete from [{SCHEMA}].[{name}];\n"
        f"delete from [{SCHEMA}].[{estate.raw}];\n"
        + forget_runtime_state(SCHEMA, name)
        + "\n".join(drop_tables(SCHEMA, name, WORKING_TABLES))
        + _insert_script(estate, rows)
    )


def _is_sentinel(at) -> bool:
    """Whether this bookmark is the sentinel.

    TDS hands back a naive datetime and the constant is aware, so the
    comparison drops the zone.
    """

    return at is not None and at.replace(tzinfo=None) == BOOKMARK_SENTINEL.replace(
        tzinfo=None
    )


def _insert_script(estate: Estate, rows) -> str:
    if not rows:
        return ""
    values = ", ".join(
        "(" + ", ".join("null" if v is None else f"'{v}'" for v in row) + ")"
        for row in rows
    )
    return (
        f"\ninsert into [{SCHEMA}].[{estate.raw}] "
        f"([Customer id], [Customer name]) values {values};"
    )


def _source_rows(estate: Estate, rows) -> None:
    estate.executor.execute_script(
        f"delete from [{SCHEMA}].[{estate.raw}];" + _insert_script(estate, rows)
    )


def _load(estate: Estate, *, fault_tolerant: bool, **policy: bool) -> LoadResult:
    """The object's own procedure, which is what an orchestrated run calls."""

    inputs = (("fault_tolerant", 1 if fault_tolerant else 0),) + tuple(
        (name, 1 if value else 0) for name, value in sorted(policy.items())
    )
    return LoadResult.from_row(
        logical_result_row(
            estate.executor.call_procedure(
                f"[_].[Load {SCHEMA}.{estate.object_name}]",
                inputs=inputs,
                outputs=PROCEDURE_RESULT_PARAMETERS,
            )
        )
    )


def _standalone(estate: Estate, *, fault_tolerant: bool = False) -> None:
    """``exec _.[Load]``, which is what a person calls and what records.

    It reports through the catalogue rather than through output parameters: the
    row it wrote is the answer, and reading that back is the claim. No
    ``@item_name`` is supplied, so the entry point recovers the logical item
    from ``_.Installation``.
    """

    estate.executor.execute_script(
        f"exec [_].[Load] @object_name = N'{SCHEMA}.{estate.object_name}'"
        f", @fault_tolerant = {1 if fault_tolerant else 0};"
    )


def _runner_mode(estate: Estate, *, item_name: str, object_name: str) -> None:
    """The runner-style call, with the logical item supplied."""

    estate.executor.execute_script(
        f"exec [_].[Load] @object_name = N'{SCHEMA}.{object_name}'"
        f", @item_name = N'{item_name}';"
    )


def _identity_predicate(estate: Estate) -> str:
    return (
        f"where [Item type] = N'{ITEM.item_type}' "
        f"and [Item name] = N'{ITEM.item_name}' "
        f"and [Schema name] = N'{SCHEMA}' "
        f"and [Object name] = N'{estate.object_name}'"
    )


#: What a sequence reads about the estate, each as its query and how its rows
#: are shaped. ``statistics`` and ``log`` are appended, so they are oldest first
#: and the order says which load is which. ``log`` is scoped by the object
#: alone, because it records the physical target rather than the logical item.
_READS = {
    "bookmark": (
        lambda estate: (
            "select [Bookmark datetime] as at from [_].[Bookmark] "
            + _identity_predicate(estate)
        ),
        lambda rows: rows[0]["at"] if rows else None,
    ),
    "status": (
        lambda estate: (
            "select [Result] as result, [Duration milliseconds] as duration "
            "from [_].[LoadStatus] " + _identity_predicate(estate)
        ),
        lambda rows: rows[0] if rows else None,
    ),
    "statistics": (
        lambda estate: (
            "select [Rows read] as [read], [Rows inserted] as inserted, "
            "[Rows updated] as updated, [Rows deleted] as deleted, "
            "[Rows rejected] as rejected, "
            "[Is reload] as reload, [Is static skip] as skip "
            "from [_].[LoadStatistic] "
            + _identity_predicate(estate)
            + " order by [Started datetime]"
        ),
        lambda rows: rows,
    ),
    "log": (
        lambda estate: (
            "select [Task type] as task, [Result] as result, "
            "[Target name] as target, [Message] as message from [_].[Log] "
            f"where [Schema name] = N'{SCHEMA}' "
            f"and [Object name] = N'{estate.object_name}' "
            "order by [Started datetime]"
        ),
        lambda rows: rows,
    ),
    "contents": (
        lambda estate: (
            "select [Customer id], [Customer name] "
            f"from [{SCHEMA}].[{estate.object_name}] order by [Customer id]"
        ),
        lambda rows: [(row["Customer id"], row["Customer name"]) for row in rows],
    ),
    "leftovers": (
        lambda estate: (
            "select count(*) as n from sys.tables "
            f"where schema_id = schema_id(N'{SCHEMA}') "
            f"and name like '{estate.object_name}[_]%'"
        ),
        lambda rows: rows[0]["n"],
    ),
}


def _read(estate: Estate, *names: str, **queries: str) -> dict:
    """Everything asked about one moment, in one round trip.

    ``names`` are entries of ``_READS``; ``queries`` are ad hoc selects whose rows
    come back as dictionaries.
    """

    asked = [(name, *_READS[name]) for name in names]
    asked += [
        (name, lambda _estate, sql=sql: sql, list) for name, sql in queries.items()
    ]
    sets = estate.executor.query_result_sets(
        ";\n".join(query(estate) for _name, query, _shape in asked) + ";"
    )
    return {
        name: shape([dict(row) for row in rows])
        for (name, _query, shape), rows in zip(asked, sets, strict=True)
    }


def _bookmark(estate: Estate):
    return _read(estate, "bookmark")["bookmark"]


def _status(estate: Estate) -> dict | None:
    return _read(estate, "status")["status"]


def _contents(estate: Estate):
    return _read(estate, "contents")["contents"]


@dataclass(frozen=True)
class Ran:
    """What one sequence produced, read while it was still the estate's state."""

    result: LoadResult
    contents: list = field(default_factory=list)
    extra: dict = field(default_factory=dict)


# --- what only Fabric can answer ---------------------------------------------


def _ordinary(estate):
    """The ordinary life of a loaded table: seed it, change it, shrink it.

    One chain rather than three, because each step's base is the step before
    it. Run separately, the update case would first have to re-seed and the
    shrink case would have to re-seed and update again, three loads bought to
    reach states two earlier loads had already produced.

    Every step captures what its own claims need before the next one runs, so
    each claim still reads the estate at the moment it is about. What the chain
    costs is that a broken step takes the later ones with it; what it buys is
    three loads instead of five, and on a Warehouse that is a minute.
    """

    _reset(estate, CLEAN)
    seeded = _load(estate, fault_tolerant=False)
    seen = _read(
        estate,
        "contents",
        "leftovers",
        identities=(
            "select count(*) as n, count(distinct [Customer key]) as distinct_keys "
            f"from [{SCHEMA}].[{estate.object_name}]"
        ),
        procedures=(
            f"select name from sys.procedures where name = N'Load {SCHEMA}.{OBJECT}'"
        ),
    )
    (identities,) = seen["identities"]
    first = Ran(
        result=seeded,
        contents=seen["contents"],
        extra={
            "rows": identities["n"],
            "distinct_keys": identities["distinct_keys"],
            "leftovers": seen["leftovers"],
            "procedures": [str(row["name"]) for row in seen["procedures"]],
        },
    )

    _source_rows(estate, CHANGED)
    updated = _load(estate, fault_tolerant=False)
    seen = _read(
        estate,
        "contents",
        audit=(
            "select [Customer id], case when [Row insert datetime] = "
            "[Row update datetime] then 1 else 0 end as untouched "
            f"from [{SCHEMA}].[{estate.object_name}] order by [Customer id]"
        ),
    )
    second = Ran(
        result=updated,
        contents=seen["contents"],
        extra={
            "audit": [(row["Customer id"], row["untouched"]) for row in seen["audit"]]
        },
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

    _reset(estate, CLEAN)
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

    _reset(estate, REJECTABLE)
    result = _load(estate, fault_tolerant=True)

    # Read here because the reject table is this run's evidence, and the next
    # sequence's `_reset` takes it away.
    seen = _read(
        estate,
        "contents",
        reasons=(
            f"select distinct [{REJECTION_REASON}] from "
            f"[{SCHEMA}].[{estate.object_name}_Reject]"
        ),
    )
    return Ran(
        result=result,
        contents=seen["contents"],
        extra={"reasons": {str(row[REJECTION_REASON]) for row in seen["reasons"]}},
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
# SQL rather than in Python. The gate is inside the procedure, see
# `weaver.declaration.tsql_load._static_gate`, so it has to be executed to be
# proved, and only a Warehouse can execute it. What the generator emits is
# asserted cheaply in `tests/test_static_load_declaration.py`; this is the half
# that needs an engine.


def _static_run(static_estate):
    """A static load into an empty target, then a second over a changed source.

    Both through the entry point. The Static gate reads a bookmark and the
    object's own procedure does not write one, so what closes the gate is the
    record, and the record belongs to whoever ran the load. Run through the
    primitive alone, a Static object would seed itself on every call.
    """

    _reset(static_estate, CLEAN)
    _standalone(static_estate)
    seeded = _read(static_estate, "contents", "statistics", "bookmark")

    _source_rows(static_estate, [("c9", "Different")])
    _standalone(static_estate)
    seen = _read(
        static_estate, "contents", "status", "statistics", "leftovers", "bookmark"
    )
    return Ran(
        result=None,
        contents=seen.pop("contents"),
        extra={"seeded": seeded, **seen},
    )


@weaver_test(remote=True, resources={"tds"})
def test_the_objects_own_procedure_records_nothing(estate):
    """It is an execution primitive: whoever called it owns the record.

    It still reports the instant it began, because that is what a caller
    advances the bookmark to.
    """

    _reset(estate, CLEAN)

    result = _load(estate, fault_tolerant=False)
    seen = _read(estate, "bookmark", "status", "log")

    assert result.succeeded is True
    assert result.bookmark_datetime is not None
    assert seen["bookmark"] is None
    assert seen["status"] is None
    assert seen["log"] == []


@weaver_test(remote=True, resources={"tds"})
def test_the_entry_point_records_a_clean_load_through_the_views(estate):
    """``exec _.[Load]`` writes the whole operational record.

    In every Warehouse but the one the catalogue lives in these tables are views
    over the catalogue's own, so this is also the claim that an insert and an
    update through such a view reach the table behind it. Fabric refuses a plain
    INSERT there and accepts a MERGE's, which is why every write is one.
    """

    _reset(estate, CLEAN)

    _standalone(estate)
    seen = _read(estate, "bookmark", "status", "log", "statistics")

    assert seen["bookmark"] is not None
    assert seen["status"]["result"] == "Succeeded"
    (logged,) = seen["log"]
    assert logged["task"] == "load"
    assert logged["result"] == "Succeeded"
    # The Warehouse the procedure ran in, taken from the connection rather than
    # baked into the generated statement.
    assert logged["target"]
    (statistic,) = seen["statistics"]
    assert statistic["read"] == len(CLEAN)
    assert statistic["reload"] is False
    assert statistic["skip"] is False


@weaver_test(remote=True, resources={"tds"})
def test_a_supplied_item_name_records_against_that_item(estate):
    """Runner mode, where the logical item is supplied rather than resolved.

    A supplied name is used as given, so the row lands against it without
    ``_.Installation`` being read at all.
    """

    from sql_support import PROCEDURE_ITEM

    _reset(estate, CLEAN)

    _runner_mode(
        estate,
        item_name=PROCEDURE_ITEM[1],
        object_name=estate.object_name,
    )

    assert _status(estate)["result"] == "Succeeded"


@weaver_test(remote=True, resources={"tds"})
def test_an_unknown_object_is_refused_before_anything_is_recorded(estate):
    """A name this Warehouse holds no implementation procedure for."""

    _reset(estate)

    import pytest as _pytest

    from weaver.sql import SqlExecutionError

    with _pytest.raises(SqlExecutionError, match="is not a loadable object"):
        estate.executor.query(
            f"exec [_].[Load] @object_name = N'{SCHEMA}.NoSuchObject';"
        )
    assert _status(estate) is None


@weaver_test(remote=True, resources={"tds"})
def test_a_second_clean_load_moves_the_bookmark_on(estate):
    """The row is updated in place, which is the half an insert cannot prove."""

    _reset(estate, CLEAN)
    _standalone(estate)
    first = _bookmark(estate)

    _source_rows(estate, CHANGED)
    _standalone(estate)
    seen = _read(estate, "bookmark", "status", "statistics")
    second = seen["bookmark"]

    assert first is not None and second is not None
    assert second > first
    # And the status is one row per object, updated rather than accumulated,
    # while the statistics accumulate.
    assert seen["status"]["result"] == "Succeeded"
    assert len(seen["statistics"]) == 2


@weaver_test(remote=True, resources={"tds"})
def test_a_load_that_rejected_rows_is_rejected_and_keeps_its_bookmark(estate):
    """It has not read its window, whether or not it was told to tolerate them."""

    _reset(estate, CLEAN)
    _standalone(estate)
    clean = _bookmark(estate)

    _source_rows(estate, REJECTABLE)
    _standalone(estate, fault_tolerant=True)
    seen = _read(estate, "bookmark", "status")

    assert seen["bookmark"] == clean
    assert seen["status"]["result"] == "Rejected"


@weaver_test(remote=True, resources={"tds"})
def test_a_refusal_is_recorded_and_then_raised_to_the_caller(estate):
    """Both halves, because either alone is the wrong behaviour.

    A refusal that left no row would make the estate silent about exactly the
    loads somebody needs to look at. A refusal that returned normally would make
    a failed load indistinguishable from a successful call.
    """

    from weaver.sql.errors import SqlError

    _reset(estate, REJECTABLE)

    with pytest.raises((SqlError, Exception)) as raised:
        _standalone(estate, fault_tolerant=False)
    seen = _read(estate, "status", "log", "bookmark")

    assert "rejected" in str(raised.value).casefold()
    # Weaver's own refusal ran under Weaver's control and produced an
    # unacceptable result, so it is Failed rather than Error.
    assert seen["status"]["result"] == "Failed"
    assert seen["log"][0]["result"] == "Failed"
    assert seen["bookmark"] is None


@weaver_test(remote=True, resources={"tds"})
def test_a_refusal_through_the_entry_point_records_what_it_counted(estate):
    """The manual call records the same evidence an orchestrated run reports.

    ``_.Load`` asks the implementation procedure to return its refusal rather
    than throw it, because a Fabric Warehouse discards output values when a
    procedure ends in an uncaught THROW. Throwing left the entry point with
    nothing to record but zeroes for counts it had already settled.
    """

    from weaver.sql.errors import SqlError

    _reset(estate, CLEAN)
    _standalone(estate)
    before = _read(estate, "contents", "bookmark")

    _source_rows(estate, REJECTABLE)
    # The same refusal as the orchestrated path settles, so what the entry
    # point recorded can be compared with what the gate actually counted.
    returned = _load(estate, fault_tolerant=False, return_refusal=True)

    with pytest.raises((SqlError, Exception)) as raised:
        _standalone(estate, fault_tolerant=False)
    seen = _read(estate, "statistics", "log", "status", "contents", "bookmark")

    statistic = seen["statistics"][-1]
    logged = seen["log"][-1]
    assert "rejected" in str(raised.value).casefold()
    assert seen["status"]["result"] == "Failed"
    assert logged["result"] == "Failed"
    assert "rejected" in str(logged["message"]).casefold()
    # What the gate had already counted, rather than the zeroes a lost output
    # set coalesces to.
    assert returned.is_refusal
    assert returned.rows_read == len(REJECTABLE)
    assert returned.rows_rejected > 0
    assert statistic["read"] == returned.rows_read
    assert statistic["rejected"] == returned.rows_rejected
    assert (statistic["inserted"], statistic["updated"], statistic["deleted"]) == (
        0,
        0,
        0,
    )
    # Refused before writing, and a refusal reads no window it can record.
    assert seen["contents"] == before["contents"]
    assert seen["bookmark"] == before["bookmark"]


@weaver_test(remote=True, resources={"tds"})
def test_a_tolerated_rejection_is_an_answer_rather_than_a_failure(estate):
    """It returned rather than threw, so the call returns too."""

    _reset(estate, REJECTABLE)

    _standalone(estate, fault_tolerant=True)

    assert _status(estate)["result"] == "Rejected"


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
    """Loaded once, and the record of that is what stops the second one."""

    static_run = _static_run(static_estate)
    seeded = static_run.extra["seeded"]

    # The seed: it ran, it wrote, and it recorded the bookmark that closes the
    # gate behind it.
    assert seeded["contents"] == CLEAN
    assert [row["inserted"] for row in seeded["statistics"]] == [2]
    assert seeded["statistics"][0]["skip"] is False
    assert seeded["bookmark"] is not None

    # The second call: skipped, and the changed source never read.
    assert static_run.contents == CLEAN
    assert static_run.extra["leftovers"] == 0
    assert static_run.extra["status"]["result"] == "Skipped"
    skipped = static_run.extra["statistics"][-1]
    assert skipped["skip"] is True
    assert skipped["read"] == 0
    # And nothing moved the bookmark on, because nothing read a window.
    assert static_run.extra["bookmark"] == seeded["bookmark"]


# --- declared constraints, executed ------------------------------------------
#
# Nullability and uniqueness are declarations, and what they mean is what the
# engine does with the generated procedure. Two more estates, because the two
# subjects are different: one is about refusing incoming rows and
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
    prepare_hand_installed(executor, SCHEMA, catalogue)
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
    estate.executor.execute_script(
        drop_load_script(SCHEMA, estate.object_name, also=("Raw", "Retire"))
    )


def _reset_wide(estate: WideEstate, rows=()) -> None:
    """Empty the target and its evidence, and seed the source in the same batch."""

    name = estate.object_name
    statements = [
        f"delete from [{SCHEMA}].[{name}];",
        forget_runtime_state(SCHEMA, name),
    ]
    statements += drop_tables(SCHEMA, name, WORKING_TABLES)
    estate.executor.execute_script(
        "\n".join(statements) + "\n" + _wide_rows_script(estate, rows)
    )


def _wide_rows(estate: WideEstate, rows, *, retire=()) -> None:
    estate.executor.execute_script(_wide_rows_script(estate, rows, retire=retire))


def _wide_rows_script(estate: WideEstate, rows, *, retire=()) -> str:
    """Replace the source, and the keys to retire, with these."""

    columns = ", ".join(f"[{column}]" for column in WIDE_COLUMNS)
    statements = [f"delete from [{SCHEMA}].[{estate.raw}];"]
    if estate.retires:
        statements.append(f"delete from [{SCHEMA}].[{estate.retire}];")
    if rows:
        values = ", ".join(
            "(" + ", ".join(literal(value) for value in row) + ")" for row in rows
        )
        statements.append(
            f"insert into [{SCHEMA}].[{estate.raw}] ({columns}) values {values};"
        )
    if retire:
        keys = ", ".join(f"({literal(key)})" for key in retire)
        statements.append(
            f"insert into [{SCHEMA}].[{estate.retire}] ([Customer id]) values {keys};"
        )
    return "\n".join(statements)


def _by_key(rows) -> dict:
    return {row[0]: row for row in rows}


_READS.update(
    wide_contents=(
        lambda estate: (
            "select [Customer id], [Customer name], [Email], [Region id], "
            f"[External ref] from [{SCHEMA}].[{estate.object_name}] order by [Customer id]"
        ),
        lambda rows: [tuple(row[column] for column in WIDE_COLUMNS) for row in rows],
    ),
    signatures=(
        lambda estate: (
            "select [Customer id], [Row signature] from "
            f"[{SCHEMA}].[{estate.object_name}] order by [Customer id]"
        ),
        lambda rows: {
            str(row["Customer id"]): bytes(row["Row signature"]) for row in rows
        },
    ),
    reject_reasons=(
        lambda estate: (
            f"select [Customer id], [{REJECTION_REASON}] from "
            f"[{SCHEMA}].[{estate.object_name}_Reject]"
        ),
        lambda rows: {
            (None if row["Customer id"] is None else str(row["Customer id"])): str(
                row[REJECTION_REASON]
            )
            for row in rows
        },
    ),
    stamps=(
        lambda estate: (
            "select [Customer id], [Row insert datetime] as inserted, "
            f"[Row update datetime] as updated from [{SCHEMA}].[{estate.object_name}] "
            "order by [Customer id]"
        ),
        lambda rows: {
            str(row["Customer id"]): (row["inserted"], row["updated"]) for row in rows
        },
    ),
)


def _wide_contents(estate: WideEstate):
    return _read(estate, "wide_contents")["wide_contents"]


@pytest.fixture(scope="module")
def constrained_estate(
    clean_disposable_warehouse, fabric_workspace, fabric_initialise_catalogue
):
    yield from _module_estate(
        _install_wide,
        _drop_wide,
        clean_disposable_warehouse,
        fabric_workspace,
        fabric_initialise_catalogue,
        CONSTRAINED_OBJECT,
        incremental=False,
    )


@pytest.fixture(scope="module")
def merge_estate(
    clean_disposable_warehouse, fabric_workspace, fabric_initialise_catalogue
):
    yield from _module_estate(
        _install_wide,
        _drop_wide,
        clean_disposable_warehouse,
        fabric_workspace,
        fabric_initialise_catalogue,
        MERGE_OBJECT,
        incremental=True,
    )


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

    _reset_wide(estate, REFUSABLE)
    refused = _load(estate, fault_tolerant=True)
    seen = _read(estate, "wide_contents", "reject_reasons", "signatures")
    first = Ran(
        result=refused,
        contents=seen["wide_contents"],
        extra={"reasons": seen["reject_reasons"], "signatures": seen["signatures"]},
    )

    # The accepted rows, restaged exactly as they were loaded. An unchanged source
    # must produce no work at all, which is what the stored signature is for.
    accepted = first.contents
    _wide_rows(estate, accepted)
    unchanged = _load(estate, fault_tolerant=False)
    seen = _read(estate, "wide_contents", "signatures", "leftovers")
    second = Ran(
        result=unchanged,
        contents=seen["wide_contents"],
        extra={"signatures": seen["signatures"], "leftovers": seen["leftovers"]},
    )

    changed = [
        (row[0], "Renamed" if row[0] == "c1" else row[1], *row[2:]) for row in accepted
    ]
    _wide_rows(estate, changed)
    updated = _load(estate, fault_tolerant=False)
    seen = _read(estate, "wide_contents", "signatures")
    third = Ran(
        result=updated,
        contents=seen["wide_contents"],
        extra={"signatures": seen["signatures"]},
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
    _reset_wide(estate, SEED)
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
    is deleted, c2 is updated, and c3 is left alone, so its insert and update
    times both survive, which deleting and re-inserting would not have preserved.
    """

    _seed_merge(merge_estate)
    stamps = _read(merge_estate, "stamps")["stamps"]

    _wide_rows(
        merge_estate,
        [
            ("c2", "Renamed", "b@x.test", 10, "B"),  # claimed, and changed
            ("c3", "Three", "c@x.test", 10, "C"),  # claimed, and unchanged
        ],
        retire=["c2", "c3", "c4"],  # c4 is claimed and not staged, so it goes
    )
    result = _load(merge_estate, fault_tolerant=False)
    seen = _read(merge_estate, "wide_contents", "stamps")
    contents = _by_key(seen["wide_contents"])
    now = seen["stamps"]

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
#: seeded state, which an abort leaves untouched, so nothing has to be re-seeded
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

# A holder that is in the upsert set while keeping the value a claimant needs is
# not among these, and cannot be: both rows would then carry that value in
# staging, which incoming uniqueness refuses before the merge check is reached.
# The generated predicate still has to distinguish the two, because that is what
# lets a genuine swap or move through, asserted in
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


# --- reload, executed ---------------------------------------------------------
#
# Reload reconstructs one table from zero: its load state is ended, its target is
# emptied, and the authored body then runs against both. The ordering is the
# claim, and only an engine can settle it, so the estate here is one whose body
# reads its own target:
#
#     select the source rows this table does not already hold
#
# Against a populated target that produces nothing. So a reload that reconstructs
# the whole population is a reload that emptied the target before the body ran,
# and one that emptied it afterwards would leave it empty.

#: Incremental, and its body anti-joins its own target.
RELOAD_OBJECT = "LoadReload"

GROWN = CLEAN + [("c3", "Three")]


def _reload_source(object_name: str, *, body: str) -> str:
    """One declaration, over whichever body a step needs.

    The shape-only build runs the body, and on a first build the target is not
    there to be read, so the table is built from a body that reads the source
    alone. The procedure is generated over the body that reads the target: a
    procedure install shapes the body into statements and reads ``sys.columns``,
    and the body itself runs only when the procedure is called.
    """

    return f"""/*
Table ID: {SCHEMA}.{object_name}

Description: Customers this table does not already hold.

Lineage: The sales system.

Primary key: Customer id

Incremental: true

Schema:
  Customer id: varchar(50)
  Customer name: varchar(200)
*/
{body}
"""


def _reads_the_source(object_name: str) -> str:
    return f"select [Customer id], [Customer name] from [{SCHEMA}].[{object_name}Raw]"


def _reads_the_target(object_name: str) -> str:
    """The anti-join: the source rows this table does not already hold."""

    return (
        f"select r.[Customer id], r.[Customer name]\n"
        f"  from [{SCHEMA}].[{object_name}Raw] as r\n"
        f"  left join [{SCHEMA}].[{object_name}] as t\n"
        f"    on t.[Customer id] = r.[Customer id]\n"
        f" where t.[Customer id] is null"
    )


def _reload_document(object_name: str, body: str):
    return read_source_document(
        f"{SCHEMA}.{object_name}.sql",
        _reload_source(object_name, body=body).encode("utf-8"),
        WAREHOUSE,
    )


def _install_reload(executor, object_name: str, catalogue: str) -> Estate:
    prepare_hand_installed(executor, SCHEMA, catalogue)
    estate = Estate(executor, object_name)
    _drop(estate)
    executor.execute_script(
        f"create table [{SCHEMA}].[{estate.raw}] "
        "([Customer id] varchar(50) null, [Customer name] varchar(200) null);"
    )
    executor.execute_script(
        _reload_document(object_name, _reads_the_source(object_name))
        .create_ddl()
        .content
    )
    executor.execute_script(
        _reload_document(object_name, _reads_the_target(object_name))
        .create_load(item=ITEM)
        .payload.decode("utf-8")
    )
    executor.execute_script(entry_point_script("Load"))
    return estate


@pytest.fixture(scope="module")
def reload_estate(
    clean_disposable_warehouse, fabric_workspace, fabric_initialise_catalogue
):
    """A target-dependent incremental table, under a name of its own."""

    yield from _module_estate(
        _install_reload,
        _drop,
        clean_disposable_warehouse,
        fabric_workspace,
        fabric_initialise_catalogue,
        RELOAD_OBJECT,
    )


def _reload(estate: Estate, *, fault_tolerant: bool = False) -> None:
    """``exec _.[Load] ... @reload = 1``, which ends the state and then clears."""

    estate.executor.execute_script(
        f"exec [_].[Load] @object_name = N'{SCHEMA}.{estate.object_name}'"
        f", @fault_tolerant = {1 if fault_tolerant else 0}"
        ", @reload = 1;"
    )


def _reload_run(estate):
    """Seed, grow, reload, then fail a reload. One chain, four states.

    Each step is the next one's starting state, and every claim reads what it is
    about at the moment it happened.
    """

    _reset(estate, CLEAN)
    _standalone(estate)
    seeded = _read(estate, "contents", "bookmark")

    # An ordinary incremental run: the body sees the two rows already there and
    # produces only the third.
    _source_rows(estate, GROWN)
    _standalone(estate)
    grown = _latest(_read(estate, "contents", "statistics", "bookmark"))

    # The same source, reloaded. An emptied target makes the body produce all
    # three; a target still holding them would make it produce none.
    _reload(estate)
    reloaded = _latest(_read(estate, "contents", "statistics", "status", "bookmark"))

    # A reload that cannot settle. The row with no key is refused, and the run
    # is intolerant, so the procedure raises after the target was emptied.
    _source_rows(estate, GROWN + [(None, "NoKey")])
    with pytest.raises(Exception) as raised:
        _reload(estate)
    seen = _latest(_read(estate, "contents", "status", "statistics", "bookmark"))
    return Ran(
        result=None,
        contents=seen.pop("contents"),
        extra={
            "seeded": seeded,
            "grown": grown,
            "reloaded": reloaded,
            "refusal": str(raised.value),
            **seen,
        },
    )


def _latest(seen: dict) -> dict:
    """The same reading, with only the newest statistic."""

    return {**seen, "statistics": seen["statistics"][-1]}


@weaver_test(remote=True, resources={"tds"})
def test_the_reload_lifecycle(reload_estate):
    """The whole mode, executed: cleared first, reconstructed, and recorded.

    One sequence, four states, every claim about the moment it happened. The body
    reads its own target, so what a reload produced is the evidence that the
    target was empty when the body ran.

    The last state is a reload that did not settle. Its target is gone, so the
    account of it as loaded goes with it: the bookmark is back at the sentinel,
    and the next run reads the whole source again.
    """

    run = _reload_run(reload_estate)
    seeded, grown, reloaded = (
        run.extra["seeded"],
        run.extra["grown"],
        run.extra["reloaded"],
    )

    # The premise: an ordinary incremental run adds the one row it did not hold.
    assert seeded["contents"] == CLEAN
    assert grown["contents"] == GROWN
    assert grown["statistics"]["read"] == 1
    assert grown["statistics"]["inserted"] == 1
    assert grown["statistics"]["reload"] is False

    # The reload: the body saw an empty target, so it read the whole population
    # and wrote it back.
    assert reloaded["contents"] == GROWN
    assert reloaded["statistics"]["read"] == len(GROWN)
    assert reloaded["statistics"]["inserted"] == len(GROWN)
    assert reloaded["statistics"]["reload"] is True
    assert reloaded["status"]["result"] == "Succeeded"
    # A clean reload settles like any other clean load, so the bookmark advances.
    assert reloaded["bookmark"] > grown["bookmark"]

    # The reload that did not settle: emptied, refused, and left saying so. The
    # bookmark stands at the sentinel, which is what a build's reconciliation
    # leaves and what the next load reads as no cursor.
    assert "rejected" in run.extra["refusal"]
    assert run.contents == []
    assert run.extra["status"]["result"] == "Failed"
    assert run.extra["statistics"]["reload"] is True
    assert _is_sentinel(run.extra["bookmark"])


def _static_reload_run(estate):
    """Seed a Static object, fail a reload of it, then load it the ordinary way.

    The regression the removed row buys. A Static object is skipped once a
    bookmark row says a clean load has run for this incarnation, so a reload that
    emptied the target and then failed must leave no row: otherwise the next
    ordinary load reports a successful load of nothing over an empty table.
    """

    _reset(estate, CLEAN)
    _standalone(estate)
    seeded = _read(estate, "contents", "bookmark")

    # A reload that cannot settle: the target is emptied, the blank key is
    # refused, and the intolerant run raises.
    _source_rows(estate, CLEAN + [(None, "NoKey")])
    with pytest.raises(Exception) as raised:
        _reload(estate)
    failed = {
        **_read(estate, "contents", "bookmark", "status"),
        "refusal": str(raised.value),
    }

    # The ordinary load that follows. Nothing here says reload.
    _source_rows(estate, CLEAN)
    _standalone(estate)
    seen = _latest(_read(estate, "contents", "statistics", "bookmark"))
    return Ran(
        result=None,
        contents=seen.pop("contents"),
        extra={"seeded": seeded, "failed": failed, **seen},
    )


@weaver_test(remote=True, resources={"tds"})
def test_a_failed_static_reload_leaves_the_next_load_to_run(static_estate):
    """What closes the Static gate is a bookmark past the sentinel.

    Stored as a sentinel instead, the row would still be there and the gate would
    still close, over a table the reload had just emptied.
    """

    run = _static_reload_run(static_estate)
    seeded, failed = run.extra["seeded"], run.extra["failed"]

    assert seeded["contents"] == CLEAN
    assert seeded["bookmark"] is not None

    # The reload emptied the target and then could not settle.
    assert "rejected" in failed["refusal"]
    assert failed["contents"] == []
    assert failed["status"]["result"] == "Failed"
    assert _is_sentinel(failed["bookmark"])

    # The ordinary load that follows runs rather than skipping, and says so.
    assert run.contents == CLEAN
    assert run.extra["statistics"]["skip"] is False
    assert run.extra["statistics"]["reload"] is False
    assert run.extra["statistics"]["read"] == len(CLEAN)
    assert run.extra["bookmark"] is not None


# --- a refusal an orchestrator can read ----------------------------------------


@weaver_test(remote=True, resources={"tds"})
def test_a_thrown_refusal_returns_no_output_values_at_all(estate):
    """The engine fact the returned-refusal contract exists for.

    The procedure assigns its outputs before it throws, and the outer batch
    catches the error, and the values are still gone. Recovering a refusal's
    counts from a thrown call is therefore not possible here.
    """

    _reset(estate, REJECTABLE)

    # The final result set, because the procedure's own statements come first.
    captured = estate.executor.query_result_sets(
        "declare @rows_read bigint;\n"
        "declare @rejected bigint;\n"
        "declare @caught int;\n"
        "begin try\n"
        f"    exec [_].[Load {SCHEMA}.{estate.object_name}]\n"
        "        @fault_tolerant = 0\n"
        "      , @weaver_rows_read = @rows_read output\n"
        "      , @weaver_rows_rejected = @rejected output;\n"
        "end try\n"
        "begin catch\n"
        "    set @caught = error_number();\n"
        "end catch;\n"
        "select @rows_read as rows_read, @rejected as rejected, "
        "@caught as caught;"
    )[-1][0]

    assert captured["caught"] == 51020
    assert captured["rows_read"] is None
    assert captured["rejected"] is None


@weaver_test(remote=True, resources={"tds"})
def test_a_returned_refusal_carries_the_counts_it_settled(estate):
    """What an orchestrated run reads instead: the same refusal, as a result.

    It says it refused, so the run records a failure rather than a success with
    rejects, and it still says how many rows it read and set aside.
    """

    _reset(estate, CLEAN)
    _load(estate, fault_tolerant=False, return_refusal=True)
    loaded = _contents(estate)

    _source_rows(estate, REJECTABLE)
    refused = _load(estate, fault_tolerant=False, return_refusal=True)

    assert refused.is_refusal
    assert not refused.succeeded
    assert refused.rows_read == len(REJECTABLE)
    assert refused.rows_rejected
    assert refused.rows_inserted == 0
    assert refused.rows_updated == 0
    assert refused.rows_deleted == 0
    # Refused before writing, so the target still holds what the clean load left.
    assert _contents(estate) == loaded


@weaver_test(remote=True, resources={"tds"})
def test_an_excessive_change_is_refused_and_the_waiver_permits_it(strict_estate):
    """One change, one contract, and only the waiver differs.

    Declared at one percent of a target of at least one row, so removing one of
    two rows breaches. Refused, the target keeps both rows. Waived, the same
    change is applied.
    """

    estate = strict_estate
    _reset(estate)
    _source_rows(estate, CLEAN)
    _load(estate, fault_tolerant=False, return_refusal=True)
    seeded = _contents(estate)

    _source_rows(estate, SHRUNK)
    refused = _load(estate, fault_tolerant=False, return_refusal=True)
    unchanged = _contents(estate)

    permitted = _load(
        estate,
        fault_tolerant=False,
        return_refusal=True,
        ignore_stability_threshold=True,
    )

    assert len(seeded) == 2
    assert refused.is_refusal
    assert "over the 1% threshold" in refused.error_message
    assert "the target was not modified" in refused.error_message
    assert refused.rows_deleted == 0
    assert unchanged == seeded

    assert permitted.succeeded
    assert permitted.rows_deleted == 1
    assert len(_contents(estate)) == 1


@weaver_test(remote=True, resources={"tds"})
def test_a_refused_load_establishes_no_bookmark(strict_estate):
    """A refusal wrote nothing, so there is no window to record having read."""

    estate = strict_estate
    _reset(estate)
    _source_rows(estate, CLEAN)
    _load(estate, fault_tolerant=False, return_refusal=True)
    clean = _bookmark(estate)

    _source_rows(estate, SHRUNK)
    refused = _load(estate, fault_tolerant=False, return_refusal=True)

    assert refused.is_refusal
    assert refused.bookmark_datetime is None
    assert _bookmark(estate) == clean
