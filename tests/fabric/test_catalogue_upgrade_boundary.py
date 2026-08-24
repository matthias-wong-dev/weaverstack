"""What a build does about catalogue tables: introduce one, and reference one.

Two claims that need a real Warehouse and nothing else.

Adding a table to ``_`` makes every existing installation older than the Weaver
building against it. The build that introduces the table has to plan against a
catalogue that does not have it yet, create it, and reconcile against it from
then on — and it has to do all three in one bundle, because every build binds
``_weaver`` and so gets the new table from the same bundle it would have needed
it for.

The other is the reference: a Warehouse that is not the catalogue holds a view
over each of the catalogue's runtime tables, which is how a generated procedure
reaches its own bookmark and records what it did.

``remote`` and Warehouse-only: the catalogue is a Warehouse, the estate's objects
are T-SQL, so nothing here starts Spark. The upgrade runs against a disposable
Warehouse that is its own catalogue, so dropping a catalogue table cannot touch
the shared estate the rest of the suite depends on.
"""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest
from sql_support import CatalogObject, user_objects
from support.build_envs import WAREHOUSE_ESTATE_FIXTURE
from support.weaver_test import register_session, weaver_test

import weaver
from weaver.catalogue.tables import (
    CATALOGUE_TABLES,
    PRESENTED_RUNTIME_TABLES,
    RUNTIME_TABLES,
)
from weaver.sessions import ConsoleSession


def _built(workspace, estate, bind):
    with ConsoleSession(workspace=workspace) as session:
        register_session(session)
        return weaver.build(str(estate), bind=[bind], session=session)


def _failures(report):
    return [(failure.action_id, failure.message) for failure in report.errors]


def _write_convergence_estate(root: Path, *, revised: bool, broken: bool) -> Path:
    """One incremental object followed by a procedure that can fail late."""

    item = root / "Warehouse" / "Convergence"
    schemas = item / "schemas"
    schemas.mkdir(parents=True, exist_ok=True)
    (schemas / "Wh.yml").write_text(
        "Schema ID: Wh\nDescription: Convergence test objects.\n", encoding="utf-8"
    )
    revision = ", cast(1 as int) as Revision" if revised else ""
    (item / "Wh.AIncremental.sql").write_text(
        f"""/*
Table ID: Wh.AIncremental

Description: Rows selected after the current bookmark.

Lineage: Deterministic test values.

Primary key: CustomerId

Incremental: true
*/
declare @bookmark_datetime datetime2(6);
declare @rows_read bigint;

set @bookmark_datetime = (
    select [Bookmark datetime]
    from _.Bookmark
    where [Item type] = N'Warehouse'
      and [Item name] = N'Convergence'
      and [Schema name] = N'Wh'
      and [Object name] = N'AIncremental'
);

select v.CustomerId, v.CustomerName{revision}
from (values
    (1, cast('Ada' as varchar(20)), cast('2026-01-01' as datetime2(6))),
    (2, cast('Bo' as varchar(20)), cast('2026-01-02' as datetime2(6))),
    (3, cast('Cy' as varchar(20)), cast('2026-01-03' as datetime2(6)))
) as v (CustomerId, CustomerName, Modified)
where v.Modified > coalesce(@bookmark_datetime, cast('1900-01-01' as datetime2(6)))
""",
        encoding="utf-8",
    )
    collision = "declare @weaver_load_datetime datetime2(6);\n\n" if broken else ""
    (item / "Wh.ZLater.sql").write_text(
        f"""/*
Table ID: Wh.ZLater

Description: A later load artefact used to stop catalogue publication.

Lineage: A deterministic value.

Primary key: Id
*/
{collision}select 1 as Id
""",
        encoding="utf-8",
    )
    return root


def _write_runtime_reference_estate(root: Path) -> Path:
    """One incremental load whose authored SQL reads the local Bookmark view."""

    item = root / "Warehouse" / "RuntimeReference"
    schemas = item / "schemas"
    schemas.mkdir(parents=True, exist_ok=True)
    (schemas / "Wh.yml").write_text(
        "Schema ID: Wh\nDescription: Runtime reference test objects.\n",
        encoding="utf-8",
    )
    (item / "Wh.Customer.sql").write_text(
        """/*
Table ID: Wh.Customer

Description: Rows selected after the current bookmark.

Lineage: Deterministic test values.

Primary key: CustomerId

Incremental: true
*/
declare @bookmark_datetime datetime2(6);
declare @rows_read bigint;

set @bookmark_datetime = (
    select [Bookmark datetime]
    from _.Bookmark
    where [Item type] = N'Warehouse'
      and [Item name] = N'RuntimeReference'
      and [Schema name] = N'Wh'
      and [Object name] = N'Customer'
);

select v.CustomerId, v.CustomerName
from (values
    (1, cast('Ada' as varchar(20)), cast('2026-01-01' as datetime2(6))),
    (2, cast('Bo' as varchar(20)), cast('2026-01-02' as datetime2(6)))
) as v (CustomerId, CustomerName, Modified)
where v.Modified > coalesce(@bookmark_datetime, cast('1900-01-01' as datetime2(6)))
""",
        encoding="utf-8",
    )
    return root


def _run_standalone_load(executor) -> None:
    executor.execute_script(
        "exec [_].[Load] @object_name = N'Wh.AIncremental', @fault_tolerant = 0;"
    )


@pytest.fixture
def catalogue_of_its_own(fabric_workspace, clean_disposable_warehouse):
    """The disposable Warehouse, emptied of the catalogue it held, afterwards.

    Every other Warehouse test uses this item as an ordinary target, recorded in
    the shared catalogue: there ``_.Bookmark`` is a *view* over the catalogue's
    table, and a real table of that name is one the next ``create or alter view``
    cannot replace. What this test installs is recorded in the item's own ``_``
    instead, so nothing else would ever remove it.

    Emptied whether or not the test passed, and in that order: through Weaver
    while the catalogue recording the objects is still there, then of the
    catalogue itself.
    """

    warehouse = clean_disposable_warehouse
    yield warehouse
    own = replace(fabric_workspace, catalogue=f"Warehouse/{warehouse.item.name}")
    with ConsoleSession(workspace=own) as session:
        register_session(session)
        weaver.wipe([f"Warehouse/{warehouse.item.name}"], session=session)
    _forget_the_catalogue_schema(warehouse.executor)


@weaver_test(remote=True, resources={"rest", "tds"})
def test_a_build_introduces_a_catalogue_table_the_installation_lacks(
    fabric_workspace, catalogue_of_its_own, tmp_path_factory
):
    """Bootstrap, drop every runtime table, build again, and read the estate once.

    The second build is the upgrade: it reads a catalogue whose shape has none of
    the runtime tables, so nothing may plan reconciliation against them, and the
    same bundle must leave all of them there. Every one at once rather than one
    of them, because an installation predating the operational-state model lacks
    the whole family and the build that catches it up is one build.
    """

    warehouse = catalogue_of_its_own
    name = warehouse.item.name
    own_catalogue = replace(fabric_workspace, catalogue=f"Warehouse/{name}")
    estate = WAREHOUSE_ESTATE_FIXTURE.disposable(tmp_path_factory.mktemp("upgrade"))
    bind = f"Warehouse/{name}=Reporting"

    # Setup, not the claim. This Warehouse is the catalogue *and* the estate's
    # target, so both items of the build want `_`: the built-in item for the
    # catalogue tables, and the estate's for its load procedures. Each plans to
    # create it and the second fails, which is a defect of its own — an installed
    # catalogue is where this test starts, so it puts the schema there.
    warehouse.executor.execute_script(
        "if schema_id(N'_') is null exec('create schema [_]');"
    )

    # One Warehouse holding both `_` and the user's own schemas, which is a
    # supported arrangement: Weaver owns `_` there and nothing else.
    first = _built(own_catalogue, estate.path, bind)
    assert first.status == "succeeded", _failures(first)

    # Older than this Weaver, as an installation predating the tables would be.
    warehouse.executor.execute_script(
        "\n".join(
            f"drop table if exists [_].[{table.name}];" for table in RUNTIME_TABLES
        )
    )
    shape = _catalogue_shape(own_catalogue)
    assert not {table.name.casefold() for table in RUNTIME_TABLES} & shape

    second = _built(own_catalogue, estate.path, bind)
    assert second.status == "succeeded", _failures(second)

    upgraded = _catalogue_shape(own_catalogue)
    assert {table.name.casefold() for table in RUNTIME_TABLES} <= upgraded

    # A third build reconciles against the table rather than introducing it, so
    # it is the ordinary case again. That it plans nothing is the core suite's
    # fixed-point claim; what is worth a Fabric round trip is that it runs.
    third = _built(own_catalogue, estate.path, bind)
    assert third.status == "succeeded", _failures(third)


@weaver_test(remote=True, resources={"rest", "tds"})
def test_a_build_recovers_catalogue_certification_without_recreating_tables(
    fabric_workspace, catalogue_of_its_own, tmp_path_factory
):
    """Losing Registry cannot authorize replacement of protected structure."""

    warehouse = catalogue_of_its_own
    name = warehouse.item.name
    own_catalogue = replace(fabric_workspace, catalogue=f"Warehouse/{name}")
    estate = WAREHOUSE_ESTATE_FIXTURE.disposable(
        tmp_path_factory.mktemp("catalogue-recovery")
    )
    bind = f"Warehouse/{name}=Reporting"
    warehouse.executor.execute_script(
        "if schema_id(N'_') is null exec('create schema [_]');"
    )

    first = _built(own_catalogue, estate.path, bind)
    assert first.status == "succeeded", _failures(first)

    warehouse.executor.execute_script(
        "delete from [_].[Registry] "
        "where [Item type] = N'Warehouse' and [Item name] = N'_weaver';"
    )
    second = _built(own_catalogue, estate.path, bind)
    assert second.status == "succeeded", _failures(second)

    evidence = warehouse.executor.query_result_sets(
        "select count(*) as physical_tables from sys.tables "
        "where schema_name(schema_id) = N'_'; "
        "select count(*) as certified_tables from [_].[Registry] "
        "where [Item type] = N'Warehouse' and [Item name] = N'_weaver';"
    )
    expected = len(CATALOGUE_TABLES)
    assert int(evidence[0][0]["physical_tables"]) == expected
    assert int(evidence[1][0]["certified_tables"]) == expected

    third = _built(own_catalogue, estate.path, bind)
    assert third.status == "succeeded", _failures(third)


@weaver_test(remote=True, resources={"rest", "tds"})
def test_failed_build_converges_from_inventory_and_reseeds_from_initial_bookmark(
    fabric_workspace, catalogue_of_its_own, tmp_path_factory
):
    warehouse = catalogue_of_its_own
    name = warehouse.item.name
    own_catalogue = replace(fabric_workspace, catalogue=f"Warehouse/{name}")
    root = tmp_path_factory.mktemp("failed-build-convergence") / "estate"
    bind = f"Warehouse/{name}=Convergence"
    warehouse.executor.execute_script(
        "if schema_id(N'_') is null exec('create schema [_]');"
    )

    _write_convergence_estate(root, revised=False, broken=False)
    first = _built(own_catalogue, root, bind)
    assert first.status == "succeeded", _failures(first)
    _run_standalone_load(warehouse.executor)
    seeded = warehouse.executor.query(
        "select "
        "(select count(*) from [Wh].[AIncremental]) as target_rows, "
        "(select count(*) from [_].[Bookmark] "
        " where [Item type] = N'Warehouse' and [Item name] = N'Convergence' "
        "   and [Schema name] = N'Wh' and [Object name] = N'AIncremental') "
        "as bookmarks;"
    )[0]
    assert (int(seeded["target_rows"]), int(seeded["bookmarks"])) == (3, 1)

    _write_convergence_estate(root, revised=True, broken=True)
    failed = _built(own_catalogue, root, bind)
    assert failed.status == "failed"
    partial = warehouse.executor.query_result_sets(
        "select count(*) as revised_columns from sys.columns "
        "where object_id = object_id(N'[Wh].[AIncremental]') "
        "and name = N'Revision'; "
        "select count(*) as certifications from [_].[Registry] "
        "where [Item type] = N'Warehouse' and [Item name] = N'Convergence' "
        "and [Schema name] = N'Wh' and [Object name] = N'AIncremental'; "
        "select count(*) as bookmarks from [_].[Bookmark] "
        "where [Item type] = N'Warehouse' and [Item name] = N'Convergence' "
        "and [Schema name] = N'Wh' and [Object name] = N'AIncremental';"
    )
    assert (
        int(partial[0][0]["revised_columns"]),
        int(partial[1][0]["certifications"]),
        int(partial[2][0]["bookmarks"]),
    ) == (1, 0, 0)

    _write_convergence_estate(root, revised=True, broken=False)
    recovered = _built(own_catalogue, root, bind)
    assert recovered.status == "succeeded", _failures(recovered)
    _run_standalone_load(warehouse.executor)
    reseeded = warehouse.executor.query(
        "select "
        "(select count(*) from [Wh].[AIncremental]) as target_rows, "
        "(select count(*) from [_].[Bookmark] "
        " where [Item type] = N'Warehouse' and [Item name] = N'Convergence' "
        "   and [Schema name] = N'Wh' and [Object name] = N'AIncremental') "
        "as bookmarks;"
    )[0]
    assert (int(reseeded["target_rows"]), int(reseeded["bookmarks"])) == (3, 1)

    _run_standalone_load(warehouse.executor)
    incremental = warehouse.executor.query(
        "select top 1 [Rows read] as rows_read "
        "from [_].[LoadStatistic] "
        "where [Item type] = N'Warehouse' and [Item name] = N'Convergence' "
        "  and [Schema name] = N'Wh' and [Object name] = N'AIncremental' "
        "order by [Completed datetime] desc, [Load statistic SK] desc;"
    )[0]
    assert int(incremental["rows_read"]) == 0


def _catalogue_shape(workspace) -> set[str]:
    """Which catalogue tables physically exist, as the catalogue reports them."""

    from weaver.catalogue.connection import catalogue_connection

    with ConsoleSession(workspace=workspace) as session:
        register_session(session)
        connection = catalogue_connection(session, workspace)
        connection.forget_shape()
        return {name.casefold() for name in connection.shape()}


@weaver_test(remote=True, resources={"rest", "tds"})
def test_a_built_warehouse_is_given_views_over_the_catalogues_runtime_tables(
    fabric_workspace, clean_disposable_warehouse, tmp_path_factory
):
    """What a generated procedure says ``[_].[Bookmark]`` to reach, and the rest.

    Installed by the ordinary build into a Warehouse that is *not* the catalogue,
    which is the case that needs a reference: the catalogue Warehouse holds the
    tables themselves and is given nothing.

    One assertion over the whole family rather than one per table: they are
    installed by one action and a reference missing from it would be a gap in the
    same decision.
    """

    warehouse = clean_disposable_warehouse
    name = warehouse.item.name
    estate = WAREHOUSE_ESTATE_FIXTURE.disposable(tmp_path_factory.mktemp("reference"))

    built = _built(fabric_workspace, estate.path, f"Warehouse/{name}=Reporting")
    assert built.status == "succeeded", _failures(built)

    # Views rather than tables: the rows live in the catalogue Warehouse, and
    # this Warehouse reads and merges them across the boundary through them.
    present = user_objects(warehouse.executor)
    assert {
        CatalogObject(schema="_", name=table.name, kind="V")
        for table in PRESENTED_RUNTIME_TABLES
    } <= present
    # And each resolves — a three-part name in another database, selected here.
    for table in PRESENTED_RUNTIME_TABLES:
        counted = warehouse.executor.query(
            f"select count(*) as n from [_].[{table.name}]"
        )
        assert int(dict(counted[0])["n"]) >= 0, table.name


@weaver_test(remote=True, resources={"rest", "tds"})
def test_a_generated_load_reads_the_catalogue_through_a_consumer_warehouse_view(
    fabric_workspace, clean_disposable_warehouse, tmp_path_factory
):
    """Exercise ``Warehouse/consumer/_.Bookmark -> Warehouse/catalogue``."""

    warehouse = clean_disposable_warehouse
    name = warehouse.item.name
    root = tmp_path_factory.mktemp("runtime-reference") / "estate"
    estate = _write_runtime_reference_estate(root)

    built = _built(
        fabric_workspace,
        estate,
        f"Warehouse/{name}=RuntimeReference",
    )
    assert built.status == "succeeded", _failures(built)

    for _ in range(2):
        warehouse.executor.execute_script(
            "exec [_].[Load] @object_name = N'Wh.Customer', @fault_tolerant = 0;"
        )

    evidence = warehouse.executor.query_result_sets(
        "select count(*) as target_rows from [Wh].[Customer]; "
        "select count(*) as bookmarks from [_].[Bookmark] "
        "where [Item type] = N'Warehouse' "
        "and [Item name] = N'RuntimeReference' "
        "and [Schema name] = N'Wh' and [Object name] = N'Customer'; "
        "select top 1 [Rows read] as rows_read from [_].[LoadStatistic] "
        "where [Item type] = N'Warehouse' "
        "and [Item name] = N'RuntimeReference' "
        "and [Schema name] = N'Wh' and [Object name] = N'Customer' "
        "order by [Completed datetime] desc, [Load statistic SK] desc;"
    )
    assert int(evidence[0][0]["target_rows"]) == 2
    assert int(evidence[1][0]["bookmarks"]) == 1
    assert int(evidence[2][0]["rows_read"]) == 0


#: Which statement drops each kind of object, in the order dependencies allow: a
#: view over a table goes before the table does.
_DROP = (("V", "view"), ("P", "procedure"), ("U", "table"))


def _forget_the_catalogue_schema(executor) -> None:
    """Leave this Warehouse holding no ``_`` at all."""

    held = {object for object in user_objects(executor) if object.schema == "_"}
    for kind, statement in _DROP:
        for object in sorted(held):
            if object.kind == kind:
                executor.execute_script(f"drop {statement} [_].[{object.name}];")
    executor.execute_script("if schema_id(N'_') is not null exec('drop schema [_]');")
