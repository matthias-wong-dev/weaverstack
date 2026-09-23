"""What a build does about catalogue tables: introduce one, and reference one.

Two claims that need a real Warehouse and nothing else.

Adding a table to ``_`` makes every existing installation older than the Weaver
building against it. The build that introduces the table has to plan against a
catalogue that does not have it yet, create it, and reconcile against it from
then on, and it has to do all three in one bundle, because every build binds
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

import pytest
from sql_support import CatalogObject, user_objects
from support.build_envs import WAREHOUSE_ESTATE_FIXTURE
from support.weaver_test import register_session, weaver_test

import weaver
from weaver.catalogue.tables import (
    CATALOGUE_TABLES,
    RUNTIME_TABLES,
    STANDARD_SURFACE_TABLES,
)
from weaver.sessions import ConsoleSession


def _built(workspace, estate, target):
    with ConsoleSession(workspace=workspace) as session:
        register_session(session)
        return weaver.build(str(estate), items=[target], session=session)


def _failures(report):
    return [(failure.action_id, failure.message) for failure in report.errors]


@pytest.fixture
def catalogue_of_its_own(fabric_workspace, clean_disposable_warehouse):
    """The disposable Warehouse, emptied of the catalogue it held, afterwards.

    Every other Warehouse test uses this item as an ordinary target, recorded in
    the shared catalogue: there ``_.Bookmark`` is a view over the catalogue's
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


@pytest.mark.slow
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
    target = f"Warehouse/Reporting=Warehouse/{name}"

    # Setup, not the claim. This Warehouse is the catalogue and the estate's
    # target, so both items of the build want `_`: the built-in item for the
    # catalogue tables, and the estate's for its load procedures. Each plans to
    # create it and the second fails, which is a defect of its own, an installed
    # catalogue is where this test starts, so it puts the schema there.
    warehouse.executor.execute_script(
        "if schema_id(N'_') is null exec('create schema [_]');"
    )

    # One Warehouse holding both `_` and the user's own schemas, which is a
    # supported arrangement: Weaver owns `_` there and nothing else.
    first = _built(own_catalogue, estate.path, target)
    assert first.status == "succeeded", _failures(first)

    # Older than this Weaver, as an installation predating the tables would be.
    warehouse.executor.execute_script(
        "\n".join(
            f"drop table if exists [_].[{table.name}];" for table in RUNTIME_TABLES
        )
    )
    shape = _catalogue_shape(own_catalogue)
    assert not {table.name.casefold() for table in RUNTIME_TABLES} & shape

    second = _built(own_catalogue, estate.path, target)
    assert second.status == "succeeded", _failures(second)

    upgraded = _catalogue_shape(own_catalogue)
    assert {table.name.casefold() for table in RUNTIME_TABLES} <= upgraded

    # A third build reconciles against the table rather than introducing it, so
    # it is the ordinary case again. That it plans nothing is the core suite's
    # fixed-point claim; what is worth a Fabric round trip is that it runs.
    third = _built(own_catalogue, estate.path, target)
    assert third.status == "succeeded", _failures(third)


@pytest.mark.slow
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
    target = f"Warehouse/Reporting=Warehouse/{name}"
    warehouse.executor.execute_script(
        "if schema_id(N'_') is null exec('create schema [_]');"
    )

    first = _built(own_catalogue, estate.path, target)
    assert first.status == "succeeded", _failures(first)

    warehouse.executor.execute_script(
        "delete from [_].[Registry] "
        "where [Item type] = N'Warehouse' and [Item name] = N'_weaver';"
    )
    second = _built(own_catalogue, estate.path, target)
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

    third = _built(own_catalogue, estate.path, target)
    assert third.status == "succeeded", _failures(third)


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

    Installed by the ordinary build into a Warehouse that is not the catalogue,
    which is the case that needs a reference: the catalogue Warehouse holds the
    tables themselves and is given nothing.

    One assertion over the whole family rather than one per table: they are
    installed by one action and a reference missing from it would be a gap in the
    same decision.
    """

    warehouse = clean_disposable_warehouse
    name = warehouse.item.name
    estate = WAREHOUSE_ESTATE_FIXTURE.disposable(tmp_path_factory.mktemp("reference"))

    built = _built(
        fabric_workspace, estate.path, f"Warehouse/Reporting=Warehouse/{name}"
    )
    assert built.status == "succeeded", _failures(built)

    # Views rather than tables: the rows live in the catalogue Warehouse, and
    # this Warehouse reads and merges them across the boundary through them.
    present = user_objects(warehouse.executor)
    assert {
        CatalogObject(schema="_", name=table.name, kind="V")
        for table in STANDARD_SURFACE_TABLES
    } <= present
    # And each resolves, a three-part name in another database, selected here.
    for table in STANDARD_SURFACE_TABLES:
        read = warehouse.executor.query(f"select top (1) * from [_].[{table.name}]")
        assert len(read) <= 1, table.name


#: Leave this Warehouse holding no ``_`` at all, in one round trip. Views before
#: the procedures and tables they read, which is the order dependencies allow.
#: A built catalogue holds thirty or more objects and this runs in a teardown
#: three tests share, so `string_agg` builds the drops and `sp_executesql`
#: runs them.
_FORGET_CATALOGUE_SQL = """\
set nocount on;

declare @weaver_sql nvarchar(max);

select
    @weaver_sql = string_agg(
        convert(nvarchar(max), N'drop view [_].' + quotename(name) + N';'),
        char(10)
    ) within group (order by object_id)
from sys.views
where schema_name(schema_id) = N'_';

if @weaver_sql is not null
begin
    exec sys.sp_executesql @weaver_sql;
end;

set @weaver_sql = null;
select
    @weaver_sql = string_agg(
        convert(nvarchar(max), N'drop procedure [_].' + quotename(name) + N';'),
        char(10)
    ) within group (order by object_id)
from sys.procedures
where schema_name(schema_id) = N'_';

if @weaver_sql is not null
begin
    exec sys.sp_executesql @weaver_sql;
end;

set @weaver_sql = null;
select
    @weaver_sql = string_agg(
        convert(nvarchar(max), N'drop table [_].' + quotename(name) + N';'),
        char(10)
    ) within group (order by object_id)
from sys.tables
where schema_name(schema_id) = N'_';

if @weaver_sql is not null
begin
    exec sys.sp_executesql @weaver_sql;
end;

if schema_id(N'_') is not null exec('drop schema [_]');
"""


def _forget_the_catalogue_schema(executor) -> None:
    """Leave this Warehouse holding no ``_`` at all."""

    executor.execute_script(_FORGET_CATALOGUE_SQL)
