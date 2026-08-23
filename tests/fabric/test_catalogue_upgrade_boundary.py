"""What a build does about catalogue tables: introduce one, and reference one.

Two claims that need a real Warehouse and nothing else.

Adding a table to ``_`` makes every existing installation older than the Weaver
building against it. The build that introduces the table has to plan against a
catalogue that does not have it yet, create it, and reconcile against it from
then on — and it has to do all three in one bundle, because every build binds
``_weaver`` and so gets the new table from the same bundle it would have needed
it for.

The other is the reference: a Warehouse that is not the catalogue holds a view
over the catalogue's ``_.Bookmark``, which is how a generated load procedure
reaches its own row.

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
from weaver.catalogue.tables import BOOKMARK
from weaver.sessions import ConsoleSession


def _built(workspace, estate, bind):
    with ConsoleSession(workspace=workspace) as session:
        register_session(session)
        return weaver.build(str(estate), bind=[bind], session=session)


def _failures(report):
    return [(failure.action_id, failure.message) for failure in report.errors]


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
    """Bootstrap, drop the table, build again, and read the estate once.

    The second build is the upgrade: it reads a catalogue whose shape has no
    ``_.Bookmark``, so nothing may plan reconciliation against it, and the same
    bundle must leave it there.
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

    # Older than this Weaver, as an installation predating the table would be.
    warehouse.executor.execute_script(f"drop table [_].[{BOOKMARK.name}];")
    shape = _catalogue_shape(own_catalogue)
    assert BOOKMARK.name.casefold() not in shape

    second = _built(own_catalogue, estate.path, bind)
    assert second.status == "succeeded", _failures(second)

    upgraded = _catalogue_shape(own_catalogue)
    assert BOOKMARK.name.casefold() in upgraded

    # A third build reconciles against the table rather than introducing it, so
    # it is the ordinary case again. That it plans nothing is the core suite's
    # fixed-point claim; what is worth a Fabric round trip is that it runs.
    third = _built(own_catalogue, estate.path, bind)
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
def test_a_built_warehouse_is_given_a_view_over_the_catalogues_bookmark(
    fabric_workspace, clean_disposable_warehouse, tmp_path_factory
):
    """What a generated load procedure says ``[_].[Bookmark]`` to reach.

    Installed by the ordinary build into a Warehouse that is *not* the catalogue,
    which is the case that needs a reference: the catalogue Warehouse holds the
    table itself and is given nothing.
    """

    warehouse = clean_disposable_warehouse
    name = warehouse.item.name
    estate = WAREHOUSE_ESTATE_FIXTURE.disposable(tmp_path_factory.mktemp("reference"))

    built = _built(fabric_workspace, estate.path, f"Warehouse/{name}=Reporting")
    assert built.status == "succeeded", _failures(built)

    # A view rather than a table: the rows live in the catalogue Warehouse, and
    # this Warehouse reads and merges them across the boundary through it.
    assert CatalogObject(schema="_", name=BOOKMARK.name, kind="V") in user_objects(
        warehouse.executor
    )
    # And it resolves — a three-part name in another database, selected here.
    counted = warehouse.executor.query(
        f"select count(*) as n from [_].[{BOOKMARK.name}]"
    )
    assert int(dict(counted[0])["n"]) >= 0


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
