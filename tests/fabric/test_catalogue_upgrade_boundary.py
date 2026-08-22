"""Building against a catalogue that is missing a table Weaver now owns.

Adding a table to ``_`` makes every existing installation older than the Weaver
building against it. The build that introduces the table has to plan against a
catalogue that does not have it yet, create it, and reconcile against it from
then on — and it has to do all three in one bundle, because every build binds
``_weaver`` and so gets the new table from the same bundle it would have needed
it for.

``remote`` and Warehouse-only: the catalogue is a Warehouse, the estate's objects
are T-SQL, so nothing here starts Spark. It runs against a disposable Warehouse
that is its own catalogue, so dropping a catalogue table cannot touch the shared
estate the rest of the suite depends on.
"""

from __future__ import annotations

from dataclasses import replace

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


@weaver_test(remote=True, resources={"rest", "tds"})
def test_a_build_introduces_a_catalogue_table_the_installation_lacks(
    fabric_workspace, clean_disposable_warehouse, tmp_path_factory
):
    """Bootstrap, drop the table, build again, and read the estate once.

    The second build is the upgrade: it reads a catalogue whose shape has no
    ``_.Bookmark``, so nothing may plan reconciliation against it, and the same
    bundle must leave it there.
    """

    warehouse = clean_disposable_warehouse
    name = warehouse.item.name
    own_catalogue = replace(fabric_workspace, catalogue=f"Warehouse/{name}")
    estate = WAREHOUSE_ESTATE_FIXTURE.disposable(tmp_path_factory.mktemp("upgrade"))
    bind = f"Warehouse/{name}=Reporting"
    target = f"Warehouse/{name}"

    # The fixture wipes this Warehouse as an ordinary target, which spares `_`
    # because `_` is the catalogue's. Here it *is* the catalogue, so wiping it
    # again under that name is what leaves nothing for the first build to find.
    with ConsoleSession(workspace=own_catalogue) as session:
        register_session(session)
        weaver.wipe([target], session=session)

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
