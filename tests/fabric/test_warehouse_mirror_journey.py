"""One Warehouse item mirrored, then driven through the ordinary lifecycle.

Build a source estate, mirror the item into another Warehouse, run its
installed validations there, build again with nothing changed, then change one
declaration and build again, then load what that build materialised.

A tenant answers what only Fabric can: whether a three-part View resolves
across Warehouses, whether a row written at the source is visible through one,
and whether a copied procedure executes where it was copied to. What the mirror
decides is settled in ``tests/targeted/test_mirror_lifecycle_cycle.py``.

Steps run in file order and do not cascade.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime

import pytest
from mirror_support import forked_config
from sql_support import warehouse_sql
from support.acceptance import Acceptance
from support.build_envs import WAREHOUSE_ESTATE_FIXTURE
from support.weaver_test import register_session, weaver_test

import weaver
from weaver.catalogue.tables import (
    BOOKMARK_SENTINEL,
    CATALOGUE_SCHEMA,
    PENDING,
    SUCCEEDED,
)
from weaver.catalogue.tables import LOAD_STATUS as LOAD_STATUS_TABLE
from weaver.catalogue.tables import MIRROR as MIRROR_TABLE

ITEM = "Warehouse/Reporting"

#: A row the source holds and the mirror has no copy of.
SENTINEL = (99, "Borrowed")

#: What the row is set to at the source once the mirror stands, so reading it
#: back through the View proves nothing was copied.
CHANGED = "Changed"

#: The object whose declaration changes, and the one every later claim watches.
MATERIALISED = "Wh.Product"

#: The object downstream of it, unchanged, and rebuilt because it is a
#: dependency impact. It carries the state a mirror borrowed into this
#: catalogue, so the build has to end that state before it drops the View.
DEPENDANT = "Wh.ProductRollup"

DEPENDANT_SOURCE = f"""/*
Table ID: {DEPENDANT}

Description: Products, rolled up from the base table.

Lineage: $Wh.Product

Primary key: ProductId
*/
select p.ProductId, p.ProductName
from [Wh].[Product] as p;
"""

#: The object whose query reads the ``_`` surface, which puts it downstream of
#: ``Warehouse/_weaver`` in the dependency graph. A fork copies every row as it
#: stands and the destination's ``Warehouse/_weaver`` rows are its own
#: catalogue build, so this stays borrowed only while freshness leaves the
#: catalogue item's instant out of the comparison.
SURFACE_READER = "Wh.CustomerDelta"

SURFACE_READER_SOURCE = f"""/*
Table ID: {SURFACE_READER}

Description: Customers read through this table's own bookmark.

Lineage: $Wh.Customer

Primary key: CustomerId
*/
declare @bookmark datetime2(6) = (
    select b.[Bookmark datetime]
      from [_].[Bookmark] as b
     where b.[Item name] = 'Reporting'
       and b.[Schema name] = 'Wh'
       and b.[Object name] = 'CustomerDelta'
);

select c.CustomerId, c.CustomerName
from [Wh].[Customer] as c
where @bookmark is null or c.CustomerId > 0;
"""

#: The validation the mirrored Warehouse runs. Both sides read borrowed Views.
VALIDATION = "Rpt.CustomerOrdersReconcile"

VALIDATION_SOURCE = f"""/*
Test ID: {VALIDATION}

Description: Every customer has an order row.

Primary key: CustomerId
*/
select CustomerId from [Wh].[Customer];

select CustomerId from [Wh].[CustomerOrder];
"""

_OBJECTS = (
    "select schema_name(o.schema_id) as s, o.name as n, o.type as t "
    "from sys.objects as o "
    "where o.is_ms_shipped = 0 and o.type in (N'U', N'V', N'P') "
    "order by s, o.name"
)


@dataclass(frozen=True)
class Estate:
    """What the estate holds at one moment, read after one transition."""

    #: ``Schema.Object`` to ``U``, ``V`` or ``P``, in the mirrored Warehouse.
    objects: dict[str, str]
    #: ``Schema.Object`` to physical type, from ``_.Mirror``.
    borrowed: dict[str, str]
    #: Item name to target name, from ``_.Installation``.
    installed: dict[str, str]
    #: Whether each data node has a load to dispatch, from the installed graph.
    loadable: dict[str, bool]
    #: ``Wh.Product`` at the source, which no build in this journey may touch.
    source_rows: list[tuple]
    #: ``Schema.Object`` to its ``_.LoadStatus`` result, from the fork catalogue.
    load_status: dict[str, str]
    #: ``Schema.Object`` to its ``_.Bookmark`` instant, as text.
    bookmarks: dict[str, str]
    #: ``Wh.ProductRollup`` in the mirrored Warehouse, once it holds its own rows.
    dependant_rows: list[tuple]
    #: ``Schema.Object`` to its ``_.LoadStatus`` result in the catalogue the
    #: mirror borrows from, which is where the inherited state comes from.
    source_load_status: dict[str, str]
    #: Every ``_.Registry`` identity with its signature and build instant, from
    #: the fork catalogue and from its source, without the catalogue's own rows.
    registry: frozenset[tuple]
    source_registry: frozenset[tuple]


# --- the journey --------------------------------------------------------------


@pytest.fixture(scope="module")
def journey(
    fabric_workspace,
    fabric_catalogue,
    fabric_fork_catalogue,
    fabric_mirror_warehouse,
    fabric_initialise_catalogue,
    session_disposable_warehouse,
    warehouse_session,
    tmp_path_factory,
    request,
):
    """One estate: built, mirrored, validated, rebuilt, then materialised."""

    register_session(warehouse_session)
    # An earlier module may have emptied the catalogue Warehouse. Every step
    # here reads the runtime catalogue, so stand it up before the first build.
    fabric_initialise_catalogue()
    estate = WAREHOUSE_ESTATE_FIXTURE.disposable(tmp_path_factory.mktemp("mirror"))
    _write(estate, f"{ITEM}/tests/{VALIDATION}.sql", VALIDATION_SOURCE)
    _write(estate, f"{ITEM}/{SURFACE_READER}.sql", SURFACE_READER_SOURCE)
    _write(estate, f"{ITEM}/{DEPENDANT}.sql", DEPENDANT_SOURCE)

    run = Acceptance(name="warehouse-mirror")
    run.source_name = session_disposable_warehouse.item.name
    run.target_name = fabric_mirror_warehouse.name
    run.catalogue_name = fabric_fork_catalogue.name
    run.source_catalogue_name = fabric_catalogue.name
    run.workspace = fabric_workspace
    run.source_sql = warehouse_sql(warehouse_session, fabric_workspace, run.source_name)
    run.target_sql = warehouse_sql(warehouse_session, fabric_workspace, run.target_name)
    run.catalogue_sql = warehouse_sql(
        warehouse_session, fabric_workspace, run.catalogue_name
    )
    run.source_catalogue_sql = warehouse_sql(
        warehouse_session, fabric_workspace, run.source_catalogue_name
    )
    run.forked = replace(fabric_workspace, catalogue=f"Warehouse/{run.catalogue_name}")
    run.session = warehouse_session
    into_mirror = [f"{ITEM}=Warehouse/{run.target_name}"]
    # Steps whose only readers run with --runslow.
    release = request.config.getoption("--runslow")

    run.step(
        "build the source",
        lambda: weaver.build(
            str(estate.path),
            items=[f"{ITEM}=Warehouse/{run.source_name}"],
            session=warehouse_session,
        ),
    )
    # The state a mirror copies in has to be settled state, or the build that
    # materialises a borrowed object has nothing to overwrite and the claim
    # about overwriting it would pass against an estate that never held one.
    run.step(
        "load the source",
        lambda: weaver.load([ITEM], session=warehouse_session),
    )
    run.step("seed the source", lambda: _seed(run.source_sql))
    run.step(
        "mirror",
        lambda: weaver.mirror(
            into_mirror,
            session=warehouse_session,
            workspace=fabric_workspace.workspace,
            catalogue=f"Warehouse/{run.catalogue_name}",
            mirror=f"Warehouse/{fabric_catalogue.name}",
        ),
        observe=lambda: _observe(run),
    )
    if release:
        run.step(
            "mirror again",
            lambda: weaver.mirror(
                into_mirror,
                session=warehouse_session,
                workspace=fabric_workspace.workspace,
                catalogue=f"Warehouse/{run.catalogue_name}",
                mirror=f"Warehouse/{fabric_catalogue.name}",
            ),
            observe=lambda: _observe(run),
        )
    run.step("read a source change through the mirror", lambda: _read_through(run))
    run.step(
        "report health over the mirror",
        lambda: weaver.health(
            [ITEM],
            session=warehouse_session,
            workspace_config=forked_config(run, tmp_path_factory.mktemp("wh-health")),
        ),
    )
    run.step(
        "validate the mirror",
        lambda: weaver.test(
            [ITEM],
            session=warehouse_session,
            workspace=fabric_workspace.workspace,
            catalogue=f"Warehouse/{run.catalogue_name}",
        ),
    )
    run.step(
        "build with nothing changed",
        lambda: weaver.build(
            str(estate.path),
            items=into_mirror,
            session=warehouse_session,
            catalogue=f"Warehouse/{run.catalogue_name}",
        ),
        observe=lambda: _observe(run),
    )
    run.step("change one declaration", lambda: _change(estate))
    run.step(
        "build the changed declaration",
        lambda: weaver.build(
            str(estate.path),
            items=into_mirror,
            session=warehouse_session,
            catalogue=f"Warehouse/{run.catalogue_name}",
        ),
        observe=lambda: _observe(run),
    )
    run.loading_config = forked_config(run, tmp_path_factory.mktemp("wh-load"))
    run.step(
        "select a stale load",
        lambda: weaver.load(
            [ITEM],
            stale=True,
            dry_run=True,
            session=warehouse_session,
            workspace_config=run.loading_config,
        ),
        observe=lambda: _observe(run),
    )
    run.step(
        "load what the build materialised",
        lambda: weaver.load(
            [ITEM],
            stale=True,
            session=warehouse_session,
            workspace_config=run.loading_config,
        ),
        observe=lambda: _observe(run),
    )
    yield run
    run.close()


# --- driving it ---------------------------------------------------------------


def _write(estate, relative: str, text: str) -> None:
    path = estate.path / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _seed(sql) -> None:
    """One row at the source, so reading through the mirror proves something."""

    sql.execute(
        "delete from [Wh].[Product];\n"
        "insert into [Wh].[Product] ([ProductId], [ProductName], [Row signature], "
        "[Row insert datetime], [Row update datetime], [Row delete datetime]) "
        f"values ({SENTINEL[0]}, N'{SENTINEL[1]}', "
        f"hashbytes('SHA2_256', N'{SENTINEL[1]}'), sysdatetime(), sysdatetime(), "
        "convert(datetime2(6), '9999-12-31 23:59:59.999999'));"
    )


def _read_through(run) -> list[str]:
    """Change the row at the source and read it back through the mirror."""

    run.source_sql.execute(
        f"update [Wh].[Product] set [ProductName] = N'{CHANGED}' "
        f"where [ProductId] = {SENTINEL[0]};"
    )
    return [
        str(row["ProductName"])
        for row in run.target_sql.query(
            f"select [ProductName] from [Wh].[Product] "
            f"where [ProductId] = {SENTINEL[0]}"
        )
    ]


def _change(estate) -> None:
    """Change one declaration, leaving every other object as it was."""

    path = estate.path / ITEM / f"{MATERIALISED}.sql"
    text = path.read_text(encoding="utf-8")
    changed = text.replace("'Widget'", "'Widget II'")
    assert changed != text, "the declaration to change was not found"
    path.write_text(changed, encoding="utf-8")


def _asked(sql, *queries: str) -> list[list[dict]]:
    """Several reads of one Warehouse, in one round trip."""

    return [
        [dict(row) for row in rows]
        for rows in sql.query_result_sets(";\n".join(queries) + ";")
    ]


def _observe(run) -> Estate:
    """The estate as this transition left it, one round trip per Warehouse."""

    from weaver.catalogue.state import catalogue_for

    with catalogue_for(run.session, run.forked) as catalogue:
        dag = catalogue.dag()
    objects, dependant = _asked(run.target_sql, _OBJECTS, _DEPENDANT_ROWS)
    borrowed, installed, load_status, bookmarks, registry = _asked(
        run.catalogue_sql,
        "select [Schema name], [Object name], [Physical type] "
        f"from {_q(MIRROR_TABLE.name)}",
        "select [Item name], [Target name] from [_].[Installation]",
        f"select [Schema name], [Object name], [Result] from {_q('LoadStatus')}",
        "select [Schema name], [Object name], [Bookmark datetime] "
        f"from {_q('Bookmark')}",
        _REGISTRY,
    )
    source_load_status, source_registry = _asked(
        run.source_catalogue_sql,
        "select [Schema name], [Object name], [Result] "
        f"from {_q('LoadStatus')} where [Item name] = N'Reporting'",
        _REGISTRY,
    )
    return Estate(
        objects={f"{row['s']}.{row['n']}": str(row["t"]).strip() for row in objects},
        borrowed={
            f"{row['Schema name']}.{row['Object name']}": str(row["Physical type"])
            for row in borrowed
        },
        installed={str(row["Item name"]): str(row["Target name"]) for row in installed},
        loadable={
            node.load_name: node.can_load
            for node in dag.nodes
            if str(node.item) == ITEM and node.load_name
        },
        source_rows=[
            (int(row["ProductId"]), str(row["ProductName"]))
            for row in run.source_sql.query(
                "select [ProductId], [ProductName] from [Wh].[Product]"
            )
        ],
        load_status={
            f"{row['Schema name']}.{row['Object name']}": str(row["Result"])
            for row in load_status
        },
        bookmarks={
            f"{row['Schema name']}.{row['Object name']}": _instant(
                row["Bookmark datetime"]
            )
            for row in bookmarks
        },
        dependant_rows=_rows_of(dependant),
        source_load_status={
            f"{row['Schema name']}.{row['Object name']}": str(row["Result"])
            for row in source_load_status
        },
        registry=_signatures(registry),
        source_registry=_signatures(source_registry),
    )


#: What incremental selection compares, for every item but the catalogue's own.
_REGISTRY = (
    "select [Item type], [Item name], [Schema name], [Object name], [Signature], "
    f"[Build datetime] from [{CATALOGUE_SCHEMA}].[Registry] "
    "where not ([Item type] = N'Warehouse' and [Item name] = N'_weaver')"
)


def _signatures(rows) -> frozenset[tuple]:
    return frozenset(
        (
            str(row["Item type"]),
            str(row["Item name"]),
            str(row["Schema name"]),
            str(row["Object name"]),
            str(row["Signature"]),
            None if row["Build datetime"] is None else _instant(row["Build datetime"]),
        )
        for row in rows
    )


def _instant(value) -> datetime:
    """A bookmark as an instant, so a comparison is not about driver spelling."""

    return value if isinstance(value, datetime) else datetime.fromisoformat(str(value))


#: ``Wh.ProductRollup`` where it stands, which is a View until it is built.
_DEPENDANT_ROWS = (
    f"select [ProductId], [ProductName] from [{DEPENDANT.replace('.', '].[')}] "
    "order by [ProductId]"
)


def _rows_of(rows) -> list[tuple]:
    return [(int(row["ProductId"]), str(row["ProductName"])) for row in rows]


def _result(value: str) -> str:
    """A load result as ``_.LoadStatus`` stores it, not as Weaver names it."""

    return str(LOAD_STATUS_TABLE.column("result").to_public(value))


def _q(name: str) -> str:
    return f"[{CATALOGUE_SCHEMA}].[{name}]"


def _relations(estate: Estate) -> dict[str, str]:
    """The item's own objects, without the ``_`` surface Weaver puts there."""

    return {
        name: kind
        for name, kind in estate.objects.items()
        if not name.startswith(f"{CATALOGUE_SCHEMA}.")
    }


# --- what the mirror stood up -------------------------------------------------


@weaver_test(remote=True)
def test_every_data_relation_becomes_a_view(journey):
    """A table and a view at the source both read the same way through one."""

    journey.require("mirror")
    relations = _relations(journey["mirror"].observation)

    assert relations["Wh.Product"] == "V"
    assert relations["Wh.Customer"] == "V"
    # The source's own view is a view here too, over the source's view.
    assert relations["Rpt.CustomerSummary"] == "V"
    assert not [name for name, kind in relations.items() if kind == "U"], (
        "a mirrored Warehouse holds no table of its own"
    )


@weaver_test(remote=True)
def test_the_rows_stay_where_they_were(journey):
    """Zero copy: a row written at the source is read through the mirror."""

    journey.require("read a source change through the mirror")

    assert journey["read a source change through the mirror"].result == [CHANGED]


@weaver_test(remote=True)
def test_the_catalogue_records_what_is_borrowed(journey):
    journey.require("mirror")
    observed = journey["mirror"].observation

    assert set(observed.borrowed) == {
        "Wh.Customer",
        "Wh.CustomerDelta",
        "Wh.CustomerDim",
        "Wh.CustomerOrder",
        "Wh.Product",
        "Wh.ProductRollup",
        "Rpt.CustomerSummary",
    }
    assert set(observed.borrowed.values()) == {"View"}
    assert not any(observed.loadable.values())


@weaver_test(remote=True)
def test_the_source_load_runs_and_settles_what_the_mirror_will_carry(journey):
    """This run's own evidence that the source was loaded, rather than residue.

    The catalogue this suite shares is reused between runs and keeps its
    ``_.LoadStatus`` rows, so a claim that the source reads Succeeded can be
    satisfied by a row an earlier run wrote. The report is what says this run
    did the work, and the settled state the mirror carries is that work's.
    """

    journey.require("load the source")
    report = journey["load the source"].result
    ran = {
        node.logical_id: node
        for node in report.nodes
        if node.logical_id and node.executed
    }

    assert ran[f"{ITEM}/{MATERIALISED}"].succeeded
    assert ran[f"{ITEM}/{DEPENDANT}"].succeeded


@weaver_test(remote=True)
def test_the_fork_inherits_the_source_catalogues_settled_state(journey):
    """What the destination starts from, and what the later build has to end.

    A mirror copies ``_.LoadStatus`` and ``_.Bookmark`` as they stand, so the
    destination opens describing loads that ran against the source's tables.
    """

    journey.require("load the source", "mirror")
    observed = journey["mirror"].observation
    sentinel = BOOKMARK_SENTINEL.replace(tzinfo=None)

    # The load that ran against the source settled it there.
    assert observed.source_load_status[MATERIALISED] == _result(SUCCEEDED)
    assert observed.source_load_status[DEPENDANT] == _result(SUCCEEDED)

    # And the destination opens holding that same state as its own.
    assert observed.load_status[MATERIALISED] == _result(SUCCEEDED)
    assert observed.load_status[DEPENDANT] == _result(SUCCEEDED)
    assert observed.bookmarks[MATERIALISED] != sentinel
    assert observed.bookmarks[DEPENDANT] != sentinel


@weaver_test(remote=True)
def test_signatures_and_instants_survive_the_fork(journey):
    """What incremental selection compares crosses the fork unchanged.

    A signature altered in transit would make every object look changed, and a
    truncated ``Build datetime`` would re-date rows no build touched.
    """

    journey.require("mirror")
    observed = journey["mirror"].observation

    assert observed.registry == observed.source_registry
    assert observed.registry, "the source certifies nothing, so equality proves nothing"


@weaver_test(remote=True)
def test_the_fork_catalogue_owns_its_own_installation_row(journey):
    """``Warehouse/_weaver`` names the Warehouse its ``_`` schema is in."""

    journey.require("mirror")

    assert journey["mirror"].observation.installed["_weaver"] == journey.catalogue_name


@weaver_test(remote=True)
def test_the_catalogues_own_tables_are_never_borrowed(journey):
    """``_`` is Weaver's own state, so the target gets real views over it."""

    journey.require("mirror")
    observed = journey["mirror"].observation

    assert not [name for name in observed.borrowed if name.startswith("_.")]
    assert f"{CATALOGUE_SCHEMA}.Installation" in observed.objects
    assert f"{CATALOGUE_SCHEMA}.{MIRROR_TABLE.name}" not in observed.objects


@weaver_test(remote=True)
def test_the_item_is_bound_to_its_new_target(journey):
    """Installation moves last, once the Views and the procedures are there."""

    journey.require("mirror")
    installed = journey["mirror"].observation.installed

    assert installed["Reporting"] == journey.target_name
    assert installed["_weaver"] != journey.target_name


@weaver_test(remote=True)
def test_the_mirrored_warehouse_holds_the_code_its_registry_certifies(journey):
    """Data borrowed, code local: the procedures came across with the mirror."""

    journey.require("mirror")
    procedures = {
        name
        for name, kind in journey["mirror"].observation.objects.items()
        if kind == "P"
    }

    assert f"{CATALOGUE_SCHEMA}.Test {VALIDATION}" in procedures
    assert f"{CATALOGUE_SCHEMA}.Load Wh.Product" in procedures


@weaver_test(remote=True)
def test_the_mirrored_warehouse_runs_its_installed_validations(journey):
    """``weaver test`` dispatches a procedure by name, and it is there."""

    journey.require("validate the mirror")
    report = journey["validate the mirror"].result

    assert report.nodes, "dispatch reached no validation"
    assert {node.status for node in report.nodes} == {"passed"}


@weaver_test(remote=True)
def test_the_result_names_every_warehouse_it_emptied(journey):
    """What the confirmation showed and what the run did are one list."""

    journey.require("mirror")
    result = journey["mirror"].result

    assert result.wiped == (
        f"Warehouse/{journey.catalogue_name}",
        f"Warehouse/{journey.target_name}",
    )
    assert result.items == (ITEM,)
    assert result.mirrored[ITEM]["source"] == f"Warehouse/{journey.source_name}"
    assert result.mirrored[ITEM]["target"] == f"Warehouse/{journey.target_name}"
    assert result.mirrored[ITEM]["relations"] == 7
    assert result.mirrored[ITEM]["programmables"] > 0


@pytest.mark.slow
@weaver_test(remote=True)
def test_mirroring_again_leaves_the_same_estate(journey):
    """A mirror is reconstruction, so a half-finished one is rerun, not repaired."""

    journey.require("mirror again")
    first = journey["mirror"].observation
    second = journey["mirror again"].observation

    assert _relations(second) == _relations(first)
    assert second.borrowed == first.borrowed


# --- an unchanged build -------------------------------------------------------


@weaver_test(remote=True)
def test_an_unchanged_build_leaves_every_relation_borrowed(journey):
    journey.require("build with nothing changed")
    before = journey["mirror"].observation
    after = journey["build with nothing changed"].observation

    assert _relations(after) == _relations(before)
    assert after.borrowed == before.borrowed


@weaver_test(remote=True)
def test_an_unchanged_build_leaves_a_surface_reader_borrowed(journey):
    """The chain through the ``_`` surface, and what a fork leaves on it.

    ``Wh.CustomerDelta`` reads ``[_].[Bookmark]``, so the graph puts it under
    the surface view over ``Warehouse/_weaver``. A fork copies every row as it
    stands, and the one row it writes for itself is the catalogue item's, which
    freshness leaves out of the comparison against a pointer.
    """

    journey.require("build with nothing changed")
    after = journey["build with nothing changed"].observation

    assert after.borrowed[SURFACE_READER] == "View"
    assert _relations(after)[SURFACE_READER] == "V"


@weaver_test(remote=True)
def test_an_unchanged_build_leaves_the_source_rows_alone(journey):
    """The rows are the source's, and a build over the mirror is not a load."""

    journey.require("build with nothing changed")

    assert journey["build with nothing changed"].observation.source_rows == [
        (SENTINEL[0], CHANGED)
    ]


# --- one changed declaration --------------------------------------------------


@weaver_test(remote=True)
def test_the_changed_object_becomes_a_local_table(journey):
    """The View is dropped and the object is built where it now belongs."""

    journey.require("build the changed declaration")
    relations = _relations(journey["build the changed declaration"].observation)

    assert relations[MATERIALISED] == "U"


@weaver_test(remote=True)
def test_a_dependency_impact_becomes_a_local_table_too(journey):
    """``Wh.ProductRollup`` is unchanged and rebuilt because its source changed."""

    journey.require("build the changed declaration")
    relations = _relations(journey["build the changed declaration"].observation)

    assert relations[DEPENDANT] == "U"


@weaver_test(remote=True)
def test_everything_unchanged_is_still_a_view(journey):
    journey.require("build the changed declaration")
    relations = _relations(journey["build the changed declaration"].observation)

    assert {
        name: kind
        for name, kind in relations.items()
        if name not in (MATERIALISED, DEPENDANT)
    } == {
        "Wh.Customer": "V",
        "Wh.CustomerDelta": "V",
        "Wh.CustomerDim": "V",
        "Wh.CustomerOrder": "V",
        "Rpt.CustomerSummary": "V",
    }


@weaver_test(remote=True)
def test_only_the_materialised_object_stops_being_borrowed(journey):
    journey.require("build the changed declaration")
    observed = journey["build the changed declaration"].observation

    assert MATERIALISED not in observed.borrowed
    assert DEPENDANT not in observed.borrowed
    assert set(observed.borrowed) == {
        "Wh.Customer",
        "Wh.CustomerDelta",
        "Wh.CustomerDim",
        "Wh.CustomerOrder",
        "Rpt.CustomerSummary",
    }


@weaver_test(remote=True)
def test_the_materialised_object_is_the_only_loadable_one(journey):
    """It holds its own rows now, so Weaver may write them."""

    journey.require("build the changed declaration")
    loadable = journey["build the changed declaration"].observation.loadable

    assert loadable[MATERIALISED] is True
    assert loadable[DEPENDANT] is True
    assert not [
        name
        for name, yes in loadable.items()
        if yes and name not in (MATERIALISED, DEPENDANT)
    ]


@weaver_test(remote=True)
def test_materialising_one_object_leaves_the_source_untouched(journey):
    """The drop reached a View in the mirror, and never the source's table."""

    journey.require("build the changed declaration")

    assert journey["build the changed declaration"].observation.source_rows == [
        (SENTINEL[0], CHANGED)
    ]


# --- health over the mirror ---------------------------------------------------


@weaver_test(remote=True)
def test_health_reads_the_mirror_and_calls_the_build_green(journey):
    """Registry says Table, ``_.Mirror`` says View, and the Warehouse holds one.

    Without ``_.Mirror`` the inventory check expects a Table at each borrowed
    address and reports every one of them missing.
    """

    journey.require("report health over the mirror")
    report = journey["report health over the mirror"].result

    assert [
        (finding.code, finding.object_id) for finding in report.build.findings
    ] == []
    assert report.build.status == "green"


# --- the runtime state a materialised object is left in -----------------------


@weaver_test(remote=True)
def test_a_materialised_object_is_left_unloaded(journey):
    """The state it borrowed described rows that are no longer at the address.

    Settled before the build, by the load that ran against the source, and
    unloaded after it. Read together with
    ``test_the_fork_inherits_the_source_catalogues_settled_state``.
    """

    journey.require("mirror", "build the changed declaration")
    observed = journey["build the changed declaration"].observation

    sentinel = BOOKMARK_SENTINEL.replace(tzinfo=None)

    assert observed.load_status[DEPENDANT] == _result(PENDING)
    assert observed.load_status[MATERIALISED] == _result(PENDING)
    assert observed.bookmarks[DEPENDANT] == sentinel
    assert observed.bookmarks[MATERIALISED] == sentinel

    # The source is untouched, so the two catalogues now disagree, which is
    # what says the destination wrote its own state rather than re-reading one.
    assert observed.source_load_status[DEPENDANT] == _result(SUCCEEDED)
    assert observed.source_load_status[MATERIALISED] == _result(SUCCEEDED)


@weaver_test(remote=True)
def test_a_materialised_table_starts_empty(journey):
    """The rows were the source's. A build creates the table, not its contents."""

    journey.require("build the changed declaration")

    assert journey["build the changed declaration"].observation.dependant_rows == []


@weaver_test(remote=True)
def test_a_stale_load_selects_every_object_the_build_materialised(journey):
    """The selection ``load --stale`` makes, before it runs anything."""

    journey.require("select a stale load")
    report = journey["select a stale load"].result
    selected = {node.logical_id for node in report.nodes if node.logical_id}

    assert f"{ITEM}/{MATERIALISED}" in selected
    assert f"{ITEM}/{DEPENDANT}" in selected
    assert f"{ITEM}/Wh.Customer" not in selected, "a borrowed object is not selected"


@weaver_test(remote=True)
def test_a_stale_load_orders_the_dependant_after_its_source(journey):
    """Both were rebuilt, so the plan carries the edge between them."""

    journey.require("select a stale load")
    report = journey["select a stale load"].result
    logical = {node.node_id: node.logical_id for node in report.nodes}
    edges = {
        (logical.get(producer), logical.get(consumer))
        for producer, consumer in report.edges
    }

    assert (f"{ITEM}/{MATERIALISED}", f"{ITEM}/{DEPENDANT}") in edges


@weaver_test(remote=True)
def test_a_stale_load_repopulates_what_the_build_materialised(journey):
    """The end of the transition: the estate holds its own rows again."""

    journey.require("load what the build materialised")
    step = journey["load what the build materialised"]

    assert step.observation.dependant_rows == [(10, "Widget II"), (20, "Gadget")]
    assert step.observation.load_status[DEPENDANT] == _result(SUCCEEDED)


@weaver_test(remote=True)
def test_a_stale_load_leaves_the_borrowed_objects_borrowed(journey):
    """A mirrored subject's rows are the source's, and a load here is not its load."""

    journey.require("load what the build materialised")
    observed = journey["load what the build materialised"].observation

    assert set(observed.borrowed) == {
        "Wh.Customer",
        "Wh.CustomerDelta",
        "Wh.CustomerDim",
        "Wh.CustomerOrder",
        "Rpt.CustomerSummary",
    }
    assert observed.source_rows == [(SENTINEL[0], CHANGED)]
