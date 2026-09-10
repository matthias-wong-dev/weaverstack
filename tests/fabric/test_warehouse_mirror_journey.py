"""One Warehouse item mirrored, then driven through the ordinary lifecycle.

Build a source estate, mirror the item into another Warehouse, run its
installed validations there, build again with nothing changed, then change one
declaration and build again.

A tenant answers what only Fabric can: whether a three-part View resolves
across Warehouses, whether a row written at the source is visible through one,
and whether a copied procedure executes where it was copied to. What the mirror
decides is settled in ``tests/targeted/test_mirror_lifecycle_cycle.py``.

Steps run in file order and do not cascade.
"""

from __future__ import annotations

from dataclasses import dataclass, replace

import pytest
from support.acceptance import Acceptance
from support.build_envs import WAREHOUSE_ESTATE_FIXTURE
from support.weaver_test import register_session, weaver_test

import weaver
from weaver.catalogue.tables import CATALOGUE_SCHEMA
from weaver.catalogue.tables import MIRROR as MIRROR_TABLE
from weaver.targets import ItemRef, WarehouseTarget

ITEM = "Warehouse/Reporting"

#: A row the source holds and the mirror has no copy of.
SENTINEL = (99, "Borrowed")

#: What the row is set to at the source once the mirror stands, so reading it
#: back through the View proves nothing was copied.
CHANGED = "Changed"

#: The object whose declaration changes, and the one every later claim watches.
MATERIALISED = "Wh.Product"

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


# --- the journey --------------------------------------------------------------


@pytest.fixture(scope="module")
def journey(
    fabric_workspace,
    fabric_catalogue,
    fabric_fork_catalogue,
    fabric_mirror_warehouse,
    session_disposable_warehouse,
    warehouse_session,
    tmp_path_factory,
):
    """One estate: built, mirrored, validated, rebuilt, then materialised."""

    register_session(warehouse_session)
    estate = WAREHOUSE_ESTATE_FIXTURE.disposable(tmp_path_factory.mktemp("mirror"))
    _write(estate, f"{ITEM}/tests/{VALIDATION}.sql", VALIDATION_SOURCE)

    run = Acceptance(name="warehouse-mirror")
    run.source_name = session_disposable_warehouse.item.name
    run.target_name = fabric_mirror_warehouse.name
    run.catalogue_name = fabric_fork_catalogue.name
    run.source_catalogue_name = fabric_catalogue.name
    run.workspace = fabric_workspace
    run.source_sql = _sql(warehouse_session, fabric_workspace, run.source_name)
    run.target_sql = _sql(warehouse_session, fabric_workspace, run.target_name)
    run.catalogue_sql = _sql(warehouse_session, fabric_workspace, run.catalogue_name)
    run.forked = replace(fabric_workspace, catalogue=f"Warehouse/{run.catalogue_name}")
    run.session = warehouse_session
    into_mirror = [f"{ITEM}=Warehouse/{run.target_name}"]

    run.step(
        "build the source",
        lambda: _built(
            weaver.build(
                str(estate.path),
                items=[f"{ITEM}=Warehouse/{run.source_name}"],
                session=warehouse_session,
            )
        ),
    )
    run.step("seed the source", lambda: _seed(run.source_sql))
    mirrored = run.step(
        "mirror",
        lambda: weaver.mirror(
            into_mirror,
            session=warehouse_session,
            workspace=fabric_workspace.workspace,
            catalogue=f"Warehouse/{run.catalogue_name}",
            mirror=f"Warehouse/{fabric_catalogue.name}",
        ),
    )
    mirrored.observation = _observe(run)
    again = run.step(
        "mirror again",
        lambda: weaver.mirror(
            into_mirror,
            session=warehouse_session,
            workspace=fabric_workspace.workspace,
            catalogue=f"Warehouse/{run.catalogue_name}",
            mirror=f"Warehouse/{fabric_catalogue.name}",
        ),
    )
    again.observation = _observe(run)
    run.step("read a source change through the mirror", lambda: _read_through(run))
    run.step(
        "report health over the mirror",
        lambda: weaver.health(
            [ITEM],
            session=warehouse_session,
            workspace_config=_forked_config(run, tmp_path_factory.mktemp("wh-health")),
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
    rebuilt = run.step(
        "build with nothing changed",
        lambda: _built(
            weaver.build(
                str(estate.path),
                items=into_mirror,
                session=warehouse_session,
                catalogue=f"Warehouse/{run.catalogue_name}",
            )
        ),
    )
    rebuilt.observation = _observe(run)
    run.step("change one declaration", lambda: _change(estate))
    materialised = run.step(
        "build the changed declaration",
        lambda: _built(
            weaver.build(
                str(estate.path),
                items=into_mirror,
                session=warehouse_session,
                catalogue=f"Warehouse/{run.catalogue_name}",
            )
        ),
    )
    materialised.observation = _observe(run)
    return run


# --- driving it ---------------------------------------------------------------


def _forked_config(run, directory):
    """A workspace configuration naming the fork and the catalogue it mirrors.

    ``mirror:`` reaches a Workspace from configuration alone, and it is what
    tells health where a mirrored object's load state is recorded.
    """

    path = directory / "workspace-config.yml"
    path.write_text(
        "\n".join(
            (
                f"workspace: {run.workspace.workspace}",
                f"catalogue: Warehouse/{run.catalogue_name}",
                f"mirror: Warehouse/{run.source_catalogue_name}",
            )
        ),
        encoding="utf-8",
    )
    return path


def _sql(session, workspace, name: str):
    return session.sql_executor(WarehouseTarget(ItemRef(name)), workspace=workspace)


def _write(estate, relative: str, text: str) -> None:
    path = estate.path / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _built(result):
    if not result.succeeded:
        raise AssertionError("; ".join(f.describe() for f in result.errors))
    return result


def _seed(sql) -> None:
    """One row at the source, so reading through the mirror proves something."""

    sql.execute("delete from [Wh].[Product];")
    sql.execute(
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


def _observe(run) -> Estate:
    """The estate as this transition left it."""

    from weaver.catalogue.state import catalogue_for

    with catalogue_for(run.session, run.forked) as catalogue:
        dag = catalogue.dag()
    return Estate(
        objects={
            f"{row['s']}.{row['n']}": str(row["t"]).strip()
            for row in run.target_sql.query(_OBJECTS)
        },
        borrowed={
            f"{row['Schema name']}.{row['Object name']}": str(row["Physical type"])
            for row in run.catalogue_sql.query(
                f"select [Schema name], [Object name], [Physical type] from {_q(MIRROR_TABLE.name)}"
            )
        },
        installed={
            str(row["Item name"]): str(row["Target name"])
            for row in run.catalogue_sql.query(
                "select [Item name], [Target name] from [_].[Installation]"
            )
        },
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
    )


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
        "Wh.CustomerDim",
        "Wh.CustomerOrder",
        "Wh.Product",
        "Rpt.CustomerSummary",
    }
    assert set(observed.borrowed.values()) == {"View"}
    assert not any(observed.loadable.values())


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
    assert result.mirrored[ITEM]["relations"] == 5
    assert result.mirrored[ITEM]["programmables"] > 0


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
def test_everything_unchanged_is_still_a_view(journey):
    journey.require("build the changed declaration")
    relations = _relations(journey["build the changed declaration"].observation)

    assert {name: kind for name, kind in relations.items() if name != MATERIALISED} == {
        "Wh.Customer": "V",
        "Wh.CustomerDim": "V",
        "Wh.CustomerOrder": "V",
        "Rpt.CustomerSummary": "V",
    }


@weaver_test(remote=True)
def test_only_the_materialised_object_stops_being_borrowed(journey):
    journey.require("build the changed declaration")
    observed = journey["build the changed declaration"].observation

    assert MATERIALISED not in observed.borrowed
    assert set(observed.borrowed) == {
        "Wh.Customer",
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
    assert not [name for name, yes in loadable.items() if yes and name != MATERIALISED]


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
