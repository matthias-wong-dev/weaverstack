"""One Lakehouse item mirrored, then driven through the ordinary lifecycle.

Build a source estate, mirror the item into another Lakehouse, run its installed
validations there, build again with nothing changed, then change one declaration
and build again.

A tenant answers what only Fabric can: whether a bulk-created shortcut becomes
readable as both a Spark relation and a Delta path, whether a persistent view
over another Lakehouse's view resolves after the session that made it is gone,
and whether OneLake releases a removed shortcut's name in time for an owned
table to take it. What the mirror decides is settled in
``tests/targeted/test_lakehouse_mirror_lifecycle_cycle.py``.

Steps run in file order and do not cascade.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any

import pytest
from support.acceptance import Acceptance
from support.build_envs import LAKEHOUSE_JOURNEY_FIXTURE
from support.weaver_test import register_session, weaver_test

import weaver
from weaver.catalogue.tables import CATALOGUE_SCHEMA, STANDARD_SURFACE_TABLES
from weaver.catalogue.tables import MIRROR as MIRROR_TABLE
from weaver.etl import LOAD_ROOT
from weaver.targets import ItemRef, WarehouseTarget

ITEM = "Lakehouse/Sales"

#: What the fixture declares, by what a mirror puts at each address. A shortcut
#: addresses storage, so a view is wrapped rather than pointed at.
POINTED_AT = ("Tables/DWG/Customer", "Tables/DWG/Order", "Files/Raw/CustomerCsv")
WRAPPED = ("DWG.ActiveCustomer", "DWG.ActiveCustomerSummary")

#: The object whose declaration changes: a table nothing else reads, so an
#: ordinary build selects it alone.
MATERIALISED = "DWG.NamedCustomer"

#: The deployed module the changed declaration compiles to. A Spark SQL table
#: installs as a ``SparkSqlTable`` carrying its query, so editing the query
#: changes these bytes.
CHANGED_MODULE = f"Tables/{MATERIALISED.replace('.', '__')}.py"

#: The runtime tree's address inside a Lakehouse, as storage spells it.
RUNTIME_TREE = ("Files", *LOAD_ROOT.split("/"))
RUNTIME_ROOT = "/".join(RUNTIME_TREE)


@dataclass(frozen=True)
class Estate:
    """What the estate holds at one moment, read after one transition."""

    #: ``path/name`` to source path, for every shortcut the mirror holds.
    shortcuts: dict[str, str]
    #: Relation names Spark reports in the item's own schema.
    relations: frozenset[str]
    #: ``Schema.Object`` to physical type, from ``_.Mirror``.
    borrowed: dict[str, str]
    #: Item name to target name, from ``_.Installation``.
    installed: dict[str, str]
    #: Whether each data node has a load to dispatch, from the installed graph.
    loadable: dict[str, bool]
    #: Rows the source still holds, which no build in this journey may touch.
    source_rows: int
    #: Deployed file to digest under ``Files/_/Load``, keyed by ``source`` and
    #: ``target``. A digest of each side at one moment is what separates a local
    #: copy from a shortcut: writes through a shortcut move both.
    runtime: dict[str, dict[str, str]]


# --- the journey --------------------------------------------------------------


@pytest.fixture(scope="module")
def journey(
    fabric_workspace,
    fabric_catalogue,
    fabric_fork_catalogue,
    fabric_mirror_lakehouse,
    fabric_target_lakehouse,
    weaver_session,
    tmp_path_factory,
):
    """One estate: built, loaded, mirrored, validated, rebuilt, materialised."""

    register_session(weaver_session)
    estate = LAKEHOUSE_JOURNEY_FIXTURE.disposable(tmp_path_factory.mktemp("lh-mirror"))

    run = Acceptance(name="lakehouse-mirror")
    run.source_name = fabric_target_lakehouse.name
    run.target_name = fabric_mirror_lakehouse.name
    run.catalogue_name = fabric_fork_catalogue.name
    run.source_catalogue_name = fabric_catalogue.name
    run.workspace = fabric_workspace
    run.session = weaver_session
    run.forked = replace(fabric_workspace, catalogue=f"Warehouse/{run.catalogue_name}")
    run.catalogue_sql = weaver_session.sql_executor(
        WarehouseTarget(ItemRef(run.catalogue_name)), workspace=fabric_workspace
    )
    into_mirror = [f"{ITEM}=Lakehouse/{run.target_name}"]

    run.step(
        "build the source",
        lambda: _built(
            weaver.build(
                str(estate.path),
                items=[f"{ITEM}=Lakehouse/{run.source_name}"],
                session=weaver_session,
            )
        ),
    )
    run.step("load the source", lambda: weaver.load([ITEM], session=weaver_session))
    mirrored = run.step("mirror", lambda: _mirror(run, into_mirror, fabric_catalogue))
    mirrored.observation = _observe(run)
    again = run.step(
        "mirror again", lambda: _mirror(run, into_mirror, fabric_catalogue)
    )
    again.observation = _observe(run)
    run.health_config = _forked_config(run, tmp_path_factory.mktemp("lh-health"))
    run.step(
        "report health over the mirror",
        lambda: weaver.health(
            [ITEM], session=weaver_session, workspace_config=run.health_config
        ),
    )
    run.step(
        "load the source again",
        lambda: weaver.load([ITEM], session=weaver_session, reload=True),
    )
    run.step(
        "report health after the source advanced",
        lambda: weaver.health(
            [ITEM], session=weaver_session, workspace_config=run.health_config
        ),
    )
    run.step(
        "validate the mirror",
        lambda: weaver.test(
            [ITEM],
            session=weaver_session,
            catalogue=f"Warehouse/{run.catalogue_name}",
        ),
    )
    rebuilt = run.step(
        "build with nothing changed",
        lambda: _built(
            weaver.build(
                str(estate.path),
                items=into_mirror,
                session=weaver_session,
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
                session=weaver_session,
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


def _mirror(run, items, source_catalogue) -> Any:
    return weaver.mirror(
        items,
        session=run.session,
        workspace=run.workspace.workspace,
        catalogue=f"Warehouse/{run.catalogue_name}",
        mirror=f"Warehouse/{source_catalogue.name}",
    )


def _built(result):
    if not result.succeeded:
        raise AssertionError("; ".join(f.describe() for f in result.errors))
    return result


def _change(estate) -> None:
    """Change one leaf table's query, leaving every other object as it was."""

    path = estate.path / ITEM / "Tables" / f"{MATERIALISED}.sql"
    text = path.read_text(encoding="utf-8")
    changed = text.replace(
        "where CustomerName is not null;",
        "where CustomerName is not null and CustomerId >= 0;",
        1,
    )
    assert changed != text, "the declaration to change was not found"
    path.write_text(changed, encoding="utf-8")


def _spark(run, statement: str):
    return run.session.execute_spark_sql(
        statement, exact_case=True, workspace=run.workspace
    )


def _runtime_tree(run, name: str) -> dict[str, str]:
    """Every deployed file under ``Files/_/Load``, by relative path and digest."""

    from hashlib import sha256

    resolver = run.session.resolver(run.workspace)
    store = run.session.store(run.workspace)
    root = resolver.lakehouse(ItemRef(name)).join(*RUNTIME_TREE)
    if not store.exists(root):
        return {}
    return {
        entry.location.value[len(root.value) :].lstrip("/"): sha256(
            store.read(entry.location)
        ).hexdigest()
        for entry in store.list(root, recursive=True)
        if not entry.is_directory
    }


def _observe(run) -> Estate:
    """The estate as this transition left it."""

    from weaver.catalogue.state import catalogue_for

    resolver = run.session.resolver(run.workspace)
    destination = resolver.spark_destination(ItemRef(run.target_name))
    source = resolver.spark_destination(ItemRef(run.source_name))
    with catalogue_for(run.session, run.forked) as catalogue:
        dag = catalogue.dag()
    return Estate(
        shortcuts={
            each.qualified: each.target_path or ""
            for each in resolver.onelake_shortcuts(ItemRef(run.target_name))
        },
        relations=frozenset(
            str(row["tableName"])
            for row in _spark(
                run, f"SHOW TABLES IN {destination.qualified_schema('DWG')}"
            )
        ),
        borrowed={
            f"{row['Schema name']}.{row['Object name']}": str(row["Physical type"])
            for row in run.catalogue_sql.query(
                "select [Schema name], [Object name], [Physical type] from "
                f"[{CATALOGUE_SCHEMA}].[{MIRROR_TABLE.name}] "
                "where [Item type] = N'Lakehouse'"
            )
        },
        installed={
            str(row["Item name"]): str(row["Target name"])
            for row in run.catalogue_sql.query(
                "select [Item name], [Target name] from "
                f"[{CATALOGUE_SCHEMA}].[Installation] where [Item type] = N'Lakehouse'"
            )
        },
        loadable={
            node.load_name: node.can_load
            for node in dag.nodes
            if str(node.item) == ITEM and node.load_name
        },
        source_rows=int(
            _spark(
                run, f"SELECT count(*) as n FROM {source.qualify('DWG', 'Customer')}"
            )[0]["n"]
        ),
        runtime={
            "source": _runtime_tree(run, run.source_name),
            "target": _runtime_tree(run, run.target_name),
        },
    )


# --- what the mirror stood up -------------------------------------------------


@weaver_test(remote=True)
def test_every_table_and_folder_becomes_a_shortcut(journey):
    """Storage is borrowed, so nothing was copied into the destination."""

    journey.require("mirror")
    held = journey["mirror"].observation.shortcuts

    for qualified in POINTED_AT:
        assert qualified in held


@weaver_test(remote=True)
def test_a_view_is_not_pointed_at_with_a_shortcut(journey):
    """A shortcut addresses storage, and a view is a definition."""

    journey.require("mirror")
    held = journey["mirror"].observation.shortcuts

    for qualified in WRAPPED:
        schema, _, name = qualified.partition(".")
        assert f"Tables/{schema}/{name}" not in held


@weaver_test(remote=True)
def test_the_standard_surface_is_there(journey):
    """A mirrored Lakehouse presents what a built one presents."""

    journey.require("mirror")
    held = journey["mirror"].observation.shortcuts

    for table in STANDARD_SURFACE_TABLES:
        assert f"Tables/{CATALOGUE_SCHEMA}/{table.name}" in held


@weaver_test(remote=True, resources={"livy"})
def test_a_borrowed_table_reads_the_sources_rows(journey):
    """Zero copy: the rows are the source's, read through the shortcut."""

    journey.require("mirror")
    resolver = journey.session.resolver(journey.workspace)
    destination = resolver.spark_destination(ItemRef(journey.target_name))

    seen = _spark(
        journey, f"SELECT count(*) as n FROM {destination.qualify('DWG', 'Customer')}"
    )

    assert int(seen[0]["n"]) == journey["mirror"].observation.source_rows


@weaver_test(remote=True, resources={"livy"})
def test_a_wrapper_view_reads_through_to_the_sources_view(journey):
    """A persistent view over another Lakehouse's view, after its session went."""

    journey.require("mirror")
    resolver = journey.session.resolver(journey.workspace)
    destination = resolver.spark_destination(ItemRef(journey.target_name))

    for qualified in WRAPPED:
        schema, _, name = qualified.partition(".")
        assert _spark(journey, f"SELECT * FROM {destination.qualify(schema, name)}")


@weaver_test(remote=True)
def test_the_deployed_load_tree_is_copied_byte_for_byte(journey):
    """The data is borrowed and the code is local, so a run has its modules.

    Read from the mirror's own observation. The two trees are equal at that
    moment and diverge at the changed build, which
    :func:`test_a_build_here_rewrites_this_items_runtime_module_alone` is about.
    """

    journey.require("mirror")
    held = journey["mirror"].observation.runtime

    assert held["source"], "the source item deployed no load tree"
    assert held["target"] == held["source"]


@weaver_test(remote=True)
def test_the_runtime_tree_is_not_a_shortcut(journey):
    """Equal bytes are also what a shortcut shows, so the shortcuts say which.

    ``Files/_.Load`` carries a data role in Registry like an authored Folder,
    and the shortcut list is where a mirror's treatment of it is visible.
    """

    journey.require("mirror")
    held = journey["mirror"].observation.shortcuts

    assert RUNTIME_ROOT not in held
    assert not [name for name in held if name.startswith(f"Files/{CATALOGUE_SCHEMA}/")]


@weaver_test(remote=True)
def test_the_catalogue_records_what_stands_at_each_address(journey):
    """``_.Mirror`` says Table, Folder or View, being what is physically there."""

    journey.require("mirror")
    borrowed = journey["mirror"].observation.borrowed

    assert borrowed["Tables/DWG.Customer"] == "Table"
    assert borrowed["Files/Raw.CustomerCsv"] == "Folder"
    assert borrowed["Tables/DWG.ActiveCustomer"] == "View"


@weaver_test(remote=True)
def test_the_item_is_bound_to_its_new_target(journey):
    journey.require("mirror")

    assert journey["mirror"].observation.installed["Sales"] == journey.target_name


@weaver_test(remote=True)
def test_no_mirrored_node_can_be_loaded_here(journey):
    """The rows belong to the target each node borrows from."""

    journey.require("mirror")
    loadable = journey["mirror"].observation.loadable

    assert loadable, "the mirror recorded nothing"
    assert not any(loadable.values())


@weaver_test(remote=True)
def test_the_mirrored_lakehouse_runs_its_installed_validations(journey):
    """The operational proof: copied code plus the surface is a working item.

    Reading files and catalogue rows says the parts are there. Dispatching a
    validation says they compose.
    """

    journey.require("validate the mirror")
    report = journey["validate the mirror"].result

    assert report.nodes, "dispatch reached no validation"
    assert {node.status for node in report.nodes} == {"passed"}


@weaver_test(remote=True)
def test_mirroring_again_leaves_the_same_estate(journey):
    """A mirror is reconstruction, so a half-finished one is rerun, not repaired."""

    journey.require("mirror again")
    first = journey["mirror"].observation
    second = journey["mirror again"].observation

    assert second.shortcuts == first.shortcuts
    assert second.borrowed == first.borrowed
    assert second.relations == first.relations


# --- an unchanged build -------------------------------------------------------


@weaver_test(remote=True)
def test_an_unchanged_build_leaves_every_relation_borrowed(journey):
    journey.require("build with nothing changed")
    before = journey["mirror"].observation
    after = journey["build with nothing changed"].observation

    assert after.shortcuts == before.shortcuts
    assert after.borrowed == before.borrowed


@weaver_test(remote=True)
def test_an_unchanged_build_leaves_the_source_rows_alone(journey):
    """The rows are the source's, and a build over the mirror is not a load."""

    journey.require("build with nothing changed")

    assert journey["build with nothing changed"].observation.source_rows == (
        journey["mirror"].observation.source_rows
    )


# --- one changed declaration --------------------------------------------------


@weaver_test(remote=True)
def test_the_changed_object_stops_being_a_shortcut(journey):
    """The borrowed pointer came off through the shortcut API."""

    journey.require("build the changed declaration")
    schema, _, name = MATERIALISED.partition(".")
    held = journey["build the changed declaration"].observation.shortcuts

    assert f"Tables/{schema}/{name}" not in held
    assert f"Tables/{schema}/{name}" in journey["mirror"].observation.shortcuts


@weaver_test(remote=True)
def test_the_changed_object_is_a_local_relation(journey):
    """Its name was released, and an owned table took it."""

    journey.require("build the changed declaration")
    _schema, _, name = MATERIALISED.partition(".")

    assert name in journey["build the changed declaration"].observation.relations


@weaver_test(remote=True)
def test_everything_unchanged_is_still_borrowed(journey):
    journey.require("build the changed declaration")
    held = journey["build the changed declaration"].observation.shortcuts

    for qualified in POINTED_AT:
        assert qualified in held


@weaver_test(remote=True)
def test_only_the_materialised_object_stops_being_borrowed(journey):
    journey.require("build the changed declaration")
    borrowed = journey["build the changed declaration"].observation.borrowed

    assert f"Tables/{MATERIALISED}" not in borrowed
    assert "Tables/DWG.Customer" in borrowed
    assert "Files/Raw.CustomerCsv" in borrowed


@weaver_test(remote=True)
def test_the_materialised_object_is_the_only_loadable_one(journey):
    """It holds its own rows now, so Weaver may write them."""

    journey.require("build the changed declaration")
    loadable = journey["build the changed declaration"].observation.loadable

    assert loadable[MATERIALISED] is True
    assert not [name for name, yes in loadable.items() if yes and name != MATERIALISED]


@weaver_test(remote=True)
def test_materialising_one_object_leaves_the_source_untouched(journey):
    """The drop reached a shortcut, and never the storage it pointed at."""

    journey.require("build the changed declaration")

    assert journey["build the changed declaration"].observation.source_rows == (
        journey["mirror"].observation.source_rows
    )
    assert journey["mirror"].observation.source_rows > 0


@weaver_test(remote=True)
def test_a_build_here_rewrites_this_items_runtime_module_alone(journey):
    """Rows are borrowed and code is not, so a build reaches only local storage.

    The changed table compiles to a module carrying its query. That module's
    bytes move here, and the source item's copy of it stays where it was.
    """

    journey.require("build the changed declaration")
    before = journey["mirror"].observation.runtime
    after = journey["build the changed declaration"].observation.runtime

    assert before["source"], "the source item deployed no load tree"
    assert after["target"][CHANGED_MODULE] != before["target"][CHANGED_MODULE]
    assert after["source"] == before["source"]


# --- health over the mirror ---------------------------------------------------


@weaver_test(remote=True)
def test_health_reads_the_mirror_and_calls_the_build_green(journey):
    """Registry says Table, ``_.Mirror`` says shortcut, and the Lakehouse holds one.

    Without ``_.Mirror`` the inventory check expects Registry's own type at each
    borrowed address.
    """

    journey.require("report health over the mirror")
    report = journey["report health over the mirror"].result

    assert [
        (finding.code, finding.object_id) for finding in report.build.findings
    ] == []
    assert report.load.subjects > 0


def _last_loaded(report, identity: str):
    """When the report says that object last loaded, from its activity window.

    A mirrored object's statistics come from the catalogue it mirrors, which is
    also the only place they exist: a fork copies ``_.LoadStatus`` and leaves
    ``_.LoadStatistic`` behind.
    """

    seen = [
        each.completed_at
        for each in report.load_activity
        if each.object_id == identity and each.completed_at is not None
    ]
    return max(seen) if seen else None


@weaver_test(remote=True)
def test_a_load_at_the_source_reaches_the_forks_report(journey):
    """The fork ran nothing, and the instant its mirrored table carries moved."""

    journey.require("report health after the source advanced")
    borrowed = f"{ITEM}/Tables/DWG.Customer"
    before = _last_loaded(journey["report health over the mirror"].result, borrowed)
    after = _last_loaded(
        journey["report health after the source advanced"].result, borrowed
    )

    assert before is not None, "the mirrored table carried no source statistic"
    assert after > before
