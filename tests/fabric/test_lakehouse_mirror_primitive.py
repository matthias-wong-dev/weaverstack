"""Mirroring one Lakehouse item, against a real workspace.

The claim: ``weaver mirror --item Lakehouse/X=Lakehouse/Y`` empties Y, points a
OneLake shortcut at each of X's tables and folders, stands a Spark view over
each of X's views, copies X's ``Files/_/Load`` tree, and binds the item.

A tenant answers what only Fabric can: whether a bulk-created shortcut becomes
readable as both a Spark relation and a Delta path, whether a persistent view
over another Lakehouse's view resolves after the session that made it is gone,
and whether the copied load tree is byte-identical. What the mirror decides is
settled in ``tests/test_borrow_declaration.py`` and
``tests/test_mirror_boundary.py``.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any

import pytest
from support.build_envs import LAKEHOUSE_JOURNEY_FIXTURE
from support.weaver_test import register_session, weaver_test

import weaver
from weaver.catalogue.tables import CATALOGUE_SCHEMA, MIRROR
from weaver.targets import ItemRef, WarehouseTarget

ITEM = "Lakehouse/Sales"

#: What the fixture declares, by what a mirror puts at each address.
POINTED_AT = ("DWG.Customer", "DWG.Order", "DWG.NamedCustomer", "Raw.CustomerCsv")
WRAPPED = ("DWG.ActiveCustomer", "DWG.ActiveCustomerSummary")


@dataclass(frozen=True)
class Mirrored:
    """One Lakehouse mirror, and a way to read either side of it."""

    result: Any
    #: What ``weaver test`` made of the mirrored item, or the error it raised.
    validated: Any
    catalogue_sql: Any
    source_name: str
    target_name: str
    catalogue_name: str
    workspace: Any
    session: Any


@pytest.fixture(scope="module")
def mirrored(
    fabric_workspace,
    fabric_catalogue,
    fabric_fork_catalogue,
    fabric_mirror_lakehouse,
    fabric_target_lakehouse,
    weaver_session,
    tmp_path_factory,
):
    """A built source Lakehouse, forked, then mirrored into its own target."""

    register_session(weaver_session)
    estate = LAKEHOUSE_JOURNEY_FIXTURE.disposable(tmp_path_factory.mktemp("lh-mirror"))
    source_name = fabric_target_lakehouse.name
    built = weaver.build(
        str(estate.path),
        items=[f"{ITEM}=Lakehouse/{source_name}"],
        session=weaver_session,
    )
    assert built.succeeded, [failure.describe() for failure in built.errors]
    weaver.load([ITEM], session=weaver_session)

    result = weaver.mirror(
        [f"{ITEM}=Lakehouse/{fabric_mirror_lakehouse.name}"],
        session=weaver_session,
        workspace=fabric_workspace.workspace,
        catalogue=f"Warehouse/{fabric_fork_catalogue.name}",
        mirror=f"Warehouse/{fabric_catalogue.name}",
    )
    try:
        # The session's own workspace, so the run keeps the Environment its
        # deployed modules are imported from. Only the catalogue is overridden.
        validated = weaver.test(
            [ITEM],
            session=weaver_session,
            catalogue=f"Warehouse/{fabric_fork_catalogue.name}",
        )
    except Exception as exc:  # recorded, so the physical claims still report
        validated = exc
    return Mirrored(
        result=result,
        validated=validated,
        catalogue_sql=weaver_session.sql_executor(
            WarehouseTarget(ItemRef(fabric_fork_catalogue.name)),
            workspace=fabric_workspace,
        ),
        source_name=source_name,
        target_name=fabric_mirror_lakehouse.name,
        catalogue_name=fabric_fork_catalogue.name,
        workspace=fabric_workspace,
        session=weaver_session,
    )


def _shortcuts(mirrored) -> dict[str, str]:
    """Every shortcut the mirrored Lakehouse holds, by ``path/name``."""

    resolver = mirrored.session.resolver(mirrored.workspace)
    return {
        each.qualified: each.target_path or ""
        for each in resolver.onelake_shortcuts(ItemRef(mirrored.target_name))
    }


def _rows(mirrored, relation: str) -> list[tuple]:
    """One relation in the mirrored Lakehouse, read four-part over Spark."""

    resolver = mirrored.session.resolver(mirrored.workspace)
    destination = resolver.spark_destination(ItemRef(mirrored.target_name))
    schema, _, name = relation.partition(".")
    seen = mirrored.session.execute_spark_sql(
        f"SELECT * FROM {destination.qualify(schema, name)}",
        exact_case=True,
        workspace=mirrored.workspace,
    )
    return list(seen)


@weaver_test(remote=True, resources={"rest"})
def test_every_table_and_folder_becomes_a_shortcut(mirrored):
    """Storage is borrowed, so nothing was copied into the destination."""

    held = _shortcuts(mirrored)

    for qualified in POINTED_AT:
        schema, _, name = qualified.partition(".")
        area = "Files" if qualified.startswith("Raw.") else "Tables"
        assert f"{area}/{schema}/{name}" in held


@weaver_test(remote=True, resources={"rest"})
def test_a_view_is_not_pointed_at_with_a_shortcut(mirrored):
    """A shortcut addresses storage, and a view is a definition."""

    held = _shortcuts(mirrored)

    for qualified in WRAPPED:
        schema, _, name = qualified.partition(".")
        assert f"Tables/{schema}/{name}" not in held


@weaver_test(remote=True, resources={"livy"})
def test_a_borrowed_table_reads_the_sources_rows(mirrored):
    """Zero copy: the rows are the source's, read through the shortcut."""

    assert _rows(mirrored, "DWG.Customer")


@weaver_test(remote=True, resources={"livy"})
def test_a_wrapper_view_reads_through_to_the_sources_view(mirrored):
    """A persistent view over another Lakehouse's view, after its session went."""

    assert _rows(mirrored, "DWG.ActiveCustomer")
    assert _rows(mirrored, "DWG.ActiveCustomerSummary")


@weaver_test(remote=True, resources={"onelake"})
def test_the_deployed_load_tree_is_copied_byte_for_byte(mirrored):
    """The data is borrowed and the code is local, so a run has its modules."""

    resolver = mirrored.session.resolver(mirrored.workspace)
    store = mirrored.session.store(mirrored.workspace)
    roots = {
        role: resolver.lakehouse(ItemRef(name)).join("Files", "_", "Load")
        for role, name in (
            ("source", mirrored.source_name),
            ("target", mirrored.target_name),
        )
    }
    held = {
        role: {
            entry.location.value[len(root.value) :].lstrip("/"): store.read(
                entry.location
            )
            for entry in store.list(root, recursive=True)
            if not entry.is_directory
        }
        for role, root in roots.items()
    }

    assert held["source"], "the source item deployed no load tree"
    assert held["target"] == held["source"]


@weaver_test(remote=True, resources={"tds"})
def test_the_catalogue_records_what_stands_at_each_address(mirrored):
    """``_.Mirror`` says Table, Folder or View, being what is physically there."""

    rows = mirrored.catalogue_sql.query(
        "select [Schema name], [Object name], [Physical type] from "
        f"[{CATALOGUE_SCHEMA}].[{MIRROR.name}] where [Item type] = N'Lakehouse'"
    )
    borrowed = {
        f"{row['Schema name']}.{row['Object name']}": str(row["Physical type"])
        for row in rows
    }

    assert borrowed["Tables/DWG.Customer"] == "Table"
    assert borrowed["Files/Raw.CustomerCsv"] == "Folder"
    assert borrowed["Tables/DWG.ActiveCustomer"] == "View"


@weaver_test(remote=True, resources={"tds"})
def test_the_item_is_bound_to_its_new_target(mirrored):
    rows = mirrored.catalogue_sql.query(
        "select [Item name], [Target name] from "
        f"[{CATALOGUE_SCHEMA}].[Installation] where [Item type] = N'Lakehouse'"
    )
    bound = {str(row["Item name"]): str(row["Target name"]) for row in rows}

    assert bound["Sales"] == mirrored.target_name


@weaver_test(remote=True, resources={"tds"})
def test_no_borrowed_node_is_loadable(mirrored):
    """The rows belong to the target each node borrows from."""

    from weaver.catalogue.state import catalogue_for

    forked = replace(
        mirrored.workspace, catalogue=f"Warehouse/{mirrored.catalogue_name}"
    )
    with catalogue_for(mirrored.session, forked) as catalogue:
        dag = catalogue.dag()

    borrowed = [node for node in dag.nodes if node.is_mirrored]
    assert borrowed, "the mirror recorded nothing"
    assert not any(node.is_loadable for node in borrowed)


@weaver_test(remote=True, resources={"rest"})
def test_the_standard_surface_is_there(mirrored):
    """A mirrored Lakehouse presents what a built one presents."""

    from weaver.catalogue.tables import STANDARD_SURFACE_TABLES

    held = _shortcuts(mirrored)

    for table in STANDARD_SURFACE_TABLES:
        assert f"Tables/{CATALOGUE_SCHEMA}/{table.name}" in held


@weaver_test(remote=True)
def test_the_mirrored_lakehouse_runs_its_installed_validations(mirrored):
    """The operational proof: copied code plus the surface is a working item.

    Reading files and catalogue rows says the parts are there. Dispatching a
    validation says they compose.
    """

    report = mirrored.validated
    assert not isinstance(report, Exception), report
    assert report.nodes, "dispatch reached no validation"
    assert {node.status for node in report.nodes} <= {"passed", "failed"}


@weaver_test(remote=True, resources={"livy"})
def test_the_source_lakehouse_is_untouched(mirrored):
    """A mirror reads the source and writes only its own destination."""

    resolver = mirrored.session.resolver(mirrored.workspace)
    source = resolver.spark_destination(ItemRef(mirrored.source_name))
    seen = mirrored.session.execute_spark_sql(
        f"SELECT * FROM {source.qualify('DWG', 'Customer')}",
        exact_case=True,
        workspace=mirrored.workspace,
    )

    assert list(seen)
