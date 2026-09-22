"""Whatever a build physically rebuilds, it leaves unloaded.

`test_mirrored_rebuild_runtime_state_cycle` drives one topology end to end. This
asks the same question of every object shape at once, because the failure it
guards is a subset one: a build that resets most of what it rebuilt and misses
a shape looks correct from any single example.

The property, read off the generated plan rather than from a list this module
keeps: every loadable the plan drops or builds carries an establishment row
saying Pending with the bookmark sentinel, and nothing else does.

The estate mixes an authored change, a dependency impact, a borrowed object, a
Folder, a View, a table Weaver does not load, a Static table and a table that
refuses rebuild, so a shape that falls out of the establishment population
shows up here as a rebuilt object with no row.
"""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import pytest
from factories import (
    FixtureInventory,
    catalogue_inventory,
    item_bindings,
    item_id,
    schema_document,
    warehouse_table,
    warehouse_view,
)
from support.weaver_test import weaver_test
from support.workspaces import WORKSPACE

from weaver.build_bundle import WarehouseBinding, generate_item_build_bundle
from weaver.build_bundle.catalogue_actions import desired_catalogue
from weaver.build_bundle.planner import certifiable_identities
from weaver.build_bundle.runtime_tables import RECONCILE_SLUG
from weaver.catalogue.builtin import BUILTIN_ITEM
from weaver.catalogue.state import Catalogue, reconcile_catalogue_state
from weaver.catalogue.tables import (
    BOOKMARK,
    BOOKMARK_SENTINEL_TEXT,
    LOAD_STATUS,
    MIRROR,
    PENDING,
    PROJECTED_TABLES,
)
from weaver.declaration import parse_item_repository
from weaver.declaration.metadata import ObjectId
from weaver.declaration.model import WeaverDocumentId
from weaver.etl import item_bookmarkable_objects
from weaver.locations import Location
from weaver.store import FilesystemStore
from weaver.targets import ItemRef

ITEM = "Warehouse/Model"
TARGET = "Model_Dev"
SOURCE_TARGET = "Model"

#: Borrowed to begin with. ``Sales.Owned`` is not: it holds its own rows, so
#: it is the half of ``Prohibit rebuild`` that has data to protect.
BORROWED = (
    "Source",
    "Aggregate",
    "Report",
    "Reference",
    "Unloaded",
    "Frozen",
    "Protected",
)

#: The action kinds that mean the object at that address was physically touched.
REBUILDING = frozenset(
    {
        "drop_table",
        "drop_view",
        "drop_folder",
        "build_table",
        "build_view",
        "build_folder",
    }
)

CHANGED = "select cast(1 as int) as SourceId, cast('x' as varchar(10)) as Name"

#: What this estate declares that Weaver loads, written out rather than derived.
#: The property below compares the plan against this rather than against
#: ``item_bookmarkable_objects``, which is what builds the establishment
#: population, so a shape leaving that function is a failure here and not an
#: agreement between two readings of the same mistake.
#:
#: ``Summary`` is a View and ``Unloaded`` declares no load procedure, so neither
#: holds a bookmark and neither belongs here.
DECLARED_LOADABLES = frozenset(
    {
        "Source",
        "Aggregate",
        "Report",
        "Frozen",
        "Protected",
        "Owned",
        "Reference",
    }
)


def _estate(root: Path, *, source: str = "select cast(1 as int) as SourceId"):
    """One item carrying one of each shape a keyed Warehouse build handles."""

    documents = {
        f"{ITEM}/schemas/Sales.yml": schema_document("Sales"),
        # The authored change, and the chain it drags with it.
        f"{ITEM}/Sales.Source.sql": warehouse_table(
            "Sales.Source", select=source, primary_key="SourceId"
        ),
        f"{ITEM}/Sales.Aggregate.sql": warehouse_table(
            "Sales.Aggregate",
            select="select cast(SourceId as int) as SourceId from Sales.Source",
            primary_key="SourceId",
        ),
        f"{ITEM}/Sales.Report.sql": warehouse_table(
            "Sales.Report",
            select="select cast(SourceId as int) as SourceId from Sales.Aggregate",
            primary_key="SourceId",
        ),
        # A View over the chain: a build settles it, and it holds no bookmark.
        f"{ITEM}/Sales.Summary.sql": warehouse_view(
            "Sales.Summary",
            select="select SourceId from Sales.Report",
            depends_on="Sales.Report",
        ),
        # A table downstream of the change that Weaver does not load.
        f"{ITEM}/Sales.Unloaded.sql": warehouse_table(
            "Sales.Unloaded",
            select="select cast(SourceId as int) as SourceId from Sales.Aggregate",
            primary_key="SourceId",
            has_load_procedure=False,
        ),
        # Static: loaded once, and downstream of the change all the same.
        f"{ITEM}/Sales.Frozen.sql": _flagged(
            warehouse_table(
                "Sales.Frozen",
                select="select cast(SourceId as int) as SourceId from Sales.Aggregate",
                primary_key="SourceId",
            ),
            "Static: true",
        ),
        # Refuses rebuild, so the build must leave both it and its state alone.
        f"{ITEM}/Sales.Protected.sql": _flagged(
            warehouse_table(
                "Sales.Protected",
                select="select cast(SourceId as int) as SourceId from Sales.Aggregate",
                primary_key="SourceId",
            ),
            "Prohibit rebuild: true",
        ),
        # Refuses rebuild and holds its own rows, so the build must not touch it.
        f"{ITEM}/Sales.Owned.sql": _flagged(
            warehouse_table(
                "Sales.Owned",
                select="select cast(SourceId as int) as SourceId from Sales.Aggregate",
                primary_key="SourceId",
            ),
            "Prohibit rebuild: true",
        ),
        # Depends on nothing, so no build reaches it.
        f"{ITEM}/Sales.Reference.sql": warehouse_table(
            "Sales.Reference",
            select="select cast(1 as int) as ReferenceId",
            primary_key="ReferenceId",
        ),
    }
    for relative, text in documents.items():
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    return parse_item_repository(Location(str(root)))


def _flagged(source: str, line: str) -> str:
    """Add one build-behaviour line to a generated declaration's header."""

    marker = "Primary key: "
    at = source.index(marker)
    return source[:at] + line + "\n\n" + source[at:]


def _object(name: str) -> WeaverDocumentId:
    return WeaverDocumentId(item_id(ITEM), ObjectId("Sales", name))


def _bindings():
    return item_bindings((ITEM, TARGET))


def _installed(repository) -> Catalogue:
    bindings = _bindings()
    by_item = {binding.item: binding for binding in bindings.entries}
    state = desired_catalogue(
        repository,
        certifiable_identities(repository, by_item),
        {binding.item: binding.to_bound_target() for binding in bindings.entries},
    )
    return Catalogue(
        rows=state.rows,
        materialised=frozenset(table.name for table in PROJECTED_TABLES),
    )


def _borrowing(catalogue: Catalogue, *names: str) -> Catalogue:
    item = item_id(ITEM)
    rows = {each: dict(tables) for each, tables in catalogue.rows.items()}
    rows[item][MIRROR.name] = tuple(
        {
            "item_type": item.item_type,
            "item_name": item.item_name,
            "schema_name": "Sales",
            "object_name": name,
            "source_workspace_name": WORKSPACE,
            "source_target_name": SOURCE_TARGET,
            "source_schema_name": "Sales",
            "source_object_name": name,
            "physical_type": "view",
        }
        for name in names
    )
    return Catalogue(rows=rows, materialised=catalogue.materialised | {MIRROR.name})


def _settled(catalogue: Catalogue, repository) -> Catalogue:
    """Every bookmarkable object loaded and settled, as a mirror copies them."""

    from factories import bookmark_row_for, load_status_row
    from test_health_representation import at

    item = item_id(ITEM)
    rows = {each: dict(tables) for each, tables in catalogue.rows.items()}
    identities = tuple(item_bookmarkable_objects(repository, item=item))
    rows[item][LOAD_STATUS.name] = tuple(
        load_status_row(identity, result="succeeded", completed_at=at(1))
        for identity in identities
    )
    rows[item][BOOKMARK.name] = tuple(
        bookmark_row_for(identity, at(1)) for identity in identities
    )
    return Catalogue(
        rows=rows,
        materialised=catalogue.materialised | {LOAD_STATUS.name, BOOKMARK.name},
    )


def _inventory(repository, *, borrowed: tuple[str, ...] = BORROWED):
    bound = {b.item: b.to_bound_target() for b in _bindings().entries}
    inventory = FixtureInventory.from_repository(
        repository,
        item=ITEM,
        target_id=bound[item_id(ITEM)].id,
        kind="warehouse",
        target_name=TARGET,
    )
    names = {f"Sales.{name}" for name in borrowed}
    return replace(
        inventory,
        tables=tuple(name for name in inventory.tables if name not in names),
        views=tuple(sorted(set(inventory.views) | names)),
    )


@pytest.fixture
def rebuilt(tmp_path):
    """A build of a changed ``Sales.Source`` over a fully borrowed, settled estate."""

    installed = _estate(tmp_path / "installed")
    changed = _estate(tmp_path / "changed", source=CHANGED)
    catalogue = _settled(_borrowing(_installed(installed), *BORROWED), installed)
    inventories = {
        item_id(ITEM): _inventory(installed),
        BUILTIN_ITEM: catalogue_inventory(holding=True),
    }
    reconciliation = reconcile_catalogue_state(catalogue, inventories=inventories)
    return generate_item_build_bundle(
        changed,
        bindings=_bindings(),
        output=Location(str(tmp_path / "bundle")),
        store=FilesystemStore(),
        target_inventories=inventories,
        catalogue=reconciliation.catalogue,
        stale_claims=reconciliation.stale_claims,
        catalogue_binding=WarehouseBinding(
            ItemRef("Weaver_Control"), workspace_name=WORKSPACE
        ),
    )


def _physically_rebuilt(bundle) -> set[str]:
    """Every address the plan drops or builds, read off the actions themselves."""

    return {
        action.resource_node_id
        for _sequence, _batch, action in bundle.plan.actions()
        if action.kind in REBUILDING and action.resource_node_id
    }


def _established(bundle) -> dict[str, dict[str, dict]]:
    """The rows the reset before physical work carries, by table and object.

    Not the ``view-state`` action, which is the other ``reconcile_runtime_state``
    in the plan and records Succeeded for Views once their DDL has run.
    """

    payloads = [
        action
        for _sequence, _batch, action in bundle.plan.actions()
        if action.kind == "reconcile_runtime_state" and action.id == RECONCILE_SLUG
    ]
    if not payloads:
        return {}
    (action,) = payloads
    content = FilesystemStore().read(bundle.location.join(*action.payload.split("/")))
    return {
        one["table"]: {row["object_name"]: row for row in one["rows"]}
        for one in json.loads(content.decode("utf-8"))["establish"]
    }


def _names(identities) -> set[str]:
    return {identity.rsplit(".", 1)[-1] for identity in identities}


def _bookmarkable(repository) -> set[str]:
    """The loadable population as a build reads it, by bare object name."""

    return _names(
        str(identity)
        for identity in item_bookmarkable_objects(repository, item=item_id(ITEM))
    )


# --- the property -------------------------------------------------------------


@weaver_test()
def test_every_rebuilt_loadable_is_established_as_unloaded(rebuilt):
    """The invariant, over whatever this estate's shapes made the plan do.

    Read forwards from the actions, so a shape that stops being selected, or
    stops being bookmarkable, fails here rather than going quietly unreset.
    """

    established = _established(rebuilt)
    statuses = established.get(LOAD_STATUS.name, {})
    bookmarks = established.get(BOOKMARK.name, {})

    rebuilt_loadables = _names(_physically_rebuilt(rebuilt)) & DECLARED_LOADABLES
    assert rebuilt_loadables, "the estate rebuilt no loadable, so this proves nothing"

    unreset = {
        name
        for name in rebuilt_loadables
        if statuses.get(name, {}).get("result") != PENDING
    }
    assert unreset == set()

    without_sentinel = {
        name
        for name in rebuilt_loadables
        if bookmarks.get(name, {}).get("bookmark_datetime") != BOOKMARK_SENTINEL_TEXT
    }
    assert without_sentinel == set()


@weaver_test()
def test_the_estate_declares_the_loadables_this_module_says_it_does(tmp_path):
    """Ties the written population to the one a build reads.

    A disagreement here means either the estate changed or
    ``item_bookmarkable_objects`` stopped returning a shape. Both are failures,
    and neither is visible from the property alone, which would simply cover
    less.
    """

    assert _bookmarkable(_estate(tmp_path / "declared")) == DECLARED_LOADABLES


@weaver_test()
def test_nothing_the_build_left_standing_is_reset(rebuilt):
    """The other half. A reset that reached everything would pass the first test.

    ``Sales.Owned`` refuses rebuild and holds its own rows, and
    ``Sales.Reference`` depends on nothing, so the build reaches neither and
    both keep the state they had.
    """

    statuses = _established(rebuilt).get(LOAD_STATUS.name, {})
    touched = _names(_physically_rebuilt(rebuilt))

    assert "Owned" not in touched
    assert "Reference" not in touched
    assert "Owned" not in statuses
    assert "Reference" not in statuses


@weaver_test()
def test_refusing_rebuild_protects_owned_rows_and_not_a_borrowed_address(rebuilt):
    """``Prohibit rebuild`` guards data. A mirrored object holds none of its own.

    ``Sales.Protected`` and ``Sales.Owned`` carry the same declaration flag and
    the same dependency on the changed table. The one difference is that
    ``Sales.Protected`` is borrowed, so materialising it destroys nothing, and
    its inherited state has to go with the View it replaces.
    """

    statuses = _established(rebuilt).get(LOAD_STATUS.name, {})
    touched = _names(_physically_rebuilt(rebuilt))

    assert "Protected" in touched
    assert statuses["Protected"]["result"] == PENDING
    assert "Owned" not in touched
    assert "Owned" not in statuses


@weaver_test()
def test_the_shapes_this_estate_is_here_to_cover_all_reach_the_build(rebuilt):
    """Names the population, so a shape silently dropping out is a failure.

    Without this, an estate that stopped producing one of these would keep
    passing the property above by covering less.
    """

    assert {
        "Source",
        "Aggregate",
        "Report",
        "Summary",
        "Unloaded",
        "Frozen",
        "Protected",
    } <= _names(_physically_rebuilt(rebuilt))
