"""Shortcuts: every decision, in pure Python.

A shortcut is the one construct that reaches outside its item, and almost
everything about it is a decision: is it planned, is it left alone, does its
schema still get created, does its consumer wait for its producer, is it stale
because the producer moved on, and what pair of addresses is frozen for the
installer. None of that needs a workspace. It is computed from the repository and
the catalogue, both of which can be built directly.

What needs Fabric is narrower, and it is the installation: a OneLake
shortcut is an API call, Fabric discovers one asynchronously, and a Warehouse
shortcut materialises as a view over a SQL endpoint. Those live in `tests/fabric`.

The split matters because the expensive suite used to prove the decisions by
building three estates. A decision proven here costs milliseconds and says which
decision was wrong.
"""

from __future__ import annotations

import pytest
from factories import (
    bound_target,
    catalogue_target,
    document_id,
    item_id,
    registered_document,
    shortcut_repository,
    target_inventory,
)
from support.weaver_test import weaver_test

from weaver.build_bundle import plan_item_build
from weaver.build_bundle.incremental import (
    declared_signatures,
    select_build,
    stale_through_shortcuts,
)
from weaver.build_bundle.shortcuts import plan_lakehouse_shortcuts

PRODUCER = "Lakehouse/Raw"
CONSUMER = "Lakehouse/Curated"
SHORTCUT = "Lakehouse/Curated/DWG.PortableCustomer"
SOURCE = "Lakehouse/Raw/DWG.Customer"
VIEW = "Lakehouse/Curated/DWG.CustomerName"


@pytest.fixture
def estate(tmp_path):
    return shortcut_repository(tmp_path / "repo")


def targets():
    return {
        item_id(PRODUCER): bound_target(id="raw", item_id="Raw_LH"),
        item_id(CONSUMER): bound_target(id="curated", item_id="Curated_LH"),
    }


def inventories():
    return {
        item_id(PRODUCER): target_inventory(tables=("DWG.Customer",)),
        item_id(CONSUMER): target_inventory(
            target_id="curated",
            target_name="Curated_LH",
            tables=("DWG.PortableCustomer",),
            views=("DWG.CustomerName",),
        ),
    }


def plan_shortcuts(repository, *, selected=(SHORTCUT,)):
    by_item = targets()
    return plan_lakehouse_shortcuts(
        repository,
        item=item_id(CONSUMER),
        target=by_item[item_id(CONSUMER)],
        target_by_item=by_item,
        selected={document_id(name) for name in selected},
    )


# --- planning the shortcut itself ------------------------------------------------


@weaver_test()
def test_a_selected_shortcut_is_planned_as_one_action(estate):
    planned = plan_shortcuts(estate)

    assert planned.stage is not None
    kinds = [action.kind for batch in planned.stage.batches for action in batch.actions]
    assert kinds == ["create_shortcut"]


@weaver_test()
def test_an_unselected_shortcut_is_left_alone(estate):
    """Incremental selection applies to shortcuts exactly as to documents.

    A shortcut absent from the selection is current, its declaration is unchanged,
    its destination is there, and its source has not moved, so replacing it
    would destroy a working pointer for nothing.
    """

    planned = plan_shortcuts(estate, selected=())

    assert planned.stage is None


@weaver_test()
def test_a_retained_shortcut_still_reports_its_schema(estate):
    """The subtle one, and the reason schemas are reported separately.

    A shortcut that is not being replaced still lives in a namespace the item
    must have. A build that created only the schemas its rebuilt shortcuts needed
    would leave the retained ones homeless.
    """

    planned = plan_shortcuts(estate, selected=())

    assert planned.schemas == ("DWG",)


@weaver_test()
def test_a_shortcut_whose_target_item_is_unbound_is_omitted(estate):
    """It has no physical form under these bindings, so it cannot be planned.

    And, the part that matters, it must not be certified either. A Registry row
    would claim an installation that never happened.
    """

    planned = plan_lakehouse_shortcuts(
        estate,
        item=item_id(CONSUMER),
        target=bound_target(id="curated", item_id="Curated_LH"),
        target_by_item={item_id(CONSUMER): bound_target(id="curated")},
        selected={document_id(SHORTCUT)},
    )

    assert planned.stage is None
    assert planned.omitted
    assert document_id(SHORTCUT) in planned.omitted_destinations


@weaver_test()
def test_an_unmaterialisable_shortcut_is_withheld_from_certification(estate):
    """The whole-item view of the claim above."""

    item = item_id(CONSUMER)
    target = bound_target(id="curated", item_id="Curated_LH")
    selected = {document_id(SHORTCUT)}

    planned = plan_item_build(
        estate,
        item=item,
        target=target,
        inventory=target_inventory(target_id="curated"),
        target_by_item={item: target},  # the producer is not bound
        selected_documents=set(),
        selected_shortcuts=selected,
        selected_for_drop=set(),
        selected_for_build=selected,
        registered={},
        catalogue_target=catalogue_target(),
    )

    assert document_id(SHORTCUT) in planned.uncertified


# --- ordering across items ----------------------------------------------------


@weaver_test()
def test_the_consumer_builds_its_view_after_the_shortcut_it_reads(estate):
    """Inside the consumer, the shortcut must exist before the view over it runs."""

    by_item = targets()
    item = item_id(CONSUMER)
    selected = {document_id(SHORTCUT), document_id(VIEW)}

    planned = plan_item_build(
        estate,
        item=item,
        target=by_item[item],
        inventory=target_inventory(target_id="curated"),
        target_by_item=by_item,
        selected_documents={document_id(VIEW)},
        selected_shortcuts={document_id(SHORTCUT)},
        selected_for_drop=set(),
        selected_for_build=selected,
        registered={},
        catalogue_target=catalogue_target(),
    )

    kinds = [
        action.kind
        for stage in planned.stages
        for batch in stage.batches
        for action in batch.actions
    ]
    assert kinds.index("create_shortcut") < kinds.index("build_view")


@weaver_test()
def test_the_consumer_gets_its_own_endpoint_refresh(estate):
    """An item that mutated Delta is closed by a refresh, shortcut or not."""

    by_item = targets()
    item = item_id(CONSUMER)

    planned = plan_item_build(
        estate,
        item=item,
        target=by_item[item],
        inventory=target_inventory(target_id="curated"),
        target_by_item=by_item,
        selected_documents={document_id(VIEW)},
        selected_shortcuts={document_id(SHORTCUT)},
        selected_for_drop=set(),
        selected_for_build={document_id(SHORTCUT), document_id(VIEW)},
        registered={},
        catalogue_target=catalogue_target(),
    )

    assert planned.stages[-1].phase == "refresh"


# --- staleness the graph cannot see -------------------------------------------


def certified(repository, *names, build_datetime=None):
    """The Registry as a successful build of these nodes would have left it.

    `declared_signatures` is used for shortcuts too, and: a shortcut
    destination is signed by the pair it declares. This destination, that
    source, not by any file, because that pair is the whole of what a shortcut is.
    A hand-written signature here would make the shortcut look changed and drag its
    consumers into the build, which is how the first version of this file was
    wrong.
    """

    signatures = declared_signatures(repository, {document_id(name) for name in names})
    return {
        document_id(name): registered_document(
            name, signature=signatures[document_id(name)], build_datetime=build_datetime
        )
        for name in names
    }


@weaver_test()
def test_a_pointer_is_stale_when_the_source_it_names_was_published_later(estate):
    """The half of cross-item freshness the dependency graph cannot answer.

    A producer rebuilt by some earlier build is, to this one, entirely
    unchanged. Nothing in the repository records that it moved. The only
    surviving evidence is that its Registry row carries a later build datetime
    than the pointer standing on it.

    What comes back is the pointer. Refreshing it re-dates its row, and the
    descendant walk carries the rebuild on to what reads it.
    """

    registered = {
        **certified(estate, SOURCE, build_datetime="2026-01-02T00:00:00"),
        **certified(estate, SHORTCUT, VIEW, build_datetime="2026-01-01T00:00:00"),
    }

    stale = stale_through_shortcuts(estate, registered, bound_items={item_id(CONSUMER)})

    assert stale == (document_id(SHORTCUT),)


@weaver_test()
def test_a_reader_behind_a_current_pointer_is_stale_on_its_own(estate):
    """The estate a build that refreshed the pointer and then stopped leaves.

    The pointer is dated after its source and needs nothing. The reader behind
    it is dated before the pointer, so it is named directly rather than through
    a walk from anything above it.
    """

    registered = {
        **certified(estate, SOURCE, build_datetime="2026-01-01T00:00:00"),
        **certified(estate, SHORTCUT, build_datetime="2026-01-02T00:00:00"),
        **certified(estate, VIEW, build_datetime="2026-01-01T00:00:00"),
    }

    stale = stale_through_shortcuts(estate, registered, bound_items={item_id(CONSUMER)})

    assert stale == (document_id(VIEW),)


@weaver_test()
def test_a_consumer_published_after_the_source_it_reads_is_current(estate):
    registered = {
        **certified(estate, SOURCE, build_datetime="2026-01-01T00:00:00"),
        **certified(estate, SHORTCUT, VIEW, build_datetime="2026-01-02T00:00:00"),
    }

    stale = stale_through_shortcuts(estate, registered, bound_items={item_id(CONSUMER)})

    assert stale == ()


@weaver_test()
def test_a_missing_registry_row_is_not_staleness(estate):
    """Absent is new, which signature classification already handles.

    Treating it as stale here would double-count, and worse would report an
    object as having moved when it was simply never installed.
    """

    registered = certified(estate, SOURCE, build_datetime="2026-01-02T00:00:00")

    stale = stale_through_shortcuts(estate, registered, bound_items={item_id(CONSUMER)})

    assert stale == ()


@weaver_test()
def test_an_unbound_consumer_stays_behind_its_source(estate):
    """That is the deferral: a build acts only on items it was pointed at."""

    registered = {
        **certified(estate, SOURCE, build_datetime="2026-01-02T00:00:00"),
        **certified(estate, SHORTCUT, VIEW, build_datetime="2026-01-01T00:00:00"),
    }

    stale = stale_through_shortcuts(estate, registered, bound_items={item_id(PRODUCER)})

    assert stale == ()


# --- when a pointer is refreshed, and when it is replaced ---------------------
#
# Four selections over one estate: the producer moved in an earlier build, the
# pair changed, nothing changed, and the producer changed in this build. A
# pointer is replaced when the pair it declares changes, refreshed over its own
# address when a source moves under it, and left alone otherwise.


def _selection(estate, registered, *, bound=None):
    """The whole estate selected, with freshness read as the planner reads it."""

    everything = {document_id(SOURCE), document_id(VIEW), document_id(SHORTCUT)}
    bound = set(targets()) if bound is None else bound
    return select_build(
        estate,
        registered,
        selected=everything,
        stale_consumers=stale_through_shortcuts(estate, registered, bound_items=bound),
        inventories=inventories(),
    )


@weaver_test()
def test_a_source_rebuilt_earlier_refreshes_the_pointer_and_the_reader(estate):
    """The cross-build case, and the whole reason freshness is read at all.

    The producer was rebuilt by an earlier build, which left nothing in the
    repository. The reader behind the shortcut is dated before it, so the reader
    is built again and the pointer it stands behind is materialised over its own
    address. The pointer is not dropped to do it: Fabric holds a deleted
    shortcut's name for tens of seconds.
    """

    registered = {
        **certified(estate, SOURCE, build_datetime="2026-01-02T00:00:00"),
        **certified(estate, VIEW, SHORTCUT, build_datetime="2026-01-01T00:00:00"),
    }

    selection = _selection(estate, registered)

    assert document_id(VIEW) in selection.selected_for_build
    assert document_id(SHORTCUT) in selection.selected_for_build
    assert document_id(SHORTCUT) not in selection.selected_for_drop


@weaver_test()
def test_a_changed_pair_replaces_the_pointer(estate):
    """The signature is the pair, so a repointed shortcut is a changed one."""

    registered = {
        **certified(
            estate, SOURCE, VIEW, SHORTCUT, build_datetime="2026-01-01T00:00:00"
        ),
        document_id(SHORTCUT): registered_document(SHORTCUT, signature="an-old-pair"),
    }

    selection = _selection(estate, registered)

    assert document_id(SHORTCUT) in selection.impact.changed
    assert document_id(SHORTCUT) in selection.selected_for_build


@weaver_test()
def test_a_second_build_over_an_unchanged_estate_plans_no_shortcut_action(estate):
    """An unchanged shortcut over an unchanged source must not be replaced.

    This is the decision `test_cross_item_shortcut_primitive.py` spent a full
    generate-and-install to observe. It is made from signatures and build datetimes before
    any pointer is touched, so it belongs here. What Fabric can still say is
    that the shortcut object itself was not disturbed.
    """

    registered = certified(
        estate, SOURCE, VIEW, SHORTCUT, build_datetime="2026-01-01T00:00:00"
    )

    selection = _selection(estate, registered)

    assert selection.selected_for_build == ()


@weaver_test()
def test_a_target_changed_in_this_build_refreshes_the_pointer_and_the_consumer(
    estate,
):
    """The graph carries a producer's change across the shortcut in one walk.

    Everything on the path is built: the pointer over its own address, and the
    consumer behind it. The producer's declaration changed in this build, so the
    descendant walk is what reaches them. The catalogue instants answer the other
    half, where a producer moved in an earlier build and the repository records
    nothing about it.
    """

    everything = {document_id(SOURCE), document_id(VIEW), document_id(SHORTCUT)}
    registered = {
        **certified(estate, SOURCE, VIEW, SHORTCUT),
        # Only the producer moved.
        document_id(SOURCE): registered_document(SOURCE, signature="an-old-hash"),
    }

    selection = select_build(
        estate, registered, selected=everything, inventories=inventories()
    )

    assert document_id(SOURCE) in selection.selected_for_build
    assert document_id(VIEW) in selection.selected_for_build
    # The pointer is refreshed over its own address, because a source rebuilt
    # under it may have been dropped and recreated. It is never dropped to do it.
    assert document_id(SHORTCUT) in selection.selected_for_build
    assert document_id(SHORTCUT) not in selection.selected_for_drop


# --- direct shortcuts, and the addresses frozen for them ----------------------


def _direct(tmp_path, body: str):
    """One item declaring only direct shortcuts, with nothing to bind to."""

    from factories import _write, lakehouse_table, schema_document

    from weaver.declaration import parse_item_repository
    from weaver.locations import Location

    root = tmp_path / "direct"
    _write(root, f"{CONSUMER}/schemas/DWG.yml", schema_document("DWG"))
    _write(root, f"{CONSUMER}/DWG__Report.py", lakehouse_table("DWG.Report"))
    _write(root, f"{CONSUMER}/shortcuts.py", "from weaver import Shortcut\n\n" + body)
    return parse_item_repository(Location(str(root)))


def _plan_direct(repository, sources, *, selected):
    by_item = targets()
    return plan_lakehouse_shortcuts(
        repository,
        item=item_id(CONSUMER),
        target=by_item[item_id(CONSUMER)],
        target_by_item=by_item,
        selected=selected,
        sources=sources,
    )


def _frozen(planned):
    import json

    action = next(action for batch in planned.stage.batches for action in batch.actions)
    return json.loads(planned.stage.payloads[action.payload].decode("utf-8"))[
        "shortcuts"
    ]


@weaver_test()
def test_a_direct_table_shortcut_freezes_the_resolved_physical_source(tmp_path):
    """The installer resolves targets of this build, and a direct source is not one."""

    from weaver.build_bundle.shortcuts import ResolvedShortcutSource

    repository = _direct(
        tmp_path,
        "DWG__External = Shortcut(\n"
        '    shortcut_type="table",\n'
        '    target_type="physical",\n'
        '    target="Lakehouse/Reference/DWG.Customer",\n'
        '    workspace="Shared Data",\n)\n',
    )
    declaration = repository.shortcuts[0]
    planned = _plan_direct(
        repository,
        {
            f"{declaration.owner}/{declaration.name}": ResolvedShortcutSource(
                workspace_id="ws-external",
                item_id="item-reference",
                item_name="Reference",
                path="Tables/DWG/Customer",
            )
        },
        selected={declaration.destination},
    )

    assert _frozen(planned) == [
        {
            "shortcut": "Lakehouse/Curated/DWG.External",
            "type": "table",
            "path": "Tables/DWG",
            "name": "External",
            "source": "Lakehouse/Reference/DWG.Customer",
            "source_workspace_id": "ws-external",
            "source_item_id": "item-reference",
            "source_item_name": "Reference",
            "source_path": "Tables/DWG/Customer",
        }
    ]


@weaver_test()
def test_a_schema_shortcut_is_created_directly_under_tables(tmp_path):
    """Measured against Fabric: a schema shortcut is path=Tables, name=<Schema>."""

    from weaver.build_bundle.shortcuts import ResolvedShortcutSource

    repository = _direct(
        tmp_path,
        "Reference = Shortcut(\n"
        '    shortcut_type="schema",\n'
        '    target_type="physical",\n'
        '    target="Lakehouse/Reference/DWG",\n'
        '    workspace="Shared Data",\n)\n',
    )
    declaration = repository.shortcuts[0]
    planned = _plan_direct(
        repository,
        {
            f"{declaration.owner}/{declaration.name}": ResolvedShortcutSource(
                workspace_id="ws-external",
                item_id="item-reference",
                item_name="Reference",
                path="Tables/DWG",
            )
        },
        selected={declaration.destination},
    )

    frozen = _frozen(planned)[0]
    assert (frozen["path"], frozen["name"], frozen["type"]) == (
        "Tables",
        "Reference",
        "schema",
    )


@weaver_test()
def test_a_schema_shortcut_asks_for_no_schema_of_its_own(tmp_path):
    """It is the namespace, and the item it points at owns what is inside."""

    from weaver.build_bundle.shortcuts import ResolvedShortcutSource

    repository = _direct(
        tmp_path,
        "Reference = Shortcut(\n"
        '    shortcut_type="schema",\n'
        '    target_type="physical",\n'
        '    target="Lakehouse/Reference/DWG",\n'
        '    workspace="Shared Data",\n)\n',
    )
    declaration = repository.shortcuts[0]
    planned = _plan_direct(
        repository,
        {
            f"{declaration.owner}/{declaration.name}": ResolvedShortcutSource(
                workspace_id="ws",
                item_id="item",
                item_name="Reference",
                path="Tables/DWG",
            )
        },
        selected={declaration.destination},
    )

    assert "Reference" not in planned.schemas


@weaver_test()
def test_a_direct_shortcut_with_no_resolved_source_is_omitted(tmp_path):
    """The installer may only run an action already frozen for it."""

    repository = _direct(
        tmp_path,
        "DWG__External = Shortcut(\n"
        '    shortcut_type="table",\n'
        '    target_type="physical",\n'
        '    target="Lakehouse/Reference/DWG.Customer",\n'
        '    workspace="Shared Data",\n)\n',
    )
    declaration = repository.shortcuts[0]
    planned = _plan_direct(repository, {}, selected={declaration.destination})

    assert planned.stage is None
    assert planned.omitted_destinations == (declaration.destination,)
    assert "not resolved when this bundle was generated" in planned.omitted[0].detail


@weaver_test()
def test_a_lakehouse_table_shortcut_can_read_a_bound_warehouse(tmp_path):
    """A Warehouse table is published in OneLake and uses the ordinary relation plan."""

    from factories import _write, lakehouse_table, schema_document, warehouse_table

    from weaver.declaration import parse_item_repository
    from weaver.locations import Location

    root = tmp_path / "warehouse-source"
    producer = "Warehouse/Serving"
    consumer = "Lakehouse/Published"
    _write(root, f"{producer}/schemas/SERVE.yml", schema_document("SERVE"))
    _write(
        root,
        f"{producer}/SERVE.Reporting.sql",
        warehouse_table("SERVE.Reporting"),
    )
    _write(root, f"{consumer}/schemas/PUB.yml", schema_document("PUB"))
    _write(root, f"{consumer}/schemas/WH.yml", schema_document("WH"))
    _write(root, f"{consumer}/PUB__Copy.py", lakehouse_table("PUB.Copy"))
    _write(
        root,
        f"{consumer}/shortcuts.py",
        "from weaver import Shortcut\n\n"
        "WH__Reporting = Shortcut(\n"
        '    shortcut_type="table",\n'
        '    target_type="logical",\n'
        '    target="Warehouse/Serving/SERVE.Reporting",\n)\n',
    )
    repository = parse_item_repository(Location(str(root)))
    declaration = next(
        declaration
        for declaration in repository.shortcuts
        if declaration.destination.object_id.qualified == "WH.Reporting"
    )
    by_item = {
        item_id(producer): bound_target(
            id="serving", kind="warehouse", item_id="Serving_WH"
        ),
        item_id(consumer): bound_target(id="published", item_id="Published_LH"),
    }

    planned = plan_lakehouse_shortcuts(
        repository,
        item=item_id(consumer),
        target=by_item[item_id(consumer)],
        target_by_item=by_item,
        selected={declaration.destination},
    )

    assert planned.omitted == ()
    assert _frozen(planned)[0]["source_target_id"] == "serving"
    assert _frozen(planned)[0]["source_area"] == "Tables"


@weaver_test()
def test_an_unreachable_physical_target_in_an_unbound_item_is_not_resolved(tmp_path):
    """A build resolves the physical targets of the items it is building.

    An item nobody is building has no business failing someone else's build, and
    a target Weaver cannot reach is a fault in the item that declares it.
    """

    from unittest.mock import patch

    from factories import _write, lakehouse_table, schema_document
    from support.sessions import given_session

    from weaver.build_bundle.targets import ItemBinding, ItemBindings, LakehouseBinding
    from weaver.build_bundle.workflow import read_build_state
    from weaver.catalogue.state import Catalogue
    from weaver.declaration import parse_item_repository
    from weaver.locations import Location
    from weaver.targets import ItemRef
    from weaver.workspaces import TargetDeclaration, Workspace

    root = tmp_path / "estate"
    for item, schema in ((CONSUMER, "DWG"), (PRODUCER, "DWG")):
        _write(root, f"{item}/schemas/{schema}.yml", schema_document(schema))
        _write(
            root, f"{item}/{schema}__Customer.py", lakehouse_table(f"{schema}.Customer")
        )
    # Declared by the item this build does not bind, and pointing at something
    # that would not resolve.
    _write(
        root,
        f"{PRODUCER}/shortcuts.py",
        "from weaver import Shortcut\n\n"
        "DWG__Absent = Shortcut(\n"
        '    shortcut_type="table",\n'
        '    target_type="physical",\n'
        '    target="Lakehouse/NoSuchItem/DWG.Customer",\n'
        '    workspace="No Such Workspace",\n)\n',
    )
    repository = parse_item_repository(Location(str(root)))

    workspace = Workspace(
        workspace="Demo",
        catalogue="Warehouse/Weaver",
        targets={
            item_id(CONSUMER): TargetDeclaration("Curated_LH"),
        },
    )
    bindings = ItemBindings(
        (
            ItemBinding(
                item_id(CONSUMER),
                LakehouseBinding(ItemRef("Curated_LH"), workspace_name="Demo"),
            ),
        )
    )

    resolved: list[str] = []

    def _refuse(shortcuts, **_kwargs):
        resolved.extend(f"{each.owner}/{each.name}" for each in shortcuts)
        raise AssertionError("an unbound item's shortcut must not be resolved")

    with (
        given_session(workspace=workspace, lakehouses=("Curated_LH",)) as session,
        patch("weaver.build_bundle.workflow.read_target_inventories", return_value={}),
        # An empty catalogue rather than None: a build reads a Catalogue, and
        # nothing is installed anywhere in this one.
        patch(
            "weaver.build_bundle.workflow._read_catalogue",
            return_value=Catalogue({}),
        ),
        patch(
            "weaver.build_bundle.workflow.read_shortcut_sources", side_effect=_refuse
        ),
    ):
        state = read_build_state(
            bindings,
            required_catalogue_items=(),
            session=session,
            workspace=workspace,
            shortcuts=repository.shortcuts,
        )

    assert resolved == []
    assert state.shortcut_sources == {}


@weaver_test()
def test_a_changed_schema_shortcut_does_not_walk_the_graph(tmp_path):
    """
    Intent: A build over an estate holding a schema shortcut selects work rather
    than failing.

    Proof: a schema shortcut establishes a namespace, so it is not a node in the
    authored graph. Classified as changed, it ends the impact walk instead of
    being looked up and refused.
    """

    from factories import _write, physical_schema_shortcut, schema_document

    from weaver.build_bundle.incremental import determine_impact
    from weaver.declaration.repository import parse_item_repository
    from weaver.locations import Location

    root = tmp_path / "repo"
    item = "Lakehouse/Landing"
    _write(root, f"{item}/schemas/LAND.yml", schema_document("LAND"))
    _write(
        root,
        *physical_schema_shortcut(
            item, target="Lakehouse/Foreign/Reference", workspace="Upstream"
        ),
    )
    repository = parse_item_repository(Location(str(root)))
    destination = repository.shortcuts[0].destination

    impact = determine_impact(
        repository,
        {destination: _Registered("a signature from an earlier build")},
        selected={destination},
        physical_types={destination: "schema"},
    )

    assert [str(one) for one in impact.changed] == [f"{item}/Reference"]
    assert impact.impacted_descendants == ()


class _Registered:
    """One Registry row, as classification reads one."""

    def __init__(self, signature: str) -> None:
        self.signature = signature
