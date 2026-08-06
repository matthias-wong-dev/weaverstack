"""Incremental impact and physical-action selection from prepared state."""

from __future__ import annotations

from datetime import datetime

from weaver.targets import ItemRef
from weaver.store import FilesystemStore
from weaver.locations import Location
from weaver.build_bundle import (
    ItemBinding,
    ItemBindings,
    LakehouseBinding,
    determine_impact,
    generate_item_build_bundle,
)
from weaver.build_bundle.incremental import select_build, stale_alias_destinations
from weaver.build_bundle.models import (
    BUILD_FOLDER,
    BUILD_TABLE,
    BUILD_VIEW,
    DELETE_CATALOGUE_CLAIMS,
    DROP_FOLDER,
    DROP_TABLE,
    PRUNE_FOLDER,
    BuildPlan,
)
from weaver.build_bundle.prune import TargetInventory
from weaver.catalogue.projection import (
    project_alias_registry,
    project_item_catalogue,
)
from weaver.catalogue.state import (
    Catalogue,
    reconcile_catalogue_state,
)
from weaver.catalogue.tables import REGISTRY
from weaver.declaration import parse_item_repository
from weaver.declaration.model import WeaverDocumentId, WeaverItemId

from test_item_dependencies import _dependency_estate
from test_item_repository import _estate, _folder, _table, _write


def _repository(root):
    return parse_item_repository(Location(str(root)))


def _catalogue(repository, item_text: str, *, old=()) -> ReconciledCatalogue:
    """One item's catalogue as a completed build would have left it.

    Every kind of registered object is included — documents, the item's alias
    destinations and its load artefacts — because that is what a real
    installation holds, and selection reads them all the same way.
    """

    from weaver.etl import item_load_artefacts

    item = WeaverItemId.parse(item_text)
    retained = [identity for identity in repository.source_documents if identity.item == item]
    retained.extend(
        alias.destination
        for alias in repository.aliases
        if alias.destination.item == item
    )
    retained.extend(
        artefact.identity for artefact in item_load_artefacts(repository, item=item)
    )
    projection = project_item_catalogue(repository, item=item, retained=retained)
    # Alias certification is a binding-time step now, so it is composed here to
    # give these tests the same complete catalogue they asserted against before.
    projected = dict(projection.rows)
    projected[REGISTRY.name] = tuple(
        projected.get(REGISTRY.name, ())
    ) + project_alias_registry(
        repository,
        item=item,
        retained=retained,
        target_kind="warehouse" if item.item_type == "Warehouse" else "lakehouse",
    )
    old = set(old)
    rows = {}
    for table, values in projected.items():
        copied = []
        for value in values:
            row = dict(value)
            schema = row.get("schema_name")
            name = row.get("object_name")
            if table == REGISTRY.name and (schema, name) in old:
                row["signature"] = "old-signature"
            copied.append(row)
        rows[table] = tuple(copied)
    return Catalogue({item: rows})


def _raw_binding(target="Raw_Target"):
    item = WeaverItemId.parse("Lakehouse/Raw")
    return ItemBindings((ItemBinding(item, LakehouseBinding(ItemRef(target))),))


def _raw_inventory(repository, target="Raw_Target"):
    item = WeaverItemId.parse("Lakehouse/Raw")
    bound = _raw_binding(target).by_item[item].to_bound_target()
    documents = [
        (identity, repository.source_documents[identity])
        for identity in repository.source_documents
        if identity.item == item
    ]
    return {
        item: TargetInventory(
            target_id=bound.id,
            kind=bound.kind,
            target_name=bound.name,
            schemas=("Sales",),
            folder_schemas=("Sales",),
            tables=tuple(
                source.qualified for identity, source in documents if not identity.is_files
            ),
            folders=tuple(
                source.qualified for identity, source in documents if identity.is_files
            ),
        )
    }


def test_impact_classifies_new_changed_and_unchanged_documents(tmp_path):
    repository = _repository(_estate(tmp_path))
    raw = {
        identity
        for identity in repository.source_documents
        if str(identity.item) == "Lakehouse/Raw"
    }
    empty = determine_impact(
        repository, Catalogue({}).registered, selected=raw
    )
    assert set(empty.new) == raw
    assert empty.changed == empty.impacted == ()

    installed = _catalogue(
        repository, "Lakehouse/Raw", old=(("Sales", "Customer"),)
    )
    impact = determine_impact(repository, installed.registered, selected=raw)
    assert [str(value) for value in impact.changed] == [
        "Lakehouse/Raw/Sales.Customer"
    ]
    assert set(impact.to_mapping()) == {"new", "changed", "impacted_descendants"}


def test_changed_root_expands_through_same_item_descendants(tmp_path):
    repository = _repository(_dependency_estate(tmp_path))
    raw = {
        identity
        for identity in repository.source_documents
        if str(identity.item) == "Lakehouse/Raw"
    }
    catalogue = _catalogue(
        repository, "Lakehouse/Raw", old=(("Files/Sales", "Landing"),)
    )
    impact = determine_impact(repository, catalogue.registered, selected=raw)
    assert [str(value) for value in impact.changed] == [
        "Lakehouse/Raw/Files/Sales.Landing"
    ]
    assert {str(value) for value in impact.impacted_descendants} == {
        "Lakehouse/Raw/Files/Sales.Archive",
        "Lakehouse/Raw/Files/Sales.Export",
        "Lakehouse/Raw/Sales.Customer",
    }


def _stale(rows, item_text: str, object_name: str) -> None:
    """Age one certified object's signature, found by name rather than position.

    An item's Registry rows are whatever it declares, and that set grows — the
    generated runtime folder joined it when load artefacts arrived. Reaching for
    a row by index made a test about signature comparison depend on how the rows
    happened to sort.
    """

    registry = rows[WeaverItemId.parse(item_text)][REGISTRY.name]
    matched = [row for row in registry if row["object_name"] == object_name]
    assert len(matched) == 1, f"{object_name} is not one row in {item_text}"
    matched[0]["signature"] = "old-signature"


def test_cross_item_descendants_propagate_when_both_items_are_bound(tmp_path):
    """Impact crosses the alias, because the alias is in the graph.

    The consumer is in another item and reaches its producer only through
    ``Sales.PortableCustomer``. Nothing here special-cases that: the walk is the
    ordinary descendant walk, and the alias is an ordinary hop on it.
    """

    repository = _repository(_dependency_estate(tmp_path))
    curated = WeaverDocumentId.parse("Lakehouse/Curated/Sales.Customer")
    reporting = WeaverDocumentId.parse("Warehouse/Reporting/Sales.Customer")
    rows = {}
    for item_text in ("Lakehouse/Curated", "Warehouse/Reporting"):
        rows.update(_catalogue(repository, item_text).rows)
    _stale(rows, "Lakehouse/Curated", "Customer")
    catalogue = Catalogue(rows)
    impact = determine_impact(
        repository, catalogue.registered, selected=(curated, reporting)
    )
    assert impact.changed == (curated,)
    assert reporting in impact.impacted_descendants


def test_an_item_left_out_of_the_build_is_still_deferred(tmp_path):
    """Deferral is now by construction rather than by rule.

    The same changed producer, with the consumer simply not selected: nothing
    reaches it, because impact only ever names nodes the build was asked about.
    """

    repository = _repository(_dependency_estate(tmp_path))
    curated = WeaverDocumentId.parse("Lakehouse/Curated/Sales.Customer")
    reporting = WeaverDocumentId.parse("Warehouse/Reporting/Sales.Customer")
    rows = {}
    for item_text in ("Lakehouse/Curated", "Warehouse/Reporting"):
        rows.update(_catalogue(repository, item_text).rows)
    _stale(rows, "Lakehouse/Curated", "Customer")
    catalogue = Catalogue(rows)
    impact = determine_impact(repository, catalogue.registered, selected=(curated,))

    assert impact.changed == (curated,)
    assert reporting not in impact.impacted


def test_prohibit_rebuild_retains_physical_object_but_builds_new_object(tmp_path):
    root = _estate(tmp_path)
    existing_path = root / "Lakehouse/Raw/Sales__Customer.py"
    existing_path.write_text(
        existing_path.read_text().replace(
            "Description: A declared table.",
            "Description: The current governed declaration.",
        ).replace(
            "Lineage: A source system.",
            "Lineage: A revised source.",
        ).replace(
            "Primary key: Id", "Primary key: Id\nProhibit rebuild: true"
        ).replace(
            "from weaver import Table",
            "from weaver import Table\n"
            "from .Files.Sales__Customer import Sales__Customer as SourceCustomer",
        ),
        encoding="utf-8",
    )
    _write(
        root,
        "Lakehouse/Raw/Files/Sales__Protected.py",
        _folder("Sales.Protected").replace(
            'File key: "*.csv"', 'File key: "*.csv"\nProhibit rebuild: true'
        ),
    )
    repository = _repository(root)
    selected = {
        identity
        for identity in repository.source_documents
        if str(identity.item) == "Lakehouse/Raw"
    }
    catalogue = _catalogue(
        repository, "Lakehouse/Raw", old=(("Sales", "Customer"),)
    )
    # The newly authored protected folder is not installed yet.
    item = WeaverItemId.parse("Lakehouse/Raw")
    tables = dict(catalogue.rows[item])
    tables[REGISTRY.name] = tuple(
        row for row in tables[REGISTRY.name] if row["object_name"] != "Protected"
    )
    catalogue = Catalogue({item: tables})
    selection = select_build(repository, catalogue.registered, selected=selected)
    existing = WeaverDocumentId.parse("Lakehouse/Raw/Sales.Customer")
    new = WeaverDocumentId.parse("Lakehouse/Raw/Files/Sales.Protected")
    assert selection.prohibited == (existing,)
    assert existing not in selection.selected_for_drop
    assert existing not in selection.selected_for_build
    assert new in selection.selected_for_build

    store = FilesystemStore()
    bundle = generate_item_build_bundle(
        repository,
        bindings=_raw_binding(),
        output=Location(str(tmp_path / "bundle")),
        store=store,
        target_inventories=_raw_inventory(repository),
        catalogue=catalogue,
        control_lakehouse=LakehouseBinding(ItemRef("Weaver_Control")),
    )
    customer_actions = [
        action
        for _sequence, _batch, action in bundle.plan.actions()
        if action.resource_node_id == str(existing)
    ]
    assert not any(
        action.kind in {DROP_TABLE, BUILD_TABLE} for action in customer_actions
    )
    catalogue_action = next(
        action
        for _sequence, _batch, action in bundle.plan.actions()
        if action.kind == "publish_catalogue"
    )
    catalogue_payload = store.read(
        bundle.location.join(*catalogue_action.payload.split("/"))
    ).decode()
    assert "The current governed declaration." in catalogue_payload
    assert ".Files.Sales__Customer" in catalogue_payload
    registry_action = next(
        action
        for _sequence, _batch, action in bundle.plan.actions()
        if action.kind == "publish_registry"
    )
    registry_payload = store.read(
        bundle.location.join(*registry_action.payload.split("/"))
    ).decode()
    assert repository.source_documents[existing].effective_signature in registry_payload


ALIAS_DESTINATION = "Warehouse/Reporting/Sales.PortableCustomer"

#: Two publication instants, in order. Datetimes because that is what Spark
#: hands back for a timestamp column.
EARLIER = datetime(2026, 7, 30, 9, 0, 0)
LATER = datetime(2026, 7, 31, 9, 0, 0)


def _alias_bindings():
    """The producer and the consumer that aliases it, both bound."""

    from weaver.build_bundle import WarehouseBinding

    return ItemBindings(
        (
            ItemBinding(
                WeaverItemId.parse("Lakehouse/Curated"),
                LakehouseBinding(ItemRef("Curated_Target")),
            ),
            ItemBinding(
                WeaverItemId.parse("Warehouse/Reporting"),
                WarehouseBinding(ItemRef("Reporting_Target")),
            ),
        )
    )


def _alias_inventories(repository, *, alias_installed=True):
    """Both targets as they stand, with the alias view present or absent.

    The alias destination is an ordinary view in the Warehouse's inventory —
    which is exactly the point of registering it as one — so leaving it out is
    how "somebody deleted the alias" is expressed.
    """

    inventories = {}
    for binding in _alias_bindings().entries:
        target = binding.to_bound_target()
        item = binding.item
        objects = [
            repository.source_documents[identity].qualified
            for identity in repository.source_documents
            if identity.item == item and not identity.is_files
        ]
        views = list(objects) if target.kind == "warehouse" else []
        tables = [] if target.kind == "warehouse" else list(objects)
        if alias_installed and item == WeaverItemId.parse("Warehouse/Reporting"):
            views.append("Sales.PortableCustomer")
        inventories[item] = TargetInventory(
            target_id=target.id,
            kind=target.kind,
            target_name=target.name,
            schemas=("Sales",),
            tables=tuple(tables),
            views=tuple(views),
        )
    return inventories


def _alias_catalogue(repository):
    rows = {}
    for item_text in ("Lakehouse/Curated", "Warehouse/Reporting"):
        rows.update(_catalogue(repository, item_text).rows)
    return rows


def _alias_bundle(tmp_path, repository, *, rows, alias_installed=True, name="bundle"):
    return generate_item_build_bundle(
        repository,
        bindings=_alias_bindings(),
        output=Location(str(tmp_path / name)),
        store=FilesystemStore(),
        target_inventories=_alias_inventories(
            repository, alias_installed=alias_installed
        ),
        catalogue=Catalogue(rows),
        control_lakehouse=LakehouseBinding(ItemRef("Weaver_Control")),
    )


def _alias_actions(bundle):
    return [
        action
        for _sequence, _batch, action in bundle.plan.actions()
        if action.kind == "create_alias"
    ]


def test_an_unchanged_alias_is_not_replaced(tmp_path):
    """The behaviour this whole change exists for.

    An alias used to be remade on every build. Now its declaration is unchanged,
    its destination is present and its source was not rebuilt — so there is
    nothing to do, and a shortcut that takes seconds to become readable is not
    torn down and remade for nothing.
    """

    repository = _repository(_dependency_estate(tmp_path))
    bundle = _alias_bundle(tmp_path, repository, rows=_alias_catalogue(repository))

    assert _alias_actions(bundle) == []


def test_a_repointed_alias_is_replaced(tmp_path):
    """The declaration *is* the alias, so changing what it points at changes it."""

    root = _dependency_estate(tmp_path)
    _write(root, "Lakehouse/Curated/Sales__Archive.py", _table("Sales.Archive"))
    installed = _alias_catalogue(_repository(root))
    _write(
        root,
        "Warehouse/Reporting/alias.yml",
        "aliases:\n  Sales.PortableCustomer: Lakehouse/Curated/Sales.Archive\n",
    )
    repository = _repository(root)
    bundle = _alias_bundle(tmp_path, repository, rows=installed)

    assert len(_alias_actions(bundle)) == 1


def test_an_alias_whose_destination_is_gone_is_remade(tmp_path):
    """Registered but not there: reconciliation drops the row, so it reads as new.

    Nothing alias-specific does this — the alias is registered as a view, and the
    Warehouse inventory simply does not hold one.
    """

    repository = _repository(_dependency_estate(tmp_path))
    state = Catalogue(
        rows=_alias_catalogue(repository),
        present_tables=frozenset({REGISTRY.name}),
    )
    reconciled = reconcile_catalogue_state(
        state, inventories=_alias_inventories(repository, alias_installed=False)
    )

    assert ALIAS_DESTINATION in reconciled.stale_objects

    bundle = _alias_bundle(
        tmp_path, repository, rows=reconciled.catalogue.rows, alias_installed=False
    )
    assert len(_alias_actions(bundle)) == 1


def test_an_alias_is_never_dropped_by_the_document_pipeline(tmp_path):
    """Replacing an alias is the alias executor's job, not a drop and a build.

    It holds no data, so it is remade in place; routing it through the generic
    drop would emit a ``drop view`` for a shortcut and ask the build pipeline for
    a source document that does not exist.
    """

    root = _dependency_estate(tmp_path)
    _write(root, "Lakehouse/Curated/Sales__Archive.py", _table("Sales.Archive"))
    installed = _alias_catalogue(_repository(root))
    _write(
        root,
        "Warehouse/Reporting/alias.yml",
        "aliases:\n  Sales.PortableCustomer: Lakehouse/Curated/Sales.Archive\n",
    )
    repository = _repository(root)
    bundle = _alias_bundle(tmp_path, repository, rows=installed)

    assert len(_alias_actions(bundle)) == 1
    assert all(
        action.resource_node_id != ALIAS_DESTINATION
        for _sequence, _batch, action in bundle.plan.actions()
        if action.kind != "create_alias"
    )


def _dated(rows, item_text, schema, name, epoch):
    """Stamp one Registry row with a build epoch, as a publication would."""

    item = WeaverItemId.parse(item_text)
    tables = dict(rows[item])
    tables[REGISTRY.name] = tuple(
        {**row, "build_epoch": epoch}
        if (row["schema_name"], row["object_name"]) == (schema, name)
        else row
        for row in tables[REGISTRY.name]
    )
    return {**rows, item: tables}


def _consumer_only_selection(repository, rows):
    """Select as a build of the consumer item alone would."""

    consumer = WeaverItemId.parse("Warehouse/Reporting")
    registered = Catalogue(rows).registered
    return select_build(
        repository,
        {
            identity: document
            for identity, document in registered.items()
            if identity.item == consumer
        },
        selected=(
            {
                identity
                for identity in repository.source_documents
                if identity.item == consumer
            }
            | {
                alias.destination
                for alias in repository.aliases
                if alias.destination.item == consumer
            }
        ),
        stale_aliases=stale_alias_destinations(
            repository, registered, bound_items={consumer}
        ),
    )


def test_an_alias_is_stale_when_its_unbound_source_was_published_later(tmp_path):
    """The case the graph cannot answer.

    The producer is not in this build, so there is no walk from it. It was
    rebuilt at some earlier time by some earlier build, and the only surviving
    evidence is that its Registry row is dated after the alias's.
    """

    repository = _repository(_dependency_estate(tmp_path))
    rows = _alias_catalogue(repository)
    rows = _dated(rows, "Warehouse/Reporting", "Sales", "PortableCustomer", EARLIER)
    rows = _dated(rows, "Lakehouse/Curated", "Sales", "Customer", LATER)

    selection = _consumer_only_selection(repository, rows)
    destination = WeaverDocumentId.parse(ALIAS_DESTINATION)

    assert destination in selection.impact.changed
    assert destination in selection.selected_for_build


def test_a_stale_alias_carries_its_consumers_with_it(tmp_path):
    """It joins the ordinary changed roots, so the ordinary walk does the rest —
    there is no separate cross-item descendant handling."""

    repository = _repository(_dependency_estate(tmp_path))
    rows = _alias_catalogue(repository)
    rows = _dated(rows, "Warehouse/Reporting", "Sales", "PortableCustomer", EARLIER)
    rows = _dated(rows, "Lakehouse/Curated", "Sales", "Customer", LATER)

    selection = _consumer_only_selection(repository, rows)
    consumer = WeaverDocumentId.parse("Warehouse/Reporting/Sales.Customer")

    assert consumer in selection.impact.impacted_descendants
    assert consumer in selection.selected_for_build


def test_an_alias_published_after_its_source_is_left_alone(tmp_path):
    """The ordinary case, and the one that has to stay cheap."""

    repository = _repository(_dependency_estate(tmp_path))
    rows = _alias_catalogue(repository)
    rows = _dated(rows, "Warehouse/Reporting", "Sales", "PortableCustomer", LATER)
    rows = _dated(rows, "Lakehouse/Curated", "Sales", "Customer", EARLIER)

    selection = _consumer_only_selection(repository, rows)

    assert selection.selected_for_build == ()


def test_a_catalogue_with_no_epochs_at_all_reports_nothing_stale(tmp_path):
    """Upgrading from a catalogue written before epochs existed must not rebuild
    the estate. Both rows read as null, and null is not newer than null."""

    repository = _repository(_dependency_estate(tmp_path))
    registered = Catalogue(_alias_catalogue(repository)).registered

    assert all(document.build_epoch is None for document in registered.values())
    assert stale_alias_destinations(
        repository, registered, bound_items={WeaverItemId.parse("Warehouse/Reporting")}
    ) == ()


def test_a_source_inside_the_build_is_still_judged_by_its_epoch(tmp_path):
    """A producer rebuilt by an *earlier* build is unchanged to this one.

    Binding it changes nothing: its signature matches the repository, so the
    descendant walk never starts from it, and only the epochs record that it
    moved after the alias was made. Were the comparison skipped whenever the
    producer happened to be bound, that estate would stay stale forever.
    """

    repository = _repository(_dependency_estate(tmp_path))
    rows = _alias_catalogue(repository)
    rows = _dated(rows, "Warehouse/Reporting", "Sales", "PortableCustomer", EARLIER)
    rows = _dated(rows, "Lakehouse/Curated", "Sales", "Customer", LATER)
    both = {
        WeaverItemId.parse("Warehouse/Reporting"),
        WeaverItemId.parse("Lakehouse/Curated"),
    }

    assert stale_alias_destinations(
        repository, Catalogue(rows).registered, bound_items=both
    ) == (WeaverDocumentId.parse(ALIAS_DESTINATION),)


def test_an_unbuilt_consumer_keeps_its_stale_alias(tmp_path):
    """Deferral: only the producer is bound, so nothing about the consumer is
    touched and its alias stays stale until the consumer is next built."""

    repository = _repository(_dependency_estate(tmp_path))
    rows = _alias_catalogue(repository)
    rows = _dated(rows, "Warehouse/Reporting", "Sales", "PortableCustomer", EARLIER)
    rows = _dated(rows, "Lakehouse/Curated", "Sales", "Customer", LATER)

    assert stale_alias_destinations(
        repository,
        Catalogue(rows).registered,
        bound_items={WeaverItemId.parse("Lakehouse/Curated")},
    ) == ()


def test_a_stale_alias_is_replaced_by_the_alias_executor(tmp_path):
    """End to end through the planner: the freshness comparison reaches the
    physical action, and reaches it as an alias action rather than a drop."""

    repository = _repository(_dependency_estate(tmp_path))
    rows = _alias_catalogue(repository)
    rows = _dated(rows, "Warehouse/Reporting", "Sales", "PortableCustomer", EARLIER)
    rows = _dated(rows, "Lakehouse/Curated", "Sales", "Customer", LATER)

    bundle = _alias_bundle(tmp_path, repository, rows=rows)

    assert len(_alias_actions(bundle)) == 1


def test_the_epoch_leaves_bundle_identity_alone(tmp_path):
    """Generating twice must give the same bytes, or a bundle could not be
    compared against another built from the same source. The epoch is a token in
    the payload for exactly this reason — the installer resolves it, not the
    planner."""

    repository = _repository(_dependency_estate(tmp_path))
    rows = _alias_catalogue(repository)
    first = _alias_bundle(tmp_path, repository, rows=rows, name="first")
    second = _alias_bundle(tmp_path, repository, rows=rows, name="second")

    assert first.plan.bundle_id == second.plan.bundle_id


def test_the_registry_payload_carries_the_token_unresolved(tmp_path):
    repository = _repository(_dependency_estate(tmp_path))
    store = FilesystemStore()
    bundle = _alias_bundle(tmp_path, repository, rows={})
    registry = next(
        action
        for _sequence, _batch, action in bundle.plan.actions()
        if action.kind == "publish_registry"
    )
    payload = store.read(
        bundle.location.join(*registry.payload.split("/"))
    ).decode()

    assert "{{epoch}}" in payload
    assert "build_epoch" in payload


def test_planner_emits_no_physical_work_for_unchanged_repository(tmp_path):
    repository = _repository(_estate(tmp_path))
    store = FilesystemStore()
    bundle = generate_item_build_bundle(
        repository,
        bindings=_raw_binding(),
        output=Location(str(tmp_path / "bundle")),
        store=store,
        target_inventories=_raw_inventory(repository),
        catalogue=_catalogue(repository, "Lakehouse/Raw"),
        control_lakehouse=LakehouseBinding(ItemRef("Weaver_Control")),
    )
    physical = {
        BUILD_FOLDER,
        BUILD_TABLE,
        DROP_FOLDER,
        DROP_TABLE,
    }
    assert not any(action.kind in physical for _sequence, _batch, action in bundle.plan.actions())
    assert bundle.plan.selection.selected_for_build == ()
    restored = BuildPlan.from_mapping(bundle.plan.to_mapping())
    assert restored.selection == bundle.plan.selection


def test_changed_root_uncertifies_drops_and_rebuilds_in_dependency_order(tmp_path):
    root = _dependency_estate(tmp_path)
    for name in ("Sales__Landing.py", "Sales__Archive.py", "Sales__Export.py"):
        path = root / "Lakehouse/Raw/Files" / name
        path.write_text(
            path.read_text().replace(
                'File key: "*.csv"',
                'File key: "*.csv"\nProhibit rebuild: false',
            ),
            encoding="utf-8",
        )
    repository = _repository(root)
    bundle = generate_item_build_bundle(
        repository,
        bindings=_raw_binding(),
        output=Location(str(tmp_path / "bundle")),
        store=FilesystemStore(),
        target_inventories=_raw_inventory(repository),
        catalogue=_catalogue(
            repository, "Lakehouse/Raw", old=(("Files/Sales", "Landing"),)
        ),
        control_lakehouse=LakehouseBinding(ItemRef("Weaver_Control")),
    )
    actions = [
        (sequence.number, action.kind, action.resource_node_id)
        for sequence, _batch, action in bundle.plan.actions()
    ]
    catalogue_numbers = [
        number for number, kind, _identity in actions if kind == DELETE_CATALOGUE_CLAIMS
    ]
    assert len(catalogue_numbers) == 1
    drop_numbers = [
        number for number, kind, _identity in actions if kind in {DROP_FOLDER, DROP_TABLE}
    ]
    build_numbers = [
        number for number, kind, _identity in actions if kind in {BUILD_FOLDER, BUILD_TABLE}
    ]
    assert max(catalogue_numbers) < min(drop_numbers) < min(build_numbers)
    dropped = [
        identity for _number, kind, identity in actions if kind in {DROP_FOLDER, DROP_TABLE}
    ]
    built = [
        identity for _number, kind, identity in actions if kind in {BUILD_FOLDER, BUILD_TABLE}
    ]
    assert dropped.index("Lakehouse/Raw/Files/Sales.Export") < dropped.index(
        "Lakehouse/Raw/Sales.Customer"
    )
    assert built.index("Lakehouse/Raw/Sales.Customer") < built.index(
        "Lakehouse/Raw/Files/Sales.Export"
    )


def test_managed_drop_uses_the_installed_type_when_an_object_changes_type(tmp_path):
    root = _estate(tmp_path)
    installed = _repository(root)
    catalogue = _catalogue(installed, "Lakehouse/Raw")
    (root / "Lakehouse/Raw/Sales__Customer.py").unlink()
    _write(
        root,
        "Lakehouse/Raw/Sales.Customer.sql",
        """/*
View ID: Sales.Customer
Description: A replacement view.
Lineage: A source system.
Dependencies: []
*/
select 1 as Id
""",
    )
    desired = _repository(root)
    store = FilesystemStore()
    bundle = generate_item_build_bundle(
        desired,
        bindings=_raw_binding(),
        output=Location(str(tmp_path / "bundle")),
        store=store,
        target_inventories=_raw_inventory(installed),
        catalogue=catalogue,
        control_lakehouse=LakehouseBinding(ItemRef("Weaver_Control")),
    )
    actions = [action for _sequence, _batch, action in bundle.plan.actions()]
    customer = "Lakehouse/Raw/Sales.Customer"
    assert any(
        action.kind == DROP_TABLE and action.resource_node_id == customer
        for action in actions
    )
    assert any(
        action.kind == BUILD_VIEW and action.resource_node_id == customer
        for action in actions
    )


def test_registered_document_removed_from_repository_is_uncertified_before_prune(
    tmp_path,
):
    root = _estate(tmp_path)
    _write(
        root,
        "Lakehouse/Raw/Files/Sales__Retired.py",
        _folder("Sales.Retired"),
    )
    installed = _repository(root)
    catalogue = _catalogue(installed, "Lakehouse/Raw")
    (root / "Lakehouse/Raw/Files/Sales__Retired.py").unlink()
    desired = _repository(root)
    inventories = _raw_inventory(desired)
    item = WeaverItemId.parse("Lakehouse/Raw")
    inventory = inventories[item]
    inventories[item] = TargetInventory(
        target_id=inventory.target_id,
        kind=inventory.kind,
        target_name=inventory.target_name,
        schemas=inventory.schemas,
        folder_schemas=inventory.folder_schemas,
        tables=inventory.tables,
        folders=inventory.folders + ("Sales.Retired",),
    )

    store = FilesystemStore()
    bundle = generate_item_build_bundle(
        desired,
        bindings=_raw_binding(),
        output=Location(str(tmp_path / "bundle")),
        store=store,
        target_inventories=inventories,
        catalogue=catalogue,
        control_lakehouse=LakehouseBinding(ItemRef("Weaver_Control")),
    )
    actions = [
        (sequence.number, action.kind, action.resource_node_id)
        for sequence, _batch, action in bundle.plan.actions()
    ]
    delete_action = next(
        (sequence.number, action)
        for sequence, _batch, action in bundle.plan.actions()
        if action.kind == DELETE_CATALOGUE_CLAIMS
    )
    prune_number = next(
        number
        for number, kind, identity in actions
        if kind == PRUNE_FOLDER and identity == "folder:Sales.Retired"
    )
    payload = store.read(
        bundle.location.join(*delete_action[1].payload.split("/"))
    ).decode()
    assert "'Files/Sales'" in payload and "'Retired'" in payload
    assert delete_action[0] < prune_number
