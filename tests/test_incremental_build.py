"""Incremental impact and physical-action selection from prepared state."""

from __future__ import annotations

from weaver import ItemRef, LocalStore, Location
from weaver.build_bundle import (
    ItemBinding,
    ItemBindings,
    LakehouseBinding,
    determine_impact,
    generate_item_build_bundle,
)
from weaver.build_bundle.incremental import select_incremental_build
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
from weaver.catalogue.projection import project_item_installation
from weaver.catalogue.state import ReconciledCatalogue
from weaver.catalogue.tables import REGISTRY
from weaver.declaration import parse_item_repository
from weaver.declaration.model import WeaverDocumentId, WeaverItemId

from test_item_dependencies import _dependency_estate
from test_item_repository import _estate, _folder, _write


def _repository(root):
    return parse_item_repository(Location(str(root)))


def _catalogue(repository, item_text: str, *, old=()) -> ReconciledCatalogue:
    item = WeaverItemId.parse(item_text)
    retained = [identity for identity in repository.source_documents if identity.item == item]
    projection = project_item_installation(
        repository,
        item=item,
        retained=retained,
        target_name=f"{item.item_name}_Target",
        weaver_version="test",
    )
    old = set(old)
    rows = {}
    for table, values in projection.rows.items():
        copied = []
        for value in values:
            row = dict(value)
            schema = row.get("schema_name")
            name = row.get("object_name")
            if table == REGISTRY.name and (schema, name) in old:
                row["signature"] = "old-signature"
            copied.append(row)
        rows[table] = tuple(copied)
    return ReconciledCatalogue({item: rows})


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
    empty = determine_impact(repository, ReconciledCatalogue({}), selected=raw)
    assert set(empty.new) == raw
    assert empty.changed == empty.impacted == empty.unchanged == ()

    installed = _catalogue(
        repository, "Lakehouse/Raw", old=(("Sales", "Customer"),)
    )
    impact = determine_impact(repository, installed, selected=raw)
    assert [str(value) for value in impact.changed] == [
        "Lakehouse/Raw/Sales.Customer"
    ]
    assert set(impact.unchanged) == raw - set(impact.changed)


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
    impact = determine_impact(repository, catalogue, selected=raw)
    assert [str(value) for value in impact.changed] == [
        "Lakehouse/Raw/Files/Sales.Landing"
    ]
    assert {str(value) for value in impact.impacted_descendants} == {
        "Lakehouse/Raw/Files/Sales.Archive",
        "Lakehouse/Raw/Files/Sales.Export",
        "Lakehouse/Raw/Sales.Customer",
    }


def test_cross_item_descendants_are_deferred(tmp_path):
    repository = _repository(_dependency_estate(tmp_path))
    curated = WeaverDocumentId.parse("Lakehouse/Curated/Sales.Customer")
    reporting = WeaverDocumentId.parse("Warehouse/Reporting/Sales.Customer")
    rows = {}
    for item_text in ("Lakehouse/Curated", "Warehouse/Reporting"):
        rows.update(_catalogue(repository, item_text).rows)
    catalogue = ReconciledCatalogue(rows)
    catalogue.rows[WeaverItemId.parse("Lakehouse/Curated")][REGISTRY.name][0][
        "signature"
    ] = "old-signature"
    impact = determine_impact(
        repository, catalogue, selected=(curated, reporting)
    )
    assert impact.changed == (curated,)
    assert reporting not in impact.impacted


def test_prohibit_rebuild_retains_physical_object_but_builds_new_object(tmp_path):
    root = _estate(tmp_path)
    existing_path = root / "Lakehouse/Raw/Sales__Customer.py"
    existing_path.write_text(
        existing_path.read_text().replace(
            "Primary key: Id", "Primary key: Id\nProhibit rebuild: true"
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
    catalogue = ReconciledCatalogue({item: tables})
    selection = select_incremental_build(
        repository, catalogue, selected=selected
    )
    existing = WeaverDocumentId.parse("Lakehouse/Raw/Sales.Customer")
    new = WeaverDocumentId.parse("Lakehouse/Raw/Files/Sales.Protected")
    assert selection.prohibited == (existing,)
    assert existing not in selection.selected_for_drop
    assert existing not in selection.selected_for_build
    assert new in selection.selected_for_build


def test_planner_emits_no_physical_work_for_unchanged_repository(tmp_path):
    repository = _repository(_estate(tmp_path))
    bundle = generate_item_build_bundle(
        repository,
        bindings=_raw_binding(),
        output=Location(str(tmp_path / "bundle")),
        store=LocalStore(),
        target_inventories=_raw_inventory(repository),
        reconciled_catalogue=_catalogue(repository, "Lakehouse/Raw"),
        prune=True,
        control_lakehouse=LakehouseBinding(ItemRef("Weaver_Control")),
    )
    physical = {
        BUILD_FOLDER,
        BUILD_TABLE,
        DROP_FOLDER,
        DROP_TABLE,
    }
    assert not any(action.kind in physical for _sequence, _batch, action in bundle.plan.actions())
    assert bundle.plan.incremental_selection.selected_for_build == ()
    restored = BuildPlan.from_mapping(bundle.plan.to_mapping())
    assert restored.incremental_selection == bundle.plan.incremental_selection


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
        store=LocalStore(),
        target_inventories=_raw_inventory(repository),
        reconciled_catalogue=_catalogue(
            repository, "Lakehouse/Raw", old=(("Files/Sales", "Landing"),)
        ),
        prune=False,
        control_lakehouse=LakehouseBinding(ItemRef("Weaver_Control")),
    )
    actions = [
        (sequence.number, action.kind, action.resource_node_id)
        for sequence, _batch, action in bundle.plan.actions()
    ]
    catalogue_numbers = [
        number for number, kind, _identity in actions if kind == DELETE_CATALOGUE_CLAIMS
    ]
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
    bundle = generate_item_build_bundle(
        desired,
        bindings=_raw_binding(),
        output=Location(str(tmp_path / "bundle")),
        store=LocalStore(),
        target_inventories=_raw_inventory(installed),
        reconciled_catalogue=catalogue,
        prune=False,
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

    bundle = generate_item_build_bundle(
        desired,
        bindings=_raw_binding(),
        output=Location(str(tmp_path / "bundle")),
        store=LocalStore(),
        target_inventories=inventories,
        reconciled_catalogue=catalogue,
        prune=True,
        control_lakehouse=LakehouseBinding(ItemRef("Weaver_Control")),
    )
    actions = [
        (sequence.number, action.kind, action.resource_node_id)
        for sequence, _batch, action in bundle.plan.actions()
    ]
    delete_numbers = [
        number
        for number, kind, identity in actions
        if kind == DELETE_CATALOGUE_CLAIMS
        and identity == "Lakehouse/Raw/Files/Sales.Retired"
    ]
    prune_number = next(
        number
        for number, kind, identity in actions
        if kind == PRUNE_FOLDER and identity == "folder:Sales.Retired"
    )
    assert delete_numbers
    assert max(delete_numbers) < prune_number


def test_no_prune_preserves_removed_physical_object_and_its_catalogue_claims(
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
    inventories = _raw_inventory(installed)
    (root / "Lakehouse/Raw/Files/Sales__Retired.py").unlink()
    desired = _repository(root)
    store = LocalStore()
    bundle = generate_item_build_bundle(
        desired,
        bindings=_raw_binding(),
        output=Location(str(tmp_path / "bundle")),
        store=store,
        target_inventories=inventories,
        reconciled_catalogue=catalogue,
        prune=False,
        control_lakehouse=LakehouseBinding(ItemRef("Weaver_Control")),
    )

    actions = [action for _sequence, _batch, action in bundle.plan.actions()]
    retired = "Lakehouse/Raw/Files/Sales.Retired"
    assert not any(
        action.resource_node_id == retired
        and action.kind in {DELETE_CATALOGUE_CLAIMS, PRUNE_FOLDER}
        for action in actions
    )
    registry_delete = next(
        action
        for action in actions
        if action.id == "catalogue-Lakehouse--Raw-Registry-delete"
    )
    payload = store.read(
        bundle.location.join(*registry_delete.payload.split("/"))
    ).decode("utf-8")
    assert "Sales.Retired" not in payload  # identity is rendered as separate keys
    assert "'Files/Sales'" in payload
    assert "'Retired'" in payload
