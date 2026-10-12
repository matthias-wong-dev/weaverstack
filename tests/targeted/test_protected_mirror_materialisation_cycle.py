"""Protected borrowed contents take a preserving physical path during Build."""

import pytest
from factories import (
    ITEM,
    built_catalogue,
    catalogue_inventory,
    full_estate,
    item_id,
    plan_actions,
)
from support.bundles import build_metadata
from support.weaver_test import weaver_test
from support.workspaces import WORKSPACE
from test_lakehouse_mirror_lifecycle_cycle import (
    _bindings,
    _borrowing,
    _inventory,
    _object,
)

from weaver.build_bundle import WarehouseBinding, generate_item_build_bundle
from weaver.catalogue.builtin import BUILTIN_ITEM
from weaver.declaration import parse_item_repository
from weaver.locations import Location
from weaver.store import FilesystemStore
from weaver.targets import ItemRef


def protected_case(tmp_path, kind="folder"):
    root = tmp_path / "repo"
    full_estate(root)
    files = kind == "folder"
    name = "Raw.CustomerCsv" if files else "DWG.Customer"
    path = (
        root
        / ITEM
        / ("Files/Raw__CustomerCsv.py" if files else "Tables/DWG__Customer.py")
    )
    original = path.read_text()
    label = "Folder" if files else "Table"
    protected = original.replace(
        f"{label} ID: {name}", f"{label} ID: {name}\nProhibit rebuild: true"
    )
    assert protected != original
    path.write_bytes(protected.encode())
    installed = parse_item_repository(Location(str(root)))
    path.write_bytes(
        protected.replace("Description:", "Description: revised ", 1).encode()
    )
    changed = parse_item_repository(Location(str(root)))
    bundle = generate_item_build_bundle(
        changed,
        output=Location(str(tmp_path / "bundle")),
        bindings=_bindings(),
        store=FilesystemStore(),
        catalogue=_borrowing(built_catalogue(installed, _bindings())),
        target_inventories={
            item_id(ITEM): _inventory(installed),
            BUILTIN_ITEM: catalogue_inventory(holding=True),
        },
        catalogue_binding=WarehouseBinding(
            ItemRef("Weaver_Control"), workspace_name=WORKSPACE
        ),
    )
    return bundle, _object(name, files=files)


@weaver_test()
@pytest.mark.parametrize("kind", ["folder", "table"])
def test_edited_protected_mirror_is_materialised_instead_of_reset(tmp_path, kind):
    bundle, identity = protected_case(tmp_path, kind)
    physical = [a for a in plan_actions(bundle) if a.resource_node_id == str(identity)]
    assert [a.kind for a in physical] == ["materialise_mirror"]
    assert physical[0].executor == "materialise_mirror"


@weaver_test()
@pytest.mark.parametrize("kind", ["folder", "table"])
def test_materialisation_does_not_reset_the_retained_contents_bookmark(tmp_path, kind):
    bundle, identity = protected_case(tmp_path, kind)
    for state in build_metadata(bundle.plan).runtime_state_established:
        if state.table == "Bookmark":
            assert not [
                row
                for row in state.rows
                if row["object_name"] == identity.object_id.object
            ]


def protected_warehouse_case(tmp_path):
    from test_mirror_lifecycle_cycle import BORROWED, _build, _estate
    from warehouse_mirror import ITEM as WAREHOUSE_ITEM
    from warehouse_mirror import (
        mirror_bindings,
        mirror_inventory,
        mirror_object,
        with_borrowed,
    )

    root = tmp_path / "warehouse"
    _estate(root)
    path = root / WAREHOUSE_ITEM / "Sales.Customer.sql"
    text = path.read_text()
    protected = text.replace(
        "Table ID: Sales.Customer", "Table ID: Sales.Customer\nProhibit rebuild: true"
    )
    assert protected != text
    path.write_text(protected)
    installed = parse_item_repository(Location(str(root)))
    path.write_text(protected.replace("Description:", "Description: changed ", 1))
    changed = parse_item_repository(Location(str(root)))
    _, bundle = _build(
        changed,
        tmp_path / "warehouse-bundle",
        catalogue=with_borrowed(
            built_catalogue(installed, mirror_bindings()), *BORROWED
        ),
        inventory=mirror_inventory(installed, borrowed=BORROWED),
    )
    return bundle, mirror_object("Customer")


@weaver_test()
def test_protected_warehouse_wrapper_table_plans_native_materialisation_without_spark(
    tmp_path,
):
    from weaver.build_bundle.execution import plan_needs_spark

    bundle, identity = protected_warehouse_case(tmp_path)
    physical = [a for a in plan_actions(bundle) if a.resource_node_id == str(identity)]
    assert [a.kind for a in physical] == ["materialise_mirror"]
    assert not plan_needs_spark(bundle.plan)
    target = next(
        target for target in bundle.plan.targets if target.kind == "warehouse"
    )
    assert physical[0].resources == ("warehouse:" + target.item_id,)


@weaver_test()
@pytest.mark.parametrize("kind", ["folder", "table"])
def test_lakehouse_native_materialisation_has_spark_execution_and_limits(
    tmp_path, kind
):
    from weaver.build_bundle.execution import plan_needs_spark

    bundle, _ = protected_case(tmp_path, kind)
    assert plan_needs_spark(bundle.plan)
    action = next(a for a in plan_actions(bundle) if a.executor == "materialise_mirror")
    assert "spark" in action.resources


@weaver_test()
def test_preview_names_preserving_materialisation(tmp_path):
    from weaver.build_bundle.preview import BuildPreview

    bundle, _identity = protected_case(tmp_path)
    actions = [
        a
        for a in BuildPreview(bundle.plan, ()).to_mapping()["actions"]
        if a["kind"] == "materialise_mirror"
    ]
    assert len(actions) == 1
    assert actions[0]["classification"] == "materialise"
    assert actions[0]["destructive"] is True
    assert actions[0]["consequences"] == [
        "Remove the destination shortcut, retaining its source content",
        "Recursively copy source Folder contents into the disposable destination",
    ]


@weaver_test()
@pytest.mark.parametrize("kind", ["folder", "table", "warehouse"])
def test_materialisation_progress_names_bound_source_and_destination_without_action_count(
    tmp_path, kind
):
    from weaver.sessions.mutation_progress import stage_label

    bundle, _ = (
        protected_warehouse_case(tmp_path)
        if kind == "warehouse"
        else protected_case(tmp_path, kind)
    )
    sequence = next(
        s
        for s in bundle.plan.sequences
        if any(a.executor == "materialise_mirror" for b in s.batches for a in b.actions)
    )
    targets = {t.id: t for t in bundle.plan.targets}
    assert stage_label(sequence, targets, 1) == (
        "Materialising Warehouse/Model_Dev from Warehouse/Model"
        if kind == "warehouse"
        else "Materialising Lakehouse/Sales_Dev from Lakehouse/Sales_LH"
    )


@weaver_test()
@pytest.mark.parametrize("kind", ["table", "warehouse"])
def test_native_table_dry_run_names_mechanism_and_clone_retention(tmp_path, kind):
    from weaver.build_bundle.preview import BuildPreview

    bundle, _ = (
        protected_warehouse_case(tmp_path)
        if kind == "warehouse"
        else protected_case(tmp_path, kind)
    )
    action = next(
        a
        for a in BuildPreview(bundle.plan, ()).to_mapping()["actions"]
        if a["kind"] == "materialise_mirror"
    )
    text = "\n".join(action["consequences"])
    if kind == "warehouse":
        assert "CTAS" in text and "wrapper View" in text
    else:
        assert "SHALLOW CLONE" in text and "VACUUM" in text and "retention" in text
