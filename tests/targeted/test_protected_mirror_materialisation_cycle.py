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
    assert actions[0]["destructive"] is False
    assert actions[0]["consequences"] == [
        "Copy protected contents into an independent local object before removing the shortcut"
    ]
