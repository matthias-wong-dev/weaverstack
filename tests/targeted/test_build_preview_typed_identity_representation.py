"""Preview changes retain the physical namespace of canonical actions."""

from dataclasses import replace

import pytest
from factories import _write, folder_document, full_estate
from support.weaver_test import weaver_test
from test_build_fixed_point_cycle import _bindings, _inventories, installed_catalogue
from test_build_preview_representation import preview

from weaver.declaration import parse_item_repository
from weaver.declaration.model import WeaverItemId
from weaver.locations import Location

ITEM = WeaverItemId("Lakehouse", "Sales")
NAME = "DWG.Customer"


def collision_estate(root, *, validation=False):
    full_estate(root)
    _write(root, "Lakehouse/Sales/Files/DWG__Customer.py", folder_document(NAME))
    _write(root, "Lakehouse/Sales/Files/DWG__Other.py", folder_document("DWG.Other"))
    if validation:
        _write(
            root,
            "Lakehouse/Sales/tests/DWG.Customer.sql",
            "/*\nTest ID: DWG.Customer\nDescription: Customer rows.\n"
            "Primary key: CustomerId\n*/\n"
            "select 1 as CustomerId;\nselect 1 as CustomerId;\n",
        )
    return parse_item_repository(Location(str(root)))


def inventories(repository):
    return _inventories(
        repository, {b.item: b.to_bound_target() for b in _bindings().entries}
    )


@pytest.mark.parametrize(
    "area,kind,collection",
    [("Tables", "table", "tables"), ("Files", "folder", "folders")],
)
@pytest.mark.parametrize("validation", [False, True])
@weaver_test()
def test_missing_typed_object_is_created_despite_same_named_retained_object(
    tmp_path, area, kind, collection, validation
):
    repository = collision_estate(tmp_path / "repo", validation=validation)
    found = inventories(repository)
    found[ITEM] = replace(
        found[ITEM],
        **{collection: tuple(n for n in getattr(found[ITEM], collection) if n != NAME)},
    )
    result = preview(
        repository,
        tmp_path / "plan",
        installed_catalogue(repository),
        inventories=found,
    )
    expected = f"{ITEM}/{area}/{NAME}"
    mapping = result.to_mapping()
    additions = [
        c
        for a in mapping["actions"]
        for c in a["physical_changes"]
        if c["name"] == NAME and c["object_kind"] == kind and c["effect"] == "add"
    ]
    assert additions
    assert all(c["object_id"] == expected for c in additions)
    assert all(c["classification"] == "create" for c in additions)
    objects = {o["identity"]: o for o in mapping["objects"]}
    assert objects[expected]["classification"] == "new"
    other = "Files" if area == "Tables" else "Tables"
    assert objects[f"{ITEM}/{other}/{NAME}"]["classification"] == "unchanged"


@pytest.mark.parametrize("area,kind", [("Tables", "table"), ("Files", "folder")])
@pytest.mark.parametrize("validation", [False, True])
@weaver_test()
def test_rebuild_keeps_typed_add_and_drop_identity(tmp_path, area, kind, validation):
    root = tmp_path / "repo"
    before = collision_estate(root, validation=validation)
    catalogue = installed_catalogue(before)
    found = inventories(before)
    path = root / f"Lakehouse/Sales/{area}/DWG__Customer.py"
    path.write_bytes(
        path.read_bytes().replace(b"Description:", b"Description: Revised")
    )
    repository = parse_item_repository(Location(str(root)))
    result = preview(repository, tmp_path / "plan", catalogue, inventories=found)
    expected = f"{ITEM}/{area}/{NAME}"
    changes = [
        c
        for a in result.to_mapping()["actions"]
        for c in a["physical_changes"]
        if c["name"] == NAME and c["object_kind"] == kind
    ]
    assert {c["effect"] for c in changes} == {"add", "remove"}
    assert all(c["object_id"] == expected for c in changes)
    assert {c["classification"] for c in changes} == {"drop", "replace"}


@pytest.mark.parametrize("area,kind", [("Tables", "table"), ("Files", "folder")])
@pytest.mark.parametrize("validation", [False, True])
@weaver_test()
def test_removal_keeps_the_deleted_physical_namespace(tmp_path, area, kind, validation):
    root = tmp_path / "repo"
    before = collision_estate(root, validation=validation)
    catalogue = installed_catalogue(before)
    found = inventories(before)
    (root / f"Lakehouse/Sales/{area}/DWG__Customer.py").unlink()
    if area == "Tables":
        (root / "Lakehouse/Sales/Tables/DWG.ActiveCustomer.sql").unlink()
    repository = parse_item_repository(Location(str(root)))
    result = preview(repository, tmp_path / "plan", catalogue, inventories=found)
    expected = f"{ITEM}/{area}/{NAME}"
    changes = [
        c
        for a in result.to_mapping()["actions"]
        for c in a["physical_changes"]
        if c["name"] == NAME and c["object_kind"] == kind
    ]
    assert changes
    assert all(c["object_id"] == expected for c in changes)
    assert all(c["classification"] == "drop" for c in changes)
    assert all(c["effect"] == "remove" for c in changes)
