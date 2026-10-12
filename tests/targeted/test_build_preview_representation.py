"""Preview facts come from the actual planner, including its fixed point."""

from dataclasses import replace

import pytest
from factories import full_estate
from support.weaver_test import weaver_test
from test_build_fixed_point_cycle import (
    PRESENTED,
    _bindings,
    _inventories,
    build,
    installed_catalogue,
)

from weaver.build_bundle import BuildState, WarehouseBinding, generate_item_build_bundle
from weaver.build_bundle.preview import preview_build
from weaver.catalogue.state import Catalogue
from weaver.declaration import parse_item_repository
from weaver.locations import Location
from weaver.store import FilesystemStore
from weaver.targets import ItemRef


def preview(repository, path, catalogue, *, inventories=None):
    bindings = _bindings()
    bound = {b.item: b.to_bound_target() for b in bindings.entries}
    if inventories:
        bundle = generate_item_build_bundle(
            repository,
            bindings=bindings,
            output=Location(str(path / "bundle")),
            store=FilesystemStore(),
            catalogue=catalogue,
            target_inventories=inventories,
            catalogue_binding=WarehouseBinding(
                ItemRef("Weaver_Control"), workspace_name="Demo"
            ),
        )
    else:
        bundle = build(repository, path, catalogue=catalogue)
    state = BuildState(
        catalogue=catalogue,
        target_inventories=inventories or _inventories(repository, bound),
    )
    return preview_build(
        bundle.plan, repository=repository, bindings=bindings, state=state
    )


@weaver_test()
def test_preview_identifies_a_whole_plan_no_op(tmp_path):
    repository = full_estate(tmp_path / "repo")
    result = preview(repository, tmp_path / "plan", installed_catalogue(repository))
    mapping = result.to_mapping()
    assert mapping["no_op"] and not mapping["destructive"]
    assert not mapping["actions"]
    assert {o["classification"] for o in mapping["objects"]} == {"unchanged"}
    assert mapping["certification"] == {
        "withdraw_before_work": [],
        "publish_after_success": [],
        "remove_on_publication": [],
    }
    assert "No changes" in result.describe()


@weaver_test()
def test_new_estate_preview_names_creates_bindings_and_runtime_state(tmp_path):
    repository = full_estate(tmp_path / "repo")
    bindings = _bindings()
    inventories = _inventories(
        repository, {b.item: b.to_bound_target() for b in bindings.entries}
    )
    inventories = {
        i: replace(
            v,
            schemas=(),
            folder_schemas=(),
            tables=(),
            views=(),
            folders=(),
            files=(),
            procedures=(),
        )
        for i, v in inventories.items()
    }
    result = preview(
        repository, tmp_path / "plan", Catalogue({}), inventories=inventories
    )
    mapping = result.to_mapping()
    assert not mapping["no_op"]
    assert (
        next(
            o
            for o in mapping["objects"]
            if o["identity"] == "Lakehouse/Sales/Tables/DWG.Customer"
        )["classification"]
        == "new"
    )
    creates = [a for a in mapping["actions"] if a["classification"] == "create"]
    assert creates and all(
        c["effect"] == "add" for a in creates for c in a["physical_changes"]
    )
    assert not mapping["certification"]["withdraw_before_work"]
    assert mapping["certification"]["publish_after_success"]
    assert mapping["runtime_state_established"]
    assert "Sales_LH" in result.describe() and "Reporting_WH" in result.describe()
    assert mapping["execution_runtime_checked"] is False


@weaver_test()
def test_edited_estate_preview_exposes_replacement_and_status_reset(tmp_path):
    repository = full_estate(tmp_path / "repo")
    catalogue = installed_catalogue(repository)
    # An installed signature mismatch exercises production impact selection.
    rows = {item: dict(tables) for item, tables in catalogue.rows.items()}
    for tables in rows.values():
        tables["Registry"] = tuple(
            dict(row, signature="previous") for row in tables["Registry"]
        )
    result = preview(repository, tmp_path / "plan", Catalogue(rows))
    mapping = result.to_mapping()
    assert any(o["classification"] == "changed" for o in mapping["objects"])
    assert any(a["classification"] == "replace" for a in mapping["actions"])
    assert any(
        a["classification"] == "drop" and a["destructive"] for a in mapping["actions"]
    )
    assert mapping["destructive"] and mapping["runtime_state_established"]
    assert "[destructive]" in result.describe()


@weaver_test()
def test_removed_object_preview_names_registry_claim_and_drop(tmp_path):
    repository = full_estate(tmp_path / "repo")
    catalogue = installed_catalogue(repository)
    bindings = _bindings()
    inventories = _inventories(
        repository, {b.item: b.to_bound_target() for b in bindings.entries}
    )
    inventories = {
        i: replace(v, runtime_references=PRESENTED) if v.kind == "lakehouse" else v
        for i, v in inventories.items()
    }
    identity = "Lakehouse/Sales/Tables/DWG.Summary"
    (tmp_path / "repo/Lakehouse/Sales/Tables/DWG.Summary.sql").unlink()
    repository = parse_item_repository(Location(str(tmp_path / "repo")))
    result = preview(repository, tmp_path / "plan", catalogue, inventories=inventories)
    mapping = result.to_mapping()
    removed = next(o for o in mapping["objects"] if o["identity"] == str(identity))
    assert removed["classification"] == "removed"
    assert str(identity) in mapping["certification"]["withdraw_before_work"]
    assert any(a["destructive"] for a in mapping["actions"])


@pytest.mark.parametrize("field", ["objects", "actions", "runtime_state_established"])
@weaver_test()
def test_preview_mapping_is_an_owned_copy(tmp_path, field):
    repository = full_estate(tmp_path / "repo")
    result = preview(repository, tmp_path / "plan", Catalogue({}))
    first = result.to_mapping()
    expected = result.to_mapping()
    first[field].clear()
    assert result.to_mapping() == expected
