"""Canonical facts cover grouped destinations, deferred shape and publication."""

from dataclasses import replace

import pytest
from factories import full_estate
from support.weaver_test import weaver_test
from test_build_fixed_point_cycle import _bindings, _inventories, installed_catalogue
from test_build_preview_representation import preview

from weaver.catalogue.state import Catalogue
from weaver.declaration import parse_item_repository
from weaver.declaration.model import WeaverItemId
from weaver.locations import Location


def inventories(repository):
    return _inventories(
        repository, {b.item: b.to_bound_target() for b in _bindings().entries}
    )


@weaver_test()
def test_grouped_shortcut_action_describes_every_create_and_refresh(tmp_path):
    repository = full_estate(tmp_path / "repo")
    found = inventories(repository)
    item = WeaverItemId("Warehouse", "Reporting")
    found[item] = replace(
        found[item], views=tuple(v for v in found[item].views if v != "_.TestStatus")
    )
    result = preview(repository, tmp_path / "plan", Catalogue({}), inventories=found)
    mapping = result.to_mapping()
    groups = [a for a in mapping["actions"] if len(a["physical_changes"]) > 1]
    assert groups
    canonical = mapping["plan"]["build_envelope"]["target_changes"]
    for group in groups:
        expected = [
            c
            for values in canonical.values()
            for c in values
            if c["action_id"] == group["id"]
        ]
        assert len(group["physical_changes"]) == len(expected)
        assert {c["name"] for c in group["physical_changes"]} == {
            c["name"] for c in expected
        }
        assert all(
            c["object_id"]
            for c in group["physical_changes"]
            if c["object_kind"] != "schema"
        ), group
        assert all(c["name"] in result.describe() for c in group["physical_changes"])
    warehouse = next(
        a
        for a in groups
        if a["target_id"] == _bindings().by_item[item].to_bound_target().id
        and any(c["name"] == "_.TestStatus" for c in a["physical_changes"])
    )
    changes = {c["name"]: c["classification"] for c in warehouse["physical_changes"]}
    assert changes["_.TestStatus"] == "create"
    assert "refresh" in changes.values()
    assert warehouse["classification"] == "mixed"
    assert not warehouse["destructive"]


@weaver_test()
def test_spark_sql_table_reports_deferred_shape_and_setup_without_running_it(tmp_path):
    root = tmp_path / "repo"
    full_estate(root)
    path = root / "Lakehouse/Sales/Tables/DWG.Summary.sql"
    path.write_text(
        path.read_text()
        .replace("Schema:\n  CustomerId: string\n", "")
        .replace(
            "select cast(null as string)",
            "create or replace temp view Source as select 1 as N;\nselect cast(null as string)",
        )
    )
    repository = parse_item_repository(Location(str(root)))
    result = preview(repository, tmp_path / "plan", Catalogue({}))
    actions = {a["resource_node_id"]: a for a in result.to_mapping()["actions"]}
    sql = actions["Lakehouse/Sales/Tables/DWG.Summary"]
    assert sql["table_shape"]["schema_mode"] == "inferred"
    assert sql["table_shape"]["declared_columns"] is None
    assert sql["table_shape"]["query_shape_deferred"]
    assert sql["table_shape"]["authored_setup_deferred"]
    assert (
        "columns and types" in sql["uncertainty"]
        and "setup effects" in sql["uncertainty"]
    )
    assert sql["uncertainty"] in result.describe()
    python = actions["Lakehouse/Sales/Tables/DWG.Customer"]
    assert python["table_shape"]["schema_mode"] == "declared"
    assert python["table_shape"]["declared_columns"]
    assert not python["table_shape"]["query_shape_deferred"]
    assert python["uncertainty"] is None


@pytest.mark.parametrize("item", ["Lakehouse/Sales", "Warehouse/Reporting"])
@pytest.mark.parametrize(
    "kind,directory", [("Test", "tests"), ("Assumption", "assumptions")]
)
@pytest.mark.parametrize("change", ["new", "changed", "removed"])
@weaver_test()
def test_logical_validation_definitions_follow_publication(
    tmp_path, item, kind, directory, change
):
    root = tmp_path / "repo"
    original = full_estate(root)
    path = root / item / directory / "Checks.RowCount.sql"
    path.parent.mkdir(parents=True)
    text = f"/*\n{kind} ID: Checks.RowCount\nDescription: Count rows.\n"
    if kind == "Test":
        text += "Primary key: Id\n"
    text += "*/\nselect 1 as Id;\n"
    if kind == "Test":
        text += "select 1 as Id;\n"
    path.write_text(text)
    before = parse_item_repository(Location(str(root))) if change != "new" else original
    catalogue = installed_catalogue(before)
    found = inventories(before)
    if change == "changed":
        path.write_text(text.replace("Count rows.", "Count selected rows."))
    elif change == "removed":
        path.unlink()
    repository = parse_item_repository(Location(str(root)))
    result = preview(repository, tmp_path / "plan", catalogue, inventories=found)
    mapping = result.to_mapping()
    identity = f"{item}/Checks.RowCount"
    obj = next(o for o in mapping["objects"] if o["identity"] == identity)
    assert obj["classification"] == change
    assert not obj["selected_for_build"] and not obj["selected_for_drop"]
    when = "remove" if change == "removed" else "publish"
    assert identity in {r["object_id"] for r in mapping["validation_definitions"][when]}
    assert f"Validation definition {when}: {identity}" in result.describe()
    if change != "removed":
        assert obj["validation_artefacts_selected"]


@pytest.mark.parametrize("certified", [False, True])
@weaver_test()
def test_retained_protected_object_has_actual_registry_publication_without_withdrawal(
    tmp_path, certified
):
    root = tmp_path / "repo"
    before = full_estate(root)
    catalogue = installed_catalogue(before) if certified else Catalogue({})
    path = root / "Lakehouse/Sales/Tables/DWG__Customer.py"
    path.write_text(
        path.read_text().replace(
            "Primary key: CustomerId", "Primary key: CustomerId\nProhibit rebuild: true"
        )
    )
    repository = parse_item_repository(Location(str(root)))
    result = preview(
        repository, tmp_path / "plan", catalogue, inventories=inventories(before)
    )
    mapping = result.to_mapping()
    identity = "Lakehouse/Sales/Tables/DWG.Customer"
    obj = next(o for o in mapping["objects"] if o["identity"] == identity)
    assert obj["classification"] == "prohibited"
    assert not obj["selected_for_build"] and not obj["selected_for_drop"]
    assert identity not in mapping["certification"]["withdraw_before_work"]
    assert identity in mapping["certification"]["publish_after_success"]
    assert f"Certification publish_after_success: {identity}" in result.describe()
    assert not [a for a in mapping["actions"] if a["resource_node_id"] == identity]
    assert not [
        row
        for state in mapping["runtime_state_established"]
        for row in state["rows"]
        if row.get("object_name") == "Customer" and row.get("item_type") == "Lakehouse"
    ]
