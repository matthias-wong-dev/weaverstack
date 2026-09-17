"""Incremental installation follows validation artefacts, not declarations."""

from __future__ import annotations

from pathlib import Path

from factories import (
    FixtureCatalogue,
    FixtureInventory,
    installed_catalogue,
    item_bindings,
    item_id,
)
from support.weaver_test import weaver_test
from support.workspaces import WORKSPACE

from weaver.build_bundle import WarehouseBinding, generate_item_build_bundle
from weaver.catalogue.builtin import BUILTIN_ITEM
from weaver.declaration import parse_item_repository
from weaver.locations import Location
from weaver.store import FilesystemStore
from weaver.targets import ItemRef

LAKEHOUSE = "Lakehouse/Sales"
WAREHOUSE = "Warehouse/Reporting"

SCHEMA = "Schema ID: Sales\nDescription: Sales objects.\n"

LAKEHOUSE_TABLE = '''"""
Table ID: Sales.Order
Description: Orders.
Lineage: A source system.
Primary key: Id
Schema:
  Id: string
"""
from weaver import Table

class Sales__Order(Table):
    def read(self):
        return [], []
'''

LAKEHOUSE_ASSUMPTION = """/*
Assumption ID: Sales.NoOrphans
Description: No orphan orders.
*/
select Id from Sales.Order where Id is null;
"""

WAREHOUSE_TABLE = """/*
Table ID: Sales.Report
Description: A report.
Lineage: A source system.
*/
select 1 as Id;
"""

WAREHOUSE_TEST = """/*
Test ID: Sales.Reconciles
Description: The report reconciles.
Primary key: Id
*/
select Id from Sales.Report;
select Id from Sales.Report;
"""


def _write(root: Path, relative: str, text: str) -> None:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8", newline="")


def _repository(root: Path):
    for relative, text in {
        f"{LAKEHOUSE}/schemas/Sales.yml": SCHEMA,
        f"{LAKEHOUSE}/Tables/Sales__Order.py": LAKEHOUSE_TABLE,
        f"{LAKEHOUSE}/assumptions/Sales.NoOrphans.sql": LAKEHOUSE_ASSUMPTION,
        f"{WAREHOUSE}/schemas/Sales.yml": SCHEMA,
        f"{WAREHOUSE}/Sales.Report.sql": WAREHOUSE_TABLE,
        f"{WAREHOUSE}/tests/Sales.Reconciles.sql": WAREHOUSE_TEST,
    }.items():
        _write(root, relative, text)
    return parse_item_repository(Location(str(root)))


def _bundle(
    repository,
    tmp_path: Path,
    *,
    logical: str,
    physical: str,
    installed_repository=None,
    catalogue=None,
):
    installed_repository = installed_repository or repository
    bindings = item_bindings((logical, physical))
    bound = bindings.entries[0].to_bound_target()
    catalogue_binding = WarehouseBinding(
        ItemRef("Weaver_Control"), workspace_name=WORKSPACE
    )
    inventories = {
        item_id(logical): FixtureInventory.from_repository(
            installed_repository,
            item=logical,
            target_id=bound.id,
            kind=bound.kind,
            target_name=bound.name,
        ),
        BUILTIN_ITEM: FixtureInventory.from_repository(
            installed_repository,
            item=BUILTIN_ITEM,
            target_id=catalogue_binding.to_bound_target().id,
            kind="warehouse",
            target_name="Weaver_Control",
        ),
    }
    return generate_item_build_bundle(
        repository,
        bindings=bindings,
        output=Location(str(tmp_path / physical)),
        store=FilesystemStore(),
        target_inventories=inventories,
        catalogue=catalogue or installed_catalogue(installed_repository, bindings),
        catalogue_binding=catalogue_binding,
    )


def _assert_no_install_selection(bundle) -> None:
    selection = bundle.plan.selection
    assert selection.impact.new == ()
    assert selection.impact.changed == ()
    assert selection.selected_for_build == ()
    assert bundle.plan.runtime_state_established == ()
    assert not [
        action
        for _sequence, _batch, action in bundle.plan.actions()
        if action.kind == "reconcile_runtime_state"
    ]


@weaver_test()
def test_an_unchanged_warehouse_validation_selects_no_install_or_state_reset(tmp_path):
    repository = _repository(tmp_path / "repo")

    bundle = _bundle(
        repository, tmp_path / "bundle", logical=WAREHOUSE, physical="Reporting_WH"
    )

    _assert_no_install_selection(bundle)


@weaver_test()
def test_an_unchanged_lakehouse_validation_selects_no_install_or_state_reset(tmp_path):
    repository = _repository(tmp_path / "repo")

    bundle = _bundle(
        repository, tmp_path / "bundle", logical=LAKEHOUSE, physical="Sales_LH"
    )

    _assert_no_install_selection(bundle)


def _test_state_rows(bundle):
    return [
        row
        for establishment in bundle.plan.runtime_state_established
        if establishment.table == "TestStatus"
        for row in establishment.rows
    ]


@weaver_test()
def test_a_changed_warehouse_validation_selects_its_procedure_and_resets_test_state(
    tmp_path,
):
    root = tmp_path / "repo"
    before = _repository(root)
    _write(
        root,
        f"{WAREHOUSE}/tests/Sales.Reconciles.sql",
        WAREHOUSE_TEST.replace(
            "The report reconciles.", "The report reconciles exactly."
        ),
    )
    after = parse_item_repository(Location(str(root)))

    bundle = _bundle(
        after,
        tmp_path / "bundle",
        logical=WAREHOUSE,
        physical="Reporting_WH",
        installed_repository=before,
    )

    assert [str(identity) for identity in bundle.plan.selection.selected_for_build] == [
        "Warehouse/Reporting/procedure:_/Test Sales.Reconciles"
    ]
    assert _test_state_rows(bundle) == [
        {
            "item_type": "Warehouse",
            "item_name": "Reporting",
            "schema_name": "Sales",
            "object_name": "Reconciles",
            "test_type": "test",
            "result": "pending",
        }
    ]


@weaver_test()
def test_a_changed_lakehouse_validation_selects_its_module_and_resets_test_state(
    tmp_path,
):
    root = tmp_path / "repo"
    before = _repository(root)
    _write(
        root,
        f"{LAKEHOUSE}/assumptions/Sales.NoOrphans.sql",
        LAKEHOUSE_ASSUMPTION.replace("No orphan orders.", "No orphan sales orders."),
    )
    after = parse_item_repository(Location(str(root)))

    bundle = _bundle(
        after,
        tmp_path / "bundle",
        logical=LAKEHOUSE,
        physical="Sales_LH",
        installed_repository=before,
    )

    assert [str(identity) for identity in bundle.plan.selection.selected_for_build] == [
        "Lakehouse/Sales/file:_/Load/assumptions/Sales__NoOrphans.py"
    ]
    assert _test_state_rows(bundle) == [
        {
            "item_type": "Lakehouse",
            "item_name": "Sales",
            "schema_name": "Sales",
            "object_name": "NoOrphans",
            "test_type": "assumption",
            "result": "pending",
        }
    ]


@weaver_test()
def test_a_validation_implementation_change_selects_the_generated_artefact(
    tmp_path, monkeypatch
):
    from weaver.declaration import validation

    root = tmp_path / "repo"
    before = _repository(root)
    monkeypatch.setattr(
        validation, "TSQL_VALIDATION_VERSION", validation.TSQL_VALIDATION_VERSION + 1
    )
    after = parse_item_repository(Location(str(root)))

    bundle = _bundle(
        after,
        tmp_path / "bundle",
        logical=WAREHOUSE,
        physical="Reporting_WH",
        installed_repository=before,
    )
    selected = bundle.plan.selection.selected_for_build

    assert [str(identity) for identity in selected] == [
        "Warehouse/Reporting/procedure:_/Test Sales.Reconciles"
    ]
    artefact = next(
        artefact
        for artefact in after.programmables.values()
        if artefact.origin is not None
        and str(artefact.origin) == "Warehouse/Reporting/Sales.Reconciles"
    )
    assert artefact.implementation_version == validation.TSQL_VALIDATION_VERSION


@weaver_test()
def test_a_lakehouse_validation_implementation_change_selects_and_resets(
    tmp_path, monkeypatch
):
    from weaver.declaration import validation
    from weaver.etl import ROLE_ASSUMPTION, item_validation_artefacts

    root = tmp_path / "repo"
    before = _repository(root)
    bindings = item_bindings((LAKEHOUSE, "Sales_LH"))
    catalogue = installed_catalogue(before, bindings)
    monkeypatch.setattr(
        validation, "SPARK_VALIDATION_VERSION", validation.SPARK_VALIDATION_VERSION + 1
    )
    after = parse_item_repository(Location(str(root)))

    bundle = _bundle(
        after,
        tmp_path / "bundle",
        logical=LAKEHOUSE,
        physical="Sales_LH",
        installed_repository=before,
        catalogue=catalogue,
    )

    assert [str(identity) for identity in bundle.plan.selection.selected_for_build] == [
        "Lakehouse/Sales/file:_/Load/assumptions/Sales__NoOrphans.py"
    ]
    artefact = next(
        artefact
        for artefact in item_validation_artefacts(after, item=item_id(LAKEHOUSE))
        if artefact.role == ROLE_ASSUMPTION
    )
    assert artefact.implementation_version == validation.SPARK_VALIDATION_VERSION
    assert _test_state_rows(bundle)[0]["object_name"] == "NoOrphans"


@weaver_test()
def test_a_registered_validation_missing_physically_is_selected_as_new(tmp_path):
    from dataclasses import replace

    from weaver.build_bundle.incremental import select_build
    from weaver.build_bundle.planner import installable_identities
    from weaver.build_bundle.targets import LakehouseBinding

    repository = _repository(tmp_path / "repo")
    item = item_id(LAKEHOUSE)
    binding = LakehouseBinding(ItemRef("Sales_LH"), workspace_name=WORKSPACE)
    inventory = FixtureInventory.from_repository(repository, item=item)
    missing = "_/Load/assumptions/Sales__NoOrphans.py"
    inventory = replace(
        inventory, files=tuple(path for path in inventory.files if path != missing)
    )
    expected = "Lakehouse/Sales/file:_/Load/assumptions/Sales__NoOrphans.py"
    artefact_identity = next(
        identity
        for identity in installable_identities(repository, {item: binding})
        if str(identity) == expected
    )
    selection = select_build(
        repository,
        FixtureCatalogue.from_repository(repository, item=item).registered,
        selected={artefact_identity},
        inventories={item: inventory},
    )

    assert [str(identity) for identity in selection.impact.new] == [expected]
    assert [str(identity) for identity in selection.selected_for_build] == [expected]


@weaver_test()
def test_a_direct_artefact_at_implementation_version_one_is_stable(tmp_path):
    first = _repository(tmp_path / "one")
    second = _repository(tmp_path / "two")
    identity = next(
        identity
        for identity in first.source_documents
        if str(identity) == "Warehouse/Reporting/Sales.Report"
    )

    assert first.source_documents[identity].implementation_version == 1
    assert (
        first.source_documents[identity].physical_signature
        == second.source_documents[identity].physical_signature
    )


@weaver_test()
def test_direct_signature_upgrade_converges_after_one_selection(tmp_path):
    from dataclasses import replace

    from weaver.build_bundle.incremental import select_build
    from weaver.build_bundle.planner import installable_identities
    from weaver.build_bundle.targets import WarehouseBinding as ItemWarehouseBinding

    repository = _repository(tmp_path / "repo")
    item = item_id(WAREHOUSE)
    binding = ItemWarehouseBinding(ItemRef("Reporting_WH"), workspace_name=WORKSPACE)
    installed = FixtureCatalogue.from_repository(repository, item=item)
    identity = next(
        identity
        for identity in repository.source_documents
        if str(identity) == "Warehouse/Reporting/Sales.Report"
    )
    assert identity in installable_identities(repository, {item: binding})
    selected = {identity}
    previous = dict(installed.registered)
    previous[identity] = replace(
        previous[identity],
        signature=repository.source_documents[identity].effective_signature,
    )
    inventories = {
        item: FixtureInventory.from_repository(
            repository, item=item, kind="warehouse", target_name="Reporting_WH"
        )
    }

    upgraded = select_build(
        repository, previous, selected=selected, inventories=inventories
    )
    settled = select_build(
        repository, installed.registered, selected=selected, inventories=inventories
    )

    assert identity in upgraded.impact.changed
    assert identity in upgraded.selected_for_build
    assert settled.impact.changed == ()
    assert settled.selected_for_build == ()
