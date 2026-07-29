"""Local resolution is arithmetic: every location inspectable before any mutation."""

from __future__ import annotations

import pytest

from weaver import (
    DeltaTarget,
    FolderTarget,
    ItemRef,
    LocalWorkspace,
    LocalResolver,
    WarehouseTarget,
)
from weaver.errors import CommandError


@pytest.fixture
def resolver() -> LocalResolver:
    return LocalResolver(LocalWorkspace(workspace="/srv/.local", weaver_lakehouse="Weaver"))


def test_an_item_holds_files_and_tables(resolver):
    item = ItemRef("Sales")
    assert resolver.lakehouse(item).value == "/srv/.local/Sales"
    assert resolver.files_root(item).value == "/srv/.local/Sales/Files"
    assert resolver.tables_root(item).value == "/srv/.local/Sales/Tables"


def test_a_folder_target_may_carry_a_subpath(resolver):
    assert resolver.folder_root(FolderTarget.parse("Sales/Files")).value == (
        "/srv/.local/Sales/Files"
    )
    assert resolver.folder_root(FolderTarget.parse("Sales/Files/Extracts")).value == (
        "/srv/.local/Sales/Files/Extracts"
    )


def test_a_folder_object_materialises_beneath_the_configured_root(resolver):
    target = FolderTarget.parse("Sales/Files/Extracts")
    assert resolver.folder_object(target, "Budget", "BudgetPaper").value == (
        "/srv/.local/Sales/Files/Extracts/Budget/BudgetPaper"
    )


def test_staging_is_an_object_local_sibling(resolver):
    """There is no shared staging area — it sits beside its own destination."""
    target = FolderTarget.parse("Sales/Files")
    assert resolver.folder_staging(target, "Budget", "BudgetPaper").value == (
        "/srv/.local/Sales/Files/Budget/BudgetPaper_Staging"
    )


def test_a_delta_table_lands_under_tables(resolver):
    assert resolver.delta_table(DeltaTarget.parse("Sales"), "Budget", "Expense").value == (
        "/srv/.local/Sales/Tables/Budget/Expense"
    )


def test_a_local_destination_folds_the_lakehouse_into_the_database_name(resolver):
    """Local Spark has one namespace level, so the Lakehouse shares it.

    That is what keeps two destinations declaring the same schema apart. Fabric
    keeps them apart with a namespace of its own; here the name does it.
    """

    destination = resolver.spark_destination(ItemRef("Sales"))

    assert destination.qualify("Budget", "Expense") == "`sales__budget`.`Expense`"
    assert destination.qualified_schema("Budget") == "`sales__budget`"


def test_a_local_schema_pins_its_storage_under_the_lakehouse_tables_area(resolver):
    """Emulating what a schema-enabled Fabric Lakehouse does natively.

    The folding is in the *name* only: a managed table still lands at
    ``<lakehouse>/Tables/<schema>/<object>``, so the emulator keeps mirroring the
    OneLake layout every other part of Weaver resolves against.
    """

    destination = resolver.spark_destination(ItemRef("Sales"))

    assert destination.schema_location("Budget") == "/srv/.local/Sales/Tables/Budget"


def test_two_local_destinations_sharing_a_schema_name_stay_apart(resolver):
    """The defect this replaced: `IF NOT EXISTS` meant the first Lakehouse won."""

    first = resolver.spark_destination(ItemRef("Sales"))
    second = resolver.spark_destination(ItemRef("Inventory"))

    assert first.qualify("Budget", "Expense") != second.qualify("Budget", "Expense")
    assert first.schema_location("Budget") != second.schema_location("Budget")


def test_schema_and_object_are_separate_segments(resolver):
    """Never joined into one dotted directory name."""
    location = resolver.delta_table(DeltaTarget.parse("Sales"), "Budget", "Expense")
    assert location.value.endswith("/Budget/Expense")


def test_a_warehouse_fails_explicitly_rather_than_silently(resolver):
    with pytest.raises(CommandError, match="Fabric-only"):
        resolver.warehouse(WarehouseTarget.parse("Reporting"))


def test_weaver_items_live_directly_under_the_control_lakehouse(resolver):
    assert resolver.weaver_items_root.value == (
        "/srv/.local/Weaver/Files/weaver_items"
    )


def test_control_tables_live_under_the_weaver_lakehouse(resolver):
    assert resolver.control_tables_root.value == "/srv/.local/Weaver/Tables"


def test_the_weaver_lakehouse_is_just_another_item(resolver):
    assert resolver.weaver_lakehouse == resolver.lakehouse(ItemRef("Weaver"))


def test_a_workspace_without_a_weaver_lakehouse_says_so():
    resolver = LocalResolver(LocalWorkspace(workspace="/srv/.local"))
    with pytest.raises(CommandError, match="weaver_lakehouse"):
        resolver.weaver_items_root


def test_resolution_touches_nothing(tmp_path):
    """Locations are computed for paths that do not exist."""
    resolver = LocalResolver(LocalWorkspace(workspace=tmp_path, weaver_lakehouse="Weaver"))
    location = resolver.delta_table(DeltaTarget.parse("Sales"), "Budget", "Expense")
    assert not location.path.exists()
    assert list(tmp_path.iterdir()) == []


def test_a_fabric_workspace_is_refused():
    from weaver import FabricWorkspace

    with pytest.raises(CommandError, match="LocalWorkspace"):
        LocalResolver(FabricWorkspace(workspace="Analytics"))
