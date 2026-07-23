"""Local resolution is arithmetic: every location inspectable before any mutation."""

from __future__ import annotations

import pytest

from weaver import (
    DeltaTarget,
    FolderTarget,
    ItemRef,
    LocalHost,
    LocalResolver,
    RepositoryRef,
    WarehouseTarget,
)
from weaver.errors import CommandError


@pytest.fixture
def resolver() -> LocalResolver:
    return LocalResolver(LocalHost(root="/srv/.local", weaver_lakehouse="Weaver"))


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


def test_schema_and_object_are_separate_segments(resolver):
    """Never joined into one dotted directory name."""
    location = resolver.delta_table(DeltaTarget.parse("Sales"), "Budget", "Expense")
    assert location.value.endswith("/Budget/Expense")


def test_a_warehouse_fails_explicitly_rather_than_silently(resolver):
    with pytest.raises(CommandError, match="Fabric-only"):
        resolver.warehouse(WarehouseTarget.parse("Reporting"))


def test_repositories_live_under_the_weaver_lakehouse(resolver):
    assert resolver.repos_root.value == "/srv/.local/Weaver/Files/repos"
    assert resolver.repository(RepositoryRef("sales-etl")).value == (
        "/srv/.local/Weaver/Files/repos/sales-etl"
    )


def test_control_tables_live_under_the_weaver_lakehouse(resolver):
    assert resolver.control_tables_root.value == "/srv/.local/Weaver/Tables"


def test_the_weaver_lakehouse_is_just_another_item(resolver):
    assert resolver.weaver_lakehouse == resolver.lakehouse(ItemRef("Weaver"))


def test_a_host_without_a_weaver_lakehouse_says_so():
    resolver = LocalResolver(LocalHost(root="/srv/.local"))
    with pytest.raises(CommandError, match="weaver_lakehouse"):
        resolver.repos_root


def test_resolution_touches_nothing(tmp_path):
    """Locations are computed for paths that do not exist."""
    resolver = LocalResolver(LocalHost(root=tmp_path, weaver_lakehouse="Weaver"))
    location = resolver.delta_table(DeltaTarget.parse("Sales"), "Budget", "Expense")
    assert not location.path.exists()
    assert list(tmp_path.iterdir()) == []


def test_a_fabric_host_is_refused():
    from weaver import FabricHost

    with pytest.raises(CommandError, match="LocalHost"):
        LocalResolver(FabricHost(workspace="Analytics"))
