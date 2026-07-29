"""The minimal public surface at checkpoint 0."""

from __future__ import annotations

import weaver
from weaver.errors import CommandError, WeaverError


def test_version_is_exposed():
    assert weaver.__version__


def test_error_hierarchy_has_one_root():
    assert issubclass(CommandError, WeaverError)
    assert issubclass(WeaverError, Exception)


def test_item_repository_and_build_are_the_primary_public_surface():
    expected = {
        "WeaverRepository",
        "WeaverItem",
        "WeaverDocument",
        "WeaverItemId",
        "WeaverDocumentId",
        "parse_item_repository",
        "ItemBinding",
        "ItemBindings",
        "LakehouseBinding",
        "WarehouseBinding",
        "parse_item_binding",
        "generate_item_build_bundle",
        "InstallationEnvironment",
        "install_bundle",
    }
    assert expected <= set(weaver.__all__)
    assert {
        "FolderTarget",
        "DeltaTarget",
        "wipe_folder_target",
        "wipe_delta_target",
    }.isdisjoint(weaver.__all__)
    assert {"InitialiseResult", "initialise_weaver_lakehouse"} <= set(weaver.__all__)


def test_public_binding_parser_separates_logical_and_physical_identity():
    lakehouse = weaver.parse_item_binding(
        "Lakehouses/Curated_Dev=Lakehouse/Curated"
    )
    warehouse = weaver.parse_item_binding(
        "Warehouses/Reporting_Dev=Warehouse/Reporting"
    )

    assert str(lakehouse.item) == "Lakehouse/Curated"
    assert lakehouse.target.lakehouse.name == "Curated_Dev"
    assert str(warehouse.item) == "Warehouse/Reporting"
    assert warehouse.target.warehouse.name == "Reporting_Dev"
