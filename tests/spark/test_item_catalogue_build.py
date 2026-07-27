"""The repository/item catalogue seam through the real local Spark installer."""

from __future__ import annotations

import pytest

from weaver import ItemRef, Location
from weaver.build_bundle import (
    InstallationEnvironment,
    ItemBinding,
    ItemBindings,
    LakehouseBinding,
    generate_item_build_bundle,
    install_bundle,
)
from weaver.catalogue.item_tables import CATALOGUE_TABLES, INSTALLATION, REGISTRY
from weaver.ses import read_weaver_repository
from weaver.ses.model import WeaverItemId

pytestmark = pytest.mark.spark


def _failures(report) -> str:
    return "\n".join(
        f"{action.action_id}: {action.error_type}: {action.error_message}"
        for sequence in report.sequences
        for action in sequence.actions
        if action.status == "failed"
    )


def test_builtin_item_is_built_and_published_by_one_item_bundle(
    lakehouses, spark, weaver_catalogue
):
    """No integration-only shortcut: discovery, planning and install are real."""

    repository_root = lakehouses.location("Estate")
    lakehouses.store.make_directory(repository_root)
    repository = read_weaver_repository(repository_root, store=lakehouses.store)

    logical = WeaverItemId.parse("Lakehouse/_weaver")
    control = LakehouseBinding(ItemRef(lakehouses.weaver.name))
    bundle = generate_item_build_bundle(
        repository,
        bindings=ItemBindings((ItemBinding(logical, control),)),
        output=lakehouses.location("bundle"),
        store=lakehouses.store,
        prune=False,
        catalogue=True,
        control_lakehouse=control,
    )
    report = install_bundle(
        bundle,
        environment=InstallationEnvironment(
            store=lakehouses.store,
            resolver=lakehouses.resolver,
            spark=spark,
        ),
    )

    assert report.status == "succeeded", _failures(report)
    assert {name.lower() for name in weaver_catalogue.tables("_")} == {
        table.name.lower() for table in CATALOGUE_TABLES
    }

    installation = spark.table(
        weaver_catalogue.qualify("_", INSTALLATION.name)
    ).select("repository", "item_type", "item_name", "target_name").collect()
    assert [tuple(row) for row in installation] == [
        ("Estate", "Lakehouse", "_weaver", lakehouses.weaver.name)
    ]

    registry = spark.table(weaver_catalogue.qualify("_", REGISTRY.name))
    assert registry.where(
        "repository = 'Estate' AND item_type = 'Lakehouse' "
        "AND item_name = '_weaver' AND object_namespace = 'Tables'"
    ).count() == len(CATALOGUE_TABLES)
