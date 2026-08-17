"""Warehouse inventory reads bind a physical estate to Weaver's model."""

from __future__ import annotations

from factories import FixtureInventory, item_id
from support.weaver_test import weaver_test

from weaver.build_bundle.physical import item_prune_stage
from weaver.build_bundle.prune import read_warehouse_inventory
from weaver.declaration.metadata import SQL_TARGET


def _folded(names):
    return {name.casefold() for name in names}


@weaver_test(remote=True, resources={"tds"})
def test_a_built_warehouse_reads_back_as_the_fixture_predicts(
    warehouse_primitive_estate,
):
    actual = read_warehouse_inventory(
        warehouse_primitive_estate.target.bound,
        sql=warehouse_primitive_estate.warehouse.executor,
    )
    predicted = FixtureInventory.from_repository(
        warehouse_primitive_estate.repository,
        item="Warehouse/Reporting",
        target_kind=SQL_TARGET,
        target_id="target-1",
        kind="warehouse",
    )

    assert _folded(actual.tables) == _folded(predicted.tables)
    assert _folded(actual.views) == _folded(predicted.views)
    assert _folded(actual.schemas) == _folded(predicted.schemas)


@weaver_test(remote=True, resources={"tds"})
def test_an_unmanaged_object_is_seen_and_would_be_pruned(
    warehouse_primitive_estate,
):
    executor = warehouse_primitive_estate.warehouse.executor
    executor.execute_script("create table [DWG].[OldTable] ([x] int not null);")
    try:
        actual = read_warehouse_inventory(
            warehouse_primitive_estate.target.bound,
            sql=executor,
        )
        assert "dwg.oldtable" in _folded(actual.tables)

        stage = item_prune_stage(
            warehouse_primitive_estate.repository,
            set(warehouse_primitive_estate.repository.source_documents),
            item=item_id("Warehouse/Reporting"),
            target=warehouse_primitive_estate.target.bound,
            inventory=actual,
        )
        assert stage is not None
        assert any(
            "OldTable" in action.id
            for batch in stage.batches
            for action in batch.actions
        )
    finally:
        executor.execute_script("drop table if exists [DWG].[OldTable];")


@weaver_test(remote=True, resources={"tds"})
def test_prune_against_a_freshly_built_warehouse_finds_nothing(
    warehouse_primitive_estate,
):
    stage = item_prune_stage(
        warehouse_primitive_estate.repository,
        set(warehouse_primitive_estate.repository.source_documents),
        item=item_id("Warehouse/Reporting"),
        target=warehouse_primitive_estate.target.bound,
        inventory=read_warehouse_inventory(
            warehouse_primitive_estate.target.bound,
            sql=warehouse_primitive_estate.warehouse.executor,
        ),
    )

    assert stage is None
