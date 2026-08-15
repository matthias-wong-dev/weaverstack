"""The load layer as a stage: where it sits, what it installs, what it removes.

`test_load_artefacts_representation.py` asks what a repository owns. This asks what a build
*does* with it — the ordering claim that an item's runtime code goes down after
its structure is built, the frozen actions and payloads that carry it, and the
removals that come from the catalogue rather than from a scan.

Every input is constructed directly, so a claim about barrier order is not paid
for with an installation.
"""

from __future__ import annotations

import pytest
from factories import (
    ITEM,
    WAREHOUSE_ITEM,
    bound_target,
    document_id,
    item_id,
    lakehouse_table,
    registered_document,
    schema_document,
    single_document_repository,
    target_inventory,
    warehouse_table,
)

from weaver.build_bundle import plan_item_build
from weaver.declaration import parse_item_repository
from weaver.etl import LOAD_ROOT, item_load_artefacts
from weaver.locations import Location

CUSTOMER = "DWG.Customer"


def _write(root, relative: str, text: str) -> None:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


@pytest.fixture
def lakehouse(tmp_path):
    return single_document_repository(
        tmp_path / "repo",
        documents={"DWG__Customer.py": lakehouse_table(CUSTOMER)},
    )


@pytest.fixture
def warehouse(tmp_path):
    root = tmp_path / "repo"
    _write(root, f"{WAREHOUSE_ITEM}/schemas/Sales.yml", schema_document("Sales"))
    _write(
        root, f"{WAREHOUSE_ITEM}/Sales.Customer.sql", warehouse_table("Sales.Customer")
    )
    return parse_item_repository(Location(str(root)))


def plan(repository, *, item=None, target=None, **overrides):
    """Plan one item, with nothing selected unless a test says otherwise."""

    item = item or item_id()
    target = target or bound_target()
    arguments = {
        "selected_documents": set(),
        "selected_aliases": set(),
        "selected_for_drop": set(),
        "selected_for_build": set(),
        "selected_loads": set(),
        "removed": set(),
        "registered": {},
        "inventory": target_inventory(),
    }
    arguments.update(overrides)
    return plan_item_build(
        repository, item=item, target=target, target_by_item={item: target}, **arguments
    )


def loads_of(repository, item=None):
    return {a.identity for a in item_load_artefacts(repository, item=item or item_id())}


def phases(planned) -> list[str]:
    return [stage.phase for stage in planned.stages]


def actions_of(planned, phase: str):
    return [
        action
        for stage in planned.stages
        if stage.phase == phase
        for batch in stage.batches
        for action in batch.actions
    ]


# --- where the layer sits -----------------------------------------------------


def test_load_is_the_last_thing_an_item_does(lakehouse):
    """The ordering claim, and the only one the layer makes.

    Its artefacts depend on the item's structural work being finished, which is
    expressed by the layer being last rather than by any edge — and nothing
    within it is ordered against anything else, because nothing here runs.
    """

    selected = {key for key in lakehouse.source_documents if key.item == item_id()}
    planned = plan(
        lakehouse,
        selected_documents=selected,
        selected_for_build=selected,
        selected_loads=loads_of(lakehouse),
    )

    assert phases(planned)[-1] == "load"
    assert phases(planned).count("load") == 1


def test_an_item_with_no_selected_load_work_gets_no_layer(lakehouse):
    """A phase with nothing to do is not a barrier, and takes no number."""

    selected = {key for key in lakehouse.source_documents if key.item == item_id()}
    planned = plan(
        lakehouse, selected_documents=selected, selected_for_build=selected
    )

    assert "load" not in phases(planned)


# --- what it installs ---------------------------------------------------------


def test_each_artefact_becomes_one_action_carrying_its_own_bytes(lakehouse):
    """The bundle carries the content, so the installer never reopens source."""

    planned = plan(lakehouse, selected_loads=loads_of(lakehouse))
    actions = actions_of(planned, "load")
    payloads = {
        name: data for stage in planned.stages for name, data in stage.payloads.items()
    }

    assert [action.kind for action in actions] == ["write_file"]
    action = actions[0]
    assert action.executor == "load_file"
    assert action.resource_node_id == f"{ITEM}/file:{LOAD_ROOT}/DWG__Customer.py"
    assert b"class DWG__Customer" in payloads[action.payload]


def test_a_generated_procedure_is_ordinary_t_sql(warehouse):
    """It needs no executor of its own: a create-or-alter is a script, and the
    T-SQL executor runs the script it is given without knowing what it builds."""

    item = item_id(WAREHOUSE_ITEM)
    target = bound_target(
        id="target-1", kind="warehouse", item_id="Reporting_WH",
        logical_item_name="Reporting", logical_item_type="Warehouse",
    )
    planned = plan(
        warehouse,
        item=item,
        target=target,
        selected_loads=loads_of(warehouse, item),
    )
    actions = actions_of(planned, "load")

    assert [action.kind for action in actions] == ["build_procedure"]
    assert actions[0].executor == "tsql"
    assert actions[0].resource_node_id.endswith("procedure:_/Load Sales.Customer")


def test_the_procedure_schema_is_created_in_the_ordinary_schema_phase(warehouse):
    """`_` is a managed schema, not a reserved word.

    No document declares an object in it, so it would never be created if only
    documents were consulted — and it is derived from the artefacts, so an item
    with no procedures asks for no schema and the ordinary schema prune can take
    one that is left behind.
    """

    item = item_id(WAREHOUSE_ITEM)
    target = bound_target(
        id="target-1", kind="warehouse", item_id="Reporting_WH",
        logical_item_name="Reporting", logical_item_type="Warehouse",
    )
    planned = plan(
        warehouse,
        item=item,
        target=target,
        selected_loads=loads_of(warehouse, item),
    )

    created = {
        stage.payloads[action.payload].decode()
        for action in actions_of(planned, "schema")
        for stage in planned.stages
        if stage.phase == "schema" and action.payload in stage.payloads
    }
    assert any("create schema [_]" in statement for statement in created)


# --- what it removes ----------------------------------------------------------


def test_a_source_that_stopped_claiming_its_file_removes_it(lakehouse):
    """Driven by the previous Registry row, because the inventory diff cannot
    reach individual files inside the runtime tree."""

    gone = document_id(f"{ITEM}/file:{LOAD_ROOT}/lib/retired.py")
    planned = plan(
        lakehouse,
        removed={gone},
        registered={gone: registered_document(gone, object_type="file")},
    )
    actions = actions_of(planned, "load")

    assert [action.kind for action in actions] == ["delete_file"]
    assert actions[0].resource_node_id == str(gone)
    assert actions[0].payload is None


def test_a_removed_procedure_is_dropped_by_name(warehouse):
    gone = document_id(f"{WAREHOUSE_ITEM}/procedure:_/Load Sales.Retired")
    item = item_id(WAREHOUSE_ITEM)
    target = bound_target(
        id="target-1", kind="warehouse", item_id="Reporting_WH",
        logical_item_name="Reporting", logical_item_type="Warehouse",
    )
    planned = plan(
        warehouse,
        item=item,
        target=target,
        removed={gone},
        registered={gone: registered_document(gone, object_type="stored_procedure")},
    )
    actions = actions_of(planned, "load")
    statement = next(
        stage.payloads[actions[0].payload]
        for stage in planned.stages
        if actions[0].payload in stage.payloads
    ).decode()

    assert [action.kind for action in actions] == ["drop_procedure"]
    assert statement == "drop procedure if exists [_].[Load Sales.Retired];\n"


def test_a_removed_table_is_not_mistaken_for_a_load_artefact(lakehouse):
    """Scoped by what the Registry says each object *is*.

    A removed table is removed by the inventory prune, which can see it. Reading
    the removals by identity shape rather than by installed type would have this
    layer issue a `drop procedure` for a dropped table.
    """

    gone = document_id("DWG.Retired")
    planned = plan(
        lakehouse,
        removed={gone},
        registered={gone: registered_document(gone, object_type="table")},
    )

    assert "load" not in phases(planned)
