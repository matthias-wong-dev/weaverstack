"""What a build removes, and what it spares — in pure Python.

Prune is a set difference: the desired physical state of an item, against the
physical state actually there. Both sides can be built directly, so none of this
needs a Lakehouse. Previously it did — a test asserting "a declared table is
spared" had to stand up a target, build into it, and read the result back, which
meant a prune defect and a build defect failed the same test.

Three seams, tested separately because they fail for different reasons:

``managed_sets``            what the item wants to exist
``render_inventory_prune``  desired plus actual, into removal actions
``item_prune_stage``        item scoping, target binding, action packaging

Prune is the destructive direction, so most of what is asserted here is what it
*does not* remove.
"""

from __future__ import annotations

import pytest
from factories import (
    ITEM,
    FixtureInventory,
    bound_target,
    folder_document,
    item_id,
    lakehouse_table,
    single_document_repository,
    spark_view,
    target_inventory,
)

from weaver.build_bundle.physical import item_prune_stage
from weaver.build_bundle.prune import managed_sets
from weaver.declaration.metadata import DELTA_TARGET, SQL_TARGET


@pytest.fixture
def estate(tmp_path):
    """A table, a view over it, and a folder — one of each physical form."""

    return single_document_repository(
        tmp_path,
        schemas=("DWG", "Raw"),
        documents={
            "DWG__Customer.py": lakehouse_table("DWG.Customer"),
            "DWG.ActiveCustomer.sql": spark_view(
                "DWG.ActiveCustomer", depends_on="DWG.Customer"
            ),
            "Files/Raw__CustomerCsv.py": folder_document("Raw.CustomerCsv"),
        },
    )


def documents_of(repository, item=ITEM):
    identity = item_id(item) if isinstance(item, str) else item
    return {
        str(key): value
        for key, value in repository.source_documents.items()
        if key.item == identity
    }


def prune_kinds(stage) -> list[str]:
    if stage is None:
        return []
    return [action.kind for batch in stage.batches for action in batch.actions]


def prune_targets(stage) -> set[str]:
    """What each prune action is about, read from its id.

    Not from ``resource_node_id``, which is ``None`` on every prune action — a
    pruned object has no node in the repository, which is precisely why it is
    being pruned. The identity lives in the action id and the payload.
    """

    if stage is None:
        return set()
    return {action.id for batch in stage.batches for action in batch.actions}


# --- desired state ------------------------------------------------------------


def test_the_keep_set_is_what_the_item_declares(estate):
    """Each declaration lands in the set matching its physical form."""

    managed = managed_sets(documents_of(estate), DELTA_TARGET)

    # Folded, because this set exists to be compared against whatever case the
    # target reports — Fabric lowercases a managed table's directory, the local
    # metastore does the same, and neither promises the declared spelling.
    assert managed.tables == frozenset({"dwg.customer"})
    assert managed.views == frozenset({"dwg.activecustomer"})
    # `_.load` is the generated folder that owns the item's deployed runtime
    # tree. It is declared like any other folder, so it is spared like one — and
    # an item with no load code never declares it, which is how the tree is
    # eventually removed.
    assert managed.folders == frozenset({"raw.customercsv", "_.load"})


def test_the_keep_set_is_per_physical_side(estate):
    """Asking for the Warehouse side of a Lakehouse item keeps no objects.

    One item is diffed against one target, and a document belongs to one
    physical side. Were this to leak, a Warehouse build would spare Lakehouse
    names and a prune would silently do nothing.
    """

    managed = managed_sets(documents_of(estate), SQL_TARGET)

    assert managed.tables == frozenset()
    assert managed.views == frozenset()
    # A folder lives under Files whichever object side is being asked about.
    assert managed.folders == frozenset({"raw.customercsv", "_.load"})


def test_the_keep_set_and_an_inventory_are_the_same_shape(estate):
    """Why the diff is a set difference at all, asserted rather than assumed.

    `_Managed` and `TargetInventory` carry the same five sets. That is what
    makes "already built" expressible as an inventory built from the repository,
    and it is the property the whole prune design rests on.
    """

    managed = managed_sets(documents_of(estate), DELTA_TARGET)
    installed = FixtureInventory.from_repository(estate)

    # Compared case-insensitively: the inventory keeps the names a target would
    # report, the keep-set folds them, and the diff closes that gap itself.
    assert {name.casefold() for name in installed.tables} == set(managed.tables)
    assert {name.casefold() for name in installed.views} == set(managed.views)
    assert {name.casefold() for name in installed.folders} == set(managed.folders)


# --- the diff -----------------------------------------------------------------


def stage_for(repository, inventory, *, item=ITEM):
    identity = item_id(item) if isinstance(item, str) else item
    return item_prune_stage(
        repository,
        {key for key in repository.source_documents if key.item == identity},
        item=identity,
        target=bound_target(),
        inventory=inventory,
    )


def test_an_estate_that_already_matches_is_left_entirely_alone(estate):
    """The claim that most needed a real build, now a one-liner.

    An inventory built from the repository is exactly what a successful build
    would have left, so a prune against it must find nothing at all.
    """

    stage = stage_for(estate, FixtureInventory.from_repository(estate))

    assert stage is None


def test_an_object_the_item_does_not_declare_is_removed(estate):
    stage = stage_for(
        estate,
        target_inventory(schemas=("DWG",), tables=("DWG.Customer", "DWG.OldTable")),
    )

    assert prune_kinds(stage) == ["prune_table"]
    assert any("OldTable" in node for node in prune_targets(stage))


def test_an_undeclared_view_folder_and_schema_are_each_removed(estate):
    stage = stage_for(
        estate,
        target_inventory(
            schemas=("DWG", "Legacy"),
            folder_schemas=("Raw",),
            views=("DWG.OldView",),
            folders=("Raw.OldFolder",),
        ),
    )

    assert set(prune_kinds(stage)) >= {"prune_view", "prune_folder", "prune_schema"}


def test_an_empty_inventory_prunes_nothing_rather_than_everything(estate):
    """Nothing there is nothing to remove — not everything to remove.

    The direction matters: a diff computed the wrong way round against a fresh
    target would emit a removal for every declared object, and the actions would
    run against objects that do not exist.
    """

    stage = stage_for(estate, target_inventory())

    assert stage is None


def test_a_declared_schema_is_never_pruned_even_when_empty(estate):
    """The schema is desired state; that nothing is in it yet is not a reason."""

    stage = stage_for(estate, target_inventory(schemas=("DWG",)))

    assert "prune_schema" not in prune_kinds(stage)


# --- item scoping -------------------------------------------------------------


def test_another_items_objects_are_not_this_items_to_remove(tmp_path):
    """One item is diffed against one target, and reaches no further.

    Items share physical targets in real estates, so a prune that ignored item
    scoping would delete a neighbour's objects — and would look correct against
    a single-item fixture.
    """

    root = tmp_path / "estate"
    single_document_repository(
        root, documents={"DWG__Customer.py": lakehouse_table("DWG.Customer")}
    )
    other = single_document_repository(
        root, item="Lakehouse/Other", documents={
            "DWG__Neighbour.py": lakehouse_table("DWG.Neighbour")
        }
    )

    stage = item_prune_stage(
        other,
        {
            key
            for key in other.source_documents
            if key.item == item_id("Lakehouse/Other")
        },
        item=item_id("Lakehouse/Other"),
        target=bound_target(),
        inventory=target_inventory(
            schemas=("DWG",), tables=("DWG.Neighbour", "DWG.Customer")
        ),
    )

    # `DWG.Customer` belongs to another item, but this item cannot know that from
    # the inventory alone — so it is pruned. That is correct and worth pinning:
    # separation comes from binding items to different targets, not from prune.
    assert any("Customer" in node for node in prune_targets(stage))
    assert not any("Neighbour" in node for node in prune_targets(stage))


# --- the Warehouse side -------------------------------------------------------


@pytest.fixture
def warehouse_estate(tmp_path):
    """A Warehouse item, which is the side nothing here used to cover.

    Every other estate in this module is a Lakehouse, and that asymmetry hid a
    real defect: a Lakehouse's generated `_` is a *folder document*, so it enters
    the keep-set through the ordinary document path and needs no help, while a
    Warehouse's `_` is a schema no document declares. The two look interchangeable
    from a distance and are not.
    """

    from factories import WAREHOUSE_ITEM, schema_document, warehouse_table

    from weaver.declaration import parse_item_repository
    from weaver.locations import Location

    root = tmp_path / "repo"
    for relative, text in {
        f"{WAREHOUSE_ITEM}/schemas/Sales.yml": schema_document("Sales"),
        f"{WAREHOUSE_ITEM}/Sales.Customer.sql": warehouse_table("Sales.Customer"),
    }.items():
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    return parse_item_repository(Location(str(root)))


def test_a_freshly_built_warehouse_is_pruned_of_nothing(warehouse_estate):
    """The claim the Fabric tier was making alone, one layer down and free.

    A build that creates `_` for its load procedures and then prunes it is not a
    subtle failure — it destroys the schema it just made, every build. It was
    invisible here because the keep-set's load half arrived as a defaulted
    argument that only production passed, which is why `item_prune_stage` now
    derives it instead of accepting it.
    """

    from factories import WAREHOUSE_ITEM

    item = item_id(WAREHOUSE_ITEM)
    target = bound_target(
        id="target-1",
        kind="warehouse",
        item_id="Reporting_WH",
        logical_item_name="Reporting",
        logical_item_type="Warehouse",
    )

    stage = item_prune_stage(
        warehouse_estate,
        set(warehouse_estate.source_documents),
        item=item,
        target=target,
        inventory=FixtureInventory.from_repository(
            warehouse_estate,
            item=WAREHOUSE_ITEM,
            target_kind=SQL_TARGET,
            kind="warehouse",
            target_name="Reporting_WH",
        ),
    )

    assert stage is None


def test_the_procedure_schema_is_in_the_keep_set(warehouse_estate):
    """Stated directly, so a failure names the keep-set rather than the stage."""

    from factories import WAREHOUSE_ITEM

    from weaver.etl import item_load_artefacts

    documents = {
        str(identity): document
        for identity, document in warehouse_estate.source_documents.items()
        if identity.item == item_id(WAREHOUSE_ITEM)
    }
    managed = managed_sets(
        documents,
        SQL_TARGET,
        load_identities=[
            artefact.identity
            for artefact in item_load_artefacts(
                warehouse_estate, item=item_id(WAREHOUSE_ITEM)
            )
        ],
    )

    assert "_" in managed.schemas
