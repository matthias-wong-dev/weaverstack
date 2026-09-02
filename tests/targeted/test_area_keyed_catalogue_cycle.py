"""One ``Schema.Object`` in both Lakehouse areas, through every table that keys it.

A Lakehouse may hold ``Sales.Thing`` as a Delta table and as a Folder. They are
two objects, and every table that identifies a Weaver document by
``schema_name`` and ``object_name`` has to keep them apart: Registry, the
dictionaries, Bookmark, LoadStatus, LoadStatistic, the dependency rows the
installed graph is rebuilt from, and the line a run prints.

The area is what keeps them apart, and it is stored on both sides. Storing it on
the Folder alone was what made ``Sales.Thing`` in Registry mean the table while
``Sales.Thing`` in a logical identity meant a validation.

Asserted end to end from one repository, so what is compared is what a build
publishes and what a run records.
"""

from __future__ import annotations

import pytest
from factories import (
    folder_document,
    installed_catalogue,
    item_bindings,
    lakehouse_table,
    single_document_repository,
)
from support.weaver_test import weaver_test

from weaver.catalogue.claims import bookmark_row, catalogue_columns
from weaver.catalogue.tables import BOOKMARK, LOAD_STATUS, REGISTRY
from weaver.declaration.model import WeaverDocumentId, WeaverItemId
from weaver.installed import stored_identity

ITEM = "Lakehouse/Sales"
TARGET = "Sales_LH"
SHARED = "Sales.Thing"

TABLE = WeaverDocumentId.parse(f"{ITEM}/Tables/{SHARED}")
FOLDER = WeaverDocumentId.parse(f"{ITEM}/Files/{SHARED}")
VALIDATION = WeaverDocumentId.parse(f"{ITEM}/{SHARED}Count")


@pytest.fixture
def repository(tmp_path):
    """One item holding a table and a Folder of one ``Schema.Object``."""

    return single_document_repository(
        tmp_path / "repo",
        item=ITEM,
        schemas=("Sales",),
        documents={
            "Tables/Sales__Thing.py": lakehouse_table(SHARED),
            "Files/Sales__Thing.py": folder_document(SHARED),
        },
    )


def _keys(catalogue, table) -> set[tuple[str, str]]:
    return {
        (str(row.get("schema_name")), str(row.get("object_name")))
        for row in catalogue.table_rows(table)
        if row.get("object_role") in (None, "data")
    }


# --- the rule ------------------------------------------------------------------


@weaver_test()
def test_the_two_areas_are_two_registry_rows(repository):
    catalogue = installed_catalogue(repository, item_bindings((ITEM, TARGET)))

    assert {("Tables/Sales", "Thing"), ("Files/Sales", "Thing")} <= _keys(
        catalogue, REGISTRY
    )


@weaver_test()
def test_each_area_reads_back_as_the_identity_that_wrote_it():
    """Round trip, which is the property the two directions of the rule have."""

    item = WeaverItemId.parse(ITEM)
    warehouse = WeaverDocumentId.parse("Warehouse/Reporting/Sales.Customer")

    for identity in (TABLE, FOLDER):
        schema_name, object_name = catalogue_columns(identity)
        assert stored_identity(item, schema_name, object_name) == identity

    schema_name, object_name = catalogue_columns(warehouse)
    assert stored_identity(warehouse.item, schema_name, object_name) == warehouse


@weaver_test()
def test_a_validation_is_read_back_by_the_reader_that_knows_it_is_one():
    """It names no area, and the table it came from is what says it is one.

    ``stored_identity`` reads an object row, because Registry, Bookmark,
    LoadStatus and LoadStatistic hold nothing else.
    """

    item = WeaverItemId.parse(ITEM)
    schema_name, object_name = catalogue_columns(VALIDATION)

    assert (
        WeaverDocumentId.validation(
            item, VALIDATION.object_id.__class__(schema_name, object_name)
        )
        == VALIDATION
    )
    assert stored_identity(item, schema_name, object_name) != VALIDATION


@weaver_test()
def test_the_two_areas_are_two_bookmarks():
    """The row a load advances, which is what an incremental read depends on."""

    table = bookmark_row(TABLE)
    folder = bookmark_row(FOLDER)

    assert table["schema_name"] == "Tables/Sales"
    assert folder["schema_name"] == "Files/Sales"
    assert table != folder


@weaver_test()
def test_the_operational_tables_share_the_registry_key():
    """Bookmark, LoadStatus and LoadStatistic key what Registry keys."""

    from weaver.run.record import load_statistic_row, load_status_row
    from weaver.run.result import RunNodeResult

    for identity in (TABLE, FOLDER):
        expected = catalogue_columns(identity)
        node = RunNodeResult(
            node_id="n",
            physical_target=f"Lakehouse/{TARGET}",
            primitive_kind="python_table",
            status="succeeded",
            logical_id=str(identity),
        )
        rows = (
            load_status_row(node, identity, workflow_id="w"),
            load_statistic_row(node, identity, workflow_id="w"),
            bookmark_row(identity),
        )

        for row in rows:
            assert (row["schema_name"], row["object_name"]) == expected


@weaver_test()
def test_a_dependency_on_each_area_reconstructs_to_that_area(repository):
    """The installed graph is rebuilt from the stored key, so it must be exact."""

    catalogue = installed_catalogue(repository, item_bindings((ITEM, TARGET)))
    dag = catalogue.dag()

    assert dag.node(str(TABLE)).object_type == "table"
    assert dag.node(str(FOLDER)).object_type == "folder"


@weaver_test()
def test_a_run_names_each_area_on_its_own_line():
    """The canonical logical identity, which is what a request selects by."""

    from weaver.run.graph import RunNode
    from weaver.run.runner import node_label

    def _node(identity, kind):
        return RunNode(
            node_id="n",
            physical_target=f"Lakehouse/{TARGET}",
            primitive_kind=kind,
            logical_id=identity,
            role="load",
        )

    assert (
        node_label(_node(TABLE, "python_table"))
        == "Load Lakehouse/Sales/Tables/Sales.Thing"
    )
    assert (
        node_label(_node(FOLDER, "python_folder"))
        == "Load Lakehouse/Sales/Files/Sales.Thing"
    )


@weaver_test()
def test_a_run_names_a_validation_and_a_warehouse_relation_without_an_area():
    from weaver.run.graph import RunNode
    from weaver.run.runner import node_label

    relation = WeaverDocumentId.parse("Warehouse/Reporting/Sales.Customer")
    check = RunNode(
        node_id="n",
        physical_target="Lakehouse/Sales_LH",
        primitive_kind="python_test",
        logical_id=VALIDATION,
        role="Test",
    )
    load = RunNode(
        node_id="n",
        physical_target="Warehouse/Reporting_WH",
        primitive_kind="warehouse_procedure",
        logical_id=relation,
        role="load",
    )

    assert node_label(check) == "Test Lakehouse/Sales/Sales.ThingCount"
    assert node_label(load) == "Load Warehouse/Reporting/Sales.Customer"


@weaver_test()
def test_the_claim_predicates_name_the_area_they_delete():
    """Claim deletion finds a row by the key it was written under."""

    from weaver.catalogue.claims import claim_rules_for_object_type

    for identity, object_type, area in (
        (TABLE, "table", "Tables/Sales"),
        (FOLDER, "folder", "Files/Sales"),
    ):
        rules = claim_rules_for_object_type(object_type)
        assert rules
        for rule in rules:
            assert rule.values(identity)[0] == area
            assert rule.owns(
                dict(zip(rule.predicate_columns, rule.values(identity))), identity
            )


@weaver_test()
def test_the_status_tables_hold_one_row_for_each_area(repository):
    """The end of it: two loads, two rows, and neither overwrote the other."""

    written = {
        (row["schema_name"], row["object_name"])
        for row in (bookmark_row(TABLE, None), bookmark_row(FOLDER, None))
    }

    assert written == {("Tables/Sales", "Thing"), ("Files/Sales", "Thing")}
    assert LOAD_STATUS.key[:2] == BOOKMARK.key[:2]
