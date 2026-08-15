"""The whole path over one workspace declaration.

Filename classification -> metadata extraction -> structural checks -> SQL
analysis -> discovered references -> signature -> graph, asserted together
rather than in pieces, so a regression anywhere in the chain surfaces here.

The estate is deliberately awkward in the ways real ones are. ``Sales.Customer``
exists twice — as a Delta table in ``Lakehouse/Sales`` and as a Warehouse table
in ``Warehouse/Reporting`` — which the item model resolves by ownership rather
than by disambiguating a shared name. Both SQL documents stage through a
temporary object, both mention a retired object in a comment, and the Warehouse
reads the Lakehouse by its three-part physical name, which stays a reference and
never becomes a graph edge.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from weaver.declaration import PYTHON, SPARK_SQL, SQL, parse_item_repository
from weaver.locations import Location

FIXTURE = Location(str(Path(__file__).parent / "fixtures" / "estate-item"))

#: Weaver generates this item into every declaration. It is covered by its own
#: tests; here it would only add noise to assertions about authored content.
BUILTIN = "Lakehouse/_weaver"


@pytest.fixture(scope="module")
def repository():
    return parse_item_repository(FIXTURE)


@pytest.fixture(scope="module")
def authored(repository):
    """Every authored document, keyed by its full logical identity.

    Generated declarations are excluded, and the word is meant literally: a
    parsed repository also carries what Weaver composes into it — the builtin
    catalogue item, and the runtime folder an item with load code is deployed
    into. Those are real documents and are built like any other; they are simply
    not what a test about *authoring* is describing.
    """

    return {
        str(identity): document
        for identity, document in repository.source_documents.items()
        if str(identity.item) != BUILTIN
        and document.relative_path not in repository.generated_files
    }


# --- classification ----------------------------------------------------------


def test_every_object_is_classified_from_its_filename_and_item(authored):
    """The item picks the SQL dialect; the filename picks nothing but the name."""

    assert {
        name: (document.language, document.kind) for name, document in authored.items()
    } == {
        "Lakehouse/Sales/Files/Sales.OrderExport": (PYTHON, "Folder"),
        "Lakehouse/Sales/Sales.Order": (PYTHON, "Table"),
        "Lakehouse/Sales/Sales.Customer": (PYTHON, "Table"),
        "Lakehouse/Sales/Sales.OrderSummary": (SPARK_SQL, "Table"),
        "Warehouse/Reporting/Sales.Customer": (SQL, "Table"),
        "Warehouse/Reporting/Reporting.OrderReport": (SQL, "Table"),
        "Warehouse/Reporting/Reporting.OrderView": (SQL, "View"),
    }


def test_one_id_belongs_to_two_items_without_ambiguity(authored):
    """``Sales.Customer`` twice is two documents, not one name to disambiguate.

    The flat model had to ask which target was meant. Ownership answers it here:
    the item is part of the identity, so there is nothing left to resolve.
    """

    lakehouse = authored["Lakehouse/Sales/Sales.Customer"]
    warehouse = authored["Warehouse/Reporting/Sales.Customer"]
    assert lakehouse.qualified == warehouse.qualified == "Sales.Customer"
    assert lakehouse.language == PYTHON and warehouse.language == SQL
    assert lakehouse.node_id != warehouse.node_id


# --- metadata ----------------------------------------------------------------


def test_a_python_table_carries_its_full_contract(authored):
    document = authored["Lakehouse/Sales/Sales.Order"].document
    assert document.primary_key == ("Order id",)
    assert document.not_null == ("Order id", "Order date")
    assert document.comparison_columns == ("Last modified",)
    assert document.lineage.reference.object_id.qualified == "Sales.OrderExport"
    assert [column.name for column in document.effective_schema][-3:] == [
        "row_insert_datetime",
        "row_update_datetime",
        "row_delete_datetime",
    ]


def test_a_folder_carries_its_file_key_and_defaults(authored):
    document = authored["Lakehouse/Sales/Files/Sales.OrderExport"].document
    assert document.file_keys == ("*.csv",)
    assert document.is_incremental is True
    assert document.prohibit_rebuild is True


def test_a_warehouse_object_defers_column_validation(authored):
    report = authored["Warehouse/Reporting/Reporting.OrderReport"].document
    assert report.defers_column_validation is True
    assert [column.name for column in report.audit_columns] == [
        "Row insert datetime",
        "Row update datetime",
        "Row delete datetime",
    ]


def test_a_view_prohibits_rebuild_with_its_reason(authored):
    document = authored["Warehouse/Reporting/Reporting.OrderView"].document
    assert document.prohibit_rebuild is True
    assert "Row-level security" in document.notes


def test_a_spark_table_declares_schema_and_dependencies(authored):
    document = authored["Lakehouse/Sales/Sales.OrderSummary"].document
    assert [column.name for column in document.schema] == [
        "Customer id",
        "Order count",
        "Total amount",
    ]
    assert [str(d) for d in document.dependencies] == ["Sales.Order"]
    assert [column.name for column in document.audit_columns][
        0
    ] == "row_insert_datetime"


# --- structural --------------------------------------------------------------


def test_python_objects_name_their_class_for_the_file(authored):
    assert authored["Lakehouse/Sales/Sales.Order"].class_name == "Sales__Order"
    assert (
        authored["Lakehouse/Sales/Files/Sales.OrderExport"].class_name
        == "Sales__OrderExport"
    )


def test_lib_files_travel_with_their_item_but_are_not_objects(repository):
    assert repository.support_files == ("Lakehouse/Sales/lib/dates.py",)


# --- sql analysis ------------------------------------------------------------


def test_the_spark_table_stages_through_a_temporary_view(authored):
    analysis = authored["Lakehouse/Sales/Sales.OrderSummary"].sql_analysis
    assert analysis.statement_count == 2
    assert analysis.result_set_count == 1
    assert analysis.permanent_ddl == ()


def test_the_warehouse_table_stages_through_a_temp_table(authored):
    analysis = authored["Warehouse/Reporting/Reporting.OrderReport"].sql_analysis
    assert analysis.statement_count == 2
    assert analysis.result_set_count == 1


def test_the_view_is_a_single_statement(authored):
    analysis = authored["Warehouse/Reporting/Reporting.OrderView"].sql_analysis
    assert analysis.statement_count == 1
    assert analysis.result_set_count == 1


# --- discovered references ---------------------------------------------------


def test_references_across_every_language(authored):
    assert {
        name: sorted(str(r) for r in document.discovered_references)
        for name, document in authored.items()
    } == {
        "Lakehouse/Sales/Files/Sales.OrderExport": [],
        "Lakehouse/Sales/Sales.Order": [],
        "Lakehouse/Sales/Sales.Customer": [],
        "Lakehouse/Sales/Sales.OrderSummary": ["Sales.Cancelled", "Sales.Order"],
        "Warehouse/Reporting/Sales.Customer": ["Sales_LH.Sales.Customer"],
        "Warehouse/Reporting/Reporting.OrderReport": [
            "Sales.Customer",
            "Sales.OrderLineCount",
            "Sales_LH.Sales.Order",
        ],
        "Warehouse/Reporting/Reporting.OrderView": ["Reporting.OrderReport"],
    }


def test_the_spark_temporary_view_is_not_a_reference(authored):
    document = authored["Lakehouse/Sales/Sales.OrderSummary"]
    assert "recent" not in str(document.discovered_references)


def test_the_warehouse_temp_table_is_not_a_reference(authored):
    document = authored["Warehouse/Reporting/Reporting.OrderReport"]
    assert "#recent" not in str(document.discovered_references)


def test_comments_in_the_fixtures_contribute_nothing(authored):
    """Both SQL fixtures mention a Legacy object in a comment."""

    for document in authored.values():
        assert not any(
            "Legacy" in str(reference) for reference in document.discovered_references
        )


def test_a_declaration_sits_beside_what_was_discovered(authored):
    """Declaration overrides discovery; both are kept so a lint can compare."""

    summary = authored["Lakehouse/Sales/Sales.OrderSummary"]
    assert [str(d) for d in summary.declared_dependencies] == ["Sales.Order"]
    assert "Sales.Cancelled" in [str(r) for r in summary.discovered_references]


# --- the signature -----------------------------------------------------------


def test_the_signature_covers_every_file(repository, tmp_path):
    import shutil

    copy = tmp_path / "estate"
    shutil.copytree(FIXTURE.value, copy)
    assert parse_item_repository(Location(str(copy))).signature == repository.signature

    (copy / "Lakehouse" / "Sales" / "lib" / "dates.py").write_text(
        "# changed\n", encoding="utf-8"
    )
    assert parse_item_repository(Location(str(copy))).signature != repository.signature


def test_an_item_signature_moves_only_for_its_own_content(repository, tmp_path):
    import shutil

    copy = tmp_path / "estate"
    shutil.copytree(FIXTURE.value, copy)
    (copy / "Warehouse" / "Reporting" / "Reporting.OrderView.sql").write_text(
        (copy / "Warehouse" / "Reporting" / "Reporting.OrderView.sql")
        .read_text(encoding="utf-8")
        .replace("without deleted rows", "without deleted rows, revised"),
        encoding="utf-8",
    )
    after = parse_item_repository(Location(str(copy)))

    assert (
        after["Warehouse/Reporting"].signature
        != repository["Warehouse/Reporting"].signature
    )
    assert after["Lakehouse/Sales"].signature == repository["Lakehouse/Sales"].signature


# --- the graph ---------------------------------------------------------------


def test_the_declaration_orders_upstream_before_downstream(repository):
    order = repository.dependency_graph.order()
    assert order.index("Lakehouse/Sales/Files/Sales.OrderExport") < order.index(
        "Lakehouse/Sales/Sales.Order"
    )
    assert order.index("Lakehouse/Sales/Sales.Order") < order.index(
        "Lakehouse/Sales/Sales.OrderSummary"
    )
    assert order.index("Warehouse/Reporting/Reporting.OrderReport") < order.index(
        "Warehouse/Reporting/Reporting.OrderView"
    )


def test_the_layers_show_what_can_run_together(repository, authored):
    layers = tuple(
        tuple(node for node in layer if node in authored)
        for layer in repository.dependency_graph.layers()
    )
    assert tuple(layer for layer in layers if layer) == (
        (
            "Lakehouse/Sales/Files/Sales.OrderExport",
            "Warehouse/Reporting/Sales.Customer",
        ),
        (
            "Lakehouse/Sales/Sales.Customer",
            "Lakehouse/Sales/Sales.Order",
            "Warehouse/Reporting/Reporting.OrderReport",
        ),
        (
            "Lakehouse/Sales/Sales.OrderSummary",
            "Warehouse/Reporting/Reporting.OrderView",
        ),
    )


def test_a_physical_three_part_read_is_a_reference_not_an_edge(repository):
    """The Warehouse reads the Lakehouse by physical name, which stays outside.

    Weaver records it exactly as written and resolves it to nothing: naming a
    physical item is the author's escape hatch, and turning it into a logical
    edge would invent an item ownership nobody declared.
    """

    physical = [
        edge
        for edge in repository.dependency_edges
        if edge.resolution_kind == "physical"
    ]
    assert {edge.reference for edge in physical} == {
        "Sales_LH.Sales.Order",
        "Sales_LH.Sales.Customer",
    }
    assert all(edge.producer is None for edge in physical)
    assert all(edge.is_within_item is False for edge in physical)


def test_a_two_part_name_resolves_inside_the_writers_own_item(repository):
    """``Sales.Customer`` in the Warehouse binds to the Warehouse's own table.

    The Lakehouse has a ``Sales.Customer`` too. A short name never reaches it —
    that is what makes the three-part physical read above necessary.
    """

    edge = next(
        edge
        for edge in repository.dependency_edges
        if str(edge.consumer) == "Warehouse/Reporting/Reporting.OrderReport"
        and edge.reference == "Sales.Customer"
    )
    assert str(edge.producer) == "Warehouse/Reporting/Sales.Customer"
    assert edge.is_within_item is True


def test_descendants_are_what_a_rebuild_would_uncertify(repository):
    assert set(
        repository.dependency_graph.descendants(
            "Lakehouse/Sales/Files/Sales.OrderExport"
        )
    ) == {
        "Lakehouse/Sales/Sales.Order",
        "Lakehouse/Sales/Sales.Customer",
        "Lakehouse/Sales/Sales.OrderSummary",
    }
