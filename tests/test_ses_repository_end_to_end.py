"""The whole path over the example repository.

Filename classification -> metadata extraction -> structural checks -> SQL
analysis -> discovered references, asserted together rather than in pieces, so
a regression anywhere in the chain surfaces here.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from weaver import Location
from weaver.ses import PYTHON, SPARK_SQL, SQL, read_repository

FIXTURE = Location(str(Path(__file__).parent / "fixtures" / "sales-etl"))


@pytest.fixture(scope="module")
def repo():
    return read_repository(FIXTURE)


# --- classification ----------------------------------------------------------


def test_every_object_is_classified_from_its_filename(repo):
    assert {
        document.qualified: (document.language, document.kind) for document in repo
    } == {
        "Sales.OrderExport": (PYTHON, "Folder"),
        "Sales.Order": (PYTHON, "Table"),
        "Sales.Customer": (PYTHON, "Table"),
        "Sales.OrderSummary": (SPARK_SQL, "Table"),
        "Reporting.OrderReport": (SQL, "Table"),
        "Reporting.OrderView": (SQL, "View"),
    }


# --- metadata ----------------------------------------------------------------


def test_a_python_table_carries_its_full_contract(repo):
    document = repo["Sales.Order"].document
    assert document.primary_key == ("Order id",)
    assert document.not_null == ("Order id", "Order date")
    assert document.comparison_columns == ("Last modified",)
    assert document.lineage.reference.object_id.qualified == "Sales.OrderExport"
    assert [column.name for column in document.effective_schema][-3:] == [
        "Row_insert_datetime", "Row_update_datetime", "Row_delete_datetime",
    ]


def test_a_folder_carries_its_file_key_and_defaults(repo):
    document = repo["Sales.OrderExport"].document
    assert document.file_keys == ("*.csv",)
    assert document.is_incremental is True
    assert document.prohibit_rebuild is True


def test_a_warehouse_object_defers_column_validation(repo):
    report = repo["Reporting.OrderReport"].document
    assert report.defers_column_validation is True
    assert [column.name for column in report.audit_columns] == [
        "Row insert datetime", "Row update datetime", "Row delete datetime",
    ]


def test_a_view_prohibits_rebuild_with_its_reason(repo):
    document = repo["Reporting.OrderView"].document
    assert document.prohibit_rebuild is True
    assert "Row-level security" in document.notes


def test_a_spark_table_declares_schema_and_dependencies(repo):
    document = repo["Sales.OrderSummary"].document
    assert [column.name for column in document.schema] == [
        "Customer id", "Order count", "Total amount",
    ]
    assert [str(d) for d in document.dependencies] == ["Sales.Order"]
    assert [column.name for column in document.audit_columns][0] == "Row_insert_datetime"


# --- structural --------------------------------------------------------------


def test_python_objects_name_their_class_for_the_file(repo):
    assert repo["Sales.Order"].class_name == "Sales__Order"
    assert repo["Sales.OrderExport"].class_name == "Sales__OrderExport"


def test_support_files_are_not_objects(repo):
    assert repo.support_files == ("_helpers/dates.py",)


# --- sql analysis ------------------------------------------------------------


def test_the_spark_table_stages_through_a_temporary_view(repo):
    analysis = repo["Sales.OrderSummary"].sql_analysis
    assert analysis.statement_count == 2
    assert analysis.result_set_count == 1
    assert analysis.permanent_ddl == ()


def test_the_warehouse_table_stages_through_a_temp_table(repo):
    analysis = repo["Reporting.OrderReport"].sql_analysis
    assert analysis.statement_count == 2
    assert analysis.result_set_count == 1


def test_the_view_is_a_single_statement(repo):
    analysis = repo["Reporting.OrderView"].sql_analysis
    assert analysis.statement_count == 1
    assert analysis.result_set_count == 1


# --- discovered references ---------------------------------------------------


def test_references_across_every_language(repo):
    assert {
        document.qualified: sorted(str(r) for r in document.discovered_references)
        for document in repo
    } == {
        "Sales.OrderExport": [],
        "Sales.Order": ["Sales.OrderExport"],
        "Sales.Customer": ["Sales.OrderExport"],
        "Sales.OrderSummary": ["Sales.Cancelled", "Sales.Order"],
        "Reporting.OrderReport": [
            "Sales.Customer", "Sales.Order", "Sales.OrderLineCount",
        ],
        "Reporting.OrderView": ["Reporting.OrderReport"],
    }


def test_the_spark_temporary_view_is_not_a_reference(repo):
    assert "recent" not in str(repo["Sales.OrderSummary"].discovered_references)


def test_the_warehouse_temp_table_is_not_a_reference(repo):
    assert "#recent" not in str(repo["Reporting.OrderReport"].discovered_references)


def test_comments_in_the_fixtures_contribute_nothing(repo):
    """Both SQL fixtures mention a Legacy object in a comment."""
    for document in repo:
        assert not any(
            "Legacy" in str(reference) for reference in document.discovered_references
        )


def test_a_declaration_sits_beside_what_was_discovered(repo):
    """Declaration overrides at build; both are kept so a lint can compare."""
    summary = repo["Sales.OrderSummary"]
    assert [str(d) for d in summary.declared_dependencies] == ["Sales.Order"]
    assert "Sales.Cancelled" in [str(r) for r in summary.discovered_references]


def test_references_outside_the_repository_are_simply_recorded(repo):
    """Sales.Cancelled and Sales.OrderLineCount are not objects here.

    Whether they resolve — to a shortcut, or to nothing — needs the
    external-dependency configuration and is settled at build.
    """
    known = set(repo.by_id)
    referenced = {
        str(reference)
        for document in repo
        for reference in document.discovered_references
    }
    assert {"Sales.Cancelled", "Sales.OrderLineCount"} <= referenced - known


# --- the signature -----------------------------------------------------------


def test_the_signature_covers_every_file(repo, tmp_path):
    import shutil

    copy = tmp_path / "sales-etl"
    shutil.copytree(FIXTURE.value, copy)
    assert read_repository(Location(str(copy))).signature == repo.signature

    (copy / "_helpers" / "dates.py").write_text("# changed\n", encoding="utf-8")
    assert read_repository(Location(str(copy))).signature != repo.signature
