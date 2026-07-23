"""The SES contract, validated to exhaustion before anything physical happens."""

from __future__ import annotations

import textwrap

import pytest

from weaver.errors import MetadataError
from weaver.ses import (
    AUDIT_COLUMNS,
    FOLDER,
    PYTHON,
    SPARK_SQL,
    SQL,
    TABLE,
    VIEW,
    parse_document,
    parse_python_document,
    parse_sql_document,
)

# Fixtures follow the layout convention: a blank line between subsections, so
# the convention is learned by reading rather than by being told.
TABLE_YAML = """
Table ID: Sales.Order

Description: One row per customer order.

Lineage: Sales system order export.

Primary key: Order id

Schema:
  Order id: string
  Order date: date
  Amount: decimal(18,2)
"""

FOLDER_YAML = """
Folder ID: Sales.OrderExport

Description: Raw order export files.

Lineage: Nightly drop from the sales system.

File key: "*.csv"
"""


def parse(yaml_text: str, *, language: str = PYTHON):
    return parse_document(textwrap.dedent(yaml_text), language=language)


# --- identity and kind -----------------------------------------------------


def test_a_table_parses():
    document = parse(TABLE_YAML)
    assert document.kind == TABLE
    assert document.qualified == "Sales.Order"
    assert document.primary_key == ("Order id",)


def test_exactly_one_id_key_is_required():
    with pytest.raises(MetadataError, match="exactly one"):
        parse("Description: x\nLineage: y")
    with pytest.raises(MetadataError, match="exactly one"):
        parse("Table ID: A.B\nView ID: A.C\nDescription: x\nLineage: y")


def test_the_id_must_be_two_parts():
    with pytest.raises(MetadataError, match="two-part"):
        parse("Table ID: Order\nDescription: x\nLineage: y")


def test_duplicate_keys_are_refused():
    with pytest.raises(MetadataError, match="duplicate"):
        parse(FOLDER_YAML + "\nDescription: again")


# --- unknown keys ----------------------------------------------------------


def test_unknown_keys_are_named_not_ignored():
    """A mistyped 'Primary Key' must not parse as no primary key at all."""
    with pytest.raises(MetadataError, match="Primary Key"):
        parse(TABLE_YAML + "\nPrimary Key: Order id")


def test_a_key_from_another_kind_says_so():
    with pytest.raises(MetadataError, match="another object kind"):
        parse(FOLDER_YAML + "\nPrimary key: Order id")


def test_retired_keys_explain_the_migration():
    with pytest.raises(MetadataError, match="Incremental"):
        parse(FOLDER_YAML + "\nAuto delete: true")


def test_load_mode_is_gone():
    with pytest.raises(MetadataError, match="Load mode"):
        parse(TABLE_YAML + "\nLoad mode: upsert")


# --- text, placeholders and references -------------------------------------


def test_description_and_lineage_are_required():
    with pytest.raises(MetadataError, match="Description"):
        parse("Table ID: A.B\nLineage: y")


def test_placeholders_are_refused():
    with pytest.raises(MetadataError, match="placeholder"):
        parse("Folder ID: A.B\nDescription: TBD\nLineage: y\nFile key: '*'")


def test_a_whole_value_reference_is_a_reference():
    document = parse(TABLE_YAML.replace("Description: One row per customer order.",
                                        "Description: $Sales.OrderSource"))
    assert document.description.is_reference
    assert document.description.reference.object_id.qualified == "Sales.OrderSource"
    assert document.description.reference.column is None


def test_a_column_reference_carries_the_column():
    document = parse(TABLE_YAML.replace("Description: One row per customer order.",
                                        "Description: $Sales.OrderSource[Order date]"))
    assert document.description.reference.column == "Order date"


def test_mixed_prose_and_reference_is_refused():
    """A contract that is only sometimes machine-readable is not a contract."""
    with pytest.raises(MetadataError, match="not a mix"):
        parse(TABLE_YAML.replace("Description: One row per customer order.",
                                 "Description: See $Sales.OrderSource"))


def test_a_literal_dollar_can_be_escaped():
    document = parse(TABLE_YAML.replace("Description: One row per customer order.",
                                        "Description: Amounts are in $$AUD."))
    assert document.description.literal == "Amounts are in $AUD."
    assert not document.description.is_reference


# --- notes and revision notes ----------------------------------------------


def test_notes_are_free_range():
    """Unpoliced by design — no reference parsing, no placeholder rules."""
    document = parse(TABLE_YAML + "\nNotes: |\n  Amounts are $AUD.\n  TBD whether tax is included.")
    assert document.notes.startswith("Amounts are $AUD.")


def test_notes_must_not_be_blank_when_present():
    with pytest.raises(MetadataError, match="Notes"):
        parse(TABLE_YAML + "\nNotes: '   '")


def test_revision_notes_keep_their_date_and_note():
    document = parse(
        TABLE_YAML + "\nRevision notes:\n  - 2026-07-23 Added the amount column."
    )
    assert document.revision_notes[0].date == "2026-07-23"
    assert document.revision_notes[0].note == "Added the amount column."
    assert document.revision_date_format == "YYYY-MM-DD"


@pytest.mark.parametrize(
    "entry,shape",
    [
        ("2026-07-23 note", "YYYY-MM-DD"),
        ("2026/07/23 note", "YYYY/MM/DD"),
        ("23/07/2026 note", "DD/MM/YYYY"),
        ("23-07-2026 note", "DD-MM-YYYY"),
        ("23.07.2026 note", "DD.MM.YYYY"),
    ],
)
def test_any_consistent_date_spelling_is_accepted(entry, shape):
    document = parse(TABLE_YAML + f"\nRevision notes:\n  - {entry}")
    assert document.revision_date_format == shape


def test_mixing_date_formats_within_an_object_is_refused():
    with pytest.raises(MetadataError, match="mix date formats"):
        parse(
            TABLE_YAML
            + "\nRevision notes:\n  - 2026-07-23 first\n  - 24/07/2026 second"
        )


def test_month_first_and_day_first_are_the_same_shape():
    """Indistinguishable, so Weaver checks the shape rather than the reading."""
    document = parse(
        TABLE_YAML + "\nRevision notes:\n  - 07/23/2026 first\n  - 24/07/2026 second"
    )
    assert document.revision_date_format == "DD/MM/YYYY"


def test_an_entry_without_a_date_is_refused():
    with pytest.raises(MetadataError, match="must open with a date"):
        parse(TABLE_YAML + "\nRevision notes:\n  - Added the amount column.")


def test_an_entry_with_a_date_but_no_note_is_refused():
    with pytest.raises(MetadataError, match="no note"):
        parse(TABLE_YAML + "\nRevision notes:\n  - 2026-07-23")


def test_an_impossible_date_is_refused():
    with pytest.raises(MetadataError, match="real date"):
        parse(TABLE_YAML + "\nRevision notes:\n  - 2026-13-45 nonsense")


def test_revision_notes_must_be_a_list():
    with pytest.raises(MetadataError, match="YAML list"):
        parse(TABLE_YAML + "\nRevision notes: 2026-07-23 one note")


def test_notes_and_revision_notes_apply_to_every_kind():
    document = parse(
        FOLDER_YAML + "\nNotes: Free text.\nRevision notes:\n  - 2026-07-23 Created."
    )
    assert document.notes == "Free text."
    assert len(document.revision_notes) == 1


# --- column set versus column list -----------------------------------------


def test_a_column_set_is_comma_separated():
    document = parse(TABLE_YAML.replace("Primary key: Order id",
                                        "Primary key: Order id, Order date"))
    assert document.primary_key == ("Order id", "Order date")


def test_a_column_set_refuses_a_yaml_list():
    with pytest.raises(MetadataError, match="column set"):
        parse(TABLE_YAML.replace("Primary key: Order id",
                                 "Primary key:\n  - Order id"))


def test_a_column_list_is_a_yaml_list():
    document = parse(TABLE_YAML + "\nNot null:\n  - Order date\n  - Amount")
    assert document.declared_not_null == ("Order date", "Amount")


def test_a_column_list_refuses_comma_separated_text():
    with pytest.raises(MetadataError, match="YAML list"):
        parse(TABLE_YAML + "\nNot null: Order date, Amount")


# --- cross-column guards ---------------------------------------------------


def test_columns_must_exist_in_schema():
    with pytest.raises(MetadataError, match="not in Schema"):
        parse(TABLE_YAML.replace("Primary key: Order id", "Primary key: Ordr id"))


def test_not_null_repeating_the_primary_key_is_refused():
    with pytest.raises(MetadataError, match="already not null"):
        parse(TABLE_YAML + "\nNot null:\n  - Order id")


def test_comparison_columns_may_not_include_the_key():
    with pytest.raises(MetadataError, match="equal keys by definition"):
        parse(TABLE_YAML + "\nComparison columns: Order id, Amount")


def test_comparison_columns_require_a_primary_key():
    without_key = TABLE_YAML.replace("Primary key: Order id\n", "")
    with pytest.raises(MetadataError, match="require a Primary key"):
        parse(without_key + "\nComparison columns: Amount")


def test_incremental_requires_a_primary_key():
    without_key = TABLE_YAML.replace("Primary key: Order id\n", "")
    with pytest.raises(MetadataError, match="requires a Primary key"):
        parse(without_key + "\nIncremental: true")


def test_static_and_incremental_contradict():
    with pytest.raises(MetadataError, match="contradict"):
        parse(TABLE_YAML + "\nIncremental: true\nStatic: true")


def test_audit_column_names_are_reserved():
    with pytest.raises(MetadataError, match="reserved"):
        parse(TABLE_YAML + "\n  Row_insert_datetime: timestamp")


def test_identity_is_a_single_column():
    with pytest.raises(MetadataError, match="single column"):
        parse(TABLE_YAML + "\nIdentity: Order id, Order date")


def test_identity_may_be_the_primary_key():
    """Unusual but legitimate: no business key, every inserted row is a row."""
    document = parse(TABLE_YAML + "\nIdentity: Order id")
    assert document.identity == "Order id"


# --- defaults --------------------------------------------------------------


def test_folder_defaults_to_incremental_and_prohibited_rebuild():
    document = parse(FOLDER_YAML)
    assert document.kind == FOLDER
    assert document.is_incremental is True
    assert document.prohibit_rebuild is True


def test_a_table_defaults_to_neither():
    document = parse(TABLE_YAML)
    assert document.is_incremental is False
    assert document.prohibit_rebuild is False


def test_prohibit_rebuild_works_on_views():
    """Admins add security to views; a rebuild would lose it."""
    document = parse(
        "View ID: Sales.OrderView\nDescription: x\nLineage: y\nProhibit rebuild: true",
        language=SQL,
    )
    assert document.kind == VIEW
    assert document.prohibit_rebuild is True


def test_incremental_is_refused_on_a_view():
    with pytest.raises(MetadataError, match="View"):
        parse("View ID: A.B\nDescription: x\nLineage: y\nIncremental: true", language=SQL)


def test_not_null_includes_the_primary_key():
    document = parse(TABLE_YAML + "\nNot null:\n  - Order date")
    assert document.not_null == ("Order id", "Order date")


def test_comparison_columns_default_to_every_non_key_column():
    assert parse(TABLE_YAML).comparison_columns == ("Order date", "Amount")


def test_a_narrower_comparison_set_is_kept():
    document = parse(TABLE_YAML + "\nComparison columns: Order date")
    assert document.comparison_columns == ("Order date",)


# --- audit columns ---------------------------------------------------------


def test_declared_schema_stays_exactly_what_was_written():
    document = parse(TABLE_YAML)
    assert [column.name for column in document.schema] == [
        "Order id", "Order date", "Amount",
    ]


def test_the_effective_schema_adds_the_audit_columns():
    document = parse(TABLE_YAML)
    assert [column.name for column in document.effective_schema][-3:] == [
        "Row_insert_datetime", "Row_update_datetime", "Row_delete_datetime",
    ]


def test_a_warehouse_table_keeps_the_spaced_audit_names():
    document = parse(
        "Table ID: Sales.Order\nDescription: x\nLineage: y\nPrimary key: Order id",
        language=SQL,
    )
    assert [column.name for column in document.audit_columns] == list(AUDIT_COLUMNS)


def test_a_live_row_carries_a_delete_datetime():
    document = parse(TABLE_YAML)
    delete = document.audit_columns[-1]
    assert delete.not_null is True


def test_folders_have_no_audit_columns():
    assert parse(FOLDER_YAML).audit_columns == ()


# --- schema declaration by representation ----------------------------------


def test_a_delta_table_must_declare_schema():
    with pytest.raises(MetadataError, match="must declare Schema"):
        parse("Table ID: A.B\nDescription: x\nLineage: y")


def test_a_warehouse_table_must_not_declare_schema():
    with pytest.raises(MetadataError, match="takes its shape from its query"):
        parse(TABLE_YAML, language=SQL)


def test_a_warehouse_table_defers_column_validation():
    document = parse(
        "Table ID: Sales.Order\nDescription: x\nLineage: y\nPrimary key: Order id",
        language=SQL,
    )
    assert document.defers_column_validation is True
    assert document.primary_key == ("Order id",)


def test_a_delta_table_validates_now():
    assert parse(TABLE_YAML).defers_column_validation is False


# --- column notes ----------------------------------------------------------


def test_column_notes_attach_to_declared_columns():
    document = parse(TABLE_YAML + "\nColumn notes:\n  Amount: Order total including tax.")
    amount = next(column for column in document.schema if column.name == "Amount")
    assert amount.note.literal == "Order total including tax."


def test_column_notes_may_reference_another_object():
    document = parse(TABLE_YAML + "\nColumn notes:\n  Amount: $Sales.Invoice[Amount]")
    amount = next(column for column in document.schema if column.name == "Amount")
    assert amount.note.reference.column == "Amount"


def test_column_notes_must_name_declared_columns():
    with pytest.raises(MetadataError, match="not in Schema"):
        parse(TABLE_YAML + "\nColumn notes:\n  Amont: typo")


def test_a_warehouse_object_describes_columns_without_a_schema():
    document = parse(
        "Table ID: Sales.Order\nDescription: x\nLineage: y\n"
        "Column notes:\n  Amount: Order total.",
        language=SQL,
    )
    assert document.defers_column_validation is True


# --- folders ---------------------------------------------------------------


def test_a_folder_must_declare_file_keys():
    with pytest.raises(MetadataError, match="File key"):
        parse("Folder ID: A.B\nDescription: x\nLineage: y")


def test_file_keys_may_not_traverse():
    with pytest.raises(MetadataError, match="traverse"):
        parse(FOLDER_YAML.replace('File key: "*.csv"', 'File key: "../*.csv"'))


def test_not_null_columns_are_marked_on_the_schema():
    document = parse(TABLE_YAML + "\nNot null:\n  - Amount")
    marked = {column.name for column in document.schema if column.not_null}
    assert marked == {"Order id", "Amount"}


# --- extraction ------------------------------------------------------------


def test_python_metadata_comes_from_the_module_docstring():
    source = f'"""{TABLE_YAML}"""\n\nclass Order:\n    pass\n'
    assert parse_python_document(source).qualified == "Sales.Order"


def test_a_python_object_without_a_docstring_is_refused():
    with pytest.raises(MetadataError, match="docstring"):
        parse_python_document("class Order:\n    pass\n")


def test_sql_metadata_comes_from_the_opening_comment():
    source = (
        "/*\nTable ID: Sales.Order\nDescription: x\nLineage: y\n*/\n"
        "select 1 as [Order id]\n"
    )
    document, body = parse_sql_document(source)
    assert document.qualified == "Sales.Order"
    assert body.startswith("select 1")


def test_sql_without_a_metadata_block_is_refused():
    with pytest.raises(MetadataError, match="metadata block"):
        parse_sql_document("select 1\n")


# --- spark sql and declared dependencies ------------------------------------


SPARK_YAML = """
Table ID: Sales.OrderSummary

Description: Order totals by customer.

Lineage: Aggregated from the order table.

Primary key: Customer id

Dependencies:
  - Sales.Order

Schema:
  Customer id: string
  Total: decimal(18,2)
"""


def test_a_spark_sql_table_parses():
    document = parse(SPARK_YAML, language=SPARK_SQL)
    assert document.language == SPARK_SQL
    assert document.dependencies[0].qualified == "Sales.Order"


def test_a_spark_sql_table_must_declare_schema():
    """It materialises Delta, so its shape is declared like Python's."""
    without = SPARK_YAML.split("Schema:")[0]
    with pytest.raises(MetadataError, match="must declare Schema"):
        parse(without, language=SPARK_SQL)


def test_a_spark_sql_object_must_declare_dependencies():
    """Its query may read by path, which cannot resolve back to an object."""
    without = SPARK_YAML.replace("Dependencies:\n  - Sales.Order\n", "")
    with pytest.raises(MetadataError, match="must declare Dependencies"):
        parse(without, language=SPARK_SQL)


def test_a_spark_sql_table_uses_the_delta_audit_spelling():
    document = parse(SPARK_YAML, language=SPARK_SQL)
    assert [column.name for column in document.audit_columns] == [
        "Row_insert_datetime", "Row_update_datetime", "Row_delete_datetime",
    ]


def test_a_spark_sql_view_is_a_real_object():
    """Fabric Lakehouse views persist in the metastore."""
    document = parse(
        "View ID: Sales.OrderView\nDescription: x\nLineage: y\n"
        "Dependencies:\n  - Sales.Order",
        language=SPARK_SQL,
    )
    assert document.kind == VIEW
    assert document.audit_columns == ()


def test_dependencies_are_optional_for_python_and_sql():
    assert parse(TABLE_YAML).dependencies == ()


def test_declared_dependencies_are_two_part_names():
    with pytest.raises(MetadataError, match="two-part"):
        parse(TABLE_YAML + "\nDependencies:\n  - Order")


def test_an_object_may_not_depend_on_itself():
    with pytest.raises(MetadataError, match="cannot depend on itself"):
        parse(TABLE_YAML + "\nDependencies:\n  - Sales.Order")


def test_dependencies_may_not_repeat():
    with pytest.raises(MetadataError, match="repeats"):
        parse(TABLE_YAML + "\nDependencies:\n  - Sales.Customer\n  - Sales.Customer")


def test_dependencies_must_be_a_list():
    with pytest.raises(MetadataError, match="YAML list"):
        parse(TABLE_YAML + "\nDependencies: Sales.Customer")
