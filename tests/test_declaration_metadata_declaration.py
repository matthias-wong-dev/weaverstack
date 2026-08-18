"""The Weaver document contract, validated to exhaustion before anything physical happens."""

from __future__ import annotations

import textwrap

import pytest
from support.weaver_test import weaver_test

from weaver.declaration import (
    ASSUMPTION,
    AUDIT_COLUMNS,
    FOLDER,
    PYTHON,
    SPARK_SQL,
    SQL,
    TABLE,
    TEST,
    VIEW,
    parse_document,
    parse_python_document,
    parse_sql_document,
)
from weaver.errors import MetadataError

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


@weaver_test()
def test_a_table_parses():
    document = parse(TABLE_YAML)
    assert document.kind == TABLE
    assert document.qualified == "Sales.Order"
    assert document.primary_key == ("Order id",)


@weaver_test()
def test_exactly_one_id_key_is_required():
    with pytest.raises(MetadataError, match="exactly one"):
        parse("Description: x\nLineage: y")
    with pytest.raises(MetadataError, match="exactly one"):
        parse("Table ID: A.B\nView ID: A.C\nDescription: x\nLineage: y")


@weaver_test()
def test_the_id_must_be_two_parts():
    with pytest.raises(MetadataError, match="two-part"):
        parse("Table ID: Order\nDescription: x\nLineage: y")


@weaver_test()
def test_duplicate_keys_are_refused():
    with pytest.raises(MetadataError, match="duplicate"):
        parse(FOLDER_YAML + "\nDescription: again")


# --- unknown keys ----------------------------------------------------------


@weaver_test()
def test_unknown_keys_are_named_not_ignored():
    """A mistyped 'Primary Key' must not parse as no primary key at all."""
    with pytest.raises(MetadataError, match="Primary Key"):
        parse(TABLE_YAML + "\nPrimary Key: Order id")


@weaver_test()
def test_a_key_from_another_kind_names_the_kinds_that_have_it():
    """Naming them turns "unknown key" into "wrong kind of declaration"."""
    with pytest.raises(
        MetadataError, match=r"Primary key belongs to Table, Test and View"
    ):
        parse(FOLDER_YAML + "\nPrimary key: Order id")


@weaver_test()
def test_retired_keys_explain_the_migration():
    with pytest.raises(MetadataError, match="Incremental"):
        parse(FOLDER_YAML + "\nAuto delete: true")


@weaver_test()
def test_load_mode_is_gone():
    with pytest.raises(MetadataError, match="Load mode"):
        parse(TABLE_YAML + "\nLoad mode: upsert")


# --- text, placeholders and references -------------------------------------


@weaver_test()
def test_description_and_lineage_are_required():
    with pytest.raises(MetadataError, match="Description"):
        parse("Table ID: A.B\nLineage: y")


@weaver_test()
def test_placeholders_are_refused():
    with pytest.raises(MetadataError, match="placeholder"):
        parse("Folder ID: A.B\nDescription: TBD\nLineage: y\nFile key: '*'")


@weaver_test()
def test_a_whole_value_reference_is_a_reference():
    document = parse(
        TABLE_YAML.replace(
            "Description: One row per customer order.",
            "Description: $Sales.OrderSource",
        )
    )
    assert document.description.is_reference
    assert document.description.reference.object_id.qualified == "Sales.OrderSource"
    assert document.description.reference.column is None


@weaver_test()
def test_a_column_reference_carries_the_column():
    document = parse(
        TABLE_YAML.replace(
            "Description: One row per customer order.",
            "Description: $Sales.OrderSource[Order date]",
        )
    )
    assert document.description.reference.column == "Order date"


@weaver_test()
def test_mixed_prose_and_reference_is_refused():
    """A contract that is only sometimes machine-readable is not a contract."""
    with pytest.raises(MetadataError, match="not a mix"):
        parse(
            TABLE_YAML.replace(
                "Description: One row per customer order.",
                "Description: See $Sales.OrderSource",
            )
        )


@weaver_test()
def test_a_literal_dollar_can_be_escaped():
    document = parse(
        TABLE_YAML.replace(
            "Description: One row per customer order.",
            "Description: Amounts are in $$AUD.",
        )
    )
    assert document.description.literal == "Amounts are in $AUD."
    assert not document.description.is_reference


# --- notes and revision notes ----------------------------------------------


@weaver_test()
def test_notes_are_free_range():
    """Unpoliced by design — no reference parsing, no placeholder rules."""
    document = parse(
        TABLE_YAML + "\nNotes: |\n  Amounts are $AUD.\n  TBD whether tax is included."
    )
    assert document.notes.startswith("Amounts are $AUD.")


@weaver_test()
def test_notes_must_not_be_blank_when_present():
    with pytest.raises(MetadataError, match="Notes"):
        parse(TABLE_YAML + "\nNotes: '   '")


@weaver_test()
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
@weaver_test()
def test_any_consistent_date_spelling_is_accepted(entry, shape):
    document = parse(TABLE_YAML + f"\nRevision notes:\n  - {entry}")
    assert document.revision_date_format == shape


@weaver_test()
def test_mixing_date_formats_within_an_object_is_refused():
    with pytest.raises(MetadataError, match="mix date formats"):
        parse(
            TABLE_YAML
            + "\nRevision notes:\n  - 2026-07-23 first\n  - 24/07/2026 second"
        )


@weaver_test()
def test_month_first_and_day_first_are_the_same_shape():
    """Indistinguishable, so Weaver checks the shape rather than the reading."""
    document = parse(
        TABLE_YAML + "\nRevision notes:\n  - 07/23/2026 first\n  - 24/07/2026 second"
    )
    assert document.revision_date_format == "DD/MM/YYYY"


@weaver_test()
def test_an_entry_without_a_date_is_refused():
    with pytest.raises(MetadataError, match="must open with a date"):
        parse(TABLE_YAML + "\nRevision notes:\n  - Added the amount column.")


@weaver_test()
def test_an_entry_with_a_date_but_no_note_is_refused():
    with pytest.raises(MetadataError, match="no note"):
        parse(TABLE_YAML + "\nRevision notes:\n  - 2026-07-23")


@weaver_test()
def test_an_impossible_date_is_refused():
    with pytest.raises(MetadataError, match="real date"):
        parse(TABLE_YAML + "\nRevision notes:\n  - 2026-13-45 nonsense")


@weaver_test()
def test_revision_notes_must_be_a_list():
    with pytest.raises(MetadataError, match="YAML list"):
        parse(TABLE_YAML + "\nRevision notes: 2026-07-23 one note")


@weaver_test()
def test_notes_and_revision_notes_apply_to_every_kind():
    document = parse(
        FOLDER_YAML + "\nNotes: Free text.\nRevision notes:\n  - 2026-07-23 Created."
    )
    assert document.notes == "Free text."
    assert len(document.revision_notes) == 1


# --- column set versus column list -----------------------------------------


@weaver_test()
def test_a_column_set_is_comma_separated():
    document = parse(
        TABLE_YAML.replace("Primary key: Order id", "Primary key: Order id, Order date")
    )
    assert document.primary_key == ("Order id", "Order date")


@weaver_test()
def test_a_column_set_refuses_a_yaml_list():
    with pytest.raises(MetadataError, match="column set"):
        parse(TABLE_YAML.replace("Primary key: Order id", "Primary key:\n  - Order id"))


@weaver_test()
def test_a_column_list_is_a_yaml_list():
    document = parse(TABLE_YAML + "\nNot null:\n  - Order date\n  - Amount")
    assert document.declared_not_null == ("Order date", "Amount")


@weaver_test()
def test_a_column_list_refuses_comma_separated_text():
    with pytest.raises(MetadataError, match="YAML list"):
        parse(TABLE_YAML + "\nNot null: Order date, Amount")


# --- cross-column guards ---------------------------------------------------


@weaver_test()
def test_columns_must_exist_in_schema():
    with pytest.raises(MetadataError, match="not in Schema"):
        parse(TABLE_YAML.replace("Primary key: Order id", "Primary key: Ordr id"))


@weaver_test()
def test_not_null_repeating_the_primary_key_is_refused():
    with pytest.raises(MetadataError, match="already not null"):
        parse(TABLE_YAML + "\nNot null:\n  - Order id")


@weaver_test()
def test_comparison_columns_may_not_include_the_key():
    with pytest.raises(MetadataError, match="equal keys by definition"):
        parse(TABLE_YAML + "\nComparison columns: Order id, Amount")


@weaver_test()
def test_comparison_columns_require_a_primary_key():
    without_key = TABLE_YAML.replace("Primary key: Order id\n", "")
    with pytest.raises(MetadataError, match="require a Primary key"):
        parse(without_key + "\nComparison columns: Amount")


@weaver_test()
def test_incremental_requires_a_primary_key():
    without_key = TABLE_YAML.replace("Primary key: Order id\n", "")
    with pytest.raises(MetadataError, match="requires a Primary key"):
        parse(without_key + "\nIncremental: true")


@weaver_test()
def test_static_and_incremental_stand_together():
    """Incremental shapes the load; Static decides whether it is invoked."""
    document = parse(TABLE_YAML + "\nIncremental: true\nStatic: true")
    assert document.is_incremental
    assert document.static

    folder = parse(FOLDER_YAML + "\nStatic: true")
    assert folder.is_incremental
    assert folder.static


@weaver_test()
def test_audit_column_names_are_reserved():
    with pytest.raises(MetadataError, match="reserved"):
        parse(TABLE_YAML + "\n  Row_insert_datetime: timestamp")


@weaver_test()
def test_identity_is_a_single_column():
    with pytest.raises(MetadataError, match="single column"):
        parse(TABLE_YAML + "\nIdentity: Order id, Order date")


@weaver_test()
def test_identity_is_an_engine_generated_bigint_column():
    """The Identity header names a surrogate the Warehouse generates: a not-null
    bigint outside the business schema, which no load ever inserts into."""
    document = parse(
        "Table ID: Sales.Order\nDescription: x\nLineage: y\n"
        "Primary key: Order id\nIdentity: OrderKey\n"
        "Schema:\n  Order id: string\n  Amount: decimal(18,2)\n",
        language=SQL,
    )
    assert document.identity == "OrderKey"
    identity = document.identity_column
    assert (identity.type, identity.not_null, identity.is_identity) == (
        "bigint",
        True,
        True,
    )
    # It leads the effective schema and is not one of the declared columns.
    assert document.effective_schema[0].name == "OrderKey"
    assert "OrderKey" not in {column.name for column in document.schema}


@weaver_test()
def test_identity_must_not_be_declared_in_schema():
    """The identity column is Weaver's own, so declaring it is a collision.

    Named on a non-key column, so this isolates the schema collision from the
    separate rule that the primary key may not be the identity.
    """

    with pytest.raises(MetadataError, match="must not appear in Schema"):
        parse(TABLE_YAML + "\nIdentity: Amount", language=SQL)


@weaver_test()
def test_the_primary_key_may_not_be_the_identity_column():
    """A load could never match on it, so every run would insert duplicates.

    The engine assigns the identity on insert, so no source produces it. Caught
    at parse because the alternative is an "Invalid column name" from the engine
    at install, which says nothing about what the declaration got wrong.
    """

    with pytest.raises(MetadataError, match="Primary key names the Identity column"):
        parse(
            "Table ID: Sales.Order\nDescription: x\nLineage: y\n"
            "Primary key: OrderKey\nIdentity: OrderKey\n"
            "Schema:\n  Amount: decimal(18,2)\n",
            language=SQL,
        )


@pytest.mark.parametrize("language", [PYTHON, SPARK_SQL])
@weaver_test()
def test_a_delta_table_may_not_declare_identity(language):
    """Identity is a Warehouse declaration.

    Native generation is the whole value of the column, and no Delta version
    Weaver runs on provides it — so a Delta table declares none rather than
    carrying a column Weaver would have to populate itself and could not
    promise to keep unique.
    """

    with pytest.raises(MetadataError, match="Warehouse tables only"):
        parse(
            "Table ID: Sales.Order\nDescription: x\nLineage: y\nDependencies: []\n"
            "Primary key: OrderKey\nIdentity: OrderKey\n"
            "Schema:\n  Amount: decimal(18,2)\n",
            language=language,
        )


# --- defaults --------------------------------------------------------------


@weaver_test()
def test_folder_defaults_to_incremental_and_prohibited_rebuild():
    document = parse(FOLDER_YAML)
    assert document.kind == FOLDER
    assert document.is_incremental is True
    assert document.prohibit_rebuild is True


@weaver_test()
def test_a_table_defaults_to_neither():
    document = parse(TABLE_YAML)
    assert document.is_incremental is False
    assert document.prohibit_rebuild is False


@weaver_test()
def test_prohibit_rebuild_works_on_views():
    """Admins add security to views; a rebuild would lose it."""
    document = parse(
        "View ID: Sales.OrderView\nDescription: x\nLineage: y\nProhibit rebuild: true",
        language=SQL,
    )
    assert document.kind == VIEW
    assert document.prohibit_rebuild is True


@weaver_test()
def test_incremental_is_refused_on_a_view():
    with pytest.raises(MetadataError, match="View"):
        parse(
            "View ID: A.B\nDescription: x\nLineage: y\nIncremental: true", language=SQL
        )


@weaver_test()
def test_not_null_includes_the_primary_key():
    document = parse(TABLE_YAML + "\nNot null:\n  - Order date")
    assert document.not_null == ("Order id", "Order date")


@weaver_test()
def test_comparison_columns_default_to_every_non_key_column():
    assert parse(TABLE_YAML).comparison_columns == ("Order date", "Amount")


@weaver_test()
def test_a_narrower_comparison_set_is_kept():
    document = parse(TABLE_YAML + "\nComparison columns: Order date")
    assert document.comparison_columns == ("Order date",)


# --- audit columns ---------------------------------------------------------


@weaver_test()
def test_declared_schema_stays_exactly_what_was_written():
    document = parse(TABLE_YAML)
    assert [column.name for column in document.schema] == [
        "Order id",
        "Order date",
        "Amount",
    ]


@weaver_test()
def test_the_effective_schema_adds_the_audit_columns():
    document = parse(TABLE_YAML)
    assert [column.name for column in document.effective_schema][-4:] == [
        "row_insert_datetime",
        "row_update_datetime",
        "row_delete_datetime",
        "row_signature",
    ]


@weaver_test()
def test_a_warehouse_table_keeps_the_spaced_audit_names():
    document = parse(
        "Table ID: Sales.Order\nDescription: x\nLineage: y\nPrimary key: Order id",
        language=SQL,
    )
    assert [column.name for column in document.audit_columns] == list(AUDIT_COLUMNS)


@weaver_test()
def test_every_audit_column_is_not_null():
    """Weaver populates all three on every loaded row, so none may be null."""
    document = parse(TABLE_YAML)
    assert [column.not_null for column in document.audit_columns] == [True, True, True]


@weaver_test()
def test_folders_have_no_audit_columns():
    assert parse(FOLDER_YAML).audit_columns == ()


# --- the row signature column ----------------------------------------------


@weaver_test()
def test_a_keyed_table_carries_a_row_signature_column():
    """Spelled and typed for the representation.

    A Warehouse keeps the digest as bytes; Spark's ``sha2`` returns hex text, so
    Delta keeps that. The two are never compared with each other.
    """

    delta = parse(TABLE_YAML).signature_column
    warehouse = parse(TABLE_YAML, language=SQL).signature_column

    assert (delta.name, delta.type) == ("row_signature", "string")
    assert (warehouse.name, warehouse.type) == ("Row signature", "varbinary(32)")


@weaver_test()
def test_the_row_signature_column_is_not_null():
    """A load computes it for every row it writes, so there is no absent state."""

    assert parse(TABLE_YAML).signature_column.not_null is True


@weaver_test()
def test_a_table_with_no_load_carries_no_row_signature():
    """The signature serves a load, so a table without one has nothing to keep.

    Weaver's own catalogue tables are written by the catalogue's DML. Giving them
    the column made every catalogue publication fail on a not-null violation, and
    ``Prohibit rebuild: true`` meant an installed one could never acquire it.
    """

    source = TABLE_YAML + "\nHas load procedure: false\n"

    assert parse(source).signature_column is None
    assert parse(source).has_load_procedure is False
    assert parse(TABLE_YAML).has_load_procedure is True


@weaver_test()
def test_an_unkeyed_table_carries_no_row_signature_column():
    """Its load replaces the target wholesale, so no row is ever compared."""

    document = parse(TABLE_YAML.replace("Primary key: Order id\n\n", ""), language=SQL)

    assert document.signature_column is None
    assert document.internal_columns == document.audit_columns


@weaver_test()
def test_a_folder_carries_no_row_signature_column():
    assert parse(FOLDER_YAML).signature_column is None


@weaver_test()
def test_the_row_signature_column_is_not_a_business_or_comparison_column():
    """Weaver's own column: no query produces it and no change is measured by it."""

    document = parse(TABLE_YAML)

    assert "row_signature" not in [column.name for column in document.schema]
    assert "row_signature" not in document.comparison_columns
    assert "row_signature" not in document.not_null


@weaver_test()
def test_a_declaration_cannot_name_the_row_signature_column():
    for spelling in ("Row signature", "row_signature", "ROW SIGNATURE"):
        source = TABLE_YAML.replace("  Amount: decimal(18,2)", f"  {spelling}: string")
        with pytest.raises(MetadataError, match="reserved for Weaver's row signature"):
            parse(source)


@weaver_test()
def test_an_identity_cannot_name_the_row_signature_column():
    source = TABLE_YAML.replace(
        "Primary key: Order id", "Primary key: Order id\n\nIdentity: Row signature"
    )
    with pytest.raises(MetadataError, match="row signature column name"):
        parse(source, language=SQL)


# --- schema declaration by representation ----------------------------------


@weaver_test()
def test_a_delta_table_must_declare_schema():
    with pytest.raises(MetadataError, match="must declare Schema"):
        parse("Table ID: A.B\nDescription: x\nLineage: y")


@weaver_test()
def test_a_warehouse_table_may_declare_schema():
    """T-SQL tables may declare a schema; when they do it is authoritative and
    validated now, exactly like a Delta table's."""
    document = parse(TABLE_YAML, language=SQL)
    assert document.has_declared_schema is True
    assert document.defers_column_validation is False
    assert [column.name for column in document.schema] == [
        "Order id",
        "Order date",
        "Amount",
    ]


@weaver_test()
def test_a_warehouse_table_defers_column_validation():
    document = parse(
        "Table ID: Sales.Order\nDescription: x\nLineage: y\nPrimary key: Order id",
        language=SQL,
    )
    assert document.defers_column_validation is True
    assert document.primary_key == ("Order id",)


@weaver_test()
def test_a_delta_table_validates_now():
    assert parse(TABLE_YAML).defers_column_validation is False


# --- column notes ----------------------------------------------------------


@weaver_test()
def test_column_notes_attach_to_declared_columns():
    document = parse(
        TABLE_YAML + "\nColumn notes:\n  Amount: Order total including tax."
    )
    amount = next(column for column in document.schema if column.name == "Amount")
    assert amount.note.literal == "Order total including tax."


@weaver_test()
def test_column_notes_may_reference_another_object():
    document = parse(TABLE_YAML + "\nColumn notes:\n  Amount: $Sales.Invoice[Amount]")
    amount = next(column for column in document.schema if column.name == "Amount")
    assert amount.note.reference.column == "Amount"


@weaver_test()
def test_column_notes_must_name_declared_columns():
    with pytest.raises(MetadataError, match="not in Schema"):
        parse(TABLE_YAML + "\nColumn notes:\n  Amont: typo")


@weaver_test()
def test_a_warehouse_object_describes_columns_without_a_schema():
    document = parse(
        "Table ID: Sales.Order\nDescription: x\nLineage: y\n"
        "Column notes:\n  Amount: Order total.",
        language=SQL,
    )
    assert document.defers_column_validation is True


# --- folders ---------------------------------------------------------------


@weaver_test()
def test_a_folder_must_declare_file_keys():
    with pytest.raises(MetadataError, match="File key"):
        parse("Folder ID: A.B\nDescription: x\nLineage: y")


@weaver_test()
def test_file_keys_may_not_traverse():
    with pytest.raises(MetadataError, match="traverse"):
        parse(FOLDER_YAML.replace('File key: "*.csv"', 'File key: "../*.csv"'))


@weaver_test()
def test_not_null_columns_are_marked_on_the_schema():
    document = parse(TABLE_YAML + "\nNot null:\n  - Amount")
    marked = {column.name for column in document.schema if column.not_null}
    assert marked == {"Order id", "Amount"}


# --- extraction ------------------------------------------------------------


@weaver_test()
def test_python_metadata_comes_from_the_module_docstring():
    source = f'"""{TABLE_YAML}"""\n\nclass Order:\n    pass\n'
    assert parse_python_document(source).qualified == "Sales.Order"


@weaver_test()
def test_a_python_object_without_a_docstring_is_refused():
    with pytest.raises(MetadataError, match="docstring"):
        parse_python_document("class Order:\n    pass\n")


@weaver_test()
def test_sql_metadata_comes_from_the_opening_comment():
    source = (
        "/*\nTable ID: Sales.Order\nDescription: x\nLineage: y\n*/\n"
        "select 1 as [Order id]\n"
    )
    document, body = parse_sql_document(source)
    assert document.qualified == "Sales.Order"
    assert body.startswith("select 1")


@weaver_test()
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


@weaver_test()
def test_a_spark_sql_table_parses():
    document = parse(SPARK_YAML, language=SPARK_SQL)
    assert document.language == SPARK_SQL
    assert document.dependencies[0].qualified == "Sales.Order"


@weaver_test()
def test_a_spark_sql_table_may_omit_schema():
    """Unlike Python, a Spark SQL table has a query, so it may omit Schema and
    take its shape from the query at build (how-does-build-work §2)."""
    without = SPARK_YAML.split("Schema:")[0]
    document = parse(without, language=SPARK_SQL)
    assert document.has_declared_schema is False
    assert document.defers_column_validation is True
    # Metadata that names columns is still parsed; it is validated at build.
    assert document.primary_key == ("Customer id",)


@weaver_test()
def test_a_spark_sql_table_may_declare_schema():
    """When a Spark SQL table declares a schema it is authoritative, like Python's."""
    document = parse(SPARK_YAML, language=SPARK_SQL)
    assert document.has_declared_schema is True
    assert document.defers_column_validation is False


@weaver_test()
def test_a_spark_sql_object_must_declare_dependencies():
    """Its query may read by path, which cannot resolve back to an object."""
    without = SPARK_YAML.replace("Dependencies:\n  - Sales.Order\n", "")
    with pytest.raises(MetadataError, match="must declare Dependencies"):
        parse(without, language=SPARK_SQL)


@weaver_test()
def test_a_spark_sql_table_uses_the_delta_audit_spelling():
    document = parse(SPARK_YAML, language=SPARK_SQL)
    assert [column.name for column in document.audit_columns] == [
        "row_insert_datetime",
        "row_update_datetime",
        "row_delete_datetime",
    ]


@weaver_test()
def test_a_spark_sql_view_is_a_real_object():
    """Fabric Lakehouse views persist in the metastore."""
    document = parse(
        "View ID: Sales.OrderView\nDescription: x\nLineage: y\n"
        "Dependencies:\n  - Sales.Order",
        language=SPARK_SQL,
    )
    assert document.kind == VIEW
    assert document.audit_columns == ()


@weaver_test()
def test_dependencies_are_optional_for_python_and_sql():
    assert parse(TABLE_YAML).dependencies == ()


@weaver_test()
def test_declared_dependencies_are_two_part_names():
    with pytest.raises(MetadataError, match="two-part"):
        parse(TABLE_YAML + "\nDependencies:\n  - Order")


@weaver_test()
def test_an_object_may_not_depend_on_itself():
    with pytest.raises(MetadataError, match="cannot depend on itself"):
        parse(TABLE_YAML + "\nDependencies:\n  - Sales.Order")


@weaver_test()
def test_dependencies_may_not_repeat():
    with pytest.raises(MetadataError, match="repeats"):
        parse(TABLE_YAML + "\nDependencies:\n  - Sales.Customer\n  - Sales.Customer")


@weaver_test()
def test_dependencies_must_be_a_list():
    with pytest.raises(MetadataError, match="YAML list"):
        parse(TABLE_YAML + "\nDependencies: Sales.Customer")


# --- validation declarations -----------------------------------------------
#
# A Test and an Assumption are Weaver declarations that produce no data. The
# contract is therefore mostly about what they may *not* say: everything
# describing how data is materialised, keyed or rebuilt reads as plausible on a
# Test until you ask what it would do, so each is refused by name.

TEST_YAML = """
Test ID: Sales.OrdersReconcile

Description: Orders reconcile to the independently derived expected relation.

Primary key: Order id
"""

ASSUMPTION_YAML = """
Assumption ID: Sales.OrdersUpToDate

Description: Orders contain data up to the expected business date.
"""


@weaver_test()
def test_a_test_parses():
    document = parse(TEST_YAML)
    assert document.kind == TEST
    assert document.qualified == "Sales.OrdersReconcile"
    assert document.primary_key == ("Order id",)
    assert document.is_validation


@weaver_test()
def test_an_assumption_parses():
    document = parse(ASSUMPTION_YAML)
    assert document.kind == ASSUMPTION
    assert document.qualified == "Sales.OrdersUpToDate"
    assert document.is_validation


@weaver_test()
def test_a_validation_declares_no_lineage():
    """It reads data and produces none, so it has no lineage of its own."""
    assert parse(TEST_YAML).lineage is None
    assert parse(ASSUMPTION_YAML).lineage is None


@weaver_test()
def test_a_test_primary_key_may_be_composite():
    document = parse(
        TEST_YAML.replace("Primary key: Order id", "Primary key: Order id, Line no")
    )
    assert document.primary_key == ("Order id", "Line no")


@weaver_test()
def test_a_test_may_declare_no_primary_key():
    document = parse(TEST_YAML.replace("\nPrimary key: Order id\n", "\n"))
    assert document.primary_key == ()
    assert not document.has_primary_key


@weaver_test()
def test_an_assumption_may_not_declare_a_primary_key():
    """There is one side to pair, so a key would have nothing to correlate."""
    with pytest.raises(MetadataError, match="must not declare a Primary key"):
        parse(ASSUMPTION_YAML + "\nPrimary key: Order id")


@weaver_test()
def test_a_validation_takes_the_shared_document_keys():
    document = parse(
        TEST_YAML
        + "\nNotes: Slow against a full year.\n"
        + "\nRevision notes:\n  - 2026-08-08 First cut.\n"
        + "\nDependencies:\n  - Sales.Order\n"
    )
    assert document.notes == "Slow against a full year."
    assert [str(revision) for revision in document.revision_notes] == [
        "2026-08-08 First cut."
    ]
    assert [dependency.qualified for dependency in document.dependencies] == [
        "Sales.Order"
    ]
    assert document.declares_dependencies


@weaver_test()
def test_a_validation_description_may_reference_another_object():
    document = parse(
        TEST_YAML.replace(
            "Description: Orders reconcile to the independently derived expected relation.",
            "Description: $Sales.Order",
        )
    )
    assert document.description.reference.object_id.qualified == "Sales.Order"


@pytest.mark.parametrize(
    "key",
    [
        "Lineage: Sales system order export.",
        "Static: true",
        "Prohibit rebuild: true",
        "Incremental: true",
        "Comparison columns: Amount",
        "Identity: Order key",
        "Not null:\n  - Amount",
        "Unique keys:\n  - Order id",
        "Foreign keys:\n  - Customer id: Sales.Customer[Customer id]",
        "Delete percentage threshold: 10",
        "Warehouse alias: Sales.OrdersReconcile",
        "Schema:\n  Order id: string",
        'File key: "*.csv"',
    ],
)
@weaver_test()
def test_data_object_metadata_is_refused_on_a_test(key):
    with pytest.raises(MetadataError, match="unknown metadata key"):
        parse(TEST_YAML + "\n" + key)


@pytest.mark.parametrize(
    "key",
    ["Lineage: Sales system order export.", "Static: true", "Incremental: true"],
)
@weaver_test()
def test_data_object_metadata_is_refused_on_an_assumption(key):
    with pytest.raises(MetadataError, match="unknown metadata key"):
        parse(ASSUMPTION_YAML + "\n" + key)


@weaver_test()
def test_refusing_a_data_key_on_a_validation_explains_why():
    with pytest.raises(MetadataError, match="declares no data of its own"):
        parse(TEST_YAML + "\nLineage: Sales system order export.")


@weaver_test()
def test_a_validation_still_needs_a_description():
    with pytest.raises(MetadataError, match="Description is required"):
        parse("Test ID: Sales.OrdersReconcile")


@weaver_test()
def test_a_validation_id_is_two_parts():
    with pytest.raises(MetadataError, match="two-part"):
        parse("Test ID: OrdersReconcile\nDescription: x")


@weaver_test()
def test_one_id_key_only_still_holds_across_validation_and_object():
    with pytest.raises(MetadataError, match="exactly one"):
        parse("Test ID: A.B\nAssumption ID: A.C\nDescription: x")
    with pytest.raises(MetadataError, match="exactly one"):
        parse("Table ID: A.B\nTest ID: A.C\nDescription: x\nLineage: y")


@weaver_test()
def test_a_spark_sql_validation_need_not_declare_dependencies():
    """Unlike a Spark SQL object, whose declaration replaces inference entirely.

    A validation's supplements it, so a header naming nothing leaves the
    inferred graph intact rather than emptying it.
    """
    document = parse(TEST_YAML, language=SPARK_SQL)
    assert document.dependencies == ()
    assert not document.declares_dependencies


@weaver_test()
def test_a_validation_may_still_declare_what_inference_cannot_reach():
    document = parse(TEST_YAML + "\nDependencies:\n  - Sales.Order", language=SPARK_SQL)
    assert [dependency.qualified for dependency in document.dependencies] == [
        "Sales.Order"
    ]


@weaver_test()
def test_a_validation_may_not_depend_on_itself():
    with pytest.raises(MetadataError, match="cannot depend on itself"):
        parse(TEST_YAML + "\nDependencies:\n  - Sales.OrdersReconcile")
