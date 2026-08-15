"""Unique keys and foreign keys — the semantic model, not physical constraints.

These declarations create nothing. No index is built and no constraint is
enforced; they record that a column set identifies a row, or that it means
another object's row. The catalogue projects them so a reader can see the model,
and later quality checks can ask whether it is complete.

Because they are semantic, they carry no names, several relationships may run
between the same pair of objects, and an object may reference itself.
"""

from __future__ import annotations

import textwrap

import pytest

from weaver.declaration import PYTHON, SQL, ObjectId, parse_document
from weaver.errors import MetadataError

TABLE = """
Table ID: Sales.Order

Description: One row per customer order.

Lineage: Sales system order export.

Primary key: Order id

Schema:
  Order id: string
  Order number: string
  Customer id: string
  Order date: date
  Parent order id: string
  Region: string
  Country: string
"""

VIEW = """
View ID: Reporting.OrderView

Description: Orders shaped for reporting.

Lineage: $Sales.Order
"""


def parse(*blocks: str, language: str = PYTHON):
    """Parse one or more YAML fragments, each dedented on its own.

    Fragments are written at their call site's indentation, so each is dedented
    separately before they are joined — a shared prefix across a constant and an
    indented literal would otherwise be no prefix at all.
    """

    return parse_document(
        "\n".join(textwrap.dedent(block) for block in blocks), language=language
    )


# --- unique keys ------------------------------------------------------------


def test_unique_keys_are_a_list_of_column_sets():
    document = parse(
        TABLE,
        """
        Unique keys:
          - Order number
          - Customer id, Order date
        """,
    )
    assert document.unique_keys == (
        ("Order number",),
        ("Customer id", "Order date"),
    )


def test_a_unique_key_preserves_its_declared_column_order():
    document = parse(TABLE, "\nUnique keys:\n  - Country, Region\n")
    assert document.unique_keys == (("Country", "Region"),)


def test_unique_keys_must_be_a_list():
    with pytest.raises(MetadataError, match="non-empty YAML list"):
        parse(TABLE, "\nUnique keys: Order number\n")


def test_a_unique_key_is_not_a_nested_list():
    with pytest.raises(MetadataError, match="not a nested YAML list"):
        parse(TABLE, "\nUnique keys:\n  - - Order number\n    - Customer id\n")


def test_unique_keys_must_not_repeat_a_key():
    with pytest.raises(MetadataError, match="repeats the key"):
        parse(TABLE, "\nUnique keys:\n  - Order number\n  - Order number\n")


def test_a_unique_key_must_not_repeat_the_primary_key():
    with pytest.raises(MetadataError, match="already unique"):
        parse(TABLE, "\nUnique keys:\n  - Order id\n")


def test_unique_key_columns_must_be_in_the_schema():
    with pytest.raises(MetadataError, match="Unique keys names column"):
        parse(TABLE, "\nUnique keys:\n  - Nonexistent\n")


# --- foreign keys -----------------------------------------------------------


def test_a_foreign_key_pairs_this_object_s_columns_with_a_parent_s():
    document = parse(
        TABLE, "\nForeign keys:\n  - Customer id: Sales.Customer[Customer id]\n"
    )
    (key,) = document.foreign_keys
    assert key.columns == ("Customer id",)
    assert key.reference == ObjectId(schema="Sales", object="Customer")
    assert key.reference_columns == ("Customer id",)


def test_a_composite_foreign_key_keeps_both_column_orders():
    document = parse(
        TABLE,
        "\nForeign keys:\n  - Region, Country: Sales.Territory[Territory region, Territory country]\n",
    )
    (key,) = document.foreign_keys
    assert key.columns == ("Region", "Country")
    assert key.reference_columns == ("Territory region", "Territory country")


def test_an_object_may_reference_itself():
    document = parse(
        TABLE, "\nForeign keys:\n  - Parent order id: Sales.Order[Order id]\n"
    )
    (key,) = document.foreign_keys
    assert key.reference == ObjectId(schema="Sales", object="Order")


def test_two_relationships_may_run_to_the_same_parent():
    document = parse(
        TABLE,
        """
        Foreign keys:
          - Customer id: Sales.Customer[Customer id]
          - Region: Sales.Customer[Region]
        """,
    )
    assert len(document.foreign_keys) == 2
    assert {str(key.reference) for key in document.foreign_keys} == {"Sales.Customer"}


def test_a_foreign_key_has_no_name_so_an_identical_pair_is_a_duplicate():
    with pytest.raises(MetadataError, match="repeats the relationship"):
        parse(
            TABLE,
            """
            Foreign keys:
              - Customer id: Sales.Customer[Customer id]
              - Customer id: Sales.Customer[Customer id]
            """,
        )


def test_the_two_sides_must_be_the_same_size():
    with pytest.raises(MetadataError, match="the two sets must be the same size"):
        parse(TABLE, "\nForeign keys:\n  - Region, Country: Sales.Territory[Region]\n")


def test_the_parent_must_carry_its_columns():
    with pytest.raises(MetadataError, match=r"Schema.Object\[Column, Column\]"):
        parse(TABLE, "\nForeign keys:\n  - Customer id: Sales.Customer\n")


def test_the_parent_must_be_a_two_part_name():
    with pytest.raises(MetadataError, match=r"Schema.Object\[Column, Column\]"):
        parse(
            TABLE,
            "\nForeign keys:\n  - Customer id: Lakehouse.Sales.Customer[Customer id]\n",
        )


def test_each_entry_maps_one_column_set_to_one_parent():
    with pytest.raises(MetadataError, match="maps one column set to one parent"):
        parse(TABLE, "\nForeign keys:\n  - Sales.Customer[Customer id]\n")


def test_foreign_key_columns_must_be_in_the_schema():
    with pytest.raises(MetadataError, match="Foreign keys names column"):
        parse(TABLE, "\nForeign keys:\n  - Nonexistent: Sales.Customer[Customer id]\n")


def test_foreign_keys_must_be_a_list():
    with pytest.raises(MetadataError, match="non-empty YAML list"):
        parse(TABLE, "\nForeign keys: Sales.Customer\n")


# --- views ------------------------------------------------------------------


def test_a_view_declares_logical_keys():
    document = parse(
        VIEW,
        """
        Primary key: Order id

        Unique keys:
          - Order number

        Foreign keys:
          - Customer id: Sales.Customer[Customer id]
        """,
        language=SQL,
    )
    assert document.primary_key == ("Order id",)
    assert document.unique_keys == (("Order number",),)
    assert document.foreign_keys[0].columns == ("Customer id",)


def test_a_view_has_no_declared_schema_so_its_key_columns_defer_to_build():
    # No Schema key exists for a view, so nothing can be checked here. The
    # columns are checked against the built shape instead.
    document = parse(VIEW, "\nPrimary key: Whatever\n", language=SQL)
    assert document.primary_key == ("Whatever",)
    assert document.defers_column_validation


@pytest.mark.parametrize(
    "key", ["Comparison columns", "Identity", "Not null", "Incremental"]
)
def test_a_view_declares_nothing_that_implies_storage(key):
    value = "\n  - Order id" if key == "Not null" else " Order id"
    if key == "Incremental":
        value = " true"
    with pytest.raises(MetadataError):
        parse(VIEW + f"\n{key}:{value}\n", language=SQL)


def test_a_folder_declares_no_logical_keys():
    folder = """
    Folder ID: Sales.OrderExport

    Description: Raw order export files.

    Lineage: Nightly drop.

    File key: order_*.csv
    """
    with pytest.raises(MetadataError, match="unknown metadata key"):
        parse(folder, "\nUnique keys:\n  - Order id\n")


# --- the deferred build guard sees them -------------------------------------


def test_an_inferred_table_checks_its_key_columns_against_the_built_shape():
    from weaver.declaration.columns import metadata_column_references

    document = parse(
        """
        Table ID: Sales.Summary

        Description: Summary of orders.

        Lineage: $Sales.Order

        Dependencies:
          - Sales.Order

        Primary key: Customer id

        Unique keys:
          - Order number

        Foreign keys:
          - Region: Sales.Territory[Region]
        """,
        language="spark_sql",
    )
    references = metadata_column_references(document)
    assert ("Unique keys", "Order number") in references
    assert ("Foreign keys", "Region") in references
    # The parent's column is the parent's business, not this object's.
    assert not any(column == "Territory region" for _label, column in references)
