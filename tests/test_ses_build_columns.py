"""The shared build-time column model — the deferred twin of parse-time guards.

An inferred SQL table cannot have its column-referencing metadata checked until
the query's output shape is known, which is at build. These tests pin the rules
:mod:`weaver.ses.columns` enforces there, case-insensitively, for both declared
and inferred tables. The T-SQL template mirrors the same rules server-side.
"""

from __future__ import annotations

import textwrap

import pytest

from weaver.errors import BuildError
from weaver.ses import SPARK_SQL, SQL, parse_document
from weaver.ses.columns import metadata_column_references, resolve_build_columns


def _doc(yaml_text: str, *, language: str = SQL):
    return parse_document(textwrap.dedent(yaml_text).lstrip(), language=language)


INFERRED = """
Table ID: Sales.Order
Description: x
Lineage: y
Primary key: Order id
Comparison columns: Amount
Column notes:
  Amount: Order total.
"""

DECLARED = """
Table ID: Sales.Order
Description: x
Lineage: y
Primary key: Order id
Schema:
  Order id: string
  Amount: decimal(18,2)
"""


# --- inferred tables --------------------------------------------------------


def test_inferred_business_columns_are_the_query_columns():
    document = _doc(INFERRED)
    columns = resolve_build_columns(document, ("Order id", "Amount"))
    assert columns == ("Order id", "Amount")


def test_inferred_reference_check_is_case_sensitive():
    document = _doc(INFERRED)
    # PK "Order id" is a case-sensitive contract; a lowercase query column is a
    # different column and must not satisfy it.
    with pytest.raises(BuildError, match="Primary key names column 'Order id'"):
        resolve_build_columns(document, ("order id", "Amount"))


def test_a_primary_key_naming_a_missing_column_fails_at_build():
    document = _doc(INFERRED)
    with pytest.raises(BuildError, match="Primary key names column 'Order id'"):
        resolve_build_columns(document, ("Amount",))


def test_a_comparison_column_naming_a_missing_column_fails_at_build():
    document = _doc(
        """
        Table ID: Sales.Order
        Description: x
        Lineage: y
        Primary key: Order id
        Comparison columns: Nope
        """
    )
    with pytest.raises(BuildError, match="Comparison columns names column 'Nope'"):
        resolve_build_columns(document, ("Order id", "Amount"))


def test_a_column_note_naming_a_missing_column_fails_at_build():
    document = _doc(
        """
        Table ID: Sales.Order
        Description: x
        Lineage: y
        Column notes:
          Ghost: nothing here
        """
    )
    with pytest.raises(BuildError, match="Column notes names column 'Ghost'"):
        resolve_build_columns(document, ("Order id",))


def test_an_identity_naming_a_missing_column_fails_at_build():
    document = _doc(
        """
        Table ID: Sales.Order
        Description: x
        Lineage: y
        Identity: Surrogate
        """
    )
    with pytest.raises(BuildError, match="Identity names column 'Surrogate'"):
        resolve_build_columns(document, ("Order id",))


def test_columns_that_collide_only_by_case_are_ambiguous():
    document = _doc(
        "Table ID: Sales.Order\nDescription: x\nLineage: y"
    )
    with pytest.raises(BuildError, match="collide by name"):
        resolve_build_columns(document, ("Amount", "amount"))


# --- declared tables --------------------------------------------------------


def test_declared_columns_are_authoritative_and_order_is_kept():
    document = _doc(DECLARED)
    # Query returns them in a different order; declared order wins.
    assert resolve_build_columns(document, ("Amount", "Order id")) == (
        "Order id",
        "Amount",
    )


def test_a_declared_column_missing_from_the_query_fails():
    document = _doc(DECLARED)
    with pytest.raises(BuildError, match="not returned by the query under the same case: Amount"):
        resolve_build_columns(document, ("Order id",))


def test_an_undeclared_query_column_fails():
    document = _doc(DECLARED)
    with pytest.raises(BuildError, match="not in the declared schema"):
        resolve_build_columns(document, ("Order id", "Amount", "Extra"))


def test_declared_equivalence_ignores_order_but_not_case():
    document = _doc(DECLARED)
    # Order may differ; the declared order still wins.
    assert resolve_build_columns(document, ("Amount", "Order id")) == (
        "Order id",
        "Amount",
    )


def test_declared_equivalence_requires_exact_case():
    document = _doc(DECLARED)
    # The query spells them differently; declared "Order id"/"Amount" are not met.
    with pytest.raises(BuildError, match="not returned by the query under the same case"):
        resolve_build_columns(document, ("amount", "order id"))


# --- the shared reference set ----------------------------------------------


def test_reference_set_covers_every_column_naming_field_when_inferred():
    document = _doc(INFERRED)
    references = metadata_column_references(document)
    assert ("Primary key", "Order id") in references
    assert ("Comparison columns", "Amount") in references
    assert ("Column notes", "Amount") in references


def test_reference_set_reads_declared_notes_from_the_schema():
    document = _doc(
        DECLARED + "Column notes:\n  Amount: Order total.\n"
    )
    references = metadata_column_references(document)
    assert ("Column notes", "Amount") in references
    assert ("Primary key", "Order id") in references


def test_spark_inferred_tables_use_the_same_model():
    document = parse_document(
        textwrap.dedent(
            """
            Table ID: Sales.OrderSummary
            Description: x
            Lineage: y
            Primary key: Customer id
            Dependencies:
              - Sales.Order
            """
        ).lstrip(),
        language=SPARK_SQL,
    )
    assert resolve_build_columns(document, ("Customer id", "Total")) == (
        "Customer id",
        "Total",
    )
    with pytest.raises(BuildError, match="Primary key names column 'Customer id'"):
        resolve_build_columns(document, ("Total",))
