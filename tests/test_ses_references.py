"""Following a ``$Schema.Object`` metadata reference to the text it points at.

A reference means "the text over there is the text here", so resolving one is a
copy. What matters architecturally is that a *documentation* reference is not a
dependency: it names the object of that name, and the case it exists for is the
cross-target one — a Warehouse table saying it comes from the Delta table that
shares its ID.
"""

from __future__ import annotations

import pytest

from pathlib import Path

from weaver import LocalStore, Location
from weaver.errors import DiscoveryError
from weaver.ses import (
    IDENTITY_COLUMN_NOTE,
    declared_column_notes,
    read_repository,
    resolve_text,
)

FIXTURES = Location(value=str(Path(__file__).parent / "fixtures"))


def _write(root, name: str, text: str) -> None:
    (root / name).write_text(text, encoding="utf-8")


def _repo(tmp_path, files: dict[str, str], schemas=("Sales",)):
    root = tmp_path / "repo"
    root.mkdir()
    (root / "_schemas").mkdir()
    for schema in schemas:
        _write(root / "_schemas", f"{schema}.yml", f"Schema ID: {schema}\n")
    for name, text in files.items():
        _write(root, name, text)
    return read_repository(Location(value=str(root)), store=LocalStore(), name="repo")


PARENT = """\
/*
Table ID: Sales.Order

Description: One row per confirmed customer order.

Lineage: The sales system.

Dependencies: []

Schema:
  Order id: string
  Amount: decimal(18,2)

Column notes:
  Amount: Gross order amount, before discount.
*/
select cast(null as string) as `Order id`
     , cast(null as decimal(18,2)) as `Amount`
 where 1 = 0
"""


def test_literal_prose_resolves_to_itself_with_no_reference(tmp_path):
    repo = _repo(tmp_path, {"Sales.Order.spark.sql": PARENT})
    document = repo["Sales.Order"]
    resolved = resolve_text(
        document.document.description, owner=document, documents=repo.documents
    )
    assert resolved.literal == "One row per confirmed customer order."
    assert resolved.reference is None
    assert not resolved.is_reference


def test_a_reference_copies_the_target_s_description(tmp_path):
    child = PARENT.replace("Table ID: Sales.Order", "Table ID: Sales.OrderCopy").replace(
        "Description: One row per confirmed customer order.", "Description: $Sales.Order"
    )
    repo = _repo(
        tmp_path, {"Sales.Order.spark.sql": PARENT, "Sales.OrderCopy.spark.sql": child}
    )
    document = repo["Sales.OrderCopy"]
    resolved = resolve_text(
        document.document.description, owner=document, documents=repo.documents
    )
    assert resolved.literal == "One row per confirmed customer order."
    assert resolved.reference == "$Sales.Order"


def test_a_column_reference_copies_that_column_s_note(tmp_path):
    child = PARENT.replace("Table ID: Sales.Order", "Table ID: Sales.OrderCopy").replace(
        "Description: One row per confirmed customer order.",
        "Description: $Sales.Order[Amount]",
    )
    repo = _repo(
        tmp_path, {"Sales.Order.spark.sql": PARENT, "Sales.OrderCopy.spark.sql": child}
    )
    document = repo["Sales.OrderCopy"]
    resolved = resolve_text(
        document.document.description, owner=document, documents=repo.documents
    )
    assert resolved.literal == "Gross order amount, before discount."


def test_a_chain_is_followed_to_the_literal_at_its_end(tmp_path):
    middle = PARENT.replace("Table ID: Sales.Order", "Table ID: Sales.Middle").replace(
        "Description: One row per confirmed customer order.", "Description: $Sales.Order"
    )
    last = PARENT.replace("Table ID: Sales.Order", "Table ID: Sales.Last").replace(
        "Description: One row per confirmed customer order.", "Description: $Sales.Middle"
    )
    repo = _repo(
        tmp_path,
        {
            "Sales.Order.spark.sql": PARENT,
            "Sales.Middle.spark.sql": middle,
            "Sales.Last.spark.sql": last,
        },
    )
    document = repo["Sales.Last"]
    resolved = resolve_text(
        document.document.description, owner=document, documents=repo.documents
    )
    assert resolved.literal == "One row per confirmed customer order."
    # The reference recorded is the one written here, not the end of the chain.
    assert resolved.reference == "$Sales.Middle"


def test_a_cycle_is_an_error_because_it_has_no_text_to_copy(tmp_path):
    left = PARENT.replace("Table ID: Sales.Order", "Table ID: Sales.Left").replace(
        "Description: One row per confirmed customer order.", "Description: $Sales.Right"
    )
    right = PARENT.replace("Table ID: Sales.Order", "Table ID: Sales.Right").replace(
        "Description: One row per confirmed customer order.", "Description: $Sales.Left"
    )
    repo = _repo(
        tmp_path, {"Sales.Left.spark.sql": left, "Sales.Right.spark.sql": right}
    )
    document = repo["Sales.Left"]
    with pytest.raises(DiscoveryError, match="reference cycle"):
        resolve_text(document.document.description, owner=document, documents=repo.documents)


def test_an_unresolved_reference_keeps_the_pointer_and_reports_no_text(tmp_path):
    # A reference may legitimately name another repository's object. Refusing it
    # would cost a working object for a documentation nicety, so it is recorded.
    child = PARENT.replace("Table ID: Sales.Order", "Table ID: Sales.OrderCopy").replace(
        "Description: One row per confirmed customer order.", "Description: $Sales.Elsewhere"
    )
    repo = _repo(tmp_path, {"Sales.OrderCopy.spark.sql": child})
    document = repo["Sales.OrderCopy"]
    resolved = resolve_text(
        document.document.description, owner=document, documents=repo.documents
    )
    assert resolved.literal is None
    assert resolved.reference == "$Sales.Elsewhere"


def test_a_reference_resolves_across_targets_excluding_the_referrer_itself():
    """The sales-etl fixture's Warehouse Sales.Customer names $Sales.Customer.

    It means the Delta table of the same ID — it cannot sensibly mean itself.
    This is the case a documentation reference exists for, and the reason it is
    not resolved in the referrer's execution namespace the way a dependency is.
    """

    repo = read_repository(FIXTURES / "sales-etl", store=LocalStore(), name="sales-etl")
    warehouse = repo["sql:Sales.Customer"]
    resolved = resolve_text(
        warehouse.document.lineage, owner=warehouse, documents=repo.documents
    )
    assert resolved.reference == "$Sales.Customer"
    # Resolved to the Delta table's description, not to its own.
    assert resolved.literal == repo["delta:Sales.Customer"].document.description.literal


# --- what the column dictionary describes -----------------------------------


def test_declared_column_notes_are_the_columns_an_author_described(tmp_path):
    repo = _repo(tmp_path, {"Sales.Order.spark.sql": PARENT})
    notes = declared_column_notes(repo["Sales.Order"])
    assert [name for name, _note in notes] == ["Amount"]


def test_the_identity_column_gets_a_generic_note_no_author_writes(tmp_path):
    source = PARENT.replace(
        "Schema:", "Identity: Order key\n\nSchema:"
    )
    repo = _repo(tmp_path, {"Sales.Order.spark.sql": source})
    notes = declared_column_notes(repo["Sales.Order"])
    assert notes[0] == ("Order key", notes[0][1])
    assert notes[0][1].literal == IDENTITY_COLUMN_NOTE
    assert [name for name, _note in notes] == ["Order key", "Amount"]


def test_an_inferred_object_s_notes_come_from_its_raw_block(tmp_path):
    source = """\
/*
Table ID: Sales.Summary

Description: Order totals by customer.

Lineage: $Sales.Order

Dependencies: []

Column notes:
  Total amount: Sum of gross order amounts.
*/
select cast(null as string) as `Customer id`
     , cast(null as decimal(18,2)) as `Total amount`
 where 1 = 0
"""
    repo = _repo(tmp_path, {"Sales.Summary.spark.sql": source})
    notes = declared_column_notes(repo["Sales.Summary"])
    assert [name for name, _note in notes] == ["Total amount"]
    assert notes[0][1].literal == "Sum of gross order amounts."
