"""Following a ``$Schema.Object`` metadata reference to the text it points at.

A reference means "the text over there is the text here", so resolving one is a
copy. What matters architecturally is that a *documentation* reference is not a
dependency: it names the object of that name, and the case it exists for is the
cross-target one — a Warehouse table saying it comes from the Delta table that
shares its ID.
"""

from __future__ import annotations

import pytest

from weaver.declaration import (
    IDENTITY_COLUMN_NOTE,
    declared_column_notes,
    parse_item_repository,
    resolve_text,
)
from weaver.declaration.model import WeaverDocumentId
from weaver.errors import DiscoveryError
from weaver.locations import Location
from weaver.store import FilesystemStore

ITEM = "Lakehouse/Raw"


def _write(root, relative: str, text: str) -> None:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


class _Documents:
    """The item's documents, addressed by the local ``Schema.Object`` name."""

    def __init__(self, repository, item=ITEM):
        self.repository = repository
        self.item = item

    def __getitem__(self, qualified: str):
        return self.repository.source_documents[
            WeaverDocumentId.parse(f"{self.item}/{qualified}")
        ]

    @property
    def documents(self):
        return tuple(self.repository.source_documents.values())


def _repo(tmp_path, files: dict[str, str], schemas=("Sales",), item=ITEM):
    root = tmp_path / "repo"
    for schema in schemas:
        _write(root, f"{item}/schemas/{schema}.yml", f"Schema ID: {schema}\n")
    for name, text in files.items():
        _write(root, f"{item}/{name}", text)
    return _Documents(
        parse_item_repository(Location(value=str(root)), store=FilesystemStore()), item
    )


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
 where 1 = 0;
"""


def test_literal_prose_resolves_to_itself_with_no_reference(tmp_path):
    repo = _repo(tmp_path, {"Sales.Order.sql": PARENT})
    document = repo["Sales.Order"]
    resolved = resolve_text(
        document.document.description, owner=document, documents=repo.documents
    )
    assert resolved.literal == "One row per confirmed customer order."
    assert resolved.reference is None
    assert not resolved.is_reference


def test_a_reference_copies_the_target_s_description(tmp_path):
    child = PARENT.replace(
        "Table ID: Sales.Order", "Table ID: Sales.OrderCopy"
    ).replace(
        "Description: One row per confirmed customer order.",
        "Description: $Sales.Order",
    )
    repo = _repo(tmp_path, {"Sales.Order.sql": PARENT, "Sales.OrderCopy.sql": child})
    document = repo["Sales.OrderCopy"]
    resolved = resolve_text(
        document.document.description, owner=document, documents=repo.documents
    )
    assert resolved.literal == "One row per confirmed customer order."
    assert resolved.reference == "$Sales.Order"


def test_a_column_reference_copies_that_column_s_note(tmp_path):
    child = PARENT.replace(
        "Table ID: Sales.Order", "Table ID: Sales.OrderCopy"
    ).replace(
        "Description: One row per confirmed customer order.",
        "Description: $Sales.Order[Amount]",
    )
    repo = _repo(tmp_path, {"Sales.Order.sql": PARENT, "Sales.OrderCopy.sql": child})
    document = repo["Sales.OrderCopy"]
    resolved = resolve_text(
        document.document.description, owner=document, documents=repo.documents
    )
    assert resolved.literal == "Gross order amount, before discount."


def test_a_chain_is_followed_to_the_literal_at_its_end(tmp_path):
    middle = PARENT.replace("Table ID: Sales.Order", "Table ID: Sales.Middle").replace(
        "Description: One row per confirmed customer order.",
        "Description: $Sales.Order",
    )
    last = PARENT.replace("Table ID: Sales.Order", "Table ID: Sales.Last").replace(
        "Description: One row per confirmed customer order.",
        "Description: $Sales.Middle",
    )
    repo = _repo(
        tmp_path,
        {
            "Sales.Order.sql": PARENT,
            "Sales.Middle.sql": middle,
            "Sales.Last.sql": last,
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
        "Description: One row per confirmed customer order.",
        "Description: $Sales.Right",
    )
    right = PARENT.replace("Table ID: Sales.Order", "Table ID: Sales.Right").replace(
        "Description: One row per confirmed customer order.", "Description: $Sales.Left"
    )
    with pytest.raises(DiscoveryError, match="reference cycle"):
        _repo(tmp_path, {"Sales.Left.sql": left, "Sales.Right.sql": right})


def test_a_reference_that_names_nothing_is_rejected_when_read(tmp_path):
    """Exact identity means a dangling documentation pointer is an error.

    The flat model recorded it and moved on, because a reference could name
    another repository's object. Inside one workspace declaration there is no
    such elsewhere: every item is present, so a name that resolves to nothing is
    a mistake in the declaration.
    """

    child = PARENT.replace(
        "Table ID: Sales.Order", "Table ID: Sales.OrderCopy"
    ).replace(
        "Description: One row per confirmed customer order.",
        "Description: $Sales.Elsewhere",
    )
    with pytest.raises(DiscoveryError, match="does not resolve exactly"):
        _repo(tmp_path, {"Sales.OrderCopy.sql": child})


def test_a_reference_may_name_the_same_id_in_another_item(tmp_path):
    """A Warehouse table's lineage names the Lakehouse table it came from.

    The two share an ID and differ only by owning item, so the reference cannot
    sensibly mean the referrer itself. This is the case a documentation
    reference exists for, and the reason it is not resolved in the referrer's
    own execution namespace the way a dependency is.
    """

    root = tmp_path / "repo"
    _write(root, "Lakehouse/Raw/schemas/Sales.yml", "Schema ID: Sales\n")
    _write(root, "Warehouse/Reporting/schemas/Sales.yml", "Schema ID: Sales\n")
    _write(root, "Lakehouse/Raw/Sales.Order.sql", PARENT)
    warehouse_source = PARENT.replace(
        "Lineage: The sales system.", "Lineage: $Lakehouse/Raw/Sales.Order"
    ).replace("Dependencies: []\n\n", "")
    _write(root, "Warehouse/Reporting/Sales.Order.sql", warehouse_source)

    repository = parse_item_repository(
        Location(value=str(root)), store=FilesystemStore()
    )
    warehouse = repository.source_documents[
        WeaverDocumentId.parse("Warehouse/Reporting/Sales.Order")
    ]
    lakehouse = repository.source_documents[
        WeaverDocumentId.parse("Lakehouse/Raw/Sales.Order")
    ]
    resolved = resolve_text(
        warehouse.document.lineage,
        owner=warehouse,
        documents=tuple(repository.source_documents.values()),
    )
    assert resolved.reference == "$Lakehouse/Raw/Sales.Order"
    assert resolved.literal == lakehouse.document.description.literal


# --- what the column dictionary describes -----------------------------------


def test_declared_column_notes_are_the_columns_an_author_described(tmp_path):
    repo = _repo(tmp_path, {"Sales.Order.sql": PARENT})
    notes = declared_column_notes(repo["Sales.Order"])
    assert [name for name, _note in notes] == ["Amount"]


def test_the_identity_column_gets_a_generic_note_no_author_writes(tmp_path):
    # A Warehouse item, because only a Warehouse table may declare Identity.
    source = PARENT.replace("Schema:", "Identity: Order key\n\nSchema:")
    repo = _repo(tmp_path, {"Sales.Order.sql": source}, item="Warehouse/Reporting")
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
 where 1 = 0;
"""
    repo = _repo(tmp_path, {"Sales.Order.sql": PARENT, "Sales.Summary.sql": source})
    notes = declared_column_notes(repo["Sales.Summary"])
    assert [name for name, _note in notes] == ["Total amount"]
    assert notes[0][1].literal == "Sum of gross order amounts."
