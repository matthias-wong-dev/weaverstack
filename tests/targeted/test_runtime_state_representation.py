"""The catalogue's runtime-state intent, and the DML it renders to.

The intent is the decision — which current-state table, and which keyed rows a
build has ended the incarnation of. This is the narrow claim that the decision
becomes valid backend DML, and it is the only place a statement's text is read:
everything about *which* rows go is asserted against the intent itself.
"""

from __future__ import annotations

import pytest
from support.weaver_test import weaver_test

from weaver.catalogue.runtime_state import (
    FORMAT_VERSION,
    RuntimeStateInvalidation,
    invalidation_payload,
    read_invalidation,
    render_invalidation,
    without_invalidated,
)
from weaver.catalogue.tables import BOOKMARK
from weaver.errors import BuildError

CUSTOMER = {
    "item_type": "Lakehouse",
    "item_name": "Sales",
    "schema_name": "DWG",
    "object_name": "Customer",
}
CSV = {
    "item_type": "Lakehouse",
    "item_name": "Sales",
    "schema_name": "Files/Raw",
    "object_name": "CustomerCsv",
}


def _bookmark(*rows) -> RuntimeStateInvalidation:
    return RuntimeStateInvalidation(table=BOOKMARK.name, rows=tuple(rows))


# --- the DML ------------------------------------------------------------------


@weaver_test()
def test_one_table_renders_one_scoped_delete():
    """One statement naming the rows that go, not one per row."""

    (statement,) = render_invalidation((_bookmark(CUSTOMER, CSV),))

    assert statement.startswith("DELETE")
    assert "[_].[Bookmark]" in statement
    assert statement.count("DELETE") == 1


@weaver_test()
def test_the_statement_names_every_invalidated_row_and_nothing_else():
    """The rows are values in the statement, spelled as the table stores them."""

    (statement,) = render_invalidation((_bookmark(CUSTOMER, CSV),))

    assert "N'DWG', N'Customer'" in statement
    assert "N'Files/Raw', N'CustomerCsv'" in statement
    assert "N'ActiveCustomer'" not in statement


@weaver_test()
def test_nothing_to_invalidate_renders_nothing():
    """An empty decision is an absent statement, not an empty one."""

    assert render_invalidation((_bookmark(),)) == ()
    assert render_invalidation(()) == ()


@weaver_test()
def test_a_table_the_catalogue_does_not_have_is_refused():
    """A misspelled table would render a DELETE against nothing."""

    with pytest.raises(KeyError, match="is not a catalogue table"):
        render_invalidation(
            (RuntimeStateInvalidation(table="Bookmarks", rows=(CUSTOMER,)),)
        )


# --- the frozen payload -------------------------------------------------------


@weaver_test()
def test_the_payload_round_trips():
    """What the bundle freezes is what the installer reads back."""

    invalidation = (_bookmark(CUSTOMER, CSV),)

    assert read_invalidation(invalidation_payload(invalidation)) == invalidation


@weaver_test()
def test_the_payload_is_deterministic():
    """Bundle identity hashes the plan, so the same decision is the same bytes."""

    assert invalidation_payload((_bookmark(CUSTOMER),)) == invalidation_payload(
        (_bookmark(CUSTOMER),)
    )


@weaver_test()
def test_a_payload_version_this_weaver_cannot_read_is_refused():
    """A newer bundle says so rather than being read as an empty decision."""

    payload = invalidation_payload((_bookmark(CUSTOMER),)).replace(
        f'"format_version": {FORMAT_VERSION}'.encode(),
        b'"format_version": 99',
    )

    with pytest.raises(BuildError, match="format_version"):
        read_invalidation(payload)


@weaver_test()
def test_an_invalidation_names_a_table():
    """A row set with no table is not a decision about anything."""

    with pytest.raises(BuildError, match="names a table"):
        RuntimeStateInvalidation(table="", rows=(CUSTOMER,))


# --- applying it in memory ----------------------------------------------------


@weaver_test()
def test_applying_the_intent_removes_the_named_rows_and_keeps_the_rest():
    """The same intent, applied to rows rather than rendered.

    Two readers of one decision, and this is the one a lifecycle test uses.
    """

    rows = {
        "Lakehouse/Sales": {
            "Bookmark": (
                dict(CUSTOMER, bookmark_datetime=1),
                dict(CSV, bookmark_datetime=2),
            ),
            "Log": ({"log_sk": "a"},),
        }
    }

    remaining = without_invalidated(rows, (_bookmark(CUSTOMER),))

    assert [row["object_name"] for row in remaining["Lakehouse/Sales"]["Bookmark"]] == [
        "CustomerCsv"
    ]
    assert remaining["Lakehouse/Sales"]["Log"] == ({"log_sk": "a"},)


@weaver_test()
def test_a_row_matching_on_something_other_than_its_identity_stays():
    """Matched on the invalidated row's own columns, which are the table's key."""

    other = dict(CUSTOMER, item_name="Inventory")
    rows = {"Lakehouse/Inventory": {"Bookmark": (dict(other, bookmark_datetime=1),)}}

    remaining = without_invalidated(rows, (_bookmark(CUSTOMER),))

    assert len(remaining["Lakehouse/Inventory"]["Bookmark"]) == 1


__all__: tuple = ()
