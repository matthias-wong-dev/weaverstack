"""What happens when the catalogue claims something the target does not have.

Reconciliation is the honesty check between the two prepared states: a catalogue
says an object is installed, an inventory says what is physically there, and a
claim the inventory disproves must stop being believed. Both inputs are built
directly here, so "the object is gone" needs no Lakehouse to arrange — which is
what it used to need, because the only way to produce a disproved claim was to
build something and then delete it.

The pairing is the point, and it is why one `Catalogue` and one `TargetInventory`
class are worth having: every question below is "these two disagree — who wins".
"""

from __future__ import annotations

import pytest
from factories import (
    ITEM,
    FixtureCatalogue,
    FixtureInventory,
    document_id,
    lakehouse_table,
    registry_row,
    single_document_repository,
    target_inventory,
)

from weaver.catalogue.state import reconcile_catalogue_state
from weaver.errors import BuildError


def reconcile(catalogue, inventory):
    from factories import item_id

    return reconcile_catalogue_state(catalogue, inventories={item_id(): inventory})


# --- the two agree ------------------------------------------------------------


def test_a_claim_the_inventory_confirms_is_retained():
    result = reconcile(
        FixtureCatalogue.from_registry_rows(registry_row("DWG.Customer")),
        target_inventory(schemas=("DWG",), tables=("DWG.Customer",)),
    )

    assert document_id("DWG.Customer") in result.catalogue.registered
    assert result.stale_claims == ()


def test_a_repository_and_its_installed_estate_agree_completely(tmp_path):
    """The "already built, nothing changed" pairing, in two lines.

    Both sides derived from the same repository, so reconciliation must find
    nothing at all to retire. Previously this premise cost a real build.
    """

    repository = single_document_repository(
        tmp_path, documents={"DWG__Customer.py": lakehouse_table("DWG.Customer")}
    )

    result = reconcile(
        FixtureCatalogue.from_repository(repository),
        FixtureInventory.from_repository(repository),
    )

    assert result.stale_claims == ()
    assert result.stale_objects == ()
    assert document_id("DWG.Customer") in result.catalogue.registered


# --- the two disagree ---------------------------------------------------------


def test_a_claim_the_inventory_disproves_becomes_stale():
    """The certification the Registry offered is withdrawn, not merely noted."""

    result = reconcile(
        FixtureCatalogue.from_registry_rows(registry_row("DWG.Customer")),
        target_inventory(schemas=("DWG",)),  # the table is not there
    )

    assert document_id("DWG.Customer") not in result.catalogue.registered
    assert result.stale_objects == ("Lakehouse/Sales/DWG.Customer",)
    assert result.stale_claims


def test_a_disproved_claim_is_also_removed_from_the_rows():
    """The rows carry forward into republication, so a stale one must not."""

    result = reconcile(
        FixtureCatalogue.from_registry_rows(registry_row("DWG.Customer")),
        target_inventory(schemas=("DWG",)),
    )

    assert result.catalogue.rows[_item()]["Registry"] == ()


def test_only_the_disproved_claim_is_withdrawn():
    result = reconcile(
        FixtureCatalogue.from_registry_rows(
            registry_row("DWG.Customer"), registry_row("DWG.Order")
        ),
        target_inventory(schemas=("DWG",), tables=("DWG.Customer",)),
    )

    assert document_id("DWG.Customer") in result.catalogue.registered
    assert document_id("DWG.Order") not in result.catalogue.registered


def test_a_physical_object_with_no_claim_produces_no_deletes():
    """Reconciliation withdraws claims; it does not invent them.

    An unmanaged object is prune's business, and confusing the two would have
    the catalogue emitting deletes for rows that were never there.
    """

    result = reconcile(
        FixtureCatalogue.from_registry_rows(),
        target_inventory(schemas=("DWG",), tables=("DWG.Unmanaged",)),
    )

    assert result.stale_claims == ()
    assert result.stale_objects == ()


def test_an_item_with_no_inventory_has_nothing_disproved():
    """A build has no business retiring claims about a target it was not given.

    Producers of aliases are read without an inventory for exactly this reason:
    their rows are needed to judge alias freshness, and nothing about them is
    being built, so nothing about them may be withdrawn.
    """

    catalogue = FixtureCatalogue.from_registry_rows(registry_row("DWG.Customer"))

    result = reconcile_catalogue_state(catalogue, inventories={})

    assert document_id("DWG.Customer") in result.catalogue.registered
    assert result.stale_claims == ()


# --- the Registry has to be well formed ---------------------------------------


def test_a_registry_row_with_an_unsupported_object_type_is_rejected():
    """Weaver drops what it certified by the type it certified — never a guess."""

    with pytest.raises(BuildError, match="unsupported object_type"):
        reconcile(
            FixtureCatalogue.from_registry_rows(
                registry_row("DWG.Load", object_type="procedure")
            ),
            target_inventory(),
        )


def test_a_registry_row_without_a_signature_is_rejected():
    """No signature means no basis for deciding changed — and so no build."""

    with pytest.raises(BuildError, match="no signature"):
        reconcile(
            FixtureCatalogue.from_registry_rows(
                registry_row("DWG.Customer", signature="")
            ),
            target_inventory(),
        )


def test_a_files_schema_is_understood_as_a_folder_claim():
    """`Files/Raw` in the Registry is the folder side, not a schema called that.

    The two namespaces collide by name — a table and a folder may both be
    `Raw.CustomerCsv` — so a claim that read the prefix wrongly would disprove
    one using the other's inventory.
    """

    result = reconcile(
        FixtureCatalogue.from_registry_rows(
            registry_row(
                "Lakehouse/Sales/Files/Raw.CustomerCsv", object_type="folder"
            )
        ),
        target_inventory(folder_schemas=("Raw",), folders=("Raw.CustomerCsv",)),
    )

    assert result.stale_claims == ()


def _item():
    from factories import item_id

    return item_id()
