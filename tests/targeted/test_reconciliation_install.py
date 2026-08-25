"""What happens when the catalogue claims something the target does not have.

Reconciliation is the honesty check between the two prepared states: a catalogue
says an object is installed, an inventory says what is physically there, and a
claim the inventory disproves must stop being believed. Both inputs are built
directly here, so "the object is gone" needs no Lakehouse to arrange, which is
what it used to need, because the only way to produce a disproved claim was to
build something and then delete it.

The pairing is the point, and it is why one `Catalogue` and one `TargetInventory`
class are worth having: every question below is "these two disagree, who wins".
"""

from __future__ import annotations

import pytest
from factories import (
    FixtureCatalogue,
    FixtureInventory,
    document_id,
    lakehouse_table,
    registered_document,
    registry_row,
    single_document_repository,
    target_inventory,
)
from support.weaver_test import weaver_test

from weaver.catalogue.state import reconcile_catalogue_state
from weaver.declaration.model import WeaverSchemaId
from weaver.errors import BuildError


def reconcile(catalogue, inventory):
    from factories import item_id

    return reconcile_catalogue_state(catalogue, inventories={item_id(): inventory})


# --- the two agree ------------------------------------------------------------


@weaver_test()
def test_a_claim_the_inventory_confirms_is_retained():
    result = reconcile(
        FixtureCatalogue.from_registry_rows(registry_row("DWG.Customer")),
        target_inventory(schemas=("DWG",), tables=("DWG.Customer",)),
    )

    assert document_id("DWG.Customer") in result.catalogue.registered
    assert result.stale_claims == ()


@weaver_test()
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


@weaver_test()
def test_a_schema_shortcut_is_compared_with_the_inventory_schema():
    """Both planner and Registry spellings ask about the same namespace."""

    identity = WeaverSchemaId(_item(), "Reference")
    inventory = target_inventory(schemas=("Reference",))

    assert inventory.physical_type(identity) == "schema"
    assert inventory.has_object("Reference", "Reference", "schema")
    assert target_inventory().physical_type(identity) is None


@weaver_test()
def test_a_missing_schema_shortcut_retires_only_its_registry_claim():
    # Stored with the schema in both columns and read back as the schema
    # identity, which is what the declaration keys it by.
    stored = document_id("Reference.Reference")
    identity = WeaverSchemaId(_item(), "Reference")
    result = reconcile(
        FixtureCatalogue.from_registry_rows(
            registry_row(stored, object_type="schema", object_role="shortcut")
        ),
        target_inventory(),
    )

    assert result.stale_objects == (str(identity),)
    assert {claim.rule.table.name for claim in result.stale_claims} == {"Registry"}


# --- the two disagree ---------------------------------------------------------


@weaver_test()
def test_a_claim_the_inventory_disproves_becomes_stale():
    """The certification the Registry offered is withdrawn, not noted."""

    result = reconcile(
        FixtureCatalogue.from_registry_rows(registry_row("DWG.Customer")),
        target_inventory(schemas=("DWG",)),  # the table is not there
    )

    assert document_id("DWG.Customer") not in result.catalogue.registered
    assert result.stale_objects == ("Lakehouse/Sales/DWG.Customer",)
    assert result.stale_claims


@weaver_test()
def test_a_disproved_claim_is_also_removed_from_the_rows():
    """The rows carry forward into republication, so a stale one must not."""

    result = reconcile(
        FixtureCatalogue.from_registry_rows(registry_row("DWG.Customer")),
        target_inventory(schemas=("DWG",)),
    )

    assert result.catalogue.rows[_item()]["Registry"] == ()


@weaver_test()
def test_only_the_disproved_claim_is_withdrawn():
    result = reconcile(
        FixtureCatalogue.from_registry_rows(
            registry_row("DWG.Customer"), registry_row("DWG.Order")
        ),
        target_inventory(schemas=("DWG",), tables=("DWG.Customer",)),
    )

    assert document_id("DWG.Customer") in result.catalogue.registered
    assert document_id("DWG.Order") not in result.catalogue.registered


@weaver_test()
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


@weaver_test()
def test_an_item_with_no_inventory_has_nothing_disproved():
    """A build has no business retiring claims about a target it was not given.

    Producers of shortcuts are read without an inventory for exactly this reason:
    their rows are needed to judge shortcut freshness, and nothing about them is
    being built, so nothing about them may be withdrawn.
    """

    catalogue = FixtureCatalogue.from_registry_rows(registry_row("DWG.Customer"))

    result = reconcile_catalogue_state(catalogue, inventories={})

    assert document_id("DWG.Customer") in result.catalogue.registered
    assert result.stale_claims == ()


# --- the Registry has to be well formed ---------------------------------------


@weaver_test()
def test_a_registry_row_with_an_unsupported_object_type_is_rejected():
    """Weaver drops what it certified by the type it certified: never a guess."""

    with pytest.raises(BuildError, match="unsupported object_type"):
        reconcile(
            FixtureCatalogue.from_registry_rows(
                registry_row("DWG.Load", object_type="procedure")
            ),
            target_inventory(),
        )


@weaver_test()
def test_a_registry_row_without_a_signature_is_rejected():
    """No signature means no basis for deciding changed, and so no build."""

    with pytest.raises(BuildError, match="no signature"):
        reconcile(
            FixtureCatalogue.from_registry_rows(
                registry_row("DWG.Customer", signature="")
            ),
            target_inventory(),
        )


@weaver_test()
def test_a_files_schema_is_understood_as_a_folder_claim():
    """`Files/Raw` in the Registry is the folder side, not a schema called that.

    The two namespaces collide by name, a table and a folder may both be
    `Raw.CustomerCsv`, so a claim that read the prefix wrongly would disprove
    one using the other's inventory.
    """

    result = reconcile(
        FixtureCatalogue.from_registry_rows(
            registry_row("Lakehouse/Sales/Files/Raw.CustomerCsv", object_type="folder")
        ),
        target_inventory(folder_schemas=("Raw",), folders=("Raw.CustomerCsv",)),
    )

    assert result.stale_claims == ()


def _item():
    from factories import item_id

    return item_id()


# --- what a load artefact's claim is tested against ---------------------------


@weaver_test()
def test_a_deployed_file_the_inventory_holds_is_retained():
    """The claim that keeps a runtime tree from being rebuilt every build.

    `has_object` branches by type, and this is why it must: a `file` falling
    through to the table collection would be found missing every time, its claim
    deleted, and the whole tree redeployed on every build with nothing saying so.
    """

    identity = document_id("Lakehouse/Sales/file:_/Load/lib/dates.py")
    result = reconcile(
        FixtureCatalogue.from_registry_rows(
            registry_row(identity, object_type="file", object_role="load")
        ),
        target_inventory(files=("_/Load/lib/dates.py",)),
    )

    assert result.stale_claims == ()
    assert identity in result.catalogue.registered


@weaver_test()
def test_a_deployed_file_the_inventory_does_not_hold_is_disproved():
    """Deleted from the target behind Weaver's back, and noticed."""

    identity = document_id("Lakehouse/Sales/file:_/Load/lib/dates.py")
    result = reconcile(
        FixtureCatalogue.from_registry_rows(
            registry_row(identity, object_type="file", object_role="load")
        ),
        target_inventory(files=("_/Load/lib/other.py",)),
    )

    assert result.stale_objects == (str(identity),)
    assert identity not in result.catalogue.registered


@weaver_test()
def test_a_generated_procedure_is_reconciled_against_the_procedures_read():
    identity = document_id("Warehouse/Reporting/procedure:_/Load Sales.Customer")
    rows = FixtureCatalogue.from_registry_rows(
        registry_row(identity, object_type="stored_procedure", object_role="load"),
        item="Warehouse/Reporting",
    )
    from factories import item_id

    from weaver.catalogue.state import reconcile_catalogue_state

    held = reconcile_catalogue_state(
        rows,
        inventories={
            item_id("Warehouse/Reporting"): target_inventory(
                kind="warehouse", procedures=("_.Load Sales.Customer",)
            )
        },
    )
    gone = reconcile_catalogue_state(
        rows,
        inventories={
            item_id("Warehouse/Reporting"): target_inventory(kind="warehouse")
        },
    )

    assert held.stale_claims == ()
    assert gone.stale_objects == (str(identity),)


# --- the shape Weaver gives a table, versioned ---------------------------------


def _keyed_and_unkeyed(tmp_path):
    """One keyed table and one unkeyed, read as documents."""

    keyed = lakehouse_table("DWG.Customer")
    unkeyed = keyed.replace("Primary key: CustomerId\n", "")
    repository = single_document_repository(
        tmp_path,
        documents={
            "DWG__Customer.py": keyed,
            "DWG__Event.py": unkeyed.replace("DWG.Customer", "DWG.Event").replace(
                "DWG__Customer", "DWG__Event"
            ),
        },
    )
    documents = repository.source_documents
    return (
        documents[document_id("DWG.Customer")],
        documents[document_id("DWG.Event")],
    )


@weaver_test()
def test_a_keyed_table_is_signed_by_its_shape_as_well_as_its_source(tmp_path):
    """So a change to the shape Weaver gives it rebuilds it.

    The row-signature column is not in the authored source, so signing a keyed
    table by the source alone would leave every installed one standing at the old
    shape, and the load reading a column that is not there.
    """

    keyed, _unkeyed = _keyed_and_unkeyed(tmp_path)

    assert keyed.physical_signature != keyed.effective_signature


@weaver_test()
def test_an_unkeyed_table_is_signed_by_its_source_alone(tmp_path):
    """It gains no signature column, so it is not rebuilt for one."""

    _keyed, unkeyed = _keyed_and_unkeyed(tmp_path)

    assert unkeyed.physical_signature == unkeyed.effective_signature


@weaver_test()
def test_a_keyed_table_whose_shape_version_moves_is_selected_for_rebuild(tmp_path):
    """The mechanism, exercised through selection rather than described.

    An installed row signed the way the previous shape version signed it is a
    change, and the walk carries it exactly as an edited source would.
    """

    from weaver.build_bundle.incremental import declared_signatures, determine_impact
    from weaver.declaration.ddl import KEYED_TABLE_VERSION
    from weaver.declaration.source import salted_signature

    keyed, _unkeyed = _keyed_and_unkeyed(tmp_path)
    identity = keyed.logical_id
    repository = single_document_repository(
        tmp_path, documents={"DWG__Customer.py": lakehouse_table("DWG.Customer")}
    )
    selected = {document_id("DWG.Customer")}

    def impact(signature: str):
        return determine_impact(
            repository,
            {
                document_id("DWG.Customer"): registered_document(
                    identity, signature=signature
                )
            },
            selected=selected,
            physical_types={document_id("DWG.Customer"): "table"},
        )

    declared = declared_signatures(repository, selected)
    at_another_version = salted_signature(
        keyed.effective_signature, KEYED_TABLE_VERSION + 1
    )

    assert impact(declared[document_id("DWG.Customer")]).changed == ()
    # An installed table at the shape before the column existed, and one at any
    # other shape version, are both changes the walk carries.
    assert impact(keyed.effective_signature).changed == (document_id("DWG.Customer"),)
    assert impact(at_another_version).changed == (document_id("DWG.Customer"),)
