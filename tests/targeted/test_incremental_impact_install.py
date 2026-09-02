"""Which objects a build decides to touch, from one Registry row at a time.

Incremental selection compares a declared signature against a certified one and
walks the dependency graph from what changed. That is all it does, and all it
needs: a repository and a mapping of `RegisteredDocument`.

Constructing a whole catalogue projection to make these claims, which is what
the old tests did, meant a signature-comparison defect and a catalogue-
projection defect failed the same test the same way. Here a failure can only mean
selection is wrong.
"""

from __future__ import annotations

import pytest
from factories import (
    document_id,
    lakehouse_table,
    registered_document,
    single_document_repository,
    spark_view,
    target_inventory,
)
from support.weaver_test import weaver_test

from weaver.build_bundle import determine_impact
from weaver.build_bundle.incremental import declared_signatures, select_build

TABLE = "DWG.Customer"
VIEW = "DWG.ActiveCustomer"


@pytest.fixture
def chain(tmp_path):
    """A table and the view that reads it, the smallest graph with a descendant."""

    return single_document_repository(
        tmp_path,
        documents={
            "Tables/DWG__Customer.py": lakehouse_table(TABLE),
            "Tables/DWG.ActiveCustomer.sql": spark_view(VIEW, depends_on=TABLE),
        },
    )


def signature_of(repository, identity: str) -> str:
    document = document_id(identity)
    return declared_signatures(repository, {document})[document]


def certified(repository, *identities: str, signature: str | None = None):
    """The Registry as it would be after a successful build of these objects."""

    return {
        document_id(name): registered_document(
            name, signature=signature or signature_of(repository, name)
        )
        for name in identities
    }


def physical_inventory(*, tables=(), views=()):
    item = document_id(TABLE).item
    return {
        item: target_inventory(
            tables=tuple(tables),
            views=tuple(views),
        )
    }


def impact_of(repository, registered, *selected: str, physical=()):
    return determine_impact(
        repository,
        registered,
        selected={document_id(name) for name in selected},
        physical_types={
            document_id(name): "view" if name == VIEW else "table" for name in physical
        },
    )


# --- one object's classification ----------------------------------------------


@weaver_test()
def test_an_object_absent_from_inventory_is_new(chain):
    impact = impact_of(chain, {}, TABLE, physical=())

    assert list(impact.new) == [document_id(TABLE)]
    assert impact.changed == ()


@weaver_test()
def test_an_uncertified_physical_object_is_changed(chain):
    impact = impact_of(chain, {}, TABLE, physical=(TABLE,))

    assert impact.new == ()
    assert impact.changed == (document_id(TABLE),)


@weaver_test()
def test_an_object_whose_signature_still_matches_is_neither_new_nor_changed(chain):
    """The unchanged case, and the one that makes a rebuild cost nothing."""

    impact = impact_of(chain, certified(chain, TABLE), TABLE, physical=(TABLE,))

    assert impact.new == ()
    assert impact.changed == ()
    assert impact.impacted == ()


@weaver_test()
def test_an_object_whose_signature_differs_is_changed(chain):
    """One Registry row with a stale signature is the entire input needed."""

    stale = {document_id(TABLE): registered_document(TABLE, signature="an-old-hash")}

    impact = impact_of(chain, stale, TABLE, physical=(TABLE,))

    assert list(impact.changed) == [document_id(TABLE)]
    assert impact.new == ()


@weaver_test()
def test_a_signature_is_derived_from_the_declaration_not_stored_anywhere(
    chain, tmp_path
):
    """Two identical declarations must produce the same signature.

    This is what makes "unchanged" mean unchanged. If a signature carried a path,
    a timestamp or anything else incidental, every build would see every object
    as changed and incremental selection would silently do nothing.
    """

    twin = single_document_repository(
        tmp_path / "twin",
        documents={
            "Tables/DWG__Customer.py": lakehouse_table(TABLE),
            "Tables/DWG.ActiveCustomer.sql": spark_view(VIEW, depends_on=TABLE),
        },
    )

    assert signature_of(chain, TABLE) == signature_of(twin, TABLE)


# --- propagation across the graph ---------------------------------------------


@weaver_test()
def test_a_changed_table_impacts_the_view_that_reads_it(chain):
    """The descendant is certified and unchanged, and must still be rebuilt."""

    registered = {
        document_id(TABLE): registered_document(TABLE, signature="an-old-hash"),
        **certified(chain, VIEW),
    }

    impact = impact_of(chain, registered, TABLE, VIEW, physical=(TABLE, VIEW))

    assert list(impact.changed) == [document_id(TABLE)]
    assert list(impact.impacted_descendants) == [document_id(VIEW)]


@weaver_test()
def test_an_unchanged_table_impacts_nothing(chain):
    impact = impact_of(
        chain,
        certified(chain, TABLE, VIEW),
        TABLE,
        VIEW,
        physical=(TABLE, VIEW),
    )

    assert impact.impacted == ()


@weaver_test()
def test_a_descendant_left_out_of_the_selection_is_not_reached(chain):
    """Selection bounds the walk: an item not in this build stays deferred.

    Not a rule applied afterwards, the unselected node is simply not in the set
    being classified, so nothing propagates to it by construction.
    """

    registered = {
        document_id(TABLE): registered_document(TABLE, signature="an-old-hash"),
        **certified(chain, VIEW),
    }

    impact = impact_of(chain, registered, TABLE, physical=(TABLE,))

    assert impact.impacted_descendants == ()


# --- what selection then does with it -----------------------------------------


@weaver_test()
def test_a_new_object_is_built_but_not_dropped_first(chain):
    """There is nothing to drop: it has never been installed."""

    selection = select_build(
        chain,
        {},
        selected={document_id(TABLE)},
        inventories=physical_inventory(),
    )

    assert document_id(TABLE) in selection.selected_for_build
    assert document_id(TABLE) not in selection.selected_for_drop


@weaver_test()
def test_a_changed_object_is_dropped_and_rebuilt(chain):
    stale = {document_id(TABLE): registered_document(TABLE, signature="an-old-hash")}

    selection = select_build(
        chain,
        stale,
        selected={document_id(TABLE)},
        inventories=physical_inventory(tables=(TABLE,)),
    )

    assert document_id(TABLE) in selection.selected_for_drop
    assert document_id(TABLE) in selection.selected_for_build


@weaver_test()
def test_an_unchanged_object_is_neither_dropped_nor_rebuilt(chain):
    selection = select_build(
        chain,
        certified(chain, TABLE),
        selected={document_id(TABLE)},
        inventories=physical_inventory(tables=(TABLE,)),
    )

    assert selection.selected_for_drop == ()
    assert selection.selected_for_build == ()


@weaver_test()
def test_an_object_that_prohibits_rebuild_is_never_dropped(tmp_path):
    """The one thing that outranks a changed signature.

    A table holding data a rebuild would destroy says so in its declaration, and
    the selection must honour that even though the signature moved. It is
    reported as prohibited rather than built.
    """

    declaration = lakehouse_table(TABLE).replace(
        "Description: A declared table.",
        "Description: A declared table.\nProhibit rebuild: true",
    )
    repository = single_document_repository(
        tmp_path, documents={"Tables/DWG__Customer.py": declaration}
    )
    stale = {document_id(TABLE): registered_document(TABLE, signature="an-old-hash")}

    selection = select_build(
        repository,
        stale,
        selected={document_id(TABLE)},
        inventories=physical_inventory(tables=(TABLE,)),
    )

    assert document_id(TABLE) in selection.prohibited
    assert selection.selected_for_drop == ()
    assert selection.selected_for_build == ()
