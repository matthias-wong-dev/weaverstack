"""The catalogue a repository describes, derived from source alone.

`catalogue_from_repository` is the logical twin of reading the persisted
catalogue: one says what *should* be, the other what *is*, and both produce the
same class so the two can be compared.

It lives in production rather than in a fixture on purpose. A projection the
build itself uses cannot drift; a fixture listing the rows a repository ought to
produce has to be updated by hand every time an artefact is added, and will be
wrong the first time someone forgets. So the tests below assert the *properties*
that make it trustworthy — it is a function of source, it carries no binding, it
is stable — rather than enumerating rows, which would reintroduce exactly the
hand-maintained list the constructor exists to remove.
"""

from __future__ import annotations

import pytest
from factories import (
    document_id,
    folder_document,
    item_id,
    lakehouse_table,
    single_document_repository,
    spark_view,
)

from weaver.catalogue import catalogue_from_repository
from weaver.catalogue.tables import INSTALLATION, REGISTRY

CUSTOMER = "DWG.Customer"
VIEW = "DWG.ActiveCustomer"
FOLDER = "Lakehouse/Sales/Files/Raw.CustomerCsv"


@pytest.fixture
def repository(tmp_path):
    return single_document_repository(
        tmp_path,
        schemas=("DWG", "Raw"),
        documents={
            "DWG__Customer.py": lakehouse_table(CUSTOMER),
            "DWG.ActiveCustomer.sql": spark_view(VIEW, depends_on=CUSTOMER),
            "Files/Raw__CustomerCsv.py": folder_document("Raw.CustomerCsv"),
        },
    )


def derived(repository, *names, **kwargs):
    return catalogue_from_repository(
        repository,
        retained={item_id(): {document_id(name) for name in names}},
        **kwargs,
    )


# --- it is a function of source -----------------------------------------------


def test_everything_declared_is_certified(repository):
    """The premise of the whole idea: adding a declaration adds a row.

    No fixture to update, no list to keep in step — the constructor reads the
    repository, so a new artefact appears in the desired catalogue by existing.
    """

    catalogue = derived(repository, CUSTOMER, VIEW, FOLDER)

    assert {identity for identity in catalogue.registered} == {
        document_id(CUSTOMER),
        document_id(VIEW),
        document_id(FOLDER),
    }


def test_each_object_is_registered_as_what_it_is(repository):
    catalogue = derived(repository, CUSTOMER, VIEW, FOLDER)

    kinds = {
        identity.object_id.object: document.object_type
        for identity, document in catalogue.registered.items()
    }
    assert kinds == {"Customer": "table", "ActiveCustomer": "view", "CustomerCsv": "folder"}


def test_signatures_are_the_declarations_own(repository):
    """The field incremental selection compares, so it has to be the same value.

    If the desired catalogue signed an object differently from the way selection
    computes it, every build would see a change that never happened.
    """

    from weaver.build_bundle.incremental import declared_signatures

    catalogue = derived(repository, CUSTOMER, VIEW)
    declared = declared_signatures(
        repository, {document_id(CUSTOMER), document_id(VIEW)}
    )

    assert {
        identity: document.signature
        for identity, document in catalogue.registered.items()
    } == declared


def test_an_object_the_build_is_not_retaining_is_absent(repository):
    """Desired state is what this build claims, not everything in the repository.

    A build that projected objects it was not installing would be claiming them,
    and the comparison would then propose removing whatever it had missed.
    """

    catalogue = derived(repository, CUSTOMER)

    assert document_id(VIEW) not in catalogue.registered


def test_it_is_stable_across_calls(repository):
    """Two derivations of one repository must agree, or a diff is meaningless."""

    assert derived(repository, CUSTOMER, VIEW).rows == derived(
        repository, CUSTOMER, VIEW
    ).rows


# --- it carries no binding ----------------------------------------------------


def test_no_installation_row_is_claimed(repository):
    """A repository does not know which target it was installed to, and must not
    pretend to. The Installation row is a build's to write."""

    catalogue = derived(repository, CUSTOMER)

    assert catalogue.rows[item_id()].get(INSTALLATION.name, ()) == ()


def test_no_publication_epoch_is_stamped(repository):
    """The epoch is one value per *installation*, resolved when it runs. A
    repository-derived row carrying one would be inventing a build."""

    catalogue = derived(repository, CUSTOMER)

    for row in catalogue.rows[item_id()][REGISTRY.name]:
        assert not row.get("build_epoch")


def test_the_target_kind_reaches_the_alias_type_and_nothing_else(tmp_path):
    """The one binding fact that changes a logical value, and why it is allowed.

    An alias destination is registered as the thing it physically *is* — a view
    in a Warehouse, a table in a Lakehouse — because every reader of the
    catalogue needs the real type. So the binding reaches this far, and the test
    pins that it reaches no further than the alias.
    """

    from factories import alias_repository

    repository = alias_repository(tmp_path / "repo")
    producer, consumer = item_id("Lakehouse/Raw"), item_id("Lakehouse/Curated")
    alias = document_id("Lakehouse/Curated/DWG.PortableCustomer")

    as_lakehouse = catalogue_from_repository(
        repository,
        retained={consumer: {alias}},
        target_kinds={consumer: "lakehouse"},
    )
    as_warehouse = catalogue_from_repository(
        repository,
        retained={consumer: {alias}},
        target_kinds={consumer: "warehouse"},
    )

    assert as_lakehouse.registered[alias].object_type == "table"
    assert as_warehouse.registered[alias].object_type == "view"
    # Same declaration either way: only the physical type moved.
    assert (
        as_lakehouse.registered[alias].signature
        == as_warehouse.registered[alias].signature
    )


# --- more than one item -------------------------------------------------------


def test_several_items_project_into_one_catalogue(tmp_path):
    """Which is what makes it comparable to a persisted catalogue at all.

    A read covers every bound item at once; a desired state that could only
    describe one would have to be compared piecewise, and the scoping mistakes
    that invites are exactly what the item scope exists to prevent.
    """

    from factories import alias_repository

    repository = alias_repository(tmp_path / "repo")
    producer, consumer = item_id("Lakehouse/Raw"), item_id("Lakehouse/Curated")

    catalogue = catalogue_from_repository(
        repository,
        retained={
            producer: {document_id("Lakehouse/Raw/DWG.Customer")},
            consumer: {document_id("Lakehouse/Curated/DWG.CustomerName")},
        },
    )

    assert set(catalogue.rows) == {producer, consumer}
    assert {identity.item for identity in catalogue.registered} == {producer, consumer}


def test_an_items_rows_carry_its_own_scope(tmp_path):
    """Every row is stamped with the item it belongs to.

    The catalogue is keyed by logical item and two estates can share a physical
    target, so a row that carried the wrong scope would be written into another
    item's installation.
    """

    from factories import alias_repository

    repository = alias_repository(tmp_path / "repo")
    consumer = item_id("Lakehouse/Curated")

    catalogue = catalogue_from_repository(
        repository,
        retained={consumer: {document_id("Lakehouse/Curated/DWG.CustomerName")}},
    )

    for row in catalogue.rows[consumer][REGISTRY.name]:
        assert row["item_type"] == "Lakehouse"
        assert row["item_name"] == "Curated"
