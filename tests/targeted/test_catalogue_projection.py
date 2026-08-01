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

from weaver.catalogue.state import Catalogue, for_targets, retaining
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


def derived(repository, *names):
    """The logical catalogue, narrowed to the objects a test is talking about.

    `from_repository` takes no selection — it is everything the source declares —
    so narrowing is a separate step, which is the whole point of the contract.
    """

    catalogue = Catalogue.from_repository(repository)
    if not names:
        return catalogue
    return retaining(catalogue, repository, {document_id(name) for name in names})


# --- it is a function of source -----------------------------------------------


def test_everything_declared_is_certified(repository):
    """The premise of the whole idea: adding a declaration adds a row.

    No fixture to update, no list to keep in step, and no selection to pass —
    the constructor reads the repository, so a new artefact appears in the
    desired catalogue by existing.
    """

    catalogue = Catalogue.from_repository(repository)

    # Scoped to this item: a repository always carries Weaver's own builtin
    # catalogue item too, and projecting *everything declared* means projecting
    # that as well — which is right, and worth seeing rather than filtering away
    # inside the constructor.
    #
    # The last three are here for the same reason. An item with load code
    # declares the runtime folder that code is deployed into and the files
    # deployed there; all of it is derived during interpretation, so *the source
    # declares it* just as surely as the author's own documents do.
    assert {
        identity for identity in catalogue.registered if identity.item == item_id()
    } == {
        document_id(CUSTOMER),
        document_id(VIEW),
        document_id(FOLDER),
        document_id(f"{item_id()}/Files/_.Load"),
        document_id(f"{item_id()}/file:_/Load/DWG__Customer.py"),
        document_id(f"{item_id()}/file:_/Load/Files/Raw__CustomerCsv.py"),
    }
    assert any(
        identity.item.item_name == "_weaver" for identity in catalogue.registered
    ), "the builtin catalogue item is declared source too"


def test_each_object_is_registered_as_what_it_is(repository):
    catalogue = Catalogue.from_repository(repository)

    kinds = {
        identity.object_id.object: document.object_type
        for identity, document in catalogue.registered.items()
        if identity.item == item_id()
    }
    assert kinds == {
        "Customer": "table",
        "ActiveCustomer": "view",
        "CustomerCsv": "folder",
        "Load": "folder",
        # The deployed copies of the two Python documents above. A Python file
        # authors a structural object *and* is runtime source, and those are two
        # targets rather than one thing described twice.
        "DWG__Customer.py": "file",
        "Raw__CustomerCsv.py": "file",
    }


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


def test_narrowing_is_a_later_step_not_an_input(repository):
    """Selection transforms the desired catalogue; it does not construct it.

    The full logical catalogue claims everything declared. What a *build* may
    certify is a narrowing of it — and it has to be, because a Registry row means
    the work succeeded, so publishing the whole declaration would claim objects a
    build omitted or failed to materialise.
    """

    everything = Catalogue.from_repository(repository)
    assert {document_id(CUSTOMER), document_id(VIEW)} <= set(everything.registered)

    narrowed = retaining(everything, repository, {document_id(CUSTOMER)})

    assert document_id(CUSTOMER) in narrowed.registered
    assert document_id(VIEW) not in narrowed.registered


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


def test_an_alias_is_not_certified_until_it_is_bound(tmp_path):
    """The logical catalogue declares the alias and certifies nothing about it.

    An alias is a view in a Warehouse and a table in a Lakehouse. The Alias row —
    this name points at that object — is a declaration and belongs to the source.
    The Registry row says a physical object exists *and what it is*, which cannot
    be answered without knowing what it was bound to. So it is not answered.
    """

    from factories import alias_repository
    from weaver.catalogue.tables import ALIAS

    repository = alias_repository(tmp_path / "repo")
    consumer = item_id("Lakehouse/Curated")
    alias = document_id("Lakehouse/Curated/DWG.PortableCustomer")

    logical = Catalogue.from_repository(repository)

    assert logical.rows[consumer][ALIAS.name], "the declaration is source"
    assert alias not in logical.registered, "the certification is not"


def test_binding_certifies_the_alias_as_what_it_physically_is(tmp_path):
    from factories import alias_repository

    repository = alias_repository(tmp_path / "repo")
    producer, consumer = item_id("Lakehouse/Raw"), item_id("Lakehouse/Curated")
    alias = document_id("Lakehouse/Curated/DWG.PortableCustomer")
    kinds = {producer: "lakehouse", consumer: "lakehouse"}

    logical = Catalogue.from_repository(repository)
    declared = set(logical.registered) | {
        alias.destination for alias in repository.aliases
    }
    as_lakehouse = for_targets(logical, repository, declared, kinds)
    as_warehouse = for_targets(
        logical, repository, declared, {**kinds, consumer: "warehouse"}
    )

    assert as_lakehouse.registered[alias].object_type == "table"
    assert as_warehouse.registered[alias].object_type == "view"
    # Same declaration either way: only the physical form moved.
    assert (
        as_lakehouse.registered[alias].signature
        == as_warehouse.registered[alias].signature
    )


def test_an_item_that_is_not_bound_is_not_published(tmp_path):
    """Binding and scoping are one decision, which is what removes the hazard.

    An alias certified against a *guessed* kind would record a Warehouse alias as
    a table — wrong, quiet, and in the authoritative record. There is no path to
    that here: naming the item is how it gets published, and naming it means
    stating its kind. An item left out is simply out of scope.
    """

    from factories import alias_repository

    repository = alias_repository(tmp_path / "repo")
    producer, consumer = item_id("Lakehouse/Raw"), item_id("Lakehouse/Curated")

    logical = Catalogue.from_repository(repository)
    published = for_targets(
        logical,
        repository,
        {alias.destination for alias in repository.aliases},
        {producer: "lakehouse"},
    )

    assert set(published.rows) == {producer}
    assert not any(
        identity.item == consumer for identity in published.registered
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

    catalogue = retaining(
        Catalogue.from_repository(repository),
        repository,
        {
            document_id("Lakehouse/Raw/DWG.Customer"),
            document_id("Lakehouse/Curated/DWG.CustomerName"),
        },
    )

    assert {producer, consumer} <= set(catalogue.rows)
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

    catalogue = retaining(
        Catalogue.from_repository(repository),
        repository,
        {document_id("Lakehouse/Curated/DWG.CustomerName")},
    )

    for row in catalogue.rows[consumer][REGISTRY.name]:
        assert row["item_type"] == "Lakehouse"
        assert row["item_name"] == "Curated"


# --- completeness -------------------------------------------------------------


def test_catalogue_from_repository_has_all_artefacts(tmp_path):
    """Every kind of object Weaver installs is registered by the projection.

    The tripwire for a new artefact, and the first thing that should fail when
    one is added. Adding a member to `OBJECT_TYPES` without teaching the
    projection to emit it — or without adding a source that owns one — breaks
    this immediately, and names the type that is missing.

    Asserted as equality rather than containment on purpose. Containment would
    let a type be declared and never projected, which is the exact shape of the
    omission this exists to catch.
    """

    from factories import full_estate
    from weaver.catalogue.tables import OBJECT_TYPES

    repository = full_estate(tmp_path / "repo")
    catalogue = Catalogue.from_repository(repository)

    assert {
        document.object_type for document in catalogue.registered.values()
    } == set(OBJECT_TYPES)


def test_every_declared_object_and_artefact_is_registered(tmp_path):
    """Nothing the source declares is left out of what it claims to install.

    The other half of the same idea: the first test says every *kind* appears,
    this says every *instance* does. A projection that emitted one file and
    forgot the rest would satisfy the first and fail here.
    """

    from factories import ITEM, WAREHOUSE_ITEM, full_estate, item_id
    from weaver.etl import item_load_artefacts

    repository = full_estate(tmp_path / "repo")
    catalogue = Catalogue.from_repository(repository)

    for item in (ITEM, WAREHOUSE_ITEM):
        identity = item_id(item)
        expected = {
            key for key in repository.source_documents if key.item == identity
        } | {
            artefact.identity
            for artefact in item_load_artefacts(repository, item=identity)
        }
        assert expected <= set(catalogue.registered), sorted(
            str(value) for value in expected - set(catalogue.registered)
        )
