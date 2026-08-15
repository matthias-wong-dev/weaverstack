"""What a build writes to the catalogue, before and after its physical work.

Two renderings, both pure. The *before* stage removes claims: the ones
reconciliation disproved, and the ones held by objects this build is about to
drop. The *after* stage publishes what the build now certifies.

Neither needs a session. The old Fabric tests reached these by installing an
estate and reading the catalogue back, which meant a claim about statement
*ordering* was paid for with a full build and could fail for any reason a build
can fail.

The ordering is the strict invariant here and most of what is asserted: the
dictionaries describe, Installation records the binding, and Registry certifies
— so Registry is written last, and a row in it can never outrun the work it
attests to.
"""

from __future__ import annotations

import json

import pytest
from factories import (
    FixtureCatalogue,
    bound_target,
    document_id,
    item_id,
    lakehouse_table,
    registry_row,
    single_document_repository,
    target_inventory,
)

from weaver.build_bundle.catalogue_actions import (
    collect_claims,
    render_catalogue_after_build,
    render_catalogue_before_build,
)
from weaver.catalogue.state import reconcile_catalogue_state
from weaver.spark import FabricSparkTarget

#: The Weaver Lakehouse every catalogue statement is addressed to.
WEAVER = FabricSparkTarget(workspace="Demo", lakehouse="Weaver")

CUSTOMER = "DWG.Customer"


@pytest.fixture
def repository(tmp_path):
    return single_document_repository(
        tmp_path, documents={"DWG__Customer.py": lakehouse_table(CUSTOMER)}
    )


def statements(stage) -> list[str]:
    """The SQL a catalogue stage carries, which is its whole payload."""

    if stage is None:
        return []
    return [line for content in stage.payloads.values() for line in json.loads(content)]


def after(repository, *names, current=None):
    """Publication for one build.

    ``current`` is what the catalogue already holds. It is the difference
    against that state which decides what gets written, so a test asserting a
    delete has to say what there was to delete — a build against a catalogue
    holding nothing has nothing to remove, however little it certifies.
    """

    item = item_id()
    target = bound_target()
    return render_catalogue_after_build(
        repository,
        {document_id(name) for name in names},
        {item: target},
        catalogue_target=target,
        current=current,
    )


# --- before the build: removing claims ----------------------------------------


def test_a_build_that_removes_nothing_writes_no_deletes(repository):
    """No stage at all, rather than a stage with no statements.

    An empty barrier would still be a barrier the installer had to run and the
    report had to record, for nothing.
    """

    stage = render_catalogue_before_build(
        FixtureCatalogue.from_registry_rows(),
        (),
        catalogue_target=bound_target(),
    )

    assert stage is None


def test_an_objects_claims_are_deleted_when_it_is_being_dropped(repository):
    """Uncertify first: nothing may stay certified while its description goes."""

    catalogue = FixtureCatalogue.from_registry_rows(registry_row(CUSTOMER))

    stage = render_catalogue_before_build(
        catalogue,
        {document_id(CUSTOMER)},
        catalogue_target=bound_target(),
    )

    assert stage is not None
    assert all(line.startswith("DELETE FROM") for line in statements(stage))
    assert any("Registry" in line for line in statements(stage))


def test_the_registry_claim_is_deleted_before_the_dictionaries(repository):
    """Prune runs the publication order backwards, and that is the invariant.

    Registry certifies; the dictionaries describe. Removing a description while
    the object is still certified would leave the catalogue claiming something it
    could no longer describe.
    """

    # Both tables must actually hold a row: a claim is only collected where one
    # exists, so a Registry-only catalogue cannot demonstrate the ordering.
    catalogue = FixtureCatalogue.holding(
        Registry=[registry_row(CUSTOMER)],
        TableDictionary=[registry_row(CUSTOMER)],
    )

    lines = statements(
        render_catalogue_before_build(
            catalogue,
            {document_id(CUSTOMER)},
            catalogue_target=bound_target(),
        )
    )

    registry = next(i for i, line in enumerate(lines) if "Registry" in line)
    dictionary = next(i for i, line in enumerate(lines) if "Dictionary" in line)
    assert registry < dictionary


def test_a_claim_the_catalogue_never_held_produces_no_delete(repository):
    """The build asks to remove an object with no rows: there is nothing to do."""

    stage = render_catalogue_before_build(
        FixtureCatalogue.from_registry_rows(),
        {document_id(CUSTOMER)},
        catalogue_target=bound_target(),
    )

    assert stage is None


def test_claims_disproved_by_reconciliation_are_deleted_too(repository):
    """The two claim sources meet here, and both must reach the same stage.

    One is found by diffing the inventory, the other by the build's own
    selection. A stage carrying only one of them would leave the catalogue
    certifying an object that is demonstrably absent.
    """

    catalogue = FixtureCatalogue.from_registry_rows(registry_row(CUSTOMER))
    reconciled = reconcile_catalogue_state(
        catalogue, inventories={item_id(): target_inventory(schemas=("DWG",))}
    )
    assert reconciled.stale_claims, "the fixture should have disproved the claim"

    stage = render_catalogue_before_build(
        reconciled.catalogue,
        (),  # this build drops nothing itself
        catalogue_target=bound_target(),
        stale_claims=reconciled.stale_claims,
    )

    assert stage is not None
    assert any("Registry" in line for line in statements(stage))


def test_collecting_claims_takes_both_sources_without_duplicating(repository):
    catalogue = FixtureCatalogue.from_registry_rows(registry_row(CUSTOMER))
    reconciled = reconcile_catalogue_state(
        catalogue, inventories={item_id(): target_inventory(schemas=("DWG",))}
    )

    claims = collect_claims(
        catalogue,
        {document_id(CUSTOMER)},
        stale_claims=reconciled.stale_claims,
    )

    assert len(claims) == len(set(claims))


# --- after the build: publishing what it certifies ----------------------------


def test_the_registry_is_published_in_its_own_later_stage(repository):
    """The ordering invariant, stated as barriers rather than as statements.

    Registry is written last so a row in it cannot outrun the work it attests
    to. Sharing a stage with the dictionaries would let the installer run them
    together and lose that.
    """

    stages = after(repository, CUSTOMER)

    slugs = [stage.slug for stage in stages]
    assert slugs.index("publish-catalogue") < slugs.index("publish-registry")


def test_publication_ends_with_the_registry_and_nothing_after_it(repository):
    """Nothing closes the build.

    Catalogue rows are written over TDS into the Warehouse that holds them, so
    they are readable as soon as they commit. There is no endpoint standing
    between the write and the next reader to refresh.
    """

    stages = after(repository, CUSTOMER)

    assert stages[-1].slug == "publish-registry"
    assert not any("refresh" in stage.slug for stage in stages)


def test_a_build_certifying_nothing_removes_what_the_catalogue_still_claims(
    repository,
):
    """Not "nothing to publish, nothing to do" — the opposite.

    Projecting no rows for a scope means everything persisted under it is
    obsolete, so the build must say so. Skipping it would leave the catalogue
    certifying an item that no longer declares anything, which is the one state
    nothing else would ever correct.
    """

    held = FixtureCatalogue.holding(
        Registry=[registry_row(CUSTOMER)],
        TableDictionary=[registry_row(CUSTOMER)],
        ColumnDictionary=[registry_row(CUSTOMER)],
        Alias=[registry_row(CUSTOMER)],
    )

    stages = after(repository, current=held)

    assert stages, "an empty projection still has removals to publish"
    lines = statements(stages[0]) + statements(stages[1])

    # Every description and certification the item held is removed...
    for table in ("TableDictionary", "ColumnDictionary", "Registry", "Alias"):
        assert any(
            line.startswith("DELETE FROM") and table in line for line in lines
        ), table

    # ...and the Installation row survives, because it records *which target this
    # item was built against*, which remains true of a build that certified
    # nothing. Losing it would make the item look as though it had never been
    # bound at all.
    assert any(
        line.startswith("MERGE INTO") and "Installation" in line for line in lines
    )


def test_a_build_certifying_nothing_against_an_empty_catalogue_removes_nothing(
    repository,
):
    """The other side of it, and the whole point of the diff.

    A delete is emitted because rows exist that the desired state no longer
    claims — never merely because a table was considered. With nothing
    persisted there is nothing to remove, and the build says nothing.
    """

    stages = after(repository)

    lines = [line for stage in stages for line in statements(stage)]

    assert not any(line.startswith("DELETE FROM") for line in lines)


def test_every_published_statement_is_scoped_to_its_item(repository):
    """The reach of a build's catalogue work is bounded by construction.

    Every statement carries the item scope, so a build cannot touch another
    item's rows even by mistake — which matters because the catalogue is keyed
    by logical item and two estates can share one.
    """

    lines = statements(after(repository, CUSTOMER)[0])

    assert lines
    assert all("[Item name] = N'Sales'" in line for line in lines)


def test_the_publication_epoch_stays_a_token(repository):
    """Rendered at install, not at plan time — and the bundle's identity is its
    bytes, so a clock in a payload would make every build differ.

    It also has to be one value across the whole installation, which a token
    resolved once by the installer guarantees and a per-statement literal could
    not.
    """

    lines = statements(after(repository, CUSTOMER)[1])

    assert any("{{build_datetime}}" in line for line in lines)


def test_a_mixed_change_removes_then_merges(repository):
    """Delete first, merge second, when the table has both to do.

    The order is not cosmetic: the merge re-asserts what the build certifies, so
    a delete running after it could remove a row the build had just written.
    """

    held = FixtureCatalogue.holding(
        Registry=[registry_row("DWG.Departed")],
        TableDictionary=[registry_row("DWG.Departed")],
    )

    lines = statements(after(repository, CUSTOMER, current=held)[0])

    # Within one table. Tables are independent of each other, so one table's
    # merge may well precede another's delete; what may never happen is a
    # delete running after the merge that re-asserted the same table's rows.
    dictionary = [
        index for index, line in enumerate(lines) if "TableDictionary" in line
    ]
    deletes = [index for index in dictionary if lines[index].startswith("DELETE FROM")]
    merges = [index for index in dictionary if lines[index].startswith("MERGE INTO")]

    assert deletes and merges
    assert max(deletes) < min(merges)


def test_an_unchanged_table_produces_no_statement_at_all(repository):
    """The fixed point, at the level the statements are decided.

    A table whose desired rows are exactly what is persisted needs neither a
    delete nor a merge. Emitting an idempotent pair anyway would be correct and
    would still make every build write the whole catalogue.
    """

    first = after(repository, CUSTOMER)
    desired = _published_catalogue(repository, CUSTOMER)

    again = after(repository, CUSTOMER, current=desired)

    assert first, "the first build against an empty catalogue does publish"
    assert not any(
        line
        for stage in again
        for line in statements(stage)
        if "TableDictionary" in line
    )


def _published_catalogue(repository, *names):
    """The catalogue state a successful build of ``names`` would have left."""

    from weaver.catalogue.state import Catalogue, for_targets, retaining

    identities = {document_id(name) for name in names}
    logical = Catalogue.from_repository(repository)
    return for_targets(
        retaining(logical, repository, identities),
        repository,
        identities,
        {item_id(): bound_target().kind},
    )
