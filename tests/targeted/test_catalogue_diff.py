"""How a persisted catalogue moves toward the one a repository describes.

Two things come out of the same comparison, and it is worth being clear which is
which:

- ``current.diff(desired).per_table()`` is the **report** — new, changed,
  unchanged, removed — so a reviewer can see what a bundle will do before it
  runs.
- :func:`weaver.catalogue.reconcile.publish` produces the **statements**.

Both now read both sides. Statements used to be rendered from ``desired`` alone,
which made them correct against any prior state — including one the reader had
got wrong — at the cost of rewriting every catalogue table on every build. The
read is authoritative now: it validates each table's shape, tells a bootstrap
absence from a damaged catalogue, and refuses rows outside the scopes it asked
for, so an unreadable catalogue stops the build instead of quietly becoming a
diff. What that buys is the property tested hardest here — a table with nothing
to change produces no statement at all.
"""

from __future__ import annotations

import pytest
from factories import (
    FixtureCatalogue,
    document_id,
    item_id,
    lakehouse_table,
    registry_row,
    single_document_repository,
)

from weaver.catalogue.reconcile import publish
from weaver.catalogue.state import Catalogue, retaining
from weaver.catalogue.tables import REGISTRY

CUSTOMER = "DWG.Customer"
ORDER = "DWG.Order"


@pytest.fixture
def repository(tmp_path):
    return single_document_repository(
        tmp_path,
        documents={
            "DWG__Customer.py": lakehouse_table(CUSTOMER),
            "DWG__Order.py": lakehouse_table(ORDER),
        },
    )


def desired_from(repository, *names):
    """Logical, then narrowed — the order publication uses."""

    return retaining(
        Catalogue.from_repository(repository),
        repository,
        {document_id(name) for name in names},
    )


def registry_changes(changes):
    return next(
        change
        for change in changes.per_table()[item_id()]
        if change.table is REGISTRY
    )


def statements(current, desired) -> list[str]:
    return list(publish(current, desired).statements)


def registry_statements(current, desired) -> list[str]:
    return list(publish(current, desired).registry.statements)


# --- the report reads both sides ----------------------------------------------


def test_an_object_the_catalogue_has_never_seen_is_reported_new(repository):
    changes = Catalogue(rows={}).diff(desired_from(repository, CUSTOMER))

    assert registry_changes(changes).inserted == 1
    assert registry_changes(changes).deleted == 0


def test_an_object_no_longer_desired_is_reported_removed(repository):
    current = FixtureCatalogue.from_registry_rows(
        registry_row(CUSTOMER), registry_row(ORDER)
    )

    changes = current.diff(desired_from(repository, CUSTOMER))

    assert registry_changes(changes).deleted == 1


def test_an_identical_catalogue_reports_no_work(repository):
    desired = desired_from(repository, CUSTOMER, ORDER)

    changes = Catalogue(rows=desired.rows).diff(desired)

    assert registry_changes(changes).unchanged == 2
    assert registry_changes(changes).is_noop


def test_a_changed_signature_is_reported_changed_not_replaced(repository):
    current = FixtureCatalogue.from_registry_rows(
        registry_row(CUSTOMER, signature="an-old-hash")
    )

    changes = current.diff(desired_from(repository, CUSTOMER))

    assert registry_changes(changes).updated == 1
    assert registry_changes(changes).inserted == 0
    assert registry_changes(changes).deleted == 0


# --- and the statements follow it ---------------------------------------------


def test_a_catalogue_that_already_matches_produces_no_statements(repository):
    """The property the whole change exists for.

    Not "produces statements that write nothing" — produces none. An idempotent
    delete-and-merge pair would also leave the rows alone, and would still make
    every build write every catalogue table and refresh the endpoint after.
    """

    desired = desired_from(repository, CUSTOMER, ORDER)

    assert publish(Catalogue(rows=desired.rows), desired).is_noop
    assert statements(Catalogue(rows=desired.rows), desired) == []


def test_a_new_object_merges_without_a_delete(repository):
    """Nothing is obsolete, so nothing is deleted — the table is only added to."""

    lines = statements(Catalogue(rows={}), desired_from(repository, CUSTOMER))

    assert any(line.startswith("MERGE INTO") for line in lines)
    assert not any(line.startswith("DELETE FROM") for line in lines)


def test_a_changed_object_merges_without_a_delete(repository):
    """Its key is unchanged, so the row is updated in place, not replaced."""

    current = FixtureCatalogue.from_registry_rows(
        registry_row(CUSTOMER, signature="an-old-hash")
    )

    lines = registry_statements(current, desired_from(repository, CUSTOMER))

    assert any(line.startswith("MERGE INTO") for line in lines)
    assert not any(line.startswith("DELETE FROM") for line in lines)


def test_an_object_no_longer_claimed_is_deleted(repository):
    current = FixtureCatalogue.from_registry_rows(
        registry_row(CUSTOMER), registry_row(ORDER)
    )

    lines = registry_statements(current, desired_from(repository, CUSTOMER))

    assert any(line.startswith("DELETE FROM") for line in lines)


def test_a_removal_deletes_and_merges_in_that_order(repository):
    """One object gone and another changed: both statements, delete first."""

    current = FixtureCatalogue.from_registry_rows(
        registry_row(CUSTOMER, signature="an-old-hash"), registry_row(ORDER)
    )

    lines = registry_statements(current, desired_from(repository, CUSTOMER))

    assert lines[0].startswith("DELETE FROM")
    assert lines[1].startswith("MERGE INTO")


def test_the_delete_spares_the_rows_the_build_still_claims(repository):
    """Bounded by what is desired, so a survivor is named as kept, not re-merged."""

    current = FixtureCatalogue.from_registry_rows(
        registry_row(CUSTOMER), registry_row(ORDER)
    )

    delete = next(
        line
        for line in registry_statements(current, desired_from(repository, CUSTOMER))
        if line.startswith("DELETE FROM")
    )

    assert "NOT (" in delete
    assert "'Customer'" in delete


def test_every_statement_stays_scoped_to_the_item(repository):
    current = FixtureCatalogue.from_registry_rows(registry_row(ORDER))

    lines = statements(current, desired_from(repository, CUSTOMER))

    assert lines
    assert all("`item_name` = 'Sales'" in line for line in lines)


def test_an_installation_the_build_did_not_name_is_never_touched(repository):
    """A scoped build must not reach an item it was not pointed at.

    The catalogue read covers more items than a build publishes — alias
    producers come back with it — so "everything I read" and "everything I may
    write" are different sets, and only the second may drive a statement.
    """

    stranger = FixtureCatalogue.from_registry_rows(
        registry_row(CUSTOMER), item="Lakehouse/SomeoneElse"
    )

    lines = statements(stranger, desired_from(repository, CUSTOMER))

    assert all("SomeoneElse" not in line for line in lines)


def test_registry_statements_are_kept_separate_from_the_rest(repository):
    """Registry is written last, in its own barrier, so it is returned apart."""

    result = publish(Catalogue(rows={}), desired_from(repository, CUSTOMER))

    assert result.registry.statements
    assert all(
        "Registry" not in line
        for plan in result.dictionaries
        for line in plan.statements
    )


def test_no_build_module_reaches_the_unconditional_renderer():
    """`reconcile()` renders a projection whatever the catalogue holds.

    That is the right shape for an explicit repair mode and the wrong shape for
    a build: reaching it from the build path would restore the unconditional
    rewrite that made a second identical build write the whole catalogue. The
    two are kept apart by name, so this is what holds them apart.
    """

    from pathlib import Path

    build_modules = sorted(
        (Path(__file__).resolve().parents[2] / "src" / "weaver" / "build_bundle").rglob(
            "*.py"
        )
    )

    offenders = [
        module.name
        for module in build_modules
        if "import reconcile" in module.read_text(encoding="utf-8")
        or "reconcile(" in module.read_text(encoding="utf-8")
    ]

    assert not offenders, (
        f"these build modules reach the unconditional renderer: {offenders}"
    )


def test_an_unchanged_table_beside_a_changed_one_stays_silent(repository):
    """Per table, not per build: one table's change does not rewrite the others."""

    desired = desired_from(repository, CUSTOMER)
    # Everything matches except the Registry signature.
    current_rows = {
        item: {
            name: tuple(
                {**row, "signature": "an-old-hash"} if name == REGISTRY.name else row
                for row in rows
            )
            for name, rows in tables.items()
        }
        for item, tables in desired.rows.items()
    }

    result = publish(Catalogue(rows=current_rows), desired)

    assert result.registry.statements, "the changed table publishes"
    assert all(plan.is_noop for plan in result.dictionaries), (
        "unchanged tables must not be rewritten because another table changed"
    )
