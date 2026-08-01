"""How a persisted catalogue moves toward the one a repository describes.

`current.diff(desired)` carries both sides and uses them for different things,
and that asymmetry is the whole design:

- ``current`` informs the **report** — new, changed, unchanged, removed — so a
  reviewer can see what a bundle will do before it runs.
- ``desired`` alone drives the **statements**.

The second half is what these tests mostly pin, because it is the part that
looks safe to change. Deriving the delete from the row-level difference would
seem equivalent and would not be: a partial or wrongly-scoped read returns fewer
rows in ``current``, so fewer deletes would be emitted and obsolete claims would
survive indefinitely with nothing to notice. As it stands a bad read costs a
misleading report and cannot corrupt the catalogue.
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

from weaver.catalogue.state import Catalogue
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

    return Catalogue.from_repository(repository).retaining(
        {document_id(name) for name in names}
    )


def registry_changes(changes):
    return next(
        change
        for change in changes.per_table()[item_id()]
        if change.table is REGISTRY
    )


def statements(changes) -> list[str]:
    """The statements for the item under test.

    Scoped, because a repository declares Weaver's own catalogue item as well and
    a desired catalogue derived from source carries it — correctly, and it is not
    what these tests are about.
    """

    return list(changes.render_dml()[item_id()].statements)


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
    """A reported no-op is a real one: the statements still run and write nothing.

    Worth asserting because the report is what a reviewer trusts, and a merge
    that claimed to change nothing while changing something would be worse than
    no report at all.
    """

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


# --- the statements read only the desired side --------------------------------


def test_the_statements_do_not_depend_on_what_was_read(repository):
    """The property the whole design protects, stated as a test.

    Three very different beliefs about the current catalogue — nothing, the
    truth, and something wrong — must all render the same statements, because
    the delete keeps exactly the keys the desired catalogue claims and the merge
    is idempotent. That is what makes the pair correct against any prior state,
    including one the reader never saw.
    """

    desired = desired_from(repository, CUSTOMER)
    empty = Catalogue(rows={})
    truthful = Catalogue(rows=desired.rows)
    wrong = FixtureCatalogue.from_registry_rows(
        registry_row("DWG.SomethingElse", signature="unrelated")
    )

    rendered = {
        name: statements(catalogue.diff(desired))
        for name, catalogue in (
            ("empty", empty),
            ("truthful", truthful),
            ("wrong", wrong),
        )
    }

    assert rendered["empty"] == rendered["truthful"] == rendered["wrong"]


def test_a_read_that_missed_rows_still_removes_them(repository):
    """The failure a row-level delete would introduce, and does not.

    If the delete followed the diff, a catalogue row the read did not return
    would simply never be deleted — it would survive every future build, silently.
    The delete is scoped instead: everything in this item that the desired
    catalogue does not claim goes, whether or not the read saw it.
    """

    lines = statements(Catalogue(rows={}).diff(desired_from(repository, CUSTOMER)))

    deletes = [line for line in lines if line.startswith("DELETE FROM")]

    # `current` is empty, so a row-level diff would emit no deletes at all.
    assert deletes

    # Every one is scoped to the item and bounded only by what *desired* claims:
    # tables the projection claims keys for spare them with a NOT clause, and
    # tables it claims nothing for are emptied entirely. Neither mentions a key
    # observed in `current`, because none was.
    assert all("`item_name` = 'Sales'" in line for line in deletes)
    claimed = [line for line in deletes if "NOT (" in line]
    unclaimed = [line for line in deletes if "NOT (" not in line]
    assert claimed, "a table the projection claims keys for must spare them"
    assert unclaimed, "a table it claims nothing for must be emptied in scope"


def test_registry_statements_are_kept_separate_from_the_rest(repository):
    """Registry is written last, in its own barrier, so it is returned apart.

    Structural rather than recovered from the SQL: a caller that had to find the
    Registry statements by matching on their text would be guessing at an
    ordering invariant the type can simply carry.
    """

    result = Catalogue(rows={}).diff(desired_from(repository, CUSTOMER)).render_dml()[
        item_id()
    ]

    assert result.registry.statements
    assert all(
        "Registry" not in line
        for plan in result.dictionaries
        for line in plan.statements
    )


# --- binding facts are supplied, never derived --------------------------------


def test_no_installation_row_is_written_unless_one_is_supplied(repository):
    """A repository-derived catalogue does not know its binding and must not
    invent one; publishing without supplying it writes no Installation row."""

    result = Catalogue(rows={}).diff(desired_from(repository, CUSTOMER)).render_dml()[
        item_id()
    ]

    assert not any("MERGE" in line for line in result.installation.statements)


def test_a_supplied_installation_row_is_published(repository):
    result = Catalogue(rows={}).diff(desired_from(repository, CUSTOMER)).render_dml(
        installation={
            item_id(): (
                {
                    "item_type": "Lakehouse",
                    "item_name": "Sales",
                    "target_name": "Sales_LH",
                    "weaver_version": "1.2.3",
                    "signature": "item-signature",
                },
            )
        }
    )[item_id()]

    merged = " ".join(result.installation.statements)
    assert "Sales_LH" in merged
    assert "1.2.3" in merged
