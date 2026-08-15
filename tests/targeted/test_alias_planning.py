"""Cross-item aliases: every decision, in pure Python.

An alias is the one construct that reaches across items, and almost everything
about it is a *decision* — is it planned, is it left alone, does its schema still
get created, does its consumer wait for its producer, is it stale because the
producer moved on. None of that needs a workspace: it is computed from the
repository and the catalogue, both of which can be built directly.

What genuinely needs Fabric is narrower, and it is the *installation*: a OneLake
shortcut is an API call, Fabric discovers one asynchronously, and a Warehouse
alias is a view over a SQL endpoint. Those live in `tests/fabric`.

The split matters because the expensive suite used to prove the decisions by
building three estates. A decision proven here costs milliseconds and says which
decision was wrong.
"""

from __future__ import annotations

import pytest
from factories import (
    alias_repository,
    bound_target,
    document_id,
    item_id,
    registered_document,
    target_inventory,
)

from weaver.build_bundle import plan_item_build
from weaver.build_bundle.aliases import plan_item_aliases
from weaver.build_bundle.incremental import (
    declared_signatures,
    select_build,
    stale_alias_destinations,
)

PRODUCER = "Lakehouse/Raw"
CONSUMER = "Lakehouse/Curated"
ALIAS = "Lakehouse/Curated/DWG.PortableCustomer"
SOURCE = "Lakehouse/Raw/DWG.Customer"
VIEW = "Lakehouse/Curated/DWG.CustomerName"


@pytest.fixture
def estate(tmp_path):
    return alias_repository(tmp_path / "repo")


def targets():
    return {
        item_id(PRODUCER): bound_target(id="raw", item_id="Raw_LH"),
        item_id(CONSUMER): bound_target(id="curated", item_id="Curated_LH"),
    }


def plan_aliases(repository, *, selected=(ALIAS,)):
    by_item = targets()
    return plan_item_aliases(
        repository,
        item=item_id(CONSUMER),
        target=by_item[item_id(CONSUMER)],
        target_by_item=by_item,
        selected={document_id(name) for name in selected},
    )


# --- planning the alias itself ------------------------------------------------


def test_a_selected_alias_is_planned_as_one_action(estate):
    planned = plan_aliases(estate)

    assert planned.stage is not None
    kinds = [
        action.kind for batch in planned.stage.batches for action in batch.actions
    ]
    assert kinds == ["create_alias"]


def test_an_unselected_alias_is_left_alone(estate):
    """Incremental selection applies to aliases exactly as to documents.

    An alias absent from the selection is current — its declaration is unchanged,
    its destination is there, and its source has not moved — so replacing it
    would destroy a working pointer for nothing.
    """

    planned = plan_aliases(estate, selected=())

    assert planned.stage is None


def test_a_retained_alias_still_reports_its_schema(estate):
    """The subtle one, and the reason schemas are reported separately.

    An alias that is *not* being replaced still lives in a namespace the item
    must have. A build that created only the schemas its rebuilt aliases needed
    would leave the retained ones homeless.
    """

    planned = plan_aliases(estate, selected=())

    assert planned.schemas == ("DWG",)


def test_an_alias_whose_source_item_is_unbound_is_omitted(estate):
    """It has no physical form under these bindings, so it cannot be planned.

    And — the part that matters — it must not be certified either. A Registry row
    would claim an installation that never happened.
    """

    planned = plan_item_aliases(
        estate,
        item=item_id(CONSUMER),
        target=bound_target(id="curated", item_id="Curated_LH"),
        target_by_item={item_id(CONSUMER): bound_target(id="curated")},
        selected={document_id(ALIAS)},
    )

    assert planned.stage is None
    assert planned.omitted
    assert document_id(ALIAS) in planned.omitted_destinations


def test_an_unmaterialisable_alias_is_withheld_from_certification(estate):
    """The whole-item view of the claim above."""

    item = item_id(CONSUMER)
    target = bound_target(id="curated", item_id="Curated_LH")
    selected = {document_id(ALIAS)}

    planned = plan_item_build(
        estate,
        item=item,
        target=target,
        inventory=target_inventory(target_id="curated"),
        target_by_item={item: target},  # the producer is not bound
        selected_documents=set(),
        selected_aliases=selected,
        selected_for_drop=set(),
        selected_for_build=selected,
        registered={},
    )

    assert document_id(ALIAS) in planned.uncertified


# --- ordering across items ----------------------------------------------------


def test_the_consumer_builds_its_view_after_the_alias_it_reads(estate):
    """Inside the consumer, the alias must exist before the view over it runs."""

    by_item = targets()
    item = item_id(CONSUMER)
    selected = {document_id(ALIAS), document_id(VIEW)}

    planned = plan_item_build(
        estate,
        item=item,
        target=by_item[item],
        inventory=target_inventory(target_id="curated"),
        target_by_item=by_item,
        selected_documents={document_id(VIEW)},
        selected_aliases={document_id(ALIAS)},
        selected_for_drop=set(),
        selected_for_build=selected,
        registered={},
    )

    kinds = [
        action.kind
        for stage in planned.stages
        for batch in stage.batches
        for action in batch.actions
    ]
    assert kinds.index("create_alias") < kinds.index("build_view")


def test_the_consumer_gets_its_own_endpoint_refresh(estate):
    """An item that mutated Delta is closed by a refresh, alias or not."""

    by_item = targets()
    item = item_id(CONSUMER)

    planned = plan_item_build(
        estate,
        item=item,
        target=by_item[item],
        inventory=target_inventory(target_id="curated"),
        target_by_item=by_item,
        selected_documents={document_id(VIEW)},
        selected_aliases={document_id(ALIAS)},
        selected_for_drop=set(),
        selected_for_build={document_id(ALIAS), document_id(VIEW)},
        registered={},
    )

    assert planned.stages[-1].phase == "refresh"


# --- staleness the graph cannot see -------------------------------------------


def certified(repository, *names, epoch=None):
    """The Registry as a successful build of these nodes would have left it.

    `declared_signatures` is used for aliases too, and deliberately: an alias
    destination is signed by *the pair it declares* — this destination, that
    source — not by any file, because that pair is the whole of what an alias is.
    A hand-written signature here would make the alias look changed and drag its
    consumers into the build, which is how the first version of this file was
    wrong.
    """

    signatures = declared_signatures(
        repository, {document_id(name) for name in names}
    )
    return {
        document_id(name): registered_document(
            name, signature=signatures[document_id(name)], build_epoch=epoch
        )
        for name in names
    }


def test_an_alias_is_stale_when_its_source_was_published_later(estate):
    """The half of cross-item freshness the dependency graph cannot answer.

    A producer rebuilt by some *earlier* build is, to this one, entirely
    unchanged — nothing in the repository records that it moved. The only
    surviving evidence is that its Registry row carries a later epoch than the
    alias over it.
    """

    registered = {
        **certified(estate, SOURCE, epoch="2026-01-02T00:00:00"),
        **certified(estate, ALIAS, epoch="2026-01-01T00:00:00"),
    }

    stale = stale_alias_destinations(
        estate, registered, bound_items={item_id(CONSUMER)}
    )

    assert document_id(ALIAS) in stale


def test_an_alias_published_after_its_source_is_current(estate):
    registered = {
        **certified(estate, SOURCE, epoch="2026-01-01T00:00:00"),
        **certified(estate, ALIAS, epoch="2026-01-02T00:00:00"),
    }

    stale = stale_alias_destinations(
        estate, registered, bound_items={item_id(CONSUMER)}
    )

    assert stale == ()


def test_a_missing_registry_row_is_not_staleness(estate):
    """Absent is *new*, which signature classification already handles.

    Treating it as stale here would double-count, and worse would report an
    object as having moved when it was simply never installed.
    """

    registered = certified(estate, SOURCE, epoch="2026-01-02T00:00:00")

    stale = stale_alias_destinations(
        estate, registered, bound_items={item_id(CONSUMER)}
    )

    assert stale == ()


def test_an_unbound_consumer_keeps_its_stale_alias(estate):
    """That is the deferral: a build acts only on items it was pointed at."""

    registered = {
        **certified(estate, SOURCE, epoch="2026-01-02T00:00:00"),
        **certified(estate, ALIAS, epoch="2026-01-01T00:00:00"),
    }

    stale = stale_alias_destinations(
        estate, registered, bound_items={item_id(PRODUCER)}
    )

    assert stale == ()


# --- the incremental claim the Fabric suite used to buy with a whole build -----


def test_a_second_build_over_an_unchanged_estate_plans_no_alias_action(estate):
    """An unchanged alias over an unchanged source must not be replaced.

    This is the decision `test_cross_item_alias.py` spent a full
    generate-and-install to observe. It is made from signatures and epochs before
    any pointer is touched, so it belongs here — what Fabric can still say is
    that the shortcut object itself was not disturbed.
    """

    everything = {document_id(SOURCE), document_id(VIEW), document_id(ALIAS)}
    registered = certified(
        estate, SOURCE, VIEW, ALIAS, epoch="2026-01-01T00:00:00"
    )
    stale = stale_alias_destinations(
        estate, registered, bound_items=set(targets())
    )

    selection = select_build(
        estate, registered, selected=everything, stale_aliases=stale
    )

    assert selection.selected_for_build == ()


def test_a_changed_source_reaches_the_alias_and_its_consumer(estate):
    """The graph carries a producer's change across the alias in one walk."""

    everything = {document_id(SOURCE), document_id(VIEW), document_id(ALIAS)}
    registered = {
        **certified(estate, SOURCE, VIEW, ALIAS),
        # Only the producer moved.
        document_id(SOURCE): registered_document(SOURCE, signature="an-old-hash"),
    }

    selection = select_build(estate, registered, selected=everything)

    assert document_id(SOURCE) in selection.selected_for_build
    assert document_id(VIEW) in selection.selected_for_build
