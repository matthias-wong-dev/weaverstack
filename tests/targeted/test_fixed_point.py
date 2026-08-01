"""A correct estate plans nothing. The whole build, as one property.

Every other test here asks whether one decision is right. This asks whether they
compose: give the planner a catalogue derived from the source, an inventory
derived from the source, and that same source, and it must find nothing to do.

```text
Catalogue.from_repository(...)      what the source says should be installed
FixtureInventory.from_repository()  what the source says should be there
generate_item_build_bundle(...)     must produce no physical action at all
```

The three states agree by construction, so any physical action is a *false*
one — something claimed as absent that is present, or as changed that is not.
That is a different class of defect from the ones a narrow test finds, and it is
the class that costs a real estate something: an object dropped and rebuilt for
no reason, or a schema removed the same build created.

Both item types, deliberately. The two physical sides are not symmetric —
a Lakehouse's generated `_` is a folder *document* while a Warehouse's is a
schema nothing declares — and a Lakehouse-only fixture stops being
representative exactly where that asymmetry begins.

Catalogue publication is *not* physical work and is expected: its statements are
idempotent and are emitted whether or not anything changed, which is what makes
them correct against a prior state the planner never saw.
"""

from __future__ import annotations

import pytest
from factories import (
    ITEM,
    WAREHOUSE_ITEM,
    FixtureInventory,
    folder_document,
    item_bindings,
    item_id,
    lakehouse_table,
    schema_document,
    spark_view,
    warehouse_table,
    warehouse_view,
)

from weaver import ItemRef, LocalStore, Location
from weaver.build_bundle import LakehouseBinding, generate_item_build_bundle
from weaver.catalogue.state import Catalogue
from weaver.declaration import parse_item_repository
from weaver.declaration.metadata import DELTA_TARGET, SQL_TARGET

LAKEHOUSE_TARGET_NAME = "Sales_LH"
WAREHOUSE_TARGET_NAME = "Reporting_WH"

#: Everything a build does *to a target*. Deliberately exhaustive rather than a
#: sample: this test's value is that a new physical kind cannot be added without
#: someone deciding whether a no-op build may emit it, and a list of four kinds
#: is how a spurious `prune_schema` went unnoticed.
PHYSICAL_KINDS = frozenset(
    {
        "create_schema",
        "create_alias",
        "build_folder",
        "build_table",
        "build_view",
        "drop_folder",
        "drop_table",
        "drop_view",
        "prune_table",
        "prune_view",
        "prune_schema",
        "prune_folder",
        "write_file",
        "build_procedure",
        "delete_file",
        "drop_procedure",
        "refresh_sql_endpoint",
    }
)


def _write(root, relative: str, text: str) -> None:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


@pytest.fixture
def estate(tmp_path):
    """Both item types, and every source that owns a load artefact."""

    root = tmp_path / "repo"
    _write(root, f"{ITEM}/schemas/DWG.yml", schema_document("DWG"))
    _write(root, f"{ITEM}/schemas/Raw.yml", schema_document("Raw"))
    _write(root, f"{ITEM}/DWG__Customer.py", lakehouse_table("DWG.Customer"))
    _write(
        root,
        f"{ITEM}/DWG.ActiveCustomer.sql",
        spark_view("DWG.ActiveCustomer", depends_on="DWG.Customer"),
    )
    _write(root, f"{ITEM}/Files/Raw__CustomerCsv.py", folder_document("Raw.CustomerCsv"))
    _write(root, f"{ITEM}/lib/dates.py", "def parse(value):\n    return value\n")
    _write(root, f"{WAREHOUSE_ITEM}/schemas/Sales.yml", schema_document("Sales"))
    _write(
        root, f"{WAREHOUSE_ITEM}/Sales.Customer.sql", warehouse_table("Sales.Customer")
    )
    _write(
        root,
        f"{WAREHOUSE_ITEM}/Sales.Live.sql",
        warehouse_view(
            "Sales.Live", select="select 1 as CustomerId", depends_on="Sales.Customer"
        ),
    )
    return parse_item_repository(Location(str(root)))


def build(repository, tmp_path):
    """Plan the whole estate against the state the source itself describes."""

    bindings = item_bindings(
        (ITEM, LAKEHOUSE_TARGET_NAME),
        (WAREHOUSE_ITEM, WAREHOUSE_TARGET_NAME),
    )
    # Target ids come from the binding rather than being spelled here: the
    # planner refuses an inventory that describes a different target, which is
    # the check that stops a fixture quietly answering for the wrong one.
    bound = {binding.item: binding.to_bound_target() for binding in bindings.entries}
    bundle = generate_item_build_bundle(
        repository,
        bindings=bindings,
        output=Location(str(tmp_path / "bundle")),
        store=LocalStore(),
        target_inventories={
            item_id(ITEM): FixtureInventory.from_repository(
                repository,
                item=ITEM,
                target_kind=DELTA_TARGET,
                target_id=bound[item_id(ITEM)].id,
                kind="lakehouse",
                target_name=LAKEHOUSE_TARGET_NAME,
            ),
            item_id(WAREHOUSE_ITEM): FixtureInventory.from_repository(
                repository,
                item=WAREHOUSE_ITEM,
                target_kind=SQL_TARGET,
                target_id=bound[item_id(WAREHOUSE_ITEM)].id,
                kind="warehouse",
                target_name=WAREHOUSE_TARGET_NAME,
            ),
        },
        # Production, not a fixture: the desired catalogue the build itself uses.
        catalogue=Catalogue.from_repository(repository),
        control_lakehouse=LakehouseBinding(ItemRef("Weaver_Control")),
    )
    return bundle, {target.id for target in bound.values()}


def physical(bundle, estate_targets) -> list[str]:
    """Physical actions against the *estate*, which is what "no work" is about.

    The control-plane Lakehouse is excluded, and only it. Its endpoint refresh is
    unconditional like the publication it follows — the catalogue's own tables
    were just written to, so its analytics endpoint has to catch up whether or
    not the estate changed. Counting it would make a correct no-op build look
    like work, and hiding it by kind would hide a real estate refresh too.
    """

    return [
        action.id
        for _sequence, batch, action in bundle.plan.actions()
        if action.kind in PHYSICAL_KINDS and batch.target_id in estate_targets
    ]


def test_an_estate_that_already_matches_its_source_plans_no_physical_work(
    estate, tmp_path
):
    """The property, stated once.

    Reported by action id rather than as a count, because the useful failure
    names *which* object a build wanted to touch and the count does not.
    """

    assert physical(*build(estate, tmp_path)) == []


def test_nothing_is_selected_to_build_or_drop(estate, tmp_path):
    """The decision behind the actions, asserted separately.

    An empty selection and an empty action list fail for different reasons —
    selection could be right while a stage rendered work anyway, which is exactly
    what a keep-set defect looks like.
    """

    selection = build(estate, tmp_path)[0].plan.selection

    assert selection.selected_for_build == ()
    assert selection.selected_for_drop == ()
    assert selection.impact.new == ()
    assert selection.impact.changed == ()


def test_the_catalogue_tail_is_still_published(estate, tmp_path):
    """The half that is *meant* to run, so "no work" cannot pass by planning
    nothing at all.

    Publication is idempotent and unconditional by design — the statements are
    correct against any prior state, including one the planner never read — so a
    no-op build still writes them. A bundle with no actions whatever would
    satisfy the test above for entirely the wrong reason.
    """

    kinds = {
        action.kind
        for _sequence, _batch, action in build(estate, tmp_path)[0].plan.actions()
    }

    assert "publish_registry" in kinds
    # The control Lakehouse's endpoint refresh rides with the publication, for
    # the same reason: its catalogue tables were just written.
    assert kinds <= {
        "delete_catalogue_claims",
        "publish_catalogue",
        "publish_registry",
        "refresh_sql_endpoint",
    }


def test_the_bundle_is_identical_the_second_time(estate, tmp_path):
    """Same inputs, same identity — the determinism claim, on the no-op path.

    Cheap here and worth having: a plan that varied between two runs of an
    unchanged estate would mean something non-deterministic reached it, and the
    no-op case is where that is easiest to see.
    """

    first, _ = build(estate, tmp_path / "one")
    second, _ = build(estate, tmp_path / "two")

    assert first.bundle_id == second.bundle_id
