"""Does a real read produce the object a fixture builds?

This is the test that pays for the pure-Python prune suite. Those tests express
"the estate is already correct" as `FixtureInventory.from_repository(...)` and
reason from it — which is only legitimate if a genuine Lakehouse, built from that
same repository and read back, gives the same thing.

Without this, the fixture and the reader could drift apart silently and every
prune claim built on the fixture would keep passing while being about nothing.

The estate is built by executing the item's own planned actions, so what is read
back is what Weaver actually does, not what a test arranged to be there.
"""

from __future__ import annotations

import pytest
from factories import (
    FixtureInventory,
    bound_target,
    folder_document,
    lakehouse_table,
    single_document_repository,
    spark_view,
)

from weaver.build_bundle.prune import read_lakehouse_inventory

pytestmark = pytest.mark.spark

TARGET = "Sales_LH"


@pytest.fixture
def estate(tmp_path):
    """One of each physical form, so every field of an inventory is exercised."""

    return single_document_repository(
        tmp_path / "repo",
        schemas=("DWG", "Raw"),
        documents={
            "DWG__Customer.py": lakehouse_table("DWG.Customer"),
            "DWG.ActiveCustomer.sql": spark_view(
                "DWG.ActiveCustomer", depends_on="DWG.Customer"
            ),
            "Files/Raw__CustomerCsv.py": folder_document("Raw.CustomerCsv"),
        },
    )


def read_back(lakehouses, spark) -> "object":
    return read_lakehouse_inventory(
        bound_target(id="target-1", item_id=TARGET),
        resolver=lakehouses.resolver,
        store=lakehouses.store,
        spark=spark,
    )


def folded(names) -> set:
    return {name.casefold() for name in names}


def test_the_item_builds_before_anything_is_read(estate, build_item):
    """Guard the guard: a fidelity claim about an estate that failed to build
    would be comparing two empty things and passing."""

    results = build_item(estate, target=TARGET)

    assert results, "the item planned no actions at all"
    failures = {r.action_id: r.error_message for r in results if r.status == "failed"}
    assert not failures, failures


def test_a_built_estate_reads_back_as_the_fixture_predicts(
    estate, build_item, lakehouses, spark
):
    """The whole point. Build it for real, read it, compare to the prediction."""

    build_item(estate, target=TARGET)

    actual = read_back(lakehouses, spark)
    predicted = FixtureInventory.from_repository(estate, target_id="target-1")

    assert folded(actual.tables) == folded(predicted.tables)
    assert folded(actual.views) == folded(predicted.views)
    assert folded(actual.folders) == folded(predicted.folders)
    assert folded(actual.schemas) == folded(predicted.schemas)
    assert folded(actual.folder_schemas) == folded(predicted.folder_schemas)
    # The deployed runtime tree, read back file by file. This is what makes a
    # load artefact's claim disprovable: an inventory that could not see these
    # would report every one of them missing, and reconciliation would rebuild
    # the whole tree on every build without anything noticing.
    assert folded(actual.files) == folded(predicted.files)
    assert actual.files, "the estate deployed no load files at all"


def test_prune_against_a_freshly_built_estate_finds_nothing(
    estate, build_item, lakehouses, spark
):
    """The pure-Python claim, restated against a real read.

    `test_prune.py` asserts this with a fixture inventory. Here the inventory is
    a genuine Lakehouse read, so the two together say: prune spares a correct
    estate, and a correct estate really does look like that.
    """

    from weaver.build_bundle.physical import item_prune_stage
    from factories import item_id

    build_item(estate, target=TARGET)

    stage = item_prune_stage(
        estate,
        set(estate.source_documents),
        item=item_id(),
        target=bound_target(id="target-1", item_id=TARGET),
        inventory=read_back(lakehouses, spark),
    )

    assert stage is None


def test_an_unmanaged_object_is_the_only_difference_a_read_reports(
    estate, build_item, lakehouses, spark
):
    """Seed one orphan and the read must show exactly that, and nothing else.

    Proves the reader reports what is there rather than what it expects — a
    reader that returned the declared set would satisfy the test above and fail
    this one.
    """

    from weaver.spark import SparkCatalogue

    build_item(estate, target=TARGET)
    catalogue = SparkCatalogue(
        spark, lakehouses.resolver.spark_destination(lakehouses.target)
    )
    catalogue.sql("CREATE TABLE {{object:DWG.OldTable}} (x int) USING delta")

    actual = read_back(lakehouses, spark)
    predicted = FixtureInventory.from_repository(estate, target_id="target-1")

    assert folded(actual.tables) - folded(predicted.tables) == {"dwg.oldtable"}


def test_the_reader_carries_the_target_identity_it_was_asked_about(
    estate, build_item, lakehouses, spark
):
    """An inventory that did not name its target could be diffed against the
    wrong item's declarations, and the planner checks exactly this."""

    build_item(estate, target=TARGET)

    actual = read_back(lakehouses, spark)

    assert actual.target_id == "target-1"
