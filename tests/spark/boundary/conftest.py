"""Boundary tests: a real estate, built out of the narrow seams and read back.

Its own directory, and the reason is the autouse cleanup below. These tests
create schemas ad hoc and must drop them per test; the estate-based suites
alongside install once per *module* and are torn down by an autouse fixture that
did not know the difference — which is exactly what merging the two conftests
caused, and how a module's second test came to find nothing.
"""

from __future__ import annotations

import pytest
from factories import bound_target, item_id, registered_document, target_inventory

from weaver import ItemRef
from weaver.build_bundle import execute_action, plan_item_build
from weaver.build_bundle.executors.base import InstallationContext, ResolvedTarget
from weaver.etl import item_load_artefacts
from weaver.locations import Location



def resolved_for(lakehouses, item: str, *, target_id: str = "target-1") -> ResolvedTarget:
    """One local Lakehouse, resolved exactly as the installer resolves it."""

    reference = ItemRef(item)
    return ResolvedTarget(
        bound=bound_target(id=target_id, item_id=item),
        lakehouse=reference,
        location=lakehouses.resolver.lakehouse_spark_location(reference),
        destination=lakehouses.resolver.spark_destination(reference),
    )


def context_for(
    lakehouses, spark, item: str, *, target_id: str = "target-1", epoch: str | None = None
) -> InstallationContext:
    """The context an installer builds for one batch.

    ``epoch`` matters for catalogue work and nothing else: the publication
    statements carry `{{epoch}}` so a bundle's bytes stay stable, and the
    installer resolves it once per installation. Executing such a payload without
    one sends `{{` to the engine, which is not Spark SQL — deliberately, so an
    unresolved token is a syntax error rather than a name that quietly means
    something else.
    """

    target = resolved_for(lakehouses, item, target_id=target_id)
    return InstallationContext(
        spark=spark,
        resolver=lakehouses.resolver,
        store=lakehouses.store,
        snapshot=Location(str(lakehouses.root)),
        target=target,
        targets={target.bound.id: target},
        epoch=epoch,
    )


@pytest.fixture(autouse=True)
def _drop_registered_schemas(request, lakehouses):
    """Drop every schema this test registered, after it.

    Harness isolation, not product behaviour, and it is load-bearing here. The
    Spark session is session-scoped while `lakehouses` is per-test, so each test
    gets a fresh `tmp_path` under the *same logical Lakehouse name*. A schema is
    not a cache: left registered, the next test's Lakehouse resolves to a
    database still pointing at the previous test's deleted directory, and the
    failure surfaces as a missing table in whichever test ran second.

    Found by exactly that — these tests passed alone and failed in sequence.
    """

    yield
    if "spark" not in request.fixturenames:
        return
    spark = request.getfixturevalue("spark")
    for item in (lakehouses.weaver, lakehouses.target):
        prefix = lakehouses.resolver.spark_destination(item).schema_prefix
        if not prefix:
            continue
        for row in spark.sql("SHOW DATABASES").collect():
            name = row[0]
            if name.casefold().startswith(prefix.casefold()):
                spark.sql(f"DROP SCHEMA IF EXISTS `{name}` CASCADE")


@pytest.fixture
def build_item(lakehouses, spark):
    """Plan one item from nothing and run every action it planned.

    Returns the executed results in order, so a caller can assert that the build
    genuinely succeeded before reading anything back — otherwise a fidelity test
    could pass by finding nothing and predicting nothing.
    """

    def run(
        repository,
        *,
        item: str = "Lakehouse/Sales",
        target: str = "Sales_LH",
        inventory=None,
        rebuild: bool = False,
    ):
        identity = item_id(item)
        bound = bound_target(id="target-1", item_id=target)
        selected = {
            key for key in repository.source_documents if key.item == identity
        }
        # A rebuild drops before it creates. That is not a detail of this helper
        # but how Weaver rebuilds at all: a Lakehouse table's generated DDL is
        # `CREATE TABLE`, so the planner clears the way with a drop stage rather
        # than relying on a replace-shaped statement.
        #
        # Each fabricated row records what its document actually is. A drop is
        # rendered from the *installed* type, so calling everything a table would
        # have this helper ask for `DROP TABLE` on a folder — which it did, until
        # an item with load code began declaring one.
        registered = (
            {
                key: registered_document(
                    key, object_type=_registered_type(repository, key)
                )
                for key in selected
            }
            if rebuild
            else {}
        )
        # The item's load layer is built too, and has to be: a fidelity test
        # reads back what a build leaves, so a harness that skipped the runtime
        # tree would predict files that were never written.
        loads = {
            artefact.identity
            for artefact in item_load_artefacts(repository, item=identity)
        }
        planned = plan_item_build(
            repository,
            item=identity,
            target=bound,
            inventory=inventory
            if inventory is not None
            else target_inventory(target_id="target-1"),
            target_by_item={identity: bound},
            selected_documents=selected,
            selected_aliases=set(),
            selected_for_drop=set(selected) if rebuild else set(),
            selected_for_build=selected,
            selected_loads=loads,
            registered=registered,
        )
        context = context_for(lakehouses, spark, target)
        results = []
        for stage in planned.stages:
            for batch in stage.batches:
                for action in batch.actions:
                    results.append(
                        execute_action(
                            action,
                            stage.payloads.get(action.payload)
                            if action.payload
                            else None,
                            context=context,
                        )
                    )
        return results

    return run


def _registered_type(repository, identity) -> str:
    """What the Registry would have recorded this document as."""

    kind = str(repository.source_documents[identity].kind)
    return {"Table": "table", "View": "view", "Folder": "folder"}[kind]
