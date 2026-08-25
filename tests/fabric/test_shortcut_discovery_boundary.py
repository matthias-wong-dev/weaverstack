"""The shortcut action waits for Fabric to discover the shortcut it just made.

Fabric creates a OneLake shortcut synchronously and discovers it asynchronously:
the consumer's next statement failed with "neither a view nor a table" until the
action learned to wait for a real read to succeed.

The wait runs where the Installer runs. Creating the shortcut is a REST call and
the wait asks Spark a question through ``context.spark_sql``, so a desktop
install performs both without a session of its own, and that is the arrangement
under test here. Both crossings run through the registered Session, so they
show up in its telemetry; the estate the action needs, the repository, the
generated bundle and the producer's table, is arranged in a fixture and never
claimed to be either.

Everything else about a shortcut, including its creation, its target, the
endpoint refresh and reading through the name it establishes, is proven in
`test_cross_item_shortcut_primitive.py`.
"""

from __future__ import annotations

import pytest
from conftest import staged_repository_root
from factories import FixtureCatalogue, item_bindings, shortcut_repository
from support.weaver_test import weaver_test

from weaver.targets import ItemRef

PRODUCER = "Lakehouse/DiscoveryProducer"
CONSUMER = "Lakehouse/DiscoveryConsumer"


@pytest.fixture
def discovery_estate(
    fabric_workspace,
    fabric_client,
    fabric_shortcut_lakehouses,
    fabric_staging_lakehouse,
    livy_session,
    session_catalogue_sql,
    tmp_path_factory,
):
    """The generated bundle and a producer table for the shortcut to point at.

    Arrangement only: the repository, the bundle and the producer's table are
    built over raw harness capabilities and plain Spark, none of it imports
    Weaver, and none of it is the claim under test.
    """

    from test_cross_item_shortcut_primitive import action_of, generate, upload

    from weaver.declaration import parse_item_repository
    from weaver.fabric import FabricResolver, OneLakeDfsClient

    resolver = FabricResolver(fabric_workspace, client=fabric_client)
    store = OneLakeDfsClient()
    producer = fabric_shortcut_lakehouses["producer"]
    consumer = fabric_shortcut_lakehouses["consumer"]

    root = tmp_path_factory.mktemp("discovery-repo")
    shortcut_repository(root, producer=PRODUCER, consumer=CONSUMER)
    staged = staged_repository_root(resolver, fabric_staging_lakehouse.name)
    upload(store, staged, root)
    repository = parse_item_repository(staged, store=store)

    bundle = generate(
        workspace=fabric_workspace,
        resolver=resolver,
        store=store,
        repository=repository,
        bindings=item_bindings((PRODUCER, producer.name), (CONSUMER, consumer.name)),
        catalogue=FixtureCatalogue.from_repository(
            repository, item="Warehouse/_weaver"
        ),
        name="shortcutdiscovery",
        staging=producer.name,
        catalogue_sql=session_catalogue_sql,
    )
    # Named, because the producer's runtime references are shortcut actions too.
    batch, shortcut_action = action_of(
        bundle.plan, "create_shortcut", naming=CONSUMER.split("/", 1)[1]
    )

    at = {
        role: resolver.spark_destination(ItemRef(item.name))
        for role, item in (("producer", producer), ("consumer", consumer))
    }
    source = at["producer"].qualify("DWG", "Customer")

    # Plain Spark, importing no Weaver: the estate this action needs, and a
    # destination with nothing in it. A shortcut already there would make the
    # creation a no-op and the wait trivially zero.
    livy_session.run(
        f"spark.sql('DROP SCHEMA IF EXISTS {at['consumer'].qualified_schema('DWG')} CASCADE')\n"
        f"spark.sql('CREATE SCHEMA IF NOT EXISTS {at['producer'].qualified_schema('DWG')}')\n"
        f"spark.sql('CREATE SCHEMA IF NOT EXISTS {at['consumer'].qualified_schema('DWG')}')\n"
        # Exact case, as Weaver's own table build creates. Fabric folds a table
        # identifier to lower case otherwise, and the shortcut this test creates
        # points at a path spelled the declared way. The producer is shared, so
        # this only has to get a new table's spelling right; a properly-cased
        # one already there makes the create the no-op it should be.
        "previous = spark.conf.get('spark.sql.caseSensitive')\n"
        "spark.conf.set('spark.sql.caseSensitive', 'true')\n"
        "try:\n"
        f"    spark.sql('CREATE TABLE IF NOT EXISTS {source} (CustomerId string) USING delta')\n"
        "finally:\n"
        "    spark.conf.set('spark.sql.caseSensitive', previous)\n"
        "emit({'ready': True})\n"
    )

    return {
        "bundle": bundle,
        "batch": batch,
        "shortcut_action": shortcut_action,
        "store": store,
    }


@weaver_test(remote=True, resources={"livy", "rest"})
def test_the_executor_waits_for_fabric_to_discover_the_shortcut(
    fabric_workspace,
    weaver_session,
    discovery_estate,
):
    """The action reports how long it waited, which is the behaviour itself."""

    from test_cross_item_shortcut_primitive import run_from_here

    result = run_from_here(
        discovery_estate["shortcut_action"],
        discovery_estate["bundle"],
        workspace=fabric_workspace,
        store=discovery_estate["store"],
        batch_target=discovery_estate["batch"].target_id,
        session=weaver_session,
    )

    assert result.status == "succeeded", result.error_message

    # The wait ran, and reported itself. Without it the action would return as
    # soon as the REST call did, and the next statement to read the shortcut would
    # fail with "neither a view nor a table".
    details = result.details or {}
    assert "addressable_after_seconds" in details, (
        "the executor did not wait for discovery: a shortcut it created was "
        "assumed readable, which is the race this behaviour exists for"
    )
