"""The alias action waits for Fabric to discover the shortcut it just made.

Fabric creates a OneLake shortcut synchronously and *discovers* it
asynchronously: the consumer's next statement failed with "neither a view nor a
table" until the alias action learned to wait for a real read to succeed.

The wait runs where the Installer runs. Creating the shortcut is a REST call and
the wait asks Spark a question through ``context.spark_sql``, so a desktop
install performs both without a session of its own, and that is the arrangement
under test here. Only the setup needs Spark, and it imports no Weaver.

Everything else about aliases, including shortcut creation, its target, the
endpoint refresh and reading through the aliased name, is proven in
`test_cross_item_alias.py`.
"""

from __future__ import annotations

import pytest
from factories import FixtureCatalogue, alias_repository, item_bindings

from weaver.targets import ItemRef

pytestmark = [pytest.mark.fabric, pytest.mark.remote]

PRODUCER = "Lakehouse/DiscoveryProducer"
CONSUMER = "Lakehouse/DiscoveryConsumer"


def test_the_executor_waits_for_fabric_to_discover_the_shortcut(
    fabric_workspace, fabric_client, fabric_alias_lakehouses, livy_session,
    weaver_session, tmp_path_factory,
):
    """The action reports how long it waited, which is the behaviour itself."""

    from test_cross_item_alias import action_of, generate, run_from_here, upload

    from weaver.declaration import parse_item_repository
    from weaver.fabric import FabricResolver, OneLakeDfsClient

    resolver = FabricResolver(fabric_workspace, client=fabric_client)
    store = OneLakeDfsClient()
    producer = fabric_alias_lakehouses["producer"]
    consumer = fabric_alias_lakehouses["consumer"]

    root = tmp_path_factory.mktemp("discovery-repo")
    alias_repository(root, producer=PRODUCER, consumer=CONSUMER)
    upload(store, resolver.weaver_items_root, root)
    repository = parse_item_repository(resolver.weaver_items_root, store=store)

    bundle = generate(
        workspace=fabric_workspace,
        resolver=resolver,
        store=store,
        repository=repository,
        bindings=item_bindings((PRODUCER, producer.name), (CONSUMER, consumer.name)),
        catalogue=FixtureCatalogue.from_repository(
            repository, item="Lakehouse/_weaver"
        ),
        name="aliasdiscovery",
    )
    batch, alias_action = action_of(bundle.plan, "create_alias")

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
        # this only has to get a *new* table's spelling right; a properly-cased
        # one already there makes the create the no-op it should be.
        "previous = spark.conf.get('spark.sql.caseSensitive')\n"
        "spark.conf.set('spark.sql.caseSensitive', 'true')\n"
        "try:\n"
        f"    spark.sql('CREATE TABLE IF NOT EXISTS {source} (CustomerId string) USING delta')\n"
        "finally:\n"
        "    spark.conf.set('spark.sql.caseSensitive', previous)\n"
        "emit({'ready': True})\n"
    )

    result = run_from_here(
        alias_action,
        bundle,
        workspace=fabric_workspace,
        resolver=resolver,
        store=store,
        batch_target=batch.target_id,
        session=weaver_session,
    )

    assert result.status == "succeeded", result.error_message

    # The wait ran, and reported itself. Without it the action would return as
    # soon as the REST call did, and the next statement to read the alias would
    # fail with "neither a view nor a table".
    details = result.details or {}
    assert "addressable_after_seconds" in details, (
        "the executor did not wait for discovery: a shortcut it created was "
        "assumed readable, which is the race this behaviour exists for"
    )
