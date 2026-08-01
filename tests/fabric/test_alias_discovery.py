"""The one alias claim that needs the installed package.

Fabric creates a OneLake shortcut synchronously and *discovers* it
asynchronously: the consumer's next statement failed with "neither a view nor a
table" until the alias action learned to wait for a real read to succeed.

That wait is guarded by ``context.spark is not None``, so an action executed from
a desktop — where there is no session — skips it entirely. Running it there would
not be the same test somewhere cheaper; it would stop testing the thing the code
exists for.

So this runs the action *in* a session, through the installed package, and
asserts the executor waited. Everything else about aliases —
shortcut creation, its target, the endpoint refresh, reading through the aliased
name — is proven from the checkout in `test_cross_item_alias.py`.
"""

from __future__ import annotations

import pytest
from factories import FixtureCatalogue, alias_repository, item_bindings

from weaver import ItemRef

pytestmark = pytest.mark.published_weaver

PRODUCER = "Lakehouse/DiscoveryProducer"
CONSUMER = "Lakehouse/DiscoveryConsumer"


def test_the_executor_waits_for_fabric_to_discover_the_shortcut(
    fabric_workspace, fabric_client, fabric_alias_lakehouses, livy_session,
    tmp_path_factory,
):
    """The action reports how long it waited, which is the behaviour itself."""

    from test_cross_item_alias import action_of, generate, upload
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

    payload = livy_session.run(
        "from weaver import FabricWorkspace, ItemRef\n"
        "from weaver.resolution import resolver_for, store_for\n"
        "from weaver.build_bundle import (InstallationEnvironment, execute_action, "
        "load_bundle)\n"
        "from weaver.build_bundle.executors.base import InstallationContext\n"
        f"workspace = FabricWorkspace(workspace={fabric_workspace.workspace!r}, "
        f"weaver_lakehouse={fabric_workspace.weaver_lakehouse!r}, "
        f"environment={fabric_workspace.environment!r})\n"
        "store = store_for(workspace)\n"
        "resolver = resolver_for(workspace)\n"
        # A shortcut already there would make the creation a no-op and the wait
        # trivially zero, so the destination starts empty.
        f"spark.sql('DROP SCHEMA IF EXISTS {at['consumer'].qualified_schema('DWG')} CASCADE')\n"
        f"spark.sql('CREATE SCHEMA IF NOT EXISTS {at['producer'].qualified_schema('DWG')}')\n"
        f"spark.sql('CREATE SCHEMA IF NOT EXISTS {at['consumer'].qualified_schema('DWG')}')\n"
        f"spark.sql('CREATE TABLE IF NOT EXISTS {source} (CustomerId string) USING delta')\n"
        "environment = InstallationEnvironment(store=store, resolver=resolver, "
        "spark=spark, workspace=workspace)\n"
        "bundle = load_bundle(resolver.build_bundle('aliasdiscovery'), store=store)\n"
        "resolved = {t.id: environment.resolve_target(t) for t in bundle.plan.targets}\n"
        "alias = next(a for _s, _b, a in bundle.plan.actions() "
        f"if a.id == {alias_action.id!r})\n"
        "payload = None\n"
        "if alias.payload is not None:\n"
        "    payload = store.read(bundle.location.join(*alias.payload.split('/')))\n"
        "context = InstallationContext(spark=spark, resolver=resolver, store=store,\n"
        "    snapshot=bundle.location.join('repository'),\n"
        f"    target=resolved[{batch.target_id!r}], targets=resolved)\n"
        "result = execute_action(alias, payload, context=context)\n"
        "emit({'status': result.status, 'error': result.error_message,\n"
        "      'details': result.details})\n",
        label="alias with the discovery wait",
    ).payload

    assert payload["status"] == "succeeded", payload["error"]

    # The wait ran, and reported itself. A context without Spark would have
    # skipped it and this key would simply be absent — which is exactly why this
    # claim cannot move to the desktop.
    details = payload["details"] or {}
    assert "addressable_after_seconds" in details, (
        "the executor did not wait for discovery — a shortcut it created was "
        "assumed readable, which is the race this behaviour exists for"
    )
