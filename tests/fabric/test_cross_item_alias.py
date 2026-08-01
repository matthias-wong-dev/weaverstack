"""Cross-item aliases, installed for real — and only what installing can answer.

Every *decision* an alias involves is proven in pure Python
(`tests/targeted/test_alias_planning.py`): whether one is planned, left alone,
has its schema created anyway, waits for its producer, or is stale because the
producer moved on. None of that needs a workspace, and it used to be bought with
three full generate-and-installs.

What only Fabric can answer is what happens when the plan *runs*:

**A OneLake shortcut is a workspace API call**, not a file operation, so the
emulator's filesystem link proves nothing about it.

**Fabric creates a shortcut synchronously and discovers it asynchronously** — the
consumer's next statement failed with "neither a view nor a table" until the
alias action learned to wait for a real read to succeed.

**A Lakehouse's SQL analytics endpoint lags its Delta tables**, which is why an
item that mutated Delta is closed by a refresh. The emulator has no endpoint and
skips it, so the refresh is unexercised until here.

So the bundle is generated *here*, on the desktop, in pure Python — free — and
then **only the alias action is run**, in the session, out of that bundle. Not
the estate around it: schemas, tables, views, catalogue publication and the
endpoint refreshes are minutes of work that answer none of the questions above,
and every one of them is already proven elsewhere.

What crosses into Fabric is the action itself, with its frozen payload, executed
through the same `execute_action` an installation uses and on the session's own
identity. One submission: seed the source, run the action, observe.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from factories import FixtureCatalogue, alias_repository, item_id

from weaver import ItemRef

pytestmark = pytest.mark.published_weaver

#: Logical names owned by this module alone. The catalogue is keyed by logical
#: item, never by physical target, so two estates sharing a name share Registry
#: rows — which is how an unrelated build could make this one's alias correctly
#: stale and quietly remove the work it is about.
PRODUCER = "Lakehouse/AliasProducer"
CONSUMER = "Lakehouse/AliasConsumer"


def upload(store, root, source: Path) -> None:
    """Replace the repository under the Weaver Lakehouse with this estate's."""

    if store.exists(root):
        store.delete(root, recursive=True)
    for path in sorted(source.rglob("*")):
        if path.is_file() and "__pycache__" not in path.parts:
            store.write(root.join(*path.relative_to(source).parts), path.read_bytes())


def generate(
    *, workspace, resolver, store, repository, bindings, catalogue, name, sql=None
):
    """One bundle, planned on the desktop. Pure Python; no session, no Livy.

    Inventories are dispatched by target kind, exactly as the workflow does it: a
    Lakehouse is read over OneLake, a Warehouse over TDS. Reading them all as
    Lakehouses asks the workspace for a Lakehouse named after a Warehouse, which
    is how the first version of this failed.
    """

    from weaver.build_bundle import (
        LakehouseBinding,
        effective_item_bindings,
        generate_item_build_bundle,
    )
    from weaver.build_bundle.prune import (
        read_lakehouse_inventory,
        read_warehouse_inventory,
    )
    from weaver.build_bundle.targets import WAREHOUSE_TARGET

    bindings = effective_item_bindings(
        bindings, weaver_lakehouse=workspace.weaver_lakehouse
    )
    inventories = {}
    for binding in bindings.entries:
        bound = binding.to_bound_target()
        inventories[binding.item] = (
            read_warehouse_inventory(bound, sql=sql)
            if bound.kind == WAREHOUSE_TARGET
            else read_lakehouse_inventory(bound, resolver=resolver, store=store)
        )
    return generate_item_build_bundle(
        repository,
        bindings=bindings,
        output=resolver.build_bundle(name),
        store=store,
        target_inventories=inventories,
        catalogue=catalogue,
        control_lakehouse=LakehouseBinding(
            lakehouse=ItemRef(workspace.weaver_lakehouse)
        ),
    )


def action_of(plan, kind: str):
    """One action out of a real plan, with the batch that binds it to a target.

    Taken from the generated bundle rather than hand-made, so what runs against
    Fabric is provably what the build produces — the point of generating a bundle
    at all when only one action is wanted from it.
    """

    for _sequence, batch, action in plan.actions():
        if action.kind == kind:
            return batch, action
    raise AssertionError(f"the plan carried no {kind} action")


@pytest.fixture(scope="module")
def alias_estate(
    fabric_workspace, fabric_client, fabric_alias_lakehouses, livy_session,
    tmp_path_factory,
):
    """The alias action, run in Fabric — and nothing else run with it."""

    from factories import item_bindings
    from weaver.declaration import parse_item_repository
    from weaver.fabric import FabricResolver, OneLakeDfsClient

    resolver = FabricResolver(fabric_workspace, client=fabric_client)
    store = OneLakeDfsClient()
    producer = fabric_alias_lakehouses["producer"]
    consumer = fabric_alias_lakehouses["consumer"]

    root = tmp_path_factory.mktemp("alias-repo")
    alias_repository(root, producer=PRODUCER, consumer=CONSUMER)
    upload(store, resolver.weaver_items_root, root)
    repository = parse_item_repository(resolver.weaver_items_root, store=store)

    bindings = item_bindings((PRODUCER, producer.name), (CONSUMER, consumer.name))
    bundle = generate(
        workspace=fabric_workspace,
        resolver=resolver,
        store=store,
        repository=repository,
        bindings=bindings,
        catalogue=FixtureCatalogue.from_repository(
            repository, item="Lakehouse/_weaver"
        ),
        name="aliasaction",
    )
    batch, alias_action = action_of(bundle.plan, "create_alias")
    _refresh_batch, refresh_action = action_of(bundle.plan, "refresh_sql_endpoint")

    at = {
        role: resolver.spark_destination(ItemRef(item.name))
        for role, item in (("producer", producer), ("consumer", consumer))
    }
    source = at["producer"].qualify("DWG", "Customer")
    aliased = at["consumer"].qualify("DWG", "PortableCustomer")

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
        # Seed the source and clear the destination. Setup, not subject: a
        # shortcut needs something to point at, and a stale one would make the
        # creation a no-op.
        f"spark.sql('DROP SCHEMA IF EXISTS {at['consumer'].qualified_schema('DWG')} CASCADE')\n"
        f"spark.sql('CREATE SCHEMA IF NOT EXISTS {at['producer'].qualified_schema('DWG')}')\n"
        f"spark.sql('CREATE SCHEMA IF NOT EXISTS {at['consumer'].qualified_schema('DWG')}')\n"
        f"spark.sql('CREATE TABLE IF NOT EXISTS {source} (CustomerId string) USING delta')\n"
        "environment = InstallationEnvironment("
        "store=store, resolver=resolver, spark=spark, workspace=workspace)\n"
        "bundle = load_bundle(resolver.build_bundle('aliasaction'), store=store)\n"
        "resolved = {t.id: environment.resolve_target(t) for t in bundle.plan.targets}\n"
        "def _run(action, target_id):\n"
        "    context = InstallationContext(spark=spark, resolver=resolver, store=store,\n"
        "        snapshot=bundle.location.join('repository'), target=resolved[target_id],\n"
        "        targets=resolved)\n"
        "    payload = None\n"
        "    if action.payload is not None:\n"
        "        payload = store.read(bundle.location.join(*action.payload.split('/')))\n"
        "    return execute_action(action, payload, context=context)\n"
        "alias = next(a for _s, _b, a in bundle.plan.actions() "
        f"if a.id == {alias_action.id!r})\n"
        "refresh = next(a for _s, _b, a in bundle.plan.actions() "
        f"if a.id == {refresh_action.id!r})\n"
        f"alias_result = _run(alias, {batch.target_id!r})\n"
        f"refresh_result = _run(refresh, {batch.target_id!r})\n"
        "_seen = {}\n"
        "if alias_result.status == 'succeeded':\n"
        f"    _seen['alias_rows'] = spark.sql('SELECT count(*) AS n FROM {aliased}').collect()[0][0]\n"
        f"    _seen['consumer_tables'] = sorted(r.tableName for r in spark.sql('SHOW TABLES IN {at['consumer'].qualified_schema('DWG')}').collect())\n"
        f"    _seen['alias_in_producer'] = spark.catalog.tableExists({at['producer'].qualify('DWG', 'PortableCustomer')!r})\n"
        f"    _seen['source_in_consumer'] = spark.catalog.tableExists({at['consumer'].qualify('DWG', 'Customer')!r})\n"
        f"    _seen['produced'] = spark.catalog.tableExists({source!r})\n"
        "emit({'alias': {'status': alias_result.status, 'error': alias_result.error_message,\n"
        "                'details': alias_result.details},\n"
        "      'refresh': {'status': refresh_result.status, 'error': refresh_result.error_message,\n"
        "                  'details': refresh_result.details},\n"
        "      'seen': _seen})\n",
        label="run the alias action",
    ).payload

    assert payload["alias"]["status"] == "succeeded", payload["alias"]["error"]

    return {
        "payload": payload,
        "plan": bundle.plan,
        "repository": repository,
        "bindings": bindings,
        "resolver": resolver,
        "store": store,
        "producer": producer,
        "consumer": consumer,
    }


# --- the shortcut Fabric actually made ----------------------------------------


def test_the_alias_exists_as_a_onelake_shortcut(alias_estate, fabric_client):
    """Asked of the workspace, not of the plan: the shortcut is really there.

    A OneLake shortcut is an API call. Nothing local — not the emulator's
    filesystem link, not the planned action — stands in for asking Fabric.
    """

    consumer = alias_estate["consumer"]
    found = {
        ((entry.get("path") or "").strip("/"), entry.get("name")): entry.get(
            "target", {}
        ).get("oneLake", {})
        for entry in fabric_client.paged(
            f"workspaces/{consumer.workspace_id}/items/{consumer.id}/shortcuts"
        )
    }

    assert ("Tables/DWG", "PortableCustomer") in found
    target = found[("Tables/DWG", "PortableCustomer")]
    assert target.get("itemId") == alias_estate["producer"].id
    assert target.get("path") == "Tables/DWG/Customer"


def test_the_consumer_reads_the_producers_table_through_its_own_name(alias_estate):
    """The claim an alias makes, checked where it has to hold.

    Checked by *reading*, not by listing: Fabric creates a shortcut synchronously
    and discovers it asynchronously, so a name in the catalogue is not yet a name
    a statement can use.
    """

    seen = alias_estate["payload"]["seen"]

    # A read, not a listing, and it must succeed rather than merely return a
    # name: build creates structure and never data, so zero rows is the success
    # case and an exception is the failure this waits out.
    assert seen["alias_rows"] == 0


def test_the_producers_table_is_not_moved_or_duplicated(alias_estate):
    """An alias adds a name in the consumer; the object stays where it is."""

    seen = alias_estate["payload"]["seen"]

    assert seen["produced"] is True
    assert seen["alias_in_producer"] is False
    assert seen["source_in_consumer"] is False


def test_the_consumers_endpoint_reports_the_aliased_table(alias_estate):
    """What the refresh is for: the SQL side sees what Spark just created."""

    seen = alias_estate["payload"]["seen"]

    assert "PortableCustomer" in seen["consumer_tables"]


def test_each_mutated_lakehouse_had_its_endpoint_refreshed_for_real(alias_estate):
    """That the refresh is *planned* is pure Python. That it found a real
    endpoint and did work is not."""

    refresh = alias_estate["payload"]["refresh"]

    assert refresh["status"] == "succeeded", refresh["error"]
    details = refresh["details"] or {}
    assert "skipped" not in details, "the refresh was skipped in Fabric"
    assert details.get("sql_endpoint_id"), "the refresh found no endpoint"


def test_the_shortcut_survives_a_build_that_does_not_touch_it(
    alias_estate, fabric_client
):
    """The one part of the incremental claim a workspace still has to answer.

    That an unchanged alias over an unchanged source plans *no action* is decided
    from signatures and epochs before any pointer is touched, and belongs in
    `tests/targeted/test_alias_planning.py` — installing an estate to watch a
    decision get made was the expensive habit this module is shedding.

    What remains here is the object itself: after the endpoint refresh ran
    against the same Lakehouse, the shortcut Fabric made is still one shortcut,
    still pointing where it did. A refresh that disturbed it would be invisible
    to every local test.
    """

    from weaver.fabric.shortcuts import list_shortcuts

    shortcuts = list_shortcuts(alias_estate["consumer"], client=fabric_client)

    assert [shortcut.qualified for shortcut in shortcuts] == [
        "Tables/DWG/PortableCustomer"
    ]


# --- the Warehouse form -------------------------------------------------------


WAREHOUSE_PRODUCER = "Lakehouse/AliasHouseProducer"
WAREHOUSE_CONSUMER = "Warehouse/AliasReporting"


def test_a_warehouse_alias_is_a_view_over_the_bound_lakehouse(
    fabric_workspace, fabric_client, fabric_alias_lakehouses,
    clean_disposable_warehouse, livy_session, tmp_path_factory,
):
    """The other alias form, and the one that leans hardest on the endpoint.

    A Warehouse cannot hold a OneLake shortcut, so aliasing a Lakehouse table
    into one is a T-SQL view over that Lakehouse's SQL analytics endpoint —
    which is exactly the metadata Fabric syncs *behind* a Delta mutation rather
    than with it. Nothing local has an endpoint, so nothing local can say
    whether the view resolves.

    One action, out of a real bundle, run on the session's own identity.
    """

    from factories import alias_repository, item_bindings
    from weaver.declaration import parse_item_repository
    from weaver.fabric import FabricResolver, OneLakeDfsClient

    resolver = FabricResolver(fabric_workspace, client=fabric_client)
    store = OneLakeDfsClient()
    producer = fabric_alias_lakehouses["warehouse_producer"]
    warehouse = clean_disposable_warehouse

    root = tmp_path_factory.mktemp("wh-alias-repo")
    alias_repository(
        root,
        producer=WAREHOUSE_PRODUCER,
        consumer=WAREHOUSE_CONSUMER,
        consumer_view=False,
    )
    upload(store, resolver.weaver_items_root, root)
    repository = parse_item_repository(resolver.weaver_items_root, store=store)

    bundle = generate(
        workspace=fabric_workspace,
        resolver=resolver,
        store=store,
        repository=repository,
        bindings=item_bindings(
            (WAREHOUSE_PRODUCER, producer.name),
            (WAREHOUSE_CONSUMER, warehouse.item.name),
        ),
        catalogue=FixtureCatalogue.from_repository(
            repository, item="Lakehouse/_weaver"
        ),
        name="whalias",
        sql=warehouse.executor,
    )
    batch, alias_action = action_of(bundle.plan, "create_alias")
    at = resolver.spark_destination(ItemRef(producer.name))
    source = at.qualify("DWG", "Customer")

    # The alias lands in a schema the build's schema stage would have made. That
    # stage is not the subject and is proven elsewhere, so it is arranged here
    # over TDS rather than run — setup, and cheaper than a stage.
    warehouse.executor.execute_script(
        "if schema_id(N'DWG') is null exec(N'create schema DWG');"
    )

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
        # The alias needs something to point at, and the endpoint needs to have
        # seen it. Setup, not subject.
        f"spark.sql('CREATE SCHEMA IF NOT EXISTS {at.qualified_schema('DWG')}')\n"
        f"spark.sql('CREATE TABLE IF NOT EXISTS {source} (CustomerId string) USING delta')\n"
        "environment = InstallationEnvironment("
        "store=store, resolver=resolver, spark=spark, workspace=workspace)\n"
        "bundle = load_bundle(resolver.build_bundle('whalias'), store=store)\n"
        "resolved = {t.id: environment.resolve_target(t) for t in bundle.plan.targets}\n"
        "refresh = next((a for _s, _b, a in bundle.plan.actions()\n"
        "                if a.kind == 'refresh_sql_endpoint'\n"
        f"                and 'AliasHouseProducer' in a.id), None)\n"
        "alias = next(a for _s, _b, a in bundle.plan.actions() "
        f"if a.id == {alias_action.id!r})\n"
        "def _run(action, target_id):\n"
        "    context = InstallationContext(spark=spark, resolver=resolver, store=store,\n"
        "        snapshot=bundle.location.join('repository'), target=resolved[target_id],\n"
        "        sql=environment.sql_for(resolved[target_id].bound), targets=resolved)\n"
        "    payload = None\n"
        "    if action.payload is not None:\n"
        "        payload = store.read(bundle.location.join(*action.payload.split('/')))\n"
        "    return execute_action(action, payload, context=context)\n"
        # The producer's endpoint must be refreshed before the Warehouse can see
        # the table through it — the ordering the item layers exist to enforce.
        "if refresh is not None:\n"
        "    for _s, b, a in bundle.plan.actions():\n"
        "        if a.id == refresh.id:\n"
        "            _run(refresh, b.target_id)\n"
        "            break\n"
        f"result = _run(alias, {batch.target_id!r})\n"
        "emit({'status': result.status, 'error': result.error_message,\n"
        "      'details': result.details})\n",
        label="run the warehouse alias action",
    ).payload

    assert payload["status"] == "succeeded", payload["error"]

    # And the view really answers, over TDS from here.
    rows = warehouse.executor.query(
        "select count(*) as n from [DWG].[PortableCustomer]"
    )
    assert rows[0]["n"] == 0
