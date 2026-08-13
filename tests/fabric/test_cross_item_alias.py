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

So the bundle is generated *here*, in pure Python, and **only the alias action is
run** out of it — not the estate around it. Schemas, tables, views, catalogue
publication and the refreshes are minutes of work that answer none of the
questions above, and every one is proven elsewhere.

And the action runs *from here too*, against the real workspace: creating a
OneLake shortcut is a REST call and refreshing an endpoint is another, so both
reach Fabric perfectly well from this checkout. Only two things need a session,
and neither imports Weaver — making the source table, and reading it back
through its alias.

That leaves exactly one claim needing the published wheel, and it has its own
file: the executor's *wait* for asynchronous discovery is guarded by
``context.spark is not None``, so running the action from here skips it. See
`test_alias_discovery.py`.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from factories import FixtureCatalogue, alias_repository, item_id

from weaver.targets import ItemRef

pytestmark = [pytest.mark.fabric, pytest.mark.remote]

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


def run_from_here(
    action, bundle, *, workspace, resolver, store, batch_target, sql=None, session=None
):
    """Execute one real action against real Fabric, from this process.

    The same `execute_install_action` an installation calls, with its frozen payload,
    given the capabilities a desktop caller injects rather than the ones a
    session acquires. That the session can acquire its own is a separate claim,
    made once in `test_published_weaver.py`.

    **The Session is given, never built here.** This capacity permits one
    concurrent Livy session and the harness already holds it, so a Session that
    acquired its own would ask for a second and be handed one in state 'dead'.
    That did not matter while the Installer ran inside Fabric and this helper
    needed no Spark; it matters now that the desktop Installer reaches for one.
    """

    from weaver.build_bundle import execute_install_action
    from weaver.build_bundle.executors.base import InstallationContext
    from weaver.build_bundle.installer import Installer

    if session is None:
        raise AssertionError(
            "run_from_here needs the harness's Session: building one here asks "
            "the capacity for a second Livy session and gets a dead one"
        )
    installer = Installer(session, workspace=workspace)
    resolved = {
        target.id: installer.resolve_target(target) for target in bundle.plan.targets
    }
    payload = None
    if action.payload is not None:
        payload = store.read(bundle.location.join(*action.payload.split("/")))
    return execute_install_action(
        action,
        payload,
        context=InstallationContext(
            spark=None,
            # From the Installer, as every production context gets them. An
            # executor stays on the desktop and only its statements cross — a
            # table alias asking whether it has become readable, a table build
            # asking what shape its query has.
            spark_sql=installer.spark_sql(),
            spark_sql_batch=installer.spark_sql_batch(),
            resolver=resolver,
            store=store,
            target=resolved[batch_target],
            targets=resolved,
            # A Warehouse alias is a T-SQL view, so it needs a SQL capability.
            # Injected here because a desktop caller has no session identity to
            # acquire one from — which is exactly the difference the parity
            # probes exist to cover.
            sql=sql,
        ),
    )


@pytest.fixture(scope="module")
def alias_estate(
    fabric_workspace, fabric_client, fabric_alias_lakehouses, livy_session,
    weaver_session, tmp_path_factory,
):
    """The alias action, run from here against real Fabric."""

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

    # Setup, and the only part that needs a session: a Delta table is the one
    # thing a desktop cannot make. The body imports nothing.
    livy_session.run(
        f"spark.sql('DROP SCHEMA IF EXISTS {at['consumer'].qualified_schema('DWG')} CASCADE')\n"
        f"spark.sql('CREATE SCHEMA IF NOT EXISTS {at['producer'].qualified_schema('DWG')}')\n"
        f"spark.sql('CREATE SCHEMA IF NOT EXISTS {at['consumer'].qualified_schema('DWG')}')\n"
        f"spark.sql('CREATE TABLE IF NOT EXISTS {source} (CustomerId string) USING delta')\n"
        "emit(True)\n",
        label="seed the source",
    )

    alias_result = run_from_here(
        alias_action, bundle, workspace=fabric_workspace, resolver=resolver,
        store=store, batch_target=batch.target_id, session=weaver_session,
    )
    assert alias_result.status == "succeeded", alias_result.error_message
    refresh_result = run_from_here(
        refresh_action, bundle, workspace=fabric_workspace, resolver=resolver,
        store=store, batch_target=batch.target_id, session=weaver_session,
    )

    aliased = at["consumer"].qualify("DWG", "PortableCustomer")
    # Fabric discovers a shortcut asynchronously, and running the action from
    # here skipped the executor's own wait — so the read retries. That the
    # *executor* waits is asserted in `test_alias_discovery.py`, where it can be.
    seen = livy_session.run(
        "import time\n"
        "_deadline = time.monotonic() + 180\n"
        "_seen = {}\n"
        "while True:\n"
        "    try:\n"
        f"        _seen['alias_rows'] = spark.sql('SELECT count(*) AS n FROM {aliased}').collect()[0][0]\n"
        "        break\n"
        "    except Exception as exc:\n"
        "        if time.monotonic() >= _deadline:\n"
        "            raise\n"
        "        time.sleep(5)\n"
        f"_seen['consumer_tables'] = sorted(r.tableName for r in spark.sql('SHOW TABLES IN {at['consumer'].qualified_schema('DWG')}').collect())\n"
        f"_seen['alias_in_producer'] = spark.catalog.tableExists({at['producer'].qualify('DWG', 'PortableCustomer')!r})\n"
        f"_seen['source_in_consumer'] = spark.catalog.tableExists({at['consumer'].qualify('DWG', 'Customer')!r})\n"
        f"_seen['produced'] = spark.catalog.tableExists({source!r})\n"
        "emit(_seen)\n",
        label="read back through the alias",
    ).payload

    return {
        "payload": {
            "seen": seen,
            "refresh": {
                "status": refresh_result.status,
                "error": refresh_result.error_message,
                "details": refresh_result.details,
            },
        },
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
    source_schema = alias_estate["resolver"].tables_root(
        ItemRef(alias_estate["producer"].name)
    ) / "DWG"
    physical_source = next(
        entry.name
        for entry in alias_estate["store"].list(source_schema)
        if entry.name.casefold() == "customer"
    )
    assert target.get("path") == f"Tables/DWG/{physical_source}"


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
    clean_disposable_warehouse, livy_session, weaver_session, tmp_path_factory,
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
    # over TDS rather than run.
    warehouse.executor.execute_script(
        "if schema_id(N'DWG') is null exec(N'create schema DWG');"
    )
    # And the source has to exist, which is the one thing needing a session.
    #
    # Seeded in the same exact-case scope Weaver's own table build uses
    # (`weaver.build_bundle.executors.spark_case.exact_identifier_case`, switched
    # on by `fabric_destination`'s `preserve_table_identifier_case`). Fabric folds
    # a table identifier to lower case at creation otherwise, and a Warehouse
    # collates case-sensitively — so a bare CREATE lands `customer` while the view
    # below asks, correctly, for `Customer`, and the alias fails with "Invalid
    # object name" as though the alias SQL were wrong. The conf is set inline
    # rather than through the helper because this body must not need the wheel.
    #
    # The DROP is deliberately *outside* that scope, so it resolves
    # case-insensitively and catches a predecessor of any spelling. Inside the
    # scope it would miss `customer`, and `CREATE ... IF NOT EXISTS` would then
    # match that predecessor case-insensitively and skip — which is exactly how
    # this failed. A build never does this: it refuses to drop a case variant
    # implicitly (test_fabric_creation_never_drops_a_legacy_case_variant_implicitly),
    # so a fixture that wants one gone has to say so itself.
    livy_session.run(
        f"spark.sql('DROP TABLE IF EXISTS {source}')\n"
        "previous = spark.conf.get('spark.sql.caseSensitive')\n"
        "spark.conf.set('spark.sql.caseSensitive', 'true')\n"
        "try:\n"
        f"    spark.sql('CREATE SCHEMA IF NOT EXISTS {at.qualified_schema('DWG')}')\n"
        f"    spark.sql('CREATE TABLE {source} (CustomerId string) USING delta')\n"
        "finally:\n"
        "    spark.conf.set('spark.sql.caseSensitive', previous)\n"
        "emit(True)\n",
        label="seed the source",
    )

    # The producer's endpoint must catch up before the Warehouse can see the
    # table through it — a REST call, made from here.
    #
    # Whether the plan *contains* that refresh is a claim in its own right: the
    # stage is only emitted when the item's planned work mutated Delta (see
    # `weaver.build_bundle.endpoints.item_refresh_stage`). A plan that dropped it
    # would leave the Warehouse reading an endpoint that never caught up, and the
    # alias below would fail with "Invalid object name" — a symptom that reads
    # like broken alias SQL and is nothing of the kind. So the search says so
    # rather than falling through in silence.
    refreshes = [
        (refresh_batch, action)
        for _sequence, refresh_batch, action in bundle.plan.actions()
        if action.kind == "refresh_sql_endpoint" and "AliasHouseProducer" in action.id
    ]
    assert refreshes, (
        "the plan carries no SQL endpoint refresh for the producer, so the "
        "Warehouse would read an endpoint that never caught up: "
        f"{[a.id for _s, _b, a in bundle.plan.actions()]}"
    )
    refresh_batch, refresh_action = refreshes[0]
    run_from_here(
        refresh_action, bundle, workspace=fabric_workspace, resolver=resolver,
        store=store, batch_target=refresh_batch.target_id, session=weaver_session,
    )

    result = run_from_here(
        alias_action, bundle, workspace=fabric_workspace, resolver=resolver,
        store=store, batch_target=batch.target_id, sql=warehouse.executor,
        session=weaver_session,
    )

    assert result.status == "succeeded", result.error_message

    # And the view really answers, over TDS from here.
    rows = warehouse.executor.query(
        "select count(*) as n from [DWG].[PortableCustomer]"
    )
    assert rows[0]["n"] == 0
