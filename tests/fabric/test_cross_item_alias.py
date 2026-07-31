"""Cross-item aliases, built for real in Fabric, in both forms.

**Lakehouse to Lakehouse.** ``Lakehouse/Raw`` produces ``DWG.Customer``, and
``Lakehouse/Curated`` aliases it and builds a view over that alias by its own local
name. The alias is a OneLake shortcut.

**Lakehouse to Warehouse.** The same producer, consumed by ``Warehouse/Reporting``
through a T-SQL view over the Lakehouse's SQL analytics endpoint.

This is the Fabric half of the multi-item build claim, and it is the half only a
real workspace can answer. Three things exist nowhere else:

**A OneLake shortcut is a workspace API call**, not a file operation, so the
emulator's filesystem link proves nothing about it.

**An alias has to be created after the object it points at exists**, which is what
the item layers are for — and each consumer's whole group sits behind Raw's.

**A Lakehouse's SQL analytics endpoint lags its Delta tables**, which is why an
item that mutated Delta is closed by a refresh. The emulator has no endpoint and
skips it, so the refresh itself is unexercised until here — and the Warehouse case
is where it does the most work, because a Warehouse reads a Lakehouse *through*
that endpoint.

Fabric also turned out to create a shortcut synchronously and discover it
asynchronously: the consumer's next statement failed with "neither a view nor a
table" until the alias action learned to wait for a real read to succeed.

The suite's own Weaver Lakehouse, Livy session and disposable Warehouse are
reused; the two destination Lakehouses are disposable and are deleted when the run
ends.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from observation import observe_in_session

from weaver import ItemRef

pytestmark = pytest.mark.fabric

_FIXTURES = Path(__file__).parent.parent / "fixtures"
FIXTURE = _FIXTURES / "cross-item-alias"
WAREHOUSE_FIXTURE = _FIXTURES / "cross-item-alias-warehouse"
PRODUCER = "Lakehouse/Raw"
CONSUMER = "Lakehouse/Curated"
WAREHOUSE_CONSUMER = "Warehouse/Reporting"


def _workspace_literal(workspace) -> str:
    return (
        f"FabricWorkspace(workspace={workspace.workspace!r}, "
        f"weaver_lakehouse={workspace.weaver_lakehouse!r}, "
        f"environment={workspace.environment!r})"
    )


def _await_addressable_lakehouse(session, destination, *, attempts=40, pause=5.0):
    """Wait until a Spark statement in the session can address this Lakehouse.

    Test infrastructure, not product behaviour. ``_create_schema_enabled_lakehouse``
    waits for the item to appear in the REST item list, which is not the same thing
    as the session's Spark metadata service knowing about it — and a Lakehouse
    created into an already-warm session gets asked much sooner than one created
    before the session started. Without this the first ``CREATE SCHEMA`` against a
    fresh destination fails, and it fails as a Fabric metadata error that looks
    nothing like a provisioning race.
    """

    import time

    # The probe reads the default schema every schema-enabled Lakehouse has, so it
    # must resolve the artifact and changes nothing. `databaseExists` is not enough:
    # it can answer False for a Lakehouse the session cannot address at all, which
    # is indistinguishable from an absent schema.
    probe = (
        "try:\n"
        f"    spark.sql('SHOW TABLES IN {destination.qualified_schema('dbo')}').collect()\n"
        "    emit(True)\n"
        "except Exception as exc:\n"
        "    emit(str(exc)[:300])\n"
    )
    for _ in range(attempts):
        answer = session.run(probe, label="await lakehouse").payload
        if answer is True:
            return
        time.sleep(pause)
    raise AssertionError(
        f"{destination.item} never became addressable in the session: {answer}"
    )


def _build_in_session(
    fabric_workspace,
    fabric_client,
    session,
    *,
    fixture,
    bindings,
    empty=None,
    bundle_name="aliastest",
):
    """Push one repository, then generate **and** install it inside the session.

    Both phases run in Fabric because that is the product: the desktop only
    uploads the declaration and reads results back for assertions. ``bindings``
    maps a logical item to ``("Lakehouse" | "Warehouse", physical name)``, which is
    what lets one helper serve a Lakehouse alias and a Warehouse one — the single
    shared harness binds every item to one target, and a cross-item alias needs two.
    """

    from weaver.build_bundle import BuildPlan
    from weaver.fabric import FabricResolver, OneLakeDfsClient

    resolver = FabricResolver(fabric_workspace, client=fabric_client)
    store = OneLakeDfsClient()

    # Fixed Lakehouses carry whatever the last run left. Emptied here because
    # every ordering assertion below presumes there is work to do: a producer
    # whose table already matches is correctly not rebuilt, and then the build
    # action the test looks for is simply not in the plan.
    for kind, name in bindings.values():
        if kind == "Lakehouse":
            if empty is not None:
                empty(name)
            _await_addressable_lakehouse(session, resolver.spark_destination(ItemRef(name)))

    def install_repository() -> None:
        """Put this estate's declaration back under the shared repository root.

        Every estate in the run writes to the *same* ``weaver_items`` root, so an
        estate created later replaces what an earlier one put there. A rebuild
        therefore cannot assume its own repository is still installed — it has to
        re-establish it, or it will plan against whichever fixture happened to
        land last. That is not hypothetical: the Warehouse alias estate is
        created between this one's first build and its rebuild, and the rebuild
        failed with "binding names item(s) absent from the repository".

        Re-uploading identical bytes changes no signature, so an incremental
        assertion still sees an unchanged repository.
        """

        root = resolver.weaver_items_root
        if store.exists(root):
            store.delete(root, recursive=True)
        for path in sorted(fixture.rglob("*")):
            if path.is_file() and "__pycache__" not in path.parts:
                store.write(root.join(*path.relative_to(fixture).parts), path.read_bytes())

    def run(bundle_name: str) -> dict:
        install_repository()
        payload = session.run(
            _build_body(fabric_workspace, bindings, bundle_name),
            label="generate and install",
        ).payload
        plan = BuildPlan.from_mapping(payload["plan"])
        failures = {a["id"]: a["error"] for a in payload["actions"] if a["error"]}
        if failures:
            # Printed as well as asserted: pytest truncates a long repr, and a
            # Fabric stack trace is the only thing that says what actually went
            # wrong.
            for name, error in failures.items():
                print(f"ALIAS INSTALL FAILURE {name}:\n{error}")
        assert payload["status"] == "succeeded", sorted(failures)
        return {
            "plan": plan,
            "actions": {a["id"]: a for a in payload["actions"]},
            "at": {
                action.id: sequence.number
                for sequence, _batch, action in plan.actions()
            },
            "resolver": resolver,
            "session": session,
        }

    # ``rebuild`` runs the same generate-and-install again over the estate this
    # one just made — the targets are not emptied, which is the whole point. It
    # makes the incremental claim testable here at the cost of one extra round
    # trip rather than a second pair of Lakehouses.
    return {**run(bundle_name), "rebuild": run}


def _build_body(fabric_workspace, bindings, bundle_name: str) -> str:
    """One generate-and-install, as the program the Livy session runs."""

    binds = ", ".join(
        (
            f"ItemBinding(WeaverItemId.parse({item!r}), "
            f"LakehouseBinding(lakehouse=ItemRef({name!r})))"
            if kind == "Lakehouse"
            else f"ItemBinding(WeaverItemId.parse({item!r}), "
            f"WarehouseBinding(warehouse=ItemRef({name!r})))"
        )
        for item, (kind, name) in bindings.items()
    )
    return (
        "from weaver import ItemRef, FabricWorkspace, WeaverItemId\n"
        "from weaver.resolution import resolver_for, store_for\n"
        "from weaver.declaration import parse_item_repository\n"
        "from weaver.build_bundle import (ItemBinding, ItemBindings, LakehouseBinding, "
        "WarehouseBinding, InstallationEnvironment, effective_item_bindings, "
        "install_bundle)\n"
        "from weaver.build_bundle.workflow import (read_target_inventories, "
        "read_reconciled_catalogue)\n"
        "from weaver.build_bundle.planner import generate_item_build_bundle\n"
        f"workspace = {_workspace_literal(fabric_workspace)}\n"
        "store = store_for(workspace)\n"
        "resolver = resolver_for(workspace)\n"
        "repository = parse_item_repository(resolver.weaver_items_root, store=store)\n"
        "control = LakehouseBinding("
        "lakehouse=ItemRef(workspace.weaver_lakehouse))\n"
        f"selected = ItemBindings(({binds},))\n"
        "bindings = effective_item_bindings("
        "selected, weaver_lakehouse=workspace.weaver_lakehouse)\n"
        "environment = InstallationEnvironment("
        "store=store, resolver=resolver, spark=spark, workspace=workspace)\n"
        "inventories = read_target_inventories(bindings, environment=environment)\n"
        "reconciled = read_reconciled_catalogue("
        "bindings, inventories=inventories, environment=environment, "
        "repository=repository)\n"
        "bundle = generate_item_build_bundle(\n"
        "    repository, bindings=bindings,\n"
        f"    output=resolver.build_bundle({bundle_name!r}),\n"
        "    store=store, control_lakehouse=control,\n"
        "    target_inventories=inventories, reconciled_catalogue=reconciled)\n"
        "report = install_bundle(bundle, environment=environment)\n"
        "emit({'plan': bundle.plan.to_mapping(), 'status': report.status,\n"
        "      'actions': [{'id': a.action_id, 'status': a.status, 'details': a.details,\n"
        "                   'error': (a.error_type + ': ' + str(a.error_message))\n"
        "                             if a.error_type else None}\n"
        "                  for a in report.action_results()]})\n"
    )


@pytest.fixture(scope="module")
def alias_estate(
    fabric_workspace,
    fabric_client,
    fabric_alias_lakehouses,
    fabric_empty_lakehouse,
    livy_session,
):
    """Two Lakehouse items in one bundle, the second aliasing the first."""

    producer = fabric_alias_lakehouses["producer"]
    consumer = fabric_alias_lakehouses["consumer"]
    estate = _build_in_session(
        fabric_workspace,
        fabric_client,
        livy_session,
        fixture=FIXTURE,
        bindings={
            PRODUCER: ("Lakehouse", producer.name),
            CONSUMER: ("Lakehouse", consumer.name),
        },
        empty=fabric_empty_lakehouse,
    )
    return {
        **estate,
        "producer": producer,
        "consumer": consumer,
        "observed": _observe_the_alias(estate, producer, consumer),
    }


def _observe_the_alias(estate, producer, consumer):
    """Everything the built estate has to show, in one round trip.

    Three tests below used to ask three separate questions of Fabric, each
    costing a full Livy submission and each seeing the estate at a different
    instant. They are one question — "what did this build leave?" — so they get
    one payload, taken here, before the rebuild test moves the estate on.
    """

    resolver, session = estate["resolver"], estate["session"]
    at_producer = resolver.spark_destination(ItemRef(producer.name))
    at_consumer = resolver.spark_destination(ItemRef(consumer.name))

    return observe_in_session(
        session,
        queries={
            "view_rows": (
                f"SELECT count(*) AS n FROM {at_consumer.qualify('DWG', 'CustomerName')}"
            ),
            "alias_rows": (
                "SELECT count(*) AS n FROM "
                f"{at_consumer.qualify('DWG', 'PortableCustomer')}"
            ),
            # What the endpoint refresh is for: the SQL side sees what Spark
            # just created.
            "consumer_tables": (
                f"SHOW TABLES IN {at_consumer.qualified_schema('DWG')}"
            ),
        },
        tables={
            "produced": at_producer.qualify("DWG", "Customer"),
            # An alias adds a name in the consumer; the object stays put.
            "alias_in_producer": at_producer.qualify("DWG", "PortableCustomer"),
            "source_in_consumer": at_consumer.qualify("DWG", "Customer"),
        },
        label="observe alias estate",
    )


# --- the alias itself ---------------------------------------------------------


def test_the_alias_exists_as_a_onelake_shortcut_in_the_consumer(
    alias_estate, fabric_client
):
    """Asked of the workspace, not of the plan: the shortcut is really there."""

    consumer = alias_estate["consumer"]
    shortcuts = fabric_client.paged(
        f"workspaces/{consumer.workspace_id}/items/{consumer.id}/shortcuts"
    )
    # Fabric echoes the path back rooted — "/Tables/DWG" for the "Tables/DWG" it
    # was given — so the leading separator is normalised rather than asserted on.
    found = {
        ((entry.get("path") or "").strip("/"), entry.get("name")): entry.get(
            "target", {}
        ).get("oneLake", {})
        for entry in shortcuts
    }

    assert ("Tables/DWG", "PortableCustomer") in found
    target = found[("Tables/DWG", "PortableCustomer")]
    assert target.get("itemId") == alias_estate["producer"].id
    assert target.get("path") == "Tables/DWG/Customer"


def test_the_consumer_reads_the_producers_table_through_its_own_name(alias_estate):
    """The claim an alias makes, checked where it has to hold: in Fabric."""

    seen = alias_estate["observed"]

    # Build creates structure, never data — an empty read is the success case.
    assert seen.scalar("view_rows") == 0
    assert seen.scalar("alias_rows") == 0


def test_the_producers_table_is_not_moved_or_duplicated(alias_estate):
    """An alias adds a name in the consumer; the object stays where it is."""

    seen = alias_estate["observed"]

    assert seen.table("produced")
    assert not seen.table("alias_in_producer")
    assert not seen.table("source_in_consumer")


# --- item order and the endpoint barrier --------------------------------------


def test_the_consumer_items_whole_group_ran_after_the_producers(alias_estate):
    at = alias_estate["at"]

    assert (
        at["object-Lakehouse--Raw--DWG.Customer"]
        < at["refresh-sql-endpoint-Lakehouse--Raw"]
        < at["aliases-Lakehouse--Curated"]
        < at["object-Lakehouse--Curated--DWG.CustomerName"]
    )


def test_each_mutated_lakehouse_had_its_sql_endpoint_refreshed(alias_estate):
    """The emulator skips this; Fabric is where it does something."""

    refreshes = {
        name: action
        for name, action in alias_estate["actions"].items()
        if name.startswith("refresh-sql-endpoint-")
    }

    assert set(refreshes) >= {
        "refresh-sql-endpoint-Lakehouse--Raw",
        "refresh-sql-endpoint-Lakehouse--Curated",
        "refresh-sql-endpoint-control",
    }
    for name, action in refreshes.items():
        assert action["status"] == "succeeded", name
        details = action["details"] or {}
        assert "skipped" not in details, f"{name} was skipped in Fabric"
        assert details.get("sql_endpoint_id"), f"{name} refreshed no endpoint"


def test_the_consumers_endpoint_reports_the_aliased_table(alias_estate):
    """What the refresh is for: the SQL side sees what Spark just created.

    A Warehouse view over another item, a report, a downstream shortcut — all read
    this metadata, and Fabric syncs it behind the mutation rather than with it.
    """

    seen = alias_estate["observed"]

    assert "portablecustomer" in seen.values("consumer_tables", "tableName")


# --- building the same estate again --------------------------------------------
#
# Before the Warehouse section, and that ordering is load-bearing. Both fixtures
# declare the *same logical item* — `Lakehouse/Raw` producing `DWG.Customer` — and
# the catalogue is keyed by logical item, never by physical target. So building
# the Warehouse estate republishes the very Registry row this alias points at,
# with a later build epoch, and the alias is then correctly stale. Asserting "an
# unchanged alias is not replaced" after that would be asserting against a source
# that genuinely moved.
#
# The estates interfering through the catalogue is the real problem; running in a
# safe order is the cheap fix. Giving each estate its own logical identity is the
# proper one.


def test_a_second_build_leaves_the_shortcut_alone(alias_estate, fabric_client, fabric_alias_lakehouses):
    """The incremental claim, against a real OneLake shortcut.

    The emulator proves the *decision* — no alias action is planned — and does so
    cheaply, over several scenarios, in ``test_cross_item_alias_incremental``.
    What only a workspace can show is that the shortcut Fabric actually made is
    still the same shortcut afterwards: not deleted and recreated, and still
    pointing at the same item.

    It costs one extra generate-and-install over the estate already provisioned
    here, rather than a second pair of Lakehouses.
    """

    from weaver.fabric.shortcuts import list_shortcuts

    consumer = alias_estate["consumer"]
    before = list_shortcuts(consumer, client=fabric_client)
    assert [shortcut.qualified for shortcut in before] == ["Tables/DWG/PortableCustomer"]

    again = alias_estate["rebuild"]("aliasrebuild")

    planned = [
        action.kind for _sequence, _batch, action in again["plan"].actions()
    ]
    assert "create_alias" not in planned, (
        "an unchanged alias over an unchanged source must not be replaced"
    )

    after = list_shortcuts(consumer, client=fabric_client)
    assert after == before, "the shortcut itself must be untouched, not remade"


# --- the other alias form: a Warehouse view over a Lakehouse -------------------


@pytest.fixture(scope="module")
def warehouse_alias_estate(
    fabric_workspace,
    fabric_client,
    fabric_alias_lakehouses,
    fabric_empty_lakehouse,
    clean_disposable_warehouse,
    livy_session,
):
    """A Lakehouse producer and a Warehouse consumer, in one bundle.

    The mirror image of the shortcut case, and the one that most needs the
    producer's endpoint refresh: a Warehouse reads a Lakehouse through the
    Lakehouse's *SQL analytics endpoint*, so the view cannot be created until that
    endpoint has caught up with the table Spark just made.
    """

    # Its own producer: sharing the Lakehouse estate's would leave nothing for
    # incremental selection to build, and the ordering below is the subject here.
    producer = fabric_alias_lakehouses["warehouse_producer"]
    warehouse = clean_disposable_warehouse
    estate = _build_in_session(
        fabric_workspace,
        fabric_client,
        livy_session,
        fixture=WAREHOUSE_FIXTURE,
        bindings={
            PRODUCER: ("Lakehouse", producer.name),
            WAREHOUSE_CONSUMER: ("Warehouse", warehouse.item.name),
        },
        empty=fabric_empty_lakehouse,
    )
    return {**estate, "producer": producer, "warehouse": warehouse}


def test_a_warehouse_alias_is_a_view_over_the_bound_lakehouse(warehouse_alias_estate):
    """Asked of the Warehouse itself: the view exists and names the producer."""

    warehouse = warehouse_alias_estate["warehouse"]
    rows = warehouse.executor.query(
        "select v.TABLE_SCHEMA as s, v.TABLE_NAME as n, v.VIEW_DEFINITION as d "
        "from INFORMATION_SCHEMA.VIEWS as v "
        "where v.TABLE_SCHEMA = N'Rpt'"
    )
    views = {(row["s"], row["n"]): row["d"] or "" for row in rows}

    assert ("Rpt", "PortableCustomer") in views
    assert warehouse_alias_estate["producer"].name in views[("Rpt", "PortableCustomer")]


def test_the_warehouse_reads_the_lakehouse_table_through_its_alias(
    warehouse_alias_estate,
):
    """The claim, end to end: T-SQL in one item reading Delta owned by another."""

    warehouse = warehouse_alias_estate["warehouse"]

    through_alias = warehouse.executor.query(
        "select count(*) as n from [Rpt].[PortableCustomer]"
    )
    through_view = warehouse.executor.query(
        "select count(*) as n from [Rpt].[CustomerReport]"
    )

    # Build creates structure, never data — an empty read is the success case.
    assert through_alias[0]["n"] == 0
    assert through_view[0]["n"] == 0


def test_the_producers_endpoint_is_refreshed_before_the_warehouse_alias(
    warehouse_alias_estate,
):
    at = warehouse_alias_estate["at"]

    assert (
        at["object-Lakehouse--Raw--DWG.Customer"]
        < at["refresh-sql-endpoint-Lakehouse--Raw"]
        < at["aliases-Warehouse--Reporting"]
        < at["object-Warehouse--Reporting--Rpt.CustomerReport"]
    )


def test_the_warehouse_item_gets_no_endpoint_refresh_of_its_own(
    warehouse_alias_estate,
):
    """A Warehouse *is* reached over SQL; it has no endpoint to sync."""

    refreshes = {
        name
        for name in warehouse_alias_estate["actions"]
        if name.startswith("refresh-sql-endpoint-")
    }

    assert "refresh-sql-endpoint-Warehouse--Reporting" not in refreshes
    assert {
        "refresh-sql-endpoint-Lakehouse--Raw",
        "refresh-sql-endpoint-control",
    } <= refreshes


# --- wiping a Lakehouse that holds a shortcut ----------------------------------
#
# Last in the module deliberately: it destroys the estate the tests above assert
# on, and it is the one case where getting it wrong destroys someone else's data.


def test_wiping_the_consumer_takes_the_shortcut_and_leaves_the_producer(
    alias_estate, fabric_workspace, fabric_client, fabric_alias_lakehouses
):
    """The guarantee that matters: a wipe never reaches through a pointer.

    Removing a shortcut takes away this Lakehouse's *name* for another item's
    data. Sweeping the storage it appears in would instead operate on the
    producer's bytes — so the shortcut has to go first, through the workspace.
    """

    from weaver import wipe_lakehouse
    from weaver.fabric import OneLakeDfsClient
    from weaver.fabric.shortcuts import list_shortcuts

    producer = fabric_alias_lakehouses["producer"]
    consumer = fabric_alias_lakehouses["consumer"]
    resolver = alias_estate["resolver"]
    store = OneLakeDfsClient()
    produced = resolver.tables_root(ItemRef(producer.name)) / "DWG" / "Customer"
    assert store.exists(produced), "the producer's table must exist before the wipe"

    reports = wipe_lakehouse(
        ItemRef(consumer.name), fabric_workspace, store=store
    )

    removed = {name for report in reports for name in report.removed}
    assert "shortcut:Tables/DWG/PortableCustomer" in removed
    assert list_shortcuts(consumer, client=fabric_client) == ()
    # The whole point: the producer still has its table, and its rows.
    assert store.exists(produced)
    assert store.exists(produced / "_delta_log")
