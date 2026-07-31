"""A Warehouse boundary, reached over TDS — and costing no Livy at all.

The Warehouse questions that genuinely need Fabric are narrow: does it accept the
T-SQL Weaver generates, does the object come out with the declared physical
types, and does an inventory read report what is there. None of them needs a
repository parsed in a session, a catalogue read, a bundle planned or an
installation reported — and reaching them that way cost two Livy submissions and
about four minutes per module.

A Warehouse is reached over TDS. So the estate is built here the same way the
Spark boundary builds one: plan the item, execute its actions. Every statement
goes over the SQL connection this process already holds, and the Livy ledger
stays at zero.

That is the whole argument for `execute_action` in one file.
"""

from __future__ import annotations

import pytest
from factories import (
    FixtureInventory,
    bound_target,
    document_id,
    item_id,
    registered_document,
    single_document_repository,
    target_inventory,
    warehouse_table,
    warehouse_view,
)

from weaver.build_bundle import execute_action, plan_item_build
from weaver.build_bundle.executors.base import InstallationContext, ResolvedTarget
from weaver.build_bundle.prune import read_warehouse_inventory
from weaver.declaration.metadata import SQL_TARGET
from weaver.locations import Location
from weaver.targets import ItemRef

pytestmark = pytest.mark.fabric

ITEM = "Warehouse/Reporting"
AUDIT = {"row insert datetime", "row update datetime", "row delete datetime"}


def warehouse_target(warehouse) -> ResolvedTarget:
    """A Warehouse resolves to no Spark address at all — it is reached over TDS."""

    return ResolvedTarget(
        bound=bound_target(
            id="target-1",
            kind="warehouse",
            item_id=warehouse.item.name,
            logical_item_name="Reporting",
            logical_item_type="Warehouse",
        ),
        lakehouse=ItemRef(warehouse.item.name),
        location=None,
        destination=None,
    )


@pytest.fixture(scope="module")
def build_warehouse_item(clean_disposable_warehouse):
    """Plan one Warehouse item and run its actions over TDS. No Livy."""

    warehouse = clean_disposable_warehouse
    target = warehouse_target(warehouse)

    def run(repository, *, inventory=None, rebuild=False, build=True):
        """``build=False`` is the unchanged estate: nothing to rebuild, only prune.

        That is the real second-build shape — signatures match, so incremental
        selection chooses no work — and it is the only way to exercise prune
        against an estate that is already correct.
        """

        identity = item_id(ITEM)
        selected = {
            key for key in repository.source_documents if key.item == identity
        }
        planned = plan_item_build(
            repository,
            item=identity,
            target=target.bound,
            inventory=inventory
            if inventory is not None
            else target_inventory(
                target_id="target-1", kind="warehouse", target_name=warehouse.item.name
            ),
            target_by_item={identity: target.bound},
            selected_documents=selected,
            selected_aliases=set(),
            selected_for_drop=set(selected) if rebuild else set(),
            selected_for_build=selected if build else set(),
            registered=(
                {key: registered_document(key) for key in selected} if rebuild else {}
            ),
        )
        context = InstallationContext(
            spark=None,
            resolver=None,
            store=None,
            snapshot=Location("/snapshot"),
            target=target,
            sql=warehouse.executor,
            targets={target.bound.id: target},
        )
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


@pytest.fixture(scope="module")
def estate(tmp_path_factory, build_warehouse_item):
    """One Warehouse estate, built once, that every assertion below reads.

    Module-scoped deliberately. A Warehouse is emptied per module, not per test,
    so a per-test build would plan against an inventory that no longer matches
    what is there — asking for a schema that already exists, which is a thing the
    planner never does. And the estate is the cost here: seven assertions over
    one build cost what one does.
    """

    root = tmp_path_factory.mktemp("warehouse-estate")
    repository = single_document_repository(
        root / "repo",
        item=ITEM,
        documents={
            "DWG.Customer.sql": warehouse_table(
                "DWG.Customer",
                select=(
                    "select cast(1 as int) as CustomerId, "
                    "cast('a' as varchar(50)) as CustomerName, "
                    "cast(1.5 as decimal(10,2)) as Score"
                ),
            ),
            "DWG.ActiveCustomer.sql": warehouse_view(
                "DWG.ActiveCustomer",
                select="select CustomerId from [DWG].[Customer]",
                depends_on="DWG.Customer",
            ),
            "DWG.CustomerDim.sql": warehouse_table(
                "DWG.CustomerDim",
                select="select CustomerId, CustomerName from [DWG].[Customer]",
                primary_key="CustomerKey",
                identity="CustomerKey",
            ),
        },
    )
    results = build_warehouse_item(repository)
    failures = {r.action_id: r.error_message for r in results if r.status == "failed"}
    assert not failures, failures
    return repository


def columns_of(warehouse, schema: str, name: str) -> dict:
    rows = warehouse.executor.query(
        f"""
        select columns.name as column_name,
               types.name as type_name,
               columns.is_nullable as is_nullable
        from sys.columns as columns
        join sys.types as types on types.user_type_id = columns.user_type_id
        where columns.object_id = object_id(N'{schema}.{name}')
        """
    )
    return {str(row["column_name"]).casefold(): row for row in rows}


# --- does Fabric accept what Weaver generates? --------------------------------


def test_a_declared_warehouse_table_builds(estate, clean_disposable_warehouse):
    """The narrowest Fabric-only claim there is: this T-SQL is valid there.

    Weaver's table script materialises and inspects its own query shape
    server-side, so nothing local can say whether Fabric accepts it. The estate
    fixture asserts the build succeeded; this asserts the object is really there
    and answers a read.
    """

    rows = clean_disposable_warehouse.executor.query(
        "select count(*) as n from [DWG].[Customer]"
    )

    assert rows[0]["n"] == 0


def test_the_declared_types_are_what_the_warehouse_creates(
    estate, clean_disposable_warehouse
):
    """Types are inferred from the query, server-side — only Fabric can confirm."""

    columns = columns_of(clean_disposable_warehouse, "DWG", "Customer")
    assert columns["customerid"]["type_name"] == "int"
    assert columns["customername"]["type_name"] == "varchar"
    assert columns["score"]["type_name"] == "decimal"


def test_the_primary_key_and_audit_columns_are_not_nullable(
    estate, clean_disposable_warehouse
):
    columns = columns_of(clean_disposable_warehouse, "DWG", "Customer")
    assert columns["customerid"]["is_nullable"] is False
    for audit in AUDIT:
        assert columns[audit]["is_nullable"] is False, audit


def test_a_warehouse_view_builds_over_the_table_it_reads(
    estate, clean_disposable_warehouse
):
    """A view is one CREATE VIEW and must be the first statement in its batch.

    That constraint is a T-SQL fact, not a Weaver decision, and it is why the
    batch executor exists. Only a real engine rejects the alternative.
    """

    rows = clean_disposable_warehouse.executor.query(
        "select count(*) as n from [DWG].[ActiveCustomer]"
    )

    assert rows[0]["n"] == 0


def test_a_dimension_gets_a_weaver_managed_bigint_surrogate(
    estate, clean_disposable_warehouse
):
    """A column the declaration asks for and the query never produces.

    `Identity: CustomerKey` names a surrogate Weaver adds itself, so the query
    below it selects only the business columns. Whether the engine then creates a
    `bigint` — and whether it tolerates the create at all — is a Fabric answer.
    """

    columns = columns_of(clean_disposable_warehouse, "DWG", "CustomerDim")

    assert columns["customerkey"]["type_name"] == "bigint"
    assert {"customerid", "customername"} <= set(columns)


def test_prune_removes_the_unmanaged_and_spares_the_managed(
    estate, build_warehouse_item, clean_disposable_warehouse
):
    """Prune executed, not merely planned — the destructive direction, for real.

    Everywhere else prune is asserted as a *decision*: pure Python renders the
    actions, and the boundary tests confirm the inventory it decided from is
    accurate. This runs the frozen T-SQL drops against a real Warehouse, which is
    the one thing neither can say — and then checks what survived, because what
    prune spares is the assertion worth making.
    """

    executor = clean_disposable_warehouse.executor
    executor.execute_script("create schema Legacy;")
    executor.execute_script("create table [Legacy].[Thing] ([x] int not null);")
    executor.execute_script("create table [DWG].[OldTable] ([x] int not null);")

    installed = read_warehouse_inventory(
        warehouse_target(clean_disposable_warehouse).bound, sql=executor
    )
    results = build_warehouse_item(estate, inventory=installed, build=False)

    failures = {r.action_id: r.error_message for r in results if r.status == "failed"}
    assert not failures, failures

    after = read_warehouse_inventory(
        warehouse_target(clean_disposable_warehouse).bound, sql=executor
    )
    remaining = {name.casefold() for name in after.tables}
    assert "dwg.oldtable" not in remaining
    assert "legacy.thing" not in remaining
    assert "legacy" not in {name.casefold() for name in after.schemas}

    # And the declared estate is untouched.
    assert {"dwg.customer", "dwg.customerdim"} <= remaining
    assert "dwg.activecustomer" in {name.casefold() for name in after.views}


# --- does a read report what a build left? ------------------------------------


def test_a_built_warehouse_reads_back_as_the_fixture_predicts(
    estate, clean_disposable_warehouse
):
    """The Warehouse half of the fidelity claim the prune suite rests on."""

    actual = read_warehouse_inventory(
        warehouse_target(clean_disposable_warehouse).bound,
        sql=clean_disposable_warehouse.executor,
    )
    predicted = FixtureInventory.from_repository(
        estate,
        item=ITEM,
        target_kind=SQL_TARGET,
        target_id="target-1",
        kind="warehouse",
    )

    folded = lambda names: {name.casefold() for name in names}  # noqa: E731
    assert folded(actual.tables) == folded(predicted.tables)
    assert folded(actual.views) == folded(predicted.views)
    assert folded(actual.schemas) == folded(predicted.schemas)


def test_an_unmanaged_object_is_seen_and_would_be_pruned(
    estate, clean_disposable_warehouse
):
    """The reader reports what is there, not what Weaver expected to be there.

    Cleans up after itself rather than relying on running last: the estate is
    shared, and a test that left an orphan behind would make the *next* test's
    prune assertion fail for a reason that has nothing to do with it.
    """

    from weaver.build_bundle.physical import item_prune_stage

    executor = clean_disposable_warehouse.executor
    executor.execute_script("create table [DWG].[OldTable] ([x] int not null);")
    try:
        actual = read_warehouse_inventory(
            warehouse_target(clean_disposable_warehouse).bound,
            sql=executor,
        )
        assert "dwg.oldtable" in {name.casefold() for name in actual.tables}

        # And the diff turns that into a removal — the pure-Python claim, now
        # against an object a real Warehouse really has.
        stage = item_prune_stage(
            estate,
            set(estate.source_documents),
            item=item_id(ITEM),
            target=warehouse_target(clean_disposable_warehouse).bound,
            inventory=actual,
        )
        assert stage is not None
        assert any(
            "OldTable" in action.id
            for batch in stage.batches
            for action in batch.actions
        )
    finally:
        executor.execute_script("drop table if exists [DWG].[OldTable];")


def test_prune_against_a_freshly_built_warehouse_finds_nothing(
    estate, clean_disposable_warehouse
):
    """Pure-Python prune, restated against a genuine Warehouse read."""

    from weaver.build_bundle.physical import item_prune_stage

    stage = item_prune_stage(
        estate,
        set(estate.source_documents),
        item=item_id(ITEM),
        target=warehouse_target(clean_disposable_warehouse).bound,
        inventory=read_warehouse_inventory(
            warehouse_target(clean_disposable_warehouse).bound,
            sql=clean_disposable_warehouse.executor,
        ),
    )

    assert stage is None


def test_the_whole_module_spends_no_livy_at_all():
    """The claim this file exists to make.

    A Warehouse is reached over TDS. Every test above builds real objects in a
    real Fabric Warehouse and reads them back, and not one of them opens a Spark
    session — where the module this replaces spent two submissions and minutes.
    """

    from livy_telemetry import LEDGER

    mine = [
        call
        for call in LEDGER.calls
        if "test_warehouse_boundary" in call.nodeid
    ]
    assert mine == []


# --- row 3: Weaver running *in* Fabric ----------------------------------------
#
# Everything above runs Weaver on this machine and reaches into the workspace
# over TDS. That is row 2, and it is the right place for the DDL and shape
# claims — they are transport-independent, and paying a session for them buys
# nothing.
#
# It is *not* the product claim. Inside a session, `sql_for` acquires its
# executor from the session's own identity through `fabric_sql_executor`; only a
# desktop caller injects one. Different authentication, different code path, and
# nothing above exercises it.
#
# So one test does. The bundle is generated on the desktop — free, pure Python,
# and the actions are provably the ones the build produces — and *installed
# inside Fabric*, which is the part that has to be real. One Livy call for what
# used to take four.


def test_a_locally_generated_bundle_installs_inside_fabric(
    tmp_path, fabric_workspace, clean_disposable_warehouse, livy_session
):
    """Weaver installing a Warehouse from inside a session, on its own identity.

    The generation is local because it is pure Python and costs nothing there.
    The installation is remote because that is where the claim lives: the frozen
    T-SQL runs through the session's Fabric-native connector, not through a
    connection this process opened.

    The same body reads the inventory back Fabric-natively, so one call answers
    both — did the install work, and does an in-session read agree with the
    desktop read the tests above rely on.
    """

    from weaver import wipe_sql_target
    from weaver.build_bundle import generate_item_build_bundle
    from weaver.declaration import parse_item_repository
    from weaver.fabric import FabricResolver, OneLakeDfsClient
    from factories import FixtureCatalogue, item_bindings

    resolver = FabricResolver(fabric_workspace)
    store = OneLakeDfsClient()
    warehouse = clean_disposable_warehouse

    # This test installs from nothing, so it starts from nothing. The Warehouse
    # is emptied per *module*, and the estate fixture above has already built
    # into it — without this the install would be asked to create a table that
    # exists, and the test would fail for a reason that is not its subject.
    # Found by exactly that: it passed alone and failed in the suite.
    wipe_sql_target(warehouse.target, warehouse.workspace, sql=warehouse.executor)

    # The declaration goes into the Weaver Lakehouse, as a user's would.
    root = resolver.weaver_items_root
    if store.exists(root):
        store.delete(root, recursive=True)
    local = tmp_path / "repo"
    single_document_repository(
        local,
        item=ITEM,
        documents={
            "DWG.Customer.sql": warehouse_table(
                "DWG.Customer",
                select="select cast(1 as int) as CustomerId",
            )
        },
    )
    for path in sorted(local.rglob("*")):
        if path.is_file() and "__pycache__" not in path.parts:
            store.write(root.join(*path.relative_to(local).parts), path.read_bytes())

    repository = parse_item_repository(root, store=store)
    bindings = item_bindings((ITEM, warehouse.item.name))
    from weaver.build_bundle import LakehouseBinding, effective_item_bindings

    bindings = effective_item_bindings(
        bindings, weaver_lakehouse=fabric_workspace.weaver_lakehouse
    )
    inventory = read_warehouse_inventory(
        warehouse_target(warehouse).bound, sql=warehouse.executor
    )
    item = item_id(ITEM)
    # Each item's inventory must carry *its own* bound target id — the planner
    # checks that pairing, and the control item's id is nothing like the
    # Warehouse's. Deriving it from the binding rather than defaulting is the
    # difference between a prepared inventory and a plausible-looking one.
    inventories = {}
    for binding in bindings.entries:
        bound = binding.to_bound_target()
        if binding.item == item:
            inventories[binding.item] = read_warehouse_inventory(
                bound, sql=warehouse.executor
            )
        else:
            # The control Lakehouse is read for real, over OneLake from here.
            # An empty inventory would be a lie rather than a simplification —
            # the catalogue schema is already there, and claiming otherwise makes
            # the planner emit a create that the session then rejects.
            from weaver.build_bundle.prune import read_lakehouse_inventory

            inventories[binding.item] = read_lakehouse_inventory(
                bound, resolver=resolver, store=store
            )
    bundle = generate_item_build_bundle(
        repository,
        bindings=bindings,
        output=resolver.build_bundle("whrow3"),
        store=store,
        target_inventories=inventories,
        # The control item's own catalogue documents are already installed, so
        # the catalogue must say so — otherwise they look new, the build tries to
        # create them again, and the session rejects tables that exist. Nothing
        # is certified for the Warehouse item, which is what makes it build.
        catalogue=FixtureCatalogue.from_repository(
            repository, item="Lakehouse/_weaver"
        ),
        control_lakehouse=LakehouseBinding(
            lakehouse=ItemRef(fabric_workspace.weaver_lakehouse)
        ),
    )

    payload = livy_session.run(
        "from weaver import FabricWorkspace, ItemRef\n"
        "from weaver.resolution import resolver_for, store_for\n"
        "from weaver.build_bundle import (InstallationEnvironment, install_bundle, "
        "load_bundle)\n"
        "from weaver.build_bundle.prune import read_warehouse_inventory\n"
        f"workspace = FabricWorkspace(workspace={fabric_workspace.workspace!r}, "
        f"weaver_lakehouse={fabric_workspace.weaver_lakehouse!r}, "
        f"environment={fabric_workspace.environment!r})\n"
        "store = store_for(workspace)\n"
        "resolver = resolver_for(workspace)\n"
        "environment = InstallationEnvironment("
        "store=store, resolver=resolver, spark=spark, workspace=workspace)\n"
        f"bundle = load_bundle(resolver.build_bundle('whrow3'), store=store)\n"
        "report = install_bundle(bundle, environment=environment)\n"
        # The same session, its own identity, reading the target back.
        "target = next(t for t in bundle.plan.targets if t.kind == 'warehouse')\n"
        "sql = environment.sql_for(target)\n"
        "seen = read_warehouse_inventory(target, sql=sql)\n"
        "emit({'status': report.status,\n"
        "      'errors': {a.action_id: a.error_message\n"
        "                 for a in report.action_results() if a.error_type},\n"
        "      'tables': list(seen.tables), 'schemas': list(seen.schemas)})\n",
        label="install in session",
    ).payload

    assert payload["status"] == "succeeded", payload["errors"]
    # The in-session read agrees with the desktop read the rest of this file uses.
    assert "DWG.Customer" in payload["tables"] or "dwg.customer" in {
        name.casefold() for name in payload["tables"]
    }
