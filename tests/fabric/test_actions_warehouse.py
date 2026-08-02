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

**The names are the checklist.** One `test_<kind>_action_<what it proves>` per
action kind a Warehouse can receive, so `pytest --collect-only -q -k _action_`
lists what is actually checked. `test_action_checklist.py` holds this file to
that list.

The inventory-fidelity tests keep their own names and stay here rather than
moving to a module of their own. A Warehouse is emptied per module and this
estate is module-scoped, so a second file would mean a second Warehouse build —
paying a real Fabric cost for a filing decision.

Each test starts from a **Weaver document**. The subject is never "can Fabric
create a view" but "the view this document declares is the view that appears".
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


def test_create_schema_action_creates_the_schema(estate, clean_disposable_warehouse):
    """A namespace with nothing in it, so nothing else proves it exists.

    Every other test reaches `DWG` by creating an object inside it. If the schema
    action were wrong the object would fail too, and the failure would name the
    object rather than the namespace.
    """

    rows = clean_disposable_warehouse.executor.query(
        "select name from sys.schemas where name = N'DWG'"
    )

    assert [str(row["name"]) for row in rows] == ["DWG"]


def test_build_table_action_is_accepted_by_fabric(estate, clean_disposable_warehouse):
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


def test_build_table_action_uses_the_declared_types(
    estate, clean_disposable_warehouse
):
    """Types are inferred from the query, server-side — only Fabric can confirm."""

    columns = columns_of(clean_disposable_warehouse, "DWG", "Customer")
    assert columns["customerid"]["type_name"] == "int"
    assert columns["customername"]["type_name"] == "varchar"
    assert columns["score"]["type_name"] == "decimal"


def test_build_table_action_makes_the_primary_key_not_nullable(
    estate, clean_disposable_warehouse
):
    columns = columns_of(clean_disposable_warehouse, "DWG", "Customer")
    assert columns["customerid"]["is_nullable"] is False
    for audit in AUDIT:
        assert columns[audit]["is_nullable"] is False, audit


def test_build_view_action_creates_a_view_over_the_table_it_reads(
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


def test_build_table_action_adds_the_declared_identity_column(
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


def test_prune_table_action_removes_an_object_nothing_declares(
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

    from support.livy_telemetry import LEDGER

    mine = [
        call
        for call in LEDGER.calls
        if "test_warehouse_boundary" in call.nodeid
    ]
    assert mine == []
