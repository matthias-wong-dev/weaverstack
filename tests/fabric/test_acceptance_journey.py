"""The acceptance estate, driven through Weaver's public operations.

External → Landing → Curated → Serving → Published, built and loaded from the desktop against
a real workspace. One estate, moved through an ordered series of transitions, with
the evidence for each kept on the step it belongs to.

The estate reads the foreign workspace through every shortcut shape Weaver
supports, and it reads them by consuming them: a broken shortcut fails because
a load could not materialise what it points at.

Scenarios run in file order and do not cascade. A failed transition is recorded
and every later scenario skips naming the step that broke.

Table shortcut installation waits for both the named relation and the Delta path
that authored Python loads read before the next item starts.

Claims that used to have Fabric modules of their own and are now made here,
against this estate, at no extra Livy cost:

- the catalogue Warehouse holds Weaver's own tables and a user's schema beside
  them, and a build reconciling ``_`` leaves the neighbour alone;
- ``_weaver`` is installed and certified, table for table;
- one endpoint refresh stands between the Curated loads and the Serving loads;
- a load reads the foreign estate through a table, a folder and a schema
  shortcut;
- a failed build takes the certification and the bookmark of the table it was
  replacing, leaves a protected table's alone, and the repaired build and the
  load after it put the first pair back.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

import pytest
from support import external_estate, external_seed
from support.acceptance import Acceptance
from support.build_envs import ACCEPTANCE_FIXTURE
from support.observation import observation_from, observe_body
from support.weaver_test import weaver_test

import weaver
from weaver.sessions.program import RemoteProgram

#: What a scenario crosses, named for the operation it drives. The two sets hold
#: the same four resources: a build and a run each reach Livy, OneLake, REST and
#: TDS over this estate. Pytest compares a declaration with the crossings its
#: claim body made, so a set that stopped being accurate would fail.
BUILDING = {"livy", "onelake", "rest", "tds"}
RUNNING = {"livy", "onelake", "rest", "tds"}

#: A schema a user owns inside the catalogue Warehouse. Weaver owns ``_`` there
#: and nothing else, so every build in this journey reconciles a catalogue that
#: has a neighbour sitting next to it.
NEIGHBOUR_SCHEMA = "Finance"
NEIGHBOUR_TABLE = "Ledger"
NEIGHBOUR_VIEW = "OpenLedger"


@pytest.fixture(scope="module")
def acceptance(
    tmp_path_factory,
    weaver_session,
    fabric_workspace,
    fabric_external_workspace_item,
    fabric_external_lakehouse,
    fabric_external_warehouse,
    fabric_target_lakehouse,
    fabric_shortcut_lakehouses,
    disposable_warehouse,
):
    """One estate, its physical bindings, and the transitions taken over it."""

    physical = {
        "Lakehouse/Landing": f"Lakehouse/{fabric_target_lakehouse.name}",
        "Lakehouse/Curated": f"Lakehouse/{fabric_shortcut_lakehouses['producer'].name}",
        "Warehouse/Serving": f"Warehouse/{disposable_warehouse.item.name}",
        "Lakehouse/Published": f"Lakehouse/{fabric_shortcut_lakehouses['consumer'].name}",
    }
    estate = ACCEPTANCE_FIXTURE.substituted(
        tmp_path_factory.mktemp("acceptance"),
        {
            "EXTERNAL_WORKSPACE": fabric_external_workspace_item.name,
            "EXTERNAL_LAKEHOUSE": fabric_external_lakehouse.name,
            "EXTERNAL_WAREHOUSE": fabric_external_warehouse.name,
        },
    )

    journey = Acceptance(name="acceptance")
    journey.estate = estate
    journey.repository = str(estate.path)
    journey.session = weaver_session
    journey.workspace = fabric_workspace
    journey.external = fabric_external_workspace_item
    journey.external_lakehouse = fabric_external_lakehouse
    journey.external_warehouse = fabric_external_warehouse
    journey.physical = physical
    journey.warehouse_target = disposable_warehouse.target
    #: What load, test and health name: the logical items.
    journey.items = sorted(physical)
    #: What a wipe names: the physical targets it empties.
    journey.targets = sorted(physical.values())
    #: What a build names: both halves, so this journey needs no configured
    #: `targets:` mapping to bind its fixed items to this tenant's own.
    journey.build_items = [f"{item}={name}" for item, name in physical.items()]

    # This module reuses a foreign estate that Scenario E mutates, so the run
    # establishes its own starting state. Trusting the previous run's teardown
    # made the next invocation depend on the last one finishing: an interrupt,
    # an earlier failure or a subset selection left Scenario E's state standing,
    # and Scenario C then read [1, 2, 4].
    _restore_the_foreign_baseline(journey)
    _require_the_foreign_baseline(journey)
    _seed_the_neighbour(journey)
    try:
        yield journey
    finally:
        # Desirable, so a person looking at the estate afterwards sees the
        # baseline. Correctness of the next run rests on the setup above.
        _restore_the_foreign_baseline(journey)


# --- addressing the estate ---------------------------------------------------


def _item(journey, logical: str) -> str:
    """Four-part Spark naming for the Lakehouse an item is bound to."""

    workspace = journey.workspace.workspace
    name = journey.physical[logical].split("/", 1)[1]
    return f"`{workspace}`.`{name}`"


def _observe(journey, queries):
    """One payload of evidence about the estate as it now stands.

    Through the Session's own Python execution, so the crossing is the production
    one and its telemetry is attributed to this test.
    """

    return observation_from(
        journey.session.execute_python(
            RemoteProgram(
                name="observe the acceptance estate",
                call=lambda: None,
                source=observe_body(queries, {}, {}),
            )
        )
    )


def _estate_evidence(journey) -> dict:
    """Every question about the loaded estate, gathered into one body."""

    landing = _item(journey, "Lakehouse/Landing")
    curated = _item(journey, "Lakehouse/Curated")
    published = _item(journey, "Lakehouse/Published")
    return {
        "land_customer": (
            f"select CustomerId, CustomerName from {landing}.`LAND`.`Customer` "
            "order by CustomerId"
        ),
        "land_region": f"select RegionId, RegionName from {landing}.`LAND`.`Region`",
        "land_transaction": (
            f"select TransactionId from {landing}.`LAND`.`Transaction`"
        ),
        "cur_customer": (
            f"select CustomerId, CustomerName from {curated}.`CUR`.`Customer` "
            "order by CustomerId"
        ),
        "cur_product": f"select ProductId from {curated}.`CUR`.`Product`",
        "cur_event": (
            f"select EventId, Source, Kind, SourceFile from {curated}.`CUR`.`Event`"
        ),
        "cur_retired": f"select CustomerId from {curated}.`CUR`.`RetiredCustomer`",
        "cur_summary": (
            "select CustomerId, TransactionCount, TotalAmount "
            f"from {curated}.`CUR`.`CustomerSummary` order by CustomerId"
        ),
        "pub_reporting": (
            "select CustomerId, CustomerName, TransactionCount, TotalAmount "
            f"from {published}.`PUB`.`Reporting` order by CustomerId"
        ),
    }


def _warehouse_rows(journey, statement: str) -> list:
    """One query against the Serving Warehouse, over the session's own TDS."""

    executor = journey.session.sql_executor(
        journey.warehouse_target, workspace=journey.workspace
    )
    return [dict(row) for row in executor.query(statement)]


def _catalogue_sql(journey):
    """TDS against the Warehouse the Weaver catalogue lives in."""

    from weaver.targets import ItemRef, WarehouseTarget

    catalogue = journey.workspace.catalogue.split("/", 1)[1]
    return journey.session.sql_executor(
        WarehouseTarget(ItemRef(catalogue)), workspace=journey.workspace
    )


def _catalogue_rows(journey, statement: str) -> list:
    """One query against the Weaver catalogue.

    The catalogue's own tables live in the catalogue Warehouse. A built
    Warehouse gets views over the runtime tables it needs and no others, so
    Registry and Shortcut are asked of the catalogue itself.
    """

    return [dict(row) for row in _catalogue_sql(journey).query(statement)]


def _seed_the_neighbour(journey) -> None:
    """Put a user-owned table and view in the catalogue Warehouse.

    Weaver owns ``_`` in that Warehouse and nothing else. Seeding this before the
    first build is what makes every later reading of it a claim: the builds in
    this journey each reconcile ``_`` with these two objects sitting beside it.
    """

    # One script per statement. A batch is compiled whole, so a create that names
    # a schema an earlier statement in the same batch made is an invalid object
    # name, and `create schema` and `create view` each have to begin their own.
    sql = _catalogue_sql(journey)
    for statement in (
        f"if schema_id(N'{NEIGHBOUR_SCHEMA}') is null "
        f"exec('create schema [{NEIGHBOUR_SCHEMA}]');",
        f"drop view if exists [{NEIGHBOUR_SCHEMA}].[{NEIGHBOUR_VIEW}];",
        f"drop table if exists [{NEIGHBOUR_SCHEMA}].[{NEIGHBOUR_TABLE}];",
        f"create table [{NEIGHBOUR_SCHEMA}].[{NEIGHBOUR_TABLE}] "
        "([Entry id] int not null, [Amount] decimal(10,2) not null);",
        f"insert into [{NEIGHBOUR_SCHEMA}].[{NEIGHBOUR_TABLE}] "
        "([Entry id], [Amount]) values (1, 10.00), (2, 20.00);",
        f"create view [{NEIGHBOUR_SCHEMA}].[{NEIGHBOUR_VIEW}] as "
        f"select [Entry id], [Amount] from [{NEIGHBOUR_SCHEMA}].[{NEIGHBOUR_TABLE}];",
    ):
        sql.execute_script(statement)


def _assert_the_neighbour_survived(journey) -> None:
    """The user's schema is there, whole, and still readable."""

    rows = _catalogue_rows(
        journey,
        "select TABLE_NAME as name from INFORMATION_SCHEMA.TABLES "
        f"where TABLE_SCHEMA = N'{NEIGHBOUR_SCHEMA}'",
    )
    assert {row["name"] for row in rows} == {NEIGHBOUR_TABLE, NEIGHBOUR_VIEW}
    readable = _catalogue_rows(
        journey,
        f"select count(*) as n from [{NEIGHBOUR_SCHEMA}].[{NEIGHBOUR_VIEW}]",
    )
    assert readable[0]["n"] == 2


def _assert_the_catalogue_is_installed(journey) -> None:
    """Weaver's own item, as the Warehouse holding ``_`` reports it.

    Every catalogue table physically there, one Installation row saying which
    Warehouse holds ``_``, and one Registry certification per table. A build
    binds ``_weaver`` and reconciles it, so an estate that built is an estate
    whose catalogue is installed.
    """

    from weaver.catalogue.tables import CATALOGUE_TABLES

    expected = {table.name.casefold() for table in CATALOGUE_TABLES}
    physical = {
        str(row["name"]).casefold()
        for row in _catalogue_rows(
            journey,
            "select name from sys.tables where schema_name(schema_id) = N'_'",
        )
    }
    assert physical == expected, sorted(physical ^ expected)

    installation = _catalogue_rows(
        journey,
        "select [Target name] as target from [_].[Installation] "
        "where [Item type] = N'Warehouse' and [Item name] = N'_weaver'",
    )
    assert [row["target"] for row in installation] == [
        str(journey.workspace.catalogue_item)
    ]

    certified = _catalogue_rows(
        journey,
        "select count(*) as n from [_].[Registry] "
        "where [Item type] = N'Warehouse' and [Item name] = N'_weaver'",
    )
    assert certified[0]["n"] == len(CATALOGUE_TABLES)


def _assert_load_status(journey, report) -> None:
    """Current operational state agrees with this successful physical load."""

    from weaver.catalogue.claims import bookmark_row
    from weaver.declaration.model import WeaverDocumentId, parse_installed_identity

    expected = set()
    for node in report.nodes:
        if not node.logical_id:
            continue
        identity = parse_installed_identity(str(node.logical_id))
        if not isinstance(identity, WeaverDocumentId):
            continue
        row = bookmark_row(identity)
        expected.add(
            (
                row["item_type"],
                row["item_name"],
                row["schema_name"],
                row["object_name"],
            )
        )

    rows = _catalogue_rows(
        journey,
        "select [Item type] as item_type, [Item name] as item_name, "
        "[Schema name] as schema_name, [Object name] as object_name, "
        "[Result] as result, [Workflow ID] as workflow_id "
        "from [_].[LoadStatus] where [Item name] in "
        "('Landing', 'Curated', 'Serving', 'Published')",
    )
    actual = {
        (
            row["item_type"],
            row["item_name"],
            row["schema_name"],
            row["object_name"],
        )
        for row in rows
    }
    assert actual == expected
    assert {row["result"] for row in rows} == {"Succeeded"}
    assert {row["workflow_id"] for row in rows} == {report.workflow_id}


def _run_identities(report) -> set:
    """Each settled node as the catalogue keys it: schema and object."""

    from weaver.catalogue.claims import bookmark_row
    from weaver.declaration.model import WeaverDocumentId, parse_installed_identity

    identities = set()
    for node in report.nodes:
        if not node.logical_id:
            continue
        identity = parse_installed_identity(str(node.logical_id))
        if not isinstance(identity, WeaverDocumentId):
            continue
        row = bookmark_row(identity)
        identities.add((row["schema_name"], row["object_name"]))
    return identities


def _assert_run_evidence(journey, report, task_type: str) -> None:
    """What a run left in `_.Log`, and in the tables that answer for its kind.

    `_.LoadStatus` says the current state and `_assert_load_status` reads it. This
    is the rest of the operational record: the history `_.Log` keeps of every
    settled node, the counts `_.LoadStatistic` keeps of what a load moved, the
    position `_.Bookmark` keeps for each loadable object, and the outcome
    `_.TestStatus` keeps per validation.

    Held here because this is the run that produced them. It moved from the
    desktop journey, which drove a build, load and test of its own to reach the
    same tables.
    """

    assert report.workflow_id
    log = _catalogue_rows(
        journey,
        "select [Log SK] as log_sk, [Task type] as task_type, "
        "[Schema name] as schema_name, [Object name] as object_name, "
        "[Result] as result from [_].[Log] "
        f"where [Workflow ID] = N'{report.workflow_id}'",
    )
    assert len(log) == len(report.nodes)
    assert {row["task_type"] for row in log} == {task_type}
    assert {row["result"] for row in log} == {"Succeeded"}
    assert all(row["log_sk"] for row in log)

    settled = _run_identities(report)
    if task_type == "load":
        statistics = _catalogue_rows(
            journey,
            "select [Schema name] as schema_name, [Object name] as object_name, "
            "[Rows read] as rows_read, [Is reload] as is_reload "
            f"from [_].[LoadStatistic] where [Workflow ID] = N'{report.workflow_id}'",
        )
        assert {
            (row["schema_name"], row["object_name"]) for row in statistics
        } == settled
        assert not [row for row in statistics if row["is_reload"]]
        # One object read something. An estate whose every count were zero would
        # satisfy the shape of this with nothing having moved.
        assert [row for row in statistics if (row["rows_read"] or 0) > 0]

        # Filter Bookmark rows to the logical items in this acceptance journey.
        # The catalogue's Bookmark table is estate-wide; other logical items using
        # the same catalogue will have their own rows there.
        item_conditions = " or ".join(
            f"([Item type] = N'{logical.split('/', 1)[0]}' and [Item name] = N'{logical.split('/', 1)[1]}')"
            for logical in journey.physical.keys()
        )
        bookmarks = _catalogue_rows(
            journey,
            "select [Schema name] as schema_name, [Object name] as object_name "
            f"from [_].[Bookmark] where {item_conditions}",
        )
        # A view has no load, so it is among the nodes and not here.
        assert {
            (row["schema_name"], row["object_name"]) for row in bookmarks
        } <= settled
        return

    status = _catalogue_rows(
        journey,
        "select [Schema name] as schema_name, [Object name] as object_name, "
        "[Test type] as kind, [Result] as result, [Failure count] as failures "
        f"from [_].[TestStatus] where [Workflow ID] = N'{report.workflow_id}'",
    )
    assert {(row["schema_name"], row["object_name"]) for row in status} == settled
    assert {row["result"] for row in status} == {"Succeeded"}
    assert {row["kind"] for row in status} <= {"Test", "Assumption"}
    assert {row["failures"] for row in status} == {0}


def _assert_the_endpoint_barrier_is_in_the_graph(journey, report) -> None:
    """The one crossing only a real workspace has, read off the run's own graph.

    Serving reads Curated's Delta tables through its SQL analytics endpoint, and
    the endpoint lags the Delta mutation. The barrier is a node so that it can be
    ordered and inspected: every Curated load runs before it, and the Serving
    loads run after it. Landing and Published are Lakehouses, so no endpoint
    stands between them and there is one refresh in the whole run.
    """

    curated = journey.physical["Lakehouse/Curated"]
    serving = journey.physical["Warehouse/Serving"]
    refresh = f"refresh:{curated}"

    assert [
        node.node_id
        for node in report.nodes
        if node.primitive_kind == "endpoint_refresh"
    ] == [refresh]

    edges = {tuple(edge) for edge in report.edges}
    assert (f"load:{curated}/Tables/CUR.Customer", refresh) in edges
    assert (refresh, f"load:{serving}/SERVE.Customer") in edges

    # Each node names the primitive it reached. A Warehouse load is the
    # generated procedure; a Lakehouse load is the module the installer deployed,
    # addressed through the Lakehouse that owns it rather than an attachment.
    by_node = report.by_node
    assert by_node[f"load:{serving}/SERVE.Customer"].dispatch_location == (
        f"{serving}/[_].[Load SERVE.Customer]"
    )
    deployed = by_node[f"load:{curated}/Tables/CUR.Customer"].dispatch_location
    assert deployed.startswith(f"{curated}/")
    # Under the area it was authored in, which is what the import namespace is.
    assert deployed.endswith("/_/Load/Tables/CUR__Customer.py")


def _ids(observation, name: str, column: str) -> list:
    return sorted(row[column] for row in observation[name])


# --- Scenario A: establish the estate ----------------------------------------


@weaver_test(integration=True, resources=BUILDING)
def test_a_realistic_estate_builds_from_nothing(acceptance):
    """
    Intent: A realistic multi-item repository builds from nothing against real
    Fabric, including shortcuts into a foreign workspace and a foreign Warehouse.

    Proof: after wiping the targets, one build succeeds and the estate holds
    every declared object, shortcut and relation.
    """

    acceptance.step(
        "wipe",
        lambda: weaver.wipe(
            acceptance.targets,
            session=acceptance.session,
        ),
    )
    acceptance.require("wipe")

    step = acceptance.step(
        "build",
        lambda: weaver.build(
            acceptance.repository,
            items=acceptance.build_items,
            session=acceptance.session,
        ),
    )
    acceptance.require("build")
    built = step.result
    if built.status != "succeeded":
        acceptance.fail("build")
    assert built.status == "succeeded", [
        (failure.action_id, failure.message) for failure in built.errors
    ]

    landing = _item(acceptance, "Lakehouse/Landing")
    curated = _item(acceptance, "Lakehouse/Curated")
    published = _item(acceptance, "Lakehouse/Published")
    step.observation = _observe(
        acceptance,
        {
            "land": f"show tables in {landing}.`LAND`",
            "landing_shortcuts": f"show tables in {landing}.`Source`",
            "landing_reference": f"show tables in {landing}.`Reference`",
            "cur": f"show tables in {curated}.`CUR`",
            "curated_shortcuts": f"show tables in {curated}.`SRC`",
            "published": f"show tables in {published}.`PUB`",
            "published_shortcuts": f"show tables in {published}.`WH`",
        },
    )
    seen = step.observation
    assert seen.values("land", "tableName") == {
        "customer",
        "product",
        "transaction",
        "region",
    }
    assert seen.values("landing_shortcuts", "tableName") == {
        "customer",
        "transaction",
        "region",
    }
    # The schema shortcut, which presents the foreign namespace whole. Landing
    # declares no schema of this name and `LAND.Product` reads its tables.
    assert seen.values("landing_reference", "tableName") == {"customer", "product"}
    assert seen.values("cur", "tableName") == {
        "customer",
        "customercurrent",
        "customersummary",
        "event",
        "product",
        "retiredcustomer",
        "transaction",
    }
    assert seen.values("curated_shortcuts", "tableName") == {
        "customer",
        "product",
        "region",
        "transaction",
    }
    assert seen.values("published", "tableName") == {"reporting"}
    assert seen.values("published_shortcuts", "tableName") == {"reporting"}

    # Folder shortcuts land in Files, so SHOW TABLES cannot see them. The
    # catalogue records every shortcut the build made, whichever area it is in.
    shortcuts = {
        (row["item"], row["id"], row["kind"])
        for row in _catalogue_rows(
            acceptance,
            "select [Item name] as item, [Shortcut ID] as id, "
            "[Shortcut type] as kind from [_].[Shortcut] "
            "where [Item name] in ('Landing', 'Curated', 'Published')",
        )
    }
    assert ("Landing", "Reference", "Schema") in shortcuts
    assert ("Landing", "Source.Events", "Folder") in shortcuts
    assert ("Curated", "SRC.SourceEvents", "Folder") in shortcuts
    assert ("Curated", "SRC.GeneratedEvents", "Folder") in shortcuts
    assert ("Published", "WH.Reporting", "Table") in shortcuts

    serving = {
        row["name"]
        for row in _warehouse_rows(
            acceptance,
            "select concat(s.name, '.', o.name) as name from sys.objects as o "
            "join sys.schemas as s on s.schema_id = o.schema_id "
            "where o.type in ('U', 'V') and s.name in ('SERVE', 'SRC')",
        )
    }
    assert {
        "SERVE.Customer",
        "SERVE.Transaction",
        "SERVE.Summary",
        "SERVE.vSummary",
        "SERVE.Reporting",
        "SRC.Customer",
        "SRC.Transaction",
        "SRC.Product",
        "SRC.RetiredCustomer",
    } <= serving, sorted(serving)

    _assert_the_catalogue_is_installed(acceptance)
    _assert_the_neighbour_survived(acceptance)


# --- Scenario B: an unchanged build is a fixed point -------------------------


@weaver_test(integration=True, resources=BUILDING)
def test_an_unchanged_build_is_a_true_fixed_point(acceptance):
    """
    Intent: An unchanged healthy estate causes no physical or catalogue churn.

    Proof: the same build again reports success and plans no action at all.
    """

    acceptance.require("build")
    before = _registry_rows(acceptance)
    step = acceptance.step(
        "rebuild",
        lambda: weaver.build(
            acceptance.repository,
            items=acceptance.build_items,
            session=acceptance.session,
        ),
    )
    acceptance.require("rebuild")
    rebuilt = step.result
    assert rebuilt.status == "succeeded", [
        (failure.action_id, failure.message) for failure in rebuilt.errors
    ]

    # Every certified object keeps the build that certified it. A rebuilt object
    # would carry the second build's instant instead.
    after = _registry_rows(acceptance)
    assert set(after) == set(before)
    churned = {
        key for key, row in after.items() if row["built"] != before[key]["built"]
    }
    assert churned == set(), sorted(churned)

    # The build's item-closing endpoint refresh is itself a state transition.
    # Observe both shortcut surfaces after that final transition, not only while
    # the shortcut action that created them was still running.
    from weaver.targets import ItemRef

    landing_name = acceptance.physical["Lakehouse/Landing"].split("/", 1)[1]
    resolver = acceptance.session.resolver(acceptance.workspace)
    landing_location = resolver.lakehouse_spark_location(ItemRef(landing_name))
    landing = _item(acceptance, "Lakehouse/Landing")
    # `Source.Region` is a table shortcut into the foreign Warehouse's stable
    # schema, so its two rows are the rows provisioning wrote and no scenario
    # moves them.
    step.observation = _observe(
        acceptance,
        {
            "named_region": (f"select RegionId from {landing}.`Source`.`Region`"),
            "delta_region": (
                "select RegionId from delta.`"
                f"{landing_location.table_path('Source', 'Region')}`"
            ),
        },
    )
    assert _ids(step.observation, "named_region", "RegionId") == [1, 2]
    assert _ids(step.observation, "delta_region", "RegionId") == [1, 2]


# --- Scenario C: the first end-to-end load ----------------------------------


@weaver_test(integration=True, resources=RUNNING)
def test_seeded_foreign_data_flows_through_every_layer(acceptance):
    """
    Intent: Seeded foreign data flows through every layer and produces the exact
    expected final data, and the installed validations agree.

    Proof: one load over all four items, then exact rows at each layer, then a
    test run reporting no failures.
    """

    acceptance.require("build")
    acceptance.step(
        "load",
        lambda: weaver.load(acceptance.items, session=acceptance.session),
    )
    acceptance.require("load")
    loaded = acceptance["load"].result
    assert loaded.succeeded, loaded.to_mapping()
    _assert_load_status(acceptance, loaded)
    _assert_run_evidence(acceptance, loaded, "load")
    _assert_the_endpoint_barrier_is_in_the_graph(acceptance, loaded)

    step = acceptance.steps["load"]
    step.observation = _observe(acceptance, _estate_evidence(acceptance))
    seen = step.observation

    # Landing copied the foreign Lakehouse and the foreign Warehouse whole.
    assert _ids(seen, "land_customer", "CustomerId") == [1, 2, 3]
    assert _ids(seen, "land_region", "RegionId") == [1, 2]
    assert _ids(seen, "land_transaction", "TransactionId") == [10, 20, 30]

    # Curated shaped them, incrementally for customers.
    assert _ids(seen, "cur_customer", "CustomerId") == [1, 2, 3]
    assert _ids(seen, "cur_product", "ProductId") == [10, 20]
    assert seen["cur_retired"] == []

    # Two foreign event files and one generated by this load.
    kinds = sorted(row["Source"] for row in seen["cur_event"])
    assert kinds == ["generated", "source", "source"]

    # The summary joins the view to the transactions.
    summary = {row["CustomerId"]: row for row in seen["cur_summary"]}
    assert sorted(summary) == [1, 2, 3]
    assert summary[1]["TransactionCount"] == 1
    assert Decimal(str(summary[1]["TotalAmount"])) == Decimal("100.00")

    # Serving materialised from Curated.
    reporting = _warehouse_rows(
        acceptance,
        "select CustomerId, TransactionCount, TotalAmount from [SERVE].[Reporting] "
        "order by CustomerId",
    )
    assert [row["CustomerId"] for row in reporting] == [1, 2, 3]
    assert _ids(seen, "pub_reporting", "CustomerId") == [1, 2, 3]

    tested = acceptance.step(
        "test",
        lambda: weaver.test(acceptance.items, session=acceptance.session),
    ).result
    acceptance.require("test")
    totals = tested.totals()
    assert totals["failed"] == 0, tested.to_mapping()
    assert totals["invalid"] == 0, tested.to_mapping()
    assert totals["passed"], tested.to_mapping()
    _assert_run_evidence(acceptance, tested, "test")


# --- Scenario D: an unchanged load moves only what should move ---------------


@weaver_test(integration=True, resources=RUNNING)
def test_an_unchanged_load_moves_only_the_appending_branch(acceptance):
    """
    Intent: Incremental state prevents unnecessary movement while an appending
    branch and the non-incremental branches still behave as declared.

    Proof: a second load with no source change leaves the customer rows alone,
    adds exactly one generated event file, and leaves the stable branches equal.
    """

    acceptance.require("load")
    before = acceptance["load"].observation

    acceptance.step(
        "reload",
        lambda: weaver.load(acceptance.items, session=acceptance.session),
    )
    acceptance.require("reload")
    assert acceptance["reload"].result.succeeded, acceptance[
        "reload"
    ].result.to_mapping()
    _assert_load_status(acceptance, acceptance["reload"].result)

    step = acceptance.steps["reload"]
    step.observation = _observe(acceptance, _estate_evidence(acceptance))
    after = step.observation

    # Unchanged sources, so the derived rows are the rows they were.
    assert _ids(after, "cur_customer", "CustomerId") == _ids(
        before, "cur_customer", "CustomerId"
    )
    assert _ids(after, "land_region", "RegionId") == _ids(
        before, "land_region", "RegionId"
    )
    assert _ids(after, "cur_product", "ProductId") == _ids(
        before, "cur_product", "ProductId"
    )

    # The generated branch appends one file per load, so the event count grows by
    # exactly one and everything else about it is the same.
    assert len(after["cur_event"]) == len(before["cur_event"]) + 1
    generated = [row for row in after["cur_event"] if row["Source"] == "generated"]
    assert len(generated) == 2


# --- Scenario E: the foreign world moves ------------------------------------


@weaver_test(integration=True, resources=RUNNING)
def test_foreign_source_movement_propagates_through_the_whole_chain(acceptance):
    """
    Intent: Real source movement propagates through shortcuts and incremental
    processing, including a retirement, while unchanged branches stay unchanged.

    Proof: one insert, one update, one delete, one retirement event and one
    withdrawn event file in the foreign workspace, then a load, then exact rows
    at every layer.
    """

    acceptance.require("reload")
    before = acceptance["reload"].observation

    acceptance.step("mutate", lambda: _mutate_the_foreign_world(acceptance))
    acceptance.require("mutate")

    acceptance.step(
        "load-mutated",
        lambda: weaver.load(acceptance.items, session=acceptance.session),
    )
    acceptance.require("load-mutated")
    assert acceptance["load-mutated"].result.succeeded, acceptance[
        "load-mutated"
    ].result.to_mapping()

    step = acceptance.steps["load-mutated"]
    step.observation = _observe(acceptance, _estate_evidence(acceptance))
    after = step.observation

    # Landing is a whole copy, so it shows the foreign estate as it now stands.
    landed = {row["CustomerId"]: row["CustomerName"] for row in after["land_customer"]}
    assert landed == {1: "Alice", 2: "Bob Updated", 4: "Diana"}

    # Curated is incremental: the update and the insert arrive through the
    # window, and the delete arrives as the explicit retire claim.
    curated = {row["CustomerId"]: row["CustomerName"] for row in after["cur_customer"]}
    assert curated == {1: "Alice", 2: "Bob Updated", 4: "Diana"}

    # The retirement event reached the feed the Warehouse reads.
    assert _ids(after, "cur_retired", "CustomerId") == [3]

    # Both halves of the Folder change feed crossed the logical shortcut. The
    # new file became a row, and the withdrawn one took its row with it.
    events = {row["EventId"]: row["SourceFile"] for row in after["cur_event"]}
    assert events[3] == "source/event-003.json"
    assert "source/event-001.json" not in events.values()
    assert 1 not in events
    assert {row["EventId"] for row in before["cur_event"]} - set(events) == {1}

    # Nothing touched the stable branches.
    assert _ids(after, "land_region", "RegionId") == _ids(
        before, "land_region", "RegionId"
    )
    assert _ids(after, "cur_product", "ProductId") == _ids(
        before, "cur_product", "ProductId"
    )

    # Serving retired 3 through its own incremental load.
    serving = _warehouse_rows(
        acceptance, "select CustomerId from [SERVE].[Customer] order by CustomerId"
    )
    assert [row["CustomerId"] for row in serving] == [1, 2, 4]
    assert _ids(after, "pub_reporting", "CustomerId") == [1, 2, 4]

    tested = acceptance.step(
        "test-mutated",
        lambda: weaver.test(acceptance.items, session=acceptance.session),
    ).result
    acceptance.require("test-mutated")
    assert tested.totals()["failed"] == 0, tested.to_mapping()
    assert tested.totals()["invalid"] == 0, tested.to_mapping()


def _mutate_the_foreign_world(journey) -> None:
    """One insert, one update, one delete, and both halves of the event drop.

    The event drop gains a retirement event and loses its first file, so Landing
    records an insert and a delete in one change document and the Curated table
    has something to retire as well as something to add.

    The stable ``Reference`` schema is left alone, so the load has branches that
    must not move as well as branches that must.
    """

    from weaver.fabric.onelake import abfss_root

    item = journey.external_lakehouse
    root = abfss_root(item.workspace_id, item.id)
    path = f"{root}/{external_estate.mutable_table_path('Customer')}"
    # Written through Spark, because the target is Delta. The instant is now, so
    # the changed rows fall inside the next incremental window.
    journey.session.execute_python(
        RemoteProgram(
            name="mutate the foreign world",
            call=lambda: None,
            source="from pyspark.sql import functions as F\n"
            f"frame = spark.read.format('delta').load({path!r})\n"
            "kept = frame.where('CustomerId in (1, 2)')\n"
            "updated = kept.withColumn(\n"
            "    'CustomerName',\n"
            "    F.when(F.col('CustomerId') == 2, F.lit('Bob Updated')).otherwise(\n"
            "        F.col('CustomerName')\n"
            "    ),\n"
            ").withColumn(\n"
            "    'UpdatedAt',\n"
            "    F.when(F.col('CustomerId') == 2, F.current_timestamp()).otherwise(\n"
            "        F.col('UpdatedAt')\n"
            "    ),\n"
            ")\n"
            "added = spark.createDataFrame(\n"
            "    [(4, 'Diana')], 'CustomerId int, CustomerName string'\n"
            ").withColumn('UpdatedAt', F.current_timestamp())\n"
            "updated.unionByName(added).write.format('delta').mode('overwrite')"
            f".save({path!r})\n"
            "emit(True)\n",
        )
    )

    # A retirement event for the customer the foreign source dropped, and the
    # withdrawal of the drop's first file.
    from weaver.fabric import OneLakeDfsClient

    store = OneLakeDfsClient()
    store.write(
        _event_location(item, "event-003.json"),
        b'{"EventId": 3, "CustomerId": 3, "Kind": "retired"}\n',
    )
    store.delete(_event_location(item, "event-001.json"))


def _event_location(item, name: str):
    """One file in the foreign event drop."""

    from weaver.fabric.onelake import onelake_url
    from weaver.locations import Location

    return Location(
        onelake_url(item.workspace_id, item.id, external_estate.events_path(name))
    )


# --- Scenario F: what the estate says about itself ---------------------------

#: What a health report crosses. No Livy: health runs no authored code, reads a
#: Warehouse over TDS and a Lakehouse over its storage.
REPORTING = {"onelake", "rest", "tds"}


@weaver_test(integration=True, resources=REPORTING)
def test_a_loaded_and_validated_estate_reports_green(acceptance):
    """
    Intent: The operational surface an operator reads agrees with the estate the
    previous scenarios built, loaded and validated.

    Proof: one health report over the whole estate, Green in every section, and
    the installed graph it evaluated against holding the relationships the
    repository declared.
    """

    acceptance.require("test-mutated")
    report = acceptance.step(
        "health",
        lambda: weaver.health(session=acceptance.session),
    ).result
    acceptance.require("health")

    assert report.load.status == "green", report.to_mapping()
    assert report.tests.status == "green", report.to_mapping()
    assert report.build.status == "green", report.to_mapping()
    assert report.status == "green"

    # The bounded window found the load that ran, and it moved rows.
    assert report.latest_load is not None
    assert report.load_activity
    assert any(each.rows_read for each in report.load_activity)

    # Every target the catalogue binds an item to was reported on.
    assert set(report.targets) >= {
        f"{name.split('/', 1)[0]}/{name.split('/', 1)[1]}"
        for name in acceptance.targets
    }


@weaver_test(integration=True, resources={"tds"})
def test_the_installed_graph_holds_the_relationships_the_repository_declared(
    acceptance,
):
    """
    Intent: `Catalogue.dag()` over a real installed estate is the composition
    proof for the graph every operation reads.

    Proof: the real catalogue, read over TDS, produces the cross-item chain the
    repository declares and holds each validation as a terminal node.
    """

    acceptance.require("health")
    dag = _installed_dag(acceptance)

    landing = _dag_id(acceptance, "Lakehouse/Landing", "LAND.Customer")
    curated = _dag_id(acceptance, "Lakehouse/Curated", "CUR.Customer")
    serving = _dag_id(acceptance, "Warehouse/Serving", "SERVE.Customer")

    # The chain crosses two item boundaries and two engines.
    assert landing in {node.node_id for node in dag.ancestors(curated)}
    assert curated in {node.node_id for node in dag.ancestors(serving)}
    assert landing in {node.node_id for node in dag.ancestors(serving)}

    # A validation reads what it validates, and nothing reads a validation.
    validations = dag.validations()
    assert validations
    for validation in validations:
        assert dag.children(validation.identity) == ()
        assert validation.is_installed

    # Every loadable names the primitive its dispatch runs.
    for node in dag.loadables():
        assert node.artefact_kind
        assert node.artefact


@weaver_test(integration=True, resources=RUNNING)
def test_loading_an_upstream_after_a_test_passed_turns_health_amber(acceptance):
    """
    Intent: Freshness is decided against the same installed graph load planning
    uses, so moving data upstream is what makes a downstream object and the
    validation over it stale.

    Proof: load one upstream table by name, which adds no ordering and moves
    nothing else, then read health again.
    """

    acceptance.require("health")
    acceptance.step(
        "load-upstream",
        lambda: weaver.load(
            ["Lakehouse/Landing"],
            names=["LAND.Customer"],
            session=acceptance.session,
        ),
    )
    acceptance.require("load-upstream")
    assert acceptance["load-upstream"].result.succeeded

    stale = acceptance.step(
        "health-stale",
        lambda: weaver.health(session=acceptance.session),
    ).result
    acceptance.require("health-stale")

    assert stale.status == "amber", stale.to_mapping()
    assert stale.build.status == "green", stale.to_mapping()
    codes = {finding.code for finding in stale.findings}
    assert "load_stale_ancestor" in codes, stale.to_mapping()
    assert "test_stale_dependency" in codes, stale.to_mapping()

    # The estate reloads and revalidates back to Green.
    acceptance.step(
        "reload-after-stale",
        lambda: weaver.load(acceptance.items, session=acceptance.session),
    )
    acceptance.require("reload-after-stale")
    acceptance.step(
        "retest-after-stale",
        lambda: weaver.test(acceptance.items, session=acceptance.session),
    )
    acceptance.require("retest-after-stale")

    recovered = acceptance.step(
        "health-recovered",
        lambda: weaver.health(session=acceptance.session),
    ).result
    acceptance.require("health-recovered")
    assert recovered.status == "green", recovered.to_mapping()


@weaver_test(integration=True)
def test_the_json_report_is_a_publishable_artefact(acceptance):
    """
    Intent: The JSON a scheduled check publishes is what the Python report says,
    over a real installed catalogue.

    Proof: the report the previous scenario already read, round-tripped through
    JSON. It crosses nothing: the report is a value, and this is what a consumer
    does with one.
    """

    import json

    acceptance.require("health-recovered")
    report = acceptance["health-recovered"].result

    payload = json.loads(json.dumps(report.to_mapping()))

    assert payload["format_version"] == 1
    assert payload["status"] == "green"
    assert set(payload["sections"]) == {"load", "tests", "build"}
    assert payload["as_of"].endswith("+00:00")
    assert payload["load_activity"]


def _installed_dag(journey):
    """The installed graph, read from the real catalogue over TDS."""

    from weaver.catalogue.state import catalogue_for
    from weaver.operations.health import HEALTH_TABLES

    catalogue = catalogue_for(journey.session, journey.workspace, tables=HEALTH_TABLES)
    return catalogue.dag()


def _dag_id(journey, item: str, qualified: str) -> str:
    """One node id, as the installed graph spells it.

    A Lakehouse data object names the area it sits in, and these are all tables.
    """

    area = "Tables/" if item.startswith("Lakehouse/") else ""
    return f"{item}/{area}{qualified}"


# --- Scenario G: the repository moves ---------------------------------------

#: What scenario G changes, and what each change is there to prove.
REPOSITORY_EDITS = (
    # A description only. Nothing physical follows from it.
    (
        "Lakehouse/Landing/Tables/LAND__Product.py",
        "Description: The foreign product table, copied whole through the schema "
        "shortcut.",
        "Description: Products, as the foreign estate delivers them.",
    ),
    # A new column on an unprotected table, so the physical table is replaced.
    (
        "Lakehouse/Landing/Tables/LAND__Region.py",
        "  RegionName: string",
        "  RegionName: string\n  RegionLabel: string",
    ),
    (
        "Lakehouse/Landing/Tables/LAND__Region.py",
        "        return Source__Region(self).dataframe()",
        "        return Source__Region(self).dataframe().selectExpr(\n"
        '            "RegionId", "RegionName", "upper(RegionName) as RegionLabel"\n'
        "        )",
    ),
    # Two changes to the protected table, both of the kind Prohibit rebuild
    # permits: a description, and load code for the loads still to come. Its
    # schema is left alone, because a declared column it does not have would
    # need the physical table altered. See design/todo/preservative-build.md.
    (
        "Lakehouse/Curated/Tables/CUR__Customer.py",
        "Description: One row per current customer, kept up to date incrementally.",
        "Description: One row per current customer, kept current incrementally.",
    ),
    (
        "Lakehouse/Curated/Tables/CUR__Customer.py",
        "        changed = source.where(source.UpdatedAt > self.bookmark())",
        "        changed = source.where(source.UpdatedAt > self.bookmark()).select(\n"
        '            "CustomerId", "CustomerName", "UpdatedAt"\n'
        "        )",
    ),
    # A view definition, which is replaced rather than migrated.
    (
        "Lakehouse/Curated/Tables/CUR.CustomerCurrent.sql",
        "    CustomerId\n  , CustomerName",
        "    CustomerId\n  , CustomerName\n  , upper(CustomerName) as CustomerUpper",
    ),
)


def _edit(journey, edits) -> None:
    """Apply one set of substitutions to the repository copy."""

    from pathlib import Path

    for relative, old, new in edits:
        path = Path(journey.repository) / relative
        text = path.read_text(encoding="utf-8")
        assert old in text, f"{relative}: {old!r} is not there to replace"
        path.write_text(text.replace(old, new, 1), encoding="utf-8")


def _registry_rows(journey) -> dict:
    """Every certified object, by item and name, with what certified it."""

    rows = _catalogue_rows(
        journey,
        "select [Item name] as item, [Schema name] as [schema], "
        "[Object name] as [object], [Signature] as signature, "
        "[Build datetime] as built from [_].[Registry]",
    )
    return {(row["item"], row["schema"], row["object"]): row for row in rows}


@weaver_test(integration=True, resources=BUILDING)
def test_a_declaration_change_rebuilds_exactly_what_it_must(acceptance):
    """
    Intent: Build changes exactly what the desired repository requires, and never
    physically replaces an object declared Prohibit rebuild.

    Proof: five edits of different impact classes, then one build. The
    unprotected table gains its new column, the protected one does not, and the
    untouched objects keep the certification they already had.
    """

    acceptance.require("load-mutated")
    before = acceptance.step("registry-before", lambda: _registry_rows(acceptance))
    acceptance.require("registry-before")

    protected = f"{_item(acceptance, 'Lakehouse/Curated')}.`CUR`.`Customer`"
    kept = acceptance.step(
        "protected-before",
        lambda: _observe(
            acceptance,
            {
                "columns": f"describe table {protected}",
                "rows": f"select CustomerId from {protected}",
            },
        ),
    )
    acceptance.require("protected-before")

    acceptance.step("edit", lambda: _edit(acceptance, REPOSITORY_EDITS))
    acceptance.require("edit")

    step = acceptance.step(
        "rebuild-changed",
        lambda: weaver.build(
            acceptance.repository,
            items=acceptance.build_items,
            session=acceptance.session,
        ),
    )
    acceptance.require("rebuild-changed")
    result = step.result
    if result.status != "succeeded":
        acceptance.fail("rebuild-changed")
    assert result.status == "succeeded", [
        (failure.action_id, failure.message) for failure in result.errors
    ]

    landing = _item(acceptance, "Lakehouse/Landing")
    curated = _item(acceptance, "Lakehouse/Curated")
    step.observation = _observe(
        acceptance,
        {
            "region": f"describe table {landing}.`LAND`.`Region`",
            "protected": f"describe table {curated}.`CUR`.`Customer`",
            "protected_rows": f"select CustomerId from {curated}.`CUR`.`Customer`",
            "view": f"describe table {curated}.`CUR`.`CustomerCurrent`",
        },
    )
    seen = step.observation

    # The unprotected table was rebuilt into its new shape.
    assert "regionlabel" in seen.values("region", "col_name")

    # The protected table was not touched at all: same columns, same rows. Its
    # declaration changed, and Prohibit rebuild protects the data it holds.
    assert seen.values("protected", "col_name") == kept.result.values(
        "columns", "col_name"
    )
    assert _ids(seen, "protected_rows", "CustomerId") == _ids(
        kept.result, "rows", "CustomerId"
    )

    # A view is replaced rather than migrated, so its new column is there.
    assert "customerupper" in seen.values("view", "col_name")

    # Nothing else lost or changed its certification.
    after = _registry_rows(acceptance)
    unchanged = {
        key
        for key, row in after.items()
        if key in before.result and row["signature"] == before.result[key]["signature"]
    }
    assert ("Landing", "Tables/LAND", "Transaction") in unchanged
    assert ("Curated", "Tables/CUR", "Product") in unchanged

    # This build reconciled the catalogue again, and the schema beside `_` is
    # still whole and still readable.
    _assert_the_neighbour_survived(acceptance)


@weaver_test(integration=True, resources=BUILDING)
def test_the_changed_estate_reaches_a_new_fixed_point(acceptance):
    """
    Intent: A changed estate settles, so a build immediately after a successful
    one plans nothing.

    Proof: the same build again succeeds and leaves every Registry signature and
    certification instant as it found them.
    """

    acceptance.require("rebuild-changed")
    before = _registry_rows(acceptance)
    acceptance.step(
        "rebuild-settled",
        lambda: weaver.build(
            acceptance.repository,
            items=acceptance.build_items,
            session=acceptance.session,
        ),
    )
    acceptance.require("rebuild-settled")
    assert acceptance["rebuild-settled"].result.status == "succeeded"

    after = _registry_rows(acceptance)
    assert set(after) == set(before)
    churned = {
        key for key, row in after.items() if row["built"] != before[key]["built"]
    }
    assert churned == set(), sorted(churned)


@weaver_test(integration=True, resources=RUNNING)
def test_the_rebuilt_estate_still_loads_and_validates(acceptance):
    """
    Intent: Rebuilt objects reload correctly while unaffected objects keep useful
    incremental state.

    Proof: a load and a test over the changed estate succeed, and the rebuilt
    column is populated.
    """

    acceptance.require("rebuild-settled")
    acceptance.step(
        "load-changed",
        lambda: weaver.load(acceptance.items, session=acceptance.session),
    )
    acceptance.require("load-changed")
    assert acceptance["load-changed"].result.succeeded, acceptance[
        "load-changed"
    ].result.to_mapping()
    _assert_load_status(acceptance, acceptance["load-changed"].result)

    landing = _item(acceptance, "Lakehouse/Landing")
    step = acceptance.steps["load-changed"]
    step.observation = _observe(
        acceptance,
        {
            "region": (
                f"select RegionId, RegionLabel from {landing}.`LAND`.`Region` "
                "order by RegionId"
            )
        },
    )
    assert [row["RegionLabel"] for row in step.observation["region"]] == [
        "NORTH",
        "SOUTH",
    ]

    tested = acceptance.step(
        "test-changed",
        lambda: weaver.test(acceptance.items, session=acceptance.session),
    ).result
    acceptance.require("test-changed")
    assert tested.totals()["failed"] == 0, tested.to_mapping()
    assert tested.totals()["invalid"] == 0, tested.to_mapping()


# --- Scenario H: a failed build converges ------------------------------------

#: One revision where earlier items need real physical work and a later one
#: cannot install. The column and the description are legitimate; the Warehouse
#: query names something that is not there, which Fabric refuses when the
#: statement runs.
#:
#: ``CUR.Customer`` is in it because it is the estate's incremental object. A
#: build invalidates the current state of everything it selects, so this revision
#: is what puts a bookmark's whole life inside one failed build and its recovery.
BREAKING_EDITS = (
    (
        "Lakehouse/Landing/Tables/LAND__Product.py",
        "  ProductName: string",
        "  ProductName: string\n  ProductLabel: string",
    ),
    (
        "Lakehouse/Landing/Tables/LAND__Product.py",
        "        return Reference(self).Product.dataframe()",
        "        return Reference(self).Product.dataframe().selectExpr(\n"
        '            "ProductId", "ProductName", "upper(ProductName) as ProductLabel"\n'
        "        )",
    ),
    (
        "Lakehouse/Curated/Tables/CUR__Customer.py",
        "Description: One row per current customer, kept current incrementally.",
        "Description: One row per current customer, kept current by the window.",
    ),
    (
        "Warehouse/Serving/SERVE.Summary.sql",
        "  , coalesce(sum(t.Amount), 0) as TotalAmount",
        "  , coalesce(sum(t.NoSuchColumn), 0) as TotalAmount",
    ),
)

#: Repairing only the invalid definition. The legitimate changes stay.
REPAIR_EDITS = (
    (
        "Warehouse/Serving/SERVE.Summary.sql",
        "  , coalesce(sum(t.NoSuchColumn), 0) as TotalAmount",
        "  , coalesce(sum(t.Amount), 0) as TotalAmount",
    ),
)

#: The estate's incremental object, as the catalogue keys it. Its load reads the
#: source rows changed since the bookmark it holds, so an absent bookmark reads
#: the whole source and a stale one reads almost nothing.
#:
#: Declared ``Prohibit rebuild``, so the failing revision's change to it is not
#: selected at all. That is what makes it the other half of this scenario: the
#: incremental position of a protected object survives a build that fails around
#: it.
INCREMENTAL = ("Lakehouse", "Curated", "Tables/CUR", "Customer")

#: The object the failing revision replaces. A new column on an unprotected table
#: means the physical table goes, so this is the one whose Registry claim the
#: build deletes before it starts.
REPLACED = ("Lakehouse", "Landing", "Tables/LAND", "Product")

#: What a bookmark reads before anything has loaded.
SENTINEL = datetime(1900, 1, 1)


def _keyed(key) -> str:
    """The four columns every current-state and Registry row is keyed by."""

    item_type, item, schema, name = key
    return (
        f"[Item type] = N'{item_type}' and [Item name] = N'{item}' "
        f"and [Schema name] = N'{schema}' and [Object name] = N'{name}'"
    )


def _certifications(journey, key) -> list:
    """When each build certified this object, one row per certification."""

    return [
        row["built"]
        for row in _catalogue_rows(
            journey,
            f"select [Build datetime] as built from [_].[Registry] where {_keyed(key)}",
        )
    ]


def _bookmarks(journey, key) -> list:
    """How far this object has loaded, one row per bookmark."""

    return [
        row["at"]
        for row in _catalogue_rows(
            journey,
            f"select [Bookmark datetime] as at from [_].[Bookmark] where {_keyed(key)}",
        )
    ]


def _rows_read(journey, key, workflow_id: str) -> list:
    """What one run's load of this object read."""

    return [
        int(row["rows_read"] or 0)
        for row in _catalogue_rows(
            journey,
            "select [Rows read] as rows_read from [_].[LoadStatistic] "
            f"where {_keyed(key)} and [Workflow ID] = N'{workflow_id}'",
        )
    ]


@weaver_test(integration=True, resources=BUILDING)
def test_a_failed_build_leaves_partial_state_and_the_next_one_converges(acceptance):
    """
    Intent: A failed build may leave partial physical state, and a corrected
    build discovers physical truth and converges without manual cleanup.

    Proof: one revision where Landing and Curated legitimately change and Serving
    cannot install. The build fails, Landing's work is really there, the replaced
    table has lost its certification and its bookmark, and the protected table
    has kept both. Repairing only the invalid definition makes the next build
    succeed, and the load after it reseeds the replaced object from nothing while
    the protected one carries on from where it was.
    """

    acceptance.require("test-changed")

    # The loads so far carried both objects forward, so each holds a bookmark of
    # its own and a certification from the build that made it.
    assert len(_certifications(acceptance, REPLACED)) == 1
    assert len(_certifications(acceptance, INCREMENTAL)) == 1
    before = {}
    for key in (REPLACED, INCREMENTAL):
        advanced = _bookmarks(acceptance, key)
        assert len(advanced) == 1, key
        assert advanced[0] > SENTINEL, key
        before[key] = advanced[0]

    acceptance.step("break", lambda: _edit(acceptance, BREAKING_EDITS))
    acceptance.require("break")

    # Recorded rather than required: this build is meant to fail, so its failure
    # must not skip the repair that follows.
    failed = weaver.build(
        acceptance.repository,
        items=acceptance.build_items,
        session=acceptance.session,
    )
    assert failed.status != "succeeded", failed.to_mapping()
    assert failed.errors, failed.to_mapping()

    # The estate is partial: Landing's legitimate work happened.
    landing = _item(acceptance, "Lakehouse/Landing")
    partial = _observe(
        acceptance, {"product": f"describe table {landing}.`LAND`.`Product`"}
    )
    assert "productlabel" in partial.values("product", "col_name")

    # Decertification and invalidation both happen before any physical work, and
    # publication is what puts certification back. This build never reached it,
    # so the replaced table is uncertified and holds no bookmark. That ordering is
    # the safety property: the next load reads the whole source rather than
    # reading almost nothing over a table that was replaced.
    assert _certifications(acceptance, REPLACED) == []
    assert _bookmarks(acceptance, REPLACED) == []

    # And the protected table kept both. Its declaration changed in this same
    # revision, and `Prohibit rebuild` keeps it out of the selection, so there
    # was no incarnation to end: the physical table stands and the position it
    # had loaded to is still recorded.
    assert len(_certifications(acceptance, INCREMENTAL)) == 1
    assert _bookmarks(acceptance, INCREMENTAL) == [before[INCREMENTAL]]

    acceptance.step("repair", lambda: _edit(acceptance, REPAIR_EDITS))
    acceptance.require("repair")

    step = acceptance.step(
        "rebuild-repaired",
        lambda: weaver.build(
            acceptance.repository,
            items=acceptance.build_items,
            session=acceptance.session,
        ),
    )
    acceptance.require("rebuild-repaired")
    repaired = step.result
    if repaired.status != "succeeded":
        acceptance.fail("rebuild-repaired")
    assert repaired.status == "succeeded", [
        (failure.action_id, failure.message) for failure in repaired.errors
    ]

    # The catalogue converged from what is physically there: certified again,
    # and still no bookmark, because a build records how far nothing has loaded.
    assert len(_certifications(acceptance, REPLACED)) == 1
    assert _bookmarks(acceptance, REPLACED) == []

    # No manual cleanup: the corrected build settles on the next attempt.
    settled = acceptance.step(
        "rebuild-converged",
        lambda: weaver.build(
            acceptance.repository,
            items=acceptance.build_items,
            session=acceptance.session,
        ),
    ).result
    acceptance.require("rebuild-converged")
    assert settled.status == "succeeded", settled.to_mapping()

    # And the healed estate loads and validates.
    acceptance.step(
        "load-repaired",
        lambda: weaver.load(acceptance.items, session=acceptance.session),
    )
    acceptance.require("load-repaired")
    reloaded = acceptance["load-repaired"].result
    assert reloaded.succeeded, reloaded.to_mapping()
    _assert_load_status(acceptance, reloaded)

    # The replaced object's bookmark is back, seeded by the load rather than by
    # the build, and later than the one the failed build took away. It read its
    # source whole, which is what a non-incremental object always reads.
    reseeded = _bookmarks(acceptance, REPLACED)
    assert len(reseeded) == 1
    assert reseeded[0] > before[REPLACED]
    assert _rows_read(acceptance, REPLACED, reloaded.workflow_id) == [2]

    # The protected object carried on from the position it kept. Nothing moved at
    # the source, so its window was empty and it merged nothing: three customers,
    # not six.
    carried = _bookmarks(acceptance, INCREMENTAL)
    assert len(carried) == 1
    assert carried[0] > before[INCREMENTAL]
    assert _rows_read(acceptance, INCREMENTAL, reloaded.workflow_id) == [0]

    curated = _item(acceptance, "Lakehouse/Curated")
    step = acceptance.steps["load-repaired"]
    step.observation = _observe(
        acceptance,
        {
            "customer": (
                f"select CustomerId, CustomerName from {curated}.`CUR`.`Customer` "
                "order by CustomerId"
            )
        },
    )
    assert _ids(step.observation, "customer", "CustomerId") == [1, 2, 4]

    tested = acceptance.step(
        "test-repaired",
        lambda: weaver.test(acceptance.items, session=acceptance.session),
    ).result
    acceptance.require("test-repaired")
    assert tested.totals()["failed"] == 0, tested.to_mapping()
    assert tested.totals()["invalid"] == 0, tested.to_mapping()


# --- Scenario I: the ownership boundary -------------------------------------


@weaver_test(integration=True, resources=BUILDING)
def test_wipe_removes_the_managed_estate_and_not_the_foreign_one(acceptance):
    """
    Intent: Weaver removes the estate it owns without mutating the foreign source
    workspace.

    Proof: after wiping every managed target, the foreign Lakehouse and Warehouse
    still hold their baseline, and the acceptance mutations are restored.
    """

    acceptance.require("build")
    acceptance.step(
        "final-wipe",
        lambda: weaver.wipe(
            acceptance.targets,
            session=acceptance.session,
        ),
    )
    acceptance.require("final-wipe")

    # The stable foreign tables are exactly as provisioning left them.
    item = acceptance.external_lakehouse
    foreign = _observe(
        acceptance,
        {
            "reference_customer": (
                "select CustomerId, CustomerName from delta.`"
                f"{_abfss(item, external_estate.table_path('Customer'))}`"
            ),
            "reference_product": (
                "select ProductId from delta.`"
                f"{_abfss(item, external_estate.table_path('Product'))}`"
            ),
            "source_customer": (
                "select CustomerId from delta.`"
                f"{_abfss(item, external_estate.mutable_table_path('Customer'))}`"
            ),
        },
    )
    assert _ids(foreign, "reference_customer", "CustomerId") == [1, 2]
    assert _ids(foreign, "reference_product", "ProductId") == [10, 20]
    # The mutation this journey made is still there: wipe touched the managed
    # estate and nothing else.
    assert _ids(foreign, "source_customer", "CustomerId") == [1, 2, 4]

    region = _foreign_warehouse_rows(
        acceptance,
        "select RegionId from [Reference].[Region] order by RegionId",
    )
    assert [row["RegionId"] for row in region] == [1, 2]


def _abfss(item, relative: str) -> str:
    from weaver.fabric.onelake import abfss_root

    return f"{abfss_root(item.workspace_id, item.id)}/{relative}"


def _foreign_warehouse(journey):
    """The Session's TDS capability for the Warehouse in the foreign workspace.

    Through the Session, so the crossing is attributed to the test that made it.
    The foreign workspace holds no catalogue and is never built into.
    """

    from weaver.targets import ItemRef, WarehouseTarget
    from weaver.workspaces import Workspace

    return journey.session.sql_executor(
        WarehouseTarget(ItemRef(journey.external_warehouse.name)),
        workspace=Workspace(workspace=journey.external.name, targets={}),
    )


def _foreign_warehouse_rows(journey, statement: str) -> list:
    return [dict(row) for row in _foreign_warehouse(journey).query(statement)]


def _require_the_foreign_baseline(journey) -> None:
    """Prove the restoration landed, before fifteen minutes rest on it.

    A partial restore is the failure worth catching here: the Delta re-seed and
    the event files are separate crossings, and a run that started from half a
    baseline reported its first wrong answer several scenarios later, as a
    difference in customer ids.
    """

    from weaver.fabric import OneLakeDfsClient

    item = journey.external_lakehouse
    seen = _observe(
        journey,
        {
            "source_customer": (
                "select CustomerId from delta.`"
                f"{_abfss(item, external_estate.mutable_table_path('Customer'))}`"
            ),
        },
    )
    customers = _ids(seen, "source_customer", "CustomerId")
    assert customers == [1, 2, 3], (
        f"the foreign baseline was not restored: Source.Customer holds "
        f"{customers}, and the acceptance journey starts from [1, 2, 3]"
    )

    store = OneLakeDfsClient()
    for name in external_estate.EVENT_FILES:
        assert store.exists(_event_location(item, name)), (
            f"the foreign baseline was not restored: {name} is absent"
        )
    withdrawn = "event-003.json"
    assert not store.exists(_event_location(item, withdrawn)), (
        f"the foreign baseline was not restored: {withdrawn} is still there, so "
        "a previous run's insertion is standing"
    )

    region = _foreign_warehouse_rows(
        journey, "select RegionId from [Reference].[Region] order by RegionId"
    )
    assert [row["RegionId"] for row in region] == [1, 2], (
        "the foreign Warehouse baseline was not restored"
    )


def _restore_the_foreign_baseline(journey) -> None:
    """Put the foreign estate back, so the next run starts where this one did."""

    from weaver.fabric import OneLakeDfsClient
    from weaver.fabric.onelake import abfss_root

    item = journey.external_lakehouse
    journey.session.execute_python(
        RemoteProgram(
            name="restore the foreign baseline",
            call=lambda: None,
            source=external_seed.lakehouse_seed_program(
                abfss_root(item.workspace_id, item.id)
            ),
        )
    )
    store = OneLakeDfsClient()
    for name, content in external_estate.EVENT_FILES.items():
        store.write(_event_location(item, name), content)
    store.delete(_event_location(item, "event-003.json"))
    _foreign_warehouse(journey).execute_script(external_seed.warehouse_baseline())
