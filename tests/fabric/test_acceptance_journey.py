"""The acceptance estate, driven through Weaver's public operations.

External → Landing → Curated → Serving → Published, built and loaded from the desktop against
a real workspace. One estate, moved through an ordered series of transitions, with
the evidence for each kept on the step it belongs to.

The estate reads the foreign workspace through every shortcut shape Weaver
supports, and it reads them by *consuming* them: a broken shortcut fails because
a load could not materialise what it points at.

Scenarios run in file order and do not cascade. A failed transition is recorded
and every later scenario skips naming the step that broke.

Table shortcut installation waits for both the named relation and the Delta path
that authored Python loads read before the next item starts.
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from support import external_estate, external_seed
from support.acceptance import Acceptance
from support.build_envs import ACCEPTANCE_FIXTURE
from support.observation import observation_from, observe_body
from support.weaver_test import weaver_test

import weaver
from weaver.sessions.program import RemoteProgram

#: A build stages its repository and reads target inventories over OneLake. A
#: load and a test do neither: they submit programs and record centrally.
BUILDING = {"livy", "onelake", "rest", "tds"}
RUNNING = {"livy", "onelake", "rest", "tds"}


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
    journey.targets = sorted(physical.values())
    journey.bind = [
        f"{name}={item.split('/', 1)[1]}" for item, name in physical.items()
    ]
    return journey


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
        "cur_event": f"select EventId, Source, Kind from {curated}.`CUR`.`Event`",
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


def _catalogue_rows(journey, statement: str) -> list:
    """One query against the Weaver catalogue.

    The catalogue's own tables live in the catalogue Warehouse. A built
    Warehouse gets views over the runtime tables it needs and no others, so
    Registry and Shortcut are asked of the catalogue itself.
    """

    from weaver.targets import ItemRef, WarehouseTarget

    catalogue = journey.workspace.catalogue.split("/", 1)[1]
    executor = journey.session.sql_executor(
        WarehouseTarget(ItemRef(catalogue)), workspace=journey.workspace
    )
    return [dict(row) for row in executor.query(statement)]


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
            unbind_from=acceptance.workspace.catalogue,
            session=acceptance.session,
        ),
    )
    acceptance.require("wipe")

    step = acceptance.step(
        "build",
        lambda: weaver.build(
            acceptance.repository,
            bind=acceptance.bind,
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
        "product",
        "transaction",
        "region",
    }
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
            bind=acceptance.bind,
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
    step.observation = _observe(
        acceptance,
        {
            "named_product": (f"select ProductId from {landing}.`Source`.`Product`"),
            "delta_product": (
                "select ProductId from delta.`"
                f"{landing_location.table_path('Source', 'Product')}`"
            ),
        },
    )
    assert _ids(step.observation, "named_product", "ProductId") == [10, 20]
    assert _ids(step.observation, "delta_product", "ProductId") == [10, 20]


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
        lambda: weaver.load(acceptance.targets, session=acceptance.session),
    )
    acceptance.require("load")
    loaded = acceptance["load"].result
    assert loaded.succeeded, loaded.to_mapping()
    _assert_load_status(acceptance, loaded)

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
        lambda: weaver.test(acceptance.targets, session=acceptance.session),
    ).result
    acceptance.require("test")
    totals = tested.totals()
    assert totals["failed"] == 0, tested.to_mapping()
    assert totals["invalid"] == 0, tested.to_mapping()
    assert totals["passed"], tested.to_mapping()


# --- Scenario D: an unchanged load moves only what should move ---------------


@weaver_test(integration=True, resources=RUNNING)
def test_an_unchanged_load_moves_only_the_appending_branch(acceptance):
    """
    Intent: Incremental state prevents unnecessary movement while a deliberately
    appending branch and the non-incremental branches still behave as declared.

    Proof: a second load with no source change leaves the customer rows alone,
    adds exactly one generated event file, and leaves the stable branches equal.
    """

    acceptance.require("load")
    before = acceptance["load"].observation

    acceptance.step(
        "reload",
        lambda: weaver.load(acceptance.targets, session=acceptance.session),
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

    Proof: one insert, one update, one delete and one retirement event in the
    foreign workspace, then a load, then exact rows at every layer.
    """

    acceptance.require("reload")
    before = acceptance["reload"].observation

    acceptance.step("mutate", lambda: _mutate_the_foreign_world(acceptance))
    acceptance.require("mutate")

    acceptance.step(
        "load-mutated",
        lambda: weaver.load(acceptance.targets, session=acceptance.session),
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
        lambda: weaver.test(acceptance.targets, session=acceptance.session),
    ).result
    acceptance.require("test-mutated")
    assert tested.totals()["failed"] == 0, tested.to_mapping()
    assert tested.totals()["invalid"] == 0, tested.to_mapping()


def _mutate_the_foreign_world(journey) -> None:
    """One insert, one update, one delete, and one retirement event.

    The stable ``Reference`` schema is left alone, so the load has branches that
    must not move as well as branches that must.
    """

    from weaver.fabric.onelake import abfss_root, onelake_url
    from weaver.locations import Location

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

    # A retirement event for the customer the foreign source dropped.
    from weaver.fabric import OneLakeDfsClient

    OneLakeDfsClient().write(
        Location(
            onelake_url(
                item.workspace_id,
                item.id,
                external_estate.events_path("event-003.json"),
            )
        ),
        b'{"EventId": 3, "CustomerId": 3, "Kind": "retired"}\n',
    )


# --- Scenario F: the repository moves ---------------------------------------

#: What scenario F changes, and what each change is there to prove.
REPOSITORY_EDITS = (
    # A description only. Nothing physical follows from it.
    (
        "Lakehouse/Landing/LAND__Product.py",
        "Description: The foreign product table, copied whole.",
        "Description: Products, as the foreign estate delivers them.",
    ),
    # A new column on an unprotected table, so the physical table is replaced.
    (
        "Lakehouse/Landing/LAND__Region.py",
        "  RegionName: string",
        "  RegionName: string\n  RegionLabel: string",
    ),
    (
        "Lakehouse/Landing/LAND__Region.py",
        "        return Source__Region(self).dataframe()",
        "        return Source__Region(self).dataframe().selectExpr(\n"
        '            "RegionId", "RegionName", "upper(RegionName) as RegionLabel"\n'
        "        )",
    ),
    # A new column on the protected table. Its declaration changes and its
    # physical table must not.
    (
        "Lakehouse/Curated/CUR__Customer.py",
        "  UpdatedAt: timestamp",
        "  UpdatedAt: timestamp\n  CustomerLabel: string",
    ),
    # A view definition, which is replaced rather than migrated.
    (
        "Lakehouse/Curated/CUR.CustomerCurrent.sql",
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

    acceptance.step("edit", lambda: _edit(acceptance, REPOSITORY_EDITS))
    acceptance.require("edit")

    step = acceptance.step(
        "rebuild-changed",
        lambda: weaver.build(
            acceptance.repository,
            bind=acceptance.bind,
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
            "view": f"describe table {curated}.`CUR`.`CustomerCurrent`",
        },
    )
    seen = step.observation

    # The unprotected table was rebuilt into its new shape.
    assert "regionlabel" in seen.values("region", "col_name")

    # The protected table was not replaced, so its new column is not there.
    assert "customerlabel" not in seen.values("protected", "col_name")

    # A view is replaced rather than migrated, so its new column is there.
    assert "customerupper" in seen.values("view", "col_name")

    # Nothing else lost or changed its certification.
    after = _registry_rows(acceptance)
    unchanged = {
        key
        for key, row in after.items()
        if key in before.result and row["signature"] == before.result[key]["signature"]
    }
    assert ("Landing", "LAND", "Transaction") in unchanged
    assert ("Curated", "CUR", "Product") in unchanged


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
            bind=acceptance.bind,
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
        lambda: weaver.load(acceptance.targets, session=acceptance.session),
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
        lambda: weaver.test(acceptance.targets, session=acceptance.session),
    ).result
    acceptance.require("test-changed")
    assert tested.totals()["failed"] == 0, tested.to_mapping()
    assert tested.totals()["invalid"] == 0, tested.to_mapping()


# --- Scenario G: a failed build converges ------------------------------------

#: One revision where an earlier item needs real physical work and a later one
#: cannot install. The column is legitimate; the Warehouse query names something
#: that is not there, which Fabric refuses when the statement runs.
BREAKING_EDITS = (
    (
        "Lakehouse/Landing/LAND__Product.py",
        "  ProductName: string",
        "  ProductName: string\n  ProductLabel: string",
    ),
    (
        "Lakehouse/Landing/LAND__Product.py",
        '        return Reference(self).table("Product").dataframe()',
        '        return Reference(self).table("Product").dataframe().selectExpr(\n'
        '            "ProductId", "ProductName", "upper(ProductName) as ProductLabel"\n'
        "        )",
    ),
    (
        "Warehouse/Serving/SERVE.Summary.sql",
        "  , coalesce(sum(t.Amount), 0) as TotalAmount",
        "  , coalesce(sum(t.NoSuchColumn), 0) as TotalAmount",
    ),
)

#: Repairing only the invalid definition. The legitimate column stays.
REPAIR_EDITS = (
    (
        "Warehouse/Serving/SERVE.Summary.sql",
        "  , coalesce(sum(t.NoSuchColumn), 0) as TotalAmount",
        "  , coalesce(sum(t.Amount), 0) as TotalAmount",
    ),
)


@weaver_test(integration=True, resources=BUILDING)
def test_a_failed_build_leaves_partial_state_and_the_next_one_converges(acceptance):
    """
    Intent: A failed build may leave partial physical state, and a corrected
    build discovers physical truth and converges without manual cleanup.

    Proof: one revision where Landing legitimately changes and Serving cannot
    install. The build fails, Landing's work is really there, and after repairing
    only the invalid definition the next build succeeds and the estate is healthy.
    """

    acceptance.require("test-changed")
    acceptance.step("break", lambda: _edit(acceptance, BREAKING_EDITS))
    acceptance.require("break")

    # Recorded rather than required: this build is meant to fail, so its failure
    # must not skip the repair that follows.
    failed = weaver.build(
        acceptance.repository,
        bind=acceptance.bind,
        session=acceptance.session,
    )
    assert failed.status != "succeeded", failed.to_mapping()
    assert failed.errors, failed.to_mapping()

    # The estate is genuinely partial: Landing's legitimate work happened.
    landing = _item(acceptance, "Lakehouse/Landing")
    partial = _observe(
        acceptance, {"product": f"describe table {landing}.`LAND`.`Product`"}
    )
    assert "productlabel" in partial.values("product", "col_name")

    acceptance.step("repair", lambda: _edit(acceptance, REPAIR_EDITS))
    acceptance.require("repair")

    step = acceptance.step(
        "rebuild-repaired",
        lambda: weaver.build(
            acceptance.repository,
            bind=acceptance.bind,
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

    # No manual cleanup: the corrected build settles on the next attempt.
    settled = acceptance.step(
        "rebuild-converged",
        lambda: weaver.build(
            acceptance.repository,
            bind=acceptance.bind,
            session=acceptance.session,
        ),
    ).result
    acceptance.require("rebuild-converged")
    assert settled.status == "succeeded", settled.to_mapping()

    # And the healed estate loads and validates.
    acceptance.step(
        "load-repaired",
        lambda: weaver.load(acceptance.targets, session=acceptance.session),
    )
    acceptance.require("load-repaired")
    assert acceptance["load-repaired"].result.succeeded, acceptance[
        "load-repaired"
    ].result.to_mapping()
    _assert_load_status(acceptance, acceptance["load-repaired"].result)

    tested = acceptance.step(
        "test-repaired",
        lambda: weaver.test(acceptance.targets, session=acceptance.session),
    ).result
    acceptance.require("test-repaired")
    assert tested.totals()["failed"] == 0, tested.to_mapping()
    assert tested.totals()["invalid"] == 0, tested.to_mapping()


# --- Scenario H: the ownership boundary -------------------------------------


@weaver_test(integration=True, resources=BUILDING)
def test_wipe_removes_the_managed_estate_and_not_the_foreign_one(acceptance):
    """
    Intent: Weaver removes the estate it owns without mutating the foreign source
    workspace.

    Proof: after wiping every managed target, the foreign Lakehouse and Warehouse
    still hold their baseline, and the acceptance mutations are restored.
    """

    acceptance.require("build")
    try:
        acceptance.step(
            "final-wipe",
            lambda: weaver.wipe(
                acceptance.targets,
                unbind_from=acceptance.workspace.catalogue,
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
    finally:
        _restore_the_foreign_baseline(acceptance)


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
        workspace=Workspace(workspace=journey.external.name, lakehouses={}),
    )


def _foreign_warehouse_rows(journey, statement: str) -> list:
    return [dict(row) for row in _foreign_warehouse(journey).query(statement)]


def _restore_the_foreign_baseline(journey) -> None:
    """Put the foreign estate back, so the next run starts where this one did."""

    from weaver.fabric import OneLakeDfsClient
    from weaver.fabric.onelake import abfss_root, onelake_url
    from weaver.locations import Location

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
    store.delete(
        Location(
            onelake_url(
                item.workspace_id,
                item.id,
                external_estate.events_path("event-003.json"),
            )
        )
    )
    _foreign_warehouse(journey).execute_script(external_seed.warehouse_baseline())
