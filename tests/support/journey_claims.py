"""The claims one Lakehouse estate makes as it is driven through a lifecycle.

The estate is installed once and each move after it is an incremental build over
a target that is already correct, so a whole build lifecycle costs roughly what a
single install used to. It replaces three module estates that each paid a full
install to ask what a *first* build does — the question the local suite already
answers, and not the one incremental logic lives in.

**One test, not many, and that is the point.** Each phase asserts the physical
state *immediately* after the transition it is about. Split into separate tests
sharing a fixture that had already run every move, each would instead be reading
the final estate — and a later move could repair what an earlier one broke, so
the earlier assertion would pass on evidence that no longer existed. A build
lifecycle is a sequence; asserting it out of order asserts something else.

The phases, in the order they must run:

``install``    the estate from nothing, the only full build. The source
               repository is deleted between generation and installation, so
               this also proves a bundle installs from itself
``unchanged``  build again having changed nothing — an estate that is already
               correct must cost nothing, which no one-shot estate could check
``prune``      seed objects the item does not declare, then build again
``broken``     last, because a failing install leaves the estate part-built and
               nothing after it could rely on what it found

**Every assertion names the Lakehouse it is about.** The session is attached to
the Weaver Lakehouse and the build writes to a different one, so a query for a
bare ``DWG.Customer`` would ask the control plane. ``env.query`` resolves object
tokens against a named destination, so a read can only succeed where the write
actually landed.
"""

from __future__ import annotations

import hashlib
from dataclasses import replace

import pytest
from support.build_envs import LAKEHOUSE_JOURNEY_FIXTURE

from weaver.targets import FolderTarget

#: `full_integration` is this file's *only* selector — it carries neither `spark`
#: nor `fabric`, so `pytest -m fabric` runs the targeted probes and leaves the
#: journey alone. That is the right default: the journey is the most expensive
#: thing in the suite and should rarely be where a component defect is found
#: first, so it is run by exception (`pytest -m full_integration`) rather than
#: paid for on every transport run. Both transports run when it is asked for.
AUDIT = {"row_insert_datetime", "row_update_datetime", "row_delete_datetime"}

#: Action kinds that are catalogue bookkeeping rather than work on the estate. A
#: build with nothing to do still writes the catalogue and closes the endpoint.
BOOKKEEPING = {
    "publish_catalogue",
    "publish_registry",
    "delete_catalogue_claims",
    "refresh_sql_endpoint",
}


def _folder(env, schema, name):
    return env.resolver.folder_object(FolderTarget(lakehouse=env.target), schema, name)


def _raise_if_the_transition_broke(step) -> None:
    """Surface a transition's own exception instead of what it left behind.

    A `Journey` records a failed move on the step rather than raising, so the
    run can report once and name the move. But an assertion that reaches
    straight for `step.bundle` then fails with `NoneType has no attribute
    'plan'` — which names neither the move nor the cause, and sends the reader
    to the assertion instead of to the build.
    """

    if step.error is not None:
        raise AssertionError(
            f"journey step {step.name!r} failed: "
            f"{type(step.error).__name__}: {step.error}"
        ) from step.error


def _observe(env, step):
    """One round trip, and everything any assertion about this moment needs.

    Taken *after* the transition's own outcome has been checked and kept on the
    step, so a failed install reports what failed rather than a bewildering error
    from querying an estate that was never built.

    Deliberately the *same* evidence after every transition, not the subset each
    one happens to check. A round trip is the cost; the statements inside it are
    nearly free, so narrowing the payload per phase would save nothing and would
    mean each transition proved a different thing about the estate.

    It spans both Lakehouses in one observation. That is the claim two-part names
    made impossible — the build wrote where it said and nowhere else — and it is
    a claim about one instant, so two calls could not make it.
    """

    weaver = env.weaver_destination
    step.observation = env.observe(
        queries={
            "tables": "SHOW TABLES IN {{schema:DWG}}",
            "views": "SHOW VIEWS IN {{schema:DWG}}",
            "customer_columns": "DESCRIBE TABLE {{object:DWG.Customer}}",
            "customer_rows": "SELECT count(*) AS n FROM {{object:DWG.Customer}}",
            "order_rows": "SELECT count(*) AS n FROM {{object:DWG.Order}}",
            "order_total": "SELECT coalesce(sum(Amount), 0) AS n FROM {{object:DWG.Order}}",
            "summary": (
                "SELECT CustomerCount FROM {{object:DWG.ActiveCustomerSummary}}"
            ),
        },
        schemas={
            "dwg": "DWG",
            "raw": "Raw",
            "legacy": "Legacy",
            # The control plane is not the destination. An unqualified CREATE
            # TABLE would land in the attached Lakehouse — this one — and a read
            # through the same session would find it and pass.
            "weaver_dwg": ("DWG", weaver),
            "weaver_raw": ("Raw", weaver),
        },
        label=f"observe {step.name}",
    )
    return step.observation


def _readable(env, seen) -> None:
    """Every declared object is present and answers a read.

    Asserted from the evidence captured *at* each transition rather than by
    re-reading at the end, because "the estate still works" is a claim about a
    moment — and a later move could otherwise repair what an earlier one broke.
    """

    assert {"customer", "order"} <= seen.values("tables", "tableName")
    assert {"activecustomer", "activecustomersummary"} <= seen.values(
        "views", "viewName"
    )
    assert seen.scalar("customer_rows") == 0
    assert seen.scalar("summary") == 0
    assert seen.schema("dwg")

    # Storage, not the catalogue — and over OneLake DFS from here rather than
    # through the session, so it stays outside the Livy budget this file is
    # careful about.
    assert env.store.exists(_folder(env, "Raw", "CustomerCsv"))


# --- the body a developer's own code runs --------------------------------------
#
# One round trip into the environment. The body is a single string run wherever
# the environment runs — in this process against local Spark, or inside a Fabric
# session over Livy — so what is asserted is genuinely the same code either side.
# Its only transport-dependent line is ``resolver``, which the environment binds
# before the body starts.
#
# The classes are declared in the body rather than imported from the installed
# repository because importing one is the load executor's job, and that does not
# exist yet. They mirror the fixture's own documents.

AUTHORED = '''
from weaver.targets import DeltaTarget, FolderTarget
from weaver import Folder, Table, lakehouse_for

lakehouse = lakehouse_for(resolver, target)
delta_target = DeltaTarget(lakehouse=target)
folder_target = FolderTarget(lakehouse=target)


class Raw__CustomerCsv(Folder):
    def read(self):
        return self.staging_folder(), []


class DWG__Customer(Table):
    def read(self):
        return [], []


class DWG__Order(Table):
    def read(self):
        return [], []


order = DWG__Order(spark, lakehouse=lakehouse)
customer = DWG__Customer(order)
export = Raw__CustomerCsv(order)

emit({
    "ids": [order.object_id, customer.object_id, export.object_id],
    "table_path": lakehouse.table_path(*order.identity),
    # Two spellings of one location, and the journey checks both: the mounted
    # Path an author writes through, and the abfss:// form an engine reads.
    "folder_path": str(export.path()),
    "folder_spark_path": export.spark_path(),
    "staging_path": str(export._staging_path()),
    "resolved_table_path": resolver.delta_table(delta_target, "DWG", "Order").value,
    "resolved_folder_path": resolver.folder_object(
        folder_target, "Raw", "CustomerCsv"
    ).value,
    "resolved_staging_path": resolver.folder_staging(
        folder_target, "Raw", "CustomerCsv"
    ).value,
    "order_columns": sorted(f.name.lower() for f in order.dataframe().schema),
    "order_rows": order.dataframe().count(),
    "empty_columns": sorted(f.name.lower() for f in order.empty_dataframe().schema),
    "customer_columns": sorted(f.name.lower() for f in customer.dataframe().schema),
    "customer_rows": customer.dataframe().count(),
})
'''


def _assert_installed(env, step) -> None:
    """The first build: everything declared exists, and nothing landed elsewhere."""

    _raise_if_the_transition_broke(step)
    plan = step.bundle.plan
    assert plan.format_version == 1
    assert plan.repository_name == env.repository_root.name
    assert {target.logical_item_name for target in plan.targets} == {"Sales", "_weaver"}
    assert plan.omitted_nodes == ()

    # The source repository was deleted between generating this bundle and
    # installing it. A bundle carries everything it needs; it is not a pointer
    # back at a repository that has to still be there.
    assert step.outcome.status == "succeeded", step.outcome.action_error
    assert list(step.outcome.action_order) == [
        action.id for _s, _b, action in plan.actions()
    ]
    assert step.outcome.bundle_id == step.bundle.bundle_id

    seen = _observe(env, step)
    _readable(env, seen)

    columns = {
        name
        for name in seen.values("customer_columns", "col_name")
        if not name.startswith("#")
    }
    assert {"customerid", "customername", "isactive"} <= columns
    assert AUDIT <= columns

    # A name alone cannot say which Lakehouse answered, so the storage is asked.
    # Case-insensitively and not by exact path: the physical name is the
    # workspace's to choose, and Fabric lowercases a managed table's directory
    # exactly as the local metastore does.
    stored = {
        entry.name.lower()
        for entry in env.store.list(env.resolver.tables_root(env.target).join("DWG"))
        if entry.is_directory
    }
    assert "customer" in stored

    # The control plane is not the destination — read out of the same payload as
    # the destination's own schemas, so both are true of the same instant.
    assert not seen.schema("weaver_dwg")
    assert not seen.schema("weaver_raw")
    assert seen.schema("dwg")

    assert env.store.exists(step.bundle.location.join("install-report.yml"))

    at = step.sequence_of

    def when(name):
        return next(seq for action_id, seq in at.items() if action_id.endswith(name))

    assert when("DWG.Customer") < when("DWG.ActiveCustomer") < when(
        "DWG.ActiveCustomerSummary"
    )
    assert when("DWG.Customer") < when("DWG.Order")
    assert when("DWG.Customer") < when("DWG.NamedCustomer")

    _assert_validation_installed(env)


def _assert_validation_installed(env) -> None:
    """The estate's Tests and Assumptions are installed, and are not objects.

    Two claims in one place, because they are two halves of the same design. The
    *runnable* things are where a build put them, under the item's own runtime
    root so their imports resolve; and nothing was materialised under a logical
    validation ID, because a Test is a declaration and not a data object.
    """

    root = env.resolver.files_root(env.target)
    for relative in (
        ("_", "Load", "tests", "DWG__OrderAmounts.py"),
        ("_", "Load", "assumptions", "DWG__OrderHasCustomer.py"),
    ):
        module = root
        for part in relative:
            module = module.join(part)
        assert env.store.exists(module), f"no validation module at {module}"

    # A compiled Spark SQL Assumption is a deployed Python module carrying the
    # authored SQL — the same arrangement a Spark SQL table gets.
    compiled = env.store.read(
        root.join("_").join("Load").join("assumptions").join("DWG__OrderHasCustomer.py")
    )
    assert b"SparkSqlAssumption" in compiled
    assert b"Assumption ID: DWG.OrderHasCustomer" in compiled


def _assert_authored_objects_reach_the_build(env) -> None:
    """A developer's own classes, resolving to what the build just made."""

    reached = env.run_python(AUTHORED)

    assert reached["ids"] == ["DWG.Order", "DWG.Customer", "Raw.CustomerCsv"]
    assert reached["table_path"] == reached["resolved_table_path"]
    assert reached["order_rows"] == 0
    assert set(reached["order_columns"]) == {"orderid", "customerid", "amount"} | AUDIT
    assert reached["customer_rows"] == 0
    assert set(reached["customer_columns"]) == {
        "customerid",
        "customername",
        "isactive",
    } | AUDIT
    assert reached["empty_columns"] == reached["order_columns"]

    # Object identity resolves through the Lakehouse's own root. Staging access
    # is transport-specific: a local path in the emulator and a session mount in
    # Fabric, both naming the same Files-relative object.
    assert reached["folder_spark_path"] == reached["resolved_folder_path"]
    assert reached["folder_path"].endswith("/Files/Raw/CustomerCsv")
    assert reached["staging_path"].endswith("/Files/Raw/CustomerCsv_Staging")
    assert reached["resolved_staging_path"].endswith(
        "/Files/Raw/CustomerCsv_Staging"
    )


def _assert_unchanged(env, step) -> None:
    """Building an already-correct estate must cost nothing and break nothing."""

    _raise_if_the_transition_broke(step)

    assert step.outcome.status == "succeeded", step.outcome.action_error

    physical = step.kinds() - BOOKKEEPING
    assert physical == set(), (
        f"a build with nothing to do performed {sorted(physical)} — something was "
        "rebuilt that did not need to be"
    )

    # Doing nothing has to mean nothing, not quietly dropping something. Read
    # here, immediately after this transition, because that is what it claims.
    _readable(env, _observe(env, step))


def _assert_pruned(env, step) -> None:
    """What the item no longer declares goes; what it declares stays."""

    _raise_if_the_transition_broke(step)

    assert step.outcome.status == "succeeded", step.outcome.action_error

    seen = _observe(env, step)
    tables = seen.values("tables", "tableName")
    assert "oldtable" not in tables
    # SHOW TABLES lists views too, so the orphaned view is checked in both.
    assert "oldview" not in tables | seen.values("views", "viewName")
    assert not seen.schema("legacy")
    assert not env.store.exists(_folder(env, "Legacy", "Stuff"))

    # Prune is the destructive direction, so what it spares is the assertion
    # worth making.
    _readable(env, seen)

    prunes = [seq for action_id, seq in step.sequence_of.items() if "prune" in action_id]
    assert prunes, "the seeded orphans should have produced prune actions"
    builds = [
        seq
        for action_id, seq in step.sequence_of.items()
        if step.actions[action_id].startswith("build_")
    ]
    if builds:
        assert max(prunes) < min(builds), "prune must run before anything is built"


# --- load orchestration -------------------------------------------------------
#
# One more transition in the journey, not a replacement for its build lifecycle.
# Its purpose here is strategic: can a real installed estate be loaded *from the
# catalogue*, with the repository playing no part in deciding what runs? The
# detailed graph, dispatch-location and task-log claims belong to the small
# orchestration test and the targeted seams; what this asks is whether the whole
# thing composes over an estate that a real build actually made.
#
# Run through `run_load` over a Session this body already holds rather than
# through `weaver.load(...)` for one reason, and it is a harness reason: the
# public entry acquires its own Spark session, and the local twin of this
# journey shares one. Everything below that line is the same code the public
# entry runs.

LOADED = '''
from weaver.load import run_load
from weaver.load_plan import PhysicalTargetRef
from weaver.locations import Location
from weaver.session import ConsoleSession

requested = (PhysicalTargetRef("lakehouse", target.name),)


def orchestrate(dry_run):
    # The Session around the Spark and store this body already has: a run
    # reaches its engines through one, and nothing here should acquire a second.
    with ConsoleSession(workspace=workspace, spark=spark, store=store) as session:
        return run_load(
            session, workspace=workspace, requested=requested, dry_run=dry_run
        ).to_mapping()


dry = orchestrate(True)
real = orchestrate(False)

# Listed here rather than from the desktop, and that is not an economy. A
# location has two spellings — the session's `abfss://` and the desktop's https
# handle over OneLake DFS — so the evidence is read by whoever wrote it.
emit({
    "dry": dry,
    "real": real,
    "log": sorted(
        entry.location.value.rsplit("/", 1)[-1]
        for entry in store.list(Location(real["task_log"]))
        if not entry.is_directory
    ),
})
'''


def _why(report) -> str:
    """Every node's status and messages, for an assertion that has to explain.

    A load run's detail is per node — the run-level message stream is usually
    empty, because what went wrong went wrong somewhere in particular. An
    assertion reporting only the run level says "failed" and nothing else.
    """

    return "\n".join(
        [f"status={report['status']} messages={report['messages']}"]
        + [
            f"  {node['status']:<24} {node['node_id']}"
            + "".join(
                f"\n      {message['code']}: {message['message']}"
                for message in node["messages"]
            )
            for node in report["nodes"]
        ]
    )


def _assert_loaded(env, seen) -> None:
    """The installed estate loads itself, from the catalogue and nothing else."""

    dry, real = seen["dry"], seen["real"]

    # Catalogue-driven: the repository is not the orchestration source, and the
    # loadable objects, their order and their primitives all came from what the
    # build recorded.
    assert dry["status"] == "succeeded", _why(dry)
    assert dry["dry_run"] is True
    assert [node["node_id"] for node in dry["nodes"]] == list(dry["order"])
    assert all(node["status"] == "validated" for node in dry["nodes"])
    assert all(node["dispatch_location"] for node in dry["nodes"])
    # Dry run is validation only: no evidence, because nothing happened.
    assert dry["task_log"] is None

    # The two views own no load work, so the graph is the folder and the three
    # tables — and the order is the one the dependencies force.
    #
    # ``DWG.NamedCustomer`` is the SQL-authored one, and it is here to prove that
    # orchestration cannot tell: it installs as a deployed module like the others
    # and takes its place in the graph by its declared dependency, not by its
    # authoring language.
    order = list(real["order"])
    assert [node_id.rsplit("/", 1)[-1] for node_id in order] == [
        "Raw.CustomerCsv",
        "DWG.Customer",
        "DWG.NamedCustomer",
        "DWG.Order",
    ]
    kinds = {
        node["node_id"].rsplit("/", 1)[-1]: node["primitive_kind"]
        for node in real["nodes"]
    }
    assert kinds["DWG.NamedCustomer"] == kinds["DWG.Customer"] == "python_table"
    assert real["status"] == "succeeded", _why(real)
    assert all(node["executed"] for node in real["nodes"])
    assert not any(
        node["status"] in ("blocked", "skipped", "failed") for node in real["nodes"]
    )
    # The dry run planned what the real run ran.
    assert order == list(dry["order"])
    assert real["edges"] == dry["edges"]


def _assert_load_materialised(env, journey) -> None:
    """Recognisable fixture values, where the estate says they should be.

    The strategic physical claim, and deliberately not a matrix: four customers
    in, three of them active, and every order derived from a customer that must
    already have been loaded for the derivation to see it.

    Its own step, so the evidence belongs to *this* moment rather than
    overwriting the transition before it — the estate has moved since.
    """

    from support.build_env import Step

    step = journey.steps["load"] = Step(name="load")
    seen = _observe(env, step)

    assert seen.scalar("customer_rows") == 4
    assert seen.scalar("summary") == 3
    assert seen.scalar("order_rows") == 4
    # Read as a number rather than compared as one: a decimal comes back as a
    # `Decimal` in this process and as its JSON spelling over Livy, and the claim
    # is about the value either way.
    assert float(seen.scalar("order_total")) == 100.0
    # The Python-defined folder materialised its files, which is what the table
    # above read to get its rows.
    assert env.store.exists(_folder(env, "Raw", "CustomerCsv").join("customers.csv"))


def _assert_task_log(env, seen) -> None:
    """One coherent task folder beneath the declared `_.Log` folder.

    Only that it is coherent. Filenames, field contracts and reconciliation
    belong to the small orchestration and task-logging tests.
    """

    from weaver.catalogue.builtin import LOG_FOLDER

    real = seen["real"]
    declared = env.resolver.folder_object(
        FolderTarget(lakehouse=env.weaver), "_", LOG_FOLDER
    )

    # `_.Log` is an ordinary Weaver folder artefact, so the desktop finds it
    # where the build installed it.
    assert env.store.exists(declared), "`_.Log` must exist as a normal Weaver folder"
    # And the task wrote beneath it. Matched on the item-relative part, because
    # the run addressed it as its own environment does and the desktop addresses
    # the same bytes differently.
    assert f"/Files/_/{LOG_FOLDER}/task_date=" in real["task_log"]

    written = seen["log"]
    assert "plan.json" in written
    assert sum("_load_" in name for name in written) == len(real["nodes"])
    assert sum("_complete_" in name for name in written) == 1


#: The summary view, rewritten. Changing it is what gives the failing transition
#: something to build: by this point the estate is correct, so an unchanged
#: repository would plan no work at all and there would be no payload to corrupt.
#: That is incremental build working — and the reason a failure case in a journey
#: has to create its own reason to run.
CHANGED_SUMMARY = """/*
View ID: DWG.ActiveCustomerSummary

Description: How many customers are active.

Lineage: $DWG.ActiveCustomer

Dependencies:
  - DWG.ActiveCustomer
*/
select
    count(1) as CustomerCount
from DWG.ActiveCustomer
"""


def _change_the_summary(env) -> None:
    env.install_repo()
    env.write_repo_file(
        "Lakehouse/Sales/DWG.ActiveCustomerSummary.sql", CHANGED_SUMMARY
    )


def _corrupt(env, bundle):
    """The same bundle with one view's payload made invalid, hashes matching."""

    from weaver.build_bundle import compute_bundle_id, write_bundle

    store = env.store
    payloads = {
        action.payload: store.read(bundle.location.join(*action.payload.split("/")))
        for _s, _b, action in bundle.plan.actions()
        if action.payload is not None
    }
    broken = (
        b"CREATE VIEW {{object:DWG.ActiveCustomerSummary}} AS\n"
        b"select count(*) as CustomerCount from {{object:DWG.ActiveCustomer}} "
        b"where NoSuchColumn = 1\n"
    )

    def fix(action):
        if action.id.endswith("DWG.ActiveCustomerSummary"):
            payloads[action.payload] = broken
            return replace(action, payload_sha256=hashlib.sha256(broken).hexdigest())
        return action

    plan = bundle.plan
    plan = replace(
        plan,
        bundle_id="",
        sequences=tuple(
            replace(
                sequence,
                batches=tuple(
                    replace(batch, actions=tuple(fix(a) for a in batch.actions))
                    for batch in sequence.batches
                ),
            )
            for sequence in plan.sequences
        ),
    )
    plan = replace(plan, bundle_id=compute_bundle_id(plan))
    return write_bundle(
        env.resolver.build_bundle("broken"),
        plan=plan,
        payloads=payloads,
        store=store,
    )


#: Validation, over the estate the load has just filled. Run through `run_test`
#: over a Session this body holds, for the reason the load body is: the public
#: entry acquires its own Spark session and the local twin shares one.
VALIDATED = '''
from weaver.load_plan import PhysicalTargetRef
from weaver.locations import Location
from weaver.session import ConsoleSession
from weaver.test import run_test

requested = (PhysicalTargetRef("lakehouse", target.name),)


def validate(**kwargs):
    with ConsoleSession(workspace=workspace, spark=spark, store=store) as session:
        return run_test(
            session, workspace=workspace, requested=requested, **kwargs
        ).to_mapping()


everything = validate()
named = validate(name="DWG.OrderAmounts")

emit({
    "everything": everything,
    "named": named,
    "log": sorted(
        entry.location.value.rsplit("/", 1)[-1]
        for entry in store.list(Location(everything["task_log"]))
        if not entry.is_directory
    ),
})
'''


def _assert_validated(env, seen) -> None:
    """The loaded estate satisfies what the repository says about it.

    This is the claim the journey was missing: a build that installs and a load
    that fills are both provable on their own, and neither says whether the rows
    that landed are the rows the estate declared they would be. A Test does, and
    it runs here — against real loaded data, through the installed catalogue,
    with the repository playing no part in deciding what runs.
    """

    everything = seen["everything"]
    assert everything["status"] == "passed", everything

    ran = {node["logical_id"].rsplit("/", 1)[-1]: node for node in everything["nodes"]}
    assert set(ran) == {"DWG.OrderAmounts", "DWG.OrderHasCustomer"}

    # One authored in Python and one compiled from Spark SQL, reaching the same
    # comparison and reporting in the same vocabulary.
    assert ran["DWG.OrderAmounts"]["kind"] == "test"
    assert ran["DWG.OrderAmounts"]["failure_count"] == 0
    assert ran["DWG.OrderHasCustomer"]["kind"] == "assumption"
    assert ran["DWG.OrderHasCustomer"]["violation_count"] == 0
    assert all(node["executed"] for node in everything["nodes"])

    totals = everything["totals"]
    assert totals == {
        "planned": 2,
        "executed": 2,
        "passed": 2,
        "failed": 0,
        "invalid": 0,
        "missing_count": 0,
        "unexpected_count": 0,
        "violation_count": 0,
    }

    # Naming one runs only it.
    named = seen["named"]
    assert [node["logical_id"].rsplit("/", 1)[-1] for node in named["nodes"]] == [
        "DWG.OrderAmounts"
    ]

    # A validation task log of its own, beside the load's, recording counts.
    written = seen["log"]
    assert "plan.json" in written
    assert sum("_test_" in name for name in written) == 1
    assert sum("_assumption_" in name for name in written) == 1
    assert sum("_complete_" in name for name in written) == 1
    assert not any("_weaver_sk" in name for name in written)


def _assert_failed(journey, step) -> None:
    """A payload that cannot run stops its barrier rather than being skipped past."""

    assert step.outcome is not None, "the failing install should have been recorded"
    assert "build_view" in step.kinds(), (
        "the changed summary should have planned a rebuild for the corruption to hit"
    )
    assert step.outcome.status == "failed"
    assert any(status == "failed" for status in step.outcome.action_status.values())

    # And the journey knows it failed, so anything after it is skipped rather
    # than asserting against a part-built estate.
    after = journey.run("after-failure")
    assert after.error is not None
    assert "broken" in str(after.error)


def drive(journey):
    """Install, rebuild unchanged, prune, then fail — asserting each in turn.

    The whole journey, transport-free. Each transition observes the estate
    exactly once, immediately, and is asserted against that payload: the journey
    mutates a live estate, so evidence read later is evidence about a different
    estate than the one the assertion names.

    The two transports differ only in which environment they hand in, so they
    share this rather than each keeping a copy that could drift.
    """

    env = journey.env

    env.install_repo()
    _assert_installed(env, journey.run("install", between=lambda e, _b: e.remove_repo()))
    _assert_authored_objects_reach_the_build(env)

    # The source goes back before every later transition: generation reads it.
    _assert_unchanged(env, journey.run("unchanged", before=lambda e: e.install_repo()))

    _assert_pruned(env, journey.run("prune", before=lambda e: e.seed_orphans()))

    # Load is one more transition, and it comes here rather than earlier for a
    # reason the journey's own shape gives: every phase before it asserts an
    # estate a build made and a load has not touched, so putting rows in one
    # would change what those phases are about. What follows is the failing
    # build, which asserts actions rather than rows.
    loaded = env.run_python(LOADED, label="load the installed estate")
    _assert_loaded(env, loaded)
    _assert_load_materialised(env, journey)
    _assert_task_log(env, loaded)

    # And now the question the estate exists to answer: does the data the load
    # just wrote satisfy what the repository says about it? Here rather than
    # anywhere earlier, because a validation over an unloaded estate would be
    # comparing two empty relations and passing for the wrong reason.
    validated = env.run_python(VALIDATED, label="validate the loaded estate")
    _assert_validated(env, validated)

    _assert_failed(
        journey,
        journey.run("broken", before=_change_the_summary, between=_corrupt),
    )
