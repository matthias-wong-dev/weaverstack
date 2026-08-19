"""The claims one Lakehouse estate makes as it is driven through a lifecycle.

The estate is installed once and each move after it is an incremental build over
a target that is already correct, so a whole build lifecycle costs roughly what a
single install used to. It replaces three module estates that each paid a full
install to ask what a *first* build does — the question the fast suite already
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

**Every assertion names the Lakehouse it is about.** A bare ``DWG.Customer``
asks whichever Lakehouse the session is attached to, which is not a claim about
where the build wrote. ``env.query`` resolves object tokens against a named
destination, so a read can only succeed where the write actually landed.
"""

from __future__ import annotations

import hashlib
from dataclasses import replace

from weaver.build_bundle.bundle import SUPPORTED_FORMAT_VERSION
from weaver.runtime.delta_sql import delta_audit_names, delta_signature_name
from weaver.targets import FolderTarget

#: `full_integration` is this file's *only* selector — it carries neither `spark`
#: nor `fabric`, so `pytest -m fabric` runs the targeted probes and leaves the
#: journey alone. That is the right default: the journey is the most expensive
#: thing in the suite and should rarely be where a component defect is found
#: first, so it is run by exception (`pytest -m full_integration`) rather than
#: paid for on every transport run. Both transports run when it is asked for.
AUDIT = set(delta_audit_names())

#: Weaver's own columns on a keyed table its load populates: the audit columns
#: and the row signature. Every table in this estate is one.
INTERNAL = AUDIT | {delta_signature_name()}

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

    One observation, so every claim below is about one instant.
    """

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
# One round trip into the environment. The body is a single string, run inside
# the Fabric session over Livy, so what is asserted is the code a developer's own
# object would run. Its only environment-dependent line is ``resolver``, which
# the environment binds before the body starts.
#
# The classes are declared in the body rather than imported from the installed
# repository because importing one is the load executor's job, and that does not
# exist yet. They mirror the fixture's own documents.

AUTHORED = """
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
"""


def _assert_installed(env, step, *, items=frozenset({"Sales", "_weaver"})) -> None:
    """The first build: everything declared exists, and nothing landed elsewhere.

    ``items`` is the logical items the bundle must carry. It is a parameter
    because the same Lakehouse estate is driven twice — alone, and with the
    Warehouse that reports on it — and every other claim below is about the
    Lakehouse either way.
    """

    _raise_if_the_transition_broke(step)
    plan = step.bundle.plan
    assert plan.format_version == SUPPORTED_FORMAT_VERSION
    assert plan.repository_name == env.repository_root.name
    assert {target.logical_item_name for target in plan.targets} == set(items)
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
    assert INTERNAL <= columns

    # A name alone cannot say which Lakehouse answered, so the storage is asked.
    # Case-insensitively and not by exact path: the physical name is the
    # workspace's to choose, and Fabric lowercases a managed table's directory
    # exactly as a case-folding metastore does.
    stored = {
        entry.name.lower()
        for entry in env.store.list(env.resolver.tables_root(env.target).join("DWG"))
        if entry.is_directory
    }
    assert "customer" in stored

    assert seen.schema("dwg")

    assert env.store.exists(step.bundle.location.join("install-report.yml"))

    at = step.sequence_of

    def when(name):
        return next(seq for action_id, seq in at.items() if action_id.endswith(name))

    assert (
        when("DWG.Customer")
        < when("DWG.ActiveCustomer")
        < when("DWG.ActiveCustomerSummary")
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
    assert (
        set(reached["order_columns"]) == {"orderid", "customerid", "amount"} | INTERNAL
    )
    assert reached["customer_rows"] == 0
    assert (
        set(reached["customer_columns"])
        == {
            "customerid",
            "customername",
            "isactive",
        }
        | INTERNAL
    )
    assert reached["empty_columns"] == reached["order_columns"]

    # Object identity resolves through the Lakehouse's own root, and staging
    # through the session's mount of it — the same Files-relative object,
    # addressed as Spark reads it and as Python opens it.
    assert reached["folder_spark_path"] == reached["resolved_folder_path"]
    assert reached["folder_path"].endswith("/Files/Raw/CustomerCsv")
    assert reached["staging_path"].endswith("/Files/Raw/CustomerCsv_Staging")
    assert reached["resolved_staging_path"].endswith("/Files/Raw/CustomerCsv_Staging")


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

    prunes = [
        seq for action_id, seq in step.sequence_of.items() if "prune" in action_id
    ]
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
#
# The Session is a `NotebookSession` because this body *is* inside Fabric. A
# `ConsoleSession` naming a Fabric workspace means the opposite — Weaver on a
# desktop, reaching in — and correctly refuses to hand out a Spark session of
# its own, however many it is given. Which host a Session is, is not a detail
# the caller may fudge; it is the whole distinction the two classes make.

#: Read a workflow's rows back from `_.Log`, in the body that produced them.
#: Shared by the two journey programs, and defined as source so it crosses with
#: them rather than being resolved on the desktop.
_LOG_ROWS = """
def _log_rows(workspace, workflow_id):
    from weaver.catalogue.connection import catalogue_connection
    from weaver.sessions.host import use_or_create_session

    with use_or_create_session(None, workspace=workspace) as reader:
        rows = catalogue_connection(reader, workspace).rows(
            "select [Task type], [Target type], [Target name], [Schema name], "
            "[Object name], [Result] from [_].[Log] "
            "where [Workflow ID] = N'" + str(workflow_id) + "'"
        )
        return [dict(row) for row in rows]
"""

LOADED = """
from weaver.operations.load import run_load
from weaver.load_plan import PhysicalTargetRef
from weaver.sessions import NotebookSession

requested = (PhysicalTargetRef("lakehouse", target.name),)


def orchestrate(dry_run):
    # The Session around the Spark and store this body already has: a run
    # reaches its engines through one, and nothing here should acquire a second.
    with NotebookSession(workspace=workspace, spark=spark, store=store) as session:
        return run_load(
            session, workspace=workspace, requested=requested, dry_run=dry_run
        ).to_mapping()


dry = orchestrate(True)
real = orchestrate(False)

# Read here rather than from the desktop, and that is not an economy. `_.Log` is
# written asynchronously and the barrier is the Session closing, so a reader in
# another crossing could see a partial tail.
emit({
    "dry": dry,
    "real": real,
    "log": _log_rows(workspace, real["workflow_id"]),
})
"""


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
    assert dry["workflow_id"] is None

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
    """One coherent workflow in `_.Log`.

    Only that it is coherent. Column contracts and the flusher's own promises
    belong to the small orchestration and flusher tests.
    """

    real = seen["real"]
    written = seen["log"]

    assert real["workflow_id"], "a real run correlates its evidence"
    # One row per settled node, and nothing else: no plan row, no completion row.
    assert len(written) == len(real["nodes"])
    assert {row["Task type"] for row in written} == {"load"}
    assert {row["Result"] for row in written} == {"Succeeded"}


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
        env.bundle_location("broken"),
        plan=plan,
        payloads=payloads,
        store=store,
    )


#: Validation, over the estate the load has just filled. Run through `run_test`
#: over a Session this body holds, for the reason the load body is: the public
#: entry acquires its own Spark session and the local twin shares one.
VALIDATED = """
from weaver.load_plan import PhysicalTargetRef
from weaver.locations import Location
from weaver.sessions import NotebookSession
from weaver.operations.test import run_test

requested = (PhysicalTargetRef("lakehouse", target.name),)


def validate(**kwargs):
    with NotebookSession(workspace=workspace, spark=spark, store=store) as session:
        return run_test(
            session, workspace=workspace, requested=requested, **kwargs
        ).to_mapping()


everything = validate()
named = validate(name="DWG.OrderAmounts")

emit({
    "everything": everything,
    "named": named,
    "log": _log_rows(workspace, everything["workflow_id"]),
})
"""


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

    # A workflow of its own in `_.Log`, beside the load's.
    written = seen["log"]
    assert {row["Task type"] for row in written} == {"test"}
    assert len(written) == len(seen["everything"]["nodes"])
    assert {row["Result"] for row in written} == {"Succeeded"}


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
    _assert_installed(
        env, journey.run("install", between=lambda e, _b: e.remove_repo())
    )
    _assert_authored_objects_reach_the_build(env)

    # The source goes back before every later transition: generation reads it.
    _assert_unchanged(env, journey.run("unchanged", before=lambda e: e.install_repo()))

    _assert_pruned(env, journey.run("prune", before=lambda e: e.seed_orphans()))

    # Load is one more transition, and it comes here rather than earlier for a
    # reason the journey's own shape gives: every phase before it asserts an
    # estate a build made and a load has not touched, so putting rows in one
    # would change what those phases are about. What follows is the failing
    # build, which asserts actions rather than rows.
    loaded = env.run_python(_LOG_ROWS + LOADED, label="load the installed estate")
    _assert_loaded(env, loaded)
    _assert_load_materialised(env, journey)
    _assert_task_log(env, loaded)

    # And now the question the estate exists to answer: does the data the load
    # just wrote satisfy what the repository says about it? Here rather than
    # anywhere earlier, because a validation over an unloaded estate would be
    # comparing two empty relations and passing for the wrong reason.
    validated = env.run_python(
        _LOG_ROWS + VALIDATED, label="validate the loaded estate"
    )
    _assert_validated(env, validated)

    _assert_failed(
        journey,
        journey.run("broken", before=_change_the_summary, between=_corrupt),
    )


# --- the same estate, with the Warehouse that reports on it -------------------
#
# `cross-item-journey` is `lakehouse-journey` byte for byte, plus a Warehouse
# item that aliases into it. So every claim above is the same claim, and what is
# added here is the composition: a Delta table published into a Warehouse
# through an alias, materialised there, and reconciled against its source.
#
# A Warehouse, so a real workspace is the only place this can run.

CROSS_ITEM_ITEMS = frozenset({"Sales", "Reporting", "_weaver"})

#: The load, over both physical sides. The Warehouse's report is a table with a
#: generated load procedure of its own, so the run graph has work either side of
#: the endpoint and a real ordering constraint between them.
CROSS_ITEM_LOADED = """
from weaver.operations.load import run_load
from weaver.load_plan import PhysicalTargetRef
from weaver.sessions import NotebookSession

requested = (
    PhysicalTargetRef("lakehouse", target.name),
    PhysicalTargetRef("warehouse", warehouse.name),
)


def orchestrate(dry_run):
    with NotebookSession(workspace=workspace, spark=spark, store=store) as session:
        return run_load(
            session, workspace=workspace, requested=requested, dry_run=dry_run
        ).to_mapping()


dry = orchestrate(True)
real = orchestrate(False)

emit({"dry": dry, "real": real})
"""

#: The validation, over both sides. The Warehouse's Test is the reconciliation —
#: the claim neither side can make alone.
CROSS_ITEM_VALIDATED = """
from weaver.load_plan import PhysicalTargetRef
from weaver.sessions import NotebookSession
from weaver.operations.test import run_test

requested = (
    PhysicalTargetRef("lakehouse", target.name),
    PhysicalTargetRef("warehouse", warehouse.name),
)

with NotebookSession(workspace=workspace, spark=spark, store=store) as session:
    emit(
        run_test(session, workspace=workspace, requested=requested).to_mapping()
    )
"""


def _warehouse_objects(env) -> dict:
    """What the Warehouse holds, in one query, as name to type."""

    rows = env.warehouse.executor.query(
        "select s.name as schema_name, o.name as object_name, o.type_desc as kind "
        "from sys.objects o join sys.schemas s on s.schema_id = o.schema_id "
        "where o.type in ('U', 'V', 'P')"
    )
    return {
        f"{row['schema_name']}.{row['object_name']}": str(row["kind"]) for row in rows
    }


def _assert_warehouse_installed(env, step) -> None:
    """The reporting side exists, and it was built after what it reads.

    The order is the claim. A Warehouse object reading an aliased Delta table
    reaches it over the SQL analytics endpoint, which is eventually consistent
    with the Lakehouse — so building the Warehouse before the refresh would read
    a table the endpoint has not seen, and each side would be self-consistent
    while the crossing between them was stale.
    """

    at = step.sequence_of

    def when(ending):
        matching = [
            number for action_id, number in at.items() if action_id.endswith(ending)
        ]
        assert matching, f"no action ends with {ending!r}"
        return min(matching)

    assert (
        when("Lakehouse--Sales--DWG.Customer")
        < when("refresh-sql-endpoint-Lakehouse--Sales")
        < when("aliases-Warehouse--Reporting")
        < when("Warehouse--Reporting--Rpt.CustomerReport")
        < when("Warehouse--Reporting--Rpt.ActiveCustomerReport")
    )

    held = _warehouse_objects(env)
    assert held.get("Rpt.PortableCustomer") == "VIEW", held
    assert held.get("Rpt.CustomerReport") == "USER_TABLE", held
    assert held.get("Rpt.ActiveCustomerReport") == "VIEW", held

    # A Warehouse table carries a generated load procedure, and its Test one of
    # its own — which is what gives the run graph something to dispatch on this
    # side of the crossing rather than only on the Lakehouse's.
    procedures = {name for name, kind in held.items() if kind == "SQL_STORED_PROCEDURE"}
    assert procedures == {
        "_.Load Rpt.CustomerReport",
        "_.Test Rpt.ReportReconciles",
    }, held


def _assert_warehouse_loaded(env, seen) -> None:
    """The report holds what the Lakehouse produced, read through the alias."""

    dry, real = seen["dry"], seen["real"]
    assert dry["status"] == "succeeded", _why(dry)
    assert real["status"] == "succeeded", _why(real)

    # Both sides ran, in one run, ordered by the crossing between them: the
    # report cannot be materialised before the table it reads through the alias.
    order = [node_id.rsplit("/", 1)[-1] for node_id in real["order"]]
    assert {"DWG.Customer", "Rpt.CustomerReport"} <= set(order), order
    assert order.index("DWG.Customer") < order.index("Rpt.CustomerReport"), order
    assert all(node["executed"] for node in real["nodes"])

    rows = env.warehouse.executor.query(
        "select count(*) as n from [Rpt].[CustomerReport]"
    )
    assert rows[0]["n"] > 0, "the report materialised nothing from its source"


def _assert_reconciled(env, seen) -> None:
    """The Test that spans both sides passes, which is the composition's claim.

    Each side is self-consistent when the shortcut between them is stale, so this
    is the one assertion that could not be made on either alone.
    """

    assert seen["status"] == "passed", seen

    ran = {node["logical_id"].rsplit("/", 1)[-1]: node for node in seen["nodes"]}
    assert "Rpt.ReportReconciles" in ran, sorted(ran)
    assert ran["Rpt.ReportReconciles"]["kind"] == "test"
    assert ran["Rpt.ReportReconciles"]["failure_count"] == 0
    assert ran["Rpt.ReportReconciles"]["executed"]


def drive_across_items(journey):
    """The journey, over an estate that spans a Lakehouse and a Warehouse.

    The Lakehouse phases are the same phases: this is the same estate with a
    second physical side, so a claim that changed here would mean the
    composition had altered what the Lakehouse half does, which it must not.
    What is added is asserted immediately after the transition it belongs to,
    for the reason every phase here is: the journey mutates a live estate.

    The failing build is deliberately absent. It corrupts a Lakehouse payload
    and asserts what a part-built estate reports, which the Lakehouse journey
    already proves and which would leave this estate's Warehouse half in a state
    nothing after it could rely on.
    """

    env = journey.env

    env.install_repo()
    step = journey.run("install", between=lambda e, _b: e.remove_repo())
    _assert_installed(env, step, items=CROSS_ITEM_ITEMS)
    _assert_warehouse_installed(env, step)

    _assert_unchanged(env, journey.run("unchanged", before=lambda e: e.install_repo()))

    loaded = env.run_python(CROSS_ITEM_LOADED, label="load across both items")
    _assert_warehouse_loaded(env, loaded)

    _assert_reconciled(
        env, env.run_python(CROSS_ITEM_VALIDATED, label="reconcile the two sides")
    )
