"""Fixtures for opt-in Fabric integration tests.

These touch a real workspace and a running capacity, so they are deselected by
default: `-m fabric` is what asks for them.

The estate is permanent and named by default — `PYTEST_WORKSPACE`, holding
`PYTEST_WEAVER` and the `PYTEST_LH_*` Lakehouses — so a run needs no environment
at all. Every name is still overridable (`WEAVER_FABRIC_WORKSPACE`,
`WEAVER_PYTEST_<ROLE>`) for a tenant that arranges its items differently.

Items are found rather than made, and emptied rather than re-created. The
lifecycle tests that do create and delete carry their own `provision` marker.
"""

from __future__ import annotations

import os
import time
import uuid
from collections.abc import Callable
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping

import pytest
from support.build_env import (
    PopulatedLakehouse,
    BuildEnv,
    InstallOutcome,
    InstalledEstate,
    Journey,
    Step,
    _bindings_for,
    _install_estate,
    _outcome_from_report,
    _upload_tree,
)
from support.build_envs import LAKEHOUSE_JOURNEY_FIXTURE
from support.livy_telemetry import LEDGER, OUTSIDE_A_TEST, CountedLivySession
from support.observation import Observation, observation_from, observe_body

from weaver.workspaces import Workspace
from weaver.targets import ItemRef
from weaver.store import Store
from weaver.spark import SparkCatalogue

WORKSPACE_ENV = "WEAVER_FABRIC_WORKSPACE"
#: The permanent suite workspace. A default rather than a required export: the
#: estate it holds is permanent too, so naming it per run was ceremony.
DEFAULT_WORKSPACE = "PYTEST_WORKSPACE"
#: The Environment the session attaches to — installed once with `weaver install`
#: and consumed by the suite, never uploaded by it.
ENVIRONMENT_ENV = "WEAVER_FABRIC_ENVIRONMENT"
DEFAULT_ENVIRONMENT = "weaver"

#: Disposable items carry this prefix so an abandoned one is obvious.
TEST_PREFIX = "weavertest"
WAREHOUSE_READY_TIMEOUT = 600.0
WAREHOUSE_POLL_INTERVAL = 5.0


def _timed_session_run(session, label: str, body: str):
    """Run one meaningful Fabric phase and leave a compact timing breadcrumb.

    The label reaches the Livy ledger as well as the printed line, so the
    end-of-run breakdown can say which *phase* the round trips went on rather
    than only how many there were.
    """

    started = time.monotonic()
    try:
        return session.run(body, label=label)
    finally:
        print(f"Fabric {label}: {time.monotonic() - started:.2f}s")


# --- Livy accounting ---------------------------------------------------------
#
# Attribution is by whichever test is running when a statement is submitted.
# Session-scoped fixture setup therefore lands on the first test that asked for
# it, which is honest — that test is what paid for it — and the phase labels say
# which part was fixture work.


def pytest_runtest_logstart(nodeid, location):
    LEDGER.nodeid = nodeid


def pytest_runtest_logfinish(nodeid, location):
    LEDGER.nodeid = OUTSIDE_A_TEST


def pytest_terminal_summary(terminalreporter, exitstatus, config):
    lines = LEDGER.report()
    if not lines:
        return
    terminalreporter.write_sep("=", "Livy transport")
    for line in lines:
        terminalreporter.write_line(line)


@pytest.fixture(scope="session")
def fabric_workspace_item():
    """The suite's workspace — ``PYTEST_WORKSPACE`` unless one is named."""

    pytest.importorskip("azure.identity", reason="install the [fabric] extra")
    pytest.importorskip("requests", reason="install the [fabric] extra")

    # Credential choice is caller policy, not core's; the test infra is a caller.
    from weaver.fabric.auth import prefer_cli_credential

    prefer_cli_credential()

    name = os.environ.get(WORKSPACE_ENV, DEFAULT_WORKSPACE)

    from weaver.fabric import find_workspace

    try:
        return find_workspace(name)
    except Exception as exc:
        # Any reason at all: no credential, no network, no such workspace. Every
        # one of them means "this machine cannot run the Fabric suite", which is a
        # skip — only a WeaverError was caught before, so an unauthenticated
        # machine raised azure's ClientAuthenticationError and errored instead.
        pytest.skip(f"cannot reach workspace {name!r}: {type(exc).__name__}: {exc}")


@pytest.fixture(scope="session")
def fabric_client(fabric_workspace_item):
    from weaver.fabric import FabricClient

    return FabricClient()


def _disposable_name(role: str) -> str:
    """A name no human would have chosen, so cleanup is unambiguous.

    Still used by the ``provision`` tests, whose subject *is* creating and
    deleting items.
    """

    return f"{TEST_PREFIX}_{role}_{uuid.uuid4().hex[:8]}"


#: The estate the suite expects to already exist, by role. Fixed rather than
#: generated because creating an item is quick but waiting for its SQL endpoint
#: to provision is not bounded — and a reused item's endpoint is already there.
#: Reuse also stops the suite churning workspace artifacts underneath a
#: long-lived Spark session, which Fabric's namespace resolver reacts badly to.
#:
#: Every name is overridable, so another tenant runs the suite against its own.
FIXED_ITEMS = {
    "weaver": "PYTEST_WEAVER",
    "target": "PYTEST_LH_1",
    "producer": "PYTEST_LH_2",
    "consumer": "PYTEST_LH_3",
    "warehouse_producer": "PYTEST_HOUSE",
    "warehouse": "PYTEST_WH_1",
}


def _fixed_name(role: str) -> str:
    return os.environ.get(f"WEAVER_PYTEST_{role.upper()}", FIXED_ITEMS[role])


def _ensure_lakehouse(client, workspace, role: str):
    """The fixed Lakehouse for a role, created if this tenant has not got it yet.

    Self-healing rather than a precondition, so a first run against a new
    workspace works without a provisioning ritual — and so a run is never left
    guessing whether an absent item is a setup mistake or a test failure.
    """

    from weaver.fabric.resources import LAKEHOUSE, create_lakehouse, find_item

    name = _fixed_name(role)
    try:
        return find_item(workspace, name, item_type=LAKEHOUSE, client=client)
    except Exception:
        # The product's own creation, deliberately: the harness used to carry a
        # copy that passed enableSchemas, which is how create_lakehouse went on
        # omitting it without anything noticing.
        return create_lakehouse(workspace, name, client=client)


def _warehouse_name() -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return f"Weaver_Pytest_{timestamp}_{uuid.uuid4().hex[:4]}"


@pytest.fixture
def fabric_lakehouses(fabric_workspace_item, fabric_client):
    """A Weaver Lakehouse and a target Lakehouse, created and then deleted.

    The local equivalent of this fixture is `lakehouses`, and the pair are
    deliberately shaped the same so a test can be written against either.
    """

    from weaver.fabric import create_lakehouse, delete_item

    created = []
    try:
        weaver = create_lakehouse(
            fabric_workspace_item, _disposable_name("weaver"), client=fabric_client
        )
        created.append(weaver)
        target = create_lakehouse(
            fabric_workspace_item, _disposable_name("target"), client=fabric_client
        )
        created.append(target)
        yield {"workspace": fabric_workspace_item, "weaver": weaver, "target": target}
    finally:
        for item in created:
            try:
                delete_item(item, client=fabric_client)
            except Exception as exc:  # cleanup must not mask a test failure
                print(f"warning: could not delete {item}: {exc}")


# --- running Weaver inside Fabric --------------------------------------------
#
# Session-scoped, because a Lakehouse and a Livy session are expensive to obtain
# and cheap to reuse. This is the third execution position: not Weaver reaching
# into a workspace, but Weaver running there.
#
# Weaver is *installed* into a Fabric Environment beforehand with `weaver
# install`; the suite attaches that Environment and imports it. It never uploads
# Weaver source, copies it into /tmp, or edits sys.path.


@pytest.fixture(scope="session")
def fabric_weaver_lakehouse(fabric_workspace_item, fabric_client):
    """The fixed Lakehouse standing in as the Weaver Lakehouse for the run.

    The Livy session is created against it; Weaver itself comes from the attached
    Environment, not from here.

    **Schema-enabled**, because the catalogue lives in a schema called ``_`` and a
    Lakehouse without schemas cannot hold one. **Reused, not recreated** — which is
    also the more faithful arrangement: production has one Weaver Lakehouse for the
    life of a session, and its catalogue is meant to persist. ``initialise`` is
    idempotent, so an existing catalogue reconciles rather than collides.
    """

    return _ensure_lakehouse(fabric_client, fabric_workspace_item, "weaver")


@pytest.fixture(scope="session")
def fabric_target_lakehouse(fabric_workspace_item, fabric_client):
    """The fixed destination Lakehouse for the run.

    Isolation comes from **emptying** it on the way into each build context
    (``_empty_the_target``) rather than from replacing it. That is the same
    reconciliation a build performs, so the cleaning path is exercised rather than
    bypassed — and it is what a real installation looks like, since you do not get
    a new Lakehouse per build.
    """

    return _ensure_lakehouse(fabric_client, fabric_workspace_item, "target")


#: The roles a cross-item alias run needs its own Lakehouses for.
#:
#: A producer and a consumer, because a cross-item alias is the one thing a single
#: destination cannot express — there has to be something to point across to. And a
#: *second* producer for the Warehouse case, because sharing one would leave that
#: estate building into a Lakehouse the Lakehouse estate had already built: the
#: producer's table would be unchanged, incremental selection would correctly emit
#: no work and no endpoint refresh for it, and the ordering that test is about would
#: not be in the plan at all. Cheaper to give it its own than to weaken the test.
ALIAS_LAKEHOUSE_ROLES = ("producer", "consumer", "warehouse_producer")


@pytest.fixture(scope="session")
def fabric_alias_lakehouses(fabric_workspace_item, fabric_client):
    """The fixed Lakehouses a cross-item alias run needs, by role.

    A producer and a consumer, because a cross-item alias is the one thing a
    single destination cannot express — there has to be something to point across
    to. And a *second* producer for the Warehouse case, because sharing one would
    leave that estate building into a Lakehouse the Lakehouse estate had already
    built: the producer's table would be unchanged, incremental selection would
    correctly emit no work and no endpoint refresh for it, and the ordering that
    test is about would not be in the plan at all.
    """

    return {
        role: _ensure_lakehouse(fabric_client, fabric_workspace_item, role)
        for role in ("producer", "consumer", "warehouse_producer")
    }


@pytest.fixture(scope="session")
def environment_name():
    return os.environ.get(ENVIRONMENT_ENV, DEFAULT_ENVIRONMENT)


@pytest.fixture(scope="session")
def fabric_workspace(fabric_workspace_item, fabric_weaver_lakehouse, environment_name):
    """A workspace that names the Environment Weaver was installed into."""

    from weaver.workspaces import FabricWorkspace

    return FabricWorkspace(
        workspace=fabric_workspace_item.name,
        weaver_lakehouse=fabric_weaver_lakehouse.name,
        environment=environment_name,
    )


@pytest.fixture(scope="session")
def livy_session(fabric_workspace, fabric_client):
    """One Spark session in Fabric with the Weaver Environment attached.

    Skips — rather than fails — when the Environment is missing or carries no
    usable Weaver, because that means ``weaver install`` has not been run, which
    is a setup step, not a defect in what is under test.
    """

    from weaver.errors import CommandError
    from weaver.fabric import LivyError, LivySession, list_workspace_livy_sessions

    occupied: list[str] = []
    active_sessions = None
    try:
        active_sessions = list_workspace_livy_sessions(
            fabric_workspace, client=fabric_client, active_only=True
        )
    except Exception as exc:
        print(f"warning: could not inspect Fabric Spark sessions: {exc}")
    else:
        occupied = [
            f"{entry.lakehouse_name}/{entry.session.id}"
            + (
                f" (submitted by {entry.session.submitter_name})"
                if entry.session.submitter_name
                else ""
            )
            for entry in active_sessions
        ]
    if active_sessions is not None:
        if not active_sessions:
            print("Fabric Spark preflight: no active or queued sessions.")
        for entry in active_sessions:
            session_info = entry.session
            states = "/".join(
                state or "-" for state in (
                    session_info.scheduler_state,
                    session_info.plugin_state,
                    session_info.livy_state,
                )
            )
            print(
                f"Fabric Spark preflight: {entry.lakehouse_name}: session "
                f"{session_info.id or '?'} ({states})"
                + (
                    f"; submitted by {session_info.submitter_name}"
                    if session_info.submitter_name else ""
                )
            )

    try:
        # The session does not assert the wheel: a body that needs Weaver
        # imports it and fails on its own terms, which is both more precise
        # and what lets a test use Spark without needing a publish.
        session = LivySession.for_workspace(
            fabric_workspace, require_weaver=False
        )
    except CommandError as exc:
        pytest.skip(f"{exc}; run `weaver install` into the Environment first")
    started = time.monotonic()
    try:
        session.start()
    except LivyError as exc:
        # Name the capacity before the Environment. A capacity commonly permits
        # one Spark session, so the usual cause is that something else already
        # holds the slot — an interrupted run that never got to close its
        # session, or a notebook someone left open. The preflight already knows
        # which, and saying "Environment installed?" instead sent at least one
        # reader off to check a wheel that was perfectly fine.
        held = (
            "the capacity's session slot is held by "
            + ", ".join(occupied)
            + " — wait for it to release, or end it if it is yours; otherwise "
            if occupied
            else ""
        )
        pytest.skip(
            f"could not start a Livy session: {held}check the Environment is "
            f"installed ({exc})"
        )
    startup = time.monotonic() - started
    LEDGER.startup_seconds = startup
    print(f"Fabric Livy session startup: {startup:.2f}s")

    # Counted from here, not before: `start()` submits the bootstrap, which is
    # part of standing the session up rather than a round trip a test chose to
    # make. It is reported as startup so no one tries to optimise it away.
    counted = CountedLivySession(session)
    counted.weaver_startup_seconds = startup
    try:
        yield counted
    finally:
        session.close()


@pytest.fixture(scope="session")
def weaver_session(fabric_workspace, livy_session):
    """The suite's one :class:`~weaver.session.console.ConsoleSession`.

    The canonical Fabric fixture: one credential, one REST client, one resolver
    with one item cache, one Livy session and one TDS connection per Warehouse,
    for the whole run. Everything that used to acquire its own gets this
    instead, which is where the suite's Fabric time goes back.

    The Livy session is **given**, not acquired. The suite already starts one —
    with the preflight and the skip semantics that belong to a harness rather
    than to the product — and a capacity commonly permits exactly one, so a
    Session that started its own would queue behind the fixture's forever. A
    given resource is not closed by the Session that borrowed it.
    """

    from weaver.session import ConsoleSession

    with ConsoleSession(
        workspace=fabric_workspace, livy=livy_session, require_weaver=False
    ) as session:
        yield session
        print(f"\n{session.telemetry.report()}")


@pytest.fixture
def fresh_weaver_session(fabric_workspace):
    """A Session of its own, for a test whose claim *is* runtime isolation.

    Explicit because it is expensive and rarely what a test means. Estate
    isolation is a different question and is answered by a different fixture: a
    test needing a clean target and nothing more should keep this Session.
    """

    from weaver.session import ConsoleSession

    with ConsoleSession(workspace=fabric_workspace, require_weaver=False) as session:
        yield session


# --- disposable Warehouse ----------------------------------------------------


@dataclass
class DisposableWarehouse:
    item: Any
    workspace: Workspace
    target: Any
    endpoint: Any
    executor: Any
    timings: dict[str, float]
    started: float


@pytest.fixture(scope="session")
def disposable_warehouse(fabric_workspace_item, fabric_client, fabric_workspace):
    """The fixed Warehouse for the run, found or created, and never deleted.

    Named ``disposable`` for history: its contents are, its existence is not.
    Creating a Warehouse is quick, but waiting for its SQL endpoint to become
    connectable is not bounded — the loop below tolerates ten minutes — and that
    wait is pure variance in every measurement the suite takes. A Warehouse that
    already exists has already paid it.

    The readiness loops stay, because a *first* run on a new tenant creates the
    Warehouse and must still wait. They simply cost nothing on the runs after.
    """

    from weaver.targets import WarehouseTarget
    from weaver.fabric import (
        FabricResolver,
        WAREHOUSE,
        create_warehouse,
        desktop_sql_executor,
        find_item,
    )

    started = time.monotonic()
    timings: dict[str, float] = {}
    executor = None
    name = _fixed_name("warehouse")
    try:
        stage = time.monotonic()
        try:
            item = find_item(
                fabric_workspace_item, name, item_type=WAREHOUSE, client=fabric_client
            )
            timings["item creation"] = 0.0
            print(f"Warehouse {name}: reused")
        except Exception:
            item = create_warehouse(fabric_workspace_item, name, client=fabric_client)
            timings["item creation"] = time.monotonic() - stage
            print(f"Warehouse {name} item creation: {timings['item creation']:.2f}s")

        target = WarehouseTarget.parse(name)
        deadline = time.monotonic() + WAREHOUSE_READY_TIMEOUT
        last_error: Exception | None = None
        endpoint = None

        stage = time.monotonic()
        while time.monotonic() < deadline:
            try:
                resolver = FabricResolver(fabric_workspace, client=fabric_client)
                endpoint = resolver.sql_endpoint(target)
                break
            except Exception as exc:  # provisioning returns several transient shapes
                last_error = exc
                time.sleep(WAREHOUSE_POLL_INTERVAL)
        if endpoint is None:
            raise RuntimeError(
                f"Warehouse {name!r} ({item.id}) exposed no SQL endpoint within "
                f"{int(WAREHOUSE_READY_TIMEOUT)}s; last error: {last_error}"
            )
        timings["endpoint readiness"] = time.monotonic() - stage
        print(
            f"Warehouse {name} endpoint readiness: "
            f"{timings['endpoint readiness']:.2f}s"
        )

        stage = time.monotonic()
        while time.monotonic() < deadline:
            candidate = None
            try:
                candidate = desktop_sql_executor(
                    target,
                    fabric_workspace,
                    resolver=FabricResolver(fabric_workspace, client=fabric_client),
                )
                connection_started = time.monotonic()
                with candidate.pool.lease():
                    pass
                timings["first SQL connection"] = (
                    time.monotonic() - connection_started
                )
                query_started = time.monotonic()
                assert candidate.query("select 1 as ready")[0]["ready"] == 1
                timings["first select 1"] = time.monotonic() - query_started
                executor = candidate
                break
            except Exception as exc:
                last_error = exc
                if candidate is not None:
                    candidate.close()
                time.sleep(WAREHOUSE_POLL_INTERVAL)
        if executor is None:
            raise RuntimeError(
                f"Warehouse {name!r} ({item.id}) was not SQL-queryable within "
                f"{int(WAREHOUSE_READY_TIMEOUT)}s; last error: {last_error}"
            )
        timings["SQL readiness"] = time.monotonic() - stage
        print(
            f"Warehouse {name} first SQL connection: "
            f"{timings['first SQL connection']:.2f}s; "
            f"first select 1: {timings['first select 1']:.2f}s"
        )

        yield DisposableWarehouse(
            item=item,
            workspace=fabric_workspace,
            target=target,
            endpoint=endpoint,
            executor=executor,
            timings=timings,
            started=started,
        )
    finally:
        if executor is not None:
            executor.close()
        print(
            f"Warehouse {name} kept; total fixture lifetime: "
            f"{time.monotonic() - started:.2f}s"
        )


@pytest.fixture(scope="session")
def fabric_empty_lakehouse(fabric_workspace, fabric_client, livy_session):
    """Empty one fixed Lakehouse, leaving it as though nothing had been built.

    Reusing items makes residue possible in a way disposable ones never allowed,
    and residue is not inert. A producer whose table already matches leaves
    incremental selection with nothing to do — correctly — so a test asserting
    *build order* then finds no build action in the plan at all. That is not a
    hypothetical: it is exactly how the Warehouse alias tests failed when two
    estates shared a producer.

    So ask for this wherever freshness is the premise, and say so in the test.
    """

    from weaver.fabric import FabricResolver, OneLakeDfsClient

    resolver = FabricResolver(fabric_workspace, client=fabric_client)
    store = OneLakeDfsClient()

    def empty(name: str) -> None:
        target = ItemRef(name)
        _empty_the_target(
            livy_session,
            store,
            target,
            resolver.spark_destination(target),
            fabric_workspace,
        )

    return empty


@pytest.fixture(scope="module")
def clean_disposable_warehouse(disposable_warehouse):
    from weaver.physical_wipe import wipe_sql_target

    wipe_sql_target(
        disposable_warehouse.target,
        disposable_warehouse.workspace,
        sql=disposable_warehouse.executor,
    )

    yield disposable_warehouse


@dataclass(frozen=True)
class WarehousePrimitiveEstate:
    """One real Warehouse build shared by its action, shape and inventory claims."""

    warehouse: Any
    repository: Any
    run: Callable[..., list[Any]]
    target: Any

    def columns(self, schema: str, name: str) -> dict[str, Any]:
        rows = self.warehouse.executor.query(
            f"""
            select columns.name as column_name,
                   types.name as type_name,
                   columns.is_nullable as is_nullable,
                   columns.is_identity as is_identity
            from sys.columns as columns
            join sys.types as types on types.user_type_id = columns.user_type_id
            where columns.object_id = object_id(N'{schema}.{name}')
            """
        )
        return {str(row["column_name"]).casefold(): row for row in rows}


@pytest.fixture(scope="session")
def warehouse_primitive_estate(disposable_warehouse, tmp_path_factory):
    """Build the Warehouse primitive estate once for three claim modules.

    These tests used to share a module-scoped estate in one mixed-claim module.
    The naming architecture requires separate modules, but a filing decision
    must not triple the real Fabric build. Session scope preserves the one-build
    cost; the modules are named together and leave the declared estate intact.
    """

    from factories import (
        bound_target,
        item_id,
        registered_document,
        single_document_repository,
        target_inventory,
        warehouse_table,
        warehouse_view,
    )

    from weaver.targets import ItemRef
    from weaver.physical_wipe import wipe_sql_target
    from weaver.build_bundle import execute_install_action, plan_item_build
    from weaver.build_bundle.executors.base import (
        InstallationContext,
        ResolvedTarget,
    )

    wipe_sql_target(
        disposable_warehouse.target,
        disposable_warehouse.workspace,
        sql=disposable_warehouse.executor,
    )
    target = ResolvedTarget(
        bound=bound_target(
            id="target-1",
            kind="warehouse",
            item_id=disposable_warehouse.item.name,
            logical_item_name="Reporting",
            logical_item_type="Warehouse",
        ),
        lakehouse=ItemRef(disposable_warehouse.item.name),
        location=None,
        destination=None,
    )

    def run(repository, *, inventory=None, rebuild=False, build=True):
        identity = item_id("Warehouse/Reporting")
        selected = {
            key for key in repository.source_documents if key.item == identity
        }
        planned = plan_item_build(
            repository,
            item=identity,
            target=target.bound,
            inventory=(
                inventory
                if inventory is not None
                else target_inventory(
                    target_id="target-1",
                    kind="warehouse",
                    target_name=disposable_warehouse.item.name,
                )
            ),
            target_by_item={identity: target.bound},
            selected_documents=selected,
            selected_aliases=set(),
            selected_for_drop=set(selected) if rebuild else set(),
            selected_for_build=selected if build else set(),
            registered=(
                {key: registered_document(key) for key in selected}
                if rebuild
                else {}
            ),
        )
        context = InstallationContext(
                resolver=None,
            store=None,
            target=target,
            sql=disposable_warehouse.executor,
            targets={target.bound.id: target},
        )
        results = []
        for stage in planned.stages:
            for batch in stage.batches:
                for action in batch.actions:
                    results.append(
                        execute_install_action(
                            action,
                            (
                                stage.payloads.get(action.payload)
                                if action.payload
                                else None
                            ),
                            context=context,
                        )
                    )
        return results

    root = tmp_path_factory.mktemp("warehouse-primitive-estate")
    repository = single_document_repository(
        root / "repo",
        item="Warehouse/Reporting",
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
                primary_key="CustomerId",
                identity="CustomerKey",
            ),
        },
    )
    results = run(repository)
    failures = {r.action_id: r.error_message for r in results if r.status == "failed"}
    assert not failures, failures

    return WarehousePrimitiveEstate(
        warehouse=disposable_warehouse,
        repository=repository,
        run=run,
        target=target,
    )

# --- one populated lifecycle, on either workspace --------------------------------


@pytest.fixture
def populated_fabric_lakehouse(
    fabric_workspace_item,
    fabric_client,
    fabric_workspace,
    livy_session,
    lakehouse_sql_statements,
    populate_folder_files,
):
    """A disposable Fabric target populated through Environment-backed Livy."""

    from weaver.fabric import (
        FabricResolver,
        OneLakeDfsClient,
        create_lakehouse,
        delete_item,
    )

    item = None
    try:
        item = create_lakehouse(
            fabric_workspace_item,
            _disposable_name("target"),
            client=fabric_client,
        )
        target = ItemRef(item.name)
        resolver = FabricResolver(fabric_workspace, client=fabric_client)
        store = OneLakeDfsClient()

        populate_folder_files(store, resolver, target)
        tables_root = f"{resolver.spark_root(target)}/Tables"
        statements = [
            statement
            for script in ("build.spark.sql", "load.spark.sql")
            for statement in lakehouse_sql_statements(script, tables_root)
        ]
        body = "\n".join(f"spark.sql({statement!r})" for statement in statements)
        # Raw Spark, no Weaver import: a session is needed to make a Delta table,
        # the installed package is not.
        result = livy_session.run(f"{body}\nemit(True)\n", label="seed")
        assert result.payload is True

        def wipe() -> tuple[str, ...]:
            """Weaver's own wipe, run from here.

            `wipe_delta_target` never needed a session: a Delta table is a
            directory, there is no catalogue to enumerate, and shortcuts go over
            REST. It ran in one only because that is where the installed package
            lived — which put a wheel publish in front of a claim about directory
            removal.
            """

            from weaver.targets import DeltaTarget
            from weaver.physical_wipe import wipe_delta_target

            report = wipe_delta_target(
                DeltaTarget(lakehouse=target), fabric_workspace, store=store
            )
            return tuple(report.removed)

        yield PopulatedLakehouse(
            workspace=fabric_workspace,
            target=target,
            resolver=resolver,
            store=store,
            wipe=wipe,
        )
    finally:
        if item is not None:
            try:
                delete_item(item, client=fabric_client)
            except Exception as exc:
                print(f"warning: could not delete {item}: {exc}")


@pytest.fixture(scope="module")
def weaver_repo_fixture(request):
    """Which Weaver document repository a build env installs — the default, or a test's choice.

    Module-scoped so an estate is provisioned once per test module. Parametrise
    indirectly (with a fixture from ``build_envs``) to point an environment at
    another fixture without changing the environment fixtures::

        @pytest.mark.parametrize("weaver_repo_fixture", [SQL_TABLE_FIXTURE], indirect=True)
    """

    return getattr(request, "param", LAKEHOUSE_JOURNEY_FIXTURE)


@contextmanager
def _fabric_build_context(
    fabric_workspace_item,
    fabric_client,
    workspace,
    target_lh,
    session,
    weaver_repo_fixture,
    warehouse=None,
):
    """A build environment run entirely inside Fabric over Livy.

    Weaver is Fabric-first: both **generation and installation** run in the
    session, against the native Spark catalogue, so planning sees catalogue views
    and nothing is contorted to fit a desktop planner. The desktop only places
    the explicit repository source and reads back results. Both Lakehouses are disposable and are
    deleted when the run ends.

    **The session attaches to the Weaver Lakehouse**, which is the production
    model: the control plane is the fixed attachment and destinations are the
    variable data plane. It used to attach to the *target*, so that a two-part
    ``Schema.Object`` happened to land in the right place — which made the whole
    suite blind to the thing it most needed to check. Under the real model an
    unqualified name lands in the control plane, and every statement therefore has
    to name its Lakehouse.

    **Both Lakehouses are schema-enabled.** The target so a managed table appears
    under ``Tables/<schema>/<table>``; the Weaver Lakehouse because the catalogue
    lives in a schema called ``_`` and a Lakehouse without schemas cannot hold one.

    **The Weaver Lakehouse and the Livy session are the run's, not this context's.**
    A destination is disposable and a control plane is not — that is the
    architecture, and modelling it costs less as well. A Livy session takes one to
    two minutes to reach ``idle``, which was about seventy per cent of this
    module's wall clock when every test started its own; a capacity that permits
    one session at a time also has to release the previous one before the next can
    start, and at the tail of a long run it did not, so two estates were refused
    with ``did not reach 'idle' within 600s``.

    The **target** Lakehouse is the run's too, and is *emptied* on the way in
    rather than re-created. Tests that want a target nobody has touched — prune,
    the failure paths — get one that way, and the workspace's artifacts stop
    churning underneath a long-lived session. It is also what the local context
    has always done.
    """

    from weaver.build_bundle import BuildBundle, BuildPlan
    from weaver.fabric import FabricResolver, OneLakeDfsClient

    # Nothing is torn down here. The Weaver Lakehouse, the target and the
    # session all outlive this context; the next one empties the target again
    # on its way in.
    resolver = FabricResolver(workspace, client=fabric_client)
    store = OneLakeDfsClient()
    weaver = ItemRef(workspace.weaver_lakehouse)
    target = ItemRef(target_lh.name)
    warehouse_name = warehouse.item.name if warehouse is not None else None
    repository_relative = ("test_repositories", weaver_repo_fixture.name)
    repository_root = resolver.files_root(weaver).join(*repository_relative)

    destination = resolver.spark_destination(target)
    weaver_destination = resolver.spark_destination(weaver)
    _empty_the_target(
        session,
        store,
        target,
        destination,
        workspace,
        warehouse=warehouse_name,
        weaver_destination=weaver_destination,
    )

    def install_repo() -> None:
        destination = repository_root
        if store.exists(destination):
            store.delete(destination, recursive=True)
        _upload_tree(store, weaver_repo_fixture.path, destination)

    def remove_repo() -> None:
        store.delete(repository_root, recursive=True)

    def _workspace_literal() -> str:
        return (
            f"FabricWorkspace(workspace={workspace.workspace!r}, "
            f"weaver_lakehouse={workspace.weaver_lakehouse!r}, "
            f"environment={workspace.environment!r})"
        )

    def generate(bundle_name: str = "buildtest"):
        # Generation runs IN the session, against the native Spark catalogue.
        # The item type chooses its binding, so one context serves a Lakehouse
        # estate or a mixed one without a test naming a target kind — the same
        # rule `_bindings_for` applies locally.
        binds = ", ".join(
            (
                f"ItemBinding(WeaverItemId.parse({item!r}), "
                f"WarehouseBinding(warehouse=ItemRef({warehouse_name!r}), "
                f"workspace_name=workspace.workspace))"
                if item.startswith("Warehouse/")
                else f"ItemBinding(WeaverItemId.parse({item!r}), "
                f"LakehouseBinding(lakehouse=ItemRef({target.name!r}), "
                f"workspace_name=workspace.workspace))"
            )
            for item in weaver_repo_fixture.items
        )
        body = (
            "from weaver.targets import ItemRef\n"
            "from weaver.workspaces import FabricWorkspace\n"
            "from weaver.declaration.model import WeaverItemId\n"
            "from weaver.resolution import resolver_for, store_for\n"
            "from weaver.declaration import parse_item_repository\n"
            "from weaver.build_bundle import ItemBinding, ItemBindings, "
            "LakehouseBinding, WarehouseBinding, effective_item_bindings\n"
            "from weaver.session import NotebookSession\n"
            "from weaver.build_bundle.workflow import (read_target_inventories, "
            "read_reconciled_catalogue)\n"
            "from weaver.build_bundle.planner import generate_item_build_bundle\n"
            f"workspace = {_workspace_literal()}\n"
            "store = store_for(workspace)\n"
            "resolver = resolver_for(workspace)\n"
            f"repository_root = resolver.files_root(ItemRef({weaver.name!r})).join"
            f"(*{repository_relative!r})\n"
            "repository = parse_item_repository(repository_root, store=store)\n"
            f"control = LakehouseBinding(lakehouse=ItemRef({weaver.name!r}), "
            f"workspace_name=workspace.workspace)\n"
            f"selected = ItemBindings(({binds},))\n"
            "bindings = effective_item_bindings("
            "selected, weaver_lakehouse=workspace.weaver_lakehouse)\n"
            "session = NotebookSession(workspace=workspace, spark=spark)\n"
            "inventories = read_target_inventories(bindings, session=session)\n"
            "reconciled = read_reconciled_catalogue("
            "bindings, inventories=inventories, session=session, "
            "repository=repository)\n"
            "bundle = generate_item_build_bundle(\n"
            "    repository,\n"
            "    bindings=bindings,\n"
            f"    output=resolver.build_bundle({bundle_name!r}),\n"
            "    store=store, control_lakehouse=control,\n"
            "    target_inventories=inventories, catalogue=reconciled.catalogue,\n"
            "    stale_claims=reconciled.stale_claims)\n"
            "emit({'name': bundle.location.name, 'bundle_id': bundle.bundle_id, "
            "'plan': bundle.plan.to_mapping()})\n"
        )
        payload = _timed_session_run(
            session, "Lakehouse bundle generation", body
        ).payload
        plan = BuildPlan.from_mapping(payload["plan"])
        # A desktop-addressed (https) handle to the same physical bundle, so the
        # test can read it and the install can re-resolve it by name in-session.
        return BuildBundle(location=resolver.build_bundle(payload["name"]), plan=plan)

    def install(bundle) -> InstallOutcome:
        # Generation wrote the bundle through the session's abfss path. The
        # desktop handle names the same physical place over https, so install
        # re-resolves the name to the session-native path.
        bundle_name = bundle.location.name
        body = (
            "from weaver.workspaces import FabricWorkspace\n"
            "from weaver.resolution import resolver_for, store_for\n"
            "from weaver.build_bundle import Installer, load_bundle\n"
            "from weaver.session import NotebookSession\n"
            f"workspace = {_workspace_literal()}\n"
            "store = store_for(workspace)\n"
            "resolver = resolver_for(workspace)\n"
            # The workspace is what lets a Warehouse batch acquire SQL on the
            # session's own identity. A Lakehouse-only bundle never asks.
            "installer = Installer(NotebookSession(workspace=workspace, spark=spark))\n"
            f"bundle = load_bundle(resolver.build_bundle({bundle_name!r}), store=store)\n"
            "report = installer.install(bundle)\n"
            "emit({'status': report.status, 'bundle_id': report.bundle_id, "
            "'sequences': [{'number': s.number, 'status': s.status} for s in report.sequences], "
            "'actions': [{'id': a.action_id, 'status': a.status, "
            "'error': (a.error_type + ': ' + str(a.error_message)) if a.error_type else None} "
            "for a in report.action_results()]})\n"
        )
        payload = _timed_session_run(
            session, "Lakehouse bundle installation", body
        ).payload
        outcome = InstallOutcome(
            status=payload["status"],
            bundle_id=payload["bundle_id"],
            sequence_status={s["number"]: s["status"] for s in payload["sequences"]},
            action_status={a["id"]: a["status"] for a in payload["actions"]},
            action_order=tuple(a["id"] for a in payload["actions"]),
            action_error={a["id"]: a["error"] for a in payload["actions"] if a["error"]},
        )
        if outcome.status != "succeeded":
            print("INSTALL ACTION ERRORS:", outcome.action_error)
        return outcome

    def query(sql: str) -> list:
        body = f"emit([row.asDict() for row in spark.sql({sql!r}).collect()])\n"
        return session.run(body, label="query").payload

    def columns(table: str) -> list:
        body = (
            "emit([{'name': f.name, 'type': f.dataType.simpleString(), "
            f"'nullable': f.nullable}} for f in spark.table({table!r}).schema])\n"
        )
        return session.run(body, label="query").payload

    def schema_exists(qualified: str) -> bool:
        body = f"emit(bool(spark.catalog.databaseExists({qualified!r})))\n"
        return session.run(body, label="query").payload

    def run_python(body: str, *, label: str = "shared body"):
        """Run a shared body *in Fabric*, with the session's own resolver bound.

        The preamble is the whole transport difference: ``resolver_for`` returns
        the session-native resolver here and the local one on a laptop, so the
        body that follows is byte-identical either side.
        """

        preamble = (
            "from weaver.workspaces import FabricWorkspace\n"
            "from weaver.targets import ItemRef\n"
            "from weaver.resolution import resolver_for, store_for\n"
            f"workspace = {_workspace_literal()}\n"
            "resolver = resolver_for(workspace)\n"
            "store = store_for(workspace)\n"
            f"target = ItemRef({target.name!r})\n"
            # Bound only where the estate has one, so a body that names it is a
            # body that could not have run against a Lakehouse-only estate.
            + (
                f"warehouse = ItemRef({warehouse_name!r})\n"
                if warehouse_name is not None
                else ""
            )
        )
        return _timed_session_run(session, label, preamble + body).payload

    def seed_orphans() -> None:
        # Seeded in the *destination*, by its four-part name — the session is
        # attached to the Weaver Lakehouse, so an unqualified create here would
        # put the orphans in the control plane and prune would rightly not find
        # them.
        statements = [
            f"CREATE SCHEMA IF NOT EXISTS {destination.qualified_schema('DWG')}",
            f"CREATE SCHEMA IF NOT EXISTS {destination.qualified_schema('Legacy')}",
            f"CREATE TABLE IF NOT EXISTS {destination.qualify('DWG', 'OldTable')} "
            "(x int) USING delta",
            f"CREATE OR REPLACE VIEW {destination.qualify('DWG', 'OldView')} "
            "AS SELECT 1 AS x",
            f"CREATE TABLE IF NOT EXISTS {destination.qualify('Legacy', 'OldThing')} "
            "(x int) USING delta",
        ]
        body = "".join(f"spark.sql({s!r})\n" for s in statements) + "emit(True)\n"
        session.run(body, label="seed")
        files_root = resolver.files_root(target)
        store.write(files_root.join("Raw", "OldFolder", "stale.csv"), b"old\n")
        store.write(files_root.join("Legacy", "Stuff", "f.txt"), b"x\n")

    yield BuildEnv(
        label="fabric", workspace=workspace, weaver=weaver, target=target,
        resolver=resolver, store=store, repository_root=repository_root,
        generate_spark=True,  # in-session catalogue
        install_repo=install_repo, remove_repo=remove_repo, generate=generate,
        install=install, run_query=query, run_columns=columns,
        seed_orphans=seed_orphans, run_schema_exists=schema_exists,
        run_python=run_python,
        destination=destination, weaver_destination=weaver_destination,
    )


#: Every schema a Lakehouse build fixture registers in the target, and every Files
#: area it writes. Emptied between contexts, which is what a fresh target used to
#: be for. The local context drops the same set — see `_LOCAL_SCHEMAS`.
_FABRIC_TARGET_SCHEMAS = ("DWG", "Raw", "Legacy", "Sales", "Reporting")


def _empty_the_target(
    session,
    store,
    target,
    destination,
    workspace,
    warehouse=None,
    weaver_destination=None,
) -> None:
    """Leave the target Lakehouse *and the control plane* as though nothing had run.

    **Through Weaver's own wipe.** This used to sweep the two areas with
    ``store.delete`` and it is exactly the mistake ``AGENTS.md`` records against
    the first Fabric suite — test code reproducing what the function would have
    done, and looking like it was testing ``wipe``. It cost twice over. Once
    because a hand-rolled sweep is a *list* of what to remove, so it can omit
    something: skipping ``Files/`` on the Weaver Lakehouse left the declared
    ``_.Log`` folder behind while its Registry row went, and the next build
    planned to create a folder already there. And once latently, because it
    recursively deleted directories without removing shortcuts first — which
    ``wipe_folder_target`` is careful never to do, since a recursive delete
    through a shortcut reaches the *producer's* data.

    ``wipe_lakehouse`` has neither problem: it clears both areas completely,
    keeps only what nothing recreates, and takes shortcuts out through the
    workspace before any storage is swept.

    **The control plane is wiped too, and that is the point.** Weaver's own
    record is state like any other, and it accumulates faster than anything else
    here: the Weaver Lakehouse is shared and permanent while the *logical item
    names* bound to one physical target change from module to module, so its
    Registry ends up holding several items' claims on one physical object. No
    build repairs that, and correctly so — reconciliation is scoped to the items
    a build binds (``catalogue_items_for_build`` returns those plus alias
    sources), because an item you are not building has claims you have no
    business deleting. Right in production; in a suite reusing one Lakehouse
    under many names it makes the residue permanent.

    What storage cannot reach is the Spark metastore: a *view* exists only
    there, and the catalogue's schema is assumed by a build rather than created
    by one. So the session drops the destination's schemas, and drops and
    restores ``_``.
    """

    statements = [
        f"DROP SCHEMA IF EXISTS {destination.qualified_schema(schema)} CASCADE"
        for schema in _FABRIC_TARGET_SCHEMAS
    ]
    if weaver_destination is not None:
        # Dropped and put back. A build creates the catalogue's tables and
        # *assumes* its schema, so leaving `_` gone is not clearing the control
        # plane but damaging it — the same judgement `physical_wipe`
        # already makes about `dbo`.
        catalogue_schema = weaver_destination.qualified_schema("_")
        statements.append(f"DROP SCHEMA IF EXISTS {catalogue_schema} CASCADE")
        statements.append(f"CREATE SCHEMA IF NOT EXISTS {catalogue_schema}")
    session.run(
        "".join(f"spark.sql({s!r})\n" for s in statements) + "emit(True)\n",
        label="empty target",
    )

    from weaver.physical_wipe import wipe_lakehouse

    wiped = [target]
    if weaver_destination is not None:
        wiped.append(ItemRef(workspace.weaver_lakehouse))
    for item in wiped:
        for report in wipe_lakehouse(item, workspace, store=store):
            print(f"Fabric wipe: {report}")


@pytest.fixture
def fabric_build_env(
    fabric_workspace_item, fabric_client, fabric_workspace, fabric_target_lakehouse,
    livy_session, weaver_repo_fixture,
):
    """One Fabric build environment per test, over the run's Weaver Lakehouse,
    target Lakehouse and Livy session. The target is emptied on the way in, which
    is what a freshly created one used to provide."""

    with _fabric_build_context(
        fabric_workspace_item, fabric_client, fabric_workspace, fabric_target_lakehouse,
        livy_session, weaver_repo_fixture,
    ) as env:
        yield env


def _warehouse_build_env(
    fabric_workspace, weaver_lakehouse, warehouse, weaver_repo_fixture, session
) -> "BuildEnv":
    """A Warehouse BuildEnv that runs **inside Fabric**, like the Lakehouse one.

    Weaver is a Fabric tool: it is installed into the Environment and does the
    work there. So both phases run in the Livy session — generation reads the
    target Warehouse's system schema through Weaver's *own* Fabric-native mssql
    connector (``fabric_sql_executor``, the session identity) to compile the prune
    into the bundle, and installation runs the frozen T-SQL through that same
    connector. The desktop uploads only the explicit Weaver repository source and
    reads results back for assertions; it never plans and never compiles a bundle locally.
    """

    from weaver.targets import ItemRef as _ItemRef
    from weaver.build_bundle import BuildBundle, BuildPlan
    from weaver.fabric import FabricResolver, OneLakeDfsClient

    resolver = FabricResolver(fabric_workspace, client=None)
    store = OneLakeDfsClient()
    weaver = _ItemRef(weaver_lakehouse.name)
    warehouse_ref = _ItemRef(warehouse.item.name)
    repository_relative = ("test_repositories", weaver_repo_fixture.name)
    repository_root = resolver.files_root(weaver).join(*repository_relative)
    # Desktop SQL is test infrastructure only: it stages fixtures and inspects the
    # catalogue for assertions. Weaver itself never uses it here.
    sql = warehouse.executor

    def _workspace_literal() -> str:
        return (
            f"FabricWorkspace(workspace={fabric_workspace.workspace!r}, "
            f"weaver_lakehouse={fabric_workspace.weaver_lakehouse!r}, "
            f"environment={fabric_workspace.environment!r})"
        )

    def install_repo() -> None:
        destination = repository_root
        if store.exists(destination):
            store.delete(destination, recursive=True)
        _upload_tree(store, weaver_repo_fixture.path, destination)

    def remove_repo() -> None:
        store.delete(repository_root, recursive=True)

    def generate(bundle_name: str = "whtest"):
        # Generation runs IN Fabric and reads the Warehouse catalogue there
        # through its own Fabric-native SQL — no sql= injection.
        binds = ", ".join(
            f"ItemBinding(WeaverItemId.parse({item!r}), "
            f"WarehouseBinding(warehouse=ItemRef({warehouse_ref.name!r}), "
            f"workspace_name=workspace.workspace))"
            for item in weaver_repo_fixture.items
        )
        body = (
            "from weaver.targets import ItemRef\n"
            "from weaver.workspaces import FabricWorkspace\n"
            "from weaver.declaration.model import WeaverItemId\n"
            "from weaver.resolution import resolver_for, store_for\n"
            "from weaver.declaration import parse_item_repository\n"
            "from weaver.build_bundle import ItemBinding, ItemBindings, "
            "WarehouseBinding, LakehouseBinding, effective_item_bindings\n"
            "from weaver.session import NotebookSession\n"
            "from weaver.build_bundle.workflow import (read_target_inventories, "
            "read_reconciled_catalogue)\n"
            "from weaver.build_bundle.planner import generate_item_build_bundle\n"
            f"workspace = {_workspace_literal()}\n"
            "store = store_for(workspace)\n"
            "resolver = resolver_for(workspace)\n"
            f"repository_root = resolver.files_root(ItemRef({weaver.name!r})).join"
            f"(*{repository_relative!r})\n"
            "repository = parse_item_repository(repository_root, store=store)\n"
            f"selected = ItemBindings(({binds},))\n"
            "bindings = effective_item_bindings("
            "selected, weaver_lakehouse=workspace.weaver_lakehouse)\n"
            "control = LakehouseBinding(ItemRef(workspace.weaver_lakehouse), "
            "workspace_name=workspace.workspace)\n"
            "session = NotebookSession(workspace=workspace, spark=spark)\n"
            "inventories = read_target_inventories(bindings, session=session)\n"
            "reconciled = read_reconciled_catalogue("
            "bindings, inventories=inventories, session=session, "
            "repository=repository)\n"
            "bundle = generate_item_build_bundle(\n"
            "    repository,\n"
            "    bindings=bindings,\n"
            f"    output=resolver.build_bundle({bundle_name!r}),\n"
            "    store=store, control_lakehouse=control,\n"
            "    target_inventories=inventories, catalogue=reconciled.catalogue,\n"
            "    stale_claims=reconciled.stale_claims)\n"
            "emit({'name': bundle.location.name, 'bundle_id': bundle.bundle_id, "
            "'plan': bundle.plan.to_mapping()})\n"
        )
        payload = _timed_session_run(
            session, "Warehouse bundle generation", body
        ).payload
        plan = BuildPlan.from_mapping(payload["plan"])
        return BuildBundle(location=resolver.build_bundle(payload["name"]), plan=plan)

    def install(bundle) -> InstallOutcome:
        # Installation runs IN Fabric too; the Warehouse SQL comes from the
        # session identity, so no executor is injected.
        bundle_name = bundle.location.name
        body = (
            "from weaver.workspaces import FabricWorkspace\n"
            "from weaver.resolution import resolver_for, store_for\n"
            "from weaver.build_bundle import Installer, load_bundle\n"
            "from weaver.session import NotebookSession\n"
            f"workspace = {_workspace_literal()}\n"
            "store = store_for(workspace)\n"
            "resolver = resolver_for(workspace)\n"
            "installer = Installer(NotebookSession(workspace=workspace, spark=spark))\n"
            f"bundle = load_bundle(resolver.build_bundle({bundle_name!r}), store=store)\n"
            "report = installer.install(bundle)\n"
            "emit({'status': report.status, 'bundle_id': report.bundle_id, "
            "'sequences': [{'number': s.number, 'status': s.status} for s in report.sequences], "
            "'actions': [{'id': a.action_id, 'status': a.status, "
            "'error': (a.error_type + ': ' + str(a.error_message)) if a.error_type else None} "
            "for a in report.action_results()]})\n"
        )
        payload = _timed_session_run(
            session, "Warehouse bundle installation", body
        ).payload
        outcome = InstallOutcome(
            status=payload["status"],
            bundle_id=payload["bundle_id"],
            sequence_status={s["number"]: s["status"] for s in payload["sequences"]},
            action_status={a["id"]: a["status"] for a in payload["actions"]},
            action_order=tuple(a["id"] for a in payload["actions"]),
            action_error={a["id"]: a["error"] for a in payload["actions"] if a["error"]},
        )
        if outcome.status != "succeeded":
            print("WAREHOUSE INSTALL ACTION ERRORS:", outcome.action_error)
        return outcome

    def query(statement: str) -> list:
        return list(sql.query(statement))

    def columns(table: str) -> list:
        # Fabric Warehouses use a case-sensitive collation, so INFORMATION_SCHEMA
        # and its columns must be referenced in their exact (upper) case.
        schema, name = table.split(".", 1)
        rows = sql.query(
            "select COLUMN_NAME, DATA_TYPE, IS_NULLABLE from INFORMATION_SCHEMA.COLUMNS "
            f"where TABLE_SCHEMA = N'{schema}' and TABLE_NAME = N'{name}'"
        )
        return [
            {
                "name": row["COLUMN_NAME"],
                "type": row["DATA_TYPE"],
                "nullable": str(row["IS_NULLABLE"]).upper() == "YES",
            }
            for row in rows
        ]

    def seed_orphans() -> None:
        """Objects the bundle does not manage, for prune to reconcile away.

        An orphan table and view inside a managed schema, plus a whole orphan
        schema holding both — so the frozen drops must cover object-level and
        schema-level reconciliation, in a dependency-safe order.
        """

        # One statement per call: T-SQL requires CREATE VIEW to be the first
        # statement in its batch, so these cannot be bundled into one script.
        for statement in (
            "if not exists (select 1 from sys.schemas where name = N'Wh')"
            " exec('create schema [Wh]');",
            "if not exists (select 1 from sys.schemas where name = N'Legacy')"
            " exec('create schema [Legacy]');",
            "create table [Wh].[OldTable] ([x] int not null);",
            "create view [Wh].[OldView] as select 1 as [x];",
            "create table [Legacy].[Thing] ([x] int not null);",
            "create view [Legacy].[ThingView] as select [x] from [Legacy].[Thing];",
        ):
            sql.execute_script(statement)

    return BuildEnv(
        label="warehouse", workspace=fabric_workspace, weaver=weaver, target=warehouse_ref,
        resolver=resolver, store=store, repository_root=repository_root,
        generate_spark=None,
        install_repo=install_repo, remove_repo=remove_repo, generate=generate,
        install=install, run_query=query, run_columns=columns,
        seed_orphans=seed_orphans,
    )


@pytest.fixture(scope="module")
def warehouse_estate(
    fabric_workspace, fabric_weaver_lakehouse, clean_disposable_warehouse, weaver_repo_fixture, livy_session
):
    """The Warehouse estate, built **in Fabric** and installed once per module.

    One disposable Warehouse and one install for the whole module's checks. Prune
    is on: reconciliation is part of a normal build, and Weaver reads the target's
    system schema in-session through its own Fabric-native connector.
    """

    env = _warehouse_build_env(
        fabric_workspace,
        fabric_weaver_lakehouse,
        clean_disposable_warehouse,
        weaver_repo_fixture,
        livy_session,
    )
    yield _install_estate(env)




# --- the estates a Fabric test drives ----------------------------------------
#
# Fabric-only, deliberately. Their local halves live in `tests/spark`, built from
# the same `tests/support` machinery. A single parametrised fixture spanning both
# transports is what used to make `pytest -m spark` reach into this file, and
# with it a workspace, a credential and a session.


@pytest.fixture(scope="module")
def fabric_lakehouse_journey(request, weaver_repo_fixture):
    """One Fabric estate for a journey to drive.

    **The transitions are deliberately not run here.** A fixture that took every
    move up front and handed the result to a set of tests would leave each one
    inspecting the *final* estate, whatever transition it claimed to be about —
    and a later move could repair what an earlier one broke, so the earlier
    assertion would pass on evidence that no longer existed.
    """

    with _fabric_build_context(
        request.getfixturevalue("fabric_workspace_item"),
        request.getfixturevalue("fabric_client"),
        request.getfixturevalue("fabric_workspace"),
        request.getfixturevalue("fabric_target_lakehouse"),
        request.getfixturevalue("livy_session"),
        weaver_repo_fixture,
    ) as env:
        yield Journey(env, "lakehouse")


@pytest.fixture(scope="module")
def fabric_cross_item_journey(request, weaver_repo_fixture):
    """One Fabric estate spanning a Lakehouse and a Warehouse, for a journey.

    The same arrangement as :func:`fabric_mixed_estate` — one bundle installing
    both halves, so the ordering the build gives them is the ordering a real
    estate has — but handed over untouched, because a journey's transitions are
    its subject and a fixture that had already taken them would leave every
    assertion reading the final estate.
    """

    warehouse = request.getfixturevalue("clean_disposable_warehouse")
    with _fabric_build_context(
        request.getfixturevalue("fabric_workspace_item"),
        request.getfixturevalue("fabric_client"),
        request.getfixturevalue("fabric_workspace"),
        request.getfixturevalue("fabric_target_lakehouse"),
        request.getfixturevalue("livy_session"),
        weaver_repo_fixture,
        warehouse=warehouse,
    ) as env:
        env.warehouse = warehouse
        yield Journey(env, "cross-item")


@pytest.fixture(scope="module")
def fabric_lakehouse_estate(request, weaver_repo_fixture):
    """One Fabric Lakehouse estate, installed once per module."""

    with _fabric_build_context(
        request.getfixturevalue("fabric_workspace_item"),
        request.getfixturevalue("fabric_client"),
        request.getfixturevalue("fabric_workspace"),
        request.getfixturevalue("fabric_target_lakehouse"),
        request.getfixturevalue("livy_session"),
        weaver_repo_fixture,
    ) as env:
        yield _install_estate(env)


@pytest.fixture(scope="module")
def fabric_mixed_estate(request, weaver_repo_fixture):
    """One estate spanning a Lakehouse *and* a Warehouse, installed together.

    The one arrangement neither single-target context can express, and the one
    the physical load graph most needs: a Delta table published into a Warehouse
    through an alias is read across a SQL analytics endpoint, and that boundary
    is where the refresh barrier lives. Nothing with one physical side has such a
    boundary to cross.

    Both halves are installed by one bundle, in the session, so the ordering the
    build gives them is the ordering a real estate has.
    """

    warehouse = request.getfixturevalue("clean_disposable_warehouse")
    with _fabric_build_context(
        request.getfixturevalue("fabric_workspace_item"),
        request.getfixturevalue("fabric_client"),
        request.getfixturevalue("fabric_workspace"),
        request.getfixturevalue("fabric_target_lakehouse"),
        request.getfixturevalue("livy_session"),
        weaver_repo_fixture,
        warehouse=warehouse,
    ) as env:
        env.warehouse = warehouse
        yield _install_estate(env)
