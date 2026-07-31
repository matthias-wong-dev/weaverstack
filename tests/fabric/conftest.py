"""Fixtures for opt-in Fabric integration tests.

These touch a real workspace and a running capacity, so they are deselected by
default: `-m fabric` is what asks for them.

The estate is permanent and named by default — `PYTEST_WORKSPACE`, holding
`PYTEST_WEAVER` and the `PYTEST_LH_*` Lakehouses — so a run needs no environment
at all. Every name is still overridable (`WEAVER_FABRIC_WORKSPACE`,
`WEAVER_PYTEST_<ROLE>`) for a tenant that arranges its items differently.

Items are found rather than made, and emptied rather than re-created. The
lifecycle tests that do create and delete carry their own `provisioning` marker.
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
from livy_telemetry import LEDGER, OUTSIDE_A_TEST, CountedLivySession
from observation import Observation, observation_from, observe_body

from weaver import Workspace, ItemRef, Store
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

    Still used by the ``provisioning`` tests, whose subject *is* creating and
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


@dataclass(frozen=True)
class PopulatedLakehouse:
    """One populated target, with transport hidden from the shared test."""

    workspace: Workspace
    target: ItemRef
    resolver: Any
    store: Store
    wipe: Callable[[], tuple[str, ...]]


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

    from weaver import FabricWorkspace

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

    try:
        active_sessions = list_workspace_livy_sessions(
            fabric_workspace, client=fabric_client, active_only=True
        )
    except Exception as exc:
        print(f"warning: could not inspect Fabric Spark sessions: {exc}")
    else:
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
        session = LivySession.for_workspace(fabric_workspace)
    except CommandError as exc:
        pytest.skip(f"{exc}; run `weaver install` into the Environment first")
    started = time.monotonic()
    try:
        session.start()
    except LivyError as exc:
        pytest.skip(f"could not start a Livy session (Environment installed?): {exc}")
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

    from weaver import WarehouseTarget
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
            livy_session, store, resolver, target, resolver.spark_destination(target)
        )

    return empty


@pytest.fixture(scope="module")
def clean_disposable_warehouse(disposable_warehouse):
    from weaver import wipe_sql_target

    wipe_sql_target(
        disposable_warehouse.target,
        disposable_warehouse.workspace,
        sql=disposable_warehouse.executor,
    )

    yield disposable_warehouse

# --- one populated lifecycle, on either workspace --------------------------------


@pytest.fixture
def populated_local_lakehouse(populated_local_lakehouses):
    """Adapt the preserved local lifecycle to the shared fixture result."""

    from weaver import DeltaTarget, wipe_delta_target

    def wipe() -> tuple[str, ...]:
        report = wipe_delta_target(
            DeltaTarget(lakehouse=populated_local_lakehouses.target),
            populated_local_lakehouses.workspace,
        )
        return report.removed

    return PopulatedLakehouse(
        workspace=populated_local_lakehouses.workspace,
        target=populated_local_lakehouses.target,
        resolver=populated_local_lakehouses.resolver,
        store=populated_local_lakehouses.store,
        wipe=wipe,
    )


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
        result = livy_session.run(f"{body}\nemit(True)\n", label="seed")
        assert result.payload is True

        def wipe() -> tuple[str, ...]:
            body = (
                "from weaver import FabricWorkspace, DeltaTarget, wipe_delta_target\n"
                f"workspace = FabricWorkspace(workspace={fabric_workspace.workspace!r}, "
                f"weaver_lakehouse={fabric_workspace.weaver_lakehouse!r}, "
                f"environment={fabric_workspace.environment!r})\n"
                f"target = DeltaTarget.parse({target.name!r})\n"
                "report = wipe_delta_target(target, workspace)\n"
                "emit({'removed': list(report.removed)})\n"
            )
            result = livy_session.run(body, label="wipe")
            return tuple(result.payload["removed"])

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


@pytest.fixture
def populated_lakehouse(request):
    """Select a concrete populated lifecycle by indirect parameter."""

    return request.getfixturevalue(request.param)


# --- Fabric-first build environment -----------------------------------------
#
# The same shape as PopulatedLakehouse above: one dataclass hides whether a build
# runs in-process against the local emulator or inside Fabric over Livy, so one
# behavioural test covers both execution paths. Generation and installation both
# run in that environment. For Fabric, the desktop only stages the repository and
# reads results.

# The default a build env installs when a module does not name one. The journey
# fixture, because it is the one Lakehouse estate that carries every shape a
# build has to handle — and the only Fabric fixture declaring Lakehouse/Sales.
from build_envs import LAKEHOUSE_JOURNEY_FIXTURE


@pytest.fixture(scope="module")
def weaver_repo_fixture(request):
    """Which Weaver document repository a build env installs — the default, or a test's choice.

    Module-scoped so an estate is provisioned once per test module. Parametrise
    indirectly (with a fixture from ``build_envs``) to point an environment at
    another fixture without changing the environment fixtures::

        @pytest.mark.parametrize("weaver_repo_fixture", [SQL_TABLE_FIXTURE], indirect=True)
    """

    return getattr(request, "param", LAKEHOUSE_JOURNEY_FIXTURE)


@dataclass
class InstalledEstate:
    """One estate provisioned and installed once, for read-only assertions.

    A test that rebuilds — to exercise prune, say — calls ``env.generate()``
    again; the bindings come from the environment's fixture, so nothing has to
    be named twice.
    """

    env: "BuildEnv"
    bundle: Any


@dataclass
class Step:
    """One transition of a journey, and what it produced.

    Assertions read this rather than doing the work themselves, which is what
    lets a dozen checks share one build.
    """

    name: str
    bundle: Any = None
    outcome: "InstallOutcome | None" = None
    #: Set when the transition raised. Later steps then report *this* name, so a
    #: journey fails once and says where.
    error: BaseException | None = None
    #: The one evidence payload taken at this transition, set by the test that
    #: asserts it. Kept on the step so an assertion names the moment it is about:
    #: the estate itself has moved on by the time the next transition finishes.
    observation: "Observation | None" = None

    @property
    def actions(self) -> dict:
        """Planned action kind by action id, for order and presence assertions."""

        return {
            action.id: action.kind
            for _sequence, _batch, action in self.bundle.plan.actions()
        }

    @property
    def sequence_of(self) -> dict:
        """Which barrier each action landed in, for ordering assertions."""

        return {
            action.id: sequence.number
            for sequence, _batch, action in self.bundle.plan.actions()
        }

    def kinds(self) -> set:
        return set(self.actions.values())


class Journey:
    """One estate driven through an ordered series of transitions.

    The suite's cost is *estates*, not assertions: a module-scoped Fabric estate
    is one full generate-and-install, and six checks over it cost exactly what
    one does. The old shape therefore paid an install per module and could only
    ever ask "what did a first build do?" — which is the question the local suite
    already answers, and not the one incremental logic lives in.

    A journey inverts that. It installs once, then *moves* the estate — change a
    document, seed an orphan, break a payload, wipe — and each move costs one
    round trip while every assertion about it costs nothing. So it is both
    cheaper and able to ask the questions that matter: what did the *second*
    build do, and did it correctly do nothing?

    **A journey owns its state.** Its own repository root, its own logical item
    names, its own physical targets. Estates in one run otherwise collide through
    three shared things — the ``weaver_items`` root, where the last writer wins;
    the Registry, which is keyed by logical item so identically-named items in
    two fixtures are one row; and the fixed Lakehouses. Each of those has already
    produced a confusing failure.

    **A failed transition does not cascade.** The step records the exception and
    every later step is skipped with its name, so a broken journey reports one
    failure naming the move that broke rather than a screen of errors whose cause
    is the first of them.
    """

    def __init__(self, env: "BuildEnv", name: str) -> None:
        self.env = env
        self.name = name
        self.steps: dict[str, Step] = {}
        self._failed: str | None = None

    def run(self, name: str, *, before=None, between=None) -> Step:
        """Take one transition: optionally change something, then build.

        ``before`` mutates the repository or the target — it is the *move*, and
        the build that follows is what the assertions are about.

        ``between(env, bundle)`` runs after generation and before installation,
        for the two claims that need the world to change mid-transition. It may
        return a bundle to install *instead* of the generated one:

        - removing the source repository, to prove a bundle installs from itself
        - substituting a corrupted bundle, to prove a failing action stops its
          barrier

        A transition whose installation reports failure is recorded in full — the
        step keeps its bundle and outcome, so the test that expects a failure can
        inspect it — but the journey is marked failed, so anything after it is
        skipped rather than asserting against a part-built estate.
        """

        if self._failed is not None:
            step = Step(name=name, error=RuntimeError(f"upstream step {self._failed!r} failed"))
            self.steps[name] = step
            return step
        try:
            if before is not None:
                before(self.env)
            bundle = self.env.generate(f"{self.name}-{name}")
            if between is not None:
                bundle = between(self.env, bundle) or bundle
            outcome = self.env.install(bundle)
            step = Step(name=name, bundle=bundle, outcome=outcome)
            if outcome.status != "succeeded":
                self._failed = name
        except BaseException as exc:  # recorded, not raised: the journey continues
            self._failed = name
            step = Step(name=name, error=exc)
        self.steps[name] = step
        return step

    def __getitem__(self, name: str) -> Step:
        step = self.steps[name]
        if step.error is not None:
            raise AssertionError(f"journey step {name!r} failed: {step.error}")
        return step


@dataclass
class InstallOutcome:
    """An environment-neutral view of an installation report."""

    status: str
    bundle_id: str
    sequence_status: dict[int, str]
    action_status: dict[str, str]
    action_order: tuple[str, ...]
    action_error: dict[str, str] = None  # action_id -> "Type: message", failures only


@dataclass
class BuildEnv:
    """Everything a build test needs, with transport hidden behind callables.

    Assertions are written in the same logical names a payload uses —
    ``{{object:DWG.Customer}}`` — and resolved against a named destination before
    they run. That is not sugar. A test that asked for ``DWG.Customer`` would
    resolve it through the session's own catalogue, which is exactly the mistake
    the build no longer makes: it would read back from the Lakehouse the object
    was wrongly written to and pass. Naming the destination in the assertion is
    what makes the assertion able to see the thing it claims about.
    """

    label: str
    workspace: Any
    weaver: ItemRef
    target: ItemRef
    resolver: Any
    store: Store
    generate_spark: Any
    #: Install this env's declaration into the Weaver Lakehouse, replacing
    #: whatever was there. The fixture chooses the content and the bindings.
    install_repo: Callable[[], None]
    remove_repo: Callable[[], None]
    generate: Callable[..., Any]
    install: Callable[[Any], InstallOutcome]
    #: Raw SQL, run wherever this environment runs. Prefer ``query``.
    run_query: Callable[[str], list]
    #: ``[{"name", "type", "nullable"}]`` for a table — schema with nullability,
    #: which ``query``/DESCRIBE cannot give. Warehouse reads it from the catalogue.
    run_columns: Callable[[str], list]
    seed_orphans: Callable[[], None]
    #: Whether a fully-qualified schema exists. Asked rather than listed, because
    #: an absent schema is the answer a prune assertion wants and both workspaces raise
    #: for `SHOW TABLES` in one.
    run_schema_exists: Callable[[str], bool] = None
    #: Python source, run wherever this environment runs, returning whatever it
    #: ``emit``s. The namespace carries ``spark``, ``resolver`` and ``target``, so
    #: one body serves both transports — the only way to exercise code that must
    #: behave identically in a notebook and on a laptop.
    run_python: Callable[[str], Any] = None
    #: The destination Lakehouse being built, and the Weaver Lakehouse holding the
    #: catalogue. Two, always — even the simplest install writes to both.
    destination: Any = None
    weaver_destination: Any = None
    #: Every Lakehouse this fixture bound, by the item that owns it. Empty unless
    #: the fixture asked for more than one — a cross-item alias is the only thing
    #: that does, and it needs both ends addressable to prove the alias points
    #: across rather than at itself.
    destinations: Mapping[str, Any] = field(default_factory=dict)

    def at(self, destination=None):
        return destination or self.destination

    def _addressed(self, text: str, destination) -> str:
        """Resolve object tokens, where there is a Spark destination to resolve to.

        A Warehouse environment has none — it is reached over TDS and its names
        are ordinary T-SQL — so its statements pass through untouched.
        """

        from weaver.spark import expand

        place = self.at(destination)
        return text if place is None else expand(text, place)

    def query(self, sql: str, *, destination=None) -> list:
        """Run a query, resolving its object tokens against one destination.

        One round trip per call. Prefer :meth:`observe` for anything asking more
        than one question of the same estate at the same moment.
        """

        return self.run_query(self._addressed(sql, destination))

    def observe(
        self, queries=None, schemas=None, tables=None, *, label="observe"
    ) -> Observation:
        """Ask the estate everything at once and bring back one evidence payload.

        A Fabric round trip costs seconds, so the number of them — not the work
        inside them — sets what this suite costs. Six ``query`` calls describing
        one moment are six waits for the same answer; this submits their bodies
        together and returns one payload, which the test then asserts against
        locally.

        That is not only cheaper, it is more accurate. Separate calls interrogate
        a *mutable remote estate* at six different instants, so a claim about
        "the estate after prune" is really six claims about six moments. One
        payload is one observation of one moment, which is what the assertion
        says it is.

        ``queries``, ``schemas`` and ``tables`` are mappings of evidence name to,
        respectively, a statement, a schema name and a ``Schema.Object`` pair. A
        value may instead be a ``(text, destination)`` pair, so one observation
        can span the destination Lakehouse *and* the control plane — the pairing
        that proves a build wrote where it claimed and nowhere else, and which
        two calls could never make about the same instant.

        Ask ``tables`` rather than ``queries`` where *absent* is a legitimate
        answer: a SELECT against a missing table raises instead of reporting.

        Failures stay local: :class:`Observation` names the piece of evidence
        that disappointed, rather than a traceback from inside a Spark session.
        """

        addressed = {}
        for name, probe in (queries or {}).items():
            text, destination = self._probe(probe)
            addressed[name] = self._addressed(text, destination)

        wanted_schemas = {}
        for name, probe in (schemas or {}).items():
            schema, destination = self._probe(probe)
            wanted_schemas[name] = self.schema_name(schema, destination=destination)

        wanted_tables = {}
        for name, probe in (tables or {}).items():
            qualified, destination = self._probe(probe)
            schema, _, obj = qualified.partition(".")
            wanted_tables[name] = self.name(schema, obj, destination=destination)

        # A Warehouse environment has no Spark session to batch into: it is
        # reached over TDS, where a statement is a cheap local round trip and not
        # a Livy submission. Batching there would buy nothing and hide the shape
        # of what ran, so it stays a loop — and the test-facing API is the same
        # either way, which is what lets one journey run against both.
        if self.run_python is None:
            return Observation(
                rows={name: self.run_query(sql) for name, sql in addressed.items()},
                schemas={
                    name: self.run_schema_exists(sql)
                    for name, sql in wanted_schemas.items()
                },
            )
        return observation_from(
            self.run_python(
                observe_body(addressed, wanted_schemas, wanted_tables), label=label
            )
        )

    def _probe(self, value) -> tuple[str, Any]:
        """A probe is ``text`` against the default destination, or ``(text, dest)``."""

        if isinstance(value, tuple):
            return value
        return (value, None)

    def columns(self, table: str, *, destination=None) -> list:
        return self.run_columns(self._addressed(table, destination))

    def name(self, schema: str, obj: str, *, destination=None) -> str:
        return self.at(destination).qualify(schema, obj)

    def schema_name(self, schema: str, *, destination=None) -> str:
        return self.at(destination).qualified_schema(schema)

    def schema_exists(self, schema: str, *, destination=None) -> bool:
        return self.run_schema_exists(self.schema_name(schema, destination=destination))

    def write_repo_file(self, relative: str, content: str) -> None:
        """Change the installed declaration between two builds.

        Incremental behaviour can only be asserted across builds, and what
        changes between them is the repository — so a test needs to edit it in
        place rather than install a second fixture. Both transports write through
        the same store abstraction, so this needs no per-environment form.
        """

        self.store.write(
            self.resolver.weaver_items_root.join(*relative.split("/")),
            content.encode("utf-8"),
        )


def _outcome_from_report(report) -> InstallOutcome:
    return InstallOutcome(
        status=report.status,
        bundle_id=report.bundle_id,
        sequence_status={s.number: s.status for s in report.sequences},
        action_status={a.action_id: a.status for a in report.action_results()},
        action_order=tuple(a.action_id for a in report.action_results()),
        action_error={
            a.action_id: f"{a.error_type}: {a.error_message}"
            for a in report.action_results()
            if a.error_type
        },
    )


def _upload_tree(store, source: Path, destination) -> None:
    """Install a repository, *replacing* whatever was there under that name.

    Replacing, not merging. Two modules install different fixtures under the same
    repository name into one shared Weaver Lakehouse, and a plain file-by-file
    write left the previous fixture's objects behind — so a Warehouse estate
    inherited a Lakehouse-reading table from a repository it had never heard of,
    and failed on a three-part name naming a Lakehouse that does not exist here.
    Installing a repository has never meant "add to whatever is already called
    that".
    """

    try:
        store.delete(destination, recursive=True)
    except Exception:  # nothing there yet, which is the ordinary case
        pass
    for path in sorted(source.rglob("*")):
        if path.is_file():
            store.write(destination.join(*path.relative_to(source).parts), path.read_bytes())


def _bindings_for(
    weaver_repo_fixture, *, lakehouse=None, warehouse=None, lakehouses=None
):
    """Bind the fixture's declared items to whichever targets this env has.

    The item type chooses the binding, so one environment serves a Lakehouse
    fixture, a Warehouse fixture or a mixed one without a test naming a target
    kind. Items the fixture does not list stay unbound, which is how the mixed
    estate proves its Warehouse leaves are omitted.

    ``lakehouses`` maps a specific item to its own Lakehouse, for the one thing a
    single destination cannot express: a cross-item alias needs the producer and
    the consumer in *different* Lakehouses, or the alias would point a name at
    something already in the same place. ``lakehouse`` remains the default for
    every item not named there, so single-target fixtures are untouched.
    """

    from weaver.build_bundle import (
        ItemBinding,
        ItemBindings,
        LakehouseBinding,
        WarehouseBinding,
    )
    from weaver.declaration.model import LAKEHOUSE, WeaverItemId

    by_item = {WeaverItemId.parse(name): ref for name, ref in (lakehouses or {}).items()}
    entries = []
    for name in weaver_repo_fixture.items:
        item = WeaverItemId.parse(name)
        if item.item_type == LAKEHOUSE:
            bound = by_item.get(item, lakehouse)
            if bound is None:
                raise AssertionError(f"{item} needs a Lakehouse this env does not have")
            entries.append(ItemBinding(item, LakehouseBinding(lakehouse=bound)))
        else:
            if warehouse is None:
                raise AssertionError(f"{item} needs a Warehouse this env does not have")
            entries.append(ItemBinding(item, WarehouseBinding(warehouse=warehouse)))
    return ItemBindings(tuple(entries))


#: Every schema any build fixture registers, in either Lakehouse. Dropped on
#: local-env teardown, so a shared Spark catalogue never leaks one test's objects
#: into the next — the one place catalogue cleanup lives; tests never do it
#: themselves. They are dropped through the destination, because a local schema's
#: real database name carries the Lakehouse it belongs to.
_LOCAL_SCHEMAS = ("DWG", "Raw", "Legacy", "Sales", "Reporting", "Wh", "Rpt", "_")


def _local_lakehouse_setup(root, extra=()):
    from weaver import ItemRef, LocalWorkspace, LocalResolver, LocalStore

    workspace = LocalWorkspace(workspace=root, weaver_lakehouse="Weaver")
    store = LocalStore()
    resolver = LocalResolver(workspace)
    weaver, target = ItemRef("Weaver"), ItemRef("Sales_LH")
    for item in (weaver, target, *(ItemRef(name) for name in extra)):
        store.make_directory(resolver.files_root(item))
        store.make_directory(resolver.tables_root(item))
    store.make_directory(resolver.weaver_items_root)
    return workspace, weaver, target, resolver, store


@contextmanager
def _local_build_context(root, spark, weaver_repo_fixture):
    """A local Spark BuildEnv over a fresh Lakehouse root. Used by both the
    function-scoped fixture and the module-scoped estate."""

    from weaver.build_bundle import (
        InstallationEnvironment,
        LakehouseBinding,
        effective_item_bindings,
        install_bundle,
        load_bundle,
    )
    from weaver.build_bundle.workflow import (
        read_reconciled_catalogue,
        read_target_inventories,
    )
    from weaver.build_bundle.planner import generate_item_build_bundle
    from weaver.declaration import parse_item_repository

    workspace, weaver, target, resolver, store = _local_lakehouse_setup(
        root, extra=weaver_repo_fixture.extra_lakehouses
    )
    destination = resolver.spark_destination(target)
    weaver_destination = resolver.spark_destination(weaver)
    from weaver import ItemRef as _ItemRef

    named_lakehouses = {
        item: _ItemRef(name)
        for item, name in weaver_repo_fixture.lakehouse_names.items()
    }

    def install_repo() -> None:
        destination = resolver.weaver_items_root
        if store.exists(destination):
            store.delete(destination, recursive=True)
        _upload_tree(store, weaver_repo_fixture.path, destination)

    def remove_repo() -> None:
        store.delete(resolver.weaver_items_root, recursive=True)

    def generate(bundle_name: str = "buildtest"):
        root_location = resolver.weaver_items_root
        repository = parse_item_repository(root_location, store=store)
        control = LakehouseBinding(lakehouse=weaver)
        bindings = effective_item_bindings(
            _bindings_for(
                weaver_repo_fixture, lakehouse=target, lakehouses=named_lakehouses
            ),
            weaver_lakehouse=weaver.name,
        )
        environment = InstallationEnvironment(
            store=store,
            resolver=resolver,
            spark=spark,
            workspace=workspace,
        )
        inventories = read_target_inventories(bindings, environment=environment)
        reconciled = read_reconciled_catalogue(
            bindings,
            inventories=inventories,
            environment=environment,
            repository=repository,
        )
        return generate_item_build_bundle(
            repository,
            bindings=bindings,
            output=resolver.build_bundle(bundle_name),
            store=store,
            control_lakehouse=control,
            target_inventories=inventories,
            reconciled_catalogue=reconciled,
        )

    def install(bundle) -> InstallOutcome:
        report = install_bundle(
            load_bundle(bundle.location, store=store),
            environment=InstallationEnvironment(store=store, resolver=resolver, spark=spark),
        )
        return _outcome_from_report(report)

    def query(sql: str) -> list:
        return [row.asDict() for row in spark.sql(sql).collect()]

    def columns(table: str) -> list:
        return [
            {"name": f.name, "type": f.dataType.simpleString(), "nullable": f.nullable}
            for f in spark.table(table).schema
        ]

    def schema_exists(qualified: str) -> bool:
        return bool(spark.catalog.databaseExists(qualified))

    def run_python(body: str, *, label: str = "shared body"):
        """Run a shared body in this process, against the local session.

        The Fabric side sends the same text to a Livy session, where ``emit`` is
        part of the bootstrap. Here it is a local closure, so a body written once
        runs unchanged either side. ``label`` exists for the Fabric side's Livy
        accounting and is ignored here, where a body costs nothing to run.
        """

        emitted = []
        namespace = {
            "spark": spark,
            "resolver": resolver,
            "target": target,
            "emit": emitted.append,
        }
        exec(compile(body, "<shared body>", "exec"), namespace)
        return emitted[-1] if emitted else None

    def seed_orphans() -> None:
        # Seeded *in the destination*, through the same addressing the build uses,
        # so what prune has to find is genuinely in the Lakehouse under test.
        catalogue = SparkCatalogue(spark, destination)
        for schema in ("DWG", "Legacy"):
            catalogue.create_schema(schema)
        catalogue.sql("CREATE TABLE {{object:DWG.OldTable}} (x int) USING delta")
        catalogue.sql("CREATE OR REPLACE VIEW {{object:DWG.OldView}} AS SELECT 1 AS x")
        catalogue.sql("CREATE TABLE {{object:Legacy.OldThing}} (x int) USING delta")
        files_root = resolver.files_root(target)
        store.write(files_root.join("Raw", "OldFolder", "stale.csv"), b"old\n")
        store.write(files_root.join("Legacy", "Stuff", "f.txt"), b"x\n")

    named_destinations = {
        item: resolver.spark_destination(ref)
        for item, ref in named_lakehouses.items()
    }
    try:
        yield BuildEnv(
            label="local", workspace=workspace, weaver=weaver, target=target,
            resolver=resolver, store=store, generate_spark=spark,
            install_repo=install_repo, remove_repo=remove_repo, generate=generate,
            install=install, run_query=query, run_columns=columns,
            seed_orphans=seed_orphans, run_schema_exists=schema_exists,
            run_python=run_python,
            destination=destination, weaver_destination=weaver_destination,
            destinations=named_destinations,
        )
    finally:
        for schema in _LOCAL_SCHEMAS:
            for place in (destination, weaver_destination, *named_destinations.values()):
                spark.sql(
                    f"DROP SCHEMA IF EXISTS {place.qualified_schema(schema)} CASCADE"
                )


@pytest.fixture
def local_build_env(tmp_path, spark, weaver_repo_fixture):
    """A build environment installed in-process against local Spark, per test."""

    with _local_build_context(tmp_path, spark, weaver_repo_fixture) as env:
        yield env


@contextmanager
def _fabric_build_context(
    fabric_workspace_item, fabric_client, workspace, target_lh, session, weaver_repo_fixture
):
    """A build environment run entirely inside Fabric over Livy.

    Weaver is Fabric-first: both **generation and installation** run in the
    session, against the native Spark catalogue, so planning sees catalogue views
    and nothing is contorted to fit a desktop planner. The desktop only pushes the
    repository and reads back results. Both Lakehouses are disposable and are
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

    destination = resolver.spark_destination(target)
    _empty_the_target(session, store, resolver, target, destination)
    weaver_destination = resolver.spark_destination(weaver)

    def install_repo() -> None:
        destination = resolver.weaver_items_root
        if store.exists(destination):
            store.delete(destination, recursive=True)
        _upload_tree(store, weaver_repo_fixture.path, destination)

    def remove_repo() -> None:
        store.delete(resolver.weaver_items_root, recursive=True)

    def _workspace_literal() -> str:
        return (
            f"FabricWorkspace(workspace={workspace.workspace!r}, "
            f"weaver_lakehouse={workspace.weaver_lakehouse!r}, "
            f"environment={workspace.environment!r})"
        )

    def generate(bundle_name: str = "buildtest"):
        # Generation runs IN the session, against the native Spark catalogue.
        binds = ", ".join(
            f"ItemBinding(WeaverItemId.parse({item!r}), "
            f"LakehouseBinding(lakehouse=ItemRef({target.name!r})))"
            for item in weaver_repo_fixture.items
        )
        body = (
            "from weaver import ItemRef, FabricWorkspace, WeaverItemId\n"
            "from weaver.resolution import resolver_for, store_for\n"
            "from weaver.declaration import parse_item_repository\n"
            "from weaver.build_bundle import ItemBinding, ItemBindings, "
            "LakehouseBinding, InstallationEnvironment, effective_item_bindings\n"
            "from weaver.build_bundle.workflow import (read_target_inventories, "
            "read_reconciled_catalogue)\n"
            "from weaver.build_bundle.planner import generate_item_build_bundle\n"
            f"workspace = {_workspace_literal()}\n"
            "store = store_for(workspace)\n"
            "resolver = resolver_for(workspace)\n"
            "repository = parse_item_repository(resolver.weaver_items_root, store=store)\n"
            f"control = LakehouseBinding(lakehouse=ItemRef({weaver.name!r}))\n"
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
            "    repository,\n"
            "    bindings=bindings,\n"
            f"    output=resolver.build_bundle({bundle_name!r}),\n"
            "    store=store, control_lakehouse=control,\n"
            "    target_inventories=inventories, reconciled_catalogue=reconciled)\n"
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
            "from weaver import FabricWorkspace\n"
            "from weaver.resolution import resolver_for, store_for\n"
            "from weaver.build_bundle import install_bundle, load_bundle, "
            "InstallationEnvironment\n"
            f"workspace = {_workspace_literal()}\n"
            "store = store_for(workspace)\n"
            "resolver = resolver_for(workspace)\n"
            "env = InstallationEnvironment(store=store, resolver=resolver, spark=spark)\n"
            f"bundle = load_bundle(resolver.build_bundle({bundle_name!r}), store=store)\n"
            "report = install_bundle(bundle, environment=env)\n"
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
            "from weaver import FabricWorkspace, ItemRef\n"
            "from weaver.resolution import resolver_for\n"
            f"resolver = resolver_for({_workspace_literal()})\n"
            f"target = ItemRef({target.name!r})\n"
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
        resolver=resolver, store=store, generate_spark=True,  # in-session catalogue
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


def _empty_the_target(session, store, resolver, target, destination) -> None:
    """Leave the target Lakehouse as though nothing had been built into it.

    Catalogue first, then storage: dropping a schema removes its tables and views
    from the catalogue, and anything left on disk afterwards was never registered.
    """

    statements = [
        f"DROP SCHEMA IF EXISTS {destination.qualified_schema(schema)} CASCADE"
        for schema in _FABRIC_TARGET_SCHEMAS
    ]
    session.run(
        "".join(f"spark.sql({s!r})\n" for s in statements) + "emit(True)\n",
        label="empty target",
    )

    for area in (resolver.tables_root(target), resolver.files_root(target)):
        try:
            entries = store.list(area)
        except Exception:  # the area may not exist yet
            continue
        for entry in entries:
            if entry.is_directory and entry.name.lower() != "dbo":
                try:
                    store.delete(entry.location, recursive=True)
                except Exception as exc:
                    print(f"warning: could not empty {entry.location}: {exc}")


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
    connector. The desktop uploads only the Weaver document repository and reads results back
    for assertions; it never plans and never compiles a bundle locally.
    """

    from weaver import ItemRef as _ItemRef
    from weaver.build_bundle import BuildBundle, BuildPlan
    from weaver.fabric import FabricResolver, OneLakeDfsClient

    resolver = FabricResolver(fabric_workspace, client=None)
    store = OneLakeDfsClient()
    weaver = _ItemRef(weaver_lakehouse.name)
    warehouse_ref = _ItemRef(warehouse.item.name)
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
        destination = resolver.weaver_items_root
        if store.exists(destination):
            store.delete(destination, recursive=True)
        _upload_tree(store, weaver_repo_fixture.path, destination)

    def remove_repo() -> None:
        store.delete(resolver.weaver_items_root, recursive=True)

    def generate(bundle_name: str = "whtest"):
        # Generation runs IN Fabric and reads the Warehouse catalogue there
        # through its own Fabric-native SQL — no sql= injection.
        binds = ", ".join(
            f"ItemBinding(WeaverItemId.parse({item!r}), "
            f"WarehouseBinding(warehouse=ItemRef({warehouse_ref.name!r})))"
            for item in weaver_repo_fixture.items
        )
        body = (
            "from weaver import ItemRef, FabricWorkspace, WeaverItemId\n"
            "from weaver.resolution import resolver_for, store_for\n"
            "from weaver.declaration import parse_item_repository\n"
            "from weaver.build_bundle import ItemBinding, ItemBindings, "
            "WarehouseBinding, LakehouseBinding, InstallationEnvironment, "
            "effective_item_bindings\n"
            "from weaver.build_bundle.workflow import (read_target_inventories, "
            "read_reconciled_catalogue)\n"
            "from weaver.build_bundle.planner import generate_item_build_bundle\n"
            f"workspace = {_workspace_literal()}\n"
            "store = store_for(workspace)\n"
            "resolver = resolver_for(workspace)\n"
            "repository = parse_item_repository(resolver.weaver_items_root, store=store)\n"
            f"selected = ItemBindings(({binds},))\n"
            "bindings = effective_item_bindings("
            "selected, weaver_lakehouse=workspace.weaver_lakehouse)\n"
            "control = LakehouseBinding(ItemRef(workspace.weaver_lakehouse))\n"
            "environment = InstallationEnvironment("
            "store=store, resolver=resolver, spark=spark, workspace=workspace)\n"
            "inventories = read_target_inventories(bindings, environment=environment)\n"
            "reconciled = read_reconciled_catalogue("
            "bindings, inventories=inventories, environment=environment, "
            "repository=repository)\n"
            "bundle = generate_item_build_bundle(\n"
            "    repository,\n"
            "    bindings=bindings,\n"
            f"    output=resolver.build_bundle({bundle_name!r}),\n"
            "    store=store, control_lakehouse=control,\n"
            "    target_inventories=inventories, reconciled_catalogue=reconciled)\n"
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
            "from weaver import FabricWorkspace\n"
            "from weaver.resolution import resolver_for, store_for\n"
            "from weaver.build_bundle import install_bundle, load_bundle, "
            "InstallationEnvironment\n"
            f"workspace = {_workspace_literal()}\n"
            "store = store_for(workspace)\n"
            "resolver = resolver_for(workspace)\n"
            "env = InstallationEnvironment(store=store, resolver=resolver, "
            "spark=spark, workspace=workspace)\n"
            f"bundle = load_bundle(resolver.build_bundle({bundle_name!r}), store=store)\n"
            "report = install_bundle(bundle, environment=env)\n"
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
        resolver=resolver, store=store, generate_spark=None,
        install_repo=install_repo, remove_repo=remove_repo, generate=generate,
        install=install, run_query=query, run_columns=columns,
        seed_orphans=seed_orphans,
    )


def _install_estate(env) -> InstalledEstate:
    """Install one estate through a BuildEnv, once, and assert it succeeded."""

    env.install_repo()
    bundle = env.generate()
    outcome = env.install(bundle)
    assert outcome.status == "succeeded", outcome.action_error
    return InstalledEstate(env=env, bundle=bundle)


#: No transport marks, deliberately — unlike every other parametrised estate here.
#: The journey answers to `full_integration` alone, so it is opted into by name
#: and never swept up by `-m spark` or `-m fabric`. Both transports run when it is
#: asked for, because the composition proof is worth having on both; `-k local` or
#: `-k fabric` narrows it. Each param still skips itself when its prerequisite is
#: missing, so asking for the journey without a JDK or a workspace says so.
@pytest.fixture(
    scope="module",
    params=[
        pytest.param("local", id="local"),
        pytest.param("fabric", id="fabric"),
    ],
)
def lakehouse_journey(request, weaver_repo_fixture):
    """One Lakehouse estate for a journey to drive.

    **The transitions are deliberately not run here.** A fixture that took every
    move up front and then handed the result to a set of tests would leave every
    one of them inspecting the *final* estate, whatever transition each claimed
    to be about — and a later move could then repair what an earlier one broke,
    so the earlier assertion would pass on evidence that no longer existed. The
    physical state has to be read at the point it is claimed about, which means
    the moves and the assertions have to be interleaved by the test.

    What is shared is the estate: one target Lakehouse, installed from nothing
    once, with every move after it an incremental build over a target that is
    already correct.
    """

    with _journey_context(request, weaver_repo_fixture) as env:
        yield Journey(env, "lakehouse")


@contextmanager
def _journey_context(request, weaver_repo_fixture):
    """The build environment a journey drives, on whichever transport it asked for."""

    if request.param == "local":
        spark = request.getfixturevalue("spark")
        root = request.getfixturevalue("tmp_path_factory").mktemp("journey")
        with _local_build_context(root, spark, weaver_repo_fixture) as env:
            yield env
    else:
        with _fabric_build_context(
            request.getfixturevalue("fabric_workspace_item"),
            request.getfixturevalue("fabric_client"),
            request.getfixturevalue("fabric_workspace"),
            request.getfixturevalue("fabric_target_lakehouse"),
            request.getfixturevalue("livy_session"),
            weaver_repo_fixture,
        ) as env:
            yield env


@pytest.fixture(
    scope="module",
    params=[pytest.param("local", marks=pytest.mark.spark, id="local")],
)
def local_lakehouse_estate(request, weaver_repo_fixture):
    """One Lakehouse estate on local Spark only.

    For modules whose subject is transport-independent — what the planner
    decides, what DDL is emitted, how a dependency chain orders — where the
    Fabric half of :func:`lakehouse_estate` runs the same body against the same
    assertions and proves nothing further.

    That duplicate is not free. Each module-scoped Fabric estate is one full
    generate-and-install, 75–123s, and the *estate* is the cost rather than the
    tests: six assertions over one estate cost exactly what one does. So a module
    that has no Fabric-specific claim to make is a whole install that can go,
    while its coverage stays exactly where it was.

    Reach for :func:`lakehouse_estate` when the claim genuinely differs by
    transport — a resolver path, a SQL endpoint, a real catalogue read.
    """

    spark = request.getfixturevalue("spark")
    root = request.getfixturevalue("tmp_path_factory").mktemp("estate")
    with _local_build_context(root, spark, weaver_repo_fixture) as env:
        yield _install_estate(env)


@pytest.fixture(
    scope="module",
    params=[
        pytest.param("local", marks=pytest.mark.spark, id="local"),
        pytest.param("fabric", marks=pytest.mark.fabric, id="fabric"),
    ],
)
def lakehouse_estate(request, weaver_repo_fixture):
    """One Lakehouse estate, provisioned and installed **once per module** on both
    local Spark and Fabric. Read-only assertions reuse it, so a whole module of
    Fabric checks costs one target Lakehouse and one install — the Weaver
    Lakehouse and the Livy session are the run's."""

    if request.param == "local":
        spark = request.getfixturevalue("spark")
        root = request.getfixturevalue("tmp_path_factory").mktemp("estate")
        with _local_build_context(root, spark, weaver_repo_fixture) as env:
            yield _install_estate(env)
    else:
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


@pytest.fixture
def build_env(request, weaver_repo_fixture):
    """Select a concrete build environment by indirect parameter.

    Requesting ``weaver_repo_fixture`` here (though the concrete env fixture is the one
    that uses it) anchors it in the test's static fixture closure, so a test can
    point the environment at another repository with
    ``@pytest.mark.parametrize("weaver_repo_fixture", [...], indirect=True)`` even though
    the environment itself is resolved dynamically.
    """

    return request.getfixturevalue(request.param)
