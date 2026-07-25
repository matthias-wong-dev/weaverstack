"""Fixtures for opt-in Fabric integration tests.

These touch a real workspace and a running capacity, so they are deselected by
default and skip unless `WEAVER_FABRIC_WORKSPACE` names a workspace to use.

They create their own Lakehouses and delete them afterwards. Nothing
pre-existing in the workspace is touched, and the names are prefixed so a
leftover from an interrupted run is recognisable.
"""

from __future__ import annotations

import os
import time
import uuid
from collections.abc import Callable
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import pytest

from weaver import Host, ItemRef, Store

WORKSPACE_ENV = "WEAVER_FABRIC_WORKSPACE"
#: The Environment the session attaches to — installed once with `weaver install`
#: and consumed by the suite, never uploaded by it.
ENVIRONMENT_ENV = "WEAVER_FABRIC_ENVIRONMENT"
DEFAULT_ENVIRONMENT = "weaver"

#: Disposable items carry this prefix so an abandoned one is obvious.
TEST_PREFIX = "weavertest"
WAREHOUSE_READY_TIMEOUT = 600.0
WAREHOUSE_POLL_INTERVAL = 5.0


@pytest.fixture(scope="session")
def fabric_workspace():
    """The workspace named by WEAVER_FABRIC_WORKSPACE."""

    pytest.importorskip("azure.identity", reason="install the [fabric] extra")
    pytest.importorskip("requests", reason="install the [fabric] extra")

    # Credential choice is caller policy, not core's; the test infra is a caller.
    from weaver.fabric.auth import prefer_cli_credential

    prefer_cli_credential()

    name = os.environ.get(WORKSPACE_ENV)
    if not name:
        pytest.skip(f"set {WORKSPACE_ENV} to run Fabric tests")

    from weaver.errors import WeaverError
    from weaver.fabric import find_workspace

    try:
        return find_workspace(name)
    except WeaverError as exc:
        pytest.skip(f"cannot reach workspace {name!r}: {exc}")


@pytest.fixture(scope="session")
def fabric_client(fabric_workspace):
    from weaver.fabric import FabricClient

    return FabricClient()


def _disposable_name(role: str) -> str:
    """A name no human would have chosen, so cleanup is unambiguous."""

    return f"{TEST_PREFIX}_{role}_{uuid.uuid4().hex[:8]}"


def _warehouse_name() -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return f"Weaver_Pytest_{timestamp}_{uuid.uuid4().hex[:4]}"


@dataclass(frozen=True)
class PopulatedLakehouse:
    """One populated target, with transport hidden from the shared test."""

    host: Host
    target: ItemRef
    resolver: Any
    store: Store
    wipe: Callable[[], tuple[str, ...]]


@pytest.fixture
def fabric_lakehouses(fabric_workspace, fabric_client):
    """A Weaver Lakehouse and a target Lakehouse, created and then deleted.

    The local equivalent of this fixture is `lakehouses`, and the pair are
    deliberately shaped the same so a test can be written against either.
    """

    from weaver.fabric import create_lakehouse, delete_item

    created = []
    try:
        weaver = create_lakehouse(
            fabric_workspace, _disposable_name("weaver"), client=fabric_client
        )
        created.append(weaver)
        target = create_lakehouse(
            fabric_workspace, _disposable_name("target"), client=fabric_client
        )
        created.append(target)
        yield {"workspace": fabric_workspace, "weaver": weaver, "target": target}
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
def fabric_weaver_lakehouse(fabric_workspace, fabric_client):
    """One Lakehouse standing in as the Weaver Lakehouse for the whole run.

    The Livy session is created against it; Weaver itself comes from the
    attached Environment, not from here.
    """

    from weaver.fabric import create_lakehouse, delete_item

    item = create_lakehouse(
        fabric_workspace, _disposable_name("home"), client=fabric_client
    )
    try:
        yield item
    finally:
        try:
            delete_item(item, client=fabric_client)
        except Exception as exc:
            print(f"warning: could not delete {item}: {exc}")


@pytest.fixture(scope="session")
def fabric_environment_name():
    return os.environ.get(ENVIRONMENT_ENV, DEFAULT_ENVIRONMENT)


@pytest.fixture(scope="session")
def fabric_host(fabric_workspace, fabric_weaver_lakehouse, fabric_environment_name):
    """A host that names the Environment Weaver was installed into."""

    from weaver import FabricHost

    return FabricHost(
        workspace=fabric_workspace.name,
        weaver_lakehouse=fabric_weaver_lakehouse.name,
        fabric_environment=fabric_environment_name,
    )


@pytest.fixture(scope="session")
def livy_session(fabric_host):
    """One Spark session in Fabric with the Weaver Environment attached.

    Skips — rather than fails — when the Environment is missing or carries no
    usable Weaver, because that means ``weaver install`` has not been run, which
    is a setup step, not a defect in what is under test.
    """

    from weaver.errors import CommandError
    from weaver.fabric import LivyError, LivySession

    try:
        session = LivySession.for_host(fabric_host)
    except CommandError as exc:
        pytest.skip(f"{exc}; run `weaver install` into the Environment first")
    started = time.monotonic()
    try:
        session.start()
    except LivyError as exc:
        pytest.skip(f"could not start a Livy session (Environment installed?): {exc}")
    session.weaver_startup_seconds = time.monotonic() - started
    print(f"Fabric Livy session startup: {session.weaver_startup_seconds:.2f}s")
    try:
        yield session
    finally:
        session.close()


# --- disposable Warehouse ----------------------------------------------------


@dataclass
class DisposableWarehouse:
    item: Any
    host: Host
    target: Any
    endpoint: Any
    executor: Any
    timings: dict[str, float]
    started: float


@pytest.fixture(scope="module")
def disposable_warehouse(fabric_workspace, fabric_client, fabric_host):
    """Create, await, expose, and always delete one disposable Warehouse per module.

    A Warehouse takes minutes to provision, so it is shared across a module's
    tests rather than recreated per test."""

    from weaver import WarehouseTarget
    from weaver.fabric import (
        FabricResolver,
        create_warehouse,
        delete_item,
        desktop_sql_executor,
    )

    started = time.monotonic()
    timings: dict[str, float] = {}
    item = None
    executor = None
    name = _warehouse_name()
    try:
        stage = time.monotonic()
        item = create_warehouse(fabric_workspace, name, client=fabric_client)
        timings["item creation"] = time.monotonic() - stage
        print(f"Warehouse {name} item creation: {timings['item creation']:.2f}s")

        target = WarehouseTarget.parse(name)
        deadline = time.monotonic() + WAREHOUSE_READY_TIMEOUT
        last_error: Exception | None = None
        endpoint = None

        stage = time.monotonic()
        while time.monotonic() < deadline:
            try:
                resolver = FabricResolver(fabric_host, client=fabric_client)
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
                    fabric_host,
                    resolver=FabricResolver(fabric_host, client=fabric_client),
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
            host=fabric_host,
            target=target,
            endpoint=endpoint,
            executor=executor,
            timings=timings,
            started=started,
        )
    finally:
        if executor is not None:
            executor.close()
        if item is not None:
            deletion_started = time.monotonic()
            try:
                delete_item(item, client=fabric_client)
                deletion = time.monotonic() - deletion_started
                total = time.monotonic() - started
                print(
                    f"Warehouse {name} deletion: {deletion:.2f}s; "
                    f"total fixture lifetime: {total:.2f}s"
                )
            except Exception as exc:
                print(
                    f"warning: leaked Warehouse {name!r} ({item.id}); "
                    f"cleanup failed: {exc}"
                )


# --- one populated lifecycle, on either host --------------------------------


@pytest.fixture
def populated_local_lakehouse(populated_local_lakehouses):
    """Adapt the preserved local lifecycle to the shared fixture result."""

    from weaver import DeltaTarget, wipe_delta_target

    def wipe() -> tuple[str, ...]:
        report = wipe_delta_target(
            DeltaTarget(lakehouse=populated_local_lakehouses.target),
            populated_local_lakehouses.host,
        )
        return report.removed

    return PopulatedLakehouse(
        host=populated_local_lakehouses.host,
        target=populated_local_lakehouses.target,
        resolver=populated_local_lakehouses.resolver,
        store=populated_local_lakehouses.store,
        wipe=wipe,
    )


@pytest.fixture
def populated_fabric_lakehouse(
    fabric_workspace,
    fabric_client,
    fabric_host,
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
            fabric_workspace,
            _disposable_name("target"),
            client=fabric_client,
        )
        target = ItemRef(item.name)
        resolver = FabricResolver(fabric_host, client=fabric_client)
        store = OneLakeDfsClient()

        populate_folder_files(store, resolver, target)
        tables_root = f"{resolver.spark_root(target)}/Tables"
        statements = [
            statement
            for script in ("build.spark.sql", "load.spark.sql")
            for statement in lakehouse_sql_statements(script, tables_root)
        ]
        body = "\n".join(f"spark.sql({statement!r})" for statement in statements)
        result = livy_session.run(f"{body}\nemit(True)\n")
        assert result.payload is True

        def wipe() -> tuple[str, ...]:
            body = (
                "from weaver import FabricHost, DeltaTarget, wipe_delta_target\n"
                f"host = FabricHost(workspace={fabric_host.workspace!r}, "
                f"weaver_lakehouse={fabric_host.weaver_lakehouse!r}, "
                f"fabric_environment={fabric_host.fabric_environment!r})\n"
                f"target = DeltaTarget.parse({target.name!r})\n"
                "report = wipe_delta_target(target, host)\n"
                "emit({'removed': list(report.removed)})\n"
            )
            result = livy_session.run(body)
            return tuple(result.payload["removed"])

        yield PopulatedLakehouse(
            host=fabric_host,
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

from build_envs import BUILD_FIXTURE  # the default fixture a build env installs


@pytest.fixture(scope="module")
def ses_fixture(request):
    """Which SES repository a build env installs — the default, or a test's choice.

    Module-scoped so an estate is provisioned once per test module. Parametrise
    indirectly (with a fixture from ``build_envs``) to point an environment at
    another fixture without changing the environment fixtures::

        @pytest.mark.parametrize("ses_fixture", [SQL_TABLE_FIXTURE], indirect=True)
    """

    return getattr(request, "param", BUILD_FIXTURE)


@dataclass
class InstalledEstate:
    """One estate provisioned and installed once, for read-only assertions.

    ``repo`` is the installed repository name, so a test that rebuilds (e.g. to
    exercise prune) names the same repository rather than guessing it.
    """

    env: "BuildEnv"
    bundle: Any
    repo: str


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
    """Everything a build test needs, with transport hidden behind callables."""

    label: str
    host: Any
    weaver: ItemRef
    target: ItemRef
    resolver: Any
    store: Store
    generate_spark: Any
    install_repo: Callable[[str], str]
    remove_repo: Callable[[str], None]
    generate: Callable[..., Any]
    install: Callable[[Any], InstallOutcome]
    query: Callable[[str], list]
    #: ``[{"name", "type", "nullable"}]`` for a table — schema with nullability,
    #: which ``query``/DESCRIBE cannot give. Warehouse reads it from the catalogue.
    columns: Callable[[str], list]
    seed_orphans: Callable[[], None]


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
    for path in sorted(source.rglob("*")):
        if path.is_file():
            store.write(destination.join(*path.relative_to(source).parts), path.read_bytes())


def _create_schema_enabled_lakehouse(client, workspace, name):
    """A Lakehouse with schemas enabled, so `schema.table` resolves and a managed
    table lands under Tables/<schema>/<table> — which plain create_lakehouse omits."""

    import time as _time

    from weaver.fabric.resources import LAKEHOUSE, Item, find_item

    resp = client.request(
        "POST",
        f"workspaces/{workspace.id}/lakehouses",
        payload={"displayName": name, "creationPayload": {"enableSchemas": True}},
        expected=(200, 201, 202, 409),
    )
    if resp.status_code == 202:
        for _ in range(40):
            try:
                return find_item(workspace, name, item_type=LAKEHOUSE, client=client)
            except Exception:
                _time.sleep(3)
        raise RuntimeError(f"schema-enabled Lakehouse {name!r} never appeared")
    body = resp.json()
    return Item(id=body["id"], name=name, type=LAKEHOUSE, workspace_id=workspace.id)


#: Every schema any build fixture registers. Dropped on local-env teardown, so a
#: shared Spark catalog never leaks one test's objects into the next — the one
#: place catalog cleanup lives; tests never do it themselves.
_LOCAL_SCHEMAS = ("DWG", "Raw", "Legacy", "Sales", "Reporting", "Wh", "Rpt")


def _local_lakehouse_setup(root):
    from weaver import ItemRef, LocalHost, LocalResolver, LocalStore

    host = LocalHost(root=root, weaver_lakehouse="Weaver")
    store = LocalStore()
    resolver = LocalResolver(host)
    weaver, target = ItemRef("Weaver"), ItemRef("Sales_LH")
    for item in (weaver, target):
        store.make_directory(resolver.files_root(item))
        store.make_directory(resolver.tables_root(item))
    store.make_directory(resolver.repos_root)
    return host, weaver, target, resolver, store


@contextmanager
def _local_build_context(root, spark, ses_fixture):
    """A local Spark BuildEnv over a fresh Lakehouse root. Used by both the
    function-scoped fixture and the module-scoped estate."""

    from weaver import RepositoryRef
    from weaver.build_bundle import (
        InstallationEnvironment,
        LakehouseBinding,
        TargetBindings,
        generate_build_bundle,
        install_bundle,
        load_bundle,
    )

    host, weaver, target, resolver, store = _local_lakehouse_setup(root)

    def install_repo(name: str) -> str:
        _upload_tree(store, ses_fixture, resolver.repository(RepositoryRef(name)))
        return name

    def remove_repo(name: str) -> None:
        store.delete(resolver.repository(RepositoryRef(name)), recursive=True)

    def generate(bundle_name: str = "buildtest", *, repository_name: str = "MyRepo", prune: bool = True):
        return generate_build_bundle(
            weaver_lakehouse=weaver,
            repository_name=repository_name,
            targets=TargetBindings(lakehouse=LakehouseBinding(lakehouse=target)),
            output=resolver.build_bundle(bundle_name),
            host=host,
            store=store,
            prune=prune,
            spark=spark,
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

    def seed_orphans() -> None:
        tables_root = resolver.tables_root(target).value
        files_root = resolver.files_root(target)
        spark.sql(f"CREATE DATABASE IF NOT EXISTS DWG LOCATION '{tables_root}/DWG'")
        spark.sql("CREATE TABLE DWG.OldTable (x int) USING delta")
        spark.sql("CREATE OR REPLACE VIEW DWG.OldView AS SELECT 1 AS x")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS Legacy LOCATION '{tables_root}/Legacy'")
        spark.sql("CREATE TABLE Legacy.OldThing (x int) USING delta")
        store.write(files_root.join("Raw", "OldFolder", "stale.csv"), b"old\n")
        store.write(files_root.join("Legacy", "Stuff", "f.txt"), b"x\n")

    try:
        yield BuildEnv(
            label="local", host=host, weaver=weaver, target=target,
            resolver=resolver, store=store, generate_spark=spark,
            install_repo=install_repo, remove_repo=remove_repo, generate=generate,
            install=install, query=query, columns=columns, seed_orphans=seed_orphans,
        )
    finally:
        for database in _LOCAL_SCHEMAS:
            spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")


@pytest.fixture
def local_build_env(tmp_path, spark, ses_fixture):
    """A build environment installed in-process against local Spark, per test."""

    with _local_build_context(tmp_path, spark, ses_fixture) as env:
        yield env


@contextmanager
def _fabric_build_context(fabric_workspace, fabric_client, fabric_environment_name, ses_fixture):
    """A build environment run entirely inside Fabric over Livy.

    Weaver is Fabric-first: both **generation and installation** run in the
    session, against the native Spark catalogue, so planning sees catalogue views
    and nothing is contorted to fit a desktop planner. The session defaults to the
    **target** Lakehouse (so two-part ``Schema.Object`` names land there), which is
    **schema-enabled** (so a managed table appears under ``Tables/<schema>/<table>``
    and views bind by name). The desktop only pushes the repository and reads back
    results. Both Lakehouses are disposable and deleted on teardown.
    """

    from weaver import FabricHost, RepositoryRef
    from weaver.build_bundle import BuildBundle, BuildPlan
    from weaver.fabric import (
        FabricResolver,
        LivySession,
        OneLakeDfsClient,
        create_lakehouse,
        delete_item,
    )

    created = []
    session = None
    try:
        weaver_lh = create_lakehouse(fabric_workspace, _disposable_name("weaver"), client=fabric_client)
        created.append(weaver_lh)
        target_lh = _create_schema_enabled_lakehouse(
            fabric_client, fabric_workspace, _disposable_name("target")
        )
        created.append(target_lh)

        host = FabricHost(
            workspace=fabric_workspace.name,
            weaver_lakehouse=weaver_lh.name,
            fabric_environment=fabric_environment_name,
        )
        resolver = FabricResolver(host, client=fabric_client)
        store = OneLakeDfsClient()
        weaver = ItemRef(weaver_lh.name)
        target = ItemRef(target_lh.name)

        # One session, defaulted to the target Lakehouse, with the Weaver
        # Environment attached so the install program can import weaver.build_bundle.
        session_host = FabricHost(
            workspace=fabric_workspace.name,
            weaver_lakehouse=target_lh.name,
            fabric_environment=fabric_environment_name,
        )
        session = LivySession.for_host(session_host)
        session.start()

        def install_repo(name: str) -> str:
            _upload_tree(store, ses_fixture, resolver.repository(RepositoryRef(name)))
            return name

        def remove_repo(name: str) -> None:
            store.delete(resolver.repository(RepositoryRef(name)), recursive=True)

        def _host_literal() -> str:
            return (
                f"FabricHost(workspace={host.workspace!r}, "
                f"weaver_lakehouse={host.weaver_lakehouse!r}, "
                f"fabric_environment={host.fabric_environment!r})"
            )

        def generate(bundle_name: str = "buildtest", *, repository_name: str = "MyRepo", prune: bool = True):
            # Generation runs IN the session, against the native Spark catalogue —
            # so prune sees catalogue views, matching how a notebook would build.
            body = (
                "from weaver import ItemRef, FabricHost\n"
                "from weaver.resolution import resolver_for, store_for\n"
                "from weaver.build_bundle import generate_build_bundle, TargetBindings, "
                "LakehouseBinding\n"
                f"host = {_host_literal()}\n"
                "store = store_for(host)\n"
                "resolver = resolver_for(host)\n"
                "bundle = generate_build_bundle(\n"
                f"    weaver_lakehouse=ItemRef({weaver.name!r}),\n"
                f"    repository_name={repository_name!r},\n"
                f"    targets=TargetBindings(lakehouse=LakehouseBinding(lakehouse=ItemRef({target.name!r}))),\n"
                f"    output=resolver.build_bundle({bundle_name!r}),\n"
                f"    host=host, store=store, prune={prune!r}, spark=spark)\n"
                "emit({'name': bundle.location.name, 'bundle_id': bundle.bundle_id, "
                "'plan': bundle.plan.to_mapping()})\n"
            )
            payload = session.run(body).payload
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
                "from weaver import FabricHost\n"
                "from weaver.resolution import resolver_for, store_for\n"
                "from weaver.build_bundle import install_bundle, load_bundle, "
                "InstallationEnvironment\n"
                f"host = {_host_literal()}\n"
                "store = store_for(host)\n"
                "resolver = resolver_for(host)\n"
                "env = InstallationEnvironment(store=store, resolver=resolver, spark=spark)\n"
                f"bundle = load_bundle(resolver.build_bundle({bundle_name!r}), store=store)\n"
                "report = install_bundle(bundle, environment=env)\n"
                "emit({'status': report.status, 'bundle_id': report.bundle_id, "
                "'sequences': [{'number': s.number, 'status': s.status} for s in report.sequences], "
                "'actions': [{'id': a.action_id, 'status': a.status, "
                "'error': (a.error_type + ': ' + str(a.error_message)) if a.error_type else None} "
                "for a in report.action_results()]})\n"
            )
            payload = session.run(body).payload
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
            return session.run(body).payload

        def columns(table: str) -> list:
            body = (
                "emit([{'name': f.name, 'type': f.dataType.simpleString(), "
                f"'nullable': f.nullable}} for f in spark.table({table!r}).schema])\n"
            )
            return session.run(body).payload

        def seed_orphans() -> None:
            # Schema-enabled Lakehouse: CREATE SCHEMA + a managed table lands at
            # Tables/<schema>/<table>; no CREATE DATABASE / LOCATION.
            body = (
                "spark.sql('CREATE SCHEMA IF NOT EXISTS DWG')\n"
                "spark.sql('CREATE TABLE IF NOT EXISTS DWG.OldTable (x int) USING delta')\n"
                "spark.sql('CREATE OR REPLACE VIEW DWG.OldView AS SELECT 1 AS x')\n"
                "spark.sql('CREATE SCHEMA IF NOT EXISTS Legacy')\n"
                "spark.sql('CREATE TABLE IF NOT EXISTS Legacy.OldThing (x int) USING delta')\n"
                "emit(True)\n"
            )
            session.run(body)
            files_root = resolver.files_root(target)
            store.write(files_root.join("Raw", "OldFolder", "stale.csv"), b"old\n")
            store.write(files_root.join("Legacy", "Stuff", "f.txt"), b"x\n")

        yield BuildEnv(
            label="fabric", host=host, weaver=weaver, target=target,
            resolver=resolver, store=store, generate_spark=True,  # in-session catalogue
            install_repo=install_repo, remove_repo=remove_repo, generate=generate,
            install=install, query=query, columns=columns, seed_orphans=seed_orphans,
        )
    finally:
        if session is not None:
            try:
                session.close()
            except Exception as exc:
                print(f"warning: could not close Livy session: {exc}")
        for item in created:
            try:
                delete_item(item, client=fabric_client)
            except Exception as exc:
                print(f"warning: could not delete {item}: {exc}")


@pytest.fixture
def fabric_build_env(fabric_workspace, fabric_client, fabric_environment_name, ses_fixture):
    """One Fabric build environment per test — its own disposable Lakehouses and
    Livy session. Used where each test needs an isolated target (e.g. prune)."""

    with _fabric_build_context(
        fabric_workspace, fabric_client, fabric_environment_name, ses_fixture
    ) as env:
        yield env


def _warehouse_build_env(fabric_host, weaver_lakehouse, warehouse, ses_fixture) -> "BuildEnv":
    """A Warehouse BuildEnv: desktop generation, T-SQL install over the live SQL
    endpoint of a disposable Fabric Warehouse.

    A Warehouse build is single-target and Lakehouse-free here: generation is pure
    T-SQL text (no Spark), and installation runs the self-contained scripts through
    the pooled SQL executor the disposable Warehouse exposes. That same executor
    inspects the catalogue at plan time, so ``prune=True`` reconciles. Its
    ``query`` is T-SQL, so behavioural assertions read the Warehouse catalogue,
    not a Spark session.
    """

    from weaver import ItemRef as _ItemRef, RepositoryRef
    from weaver.build_bundle import (
        InstallationEnvironment,
        TargetBindings,
        WarehouseBinding,
        generate_build_bundle,
        install_bundle,
        load_bundle,
    )
    from weaver.fabric import FabricResolver, OneLakeDfsClient

    resolver = FabricResolver(fabric_host, client=None)
    store = OneLakeDfsClient()
    weaver = _ItemRef(weaver_lakehouse.name)
    warehouse_ref = _ItemRef(warehouse.item.name)
    sql = warehouse.executor

    def install_repo(name: str) -> str:
        _upload_tree(store, ses_fixture, resolver.repository(RepositoryRef(name)))
        return name

    def remove_repo(name: str) -> None:
        store.delete(resolver.repository(RepositoryRef(name)), recursive=True)

    def generate(bundle_name: str = "whtest", *, repository_name: str = "MyRepo", prune: bool = False):
        return generate_build_bundle(
            weaver_lakehouse=weaver,
            repository_name=repository_name,
            targets=TargetBindings(warehouse=WarehouseBinding(warehouse=warehouse_ref)),
            output=resolver.build_bundle(bundle_name),
            host=fabric_host,
            store=store,
            prune=prune,
            # Reconciliation reads the Warehouse catalogue at plan time.
            sql=sql,
        )

    def install(bundle) -> InstallOutcome:
        report = install_bundle(
            load_bundle(bundle.location, store=store),
            environment=InstallationEnvironment(store=store, resolver=resolver, sql=sql),
        )
        outcome = _outcome_from_report(report)
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
        label="warehouse", host=fabric_host, weaver=weaver, target=warehouse_ref,
        resolver=resolver, store=store, generate_spark=None,
        install_repo=install_repo, remove_repo=remove_repo, generate=generate,
        install=install, query=query, columns=columns, seed_orphans=seed_orphans,
    )


def _install_estate(env, repo: str = "Estate", *, prune: bool = True) -> InstalledEstate:
    """Install one estate through a BuildEnv, once, and assert it succeeded."""

    env.install_repo(repo)
    bundle = env.generate(repository_name=repo, prune=prune)
    outcome = env.install(bundle)
    assert outcome.status == "succeeded", outcome.action_error
    return InstalledEstate(env=env, bundle=bundle, repo=repo)


@pytest.fixture(
    scope="module",
    params=[
        pytest.param("local", marks=pytest.mark.spark, id="local"),
        pytest.param("fabric", marks=pytest.mark.fabric, id="fabric"),
    ],
)
def lakehouse_estate(request, ses_fixture):
    """One Lakehouse estate, provisioned and installed **once per module** on both
    local Spark and Fabric. Read-only assertions reuse it, so a whole module of
    Fabric checks costs one Lakehouse, one Livy session and one install."""

    if request.param == "local":
        spark = request.getfixturevalue("spark")
        root = request.getfixturevalue("tmp_path_factory").mktemp("estate")
        with _local_build_context(root, spark, ses_fixture) as env:
            yield _install_estate(env)
    else:
        with _fabric_build_context(
            request.getfixturevalue("fabric_workspace"),
            request.getfixturevalue("fabric_client"),
            request.getfixturevalue("fabric_environment_name"),
            ses_fixture,
        ) as env:
            yield _install_estate(env)


@pytest.fixture(scope="module")
def warehouse_estate(fabric_host, fabric_weaver_lakehouse, disposable_warehouse, ses_fixture):
    """The Warehouse estate, provisioned and installed **once per module** — one
    disposable Warehouse and one install for the whole module's checks."""

    env = _warehouse_build_env(
        fabric_host, fabric_weaver_lakehouse, disposable_warehouse, ses_fixture
    )
    yield _install_estate(env, prune=False)


@pytest.fixture
def build_env(request, ses_fixture):
    """Select a concrete build environment by indirect parameter.

    Requesting ``ses_fixture`` here (though the concrete env fixture is the one
    that uses it) anchors it in the test's static fixture closure, so a test can
    point the environment at another repository with
    ``@pytest.mark.parametrize("ses_fixture", [...], indirect=True)`` even though
    the environment itself is resolved dynamically.
    """

    return request.getfixturevalue(request.param)
