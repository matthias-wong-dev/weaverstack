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


@pytest.fixture
def disposable_warehouse(fabric_workspace, fabric_client, fabric_host):
    """Create, await, expose, and always delete one disposable Warehouse."""

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


# --- host-neutral build environment -----------------------------------------
#
# The same shape as PopulatedLakehouse above: one dataclass hides whether a build
# is installed in-process against a local Spark session or inside Fabric over
# Livy, so a single behavioural test runs on both. Generation always runs on the
# caller (position B) writing the bundle to the Weaver Lakehouse over the store;
# only installation and catalog queries differ by host.

from pathlib import Path as _Path

BUILD_FIXTURE = _Path(__file__).parent.parent / "fixtures" / "build-lakehouse"


@dataclass
class InstallOutcome:
    """A host-neutral view of an installation report."""

    status: str
    bundle_id: str
    sequence_status: dict[int, str]
    action_status: dict[str, str]
    action_order: tuple[str, ...]


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
    seed_orphans: Callable[[], None]


def _outcome_from_report(report) -> InstallOutcome:
    return InstallOutcome(
        status=report.status,
        bundle_id=report.bundle_id,
        sequence_status={s.number: s.status for s in report.sequences},
        action_status={a.action_id: a.status for a in report.action_results()},
        action_order=tuple(a.action_id for a in report.action_results()),
    )


def _upload_tree(store, source: _Path, destination) -> None:
    for path in sorted(source.rglob("*")):
        if path.is_file():
            store.write(destination.join(*path.relative_to(source).parts), path.read_bytes())


@pytest.fixture
def local_build_env(lakehouses, spark):
    """A build environment installed in-process against local Spark."""

    from weaver import RepositoryRef
    from weaver.build_bundle import (
        InstallationEnvironment,
        LakehouseBinding,
        TargetBindings,
        generate_build_bundle,
        install_bundle,
        load_bundle,
    )

    resolver, store = lakehouses.resolver, lakehouses.store

    def install_repo(name: str) -> str:
        _upload_tree(store, BUILD_FIXTURE, resolver.repository(RepositoryRef(name)))
        return name

    def remove_repo(name: str) -> None:
        store.delete(resolver.repository(RepositoryRef(name)), recursive=True)

    def generate(bundle_name: str = "buildtest", *, repository_name: str = "MyRepo", prune: bool = True):
        return generate_build_bundle(
            weaver_lakehouse=lakehouses.weaver,
            repository_name=repository_name,
            targets=TargetBindings(lakehouse=LakehouseBinding(lakehouse=lakehouses.target)),
            output=resolver.build_bundle(bundle_name),
            host=lakehouses.host,
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

    def seed_orphans() -> None:
        tables_root = resolver.tables_root(lakehouses.target).value
        files_root = resolver.files_root(lakehouses.target)
        spark.sql(f"CREATE DATABASE IF NOT EXISTS DWG LOCATION '{tables_root}/DWG'")
        spark.sql("CREATE TABLE DWG.OldTable (x int) USING delta")
        spark.sql("CREATE OR REPLACE VIEW DWG.OldView AS SELECT 1 AS x")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS Legacy LOCATION '{tables_root}/Legacy'")
        spark.sql("CREATE TABLE Legacy.OldThing (x int) USING delta")
        store.write(files_root.join("Raw", "OldFolder", "stale.csv"), b"old\n")
        store.write(files_root.join("Legacy", "Stuff", "f.txt"), b"x\n")

    try:
        yield BuildEnv(
            label="local", host=lakehouses.host, weaver=lakehouses.weaver, target=lakehouses.target,
            resolver=resolver, store=store, generate_spark=spark,
            install_repo=install_repo, remove_repo=remove_repo, generate=generate,
            install=install, query=query, seed_orphans=seed_orphans,
        )
    finally:
        # The Spark catalog is shared across the run; drop what a test created.
        for database in ("DWG", "Raw", "Legacy"):
            spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")


@pytest.fixture
def fabric_build_env(fabric_workspace, fabric_client, fabric_environment_name, livy_session):
    """A build environment installed inside Fabric over Livy.

    Generation runs here on the caller, writing the bundle to OneLake over DFS;
    installation runs *in* the Fabric session, reading that bundle through the
    session-native store and executing DDL on the session's Spark. Both Lakehouses
    are disposable and deleted on teardown, so no catalog cleanup is needed.
    """

    import json

    from weaver import FabricHost, RepositoryRef
    from weaver.build_bundle import (
        LakehouseBinding,
        TargetBindings,
        generate_build_bundle,
    )
    from weaver.fabric import FabricResolver, OneLakeDfsClient, create_lakehouse, delete_item

    created = []
    try:
        weaver_lh = create_lakehouse(fabric_workspace, _disposable_name("weaver"), client=fabric_client)
        created.append(weaver_lh)
        target_lh = create_lakehouse(fabric_workspace, _disposable_name("target"), client=fabric_client)
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

        def install_repo(name: str) -> str:
            _upload_tree(store, BUILD_FIXTURE, resolver.repository(RepositoryRef(name)))
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
                spark=None,  # no catalog on the caller; storage-scoped prune
            )

        def _host_literal() -> str:
            return (
                f"FabricHost(workspace={host.workspace!r}, "
                f"weaver_lakehouse={host.weaver_lakehouse!r}, "
                f"fabric_environment={host.fabric_environment!r})"
            )

        def install(bundle) -> InstallOutcome:
            body = (
                "from weaver import FabricHost, Location\n"
                "from weaver.resolution import resolver_for, store_for\n"
                "from weaver.build_bundle import install_bundle, load_bundle, "
                "InstallationEnvironment\n"
                f"host = {_host_literal()}\n"
                "store = store_for(host)\n"
                "resolver = resolver_for(host)\n"
                "env = InstallationEnvironment(store=store, resolver=resolver, spark=spark)\n"
                f"bundle = load_bundle(Location({bundle.location.value!r}), store=store)\n"
                "report = install_bundle(bundle, environment=env)\n"
                "emit({'status': report.status, 'bundle_id': report.bundle_id, "
                "'sequences': [{'number': s.number, 'status': s.status} for s in report.sequences], "
                "'actions': [{'id': a.action_id, 'status': a.status} for a in report.action_results()]})\n"
            )
            payload = livy_session.run(body).payload
            return InstallOutcome(
                status=payload["status"],
                bundle_id=payload["bundle_id"],
                sequence_status={s["number"]: s["status"] for s in payload["sequences"]},
                action_status={a["id"]: a["status"] for a in payload["actions"]},
                action_order=tuple(a["id"] for a in payload["actions"]),
            )

        def query(sql: str) -> list:
            body = f"emit([row.asDict() for row in spark.sql({sql!r}).collect()])\n"
            return livy_session.run(body).payload

        def seed_orphans() -> None:
            tables_root = resolver.tables_root(target).value
            body = (
                f"spark.sql(\"CREATE DATABASE IF NOT EXISTS DWG LOCATION '{tables_root}/DWG'\")\n"
                "spark.sql('CREATE TABLE DWG.OldTable (x int) USING delta')\n"
                "spark.sql('CREATE OR REPLACE VIEW DWG.OldView AS SELECT 1 AS x')\n"
                f"spark.sql(\"CREATE DATABASE IF NOT EXISTS Legacy LOCATION '{tables_root}/Legacy'\")\n"
                "spark.sql('CREATE TABLE Legacy.OldThing (x int) USING delta')\n"
                "emit(True)\n"
            )
            livy_session.run(body)
            files_root = resolver.files_root(target)
            store.write(files_root.join("Raw", "OldFolder", "stale.csv"), b"old\n")
            store.write(files_root.join("Legacy", "Stuff", "f.txt"), b"x\n")

        yield BuildEnv(
            label="fabric", host=host, weaver=weaver, target=target,
            resolver=resolver, store=store, generate_spark=None,
            install_repo=install_repo, remove_repo=remove_repo, generate=generate,
            install=install, query=query, seed_orphans=seed_orphans,
        )
    finally:
        for item in created:
            try:
                delete_item(item, client=fabric_client)
            except Exception as exc:
                print(f"warning: could not delete {item}: {exc}")


@pytest.fixture
def build_env(request):
    """Select a concrete build environment by indirect parameter."""

    return request.getfixturevalue(request.param)
