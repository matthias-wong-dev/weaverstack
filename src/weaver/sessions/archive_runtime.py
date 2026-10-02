"""Native storage used by a verified archived installation."""

from __future__ import annotations

import threading

from ..fabric.onelake import OneLakeDfsClient
from .direct_delta import create_bound_delta_table, run_direct_delta_actions
from .notebook import NotebookSession


class ArchiveDfs(OneLakeDfsClient):
    """Keep native ABFSS destinations on the qualified HTTPS publication route."""

    def _url(self, location, query=None):
        return super()._url(self._publication_location(location), query)


class ArchiveStore:
    """Use NotebookUtils for text and the native DFS client for arbitrary bytes."""

    def __init__(self, native, dfs):
        self.native = native
        self.dfs = dfs

    def __getattr__(self, name):
        return getattr(self.native, name)

    def read(self, location):
        return self.dfs.read(location)

    def write(self, location, data):
        try:
            data.decode("utf-8")
        except UnicodeDecodeError:
            return self.dfs.write(location, data)
        return self.native.write(location, data)


class ArchiveSession(NotebookSession):
    """Retain the caller's pinned Delta writer inside the attached runtime."""

    def __init__(self, *, token=None, direct_delta_workers=16, **kwargs):
        super().__init__(**kwargs)
        if type(direct_delta_workers) is not int or not 1 <= direct_delta_workers <= 16:
            raise ValueError("direct Delta workers must be an integer from 1 to 16")
        self.direct_delta_workers = direct_delta_workers
        if token is None:
            from importlib import import_module

            def native_token():
                return import_module("notebookutils").credentials.getToken("storage")

            token = native_token
        self._delta_store = ArchiveDfs(token=token, telemetry=self.telemetry)
        self._delta_lock = threading.Lock()
        self.created_profiles = []
        self._archive_stores = {}

    def store(self, workspace=None):
        scope = self.scope(workspace)
        with self._delta_lock:
            if id(scope) not in self._archive_stores:
                self._archive_stores[id(scope)] = ArchiveStore(
                    scope.store, self._delta_store
                )
            return self._archive_stores[id(scope)]

    def create_direct_delta_table(
        self,
        qualified_name,
        columns,
        *,
        identity_column=None,
        protocol_minima=None,
        workspace=None,
    ):
        with self.telemetry.timing("onelake.delta_table"):
            allocated = create_bound_delta_table(
                qualified_name=qualified_name,
                columns=columns,
                identity_column=identity_column,
                protocol_minima=protocol_minima,
                resolver=self.resolver(workspace),
                store=self._delta_store,
                publish=self._delta_store.rename_directory,
                resolver_lock=self._delta_lock,
            )
            with self._delta_lock:
                self.created_profiles.append(qualified_name)
            return allocated

    def create_direct_delta_table_actions(self, actions, *, workspace=None):
        self.scope(workspace)
        context = self.telemetry.capture_context()

        def create(qualified, columns, *, identity_column, workspace, **options):
            with self.telemetry.use_context(context):
                return self.create_direct_delta_table(
                    qualified,
                    columns,
                    identity_column=identity_column,
                    workspace=workspace,
                    **options,
                )

        return run_direct_delta_actions(
            create, actions, workspace=workspace, max_workers=self.direct_delta_workers
        )


#: Concurrent actions by constrained capability. A Warehouse connection runs one
#: statement at a time, and Fabric Warehouse snapshot isolation aborts concurrent
#: DDL that contends on catalogue metadata, so each Warehouse has one lane.
WAREHOUSE_LANES = 1
SPARK_LANES = 8
ONELAKE_LANES = 16
SHORTCUT_API_LANES = 4
WORKERS = 32


def resource_limits(plan) -> dict[str, int]:
    """Capacity for every resource the plan's actions occupy."""

    from ..build_bundle.stages import SPARK

    limits = {}
    for _, _, action in plan.actions():
        for key in action.resources:
            prefix = key.split(":", 1)[0]
            limits[key] = {
                "warehouse": WAREHOUSE_LANES,
                SPARK: SPARK_LANES,
                "onelake": ONELAKE_LANES,
                "shortcuts": SHORTCUT_API_LANES,
            }.get(prefix, 1)
    return limits


def execute_mutation(
    plan,
    payloads,
    session,
    *,
    workers=WORKERS,
    invocation_id=None,
    timeout=600,
    build_datetime=None,
    executors=None,
):
    """Bind physical capabilities and execute one complete MutationPlan.

    Resource limits, not ``workers``, bound concurrency on each capability.
    """
    from dataclasses import replace

    from ..build_bundle.executors import default_executors
    from ..build_bundle.executors.base import InstallationContext
    from ..build_bundle.executors.sql_endpoint_refresh import endpoint_refresh_drivers
    from ..build_bundle.installer import MutationBindings
    from ..errors import BuildError, WeaverError
    from ..mutation.executor import (
        Failed,
        MutationExecutor,
        physical_driver,
        validate_inputs,
    )

    payloads = validate_inputs(plan, payloads)
    if build_datetime is None:
        from datetime import datetime, timezone

        build_datetime = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
    executors = default_executors() if executors is None else executors
    contracts = {contract.executor for contract in plan.driver_contracts}
    if any(
        a.executor not in executors
        and a.executor not in contracts
        and a.executor != "completion_gate"
        for _, _, a in plan.actions()
    ):
        raise BuildError("mutation execution requires supported physical drivers")
    bindings = MutationBindings(session)
    bindings._bind(plan)
    resolved = {t.id: bindings.resolve_target(t) for t in plan.targets}
    contexts = {
        key: InstallationContext(
            resolver=bindings.resolver,
            store=bindings.store,
            target=target,
            sql=bindings.sql_for(target.bound),
            spark_sql=bindings.spark_sql(),
            spark_sql_batch=bindings.spark_sql_batch(),
            create_delta_table=bindings.delta_table_creator(),
            create_direct_delta_table=bindings.direct_delta_table_creator(),
            targets=resolved,
            build_datetime=build_datetime,
        )
        for key, target in resolved.items()
    }

    def failed(error):
        return Failed(f"{type(error).__name__}: {error}")

    drivers = {}
    for name, executor in executors.items():
        driver = physical_driver(
            executor, contexts, required_capabilities=("resolver", "store")
        )

        def run(request, physical=driver.run):
            try:
                return physical(request)
            except (WeaverError, ValueError) as error:
                return failed(error)

        drivers[name] = replace(driver, run=run)
    drivers.update(endpoint_refresh_drivers(contexts, failed=failed))
    return MutationExecutor(
        drivers, workers=workers, limits=resource_limits(plan), timeout=timeout
    ).execute(plan, payloads, invocation_id=invocation_id)


def run_mutation(root, spark, output, archive_sha256, *, workers):
    from ..build_bundle.execution import execution_workspace
    from ..locations import Location
    from ..mutation.bundle import load_bundle
    from ..store import FilesystemStore
    from .mutation_report import encode_report, loads

    request = loads((root / "request.json").read_bytes())
    bundle = load_bundle(Location(str(root / "bundle")), store=FilesystemStore())
    plan = bundle.plan
    if request["plan_id"] != plan.bundle_id:
        raise ValueError("mutation request plan differs")
    payloads = {
        a.payload: bundle.store.read(bundle.location.join(*a.payload.split("/")))
        for _, _, a in plan.actions()
        if a.payload is not None
    }
    workspace = execution_workspace(plan.execution, plan)
    with ArchiveSession(
        workspace=workspace, spark=spark, direct_delta_workers=workers
    ) as session:
        report = execute_mutation(
            plan,
            payloads,
            session,
            invocation_id=request["invocation_id"],
            timeout=request["timeout"],
            build_datetime=request["build_datetime"],
        )
    return {
        "status": "completed",
        "archive_sha256": archive_sha256,
        "request": request,
        "plan_id": plan.bundle_id,
        "invocation_id": request["invocation_id"],
        "report": encode_report(report),
    }
