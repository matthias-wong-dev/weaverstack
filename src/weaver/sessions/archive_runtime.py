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


def execute_mutation(
    plan,
    payloads,
    session,
    *,
    workers=1,
    journal=None,
    selected=None,
    prerequisites=(),
    invocation_id=None,
    timeout=600,
    build_datetime=None,
):
    """Internal native binding; public Build still uses the format-4 Installer."""
    from dataclasses import replace

    from ..build_bundle import Installer
    from ..build_bundle.executors import default_executors
    from ..build_bundle.executors.base import InstallationContext
    from ..errors import BuildError, InstallError
    from ..mutation.executor import (
        Failed,
        MutationExecutor,
        physical_driver,
        validate_inputs,
    )
    from ..mutation.fragments import validate_fragment

    payloads = validate_inputs(plan, payloads)
    selected = (
        tuple(a.id for _, _, a in plan.actions())
        if selected is None
        else tuple(selected)
    )
    validate_fragment(plan, selected, prerequisites)
    executors = default_executors()
    if any(
        a.executor not in executors and a.executor != "completion_gate"
        for _, _, a in plan.actions()
        if a.id in selected
    ):
        raise BuildError("native archive requires supported physical drivers")
    installer = Installer(session)
    installer._bind(plan)
    resolved = {t.id: installer.resolve_target(t) for t in plan.targets}
    contexts = {
        key: InstallationContext(
            resolver=installer.resolver,
            store=installer.store,
            target=target,
            sql=installer.sql_for(target.bound),
            spark_sql=installer.spark_sql(),
            spark_sql_batch=installer.spark_sql_batch(),
            create_delta_table=installer.delta_table_creator(),
            create_direct_delta_table=installer.direct_delta_table_creator(),
            targets=resolved,
            build_datetime=build_datetime,
        )
        for key, target in resolved.items()
    }
    drivers = {}
    for name, executor in executors.items():
        driver = physical_driver(
            executor,
            contexts,
            lane="native-session",
            required_capabilities=("resolver", "store"),
        )

        def run(request, physical=driver.run):
            try:
                return physical(request)
            except InstallError as error:
                return Failed(str(error))

        drivers[name] = replace(driver, run=run)
    return MutationExecutor(
        drivers,
        workers=workers,
        limits={"native-session": 1},
        journal=journal,
        timeout=timeout,
    ).execute(
        plan,
        payloads,
        selected=selected,
        prerequisites=prerequisites,
        invocation_id=invocation_id,
    )


def run_mutation(bundle, root, spark, output, archive_sha256, *, workers):
    from importlib import import_module

    from ..build_bundle.execution import execution_workspace
    from ..mutation.executor import MutationJournal
    from .mutation_receipts import DurableJournal, checked_request, encode_report, loads

    request = loads((root / "request.json").read_bytes())
    prerequisites = checked_request(bundle.plan, request)
    payloads = {
        a.payload: bundle.store.read(bundle.location.join(*a.payload.split("/")))
        for _, _, a in bundle.plan.actions()
        if a.payload is not None
    }
    fs = import_module("notebookutils").fs
    context = {"archive_sha256": archive_sha256, "request": request}
    sink = DurableJournal(
        lambda path, data: fs.put(path, data.decode("utf-8"), True),
        output,
        bundle.bundle_id,
        request["invocation_id"],
        context=context,
    )
    workspace = execution_workspace(bundle.plan.execution, bundle.plan)
    with ArchiveSession(
        workspace=workspace, spark=spark, direct_delta_workers=workers
    ) as session:
        report = execute_mutation(
            bundle.plan,
            payloads,
            session,
            workers=workers,
            journal=MutationJournal(sink),
            selected=request["selected"],
            prerequisites=prerequisites,
            invocation_id=request["invocation_id"],
            timeout=request["timeout"],
            build_datetime=request["build_datetime"],
        )
    return context | {
        "status": "completed",
        "plan_id": bundle.bundle_id,
        "invocation_id": request["invocation_id"],
        "report": encode_report(report),
    }


def run_bundle(root, spark, output, archive_sha256, *, workers):
    import json
    import time
    from dataclasses import replace
    from importlib import import_module

    from ..build_bundle import Installer, load_bundle
    from ..build_bundle.bundle import validate_bundle
    from ..build_bundle.execution import execution_workspace
    from ..build_bundle.installer import select_install_batches
    from ..locations import Location
    from ..store import FilesystemStore

    bundle = load_bundle(
        Location(str(root / "bundle")), store=FilesystemStore(), allow_mutation=True
    )
    from ..mutation.models import MutationPlan

    if isinstance(bundle.plan, MutationPlan):
        return run_mutation(
            bundle, root, spark, output, archive_sha256, workers=workers
        )
    request_path = root / "request.json"
    request = json.loads(request_path.read_text()) if request_path.exists() else None
    build_datetime = None
    if request is not None:
        validate_bundle(bundle.location, bundle.plan, store=bundle.store)
        selected = select_install_batches(
            bundle.plan,
            sequence_number=request["sequence_number"],
            batch_ids=request["batch_ids"],
        )
        build_datetime = request["build_datetime"]
        bundle = replace(bundle, plan=selected)
    workspace = execution_workspace(bundle.plan.execution, bundle.plan)
    fs = import_module("notebookutils").fs

    def journal(report):
        result = {
            "status": "running",
            "archive_sha256": archive_sha256,
            "request": request,
            "report": report.to_mapping(),
        }
        fs.put(output, json.dumps(result, separators=(",", ":"), allow_nan=False), True)

    started = time.monotonic()
    with ArchiveSession(
        workspace=workspace, spark=spark, direct_delta_workers=workers
    ) as session:
        with session.task("Install", bundle.bundle_id):
            report = Installer(session).install(
                bundle, on_sequence=journal, build_datetime=build_datetime
            )
        profiles = list(getattr(session, "created_profiles", ()))
        events = [event.to_mapping() for event in session.telemetry.events()]
    return {
        "status": "completed",
        "request": request,
        "archive_sha256": archive_sha256,
        "report": report.to_mapping(),
        "install_seconds": time.monotonic() - started,
        "direct_delta_tables": profiles,
        "events": events,
    }
