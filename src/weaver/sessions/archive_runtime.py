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

    bundle = load_bundle(Location(str(root / "bundle")), store=FilesystemStore())
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
