"""Source-neutral public build and target-oriented wipe operations.

This module is the adaptation boundary between the small notebook-facing API
and Weaver's typed planning and execution machinery.  It deliberately keeps
all optional platform imports inside the path that needs them so importing
``weaver`` remains safe without Spark, Fabric credentials, or the desktop CLI.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path
import tempfile
import uuid
from typing import Iterable, Mapping, Sequence

from .errors import BuildError, CommandError, WeaverError
from .locations import Location
from .store import FilesystemStore, Store
from .targets import (
    ItemRef,
    WarehouseTarget,
    parse_physical_target,
    physical_item,
    physical_kind,
)
from .workspaces import FabricWorkspace, LocalWorkspace, Workspace


@dataclass(frozen=True)
class BuildFailure:
    action_id: str
    error_type: str | None
    message: str | None

    def to_mapping(self) -> dict:
        return {
            "id": self.action_id,
            "type": self.error_type,
            "message": self.message,
        }


@dataclass(frozen=True)
class BuildResult:
    source: str
    items: tuple[str, ...]
    bundle_id: str
    archive: str | None
    status: str
    errors: tuple[BuildFailure, ...] = ()

    @property
    def succeeded(self) -> bool:
        return self.status == "succeeded"

    def to_mapping(self) -> dict:
        return {
            "source": self.source,
            "items": list(self.items),
            "bundle_id": self.bundle_id,
            "archive": self.archive,
            "status": self.status,
            "errors": [error.to_mapping() for error in self.errors],
        }


@dataclass(frozen=True)
class WipeTarget:
    item_type: str
    item: ItemRef

    @classmethod
    def parse(cls, text: str) -> "WipeTarget":
        target = parse_physical_target(
            text, what="wipe target", error=CommandError
        )
        return cls(item_type=physical_kind(target), item=physical_item(target))

    @property
    def physical_name(self) -> str:
        return self.item.name

    def __str__(self) -> str:
        return f"{self.item_type}/{self.item}"


def _unbind_target_names(targets: Iterable[str]) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Parse unbind selection through the same typed grammar used by wipe."""

    parsed = tuple(WipeTarget.parse(target) for target in targets)
    return (
        tuple(target.physical_name for target in parsed if target.item_type == "Lakehouse"),
        tuple(target.physical_name for target in parsed if target.item_type == "Warehouse"),
    )


@dataclass(frozen=True)
class WipeReport:
    target: str
    location: Location
    removed: tuple[str, ...]
    dry_run: bool = False

    @property
    def count(self) -> int:
        return len(self.removed)

    def to_mapping(self) -> dict:
        return {
            "target": self.target,
            "location": self.location.value,
            "removed": list(self.removed),
            "dry_run": self.dry_run,
        }


@dataclass(frozen=True)
class WipeResult:
    workspace: str
    reports: tuple[WipeReport, ...]
    unbound: Mapping | None = None
    dry_run: bool = False

    @property
    def count(self) -> int:
        return sum(report.count for report in self.reports)

    def to_mapping(self) -> dict:
        return {
            "workspace": self.workspace,
            "reports": [report.to_mapping() for report in self.reports],
            "unbound": dict(self.unbound) if self.unbound is not None else None,
            "dry_run": self.dry_run,
        }


def build(
    source=None,
    *,
    bind: str | Sequence[str] | None = None,
    workspace: str | Path | Workspace | None = None,
    workspace_config: str | Path | None = None,
    bundle: str | None = None,
) -> BuildResult:
    """Build an authored repository using simple notebook-facing values.

    ``workspace=None`` means the current Fabric session.  A typed ``Workspace``
    remains accepted as an advanced/testing seam and is what the CLI supplies
    after applying its explicit ``--workspace-type`` flags.
    """

    resolved_workspace = _operation_workspace(
        workspace=workspace, workspace_config=workspace_config
    )
    resolved_workspace = _with_inferred_control_lakehouse(resolved_workspace)
    if not resolved_workspace.weaver_lakehouse:
        raise CommandError(
            "build needs a Weaver control Lakehouse in workspace configuration "
            "or as the notebook's attached default Lakehouse"
        )

    selected = _item_bindings(bind, resolved_workspace)
    from .build_bundle.targets import LakehouseBinding, effective_item_bindings

    bindings = effective_item_bindings(
        selected, weaver_lakehouse=resolved_workspace.weaver_lakehouse
    )
    control = LakehouseBinding(ItemRef(resolved_workspace.weaver_lakehouse))
    source_location, source_store = _repository_source(source, resolved_workspace)

    # This complete parse and pure request validation is deliberately above all
    # control-plane creation, Spark start, REST item resolution, and Livy work.
    from .build_bundle.workflow import prepare_repository, validate_build_request

    with prepare_repository(
        source_location, source_store=source_store
    ) as prepared:
        validate_build_request(
            prepared.repository, bindings, control_lakehouse=control
        )
        if isinstance(resolved_workspace, LocalWorkspace):
            return _build_local(
                resolved_workspace,
                repository=prepared.repository,
                source_store=prepared.store,
                bindings=bindings,
                control_lakehouse=control,
                bundle_name=bundle,
                source=source_location.value,
            )
        if _inside_fabric_session(resolved_workspace):
            return _build_native_fabric(
                resolved_workspace,
                repository=prepared.repository,
                source_store=prepared.store,
                bindings=bindings,
                control_lakehouse=control,
                bundle_name=bundle,
                source=source_location.value,
            )
        return _build_desktop_fabric(
            resolved_workspace,
            repository=prepared.repository,
            source_store=prepared.store,
            bindings=bindings,
            control_lakehouse=control,
            bundle_name=bundle,
            source=source_location.value,
        )


def wipe(
    targets: str | Iterable[str],
    *,
    workspace: str | Path | Workspace | None = None,
    workspace_config: str | Path | None = None,
    unbind_from: str | None = None,
    dry_run: bool = False,
) -> WipeResult:
    """Empty one or more whole Lakehouse or Warehouse items."""

    values = (targets,) if isinstance(targets, str) else tuple(targets)
    parsed = tuple(WipeTarget.parse(value) for value in values)
    if not parsed:
        raise CommandError("wipe needs at least one target")
    resolved_workspace = _operation_workspace(
        workspace=workspace, workspace_config=workspace_config
    )
    if isinstance(resolved_workspace, LocalWorkspace) and any(
        target.item_type == "Warehouse" for target in parsed
    ):
        raise CommandError(
            "Warehouse targets require a Fabric Workspace; the local emulator has no SQL"
        )

    storage_targets = tuple(t for t in parsed if t.item_type == "Lakehouse")
    store = _operation_store(resolved_workspace) if storage_targets else None
    reports: list[WipeReport] = []
    if not dry_run:
        _drop_local_catalogue(resolved_workspace, storage_targets)
    for target in parsed:
        reports.extend(
            _wipe_one(target, resolved_workspace, store=store, dry_run=dry_run)
        )

    unbound = None
    control = unbind_from or resolved_workspace.weaver_lakehouse
    whole_lakehouses = {
        target.physical_name
        for target in parsed
        if target.item_type == "Lakehouse"
    }
    if not dry_run and control and control not in whole_lakehouses:
        catalogue_workspace = replace(
            resolved_workspace, weaver_lakehouse=ItemRef.parse(control).name
        )
        unbound = _unbind_physical_targets(catalogue_workspace, parsed)

    return WipeResult(
        workspace=str(resolved_workspace.workspace),
        reports=tuple(reports),
        unbound=unbound,
        dry_run=dry_run,
    )


def _operation_workspace(*, workspace, workspace_config) -> Workspace:
    if isinstance(workspace, Workspace):
        if workspace_config is not None:
            raise CommandError(
                "workspace_config cannot be combined with an already resolved Workspace"
            )
        return workspace
    if workspace is None and workspace_config is None:
        return _current_fabric_workspace()
    from .config import resolve_workspace

    return resolve_workspace(workspace=workspace, workspace_config=workspace_config)


def _current_fabric_workspace() -> FabricWorkspace:
    try:
        from notebookutils import runtime
    except ImportError as exc:
        raise CommandError(
            "give workspace or workspace_config outside a Fabric notebook"
        ) from exc
    context = runtime.context
    if callable(context):
        context = context()
    if not isinstance(context, Mapping):
        raise CommandError("Fabric runtime context is not a mapping")
    name = context.get("currentWorkspaceName")
    if not name:
        raise CommandError("Fabric runtime context carries no current workspace")
    return FabricWorkspace(workspace=str(name))


def _with_inferred_control_lakehouse(workspace: Workspace) -> Workspace:
    if workspace.weaver_lakehouse or not isinstance(workspace, FabricWorkspace):
        return workspace
    if not _inside_fabric_session(workspace):
        return workspace
    from .lakehouse import default_lakehouse

    spark = _active_spark()
    return replace(workspace, weaver_lakehouse=default_lakehouse(spark).name)


def _active_spark():
    try:
        from importlib import import_module

        SparkSession = import_module("pyspark.sql").SparkSession
    except ImportError as exc:
        raise CommandError("this operation needs an active Spark session") from exc
    spark = SparkSession.getActiveSession()
    if spark is None:
        raise CommandError("this operation needs an active Spark session")
    return spark


def _inside_fabric_session(workspace: FabricWorkspace) -> bool:
    try:
        from notebookutils import runtime
    except ImportError:
        return False
    context = runtime.context
    if callable(context):
        context = context()
    if not isinstance(context, Mapping):
        return False
    return context.get("currentWorkspaceName") == workspace.workspace


def _repository_source(source, workspace: Workspace) -> tuple[Location, Store]:
    if source is None:
        if not isinstance(workspace, FabricWorkspace) or not _inside_fabric_session(workspace):
            source = "."
        else:
            # Fabric exposes built-in Notebook Resources as the notebook's
            # process-local working tree.  No OneLake adapter is involved.
            source = Path.cwd()
    location = source if isinstance(source, Location) else Location(str(source))
    if location.value.startswith("abfss://"):
        if not isinstance(workspace, FabricWorkspace) or not _inside_fabric_session(workspace):
            raise CommandError(
                "an abfss repository source can be read only inside a Fabric session"
            )
        from .fabric.store import FabricStore

        return location, FabricStore()
    return location, FilesystemStore()


def _item_bindings(bind, workspace: Workspace):
    from .build_bundle.targets import ItemBindings, parse_item_binding

    if bind is None:
        values = [f"Lakehouse/{name}" for name in workspace.lakehouses]
        values += [f"Warehouse/{name}" for name in workspace.warehouses]
    elif isinstance(bind, str):
        values = [bind]
    else:
        values = list(bind)
    if not values:
        raise BuildError(
            "build needs bind values or configured Lakehouse/Warehouse targets"
        )
    return ItemBindings(
        tuple(parse_item_binding(value, workspace=workspace) for value in values)
    )


def _archive_location(resolver, bundle_name: str | None):
    if bundle_name is None:
        return None
    from .build_bundle.workflow import timestamped_archive_name

    name = bundle_name or timestamped_archive_name()
    if not name.endswith(".weaver.zip"):
        name += ".weaver.zip"
    return resolver.build_bundle(name)


def _result_from_item_build(source, bindings, result) -> BuildResult:
    report = result.report
    return BuildResult(
        source=source,
        items=tuple(str(binding.item) for binding in bindings.entries),
        bundle_id=result.bundle_id,
        archive=result.archive.value if result.archive else None,
        status=report.status,
        errors=tuple(
            BuildFailure(
                action.action_id, action.error_type, action.error_message
            )
            for action in report.action_results()
            if action.status == "failed"
        ),
    )


def _build_in_process(
    workspace,
    *,
    spark,
    store,
    repository,
    source_store,
    bindings,
    control_lakehouse,
    bundle_name,
    source,
) -> BuildResult:
    from .build_bundle import (
        InstallationEnvironment,
        build_item_repository,
        catalogue_items_for_build,
        read_build_state,
    )
    from .catalogue.state import reconcile_catalogue_state
    from .resolution import resolver_for

    resolver = resolver_for(workspace)
    environment = InstallationEnvironment(
        store=store, resolver=resolver, spark=spark, workspace=workspace
    )
    state = read_build_state(
        bindings,
        required_catalogue_items=catalogue_items_for_build(repository, bindings),
        environment=environment,
    )
    result = build_item_repository(
        repository,
        bindings=bindings,
        target_inventories=state.target_inventories,
        reconciliation=reconcile_catalogue_state(
            state.catalogue, inventories=state.target_inventories
        ),
        environment=environment,
        source_store=source_store,
        control_lakehouse=control_lakehouse,
        archive=_archive_location(resolver, bundle_name),
    )
    return _result_from_item_build(source, bindings, result)


def _build_local(workspace, **kwargs) -> BuildResult:
    warehouse_items = [
        str(binding.item)
        for binding in kwargs["bindings"].entries
        if binding.item.item_type == "Warehouse"
    ]
    if warehouse_items:
        raise CommandError(
            "local Workspace builds cannot target Warehouses: "
            + ", ".join(warehouse_items)
        )
    from .spark import local_delta_session

    with local_delta_session(workspace) as session:
        return _build_in_process(
            workspace, spark=session, store=FilesystemStore(), **kwargs
        )


def _build_native_fabric(workspace, **kwargs) -> BuildResult:
    from .resolution import store_for

    return _build_in_process(
        workspace, spark=_active_spark(), store=store_for(workspace), **kwargs
    )


def _build_desktop_fabric(
    workspace,
    *,
    repository,
    source_store,
    bindings,
    control_lakehouse,
    bundle_name,
    source,
) -> BuildResult:
    if not workspace.environment:
        raise CommandError(
            "Fabric build requires an Environment in workspace configuration"
        )
    from .build_bundle import (
        BuildState,
        catalogue_items_for_build,
        generate_item_build_bundle,
        persist_bundle_archive,
    )
    from .catalogue.state import reconcile_catalogue_state
    from .fabric import LivySession, OneLakeDfsClient
    from .resolution import resolver_for

    resolver = resolver_for(workspace)
    transport_store = OneLakeDfsClient()
    binding_texts = [_binding_text(binding) for binding in bindings.entries]
    required_items = [
        str(item) for item in catalogue_items_for_build(repository, bindings)
    ]
    workspace_literal = (
        f"FabricWorkspace(workspace={workspace.workspace!r}, "
        f"weaver_lakehouse={workspace.weaver_lakehouse!r}, "
        f"environment={workspace.environment!r})"
    )
    state_body = (
        "from weaver.workspaces import FabricWorkspace\n"
        "from weaver.declaration.model import WeaverItemId\n"
        "from weaver.build_bundle import (InstallationEnvironment, ItemBindings, "
        "parse_item_binding, read_build_state)\n"
        "from weaver.resolution import resolver_for, store_for\n"
        f"workspace = {workspace_literal}\n"
        "store = store_for(workspace)\n"
        "resolver = resolver_for(workspace)\n"
        f"bindings = ItemBindings(tuple(parse_item_binding(text) for text in {binding_texts!r}))\n"
        "environment = InstallationEnvironment("
        "store=store, resolver=resolver, spark=spark, workspace=workspace)\n"
        f"items = tuple(WeaverItemId.parse(value) for value in {required_items!r})\n"
        "emit(read_build_state(bindings, required_catalogue_items=items, "
        "environment=environment).to_mapping())\n"
    )
    execution_id = uuid.uuid4().hex
    execution = resolver.cli_execution(execution_id)
    remote_archive = resolver.cli_bundle(execution_id)
    retained_archive = _archive_location(resolver, bundle_name)
    bundle = None
    report = None
    with LivySession.for_workspace(workspace) as session:
        state = BuildState.from_mapping(session.run(state_body).payload)
        reconciliation = reconcile_catalogue_state(
            state.catalogue, inventories=state.target_inventories
        )
        try:
            with tempfile.TemporaryDirectory(prefix="weaver-cli-build-") as temporary:
                root = Path(temporary)
                bundle = generate_item_build_bundle(
                    repository,
                    bindings=bindings,
                    output=Location((root / "bundle").as_posix()),
                    store=source_store,
                    target_inventories=state.target_inventories,
                    catalogue=reconciliation.catalogue,
                    stale_claims=reconciliation.stale_claims,
                    control_lakehouse=control_lakehouse,
                )
                local_archive = Location((root / "install.weaver.zip").as_posix())
                persist_bundle_archive(bundle, local_archive, store=FilesystemStore())
                archive_bytes = FilesystemStore().read(local_archive)
                transport_store.make_directory(resolver.cli_root)
                transport_store.make_directory(execution)
                transport_store.write(remote_archive, archive_bytes)
                install_body = (
                    "from weaver.workspaces import FabricWorkspace\n"
                    "from weaver.build_bundle import InstallationEnvironment, "
                    "install_bundle_archive\n"
                    "from weaver.resolution import resolver_for, store_for\n"
                    f"workspace = {workspace_literal}\n"
                    "store = store_for(workspace)\n"
                    "resolver = resolver_for(workspace)\n"
                    "environment = InstallationEnvironment("
                    "store=store, resolver=resolver, spark=spark, workspace=workspace)\n"
                    f"archive = resolver.cli_bundle({execution_id!r})\n"
                    "report = install_bundle_archive(archive, archive_store=store, "
                    "environment=environment)\n"
                    "emit(report.to_mapping())\n"
                )
                report = session.run(install_body).payload
                if retained_archive is not None:
                    transport_store.make_directory(resolver.build_bundles_root)
                    transport_store.write(retained_archive, archive_bytes)
        finally:
            try:
                transport_store.delete(execution, recursive=True)
            except WeaverError:
                pass
    assert bundle is not None and report is not None
    return BuildResult(
        source=source,
        items=tuple(str(binding.item) for binding in bindings.entries),
        bundle_id=bundle.bundle_id,
        archive=retained_archive.value if retained_archive else None,
        status=report["status"],
        errors=tuple(
            BuildFailure(
                action["action_id"],
                action.get("error_type"),
                action.get("error_message"),
            )
            for sequence in report.get("sequences", ())
            for action in sequence.get("actions", ())
            if action.get("status") == "failed"
        ),
    )


def _binding_text(binding) -> str:
    target = binding.target
    if hasattr(target, "lakehouse"):
        physical = f"Lakehouse/{target.lakehouse.name}"
    else:
        physical = f"Warehouse/{target.warehouse.name}"
    return f"{physical}={binding.item}"


def _operation_store(workspace: Workspace) -> Store:
    if isinstance(workspace, LocalWorkspace):
        return FilesystemStore()
    if _inside_fabric_session(workspace):
        from .fabric.store import FabricStore

        return FabricStore()
    from .fabric import OneLakeDfsClient

    return OneLakeDfsClient()


def _wipe_one(target: WipeTarget, workspace, *, store, dry_run):
    from .physical_wipe import wipe_lakehouse, wipe_sql_target

    if target.item_type == "Lakehouse":
        low = wipe_lakehouse(target.item, workspace, store=store, dry_run=dry_run)
        return tuple(
            WipeReport(
                target=str(target),
                location=report.location,
                removed=report.removed,
                dry_run=dry_run,
            )
            for report in low
        )

    report = WipeReport(
        target=str(target),
        location=Location(f"warehouse://{target.item.name}"),
        removed=("all user-created SQL objects",),
        dry_run=dry_run,
    )
    if dry_run:
        return (report,)
    warehouse = WarehouseTarget(target.item)
    if _inside_fabric_session(workspace):
        wipe_sql_target(warehouse, workspace)
    else:
        from .fabric import desktop_sql_executor

        with desktop_sql_executor(warehouse, workspace) as sql:
            wipe_sql_target(warehouse, workspace, sql=sql)
    return (report,)


def _drop_local_catalogue(workspace, targets: Sequence[WipeTarget]) -> None:
    if not isinstance(workspace, LocalWorkspace):
        return
    lakehouses = {target.item for target in targets}
    if not lakehouses:
        return
    from .resolution import resolver_for
    from .spark import drop_local_destination_catalogue, local_delta_session

    resolver = resolver_for(workspace)
    with local_delta_session(workspace) as session:
        for lakehouse in lakehouses:
            drop_local_destination_catalogue(
                session, resolver.spark_destination(lakehouse)
            )


def _unbind_physical_targets(workspace: Workspace, targets: Sequence[WipeTarget]):
    lakehouses = sorted(
        {target.physical_name for target in targets if target.item_type == "Lakehouse"}
    )
    warehouses = sorted(
        {target.physical_name for target in targets if target.item_type == "Warehouse"}
    )
    if isinstance(workspace, LocalWorkspace) or _inside_fabric_session(workspace):
        from .resolution import resolver_for
        from .spark import SparkCatalogue
        from .unbind import unbind_targets

        if isinstance(workspace, LocalWorkspace):
            from .spark import local_delta_session

            with local_delta_session(workspace) as session:
                catalogue = SparkCatalogue(
                    session,
                    resolver_for(workspace).spark_destination(
                        ItemRef(workspace.weaver_lakehouse)
                    ),
                )
                return unbind_targets(
                    catalogue, lakehouses=lakehouses, warehouses=warehouses
                ).to_mapping()
        catalogue = SparkCatalogue(
            _active_spark(),
            resolver_for(workspace).spark_destination(
                ItemRef(workspace.weaver_lakehouse)
            ),
        )
        return unbind_targets(
            catalogue, lakehouses=lakehouses, warehouses=warehouses
        ).to_mapping()

    if not workspace.environment:
        raise CommandError(
            "Fabric catalogue unbind requires an Environment in workspace configuration"
        )
    from .fabric import LivySession

    body = (
        "from weaver.workspaces import FabricWorkspace\n"
        "from weaver.targets import ItemRef\n"
        "from weaver.unbind import unbind_targets\n"
        "from weaver.resolution import resolver_for\n"
        "from weaver.spark import SparkCatalogue\n"
        f"workspace = FabricWorkspace(workspace={workspace.workspace!r}, "
        f"environment={workspace.environment!r}, "
        f"weaver_lakehouse={workspace.weaver_lakehouse!r})\n"
        "catalogue = SparkCatalogue(spark, resolver_for(workspace).spark_destination("
        "ItemRef(workspace.weaver_lakehouse)))\n"
        f"emit(unbind_targets(catalogue, lakehouses={tuple(lakehouses)!r}, "
        f"warehouses={tuple(warehouses)!r}).to_mapping())\n"
    )
    with LivySession.for_workspace(workspace) as session:
        return session.run(body).payload
