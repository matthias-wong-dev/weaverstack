"""Public build operation."""

from __future__ import annotations

import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from ..errors import BuildError, CommandError
from ..locations import Location
from ..sessions.host import inside_fabric_session as _inside_fabric_session
from ..store import FilesystemStore, Store
from ..workspaces import Workspace
from .workspace import operation_workspace


@dataclass(frozen=True)
class BuildFailure:
    """A failed action, with its authored artefact and source path when available."""

    action_id: str
    error_type: str | None
    message: str | None
    artefact: str | None = None
    source_path: str | None = None

    def to_mapping(self) -> dict:
        return {
            "id": self.action_id,
            "type": self.error_type,
            "message": self.message,
            "artefact": self.artefact,
            "source": self.source_path,
        }

    def describe(self) -> str:
        """Describe what failed, its source when available and why."""

        subject = self.artefact or self.action_id
        lines = [f"Error installing {subject}"]
        if self.source_path:
            lines.append(f"Source: {self.source_path}")
        if self.message:
            lines.append(str(self.message))
        return "\n".join(lines)


@dataclass(frozen=True)
class BuildResult:
    source: str
    items: tuple[str, ...]
    bundle_id: str
    installation: bool
    bundle_path: str | None
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
            "installation": self.installation,
            "bundle_path": self.bundle_path,
            "status": self.status,
            "errors": [error.to_mapping() for error in self.errors],
        }


def build(
    source=None,
    *,
    items: str | Sequence[str] | None = None,
    workspace: str | None = None,
    catalogue: str | None = None,
    environment: str | None = None,
    workspace_config: str | Path | None = None,
    bundle_only: bool = False,
    bundle_path: str | Path | None = None,
    session=None,
) -> BuildResult:
    """Build a source.

    ``items`` are the Weaver items to build, each written
    ``Lakehouse/Landing`` or ``Lakehouse/Landing=Lakehouse/Landing_Dev``. Naming
    none builds every item the workspace configuration declares.

    ``catalogue`` names the catalogue Warehouse as ``Warehouse/Weaver``.

    A supplied ``session`` is reused and left open. Otherwise this operation
    creates and closes one.
    """

    if bundle_path is not None and not bundle_only:
        raise CommandError("bundle_path requires bundle_only=True")

    resolved_workspace = operation_workspace(
        "build",
        workspace=workspace,
        catalogue=catalogue,
        environment=environment,
        workspace_config=workspace_config,
        session=session,
    )

    selected = _item_bindings(items, resolved_workspace)
    from ..build_bundle.targets import WarehouseBinding, effective_item_bindings

    workspace_name = getattr(resolved_workspace, "workspace", None)
    bindings = effective_item_bindings(
        selected,
        control_item=resolved_workspace.catalogue_item,
        workspace_name=workspace_name,
    )
    control = WarehouseBinding(
        resolved_workspace.catalogue_item, workspace_name=workspace_name
    )
    source_location, source_store = _repository_source(source, resolved_workspace)

    # Parse and validate the complete request before REST target resolution,
    # Spark startup or Livy work.
    from ..build_bundle.workflow import prepare_repository, validate_build_request
    from ..sessions.host import use_or_create_session

    with prepare_repository(source_location, source_store=source_store) as prepared:
        validate_build_request(prepared.repository, bindings, catalogue_binding=control)
        _preflight(resolved_workspace, bindings, session=session)
        with use_or_create_session(session, workspace=resolved_workspace) as opened:
            # Fabric requires a Lakehouse attachment before Spark starts.
            opened.offer_spark_home(_bound_lakehouses(bindings))
            arguments = dict(
                repository=prepared.repository,
                source_store=prepared.store,
                bindings=bindings,
                catalogue_binding=control,
                bundle_only=bundle_only,
                bundle_path=bundle_path,
                source=source_location.value,
            )
            with opened.task("Build", resolved_workspace.workspace):
                return _run_build(resolved_workspace, session=opened, **arguments)


def _bound_lakehouses(bindings) -> tuple[str, ...]:
    """The physical Lakehouse names this build is bound to, in binding order."""

    from ..declaration.model import LAKEHOUSE

    return tuple(
        binding.target.item.name
        for binding in bindings.entries
        if binding.target.physical_kind == LAKEHOUSE
    )


def _preflight(workspace: Workspace, bindings, *, session) -> None:
    """On desktop, verify all targets in one REST call before opening Spark."""

    if _inside_fabric_session(workspace):
        return
    from ..fabric.preflight import preflight_fabric_targets

    preflight_fabric_targets(
        bindings,
        workspace=workspace.workspace,
        control_item=workspace.catalogue_item,
        environment=workspace.environment,
    )


def _repository_source(source, workspace: Workspace) -> tuple[Location, Store]:
    if source is None:
        if not _inside_fabric_session(workspace):
            source = "."
        else:
            # Notebook Resources are exposed as the process-local working tree.
            source = Path.cwd()
    location = source if isinstance(source, Location) else Location(str(source))
    if location.value.startswith("abfss://"):
        if not _inside_fabric_session(workspace):
            raise CommandError("an abfss source requires a Fabric session")
        from ..fabric.store import FabricStore

        return location, FabricStore()
    return location, FilesystemStore()


def _item_bindings(items, workspace: Workspace):
    """Bind each item to an explicit target or its configured target."""

    from ..build_bundle.targets import ItemBindings, parse_build_item

    if items is None:
        values = [str(item) for item in workspace.configured_items]
    elif isinstance(items, str):
        values = [items]
    else:
        values = list(items)
    if not values:
        raise BuildError(
            "build needs at least one item or a targets mapping in workspace "
            "configuration"
        )
    return ItemBindings(
        tuple(parse_build_item(value, workspace=workspace) for value in values)
    )


def _result_from_item_build(source, bindings, result) -> BuildResult:
    report = result.report
    return BuildResult(
        source=source,
        items=tuple(str(binding.item) for binding in bindings.entries),
        bundle_id=result.bundle_id,
        installation=True,
        bundle_path=None,
        status=report.status,
        errors=tuple(
            BuildFailure(
                action.action_id,
                action.error_type,
                action.error_message,
                artefact=action.resource_node_id,
                source_path=action.source_path,
            )
            for action in report.action_results()
            if action.status == "failed"
        ),
    )


def _run_build(
    workspace,
    *,
    session,
    repository,
    source_store,
    bindings,
    catalogue_binding,
    bundle_only,
    bundle_path,
    source,
) -> BuildResult:
    """Run one build through the supplied Session."""

    from ..build_bundle import (
        build_item_repository,
        catalogue_items_for_build,
        read_build_state,
    )

    # Each state part is its own Step; nesting one here would exceed the
    # Task/Step/Sub-step telemetry hierarchy.
    state = read_build_state(
        bindings,
        required_catalogue_items=catalogue_items_for_build(repository, bindings),
        session=session,
        workspace=workspace,
        shortcuts=repository.shortcuts,
    )
    if bundle_only:
        from ..build_bundle import build_repository_bundle

        output = _bundle_output(bundle_path)
        with session.step("Build bundle"):
            bundle = build_repository_bundle(
                repository,
                bindings=bindings,
                state=state,
                source_store=source_store,
                catalogue_binding=catalogue_binding,
                output=output,
            )
        return BuildResult(
            source=source,
            items=tuple(str(binding.item) for binding in bindings.entries),
            bundle_id=bundle.bundle_id,
            installation=False,
            bundle_path=bundle.location.value,
            status="succeeded",
        )

    with session.step("Build and install"):
        result = build_item_repository(
            repository,
            bindings=bindings,
            state=state,
            session=session,
            workspace=workspace,
            source_store=source_store,
            catalogue_binding=catalogue_binding,
        )
    return _result_from_item_build(source, bindings, result)


def _bundle_output(path: str | Path | None) -> Location:
    """A durable local directory for a bundle-only build."""

    if path is None:
        return Location(tempfile.mkdtemp(prefix="weaver-bundle-"))
    output = Path(path)
    if output.exists() and (not output.is_dir() or any(output.iterdir())):
        raise BuildError(f"bundle path must not exist or must be empty: {output}")
    return Location(str(output))
