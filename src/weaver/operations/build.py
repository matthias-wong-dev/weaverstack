"""Source-neutral public build and target-oriented wipe operations.

Optional platform imports remain inside the operation paths that require them,
so importing ``weaver`` does not require Spark, Fabric credentials, or the CLI.
"""

from __future__ import annotations

import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from ..errors import BuildError, CommandError
from ..locations import Location

# Position — whether this process is inside the Fabric session it addresses, and
# what Spark it would use if it were — is the first half of "where am I
# running", so a Session owns it. Kept under the names this module already uses.
from ..sessions.host import inside_fabric_session as _inside_fabric_session
from ..store import FilesystemStore, Store
from ..workspaces import Workspace
from .workspace import operation_workspace


@dataclass(frozen=True)
class BuildFailure:
    """One action that failed, described the way a developer needs to read it.

    ``artefact`` is the Weaver thing that failed and ``source_path`` the
    repository file to open. Both are carried from the build rather than
    recovered from ``action_id``, which by this point spells a slug.
    """

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
        """The failure as the plan's error shape: what, where, then why.

        The Weaver operation leads and the infrastructure comes last: TDS
        raising something is how the syntax error was found, not what it was.
        """

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
    bind: str | Sequence[str] | None = None,
    workspace: str | None = None,
    catalogue: str | None = None,
    environment: str | None = None,
    workspace_config: str | Path | None = None,
    bundle_only: bool = False,
    bundle_path: str | Path | None = None,
    session=None,
) -> BuildResult:
    """Build an authored repository.

    Every value is a *name*: ``workspace``, ``catalogue`` and ``environment``
    are strings, resolved the same way each operation resolves them — an
    explicit argument, then workspace configuration, then the Session's own
    context, then, inside a Fabric notebook, what the notebook is attached to.
    Anything still unresolved is an error stated in one sentence.

    ``catalogue`` names where the Weaver catalogue lives, typed:
    ``Warehouse/Weaver``. Weaver owns the ``_`` schema of that Warehouse and
    nothing else in it, so it may be one of your own.

    ``session`` is a Session to run in, and is where an already-resolved
    ``Workspace`` travels. Supplied, its resources are reused and it is left
    open; omitted, this operation creates and closes one.
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

    selected = _item_bindings(bind, resolved_workspace)
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

    # This complete parse and pure request validation is deliberately above all
    # control-plane creation, Spark start, REST item resolution, and Livy work.
    from ..build_bundle.workflow import prepare_repository, validate_build_request
    from ..sessions.host import use_or_create_session

    with prepare_repository(source_location, source_store=source_store) as prepared:
        validate_build_request(prepared.repository, bindings, catalogue_binding=control)
        _preflight(resolved_workspace, bindings, session=session)
        with use_or_create_session(session, workspace=resolved_workspace) as opened:
            # Fabric attaches a Spark session to a Lakehouse, so a host that
            # crosses needs one of the Lakehouses this build is actually for.
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
    """Prove the workspace can host this build, before anything expensive opens.

    Only from a desktop: inside Fabric the items are already resolvable and one
    workspace listing would be a REST round trip for what the session can see.
    Every item this build needs is proved from that one listing, so a missing
    target costs a call rather than a Livy session and a Spark traceback about
    a catalogue.
    """

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
            # Fabric exposes built-in Notebook Resources as the notebook's
            # process-local working tree.  No OneLake adapter is involved.
            source = Path.cwd()
    location = source if isinstance(source, Location) else Location(str(source))
    if location.value.startswith("abfss://"):
        if not _inside_fabric_session(workspace):
            raise CommandError(
                "an abfss repository source can be read only inside a Fabric session"
            )
        from ..fabric.store import FabricStore

        return location, FabricStore()
    return location, FilesystemStore()


def _item_bindings(bind, workspace: Workspace):
    from ..build_bundle.targets import ItemBindings, parse_item_binding

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
    """One build, wherever this process happens to be.

    .. code-block:: text

        read the build state    → through Session capabilities, per part
        Builder                 → the bundle this estate needs
        Installer               → each action to the capability it needs

    The Session answers which Spark, which store and which resolver, so a
    notebook build and a desktop build take this path unchanged. What differs
    between them is what surrounds it: a desktop proves its items exist over
    REST first, which :func:`build` does before opening anything.
    """

    from ..build_bundle import (
        build_item_repository,
        catalogue_items_for_build,
        read_build_state,
    )

    # No wrapping Step: `read_build_state` opens one per part it reads, and a
    # Step inside a Step would make a fourth level of a hierarchy that has
    # three. What a reader wants is the parts — the catalogue and the
    # inventories are separately slow, and separately fixable.
    state = read_build_state(
        bindings,
        required_catalogue_items=catalogue_items_for_build(repository, bindings),
        session=session,
        workspace=workspace,
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


def _binding_text(binding) -> str:
    return f"{binding.target.physical_kind}/{binding.target.item.name}={binding.item}"
