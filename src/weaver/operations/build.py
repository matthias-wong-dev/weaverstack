"""Source-neutral public build and target-oriented wipe operations.

Optional platform imports remain inside the operation paths that require them,
so importing ``weaver`` does not require Spark, Fabric credentials, or the CLI.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path
import tempfile
import uuid
from typing import Iterable, Mapping, Sequence

from ..errors import BuildError, CommandError, WeaverError
from ..locations import Location
# Position — whether this process is inside the Fabric session it addresses, and
# what Spark it would use if it were — is the first half of "where am I
# running", so a Session owns it. Kept under the names this module already uses.
from ..sessions.host import inside_fabric_session as _inside_fabric_session
from ..store import FilesystemStore, Store
from ..targets import (
    ItemRef,
    WarehouseTarget,
    parse_physical_target,
    physical_item,
    physical_kind,
)
from ..workspaces import FabricWorkspace, Workspace
from .workspace import (
    _current_fabric_workspace,
    _operation_workspace,
    _with_inferred_control_lakehouse,
    current_workspace,
)


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


def build(
    source=None,
    *,
    bind: str | Sequence[str] | None = None,
    workspace: str | None = None,
    catalogue: str | None = None,
    environment: str | None = None,
    workspace_config: str | Path | None = None,
    bundle: str | None = None,
    session=None,
) -> BuildResult:
    """Build an authored repository using simple notebook-facing values.

    Every value resolves the same way: an explicit argument, then an
    already-resolved typed ``Workspace``, then workspace configuration, then —
    inside a Fabric notebook only — the session's own context. Anything still
    unresolved is an error stated in one sentence.

    ``workspace=None`` means the current Fabric session. A typed ``Workspace``
    arrives already resolved, so configuration is never layered over it.

    ``catalogue`` names the Weaver control Lakehouse. Inside a notebook it
    defaults to the attached Lakehouse, which is the control Lakehouse only and
    becomes an authored target only if a binding says so.

    ``session`` is a Session to run in. Supplied, its resources are reused and it
    is left open; omitted, this operation creates and closes one.
    """

    resolved_workspace = _operation_workspace(
        workspace=workspace,
        workspace_config=workspace_config,
        catalogue=catalogue,
        environment=environment,
        session=session,
    )
    if catalogue is not None:
        # An explicit argument outranks a configured or already-resolved value,
        # so a notebook can override what it inferred without rebuilding the
        # Workspace it inferred it into.
        resolved_workspace = replace(
            resolved_workspace,
            catalogue=str(catalogue),
        )
    resolved_workspace = _with_inferred_control_lakehouse(resolved_workspace)
    if not resolved_workspace.catalogue:
        raise CommandError(
            "build needs a Weaver control Lakehouse: pass catalogue=, "
            "give one in workspace configuration, or run inside a Fabric "
            "notebook with one attached as the default Lakehouse"
        )

    selected = _item_bindings(bind, resolved_workspace)
    from ..build_bundle.targets import LakehouseBinding, effective_item_bindings

    workspace_name = getattr(resolved_workspace, "workspace", None)
    bindings = effective_item_bindings(
        selected,
        control_item=resolved_workspace.catalogue_item,
        workspace_name=workspace_name,
    )
    control = LakehouseBinding(
        resolved_workspace.catalogue_item, workspace_name=workspace_name
    )
    source_location, source_store = _repository_source(source, resolved_workspace)

    # This complete parse and pure request validation is deliberately above all
    # control-plane creation, Spark start, REST item resolution, and Livy work.
    from ..build_bundle.workflow import prepare_repository, validate_build_request

    from ..sessions.host import use_or_create_session

    with prepare_repository(
        source_location, source_store=source_store
    ) as prepared:
        validate_build_request(
            prepared.repository, bindings, control_lakehouse=control
        )
        with use_or_create_session(session, workspace=resolved_workspace) as opened:
            arguments = dict(
                repository=prepared.repository,
                source_store=prepared.store,
                bindings=bindings,
                control_lakehouse=control,
                bundle_name=bundle,
                source=source_location.value,
            )
            with opened.task("Build", str(resolved_workspace.workspace)):
                if opened.executes_here(resolved_workspace):
                    # The emulator and a notebook alike: this process is already
                    # where the data engineering happens, so the build runs here.
                    return _build_in_process(
                        resolved_workspace, session=opened, **arguments
                    )
                return _build_desktop_fabric(
                    resolved_workspace, session=opened, **arguments
                )


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


def _archive_location(resolver, bundle_name: str | None):
    if bundle_name is None:
        return None
    from ..build_bundle.workflow import timestamped_archive_name

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


def _build_in_process(
    workspace,
    *,
    session,
    repository,
    source_store,
    bindings,
    control_lakehouse,
    bundle_name,
    source,
) -> BuildResult:
    """One build, where this process is already where the data is.

    Which Spark, which store and which resolver are the Session's answers, so a
    notebook build takes the same path a desktop build does.
    """

    from ..build_bundle import (
        build_item_repository,
        catalogue_items_for_build,
        read_build_state,
    )

    resolver = session.resolver(workspace)
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
    with session.step("Build and install"):
        result = build_item_repository(
            repository,
            bindings=bindings,
            state=state,
            session=session,
            workspace=workspace,
            source_store=source_store,
            control_lakehouse=control_lakehouse,
            archive=_archive_location(resolver, bundle_name),
        )
    return _result_from_item_build(source, bindings, result)




def _build_desktop_fabric(
    workspace,
    *,
    session,
    repository,
    source_store,
    bindings,
    control_lakehouse,
    bundle_name,
    source,
) -> BuildResult:
    """One build driven from a console.

    .. code-block:: text

        read the build state    → through Session capabilities, per part
        plan the bundle         → here, in Python
        install the bundle      → here, each action to the capability it needs

    Each part of the state read asks for only what it needs — a Warehouse
    inventory over TDS, a Lakehouse's objects from storage, the catalogue and
    its views as Spark SQL — and each is timed as its own Step.

    Nothing is packed to install. ``--bundle`` keeps a build record, written
    after the install and read by nobody in this path.
    """

    if not workspace.environment:
        raise CommandError(
            "Fabric build requires an Environment in workspace configuration"
        )
    from ..build_bundle import (
        Installer,
        catalogue_items_for_build,
        generate_item_build_bundle,
        persist_bundle_archive,
        read_build_state,
    )
    from ..catalogue.state import reconcile_catalogue_state
    from ..fabric.preflight import preflight_fabric_targets
    # Above the session, deliberately. Every item this build needs is proved to
    # exist from one workspace listing, so a missing target costs a REST call
    # rather than a Livy session and a Spark traceback about a catalogue.
    preflight_fabric_targets(
        bindings,
        workspace=workspace.workspace,
        control_item=workspace.catalogue_item,
        environment=workspace.environment,
    )
    resolver = session.resolver(workspace)
    transport_store = session.transport_store(workspace)
    retained_archive = _archive_location(resolver, bundle_name)

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
    reconciliation = reconcile_catalogue_state(
        state.catalogue, inventories=state.target_inventories
    )
    with tempfile.TemporaryDirectory(prefix="weaver-cli-build-") as temporary:
        root = Path(temporary)
        with session.step("Build bundle"):
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
        with session.step("Install"):
            # The Installer runs here, so nothing is packed to install: the
            # deployed Python tree goes straight to OneLake. The archive below
            # is a retained build record, not a delivery mechanism.
            report = Installer(session, workspace=workspace).install(bundle).to_mapping()
        if retained_archive is not None:
            with session.step("Retain bundle", bundle.bundle_id):
                local_archive = Location((root / "install.weaver.zip").as_posix())
                persist_bundle_archive(bundle, local_archive, store=FilesystemStore())
                transport_store.make_directory(resolver.build_bundles_root)
                transport_store.write(
                    retained_archive, FilesystemStore().read(local_archive)
                )

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
                artefact=action.get("resource_node_id"),
                source_path=action.get("source_path"),
            )
            for sequence in report.get("sequences", ())
            for action in sequence.get("actions", ())
            if action.get("status") == "failed"
        ),
    )


def _binding_text(binding) -> str:
    return f"{binding.target.physical_kind}/{binding.target.item.name}={binding.item}"


