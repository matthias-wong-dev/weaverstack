"""Prepare a Weaver Lakehouse and run the compatibility initialisation path.

The package-owned ``_weaver`` item creates catalogue tables through ordinary
build actions. This module provisions a missing Fabric item or local emulator
skeleton; it does not create catalogue tables directly.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import tempfile
from typing import Any

from .build_bundle.models import BuildPlan
from .build_bundle.report import InstallationReport
from .build_bundle.targets import ItemBinding, ItemBindings, LakehouseBinding
from .build_bundle.workflow import build_item_repository_source
from .catalogue.tables import CATALOGUE_TABLES
from .declaration.model import WeaverItemId
from .errors import CommandError
from .locations import Location
from .resolution import resolver_for
from .store import FilesystemStore, Store
from .targets import ItemRef
from .workspaces import FabricWorkspace, LocalWorkspace


@dataclass(frozen=True)
class InitialiseResult:
    """What initialisation did, in terms a caller can print or assert on."""

    item: str
    weaver_lakehouse: str
    plan: BuildPlan
    report: InstallationReport

    @property
    def succeeded(self) -> bool:
        return self.report.status == "succeeded"

    @property
    def tables(self) -> tuple[str, ...]:
        return tuple(table.qualified for table in CATALOGUE_TABLES)

    def to_mapping(self) -> dict[str, Any]:
        """A plain structure, for a CLI to serialise. The CLI owns no semantics."""

        return {
            "item": self.item,
            "weaver_lakehouse": self.weaver_lakehouse,
            "bundle_id": self.plan.bundle_id,
            "status": self.report.status,
            "tables": list(self.tables),
        }


@dataclass(frozen=True)
class PreparedWeaverLakehouse:
    workspace: str
    weaver_lakehouse: str
    created: bool


def prepare_weaver_lakehouse(
    workspace,
    *,
    exists_ok: bool = False,
    store: Store | None = None,
    client=None,
) -> PreparedWeaverLakehouse:
    """Create the configured Weaver Lakehouse and its required Files areas."""

    if not workspace.weaver_lakehouse:
        raise CommandError("initialise requires a configured Weaver Lakehouse")
    name = workspace.weaver_lakehouse
    if isinstance(workspace, LocalWorkspace):
        from .store import FilesystemStore

        store = store or FilesystemStore()
        resolver = resolver_for(workspace)
        existed = store.exists(resolver.weaver_lakehouse)
        if existed and not exists_ok:
            raise CommandError(
                f"Weaver Lakehouse {name!r} already exists; pass --exists-ok"
            )
        store.make_directory(resolver.files_root(ItemRef(name)))
        store.make_directory(resolver.tables_root(ItemRef(name)))
        return PreparedWeaverLakehouse(str(workspace.workspace), name, not existed)

    if isinstance(workspace, FabricWorkspace):
        from .fabric.resources import (
            LAKEHOUSE,
            ItemNotFoundError,
            create_lakehouse,
            find_item,
            find_workspace,
        )

        physical_workspace = find_workspace(workspace.workspace, client=client)
        try:
            find_item(physical_workspace, name, item_type=LAKEHOUSE, client=client)
        except ItemNotFoundError:
            create_lakehouse(physical_workspace, name, client=client)
            created = True
        else:
            if not exists_ok:
                raise CommandError(
                    f"Weaver Lakehouse {name!r} already exists; pass --exists-ok"
                )
            created = False
        return PreparedWeaverLakehouse(workspace.workspace, name, created)

    raise CommandError(f"unsupported Workspace type: {type(workspace).__name__}")


def _session_around(workspace, *, spark, store):
    """A Session wrapped around resources the caller already holds.

    Both are *given*, so the Session closes neither. This is how a caller that
    is already inside its own Spark session — a notebook, or a test holding one
    open for a module — reaches the build path without the build acquiring a
    second one.
    """

    from .session import ConsoleSession

    return ConsoleSession(workspace=workspace, spark=spark, store=store)


def initialise_weaver_lakehouse(
    *,
    weaver_lakehouse: ItemRef,
    workspace,
    store: Store,
    spark: Any = None,
    output: Location | None = None,
    session=None,
) -> InitialiseResult:
    """Build the built-in Weaver item alone, through the ordinary build path.

    A compatibility wrapper and nothing more. It owns no catalogue DDL, no
    catalogue publication and no control-plane preparation: it selects no
    authored item, and the built-in ``Lakehouse/_weaver`` that every build
    injects is therefore the whole of what it builds.

    Ordinary builds do not call this. They inject the same item and bind it the
    same way, so calling it first would build the catalogue twice — see
    :mod:`weaver.operations`, which used to.

    An empty source directory is the input because the built-in item is composed
    into a *parsed* repository rather than authored into one: there is nothing
    for a caller to supply, and supplying a real repository here would silently
    ignore it.
    """

    control = LakehouseBinding(lakehouse=weaver_lakehouse)
    bindings = ItemBindings(
        (
            ItemBinding(
                WeaverItemId.parse("Lakehouse/_weaver"),
                control,
            ),
        )
    )
    from .session.host import use_or_create_session

    # A Session built around what this caller already holds: the Spark it is
    # running in and the store it reads through are given, so nothing here
    # acquires — or closes — a resource it did not open.
    owned = (
        None
        if session is not None
        else _session_around(workspace, spark=spark, store=store)
    )
    with use_or_create_session(session or owned, workspace=workspace) as opened:
        with tempfile.TemporaryDirectory(prefix="weaver-initialise-") as temporary:
            repository_root = Path(temporary) / "repository"
            repository_root.mkdir()
            result = build_item_repository_source(
                Location(repository_root.as_posix()),
                source_store=FilesystemStore(),
                bindings=bindings,
                session=opened,
                workspace=workspace,
                control_lakehouse=control,
                output=output,
            )

    return InitialiseResult(
        item="Lakehouse/_weaver",
        weaver_lakehouse=weaver_lakehouse.name,
        plan=result.plan,
        report=result.report,
    )
