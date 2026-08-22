"""Prepare the Warehouse a build needs before it can hold the Weaver catalogue.

The package-owned ``_weaver`` item creates the catalogue tables through ordinary
build actions. This provisions the Fabric item they live in; it creates no
catalogue tables of its own.

Weaver owns the ``_`` schema of that Warehouse and nothing else in it, so an
existing Warehouse holding a user's own schemas is an ordinary host rather than
a collision.
"""

from __future__ import annotations

import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .build_bundle.models import BuildPlan
from .build_bundle.report import InstallationReport
from .build_bundle.targets import ItemBinding, ItemBindings, WarehouseBinding
from .build_bundle.workflow import build_item_repository_source
from .catalogue.tables import CATALOGUE_TABLES
from .declaration.model import WeaverItemId
from .errors import CommandError
from .locations import Location
from .store import FilesystemStore, Store
from .targets import ItemRef


@dataclass(frozen=True)
class InitialiseResult:
    """What initialisation did, in terms a caller can print or assert on."""

    item: str
    catalogue: str
    plan: BuildPlan
    report: InstallationReport

    @property
    def succeeded(self) -> bool:
        return self.report.status == "succeeded"

    @property
    def tables(self) -> tuple[str, ...]:
        """Every ``_`` table this created, however each one is maintained."""

        return tuple(table.qualified for table in CATALOGUE_TABLES)

    def to_mapping(self) -> dict[str, Any]:
        """A plain structure, for a CLI to serialise. The CLI owns no semantics."""

        return {
            "item": self.item,
            "catalogue": self.catalogue,
            "bundle_id": self.plan.bundle_id,
            "status": self.report.status,
            "tables": list(self.tables),
        }


@dataclass(frozen=True)
class PreparedCatalogueHost:
    """The Warehouse the catalogue will live in, and whether this made it."""

    workspace: str
    catalogue: str
    created: bool


def prepare_catalogue(
    workspace,
    *,
    store: Store | None = None,
    client=None,
) -> PreparedCatalogueHost:
    """Find or create the Warehouse the Weaver catalogue lives in.

    An existing Warehouse is the ordinary case rather than a collision: Weaver
    owns the ``_`` schema of its host and nothing else, so a Warehouse already
    holding a user's schemas is a perfectly good catalogue host. What is
    distinguished here is only whether the Warehouse existed — whether its `_`
    tables are there is the build's question, answered by reading them.
    """

    if not workspace.catalogue:
        raise CommandError("initialise requires a configured Weaver catalogue")
    name = workspace.catalogue_item.name
    from .fabric.resources import (
        WAREHOUSE,
        ItemNotFoundError,
        create_warehouse,
        find_item,
        find_workspace,
    )

    physical_workspace = find_workspace(workspace.workspace, client=client)
    try:
        find_item(physical_workspace, name, item_type=WAREHOUSE, client=client)
        created = False
    except ItemNotFoundError:
        create_warehouse(physical_workspace, name, client=client)
        created = True
    return PreparedCatalogueHost(workspace.workspace, name, created)


def _session_around(workspace, *, spark, store):
    """A Session wrapped around resources the caller already holds.

    Both are *given*, so the Session closes neither. This is how a caller that
    is already inside its own Spark session — a notebook, or a test holding one
    open for a module — reaches the build path without the build acquiring a
    second one.
    """

    from .sessions import ConsoleSession

    return ConsoleSession(workspace=workspace, spark=spark, store=store)


def initialise_catalogue(
    *,
    catalogue: ItemRef,
    workspace,
    store: Store,
    spark: Any = None,
    output: Location | None = None,
    session=None,
) -> InitialiseResult:
    """Build the built-in Weaver item alone, through the ordinary build path.

    A compatibility wrapper and nothing more. It owns no catalogue DDL, no
    catalogue publication and no control-plane preparation: it selects no
    authored item, and the built-in ``Warehouse/_weaver`` that every build
    injects is therefore the whole of what it builds.

    Ordinary builds inject and bind the same Item directly.

    An empty source directory is the input because the built-in item is composed
    into a *parsed* repository rather than authored into one: there is nothing
    for a caller to supply, and supplying a real repository here would silently
    ignore it.
    """

    control = WarehouseBinding(warehouse=catalogue)
    bindings = ItemBindings(
        (
            ItemBinding(
                WeaverItemId.parse("Warehouse/_weaver"),
                control,
            ),
        )
    )
    from .sessions.host import use_or_create_session

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
                catalogue_binding=control,
                output=output,
            )

    return InitialiseResult(
        item="Warehouse/_weaver",
        catalogue=catalogue.name,
        plan=result.plan,
        report=result.report,
    )
