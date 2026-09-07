"""Forking an installed estate: the destination catalogue, then its items.

``weaver mirror`` gives this workspace the installed state of another catalogue,
so an estate can be worked on without touching the estate it came from. The fork
is phase one and always runs. Phase two rebinds selected physical items, and
lands with the Mirror representations it needs.

The fork is reconstruction, not repair. The destination Warehouse is emptied by
an ordinary :func:`weaver.wipe`, its ``_`` schema is rebuilt by an ordinary
build, and the source's rows are copied in. Running it again does the same thing
again, which is what makes a half-finished fork recoverable.

**Installation is copied as it stands.** A forked catalogue names the source's
physical targets, so every item begins where it already is. One
``weaver build --item Warehouse/Model=Warehouse/Model_Dev`` then moves that item
alone, and the rest of the estate stays put. That is the fork's whole point: the
choice of what to diverge is made per item, after the state is branched.

**A kept item's ``_`` surface still addresses the source catalogue.** The views
and OneLake shortcuts in an item's ``_`` schema were built pointing at the
catalogue that built them, and a fork does not touch the item. ``weaver load``
driven against the fork records centrally and is unaffected, but
``exec [_].[Load]`` typed inside a kept item reaches the source catalogue. An
item is only fully the fork's once it has been built into a target of its own.
"""

from __future__ import annotations

import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping, Sequence

from ..errors import CommandError
from ..locations import Location
from ..workspaces import CatalogueRef, Workspace
from .workspace import operation_workspace


@dataclass(frozen=True)
class MirrorResult:
    """What one fork did, and to what."""

    workspace: str
    source_catalogue: str
    destination_catalogue: str
    #: The physical targets this operation emptied, in the order it emptied them.
    wiped: tuple[str, ...]
    #: Rows now in each copied catalogue table, read back after the copy.
    copied: Mapping[str, int] = field(default_factory=dict)
    #: Catalogue tables the fork rebuilt and left empty, being history.
    uncopied: tuple[str, ...] = ()
    #: The logical items whose physical binding this operation changed. Empty
    #: while the fork is all ``mirror`` does.
    items: tuple[str, ...] = ()
    status: str = "succeeded"

    @property
    def rows(self) -> int:
        return sum(self.copied.values())

    def to_mapping(self) -> dict:
        return {
            "workspace": self.workspace,
            "source_catalogue": self.source_catalogue,
            "destination_catalogue": self.destination_catalogue,
            "wiped": list(self.wiped),
            "copied": dict(self.copied),
            "uncopied": list(self.uncopied),
            "items": list(self.items),
            "status": self.status,
        }


def mirror(
    items: str | Sequence[str] | None = None,
    *,
    no_item: bool = False,
    workspace: str | None = None,
    catalogue: str | None = None,
    source: str | None = None,
    environment: str | None = None,
    workspace_config: str | Path | None = None,
    session=None,
) -> MirrorResult:
    """Fork another catalogue's installed estate into this workspace.

    ``items`` are the logical items to rebind, written the way ``build`` writes
    them: ``Warehouse/Model`` or ``Warehouse/Model=Warehouse/Model_Dev``. Naming
    none selects every item the workspace configuration declares, and
    ``no_item=True`` selects none, which forks the catalogue and stops.

    ``source`` names the catalogue to fork, typed as ``Warehouse/Weaver``, and
    outranks ``mirror:`` in workspace configuration. Where it is forked to is
    this workspace's own ``catalogue``.
    """

    if items is not None and no_item:
        raise CommandError("mirror takes items or no_item=True, not both")

    resolved_workspace = operation_workspace(
        "mirror",
        workspace=workspace,
        catalogue=catalogue,
        mirror=source,
        environment=environment,
        workspace_config=workspace_config,
        session=session,
    )
    forked = _source_catalogue(resolved_workspace)
    destination = resolved_workspace.catalogue_ref
    _refuse_unreachable_source(forked, destination, resolved_workspace)

    selected = _selected_items(items, no_item, resolved_workspace)
    if selected:
        raise CommandError(
            "mirror rebinds no physical item yet: "
            + ", ".join(selected)
            + " would be left where the forked catalogue puts them. Fork the "
            "catalogue with --no-item, then move an item with "
            "'weaver build --item ITEM=TYPE/NAME'."
        )

    from ..sessions.host import use_or_create_session

    with use_or_create_session(session, workspace=resolved_workspace) as opened:
        with opened.task("Mirror", f"{forked} into {destination}"):
            wiped = _wipe_destination(resolved_workspace, destination, session=opened)
            _rebuild_catalogue(resolved_workspace, session=opened)
            copied = _copy_catalogue_state(
                resolved_workspace, forked, destination, session=opened
            )

    from ..catalogue.fork import uncopied_table_names

    return MirrorResult(
        workspace=str(resolved_workspace.workspace),
        source_catalogue=str(forked),
        destination_catalogue=str(destination),
        wiped=wiped,
        copied=copied,
        uncopied=uncopied_table_names(),
        items=(),
    )


def _source_catalogue(workspace: Workspace) -> CatalogueRef:
    """The catalogue this workspace forks, or a failure saying it names none."""

    if workspace.mirror is None:
        raise CommandError(
            "this Workspace forks no catalogue: set mirror: to the catalogue "
            "holding the estate to fork, for example "
            "mirror: Warehouse/Weaver, in workspace configuration"
        )
    return workspace.mirror


def _refuse_unreachable_source(
    source: CatalogueRef, destination: CatalogueRef, workspace: Workspace
) -> None:
    """Refuse a fork that cannot be performed, before anything is emptied.

    A fork copies server-side, and a Fabric Warehouse reaches another item in
    its own workspace and no further, so a source elsewhere is refused rather
    than attempted. Refused here, because the next step empties a Warehouse.
    """

    if not source.is_local_to(workspace.workspace):
        raise CommandError(
            f"mirror reads {source}, which is in workspace "
            f"{source.owner(workspace.workspace)} rather than "
            f"{workspace.workspace}. A fork copies through a Fabric Warehouse's "
            "own workspace, so the source catalogue must be in the workspace "
            "being built."
        )
    if source.name.casefold() == destination.name.casefold():
        raise CommandError(
            f"mirror reads and writes {destination}, so the fork would empty the "
            "catalogue it copies from. Name a different catalogue: in workspace "
            "configuration."
        )


def _selected_items(items, no_item: bool, workspace: Workspace) -> tuple[str, ...]:
    """The logical items this run rebinds, in the grammar ``build`` uses."""

    if no_item:
        return ()
    if items is None:
        return tuple(str(item) for item in workspace.configured_items)
    values = (items,) if isinstance(items, str) else tuple(items)
    return values


def _wipe_destination(
    workspace: Workspace, destination: CatalogueRef, *, session
) -> tuple[str, ...]:
    """Empty the destination Warehouse, through the ordinary wipe.

    The public operation rather than a private cleanup: a fork's destination
    starts from the same state ``weaver wipe`` leaves, including the schemas the
    Warehouse held that this estate does not declare.
    """

    from .wipe import wipe

    target = f"Warehouse/{destination.name}"
    with session.step(f"Empty {target}"):
        # The catalogue is named, so the wipe unbinds claims from the
        # one being emptied rather than whichever the session holds.
        wipe(
            target,
            session=session,
            workspace=workspace.workspace,
            catalogue=workspace.catalogue,
        )
    return (target,)


def _rebuild_catalogue(workspace: Workspace, *, session) -> None:
    """Build the ``_`` schema, through an ordinary build bound to nothing else.

    The catalogue's tables are declared in :mod:`weaver.fragments` and composed
    into every repository, so a build over an empty tree builds the catalogue
    and only the catalogue. That gives the destination the declared shape,
    including the constraints, and the Registry rows certifying it.
    """

    from ..build_bundle.targets import (
        ItemBindings,
        WarehouseBinding,
        effective_item_bindings,
    )
    from ..build_bundle.workflow import prepare_repository, validate_build_request
    from ..store import FilesystemStore
    from .build import _run_build

    control = WarehouseBinding(
        workspace.catalogue_item, workspace_name=workspace.workspace
    )
    bindings = effective_item_bindings(
        ItemBindings(()),
        control_item=workspace.catalogue_item,
        workspace_name=workspace.workspace,
    )
    with session.step("Build the catalogue"):
        with tempfile.TemporaryDirectory(prefix="weaver-fork-") as empty:
            location = Location(Path(empty).as_posix())
            with prepare_repository(location, source_store=FilesystemStore()) as ready:
                validate_build_request(
                    ready.repository, bindings, catalogue_binding=control
                )
                result = _run_build(
                    workspace,
                    session=session,
                    repository=ready.repository,
                    source_store=ready.store,
                    bindings=bindings,
                    catalogue_binding=control,
                    bundle_only=False,
                    bundle_path=None,
                    source=location.value,
                )
    if not result.succeeded:
        raise CommandError(
            "the destination catalogue could not be built, so nothing was "
            "copied into it: " + "; ".join(error.describe() for error in result.errors)
        )


def _copy_catalogue_state(
    workspace: Workspace,
    source: CatalogueRef,
    destination: CatalogueRef,
    *,
    session,
) -> dict[str, int]:
    """Copy the source catalogue's rows in, then read back what landed."""

    from ..catalogue.fork import FORKED_TABLES, fork_statements
    from ..targets import WarehouseTarget

    sql = session.sql_executor(WarehouseTarget(destination.item), workspace=workspace)
    with session.step(f"Copy catalogue state from {source}"):
        sql.execute_script("\n".join(fork_statements(source_catalogue=source.name)))
    with session.step("Count what was copied"):
        rows = sql.query(_count_statement())
    counted = {str(row["Table"]): int(row["Rows"]) for row in rows}
    return {table.name: counted.get(table.name, 0) for table in FORKED_TABLES}


def _count_statement() -> str:
    """One statement counting every copied table, so the read is one crossing."""

    from ..catalogue.fork import FORKED_TABLES
    from ..catalogue.tables import CATALOGUE_SCHEMA
    from ..catalogue.tsql import identifier, literal

    return "\nunion all\n".join(
        f"select {literal(table.name)} as [Table], count(*) as [Rows] "
        f"from {identifier(CATALOGUE_SCHEMA)}.{identifier(table.name)}"
        for table in FORKED_TABLES
    )
