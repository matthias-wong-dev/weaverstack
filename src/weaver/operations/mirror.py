"""Forking an installed estate: the destination catalogue, then its items.

``weaver mirror`` gives this workspace the installed state of another catalogue,
so an estate can be worked on without touching the estate it came from. The fork
is phase one and always runs. Phase two rebinds selected physical items, and
lands with the Mirror representations it needs.

A fork has two sides, and both are named in one vocabulary. ``mirror`` is the
catalogue read from and ``catalogue`` the catalogue written to, whether they are
written in configuration or on the command:

.. code-block:: yaml

    catalogue: Warehouse/Weaver_Dev
    mirror: Warehouse/Weaver

:class:`MirrorPlan` is that pair once resolved. It is produced once by
:func:`plan_mirror`, proved by :func:`check_mirror`, and handed to
:func:`mirror`, so what a confirmation prompt shows and what the fork empties
are the same two addresses.

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
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Mapping, Sequence

from ..errors import CommandError
from ..locations import Location
from ..workspaces import CATALOGUE_KIND, CatalogueRef, Workspace
from .workspace import operation_workspace


@dataclass(frozen=True)
class MirrorPlan:
    """The two catalogues one fork moves state between.

    ``workspace`` carries the resolved pair, so the operation reads its
    destination from :attr:`Workspace.catalogue` the way every other operation
    does. The two refs are held separately because a prompt names them, and a
    prompt that recomputed them could name a different pair from the one the
    fork acts on.
    """

    workspace: Workspace
    source: CatalogueRef
    destination: CatalogueRef
    #: The logical items this fork rebinds, in the grammar ``build`` uses.
    items: tuple[str, ...] = ()

    @property
    def target(self) -> str:
        """The destination as a wipe names it."""

        return f"{CATALOGUE_KIND}/{self.destination.name}"

    def describe(self) -> str:
        """The one sentence a confirmation and the operation's task share."""

        return f"{self.target} will be emptied and rebuilt from {self.source}."

    def __str__(self) -> str:
        return f"{self.source} into {self.destination}"


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


def plan_mirror(
    items: str | Sequence[str] | None = None,
    *,
    no_item: bool = False,
    workspace: str | None = None,
    catalogue: str | None = None,
    mirror: str | None = None,
    environment: str | None = None,
    workspace_config: str | Path | None = None,
    session=None,
) -> MirrorPlan:
    """The pair a fork would move state between, and what it refuses.

    Nothing here reaches the workspace, so a fork that cannot be performed says
    so before a Warehouse is emptied. :func:`check_mirror` is what proves the
    source is there.
    """

    if items is not None and no_item:
        raise CommandError("mirror takes items or no_item=True, not both")

    base = operation_workspace(
        "mirror",
        workspace=workspace,
        environment=environment,
        workspace_config=workspace_config,
        session=session,
        # Both sides are resolved here rather than by the catalogue override,
        # because which configured value is which depends on what else is set.
        needs_catalogue=False,
    )
    source, destination = _resolved_pair(base, catalogue=catalogue, mirror=mirror)
    _refuse_unusable_pair(source, destination, base)
    # Both are in this workspace by now, so both are named the way configuration
    # names one: the workspace is on the plan, not repeated in each address.
    source, destination = source.local, destination.local
    resolved = replace(
        base, catalogue=f"{CATALOGUE_KIND}/{destination.name}", mirror=source
    )
    return MirrorPlan(
        workspace=resolved,
        source=source,
        destination=destination,
        items=_selected_items(items, no_item, resolved),
    )


def check_mirror(plan: MirrorPlan, *, session=None) -> None:
    """Prove the source catalogue is there and readable.

    Run before a confirmation and before anything is emptied, so a misspelled
    source fails while the destination is still intact. It reads and does not
    write: what it asks is whether the Warehouse resolves and whether its ``_``
    schema holds the tables a fork copies.
    """

    from ..catalogue.fork import FORKED_TABLES
    from ..sessions.host import use_or_create_session

    with use_or_create_session(session, workspace=plan.workspace) as opened:
        found = _source_catalogue_tables(plan, session=opened)

    if not found:
        raise CommandError(
            f"mirror reads {plan.source}, which holds no catalogue: its "
            f"[_] schema has no tables. Name the Warehouse the Weaver "
            "catalogue lives in."
        )
    missing = [
        table.name for table in FORKED_TABLES if table.name.casefold() not in found
    ]
    if missing:
        raise CommandError(
            f"mirror reads {plan.source}, whose catalogue is missing "
            + ", ".join(f"[_].[{name}]" for name in missing)
            + ". Build against it with this Weaver version first, so its "
            "catalogue carries every table a fork copies."
        )


def mirror(
    items: str | Sequence[str] | None = None,
    *,
    no_item: bool = False,
    plan: MirrorPlan | None = None,
    workspace: str | None = None,
    catalogue: str | None = None,
    mirror: str | None = None,
    environment: str | None = None,
    workspace_config: str | Path | None = None,
    session=None,
) -> MirrorResult:
    """Fork another catalogue's installed estate into this workspace.

    ``items`` are the logical items to rebind, written the way ``build`` writes
    them: ``Warehouse/Model`` or ``Warehouse/Model=Warehouse/Model_Dev``. Naming
    none selects every item the workspace configuration declares, and
    ``no_item=True`` selects none, which forks the catalogue and stops.

    ``mirror`` names the catalogue to fork and ``catalogue`` the catalogue to
    fork into, the same two names workspace configuration uses. ``plan`` is a
    pair :func:`plan_mirror` already resolved, for a caller that showed it to
    somebody first; supplied, the names are already settled and are not read
    again. The source is proved either way.
    """

    forking = plan or plan_mirror(
        items,
        no_item=no_item,
        workspace=workspace,
        catalogue=catalogue,
        mirror=mirror,
        environment=environment,
        workspace_config=workspace_config,
        session=session,
    )
    if forking.items:
        raise CommandError(
            "mirror rebinds no physical item yet: "
            + ", ".join(forking.items)
            + " would be left where the forked catalogue puts them. Fork the "
            "catalogue with --no-item, then move an item with "
            "'weaver build --item ITEM=TYPE/NAME'."
        )

    from ..catalogue.fork import uncopied_table_names
    from ..sessions.host import use_or_create_session

    resolved = forking.workspace
    with use_or_create_session(session, workspace=resolved) as opened:
        # Proved again for a supplied plan, because a caller holding one is not
        # proof it was checked. The source Warehouse is already resolved by
        # then, so the second read is one query.
        check_mirror(forking, session=opened)
        with opened.task("Mirror", str(forking)):
            wiped = _wipe_destination(resolved, forking.destination, session=opened)
            _rebuild_catalogue(resolved, session=opened)
            copied = _copy_catalogue_state(
                resolved, forking.source, forking.destination, session=opened
            )

    return MirrorResult(
        workspace=str(resolved.workspace),
        source_catalogue=str(forking.source),
        destination_catalogue=str(forking.destination),
        wiped=wiped,
        copied=copied,
        uncopied=uncopied_table_names(),
        items=(),
    )


# --- resolving the pair -------------------------------------------------------


def _resolved_pair(
    base: Workspace, *, catalogue: str | None, mirror: str | None
) -> tuple[CatalogueRef, CatalogueRef]:
    """Which catalogue is read and which is written, from what is known.

    Three arrangements, and the rule is the same in all of them.

    Nothing configured: both sides are named on the command.

    A configuration naming ``catalogue:`` alone describes the estate being
    forked from, so it supplies the source. It never supplies the destination:
    a production configuration's catalogue is the last Warehouse a fork should
    empty, and treating one known side as both would do exactly that.

    A configuration naming ``catalogue:`` and ``mirror:`` describes a fork
    already, so it supplies both.
    """

    configured = base.catalogue_ref if base.catalogue else None
    named_source = CatalogueRef.parse(mirror) if mirror is not None else None
    named_destination = (
        _local_destination(catalogue, base) if catalogue is not None else None
    )

    source = named_source or base.mirror or configured
    if source is None:
        raise CommandError(
            "mirror needs the catalogue to fork. Name it with "
            f"--mirror {CATALOGUE_KIND}/<name>, or set mirror: in workspace "
            "configuration."
        )

    destination = named_destination
    if destination is None and base.mirror is not None:
        destination = configured
    if destination is None:
        raise CommandError(
            "mirror needs the catalogue to fork into. Name it with "
            f"--catalogue {CATALOGUE_KIND}/<name>. The catalogue: in workspace "
            "configuration is the estate being forked from, so a fork does not "
            "empty it; a configuration that also sets mirror: describes a fork "
            "already, and its catalogue: is the destination."
        )
    return source, destination


def _local_destination(catalogue: str, base: Workspace) -> CatalogueRef:
    """A named destination, which is always in the workspace being built."""

    parsed = CatalogueRef.parse(catalogue)
    if not parsed.is_local_to(base.workspace):
        raise CommandError(
            f"mirror writes {parsed}, which is in workspace "
            f"{parsed.owner(base.workspace)} rather than {base.workspace}. A "
            "fork writes the catalogue of the workspace it runs against."
        )
    return CatalogueRef(workspace=base.workspace, name=parsed.name)


def _refuse_unusable_pair(
    source: CatalogueRef, destination: CatalogueRef, base: Workspace
) -> None:
    """Refuse a fork that cannot be performed, before anything is emptied.

    A fork copies server-side, and a Fabric Warehouse reaches another item in
    its own workspace and no further, so a source elsewhere is refused rather
    than attempted.
    """

    if not source.is_local_to(base.workspace):
        raise CommandError(
            f"mirror reads {source}, which is in workspace "
            f"{source.owner(base.workspace)} rather than {base.workspace}. A "
            "fork copies through a Fabric Warehouse's own workspace, so the "
            "source catalogue must be in the workspace being built."
        )
    if source.name.casefold() == destination.name.casefold():
        raise CommandError(
            f"mirror reads and writes {destination}, so the fork would empty "
            "the catalogue it copies from. Name a different destination with "
            f"--catalogue {CATALOGUE_KIND}/<name>."
        )


def _selected_items(items, no_item: bool, workspace: Workspace) -> tuple[str, ...]:
    """The logical items this run rebinds, in the grammar ``build`` uses."""

    if no_item:
        return ()
    if items is None:
        return tuple(str(item) for item in workspace.configured_items)
    return (items,) if isinstance(items, str) else tuple(items)


# --- reading the source, and then doing the work ------------------------------


def _source_catalogue_tables(plan: MirrorPlan, *, session) -> set[str]:
    """The tables the source catalogue's ``_`` schema holds, folded.

    A Warehouse this workspace does not hold fails here, which is the point of
    reading before emptying anything.
    """

    from ..catalogue.tables import CATALOGUE_SCHEMA
    from ..errors import WeaverError
    from ..targets import WarehouseTarget

    try:
        sql = session.sql_executor(
            WarehouseTarget(plan.source.item), workspace=plan.workspace
        )
        rows = sql.query(
            "select objects.name as name "
            "from sys.objects as objects "
            "where objects.is_ms_shipped = 0 "
            "and objects.type = N'U' "
            f"and schema_name(objects.schema_id) = N'{CATALOGUE_SCHEMA}'"
        )
    except WeaverError as exc:
        raise CommandError(f"mirror could not read {plan.source}: {exc}") from exc
    return {str(row["name"]).casefold() for row in rows}


def _wipe_destination(
    workspace: Workspace, destination: CatalogueRef, *, session
) -> tuple[str, ...]:
    """Empty the destination Warehouse, through the ordinary wipe.

    The public operation rather than a private cleanup: a fork's destination
    starts from the same state ``weaver wipe`` leaves, including the schemas the
    Warehouse held that this estate does not declare.
    """

    from .wipe import wipe

    target = f"{CATALOGUE_KIND}/{destination.name}"
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
