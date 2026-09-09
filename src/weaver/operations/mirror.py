"""Forking an installed estate: the destination catalogue, then its items.

``mirror`` is the catalogue read from and ``catalogue`` the one written to, in
configuration and on the command alike. A run resolves, validates, empties,
builds the mirrors and records them.

See ``design/catalogue.md``.
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
    """The two catalogues one fork moves state between, and what it rebinds."""

    workspace: Workspace
    source: CatalogueRef
    destination: CatalogueRef
    #: Logical items to rebind, in the grammar ``build`` uses.
    items: tuple[str, ...] = ()

    @property
    def target(self) -> str:
        return f"{CATALOGUE_KIND}/{self.destination.name}"

    def describe(self) -> str:
        return f"{self.target} will be emptied and rebuilt from {self.source}."

    def __str__(self) -> str:
        return f"{self.source} into {self.destination}"


@dataclass(frozen=True)
class MirrorItem:
    """One selected item, with the Warehouse it borrows from and the one it fills."""

    item: object
    source_target: str
    destination: str
    relations: tuple = ()
    programmables: tuple = ()

    @property
    def target(self) -> str:
        return f"{CATALOGUE_KIND}/{self.destination}"

    def describe(self) -> str:
        return (
            f"{CATALOGUE_KIND}/{self.source_target} will be mirrored into "
            f"{self.target}."
        )


@dataclass(frozen=True)
class ResolvedMirror:
    """A plan with its items settled against the source catalogue."""

    plan: MirrorPlan
    #: Whether the source catalogue holds a ``_.Mirror``.
    borrowed: bool = False
    items: tuple[MirrorItem, ...] = ()

    @property
    def workspace(self) -> Workspace:
        return self.plan.workspace

    @property
    def source(self) -> CatalogueRef:
        return self.plan.source

    @property
    def destination(self) -> CatalogueRef:
        return self.plan.destination

    @property
    def wiped(self) -> tuple[str, ...]:
        """Every Warehouse this run empties, in the order it empties them."""

        return (self.plan.target, *(item.target for item in self.items))

    def describe(self) -> str:
        emptied = "\n".join(f"  {target}" for target in self.wiped)
        lines = [f"Mirror will empty:\n\n{emptied}\n", self.plan.describe()]
        lines.extend(item.describe() for item in self.items)
        return "\n".join(lines)

    def __str__(self) -> str:
        return str(self.plan)


@dataclass(frozen=True)
class MirrorResult:
    """What one fork did, and to what."""

    workspace: str
    source_catalogue: str
    destination_catalogue: str
    wiped: tuple[str, ...]
    copied: Mapping[str, int] = field(default_factory=dict)
    #: Catalogue tables the fork rebuilt and left empty, being history.
    uncopied: tuple[str, ...] = ()
    items: tuple[str, ...] = ()
    borrowed: Mapping[str, Mapping] = field(default_factory=dict)
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
            "borrowed": {item: dict(each) for item, each in self.borrowed.items()},
            "status": self.status,
        }


# --- the operation ------------------------------------------------------------


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
    """The pair a fork would move state between, resolved without a workspace."""

    if items is not None and no_item:
        raise CommandError("mirror takes items or no_item=True, not both")

    base = operation_workspace(
        "mirror",
        workspace=workspace,
        environment=environment,
        workspace_config=workspace_config,
        session=session,
        # Which configured value is the source and which the destination depends
        # on what else is set, so the catalogue override cannot decide it here.
        needs_catalogue=False,
    )
    source, destination = _resolved_pair(base, catalogue=catalogue, mirror=mirror)
    _refuse_unusable_pair(source, destination, base)
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


def check_mirror(plan: MirrorPlan, *, session=None) -> ResolvedMirror:
    """Read the source catalogue and settle the run against it.

    Reads and does not write, so a misspelled source or an item the catalogue
    never installed fails while every Warehouse is still intact.
    """

    from ..catalogue.state import catalogue_for
    from ..sessions.host import use_or_create_session

    with use_or_create_session(session, workspace=plan.workspace) as opened:
        borrowed = _prove_source(plan, session=opened)
        catalogue = (
            catalogue_for(opened, _source_workspace(plan)) if plan.items else None
        )
        return resolve_mirror(plan, catalogue, borrowed=borrowed)


def resolve_mirror(
    plan: MirrorPlan, catalogue, *, borrowed: bool = False
) -> ResolvedMirror:
    """Settle a plan against the source catalogue. ``None`` selects no item."""

    resolved = ResolvedMirror(
        plan=plan,
        borrowed=borrowed,
        items=_resolved_items(plan, catalogue) if plan.items else (),
    )
    _refuse_unsafe(resolved)
    return resolved


def mirror(
    items: str | Sequence[str] | None = None,
    *,
    no_item: bool = False,
    plan: MirrorPlan | ResolvedMirror | None = None,
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
    fork into, the same two names workspace configuration uses. A
    :class:`ResolvedMirror` as ``plan`` is acted on as it stands; a
    :class:`MirrorPlan` goes through :func:`check_mirror` first.
    """

    forking = plan if isinstance(plan, ResolvedMirror) else None
    if forking is None:
        forking = check_mirror(
            plan
            or plan_mirror(
                items,
                no_item=no_item,
                workspace=workspace,
                catalogue=catalogue,
                mirror=mirror,
                environment=environment,
                workspace_config=workspace_config,
                session=session,
            ),
            session=session,
        )
    from ..catalogue.fork import uncopied_table_names
    from ..sessions.host import use_or_create_session

    resolved = forking.workspace
    with use_or_create_session(session, workspace=resolved) as opened:
        with opened.task("Mirror", str(forking)):
            wiped = [_wipe_destination(resolved, forking.destination, session=opened)]
            _rebuild_catalogue(resolved, session=opened)
            copied = _copy_catalogue_state(
                resolved,
                forking.source,
                forking.destination,
                borrowed=forking.borrowed,
                session=opened,
            )
            mirrored = {}
            for each in forking.items:
                wiped.append(_wipe_target(resolved, each, session=opened))
                mirrored[str(each.item)] = _mirror_item(resolved, each, session=opened)

    return MirrorResult(
        workspace=str(resolved.workspace),
        source_catalogue=str(forking.source),
        destination_catalogue=str(forking.destination),
        wiped=tuple(wiped),
        copied=copied,
        uncopied=uncopied_table_names(),
        items=tuple(sorted(mirrored)),
        borrowed=mirrored,
    )


# --- resolving ----------------------------------------------------------------


def _resolved_pair(
    base: Workspace, *, catalogue: str | None, mirror: str | None
) -> tuple[CatalogueRef, CatalogueRef]:
    """Which catalogue is read and which is written, from what is known.

    A configuration naming ``catalogue:`` alone supplies the source and never
    the destination: treating one known side as both would empty a production
    catalogue. One naming ``mirror:`` too describes a fork already.
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
    """Refuse a pair no fork could carry out."""

    # A fork copies server-side, and a Fabric Warehouse reaches another item in
    # its own workspace and no further.
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


def _prove_source(plan: MirrorPlan, *, session) -> bool:
    """Whether the source catalogue holds a ``_.Mirror``, having proved the rest.

    Nothing declares that table, so a source that has never mirrored anything
    has none.
    """

    from ..catalogue.fork import FORKED_TABLES
    from ..catalogue.tables import MIRROR

    found = _source_catalogue_tables(plan, session=session)
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
    return MIRROR.name.casefold() in found


def _source_catalogue_tables(plan: MirrorPlan, *, session) -> set[str]:
    """The tables the source catalogue's ``_`` schema holds, folded."""

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


def _source_workspace(plan: MirrorPlan) -> Workspace:
    """The plan's workspace, pointed at the catalogue being forked."""

    return replace(plan.workspace, catalogue=f"{CATALOGUE_KIND}/{plan.source.name}")


def _resolved_items(plan: MirrorPlan, catalogue) -> tuple[MirrorItem, ...]:
    """Each selected item, against the catalogue being forked.

    The source catalogue is what is read: it says where each item is installed
    and what a mirror stands over, and it is intact at this point in the run.
    """

    from ..build_bundle.targets import parse_build_item
    from ..catalogue.borrow import borrowable, executable
    from ..catalogue.tables import INSTALLATION
    from ..declaration.model import WAREHOUSE

    installed = {
        (str(row.get("item_type")), str(row.get("item_name"))): str(
            row.get("target_name") or ""
        )
        for row in catalogue.table_rows(INSTALLATION)
    }
    resolved = []
    for written in plan.items:
        binding = parse_build_item(written, workspace=plan.workspace)
        item = binding.item
        if item.item_type != WAREHOUSE:
            raise CommandError(
                f"mirror does not yet mirror {item}: only a Warehouse item can "
                "be mirrored. Build a Lakehouse item into its target with "
                "'weaver build --item ITEM=TYPE/NAME'."
            )
        registered = {
            identity: document
            for identity, document in catalogue.registered.items()
            if identity.item == item
        }
        resolved.append(
            MirrorItem(
                item=item,
                source_target=_installed_target(installed, item),
                destination=binding.target.item.name,
                relations=borrowable(registered),
                programmables=executable(registered),
            )
        )
    return tuple(resolved)


def _installed_target(installed: Mapping[tuple, str], item) -> str:
    """Where the source catalogue says this item is installed."""

    target = installed.get((item.item_type, item.item_name))
    if not target:
        raise CommandError(
            f"the catalogue records no installation for {item}, so there is "
            "nothing to mirror. Fork a catalogue that has one, or build the "
            "item first."
        )
    return target


def _refuse_unsafe(resolved: ResolvedMirror) -> None:
    """Refuse a run that would empty a Warehouse it reads from.

    A named destination is emptied, and its contents are the caller's to
    discard. The exception is a Warehouse this run needs: the catalogue it
    copies state from, and the target each item borrows its rows through.
    """

    read = {resolved.source.name.casefold(): str(resolved.source)}
    for each in resolved.items:
        read.setdefault(
            each.source_target.casefold(), f"{CATALOGUE_KIND}/{each.source_target}"
        )
    for each in resolved.items:
        source = read.get(each.destination.casefold())
        if source is not None:
            raise CommandError(
                f"mirror reads {source} and would empty it for {each.item}. "
                "Name a destination this run does not read from."
            )


# --- doing it -----------------------------------------------------------------


def _wipe_destination(
    workspace: Workspace, destination: CatalogueRef, *, session
) -> str:
    """Empty the destination Warehouse, through the ordinary wipe."""

    from .wipe import wipe

    target = f"{CATALOGUE_KIND}/{destination.name}"
    with session.step(f"Empty {target}"):
        # Named, so the wipe unbinds claims from the catalogue being emptied
        # rather than whichever the session holds.
        wipe(
            target,
            session=session,
            workspace=workspace.workspace,
            catalogue=workspace.catalogue,
        )
    return target


def _rebuild_catalogue(workspace: Workspace, *, session) -> None:
    """Build the ``_`` schema, through an ordinary build bound to nothing else.

    The catalogue's tables are declared in :mod:`weaver.fragments` and composed
    into every repository, so a build over an empty tree builds the catalogue
    and only the catalogue, constraints and Registry rows included.
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
    borrowed: bool,
    session,
) -> dict[str, int]:
    """Copy the source catalogue's rows in, then read back what landed."""

    from ..catalogue.fork import copied_tables, fork_statements
    from ..targets import WarehouseTarget

    sql = session.sql_executor(WarehouseTarget(destination.item), workspace=workspace)
    with session.step(f"Copy catalogue state from {source}"):
        sql.execute_script(
            "\n".join(fork_statements(source_catalogue=source.name, borrowed=borrowed))
        )
    with session.step("Count what was copied"):
        rows = sql.query(_count_statement(borrowed=borrowed))
    counted = {str(row["Table"]): int(row["Rows"]) for row in rows}
    return {
        table.name: counted.get(table.name, 0)
        for table in copied_tables(borrowed=borrowed)
    }


def _count_statement(*, borrowed: bool) -> str:
    """One statement counting every copied table, so the read is one crossing."""

    from ..catalogue.fork import copied_tables
    from ..catalogue.tables import CATALOGUE_SCHEMA
    from ..catalogue.tsql import identifier, literal

    return "\nunion all\n".join(
        f"select {literal(table.name)} as [Table], count(*) as [Rows] "
        f"from {identifier(CATALOGUE_SCHEMA)}.{identifier(table.name)}"
        for table in copied_tables(borrowed=borrowed)
    )


def _wipe_target(workspace: Workspace, each: MirrorItem, *, session) -> str:
    """Empty the Warehouse a mirror is about to be built in."""

    from .wipe import wipe

    with session.step(f"Empty {each.target}"):
        wipe(
            each.target,
            session=session,
            workspace=workspace.workspace,
            catalogue=workspace.catalogue,
        )
    return each.target


def _mirror_item(workspace: Workspace, each: MirrorItem, *, session) -> dict:
    """Point one Warehouse item at another target's rows."""

    from ..catalogue.borrow import borrow_statements
    from ..targets import ItemRef, WarehouseTarget

    sql = session.sql_executor(
        WarehouseTarget(ItemRef(each.destination)), workspace=workspace
    )
    with session.step(f"Borrow {each.item} from {each.source_target}"):
        for statement in borrow_statements(
            each.relations,
            source_target=each.source_target,
            catalogue_name=workspace.catalogue_item.name,
        ):
            sql.execute(statement)

    code = _copy_programmables(workspace, each, sql=sql, session=session)
    _record_borrowed(workspace, each, session=session)
    # Last, so a run that fails before here leaves the item installed where it
    # was rather than bound to a half-built mirror.
    _switch_installation(workspace, each, session=session)
    return {
        "source": each.source_target,
        "target": each.destination,
        "relations": len(each.relations),
        "programmables": code,
    }


def _copy_programmables(workspace: Workspace, each: MirrorItem, *, sql, session) -> int:
    """Copy the source's procedures and functions, in every schema.

    ``_`` included: the copied Registry certifies ``_.[Load X.Y]`` and
    ``_.[Test X.Y]``, and ``weaver test`` dispatches those by name.
    """

    from ..catalogue.borrow import (
        missing_programmables,
        programmable_statements,
        schema_statements,
    )
    from ..targets import ItemRef, WarehouseTarget

    source_sql = session.sql_executor(
        WarehouseTarget(ItemRef(each.source_target)), workspace=workspace
    )
    rows = tuple(
        source_sql.query(
            "select schema_name(o.schema_id) as schema_name, o.name as object_name, "
            "m.definition as definition "
            "from sys.sql_modules as m "
            "join sys.objects as o on o.object_id = m.object_id "
            "where o.is_ms_shipped = 0 and o.type in (N'P', N'FN', N'IF', N'TF')"
        )
    )
    absent = missing_programmables(
        each.programmables,
        tuple(f"{row['schema_name']}.{row['object_name']}" for row in rows),
    )
    if absent:
        raise CommandError(
            f"mirror cannot give {each.target} the code {each.item} is certified "
            f"with: {CATALOGUE_KIND}/{each.source_target} does not hold "
            + ", ".join(identity.object_id.qualified for identity in absent)
            + ". Build the source item, so its catalogue and its Warehouse agree."
        )
    statements = programmable_statements(str(row["definition"]) for row in rows)
    if not statements:
        return 0
    with session.step(f"Copy {len(statements)} programmable(s)"):
        # A schema holding only procedures has no borrowed relation to have
        # created it.
        for statement in schema_statements(str(row["schema_name"]) for row in rows):
            sql.execute(statement)
        for statement in statements:
            sql.execute(statement)
    return len(statements)


def _record_borrowed(workspace: Workspace, each: MirrorItem, *, session) -> None:
    """Write the ``_.Mirror`` rows, into the catalogue rather than the target."""

    from ..catalogue.borrow import record_statements
    from ..targets import WarehouseTarget

    statements = record_statements(
        each.relations,
        source_workspace=workspace.workspace,
        source_target=each.source_target,
    )
    if not statements:
        return
    sql = session.sql_executor(
        WarehouseTarget(workspace.catalogue_item), workspace=workspace
    )
    with session.step("Record what is borrowed"):
        for statement in statements:
            sql.execute(statement)


def _switch_installation(workspace: Workspace, each: MirrorItem, *, session) -> None:
    """Bind the item to its new target."""

    from .. import __version__
    from ..catalogue.render import InstallationScope, render_merge
    from ..catalogue.state import catalogue_in
    from ..catalogue.tables import INSTALLATION
    from ..targets import WarehouseTarget

    item = each.item
    with catalogue_in(workspace) as catalogue:
        existing = [
            row
            for row in catalogue.table_rows(INSTALLATION)
            if str(row.get("item_type")) == item.item_type
            and str(row.get("item_name")) == item.item_name
        ]
    row = dict(existing[0]) if existing else {}
    row.update(
        {
            "item_type": item.item_type,
            "item_name": item.item_name,
            "target_name": each.destination,
            "weaver_version": __version__,
            "signature": str(row.get("signature") or ""),
        }
    )
    statement = render_merge(
        INSTALLATION,
        [row],
        scope=InstallationScope(item.item_type, item.item_name),
    )
    sql = session.sql_executor(
        WarehouseTarget(workspace.catalogue_item), workspace=workspace
    )
    with session.step(f"Bind {item} to {each.destination}"):
        sql.execute(statement)
