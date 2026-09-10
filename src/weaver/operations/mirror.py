"""Forking an installed estate: the destination catalogue, then its items.

``mirror`` is the catalogue read from and ``catalogue`` the one written to, in
configuration and on the command alike. A run resolves, validates, empties,
builds the mirrors and records them.

See ``design/catalogue.md``.
"""

from __future__ import annotations

import tempfile
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping, Sequence

from ..errors import CommandError
from ..locations import Location
from ..workspaces import CATALOGUE_KIND, CatalogueRef, Workspace
from .workspace import operation_workspace

#: What separates a destination from the target it reads, in a described run.
#: ASCII, because a Windows console runs on the system codepage and a described
#: run is the last thing printed before a destination is emptied.
MIRRORS = "<-"


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

    @property
    def mapping(self) -> tuple[str, str]:
        """The catalogue this fork writes, and the one it reads."""

        return self.target, str(self.source)

    def __str__(self) -> str:
        return f"{self.source} into {self.destination}"


@dataclass(frozen=True)
class MirrorItem:
    """One selected item, with the target it mirrors and the one it fills."""

    item: object
    source_target: str
    destination: str
    relations: tuple = ()
    programmables: tuple = ()

    @property
    def kind(self) -> str:
        """The physical kind, which is the item's: a Lakehouse item deploys to one."""

        return self.item.item_type

    @property
    def target(self) -> str:
        return f"{self.kind}/{self.destination}"

    @property
    def source(self) -> str:
        return f"{self.kind}/{self.source_target}"

    @property
    def mapping(self) -> tuple[str, str]:
        """The target this item is mirrored into, and the one it reads."""

        return self.target, self.source


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
    def mappings(self) -> tuple[tuple[str, str], ...]:
        """Each target this run empties and what fills it, target first.

        The destination catalogue leads, being the first target emptied and the
        one the items are rebound through.
        """

        return (self.plan.mapping, *(item.mapping for item in self.items))

    @property
    def wiped(self) -> tuple[str, ...]:
        """Every physical target this run empties, in the order it empties them."""

        return tuple(target for target, _source in self.mappings)

    def describe(self) -> str:
        """One row per target this run empties, and what it will read."""

        width = max(len(target) for target, _source in self.mappings)
        return "\n".join(
            f"  {target.ljust(width)}  {MIRRORS} {source}"
            for target, source in self.mappings
        )

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
    #: What each rebound item now mirrors, by item.
    mirrored: Mapping[str, Mapping] = field(default_factory=dict)
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
            "mirrored": {item: dict(each) for item, each in self.mirrored.items()},
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
        mirrored=mirrored,
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
                relations=borrowable(registered, kind=item.item_type),
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
    """Refuse a plan that contradicts itself.

    Naming a destination says its contents are disposable, so nothing here asks
    what is in one. What it asks is whether the plan is coherent. Every output
    is emptied and rebuilt on its own, so each needs a physical identity of its
    own, and none of them may be something the run reads.
    """

    # Keyed on kind and name, which is a physical item's identity: a Lakehouse
    # and a Warehouse may share a display name. The destination catalogue is an
    # output like any other, and an item rebuilt over it would take the rows
    # this run has just copied in.
    emptied: dict[tuple[str, str], str] = {}
    whose: dict[tuple[str, str], str] = {}
    for kind, name, label, claimant in (
        (
            CATALOGUE_KIND,
            resolved.destination.name,
            resolved.plan.target,
            "the destination catalogue",
        ),
        *(
            (each.kind, each.destination, each.target, str(each.item))
            for each in resolved.items
        ),
    ):
        key = (kind, name.casefold())
        if key in emptied:
            raise CommandError(
                f"mirror would empty {label} for both {whose[key]} and "
                f"{claimant}. Each is rebuilt on its own, so give each a "
                "destination of its own."
            )
        emptied[key] = label
        whose[key] = claimant

    read = {(CATALOGUE_KIND, resolved.source.name.casefold()): str(resolved.source)}
    for each in resolved.items:
        read.setdefault((each.kind, each.source_target.casefold()), each.source)

    collision = sorted(set(read) & set(emptied))
    if collision:
        raise CommandError(
            "mirror reads and empties "
            + ", ".join(emptied[name] for name in collision)
            + " in the same run. Name a destination this run does not read from."
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
    # One instant for the whole fork: this is when the destination's objects are
    # published, and every copied row carries it.
    published = datetime.now(timezone.utc).replace(tzinfo=None)
    with session.step(f"Copy catalogue state from {source}"):
        sql.execute_script(
            "\n".join(
                fork_statements(
                    source_catalogue=source.name,
                    published=published,
                    borrowed=borrowed,
                )
            )
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
    """Point one item at another target's data, however its kind mirrors."""

    from ..declaration.model import LAKEHOUSE

    if each.kind == LAKEHOUSE:
        return _mirror_lakehouse_item(workspace, each, session=session)
    return _mirror_warehouse_item(workspace, each, session=session)


def _mirror_warehouse_item(workspace: Workspace, each: MirrorItem, *, session) -> dict:
    """Point one Warehouse item at another target's rows."""

    from ..catalogue.borrow import borrow_statements
    from ..targets import ItemRef, WarehouseTarget

    sql = session.sql_executor(
        WarehouseTarget(ItemRef(each.destination)), workspace=workspace
    )
    with session.step(f"Mirror {each.item} from {each.source}"):
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
        "source": each.source,
        "target": each.target,
        "relations": len(each.relations),
        "programmables": code,
    }


def _mirror_lakehouse_item(workspace: Workspace, each: MirrorItem, *, session) -> dict:
    """Point one Lakehouse item at another target's tables, folders and views.

    Storage is borrowed through OneLake shortcuts, and a source view through a
    view of Weaver's own over the source's four-part name. The deployed load
    tree is copied, because a run imports it where Spark is.
    """

    from ..build_bundle.executors.shortcut import await_addressable
    from ..catalogue.borrow import wrapper_view_statement
    from ..targets import ItemRef

    resolver = session.resolver(workspace)
    destination = resolver.spark_destination(ItemRef(each.destination))
    source = resolver.spark_destination(ItemRef(each.source_target))

    # The borrowed data and the ``_`` surface in one submission: both are
    # shortcuts into this Lakehouse, and both are waited on together.
    shortcuts = _pointer_requests(workspace, each, session=session) + _surface_requests(
        workspace, each, session=session
    )
    if shortcuts:
        with session.step(f"Mirror {len(shortcuts)} object(s) from {each.source}"):
            resolver.create_onelake_shortcuts(ItemRef(each.destination), shortcuts)
        with session.step("Wait for the shortcuts to become readable"):
            # The build's own readiness rule: a table shortcut is not finished
            # until its relation and its Delta path can both be read.
            await_addressable(
                shortcuts,
                destination=destination,
                location=resolver.lakehouse_spark_location(ItemRef(each.destination)),
                spark_sql=_spark_sql(workspace, session=session),
            )

    wrapped = tuple(
        wrapper_view_statement(borrowed, destination=destination, source=source)
        for borrowed in each.relations
        if not borrowed.is_pointer
    )
    if wrapped:
        with session.step(f"Wrap {len(wrapped)} source view(s)"):
            # Every schema, because one holding only views has no shortcut to
            # have created it.
            statements = [
                destination.create_schema_statement(schema)
                for schema in sorted({borrowed.schema for borrowed in each.relations})
            ]
            session.execute_spark_sql_batch(
                statements + list(wrapped), exact_case=True, workspace=workspace
            )

    files = _copy_load_tree(workspace, each, session=session)
    _record_borrowed(workspace, each, session=session)
    _switch_installation(workspace, each, session=session)
    return {
        "source": each.source,
        "target": each.target,
        "relations": len(each.relations),
        "shortcuts": len(shortcuts),
        "views": len(wrapped),
        "files": files,
    }


def _spark_sql(workspace: Workspace, *, session):
    """One Spark SQL statement, wherever this is running."""

    def run(statement: str, *, exact_case: bool = False):
        return session.execute_spark_sql(
            statement, exact_case=exact_case, workspace=workspace
        )

    return run


def _pointer_requests(workspace: Workspace, each: MirrorItem, *, session) -> tuple:
    """Each borrowed table and folder, addressed in the source's own spelling."""

    from ..build_bundle.shortcut_sources import stored_path
    from ..catalogue.borrow import pointer_shortcuts
    from ..fabric.resources import LAKEHOUSE as LAKEHOUSE_ITEM

    resolver = session.resolver(workspace)
    item = resolver.external_item(each.source_target, item_type=LAKEHOUSE_ITEM)
    root = resolver.external_root(item)
    store = session.store(workspace)

    def path_of(borrowed) -> str:
        return stored_path(
            root,
            (borrowed.area, borrowed.schema, borrowed.name),
            store=store,
            what=f"mirror reads {each.source} for {borrowed.identity}",
        )

    return pointer_shortcuts(each.relations, source=item, path_of=path_of)


def _surface_requests(workspace: Workspace, each: MirrorItem, *, session) -> tuple:
    """The standard ``_`` surface, reading this workspace's own catalogue.

    A mirrored Lakehouse runs the code copied into it, and that code reads
    Weaver state through the same surface a built one has.
    """

    from ..catalogue.borrow import surface_shortcuts
    from ..fabric.resources import WAREHOUSE as WAREHOUSE_ITEM

    resolver = session.resolver(workspace)
    catalogue = resolver.external_item(
        workspace.catalogue_item.name, item_type=WAREHOUSE_ITEM
    )
    return surface_shortcuts(each.item, catalogue=catalogue)


#: The one part of ``Files/_`` a mirror copies. Everything else under it is
#: state the destination's own runs write.
LOAD_TREE = ("Files", "_", "Load")


def _copy_load_tree(workspace: Workspace, each: MirrorItem, *, session) -> int:
    """Copy the source's deployed load tree, and remove what it no longer holds.

    The data is borrowed and the code is local: a run imports these modules
    where Spark is, from the item it is running against.
    """

    from ..fabric.resources import LAKEHOUSE as LAKEHOUSE_ITEM

    resolver = session.resolver(workspace)
    store = session.store(workspace)
    source = resolver.external_root(
        resolver.external_item(each.source_target, item_type=LAKEHOUSE_ITEM)
    ).join(*LOAD_TREE)
    destination = resolver.external_root(
        resolver.external_item(each.destination, item_type=LAKEHOUSE_ITEM)
    ).join(*LOAD_TREE)

    if not store.exists(source):
        return 0
    held = {
        entry.location.value[len(source.value) :].lstrip("/"): entry
        for entry in store.list(source, recursive=True)
        if not entry.is_directory
    }
    with session.step(f"Copy {len(held)} deployed file(s)"):
        for relative, entry in sorted(held.items()):
            store.write(
                destination.join(*relative.split("/")), store.read(entry.location)
            )
        stale = [
            entry.location
            for entry in (
                store.list(destination, recursive=True)
                if store.exists(destination)
                else ()
            )
            if not entry.is_directory
            and entry.location.value[len(destination.value) :].lstrip("/") not in held
        ]
        for location in stale:
            store.delete(location)
    return len(held)


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
    with session.step("Record the mirror source"):
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


# --- reading the catalogue an estate mirrors ---------------------------------


def mirrored_source(
    catalogue, *, workspace: Workspace, session, operation: str, tables, history=False
):
    """The catalogue this estate mirrors, read for its runtime state.

    ``None`` where ``_.Mirror`` holds nothing, which is every estate that forks
    nothing. Where it holds rows, the workspace has to name the catalogue this
    one was forked from: a fork copies ``_.LoadStatus`` at the moment it is
    made, and live state for a mirrored object is the source's.
    """

    from ..catalogue.state import catalogue_for

    if not catalogue.mirrors:
        return None
    if workspace.mirror is None:
        raise CommandError(
            f"{operation} reads live load state from the catalogue this estate "
            f"mirrors, and this workspace names none. "
            f"{len(catalogue.mirrors)} object(s) are recorded in "
            f"{CATALOGUE_KIND}/{workspace.catalogue_item.name} as mirrored. Set "
            f"mirror: {CATALOGUE_KIND}/<name> in workspace configuration, naming "
            "the catalogue this one was forked from."
        )
    if not workspace.mirror.is_local_to(workspace.workspace):
        raise CommandError(
            f"{operation} reads the mirrored catalogue in the workspace it runs "
            f"against, and mirror: names {workspace.mirror} in another one. Run "
            f"{operation} against workspace {workspace.mirror.owner(workspace.workspace)}, "
            "or name a catalogue in this workspace."
        )
    return catalogue_for(
        session,
        _mirrored_workspace(workspace),
        tables=tables,
        load_history=history,
    )


def _mirrored_workspace(workspace: Workspace) -> Workspace:
    """The same workspace, pointed at the catalogue it mirrors."""

    return replace(workspace, catalogue=f"{CATALOGUE_KIND}/{workspace.mirror.name}")
