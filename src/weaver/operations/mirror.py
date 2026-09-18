"""Mirror a catalogue and selected items into another catalogue.

``mirror`` identifies the source catalogue. ``catalogue`` identifies the
destination. Weaver validates the plan, empties the destination targets, copies
the catalogue state and mirrors the selected items.

See https://docs.weaverstack.dev/core-concepts/catalogue/.
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
        return self.target, str(self.source)

    def __str__(self) -> str:
        return f"{self.source} into {self.destination}"


@dataclass(frozen=True)
class MirrorItem:
    item: object
    source_target: str
    destination: str
    relations: tuple = ()
    programmables: tuple = ()
    #: Recorded ``_.Shortcut`` rows that Weaver recreates in the destination.
    shortcuts: tuple = ()

    @property
    def kind(self) -> str:
        return self.item.item_type

    @property
    def target(self) -> str:
        return f"{self.kind}/{self.destination}"

    @property
    def source(self) -> str:
        return f"{self.kind}/{self.source_target}"

    @property
    def mapping(self) -> tuple[str, str]:
        return self.target, self.source


@dataclass(frozen=True)
class ResolvedMirror:
    plan: MirrorPlan
    #: Whether the source catalogue holds a ``_.Mirror``.
    borrowed: bool = False
    items: tuple[MirrorItem, ...] = ()
    #: Final physical target for each logical item. Resolution completes before
    #: any writes so shortcut rebuilding does not depend on item order.
    bindings: Mapping[object, str] = field(default_factory=dict)

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
        """Return destination-source pairs in physical wipe order."""

        return (self.plan.mapping, *(item.mapping for item in self.items))

    @property
    def wiped(self) -> tuple[str, ...]:
        return tuple(target for target, _source in self.mappings)

    def describe(self) -> str:
        lines = [
            f"Workspace              {self.workspace.workspace}",
            f"Source catalogue       {self.source}",
            f"Destination catalogue  {self.destination}",
            "Targets",
        ]
        lines.extend(f"  {item.item} → {item.target}" for item in self.items)
        if not self.items:
            lines.append("  none")
        return "\n".join(lines)

    def __str__(self) -> str:
        return str(self.plan)


@dataclass(frozen=True)
class MirrorResult:
    workspace: str
    source_catalogue: str
    destination_catalogue: str
    wiped: tuple[str, ...]
    copied: Mapping[str, int] = field(default_factory=dict)
    #: Historical catalogue tables that Weaver rebuilds without copying rows.
    uncopied: tuple[str, ...] = ()
    items: tuple[str, ...] = ()
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
    """Resolve source, destination and selected items without reading Fabric."""

    if items is not None and no_item:
        raise CommandError("mirror takes items or no_item=True, not both")

    base = operation_workspace(
        "mirror",
        workspace=workspace,
        environment=environment,
        workspace_config=workspace_config,
        session=session,
        # ``catalogue:`` changes role when ``mirror:`` is present.
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
    """Validate the plan against the source catalogue without changing Fabric."""

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
    """Settle a plan against the source catalogue; ``None`` selects no items."""

    items = _resolved_items(plan, catalogue) if plan.items else ()
    resolved = ResolvedMirror(
        plan=plan,
        borrowed=borrowed,
        items=items,
        bindings=_final_bindings(plan, catalogue, items),
    )
    _refuse_unsafe(resolved)
    return resolved


def _final_bindings(plan: MirrorPlan, catalogue, items) -> dict:
    """Resolve shortcut targets before any destination is emptied."""

    from ..catalogue.builtin import BUILTIN_ITEM
    from ..catalogue.tables import INSTALLATION
    from ..declaration.model import WeaverItemId

    bound = {
        WeaverItemId(
            str(row.get("item_type") or ""), str(row.get("item_name") or "")
        ): str(row.get("target_name") or "")
        for row in (catalogue.table_rows(INSTALLATION) if catalogue else ())
    }
    # The fork excludes the destination catalogue's build-owned Installation row.
    bound[BUILTIN_ITEM] = plan.destination.name
    bound.update({each.item: each.destination for each in items})
    return {item: name for item, name in bound.items() if name}


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
    """Fork another catalogue's installed state into this workspace.

    ``items`` are the logical items to rebind, written the way ``build`` writes
    them: ``Warehouse/Model`` or ``Warehouse/Model=Warehouse/Model_Dev``. Naming
    none selects every item the workspace configuration declares, and
    ``no_item=True`` selects none, which forks the catalogue and stops.

    ``mirror`` names the source catalogue and ``catalogue`` the destination, the
    same roles as workspace configuration. A
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
                mirrored[str(each.item)] = _mirror_item(
                    resolved, each, bindings=forking.bindings, session=opened
                )

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


def _resolved_pair(
    base: Workspace, *, catalogue: str | None, mirror: str | None
) -> tuple[CatalogueRef, CatalogueRef]:
    """Resolve the source and destination catalogue roles.

    In workspace configuration, ``catalogue:`` identifies the source unless
    ``mirror:`` is set. With ``mirror:``, ``catalogue:`` identifies the
    destination.
    """

    configured = base.catalogue_ref if base.catalogue else None
    named_source = CatalogueRef.parse(mirror) if mirror is not None else None
    named_destination = (
        _local_destination(catalogue, base) if catalogue is not None else None
    )

    source = named_source or base.mirror or configured
    if source is None:
        raise CommandError(
            "mirror needs a source catalogue. Name it with "
            f"--mirror {CATALOGUE_KIND}/<name>, or set mirror: in workspace "
            "configuration."
        )

    destination = named_destination
    if destination is None and base.mirror is not None:
        destination = configured
    if destination is None:
        raise CommandError(
            "mirror needs a destination catalogue. Use "
            f"--catalogue {CATALOGUE_KIND}/<name>."
        )
    return source, destination


def _local_destination(catalogue: str, base: Workspace) -> CatalogueRef:
    parsed = CatalogueRef.parse(catalogue)
    if not parsed.is_local_to(base.workspace):
        raise CommandError(
            f"{parsed} is in workspace {parsed.owner(base.workspace)}. The "
            f"destination catalogue must be in workspace {base.workspace}."
        )
    return CatalogueRef(workspace=base.workspace, name=parsed.name)


def _refuse_unusable_pair(
    source: CatalogueRef, destination: CatalogueRef, base: Workspace
) -> None:
    if not source.is_local_to(base.workspace):
        raise CommandError(
            f"Source catalogue {source} must be in workspace {base.workspace}. "
            f"Use a catalogue in {base.workspace}."
        )
    if source.name.casefold() == destination.name.casefold():
        raise CommandError(
            f"{destination} is both the source and destination catalogue. mirror "
            "empties the destination before copying catalogue state. Use a "
            "different destination with "
            f"--catalogue {CATALOGUE_KIND}/<name>."
        )


def _selected_items(items, no_item: bool, workspace: Workspace) -> tuple[str, ...]:
    if no_item:
        return ()
    if items is None:
        return tuple(str(item) for item in workspace.configured_items)
    return (items,) if isinstance(items, str) else tuple(items)


def _prove_source(plan: MirrorPlan, *, session) -> bool:
    """Validate the source catalogue and report whether it holds ``_.Mirror``."""

    from ..catalogue.fork import FORKED_TABLES
    from ..catalogue.tables import MIRROR

    found = _source_catalogue_tables(plan, session=session)
    if not found:
        raise CommandError(
            f"{plan.source} does not contain a Weaver catalogue. Name the "
            "Warehouse that holds the Weaver catalogue."
        )
    missing = [
        table.name for table in FORKED_TABLES if table.name.casefold() not in found
    ]
    if missing:
        raise CommandError(
            f"The catalogue in {plan.source} is not compatible with this Weaver "
            "version. Build against it with this version before mirroring."
        )
    return MIRROR.name.casefold() in found


def _source_catalogue_tables(plan: MirrorPlan, *, session) -> set[str]:
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
    return replace(plan.workspace, catalogue=f"{CATALOGUE_KIND}/{plan.source.name}")


def _resolved_items(plan: MirrorPlan, catalogue) -> tuple[MirrorItem, ...]:
    """Resolve selected items from the intact source catalogue."""

    from ..build_bundle.targets import parse_build_item
    from ..catalogue.borrow import borrowable, executable
    from ..catalogue.tables import INSTALLATION
    from ..installed import installed_shortcuts

    installed = {
        (str(row.get("item_type")), str(row.get("item_name"))): str(
            row.get("target_name") or ""
        )
        for row in catalogue.table_rows(INSTALLATION)
    }
    recorded = installed_shortcuts(catalogue)
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
                shortcuts=tuple(
                    shortcut
                    for shortcut in recorded
                    if shortcut.destination.item == item
                ),
            )
        )
    _refuse_repeated_items(resolved)
    return _in_producer_order(resolved, recorded)


def _refuse_repeated_items(items: Sequence[MirrorItem]) -> None:
    seen: dict[str, MirrorItem] = {}
    for each in items:
        name = str(each.item)
        first = seen.get(name)
        if first is not None:
            raise CommandError(
                f"mirror was given {name} twice, for {first.target} and "
                f"{each.target}. Name each item once."
            )
        seen[name] = each


def _in_producer_order(items, shortcuts) -> tuple[MirrorItem, ...]:
    """Order selected items with shortcut producers before consumers.

    Recorded shortcuts, not command-line order, establish dependencies. Unrelated
    items retain their requested order.
    """

    from ..errors import GraphError
    from ..graph import Graph

    by_name = {str(each.item): each for each in items}
    given = {name: position for position, name in enumerate(by_name)}
    edges = []
    for shortcut in shortcuts:
        if not shortcut.is_logical or shortcut.target_item is None:
            continue
        producer, consumer = str(shortcut.target_item), str(shortcut.destination.item)
        if producer != consumer and producer in by_name and consumer in by_name:
            edges.append((producer, consumer))
    try:
        ordered = Graph(by_name, edges).order(key=given.get)
    except GraphError as exc:
        raise CommandError(
            f"mirror cannot order the items it was given: {exc}. A recreated "
            "shortcut must follow its producer. Mirror the items in separate runs."
        ) from exc
    return tuple(by_name[name] for name in ordered)


def _installed_target(installed: Mapping[tuple, str], item) -> str:
    target = installed.get((item.item_type, item.item_name))
    if not target:
        raise CommandError(
            f"the catalogue records no installation for {item}, so there is "
            "nothing to mirror. Fork a catalogue that has one, or build the "
            "item first."
        )
    return target


def _refuse_mirrored_source(resolved: ResolvedMirror) -> None:
    """Enforce the one-hop limit when rebinding items.

    ``_.Mirror`` records one hop. Rebinding from a mirrored catalogue would put
    the rows two catalogues away. A catalogue-only fork may copy the rows as they
    stand.
    """

    if not resolved.borrowed or not resolved.items:
        return
    named = ", ".join(str(each.item) for each in resolved.items)
    raise CommandError(
        f"{resolved.source} is already a mirrored catalogue. Mirror {named} from "
        "their source catalogue."
    )


def _refuse_unsafe(resolved: ResolvedMirror) -> None:
    """Require distinct physical outputs that do not overlap any input."""

    _refuse_mirrored_source(resolved)
    for each in resolved.items:
        _recreatable(each, bindings=resolved.bindings)

    # Physical identity is kind plus name; different item kinds may share a name.
    # The destination catalogue is also an output and must remain distinct.
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


def _wipe_destination(
    workspace: Workspace, destination: CatalogueRef, *, session
) -> str:
    """Empty only the destination Warehouse, not its recorded installation."""

    from .wipe import PHYSICAL_ONLY, wipe

    target = f"{CATALOGUE_KIND}/{destination.name}"
    wipe(
        target,
        session=session,
        workspace=workspace.workspace,
        catalogue=workspace.catalogue,
        catalogue_action=PHYSICAL_ONLY,
    )
    return target


def _rebuild_catalogue(workspace: Workspace, *, session) -> None:
    """Build the catalogue from the standard fragments and an empty source.

    This builds only the catalogue, including constraints and Registry rows.
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
    with session.step("Preparing destination catalogue"):
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
                    present_selection=False,
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
    from ..catalogue.fork import copied_tables, fork_statements
    from ..targets import WarehouseTarget

    sql = session.sql_executor(WarehouseTarget(destination.item), workspace=workspace)
    with session.step("Forking catalogue"):
        sql.execute_script(
            "\n".join(fork_statements(source_catalogue=source.name, borrowed=borrowed))
        )
    with session.step("Reading catalogue copy result"):
        rows = sql.query(_count_statement(borrowed=borrowed))
    counted = {str(row["Table"]): int(row["Rows"]) for row in rows}
    return {
        table.name: counted.get(table.name, 0)
        for table in copied_tables(borrowed=borrowed)
    }


def _count_statement(*, borrowed: bool) -> str:
    """Count all copied tables with one Warehouse query."""

    from ..catalogue.fork import copied_tables
    from ..catalogue.tables import CATALOGUE_SCHEMA
    from ..catalogue.tsql import identifier, literal

    return "\nunion all\n".join(
        f"select {literal(table.name)} as [Table], count(*) as [Rows] "
        f"from {identifier(CATALOGUE_SCHEMA)}.{identifier(table.name)}"
        for table in copied_tables(borrowed=borrowed)
    )


def _wipe_target(workspace: Workspace, each: MirrorItem, *, session) -> str:
    """Empty one physical target without changing catalogue claims."""

    from .wipe import PHYSICAL_ONLY, wipe

    wipe(
        each.target,
        session=session,
        workspace=workspace.workspace,
        catalogue=workspace.catalogue,
        catalogue_action=PHYSICAL_ONLY,
    )
    return each.target


def _mirror_item(workspace: Workspace, each: MirrorItem, *, bindings, session) -> dict:
    from ..declaration.model import LAKEHOUSE

    if each.kind == LAKEHOUSE:
        return _mirror_lakehouse_item(
            workspace, each, bindings=bindings, session=session
        )
    return _mirror_warehouse_item(workspace, each, bindings=bindings, session=session)


def _mirror_warehouse_item(
    workspace: Workspace, each: MirrorItem, *, bindings, session
) -> dict:
    from ..catalogue.borrow import borrow_statements, schema_statements
    from ..catalogue.shortcuts import schemas_of, view_statement
    from ..targets import ItemRef, WarehouseTarget

    sql = session.sql_executor(
        WarehouseTarget(ItemRef(each.destination)), workspace=workspace
    )
    with session.step(f"Mirroring {each.item}"):
        for statement in borrow_statements(
            each.relations,
            source_target=each.source_target,
            catalogue_name=workspace.catalogue_item.name,
        ):
            sql.execute(statement)

    pointers = _recreatable(each, bindings=bindings)
    if pointers:
        with session.step(f"Recreating shortcuts in {each.target}"):
            for statement in schema_statements(schemas_of(pointers)):
                sql.execute(statement)
            for pointer in pointers:
                sql.execute(view_statement(pointer))

    code = _copy_programmables(workspace, each, sql=sql, session=session)
    _record_borrowed(workspace, each, session=session)
    # Last, so a run that fails before here leaves the item installed where it
    # was rather than bound to a half-built mirror.
    _switch_installation(workspace, each, session=session)
    return {
        "source": each.source,
        "target": each.target,
        "relations": len(each.relations),
        "pointers": len(pointers),
        "programmables": code,
    }


def _recreatable(each: MirrorItem, *, bindings) -> tuple:
    """Resolve shortcuts and reject unsupported forms before physical writes."""

    from ..catalogue.shortcuts import UnresolvedShortcut, recreatable, unsupported

    try:
        found = recreatable(each.shortcuts, item=each.item, bindings=bindings)
    except UnresolvedShortcut as exc:
        raise CommandError(
            f"mirror cannot reconstruct shortcuts in {each.target}: {exc}. Mirror "
            "the target item in the same run, or build this item instead."
        ) from exc
    for pointer in found:
        why = unsupported(pointer, kind=each.kind)
        if why is not None:
            raise CommandError(
                f"mirror cannot recreate {pointer.destination} in {each.target}: {why}"
            )
    return found


def _mirror_lakehouse_item(
    workspace: Workspace, each: MirrorItem, *, bindings, session
) -> dict:
    """Mirror a Lakehouse's data, shortcuts and deployed load tree.

    Stored data uses OneLake shortcuts; source views use four-part wrapper views.
    The load tree is local because Spark imports it from the destination item.
    """

    from ..build_bundle.executors.shortcut import await_addressable
    from ..catalogue.borrow import wrapper_view_statement
    from ..targets import ItemRef

    resolver = session.resolver(workspace)
    destination = resolver.spark_destination(ItemRef(each.destination))
    source = resolver.spark_destination(ItemRef(each.source_target))

    # Create and await all destination shortcuts as one state transition.
    pointers = _recreatable(each, bindings=bindings)
    shortcuts = (
        _pointer_requests(workspace, each, session=session)
        + _surface_requests(workspace, each, session=session)
        + _recreated_requests(workspace, each, pointers, session=session)
    )
    if shortcuts:
        with session.step(f"Mirroring {each.item}"):
            resolver.create_onelake_shortcuts(ItemRef(each.destination), shortcuts)
        with session.step("Wait for the shortcuts to become readable"):
            # A table shortcut is ready only when its relation and Delta path read.
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
        with session.step(f"Creating mirror views in {each.target}"):
            # A schema containing only views has no shortcut to create it.
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
        "pointers": len(pointers),
        "views": len(wrapped),
        "files": files,
    }


def _spark_sql(workspace: Workspace, *, session):
    def run(statement: str, *, exact_case: bool = False):
        return session.execute_spark_sql(
            statement, exact_case=exact_case, workspace=workspace
        )

    return run


def _pointer_requests(workspace: Workspace, each: MirrorItem, *, session) -> tuple:
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


def _recreated_requests(
    workspace: Workspace, each: MirrorItem, pointers, *, session
) -> tuple:
    """Build requests for recorded shortcuts using physical target paths.

    Logical targets use the run's final bindings. Physical targets retain their
    recorded workspace and item.
    """

    from ..build_bundle.shortcut_sources import stored_path
    from ..catalogue.shortcuts import shortcut_request

    if not pointers:
        return ()
    resolver = session.resolver(workspace)
    store = session.store(workspace)
    requests = []
    for pointer in pointers:
        item = resolver.external_item(
            pointer.target_name,
            item_type=pointer.shortcut.target_item.item_type,
            workspace=pointer.target_workspace,
        )
        requests.append(
            shortcut_request(
                pointer,
                source=item,
                source_path=stored_path(
                    resolver.external_root(item),
                    pointer.source_components,
                    store=store,
                    what=(
                        f"mirror recreates {pointer.destination}, which reads "
                        f"{pointer.target_name}"
                    ),
                ),
            )
        )
    return tuple(requests)


def _surface_requests(workspace: Workspace, each: MirrorItem, *, session) -> tuple:
    """Build the standard ``_`` surface over the destination catalogue."""

    from ..catalogue.borrow import surface_shortcuts
    from ..fabric.resources import WAREHOUSE as WAREHOUSE_ITEM

    resolver = session.resolver(workspace)
    catalogue = resolver.external_item(
        workspace.catalogue_item.name, item_type=WAREHOUSE_ITEM
    )
    return surface_shortcuts(each.item, catalogue=catalogue)


#: Deployed code is copied; other ``Files/_`` state remains destination-owned.
LOAD_TREE = ("Files", "_", "Load")


def _copy_load_tree(workspace: Workspace, each: MirrorItem, *, session) -> int:
    """Replace the destination's local load tree with the source's deployed code."""

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
    with session.step(f"Copying load and test artefacts to {each.target}"):
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
    """Copy source procedures and functions, including those in ``_``.

    The copied Registry certifies load and test procedures that runtime dispatches
    by name.
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
            f"The source item {each.item} is missing deployed code: "
            + ", ".join(identity.object_id.qualified for identity in absent)
            + f". Build {each.item} before mirroring it."
        )
    statements = programmable_statements(str(row["definition"]) for row in rows)
    if not statements:
        return 0
    with session.step(f"Copying load and test artefacts to {each.target}"):
        # A schema containing only procedures has no borrowed relation to create it.
        for statement in schema_statements(str(row["schema_name"]) for row in rows):
            sql.execute(statement)
        for statement in statements:
            sql.execute(statement)
    return len(statements)


def _record_borrowed(workspace: Workspace, each: MirrorItem, *, session) -> None:
    """Write ``_.Mirror`` claims to the catalogue, not the borrowed target."""

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
    with session.step("Updating mirror records"):
        for statement in statements:
            sql.execute(statement)


def _switch_installation(workspace: Workspace, each: MirrorItem, *, session) -> None:
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
    with session.step(f"Updating the binding for {item}"):
        sql.execute(statement)


def mirrored_source(
    catalogue, *, workspace: Workspace, session, operation: str, tables, history=False
):
    """Return the source catalogue used for mirrored runtime state.

    Return ``None`` when ``_.Mirror`` is empty. Otherwise the workspace must name
    the source because copied load status is only a snapshot.
    """

    from ..catalogue.state import catalogue_for

    if not catalogue.mirrors:
        return None
    if workspace.mirror is None:
        raise CommandError(
            f"{operation} needs the source catalogue for mirrored objects. Set "
            f"mirror: {CATALOGUE_KIND}/<name> in workspace configuration."
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
    return replace(workspace, catalogue=f"{CATALOGUE_KIND}/{workspace.mirror.name}")
