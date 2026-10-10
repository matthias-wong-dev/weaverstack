"""Mirror a catalogue and selected items into another catalogue.

``mirror`` identifies the source catalogue. ``catalogue`` identifies the
destination. Weaver validates the plan, empties the destination targets, copies
the catalogue state and mirrors the selected items.

See https://docs.weaverstack.dev/core-concepts/catalogue/.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Mapping, Sequence

from ..errors import CommandError
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
    #: Whether the source catalogue holds the graph tables.
    graphed: bool = True
    items: tuple[MirrorItem, ...] = ()
    #: Final physical target for each logical item. Resolution completes before
    #: any writes so shortcut rebuilding does not depend on item order.
    bindings: Mapping[object, str] = field(default_factory=dict)
    #: Each selected item's source ``_.Installation`` row, which the fork copies
    #: and the mirror rebinds last.
    installations: Mapping[object, Mapping] = field(default_factory=dict)

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
        borrowed, graphed = _prove_source(plan, session=opened)
        catalogue = (
            catalogue_for(opened, _source_workspace(plan)) if plan.items else None
        )
        return resolve_mirror(plan, catalogue, borrowed=borrowed, graphed=graphed)


def resolve_mirror(
    plan: MirrorPlan, catalogue, *, borrowed: bool = False, graphed: bool = True
) -> ResolvedMirror:
    """Settle a plan against the source catalogue; ``None`` selects no items."""

    items = _resolved_items(plan, catalogue) if plan.items else ()
    resolved = ResolvedMirror(
        plan=plan,
        borrowed=borrowed,
        graphed=graphed,
        items=items,
        bindings=_final_bindings(plan, catalogue, items),
        installations=_installations(catalogue, items),
    )
    _refuse_unsafe(resolved)
    return resolved


def _installations(catalogue, items) -> dict:
    from ..catalogue.tables import INSTALLATION

    wanted = {(each.item.item_type, each.item.item_name): each.item for each in items}
    found = {}
    for row in catalogue.table_rows(INSTALLATION) if catalogue and items else ():
        key = (str(row.get("item_type")), str(row.get("item_name")))
        if key in wanted:
            found.setdefault(wanted[key], dict(row))
    return found


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
            from ..mirror_plan import mirror_mutation_plan

            with opened.step("Planning the mirror"):
                plan, payloads, mirrored = mirror_mutation_plan(forking, session=opened)
            with opened.step("Mirroring"):
                report = opened.execute_mutation(plan, payloads)
            failed = [
                f"{result.action_id}: {result.error or result.status}"
                for result in report.results
                if result.status != "succeeded"
            ]
            if failed:
                raise CommandError(
                    "the mirror did not complete, and no item was bound to an "
                    "incomplete destination: " + "; ".join(failed[:20])
                )
            copied = _copied(resolved, forking, session=opened)

    return MirrorResult(
        workspace=str(resolved.workspace),
        source_catalogue=str(forking.source),
        destination_catalogue=str(forking.destination),
        wiped=forking.wiped,
        copied=copied,
        uncopied=uncopied_table_names(),
        items=tuple(sorted(mirrored)),
        mirrored=mirrored,
    )


def _copied(workspace: Workspace, forking: ResolvedMirror, *, session) -> dict:
    """Count the rows the fork copied, in one query."""

    from ..catalogue.fork import copied_tables
    from ..targets import WarehouseTarget

    sql = session.sql_executor(
        WarehouseTarget(forking.destination.item), workspace=workspace
    )
    rows = sql.query(
        _count_statement(borrowed=forking.borrowed, graphed=forking.graphed)
    )
    counted = {str(row["Table"]): int(row["Rows"]) for row in rows}
    return {
        table.name: counted.get(table.name, 0)
        for table in copied_tables(borrowed=forking.borrowed, graphed=forking.graphed)
    }


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


def _prove_source(plan: MirrorPlan, *, session) -> tuple[bool, bool]:
    """Validate the source catalogue.

    Returns whether it holds ``_.Mirror`` and whether it holds every optional
    table.
    """

    from ..catalogue.fork import FORKED_TABLES, OPTIONAL_TABLES
    from ..catalogue.tables import MIRROR

    found = _source_catalogue_tables(plan, session=session)
    if not found:
        raise CommandError(
            f"{plan.source} does not contain a Weaver catalogue. Name the "
            "Warehouse that holds the Weaver catalogue."
        )
    missing = [
        table.name
        for table in FORKED_TABLES
        if table not in OPTIONAL_TABLES and table.name.casefold() not in found
    ]
    if missing:
        raise CommandError(
            f"The catalogue in {plan.source} is not compatible with this Weaver "
            "version. Build against it with this version before mirroring."
        )
    return MIRROR.name.casefold() in found, all(
        table.name.casefold() in found for table in OPTIONAL_TABLES
    )


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


def _count_statement(*, borrowed: bool, graphed: bool = True) -> str:
    """Count all copied tables with one Warehouse query."""

    from ..catalogue.fork import copied_tables
    from ..catalogue.tables import CATALOGUE_SCHEMA
    from ..catalogue.tsql import identifier, literal

    return "\nunion all\n".join(
        f"select {literal(table.name)} as [Table], count(*) as [Rows] "
        f"from {identifier(CATALOGUE_SCHEMA)}.{identifier(table.name)}"
        for table in copied_tables(borrowed=borrowed, graphed=graphed)
    )


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
