"""Forking an installed estate: the destination catalogue, then its items.

``mirror`` is the catalogue read from and ``catalogue`` the one written to, in
configuration and on the command alike. :class:`MirrorPlan` is that pair once
resolved: :func:`plan_mirror` produces it, :func:`check_mirror` proves the
source, and :func:`mirror` acts on it, so a prompt and a wipe name one pair.

A fork empties its destination and rebuilds it, so running it again does the
same work again. Installation is copied as it stands, and one
``weaver build --item Warehouse/Model=Warehouse/Model_Dev`` then moves that item
alone.

See ``design/catalogue.md``, including what a kept item's ``_`` surface still
addresses.
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
    #: The logical items whose physical binding this operation changed.
    items: tuple[str, ...] = ()
    #: What each mirrored item now borrows, by item.
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


def check_mirror(plan: MirrorPlan, *, session=None) -> bool:
    """Prove the source catalogue is there and readable, and say what it holds.

    Run before a confirmation and before anything is emptied, so a misspelled
    source fails while the destination is still intact. It reads and does not
    write: what it asks is whether the Warehouse resolves and whether its ``_``
    schema holds the tables a fork copies.

    Returns whether the source holds a ``_.Mirror``. Nothing declares that
    table, so a source that has never mirrored anything has none, and a fork of
    one has nothing borrowed to bring across.
    """

    from ..catalogue.fork import FORKED_TABLES
    from ..catalogue.tables import MIRROR
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
    return MIRROR.name.casefold() in found


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
    from ..catalogue.fork import uncopied_table_names
    from ..sessions.host import use_or_create_session

    resolved = forking.workspace
    bindings = _item_bindings(forking)
    with use_or_create_session(session, workspace=resolved) as opened:
        # Proved again for a supplied plan, because a caller holding one is not
        # proof it was checked. The source Warehouse is already resolved by
        # then, so the second read is one query.
        borrowed = check_mirror(forking, session=opened)
        with opened.task("Mirror", str(forking)):
            wiped = [_wipe_destination(resolved, forking.destination, session=opened)]
            _rebuild_catalogue(resolved, session=opened)
            copied = _copy_catalogue_state(
                resolved,
                forking.source,
                forking.destination,
                borrowed=borrowed,
                session=opened,
            )
            mirrored = {}
            for item, target in bindings:
                wiped.append(_wipe_target(resolved, target, session=opened))
                mirrored[str(item)] = _mirror_item(
                    resolved, item, target, session=opened
                )

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


# --- mirroring one item -------------------------------------------------------


def _item_bindings(plan: MirrorPlan) -> tuple[tuple, ...]:
    """Each selected item with the physical target it is mirrored into.

    ``build``'s grammar, so ``Warehouse/Model`` reads its destination from
    ``targets:`` and ``Warehouse/Model=Warehouse/Model_Dev`` names one.
    """

    from ..build_bundle.targets import parse_build_item
    from ..declaration.model import WAREHOUSE

    bindings = []
    for written in plan.items:
        binding = parse_build_item(written, workspace=plan.workspace)
        if binding.item.item_type != WAREHOUSE:
            raise CommandError(
                f"mirror does not yet mirror {binding.item}: only a Warehouse "
                "item can be mirrored. Build a Lakehouse item into its target "
                "with 'weaver build --item ITEM=TYPE/NAME'."
            )
        bindings.append((binding.item, binding.target.item))
    return tuple(bindings)


def _wipe_target(workspace: Workspace, target, *, session) -> str:
    """Empty the Warehouse a mirror is about to be built in."""

    from .wipe import wipe

    named = f"{CATALOGUE_KIND}/{target.name}"
    with session.step(f"Empty {named}"):
        wipe(
            named,
            session=session,
            workspace=workspace.workspace,
            catalogue=workspace.catalogue,
        )
    return named


def _mirror_item(workspace: Workspace, item, target, *, session) -> dict:
    """Point one Warehouse item at another target's rows.

    The destination is emptied first, so what stands in it afterwards is what
    this wrote. Installation moves last: until the Views are there, the item is
    still installed where it was.
    """

    from ..catalogue.borrow import borrow_statements, borrowable
    from ..catalogue.state import catalogue_in
    from ..targets import ItemRef, WarehouseTarget

    with catalogue_in(workspace) as catalogue:
        source_target = _installed_target(catalogue, item)
        registered = {
            identity: document
            for identity, document in catalogue.registered.items()
            if identity.item == item
        }
    relations = borrowable(registered)

    sql = session.sql_executor(WarehouseTarget(target), workspace=workspace)
    with session.step(f"Borrow {item} from {source_target}"):
        for statement in borrow_statements(
            relations,
            source_target=source_target,
            catalogue_name=workspace.catalogue_item.name,
        ):
            sql.execute(statement)

    code = _copy_programmables(
        workspace, ItemRef(source_target), sql=sql, session=session
    )
    _record_borrowed(workspace, relations, source_target=source_target, session=session)
    _switch_installation(workspace, item, target, session=session)
    return {
        "source": source_target,
        "target": target.name,
        "relations": len(relations),
        "programmables": code,
    }


def _copy_programmables(workspace: Workspace, source, *, sql, session) -> int:
    """Copy the source's authored procedures and functions into the mirror.

    Data is borrowed and code is local. Weaver's own generated code sits in
    ``_`` and is left to the next ordinary build, which is what makes it.
    """

    from ..catalogue.borrow import programmable_statements
    from ..catalogue.tables import CATALOGUE_SCHEMA
    from ..targets import WarehouseTarget

    source_sql = session.sql_executor(WarehouseTarget(source), workspace=workspace)
    rows = source_sql.query(
        "select m.definition as definition "
        "from sys.sql_modules as m "
        "join sys.objects as o on o.object_id = m.object_id "
        "where o.is_ms_shipped = 0 and o.type in (N'P', N'FN', N'IF', N'TF') "
        f"and schema_name(o.schema_id) <> N'{CATALOGUE_SCHEMA}'"
    )
    statements = programmable_statements(str(row["definition"]) for row in rows)
    if not statements:
        return 0
    with session.step(f"Copy {len(statements)} programmable(s)"):
        for statement in statements:
            sql.execute(statement)
    return len(statements)


def _installed_target(catalogue, item) -> str:
    """Where the catalogue says this item is installed, which is what it reads."""

    from ..catalogue.tables import INSTALLATION

    for row in catalogue.table_rows(INSTALLATION):
        if (
            str(row.get("item_type")) == item.item_type
            and str(row.get("item_name")) == item.item_name
        ):
            return str(row.get("target_name"))
    raise CommandError(
        f"the catalogue records no installation for {item}, so there is nothing "
        "to mirror. Fork a catalogue that has one, or build the item first."
    )


def _record_borrowed(
    workspace: Workspace, relations, *, source_target: str, session
) -> None:
    """Write the ``_.Mirror`` rows, into the catalogue rather than the target."""

    from ..catalogue.borrow import record_statements
    from ..targets import WarehouseTarget

    statements = record_statements(
        relations,
        source_workspace=workspace.workspace,
        source_target=source_target,
    )
    if not statements:
        return
    sql = session.sql_executor(
        WarehouseTarget(workspace.catalogue_item), workspace=workspace
    )
    with session.step("Record what is borrowed"):
        for statement in statements:
            sql.execute(statement)


def _switch_installation(workspace: Workspace, item, target, *, session) -> None:
    """Bind the item to its new target, once the mirror stands."""

    from .. import __version__
    from ..catalogue.render import InstallationScope, render_merge
    from ..catalogue.state import catalogue_in
    from ..catalogue.tables import INSTALLATION
    from ..targets import WarehouseTarget

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
            "target_name": target.name,
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
    with session.step(f"Bind {item} to {target.name}"):
        sql.execute(statement)


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
) -> str:
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
    return target


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
    borrowed: bool,
    session,
) -> dict[str, int]:
    """Copy the source catalogue's rows in, then read back what landed.

    ``borrowed`` says the source holds a ``_.Mirror``. Set, the destination is
    given one and its rows come across; unset, this catalogue has none, which
    is what a catalogue only ever reached by ``weaver build`` looks like.
    """

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
