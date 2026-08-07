"""Internal mechanics for clearing a physical target.

Wipe is per target, because the three are different places with different
mechanics. It is also the bluntest thing Weaver does, so it reports what it
would remove before it removes anything.

**Delta.** Weaver addresses Delta tables by explicit path and never registers
them in a metastore, so there is no catalogue to consult and none to leave
dangling — a table is a directory, and wiping is removing it. That is the same
property that lets a Fabric notebook write to a Lakehouse it is not attached
to, showing up again as a simplification. On Fabric the Lakehouse auto-discovers
what appears under ``Tables/``, so removing the directory is expected to
de-register it; that is worth confirming against a real workspace before relying
on it.

**Shortcuts, first.** A shortcut is the one thing in a Lakehouse that is not the
Lakehouse's own data: it is a name this item holds for data another item owns. So
it cannot be removed by deleting a directory, and *must not be* — a recursive
delete of a directory holding one would be reaching through the pointer at the
producer's data, which is the one outcome a wipe of *this* Lakehouse must never
have. Shortcuts are therefore removed through the workspace, and removed before
any storage is swept, so the sweep can never meet one.

The local emulator materialises an alias as a symbolic link, and
:meth:`weaver.store.FilesystemStore.delete` unlinks a link rather than following it —
so the emulator reaches the same guarantee by a different mechanism, and neither
needs the other's.

**Folders.** The Files area is kept and its contents removed, so the target
survives and only what it held goes. Shortcuts under ``Files/`` go first, for the
same reason.

**Warehouse.** One dynamic statement enumerates and removes user objects in
dependency-safe order while preserving the Warehouse item and system schemas.

Nothing here is scoped to Weaver-managed objects: a wipe clears the target. That
suits a development loop, and makes the function something a CLI must gate
rather than something safe by default.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from .errors import CommandError
from .workspaces import Workspace, LocalWorkspace
from .locations import Location
from .resolution import TABLES_AREA, LocalResolver, resolver_for, store_for
from .store import FilesystemStore, Store
from .targets import FILES_AREA, DeltaTarget, FolderTarget, ItemRef, WarehouseTarget


@dataclass(frozen=True)
class WipeReport:
    """What a wipe removed, or would remove."""

    target: str
    location: Location
    removed: tuple[str, ...]
    dry_run: bool = False

    @property
    def count(self) -> int:
        return len(self.removed)

    def __str__(self) -> str:
        verb = "would remove" if self.dry_run else "removed"
        return f"{self.target}: {verb} {self.count} from {self.location}"


#: Schemas a wipe empties but does not remove. A schema-enabled Fabric Lakehouse
#: is created holding ``dbo``; Fabric owns it, Weaver never manages it, and
#: nothing recreates it once its directory is gone — the Lakehouse is simply left
#: unable to resolve a schema it is supposed to have. Deleting it is therefore
#: not "clearing the target" but damaging it, which is a different act and not
#: one a wipe is asking for.
#:
#: This is the same judgement :data:`weaver.build_bundle.prune._RESERVED_SCHEMAS`
#: already makes for prune. The two agreeing is the point: an operator should not
#: have to know which of Weaver's destructive paths respects the default schema.
_KEPT_SCHEMAS = ("dbo",)


def _guard(location: Location, root: Location) -> None:
    """Never remove anything outside the workspace root.

    Locations are derived rather than supplied, so this should be unreachable —
    which is exactly why it is worth having, since the failure it prevents is
    unrecoverable.
    """

    inside = location.value == root.value or location.value.startswith(
        root.value.rstrip("/") + "/"
    )
    if not inside:
        raise CommandError(
            f"refusing to wipe {location.value!r}: outside the workspace root {root.value!r}"
        )


def _clear(
    store: Store, location: Location, root: Location, *, dry_run: bool, keep=()
) -> tuple[str, ...]:
    """Remove the contents of a location, keeping the location itself.

    ``keep`` names entries the wipe passes over. It is not a scoping of what a
    wipe is *for* — a wipe still clears the target — but a recognition that not
    everything under an area belongs to the target. See :data:`_KEPT_SCHEMAS`.
    """

    _guard(location, root)
    if not store.exists(location):
        return ()
    kept = {name.casefold() for name in keep}
    entries = [
        entry for entry in store.list(location) if entry.location.name.casefold() not in kept
    ]
    removed = tuple(sorted(entry.location.name for entry in entries))
    if not dry_run:
        for entry in entries:
            _guard(entry.location, root)
            store.delete(entry.location, recursive=entry.is_directory)
    return removed


def _remove_shortcuts(
    resolver, lakehouse: ItemRef, *, prefix: str, dry_run: bool
) -> tuple[str, ...]:
    """Take away this Lakehouse's shortcuts beneath ``prefix``, before storage is swept.

    Scoped by path prefix, so a wipe of one area leaves the other's pointers
    alone — a Files wipe must not take away a shortcut under ``Tables/``.

    Reported as ``shortcut:<path>/<name>`` so a dry run distinguishes a pointer
    being taken away from a directory being deleted — they are not the same act,
    and only one of them destroys data.

    An environment with no shortcuts to remove offers no capability and this
    answers nothing: the emulator's links are removed by the storage sweep, which
    unlinks rather than follows.
    """

    enumerate_shortcuts = getattr(resolver, "onelake_shortcuts", None)
    remove = getattr(resolver, "remove_onelake_shortcut", None)
    if enumerate_shortcuts is None or remove is None:
        return ()

    within = prefix.strip("/").casefold()
    shortcuts = tuple(
        shortcut
        for shortcut in enumerate_shortcuts(lakehouse)
        if shortcut.path.casefold() == within
        or shortcut.path.casefold().startswith(within + "/")
    )
    if not dry_run:
        for shortcut in shortcuts:
            remove(lakehouse, path=shortcut.path, name=shortcut.name)
    return tuple(f"shortcut:{shortcut.qualified}" for shortcut in shortcuts)


def wipe_folder_target(
    target: FolderTarget,
    workspace: Workspace,
    *,
    store: Store | None = None,
    dry_run: bool = False,
) -> WipeReport:
    """Empty a folder target, keeping the Files area itself."""

    store = store or store_for(workspace)
    resolver = resolver_for(workspace)
    location = resolver.folder_root(target)
    shortcuts = _remove_shortcuts(
        resolver, target.lakehouse, prefix=FILES_AREA, dry_run=dry_run
    )
    return WipeReport(
        target=f"folder:{target}",
        location=location,
        removed=shortcuts + _clear(store, location, resolver.root, dry_run=dry_run),
        dry_run=dry_run,
    )


def wipe_delta_target(
    target: DeltaTarget,
    workspace: Workspace,
    *,
    store: Store | None = None,
    dry_run: bool = False,
) -> WipeReport:
    """Remove every Delta table in a Lakehouse, keeping the Tables area.

    A table is a directory. There is no catalogue to enumerate from and none to
    leave behind, because Weaver never registered one — with one exception, and it
    is the reason shortcuts go first: a table shortcut is a directory whose bytes
    belong to another item.
    """

    store = store or store_for(workspace)
    resolver = resolver_for(workspace)
    location = resolver.tables_root(target.lakehouse)
    shortcuts = _remove_shortcuts(
        resolver, target.lakehouse, prefix=TABLES_AREA, dry_run=dry_run
    )
    return WipeReport(
        target=f"delta:{target}",
        location=location,
        removed=shortcuts
        + _clear(
            store, location, resolver.root, dry_run=dry_run, keep=_KEPT_SCHEMAS
        ),
        dry_run=dry_run,
    )


def wipe_sql_target(
    target: WarehouseTarget,
    workspace: Workspace,
    *,
    sql=None,
) -> None:
    """Clear a Warehouse through the common SQL capability.

    The default is deliberately Fabric-native.  A desktop caller crossing into
    Fabric constructs and injects ``desktop_sql_executor`` explicitly.
    """

    from .sql import SqlError, SqlExecutionError, generate_warehouse_wipe_sql

    owns_sql = sql is None
    if sql is None:
        from .fabric.sql import fabric_sql_executor

        sql = fabric_sql_executor(target, workspace)
    try:
        sql.execute_script(generate_warehouse_wipe_sql())
    except SqlError as exc:
        raise SqlExecutionError(
            f"failed to wipe Warehouse {target.warehouse.name!r}: {exc}"
        ) from exc
    except Exception as exc:
        raise SqlExecutionError(
            f"failed to wipe Warehouse {target.warehouse.name!r}: {exc}"
        ) from exc
    finally:
        if owns_sql and hasattr(sql, "close"):
            sql.close()


def wipe(
    workspace: Workspace,
    *,
    folder_target: FolderTarget | None = None,
    delta_target: DeltaTarget | None = None,
    sql_target: WarehouseTarget | None = None,
    store: Store | None = None,
    sql=None,
    dry_run: bool = False,
) -> tuple[WipeReport, ...]:
    """Wipe each supplied target. At least one is required.

    Targets are independently optional, so a development loop can clear the
    Delta tables while leaving downloaded source files alone.
    """

    if not any((folder_target, delta_target, sql_target)):
        raise CommandError("wipe needs at least one target")

    reports: list[WipeReport] = []
    storage = store
    if folder_target is not None:
        storage = storage or store_for(workspace)
        reports.append(
            wipe_folder_target(folder_target, workspace, store=storage, dry_run=dry_run)
        )
    if delta_target is not None:
        storage = storage or store_for(workspace)
        reports.append(
            wipe_delta_target(delta_target, workspace, store=storage, dry_run=dry_run)
        )
    if sql_target is not None:
        if dry_run:
            raise CommandError("Warehouse wipe does not support dry_run")
        wipe_sql_target(sql_target, workspace, sql=sql)
    return tuple(reports)


def wipe_lakehouse(
    lakehouse: ItemRef,
    workspace: Workspace,
    *,
    store: Store | None = None,
    dry_run: bool = False,
) -> tuple[WipeReport, ...]:
    """Clear both areas of a Lakehouse — its Files and its Tables.

    The item is resolved *as a Lakehouse*, so there is no untyped "what is this
    name?" discovery: a Warehouse of the same name resolves elsewhere and is not
    reached here. A destructive operation must not depend on name inference.
    """

    store = store or store_for(workspace)
    resolver = resolver_for(workspace)
    if not _lakehouse_exists(resolver, lakehouse):
        raise CommandError(
            f"no Lakehouse named {lakehouse.name!r} on this workspace — nothing to wipe"
        )
    return (
        wipe_folder_target(
            FolderTarget(lakehouse=lakehouse), workspace, store=store, dry_run=dry_run
        ),
        wipe_delta_target(
            DeltaTarget(lakehouse=lakehouse), workspace, store=store, dry_run=dry_run
        ),
    )


def _lakehouse_exists(resolver, lakehouse: ItemRef) -> bool:
    """Whether the Lakehouse is there, resolved as a Lakehouse.

    Locally that is a directory check; on Fabric, resolving it as a Lakehouse
    both proves it exists and refuses a same-named Warehouse.
    """

    if hasattr(resolver, "lakehouse_exists"):
        return resolver.lakehouse_exists(lakehouse)
    from .errors import CommandError as _CommandError

    try:
        resolver.lakehouse(lakehouse)
        return True
    except _CommandError:
        return False


def wipe_selection(
    selection: Iterable[str],
    workspace: Workspace,
    *,
    store: Store | None = None,
    dry_run: bool = False,
) -> tuple[WipeReport, ...]:
    """Wipe each named target, taking its type from its shape.

    ``Sales_LH`` is a **Lakehouse** and clears both its areas.
    ``Sales_LH/Files`` is that Lakehouse's Files area and clears only that. A bare
    name is always a Lakehouse — a Warehouse must be wiped through a
    :class:`~weaver.targets.WarehouseTarget`, never inferred from a name.
    """

    names = list(selection)
    if not names:
        raise CommandError("wipe needs at least one target")

    store = store or store_for(workspace)
    reports: list[WipeReport] = []
    for name in names:
        if "/" in name:
            reports.append(
                wipe_folder_target(
                    FolderTarget.parse(name), workspace, store=store, dry_run=dry_run
                )
            )
        else:
            reports.extend(
                wipe_lakehouse(ItemRef.parse(name), workspace, store=store, dry_run=dry_run)
            )
    return tuple(reports)
