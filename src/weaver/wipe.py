"""Clearing a physical target.

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

**Folders.** The configured root is kept and its contents removed, so the target
survives and only what it held goes.

**Warehouse.** Not implemented. It needs a single dynamic statement built from
the catalogue views, and there is no local SQL to develop it against.

Nothing here is scoped to Weaver-managed objects: a wipe clears the target. That
suits a development loop, and makes the function something a CLI must gate
rather than something safe by default.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from .errors import CommandError
from .hosts import Host, LocalHost
from .locations import Location
from .resolution import LocalResolver
from .store import LocalStore, Store
from .targets import DeltaTarget, FolderTarget, ItemRef, WarehouseTarget


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


def _guard(location: Location, root: Location) -> None:
    """Never remove anything outside the host root.

    Locations are derived rather than supplied, so this should be unreachable —
    which is exactly why it is worth having, since the failure it prevents is
    unrecoverable.
    """

    inside = location.value == root.value or location.value.startswith(
        root.value.rstrip("/") + "/"
    )
    if not inside:
        raise CommandError(
            f"refusing to wipe {location.value!r}: outside the host root {root.value!r}"
        )


def _clear(
    store: Store, location: Location, root: Location, *, dry_run: bool
) -> tuple[str, ...]:
    """Remove the contents of a location, keeping the location itself."""

    _guard(location, root)
    if not store.exists(location):
        return ()
    entries = store.list(location)
    removed = tuple(sorted(entry.location.name for entry in entries))
    if not dry_run:
        for entry in entries:
            _guard(entry.location, root)
            store.delete(entry.location, recursive=entry.is_directory)
    return removed


def wipe_folder_target(
    target: FolderTarget,
    host: LocalHost,
    *,
    store: Store | None = None,
    dry_run: bool = False,
) -> WipeReport:
    """Empty a folder target, keeping the configured root itself."""

    store = store or LocalStore()
    resolver = LocalResolver(host)
    location = resolver.folder_root(target)
    return WipeReport(
        target=f"folder:{target}",
        location=location,
        removed=_clear(store, location, resolver.root, dry_run=dry_run),
        dry_run=dry_run,
    )


def wipe_delta_target(
    target: DeltaTarget,
    host: LocalHost,
    *,
    store: Store | None = None,
    dry_run: bool = False,
) -> WipeReport:
    """Remove every Delta table in a Lakehouse, keeping the Tables area.

    A table is a directory. There is no catalogue to enumerate from and none to
    leave behind, because Weaver never registered one.
    """

    store = store or LocalStore()
    resolver = LocalResolver(host)
    location = resolver.tables_root(target.lakehouse)
    return WipeReport(
        target=f"delta:{target}",
        location=location,
        removed=_clear(store, location, resolver.root, dry_run=dry_run),
        dry_run=dry_run,
    )


def wipe_sql_target(
    target: WarehouseTarget,
    host: Host,
    *,
    store: Store | None = None,
    dry_run: bool = False,
) -> WipeReport:
    """Not implemented.

    A Warehouse wipe is one dynamic statement generated from the catalogue
    views — droppable objects in dependency order. There is no local SQL to
    develop it against, so it waits for the Fabric vertical rather than being
    guessed at now.
    """

    raise NotImplementedError(
        f"wiping the Warehouse target {target.warehouse.name!r} is not implemented — "
        "it needs a Warehouse to develop against, and a local host has no SQL"
    )


def wipe(
    host: LocalHost,
    *,
    folder_target: FolderTarget | None = None,
    delta_target: DeltaTarget | None = None,
    sql_target: WarehouseTarget | None = None,
    store: Store | None = None,
    dry_run: bool = False,
) -> tuple[WipeReport, ...]:
    """Wipe each supplied target. At least one is required.

    Targets are independently optional, so a development loop can clear the
    Delta tables while leaving downloaded source files alone.
    """

    if not any((folder_target, delta_target, sql_target)):
        raise CommandError("wipe needs at least one target")

    store = store or LocalStore()
    reports: list[WipeReport] = []
    if folder_target is not None:
        reports.append(wipe_folder_target(folder_target, host, store=store, dry_run=dry_run))
    if delta_target is not None:
        reports.append(wipe_delta_target(delta_target, host, store=store, dry_run=dry_run))
    if sql_target is not None:
        reports.append(wipe_sql_target(sql_target, host, store=store, dry_run=dry_run))
    return tuple(reports)


def wipe_item(
    item: ItemRef,
    host: Host,
    *,
    store: Store | None = None,
    dry_run: bool = False,
) -> tuple[WipeReport, ...]:
    """Clear a whole level-three item.

    A Lakehouse holds both areas, so wiping one clears its Files and its
    Tables. What an item *is* comes from the host: locally every item is
    Lakehouse-shaped, while on Fabric it has to be asked for.
    """

    if not isinstance(host, LocalHost):
        raise NotImplementedError(
            f"wiping {item.name!r} on a {type(host).__name__} is not implemented — "
            "it needs Fabric item resolution to know whether the item is a "
            "Lakehouse or a Warehouse"
        )

    store = store or LocalStore()
    resolver = LocalResolver(host)
    if not store.exists(resolver.item(item)):
        raise CommandError(
            f"{item.name!r} does not exist under {resolver.root} — nothing to wipe"
        )

    return (
        wipe_folder_target(
            FolderTarget(lakehouse=item), host, store=store, dry_run=dry_run
        ),
        wipe_delta_target(
            DeltaTarget(lakehouse=item), host, store=store, dry_run=dry_run
        ),
    )


def wipe_selection(
    selection: Iterable[str],
    host: Host,
    *,
    store: Store | None = None,
    dry_run: bool = False,
) -> tuple[WipeReport, ...]:
    """Wipe each named target, reading its kind from its shape.

    ``Sales_LH`` names an item and clears all of it.
    ``Sales_LH/Files/Extracts`` names a folder root and clears only that.
    """

    names = list(selection)
    if not names:
        raise CommandError("wipe needs at least one target")

    store = store or LocalStore()
    reports: list[WipeReport] = []
    for name in names:
        if "/" in name:
            reports.append(
                wipe_folder_target(
                    FolderTarget.parse(name), host, store=store, dry_run=dry_run
                )
            )
        else:
            reports.extend(
                wipe_item(ItemRef.parse(name), host, store=store, dry_run=dry_run)
            )
    return tuple(reports)
