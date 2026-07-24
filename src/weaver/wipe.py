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
from .hosts import Host, LocalHost
from .locations import Location
from .resolution import LocalResolver, resolver_for, store_for
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
    host: Host,
    *,
    store: Store | None = None,
    dry_run: bool = False,
) -> WipeReport:
    """Empty a folder target, keeping the configured root itself."""

    store = store or store_for(host)
    resolver = resolver_for(host)
    location = resolver.folder_root(target)
    return WipeReport(
        target=f"folder:{target}",
        location=location,
        removed=_clear(store, location, resolver.root, dry_run=dry_run),
        dry_run=dry_run,
    )


def wipe_delta_target(
    target: DeltaTarget,
    host: Host,
    *,
    store: Store | None = None,
    dry_run: bool = False,
) -> WipeReport:
    """Remove every Delta table in a Lakehouse, keeping the Tables area.

    A table is a directory. There is no catalogue to enumerate from and none to
    leave behind, because Weaver never registered one.
    """

    store = store or store_for(host)
    resolver = resolver_for(host)
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

        sql = fabric_sql_executor(target, host)
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
    host: Host,
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
        storage = storage or store_for(host)
        reports.append(
            wipe_folder_target(folder_target, host, store=storage, dry_run=dry_run)
        )
    if delta_target is not None:
        storage = storage or store_for(host)
        reports.append(
            wipe_delta_target(delta_target, host, store=storage, dry_run=dry_run)
        )
    if sql_target is not None:
        if dry_run:
            raise CommandError("Warehouse wipe does not support dry_run")
        wipe_sql_target(sql_target, host, sql=sql)
    return tuple(reports)


def wipe_lakehouse(
    lakehouse: ItemRef,
    host: Host,
    *,
    store: Store | None = None,
    dry_run: bool = False,
) -> tuple[WipeReport, ...]:
    """Clear both areas of a Lakehouse — its Files and its Tables.

    The item is resolved *as a Lakehouse*, so there is no untyped "what is this
    name?" discovery: a Warehouse of the same name resolves elsewhere and is not
    reached here. A destructive operation must not depend on name inference.
    """

    store = store or store_for(host)
    resolver = resolver_for(host)
    if not _lakehouse_exists(resolver, lakehouse):
        raise CommandError(
            f"no Lakehouse named {lakehouse.name!r} on this host — nothing to wipe"
        )
    return (
        wipe_folder_target(
            FolderTarget(lakehouse=lakehouse), host, store=store, dry_run=dry_run
        ),
        wipe_delta_target(
            DeltaTarget(lakehouse=lakehouse), host, store=store, dry_run=dry_run
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
    host: Host,
    *,
    store: Store | None = None,
    dry_run: bool = False,
) -> tuple[WipeReport, ...]:
    """Wipe each named target, taking its type from its shape.

    ``Sales_LH`` is a **Lakehouse** and clears both its areas.
    ``Sales_LH/Files/Extracts`` is a folder root and clears only that. A bare
    name is always a Lakehouse — a Warehouse must be wiped through a
    :class:`~weaver.targets.WarehouseTarget`, never inferred from a name.
    """

    names = list(selection)
    if not names:
        raise CommandError("wipe needs at least one target")

    store = store or store_for(host)
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
                wipe_lakehouse(ItemRef.parse(name), host, store=store, dry_run=dry_run)
            )
    return tuple(reports)
