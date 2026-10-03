"""Internal mechanics for clearing a physical target.

Wipe clears a target after reporting its planned removals. Lakehouse shortcuts
are removed through the workspace before storage is swept, so a wipe cannot
affect their source items. Warehouse system schemas are retained.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from .errors import CommandError
from .locations import Location
from .resolution import resolver_for, store_for
from .store import Store
from .targets import (
    FILES_AREA,
    TABLES_AREA,
    DeltaTarget,
    FolderTarget,
    ItemRef,
    WarehouseTarget,
)
from .workspaces import Workspace


@dataclass(frozen=True)
class WipeReport:
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


#: Fabric owns the default ``dbo`` schema. Wipe empties it but does not remove it.
KEPT_SCHEMAS = ("dbo",)


def _guard(location: Location, root: Location) -> None:
    """Reject a deletion outside the workspace root."""

    inside = location.value == root.value or location.value.startswith(
        root.value.rstrip("/") + "/"
    )
    if not inside:
        raise CommandError(
            f"refusing to wipe {location.value!r}: outside the workspace root {root.value!r}"
        )


#: Recursive deletes issued at once while clearing one area.
ENTRY_DELETIONS = 16


def clear_area(
    store: Store, location: Location, root: Location, *, dry_run: bool, keep=()
) -> tuple[str, ...]:
    """Remove the contents of a location, keeping the location itself.

    ``keep`` names entries the wipe passes over: not everything under an area
    belongs to the target. See :data:`KEPT_SCHEMAS`.
    """

    _guard(location, root)
    if not store.exists(location):
        return ()
    kept = {name.casefold() for name in keep}
    entries = [
        entry
        for entry in store.list(location)
        if entry.location.name.casefold() not in kept
    ]
    removed = tuple(sorted(entry.location.name for entry in entries))
    if not dry_run and entries:
        for entry in entries:
            _guard(entry.location, root)
        from concurrent.futures import ThreadPoolExecutor
        from contextvars import copy_context

        # Each entry is one recursive delete, independent of the others.
        with ThreadPoolExecutor(max_workers=min(ENTRY_DELETIONS, len(entries))) as pool:
            for done in [
                pool.submit(
                    copy_context().run,
                    store.delete,
                    entry.location,
                    recursive=entry.is_directory,
                )
                for entry in entries
            ]:
                done.result()
    return removed


#: Shortcut removals are independent REST calls, so a wipe issues several at once.
SHORTCUT_REMOVALS = 8

#: OneLake may answer for a removed shortcut's path after Fabric stops listing it.
NAME_RELEASE_TIMEOUT = 300.0
NAME_RELEASE_POLL_INTERVAL = 1.0


@dataclass(frozen=True)
class DetachedShortcuts:
    """Removed shortcuts, reported as ``shortcut:<path>/<name>``, and their paths."""

    removed: tuple[str, ...]
    locations: tuple[Location, ...]


def detach_shortcuts(
    resolver, lakehouse: ItemRef, *, prefix: str, dry_run: bool = False
) -> DetachedShortcuts:
    """Remove this Lakehouse's shortcuts beneath ``prefix`` through the workspace.

    The path prefix keeps a wipe of one area from removing the other's shortcuts.
    Removing a shortcut does not delete its source data; deleting through a path
    that still resolves to one would, so a sweep waits for :func:`released`.
    """

    enumerate_shortcuts = getattr(resolver, "onelake_shortcuts", None)
    remove = getattr(resolver, "remove_onelake_shortcut", None)
    if enumerate_shortcuts is None or remove is None:
        return DetachedShortcuts((), ())

    within = prefix.strip("/").casefold()
    shortcuts = tuple(
        shortcut
        for shortcut in enumerate_shortcuts(lakehouse)
        if shortcut.path.casefold() == within
        or shortcut.path.casefold().startswith(within + "/")
    )
    if not shortcuts:
        return DetachedShortcuts((), ())
    if not dry_run:
        from concurrent.futures import ThreadPoolExecutor
        from contextvars import copy_context

        with ThreadPoolExecutor(
            max_workers=min(SHORTCUT_REMOVALS, len(shortcuts))
        ) as pool:
            for done in [
                pool.submit(
                    copy_context().run,
                    remove,
                    lakehouse,
                    path=shortcut.path,
                    name=shortcut.name,
                )
                for shortcut in shortcuts
            ]:
                done.result()
    root = resolver.lakehouse(lakehouse)
    return DetachedShortcuts(
        removed=tuple(f"shortcut:{shortcut.qualified}" for shortcut in shortcuts),
        locations=tuple(
            root.join(*shortcut.path.split("/"), shortcut.name)
            for shortcut in shortcuts
        ),
    )


def released(store: Store, locations) -> bool:
    """Whether OneLake has released every removed shortcut's path.

    A path OneLake is still releasing can answer 403 for a while, so a refusal
    reads as not yet released.
    """

    for location in locations:
        try:
            if store.exists(location):
                return False
        except Exception as exc:  # notebookutils raises a bare Py4J error
            if not _forbidden(exc):
                raise
            return False
    return True


def _forbidden(exc: BaseException) -> bool:
    text = str(exc)
    return "Forbidden" in text or "AccessDenied" in text or "returned 403" in text


def _await_release(store: Store, detached: DetachedShortcuts) -> None:
    import time

    deadline = time.monotonic() + NAME_RELEASE_TIMEOUT
    while not released(store, detached.locations):
        if time.monotonic() >= deadline:
            raise unreleased(detached)
        time.sleep(NAME_RELEASE_POLL_INTERVAL)


def unreleased(detached: DetachedShortcuts) -> CommandError:
    return CommandError(
        "OneLake still answers for removed shortcut(s) "
        + ", ".join(detached.removed)
        + f" after {NAME_RELEASE_TIMEOUT:.0f}s, so their area was not swept. "
        "Run the wipe again."
    )


def _remove_shortcuts(
    resolver, lakehouse: ItemRef, *, prefix: str, dry_run: bool, store: Store
) -> tuple[str, ...]:
    detached = detach_shortcuts(resolver, lakehouse, prefix=prefix, dry_run=dry_run)
    if not dry_run:
        _await_release(store, detached)
    return detached.removed


def _store_for(workspace: Workspace, session):
    return session.store(workspace) if session is not None else store_for(workspace)


def _resolver_for(workspace: Workspace, session):
    """The resolver, from the Session that owns one, or built for this call.

    A Session's resolver carries the item cache the operations before this one
    filled; one built here starts empty and re-asks the workspace what the same
    names mean.
    """

    return (
        session.resolver(workspace) if session is not None else resolver_for(workspace)
    )


def wipe_folder_target(
    target: FolderTarget,
    workspace: Workspace,
    *,
    store: Store | None = None,
    dry_run: bool = False,
    session=None,
) -> WipeReport:
    """Empty a folder target while keeping its Files area."""

    store = store or _store_for(workspace, session)
    resolver = _resolver_for(workspace, session)
    location = resolver.folder_root(target)
    shortcuts = _remove_shortcuts(
        resolver, target.lakehouse, prefix=FILES_AREA, dry_run=dry_run, store=store
    )
    return WipeReport(
        target=f"folder:{target}",
        location=location,
        removed=shortcuts + clear_area(store, location, resolver.root, dry_run=dry_run),
        dry_run=dry_run,
    )


def wipe_delta_target(
    target: DeltaTarget,
    workspace: Workspace,
    *,
    store: Store | None = None,
    dry_run: bool = False,
    session=None,
) -> WipeReport:
    """Remove every Delta table in a Lakehouse, keeping the Tables area.

    Tables are discovered from storage. Shortcuts are removed first because their
    directories refer to data owned by another item.
    """

    store = store or _store_for(workspace, session)
    resolver = _resolver_for(workspace, session)
    location = resolver.tables_root(target.lakehouse)
    shortcuts = _remove_shortcuts(
        resolver, target.lakehouse, prefix=TABLES_AREA, dry_run=dry_run, store=store
    )
    return WipeReport(
        target=f"delta:{target}",
        location=location,
        removed=shortcuts
        + clear_area(
            store, location, resolver.root, dry_run=dry_run, keep=KEPT_SCHEMAS
        ),
        dry_run=dry_run,
    )


def wipe_sql_target(
    target: WarehouseTarget,
    workspace: Workspace,
    *,
    sql=None,
) -> None:
    """Clear a Warehouse with an injected or Fabric-native SQL executor."""

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
    session=None,
) -> tuple[WipeReport, ...]:
    """Wipe each supplied target. At least one is required.

    Targets are independent; an omitted target is left untouched.
    """

    if not any((folder_target, delta_target, sql_target)):
        raise CommandError("wipe needs at least one target")

    reports: list[WipeReport] = []
    storage = store
    if folder_target is not None:
        storage = storage or _store_for(workspace, session)
        reports.append(
            wipe_folder_target(
                folder_target,
                workspace,
                store=storage,
                dry_run=dry_run,
                session=session,
            )
        )
    if delta_target is not None:
        storage = storage or _store_for(workspace, session)
        reports.append(
            wipe_delta_target(
                delta_target,
                workspace,
                store=storage,
                dry_run=dry_run,
                session=session,
            )
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
    session=None,
) -> tuple[WipeReport, ...]:
    """Clear both areas of a Lakehouse, its Files and its Tables.

    The item is resolved by type. A same-named Warehouse is not reached.

    ``session`` supplies the store and the resolver where the caller has one, so
    the name this wipe resolves is the name the build before it already
    resolved.
    """

    store = store or _store_for(workspace, session)
    resolver = _resolver_for(workspace, session)
    if not _lakehouse_exists(resolver, lakehouse):
        raise CommandError(
            f"no Lakehouse named {lakehouse.name!r} exists in this workspace"
        )
    return (
        wipe_folder_target(
            FolderTarget(lakehouse=lakehouse),
            workspace,
            store=store,
            dry_run=dry_run,
            session=session,
        ),
        wipe_delta_target(
            DeltaTarget(lakehouse=lakehouse),
            workspace,
            store=store,
            dry_run=dry_run,
            session=session,
        ),
    )


def _lakehouse_exists(resolver, lakehouse: ItemRef) -> bool:
    """Check existence by type so a same-named Warehouse cannot be deleted."""

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
    name is always a Lakehouse. A Warehouse is wiped through a
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
                wipe_lakehouse(
                    ItemRef.parse(name), workspace, store=store, dry_run=dry_run
                )
            )
    return tuple(reports)
