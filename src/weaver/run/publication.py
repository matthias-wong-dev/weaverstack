"""Wait for Warehouse data to become readable through Lakehouse shortcuts.

Require a new Delta commit and open every added Parquet file through each
consumer. A row count is insufficient because Delta may answer it from commit
statistics before the files are readable.
"""

from __future__ import annotations

import json
import time
from urllib.parse import unquote

from .result import RunError

PUBLICATION_TIMEOUT = 180.0
PUBLICATION_POLL_INTERVAL = 5.0


def published_commits(target_name, object_id, session, workspace) -> frozenset[str]:

    store = session.transport_store(workspace)
    log = _commit_log(target_name, object_id, session, workspace)
    if not store.exists(log):
        return frozenset()
    return frozenset(
        entry.name
        for entry in store.list(log)
        if not entry.is_directory and entry.name.endswith(".json")
    )


def await_publication(
    node,
    session,
    workspace,
    *,
    before: frozenset[str],
    readiness,
    timeout: float = PUBLICATION_TIMEOUT,
    poll: float = PUBLICATION_POLL_INTERVAL,
) -> None:

    object_id = node.publication_of.object_id
    store = session.transport_store(workspace)
    log = _commit_log(node.physical_target.name, object_id, session, workspace)
    roots = tuple(_shortcut_root(one, session, workspace) for one in readiness)
    deadline = time.monotonic() + timeout
    commits: tuple[str, ...] = ()
    last_error: Exception | None = None
    while True:
        try:
            # Re-list on every attempt so concurrent commits join the same wait.
            commits = _new_commits(store, log, before) or commits
            if commits:
                statements = _probe_statements(_added_files(store, log, commits), roots)
                if statements:
                    session.execute_spark_sql_batch(statements, workspace=workspace)
                return
        except Exception as exc:  # Fabric settles this surface asynchronously.
            last_error = exc
        if time.monotonic() >= deadline:
            raise _not_published(node, commits, last_error, timeout)
        time.sleep(poll)


def _commit_log(target_name, object_id, session, workspace):

    from ..fabric.resources import WAREHOUSE
    from ..resolution import TABLES_AREA
    from ..targets import ItemRef

    resolver = session.resolver(workspace)
    item = resolver.resolve(ItemRef(target_name), item_type=WAREHOUSE)
    root = resolver.external_root(item)
    return root.join(TABLES_AREA, object_id.schema, object_id.object, "_delta_log")


def _shortcut_root(readiness, session, workspace) -> str:

    from ..targets import ItemRef

    resolver = session.resolver(workspace)
    location = resolver.lakehouse_spark_location(ItemRef(readiness.target.name))
    return location.table_path(readiness.schema, readiness.object)


def _new_commits(store, log, before: frozenset[str]) -> tuple[str, ...]:

    if not store.exists(log):
        return ()
    return tuple(
        sorted(
            entry.name
            for entry in store.list(log)
            if not entry.is_directory
            and entry.name.endswith(".json")
            and entry.name not in before
        )
    )


def _added_files(store, log, commits) -> tuple[str, ...]:
    """Return live added files, decoding their log paths for Spark."""

    added: list[str] = []
    removed: set[str] = set()
    for name in commits:
        for line in store.read(log / name).splitlines():
            if not line.strip():
                continue
            action = json.loads(line)
            add = (action.get("add") or {}).get("path")
            if add:
                added.append(unquote(add))
            remove = (action.get("remove") or {}).get("path")
            if remove:
                removed.add(unquote(remove))
    return tuple(dict.fromkeys(path for path in added if path not in removed))


def _probe_statements(paths, roots) -> list[str]:

    return [
        f"select * from parquet.`{root}/{path}` limit 1"
        for root in roots
        for path in paths
    ]


def _not_published(node, commits, last_error, timeout: float) -> RunError:

    table = f"{node.physical_target.name}/{node.publication_of.object_id.qualified}"
    waited = int(timeout)
    if not commits:
        return RunError(
            f"No new OneLake Delta commit appeared for Warehouse table {table} "
            f"within {waited}s. Retry after publication completes."
        )
    detail = f": {type(last_error).__name__}: {last_error}" if last_error else ""
    return RunError(
        f"The new Parquet files for Warehouse table {table} were not readable "
        f"through its Lakehouse shortcuts within {waited}s{detail}. Retry after "
        "publication completes."
    )


class PublicationLedger:
    """Publication baselines and movement for loads followed by barriers.

    Capture each baseline before its load because the later barrier cannot
    reconstruct it. Loads without barriers do not read a Delta log.
    """

    def __init__(self, awaited: frozenset[str]) -> None:
        self._awaited = frozenset(awaited)
        self._before: dict[str, frozenset[str]] = {}
        self._moved: set[str] = set()

    def awaits(self, node_id: str) -> bool:

        return node_id in self._awaited

    def observe(self, node, session, workspace) -> None:

        if not self.awaits(node.node_id):
            return
        self._before[node.node_id] = published_commits(
            node.physical_target.name, node.logical_id.object_id, session, workspace
        )

    def settled(self, node_id: str, result) -> None:

        if result.rows_inserted or result.rows_updated or result.rows_deleted:
            self._moved.add(node_id)

    def moved(self, node_id: str) -> bool:
        return node_id in self._moved

    def baseline(self, node_id: str) -> frozenset[str]:
        return self._before.get(node_id, frozenset())
