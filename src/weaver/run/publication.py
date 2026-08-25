"""Wait for a Warehouse's OneLake publication to reach its Lakehouse consumers.

A Warehouse table settles over TDS, and Fabric then publishes the table's Delta
log in the background. Until that publication lands and its Parquet files can be
opened through the consuming shortcut, a Lakehouse consumer reading the same
table sees either the previous snapshot or a snapshot whose files it cannot read.

This is the Warehouse-side counterpart of the Lakehouse SQL analytics endpoint
refresh: the producer has finished writing, and the surface the consumer reads
has not caught up yet. The proof has two parts, because the two lags are
different failures:

* a commit that was not there before the load, so the consumer's snapshot is the
  one this load produced;
* an opened read of each Parquet file that commit added, through the consuming
  shortcut, so the snapshot's files are addressable from where they are needed.

The second part opens the files rather than counting rows. Delta answers
``count(*)`` from the commit's own statistics, so a count can succeed against a
snapshot whose files are not readable yet.
"""

from __future__ import annotations

import json
import time
from urllib.parse import unquote

from .result import RunError

PUBLICATION_TIMEOUT = 180.0
PUBLICATION_POLL_INTERVAL = 5.0


def published_commits(node, session, workspace) -> frozenset[str]:
    """The commit files published for one Warehouse table at this instant."""

    store = session.transport_store(workspace)
    log = _commit_log(node, session, workspace)
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
    """Block until this load's publication is readable at every consuming path."""

    store = session.transport_store(workspace)
    log = _commit_log(node, session, workspace)
    roots = tuple(_shortcut_root(one, session, workspace) for one in readiness)
    deadline = time.monotonic() + timeout
    commits: tuple[str, ...] = ()
    last_error: Exception | None = None
    while True:
        try:
            # Every attempt re-lists, so a second publication arriving during
            # the wait is covered by the same interval.
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


def _commit_log(node, session, workspace):
    """Where OneLake holds this Warehouse table's Delta log."""

    from ..fabric.resources import WAREHOUSE
    from ..resolution import TABLES_AREA
    from ..targets import ItemRef

    resolver = session.resolver(workspace)
    item = resolver.resolve(ItemRef(node.physical_target.name), item_type=WAREHOUSE)
    object_id = node.logical_id.object_id
    root = resolver.external_root(item)
    return root.join(TABLES_AREA, object_id.schema, object_id.object, "_delta_log")


def _shortcut_root(readiness, session, workspace) -> str:
    """The destination path a downstream Lakehouse primitive will read."""

    from ..targets import ItemRef

    resolver = session.resolver(workspace)
    location = resolver.lakehouse_spark_location(ItemRef(readiness.target.name))
    return location.table_path(readiness.schema, readiness.object)


def _new_commits(store, log, before: frozenset[str]) -> tuple[str, ...]:
    """The commit files published since the load began, oldest first."""

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
    """The Parquet files these commits add and do not go on to remove.

    Paths are relative to the table root and percent-encoded in the log, so they
    are decoded here into the spelling a path expression needs.
    """

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
    """One opened read per newly published file, per consuming shortcut."""

    return [
        f"select * from parquet.`{root}/{path}` limit 1"
        for root in roots
        for path in paths
    ]


def _not_published(node, commits, last_error, timeout: float) -> RunError:
    """The error for a publication that did not settle within the wait."""

    table = f"{node.physical_target.name}/{node.logical_id.object_id.qualified}"
    waited = int(timeout)
    if not commits:
        return RunError(
            f"Warehouse table {table} changed over TDS, and OneLake published no "
            f"new Delta commit for it within {waited}s. The Lakehouse consumers "
            "that read it through a shortcut would see the previous snapshot."
        )
    detail = f": {type(last_error).__name__}: {last_error}" if last_error else ""
    return RunError(
        f"Warehouse table {table} published a Delta commit, and the Parquet files "
        f"it added were not readable through its Lakehouse shortcuts within "
        f"{waited}s{detail}"
    )
