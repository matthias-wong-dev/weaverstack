"""Bounded reads of the catalogue's history tables.

``_.Log`` and ``_.LoadStatistic`` grow with the estate's age, so nothing reads
them whole. These two functions read one window: the most recent load workflow,
and the statistics that workflow's loads appended.

Both go through :class:`weaver.catalogue.connection.CatalogueConnection`, so an
absent table reads as no rows and a permission failure stays a failure. Rows come
back under the internal keys every other catalogue read uses; what they mean to
an operator is :mod:`weaver.health`'s to say.
"""

from __future__ import annotations

from typing import Any

from .reader import read_table
from .tables import LOAD_RESULT_VOCABULARY, LOAD_STATISTIC, LOG
from .tsql import identifier, literal, qualified_name

#: The task type an orchestrated load and a standalone object load both record
#: under. Restated rather than imported, because this package sits below
#: :mod:`weaver.run`, and asserted equal to it by
#: ``tests/test_core_boundary.py``.
LOAD_TASK = "load"

#: How many statistic rows one window may return. A workflow loads what one
#: request selected, so this bounds a pathological estate rather than an
#: ordinary one.
DEFAULT_LIMIT = 500


def latest_load_workflow(catalogue: Any) -> dict | None:
    """The most recent recorded load activity, from ``_.Log``.

    One workflow, chosen by the latest completion instant its load rows carry,
    with the window it spans and its rows counted by result. The runtime records
    orchestrated and standalone loads under one task type, so this is the latest
    load activity and says nothing about who started it.
    """

    if catalogue.columns_of(LOG) is None:
        return None
    task = identifier(LOG.public_name_of("task_type"))
    completed = identifier(LOG.public_name_of("completed_datetime"))
    started = identifier(LOG.public_name_of("started_datetime"))
    workflow = identifier(LOG.public_name_of("workflow_id"))
    result = identifier(LOG.public_name_of("result"))
    table = qualified_name(LOG)

    latest = [
        str(dict(row)["workflow_id"])
        for row in catalogue.rows(
            f"SELECT TOP 1 {workflow} AS workflow_id FROM {table} "
            f"WHERE {task} = {literal(LOAD_TASK)} "
            f"ORDER BY {completed} DESC"
        )
    ]
    if not latest:
        return None
    workflow_id = latest[0]
    counts: dict[str, int] = {}
    window_started = None
    window_completed = None
    for row in catalogue.rows(
        f"SELECT {result} AS result, "
        f"MIN({started}) AS started_datetime, "
        f"MAX({completed}) AS completed_datetime, "
        "COUNT(*) AS row_count "
        f"FROM {table} "
        f"WHERE {workflow} = {literal(workflow_id)} AND {task} = {literal(LOAD_TASK)} "
        f"GROUP BY {result}"
    ):
        values = dict(row)
        counts[_internal_result(str(values.get("result") or ""))] = int(
            values.get("row_count") or 0
        )
        window_started = _earliest(window_started, values.get("started_datetime"))
        window_completed = _latest(window_completed, values.get("completed_datetime"))
    return {
        "workflow_id": workflow_id,
        "started_datetime": window_started,
        "completed_datetime": window_completed,
        "counts": counts,
    }


def load_statistics(
    catalogue: Any, *, workflow_id: str, limit: int = DEFAULT_LIMIT
) -> tuple[dict, ...]:
    """What one workflow's loads did, from ``_.LoadStatistic``.

    Scoped to the workflow, so the window is what that run touched, in identity
    order and capped at ``limit``.
    """

    found = [
        row
        for row in read_table(catalogue, LOAD_STATISTIC)
        if str(row.get("workflow_id") or "") == workflow_id
    ]
    found.sort(
        key=lambda row: (
            str(row.get("item_type") or ""),
            str(row.get("item_name") or ""),
            str(row.get("schema_name") or ""),
            str(row.get("object_name") or ""),
        )
    )
    return tuple(found[:limit])


def _internal_result(stored: str) -> str:
    """One stored ``[Result]`` value back in the vocabulary Weaver writes."""

    for internal, public in LOAD_RESULT_VOCABULARY.items():
        if public.casefold() == stored.casefold():
            return internal
    return stored.casefold()


def _earliest(current, candidate):
    return candidate if _replaces(current, candidate, earlier=True) else current


def _latest(current, candidate):
    return candidate if _replaces(current, candidate, earlier=False) else current


def _replaces(current, candidate, *, earlier: bool) -> bool:
    if candidate is None:
        return False
    if current is None:
        return True
    return candidate < current if earlier else candidate > current


__all__ = [
    "DEFAULT_LIMIT",
    "LOAD_TASK",
    "latest_load_workflow",
    "load_statistics",
]
