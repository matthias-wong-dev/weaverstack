"""One bounded window of the catalogue's history tables.

``_.Log`` and ``_.LoadStatistic`` grow with the estate's age, so nothing reads
them whole. :func:`read_load_history` reads one window: the most recent load
workflow, and the statistics that workflow's loads appended. The engine does the
filtering and the limiting.

The window is carried on a :class:`weaver.catalogue.state.Catalogue` as
:attr:`~weaver.catalogue.state.Catalogue.load_history`, apart from ``rows``.
``Catalogue.table_rows`` answers for a materialised table, and a window is not
one. Rows come back under the internal keys every other catalogue read uses;
what they mean to an operator is :mod:`weaver.health`'s to say.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from types import MappingProxyType
from typing import Any, Mapping

from .reader import read_table
from .render import Row
from .tables import LOAD_RESULT_VOCABULARY, LOAD_STATISTIC, LOG
from .tsql import identifier, literal, qualified_name

#: The task type an orchestrated load and a standalone object load both record
#: under. Restated rather than imported, because this package sits below
#: :mod:`weaver.run`, and asserted equal to it by
#: ``tests/test_core_boundary.py``.
LOAD_TASK = "load"

#: How many statistic rows one window may hold. A workflow loads what one
#: request selected, so this bounds a pathological estate rather than an
#: ordinary one.
DEFAULT_LIMIT = 500

#: What orders a window's statistic rows, so its prefix is the same prefix every
#: time it is read.
_STATISTIC_ORDER = ("item_type", "item_name", "schema_name", "object_name")


@dataclass(frozen=True)
class LoadHistory:
    """The most recent recorded load activity, as one window.

    ``workflow_id`` correlates every row one workflow produced, and the window
    spans what its ``_.Log`` rows carry. ``statistics`` are that workflow's
    ``_.LoadStatistic`` rows, capped, so a caller reads what a load moved
    without reading the estate's whole history.

    The runtime records orchestrated and standalone loads under one task type,
    so this is the latest load activity and says nothing about who started it.
    """

    workflow_id: str
    started_at: datetime | None = None
    completed_at: datetime | None = None
    #: How the workflow's settled nodes ended, counted by result.
    counts: Mapping[str, int] = field(default_factory=dict)
    statistics: tuple[Row, ...] = ()
    #: Whether the window filled its limit, so what it holds is a prefix.
    is_truncated: bool = False


def read_load_history(
    catalogue: Any, *, limit: int = DEFAULT_LIMIT
) -> LoadHistory | None:
    """The most recent load workflow, and the statistics it appended.

    ``None`` where ``_.Log`` holds no load, which is bootstrap: nothing has run
    yet.
    """

    workflow_id = _latest_workflow(catalogue)
    if workflow_id is None:
        return None
    started, completed, counts = _window(catalogue, workflow_id)
    statistics = _statistics(catalogue, workflow_id=workflow_id, limit=limit)
    return LoadHistory(
        workflow_id=workflow_id,
        started_at=started,
        completed_at=completed,
        counts=MappingProxyType(counts),
        statistics=statistics,
        is_truncated=len(statistics) == limit,
    )


def _latest_workflow(catalogue: Any) -> str | None:
    """The load workflow whose rows carry the latest completion instant."""

    if catalogue.columns_of(LOG) is None:
        return None
    task = identifier(LOG.public_name_of("task_type"))
    completed = identifier(LOG.public_name_of("completed_datetime"))
    workflow = identifier(LOG.public_name_of("workflow_id"))
    found = [
        str(dict(row)["workflow_id"])
        for row in catalogue.rows(
            f"SELECT TOP 1 {workflow} AS workflow_id FROM {qualified_name(LOG)} "
            f"WHERE {task} = {literal(LOAD_TASK)} "
            f"ORDER BY {completed} DESC"
        )
    ]
    return found[0] if found else None


def _window(catalogue: Any, workflow_id: str):
    """What one workflow spanned, and how its settled nodes ended."""

    task = identifier(LOG.public_name_of("task_type"))
    completed = identifier(LOG.public_name_of("completed_datetime"))
    started = identifier(LOG.public_name_of("started_datetime"))
    workflow = identifier(LOG.public_name_of("workflow_id"))
    result = identifier(LOG.public_name_of("result"))

    counts: dict[str, int] = {}
    window_started = None
    window_completed = None
    for row in catalogue.rows(
        f"SELECT {result} AS result, "
        f"MIN({started}) AS started_datetime, "
        f"MAX({completed}) AS completed_datetime, "
        "COUNT(*) AS row_count "
        f"FROM {qualified_name(LOG)} "
        f"WHERE {workflow} = {literal(workflow_id)} AND {task} = {literal(LOAD_TASK)} "
        f"GROUP BY {result}"
    ):
        values = dict(row)
        counts[_internal_result(str(values.get("result") or ""))] = int(
            values.get("row_count") or 0
        )
        window_started = _earliest(window_started, values.get("started_datetime"))
        window_completed = _latest(window_completed, values.get("completed_datetime"))
    return window_started, window_completed, counts


def _statistics(catalogue: Any, *, workflow_id: str, limit: int) -> tuple[Row, ...]:
    """What one workflow's loads did, bounded by the engine.

    The predicate and the limit are in the statement, so an estate with years of
    statistics costs one workflow's worth to read.
    """

    workflow = identifier(LOAD_STATISTIC.public_name_of("workflow_id"))
    return read_table(
        catalogue,
        LOAD_STATISTIC,
        predicate=f"{workflow} = {literal(workflow_id)}",
        order=_STATISTIC_ORDER,
        top=limit,
    )


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


__all__ = ["DEFAULT_LIMIT", "LOAD_TASK", "LoadHistory", "read_load_history"]
