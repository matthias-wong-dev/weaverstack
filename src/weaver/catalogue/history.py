"""Current load state, and the statistics that explain it.

``_.LoadStatus`` is the estate's current load state: one row per loadable
object, carrying how that object's load ended, when, and the workflow that ran
it. :func:`read_load_history` summarises that table and nothing else, so the
summary covers every current object including the ones no statistic describes.
A Blocked load settles a status row and appends no ``_.LoadStatistic`` row.

``_.LoadStatistic`` then enriches those states with what each load moved,
matched on the workflow and the object's four-part logical identity:

.. code-block:: text

    _.LoadStatus                     the current-state summary
        + workflow id
        + item type, item name
        + schema name, object name
             ↓
    _.LoadStatistic                  what those loads moved

``_.LoadStatistic`` accumulates, so it is never read whole. The match is the
bound: it holds the read to the current estate, which is one row per loadable
object at most.

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
from .tables import LOAD_RESULT_VOCABULARY, LOAD_STATISTIC, LOAD_STATUS
from .tsql import identifier, qualified_name

#: What identifies one object's load on both sides of the match. The workflow
#: alone would carry every other object that workflow loaded, and the object
#: alone would carry every historical execution of it.
_MATCH = ("workflow_id", "item_type", "item_name", "schema_name", "object_name")

#: What orders the statistic rows, so a report lists them the same way twice.
_STATISTIC_ORDER = ("item_type", "item_name", "schema_name", "object_name")

#: The alias current state is matched under, inside the statistic read.
_STATUS = "weaver_status"


@dataclass(frozen=True)
class LoadHistory:
    """What the estate's current load state is, and what those loads moved.

    ``counts``, ``workflow_ids``, ``started_at`` and ``completed_at`` summarise
    ``_.LoadStatus``, so they cover every current object. ``statistics`` are the
    ``_.LoadStatistic`` rows behind those states, which is a subset: a Blocked
    load has a status and no statistic.

    ``workflow_ids`` are sorted, which is an order for reading and not a
    chronology. Current state spans as many workflows as it took to reach,
    because a partial load leaves the objects it did not touch explained by the
    load that last did.

    The runtime records orchestrated and standalone loads under one task type,
    so none of this says who started any of them.
    """

    workflow_ids: tuple[str, ...] = ()
    started_at: datetime | None = None
    completed_at: datetime | None = None
    #: How each loadable object's current state ended, counted by result.
    counts: Mapping[str, int] = field(default_factory=dict)
    statistics: tuple[Row, ...] = ()


def read_load_history(catalogue: Any) -> LoadHistory | None:
    """Current load state, and the statistics behind it.

    ``None`` where ``_.LoadStatus`` holds no row, which is bootstrap: nothing
    has settled a load yet.
    """

    summary = _current_state(catalogue)
    if summary is None:
        return None
    counts, workflow_ids, started, completed = summary
    return LoadHistory(
        workflow_ids=workflow_ids,
        started_at=started,
        completed_at=completed,
        counts=MappingProxyType(counts),
        statistics=_statistics(catalogue),
    )


def _current_state(catalogue: Any):
    """``_.LoadStatus`` summarised: results, workflows, and the span they cover.

    One aggregate, so the cost is the number of result and workflow pairs rather
    than the number of objects. ``None`` where the table holds nothing, which is
    what separates bootstrap from an estate whose every object failed.
    """

    if catalogue.columns_of(LOAD_STATUS) is None:
        return None
    result = identifier(LOAD_STATUS.public_name_of("result"))
    workflow = identifier(LOAD_STATUS.public_name_of("workflow_id"))
    started = identifier(LOAD_STATUS.public_name_of("started_datetime"))
    completed = identifier(LOAD_STATUS.public_name_of("completed_datetime"))

    counts: dict[str, int] = {}
    workflow_ids: set[str] = set()
    window_started = None
    window_completed = None
    for row in catalogue.rows(
        f"SELECT {result} AS result, {workflow} AS workflow_id, "
        "COUNT(*) AS row_count, "
        f"MIN({started}) AS started_datetime, "
        f"MAX({completed}) AS completed_datetime "
        f"FROM {qualified_name(LOAD_STATUS)} "
        f"GROUP BY {result}, {workflow}"
    ):
        values = dict(row)
        outcome = _internal_result(str(values.get("result") or ""))
        counts[outcome] = counts.get(outcome, 0) + int(values.get("row_count") or 0)
        workflow_id = str(values.get("workflow_id") or "")
        if workflow_id:
            workflow_ids.add(workflow_id)
        window_started = _earliest(window_started, values.get("started_datetime"))
        window_completed = _latest(window_completed, values.get("completed_datetime"))
    if not counts:
        return None
    return counts, tuple(sorted(workflow_ids)), window_started, window_completed


def _statistics(catalogue: Any) -> tuple[Row, ...]:
    """The statistic row behind each current status row, matched by the engine.

    A semi-join rather than an inner one: ``_.LoadStatus`` is keyed by the
    object, so matching cannot multiply a statistic row, and the read stays one
    table's projection.
    """

    return read_table(
        catalogue,
        LOAD_STATISTIC,
        predicate=_matches_current_state(),
        order=_STATISTIC_ORDER,
    )


def _matches_current_state() -> str:
    """A statistic row that one current ``_.LoadStatus`` row points at."""

    status = identifier(_STATUS)
    conditions = " AND ".join(
        f"{status}.{identifier(LOAD_STATUS.public_name_of(name))} "
        f"= {qualified_name(LOAD_STATISTIC)}."
        f"{identifier(LOAD_STATISTIC.public_name_of(name))}"
        for name in _MATCH
    )
    return (
        f"EXISTS (SELECT 1 FROM {qualified_name(LOAD_STATUS)} AS {status} "
        f"WHERE {conditions})"
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


__all__ = ["LoadHistory", "read_load_history"]
