"""The history behind the estate's current load state.

``_.LoadStatus`` holds one row per loadable object, carrying the workflow whose
load produced that object's current state. :func:`read_load_history` starts
there and retrieves the ``_.LoadStatistic`` row behind each of those rows,
matching on workflow and the object's four-part logical identity. Current state
spans as many workflows as it took to reach, so the window is not one workflow's
worth: a later partial load explains its own objects and leaves the rest
explained by the load that last touched them.

``_.LoadStatistic`` grows with the estate's age, so nothing reads it whole. The
match and the limit are in the statement.

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

#: How many statistic rows one window may hold. It bounds a pathological estate
#: rather than an ordinary one: the match is against current state, so the
#: window is the size of the estate and not of its history.
DEFAULT_LIMIT = 500

#: What identifies one object's load on both sides of the match. The workflow
#: alone would carry every other object that workflow loaded, and the object
#: alone would carry every historical execution of it.
_MATCH = ("workflow_id", "item_type", "item_name", "schema_name", "object_name")

#: What orders a window's statistic rows, so its prefix is the same prefix every
#: time it is read.
_STATISTIC_ORDER = ("item_type", "item_name", "schema_name", "object_name")

#: The alias current state is matched under, inside the statistic read.
_STATUS = "weaver_status"


@dataclass(frozen=True)
class LoadHistory:
    """What the estate's current load state is, and the loads that produced it.

    ``statistics`` are the ``_.LoadStatistic`` rows behind the current
    ``_.LoadStatus`` rows, capped. ``workflow_ids`` are the workflows those rows
    came from, so a caller can say how many loads the current state took without
    one of them standing for the rest. ``counts`` are current state's own
    results, from ``_.LoadStatus``.

    The runtime records orchestrated and standalone loads under one task type,
    so this says nothing about who started any of them.
    """

    workflow_ids: tuple[str, ...] = ()
    started_at: datetime | None = None
    completed_at: datetime | None = None
    #: How each loadable object's current state ended, counted by result.
    counts: Mapping[str, int] = field(default_factory=dict)
    statistics: tuple[Row, ...] = ()
    #: Whether the window filled its limit, so what it holds is a prefix.
    is_truncated: bool = False


def read_load_history(
    catalogue: Any, *, limit: int = DEFAULT_LIMIT
) -> LoadHistory | None:
    """Current load state, and the statistics behind it.

    ``None`` where ``_.LoadStatus`` holds no row, which is bootstrap: nothing
    has settled a load yet.
    """

    counts = _counts(catalogue)
    if counts is None:
        return None
    statistics = _statistics(catalogue, limit=limit)
    started, completed = _span(statistics)
    return LoadHistory(
        workflow_ids=_workflow_ids(statistics),
        started_at=started,
        completed_at=completed,
        counts=MappingProxyType(counts),
        statistics=statistics,
        is_truncated=len(statistics) == limit,
    )


def _counts(catalogue: Any) -> dict[str, int] | None:
    """How current ``_.LoadStatus`` rows ended, counted by result.

    ``None`` where the table holds nothing, which is what separates bootstrap
    from an estate whose every object failed.
    """

    if catalogue.columns_of(LOAD_STATUS) is None:
        return None
    result = identifier(LOAD_STATUS.public_name_of("result"))
    counts: dict[str, int] = {}
    for row in catalogue.rows(
        f"SELECT {result} AS result, COUNT(*) AS row_count "
        f"FROM {qualified_name(LOAD_STATUS)} "
        f"GROUP BY {result}"
    ):
        values = dict(row)
        counts[_internal_result(str(values.get("result") or ""))] = int(
            values.get("row_count") or 0
        )
    return counts or None


def _statistics(catalogue: Any, *, limit: int) -> tuple[Row, ...]:
    """The statistic row behind each current status row, bounded by the engine.

    A semi-join rather than an inner one: ``_.LoadStatus`` is keyed by the
    object, so matching cannot multiply a statistic row, and the read stays one
    table's projection.
    """

    return read_table(
        catalogue,
        LOAD_STATISTIC,
        predicate=_matches_current_state(),
        order=_STATISTIC_ORDER,
        top=limit,
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


def _workflow_ids(statistics: tuple[Row, ...]) -> tuple[str, ...]:
    """Every workflow the window's rows came from, in the order they appear."""

    found = {}
    for row in statistics:
        workflow_id = str(row.get("workflow_id") or "")
        if workflow_id:
            found[workflow_id] = None
    return tuple(found)


def _span(statistics: tuple[Row, ...]):
    """When the window's loads started and when the last of them finished."""

    started = None
    completed = None
    for row in statistics:
        started = _earliest(started, row.get("started_datetime"))
        completed = _latest(completed, row.get("completed_datetime"))
    return started, completed


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


__all__ = ["DEFAULT_LIMIT", "LoadHistory", "read_load_history"]
