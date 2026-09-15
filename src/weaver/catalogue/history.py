"""Read current load state and its matching statistics.

``_.LoadStatus`` has one current row per loadable object. ``_.LoadStatistic`` is
historical, so reads are bounded to matching workflow and object identities.
Blocked loads have status but no statistic.
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

# Workflow and object identity together select one object's load execution.
_MATCH = ("workflow_id", "item_type", "item_name", "schema_name", "object_name")

_STATISTIC_ORDER = ("item_type", "item_name", "schema_name", "object_name")

_STATUS = "weaver_status"


@dataclass(frozen=True)
class LoadHistory:
    """Current load outcomes and the statistics behind them.

    Sorted ``workflow_ids`` are for stable display, not chronology. A partial
    load leaves untouched objects associated with earlier workflows.
    """

    workflow_ids: tuple[str, ...] = ()
    started_at: datetime | None = None
    completed_at: datetime | None = None
    #: How each loadable object's current state ended, counted by result.
    counts: Mapping[str, int] = field(default_factory=dict)
    statistics: tuple[Row, ...] = ()


def read_load_history(catalogue: Any) -> LoadHistory | None:
    """Return ``None`` until ``_.LoadStatus`` has a settled load."""

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
    """Summarise statuses by result and workflow, including their time span."""

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
    """Read statistics selected by current status through a semi-join."""

    return read_table(
        catalogue,
        LOAD_STATISTIC,
        predicate=_matches_current_state(),
        order=_STATISTIC_ORDER,
    )


def _matches_current_state() -> str:
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
