"""Read the load statistics that match current load state.

``_.LoadStatus`` has one current row per loadable object. ``_.LoadStatistic`` is
historical, so reads are bounded to matching workflow and object identities.
Blocked loads have status but no statistic.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .reader import read_table
from .render import Row
from .tables import LOAD_STATISTIC, LOAD_STATUS
from .tsql import identifier, qualified_name

# Workflow and object identity together select one object's load execution.
_MATCH = ("workflow_id", "item_type", "item_name", "schema_name", "object_name")

_STATISTIC_ORDER = ("item_type", "item_name", "schema_name", "object_name")

_STATUS = "weaver_status"


@dataclass(frozen=True)
class LoadHistory:
    """The statistics behind current load state."""

    statistics: tuple[Row, ...] = ()


def read_load_history(catalogue: Any) -> LoadHistory | None:
    """Return ``None`` while the catalogue has no ``_.LoadStatus``."""

    if catalogue.columns_of(LOAD_STATUS) is None:
        return None
    return LoadHistory(statistics=_statistics(catalogue))


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


__all__ = ["LoadHistory", "read_load_history"]
