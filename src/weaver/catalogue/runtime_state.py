"""What a build ends the life of in the catalogue's current-state tables.

A current-state row describes the *current incarnation* of one object: how far
it has been loaded, how its last load ended, what its last validation found.
A build that drops and rebuilds that object, or stops installing it altogether,
ends that incarnation, and the row goes with it.

That decision is held here as structured intent — which table, and which keyed
rows — rather than as the SQL it eventually becomes. Two things read it, and
neither should have to parse a statement to find out what a build meant:

.. code-block:: text

    the installer     renders one scoped DELETE per table and runs it
    a Catalogue       drops the same rows, so a plan can be applied in memory

Historical tables are absent by construction. ``_.Log`` and ``_.LoadStatistic``
record what happened, and what happened does not stop having happened because
the object was rebuilt.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Sequence

from ..errors import BuildError
from .render import Row, render_delete_rows

#: The intent document's version, carried in the payload a bundle freezes.
FORMAT_VERSION = 1


@dataclass(frozen=True)
class RuntimeStateInvalidation:
    """One current-state table, and the keyed rows a build ends the life of.

    ``rows`` carry the table's key columns and nothing else: the row is being
    removed, so its other values are not part of the decision.
    """

    table: str
    rows: tuple[Mapping[str, Any], ...]

    def __post_init__(self) -> None:
        if not self.table:
            raise BuildError("a runtime-state invalidation names a table")

    def keys(self) -> frozenset[tuple]:
        """This table's invalidated rows, as comparable key tuples."""

        return frozenset(tuple(sorted(row.items())) for row in self.rows)

    def to_mapping(self) -> dict[str, Any]:
        return {"table": self.table, "rows": [dict(row) for row in self.rows]}

    @classmethod
    def from_mapping(cls, mapping: Mapping[str, Any]) -> "RuntimeStateInvalidation":
        return cls(
            table=mapping["table"],
            rows=tuple(dict(row) for row in mapping.get("rows", ())),
        )


def invalidation_payload(
    invalidations: Sequence[RuntimeStateInvalidation],
) -> bytes:
    """The frozen intent document one reconciliation action carries."""

    document = {
        "format_version": FORMAT_VERSION,
        "invalidate": [one.to_mapping() for one in invalidations],
    }
    return (json.dumps(document, indent=2, ensure_ascii=False) + "\n").encode("utf-8")


def read_invalidation(payload: bytes) -> tuple[RuntimeStateInvalidation, ...]:
    """The intent an action's payload carries, refusing a version it cannot read."""

    document = json.loads(payload.decode("utf-8"))
    version = document.get("format_version")
    if version != FORMAT_VERSION:
        raise BuildError(
            f"unsupported runtime state format_version {version!r}; "
            f"expected {FORMAT_VERSION}"
        )
    return tuple(
        RuntimeStateInvalidation.from_mapping(one)
        for one in document.get("invalidate", ())
    )


def render_invalidation(
    invalidations: Iterable[RuntimeStateInvalidation],
) -> tuple[str, ...]:
    """One scoped DELETE per table, in the order the intent names them.

    Named rows rather than a predicate over the estate: a build that ended one
    object's incarnation says so in one row, and says nothing at all about the
    objects it left alone.
    """

    from .tables import table as catalogue_table

    statements = []
    for one in invalidations:
        if not one.rows:
            continue
        statement = render_delete_rows(catalogue_table(one.table), list(one.rows))
        if statement is not None:
            statements.append(statement)
    return tuple(statements)


def without_invalidated(
    rows: Mapping[Any, Mapping[str, tuple[Row, ...]]],
    invalidations: Iterable[RuntimeStateInvalidation],
) -> dict[Any, dict[str, tuple[Row, ...]]]:
    """These catalogue rows with every invalidated row removed.

    Matched on the invalidated row's own columns, which are the table's key, so
    a row is removed when the intent names its identity rather than when it
    happens to agree about some other column.
    """

    removed: dict[str, frozenset[tuple]] = {}
    for one in invalidations:
        removed[one.table] = removed.get(one.table, frozenset()) | one.keys()
    remaining: dict[Any, dict[str, tuple[Row, ...]]] = {}
    for item, tables in rows.items():
        kept_tables: dict[str, tuple[Row, ...]] = {}
        for name, table_rows in tables.items():
            keys = removed.get(name)
            if keys is None:
                kept_tables[name] = tuple(table_rows)
                continue
            kept_tables[name] = tuple(
                row for row in table_rows if not _is_named(row, keys)
            )
        remaining[item] = kept_tables
    return remaining


def _is_named(row: Row, keys: frozenset[tuple]) -> bool:
    """Whether one row is named by any of these invalidated identities."""

    return any(all(row.get(column) == value for column, value in key) for key in keys)


__all__ = [
    "FORMAT_VERSION",
    "RuntimeStateInvalidation",
    "invalidation_payload",
    "read_invalidation",
    "render_invalidation",
    "without_invalidated",
]
