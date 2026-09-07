"""What a build writes into the catalogue's current-state tables.

A current-state row describes one object's current incarnation: how far it has
been loaded, how its last load ended, what its last validation found. A build
decides two things about those rows.

.. code-block:: text

    establish    the object is installed, so it has explicit state
    invalidate   the object is no longer installed, so its row goes

``Pending`` is how Weaver says "not run yet", and the bookmark sentinel how it
says no clean load has set a cursor.

The decision is structured intent, so two things can read it:

.. code-block:: text

    the installer     renders a scoped MERGE and a scoped DELETE per table
    a Catalogue       applies the same rows, so a plan can be checked in memory

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
#: Version 2 added ``establish``, the rows a build writes.
FORMAT_VERSION = 2


@dataclass(frozen=True)
class RuntimeStateInvalidation:
    """One current-state table, and the keyed rows a build removes.

    ``rows`` carry the table's key columns alone. The row is being removed, so
    its other values are not part of the decision.
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


@dataclass(frozen=True)
class RuntimeStateEstablishment:
    """One current-state table, and the whole rows a build writes into it."""

    table: str
    rows: tuple[Mapping[str, Any], ...]

    def __post_init__(self) -> None:
        if not self.table:
            raise BuildError("a runtime-state establishment names a table")

    def to_mapping(self) -> dict[str, Any]:
        return {"table": self.table, "rows": [dict(row) for row in self.rows]}

    @classmethod
    def from_mapping(cls, mapping: Mapping[str, Any]) -> "RuntimeStateEstablishment":
        return cls(
            table=mapping["table"],
            rows=tuple(dict(row) for row in mapping.get("rows", ())),
        )


def invalidation_payload(
    invalidations: Sequence[RuntimeStateInvalidation],
    establishments: Sequence[RuntimeStateEstablishment] = (),
) -> bytes:
    """The frozen intent document one reconciliation action carries."""

    document = {
        "format_version": FORMAT_VERSION,
        "establish": [one.to_mapping() for one in establishments],
        "invalidate": [one.to_mapping() for one in invalidations],
    }
    return (json.dumps(document, indent=2, ensure_ascii=False) + "\n").encode("utf-8")


def read_invalidation(payload: bytes):
    """The intent a payload carries: establishments, then invalidations."""

    document = json.loads(payload.decode("utf-8"))
    version = document.get("format_version")
    if version != FORMAT_VERSION:
        raise BuildError(
            f"unsupported runtime state format_version {version!r}; "
            f"expected {FORMAT_VERSION}"
        )
    return (
        tuple(
            RuntimeStateEstablishment.from_mapping(one)
            for one in document.get("establish", ())
        ),
        tuple(
            RuntimeStateInvalidation.from_mapping(one)
            for one in document.get("invalidate", ())
        ),
    )


def render_establishment(
    establishments: Iterable[RuntimeStateEstablishment],
) -> tuple[str, ...]:
    """One scoped MERGE per table."""

    from .reconcile import InstallationScope, InstallationScopes
    from .render import render_merge
    from .tables import SCOPE_ITEM_NAME, SCOPE_ITEM_TYPE
    from .tables import table as catalogue_table

    statements = []
    for one in establishments:
        if not one.rows:
            continue
        scope = InstallationScopes(
            tuple(
                sorted(
                    {
                        InstallationScope(
                            str(row.get(SCOPE_ITEM_TYPE) or ""),
                            str(row.get(SCOPE_ITEM_NAME) or ""),
                        )
                        for row in one.rows
                    },
                    key=str,
                )
            )
        )
        statement = render_merge(
            catalogue_table(one.table), list(one.rows), scope=scope
        )
        if statement is not None:
            statements.append(statement)
    return tuple(statements)


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


def with_established(
    rows: Mapping[Any, Mapping[str, tuple[Row, ...]]],
    establishments: Iterable[RuntimeStateEstablishment],
) -> dict[Any, dict[str, tuple[Row, ...]]]:
    """These catalogue rows, with each established row written over its key."""

    from ..declaration.model import WeaverItemId
    from .tables import SCOPE_ITEM_NAME, SCOPE_ITEM_TYPE
    from .tables import table as catalogue_table

    written: dict[Any, dict[str, dict[tuple, Row]]] = {}
    for one in establishments:
        table = catalogue_table(one.table)
        for row in one.rows:
            item = WeaverItemId(
                str(row.get(SCOPE_ITEM_TYPE) or ""), str(row.get(SCOPE_ITEM_NAME) or "")
            )
            key = tuple(row.get(name) for name in table.key)
            written.setdefault(item, {}).setdefault(one.table, {})[key] = dict(row)

    remaining = {item: dict(tables) for item, tables in rows.items()}
    for item, tables in written.items():
        held = remaining.setdefault(item, {})
        for name, by_key in tables.items():
            table = catalogue_table(name)
            kept = [
                row
                for row in held.get(name, ())
                if tuple(row.get(column) for column in table.key) not in by_key
            ]
            held[name] = tuple(kept) + tuple(by_key.values())
    return remaining


__all__ = [
    "FORMAT_VERSION",
    "RuntimeStateEstablishment",
    "RuntimeStateInvalidation",
    "invalidation_payload",
    "read_invalidation",
    "render_establishment",
    "render_invalidation",
    "with_established",
    "without_invalidated",
]
