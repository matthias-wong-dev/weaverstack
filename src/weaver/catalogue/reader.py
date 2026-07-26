"""Reading the catalogue through its fixed expected schema.

A build compares what the catalogue holds with what the repository now declares,
so it has to read tables that may not be there yet and may be an older shape than
this Weaver knows about. Both are ordinary states, not faults:

**Bootstrap.** The very first build has no catalogue at all — the tables are
created by the build that then writes to them. A missing table reads as no rows.

**Upgrade.** A Weaver that adds a column runs against a catalogue built by an
older one. A missing column reads as a typed null, so newer code compares
successfully against an older shape and the next build repairs it. An unexpected
extra column is ignored, which is the other half: an older Weaver must not choke
on a catalogue a newer one extended.

What must **not** happen is a genuine failure being read as an empty catalogue. A
permission error, a corrupt Delta log, an unavailable store or a broken session
that returned "no rows" would make the next build's comparison conclude that
everything is new — and, once drop policy lands, that everything the catalogue no
longer mentions may be removed. So the absence check is deliberately narrow: only
Spark's own ``TABLE_OR_VIEW_NOT_FOUND`` is absence. Everything else propagates.

That asymmetry is the whole design of this module. Tolerance is cheap when it is
specific and dangerous when it is a bare ``except``.

**A read names the Lakehouse it reads from.** The catalogue lives in the Weaver
Lakehouse; a build's other work is aimed at a destination Lakehouse; one session
serves both. So a read takes a :class:`~weaver.spark.catalogue.SparkCatalogue`
rather than a bare session — asking "the catalogue" of whatever the session is
attached to would answer for the wrong Lakehouse, and answer *plausibly*, which
is the failure mode this whole area exists to remove.
"""

from __future__ import annotations

from typing import Any

from .render import InstallationScope, Row, identifier, qualified_name
from .tables import CatalogueTable

#: Spark's error class for a table or view that is not registered. A missing
#: *schema* reports the same class, which is what we want — an installation whose
#: schema `_` has never been created is as absent as one whose table has not.
_ABSENT = frozenset({"TABLE_OR_VIEW_NOT_FOUND"})


def _is_absent(exception: Exception) -> bool:
    """Whether this exception means "not created yet" rather than "went wrong".

    Keyed on Spark's error class rather than on message text, so a reworded
    message cannot silently turn an infrastructure failure into an empty read. The
    message is consulted only when no class is available, which is the case for a
    session or connector that raises a plain error.
    """

    error_class = getattr(exception, "getErrorClass", None)
    if callable(error_class):
        try:
            found = error_class()
        except Exception:  # pragma: no cover - defensive; a broken accessor is not absence
            found = None
        if found:
            return found in _ABSENT
    return False


def read_table(
    catalogue: Any,
    table: CatalogueTable,
    *,
    scope: InstallationScope | None = None,
) -> tuple[Row, ...]:
    """Every row of one catalogue table, projected through its expected schema.

    ``catalogue`` is a :class:`~weaver.spark.catalogue.SparkCatalogue` bound to
    the Weaver Lakehouse — the read has to say where the catalogue is, because
    the session is not necessarily pointed at it.

    ``scope`` narrows the read to one installation, which is what a build wants:
    it compares and writes within one ``(repository, target_type)`` and has no
    business seeing another's rows.

    Returns plain dictionaries of ``str``/``bool``/``None`` — the same shape the
    projection produces, so the two can be compared directly.
    """

    if catalogue is None:
        raise ValueError(
            f"reading {table.qualified} needs a Spark catalogue bound to the Weaver "
            "Lakehouse — the catalogue lives there, and a session alone does not "
            "say which Lakehouse that is"
        )

    name = catalogue.expand(qualified_name(table))
    try:
        existing = catalogue.spark.table(name).columns
    except Exception as exception:
        if _is_absent(exception):
            return ()
        raise

    # Case-folded, because the local metastore lowercases column names where
    # Fabric preserves them, and a column's presence must not depend on that.
    present = {column.lower(): column for column in existing}

    projected = ", ".join(
        _projected_column(column, present.get(column.name.lower()))
        for column in table.columns
    )
    where = ""
    if scope is not None:
        where = f" WHERE {scope.predicate}"

    rows = catalogue.spark.sql(f"SELECT {projected} FROM {name}{where}").collect()
    return tuple(row.asDict() for row in rows)


def _projected_column(column, actual: str | None) -> str:
    """One column of the expected schema, as a select expression.

    Rendered as SQL rather than built with ``pyspark.sql.functions`` so this module
    names no Spark API — the core stays importable without PySpark, and a session
    is only ever duck-typed. It also keeps the projection inspectable as text.
    """

    if actual is None:
        # Older shape: give this Weaver the column it expects, as a typed null.
        return f"CAST(NULL AS {column.type.upper()}) AS {identifier(column.name)}"
    # Cast even when present: an older catalogue may have stored a boolean as a
    # string, and a comparison against a projected boolean would then differ for a
    # row that has not actually changed.
    return (
        f"CAST({identifier(actual)} AS {column.type.upper()}) "
        f"AS {identifier(column.name)}"
    )


def read_installation(
    catalogue: Any, *, scope: InstallationScope, tables=None
) -> dict[str, tuple[Row, ...]]:
    """Every catalogue table, read for one installation.

    Keyed by table name, so a caller compares table by table against the
    projection without repeating the scope — which it could otherwise forget.
    """

    from .tables import CATALOGUE_TABLES

    return {
        table.name: read_table(catalogue, table, scope=scope)
        for table in (tables if tables is not None else CATALOGUE_TABLES)
    }
