"""The one column-validation model both build engines share.

A SQL-backed table's physical business columns are known in two ways. A declared
schema states them up front and is validated at parse time by
:func:`weaver.ses.metadata._validate_columns`. An *inferred* table has no
declared schema, so the same guards cannot run until the query's output shape is
known — which, per build-philosophy §7.3, is at build, inside the install action.

This module is that deferred guard, expressed once so Spark and T-SQL agree. The
Spark executor calls :func:`resolve_build_columns` directly with the columns its
``DataFrame`` reported; the generated T-SQL script mirrors the identical rules in
SQL because a Warehouse only knows its query's shape server-side. Both draw the
set of column-referencing metadata from :func:`metadata_column_references`, so
neither can drift from the other.

A column name is an exact, case-sensitive Weaver contract, even where the target
engine would fold case. The rules:

- a query may not produce two columns whose names collide **case-insensitively**
  — ``CustomerId`` and ``customerid`` together are ambiguous, and no unambiguous
  table could be built from them;
- when a schema is declared, the declared column set and the query's output set
  must be equivalent **by exact name** — every declared column returned under the
  same spelling, no undeclared column produced — while types, order, width,
  precision and nullability are not compared, so a declaration may deliberately
  be wider or more stable;
- every column named by ``Primary key``, ``Not null``, ``Identity``,
  ``Comparison columns`` or ``Column notes`` must exist among the business
  columns **under exactly its declared spelling**.

Case is thus exact for identity and equivalence, but case-insensitive for the
ambiguity guard — a name that only sometimes matches is not a name Weaver can
rely on.
"""

from __future__ import annotations

from ..errors import BuildError
from .metadata import SesDocument

#: The metadata fields that name columns, paired with the label an error uses.
#: One definition, shared by the Python guard and mirrored by the T-SQL template,
#: so "which metadata references a column" has a single source of truth.
_REFERENCE_LABELS = (
    "Primary key",
    "Not null",
    "Identity",
    "Comparison columns",
    "Column notes",
)


def metadata_column_references(document: SesDocument) -> tuple[tuple[str, str], ...]:
    """The ``(label, column)`` pairs this document's metadata references.

    Every pair must resolve to a produced business column. The set is the same
    whether the table is declared or inferred; only *when* it is checked differs.
    """

    references: list[tuple[str, str]] = []
    references.extend(("Primary key", column) for column in document.primary_key)
    references.extend(("Not null", column) for column in document.declared_not_null)
    if document.identity is not None:
        references.append(("Identity", document.identity))
    references.extend(
        ("Comparison columns", column)
        for column in document.declared_comparison_columns
    )
    references.extend(
        ("Column notes", column.name)
        for column in document.schema
        if column.note is not None
    )
    # For an inferred table the notes live on no declared column, so read them
    # from the raw metadata block instead.
    if not document.has_declared_schema:
        notes = document.raw.get("Column notes") or {}
        if isinstance(notes, dict):
            references.extend(("Column notes", str(name)) for name in notes)
    return tuple(references)


def resolve_build_columns(
    document: SesDocument, query_columns: tuple[str, ...]
) -> tuple[str, ...]:
    """Validate a built table's columns from a live document — the tests' entry.

    A convenience wrapper over :func:`validate_build_columns` that reads the
    declared columns and metadata references off ``document``. The installer never
    uses this — it holds no document — and calls the data-level function with the
    values the bundle froze (build-philosophy §7.3).
    """

    declared = (
        tuple(column.name for column in document.schema)
        if document.has_declared_schema
        else None
    )
    return validate_build_columns(
        document.qualified,
        query_columns,
        declared_columns=declared,
        references=metadata_column_references(document),
    )


def validate_build_columns(
    qualified: str,
    query_columns: tuple[str, ...],
    *,
    declared_columns: tuple[str, ...] | None,
    references: tuple[tuple[str, str], ...],
) -> tuple[str, ...]:
    """The rule core, over plain data so the frozen payload can drive it.

    ``query_columns`` are what the engine reported for the object's query.
    ``declared_columns`` are the declared business-column names, or ``None`` when
    the table is inferred. ``references`` are the ``(label, column)`` pairs from
    :func:`metadata_column_references`. Returns the physical business columns in
    order — declared names when declared (authoritative), else the query's own —
    and raises :class:`BuildError` on any violation.
    """

    _reject_duplicate_query_columns(qualified, query_columns)

    if declared_columns is not None:
        _require_set_equivalence(qualified, declared_columns, query_columns)
        business_columns = tuple(declared_columns)
    else:
        business_columns = tuple(query_columns)

    _require_references_exist(qualified, business_columns, references)
    return business_columns


def _reject_duplicate_query_columns(
    qualified: str, query_columns: tuple[str, ...]
) -> None:
    # Case-insensitive on purpose: two names that differ only by case cannot be
    # told apart reliably downstream, so they are ambiguous, not distinct.
    groups: dict[str, list[str]] = {}
    for column in query_columns:
        groups.setdefault(column.lower(), []).append(column)
    colliding = sorted(
        ", ".join(names) for names in groups.values() if len(names) > 1
    )
    if colliding:
        raise BuildError(
            f"{qualified}: the query produces columns that collide by name "
            "(case-insensitively): "
            + "; ".join(colliding)
            + " — no unambiguous table can be built. Give them distinct names."
        )


def _require_set_equivalence(
    qualified: str,
    declared: tuple[str, ...],
    query_columns: tuple[str, ...],
) -> None:
    # Exact, case-sensitive: a declared name and a query name that differ only by
    # case are two different columns, and the declaration must win exactly.
    declared_set = set(declared)
    query_set = set(query_columns)

    missing = [name for name in declared if name not in query_set]
    if missing:
        raise BuildError(
            f"{qualified}: declared column(s) not returned by the query under the "
            "same case: "
            + ", ".join(missing)
            + ". A declared schema must match the query's column set exactly by "
            "name (types aside)."
        )

    extra = [name for name in query_columns if name not in declared_set]
    if extra:
        raise BuildError(
            f"{qualified}: the query returns column(s) not in the declared schema "
            "(names are case-sensitive): "
            + ", ".join(extra)
            + ". Declare them under the same spelling, or drop them from the query."
        )


def _require_references_exist(
    qualified: str,
    business_columns: tuple[str, ...],
    references: tuple[tuple[str, str], ...],
) -> None:
    available = set(business_columns)
    for label, column in references:
        if column not in available:
            raise BuildError(
                f"{qualified}: {label} names column {column!r}, which the built "
                "table does not have under that exact name (names are "
                "case-sensitive)."
            )
