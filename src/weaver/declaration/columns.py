"""Validate physical columns for declared and inferred tables.

The functions check column names, metadata references, and identity-column
collisions after the engine reports a query's output shape.
"""

from __future__ import annotations

from ..errors import BuildError
from .metadata import SesDocument


def metadata_column_references(document: SesDocument) -> tuple[tuple[str, str], ...]:
    """The ``(label, column)`` pairs this document's metadata references.

    Every pair must resolve to a produced business column. The set is the same
    whether the table is declared or inferred; only *when* it is checked differs.
    """

    references: list[tuple[str, str]] = []
    references.extend(("Primary key", column) for column in document.primary_key)
    references.extend(
        ("Unique keys", column)
        for unique_key in document.unique_keys
        for column in unique_key
    )
    # Only this side of a relationship: the parent's columns belong to the parent
    # and are checked when it is built, not here.
    references.extend(
        ("Foreign keys", column)
        for foreign_key in document.foreign_keys
        for column in foreign_key.columns
    )
    references.extend(("Not null", column) for column in document.declared_not_null)
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
    values the bundle froze (how-does-build-work §2).
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
        identity=document.identity,
    )


def validate_build_columns(
    qualified: str,
    query_columns: tuple[str, ...],
    *,
    declared_columns: tuple[str, ...] | None,
    references: tuple[tuple[str, str], ...],
    identity: str | None = None,
) -> tuple[str, ...]:
    """The rule core, over plain data so the frozen payload can drive it.

    ``query_columns`` are what the engine reported for the object's query.
    ``declared_columns`` are the declared business-column names, or ``None`` when
    the table is inferred. ``references`` are the ``(label, column)`` pairs from
    :func:`metadata_column_references`. ``identity`` names the Weaver-managed
    surrogate column, when one is declared: it is not a business column, so it may
    not clash with the query's output, but the primary key may name it. Returns
    the physical business columns in order — declared names when declared
    (authoritative), else the query's own — and raises :class:`BuildError` on any
    violation.
    """

    _reject_duplicate_query_columns(qualified, query_columns)

    if declared_columns is not None:
        _require_set_equivalence(qualified, declared_columns, query_columns)
        business_columns = tuple(declared_columns)
    else:
        business_columns = tuple(query_columns)

    _reject_identity_collision(qualified, identity, business_columns)
    # The identity column is Weaver's own, so the primary key may name it even
    # though it is not a business column.
    available = business_columns + ((identity,) if identity is not None else ())
    _require_references_exist(qualified, available, references)
    return business_columns


def _reject_identity_collision(
    qualified: str, identity: str | None, business_columns: tuple[str, ...]
) -> None:
    if identity is None:
        return
    if any(identity.lower() == name.lower() for name in business_columns):
        raise BuildError(
            f"{qualified}: Identity {identity!r} collides with a business column — "
            "the identity column is Weaver-managed and must not be one the query "
            "produces or the schema declares."
        )


def _reject_duplicate_query_columns(
    qualified: str, query_columns: tuple[str, ...]
) -> None:
    # Case-insensitive on purpose: two names that differ only by case cannot be
    # told apart reliably downstream, so they are ambiguous, not distinct.
    groups: dict[str, list[str]] = {}
    for column in query_columns:
        groups.setdefault(column.lower(), []).append(column)
    colliding = sorted(", ".join(names) for names in groups.values() if len(names) > 1)
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
