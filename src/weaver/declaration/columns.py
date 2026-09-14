"""Validate the columns produced for declared and inferred tables."""

from __future__ import annotations

from ..errors import BuildError
from .metadata import SesDocument


def metadata_column_references(document: SesDocument) -> tuple[tuple[str, str], ...]:
    """Return metadata references that must resolve to built columns."""

    references: list[tuple[str, str]] = []
    references.extend(("Primary key", column) for column in document.primary_key)
    references.extend(
        ("Unique keys", column)
        for unique_key in document.unique_keys
        for column in unique_key
    )
    # Parent columns are checked when the parent is built.
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
    # An inferred table has no declared columns to carry its column notes.
    if not document.has_declared_schema:
        notes = document.raw.get("Column notes") or {}
        if isinstance(notes, dict):
            references.extend(("Column notes", str(name)) for name in notes)
    return tuple(references)


def resolve_build_columns(
    document: SesDocument, query_columns: tuple[str, ...]
) -> tuple[str, ...]:

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
    """Resolve physical business columns from the bundle's frozen column data.

    An identity is not a business column, but metadata may refer to it.
    """

    _reject_duplicate_query_columns(qualified, query_columns)

    if declared_columns is not None:
        _require_set_equivalence(qualified, declared_columns, query_columns)
        business_columns = tuple(declared_columns)
    else:
        business_columns = tuple(query_columns)

    _reject_identity_collision(qualified, identity, business_columns)
    # Metadata may name the managed identity even though the query does not.
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
            f"{qualified}: Identity {identity!r} duplicates a query or declared "
            "column. Choose a different Identity column name."
        )


def _reject_duplicate_query_columns(
    qualified: str, query_columns: tuple[str, ...]
) -> None:
    # Physical column names must also be distinct under case-insensitive lookup.
    groups: dict[str, list[str]] = {}
    for column in query_columns:
        groups.setdefault(column.lower(), []).append(column)
    colliding = sorted(", ".join(names) for names in groups.values() if len(names) > 1)
    if colliding:
        raise BuildError(
            f"{qualified}: query column names are ambiguous when compared "
            "case-insensitively: "
            + "; ".join(colliding)
            + ". Rename them so each column name is distinct."
        )


def _require_set_equivalence(
    qualified: str,
    declared: tuple[str, ...],
    query_columns: tuple[str, ...],
) -> None:
    # Declared and query column names must match case exactly.
    declared_set = set(declared)
    query_set = set(query_columns)

    missing = [name for name in declared if name not in query_set]
    if missing:
        raise BuildError(
            f"{qualified}: the query does not return these declared columns with "
            "the same spelling and case: "
            + ", ".join(missing)
            + ". Return them under their declared names or update the schema."
        )

    extra = [name for name in query_columns if name not in declared_set]
    if extra:
        raise BuildError(
            f"{qualified}: the query returns columns not in the declared schema: "
            + ", ".join(extra)
            + ". Declare them with the same spelling and case, or remove them from "
            "the query."
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
                f"{qualified}: {label} names column {column!r}, but the built table "
                "does not contain that exact name. Update the declaration to name "
                "an existing column with matching case."
            )
