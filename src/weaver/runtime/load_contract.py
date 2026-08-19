"""Load metadata parsed from a declaration or installed module."""

from __future__ import annotations

import inspect
from dataclasses import dataclass

from ..declaration.metadata import (
    AUDIT_DELETE,
    AUDIT_INSERT,
    AUDIT_UPDATE,
    DEFAULT_DELETE_THRESHOLD,
    DEFAULT_STABILITY_ROWS,
    DEFAULT_UPDATE_THRESHOLD,
    FOLDER,
    PYTHON,
    TABLE,
    ObjectId,
    SesDocument,
    audit_column_name,
    parse_document,
)
from ..errors import LoadError


@dataclass(frozen=True)
class LoadContract:
    """Metadata required to load one table."""

    object_id: ObjectId
    primary_key: tuple[str, ...] = ()
    #: The declared unique keys, in declaration order, each a column tuple. Order
    #: matters: a row rejected by an earlier key does not choose the survivor of a
    #: later one, so the sequence is part of what the load means.
    unique_keys: tuple[tuple[str, ...], ...] = ()
    #: Columns a row must supply a value for. The primary key is stronger than
    #: this and validated separately, so it is not repeated here.
    not_null_columns: tuple[str, ...] = ()
    comparison_columns: tuple[str, ...] = ()
    identity_column: str | None = None
    incremental: bool = False
    #: Load the target only when it is empty.
    static: bool = False
    delete_threshold: int = DEFAULT_DELETE_THRESHOLD
    update_threshold: int = DEFAULT_UPDATE_THRESHOLD
    stability_rows: int = DEFAULT_STABILITY_ROWS

    @property
    def qualified(self) -> str:
        return self.object_id.qualified

    @property
    def replaces_wholesale(self) -> bool:
        """Return whether the load has no matching key."""

        return not self.primary_key

    @property
    def deletes_absent_rows(self) -> bool:
        """Return whether a keyed full load deletes rows absent from the source."""

        return bool(self.primary_key) and not self.incremental

    @property
    def checks_merge_uniqueness(self) -> bool:
        """Whether a load must ask what its proposed changes would leave behind.

        Only an incremental keyed load with unique keys. A non-incremental load
        reconciles the whole target against clean staging, so the target it
        leaves *is* staging and staging has already been made unique; an
        incremental load changes part of a target it cannot see the rest of.
        """

        return bool(self.primary_key) and self.incremental and bool(self.unique_keys)

    def may_breach(self, *, deleting: int, updating: int) -> bool:
        """Whether these settled counts could breach a threshold at all.

        Reading the target's size costs an action on Delta and a scan on the
        Warehouse, and :meth:`breaches` needs it. It cannot report a breach for a
        target smaller than ``stability_rows``, and the larger the target the
        larger a count has to be to reach a given percentage of it, so a count
        that would not breach a target of exactly ``stability_rows`` cannot
        breach any target the gate applies to. That is answerable from the
        counts alone, and where it says no the size is never asked for.

        The comparison is the one :meth:`breaches` makes, moved across the
        division and held at the smallest target it can act on. It decides only
        whether to ask; what a breach *is* stays there.
        """

        if self.replaces_wholesale:
            return False
        return (
            deleting * 100 > self.delete_threshold * self.stability_rows
            or updating * 100 > self.update_threshold * self.stability_rows
        )

    def breaches(self, *, target_rows: int, deleting: int, updating: int) -> str | None:
        """Return a stability-threshold breach, or ``None``."""

        # Stability thresholds do not apply to empty, small, or replacement loads.
        if (
            self.replaces_wholesale
            or target_rows == 0
            or target_rows < self.stability_rows
        ):
            return None
        for count, limit, what in (
            (deleting, self.delete_threshold, "delete"),
            (updating, self.update_threshold, "update"),
        ):
            percentage = count * 100 / target_rows
            if percentage > limit:
                return (
                    f"{what} of {count} rows is {percentage:.1f}% of {target_rows}, "
                    f"over the {limit}% threshold"
                )
        return None

    @classmethod
    def from_document(cls, document: SesDocument) -> "LoadContract":
        """Create a table load contract from a parsed document."""

        if document.kind != TABLE:
            raise LoadError(
                f"{document.qualified}: a {document.kind} has no table load contract"
            )
        return cls(
            object_id=document.object_id,
            primary_key=document.primary_key,
            unique_keys=document.unique_keys,
            not_null_columns=document.declared_not_null,
            comparison_columns=document.comparison_columns,
            identity_column=document.identity,
            incremental=document.is_incremental,
            static=document.static,
            delete_threshold=document.delete_threshold,
            update_threshold=document.update_threshold,
            stability_rows=document.stability_rows,
        )


@dataclass(frozen=True)
class FolderLoadContract:
    """Metadata required to load one managed folder."""

    object_id: ObjectId
    file_keys: tuple[str, ...] = ()
    incremental: bool = False
    #: Load the folder only when it is absent.
    static: bool = False

    @property
    def qualified(self) -> str:
        return self.object_id.qualified

    @property
    def replaces_wholesale(self) -> bool:
        """Return whether the folder load replaces its contents."""

        return not self.incremental

    @classmethod
    def from_document(cls, document: SesDocument) -> "FolderLoadContract":
        if document.kind != FOLDER:
            raise LoadError(
                f"{document.qualified}: a {document.kind} has no folder load contract"
            )
        return cls(
            object_id=document.object_id,
            file_keys=document.file_keys,
            incremental=document.is_incremental,
            static=document.static,
        )


def module_metadata_text(module) -> str:
    """Return an installed module's dedented metadata docstring."""

    doc = getattr(module, "__doc__", None)
    name = getattr(module, "__name__", "<module>")
    if doc is None or not doc.strip():
        raise LoadError(
            f"{name} carries no Weaver metadata: a deployed object module must "
            "begin with its docstring metadata block, which is the contract its "
            "load runs from"
        )
    return inspect.cleandoc(doc)


def document_for_module(module) -> SesDocument:
    """Parse an authored Python module's own docstring into a document."""

    return parse_document(module_metadata_text(module), language=PYTHON)


def normalise_read_result(returned):
    """Return an authored ``read()`` value as ``(staging, deletes)``.

    A single value is the ordinary case: it stages data and makes no explicit
    delete claim. A tuple remains the explicit form and must contain exactly
    those two values.
    """

    if not isinstance(returned, tuple):
        return returned, None
    if len(returned) != 2:
        raise LoadError(
            "read() returned a tuple with "
            f"{len(returned)} values; return data, or (data, deletes)"
        )
    return returned


#: Rejection reasons shared by Warehouse and Delta load results. A reject table
#: is read by people, so a Warehouse reject and a Delta reject for the same
#: problem say the same thing.
#:
#: All four describe a *recoverable* problem with one incoming row. A load that
#: would leave the target itself invalid is a different matter and fails outright
#: — see :attr:`LoadContract.checks_merge_uniqueness`.
REASON_BLANK_PK = "blank_primary_key"
REASON_DUPLICATE_PK = "duplicate_primary_key"
REASON_NULL_COLUMN = "null_column"
REASON_DUPLICATE_UNIQUE = "duplicate_unique_key"

#: The column a reject table carries the reason in.
REJECTION_REASON = "_reject_reason"

#: How wide that column is. Wide enough for the longest reason plus the columns
#: it names, so a composite key's reason is not truncated into ambiguity.
REJECTION_REASON_WIDTH = 1000


def null_column_reason(column: str) -> str:
    """Why one row was refused: it left a declared not-null column empty."""

    return f"{REASON_NULL_COLUMN}: {column}"


def duplicate_unique_reason(columns) -> str:
    """Why one row was refused: another incoming row already holds this key.

    Names the columns, because a table may declare several unique keys and
    "duplicate" alone would not say which one the row lost.
    """

    return f"{REASON_DUPLICATE_UNIQUE}: {', '.join(columns)}"


def delta_audit_columns() -> tuple[str, str, str]:
    """Return Delta audit column names."""

    return tuple(
        audit_column_name(logical, PYTHON)
        for logical in (AUDIT_INSERT, AUDIT_UPDATE, AUDIT_DELETE)
    )


__all__ = [
    "FolderLoadContract",
    "LoadContract",
    "delta_audit_columns",
    "document_for_module",
    "duplicate_unique_reason",
    "module_metadata_text",
    "normalise_read_result",
    "null_column_reason",
]
