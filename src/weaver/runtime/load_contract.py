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
                f"{document.qualified}: a {document.kind} has no table load "
                "contract"
            )
        return cls(
            object_id=document.object_id,
            primary_key=document.primary_key,
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
                f"{document.qualified}: a {document.kind} has no folder load "
                "contract"
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


#: Rejection reasons shared by Warehouse and Delta load results.
REASON_BLANK_PK = "blank_primary_key"
REASON_DUPLICATE_PK = "duplicate_primary_key"

#: The column a reject table carries the reason in.
REJECTION_REASON = "_reject_reason"


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
    "module_metadata_text",
]
