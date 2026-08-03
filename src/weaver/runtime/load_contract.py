"""What one object's load needs to know, and where a running module reads it.

A load contract is the *whole* input to loading one object: the key it matches
on, the columns whose change means an update, whether absent rows are deleted.
It is deliberately small. Everything a repository knows and a load does not need
— dependencies, lineage, revision notes, aliases, the other documents in the
item — is absent, so a primitive cannot come to rely on state only an
orchestrator could supply.

Two ways in, one model out:

.. code-block:: text

    a parsed SesDocument      -> LoadContract   (generation, on the desktop)
    an installed module's
    docstring                 -> LoadContract   (runtime, inside the session)

The second is why this module exists. A deployed ``Sales__Customer.py`` is a
complete executable artefact: it carries its own contract in its docstring, and
``Sales__Customer(spark).load()`` reads it from there. No repository is opened,
no catalogue is queried and no build bundle is consulted — which is what lets a
developer edit a module in a notebook and see the change on the next reload,
and what stops the load primitives quietly acquiring an orchestrator.

**The runtime parser is not the repository parser.** It reuses the same metadata
model, because two spellings of one contract would be a defect waiting to
happen, but it validates only what loading one object requires. Filename and
class agreement, dependency resolution and repository-wide constraints are the
repository's business and were settled before the module was ever installed.
Importing a module edited by hand after deployment is therefore at the
operator's risk, exactly as running an altered stored procedure is.
"""

from __future__ import annotations

import inspect
from dataclasses import dataclass

from ..declaration.metadata import (
    DEFAULT_DELETE_THRESHOLD,
    DEFAULT_STABILITY_ROWS,
    DEFAULT_UPDATE_THRESHOLD,
    AUDIT_DELETE,
    AUDIT_INSERT,
    AUDIT_UPDATE,
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
    """Everything needed to execute one table's load, and nothing else.

    ``primary_key`` empty means full replacement: with no way to match a source
    row to a target row there is no such thing as an update, so the target's
    contents are replaced by the source's. Every other field is meaningful only
    when there is a key, which is why the parser refuses ``Incremental`` and
    ``Comparison columns`` without one rather than letting them sit unused.
    """

    object_id: ObjectId
    primary_key: tuple[str, ...] = ()
    comparison_columns: tuple[str, ...] = ()
    identity_column: str | None = None
    incremental: bool = False
    delete_threshold: int = DEFAULT_DELETE_THRESHOLD
    update_threshold: int = DEFAULT_UPDATE_THRESHOLD
    stability_rows: int = DEFAULT_STABILITY_ROWS

    @property
    def qualified(self) -> str:
        return self.object_id.qualified

    @property
    def replaces_wholesale(self) -> bool:
        """No key, so the load replaces the target's contents entirely."""

        return not self.primary_key

    @property
    def deletes_absent_rows(self) -> bool:
        """Whether a target row the source stopped producing is removed.

        Only a keyed, non-incremental load deletes. An incremental load is a
        statement that the source shows a *window* rather than the whole truth,
        so absence from it says nothing about whether a row should still exist.
        """

        return bool(self.primary_key) and not self.incremental

    def breaches(self, *, target_rows: int, deleting: int, updating: int) -> str | None:
        """Why this load looks wrong, or ``None`` if it does not.

        The guard against a load that is *technically* correct and obviously
        wrong: a source that broke overnight and returned a tenth of its rows
        produces a change Weaver would otherwise carry out faithfully.

        Both percentages are of the target as it stands *before* the load, which
        is the number an operator means by "5% of the table". Neither applies
        below the row threshold, because on a small table one row is a large
        percentage and tripping on that would teach everyone to disable the
        guard.

        An unkeyed load is exempt: with no key there is nothing to match, so
        replacing every row is what the declaration asked for rather than a
        symptom of anything.
        """

        # An empty target has no proportion to be a percentage of, and a first
        # load into one is the case the guard must never stand in the way of.
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
        """The contract a parsed Weaver document describes.

        One derivation, used by generation and by the runtime alike, so the
        procedure Weaver generates for a Warehouse table and the Python load of
        a Delta table cannot come to disagree about what the same header meant.
        """

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
            delete_threshold=document.delete_threshold,
            update_threshold=document.update_threshold,
            stability_rows=document.stability_rows,
        )


@dataclass(frozen=True)
class FolderLoadContract:
    """What a folder load needs: what it manages, and whether it accumulates.

    A folder has no rows, so none of the row machinery applies. What it has is a
    file key naming the scope of what Weaver manages inside it — which is what
    makes replacement safe, because it says which files a replacement is
    entitled to remove.
    """

    object_id: ObjectId
    file_keys: tuple[str, ...] = ()
    incremental: bool = False

    @property
    def qualified(self) -> str:
        return self.object_id.qualified

    @property
    def replaces_wholesale(self) -> bool:
        """A non-incremental folder is replaced; an incremental one accumulates."""

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
        )


def document_for_module(module) -> SesDocument:
    """Parse an installed Python module's own docstring into a document.

    The docstring *is* the metadata block — the repository reader extracts the
    same text with :func:`ast.get_docstring`, and ``cleandoc`` reproduces the
    dedenting it does, so the runtime and the repository read one document from
    one source of truth.
    """

    doc = getattr(module, "__doc__", None)
    name = getattr(module, "__name__", "<module>")
    if doc is None or not doc.strip():
        raise LoadError(
            f"{name} carries no Weaver metadata: a deployed object module must "
            "begin with its docstring metadata block, which is the contract its "
            "load runs from"
        )
    return parse_document(inspect.cleandoc(doc), language=PYTHON)


#: Why a row was refused. One spelling for all four primitives, so a reject
#: table written by a Warehouse procedure and one written by a Python load can
#: be read by the same query. Taken from the reference implementation rather
#: than reinvented — these strings are already in use against real data.
REASON_BLANK_PK = "blank_primary_key"
REASON_DUPLICATE_PK = "duplicate_primary_key"

#: The column a reject table carries the reason in.
REJECTION_REASON = "_reject_reason"


def delta_audit_columns() -> tuple[str, str, str]:
    """The insert, update and delete audit column names, spelled for Delta."""

    return tuple(
        audit_column_name(logical, PYTHON)
        for logical in (AUDIT_INSERT, AUDIT_UPDATE, AUDIT_DELETE)
    )


__all__ = [
    "FolderLoadContract",
    "LoadContract",
    "delta_audit_columns",
    "document_for_module",
]
