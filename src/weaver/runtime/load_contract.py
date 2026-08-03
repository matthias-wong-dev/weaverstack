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
