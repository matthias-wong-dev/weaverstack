"""Shared fixtures for the whole suite.

The core tier runs pure Python: it renders, plans and reconciles against a
:class:`~weaver.session.testing.TestSession`, which records what a host would
have been asked to do. Nothing here starts a JVM, holds a Spark session or
reaches a workspace — the tests that need a real one carry the ``fabric``
marker and build their own in ``tests/fabric``.
"""

from __future__ import annotations

from pathlib import Path

import pytest

import sys as _sys
from pathlib import Path as _Path

# The narrow fixture constructors are shared by every layer — pure Python,
# local Spark and Fabric all build their inputs the same way — so they are
# importable from anywhere in the suite rather than copied per directory.
_sys.path.insert(0, str(_Path(__file__).parent / "targeted"))


from weaver.targets import ItemRef

WORKSPACE = "Demo"
WEAVER_LAKEHOUSE = "Weaver"
TARGET_LAKEHOUSE = "Sales_LH"
LAKEHOUSE_SQL = Path(__file__).parent / "fixtures" / "local-lakehouse"


def pytest_collection_modifyitems(items):
    """Make Fabric place and Weaver position a collection-time invariant."""

    errors = []
    fabric_root = Path(__file__).parent / "fabric"
    for item in items:
        path = Path(str(item.path))
        if path.parent != fabric_root or not path.name.startswith("test_"):
            continue

        marks = {mark.name for mark in item.iter_markers()}
        positions = marks & {"remote", "hosted"}
        if "fabric" not in marks:
            errors.append(f"{item.nodeid}: missing fabric marker")
        if len(positions) != 1:
            errors.append(
                f"{item.nodeid}: expected exactly one Weaver position "
                f"(remote or hosted), got {sorted(positions)}"
            )

    if errors:
        raise pytest.UsageError("invalid Fabric test markers:\n" + "\n".join(errors))


@pytest.fixture(autouse=True)
def no_credentials_outside_fabric(request, monkeypatch):
    """Nothing but a Fabric test may ask for a real credential.

    ``DefaultAzureCredential`` is a network call that, on a build agent with no
    identity, hangs and then fails — and the test it fails is whichever one
    happened to construct a Fabric-shaped Session, which says nothing about the
    cause. It is not enough to mock it in the tests that reach it today: a
    ``Resource`` binds its acquisition when the scope is *constructed*, so a
    patch applied to a scope afterwards leaves the original in place and the
    call happens anyway. That is exactly how this escaped once.

    So the default is refusal, and a Fabric test opts out by carrying the
    marker that says it needs a workspace.
    """

    if request.node.get_closest_marker("fabric"):
        return

    def refuse():
        raise AssertionError(
            "a test outside `-m fabric` asked for an Azure credential. Replace "
            "`weaver.fabric.auth.credential` before the Session is constructed "
            "— a Resource binds its acquisition at construction, so patching "
            "the scope afterwards is too late."
        )

    monkeypatch.setattr("weaver.fabric.auth.credential", refuse)


def _sql_statements(name: str, tables_root: str) -> tuple[str, ...]:
    """The saved Spark SQL fixture, rendered for one explicit Tables root."""

    raw = (LAKEHOUSE_SQL / name).read_text(encoding="utf-8").format(tables=tables_root)
    code = "\n".join(
        line for line in raw.splitlines() if not line.lstrip().startswith("--")
    )
    return tuple(
        statement
        for statement in (part.strip() for part in code.split(";"))
        if statement
    )


@pytest.fixture
def lakehouse_sql_statements():
    """Shared DDL/DML renderer for local Spark and Fabric Livy fixtures."""

    return _sql_statements


def _populate_folder_files(store, resolver, target: ItemRef) -> None:
    """The file side of the populated-Lakehouse fixture, transport-neutral."""

    from weaver.targets import FolderTarget

    folder_target = FolderTarget(lakehouse=target)
    export = resolver.folder_object(folder_target, "Sales", "OrderExport")
    for day in ("20260721", "20260722", "20260723"):
        store.write(export / f"order_{day}.csv", b"id,amount\n1,10\n2,20\n")

    invoices = resolver.folder_object(folder_target, "Sales", "InvoicePdf")
    store.write(invoices / "INV-001.pdf", b"%PDF-1.4 fake\n")
    store.write(invoices / "archive" / "INV-000.pdf", b"%PDF-1.4 older\n")
    store.write(resolver.files_root(target) / "notes.txt", b"scratch\n")


@pytest.fixture
def populate_folder_files():
    """Shared fixture setup through FilesystemStore or desktop OneLake access."""

    return _populate_folder_files
