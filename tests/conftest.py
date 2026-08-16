"""Shared fixtures for the whole suite.

The core tier runs pure Python: it renders, plans and reconciles against a
:class:`~weaver.sessions.testing.TestSession`, which records what a host would
have been asked to do. Nothing here starts a JVM, holds a Spark session or
reaches a workspace — the tests that need a real one carry the ``fabric``
marker and build their own in ``tests/fabric``.
"""

from __future__ import annotations

import sys as _sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from pathlib import Path as _Path

import pytest
from support.weaver_test import (
    begin_test,
    end_test,
    event_snapshot,
    observed_resources,
    register_session,
    registered_sessions,
)

# The narrow fixture constructors are shared by every layer — the core suite
# and the Fabric one build their inputs the same way — so they are importable
# from anywhere in the suite rather than copied per directory.
_sys.path.insert(0, str(_Path(__file__).parent / "targeted"))


from weaver.targets import ItemRef

WORKSPACE = "Demo"
WEAVER_WAREHOUSE = "Weaver"
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


def pytest_runtest_setup(item):
    """Make fixture-provided Sessions attributable before fixtures run."""

    item._weaver_test_context = begin_test()


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_call(item):
    """Compare a test declaration with external Session telemetry it caused."""

    declaration = getattr(item.obj, "__weaver_test_declaration__", None)
    before = event_snapshot()
    yield
    events = [
        event
        for session in registered_sessions()
        for event in session.telemetry.events()[before.get(id(session), 0) :]
    ]
    item._weaver_telemetry_events = tuple(events)
    if declaration is None:
        return
    observed = observed_resources(before)
    if observed != declaration.resources:
        unexpected = sorted(observed - declaration.resources)
        unused = sorted(declaration.resources - observed)
        parts = [
            "Weaver test resource declaration did not match Session telemetry:",
            f"declared: {sorted(declaration.resources)}",
            f"observed: {sorted(observed)}",
        ]
        if unexpected:
            parts.append(f"unexpected resource: {', '.join(unexpected)}")
        if unused:
            parts.append(f"declared but unused: {', '.join(unused)}")
        raise AssertionError("\n".join(parts))


def pytest_runtest_teardown(item):
    """Release the ContextVar registry after fixture teardown has finished."""

    end_test(item._weaver_test_context)


@pytest.fixture(autouse=True)
def register_created_sessions(monkeypatch):
    """Register production Sessions created by a test without changing core."""

    from weaver.sessions.base import Session

    original = Session.__init__

    def tracked(self, *args, **kwargs):
        original(self, *args, **kwargs)
        register_session(self)

    monkeypatch.setattr(Session, "__init__", tracked)


def pytest_terminal_summary(terminalreporter, exitstatus, config):
    """Report declared topology and the external cost observed by Sessions."""

    items = getattr(terminalreporter._session, "items", ())
    declarations = [
        declaration
        for item in items
        if (declaration := getattr(item.obj, "__weaver_test_declaration__", None))
        is not None
    ]
    if declarations:
        terminalreporter.write_sep("=", "Weaver tests")
        for scope, count in sorted(Counter(one.scope for one in declarations).items()):
            terminalreporter.write_line(f"{scope:<12} {count}")
        terminalreporter.write_line("")
        terminalreporter.write_line("Declared resources")
        for resource, count in sorted(
            Counter(resource for one in declarations for resource in one.resources).items()
        ):
            terminalreporter.write_line(f"  {resource:<10} {count} tests")

    by_resource = defaultdict(lambda: [0, 0.0])
    by_test = defaultdict(lambda: [0, 0.0, defaultdict(float)])
    for item in items:
        for event in getattr(item, "_weaver_telemetry_events", ()):
            by_resource[event.resource][0] += 1
            by_resource[event.resource][1] += event.seconds
            by_test[item.nodeid][0] += 1
            by_test[item.nodeid][1] += event.seconds
            by_test[item.nodeid][2][event.resource] += event.seconds
    if not by_resource:
        return
    terminalreporter.write_sep("=", "External resource telemetry")
    terminalreporter.write_line("By resource")
    for resource, (calls, seconds) in sorted(
        by_resource.items(), key=lambda item: item[1][1], reverse=True
    ):
        terminalreporter.write_line(f"  {resource:<10} {calls:>4} operations {seconds:>8.1f}s")
    terminalreporter.write_line("")
    terminalreporter.write_line("Top tests by external time")
    for nodeid, (_, seconds, resources) in sorted(
        by_test.items(), key=lambda item: item[1][1], reverse=True
    )[:12]:
        detail = " ".join(
            f"{resource}={cost:.1f}s" for resource, cost in sorted(resources.items())
        )
        terminalreporter.write_line(f"  {nodeid}: {seconds:.1f}s  {detail}")


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
    """Shared DDL/DML renderer for the fixtures that seed a Lakehouse."""

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


# --- one workspace, resolved onto a temporary filesystem ---------------------


@dataclass(frozen=True)
class Lakehouses:
    """A catalogue Warehouse and a destination Lakehouse in one workspace.

    Resolution is the production :class:`~weaver.fabric.resolution.FabricResolver`
    over an inventory declared here; ``root`` puts what it resolves on this
    test's own filesystem, so a store operation is real. Nothing emulates
    Fabric: the arithmetic is Fabric's, and only the base URL differs.
    """

    workspace: object
    resolver: object
    store: object
    weaver: ItemRef
    target: ItemRef
    root: Path
    #: A Session over the two, because that is how a desktop caller hands an
    #: operation the resolver and store it reaches a workspace with.
    session: object


def _lakehouses(root: Path, *, weaver: str, target: str, extra=()) -> Lakehouses:
    from support.workspaces import given_resolver, given_workspace

    from weaver.store import FilesystemStore

    workspace = given_workspace(catalogue=f"Warehouse/{weaver}")
    resolver = given_resolver(
        workspace=workspace, lakehouses=(weaver, target, *extra), root=root
    )
    store = FilesystemStore()
    weaver_ref, target_ref = ItemRef(weaver), ItemRef(target)
    for item in (weaver_ref, target_ref, *(ItemRef(name) for name in extra)):
        store.make_directory(resolver.files_root(item))
        store.make_directory(resolver.tables_root(item))
    from support.sessions import given_session

    return Lakehouses(
        workspace=workspace,
        resolver=resolver,
        store=store,
        weaver=weaver_ref,
        target=target_ref,
        root=root,
        session=given_session(workspace=workspace, resolver=resolver, store=store),
    )


@pytest.fixture
def lakehouses(tmp_path: Path) -> Lakehouses:
    """One workspace per test, because a test's own tree costs almost nothing."""

    return _lakehouses(tmp_path, weaver=WEAVER_WAREHOUSE, target=TARGET_LAKEHOUSE)


@pytest.fixture
def populated_folders(lakehouses) -> Lakehouses:
    """One Lakehouse holding folder objects, so a wipe has something to take."""

    _populate_folder_files(lakehouses.store, lakehouses.resolver, lakehouses.target)
    return lakehouses


@pytest.fixture
def more_lakehouses(tmp_path: Path):
    """The same workspace, holding whichever further Lakehouses a test names.

    A workspace answers for the items it holds and not for others, so a test
    reaching a second Lakehouse says which one rather than relying on the
    inventory to invent it.
    """

    def build(*names: str) -> Lakehouses:
        return _lakehouses(
            tmp_path, weaver=WEAVER_WAREHOUSE, target=TARGET_LAKEHOUSE, extra=names
        )

    return build


@pytest.fixture
def desktop_credential(monkeypatch):
    """A credential a desktop command can acquire without a tenant.

    The sanctioned way past :func:`no_credentials_outside_fabric`: the CLI does
    prefer a real credential, and a test about what it *parses* should not need
    one. Replaced before any Session is constructed, because a Resource binds
    its acquisition then.
    """

    class _Token:
        token = "test-token"
        expires_on = 4102444800  # well beyond any test run

    class _Credential:
        def get_token(self, *scopes, **_):
            return _Token()

    monkeypatch.setattr("weaver.fabric.auth.credential", lambda *a, **k: _Credential())
    return _Credential()
