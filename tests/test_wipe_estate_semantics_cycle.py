"""Whole-estate wipe ordering, and unbind through a real resolution path."""

from __future__ import annotations

import pytest
from support.sessions import given_session
from support.weaver_test import weaver_test
from support.workspaces import given_workspace

from weaver import wipe as public_wipe
from weaver.catalogue.tables import INSTALLATION
from weaver.errors import CommandError


def installation_session(rows, *, catalogue="Warehouse/Control"):
    """A Session whose catalogue Warehouse holds these Installation rows.

    Answered at the TDS boundary: the shape read is answered directly, and the
    Installation read is primed by issuing it once, so the production statement
    is what gets its rows.
    """

    session = given_session(
        workspace=given_workspace(catalogue=catalogue),
        lakehouses=("Sales", "Sales_LH"),
        warehouses=("Control", "Reporting"),
        store=_Store(),
    )
    session.answer_tsql(
        "SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
        "WHERE TABLE_SCHEMA = N'_'",
        [
            {
                "TABLE_NAME": INSTALLATION.name,
                "COLUMN_NAME": INSTALLATION.public_name_of(name),
            }
            for name in INSTALLATION.column_names
        ],
    )
    _prime_installation_read(session)
    _answer_installation(session, rows)
    return session


def _prime_installation_read(session):
    """Issue the Installation read once so its statement is recorded."""

    from weaver.catalogue.connection import catalogue_connection
    from weaver.catalogue.reader import read_table
    from weaver.catalogue.tables import INSTALLATION

    read_table(catalogue_connection(session, session.workspace), INSTALLATION)


def _answer_installation(session, rows):
    """Answer the recorded Installation statement with these rows."""

    statements = [
        statement
        for statement in session.tsql[1:]
        if "FROM [_].[Installation]" in statement
    ]
    session.answer_tsql(statements[-1], list(rows))


@weaver_test()
def test_an_untargeted_wipe_empties_the_catalogue_last(monkeypatch):
    """The catalogue is the index a half-finished wipe is retried from."""

    session = installation_session(
        [
            {
                "item_type": "Lakehouse",
                "item_name": "Sales",
                "target_name": "Sales",
                "weaver_version": "0.0.0",
                "signature": "installation",
            },
            {
                "item_type": "Warehouse",
                "item_name": "Reporting",
                "target_name": "Reporting",
                "weaver_version": "0.0.0",
                "signature": "installation",
            },
        ]
    )
    removed = []
    operations = __import__("weaver.operations.wipe", fromlist=["wipe"])
    monkeypatch.setattr(
        operations,
        "_wipe_one",
        lambda target, *a, **k: removed.append(str(target)) or (),
    )

    public_wipe(session=session)

    assert removed == ["Lakehouse/Sales", "Warehouse/Reporting", "Warehouse/Control"]


@weaver_test()
def test_a_named_catalogue_wipe_still_goes_last(monkeypatch):
    """Ordering is a property of the run, not of how the scope was named."""

    session = installation_session([])
    removed = []
    operations = __import__("weaver.operations.wipe", fromlist=["wipe"])
    monkeypatch.setattr(
        operations,
        "_wipe_one",
        lambda target, *a, **k: removed.append(str(target)) or (),
    )

    public_wipe(
        ("Lakehouse/Sales", "Warehouse/Control"),
        session=session,
    )

    assert removed == ["Lakehouse/Sales", "Warehouse/Control"]


@weaver_test()
def test_unbind_through_the_resolution_path_unbinds_and_preserves(monkeypatch):
    """``--workspace-config`` resolves the catalogue from configuration.

    No ``_resolve_workspace`` monkeypatch: the command goes through
    ``resolve_workspace`` and the catalogue comes back on the resolved
    Workspace, which is what decides that claims are removed.
    """

    import tempfile
    from pathlib import Path

    session = given_session(store=_Store(), lakehouses=("Sales",), warehouses=())
    unbound = []
    operations = __import__("weaver.operations.wipe", fromlist=["wipe"])
    monkeypatch.setattr(
        operations,
        "_wipe_one",
        lambda target, *a, **k: (),
    )
    monkeypatch.setattr(
        operations,
        "_unbind_physical_targets",
        lambda workspace, targets, **_k: unbound.append(tuple(map(str, targets)))
        or {"targets": [], "logical_items": ["Lakehouse/Sales"]},
    )

    directory = Path(tempfile.mkdtemp())
    configuration = directory / "workspace-config.yml"
    configuration.write_text(
        "workspace: Demo\ncatalogue: Warehouse/Control\ntargets:\n"
        "  Lakehouse/Sales: Sales\n"
    )

    result = public_wipe(
        "Lakehouse/Sales",
        unbind=True,
        workspace_config=configuration,
        session=session,
    )

    assert unbound == [("Lakehouse/Sales",)]
    assert result.catalogue_role == "preserved; claims unbound"


@weaver_test()
def test_unbind_with_an_explicit_catalogue_resolves_it_onto_the_session(monkeypatch):
    session = given_session(store=_Store(), lakehouses=("Sales",), warehouses=())
    unbound = []
    operations = __import__("weaver.operations.wipe", fromlist=["wipe"])
    monkeypatch.setattr(
        operations,
        "_wipe_one",
        lambda target, *a, **k: (),
    )
    monkeypatch.setattr(
        operations,
        "_unbind_physical_targets",
        lambda workspace, targets, **_k: unbound.append(workspace.catalogue)
        or {"targets": [], "logical_items": []},
    )

    result = public_wipe(
        "Lakehouse/Sales",
        unbind=True,
        workspace="Foo",
        catalogue="Warehouse/Catalogue",
        session=session,
    )

    assert unbound == ["Warehouse/Catalogue"]
    assert result.catalogue_role == "preserved; claims unbound"


@weaver_test()
def test_unbind_naming_the_catalogue_itself_is_refused():
    """``--unbind`` preserves the catalogue, so wiping it is a contradiction."""

    with pytest.raises(CommandError, match="preserves the catalogue"):
        public_wipe(
            ("Lakehouse/Sales", "Warehouse/Control"),
            unbind=True,
            workspace="Foo",
            catalogue="Warehouse/Control",
            session=given_session(store=_Store(), lakehouses=("Sales",), warehouses=()),
        )


@weaver_test()
def test_unbind_naming_only_other_targets_leaves_the_catalogue_standing(monkeypatch):
    session = given_session(store=_Store(), lakehouses=("Sales",), warehouses=("Reporting",))
    removed = []
    unbound = []
    operations = __import__("weaver.operations.wipe", fromlist=["wipe"])
    monkeypatch.setattr(
        operations,
        "_wipe_one",
        lambda target, *a, **k: removed.append(str(target)) or (),
    )
    monkeypatch.setattr(
        operations,
        "_unbind_physical_targets",
        lambda workspace, targets, **_k: unbound.append(tuple(map(str, targets)))
        or {"targets": [], "logical_items": []},
    )

    result = public_wipe(
        ("Lakehouse/Sales", "Warehouse/Reporting"),
        unbind=True,
        workspace="Foo",
        catalogue="Warehouse/Control",
        session=session,
    )

    assert removed == ["Lakehouse/Sales", "Warehouse/Reporting"]
    assert unbound == [("Lakehouse/Sales", "Warehouse/Reporting")]
    assert result.catalogue_role == "preserved; claims unbound"


class _Store:
    """A store that holds nothing and records nothing: the wipes are stubbed."""

    def exists(self, location):
        return False

    def list(self, location, recursive=False):
        return ()

    def read(self, location):
        return b""
