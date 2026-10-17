"""Mirror an older Weaver version's catalogue into this version's.

Production runs one version while development trials the next, so a mirror
reads a catalogue it did not build. Columns copy by name, tables the source
predates start empty, and a legacy Installation takes the Fabric identities
preflight verifies. The source is only read.
"""

from __future__ import annotations

from dataclasses import replace

import pytest
from support.catalogue_shapes import (
    INTERIM_INSTALLATION,
    current,
    released,
    rows,
    shape,
)
from support.weaver_test import weaver_test
from support.workspaces import _identifier, given_resolver

from weaver.catalogue.connection import CatalogueConnection
from weaver.catalogue.fork import (
    copy_statement,
    copy_statements,
    installation_copy_statement,
    project_source,
)
from weaver.catalogue.reader import table_query
from weaver.catalogue.tables import (
    INSTALLATION,
    REGISTRY,
    SEMANTIC_MODEL_TABLE,
    SEMANTIC_MODEL_TEST,
    SEMANTIC_TABLES,
)
from weaver.errors import CatalogueStateError, CommandError, InstallationIdentityError

SOURCE = "Warehouse/Weaver"
OTHER = "11111111-1111-1111-1111-111111111111"
SEMANTIC = {table.name for table in SEMANTIC_TABLES} | {SEMANTIC_MODEL_TEST.name}


def _fork(tables):
    return project_source(shape(tables), source=SOURCE)


def _with(tables, table, columns):
    return {**tables, table: tuple(columns)}


# --- mapping ------------------------------------------------------------------


@weaver_test()
@pytest.mark.parametrize("borrowed", [False, True])
def test_a_source_of_this_version_copies_exactly_as_before(borrowed):
    fork = _fork(current(borrowed=borrowed))

    assert fork.borrowed is borrowed
    assert not fork.legacy_installation
    assert copy_statements(source_catalogue="Weaver", source=fork) == (
        copy_statements(source_catalogue="Weaver", borrowed=borrowed)
    )


@weaver_test()
def test_tables_a_released_catalogue_predates_start_empty():
    fork = replace(_fork(released()), identities=())
    statements = copy_statements(source_catalogue="Weaver", source=fork)

    assert SEMANTIC.isdisjoint(fork.columns)
    for name in SEMANTIC:
        assert f"[{name}]" not in "\n".join(statements)
    # Installation has no rows to copy here.
    assert len(statements) == len(fork.columns) - 1


@weaver_test()
def test_a_nullable_column_the_source_predates_is_left_null():
    tables = _with(
        current(),
        SEMANTIC_MODEL_TABLE.name,
        (c for c in current()[SEMANTIC_MODEL_TABLE.name] if c != "Table ordinal"),
    )
    columns = _fork(tables).columns[SEMANTIC_MODEL_TABLE.name]

    statement = copy_statement(
        SEMANTIC_MODEL_TABLE, source_catalogue="Weaver", columns=columns
    )
    assert "[Table ordinal]" not in statement
    assert "[Table name]" in statement


@weaver_test()
def test_a_column_this_version_does_not_hold_is_left_behind():
    tables = _with(current(), REGISTRY.name, (*current()[REGISTRY.name], "Properties"))
    columns = _fork(tables).columns[REGISTRY.name]

    assert "[Properties]" not in copy_statement(
        REGISTRY, source_catalogue="Weaver", columns=columns
    )


@weaver_test()
def test_the_source_spelling_is_what_the_copy_reads():
    """A Warehouse identifier is case-sensitive."""

    tables = _with(
        current(), REGISTRY.name, (name.lower() for name in current()[REGISTRY.name])
    )
    statement = copy_statement(
        REGISTRY, source_catalogue="Weaver", columns=_fork(tables).columns["Registry"]
    )

    assert "insert into [_].[Registry] ([Item type]" in statement
    assert "select [item type], [item name]" in statement
    assert "where not ([item type] = N'Warehouse'" in statement


@weaver_test()
def test_a_source_lacking_what_a_copy_needs_is_refused():
    tables = {k: v for k, v in released().items() if k != "Shortcut"}
    tables = _with(
        tables, "Registry", (c for c in released()["Registry"] if c != "Signature")
    )

    with pytest.raises(CatalogueStateError, match="cannot be mirrored") as raised:
        _fork(tables)

    assert "Shortcut" in str(raised.value)
    assert "Registry.Signature" in str(raised.value)


# --- a legacy Installation ----------------------------------------------------


@weaver_test()
@pytest.mark.parametrize("installation", ["released", "interim"])
def test_an_installation_without_fabric_identities_is_legacy(installation):
    tables = released()
    if installation == "interim":
        tables = _with(tables, "Installation", INTERIM_INSTALLATION)
    fork = _fork(tables)

    assert fork.legacy_installation
    assert "Target item GUID" not in fork.columns[INSTALLATION.name]


@weaver_test()
def test_a_legacy_installation_copies_the_verified_identities():
    fork = _fork(released())
    identities = [
        {
            "item_type": kind,
            "item_name": name,
            "workspace_name": "Analytics",
            "workspace_id": "bc690e88-80e3-4e97-8184-f919b46b91cf",
            "target_name": name,
            "item_id": _identifier(kind, name),
        }
        for kind, name in (("Warehouse", "Model"), ("Lakehouse", "Input"))
    ]

    statement = installation_copy_statement(
        source_catalogue="Weaver",
        columns=fork.columns[INSTALLATION.name],
        identities=identities,
    )

    assert statement.startswith(
        "insert into [_].[Installation] ([Item type], [Item name], "
        "[Workspace name], [Workspace GUID], [Target name], [Target item GUID], "
        "[Weaver version], [Signature], [Row insert datetime]"
    )
    assert "select s.[Item type], s.[Item name], r.[Workspace name]" in statement
    assert "from [Weaver].[_].[Installation] as s" in statement
    for identity in identities:
        assert f"N'{identity['item_id']}'" in statement


# --- preflight ----------------------------------------------------------------


def _preflight(monkeypatch, tables, installations, *, warehouses, items=()):
    """``check_mirror`` over a source answering with ``tables`` and ``installations``."""

    import weaver.operations.mirror as module
    from weaver.catalogue.state import Catalogue
    from weaver.declaration.model import WeaverItemId
    from weaver.sessions import TestSession
    from weaver.workspaces import CatalogueRef

    resolver = given_resolver(warehouses=["Weaver", "Weaver_Dev", *warehouses])
    workspace = replace(
        resolver.configuration, catalogue="Warehouse/Weaver_Dev", mirror=SOURCE
    )
    plan = module.MirrorPlan(
        workspace=workspace,
        source=CatalogueRef(workspace=workspace.workspace, name="Weaver"),
        destination=CatalogueRef(workspace=workspace.workspace, name="Weaver_Dev"),
        items=tuple(items),
    )
    session = TestSession(workspace=workspace, resolver=resolver)
    information = rows(tables)
    session.answer_tsql(
        "SELECT TABLE_NAME, COLUMN_NAME, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS "
        "WHERE TABLE_SCHEMA = N'_'",
        information,
    )
    if INSTALLATION.name in tables:
        connection = CatalogueConnection(lambda _statement: information)
        session.answer_tsql(table_query(connection, INSTALLATION), installations)
    monkeypatch.setattr(
        "weaver.catalogue.state.catalogue_for",
        lambda *_a, **_k: Catalogue(
            {
                WeaverItemId(r["item_type"], r["item_name"]): {
                    INSTALLATION.name: (dict(r),)
                }
                for r in installations
            }
        ),
    )
    return plan, session, module


def _released_row(kind, name, target, **recorded):
    return {
        "item_type": kind,
        "item_name": name,
        "target_name": target,
        "workspace_id": None,
        "workspace_name": None,
        "item_id": None,
        "weaver_version": "0.9.0",
        "signature": "installation",
        **recorded,
    }


@weaver_test()
def test_preflight_verifies_every_installation_a_released_source_records(
    monkeypatch,
):
    installations = [
        _released_row("Warehouse", "_weaver", "Weaver"),
        _released_row("Warehouse", "Model", "Sales"),
        _released_row("Warehouse", "Feed", "Producer"),
    ]
    plan, session, module = _preflight(
        monkeypatch,
        released(),
        installations,
        warehouses=["Sales", "Producer", "Sales_Dev"],
        items=["Warehouse/Model=Warehouse/Sales_Dev"],
    )

    resolved = module.check_mirror(plan, session=session)

    assert [(r["item_name"], r["item_id"]) for r in resolved.fork.identities] == [
        ("Model", _identifier("Warehouse", "Sales")),
        ("Feed", _identifier("Warehouse", "Producer")),
    ]
    # The source is only read.
    assert all(
        statement.lstrip().lower().startswith("select")
        for call in session.calls
        for statement in call.body
    )


@weaver_test()
def test_no_item_from_a_released_source_still_resolves_its_installations(
    monkeypatch,
):
    plan, session, module = _preflight(
        monkeypatch,
        released(),
        [_released_row("Warehouse", "Model", "Sales")],
        warehouses=["Sales"],
    )

    resolved = module.check_mirror(plan, session=session)

    assert resolved.items == ()
    assert [r["target_name"] for r in resolved.fork.identities] == ["Sales"]


@weaver_test()
def test_a_released_installation_whose_target_is_gone_refuses_before_any_wipe(
    monkeypatch,
):
    import weaver.mirror_plan as compiled

    plan, session, module = _preflight(
        monkeypatch,
        released(),
        [_released_row("Warehouse", "Feed", "Retired")],
        warehouses=[],
    )
    monkeypatch.setattr(
        compiled,
        "mirror_mutation_plan",
        lambda *_a, **_k: pytest.fail("the destination was planned for emptying"),
    )

    with pytest.raises(InstallationIdentityError, match="Warehouse/Feed"):
        module.mirror(plan=plan, session=session)


@weaver_test()
@pytest.mark.parametrize(
    "recorded,refused",
    [("Sales", False), (_identifier("Warehouse", "Sales"), False), (OTHER, True)],
    ids=["target name", "matching GUID", "replaced"],
)
def test_an_interim_item_id_is_backfilled_only_when_it_holds_the_target_name(
    monkeypatch, recorded, refused
):
    plan, session, module = _preflight(
        monkeypatch,
        _with(released(), "Installation", INTERIM_INSTALLATION),
        [_released_row("Warehouse", "Model", "Sales", item_id=recorded)],
        warehouses=["Sales"],
    )

    if refused:
        with pytest.raises(InstallationIdentityError, match="Target item GUID"):
            module.check_mirror(plan, session=session)
        return
    resolved = module.check_mirror(plan, session=session)
    assert resolved.fork.identities[0]["item_id"] == _identifier("Warehouse", "Sales")


@weaver_test()
def test_a_source_holding_no_catalogue_is_named(monkeypatch):
    plan, session, module = _preflight(monkeypatch, {}, [], warehouses=[])

    with pytest.raises(CommandError, match="does not contain a Weaver catalogue"):
        module.check_mirror(plan, session=session)
