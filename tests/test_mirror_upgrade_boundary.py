"""Mirror an older Weaver version's catalogue into this version's.

Production runs one version while development trials the next, so a mirror
reads a catalogue it did not build. Preflight maps every source table and column
onto this version's, verifies a legacy Installation's Fabric identities, and
refuses whatever cannot move, all before anything is emptied. The source is only
read.
"""

from __future__ import annotations

from dataclasses import replace

import pytest
from support.catalogue_shapes import (
    INTERIM_INSTALLATION,
    current,
    released,
    rows,
)
from support.weaver_test import weaver_test
from support.workspaces import _identifier, given_resolver

from weaver.catalogue.connection import CatalogueConnection
from weaver.catalogue.fork import (
    FORKED_TABLES,
    SOURCE_SHAPE_QUERY,
    copy_statement,
    copy_statements,
    fork_statements,
    installation_copy_statement,
    null_count_statement,
    project_source,
    source_shape,
)
from weaver.catalogue.reader import table_query
from weaver.catalogue.tables import (
    INSTALLATION,
    REGISTRY,
    SEMANTIC_MODEL_TABLE,
    SEMANTIC_MODEL_TEST,
    SEMANTIC_TABLES,
    TABLE_DICTIONARY,
)
from weaver.errors import CatalogueStateError, CommandError, InstallationIdentityError

SOURCE = "Warehouse/Weaver"
OTHER = "11111111-1111-1111-1111-111111111111"
SEMANTIC = {table.name for table in SEMANTIC_TABLES} | {SEMANTIC_MODEL_TEST.name}


def _fork(tables):
    return project_source(source_shape(rows(tables)), source=SOURCE)


def _with(tables, table, columns):
    return {**tables, table: tuple(columns)}


def _without(tables, table, *names):
    return _with(tables, table, (c for c in tables[table] if c[0] not in names))


# --- the same version ---------------------------------------------------------


@weaver_test()
@pytest.mark.parametrize("borrowed", [False, True])
def test_a_source_of_this_version_copies_exactly_as_before(borrowed):
    fork = _fork(current(borrowed=borrowed))

    assert fork.is_current
    assert fork.borrowed is borrowed
    assert not fork.introduced and not fork.nullable and not fork.legacy_installation
    assert fork_statements(source_catalogue="Weaver", source=fork) == (
        fork_statements(source_catalogue="Weaver", borrowed=borrowed)
    )


# --- the previous release -----------------------------------------------------


@weaver_test()
def test_a_released_catalogue_maps_onto_this_version():
    fork = _fork(released())

    assert not fork.is_current
    assert set(fork.introduced) == SEMANTIC
    assert fork.legacy_installation
    assert fork.installation_columns == (
        "Item type",
        "Item name",
        "Target name",
        "Weaver version",
        "Signature",
        "Row insert datetime",
        "Row update datetime",
        "Row delete datetime",
    )


@weaver_test()
def test_tables_the_source_predates_start_empty():
    """No statement reads a table the source never had."""

    fork = replace(_fork(released()), identities=())
    body = "\n".join(copy_statements(source_catalogue="Weaver", source=fork))

    for name in SEMANTIC:
        assert f"[{name}]" not in body
    assert len(copy_statements(source_catalogue="Weaver", source=fork)) == len(
        FORKED_TABLES
    ) - len(SEMANTIC)


@weaver_test()
def test_a_column_the_source_holds_narrower_copies_into_the_wider_one():
    fork = _fork(released())

    assert fork.columns[TABLE_DICTIONARY.name]["Comparison columns"] == (
        "Comparison columns"
    )


@weaver_test()
def test_a_nullable_column_the_source_predates_is_left_null():
    tables = _without(current(), SEMANTIC_MODEL_TABLE.name, "Table ordinal")
    fork = _fork(tables)

    statement = copy_statement(
        SEMANTIC_MODEL_TABLE,
        source_catalogue="Weaver",
        columns=fork.columns[SEMANTIC_MODEL_TABLE.name],
    )
    assert "[Table ordinal]" not in statement
    assert "[Table name]" in statement
    assert not fork.is_current


@weaver_test()
def test_retired_columns_are_left_behind():
    tables = _with(
        current(),
        SEMANTIC_MODEL_TABLE.name,
        (
            *current()[SEMANTIC_MODEL_TABLE.name],
            ("Schema name", "varchar(128)", True),
            ("Properties", "varchar(max)", False),
        ),
    )
    fork = _fork(tables)

    statement = copy_statement(
        SEMANTIC_MODEL_TABLE,
        source_catalogue="Weaver",
        columns=fork.columns[SEMANTIC_MODEL_TABLE.name],
    )
    assert "[Properties]" not in statement and "[Schema name]" not in statement
    assert not fork.is_current


@weaver_test()
def test_a_source_spelling_is_used_for_the_columns_it_reads():
    """A Warehouse identifier is case-sensitive, so the source's spelling is read."""

    tables = _with(
        current(),
        REGISTRY.name,
        (
            (name.lower(), spelled, not_null)
            for name, spelled, not_null in current()[REGISTRY.name]
        ),
    )
    statement = copy_statement(
        REGISTRY, source_catalogue="Weaver", columns=_fork(tables).columns["Registry"]
    )

    assert "insert into [_].[Registry] ([Item type]" in statement
    assert "select [item type], [item name]" in statement
    assert "where not ([item type] = N'Warehouse'" in statement


# --- what cannot move ---------------------------------------------------------


@weaver_test()
@pytest.mark.parametrize(
    "tables,problem",
    [
        (
            lambda: {k: v for k, v in released().items() if k != "Registry"},
            "it has no Registry table",
        ),
        (
            lambda: _without(released(), "Registry", "Object role"),
            "Registry.Object role is missing",
        ),
        (
            lambda: _with(
                released(),
                "Registry",
                (*released()["Registry"], ("Owner", "varchar(128)", False)),
            ),
            "Registry.Owner is not a column this Weaver version knows",
        ),
        (
            lambda: _with(
                released(),
                "Registry",
                (
                    ("Object type", "varchar(4000)", True)
                    if c[0] == "Object type"
                    else c
                    for c in released()["Registry"]
                ),
            ),
            "Registry.Object type is varchar(4000), which does not fit varchar(128)",
        ),
        (
            lambda: _with(
                released(),
                "Bookmark",
                (
                    ("Bookmark datetime", "varchar(128)", True)
                    if c[0] == "Bookmark datetime"
                    else c
                    for c in released()["Bookmark"]
                ),
            ),
            "Bookmark.Bookmark datetime is varchar(128), which does not fit",
        ),
        (
            lambda: _with(
                released(),
                "Installation",
                (
                    *INTERIM_INSTALLATION,
                    ("Target item GUID", "varchar(128)", False),
                ),
            ),
            "Installation holds both Item ID and Target item GUID",
        ),
    ],
    ids=["table", "column", "unknown", "narrower", "type", "identity"],
)
def test_a_source_this_version_cannot_hold_is_refused(tables, problem):
    with pytest.raises(CatalogueStateError, match="cannot be mirrored") as raised:
        _fork(tables())

    assert problem in str(raised.value)
    assert SOURCE in str(raised.value)


@weaver_test()
def test_every_problem_is_named_at_once():
    tables = _without(
        {k: v for k, v in released().items() if k != "Shortcut"},
        "Registry",
        "Signature",
    )

    with pytest.raises(CatalogueStateError) as raised:
        _fork(tables)

    assert "it has no Shortcut table" in str(raised.value)
    assert "Registry.Signature is missing" in str(raised.value)


@weaver_test()
def test_a_source_allowing_null_where_a_value_is_required_is_counted():
    tables = _with(
        released(),
        "Registry",
        (
            ("Signature", "varchar(128)", False) if c[0] == "Signature" else c
            for c in released()["Registry"]
        ),
    )
    fork = _fork(tables)

    assert fork.nullable == (("Registry", "Signature"),)
    statement = null_count_statement(fork.nullable)
    assert "from [_].[Registry] where [Signature] is null" in statement
    assert "and not ([Item type] = N'Warehouse'" in statement


# --- a legacy Installation ----------------------------------------------------


def _identities(*items):
    return tuple(
        {
            "item_type": kind,
            "item_name": name,
            "workspace_name": "Analytics",
            "workspace_id": "bc690e88-80e3-4e97-8184-f919b46b91cf",
            "target_name": target,
            "item_id": _identifier(kind, target),
        }
        for kind, name, target in items
    )


@weaver_test()
def test_a_legacy_installation_copies_the_verified_identities():
    fork = _fork(released())
    identities = _identities(
        ("Warehouse", "Model", "Model"), ("Lakehouse", "Input", "Input")
    )

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
    assert "s.[Weaver version], s.[Signature]" in statement
    assert "from [Weaver].[_].[Installation] as s" in statement
    for identity in identities:
        assert f"N'{identity['item_id']}'" in statement
    assert "on r.[Item type] = s.[Item type] and r.[Item name] = s.[Item name]" in (
        statement
    )
    assert "[Item ID]" not in statement
    # A row added at the source after preflight would arrive without one.
    assert ") <> 2\n" in statement and "throw 50000" in statement


@weaver_test()
def test_a_legacy_installation_is_never_copied_unverified():
    fork = _fork(released())

    with pytest.raises(CatalogueStateError, match="verified identities"):
        copy_statements(source_catalogue="Weaver", source=fork)


@weaver_test()
def test_an_interim_installation_is_a_legacy_one():
    fork = _fork(_with(released(), "Installation", INTERIM_INSTALLATION))

    assert fork.legacy_installation
    assert "Item ID" in fork.installation_columns
    assert "Target item GUID" not in fork.columns[INSTALLATION.name]


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
    shape = rows(tables)
    session.answer_tsql(SOURCE_SHAPE_QUERY, shape)
    connection = CatalogueConnection(lambda _statement: shape)
    session.answer_tsql(
        "SELECT TABLE_NAME, COLUMN_NAME, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS "
        "WHERE TABLE_SCHEMA = N'_'",
        shape,
    )
    if INSTALLATION.name in tables:
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


def _statements(session):
    return [statement for call in session.calls for statement in call.body]


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
    assert {r["workspace_id"] for r in resolved.fork.identities} == {
        _identifier("workspace", plan.workspace.workspace)
    }
    assert "older Weaver version" in resolved.describe()
    # The source is only read.
    assert all(
        statement.lstrip().lower().startswith("select")
        for statement in _statements(session)
    )


@weaver_test()
def test_no_item_from_a_released_source_still_verifies_its_installations(
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
def test_a_source_holding_null_where_a_value_is_required_refuses(monkeypatch):
    tables = _with(
        current(),
        "Registry",
        (
            ("Signature", "varchar(128)", False) if c[0] == "Signature" else c
            for c in current()["Registry"]
        ),
    )
    plan, session, module = _preflight(monkeypatch, tables, [], warehouses=[])
    fork = _fork(tables)
    session.answer_tsql(
        null_count_statement(fork.nullable),
        [{"Table": "Registry", "Column": "Signature", "Rows": 2}],
    )

    with pytest.raises(CommandError, match=r"Registry\.Signature \(2 rows\)"):
        module.check_mirror(plan, session=session)


@weaver_test()
def test_a_source_holding_no_catalogue_is_named(monkeypatch):
    plan, session, module = _preflight(monkeypatch, {}, [], warehouses=[])

    with pytest.raises(CommandError, match="does not contain a Weaver catalogue"):
        module.check_mirror(plan, session=session)


@weaver_test()
def test_a_current_source_reads_nothing_more_than_before(monkeypatch):
    """Same-version preflight is the shape read and the selected installations."""

    plan, session, module = _preflight(
        monkeypatch,
        current(),
        [],
        warehouses=[],
    )

    resolved = module.check_mirror(plan, session=session)

    assert resolved.fork.is_current
    assert "older" not in resolved.describe()
    assert _statements(session) == [SOURCE_SHAPE_QUERY]
