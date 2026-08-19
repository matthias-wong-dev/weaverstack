"""What a repository's shortcut and external-reference declarations mean.

Pure Python. Everything here is a decision a build makes before it touches a
workspace: what a declaration names, what it refuses, what a program importing it
depends on, and what is deployed so the same name can be imported at load time.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from support.weaver_test import weaver_test
from test_item_repository_declaration import _estate, _table, _write

from weaver.declaration import parse_item_repository
from weaver.declaration.model import (
    ShortcutDeclaration,
    WeaverItemId,
)
from weaver.errors import DiscoveryError
from weaver.etl import item_load_artefacts
from weaver.locations import Location

CURATED = WeaverItemId.parse("Lakehouse/Curated")


def _shortcuts(root: Path, item: str, body: str) -> None:
    _write(root, f"{item}/shortcuts.py", "from weaver import Shortcut\n\n" + body)


def _parse(root: Path):
    return parse_item_repository(Location(str(root)))


# --- what a declaration names -------------------------------------------------


@weaver_test()
def test_a_table_shortcut_is_named_by_its_symbol(tmp_path):
    """The authored name is the destination, as it is for every other document."""

    root = _estate(tmp_path)
    _shortcuts(
        root,
        "Lakehouse/Curated",
        "Sales__Landed = Shortcut(\n"
        '    shortcut_type="table",\n'
        '    target="Lakehouse/Raw/Sales.Customer",\n'
        "    bind=True,\n)\n",
    )
    repository = _parse(root)

    declaration = repository.shortcuts[0]
    assert str(declaration.destination) == "Lakehouse/Curated/Sales.Landed"
    assert declaration.relative_path == "Lakehouse/Curated/shortcuts.py"


@weaver_test()
def test_a_schema_shortcut_is_named_by_the_schema_it_presents(tmp_path):
    root = _estate(tmp_path)
    _shortcuts(
        root,
        "Lakehouse/Curated",
        "Reference = Shortcut(\n"
        '    shortcut_type="schema",\n'
        '    target="Lakehouse/Reference/Sales",\n'
        '    workspace="Shared Data",\n)\n',
    )
    repository = _parse(root)

    declaration = repository.shortcuts[0]
    assert str(declaration.destination) == "Lakehouse/Curated/Reference"
    assert declaration.workspace == "Shared Data"


@weaver_test()
def test_a_folder_shortcut_points_beneath_files(tmp_path):
    root = _estate(tmp_path)
    _shortcuts(
        root,
        "Lakehouse/Curated",
        "Sales__Incoming = Shortcut(\n"
        '    shortcut_type="folder",\n'
        '    target="Lakehouse/Landing/Files/Incoming",\n'
        '    workspace="Shared Data",\n)\n',
    )
    repository = _parse(root)

    destination = repository.shortcuts[0].destination
    assert destination.is_files
    assert str(destination) == "Lakehouse/Curated/Files/Sales.Incoming"


# --- what a declaration refuses -----------------------------------------------


@weaver_test()
@pytest.mark.parametrize(
    ("body", "expected"),
    [
        (
            'X = Shortcut(shortcut_type="view", target="Lakehouse/Raw/Sales.Customer")',
            "shortcut_type must be one of",
        ),
        (
            "Sales__X = Shortcut(\n"
            '    shortcut_type="table",\n'
            '    target="Lakehouse/Raw/Sales.Customer",\n'
            '    workspace="Elsewhere",\n'
            "    bind=True,\n)",
            "cannot also name a workspace",
        ),
        (
            "Reference = Shortcut(\n"
            '    shortcut_type="schema",\n'
            '    target="Lakehouse/Raw/Sales",\n'
            "    bind=True,\n)",
            "has no bound form",
        ),
        (
            'Landed = Shortcut(shortcut_type="table", target="Lakehouse/Raw/Sales.Customer")',
            "must be Schema__Object",
        ),
        (
            'Sales__X = Shortcut(shortcut_type="table", target="Lakehouse/Raw")',
            "followed by what it points at",
        ),
        (
            "Sales__X = Shortcut(\n"
            '    shortcut_type="folder",\n'
            '    target="Lakehouse/Raw/Tables/Sales",\n)',
            "must name a path beneath Files/",
        ),
    ],
)
def test_a_malformed_declaration_is_refused(tmp_path, body, expected):
    root = _estate(tmp_path)
    _shortcuts(root, "Lakehouse/Curated", body + "\n")
    with pytest.raises(DiscoveryError, match=expected):
        _parse(root)


@weaver_test()
@pytest.mark.parametrize(
    ("body", "expected"),
    [
        ("for name in ('a',):\n    pass\n", "declarations, imports and comments only"),
        (
            'Sales__X = Shortcut(shortcut_type=KIND, target="a/b/c.d")\n',
            "must be a constant",
        ),
        (
            'Sales__X = other(shortcut_type="table", target="Lakehouse/Raw/Sales.Customer")\n',
            "declares Shortcuts only",
        ),
        (
            'Sales__X = Shortcut(shortcut_type="table")\n',
            "declares no target",
        ),
        (
            "Sales__X = Shortcut(\n"
            '    shortcut_type="table",\n'
            '    target="Lakehouse/Raw/Sales.Customer",\n'
            '    mode="fast",\n)\n',
            "names 'mode'",
        ),
    ],
)
def test_shortcuts_py_is_declarations_rather_than_a_program(tmp_path, body, expected):
    """Parsed, never run, so anything with a meaning only when executed is refused."""

    root = _estate(tmp_path)
    _shortcuts(root, "Lakehouse/Curated", body)
    with pytest.raises(DiscoveryError, match=expected):
        _parse(root)


@weaver_test()
def test_two_declarations_may_not_differ_only_by_case(tmp_path):
    root = _estate(tmp_path)
    _shortcuts(
        root,
        "Lakehouse/Curated",
        'Sales__Landed = Shortcut(shortcut_type="table", target="Lakehouse/Raw/Sales.Customer")\n'
        'sales__landed = Shortcut(shortcut_type="table", target="Lakehouse/Raw/Sales.Order")\n',
    )
    with pytest.raises(DiscoveryError, match="differ only by case"):
        _parse(root)


@weaver_test()
def test_a_shortcut_may_not_be_called_what_the_item_already_declares(tmp_path):
    root = _estate(tmp_path)
    _shortcuts(
        root,
        "Lakehouse/Curated",
        "Sales__Customer = Shortcut(\n"
        '    shortcut_type="table",\n'
        '    target="Lakehouse/Raw/Sales.Customer",\n'
        "    bind=True,\n)\n",
    )
    with pytest.raises(DiscoveryError, match="already declares"):
        _parse(root)


@weaver_test()
def test_shortcuts_py_belongs_to_a_lakehouse(tmp_path):
    root = _estate(tmp_path)
    _shortcuts(
        root,
        "Warehouse/Reporting",
        'Sales__X = Shortcut(shortcut_type="table", target="Lakehouse/Raw/Sales.Customer")\n',
    )
    with pytest.raises(DiscoveryError, match="belong to a Lakehouse item"):
        _parse(root)


# --- Weaver owns the shortcut root and nothing beneath it ---------------------


@weaver_test()
def test_nothing_may_be_declared_inside_a_schema_shortcut(tmp_path):
    """OneLake makes a shortcut a read-write window into the item it points at.

    A write beneath one lands in that item, so the namespace a schema shortcut
    presents is not this item's to put anything in.
    """

    root = _estate(tmp_path)
    _shortcuts(
        root,
        "Lakehouse/Curated",
        "Sales = Shortcut(\n"
        '    shortcut_type="schema",\n'
        '    target="Lakehouse/Reference/Sales",\n'
        '    workspace="Shared Data",\n)\n',
    )
    with pytest.raises(DiscoveryError, match="Weaver owns the shortcut and nothing"):
        _parse(root)


@weaver_test()
def test_a_table_shortcut_may_not_sit_inside_a_schema_shortcut(tmp_path):
    root = _estate(tmp_path)
    _write(
        root, "Lakehouse/Curated/schemas/Ref.yml", "Schema ID: Ref\nDescription: x.\n"
    )
    _shortcuts(
        root,
        "Lakehouse/Curated",
        "Reference = Shortcut(\n"
        '    shortcut_type="schema",\n'
        '    target="Lakehouse/Ref/Sales",\n'
        '    workspace="Shared Data",\n)\n'
        "\n"
        "Reference__Extra = Shortcut(\n"
        '    shortcut_type="table",\n'
        '    target="Lakehouse/Ref/Sales.Extra",\n'
        '    workspace="Shared Data",\n)\n',
    )
    with pytest.raises(DiscoveryError, match="Weaver owns the shortcut and nothing"):
        _parse(root)


# --- what importing one means -------------------------------------------------


@weaver_test()
def test_importing_a_bound_shortcut_depends_on_what_it_points_at(tmp_path):
    root = _estate(tmp_path)
    _shortcuts(
        root,
        "Lakehouse/Curated",
        "Sales__Landed = Shortcut(\n"
        '    shortcut_type="table",\n'
        '    target="Lakehouse/Raw/Sales.Customer",\n'
        "    bind=True,\n)\n",
    )
    _write(
        root,
        "Lakehouse/Curated/Sales__Report.py",
        _table("Sales.Report").replace(
            "from weaver import Table",
            "from shortcuts import Sales__Landed\n\nfrom weaver import Table",
        ),
    )
    repository = _parse(root)

    edge = next(
        edge
        for edge in repository.dependency_edges
        if str(edge.consumer) == "Lakehouse/Curated/Sales.Report"
    )
    assert edge.reference == "shortcuts.Sales__Landed"
    assert str(edge.producer) == "Lakehouse/Raw/Sales.Customer"
    assert edge.resolution_kind == "alias"


@weaver_test()
def test_importing_a_direct_shortcut_is_a_physical_boundary(tmp_path):
    """It names something Weaver does not manage, so there is no producer here."""

    root = _estate(tmp_path)
    _shortcuts(
        root,
        "Lakehouse/Curated",
        "Sales__External = Shortcut(\n"
        '    shortcut_type="table",\n'
        '    target="Lakehouse/Reference/Sales.Customer",\n'
        '    workspace="Shared Data",\n)\n',
    )
    _write(
        root,
        "Lakehouse/Curated/Sales__Report.py",
        _table("Sales.Report").replace(
            "from weaver import Table",
            "from shortcuts import Sales__External\n\nfrom weaver import Table",
        ),
    )
    repository = _parse(root)

    edge = next(
        edge
        for edge in repository.dependency_edges
        if edge.reference == "shortcuts.Sales__External"
    )
    assert edge.producer is None
    assert edge.is_physical


@weaver_test()
def test_importing_a_name_no_shortcut_declares_says_what_is_declared(tmp_path):
    root = _estate(tmp_path)
    _shortcuts(
        root,
        "Lakehouse/Curated",
        "Sales__Landed = Shortcut(\n"
        '    shortcut_type="table",\n'
        '    target="Lakehouse/Raw/Sales.Customer",\n'
        "    bind=True,\n)\n",
    )
    _write(
        root,
        "Lakehouse/Curated/Sales__Report.py",
        _table("Sales.Report").replace(
            "from weaver import Table",
            "from shortcuts import Sales__Missing\n\nfrom weaver import Table",
        ),
    )
    with pytest.raises(DiscoveryError, match="declares Sales__Landed"):
        _parse(root)


# --- what a load can import ---------------------------------------------------


@weaver_test()
def test_the_deployed_module_names_the_destination_rather_than_the_source(tmp_path):
    """A program reads this item's own table. The source was settled at build."""

    root = _estate(tmp_path)
    _shortcuts(
        root,
        "Lakehouse/Curated",
        "Sales__Landed = Shortcut(\n"
        '    shortcut_type="table",\n'
        '    target="Lakehouse/Raw/Sales.Customer",\n'
        "    bind=True,\n)\n"
        "\n"
        "Reference = Shortcut(\n"
        '    shortcut_type="schema",\n'
        '    target="Lakehouse/Reference/Sales",\n'
        '    workspace="Shared Data",\n)\n',
    )
    repository = _parse(root)

    artefact = next(
        artefact
        for artefact in item_load_artefacts(repository, item=CURATED)
        if artefact.identity.object_id.object == "shortcuts.py"
    )
    deployed = artefact.payload.decode("utf-8")

    assert "Sales__Landed = TableShortcut(schema='Sales', object='Landed')" in deployed
    assert "Reference = SchemaShortcut(schema='Reference')" in deployed
    assert "Lakehouse/Raw" not in deployed


@weaver_test()
def test_the_authored_declaration_is_not_the_runtime_one():
    """Importing ``weaver.Shortcut`` in a load says what to do instead."""

    from weaver import Shortcut
    from weaver.errors import LoadError

    declaration = Shortcut(shortcut_type="table", target="Lakehouse/Raw/Sales.Customer")
    with pytest.raises(LoadError, match="deployed 'shortcuts' module"):
        declaration(object())


@weaver_test()
def test_a_schema_shortcut_names_its_tables_when_they_are_read():
    """Its contents belong to the item it points at and change without a build."""

    from weaver.shortcuts import SchemaShortcut

    class _Lakehouse:
        def table_path(self, schema, name):
            return f"/lake/Tables/{schema}/{name}"

    class _Owner:
        spark = None
        lakehouse = _Lakehouse()

    reader = SchemaShortcut(schema="Reference")(_Owner())
    assert reader.table("Customer")._name == "Customer"
    with pytest.raises(Exception, match="reads a table by name"):
        reader.table("")


@weaver_test()
def test_a_declaration_is_certified_with_the_item_that_declares_it(tmp_path):
    root = _estate(tmp_path)
    before = _parse(root)
    _shortcuts(
        root,
        "Lakehouse/Curated",
        "Sales__Landed = Shortcut(\n"
        '    shortcut_type="table",\n'
        '    target="Lakehouse/Raw/Sales.Customer",\n'
        "    bind=True,\n)\n",
    )
    after = _parse(root)

    assert after["Lakehouse/Curated"].signature != before["Lakehouse/Curated"].signature
    assert after["Lakehouse/Raw"].signature == before["Lakehouse/Raw"].signature


@weaver_test()
def test_a_declaration_carries_its_own_signature():
    """The declaration and nothing about what it points at.

    A rebuilt source does not redeclare a shortcut. That it was rebuilt is
    freshness, answered from the Registry, which is what lets an unchanged
    shortcut over an unchanged source be left alone.
    """

    first = ShortcutDeclaration(
        owner=CURATED,
        name="Sales__Landed",
        shortcut_type="table",
        target="Lakehouse/Raw/Sales.Customer",
        bind=True,
    )
    same = ShortcutDeclaration(
        owner=CURATED,
        name="Sales__Landed",
        shortcut_type="table",
        target="Lakehouse/Raw/Sales.Customer",
        bind=True,
        relative_path="somewhere/else/shortcuts.py",
    )
    moved = ShortcutDeclaration(
        owner=CURATED,
        name="Sales__Landed",
        shortcut_type="table",
        target="Lakehouse/Raw/Sales.Order",
        bind=True,
    )

    assert first.signature == same.signature
    assert first.signature != moved.signature
