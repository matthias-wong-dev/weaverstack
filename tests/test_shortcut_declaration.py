"""What a repository's shortcut declarations mean.

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
from weaver.declaration.model import ShortcutDeclaration, WeaverItemId
from weaver.errors import DiscoveryError
from weaver.etl import item_load_artefacts
from weaver.locations import Location

CURATED = WeaverItemId.parse("Lakehouse/Curated")


def _shortcuts(root: Path, item: str, body: str) -> None:
    _write(root, f"{item}/shortcuts.py", "from weaver import Shortcut\n\n" + body)


def _parse(root: Path):
    return parse_item_repository(Location(str(root)))


def _declaration(shortcut_type: str, target_type: str, target: str, **extra) -> str:
    arguments = "".join(f"    {name}={value!r},\n" for name, value in extra.items())
    return (
        f'    shortcut_type="{shortcut_type}",\n'
        f'    target_type="{target_type}",\n'
        f'    target="{target}",\n' + arguments
    )


# --- what a declaration names -------------------------------------------------


@weaver_test()
def test_a_table_shortcut_is_named_by_its_symbol(tmp_path):
    """The authored name is the destination, as it is for every other document."""

    root = _estate(tmp_path)
    _shortcuts(
        root,
        "Lakehouse/Curated",
        "Sales__Landed = Shortcut(\n"
        + _declaration("table", "logical", "Lakehouse/Raw/Tables/Sales.Customer")
        + ")\n",
    )
    repository = _parse(root)

    declaration = repository.shortcuts[0]
    assert str(declaration.destination) == "Lakehouse/Curated/Tables/Sales.Landed"
    assert declaration.relative_path == "Lakehouse/Curated/shortcuts.py"
    assert declaration.is_logical


@weaver_test()
def test_a_schema_shortcut_is_named_by_the_schema_it_presents(tmp_path):
    root = _estate(tmp_path)
    _shortcuts(
        root,
        "Lakehouse/Curated",
        "Reference = Shortcut(\n"
        + _declaration(
            "schema", "physical", "Lakehouse/Reference/Sales", workspace="Shared Data"
        )
        + ")\n",
    )
    repository = _parse(root)

    declaration = repository.shortcuts[0]
    assert str(declaration.destination) == "Lakehouse/Curated/Reference"
    assert declaration.workspace == "Shared Data"
    assert declaration.target_schema == "Sales"
    assert declaration.target_object is None


@weaver_test()
def test_a_folder_shortcut_points_beneath_files(tmp_path):
    root = _estate(tmp_path)
    _shortcuts(
        root,
        "Lakehouse/Curated",
        "Sales__Incoming = Shortcut(\n"
        + _declaration(
            "folder",
            "physical",
            "Lakehouse/Landing/Files/Incoming/Daily",
            workspace="Shared Data",
        )
        + ")\n",
    )
    repository = _parse(root)

    declaration = repository.shortcuts[0]
    assert declaration.destination.is_files
    assert str(declaration.destination) == "Lakehouse/Curated/Files/Sales.Incoming"
    assert declaration.target_schema == "Incoming/Daily"


@weaver_test()
def test_a_warehouse_declares_its_shortcuts_by_section(tmp_path):
    """The section says how the target is read. The mapping is destination to target."""

    root = _estate(tmp_path)
    _write(
        root,
        "Warehouse/Reporting/shortcuts.yml",
        "logical:\n"
        "  Warehouse/Reporting/Sales.Landed: Lakehouse/Curated/Tables/Sales.Customer\n"
        "physical:\n"
        "  Warehouse/Reporting/Sales.External: Warehouse/Reference/Sales.Customer\n",
    )
    repository = _parse(root)

    by_name = {
        declaration.name: declaration
        for declaration in repository.shortcuts
        if declaration.owner == WeaverItemId.parse("Warehouse/Reporting")
    }
    assert by_name["Sales__Landed"].target_type == "logical"
    assert by_name["Sales__External"].target_type == "physical"
    assert {each.shortcut_type for each in by_name.values()} == {"view"}


# --- what a declaration refuses -----------------------------------------------


@weaver_test()
@pytest.mark.parametrize(
    ("body", "expected"),
    [
        (
            "X = Shortcut(\n"
            + _declaration("view", "logical", "Lakehouse/Raw/Tables/Sales.Customer")
            + ")",
            "belongs to a Warehouse item",
        ),
        (
            "Sales__X = Shortcut(\n"
            + _declaration(
                "table",
                "logical",
                "Lakehouse/Raw/Tables/Sales.Customer",
                workspace="Other",
            )
            + ")",
            "cannot name a workspace",
        ),
        (
            "Reference = Shortcut(\n"
            + _declaration("schema", "logical", "Lakehouse/Raw/Sales")
            + ")",
            "target must be physical",
        ),
        (
            "Landed = Shortcut(\n"
            + _declaration("table", "logical", "Lakehouse/Raw/Tables/Sales.Customer")
            + ")",
            "must be Schema__Object",
        ),
        (
            "Sales__X = Shortcut(\n"
            + _declaration("table", "physical", "Lakehouse/Raw")
            + ")",
            "followed by what it points at",
        ),
        (
            "Sales__X = Shortcut(\n"
            + _declaration("folder", "physical", "Lakehouse/Raw/Tables/Sales")
            + ")",
            "must name a path beneath Files/",
        ),
        (
            "Sales__X = Shortcut(\n"
            + _declaration("folder", "physical", "Lakehouse/Raw/Files/a/../b")
            + ")",
            "must not contain '.' or '..'",
        ),
        (
            "Sales__X = Shortcut(\n"
            + _declaration("table", "sortof", "Lakehouse/Raw/Tables/Sales.Customer")
            + ")",
            "target_type must be one of logical, physical",
        ),
        (
            "Sales__X = Shortcut(\n"
            + _declaration(
                "table", "logical", "Lakehouse/Curated/Tables/Sales.Customer"
            )
            + ")",
            "which is the item declaring it",
        ),
    ],
)
def test_a_malformed_declaration_is_refused(tmp_path, body, expected):
    root = _estate(tmp_path)
    _shortcuts(root, "Lakehouse/Curated", body + "\n")
    with pytest.raises(DiscoveryError, match=expected):
        _parse(root)


@weaver_test()
def test_a_target_type_is_compared_rather_than_coerced():
    """A closed vocabulary, so anything truthy is not silently logical."""

    from weaver.errors import IdentityError

    for value in (True, 1, "Logical", "", None):
        with pytest.raises(IdentityError, match="target_type must be one of"):
            ShortcutDeclaration(
                owner=CURATED,
                name="Sales__X",
                shortcut_type="table",
                target_type=value,
                target="Lakehouse/Raw/Tables/Sales.Customer",
            )


@weaver_test()
@pytest.mark.parametrize(
    ("body", "expected"),
    [
        ("for name in ('a',):\n    pass\n", "declarations, imports and comments only"),
        (
            'Sales__X = Shortcut(shortcut_type=KIND, target_type="logical", target="a/b/c.d")\n',
            "must be a constant",
        ),
        (
            'Sales__X = other(shortcut_type="table", target_type="logical", target="Lakehouse/Raw/Tables/Sales.Customer")\n',
            "declares Shortcuts only",
        ),
        ('Sales__X = Shortcut(shortcut_type="table")\n', "declares no target_type"),
        (
            "Sales__X = Shortcut(\n"
            + _declaration(
                "table", "logical", "Lakehouse/Raw/Tables/Sales.Customer", mode="fast"
            )
            + ")\n",
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
def test_an_unknown_warehouse_section_is_refused(tmp_path):
    root = _estate(tmp_path)
    _write(
        root,
        "Warehouse/Reporting/shortcuts.yml",
        "bound:\n  Warehouse/Reporting/Sales.X: Lakehouse/Curated/Tables/Sales.Customer\n",
    )
    with pytest.raises(DiscoveryError, match="names section\\(s\\) bound"):
        _parse(root)


@weaver_test()
def test_two_declarations_may_not_differ_only_by_case(tmp_path):
    root = _estate(tmp_path)
    _shortcuts(
        root,
        "Lakehouse/Curated",
        "Sales__Landed = Shortcut(\n"
        + _declaration("table", "logical", "Lakehouse/Raw/Tables/Sales.Customer")
        + ")\n"
        "sales__landed = Shortcut(\n"
        + _declaration("table", "logical", "Lakehouse/Raw/Tables/Sales.Order")
        + ")\n",
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
        + _declaration("table", "logical", "Lakehouse/Raw/Tables/Sales.Customer")
        + ")\n",
    )
    with pytest.raises(DiscoveryError, match="already declares"):
        _parse(root)


@weaver_test()
def test_each_item_type_declares_on_its_own_surface(tmp_path):
    root = _estate(tmp_path)
    _shortcuts(
        root,
        "Warehouse/Reporting",
        "Sales__X = Shortcut(\n"
        + _declaration("table", "logical", "Lakehouse/Raw/Tables/Sales.Customer")
        + ")\n",
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
        + _declaration(
            "schema", "physical", "Lakehouse/Reference/Sales", workspace="Shared Data"
        )
        + ")\n",
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
        + _declaration(
            "schema", "physical", "Lakehouse/Ref/Sales", workspace="Shared Data"
        )
        + ")\n\n"
        "Reference__Extra = Shortcut(\n"
        + _declaration(
            "table",
            "physical",
            "Lakehouse/Ref/Tables/Sales.Extra",
            workspace="Shared Data",
        )
        + ")\n",
    )
    with pytest.raises(DiscoveryError, match="Weaver owns the shortcut and nothing"):
        _parse(root)


# --- what importing one means -------------------------------------------------


@weaver_test()
def test_importing_a_logical_shortcut_depends_on_what_it_points_at(tmp_path):
    root = _estate(tmp_path)
    _shortcuts(
        root,
        "Lakehouse/Curated",
        "Sales__Landed = Shortcut(\n"
        + _declaration("table", "logical", "Lakehouse/Raw/Tables/Sales.Customer")
        + ")\n",
    )
    _write(
        root,
        "Lakehouse/Curated/Tables/Sales__Report.py",
        _table("Sales.Report").replace(
            "from weaver import Table",
            "from shortcuts import Sales__Landed\n\nfrom weaver import Table",
        ),
    )
    repository = _parse(root)

    edge = next(
        edge
        for edge in repository.dependency_edges
        if str(edge.consumer) == "Lakehouse/Curated/Tables/Sales.Report"
    )
    assert edge.reference == "shortcuts.Sales__Landed"
    assert str(edge.producer) == "Lakehouse/Raw/Tables/Sales.Customer"
    assert edge.uses_shortcut


@weaver_test()
def test_importing_a_physical_shortcut_is_a_boundary(tmp_path):
    """It names something Weaver does not manage, so there is no producer here."""

    root = _estate(tmp_path)
    _shortcuts(
        root,
        "Lakehouse/Curated",
        "Sales__External = Shortcut(\n"
        + _declaration(
            "table",
            "physical",
            "Lakehouse/Reference/Tables/Sales.Customer",
            workspace="Shared Data",
        )
        + ")\n",
    )
    _write(
        root,
        "Lakehouse/Curated/Tables/Sales__Report.py",
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
        + _declaration("table", "logical", "Lakehouse/Raw/Tables/Sales.Customer")
        + ")\n",
    )
    _write(
        root,
        "Lakehouse/Curated/Tables/Sales__Report.py",
        _table("Sales.Report").replace(
            "from weaver import Table",
            "from shortcuts import Sales__Missing\n\nfrom weaver import Table",
        ),
    )
    with pytest.raises(DiscoveryError, match="declares Sales__Landed"):
        _parse(root)


# --- what a load can import ---------------------------------------------------


def _deployed(root: Path) -> str:
    """The generated ``shortcuts.py`` the Curated item deploys."""

    artefact = next(
        artefact
        for artefact in item_load_artefacts(_parse(root), item=CURATED)
        if artefact.identity.object_id.object == "shortcuts.py"
    )
    return artefact.payload.decode("utf-8")


@weaver_test()
def test_the_deployed_module_reads_the_destination_and_names_the_source(tmp_path):
    """Data comes from this item's own object; Weaver metadata from the source.

    A logical declaration deploys both: the destination it reads through and the
    Weaver document it points at. Which workspace and which Fabric item that
    document was bound to was settled at build.
    """

    root = _estate(tmp_path)
    _shortcuts(
        root,
        "Lakehouse/Curated",
        "Sales__Landed = Shortcut(\n"
        + _declaration("table", "logical", "Lakehouse/Raw/Tables/Sales.Customer")
        + ")\n\n"
        "Sales__Incoming = Shortcut(\n"
        + _declaration("folder", "logical", "Lakehouse/Raw/Files/Sales.Customer")
        + ")\n\n"
        "Reference = Shortcut(\n"
        + _declaration(
            "schema", "physical", "Lakehouse/Reference/Sales", workspace="Shared Data"
        )
        + ")\n",
    )

    deployed = _deployed(root)

    assert (
        "Sales__Landed = TableShortcut(schema='Sales', object='Landed', "
        "source='Lakehouse/Raw/Tables/Sales.Customer')" in deployed
    )
    assert (
        "Sales__Incoming = FolderShortcut(schema='Sales', object='Incoming', "
        "source='Lakehouse/Raw/Files/Sales.Customer')" in deployed
    )
    assert "Reference = SchemaShortcut(schema='Reference')" in deployed


@weaver_test()
def test_a_physical_declaration_deploys_no_weaver_source(tmp_path):
    """It names a Fabric location, so there is no Weaver document to carry."""

    root = _estate(tmp_path)
    _shortcuts(
        root,
        "Lakehouse/Curated",
        "Sales__Landed = Shortcut(\n"
        + _declaration(
            "table",
            "physical",
            "Lakehouse/Reference/Tables/Sales.Customer",
            workspace="Shared",
        )
        + ")\n",
    )

    deployed = _deployed(root)

    assert "Sales__Landed = TableShortcut(schema='Sales', object='Landed')" in deployed
    assert "source=" not in deployed


@weaver_test()
def test_the_authored_declaration_is_not_the_runtime_one():
    """Importing ``weaver.Shortcut`` in a load says what to do instead."""

    from weaver import Shortcut
    from weaver.errors import LoadError

    declaration = Shortcut(
        shortcut_type="table",
        target_type="logical",
        target="Lakehouse/Raw/Tables/Sales.Customer",
    )
    with pytest.raises(LoadError, match="deployed 'shortcuts' module"):
        declaration(object())


@weaver_test()
def test_a_declaration_is_certified_with_the_item_that_declares_it(tmp_path):
    root = _estate(tmp_path)
    before = _parse(root)
    _shortcuts(
        root,
        "Lakehouse/Curated",
        "Sales__Landed = Shortcut(\n"
        + _declaration("table", "logical", "Lakehouse/Raw/Tables/Sales.Customer")
        + ")\n",
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

    def declaration(**overrides):
        arguments = {
            "owner": CURATED,
            "name": "Sales__Landed",
            "shortcut_type": "table",
            "target_type": "logical",
            "target": "Lakehouse/Raw/Tables/Sales.Customer",
        }
        arguments.update(overrides)
        return ShortcutDeclaration(**arguments)

    assert (
        declaration().signature
        == declaration(relative_path="elsewhere/shortcuts.py").signature
    )
    assert (
        declaration().signature
        != declaration(target="Lakehouse/Raw/Tables/Sales.Order").signature
    )


# --- the runtime a program uses ----------------------------------------------
#
# A shortcut is materialised locally, so a program addresses the destination
# item's own object. Whether the source was logical or physical, and which
# workspace it was in, was settled when the bundle was generated.


class _Lakehouse:
    """Enough of a resolved Lakehouse to record what was addressed."""

    name = "Curated_LH"

    def table_path(self, schema, name):
        return f"/lake/Curated_LH/Tables/{schema}/{name}"

    def folder_path(self, schema, name):
        return f"/lake/Curated_LH/Files/{schema}/{name}"

    def folder_spark_path(self, schema, name):
        return f"abfss://ws@onelake/Curated_LH/Files/{schema}/{name}"


class _Spark:
    def __init__(self) -> None:
        self.loaded: list[str] = []
        self.read = self

    def format(self, _name):
        return self

    def load(self, path):
        self.loaded.append(path)
        return path


class _Owner:
    def __init__(self) -> None:
        self.spark = _Spark()
        self.lakehouse = _Lakehouse()


@weaver_test()
def test_a_table_shortcut_reads_the_local_destination():
    from weaver.shortcuts import TableShortcut

    owner = _Owner()
    TableShortcut(schema="Sales", object="Customer")(owner).dataframe()

    assert owner.spark.loaded == ["/lake/Curated_LH/Tables/Sales/Customer"]


@weaver_test()
def test_a_folder_shortcut_has_both_spellings_of_its_location():
    from weaver.shortcuts import FolderShortcut

    reader = FolderShortcut(schema="Sales", object="Incoming")(_Owner())

    assert reader.path() == "/lake/Curated_LH/Files/Sales/Incoming"
    assert reader.spark_path() == "abfss://ws@onelake/Curated_LH/Files/Sales/Incoming"


@weaver_test()
def test_a_schema_shortcut_reads_a_table_by_attribute_or_by_name():
    """Attribute access is the ordinary form; ``table`` stays for other names.

    Both reach the same place, and neither generates a symbol: the tables belong
    to the item the shortcut points at and can change without a build.
    """

    from weaver.shortcuts import SchemaShortcut

    owner = _Owner()
    shortcut = SchemaShortcut(schema="Reference")(owner)

    shortcut.Customer.dataframe()
    shortcut.table("Customer").dataframe()
    shortcut.table("Customer Detail").dataframe()

    assert owner.spark.loaded == [
        "/lake/Curated_LH/Tables/Reference/Customer",
        "/lake/Curated_LH/Tables/Reference/Customer",
        "/lake/Curated_LH/Tables/Reference/Customer Detail",
    ]


@weaver_test()
def test_a_schema_shortcut_does_not_answer_for_private_names():
    """So a copy or a pickle does not read as a table lookup."""

    from weaver.shortcuts import SchemaShortcut

    shortcut = SchemaShortcut(schema="Reference")(_Owner())

    with pytest.raises(AttributeError):
        shortcut.__deepcopy__


@weaver_test()
def test_a_schema_shortcut_reads_a_table_by_name():
    from weaver.errors import LoadError
    from weaver.shortcuts import SchemaShortcut

    shortcut = SchemaShortcut(schema="Reference")(_Owner())

    with pytest.raises(LoadError, match="reads a table by name"):
        shortcut.table("")


# --- destinations and targets name the area ------------------------------------


@weaver_test()
def test_a_shortcut_destination_sits_in_the_area_its_type_gives_it():
    """``shortcut_type`` decides the destination's area, as it decides its path."""

    table = ShortcutDeclaration(
        owner=CURATED,
        name="Sales__Customer",
        shortcut_type="table",
        target_type="logical",
        target="Lakehouse/Raw/Tables/Sales.Customer",
    )
    folder = ShortcutDeclaration(
        owner=CURATED,
        name="Sales__Customer",
        shortcut_type="folder",
        target_type="logical",
        target="Lakehouse/Raw/Files/Sales.Customer",
    )

    assert str(table.destination) == "Lakehouse/Curated/Tables/Sales.Customer"
    assert str(folder.destination) == "Lakehouse/Curated/Files/Sales.Customer"
    assert table.destination != folder.destination


@weaver_test()
def test_a_warehouse_view_destination_stays_a_bare_relation():
    view = ShortcutDeclaration(
        owner=WeaverItemId.parse("Warehouse/Reporting"),
        name="Sales__Customer",
        shortcut_type="view",
        target_type="logical",
        target="Lakehouse/Curated/Tables/Sales.Customer",
    )

    assert str(view.destination) == "Warehouse/Reporting/Sales.Customer"
    assert str(view.logical_source) == "Lakehouse/Curated/Tables/Sales.Customer"


@weaver_test()
def test_a_logical_lakehouse_table_target_names_its_area(tmp_path):
    """A logical target is a Weaver identity, and the old spelling is not one."""

    root = _estate(tmp_path)
    _shortcuts(
        root,
        "Lakehouse/Curated",
        "Sales__Portable = Shortcut(\n"
        + _declaration("table", "logical", "Lakehouse/Raw/Sales.Customer")
        + ")\n",
    )

    with pytest.raises(DiscoveryError, match="is not a managed object"):
        _parse(root)


@weaver_test()
def test_a_logical_target_that_names_its_area_resolves(tmp_path):
    root = _estate(tmp_path)
    _shortcuts(
        root,
        "Lakehouse/Curated",
        "Sales__Portable = Shortcut(\n"
        + _declaration("table", "logical", "Lakehouse/Raw/Tables/Sales.Customer")
        + ")\n",
    )

    pairs = {
        str(pair.destination): str(pair.source)
        for pair in _parse(root).logical_shortcuts
    }

    assert pairs["Lakehouse/Curated/Tables/Sales.Portable"] == (
        "Lakehouse/Raw/Tables/Sales.Customer"
    )


@weaver_test()
def test_a_physical_table_target_may_spell_its_area_or_leave_it_out():
    """A physical target names a Fabric item, whose Tables area the path adds."""

    spelled = ShortcutDeclaration(
        owner=CURATED,
        name="Sales__Portable",
        shortcut_type="table",
        target_type="physical",
        target="Lakehouse/External/Tables/Sales.Customer",
        workspace="Shared Data",
    )
    bare = ShortcutDeclaration(
        owner=CURATED,
        name="Sales__Portable",
        shortcut_type="table",
        target_type="physical",
        target="Lakehouse/External/Sales.Customer",
        workspace="Shared Data",
    )

    assert spelled.target_tail == bare.target_tail == "Sales.Customer"
    assert spelled.target_object == bare.target_object
    assert spelled.target_schema == bare.target_schema == "Sales"
