"""Pure-Python, end-to-end tests for item-oriented static discovery."""

from __future__ import annotations

from pathlib import Path

import pytest
from support.weaver_test import weaver_test

from weaver.catalogue.tables import (
    CATALOGUE_TABLES,
    PROJECTED_TABLES,
    RUNTIME_TABLES,
)
from weaver.declaration import parse_item_repository
from weaver.declaration.model import WeaverDocumentId, WeaverSchemaId
from weaver.errors import DiscoveryError
from weaver.locations import Location


def _write(root: Path, relative: str, text: str) -> None:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _schema(name: str) -> str:
    return f"Schema ID: {name}\nDescription: {name} objects.\n"


def _table(object_id: str) -> str:
    class_name = object_id.replace(".", "__")
    return f'''\
"""
Table ID: {object_id}
Description: A declared table.
Lineage: A source system.
Primary key: Id
Schema:
  Id: string
"""
from weaver import Table

raise RuntimeError("static discovery must never execute this module")

class {class_name}(Table):
    def read(self):
        return [], []
'''


def _folder(object_id: str) -> str:
    class_name = object_id.replace(".", "__")
    return f'''\
"""
Folder ID: {object_id}
Description: A declared folder.
Lineage: A source system.
File key: "*.csv"
"""
from weaver import Folder

class {class_name}(Folder):
    def read(self):
        return self.staging_folder(), []
'''


def _spark_view(object_id: str) -> str:
    return f"""\
/*
View ID: {object_id}
Description: A declared view.
Lineage: A source system.
Dependencies:
  - Sales.Customer
*/
select Id from Sales.Customer
"""


def _warehouse_table(object_id: str) -> str:
    return f"""\
/*
Table ID: {object_id}
Description: A reporting table.
Lineage: A source system.
Primary key: Id
*/
select cast(1 as varchar(20)) as Id;
"""


def _estate(tmp_path: Path) -> Path:
    root = tmp_path / "Estate"
    for item in (
        "Lakehouse/Raw",
        "Lakehouse/Curated",
        "Warehouse/Reporting",
        "Warehouse/Audit",
    ):
        _write(root, f"{item}/schemas/Sales.yml", _schema("Sales"))
    _write(root, "Lakehouse/Raw/Sales__Customer.py", _table("Sales.Customer"))
    _write(
        root,
        "Lakehouse/Raw/Files/Sales__Customer.py",
        _folder("Sales.Customer"),
    )
    _write(root, "Lakehouse/Curated/Sales__Customer.py", _table("Sales.Customer"))
    _write(
        root,
        "Warehouse/Reporting/Sales.Customer.sql",
        _warehouse_table("Sales.Customer"),
    )
    _write(
        root,
        "Warehouse/Audit/Sales.Change.sql",
        _warehouse_table("Sales.Change"),
    )
    _write(root, "Lakehouse/Raw/lib/csv_helpers.py", "def rows():\n    return []\n")
    _write(root, "Warehouse/Reporting/shortcuts.yml", "# no shortcuts yet\n")
    _write(root, "_ignore/broken/__init__.py", "this is not python\n")
    _write(root, "_ignore/unfinished.py", "not valid\n")
    return root


@weaver_test()
def test_reads_multiple_items_and_owned_documents_without_execution(tmp_path):
    root = _estate(tmp_path)
    repository = parse_item_repository(Location(str(root)))

    assert repository.name == "Estate"
    assert tuple(str(item.identity) for item in repository.items) == (
        "Lakehouse/Curated",
        "Lakehouse/Raw",
        "Warehouse/Audit",
        "Warehouse/Reporting",
        "Warehouse/_weaver",
    )
    assert WeaverSchemaId.parse("Lakehouse/Raw/Sales") in repository.schema_documents
    table = WeaverDocumentId.parse("Lakehouse/Raw/Sales.Customer")
    folder = WeaverDocumentId.parse("Lakehouse/Raw/Files/Sales.Customer")
    assert table in repository.source_documents
    assert folder in repository.source_documents
    assert repository.source_documents[table].node_id == str(table)
    assert "Lakehouse/Raw/lib/csv_helpers.py" in repository.support_files


@weaver_test()
def test_ignore_is_completely_absent_including_from_signature(tmp_path):
    root = _estate(tmp_path)
    before = parse_item_repository(Location(str(root)))
    _write(root, "_ignore/new/broken.py", "different invalid content\n")
    after = parse_item_repository(Location(str(root)))

    assert before.signature == after.signature
    assert all("_ignore" not in path for path in after.support_files)


@weaver_test()
def test_ordinary_content_changes_the_signature(tmp_path):
    root = _estate(tmp_path)
    before = parse_item_repository(Location(str(root))).signature
    _write(root, "Lakehouse/Raw/lib/csv_helpers.py", "def rows():\n    return [1]\n")
    assert parse_item_repository(Location(str(root))).signature != before


@weaver_test()
def test_an_unrelated_item_changes_the_repository_but_not_this_item_signature(tmp_path):
    root = _estate(tmp_path)
    before = parse_item_repository(Location(str(root)))

    changed = _warehouse_table("Sales.Change").replace(
        "A reporting table.", "An independently changed audit table."
    )
    _write(root, "Warehouse/Audit/Sales.Change.sql", changed)
    after = parse_item_repository(Location(str(root)))

    assert after.signature != before.signature
    assert after["Lakehouse/Raw"].signature == before["Lakehouse/Raw"].signature
    assert after["Warehouse/Audit"].signature != before["Warehouse/Audit"].signature


@weaver_test()
def test_item_signature_covers_its_schema_document_and_support_files(tmp_path):
    root = _estate(tmp_path)
    before = parse_item_repository(Location(str(root)))

    _write(root, "Lakehouse/Raw/lib/csv_helpers.py", "def rows():\n    return [1]\n")
    support_changed = parse_item_repository(Location(str(root)))
    assert (
        support_changed["Lakehouse/Raw"].signature != before["Lakehouse/Raw"].signature
    )
    assert (
        support_changed["Lakehouse/Curated"].signature
        == before["Lakehouse/Curated"].signature
    )

    _write(
        root,
        "Lakehouse/Raw/schemas/Sales.yml",
        _schema("Sales").replace("Sales objects.", "Changed Sales objects."),
    )
    schema_changed = parse_item_repository(Location(str(root)))
    assert (
        schema_changed["Lakehouse/Raw"].signature
        != support_changed["Lakehouse/Raw"].signature
    )


@weaver_test()
def test_python_document_signature_covers_only_its_transitive_lib_imports(tmp_path):
    root = _estate(tmp_path)
    customer_path = root / "Lakehouse/Raw/Sales__Customer.py"
    customer_path.write_text(
        customer_path.read_text().replace(
            "from weaver import Table",
            "from weaver import Table\nfrom .lib.entry import rows",
        ),
        encoding="utf-8",
    )
    _write(
        root,
        "Lakehouse/Raw/lib/entry.py",
        "from .csv_helpers import rows\n",
    )
    _write(root, "Lakehouse/Raw/lib/unused.py", "VALUE = 1\n")

    before = parse_item_repository(Location(str(root)))
    customer = WeaverDocumentId.parse("Lakehouse/Raw/Sales.Customer")
    folder = WeaverDocumentId.parse("Lakehouse/Raw/Files/Sales.Customer")

    _write(root, "Lakehouse/Raw/lib/unused.py", "VALUE = 2\n")
    unused_changed = parse_item_repository(Location(str(root)))
    assert (
        unused_changed.source_documents[customer].effective_signature
        == before.source_documents[customer].effective_signature
    )

    _write(root, "Lakehouse/Raw/lib/csv_helpers.py", "def rows():\n    return [1]\n")
    imported_changed = parse_item_repository(Location(str(root)))
    assert (
        imported_changed.source_documents[customer].effective_signature
        != unused_changed.source_documents[customer].effective_signature
    )
    assert (
        imported_changed.source_documents[folder].effective_signature
        == unused_changed.source_documents[folder].effective_signature
    )


@weaver_test()
def test_an_external_reference_contributes_only_to_its_own_item_signature(tmp_path):
    root = _estate(tmp_path)
    before = parse_item_repository(Location(str(root)))
    _write(
        root,
        "Warehouse/Reporting/shortcuts.yml",
        "logical:\n"
        "  Warehouse/Reporting/Sales.PortableCustomer: "
        "Lakehouse/Curated/Sales.Customer\n",
    )
    after = parse_item_repository(Location(str(root)))

    assert (
        after["Warehouse/Reporting"].signature
        != before["Warehouse/Reporting"].signature
    )
    for unchanged in ("Lakehouse/Curated", "Lakehouse/Raw", "Warehouse/Audit"):
        assert after[unchanged].signature == before[unchanged].signature


@weaver_test()
def test_user_authored_init_is_rejected_outside_ignore(tmp_path):
    root = _estate(tmp_path)
    _write(root, "Lakehouse/Raw/lib/__init__.py", "")
    with pytest.raises(DiscoveryError, match="user-authored __init__.py"):
        parse_item_repository(Location(str(root)))


@weaver_test()
def test_other_underscore_directory_is_not_ignored(tmp_path):
    root = _estate(tmp_path)
    _write(root, "Lakehouse/Raw/_draft/note.txt", "parked in the wrong place")
    with pytest.raises(DiscoveryError, match="only schemas/.*lib/.*Files/"):
        parse_item_repository(Location(str(root)))


@weaver_test()
def test_empty_other_underscore_directory_is_still_discovered(tmp_path):
    root = _estate(tmp_path)
    (root / "Lakehouse/Raw/_draft").mkdir(parents=True)
    with pytest.raises(DiscoveryError, match="only schemas/.*lib/.*Files/"):
        parse_item_repository(Location(str(root)))


@weaver_test()
def test_the_owning_item_decides_which_sql_a_document_speaks(tmp_path):
    """One filename, two dialects, the directory above it is the difference.

    This is why a document needs no dialect suffix: a Lakehouse materialises
    Delta through Spark and a Warehouse materialises through T-SQL, so the
    containing item has already answered.
    """

    root = _estate(tmp_path)
    _write(root, "Lakehouse/Curated/Sales.Rollup.sql", _spark_view("Sales.Rollup"))
    repository = parse_item_repository(Location(str(root)))

    lakehouse = repository.source_documents[
        WeaverDocumentId.parse("Lakehouse/Curated/Sales.Rollup")
    ]
    warehouse = repository.source_documents[
        WeaverDocumentId.parse("Warehouse/Reporting/Sales.Customer")
    ]
    assert lakehouse.language == "spark_sql"
    assert warehouse.language == "sql"


@weaver_test()
def test_a_dialect_suffix_is_not_a_document_name(tmp_path):
    """``Sales.Rollup.spark`` is not Schema.Object, so the file names nothing.

    There is no special case for the retired ``.spark.sql`` spelling: with the
    item choosing the dialect, that suffix is simply a filename with an extra
    dot in it.
    """

    root = _estate(tmp_path)
    _write(
        root, "Lakehouse/Curated/Sales.Rollup.spark.sql", _spark_view("Sales.Rollup")
    )
    with pytest.raises(DiscoveryError, match="must name Schema and Object"):
        parse_item_repository(Location(str(root)))


@weaver_test()
def test_a_declaration_surface_at_the_root_names_the_item_it_belongs_to(tmp_path):
    root = _estate(tmp_path)
    _write(
        root,
        "shortcuts.yml",
        "logical:\n"
        "  Warehouse/Reporting/Sales.PortableCustomer: "
        "Lakehouse/Curated/Sales.Customer\n",
    )
    with pytest.raises(DiscoveryError, match="belongs to the item that declares it"):
        parse_item_repository(Location(str(root)))


@weaver_test()
def test_an_external_destination_belongs_to_the_item_declaring_it(tmp_path):
    root = _estate(tmp_path)
    _write(
        root,
        "Warehouse/Reporting/shortcuts.yml",
        "logical:\n"
        "  Warehouse/Audit/Sales.PortableCustomer: "
        "Lakehouse/Curated/Sales.Customer\n",
    )
    with pytest.raises(DiscoveryError, match="declares Warehouse/Reporting's own"):
        parse_item_repository(Location(str(root)))


@weaver_test()
def test_an_external_reference_certifies_only_its_own_item(tmp_path):
    root = _estate(tmp_path)
    before = parse_item_repository(Location(str(root)))
    _write(
        root,
        "Warehouse/Audit/shortcuts.yml",
        "logical:\n"
        "  Warehouse/Audit/Sales.PortableCustomer: "
        "Lakehouse/Curated/Sales.Customer\n",
    )
    after = parse_item_repository(Location(str(root)))

    assert after["Warehouse/Audit"].signature != before["Warehouse/Audit"].signature
    for unchanged in ("Lakehouse/Curated", "Lakehouse/Raw", "Warehouse/Reporting"):
        assert after[unchanged].signature == before[unchanged].signature
    assert "Warehouse/Audit/shortcuts.yml" in after.support_files


@weaver_test()
def test_schema_must_be_declared_by_the_owning_item(tmp_path):
    root = _estate(tmp_path)
    _write(root, "Lakehouse/Raw/Other__Thing.py", _table("Other.Thing"))
    with pytest.raises(DiscoveryError, match="not declared by item Lakehouse/Raw"):
        parse_item_repository(Location(str(root)))


@weaver_test()
def test_item_type_is_exact(tmp_path):
    root = _estate(tmp_path)
    # Use a distinct spelling so this remains meaningful on case-insensitive
    # developer filesystems; casing itself is covered by the identity tests.
    _write(root, "Inventory/Wrong/schemas/Sales.yml", _schema("Sales"))
    with pytest.raises(DiscoveryError, match="first directory must be exactly"):
        parse_item_repository(Location(str(root)))


@weaver_test()
def test_weaver_catalogue_is_a_generated_builtin_item(tmp_path):
    repository = parse_item_repository(Location(str(_estate(tmp_path))))
    builtin = repository["Warehouse/_weaver"]

    # The projected dictionaries, plus the runtime tables: the history a run
    # appends and the current state it maintains. Declared as ordinary Weaver
    # documents and built by Weaver's own build, whichever they are.
    assert len(builtin.documents) == len(PROJECTED_TABLES) + len(RUNTIME_TABLES)
    assert WeaverSchemaId.parse("Warehouse/_weaver/_") in repository.schema_documents
    assert {str(identity) for identity in builtin.documents} == {
        f"Warehouse/_weaver/_.{table.name}" for table in CATALOGUE_TABLES
    }
    assert repository.generated_files


@weaver_test()
def test_generated_weaver_item_is_composed_without_mutating_authored_tree(tmp_path):
    root = _estate(tmp_path)
    location = Location(str(root))
    repository = parse_item_repository(location)

    assert repository["Warehouse/_weaver"].documents
    assert not (root / "Lakehouse" / "_weaver").exists()


@weaver_test()
def test_authored_weaver_item_is_rejected(tmp_path):
    root = _estate(tmp_path)
    _write(root, "Warehouse/_weaver/schemas/_.yml", _schema("_"))
    with pytest.raises(DiscoveryError, match="package-owned"):
        parse_item_repository(Location(str(root)))


@weaver_test()
def test_canonical_metadata_reference_resolves_across_items(tmp_path):
    root = _estate(tmp_path)
    source = _table("Sales.Customer").replace(
        "Description: A declared table.",
        "Description: $Lakehouse/Curated/Sales.Customer",
    )
    _write(root, "Lakehouse/Raw/Sales__Customer.py", source)

    repository = parse_item_repository(Location(str(root)))
    assert repository.source_documents[
        WeaverDocumentId.parse("Lakehouse/Raw/Sales.Customer")
    ].document.description.is_reference


@weaver_test()
def test_short_metadata_reference_is_item_relative_and_exact_case(tmp_path):
    root = _estate(tmp_path)
    source = _table("Sales.Customer").replace(
        "Description: A declared table.", "Description: $sales.Missing"
    )
    _write(root, "Lakehouse/Raw/Sales__Customer.py", source)
    with pytest.raises(DiscoveryError, match="does not resolve exactly"):
        parse_item_repository(Location(str(root)))


@weaver_test()
def test_files_metadata_reference_uses_its_distinct_namespace(tmp_path):
    root = _estate(tmp_path)
    source = _table("Sales.Customer").replace(
        "Lineage: A source system.", "Lineage: $Files/Sales.Customer"
    )
    _write(root, "Lakehouse/Raw/Sales__Customer.py", source)
    parse_item_repository(Location(str(root)))


@weaver_test()
def test_external_references_are_item_local_and_one_source_may_repeat(tmp_path):
    """Two items may each name the same producer under their own local name."""

    root = _estate(tmp_path)
    for item in ("Warehouse/Reporting", "Warehouse/Audit"):
        _write(
            root,
            f"{item}/shortcuts.yml",
            "logical:\n"
            f"  {item}/Sales.PortableCustomer: Lakehouse/Curated/Sales.Customer\n",
        )
    repository = parse_item_repository(Location(str(root)))
    authored = repository.shortcuts

    assert len(authored) == 2
    assert authored[0].target == authored[1].target
    assert {str(shortcut.destination) for shortcut in authored} == {
        "Warehouse/Reporting/Sales.PortableCustomer",
        "Warehouse/Audit/Sales.PortableCustomer",
    }


@weaver_test()
def test_a_shortcut_destination_must_not_collide_with_a_native_document(tmp_path):
    root = _estate(tmp_path)
    _write(
        root,
        "Warehouse/Reporting/shortcuts.yml",
        "logical:\n"
        "  Warehouse/Reporting/Sales.Customer: Lakehouse/Curated/Sales.Customer\n",
    )
    with pytest.raises(DiscoveryError, match="the repository already declares"):
        parse_item_repository(Location(str(root)))


@weaver_test()
def test_a_logical_shortcut_may_not_name_its_own_item(tmp_path):
    """A bound reference crosses items. Within one, the document graph already
    orders producer before consumer, and the reference stage runs before every
    document the item declares, so it would be planned before its own source."""

    root = _estate(tmp_path)
    _write(
        root,
        "Warehouse/Reporting/shortcuts.yml",
        "logical:\n"
        "  Warehouse/Reporting/Sales.PortableCustomer: "
        "Warehouse/Reporting/Sales.Customer\n",
    )
    with pytest.raises(DiscoveryError, match="which is the item declaring it"):
        parse_item_repository(Location(str(root)))


@weaver_test()
def test_an_external_target_must_resolve_with_exact_case(tmp_path):
    root = _estate(tmp_path)
    _write(
        root,
        "Warehouse/Reporting/shortcuts.yml",
        "logical:\n"
        "  Warehouse/Reporting/Sales.PortableCustomer: "
        "Lakehouse/Curated/sales.Customer\n",
    )
    with pytest.raises(DiscoveryError, match="declared spelling"):
        parse_item_repository(Location(str(root)))


@weaver_test()
def test_an_external_target_rejects_physical_three_part_names(tmp_path):
    root = _estate(tmp_path)
    _write(
        root,
        "Warehouse/Reporting/shortcuts.yml",
        "physical:\n"
        "  Warehouse/Reporting/Sales.PortableCustomer: Curated_LH.Sales.Customer\n",
    )
    with pytest.raises(DiscoveryError, match="must be ItemType/ItemName"):
        parse_item_repository(Location(str(root)))


@weaver_test()
def test_duplicate_external_destination_is_rejected_by_the_yaml_reader(tmp_path):
    root = _estate(tmp_path)
    _write(
        root,
        "Warehouse/Reporting/shortcuts.yml",
        "logical:\n"
        "  Warehouse/Reporting/Sales.PortableCustomer: Lakehouse/Raw/Sales.Customer\n"
        "  Warehouse/Reporting/Sales.PortableCustomer: "
        "Lakehouse/Curated/Sales.Customer\n",
    )
    with pytest.raises(Exception, match="duplicate metadata key"):
        parse_item_repository(Location(str(root)))


@weaver_test()
def test_metadata_reference_may_resolve_through_an_external_destination(tmp_path):
    root = _estate(tmp_path)
    source = _warehouse_table("Sales.Change").replace(
        "Description: A reporting table.",
        "Description: $Sales.PortableCustomer",
    )
    _write(root, "Warehouse/Audit/Sales.Change.sql", source)
    _write(
        root,
        "Warehouse/Audit/shortcuts.yml",
        "logical:\n"
        "  Warehouse/Audit/Sales.PortableCustomer: "
        "Lakehouse/Curated/Sales.Customer\n",
    )
    parse_item_repository(Location(str(root)))


@weaver_test()
def test_item_metadata_reference_cycle_is_a_hard_error(tmp_path):
    root = _estate(tmp_path)
    raw = _table("Sales.Customer").replace(
        "Description: A declared table.",
        "Description: $Lakehouse/Curated/Sales.Customer",
    )
    curated = _table("Sales.Customer").replace(
        "Description: A declared table.",
        "Description: $Lakehouse/Raw/Sales.Customer",
    )
    _write(root, "Lakehouse/Raw/Sales__Customer.py", raw)
    _write(root, "Lakehouse/Curated/Sales__Customer.py", curated)
    with pytest.raises(DiscoveryError, match="metadata reference cycle"):
        parse_item_repository(Location(str(root)))


@weaver_test()
def test_canonical_foreign_key_target_is_validated(tmp_path):
    root = _estate(tmp_path)
    source = _table("Sales.Customer").replace(
        "Schema:\n  Id: string",
        "Foreign keys:\n"
        "  - Id: Lakehouse/Curated/Sales.Customer[Id]\n"
        "Schema:\n  Id: string",
    )
    _write(root, "Lakehouse/Raw/Sales__Customer.py", source)
    parse_item_repository(Location(str(root)))


@weaver_test()
def test_document_local_shortcut_headers_are_rejected(tmp_path):
    root = _estate(tmp_path)
    source = _table("Sales.Customer").replace(
        "Lineage: A source system.",
        "Lineage: A source system.\nWarehouse alias: Sales.CustomerAlias",
    )
    _write(root, "Lakehouse/Raw/Sales__Customer.py", source)
    with pytest.raises(DiscoveryError, match="have been replaced"):
        parse_item_repository(Location(str(root)))
