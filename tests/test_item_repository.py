"""Pure-Python, end-to-end tests for item-oriented static discovery."""

from __future__ import annotations

from pathlib import Path

import pytest

from weaver.errors import DiscoveryError
from weaver.locations import Location
from weaver.ses import read_weaver_repository
from weaver.ses.model import WeaverDocumentId, WeaverSchemaId


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


def _warehouse_table(object_id: str) -> str:
    return f'''\
/*
Table ID: {object_id}
Description: A reporting table.
Lineage: A source system.
Primary key: Id
*/
select cast(1 as varchar(20)) as Id;
'''


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
    _write(root, "alias.yml", "aliases: {}\n")
    _write(root, "_ignore/broken/__init__.py", "this is not python\n")
    _write(root, "_ignore/unfinished.py", "not valid\n")
    return root


def test_reads_multiple_items_and_owned_documents_without_execution(tmp_path):
    root = _estate(tmp_path)
    repository = read_weaver_repository(Location(str(root)))

    assert repository.name == "Estate"
    assert tuple(str(item.identity) for item in repository.items) == (
        "Lakehouse/Curated",
        "Lakehouse/Raw",
        "Warehouse/Audit",
        "Warehouse/Reporting",
    )
    assert WeaverSchemaId.parse("Lakehouse/Raw/Sales") in repository.schema_documents
    table = WeaverDocumentId.parse("Lakehouse/Raw/Sales.Customer")
    folder = WeaverDocumentId.parse("Lakehouse/Raw/Files/Sales.Customer")
    assert table in repository.source_documents
    assert folder in repository.source_documents
    assert repository.source_documents[table].node_id == str(table)
    assert "Lakehouse/Raw/lib/csv_helpers.py" in repository.support_files


def test_ignore_is_completely_absent_including_from_signature(tmp_path):
    root = _estate(tmp_path)
    before = read_weaver_repository(Location(str(root)))
    _write(root, "_ignore/new/broken.py", "different invalid content\n")
    after = read_weaver_repository(Location(str(root)))

    assert before.signature == after.signature
    assert all("_ignore" not in path for path in after.support_files)


def test_ordinary_content_changes_the_signature(tmp_path):
    root = _estate(tmp_path)
    before = read_weaver_repository(Location(str(root))).signature
    _write(root, "Lakehouse/Raw/lib/csv_helpers.py", "def rows():\n    return [1]\n")
    assert read_weaver_repository(Location(str(root))).signature != before


def test_user_authored_init_is_rejected_outside_ignore(tmp_path):
    root = _estate(tmp_path)
    _write(root, "Lakehouse/Raw/lib/__init__.py", "")
    with pytest.raises(DiscoveryError, match="user-authored __init__.py"):
        read_weaver_repository(Location(str(root)))


def test_other_underscore_directory_is_not_ignored(tmp_path):
    root = _estate(tmp_path)
    _write(root, "Lakehouse/Raw/_draft/note.txt", "parked in the wrong place")
    with pytest.raises(DiscoveryError, match="only schemas/.*lib/.*Files/"):
        read_weaver_repository(Location(str(root)))


def test_empty_other_underscore_directory_is_still_discovered(tmp_path):
    root = _estate(tmp_path)
    (root / "Lakehouse/Raw/_draft").mkdir(parents=True)
    with pytest.raises(DiscoveryError, match="only schemas/.*lib/.*Files/"):
        read_weaver_repository(Location(str(root)))


def test_schema_must_be_declared_by_the_owning_item(tmp_path):
    root = _estate(tmp_path)
    _write(root, "Lakehouse/Raw/Other__Thing.py", _table("Other.Thing"))
    with pytest.raises(DiscoveryError, match="not declared by item Lakehouse/Raw"):
        read_weaver_repository(Location(str(root)))


def test_item_type_is_exact(tmp_path):
    root = _estate(tmp_path)
    # Use a distinct spelling so this remains meaningful on case-insensitive
    # developer filesystems; casing itself is covered by the identity tests.
    _write(root, "Inventory/Wrong/schemas/Sales.yml", _schema("Sales"))
    with pytest.raises(DiscoveryError, match="first directory must be exactly"):
        read_weaver_repository(Location(str(root)))


def test_canonical_metadata_reference_resolves_across_items(tmp_path):
    root = _estate(tmp_path)
    source = _table("Sales.Customer").replace(
        "Description: A declared table.",
        "Description: $Lakehouse/Curated/Sales.Customer",
    )
    _write(root, "Lakehouse/Raw/Sales__Customer.py", source)

    repository = read_weaver_repository(Location(str(root)))
    assert repository.source_documents[
        WeaverDocumentId.parse("Lakehouse/Raw/Sales.Customer")
    ].document.description.is_reference


def test_short_metadata_reference_is_item_relative_and_exact_case(tmp_path):
    root = _estate(tmp_path)
    source = _table("Sales.Customer").replace(
        "Description: A declared table.", "Description: $sales.Missing"
    )
    _write(root, "Lakehouse/Raw/Sales__Customer.py", source)
    with pytest.raises(DiscoveryError, match="does not resolve exactly"):
        read_weaver_repository(Location(str(root)))


def test_files_metadata_reference_uses_its_distinct_namespace(tmp_path):
    root = _estate(tmp_path)
    source = _table("Sales.Customer").replace(
        "Lineage: A source system.", "Lineage: $Files/Sales.Customer"
    )
    _write(root, "Lakehouse/Raw/Sales__Customer.py", source)
    read_weaver_repository(Location(str(root)))


def test_aliases_are_destination_keyed_and_one_source_may_repeat(tmp_path):
    root = _estate(tmp_path)
    _write(
        root,
        "alias.yml",
        """aliases:
  Warehouse/Reporting/Sales.PortableCustomer: Lakehouse/Curated/Sales.Customer
  Warehouse/Audit/Sales.PortableCustomer: Lakehouse/Curated/Sales.Customer
""",
    )
    repository = read_weaver_repository(Location(str(root)))

    assert len(repository.aliases) == 2
    assert repository.aliases[0].source == repository.aliases[1].source


def test_alias_destination_must_not_collide_with_a_native_document(tmp_path):
    root = _estate(tmp_path)
    _write(
        root,
        "alias.yml",
        """aliases:
  Warehouse/Reporting/Sales.Customer: Lakehouse/Curated/Sales.Customer
""",
    )
    with pytest.raises(DiscoveryError, match="collides with native document"):
        read_weaver_repository(Location(str(root)))


def test_alias_source_must_resolve_with_exact_case(tmp_path):
    root = _estate(tmp_path)
    _write(
        root,
        "alias.yml",
        """aliases:
  Warehouse/Reporting/Sales.PortableCustomer: Lakehouse/Curated/sales.Customer
""",
    )
    with pytest.raises(DiscoveryError, match="declared spelling"):
        read_weaver_repository(Location(str(root)))


def test_alias_rejects_physical_three_part_names(tmp_path):
    root = _estate(tmp_path)
    _write(
        root,
        "alias.yml",
        """aliases:
  Warehouse/Reporting/Sales.PortableCustomer: Curated_LH.Sales.Customer
""",
    )
    with pytest.raises(DiscoveryError, match="document identity must be"):
        read_weaver_repository(Location(str(root)))


def test_duplicate_alias_destination_is_rejected_by_yaml_reader(tmp_path):
    root = _estate(tmp_path)
    _write(
        root,
        "alias.yml",
        """aliases:
  Warehouse/Reporting/Sales.PortableCustomer: Lakehouse/Raw/Sales.Customer
  Warehouse/Reporting/Sales.PortableCustomer: Lakehouse/Curated/Sales.Customer
""",
    )
    with pytest.raises(Exception, match="duplicate metadata key"):
        read_weaver_repository(Location(str(root)))


def test_metadata_reference_may_resolve_through_alias_destination(tmp_path):
    root = _estate(tmp_path)
    source = _warehouse_table("Sales.Change").replace(
        "Description: A reporting table.",
        "Description: $Sales.PortableCustomer",
    )
    _write(root, "Warehouse/Audit/Sales.Change.sql", source)
    _write(
        root,
        "alias.yml",
        """aliases:
  Warehouse/Audit/Sales.PortableCustomer: Lakehouse/Curated/Sales.Customer
""",
    )
    read_weaver_repository(Location(str(root)))


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
        read_weaver_repository(Location(str(root)))


def test_canonical_foreign_key_target_is_validated(tmp_path):
    root = _estate(tmp_path)
    source = _table("Sales.Customer").replace(
        "Schema:\n  Id: string",
        "Foreign keys:\n"
        "  - Id: Lakehouse/Curated/Sales.Customer[Id]\n"
        "Schema:\n  Id: string",
    )
    _write(root, "Lakehouse/Raw/Sales__Customer.py", source)
    read_weaver_repository(Location(str(root)))


def test_document_local_alias_headers_are_rejected_in_new_layout(tmp_path):
    root = _estate(tmp_path)
    source = _table("Sales.Customer").replace(
        "Lineage: A source system.",
        "Lineage: A source system.\nWarehouse alias: Sales.CustomerAlias",
    )
    _write(root, "Lakehouse/Raw/Sales__Customer.py", source)
    with pytest.raises(DiscoveryError, match="replaced by repository-level alias.yml"):
        read_weaver_repository(Location(str(root)))
