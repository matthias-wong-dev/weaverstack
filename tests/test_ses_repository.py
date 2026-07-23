"""Reading a repository: the structural contract every object must satisfy."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from weaver import Location
from weaver.errors import DiscoveryError, MetadataError
from weaver.ses import PYTHON, SPARK_SQL, SQL, analyse_sql, content_hash, read_repository

FIXTURE = Location(str(Path(__file__).parent / "fixtures" / "sales-etl"))

# Minimal well-formed sources, used to build failure cases one change at a time.
PY_TABLE = '''"""
Table ID: Sales.Order

Description: One row per order.

Lineage: The order export.

Primary key: Order id

Schema:
  Order id: string
"""

from weaver import Table


class Sales__Order(Table):
    def read(self):
        return [], []
'''

SQL_TABLE = """/*
Table ID: Reporting.Order

Description: Orders for reporting.

Lineage: The order table.
*/

select 1 as [Order id]
"""


@pytest.fixture
def repo_dir(tmp_path: Path) -> Path:
    return tmp_path


def write(root: Path, name: str, text: str) -> Location:
    (root / name).write_text(textwrap.dedent(text), encoding="utf-8")
    return Location(str(root))


# --- the example repository -------------------------------------------------


def test_the_example_repository_reads():
    repo = read_repository(FIXTURE)
    assert {document.qualified for document in repo} == {
        "Sales.OrderExport",
        "Sales.Order",
        "Sales.OrderSummary",
        "Reporting.OrderReport",
        "Reporting.OrderView",
    }


def test_every_language_is_represented():
    repo = read_repository(FIXTURE)
    assert repo["Sales.Order"].language == PYTHON
    assert repo["Sales.OrderSummary"].language == SPARK_SQL
    assert repo["Reporting.OrderReport"].language == SQL


def test_subdirectories_are_support_not_objects():
    repo = read_repository(FIXTURE)
    assert repo.support_files == ("_helpers/dates.py",)


def test_object_modules_are_known_by_importable_name():
    """This is what lets an import be read as a dependency."""
    repo = read_repository(FIXTURE)
    assert set(repo.module_names) == {"Sales__Order", "Sales__OrderExport"}


def test_imports_are_captured_for_dependency_analysis():
    repo = read_repository(FIXTURE)
    assert "Sales__OrderExport" in repo["Sales.Order"].imported_modules


def test_relative_imports_are_not_dependencies():
    """`from ._helpers.dates import …` is a helper by construction."""
    repo = read_repository(FIXTURE)
    assert "_helpers" not in repo["Sales.Order"].imported_modules


def test_the_repository_takes_its_name_from_the_root():
    assert read_repository(FIXTURE).name == "sales-etl"


def test_an_unknown_object_says_which_repository():
    repo = read_repository(FIXTURE)
    with pytest.raises(DiscoveryError, match="sales-etl"):
        repo["Sales.Missing"]


# --- filename, ID and class agreement ---------------------------------------


def test_the_filename_must_match_the_declared_id(repo_dir):
    root = write(repo_dir, "Sales__Different.py", PY_TABLE)
    with pytest.raises(DiscoveryError, match="they must agree"):
        read_repository(root)


def test_a_python_file_uses_the_double_underscore(repo_dir):
    root = write(repo_dir, "Sales.Order.py", PY_TABLE)
    with pytest.raises(DiscoveryError, match="cannot contain a dot"):
        read_repository(root)


def test_a_sql_file_uses_the_dot(repo_dir):
    root = write(repo_dir, "Reporting__Order.sql", SQL_TABLE)
    with pytest.raises(DiscoveryError, match="separates schema and object with"):
        read_repository(root)


def test_a_filename_must_name_schema_and_object(repo_dir):
    root = write(repo_dir, "Order.py", PY_TABLE)
    with pytest.raises(DiscoveryError, match="must name Schema and Object"):
        read_repository(root)


def test_the_class_must_carry_the_full_name(repo_dir):
    root = write(repo_dir, "Sales__Order.py", PY_TABLE.replace("class Sales__Order", "class Order"))
    with pytest.raises(DiscoveryError, match="all carry the same name"):
        read_repository(root)


def test_exactly_one_class_per_file(repo_dir):
    root = write(repo_dir, "Sales__Order.py", PY_TABLE + "\n\nclass Helper:\n    pass\n")
    with pytest.raises(DiscoveryError, match="exactly one class"):
        read_repository(root)


def test_a_file_with_no_class_is_refused(repo_dir):
    source = PY_TABLE.split("class ")[0]
    root = write(repo_dir, "Sales__Order.py", source)
    with pytest.raises(DiscoveryError, match="found none"):
        read_repository(root)


# --- the read contract ------------------------------------------------------


def test_read_must_exist(repo_dir):
    source = PY_TABLE.replace("    def read(self):\n        return [], []\n", "    pass\n")
    root = write(repo_dir, "Sales__Order.py", source)
    with pytest.raises(DiscoveryError, match="must implement read"):
        read_repository(root)


def test_read_must_not_be_defined_twice(repo_dir):
    """The later definition silently replaces the earlier one."""
    root = write(repo_dir, "Sales__Order.py", PY_TABLE + "\n    def read(self):\n        return [], []\n")
    with pytest.raises(DiscoveryError, match="silently replaces"):
        read_repository(root)


def test_read_must_not_be_async(repo_dir):
    root = write(repo_dir, "Sales__Order.py", PY_TABLE.replace("def read", "async def read"))
    with pytest.raises(DiscoveryError, match="must not be async"):
        read_repository(root)


def test_the_base_class_must_match_the_declared_kind(repo_dir):
    root = write(repo_dir, "Sales__Order.py", PY_TABLE.replace("(Table)", "(Folder)"))
    with pytest.raises(DiscoveryError, match="must inherit Table"):
        read_repository(root)


def test_a_view_cannot_be_declared_in_python(repo_dir):
    source = '''"""
View ID: Sales.OrderView

Description: A view.

Lineage: The order table.
"""

from weaver import View


class Sales__OrderView(View):
    def read(self):
        return [], []
'''
    root = write(repo_dir, "Sales__OrderView.py", source)
    with pytest.raises(DiscoveryError, match="View is declared in SQL"):
        read_repository(root)


def test_a_folder_cannot_be_declared_in_sql(repo_dir):
    source = """/*
Folder ID: Sales.Export

Description: Files.

Lineage: A drop.

File key: "*.csv"
*/

select 1 as [x]
"""
    root = write(repo_dir, "Sales.Export.sql", source)
    with pytest.raises(DiscoveryError, match="Folder is declared in Python"):
        read_repository(root)


# --- one result set ---------------------------------------------------------


def test_a_single_select_is_one_result_set():
    assert analyse_sql("select 1 as x").result_set_count == 1


def test_intermediate_work_does_not_count():
    body = """
        select * into #recent from Sales.Order where 1 = 1;
        select * from #recent;
    """
    assert analyse_sql(body).result_set_count == 1


def test_a_temp_view_does_not_count():
    body = """
        create or replace temp view recent as select * from Sales.Order;
        select * from recent
    """
    assert analyse_sql(body).result_set_count == 1


def test_two_selects_are_refused(repo_dir):
    root = write(repo_dir, "Reporting.Order.sql", SQL_TABLE + "\n;\nselect 2 as [Other]\n")
    with pytest.raises(DiscoveryError, match="exactly one result set"):
        read_repository(root)


def test_no_select_at_all_is_refused(repo_dir):
    body = SQL_TABLE.replace("select 1 as [Order id]", "create table #x (a int)")
    root = write(repo_dir, "Reporting.Order.sql", body)
    with pytest.raises(DiscoveryError, match="found 0"):
        read_repository(root)


def test_the_check_stands_down_on_dynamic_sql():
    """A wrong rejection blocks a valid object; a miss merely fails at build."""
    analysis = analyse_sql("exec sp_executesql N'select 1'")
    assert analysis.result_set_count is None
    assert "dynamic SQL" in analysis.undetermined_because


def test_dynamic_sql_is_therefore_not_rejected(repo_dir):
    body = SQL_TABLE.replace("select 1 as [Order id]", "exec Reporting.BuildOrders")
    root = write(repo_dir, "Reporting.Order.sql", body)
    assert read_repository(root)["Reporting.Order"].sql_analysis.determined is False


def test_comments_alone_are_not_a_statement():
    assert analyse_sql("-- nothing here\nselect 1").result_set_count == 1


# --- hashing and signature --------------------------------------------------


def test_line_endings_do_not_change_the_hash():
    """A checkout with autocrlf is not a changed file."""
    assert content_hash(b"a\r\nb\r\n") == content_hash(b"a\nb\n")


def test_a_byte_order_mark_does_not_change_the_hash():
    assert content_hash("﻿select 1".encode("utf-8")) == content_hash(b"select 1")


def test_different_content_hashes_differently():
    assert content_hash(b"select 1") != content_hash(b"select 2")


def test_the_signature_is_stable():
    assert read_repository(FIXTURE).signature == read_repository(FIXTURE).signature


def test_the_signature_covers_support_files(repo_dir):
    (repo_dir / "Sales__Order.py").write_text(PY_TABLE, encoding="utf-8")
    (repo_dir / "_helpers").mkdir()
    helper = repo_dir / "_helpers" / "dates.py"
    helper.write_text("x = 1\n", encoding="utf-8")
    before = read_repository(Location(str(repo_dir))).signature
    helper.write_text("x = 2\n", encoding="utf-8")
    assert read_repository(Location(str(repo_dir))).signature != before


# --- repository-level guards ------------------------------------------------


def test_duplicate_object_ids_are_refused(repo_dir):
    (repo_dir / "Sales__Order.py").write_text(PY_TABLE, encoding="utf-8")
    (repo_dir / "Sales.Order.spark.sql").write_text(
        "/*\nTable ID: Sales.Order\n\nDescription: x\n\nLineage: y\n\n"
        "Dependencies:\n  - Sales.Other\n\nSchema:\n  Order id: string\n*/\nselect 1\n",
        encoding="utf-8",
    )
    with pytest.raises(DiscoveryError, match="declared twice"):
        read_repository(Location(str(repo_dir)))


def test_a_helper_may_not_shadow_an_object_module(repo_dir):
    """An import of it would be read as a dependency on that object."""
    (repo_dir / "Sales__Order.py").write_text(PY_TABLE, encoding="utf-8")
    (repo_dir / "_helpers").mkdir()
    (repo_dir / "_helpers" / "Sales__Order.py").write_text("x = 1\n", encoding="utf-8")
    with pytest.raises(DiscoveryError, match="shares the module name"):
        read_repository(Location(str(repo_dir)))


def test_underscore_prefixed_root_files_are_not_objects(repo_dir):
    (repo_dir / "Sales__Order.py").write_text(PY_TABLE, encoding="utf-8")
    (repo_dir / "_scratch.py").write_text("not an object\n", encoding="utf-8")
    repo = read_repository(Location(str(repo_dir)))
    assert len(repo) == 1
    assert "_scratch.py" in repo.support_files


def test_caches_are_ignored_entirely(repo_dir):
    (repo_dir / "Sales__Order.py").write_text(PY_TABLE, encoding="utf-8")
    (repo_dir / "__pycache__").mkdir()
    (repo_dir / "__pycache__" / "Sales__Order.pyc").write_bytes(b"\x00")
    repo = read_repository(Location(str(repo_dir)))
    assert repo.support_files == ()


def test_a_missing_root_is_reported(tmp_path):
    with pytest.raises(DiscoveryError, match="does not exist"):
        read_repository(Location(str(tmp_path / "absent")))
