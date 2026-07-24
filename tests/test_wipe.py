"""Wiping a target. Folder and Warehouse cases need no JVM."""

from __future__ import annotations

import pytest

from weaver import DeltaTarget, FolderTarget, WarehouseTarget, wipe, wipe_folder_target
from weaver.errors import CommandError
from weaver.sql import SqlExecutionError


def folder_target(name: str = "Sales_LH/Files") -> FolderTarget:
    return FolderTarget.parse(name)


# --- folders -----------------------------------------------------------------


def test_a_wipe_empties_the_folder_target(populated_folders):
    report = wipe_folder_target(folder_target(), populated_folders.host)
    assert report.count == 2  # the Sales schema directory, and the stray file
    assert populated_folders.resolver.files_root(populated_folders.target).path.is_dir()
    assert list(
        populated_folders.resolver.files_root(populated_folders.target).path.iterdir()
    ) == []


def test_the_configured_root_survives_its_own_wipe(populated_folders):
    root = populated_folders.resolver.folder_root(folder_target())
    wipe_folder_target(folder_target(), populated_folders.host)
    assert root.path.is_dir()


def test_a_subpath_target_wipes_only_beneath_itself(populated_folders):
    """A folder target may be a root within Files; a wipe respects that."""
    store, resolver = populated_folders.store, populated_folders.resolver
    narrow = folder_target("Sales_LH/Files/Extracts")
    store.write(resolver.folder_root(narrow) / "landing" / "a.csv", b"x")

    wipe_folder_target(narrow, populated_folders.host)

    assert resolver.folder_root(narrow).path.is_dir()
    assert not (resolver.folder_root(narrow) / "landing").path.exists()
    assert (resolver.files_root(populated_folders.target) / "notes.txt").path.exists()


def test_a_dry_run_reports_without_removing(populated_folders):
    report = wipe_folder_target(folder_target(), populated_folders.host, dry_run=True)
    assert report.dry_run is True
    assert report.count == 2
    assert (
        populated_folders.resolver.files_root(populated_folders.target) / "notes.txt"
    ).path.exists()


def test_wiping_an_empty_target_is_quiet(lakehouses):
    report = wipe_folder_target(folder_target(), lakehouses.host)
    assert report.removed == ()


def test_wiping_a_target_that_was_never_created_is_quiet(lakehouses):
    report = wipe_folder_target(folder_target("Sales_LH/Files/Never"), lakehouses.host)
    assert report.removed == ()


def test_a_wipe_takes_everything_not_only_what_weaver_manages(populated_folders):
    """A wipe clears the target. That is why a CLI must gate it."""
    report = wipe_folder_target(folder_target(), populated_folders.host)
    assert "notes.txt" in report.removed


# --- warehouse ---------------------------------------------------------------


def test_wiping_a_warehouse_executes_the_core_wipe_without_a_store(lakehouses, monkeypatch):
    import importlib

    class Sql:
        scripts = []

        def execute_script(self, script):
            self.scripts.append(script)

    sql = Sql()

    def forbidden_store(_host):
        raise AssertionError("Warehouse-only wipe asked for a Store")

    monkeypatch.setattr(importlib.import_module("weaver.wipe"), "store_for", forbidden_store)
    reports = wipe(
        lakehouses.host,
        sql_target=WarehouseTarget.parse("Reporting_WH"),
        sql=sql,
    )

    assert reports == ()
    assert len(sql.scripts) == 1
    assert "drop table" in sql.scripts[0].lower()


def test_a_warehouse_sql_failure_names_the_selected_warehouse(lakehouses):
    class BrokenSql:
        def execute_script(self, script):
            raise RuntimeError("driver broke")

    with pytest.raises(SqlExecutionError, match="Reporting_WH.*driver broke"):
        wipe(
            lakehouses.host,
            sql_target=WarehouseTarget.parse("Reporting_WH"),
            sql=BrokenSql(),
        )


def test_a_warehouse_wipe_has_no_dry_run_mode(lakehouses):
    with pytest.raises(CommandError, match="does not support dry_run"):
        wipe(
            lakehouses.host,
            sql_target=WarehouseTarget.parse("Reporting_WH"),
            sql=object(),
            dry_run=True,
        )


# --- composition and safety --------------------------------------------------


def test_wipe_needs_at_least_one_target(lakehouses):
    with pytest.raises(CommandError, match="at least one target"):
        wipe(lakehouses.host)


def test_targets_are_independently_optional(populated_folders):
    """Clear the tables and leave downloaded source files alone, or the reverse."""
    reports = wipe(populated_folders.host, folder_target=folder_target())
    assert len(reports) == 1
    assert reports[0].target.startswith("folder:")


def test_a_wipe_refuses_to_reach_outside_the_host_root(lakehouses, tmp_path):
    from weaver.locations import Location
    from weaver.wipe import _guard

    with pytest.raises(CommandError, match="outside the host root"):
        _guard(Location(str(tmp_path.parent / "elsewhere")), Location(str(lakehouses.root)))


def test_the_report_reads_usefully(populated_folders):
    report = wipe_folder_target(folder_target(), populated_folders.host, dry_run=True)
    assert "would remove" in str(report)
    assert "Sales_LH/Files" in str(report)
