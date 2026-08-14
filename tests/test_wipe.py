"""Wiping a target. Folder and Warehouse cases need no JVM."""

from __future__ import annotations

import pytest

from weaver.targets import DeltaTarget, FolderTarget, WarehouseTarget
from weaver.locations import Location
from weaver import wipe as public_wipe
from weaver.operations import WipeReport as PublicWipeReport, WipeTarget
from weaver.physical_wipe import wipe, wipe_folder_target
from weaver.errors import CommandError
from weaver.sql import SqlExecutionError
from support.workspaces import given_resolver, given_workspace


def folder_target(name: str = "Sales_LH/Files") -> FolderTarget:
    return FolderTarget.parse(name)


# --- folders -----------------------------------------------------------------


def test_a_wipe_empties_the_folder_target(populated_folders):
    report = wipe_folder_target(folder_target(), populated_folders.workspace)
    assert report.count == 2  # the Sales schema directory, and the stray file
    assert populated_folders.resolver.files_root(populated_folders.target).path.is_dir()
    assert list(
        populated_folders.resolver.files_root(populated_folders.target).path.iterdir()
    ) == []


def test_the_configured_root_survives_its_own_wipe(populated_folders):
    root = populated_folders.resolver.folder_root(folder_target())
    wipe_folder_target(folder_target(), populated_folders.workspace)
    assert root.path.is_dir()


def test_a_dry_run_reports_without_removing(populated_folders):
    report = wipe_folder_target(folder_target(), populated_folders.workspace, dry_run=True)
    assert report.dry_run is True
    assert report.count == 2
    assert (
        populated_folders.resolver.files_root(populated_folders.target) / "notes.txt"
    ).path.exists()


def test_wiping_an_empty_target_is_quiet(lakehouses):
    report = wipe_folder_target(folder_target(), lakehouses.workspace)
    assert report.removed == ()


def test_wiping_a_target_that_was_never_created_is_quiet(lakehouses):
    report = wipe_folder_target(folder_target("Never_LH/Files"), lakehouses.workspace)
    assert report.removed == ()


def test_a_wipe_takes_everything_not_only_what_weaver_manages(populated_folders):
    """A wipe clears the target. That is why a CLI must gate it."""
    report = wipe_folder_target(folder_target(), populated_folders.workspace)
    assert "notes.txt" in report.removed


# --- warehouse ---------------------------------------------------------------


def test_wiping_a_warehouse_executes_the_core_wipe_without_a_store(lakehouses, monkeypatch):
    import importlib

    class Sql:
        scripts = []

        def execute_script(self, script):
            self.scripts.append(script)

    sql = Sql()

    def forbidden_store(_workspace):
        raise AssertionError("Warehouse-only wipe asked for a Store")

    monkeypatch.setattr(importlib.import_module("weaver.physical_wipe"), "store_for", forbidden_store)
    reports = wipe(
        lakehouses.workspace,
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
            lakehouses.workspace,
            sql_target=WarehouseTarget.parse("Reporting_WH"),
            sql=BrokenSql(),
        )


def test_a_warehouse_wipe_has_no_dry_run_mode(lakehouses):
    with pytest.raises(CommandError, match="does not support dry_run"):
        wipe(
            lakehouses.workspace,
            sql_target=WarehouseTarget.parse("Reporting_WH"),
            sql=object(),
            dry_run=True,
        )


# --- composition and safety --------------------------------------------------


def test_wipe_needs_at_least_one_target(lakehouses):
    with pytest.raises(CommandError, match="at least one target"):
        wipe(lakehouses.workspace)


def test_targets_are_independently_optional(populated_folders):
    """Clear the tables and leave downloaded source files alone, or the reverse."""
    reports = wipe(populated_folders.workspace, folder_target=folder_target())
    assert len(reports) == 1
    assert reports[0].target.startswith("folder:")


def test_a_wipe_refuses_to_reach_outside_the_workspace_root(lakehouses, tmp_path):
    from weaver.locations import Location
    from weaver.physical_wipe import _guard

    with pytest.raises(CommandError, match="outside the workspace root"):
        _guard(Location(str(tmp_path.parent / "elsewhere")), Location(str(lakehouses.root)))


def test_the_report_reads_usefully(populated_folders):
    report = wipe_folder_target(folder_target(), populated_folders.workspace, dry_run=True)
    assert "would remove" in str(report)
    assert "Sales_LH/Files" in str(report)


# --- public operation --------------------------------------------------------


@pytest.mark.parametrize(
    ("value", "item_type"),
    [
        ("Lakehouse/Sales", "Lakehouse"),
        ("Warehouse/Reporting", "Warehouse"),
    ],
)
def test_public_wipe_uses_one_typed_target_grammar(value, item_type):
    target = WipeTarget.parse(value)
    assert target.item_type == item_type
    assert str(target) == value


@pytest.mark.parametrize(
    "value",
    [
        "Lakehouse/Sales/Files",
        "Lakehouse/Sales/Tables",
    ],
)
def test_public_wipe_rejects_partial_lakehouse_targets(value):
    with pytest.raises(CommandError, match="whole physical item"):
        WipeTarget.parse(value)


def test_public_physical_wipe_does_not_require_unbind(monkeypatch):
    operations = __import__("weaver.operations", fromlist=["operations"])
    workspace = given_workspace()
    monkeypatch.setattr(
        operations, "_drop_local_catalogue", lambda *_args, **_kwargs: None
    )
    monkeypatch.setattr(
        operations,
        "_wipe_one",
        lambda target, *_args, **kwargs: (
            PublicWipeReport(
                str(target), Location("/tmp/local/Sales"), ("table",), kwargs["dry_run"]
            ),
        ),
    )
    monkeypatch.setattr(
        operations,
        "_unbind_physical_targets",
        lambda *_args, **_kwargs: pytest.fail("unbind was not requested"),
    )

    result = public_wipe("Lakehouse/Sales", workspace=workspace)
    assert result.count == 1
    assert result.unbound is None


def test_public_wipe_uses_configured_control_catalogue_and_skips_it_when_wiped(
    monkeypatch,
):
    operations = __import__("weaver.operations", fromlist=["operations"])
    workspace = given_workspace(weaver_lakehouse="Control")
    calls = []
    monkeypatch.setattr(
        operations, "_drop_local_catalogue", lambda *_args, **_kwargs: None
    )
    monkeypatch.setattr(
        operations,
        "_wipe_one",
        lambda target, *_args, **kwargs: (
            PublicWipeReport(
                str(target), Location("/tmp/local/target"), (), kwargs["dry_run"]
            ),
        ),
    )
    monkeypatch.setattr(
        operations,
        "_unbind_physical_targets",
        lambda control, targets, **_kwargs: calls.append(
            (control.weaver_lakehouse, tuple(map(str, targets)))
        )
        or {"targets": []},
    )

    public_wipe("Lakehouse/Sales", workspace=workspace)
    public_wipe("Lakehouse/Control", workspace=workspace)
    assert calls == [("Control", ("Lakehouse/Sales",))]
