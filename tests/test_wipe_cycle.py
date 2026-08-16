"""Wiping a target. Folder and Warehouse cases need no JVM."""

from __future__ import annotations

import pytest
from support.weaver_test import weaver_test

from weaver import wipe as public_wipe
from weaver.errors import CommandError
from weaver.fabric.resources import ItemNotFoundError
from weaver.locations import Location
from weaver.operations.wipe import (
    WipeReport as PublicWipeReport,
)
from weaver.operations.wipe import (
    WipeTarget,
)
from weaver.physical_wipe import wipe, wipe_folder_target
from weaver.sql import SqlExecutionError
from weaver.targets import FolderTarget, WarehouseTarget


def folder_target(name: str = "Sales_LH/Files") -> FolderTarget:
    return FolderTarget.parse(name)


# --- folders -----------------------------------------------------------------


@weaver_test()
def test_a_wipe_empties_the_folder_target(populated_folders):
    report = wipe_folder_target(
        folder_target(),
        populated_folders.workspace,
        store=populated_folders.store,
        session=populated_folders.session,
    )
    assert report.count == 2  # the Sales schema directory, and the stray file
    assert populated_folders.resolver.files_root(populated_folders.target).path.is_dir()
    assert (
        list(
            populated_folders.resolver.files_root(
                populated_folders.target
            ).path.iterdir()
        )
        == []
    )


@weaver_test()
def test_the_configured_root_survives_its_own_wipe(populated_folders):
    root = populated_folders.resolver.folder_root(folder_target())
    wipe_folder_target(
        folder_target(),
        populated_folders.workspace,
        store=populated_folders.store,
        session=populated_folders.session,
    )
    assert root.path.is_dir()


@weaver_test()
def test_a_dry_run_reports_without_removing(populated_folders):
    report = wipe_folder_target(
        folder_target(),
        populated_folders.workspace,
        store=populated_folders.store,
        session=populated_folders.session,
        dry_run=True,
    )
    assert report.dry_run is True
    assert report.count == 2
    assert (
        populated_folders.resolver.files_root(populated_folders.target) / "notes.txt"
    ).path.exists()


@weaver_test()
def test_wiping_an_empty_target_is_quiet(lakehouses):
    report = wipe_folder_target(
        folder_target(), lakehouses.workspace, session=lakehouses.session
    )
    assert report.removed == ()


@weaver_test()
def test_wiping_a_lakehouse_that_does_not_exist_says_so(lakehouses):
    """Absent is not empty.

    A store that answered an unknown name with an empty directory made a wipe of
    something that was never created looked quiet. Fabric resolves an item or
    does not, and a destructive command aimed at a name nothing answers to
    should say that rather than report success over nothing.
    """

    with pytest.raises(ItemNotFoundError, match="Never_LH"):
        wipe_folder_target(
            folder_target("Never_LH/Files"),
            lakehouses.workspace,
            session=lakehouses.session,
        )


@weaver_test()
def test_a_wipe_takes_everything_not_only_what_weaver_manages(populated_folders):
    """A wipe clears the target. That is why a CLI must gate it."""
    report = wipe_folder_target(
        folder_target(),
        populated_folders.workspace,
        store=populated_folders.store,
        session=populated_folders.session,
    )
    assert "notes.txt" in report.removed


# --- warehouse ---------------------------------------------------------------


@weaver_test()
def test_wiping_a_warehouse_executes_the_core_wipe_without_a_store(
    lakehouses, monkeypatch
):
    import importlib

    class Sql:
        scripts = []

        def execute_script(self, script):
            self.scripts.append(script)

    sql = Sql()

    def forbidden_store(_workspace):
        raise AssertionError("Warehouse-only wipe asked for a Store")

    monkeypatch.setattr(
        importlib.import_module("weaver.physical_wipe"), "store_for", forbidden_store
    )
    reports = wipe(
        lakehouses.workspace,
        store=lakehouses.store,
        session=lakehouses.session,
        sql_target=WarehouseTarget.parse("Reporting_WH"),
        sql=sql,
    )

    assert reports == ()
    assert len(sql.scripts) == 1
    assert "drop table" in sql.scripts[0].lower()


@weaver_test()
def test_a_warehouse_sql_failure_names_the_selected_warehouse(lakehouses):
    class BrokenSql:
        def execute_script(self, script):
            raise RuntimeError("driver broke")

    with pytest.raises(SqlExecutionError, match="Reporting_WH.*driver broke"):
        wipe(
            lakehouses.workspace,
            store=lakehouses.store,
            session=lakehouses.session,
            sql_target=WarehouseTarget.parse("Reporting_WH"),
            sql=BrokenSql(),
        )


@weaver_test()
def test_a_warehouse_wipe_has_no_dry_run_mode(lakehouses):
    with pytest.raises(CommandError, match="does not support dry_run"):
        wipe(
            lakehouses.workspace,
            store=lakehouses.store,
            session=lakehouses.session,
            sql_target=WarehouseTarget.parse("Reporting_WH"),
            sql=object(),
            dry_run=True,
        )


# --- composition and safety --------------------------------------------------


@weaver_test()
def test_wipe_needs_at_least_one_target(lakehouses):
    with pytest.raises(CommandError, match="at least one target"):
        wipe(lakehouses.workspace, store=lakehouses.store, session=lakehouses.session)


@weaver_test()
def test_targets_are_independently_optional(populated_folders):
    """Clear the tables and leave downloaded source files alone, or the reverse."""
    reports = wipe(
        populated_folders.workspace,
        store=populated_folders.store,
        session=populated_folders.session,
        folder_target=folder_target(),
    )
    assert len(reports) == 1
    assert reports[0].target.startswith("folder:")


@weaver_test()
def test_a_wipe_refuses_to_reach_outside_the_workspace_root(lakehouses, tmp_path):
    from weaver.locations import Location
    from weaver.physical_wipe import _guard

    with pytest.raises(CommandError, match="outside the workspace root"):
        _guard(
            Location(str(tmp_path.parent / "elsewhere")), Location(str(lakehouses.root))
        )


@weaver_test()
def test_the_report_reads_usefully(populated_folders):
    report = wipe_folder_target(
        folder_target(),
        populated_folders.workspace,
        store=populated_folders.store,
        session=populated_folders.session,
        dry_run=True,
    )
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
@weaver_test()
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
@weaver_test()
def test_public_wipe_rejects_partial_lakehouse_targets(value):
    with pytest.raises(CommandError, match="whole physical item"):
        WipeTarget.parse(value)


@weaver_test()
def test_public_physical_wipe_does_not_require_unbind(monkeypatch):
    operations = __import__("weaver.operations.wipe", fromlist=["wipe"])
    None
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

    result = public_wipe("Lakehouse/Sales", workspace="Demo")
    assert result.count == 1
    assert result.unbound is None


@weaver_test()
def test_public_wipe_uses_configured_control_catalogue_and_skips_it_when_wiped(
    monkeypatch,
):
    operations = __import__("weaver.operations.wipe", fromlist=["wipe"])

    calls = []
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
        lambda control, targets, **_kwargs: (
            calls.append((control.catalogue, tuple(map(str, targets))))
            or {"targets": []}
        ),
    )

    public_wipe("Lakehouse/Sales", workspace="Demo", catalogue="Warehouse/Control")
    public_wipe("Warehouse/Control", workspace="Demo", catalogue="Warehouse/Control")
    assert calls == [("Warehouse/Control", ("Lakehouse/Sales",))]
