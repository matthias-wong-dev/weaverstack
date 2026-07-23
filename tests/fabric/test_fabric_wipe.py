"""The wipe module itself, run against a real Fabric Lakehouse.

Not hand-rolled deletion through the store — `weaver.wipe` doing its job, with
the host being the only difference from the local suite.
"""

from __future__ import annotations

import pytest

from weaver import (
    DeltaTarget,
    FabricHost,
    FolderTarget,
    Location,
    wipe,
    wipe_delta_target,
    wipe_folder_target,
    wipe_selection,
)
from weaver.fabric import FabricStore, onelake_url

pytestmark = pytest.mark.fabric


@pytest.fixture
def host(fabric_lakehouses):
    return FabricHost(
        workspace=fabric_lakehouses["workspace"].name,
        weaver_lakehouse=fabric_lakehouses["weaver"].name,
    )


@pytest.fixture
def target(fabric_lakehouses):
    return fabric_lakehouses["target"].name


@pytest.fixture
def populated(fabric_lakehouses, target):
    """Files and a Delta-shaped tree, so a wipe has something real to clear."""

    store = FabricStore()
    root = Location(
        onelake_url(fabric_lakehouses["workspace"].id, fabric_lakehouses["target"].id)
    )
    for relative, body in (
        ("Files/Sales/OrderExport/order_20260723.csv", b"id,amount\n1,10\n"),
        ("Files/Sales/OrderExport/order_20260722.csv", b"id,amount\n2,20\n"),
        ("Files/notes.txt", b"scratch\n"),
        ("Tables/Sales/Order/part-0000.parquet", b"PAR1fake"),
        ("Tables/Sales/Order/_delta_log/00000000000000000000.json", b"{}\n"),
        ("Tables/Reporting/Summary/part-0000.parquet", b"PAR1fake"),
    ):
        store.write(root.join(*relative.split("/")), body)
    return {"store": store, "root": root}


def files_names(populated):
    return {e.location.name for e in populated["store"].list(populated["root"] / "Files")}


def table_names(populated):
    return {e.location.name for e in populated["store"].list(populated["root"] / "Tables")}


# --- folder ------------------------------------------------------------------


def test_wipe_clears_a_fabric_folder_target(host, target, populated):
    report = wipe_folder_target(FolderTarget.parse(f"{target}/Files"), host)
    assert set(report.removed) == {"Sales", "notes.txt"}
    assert files_names(populated) == set()


def test_a_fabric_folder_target_keeps_its_root(host, target, populated):
    wipe_folder_target(FolderTarget.parse(f"{target}/Files"), host)
    assert populated["store"].is_directory(populated["root"] / "Files")


def test_a_fabric_subpath_target_wipes_only_beneath_itself(host, target, populated):
    wipe_folder_target(FolderTarget.parse(f"{target}/Files/Sales"), host)
    assert files_names(populated) == {"Sales", "notes.txt"}
    assert populated["store"].list(populated["root"] / "Files" / "Sales") == []


def test_a_fabric_dry_run_removes_nothing(host, target, populated):
    report = wipe_folder_target(FolderTarget.parse(f"{target}/Files"), host, dry_run=True)
    assert report.dry_run is True
    assert set(report.removed) == {"Sales", "notes.txt"}
    assert files_names(populated) == {"Sales", "notes.txt"}


# --- delta -------------------------------------------------------------------


def test_wipe_clears_fabric_delta_tables(host, target, populated):
    report = wipe_delta_target(DeltaTarget.parse(target), host)
    assert set(report.removed) == {"Sales", "Reporting"}
    assert table_names(populated) == set()


def test_the_delta_log_goes_with_the_table(host, target, populated):
    store, root = populated["store"], populated["root"]
    assert store.exists(root / "Tables" / "Sales" / "Order" / "_delta_log")
    wipe_delta_target(DeltaTarget.parse(target), host)
    assert not store.exists(root / "Tables" / "Sales" / "Order")


def test_wiping_delta_leaves_the_files_alone(host, target, populated):
    wipe_delta_target(DeltaTarget.parse(target), host)
    assert files_names(populated) == {"Sales", "notes.txt"}


# --- composition -------------------------------------------------------------


def test_both_targets_wipe_together(host, target, populated):
    reports = wipe(
        host,
        folder_target=FolderTarget.parse(f"{target}/Files"),
        delta_target=DeltaTarget.parse(target),
    )
    assert [r.target for r in reports] == [f"folder:{target}/Files", f"delta:{target}"]
    assert files_names(populated) == set()
    assert table_names(populated) == set()


def test_an_item_name_clears_the_whole_lakehouse(host, target, populated):
    """The workspace is asked what the item is; a Lakehouse means both areas."""
    reports = wipe_selection([target], host)
    assert len(reports) == 2
    assert files_names(populated) == set()
    assert table_names(populated) == set()


def test_an_unknown_item_is_reported(host):
    from weaver.errors import CommandError

    with pytest.raises(CommandError, match="no item named"):
        wipe_selection(["weavertest_absent"], host)
