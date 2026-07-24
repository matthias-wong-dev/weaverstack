"""The load runtime — a Python payload materialises its object for real.

This exercises ``weaver.build.runtime.materialise`` the way a generated payload
does: with an installation bound, it promotes a Folder's staged files and writes
and registers a Delta table from the object's proposed rows. It proves the
enabling piece the build bundle depends on before the installer is wired up.
"""

from __future__ import annotations

import importlib
import shutil
import sys
from pathlib import Path

import pytest

from weaver import DeltaTarget, FolderTarget, ItemRef
from weaver.build.runtime import Installation, installing

pytestmark = pytest.mark.spark

FIXTURE = Path(__file__).parent.parent / "fixtures" / "build-lakehouse"


@pytest.fixture
def snapshot(tmp_path):
    """A copy of the repository on the import path, cleaned up after."""

    root = tmp_path / "snapshot"
    shutil.copytree(FIXTURE, root)
    sys.path.insert(0, str(root))
    try:
        yield root
    finally:
        sys.path.remove(str(root))
        for name in ("Raw__CustomerCsv", "DWG__Customer"):
            sys.modules.pop(name, None)


def test_materialise_builds_folder_then_delta(spark, lakehouses, snapshot):
    installation = Installation(spark=spark, resolver=lakehouses.resolver, lakehouse=lakehouses.target)

    try:
        with installing(installation):
            folder_cls = importlib.import_module("Raw__CustomerCsv").Raw__CustomerCsv
            from weaver.build.runtime import materialise

            materialise(folder_cls)

            # The Folder's files are promoted to its real destination.
            folder = lakehouses.resolver.folder_object(
                FolderTarget(lakehouse=lakehouses.target), "Raw", "CustomerCsv"
            )
            assert (Path(folder.value) / "customers.csv").exists()

            table_cls = importlib.import_module("DWG__Customer").DWG__Customer
            materialise(table_cls)

        # The Delta table exists at its resolved path with the input rows.
        table_path = lakehouses.resolver.delta_table(
            DeltaTarget(lakehouse=lakehouses.target), "DWG", "Customer"
        ).value
        rows = spark.read.format("delta").load(table_path).collect()
        assert len(rows) == 4
        by_id = {row["CustomerId"]: row for row in rows}
        assert by_id[1]["CustomerName"] == "Ada"
        assert by_id[1]["IsActive"] is True
        assert by_id[2]["IsActive"] is False

        # And it is registered, so its two-part name binds like a Fabric table.
        registered = spark.sql("SELECT count(*) AS n FROM DWG.Customer").collect()[0]["n"]
        assert registered == 4
    finally:
        spark.sql("DROP TABLE IF EXISTS DWG.Customer")
        spark.sql("DROP DATABASE IF EXISTS DWG CASCADE")
