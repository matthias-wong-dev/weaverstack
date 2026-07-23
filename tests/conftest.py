"""Shared fixtures for local build and load.

Two costs, measured, and they pull in opposite directions:

===============================  =========
Spark session start               ~1.2 s
first Delta operation (warm-up)   ~4.3 s
later Delta operations            ~0.8 s
**a local Lakehouse skeleton**    **0.2 ms**
===============================  =========

So the Spark session is built once per run and the Lakehouses are built per
test. Only one `SparkSession` may be active in a process in any case, and the
JVM warm-up is not worth paying twice.

Sharing one session across tests is safe here because Weaver addresses Delta by
explicit path rather than through a metastore, so a session carries no state
between tests. Each test gets its own `tmp_path`, and teardown is pytest's.
"""

from __future__ import annotations

import os
import shutil
import sys
from dataclasses import dataclass
from pathlib import Path

import pytest

from weaver import ItemRef, LocalHost, LocalResolver, LocalStore, Location

WEAVER_LAKEHOUSE = "Weaver"
TARGET_LAKEHOUSE = "Sales_LH"


# --- local lakehouses --------------------------------------------------------


@dataclass(frozen=True)
class LocalLakehouses:
    """A local host holding a Weaver Lakehouse and a target Lakehouse."""

    host: LocalHost
    resolver: LocalResolver
    store: LocalStore
    root: Path

    @property
    def weaver(self) -> ItemRef:
        return ItemRef(WEAVER_LAKEHOUSE)

    @property
    def target(self) -> ItemRef:
        return ItemRef(TARGET_LAKEHOUSE)

    def location(self, *parts: str) -> Location:
        return Location(str(self.root)).join(*parts)

    def tree(self) -> list[str]:
        """Every path beneath the root, relative and sorted — for assertions."""

        return sorted(
            str(path.relative_to(self.root))
            for path in self.root.rglob("*")
        )


@pytest.fixture
def lakehouses(tmp_path: Path) -> LocalLakehouses:
    """A Weaver Lakehouse and one target Lakehouse, empty and disposable.

    Both carry the ``Files/`` and ``Tables/`` areas a Fabric Lakehouse presents,
    so the same resolution serves local and Fabric.
    """

    host = LocalHost(root=tmp_path, weaver_lakehouse=WEAVER_LAKEHOUSE)
    store = LocalStore()
    resolver = LocalResolver(host)

    for item in (WEAVER_LAKEHOUSE, TARGET_LAKEHOUSE):
        store.make_directory(resolver.files_root(ItemRef(item)))
        store.make_directory(resolver.tables_root(ItemRef(item)))
    store.make_directory(resolver.repos_root)

    return LocalLakehouses(host=host, resolver=resolver, store=store, root=tmp_path)


@pytest.fixture
def installed_repository(lakehouses: LocalLakehouses) -> Location:
    """The example repository, copied into the Weaver Lakehouse repos area."""

    source = Path(__file__).parent / "fixtures" / "sales-etl"
    destination = lakehouses.resolver.repos_root / source.name
    shutil.copytree(source, destination.path)
    return destination


# --- spark -------------------------------------------------------------------


@pytest.fixture(scope="session")
def spark():
    """One Delta-enabled Spark session for the whole run.

    Session-scoped because the JVM warm-up costs seconds and only one session
    may be active per process. Tests stay isolated through their own
    directories, not through their own session.
    """

    pytest.importorskip("pyspark", reason="install the [spark] extra")
    pytest.importorskip("delta", reason="install the [spark] extra")

    from weaver.diagnostics import SUPPORTED_JAVA, find_java_home

    java_home = find_java_home()
    if java_home is None:
        pytest.skip(
            f"no JDK found — local Spark needs Java {' or '.join(SUPPORTED_JAVA)}. "
            "Run: weaver doctor"
        )
    os.environ["JAVA_HOME"] = java_home

    # The workers must run the same interpreter as the driver, or Spark fails
    # deep inside a task with a version mismatch.
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName("weaverstack-tests")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.databricks.delta.snapshotPartitions", "1")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    try:
        yield session
    finally:
        session.stop()


# --- populated lakehouses ----------------------------------------------------


@pytest.fixture
def populated_folders(lakehouses: LocalLakehouses) -> LocalLakehouses:
    """Folder materialisations with files in them. Needs no JVM.

    Two managed folders and a stray file beside them, so a wipe has something
    to clear and something to be careless with.
    """

    from weaver import FolderTarget

    store = lakehouses.store
    resolver = lakehouses.resolver
    target = FolderTarget.parse(f"{TARGET_LAKEHOUSE}/Files")

    export = resolver.folder_object(target, "Sales", "OrderExport")
    for day in ("20260721", "20260722", "20260723"):
        store.write(export / f"order_{day}.csv", b"id,amount\n1,10\n2,20\n")

    invoices = resolver.folder_object(target, "Sales", "InvoicePdf")
    store.write(invoices / "INV-001.pdf", b"%PDF-1.4 fake\n")
    store.write(invoices / "archive" / "INV-000.pdf", b"%PDF-1.4 older\n")

    # Not a Weaver materialisation — something a person left in the Files area.
    store.write(resolver.files_root(lakehouses.target) / "notes.txt", b"scratch\n")

    return lakehouses


@pytest.fixture
def populated_lakehouse(spark, populated_folders: LocalLakehouses) -> LocalLakehouses:
    """Folders as above, plus real Delta tables with rows. Needs Spark."""

    from weaver import DeltaTarget

    target = DeltaTarget.parse(TARGET_LAKEHOUSE)
    resolver = populated_folders.resolver

    orders = resolver.delta_table(target, "Sales", "Order")
    spark.createDataFrame(
        [("A1", "C1", 10.0), ("A2", "C1", 20.0), ("A3", "C2", 30.0)],
        "Order_id string, Customer_id string, Amount double",
    ).write.format("delta").mode("overwrite").save(orders.value)

    customers = resolver.delta_table(target, "Sales", "Customer")
    spark.createDataFrame(
        [("C1", "Ackland"), ("C2", "Beattie")],
        "Customer_id string, Customer_name string",
    ).write.format("delta").mode("overwrite").save(customers.value)

    summary = resolver.delta_table(target, "Reporting", "OrderSummary")
    spark.createDataFrame(
        [("C1", 2, 30.0), ("C2", 1, 30.0)],
        "Customer_id string, Order_count int, Total_amount double",
    ).write.format("delta").mode("overwrite").save(summary.value)

    return populated_folders
