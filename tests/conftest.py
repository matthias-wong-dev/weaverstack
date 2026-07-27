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
LAKEHOUSE_SQL = Path(__file__).parent / "fixtures" / "local-lakehouse"


def _sql_statements(name: str, tables_root: str) -> tuple[str, ...]:
    """The saved Spark SQL fixture, rendered for one explicit Tables root."""

    raw = (LAKEHOUSE_SQL / name).read_text(encoding="utf-8").format(tables=tables_root)
    code = "\n".join(
        line for line in raw.splitlines() if not line.lstrip().startswith("--")
    )
    return tuple(
        statement
        for statement in (part.strip() for part in code.split(";"))
        if statement
    )


@pytest.fixture
def lakehouse_sql_statements():
    """Shared DDL/DML renderer for local Spark and Fabric Livy fixtures."""

    return _sql_statements


def _populate_folder_files(store, resolver, target: ItemRef) -> None:
    """The file side of the populated-Lakehouse fixture, transport-neutral."""

    from weaver import FolderTarget

    folder_target = FolderTarget(lakehouse=target)
    export = resolver.folder_object(folder_target, "Sales", "OrderExport")
    for day in ("20260721", "20260722", "20260723"):
        store.write(export / f"order_{day}.csv", b"id,amount\n1,10\n2,20\n")

    invoices = resolver.folder_object(folder_target, "Sales", "InvoicePdf")
    store.write(invoices / "INV-001.pdf", b"%PDF-1.4 fake\n")
    store.write(invoices / "archive" / "INV-000.pdf", b"%PDF-1.4 older\n")
    store.write(resolver.files_root(target) / "notes.txt", b"scratch\n")


@pytest.fixture
def populate_folder_files():
    """Shared fixture setup through LocalStore or desktop OneLake access."""

    return _populate_folder_files


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
    store.make_directory(resolver.weaver_items_root)

    return LocalLakehouses(host=host, resolver=resolver, store=store, root=tmp_path)


@pytest.fixture
def installed_repository(lakehouses: LocalLakehouses) -> Location:
    """The example declaration, copied into the Weaver Lakehouse item area."""

    source = Path(__file__).parent / "fixtures" / "sales-etl"
    destination = lakehouses.resolver.weaver_items_root
    shutil.copytree(source, destination.path, dirs_exist_ok=True)
    return destination


@pytest.fixture
def installed_build_repository(lakehouses: LocalLakehouses) -> str:
    """The build-lakehouse fixture, installed under a deliberately different name.

    The installed name is what the build reads, not the fixture directory name,
    so ``MyRepo`` proves the input chooses the installed repository.
    """

    source = Path(__file__).parent / "fixtures" / "build-lakehouse"
    destination = lakehouses.resolver.weaver_items_root
    shutil.copytree(source, destination.path, dirs_exist_ok=True)
    return "MyRepo"


@pytest.fixture
def lakehouse_only_bindings(lakehouses: LocalLakehouses):
    """A Lakehouse binding to the target Lakehouse, no Warehouse."""

    from weaver.build_bundle import LakehouseBinding, TargetBindings

    return TargetBindings(lakehouse=LakehouseBinding(lakehouse=lakehouses.target))


@pytest.fixture
def installation_environment(spark, lakehouses: LocalLakehouses):
    """A local installer environment: shared Spark, local resolver and store."""

    from weaver.build_bundle import InstallationEnvironment

    return InstallationEnvironment(
        store=lakehouses.store, resolver=lakehouses.resolver, spark=spark
    )


# --- spark -------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _release_spark_caches():
    """Let the session forget the Lakehouses each test throws away.

    A combined ``-m spark`` run used to exhaust the driver's default 1 GB heap
    partway through, and the failure looked like anything: a `Py4JJavaError` whose
    own `str()` failed, attributed to whichever test happened to run when the JVM
    gave out. Every file passed alone, so it read as flakiness.

    It is not. Measured with a forced collection before each reading — which
    separates garbage from retention — the live heap climbs by about 5.6 MB per
    test and never comes down. A class histogram says what is being kept: Catalyst
    expression trees of exactly the shape an `ExpressionEncoder` builds, plus the
    bytecode generated for them.

    They belong to Delta. `DeltaLog` caches one instance per table *path*, each
    holding a `Snapshot` whose state is a `Dataset` — and therefore a whole query
    execution and its encoder. Every test builds its tables under a fresh
    `tmp_path`, so every table is a new path, and the cache keeps the snapshot of
    each one alive long after the directory it describes has been deleted. The
    tests are isolated; the session's memory of them was not.

    So the harness releases it. Both caches here are caches — dropping them costs
    a re-read of a transaction log and never changes an answer — and clearing them
    between tests turns unbounded growth into a bounded sawtooth. This is the fix;
    raising the heap would only have moved the ceiling.

    Autouse across the whole suite, and free when no session was started: the
    core tests never begin one, so this is a single attribute check for them.
    """

    yield

    try:
        from pyspark.sql import SparkSession
    except ImportError:  # no [spark] extra installed — nothing to release
        return
    session = SparkSession._instantiatedSession
    if session is None:
        return
    session._jvm.org.apache.spark.sql.delta.DeltaLog.clearCache()
    session.catalog.clearCache()


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
def populated_folders(
    lakehouses: LocalLakehouses, populate_folder_files
) -> LocalLakehouses:
    """Folder materialisations with files in them. Needs no JVM.

    Two managed folders and a stray file beside them, so a wipe has something
    to clear and something to be careless with.
    """

    populate_folder_files(
        lakehouses.store, lakehouses.resolver, lakehouses.target
    )

    return lakehouses


@pytest.fixture
def populated_local_lakehouses(
    spark,
    populated_folders: LocalLakehouses,
    lakehouse_sql_statements,
) -> LocalLakehouses:
    """The local populated lifecycle, driven by the shared saved Spark SQL."""

    tables_root = populated_folders.resolver.tables_root(
        populated_folders.target
    ).value
    for script in ("build.spark.sql", "load.spark.sql"):
        for statement in lakehouse_sql_statements(script, tables_root):
            spark.sql(statement)

    return populated_folders


@pytest.fixture
def populated_lakehouse(
    populated_local_lakehouses: LocalLakehouses,
) -> LocalLakehouses:
    """Backwards-compatible name for the local populated Lakehouse."""

    return populated_local_lakehouses
