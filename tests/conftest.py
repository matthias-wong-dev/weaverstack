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

import sys as _sys
from pathlib import Path as _Path

# The narrow fixture constructors are shared by every layer — pure Python,
# local Spark and Fabric all build their inputs the same way — so they are
# importable from anywhere in the suite rather than copied per directory.
_sys.path.insert(0, str(_Path(__file__).parent / "targeted"))


from weaver.targets import ItemRef
from weaver.workspaces import LocalWorkspace
from weaver.resolution import LocalResolver
from weaver.store import FilesystemStore
from weaver.locations import Location

WEAVER_LAKEHOUSE = "Weaver"
TARGET_LAKEHOUSE = "Sales_LH"
#: The module-scoped pair, named apart so it can coexist with the per-test one.
SHARED_WEAVER_LAKEHOUSE = "Weaver_Shared"
SHARED_TARGET_LAKEHOUSE = "Sales_Shared_LH"
LAKEHOUSE_SQL = Path(__file__).parent / "fixtures" / "local-lakehouse"


def pytest_collection_modifyitems(items):
    """Make Fabric place and Weaver position a collection-time invariant."""

    errors = []
    fabric_root = Path(__file__).parent / "fabric"
    for item in items:
        path = Path(str(item.path))
        if path.parent != fabric_root or not path.name.startswith("test_"):
            continue

        marks = {mark.name for mark in item.iter_markers()}
        positions = marks & {"remote", "hosted"}
        if "fabric" not in marks:
            errors.append(f"{item.nodeid}: missing fabric marker")
        if len(positions) != 1:
            errors.append(
                f"{item.nodeid}: expected exactly one Weaver position "
                f"(remote or hosted), got {sorted(positions)}"
            )

    if errors:
        raise pytest.UsageError("invalid Fabric test markers:\n" + "\n".join(errors))


@pytest.fixture(autouse=True)
def no_credentials_outside_fabric(request, monkeypatch):
    """Nothing but a Fabric test may ask for a real credential.

    ``DefaultAzureCredential`` is a network call that, on a build agent with no
    identity, hangs and then fails — and the test it fails is whichever one
    happened to construct a Fabric-shaped Session, which says nothing about the
    cause. It is not enough to mock it in the tests that reach it today: a
    ``Resource`` binds its acquisition when the scope is *constructed*, so a
    patch applied to a scope afterwards leaves the original in place and the
    call happens anyway. That is exactly how this escaped once.

    So the default is refusal, and a Fabric test opts out by carrying the
    marker that says it needs a workspace.
    """

    if request.node.get_closest_marker("fabric"):
        return

    def refuse():
        raise AssertionError(
            "a test outside `-m fabric` asked for an Azure credential. Replace "
            "`weaver.fabric.auth.credential` before the Session is constructed "
            "— a Resource binds its acquisition at construction, so patching "
            "the scope afterwards is too late."
        )

    monkeypatch.setattr("weaver.fabric.auth.credential", refuse)


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

    from weaver.targets import FolderTarget

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
    """Shared fixture setup through FilesystemStore or desktop OneLake access."""

    return _populate_folder_files


# --- local lakehouses --------------------------------------------------------


@dataclass(frozen=True)
class LocalLakehouses:
    """A local workspace holding a Weaver Lakehouse and a target Lakehouse."""

    workspace: LocalWorkspace
    resolver: LocalResolver
    store: FilesystemStore
    root: Path
    weaver_name: str = WEAVER_LAKEHOUSE
    target_name: str = TARGET_LAKEHOUSE

    @property
    def weaver(self) -> ItemRef:
        return ItemRef(self.weaver_name)

    @property
    def target(self) -> ItemRef:
        return ItemRef(self.target_name)

    def location(self, *parts: str) -> Location:
        return Location(str(self.root)).join(*parts)

    def tree(self) -> list[str]:
        """Every path beneath the root, relative and sorted — for assertions."""

        return sorted(
            str(path.relative_to(self.root))
            for path in self.root.rglob("*")
        )


def _lakehouses(root: Path, *, weaver: str, target: str) -> LocalLakehouses:
    workspace = LocalWorkspace(workspace=root, weaver_lakehouse=weaver)
    store = FilesystemStore()
    resolver = LocalResolver(workspace)

    for item in (weaver, target):
        store.make_directory(resolver.files_root(ItemRef(item)))
        store.make_directory(resolver.tables_root(ItemRef(item)))
    store.make_directory(resolver.weaver_items_root)

    return LocalLakehouses(
        workspace=workspace,
        resolver=resolver,
        store=store,
        root=root,
        weaver_name=weaver,
        target_name=target,
    )


@pytest.fixture
def lakehouses(tmp_path: Path) -> LocalLakehouses:
    """A Weaver Lakehouse and one target Lakehouse, empty and disposable.

    Both carry the ``Files/`` and ``Tables/`` areas a Fabric Lakehouse presents,
    so the same resolution serves local and Fabric.

    Per test, so a module whose claims *mutate* an estate gets a clean one each
    time. A module whose claims only read should take
    :func:`shared_lakehouses` instead — see there for why.
    """

    return _lakehouses(tmp_path, weaver=WEAVER_LAKEHOUSE, target=TARGET_LAKEHOUSE)


@pytest.fixture(scope="module")
def shared_lakehouses(tmp_path_factory) -> LocalLakehouses:
    """The same pair of Lakehouses for a whole module.

    Building and loading an estate costs seconds; asking it a question costs
    milliseconds. A module that asks twenty read-only questions of one estate
    and rebuilds it twenty times is paying for isolation it does not use, and
    the arithmetic is not close — in this suite that pattern was a quarter of
    the whole Spark run.

    So the rule is the other way round from the usual instinct: share the
    estate, and take a fresh one only where *isolation itself* is the claim —
    where a test mutates the estate, or where what a build leaves behind is the
    thing being asserted.
    """

    # Named apart from the per-test pair on purpose. A local Lakehouse folds to
    # a Spark schema by name, and one session holds one namespace — so a module
    # mixing a shared estate with a fresh one would have the two writing into
    # each other. Distinct names are what make the two fixtures composable.
    return _lakehouses(
        tmp_path_factory.mktemp("estate"),
        weaver=SHARED_WEAVER_LAKEHOUSE,
        target=SHARED_TARGET_LAKEHOUSE,
    )


@pytest.fixture
def installed_repository(lakehouses: LocalLakehouses) -> Location:
    """The example declaration, copied into the Weaver Lakehouse item area."""

    source = Path(__file__).parent / "fixtures" / "build-lakehouse-item"
    destination = lakehouses.resolver.weaver_items_root
    shutil.copytree(source, destination.path, dirs_exist_ok=True)
    return destination






@pytest.fixture
def installer_session(spark, lakehouses: LocalLakehouses):
    """A Session around the shared Spark, local resolver and store.

    Given all three rather than acquiring any, so the suite's one JVM is reused
    and nothing here closes what the fixture above it owns.
    """

    from weaver.session import ConsoleSession

    with ConsoleSession(
        workspace=lakehouses.workspace,
        spark=spark,
        store=lakehouses.store,
        resolver=lakehouses.resolver,
    ) as session:
        yield session


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
