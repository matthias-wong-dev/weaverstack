"""The authoring surface: what a developer writes, and what it reaches.

Fakes rather than a session: every call an authored object makes is an ordinary
one on the Spark object it was handed, so the assertions here are about *which*
call is made with *which* address. That a real session and a real Delta table
answer those calls is proved under ``pytest -m spark`` in
``tests/spark/test_authored_objects.py``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import pytest
from support.workspaces import mounted_lakehouse

from weaver import Assumption, Folder, Lakehouse, Table, Test, View, WeaverObject
from weaver.errors import LoadError
from weaver.spark import FabricSparkTarget


@dataclass
class FakeFrame:
    """Just enough DataFrame for ``empty_dataframe`` to be observable."""

    rows: tuple = ((1,), (2,))

    def limit(self, count: int) -> "FakeFrame":
        return FakeFrame(rows=self.rows[:count])


@dataclass
class FakeReader:
    calls: list = field(default_factory=list)
    fmt: str | None = None

    def format(self, fmt: str) -> "FakeReader":
        return FakeReader(calls=self.calls, fmt=fmt)

    def load(self, path: str) -> FakeFrame:
        self.calls.append((self.fmt, path))
        return FakeFrame()


@dataclass
class FakeSpark:
    """A session that records what it was asked for."""

    settings: dict = field(default_factory=dict)
    read: FakeReader = field(default_factory=FakeReader)
    tables: list = field(default_factory=list)

    def table(self, name: str) -> str:
        self.tables.append(name)
        return f"rows of {name}"

    @property
    def conf(self):
        return self

    def get(self, key: str, default=None):
        return self.settings.get(key, default)


LAKEHOUSE = Lakehouse(
    name="Sales_LH",
    spark_root="abfss://ws@onelake.dfs.fabric.microsoft.com/lh",
    destination=FabricSparkTarget(workspace="Weaver", lakehouse="Sales_LH"),
)


class Sales__Order(Table):
    def read(self):
        return [], []


class Sales__Customer(Table):
    def read(self):
        return [], []


class Sales__OrderExport(Folder):
    def read(self):
        return self.staging_folder(), []


class Sales__Enriched(View):
    pass


@pytest.fixture
def spark() -> FakeSpark:
    return FakeSpark()


# --- construction -----------------------------------------------------------


def test_an_object_binds_a_session_and_a_resolved_lakehouse(spark):
    order = Sales__Order(spark, lakehouse=LAKEHOUSE)

    assert order.spark is spark
    assert order.lakehouse is LAKEHOUSE
    assert order.spark_root == "abfss://ws@onelake.dfs.fabric.microsoft.com/lh"


def test_the_session_is_mandatory():
    with pytest.raises(LoadError, match="needs the Spark session"):
        Sales__Order(None, lakehouse=LAKEHOUSE)


def test_a_lakehouse_name_is_refused(spark):
    """Resolving a name needs a workspace resolver, which authored code has not."""

    with pytest.raises(LoadError, match="not the name 'Sales_LH'"):
        Sales__Order(spark, lakehouse="Sales_LH")


def test_an_unresolved_lakehouse_is_refused(spark):
    with pytest.raises(LoadError, match="takes a resolved Lakehouse, got dict"):
        Sales__Order(spark, lakehouse={"name": "Sales_LH"})


def test_no_lakehouse_and_no_attachment_fails_rather_than_guessing(spark):
    with pytest.raises(LoadError, match="no Lakehouse is attached"):
        Sales__Order(spark)


def test_the_notebook_case_infers_the_attached_lakehouse():
    spark = FakeSpark(
        settings={
            "trident.workspace.id": "ws-id",
            "trident.lakehouse.id": "lh-id",
            "trident.lakehouse.name": "Sales_LH",
        }
    )

    order = Sales__Order(spark)

    assert order.lakehouse.name == "Sales_LH"
    assert order.spark_root == "abfss://ws-id@onelake.dfs.fabric.microsoft.com/lh-id"


# --- depending on another object --------------------------------------------


def test_a_dependency_inherits_the_session_and_the_lakehouse(spark):
    order = Sales__Order(spark, lakehouse=LAKEHOUSE)

    customer = Sales__Customer(order)

    assert customer.spark is order.spark
    assert customer.lakehouse is order.lakehouse


def test_a_dependency_resolves_against_the_callers_environment(spark):
    """Same class, two destinations — whichever the dependent was given."""

    other = Lakehouse(
        name="Sales_Prod",
        spark_root="abfss://ws@onelake.dfs.fabric.microsoft.com/prod",
    )

    Sales__Customer(Sales__Order(spark, lakehouse=LAKEHOUSE)).dataframe()
    Sales__Customer(Sales__Order(spark, lakehouse=other)).dataframe()

    assert [path for _, path in spark.read.calls] == [
        "abfss://ws@onelake.dfs.fabric.microsoft.com/lh/Tables/Sales/Customer",
        "abfss://ws@onelake.dfs.fabric.microsoft.com/prod/Tables/Sales/Customer",
    ]


# --- identity ---------------------------------------------------------------


def test_identity_comes_from_the_class_name(spark):
    order = Sales__Order(spark, lakehouse=LAKEHOUSE)

    assert order.identity == ("Sales", "Order")
    assert order.object_id == "Sales.Order"


def test_a_class_that_names_no_object_says_so(spark):
    class Order(Table):
        def read(self):
            return [], []

    with pytest.raises(LoadError, match="does not name an object"):
        Order(spark, lakehouse=LAKEHOUSE).object_id


# --- tables -----------------------------------------------------------------


def test_a_table_reads_its_own_delta_files(spark):
    Sales__Order(spark, lakehouse=LAKEHOUSE).dataframe()

    assert spark.read.calls == [
        ("delta", "abfss://ws@onelake.dfs.fabric.microsoft.com/lh/Tables/Sales/Order")
    ]


def test_a_dependencys_table_is_read_the_same_way(spark):
    order = Sales__Order(spark, lakehouse=LAKEHOUSE)

    Sales__Customer(order).dataframe()

    assert spark.read.calls == [
        (
            "delta",
            "abfss://ws@onelake.dfs.fabric.microsoft.com/lh/Tables/Sales/Customer",
        )
    ]


def test_an_empty_dataframe_is_the_existing_table_with_no_rows(spark):
    empty = Sales__Order(spark, lakehouse=LAKEHOUSE).empty_dataframe()

    assert empty.rows == ()
    assert spark.read.calls == [
        ("delta", "abfss://ws@onelake.dfs.fabric.microsoft.com/lh/Tables/Sales/Order")
    ]


# --- views ------------------------------------------------------------------


def test_a_view_is_read_by_name_because_it_has_no_path(spark):
    Sales__Enriched(spark, lakehouse=LAKEHOUSE).dataframe()

    assert spark.tables == ["`Weaver`.`Sales_LH`.`Sales`.`Enriched`"]


# --- folders ----------------------------------------------------------------


def test_a_folder_is_addressed_as_a_filesystem_path(spark, tmp_path):
    """A Folder's authored code writes ordinary files, so it needs a real path.

    A mount of the root Weaver resolved, so authored code that globs and opens
    files addresses OneLake through ordinary Python.
    """

    export = Sales__OrderExport(
        spark, lakehouse=mounted_lakehouse("Sales_LH", tmp_path)
    )

    assert export.path() == tmp_path / "Files/Sales/OrderExport"
    assert isinstance(export.path(), Path)


def test_a_folder_hands_spark_a_string_and_python_a_path(spark, tmp_path):
    """Neither consumer can use the other's spelling, so there are two methods."""

    export = Sales__OrderExport(
        spark, lakehouse=mounted_lakehouse("Sales_LH", tmp_path)
    )

    assert isinstance(export.path(), Path)
    assert isinstance(export.spark_path(), str)
    # Two spellings of one folder, and they are not interchangeable: Python
    # opens files under the mount, Spark reads them at the OneLake address.
    assert export.path() == tmp_path / "Files/Sales/OrderExport"
    assert export.spark_path().startswith("abfss://")
    assert export.spark_path().endswith("/Files/Sales/OrderExport")


def test_staging_is_the_folder_path_with_a_staging_suffix(spark, tmp_path):
    export = Sales__OrderExport(
        spark, lakehouse=mounted_lakehouse("Sales_LH", tmp_path)
    )

    assert export._staging_path() == tmp_path / "Files/Sales/OrderExport_Staging"


def test_staging_is_issued_by_a_load_and_asking_outside_one_says_so(spark, tmp_path):
    """There is nothing to hand back before a load has reset one.

    Answering anyway would name a directory nobody emptied, which is exactly the
    state a load must never publish from.
    """

    export = Sales__OrderExport(
        spark, lakehouse=mounted_lakehouse("Sales_LH", tmp_path)
    )

    with pytest.raises(LoadError, match="only available while a load is running"):
        export.staging_folder()


def test_a_detached_lakehouse_is_reached_exactly_like_an_attached_one(
    spark, monkeypatch
):
    """Weaver mounts the root it resolved, never the notebook's attachment.

    That is what keeps a detached orchestrator able to load a Lakehouse nobody
    attached — the property `/lakehouse/default` could never have provided.
    """

    import weaver.lakehouse as module

    class FakeFs:
        def mount(self, source, point, options=None):
            pass

        def getMountPath(self, point):
            return f"/synfs/notebook/session-1{point}"

    monkeypatch.setattr(
        module, "_notebook_utils", lambda: type("U", (), {"fs": FakeFs()})()
    )
    monkeypatch.setattr(module, "_MOUNTS", {})

    export = Sales__OrderExport(
        spark, lakehouse=Lakehouse(name="Other", spark_root="abfss://ws@host/other")
    )

    assert export.spark_path() == "abfss://ws@host/other/Files/Sales/OrderExport"
    assert export.path() == Path(
        "/synfs/notebook/session-1/weaver/other/Files/Sales/OrderExport"
    )


# --- the surface is only what is documented ---------------------------------


def test_read_must_be_implemented(spark):
    class Sales__Unfinished(Table):
        pass

    with pytest.raises(NotImplementedError, match="must implement read"):
        Sales__Unfinished(spark, lakehouse=LAKEHOUSE).read()


@pytest.mark.parametrize(
    "removed",
    [
        "current_dataframe",
        "empty_frame",
        "folder_path",
        "context",
        "fuse_root",
        "schema",
        "primary_key",
        "is_incremental",
    ],
)
def test_the_context_era_surface_is_gone(removed):
    """Removed outright rather than deprecated — pre-alpha, and one API is enough."""

    assert not any(hasattr(base, removed) for base in (WeaverObject, Table, View))


def test_the_folder_keeps_only_the_methods_it_documents():
    assert not hasattr(Folder, "folder_path")
    # The Spark-meaning-of-path() era. `path()` is now the filesystem spelling,
    # so a call site that meant the other one must fail rather than silently
    # hand Spark a mount it cannot resolve.
    assert not hasattr(Folder, "local_path")
    assert callable(Folder.path)
    assert callable(Folder.spark_path)
    assert callable(Folder.staging_folder)


def test_there_is_no_ambient_resolver_or_context():
    import weaver
    import weaver.objects as objects

    assert not hasattr(objects, "_active_resolver")
    assert not hasattr(objects, "ObjectContext")
    assert not hasattr(weaver, "ObjectContext")


# --- the module stays light -------------------------------------------------


def test_the_authoring_module_imports_without_spark():
    import subprocess
    import sys

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys, weaver.objects; print('pyspark' in sys.modules)",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.stdout.strip() == "False"


def test_the_base_classes_are_registered_by_kind():
    from weaver.objects import BASE_CLASS_NAMES, BASE_CLASSES

    assert BASE_CLASSES == {
        "Folder": Folder,
        "Table": Table,
        "View": View,
        "Test": Test,
        "Assumption": Assumption,
    }
    assert BASE_CLASS_NAMES == {"Folder", "Table", "View", "Test", "Assumption"}


def test_every_authored_object_shares_one_base():
    assert issubclass(Folder, WeaverObject)
    assert issubclass(Table, WeaverObject)
    assert issubclass(View, WeaverObject)
    assert issubclass(Test, WeaverObject)
    assert issubclass(Assumption, WeaverObject)
