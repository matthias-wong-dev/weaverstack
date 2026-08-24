"""The authoring surface: what a developer writes, and what it reaches.

Fakes rather than a session: every call an authored object makes is an ordinary
one on the Spark object it was handed, so the assertions here are about *which*
call is made with *which* address. That a real session and a real Delta table
answer those calls is proved in ``tests/fabric``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import pytest
from support.catalogues import never
from support.spark import MockSpark
from support.weaver_test import weaver_test
from support.workspaces import mounted_lakehouse

from weaver import Assumption, Folder, Lakehouse, Table, Test, View, WeaverObject
from weaver.errors import LoadError
from weaver.runtime.load_result import LoadResult
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


@weaver_test()
def test_an_object_binds_a_session_and_a_resolved_lakehouse(spark):
    order = Sales__Order(spark, lakehouse=LAKEHOUSE)

    assert order.spark is spark
    assert order.lakehouse is LAKEHOUSE
    assert order.spark_root == "abfss://ws@onelake.dfs.fabric.microsoft.com/lh"


@weaver_test()
def test_the_session_is_mandatory():
    with pytest.raises(LoadError, match="needs the Spark session"):
        Sales__Order(None, lakehouse=LAKEHOUSE)


@weaver_test()
def test_a_lakehouse_name_is_refused(spark):
    """Resolving a name needs a workspace resolver, which authored code has not."""

    with pytest.raises(LoadError, match="not the name 'Sales_LH'"):
        Sales__Order(spark, lakehouse="Sales_LH")


@weaver_test()
def test_an_unresolved_lakehouse_is_refused(spark):
    with pytest.raises(LoadError, match="takes a resolved Lakehouse, got dict"):
        Sales__Order(spark, lakehouse={"name": "Sales_LH"})


@weaver_test()
def test_no_lakehouse_and_no_attachment_fails_rather_than_guessing(spark):
    with pytest.raises(LoadError, match="no Lakehouse is attached"):
        Sales__Order(spark)


@weaver_test()
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


@weaver_test()
def test_a_dependency_inherits_the_session_and_the_lakehouse(spark):
    order = Sales__Order(spark, lakehouse=LAKEHOUSE)

    customer = Sales__Customer(order)

    assert customer.spark is order.spark
    assert customer.lakehouse is order.lakehouse


@weaver_test()
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


@weaver_test()
def test_identity_comes_from_the_class_name(spark):
    order = Sales__Order(spark, lakehouse=LAKEHOUSE)

    assert order.identity == ("Sales", "Order")
    assert order.object_id == "Sales.Order"


@weaver_test()
def test_a_class_that_names_no_object_says_so(spark):
    class Order(Table):
        def read(self):
            return [], []

    with pytest.raises(LoadError, match="does not name an object"):
        Order(spark, lakehouse=LAKEHOUSE).object_id


# --- tables -----------------------------------------------------------------


@weaver_test()
def test_a_table_reads_its_own_delta_files(spark):
    Sales__Order(spark, lakehouse=LAKEHOUSE).dataframe()

    assert spark.read.calls == [
        ("delta", "abfss://ws@onelake.dfs.fabric.microsoft.com/lh/Tables/Sales/Order")
    ]


@weaver_test()
def test_a_dependencys_table_is_read_the_same_way(spark):
    order = Sales__Order(spark, lakehouse=LAKEHOUSE)

    Sales__Customer(order).dataframe()

    assert spark.read.calls == [
        (
            "delta",
            "abfss://ws@onelake.dfs.fabric.microsoft.com/lh/Tables/Sales/Customer",
        )
    ]


@weaver_test()
def test_an_empty_dataframe_is_the_existing_table_with_no_rows(spark):
    empty = Sales__Order(spark, lakehouse=LAKEHOUSE).empty_dataframe()

    assert empty.rows == ()
    assert spark.read.calls == [
        ("delta", "abfss://ws@onelake.dfs.fabric.microsoft.com/lh/Tables/Sales/Order")
    ]


def _customer(returned, *, incremental: bool):
    """One table whose ``read()`` returns whatever a case wants it to."""

    from weaver.declaration.metadata import PYTHON, parse_document

    declared = "\n                Incremental: true\n" if incremental else ""

    class Sales__Customer(Table):
        def _document(self):
            return parse_document(
                f"""
                Table ID: Sales.Customer

                Description: One row per customer.

                Lineage: The sales system.

                Primary key: Customer id
{declared}
                Schema:
                  Customer id: string
                """,
                language=PYTHON,
            )

        def read(self):
            return returned

    return Sales__Customer


def _loaded(spark, monkeypatch, table):
    """One load, with the runtime replaced by what it was handed."""

    import weaver.runtime.table_load as table_load

    seen = {}

    def load_table(*_args, **kwargs):
        seen.update(kwargs)
        return LoadResult(succeeded=True)

    monkeypatch.setattr(table_load, "load_table", load_table)
    result = (
        table(spark, lakehouse=LAKEHOUSE)
        .with_catalogue(never(table.__name__.replace("__", ".")))
        .load()
    )
    return result, seen


@pytest.mark.parametrize("incremental", [False, True])
@weaver_test()
def test_a_table_that_stages_alone_makes_no_delete_claim(
    spark, monkeypatch, incremental
):
    """The ordinary return, and the only one a non-incremental table has."""

    staged = object()

    result, seen = _loaded(
        spark, monkeypatch, _customer(staged, incremental=incremental)
    )

    assert result.succeeded
    assert seen["staging_frame"] is staged
    assert seen["deletes"] is None


@weaver_test()
def test_an_incremental_tables_claim_reaches_the_runtime(spark, monkeypatch):
    staged, claimed = object(), ["Customer id"]

    result, seen = _loaded(
        spark, monkeypatch, _customer((staged, claimed), incremental=True)
    )

    assert result.succeeded
    assert seen["staging_frame"] is staged
    assert seen["deletes"] == claimed


@weaver_test()
def test_a_non_incremental_table_returning_a_tuple_is_refused(spark, monkeypatch):
    """The source is the whole truth, so a second value states it twice.

    Refused on the shape of the return, before anything is asked of Spark: what
    the frame holds is not the question, and reading it to find out would be a
    job run to learn that it was empty.
    """

    import weaver.runtime.table_load as table_load

    def load_table(*_args, **_kwargs):
        raise AssertionError("the load reached the runtime")

    monkeypatch.setattr(table_load, "load_table", load_table)
    table = _customer((object(), []), incremental=False)

    with pytest.raises(LoadError, match="returns staging on its own"):
        table(spark, lakehouse=LAKEHOUSE).with_catalogue(
            never(table.__name__.replace("__", "."))
        ).load()


# --- an incremental source with nothing to do -------------------------------


@weaver_test()
def test_an_incremental_table_returning_none_is_a_successful_no_op(monkeypatch):
    """``return None`` means there is no work, and costs nothing to say.

    Constructed with a session that fails on any use, so this proves no Spark
    call happened rather than asserting against a stand-in that answered one.
    An authored source that already knows its window is empty should not launch
    a job to rediscover that.
    """

    import weaver.runtime.table_load as table_load

    monkeypatch.setattr(
        table_load,
        "load_table",
        lambda *_a, **_k: pytest.fail("the load reached the runtime"),
    )
    table = _customer(None, incremental=True)

    result = (
        table(MockSpark(), lakehouse=LAKEHOUSE)
        .with_catalogue(never("Sales.Customer"))
        .load()
    )

    assert result.succeeded
    assert result == LoadResult(
        succeeded=True, bookmark_datetime=result.bookmark_datetime
    )


@weaver_test()
def test_a_no_op_load_still_advances_the_bookmark(monkeypatch):
    """It read its window and found nothing, which is a clean load of nothing.

    Leaving the bookmark where it was would make the next load read the same
    window again, and go on doing so for as long as the source stayed quiet.
    """

    import weaver.runtime.table_load as table_load

    monkeypatch.setattr(
        table_load, "load_table", lambda *_a, **_k: pytest.fail("reached the runtime")
    )
    table = _customer(None, incremental=True)
    catalogue = never("Sales.Customer")

    result = table(MockSpark(), lakehouse=LAKEHOUSE).with_catalogue(catalogue).load()

    assert result.bookmark_datetime is not None


@weaver_test()
def test_none_and_none_deletes_are_the_same_no_op(monkeypatch):
    """``(None, None)`` is what ``None`` normalises to, so it means the same."""

    import weaver.runtime.table_load as table_load

    monkeypatch.setattr(
        table_load, "load_table", lambda *_a, **_k: pytest.fail("reached the runtime")
    )
    table = _customer((None, None), incremental=True)

    result = (
        table(MockSpark(), lakehouse=LAKEHOUSE)
        .with_catalogue(never("Sales.Customer"))
        .load()
    )

    assert result.succeeded


@weaver_test()
def test_an_incremental_table_may_claim_deletes_without_staging(spark, monkeypatch):
    """Deletion-only work: nothing arrived, and some rows are retired.

    The target's own empty shape stands in for staging, so the reconciliation
    retires exactly what was claimed and inserts nothing.
    """

    claimed = ["Customer id"]

    result, seen = _loaded(
        spark, monkeypatch, _customer((None, claimed), incremental=True)
    )

    assert result.succeeded
    assert seen["deletes"] == claimed
    # `empty_dataframe()` is the target's shape with no rows.
    assert seen["staging_frame"].rows == ()


@weaver_test()
def test_a_non_incremental_table_returning_none_is_refused(monkeypatch):
    """An explicitly empty source retires every row, which is a load.

    So there is nothing ``None`` could be read as, and the author is told what
    to write instead. Refused before Spark is asked anything.
    """

    import weaver.runtime.table_load as table_load

    monkeypatch.setattr(
        table_load, "load_table", lambda *_a, **_k: pytest.fail("reached the runtime")
    )
    table = _customer(None, incremental=False)

    with pytest.raises(LoadError, match="cannot return None"):
        table(MockSpark(), lakehouse=LAKEHOUSE).with_catalogue(
            never("Sales.Customer")
        ).load()


def _export(returned, *, incremental: bool):
    """One folder whose ``read()`` returns whatever a case wants it to."""

    from weaver.declaration.metadata import PYTHON, parse_document

    # Stated either way: a Folder is incremental unless it says otherwise, so a
    # case about the non-incremental contract has to say so.
    declared = "true" if incremental else "false"

    class Raw__Export(Folder):
        def _document(self):
            return parse_document(
                f"""
                Folder ID: Raw.Export

                Description: One file per export.

                Lineage: The sales system.

                File key: "*.json"

                Incremental: {declared}
                """,
                language=PYTHON,
            )

        def read(self):
            return returned

    return Raw__Export


@weaver_test()
def test_an_incremental_folder_returning_none_is_a_successful_no_op(tmp_path):
    """Nothing is staged and nothing is claimed, so nothing is scanned."""

    lakehouse = mounted_lakehouse("Sales_LH", tmp_path)
    folder = _export(None, incremental=True)

    result = (
        folder(MockSpark(), lakehouse=lakehouse)
        .with_catalogue(never("Raw.Export", files=True))
        .load()
    )

    assert result.succeeded
    assert result.rows_read == 0
    assert result.rows_deleted == 0


@weaver_test()
def test_a_non_incremental_folder_returning_none_is_refused(tmp_path):
    """An empty staging folder retires every file, which is a load."""

    lakehouse = mounted_lakehouse("Sales_LH", tmp_path)
    folder = _export(None, incremental=False)

    with pytest.raises(LoadError, match="cannot return None"):
        folder(MockSpark(), lakehouse=lakehouse).with_catalogue(
            never("Raw.Export", files=True)
        ).load()


@weaver_test()
def test_an_incremental_folder_may_claim_deletes_without_staging(tmp_path):
    """Deletion-only work, with the issued staging standing for "nothing new"."""

    lakehouse = mounted_lakehouse("Sales_LH", tmp_path)
    destination = lakehouse.folder_path("Raw", "Export")
    destination.mkdir(parents=True, exist_ok=True)
    (destination / "old.json").write_text("{}", encoding="utf-8")
    folder = _export((None, ["old.json"]), incremental=True)

    result = (
        folder(MockSpark(), lakehouse=lakehouse)
        .with_catalogue(never("Raw.Export", files=True))
        .load()
    )

    assert result.succeeded
    assert result.rows_deleted == 1
    assert not (destination / "old.json").exists()


# --- views ------------------------------------------------------------------


@weaver_test()
def test_a_view_is_read_by_name_because_it_has_no_path(spark):
    Sales__Enriched(spark, lakehouse=LAKEHOUSE).dataframe()

    assert spark.tables == ["`Weaver`.`Sales_LH`.`Sales`.`Enriched`"]


# --- folders ----------------------------------------------------------------


@weaver_test()
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


@weaver_test()
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


@weaver_test()
def test_staging_is_the_folder_path_with_a_staging_suffix(spark, tmp_path):
    export = Sales__OrderExport(
        spark, lakehouse=mounted_lakehouse("Sales_LH", tmp_path)
    )

    assert export._staging_path() == tmp_path / "Files/Sales/OrderExport_Staging"


@weaver_test()
def test_staging_is_available_outside_a_load(spark, tmp_path):

    export = Sales__OrderExport(
        spark, lakehouse=mounted_lakehouse("Sales_LH", tmp_path)
    )

    staging = export.staging_folder()

    assert staging.path.is_dir()
    assert staging.path != export.path()
    export._clear_read_staging()


@weaver_test()
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


@weaver_test()
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
@weaver_test()
def test_the_context_era_surface_is_gone(removed):
    """Removed outright rather than deprecated — pre-alpha, and one API is enough."""

    assert not any(hasattr(base, removed) for base in (WeaverObject, Table, View))


@weaver_test()
def test_the_folder_keeps_only_the_methods_it_documents():
    assert not hasattr(Folder, "folder_path")
    # The Spark-meaning-of-path() era. `path()` is now the filesystem spelling,
    # so a call site that meant the other one must fail rather than silently
    # hand Spark a mount it cannot resolve.
    assert not hasattr(Folder, "local_path")
    assert callable(Folder.path)
    assert callable(Folder.spark_path)
    assert callable(Folder.staging_folder)


@weaver_test()
def test_there_is_no_ambient_resolver_or_context():
    import weaver
    import weaver.objects as objects

    assert not hasattr(objects, "_active_resolver")
    assert not hasattr(objects, "ObjectContext")
    assert not hasattr(weaver, "ObjectContext")


# --- the module stays light -------------------------------------------------


@weaver_test()
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


@weaver_test()
def test_the_repeated_constants_match_the_ones_they_are_repeated_from():
    """The authoring surface pulls in no parser, so it restates a few names."""

    from weaver.declaration.metadata import ASSUMPTION, TEST
    from weaver.declaration.source import python_id_parts
    from weaver.objects import ASSUMPTION as AUTHORED_ASSUMPTION
    from weaver.objects import CLASS_ID_SEPARATOR
    from weaver.objects import STAGING_SUFFIX as AUTHORED_STAGING
    from weaver.objects import TEST as AUTHORED_TEST
    from weaver.runtime.folder_load import STAGING_SUFFIX

    assert (AUTHORED_TEST, AUTHORED_ASSUMPTION) == (TEST, ASSUMPTION)
    assert AUTHORED_STAGING == STAGING_SUFFIX
    assert python_id_parts(f"Sales{CLASS_ID_SEPARATOR}Order") == ["Sales", "Order"]


@weaver_test()
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


@weaver_test()
def test_every_authored_object_shares_one_base():
    assert issubclass(Folder, WeaverObject)
    assert issubclass(Table, WeaverObject)
    assert issubclass(View, WeaverObject)
    assert issubclass(Test, WeaverObject)
    assert issubclass(Assumption, WeaverObject)
