"""One resolved Lakehouse — the destination half of an authored object's binding."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import pytest
from support.workspaces import given_resolver, given_workspace, mounted_lakehouse

from weaver import Lakehouse, lakehouse_for
from weaver.errors import LoadError
from weaver.lakehouse import MOUNT_OPTIONS, default_lakehouse
from weaver.spark import FabricSparkTarget
from weaver.targets import DeltaTarget, FolderTarget, ItemRef


@dataclass
class FakeSpark:
    settings: dict = field(default_factory=dict)

    @property
    def conf(self):
        return self

    def get(self, key: str, default=None):
        return self.settings.get(key, default)


# --- one root, both areas ---------------------------------------------------


def test_one_root_carries_both_lakehouse_areas():
    lakehouse = Lakehouse(name="Sales_LH", spark_root="abfss://ws@host/lh")

    assert (
        lakehouse.table_path("Sales", "Order")
        == "abfss://ws@host/lh/Tables/Sales/Order"
    )
    assert lakehouse.location.files_root == "abfss://ws@host/lh/Files"


def test_a_folder_path_is_a_real_path_and_a_spark_path_is_a_string(tmp_path):
    """The distinction the two methods exist to keep.

    Authored folder code globs and opens files, so it is handed something that
    can; Spark cannot use that at all, so it is handed a string it can parse.
    """

    lakehouse = mounted_lakehouse("Sales_LH", tmp_path)

    assert isinstance(lakehouse.folder_path("Sales", "Export"), Path)
    assert isinstance(lakehouse.folder_spark_path("Sales", "Export"), str)


def test_a_trailing_separator_does_not_double_up():
    lakehouse = Lakehouse(name="Sales_LH", spark_root="abfss://ws@host/lh/")

    assert (
        lakehouse.table_path("Sales", "Order")
        == "abfss://ws@host/lh/Tables/Sales/Order"
    )
    assert lakehouse.folder_spark_path("Sales", "Export") == (
        "abfss://ws@host/lh/Files/Sales/Export"
    )


# --- two roots, because two things read them --------------------------------


def test_a_table_is_addressed_by_the_spark_root_in_fabric():
    """Spark reads abfss natively, so a table needs nothing else."""

    lakehouse = Lakehouse(name="Sales_LH", spark_root="abfss://ws@host/lh")

    assert (
        lakehouse.table_path("Sales", "Order")
        == "abfss://ws@host/lh/Tables/Sales/Order"
    )


def test_a_folder_in_onelake_is_addressed_through_a_mount(monkeypatch):
    """A Folder's authored code is ordinary Python, and `open()` cannot read a URL.

    So the same bytes are presented as a filesystem path: Weaver mounts the root
    it resolved — not the attachment — and a write through the mount lands in
    OneLake with nothing copied.
    """

    import weaver.lakehouse as module

    mounted = {}

    class FakeFs:
        def mount(self, source, point, options=None):
            mounted[point] = (source, options)

        def getMountPath(self, point):
            return f"/synfs/notebook/session-1{point}"

    monkeypatch.setattr(
        module, "_notebook_utils", lambda: type("U", (), {"fs": FakeFs()})()
    )
    monkeypatch.setattr(module, "_MOUNTS", {})

    lakehouse = Lakehouse(name="Sales_LH", spark_root="abfss://ws@host/lh")

    # Spark keeps the URL; Python gets the mount. Two spellings of one location,
    # because neither consumer understands the other's.
    assert lakehouse.folder_spark_path("Sales", "Export") == (
        "abfss://ws@host/lh/Files/Sales/Export"
    )
    assert lakehouse.folder_path("Sales", "Export") == Path(
        "/synfs/notebook/session-1/weaver/lh/Files/Sales/Export"
    )
    # Mounted by item id, so a second Lakehouse in the same session cannot
    # silently address the first.
    assert mounted == {"/weaver/lh": ("abfss://ws@host/lh", MOUNT_OPTIONS)}


def test_the_mount_caches_nothing(monkeypatch):
    """The repair for a mount that disagrees with the storage behind it.

    Weaver reaches one Files area two ways, and changes made through the other
    one — a DFS wipe, a shortcut created by REST — have to be visible here
    immediately. With caching on they are not, and the symptom is a listing that
    still holds entries the storage no longer has.
    """

    import weaver.lakehouse as module

    options = {}

    class FakeFs:
        def mount(self, source, point, config=None):
            options.update(config or {})

        def getMountPath(self, point):
            return f"/synfs/notebook/session-1{point}"

    monkeypatch.setattr(
        module, "_notebook_utils", lambda: type("U", (), {"fs": FakeFs()})()
    )
    monkeypatch.setattr(module, "_MOUNTS", {})

    Lakehouse(name="Sales_LH", spark_root="abfss://ws@host/lh").folder_path(
        "Sales", "Export"
    )

    assert options["fileCacheTimeout"] == 0


def test_the_mount_is_made_once_per_session(monkeypatch):
    """Fabric refuses a second mount of the same point, and there is no need."""

    import weaver.lakehouse as module

    calls = []

    class FakeFs:
        def mount(self, source, point, options=None):
            calls.append(point)

        def getMountPath(self, point):
            return f"/synfs/notebook/session-1{point}"

    monkeypatch.setattr(
        module, "_notebook_utils", lambda: type("U", (), {"fs": FakeFs()})()
    )
    monkeypatch.setattr(module, "_MOUNTS", {})

    lakehouse = Lakehouse(name="Sales_LH", spark_root="abfss://ws@host/lh")
    lakehouse.folder_path("Sales", "Export")
    lakehouse.folder_path("Sales", "Other")

    assert calls == ["/weaver/lh"]


def test_a_onelake_folder_outside_fabric_says_why_it_cannot_be_reached(monkeypatch):
    import weaver.lakehouse as module

    monkeypatch.setattr(module, "_notebook_utils", lambda: None)
    monkeypatch.setattr(module, "_MOUNTS", {})

    lakehouse = Lakehouse(name="Sales_LH", spark_root="abfss://ws@host/lh")

    with pytest.raises(LoadError, match="Fabric notebook utilities"):
        lakehouse.folder_path("Sales", "Export")


def test_a_root_must_be_a_real_root():
    with pytest.raises(LoadError, match="must be a non-empty string"):
        Lakehouse(name="Sales_LH", spark_root="  ")


def test_a_root_that_is_not_onelake_is_refused():
    """A Lakehouse is in OneLake, so a directory cannot stand in for one.

    Its Files area is reached through a Fabric mount and its tables through an
    ``abfss://`` URL; a local path would satisfy neither and would fail later,
    somewhere that could not say why.
    """

    with pytest.raises(LoadError, match="abfss://"):
        Lakehouse(name="Sales_LH", spark_root="/srv/lh")


def test_a_path_segment_that_escaped_its_parent_is_refused():
    """The same guard the resolved locations apply — these strings become paths."""

    lakehouse = Lakehouse(name="Sales_LH", spark_root="abfss://ws@host/lh")

    with pytest.raises(Exception, match="path segment"):
        lakehouse.table_path("Sales", "../../etc")


# --- naming -----------------------------------------------------------------


def test_a_lakehouse_with_no_destination_will_not_name_an_object():
    """A bare Schema.Object resolves through whatever is attached — the anti-pattern."""

    lakehouse = Lakehouse(name="Sales_LH", spark_root="abfss://ws@host/lh")

    with pytest.raises(LoadError, match="without a Spark destination"):
        lakehouse.qualify("Sales", "Order")


def test_the_attached_lakehouse_is_the_one_named_two_part():
    spark = FakeSpark(
        settings={"trident.workspace.id": "ws-id", "trident.lakehouse.id": "lh-id"}
    )

    assert default_lakehouse(spark).qualify("Sales", "Order") == "`Sales`.`Order`"


def test_a_supplied_destination_is_what_names_objects():
    lakehouse = Lakehouse(
        name="Sales_LH",
        spark_root="abfss://ws@host/Sales_LH",
        destination=FabricSparkTarget(workspace="Demo", lakehouse="Sales_LH"),
    )

    assert lakehouse.qualify("Sales", "Order") == ("`Demo`.`Sales_LH`.`Sales`.`Order`")


# --- resolved by name, through a resolver -----------------------------------


def test_a_resolver_resolves_a_lakehouse_by_name(tmp_path: Path):
    resolver = given_resolver(workspace=given_workspace(catalogue="Warehouse/Weaver"))

    lakehouse = lakehouse_for(resolver, ItemRef("Sales_LH"))

    assert lakehouse.name == "Sales_LH"
    # Storage is OneLake, keyed by item id; the catalogue is named in full.
    assert lakehouse.spark_root == resolver.spark_root(ItemRef("Sales_LH"))
    assert lakehouse.qualify("Sales", "Order") == ("`Demo`.`Sales_LH`.`Sales`.`Order`")


def test_a_name_is_accepted_as_a_string_there_and_only_there(tmp_path: Path):
    resolver = given_resolver(workspace=given_workspace(catalogue="Warehouse/Weaver"))

    assert lakehouse_for(resolver, "Sales_LH") == lakehouse_for(
        resolver, ItemRef("Sales_LH")
    )


def test_the_resolved_roots_agree_with_the_resolvers_own_arithmetic(tmp_path: Path):
    """One layout, reached by the two transports a Lakehouse has.

    Spark writes through ``abfss://`` and the store lists through the DFS
    ``https://`` endpoint. Both address the same object, and they are not the
    same string — conflating them would have a write going through a transport
    that cannot perform it.
    """

    resolver = given_resolver(workspace=given_workspace(catalogue="Warehouse/Weaver"))
    lakehouse = lakehouse_for(resolver, ItemRef("Sales_LH"))

    assert lakehouse.location == resolver.lakehouse_spark_location(ItemRef("Sales_LH"))

    spark_path = lakehouse.table_path("Sales", "Order")
    store_path = resolver.delta_table(
        DeltaTarget.parse("Sales_LH"), "Sales", "Order"
    ).value

    assert spark_path.startswith("abfss://")
    assert store_path.startswith("https://")
    assert spark_path.endswith("/Tables/Sales/Order")
    assert store_path.endswith("/Tables/Sales/Order")


def test_a_folder_path_agrees_with_the_resolvers_staging_sibling(tmp_path: Path):
    resolver = given_resolver(workspace=given_workspace(catalogue="Warehouse/Weaver"))
    lakehouse = lakehouse_for(resolver, ItemRef("Sales_LH"))
    target = FolderTarget(lakehouse=ItemRef("Sales_LH"))

    # A Folder's files are reached as a filesystem, which outside a Fabric
    # session there is no way to do — so what agrees here is the *address*.
    assert lakehouse.location.folder_path("Sales", "Export").endswith(
        "/Files/Sales/Export"
    )
    assert resolver.folder_object(target, "Sales", "Export").value.endswith(
        "/Files/Sales/Export"
    )
    assert resolver.folder_staging(target, "Sales", "Export").value.endswith(
        "/Files/Sales/Export_Staging"
    )


# --- inferred from the session ----------------------------------------------


def test_the_attached_lakehouse_comes_from_the_sessions_own_settings():
    spark = FakeSpark(
        settings={
            "trident.workspace.id": "ws-id",
            "trident.lakehouse.id": "lh-id",
            "trident.lakehouse.name": "Sales_LH",
        }
    )

    lakehouse = default_lakehouse(spark)

    assert lakehouse.name == "Sales_LH"
    assert (
        lakehouse.spark_root == "abfss://ws-id@onelake.dfs.fabric.microsoft.com/lh-id"
    )


def test_an_unnamed_attachment_falls_back_to_its_id():
    spark = FakeSpark(
        settings={"trident.workspace.id": "ws-id", "trident.lakehouse.id": "lh-id"}
    )

    assert default_lakehouse(spark).name == "lh-id"


def test_the_notebook_runtime_answers_when_the_session_does_not(monkeypatch):
    """A host that carries the context but not the session settings."""

    import sys
    import types

    module = types.ModuleType("notebookutils")
    module.runtime = types.SimpleNamespace(
        context={
            "currentWorkspaceId": "ws-id",
            "defaultLakehouseId": "lh-id",
            "defaultLakehouseName": "Sales_LH",
        }
    )
    monkeypatch.setitem(sys.modules, "notebookutils", module)

    lakehouse = default_lakehouse(FakeSpark())

    assert (
        lakehouse.spark_root == "abfss://ws-id@onelake.dfs.fabric.microsoft.com/lh-id"
    )


def test_no_attachment_fails_immediately():
    with pytest.raises(LoadError, match="no Lakehouse is attached"):
        default_lakehouse(FakeSpark())


def test_an_attachment_with_no_workspace_fails_rather_than_composing_a_root():
    spark = FakeSpark(settings={"trident.lakehouse.id": "lh-id"})

    with pytest.raises(LoadError, match="no workspace"):
        default_lakehouse(spark)


def test_a_session_that_raises_for_unset_settings_is_not_fatal():
    class Strict:
        @property
        def conf(self):
            return self

        def get(self, key, default=None):
            raise KeyError(key)

    with pytest.raises(LoadError, match="no Lakehouse is attached"):
        default_lakehouse(Strict())


# --- the one repeated constant ----------------------------------------------


def test_the_inferred_root_is_spelled_exactly_as_the_fabric_one():
    """Repeated because the core imports without the fabric extra; kept identical here."""

    pytest.importorskip("requests", reason="install the [cli] extra")
    from weaver.fabric.onelake import abfss_root
    from weaver.lakehouse import _ABFSS_ROOT

    assert _ABFSS_ROOT.format(workspace="ws-id", item="lh-id") == abfss_root(
        "ws-id", "lh-id"
    )
