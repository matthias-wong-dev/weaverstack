"""One resolved Lakehouse — the destination half of an authored object's binding."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import pytest

from weaver import (
    DeltaTarget,
    FolderTarget,
    ItemRef,
    Lakehouse,
    LocalResolver,
    LocalWorkspace,
    lakehouse_for,
)
from weaver.errors import LoadError
from weaver.lakehouse import default_lakehouse
from weaver.spark import local_destination


@dataclass
class FakeSpark:
    settings: dict = field(default_factory=dict)

    @property
    def conf(self):
        return self

    def get(self, key: str, default=None):
        return self.settings.get(key, default)


# --- the two transports -----------------------------------------------------


def test_one_root_carries_both_lakehouse_areas():
    lakehouse = Lakehouse(name="Sales_LH", spark_root="abfss://ws@host/lh")

    assert lakehouse.table_path("Sales", "Order") == "abfss://ws@host/lh/Tables/Sales/Order"
    assert lakehouse.location.files_root == "abfss://ws@host/lh/Files"


def test_the_fuse_root_addresses_the_same_layout():
    lakehouse = Lakehouse(
        name="Sales_LH", spark_root="abfss://ws@host/lh", fuse_root="/lakehouse/default"
    )

    assert lakehouse.folder_path("Sales", "Export") == "/lakehouse/default/Files/Sales/Export"


def test_an_unmounted_lakehouse_has_no_filesystem_path():
    lakehouse = Lakehouse(name="Sales_LH", spark_root="abfss://ws@host/lh")

    with pytest.raises(LoadError, match="no FUSE mount"):
        lakehouse.folder_path("Sales", "Export")


def test_a_trailing_separator_does_not_double_up():
    lakehouse = Lakehouse(name="Sales_LH", spark_root="abfss://ws@host/lh/", fuse_root="/mnt/lh/")

    assert lakehouse.table_path("Sales", "Order") == "abfss://ws@host/lh/Tables/Sales/Order"
    assert lakehouse.folder_path("Sales", "Export") == "/mnt/lh/Files/Sales/Export"


def test_a_root_must_be_a_real_root():
    with pytest.raises(LoadError, match="must be a non-empty string"):
        Lakehouse(name="Sales_LH", spark_root="  ")


def test_a_path_segment_that_escaped_its_parent_is_refused():
    """The same guard the resolved locations apply — these strings become paths."""

    lakehouse = Lakehouse(name="Sales_LH", spark_root="/srv/lh")

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
        spark_root="/srv/.local/Sales_LH",
        destination=local_destination(item="Sales_LH", tables_root="/srv/.local/Sales_LH/Tables"),
    )

    assert lakehouse.qualify("Sales", "Order") == "`sales_lh__sales`.`Order`"


# --- resolved by name, through a resolver -----------------------------------


def test_a_resolver_resolves_a_lakehouse_by_name(tmp_path: Path):
    resolver = LocalResolver(LocalWorkspace(workspace=tmp_path, weaver_lakehouse="Weaver"))

    lakehouse = lakehouse_for(resolver, ItemRef("Sales_LH"))

    assert lakehouse.name == "Sales_LH"
    assert lakehouse.spark_root == str(tmp_path / "Sales_LH")
    assert lakehouse.fuse_root == str(tmp_path / "Sales_LH")
    assert lakehouse.qualify("Sales", "Order") == "`sales_lh__sales`.`Order`"


def test_a_name_is_accepted_as_a_string_there_and_only_there(tmp_path: Path):
    resolver = LocalResolver(LocalWorkspace(workspace=tmp_path, weaver_lakehouse="Weaver"))

    assert lakehouse_for(resolver, "Sales_LH") == lakehouse_for(resolver, ItemRef("Sales_LH"))


def test_the_resolved_roots_agree_with_the_resolvers_own_arithmetic(tmp_path: Path):
    """One layout, whichever type is asked — the emulator mirrors OneLake."""

    resolver = LocalResolver(LocalWorkspace(workspace=tmp_path, weaver_lakehouse="Weaver"))
    lakehouse = lakehouse_for(resolver, ItemRef("Sales_LH"))

    assert lakehouse.location == resolver.lakehouse_spark_location(ItemRef("Sales_LH"))
    assert lakehouse.table_path("Sales", "Order") == resolver.delta_table(
        DeltaTarget.parse("Sales_LH"), "Sales", "Order"
    ).value


def test_a_folder_path_agrees_with_the_resolvers_staging_sibling(tmp_path: Path):
    resolver = LocalResolver(LocalWorkspace(workspace=tmp_path, weaver_lakehouse="Weaver"))
    lakehouse = lakehouse_for(resolver, ItemRef("Sales_LH"))
    target = FolderTarget(lakehouse=ItemRef("Sales_LH"))

    assert lakehouse.folder_path("Sales", "Export") == resolver.folder_object(
        target, "Sales", "Export"
    ).value
    assert f"{lakehouse.folder_path('Sales', 'Export')}_Staging" == resolver.folder_staging(
        target, "Sales", "Export"
    ).value


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
    assert lakehouse.spark_root == "abfss://ws-id@onelake.dfs.fabric.microsoft.com/lh-id"
    assert lakehouse.fuse_root == "/lakehouse/default"


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

    assert lakehouse.spark_root == "abfss://ws-id@onelake.dfs.fabric.microsoft.com/lh-id"


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

    assert _ABFSS_ROOT.format(workspace="ws-id", item="lh-id") == abfss_root("ws-id", "lh-id")
