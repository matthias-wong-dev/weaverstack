"""Resolving a destination Lakehouse to the roots Spark writes through.

The execution model these serve is fixed: **the Spark session is attached to the
Weaver Lakehouse.** That is the control plane, and it is why Weaver's own
catalogue is reached as ordinary two-part names in schema ``_``. Destination
Lakehouses are the variable data plane, so they are addressed through explicit
roots and never by pointing the session somewhere else.

That is what allows one invocation to build several Lakehouses. Switching the
current catalogue between targets would make two destinations that share a schema
name indistinguishable; resolving each target's roots separates them by
construction.
"""

from __future__ import annotations

import pytest

from weaver import ItemRef, LocalWorkspace, LocalResolver
from weaver.errors import IdentityError
from weaver.locations import LakehouseSparkLocation


@pytest.fixture
def resolver(tmp_path):
    return LocalResolver(LocalWorkspace(workspace=tmp_path, weaver_lakehouse="Weaver"))


def test_a_target_resolves_to_its_two_roots(resolver, tmp_path):
    location = resolver.lakehouse_spark_location(ItemRef("Sales_LH"))
    root = tmp_path.as_posix()
    assert location.item == "Sales_LH"
    assert location.tables_root == f"{root}/Sales_LH/Tables"
    assert location.files_root == f"{root}/Sales_LH/Files"


def test_a_table_is_addressed_under_the_tables_root(resolver, tmp_path):
    location = resolver.lakehouse_spark_location(ItemRef("Sales_LH"))
    root = tmp_path.as_posix()
    assert location.table_path("Sales", "Customer") == (
        f"{root}/Sales_LH/Tables/Sales/Customer"
    )
    assert location.schema_root("Sales") == f"{root}/Sales_LH/Tables/Sales"


def test_a_folder_is_addressed_under_the_files_root(resolver, tmp_path):
    location = resolver.lakehouse_spark_location(ItemRef("Sales_LH"))
    root = tmp_path.as_posix()
    assert location.folder_path("Sales", "Export") == (
        f"{root}/Sales_LH/Files/Sales/Export"
    )


def test_two_destinations_resolve_separately(resolver):
    """The property the whole abstraction exists for.

    ``Sales`` in Lakehouse A and ``Sales`` in Lakehouse B are different places. A
    build that switched the session's catalogue between them could not tell them
    apart; resolved roots can, and one session can therefore address both.
    """

    first = resolver.lakehouse_spark_location(ItemRef("Lakehouse_A"))
    second = resolver.lakehouse_spark_location(ItemRef("Lakehouse_B"))
    assert first.table_path("Sales", "Customer") != second.table_path("Sales", "Customer")
    assert "Lakehouse_A" in first.table_path("Sales", "Customer")
    assert "Lakehouse_B" in second.table_path("Sales", "Customer")


def test_the_weaver_lakehouse_resolves_like_any_other_item(resolver):
    """It is the attached one, not a special case of resolution.

    Initialisation builds the catalogue *into* the Weaver Lakehouse, so it is a destination
    on that one occasion. Nothing about resolving it differs.
    """

    location = resolver.lakehouse_spark_location(ItemRef("Weaver"))
    assert location.item == "Weaver"
    assert location.tables_root.endswith("/Weaver/Tables")


@pytest.mark.parametrize("bad", ["..", ".", "", "  ", "a/b", "a\\b"])
def test_a_segment_that_could_escape_the_lakehouse_is_refused(resolver, bad):
    """These strings become paths Spark writes through.

    A segment that traversed upward would write outside the Lakehouse it names, so
    it is checked rather than trusted — even though schema and object names are
    already validated upstream.
    """

    location = resolver.lakehouse_spark_location(ItemRef("Sales_LH"))
    with pytest.raises(IdentityError):
        location.table_path(bad, "Customer")
    with pytest.raises(IdentityError):
        location.folder_path("Sales", bad)


def test_roots_are_strings_because_spark_addresses_them_not_the_store():
    """Deliberately not ``Location``.

    On Fabric a Lakehouse has two addresses: the DFS location the store lists
    through, and the ``abfss://`` root Spark reads and writes through. This type
    carries the second. Conflating them would have an inspection listing a URL
    Spark cannot enumerate, or a write going through a transport it cannot use.
    """

    location = LakehouseSparkLocation(
        item="Sales_LH",
        tables_root="abfss://ws@onelake.dfs.fabric.microsoft.com/item/Tables",
        files_root="abfss://ws@onelake.dfs.fabric.microsoft.com/item/Files",
    )
    assert isinstance(location.tables_root, str)
    assert location.table_path("Sales", "Customer").startswith("abfss://")


def test_a_trailing_separator_on_a_root_does_not_double(resolver):
    location = LakehouseSparkLocation(
        item="Sales_LH", tables_root="/root/Tables/", files_root="/root/Files/"
    )
    assert location.table_path("Sales", "Customer") == "/root/Tables/Sales/Customer"
    assert location.folder_path("Sales", "Export") == "/root/Files/Sales/Export"


# --- the installer resolves it once, and executors receive it -----------------


def test_the_installer_resolves_the_destination_for_its_executors(tmp_path):
    """An executor must not derive its own path — that is a planning decision.

    So the installation context carries the destination already resolved, once per
    target.
    """

    from weaver.build_bundle.installer import InstallationEnvironment
    from weaver.build_bundle.targets import BoundTarget

    workspace = LocalWorkspace(workspace=tmp_path, weaver_lakehouse="Weaver")
    environment = InstallationEnvironment(store=None, resolver=LocalResolver(workspace))
    resolved = environment.resolve_target(
        BoundTarget(
            id="lakehouse-Sales_LH",
            kind="lakehouse",
            item_id="Sales_LH",
            item_name="Sales_LH",
        )
    )
    assert resolved.lakehouse == ItemRef("Sales_LH")
    assert resolved.location is not None
    assert resolved.location.tables_root == f"{tmp_path.as_posix()}/Sales_LH/Tables"


def test_a_warehouse_target_has_no_lakehouse_roots(tmp_path):
    from weaver.build_bundle.installer import InstallationEnvironment
    from weaver.build_bundle.targets import BoundTarget

    workspace = LocalWorkspace(workspace=tmp_path, weaver_lakehouse="Weaver")
    environment = InstallationEnvironment(store=None, resolver=LocalResolver(workspace))
    resolved = environment.resolve_target(
        BoundTarget(id="warehouse-Sales_WH", kind="warehouse", item_id="Sales_WH")
    )
    assert resolved.location is None


def test_a_resolver_without_the_method_is_not_a_failure(tmp_path):
    """The actions that need roots fail explicitly; resolution does not pre-empt them."""

    from weaver.build_bundle.installer import InstallationEnvironment
    from weaver.build_bundle.targets import BoundTarget

    class Minimal:
        pass

    environment = InstallationEnvironment(store=None, resolver=Minimal())
    resolved = environment.resolve_target(
        BoundTarget(id="lakehouse-X", kind="lakehouse", item_id="X")
    )
    assert resolved.location is None


# --- a bundle never carries one ----------------------------------------------


def test_a_bound_target_carries_no_resolved_root():
    """Roots are derived at install time, deliberately.

    On Fabric a root embeds workspace and item ids; locally it embeds a temporary
    directory. A bundle whose identity moved with a temporary path would not be
    comparable between environments (build-philosophy §10), and a bundle carrying
    a stale root would install somewhere the caller no longer means.
    """

    from weaver.build_bundle.targets import BoundTarget

    mapping = BoundTarget(
        id="lakehouse-Sales_LH",
        kind="lakehouse",
        item_id="Sales_LH",
        item_name="Sales_LH",
    ).to_mapping()
    assert "tables_root" not in mapping
    assert "files_root" not in mapping
    assert mapping["item_id"] == "Sales_LH"
