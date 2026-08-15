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
from support.workspaces import given_resolver, given_workspace

from weaver.errors import IdentityError
from weaver.locations import LakehouseSparkLocation
from weaver.targets import ItemRef


@pytest.fixture
def resolver(tmp_path):
    return given_resolver(
        workspace=given_workspace(catalogue="Lakehouse/Weaver"),
        lakehouses=("Weaver", "Lakehouse_A", "Lakehouse_B", "Sales_LH"),
    )


def _spark_root(resolver, name: str) -> str:
    """What Spark writes through for one Lakehouse: OneLake, keyed by ids.

    Display names address the *catalogue*; storage is addressed by id, so the
    two roots are composed from what resolution answered rather than from the
    name the caller typed.
    """

    return resolver.spark_root(ItemRef(name))


def test_a_target_resolves_to_its_two_roots(resolver):
    location = resolver.lakehouse_spark_location(ItemRef("Sales_LH"))
    root = _spark_root(resolver, "Sales_LH")

    assert location.item == "Sales_LH"
    assert root.startswith("abfss://")
    assert location.tables_root == f"{root}/Tables"
    assert location.files_root == f"{root}/Files"


def test_a_table_is_addressed_under_the_tables_root(resolver):
    location = resolver.lakehouse_spark_location(ItemRef("Sales_LH"))
    root = _spark_root(resolver, "Sales_LH")
    assert location.table_path("Sales", "Customer") == (f"{root}/Tables/Sales/Customer")
    assert location.schema_root("Sales") == f"{root}/Tables/Sales"


def test_a_folder_is_addressed_under_the_files_root(resolver):
    location = resolver.lakehouse_spark_location(ItemRef("Sales_LH"))
    root = _spark_root(resolver, "Sales_LH")
    assert location.folder_path("Sales", "Export") == f"{root}/Files/Sales/Export"


def test_two_destinations_resolve_separately(resolver):
    """The property the whole abstraction exists for.

    ``Sales`` in Lakehouse A and ``Sales`` in Lakehouse B are different places. A
    build that switched the session's catalogue between them could not tell them
    apart; resolved roots can, and one session can therefore address both.
    """

    first = resolver.lakehouse_spark_location(ItemRef("Lakehouse_A"))
    second = resolver.lakehouse_spark_location(ItemRef("Lakehouse_B"))
    assert first.table_path("Sales", "Customer") != second.table_path(
        "Sales", "Customer"
    )
    # Storage is keyed by item id rather than display name, so what tells the
    # two apart is what resolution answered for each.
    assert first.table_path("Sales", "Customer").startswith(
        _spark_root(resolver, "Lakehouse_A")
    )
    assert second.table_path("Sales", "Customer").startswith(
        _spark_root(resolver, "Lakehouse_B")
    )


def test_the_catalogue_resolves_like_any_other_item(resolver):
    """It is the attached one, not a special case of resolution.

    Initialisation builds the catalogue *into* the Weaver Lakehouse, so it is a destination
    on that one occasion. Nothing about resolving it differs.
    """

    location = resolver.lakehouse_spark_location(ItemRef("Weaver"))
    assert location.item == "Weaver"
    assert location.tables_root == f"{_spark_root(resolver, 'Weaver')}/Tables"


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

    from support.sessions import given_installer

    from weaver.build_bundle.targets import BoundTarget

    workspace = given_workspace(catalogue="Lakehouse/Weaver")
    resolver = given_resolver(
        workspace=workspace,
        lakehouses=("Weaver", "Lakehouse_A", "Lakehouse_B", "Sales_LH"),
    )
    installer = given_installer(workspace=workspace, resolver=resolver)
    resolved = installer.resolve_target(
        BoundTarget(
            id="lakehouse-Sales_LH",
            kind="lakehouse",
            item_id="Sales_LH",
            item_name="Sales_LH",
        )
    )
    assert resolved.lakehouse == ItemRef("Sales_LH")
    assert resolved.location is not None
    assert resolved.location.tables_root == (
        f"{resolver.spark_root(ItemRef('Sales_LH'))}/Tables"
    )


def test_a_warehouse_target_has_no_lakehouse_roots(tmp_path):
    from support.sessions import given_installer

    from weaver.build_bundle.targets import BoundTarget

    workspace = given_workspace(catalogue="Lakehouse/Weaver")
    resolver = given_resolver(
        workspace=workspace,
        lakehouses=("Weaver", "Lakehouse_A", "Lakehouse_B", "Sales_LH"),
    )
    installer = given_installer(workspace=workspace, resolver=resolver)
    resolved = installer.resolve_target(
        BoundTarget(id="warehouse-Sales_WH", kind="warehouse", item_id="Sales_WH")
    )
    assert resolved.location is None


def test_a_resolver_without_the_method_is_not_a_failure(tmp_path):
    """The actions that need roots fail explicitly; resolution does not pre-empt them."""

    from support.sessions import given_installer

    from weaver.build_bundle.targets import BoundTarget

    class Minimal:
        pass

    installer = given_installer(resolver=Minimal())
    resolved = installer.resolve_target(
        BoundTarget(id="lakehouse-X", kind="lakehouse", item_id="X")
    )
    assert resolved.location is None


# --- a bundle never carries one ----------------------------------------------


def test_a_bound_target_carries_no_resolved_root():
    """Roots are derived at install time, deliberately.

    On Fabric a root embeds workspace and item ids; locally it embeds a temporary
    directory. A bundle whose identity moved with a temporary path would not be
    comparable between environments (how-does-build-work §15), and a bundle carrying
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
