"""Level-three identities: parsing, normalisation and round-tripping."""

from __future__ import annotations

import pytest
from support.weaver_test import weaver_test

from weaver.errors import IdentityError
from weaver.targets import DeltaTarget, FolderTarget, ItemRef, WarehouseTarget

ROUND_TRIP = [
    (FolderTarget, "Sales/Files"),
    (FolderTarget, "Control/Files"),
    (DeltaTarget, "Sales"),
    (WarehouseTarget, "Reporting"),
    (ItemRef, "Weaver"),
]


@pytest.mark.parametrize(
    "kind,text", ROUND_TRIP, ids=[f"{k.__name__}:{t}" for k, t in ROUND_TRIP]
)
@weaver_test()
def test_parse_then_str_is_identity(kind, text):
    assert str(kind.parse(text)) == text


@pytest.mark.parametrize(
    "kind,text", ROUND_TRIP, ids=[f"{k.__name__}:{t}" for k, t in ROUND_TRIP]
)
@weaver_test()
def test_parsing_is_stable(kind, text):
    assert kind.parse(text) == kind.parse(str(kind.parse(text)))


@weaver_test()
def test_folder_target_names_a_lakehouse_and_its_files_area():
    assert FolderTarget.parse("Sales/Files").lakehouse == ItemRef("Sales")


@weaver_test()
def test_folder_target_refuses_anything_beneath_the_files_area():
    """A folder object lands at Files/<Schema>/<Object>, derived from its identity.

    A configurable root would make that derivation false, so the binding cannot
    offer one, authored code composes this path from identity alone.
    """

    with pytest.raises(IdentityError, match="nothing to configure"):
        FolderTarget.parse("Sales/Files/Extracts")


@weaver_test()
def test_folder_target_requires_the_files_area():
    with pytest.raises(IdentityError, match="Files"):
        FolderTarget.parse("Sales/Tables/Thing")


@weaver_test()
def test_folder_target_requires_more_than_a_lakehouse():
    with pytest.raises(IdentityError, match="folder target"):
        FolderTarget.parse("Sales")


@weaver_test()
def test_delta_target_rejects_an_explicit_tables_area():
    with pytest.raises(IdentityError, match="implicit"):
        DeltaTarget.parse("Sales/Tables")


@weaver_test()
def test_warehouse_target_rejects_a_path():
    with pytest.raises(IdentityError):
        WarehouseTarget.parse("Reporting/dbo")


@weaver_test()
def test_the_same_name_serves_different_slots():
    """Kind comes from the slot, never from the string."""
    assert (
        DeltaTarget.parse("Shared").lakehouse
        == WarehouseTarget.parse("Shared").warehouse
    )


@pytest.mark.parametrize("bad", ["", "   ", "a\\b", "a:b", "a*b", "..", "a|b"])
@weaver_test()
def test_illegal_names_are_rejected(bad):
    with pytest.raises(IdentityError):
        ItemRef.parse(bad)


@weaver_test()
def test_surrounding_whitespace_is_normalised():
    assert ItemRef("  Sales  ").name == "Sales"


@weaver_test()
def test_identities_are_immutable():
    target = DeltaTarget.parse("Sales")
    with pytest.raises(Exception):
        target.lakehouse = ItemRef("Other")


# --- the build target grammar -------------------------------------------------
#
# `Lakehouse/Landing=Lakehouse/Landing_Dev`: the logical item leads, and the
# physical item follows where the caller supplies one. Both sides are typed and
# both types must agree.


@weaver_test()
def test_a_build_target_may_name_the_logical_item_alone():
    """The portable form. Where it deploys is the workspace configuration's answer."""

    from weaver.build_bundle.targets import parse_build_target

    binding = parse_build_target(
        "Lakehouse/Landing",
        workspace=_workspace_with("Lakehouse/Landing", "Landing_Dev"),
    )

    assert str(binding.item) == "Lakehouse/Landing"
    assert binding.target.item.name == "Landing_Dev"


@weaver_test()
def test_a_build_target_may_supply_the_physical_item_itself():
    from weaver.build_bundle.targets import parse_build_target

    binding = parse_build_target("Lakehouse/Landing=Lakehouse/Landing_Dev")

    assert str(binding.item) == "Lakehouse/Landing"
    assert binding.target.item.name == "Landing_Dev"


@weaver_test()
def test_a_warehouse_build_target_reads_the_same_way():
    from weaver.build_bundle.targets import parse_build_target

    binding = parse_build_target("Warehouse/Curated=Warehouse/Curated_Dev")

    assert str(binding.item) == "Warehouse/Curated"
    assert binding.target.item.name == "Curated_Dev"


@weaver_test()
def test_both_routes_to_one_physical_item_produce_one_binding():
    """Configuration and an explicit physical target are two ways to say it once."""

    from weaver.build_bundle.targets import parse_build_target

    configured = parse_build_target(
        "Lakehouse/Landing",
        workspace=_workspace_with("Lakehouse/Landing", "Landing_Dev"),
    )
    explicit = parse_build_target("Lakehouse/Landing=Lakehouse/Landing_Dev")

    assert configured.item == explicit.item
    assert configured.target.item == explicit.target.item
    assert configured.to_bound_target().id == explicit.to_bound_target().id


@weaver_test()
def test_an_explicit_physical_target_needs_no_configured_entry():
    from weaver.build_bundle.targets import parse_build_target

    binding = parse_build_target(
        "Lakehouse/Landing=Lakehouse/Landing_Dev",
        workspace=_workspace_with("Warehouse/Curated", "Curated_Dev"),
    )

    assert binding.target.item.name == "Landing_Dev"


@weaver_test()
def test_the_same_bare_name_under_two_types_is_two_items():
    """`Lakehouse/Sales` and `Warehouse/Sales` are distinct logical items."""

    from weaver.build_bundle.targets import parse_build_target

    lakehouse = parse_build_target("Lakehouse/Sales=Lakehouse/Sales_LH")
    warehouse = parse_build_target("Warehouse/Sales=Warehouse/Sales_WH")

    assert lakehouse.item != warehouse.item


@pytest.mark.parametrize(
    "text",
    [
        "Lakehouse/Landing=Warehouse/Curated_Dev",
        "Warehouse/Curated=Lakehouse/Landing_Dev",
    ],
)
@weaver_test()
def test_the_two_halves_must_be_the_same_kind(text):
    from weaver.build_bundle.targets import parse_build_target
    from weaver.errors import BuildError

    with pytest.raises(BuildError, match="both must be"):
        parse_build_target(text)


@weaver_test()
def test_an_untyped_physical_target_is_refused():
    """The physical half is typed, so a kind mismatch reads as one."""

    from weaver.build_bundle.targets import parse_build_target
    from weaver.errors import BuildError

    with pytest.raises(BuildError, match="Lakehouse/Name or Warehouse/Name"):
        parse_build_target("Lakehouse/Landing=Landing_Dev")


@pytest.mark.parametrize(
    "text",
    ["Lakehouse/Landing=", "=Lakehouse/Landing", "a=b=c", "", "   "],
)
@weaver_test()
def test_malformed_build_targets_are_refused(text):
    from weaver.build_bundle.targets import parse_build_target
    from weaver.errors import BuildError

    with pytest.raises(BuildError):
        parse_build_target(text)


@weaver_test()
def test_an_untyped_logical_item_is_refused():
    from weaver.build_bundle.targets import parse_build_target
    from weaver.errors import BuildError

    with pytest.raises(BuildError, match="logical Weaver item"):
        parse_build_target("Landing")


@weaver_test()
def test_a_logical_item_with_no_configured_target_says_both_ways_to_fix_it():
    from weaver.build_bundle.targets import parse_build_target
    from weaver.errors import ConfigError

    with pytest.raises(ConfigError, match="no physical target"):
        parse_build_target(
            "Lakehouse/Landing",
            workspace=_workspace_with("Lakehouse/Other", "Other_LH"),
        )


def _workspace_with(logical: str, physical: str):
    from weaver.declaration.model import WeaverItemId
    from weaver.workspaces import TargetDeclaration, Workspace

    item = WeaverItemId.parse(logical)
    return Workspace(
        workspace="Demo", targets={item: TargetDeclaration(item, physical)}
    )


# --- the run target grammar ---------------------------------------------------
#
# A load or a test names logical items only. Where each one runs is the
# catalogue's answer, so a run target has no physical half to override it with.


@pytest.mark.parametrize("what", ["load", "test"])
@pytest.mark.parametrize("text", ["Lakehouse/Landing", "Warehouse/Curated"])
@weaver_test()
def test_a_run_target_is_one_logical_item(what, text):
    from weaver.operations.items import parse_run_item

    assert str(parse_run_item(text, what=what)) == text


@pytest.mark.parametrize("what", ["load", "test"])
@weaver_test()
def test_a_run_target_carrying_a_physical_half_is_refused(what):
    from weaver.errors import CommandError
    from weaver.operations.items import parse_run_item

    with pytest.raises(CommandError, match="read from the Weaver catalogue"):
        parse_run_item("Lakehouse/Landing=Lakehouse/Anything", what=what)


@weaver_test()
def test_the_refusal_says_what_to_write_instead():
    from weaver.errors import CommandError
    from weaver.operations.items import parse_run_item

    with pytest.raises(CommandError, match=r"Write Lakehouse/Landing\."):
        parse_run_item("Lakehouse/Landing=Lakehouse/Landing_Dev", what="load")


@pytest.mark.parametrize("text", ["Landing", "Lakehouse", "Lakehouse/A/B", ""])
@weaver_test()
def test_a_malformed_run_target_is_refused(text):
    from weaver.errors import CommandError
    from weaver.operations.items import parse_run_item

    with pytest.raises(CommandError):
        parse_run_item(text, what="load")


@weaver_test()
def test_a_run_needs_at_least_one_target():
    from weaver.errors import CommandError
    from weaver.operations.items import requested_items

    with pytest.raises(CommandError, match="load needs at least one target"):
        requested_items([], what="load")


@weaver_test()
def test_a_repeated_run_target_is_named_once():
    from weaver.operations.items import requested_items

    assert requested_items(
        ["Lakehouse/Landing", "Lakehouse/Landing"], what="load"
    ) == requested_items(["Lakehouse/Landing"], what="load")
