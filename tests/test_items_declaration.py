"""The two grammars a caller writes a logical Weaver item in.

A build names an item and may name the physical target it installs into. A load
or a test names an item alone: the catalogue says where it is installed, and a
run cannot be sent anywhere else.
"""

from __future__ import annotations

import pytest
from support.weaver_test import weaver_test

# --- a build item -------------------------------------------------------------


@weaver_test()
def test_a_build_item_may_be_named_alone():
    """The portable form. Where it deploys is the configuration's answer."""

    from weaver.build_bundle.targets import parse_build_item

    binding = parse_build_item(
        "Lakehouse/Landing",
        workspace=_workspace_with("Lakehouse/Landing", "Landing_Dev"),
    )

    assert str(binding.item) == "Lakehouse/Landing"
    assert binding.target.item.name == "Landing_Dev"


@weaver_test()
def test_a_build_item_may_supply_its_physical_target():
    from weaver.build_bundle.targets import parse_build_item

    binding = parse_build_item("Lakehouse/Landing=Lakehouse/Landing_Dev")

    assert str(binding.item) == "Lakehouse/Landing"
    assert binding.target.item.name == "Landing_Dev"


@weaver_test()
def test_a_warehouse_build_item_reads_the_same_way():
    from weaver.build_bundle.targets import parse_build_item

    binding = parse_build_item("Warehouse/Curated=Warehouse/Curated_Dev")

    assert str(binding.item) == "Warehouse/Curated"
    assert binding.target.item.name == "Curated_Dev"


@weaver_test()
def test_both_routes_to_one_target_produce_one_binding():
    """Configuration and an explicit physical target are two ways to say it once."""

    from weaver.build_bundle.targets import parse_build_item

    configured = parse_build_item(
        "Lakehouse/Landing",
        workspace=_workspace_with("Lakehouse/Landing", "Landing_Dev"),
    )
    explicit = parse_build_item("Lakehouse/Landing=Lakehouse/Landing_Dev")

    assert configured.item == explicit.item
    assert configured.target.item == explicit.target.item
    assert configured.to_bound_target().id == explicit.to_bound_target().id


@weaver_test()
def test_an_explicit_target_needs_no_configured_entry():
    from weaver.build_bundle.targets import parse_build_item

    binding = parse_build_item(
        "Lakehouse/Landing=Lakehouse/Landing_Dev",
        workspace=_workspace_with("Warehouse/Curated", "Curated_Dev"),
    )

    assert binding.target.item.name == "Landing_Dev"


@weaver_test()
def test_the_same_bare_name_under_two_types_is_two_items():
    """`Lakehouse/Sales` and `Warehouse/Sales` are distinct items."""

    from weaver.build_bundle.targets import parse_build_item

    lakehouse = parse_build_item("Lakehouse/Sales=Lakehouse/Sales_LH")
    warehouse = parse_build_item("Warehouse/Sales=Warehouse/Sales_WH")

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
    from weaver.build_bundle.targets import parse_build_item
    from weaver.errors import BuildError

    with pytest.raises(BuildError, match="both must be"):
        parse_build_item(text)


@weaver_test()
def test_an_untyped_physical_target_is_refused():
    """The physical half is typed, so a kind mismatch reads as one."""

    from weaver.build_bundle.targets import parse_build_item
    from weaver.errors import BuildError

    with pytest.raises(BuildError, match="Lakehouse/Name or Warehouse/Name"):
        parse_build_item("Lakehouse/Landing=Landing_Dev")


@pytest.mark.parametrize(
    "text",
    ["Lakehouse/Landing=", "=Lakehouse/Landing", "a=b=c", "", "   "],
)
@weaver_test()
def test_malformed_build_items_are_refused(text):
    from weaver.build_bundle.targets import parse_build_item
    from weaver.errors import BuildError

    with pytest.raises(BuildError):
        parse_build_item(text)


@weaver_test()
def test_an_untyped_logical_item_is_refused():
    from weaver.build_bundle.targets import parse_build_item
    from weaver.errors import BuildError

    with pytest.raises(BuildError, match="logical Weaver item"):
        parse_build_item("Landing")


@weaver_test()
def test_an_item_with_no_configured_target_says_both_ways_to_fix_it():
    from weaver.build_bundle.targets import parse_build_item
    from weaver.errors import ConfigError

    with pytest.raises(ConfigError, match="no physical target"):
        parse_build_item(
            "Lakehouse/Landing",
            workspace=_workspace_with("Lakehouse/Other", "Other_LH"),
        )


def _workspace_with(logical: str, physical: str):
    from weaver.declaration.model import WeaverItemId
    from weaver.workspaces import TargetDeclaration, Workspace

    item = WeaverItemId.parse(logical)
    return Workspace(workspace="Demo", targets={item: TargetDeclaration(physical)})


# --- a run item ---------------------------------------------------------------


@pytest.mark.parametrize("what", ["load", "test"])
@pytest.mark.parametrize("text", ["Lakehouse/Landing", "Warehouse/Curated"])
@weaver_test()
def test_a_run_item_is_one_logical_identity(what, text):
    from weaver.operations.items import parse_run_item

    assert str(parse_run_item(text, what=what)) == text


@pytest.mark.parametrize("what", ["load", "test"])
@weaver_test()
def test_a_run_item_carrying_a_physical_half_is_refused(what):
    from weaver.errors import CommandError
    from weaver.operations.items import parse_run_item

    with pytest.raises(CommandError, match="comes from the Weaver catalogue"):
        parse_run_item("Lakehouse/Landing=Lakehouse/Anything", what=what)


@weaver_test()
def test_the_refusal_says_what_to_write_instead():
    from weaver.errors import CommandError
    from weaver.operations.items import parse_run_item

    with pytest.raises(CommandError, match=r"Write Lakehouse/Landing\."):
        parse_run_item("Lakehouse/Landing=Lakehouse/Landing_Dev", what="load")


@pytest.mark.parametrize("text", ["Landing", "Lakehouse", "Lakehouse/A/B", ""])
@weaver_test()
def test_a_malformed_run_item_is_refused(text):
    from weaver.errors import CommandError
    from weaver.operations.items import parse_run_item

    with pytest.raises(CommandError):
        parse_run_item(text, what="load")


@pytest.mark.parametrize("written", [None, [], ()])
@weaver_test()
def test_a_run_that_names_no_item_selects_none_here(written):
    """The whole installed catalogue, and the catalogue is what answers it.

    Nothing is expanded at this layer: the scope stays empty until a run has
    read ``_.Installation``.
    """

    from weaver.operations.items import requested_items

    assert requested_items(written, what="load") == ()


@weaver_test()
def test_a_repeated_run_item_is_named_once():
    from weaver.operations.items import requested_items

    assert requested_items(
        ["Lakehouse/Landing", "Lakehouse/Landing"], what="load"
    ) == requested_items(["Lakehouse/Landing"], what="load")


# --- what an empty scope means once the catalogue has answered -----------------


@weaver_test()
def test_an_empty_scope_covers_every_installed_item():
    from weaver.operations.items import run_scope

    dag = _installed({"Lakehouse/Landing": "Landing_Dev", "Warehouse/Curated": "Cur"})

    items, installed = run_scope(dag, (), what="load")

    assert [str(item) for item in items] == ["Lakehouse/Landing", "Warehouse/Curated"]
    assert set(installed) == set(items)


@weaver_test()
def test_an_explicit_scope_restricts_to_what_was_named():
    from weaver.operations.items import requested_items, run_scope

    dag = _installed({"Lakehouse/Landing": "Landing_Dev", "Warehouse/Curated": "Cur"})

    items, installed = run_scope(
        dag, requested_items("Warehouse/Curated", what="load"), what="load"
    )

    assert [str(item) for item in items] == ["Warehouse/Curated"]
    assert [str(target) for target in installed.values()] == ["Warehouse/Cur"]


@weaver_test()
def test_an_empty_catalogue_says_to_build_first():
    from weaver.errors import CommandError
    from weaver.operations.items import run_scope

    with pytest.raises(CommandError, match="found no installed items"):
        run_scope(_installed({}), (), what="load", catalogue="Warehouse/Weaver")


def _installed(installations: dict):
    """An installed graph carrying nothing but its ``_.Installation`` rows."""

    from types import SimpleNamespace

    from weaver.declaration.model import LAKEHOUSE, WeaverItemId
    from weaver.targets import (
        LAKEHOUSE_TARGET,
        WAREHOUSE_TARGET,
        PhysicalTargetRef,
    )

    def ref(item, name):
        kind = LAKEHOUSE_TARGET if item.item_type == LAKEHOUSE else WAREHOUSE_TARGET
        return PhysicalTargetRef(kind=kind, name=name)

    parsed = {WeaverItemId.parse(item): name for item, name in installations.items()}
    return SimpleNamespace(
        installations={item: ref(item, name) for item, name in parsed.items()}
    )


# --- what the notebook API accepts --------------------------------------------
#
# One item, several, or none. The same three spellings the command line has, so a
# notebook and a terminal select the same way.


@pytest.mark.parametrize(
    ("written", "selected"),
    [
        (None, ()),
        ("Lakehouse/Landing", ("Lakehouse/Landing",)),
        (
            ["Lakehouse/Landing", "Warehouse/Curated"],
            ("Lakehouse/Landing", "Warehouse/Curated"),
        ),
    ],
)
@pytest.mark.parametrize("operation", ["load", "test"])
@weaver_test()
def test_the_api_takes_one_item_a_sequence_or_none(
    operation, written, selected, monkeypatch
):
    """Every spelling reaches the run as a tuple of item identities.

    Recorded at the orchestration seam, because what is under test is the
    selection contract. Where an empty selection expands to is proved against a
    catalogue in ``tests/targeted/test_load_dry_run_cycle.py``.
    """

    import importlib

    from support.sessions import given_session
    from support.workspaces import given_workspace

    module = importlib.import_module(f"weaver.operations.{operation}")
    seen = []
    monkeypatch.setattr(
        module, f"run_{operation}", lambda session, **asked: seen.append(asked["items"])
    )

    workspace = given_workspace()
    with given_session(workspace=workspace) as session:
        getattr(module, operation)(written, session=session)

    assert [tuple(str(item) for item in items) for items in seen] == [selected]
