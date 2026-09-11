"""What ``weaver mirror`` settles before it empties anything.

Which catalogue is read, which is written, where each selected item goes, and
which runs are refused. None of it needs a tenant.

A fork has two sides and one vocabulary for them. ``mirror`` is read from,
``catalogue`` is written to, in configuration and on the command alike.
"""

from __future__ import annotations

import pytest
from support.weaver_test import weaver_test

import weaver
from weaver.config import parse_workspace
from weaver.declaration.model import WeaverItemId
from weaver.errors import CommandError, ConfigError
from weaver.operations.mirror import (
    MirrorPlan,
    MirrorResult,
    ResolvedMirror,
    resolve_mirror,
)
from weaver.workspaces import CatalogueRef, Workspace

WORKSPACE = "Analytics"


def _workspace(**overrides) -> Workspace:
    values = {
        "workspace": WORKSPACE,
        "catalogue": "Warehouse/Weaver_Dev",
        "mirror": "Warehouse/Weaver",
    }
    values.update(overrides)
    return Workspace(**values)


def _plan(monkeypatch, configured: Workspace, items=None, **named) -> MirrorPlan:
    _given(monkeypatch, configured)
    if items is not None:
        return weaver.plan_mirror(items, session=_never(), **named)
    return weaver.plan_mirror(no_item=True, session=_never(), **named)


def _with_targets() -> Workspace:
    """A dev configuration that deploys two items."""

    from weaver.workspaces import TargetDeclaration

    return _workspace(
        targets={
            WeaverItemId.parse("Warehouse/Model"): TargetDeclaration("Model_Dev"),
            WeaverItemId.parse("Lakehouse/Input"): TargetDeclaration("Input_Dev"),
        }
    )


# --- the configured pair ------------------------------------------------------


@weaver_test()
def test_configuration_names_both_sides_in_one_vocabulary():
    workspace = parse_workspace(
        {
            "workspace": WORKSPACE,
            "catalogue": "Warehouse/Weaver_Dev",
            "mirror": "Warehouse/Weaver",
        }
    )

    assert workspace.mirror == CatalogueRef(workspace=None, name="Weaver")
    assert str(workspace.mirror) == "Warehouse/Weaver"
    assert workspace.catalogue == "Warehouse/Weaver_Dev"


@weaver_test()
def test_a_source_may_name_the_workspace_holding_it():
    """The form parses, so an address written today keeps its meaning."""

    workspace = parse_workspace(
        {
            "workspace": WORKSPACE,
            "catalogue": "Warehouse/Weaver_Dev",
            "mirror": "Production/Warehouse/Weaver",
        }
    )

    assert workspace.mirror == CatalogueRef(workspace="Production", name="Weaver")
    assert not workspace.mirror.is_local_to(WORKSPACE)


@weaver_test()
def test_a_workspace_forks_nothing_unless_it_says_so():
    assert parse_workspace({"workspace": WORKSPACE}).mirror is None


@weaver_test()
@pytest.mark.parametrize(
    "written",
    ["Weaver", "Lakehouse/Weaver", "A/B/C/D", "Analytics/Lakehouse/Weaver"],
)
def test_a_source_that_is_not_a_warehouse_catalogue_is_refused(written):
    with pytest.raises(ConfigError):
        parse_workspace(
            {"workspace": WORKSPACE, "catalogue": "Warehouse/W", "mirror": written}
        )


# --- resolving the pair, which is additive across three arrangements ----------


@weaver_test()
def test_nothing_configured_means_both_sides_are_named(monkeypatch):
    """Only ``--workspace``, so the command supplies the whole pair."""

    plan = _plan(
        monkeypatch,
        Workspace(workspace=WORKSPACE),
        catalogue="Warehouse/DEV_Catalogue",
        mirror="Warehouse/Catalogue",
    )

    assert str(plan.source) == "Warehouse/Catalogue"
    assert plan.target == "Warehouse/DEV_Catalogue"


@weaver_test()
def test_a_configured_catalogue_alone_is_the_source(monkeypatch):
    """A production configuration describes the estate being forked from.

    Its ``catalogue:`` is the known side, and the destination is named.
    """

    plan = _plan(
        monkeypatch,
        Workspace(workspace=WORKSPACE, catalogue="Warehouse/Catalogue"),
        catalogue="Warehouse/DEV_Catalogue",
    )

    assert str(plan.source) == "Warehouse/Catalogue"
    assert plan.target == "Warehouse/DEV_Catalogue"


@weaver_test()
def test_a_configured_pair_needs_nothing_on_the_command(monkeypatch):
    """A configuration naming both describes a fork already."""

    plan = _plan(monkeypatch, _workspace())

    assert str(plan.source) == "Warehouse/Weaver"
    assert plan.target == "Warehouse/Weaver_Dev"


@weaver_test()
def test_a_configured_catalogue_alone_is_never_the_destination(monkeypatch):
    """The one arrangement a fork must not guess at.

    A production configuration's catalogue is the last Warehouse a fork should
    empty, so one known side is never read as both.
    """

    _given(monkeypatch, Workspace(workspace=WORKSPACE, catalogue="Warehouse/Catalogue"))

    with pytest.raises(CommandError, match="needs the catalogue to fork into"):
        weaver.plan_mirror(no_item=True, session=_never())


@weaver_test()
def test_naming_a_source_does_not_make_a_configured_catalogue_the_destination(
    monkeypatch,
):
    """Same protection with the source supplied: the destination is still named."""

    _given(monkeypatch, Workspace(workspace=WORKSPACE, catalogue="Warehouse/Catalogue"))

    with pytest.raises(CommandError, match="needs the catalogue to fork into"):
        weaver.plan_mirror(no_item=True, mirror="Warehouse/Other", session=_never())


@weaver_test()
def test_a_named_source_outranks_the_configured_one(monkeypatch):
    plan = _plan(monkeypatch, _workspace(), mirror="Warehouse/Another")

    assert str(plan.source) == "Warehouse/Another"
    assert plan.target == "Warehouse/Weaver_Dev"


@weaver_test()
def test_a_named_destination_outranks_the_configured_one(monkeypatch):
    plan = _plan(monkeypatch, _workspace(), catalogue="Warehouse/Somewhere_Else")

    assert str(plan.source) == "Warehouse/Weaver"
    assert plan.target == "Warehouse/Somewhere_Else"


@weaver_test()
def test_a_workspace_naming_neither_side_says_what_to_set(monkeypatch):
    _given(monkeypatch, Workspace(workspace=WORKSPACE))

    with pytest.raises(CommandError, match="needs the catalogue to fork"):
        weaver.plan_mirror(no_item=True, session=_never())


# --- what is refused, and refused before anything is emptied ------------------


@weaver_test()
def test_a_source_in_another_workspace_is_refused(monkeypatch):
    """A Fabric Warehouse reaches its own workspace and no further.

    Refused rather than attempted, because the step after this empties the
    destination Warehouse.
    """

    _given(monkeypatch, _workspace(mirror="Production/Warehouse/Weaver"))

    with pytest.raises(CommandError, match="must be in the workspace"):
        weaver.plan_mirror(no_item=True, session=_never())


@weaver_test()
def test_a_destination_in_another_workspace_is_refused(monkeypatch):
    """A fork writes the catalogue of the workspace it runs against."""

    _given(monkeypatch, _workspace())

    with pytest.raises(CommandError, match="A fork writes the catalogue"):
        weaver.plan_mirror(
            no_item=True, catalogue="Production/Warehouse/Weaver_Dev", session=_never()
        )


@weaver_test()
def test_forking_a_catalogue_into_itself_is_refused(monkeypatch):
    """The destination is emptied first, so this would destroy the source."""

    _given(monkeypatch, _workspace(catalogue="Warehouse/Weaver"))

    with pytest.raises(CommandError, match="would empty the catalogue it copies"):
        weaver.plan_mirror(no_item=True, session=_never())


@weaver_test()
def test_naming_items_and_no_item_together_is_refused(monkeypatch):
    _given(monkeypatch, _workspace())

    with pytest.raises(CommandError, match="not both"):
        weaver.plan_mirror(["Warehouse/Model"], no_item=True, session=_never())


@weaver_test()
def test_either_item_kind_resolves_and_keeps_its_own_kind(monkeypatch):
    """A physical target's kind is its item's, so both halves are typed."""

    plan = _plan(monkeypatch, _with_targets(), ["Lakehouse/Input"])
    (each,) = resolve_mirror(plan, _installed({"Lakehouse/Input": "Input"})).items

    assert each.kind == "Lakehouse"
    assert each.source == "Lakehouse/Input"
    assert each.target == "Lakehouse/Input_Dev"


@weaver_test()
def test_a_lakehouse_and_a_warehouse_of_one_name_are_two_items(monkeypatch):
    """Level-three identity is type and name, so these do not collide."""

    plan = _plan(monkeypatch, _with_targets(), ["Lakehouse/Input=Lakehouse/Weaver"])

    resolved = resolve_mirror(plan, _installed({"Lakehouse/Input": "Input"}))

    assert resolved.wiped == ("Warehouse/Weaver_Dev", "Lakehouse/Weaver")


@weaver_test()
def test_an_item_reads_its_destination_from_the_configured_targets(monkeypatch):
    """``build``'s grammar, so an unqualified item resolves through targets:."""

    plan = _plan(monkeypatch, _with_targets(), ["Warehouse/Model"])
    (each,) = resolve_mirror(plan, _installed({"Warehouse/Model": "Model"})).items

    assert str(each.item) == "Warehouse/Model"
    assert each.destination == "Model_Dev"
    assert each.source_target == "Model"


@weaver_test()
def test_a_named_destination_outranks_the_configured_target(monkeypatch):
    plan = _plan(monkeypatch, _with_targets(), ["Warehouse/Model=Warehouse/Somewhere"])
    (each,) = resolve_mirror(plan, _installed({"Warehouse/Model": "Model"})).items

    assert each.destination == "Somewhere"


@weaver_test()
def test_a_configured_target_is_selected_when_no_item_is_named(monkeypatch):
    """Bare ``mirror`` means every configured target, as ``build`` does."""

    _given(monkeypatch, _with_targets())
    plan = weaver.plan_mirror(session=_never())

    assert plan.items == ("Lakehouse/Input", "Warehouse/Model")


# --- one plan through preflight, prompt and execution ------------------------


@weaver_test()
def test_a_plan_says_what_a_confirmation_has_to_show(monkeypatch):
    plan = _plan(monkeypatch, _workspace())

    assert plan.mapping == ("Warehouse/Weaver_Dev", "Warehouse/Weaver")
    # Both sides are in this workspace, so neither repeats its name.
    assert str(plan) == "Warehouse/Weaver into Warehouse/Weaver_Dev"


@weaver_test()
def test_a_plan_carries_the_destination_as_its_own_catalogue(monkeypatch):
    """The operation reads its destination where every operation reads one.

    The workspace the fork runs against names the destination, so the wipe and
    the catalogue build agree with the pair the plan resolved.
    """

    plan = _plan(
        monkeypatch,
        Workspace(workspace=WORKSPACE, catalogue="Warehouse/Catalogue"),
        catalogue="Warehouse/DEV_Catalogue",
    )

    assert plan.workspace.catalogue == "Warehouse/DEV_Catalogue"
    assert plan.workspace.mirror == plan.source


@weaver_test()
def test_a_supplied_plan_is_not_resolved_again(monkeypatch):
    """The pair somebody was shown is the pair the fork acts on."""

    import weaver.operations.mirror as module

    plan = _plan(monkeypatch, _workspace())
    # Resolving again is the failure this guards against, so resolution fails
    # the test instead of returning a pair.
    monkeypatch.setattr(
        module,
        "plan_mirror",
        lambda *_a, **_k: pytest.fail("a supplied plan was resolved again"),
    )
    seen = {}

    def check(supplied, *, session=None):
        seen["plan"] = supplied
        raise CommandError("stop before the wipe")

    monkeypatch.setattr(module, "check_mirror", check)

    with pytest.raises(CommandError, match="stop before the wipe"):
        weaver.mirror(plan=plan, session=_open())

    assert seen["plan"] is plan


@weaver_test()
def test_a_settled_scope_is_acted_on_without_a_second_read(monkeypatch):
    """What somebody was shown is what runs, and nothing settles it again."""

    import weaver.operations.mirror as module

    plan = _plan(monkeypatch, _workspace())
    monkeypatch.setattr(
        module, "check_mirror", lambda *_a, **_k: pytest.fail("the scope was re-read")
    )

    with pytest.raises(CommandError, match="reached its work"):
        weaver.mirror(plan=ResolvedMirror(plan=plan), session=_open())


@weaver_test()
def test_the_source_is_proved_before_anything_is_emptied(monkeypatch):
    """A misspelled source fails while the destination is still intact."""

    import weaver.operations.mirror as module

    plan = _plan(monkeypatch, _workspace())
    monkeypatch.setattr(
        module,
        "_wipe_destination",
        lambda *_a, **_k: pytest.fail("the destination was emptied"),
    )
    monkeypatch.setattr(
        module,
        "check_mirror",
        lambda *_a, **_k: (_ for _ in ()).throw(CommandError("no such catalogue")),
    )

    with pytest.raises(CommandError, match="no such catalogue"):
        weaver.mirror(plan=plan, session=_open())


# --- the complete destructive scope, settled before any wipe -----------------


@weaver_test()
def test_the_scope_names_the_catalogue_and_every_item_target(monkeypatch):
    """What a confirmation shows and what the run empties are one list."""

    plan = _plan(monkeypatch, _with_targets(), ["Warehouse/Model"])
    resolved = resolve_mirror(plan, _installed({"Warehouse/Model": "Model"}))

    assert resolved.wiped == ("Warehouse/Weaver_Dev", "Warehouse/Model_Dev")
    # One list, the destination catalogue first, and the source of each row
    # second. The item's source is the physical Warehouse its rows come from.
    assert resolved.describe() == (
        "  Warehouse/Weaver_Dev  <- Warehouse/Weaver\n"
        "  Warehouse/Model_Dev   <- Warehouse/Model"
    )


@weaver_test()
def test_a_run_selecting_no_item_reads_no_catalogue(monkeypatch):
    """``--no-item`` forks the catalogue and stops, so there is none to read.

    The bindings a recreated shortcut resolves against are settled for every
    run, and this one rebinds nothing: the destination catalogue's own
    Warehouse is the whole of the map.
    """

    from weaver.catalogue.builtin import BUILTIN_ITEM

    plan = _plan(monkeypatch, _with_targets())
    resolved = resolve_mirror(plan, None)

    assert resolved.items == ()
    assert resolved.bindings == {BUILTIN_ITEM: "Weaver_Dev"}
    assert resolved.wiped == ("Warehouse/Weaver_Dev",)


@weaver_test()
def test_an_item_the_catalogue_never_installed_is_refused(monkeypatch):
    """There is nothing to point a View at, and the wipe has not happened yet."""

    plan = _plan(monkeypatch, _with_targets(), ["Warehouse/Model"])

    with pytest.raises(CommandError, match="records no installation"):
        resolve_mirror(plan, _installed({}))


@weaver_test()
def test_mirroring_an_item_onto_the_warehouse_it_borrows_from_is_refused(monkeypatch):
    """A named destination is emptied, and this one holds the rows being read."""

    plan = _plan(monkeypatch, _with_targets(), ["Warehouse/Model=Warehouse/Model_Dev"])

    with pytest.raises(CommandError, match="does not read from"):
        resolve_mirror(plan, _installed({"Warehouse/Model": "Model_Dev"}))


@weaver_test()
def test_mirroring_an_item_onto_the_source_catalogue_is_refused(monkeypatch):
    """The fork copies state from it, so emptying it would take the source."""

    plan = _plan(monkeypatch, _with_targets(), ["Warehouse/Model=Warehouse/Weaver"])

    with pytest.raises(CommandError, match="does not read from"):
        resolve_mirror(plan, _installed({"Warehouse/Model": "Model"}))


@weaver_test()
def test_a_destination_catalogue_holding_an_items_rows_is_refused(monkeypatch):
    """The whole wipe set is checked, not only the item destinations.

    The catalogue Warehouse is emptied first, so an item installed there loses
    the rows the same run then tries to borrow.
    """

    plan = _plan(monkeypatch, _with_targets(), ["Warehouse/Model"])

    with pytest.raises(CommandError, match="does not read from"):
        resolve_mirror(plan, _installed({"Warehouse/Model": "Weaver_Dev"}))


@weaver_test()
def test_two_items_of_different_kinds_may_share_a_destination_name(monkeypatch):
    """Two kinds of one name are two Fabric items, so neither wipe reaches both."""

    plan = _plan(
        monkeypatch,
        _with_targets(),
        ["Warehouse/Model=Warehouse/Shared", "Lakehouse/Input=Lakehouse/Shared"],
    )

    resolved = resolve_mirror(
        plan, _installed({"Warehouse/Model": "Model", "Lakehouse/Input": "Input"})
    )

    assert resolved.wiped == (
        "Warehouse/Weaver_Dev",
        "Warehouse/Shared",
        "Lakehouse/Shared",
    )


@weaver_test()
def test_two_items_sharing_one_destination_are_refused(monkeypatch):
    """The second wipe would take the first mirror, leaving both recorded."""

    plan = _plan(
        monkeypatch,
        _with_targets(),
        ["Warehouse/Model=Warehouse/Shared", "Warehouse/Other=Warehouse/Shared"],
    )

    with pytest.raises(CommandError, match="a destination of its own"):
        resolve_mirror(
            plan, _installed({"Warehouse/Model": "Model", "Warehouse/Other": "Other"})
        )


@weaver_test()
def test_an_item_rebuilt_over_the_destination_catalogue_is_refused(monkeypatch):
    """The catalogue is an output too, and it is rebuilt before the items are.

    An item emptying it afterwards would take the state this run has just
    copied in, and then write its own Mirror and Installation rows into what it
    destroyed.
    """

    plan = _plan(monkeypatch, _with_targets(), ["Warehouse/Model=Warehouse/Weaver_Dev"])

    with pytest.raises(CommandError, match="a destination of its own"):
        resolve_mirror(plan, _installed({"Warehouse/Model": "Model"}))


@weaver_test()
def test_a_lakehouse_may_be_named_for_the_destination_catalogues_name(monkeypatch):
    """Two kinds, two items: only a Warehouse holds the catalogue."""

    plan = _plan(monkeypatch, _with_targets(), ["Lakehouse/Input=Lakehouse/Weaver_Dev"])

    resolved = resolve_mirror(plan, _installed({"Lakehouse/Input": "Input"}))

    assert resolved.wiped == ("Warehouse/Weaver_Dev", "Lakehouse/Weaver_Dev")


@weaver_test()
def test_a_destination_another_item_occupies_is_emptied_like_any_other(monkeypatch):
    """Naming a Warehouse is saying its contents are disposable."""

    plan = _plan(monkeypatch, _with_targets(), ["Warehouse/Model=Warehouse/Reporting"])

    resolved = resolve_mirror(
        plan, _installed({"Warehouse/Model": "Model", "Warehouse/Other": "Reporting"})
    )

    assert resolved.wiped == ("Warehouse/Weaver_Dev", "Warehouse/Reporting")


# --- what it reports ----------------------------------------------------------


@weaver_test()
def test_a_result_says_what_moved_and_what_did_not():
    result = MirrorResult(
        workspace=WORKSPACE,
        source_catalogue="Warehouse/Weaver",
        destination_catalogue="Analytics/Warehouse/Weaver_Dev",
        wiped=("Warehouse/Weaver_Dev",),
        copied={"Registry": 12, "Installation": 2},
        uncopied=("Log", "LoadStatistic"),
    )

    assert result.rows == 14
    assert result.to_mapping()["wiped"] == ["Warehouse/Weaver_Dev"]
    assert result.to_mapping()["uncopied"] == ["Log", "LoadStatistic"]


# --- helpers ------------------------------------------------------------------


def _installed(targets: dict[str, str]):
    """A source catalogue recording where each item is installed."""

    from weaver.catalogue.state import Catalogue
    from weaver.catalogue.tables import INSTALLATION

    rows = {}
    for written, target in targets.items():
        item = WeaverItemId.parse(written)
        rows[item] = {
            INSTALLATION.name: (
                {
                    "item_type": item.item_type,
                    "item_name": item.item_name,
                    "target_name": target,
                },
            )
        }
    return Catalogue(rows=rows)


class _Never:
    """A session that fails if a refusal ever reaches a workspace."""

    def __getattr__(self, name):
        raise AssertionError(f"mirror reached the workspace: {name}")


def _never():
    return _Never()


class _Open:
    """A Session an operation may enter, and nothing more.

    For a claim that stops inside the operation: what is under test is the
    order, so the Session only has to be open. Opening a task ends the claim,
    which is where the work begins.
    """

    closed = False

    def task(self, *_names):
        raise CommandError("the run reached its work")


def _open():
    return _Open()


def _given(monkeypatch, workspace: Workspace) -> None:
    """Resolve to this Workspace, whatever the working directory holds."""

    import weaver.operations.workspace as module

    monkeypatch.setattr(module, "_operation_workspace", lambda **_kwargs: workspace)
