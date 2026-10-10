"""Which estate a wipe empties, and what it does with the catalogue.

Two decisions. Target selection names the physical items: the ones given, or
the estate the catalogue's ``_.Installation`` rows describe. Catalogue
disposition says what happens to the catalogue: removed by default, preserved
with its claims cleaned under ``--unbind``, untouched where none resolves.

A plan settles both, and a wipe empties the plan it was handed. The plan a
caller shows is the plan it runs.
"""

from __future__ import annotations

import pytest
from support.weaver_test import weaver_test

from weaver import plan_wipe
from weaver import wipe as public_wipe
from weaver.errors import CommandError
from weaver.locations import Location
from weaver.operations.wipe import (
    LEAVE,
    PHYSICAL_ONLY,
    REMOVE,
    UNBIND,
    WipeReport,
    WipeTarget,
    installed_targets,
)
from weaver.sessions.testing import TestSession


def _session() -> TestSession:
    """A recording Session with a store, which a Lakehouse wipe asks for."""

    return TestSession(store=object())


def _cli_module():
    """The CLI command module, not the callable the package re-exports."""

    import sys

    import weaver_cli.main  # noqa: F401 - imported for sys.modules

    return sys.modules["weaver_cli.main"]


def _operations():
    import sys

    import weaver.operations.wipe  # noqa: F401 - imported for sys.modules

    return sys.modules["weaver.operations.wipe"]


class _Installations:
    """A catalogue holding the ``_.Installation`` rows a test declares.

    The ``CatalogueConnection`` shape, so :func:`read_table` projects and reads
    these rows exactly as it does over TDS.
    """

    def __init__(self, rows):
        self.declared = list(rows)
        self.statements = []

    def columns_of(self, table):
        if table.name != "Installation":
            return None
        return {
            column.public_name.casefold(): column.public_name
            for column in table.columns
        }

    def rows(self, statement):
        self.statements.append(statement)
        return [
            {
                "item_type": item_type,
                "item_name": item_name,
                "target_name": target,
                "weaver_version": "0.9.0",
                "declaration_signature": "signature",
            }
            for item_type, item_name, target in self.declared
        ]

    def execute(self, statement):
        self.statements.append(statement)


def _plan(*targets, catalogue="Warehouse/Weaver", workspace="Analytics", **rest):
    return plan_wipe(
        targets, workspace=workspace, catalogue=catalogue, session=_session(), **rest
    )


def _names(plan) -> list[str]:
    return [str(target) for target in plan.targets]


# --- selecting targets --------------------------------------------------------


@weaver_test()
def test_named_targets_are_exactly_what_a_wipe_selects():
    """No expansion. Two targets are two items, plus the catalogue."""

    plan = _plan("Lakehouse/Landing", "Warehouse/Curated")

    assert _names(plan) == [
        "Lakehouse/Landing",
        "Warehouse/Curated",
        "Warehouse/Weaver",
    ]


@weaver_test()
def test_a_resolved_catalogue_is_removed_by_default_and_goes_last():
    plan = _plan("Lakehouse/Landing")

    assert plan.catalogue_action == REMOVE
    assert _names(plan)[-1] == "Warehouse/Weaver"
    assert plan.is_catalogue(plan.targets[-1])
    assert plan.unbound == ()


@weaver_test()
def test_the_catalogue_appears_once_when_it_is_also_a_named_target():
    plan = _plan("Warehouse/Weaver", "Lakehouse/Landing")

    assert _names(plan) == ["Lakehouse/Landing", "Warehouse/Weaver"]


@weaver_test()
def test_a_lakehouse_of_the_catalogues_name_is_a_different_item():
    """Level-three identity is workspace, type and name."""

    plan = _plan("Lakehouse/Weaver")

    assert _names(plan) == ["Lakehouse/Weaver", "Warehouse/Weaver"]
    assert plan.is_catalogue(plan.targets[0]) is False


@weaver_test()
def test_no_resolvable_catalogue_leaves_the_selection_alone():
    plan = _plan("Lakehouse/Landing", catalogue=None)

    assert plan.catalogue_action == LEAVE
    assert plan.catalogue is None
    assert _names(plan) == ["Lakehouse/Landing"]


# --- unbind -------------------------------------------------------------------


@weaver_test()
def test_unbind_preserves_the_catalogue_and_names_the_claims():
    plan = _plan("Lakehouse/Landing", unbind=True)

    assert plan.catalogue_action == UNBIND
    assert _names(plan) == ["Lakehouse/Landing"]
    assert plan.unbound == ("Lakehouse/Landing",)


@weaver_test()
def test_unbind_without_a_catalogue_is_refused_before_anything_is_emptied():
    with pytest.raises(CommandError, match="requires a catalogue"):
        _plan("Lakehouse/Landing", catalogue=None, unbind=True)


@weaver_test()
def test_unbind_naming_the_catalogue_itself_is_refused():
    with pytest.raises(CommandError, match="cannot also be a target"):
        _plan("Warehouse/Weaver", unbind=True)


@weaver_test()
def test_unbind_with_no_target_selection_is_refused():
    with pytest.raises(CommandError, match="requires named targets"):
        _plan(unbind=True)


# --- the unbind invariants, however unbind was selected -----------------------
#
# `unbind=True` is what a command line says and `catalogue_action=UNBIND` what a
# caller names outright. Both mean one thing, so both are held to it.

#: The two ways one invocation asks for UNBIND.
UNBIND_ROUTES = ({"unbind": True}, {"catalogue_action": UNBIND})


@pytest.mark.parametrize("route", UNBIND_ROUTES, ids=("flag", "named"))
@weaver_test()
def test_unbind_never_includes_the_resolved_catalogue_as_a_target(route):
    """Emptying a catalogue and preserving it are two different plans."""

    with pytest.raises(CommandError, match="cannot also be a target"):
        _plan("Warehouse/Weaver", **route)


@pytest.mark.parametrize("route", UNBIND_ROUTES, ids=("flag", "named"))
@weaver_test()
def test_unbind_needs_a_resolved_catalogue(route):
    with pytest.raises(CommandError, match="requires a catalogue"):
        _plan("Lakehouse/Landing", catalogue=None, **route)


@pytest.mark.parametrize("route", UNBIND_ROUTES, ids=("flag", "named"))
@weaver_test()
def test_unbind_needs_named_targets(route):
    with pytest.raises(CommandError, match="requires named targets"):
        _plan(**route)


@pytest.mark.parametrize("route", UNBIND_ROUTES, ids=("flag", "named"))
@weaver_test()
def test_unbind_names_the_claims_it_will_delete(route):
    plan = _plan("Lakehouse/Landing", **route)

    assert plan.catalogue_action == UNBIND
    assert _names(plan) == ["Lakehouse/Landing"]
    assert plan.unbound == ("Lakehouse/Landing",)


# --- leave is the absence of a catalogue --------------------------------------


@weaver_test()
def test_leave_over_a_resolved_catalogue_is_refused():
    """Naming it over one that resolved described an estate nobody asked for.

    With no targets it would read that catalogue to discover the estate, empty
    every physical target in it, and leave the catalogue certifying objects
    that are gone.
    """

    with pytest.raises(CommandError, match="requires no resolved catalogue"):
        _plan(catalogue_action=LEAVE)


@weaver_test()
def test_leave_over_a_resolved_catalogue_is_refused_with_targets_too():
    with pytest.raises(CommandError, match="requires no resolved catalogue"):
        _plan("Lakehouse/Landing", catalogue_action=LEAVE)


@weaver_test()
def test_the_refusal_names_the_disposition_that_does_mean_this():
    with pytest.raises(CommandError, match="physical-only"):
        _plan("Lakehouse/Landing", catalogue_action=LEAVE)


@weaver_test()
def test_leave_discovers_no_estate_before_it_is_refused(monkeypatch):
    """Refused while every target is intact, so nothing is read to decide it."""

    monkeypatch.setattr(
        _operations(),
        "_installed_estate",
        lambda *_a, **_k: pytest.fail("a refused plan read the catalogue"),
    )

    with pytest.raises(CommandError, match="requires no resolved catalogue"):
        _plan(catalogue_action=LEAVE)


@weaver_test()
def test_leave_is_what_no_resolved_catalogue_produces():
    plan = _plan("Lakehouse/Landing", catalogue=None, catalogue_action=LEAVE)

    assert plan.catalogue_action == LEAVE
    assert plan.catalogue is None


# --- physical-only, which no command line reaches -----------------------------


@weaver_test()
def test_physical_only_empties_exactly_the_named_targets():
    """One physical item. A catalogue is not an estate index here."""

    plan = _plan("Warehouse/Sales_Dev", catalogue_action=PHYSICAL_ONLY)

    assert _names(plan) == ["Warehouse/Sales_Dev"]


@weaver_test()
def test_physical_only_adds_no_catalogue_and_removes_none():
    """A resolved catalogue is neither appended nor taken away."""

    plan = _plan("Warehouse/Sales_Dev", catalogue_action=PHYSICAL_ONLY)

    assert plan.catalogue == "Warehouse/Weaver"
    assert plan.empties_the_catalogue is False
    assert "Warehouse/Weaver" not in _names(plan)


@weaver_test()
def test_physical_only_unbinds_no_claim():
    plan = _plan("Warehouse/Sales_Dev", catalogue_action=PHYSICAL_ONLY)

    assert plan.unbound == ()


@weaver_test()
def test_physical_only_empties_a_catalogue_it_is_handed_as_an_ordinary_item():
    """The destination catalogue of a fork: one Warehouse, and no claims read."""

    plan = _plan("Warehouse/Weaver", catalogue_action=PHYSICAL_ONLY)

    assert _names(plan) == ["Warehouse/Weaver"]
    assert plan.unbound == ()


@weaver_test()
def test_physical_only_expands_nothing(monkeypatch):
    """It reads no catalogue, so it has no estate to discover."""

    monkeypatch.setattr(
        _operations(),
        "_installed_estate",
        lambda *_a, **_k: pytest.fail("a physical-only wipe discovered an estate"),
    )

    with pytest.raises(CommandError, match="physical-only"):
        _plan(catalogue_action=PHYSICAL_ONLY)


@weaver_test()
def test_no_command_line_can_ask_for_physical_only():
    """Internal. The CLI passes a selection and `--unbind`, and nothing else."""

    import inspect

    from weaver_cli.main import build_parser

    source = inspect.getsource(_cli_module())
    assert "catalogue_action" not in source
    assert "PHYSICAL_ONLY" not in source

    wipe = build_parser()._subparsers._group_actions[0].choices["wipe"]
    assert "--catalogue-action" not in wipe.format_help()
    assert "physical-only" not in wipe.format_help()


# --- naming a disposition -----------------------------------------------------


@weaver_test()
def test_a_disposition_and_an_unbind_flag_that_disagree_are_refused():
    with pytest.raises(CommandError, match="catalogue_action says"):
        _plan("Warehouse/Sales_Dev", unbind=True, catalogue_action=LEAVE)


@weaver_test()
def test_an_unknown_disposition_names_the_ones_there_are():
    with pytest.raises(CommandError, match="catalogue_action is one of"):
        _plan("Warehouse/Sales_Dev", catalogue_action="destroy")


@weaver_test()
def test_mirror_empties_physical_targets_without_estate_semantics():
    """Read off the wiring, so estate discovery and claims cannot reach mirror."""

    import inspect
    import sys

    import weaver.mirror_plan  # noqa: F401 - imported for sys.modules
    import weaver.operations.mirror  # noqa: F401 - imported for sys.modules

    for module in ("weaver.mirror_plan", "weaver.operations.mirror"):
        source = inspect.getsource(sys.modules[module])
        assert "plan_wipe" not in source
        assert "UNBIND" not in source
    assert (
        inspect.getsource(sys.modules["weaver.mirror_plan"]).count(
            "target_wipe_actions("
        )
        == 2
    )


# --- estate discovery ---------------------------------------------------------


@weaver_test()
def test_the_estate_comes_from_the_catalogue_installations():
    catalogue = _Installations(
        [
            ("Lakehouse", "Sales", "Landing_Dev"),
            ("Warehouse", "Reporting", "Curated_Dev"),
            # Two logical items in one target are one physical item to empty.
            ("Lakehouse", "Inventory", "Landing_Dev"),
        ]
    )

    assert [str(target) for target in installed_targets(catalogue)] == [
        "Lakehouse/Landing_Dev",
        "Warehouse/Curated_Dev",
    ]


@weaver_test()
def test_an_estate_read_names_the_installation_table():
    catalogue = _Installations([("Lakehouse", "Sales", "Landing_Dev")])
    installed_targets(catalogue)

    assert any("[_].[Installation]" in each for each in catalogue.statements)


@weaver_test()
def test_a_catalogue_with_no_installations_leaves_only_itself(monkeypatch):
    monkeypatch.setattr(_operations(), "_installed_estate", lambda *_a, **_k: ())

    assert _names(_plan()) == ["Warehouse/Weaver"]


@weaver_test()
def test_a_discovered_estate_still_puts_the_catalogue_last(monkeypatch):
    monkeypatch.setattr(
        _operations(),
        "_installed_estate",
        lambda *_a, **_k: (
            ("Warehouse/_weaver", WipeTarget.parse("Warehouse/Weaver")),
            ("Lakehouse/Sales", WipeTarget.parse("Lakehouse/Landing_Dev")),
        ),
    )

    assert _names(_plan()) == ["Lakehouse/Landing_Dev", "Warehouse/Weaver"]


def _configured(tmp_path, targets: str):
    configuration = tmp_path / "workspace-config.yml"
    configuration.write_text(
        "workspace: Analytics\ncatalogue: Warehouse/Weaver\ntargets:\n" + targets,
        encoding="utf-8",
    )
    return configuration


def _recorded(monkeypatch, *pairs):
    monkeypatch.setattr(
        _operations(),
        "_installed_estate",
        lambda *_a, **_k: tuple(
            (item, WipeTarget.parse(target)) for item, target in pairs
        ),
    )


@weaver_test()
def test_a_configured_wipe_follows_installations_the_configuration_binds(
    tmp_path, monkeypatch
):
    configuration = _configured(
        tmp_path,
        "  Lakehouse/Landing: DEV_Landing\n"
        "  Warehouse/Curated: DEV_Curated\n"
        # Bound for reading, and installed nowhere, so not emptied.
        "  Lakehouse/Reference: Landing\n",
    )
    _recorded(
        monkeypatch,
        ("Warehouse/_weaver", "Warehouse/Weaver"),
        ("Lakehouse/Landing", "Lakehouse/DEV_Landing"),
        ("Warehouse/Curated", "Warehouse/DEV_Curated"),
    )

    plan = plan_wipe(workspace_config=configuration, session=_session())

    assert _names(plan) == [
        "Lakehouse/DEV_Landing",
        "Warehouse/DEV_Curated",
        "Warehouse/Weaver",
    ]
    described = plan.describe()
    assert "Lakehouse/Landing → Lakehouse/DEV_Landing" in described
    assert "Warehouse/Curated → Warehouse/DEV_Curated" in described


@weaver_test()
def test_a_configured_wipe_follows_the_catalogue_browser_it_configures(
    tmp_path, monkeypatch
):
    configuration = _configured(tmp_path, "  Lakehouse/Landing: DEV_Landing\n")
    configuration.write_text(
        configuration.read_text() + "catalogue_browser: Estate Browser\n",
        encoding="utf-8",
    )
    _recorded(
        monkeypatch,
        ("Warehouse/_weaver", "Warehouse/Weaver"),
        ("Lakehouse/Landing", "Lakehouse/DEV_Landing"),
        ("SemanticModel/Catalogue Browser", "SemanticModel/Estate Browser"),
    )

    plan = plan_wipe(workspace_config=configuration, session=_session())

    assert "SemanticModel/Estate Browser" in _names(plan)


@weaver_test()
def test_a_wipe_names_an_item_once_where_it_is_installed_under_its_own_name(
    tmp_path, monkeypatch
):
    configuration = _configured(tmp_path, "  Lakehouse/Landing: Landing\n")
    _recorded(
        monkeypatch,
        ("Warehouse/_weaver", "Warehouse/Weaver"),
        ("Lakehouse/Landing", "Lakehouse/Landing"),
    )

    described = plan_wipe(workspace_config=configuration, session=_session()).describe()

    assert "  Lakehouse/Landing\n" in described + "\n"
    assert "→" not in described


@weaver_test()
def test_a_configured_wipe_refuses_an_installation_the_configuration_binds_elsewhere(
    tmp_path, monkeypatch
):
    """A catalogue recording another physical item is not followed into it."""

    configuration = _configured(
        tmp_path,
        "  Lakehouse/Landing: DEV_Landing\n  Warehouse/Curated: DEV_Curated\n",
    )
    _recorded(
        monkeypatch,
        ("Lakehouse/Landing", "Lakehouse/Landing"),
        ("Warehouse/Curated", "Warehouse/DEV_Curated"),
    )

    with pytest.raises(CommandError) as refused:
        plan_wipe(workspace_config=configuration, session=_session())

    said = str(refused.value)
    assert "Lakehouse/Landing  installed in Landing, configured as DEV_Landing" in said
    assert "Warehouse/Curated" not in said.split("\n")[1]
    assert "weaver wipe Lakehouse/DEV_Landing Warehouse/DEV_Curated" in said


@weaver_test()
def test_a_configured_wipe_refuses_an_installation_the_configuration_does_not_name(
    tmp_path, monkeypatch
):
    configuration = _configured(tmp_path, "  Lakehouse/Landing: DEV_Landing\n")
    _recorded(
        monkeypatch,
        ("Lakehouse/Landing", "Lakehouse/DEV_Landing"),
        ("Warehouse/Curated", "Warehouse/Curated"),
    )

    with pytest.raises(
        CommandError, match="Warehouse/Curated  installed in Curated, not configured"
    ):
        plan_wipe(workspace_config=configuration, session=_session())


@weaver_test()
def test_a_wipe_given_only_a_catalogue_follows_its_installations(monkeypatch):
    """With no configuration to check against, the catalogue is the guidance."""

    _recorded(monkeypatch, ("Lakehouse/Landing", "Lakehouse/Landing"))

    plan = plan_wipe(
        workspace="Analytics", catalogue="Warehouse/Weaver", session=_session()
    )

    assert _names(plan) == ["Lakehouse/Landing", "Warehouse/Weaver"]


@weaver_test()
def test_named_targets_are_emptied_exactly_whatever_the_configuration_binds(
    tmp_path, monkeypatch
):
    configuration = _configured(tmp_path, "  Lakehouse/Landing: DEV_Landing\n")
    monkeypatch.setattr(
        _operations(),
        "_installed_estate",
        lambda *_a, **_k: pytest.fail("named targets read the estate"),
    )

    plan = plan_wipe(
        "Lakehouse/Leftover", workspace_config=configuration, session=_session()
    )

    assert _names(plan) == ["Lakehouse/Leftover", "Warehouse/Weaver"]


@weaver_test()
def test_a_wipe_with_no_targets_and_no_catalogue_says_what_it_needs():
    with pytest.raises(CommandError, match="needs named targets or a Weaver catalogue"):
        plan_wipe(workspace="Analytics", session=_session())


# --- executing the plan -------------------------------------------------------


def _emptied(monkeypatch, removed=("Sales",), area="delta"):
    """Record which targets a wipe's plan reached, touching no physical item.

    ``removed`` is what each Lakehouse area sweep reports, split into the
    shortcuts its detach removed and the entries its sweep deleted.
    """

    from weaver.mutation.executor import MutationReport, MutationResult

    reached = []
    shortcuts = [name for name in removed if name.startswith("shortcut:")]
    entries = [name for name in removed if not name.startswith("shortcut:")]

    def execute(session, plan, payloads=None, **options):
        names = {
            target.id: f"{target.kind.title()}/{target.item_name}"
            for target in plan.targets
        }
        results = []
        for _s, batch, action in plan.actions():
            name = names[batch.target_id]
            if name not in reached and action.kind != "unbind_catalogue_claims":
                reached.append(name)
            files = action.kind.endswith("files") or "file_shortcuts" in action.kind
            mine = (area == "folder") == files
            value = {}
            if action.kind.startswith("detach"):
                value = {"removed": shortcuts if mine else []}
            elif action.kind.startswith("clear"):
                value = {
                    "location": f"/tmp/local/{name}",
                    "removed": entries if mine else [],
                }
            results.append(MutationResult(action.id, "succeeded", value))
        return MutationReport(plan.bundle_id, tuple(results))

    def enumerate_lakehouse(item, workspace, *, dry_run, **_options):
        assert dry_run, "a wipe enumerated rather than executed its plan"
        reached.append(f"Lakehouse/{item.name}")
        return (
            WipeReport(f"{area}:{item}", Location(f"/tmp/local/{item}"), removed, True),
        )

    import weaver.physical_wipe as mechanics

    monkeypatch.setattr(TestSession, "execute_mutation", execute)
    monkeypatch.setattr(mechanics, "wipe_lakehouse", enumerate_lakehouse)
    return _operations(), reached


def _unbinding(monkeypatch, asked):
    from weaver.catalogue.unbind import ClaimDeletion

    def plan_unbind(plan, _workspace, **_options):
        asked.append(plan.unbound)
        return ClaimDeletion(
            targets=plan.unbound, logical_items=(), statements=("delete claims",)
        )

    monkeypatch.setattr(_operations(), "_plan_unbind", plan_unbind)


@weaver_test()
def test_a_wipe_empties_the_plan_it_was_given(monkeypatch):
    _operations_module, reached = _emptied(monkeypatch)
    plan = _plan("Lakehouse/Landing")

    result = public_wipe(plan=plan, session=_session())

    assert reached == ["Lakehouse/Landing", "Warehouse/Weaver"]
    assert result.plan is plan


# --- a settled plan answers the planning arguments ----------------------------
#
# A wipe is destructive, so an argument that looks like it changes the supplied
# plan is refused instead of ignored.

#: Every planning argument, beside a plan that already answers it.
PLANNING_BESIDE_A_PLAN = (
    {"targets": "Lakehouse/Landing"},
    {"workspace": "Elsewhere"},
    {"catalogue": "Warehouse/Other"},
    {"environment": "Runtime"},
    {"workspace_config": "dev.yml"},
    {"unbind": True},
    {"catalogue_action": UNBIND},
)


@pytest.mark.parametrize("given", PLANNING_BESIDE_A_PLAN, ids=lambda g: next(iter(g)))
@weaver_test()
def test_a_planning_argument_beside_a_plan_is_refused(given):
    plan = _plan("Warehouse/Curated")
    targets = given.pop("targets", ())

    with pytest.raises(CommandError, match="a settled plan or planning arguments"):
        public_wipe(targets, plan=plan, session=_session(), **given)


@weaver_test()
def test_execution_arguments_travel_with_a_plan(monkeypatch):
    """`session` and `dry_run` say how this runs, and a plan answers neither."""

    _emptied(monkeypatch)

    result = public_wipe(
        plan=_plan("Warehouse/Curated"), session=_session(), dry_run=True
    )

    assert result.dry_run is True


@weaver_test()
def test_a_removed_catalogue_unbinds_nothing(monkeypatch):
    operations, _reached = _emptied(monkeypatch)
    monkeypatch.setattr(
        operations,
        "_plan_unbind",
        lambda *_a, **_k: pytest.fail("claims were deleted from an emptied catalogue"),
    )

    result = public_wipe(plan=_plan("Lakehouse/Landing"), session=_session())

    assert result.unbound is None


@weaver_test()
def test_unbind_removes_the_claims_for_the_targets_it_emptied(monkeypatch):
    _operations_module, reached = _emptied(monkeypatch)
    asked = []
    _unbinding(monkeypatch, asked)

    result = public_wipe(
        plan=_plan("Lakehouse/Landing", unbind=True), session=_session()
    )

    assert reached == ["Lakehouse/Landing"]
    assert asked == [("Lakehouse/Landing",)]
    assert result.unbound == {
        "targets": ["Lakehouse/Landing"],
        "logical_items": [],
        "statements": 1,
    }


@weaver_test()
def test_a_dry_run_removes_nothing_and_deletes_no_claim(monkeypatch):
    operations, _reached = _emptied(monkeypatch)
    monkeypatch.setattr(
        operations,
        "_plan_unbind",
        lambda *_a, **_k: pytest.fail("a dry run deleted claims"),
    )
    monkeypatch.setattr(
        TestSession,
        "execute_mutation",
        lambda *_a, **_k: pytest.fail("a dry run executed a wipe"),
    )

    result = public_wipe(
        plan=_plan("Lakehouse/Landing", unbind=True),
        session=_session(),
        dry_run=True,
    )

    assert result.dry_run
    assert all(report.dry_run for report in result.reports)


# --- one result line per physical item ----------------------------------------


@weaver_test()
def test_one_coherent_line_per_physical_item(monkeypatch):
    _emptied(monkeypatch, removed=("Sales", "Reporting", "shortcut:Tables/External"))

    result = public_wipe(plan=_plan("Lakehouse/Landing"), session=_session())

    assert [item.target for item in result.items] == [
        "Lakehouse/Landing",
        "Warehouse/Weaver",
    ]
    assert result.items[0].counts == {"entries": 2, "shortcuts": 1}
    assert "2 entries" in result.items[0].describe()
    assert "1 shortcuts" in result.items[0].describe()


@weaver_test()
def test_a_long_target_keeps_its_outcome_apart():
    from weaver.operations.wipe import EMPTIED, WipeItemResult

    item = WipeItemResult(
        target="SemanticModel/A semantic model name", outcome=EMPTIED, counts={}
    )
    assert item.describe() == "SemanticModel/A semantic model name  emptied"
    assert item.describe(40).endswith(" " * 7 + "emptied")


@weaver_test()
def test_a_file_is_not_counted_as_a_folder(monkeypatch):
    """The reports carry names and not kinds, so `entries` is what can be said.

    `notes.txt` is a file. Counting it among folders was a count nobody could
    read correctly.
    """

    _emptied(monkeypatch, removed=("notes.txt", "Sales"), area="folder")

    result = public_wipe(plan=_plan("Lakehouse/Landing"), session=_session())

    assert result.items[0].counts == {"entries": 2}
    assert "folder" not in result.items[0].describe()


@weaver_test()
def test_a_warehouse_counts_nothing_rather_than_one_placeholder(monkeypatch):
    """A placeholder name became a count of one for a Warehouse holding many."""

    _emptied(monkeypatch)

    result = public_wipe(plan=_plan("Warehouse/Curated"), session=_session())

    assert result.items[0].counts == {}
    assert result.items[0].describe().split() == ["Warehouse/Curated", "emptied"]


@weaver_test()
def test_a_preserved_catalogue_reports_itself_as_preserved(monkeypatch):
    _emptied(monkeypatch)
    _unbinding(monkeypatch, [])

    result = public_wipe(
        plan=_plan("Lakehouse/Landing", unbind=True), session=_session()
    )

    catalogue = result.items[-1]
    assert catalogue.target == "Warehouse/Weaver"
    assert catalogue.outcome == "preserved"
    assert catalogue.unbound is True
    assert "claims unbound" in catalogue.describe()


@weaver_test()
def test_the_json_result_carries_the_plan_and_the_items(monkeypatch):
    _emptied(monkeypatch)

    payload = public_wipe(
        plan=_plan("Lakehouse/Landing"), session=_session()
    ).to_mapping()

    assert payload["plan"]["catalogue_action"] == REMOVE
    assert payload["plan"]["targets"][-1] == {
        "target": "Warehouse/Weaver",
        "catalogue": True,
    }
    assert [item["target"] for item in payload["items"]] == [
        "Lakehouse/Landing",
        "Warehouse/Weaver",
    ]
    # Estate level. The low-level per-area removals stay off the result payload.
    assert "reports" not in payload


# --- the question a person answers --------------------------------------------


def _described(plan) -> list[str]:
    """The preflight, with the padding that aligns its columns collapsed."""

    return [" ".join(line.split()) for line in plan.describe().splitlines()]


@weaver_test()
def test_the_preflight_names_items_and_no_objects_inside_them():
    described = _described(_plan("Lakehouse/Landing", "Warehouse/Curated"))

    assert "Wipe on Analytics" in described
    assert "Lakehouse/Landing" in described
    assert "Warehouse/Weaver emptied last" in described
    for inventory in ("Tables/", "Files/", "abfss://", "dbo.", "shortcut:"):
        assert all(inventory not in line for line in described)


@weaver_test()
def test_the_preflight_marks_which_item_is_the_catalogue():
    assert "Warehouse/Weaver catalogue" in _described(_plan("Lakehouse/Landing"))


@weaver_test()
def test_the_preflight_says_where_claims_are_unbound():
    described = _described(_plan("Lakehouse/Landing", unbind=True))

    assert (
        "Warehouse/Weaver preserved; claims for Lakehouse/Landing unbound" in described
    )


# --- the description is what execution does -----------------------------------
#
# A plan is shown, authorised, then executed. The description is the question a
# person answers, and execution is the answer, so the two say the same thing.

#: One plan per disposition, spanning what a command line and an internal caller
#: can settle on.
EVERY_DISPOSITION = (
    ("remove-named", {"targets": ("Lakehouse/Landing",), "rest": {}}),
    ("remove-estate-shaped", {"targets": ("Warehouse/Weaver",), "rest": {}}),
    ("unbind", {"targets": ("Lakehouse/Landing",), "rest": {"unbind": True}}),
    (
        "physical-only",
        {
            "targets": ("Warehouse/Sales_Dev",),
            "rest": {"catalogue_action": PHYSICAL_ONLY},
        },
    ),
    (
        "physical-only-on-the-catalogue",
        {
            "targets": ("Warehouse/Weaver",),
            "rest": {"catalogue_action": PHYSICAL_ONLY},
        },
    ),
    (
        "leave",
        {"targets": ("Lakehouse/Landing",), "rest": {"catalogue": None}},
    ),
)


def _for(case) -> object:
    return _plan(*case["targets"], **case["rest"])


@pytest.mark.parametrize(
    "case",
    [each for _name, each in EVERY_DISPOSITION],
    ids=[n for n, _ in EVERY_DISPOSITION],
)
@weaver_test()
def test_the_description_never_calls_an_emptied_catalogue_preserved(case):
    plan = _for(case)
    described = plan.describe()

    if plan.empties_the_catalogue:
        assert "preserved" not in described
    if plan.catalogue is not None and not plan.empties_the_catalogue:
        assert "emptied last" not in described


@pytest.mark.parametrize(
    "case",
    [each for _name, each in EVERY_DISPOSITION],
    ids=[n for n, _ in EVERY_DISPOSITION],
)
@weaver_test()
def test_the_description_claims_a_claim_only_where_one_is_deleted(case):
    plan = _for(case)

    assert ("unbound" in plan.describe()) is bool(plan.unbound)


@pytest.mark.parametrize(
    "case",
    [each for _name, each in EVERY_DISPOSITION],
    ids=[n for n, _ in EVERY_DISPOSITION],
)
@weaver_test()
def test_the_items_executed_are_the_items_described(case, monkeypatch):
    """Every target the description lists is emptied, and nothing else is."""

    _operations_module, reached = _emptied(monkeypatch)
    _unbinding(monkeypatch, [])
    plan = _for(case)

    result = public_wipe(plan=plan, session=_session())

    described = plan.describe()
    emptied = [item.target for item in result.items if item.outcome == "emptied"]
    assert reached == emptied == _names(plan)
    for target in emptied:
        assert target in described
    # A catalogue the description calls preserved is not among them.
    if plan.catalogue is not None and "preserved" in described:
        assert plan.catalogue not in emptied
