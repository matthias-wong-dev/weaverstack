"""The CLI plans the estate, shows it, asks, then empties that same plan."""

from __future__ import annotations

import importlib
import json

import pytest
from support.weaver_test import weaver_test
from support.workspaces import given_workspace

from weaver import WipeItemResult, WipePlan, WipeResult
from weaver.operations.wipe import EMPTIED, PRESERVED, REMOVE, UNBIND, WipeTarget
from weaver_cli import main
from weaver_cli.main import build_parser

WORKSPACE = given_workspace(workspace="Analytics", catalogue="Warehouse/Weaver")


def _plan(
    *targets, catalogue="Warehouse/Weaver", action=REMOVE, unbound=()
) -> WipePlan:
    return WipePlan(
        workspace=WORKSPACE,
        targets=tuple(WipeTarget.parse(value) for value in targets),
        catalogue=catalogue,
        catalogue_action=action,
        unbound=unbound,
    )


def _result(plan: WipePlan, *, items=None) -> WipeResult:
    return WipeResult(
        workspace=WORKSPACE.workspace,
        items=tuple(
            items
            if items is not None
            else (
                WipeItemResult(
                    target=str(target),
                    outcome=EMPTIED,
                    is_catalogue=plan.is_catalogue(target),
                    counts={"tables": 3},
                )
                for target in plan.targets
            )
        ),
        plan=plan,
    )


def _wired(monkeypatch, plan: WipePlan, *, result: WipeResult | None = None):
    """The CLI over a planned estate, with nothing physical behind it."""

    cli = importlib.import_module("weaver_cli.main")
    monkeypatch.setattr(cli, "_resolve_workspace", lambda _args: WORKSPACE)
    planned: list = []
    executed: list = []

    def plan_wipe(targets, **kwargs):
        planned.append((tuple(targets), kwargs))
        return plan

    def wipe(*_args, **kwargs):
        executed.append(kwargs)
        return result if result is not None else _result(plan)

    monkeypatch.setattr("weaver.plan_wipe", plan_wipe)
    monkeypatch.setattr("weaver.wipe", wipe)
    return cli, planned, executed


# --- the grammar ---------------------------------------------------------------


@weaver_test()
def test_parser_uses_the_shared_typed_target_grammar():
    args = build_parser().parse_args(
        ["wipe", "Lakehouse/Shared", "Warehouse/Shared", "--workspace", "Analytics"]
    )

    assert args.targets == ["Lakehouse/Shared", "Warehouse/Shared"]


@weaver_test()
def test_removed_target_switches_are_rejected():
    with pytest.raises(SystemExit):
        build_parser().parse_args(
            ["wipe", "--lakehouse", "Sales", "--workspace", ".local"]
        )


@weaver_test()
def test_unbinding_is_reached_through_wipe_and_not_a_command_of_its_own():
    """Removing catalogue claims is part of clearing a target, not a verb.

    ``unbind_catalogue_claims`` is still the operation, and ``--unbind``
    selects it. What is gone is a separate command that removed claims for a
    target it never looked at.
    """

    with pytest.raises(SystemExit):
        build_parser().parse_args(["unbind", "Lakehouse/Sales"])


@weaver_test()
def test_the_catalogue_to_unbind_from_is_the_one_the_command_resolved():
    """``--unbind-from`` named a second catalogue for one command to reach.

    ``--catalogue`` and workspace configuration already say which catalogue a
    command means, and a wipe removing claims from a different one than it
    resolved was two answers to one question.
    """

    with pytest.raises(SystemExit):
        build_parser().parse_args(["wipe", "Lakehouse/Sales", "--unbind-from", "W"])


@weaver_test()
def test_naming_no_target_is_the_whole_estate():
    assert build_parser().parse_args(["wipe", "--workspace", "Demo"]).targets == []


@weaver_test()
def test_unbind_is_an_option_of_its_own():
    args = build_parser().parse_args(["wipe", "Lakehouse/Sales", "--unbind"])

    assert args.unbind is True
    assert build_parser().parse_args(["wipe"]).unbind is False


# --- the plan reaches the operation --------------------------------------------


@weaver_test()
def test_the_selection_and_the_disposition_both_reach_the_planner(monkeypatch):
    _cli, planned, _executed = _wired(
        monkeypatch,
        _plan("Lakehouse/Sales", action=UNBIND, unbound=("Lakehouse/Sales",)),
    )

    assert main(["wipe", "Lakehouse/Sales", "--unbind", "--yes"]) == 0

    ((targets, passed),) = planned
    assert targets == ("Lakehouse/Sales",)
    assert passed["unbind"] is True
    # The CLI hands the operation a Session rather than a resolved Workspace.
    assert passed["session"].workspace is WORKSPACE
    assert "workspace" not in passed


@weaver_test()
def test_the_plan_shown_is_the_plan_executed(monkeypatch, capsys):
    """Nothing is discovered again after the question has been answered."""

    plan = _plan("Lakehouse/Landing", "Warehouse/Weaver")
    cli, planned, executed = _wired(monkeypatch, plan)
    monkeypatch.setattr(cli, "can_prompt", lambda *_a, **_k: True)
    monkeypatch.setattr(cli, "confirm", lambda *_a, **_k: True)

    assert main(["wipe", "Lakehouse/Landing"]) == 0

    assert len(planned) == 1
    assert [passed["plan"] for passed in executed] == [plan]
    assert "Wipe on Analytics" in capsys.readouterr().out


@weaver_test()
def test_an_authorised_wipe_plans_once_and_asks_nothing(monkeypatch):
    cli, planned, executed = _wired(monkeypatch, _plan("Lakehouse/Sales"))
    monkeypatch.setattr(
        cli, "confirm", lambda *_a, **_k: pytest.fail("the question was asked")
    )

    assert main(["wipe", "Lakehouse/Sales", "--yes"]) == 0
    assert len(planned) == 1 and len(executed) == 1


@weaver_test()
def test_a_declined_wipe_empties_nothing(monkeypatch, capsys):
    cli, _planned, executed = _wired(monkeypatch, _plan("Lakehouse/Sales"))
    monkeypatch.setattr(cli, "can_prompt", lambda *_a, **_k: True)
    monkeypatch.setattr(cli, "confirm", lambda *_a, **_k: False)

    assert main(["wipe", "Lakehouse/Sales"]) == 1
    assert executed == []
    assert "Cancelled." in capsys.readouterr().out


# --- the interaction policy -----------------------------------------------------


@weaver_test()
def test_a_non_interactive_wipe_without_yes_refuses_before_mutation(
    monkeypatch, capsys
):
    _cli, planned, executed = _wired(monkeypatch, _plan("Lakehouse/Sales"))

    assert main(["wipe", "Lakehouse/Sales", "--non-interactive"]) == 1

    # The plan is resolved and rendered; nothing is emptied.
    assert len(planned) == 1 and executed == []
    printed = capsys.readouterr()
    assert "Wipe on Analytics" in printed.out
    assert "without confirmation" in printed.err


@weaver_test()
def test_a_non_interactive_wipe_with_yes_empties_the_resolved_plan(monkeypatch):
    plan = _plan("Lakehouse/Sales")
    _cli, _planned, executed = _wired(monkeypatch, plan)

    assert main(["wipe", "Lakehouse/Sales", "--non-interactive", "--yes"]) == 0
    assert [passed["plan"] for passed in executed] == [plan]


@weaver_test()
def test_a_wipe_with_no_terminal_refuses_without_yes(monkeypatch, capsys):
    _cli, _planned, executed = _wired(monkeypatch, _plan("Lakehouse/Sales"))

    assert main(["wipe", "Lakehouse/Sales"]) == 1
    assert executed == []
    assert "without confirmation" in capsys.readouterr().err


# --- what preflight shows -------------------------------------------------------


@weaver_test()
def test_a_dry_run_shows_the_estate_and_changes_nothing(monkeypatch, capsys):
    _cli, planned, executed = _wired(
        monkeypatch, _plan("Lakehouse/Landing", "Warehouse/Weaver")
    )

    assert main(["wipe", "--dry-run"]) == 0

    assert len(planned) == 1 and executed == []
    printed = capsys.readouterr().out
    assert "Wipe on Analytics" in printed
    assert "Nothing was changed." in printed


@weaver_test()
def test_preflight_exposes_no_inventory(monkeypatch, capsys):
    """The question is which estate. An inventory answers a different one."""

    _wired(monkeypatch, _plan("Lakehouse/Landing", "Warehouse/Weaver"))

    main(["wipe", "--dry-run"])

    printed = capsys.readouterr().out
    for inventory in (
        "Tables/",
        "Files/",
        "abfss://",
        "onelake",
        "dbo.",
        "shortcut:",
        "Sales.Customer",
    ):
        assert inventory not in printed


@weaver_test()
def test_a_dry_run_emits_the_plan_as_json(monkeypatch, capsys):
    _wired(monkeypatch, _plan("Lakehouse/Landing", "Warehouse/Weaver"))

    main(["wipe", "--dry-run", "--json"])

    payload = json.loads(capsys.readouterr().out)
    assert payload["catalogue_action"] == REMOVE
    assert payload["targets"][-1]["catalogue"] is True


# --- what the result shows ------------------------------------------------------


@weaver_test()
def test_one_result_line_per_physical_item(monkeypatch, capsys):
    _wired(monkeypatch, _plan("Lakehouse/Landing", "Warehouse/Weaver"))

    assert main(["wipe", "--yes"]) == 0

    lines = [
        line.strip() for line in capsys.readouterr().out.splitlines() if line.strip()
    ]
    assert lines[0] == "Wipe complete"
    assert len(lines) == 3
    assert lines[1].startswith("Lakehouse/Landing")
    assert lines[2].startswith("Warehouse/Weaver")
    assert "catalogue emptied" in lines[2]


@weaver_test()
def test_a_preserved_catalogue_reads_as_preserved(monkeypatch, capsys):
    plan = _plan("Lakehouse/Landing", action=UNBIND, unbound=("Lakehouse/Landing",))
    result = _result(
        plan,
        items=(
            WipeItemResult(target="Lakehouse/Landing", outcome=EMPTIED),
            WipeItemResult(
                target="Warehouse/Weaver",
                outcome=PRESERVED,
                is_catalogue=True,
                unbound=True,
            ),
        ),
    )
    _wired(monkeypatch, plan, result=result)

    assert main(["wipe", "Lakehouse/Landing", "--unbind", "--yes"]) == 0
    assert "catalogue preserved, claims unbound" in capsys.readouterr().out


@weaver_test()
def test_the_json_result_is_the_estate_level_model(monkeypatch, capsys):
    _wired(monkeypatch, _plan("Lakehouse/Landing", "Warehouse/Weaver"))

    assert main(["wipe", "--yes", "--json"]) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["plan"]["catalogue_action"] == REMOVE
    assert [item["target"] for item in payload["items"]] == [
        "Lakehouse/Landing",
        "Warehouse/Weaver",
    ]
    assert "reports" not in payload
