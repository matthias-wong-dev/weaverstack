"""The CLI is a confirmation and rendering adapter for public mirror.

The order it runs things in is the safety property: the scope is settled and
the source is read before anybody is asked, and what they answer names every
Warehouse the run empties.
"""

from __future__ import annotations

import importlib

import pytest
from support.weaver_test import weaver_test
from support.workspaces import given_workspace

from weaver.errors import CommandError
from weaver.operations.mirror import (
    MirrorItem,
    MirrorPlan,
    MirrorResult,
    ResolvedMirror,
)
from weaver.workspaces import CatalogueRef
from weaver_cli import main
from weaver_cli.main import build_parser


def _result() -> MirrorResult:
    return MirrorResult(
        workspace="Analytics",
        source_catalogue="Warehouse/Weaver",
        destination_catalogue="Warehouse/Weaver_Dev",
        wiped=("Warehouse/Weaver_Dev",),
        copied={"Installation": 2, "Registry": 11},
        uncopied=("Log", "LoadStatistic"),
    )


def _workspace():
    from dataclasses import replace

    return replace(
        given_workspace(catalogue="Warehouse/Weaver_Dev"),
        mirror=CatalogueRef(workspace=None, name="Weaver"),
    )


def _plan() -> MirrorPlan:
    return MirrorPlan(
        workspace=_workspace(),
        source=CatalogueRef(workspace=None, name="Weaver"),
        destination=CatalogueRef(workspace=None, name="Weaver_Dev"),
    )


def _wired(monkeypatch, *, order=None, items=()):
    """The CLI with each public step recorded rather than performed."""

    cli = importlib.import_module("weaver_cli.main")
    monkeypatch.setattr(cli, "_resolve_workspace", lambda _args: _workspace())
    steps = order if order is not None else []
    plan = _plan()
    calls: dict[str, object] = {"plan": plan, "steps": steps, "items": items}

    def plan_mirror(items=None, **kwargs):
        steps.append("plan")
        calls["planned"] = (items, kwargs)
        return plan

    def check_mirror(supplied, **kwargs):
        steps.append("check")
        calls["checked"] = supplied
        settled = ResolvedMirror(plan=supplied, items=calls["items"])
        calls["resolved"] = settled
        return settled

    def mirror(items=None, **kwargs):
        steps.append("mirror")
        calls["mirrored"] = kwargs
        return calls.get("result") or _result()

    monkeypatch.setattr("weaver.plan_mirror", plan_mirror)
    monkeypatch.setattr("weaver.check_mirror", check_mirror)
    monkeypatch.setattr("weaver.mirror", mirror)
    return calls


# --- the grammar --------------------------------------------------------------


@weaver_test()
def test_item_selection_uses_the_grammar_build_uses():
    args = build_parser().parse_args(
        [
            "mirror",
            "--item",
            "Warehouse/Model",
            "--item",
            "Lakehouse/Input=Lakehouse/Input_Dev",
            "--workspace",
            "Analytics",
        ]
    )

    assert args.items == ["Warehouse/Model", "Lakehouse/Input=Lakehouse/Input_Dev"]
    assert args.no_item is False


@weaver_test()
def test_both_sides_of_a_fork_are_named_the_way_configuration_names_them():
    """``--mirror`` is read from and ``--catalogue`` written to.

    One vocabulary across configuration and the command, so the pair reads the
    same way wherever it is written.
    """

    args = build_parser().parse_args(
        [
            "mirror",
            "--mirror",
            "Warehouse/Catalogue",
            "--catalogue",
            "Warehouse/DEV_Catalogue",
            "--workspace",
            "35 South Data",
            "--no-item",
        ]
    )

    assert args.mirror_source == "Warehouse/Catalogue"
    assert args.catalogue == "Warehouse/DEV_Catalogue"


@weaver_test()
def test_the_retired_source_switch_is_rejected():
    """``--source`` named the same thing in a second vocabulary."""

    with pytest.raises(SystemExit):
        build_parser().parse_args(
            ["mirror", "--source", "Warehouse/Catalogue", "--workspace", "A"]
        )


@weaver_test()
def test_naming_no_item_is_a_choice_rather_than_an_omission():
    """Omitting ``--item`` selects every configured target, as ``build`` does.

    ``--no-item`` is how a fork of the catalogue alone is asked for.
    """

    args = build_parser().parse_args(["mirror", "--no-item", "--workspace", "A"])

    assert args.no_item is True
    assert args.items is None


@weaver_test()
def test_a_catalogue_fork_alone_starts_no_spark_session():
    """It empties a Warehouse, rebuilds ``_`` and copies rows between two."""

    from weaver.sessions.requirements import AUTH, LIVY, ONELAKE, RESOLVER, TDS

    args = build_parser().parse_args(["mirror", "--no-item", "--workspace", "A"])
    required = args.requires(args)

    assert LIVY not in required
    assert ONELAKE not in required
    assert {AUTH, RESOLVER, TDS} <= required


@weaver_test()
def test_a_mirrored_lakehouse_asks_for_spark_and_a_warehouse_does_not():
    """A Lakehouse shortcut is not finished until Spark can read it."""

    from weaver.sessions.requirements import LIVY, ONELAKE, TDS

    lake = build_parser().parse_args(
        ["mirror", "--item", "Lakehouse/Input", "--workspace", "A"]
    )
    house = build_parser().parse_args(
        ["mirror", "--item", "Warehouse/Model", "--workspace", "A"]
    )

    assert {LIVY, ONELAKE, TDS} <= lake.requires(lake)
    assert LIVY not in house.requires(house)


# --- the order, which is what keeps a typo from emptying a Warehouse ---------


@weaver_test()
def test_the_source_is_proved_before_the_question_is_asked(monkeypatch, capsys):
    """A misspelled ``--mirror`` fails while the destination is still intact."""

    cli = importlib.import_module("weaver_cli.main")
    monkeypatch.setattr(cli, "_resolve_workspace", lambda _args: _workspace())
    order: list[str] = []
    monkeypatch.setattr("weaver.plan_mirror", lambda *_a, **_k: _plan())
    monkeypatch.setattr(cli.sys.stdin, "isatty", lambda: True)
    monkeypatch.setattr(
        "builtins.input", lambda _prompt: pytest.fail("the question was asked")
    )

    def check(_plan, **_kwargs):
        order.append("check")
        raise CommandError("mirror could not read Warehouse/Catalgoue")

    monkeypatch.setattr("weaver.check_mirror", check)
    monkeypatch.setattr("weaver.mirror", lambda *_a, **_k: pytest.fail("the fork ran"))

    assert main(["mirror", "--no-item", "--mirror", "Warehouse/Catalgoue"]) != 0

    assert order == ["check"]
    assert "Catalgoue" in capsys.readouterr().err


@weaver_test()
def test_the_steps_run_in_one_order(monkeypatch, capsys):
    order: list[str] = []
    _wired(monkeypatch, order=order)

    assert main(["mirror", "--no-item", "--yes", "--workspace", "Analytics"]) == 0
    assert order == ["plan", "check", "mirror"]


@weaver_test()
def test_the_command_line_is_what_the_pair_is_resolved_from(monkeypatch):
    """Not the workspace the CLI overlaid ``--catalogue`` onto.

    Which configured value is the source and which the destination depends on
    what else is set, so the operation is given what this command line said.
    """

    calls = _wired(monkeypatch)

    main(
        [
            "mirror",
            "--no-item",
            "--yes",
            "--mirror",
            "Warehouse/Catalogue",
            "--catalogue",
            "Warehouse/DEV_Catalogue",
            "--workspace",
            "35 South Data",
        ]
    )

    _items, passed = calls["planned"]
    assert passed["mirror"] == "Warehouse/Catalogue"
    assert passed["catalogue"] == "Warehouse/DEV_Catalogue"
    assert passed["workspace"] == "35 South Data"


@weaver_test()
def test_the_fork_acts_on_the_plan_that_was_shown(monkeypatch):
    """One plan through preflight, prompt and execution."""

    calls = _wired(monkeypatch)

    main(["mirror", "--no-item", "--yes", "--workspace", "Analytics"])

    assert calls["checked"] is calls["plan"]
    assert calls["mirrored"]["plan"] is calls["resolved"]


# --- confirmation -------------------------------------------------------------


@weaver_test()
def test_a_fork_is_refused_without_confirmation(monkeypatch, capsys):
    """The destination is emptied, so it asks the way ``wipe`` asks."""

    cli = importlib.import_module("weaver_cli.main")
    calls = _wired(monkeypatch)
    monkeypatch.setattr(cli.sys.stdin, "isatty", lambda: False)
    monkeypatch.setattr(
        "weaver.mirror", lambda *_a, **_k: pytest.fail("the fork ran unconfirmed")
    )

    assert main(["mirror", "--no-item", "--workspace", "Analytics"]) == 1
    printed = capsys.readouterr()
    assert "Refusing to empty Warehouse/Weaver_Dev" in printed.err
    # The pair it names is the resolved one, not a second answer to the question.
    assert "  Warehouse/Weaver_Dev  <- Warehouse/Weaver" in printed.out


@weaver_test()
def test_the_question_names_every_warehouse_the_run_empties(monkeypatch, capsys):
    """A selected item empties its own target too, so the question says so."""

    from weaver.declaration.model import WeaverItemId

    cli = importlib.import_module("weaver_cli.main")
    _wired(
        monkeypatch,
        items=(
            MirrorItem(
                item=WeaverItemId.parse("Warehouse/Model"),
                source_target="Sales",
                destination="Sales_Dev",
            ),
        ),
    )
    monkeypatch.setattr(cli.sys.stdin, "isatty", lambda: True)
    monkeypatch.setattr("builtins.input", lambda _prompt: "y")

    assert (
        main(["mirror", "--item", "Warehouse/Model", "--workspace", "Analytics"]) == 0
    )

    printed = capsys.readouterr().out
    # One list, target first, the catalogue leading, and each row spelled the
    # same way.
    assert (
        "  Warehouse/Weaver_Dev  <- Warehouse/Weaver\n"
        "  Warehouse/Sales_Dev   <- Warehouse/Sales\n"
    ) in printed
    assert "will be emptied and rebuilt" not in printed
    assert "will be mirrored into" not in printed


@weaver_test()
def test_an_authorised_fork_reports_the_targets_it_filled(monkeypatch, capsys):
    """As dense as ``build`` and ``load``: the status and what it wrote.

    A catalogue table's row count and an item's object counts are debugging
    detail, and ``--json`` is where they are.
    """

    calls = _wired(monkeypatch)
    calls["result"] = MirrorResult(
        workspace="Analytics",
        source_catalogue="Warehouse/Weaver",
        destination_catalogue="Warehouse/Weaver_Dev",
        wiped=("Warehouse/Weaver_Dev", "Lakehouse/Input_Dev"),
        copied={"Installation": 2, "Registry": 11},
        uncopied=("Log", "LoadStatistic"),
        items=("Lakehouse/Input",),
        mirrored={"Lakehouse/Input": {"relations": 6, "files": 11}},
    )

    assert main(["mirror", "--no-item", "--yes", "--workspace", "Analytics"]) == 0

    passed = calls["mirrored"]
    # The CLI hands the operation a Session rather than a resolved Workspace.
    assert passed["session"].workspace is calls["plan"].workspace
    printed = capsys.readouterr().out
    assert printed == (
        "mirror succeeded: Warehouse/Weaver_Dev, Lakehouse/Input_Dev\n"
    )


@weaver_test()
@pytest.mark.parametrize(
    "made",
    [
        {
            "source": "Warehouse/Model",
            "target": "Warehouse/Model_Dev",
            "relations": 5,
            "programmables": 3,
        },
        {
            "source": "Lakehouse/Input",
            "target": "Lakehouse/Input_Dev",
            "relations": 6,
            "shortcuts": 4,
            "views": 2,
            "files": 11,
        },
    ],
    ids=["warehouse", "lakehouse"],
)
def test_json_carries_what_a_kind_made(monkeypatch, capsys, made):
    """What a mirror makes differs by kind, so the payload reads the result."""

    import json

    calls = _wired(monkeypatch)
    calls["result"] = MirrorResult(
        workspace="Analytics",
        source_catalogue="Warehouse/Weaver",
        destination_catalogue="Warehouse/Weaver_Dev",
        wiped=("Warehouse/Weaver_Dev", made["target"]),
        items=("Item",),
        mirrored={"Item": made},
    )

    argv = ["mirror", "--no-item", "--yes", "--json", "--workspace", "Analytics"]

    assert main(argv) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["mirrored"]["Item"] == made
    assert "borrow" not in json.dumps(payload).casefold()


@weaver_test()
def test_selecting_items_and_no_item_together_is_refused(monkeypatch):
    cli = importlib.import_module("weaver_cli.main")
    monkeypatch.setattr(cli, "_resolve_workspace", lambda _args: _workspace())

    with pytest.raises(CommandError, match="cannot be used together"):
        cli.handle_mirror(
            build_parser().parse_args(
                ["mirror", "--item", "Warehouse/Model", "--no-item", "--yes"]
            )
        )
