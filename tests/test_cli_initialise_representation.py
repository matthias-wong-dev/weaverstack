"""What the initialise command asks, and what it shows.

The questions collect values and nothing else: both spellings of the command
reach the same core request, so the guided form is a convenience over the
explicit one and never a second way of setting a project up.

The output is the first Weaver text most users read. It is checked here for the
words it uses as much as for the values it carries.
"""

from __future__ import annotations

import io

import pytest
from support.weaver_test import weaver_test

from weaver.errors import CommandError
from weaver.initialise import (
    CREATED,
    EXISTING,
    PLANNED,
    PUBLISHED,
    ExampleOutcome,
    FabricItemOutcome,
    InitialiseReport,
)
from weaver_cli.initialise import collect, equivalent_command, render, render_dry_run
from weaver_cli.main import build_parser


def _parse(*argv):
    return build_parser().parse_args(["initialise", *argv])


class _Typed(io.StringIO):
    """Answers a person would type, from a stream that says it is a terminal."""

    def isatty(self) -> bool:
        return True


# --- parsing -------------------------------------------------------------------


@weaver_test()
def test_the_explicit_form_carries_every_name():
    args = _parse(
        ".",
        "--workspace",
        "Weaver Example",
        "--catalogue",
        "Catalogue",
        "--environment",
        "Weaver",
        "--lakehouse",
        "Landing",
        "--warehouse",
        "Curated",
        "--example",
    )

    assert args.repository == "."
    assert args.workspace == "Weaver Example"
    assert args.catalogue == "Catalogue"
    assert args.environment == "Weaver"
    assert args.lakehouse == "Landing"
    assert args.warehouse == "Curated"
    assert args.example is True


@weaver_test()
def test_the_repository_defaults_to_the_current_directory():
    assert _parse("--workspace", "Weaver Example").repository is None


@weaver_test()
def test_the_example_is_unanswered_until_it_is_asked_for():
    """`None` is what lets the question be put; `--no-example` answers it."""

    assert _parse("--workspace", "W").example is None
    assert _parse("--workspace", "W", "--no-example").example is False


@weaver_test()
def test_the_american_spelling_reaches_the_same_command():
    parser = build_parser()

    british = parser.parse_args(["initialise", "--workspace", "W"])
    american = parser.parse_args(["initialize", "--workspace", "W"])

    assert american.handler is british.handler


# --- collecting ----------------------------------------------------------------


@weaver_test()
def test_a_complete_command_line_asks_nothing():
    args = _parse("--workspace", "Weaver Example", "--lakehouse", "Landing")
    stdin = _Typed("")

    assert collect(args, stdin=stdin) is False


@weaver_test()
def test_the_questions_fill_in_what_is_missing():
    args = _parse()
    stdin = _Typed("Weaver Example\n\n\nLanding\nCurated\n\n")

    assert collect(args, stdin=stdin) is True
    assert args.workspace == "Weaver Example"
    assert args.catalogue == "Catalogue"
    assert args.environment == "Weaver"
    assert args.lakehouse == "Landing"
    assert args.warehouse == "Curated"
    assert args.example is True


@weaver_test()
def test_an_empty_answer_skips_an_optional_item():
    args = _parse()
    stdin = _Typed("Weaver Example\n\n\n\nCurated\nn\n")

    collect(args, stdin=stdin)

    assert args.lakehouse is None
    assert args.warehouse == "Curated"
    assert args.example is False


@weaver_test()
def test_a_value_already_given_is_not_asked_for_again():
    args = _parse("--workspace", "Weaver Example")
    stdin = _Typed("\n\nLanding\n\n\n")

    collect(args, stdin=stdin)

    assert args.workspace == "Weaver Example"
    assert args.lakehouse == "Landing"


@weaver_test()
def test_interactive_asks_even_when_nothing_is_missing():
    args = _parse("--workspace", "Weaver Example", "--lakehouse", "Landing")
    args.interactive = True
    stdin = _Typed("\n\nCurated\n\n")

    assert collect(args, stdin=stdin) is True
    assert args.warehouse == "Curated"


@weaver_test()
def test_no_input_never_asks_and_names_what_is_missing():
    args = _parse()
    stdin = _Typed("Weaver Example\n")

    with pytest.raises(CommandError) as raised:
        collect(args, ask=False, stdin=stdin)

    message = str(raised.value)
    assert "--workspace" in message
    assert "--lakehouse or --warehouse" in message
    assert args.workspace is None


@weaver_test()
def test_a_pipe_is_not_asked_either():
    """Nobody is there to answer, so the options are named instead."""

    args = _parse()

    with pytest.raises(CommandError, match="--workspace"):
        collect(args, stdin=io.StringIO(""))


@weaver_test()
def test_a_project_needs_a_lakehouse_or_a_warehouse():
    args = _parse("--workspace", "Weaver Example")

    with pytest.raises(CommandError, match="--lakehouse or --warehouse"):
        collect(args, ask=False)


# --- showing the reproducible form ---------------------------------------------


@weaver_test()
def test_the_equivalent_command_repeats_the_answers():
    args = _parse()
    collect(args, stdin=_Typed("Weaver Example\n\n\nLanding\nCurated\ny\n"))

    assert equivalent_command(args) == (
        'weaver initialise --workspace "Weaver Example" --catalogue Catalogue '
        "--environment Weaver --lakehouse Landing --warehouse Curated --example"
    )


# --- rendering -----------------------------------------------------------------


def _report(**kwargs):
    defaults = {
        "repository": "./repository",
        "workspace": "Weaver Example",
        "resources": (
            FabricItemOutcome("Catalogue", "Catalogue", CREATED),
            FabricItemOutcome("Environment", "Weaver", PUBLISHED),
            FabricItemOutcome("Lakehouse", "Landing", EXISTING),
            FabricItemOutcome("Warehouse", "Curated", CREATED),
        ),
        "files": ("workspace-config.yml",),
        "example": ExampleOutcome(),
    }
    defaults.update(kwargs)
    return InitialiseReport(**defaults)


@weaver_test()
def test_the_summary_reads_as_a_person_would_say_it(capsys):
    render(
        _report(
            example=ExampleOutcome(
                generated=True, build="succeeded", load="succeeded", test="passed"
            )
        )
    )

    shown = capsys.readouterr().out
    assert "Everything is ready." in shown
    assert "Your Weaver project is in ./repository." in shown
    assert "  Catalogue     Catalogue          created" in shown
    assert "  Lakehouse     Landing            already exists" in shown
    assert "built, loaded, tested" in shown
    assert "weaver build\n" in shown
    assert "weaver load\n" in shown
    assert "weaver test" in shown


@weaver_test()
def test_the_summary_uses_no_implementation_words(capsys):
    """First-run output is product vocabulary. These belong in design docs."""

    render(_report())

    shown = capsys.readouterr().out.lower()
    for word in ("topology", "provision", "bootstrap", "native definition", "artefact"):
        assert word not in shown


@weaver_test()
def test_a_dry_run_says_nothing_was_changed(capsys):
    render_dry_run(
        _report(
            resources=(
                FabricItemOutcome("Catalogue", "Catalogue", PLANNED),
                FabricItemOutcome("Lakehouse", "Landing", EXISTING),
            ),
            example=ExampleOutcome(generated=True),
            dry_run=True,
        )
    )

    shown = capsys.readouterr().out
    assert "Here's what will be set up:" in shown
    assert "  Catalogue     Catalogue          create" in shown
    assert "  Lakehouse     Landing            already exists" in shown
    assert "A small Sales example will also be added." in shown
    assert "No changes were made." in shown


@weaver_test()
def test_the_columns_line_up_without_dot_leaders(capsys):
    render(_report())

    for line in capsys.readouterr().out.splitlines():
        assert "...." not in line


# --- the command, end to end ---------------------------------------------------


class _Workspace:
    id = "ws-1"
    name = "Weaver Example"


class _Item:
    def __init__(self, name, item_type):
        self.id = f"id-{name}"
        self.name = name
        self.type = item_type
        self.workspace_id = "ws-1"


class _Credential:
    """A credential the Session can hold. Nothing here asks it for a token.

    The suite refuses a real one outside `-m fabric`, and the command opens a
    Session of its own, which acquires whatever `credential()` answers at
    construction.
    """

    def get_token(self, *scopes, **_):  # pragma: no cover - never reached
        raise AssertionError("this test asks Fabric for nothing")


@pytest.fixture
def workspace_holding_nothing(monkeypatch):
    """A reachable workspace with no items in it."""

    from weaver.fabric import resources

    monkeypatch.setattr("weaver.fabric.auth.credential", _Credential)

    created: list[str] = []
    held: list[_Item] = []

    monkeypatch.setattr(
        resources, "find_workspace", lambda name, client=None: _Workspace()
    )
    monkeypatch.setattr(
        resources,
        "list_items",
        lambda workspace, item_type=None, client=None: tuple(held),
    )

    def create(kind):
        def make(workspace, name, *, client=None):
            created.append(f"{kind}/{name}")
            held.append(_Item(name, kind))
            return held[-1]

        return make

    monkeypatch.setattr(resources, "create_lakehouse", create(resources.LAKEHOUSE))
    monkeypatch.setattr(resources, "create_warehouse", create(resources.WAREHOUSE))
    monkeypatch.setattr(
        "weaver.fabric.publish_environment",
        lambda *args, **kwargs: type("Published", (), {"published": True})(),
        raising=False,
    )
    return created


@weaver_test()
def test_the_explicit_command_sets_a_project_up(
    tmp_path, workspace_holding_nothing, capsys
):
    from weaver_cli.main import main

    status = main(
        [
            "initialise",
            str(tmp_path),
            "--workspace",
            "Weaver Example",
            "--lakehouse",
            "Landing",
            "--no-example",
        ]
    )

    assert status == 0
    assert workspace_holding_nothing == ["Warehouse/Catalogue", "Lakehouse/Landing"]
    assert (tmp_path / "workspace-config.yml").is_file()
    assert "Everything is ready." in capsys.readouterr().out


@weaver_test()
def test_a_dry_run_command_changes_nothing(tmp_path, workspace_holding_nothing, capsys):
    from weaver_cli.main import main

    status = main(
        [
            "initialise",
            str(tmp_path),
            "--workspace",
            "Weaver Example",
            "--warehouse",
            "Curated",
            "--no-example",
            "--dry-run",
        ]
    )

    assert status == 0
    assert workspace_holding_nothing == []
    assert list(tmp_path.iterdir()) == []
    assert "No changes were made." in capsys.readouterr().out


@weaver_test()
def test_the_json_form_carries_the_whole_result(
    tmp_path, workspace_holding_nothing, capsys
):
    import json

    from weaver_cli.main import main

    main(
        [
            "initialise",
            str(tmp_path),
            "--workspace",
            "Weaver Example",
            "--warehouse",
            "Curated",
            "--no-example",
            "--json",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert payload["workspace"] == "Weaver Example"
    assert payload["next_commands"] == ["weaver build", "weaver load", "weaver test"]
    assert {row["role"] for row in payload["resources"]} == {
        "Catalogue",
        "Environment",
        "Warehouse",
    }


@weaver_test()
def test_no_input_with_nothing_named_names_the_options(tmp_path, capsys):
    from weaver_cli.main import main

    status = main(["initialise", str(tmp_path), "--no-input"])

    assert status == 1
    assert "--workspace" in capsys.readouterr().err


@weaver_test()
def test_asking_and_never_asking_cannot_both_be_requested(tmp_path, capsys):
    from weaver_cli.main import main

    status = main(["initialise", str(tmp_path), "--interactive", "--no-input"])

    assert status == 1
    assert "--interactive asks and --no-input never does." in capsys.readouterr().err


@weaver_test()
def test_the_command_list_carries_one_spelling(capsys):
    """argparse renders whatever `help` holds, and that includes SUPPRESS."""

    listed = build_parser().format_help()

    assert "Set up a new Weaver project and the Fabric items it needs." in listed
    assert "initialize" not in listed
    assert "SUPPRESS" not in listed
