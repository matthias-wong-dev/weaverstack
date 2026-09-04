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
    MISSING,
    PLANNED,
    READY,
    UNPREPARED,
    ExampleOutcome,
    FabricItemOutcome,
    InitialiseReport,
)
from weaver_cli.initialise import (
    collect,
    collect_workspace,
    equivalent_command,
    render,
    render_dry_run,
)
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


ENVIRONMENTS = ("Analytics", "Data Engineering")


def _ready(name):
    return READY


def _lists():
    return ENVIRONMENTS


@weaver_test()
def test_the_questions_fill_in_what_is_missing():
    """Workspace, catalogue, the Environment choice, the items, the example."""

    args = _parse()
    stdin = _Typed("Weaver Example\n\n2\n\nLanding\nCurated\n\n")

    assert collect(args, stdin=stdin, environments=_lists, state_of=_ready) is True
    assert args.workspace == "Weaver Example"
    assert args.catalogue == "Catalogue"
    assert args.environment == "Weaver"
    assert args.lakehouse == "Landing"
    assert args.warehouse == "Curated"
    assert args.example is True


@weaver_test()
def test_an_empty_answer_skips_an_optional_item():
    args = _parse()
    stdin = _Typed("Weaver Example\n\n2\n\n\nCurated\nn\n")

    collect(args, stdin=stdin, environments=_lists, state_of=_ready)

    assert args.lakehouse is None
    assert args.warehouse == "Curated"
    assert args.example is False


@weaver_test()
def test_a_value_already_given_is_not_asked_for_again():
    args = _parse("--workspace", "Weaver Example", "--environment", "Weaver")
    stdin = _Typed("\nLanding\n\n\n")

    collect(args, stdin=stdin, state_of=_ready)

    assert args.workspace == "Weaver Example"
    assert args.lakehouse == "Landing"


@weaver_test()
def test_interactive_asks_for_the_optional_names_too():
    args = _parse(
        "--workspace",
        "Weaver Example",
        "--environment",
        "Weaver",
        "--lakehouse",
        "Landing",
    )
    args.interactive = True
    stdin = _Typed("\nCurated\n\n")

    assert collect(args, stdin=stdin, state_of=_ready) is True
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
    collect(
        args,
        stdin=_Typed("Weaver Example\n\n2\n\nLanding\nCurated\ny\n"),
        environments=_lists,
        state_of=_ready,
    )

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
            FabricItemOutcome("Environment", "Weaver", READY, action=CREATED),
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
    assert "  Environment   Weaver             ready" in shown
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
    for word in (
        "topology",
        "provision",
        "bootstrap",
        "native definition",
        "publication",
        "staged librar",
        "desired state",
        "artefact",
    ):
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

    The suite declines a real one outside `-m fabric`, and the command opens a
    Session of its own, which acquires whatever `credential()` answers at
    construction.
    """

    def get_token(self, *scopes, **_):  # pragma: no cover - never reached
        raise AssertionError("this test asks Fabric for nothing")


class _Ready:
    """An Environment definition with Weaver in it."""

    def custom_libraries(self):
        return ()

    def external_libraries(self):
        return "dependencies:\n  - pip:\n      - weaverstack\n"


@pytest.fixture
def workspace(monkeypatch):
    """A reachable workspace holding an Environment with Weaver installed."""

    from weaver.fabric import resources

    monkeypatch.setattr("weaver.fabric.auth.credential", _Credential)

    created: list[str] = []
    held: list[_Item] = [_Item("Weaver", resources.ENVIRONMENT)]

    monkeypatch.setattr(
        resources, "find_workspace", lambda name, client=None: _Workspace()
    )
    monkeypatch.setattr(
        resources,
        "list_items",
        lambda workspace, item_type=None, client=None: tuple(
            item for item in held if item_type is None or item.type == item_type
        ),
    )
    monkeypatch.setattr(
        resources,
        "find_item",
        lambda workspace, name, *, item_type=None, client=None: (
            next(
                (
                    item
                    for item in held
                    if item.name == name
                    and (item_type is None or item.type == item_type)
                ),
                None,
            )
            or _raise_missing(name)
        ),
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
        "weaver.fabric.environment.read_definition",
        lambda item, *, client=None: _Ready(),
    )
    monkeypatch.setattr(
        "weaver.fabric.environment.publish_state",
        lambda item, *, client=None: "Success",
    )
    monkeypatch.setattr(
        "weaver.fabric.publish_environment",
        lambda *args, **kwargs: type("Published", (), {"action": "created"})(),
        raising=False,
    )
    return type("Workspace", (), {"created": created, "held": held})


def _raise_missing(name):
    from weaver.fabric.resources import ItemNotFoundError

    raise ItemNotFoundError(f"{name!r} was not found")


@weaver_test()
def test_the_explicit_command_sets_a_project_up(tmp_path, workspace, capsys):
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
    assert workspace.created == ["Warehouse/Catalogue", "Lakehouse/Landing"]
    assert (tmp_path / "workspace-config.yml").is_file()
    assert "Everything is ready." in capsys.readouterr().out


@weaver_test()
def test_a_dry_run_command_changes_nothing(tmp_path, workspace, capsys):
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
    assert workspace.created == []
    assert list(tmp_path.iterdir()) == []
    assert "No changes were made." in capsys.readouterr().out


@weaver_test()
def test_the_json_form_carries_the_whole_result(tmp_path, workspace, capsys):
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


# --- choosing the Environment --------------------------------------------------
#
# A name on its own does not say whether it is one the workspace has or one to
# make, so the choice comes first. Installing Weaver in it is the one slow thing
# a run does, so it is one question asked before anything changes.


@weaver_test()
def test_an_existing_environment_is_chosen_from_the_workspaces_own():
    args = _parse("--workspace", "Weaver Example", "--lakehouse", "Landing")
    args.interactive = True
    stdin = _Typed("\n\n1\n2\n\n\n\n")

    collect(args, stdin=stdin, environments=_lists, state_of=_ready)

    assert args.environment == "Data Engineering"
    assert args.install_weaver is False


@weaver_test()
def test_a_new_environment_is_named(capsys):
    args = _parse("--workspace", "Weaver Example", "--lakehouse", "Landing")
    args.interactive = True
    stdin = _Typed("\n\n2\nAnalytics Runtime\n\n\n")

    collect(args, stdin=stdin, environments=_lists, state_of=_ready)

    assert args.environment == "Analytics Runtime"
    shown = capsys.readouterr().out
    assert "1. Use an existing Environment" in shown
    assert "2. Create a new Environment" in shown


@weaver_test()
def test_a_workspace_with_no_environments_offers_only_a_new_one(capsys):
    args = _parse("--workspace", "Weaver Example", "--lakehouse", "Landing")
    args.interactive = True
    stdin = _Typed("\n\n\n\n\n")

    collect(args, stdin=stdin, environments=lambda: (), state_of=_ready)

    assert args.environment == "Weaver"
    assert "no Environments yet" in capsys.readouterr().out


@weaver_test()
def test_installing_weaver_is_one_question_asked_once(capsys):
    args = _parse(
        "--workspace", "Weaver Example", "--environment", "Weaver", "--lakehouse", "L"
    )
    stdin = _Typed("y\n")

    collect(args, stdin=stdin, state_of=lambda name: MISSING)

    assert args.install_weaver is True
    shown = capsys.readouterr().out
    assert "Environment 'Weaver' will be created and Weaver will be installed" in shown
    assert "about 5 minutes" in shown
    assert shown.count("[Y/n]") == 1


@weaver_test()
def test_an_environment_without_weaver_says_what_is_needed(capsys):
    args = _parse(
        "--workspace",
        "Weaver Example",
        "--environment",
        "Data Engineering",
        "--lakehouse",
        "L",
    )

    collect(args, stdin=_Typed("y\n"), state_of=lambda name: UNPREPARED)

    shown = capsys.readouterr().out
    assert "Weaver needs to be installed in the Fabric Environment" in shown
    assert "about 5 minutes" in shown


@weaver_test()
def test_declining_the_installation_leaves_the_run_without_consent():
    args = _parse(
        "--workspace", "Weaver Example", "--environment", "Weaver", "--lakehouse", "L"
    )

    collect(args, stdin=_Typed("n\n"), state_of=lambda name: MISSING)

    assert args.install_weaver is False


@weaver_test()
def test_a_ready_environment_is_not_asked_about(capsys):
    args = _parse(
        "--workspace", "Weaver Example", "--environment", "Weaver", "--lakehouse", "L"
    )

    assert collect(args, stdin=_Typed(""), state_of=_ready) is False
    assert capsys.readouterr().out == ""


@weaver_test()
def test_a_dry_run_asks_nothing_about_installing(capsys):
    """It changes nothing, so the installation is reported and not asked about."""

    args = _parse(
        "--workspace",
        "Weaver Example",
        "--environment",
        "Weaver",
        "--lakehouse",
        "L",
        "--dry-run",
    )

    assert collect(args, stdin=_Typed(""), state_of=lambda name: MISSING) is False
    assert args.install_weaver is False


@weaver_test()
def test_the_workspace_is_asked_for_before_anything_that_needs_it():
    """Which Environments there are is a question about a workspace."""

    args = _parse()

    assert collect_workspace(args, stdin=_Typed("Weaver Example\n")) is True
    assert args.workspace == "Weaver Example"


@weaver_test()
def test_no_input_leaves_the_workspace_unasked():
    args = _parse()

    assert collect_workspace(args, ask=False, stdin=_Typed("Weaver Example\n")) is False
    assert args.workspace is None


@weaver_test()
def test_publishing_is_not_a_command_line_option():
    """The user chooses an Environment; how Weaver gets into it is Weaver's."""

    listed = build_parser().format_help()

    assert "--no-publish-environment" not in listed
    assert "publish" not in _initialise_help()


def _initialise_help() -> str:
    import contextlib
    import io

    parser = build_parser()
    initialise = parser._subparsers._group_actions[0].choices["initialise"]
    stream = io.StringIO()
    with contextlib.redirect_stdout(stream):
        initialise.print_help()
    return stream.getvalue()


@weaver_test()
def test_a_missing_environment_without_a_terminal_fails_before_anything_changes(
    tmp_path, workspace, capsys
):
    from weaver_cli.main import main

    workspace.held.clear()

    status = main(
        [
            "initialise",
            str(tmp_path),
            "--workspace",
            "Weaver Example",
            "--warehouse",
            "Curated",
            "--no-example",
            "--no-input",
        ]
    )

    assert status == 1
    assert workspace.created == []
    assert list(tmp_path.iterdir()) == []
    assert "does not exist" in capsys.readouterr().err


@weaver_test()
def test_a_list_of_one_is_taken_rather_than_asked(capsys):
    """A workspace with one Environment is not offered a choice between them."""

    args = _parse("--workspace", "Weaver Example", "--lakehouse", "Landing")
    args.interactive = True
    stdin = _Typed("\n\n1\n\n\n")

    collect(args, stdin=stdin, environments=lambda: ("weaver",), state_of=_ready)

    assert args.environment == "weaver"
    shown = capsys.readouterr().out
    assert "Choose an Environment: 1" in shown
    assert "Choose an Environment [1]" not in shown


@weaver_test()
def test_running_out_of_answers_stops_rather_than_asking_again():
    """End of input is not an answer, so the same question is not repeated."""

    args = _parse()
    args.interactive = True

    with pytest.raises(CommandError) as raised:
        collect(args, stdin=_Typed("Weaver Example\n"), state_of=_ready)

    assert "answers ran out" in str(raised.value)


@weaver_test()
def test_interactive_asks_even_where_there_is_no_terminal():
    """`--interactive` is the forcing flag, so it reads whatever it was given."""

    args = _parse()
    args.interactive = True

    collect_workspace(args, stdin=io.StringIO("Weaver Example\n"))

    assert args.workspace == "Weaver Example"
