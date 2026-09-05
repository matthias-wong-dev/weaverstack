"""Required destination, revisable wizard and setup rendering."""

import importlib
import io
import json

import pytest
from support.weaver_test import weaver_test

from weaver.errors import CommandError
from weaver.initialise import FabricItemOutcome, InitialiseReport
from weaver_cli.initialise import collect, equivalent_command, render
from weaver_cli.main import build_parser


class Typed(io.StringIO):
    def isatty(self):
        return True


def parse(*args):
    return build_parser().parse_args(["initialise", *args])


@weaver_test()
def test_destination_is_required_and_current_directory_is_explicit():
    with pytest.raises(SystemExit):
        parse()
    assert parse(".").repository == "."
    assert build_parser().parse_args(["initialize", "project"]).repository == "project"


@weaver_test()
def test_wizard_explains_choices_and_defaults_to_source_only_and_deferred(capsys):
    args = parse("project", "--workspace", "Analytics")
    collect(args, stdin=Typed("\n\nLanding\n\n\n1\n\n"))
    assert args.catalogue == "Catalogue" and args.environment == "Weaver"
    assert args.lakehouse == "Landing" and args.warehouse is None
    assert args.example is False and args.publish_environment is False
    text = capsys.readouterr().out
    for phrase in (
        "Warehouse where Weaver keeps",
        "definition is kept in this project",
        "Press Enter",
        "Change an answer",
        "Publish the Environment now? [y/N]",
    ):
        assert phrase in text
    assert "[skip]" not in text and "[Y/n]" not in text


@weaver_test()
def test_wizard_can_edit_a_typo_before_continue():
    args = parse(
        "project",
        "--workspace",
        "Analytics",
        "--catalogue",
        "Catalogue",
        "--environment",
        "Weaver",
        "--lakehouse",
        "Ladning",
        "--warehouse",
        "Curated",
        "--no-example",
    )
    collect(args, stdin=Typed("2\n5\nLanding\n1\n\n"))
    assert args.lakehouse == "Landing"
    assert not getattr(args, "cancelled", False)


@weaver_test()
def test_cancel_returns_without_publication():
    args = parse(
        "project",
        "--workspace",
        "Analytics",
        "--catalogue",
        "Catalogue",
        "--environment",
        "Weaver",
        "--lakehouse",
        "Landing",
        "--warehouse",
        "Curated",
        "--no-example",
    )
    collect(args, stdin=Typed("3\n"))
    assert args.cancelled and not args.publish_environment


@weaver_test()
def test_existing_item_hints_and_environment_selection(capsys):
    args = parse("project", "--workspace", "Analytics")
    collect(
        args,
        stdin=Typed("\n1\n1\nLanding\nCurated\ny\n1\nn\n"),
        environments=lambda: ("Runtime",),
        items=lambda kind: ("Landing",) if kind == "Lakehouse" else ("Curated",),
    )
    assert args.environment == "Runtime" and args.example
    assert "Existing: Landing" in capsys.readouterr().out


@weaver_test()
def test_invalid_lakehouse_answer_is_retried(capsys):
    args = parse("project", "--workspace", "Analytics")
    collect(args, stdin=Typed("\n\n1\nLanding\n\n\n1\n\n"))
    assert args.lakehouse == "Landing"
    assert "not a valid Fabric Lakehouse name" in capsys.readouterr().out


@weaver_test()
def test_no_input_validates_without_prompts(capsys):
    args = parse(
        "project", "--workspace", "Analytics", "--warehouse", "Curated", "--no-input"
    )
    assert collect(args, ask=False) is False
    assert not capsys.readouterr().out
    with pytest.raises(CommandError):
        collect(parse("project"), ask=False)


@weaver_test()
def test_exhausted_input_stops_the_wizard():
    with pytest.raises(CommandError, match="answers ran out"):
        collect(parse("project"), stdin=Typed(""))


@weaver_test()
def test_equivalent_command_includes_publication_and_is_parseable():
    import shlex

    args = parse(
        "my project",
        "--workspace",
        "Analytics",
        "--warehouse",
        "Curated",
        "--publish-environment",
        "--example",
    )
    replay = build_parser().parse_args(shlex.split(equivalent_command(args))[1:])
    assert (
        replay.repository == "my project"
        and replay.publish_environment
        and replay.no_input
    )


@weaver_test()
def test_deferred_render_names_normal_publish_and_next_commands(capsys):
    render(
        InitialiseReport(
            repository="project",
            workspace="Analytics",
            resources=(FabricItemOutcome("Environment", "Weaver", "created"),),
        )
    )
    text = capsys.readouterr().out
    assert "publication was deferred" in text
    assert (
        "weaver fabric environment publish --path Environment/Weaver.Environment"
        in text
    )
    assert "--dev" not in text
    assert all("weaver " + name in text for name in ("build", "load", "test"))


@weaver_test()
def test_cli_passes_source_and_publication_flags_to_core(monkeypatch, tmp_path, capsys):
    cli = importlib.import_module("weaver_cli.main")
    calls = []
    monkeypatch.setattr(cli, "_prefer_desktop_credential", lambda: None)
    monkeypatch.setattr(
        cli.weaver,
        "initialise",
        lambda path, **kwargs: (
            calls.append((path, kwargs))
            or InitialiseReport(repository=str(path), workspace="Analytics")
        ),
    )
    args = parse(
        str(tmp_path),
        "--workspace",
        "Analytics",
        "--warehouse",
        "Curated",
        "--example",
        "--no-input",
        "--json",
    )
    assert cli.handle_initialise(args) == 0
    assert calls[0][1]["example"] and calls[0][1]["publish_environment"] is False
    assert json.loads(capsys.readouterr().out)["environment_publication"] == "deferred"


@weaver_test()
def test_cancel_in_handler_never_calls_initialise(monkeypatch, capsys):
    from contextlib import nullcontext
    from types import SimpleNamespace

    from weaver.sessions.testing import TestSession
    from weaver.workspaces import Workspace

    cli = importlib.import_module("weaver_cli.main")
    args = parse(
        "project",
        "--workspace",
        "Analytics",
        "--catalogue",
        "Catalogue",
        "--environment",
        "Weaver",
        "--lakehouse",
        "Landing",
        "--warehouse",
        "Curated",
        "--no-example",
    )
    monkeypatch.setattr(cli, "_prefer_desktop_credential", lambda: None)
    monkeypatch.setattr(cli.sys, "stdin", Typed("3\n"))
    monkeypatch.setattr(
        cli,
        "_running_session",
        lambda *a: nullcontext(
            TestSession(
                workspace=Workspace(workspace="Analytics"),
                resolver=SimpleNamespace(client=object()),
            )
        ),
    )
    monkeypatch.setattr(
        cli, "_initialise_once", lambda *a, **k: pytest.fail("cancelled wizard mutated")
    )
    assert cli.handle_initialise(args) == 0
    assert "cancelled" in capsys.readouterr().out
