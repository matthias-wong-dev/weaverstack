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
def test_project_folder_is_optional_for_wizard_and_explicit_for_unattended_setup():
    assert parse().repository is None
    assert parse("--project-folder", ".").repository == "."
    assert (
        build_parser()
        .parse_args(["initialize", "--project-folder", "project"])
        .repository
        == "project"
    )
    with pytest.raises(SystemExit):
        parse("project")
    with pytest.raises(CommandError, match="--project-folder"):
        collect(
            parse("--workspace", "Analytics", "--warehouse", "Curated", "--no-input"),
            ask=False,
        )


@weaver_test()
def test_wizard_explains_choices_and_defaults_to_source_only_and_deferred(capsys):
    args = parse("--project-folder", "project", "--workspace", "Analytics")
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
        "--project-folder",
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
        "--project-folder",
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
    args = parse("--project-folder", "project", "--workspace", "Analytics")
    collect(
        args,
        stdin=Typed("\n1\n1\nLanding\nCurated\ny\n1\nn\n"),
        environments=lambda workspace: ("Runtime",),
        items=lambda workspace, kind: (
            ("Landing",) if kind == "Lakehouse" else ("Curated",)
        ),
    )
    assert args.environment == "Runtime" and args.example
    assert "Existing: Landing" in capsys.readouterr().out


@weaver_test()
def test_invalid_lakehouse_answer_is_retried(capsys):
    args = parse("--project-folder", "project", "--workspace", "Analytics")
    collect(args, stdin=Typed("\n\n1\nLanding\n\n\n1\n\n"))
    assert args.lakehouse == "Landing"
    assert "not a valid Fabric Lakehouse name" in capsys.readouterr().out


@weaver_test()
def test_no_input_validates_without_prompts(capsys):
    args = parse(
        "--project-folder",
        "project",
        "--workspace",
        "Analytics",
        "--warehouse",
        "Curated",
        "--no-input",
    )
    assert collect(args, ask=False) is False
    assert not capsys.readouterr().out
    with pytest.raises(CommandError):
        collect(parse("--project-folder", "project"), ask=False)


@weaver_test()
def test_exhausted_input_stops_the_wizard():
    with pytest.raises(CommandError, match="answers ran out"):
        collect(parse("--project-folder", "project"), stdin=Typed(""))


@weaver_test()
def test_equivalent_command_includes_publication_and_is_parseable():
    import shlex

    args = parse(
        "--project-folder",
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
        "--project-folder",
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
        "--project-folder",
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


@pytest.mark.parametrize(
    "folder_answer,expected_folder",
    [("", "Production"), ("custom", "custom"), ("UAT", "UAT")],
)
@weaver_test()
def test_workspace_review_change_rediscovers_and_recollects_targets(
    monkeypatch, capsys, folder_answer, expected_folder
):
    from contextlib import nullcontext
    from types import SimpleNamespace

    from weaver.sessions.testing import TestSession

    cli = importlib.import_module("weaver_cli.main")
    args = parse()
    monkeypatch.setattr(cli, "_prefer_desktop_credential", lambda: None)
    monkeypatch.setattr(
        cli.sys,
        "stdin",
        Typed(
            f"UAT\n{folder_answer}\n\n1\n1\nUatLanding\nUatReporting\nn\n"
            "2\n2\nProduction\n"
            + ("\n" if not folder_answer else "")
            + "1\n1\nProdLanding\nProdReporting\n1\nn\n"
        ),
    )
    clients = {workspace: object() for workspace in ("UAT", "Production")}
    opened = TestSession()
    monkeypatch.setattr(
        opened,
        "resolver",
        lambda workspace: SimpleNamespace(client=clients[workspace.workspace]),
    )
    monkeypatch.setattr(cli, "_running_session", lambda *a: nullcontext(opened))
    discoveries = []

    def environments(workspace, *, client):
        assert client is clients[workspace]
        discoveries.append((workspace, "Environment"))
        return ("UatRuntime",) if workspace == "UAT" else ("ProdRuntime",)

    def items(workspace, kind, *, client):
        assert client is clients[workspace]
        discoveries.append((workspace, kind))
        return (f"{'Uat' if workspace == 'UAT' else 'Prod'}{kind}",)

    onboarding = importlib.import_module("weaver.initialise")
    monkeypatch.setattr(onboarding, "available_environments", environments)
    monkeypatch.setattr(onboarding, "available_items", items)
    requests = []

    def initialise(args, *, session):
        requests.append(
            (args.workspace, args.environment, args.lakehouse, args.warehouse)
        )
        return InitialiseReport(
            repository=args.repository,
            workspace=args.workspace,
            resources=(FabricItemOutcome("Environment", args.environment, "existing"),),
        )

    monkeypatch.setattr(cli, "_initialise_once", initialise)
    assert cli.handle_initialise(args) == 0
    assert requests == [("Production", "ProdRuntime", "ProdLanding", "ProdReporting")]
    assert discoveries == [
        (workspace, kind)
        for workspace in ("UAT", "Production")
        for kind in ("Environment", "Lakehouse", "Warehouse")
    ]
    text = capsys.readouterr().out
    assert text.count("Set up a Weaver project.") == 1
    assert args.repository == expected_folder
    assert text.index("Fabric workspace:") < text.index("Project folder [UAT]:")
    assert text.count("Project folder [Production]:") == (0 if folder_answer else 1)
    final_review = text.rsplit("\nProject\n", 1)[1]
    assert "Uat" not in final_review


@weaver_test()
def test_add_example_is_not_a_public_command():
    with pytest.raises(SystemExit):
        build_parser().parse_args(["add-example"])


@weaver_test()
def test_workspace_option_starts_at_project_folder(capsys):
    args = parse("--workspace", "Analytics")
    collect(args, stdin=Typed("\n\n\nLanding\n\n\n1\n\n"))
    assert args.repository == "Analytics"
    text = capsys.readouterr().out
    assert text.count("Set up a Weaver project.") == 1
    assert "Fabric workspace:" not in text
    assert (
        text.index("A project folder contains")
        < text.index("Project folder [Analytics]:")
        < text.index("Catalogue name")
    )
    assert "Examples: Curated, Silver" in text
