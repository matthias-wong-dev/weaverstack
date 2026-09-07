"""The CLI is a confirmation and rendering adapter for public mirror."""

from __future__ import annotations

import importlib

import pytest
from support.weaver_test import weaver_test
from support.workspaces import given_workspace

from weaver import MirrorResult
from weaver_cli import main
from weaver_cli.main import build_parser


def _result() -> MirrorResult:
    return MirrorResult(
        workspace="Analytics",
        source_catalogue="Warehouse/Weaver",
        destination_catalogue="Analytics/Warehouse/Weaver_Dev",
        wiped=("Warehouse/Weaver_Dev",),
        copied={"Installation": 2, "Registry": 11},
        uncopied=("Log", "LoadStatistic"),
    )


def _workspace():
    from dataclasses import replace

    from weaver.workspaces import CatalogueRef

    return replace(
        given_workspace(catalogue="Warehouse/Weaver_Dev"),
        mirror=CatalogueRef(workspace=None, name="Weaver"),
    )


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
def test_the_catalogue_to_fork_can_be_named_on_the_command():
    args = build_parser().parse_args(
        ["mirror", "--source", "Warehouse/Weaver", "--workspace", "Analytics"]
    )

    assert args.source == "Warehouse/Weaver"


@weaver_test()
def test_naming_no_item_is_a_choice_rather_than_an_omission():
    """Omitting ``--item`` selects every configured target, as ``build`` does.

    ``--no-item`` is how a fork of the catalogue alone is asked for.
    """

    args = build_parser().parse_args(["mirror", "--no-item", "--workspace", "A"])

    assert args.no_item is True
    assert args.items is None


@weaver_test()
def test_mirror_starts_no_spark_session():
    """A fork empties a Warehouse, rebuilds ``_`` and copies rows between two.

    None of that reaches a Lakehouse, so asking for Spark would be paying for a
    session nothing submits to.
    """

    from weaver.sessions.requirements import AUTH, LIVY, ONELAKE, RESOLVER, TDS

    args = build_parser().parse_args(["mirror", "--no-item", "--workspace", "A"])
    required = args.requires(args)

    assert LIVY not in required
    assert ONELAKE not in required
    assert {AUTH, RESOLVER, TDS} <= required


# --- confirmation -------------------------------------------------------------


@weaver_test()
def test_a_fork_is_refused_without_confirmation(monkeypatch, capsys):
    """The destination is emptied, so it asks the way ``wipe`` asks."""

    cli = importlib.import_module("weaver_cli.main")
    monkeypatch.setattr(cli, "_resolve_workspace", lambda _args: _workspace())
    monkeypatch.setattr(cli.sys.stdin, "isatty", lambda: False)
    monkeypatch.setattr(
        "weaver.mirror", lambda *_a, **_k: pytest.fail("mirror ran unconfirmed")
    )

    assert main(["mirror", "--no-item", "--workspace", "Analytics"]) == 1
    assert "Refusing to empty Warehouse/Weaver_Dev" in capsys.readouterr().err


@weaver_test()
def test_an_authorised_fork_invokes_the_public_operation_once(monkeypatch, capsys):
    cli = importlib.import_module("weaver_cli.main")
    workspace = _workspace()
    monkeypatch.setattr(cli, "_resolve_workspace", lambda _args: workspace)
    calls = []

    def mirror(items, **kwargs):
        calls.append((items, kwargs))
        return _result()

    monkeypatch.setattr("weaver.mirror", mirror)

    assert main(["mirror", "--no-item", "--yes", "--workspace", "Analytics"]) == 0

    ((items, passed),) = calls
    assert items is None
    assert passed["no_item"] is True
    # The CLI hands the operation a Session rather than a resolved Workspace.
    assert passed["session"].workspace is workspace
    assert "workspace" not in passed
    printed = capsys.readouterr().out
    assert "Forked Warehouse/Weaver into Analytics/Warehouse/Weaver_Dev" in printed
    assert "Registry: 11" in printed
    assert "Log, LoadStatistic" in printed


@weaver_test()
def test_selecting_items_and_no_item_together_is_refused(monkeypatch):
    from weaver.errors import CommandError

    cli = importlib.import_module("weaver_cli.main")
    monkeypatch.setattr(cli, "_resolve_workspace", lambda _args: _workspace())

    with pytest.raises(CommandError, match="cannot be used together"):
        cli.handle_mirror(
            build_parser().parse_args(
                ["mirror", "--item", "Warehouse/Model", "--no-item", "--yes"]
            )
        )
