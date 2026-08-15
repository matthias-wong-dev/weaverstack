"""The CLI is a confirmation and rendering adapter for public wipe."""

from __future__ import annotations

import importlib

import pytest
from support.workspaces import given_workspace

from weaver import WipeReport, WipeResult
from weaver.locations import Location
from weaver_cli import main
from weaver_cli.main import build_parser


def _result(*, dry_run: bool, removed=("object",)) -> WipeResult:
    return WipeResult(
        workspace="/tmp/local",
        reports=(
            WipeReport(
                target="Lakehouse/Sales",
                location=Location("/tmp/local/Sales"),
                removed=removed,
                dry_run=dry_run,
            ),
        ),
        dry_run=dry_run,
    )


def test_parser_uses_the_shared_typed_target_grammar():
    args = build_parser().parse_args(
        [
            "wipe",
            "Lakehouse/Shared",
            "Warehouse/Shared",
            "--workspace",
            "Analytics",
            "--unbind-from",
            "Weaver",
        ]
    )
    assert args.targets == ["Lakehouse/Shared", "Warehouse/Shared"]
    assert args.unbind_from == "Weaver"


def test_removed_target_switches_are_rejected():
    with pytest.raises(SystemExit):
        build_parser().parse_args(
            ["wipe", "--lakehouse", "Sales", "--workspace", ".local"]
        )


def test_unbinding_is_reached_through_wipe_and_not_a_command_of_its_own():
    """Removing catalogue claims is part of clearing a target, not a verb.

    `unbind_catalogue_claims` is still the operation, and `--unbind-from`
    selects it. What is gone is a separate command that removed claims for a
    target it never looked at.
    """

    with pytest.raises(SystemExit):
        build_parser().parse_args(["unbind", "Lakehouse/Sales"])


def test_wipe_requires_a_typed_target():
    with pytest.raises(SystemExit):
        build_parser().parse_args(
            ["wipe", "--workspace", "Demo"]
        )


def test_dry_run_invokes_public_operation_once(monkeypatch, capsys):
    cli = importlib.import_module("weaver_cli.main")
    workspace = given_workspace(catalogue="Lakehouse/Control")
    monkeypatch.setattr(cli, "_resolve_workspace", lambda _args: workspace)
    calls = []

    def wipe(targets, **kwargs):
        calls.append((tuple(targets), kwargs))
        return _result(dry_run=True)

    monkeypatch.setattr("weaver.wipe", wipe)
    assert main(
        ["wipe", "Lakehouse/Sales", "--workspace", "/tmp/local", "--dry-run"]
    ) == 0
    # The CLI hands the operation a Session rather than a resolved Workspace:
    # operations take names, and the Session is what carries the context the
    # CLI resolved for its own inheritance and override rules.
    (targets, passed), = calls
    assert targets == ("Lakehouse/Sales",)
    assert passed["unbind_from"] is None
    assert passed["dry_run"] is True
    assert passed["session"].workspace is workspace
    assert "workspace" not in passed
    assert "Nothing was changed" in capsys.readouterr().out


def test_an_authorised_wipe_does_not_pay_for_a_preview_nobody_reads(monkeypatch):
    """``--yes`` means no question, so the listing that asks it is pure cost.

    A dry run is a full read of the estate — every target, every path — and on
    the Weaver Example it was four seconds of a twelve-second wipe, spent
    rendering a list that was never going to be answered.
    """

    cli = importlib.import_module("weaver_cli.main")
    workspace = given_workspace(catalogue="Lakehouse/Control")
    monkeypatch.setattr(cli, "_resolve_workspace", lambda _args: workspace)
    calls = []

    def wipe(targets, **kwargs):
        calls.append((tuple(targets), kwargs))
        return _result(dry_run=kwargs.get("dry_run", False))

    monkeypatch.setattr("weaver.wipe", wipe)
    assert main(
        [
            "wipe",
            "Lakehouse/Sales/Tables",
            "--workspace",
            "/tmp/local",
            "--unbind-from",
            "Control",
            "--yes",
        ]
    ) == 0
    (targets, passed), = calls
    assert targets == ("Lakehouse/Sales/Tables",)
    assert passed["unbind_from"] == "Control"
    assert passed["session"].workspace is workspace
    assert "workspace" not in passed


def test_an_unauthorised_wipe_still_previews_before_it_asks(monkeypatch):
    """The listing is the question. Remove it and there is nothing to agree to."""

    cli = importlib.import_module("weaver_cli.main")
    workspace = given_workspace(catalogue="Lakehouse/Control")
    monkeypatch.setattr(cli, "_resolve_workspace", lambda _args: workspace)
    monkeypatch.setattr(cli.sys.stdin, "isatty", lambda: True, raising=False)
    monkeypatch.setattr("builtins.input", lambda _prompt: "y")
    calls = []

    def wipe(targets, **kwargs):
        calls.append(kwargs.get("dry_run", False))
        return _result(dry_run=kwargs.get("dry_run", False))

    monkeypatch.setattr("weaver.wipe", wipe)
    assert main(["wipe", "Lakehouse/Sales/Tables", "--workspace", "/tmp/local"]) == 0
    assert calls == [True, False]


def test_noninteractive_wipe_needs_yes(monkeypatch, capsys):
    cli = importlib.import_module("weaver_cli.main")
    workspace = given_workspace(catalogue="Lakehouse/Control")
    monkeypatch.setattr(cli, "_resolve_workspace", lambda _args: workspace)
    monkeypatch.setattr("weaver.wipe", lambda *_args, **_kwargs: _result(dry_run=True))
    assert main(["wipe", "Lakehouse/Sales", "--workspace", "/tmp/local"]) == 1
    assert "without confirmation" in capsys.readouterr().err
