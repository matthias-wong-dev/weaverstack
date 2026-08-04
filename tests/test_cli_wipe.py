"""The CLI is a confirmation and rendering adapter for public wipe."""

from __future__ import annotations

import importlib

import pytest

from weaver.workspaces import LocalWorkspace
from weaver.locations import Location
from weaver import WipeReport, WipeResult
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


def test_unbind_uses_the_same_whole_target_grammar():
    args = build_parser().parse_args(
        [
            "unbind",
            "Lakehouse/Sales",
            "Warehouse/Reporting",
            "--workspace",
            "Analytics",
        ]
    )
    assert args.targets == ["Lakehouse/Sales", "Warehouse/Reporting"]


def test_wipe_requires_a_typed_target():
    with pytest.raises(SystemExit):
        build_parser().parse_args(
            ["wipe", "--workspace", "/tmp/local", "--workspace-type", "local"]
        )


def test_dry_run_invokes_public_operation_once(monkeypatch, capsys):
    cli = importlib.import_module("weaver_cli.main")
    workspace = LocalWorkspace(workspace="/tmp/local", weaver_lakehouse="Control")
    monkeypatch.setattr(cli, "_resolve_workspace", lambda _args: workspace)
    calls = []

    def wipe(targets, **kwargs):
        calls.append((tuple(targets), kwargs))
        return _result(dry_run=True)

    monkeypatch.setattr("weaver.wipe", wipe)
    assert main(
        ["wipe", "Lakehouse/Sales", "--workspace", "/tmp/local", "--dry-run"]
    ) == 0
    assert calls == [
        (
            ("Lakehouse/Sales",),
            {"workspace": workspace, "unbind_from": None, "dry_run": True},
        )
    ]
    assert "Nothing was changed" in capsys.readouterr().out


def test_confirmed_wipe_previews_then_executes_same_public_operation(monkeypatch):
    cli = importlib.import_module("weaver_cli.main")
    workspace = LocalWorkspace(workspace="/tmp/local", weaver_lakehouse="Control")
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
    assert calls == [
        (
            ("Lakehouse/Sales/Tables",),
            {"workspace": workspace, "unbind_from": "Control", "dry_run": True},
        ),
        (
            ("Lakehouse/Sales/Tables",),
            {"workspace": workspace, "unbind_from": "Control"},
        ),
    ]


def test_noninteractive_wipe_needs_yes(monkeypatch, capsys):
    cli = importlib.import_module("weaver_cli.main")
    workspace = LocalWorkspace(workspace="/tmp/local", weaver_lakehouse="Control")
    monkeypatch.setattr(cli, "_resolve_workspace", lambda _args: workspace)
    monkeypatch.setattr("weaver.wipe", lambda *_args, **_kwargs: _result(dry_run=True))
    assert main(["wipe", "Lakehouse/Sales", "--workspace", "/tmp/local"]) == 1
    assert "without confirmation" in capsys.readouterr().err
