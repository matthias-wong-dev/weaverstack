"""The CLI is a confirmation and rendering adapter for public wipe."""

from __future__ import annotations

import importlib

import pytest
from support.weaver_test import weaver_test
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
        catalogue_role="preserved",
        dry_run=dry_run,
    )


@weaver_test()
def test_parser_uses_the_shared_typed_target_grammar():
    args = build_parser().parse_args(
        [
            "wipe",
            "Lakehouse/Shared",
            "Warehouse/Shared",
            "--workspace",
            "Analytics",
        ]
    )
    assert args.targets == ["Lakehouse/Shared", "Warehouse/Shared"]


@weaver_test()
def test_unbind_is_a_wipe_switch_and_not_a_command_of_its_own():
    """Removing catalogue claims is part of wiping, not a verb of its own."""

    args = build_parser().parse_args(
        ["wipe", "Lakehouse/Sales", "--unbind", "--workspace", "Analytics"]
    )
    assert args.unbind is True
    with pytest.raises(SystemExit):
        build_parser().parse_args(["unbind", "Lakehouse/Sales"])


@weaver_test()
def test_wipe_requires_a_typed_target():
    assert build_parser().parse_args(["wipe", "--workspace", "Demo"]).targets == []


@weaver_test()
def test_dry_run_invokes_public_operation_once(monkeypatch, capsys):
    cli = importlib.import_module("weaver_cli.main")
    workspace = given_workspace(catalogue="Warehouse/Control")
    monkeypatch.setattr(cli, "_resolve_workspace", lambda _args: workspace)
    calls = []

    def wipe(targets, **kwargs):
        calls.append((tuple(targets), kwargs))
        return _result(dry_run=True)

    monkeypatch.setattr("weaver.wipe", wipe)
    assert (
        main(["wipe", "Lakehouse/Sales", "--workspace", "/tmp/local", "--dry-run"]) == 0
    )
    # The CLI hands the operation a Session rather than a resolved Workspace:
    # operations take names, and the Session is what carries the context the
    # CLI resolved for its own inheritance and override rules.
    ((targets, passed),) = calls
    assert targets == ("Lakehouse/Sales",)
    assert passed["dry_run"] is True
    assert passed["session"].workspace is workspace
    assert "workspace" not in passed
    assert "Nothing was changed" in capsys.readouterr().out


@weaver_test()
def test_an_authorised_wipe_does_not_pay_for_a_preview_nobody_reads(monkeypatch):
    """``--yes`` means no question, so the listing that asks it is pure cost."""

    cli = importlib.import_module("weaver_cli.main")
    workspace = given_workspace(catalogue="Warehouse/Control")
    monkeypatch.setattr(cli, "_resolve_workspace", lambda _args: workspace)
    calls = []

    def wipe(targets, **kwargs):
        calls.append((tuple(targets), kwargs))
        return _result(dry_run=kwargs.get("dry_run", False))

    monkeypatch.setattr("weaver.wipe", wipe)
    assert (
        main(
            [
                "wipe",
                "Lakehouse/Sales",
                "--workspace",
                "/tmp/local",
                "--yes",
            ]
        )
        == 0
    )
    ((targets, passed),) = calls
    assert targets == ("Lakehouse/Sales",)
    assert passed["session"].workspace is workspace
    assert workspace.catalogue == "Warehouse/Control"
    assert "workspace" not in passed


@weaver_test()
def test_an_unauthorised_wipe_previews_the_whole_scope_before_it_asks(
    monkeypatch, capsys
):
    """The listing is the question. Remove it and there is nothing to agree to."""

    cli = importlib.import_module("weaver_cli.main")
    workspace = given_workspace(catalogue="Warehouse/Control")
    monkeypatch.setattr(cli, "_resolve_workspace", lambda _args: workspace)
    monkeypatch.setattr(cli.sys.stdin, "isatty", lambda: True, raising=False)
    monkeypatch.setattr("builtins.input", lambda _prompt: "y")
    calls = []

    def wipe(targets, **kwargs):
        calls.append(kwargs.get("dry_run", False))
        return _result(dry_run=kwargs.get("dry_run", False))

    monkeypatch.setattr("weaver.wipe", wipe)
    assert main(["wipe", "Lakehouse/Sales", "--workspace", "/tmp/local"]) == 0
    assert calls == [True, False]
    out = capsys.readouterr().out
    assert "Lakehouse/Sales" in out
    assert "catalogue: preserved" in out


@weaver_test()
def test_unbind_is_passed_to_the_operation_and_stated_in_the_summary(
    monkeypatch, capsys
):
    cli = importlib.import_module("weaver_cli.main")
    workspace = given_workspace(catalogue="Warehouse/Control")
    monkeypatch.setattr(cli, "_resolve_workspace", lambda _args: workspace)
    passed = {}

    def wipe(targets, **kwargs):
        passed.update(kwargs)
        return WipeResult(
            workspace="/tmp/local",
            reports=_result(dry_run=False).reports,
            unbound={"targets": ["Lakehouse/Sales"], "logical_items": ["Sales.Sales"]},
            catalogue_role="preserved; claims unbound",
        )

    monkeypatch.setattr("weaver.wipe", wipe)
    assert (
        main(
            [
                "wipe",
                "Lakehouse/Sales",
                "--unbind",
                "--workspace",
                "/tmp/local",
                "--yes",
            ]
        )
        == 0
    )
    assert passed["unbind"] is True
    out = capsys.readouterr().out
    assert "catalogue: preserved; claims unbound" in out
    assert "unbound: Sales.Sales" in out


@weaver_test()
def test_an_estate_wipe_states_the_catalogue_is_removed_with_it(monkeypatch, capsys):
    cli = importlib.import_module("weaver_cli.main")
    workspace = given_workspace(catalogue="Warehouse/Control")
    monkeypatch.setattr(cli, "_resolve_workspace", lambda _args: workspace)

    def wipe(targets, **kwargs):
        passed_targets = tuple(targets)
        assert passed_targets == ()
        return WipeResult(
            workspace="/tmp/local",
            reports=(
                WipeReport(
                    target="Warehouse/Control",
                    location=Location("warehouse://Control"),
                    removed=("all user-created SQL objects",),
                    dry_run=kwargs.get("dry_run", False),
                ),
            ),
            catalogue_role="removed with the estate",
            dry_run=kwargs.get("dry_run", False),
        )

    monkeypatch.setattr("weaver.wipe", wipe)
    assert main(["wipe", "--workspace", "/tmp/local", "--yes"]) == 0
    out = capsys.readouterr().out
    assert "Warehouse/Control: removed 1" in out
    assert "catalogue: removed with the estate" in out


@weaver_test()
def test_noninteractive_wipe_needs_yes(monkeypatch, capsys):
    cli = importlib.import_module("weaver_cli.main")
    workspace = given_workspace(catalogue="Warehouse/Control")
    monkeypatch.setattr(cli, "_resolve_workspace", lambda _args: workspace)
    monkeypatch.setattr("weaver.wipe", lambda *_args, **_kwargs: _result(dry_run=True))
    assert main(["wipe", "Lakehouse/Sales", "--workspace", "/tmp/local"]) == 1
    assert "without confirmation" in capsys.readouterr().err
