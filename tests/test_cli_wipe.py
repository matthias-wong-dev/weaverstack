"""The target-directed wipe command and its catalogue handoff."""

from __future__ import annotations

import importlib
from dataclasses import dataclass

import pytest

from weaver import FabricWorkspace, LocalWorkspace
from weaver_cli import main
from weaver_cli.main import build_parser


@dataclass(frozen=True)
class _Report:
    target: str
    location: str
    removed: tuple[str, ...] = ("object",)

    @property
    def count(self):
        return len(self.removed)

    def __str__(self):
        return f"{self.target}: removed {self.count}"


def test_parser_uses_unambiguous_lakehouse_and_warehouse_flags():
    args = build_parser().parse_args(
        [
            "wipe",
            "--lakehouse",
            "Shared",
            "--warehouse",
            "Shared",
            "--workspace",
            "Analytics",
        ]
    )
    assert args.lakehouses == ["Shared"]
    assert args.warehouses == ["Shared"]


def test_removed_target_and_root_flags_are_rejected():
    with pytest.raises(SystemExit):
        build_parser().parse_args(
            ["wipe", "--lakehouse-target", "Sales", "--root", ".local"]
        )


def test_wipe_requires_a_target(capsys):
    assert main(["wipe", "--workspace", "/tmp/local", "--workspace-type", "local"]) == 1
    assert "at least one --lakehouse or --warehouse" in capsys.readouterr().err


def test_local_workspace_rejects_warehouse_before_sql(monkeypatch, capsys):
    cli = importlib.import_module("weaver_cli.main")
    monkeypatch.setattr(
        cli,
        "_resolve_workspace",
        lambda _args: LocalWorkspace(
            workspace="/tmp/local", weaver_lakehouse="Control"
        ),
    )
    assert main(["wipe", "--warehouse", "Reporting", "--workspace", "/tmp/local"]) == 1
    assert "require a Fabric Workspace" in capsys.readouterr().err


def test_dry_run_never_wipes_or_unbinds(monkeypatch, capsys):
    cli = importlib.import_module("weaver_cli.main")
    workspace = LocalWorkspace(workspace="/tmp/local", weaver_lakehouse="Control")
    monkeypatch.setattr(cli, "_resolve_workspace", lambda _args: workspace)
    monkeypatch.setattr(cli, "_desktop_store", lambda _workspace: object())
    calls = []

    def wipe(_target, _workspace, *, store, dry_run=False):
        calls.append(("wipe", dry_run))
        return (_Report("lakehouse:Sales", "/Sales"),)

    monkeypatch.setattr("weaver.wipe_lakehouse", wipe)
    monkeypatch.setattr(
        cli,
        "_run_unbind",
        lambda *_args, **_kwargs: calls.append(("unbind", False)),
    )
    assert main(["wipe", "--lakehouse", "Sales", "--workspace", "/tmp/local", "--dry-run"]) == 0
    assert calls == [("wipe", True)]
    assert "Nothing was changed" in capsys.readouterr().out


def test_confirmed_lakehouse_wipe_is_followed_by_unbind(monkeypatch):
    cli = importlib.import_module("weaver_cli.main")
    workspace = LocalWorkspace(workspace="/tmp/local", weaver_lakehouse="Control")
    monkeypatch.setattr(cli, "_resolve_workspace", lambda _args: workspace)
    monkeypatch.setattr(cli, "_desktop_store", lambda _workspace: object())
    calls = []

    def wipe(target, _workspace, *, store, dry_run=False):
        calls.append(("wipe", target.name, dry_run))
        return (_Report(f"lakehouse:{target.name}", f"/{target.name}"),)

    def unbind(_workspace, *, lakehouses, warehouses):
        calls.append(("unbind", tuple(lakehouses), tuple(warehouses)))
        return {"targets": [], "logical_items": [], "statements": 0}

    monkeypatch.setattr("weaver.wipe_lakehouse", wipe)
    monkeypatch.setattr(cli, "_run_unbind", unbind)
    assert main(["wipe", "--lakehouse", "Sales", "--workspace", "/tmp/local", "--yes"]) == 0
    assert calls == [
        ("wipe", "Sales", True),
        ("wipe", "Sales", False),
        ("unbind", ("Sales",), ()),
    ]


def test_empty_target_is_still_unbound(monkeypatch):
    cli = importlib.import_module("weaver_cli.main")
    workspace = LocalWorkspace(workspace="/tmp/local", weaver_lakehouse="Control")
    monkeypatch.setattr(cli, "_resolve_workspace", lambda _args: workspace)
    monkeypatch.setattr(cli, "_desktop_store", lambda _workspace: object())
    monkeypatch.setattr(
        "weaver.wipe_lakehouse",
        lambda target, _workspace, **_kwargs: (
            _Report(f"lakehouse:{target.name}", f"/{target.name}", ()),
        ),
    )
    calls = []
    monkeypatch.setattr(
        cli,
        "_run_unbind",
        lambda *_args, **kwargs: calls.append(kwargs)
        or {"targets": [], "logical_items": [], "statements": 0},
    )
    assert main(["wipe", "--lakehouse", "Sales", "--workspace", "/tmp/local"]) == 0
    assert calls == [{"lakehouses": ["Sales"], "warehouses": []}]


def test_warehouse_wipe_closes_executor_and_unbinds(monkeypatch):
    cli = importlib.import_module("weaver_cli.main")
    workspace = FabricWorkspace(
        workspace="Analytics", weaver_lakehouse="Control", environment="Runtime"
    )
    monkeypatch.setattr(cli, "_resolve_workspace", lambda _args: workspace)
    calls = []

    class _Sql:
        def __enter__(self):
            calls.append("open")
            return self

        def __exit__(self, *_exc):
            calls.append("close")

    monkeypatch.setattr(
        "weaver.fabric.desktop_sql_executor", lambda *_args: _Sql()
    )
    monkeypatch.setattr(
        "weaver.wipe_sql_target",
        lambda target, _workspace, *, sql: calls.append(("wipe", target.warehouse.name)),
    )
    monkeypatch.setattr(
        cli,
        "_run_unbind",
        lambda *_args, **kwargs: calls.append(("unbind", kwargs["warehouses"]))
        or {"targets": [], "logical_items": [], "statements": 0},
    )
    assert main(["wipe", "--warehouse", "Reporting", "--workspace", "Analytics", "--yes"]) == 0
    assert calls == [
        "open",
        ("wipe", "Reporting"),
        "close",
        ("unbind", ["Reporting"]),
    ]


def test_noninteractive_wipe_needs_yes(monkeypatch, capsys):
    cli = importlib.import_module("weaver_cli.main")
    workspace = LocalWorkspace(workspace="/tmp/local", weaver_lakehouse="Control")
    monkeypatch.setattr(cli, "_resolve_workspace", lambda _args: workspace)
    monkeypatch.setattr(cli, "_desktop_store", lambda _workspace: object())
    monkeypatch.setattr(
        "weaver.wipe_lakehouse",
        lambda *_args, **_kwargs: (_Report("lakehouse:Sales", "/Sales"),),
    )
    assert main(["wipe", "--lakehouse", "Sales", "--workspace", "/tmp/local"]) == 1
    assert "without confirmation" in capsys.readouterr().err
