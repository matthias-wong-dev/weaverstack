"""The CLI is a thin adapter for physical-first Workspace builds."""

from __future__ import annotations

import importlib
import json

from weaver import FabricWorkspace, ItemBindings, LocalWorkspace, parse_item_binding
from weaver_cli import main
from weaver_cli.main import build_parser


def test_build_parser_accepts_repeatable_physical_first_bindings():
    args = build_parser().parse_args(
        [
            "build",
            "--bind",
            "Lakehouses/Raw_Dev=Lakehouse/Raw",
            "--bind",
            "Warehouses/Reporting_Dev=Warehouse/Reporting",
            "--bundle",
            "reviewable-build",
            "--workspace",
            "Development",
            "--environment",
            "Runtime",
        ]
    )

    assert args.item_bindings == [
        "Lakehouses/Raw_Dev=Lakehouse/Raw",
        "Warehouses/Reporting_Dev=Warehouse/Reporting",
    ]
    assert args.no_prune is False
    assert not hasattr(args, "no_catalogue")


def test_build_parser_does_not_require_a_persisted_bundle_record():
    args = build_parser().parse_args(
        [
            "build",
            "--bind",
            "Lakehouses/Raw_Dev=Lakehouse/Raw",
            "--workspace",
            "Development",
        ]
    )
    assert args.bundle is None


def test_build_parser_accepts_timestamped_bundle_record_without_name():
    args = build_parser().parse_args(
        [
            "build",
            "--bind",
            "Lakehouses/Raw_Dev=Lakehouse/Raw",
            "--bundle",
            "--workspace",
            "Development",
        ]
    )
    assert args.bundle == ""


def test_public_help_has_workspace_type_and_no_removed_host_or_root_flags():
    help_text = build_parser().format_help()
    build_help = build_parser()._subparsers._group_actions[0].choices["build"].format_help()
    combined = help_text + build_help
    assert "--workspace-type" in combined
    assert "--host" not in combined
    assert "--root" not in combined
    assert "--no-catalogue" not in combined


def test_build_handler_adds_implicit_weaver_binding_and_emits_json(monkeypatch, capsys):
    cli = importlib.import_module("weaver_cli.main")
    workspace = FabricWorkspace(
        workspace="Analytics",
        weaver_lakehouse="Control",
        environment="Runtime",
    )
    captured = {}
    monkeypatch.setattr(cli, "_resolve_workspace", lambda _args: workspace)

    def run(_workspace, **kwargs):
        captured.update(kwargs)
        return {
            "source": "weaver_items",
            "items": [str(entry.item) for entry in kwargs["bindings"].entries],
            "bundle_id": "abc123",
            "archive": None,
            "status": "succeeded",
            "errors": [],
        }

    monkeypatch.setattr(cli, "_run_fabric_item_build", run)
    assert main(
        [
            "build",
            "--bind",
            "Lakehouses/Raw_Dev=Lakehouse/Raw",
            "--workspace",
            "Analytics",
            "--environment",
            "Runtime",
            "--weaver-lakehouse",
            "Control",
            "--no-prune",
            "--json",
        ]
    ) == 0

    assert json.loads(capsys.readouterr().out)["status"] == "succeeded"
    assert captured["prune"] is False
    assert [str(entry.item) for entry in captured["bindings"].entries] == [
        "Lakehouse/Raw",
        "Lakehouse/_weaver",
    ]


def test_local_build_routes_in_process(monkeypatch):
    cli = importlib.import_module("weaver_cli.main")
    workspace = LocalWorkspace(workspace="/tmp/local", weaver_lakehouse="Control")
    captured = {}
    monkeypatch.setattr(cli, "_resolve_workspace", lambda _args: workspace)

    def run(_workspace, **kwargs):
        captured.update(kwargs)
        return {
            "source": "weaver_items",
            "items": [str(entry.item) for entry in kwargs["bindings"].entries],
            "bundle_id": "local",
            "archive": None,
            "status": "succeeded",
            "errors": [],
        }

    monkeypatch.setattr(cli, "_run_local_item_build", run)
    assert main(
        [
            "build",
            "--bind",
            "Lakehouses/Raw_Dev=Lakehouse/Raw",
            "--workspace",
            "/tmp/local",
            "--workspace-type",
            "local",
            "--weaver-lakehouse",
            "Control",
        ]
    ) == 0
    assert captured["prune"] is True


def test_fabric_build_requires_environment(monkeypatch, capsys):
    cli = importlib.import_module("weaver_cli.main")
    monkeypatch.setattr(
        cli,
        "_resolve_workspace",
        lambda _args: FabricWorkspace(
            workspace="Analytics", weaver_lakehouse="Control"
        ),
    )
    assert main(
        [
            "build",
            "--bind",
            "Lakehouses/Raw_Dev=Lakehouse/Raw",
            "--workspace",
            "Analytics",
        ]
    ) == 1
    assert "requires --environment" in capsys.readouterr().err


def test_fabric_adapter_submits_complete_uploaded_workflow(monkeypatch):
    cli = importlib.import_module("weaver_cli.main")
    captured = {}

    class _Result:
        payload = {"status": "succeeded"}

    class _Session:
        @classmethod
        def for_workspace(cls, workspace):
            captured["workspace"] = workspace
            return cls()

        def __enter__(self):
            return self

        def __exit__(self, *_exc):
            return False

        def run(self, body):
            compile(body, "<fabric-build>", "exec")
            captured["body"] = body
            return _Result()

    monkeypatch.setattr("weaver.fabric.LivySession", _Session)
    monkeypatch.setattr("weaver.fabric.list_workspace_livy_sessions", lambda *a, **k: ())
    workspace = FabricWorkspace(
        workspace="Analytics", weaver_lakehouse="Control", environment="Runtime"
    )
    result = cli._run_fabric_item_build(
        workspace,
        bindings=ItemBindings(
            (parse_item_binding("Lakehouses/Raw_Dev=Lakehouse/Raw"),)
        ),
        bundle_name="estate-build",
        prune=True,
    )

    assert result["status"] == "succeeded"
    assert "build_uploaded_item_repository" in captured["body"]
    assert "generate_item_build_bundle" not in captured["body"]
    assert "resolver.build_bundle(record_name)" in captured["body"]


def test_fabric_adapter_reports_queued_session_before_starting(monkeypatch, capsys):
    cli = importlib.import_module("weaver_cli.main")
    from weaver.fabric import LivySessionInfo, WorkspaceLivySession

    events = []
    queued = WorkspaceLivySession(
        "lakehouse-id",
        "Sales",
        LivySessionInfo(
            "7",
            name="notebook",
            scheduler_state="Scheduled",
            plugin_state="Queued",
            livy_state="not_started",
        ),
    )

    class _Result:
        payload = {"status": "succeeded"}

    class _Session:
        @classmethod
        def for_workspace(cls, _workspace):
            events.append("start")
            return cls()

        def __enter__(self):
            return self

        def __exit__(self, *_exc):
            return False

        def run(self, _body):
            return _Result()

    def inspect(*_args, **_kwargs):
        events.append("inspect")
        return (queued,)

    monkeypatch.setattr("weaver.fabric.LivySession", _Session)
    monkeypatch.setattr("weaver.fabric.list_workspace_livy_sessions", inspect)
    workspace = FabricWorkspace(
        workspace="Analytics", weaver_lakehouse="Control", environment="Runtime"
    )
    cli._run_fabric_item_build(
        workspace,
        bindings=ItemBindings(
            (parse_item_binding("Lakehouses/Raw_Dev=Lakehouse/Raw"),)
        ),
        bundle_name=None,
        prune=True,
    )

    assert events == ["inspect", "start"]
    assert "Sales: session 7 (Scheduled/Queued/not_started)" in capsys.readouterr().err
