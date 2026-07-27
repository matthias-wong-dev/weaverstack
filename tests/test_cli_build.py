"""The CLI is a thin adapter for item-oriented Fabric builds."""

from __future__ import annotations

import json
import importlib

from weaver import FabricHost, ItemBindings, parse_item_binding
from weaver_cli import main
from weaver_cli.main import build_parser


def test_build_parser_accepts_repeatable_logical_item_bindings():
    args = build_parser().parse_args(
        [
            "build",
            "--repository",
            "Estate",
            "--bind",
            "Lakehouse/Raw=Raw_Dev",
            "--bind",
            "Warehouse/Reporting=Reporting_Dev",
            "--bundle",
            "reviewable-build",
            "--host",
            "Development",
            "--hosts",
            "env.yml",
        ]
    )

    assert args.repository == "Estate"
    assert args.item_bindings == [
        "Lakehouse/Raw=Raw_Dev",
        "Warehouse/Reporting=Reporting_Dev",
    ]
    assert args.no_prune is False
    assert args.no_catalogue is False


def test_build_handler_passes_typed_bindings_and_returns_serialisable_json(
    monkeypatch, capsys
):
    cli = importlib.import_module("weaver_cli.main")

    host = FabricHost(
        workspace="Analytics",
        weaver_lakehouse="Control",
        fabric_environment="Runtime",
    )
    captured = {}
    monkeypatch.setattr(cli, "_resolve_host", lambda args: host)

    def run(host_value, **kwargs):
        captured.update(kwargs)
        return {
            "repository": kwargs["repository_name"],
            "items": [str(entry.item) for entry in kwargs["bindings"].entries],
            "bundle_id": "abc123",
            "status": "succeeded",
            "errors": [],
        }

    monkeypatch.setattr(cli, "_run_fabric_item_build", run)

    assert main(
        [
            "build",
            "--repository",
            "Estate",
            "--bind",
            "Lakehouse/Raw=Raw_Dev",
            "--bundle",
            "estate-build",
            "--host",
            "Development",
            "--hosts",
            "env.yml",
            "--no-prune",
            "--no-catalogue",
            "--json",
        ]
    ) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "succeeded"
    assert captured["prune"] is False
    assert captured["catalogue"] is False
    assert [str(entry.item) for entry in captured["bindings"].entries] == [
        "Lakehouse/Raw"
    ]


def test_invalid_binding_fails_before_transport(monkeypatch, capsys):
    cli = importlib.import_module("weaver_cli.main")

    monkeypatch.setattr(
        cli,
        "_resolve_host",
        lambda args: (_ for _ in ()).throw(AssertionError("transport was reached")),
    )
    assert main(
        [
            "build",
            "--repository",
            "Estate",
            "--bind",
            "Raw_Dev",
            "--bundle",
            "estate-build",
            "--root",
            ".local",
        ]
    ) == 1
    assert "ItemType/LogicalName=PhysicalName" in capsys.readouterr().err


def test_desktop_build_requires_a_fabric_host(tmp_path, capsys):
    assert main(
        [
            "build",
            "--repository",
            "Estate",
            "--bind",
            "Lakehouse/Raw=Raw_Dev",
            "--bundle",
            "estate-build",
            "--root",
            str(tmp_path),
        ]
    ) == 1
    assert "submits Weaver to Fabric" in capsys.readouterr().err


def test_fabric_adapter_submits_both_core_phases_in_one_valid_program(monkeypatch):
    cli = importlib.import_module("weaver_cli.main")
    captured = {}

    class _Result:
        payload = {
            "repository": "Estate",
            "items": ["Lakehouse/Raw"],
            "bundle_id": "bundle-id",
            "status": "succeeded",
            "errors": [],
        }

    class _Session:
        @classmethod
        def for_host(cls, host):
            captured["host"] = host
            return cls()

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def run(self, body):
            compile(body, "<fabric-build>", "exec")
            captured["body"] = body
            return _Result()

    monkeypatch.setattr("weaver.fabric.LivySession", _Session)
    monkeypatch.setattr("weaver.fabric.list_workspace_livy_sessions", lambda *a, **k: ())
    host = FabricHost(
        workspace="Analytics",
        weaver_lakehouse="Control",
        fabric_environment="Runtime",
    )
    result = cli._run_fabric_item_build(
        host,
        repository_name="Estate",
        bindings=ItemBindings((parse_item_binding("Lakehouse/Raw=Raw_Dev"),)),
        bundle_name="estate-build",
        prune=True,
        catalogue=True,
    )

    assert result["status"] == "succeeded"
    assert "read_weaver_repository" in captured["body"]
    assert "generate_item_build_bundle" in captured["body"]
    assert "install_bundle" in captured["body"]


def test_fabric_adapter_reports_a_queued_session_before_starting(monkeypatch, capsys):
    cli = importlib.import_module("weaver_cli.main")
    from weaver.fabric import LivySessionInfo, WorkspaceLivySession

    events = []
    queued = WorkspaceLivySession(
        "lakehouse-id", "Play_Lakehouse_1",
        LivySessionInfo(
            "7", name="notebook", scheduler_state="Scheduled",
            plugin_state="Queued", livy_state="not_started",
        ),
    )

    class _Result:
        payload = {"status": "succeeded"}

    class _Session:
        @classmethod
        def for_host(cls, host):
            events.append("start")
            return cls()

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def run(self, body):
            return _Result()

    def inspect(*args, **kwargs):
        events.append("inspect")
        return (queued,)

    monkeypatch.setattr("weaver.fabric.LivySession", _Session)
    monkeypatch.setattr("weaver.fabric.list_workspace_livy_sessions", inspect)
    host = FabricHost(
        workspace="Analytics", weaver_lakehouse="Control",
        fabric_environment="Runtime",
    )
    cli._run_fabric_item_build(
        host, repository_name="Estate",
        bindings=ItemBindings((parse_item_binding("Lakehouse/Raw=Raw_Dev"),)),
        bundle_name="estate-build", prune=True, catalogue=True,
    )

    assert events == ["inspect", "start"]
    assert "Play_Lakehouse_1: session 7 (Scheduled/Queued/not_started)" in capsys.readouterr().err
