"""The CLI is a thin adapter for physical-first Workspace builds."""

from __future__ import annotations

import importlib
import json
from pathlib import Path

import pytest

from weaver import (
    FabricWorkspace,
    ItemBindings,
    ItemRef,
    LakehouseBinding,
    LocalStore,
    LocalWorkspace,
    Location,
    effective_item_bindings,
    parse_item_binding,
    parse_item_repository,
)
from weaver_cli import main
from weaver_cli.main import build_parser

REPOSITORY = Path(__file__).parent / "fixtures" / "build-lakehouse-item"


def test_build_parser_accepts_repeatable_physical_first_bindings():
    args = build_parser().parse_args(
        [
            "build",
            str(REPOSITORY),
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
    assert not hasattr(args, "no_catalogue")


def test_build_parser_does_not_require_a_persisted_bundle_record():
    args = build_parser().parse_args(
        [
            "build",
            str(REPOSITORY),
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
            str(REPOSITORY),
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
    assert "--no-prune" not in combined


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
            str(REPOSITORY),
            "--bind",
            "Lakehouses/Raw_Dev=Lakehouse/Raw",
            "--workspace",
            "Analytics",
            "--environment",
            "Runtime",
            "--weaver-lakehouse",
            "Control",
            "--json",
        ]
    ) == 0

    assert json.loads(capsys.readouterr().out)["status"] == "succeeded"
    assert "prune" not in captured
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
            str(REPOSITORY),
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
    assert "prune" not in captured


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
            str(REPOSITORY),
            "--bind",
            "Lakehouses/Raw_Dev=Lakehouse/Raw",
            "--workspace",
            "Analytics",
        ]
    ) == 1
    assert "requires --environment" in capsys.readouterr().err


def test_invalid_repository_fails_before_fabric_execution(monkeypatch, capsys, tmp_path):
    cli = importlib.import_module("weaver_cli.main")
    invalid = tmp_path / "repository"
    invalid.mkdir()
    (invalid / "invalid.txt").write_text("not a Weaver item", encoding="utf-8")
    monkeypatch.setattr(
        cli,
        "_resolve_workspace",
        lambda _args: FabricWorkspace(
            workspace="Analytics", weaver_lakehouse="Control", environment="Runtime"
        ),
    )
    monkeypatch.setattr(
        cli,
        "_run_fabric_item_build",
        lambda *_args, **_kwargs: pytest.fail("Fabric execution was contacted"),
    )

    assert main(
        [
            "build",
            str(invalid),
            "--bind",
            "Lakehouses/Raw_Dev=Lakehouse/Raw",
            "--workspace",
            "Analytics",
            "--weaver-lakehouse",
            "Control",
        ]
    ) == 1
    assert "invalid.txt" in capsys.readouterr().err


def _fabric_inputs():
    repository = parse_item_repository(Location(str(REPOSITORY)), store=LocalStore())
    bindings = effective_item_bindings(
        ItemBindings((parse_item_binding("Lakehouses/Raw_Dev=Lakehouse/Raw"),)),
        weaver_lakehouse="Control",
    )
    return repository, bindings


class _Transport:
    def __init__(self):
        self.files = {}
        self.directories = []
        self.deleted = []

    def make_directory(self, location):
        self.directories.append(location.value)

    def write(self, location, data):
        self.files[location.value] = data

    def delete(self, location, *, recursive=False):
        self.deleted.append((location.value, recursive))


class _Resolver:
    cli_root = Location("https://onelake/Control/Files/cli")
    build_bundles_root = Location("https://onelake/Control/Files/build_bundles")

    def cli_execution(self, execution_id):
        return self.cli_root / execution_id

    def cli_bundle(self, execution_id):
        return self.cli_execution(execution_id) / "install.weaver.zip"

    def build_bundle(self, name):
        return Location(f"https://onelake/Control/Files/build_bundles/{name}")


def _state_mapping(bindings):
    from weaver.build_bundle import BuildState
    from weaver.build_bundle.prune import TargetInventory
    from weaver.catalogue.state import Catalogue

    return BuildState(
        catalogue=Catalogue({}),
        target_inventories={
            binding.item: TargetInventory(
                target_id=binding.to_bound_target().id,
                kind=binding.to_bound_target().kind,
                target_name=binding.to_bound_target().name,
            )
            for binding in bindings.entries
        },
    ).to_mapping()


def test_fabric_adapter_reads_state_then_installs_uploaded_archive(monkeypatch):
    cli = importlib.import_module("weaver_cli.main")
    repository, bindings = _fabric_inputs()
    captured = {"bodies": []}
    transport = _Transport()

    class _Result:
        def __init__(self, payload):
            self.payload = payload

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
            captured["bodies"].append(body)
            if len(captured["bodies"]) == 1:
                return _Result(_state_mapping(bindings))
            return _Result(
                {"bundle_id": "ignored", "status": "succeeded", "sequences": []}
            )

    monkeypatch.setattr("weaver.fabric.LivySession", _Session)
    monkeypatch.setattr("weaver.fabric.list_workspace_livy_sessions", lambda *a, **k: ())
    monkeypatch.setattr("weaver.resolution.resolver_for", lambda _workspace: _Resolver())
    monkeypatch.setattr(cli, "_desktop_store", lambda _workspace: transport)
    workspace = FabricWorkspace(
        workspace="Analytics", weaver_lakehouse="Control", environment="Runtime"
    )
    result = cli._run_fabric_item_build(
        workspace,
        repository=repository,
        source_store=LocalStore(),
        bindings=bindings,
        control_lakehouse=LakehouseBinding(ItemRef("Control")),
        bundle_name="estate-build",
        source=str(REPOSITORY),
    )

    assert result["status"] == "succeeded"
    assert len(captured["bodies"]) == 2
    assert "read_build_state" in captured["bodies"][0]
    assert "install_bundle_archive" in captured["bodies"][1]
    assert all("parse_item_repository" not in body for body in captured["bodies"])
    assert any(path.endswith("/install.weaver.zip") for path in transport.files)
    assert result["archive"].endswith("/estate-build.weaver.zip")
    assert transport.deleted and transport.deleted[0][1] is True


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

    repository, bindings = _fabric_inputs()
    transport = _Transport()

    class _Result:
        def __init__(self, payload):
            self.payload = payload

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
            if events.count("run") == 0:
                events.append("run")
                return _Result(_state_mapping(bindings))
            events.append("run")
            return _Result(
                {"bundle_id": "ignored", "status": "succeeded", "sequences": []}
            )

    def inspect(*_args, **_kwargs):
        events.append("inspect")
        return (queued,)

    monkeypatch.setattr("weaver.fabric.LivySession", _Session)
    monkeypatch.setattr("weaver.fabric.list_workspace_livy_sessions", inspect)
    monkeypatch.setattr("weaver.resolution.resolver_for", lambda _workspace: _Resolver())
    monkeypatch.setattr(cli, "_desktop_store", lambda _workspace: transport)
    workspace = FabricWorkspace(
        workspace="Analytics", weaver_lakehouse="Control", environment="Runtime"
    )
    cli._run_fabric_item_build(
        workspace,
        repository=repository,
        source_store=LocalStore(),
        bindings=bindings,
        control_lakehouse=LakehouseBinding(ItemRef("Control")),
        bundle_name=None,
        source=str(REPOSITORY),
    )

    assert events == ["inspect", "start", "run", "run"]
    assert "Sales: session 7 (Scheduled/Queued/not_started)" in capsys.readouterr().err
