"""The CLI is a thin adapter for physical-first Workspace builds."""

from __future__ import annotations

import ast
import base64
import importlib
import io
import json
from pathlib import Path
import zipfile

import pytest

from weaver import BuildResult
from weaver.workspaces import FabricWorkspace, LocalWorkspace
from weaver.build_bundle import ItemBindings, LakehouseBinding, effective_item_bindings, parse_item_binding
from weaver.targets import ItemRef
from weaver.store import LocalStore
from weaver.locations import Location
from weaver.declaration.repository import parse_item_repository
from weaver_cli import main
from weaver_cli.main import build_parser

REPOSITORY = Path(__file__).parent / "fixtures" / "build-lakehouse-item"
ROOT = Path(__file__).resolve().parents[1]
EXAMPLE_NOTEBOOK = ROOT / "examples" / "sales-estate" / "Sales example.ipynb"
EXAMPLE_REPOSITORY = ROOT / "examples" / "sales-estate" / "repository"


def test_build_parser_accepts_repeatable_physical_first_bindings():
    args = build_parser().parse_args(
        [
            "build",
            str(REPOSITORY),
            "--bind",
            "Lakehouse/Raw_Dev=Lakehouse/Raw",
            "--bind",
            "Warehouse/Reporting_Dev=Warehouse/Reporting",
            "--bundle",
            "reviewable-build",
            "--workspace",
            "Development",
            "--environment",
            "Runtime",
        ]
    )

    assert args.item_bindings == [
        "Lakehouse/Raw_Dev=Lakehouse/Raw",
        "Warehouse/Reporting_Dev=Warehouse/Reporting",
    ]
    assert not hasattr(args, "no_catalogue")


def test_build_parser_does_not_require_a_persisted_bundle_record():
    args = build_parser().parse_args(
        [
            "build",
            str(REPOSITORY),
            "--bind",
            "Lakehouse/Raw_Dev=Lakehouse/Raw",
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
            "Lakehouse/Raw_Dev=Lakehouse/Raw",
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


def test_build_handler_delegates_to_public_build_and_emits_json(monkeypatch, capsys):
    cli = importlib.import_module("weaver_cli.main")
    workspace = FabricWorkspace(
        workspace="Analytics",
        weaver_lakehouse="Control",
        environment="Runtime",
    )
    captured = {}
    monkeypatch.setattr(cli, "_resolve_workspace", lambda _args: workspace)

    def run(source, **kwargs):
        captured.update(source=source, **kwargs)
        return BuildResult(
            source=str(source),
            items=("Lakehouse/Raw", "Lakehouse/_weaver"),
            bundle_id="abc123",
            archive=None,
            status="succeeded",
        )

    monkeypatch.setattr("weaver.build", run)
    assert main(
        [
            "build",
            str(REPOSITORY),
            "--bind",
            "Lakehouse/Raw_Dev=Lakehouse/Raw",
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
    assert captured == {
        "source": str(REPOSITORY),
        "bind": ["Lakehouse/Raw_Dev=Lakehouse/Raw"],
        "workspace": workspace,
        "bundle": None,
    }


def test_local_build_routes_in_process(monkeypatch):
    cli = importlib.import_module("weaver_cli.main")
    workspace = LocalWorkspace(workspace="/tmp/local", weaver_lakehouse="Control")
    captured = {}
    monkeypatch.setattr(cli, "_resolve_workspace", lambda _args: workspace)

    def run(source, **kwargs):
        captured.update(source=source, **kwargs)
        return BuildResult(
            source=str(source),
            items=("Lakehouse/Raw", "Lakehouse/_weaver"),
            bundle_id="local",
            archive=None,
            status="succeeded",
        )

    monkeypatch.setattr("weaver.build", run)
    assert main(
        [
            "build",
            str(REPOSITORY),
            "--bind",
            "Lakehouse/Raw_Dev=Lakehouse/Raw",
            "--workspace",
            "/tmp/local",
            "--workspace-type",
            "local",
            "--weaver-lakehouse",
            "Control",
        ]
    ) == 0
    assert captured["workspace"] is workspace


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
            "Lakehouse/Raw_Dev=Lakehouse/Raw",
            "--workspace",
            "Analytics",
        ]
    ) == 1
    assert "requires an Environment" in capsys.readouterr().err


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
        "weaver.operations._build_desktop_fabric",
        lambda *_args, **_kwargs: pytest.fail("Fabric execution was contacted"),
    )

    assert main(
        [
            "build",
            str(invalid),
            "--bind",
            "Lakehouse/Raw_Dev=Lakehouse/Raw",
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
        ItemBindings((parse_item_binding("Lakehouse/Raw_Dev=Lakehouse/Raw"),)),
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
    operations = importlib.import_module("weaver.operations")
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
    monkeypatch.setattr("weaver.fabric.OneLakeDfsClient", lambda: transport)
    monkeypatch.setattr(
        "weaver.initialise.prepare_weaver_lakehouse", lambda *_args, **_kwargs: None
    )
    monkeypatch.setattr("weaver.resolution.resolver_for", lambda _workspace: _Resolver())
    workspace = FabricWorkspace(
        workspace="Analytics", weaver_lakehouse="Control", environment="Runtime"
    )
    result = operations._build_desktop_fabric(
        workspace,
        repository=repository,
        source_store=LocalStore(),
        bindings=bindings,
        control_lakehouse=LakehouseBinding(ItemRef("Control")),
        bundle_name="estate-build",
        source=str(REPOSITORY),
    )

    assert result.status == "succeeded"
    assert len(captured["bodies"]) == 2
    assert "initialise_weaver_lakehouse" in captured["bodies"][0]
    assert "read_build_state" in captured["bodies"][0]
    assert "install_bundle_archive" in captured["bodies"][1]
    assert all("parse_item_repository" not in body for body in captured["bodies"])
    assert any(path.endswith("/install.weaver.zip") for path in transport.files)
    assert result.archive.endswith("/estate-build.weaver.zip")
    assert transport.deleted and transport.deleted[0][1] is True


def test_build_parser_allows_notebook_resources_and_configured_bindings():
    args = build_parser().parse_args(
        ["build", "--workspace-config", "workspace.yml"]
    )
    assert args.repository is None
    assert args.item_bindings is None


class _NotebookResponse:
    def __init__(self, status_code=200, *, headers=None, body=None):
        self.status_code = status_code
        self.headers = headers or {}
        self._body = body or {}
        self.content = json.dumps(self._body).encode() if body is not None else b""

    def json(self):
        return self._body


class _NotebookClient:
    def __init__(self, *, notebook_exists=True):
        self.notebook_exists = notebook_exists
        self.requests = []
        self.job_reads = 0

    def paged(self, path, *, key="value"):
        if path == "workspaces":
            return [{"id": "workspace-id", "displayName": "Analytics"}]
        item_type = path.split("?type=", 1)[1] if "?type=" in path else None
        items = {
            "Notebook": [
                {
                    "id": "notebook-id",
                    "displayName": "Sales example",
                    "type": "Notebook",
                }
            ]
            if self.notebook_exists
            else [],
            "Lakehouse": [
                {
                    "id": "lakehouse-id",
                    "displayName": "Control",
                    "type": "Lakehouse",
                }
            ],
            "Environment": [
                {
                    "id": "environment-id",
                    "displayName": "Runtime",
                    "type": "Environment",
                }
            ],
        }
        return items.get(item_type, [])

    def request(self, method, path, *, payload=None, expected=(200, 201, 202)):
        self.requests.append((method, path, payload, expected))
        if path.endswith("/notebooks"):
            self.notebook_exists = True
            return _NotebookResponse(201, body={"id": "notebook-id"})
        if path.endswith("/updateDefinition"):
            return _NotebookResponse(200)
        if "/jobs/execute/instances" in path:
            return _NotebookResponse(
                202,
                headers={"Location": "https://api.fabric.test/jobs/job-id"},
            )
        raise AssertionError((method, path))

    def wait_for_operation(self, response):
        return response.json()

    def get_json(self, path):
        assert path == "https://api.fabric.test/jobs/job-id"
        self.job_reads += 1
        return {"status": "Completed", "exitValue": '{"status":"succeeded"}'}


def test_notebook_push_creates_a_public_definition(tmp_path):
    from weaver.fabric.notebooks import push_notebook

    source = tmp_path / "Sales example.py"
    source.write_text("print('hello')\n", encoding="utf-8")
    client = _NotebookClient(notebook_exists=False)

    result = push_notebook(source, workspace="Analytics", client=client)

    assert result.action == "created"
    method, path, payload, _ = client.requests[0]
    assert (method, path) == ("POST", "workspaces/workspace-id/notebooks")
    assert payload["displayName"] == "Sales example"
    assert payload["definition"]["format"] == "fabricGitSource"
    part = payload["definition"]["parts"][0]
    assert part["path"] == "notebook-content.py"
    assert base64.b64decode(part["payload"]) == b"print('hello')\n"
    assert all(
        part["path"] != "Resources" for part in payload["definition"]["parts"]
    )


def test_notebook_push_updates_an_existing_definition(tmp_path):
    from weaver.fabric.notebooks import push_notebook

    source = tmp_path / "Sales example.ipynb"
    source.write_text('{"cells": []}', encoding="utf-8")
    client = _NotebookClient()

    result = push_notebook(source, workspace="Analytics", client=client)

    assert result.action == "updated"
    assert client.requests[0][1].endswith("/notebook-id/updateDefinition")
    assert client.requests[0][2]["definition"]["format"] == "ipynb"


def test_notebook_run_attaches_lakehouse_and_environment_in_one_job(monkeypatch):
    from weaver.fabric.notebooks import run_notebook

    client = _NotebookClient()
    monkeypatch.setattr("weaver.fabric.notebooks.time.sleep", lambda _seconds: None)

    result = run_notebook(
        "Sales example",
        workspace="Analytics",
        lakehouse="Control",
        environment="Runtime",
        poll_interval=0,
        client=client,
    )

    assert result.succeeded
    assert result.exit_value == '{"status":"succeeded"}'
    assert client.job_reads == 1
    method, path, payload, _ = client.requests[0]
    assert method == "POST"
    assert path.endswith(
        "/notebooks/notebook-id/jobs/execute/instances?beta=false"
    )
    configuration = payload["executionData"]["computeConfiguration"]
    assert configuration == {
        "defaultLakehouse": {
            "referenceType": "ById",
            "itemId": "lakehouse-id",
            "workspaceId": "workspace-id",
        },
        "attachedEnvironment": {
            "referenceType": "ById",
            "itemId": "environment-id",
            "workspaceId": "workspace-id",
        },
    }


def test_sales_notebook_embeds_the_checked_in_repository_exactly():
    notebook = json.loads(EXAMPLE_NOTEBOOK.read_text(encoding="utf-8"))
    code = "".join(
        cell_source
        for cell in notebook["cells"]
        if cell["cell_type"] == "code"
        for cell_source in cell["source"]
    )
    assignment = next(
        node
        for node in ast.parse(code).body
        if isinstance(node, ast.Assign)
        and any(
            isinstance(target, ast.Name) and target.id == "REPOSITORY_ARCHIVE"
            for target in node.targets
        )
    )
    embedded = ast.literal_eval(assignment.value)
    with zipfile.ZipFile(io.BytesIO(base64.b64decode(embedded))) as archive:
        archived = {
            name: archive.read(name)
            for name in archive.namelist()
            if not name.endswith("/")
        }
    checked_in = {
        path.relative_to(EXAMPLE_REPOSITORY).as_posix(): path.read_bytes()
        for path in EXAMPLE_REPOSITORY.rglob("*")
        if path.is_file() and path.name != ".DS_Store"
    }

    assert archived == checked_in
    assert "workspace=" not in code
