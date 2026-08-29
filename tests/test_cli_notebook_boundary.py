from __future__ import annotations

import importlib
from types import SimpleNamespace

import pytest
from support.weaver_test import weaver_test

from weaver.declaration.model import WeaverItemId
from weaver.errors import CommandError
from weaver.workspaces import TargetDeclaration, Workspace
from weaver_cli.main import handle_notebook_run

CLI_MAIN = importlib.import_module("weaver_cli.main")


def _args(**overrides):
    values = {
        "name": "Refresh",
        "lakehouse": None,
        "no_wait": False,
        "timeout": 10.0,
        "poll_interval": 0.1,
        "json": False,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


@weaver_test()
def test_notebook_run_does_not_treat_the_catalogue_as_a_lakehouse(monkeypatch):
    workspace = Workspace(
        workspace="Analytics",
        catalogue="Warehouse/Weaver",
        environment="Weaver",
    )
    monkeypatch.setattr(CLI_MAIN, "_fabric_cli_workspace", lambda args: workspace)

    with pytest.raises(CommandError, match="Lakehouse is required"):
        handle_notebook_run(_args())


@weaver_test()
def test_notebook_run_uses_one_configured_lakehouse(monkeypatch):
    workspace = Workspace(
        workspace="Analytics",
        catalogue="Warehouse/Weaver",
        environment="Weaver",
        targets={WeaverItemId.parse("Lakehouse/Sales"): TargetDeclaration("Sales")},
    )
    seen = []
    monkeypatch.setattr(CLI_MAIN, "_fabric_cli_workspace", lambda args: workspace)
    monkeypatch.setattr(
        "weaver.fabric.notebooks.run_notebook",
        lambda *args, **kwargs: (
            seen.append(kwargs)
            or SimpleNamespace(
                notebook="Refresh",
                status="Completed",
                job_url="job",
                exit_value=None,
                succeeded=True,
            )
        ),
    )

    assert handle_notebook_run(_args()) == 0
    assert seen[0]["lakehouse"] == "Sales"
