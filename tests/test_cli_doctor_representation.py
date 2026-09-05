"""Workspace-only doctor arguments and complete diagnostic output."""

import importlib
import json

import pytest
from support.weaver_test import weaver_test

from weaver.operations.doctor import Check, DoctorReport
from weaver_cli.doctor import render
from weaver_cli.main import build_parser


@pytest.mark.parametrize(
    "args",
    [
        [],
        ["--workspace-config", "workspace-config.yml"],
        ["--workspace", "Analytics", "--catalogue", "Warehouse/Catalogue"],
        ["--workspace", "Analytics", "--environment", "Weaver"],
    ],
)
@weaver_test()
def test_doctor_requires_workspace_and_rejects_project_options(args):
    with pytest.raises(SystemExit):
        build_parser().parse_args(["doctor", *args])


@weaver_test()
def test_render_includes_all_statuses_and_resource_details(capsys):
    render(
        DoctorReport(
            checks=(
                Check("Authentication", "ok", "Azure CLI"),
                Check("Fabric REST", "ok", "7 workspaces visible"),
                Check("OneLake", "missing", "No Lakehouse"),
                Check(
                    "Warehouse TDS", "failed", "Query rejected", via="Warehouse/Curated"
                ),
                Check("Fabric Spark / Livy", "error", "Timeout"),
            )
        )
    )
    text = capsys.readouterr().out
    for phrase in (
        "Azure CLI",
        "7 workspaces visible",
        "via Warehouse/Curated",
        "OK",
        "MISSING",
        "FAILED",
        "ERROR",
    ):
        assert phrase in text
    assert "Everything checked is reachable." not in text
    lines = [
        line
        for line in text.splitlines()
        if line.rstrip().endswith(("OK", "MISSING", "FAILED", "ERROR"))
    ]
    assert len({len(line) - len(line.split()[-1]) for line in lines}) == 1


@pytest.mark.parametrize("status,exit_code", [("ok", 0), ("missing", 1), ("error", 1)])
@weaver_test()
def test_cli_json_contains_the_whole_report_and_exit_status(
    monkeypatch, tmp_path, capsys, status, exit_code
):
    cli = importlib.import_module("weaver_cli.main")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(cli, "_prefer_desktop_credential", lambda: None)
    report = DoctorReport(
        checks=(Check("Workspace Analytics", status),),
        workspace="Analytics",
        authentication={"path": "Browser sign-in"},
    )
    calls = []
    monkeypatch.setattr(
        "weaver.operations.doctor.doctor",
        lambda **kwargs: calls.append(kwargs) or report,
    )
    args = build_parser().parse_args(["doctor", "--workspace", "Analytics", "--json"])
    assert cli.handle_doctor(args) == exit_code
    assert json.loads(capsys.readouterr().out) == report.to_mapping()
    assert set(calls[0]) == {"workspace", "session"}


@weaver_test()
def test_authentication_renders_only_known_identity_fields(capsys):
    render(
        DoctorReport(
            checks=(
                Check("Authentication", "ok", "Browser sign-in"),
                Check("Fabric REST", "ok"),
            ),
            authentication={
                "account": "user@example.com",
                "tenant": "tenant-id",
                "token": "secret-token",
                "client_secret": "secret-password",
            },
        )
    )
    text = capsys.readouterr().out
    authentication, _ = text.split("Fabric REST", 1)
    assert "Browser sign-in" in authentication
    assert "Account: user@example.com" in authentication
    assert "Tenant: tenant-id" in authentication
    assert "secret" not in text
