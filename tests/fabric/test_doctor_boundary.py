"""Workspace doctor through desktop and in-Fabric Sessions."""

import json

from support.weaver_test import weaver_test

from weaver.operations.doctor import doctor


@weaver_test(remote=True, resources={"rest", "onelake", "tds", "livy"})
def test_doctor_starts_environment_less_spark_and_probes_discovered_items(
    fabric_workspace, rest_session, exclusive_livy_slot, tmp_path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    report = doctor(workspace=fabric_workspace.workspace, session=rest_session)
    assert report.succeeded, report.to_mapping()
    assert all(check.passed for check in report.checks), report.to_mapping()
    assert report.authentication["path"]
    assert report.workspaces
    assert [check.name for check in report.checks[-3:]] == [
        "OneLake",
        "Warehouse TDS",
        "Fabric Spark / Livy",
    ]
    assert report.checks[-1].via == report.checks[-3].via


@weaver_test(remote=True, resources={"rest"})
def test_missing_workspace_stops_before_endpoint_probes(rest_session):
    report = doctor(workspace="weavertest_no_such_workspace", session=rest_session)
    assert not report.succeeded
    assert report.checks[-1].status == "missing"
    assert len(report.checks) == 3


@weaver_test(hosted=True)
def test_doctor_uses_the_notebook_session_without_project_configuration(
    fabric_workspace, livy_session
):
    source = (
        "from weaver.operations.doctor import doctor\n"
        "from weaver.sessions.notebook import NotebookSession\n"
        "from weaver.workspaces import Workspace\n"
        f"workspace = {json.dumps(fabric_workspace.workspace)}\n"
        "with NotebookSession(workspace=Workspace(workspace=workspace)) as session:\n"
        "    report = doctor(workspace=workspace, session=session)\n"
        "emit(report.to_mapping())\n"
    )
    report = livy_session.run(source).payload
    assert report["succeeded"], report
    assert all(check["status"] == "ok" for check in report["checks"]), report
    assert report["authentication"]["path"] == "Session identity"
