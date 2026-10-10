"""SELECT-only source validations through desktop and native Fabric Sessions."""

import json

import pytest
from support.weaver_test import register_session, weaver_test

import weaver
from weaver.sessions.console import ConsoleSession
from weaver.sessions.program import FabricProgram
from weaver.sessions.run_in_fabric import _workspace_literal
from weaver.test_report import FAILED, INVALID, PASSED, PLANNED
from weaver.workspaces import TargetDeclaration, Workspace

CASES = ("pass", "fail", "error", "shape", "key", "reserved")


def source(kind, case):
    header = f"/*\n{kind} ID: Sales.Scope\nDescription: Source validation.\n"
    header += "Dependencies: []\n"
    if kind == "Test":
        header += "Primary key: ID\n"
    header += "*/\n"
    value = "cast('1.123456789012345678' as decimal(38,18))"
    actual = (
        value if case == "pass" else "cast('1.123456789012345679' as decimal(38,18))"
    )
    query = f"select 1 as ID, {actual} as Value"
    if case == "error":
        query = "select * from weaver_stage0b_missing_relation_71c883"
    elif case == "shape":
        query = "select 1 as ID"
    elif case == "key":
        query += f" union all select 1 as ID, {actual} as Value"
    elif case == "reserved":
        query = "select 1 as ID, 1 as _weaver_side"
    if kind == "Assumption":
        return header + query + (" where 1 = 0" if case == "pass" else "") + ";\n"
    return header + f"select 1 as ID, {value} as Value;\n" + query + ";\n"


def workspace_for(fabric_workspace, lakehouse):
    return Workspace(
        workspace=fabric_workspace.workspace,
        environment=fabric_workspace.environment,
        targets={"Lakehouse/Source": TargetDeclaration(lakehouse.name)},
    )


def assert_result(mapping, kind, case, physical):
    status = PASSED if case == "pass" else FAILED if case == "fail" else INVALID
    assert mapping["status"] == status, mapping
    assert mapping["workflow_id"] is None
    (node,) = mapping["nodes"]
    assert node["physical_target"] == f"Lakehouse/{physical}"
    assert node["kind"] == kind.casefold()
    if status == INVALID:
        assert node["error_message"]
    elif kind == "Test":
        count = int(case == "fail")
        assert node["missing_count"] == node["unexpected_count"] == count
    else:
        assert node["violation_count"] == (
            0 if case == "pass" else 2 if case == "key" else 1
        )


@weaver_test(remote=True, resources={"rest", "onelake", "livy"})
@pytest.mark.parametrize("default", ["other", "none"])
def test_desktop_source_validations_dispatch_to_requested_fabric(
    fabric_workspace,
    fabric_external_workspace_item,
    fabric_target_lakehouse,
    fabric_credential,
    injected_weaver_bootstrap,
    exclusive_livy_slot,
    tmp_path,
    monkeypatch,
    default,
):
    from weaver.fabric import LivySession

    operation = workspace_for(fabric_workspace, fabric_target_lakehouse)
    config = tmp_path / "workspace.yml"
    config.write_text(
        f"workspace: {operation.workspace}\nenvironment: {operation.environment}\n"
        f"targets:\n  Lakehouse/Source: {fabric_target_lakehouse.name}\n",
        encoding="utf-8",
    )
    acquire = LivySession.for_workspace
    attachments = []

    def attached(cls, workspace, **kwargs):
        assert workspace == operation
        assert kwargs["lakehouse"] == fabric_target_lakehouse.name
        opened = acquire(workspace, **kwargs)
        opened.weaver_bootstrap = injected_weaver_bootstrap
        attachments.append(opened)
        return opened

    monkeypatch.setattr(LivySession, "for_workspace", classmethod(attached))
    fallback = (
        Workspace(workspace=fabric_external_workspace_item.name, environment="Wrong")
        if default == "other"
        else None
    )
    with ConsoleSession(
        workspace=fallback, credential=fabric_credential, progress=False
    ) as session:
        register_session(session)
        submitted = []
        execute_python = session.execute_python

        def observe(program, *, workspace=None, timeout=None):
            assert workspace == operation
            assert "run_source_test_in_fabric" in program.source
            assert not program.resubmit
            submitted.append(program)
            return execute_python(program, workspace=workspace, timeout=timeout)

        monkeypatch.setattr(session, "execute_python", observe)
        for kind in ("Test", "Assumption"):
            for case in CASES if kind == "Test" else CASES[:3]:
                path = tmp_path / "Sales.Scope.sql"
                path.write_bytes(source(kind, case).encode("utf-8"))
                report = weaver.test(
                    "Lakehouse/Source",
                    files=path,
                    names="Sales.Scope",
                    source=tmp_path / "empty-project",
                    workspace_config=config,
                    session=session,
                )
                assert_result(
                    report.to_mapping(), kind, case, fabric_target_lakehouse.name
                )
                if case == "fail":
                    assert (
                        any(
                            str(row.get("Value")) == "1.123456789012345679"
                            for row in report.nodes[0].diagnostics
                        )
                        if kind == "Test"
                        else report.nodes[0].diagnostics
                    )
        before = len(submitted)
        dry = weaver.test(
            "Lakehouse/Source",
            files=path,
            source=tmp_path / "empty-project",
            workspace_config=config,
            session=session,
            dry_run=True,
        )
        assert dry.status == PLANNED
        assert len(submitted) == before
        assert all(scope.workspace == operation for scope in session._scopes.values())
    assert len(attachments) == 1
    assert attachments[0].session_url is None


def native_program(operation):
    definitions = {
        kind: {
            case: source(kind, case)
            for case in (CASES if kind == "Test" else CASES[:3])
        }
        for kind in ("Test", "Assumption")
    }
    return (
        "import json\nfrom pathlib import Path\nfrom tempfile import TemporaryDirectory\n"
        "import weaver\nfrom weaver.sessions import NotebookSession\n"
        "from weaver.workspaces import *\n"
        f"workspace = {_workspace_literal(operation)}\n"
        f"definitions = json.loads({json.dumps(definitions)!r})\n"
        "results = {}\n"
        "with TemporaryDirectory() as root, NotebookSession(workspace=workspace, spark=spark) as session:\n"
        "    def forbidden(*args, **kwargs):\n"
        "        raise AssertionError('source mode acquired a catalogue or remote run')\n"
        "    session.execute_run_in_fabric = forbidden\n"
        "    session.flusher = forbidden\n"
        "    session.sql_executor = forbidden\n"
        "    for kind, cases in definitions.items():\n"
        "        results[kind] = {}\n"
        "        for case, text in cases.items():\n"
        "            path = Path(root) / 'Sales.Scope.sql'\n"
        "            path.write_bytes(text.encode('utf-8'))\n"
        "            report = weaver.test('Lakehouse/Source', files=path, source=Path(root) / 'empty-project', session=session)\n"
        "            results[kind][case] = report.to_mapping()\n"
        "emit(results)\n"
    )


@weaver_test(hosted=True, resources={"livy"})
def test_native_source_validations_have_equivalent_outcomes(
    fabric_workspace, fabric_target_lakehouse, weaver_session
):
    operation = workspace_for(fabric_workspace, fabric_target_lakehouse)

    def remote_only():
        raise AssertionError("the native fixture must execute in Fabric")

    carried = weaver_session.execute_python(
        FabricProgram(
            name="source_native_test",
            call=remote_only,
            source=native_program(operation),
        ),
        workspace=fabric_workspace,
    )
    assert set(carried) == {"Test", "Assumption"}
    for kind, results in carried.items():
        assert set(results) == set(CASES if kind == "Test" else CASES[:3])
        for case, mapping in results.items():
            assert_result(mapping, kind, case, fabric_target_lakehouse.name)
