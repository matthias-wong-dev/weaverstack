"""Invocation limits across the published desktop and native source-Test boundary."""

import json
import textwrap

from support.weaver_test import register_session, weaver_test
from test_lakehouse_file_dispatch_boundary import source, workspace_for

import weaver
from weaver.sessions.console import ConsoleSession
from weaver.sessions.program import FabricProgram
from weaver.sessions.run_in_fabric import _workspace_literal


def definitions():
    return {
        "Sales.Pass.sql": source("Test", "pass").replace("Sales.Scope", "Sales.Pass"),
        "Sales.Fail.sql": source("Test", "fail").replace("Sales.Scope", "Sales.Fail"),
        "Sales.Error.sql": source("Test", "error").replace(
            "Sales.Scope", "Sales.Error"
        ),
        "Sales.Check.sql": source("Assumption", "pass").replace(
            "Sales.Scope", "Sales.Check"
        ),
    }


def assert_outcomes(mapping, physical):
    assert mapping["status"] == "invalid"
    assert mapping["workflow_id"] is None
    nodes = {n["logical_id"].rsplit("/", 1)[-1]: n for n in mapping["nodes"]}
    assert {name: n["status"] for name, n in nodes.items()} == {
        "Sales.Pass": "passed",
        "Sales.Fail": "failed",
        "Sales.Error": "invalid",
        "Sales.Check": "passed",
    }
    assert all(n["physical_target"] == f"Lakehouse/{physical}" for n in nodes.values())
    assert (
        nodes["Sales.Fail"]["missing_count"]
        == nodes["Sales.Fail"]["unexpected_count"]
        == 1
    )
    assert nodes["Sales.Error"]["error_message"]


@weaver_test(remote=True, resources={"rest", "onelake", "livy"})
def test_published_desktop_source_test_total_cap(
    fabric_workspace,
    fabric_target_lakehouse,
    fabric_credential,
    injected_weaver_bootstrap,
    exclusive_livy_slot,
    tmp_path,
    monkeypatch,
):
    from weaver.fabric import LivySession

    operation = workspace_for(fabric_workspace, fabric_target_lakehouse)
    config = tmp_path / "workspace.yml"
    config.write_text(
        f"workspace: {operation.workspace}\nenvironment: {operation.environment}\n"
        f"targets:\n  Lakehouse/Source: {fabric_target_lakehouse.name}\n",
        encoding="utf-8",
    )
    paths = []
    for name, body in definitions().items():
        path = tmp_path / name
        path.write_bytes(body.encode())
        paths.append(path)
    acquire = LivySession.for_workspace

    def attached(cls, workspace, **kwargs):
        assert workspace == operation
        opened = acquire(workspace, **kwargs)
        opened.weaver_bootstrap = injected_weaver_bootstrap
        return opened

    monkeypatch.setattr(LivySession, "for_workspace", classmethod(attached))
    with ConsoleSession(
        workspace=None, credential=fabric_credential, progress=False
    ) as session:
        register_session(session)
        for total in (1, 2):
            report = weaver.test(
                "Lakehouse/Source",
                files=paths,
                source=tmp_path / "empty",
                workspace_config=config,
                concurrency=total,
                session=session,
            )
            assert_outcomes(report.to_mapping(), fabric_target_lakehouse.name)
        before = len(session.calls) if hasattr(session, "calls") else None
        dry = weaver.test(
            "Lakehouse/Source",
            files=paths,
            source=tmp_path / "empty",
            workspace_config=config,
            concurrency=2,
            session=session,
            dry_run=True,
        )
        assert dry.status == "planned"
        if before is not None:
            assert len(session.calls) == before


def native_program(operation):
    return (
        "import json\nfrom pathlib import Path\nfrom tempfile import TemporaryDirectory\n"
        "import weaver\nfrom weaver.sessions import NotebookSession\n"
        "from weaver.workspaces import *\n"
        f"workspace = {_workspace_literal(operation)}\n"
        f"definitions = json.loads({json.dumps(definitions())!r})\n"
        + textwrap.dedent("""\
        import threading, time
        from weaver.run.runner import Runner
        original = Runner._dispatched
        lock = threading.Lock()
        active = peak = 0
        def measured(self, *args, **kwargs):
            global active, peak
            with lock:
                active += 1
                peak = max(peak, active)
            try:
                time.sleep(0.1)
                return original(self, *args, **kwargs)
            finally:
                with lock:
                    active -= 1
        Runner._dispatched = measured
        results = {}
        try:
            with TemporaryDirectory() as root, NotebookSession(workspace=workspace, spark=spark) as session:
                def forbidden(*args, **kwargs):
                    raise AssertionError('source run acquired catalogue or dispatched remotely')
                session.flusher = forbidden
                session.sql_executor = forbidden
                session.execute_run_in_fabric = forbidden
                paths = []
                for name, body in definitions.items():
                    path = Path(root) / name
                    path.write_bytes(body.encode())
                    paths.append(path)
                for total in (1, 2):
                    peak = 0
                    report = weaver.test('Lakehouse/Source', files=paths, source=Path(root) / 'empty', session=session, concurrency=total)
                    results[str(total)] = {'report': report.to_mapping(), 'peak': peak, 'active_after': active}
        finally:
            Runner._dispatched = original
        emit(results)
        """)
    )


@weaver_test(hosted=True, resources={"livy"})
def test_published_native_source_test_actual_scheduler_bound(
    fabric_workspace,
    fabric_target_lakehouse,
    weaver_session,
):
    operation = workspace_for(fabric_workspace, fabric_target_lakehouse)

    def remote_only():
        raise AssertionError("native qualification must execute in Fabric")

    result = weaver_session.execute_python(
        FabricProgram(
            name="concurrency_native_source_test",
            call=remote_only,
            source=native_program(operation),
        ),
        workspace=fabric_workspace,
    )
    assert set(result) == {"1", "2"}
    for total, observed in result.items():
        assert observed["peak"] == int(total)
        assert observed["active_after"] == 0
        assert_outcomes(observed["report"], fabric_target_lakehouse.name)
