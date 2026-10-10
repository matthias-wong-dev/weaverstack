import importlib
from contextlib import nullcontext
from types import SimpleNamespace

import pytest
from support.weaver_test import weaver_test
from test_file_test_workspace_boundary import ASSUMPTION_SOURCE, TEST_SOURCE
from test_invocation_concurrency_boundary import Probe
from test_load_recursive_selection_cycle import installed, prepare
from test_report_build_cycle import prepared_project

import weaver
from weaver.runtime.validation_result import AssumptionResult, TestResult
from weaver.sessions import TestSession
from weaver.workspaces import ExecutionSettings, RunConcurrency, Workspace


@weaver_test()
@pytest.mark.parametrize("operation", ["build", "load", "test"])
@pytest.mark.parametrize("total", [None, 1, 3])
def test_cli_passes_override_to_public_api(monkeypatch, capsys, operation, total):
    cli = importlib.import_module("weaver_cli.main")
    words = [operation, "--json"]
    if total is not None:
        words += ["--concurrency", str(total)]
    args = cli.build_parser().parse_args(words)
    monkeypatch.setattr(
        cli, "_resolve_workspace", lambda args: Workspace(workspace="Demo")
    )
    monkeypatch.setattr(cli, "_running_session", lambda *args: nullcontext(object()))
    if operation != "build":
        monkeypatch.setattr(
            "weaver.sessions.host.use_or_create_session", lambda s, **kw: nullcontext(s)
        )
    seen = []

    def execute(*args, **asked):
        seen.append(asked)
        return SimpleNamespace(
            succeeded=True, status="passed", to_mapping=lambda: {"status": "passed"}
        )

    monkeypatch.setattr(weaver, operation, execute)
    if operation == "test":
        monkeypatch.setattr(
            cli, "_test_mapping", lambda report, **kw: report.to_mapping()
        )
    assert getattr(cli, "_" + operation + "_once")(args) == 0
    assert seen[0]["concurrency"] == total
    assert '"status": "passed"' in capsys.readouterr().out


@weaver_test()
@pytest.mark.parametrize("operation", ["build", "load", "test"])
@pytest.mark.parametrize("value", ["0", "-2", "1.5", "True", "no"])
def test_cli_rejects_invalid_concurrency(operation, value, capsys):
    cli = importlib.import_module("weaver_cli.main")
    with pytest.raises(SystemExit) as failure:
        cli.build_parser().parse_args([operation, "--concurrency", value])
    assert failure.value.code == 2
    assert "concurrency must be a positive integer" in capsys.readouterr().err


@weaver_test()
@pytest.mark.parametrize("total", [None, 1, 2])
@pytest.mark.parametrize("bundle_only", [False, True])
def test_public_build_scheduler_plumbing_and_bundle_identity(
    tmp_path, total, bundle_only
):
    root, session, events, _, _ = prepared_project(tmp_path)
    original = session.workspace
    calls = []
    execute = session.execute_mutation

    def captured(plan, payloads=None, **options):
        calls.append((plan, options))
        return execute(plan, payloads, **options)

    session.execute_mutation = captured
    result = weaver.build(
        root,
        items=[
            "SemanticModel/Reporting=SemanticModel/Reporting_Dev",
            "Report/Executive=Report/Executive_Dev",
        ],
        session=session,
        concurrency=total,
        bundle_only=bundle_only,
    )
    assert result.succeeded
    assert session.workspace is original
    if bundle_only:
        assert not calls and not events
        from weaver.locations import Location
        from weaver.mutation.bundle import load_bundle
        from weaver.store import FilesystemStore

        plan = load_bundle(Location(result.bundle_path), store=FilesystemStore()).plan
    else:
        ((plan, options),) = calls
        assert options.get("concurrency") == total
        assert ("concurrency" in options) == (total is not None)
    assert "concurrency" not in repr(plan.build_envelope)
    assert "concurrency" not in repr(plan.execution)


@weaver_test()
@pytest.mark.parametrize("transport", [False, True])
@pytest.mark.parametrize("total", [None, 1, 2])
def test_public_load_serialized_and_notebook_calls_keep_lanes(
    tmp_path, monkeypatch, transport, total
):
    catalogue = installed(tmp_path / "source", {"A": (), "B": (), "C": (), "D": ("A",)})
    session, calls, requests = prepare(monkeypatch, catalogue, transport=transport)
    original = session.workspace
    probe = Probe()

    def dispatch(n, **asked):
        probe.execute(n.logical_id.object_id.object, "warehouse")
        from weaver.runtime.load_result import LoadResult

        return LoadResult(succeeded=True)

    monkeypatch.setattr(weaver.run, "dispatch_primitive", dispatch)
    report = weaver.load("Warehouse/Reporting", concurrency=total, session=session)
    assert report.succeeded
    assert requests[0]["concurrency"] == total
    assert probe.peak == 1  # workspace Warehouse limit
    assert probe.events.index(("A", "end")) < probe.events.index(("D", "start"))
    assert session.workspace is original


@weaver_test()
@pytest.mark.parametrize("total", [None, 1, 2, 10])
@pytest.mark.parametrize("transport", [False, True])
def test_public_file_test_scheduler_and_no_recording(
    tmp_path, monkeypatch, total, transport
):
    paths = []
    for i in range(5):
        path = tmp_path / (
            ("Sales.NoViolations" if i == 4 else "Sales.Match") + str(i) + ".sql"
        )
        source = ASSUMPTION_SOURCE if i == 4 else TEST_SOURCE
        path.write_bytes(
            source.replace("Sales.Match", "Sales.Match" + str(i))
            .replace("Sales.NoViolations", "Sales.NoViolations" + str(i))
            .encode()
        )
        paths.append(path)
    workspace = Workspace(
        workspace="Demo",
        execution=ExecutionSettings(run=RunConcurrency(warehouse_concurrency=2)),
    )
    session = TestSession(workspace=workspace, executes_here=True)
    probe = Probe()
    import weaver.test_file as source_tests

    def execute(s, document, target, *, workspace):
        key = document.object_id.qualified
        probe.execute(key, "warehouse")
        if key.endswith("3"):
            raise RuntimeError("execution error")
        return (
            AssumptionResult()
            if document.kind == "assumption"
            else TestResult(missing_count=int(key.endswith("2")))
        ), ()

    monkeypatch.setattr(source_tests, "_run_warehouse", execute)
    entries = []
    if transport:

        def remote(run, *, workspace):
            entries.append(run.arguments())
            return run.decode(
                run.entry(session=session, workspace=workspace, **run.arguments())
            )

        monkeypatch.setattr(session, "execute_run", remote)
    report = weaver.test(
        "Warehouse/Reporting",
        files=paths,
        source=tmp_path / "empty",
        session=session,
        concurrency=total,
    )
    assert len(report.nodes) == 5
    assert [n.status for n in report.nodes] == [
        "passed",
        "passed",
        "failed",
        "invalid",
        "passed",
    ]
    assert probe.peak == (1 if total is None else min(total, 2))
    assert not session.tsql and not session.spark_sql
    assert report.workflow_id is None
    assert session.workspace is workspace
    if transport:
        assert entries[0]["concurrency"] == total
    count = len(probe.events)
    dry = weaver.test(
        "Warehouse/Reporting",
        files=paths,
        source=tmp_path / "empty",
        session=session,
        concurrency=total,
        dry_run=True,
    )
    assert dry.status == "planned" and len(probe.events) == count
