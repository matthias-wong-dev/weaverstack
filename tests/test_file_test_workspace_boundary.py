"""Source validation routing through real ConsoleSession scopes and resolution."""

from types import SimpleNamespace

import pytest
from support.weaver_test import weaver_test

import weaver
from weaver.sessions.console import ConsoleScope, ConsoleSession
from weaver.test_report import FAILED, INVALID, PASSED, PLANNED
from weaver.workspaces import Workspace

REQUESTED_ID = "11111111-1111-1111-1111-111111111111"
DEFAULT_ID = "22222222-2222-2222-2222-222222222222"
ITEM_IDS = {
    REQUESTED_ID: "33333333-3333-3333-3333-333333333333",
    DEFAULT_ID: "44444444-4444-4444-4444-444444444444",
}
PHYSICAL = "Reporting_WH"
TEST_SOURCE = """/*
Test ID: Sales.Match

Description: Expected and actual values match.
*/
select cast(1 as int) as Value;
select cast(1 as int) as Value;
"""
ASSUMPTION_SOURCE = """/*
Assumption ID: Sales.NoViolations

Description: No invalid values remain.
*/
select cast(1 as int) as Value where 1 = 0;
"""
SOURCES = {
    "Test": ("tests", "Sales.Match.sql", TEST_SOURCE),
    "Assumption": ("assumptions", "Sales.NoViolations.sql", ASSUMPTION_SOURCE),
}


class _Inventory:
    def __init__(self):
        self.paths = []
        self.missing = False

    def paged(self, path):
        self.paths.append(path)
        if path == "workspaces":
            return [
                {"id": REQUESTED_ID, "displayName": "Requested"},
                {"id": DEFAULT_ID, "displayName": "Other"},
            ]
        for scope, item in ITEM_IDS.items():
            if path == f"workspaces/{scope}/items?type=Warehouse":
                return (
                    []
                    if self.missing and scope == REQUESTED_ID
                    else [{"id": item, "displayName": PHYSICAL, "type": "Warehouse"}]
                )
        raise AssertionError(f"unexpected inventory request: {path}")

    def get_json(self, path):
        self.paths.append(path)
        for scope, item in ITEM_IDS.items():
            if path == f"workspaces/{scope}/warehouses/{item}/connectionString":
                return {
                    "connectionString": f"{scope}.datawarehouse.fabric.microsoft.com"
                }
        raise AssertionError(f"unexpected endpoint request: {path}")


@pytest.fixture
def routing(monkeypatch):
    recorded = SimpleNamespace(
        inventory=_Inventory(), acquired=[], queries=[], counts=0, error=None
    )

    class RecordingExecutor:
        def __init__(self, pool, *, owns_pool):
            assert owns_pool
            self.pool = pool
            recorded.acquired.append(pool.endpoint)

        def query_result_sets(self, statement, parameters=None):
            recorded.queries.append((self.pool.endpoint, statement))
            if recorded.error is not None:
                raise recorded.error
            count = (
                recorded.counts
                if self.pool.endpoint.workspace_id == REQUESTED_ID
                else 1 - recorded.counts
            )
            return (
                ({"_weaver_side": "expected", "Value": 7},) if count else (),
                (
                    {
                        "missing_count": count,
                        "unexpected_count": count,
                        "violation_count": count,
                    },
                ),
            )

        def close(self):
            self.pool.close()

    def no_spark(*args, **kwargs):
        raise AssertionError("Warehouse-only validation must not acquire Spark")

    monkeypatch.setattr(ConsoleScope, "_fabric_client", lambda self: recorded.inventory)
    monkeypatch.setattr("weaver.fabric.sql.PooledSqlExecutor", RecordingExecutor)
    monkeypatch.setattr(ConsoleScope, "_acquire_livy", no_spark)
    return recorded


def _session(default="Other"):
    return ConsoleSession(
        workspace=None if default is None else Workspace(workspace=default),
        credential=SimpleNamespace(get_token=lambda *args, **kwargs: None),
        progress=False,
    )


def _external(tmp_path):
    path = tmp_path / "Sales.Match.sql"
    path.write_bytes(TEST_SOURCE.encode("utf-8"))
    return path


def _assert_requested_only(routing, report):
    assert report.workflow_id is None
    assert len(routing.acquired) == 1
    endpoint = routing.acquired[0]
    assert endpoint.workspace_id == REQUESTED_ID
    assert endpoint.warehouse_id == ITEM_IDS[REQUESTED_ID]
    assert endpoint.database == PHYSICAL
    assert endpoint.server == f"{REQUESTED_ID}.datawarehouse.fabric.microsoft.com"
    assert routing.queries
    assert all(bound == endpoint for bound, _ in routing.queries)
    assert all(DEFAULT_ID not in path for path in routing.inventory.paths)
    assert all(node.physical_target == f"Warehouse/{PHYSICAL}" for node in report.nodes)


@weaver_test()
@pytest.mark.parametrize("count,status", [(0, PASSED), (1, FAILED)])
def test_external_test_uses_explicit_workspace_for_both_verdicts(
    tmp_path, routing, count, status
):
    routing.counts = count
    path = _external(tmp_path)
    with _session() as session:
        report = weaver.test(
            f"Warehouse/{PHYSICAL}",
            files=path,
            source=tmp_path / "project",
            workspace="Requested",
            session=session,
        )
        assert session.workspace.workspace == "Other"
        assert all(scope.livy.attempts == 0 for scope in session._scopes.values())
    _assert_requested_only(routing, report)
    assert report.status == status
    assert report.nodes[0].result.missing_count == count
    assert report.nodes[0].result.unexpected_count == count
    assert len(routing.queries) == 1
    _assert_batch(routing, "Test")


def _assert_batch(routing, kind):
    from weaver.declaration import read_source_document
    from weaver.declaration.tsql_validation import generate_tsql_validation_batch

    directory, filename, text = SOURCES[kind]
    document = read_source_document(
        f"Warehouse/Reporting/{directory}/{filename}", text.encode(), "Warehouse"
    )
    assert [sql for _, sql in routing.queries] == [
        generate_tsql_validation_batch(document.document, document.sql_body)
    ]


@pytest.fixture
def source_selection(tmp_path):
    from factories import schema_document

    project = tmp_path / "project"
    item = project / "Warehouse/Reporting"
    schema = item / "schemas/Sales.yml"
    schema.parent.mkdir(parents=True)
    schema.write_text(schema_document("Sales"), encoding="utf-8")
    config = tmp_path / "workspace.yml"
    config.write_text(
        f"workspace: Requested\ntargets:\n  Warehouse/Reporting: {PHYSICAL}\n",
        encoding="utf-8",
    )
    for directory, filename, text in SOURCES.values():
        path = item / directory / filename
        path.parent.mkdir(parents=True)
        path.write_bytes(text.encode("utf-8"))
        (tmp_path / filename).write_bytes(text.encode("utf-8"))

    def select(selection, kind):
        directory, filename, _ = SOURCES[kind]
        files = {
            "external": tmp_path / filename,
            "declared": item / directory / filename,
            "directory": item,
            "glob": str(item / "*/*.sql"),
            "project": None,
        }[selection]
        return {
            "items": "Warehouse/Reporting",
            "files": files,
            "names": filename.removesuffix(".sql"),
            "source": project,
            "workspace": "Requested",
            "workspace_config": config,
        }

    return select


@weaver_test()
@pytest.mark.parametrize("default", ["Other", None])
@pytest.mark.parametrize(
    "selection", ["external", "declared", "directory", "glob", "project"]
)
@pytest.mark.parametrize("kind", ["Test", "Assumption"])
@pytest.mark.parametrize("count,status", [(0, PASSED), (1, FAILED)])
def test_source_selection_preserves_requested_mapping(
    routing, source_selection, default, selection, kind, count, status
):
    routing.counts = count
    with _session(default) as session:
        report = weaver.test(session=session, **source_selection(selection, kind))
        assert all(scope.livy.attempts == 0 for scope in session._scopes.values())
        assert all(
            scope.workspace.workspace == "Requested"
            for scope in session._scopes.values()
        )
        assert not session.closed
    _assert_requested_only(routing, report)
    _assert_batch(routing, kind)
    assert report.status == status
    (node,) = report.nodes
    assert node.kind == kind
    assert (
        node.logical_id
        == f"Warehouse/Reporting/{SOURCES[kind][1].removesuffix('.sql')}"
    )
    assert node.result.succeeded == (count == 0)
    assert node.diagnostics == (
        ({"_weaver_side": "expected", "Value": 7},) if count else ()
    )
    if kind == "Test":
        assert node.result.missing_count == count
        assert node.result.unexpected_count == count
    else:
        assert node.result.violation_count == count


@weaver_test()
@pytest.mark.parametrize(
    "selection", ["external", "declared", "directory", "glob", "project"]
)
@pytest.mark.parametrize("kind", ["Test", "Assumption"])
def test_source_dry_run_acquires_no_sql_or_spark(
    routing, source_selection, selection, kind
):
    with _session() as session:
        report = weaver.test(
            session=session, dry_run=True, **source_selection(selection, kind)
        )
        assert all(scope.livy.attempts == 0 for scope in session._scopes.values())
        assert session.telemetry.events() == ()
    assert report.status == PLANNED
    assert report.workflow_id is None
    assert all(not node.executed for node in report.nodes)
    assert routing.inventory.paths == []
    assert routing.acquired == []
    assert routing.queries == []


@weaver_test()
@pytest.mark.parametrize("kind", ["Test", "Assumption"])
def test_unavailable_requested_target_cannot_fall_back(routing, source_selection, kind):
    routing.inventory.missing = True
    with _session() as session:
        report = weaver.test(session=session, **source_selection("external", kind))
    assert report.status == INVALID
    assert not report.succeeded
    assert report.workflow_id is None
    assert routing.acquired == []
    assert routing.queries == []
    assert all(DEFAULT_ID not in path for path in routing.inventory.paths)
    assert report.nodes[0].result.error_message
    assert "Requested" in report.nodes[0].result.error_message
    assert PHYSICAL in report.nodes[0].result.error_message


@weaver_test()
@pytest.mark.parametrize("kind", ["Test", "Assumption"])
def test_source_execution_error_is_invalid(routing, source_selection, kind):
    routing.error = RuntimeError("query refused")
    with _session() as session:
        report = weaver.test(session=session, **source_selection("declared", kind))
    _assert_requested_only(routing, report)
    _assert_batch(routing, kind)
    assert report.status == INVALID
    assert not report.succeeded
    assert not report.nodes[0].result.succeeded
    assert "query refused" in report.nodes[0].result.error_message


@weaver_test()
@pytest.mark.parametrize("kind", ["Test", "Assumption"])
def test_explicit_files_record_nothing_with_a_configured_catalogue(
    routing, source_selection, kind
):
    with _session() as session:
        report = weaver.test(
            session=session,
            catalogue="Warehouse/Catalogue",
            **source_selection("declared", kind),
        )
    assert report.status == PASSED
    _assert_requested_only(routing, report)
    _assert_batch(routing, kind)


@weaver_test()
@pytest.mark.parametrize(
    "selection", ["external", "declared", "directory", "glob", "project"]
)
@pytest.mark.parametrize("kind", ["Test", "Assumption"])
@pytest.mark.parametrize("count,status", [(0, PASSED), (1, FAILED)])
def test_cli_source_selection_reaches_requested_workspace(
    routing,
    source_selection,
    selection,
    kind,
    count,
    status,
    capsys,
    desktop_credential,
):
    import json

    from weaver.test_report import ValidationRunReport
    from weaver_cli import main

    routing.counts = count
    selected = source_selection(selection, kind)
    args = [
        "test",
        selected["items"],
        "--source",
        str(selected["source"]),
        "--workspace",
        selected["workspace"],
        "--workspace-config",
        str(selected["workspace_config"]),
        "--name",
        selected["names"],
        "--json",
    ]
    if selected["files"] is not None:
        args.extend(["--file", str(selected["files"])])
    assert main(args) == count
    payload = json.loads(capsys.readouterr().out)
    report = ValidationRunReport.from_mapping(payload)
    assert report.status == status
    _assert_requested_only(routing, report)
    _assert_batch(routing, kind)


@weaver_test()
@pytest.mark.parametrize("kind", ["Test", "Assumption"])
@pytest.mark.parametrize(
    "case,status", [("pass", PASSED), ("fail", FAILED), ("error", INVALID)]
)
def test_live_source_helper_uses_public_routing_locally(
    tmp_path, routing, source_selection, kind, case, status
):
    from fabric.test_warehouse_file_routing_boundary import _run

    from weaver.config import load_workspace

    selected = source_selection("external", kind)
    routing.counts = int(case == "fail")
    if case == "error":
        routing.error = RuntimeError("recorded execution error")
    with _session() as session:
        observed = SimpleNamespace(
            session=session,
            workspace=load_workspace(selected["workspace_config"]),
            config=selected["workspace_config"],
            warehouse=PHYSICAL,
        )
        report = _run(observed, tmp_path, kind, case)
    _assert_requested_only(routing, report)
    assert [sql for _, sql in routing.queries] == [observed.expected_batch]
    assert report.status == status
