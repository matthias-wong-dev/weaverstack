"""File-mode Warehouse routing on existing items, with SELECT-only sources."""

import os
from types import SimpleNamespace

import pytest
from support.weaver_test import register_session, weaver_test

import weaver
from weaver.config import load_workspace
from weaver.declaration import read_source_document
from weaver.declaration.tsql_validation import generate_tsql_validation_batch
from weaver.fabric.resources import WAREHOUSE, find_item
from weaver.sessions.console import ConsoleSession
from weaver.targets import WarehouseTarget
from weaver.test_report import FAILED, INVALID, PASSED, PLANNED
from weaver.workspaces import Workspace


def _source(kind, case, warehouse):
    expected = warehouse if case == "pass" else f"{warehouse}_mismatch"
    literal = expected.replace("'", "''")
    actual = "cast(db_name() as varchar(128))"
    if case == "error":
        actual = "cast(cast('invalid_integer' as int) as varchar(128))"
    body = (
        f"select cast('{literal}' as varchar(128)) as DatabaseName;\n"
        f"select {actual} as DatabaseName;\n"
        if kind == "Test"
        else f"select {actual} as DatabaseName where {actual} <> '{literal}';\n"
    )
    return f"/*\n{kind} ID: Sales.Scope\n\nDescription: Requested Warehouse is used.\n*/\n{body}"


@pytest.fixture
def routing_session(
    fabric_workspace_item,
    fabric_external_workspace_item,
    fabric_client,
    fabric_credential,
    tmp_path,
    monkeypatch,
    request,
):
    warehouse = os.environ.get("WEAVER_PYTEST_WAREHOUSE", "PYTEST_WH_1")
    item = find_item(
        fabric_workspace_item, warehouse, item_type=WAREHOUSE, client=fabric_client
    )
    config = tmp_path / "workspace.yml"
    config.write_text(
        f"workspace: {fabric_workspace_item.name}\n"
        f"targets:\n  Warehouse/Reporting: {warehouse}\n",
        encoding="utf-8",
    )
    workspace = load_workspace(config)
    default = (
        Workspace(workspace=fabric_external_workspace_item.name)
        if request.param == "other"
        else None
    )
    with ConsoleSession(
        workspace=default, credential=fabric_credential, progress=False
    ) as session:
        register_session(session)
        target = WarehouseTarget.parse(warehouse)
        executor = session.sql_executor(target, workspace=workspace)
        endpoint = executor.pool.endpoint
        assert endpoint.workspace_id == fabric_workspace_item.id
        assert endpoint.warehouse_id == item.id
        observed = SimpleNamespace(
            session=session,
            workspace=workspace,
            config=config,
            warehouse=warehouse,
            calls=[],
            statements=[],
            expected_batch=None,
        )
        sql_executor = session.sql_executor
        query_result_sets = executor.query_result_sets

        def acquire(target, *, workspace=None):
            observed.calls.append((target, workspace))
            return sql_executor(target, workspace=workspace)

        def query(statement, parameters=None):
            observed.statements.append(statement)
            assert statement == observed.expected_batch
            return query_result_sets(statement, parameters)

        def no_record(*args, **kwargs):
            raise AssertionError(
                "file mode must issue only the source validation batch"
            )

        monkeypatch.setattr(session, "sql_executor", acquire)
        monkeypatch.setattr(executor, "query_result_sets", query)
        for method in (
            "execute",
            "execute_script",
            "execute_each",
            "query",
            "call_procedure",
            "call_procedure_with_results",
        ):
            monkeypatch.setattr(executor, method, no_record)
        yield observed


def _run(observed, tmp_path, kind, case, *, dry_run=False):
    source = _source(kind, case, observed.warehouse)
    directory = "tests" if kind == "Test" else "assumptions"
    parsed = read_source_document(
        f"Warehouse/Reporting/{directory}/Sales.Scope.sql", source.encode(), WAREHOUSE
    )
    observed.expected_batch = generate_tsql_validation_batch(
        parsed.document, parsed.sql_body
    )
    path = tmp_path / "Sales.Scope.sql"
    path.write_bytes(source.encode("utf-8"))
    report = weaver.test(
        "Warehouse/Reporting",
        source=tmp_path / "project",
        files=path,
        workspace=observed.workspace.workspace,
        workspace_config=observed.config,
        session=observed.session,
        dry_run=dry_run,
    )
    assert report.workflow_id is None
    assert all(
        scope.workspace.workspace == observed.workspace.workspace
        for scope in observed.session._scopes.values()
    )
    assert all(scope.livy.attempts == 0 for scope in observed.session._scopes.values())
    assert report.nodes[0].physical_target == f"Warehouse/{observed.warehouse}"
    return report


@weaver_test(remote=True, resources={"tds"})
@pytest.mark.parametrize("routing_session", ["other", "none"], indirect=True)
@pytest.mark.parametrize("kind", ["Test", "Assumption"])
@pytest.mark.parametrize(
    "case,status", [("pass", PASSED), ("fail", FAILED), ("error", INVALID)]
)
def test_source_validation_uses_requested_warehouse(
    routing_session, tmp_path, kind, case, status
):
    observed = routing_session
    report = _run(observed, tmp_path, kind, case)
    assert report.status == status
    assert len(observed.calls) == 1
    target, workspace = observed.calls[0]
    assert target == WarehouseTarget.parse(observed.warehouse)
    assert workspace == observed.workspace
    assert observed.statements == [observed.expected_batch]
    result = report.nodes[0].result
    assert result.succeeded == (case == "pass")
    if case == "error":
        assert result.error_message
    elif kind == "Test":
        count = int(case == "fail")
        assert result.missing_count == count
        assert result.unexpected_count == count
    else:
        assert result.violation_count == int(case == "fail")


@weaver_test(remote=True)
@pytest.mark.parametrize("routing_session", ["other"], indirect=True)
@pytest.mark.parametrize("kind", ["Test", "Assumption"])
def test_source_dry_run_queries_nothing(routing_session, tmp_path, kind):
    report = _run(routing_session, tmp_path, kind, "pass", dry_run=True)
    assert report.status == PLANNED
    assert not report.nodes[0].executed
    assert routing_session.calls == []
    assert routing_session.statements == []
