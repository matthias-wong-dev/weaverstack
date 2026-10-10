"""Source validations cross the Session run boundary with operation context."""

import json
from decimal import Decimal
from types import SimpleNamespace

import pytest
from support.weaver_test import weaver_test

import weaver
from weaver.sessions.console import ConsoleSession
from weaver.test_report import FAILED, INVALID, PASSED, PLANNED, ValidationRunReport
from weaver.workspaces import Workspace

SOURCE = """/*
Test ID: Sales.Match
Description: Values match.
Dependencies: []
Primary key: ID
*/
select 1 as ID, cast(1.123456789012345678 as decimal(38,18)) as Value from Sales.Input;
select 1 as ID, cast(1.123456789012345678 as decimal(38,18)) as Value from Sales.Input;
"""


@pytest.fixture
def selection(tmp_path):
    project = tmp_path / "project"
    path = project / "Lakehouse/Sales/tests/Sales.Match.sql"
    path.parent.mkdir(parents=True)
    path.write_bytes(SOURCE.encode())
    config = tmp_path / "workspace.yml"
    config.write_text(
        "workspace: Requested\nenvironment: Runtime/Selected\n"
        "targets:\n  Lakehouse/Sales: Sales_LH\n",
        encoding="utf-8",
    )
    return dict(source=project, files=path, workspace_config=config)


@weaver_test()
@pytest.mark.parametrize("default", [None, "Other"])
def test_desktop_source_sql_uses_session_fabric_boundary(
    monkeypatch, selection, default
):
    sent = []
    expected = ValidationRunReport(status=PASSED, nodes=())

    def remote(self, run, *, workspace=None):
        sent.append((run, workspace))
        return expected

    monkeypatch.setattr(ConsoleSession, "execute_run_in_fabric", remote)
    with ConsoleSession(
        workspace=None if default is None else Workspace(workspace=default),
        credential=SimpleNamespace(get_token=lambda *args, **kwargs: None),
        progress=False,
    ) as session:
        report = weaver.test(session=session, **selection)
        assert report is expected
        ((run, workspace),) = sent
        assert workspace.workspace == "Requested"
        assert str(workspace.environment) == "Runtime/Selected"
        assert run.needs_spark
        assert run.name == "test"
        assert json.loads(json.dumps(run.arguments()))["validations"]
        assert (
            session.workspace is None
            if default is None
            else session.workspace.workspace == default
        )
        assert all(scope.livy.attempts == 0 for scope in session._scopes.values())


@weaver_test()
def test_remote_program_carries_physical_target_mappings(selection):
    from weaver.config import load_workspace
    from weaver.sessions.run_in_fabric import _workspace_literal

    workspace = load_workspace(selection["workspace_config"])
    namespace = {}
    exec(
        "from weaver.workspaces import *\n"
        + "carried = "
        + _workspace_literal(workspace),
        namespace,
    )
    assert namespace["carried"] == workspace


@weaver_test()
def test_native_source_sql_uses_operation_scope(monkeypatch, selection):
    from weaver.config import load_workspace
    from weaver.sessions.notebook import NotebookSession
    from weaver.spark import FabricSparkTarget

    workspace = load_workspace(selection["workspace_config"])
    calls = []
    resolver = SimpleNamespace(
        spark_root=lambda item: (
            "abfss://requested@onelake.dfs.fabric.microsoft.com/sales"
        ),
        spark_destination=lambda item: FabricSparkTarget("Requested", "Sales_LH"),
    )
    spark = object()
    with NotebookSession(
        workspace=Workspace(workspace="Requested", environment="Wrong"),
        spark=spark,
        resolver=resolver,
    ) as session:
        original_spark, original_resolver = session.spark, session.resolver

        def scoped_spark(selected=None):
            calls.append(("spark", selected))
            assert selected == workspace
            return original_spark(selected)

        def scoped_resolver(selected=None):
            calls.append(("resolver", selected))
            assert selected == workspace
            return original_resolver(selected)

        def read(active, *, sql, what):
            assert active is spark
            assert "1.123456789012345678" in sql
            return object(), object()

        monkeypatch.setattr(session, "spark", scoped_spark)
        monkeypatch.setattr(session, "resolver", scoped_resolver)
        monkeypatch.setattr(
            "weaver.runtime.spark_sql_validation.read_spark_sql_test", read
        )
        monkeypatch.setattr(
            "weaver.runtime.test_compare.compare",
            lambda *args, **kwargs: SimpleNamespace(collect=lambda: []),
        )
        report = weaver.test(session=session, **selection)
        assert report.status == PASSED, report.nodes
        assert report.workflow_id is None
        assert report.nodes[0].physical_target == "Lakehouse/Sales_LH"
        assert calls and all(selected == workspace for _, selected in calls)


@pytest.fixture
def transport(monkeypatch):
    from test_run_in_fabric_boundary import Store

    from weaver.fabric.resources import Item
    from weaver.locations import Location
    from weaver.runtime.spark_sql_validation import read_spark_sql_test
    from weaver.runtime.test_compare import compare as compare_results
    from weaver.sessions.notebook import NotebookSession
    from weaver.spark import FabricSparkTarget

    observed = SimpleNamespace(
        programs=[],
        opened=[],
        calls=[],
        rows=(),
        error=None,
        read_spark_sql_test=read_spark_sql_test,
        compare=compare_results,
    )
    store = Store()
    resolver = SimpleNamespace(
        files_root=lambda item: Location(
            "abfss://requested@onelake.dfs.fabric.microsoft.com/sales/Files"
        ),
        spark_root=lambda item: (
            "abfss://requested@onelake.dfs.fabric.microsoft.com/sales"
        ),
        spark_destination=lambda item: FabricSparkTarget("Requested", item.name),
    )
    spark = object()

    def native(*, workspace, spark):
        session = NotebookSession(workspace=workspace, spark=spark, resolver=resolver)
        observed.opened.append(session)
        return session

    def read(active, *, sql, what):
        assert active is spark
        observed.calls.append(("spark_sql", sql, what))
        if observed.error:
            raise observed.error
        return object(), object()

    def frame():
        return SimpleNamespace(
            collect=lambda: [
                SimpleNamespace(asDict=lambda row=row: row) for row in observed.rows
            ]
        )

    def compare(expected, actual, *, primary_key, what):
        observed.calls.append(("compare", tuple(primary_key or ()), what))
        return frame()

    def assumption(active, *, sql, what):
        read(active, sql=sql, what=what)
        return frame()

    def sql_executor(session, target, *, workspace=None):
        observed.calls.append(("warehouse", target.warehouse.name, workspace))
        assert workspace.workspace == "Requested"
        assert target.warehouse.name == "Reporting_WH"
        return SimpleNamespace(
            query_result_sets=lambda sql: (
                (),
                ({"missing_count": 0, "unexpected_count": 0},),
            )
        )

    def resolve(session, item, *, item_type, workspace=None):
        observed.calls.append(("model", item, workspace))
        assert workspace.workspace == "Requested"
        assert item == "Model_Dev"
        return Item(
            "33333333-3333-3333-3333-333333333333",
            item,
            item_type,
            "11111111-1111-1111-1111-111111111111",
        )

    def semantic_model(session, item, *, workspace=None):
        assert workspace.workspace == "Requested"
        return SimpleNamespace(
            query_dax=lambda sql: (
                []
                if "FILTER" in sql
                else [{"[Month]": 1, "[Revenue]": Decimal("10.00")}]
            )
        )

    def expected_rows(session, statement, *, target, workspace=None, **kwargs):
        observed.calls.append(("expected", target.warehouse.name, workspace))
        assert workspace.workspace == "Requested"
        assert target.warehouse.name == "Serving_WH"
        return [{"Month": 1, "Revenue": Decimal("10.00")}]

    def no_record(*args, **kwargs):
        raise AssertionError("source mode must not read or record catalogue state")

    monkeypatch.setattr("weaver.fabric.store.FabricStore", lambda: store)
    monkeypatch.setattr("weaver.sessions.NotebookSession", native)
    monkeypatch.setattr("weaver.runtime.spark_sql_validation.read_spark_sql_test", read)
    monkeypatch.setattr(
        "weaver.runtime.spark_sql_validation.read_spark_sql_assumption", assumption
    )
    monkeypatch.setattr("weaver.runtime.test_compare.compare", compare)
    for cls in (ConsoleSession, NotebookSession):
        monkeypatch.setattr(cls, "sql_executor", sql_executor)
        monkeypatch.setattr(cls, "resolve_item", resolve)
        monkeypatch.setattr(cls, "semantic_model", semantic_model)
        monkeypatch.setattr(cls, "query_tsql", expected_rows)
        monkeypatch.setattr(cls, "flusher", no_record)
    monkeypatch.setattr("weaver.catalogue.writer.writer_for", no_record)

    class Client(ConsoleSession):
        def execute_python(self, program, *, workspace=None, timeout=None):
            observed.programs.append((program, workspace))
            assert workspace.workspace == "Requested"
            assert str(workspace.environment) == "Runtime/Selected"
            emitted = []
            if observed.remove is not None:
                observed.remove.unlink()
                observed.remove = None
            exec(program.source, {"spark": spark, "emit": emitted.append})
            return emitted[0]

    observed.remove = None
    observed.store = store
    observed.resolver = resolver
    observed.spark = spark
    observed.client = lambda default=None: Client(
        workspace=None
        if default is None
        else Workspace(workspace=default, environment="Wrong"),
        store=store,
        resolver=resolver,
        progress=False,
        credential=SimpleNamespace(get_token=lambda *args, **kwargs: None),
    )
    return observed


@weaver_test()
@pytest.mark.parametrize("kind", ["Test", "Assumption"])
@pytest.mark.parametrize(
    "case,status", [("pass", PASSED), ("fail", FAILED), ("error", INVALID)]
)
@pytest.mark.parametrize(
    "selected", ["external", "declared", "directory", "glob", "project"]
)
@pytest.mark.parametrize("default", [None, "Other"])
def test_source_definitions_run_through_staged_program(
    tmp_path, selection, transport, kind, case, status, selected, default
):
    text = (
        SOURCE
        if kind == "Test"
        else "/*\nAssumption ID: Sales.Match\nDescription: Invalid values.\nDependencies: []\n*/\nselect 1 as ID from Sales.Input;\n"
    )
    path = selection["files"]
    if kind == "Assumption":
        path.unlink()
        path = path.parent.parent / "assumptions" / path.name
        path.parent.mkdir()
    path.write_bytes(text.encode())
    files = {
        "external": tmp_path / "Sales.Match.sql",
        "declared": path,
        "directory": path.parent,
        "glob": str(path.parent / "*.sql"),
        "project": None,
    }[selected]
    if selected == "external":
        files.write_bytes(text.encode())
        selection["items"] = "Lakehouse/Sales"
        transport.remove = files
    selection["files"] = files
    selection["names"] = "Sales.Match"
    if case == "fail":
        transport.rows = (
            {
                "ID": 1,
                "Value": Decimal("1.123456789012345678"),
                "_weaver_side": "expected",
            },
        )
    if case == "error":
        transport.error = RuntimeError("SQL execution failed")
    with transport.client(default) as session:
        with session.workflow("outer-workflow"):
            report = weaver.test(session=session, **selection)
        assert report.workflow_id is None
        assert report.status == status, report.nodes
        (node,) = report.nodes
        assert node.kind == kind
        assert node.physical_target == "Lakehouse/Sales_LH"
        assert (
            node.dispatch_location == str(files)
            if selected == "external"
            else node.dispatch_location.endswith("Sales.Match.sql")
        )
        assert node.executed
        assert node.result.succeeded == (case == "pass")
        if case == "error":
            assert "SQL execution failed" in node.result.error_message
            assert node.status == INVALID
        elif case == "fail":
            assert node.diagnostics[0]["Value"] == "1.123456789012345678"
            assert (
                node.result.violation_count == 1
                if kind == "Assumption"
                else node.result.missing_count == 1
            )
        else:
            assert node.diagnostics == ()
        assert all(
            scope.workspace.workspace == "Requested"
            for scope in session._scopes.values()
        )
    assert len(transport.programs) == 1
    assert not transport.programs[0][0].resubmit
    assert transport.store.files == {}
    assert all(session.closed for session in transport.opened)
    sql = next(call[1] for call in transport.calls if call[0] == "spark_sql")
    assert "`Requested`.`Sales_LH`.`Sales`.`Input`" in sql
    if kind == "Test" and case != "error":
        assert ("compare", ("ID",), "Sales.Match") in transport.calls


@weaver_test()
def test_source_dry_run_does_not_dispatch_or_acquire_spark(selection, transport):
    with transport.client("Other") as session:
        report = weaver.test(session=session, dry_run=True, **selection)
    assert report.status == PLANNED
    assert not report.nodes[0].executed
    assert not transport.calls and not transport.programs
    assert transport.store.files == {}


@weaver_test()
def test_missing_operation_environment_refuses_before_dispatch(selection, transport):
    from weaver.errors import CommandError

    selection["workspace_config"].write_text(
        "workspace: Requested\ntargets:\n  Lakehouse/Sales: Sales_LH\n",
        encoding="utf-8",
    )
    with transport.client("Other") as session:
        with pytest.raises(CommandError, match="requires a Fabric Environment"):
            weaver.test(session=session, **selection)
    assert not transport.calls and not transport.programs


@weaver_test()
@pytest.mark.parametrize("selected", ["mixed", "warehouse", "dax"])
@pytest.mark.parametrize("default", [None, "Other"])
def test_mixed_selection_preserves_bindings_without_recording(
    tmp_path, transport, selected, default
):
    from test_semantic_validation_build_cycle import with_validations

    root = with_validations(tmp_path)
    for item, name in (
        ("Lakehouse/Sales", "Sales.Match"),
        ("Warehouse/Reporting", "Sales.Warehouse"),
    ):
        path = root / item / "tests" / (name + ".sql")
        path.parent.mkdir(parents=True)
        path.write_bytes(SOURCE.replace("Sales.Match", name).encode())
    config = tmp_path / "workspace.yml"
    config.write_text(
        "workspace: Requested\n"
        + ("environment: Runtime/Selected\n" if selected == "mixed" else "")
        + "targets:\n  Lakehouse/Sales: Sales_LH\n  Warehouse/Reporting: Reporting_WH\n  SemanticModel/Reporting: Model_Dev\n  Warehouse/Serving: Serving_WH\n",
        encoding="utf-8",
    )
    items = {
        "mixed": None,
        "warehouse": "Warehouse/Reporting",
        "dax": "SemanticModel/Reporting",
    }[selected]
    with transport.client(default) as session:
        report = weaver.test(
            items, source=root, workspace_config=config, session=session
        )
        if selected != "mixed":
            assert all(scope.livy.attempts == 0 for scope in session._scopes.values())
    assert report.status == PASSED, report.nodes
    assert report.workflow_id is None
    assert len(report.nodes) == (
        4 if selected == "mixed" else 2 if selected == "dax" else 1
    )
    assert len(transport.programs) == (1 if selected == "mixed" else 0)
    assert all(
        scope.workspace.workspace == "Requested" for scope in session._scopes.values()
    )
    assert bool([call for call in transport.calls if call[0] == "warehouse"]) == (
        selected != "dax"
    )
    assert bool([call for call in transport.calls if call[0] == "model"]) == (
        selected != "warehouse"
    )
    assert bool([call for call in transport.calls if call[0] == "expected"]) == (
        selected != "warehouse"
    )
    assert bool([call for call in transport.calls if call[0] == "spark_sql"]) == (
        selected == "mixed"
    )


@weaver_test()
@pytest.mark.parametrize("selected", ["lakehouse", "mixed", "warehouse", "dax"])
@pytest.mark.parametrize("default", [None, "Other"])
def test_reused_source_scope_carries_current_operation_bindings(
    tmp_path, transport, selected, default
):
    from test_semantic_validation_build_cycle import with_validations

    from weaver.config import load_workspace

    root = with_validations(tmp_path)
    for item, name in (
        ("Lakehouse/Sales", "Sales.Match"),
        ("Warehouse/Reporting", "Sales.Warehouse"),
    ):
        path = root / item / "tests" / (name + ".sql")
        path.parent.mkdir(parents=True)
        path.write_bytes(SOURCE.replace("Sales.Match", name).encode("utf-8"))
    config = tmp_path / "workspace.yml"
    current_text = (
        "workspace: Requested\nenvironment: Runtime/Selected\n"
        "targets:\n  Lakehouse/Sales: Sales_New\n"
        "  Warehouse/Reporting: Reporting_WH\n"
        "  SemanticModel/Reporting: Model_Dev\n"
        "  Warehouse/Serving: Serving_WH\n"
    )
    config.write_text(
        current_text.replace("Sales_New", "Sales_Old")
        .replace("Reporting_WH", "Reporting_Old")
        .replace("Model_Dev", "Model_Old")
        .replace("Serving_WH", "Serving_Old"),
        encoding="utf-8",
    )
    prior = load_workspace(config)
    items = {
        "lakehouse": "Lakehouse/Sales",
        "mixed": None,
        "warehouse": "Warehouse/Reporting",
        "dax": "SemanticModel/Reporting",
    }[selected]
    with transport.client(default) as session:
        original_default = session.workspace
        planned = weaver.test(
            items, source=root, workspace_config=config, session=session, dry_run=True
        )
        assert planned.status == PLANNED
        assert planned.workflow_id is None
        assert planned.nodes and all(not node.executed for node in planned.nodes)
        scope = session.scope(prior)
        cached_workspace = scope.workspace
        resources = (scope.resolver, scope.store, scope.livy)
        assert scope.workspace == prior
        assert len(session._scopes) == 1
        assert scope.livy.attempts == 0
        assert not transport.calls and not transport.programs
        assert transport.store.files == {}

        config.write_text(current_text, encoding="utf-8")
        current = load_workspace(config)
        assert current != prior
        assert session.scope(current) is scope
        report = weaver.test(
            items, source=root, workspace_config=config, session=session
        )
        assert session.scope(current) is scope
        assert len(session._scopes) == 1
        assert scope.workspace is cached_workspace
        assert all(
            retained is original
            for retained, original in zip(
                (scope.resolver, scope.store, scope.livy), resources, strict=True
            )
        )
        assert session.workspace is original_default
        expected = {
            "Lakehouse/Sales/Sales.Match": "Lakehouse/Sales_New",
            "Warehouse/Reporting/Sales.Warehouse": "Warehouse/Reporting_WH",
            "SemanticModel/Reporting/Sales.RevenueReconciles": "SemanticModel/Model_Dev",
            "SemanticModel/Reporting/Sales.RevenueIsPositive": "SemanticModel/Model_Dev",
        }
        expected = {
            logical: physical
            for logical, physical in expected.items()
            if items is None or logical.startswith(items + "/")
        }
        assert {
            node.logical_id: node.physical_target for node in report.nodes
        } == expected
        assert report.status == PASSED, report.nodes
        assert report.workflow_id is None
        assert all(node.executed and node.result.succeeded for node in report.nodes)
        needs_spark = selected in ("lakehouse", "mixed")
        assert len(transport.programs) == int(needs_spark)
        if needs_spark:
            ((program, dispatched_workspace),) = transport.programs
            assert dispatched_workspace == current
            assert not program.resubmit
            (remote,) = transport.opened
            assert remote.workspace == current
            assert remote.workspace.workspace == "Requested"
            assert str(remote.workspace.environment) == "Runtime/Selected"
            sql = next(call[1] for call in transport.calls if call[0] == "spark_sql")
            assert "`Requested`.`Sales_New`.`Sales`.`Input`" in sql
            assert "Sales_Old" not in sql
        else:
            assert scope.livy.attempts == 0
            assert not transport.opened
            assert not [call for call in transport.calls if call[0] == "spark_sql"]
        for kind, target in (
            ("warehouse", "Reporting_WH"),
            ("model", "Model_Dev"),
            ("expected", "Serving_WH"),
        ):
            calls = [call for call in transport.calls if call[0] == kind]
            assert bool(calls) == (
                selected in ("mixed", "warehouse")
                if kind == "warehouse"
                else selected in ("mixed", "dax")
            )
            assert all(call[1] == target and call[2] == current for call in calls)
    assert transport.store.files == {}
    assert all(session.closed for session in transport.opened)


@weaver_test()
def test_unsupported_python_is_invalid_after_transport(tmp_path, selection, transport):
    source = selection["files"]
    source.unlink()
    path = source.with_name("Sales__Match.py")
    path.write_text(
        '"""Test ID: Sales.Match\nDescription: Python validation.\n"""\nfrom weaver import Test\nclass Sales__Match(Test):\n    def expected(self):\n        raise RuntimeError("must not run")\n    def actual(self):\n        raise RuntimeError("must not run")\n',
        encoding="utf-8",
    )
    selection["files"] = path
    with transport.client("Other") as session:
        report = weaver.test(session=session, **selection)
    assert report.status == INVALID
    assert "Python" in report.nodes[0].result.error_message
    assert not report.nodes[0].result.succeeded
    assert not transport.calls


@weaver_test()
def test_source_codec_preserves_metadata_and_body_without_reading_paths(selection):
    from weaver.declaration import read_source_document
    from weaver.declaration.model import WeaverItemId
    from weaver.test_file import SourceValidation

    item = WeaverItemId.parse("Lakehouse/Sales")
    document = read_source_document(
        "Lakehouse/Sales/tests/Sales.Match.sql", SOURCE.encode(), "Lakehouse"
    )
    source = SourceValidation(item, document, "C:/desktop-only/Sales.Match.sql")
    carried = json.loads(json.dumps(source.to_mapping()))
    decoded = SourceValidation.from_mapping(carried)
    assert decoded.item == item
    assert decoded.logical == source.logical
    assert decoded.path == source.path
    assert decoded.source.document == document.document
    assert decoded.source.sql_body == document.sql_body
    carried["source"]["metadata"] = (
        "Table ID: Sales.Match\nDescription: Wrong kind\nLineage: Test data\nDependencies: []\nSchema:\n  ID: int\n"
    )
    with pytest.raises(
        weaver.errors.CommandError, match="must declare a Test or Assumption"
    ):
        SourceValidation.from_mapping(carried)


@weaver_test()
def test_fabric_definitions_and_observer_use_real_declaration_and_report_codecs():
    from importlib.util import module_from_spec, spec_from_file_location
    from pathlib import Path

    from weaver.declaration import read_source_document
    from weaver.runtime.validation_result import AssumptionResult, TestResult
    from weaver.test_report import ValidationNodeReport

    path = Path(__file__).parent / "fabric/test_lakehouse_file_dispatch_boundary.py"
    spec = spec_from_file_location("stage0b_fabric_definitions", path)
    assert spec is not None and spec.loader is not None
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    import ast

    ast.parse(
        module.native_program(Workspace(workspace="Requested", environment="Selected"))
    )
    for kind in ("Test", "Assumption"):
        for case in module.CASES if kind == "Test" else module.CASES[:3]:
            text = module.source(kind, case)
            document = read_source_document(
                f"Lakehouse/Source/{'tests' if kind == 'Test' else 'assumptions'}/Sales.Scope.sql",
                text.encode(),
                "Lakehouse",
            )
            assert document.kind == kind
            assert document.sql_body
            status = PASSED if case == "pass" else FAILED if case == "fail" else INVALID
            error = "execution error" if status == INVALID else None
            result = (
                TestResult(
                    missing_count=int(case == "fail"),
                    unexpected_count=int(case == "fail"),
                    error_message=error,
                )
                if kind == "Test"
                else AssumptionResult(
                    violation_count=int(case == "fail"), error_message=error
                )
            )
            report = ValidationRunReport(
                status=status,
                nodes=(
                    ValidationNodeReport(
                        logical_id="Lakehouse/Source/Sales.Scope",
                        kind=kind,
                        physical_target="Lakehouse/Sales_LH",
                        primitive_kind="python_validation",
                        dispatch_location="Sales.Scope.sql",
                        status=status,
                        executed=True,
                        result=result,
                    ),
                ),
            )
            module.assert_result(report.to_mapping(), kind, case, "Sales_LH")


@weaver_test()
@pytest.mark.parametrize(
    "case,message",
    [
        ("shape", "expected has 2 column"),
        ("reserved", "reserved for diagnostics"),
        ("key", "repeats on the actual side"),
    ],
)
def test_incomparable_source_results_stay_invalid_across_transport(
    monkeypatch, selection, transport, case, message
):
    from importlib.util import module_from_spec, spec_from_file_location
    from pathlib import Path

    from weaver.runtime import spark_sql_validation, test_compare

    path = Path(__file__).parent / "fabric/test_lakehouse_file_dispatch_boundary.py"
    spec = spec_from_file_location("stage0b_guard_definitions", path)
    assert spec is not None and spec.loader is not None
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    text = module.source("Test", case).replace("Sales.Scope", "Sales.Match")
    selection["files"].write_bytes(text.encode())
    columns = (
        ["ID"]
        if case == "shape"
        else ["ID", "_weaver_side"]
        if case == "reserved"
        else ["ID", "Value"]
    )

    class Frame:
        def __init__(self, columns, repeated=False):
            self.columns = columns
            self.repeated = repeated

        def where(self, predicate):
            return SimpleNamespace(
                take=lambda n: (
                    [(1, 2)] if predicate == "count > 1" and self.repeated else []
                )
            )

        def groupBy(self, *columns):
            return SimpleNamespace(count=lambda: self)

    frames = iter([Frame(["ID", "Value"]), Frame(columns, repeated=case == "key")])
    queries = []

    def sql(statement):
        queries.append(statement)
        return next(frames)

    # Only Spark I/O is substituted; the SQL runtime and comparison guards run.
    real_read = transport.read_spark_sql_test
    monkeypatch.setattr(
        spark_sql_validation,
        "read_spark_sql_test",
        lambda active, **kwargs: real_read(SimpleNamespace(sql=sql), **kwargs),
    )
    monkeypatch.setattr(test_compare, "compare", transport.compare)
    with transport.client() as session:
        report = weaver.test(session=session, **selection)
    assert len(queries) == 2
    assert report.status == INVALID
    assert (
        report.nodes[0].result.error_message
        and message in report.nodes[0].result.error_message
    )
    assert not report.nodes[0].result.succeeded
    assert len(transport.programs) == 1
