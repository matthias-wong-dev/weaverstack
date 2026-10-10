"""Where an explicit stability waiver goes, and what a refusal carries back.

The waiver is one request-level policy that has to survive every layer between
a command line and the two engines that enforce it. A layer that dropped it
would load without the waiver and report success, so each hand-off is asserted
rather than assumed.

The refusal half is the other direction: a gate refuses before writing, and its
counts have to reach the operator through TDS, which moves no exception.
"""

from __future__ import annotations

import pathlib
from types import SimpleNamespace

import pytest
from support.weaver_test import weaver_test

from weaver.declaration.metadata import ObjectId
from weaver.declaration.tsql_load import (
    PROCEDURE_RESULT_PARAMETERS,
    RESULT_PARAMETERS,
)
from weaver.errors import CommandError, LoadError
from weaver.run.dispatch import dispatch_primitive
from weaver.run.outcome import settle, status_of
from weaver.run.resolution import WAREHOUSE_PROCEDURE
from weaver.run.result import FAILED, SUCCEEDED, SUCCEEDED_WITH_REJECTS
from weaver.run.runner import RunRequest
from weaver.runtime.load_result import LoadResult
from weaver.targets import PhysicalTargetRef

REPORTING = PhysicalTargetRef("warehouse", "Reporting_WH")


class _Sql:
    """A Warehouse connection that records the inputs it was called with."""

    def __init__(self, row) -> None:
        self.row = row
        self.calls: list = []

    def call_procedure(self, procedure, *, inputs=(), outputs=()):
        self.calls.append(dict(inputs))
        return self.row


def _physical(result: LoadResult) -> dict:
    row = result.as_row()
    return {
        physical: row[logical]
        for (logical, _lt), (physical, _pt) in zip(
            RESULT_PARAMETERS, PROCEDURE_RESULT_PARAMETERS, strict=True
        )
    }


def _warehouse_node():
    return SimpleNamespace(
        node_id="Sales.Customer",
        primitive_kind=WAREHOUSE_PROCEDURE,
        physical_target=REPORTING,
        logical_id=SimpleNamespace(
            object_id=ObjectId("Sales", "Customer"),
            item="Warehouse/Reporting",
        ),
    )


def _warehouse(result: LoadResult, **policy):
    sql = _Sql(_physical(result))
    node = _warehouse_node()
    session = SimpleNamespace(sql_executor=lambda target, workspace=None: sql)
    returned = dispatch_primitive(node, session=session, **policy)
    return returned, sql


class _Scope:
    """A run scope that records the policy a Python node was dispatched with."""

    def __init__(self, row) -> None:
        self.row = row
        self.calls: list = []

    def get(self):
        return self

    def dispatch_python(self, node, *, expected_class, **policy):
        self.calls.append(policy)
        return self.row


def _node(kind: str = "python_table", object: str = "Customer"):
    return SimpleNamespace(
        node_id=f"Sales.{object}",
        primitive_kind=kind,
        physical_target=PhysicalTargetRef("lakehouse", "Sales_LH"),
        logical_id=SimpleNamespace(object_id=ObjectId("Sales", object)),
    )


def _dispatch(node, scope, **policy):
    return dispatch_primitive(
        node,
        session=SimpleNamespace(),
        resolved=SimpleNamespace(
            expected_class=f"Sales__{node.logical_id.object_id.object}"
        ),
        open_runtime=scope,
        **policy,
    )


def _python(row, kind: str = "python_table", **policy):
    scope = _Scope(row)
    returned = _dispatch(_node(kind), scope, **policy)
    return returned, scope


# --- the request carries it ----------------------------------------------------


@weaver_test()
def test_a_load_request_waives_nothing_unless_asked():
    request = RunRequest.load(("Lakehouse/Sales",))

    assert request.ignore_stability_threshold is False
    assert request.to_mapping()["ignore_stability_threshold"] is False


@weaver_test()
def test_the_waiver_is_part_of_what_a_request_says_it_will_do():
    request = RunRequest.load(("Lakehouse/Sales",), ignore_stability_threshold=True)

    assert request.to_mapping()["ignore_stability_threshold"] is True


@weaver_test()
def test_a_validation_run_cannot_waive_a_load_gate():
    """Tests and Assumptions have no stability contract to waive."""

    with pytest.raises(CommandError, match="applies only to loads"):
        RunRequest.test(("Lakehouse/Sales",), ignore_stability_threshold=True)


# --- and every layer below passes it on ----------------------------------------


@weaver_test()
def test_an_ordinary_warehouse_load_names_no_waiver():
    """Named only when set, so a procedure installed before it still runs."""

    _result, sql = _warehouse(LoadResult(succeeded=True))

    assert "ignore_stability_threshold" not in sql.calls[0]


@weaver_test()
def test_a_waived_warehouse_load_passes_the_procedure_parameter():
    _result, sql = _warehouse(
        LoadResult(succeeded=True), ignore_stability_threshold=True
    )

    assert sql.calls[0]["ignore_stability_threshold"] == 1


@weaver_test()
def test_an_ordinary_python_load_names_no_waiver():
    _result, scope = _python(LoadResult(succeeded=True).as_row())

    assert scope.calls[0]["ignore_stability_threshold"] is False


@weaver_test()
def test_a_waived_python_load_reaches_the_table_primitive():
    _result, scope = _python(
        LoadResult(succeeded=True).as_row(), ignore_stability_threshold=True
    )

    assert scope.calls[0]["ignore_stability_threshold"] is True


# --- and a Folder is not given table policy ------------------------------------
#
# A Folder has no stability contract, and its ``_load`` takes no waiver. A
# selection naming a Folder and a Table is ordinary, so dropping the argument
# where the primitive kind is known is what lets one run carry both.

#: A deployed primitive's whole contract, and the two signatures that differ.
DEPLOYED = {
    "python_folder": """\
class Sales__Raw:
    def __init__(self, spark, lakehouse=None):
        self.called = None

    def _load(self, fault_tolerant=False):
        from weaver.runtime.load_result import LoadResult

        Sales__Raw.called = dict(fault_tolerant=fault_tolerant)
        return LoadResult(succeeded=True)
""",
    "python_table": """\
class Sales__Customer:
    def __init__(self, spark, lakehouse=None):
        self.called = None

    def _load(self, fault_tolerant=False, reload=False,
              ignore_stability_threshold=False):
        from weaver.runtime.load_result import LoadResult

        Sales__Customer.called = dict(
            fault_tolerant=fault_tolerant,
            reload=reload,
            ignore_stability_threshold=ignore_stability_threshold,
        )
        return LoadResult(succeeded=True)
""",
}


def _deployed(tmp_path, kind: str, object: str):
    """A real local dispatch whose deployed module is written here.

    Everything from ``dispatch_primitive`` down is production code, including
    the module import and the ``_load`` call, so a keyword the class cannot take
    is the ``TypeError`` it is in Fabric. The Fabric mount is the one boundary
    stood in for, as ``support.workspaces.mounted_lakehouse`` stands in for it.
    """

    from support.workspaces import given_resolver

    from weaver.etl import LOAD_ROOT
    from weaver.lakehouse import _MOUNTS, lakehouse_for
    from weaver.run.runtime_boundary import DirectRunScope
    from weaver.runtime.python_context import RuntimeScope
    from weaver.targets import ItemRef
    from weaver.workspaces import Workspace

    workspace = Workspace(workspace="Demo", catalogue="Warehouse/Weaver")
    resolver = given_resolver(workspace=workspace, lakehouses=("Sales_LH",))
    _MOUNTS[lakehouse_for(resolver, ItemRef("Sales_LH")).spark_root] = str(tmp_path)

    session = SimpleNamespace(
        resolver=lambda ws=None: resolver, spark=lambda ws=None: None
    )
    node = _node(kind, object=object)
    node.logical_id.item = "Lakehouse/Sales"
    node.primitive_object = SimpleNamespace(schema="Sales", object=object)

    root = pathlib.Path(tmp_path) / "Files" / LOAD_ROOT / "Sales"
    root.mkdir(parents=True, exist_ok=True)
    (root / f"{object}.py").write_text(DEPLOYED[kind], encoding="utf-8")

    scope = DirectRunScope(RuntimeScope.new(), session, workspace)
    return node, SimpleNamespace(get=lambda: scope)


@weaver_test()
def test_a_folder_load_runs_with_the_waiver_asked_for(tmp_path):
    """The mixed selection this was failing on, from the Folder's side."""

    node, runtime = _deployed(tmp_path, "python_folder", "Raw")

    result = _dispatch(node, runtime, ignore_stability_threshold=True, reload=False)

    assert result.succeeded


@weaver_test()
def test_a_table_load_in_the_same_run_still_gets_the_waiver(tmp_path):
    """One run, one waiver, two primitive kinds, and only one of them takes it."""

    node, runtime = _deployed(tmp_path, "python_table", "Customer")

    result = _dispatch(node, runtime, ignore_stability_threshold=True)

    assert result.succeeded
    assert runtime.get().runtime_scope  # the module was imported, not stubbed


@weaver_test()
def test_one_waived_run_carries_a_folder_and_a_table_together(tmp_path):
    """The selection the defect was found on: both kinds, one policy."""

    # One Lakehouse, one deployed tree, two objects, as a run reaches them.
    folder, folder_runtime = _deployed(tmp_path, "python_folder", "Raw")
    table, table_runtime = _deployed(tmp_path, "python_table", "Customer")

    assert _dispatch(folder, folder_runtime, ignore_stability_threshold=True).succeeded
    assert _dispatch(table, table_runtime, ignore_stability_threshold=True).succeeded


@weaver_test()
def test_a_folder_reload_is_dropped_the_same_way(tmp_path):
    """``reload`` is the other table-only policy, and a Folder cannot take it."""

    node, runtime = _deployed(tmp_path, "python_folder", "Raw")

    assert _dispatch(node, runtime, reload=True).succeeded


@weaver_test()
def test_an_unwaived_folder_load_is_unchanged(tmp_path):
    node, runtime = _deployed(tmp_path, "python_folder", "Raw")

    assert _dispatch(node, runtime).succeeded


@weaver_test()
def test_the_self_recording_entry_point_asks_for_the_refusal_too():
    """``_.Load`` records what a refusal counted, so it cannot let it throw.

    The counts are gone by the time an outer CATCH runs, so a manual
    ``exec [_].[Load]`` that let the procedure throw recorded zeroes for
    everything the gate had already settled.
    """

    from weaver.fragments import standard_fragment

    entry = standard_fragment("Warehouse")["programmables/_.Load.sql"].decode("utf-8")

    assert "@return_refusal = @return_refusal" in entry
    assert "@return_refusal = 1," in entry
    # Recorded first, raised after, so the evidence outlives the failure.
    assert entry.index("[_].[LoadStatistic]") < entry.index("throw 51032")


@weaver_test()
def test_the_waiver_crosses_to_fabric_with_the_request():
    """A load with Spark work runs in Fabric, planned from the request it is sent."""

    waived = RunRequest.load(["Lakehouse/Sales"], ignore_stability_threshold=True)
    ordinary = RunRequest.load(["Lakehouse/Sales"])

    assert RunRequest.from_mapping(waived.to_mapping()).ignore_stability_threshold
    assert not RunRequest.from_mapping(ordinary.to_mapping()).ignore_stability_threshold


@weaver_test()
def test_the_report_says_whether_the_run_waived_anything():
    from weaver.load_report import LoadRunReport

    report = LoadRunReport(
        requested=("Lakehouse/Sales",),
        status="succeeded",
        dry_run=False,
        fault_tolerant=False,
        ignore_stability_threshold=True,
    )

    assert report.to_mapping()["ignore_stability_threshold"] is True
    assert LoadRunReport.from_mapping(report.to_mapping()).ignore_stability_threshold


@weaver_test()
def test_the_command_line_offers_one_spelling_and_defaults_to_off():
    from weaver_cli.main import build_parser

    off = build_parser().parse_args(["load", "Lakehouse/Sales"])
    on = build_parser().parse_args(
        ["load", "Lakehouse/Sales", "--ignore-stability-threshold"]
    )

    assert off.ignore_stability_threshold is False
    assert on.ignore_stability_threshold is True


# --- a refusal is a failure, whatever it rejected ------------------------------


@weaver_test()
def test_a_refusal_with_rejected_rows_is_still_a_failure():
    """The classification the reject count alone gets wrong.

    A gate that refuses often has rejected rows to report, and nothing was
    written, so reading the count first would call it a success with rejects.
    """

    refusal = LoadResult.refusal("refused", rows_read=10, rows_rejected=4)

    assert status_of(refusal) == FAILED


@weaver_test()
def test_a_refusal_with_no_rejected_rows_is_a_failure_too():
    assert status_of(LoadResult.refusal("refused", rows_read=10)) == FAILED


@weaver_test()
def test_tolerated_rejects_keep_their_classification():
    tolerated = LoadResult(
        succeeded=False, rows_read=10, rows_inserted=6, rows_rejected=4
    )

    assert status_of(tolerated) == SUCCEEDED_WITH_REJECTS


@weaver_test()
def test_an_ordinary_success_is_unaffected():
    assert status_of(LoadResult(succeeded=True, rows_read=10)) == SUCCEEDED


@weaver_test()
def test_a_refusal_is_reported_once_as_an_error():
    node = SimpleNamespace(node_id="Sales.Customer", primitive_kind="warehouse")
    outcome = settle(
        node, returned=LoadResult.refusal("too much", rows_read=10, rows_rejected=4)
    )

    (message,) = outcome.messages
    assert message.severity == "error"
    assert "too much" in message.message


@weaver_test()
def test_a_raised_refusal_reads_as_the_returned_one_does():
    node = SimpleNamespace(node_id="Sales.Customer", primitive_kind="python_table")
    result = LoadResult.refusal("too much", rows_read=10)
    outcome = settle(node, raised=LoadError("Sales.Customer: too much", result=result))

    (message,) = outcome.messages
    assert outcome.status == FAILED
    assert message.message == "Sales.Customer refused the load: too much"
    assert outcome.result.rows_read == 10


# --- and it survives both boundaries -------------------------------------------


@weaver_test()
def test_the_discriminator_survives_serialisation():
    refusal = LoadResult.refusal("refused", rows_read=10, rows_rejected=4)

    assert LoadResult.from_row(refusal.as_row()) == refusal


@weaver_test()
def test_a_row_written_before_the_discriminator_existed_is_not_a_refusal():
    row = LoadResult(succeeded=True, rows_read=3).as_row()
    row.pop("is_refusal")

    assert LoadResult.from_row(row).is_refusal is False


@weaver_test()
def test_a_warehouse_refusal_arrives_with_its_counts():
    """The procedure returns it rather than throwing.

    A Fabric Warehouse discards output values when a procedure ends in an
    uncaught THROW, so a thrown refusal would arrive with no counts at all.
    """

    refusal = LoadResult.refusal("over the threshold", rows_read=10, rows_rejected=4)

    returned, sql = _warehouse(refusal)

    assert sql.calls[0]["return_refusal"] == 1
    assert returned.is_refusal
    assert returned.rows_read == 10
    assert returned.rows_rejected == 4
    assert returned.rows_deleted == 0


@weaver_test()
def test_a_warehouse_procedure_that_predates_the_contract_says_to_rebuild():
    """Argument binding fails before the body runs, so nothing loaded."""

    from weaver.sql.errors import SqlExecutionError

    class Old:
        def call_procedure(self, procedure, *, inputs=(), outputs=()):
            raise SqlExecutionError("SQL execution failed: reworded by the driver")

        def query(self, statement, parameters=None):
            assert parameters == ["[_].[Load Sales.Customer]"]
            return [{"name": "@fault_tolerant"}]

    node = _warehouse_node()
    session = SimpleNamespace(sql_executor=lambda target, workspace=None: Old())

    with pytest.raises(LoadError, match="Rebuild"):
        dispatch_primitive(node, session=session)


@weaver_test()
@pytest.mark.parametrize(
    "parameters",
    [[{"name": "@fault_tolerant"}, {"name": "@return_refusal"}], [], None],
    ids=["current", "missing", "unreadable"],
)
def test_a_failed_call_to_a_current_procedure_keeps_its_own_error(parameters):
    from weaver.sql.errors import SqlExecutionError

    class Current:
        def call_procedure(self, procedure, *, inputs=(), outputs=()):
            raise SqlExecutionError("has too many arguments specified")

        def query(self, statement, parameters_=None):
            if parameters is None:
                raise SqlExecutionError("sys.parameters is unavailable")
            return parameters

    node = _warehouse_node()
    session = SimpleNamespace(sql_executor=lambda target, workspace=None: Current())

    with pytest.raises(SqlExecutionError, match="too many arguments"):
        dispatch_primitive(node, session=session)


# --- and nothing was written before it refused ---------------------------------


def _generated_procedure() -> str:
    from support.generated_load import procedure

    from weaver.declaration.model import WAREHOUSE, WeaverItemId
    from weaver.declaration.source import read_source_document

    source = (
        b"/*\nTable ID: Sales.Customer\n\nDescription: Customers.\n\n"
        b"Lineage: The sales system.\n\nPrimary key: Customer id\n\n"
        b"Schema:\n  Customer id: varchar(50)\n  Customer name: varchar(200)\n*/\n"
        b"select 1 as [Customer id], 2 as [Customer name];\n"
    )
    installer = (
        read_source_document("Sales.Customer.sql", source, WAREHOUSE)
        .create_load(item=WeaverItemId("Warehouse", "Reporting"))
        .payload.decode()
    )
    return procedure(installer)


@weaver_test()
def test_every_refusal_returns_before_the_target_is_written():
    """The transaction claim, read off the order of the generated statements.

    A refusal now comes back as a successful call, which commits where a throw
    rolled back. That is only safe because each gate returns before any target
    mutation, so what commits is the evidence the load settled and nothing else.

    An explicit reload empties the target before the load body, under its own
    contract; the gates below are what these indexes measure. An empty target
    is written from staging in a branch of its own, which no stability gate
    guards, so the reject refusal precedes both branches and the breach refusal
    precedes the writes of the branch it guards.
    """

    payload = _generated_procedure()
    refusals = [
        index
        for index in range(len(payload))
        if payload.startswith("set @weaver_is_refusal = cast(1 as bit);", index)
    ]
    empty = payload.index(
        "insert into [Sales].[Customer] (", payload.index("if @weaver_target_rows = 0")
    )
    working = payload.index("create table [Sales].[Customer_Upsert]")
    existing = min(
        payload.index("update c\n", working),
        payload.index("insert into [Sales].[Customer] (", working),
    )

    assert len(refusals) == 2
    assert min(refusals) < empty < max(refusals) < existing


@weaver_test()
def test_a_refused_load_establishes_no_bookmark():
    """Every refusal assigns the outputs, and the bookmark is not among them."""

    payload = _generated_procedure()
    refusal = payload[: payload.index("throw 51020")]

    assert "set @weaver_bookmark_datetime = null;" in refusal


@weaver_test()
def test_a_refused_warehouse_load_calls_its_procedure_once():
    """One call. A second would run the load again to read its diagnostics."""

    refusal = LoadResult.refusal("over the threshold", rows_read=10, rows_rejected=4)

    _returned, sql = _warehouse(refusal)

    assert len(sql.calls) == 1
