"""Where an explicit stability waiver goes, and what a refusal carries back.

The waiver is one request-level policy that has to survive every layer between
a command line and the two engines that enforce it. A layer that dropped it
would load without the waiver and report success, so each hand-off is asserted
rather than assumed.

The refusal half is the other direction: a gate refuses before writing, and its
counts have to reach the operator through TDS and through Livy, neither of which
moves an exception.
"""

from __future__ import annotations

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
from weaver.runtime.load_refusal import (
    REFUSAL_KEY,
    REFUSAL_VERSION,
    decoded_refusal,
    refusal_envelope,
    refused,
)
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


def _python(row, **policy):
    scope = _Scope(row)
    node = SimpleNamespace(
        node_id="Sales.Customer",
        primitive_kind="python_table",
        physical_target=PhysicalTargetRef("lakehouse", "Sales_LH"),
        logical_id=SimpleNamespace(object_id=ObjectId("Sales", "Customer")),
    )
    returned = dispatch_primitive(
        node,
        session=SimpleNamespace(),
        resolved=SimpleNamespace(expected_class="Sales__Customer"),
        open_runtime=scope,
        **policy,
    )
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


@weaver_test()
def test_only_a_table_load_is_given_the_keyword():
    """A Folder's ``_load`` does not take it, and a validation is not a load."""

    import inspect

    from weaver.objects import Folder, Table
    from weaver.run.dispatch import python_primitive

    assert "ignore_stability_threshold" in inspect.signature(Table._load).parameters
    assert (
        "ignore_stability_threshold" not in inspect.signature(Folder._load).parameters
    )
    assert (
        "ignore_stability_threshold" in inspect.signature(python_primitive).parameters
    )


@weaver_test()
def test_a_remote_dispatch_names_the_waiver_only_when_it_is_asked_for():
    """The argument crosses as data, so an unset waiver need not cross at all.

    A requested one always does: an older published Weaver must fail rather than
    load without it.
    """

    from weaver.run.runtime_boundary import FabricRunScope

    submitted = []

    class Session:
        def execute_python(self, program, workspace=None):
            submitted.append(program.source)
            return LoadResult(succeeded=True).as_row()

    scope = FabricRunScope(Session(), None, "run-1")
    node = SimpleNamespace(
        node_id="n",
        logical_id=SimpleNamespace(item="Lakehouse/Sales"),
        physical_target=PhysicalTargetRef("lakehouse", "Sales_LH"),
        primitive_object=SimpleNamespace(schema="Sales", object="Customer"),
    )

    scope.dispatch_python(node, expected_class="Sales__Customer", fault_tolerant=False)
    assert "ignore_stability_threshold" not in submitted[-1]

    scope.dispatch_python(
        node,
        expected_class="Sales__Customer",
        fault_tolerant=False,
        ignore_stability_threshold=True,
    )
    assert "'ignore_stability_threshold': True" in submitted[-1]


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
            raise SqlExecutionError(
                "SQL execution failed: Procedure or function [_].[Load Sales.Customer] "
                "has too many arguments specified."
            )

    node = _warehouse_node()
    session = SimpleNamespace(sql_executor=lambda target, workspace=None: Old())

    with pytest.raises(LoadError, match="Rebuild"):
        dispatch_primitive(node, session=session)


@weaver_test()
def test_a_python_refusal_crosses_livy_as_data():
    refusal = LoadResult.refusal("over the threshold", rows_read=10, rows_rejected=4)
    envelope = refusal_envelope(LoadError("Sales.Customer: over", result=refusal))

    assert refused(envelope)
    assert envelope[REFUSAL_KEY] == REFUSAL_VERSION

    rebuilt = decoded_refusal(envelope)
    assert isinstance(rebuilt, LoadError)
    assert rebuilt.result == refusal
    assert "over" in str(rebuilt)


@weaver_test()
def test_a_decoded_refusal_reaches_the_run_as_a_raised_load_error():
    refusal = LoadResult.refusal("over the threshold", rows_read=10)
    envelope = refusal_envelope(LoadError("Sales.Customer: over", result=refusal))

    with pytest.raises(LoadError) as raised:
        _python(envelope)

    assert raised.value.result == refusal


@weaver_test()
def test_an_unknown_failure_is_not_turned_into_a_refusal():
    """Only a load error carrying a settled result crosses as one."""

    assert refusal_envelope(RuntimeError("the interpreter died")) is None
    assert refusal_envelope(LoadError("no result carried")) is None
    assert not refused(LoadResult(succeeded=True).as_row())


@weaver_test()
def test_an_envelope_this_version_cannot_read_says_what_to_publish():
    with pytest.raises(LoadError, match="Publish this version"):
        decoded_refusal({REFUSAL_KEY: REFUSAL_VERSION + 1, "result": {}})


@weaver_test()
def test_the_remote_entry_point_returns_a_refusal_instead_of_raising(monkeypatch):
    """The one place the envelope is produced, on the far side of Livy."""

    import weaver.run.entry as entry

    refusal = LoadResult.refusal("over the threshold", rows_read=10)

    def refuse(**_kwargs):
        raise LoadError("Sales.Customer: over the threshold", result=refusal)

    monkeypatch.setattr("weaver.run.dispatch.python_primitive", refuse)
    monkeypatch.setattr(entry, "get_scope", lambda run_id: None)
    monkeypatch.setattr(
        "weaver.runtime.session_scopes.scope_catalogue", lambda run_id: None
    )

    returned = entry.run_python_primitive(
        run_id="r",
        node_id="n",
        item="Lakehouse/Sales",
        target="Sales_LH",
        schema="Sales",
        object="Customer",
        expected_class="Sales__Customer",
        session=object(),
    )

    assert refused(returned)
    assert decoded_refusal(returned).result == refusal


@weaver_test()
def test_the_remote_entry_point_lets_an_unknown_failure_through(monkeypatch):
    import weaver.run.entry as entry

    def die(**_kwargs):
        raise RuntimeError("the interpreter died")

    monkeypatch.setattr("weaver.run.dispatch.python_primitive", die)
    monkeypatch.setattr(entry, "get_scope", lambda run_id: None)
    monkeypatch.setattr(
        "weaver.runtime.session_scopes.scope_catalogue", lambda run_id: None
    )

    with pytest.raises(RuntimeError, match="interpreter died"):
        entry.run_python_primitive(
            run_id="r",
            node_id="n",
            item="Lakehouse/Sales",
            target="Sales_LH",
            schema="Sales",
            object="Customer",
            expected_class="Sales__Customer",
            session=object(),
        )


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
    contract; the gates below are what these indexes measure.
    """

    payload = _generated_procedure()
    refusals = [
        index
        for index in range(len(payload))
        if payload.startswith("set @weaver_is_refusal = cast(1 as bit);", index)
    ]
    written = min(
        payload.index("\n    update c\n"),
        payload.index("\n    insert into [Sales].[Customer] ("),
    )

    assert len(refusals) == 2
    assert max(refusals) < written


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
