"""The Spark SQL executors, running wherever the Installer runs.

Neither reaches a Spark session. Each resolves its payload against the batch's
destination and hands the statements to the Session's Spark SQL capability, with
the identifier-case scope the destination asks for. Whether that capability runs
the statements here or submits them is the Session's business, and is asserted
in ``test_session_spark_sql_boundary.py``.

What these prove is the executor's half: what is run, in what order, against
which destination, and under which case scope.
"""

from __future__ import annotations

import pytest

from weaver.build_bundle.executors.base import InstallationContext, ResolvedTarget
from weaver.build_bundle.executors.spark_sql import SparkSqlExecutor
from weaver.build_bundle.executors.spark_sql_batch import SparkSqlBatchExecutor
from weaver.build_bundle.models import InstallAction
from weaver.build_bundle.targets import BoundTarget
from weaver.errors import InstallError
from weaver.spark import FabricSparkTarget
from weaver.targets import ItemRef


class _Capability:
    """The Session's Spark SQL capability, recording what it was asked to run."""

    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        #: One entry per call: the statements it carried and its case scope.
        self.calls: list[tuple[list[str], bool]] = []

    def one(self, statement: str, *, exact_case: bool = False):
        return self.many([statement], exact_case=exact_case)

    def many(self, statements, *, exact_case: bool = False):
        self.calls.append((list(statements), exact_case))
        if self.fail:
            raise RuntimeError("view failed")
        return []

    @property
    def statements(self) -> list[str]:
        return [one for statements, _case in self.calls for one in statements]


def _context(capability, destination, *, epoch=None, item="Sales"):
    return InstallationContext(
        resolver=None,
        store=None,
        target=ResolvedTarget(
            bound=BoundTarget(id=f"lakehouse-{item}", kind="lakehouse", item_id=item),
            lakehouse=ItemRef(item),
            destination=destination,
        ),
        spark_sql=capability.one,
        spark_sql_batch=capability.many,
        epoch=epoch,
    )


# --- one statement -------------------------------------------------------------


def _view_action():
    return InstallAction(
        id="view-Sales.ActiveCustomer",
        kind="build_view",
        resource_node_id="delta:Sales.ActiveCustomer",
        executor="spark_sql",
        payload="payload/view.spark.sql",
        payload_sha256="x",
    )


def _run(capability, destination):
    return SparkSqlExecutor().execute(
        _view_action(),
        b"CREATE OR REPLACE VIEW {{object:Sales.ActiveCustomer}} AS "
        b"SELECT * FROM {{object:Sales.CustomerEnriched}}",
        _context(capability, destination),
    )


def test_a_view_is_resolved_against_the_batchs_destination():
    capability = _Capability()
    destination = FabricSparkTarget(workspace="Analytics", lakehouse="Sales")

    details = _run(capability, destination)

    statement, = capability.statements
    assert "`Analytics`.`Sales`.`Sales`.`CustomerEnriched`" in statement
    assert details["destination"] == "Sales"
    assert details["statement_first_line"] == statement.splitlines()[0]


@pytest.mark.parametrize(
    "destination",
    [
        FabricSparkTarget(workspace="Analytics", lakehouse="Sales"),
        FabricSparkTarget(workspace="Demo", lakehouse="Sales"),
    ],
    ids=["fabric", "local"],
)
def test_a_view_carries_the_destinations_case_scope(destination):
    """Weaver identities are exact, so the statement says which case it needs."""

    capability = _Capability()
    _run(capability, destination)

    assert capability.calls == [(capability.statements, True)]


def test_a_view_without_a_way_to_run_a_statement_says_so():
    capability = _Capability()
    context = _context(capability, FabricSparkTarget(workspace="A", lakehouse="Sales"))
    context = InstallationContext(
        resolver=None, store=None, target=context.target
    )

    with pytest.raises(InstallError, match="no Spark SQL capability"):
        SparkSqlExecutor().execute(_view_action(), b"SELECT 1", context)


def test_a_failing_statement_is_not_swallowed():
    capability = _Capability(fail=True)

    with pytest.raises(RuntimeError, match="view failed"):
        _run(capability, FabricSparkTarget(workspace="Analytics", lakehouse="Sales"))


# --- a batch of statements ------------------------------------------------------


def _batch_action():
    return InstallAction(
        id="publish-registry",
        kind="publish_registry",
        resource_node_id=None,
        executor="spark_sql_batch",
        payload="payload/registry.spark-sql-batch.json",
        payload_sha256="x",
    )


def _batch_context(capability, *, epoch=None):
    return _context(
        capability,
        FabricSparkTarget(workspace="Analytics", lakehouse="Control"),
        epoch=epoch,
        item="Control",
    )


def test_a_batch_is_one_piece_of_work_in_payload_order():
    """The statements of one action travel together, and stay in order.

    One call, not one per statement: where they cross that is one submission,
    which is the behaviour the batch payload exists to keep.
    """

    capability = _Capability()

    details = SparkSqlBatchExecutor().execute(
        _batch_action(),
        b'["DELETE FROM {{object:_.Registry}}", "MERGE INTO {{object:_.Registry}}"]',
        _batch_context(capability),
    )

    assert len(capability.calls) == 1
    statements, exact_case = capability.calls[0]
    assert [statement.split()[0] for statement in statements] == ["DELETE", "MERGE"]
    assert exact_case is True
    assert details["statement_count"] == 2
    assert details["destination"] == "Control"


def test_every_statement_in_a_batch_gets_the_same_epoch():
    """The reason the epoch is an installation value rather than a clock call.

    One build publishes Registry rows for several items in several statements.
    Were each to read the clock, an alias and the source it points at could be
    dated milliseconds apart and then order against each other on the next build
    — which is exactly the false staleness the epoch exists to prevent.
    """

    capability = _Capability()
    payload = (
        b'["INSERT INTO {{object:_.Registry}} VALUES (CAST(\'{{epoch}}\' AS TIMESTAMP))",'
        b' "INSERT INTO {{object:_.Registry}} VALUES (CAST(\'{{epoch}}\' AS TIMESTAMP))"]'
    )

    SparkSqlBatchExecutor().execute(
        _batch_action(), payload, _batch_context(capability, epoch="2026-07-31 09:00:00.000000")
    )

    dated = capability.statements
    assert len(dated) == 2
    assert all("2026-07-31 09:00:00.000000" in statement for statement in dated)
    assert dated[0] == dated[1]
    assert all("{{epoch}}" not in statement for statement in dated)


def test_a_statement_needing_an_epoch_without_one_says_so():
    """Rather than reaching ``expand``, which would report it as an unresolvable
    name and say nothing about the missing value."""

    capability = _Capability()

    with pytest.raises(InstallError, match="supplied none"):
        SparkSqlBatchExecutor().execute(
            _batch_action(),
            b'["INSERT INTO {{object:_.Registry}} VALUES (\'{{epoch}}\')"]',
            _batch_context(capability, epoch=None),
        )

    assert capability.calls == []


def test_a_batch_naming_no_epoch_runs_without_one():
    """Only Registry publication carries the token; every other batch is
    unaffected by its absence."""

    capability = _Capability()
    SparkSqlBatchExecutor().execute(
        _batch_action(), b'["DELETE FROM {{object:_.Registry}}"]', _batch_context(capability)
    )

    assert [statement.split()[0] for statement in capability.statements] == ["DELETE"]


def test_a_batch_without_a_way_to_run_statements_says_so():
    capability = _Capability()
    bare = _batch_context(capability)
    bare = InstallationContext(
        resolver=None, store=None, target=bare.target
    )

    with pytest.raises(InstallError, match="no Spark SQL capability"):
        SparkSqlBatchExecutor().execute(_batch_action(), b'["DELETE FROM x"]', bare)
