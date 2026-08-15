"""One action executed against one target, with the installer's result semantics.

This is the layer that decides how much Fabric the suite has to buy. An executor
is where Weaver meets the engine, and almost all of what it does is checkable
without one: that the exact statement reaches the session, that a logical name is
resolved against the batch's destination before it does, that a missing
capability fails saying which, and that a failure becomes a *result* rather than
an exception.

What is left for a real workspace afterwards is genuinely narrow — does Fabric
accept this T-SQL, does the object appear in inventory — and answering it no
longer requires parsing a repository, reading a catalogue and installing a bundle
to reach the one statement in question.

`execute_install_action` and the installer share one execution path, so the semantics
asserted here are the semantics an installation gets.
"""

from __future__ import annotations

import json

from factories import (
    FakeSpark,
    FakeSql,
    build_action,
    installation_context,
    resolved_target,
    warehouse_context,
)
from support.workspaces import given_resolver, given_workspace

from weaver.build_bundle import execute_install_action
from weaver.build_bundle.executors import default_executors
from weaver.locations import Location

VIEW_SQL = b"CREATE OR REPLACE VIEW {{object:DWG.ActiveCustomer}} AS SELECT 1\n"


# --- result semantics ---------------------------------------------------------


def test_a_successful_action_reports_succeeded_against_its_target():
    spark = FakeSpark()
    action = build_action(payload="p.sql", payload_sha256="unused")

    result = execute_install_action(
        action, VIEW_SQL, context=installation_context(spark=spark)
    )

    assert result.status == "succeeded"
    assert result.action_id == action.id
    assert result.resource_node_id == "Lakehouse/Sales/DWG.Customer"
    assert result.target_id == "target-1"
    assert result.executor == "spark_sql"


def test_a_failing_action_is_recorded_rather_than_raised():
    """A failure is data, here exactly as in an installation.

    If this raised, an installer built on the same path could not record one
    action's failure and carry on to report it — and every caller would need its
    own try/except to find out what went wrong.
    """

    class Exploding(FakeSpark):
        def sql(self, statement):
            raise RuntimeError("no such column")

    result = execute_install_action(
        build_action(payload="p.sql"),
        VIEW_SQL,
        context=installation_context(spark=Exploding()),
    )

    assert result.status == "failed"
    assert result.error_type == "RuntimeError"
    assert "no such column" in result.error_message


def test_an_unknown_executor_is_a_failed_result_naming_it():
    result = execute_install_action(
        build_action(executor="no_such_executor"),
        b"",
        context=installation_context(spark=FakeSpark()),
    )

    assert result.status == "failed"
    assert "no_such_executor" in result.error_message


def test_an_action_is_timed_even_when_it_fails():
    """The report's durations must cover failures too, or a slow failure hides."""

    result = execute_install_action(
        build_action(executor="missing"), b"", context=installation_context()
    )

    assert result.started_at is not None
    assert result.finished_at is not None
    assert result.duration_seconds >= 0


def test_a_skipped_execution_reports_skipped_with_its_details():
    """Not every action does work — an endpoint refresh on a host without one."""

    from weaver.build_bundle.executors.base import SkippedExecution

    class Skipping:
        name = "skipping"

        def execute(self, action, payload, context):
            return SkippedExecution(details={"reason": "unsupported host"})

    result = execute_install_action(
        build_action(executor="skipping"),
        None,
        context=installation_context(),
        executors={"skipping": Skipping()},
    )

    assert result.status == "skipped"
    assert result.details == {"reason": "unsupported host"}


def test_supplied_executors_replace_the_registry_entirely():
    """A test naming its own executors must not silently inherit the real ones."""

    result = execute_install_action(
        build_action(executor="spark_sql"),
        VIEW_SQL,
        context=installation_context(spark=FakeSpark()),
        executors={},
    )

    assert result.status == "failed"
    assert "spark_sql" in result.error_message


def test_the_default_registry_is_used_when_none_is_named():
    assert "spark_sql" in default_executors()

    result = execute_install_action(
        build_action(payload="p.sql"),
        VIEW_SQL,
        context=installation_context(spark=FakeSpark()),
    )

    assert result.status == "succeeded"


# --- what actually reaches the engine -----------------------------------------


def test_a_spark_statement_is_resolved_against_the_batchs_destination():
    """The difference between a build that works and one that looks like it does.

    A two-part name resolves through whatever the session is attached to — the
    Weaver Lakehouse — so an unresolved statement would create the object in the
    control plane and then read it back from there, and pass. The token must be
    gone, and gone in favour of *this batch's* destination.
    """

    spark = FakeSpark()

    execute_install_action(
        build_action(payload="p.sql"),
        VIEW_SQL,
        context=installation_context(spark=spark),
    )

    (statement,) = spark.statements
    assert statement == VIEW_SQL.decode().strip()


def test_a_spark_action_with_no_way_to_run_a_statement_fails_saying_so():
    result = execute_install_action(
        build_action(payload="p.sql"), VIEW_SQL, context=installation_context()
    )

    assert result.status == "failed"
    assert "no Spark SQL capability" in result.error_message


def test_a_spark_action_with_no_destination_refuses_rather_than_guessing():
    """An action with nowhere to go must stop, not land somewhere plausible."""

    result = execute_install_action(
        build_action(payload="p.sql"),
        VIEW_SQL,
        context=installation_context(
            spark=FakeSpark(), target=resolved_target(destination=None)
        ),
    )

    assert result.status == "failed"
    assert "no Spark destination" in result.error_message


def test_a_spark_action_without_a_payload_fails_saying_so():
    result = execute_install_action(
        build_action(payload=None),
        None,
        context=installation_context(spark=FakeSpark()),
    )

    assert result.status == "failed"
    assert "no payload" in result.error_message


# --- the Warehouse side -------------------------------------------------------


def test_tsql_sends_the_script_through_unchanged():
    """The executor adds no logic: the generated script is what the engine gets."""

    sql = FakeSql()
    script = b"CREATE TABLE [DWG].[Customer] ([CustomerId] int NOT NULL);\n"

    result = execute_install_action(
        build_action(executor="tsql", payload="p.sql"),
        script,
        context=warehouse_context(sql=sql),
    )

    assert result.status == "succeeded"
    assert sql.scripts == [script.decode("utf-8")]


def test_a_tsql_action_without_a_sql_executor_fails_saying_so():
    result = execute_install_action(
        build_action(executor="tsql", payload="p.sql"),
        b"SELECT 1",
        context=installation_context(sql=None),
    )

    assert result.status == "failed"
    assert "SQL executor" in result.error_message


def test_a_failing_warehouse_script_is_recorded_as_a_failed_action():
    sql = FakeSql(error=RuntimeError("Invalid column name 'NoSuchColumn'"))

    result = execute_install_action(
        build_action(executor="tsql", payload="p.sql"),
        b"CREATE VIEW x AS SELECT NoSuchColumn FROM y",
        context=warehouse_context(sql=sql),
    )

    assert result.status == "failed"
    assert "NoSuchColumn" in result.error_message


def test_a_tsql_batch_submits_each_statement_separately():
    """Not cosmetic: T-SQL rejects two CREATE VIEWs in one batch outright."""

    sql = FakeSql()
    payload = json.dumps(
        ["CREATE OR ALTER VIEW a AS SELECT 1", "CREATE OR ALTER VIEW b AS SELECT 2"]
    ).encode("utf-8")

    result = execute_install_action(
        build_action(executor="tsql_batch", payload="p.json"),
        payload,
        context=warehouse_context(sql=sql),
    )

    assert result.status == "succeeded"
    assert sql.scripts == [
        "CREATE OR ALTER VIEW a AS SELECT 1",
        "CREATE OR ALTER VIEW b AS SELECT 2",
    ]


def test_a_tsql_batch_payload_that_is_not_an_array_is_rejected():
    result = execute_install_action(
        build_action(executor="tsql_batch", payload="p.json"),
        b'"CREATE VIEW a AS SELECT 1"',
        context=warehouse_context(),
    )

    assert result.status == "failed"
    assert "array of statements" in result.error_message


# --- the load layer's file executor -------------------------------------------


class _TableSpark:
    """A session that can say what a built table's columns are, and no more.

    That is the whole capability a generated load needs at install: the columns
    are the one thing generation cannot know, so the installer reads them.
    """

    def __init__(self, columns):
        self._columns = columns

    def table(self, name):
        fields = [type("F", (), {"name": c})() for c in self._columns]
        return type("Frame", (), {"schema": type("S", (), {"fields": fields})()})()


AUDIT = ("row_insert_datetime", "row_update_datetime", "row_delete_datetime")


def _load_context(tmp_path, columns=("Customer id", "Customer name")):
    """A real store and resolver, because placement is the claim being made."""

    from weaver.store import FilesystemStore

    workspace = given_workspace(catalogue="Warehouse/Weaver")
    return installation_context(
        store=FilesystemStore(),
        # Rooted on this test's own filesystem, so what the resolver names is
        # somewhere the store can actually write.
        resolver=given_resolver(workspace=workspace, root=tmp_path),
        target=resolved_target(),
        spark=_TableSpark(tuple(columns) + AUDIT),
    )


def _load_action(*, kind: str, relative: str, payload: str | None):
    from factories import ITEM

    from weaver.etl import LOAD_ROOT

    return build_action(
        id=f"load-{relative}",
        kind=kind,
        resource_node_id=f"{ITEM}/file:{LOAD_ROOT}/{relative}",
        executor="load_file",
        payload=payload,
        payload_sha256="unused" if payload else None,
    )


def test_a_deployed_file_lands_under_the_runtime_tree(tmp_path):
    """Placement comes from the identity and the bound target, and from nothing
    the executor decides — that was settled when the artefact was claimed."""

    from weaver.etl import LOAD_ROOT

    context = _load_context(tmp_path)
    action = _load_action(
        kind="write_file", relative="lib/dates.py", payload="p.payload"
    )

    result = execute_install_action(action, b"def parse(value):\n", context=context)

    assert result.status == "succeeded"
    written = result.details["written"]
    assert written.endswith(f"Files/{LOAD_ROOT}/lib/dates.py")
    assert context.store.read(Location(written)) == b"def parse(value):\n"


def test_a_generated_load_module_is_addressed_as_it_lands(tmp_path):
    """The bundle stays destination-free; the installed file must be runnable.

    Keyed on what the payload *is*, not what it is called. A generated module and
    an authored one are both `.py` in one tree, so only the first line tells them
    apart — and a rule that read the name instead once shipped every installed
    program with its tokens intact.
    """

    from weaver.declaration import read_source_document
    from weaver.declaration.model import LAKEHOUSE

    document = read_source_document(
        "Sales.OrderSummary.sql", _SPARK_LOAD_SOURCE.encode("utf-8"), LAKEHOUSE
    )
    payload = document.create_load(destination=resolved_target().destination).payload

    context = _load_context(
        tmp_path,
        columns=(
            "Customer id",
            "Total amount",
            "row_insert_datetime",
            "row_update_datetime",
            "row_delete_datetime",
        ),
    )
    action = _load_action(
        kind="write_file", relative="Sales__OrderSummary.py", payload="p.payload"
    )

    result = execute_install_action(action, payload, context=context)
    written = context.store.read(Location(result.details["written"])).decode()

    assert written.lstrip().startswith("# Weaver generated load")
    assert "Total amount" in written
    assert "{{" not in written, "a generated module carries no unresolved token"
    assert "`Sales_LH`" in written, "and names the Lakehouse it reads"


def test_a_deployed_python_module_is_left_exactly_as_authored(tmp_path):
    """A module is source code, not a statement.

    It addresses its target through the resolved Lakehouse it is constructed
    with, so nothing in it is Weaver's to rewrite.
    """

    context = _load_context(tmp_path)
    action = _load_action(
        kind="write_file", relative="Sales__Customer.py", payload="p.payload"
    )
    source = b'"""Table ID: Sales.Customer"""\nBRACES = "{{not a token}}"\n'

    result = execute_install_action(action, source, context=context)

    assert context.store.read(Location(result.details["written"])) == source


_SPARK_LOAD_SOURCE = """/*
Table ID: Sales.OrderSummary

Description: Order totals.

Lineage: $Sales.Order

Dependencies:
  - Sales.Order

Primary key: Customer id

Schema:
  Customer id: string
  Total amount: decimal(18,2)
*/
select `Customer id`, cast(sum(`Amount`) as decimal(18,2)) as `Total amount`
  from Sales.Order group by `Customer id`;
"""


def test_a_write_creates_the_directories_beneath_it(tmp_path):
    """A module several packages deep needs no folder action to precede it."""

    context = _load_context(tmp_path)
    action = _load_action(
        kind="write_file", relative="lib/nested/deep/dates.py", payload="p.payload"
    )

    assert (
        execute_install_action(action, b"x = 1\n", context=context).status
        == "succeeded"
    )


def test_a_write_without_its_bytes_fails_rather_than_writing_nothing(tmp_path):
    """An empty file is a plausible-looking wrong answer, so it is refused."""

    context = _load_context(tmp_path)
    action = _load_action(
        kind="write_file", relative="lib/dates.py", payload="p.payload"
    )

    result = execute_install_action(action, None, context=context)

    assert result.status == "failed"
    assert "no payload" in result.error_message


def test_removing_a_file_that_is_already_gone_is_the_state_it_wanted(tmp_path):
    """Tolerant of absence, and only here.

    A delete reconciles toward "this must not exist", and something else having
    removed it first is that state reached — unlike a create, where a collision
    means two things believe they own one name.
    """

    context = _load_context(tmp_path)
    action = _load_action(kind="delete_file", relative="lib/dates.py", payload=None)

    result = execute_install_action(action, None, context=context)

    assert result.status == "succeeded"
    assert "absent" in result.details


def test_a_deployed_file_is_removed_where_it_was_written(tmp_path):
    context = _load_context(tmp_path)
    write = _load_action(
        kind="write_file", relative="lib/dates.py", payload="p.payload"
    )
    execute_install_action(write, b"x = 1\n", context=context)

    delete = _load_action(kind="delete_file", relative="lib/dates.py", payload=None)
    result = execute_install_action(delete, None, context=context)

    assert result.status == "succeeded"
    assert not context.store.exists(Location(result.details["deleted"]))
