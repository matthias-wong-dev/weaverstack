"""The generated Warehouse validation procedure, against a real Fabric Warehouse.

A primitive test: two tables are made, the procedures are installed from
``generate_validation()``, and each is then executed directly. No bundle, no
installer, no orchestrator, the claim is that ``exec [_].[Test S.N]`` works on
its own.

Fabric is the only place several of these can be answered, and they are the ones
the renderer cannot: whether the engine accepts ``select … into #temp`` inside a
procedure, whether ``tempdb.sys.columns`` answers for a session temp table,
whether ``throw`` surfaces as an error a caller can read, and whether one
execution can return both a diagnostic result set and its output counts. The
pure renderer tests in ``tests/targeted/test_tsql_validation_representation.py`` cover
the SQL's shape; this covers the engine's answer.

The outcomes match what a Lakehouse validation produces: two
engines, one set of validation semantics. If they disagree, the semantics have
diverged.
"""

from __future__ import annotations

import re

import pytest
from sql_support import (
    entry_point_script,
    forget_installation,
    forget_runtime_state,
    install_runtime_references,
    record_installation,
)
from support.weaver_test import weaver_test

from weaver.declaration import read_source_document
from weaver.declaration.model import WAREHOUSE, WeaverItemId
from weaver.declaration.tsql_validation import (
    RESULT_PARAMETERS,
    generate_tsql_validation_batch,
)
from weaver.declaration.validation import generate_validation

SCHEMA = "DWG"

#: The logical item the installed validations belong to. A status row is keyed by
#: it, so it is named here rather than left to a default.
ITEM = WeaverItemId("Warehouse", "Reporting")

TEST_SOURCE = f"""/*
Test ID: {SCHEMA}.OrdersReconcile

Description: Orders reconcile to the independently derived expected relation.

Primary key: OrderId
*/
select [OrderId], [Amount] from [{SCHEMA}].[ValidationExpected];

select [OrderId], [Amount] from [{SCHEMA}].[ValidationActual];
"""

UNKEYED_SOURCE = TEST_SOURCE.replace(
    f"Test ID: {SCHEMA}.OrdersReconcile", f"Test ID: {SCHEMA}.OrdersPresent"
).replace("\nPrimary key: OrderId\n", "\n")

ASSUMPTION_SOURCE = f"""/*
Assumption ID: {SCHEMA}.OrdersArePositive

Description: Every order carries a positive amount.
*/
select [OrderId], [Amount] from [{SCHEMA}].[ValidationActual] where [Amount] <= 0;
"""

WIDE_SOURCE = f"""/*
Test ID: {SCHEMA}.OrdersMismatched

Description: A Test whose two sides are not the same shape.
*/
select [OrderId], [Amount] from [{SCHEMA}].[ValidationExpected];

select [OrderId] from [{SCHEMA}].[ValidationActual];
"""

PROCEDURES = {
    f"Test {SCHEMA}.OrdersReconcile": TEST_SOURCE,
    f"Test {SCHEMA}.OrdersPresent": UNKEYED_SOURCE,
    f"Test {SCHEMA}.OrdersMismatched": WIDE_SOURCE,
    f"Assumption {SCHEMA}.OrdersArePositive": ASSUMPTION_SOURCE,
}


def _document(source: str):
    from weaver.declaration.metadata import extract_sql_metadata_and_body

    header, _body = extract_sql_metadata_and_body(source)
    name = [
        line.split(":", 1)[1].strip()
        for line in header.splitlines()
        if line.startswith(("Test ID:", "Assumption ID:"))
    ][0]
    directory = "tests" if "Test ID:" in header else "assumptions"
    return read_source_document(
        f"Warehouse/Reporting/{directory}/{name}.sql", source.encode("utf-8"), WAREHOUSE
    )


@pytest.fixture(scope="module")
def estate(clean_disposable_warehouse, fabric_workspace, fabric_initialise_catalogue):
    """Two tables, four installed validation procedures, and the entry point.

    The procedures come from the generator rather than from hand-written SQL: a
    fixture that installed a procedure by hand would prove the engine accepts
    SQL somebody wrote for the test, not SQL Weaver produces.

    The catalogue is built and its runtime tables presented here, because the
    entry point records what a validation found: ``_.TestStatus`` and ``_.Log``
    have to be there for the references to resolve, and another module's wipe may
    have taken the whole ``_`` schema with it.
    """

    fabric_initialise_catalogue()
    executor = clean_disposable_warehouse.executor
    executor.execute_script(
        f"if schema_id(N'{SCHEMA}') is null exec('create schema [{SCHEMA}]');"
        "if schema_id(N'_') is null exec('create schema [_]');"
    )
    install_runtime_references(executor, fabric_workspace.catalogue_item.name)
    record_installation(executor)
    _drop(executor)
    for table in ("ValidationExpected", "ValidationActual"):
        executor.execute_script(
            f"create table [{SCHEMA}].[{table}] "
            "([OrderId] int null, [Amount] int null);"
        )
    documents = [_document(source) for source in PROCEDURES.values()]
    for document in documents:
        executor.execute_script(generate_validation(document).payload.decode("utf-8"))
    # And the fixed entry point over them, because the validation procedures
    # record nothing: `exec _.[Test]` is what runs one by hand and writes the
    # record. It dispatches on the physical procedures, so the item name it
    # records against is supplied here.
    executor.execute_script(entry_point_script("Test"))
    yield executor
    _drop(executor)


def _drop(executor) -> None:
    # The Installation row goes with it: it names this Warehouse as the target
    # of a logical item nothing built, and a row left behind makes the next
    # test's built item ambiguous to the entry points.
    forget_installation(executor)
    executor.execute_script(
        "drop procedure if exists [_].[Test];\n"
        + "\n".join(f"drop procedure if exists [_].[{name}];" for name in PROCEDURES)
        + "\n"
        + "\n".join(
            f"if object_id(N'{SCHEMA}.{table}', N'U') is not null "
            f"drop table [{SCHEMA}].[{table}];"
            for table in ("ValidationExpected", "ValidationActual")
        )
    )


def _rows(executor, table: str, rows) -> None:
    executor.execute_script(f"delete from [{SCHEMA}].[{table}];")
    if rows:
        values = ", ".join(
            "(" + ", ".join("null" if v is None else str(v) for v in row) + ")"
            for row in rows
        )
        executor.execute_script(
            f"insert into [{SCHEMA}].[{table}] ([OrderId], [Amount]) values {values};"
        )


def _working_tables(sql: str) -> tuple[str, ...]:
    """The temp tables a generated validation names, read from its own SQL.

    Taken from the generated text rather than listed here, so a working table
    added to the generator is checked without the test being told about it.
    """

    return tuple(sorted(set(re.findall(r"#weaver_[A-Za-z0-9_]+", sql))))


def _probe(names: tuple[str, ...]) -> str:
    """A row per working table, saying whether it exists in this session."""

    return "\nunion all\n".join(
        f"select N'{name}' as [name], object_id('tempdb..{name}') as [id]"
        for name in names
    )


def _sides(executor, expected, actual) -> None:
    _rows(executor, "ValidationExpected", expected)
    _rows(executor, "ValidationActual", actual)


def _counts(executor, procedure: str, *, kind: str = "Test", suppress: int = 1):
    return executor.call_procedure(
        f"[_].[{procedure}]",
        inputs=(("suppress_result_set", suppress),),
        outputs=RESULT_PARAMETERS[kind],
    )


def _standalone(executor, qualified: str) -> None:
    """``exec _.[Test]``, which is what a person calls and what records.

    No ``@item_name``, so the entry point recovers the logical item from
    ``_.Installation``.
    """

    executor.execute_script(f"exec [_].[Test] @object_name = N'{qualified}';")


def _runner_mode(executor, qualified: str, *, item_name: str) -> None:
    """The runner-style call, with the logical item supplied."""

    executor.execute_script(
        f"exec [_].[Test] @object_name = N'{qualified}', @item_name = N'{item_name}';"
    )


def _test_status(executor, name: str) -> dict | None:
    rows = executor.query(
        "select [Test type] as kind, [Result] as result, "
        "[Failure count] as failures from [_].[TestStatus] "
        f"where [Item type] = N'{ITEM.item_type}' "
        f"and [Item name] = N'{ITEM.item_name}' "
        f"and [Schema name] = N'{SCHEMA}' and [Object name] = N'{name}';"
    )
    return dict(rows[0]) if rows else None


def _forget(executor, *names: str) -> None:
    """Clear what the catalogue records about these validations.

    A sequence of claims about what one call recorded starts from nothing
    recorded, and a row an earlier claim left would be counted by the next one.
    """

    executor.execute_script(
        "".join(forget_runtime_state(SCHEMA, name) for name in names)
    )


# --- what only Fabric can answer ----------------------------------------------


@weaver_test(remote=True, resources={"tds"})
def test_the_generated_procedures_install(estate):
    """`select … into #temp` inside a procedure is the engine's call, not ours."""

    rows = estate.query(
        "select name from sys.procedures "
        f"where name in ({', '.join(repr(name) for name in sorted(PROCEDURES))});"
    )

    assert {str(row["name"]) for row in rows} == set(PROCEDURES)


@weaver_test(remote=True, resources={"tds"})
def test_a_passing_test_reports_zero_both_ways(estate):
    _sides(estate, [(1, 100), (2, 200)], [(1, 100), (2, 200)])

    result = _counts(estate, f"Test {SCHEMA}.OrdersReconcile")

    assert result["missing_count"] == 0
    assert result["unexpected_count"] == 0


@weaver_test(remote=True, resources={"tds"})
def test_the_output_parameters_carry_the_two_counts(estate):
    _sides(estate, [(1, 100), (2, 200)], [(1, 110), (3, 300)])

    result = _counts(estate, f"Test {SCHEMA}.OrdersReconcile")

    assert result["missing_count"] == 2
    assert result["unexpected_count"] == 2


@weaver_test(remote=True, resources={"tds"})
def test_a_changed_row_counts_twice(estate):
    """The physical counting the design settles on, proved on the engine."""

    _sides(estate, [(1, 100)], [(1, 110)])

    result = _counts(estate, f"Test {SCHEMA}.OrdersReconcile")

    assert result["missing_count"] == 1
    assert result["unexpected_count"] == 1


@weaver_test(remote=True, resources={"tds"})
def test_suppression_transfers_no_diagnostic_rows(estate):
    _sides(estate, [(1, 100)], [(1, 110)])

    sets = estate.call_procedure_with_results(
        f"[_].[Test {SCHEMA}.OrdersReconcile]",
        inputs=(("suppress_result_set", 1),),
        outputs=RESULT_PARAMETERS["Test"],
    )

    assert sets.result_sets == ()
    assert sets.outputs["missing_count"] == 1


@weaver_test(remote=True, resources={"tds"})
def test_one_execution_returns_diagnostics_and_counts(estate):
    """Running a Test twice would compare data that could change in between."""

    _sides(estate, [(1, 100), (2, 200)], [(1, 110)])

    sets = estate.call_procedure_with_results(
        f"[_].[Test {SCHEMA}.OrdersReconcile]",
        inputs=(("suppress_result_set", 0),),
        outputs=RESULT_PARAMETERS["Test"],
    )

    assert sets.outputs == {"missing_count": 2, "unexpected_count": 1}
    diagnostics = sets.result_sets[0]
    assert list(diagnostics[0])[:2] == ["_weaver_side", "_weaver_sk"]
    rows = sorted(
        (row["_weaver_side"], row["OrderId"], row["Amount"]) for row in diagnostics
    )
    assert rows == [
        ("actual", 1, 110),
        ("expected", 1, 100),
        ("expected", 2, 200),
    ]
    paired = {row["_weaver_sk"] for row in diagnostics if row["OrderId"] == 1}
    assert len(paired) == 1


@weaver_test(remote=True, resources={"tds"})
def test_an_unkeyed_test_pairs_nothing(estate):
    _sides(estate, [(1, 100)], [(1, 110)])

    sets = estate.call_procedure_with_results(
        f"[_].[Test {SCHEMA}.OrdersPresent]",
        inputs=(("suppress_result_set", 0),),
        outputs=RESULT_PARAMETERS["Test"],
    )

    keys = [row["_weaver_sk"] for row in sets.result_sets[0]]
    assert len(keys) == 2 and len(set(keys)) == 2


@weaver_test(remote=True, resources={"tds"})
def test_an_assumption_counts_and_returns_its_violations(estate):
    _rows(estate, "ValidationActual", [(1, 100), (2, -5), (3, 0)])

    sets = estate.call_procedure_with_results(
        f"[_].[Assumption {SCHEMA}.OrdersArePositive]",
        inputs=(("suppress_result_set", 0),),
        outputs=RESULT_PARAMETERS["Assumption"],
    )

    assert sets.outputs["violation_count"] == 2
    assert sorted(row["OrderId"] for row in sets.result_sets[0]) == [2, 3]


@weaver_test(remote=True, resources={"tds"})
def test_an_assumption_holding_reports_zero(estate):
    _rows(estate, "ValidationActual", [(1, 100)])

    result = _counts(
        estate, f"Assumption {SCHEMA}.OrdersArePositive", kind="Assumption"
    )

    assert result["violation_count"] == 0


@weaver_test(remote=True, resources={"tds"})
def test_a_duplicate_key_is_an_error_rather_than_evidence(estate):
    """The engine has to surface the throw as something a caller can read."""

    _sides(estate, [(1, 100), (1, 200)], [(1, 100)])

    with pytest.raises(Exception, match="repeats on the expected side"):
        _counts(estate, f"Test {SCHEMA}.OrdersReconcile")


@weaver_test(remote=True, resources={"tds"})
def test_a_null_key_is_an_error_rather_than_evidence(estate):
    _sides(estate, [(None, 100)], [(1, 100)])

    with pytest.raises(Exception, match="null or blank"):
        _counts(estate, f"Test {SCHEMA}.OrdersReconcile")


@weaver_test(remote=True, resources={"tds"})
def test_mismatched_shapes_are_reported_as_a_contract_failure(estate):
    """`tempdb.sys.columns` has to answer for a session temp table here."""

    _sides(estate, [(1, 100)], [(1, 100)])

    with pytest.raises(Exception, match="same shape"):
        _counts(estate, f"Test {SCHEMA}.OrdersMismatched")


@weaver_test(remote=True, resources={"tds"})
def test_the_procedure_leaves_no_working_tables_behind(estate):
    """The run and the check share one batch, so they share one session.

    A working table belongs to the connection that made it, and the guard tests
    above abandon theirs by design: a throw leaves them for the next run in that
    session to drop. Counting every ``#weaver`` table in ``tempdb`` would read
    that residue, and residue from any other connection to the Warehouse, as
    this run's.
    """

    _sides(estate, [(1, 100)], [(1, 110)])
    document = _document(TEST_SOURCE)
    names = _working_tables(generate_validation(document).payload.decode("utf-8"))

    sets = estate.query_result_sets(
        "declare @missing bigint, @unexpected bigint;\n"
        f"exec [_].[Test {SCHEMA}.OrdersReconcile]\n"
        "    @missing_count = @missing output\n"
        "  , @unexpected_count = @unexpected output\n"
        "  , @suppress_result_set = 1;\n"
        f"{_probe(names)};"
    )

    assert {str(row["name"]): row["id"] for row in sets[-1]} == dict.fromkeys(names)


@weaver_test(remote=True, resources={"tds"})
def test_a_direct_run_leaves_no_working_tables_behind(estate):
    """The batch form has no procedure to scope its temp tables to.

    ``weaver test --file`` runs the same body on the caller's own connection,
    where a working table lives until something drops it, so here the body's own
    drops are what keeps the promise that a file run installs nothing and leaves
    nothing.
    """

    _sides(estate, [(1, 100)], [(1, 110)])
    document = _document(TEST_SOURCE)
    batch = generate_tsql_validation_batch(document.document, document.sql_body)
    names = _working_tables(batch)

    sets = estate.query_result_sets(f"{batch}\n{_probe(names)};")

    assert {str(row["name"]): row["id"] for row in sets[-1]} == dict.fromkeys(names)


# --- the standalone entry point -----------------------------------------------


@weaver_test(remote=True, resources={"tds"})
def test_the_entry_point_records_a_test_that_found_nothing(estate):
    """``exec _.[Test]`` writes the record; the validation procedure does not.

    The status row is a view over the catalogue's table in every Warehouse but
    the one it lives in, so this is also the claim that a MERGE through that view
    reaches the table behind it.
    """

    _forget(estate, "OrdersReconcile")
    _sides(estate, [(1, 10)], [(1, 10)])

    _standalone(estate, f"{SCHEMA}.OrdersReconcile")

    status = _test_status(estate, "OrdersReconcile")
    assert status["result"] == "Succeeded"
    assert status["kind"] == "Test"
    assert status["failures"] == 0


@weaver_test(remote=True, resources={"tds"})
def test_the_entry_point_records_how_much_a_failing_test_found(estate):
    _forget(estate, "OrdersReconcile")
    _sides(estate, [(1, 10)], [(1, 11)])

    _standalone(estate, f"{SCHEMA}.OrdersReconcile")

    status = _test_status(estate, "OrdersReconcile")
    assert status["result"] == "Failed"
    # A changed row disagrees on both sides, which is two discrepancy rows.
    assert status["failures"] == 2


@weaver_test(remote=True, resources={"tds"})
def test_a_supplied_item_name_records_against_that_item(estate):
    """Runner mode, where the logical item is supplied rather than resolved."""

    _forget(estate, "OrdersReconcile")
    _sides(estate, [(1, 10)], [(1, 10)])

    _runner_mode(estate, f"{SCHEMA}.OrdersReconcile", item_name=ITEM.item_name)

    status = _test_status(estate, "OrdersReconcile")
    assert status["result"] == "Succeeded"


@weaver_test(remote=True, resources={"tds"})
def test_one_entry_point_serves_both_kinds_of_validation(estate):
    """A person asking by name should not have to know which it was declared as."""

    _forget(estate, "OrdersArePositive")
    _sides(estate, [], [(1, -5)])

    _standalone(estate, f"{SCHEMA}.OrdersArePositive")

    status = _test_status(estate, "OrdersArePositive")
    assert status["kind"] == "Assumption"
    assert status["result"] == "Failed"
    assert status["failures"] == 1


@weaver_test(remote=True, resources={"tds"})
def test_a_validation_that_could_not_be_evaluated_is_recorded_and_raised(estate):
    """It found nothing, and zero discrepancies is the answer it must not give.

    A duplicate key is a broken Test rather than a failing one: the procedure
    throws, the entry point records Error with no count, and then raises, a
    validation that could not run must not read as a call that succeeded.
    """

    _forget(estate, "OrdersReconcile")
    _sides(estate, [(1, 10), (1, 11)], [(1, 10)])

    with pytest.raises(Exception) as raised:
        _standalone(estate, f"{SCHEMA}.OrdersReconcile")

    assert "primary key" in str(raised.value).casefold()
    status = _test_status(estate, "OrdersReconcile")
    assert status["result"] == "Error"
    assert status["failures"] is None


@weaver_test(remote=True, resources={"tds"})
def test_a_validation_that_found_something_is_an_answer_rather_than_a_failure(estate):
    """It ran and reported, so the call returns and the row says Failed."""

    _forget(estate, "OrdersReconcile")
    _sides(estate, [(1, 10)], [(1, 11)])

    _standalone(estate, f"{SCHEMA}.OrdersReconcile")

    assert _test_status(estate, "OrdersReconcile")["result"] == "Failed"


@weaver_test(remote=True, resources={"tds"})
def test_the_entry_point_refuses_a_validation_this_warehouse_does_not_hold(estate):
    with pytest.raises(Exception, match="is not a validation"):
        _standalone(estate, f"{SCHEMA}.NotAThing")
