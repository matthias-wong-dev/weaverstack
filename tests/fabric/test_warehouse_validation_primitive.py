"""The generated Warehouse validation procedure, against a real Fabric Warehouse.

A *primitive* test: two tables are made, the procedures are installed from
``generate_validation()``, and each is then executed directly. No bundle, no
installer, no orchestrator — the claim is that ``exec [_].[Test S.N]`` works on
its own.

Fabric is the only place several of these can be answered, and they are the ones
the renderer cannot: whether the engine accepts ``select … into #temp`` inside a
procedure, whether ``tempdb.sys.columns`` answers for a session temp table,
whether ``throw`` surfaces as an error a caller can read, and whether one
execution can return both a diagnostic result set and its output counts. The
pure renderer tests in ``tests/targeted/test_tsql_validation_representation.py`` cover
the SQL's *shape*; this covers the engine's *answer*.

The outcomes match ``tests/spark/test_validation_comparison_primitive.py``
deliberately. Two engines, one set of validation semantics; if the two files
disagree, the semantics have diverged.
"""

from __future__ import annotations

import pytest

from weaver.declaration import read_source_document
from weaver.declaration.model import WAREHOUSE
from weaver.declaration.tsql_validation import RESULT_PARAMETERS
from weaver.declaration.validation import generate_validation

pytestmark = [pytest.mark.fabric, pytest.mark.remote]

SCHEMA = "DWG"

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
def estate(clean_disposable_warehouse):
    """Two tables and four installed validation procedures.

    The procedures come from the generator rather than from hand-written SQL: a
    fixture that installed a procedure by hand would prove the engine accepts
    SQL somebody wrote for the test, not SQL Weaver produces.
    """

    executor = clean_disposable_warehouse.executor
    executor.execute_script(
        f"if schema_id(N'{SCHEMA}') is null exec('create schema [{SCHEMA}]');"
        "if schema_id(N'_') is null exec('create schema [_]');"
    )
    _drop(executor)
    for table in ("ValidationExpected", "ValidationActual"):
        executor.execute_script(
            f"create table [{SCHEMA}].[{table}] "
            "([OrderId] int null, [Amount] int null);"
        )
    for source in PROCEDURES.values():
        document = _document(source)
        executor.execute_script(generate_validation(document).payload.decode("utf-8"))
    yield executor
    _drop(executor)


def _drop(executor) -> None:
    executor.execute_script(
        "\n".join(
            f"drop procedure if exists [_].[{name}];" for name in PROCEDURES
        )
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


def _sides(executor, expected, actual) -> None:
    _rows(executor, "ValidationExpected", expected)
    _rows(executor, "ValidationActual", actual)


def _counts(executor, procedure: str, *, kind: str = "Test", suppress: int = 1):
    return executor.call_procedure(
        f"[_].[{procedure}]",
        inputs=(("suppress_result_set", suppress),),
        outputs=RESULT_PARAMETERS[kind],
    )


# --- what only Fabric can answer ----------------------------------------------


def test_the_generated_procedures_install(estate):
    """`select … into #temp` inside a procedure is the engine's call, not ours."""

    rows = estate.query(
        "select name from sys.procedures "
        f"where name in ({', '.join(repr(name) for name in sorted(PROCEDURES))});"
    )

    assert {str(row["name"]) for row in rows} == set(PROCEDURES)


def test_a_passing_test_reports_zero_both_ways(estate):
    _sides(estate, [(1, 100), (2, 200)], [(1, 100), (2, 200)])

    result = _counts(estate, f"Test {SCHEMA}.OrdersReconcile")

    assert result["missing_count"] == 0
    assert result["unexpected_count"] == 0


def test_the_output_parameters_carry_the_two_counts(estate):
    _sides(estate, [(1, 100), (2, 200)], [(1, 110), (3, 300)])

    result = _counts(estate, f"Test {SCHEMA}.OrdersReconcile")

    assert result["missing_count"] == 2
    assert result["unexpected_count"] == 2


def test_a_changed_row_counts_twice(estate):
    """The physical counting the design settles on, proved on the engine."""

    _sides(estate, [(1, 100)], [(1, 110)])

    result = _counts(estate, f"Test {SCHEMA}.OrdersReconcile")

    assert result["missing_count"] == 1
    assert result["unexpected_count"] == 1


def test_suppression_transfers_no_diagnostic_rows(estate):
    _sides(estate, [(1, 100)], [(1, 110)])

    sets = estate.call_procedure_with_results(
        f"[_].[Test {SCHEMA}.OrdersReconcile]",
        inputs=(("suppress_result_set", 1),),
        outputs=RESULT_PARAMETERS["Test"],
    )

    assert sets.result_sets == ()
    assert sets.outputs["missing_count"] == 1


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


def test_an_unkeyed_test_pairs_nothing(estate):
    _sides(estate, [(1, 100)], [(1, 110)])

    sets = estate.call_procedure_with_results(
        f"[_].[Test {SCHEMA}.OrdersPresent]",
        inputs=(("suppress_result_set", 0),),
        outputs=RESULT_PARAMETERS["Test"],
    )

    keys = [row["_weaver_sk"] for row in sets.result_sets[0]]
    assert len(keys) == 2 and len(set(keys)) == 2


def test_an_assumption_counts_and_returns_its_violations(estate):
    _rows(estate, "ValidationActual", [(1, 100), (2, -5), (3, 0)])

    sets = estate.call_procedure_with_results(
        f"[_].[Assumption {SCHEMA}.OrdersArePositive]",
        inputs=(("suppress_result_set", 0),),
        outputs=RESULT_PARAMETERS["Assumption"],
    )

    assert sets.outputs["violation_count"] == 2
    assert sorted(row["OrderId"] for row in sets.result_sets[0]) == [2, 3]


def test_an_assumption_holding_reports_zero(estate):
    _rows(estate, "ValidationActual", [(1, 100)])

    result = _counts(estate, f"Assumption {SCHEMA}.OrdersArePositive", kind="Assumption")

    assert result["violation_count"] == 0


def test_a_duplicate_key_is_an_error_rather_than_evidence(estate):
    """The engine has to surface the throw as something a caller can read."""

    _sides(estate, [(1, 100), (1, 200)], [(1, 100)])

    with pytest.raises(Exception, match="repeats on the expected side"):
        _counts(estate, f"Test {SCHEMA}.OrdersReconcile")


def test_a_null_key_is_an_error_rather_than_evidence(estate):
    _sides(estate, [(None, 100)], [(1, 100)])

    with pytest.raises(Exception, match="null or blank"):
        _counts(estate, f"Test {SCHEMA}.OrdersReconcile")


def test_mismatched_shapes_are_reported_as_a_contract_failure(estate):
    """`tempdb.sys.columns` has to answer for a session temp table here."""

    _sides(estate, [(1, 100)], [(1, 100)])

    with pytest.raises(Exception, match="same shape"):
        _counts(estate, f"Test {SCHEMA}.OrdersMismatched")


def test_the_procedure_leaves_no_working_tables_behind(estate):
    _sides(estate, [(1, 100)], [(1, 110)])
    _counts(estate, f"Test {SCHEMA}.OrdersReconcile")

    rows = estate.query(
        "select count(*) as n from tempdb.sys.objects "
        "where name like N'#weaver%' and type = N'U';"
    )

    assert int(rows[0]["n"]) == 0
