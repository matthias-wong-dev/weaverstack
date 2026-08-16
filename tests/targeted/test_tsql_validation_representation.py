"""The Warehouse validation procedure, as text.

Everything here is pure: rendering a procedure needs no Warehouse, no columns
and no session, which is the property that makes exhaustive cover of the
generated SQL cheap. What a *real* Warehouse does with it is proved separately
and narrowly in ``tests/fabric``.

Two claims run through all of it. The counts live in the signature rather than a
result set, because authored setup may return rows of its own and "the result
set this produced" would then be a question with no answer. And the installed
procedure and the direct ``--file`` batch are the same body under two wrappers,
because a file run that compiled differently would test a different thing from
the one a build installs.
"""

from __future__ import annotations

import pytest
from support.weaver_test import weaver_test

from weaver.declaration import read_source_document
from weaver.declaration.model import WAREHOUSE
from weaver.declaration.tsql_validation import (
    generate_tsql_validation_batch,
    generate_tsql_validation_script,
    validation_body,
)
from weaver.errors import DiscoveryError

TEST_SOURCE = """/*
Test ID: Sales.OrdersReconcile

Description: Orders reconcile to the independent aggregation.

Primary key: OrderId
*/
select OrderId, sum(Amount) as Amount
into #expected_orders
from Sales.OrderLine
group by OrderId;

select OrderId, Amount from #expected_orders;

select OrderId, Amount from Sales.Orders;
"""

ASSUMPTION_SOURCE = """/*
Assumption ID: Sales.OrdersHaveCustomers

Description: Every order carries a customer.
*/
select OrderId, CustomerId from Sales.Orders where CustomerId is null;
"""


def _document(source: str, path: str):
    return read_source_document(path, source.encode("utf-8"), WAREHOUSE)


def _script(source: str, path: str, procedure: str) -> str:
    document = _document(source, path)
    return generate_tsql_validation_script(
        document.document, document.sql_body, procedure_name=procedure
    )


def _body(source: str, path: str) -> str:
    document = _document(source, path)
    return validation_body(document.document, document.sql_body)


@pytest.fixture
@weaver_test()
def test_script():
    return _script(
        TEST_SOURCE,
        "Warehouse/Reporting/tests/Sales.OrdersReconcile.sql",
        "[_].[Test Sales.OrdersReconcile]",
    )


@pytest.fixture
def assumption_script():
    return _script(
        ASSUMPTION_SOURCE,
        "Warehouse/Reporting/assumptions/Sales.OrdersHaveCustomers.sql",
        "[_].[Assumption Sales.OrdersHaveCustomers]",
    )


def unkeyed(source: str = TEST_SOURCE) -> str:
    return _script(
        source.replace("\nPrimary key: OrderId\n", "\n"),
        "Warehouse/Reporting/tests/Sales.OrdersReconcile.sql",
        "[_].[Test Sales.OrdersReconcile]",
    )


# --- the signature ------------------------------------------------------------


@weaver_test()
def test_a_test_exposes_optional_snake_case_counts(test_script):
    assert "@missing_count bigint = null output" in test_script
    assert "@unexpected_count bigint = null output" in test_script


@weaver_test()
def test_an_assumption_exposes_one_optional_count(assumption_script):
    assert "@violation_count bigint = null output" in assumption_script
    assert "missing_count" not in assumption_script


@weaver_test()
def test_both_expose_suppression_defaulting_to_returning_the_evidence():
    """A person running one by hand wants the rows; orchestration asks for silence."""

    for script in (
        _script(TEST_SOURCE, "t/Sales.OrdersReconcile.sql", "[_].[T]"),
        _script(ASSUMPTION_SOURCE, "a/Sales.OrdersHaveCustomers.sql", "[_].[A]"),
    ):
        assert "@suppress_result_set bit = 0" in script


@weaver_test()
def test_the_counts_come_before_the_flag(test_script):
    signature = test_script.split("as\n", 1)[0]

    assert signature.index("@missing_count") < signature.index("@suppress_result_set")


@weaver_test()
def test_the_procedure_is_created_or_altered_under_its_installed_name(test_script):
    assert test_script.startswith(
        "create or alter procedure [_].[Test Sales.OrdersReconcile]"
    )


@weaver_test()
def test_the_payload_is_the_procedure_not_an_installer(test_script):
    """Unlike a load: a validation names no target, so it needs no sys.columns."""

    assert "sp_executesql" not in test_script
    assert "sys.columns\n" not in test_script.split("as\n", 1)[0]


# --- capturing the authored contract ------------------------------------------


@weaver_test()
def test_the_two_contract_queries_are_captured_as_expected_and_actual(test_script):
    assert (
        "into #weaver_expected_Sales_OrdersReconcile from #expected_orders"
        in test_script
    )
    assert "into #weaver_actual_Sales_OrdersReconcile from Sales.Orders" in test_script


@weaver_test()
def test_the_authored_setup_travels_verbatim():
    """Their formatting, their line breaks, their temp table — untouched.

    Against the body rather than the script, because the procedure indents the
    whole of it and the claim here is about what was *not* rewritten.
    """

    body = _body(TEST_SOURCE, "Warehouse/Reporting/tests/Sales.OrdersReconcile.sql")

    assert (
        "select OrderId, sum(Amount) as Amount\n"
        "into #expected_orders\n"
        "from Sales.OrderLine\n"
        "group by OrderId;"
    ) in body


@weaver_test()
def test_a_select_into_of_the_authors_own_is_not_a_contract_query(test_script):
    """It diverts its rows, so it is working rather than a result."""

    assert "#expected_orders" in test_script
    assert test_script.count("#weaver_expected_Sales_OrdersReconcile") > 1


@weaver_test()
def test_an_assumption_captures_its_one_query(assumption_script):
    assert (
        "into #weaver_violations_Sales_OrdersHaveCustomers from Sales.Orders"
        in assumption_script
    )


@weaver_test()
def test_a_cte_gets_its_into_on_the_body_select():
    script = _script(
        """/*
Test ID: Sales.OrdersReconcile

Description: Orders reconcile.
*/
with totals as (select OrderId, sum(Amount) as Amount from Sales.OrderLine group by OrderId)
select OrderId, Amount from totals;

select OrderId, Amount from Sales.Orders;
""",
        "Warehouse/Reporting/tests/Sales.OrdersReconcile.sql",
        "[_].[T]",
    )

    assert "with totals as (" in script
    assert "select OrderId, Amount into #weaver_expected" in script


@weaver_test()
def test_dynamic_setup_is_carried_rather_than_refused():
    """The contract is about the queries Weaver can see, not about EXEC."""

    script = _script(
        """/*
Test ID: Sales.OrdersReconcile

Description: Orders reconcile.
*/
exec sp_executesql N'select 1 as Ignored into #scratch';

select OrderId from Sales.Expected;

select OrderId from Sales.Orders;
""",
        "Warehouse/Reporting/tests/Sales.OrdersReconcile.sql",
        "[_].[T]",
    )

    assert "exec sp_executesql N'select 1 as Ignored into #scratch';" in script


@weaver_test()
def test_a_wrong_result_count_is_refused():
    with pytest.raises(DiscoveryError, match="must produce exactly 2 result sets"):
        _script(
            TEST_SOURCE + "\nselect 1 as Extra;\n",
            "Warehouse/Reporting/tests/Sales.OrdersReconcile.sql",
            "[_].[T]",
        )


# --- the comparison -----------------------------------------------------------


@weaver_test()
def test_the_symmetric_difference_is_two_way(test_script):
    missing = (
        "select * from #weaver_expected_Sales_OrdersReconcile\n"
        "        except\n"
        "        select * from #weaver_actual_Sales_OrdersReconcile"
    )
    unexpected = (
        "select * from #weaver_actual_Sales_OrdersReconcile\n"
        "        except\n"
        "        select * from #weaver_expected_Sales_OrdersReconcile"
    )

    assert missing in test_script
    assert unexpected in test_script


@weaver_test()
def test_the_counts_are_assigned_from_the_two_differences(test_script):
    assert (
        "select @missing_count = count(*) from #weaver_missing_Sales_OrdersReconcile;"
        in test_script
    )
    assert (
        "select @unexpected_count = count(*) from "
        "#weaver_unexpected_Sales_OrdersReconcile;"
    ) in test_script


@weaver_test()
def test_both_sides_are_materialised_before_either_is_differenced(test_script):
    """One snapshot each, rather than re-running the author's queries."""

    body = test_script
    assert body.index("into #weaver_actual") < body.index("except")


# --- correlation --------------------------------------------------------------


@weaver_test()
def test_a_declared_key_is_ranked_once_across_both_sides(test_script):
    assert "dense_rank() over (order by [OrderId]) as [_weaver_sk]" in test_script
    assert "select [OrderId] from #weaver_missing_Sales_OrdersReconcile" in test_script
    assert "union\n" in test_script


@weaver_test()
def test_the_diagnostic_columns_lead_the_rows(test_script):
    assert "select 'expected' as [_weaver_side], k.[_weaver_sk], d.*" in test_script
    assert "select 'actual', k.[_weaver_sk], d.*" in test_script


@weaver_test()
def test_a_composite_key_ranks_by_every_column_in_declared_order():
    script = _script(
        TEST_SOURCE.replace("Primary key: OrderId", "Primary key: OrderId, LineNo"),
        "Warehouse/Reporting/tests/Sales.OrdersReconcile.sql",
        "[_].[T]",
    )

    assert "dense_rank() over (order by [OrderId], [LineNo])" in script
    assert "k.[OrderId] = d.[OrderId] and k.[LineNo] = d.[LineNo]" in script


@weaver_test()
def test_without_a_key_every_row_gets_a_distinct_one():
    script = unkeyed()

    assert "dense_rank()" not in script
    assert "row_number() over (order by (select null)) as [_weaver_sk]" in script
    assert "@missing_count + row_number() over (order by (select null))" in script


@weaver_test()
def test_without_a_key_no_key_table_is_built():
    assert "#weaver_keys" not in unkeyed()


# --- suppression --------------------------------------------------------------


@weaver_test()
def test_the_diagnostics_sit_behind_the_flag():
    bodies = (
        _body(TEST_SOURCE, "Warehouse/Reporting/tests/Sales.OrdersReconcile.sql"),
        _body(
            ASSUMPTION_SOURCE,
            "Warehouse/Reporting/assumptions/Sales.OrdersHaveCustomers.sql",
        ),
    )
    for body in bodies:
        assert "if @suppress_result_set = 0\nbegin\n" in body


@weaver_test()
def test_suppression_does_not_gate_the_counts(test_script):
    """A suppressed run still reports; it just transfers no rows."""

    counts = test_script.index("select @missing_count")
    gate = test_script.index("if @suppress_result_set = 0")

    assert counts < gate


# --- working tables -----------------------------------------------------------


@weaver_test()
def test_the_temp_tables_are_weaver_reserved_and_named_for_the_validation(test_script):
    for part in ("expected", "actual", "missing", "unexpected"):
        assert f"#weaver_{part}_Sales_OrdersReconcile" in test_script


@weaver_test()
def test_the_temp_tables_are_cleaned_at_both_ends(test_script):
    drop = (
        "if object_id('tempdb..#weaver_expected_Sales_OrdersReconcile') is not null "
        "drop table #weaver_expected_Sales_OrdersReconcile;"
    )

    assert test_script.count(drop) == 2


# --- guards, which are execution failures rather than evidence ----------------


@weaver_test()
def test_mismatched_shapes_throw_rather_than_letting_except_explain(test_script):
    assert "throw 51020" in test_script
    assert "the two sides of a Test must be the same shape" in test_script


@weaver_test()
def test_a_blank_or_null_key_throws(test_script):
    assert "throw 51021" in test_script
    assert test_script.count("is null or blank on the expected side") == 1
    assert test_script.count("is null or blank on the actual side") == 1


@weaver_test()
def test_a_repeating_key_throws(test_script):
    assert "throw 51022" in test_script
    assert "repeats on the expected side" in test_script
    assert "repeats on the actual side" in test_script


@weaver_test()
def test_an_unkeyed_test_has_no_key_guards():
    script = unkeyed()

    assert "throw 51021" not in script
    assert "throw 51022" not in script


@weaver_test()
def test_an_assumption_has_no_guards_at_all(assumption_script):
    """One side, no key, nothing to correlate — so nothing to guard."""

    assert "throw" not in assumption_script


# --- one body, two wrappers ---------------------------------------------------


@weaver_test()
def test_the_direct_batch_runs_the_same_body_as_the_procedure():
    path = "Warehouse/Reporting/tests/Sales.OrdersReconcile.sql"
    document = _document(TEST_SOURCE, path)
    core = validation_body(document.document, document.sql_body)
    batch = generate_tsql_validation_batch(document.document, document.sql_body)

    assert core in batch


@weaver_test()
def test_the_direct_batch_declares_the_procedures_parameters_as_locals():
    document = _document(TEST_SOURCE, "t/Sales.OrdersReconcile.sql")
    batch = generate_tsql_validation_batch(document.document, document.sql_body)

    assert "declare @missing_count bigint;" in batch
    assert "declare @unexpected_count bigint;" in batch
    assert "declare @suppress_result_set bit = 0;" in batch


@weaver_test()
def test_the_direct_batch_projects_the_counts_for_transport():
    document = _document(TEST_SOURCE, "t/Sales.OrdersReconcile.sql")
    batch = generate_tsql_validation_batch(document.document, document.sql_body)

    assert (
        "select @missing_count as [missing_count], "
        "@unexpected_count as [unexpected_count];" in batch
    )


@weaver_test()
def test_the_direct_batch_installs_nothing():
    document = _document(TEST_SOURCE, "t/Sales.OrdersReconcile.sql")
    batch = generate_tsql_validation_batch(document.document, document.sql_body)

    assert "create or alter procedure" not in batch


@weaver_test()
def test_an_assumptions_batch_projects_its_one_count():
    document = _document(ASSUMPTION_SOURCE, "a/Sales.OrdersHaveCustomers.sql")
    batch = generate_tsql_validation_batch(document.document, document.sql_body)

    assert "select @violation_count as [violation_count];" in batch


# --- identifiers --------------------------------------------------------------


@weaver_test()
def test_quoted_and_reserved_key_columns_survive():
    script = _script(
        TEST_SOURCE.replace("Primary key: OrderId", "Primary key: Order Key"),
        "Warehouse/Reporting/tests/Sales.OrdersReconcile.sql",
        "[_].[T]",
    )

    assert "dense_rank() over (order by [Order Key])" in script
    assert "k.[Order Key] = d.[Order Key]" in script
