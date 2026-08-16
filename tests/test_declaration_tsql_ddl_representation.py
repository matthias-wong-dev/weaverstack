"""T-SQL create generation — the self-contained Warehouse build scripts.

These are generation-level checks: the produced script must be self-contained
(materialise and drop its own temp shape table), run its query shape-only,
validate metadata inside the SQL, and create only the authored main table — no
generated view, no ``_Current`` and no ``_History``. Behavioural verification
runs against the Play Warehouse under Fabric; here we pin the generated text.
"""

from __future__ import annotations

import textwrap

from support.weaver_test import weaver_test

from weaver.declaration import read_source_document
from weaver.declaration.model import WAREHOUSE


def _ddl(path: str, text: str):
    """A Warehouse document: the item type is what makes its .sql T-SQL."""

    return read_source_document(
        path, textwrap.dedent(text).lstrip().encode("utf-8"), WAREHOUSE
    ).create_ddl()


INFERRED = """
    /*
    Table ID: Reporting.CustomerReport
    Description: A warehouse report of customers.
    Lineage: $Sales.Customer
    Primary key: CustomerId
    */
    select CustomerId, CustomerName from [Sales_LH].[Sales].[Customer]
"""

DECLARED = """
    /*
    Table ID: Reporting.CustomerReport
    Description: A warehouse report of customers.
    Lineage: $Sales.Customer
    Primary key: CustomerId
    Schema:
      CustomerId: bigint
      CustomerName: varchar(200)
    */
    select CustomerId, CustomerName from [Sales_LH].[Sales].[Customer]
"""

VIEW = """
    /*
    View ID: Reporting.ActiveReport
    Description: Active customers.
    Lineage: $Reporting.CustomerReport
    */
    select CustomerId from [Sales_LH].[Reporting].[CustomerReport]
"""


# --- shared shape -----------------------------------------------------------


@weaver_test()
def test_the_script_is_self_contained():
    content = _ddl("Reporting.CustomerReport.sql", INFERRED).content
    # It creates its own temp shape table and drops it — no external state.
    assert "if object_id('tempdb..#weaver_shape" in content
    assert content.rstrip().endswith(
        "drop table #weaver_shape_Reporting_CustomerReport;"
    )


@weaver_test()
def test_the_query_runs_shape_only():
    content = _ddl("Reporting.CustomerReport.sql", INFERRED).content
    # Guarded so it returns columns and no rows, diverted into the temp table.
    assert "where 1=0" in content
    assert "into #weaver_shape_Reporting_CustomerReport from" in content


@weaver_test()
def test_metadata_is_validated_inside_the_sql():
    content = _ddl("Reporting.CustomerReport.sql", INFERRED).content
    assert "N'Primary key' as metadata_kind" in content
    assert "N'CustomerId' as column_name" in content
    assert "throw 51004" in content
    # Column identity is case-exact, enforced by a binary collation.
    assert "collate Latin1_General_BIN2" in content


@weaver_test()
def test_only_the_main_table_is_built():
    for source in (INFERRED, DECLARED):
        content = _ddl("Reporting.CustomerReport.sql", source).content
        assert "[Reporting].[CustomerReport]" in content
        assert "_Current" not in content
        assert "_History" not in content
        assert "create or alter view" not in content.lower()


@weaver_test()
def test_every_audit_column_is_built_not_null():
    for source in (INFERRED, DECLARED):
        content = _ddl("Reporting.CustomerReport.sql", source).content
        assert "[Row insert datetime] datetime2(6) not null" in content
        assert "[Row update datetime] datetime2(6) not null" in content
        assert "[Row delete datetime] datetime2(6) not null" in content


@weaver_test()
def test_the_not_null_header_drives_inferred_nullability():
    source = """
        /*
        Table ID: Reporting.CustomerReport
        Description: x
        Lineage: $Sales.Customer
        Primary key: CustomerId
        Not null:
          - CustomerName
        */
        select CustomerId, CustomerName, Note from [Sales_LH].[Sales].[Customer]
    """
    content = _ddl("Reporting.CustomerReport.sql", source).content
    # The primary key and the Not null column are both in the not-null CTE the
    # nullability CASE reads; Note is not, so it stays nullable.
    start = content.index("not_null_columns as")
    cte = content[
        start : content.index(")", content.index("names(column_name)", start))
    ]
    assert "N'CustomerId'" in cte
    assert "N'CustomerName'" in cte
    assert "N'Note'" not in cte
    # Nullability comes from the Weaver document header, not the query's own nullability.
    assert "d.is_nullable" not in content
    assert "left join not_null_columns as nn" in content


@weaver_test()
def test_a_primary_key_constraint_is_added():
    for source in (INFERRED, DECLARED):
        content = _ddl("Reporting.CustomerReport.sql", source).content
        assert "constraint [PK_CustomerReport]" in content
        assert "primary key nonclustered" in content
        assert "not enforced" in content
    # Declared mode names the key column literally; inferred mode builds the
    # column list on the server from the primary-key CTE.
    assert (
        "primary key nonclustered ([CustomerId]) not enforced"
        in _ddl("Reporting.CustomerReport.sql", DECLARED).content
    )


# --- inferred vs declared ---------------------------------------------------


@weaver_test()
def test_inferred_builds_the_table_dynamically_from_temp_metadata():
    content = _ddl("Reporting.CustomerReport.sql", INFERRED).content
    # The physical types are computed server-side from the temp columns.
    assert "case bt.base_type" in content
    assert "string_agg" in content
    assert "exec sys.sp_executesql @weaver_create_sql" in content
    # No declared types are baked in.
    assert "varchar(200)" not in content


@weaver_test()
def test_declared_builds_the_table_from_the_declaration_and_validates_the_query():
    content = _ddl("Reporting.CustomerReport.sql", DECLARED).content
    # A static create over the declared types, plus the audit columns.
    assert "[CustomerId] bigint not null" in content
    assert "[CustomerName] varchar(200) null" in content
    # And a server-side check that the query's columns match the declaration.
    assert "is not returned by the query" in content
    assert "is not in the declared schema" in content
    assert "throw 51005" in content
    # Declared mode does not infer types from the temp table.
    assert "case bt.base_type" not in content


@weaver_test()
def test_a_warehouse_view_is_a_strict_create_view():
    ddl = _ddl("Reporting.ActiveReport.sql", VIEW)
    assert ddl.content == (
        "create view [Reporting].[ActiveReport] as\n"
        "select CustomerId from [Sales_LH].[Reporting].[CustomerReport]\n"
    )


# --- identity ---------------------------------------------------------------


IDENTITY_INFERRED = """
    /*
    Table ID: Reporting.CustomerReport
    Description: x
    Lineage: $Sales.Customer
    Primary key: CustomerId
    Identity: CustomerKey
    */
    select CustomerId, CustomerName from [Sales_LH].[Sales].[Customer]
"""

IDENTITY_DECLARED = """
    /*
    Table ID: Reporting.CustomerReport
    Description: x
    Lineage: $Sales.Customer
    Primary key: CustomerId
    Identity: CustomerKey
    Schema:
      CustomerId: bigint
      CustomerName: varchar(200)
    */
    select CustomerId, CustomerName from [Sales_LH].[Sales].[Customer]
"""


@weaver_test()
def test_declared_identity_leads_as_a_native_identity_bigint():
    content = _ddl("Reporting.CustomerReport.sql", IDENTITY_DECLARED).content
    # The Warehouse generates the values, so the column says so and a load never
    # inserts into it.
    assert "[CustomerKey] bigint identity not null" in content


@weaver_test()
def test_inferred_identity_is_added_at_the_front_with_a_collision_guard():
    content = _ddl("Reporting.CustomerReport.sql", IDENTITY_INFERRED).content
    # Added as the leading column of the dynamically built table. It leads the
    # all_columns CTE, so it must name both columns — an unnamed literal there is
    # a T-SQL error ("No column name was specified for column 1 of 'all_columns'").
    assert (
        "select 0 as column_ordinal, "
        "N'[CustomerKey] bigint identity not null' as column_definition" in content
    )
    # Guarded so a query producing the same name is refused, not silently doubled.
    assert "throw 51006" in content
    # The identity is an available column for the metadata check, so a metadata
    # reference to the surrogate resolves though the query does not produce it.
    assert "union all\n\n    select N'CustomerKey' as column_name" in content


# --- a body with two result queries -----------------------------------------


TWO_QUERY = """
    /*
    Table ID: Reporting.CustomerReport
    Description: A warehouse report of customers.
    Lineage: $Sales.Customer
    Primary key: CustomerId
    Incremental: true
    */
    select CustomerId, CustomerName into #Working from [Sales_LH].[Sales].[Customer];

    select CustomerId, CustomerName from #Working;

    select CustomerId from [Sales_LH].[Sales].[Retirement]
"""


@weaver_test()
def test_the_table_is_shaped_from_the_staging_query_not_the_last_one():
    """Which SELECT the table *is* — the first result query, and only it.

    The build and the load take that answer from the same reading, so a body
    whose last query names retired keys does not describe a one-column table.
    """

    content = _ddl("Reporting.CustomerReport.sql", TWO_QUERY).content
    staging = content.index("into #weaver_shape_Reporting_CustomerReport")
    deletes = content.index("into #weaver_delete_shape_Reporting_CustomerReport")

    assert staging < deletes
    assert "select CustomerName" not in content.split("into #weaver_delete_shape")[1]


@weaver_test()
def test_select_into_setup_keeps_its_own_destination():
    """Setup names where its rows go, so the build must not divert it."""

    content = _ddl("Reporting.CustomerReport.sql", TWO_QUERY).content

    assert "into #Working from [Sales_LH].[Sales].[Customer] where 1=0" in content


@weaver_test()
def test_the_delete_query_is_shaped_and_checked_against_the_primary_key():
    content = _ddl("Reporting.CustomerReport.sql", TWO_QUERY).content

    assert "must produce exactly the primary key" in content
    assert "throw 51007" in content
    assert "collate Latin1_General_BIN2" in content


@weaver_test()
def test_both_shape_tables_are_cleaned_up():
    content = _ddl("Reporting.CustomerReport.sql", TWO_QUERY).content

    assert (
        content.count("drop table #weaver_delete_shape_Reporting_CustomerReport;") == 2
    )
    assert content.count("drop table #weaver_shape_Reporting_CustomerReport;") == 2


@weaver_test()
def test_a_one_query_table_shapes_nothing_for_deletion():
    content = _ddl("Reporting.CustomerReport.sql", INFERRED).content

    assert "delete_shape" not in content
    assert "throw 51007" not in content


# --- determinism ------------------------------------------------------------


@weaver_test()
def test_generation_is_deterministic():
    assert _ddl("Reporting.CustomerReport.sql", INFERRED) == _ddl(
        "Reporting.CustomerReport.sql", INFERRED
    )
    assert _ddl("Reporting.CustomerReport.sql", DECLARED) == _ddl(
        "Reporting.CustomerReport.sql", DECLARED
    )
