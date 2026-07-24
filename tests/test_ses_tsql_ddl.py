"""T-SQL create generation — the self-contained Warehouse build scripts.

These are generation-level checks: the produced script must be self-contained
(materialise and drop its own temp shape table), run its query shape-only,
validate metadata inside the SQL, and create only the authored main table — no
generated view, no ``_Current`` and no ``_History``. Behavioural verification
runs against the Play Warehouse under Fabric; here we pin the generated text.
"""

from __future__ import annotations

import textwrap

from weaver.ses import read_source_document


def _ddl(path: str, text: str):
    return read_source_document(path, textwrap.dedent(text).lstrip().encode("utf-8")).create_ddl()


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


def test_the_script_is_self_contained():
    content = _ddl("Reporting.CustomerReport.sql", INFERRED).content
    # It creates its own temp shape table and drops it — no external state.
    assert "if object_id('tempdb..#weaver_shape" in content
    assert content.rstrip().endswith("drop table #weaver_shape_Reporting_CustomerReport;")


def test_the_query_runs_shape_only():
    content = _ddl("Reporting.CustomerReport.sql", INFERRED).content
    # Guarded so it returns columns and no rows, diverted into the temp table.
    assert "where 1=0" in content
    assert "into #weaver_shape_Reporting_CustomerReport from" in content


def test_metadata_is_validated_inside_the_sql():
    content = _ddl("Reporting.CustomerReport.sql", INFERRED).content
    assert "N'Primary key' as metadata_kind" in content
    assert "N'CustomerId' as column_name" in content
    assert "throw 51004" in content
    # Column identity is case-exact, enforced by a binary collation.
    assert "collate Latin1_General_BIN2" in content


def test_only_the_main_table_is_built():
    for source in (INFERRED, DECLARED):
        content = _ddl("Reporting.CustomerReport.sql", source).content
        assert "[Reporting].[CustomerReport]" in content
        assert "_Current" not in content
        assert "_History" not in content
        assert "create or alter view" not in content.lower()


def test_every_audit_column_is_built_not_null():
    for source in (INFERRED, DECLARED):
        content = _ddl("Reporting.CustomerReport.sql", source).content
        assert "[Row insert datetime] datetime2(6) not null" in content
        assert "[Row update datetime] datetime2(6) not null" in content
        assert "[Row delete datetime] datetime2(6) not null" in content


def test_a_primary_key_constraint_is_added():
    for source in (INFERRED, DECLARED):
        content = _ddl("Reporting.CustomerReport.sql", source).content
        assert "constraint [PK_CustomerReport]" in content
        assert "primary key nonclustered" in content
        assert "not enforced" in content
    # Declared mode names the key column literally; inferred mode builds the
    # column list on the server from the primary-key CTE.
    assert "primary key nonclustered ([CustomerId]) not enforced" in _ddl(
        "Reporting.CustomerReport.sql", DECLARED
    ).content


# --- inferred vs declared ---------------------------------------------------


def test_inferred_builds_the_table_dynamically_from_temp_metadata():
    content = _ddl("Reporting.CustomerReport.sql", INFERRED).content
    # The physical types are computed server-side from the temp columns.
    assert "case bt.base_type" in content
    assert "string_agg" in content
    assert "exec sys.sp_executesql @weaver_create_sql" in content
    # No declared types are baked in.
    assert "varchar(200)" not in content


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


def test_a_warehouse_view_is_a_create_or_alter_view():
    ddl = _ddl("Reporting.ActiveReport.sql", VIEW)
    assert ddl.content == (
        "create or alter view [Reporting].[ActiveReport] as\n"
        "select CustomerId from [Sales_LH].[Reporting].[CustomerReport]\n"
    )


# --- determinism ------------------------------------------------------------


def test_generation_is_deterministic():
    assert _ddl("Reporting.CustomerReport.sql", INFERRED) == _ddl(
        "Reporting.CustomerReport.sql", INFERRED
    )
    assert _ddl("Reporting.CustomerReport.sql", DECLARED) == _ddl(
        "Reporting.CustomerReport.sql", DECLARED
    )
