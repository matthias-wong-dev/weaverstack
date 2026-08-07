"""``SourceDocument.create_load()`` — the generated load, as text.

Rendering claims only. That the generated procedure *works* is proved by
executing it (``tests/fabric/test_warehouse_load_primitive.py``); what is
established here is that the right thing was generated at all, and cheaply
enough to run on every commit.

So these assert decisions, not layout: that a keyed load rejects and an unkeyed
one replaces, that an incremental load does not delete, that identity never
reaches an insert list. Pinning whole scripts would make every legitimate edit
look like a regression.

**The Warehouse procedure is the only generated load *program*.** A Spark SQL
table is compiled into a deployed module instead, so what it generates is
asserted in ``test_spark_sql_module_render.py`` and what it *does* is the
ordinary ``Table.load()``, proved once for both authoring languages.
"""

from __future__ import annotations

import pytest

from weaver.declaration import read_source_document
from weaver.declaration.load import (
    FILE_OBJECT,
    PROCEDURE_OBJECT,
    SPARK_LOAD_VERSION,
    TSQL_LOAD_VERSION,
)
from weaver.declaration.model import LAKEHOUSE, WAREHOUSE
from weaver.runtime.load_contract import REASON_BLANK_PK, REASON_DUPLICATE_PK
from weaver.runtime.load_result import RESULT_COLUMNS

WAREHOUSE_TABLE = """/*
Table ID: Sales.Customer

Description: Customers.

Lineage: The sales system.

Primary key: Customer id

Schema:
  Customer id: varchar(50)
  Customer name: varchar(200)
*/
select [Customer id], [Customer name] from [Src].[Raw]
"""

SPARK_TABLE = """/*
Table ID: Sales.Customer

Description: Customers.

Lineage: The sales system.

Dependencies: []

Primary key: Customer id

Schema:
  Customer id: string
  Customer name: string
*/
select `Customer id`, `Customer name` from sales.raw;
"""


def _warehouse(source: str = WAREHOUSE_TABLE):
    return read_source_document(
        "Sales.Customer.sql", source.encode("utf-8"), WAREHOUSE
    )


def _spark(source: str = SPARK_TABLE):
    return read_source_document(
        "Sales.Customer.sql", source.encode("utf-8"), LAKEHOUSE
    )


def _no_key(source: str) -> str:
    return source.replace("Primary key: Customer id\n\n", "")


# --- what owns a generated load ----------------------------------------------


def test_a_warehouse_table_generates_a_stored_procedure():
    load = _warehouse().create_load()

    assert load.object_type == PROCEDURE_OBJECT
    assert load.template_version == TSQL_LOAD_VERSION
    assert b"create or alter procedure [_].[Load Sales.Customer]" in load.payload


def test_a_spark_sql_table_generates_a_deployed_module():
    """Compiled into a primitive, not into a load program.

    What the artefact *is* is asserted here; what it contains is
    ``test_spark_sql_module_render.py``'s.
    """

    load = _spark().create_load()

    assert load.object_type == FILE_OBJECT
    assert load.template_version == SPARK_LOAD_VERSION
    assert load.payload.decode().lstrip().startswith("# Weaver generated load")


def test_a_view_has_no_generated_load():
    """A view's definition is its query, so there is nothing to run."""

    view = read_source_document(
        "Sales.Active.sql",
        b"/*\nView ID: Sales.Active\n\nDescription: x\n\nLineage: $Sales.Customer\n*/\n"
        b"select 1 as x from [Sales].[Customer]\n",
        WAREHOUSE,
    )
    with pytest.raises(NotImplementedError, match="no generated load"):
        view.create_load()


#: A fingerprint of what each generator currently emits, beside the version that
#: describes it. See the test below.
GENERATED_FINGERPRINTS = {
    "tsql": (8, "aad6d1857f1c284b64377206111a59159a6b43758a2ed1e77f4626acc9ae6f51"),
    "spark": (8, "817cb4d0e2cb82d571a232ee4a73f7a956f4b255cf4378bc49af6c26cd665664"),
}


def test_a_change_to_generation_must_move_its_template_version():
    """A signature is the source's plus the template version.

    So a generator edit that leaves the version alone produces different bytes
    with an unchanged signature, and incremental selection — correctly — rebuilds
    nothing: the estate keeps running the previous generation's artefacts. That
    happened, and it took a Fabric round trip to notice, which is what this test
    exists to make cheap.

    When it fails, raise the matching version *and* update the hash here in the
    same edit, so the two cannot drift apart again.
    """

    import hashlib

    actual = {
        "tsql": (
            TSQL_LOAD_VERSION,
            hashlib.sha256(_warehouse().create_load().payload).hexdigest(),
        ),
        "spark": (
            SPARK_LOAD_VERSION,
            hashlib.sha256(_spark().create_load().payload).hexdigest(),
        ),
    }

    assert actual == GENERATED_FINGERPRINTS, (
        "generated output changed. Raise the matching *_LOAD_VERSION and update "
        f"GENERATED_FINGERPRINTS together. Now: {actual}"
    )


def test_generation_is_deterministic():
    assert _warehouse().create_load() == _warehouse().create_load()
    assert _spark().create_load() == _spark().create_load()


# --- the Warehouse procedure --------------------------------------------------


def test_an_intolerant_run_raises_rather_than_returning_a_quiet_row():
    """`exec [_].[Load S.N]` must fail the way `.load()` does.

    A primitive that returned a row saying `succeeded = 0` where its sibling
    raised would make every caller special-case which one it was talking to.
    """

    payload = _warehouse().create_load().payload.decode()

    assert "throw 51020" in payload  # rows rejected, intolerant
    assert "throw 51021" in payload  # over a stability threshold, intolerant


def test_a_breach_never_writes_whatever_fault_tolerant_says():
    """Tolerating exactly the change the threshold prevents would defeat it."""

    payload = _warehouse().create_load().payload.decode()
    breach = payload.index("if @weaver_error is not null")
    insert = payload.index("insert into [Sales].[Customer] (")

    assert breach < insert
    assert "the target was not modified" in payload


def test_an_empty_target_is_never_guarded():
    payload = _warehouse().create_load().payload.decode()

    assert "@weaver_target_rows > 0" in payload


def test_the_procedure_takes_a_fault_tolerant_parameter_defaulting_to_refusal():
    """Refusing is the default because it is the safe one.

    An operator who has not thought about rejects gets the behaviour that leaves
    the target as it was.
    """

    payload = _warehouse().create_load().payload.decode()

    assert "@fault_tolerant bit = 0" in payload


def test_the_procedure_returns_the_result_contract_through_its_signature():
    """Not through a result set, which a caller could not have identified.

    Authored setup may run EXEC or sp_executesql that returns rows of its own,
    so "the result set this procedure produced" is ambiguous in exactly the
    bodies the two-query contract now encourages. A named output is not.
    """

    payload = _warehouse().create_load().payload.decode()

    for column in RESULT_COLUMNS:
        assert f"@{column} " in payload
        assert f"= null output" in payload
        assert f"set @{column} = " in payload
    assert "as succeeded" not in payload


def test_the_outputs_are_optional_so_the_procedure_stays_runnable_by_hand():
    """`exec [_].[Load Sales.Customer];` must still work, undeclared."""

    payload = _warehouse().create_load().payload.decode()

    for column in RESULT_COLUMNS:
        assert f"@{column} " in payload
    assert payload.count("= null output") == len(RESULT_COLUMNS)


def test_the_identity_column_is_excluded_by_asking_the_engine():
    """Not by naming it. The installer filters on `is_identity`, so the load
    cannot insert into a generated column whatever the declaration said."""

    payload = _warehouse().create_load().payload.decode()

    assert "c.is_identity = 0" in payload


def test_the_intermediate_tables_are_real_and_named_for_their_object():
    payload = _warehouse().create_load().payload.decode()

    for suffix in ("_Staging", "_Upsert", "_Reject"):
        assert f"[Sales].[Customer{suffix}]" in payload


def test_a_keyed_load_rejects_blank_and_duplicate_keys():
    """One vocabulary across all four primitives.

    A reject table is read by people, so a Warehouse reject saying one thing and
    a Delta reject saying another would make the same refusal look like two
    different problems.
    """

    payload = _warehouse().create_load().payload.decode()

    assert REASON_BLANK_PK in payload
    assert REASON_DUPLICATE_PK in payload


def test_an_unkeyed_load_replaces_wholesale_and_rejects_nothing():
    """With no key no row can be matched, so there is nothing to reject."""

    payload = _warehouse(_no_key(WAREHOUSE_TABLE)).create_load().payload.decode()

    assert "delete from [Sales].[Customer];" in payload
    assert "_Reject" not in payload
    assert REASON_DUPLICATE_PK not in payload


def test_a_non_incremental_load_deletes_rows_the_source_stopped_producing():
    payload = _warehouse().create_load().payload.decode()

    assert "delete c" in payload
    # Reported from cardinality, not from @@rowcount: the driver says what the
    # load intended, the target's own count says what happened.
    assert "@weaver_target_before + @weaver_rows_inserted - count(*)" in payload


def test_an_incremental_load_deletes_nothing():
    """Absence from a window is not a retirement."""

    payload = _warehouse(_incremental(WAREHOUSE_TABLE)).create_load().payload.decode()

    assert "delete c\n" not in payload
    assert "not a retirement" in payload


# --- an incremental table's explicit deletes -----------------------------------


def _incremental(source: str) -> str:
    return source.replace(
        "Primary key: Customer id", "Primary key: Customer id\n\nIncremental: true"
    )


#: Setup, the staging query, and a second query naming the keys to retire. The
#: setup sits *between* the two queries deliberately: where an author puts it is
#: where it has to run, or the second query reads a table that does not exist yet.
TWO_QUERY_WAREHOUSE = _incremental(WAREHOUSE_TABLE).replace(
    "select [Customer id], [Customer name] from [Src].[Raw]",
    """select [Customer id], [Customer name] from [Src].[Raw];

select [Customer id] into #Retired from [Src].[Retirement];

select [Customer id] from #Retired""",
)


def _two_query_payload() -> str:
    return _warehouse(TWO_QUERY_WAREHOUSE).create_load().payload.decode()


def test_a_second_query_becomes_a_delete_working_table():
    payload = _two_query_payload()

    assert "create table [Sales].[Customer_Delete] as" in payload
    assert "into #weaver_delete_claim_Sales_Customer from #Retired" in payload


def test_the_delete_claim_is_narrowed_before_anything_is_counted():
    """Distinct, not blank, and present in the target — all three, up front.

    The stability threshold has to be checked against what will really be
    removed. A count of the raw claim would be a count of what was asked for,
    which is a different number whenever a key is repeated or was never there.
    """

    payload = _two_query_payload()

    assert "select distinct c.[Customer id]" in payload
    assert "inner join #weaver_delete_claim_Sales_Customer as d" in payload
    assert "where not (nullif(trim(cast(d.[Customer id]" in payload


def test_the_delete_claim_is_what_the_threshold_counts():
    payload = _two_query_payload()

    assert (
        "select @weaver_prospective_deletes = count(*) from [Sales].[Customer_Delete];"
        in payload
    )
    assert "not a retirement" not in payload


def test_the_target_loses_exactly_the_claimed_keys():
    payload = _two_query_payload()

    assert "inner join [Sales].[Customer_Delete] as d" in payload
    # Still reported from cardinality, so a key that was not there to begin with
    # deletes nothing and inflates nothing.
    assert "@weaver_target_before + @weaver_rows_inserted - count(*)" in payload


def test_the_delete_table_is_cleaned_up_with_the_others():
    payload = _two_query_payload()

    assert payload.count("drop table [Sales].[Customer_Delete];") == 2


def test_setup_runs_where_the_author_put_it():
    """Between the two queries, because that is where it was written."""

    payload = _two_query_payload()
    staging = payload.index("create table [Sales].[Customer_Staging] as")
    setup = payload.index("select [Customer id] into #Retired")
    deletes = payload.index("create table [Sales].[Customer_Delete] as")

    assert staging < setup < deletes


CTE_WAREHOUSE = WAREHOUSE_TABLE.replace(
    "select [Customer id], [Customer name] from [Src].[Raw]",
    """with recent as (
    select [Customer id], [Customer name] from [Src].[Raw]
)
select [Customer id], [Customer name] from recent""",
)


def test_a_cte_query_is_run_as_a_statement_not_as_a_subquery():
    """``with … select …`` is a legal statement and an illegal derived table.

    So the rows cannot be reached through ``from (<query>) as s``, which is how
    the rank would otherwise be computed over them: the Warehouse rejects the
    procedure outright, for every body that opens with a CTE. The query runs as
    the statement it is, into a table, and the rank is computed from that.
    """

    payload = _warehouse(CTE_WAREHOUSE).create_load().payload.decode()

    assert "from (\n" not in payload
    assert "into #weaver_staging_source_Sales_Customer from recent;" in payload
    assert "from #weaver_staging_source_Sales_Customer as s;" in payload


def test_a_one_query_incremental_table_has_no_delete_table():
    payload = _warehouse(_incremental(WAREHOUSE_TABLE)).create_load().payload.decode()

    assert "Customer_Delete" not in payload


# --- stability thresholds ------------------------------------------------------


GUARDED_WAREHOUSE = WAREHOUSE_TABLE.replace(
    "Primary key: Customer id",
    "Primary key: Customer id\n\nDelete percentage threshold: 2"
    "\n\nUpdate percentage threshold: 7\n\nStability row threshold: 500",
)


def test_the_procedure_takes_a_threshold_waiver_defaulting_to_enforcement():
    payload = _warehouse().create_load().payload.decode()

    assert "@ignore_stability_threshold bit = 0" in payload


def test_the_declared_thresholds_reach_the_procedure():
    payload = _warehouse(GUARDED_WAREHOUSE).create_load().payload.decode()

    assert "@weaver_target_rows >= 500" in payload
    assert "/ @weaver_target_rows > 2" in payload
    assert "/ @weaver_target_rows > 7" in payload


def test_the_thresholds_are_checked_before_the_first_write():
    """A breach must leave the target as it was, so refusing has to be a
    decision not to start rather than an unwind."""

    payload = _warehouse(GUARDED_WAREHOUSE).create_load().payload.decode()
    gate = payload.index("@ignore_stability_threshold = 0 and")
    insert = payload.index("insert into [Sales].[Customer] (")

    assert gate < insert


def test_the_defaults_are_the_documented_ones():
    payload = _warehouse().create_load().payload.decode()

    assert "@weaver_target_rows >= 1000000" in payload
    assert "/ @weaver_target_rows > 5" in payload
    assert "/ @weaver_target_rows > 20" in payload
