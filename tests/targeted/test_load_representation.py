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
asserted in ``test_spark_sql_module_representation.py`` and what it *does* is the
ordinary ``Table.load()``, proved once for both authoring languages.
"""

from __future__ import annotations

import pytest
from support.generated_load import procedure as _procedure
from support.weaver_test import weaver_test

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
from weaver.spark import FabricSparkTarget

SALES = FabricSparkTarget(workspace="Demo", lakehouse="Sales_LH")

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
    return read_source_document("Sales.Customer.sql", source.encode("utf-8"), WAREHOUSE)


def _spark(source: str = SPARK_TABLE):
    return read_source_document("Sales.Customer.sql", source.encode("utf-8"), LAKEHOUSE)


def _no_key(source: str) -> str:
    return source.replace("Primary key: Customer id\n\n", "")


# --- what owns a generated load ----------------------------------------------


@weaver_test()
def test_a_warehouse_table_generates_a_stored_procedure():
    load = _warehouse().create_load()

    assert load.object_type == PROCEDURE_OBJECT
    assert load.template_version == TSQL_LOAD_VERSION
    assert b"create or alter procedure [_].[Load Sales.Customer]" in load.payload


@weaver_test()
def test_a_spark_sql_table_generates_a_deployed_module():
    """Compiled into a primitive, not into a load program.

    What the artefact *is* is asserted here; what it contains is
    ``test_spark_sql_module_representation.py``'s.
    """

    load = _spark().create_load(destination=SALES)

    assert load.object_type == FILE_OBJECT
    assert load.template_version == SPARK_LOAD_VERSION
    assert load.payload.decode().lstrip().startswith("# Weaver generated load")


@weaver_test()
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
    "tsql": (9, "90da0c9a720025aa089d91e3714206949ba88896afa27e07f079141fac29debe"),
    "spark": (9, "d0cdda197f8619dc2f679b7ef270154e439b76aaaf27f5001c79b489304a6acf"),
}


@weaver_test()
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
            hashlib.sha256(_spark().create_load(destination=SALES).payload).hexdigest(),
        ),
    }

    assert actual == GENERATED_FINGERPRINTS, (
        "generated output changed. Raise the matching *_LOAD_VERSION and update "
        f"GENERATED_FINGERPRINTS together. Now: {actual}"
    )


@weaver_test()
def test_generation_is_deterministic():
    assert _warehouse().create_load() == _warehouse().create_load()
    assert _spark().create_load(destination=SALES) == _spark().create_load(
        destination=SALES
    )


# --- the Warehouse procedure --------------------------------------------------


@weaver_test()
def test_an_intolerant_run_raises_rather_than_returning_a_quiet_row():
    """`exec [_].[Load S.N]` must fail the way `.load()` does.

    A primitive that returned a row saying `succeeded = 0` where its sibling
    raised would make every caller special-case which one it was talking to.
    """

    payload = _warehouse().create_load().payload.decode()

    assert "throw 51020" in payload  # rows rejected, intolerant
    assert "throw 51021" in payload  # over a stability threshold, intolerant


@weaver_test()
def test_a_breach_never_writes_whatever_fault_tolerant_says():
    """Tolerating exactly the change the threshold prevents would defeat it."""

    payload = _warehouse().create_load().payload.decode()
    breach = payload.index("if @weaver_error is not null")
    insert = payload.index("insert into [Sales].[Customer] (")

    assert breach < insert
    assert "the target was not modified" in payload


@weaver_test()
def test_an_empty_target_is_never_guarded():
    payload = _warehouse().create_load().payload.decode()

    assert "@weaver_target_rows > 0" in payload


@weaver_test()
def test_the_procedure_takes_a_fault_tolerant_parameter_defaulting_to_refusal():
    """Refusing is the default because it is the safe one.

    An operator who has not thought about rejects gets the behaviour that leaves
    the target as it was.
    """

    payload = _warehouse().create_load().payload.decode()

    assert "@fault_tolerant bit = 0" in payload


@weaver_test()
def test_the_procedure_returns_the_result_contract_through_its_signature():
    """Not through a result set, which a caller could not have identified.

    Authored setup may run EXEC or sp_executesql that returns rows of its own,
    so "the result set this procedure produced" is ambiguous in exactly the
    bodies the two-query contract now encourages. A named output is not.
    """

    payload = _warehouse().create_load().payload.decode()

    for column in RESULT_COLUMNS:
        assert f"@{column} " in payload
        assert "= null output" in payload
        assert f"set @{column} = " in payload
    assert "as succeeded" not in payload


@weaver_test()
def test_the_outputs_are_optional_so_the_procedure_stays_runnable_by_hand():
    """`exec [_].[Load Sales.Customer];` must still work, undeclared."""

    payload = _warehouse().create_load().payload.decode()

    for column in RESULT_COLUMNS:
        assert f"@{column} " in payload
    assert payload.count("= null output") == len(RESULT_COLUMNS)


@weaver_test()
def test_the_identity_column_is_excluded_by_asking_the_engine():
    """Not by naming it. The installer filters on `is_identity`, so the load
    cannot insert into a generated column whatever the declaration said."""

    payload = _warehouse().create_load().payload.decode()

    assert "c.is_identity = 0" in payload


@weaver_test()
def test_the_intermediate_tables_are_real_and_named_for_their_object():
    payload = _warehouse().create_load().payload.decode()

    for suffix in ("_Staging", "_Upsert", "_Reject"):
        assert f"[Sales].[Customer{suffix}]" in payload


@weaver_test()
def test_a_keyed_load_rejects_blank_and_duplicate_keys():
    """One vocabulary across all four primitives.

    A reject table is read by people, so a Warehouse reject saying one thing and
    a Delta reject saying another would make the same refusal look like two
    different problems.
    """

    payload = _warehouse().create_load().payload.decode()

    assert REASON_BLANK_PK in payload
    assert REASON_DUPLICATE_PK in payload


@weaver_test()
def test_an_unkeyed_load_replaces_wholesale_and_rejects_nothing():
    """With no key no row can be matched, so there is nothing to reject."""

    payload = _warehouse(_no_key(WAREHOUSE_TABLE)).create_load().payload.decode()

    assert "delete from [Sales].[Customer];" in payload
    assert "_Reject" not in payload
    assert REASON_DUPLICATE_PK not in payload


@weaver_test()
def test_a_non_incremental_load_deletes_rows_the_source_stopped_producing():
    payload = _warehouse().create_load().payload.decode()

    assert "delete c" in payload
    # Reported from cardinality, not from @@rowcount: the driver says what the
    # load intended, the target's own count says what happened.
    assert "@weaver_target_before + @weaver_rows_inserted - count(*)" in payload


@weaver_test()
def test_an_incremental_load_deletes_nothing():
    """Absence from a window is not a retirement."""

    payload = _warehouse(_incremental(WAREHOUSE_TABLE)).create_load().payload.decode()

    assert "delete c\n" not in payload
    assert "absence retires nothing" in payload


# --- the keyed state machine ---------------------------------------------------
#
# One reconciliation model, and these assert the decisions that make it that
# model rather than its layout. What the procedure *does* is proved by running it
# (``tests/fabric/test_warehouse_load_primitive.py``).
#
# Two texts, and which one a claim belongs to matters. The installer reads the
# built table's columns and assembles the procedure; the procedure is what Fabric
# ends up running. Which columns are hashed is the installer's claim, the
# reconciliation is the procedure's.


CONSTRAINED = WAREHOUSE_TABLE.replace(
    """Schema:
  Customer id: varchar(50)
  Customer name: varchar(200)""",
    """Not null:
  - Customer name

Unique keys:
  - Email
  - Region id, External ref

Schema:
  Customer id: varchar(50)
  Customer name: varchar(200)
  Email: varchar(100)
  Region id: int
  External ref: varchar(30)""",
).replace(
    "select [Customer id], [Customer name] from [Src].[Raw]",
    "select [Customer id], [Customer name], [Email], [Region id], [External ref] "
    "from [Src].[Raw]",
)


def _installer(source: str = WAREHOUSE_TABLE) -> str:
    return _warehouse(source).create_load().payload.decode()


def _body(source: str = WAREHOUSE_TABLE) -> str:
    return _procedure(_installer(source))


def _constrained_source(*, incremental: bool = False) -> str:
    """The constrained object, keyed and validated.

    Incremental here also names the keys it retires, so the merge check has a
    delete relation to excuse a departing holder with.
    """

    if not incremental:
        return CONSTRAINED
    return _incremental(CONSTRAINED).replace(
        "from [Src].[Raw]",
        "from [Src].[Raw];\n\nselect [Customer id] from [Src].[Retired]",
    )


@weaver_test()
def test_staging_is_the_authored_output_and_nothing_else():
    """No rank, no signature, no framework column over the whole population.

    Ranking every staged row to find the few that duplicate a key was the
    expensive half of the old keyed load, and it ranked rows that were about to be
    refused for other reasons anyway.
    """

    body = _body()

    assert "into [Sales].[Customer_Staging] from [Src].[Raw];" in body
    assert "__weaver_pk_row_number" not in body
    assert "#weaver_staging_source" not in body


@weaver_test()
def test_change_is_detected_by_a_stored_row_signature():
    """One equality test against a stored digest, not a row-wide comparison.

    The old keyed load compared every column of every matched row through a
    correlated EXCEPT. A keyed target now stores what it was last loaded as.
    """

    body = _body()

    assert "convert(varbinary(32), hashbytes('SHA2_256'" in body
    assert "q.[Row signature] <> t.[Row signature]" in body
    assert "except" not in body.lower()


@weaver_test()
def test_the_signature_is_taken_over_the_comparison_columns():
    """Not over every column, and not over Weaver's own.

    The comparison set is what the object said decides whether a row changed, so
    hashing more than it would report a change the declaration excluded. Which
    columns those are is settled by the installer, against the built table.
    """

    installer = _installer()

    assert "and lower(c.name) not in (N'customer id')" in installer
    assert "N'Row signature'" in installer  # kept out of the loadable columns


@weaver_test()
def test_a_narrowed_comparison_set_narrows_the_signature():
    source = WAREHOUSE_TABLE.replace(
        "Primary key: Customer id",
        "Primary key: Customer id\n\nComparison columns: Customer name",
    )

    assert "and lower(c.name) in (N'customer name')" in _installer(source)


@weaver_test()
def test_the_signature_distinguishes_a_null_from_an_empty_string():
    """A canonical payload, not a concatenation.

    Each value is written as its byte length, a colon and itself, so text
    containing the separator cannot be read as two values; a null is written as a
    marker that no present value can produce.
    """

    installer = _installer()

    assert "is null then N''~''" in installer
    assert "datalength(" in installer


@weaver_test()
def test_the_signature_payload_names_each_columns_physical_type():
    """Built at install time because an inferred table's types are the build's.

    A date rendered in the session's own format, or a float at the default
    precision, would move the signature without the row moving.
    """

    installer = _installer()

    assert "case lower(t.name)" in installer
    assert "convert(varchar(10), __COLUMN__, 23)" in installer
    assert "convert(varchar(27), __COLUMN__, 126)" in installer


@weaver_test()
def test_the_target_update_copies_the_signature_rather_than_recomputing_it():
    assert "N'c.[Row signature] = u.[Row signature]'" in _installer()


@weaver_test()
def test_the_target_insert_carries_the_signature_the_upsert_set_computed():
    body = _body()
    insert = body[body.index("insert into [Sales].[Customer] (") :]

    assert "[Row signature]" in insert
    assert "u.[Row signature]" in insert


@weaver_test()
def test_null_validation_follows_the_declaration():
    """A business column is nullable unless the object said otherwise."""

    body = _body(_constrained_source())

    assert "s.[Customer name] is null" in body
    assert "null_column: Customer name" in body
    assert "s.[Email] is null" not in body  # declared nullable, so never checked


@weaver_test()
def test_a_table_declaring_nothing_not_null_checks_only_the_key():
    body = _body()

    assert REASON_BLANK_PK in body
    assert "null_column" not in body


@weaver_test()
def test_duplicate_keys_are_found_by_grouping_and_ranked_only_where_they_are():
    """A narrow grouped scan, then a window over the duplicate groups alone."""

    body = _body()

    assert "group by [Customer id]\n" in body
    assert body.index("having count(*) > 1") < body.index("row_number() over (")
    assert "inner join weaver_duplicate_key as d" in body


@weaver_test()
def test_no_unique_key_machinery_is_generated_when_none_is_declared():
    body = _body()

    assert "weaver_unique_" not in body
    assert "duplicate_unique_key" not in body


@weaver_test()
def test_each_unique_key_reads_the_rows_the_ones_before_it_left():
    """Sequential population semantics, in one statement.

    Overlapping unique keys choosing survivors independently from the unfiltered
    staging population would refuse rows that only one of them objects to.
    """

    body = _body(_constrained_source())
    first = body.index("weaver_unique_1_duplicate as (")
    survivor = body.index("weaver_unique_1_survivor as (")
    second = body.index("weaver_unique_2_duplicate as (")

    assert first < survivor < second
    assert "from weaver_unique_key\n" in body
    assert "from weaver_unique_1_survivor\n" in body


@weaver_test()
def test_a_composite_unique_key_groups_on_its_whole_tuple():
    body = _body(_constrained_source())

    assert "group by [Region id], [External ref]" in body
    assert "duplicate_unique_key: Region id, External ref" in body


@weaver_test()
def test_a_null_in_a_unique_key_does_not_claim_its_value():
    """Two rows carrying a null are not two rows claiming the same thing.

    ``group by`` puts them in one group, so leaving the filter out would refuse
    rows the declaration permits.
    """

    body = _body(_constrained_source())

    assert "where [Email] is not null" in body
    assert "where [Region id] is not null and [External ref] is not null" in body


@weaver_test()
def test_the_rejection_gate_comes_before_any_staging_change():
    """Evidence first, then the decision, then the change."""

    body = _body()
    discovery = body.index("insert into [Sales].[Customer_Reject]")
    gate = body.index("throw 51020")
    purge = body.index("delete from [Sales].[Customer_Staging]")

    assert discovery < gate < purge


@weaver_test()
def test_a_load_that_refused_nothing_changes_no_staging_row():
    body = _body()
    purge = body.index("delete from [Sales].[Customer_Staging]")
    guard = body.rindex("if @weaver_rows_rejected > 0\n", 0, purge)

    assert guard < purge


@weaver_test()
def test_the_delete_set_is_derived_from_clean_staging():
    """So a target row whose only staged proposal was refused is retired.

    Deriving it before the purge would have kept that row on the strength of a
    row the load had already refused, and no later repair pass is needed.
    """

    body = _body()
    purge = body.index("delete from [Sales].[Customer_Staging]")
    derive = body.index("create table [Sales].[Customer_Delete] as")

    assert purge < derive


@weaver_test()
def test_the_upsert_set_holds_new_and_changed_rows_and_no_others():
    body = _body()
    upsert = body[body.index("create table [Sales].[Customer_Upsert] as") :]
    upsert = upsert[: upsert.index(";")]

    assert "t.[Customer id] is null" in upsert
    assert "q.[Row signature] <> t.[Row signature]" in upsert
    assert "_AffectedPK" not in body
    assert "#Loser" not in body
    assert "[_Is change]" not in body


@weaver_test()
def test_a_target_row_with_no_stored_signature_is_refreshed():
    """Not every keyed table's rows come from a keyed load.

    Weaver's own catalogue tables declare a key and are written by the catalogue's
    DML, so the column is nullable. Comparing with an absent signature answers
    unknown, which would skip the row for good rather than refresh it.
    """

    assert "or t.[Row signature] is null" in _body()


# --- would the proposed changes leave a valid target? --------------------------


@weaver_test()
def test_merge_uniqueness_is_checked_for_an_incremental_load_with_unique_keys():
    body = _body(_constrained_source(incremental=True))

    assert "@weaver_merge_conflicts = count(*)" in body
    assert "throw 51022" in body


@weaver_test()
def test_merge_uniqueness_is_not_checked_without_unique_keys():
    body = _body(_incremental(WAREHOUSE_TABLE))

    assert "@weaver_merge_conflicts = count(*)" not in body


@weaver_test()
def test_merge_uniqueness_is_not_checked_for_a_non_incremental_load():
    """It leaves the target equal to clean staging, which is already unique."""

    body = _body(_constrained_source())

    assert "@weaver_merge_conflicts = count(*)" not in body


@weaver_test()
def test_a_merge_conflict_stops_the_load_rather_than_refusing_rows():
    """The incoming rows are individually fine; the state they would leave is not.

    So there is nothing to put in the reject table and nothing to purge — and
    nothing to iterate towards, either.
    """

    body = _body(_constrained_source(incremental=True))
    check = body.index("@weaver_merge_conflicts = count(*)")

    assert check < body.index("/*-- The target, once every gate has passed --*/")
    assert check < body.index("insert into [Sales].[Customer] (")
    assert "throw 51022" in body
    assert "while" not in body.lower()


@weaver_test()
def test_a_merge_conflict_is_fatal_whatever_fault_tolerant_says():
    """That governs recoverable problems with incoming rows, which this is not."""

    body = _body(_constrained_source(incremental=True))
    throw = body.index("throw 51022")
    condition = body[body.rindex("if ", 0, throw) : throw]

    assert "@fault_tolerant" not in condition


@weaver_test()
def test_a_holder_frees_its_value_by_being_deleted_or_by_moving_off_it():
    """And by nothing else. Being in the upsert set is not enough: a row may be
    changing another column and keeping the value it has."""

    body = _body(_constrained_source(incremental=True))

    assert "from [Sales].[Customer_Delete] as d" in body
    assert "moving.[Email] <> holder.[Email] or moving.[Email] is null" in body
    assert (
        "moving.[Region id] <> holder.[Region id] or moving.[Region id] is null" in body
    )


@weaver_test()
def test_the_merge_check_reads_and_writes_nothing():
    body = _body(_constrained_source(incremental=True))
    block = body[
        body.index("@weaver_merge_conflicts = count(*)") : body.index("throw 51022")
    ]

    assert "insert" not in block
    assert "delete " not in block
    assert "update" not in block


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


@weaver_test()
def test_a_second_query_becomes_a_delete_working_table():
    payload = _two_query_payload()

    assert "create table [Sales].[Customer_Delete] as" in payload
    assert "into #weaver_delete_claim_Sales_Customer from #Retired" in payload


@weaver_test()
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


@weaver_test()
def test_the_delete_claim_is_what_the_threshold_counts():
    payload = _two_query_payload()

    assert (
        "select @weaver_prospective_deletes = count(*) from [Sales].[Customer_Delete];"
        in payload
    )
    assert "not a retirement" not in payload


@weaver_test()
def test_the_target_loses_exactly_the_claimed_keys():
    payload = _two_query_payload()

    assert "inner join [Sales].[Customer_Delete] as d" in payload
    # Still reported from cardinality, so a key that was not there to begin with
    # deletes nothing and inflates nothing.
    assert "@weaver_target_before + @weaver_rows_inserted - count(*)" in payload


@weaver_test()
def test_the_delete_table_is_cleaned_up_with_the_others():
    payload = _two_query_payload()

    assert payload.count("drop table [Sales].[Customer_Delete];") == 2


@weaver_test()
def test_setup_runs_where_the_author_put_it():
    """Between the two queries, because that is where it was written."""

    payload = _two_query_payload()
    staging = payload.index("into [Sales].[Customer_Staging]")
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


@weaver_test()
def test_a_cte_query_is_run_as_a_statement_not_as_a_subquery():
    """``with … select …`` is a legal statement and an illegal derived table.

    So a body opening with a CTE cannot be wrapped, and the ``INTO`` has to land
    on the body ``SELECT`` — which is where the offset-exact transform puts it,
    whatever shape the query has.
    """

    payload = _warehouse(CTE_WAREHOUSE).create_load().payload.decode()

    assert "with recent as (" in payload
    assert "into [Sales].[Customer_Staging] from recent;" in payload


@weaver_test()
def test_a_one_query_incremental_table_has_no_delete_table():
    payload = _warehouse(_incremental(WAREHOUSE_TABLE)).create_load().payload.decode()

    assert "Customer_Delete" not in payload


# --- stability thresholds ------------------------------------------------------


GUARDED_WAREHOUSE = WAREHOUSE_TABLE.replace(
    "Primary key: Customer id",
    "Primary key: Customer id\n\nDelete percentage threshold: 2"
    "\n\nUpdate percentage threshold: 7\n\nStability row threshold: 500",
)


@weaver_test()
def test_the_procedure_takes_a_threshold_waiver_defaulting_to_enforcement():
    payload = _warehouse().create_load().payload.decode()

    assert "@ignore_stability_threshold bit = 0" in payload


@weaver_test()
def test_the_declared_thresholds_reach_the_procedure():
    payload = _warehouse(GUARDED_WAREHOUSE).create_load().payload.decode()

    assert "@weaver_target_rows >= 500" in payload
    assert "/ @weaver_target_rows > 2" in payload
    assert "/ @weaver_target_rows > 7" in payload


@weaver_test()
def test_the_thresholds_are_checked_before_the_first_write():
    """A breach must leave the target as it was, so refusing has to be a
    decision not to start rather than an unwind."""

    payload = _warehouse(GUARDED_WAREHOUSE).create_load().payload.decode()
    gate = payload.index("@ignore_stability_threshold = 0 and")
    insert = payload.index("insert into [Sales].[Customer] (")

    assert gate < insert


@weaver_test()
def test_the_defaults_are_the_documented_ones():
    payload = _warehouse().create_load().payload.decode()

    assert "@weaver_target_rows >= 1000000" in payload
    assert "/ @weaver_target_rows > 5" in payload
    assert "/ @weaver_target_rows > 20" in payload
