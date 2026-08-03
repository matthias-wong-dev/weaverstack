"""``SourceDocument.create_load()`` — the generated load, as text.

Rendering claims only. That the generated procedure and program *work* is proved
by executing them (``tests/fabric/test_warehouse_load_primitive.py`` and
``tests/spark/test_spark_load_primitive.py``); what is established here is that
the right thing was generated at all, and cheaply enough to run on every commit.

So these assert decisions, not layout: that a keyed load rejects and an unkeyed
one replaces, that an incremental load does not delete, that identity never
reaches an insert list, that the payload is destination-free. Pinning whole
scripts would make every legitimate edit look like a regression.
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
from weaver.declaration.spark_load import (
    FAULT_TOLERANT_DEFAULT,
    FAULT_TOLERANT_MARKER,
    statements_of,
)
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
select `Customer id`, `Customer name` from sales.raw
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


def test_a_spark_sql_table_generates_a_runnable_program():
    load = _spark().create_load()

    assert load.object_type == FILE_OBJECT
    assert load.template_version == SPARK_LOAD_VERSION


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
    "tsql": (5, "4235710f72b79d3923425256aee74bfcf8ecdd60c4d28910ac20c6eacf9c5fdf"),
    "spark": (6, "823a5303282a633362159c132c5794c2d12178ca7278511a2077b4c9b5356f2e"),
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


def test_the_procedure_returns_the_result_contract():
    payload = _warehouse().create_load().payload.decode()

    for column in RESULT_COLUMNS:
        assert f"as {column}" in payload


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
    assert REASON_BLANK_PK in _program()
    assert REASON_DUPLICATE_PK in _program()


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

    source = WAREHOUSE_TABLE.replace(
        "Primary key: Customer id", "Primary key: Customer id\n\nIncremental: true"
    )
    payload = _warehouse(source).create_load().payload.decode()

    assert "delete c\n" not in payload
    assert "not a retirement" in payload


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


def test_the_program_records_the_threshold_decision_once():
    """Three things need the answer — the writes, the delete set and the result.

    Recomputing it in each would let them disagree, and a load that reported one
    thing and did another is what the guard exists to prevent.
    """

    program = _program()

    assert program.count("AS within_thresholds") == 1
    # Everything else reads the recorded column rather than deriving it again.
    assert program.count("SELECT within_thresholds FROM") >= 2


# --- the Spark SQL program ----------------------------------------------------


def _program(source: str = SPARK_TABLE) -> str:
    return _spark(source).create_load().payload.decode()


def test_the_program_is_a_statement_list_ending_in_the_result():
    statements = statements_of(_program())

    assert len(statements) > 1
    for column in RESULT_COLUMNS:
        assert column in statements[-1]


def test_the_program_is_destination_free():
    """A bundle payload must generate identically in every environment.

    The installer addresses the file as it writes it; until then every managed
    name is a token, so the same repository produces the same bytes anywhere.
    """

    program = _program()

    assert "{{object:Sales.Customer}}" in program
    assert "sales_lh" not in program.lower()


def test_fault_tolerance_is_a_substituted_answer_not_a_branch():
    """Spark has no `if`, so the gate is a predicate the runner answers.

    It is a commented literal rather than a `{{...}}` token, because that
    namespace belongs to the installer and the installer refuses any token it
    cannot resolve. Reading 0 already, the file refuses rejects unsubstituted.
    """

    program = _program()

    assert FAULT_TOLERANT_MARKER in program
    assert f"{FAULT_TOLERANT_DEFAULT} = 1" in program
    # Nothing in the installer's namespace is left for the runner to answer.
    assert "{{fault_tolerant}}" not in program


def test_the_program_creates_every_table_with_column_mapping():
    """Weaver permits declared column names with spaces; Delta needs mapping on."""

    for statement in statements_of(_program()):
        if statement.startswith("CREATE TABLE"):
            assert "delta.columnMapping.mode" in statement


def test_a_keyed_program_merges_and_only_updates_what_changed():
    program = _program()

    assert "MERGE INTO {{object:Sales.Customer}}" in program
    # Null-safe, so a column going to or from null counts as a change.
    assert "NOT (s.`Customer name` <=> t.`Customer name`)" in program


def test_a_keyed_program_deletes_through_a_materialised_key_set():
    """Delta refuses a subquery in DELETE, and NOT MATCHED BY SOURCE would empty
    the target on the one run that must not touch it."""

    program = _program()

    assert "{{object:Sales.Customer_Delete}}" in program
    assert "WHEN MATCHED THEN DELETE" in program


def test_an_incremental_program_deletes_nothing():
    source = SPARK_TABLE.replace(
        "Primary key: Customer id", "Primary key: Customer id\n\nIncremental: true"
    )
    program = _program(source)

    assert "WHEN MATCHED THEN DELETE" not in program
    assert "_Delete" not in program


def test_an_unkeyed_program_replaces_wholesale():
    program = _program(_no_key(SPARK_TABLE))

    assert "DELETE FROM {{object:Sales.Customer}}" in program
    assert "MERGE INTO" not in program
    assert "_Reject" not in program


def test_no_statement_is_comment_only():
    """The header once quoted the delimiter, so it was cut in half and its first
    line handed to Spark as a statement — which Spark rejects.

    Statements may *open* with a banner naming the section; what must never
    survive the split is a chunk that is nothing but comments.
    """

    for statement in statements_of(_program()):
        assert any(
            line.strip() and not line.lstrip().startswith("--")
            for line in statement.splitlines()
        ), statement


def test_the_authored_body_is_marked_off_from_the_generated_code():
    """A generated artefact is read when something has gone wrong, and the first
    question is which of it the author wrote."""

    program = _program()

    assert "-- Pre-processing" in program
    assert "-- Data transformation (authored)" in program
    assert "-- Post-processing" in program


def test_a_multi_statement_body_runs_its_preamble_and_stages_only_the_query():
    """A body may set a temporary view up before selecting from it.

    Wrapping the whole body in a subquery would put a CREATE inside a FROM. Only
    the last standalone query fills staging; the preamble runs as written.
    """

    source = SPARK_TABLE.replace(
        "select `Customer id`, `Customer name` from sales.raw",
        "create or replace temporary view recent as\n"
        "select * from sales.raw where `Customer id` is not null;\n\n"
        "select `Customer id`, `Customer name` from recent",
    )
    statements = statements_of(_program(source))

    preamble = [s for s in statements if "temporary view recent" in s]
    staging = [s for s in statements if "_Staging}} USING delta" in s]

    assert len(preamble) == 1
    assert "CREATE TABLE" not in preamble[0]
    # Staging selects from the view the preamble made, not from the whole body.
    assert "temporary view" not in staging[0]
    assert "FROM (\n    select `Customer id`, `Customer name` from recent\n) AS s" in staging[0]
