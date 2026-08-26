"""The shared Delta keyed reconciliation, executed in Fabric.

``hosted``, and it has to be: the subject is ``weaver.runtime.table_load``, which
runs where Spark is. A desktop can submit a script that calls it; it cannot run
it. So these bodies import the installed package and call ``load_table`` itself,
never a local approximation of what it would have done.

What is asserted is the semantics, matched to
``tests/fabric/test_warehouse_load_primitive.py`` claim for claim. Two engines,
one reconciliation model: a bad incoming row is refused and recoverable, and a
set of proposed changes that would leave a declared unique key held by two rows
is refused outright. If the two files disagree the model has diverged.

The target is arranged in the session rather than built by a bundle, because what
needs proving here is the load and the build has a suite of its own. Its shape is
taken from the declaration the load reads, business columns, the audit columns
and the row signature, so the table the load meets is the table a build makes.

One submission per state transition, per the suite's rule. Refusing and
tolerating are one transition each; the merge cases share a submission with the
seed they all start from, because an abort leaves the target untouched and that
is itself what makes the next attempt's starting state known.
"""

from __future__ import annotations

from support.weaver_test import weaver_test

SCHEMA = "DWG"

#: The declaration both estates load from, as its own docstring. A key, a
#: required column, a nullable unique column and a composite unique key. The
#: same declaration the Warehouse file uses, so the two matrices are comparable.
HEADER = """Table ID: {schema}.{object}

Description: Customers.

Lineage: The sales system.

Primary key: Customer id

Not null:
  - Customer name

Unique keys:
  - Email
  - Region id, External ref
{incremental}
Schema:
  Customer id: string
  Customer name: string
  Email: string
  Region id: int
  External ref: string
"""

#: Everything the bodies below share: the destination, the contract, a table of
#: the shape a build would have made, and a way to stage rows and read back.
PREAMBLE = '''
import json

from weaver import lakehouse_for
from weaver.declaration.metadata import parse_document, PYTHON
from weaver.runtime.load_contract import LoadContract
from weaver.runtime.table_load import load_table
from weaver.runtime.delta_sql import (
    COLUMN_MAPPING,
    delta_audit_names,
    delta_signature_name,
)
from weaver.errors import LoadError

destination = lakehouse_for(resolver, target)
COLUMNS = ["Customer id", "Customer name", "Email", "Region id", "External ref"]
TYPES = ["string", "string", "string", "int", "string"]


def contract_for(object_name, incremental):
    header = HEADER.format(
        schema=SCHEMA,
        object=object_name,
        incremental="\\nIncremental: true\\n" if incremental else "",
    )
    return LoadContract.from_document(parse_document(header, language=PYTHON))


def arrange(object_name):
    """The table a build would have made, and nothing left of a previous run."""

    qualified = destination.qualify(SCHEMA, object_name)
    spark.sql(destination.destination.create_schema_statement(SCHEMA))
    for suffix in (
        "_Upsert",
        "_Change",
        "_Reject",
        "_Delete",
        "_StagingKeep",
        "_Staging",
        "",
    ):
        spark.sql(
            f"DROP TABLE IF EXISTS {destination.qualify(SCHEMA, object_name + suffix)}"
        )
    business = ", ".join(
        f"`{name}` {kind}" for name, kind in zip(COLUMNS, TYPES)
    )
    audit = ", ".join(f"`{name}` timestamp NOT NULL" for name in delta_audit_names())
    signature = f"`{delta_signature_name()}` string NOT NULL"
    spark.sql(
        f"CREATE TABLE {qualified} ({business}, {audit}, {signature}) "
        f"USING delta {COLUMN_MAPPING}"
    )
    return qualified


def stage(rows):
    return spark.createDataFrame(
        [tuple(row) for row in rows],
        ", ".join(f"`{name}` {kind}" for name, kind in zip(COLUMNS, TYPES)),
    )


def keys(values):
    return spark.createDataFrame(
        [(value,) for value in values], "`Customer id` string"
    )


def run(contract, rows, deletes=None, fault_tolerant=False):
    """One load, reported the way a caller sees it however it ended."""

    try:
        result = load_table(
            spark,
            contract=contract,
            lakehouse=destination,
            staging_frame=stage(rows),
            deletes=None if deletes is None else keys(deletes),
            fault_tolerant=fault_tolerant,
        )
        return {"raised": None, "result": result.as_row()}
    except LoadError as error:
        return {"raised": str(error), "result": None}


def contents(object_name):
    frame = spark.sql(
        f"SELECT {', '.join(f'`{c}`' for c in COLUMNS)} "
        f"FROM {destination.qualify(SCHEMA, object_name)} ORDER BY `Customer id`"
    )
    return [[row[column] for column in COLUMNS] for row in frame.collect()]


def signatures(object_name):
    frame = spark.sql(
        f"SELECT `Customer id`, `{delta_signature_name()}` AS sig "
        f"FROM {destination.qualify(SCHEMA, object_name)}"
    )
    return {row["Customer id"]: row["sig"] for row in frame.collect()}


def stamps(object_name):
    """Each row's audit times, so a load that should not have touched it shows."""

    insert, update, _delete = delta_audit_names()
    frame = spark.sql(
        f"SELECT `Customer id`, CAST(`{insert}` AS STRING) AS inserted, "
        f"CAST(`{update}` AS STRING) AS updated "
        f"FROM {destination.qualify(SCHEMA, object_name)}"
    )
    return {row["Customer id"]: [row["inserted"], row["updated"]] for row in frame.collect()}


def artefacts(object_name):
    """Which durable working tables stand beside the object right now.

    Evidence is meant to exist only for an outcome that owes one, so the absence
    is as much a claim as the presence.
    """

    return sorted(
        suffix
        for suffix in (
            "_Staging",
            "_Reject",
            "_Delete",
            "_Upsert",
            "_Change",
            "_StagingKeep",
        )
        if spark.catalog.tableExists(destination.qualify(SCHEMA, object_name + suffix))
    )


def held():
    """Temporary views and cached relations the load left in the session.

    A load holds every phase in Spark while it runs and gives them back in a
    finally, so after one returns there must be nothing of its own left.
    """

    views = [
        view.name
        for view in spark.catalog.listTables()
        if view.isTemporary and view.name.startswith("weaver_")
    ]
    return sorted(views)


def reasons(object_name):
    """Pairs rather than a mapping: a refused row's key may be the missing part."""

    frame = spark.sql(
        f"SELECT `Customer id`, `_reject_reason` AS reason "
        f"FROM {destination.qualify(SCHEMA, object_name + '_Reject')}"
    )
    return sorted(
        ([row["Customer id"], row["reason"]] for row in frame.collect()),
        key=lambda pair: pair[1],
    )
'''

#: Every recoverable refusal the declaration can produce, matching the Warehouse
#: file's fixture row for row.
REFUSABLE = """
REFUSABLE = [
    ["c1", "One", "a@x.test", 10, "A"],
    [None, "NoKey", "b@x.test", 10, "B"],
    ["c3", None, "c@x.test", 10, "C"],
    ["c4", "Four", "d@x.test", 10, "D"],
    ["c4", "FourAgain", "e@x.test", 10, "E"],
    ["c6", "Six", "a@x.test", 10, "F"],
    ["c7", "Seven", "g@x.test", 10, "A"],
    ["c8", "Eight", None, 10, "H"],
    ["c9", "Nine", None, 10, "I"],
]
"""

CONSTRAINED_BODY = (
    PREAMBLE
    + REFUSABLE
    + """
OBJECT = "DeltaConstrained"
contract = contract_for(OBJECT, False)
arrange(OBJECT)
seen = {}

# Refusing, intolerantly: nothing may be written and the evidence must stand.
seen["intolerant"] = run(contract, REFUSABLE)
seen["intolerant_contents"] = contents(OBJECT)
seen["intolerant_reasons"] = reasons(OBJECT)
seen["intolerant_artefacts"] = artefacts(OBJECT)
seen["intolerant_held"] = held()

# The same source, tolerated: the survivors load.
seen["tolerated"] = run(contract, REFUSABLE, fault_tolerant=True)
seen["tolerated_contents"] = contents(OBJECT)
seen["tolerated_signatures"] = signatures(OBJECT)
seen["tolerated_artefacts"] = artefacts(OBJECT)

# The accepted rows, restaged as they were loaded. An unchanged source is one
# equality test per row and no work at all. A clean load also leaves nothing
# physical behind, including the evidence the run before it wrote.
accepted = contents(OBJECT)
seen["unchanged"] = run(contract, accepted)
seen["unchanged_signatures"] = signatures(OBJECT)
seen["unchanged_stamps"] = stamps(OBJECT)
seen["unchanged_artefacts"] = artefacts(OBJECT)
seen["unchanged_held"] = held()

changed = [
    [row[0], "Renamed" if row[0] == "c1" else row[1], *row[2:]] for row in accepted
]
seen["updated"] = run(contract, changed)
seen["updated_contents"] = contents(OBJECT)
seen["updated_signatures"] = signatures(OBJECT)
seen["updated_artefacts"] = artefacts(OBJECT)

emit(seen)
"""
)

MERGE_BODY = (
    PREAMBLE
    + """
OBJECT = "DeltaMerge"
contract = contract_for(OBJECT, True)
arrange(OBJECT)
seen = {}

SEED = [
    ["c1", "One", "a@x.test", 10, "A"],
    ["c2", "Two", "b@x.test", 10, "B"],
    ["c3", "Three", "c@x.test", 10, "C"],
    ["c4", "Four", "d@x.test", 10, "D"],
    ["c5", "Five", "e@x.test", 10, "E"],
]
seen["seed"] = run(contract, SEED, deletes=[])
seen["seed_artefacts"] = artefacts(OBJECT)
seen["seed_held"] = held()

# The proposals a holder really does free its value for: a two-way swap, a
# holder moving its own composite tuple, and a claim on a value whose holder
# this same load retires.
seen["allowed"] = run(
    contract,
    [
        ["c1", "One", "b@x.test", 10, "A"],
        ["c2", "Two", "a@x.test", 10, "B"],
        ["c3", "Three", "c@x.test", 10, "Z"],
        ["c4", "Four", "d@x.test", 10, "E"],
    ],
    deletes=["c5"],
)
seen["allowed_contents"] = contents(OBJECT)

# A null claims nothing, so a holder that takes one has given its value up.
seen["null_move"] = run(
    contract,
    [["c1", "One", "c@x.test", 10, "A"], ["c3", "Three", None, 10, "C"]],
    deletes=[],
)
seen["null_move_contents"] = contents(OBJECT)

# Claimed and staged at once: the claim gives the key up, so the row is loaded as
# an ordinary update rather than deleted and re-inserted. c4 is claimed and not
# staged, so it does go.
seen["stamps_before"] = stamps(OBJECT)
seen["claimed_and_staged"] = run(
    contract,
    [
        ["c1", "Renamed", "c@x.test", 10, "A"],  # claimed, and changed
        ["c4", "Four", "d@x.test", 10, "E"],  # claimed, and unchanged
    ],
    deletes=["c1", "c4", "c2"],  # c2 is claimed and not staged, so it goes
)
seen["claimed_and_staged_contents"] = contents(OBJECT)
seen["stamps_after"] = stamps(OBJECT)

# Back to the seed, because the allowed loads above changed the target and every
# conflict below is stated against the seed's holders. The aborts then need no
# reseeding between them, which is itself one of the claims.
seen["reseed"] = run(contract, SEED, deletes=[])
before = contents(OBJECT)

# A value nobody is giving up: c3 holds c@x.test and is not in this load at all.
# c2's rename is valid on its own and must not be applied anyway, the load either
# describes a valid target or does not run.
UNTOUCHED_HOLDER = [
    ["c1", "One", "c@x.test", 10, "A"],
    ["c2", "Renamed", "b@x.test", 10, "B"],
]
seen["untouched_holder"] = run(contract, UNTOUCHED_HOLDER, deletes=[])
# The same question, asked of the composite key: c2 holds (10, B).
seen["composite_holder"] = run(
    contract, [["c1", "One", "a@x.test", 10, "B"]], deletes=[]
)
# Tolerating a bad incoming row is one thing; tolerating a target that is not
# valid under its own declaration is not what fault_tolerant offers.
seen["tolerated_conflict"] = run(
    contract, UNTOUCHED_HOLDER, deletes=[], fault_tolerant=True
)
seen["after_conflicts"] = contents(OBJECT)
seen["before_conflicts"] = before
# A refusal that never reached a row still owes an explanation of what it was
# proposing, and still gives back everything it was holding.
seen["conflict_artefacts"] = artefacts(OBJECT)
seen["conflict_held"] = held()

# No claim at all, which is a different thing from an empty one: absence from an
# incremental source proves nothing, so there is nothing to retire and no delete
# relation is derived. The uniqueness question is still asked, and answered with
# no claim to read, so a holder frees its value only by moving off it.
seen["reseed_again"] = run(contract, SEED)
seen["no_claim"] = run(
    contract, [["c1", "One", "a@x.test", 10, "A"], ["c6", "Six", "f@x.test", 10, "F"]]
)
seen["no_claim_contents"] = contents(OBJECT)
seen["no_claim_artefacts"] = artefacts(OBJECT)
seen["no_claim_held"] = held()
# And with no claim, a value only an untouched holder could free is a conflict.
seen["no_claim_conflict"] = run(contract, UNTOUCHED_HOLDER)

emit(seen)
"""
)


def _header_literal() -> str:
    return f"HEADER = {HEADER!r}\nSCHEMA = {SCHEMA!r}\n"


@weaver_test(hosted=True)
def test_the_delta_keyed_load_refuses_incoming_rows_and_loads_the_survivors(
    fabric_session_env,
):
    """The recoverable half, matched to the Warehouse claim for claim."""

    seen = fabric_session_env.run_python(
        _header_literal() + CONSTRAINED_BODY, label="delta keyed refusals"
    )

    # Intolerant: nothing written, and the evidence left to explain why.
    assert "rows were rejected" in seen["intolerant"]["raised"]
    assert seen["intolerant_contents"] == []
    assert seen["intolerant_reasons"] == [
        [None, "blank_primary_key"],
        ["c4", "duplicate_primary_key"],
        ["c6", "duplicate_unique_key: Email"],
        ["c7", "duplicate_unique_key: Region id, External ref"],
        ["c3", "null_column: Customer name"],
    ]

    # What the source proposed and what was refused, and nothing else: the load
    # stopped at the gate, so it had no delete set to propose.
    assert seen["intolerant_artefacts"] == ["_Reject", "_Staging"]
    assert seen["intolerant_held"] == []

    # Tolerated: the same evidence, and the delete set it settled on. Nothing was
    # there to retire, so there is no delete table to read.
    assert seen["tolerated_artefacts"] == ["_Reject", "_Staging"]

    # Tolerated: one row per refusal refused, and the survivors loaded.
    tolerated = seen["tolerated"]["result"]
    assert tolerated["rows_rejected"] == 5
    assert tolerated["rows_inserted"] == 4
    assert [row[0] for row in seen["tolerated_contents"]] == ["c1", "c4", "c8", "c9"]

    # Valid under both declared keys, and a null claims neither.
    emails = [row[2] for row in seen["tolerated_contents"] if row[2] is not None]
    tuples = [(row[3], row[4]) for row in seen["tolerated_contents"]]
    assert len(emails) == len(set(emails))
    assert len(tuples) == len(set(tuples))
    assert [row[0] for row in seen["tolerated_contents"] if row[2] is None] == [
        "c8",
        "c9",
    ]

    # Every loaded row carries a signature of its own.
    signatures = seen["tolerated_signatures"]
    assert all(signatures.values())
    assert len(set(signatures.values())) == len(signatures)

    # An unchanged source writes nothing and moves no signature.
    unchanged = seen["unchanged"]["result"]
    assert unchanged["succeeded"] is True
    assert (
        unchanged["rows_inserted"],
        unchanged["rows_updated"],
        unchanged["rows_deleted"],
        unchanged["rows_rejected"],
    ) == (0, 0, 0, 0)
    assert seen["unchanged_signatures"] == signatures

    # And it leaves nothing physical, including the evidence the tolerated run
    # before it wrote: every phase was a relation held in Spark and given back.
    assert seen["unchanged_artefacts"] == []
    assert seen["unchanged_held"] == []

    # A changed row is updated, and its signature moves with it. Nobody else's.
    after = seen["updated_signatures"]
    assert seen["updated"]["result"]["rows_updated"] == 1
    assert seen["updated"]["result"]["rows_inserted"] == 0
    assert {row[0]: row[1] for row in seen["updated_contents"]}["c1"] == "Renamed"
    assert after["c1"] != signatures["c1"]
    assert {k: v for k, v in after.items() if k != "c1"} == {
        k: v for k, v in signatures.items() if k != "c1"
    }
    assert seen["updated_artefacts"] == []


@weaver_test(hosted=True)
def test_the_delta_keyed_load_refuses_a_target_its_changes_would_invalidate(
    fabric_session_env,
):
    """The half that is not recoverable, matched to the Warehouse claim for claim.

    A holder gives up a unique value by being deleted, by moving off it, or by
    taking a null. By nothing else, so a claim against an untouched holder stops
    the load, whatever ``fault_tolerant`` says, and leaves the target as it was.
    """

    seen = fabric_session_env.run_python(
        _header_literal() + MERGE_BODY, label="delta merge uniqueness"
    )

    assert seen["seed"]["result"]["rows_inserted"] == 5
    # A clean incremental load, so nothing physical stands and nothing is held.
    assert seen["seed_artefacts"] == []
    assert seen["seed_held"] == []

    allowed = {row[0]: row for row in seen["allowed_contents"]}
    assert seen["allowed"]["result"]["succeeded"] is True
    assert seen["allowed"]["result"]["rows_deleted"] == 1
    assert allowed["c1"][2] == "b@x.test"
    assert allowed["c2"][2] == "a@x.test"
    assert (allowed["c3"][3], allowed["c3"][4]) == (10, "Z")
    assert (allowed["c4"][3], allowed["c4"][4]) == (10, "E")
    assert "c5" not in allowed

    moved = {row[0]: row for row in seen["null_move_contents"]}
    assert seen["null_move"]["result"]["succeeded"] is True
    assert moved["c1"][2] == "c@x.test"
    assert moved["c3"][2] is None

    # A key the source still produces is not retired: the claim gives it up and the
    # row is loaded normally, which is what keeps its insert time and leaves an
    # unchanged row alone. A key claimed and not staged still goes.
    claimed = seen["claimed_and_staged"]["result"]
    now = {row[0]: row for row in seen["claimed_and_staged_contents"]}
    before_stamps = seen["stamps_before"]
    after_stamps = seen["stamps_after"]
    assert claimed["succeeded"] is True
    assert (
        claimed["rows_deleted"],
        claimed["rows_inserted"],
        claimed["rows_updated"],
    ) == (1, 0, 1)
    assert "c2" not in now
    assert now["c1"][1] == "Renamed"
    assert now["c4"][1] == "Four"
    assert after_stamps["c1"][0] == before_stamps["c1"][0]
    assert after_stamps["c4"] == before_stamps["c4"]

    assert seen["reseed"]["result"]["succeeded"] is True
    for case in ("untouched_holder", "composite_holder", "tolerated_conflict"):
        assert "declared unique key" in (seen[case]["raised"] or ""), case
    assert seen["after_conflicts"] == seen["before_conflicts"]
    assert {row[0]: row[1] for row in seen["after_conflicts"]}["c2"] == "Two"

    # The refusal never reached a row, and it still says what it was proposing.
    # No delete table: every claim in these loads was empty.
    assert seen["conflict_artefacts"] == ["_Staging"]
    assert seen["conflict_held"] == []

    # No claim at all. Nothing is retired, the staged rows load, and no delete
    # relation is derived to report that nothing goes.
    assert seen["reseed_again"]["result"]["succeeded"] is True
    no_claim = seen["no_claim"]["result"]
    assert no_claim["succeeded"] is True
    assert no_claim["rows_deleted"] == 0
    assert no_claim["rows_inserted"] == 1
    assert [row[0] for row in seen["no_claim_contents"]] == [
        "c1",
        "c2",
        "c3",
        "c4",
        "c5",
        "c6",
    ]
    assert seen["no_claim_artefacts"] == []
    assert seen["no_claim_held"] == []
    # And the uniqueness gate still refuses a value no proposal frees.
    assert "declared unique key" in (seen["no_claim_conflict"]["raised"] or "")
