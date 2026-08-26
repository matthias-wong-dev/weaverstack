"""One Delta keyed refusal, executed by a real Spark engine.

``weaver.runtime.table_load`` runs where Spark is, so what a desktop can prove
about it is what it submits. That is
``tests/targeted/test_delta_load_execution_boundary.py``: a recording double that
answers cardinalities, parses no SQL and models no relation. Exhaustive, and
blind to the engine.

This is the engine boundary, once. One declaration with a primary key, a
required column and two unique keys, one staging frame carrying every kind of bad
row, and the two outcomes that declaration can produce:

.. code-block:: text

    intolerant   nothing is written, and the rejects say why
    tolerant     the survivors load, and the rejects stand beside them

No build and no estate. The table is arranged in the session the way a build
would have made it, business columns, the audit columns and the row signature,
and the schema goes when the body ends. One submission, one evidence payload.

``full_integration``, because a real Spark reconciliation is the most expensive
thing this file could ask for and it is asked once. The rest of the matrix, the
merge cases, the untouched-holder refusals, the signature arithmetic, is the core
suite's, matched claim for claim with
``tests/fabric/test_warehouse_load_primitive.py``.
"""

from __future__ import annotations

from support.weaver_test import weaver_test

#: The schema this body owns. Its own, so nothing an estate declares is beside
#: it, and it is dropped when the body ends.
SCHEMA = "Refusal"
OBJECT = "DeltaConstrained"

#: The declaration the load reads. A key, a required column, a nullable unique
#: column and a composite unique key. The same shape the Warehouse file uses, so
#: the two engines answer the same question.
HEADER = """Table ID: {schema}.{object}

Description: Customers.

Lineage: The sales system.

Primary key: Customer id

Not null:
  - Customer name

Unique keys:
  - Email
  - Region id, External ref

Schema:
  Customer id: string
  Customer name: string
  Email: string
  Region id: int
  External ref: string
"""

#: Every recoverable refusal the declaration can produce: a blank key, a
#: duplicate key, a null in a required column, and a collision under each of the
#: two unique keys. ``c8`` and ``c9`` share a null Email, which claims neither.
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

BODY = r'''
from weaver import lakehouse_for
from weaver.declaration.metadata import PYTHON, parse_document
from weaver.errors import LoadError
from weaver.runtime.delta_sql import (
    COLUMN_MAPPING,
    delta_audit_names,
    delta_signature_name,
)
from weaver.runtime.load_contract import LoadContract
from weaver.runtime.table_load import load_table

destination = lakehouse_for(resolver, target)
COLUMNS = ["Customer id", "Customer name", "Email", "Region id", "External ref"]
TYPES = ["string", "string", "string", "int", "string"]
WORKING = ("_Staging", "_Reject", "_Delete", "_Upsert", "_Change", "_StagingKeep")

contract = LoadContract.from_document(
    parse_document(HEADER.format(schema=SCHEMA, object=OBJECT), language=PYTHON)
)


def qualified(suffix=""):
    return destination.qualify(SCHEMA, OBJECT + suffix)


def arrange():
    """The table a build would have made, and nothing left of a previous run."""

    spark.sql(destination.destination.create_schema_statement(SCHEMA))
    for suffix in (*WORKING, ""):
        spark.sql(f"DROP TABLE IF EXISTS {qualified(suffix)}")
    business = ", ".join(f"`{name}` {kind}" for name, kind in zip(COLUMNS, TYPES))
    audit = ", ".join(f"`{name}` timestamp NOT NULL" for name in delta_audit_names())
    signature = f"`{delta_signature_name()}` string NOT NULL"
    spark.sql(
        f"CREATE TABLE {qualified()} ({business}, {audit}, {signature}) "
        f"USING delta {COLUMN_MAPPING}"
    )


def run(fault_tolerant):
    """One load, reported the way a caller sees it however it ended."""

    frame = spark.createDataFrame(
        [tuple(row) for row in REFUSABLE],
        ", ".join(f"`{name}` {kind}" for name, kind in zip(COLUMNS, TYPES)),
    )
    try:
        result = load_table(
            spark,
            contract=contract,
            lakehouse=destination,
            staging_frame=frame,
            deletes=None,
            fault_tolerant=fault_tolerant,
        )
        return {"raised": None, "result": result.as_row()}
    except LoadError as refused:
        return {"raised": str(refused), "result": None}


def contents():
    columns = ", ".join("`" + name + "`" for name in COLUMNS)
    rows = spark.sql(
        f"SELECT {columns} FROM {qualified()} ORDER BY `Customer id`"
    ).collect()
    return [[row[column] for column in COLUMNS] for row in rows]


def reasons():
    """Pairs rather than a mapping: a refused row's key may be the missing part."""

    rows = spark.sql(
        "SELECT `Customer id`, `_reject_reason` AS reason "
        f"FROM {qualified('_Reject')}"
    ).collect()
    return sorted(
        ([row["Customer id"], row["reason"]] for row in rows),
        key=lambda pair: pair[1],
    )


def artefacts():
    """Which durable working tables stand beside the object right now."""

    return sorted(
        suffix
        for suffix in WORKING
        if spark.catalog.tableExists(qualified(suffix))
    )


def held():
    """Temporary views a load holds while it runs and gives back in a finally."""

    return sorted(
        view.name
        for view in spark.catalog.listTables()
        if view.isTemporary and view.name.startswith("weaver_")
    )


seen = {}
try:
    arrange()

    # Refusing, intolerantly: nothing may be written and the evidence must stand.
    seen["intolerant"] = run(False)
    seen["intolerant_contents"] = contents()
    seen["intolerant_reasons"] = reasons()
    seen["intolerant_artefacts"] = artefacts()
    seen["intolerant_held"] = held()

    # The same source, tolerated: the survivors load.
    seen["tolerated"] = run(True)
    seen["tolerated_contents"] = contents()
    seen["tolerated_signatures"] = {
        row["Customer id"]: row["sig"]
        for row in spark.sql(
            f"SELECT `Customer id`, `{delta_signature_name()}` AS sig "
            f"FROM {qualified()}"
        ).collect()
    }
    seen["tolerated_artefacts"] = artefacts()
finally:
    spark.sql(
        "DROP SCHEMA IF EXISTS "
        + destination.destination.qualified_schema(SCHEMA)
        + " CASCADE"
    )

emit(seen)
'''


@weaver_test(integration=True)
def test_the_delta_keyed_load_refuses_incoming_rows_and_loads_the_survivors(
    livy_session, fabric_workspace, fabric_target_lakehouse
):
    """Bad rows out, good rows in, and the physical state each outcome leaves."""

    preamble = (
        "from weaver.workspaces import Workspace\n"
        "from weaver.targets import ItemRef\n"
        "from weaver.resolution import resolver_for\n"
        f"workspace = Workspace(workspace={fabric_workspace.workspace!r}, "
        f"catalogue={fabric_workspace.catalogue!r}, "
        f"environment={fabric_workspace.environment!r})\n"
        "resolver = resolver_for(workspace)\n"
        f"target = ItemRef({fabric_target_lakehouse.name!r})\n"
        f"SCHEMA = {SCHEMA!r}\n"
        f"OBJECT = {OBJECT!r}\n"
        f"HEADER = {HEADER!r}\n"
        f"REFUSABLE = {REFUSABLE!r}\n"
    )

    seen = livy_session.run(preamble + BODY).payload

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
    # stopped at the gate, so it had no delete set to propose. Every phase it did
    # reach was a relation held in Spark and given back.
    assert seen["intolerant_artefacts"] == ["_Reject", "_Staging"]
    assert seen["intolerant_held"] == []

    # Tolerated: one row per refusal refused, and the survivors loaded.
    tolerated = seen["tolerated"]["result"]
    assert tolerated["rows_rejected"] == 5
    assert tolerated["rows_inserted"] == 4
    assert [row[0] for row in seen["tolerated_contents"]] == ["c1", "c4", "c8", "c9"]
    assert seen["tolerated_artefacts"] == ["_Reject", "_Staging"]

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
