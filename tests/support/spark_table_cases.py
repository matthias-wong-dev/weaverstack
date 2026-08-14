"""The shapes a Spark table build has to get right, shared by both positions.

``spark_table`` is the one build executor that cannot decide its work from the
payload alone: a query's columns and their types are only known by asking Spark.
It asks with ``DESCRIBE QUERY``, in the same submission as whatever setup the
query needs, and then renders the ``CREATE TABLE`` here and sends that.

The cases below are what makes that answer trustworthy, and they are held in one
place because both positions have to agree about them: the emulator running the
executor in process, and a desktop running it against a real Lakehouse with only
the statements crossing. A type that survived one and not the other would be a
difference between environments rather than a property of the build.

Each case is a frozen instruction, exactly as ``weaver.declaration.ddl`` writes
one, plus what the built table must then look like.
"""

from __future__ import annotations

import json
from dataclasses import dataclass

#: The schema every case builds into.
SCHEMA = "SparkTable"

#: Weaver's audit columns, in the Delta spelling and the order a build appends
#: them.
AUDIT_COLUMNS = [
    ["row_insert_datetime", "timestamp", True],
    ["row_update_datetime", "timestamp", True],
    ["row_delete_datetime", "timestamp", True],
]

AUDIT_NAMES = tuple(name for name, _type, _not_null in AUDIT_COLUMNS)


@dataclass(frozen=True)
class TableCase:
    """One table a build declares, and the shape it must end up with."""

    #: The object name within :data:`SCHEMA`.
    name: str
    #: What the table's rows come from. ``{reads}`` is filled with the managed
    #: object named by :attr:`reads`, addressed to the destination — because a
    #: build renders that name rather than deferring it.
    source_query: str
    #: Business column name to the type the built table must carry, in order.
    expected: dict
    #: Statements that must have run for the query to resolve.
    setup: tuple[str, ...] = ()
    #: Header references, as the payload carries them.
    references: tuple = ()
    declared_columns: list | None = None
    #: The object within :data:`SCHEMA` this query reads, where it reads one.
    reads: str | None = None

    def qualified(self, destination) -> str:
        """This table, named as the destination it is built into spells it."""

        return destination.qualify(SCHEMA, self.name)

    def addressed_query(self, destination) -> str:
        if self.reads is None:
            return self.source_query
        return self.source_query.format(
            reads=destination.qualify(SCHEMA, self.reads)
        )

    def payload(self, destination) -> bytes:
        """The instruction a build freezes, addressed to one destination."""

        instruction = {
            "object": self.qualified(destination),
            "schema_mode": "declared" if self.declared_columns else "inferred",
            "declared_columns": self.declared_columns,
            "setup": list(self.setup),
            "source_query": self.addressed_query(destination),
            "references": [list(pair) for pair in self.references],
            "audit_columns": AUDIT_COLUMNS,
            "column_mapping": True,
        }
        return (json.dumps(instruction, indent=2, sort_keys=True) + "\n").encode("utf-8")


#: Every type a Weaver document can declare that Spark spells structurally.
#: ``DESCRIBE QUERY`` has to answer these exactly as ``dataType.simpleString()``
#: did, because whatever it says becomes the column's declared type.
COMPLEX_TYPES = TableCase(
    name="ComplexTypes",
    source_query=(
        "select cast(1 as int) as CustomerId, "
        "cast(1.50 as decimal(18,2)) as Balance, "
        "cast('2026-01-01 00:00:00' as timestamp) as SeenAt, "
        "array(named_struct('amount', cast(1.500 as decimal(9,3)))) as Lines, "
        "map('north', cast(1 as int)) as Tags "
        "where 1 = 0"
    ),
    references=(("Primary key", "CustomerId"),),
    expected={
        "CustomerId": "int",
        "Balance": "decimal(18,2)",
        "SeenAt": "timestamp",
        "Lines": "array<struct<amount:decimal(9,3)>>",
        "Tags": "map<string,int>",
    },
)

#: An authored body may register a temporary view and then select from it. The
#: setup and the describe are one piece of work: a view registered in a different
#: session is a view the query cannot see.
FROM_TEMPORARY_VIEW = TableCase(
    name="FromView",
    setup=(
        "CREATE OR REPLACE TEMPORARY VIEW weaver_case_staged AS "
        "select cast(1 as int) as CustomerId, cast('north' as string) as Region "
        "where 1 = 0",
    ),
    source_query="select CustomerId, Region from weaver_case_staged",
    references=(("Primary key", "CustomerId"),),
    expected={"CustomerId": "int", "Region": "string"},
)

#: Weaver identities are exact, and Fabric folds a table identifier to lower case
#: unless analysis is case-sensitive. The shape and the create have to share one
#: scope, or the table is created under a name the next action cannot read.
EXACT_CASE = TableCase(
    name="CustomerEnriched",
    source_query=(
        "select cast(1 as int) as CustomerId, cast('north' as string) as CustomerRegion "
        "where 1 = 0"
    ),
    references=(("Primary key", "CustomerId"),),
    expected={"CustomerId": "int", "CustomerRegion": "string"},
)

#: The view the next action builds over :data:`EXACT_CASE`, which is what proves
#: the table is readable under the name it was declared with.
EXACT_CASE_READER = f"{EXACT_CASE.name}Reader"

#: A query naming a column that is not there. It fails during the describe now
#: rather than when the query ran, and the failure has to keep saying which
#: action and what Spark objected to.
UNRESOLVED = TableCase(
    name="Unresolved",
    source_query="select NoSuchColumn from {reads}",
    reads=EXACT_CASE.name,
    references=(),
    expected={},
)

#: The cases that build a table successfully, in the order they must run: the
#: exact-case table exists before anything reads it.
BUILDING = (COMPLEX_TYPES, FROM_TEMPORARY_VIEW, EXACT_CASE)


def install_action(case: TableCase, *, action_id: str | None = None):
    """The ``spark_table`` action a build emits for one case."""

    from weaver.build_bundle.models import InstallAction

    return InstallAction(
        id=action_id or f"build-delta-{SCHEMA}.{case.name}",
        kind="build_table",
        resource_node_id=f"Lakehouse/Sales/{SCHEMA}.{case.name}",
        executor="spark_table",
        payload=f"payload/{case.name}.spark-table.json",
        payload_sha256="unused",
    )


def view_action():
    """The ``spark_sql`` action that reads the exact-case table back."""

    from weaver.build_bundle.models import InstallAction

    return InstallAction(
        id=f"object-{SCHEMA}.{EXACT_CASE_READER}",
        kind="build_view",
        resource_node_id=f"Lakehouse/Sales/{SCHEMA}.{EXACT_CASE_READER}",
        executor="spark_sql",
        payload=f"payload/{EXACT_CASE_READER}.spark.sql",
        payload_sha256="unused",
    )


def schema_action():
    """The action that makes :data:`SCHEMA`.

    Ordinary finished Spark SQL: a Fabric Lakehouse pins its own storage, so
    there is nothing for an installer to complete.
    """

    from weaver.build_bundle.models import InstallAction

    return InstallAction(
        id=f"schema-{SCHEMA}",
        kind="create_schema",
        resource_node_id=None,
        executor="spark_sql",
        payload=f"payload/{SCHEMA}.spark.sql",
        payload_sha256="unused",
    )


def schema_payload(destination) -> bytes:
    """The ``CREATE SCHEMA`` this destination needs, as the build freezes it."""

    return (destination.create_schema_statement(SCHEMA) + "\n").encode("utf-8")


def view_sql(destination) -> bytes:
    """The view over :data:`EXACT_CASE`, addressed as the build renders it."""

    return (
        f"CREATE OR REPLACE VIEW "
        f"{destination.qualify(SCHEMA, EXACT_CASE_READER)} AS\n"
        f"SELECT CustomerId, CustomerRegion FROM "
        f"{EXACT_CASE.qualified(destination)}"
    ).encode("utf-8")


def describe_queries(destination) -> dict:
    """One ``DESCRIBE`` per built table, for a single observation of the estate."""

    queries = {
        case.name: f"DESCRIBE TABLE {destination.qualify(SCHEMA, case.name)}"
        for case in BUILDING
    }
    queries[EXACT_CASE_READER] = (
        f"SELECT * FROM {destination.qualify(SCHEMA, EXACT_CASE_READER)}"
    )
    return queries


def described_types(rows) -> dict:
    """A ``DESCRIBE TABLE`` result as column name to type, in order.

    ``DESCRIBE TABLE`` pads its output with a blank row and a partition section
    on some engines, so anything without a column name ends the columns.
    """

    types = {}
    for row in rows:
        name = (row.get("col_name") or "").strip()
        if not name or name.startswith("#"):
            break
        types[name] = (row.get("data_type") or "").strip()
    return types


def assert_case_built(case: TableCase, rows) -> None:
    """The built table is exactly the query's columns, then the audit columns."""

    types = described_types(rows)
    business = [name for name in types if name not in AUDIT_NAMES]

    assert business == list(case.expected), (
        f"{case.name} carries {business}, and its query declares "
        f"{list(case.expected)}"
    )
    for name, expected in case.expected.items():
        assert types[name] == expected, (
            f"{case.name}.{name} was built as {types[name]!r}, not {expected!r}"
        )
    for name in AUDIT_NAMES:
        assert name in types, f"{case.name} is missing the audit column {name}"
