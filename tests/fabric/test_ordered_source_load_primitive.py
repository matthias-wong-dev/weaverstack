"""A Delta keyed load whose staging frame reports an output ordering.

``spark.range`` and ``orderBy`` give a frame an ordering that Spark carries
through an aliasing projection. Fabric's Spark 4.1 cannot canonicalise a cached
relation with an ordering once an inlined CTE references it twice
(SPARK-59009), which is what reject discovery does over staging. Fabric also
fails writes that read cached staging directly, such as the evidence a rejected
row leaves, whatever the source. The load must still settle every phase, and
give back everything it held.

The frames are what a Table's ``read()`` returns; nothing else about the
object matters here. One submission: a range-sourced load, a sorted range with a
rejected row that changes, inserts and deletes, then a Delta table with a
rejected row. ``full_integration``, as the other real Spark reconciliation is.
"""

from __future__ import annotations

from support.weaver_test import weaver_test

#: Owned by this body and dropped when it ends.
SCHEMA = "OrderedSource"
OBJECT = "Order"

HEADER = """Table ID: {schema}.{object}

Description: Orders.

Lineage: The pytest suite writes it.

Primary key: Order id

Not null:
  - Revision

Schema:
  Order id: bigint
  Revision: int
"""

BODY = r'''
from pyspark.sql import functions as F

from weaver import lakehouse_for
from weaver.declaration.metadata import PYTHON, parse_document
from weaver.runtime.delta_sql import (
    COLUMN_MAPPING,
    delta_audit_names,
    delta_signature_name,
)
from weaver.runtime.load_contract import LoadContract
from weaver.runtime.table_load import load_table

destination = lakehouse_for(resolver, target)
contract = LoadContract.from_document(
    parse_document(HEADER.format(schema=SCHEMA, object=OBJECT), language=PYTHON)
)
WORKING = ("_Staging", "_Reject", "_Delete")


def qualified(suffix=""):
    return destination.qualify(SCHEMA, OBJECT + suffix)


def arrange():
    spark.sql(destination.destination.create_schema_statement(SCHEMA))
    for suffix in (*WORKING, "Source", ""):
        spark.sql(f"DROP TABLE IF EXISTS {qualified(suffix)}")
    audit = ", ".join(f"`{name}` timestamp NOT NULL" for name in delta_audit_names())
    spark.sql(
        f"CREATE TABLE {qualified()} (`Order id` bigint, `Revision` int, {audit}, "
        f"`{delta_signature_name()}` string NOT NULL) USING delta {COLUMN_MAPPING}"
    )
    spark.sql(
        f"CREATE TABLE {qualified('Source')} (`Order id` bigint, `Revision` int) "
        f"USING delta {COLUMN_MAPPING}"
    )
    spark.sql(
        f"INSERT INTO {qualified('Source')} VALUES "
        "(2, 2), (3, 2), (4, 2), (5, 2), (7, 2), (8, NULL)"
    )


#: Caches the engine keeps for itself: Delta's snapshots, and the CTE results
#: Fabric's ``spark.sql.optimizer.cte.cache.enabled`` retains after a query.
ENGINE_CACHES = ("Delta Table State", "In-memory table cte")


def persistent_rdds():
    """Persisted RDDs by id, without the engine's own caches."""

    rdds = spark.sparkContext._jsc.getPersistentRDDs()
    named = {int(key): str(rdds[key].name()) for key in rdds.keys()}
    return {
        key: name
        for key, name in named.items()
        if not name.startswith(ENGINE_CACHES)
    }


def held_views():
    return sorted(
        view.name
        for view in spark.catalog.listTables()
        if view.isTemporary and view.name.startswith("weaver_")
    )


def run(frame, fault_tolerant):
    """One load, and whatever it left held in Spark afterwards."""

    before = persistent_rdds()
    try:
        outcome = {
            "result": load_table(
                spark,
                contract=contract,
                lakehouse=destination,
                staging_frame=frame,
                fault_tolerant=fault_tolerant,
            ).as_row()
        }
    except Exception as raised:
        outcome = {"raised": f"{type(raised).__name__}: {str(raised)[:400]}"}
    after = persistent_rdds()
    outcome["leaked_rdds"] = sorted(after[key] for key in after.keys() - before.keys())
    outcome["held_views"] = held_views()
    return outcome


def contents():
    rows = spark.sql(
        f"SELECT `Order id` AS id, Revision FROM {qualified()} ORDER BY id"
    ).collect()
    return [[row["id"], row["Revision"]] for row in rows]


seen = {}
try:
    arrange()

    seen["range"] = run(
        spark.range(0, 5).selectExpr("id AS `Order id`", "0 AS Revision"),
        fault_tolerant=False,
    )
    seen["range_contents"] = contents()

    # 0 and 1 leave, 2 to 4 change, 5 arrives, and 6 is rejected for its null.
    seen["sorted"] = run(
        spark.range(2, 7)
        .selectExpr(
            "id AS `Order id`", "CASE WHEN id = 6 THEN NULL ELSE 1 END AS Revision"
        )
        .orderBy(F.desc("Order id")),
        fault_tolerant=True,
    )
    seen["sorted_contents"] = contents()

    # 2 to 5 change, 7 arrives, and 8 is rejected for its null.
    seen["delta"] = run(spark.table(qualified("Source")), fault_tolerant=True)
    seen["delta_contents"] = contents()
finally:
    spark.sql(
        "DROP SCHEMA IF EXISTS "
        + destination.destination.qualified_schema(SCHEMA)
        + " CASCADE"
    )

emit(seen)
'''


@weaver_test(integration=True)
def test_a_delta_load_settles_a_staging_frame_that_reports_an_ordering(
    livy_session, fabric_workspace, fabric_target_lakehouse
):
    preamble = (
        "from weaver.workspaces import Workspace\n"
        "from weaver.targets import ItemRef\n"
        "from weaver.resolution import resolver_for\n"
        f"workspace = Workspace(workspace={fabric_workspace.workspace!r}, "
        f"catalogue={fabric_workspace.catalogue!r}, "
        f"environment={str(fabric_workspace.environment)!r})\n"
        "resolver = resolver_for(workspace)\n"
        f"target = ItemRef({fabric_target_lakehouse.name!r})\n"
        f"SCHEMA = {SCHEMA!r}\n"
        f"OBJECT = {OBJECT!r}\n"
        f"HEADER = {HEADER!r}\n"
    )

    seen = livy_session.run(preamble + BODY).payload

    loaded = seen["range"]
    assert "raised" not in loaded, loaded["raised"]
    assert loaded["result"]["rows_read"] == 5
    assert loaded["result"]["rows_inserted"] == 5
    assert seen["range_contents"] == [[0, 0], [1, 0], [2, 0], [3, 0], [4, 0]]

    changed = seen["sorted"]
    assert "raised" not in changed, changed["raised"]
    assert changed["result"]["rows_read"] == 5
    assert changed["result"]["rows_rejected"] == 1
    assert changed["result"]["rows_inserted"] == 1
    assert changed["result"]["rows_updated"] == 3
    assert changed["result"]["rows_deleted"] == 2
    assert seen["sorted_contents"] == [[2, 1], [3, 1], [4, 1], [5, 1]]

    read = seen["delta"]
    assert "raised" not in read, read["raised"]
    assert read["result"]["rows_read"] == 6
    assert read["result"]["rows_rejected"] == 1
    assert read["result"]["rows_inserted"] == 1
    assert read["result"]["rows_updated"] == 4
    assert seen["delta_contents"] == [[2, 2], [3, 2], [4, 2], [5, 2], [7, 2]]

    # Every relation each load materialised was given back.
    for outcome in (loaded, changed, read):
        assert outcome["leaked_rdds"] == []
        assert outcome["held_views"] == []
