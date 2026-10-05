"""Delta loads that meet Fabric Spark 4.1's staging defects, and load anyway.

The claim behind the workarounds in ``weaver.runtime.table_load``. Remove a
workaround once Fabric fixes its defect, and this test must still pass.

- SPARK-59009. ``spark.range`` and ``orderBy`` give staging an ordering, and
  reject discovery's CTE chain references cached staging more than once.
- A CREATE TABLE AS SELECT that reads cached staging directly fails ("Heavy
  batch should consist of arrow vectors"), whatever the source: the evidence a
  rejected row leaves, and a full replace.
- ``spark.sql.optimizer.cte.cache.enabled`` keeps the CTEs the purge repeats
  from reject discovery, and no load may leave them behind.

The frames are what a Table's ``read()`` returns; nothing else about the
object matters here. One submission. ``full_integration``, as the other real
Spark reconciliation is.
"""

from __future__ import annotations

from support.weaver_test import weaver_test

#: Owned by this body and dropped when it ends.
SCHEMA = "FabricStaging"
KEYED = "Order"
WHOLE = "OrderSnapshot"

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

WHOLE_HEADER = """Table ID: {schema}.{object}

Description: Orders, replaced on every load.

Lineage: The pytest suite writes it.

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


def contract(header, name):
    return LoadContract.from_document(
        parse_document(header.format(schema=SCHEMA, object=name), language=PYTHON)
    )


keyed = contract(HEADER, KEYED)
whole = contract(WHOLE_HEADER, WHOLE)
WORKING = ("_Staging", "_Reject", "_Delete")


def qualified(name):
    return destination.qualify(SCHEMA, name)


def arrange():
    spark.sql(destination.destination.create_schema_statement(SCHEMA))
    names = [name + suffix for name in (KEYED, WHOLE) for suffix in ("", *WORKING)]
    for name in (*names, "Source"):
        spark.sql(f"DROP TABLE IF EXISTS {qualified(name)}")
    audit = ", ".join(f"`{name}` timestamp NOT NULL" for name in delta_audit_names())
    spark.sql(
        f"CREATE TABLE {qualified(KEYED)} (`Order id` bigint, `Revision` int, "
        f"{audit}, `{delta_signature_name()}` string NOT NULL) "
        f"USING delta {COLUMN_MAPPING}"
    )
    spark.sql(
        f"CREATE TABLE {qualified(WHOLE)} (`Order id` bigint, `Revision` int, "
        f"{audit}) USING delta {COLUMN_MAPPING}"
    )
    spark.sql(
        f"CREATE TABLE {qualified('Source')} (`Order id` bigint, `Revision` int) "
        f"USING delta {COLUMN_MAPPING}"
    )
    spark.sql(
        f"INSERT INTO {qualified('Source')} VALUES "
        "(2, 2), (3, 2), (4, 2), (5, 2), (7, 2), (8, NULL)"
    )


def persistent_rdds():
    """Persisted RDDs by id, without Delta's own snapshot cache."""

    rdds = spark.sparkContext._jsc.getPersistentRDDs()
    named = {int(key): str(rdds[key].name()) for key in rdds.keys()}
    return {
        key: name
        for key, name in named.items()
        if not name.startswith("Delta Table State")
    }


def held_views():
    return sorted(
        view.name
        for view in spark.catalog.listTables()
        if view.isTemporary and view.name.startswith("weaver_")
    )


def evidence(name):
    tables = {
        row["tableName"].lower()
        for row in spark.sql(
            "SHOW TABLES IN " + destination.destination.qualified_schema(SCHEMA)
        ).collect()
    }
    return sorted(suffix for suffix in WORKING if (name + suffix).lower() in tables)


def run(load_contract, name, frame, fault_tolerant=False):
    """One load, what it left in the table, and whatever it left held in Spark."""

    before = persistent_rdds()
    try:
        outcome = {
            "result": load_table(
                spark,
                contract=load_contract,
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
    outcome["contents"] = [
        [row["id"], row["Revision"]]
        for row in spark.sql(
            f"SELECT `Order id` AS id, Revision FROM {qualified(name)} ORDER BY id"
        ).collect()
    ]
    outcome["evidence"] = evidence(name)
    return outcome


seen = {}
try:
    arrange()

    seen["range"] = run(
        keyed,
        KEYED,
        spark.range(0, 5).selectExpr("id AS `Order id`", "0 AS Revision"),
    )

    # 0 and 1 leave, 2 to 4 change, 5 arrives, and 6 is rejected for its null.
    seen["sorted"] = run(
        keyed,
        KEYED,
        spark.range(2, 7)
        .selectExpr(
            "id AS `Order id`", "CASE WHEN id = 6 THEN NULL ELSE 1 END AS Revision"
        )
        .orderBy(F.desc("Order id")),
        fault_tolerant=True,
    )

    # 2 to 5 change, 7 arrives, and 8 is rejected for its null.
    seen["delta"] = run(keyed, KEYED, spark.table(qualified("Source")), True)

    seen["replace"] = run(
        whole,
        WHOLE,
        spark.createDataFrame([(1, 1), (2, 1)], "`Order id` bigint, Revision int"),
    )
finally:
    spark.sql(
        "DROP SCHEMA IF EXISTS "
        + destination.destination.qualified_schema(SCHEMA)
        + " CASCADE"
    )

emit(seen)
'''


@weaver_test(integration=True)
def test_staging_loads_past_fabric_spark_defects(
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
        f"KEYED = {KEYED!r}\n"
        f"WHOLE = {WHOLE!r}\n"
        f"HEADER = {HEADER!r}\n"
        f"WHOLE_HEADER = {WHOLE_HEADER!r}\n"
    )

    seen = livy_session.run(preamble + BODY).payload

    loaded = seen["range"]
    assert "raised" not in loaded, loaded["raised"]
    assert loaded["result"]["rows_inserted"] == 5
    assert loaded["contents"] == [[0, 0], [1, 0], [2, 0], [3, 0], [4, 0]]

    # An ordered source through reject discovery, with its evidence written.
    changed = seen["sorted"]
    assert "raised" not in changed, changed["raised"]
    assert changed["result"]["rows_read"] == 5
    assert changed["result"]["rows_rejected"] == 1
    assert changed["result"]["rows_inserted"] == 1
    assert changed["result"]["rows_updated"] == 3
    assert changed["result"]["rows_deleted"] == 2
    assert changed["contents"] == [[2, 1], [3, 1], [4, 1], [5, 1]]
    assert changed["evidence"] == ["_Delete", "_Reject", "_Staging"]

    # A Delta source writing the same evidence.
    read = seen["delta"]
    assert "raised" not in read, read["raised"]
    assert read["result"]["rows_read"] == 6
    assert read["result"]["rows_rejected"] == 1
    assert read["result"]["rows_inserted"] == 1
    assert read["result"]["rows_updated"] == 4
    assert read["contents"] == [[2, 2], [3, 2], [4, 2], [5, 2], [7, 2]]
    assert read["evidence"] == ["_Reject", "_Staging"]

    # A full replace, whose staging copy is written before the target empties.
    replaced = seen["replace"]
    assert "raised" not in replaced, replaced["raised"]
    assert replaced["result"]["rows_inserted"] == 2
    assert replaced["contents"] == [[1, 1], [2, 1]]
    assert replaced["evidence"] == []

    # Every relation each load materialised was given back, and no CTE cache
    # outlived a load that rejected rows.
    for outcome in seen.values():
        assert outcome["leaked_rdds"] == []
        assert outcome["held_views"] == []
