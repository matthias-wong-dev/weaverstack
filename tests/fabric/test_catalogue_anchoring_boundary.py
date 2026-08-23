"""An anchored object, inside Fabric, and the record it keeps of itself.

The core suite proves what anchoring *decides* against a constructed catalogue.
What it cannot prove is the part that only exists in a workspace: that a name
resolves through the real ``_.Installation`` and ``_.Registry``, that a load's
bookmark reaches the catalogue Warehouse, and that the Lakehouse's own reference
to each of the catalogue's runtime tables resolves in Spark.

``hosted``, because the subject is the wheel installed in the session: the
object is constructed and loaded by authored code running in Fabric, the way a
developer in a notebook constructs and loads one.

One submission, one evidence payload — one for the anchored object's own life and
one for the reference, which needs a build of its own to exist.
"""

from __future__ import annotations

from datetime import datetime, timezone

from support.weaver_test import weaver_test

SENTINEL = datetime(1900, 1, 1, tzinfo=timezone.utc)

#: One anchored life: freestanding first, then anchored, loaded, and read back
#: through a second construction that reads the catalogue again.
ANCHORED = r"""
import sys
from datetime import datetime, timezone

from weaver import lakehouse_for
from weaver.errors import LoadError

destination = lakehouse_for(resolver, target)
root = destination.files_root() + "/_/Load"
sys.path.insert(0, root)

from Files.Raw__CustomerCsv import Raw__CustomerCsv

results = {}

# Freestanding: it can be read, and it cannot be loaded. No place in the estate's
# record of itself means no bookmark to read and none to record.
free = Raw__CustomerCsv(spark, lakehouse=destination)
results["freestanding_identity"] = (
    None if free.installed is None else str(free.installed))
try:
    free.bookmark()
except LoadError as refused:
    results["freestanding_bookmark"] = str(refused)
try:
    free.load()
except LoadError as refused:
    results["freestanding_load"] = str(refused)

# Anchored by name. The identity is resolved here, at construction, through the
# real Installation and Registry rather than from the target's name.
first = Raw__CustomerCsv(spark, lakehouse=destination, catalogue=workspace.catalogue)
results["identity"] = str(first.installed)
results["before"] = first.bookmark().isoformat()

results["load"] = first.load().as_row()
results["in_memory"] = first.bookmark().isoformat()

# A second anchored construction reads the catalogue again, so what it answers
# is what the Warehouse holds rather than what the first one remembers.
second = Raw__CustomerCsv(spark, lakehouse=destination, catalogue=workspace.catalogue)
results["persisted"] = second.bookmark().isoformat()

# And the rest of what the standalone load recorded, read back the same way.
from weaver.catalogue.state import catalogue_in
from weaver.catalogue.tables import LOAD_STATISTIC, LOAD_STATUS

def _mine(rows):
    return [row for row in rows if row.get("object_name") == "CustomerCsv"]

with catalogue_in(workspace, tables=(LOAD_STATUS, LOAD_STATISTIC)) as recorded:
    results["status"] = [
        str(row.get("result")) for row in _mine(recorded.table_rows(LOAD_STATUS))
    ]
    results["statistics"] = [
        [int(row.get("rows_read") or 0), bool(row.get("is_static_skip"))]
        for row in _mine(recorded.table_rows(LOAD_STATISTIC))
    ]

emit(results)
"""


@weaver_test(hosted=True)
def test_an_anchored_object_resolves_and_records_itself_in_fabric(
    fabric_lakehouse_estate,
):
    """Construction resolves the identity; a clean load moves the row.

    The two constructions are what makes the second half a claim about the
    Warehouse: an in-memory row would satisfy the first object and tell nothing
    about what landed.
    """

    seen = fabric_lakehouse_estate.env.run_python(ANCHORED, label="anchor and load")

    # Freestanding: no identity, no bookmark to give, and no load.
    assert seen["freestanding_identity"] is None
    assert "cannot read its bookmark or record one" in seen["freestanding_bookmark"]
    assert "cannot read its bookmark or record one" in seen["freestanding_load"]

    # Anchored, and the identity is the Registry's — the *item* that declared the
    # folder, under its files identity, rather than the Lakehouse it was built into.
    assert seen["identity"].endswith("/Files/Raw.CustomerCsv")

    assert seen["load"]["succeeded"] is True
    began = datetime.fromisoformat(seen["load"]["bookmark_datetime"])
    assert began > SENTINEL

    # The instant the load reported, held in memory and then read back from the
    # catalogue Warehouse by an object that was constructed after it landed.
    assert datetime.fromisoformat(seen["in_memory"]) == began
    assert datetime.fromisoformat(seen["persisted"]) == began

    # And the rest of the operational record the standalone interface wrote. A
    # developer who ran this by hand can read what it did from the estate.
    assert seen["status"] == ["Succeeded"]
    assert seen["statistics"] and seen["statistics"][0][1] is False


#: What the Lakehouse holds under ``Tables/_``, and whether each reference
#: resolves. One body, because the references are one decision: the shortcuts are
#: created by one action, and a table missing from it is a gap in that action.
REFERENCE = r"""
from weaver import lakehouse_for
from weaver.catalogue.tables import PRESENTED_RUNTIME_TABLES
from weaver.locations import Location

destination = lakehouse_for(resolver, target)
root = Location(destination.spark_root)
results = {}
results["tables"] = sorted(entry.name for entry in store.list(root / "Tables"))
results["runtime"] = sorted(
    entry.name for entry in store.list(root / "Tables" / "_")
)
results["resolved"] = {}
for table in PRESENTED_RUNTIME_TABLES:
    reference = destination.qualify("_", table.name)
    counted = spark.sql(f"select count(*) as n from {reference}").collect()[0]["n"]
    results["resolved"][table.name] = [reference, counted]
emit(results)
"""


@weaver_test(hosted=True)
def test_one_build_installs_the_lakehouse_references_and_the_next_plans_none(
    fabric_lakehouse_estate,
):
    """One pass, and the shortcuts it ends with.

    This estate's own build created the catalogue tables and pointed at them —
    the item graph orders the two, and the source wait carries the moment between
    Fabric creating a Warehouse table and publishing it to OneLake. So the build
    here has nothing left to do, and Spark reads each runtime table in the
    Lakehouse by the four-part name a statement would use.
    """

    from weaver.catalogue.tables import PRESENTED_RUNTIME_TABLES

    env = fabric_lakehouse_estate.env

    bundle = env.generate("reference")
    planned = [
        action.id
        for _sequence, _batch, action in bundle.plan.actions()
        if "runtime-reference" in action.id
    ]
    outcome = env.install(bundle)
    assert outcome.status == "succeeded", outcome.action_error
    assert planned == [], "the references were installed by the build that made them"

    seen = env.run_python(REFERENCE, label="read the runtime references")

    # Shortcuts under `Tables/_`, which is Weaver's own rather than the item's.
    assert "_" in seen["tables"], seen["tables"]
    assert {table.name for table in PRESENTED_RUNTIME_TABLES} <= set(seen["runtime"])
    for table in PRESENTED_RUNTIME_TABLES:
        reference, counted = seen["resolved"][table.name]
        assert reference.endswith(f"`_`.`{table.name}`")
        # What it counts is the catalogue Warehouse's own table, so the number
        # belongs to the estate rather than to this test; that a count came back
        # through the shortcut at all is the claim.
        assert isinstance(counted, int)
