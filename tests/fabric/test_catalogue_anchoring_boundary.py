"""An anchored object, inside Fabric, and the record it keeps of itself.

The core suite proves what anchoring *decides* against a constructed catalogue.
What it cannot prove is the part that only exists in a workspace: that a name
resolves through the real ``_.Installation`` and ``_.Registry``, that a load's
bookmark reaches the catalogue Warehouse, and that the Lakehouse's own reference
to ``_.Bookmark`` resolves in Spark.

``hosted``, because the subject is the wheel installed in the session: the
object is constructed and loaded by authored code running in Fabric, the way a
developer in a notebook constructs and loads one.

One submission, one evidence payload. Every question about the anchored object
is asked in one body and answered here against what it brought back.
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

# Freestanding: it runs, and it has no place in the estate's record of itself.
free = Raw__CustomerCsv(spark, lakehouse=destination)
results["freestanding_identity"] = (
    None if free.installed is None else str(free.installed))
try:
    free.bookmark()
except LoadError as refused:
    results["freestanding_bookmark"] = str(refused)

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

# The Lakehouse's own reference to `_.Bookmark`. What the build installed here is
# a shortcut to the catalogue Warehouse's table, so this is the local reference
# rather than a table of its own — read first as storage, which says what is
# actually there, and then by the four-part name a statement would use.
from weaver.locations import Location

tables = Location(destination.spark_root) / "Tables"
results["tables"] = sorted(entry.name for entry in store.list(tables))
reference = destination.qualify("_", "Bookmark")
results["reference"] = reference
try:
    results["reference_rows"] = spark.sql(
        f"select count(*) as n from {reference}"
    ).collect()[0]["n"]
except Exception as refused:
    # Reported rather than raised, so the payload still carries what storage
    # holds — which is what says whether the reference was installed at all.
    results["reference_rows"] = None
    results["reference_error"] = str(refused).splitlines()[0]

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

    # Freestanding is a whole way to run: no identity, and no bookmark to give.
    assert seen["freestanding_identity"] is None
    assert "not anchored" in seen["freestanding_bookmark"]

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

    # And the Lakehouse's local reference is there, and resolves. What it counts
    # is the catalogue Warehouse's own table, so the number belongs to the estate
    # rather than to this test; that a count came back at all is the claim.
    assert "_" in seen["tables"], seen["tables"]
    assert seen["reference"].endswith("`_`.`Bookmark`")
    assert isinstance(seen["reference_rows"], int)
