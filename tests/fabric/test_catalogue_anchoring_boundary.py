"""An anchored object, inside Fabric, and the record it keeps of itself.

The core suite proves what anchoring *decides* against a constructed catalogue.
What it cannot prove is the part that only exists in a workspace: that a name
resolves through the real ``_.Installation`` and ``_.Registry``, that a load's
bookmark reaches the catalogue Warehouse, and that the Lakehouse's own reference
to ``_.Bookmark`` resolves in Spark.

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


#: What the Lakehouse holds under ``Tables``, and whether the reference resolves.
REFERENCE = r"""
from weaver import lakehouse_for
from weaver.locations import Location

destination = lakehouse_for(resolver, target)
results = {}
results["tables"] = sorted(
    entry.name for entry in store.list(Location(destination.spark_root) / "Tables")
)
reference = destination.qualify("_", "Bookmark")
results["reference"] = reference
results["rows"] = spark.sql(f"select count(*) as n from {reference}").collect()[0]["n"]
emit(results)
"""


@weaver_test(hosted=True)
def test_the_build_after_the_catalogue_installs_the_lakehouse_reference(
    fabric_lakehouse_estate,
):
    """The documented two-build behaviour, and the shortcut it ends with.

    A Warehouse publishes a table to OneLake after creating it, so the build that
    creates the catalogue has nothing for a shortcut to point at. This estate's
    first build was that build. The one here is the next, which finds the table
    installed and gives the Lakehouse its reference — and then Spark reads
    ``_.Bookmark`` in the Lakehouse by the four-part name a statement would use.
    """

    env = fabric_lakehouse_estate.env

    bundle = env.generate("reference")
    installed = [
        action.id
        for _sequence, _batch, action in bundle.plan.actions()
        if "bookmark-reference" in action.id
    ]
    outcome = env.install(bundle)
    assert outcome.status == "succeeded", outcome.action_error
    assert installed, "the build after the catalogue plans the reference"

    seen = env.run_python(REFERENCE, label="read the bookmark reference")

    # A shortcut under `Tables/_`, which is Weaver's own rather than the item's.
    assert "_" in seen["tables"], seen["tables"]
    assert seen["reference"].endswith("`_`.`Bookmark`")
    # What it counts is the catalogue Warehouse's own table, so the number
    # belongs to the estate rather than to this test; that a count came back
    # through the shortcut at all is the claim.
    assert isinstance(seen["rows"], int)
