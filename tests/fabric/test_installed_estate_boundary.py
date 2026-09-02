"""Weaver installed in Fabric, against one built Lakehouse estate.

Two subjects that each need the wheel running in the session and an estate a
bundle installed, and they share one. Building it costs about a minute, and it
used to be built three times, once per module.

**Anchoring.** A name resolves through the real ``_.Installation`` and
``_.Registry``, a load's bookmark reaches the catalogue Warehouse, and the
Lakehouse's own reference to each catalogue runtime table resolves in Spark. The
core suite proves what anchoring decides against a constructed catalogue; this is
the part that exists only in a workspace.

**The developer-facing load API.** Someone in a Fabric session can import a
deployed object and run its load, with no planner, no catalogue orchestration and
no estate-level entry point in the way. The subject is what the installed wheel
offers, rather than the load semantics underneath it, which the core suite proves
for a fraction of the cost.

Order matters here in the way it always did inside a module. The sections run in
file order against one estate, and neither asserts on a starting state:
anchoring builds twice and compares, and the developer primitives assert on what
a primitive returns.

Composing a run out of the catalogue graph is the acceptance journey's, and the
scope and dispatch counts underneath it are the core suite's.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest
from support.weaver_test import weaver_test

# --- Anchoring -----------------------------------------------------------------


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

# A validation, anchored and run the same way. Its identity comes from
# `_.TestDictionary` rather than Registry, because it materialises nothing.
#
# Loaded by path: a compiled validation lands under `tests/` in the deployed
# tree, so the import root alone does not name it, and `tests` is not a package
# name worth claiming inside a Spark driver. What it imports in turn, the object
# modules it was authored against, resolves through the root, which is on the
# path already.
import importlib.util

_spec = importlib.util.spec_from_file_location(
    "DWG__OrderAmounts", root + "/tests/DWG__OrderAmounts.py")
_module = importlib.util.module_from_spec(_spec)
# Registered before it is executed, because a Weaver object reads its own
# contract from the module it was defined in: sys.modules is where it looks.
sys.modules[_spec.name] = _module
_spec.loader.exec_module(_module)
DWG__OrderAmounts = _module.DWG__OrderAmounts

free_test = DWG__OrderAmounts(spark, lakehouse=destination)
results["validation_freestanding"] = (
    None if free_test.installed is None else str(free_test.installed))
try:
    free_test.run()
except LoadError as refused:
    results["validation_refused"] = str(refused)

checked = DWG__OrderAmounts(
    spark, lakehouse=destination, catalogue=workspace.catalogue)
results["validation_identity"] = str(checked.installed)
results["validation_result"] = checked.run().to_mapping()

# And the rest of what the standalone calls recorded, read back the same way.
from weaver.catalogue.state import catalogue_in
from weaver.catalogue.tables import LOAD_STATISTIC, LOAD_STATUS, TEST_STATUS

def _named(rows, wanted):
    return [row for row in rows if row.get("object_name") == wanted]

with catalogue_in(
    workspace, tables=(LOAD_STATUS, LOAD_STATISTIC, TEST_STATUS)
) as recorded:
    results["status"] = [
        str(row.get("result"))
        for row in _named(recorded.table_rows(LOAD_STATUS), "CustomerCsv")
    ]
    results["statistics"] = [
        [int(row.get("rows_read") or 0), bool(row.get("is_static_skip"))]
        for row in _named(recorded.table_rows(LOAD_STATISTIC), "CustomerCsv")
    ]
    results["validation_status"] = [
        [str(row.get("result")), str(row.get("test_type")),
         int(row.get("failure_count") or 0)]
        for row in _named(recorded.table_rows(TEST_STATUS), "OrderAmounts")
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

    # Anchored, and the identity is the Registry's, the item that declared the
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
    #
    # Internal values, because this read the rows back through a `Catalogue`:
    # the public sentence-case spellings exist at the persistence boundary and
    # nothing above it sees them. A reader selecting from the view gets
    # `Succeeded`. See ``tests/fabric/test_warehouse_load_primitive.py``.
    assert seen["status"] == ["succeeded"]
    assert seen["statistics"] and seen["statistics"][0][1] is False

    # A validation divides the same way: freestanding it has no identity and no
    # operational interface, anchored it resolves and records what it found.
    assert seen["validation_freestanding"] is None
    assert "not anchored" in seen["validation_refused"]
    assert seen["validation_identity"].endswith("/DWG.OrderAmounts")
    assert seen["validation_result"]["failure_count"] == 0
    assert seen["validation_status"] == [["succeeded", "test", 0]]


#: What the Lakehouse holds under ``Tables/_``, and whether each reference
#: resolves. One body, because the references are one decision: the shortcuts are
#: created by one action, and a table missing from it is a gap in that action.
REFERENCE = r"""
from weaver import lakehouse_for
from weaver.catalogue.tables import STANDARD_SURFACE_TABLES
from weaver.locations import Location

destination = lakehouse_for(resolver, target)
root = Location(destination.spark_root)
results = {}
results["tables"] = sorted(entry.name for entry in store.list(root / "Tables"))
results["runtime"] = sorted(
    entry.name for entry in store.list(root / "Tables" / "_")
)
results["resolved"] = {}
for table in STANDARD_SURFACE_TABLES:
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

    This estate's own build created the catalogue tables and pointed at them,
    the item graph orders the two, and the source wait carries the moment between
    Fabric creating a Warehouse table and publishing it to OneLake. So the build
    here has nothing left to do, and Spark reads each runtime table in the
    Lakehouse by the four-part name a statement would use.
    """

    from weaver.catalogue.tables import STANDARD_SURFACE_TABLES

    env = fabric_lakehouse_estate.env

    bundle = env.generate("reference")
    planned = [
        action.id
        for _sequence, _batch, action in bundle.plan.actions()
        if action.kind == "create_shortcut"
    ]
    outcome = env.install(bundle)
    assert outcome.status == "succeeded", outcome.action_error
    assert planned == [], "the references were installed by the build that made them"

    seen = env.run_python(REFERENCE, label="read the runtime references")

    # Shortcuts under `Tables/_`, which is Weaver's own rather than the item's.
    assert "_" in seen["tables"], seen["tables"]
    assert {table.name for table in STANDARD_SURFACE_TABLES} <= set(seen["runtime"])
    for table in STANDARD_SURFACE_TABLES:
        reference, counted = seen["resolved"][table.name]
        assert reference.endswith(f"`_`.`{table.name}`")
        # What it counts is the catalogue Warehouse's own table, so the number
        # belongs to the estate rather than to this test; that a count came back
        # through the shortcut at all is the claim.
        assert isinstance(counted, int)


# --- The developer-facing load API ---------------------------------------------


#: Everything asked of the installed estate, in one round trip. It imports from
#: the deployed tree the way the deployed tree is meant to be imported, loads the
#: folder through the resolved OneLake path, and runs the generated Spark SQL
#: program from the file the installer wrote.
BODY = r"""
import os, sys
from pathlib import Path

from weaver import lakehouse_for

# The body's `target` is an ItemRef, a name, not a destination. Resolving it is
# the orchestrator's move, and the one an object never makes for itself.
destination = lakehouse_for(resolver, target)
results = {}

# The deployed runtime tree, reached as Python must reach it: through the mount
# Weaver makes of the root it resolved, not through /lakehouse/default, which
# names only whatever a notebook happened to attach.
root = destination.files_root() + "/_/Load"
sys.path.insert(0, root)
results["deployed"] = sorted(os.listdir(root))
results["tables"] = sorted(os.listdir(os.path.join(root, "Tables")))

# The import a deployed tree is laid out for: `Files.*` and `Tables.*`, because
# the authored path is reproduced verbatim beneath the root.
# `Files/Raw__CustomerCsv.py` stays where it was written, so the module name says
# the same thing it did.
from Files.Raw__CustomerCsv import Raw__CustomerCsv

results["imported"] = Raw__CustomerCsv.__name__

# What the folder reads is deployed, not placed here: `lib/` travels whole, so
# a helper module's data file arrives beside the module that reads it.
results["lib"] = sorted(os.listdir(os.path.join(root, "lib", "data")))

# The folder load, writing ordinary files to OneLake through the mount. The
# catalogue is named because a load records how far it got, and the workspace is
# what says where it lives. Nothing infers it.
export = Raw__CustomerCsv(spark, lakehouse=destination, catalogue=workspace.catalogue)
results["folder"] = export.load().as_row()
# Two spellings of one location, and only one of them is a filesystem path.
results["folder_path_is_mounted"] = not str(export.path()).startswith("abfss://")
results["folder_path_is_a_path"] = isinstance(export.path(), Path)
results["spark_path_is_abfss"] = export.spark_path().startswith("abfss://")
results["published"] = sorted(p.name for p in export.path().iterdir())

# The SQL-authored table, which is a deployed Python module like every other:
# `Tables/DWG.NamedCustomer.sql` was compiled into `Tables/DWG__NamedCustomer.py`,
# so it imports, constructs and loads exactly as the hand-written ones do.
from Tables.DWG__NamedCustomer import DWG__NamedCustomer

results["sql_authored_module"] = DWG__NamedCustomer.__name__
results["sql_authored_is_generated"] = (
    sys.modules[DWG__NamedCustomer.__module__].__doc__ or ""
).lstrip().startswith("Table ID: DWG.NamedCustomer")
results["sql_authored_load"] = (
    DWG__NamedCustomer(spark, lakehouse=destination, catalogue=workspace.catalogue)
    .load()
    .as_row()
)

emit(results)
"""


#: A catalogue for an ad-hoc probe, built in the session that runs it.
#:
#: A load needs a catalogue, because it reads its bookmark. A probe is not part
#: of the built estate, so the estate's own catalogue does not record it. This is
#: the real :class:`~weaver.catalogue.state.Catalogue`, over the rows that make
#: one object installed. It has nowhere to write and needs nowhere: the probes
#: call ``_load()``, the interface that records nothing.
PROBE_CATALOGUE = r"""
from weaver.catalogue.state import Catalogue
from weaver.declaration.model import WeaverItemId


def probe_catalogue(schema, name, *, target):
    item = WeaverItemId("Lakehouse", "Probe")
    scope = {"item_type": item.item_type, "item_name": item.item_name}
    return Catalogue({item: {
        "Installation": ({**scope, "target_name": target,
                         "weaver_version": "0", "signature": "s"},),
        "Registry": ({**scope, "schema_name": "Files/" + schema,
                      "object_name": name, "object_type": "folder",
                      "object_role": "data", "signature": "s",
                      "build_datetime": None},),
    }})
"""


#: A real Folder state transition and its downstream consumer, in the same
#: Fabric session. The values brought back are evidence captured by authored
#: Python; the test process never reads ``_changes`` itself.
CHANGE_FEED = r"""
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path

from weaver import Folder, lakehouse_for
from weaver.declaration.metadata import PYTHON, parse_document
from weaver.runtime.folder_load import _with_retry

destination = lakehouse_for(resolver, target)


class Raw__ChangeFeedProbe(Folder):
    files = {}
    deletes = ()

    def _document(self):
        return parse_document('''
Folder ID: Raw.ChangeFeedProbe

Description: Fabric change-feed probe.

Lineage: Controlled test files.

File key: "*.csv"

Incremental: true
'''.strip(), language=PYTHON)

    def read(self):
        staging = self.staging_folder()
        for name, content in type(self).files.items():
            (staging.path / name).write_text(content, encoding="utf-8")
        return staging, type(self).deletes


# Anchored to a catalogue this body builds, because `load()` needs one. Nothing
# is recorded: the load is told the caller owns that, and here nobody does.
folder = Raw__ChangeFeedProbe(spark, lakehouse=destination).with_catalogue(
    probe_catalogue("Raw", "ChangeFeedProbe", target=destination.name)
)
reject = folder.path().with_name(folder.path().name + "_Reject")


def clear(path):
    _with_retry(lambda: shutil.rmtree(path) if path.exists() else None)


for path in (folder.path(), folder._staging_path(), reject):
    clear(path)

try:
    # Seed the prior physical state as setup. The one Weaver transition below
    # then classifies an insert, update and delete in one commit and produces one
    # evidence payload.
    folder.path().mkdir(parents=True, exist_ok=True)
    (folder.path() / "updated.csv").write_text("old", encoding="utf-8")
    (folder.path() / "deleted.csv").write_text("delete me", encoding="utf-8")

    # The seeded state is files Weaver never saw arrive, so managed history is
    # empty even though the Folder holds files its File key claims. The load
    # below adopts them first, which is what gives the Folder a history to read
    # incrementally from.
    unrecorded = Raw__ChangeFeedProbe(folder).latest_files()

    bookmark = datetime.now(timezone.utc)
    Raw__ChangeFeedProbe.files = {
        "updated.csv": "updated",
        "inserted.csv": "inserted",
    }
    Raw__ChangeFeedProbe.deletes = ("deleted.csv",)
    result = folder._load()

    # Constructed from another authored object, which is the public downstream
    # spelling: My__Folder(self).files_since(self.bookmark()).
    consumer = Raw__ChangeFeedProbe(folder)
    changed = consumer.files_since(bookmark)
    latest = consumer.latest_files()
    deleted = consumer.deleted_since(bookmark)
    documents = sorted((folder.path() / "_changes").glob("*.json"))
    boundary = datetime.strptime(
        documents[-1].stem, "%Y-%m-%dT%H-%M-%S.%fZ"
    ).replace(tzinfo=timezone.utc)

    emit({
        "result": result.as_row(),
        "unrecorded": [str(path) for path in unrecorded],
        "changed": {str(path): at.isoformat() for path, at in changed.items()},
        "latest": {str(path): at.isoformat() for path, at in latest.items()},
        "deleted": {str(path): at.isoformat() for path, at in deleted.items()},
        "keys_are_full_paths": all(
            isinstance(path, Path) and path.is_absolute()
            for path in (*changed, *latest, *deleted)
        ),
        "values_are_utc": all(
            at.utcoffset() == timedelta(0)
            for at in (*changed.values(), *latest.values(), *deleted.values())
        ),
        "contents": {path.name: path.read_text(encoding="utf-8")
                     for path in changed},
        "latest_contents": {path.name: path.read_text(encoding="utf-8")
                            for path in latest},
        "deleted_exists": [path.exists() for path in deleted],
        "change_documents": [path.name for path in documents],
        "committed_at": boundary.isoformat(),
        "strict_changed": [str(path) for path in consumer.files_since(boundary)],
        "strict_deleted": [str(path) for path in consumer.deleted_since(boundary)],
    })
finally:
    for path in (folder.path(), folder._staging_path(), reject):
        clear(path)
"""


#: One File-key outcome. The caller binds ``FAULT_TOLERANT`` so the two tests
#: exercise separate transitions and receive separate evidence payloads.
FILE_KEY_REJECTION = r"""
import shutil
from datetime import datetime, timezone

from weaver import Folder, lakehouse_for
from weaver.declaration.metadata import PYTHON, parse_document
from weaver.errors import LoadError
from weaver.runtime.folder_load import _with_retry

destination = lakehouse_for(resolver, target)


class Raw__FileKeyProbe(Folder):
    def _document(self):
        return parse_document('''
Folder ID: Raw.FileKeyProbe

Description: Fabric File-key probe.

Lineage: Controlled test files.

File key: "*.csv"
'''.strip(), language=PYTHON)

    def read(self):
        staging = self.staging_folder()
        (staging.path / "good.csv").write_text("good", encoding="utf-8")
        (staging.path / "bad.txt").write_text("bad", encoding="utf-8")
        return staging, []


# Anchored as above, and recording nothing.
folder = Raw__FileKeyProbe(spark, lakehouse=destination).with_catalogue(
    probe_catalogue("Raw", "FileKeyProbe", target=destination.name)
)
reject = folder.path().with_name(folder.path().name + "_Reject")


def clear(path):
    _with_retry(lambda: shutil.rmtree(path) if path.exists() else None)


for path in (folder.path(), folder._staging_path(), reject):
    clear(path)

try:
    bookmark = datetime.now(timezone.utc)
    raised = False
    result = None
    try:
        result = folder._load(fault_tolerant=FAULT_TOLERANT)
    except LoadError as exc:
        raised = True
        result = exc.result
    changes = Raw__FileKeyProbe(folder).files_since(bookmark)
    emit({
        "raised": raised,
        "result": result.as_row(),
        "good_published": (folder.path() / "good.csv").exists(),
        "bad_published": (folder.path() / "bad.txt").exists(),
        "bad_rejected": (reject / "bad.txt").exists(),
        "good_contents": (folder.path() / "good.csv").read_text(encoding="utf-8")
            if (folder.path() / "good.csv").exists() else None,
        "change_documents": len(list((folder.path() / "_changes").glob("*.json")))
            if (folder.path() / "_changes").exists() else 0,
        "changes": [path.name for path in changes],
    })
finally:
    for path in (folder.path(), folder._staging_path(), reject):
        clear(path)
"""


@pytest.fixture(scope="module")
def deployed(fabric_lakehouse_estate):
    """One submission of ``BODY``, and the evidence both claims below read.

    ``BODY`` already imports the deployed tree, loads the Folder and loads the
    SQL-authored table. Two tests asking it two questions are two questions about
    one moment, so it is submitted once.
    """

    return fabric_lakehouse_estate.env.run_python(BODY, label="the deployed estate")


@weaver_test(hosted=True)
def test_a_developer_can_run_a_deployed_folder_load_primitive(deployed):
    """Import the object the installer deployed, call ``.load()``, and be done.

    The folder is the subject because it is the primitive that most needs a real
    Lakehouse: its authored code writes with ``open()``, which cannot address an
    ``abfss://`` URL at all. Before the files root existed this reported success
    and wrote into a local directory called ``abfss:/…``.
    """

    seen = deployed

    # The tree the installer wrote, laid out as authored: each Lakehouse area is
    # a directory of the deployed tree, as it is a directory of the repository.
    assert "Files" in seen["deployed"]
    assert "Tables" in seen["deployed"]
    assert "DWG__Customer.py" in seen["tables"]
    # `lib/` travels whole. This fixture's holds only a CSV, so a `.py` filter
    # left it out entirely and the folder's read() found nothing to copy.
    assert "lib" in seen["deployed"]
    assert seen["lib"] == ["customers.csv"]
    # The authored path is reproduced verbatim, so the import reads the same.
    assert seen["imported"] == "Raw__CustomerCsv"
    # Two spellings of one location, because two things read them, and the one
    # authored code gets is a real Path, not a string it has to convert.
    assert seen["folder_path_is_mounted"] is True
    assert seen["folder_path_is_a_path"] is True
    assert seen["spark_path_is_abfss"] is True
    # And the files reached OneLake rather than a directory named after a URL.
    assert seen["folder"]["succeeded"] is True
    assert seen["published"], "the folder load published nothing"


@weaver_test(hosted=True)
def test_a_sql_authored_table_is_deployed_and_loaded_as_a_python_primitive(deployed):
    """The conversion's claim, asked of Fabric.

    `Tables/DWG.NamedCustomer.sql` is authored in Spark SQL and installed as
    `Tables/DWG__NamedCustomer.py`. What this asserts is that the file the build wrote
    is importable in the session, carries its authored contract, and loads
    through the ordinary `Table.load()`, so a SQL-authored table and a
    Python-authored one are the same primitive by the time anything runs.
    """

    seen = deployed

    assert "DWG__NamedCustomer.py" in seen["deployed"]
    # No installed `.sql` load file survives the conversion.
    assert not [name for name in seen["deployed"] if name.endswith(".sql")]
    assert seen["sql_authored_module"] == "DWG__NamedCustomer"
    # The authored header travelled whole and is what the primitive reads.
    assert seen["sql_authored_is_generated"] is True
    assert seen["sql_authored_load"]["succeeded"] is True


@weaver_test(hosted=True)
def test_authored_code_consumes_folder_changes_through_the_fabric_mount(
    fabric_lakehouse_estate,
):
    seen = fabric_lakehouse_estate.env.run_python(
        PROBE_CATALOGUE + CHANGE_FEED, label="consume Folder changes"
    )

    assert seen["result"]["succeeded"] is True
    assert seen["unrecorded"] == []
    assert seen["keys_are_full_paths"] is True
    assert seen["values_are_utc"] is True
    assert sorted(Path(path).name for path in seen["changed"]) == [
        "inserted.csv",
        "updated.csv",
    ]
    # Every file the load committed carries the datetime of that one commit.
    assert set(seen["changed"].values()) == {seen["committed_at"]}
    assert seen["latest"] == seen["changed"]
    # Read where the load committed, so a returned key is immediately usable.
    assert seen["contents"] == {"inserted.csv": "inserted", "updated.csv": "updated"}
    assert seen["latest_contents"] == seen["contents"]
    assert [Path(path).name for path in seen["deleted"]] == ["deleted.csv"]
    assert set(seen["deleted"].values()) == {seen["committed_at"]}
    assert seen["deleted_exists"] == [False]
    # Two: the adoption of the files that were already there, then the one
    # commit this transition made.
    assert len(seen["change_documents"]) == 2
    assert seen["strict_changed"] == []
    assert seen["strict_deleted"] == []


@weaver_test(hosted=True)
def test_a_tolerant_file_key_rejection_is_enforced_through_the_fabric_mount(
    fabric_lakehouse_estate,
):
    """The tolerant case, which is the one that shows every outcome at once.

    A bad file is rejected, a good file is published, and the change document
    records the survivor. What ``fault_tolerant=False`` decides instead is a
    semantic the core suite owns; both settle on the same reject table.
    """

    seen = fabric_lakehouse_estate.env.run_python(
        "FAULT_TOLERANT = True\n" + PROBE_CATALOGUE + FILE_KEY_REJECTION,
        label="tolerate a Folder File-key violation",
    )

    assert seen["raised"] is False
    assert seen["result"]["succeeded"] is False
    assert seen["result"]["rows_rejected"] == 1
    assert seen["good_published"] is True
    assert seen["bad_published"] is False
    assert seen["bad_rejected"] is True
    assert seen["good_contents"] == "good"
    assert seen["change_documents"] == 1
    assert seen["changes"] == ["good.csv"]
