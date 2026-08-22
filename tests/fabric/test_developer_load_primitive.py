"""A developer running a deployed load primitive in Fabric, by hand.

This is a *primitive* test, and its claim is developer-facing: someone in a
Fabric session can import a deployed object and run its load, with no planner,
no catalogue orchestration and no estate-level entry point in the way.

That is also why it is ``hosted``. The subject is the API the *wheel installed
in the session* offers — that `.load()` exists there and behaves — rather than
the load semantics underneath it, which the core suite proves for a fraction of
the cost.

Two things about Fabric make these claims impossible to prove anywhere cheaper,
and both once passed against a directory and failed against a workspace:

.. code-block:: text

    Files is object storage        -> a Folder needs a mount, not a URL
    the tree is a deployed package -> imports resolve as Files.* and lib.*

So this file asserts only what needs OneLake to be true. Detailed load semantics
remain in the core suite; the two strategic regressions here prove that Folder
change documents and reject evidence survive the real mount and authored API.

One submission, one evidence payload, per the suite's rule: every question about
the installed estate goes in one body and the assertions run here against what
it brings back.

It therefore carries ``fabric`` and ``hosted``: the first says where the
resources are, the second says where Weaver runs. The platform question
underneath it — what a mount is — is a ``fabric and remote`` test of its own and
runs without a publish.
"""

from __future__ import annotations

from pathlib import Path

from support.weaver_test import weaver_test

#: Everything asked of the installed estate, in one round trip. It imports from
#: the deployed tree the way the deployed tree is meant to be imported, loads the
#: folder through the resolved OneLake path, and runs the generated Spark SQL
#: program from the file the installer wrote.
BODY = r"""
import os, sys
from pathlib import Path

from weaver import lakehouse_for

# The body's `target` is an ItemRef — a name, not a destination. Resolving it is
# the orchestrator's move, and the one an object never makes for itself.
destination = lakehouse_for(resolver, target)
results = {}

# The deployed runtime tree, reached as Python must reach it: through the mount
# Weaver makes of the root it resolved, not through /lakehouse/default, which
# names only whatever a notebook happened to attach.
root = destination.files_root() + "/_/Load"
sys.path.insert(0, root)
results["deployed"] = sorted(os.listdir(root))

# The import a deployed tree is laid out for: `Files.*`, because the authored
# path is reproduced verbatim beneath the root — `Files/Raw__CustomerCsv.py`
# stays where it was written, so the module name says the same thing it did.
from Files.Raw__CustomerCsv import Raw__CustomerCsv

results["imported"] = Raw__CustomerCsv.__name__

# What the folder reads is deployed, not placed here: `lib/` travels whole, so
# a helper module's data file arrives beside the module that reads it.
results["lib"] = sorted(os.listdir(os.path.join(root, "lib", "data")))

# The folder load, writing ordinary files to OneLake through the mount. The
# catalogue is named because a load records how far it got, and the workspace is
# what says where it lives — nothing infers it.
export = Raw__CustomerCsv(spark, lakehouse=destination, catalogue=workspace.catalogue)
results["folder"] = export.load().as_row()
# Two spellings of one location, and only one of them is a filesystem path.
results["folder_path_is_mounted"] = not str(export.path()).startswith("abfss://")
results["folder_path_is_a_path"] = isinstance(export.path(), Path)
results["spark_path_is_abfss"] = export.spark_path().startswith("abfss://")
results["published"] = sorted(p.name for p in export.path().iterdir())

# The SQL-authored table, which is a deployed Python module like every other:
# `DWG.NamedCustomer.sql` was compiled into `DWG__NamedCustomer.py`, so it
# imports, constructs and loads exactly as the hand-written ones do.
from DWG__NamedCustomer import DWG__NamedCustomer

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


#: A real Folder state transition and its downstream consumer, in the same
#: Fabric session. The values brought back are evidence captured by authored
#: Python; the test process never reads ``_changes`` itself.
CHANGE_FEED = r"""
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path

from weaver import Folder, lakehouse_for
from weaver.declaration.metadata import PYTHON, parse_document
from weaver.errors import LoadError
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


# Freestanding: this probe is not part of the built estate, so it has no place in
# the catalogue's record and needs none — it uses no bookmark.
folder = Raw__ChangeFeedProbe(spark, lakehouse=destination)
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

    # The seeded state is a managed Folder Weaver has never written, so its
    # lifecycle datetimes are unavailable rather than empty.
    unobserved = None
    try:
        Raw__ChangeFeedProbe(folder).latest_files()
    except LoadError as exc:
        unobserved = str(exc)

    bookmark = datetime.now(timezone.utc)
    Raw__ChangeFeedProbe.files = {
        "updated.csv": "updated",
        "inserted.csv": "inserted",
    }
    Raw__ChangeFeedProbe.deletes = ("deleted.csv",)
    result = folder.load()

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
        "unobserved": unobserved,
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


# Freestanding, as above: an ad-hoc probe the estate does not record.
folder = Raw__FileKeyProbe(spark, lakehouse=destination)
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
        result = folder.load(fault_tolerant=FAULT_TOLERANT)
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


@weaver_test(hosted=True)
def test_a_developer_can_run_a_deployed_folder_load_primitive(fabric_lakehouse_estate):
    """Import the object the installer deployed, call ``.load()``, and be done.

    The folder is the subject because it is the primitive that most needs a real
    Lakehouse: its authored code writes with ``open()``, which cannot address an
    ``abfss://`` URL at all. Before the files root existed this reported success
    and wrote into a local directory called ``abfss:/…``.
    """

    env = fabric_lakehouse_estate.env

    seen = env.run_python(BODY)

    # The tree the installer wrote, laid out as authored: the item's own modules
    # at the root, and the `Files/` segment preserved rather than flattened.
    assert "Files" in seen["deployed"]
    assert "DWG__Customer.py" in seen["deployed"]
    # `lib/` travels whole. This fixture's holds only a CSV, so a `.py` filter
    # left it out entirely and the folder's read() found nothing to copy.
    assert "lib" in seen["deployed"]
    assert seen["lib"] == ["customers.csv"]
    # The authored path is reproduced verbatim, so the import reads the same.
    assert seen["imported"] == "Raw__CustomerCsv"
    # Two spellings of one location, because two things read them — and the one
    # authored code gets is a real Path, not a string it has to convert.
    assert seen["folder_path_is_mounted"] is True
    assert seen["folder_path_is_a_path"] is True
    assert seen["spark_path_is_abfss"] is True
    # And the files reached OneLake rather than a directory named after a URL.
    assert seen["folder"]["succeeded"] is True
    assert seen["published"], "the folder load published nothing"


@weaver_test(hosted=True)
def test_a_sql_authored_table_is_deployed_and_loaded_as_a_python_primitive(
    fabric_lakehouse_estate,
):
    """The conversion's claim, asked of Fabric.

    `DWG.NamedCustomer.sql` is authored in Spark SQL and installed as
    `DWG__NamedCustomer.py`. What this asserts is that the file the build wrote
    is importable in the session, carries its authored contract, and loads
    through the ordinary `Table.load()` — so a SQL-authored table and a
    Python-authored one are the same primitive by the time anything runs.
    """

    env = fabric_lakehouse_estate.env

    seen = env.run_python(BODY)

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
        CHANGE_FEED, label="consume Folder changes"
    )

    assert seen["result"]["succeeded"] is True
    assert "change metadata is unavailable" in seen["unobserved"]
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
    assert len(seen["change_documents"]) == 1
    assert seen["strict_changed"] == []
    assert seen["strict_deleted"] == []


@weaver_test(hosted=True)
def test_an_intolerant_file_key_rejection_is_enforced_through_the_fabric_mount(
    fabric_lakehouse_estate,
):
    seen = fabric_lakehouse_estate.env.run_python(
        "FAULT_TOLERANT = False\n" + FILE_KEY_REJECTION,
        label="refuse a Folder File-key violation",
    )

    assert seen["raised"] is True
    assert seen["result"]["succeeded"] is False
    assert seen["result"]["rows_rejected"] == 1
    assert seen["good_published"] is False
    assert seen["bad_published"] is False
    assert seen["bad_rejected"] is True
    assert seen["change_documents"] == 0
    assert seen["changes"] == []


@weaver_test(hosted=True)
def test_a_tolerant_file_key_rejection_is_enforced_through_the_fabric_mount(
    fabric_lakehouse_estate,
):
    seen = fabric_lakehouse_estate.env.run_python(
        "FAULT_TOLERANT = True\n" + FILE_KEY_REJECTION,
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
