"""How a Folder's staging directory is issued, filled, published and cleared.

The lifecycle is short and every step of it is load-bearing:

.. code-block:: text

    reset the fixed staging directory
    issue exactly one StagingFolder
    run read()
    require *that* object back
    publish from it
    remove it on success, keep it on failure
    clear the issued reference

What each step prevents is asserted here rather than described. The two that
carry the most weight are the reset — without it the previous run's files are
published again and a replacement concludes nothing was retired — and the
identity check, because a returned copy would have Weaver publish a directory it
never emptied.

Pure Python. A folder load is filesystem work, so a Lakehouse rooted at
``tmp_path`` exercises the whole of it; the mount is what Fabric adds, and it is
proved separately where a session exists.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from support.bookmarks import loaded, never
from support.weaver_test import weaver_test
from support.workspaces import mounted_lakehouse

from weaver import Folder
from weaver.errors import LoadError
from weaver.runtime.folder_load import StagingFolder

MODULE_DOC = """
Folder ID: Sales.Export

Description: Customer extracts.

Lineage: The sales system.

File key: "*.csv"

Incremental: false
"""


class Sales__Export(Folder):
    """The object under test. Its module docstring is *this* file's, so the
    contract is attached explicitly below rather than parsed from it."""

    files: dict = {}
    fail_in_read = False
    returns = None
    static = False
    seen: list = []

    def _document(self):
        from weaver.declaration.metadata import PYTHON, parse_document

        text = MODULE_DOC.strip()
        if self.static:
            text = f"{text}\n\nStatic: true"
        return parse_document(text, language=PYTHON)

    def read(self):
        staging = self.staging_folder()
        type(self).seen.append(staging)
        for name, text in self.files.items():
            (staging.path / name).write_text(text, encoding="utf-8")
        if self.fail_in_read:
            raise RuntimeError("the source was unreachable")
        return self.returns if self.returns is not None else staging


@pytest.fixture
def export(tmp_path):
    Sales__Export.files = {}
    Sales__Export.fail_in_read = False
    Sales__Export.returns = None
    Sales__Export.static = False
    Sales__Export.seen = []
    # Nothing loaded yet, which is the state every load here starts from. A
    # static test says otherwise by rebuilding the object with `loaded(...)`.
    return Sales__Export(
        object(),
        lakehouse=mounted_lakehouse("Sales_LH", tmp_path),
        bookmarks=never(),
    )


def _already_loaded(export):
    """The same folder, as a run that had already loaded it cleanly sees it."""

    return Sales__Export(
        object(),
        lakehouse=export.lakehouse,
        bookmarks=loaded("Sales.Export", files=True),
    )


def _staging(export) -> Path:
    return export._staging_path()


# --- what is issued -----------------------------------------------------------


@weaver_test()
def test_staging_is_a_context_manager_over_a_real_path(export):
    export.files = {"a.csv": "x"}

    export.load()
    (issued,) = Sales__Export.seen

    assert isinstance(issued, StagingFolder)
    assert isinstance(issued.path, Path)
    with issued as entered:
        assert entered is issued


@weaver_test()
def test_repeated_calls_within_one_load_hand_back_the_same_object(export):
    """An object may ask more than once, and must not get two directories."""

    seen: list = []

    class Sales__Twice(type(export)):
        def read(self):
            seen.append(self.staging_folder())
            seen.append(self.staging_folder())
            return seen[0], []

    Sales__Twice(object(), lakehouse=export.lakehouse, bookmarks=never()).load()

    assert seen[0] is seen[1]


@weaver_test()
def test_staging_sits_beside_the_destination_under_a_fixed_name(export):
    export.load()

    assert _staging(export) == export.path().with_name("Export_Staging")


@weaver_test()
def test_no_run_identifier_appears_in_the_staging_path(export):
    """A fixed path is what makes a failed load leave one directory to look at.

    Per-run directories would leave a growing pile of them, and no way to say
    which one mattered.
    """

    export.load()
    first = _staging(export)
    export.load()

    assert _staging(export) == first


# --- read-time staging -------------------------------------------------------


@weaver_test()
def test_read_issues_temporary_staging_outside_a_load(export):
    staging = export.staging_folder()

    assert staging.path.is_dir()
    assert staging.path != export.path()
    export._clear_read_staging()


@weaver_test()
def test_read_reuses_one_temporary_staging_folder(export):
    first = export.staging_folder()
    second = export.staging_folder()

    assert first is second
    export._clear_read_staging()


@weaver_test()
def test_next_read_removes_previous_temporary_staging(export):
    first = export.read()
    (first.path / "old.csv").write_text("old", encoding="utf-8")

    second = export.read()

    assert second is not first
    assert not first.path.exists()
    assert second.path.is_dir()
    export._clear_read_staging()


@weaver_test()
def test_read_staging_cleanup_tolerates_prior_removal(export):
    first = export.read()
    first.path.rmdir()

    second = export.read()

    assert second.path.is_dir()
    export._clear_read_staging()


@weaver_test()
def test_a_failed_standalone_read_keeps_staging_until_the_next_read(export):
    export.files = {"partial.csv": "half a download"}
    export.fail_in_read = True

    with pytest.raises(RuntimeError, match="source was unreachable"):
        export.read()

    (first,) = Sales__Export.seen
    assert (first.path / "partial.csv").read_text(encoding="utf-8") == "half a download"

    export.fail_in_read = False
    export.files = {}
    second = export.read()

    assert second is not first
    assert second.path != first.path
    assert not first.path.exists()
    assert second.path.is_dir()
    export._clear_read_staging()


@weaver_test()
def test_read_staging_cleanup_and_destructor_leave_no_directory(export):
    staging = export.staging_folder()
    export._clear_read_staging()

    assert not staging.path.exists()

    staging = export.staging_folder()
    export.__del__()

    assert not staging.path.exists()


@weaver_test()
def test_load_issued_staging_wins_over_read_temporary_staging(export):
    from weaver.runtime.folder_load import new_staging_folder, remove_staging

    issued = new_staging_folder(export.path(), export._staging_path())
    export._issued_staging = issued
    try:
        assert export.staging_folder() is issued
        assert export._read_staging is None
        export._clear_read_staging()
        assert issued.path.exists()
    finally:
        export._issued_staging = None
        remove_staging(issued.path)


# --- what read() must return --------------------------------------------------


@weaver_test()
def test_returning_a_raw_path_is_refused(export):
    export.returns = Path("/tmp/somewhere")

    with pytest.raises(LoadError, match="rather than the folder"):
        export.load()


@weaver_test()
def test_returning_a_string_is_refused(export):
    export.returns = "/tmp/somewhere"

    with pytest.raises(LoadError, match="rather than the folder"):
        export.load()


@weaver_test()
def test_returning_another_staging_folder_of_the_same_path_is_refused(export):
    """Equality is not the contract; identity is.

    A copy addresses the same directory, so publishing from it would *work* —
    which is exactly why the check has to be stricter than it looks. What the
    identity proves is that the object came from the call that reset the
    directory, and a reconstructed one proves nothing at all.
    """

    export.returns = StagingFolder(path=_staging(export))

    with pytest.raises(LoadError, match="rather than the folder"):
        export.load()


@weaver_test()
def test_an_explicit_folder_delete_is_applied_through_the_load_runtime(tmp_path):
    class Sales__Export(Folder):
        files: dict = {}
        deletes = ()

        def _document(self):
            from weaver.declaration.metadata import PYTHON, parse_document

            return parse_document(
                MODULE_DOC.replace("Incremental: false", "Incremental: true").strip(),
                language=PYTHON,
            )

        def read(self):
            staging = self.staging_folder()
            for name, text in self.files.items():
                (staging.path / name).write_text(text, encoding="utf-8")
            return staging, self.deletes

    export = Sales__Export(
        object(),
        lakehouse=mounted_lakehouse("Sales_LH", tmp_path),
        bookmarks=never(),
    )
    export.files = {"keep.csv": "keep", "remove.csv": "remove"}
    export.load()

    export.files = {}
    export.deletes = ("remove.csv",)
    result = export.load()

    assert result.rows_deleted == 1
    assert (export.path() / "keep.csv").exists()
    assert not (export.path() / "remove.csv").exists()


# --- reset --------------------------------------------------------------------


@weaver_test()
def test_a_load_begins_from_an_empty_staging_directory(export):
    staging = _staging(export)
    staging.mkdir(parents=True)
    (staging / "stale.csv").write_text("from a previous run", encoding="utf-8")

    export.files = {"fresh.csv": "new"}
    export.load()

    assert (export.path() / "fresh.csv").read_text(encoding="utf-8") == "new"
    # The stale file was not republished, so the replacement retired it.
    assert not (export.path() / "stale.csv").exists()


@weaver_test()
def test_a_later_sequential_load_resets_and_reuses_the_same_directory(export):
    export.files = {"one.csv": "1"}
    export.load()

    export.files = {"two.csv": "2"}
    result = export.load()

    assert result.succeeded
    assert {p.name for p in export.path().glob("*.csv")} == {"two.csv"}


# --- cleanup ------------------------------------------------------------------


@weaver_test()
def test_a_successful_publication_removes_staging(export):
    export.files = {"a.csv": "x"}

    export.load()

    assert not _staging(export).exists()


@weaver_test()
def test_staging_survives_a_failure_inside_read(export):
    export.files = {"partial.csv": "half a download"}
    export.fail_in_read = True

    with pytest.raises(RuntimeError, match="source was unreachable"):
        export.load()

    assert (_staging(export) / "partial.csv").exists()


@weaver_test()
def test_staging_survives_a_refused_return_value(export):
    export.files = {"partial.csv": "x"}
    export.returns = Path("/tmp/elsewhere")

    with pytest.raises(LoadError):
        export.load()

    assert (_staging(export) / "partial.csv").exists()


@weaver_test()
def test_staging_survives_an_intolerant_rejection(export):
    """A rejected file is the case where the directory matters most.

    The load refused because something was staged that the file key does not
    claim, and the staged tree is the evidence of what that was.
    """

    export.files = {"good.csv": "x", "notes.txt": "not claimed by *.csv"}

    with pytest.raises(LoadError, match="rejected"):
        export.load(fault_tolerant=False)

    assert (_staging(export) / "notes.txt").exists()
    # And the destination is untouched, which is the other half of the refusal.
    assert not (export.path() / "good.csv").exists()


# --- the issued reference -----------------------------------------------------


@weaver_test()
def test_the_issued_reference_is_cleared_after_a_successful_load(export):
    export.load()

    staging = export.staging_folder()

    assert staging.path.is_dir()
    export._clear_read_staging()


@weaver_test()
def test_the_issued_reference_is_cleared_after_a_failed_load(export):
    export.fail_in_read = True

    with pytest.raises(RuntimeError):
        export.load()

    staging = export.staging_folder()

    assert staging.path.is_dir()
    export._clear_read_staging()


@weaver_test()
def test_a_second_load_is_never_handed_the_first_ones_directory(export):
    export.files = {"a.csv": "x"}
    export.load()
    export.load()
    first, second = Sales__Export.seen

    assert first is not second
    assert first.path == second.path


# --- bounded retry ------------------------------------------------------------
#
# Zero-cache mounting is the real repair for a mount that disagrees with the
# storage behind it. This is the defensive half: even with nothing cached, the
# mount fronts object storage rather than a local disk, so a removal can meet an
# entry that is already gone — and the same call a moment later succeeds.


@pytest.fixture
def flaky(monkeypatch):
    """Make ``rmtree`` fail a given number of times before working."""

    import weaver.runtime.folder_load as module

    real = module.shutil.rmtree
    state = {"remaining": 0, "calls": 0}

    def rmtree(path, *args, **kwargs):
        state["calls"] += 1
        if state["remaining"] > 0:
            state["remaining"] -= 1
            raise OSError(39, "Directory not empty")
        return real(path, *args, **kwargs)

    monkeypatch.setattr(module.shutil, "rmtree", rmtree)
    monkeypatch.setattr(module, "RESET_PAUSE", 0.0)
    return state


@weaver_test()
def test_a_transient_reset_failure_is_retried_rather_than_raised(export, flaky):
    _staging(export).mkdir(parents=True)
    flaky["remaining"] = 2
    export.files = {"a.csv": "x"}

    result = export.load()

    assert result.succeeded
    assert flaky["calls"] >= 3


@weaver_test()
def test_a_reset_that_never_succeeds_is_reported_rather_than_retried_forever(
    export, flaky
):
    from weaver.runtime.folder_load import RESET_ATTEMPTS

    _staging(export).mkdir(parents=True)
    flaky["remaining"] = RESET_ATTEMPTS + 5

    with pytest.raises(OSError, match="Directory not empty"):
        export.load()

    assert flaky["calls"] == RESET_ATTEMPTS


@weaver_test()
def test_a_transient_cleanup_failure_does_not_fail_a_published_load(export, flaky):
    """The load already succeeded; what remains is tidying.

    A retry gives the storage a moment to agree, and the same bounded policy
    applies — an unbounded one would turn a published load into a hang.
    """

    export.files = {"a.csv": "x"}
    flaky["remaining"] = 0
    export.load()

    export.files = {"b.csv": "y"}
    flaky["remaining"] = 1  # the reset succeeds; the cleanup stumbles once
    result = export.load()

    assert result.succeeded
    assert not _staging(export).exists()


# --- static ------------------------------------------------------------------
#
# A static folder is materialised once and never again, and its bookmark is the
# record of whether that has happened. The check happens before anything else a
# load does, which for a folder matters more than for a table: a folder already
# loaded must not create a staging directory, must not run the author's
# download, and must not reconcile files.


@weaver_test()
def test_a_static_folder_with_no_bookmark_loads_normally(export):
    export.static = True
    export.files = {"seed.csv": "x"}

    result = export.load()

    assert result.succeeded
    assert (export.path() / "seed.csv").read_text(encoding="utf-8") == "x"
    assert result.rows_inserted == 1


@weaver_test()
def test_a_bookmarked_static_folder_is_a_successful_no_op(export):
    export.static = True
    export.files = {"seed.csv": "x"}
    export.load()

    already = _already_loaded(export)
    already.static = True
    already.files = {"seed.csv": "changed"}
    result = already.load()

    assert result.succeeded
    assert (
        result.rows_read,
        result.rows_inserted,
        result.rows_updated,
        result.rows_deleted,
        result.rows_rejected,
    ) == (0, 0, 0, 0, 0)


@weaver_test()
def test_a_bookmarked_static_folder_advances_nothing(export):
    """A skip is a clean success, so the absent instant is what holds it still."""

    export.static = True
    already = _already_loaded(export)
    already.static = True

    assert already.load().bookmark_datetime is None


@weaver_test()
def test_a_bookmarked_static_folder_does_not_run_the_authored_download(export):
    """Proved by counting, because "did nothing" is what is being claimed."""

    export.static = True
    export.files = {"seed.csv": "x"}
    export.load()
    Sales__Export.seen = []

    already = _already_loaded(export)
    already.static = True
    already.load()

    assert Sales__Export.seen == []


@weaver_test()
def test_a_bookmarked_static_folder_creates_no_staging_directory(export):
    export.static = True
    export.files = {"seed.csv": "x"}
    export.load()

    already = _already_loaded(export)
    already.static = True
    already.load()

    assert not _staging(export).exists()


@weaver_test()
def test_a_bookmarked_static_folder_leaves_its_destination_exactly_as_it_was(export):
    export.static = True
    export.files = {"seed.csv": "original"}
    export.load()

    already = _already_loaded(export)
    already.static = True
    already.files = {"other.csv": "different"}
    already.load()

    assert {p.name for p in export.path().glob("*.csv")} == {"seed.csv"}
    assert (export.path() / "seed.csv").read_text(encoding="utf-8") == "original"


@weaver_test()
def test_a_static_folder_beside_unmanaged_files_still_loads(export):
    """What the destination holds is not what decides it.

    A folder may sit beside files nobody declared, and a folder somebody
    populated by hand has still never been loaded. Only the bookmark says.
    """

    export.static = True
    export.path().mkdir(parents=True)
    (export.path() / "notes.txt").write_text("not claimed by *.csv", encoding="utf-8")
    export.files = {"seed.csv": "x"}

    result = export.load()

    assert result.rows_inserted == 1
    assert (export.path() / "seed.csv").exists()


@pytest.mark.parametrize("static", [False, True])
@weaver_test()
def test_a_folder_with_no_catalogue_refuses_to_load(export, static):
    """Static or not: a load records how far it got, and that lives in the catalogue.

    The catalogue is a constructor argument rather than a ``load()`` one, because
    an authored ``read()`` is called by Weaver and takes nothing — so anything
    ``read()`` may reach has to be set before the load begins.
    """

    from weaver.errors import LoadError

    unaware = Sales__Export(object(), lakehouse=export.lakehouse)
    unaware.static = static
    unaware.files = {"seed.csv": "x"}

    with pytest.raises(LoadError) as raised:
        unaware.load()

    assert "catalogue=" in str(raised.value)
    assert not _staging(unaware).exists()


@weaver_test()
def test_a_non_static_folder_reloads_whatever_the_destination_holds(export):
    export.static = False
    export.files = {"seed.csv": "original"}
    export.load()

    export.files = {"seed.csv": "second run"}
    result = export.load()

    assert result.rows_updated == 1
    assert (export.path() / "seed.csv").read_text(encoding="utf-8") == "second run"


@weaver_test()
def test_no_load_asks_whether_the_destination_is_populated(export, monkeypatch):
    """What the destination holds decides nothing, static or not.

    A folder somebody populated by hand has not been loaded, and a folder a
    clean load emptied has been. The bookmark is the record, so the managed tree
    is never walked to answer the question.
    """

    import weaver.runtime.folder_load as module

    asked = []
    monkeypatch.setattr(
        module,
        "folder_is_populated",
        lambda *a, **k: asked.append(True) or True,
    )
    export.files = {"a.csv": "x"}

    assert export.load().rows_inserted == 1

    export.static = True
    Sales__Export.seen = []
    export.files = {"b.csv": "y"}
    export.load()

    assert asked == []
