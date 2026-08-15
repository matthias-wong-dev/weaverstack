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
        return (self.returns if self.returns is not None else staging), []


@pytest.fixture
def export(tmp_path):
    Sales__Export.files = {}
    Sales__Export.fail_in_read = False
    Sales__Export.returns = None
    Sales__Export.static = False
    Sales__Export.seen = []
    return Sales__Export(object(), lakehouse=mounted_lakehouse("Sales_LH", tmp_path))


def _staging(export) -> Path:
    return export._staging_path()


# --- what is issued -----------------------------------------------------------


def test_staging_is_a_context_manager_over_a_real_path(export):
    export.files = {"a.csv": "x"}

    export.load()
    (issued,) = Sales__Export.seen

    assert isinstance(issued, StagingFolder)
    assert isinstance(issued.path, Path)
    with issued as entered:
        assert entered is issued


def test_repeated_calls_within_one_load_hand_back_the_same_object(export):
    """An object may ask more than once, and must not get two directories."""

    seen: list = []

    class Sales__Twice(type(export)):
        def read(self):
            seen.append(self.staging_folder())
            seen.append(self.staging_folder())
            return seen[0], []

    Sales__Twice(object(), lakehouse=export.lakehouse).load()

    assert seen[0] is seen[1]


def test_staging_sits_beside_the_destination_under_a_fixed_name(export):
    export.load()

    assert _staging(export) == export.path().with_name("Export_Staging")


def test_no_run_identifier_appears_in_the_staging_path(export):
    """A fixed path is what makes a failed load leave one directory to look at.

    Per-run directories would leave a growing pile of them, and no way to say
    which one mattered.
    """

    export.load()
    first = _staging(export)
    export.load()

    assert _staging(export) == first


# --- what read() must return --------------------------------------------------


def test_returning_a_raw_path_is_refused(export):
    export.returns = Path("/tmp/somewhere")

    with pytest.raises(LoadError, match="rather than the folder"):
        export.load()


def test_returning_a_string_is_refused(export):
    export.returns = "/tmp/somewhere"

    with pytest.raises(LoadError, match="rather than the folder"):
        export.load()


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


# --- reset --------------------------------------------------------------------


def test_a_load_begins_from_an_empty_staging_directory(export):
    staging = _staging(export)
    staging.mkdir(parents=True)
    (staging / "stale.csv").write_text("from a previous run", encoding="utf-8")

    export.files = {"fresh.csv": "new"}
    export.load()

    assert (export.path() / "fresh.csv").read_text(encoding="utf-8") == "new"
    # The stale file was not republished, so the replacement retired it.
    assert not (export.path() / "stale.csv").exists()


def test_a_later_sequential_load_resets_and_reuses_the_same_directory(export):
    export.files = {"one.csv": "1"}
    export.load()

    export.files = {"two.csv": "2"}
    result = export.load()

    assert result.succeeded
    assert {p.name for p in export.path().glob("*.csv")} == {"two.csv"}


# --- cleanup ------------------------------------------------------------------


def test_a_successful_publication_removes_staging(export):
    export.files = {"a.csv": "x"}

    export.load()

    assert not _staging(export).exists()


def test_staging_survives_a_failure_inside_read(export):
    export.files = {"partial.csv": "half a download"}
    export.fail_in_read = True

    with pytest.raises(RuntimeError, match="source was unreachable"):
        export.load()

    assert (_staging(export) / "partial.csv").exists()


def test_staging_survives_a_refused_return_value(export):
    export.files = {"partial.csv": "x"}
    export.returns = Path("/tmp/elsewhere")

    with pytest.raises(LoadError):
        export.load()

    assert (_staging(export) / "partial.csv").exists()


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


def test_the_issued_reference_is_cleared_after_a_successful_load(export):
    export.load()

    with pytest.raises(LoadError, match="only available while a load is running"):
        export.staging_folder()


def test_the_issued_reference_is_cleared_after_a_failed_load(export):
    export.fail_in_read = True

    with pytest.raises(RuntimeError):
        export.load()

    with pytest.raises(LoadError, match="only available while a load is running"):
        export.staging_folder()


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


def test_a_transient_reset_failure_is_retried_rather_than_raised(export, flaky):
    _staging(export).mkdir(parents=True)
    flaky["remaining"] = 2
    export.files = {"a.csv": "x"}

    result = export.load()

    assert result.succeeded
    assert flaky["calls"] >= 3


def test_a_reset_that_never_succeeds_is_reported_rather_than_retried_forever(
    export, flaky
):
    from weaver.runtime.folder_load import RESET_ATTEMPTS

    _staging(export).mkdir(parents=True)
    flaky["remaining"] = RESET_ATTEMPTS + 5

    with pytest.raises(OSError, match="Directory not empty"):
        export.load()

    assert flaky["calls"] == RESET_ATTEMPTS


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
# A static folder is materialised once and never again. The check happens before
# anything else a load does, which for a folder is the whole point: a populated
# static folder must not create a staging directory, must not run the author's
# download, and must not reconcile files.


def test_a_static_folder_loads_normally_into_an_empty_destination(export):
    export.static = True
    export.files = {"seed.csv": "x"}

    result = export.load()

    assert result.succeeded
    assert (export.path() / "seed.csv").read_text(encoding="utf-8") == "x"
    assert result.rows_inserted == 1


def test_a_populated_static_folder_is_a_successful_no_op(export):
    export.static = True
    export.files = {"seed.csv": "x"}
    export.load()

    export.files = {"seed.csv": "changed"}
    result = export.load()

    assert result.succeeded
    assert (
        result.rows_read,
        result.rows_inserted,
        result.rows_updated,
        result.rows_deleted,
        result.rows_rejected,
    ) == (0, 0, 0, 0, 0)


def test_a_populated_static_folder_does_not_run_the_authored_download(export):
    """Proved by counting, because "did nothing" is what is being claimed."""

    export.static = True
    export.files = {"seed.csv": "x"}
    export.load()
    Sales__Export.seen = []

    export.load()

    assert Sales__Export.seen == []


def test_a_populated_static_folder_creates_no_staging_directory(export):
    export.static = True
    export.files = {"seed.csv": "x"}
    export.load()

    export.load()

    assert not _staging(export).exists()


def test_a_populated_static_folder_leaves_its_destination_exactly_as_it_was(export):
    export.static = True
    export.files = {"seed.csv": "original"}
    export.load()

    export.files = {"other.csv": "different"}
    export.load()

    assert {p.name for p in export.path().glob("*.csv")} == {"seed.csv"}
    assert (export.path() / "seed.csv").read_text(encoding="utf-8") == "original"


def test_a_folder_holding_only_unmanaged_files_is_not_populated(export):
    """The file key scopes the question.

    A folder may sit beside files nobody declared, and those say nothing about
    whether *this* folder has been materialised.
    """

    export.static = True
    export.path().mkdir(parents=True)
    (export.path() / "notes.txt").write_text("not claimed by *.csv", encoding="utf-8")
    export.files = {"seed.csv": "x"}

    result = export.load()

    assert result.rows_inserted == 1
    assert (export.path() / "seed.csv").exists()


def test_a_non_static_folder_reloads_whatever_the_destination_holds(export):
    export.static = False
    export.files = {"seed.csv": "original"}
    export.load()

    export.files = {"seed.csv": "second run"}
    result = export.load()

    assert result.rows_updated == 1
    assert (export.path() / "seed.csv").read_text(encoding="utf-8") == "second run"


def test_a_non_static_folder_never_asks_whether_its_destination_is_populated(
    export, monkeypatch
):
    """`static` is checked before the folder is inspected, and the order matters.

    Python evaluates arguments eagerly, so asking whether the destination is
    populated *inside* a predicate would walk the managed tree on every ordinary
    load — to answer a question only a static folder can act on.
    """

    import weaver.runtime.folder_load as module

    asked = []
    monkeypatch.setattr(
        module,
        "folder_is_populated",
        lambda *a, **k: asked.append(True) or True,
    )
    export.static = False
    export.files = {"a.csv": "x"}

    result = export.load()

    assert asked == []
    assert result.rows_inserted == 1


def test_a_static_folder_does_ask(export, monkeypatch):
    import weaver.runtime.folder_load as module

    asked = []
    real = module.folder_is_populated
    monkeypatch.setattr(
        module,
        "folder_is_populated",
        lambda *a, **k: (asked.append(True), real(*a, **k))[1],
    )
    export.static = True
    export.files = {"a.csv": "x"}

    export.load()

    assert asked == [True]
