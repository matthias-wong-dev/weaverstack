"""How authored Python reaches its bookmark, and how a run advances it.

Four things are worth holding separately here.

What an object answers, given a context: the map a run supplies, keyed by the
identity the Registry uses, with an absent row reading as the sentinel.

What it answers given none: the one message that names what is missing, and
nothing else — no inferred item, no lazily queried table, no substituted
sentinel. A read that could not see its bookmark and reloaded the world instead
would look like a slow load rather than a fault.

What a child inherits: the context and not the value, so an object another
object constructs resolves its own bookmark by its own identity.

What a run advances: a clean success that reported an instant, and nothing else.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from support.bookmarks import LOADED_AT, loaded, never
from support.weaver_test import weaver_test
from support.workspaces import mounted_lakehouse

from weaver import Table
from weaver.errors import LoadError
from weaver.run.bookmarks import RunBookmarks
from weaver.run.result import (
    BLOCKED,
    FAILED,
    SKIPPED,
    SUCCEEDED,
    SUCCEEDED_WITH_REJECTS,
    RunNodeResult,
)
from weaver.runtime.bookmark import NO_CATALOGUE_MESSAGE, BookmarkContext, sentinel
from weaver.runtime.load_result import LoadResult

MODULE_DOC = """Table ID: DWG.Customer

Description: Customers.

Lineage: The sales system.

Primary key: CustomerId

Schema:
  CustomerId: string
"""


class DWG__Customer(Table):
    """Its contract is attached below rather than parsed from this file."""

    def _document(self):
        from weaver.declaration.metadata import PYTHON, parse_document

        return parse_document(MODULE_DOC, language=PYTHON)

    def read(self):
        raise AssertionError("read() is not what this module is about")


class DWG__Order(DWG__Customer):
    """A second object in the same item, for what a child resolves."""

    def _document(self):
        from weaver.declaration.metadata import PYTHON, parse_document

        return parse_document(
            MODULE_DOC.replace("DWG.Customer", "DWG.Order"), language=PYTHON
        )


@pytest.fixture
def lakehouse(tmp_path):
    return mounted_lakehouse("Sales_LH", tmp_path)


# --- what an object answers ----------------------------------------------------


@weaver_test()
def test_a_supplied_bookmark_is_read_by_the_objects_own_identity(lakehouse):
    table = DWG__Customer(
        object(), lakehouse=lakehouse, bookmarks=loaded("DWG.Customer")
    )

    assert table.bookmark == LOADED_AT


@weaver_test()
def test_an_object_with_no_row_reads_the_sentinel(lakehouse):
    """A run reads the whole table, so absent means it has never loaded cleanly."""

    table = DWG__Customer(object(), lakehouse=lakehouse, bookmarks=never())

    assert table.bookmark == sentinel()
    assert table.bookmark == datetime(1900, 1, 1, tzinfo=timezone.utc)


@weaver_test()
def test_a_bookmark_is_always_aware_utc(lakehouse):
    """A naive value compared against source timestamps moves a window by hours."""

    naive = datetime(2026, 8, 20, 6, 0)
    table = DWG__Customer(
        object(), lakehouse=lakehouse, bookmarks=loaded("DWG.Customer", at=naive)
    )

    assert table.bookmark.tzinfo is not None
    assert table.bookmark == naive.replace(tzinfo=timezone.utc)


@weaver_test()
def test_another_objects_bookmark_is_not_this_ones(lakehouse):
    context = loaded("DWG.Order")

    assert DWG__Customer(object(), lakehouse=lakehouse, bookmarks=context).bookmark == (
        sentinel()
    )
    assert DWG__Order(object(), lakehouse=lakehouse, bookmarks=context).bookmark == (
        LOADED_AT
    )


# --- what it answers given no context ------------------------------------------


@weaver_test()
def test_a_bookmark_with_no_catalogue_says_exactly_what_is_missing(lakehouse):
    table = DWG__Customer(object(), lakehouse=lakehouse)

    with pytest.raises(LoadError) as raised:
        table.bookmark

    assert str(raised.value) == NO_CATALOGUE_MESSAGE


@weaver_test()
def test_nothing_falls_back_to_the_sentinel(lakehouse):
    """The one substitution that would be silent, and the one thing ruled out.

    An object told nothing about the catalogue could plausibly answer 1900 and
    reload everything. That is a correct-looking load of the whole source on
    every run, which is why it raises instead.
    """

    table = DWG__Customer(object(), lakehouse=lakehouse)

    with pytest.raises(LoadError):
        table.bookmark


@weaver_test()
def test_no_bookmark_is_read_from_a_lakehouse_name(lakehouse):
    """A physical name is not a logical identity, and two items may share one."""

    table = DWG__Customer(
        object(),
        lakehouse=lakehouse,
        bookmarks=BookmarkContext(item=None, bookmarks={}),
    )

    with pytest.raises(LoadError) as raised:
        table.bookmark

    assert str(raised.value) == NO_CATALOGUE_MESSAGE


# --- what a child inherits -----------------------------------------------------


@weaver_test()
def test_a_child_object_inherits_the_context(lakehouse):
    parent = DWG__Customer(
        object(), lakehouse=lakehouse, bookmarks=loaded("DWG.Customer", "DWG.Order")
    )

    assert DWG__Order(parent).bookmark == LOADED_AT


@weaver_test()
def test_a_child_resolves_its_own_bookmark_and_not_its_parents(lakehouse):
    """The context travels; the value does not."""

    parent = DWG__Customer(
        object(), lakehouse=lakehouse, bookmarks=loaded("DWG.Customer")
    )

    assert parent.bookmark == LOADED_AT
    assert DWG__Order(parent).bookmark == sentinel()


# --- what a run advances -------------------------------------------------------


class _Flusher:
    def __init__(self) -> None:
        self.rows: list = []
        self.flushed = 0

    def update(self, row) -> None:
        self.rows.append(row)

    def flush(self) -> None:
        self.flushed += 1


def _settled(status, *, result=None, logical="Lakehouse/Sales/DWG.Customer"):
    return RunNodeResult(
        node_id="load:Sales_LH/DWG.Customer",
        physical_target="Lakehouse/Sales_LH",
        primitive_kind="python_table",
        status=status,
        logical_id=logical,
        role="load",
        result=result,
    )


BEGAN = datetime(2026, 8, 22, 7, 8, 9, tzinfo=timezone.utc)


@weaver_test()
def test_a_clean_load_advances_to_the_instant_the_primitive_reported():
    flusher = _Flusher()
    advanced = RunBookmarks(flusher)

    advanced.advance(
        _settled(SUCCEEDED, result=LoadResult(succeeded=True, bookmark_datetime=BEGAN))
    )

    assert flusher.rows == [
        {
            "item_type": "Lakehouse",
            "item_name": "Sales",
            "schema_name": "DWG",
            "object_name": "Customer",
            "bookmark_datetime": BEGAN,
        }
    ]


@weaver_test()
def test_a_clean_load_that_moved_no_rows_still_advances():
    """The bookmark records the window that was read, not whether rows moved."""

    flusher = _Flusher()

    RunBookmarks(flusher).advance(
        _settled(
            SUCCEEDED,
            result=LoadResult(succeeded=True, rows_read=0, bookmark_datetime=BEGAN),
        )
    )

    assert len(flusher.rows) == 1


@weaver_test()
def test_a_folder_advances_under_its_files_identity():
    """A Folder and a Table of the same name are two objects."""

    flusher = _Flusher()

    RunBookmarks(flusher).advance(
        _settled(
            SUCCEEDED,
            result=LoadResult(succeeded=True, bookmark_datetime=BEGAN),
            logical="Lakehouse/Sales/Files/Raw.CustomerCsv",
        )
    )

    assert flusher.rows[0]["schema_name"] == "Files/Raw"


@pytest.mark.parametrize(
    "status", [SUCCEEDED_WITH_REJECTS, FAILED, BLOCKED, SKIPPED, "pending"]
)
@weaver_test()
def test_only_a_clean_success_advances(status):
    """A load that rejected a row has not read its window, tolerant or not."""

    flusher = _Flusher()

    RunBookmarks(flusher).advance(
        _settled(status, result=LoadResult(succeeded=False, bookmark_datetime=BEGAN))
    )

    assert flusher.rows == []


@weaver_test()
def test_a_success_that_reported_no_instant_advances_nothing():
    """A Static skip is a clean success, and the absent instant holds it still."""

    flusher = _Flusher()

    RunBookmarks(flusher).advance(
        _settled(SUCCEEDED, result=LoadResult(succeeded=True))
    )

    assert flusher.rows == []


@weaver_test()
def test_an_endpoint_refresh_advances_nothing():
    """It names no logical object, so there is nothing it could be far into."""

    flusher = _Flusher()

    RunBookmarks(flusher).advance(
        _settled(SUCCEEDED, result=LoadResult(succeeded=True), logical=None)
    )

    assert flusher.rows == []


@weaver_test()
def test_a_bookmark_that_cannot_be_written_fails_the_run():
    """State the next load reads, so a lost advance is not evidence to shrug at."""

    from weaver.catalogue.flusher import FlushError
    from weaver.run.result import RunError

    class Refusing(_Flusher):
        def flush(self) -> None:
            raise FlushError("the Warehouse refused the merge")

    with pytest.raises(RunError) as raised:
        RunBookmarks(Refusing()).flush()

    assert "bookmarks were not recorded" in str(raised.value)
