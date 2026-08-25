"""The two interfaces an authored object has, and which of them records.

.. code-block:: text

    Table, Folder      _load()  the load itself, recording nothing
                       load()   the load, recorded and flushed
    Test, Assumption   read()   the evaluation, recording nothing
                       run()    the evaluation, recorded and flushed

Pure Python, with :class:`~support.spark.MockSpark`: these paths use no engine,
and a session that fails on any access proves that rather than asserting it.
"""

from __future__ import annotations

import pytest
from support.catalogues import Recording, never, validating
from support.spark import MockSpark
from support.weaver_test import weaver_test
from support.workspaces import mounted_lakehouse

from weaver import Assumption, Table, Test
from weaver.catalogue.tables import (
    BOOKMARK,
    LOAD_STATISTIC,
    LOAD_STATUS,
    LOG,
    TEST_STATUS,
)
from weaver.errors import LoadError


def _table(returned, *, incremental: bool = True):
    """A table whose ``read()`` returns whatever a case needs."""

    from weaver.declaration.metadata import PYTHON, parse_document

    declared = "true" if incremental else "false"

    class DWG__Customer(Table):
        def _document(self):
            return parse_document(
                f"""
                Table ID: DWG.Customer

                Description: One row per customer.

                Lineage: The sales system.

                Primary key: Customer id

                Incremental: {declared}

                Schema:
                  Customer id: string
                """,
                language=PYTHON,
            )

        def read(self):
            if isinstance(returned, BaseException):
                raise returned
            return returned

    return DWG__Customer


def _assumption(rows):
    """An Assumption whose evidence is a relation this test can count."""

    from weaver.declaration.metadata import PYTHON, parse_document

    class DWG__CustomerHasRows(Assumption):
        def _document(self):
            return parse_document(
                """
                Assumption ID: DWG.CustomerHasRows

                Description: Customers exist.
                """,
                language=PYTHON,
            )

        def read(self):
            return rows

    return DWG__CustomerHasRows


class _Rows:
    """The narrowest relation a validation's counting path asks anything of."""

    def __init__(self, count: int) -> None:
        self._count = count

    def count(self) -> int:
        return self._count


@pytest.fixture
def lakehouse(tmp_path):
    return mounted_lakehouse("Sales_LH", tmp_path)


# --- a load ---------------------------------------------------------------------


@weaver_test()
def test_a_freestanding_object_reads(lakehouse):
    """``read()`` needs no catalogue, so authored source logic runs on its own."""

    table = _table("the rows this read produced")(MockSpark(), lakehouse=lakehouse)

    assert table.read() == "the rows this read produced"
    assert table.installed is None


@weaver_test()
def test_a_freestanding_object_refuses_the_operational_interface(lakehouse):
    """A load reads a window and records how far it read, so it needs both."""

    table = _table(None)(MockSpark(), lakehouse=lakehouse)

    with pytest.raises(LoadError) as raised:
        table.load()

    assert "not anchored to the Weaver catalogue" in str(raised.value)


@weaver_test()
def test_an_anchored_load_records_and_flushes(lakehouse):
    """Everything the load produced, written and made durable before it returns."""

    catalogue = never("DWG.Customer")
    table = _table(None)(MockSpark(), lakehouse=lakehouse, catalogue=catalogue)

    result = table.load()

    assert result.succeeded
    assert [name for name, _row in catalogue.writer.submitted] == [
        LOG.name,
        LOAD_STATISTIC.name,
    ]
    assert [name for name, _row in catalogue.writer.updated] == [
        LOAD_STATUS.name,
        BOOKMARK.name,
    ]
    assert catalogue.writer.flushes == 1


@weaver_test()
def test_a_refused_load_is_recorded_and_then_raised(lakehouse):
    """A refusal is an outcome, so the estate's record of it is not silent."""

    catalogue = never("DWG.Customer")
    # A non-incremental table returning None: staging is the whole truth, so an
    # empty one retires everything, and Weaver refuses rather than guessing.
    table = _table(None, incremental=False)(
        MockSpark(), lakehouse=lakehouse, catalogue=catalogue
    )

    with pytest.raises(LoadError, match="cannot return None"):
        table.load()

    assert catalogue.writer.rows(LOAD_STATUS.name)[0]["result"] == "failed"
    assert catalogue.writer.rows(LOG.name)[0]["result"] == "failed"
    assert catalogue.writer.rows(BOOKMARK.name) == []
    assert catalogue.writer.flushes == 1


@weaver_test()
def test_an_unexpected_failure_is_recorded_as_an_error_and_re_raised(lakehouse):
    """A Spark or storage failure is not a judgement Weaver made.

    So the row says Error where a refusal says Failed, and the exception reaches
    the caller unchanged.
    """

    catalogue = never("DWG.Customer")
    table = _table(RuntimeError("the cluster went away"))(
        MockSpark(), lakehouse=lakehouse, catalogue=catalogue
    )

    with pytest.raises(RuntimeError, match="cluster went away"):
        table.load()

    assert catalogue.writer.rows(LOG.name)[0]["result"] == "error"
    assert catalogue.writer.rows(LOAD_STATUS.name)[0]["result"] == "error"
    assert catalogue.writer.rows(BOOKMARK.name) == []
    assert catalogue.writer.flushes == 1


@weaver_test()
def test_a_failure_that_carried_no_counts_still_reports_what_it_was(lakehouse):
    """The statistic describes a load that ran, and a load that threw ran."""

    catalogue = never("DWG.Customer")
    table = _table(RuntimeError("the cluster went away"))(
        MockSpark(), lakehouse=lakehouse, catalogue=catalogue
    )

    with pytest.raises(RuntimeError):
        table.load()

    (statistic,) = catalogue.writer.rows(LOAD_STATISTIC.name)
    assert statistic["rows_read"] == 0
    assert statistic["is_static_skip"] is False
    assert "RuntimeError" in catalogue.writer.rows(LOG.name)[0]["message"]


@weaver_test()
def test_the_lower_load_interface_records_nothing(lakehouse):
    """What an orchestrated run calls, so one row has one writer."""

    catalogue = never("DWG.Customer")
    table = _table(None)(MockSpark(), lakehouse=lakehouse, catalogue=catalogue)

    result = table._load()

    assert result.succeeded
    assert catalogue.writer.submitted == []
    assert catalogue.writer.updated == []
    assert catalogue.writer.flushes == 0


@weaver_test()
def test_a_no_op_load_records_what_it_did_without_touching_spark(lakehouse):
    """An incremental source with nothing to do still leaves a complete record."""

    catalogue = never("DWG.Customer")
    table = _table(None)(MockSpark(), lakehouse=lakehouse, catalogue=catalogue)

    table.load()

    statistic = catalogue.writer.rows(LOAD_STATISTIC.name)[0]
    assert statistic["rows_read"] == 0
    assert statistic["is_static_skip"] is False
    assert catalogue.writer.rows(LOAD_STATUS.name)[0]["result"] == "succeeded"


@weaver_test()
def test_the_recorded_row_names_the_lakehouse_the_object_resolved(lakehouse):
    """Never the session's attachment: a load runs detached against a resolved one."""

    catalogue = never("DWG.Customer")
    table = _table(None)(MockSpark(), lakehouse=lakehouse, catalogue=catalogue)

    table.load()

    row = catalogue.writer.rows(LOG.name)[0]
    assert row["target_type"] == "Lakehouse"
    assert row["target_name"] == "Sales_LH"


# --- a validation ---------------------------------------------------------------


@weaver_test()
def test_a_freestanding_validation_reads(lakehouse):
    """``read()`` returns the evidence, so an author can call it and look."""

    rows = _Rows(3)
    validation = _assumption(rows)(MockSpark(), lakehouse=lakehouse)

    assert validation.read() is rows
    assert validation.installed is None


@weaver_test()
def test_a_freestanding_validation_refuses_the_operational_interface(lakehouse):
    """Recording what it found needs the catalogue that records it."""

    validation = _assumption(_Rows(0))(MockSpark(), lakehouse=lakehouse)

    with pytest.raises(LoadError) as raised:
        validation.run()

    assert "not anchored to the Weaver catalogue" in str(raised.value)


@weaver_test()
def test_an_anchored_validation_resolves_its_own_identity(lakehouse):
    """Through ``_.TestDictionary``: Registry holds only the compiled artefact."""

    catalogue = validating("DWG.CustomerHasRows")
    validation = _assumption(_Rows(0))(
        MockSpark(), lakehouse=lakehouse, catalogue=catalogue
    )

    assert str(validation.installed) == "Lakehouse/Sales/DWG.CustomerHasRows"


@weaver_test()
def test_an_anchored_validation_that_holds_records_a_success(lakehouse):
    catalogue = validating("DWG.CustomerHasRows")
    validation = _assumption(_Rows(0))(
        MockSpark(), lakehouse=lakehouse, catalogue=catalogue
    )

    result = validation.run()

    assert result.succeeded
    row = catalogue.writer.rows(TEST_STATUS.name)[0]
    assert row["result"] == "succeeded"
    assert row["failure_count"] == 0
    assert row["test_type"] == "assumption"
    assert catalogue.writer.flushes == 1


@weaver_test()
def test_an_anchored_validation_that_fails_records_how_much_disagreed(lakehouse):
    catalogue = validating("DWG.CustomerHasRows")
    validation = _assumption(_Rows(4))(
        MockSpark(), lakehouse=lakehouse, catalogue=catalogue
    )

    result = validation.run()

    assert not result.succeeded
    row = catalogue.writer.rows(TEST_STATUS.name)[0]
    assert row["result"] == "failed"
    assert row["failure_count"] == 4


@weaver_test()
def test_a_validation_that_could_not_be_evaluated_is_recorded_as_an_error(lakehouse):
    """It found nothing, and zero discrepancies is the answer it must not give."""

    class _Broken:
        def count(self):
            raise RuntimeError("the source is not there")

    catalogue = validating("DWG.CustomerHasRows")
    validation = _assumption(_Broken())(
        MockSpark(), lakehouse=lakehouse, catalogue=catalogue
    )

    with pytest.raises(RuntimeError, match="not there"):
        validation.run()

    row = catalogue.writer.rows(TEST_STATUS.name)[0]
    assert row["result"] == "error"
    assert row["failure_count"] is None
    assert catalogue.writer.flushes == 1


@weaver_test()
def test_a_validation_records_no_load_state(lakehouse):
    """Two populations, and a validation belongs to one of them."""

    catalogue = validating("DWG.CustomerHasRows")
    _assumption(_Rows(0))(MockSpark(), lakehouse=lakehouse, catalogue=catalogue).run()

    assert catalogue.writer.rows(LOAD_STATUS.name) == []
    assert catalogue.writer.rows(BOOKMARK.name) == []
    assert catalogue.writer.rows(LOAD_STATISTIC.name) == []


@weaver_test()
def test_a_validation_returns_its_counts_rather_than_its_rows(lakehouse):
    """A durable record of the rows would put data into the estate's evidence."""

    catalogue = validating("DWG.CustomerHasRows", writer=Recording())
    validation = _assumption(_Rows(2))(
        MockSpark(), lakehouse=lakehouse, catalogue=catalogue
    )

    result = validation.run()

    assert result.violation_count == 2
    assert not hasattr(result, "rows")
    written = "".join(str(row) for _name, row in catalogue.writer.submitted)
    assert "_Rows" not in written


def _test(sides):
    """A Test whose two sides are whatever a case hands it.

    ``read()`` is Weaver's comparison and may not be overridden, so the sides are
    what a case controls, which is also what an author controls.
    """

    from weaver.declaration.metadata import PYTHON, parse_document

    class DWG__CustomerReconcile(Test):
        def _document(self):
            return parse_document(
                """
                Test ID: DWG.CustomerReconcile

                Description: Customers reconcile to the source.
                """,
                language=PYTHON,
            )

        def _sides(self):
            return sides

    return DWG__CustomerReconcile


@weaver_test()
def test_a_test_takes_the_same_catalogue_the_others_do(lakehouse):
    """One constructor model across every authored object."""

    catalogue = validating("DWG.CustomerReconcile")

    unanchored = _test((None, None))(MockSpark(), lakehouse=lakehouse)
    anchored = _test((None, None))(
        MockSpark(), lakehouse=lakehouse, catalogue=catalogue
    )

    assert unanchored.installed is None
    assert str(anchored.installed) == "Lakehouse/Sales/DWG.CustomerReconcile"


@weaver_test()
def test_an_unanchored_test_refuses_the_operational_interface(lakehouse):
    """Recording what it found needs the catalogue that records it."""

    validation = _test((None, None))(MockSpark(), lakehouse=lakehouse)

    with pytest.raises(LoadError, match="not anchored"):
        validation.run()


# --- the two kinds are both validations -----------------------------------------


@weaver_test()
def test_both_kinds_of_validation_share_one_interface():
    """A Test and an Assumption are told apart by the row, not by the method."""

    assert Test._task_type == Assumption._task_type == "test"
    assert Test._validation_kind == "Test"
    assert Assumption._validation_kind == "Assumption"
    assert callable(Test.run) and callable(Assumption.run)


@weaver_test()
def test_no_authored_object_carries_a_catalogue_write_switch():
    """The interface decides who records, so there is no flag to get wrong."""

    import inspect

    from weaver import Folder

    for cls in (Table, Folder):
        for name in ("load", "_load"):
            parameters = inspect.signature(getattr(cls, name)).parameters
            assert not [one for one in parameters if "catalogue" in one], (cls, name)


__all__: tuple = ()
