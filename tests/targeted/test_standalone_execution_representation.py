"""The two interfaces an authored object has, and which of them records.

Every authored object divides the same way, and the division is structural rather
than a setting:

.. code-block:: text

    Table, Folder      _load()  the load itself, recording nothing
                       load()   the load, recorded and flushed
    Test, Assumption   read()   the evaluation, recording nothing
                       run()    the evaluation, recorded and flushed

So there is no ``update_catalogue`` to get wrong. An orchestrated run calls the
lower interface and records every node through one queue; a developer calls the
upper one and is told it finished only once the record has landed.

Anchoring adds the upper interface and takes nothing away: a freestanding object
still reads, which is what lets an author call ``read()`` and look at what came
back.

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


def _table(returned):
    """An incremental table whose ``read()`` returns whatever a case wants."""

    from weaver.declaration.metadata import PYTHON, parse_document

    class DWG__Customer(Table):
        def _document(self):
            return parse_document(
                """
                Table ID: DWG.Customer

                Description: One row per customer.

                Lineage: The sales system.

                Primary key: Customer id

                Incremental: true

                Schema:
                  Customer id: string
                """,
                language=PYTHON,
            )

        def read(self):
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
    """The recording path costs no engine call either.

    An incremental source that knows there is nothing to do should be able to say
    so and still leave a complete record of having said it.
    """

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
    """A validation materialises nothing, so Registry does not record it.

    What records it is ``_.TestDictionary``, and anchoring looks there — a
    validation resolved through Registry would find only the module or procedure
    it compiles to.
    """

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
def test_a_validation_records_no_load_state(lakehouse):
    """Two populations, and a validation belongs to one of them."""

    catalogue = validating("DWG.CustomerHasRows")
    _assumption(_Rows(0))(MockSpark(), lakehouse=lakehouse, catalogue=catalogue).run()

    assert catalogue.writer.rows(LOAD_STATUS.name) == []
    assert catalogue.writer.rows(BOOKMARK.name) == []
    assert catalogue.writer.rows(LOAD_STATISTIC.name) == []


@weaver_test()
def test_a_validation_returns_its_counts_rather_than_its_rows(lakehouse):
    """The rows are what ``read()`` gives, and they are not recorded anywhere.

    They carry whatever the validation selected, and a durable record of them
    would put data into the estate's own evidence.
    """

    catalogue = validating("DWG.CustomerHasRows", writer=Recording())
    validation = _assumption(_Rows(2))(
        MockSpark(), lakehouse=lakehouse, catalogue=catalogue
    )

    result = validation.run()

    assert result.violation_count == 2
    assert not hasattr(result, "rows")
    written = "".join(str(row) for _name, row in catalogue.writer.submitted)
    assert "_Rows" not in written


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
