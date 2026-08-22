"""``Static: true`` — the contract, and what it renders into.

A static object is seeded once into an empty target and never loaded again: a
reference list, a fixed dimension, a deployed tree. The behaviour is one rule
stated four times, once per authored form, but implemented in only three places
— because a Spark-SQL-authored table *is* a Python table by the time it loads:

.. code-block:: text

    Python table         Table.load()
    Spark SQL table      Table.load(), through SparkSqlTable
    Python folder        Folder.load()
    Warehouse table      the generated T-SQL procedure

What each of those does when the target is populated is proved where it runs.
This module proves the two things that are true before anything runs: that the
declaration reaches the contract at all, and that the Warehouse procedure comes
out of generation carrying the check.
"""

from __future__ import annotations

import pytest
from support.bookmarks import loaded, never
from support.weaver_test import weaver_test
from support.workspaces import mounted_lakehouse

from weaver import Table
from weaver.declaration import read_source_document
from weaver.declaration.model import LAKEHOUSE, WAREHOUSE, WeaverItemId
from weaver.runtime.load_contract import FolderLoadContract, LoadContract
from weaver.runtime.load_result import RESULT_COLUMNS

PYTHON_TABLE = '''\
"""
Table ID: Sales.Country

Description: The country reference list.

Lineage: Seeded once from the ISO list.

Primary key: Code
{static}
Schema:
  Code: string
  Name: string
"""
from weaver import Table


class Sales__Country(Table):
    def read(self):
        return [], []
'''

SPARK_TABLE = """/*
Table ID: Sales.Country

Description: The country reference list.

Lineage: Seeded once from the ISO list.

Dependencies: []

Primary key: Code
{static}
Schema:
  Code: string
  Name: string
*/
select `Code`, `Name` from sales.iso;
"""

FOLDER = '''\
"""
Folder ID: Sales.Seed

Description: The seed files, delivered once.

Lineage: A one-off drop.

File key: "*.csv"

Incremental: false
{static}
"""
from weaver import Folder


class Sales__Seed(Folder):
    def read(self):
        return self.staging_folder(), []
'''

WAREHOUSE_TABLE = """/*
Table ID: Sales.Country

Description: The country reference list.

Lineage: Seeded once from the ISO list.

Primary key: Code
{static}
Schema:
  Code: varchar(2)
  Name: varchar(100)
*/
select [Code], [Name] from [Src].[Iso]
"""


def _document(source: str, name: str, item_type: str, *, static: bool):
    return read_source_document(
        name,
        source.format(static="\nStatic: true\n" if static else "").encode("utf-8"),
        item_type,
    )


# --- the declaration reaches the contract -------------------------------------


@pytest.mark.parametrize("static", [True, False])
@weaver_test()
def test_a_python_tables_declaration_reaches_its_load_contract(static):
    document = _document(PYTHON_TABLE, "Sales__Country.py", LAKEHOUSE, static=static)

    assert LoadContract.from_document(document.document).static is static


@pytest.mark.parametrize("static", [True, False])
@weaver_test()
def test_a_spark_sql_tables_declaration_reaches_its_load_contract(static):
    document = _document(SPARK_TABLE, "Sales.Country.sql", LAKEHOUSE, static=static)

    assert LoadContract.from_document(document.document).static is static


@pytest.mark.parametrize("static", [True, False])
@weaver_test()
def test_a_folders_declaration_reaches_its_load_contract(static):
    document = _document(FOLDER, "Sales__Seed.py", LAKEHOUSE, static=static)

    assert FolderLoadContract.from_document(document.document).static is static


@pytest.mark.parametrize("static", [True, False])
@weaver_test()
def test_a_warehouse_tables_declaration_reaches_its_load_contract(static):
    document = _document(WAREHOUSE_TABLE, "Sales.Country.sql", WAREHOUSE, static=static)

    assert LoadContract.from_document(document.document).static is static


# --- the population check is only asked when it can matter --------------------
#
# `static` is checked *before* the target is inspected, and the order is not
# style. Python evaluates arguments eagerly, so a predicate taking `populated=`
# would run the query on every ordinary load to answer a question only a static
# object can act on — a Spark action per table, a tree walk per folder, on every
# load in the estate.


class _Session:
    """A Spark session that fails loudly if a load reaches past the gate."""

    def __getattr__(self, name):
        def refuse(*args, **kwargs):
            raise AssertionError(
                f"a static no-op must not reach the session; {name} was called"
            )

        return refuse


class _TableUnderTest(Table):
    """A table whose contract is attached rather than parsed from a docstring.

    This file's own module docstring is not a Weaver document, so the contract
    is supplied directly — the parsing route is covered above.
    """

    static = False
    reads = 0

    def _document(self):
        from weaver.declaration.metadata import PYTHON, parse_document

        return parse_document(
            "Table ID: Sales.Country\n\n"
            "Description: The country reference list.\n\n"
            "Lineage: Seeded once.\n\n"
            "Primary key: Code\n\n"
            + ("Static: true\n\n" if self.static else "")
            + "Schema:\n  Code: string\n  Name: string\n",
            language=PYTHON,
        )

    def read(self):
        type(self).reads += 1
        raise AssertionError("read() must not run when the gate closed")


@weaver_test()
def test_a_bookmarked_static_table_reports_a_successful_load_of_nothing(tmp_path):
    class Sales__Country(_TableUnderTest):
        static = True

    result = Sales__Country(
        _Session(),
        lakehouse=mounted_lakehouse("LH", tmp_path),
        bookmarks=loaded("Sales.Country"),
    ).load()

    assert result.succeeded
    assert result.rows_read == 0
    # A skip is a clean success, so the absent instant is what holds the
    # bookmark still.
    assert result.bookmark_datetime is None


@weaver_test()
def test_a_static_table_with_no_bookmark_loads(tmp_path):
    """The gate opens for an object no clean load has run for."""

    class Sales__Country(_TableUnderTest):
        static = True

    table = Sales__Country(
        _Session(), lakehouse=mounted_lakehouse("LH", tmp_path), bookmarks=never()
    )

    with pytest.raises(AssertionError, match="read\\(\\) must not run"):
        table.load()


@pytest.mark.parametrize("static", [False, True])
@weaver_test()
def test_a_table_with_no_catalogue_refuses_to_load(tmp_path, static):
    """Static or not: a load records how far it got, and that lives in the catalogue.

    The catalogue is a constructor argument rather than a ``load()`` one, because
    an authored ``read()`` is called by Weaver and takes nothing — so anything
    ``read()`` may reach has to be set before the load begins. It is refused
    before ``read()`` runs, which this object asserts for itself.
    """

    from weaver.errors import LoadError

    class Sales__Country(_TableUnderTest):
        pass

    Sales__Country.static = static
    table = Sales__Country(_Session(), lakehouse=mounted_lakehouse("LH", tmp_path))

    with pytest.raises(LoadError) as raised:
        table.load()

    assert "catalogue=" in str(raised.value)


# --- the Warehouse procedure carries it ---------------------------------------


def _procedure(*, static: bool) -> str:
    document = _document(WAREHOUSE_TABLE, "Sales.Country.sql", WAREHOUSE, static=static)
    return document.create_load(
        item=WeaverItemId("Warehouse", "Reporting")
    ).payload.decode("utf-8")


@weaver_test()
def test_a_static_warehouse_load_returns_early_when_it_is_already_bookmarked():
    """Baked into the artefact, not performed by whoever calls it.

    The procedure is independently runnable — someone can execute it by hand —
    so a caller-side check would be a rule that only applied when Weaver was
    driving.

    The bookmark decides it, not the target's contents: ``Static`` means "load
    this once", and the bookmark is the record of whether that has happened. A
    table somebody populated by hand is still loaded, and a table a clean load
    emptied is still skipped.
    """

    payload = _procedure(static=True)

    # The installer carries the procedure as a SQL literal, so its own quotes
    # are doubled.
    assert (
        "if @weaver_bookmark > convert(datetime2(6), ''1900-01-01 00:00:00.000000'')"
        in payload
    )
    assert "return;" in payload
    assert "if exists (select 1 from [Sales].[Country])" not in payload


@weaver_test()
def test_the_static_gate_reports_the_same_result_contract_as_a_real_load():
    """A no-op is a *result*, and a caller must not have to tell them apart.

    Every output is set, not just the interesting ones: a field left at its
    ``null`` default would be indistinguishable from a real null, and the
    defaults exist to make the parameters optional rather than to be read.
    """

    payload = _procedure(static=True)
    gate = payload[: payload.index("Pre-processing")]

    for column in RESULT_COLUMNS:
        assert f"set @{column} = " in gate
    assert "set @succeeded = cast(1 as bit);" in gate


@weaver_test()
def test_the_static_gate_precedes_the_staging_query():
    """So a seeded static table costs one bookmark read, not a source read."""

    payload = _procedure(static=True)

    assert payload.index("if @weaver_bookmark >") < payload.index("Data transformation")


@weaver_test()
def test_the_bookmark_is_read_before_the_static_gate_that_reads_it():
    """The gate compares a local, so the local has to be filled first."""

    payload = _procedure(static=True)

    assert payload.index("select @weaver_bookmark =") < payload.index(
        "if @weaver_bookmark >"
    )


@weaver_test()
def test_a_static_skip_advances_no_bookmark():
    """A skip is a clean success, so the null is what stops it advancing.

    The gate reports ``succeeded``, which is exactly what a caller advances a
    bookmark on. What keeps a static object's bookmark still is that the exit
    reports no bookmark instant at all.
    """

    payload = _procedure(static=True)
    gate = payload[: payload.index("Pre-processing")]

    assert "set @bookmark_datetime = null;" in gate


@weaver_test()
def test_a_procedure_run_by_hand_maintains_its_own_bookmark():
    """The default is 1, so running it directly keeps the object's history right.

    An orchestrated run passes 0 and advances the row itself, beside the record
    of the node that settled — see ``tests/targeted/test_run_dispatch_representation.py``.
    """

    payload = _procedure(static=False)

    assert "@update_catalogue bit = 1" in payload
    assert "if @update_catalogue = 1" in payload
    # Keyed by the logical item that declares it, baked in: which row it means is
    # a fact about the procedure rather than an argument to it.
    assert "N''Warehouse'' as [Item type]" in payload
    assert "N''Reporting'' as [Item name]" in payload
    assert "N''Sales'' as [Schema name]" in payload
    assert "N''Country'' as [Object name]" in payload


@weaver_test()
def test_only_a_clean_load_records_a_bookmark():
    """A rejecting load has not read its window, so it establishes no instant."""

    payload = _procedure(static=False)

    assert "@weaver_error is null and @weaver_rows_rejected = 0" in payload


@weaver_test()
def test_a_non_static_warehouse_load_carries_no_gate_at_all():
    """Emitting a disabled branch would leave a reader guessing which way it went."""

    payload = _procedure(static=False)

    assert "Not static: this object is loaded on every run." in payload
    assert "if @weaver_bookmark >" not in payload
