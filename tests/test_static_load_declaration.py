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
from support.weaver_test import weaver_test
from support.workspaces import mounted_lakehouse

from weaver import Table
from weaver.declaration import read_source_document
from weaver.declaration.model import LAKEHOUSE, WAREHOUSE
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


def _counting(monkeypatch, module_name: str, attribute: str, answer: bool):
    """Replace a population check with one that records being asked."""

    import importlib

    module = importlib.import_module(module_name)
    calls = []

    def counted(*args, **kwargs):
        calls.append(True)
        return answer

    monkeypatch.setattr(module, attribute, counted)
    return calls


@weaver_test()
def test_a_non_static_table_never_asks_whether_its_target_is_populated(
    monkeypatch, tmp_path
):
    """The cost this ordering removes from every ordinary load in an estate."""

    calls = _counting(
        monkeypatch, "weaver.runtime.table_load", "table_is_populated", True
    )

    class Sales__Country(_TableUnderTest):
        static = False

    table = Sales__Country(_Session(), lakehouse=mounted_lakehouse("LH", tmp_path))
    # It goes on to read(), which this double refuses — the point is only that
    # it got there without asking the target anything.
    with pytest.raises(AssertionError, match="read\\(\\) must not run"):
        table.load()

    assert calls == []


@weaver_test()
def test_a_static_table_does_ask(monkeypatch, tmp_path):

    calls = _counting(
        monkeypatch, "weaver.runtime.table_load", "table_is_populated", True
    )

    class Sales__Country(_TableUnderTest):
        static = True

    result = Sales__Country(
        _Session(), lakehouse=mounted_lakehouse("LH", tmp_path)
    ).load()

    assert calls == [True]
    assert result.succeeded
    assert result.rows_read == 0


# --- the Warehouse procedure carries it ---------------------------------------


def _procedure(*, static: bool) -> str:
    document = _document(WAREHOUSE_TABLE, "Sales.Country.sql", WAREHOUSE, static=static)
    return document.create_load().payload.decode("utf-8")


@weaver_test()
def test_a_static_warehouse_load_returns_early_when_the_target_holds_a_row():
    """Baked into the artefact, not performed by whoever calls it.

    The procedure is independently runnable — someone can execute it by hand —
    so a caller-side check would be a rule that only applied when Weaver was
    driving.
    """

    payload = _procedure(static=True)

    assert "if exists (select 1 from [Sales].[Country])" in payload
    assert "return;" in payload


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
    """So a populated static table costs an existence check, not a source read."""

    payload = _procedure(static=True)

    assert payload.index("if exists (select 1 from [Sales].[Country])") < payload.index(
        "Data transformation"
    )


@weaver_test()
def test_a_non_static_warehouse_load_carries_no_gate_at_all():
    """Emitting a disabled branch would leave a reader guessing which way it went."""

    payload = _procedure(static=False)

    assert "Not static: this object is loaded on every run." in payload
    assert "if exists (select 1 from [Sales].[Country])" not in payload
