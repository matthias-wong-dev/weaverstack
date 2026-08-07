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

from weaver.declaration import read_source_document
from weaver.declaration.metadata import ObjectId
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
def test_a_python_tables_declaration_reaches_its_load_contract(static):
    document = _document(PYTHON_TABLE, "Sales__Country.py", LAKEHOUSE, static=static)

    assert LoadContract.from_document(document.document).static is static


@pytest.mark.parametrize("static", [True, False])
def test_a_spark_sql_tables_declaration_reaches_its_load_contract(static):
    document = _document(SPARK_TABLE, "Sales.Country.sql", LAKEHOUSE, static=static)

    assert LoadContract.from_document(document.document).static is static


@pytest.mark.parametrize("static", [True, False])
def test_a_folders_declaration_reaches_its_load_contract(static):
    document = _document(FOLDER, "Sales__Seed.py", LAKEHOUSE, static=static)

    assert FolderLoadContract.from_document(document.document).static is static


@pytest.mark.parametrize("static", [True, False])
def test_a_warehouse_tables_declaration_reaches_its_load_contract(static):
    document = _document(WAREHOUSE_TABLE, "Sales.Country.sql", WAREHOUSE, static=static)

    assert LoadContract.from_document(document.document).static is static


# --- what the contract decides ------------------------------------------------


def _table(static: bool) -> LoadContract:
    return LoadContract(
        object_id=ObjectId(schema="Sales", object="Country"), static=static
    )


def test_a_static_object_with_a_populated_target_has_nothing_to_do():
    assert _table(static=True).is_a_no_op_for(populated=True)


def test_a_static_object_with_an_empty_target_loads_normally():
    """The first load is the one a static object exists for."""

    assert not _table(static=True).is_a_no_op_for(populated=False)


def test_a_non_static_object_reloads_whatever_the_target_holds():
    assert not _table(static=False).is_a_no_op_for(populated=True)
    assert not _table(static=False).is_a_no_op_for(populated=False)


def test_a_folder_answers_the_same_question_the_same_way():
    contract = FolderLoadContract(
        object_id=ObjectId(schema="Sales", object="Seed"), static=True
    )

    assert contract.is_a_no_op_for(populated=True)
    assert not contract.is_a_no_op_for(populated=False)


# --- the Warehouse procedure carries it ---------------------------------------


def _procedure(*, static: bool) -> str:
    document = _document(WAREHOUSE_TABLE, "Sales.Country.sql", WAREHOUSE, static=static)
    return document.create_load().payload.decode("utf-8")


def test_a_static_warehouse_load_returns_early_when_the_target_holds_a_row():
    """Baked into the artefact, not performed by whoever calls it.

    The procedure is independently runnable — someone can execute it by hand —
    so a caller-side check would be a rule that only applied when Weaver was
    driving.
    """

    payload = _procedure(static=True)

    assert "if exists (select 1 from [Sales].[Country])" in payload
    assert "return;" in payload


def test_the_static_gate_projects_the_same_result_contract_as_a_real_load():
    """A no-op is a *result*, and a caller must not have to tell them apart."""

    payload = _procedure(static=True)
    gate = payload[: payload.index("Pre-processing")]

    for column in RESULT_COLUMNS:
        assert f"as {column}" in gate
    assert "cast(1 as bit) as succeeded" in gate


def test_the_static_gate_precedes_the_staging_query():
    """So a populated static table costs an existence check, not a source read."""

    payload = _procedure(static=True)

    assert payload.index("if exists (select 1 from [Sales].[Country])") < payload.index(
        "Data transformation"
    )


def test_a_non_static_warehouse_load_carries_no_gate_at_all():
    """Emitting a disabled branch would leave a reader guessing which way it went."""

    payload = _procedure(static=False)

    assert "Not static: this object is loaded on every run." in payload
    assert "if exists (select 1 from [Sales].[Country])" not in payload
