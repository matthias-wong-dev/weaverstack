"""``SourceDocument.create_ddl`` — the generated *create* DDL per source.

Build creates structure, not data. A Delta table (Python or Spark SQL) becomes a
``CREATE TABLE`` over its declared columns; a view becomes ``CREATE OR REPLACE
VIEW`` over its query body. A Folder has no DDL (it is a directory), and T-SQL is
refused at a v1 boundary. Nothing here runs ``read()`` or consults a table's
query body — that is load.
"""

from __future__ import annotations

import json
import textwrap

import pytest

from weaver.ses import read_source_document
from weaver.ses.ddl import (
    SPARK_SQL_EXECUTOR,
    SPARK_SQL_EXTENSION,
    SPARK_TABLE_EXECUTOR,
    SPARK_TABLE_EXTENSION,
    GeneratedDdl,
)


def _doc(relative_path: str, text: str):
    return read_source_document(relative_path, textwrap.dedent(text).lstrip().encode("utf-8"))


# --- Spark SQL views ---------------------------------------------------------

VIEW_BODY = "select\n    CustomerId,\n    CustomerName\nfrom DWG.Customer\nwhere IsActive = true"

VIEW_SOURCE = f"""
/*
View ID: DWG.ActiveCustomer

Description: Active customers only.

Lineage: $DWG.Customer

Dependencies:
  - DWG.Customer
*/
{VIEW_BODY}
"""


def test_view_wraps_body_in_create_or_replace_view():
    ddl = _doc("DWG.ActiveCustomer.spark.sql", VIEW_SOURCE).create_ddl()

    assert isinstance(ddl, GeneratedDdl)
    assert (ddl.executor, ddl.extension) == (SPARK_SQL_EXECUTOR, SPARK_SQL_EXTENSION)
    assert ddl.content.startswith("CREATE OR REPLACE VIEW DWG.ActiveCustomer AS\n")


def test_view_name_is_the_validated_object_id():
    ddl = _doc("DWG.ActiveCustomer.spark.sql", VIEW_SOURCE).create_ddl()
    assert "VIEW DWG.ActiveCustomer AS" in ddl.content


def test_view_preserves_the_body_verbatim():
    ddl = _doc("DWG.ActiveCustomer.spark.sql", VIEW_SOURCE).create_ddl()
    assert VIEW_BODY in ddl.content


def test_view_normalises_only_trailing_whitespace():
    ddl = _doc("DWG.ActiveCustomer.spark.sql", VIEW_SOURCE + "\n   \n\t\n").create_ddl()
    assert ddl.content == f"CREATE OR REPLACE VIEW DWG.ActiveCustomer AS\n{VIEW_BODY}\n"


def test_view_has_exactly_one_create_and_none_in_the_source():
    doc = _doc("DWG.ActiveCustomer.spark.sql", VIEW_SOURCE)
    ddl = doc.create_ddl()
    assert ddl.content.count("CREATE OR REPLACE VIEW") == 1
    assert "create" not in (doc.sql_body or "").lower()


# --- Delta tables: declared schema, no data ---------------------------------

PY_TABLE_SOURCE = """
    \"\"\"
    Table ID: DWG.Customer

    Description: One row per customer.

    Lineage: $Raw.CustomerCsv

    Schema:
      CustomerId: integer
      CustomerName: string
      IsActive: boolean
    \"\"\"
    from weaver import Table


    class DWG__Customer(Table):
        def read(self):
            return [], []
"""

SPARK_TABLE_SOURCE = """
/*
Table ID: DWG.CustomerCount

Description: How many customers there are.

Lineage: $DWG.Customer

Dependencies:
  - DWG.Customer

Schema:
  CustomerCount: bigint
*/
select count(*) as CustomerCount from DWG.Customer
"""


def test_python_delta_table_is_a_create_table_over_declared_and_audit_columns():
    ddl = _doc("DWG__Customer.py", PY_TABLE_SOURCE).create_ddl()

    assert (ddl.executor, ddl.extension) == (SPARK_SQL_EXECUTOR, SPARK_SQL_EXTENSION)
    assert ddl.content.startswith("CREATE OR REPLACE TABLE DWG.Customer (\n")
    assert "`CustomerId` integer" in ddl.content
    assert "`CustomerName` string" in ddl.content
    assert "`IsActive` boolean" in ddl.content
    # Every built table carries the audit columns, in the Delta (underscored)
    # spelling, as not-null timestamps (build-philosophy §7.1, plan "Audit
    # columns"); Weaver populates all three on every loaded row.
    assert "`Row_insert_datetime` timestamp NOT NULL" in ddl.content
    assert "`Row_update_datetime` timestamp NOT NULL" in ddl.content
    assert "`Row_delete_datetime` timestamp NOT NULL" in ddl.content
    assert "USING delta" in ddl.content
    assert "delta.columnMapping.mode" in ddl.content


def test_spark_sql_table_defers_its_build_to_the_spark_table_executor():
    """A Spark SQL table's shape is only settled by running its query, so its
    payload is a deterministic instruction the ``spark_table`` executor completes
    at install — not finished SQL (build-philosophy §7.3). The query therefore
    *does* belong in the payload; it is executed at install, not at build."""

    ddl = _doc("DWG.CustomerCount.spark.sql", SPARK_TABLE_SOURCE).create_ddl()

    assert (ddl.executor, ddl.extension) == (
        SPARK_TABLE_EXECUTOR,
        SPARK_TABLE_EXTENSION,
    )
    payload = json.loads(ddl.content)
    assert payload["object"] == "DWG.CustomerCount"
    assert payload["schema_mode"] == "declared"
    # [name, type, not_null]; CustomerCount has no primary key here, so it is
    # nullable, while every audit column is not null.
    assert payload["declared_columns"] == [["CustomerCount", "bigint", False]]
    assert payload["source_query"] == "select count(*) as CustomerCount from DWG.Customer"
    # Audit columns are frozen into the instruction so the executor never reopens
    # the SES source to learn them.
    assert ["Row_insert_datetime", "timestamp", True] in payload["audit_columns"]


def test_an_inferred_spark_sql_table_carries_no_declared_columns():
    source = SPARK_TABLE_SOURCE.split("Schema:")[0].rstrip() + "\n*/\n" + (
        "select count(*) as CustomerCount from DWG.Customer\n"
    )
    ddl = _doc("DWG.CustomerCount.spark.sql", source).create_ddl()

    payload = json.loads(ddl.content)
    assert payload["schema_mode"] == "inferred"
    assert payload["declared_columns"] is None


# --- folders and T-SQL: no create DDL ---------------------------------------

FOLDER_SOURCE = """
    \"\"\"
    Folder ID: Raw.CustomerCsv

    Description: Raw customer CSV as delivered.

    Lineage: A deterministic test drop.

    File key: "*.csv"
    \"\"\"
    from weaver import Folder


    class Raw__CustomerCsv(Folder):
        def read(self):
            return self.staging_folder(), []
"""

TSQL_SOURCE = """
/*
Table ID: Reporting.CustomerReport

Description: A Warehouse report of customers.

Lineage: $DWG.Customer
*/
select CustomerId from DWG.Customer
"""


def test_folder_has_no_create_ddl():
    with pytest.raises(NotImplementedError, match="Folder"):
        _doc("Raw__CustomerCsv.py", FOLDER_SOURCE).create_ddl()


def test_tsql_generation_is_refused_at_the_v1_boundary():
    with pytest.raises(NotImplementedError, match="T-SQL"):
        _doc("Reporting.CustomerReport.sql", TSQL_SOURCE).create_ddl()


# --- determinism -------------------------------------------------------------


@pytest.mark.parametrize(
    "path, source",
    [
        ("DWG.ActiveCustomer.spark.sql", VIEW_SOURCE),
        ("DWG.CustomerCount.spark.sql", SPARK_TABLE_SOURCE),
        ("DWG__Customer.py", PY_TABLE_SOURCE),
    ],
)
def test_create_ddl_is_deterministic(path, source):
    assert _doc(path, source).create_ddl() == _doc(path, source).create_ddl()
