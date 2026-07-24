"""``SourceDocument.create_ddl`` — the generated create definition per source.

The author writes the query or the ``read()``; Weaver writes the ``CREATE``.
These tests pin the wrapper each language gets, that a Spark SQL body is carried
through untouched apart from trailing whitespace, that generation is
deterministic, and that T-SQL is refused at a deliberate v1 boundary.
"""

from __future__ import annotations

import textwrap

import pytest

from weaver.ses import read_source_document
from weaver.ses.ddl import (
    PYTHON_EXECUTOR,
    PYTHON_EXTENSION,
    SPARK_SQL_EXECUTOR,
    SPARK_SQL_EXTENSION,
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


def test_spark_view_wraps_body_in_create_or_replace_view():
    ddl = _doc("DWG.ActiveCustomer.spark.sql", VIEW_SOURCE).create_ddl()

    assert isinstance(ddl, GeneratedDdl)
    assert ddl.executor == SPARK_SQL_EXECUTOR
    assert ddl.extension == SPARK_SQL_EXTENSION
    assert ddl.content.startswith("CREATE OR REPLACE VIEW DWG.ActiveCustomer AS\n")


def test_spark_view_name_is_the_validated_object_id():
    ddl = _doc("DWG.ActiveCustomer.spark.sql", VIEW_SOURCE).create_ddl()

    # The name comes from the parsed ID, not the filename spelling.
    assert "VIEW DWG.ActiveCustomer AS" in ddl.content


def test_spark_view_preserves_the_body_verbatim():
    ddl = _doc("DWG.ActiveCustomer.spark.sql", VIEW_SOURCE).create_ddl()

    assert VIEW_BODY in ddl.content


def test_spark_view_normalises_only_trailing_whitespace():
    trailing = VIEW_SOURCE + "\n   \n\t\n"
    ddl = _doc("DWG.ActiveCustomer.spark.sql", trailing).create_ddl()

    assert ddl.content == f"CREATE OR REPLACE VIEW DWG.ActiveCustomer AS\n{VIEW_BODY}\n"


def test_spark_view_has_exactly_one_create_and_none_in_the_source():
    doc = _doc("DWG.ActiveCustomer.spark.sql", VIEW_SOURCE)
    ddl = doc.create_ddl()

    assert ddl.content.count("CREATE OR REPLACE VIEW") == 1
    assert "create" not in (doc.sql_body or "").lower()


# --- Spark SQL tables --------------------------------------------------------

TABLE_SOURCE = """
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


def test_spark_table_wraps_body_in_create_or_replace_table_using_delta():
    ddl = _doc("DWG.CustomerCount.spark.sql", TABLE_SOURCE).create_ddl()

    assert ddl.executor == SPARK_SQL_EXECUTOR
    assert ddl.extension == SPARK_SQL_EXTENSION
    assert ddl.content.startswith("CREATE OR REPLACE TABLE DWG.CustomerCount\n")
    assert "USING delta" in ddl.content
    # Column mapping keeps the audit columns' spaced logical names legal.
    assert "delta.columnMapping.mode" in ddl.content
    assert ddl.content.rstrip().endswith("select count(*) as CustomerCount from DWG.Customer")


# --- Python objects ----------------------------------------------------------

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

PY_TABLE_SOURCE = """
    \"\"\"
    Table ID: DWG.Customer

    Description: One row per customer.

    Lineage: $Raw.CustomerCsv

    Schema:
      CustomerId: integer
      CustomerName: string
    \"\"\"
    from weaver import Table


    class DWG__Customer(Table):
        def read(self):
            return [], []
"""


def test_python_folder_generates_a_runtime_wrapper_payload():
    ddl = _doc("Raw__CustomerCsv.py", FOLDER_SOURCE).create_ddl()

    assert ddl.executor == PYTHON_EXECUTOR
    assert ddl.extension == PYTHON_EXTENSION
    assert "from Raw__CustomerCsv import Raw__CustomerCsv" in ddl.content
    assert "from weaver.build_bundle.runtime import materialise" in ddl.content
    assert "materialise(Raw__CustomerCsv)" in ddl.content


def test_python_table_generates_a_runtime_wrapper_payload():
    ddl = _doc("DWG__Customer.py", PY_TABLE_SOURCE).create_ddl()

    assert ddl.executor == PYTHON_EXECUTOR
    assert ddl.extension == PYTHON_EXTENSION
    assert "from DWG__Customer import DWG__Customer" in ddl.content
    assert "materialise(DWG__Customer)" in ddl.content


# --- determinism and refusals ------------------------------------------------


@pytest.mark.parametrize(
    "path, source",
    [
        ("DWG.ActiveCustomer.spark.sql", VIEW_SOURCE),
        ("DWG.CustomerCount.spark.sql", TABLE_SOURCE),
        ("Raw__CustomerCsv.py", FOLDER_SOURCE),
        ("DWG__Customer.py", PY_TABLE_SOURCE),
    ],
)
def test_create_ddl_is_deterministic(path, source):
    first = _doc(path, source).create_ddl()
    second = _doc(path, source).create_ddl()

    assert first == second


TSQL_SOURCE = """
/*
Table ID: Reporting.CustomerReport

Description: A Warehouse report of customers.

Lineage: $DWG.Customer
*/
select CustomerId from DWG.Customer
"""


def test_tsql_generation_is_refused_at_the_v1_boundary():
    doc = _doc("Reporting.CustomerReport.sql", TSQL_SOURCE)

    with pytest.raises(NotImplementedError, match="T-SQL"):
        doc.create_ddl()
