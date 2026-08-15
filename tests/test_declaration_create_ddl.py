"""``SourceDocument.create_ddl`` — the generated *create* DDL per source.

Build creates structure, not data. A Delta table (Python or Spark SQL) becomes a
``CREATE TABLE`` over its declared columns; a view becomes strict ``CREATE
VIEW`` over its query body. A Folder has no DDL (it is a directory). T-SQL
generation has its own test module (``test_declaration_tsql_ddl``); here we only assert a
SQL object routes to the ``tsql`` executor. Nothing here runs ``read()``.

Every Spark object is named in full, and so is every managed reference in a
body. That is not decoration: a bare two-part name resolves through whatever
catalogue the session is attached to, and the session is attached to the Weaver
Lakehouse rather than to the destination being built. The generated payload
names the Lakehouse it means, so the installer runs it as written.
"""

from __future__ import annotations

import json
import textwrap

import pytest

from weaver.declaration import read_source_document
from weaver.declaration.ddl import (
    SPARK_SQL_EXECUTOR,
    SPARK_SQL_EXTENSION,
    SPARK_TABLE_EXECUTOR,
    SPARK_TABLE_EXTENSION,
    GeneratedDdl,
)
from weaver.declaration.model import LAKEHOUSE, WAREHOUSE
from weaver.spark import FabricSparkTarget


def _doc(relative_path: str, text: str, item_type: str = LAKEHOUSE):
    """The owning item type is what chooses a ``.sql`` document's dialect."""

    return read_source_document(
        relative_path, textwrap.dedent(text).lstrip().encode("utf-8"), item_type
    )


#: The Lakehouse these documents are bound to. Naming is per destination, so a
#: generated statement cannot be read without saying which one.
SALES = FabricSparkTarget(workspace="Demo", lakehouse="Sales_LH")


# --- Spark SQL views ---------------------------------------------------------

VIEW_BODY = "select\n    CustomerId,\n    CustomerName\nfrom DWG.Customer\nwhere IsActive = true"

#: The same body as it is frozen: the reference addressed, everything else as the
#: author wrote it.
ADDRESSED_BODY = VIEW_BODY.replace("DWG.Customer", "`Demo`.`Sales_LH`.`DWG`.`Customer`")

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


def test_view_wraps_body_in_strict_create_view():
    ddl = _doc("DWG.ActiveCustomer.sql", VIEW_SOURCE).create_ddl(destination=SALES)

    assert isinstance(ddl, GeneratedDdl)
    assert (ddl.executor, ddl.extension) == (SPARK_SQL_EXECUTOR, SPARK_SQL_EXTENSION)
    assert ddl.content.startswith(
        "CREATE VIEW `Demo`.`Sales_LH`.`DWG`.`ActiveCustomer` AS\n"
    )


def test_view_name_is_the_validated_object_id():
    ddl = _doc("DWG.ActiveCustomer.sql", VIEW_SOURCE).create_ddl(destination=SALES)
    assert "VIEW `Demo`.`Sales_LH`.`DWG`.`ActiveCustomer` AS" in ddl.content


def test_view_preserves_the_body_apart_from_addressing_its_references():
    ddl = _doc("DWG.ActiveCustomer.sql", VIEW_SOURCE).create_ddl(destination=SALES)
    assert ADDRESSED_BODY in ddl.content
    # Only the reference moved. Line breaks, indentation and casing are the
    # author's, because a build freezes text it is going to execute and must not
    # quietly reformat it.
    assert ddl.content.count("\n    CustomerId,\n    CustomerName\n") == 1
    assert "where IsActive = true" in ddl.content


def test_a_view_body_keeps_a_physically_qualified_reference_as_written():
    """Three parts means the author named a physical thing. Weaver leaves it."""

    source = """
    /*
    View ID: DWG.ActiveCustomer

    Description: Active customers from another Lakehouse entirely.

    Lineage: A Lakehouse this repository does not manage.

    Dependencies: []
    */
    select CustomerId from Other_LH.DWG.Customer where IsActive = true
    """
    ddl = _doc("DWG.ActiveCustomer.sql", source).create_ddl(destination=SALES)
    assert "from Other_LH.DWG.Customer" in ddl.content
    assert "`Other_LH`" not in ddl.content


def test_view_normalises_only_trailing_whitespace():
    ddl = _doc("DWG.ActiveCustomer.sql", VIEW_SOURCE + "\n   \n\t\n").create_ddl(destination=SALES)
    assert ddl.content == (
        "CREATE VIEW `Demo`.`Sales_LH`.`DWG`.`ActiveCustomer` AS\n"
        f"{ADDRESSED_BODY}\n"
    )


def test_view_has_exactly_one_create_and_none_in_the_source():
    doc = _doc("DWG.ActiveCustomer.sql", VIEW_SOURCE)
    ddl = doc.create_ddl(destination=SALES)
    assert ddl.content.count("CREATE VIEW") == 1
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
select count(*) as CustomerCount from DWG.Customer;
"""


def test_python_delta_table_is_a_create_table_over_declared_and_audit_columns():
    ddl = _doc("DWG__Customer.py", PY_TABLE_SOURCE).create_ddl(destination=SALES)

    assert (ddl.executor, ddl.extension) == (SPARK_SQL_EXECUTOR, SPARK_SQL_EXTENSION)
    assert ddl.content.startswith(
        "CREATE TABLE `Demo`.`Sales_LH`.`DWG`.`Customer` (\n"
    )
    assert "`CustomerId` integer" in ddl.content
    assert "`CustomerName` string" in ddl.content
    assert "`IsActive` boolean" in ddl.content
    # Every built table carries the audit columns, in the Delta (underscored)
    # spelling, as not-null timestamps (how-does-build-work §2, plan "Audit
    # columns"); Weaver populates all three on every loaded row.
    assert "`row_insert_datetime` timestamp NOT NULL" in ddl.content
    assert "`row_update_datetime` timestamp NOT NULL" in ddl.content
    assert "`row_delete_datetime` timestamp NOT NULL" in ddl.content
    assert "USING delta" in ddl.content
    assert "delta.columnMapping.mode" in ddl.content


def test_spark_sql_table_defers_its_build_to_the_spark_table_executor():
    """A Spark SQL table's shape is only settled by running its query, so its
    payload is a deterministic instruction the ``spark_table`` executor completes
    at install — not finished SQL (how-does-build-work §2). The query therefore
    *does* belong in the payload; it is executed at install, not at build."""

    ddl = _doc("DWG.CustomerCount.sql", SPARK_TABLE_SOURCE).create_ddl(destination=SALES)

    assert (ddl.executor, ddl.extension) == (
        SPARK_TABLE_EXECUTOR,
        SPARK_TABLE_EXTENSION,
    )
    payload = json.loads(ddl.content)
    assert payload["object"] == "`Demo`.`Sales_LH`.`DWG`.`CustomerCount`"
    assert payload["schema_mode"] == "declared"
    # [name, type, not_null]; CustomerCount has no primary key here, so it is
    # nullable, while every audit column is not null.
    assert payload["declared_columns"] == [["CustomerCount", "bigint", False]]
    assert payload["source_query"] == (
        "select count(*) as CustomerCount from `Demo`.`Sales_LH`.`DWG`.`Customer`"
    )
    # Audit columns are frozen into the instruction so the executor never reopens
    # the Weaver document source to learn them.
    assert ["row_insert_datetime", "timestamp", True] in payload["audit_columns"]
    # Nothing to run first, so nothing is carried.
    assert payload["setup"] == []


def test_a_preamble_is_carried_apart_from_the_query_whose_shape_is_read():
    """A body is not always one statement, and only one of them is the shape.

    Handing the whole body to one ``spark.sql`` call fails on the first
    semicolon, so the setup travels separately and runs for its effect. Which
    matters more now than it did: a body may also carry a *second* query naming
    the keys to delete, and that one says nothing about the table's shape
    either.
    """

    source = SPARK_TABLE_SOURCE.replace(
        "select count(*) as CustomerCount from DWG.Customer;",
        "create or replace temporary view live as\n"
        "select * from DWG.Customer where IsActive;\n"
        "\n"
        "select count(*) as CustomerCount from live;",
    )

    payload = json.loads(_doc("DWG.CustomerCount.sql", source).create_ddl(destination=SALES).content)

    assert payload["setup"] == [
        "create or replace temporary view live as\n"
        "select * from `Demo`.`Sales_LH`.`DWG`.`Customer` where IsActive"
    ]
    assert payload["source_query"] == "select count(*) as CustomerCount from live"


def test_an_inferred_spark_sql_table_carries_no_declared_columns():
    source = SPARK_TABLE_SOURCE.split("Schema:")[0].rstrip() + "\n*/\n" + (
        "select count(*) as CustomerCount from DWG.Customer;\n"
    )
    ddl = _doc("DWG.CustomerCount.sql", source).create_ddl(destination=SALES)

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
        _doc("Raw__CustomerCsv.py", FOLDER_SOURCE).create_ddl(destination=SALES)


def test_tsql_object_routes_to_the_tsql_executor():
    from weaver.declaration.ddl import TSQL_EXECUTOR, TSQL_EXTENSION

    ddl = _doc("Reporting.CustomerReport.sql", TSQL_SOURCE, WAREHOUSE).create_ddl()
    assert (ddl.executor, ddl.extension) == (TSQL_EXECUTOR, TSQL_EXTENSION)


# --- determinism -------------------------------------------------------------


@pytest.mark.parametrize(
    "path, source",
    [
        ("DWG.ActiveCustomer.sql", VIEW_SOURCE),
        ("DWG.CustomerCount.sql", SPARK_TABLE_SOURCE),
        ("DWG__Customer.py", PY_TABLE_SOURCE),
    ],
)
def test_create_ddl_is_deterministic(path, source):
    assert _doc(path, source).create_ddl(destination=SALES) == _doc(path, source).create_ddl(destination=SALES)
