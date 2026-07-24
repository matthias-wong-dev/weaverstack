"""Cross-engine aliases: parsing, eligibility, and namespace ownership."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from weaver import Location
from weaver.errors import DiscoveryError, MetadataError
from weaver.ses import PYTHON, SPARK_SQL, SQL, ObjectId, parse_document, read_repository


# --- alias parsing and eligibility -------------------------------------------


def parse(text: str, *, language: str):
    return parse_document(textwrap.dedent(text), language=language)


PY_DELTA = """
    Table ID: Sales.Customer

    Description: x

    Lineage: y

    Primary key: Id

    Schema:
      Id: string
    {alias}
"""

SPARK_DELTA = """
    Table ID: Sales.Customer

    Description: x

    Lineage: y

    Dependencies:
      - Sales.Source

    Primary key: Id

    Schema:
      Id: string
    {alias}
"""

SPARK_VIEW = """
    View ID: Sales.CustomerView

    Description: x

    Lineage: y

    Dependencies:
      - Sales.Customer
    {alias}
"""

SQL_TABLE = """
    Table ID: Reporting.Summary

    Description: x

    Lineage: y
    {alias}
"""

SQL_VIEW = """
    View ID: Reporting.SummaryView

    Description: x

    Lineage: y
    {alias}
"""

FOLDER = """
    Folder ID: Raw.Files

    Description: x

    Lineage: y

    File key: "*.csv"
    {alias}
"""


def test_a_python_table_may_publish_a_warehouse_alias():
    document = parse(PY_DELTA.format(alias="\n    Warehouse alias: Sales.Customer"), language=PYTHON)
    assert document.warehouse_alias == ObjectId("Sales", "Customer")
    assert document.lakehouse_alias is None


def test_a_spark_table_may_publish_a_warehouse_alias():
    document = parse(SPARK_DELTA.format(alias="\n    Warehouse alias: Sales.Customer"), language=SPARK_SQL)
    assert document.warehouse_alias == ObjectId("Sales", "Customer")


def test_a_spark_view_may_publish_a_warehouse_alias():
    document = parse(SPARK_VIEW.format(alias="\n    Warehouse alias: Sales.CustomerView"), language=SPARK_SQL)
    assert document.warehouse_alias == ObjectId("Sales", "CustomerView")


def test_a_sql_table_may_publish_a_lakehouse_alias():
    document = parse(SQL_TABLE.format(alias="\n    Lakehouse alias: Reporting.Summary"), language=SQL)
    assert document.lakehouse_alias == ObjectId("Reporting", "Summary")
    assert document.warehouse_alias is None


def test_a_sql_view_may_publish_a_lakehouse_alias():
    document = parse(SQL_VIEW.format(alias="\n    Lakehouse alias: Reporting.SummaryView"), language=SQL)
    assert document.lakehouse_alias == ObjectId("Reporting", "SummaryView")


def test_an_object_with_no_alias_has_none():
    document = parse(PY_DELTA.format(alias=""), language=PYTHON)
    assert document.warehouse_alias is None
    assert document.lakehouse_alias is None


def test_an_alias_may_differ_from_the_native_id():
    document = parse(PY_DELTA.format(alias="\n    Warehouse alias: Published.Customer"), language=PYTHON)
    assert document.warehouse_alias == ObjectId("Published", "Customer")


def test_a_lakehouse_object_rejects_a_lakehouse_alias():
    with pytest.raises(MetadataError, match="Lakehouse alias.*SQL table or view"):
        parse(PY_DELTA.format(alias="\n    Lakehouse alias: Sales.Customer"), language=PYTHON)


def test_a_warehouse_object_rejects_a_warehouse_alias():
    with pytest.raises(MetadataError, match="Warehouse alias.*Delta table or Spark view"):
        parse(SQL_TABLE.format(alias="\n    Warehouse alias: Reporting.Summary"), language=SQL)


def test_a_folder_rejects_a_warehouse_alias():
    with pytest.raises(MetadataError, match="Folder is not published across engines"):
        parse(FOLDER.format(alias="\n    Warehouse alias: Raw.Files"), language=PYTHON)


def test_a_malformed_alias_is_refused():
    with pytest.raises(MetadataError, match="two-part Schema.Object"):
        parse(PY_DELTA.format(alias="\n    Warehouse alias: Customer"), language=PYTHON)


# --- namespace ownership across a repository ---------------------------------


def delta(name: str, *, warehouse_alias: str | None = None) -> str:
    schema, obj = name.split(".")
    alias = f"\nWarehouse alias: {warehouse_alias}" if warehouse_alias else ""
    return textwrap.dedent(
        f'''"""
Table ID: {name}

Description: x

Lineage: y{alias}

Primary key: Id

Schema:
  Id: string
"""

from weaver import Table


class {schema}__{obj}(Table):
    def read(self):
        return [], []
'''
    )


def sql_table(name: str, *, lakehouse_alias: str | None = None, body: str = "select 1 as [Id]") -> str:
    alias = f"\nLakehouse alias: {lakehouse_alias}" if lakehouse_alias else ""
    return textwrap.dedent(
        f"""/*
Table ID: {name}

Description: x

Lineage: y{alias}
*/

{body}
"""
    )


def spark_view(name: str, *, warehouse_alias: str | None = None, dep: str = "Sales.Customer") -> str:
    alias = f"\nWarehouse alias: {warehouse_alias}" if warehouse_alias else ""
    return textwrap.dedent(
        f"""/*
View ID: {name}

Description: x

Lineage: y{alias}

Dependencies:
  - {dep}
*/

select * from {dep}
"""
    )


def build(tmp_path: Path, *, schemas: list[str], objects: dict[str, str]) -> Location:
    directory = tmp_path / "_schemas"
    directory.mkdir()
    for schema in schemas:
        (directory / f"{schema}.yml").write_text(f"Schema ID: {schema}\n", encoding="utf-8")
    for filename, text in objects.items():
        (tmp_path / filename).write_text(text, encoding="utf-8")
    return Location(str(tmp_path))


def test_two_objects_may_not_publish_the_same_warehouse_alias(tmp_path):
    root = build(
        tmp_path,
        schemas=["Sales", "Published"],
        objects={
            "Sales__Customer.py": delta("Sales.Customer", warehouse_alias="Published.Thing"),
            "Sales__Other.py": delta("Sales.Other", warehouse_alias="Published.Thing"),
        },
    )
    with pytest.raises(DiscoveryError, match="Warehouse alias Published.Thing is published by both"):
        read_repository(root)


def test_two_objects_may_not_publish_the_same_lakehouse_alias(tmp_path):
    root = build(
        tmp_path,
        schemas=["Reporting", "Published"],
        objects={
            "Reporting.A.sql": sql_table("Reporting.A", lakehouse_alias="Published.Thing"),
            "Reporting.B.sql": sql_table("Reporting.B", lakehouse_alias="Published.Thing"),
        },
    )
    with pytest.raises(DiscoveryError, match="Lakehouse alias Published.Thing is published by both"):
        read_repository(root)


def test_a_warehouse_alias_may_not_collide_with_a_warehouse_table(tmp_path):
    root = build(
        tmp_path,
        schemas=["Sales", "Reporting"],
        objects={
            "Sales__Customer.py": delta("Sales.Customer", warehouse_alias="Reporting.Customer"),
            "Reporting.Customer.sql": sql_table("Reporting.Customer"),
        },
    )
    with pytest.raises(DiscoveryError, match="Warehouse alias Reporting.Customer.*collides"):
        read_repository(root)


def test_a_lakehouse_alias_may_not_collide_with_a_delta_table(tmp_path):
    root = build(
        tmp_path,
        schemas=["Sales", "Reporting"],
        objects={
            "Reporting.Summary.sql": sql_table("Reporting.Summary", lakehouse_alias="Sales.Feature"),
            "Sales__Feature.py": delta("Sales.Feature"),
        },
    )
    with pytest.raises(DiscoveryError, match="Lakehouse alias Sales.Feature.*collides"):
        read_repository(root)


def test_a_warehouse_alias_may_not_collide_with_a_warehouse_view(tmp_path):
    root = build(
        tmp_path,
        schemas=["Sales", "Reporting"],
        objects={
            "Sales__Customer.py": delta("Sales.Customer", warehouse_alias="Reporting.CustomerView"),
            "Reporting.CustomerView.sql": textwrap.dedent(
                """/*
View ID: Reporting.CustomerView

Description: x

Lineage: y
*/

select 1 as [Id]
"""
            ),
        },
    )
    with pytest.raises(DiscoveryError, match="Warehouse alias Reporting.CustomerView.*collides"):
        read_repository(root)


def test_a_lakehouse_native_and_its_same_named_warehouse_alias_are_valid(tmp_path):
    root = build(
        tmp_path,
        schemas=["Sales"],
        objects={"Sales__Customer.py": delta("Sales.Customer", warehouse_alias="Sales.Customer")},
    )
    repo = read_repository(root)
    assert ObjectId("Sales", "Customer") in repo.warehouse_aliases
    assert ObjectId("Sales", "Customer") in repo.lakehouse_native


def test_a_warehouse_native_and_its_same_named_lakehouse_alias_are_valid(tmp_path):
    root = build(
        tmp_path,
        schemas=["Reporting"],
        objects={"Reporting.Summary.sql": sql_table("Reporting.Summary", lakehouse_alias="Reporting.Summary")},
    )
    repo = read_repository(root)
    assert ObjectId("Reporting", "Summary") in repo.lakehouse_aliases
    assert ObjectId("Reporting", "Summary") in repo.warehouse_native
