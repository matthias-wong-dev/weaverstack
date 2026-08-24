"""Write the external estate's baseline, and put it back after a mutation.

The provisioner establishes the baseline once. The acceptance journey mutates the
``Source`` schema and restores it here, so a later run starts from the same rows.

Nothing here is a test claim: these are the harness's own crossings.
"""

from __future__ import annotations

from decimal import Decimal

from support import external_estate


def warehouse_ddl() -> str:
    """Create the external Warehouse's schemas and tables if they are absent."""

    statements = []
    for schema, tables in external_estate.WAREHOUSE_TABLES.items():
        statements.append(
            f"if schema_id(N'{schema}') is null exec('create schema [{schema}]');"
        )
        for table, (columns, _) in tables.items():
            statements.append(
                f"if object_id(N'[{schema}].[{table}]', N'U') is null "
                f"create table [{schema}].[{table}] ({columns});"
            )
    return "\n".join(statements)


def _values(rows) -> str:
    def literal(value):
        if isinstance(value, Decimal):
            return str(value)
        if isinstance(value, str):
            escaped = value.replace("'", "''")
            return f"N'{escaped}'"
        return str(value)

    return ", ".join(f"({', '.join(literal(value) for value in row)})" for row in rows)


def warehouse_reseed(schema: str) -> str:
    """Empty one external Warehouse schema's tables and write their baseline."""

    statements = []
    for table, (_, rows) in external_estate.WAREHOUSE_TABLES[schema].items():
        statements.append(f"delete from [{schema}].[{table}];")
        statements.append(f"insert into [{schema}].[{table}] values {_values(rows)};")
    return "\n".join(statements)


def warehouse_baseline() -> str:
    """Every external Warehouse table back at its baseline rows."""

    return "\n".join(
        warehouse_reseed(schema) for schema in external_estate.WAREHOUSE_TABLES
    )


def lakehouse_seed_program(root: str) -> str:
    """A Livy body writing the external Lakehouse's Delta tables.

    ``root`` is the item's ``abfss://`` root. Both schemas are written: the
    stable tables so a fresh workspace has them, the mutable ones so a run
    starts from the baseline.
    """

    lines = []
    for table, (schema, rows) in external_estate.TABLES.items():
        path = f"{root}/{external_estate.table_path(table)}"
        lines.append(
            f"spark.createDataFrame({rows!r}, {schema!r})"
            f".write.format('delta').mode('overwrite').save({path!r})"
        )
    for table, (schema, projection, rows) in external_estate.MUTABLE_TABLES.items():
        path = f"{root}/{external_estate.mutable_table_path(table)}"
        lines.append(
            f"spark.createDataFrame({rows!r}, {schema!r})"
            f".selectExpr(*{list(projection)!r})"
            f".write.format('delta').mode('overwrite').save({path!r})"
        )
    return "\n".join(lines) + "\nemit(True)\n"
