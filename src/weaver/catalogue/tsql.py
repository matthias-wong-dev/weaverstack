"""Render catalogue identifiers, types and literals as Warehouse T-SQL."""

from __future__ import annotations

from datetime import date, datetime

from .tables import BOOLEAN, CATALOGUE_SCHEMA, TIMESTAMP

TIMESTAMP_TYPE = "datetime2(6)"


def identifier(name: str) -> str:
    """Bracket-quote an identifier; public catalogue columns contain spaces."""

    return "[" + name.replace("]", "]]") + "]"


def qualified_name(table, schema: str = CATALOGUE_SCHEMA) -> str:
    """Name a table within the Warehouse database of the active connection.

    Names are two-part because the connection is already scoped to the catalogue
    Warehouse.
    """

    return f"{identifier(schema)}.{identifier(table.name)}"


def literal(value: object, column_type: str | None = None) -> str:
    """Render a typed T-SQL literal without relying on engine inference."""

    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (datetime, date)):
        return f"CAST('{value.isoformat()}' AS {TIMESTAMP_TYPE})"
    if isinstance(value, str):
        if column_type == BOOLEAN:
            raise TypeError(
                f"expected a boolean for a {column_type} column, got {value!r}"
            )
        if column_type == TIMESTAMP:
            return f"CAST('{_escaped(value)}' AS {TIMESTAMP_TYPE})"
        return f"N'{_escaped(value)}'"
    if isinstance(value, (int, float)):
        return repr(value)
    raise TypeError(
        f"catalogue values are strings, booleans, numbers, datetimes or null, "
        f"not {type(value).__name__}"
    )


def _escaped(text: str) -> str:
    """Double quotes only; backslashes are ordinary T-SQL characters."""

    return text.replace("'", "''")


def typed_literal(value: object, column) -> str:
    return literal(column.to_public(value), column.type)


__all__ = [
    "TIMESTAMP_TYPE",
    "identifier",
    "literal",
    "qualified_name",
    "typed_literal",
]
