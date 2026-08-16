"""How the catalogue is spelled in T-SQL.

The Warehouse renderer owns identifier quoting, types and literals, so the
layers above it hold plain Python values under internal snake-case keys and
never see a fragment of SQL.
"""

from __future__ import annotations

from datetime import date, datetime

from .tables import BOOLEAN, CATALOGUE_SCHEMA, TIMESTAMP

#: The type a rendered timestamp literal is cast to.
TIMESTAMP_TYPE = "datetime2(6)"


def identifier(name: str) -> str:
    """A bracket-quoted T-SQL identifier, safe for spaces and keywords.

    The public column names contain spaces by design, so quoting is not
    optional here the way it often is.
    """

    return "[" + name.replace("]", "]]") + "]"


def qualified_name(table, schema: str = CATALOGUE_SCHEMA) -> str:
    """How a rendered statement names one catalogue table.

    Two parts, not three: the connection is already open against the catalogue
    Warehouse, and a Warehouse cannot address another database's tables.
    """

    return f"{identifier(schema)}.{identifier(table.name)}"


def literal(value: object, column_type: str | None = None) -> str:
    """One value as a T-SQL literal.

    ``column_type`` is taken from the column being written, so a boolean
    reaches a ``bit`` as ``1`` and a datetime reaches ``datetime2`` cast rather
    than as a string the engine has to guess at.
    """

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
    """A string body for a T-SQL literal.

    Only the quote doubles. T-SQL has no backslash escape, so a backslash is an
    ordinary character and doubling it would change the value.
    """

    return text.replace("'", "''")


def typed_literal(value: object, column) -> str:
    """One projected value, rendered as the column it is going into."""

    return literal(column.to_public(value), column.type)


__all__ = [
    "TIMESTAMP_TYPE",
    "identifier",
    "literal",
    "qualified_name",
    "typed_literal",
]
