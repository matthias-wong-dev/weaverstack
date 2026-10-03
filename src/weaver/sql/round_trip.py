"""Run several independent groups of statements in one Warehouse round trip.

Each group runs through ``sp_executesql`` inside its own ``TRY``/``CATCH``, so a
failed group reports its own error and the others still run.
"""

from __future__ import annotations

#: Separates actions, and an action's position from its error, in the reply.
_RECORD = "nchar(30)"
_FIELD = "nchar(31)"


def round_trip_script(members) -> str:
    """One script running each member's statements and reporting its outcome."""

    lines = [
        # Fabric Warehouse refuses SET XACT_ABORT; an error ends only its TRY.
        "set nocount on;",
        "declare @weaver_outcome nvarchar(max) = N'';",
    ]
    for position, statements in enumerate(members):
        lines.append("begin try")
        lines.extend(
            "    exec sp_executesql N'" + statement.replace("'", "''") + "';"
            for statement in statements
        )
        lines.append(
            f"    set @weaver_outcome += N'{position}' + {_FIELD} + {_RECORD};"
        )
        lines.append("end try")
        lines.append("begin catch")
        lines.append(
            f"    set @weaver_outcome += N'{position}' + {_FIELD} + "
            f"coalesce(error_message(), N'unknown error') + {_RECORD};"
        )
        lines.append("end catch;")
    lines.append("select @weaver_outcome as outcome;")
    return "\n".join(lines)


def read_outcomes(reply: str, count: int) -> dict[int, str | None]:
    """Each position's error, or ``None`` when it succeeded."""

    outcomes: dict[int, str | None] = {}
    for record in reply.split(chr(30)):
        if not record:
            continue
        position, _, error = record.partition(chr(31))
        outcomes[int(position)] = error or None
    if set(outcomes) != set(range(count)):
        raise ValueError("the Warehouse did not report every action's outcome")
    return outcomes
