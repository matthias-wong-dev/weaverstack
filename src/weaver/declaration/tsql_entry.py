"""The two fixed generic entry points a person calls in a Warehouse.

.. code-block:: sql

    exec _.[Load] @object_name = 'Sales.Customer', @fault_tolerant = 0;
    exec _.[Test] @object_name = 'Sales.OrdersReconcile';

Each finds the object's own implementation procedure at runtime, wraps the call
in ``TRY``/``CATCH``, maps what came back into the catalogue's result
vocabulary, writes the operational record, and raises again anything the
procedure raised. ``_.Test`` serves both kinds of validation, so a caller need
not know which one a name was declared as.

Two properties shape the generated SQL.

**The entry points are fixed.** They are Weaver-owned Programmables whose
content is the same for every Warehouse item and does not change when loadable
objects or validations are added or removed. Dispatch reads the physical estate
instead of enumerating it: ``_.Load`` checks ``object_id`` for the
implementation procedure named after the requested object, and ``_.Test``
checks for ``_[Test X.Y]`` and ``_[Assumption X.Y]``, where the physical
procedures themselves decide which kind ran. An unknown name is remembered and
raised before any record is written.

**Identity comes from the parameter, or from ``_.Installation``.** A program
that knows the logical item it is dispatching supplies ``@item_name``. A person
calling by hand omits it, and the procedure recovers the one logical Warehouse
item bound to this database, refusing rather than guessing when the answer is
not unique. Because these are Warehouse procedures, the item type needs no
parameter.

Every write remains a MERGE, including the appends. In every Warehouse but the
one the catalogue lives in these tables are views across databases, and Fabric
refuses a plain ``INSERT`` through such a view while accepting a ``MERGE``'s.
An appended row merges on a fresh surrogate, so it never matches.

See ``design/catalogue.md`` for who records and what the results mean.
"""

from __future__ import annotations

from typing import Mapping, Sequence

from ..catalogue.tables import (
    AUDIT_COLUMN_NAMES,
    BOOKMARK,
    CATALOGUE_SCHEMA,
    INSTALLATION,
    LOAD_STATISTIC,
    LOAD_STATUS,
    LOG,
    TEST_STATUS,
    RuntimeTable,
)
from ..catalogue.tsql import identifier
from ..errors import DiscoveryError
from .metadata import AUDIT_LIVE_DELETE_DATETIME
from .tsql_load import RESULT_PARAMETER_NAMES, RESULT_PARAMETERS

#: Signature salt for the fixed entry points. The payload is signed by its own
#: bytes as well, so this salts nothing the text does not already say; raising
#: it forces every installation to replace its entry points together.
TSQL_ENTRY_VERSION = 2

#: The error numbers Weaver's own generated code throws with. A load refused
#: inside this range reads as Failed and anything else as Error; the Python side
#: draws the same line with ``isinstance(exc, WeaverError)``.
WEAVER_ERROR_MIN = 51000
WEAVER_ERROR_MAX = 51999

#: What each entry point is called. Read as a sentence: ``exec _.[Load]``.
LOAD_ENTRY = "Load"
TEST_ENTRY = "Test"

#: What a rethrow uses when the original number is below THROW's floor of 50000.
#: A Fabric error such as Invalid object name carries its own low number, and
#: the message still travels.
RETHROW_ERROR = 51031

#: How wide a ``Schema.Object`` argument may be. Both halves are Weaver logical
#: names, which are identifier-width.
_OBJECT_NAME_TYPE = "varchar(261)"

#: How wide the optional logical item name is.
_ITEM_NAME_TYPE = "varchar(128)"


def generate_load_entry() -> str:
    """``_.[Load]``, the same fixed procedure for every Warehouse item."""

    return _procedure(
        name=_qualified(LOAD_ENTRY),
        parameters=(
            f"    @object_name {_OBJECT_NAME_TYPE}",
            "  , @fault_tolerant bit = 0",
            "  , @ignore_stability_threshold bit = 0",
            f"  , @item_name {_ITEM_NAME_TYPE} = null",
        ),
        body="\n\n".join(
            [
                _load_declarations(),
                _split_object_name(),
                _resolve_item(),
                _try_catch(
                    _load_dispatch() + "\n\n" + _LOAD_EXECUTION
                ),
                _refuse_unmatched(),
                _load_result(),
                _write(LOG, _log_values(task_type="load"), keyed=False),
                _write(LOAD_STATUS, _status_values(), keyed=True),
                _write(LOAD_STATISTIC, _load_statistic_values(), keyed=False),
                _bookmark_advance(),
                _rethrow(),
            ]
        ),
    )


def generate_test_entry() -> str:
    """``_.[Test]``, the one validation entry point, for both kinds."""

    return _procedure(
        name=_qualified(TEST_ENTRY),
        parameters=(
            f"    @object_name {_OBJECT_NAME_TYPE}",
            f"  , @item_name {_ITEM_NAME_TYPE} = null",
        ),
        body="\n\n".join(
            [
                _test_declarations(),
                _split_object_name(),
                _resolve_item(),
                _try_catch(
                    _test_dispatch() + "\n\n" + _TEST_EXECUTION
                ),
                _refuse_unmatched(),
                _test_result(),
                _write(LOG, _log_values(task_type="test"), keyed=False),
                _write(TEST_STATUS, _test_status_values(), keyed=True),
                _rethrow(),
            ]
        ),
    )


# --- the shape of a wrapper -----------------------------------------------------


def _procedure(*, name: str, parameters: Sequence[str], body: str) -> str:
    return (
        f"create or alter procedure {name}\n"
        + "\n".join(parameters)
        + "\nas\nbegin\n    set nocount on;\n\n"
        + _indent(body, 4)
        + "\nend;\n"
    )


def _try_catch(dispatch: str) -> str:
    """Run the object's own procedure, holding a failure until it is recorded.

    Raised again by :func:`_rethrow` once the rows are written.
    """

    return (
        "begin try\n"
        f"{_indent(dispatch, 4)}\n"
        "end try\n"
        "begin catch\n"
        "    set @weaver_error = error_message();\n"
        "    set @weaver_error_number = error_number();\n"
        "end catch;"
    )


def _declarations(*lines: str) -> str:
    return "\n".join(lines)


def _common_declarations() -> tuple[str, ...]:
    return (
        # nvarchar, because that is what THROW takes for a message variable.
        "declare @weaver_unmatched nvarchar(2048) = null;",
        "declare @weaver_started datetime2(6) = sysutcdatetime();",
        "declare @weaver_completed datetime2(6) = null;",
        # A call by hand is a workflow of one.
        "declare @weaver_workflow varchar(128) = cast(newid() as varchar(36));",
        "declare @weaver_log_sk varchar(128) = cast(newid() as varchar(36));",
        "declare @weaver_schema varchar(128) = null;",
        "declare @weaver_object varchar(128) = null;",
        "declare @weaver_target nvarchar(261) = null;",
        "declare @weaver_call nvarchar(max) = null;",
        "declare @weaver_error varchar(4000) = null;",
        "declare @weaver_error_number int = null;",
        "declare @weaver_rethrow nvarchar(2048) = null;",
        "declare @weaver_result varchar(128) = null;",
        "declare @weaver_message varchar(4000) = null;",
    )


def _load_declarations() -> str:
    return _declarations(
        *_common_declarations(),
        "declare @weaver_statistic_sk varchar(128) = cast(newid() as varchar(36));",
        *(
            f"declare @{logical} {type_name} = null;"
            for logical, type_name in RESULT_PARAMETERS
        ),
    )


def _test_declarations() -> str:
    return _declarations(
        *_common_declarations(),
        "declare @weaver_test_type varchar(128) = null;",
        "declare @weaver_failure_count bigint = null;",
        "declare @missing_count bigint = null;",
        "declare @unexpected_count bigint = null;",
        "declare @violation_count bigint = null;",
    )


# --- resolving what was asked for -----------------------------------------------


def _split_object_name() -> str:
    """Split the requested ``Schema.Object`` once, on its first dot.

    Neither half may carry a dot, so the first is the only one, and a bare name
    is refused here rather than left to fail inside a string later.
    """

    return "\n".join(
        (
            "if charindex('.', @object_name) = 0",
            "begin",
            "    set @weaver_unmatched = concat(@object_name, "
            "N' is not a Schema.Object name');",
            "end",
            "else",
            "begin",
            "    set @weaver_schema = substring(@object_name, 1, "
            "charindex('.', @object_name) - 1);",
            "    set @weaver_object = substring(@object_name, "
            "charindex('.', @object_name) + 1, len(@object_name));",
            "end;",
        )
    )


def _resolve_item() -> str:
    """The logical item this call records against.

    Supplied, it is used as given. Omitted, exactly one logical Warehouse item
    bound to this database is the answer; anything else refuses, because
    guessing would record work against the wrong item.
    """

    scope = INSTALLATION
    item_type = scope.public_name_of("item_type")
    item_name = scope.public_name_of("item_name")
    target_name = scope.public_name_of("target_name")
    return "\n".join(
        (
            "if @item_name is null and @weaver_unmatched is null",
            "begin",
            "    declare @weaver_installations int;",
            "    select @weaver_installations = count(*)",
            f"         , @item_name = min({identifier(item_name)})",
            f"      from {_qualified(INSTALLATION.name)}",
            f"     where {identifier(item_type)} = N'Warehouse'",
            f"       and {identifier(target_name)} = db_name();",
            "    if @weaver_installations <> 1",
            "    begin",
            "        set @weaver_unmatched = concat(",
            "            N'the logical Weaver item for ', db_name(),",
            "            N' is not unique in _.Installation; supply @item_name');",
            "        set @item_name = null;",
            "    end;",
            "end;",
        )
    )


def _load_dispatch() -> str:
    """Find the object's own procedure, and prepare the call that runs it."""

    from ..etl import LOAD_PROCEDURE_PREFIX

    return "\n".join(
        [
            "if @weaver_unmatched is null",
            "begin",
            "    set @weaver_target = N'[' + replace(@weaver_schema, N']', N']]') "
            "+ N'].[' + replace(N'" + LOAD_PROCEDURE_PREFIX + "' + @object_name, "
            "N']', N']]') + N']';",
            "    if object_id(@weaver_target, 'P') is null",
            "    begin",
            "        set @weaver_unmatched = concat(@object_name, ",
            "            N' is not a loadable object in this Warehouse');",
            "    end",
            "    else",
            "    begin",
            "        set @weaver_call = N'exec ' + @weaver_target + N' '",
            "            + N'@fault_tolerant = @fault_tolerant'",
            "            + N', @ignore_stability_threshold = @ignore_stability_threshold'",
            ]
        + [
            f"            + N', @{RESULT_PARAMETER_NAMES[logical]} = @{logical} output'"
            for logical, _type_name in RESULT_PARAMETERS
        ]
        + [
            "            + N';';",
            "    end;",
            "end;",
        ]
    )


_LOAD_EXECUTION = (
    "if @weaver_call is not null\n"
    "begin\n"
    "    exec sp_executesql @weaver_call,\n"
    "        N'@fault_tolerant bit,\n"
    "           @ignore_stability_threshold bit,\n"
    + ",\n".join(
        f"           @{RESULT_PARAMETER_NAMES[logical]} {type_name} output"
        for logical, type_name in RESULT_PARAMETERS
    )
    + "',\n"
    "        @fault_tolerant = @fault_tolerant,\n"
    "        @ignore_stability_threshold = @ignore_stability_threshold,\n"
    + ",\n".join(
        f"        @{RESULT_PARAMETER_NAMES[logical]} = @{logical} output"
        for logical, _type_name in RESULT_PARAMETERS
    )
    + ";\n"
    "end;"
)


def _test_dispatch() -> str:
    """Ask the physical procedures which kind this is, and refuse ambiguity.

    ``_[Test X.Y]`` and ``_[Assumption X.Y]`` are different objects; exactly
    one may exist. Registry is not consulted: what the Warehouse holds is the
    answer, and holding both is a broken installation rather than a choice.
    """

    test_type_column = TEST_STATUS.column("test_type")
    kind_test = test_type_column.to_public("test")
    kind_assumption = test_type_column.to_public("assumption")
    return "\n".join(
        [
            "if @weaver_unmatched is null",
            "begin",
            "    declare @weaver_test_procedure nvarchar(261) = N'[' "
            "+ replace(@weaver_schema, N']', N']]') + N'].[' + replace(",
            f"        N'Test ' + @object_name, N']', N']]') + N']';",
            "    declare @weaver_assumption_procedure nvarchar(261) = N'[' "
            "+ replace(@weaver_schema, N']', N']]') + N'].[' + replace(",
            f"        N'Assumption ' + @object_name, N']', N']]') + N']';",
            "    declare @weaver_has_test int =",
            "        case when object_id(@weaver_test_procedure, 'P') is null "
            "then 0 else 1 end;",
            "    declare @weaver_has_assumption int =",
            "        case when object_id(@weaver_assumption_procedure, 'P') is null "
            "then 0 else 1 end;",
            "    if @weaver_has_test = 1 and @weaver_has_assumption = 1",
            "    begin",
            "        set @weaver_unmatched = concat(@object_name,",
            "            N' is installed as both a Test and an Assumption; "
            "repair the installation');",
            "    end",
            "    else if @weaver_has_test = 0 and @weaver_has_assumption = 0",
            "    begin",
            "        set @weaver_unmatched = concat(@object_name,",
            "            N' is not a validation in this Warehouse');",
            "    end",
            "    else if @weaver_has_test = 1",
            "    begin",
            f"        set @weaver_test_type = N'{kind_test}';",
            "        set @weaver_target = @weaver_test_procedure;",
            "        set @weaver_call = N'exec ' + @weaver_target + N' '",
            "            + N'@missing_count = @missing_count output'",
            "            + N', @unexpected_count = @unexpected_count output'",
            "            + N';';",
            "    end",
            "    else",
            "    begin",
            f"        set @weaver_test_type = N'{kind_assumption}';",
            "        set @weaver_target = @weaver_assumption_procedure;",
            "        set @weaver_call = N'exec ' + @weaver_target + N' '",
            "            + N'@violation_count = @violation_count output'",
            "            + N';';",
            "    end;",
            "end;",
        ]
    )


_TEST_EXECUTION = (
    "if @weaver_call is not null\n"
    "begin\n"
    "    exec sp_executesql @weaver_call,\n"
    "        N'@missing_count bigint output,\n"
    "           @unexpected_count bigint output,\n"
    "           @violation_count bigint output',\n"
    "        @missing_count = @missing_count output,\n"
    "        @unexpected_count = @unexpected_count output,\n"
    "        @violation_count = @violation_count output;\n"
    "end;"
)


def _rethrow() -> str:
    """Raise again what the lower procedure raised, once the record is written.

    Only what was thrown. A returned outcome is an answer rather than a failure
    and travels back as one, whether a validation finding discrepancies or a load
    whose rejections were tolerated.

    The original number is kept where THROW accepts it, so a caller can still
    match on Weaver's own refusal codes.
    """

    return (
        "if @weaver_error is not null\n"
        "begin\n"
        "    set @weaver_rethrow = cast(@weaver_error as nvarchar(2048));\n"
        "    declare @weaver_number int =\n"
        "        case when @weaver_error_number >= 50000 then @weaver_error_number\n"
        f"             else {RETHROW_ERROR} end;\n"
        "    throw @weaver_number, @weaver_rethrow, 1;\n"
        "end;"
    )


def _refuse_unmatched() -> str:
    """Raise a name that matched nothing, having recorded nothing for it.

    Nothing ran, so there is no outcome and no object to record one against, and
    every identity column is not null, so a row would be refused by the
    catalogue and would hide the message saying what went wrong.
    """

    return (
        "if @weaver_unmatched is not null\n"
        "begin\n"
        "    throw 51030, @weaver_unmatched, 1;\n"
        "end;"
    )


# --- what happened, in the catalogue's own words --------------------------------


def _weaver_refusal() -> str:
    return f"@weaver_error_number between {WEAVER_ERROR_MIN} and {WEAVER_ERROR_MAX}"


def _load_result() -> str:
    """Map the lower procedure's outcome into the public Result vocabulary.

    The cases are in the order they exclude each other.
    """

    return (
        "set @weaver_completed = sysutcdatetime();\n"
        "set @weaver_message = coalesce(@weaver_error, @error_message);\n"
        "set @weaver_result =\n"
        f"    case when {_weaver_refusal()} then N'Failed'\n"
        "         when @weaver_error is not null then N'Error'\n"
        "         when @is_static_skip = 1 then N'Skipped'\n"
        "         when @succeeded = 1 then N'Succeeded'\n"
        "         when @rows_rejected > 0 then N'Rejected'\n"
        "         else N'Failed' end;"
    )


def _test_result() -> str:
    """The same mapping for a validation, which has counts rather than rows.

    A validation that threw is an Error whatever threw it: it produced no
    judgement, so there is nothing for Failed to mean and the failure count stays
    null. No refusal-range branch here, unlike a load's.
    :func:`weaver.run.record.result_for` draws the same line in Python.
    """

    return (
        "set @weaver_completed = sysutcdatetime();\n"
        "set @weaver_message = @weaver_error;\n"
        "set @weaver_failure_count =\n"
        "    case when @weaver_error is not null then null\n"
        "         else coalesce(@missing_count, 0) + coalesce(@unexpected_count, 0)\n"
        "              + coalesce(@violation_count, 0) end;\n"
        "set @weaver_result =\n"
        "    case when @weaver_error is not null then N'Error'\n"
        "         when @weaver_failure_count > 0 then N'Failed'\n"
        "         else N'Succeeded' end;"
    )


# --- writing the record ---------------------------------------------------------


def _log_values(*, task_type: str) -> dict[str, str]:
    return {
        "log_sk": "@weaver_log_sk",
        "workflow_id": "@weaver_workflow",
        "task_type": f"N'{task_type}'",
        "target_type": "N'Warehouse'",
        "target_name": "db_name()",
        "schema_name": "@weaver_schema",
        "object_name": "@weaver_object",
        "result": "@weaver_result",
        "started_datetime": "@weaver_started",
        "completed_datetime": "@weaver_completed",
        "duration_milliseconds": _duration(),
        "message": "@weaver_message",
        # Structured detail belongs to a run, which has a node to serialise. A
        # standalone call has the counts, and they are in _.LoadStatistic.
        "details": "null",
    }


def _identity_values() -> dict[str, str]:
    return {
        "item_type": "N'Warehouse'",
        "item_name": "@item_name",
        "schema_name": "@weaver_schema",
        "object_name": "@weaver_object",
    }


def _status_values() -> dict[str, str]:
    return {
        **_identity_values(),
        "workflow_id": "@weaver_workflow",
        "result": "@weaver_result",
        "started_datetime": "@weaver_started",
        "completed_datetime": "@weaver_completed",
        "duration_milliseconds": _duration(),
    }


def _load_statistic_values() -> dict[str, str]:
    return {
        "load_statistic_sk": "@weaver_statistic_sk",
        "workflow_id": "@weaver_workflow",
        **_identity_values(),
        "started_datetime": "@weaver_started",
        "completed_datetime": "@weaver_completed",
        "duration_milliseconds": _duration(),
        "rows_read": "coalesce(@rows_read, 0)",
        "rows_inserted": "coalesce(@rows_inserted, 0)",
        "rows_updated": "coalesce(@rows_updated, 0)",
        "rows_deleted": "coalesce(@rows_deleted, 0)",
        "rows_rejected": "coalesce(@rows_rejected, 0)",
        "is_reload": "cast(0 as bit)",
        "is_static_skip": "cast(coalesce(@is_static_skip, 0) as bit)",
    }


def _test_status_values() -> dict[str, str]:
    return {
        **_identity_values(),
        "test_type": "@weaver_test_type",
        "workflow_id": "@weaver_workflow",
        "result": "@weaver_result",
        "started_datetime": "@weaver_started",
        "completed_datetime": "@weaver_completed",
        "duration_milliseconds": _duration(),
        "failure_count": "@weaver_failure_count",
    }


def _duration() -> str:
    return "datediff(millisecond, @weaver_started, @weaver_completed)"


def _write(table: RuntimeTable, values: Mapping[str, str], *, keyed: bool) -> str:
    """One row into one runtime table, as the MERGE the view will accept.

    A MERGE even for an append: Fabric refuses a plain INSERT through a
    cross-database view. An appended row merges on a surrogate generated a moment
    ago, so it never matches.
    """

    missing = [name for name in table.column_names if name not in values]
    if missing:
        raise DiscoveryError(f"{table.qualified}: no value was supplied for {missing}")
    public = table.public_name_of
    source = _listed(
        f"{values[name]} as {identifier(public(name))}" for name in table.key
    )
    on = _listed(
        (
            f"target.{identifier(public(name))} = source.{identifier(public(name))}"
            for name in table.key
        ),
        separator="and ",
    )
    columns = _listed(identifier(public(name)) for name in table.physical_columns)
    inserted = _listed(
        [
            f"source.{identifier(public(name))}" if name in table.key else values[name]
            for name in table.column_names
        ]
        + [
            "sysdatetime()",
            "sysdatetime()",
            f"convert(datetime2(6), '{AUDIT_LIVE_DELETE_DATETIME}')",
        ]
    )
    matched = ""
    if keyed:
        updates = _listed(
            [
                f"target.{identifier(public(name))} = {values[name]}"
                for name in table.comparison_columns
            ]
            + [f"target.{identifier(public(AUDIT_COLUMN_NAMES[1]))} = sysdatetime()"]
        )
        matched = f"when matched then update set\n{_indent(updates, 4)}\n"
    return (
        f"merge into {_qualified(table.name)} as target\n"
        "using (select\n"
        f"{_indent(source, 4)}\n"
        ") as source\n"
        "   on\n"
        f"{_indent(on, 4)}\n"
        f"{matched}"
        "when not matched then insert (\n"
        f"{_indent(columns, 4)}\n"
        ")\n"
        "values (\n"
        f"{_indent(inserted, 4)}\n"
        ");"
    )


def _listed(parts, *, separator: str = "") -> str:
    """One value per line, so a diff of generated SQL is legible."""

    values = list(parts)
    lead = f"  {separator}" if separator else "  "
    joined = [
        f"{'' if index == 0 else lead}{value}" for index, value in enumerate(values)
    ]
    if separator:
        return "\n".join(joined)
    return ("\n, ".join(values)) if len(values) > 1 else values[0]


def _bookmark_advance() -> str:
    """Advance the bookmark, for a clean load that established an instant.

    Two conditions, each ruling out a case the other does not: a clean result, so
    a rejecting load keeps the bookmark it had; and an instant reported, so a
    Static skip moves nothing.
    """

    values = {
        **_identity_values(),
        "bookmark_datetime": "@bookmark_datetime",
    }
    return (
        "if @weaver_result = N'Succeeded' and @bookmark_datetime is not null\n"
        "begin\n"
        f"{_indent(_write(BOOKMARK, values, keyed=True), 4)}\n"
        "end;"
    )


def _qualified(name: str) -> str:
    return f"{identifier(CATALOGUE_SCHEMA)}.{identifier(name)}"


def _indent(text: str, spaces: int) -> str:
    pad = " " * spaces
    return "\n".join(pad + line if line.strip() else line for line in text.splitlines())


__all__ = [
    "LOAD_ENTRY",
    "TEST_ENTRY",
    "TSQL_ENTRY_VERSION",
    "WEAVER_ERROR_MAX",
    "WEAVER_ERROR_MIN",
    "generate_load_entry",
    "generate_test_entry",
]
