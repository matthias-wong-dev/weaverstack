"""The two generic entry points a person calls in a Warehouse.

.. code-block:: sql

    exec _.[Load] @object_name = 'Sales.Customer', @fault_tolerant = 0;
    exec _.[Test] @object_name = 'Sales.OrdersReconcile';

Each wraps the object's own procedure in ``TRY``/``CATCH``, maps what came back
into the catalogue's result vocabulary, writes the operational record, and raises
again anything the procedure raised. ``_.Test`` serves both kinds of validation,
so a caller need not know which one a name was declared as.

Two constraints shape the generated SQL.

**Dispatch is a static chain, not dynamic SQL.** These are generated for one
item, so the objects it installs are known and each becomes a branch. That is
what lets the lower procedure's output parameters be read directly, and it makes
a name the item does not install a refusal rather than a failure inside a string.

**Every write is a MERGE**, including the appends. In every Warehouse but the one
the catalogue lives in these tables are views across databases, and Fabric
refuses a plain ``INSERT`` through such a view while accepting a ``MERGE``'s. An
appended row merges on a fresh surrogate, so it never matches.

See ``design/catalogue.md`` for who records and what the results mean.
"""

from __future__ import annotations

from typing import Mapping, Sequence

from ..catalogue.tables import (
    AUDIT_COLUMN_NAMES,
    BOOKMARK,
    CATALOGUE_SCHEMA,
    LOAD_STATISTIC,
    LOAD_STATUS,
    LOG,
    TEST_STATUS,
    RuntimeTable,
)
from ..catalogue.tsql import identifier
from ..errors import DiscoveryError
from .metadata import ASSUMPTION, AUDIT_LIVE_DELETE_DATETIME, TEST, ObjectId
from .tsql_load import RESULT_PARAMETER_NAMES, RESULT_PARAMETERS

#: Signature salt for the generated entry points. Raise it when this generator
#: changes — though the payload is signed by its own bytes as well, because the
#: dispatch chain changes with the objects the item installs.
TSQL_ENTRY_VERSION = 1

#: The error numbers Weaver's own generated code throws with. A load refused
#: inside this range reads as Failed and anything else as Error; the Python side
#: draws the same line with ``isinstance(exc, WeaverError)``.
WEAVER_ERROR_MIN = 51000
WEAVER_ERROR_MAX = 51999

#: What each entry point is called. Read as a sentence: ``exec _.[Load]``.
LOAD_ENTRY = "Load"
TEST_ENTRY = "Test"

#: What a rethrow uses when the original number is below THROW's floor of 50000.
#: A Fabric error such as *Invalid object name* carries its own low number, and
#: the message still travels.
RETHROW_ERROR = 51031

#: How wide a ``Schema.Object`` argument may be. Both halves are Weaver logical
#: names, which are identifier-width.
_OBJECT_NAME_TYPE = "varchar(261)"


def generate_load_entry(item, objects: Sequence[ObjectId]) -> str:
    """``_.[Load]`` for one Warehouse item, over the objects it loads."""

    if not objects:
        raise DiscoveryError(
            f"{item} installs no loads, so it is given no _.Load to wrap them"
        )
    return _procedure(
        name=_qualified(LOAD_ENTRY),
        parameters=(
            f"    @object_name {_OBJECT_NAME_TYPE}",
            "  , @fault_tolerant bit = 0",
            "  , @ignore_stability_threshold bit = 0",
        ),
        body="\n\n".join(
            [
                _load_declarations(),
                _try_catch(_load_dispatch(objects)),
                _refuse_unmatched(),
                _load_result(),
                _write(LOG, _log_values(task_type="load"), keyed=False),
                _write(LOAD_STATUS, _load_status_values(item), keyed=True),
                _write(LOAD_STATISTIC, _load_statistic_values(item), keyed=False),
                _bookmark_advance(item),
                _rethrow(),
            ]
        ),
    )


def generate_test_entry(item, validations: Mapping[ObjectId, str]) -> str:
    """``_.[Test]`` for one Warehouse item, over the validations it installs.

    ``validations`` maps each validation's ``Schema.Object`` to its declared kind,
    so the branch that runs it calls the right lower procedure.
    """

    if not validations:
        raise DiscoveryError(
            f"{item} installs no validations, so it is given no _.Test to wrap them"
        )
    return _procedure(
        name=_qualified(TEST_ENTRY),
        parameters=(f"    @object_name {_OBJECT_NAME_TYPE}",),
        body="\n\n".join(
            [
                _test_declarations(),
                _try_catch(_test_dispatch(validations)),
                _refuse_unmatched(),
                _test_result(),
                _write(LOG, _log_values(task_type="test"), keyed=False),
                _write(TEST_STATUS, _test_status_values(item), keyed=True),
                _rethrow(),
            ]
        ),
    )


# --- the shape of a wrapper ----------------------------------------------------


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


# --- dispatch ------------------------------------------------------------------


def _load_dispatch(objects: Sequence[ObjectId]) -> str:
    """One branch per loadable object, and a refusal for anything else."""

    branches = []
    for index, object_id in enumerate(sorted(objects, key=lambda one: one.qualified)):
        branches.append(
            _branch(
                index,
                object_id,
                _identity_locals(object_id)
                + "\n"
                + f"exec {_load_procedure(object_id)}\n"
                "      @fault_tolerant = @fault_tolerant\n"
                "    , @ignore_stability_threshold = @ignore_stability_threshold\n"
                + _load_output_arguments(),
            )
        )
    return "\n".join(branches) + "\n" + _unknown("loadable object")


def _load_output_arguments() -> str:
    """Map the lower procedure's private outputs into natural wrapper locals."""

    lines = [
        f"    , @{RESULT_PARAMETER_NAMES[logical]} = @{logical} output"
        for logical, _type_name in RESULT_PARAMETERS
    ]
    lines[-1] += ";"
    return "\n".join(lines)


def _test_dispatch(validations: Mapping[ObjectId, str]) -> str:
    """One branch per validation, calling the procedure its kind declares."""

    branches = []
    ordered = sorted(validations.items(), key=lambda pair: pair[0].qualified)
    for index, (object_id, kind) in enumerate(ordered):
        outputs = (
            "      @violation_count = @violation_count output"
            if kind == ASSUMPTION
            else "      @missing_count = @missing_count output\n"
            "    , @unexpected_count = @unexpected_count output"
        )
        branches.append(
            _branch(
                index,
                object_id,
                _identity_locals(object_id)
                + f"\nset @weaver_test_type = N'{_stored_kind(kind)}';\n"
                f"exec {_validation_procedure(kind, object_id)}\n"
                f"{outputs};",
            )
        )
    return "\n".join(branches) + "\n" + _unknown("validation")


def _branch(index: int, object_id: ObjectId, body: str) -> str:
    lead = "if" if index == 0 else "else if"
    return (
        f"{lead} @object_name = N'{_escape(object_id.qualified)}'\n"
        "begin\n"
        f"{_indent(body, 4)}\n"
        "end"
    )


def _unknown(what: str) -> str:
    """What an unrecognised name gets: a refusal naming what it is not.

    Remembered rather than thrown from here, because this sits inside the TRY.
    Raised by :func:`_refuse_unmatched` ahead of every write.
    """

    return (
        "else\n"
        "begin\n"
        f"    set @weaver_unmatched = concat(@object_name, "
        f"N' is not a {what} in this Warehouse');\n"
        "end;"
    )


def _rethrow() -> str:
    """Raise again what the lower procedure raised, once the record is written.

    Only what was *thrown*. A returned outcome — a validation finding
    discrepancies, a load whose rejections were tolerated — is an answer rather
    than a failure, and travels back as one.

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

    Nothing ran, so there is no outcome and no object to record one against —
    and every identity column is not null, so a row would be refused by the
    catalogue and would hide the message saying what went wrong.
    """

    return (
        "if @weaver_unmatched is not null\n"
        "begin\n"
        "    throw 51030, @weaver_unmatched, 1;\n"
        "end;"
    )


def _identity_locals(object_id: ObjectId) -> str:
    return (
        f"set @weaver_schema = N'{_escape(object_id.schema)}';\n"
        f"set @weaver_object = N'{_escape(object_id.object)}';"
    )


def _load_procedure(object_id: ObjectId) -> str:
    from ..etl import load_procedure_name

    return load_procedure_name(object_id)


def _validation_procedure(kind: str, object_id: ObjectId) -> str:
    from ..etl import validation_procedure_name

    return validation_procedure_name(kind, object_id)


def _stored_kind(kind: str) -> str:
    """How ``[Test type]`` spells this kind, as the ``_`` schema stores it.

    The public value: generated SQL crosses no persistence boundary, so it writes
    what the column holds rather than the internal value Python writes.
    """

    from ..catalogue.tables import ROLE_ASSUMPTION, ROLE_TEST, TEST_STATUS

    internal = {TEST: ROLE_TEST, ASSUMPTION: ROLE_ASSUMPTION}[kind]
    return TEST_STATUS.column("test_type").to_public(internal)


# --- what happened, in the catalogue's own words -------------------------------


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


# --- writing the record --------------------------------------------------------


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


def _identity_values(item) -> dict[str, str]:
    return {
        "item_type": f"N'{_escape(item.item_type)}'",
        "item_name": f"N'{_escape(item.item_name)}'",
        "schema_name": "@weaver_schema",
        "object_name": "@weaver_object",
    }


def _load_status_values(item) -> dict[str, str]:
    return {
        **_identity_values(item),
        "workflow_id": "@weaver_workflow",
        "result": "@weaver_result",
        "started_datetime": "@weaver_started",
        "completed_datetime": "@weaver_completed",
        "duration_milliseconds": _duration(),
    }


def _load_statistic_values(item) -> dict[str, str]:
    return {
        "load_statistic_sk": "@weaver_statistic_sk",
        "workflow_id": "@weaver_workflow",
        **_identity_values(item),
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


def _test_status_values(item) -> dict[str, str]:
    return {
        **_identity_values(item),
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


def _bookmark_advance(item) -> str:
    """Advance the bookmark, for a clean load that established an instant.

    Two conditions, each ruling out a case the other does not: a clean result, so
    a rejecting load keeps the bookmark it had; and an instant reported, so a
    Static skip moves nothing.
    """

    values = {
        **_identity_values(item),
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


def _escape(text: str) -> str:
    return str(text).replace("'", "''")


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
