"""The two generic entry points a person calls in a Warehouse.

Every object gets a procedure of its own — ``_.[Load Sales.Customer]``,
``_.[Test Sales.OrdersReconcile]`` — and those are execution primitives: they do
the object's work and record nothing. An orchestrated run calls them directly and
records what it did centrally.

Running one by hand should record too, and this is what does it:

.. code-block:: sql

    exec _.[Load] @object_name = 'Sales.Customer', @fault_tolerant = 0;
    exec _.[Test] @object_name = 'Sales.OrdersReconcile';

Each wraps the object's own procedure in ``TRY``/``CATCH``, maps what came back
into the catalogue's result vocabulary, and writes the operational record —
``_.Log`` and ``_.LoadStatus``, ``_.LoadStatistic`` and ``_.Bookmark`` for a
load; ``_.Log`` and ``_.TestStatus`` for a validation. So the ownership model is
structural rather than a flag: a primitive never records, a run records centrally,
a wrapper records synchronously.

**One generic wrapper for validations, not two.** ``_.Test`` dispatches to a Test
or an Assumption, because a person asking about ``Sales.OrdersReconcile`` should
not have to know which it was declared as. Which it is settled here, at
generation, from the declaration — never probed from the estate at run time.

**Dispatch is a static chain, not dynamic SQL.** These are generated for one
item, so the objects it installs are known and each becomes a branch. That is
what lets the lower procedure's output parameters be read directly, and it makes
a name the item does not install a refusal rather than a failure inside a string.

**Every write is a MERGE**, including the appends. In every Warehouse but the one
the catalogue lives in these tables are views across databases, and Fabric
refuses a plain ``INSERT`` through such a view while accepting a ``MERGE``'s. An
appended row merges on a fresh surrogate, so it never matches and is always
inserted.
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

#: Signature salt for the generated entry points. Raise it when this generator
#: changes — though the payload is signed by its own bytes as well, because the
#: dispatch chain changes with the objects the item installs.
TSQL_ENTRY_VERSION = 1

#: The error numbers Weaver's own generated code throws with. A refusal inside
#: this range is a *decision* — rows rejected, a stability threshold breached —
#: and reads as Failed; anything else could not be evaluated and reads as Error.
#: The Python side draws the same line with ``isinstance(exc, WeaverError)``.
WEAVER_ERROR_MIN = 51000
WEAVER_ERROR_MAX = 51999

#: What each entry point is called. Read as a sentence: ``exec _.[Load]``.
LOAD_ENTRY = "Load"
TEST_ENTRY = "Test"

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
    """Run the object's own procedure, and keep a failure rather than raising it.

    A refusal is an outcome to record, not a reason to leave no record: the
    caller learns what happened from the row and from the message returned at the
    end, and the estate's account of itself is complete either way.
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
        # One correlation identity per standalone call. A workflow is its rows,
        # and a call by hand is a workflow of one.
        "declare @weaver_workflow varchar(128) = cast(newid() as varchar(36));",
        "declare @weaver_log_sk varchar(128) = cast(newid() as varchar(36));",
        "declare @weaver_schema varchar(128) = null;",
        "declare @weaver_object varchar(128) = null;",
        "declare @weaver_error varchar(4000) = null;",
        "declare @weaver_error_number int = null;",
        "declare @weaver_result varchar(128) = null;",
        "declare @weaver_message varchar(4000) = null;",
    )


def _load_declarations() -> str:
    return _declarations(
        *_common_declarations(),
        "declare @weaver_statistic_sk varchar(128) = cast(newid() as varchar(36));",
        "declare @succeeded bit = null;",
        "declare @rows_read bigint = null;",
        "declare @rows_inserted bigint = null;",
        "declare @rows_updated bigint = null;",
        "declare @rows_deleted bigint = null;",
        "declare @rows_rejected bigint = null;",
        "declare @error_message varchar(4000) = null;",
        "declare @bookmark_datetime datetime2(6) = null;",
        "declare @is_static_skip bit = null;",
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
                "    , @succeeded = @succeeded output\n"
                "    , @rows_read = @rows_read output\n"
                "    , @rows_inserted = @rows_inserted output\n"
                "    , @rows_updated = @rows_updated output\n"
                "    , @rows_deleted = @rows_deleted output\n"
                "    , @rows_rejected = @rows_rejected output\n"
                "    , @error_message = @error_message output\n"
                "    , @bookmark_datetime = @bookmark_datetime output\n"
                "    , @is_static_skip = @is_static_skip output;",
            )
        )
    return "\n".join(branches) + "\n" + _unknown("loadable object")


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

    Refused rather than run, because the branches are what this Warehouse
    installs. A name it does not hold cannot be executed, and reporting a row for
    it would put an object in the estate's record the estate has never had.

    Remembered rather than thrown from here, because this sits inside the TRY
    that turns a refusal into a recorded outcome. It is raised again after the
    catch — see :func:`_refuse_unmatched`.
    """

    return (
        "else\n"
        "begin\n"
        f"    set @weaver_unmatched = concat(@object_name, "
        f"N' is not a {what} in this Warehouse');\n"
        "end;"
    )


def _refuse_unmatched() -> str:
    """Raise a name that matched nothing, having recorded nothing for it.

    The refusal is about the *request* rather than about a load: nothing ran, so
    there is no outcome, and there is no object to record one against. Every
    identity column is not null, so a row here would be refused by the catalogue
    anyway and would hide the message that says what went wrong.
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

    The public value, because that is what the column holds. Python writes the
    internal one and the catalogue's own renderer maps it at the persistence
    boundary; generated SQL has no such boundary to cross, so it writes what the
    column holds and the two agree.
    """

    from ..catalogue.tables import ROLE_ASSUMPTION, ROLE_TEST, TEST_STATUS

    internal = {TEST: ROLE_TEST, ASSUMPTION: ROLE_ASSUMPTION}[kind]
    return TEST_STATUS.column("test_type").to_public(internal)


# --- what happened, in the catalogue's own words -------------------------------


def _weaver_refusal() -> str:
    return f"@weaver_error_number between {WEAVER_ERROR_MIN} and {WEAVER_ERROR_MAX}"


def _load_result() -> str:
    """Map the lower procedure's outcome into the public Result vocabulary.

    The order is the order the cases exclude each other in. A refusal Weaver
    itself threw ran under Weaver's control and produced an unacceptable result,
    so it is Failed; anything else that threw could not be evaluated and is
    Error.
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

    A validation that could not be evaluated found nothing, and reporting zero
    discrepancies for it is the one answer a validation must never give — so its
    failure count is left null rather than defaulted to zero.
    """

    return (
        "set @weaver_completed = sysutcdatetime();\n"
        "set @weaver_message = @weaver_error;\n"
        "set @weaver_failure_count =\n"
        "    case when @weaver_error is not null then null\n"
        "         else coalesce(@missing_count, 0) + coalesce(@unexpected_count, 0)\n"
        "              + coalesce(@violation_count, 0) end;\n"
        "set @weaver_result =\n"
        f"    case when {_weaver_refusal()} then N'Failed'\n"
        "         when @weaver_error is not null then N'Error'\n"
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

    A MERGE even for an append, because in every Warehouse but the catalogue's
    own these tables are views across databases: Fabric refuses a plain INSERT
    through one and accepts a MERGE's. An appended row merges on a surrogate
    generated a moment ago, so it never matches and is always inserted.
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
    """One value per line, so a generated statement is read rather than scanned.

    The first line leads and the rest are continued, which is how T-SQL is
    conventionally laid out and what makes a diff of generated SQL legible.
    """

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
    a rejecting or failed load keeps the bookmark it had; and an instant
    reported, so a Static skip moves nothing.
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
