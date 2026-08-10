"""Running installed validation primitives, and reporting what they said.

The sibling of :mod:`weaver.load_execution`, and much smaller than it for a
reason that is the whole point of the design: a validation reads and never
writes, so there is no staging, no reconciliation, no fault tolerance and no
rollback to think about. What is left is dispatch.

.. code-block:: text

    Warehouse   exec [_].[Test Sales.X] with output counts
    Lakehouse   import the deployed module, construct it, call read()

**One failure does not stop the rest.** A validation is read-only, so a Test
that failed has told you something and the next Test can still tell you
something else. Losing that evidence to an early exit is exactly what makes a
validation run less useful than running the queries by hand.

**Suppression is not an optimisation.** A whole-target run counts without ever
materialising the rows — ``@suppress_result_set = 1`` on a Warehouse, a count
that never collects on Spark — because diagnostic rows may be enormous and may
carry sensitive business data. A targeted run asks for them, and gets them from
the *same* execution as the counts, because running a Test twice compares data
that could have changed in between.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Sequence

from .declaration.metadata import ASSUMPTION
from .errors import ValidationError
from .etl import LOAD_ROOT
from .load_plan import LAKEHOUSE_TARGET
from .runtime.test_compare import ACTUAL, EXPECTED, SIDE_COLUMN
from .runtime.validation_result import AssumptionResult, TestResult
from .test_plan import InstalledValidation
from .test_report import FAILED, INVALID, PASSED, PLANNED, ValidationNodeReport

#: What a node calls the thing it dispatches. A small vocabulary deliberately:
#: it says how to reach the primitive, not what language somebody authored it
#: in, because the second is the declaration's business and is already recorded.
WAREHOUSE_PROCEDURE = "warehouse_procedure"
PYTHON_VALIDATION = "python_validation"


def run_installed_validation(
    validation: InstalledValidation,
    *,
    session,
    workspace=None,
    runtime_scope=None,
    collect_diagnostics: bool = False,
):
    """One installed validation, run through the Session that owns the engines.

    Returns the result the validation produced, with any diagnostic rows
    attached to it — a run's *status* is the Runner's to decide, from a result,
    exactly as it is for a load. What this owes the caller is the judgement the
    validation made, not an opinion about what that means for the run.
    """

    environment = _capabilities(session, workspace, runtime_scope)
    try:
        validation.require_installed()
        if primitive_kind(validation) == WAREHOUSE_PROCEDURE:
            result, diagnostics = _dispatch_warehouse(
                validation, environment, collect_diagnostics
            )
        else:
            result, diagnostics = _dispatch_python(
                validation, environment, collect_diagnostics
            )
    except Exception as exc:  # noqa: BLE001 - a check that could not run is evidence
        # Raised so the run knows nothing was evaluated, and carrying a result
        # of the *validation's* own kind so its reader gets the counts that
        # belong to it — a load result here would offer counts it does not have.
        message = f"{type(exc).__name__}: {exc}"
        failed = (
            AssumptionResult.failed_to_run(message)
            if validation.kind == ASSUMPTION
            else TestResult.failed_to_run(message)
        )
        raise ValidationError(message, result=failed) from exc
    # Beside the result rather than inside it: diagnostic rows carry whatever a
    # check selected, and a durable record of them would put data into the
    # estate's own evidence.
    return _WithDiagnostics(result, diagnostics)


class _WithDiagnostics:
    """A validation result, carrying the rows a caller asked to see.

    A wrapper rather than a field on the result: the result types are the
    validation runtime's, shared with the primitives that produce them, and
    diagnostics are a property of *this run* having been asked for them.
    """

    def __init__(self, result, diagnostics) -> None:
        self.result = result
        self.diagnostics = diagnostics

    @property
    def succeeded(self) -> bool:
        return self.result.succeeded

    def as_row(self) -> dict:
        return self.result.as_row()

    def __getattr__(self, name):
        return getattr(self.result, name)


def _capabilities(session, workspace, runtime_scope):
    """What the validation dispatchers read, taken from the Session that owns it."""

    from types import SimpleNamespace

    from .targets import ItemRef, WarehouseTarget

    return SimpleNamespace(
        resolver=session.resolver(workspace),
        spark=session.spark(workspace),
        runtime_scope=runtime_scope,
        sql_for=lambda target: session.sql_executor(
            WarehouseTarget(ItemRef(target.name)), workspace=workspace
        ),
    )


def primitive_kind(validation: InstalledValidation) -> str:
    """How this validation is reached, from where it is installed."""

    if validation.target.kind == LAKEHOUSE_TARGET:
        return PYTHON_VALIDATION
    return WAREHOUSE_PROCEDURE


# --- Warehouse ----------------------------------------------------------------


def _dispatch_warehouse(
    validation: InstalledValidation, environment: Any, collect: bool
):
    """Execute the installed procedure once, and read what it set.

    Once, whether or not the caller wanted rows. The transport keeps the result
    sets *and* the outputs from a single execution, so asking for evidence never
    costs a second run over data that may have moved.
    """

    from .declaration.tsql_validation import RESULT_PARAMETERS

    executor = environment.sql_for(validation.target)
    if executor is None:
        raise ValidationError(
            f"{validation.logical} runs in {validation.target}, and this run has "
            "no SQL capability for it"
        )
    procedure = _procedure_name(validation)
    outputs = RESULT_PARAMETERS[validation.kind]
    inputs = (("suppress_result_set", 0 if collect else 1),)

    if collect:
        produced = executor.call_procedure_with_results(
            procedure, inputs=inputs, outputs=outputs
        )
        row = produced.outputs
        diagnostics = produced.result_sets[0] if produced.result_sets else ()
    else:
        row = executor.call_procedure(procedure, inputs=inputs, outputs=outputs)
        diagnostics = None

    return _result_from(validation, row), diagnostics


def _procedure_name(validation: InstalledValidation) -> str:
    identity = validation.artefact.object_id
    return f"{_quote(identity.schema)}.{_quote(identity.object)}"


def _quote(name: str) -> str:
    return "[" + name.replace("]", "]]") + "]"


def _result_from(validation: InstalledValidation, row):
    if validation.kind == ASSUMPTION:
        return AssumptionResult(violation_count=int(row["violation_count"] or 0))
    return TestResult(
        missing_count=int(row["missing_count"] or 0),
        unexpected_count=int(row["unexpected_count"] or 0),
    )


# --- Lakehouse ----------------------------------------------------------------


def _dispatch_python(
    validation: InstalledValidation, environment: Any, collect: bool
):
    """Import the deployed module, construct it, and read it.

    The same import machinery a load uses, in the same isolated runtime context,
    because a validation sits in the same deployed tree and imports the same
    object modules its author wrote against.
    """

    from .lakehouse import lakehouse_for
    from .runtime.python_context import import_deployed_module
    from .targets import ItemRef

    if environment.spark is None:
        raise ValidationError(
            f"{validation.logical} needs a Spark session, and this run has none"
        )
    lakehouse = lakehouse_for(environment.resolver, ItemRef(validation.target.name))
    runtime_root = _join(lakehouse.files_root(), *LOAD_ROOT.split("/"))
    identity = validation.artefact.object_id
    relative = f"{identity.schema}/{identity.object}"
    within = (
        relative[len(LOAD_ROOT) + 1 :] if relative.startswith(LOAD_ROOT) else relative
    )
    expected = _class_name(validation)
    context = environment.runtime_scope.context_for(
        logical_item=validation.logical.item,
        physical_target=validation.target,
        runtime_root=runtime_root,
    )
    module = import_deployed_module(
        context, within, expected=expected, node_id=str(validation.logical)
    )
    frame = getattr(module, expected)(environment.spark, lakehouse=lakehouse).read()

    if validation.kind == ASSUMPTION:
        return AssumptionResult(violation_count=_count(frame)), (
            _collected(frame) if collect else None
        )

    # Evaluated once, either way. A collected run counts the rows it already
    # has; a suppressed run aggregates *by side* in one action rather than
    # counting each side separately — two counts are two evaluations of the
    # whole comparison, and between them the tables can move, so the two halves
    # of one Test's answer could describe different data.
    if collect:
        rows = _collected(frame)
        sides = [str(row["_weaver_side"]) for row in rows]
        return (
            TestResult(
                missing_count=sum(1 for side in sides if side == "expected"),
                unexpected_count=sum(1 for side in sides if side == "actual"),
            ),
            rows,
        )

    # One row per side at most, so this collects counts and never evidence.
    by_side = {
        str(row[SIDE_COLUMN]): int(row["count"])
        for row in frame.groupBy(SIDE_COLUMN).count().collect()
    }
    return (
        TestResult(
            missing_count=by_side.get(EXPECTED, 0),
            unexpected_count=by_side.get(ACTUAL, 0),
        ),
        None,
    )


def _count(frame: Any) -> int:
    return int(frame.count())


def _collected(frame: Any) -> tuple:
    return tuple(row.asDict() if hasattr(row, "asDict") else dict(row) for row in frame.collect())


def _class_name(validation: InstalledValidation) -> str:
    """``Sales__OrdersReconcile``, from the deployed module's own filename.

    From the file rather than from the logical ID, because the file is what was
    installed — and the two agree by construction, since one function computed
    the path from the ID.
    """

    name = validation.artefact.object_id.object
    return name[: -len(".py")] if name.endswith(".py") else name


def _join(root: str, *parts: str) -> str:
    """The same string join the load dispatcher uses.

    A string, not a ``Location``: ``files_root()`` answers as Python addresses
    it, and calling ``.join`` on that would be ``str.join`` — which silently
    interleaves the characters instead of appending a path segment.
    """

    return "/".join([str(root).rstrip("/"), *parts])


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


__all__ = [
    "PYTHON_VALIDATION",
    "WAREHOUSE_PROCEDURE",
    "run_installed_validation",
    "primitive_kind",
]
