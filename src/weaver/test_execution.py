"""Run installed validation primitives and report their results.

Targeted runs collect diagnostics; whole-target runs collect counts only. A
failed validation does not prevent the remaining validations from running.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .declaration.metadata import ASSUMPTION
from .errors import ValidationError
from .etl import LOAD_ROOT
from .runtime.validation_result import (
    AssumptionResult,
    TestResult,
    result_from_rows,
)
from .targets import LAKEHOUSE_TARGET
from .test_plan import InstalledValidation

#: Runtime primitives for installed validations.
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
    """Run one installed validation through the Session.

    Returns the result the validation produced, with any diagnostic rows
    attached. A run's status is the Runner's to decide from that result, as it
    is for a load.
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
        # Preserve the validation's result kind so its reader gets that kind's
        # counts. A load result would expose counts the validation does not have.
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
    """Diagnostics belong to this invocation, not the durable runtime result."""

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


class _Capabilities:
    """Spark is acquired lazily so a Warehouse validation reached over TDS does not
    require a Spark session.
    """

    def __init__(self, session, workspace, runtime_scope) -> None:
        self._session = session
        self._workspace = workspace
        self.resolver = session.resolver(workspace)
        self.runtime_scope = runtime_scope

    @property
    def spark(self):
        return self._session.spark(self._workspace)

    def sql_for(self, target):
        from .targets import ItemRef, WarehouseTarget

        return self._session.sql_executor(
            WarehouseTarget(ItemRef(target.name)), workspace=self._workspace
        )


def _capabilities(session, workspace, runtime_scope):
    return _Capabilities(session, workspace, runtime_scope)


def primitive_kind(validation: InstalledValidation) -> str:
    if validation.target.kind == LAKEHOUSE_TARGET:
        return PYTHON_VALIDATION
    return WAREHOUSE_PROCEDURE


# --- Warehouse ----------------------------------------------------------------


def _dispatch_warehouse(
    validation: InstalledValidation, environment: Any, collect: bool
):
    """Execute the installed procedure once, and read what it set.

    Once, whether or not the caller wanted rows: the transport keeps the result
    sets and the outputs from one execution, so asking for evidence never costs
    a second run over data that may have moved.
    """

    from .declaration.tsql_validation import RESULT_PARAMETERS

    executor = environment.sql_for(validation.target)
    if executor is None:
        raise ValidationError(
            f"no SQL capability is available for {validation.logical} in "
            f"{validation.target}"
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


def _dispatch_python(validation: InstalledValidation, environment: Any, collect: bool):
    """Import the deployed module, construct it, and read it.

    The same import machinery a load uses, in the same isolated context: a
    validation sits in the same deployed tree and imports the same object
    modules its author wrote against.
    """

    from .lakehouse import lakehouse_for
    from .runtime.python_context import import_deployed_module
    from .targets import ItemRef

    if environment.spark is None:
        raise ValidationError(
            f"{validation.logical} needs a Spark session, but none is available"
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

    # What the rows mean is the validation runtime's, not this module's: a direct
    # call reaches the same implementation, so a Test counts the same either way.
    return result_from_rows(frame, kind=validation.kind, collect=collect)


def _class_name(validation: InstalledValidation) -> str:
    """``Sales__OrdersReconcile``, from the installed module's filename."""

    name = validation.artefact.object_id.object
    return name[: -len(".py")] if name.endswith(".py") else name


def _join(root: str, *parts: str) -> str:
    """The same string join the load dispatcher uses.

    A string, not a ``Location``: ``files_root()`` answers as Python addresses
    it, so ``.join`` on it would be ``str.join``, interleaving characters rather
    than appending a segment.
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
