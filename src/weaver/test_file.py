"""Compile and run a SQL validation source file without installing it.

The file path remains local while its content crosses to a Fabric session. File
runs use the same compilers and comparison path as an installed validation.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

from .declaration.metadata import ASSUMPTION, PYTHON, SPARK_SQL
from .declaration.model import LAKEHOUSE, WAREHOUSE
from .errors import CommandError, ValidationError
from .load_plan import LAKEHOUSE_TARGET, PhysicalTargetRef
from .runtime.validation_result import AssumptionResult, TestResult
from .test_execution import PYTHON_VALIDATION, WAREHOUSE_PROCEDURE
from .test_report import (
    FAILED,
    INVALID,
    PASSED,
    PLANNED,
    ValidationNodeReport,
)


def source_file_node(
    session: Any,
    *,
    requested: Sequence[PhysicalTargetRef],
    path: Path,
    started: datetime,
    dry_run: bool = False,
) -> ValidationNodeReport:
    """Compile one source file and run it against the requested target."""

    if len(requested) != 1:
        raise CommandError(
            "test file= runs one validation against one target, and "
            f"{len(requested)} were requested"
        )
    target = requested[0]
    if not path.exists():
        raise CommandError(f"no validation source at {path}")

    # Validate compilation before reporting a dry run.
    document = _read(path, target)
    if dry_run:
        return ValidationNodeReport(
            logical_id=document.object_id.qualified,
            kind=document.document.kind,
            physical_target=str(target),
            primitive_kind=PYTHON_VALIDATION
            if target.kind == LAKEHOUSE_TARGET
            else WAREHOUSE_PROCEDURE,
            dispatch_location=str(path),
            status=PLANNED,
            started_at=started.isoformat(),
            finished_at=datetime.now(timezone.utc).isoformat(),
        )
    return _execute(
        session, document=document, target=target, path=path, started=started
    )


def _read(path: Path, target: PhysicalTargetRef):
    """Parse a file through the ordinary validation reader.

    Source files receive the same structural checks as committed validations.
    """

    from .declaration import read_source_document

    item_type = LAKEHOUSE if target.kind == LAKEHOUSE_TARGET else WAREHOUSE
    directory = "tests"
    text = path.read_bytes()
    if b"Assumption ID:" in text:
        directory = "assumptions"
    document = read_source_document(
        f"{item_type}/_file/{directory}/{path.name}", text, item_type
    )
    if not document.is_validation:
        raise CommandError(
            f"{path} declares a {document.kind}, and test file= runs a Test or an "
            "Assumption"
        )
    if document.language == PYTHON:
        raise CommandError(
            f"{path} is Python, which test file= does not run. Import the class "
            "and call read() instead."
        )
    return document


def _execute(session, *, document, target, path: Path, started: datetime):
    common = {
        "logical_id": document.object_id.qualified,
        "kind": document.document.kind,
        "physical_target": str(target),
        "dispatch_location": str(path),
        "started_at": started.isoformat(),
    }
    try:
        if target.kind == LAKEHOUSE_TARGET:
            result, diagnostics = _run_spark(session, document, target)
            primitive = PYTHON_VALIDATION
        else:
            result, diagnostics = _run_warehouse(session, document, target)
            primitive = WAREHOUSE_PROCEDURE
    except Exception as exc:  # noqa: BLE001 - any failure is the run's evidence
        message = f"{type(exc).__name__}: {exc}"
        failed = (
            AssumptionResult.failed_to_run(message)
            if document.document.kind == ASSUMPTION
            else TestResult.failed_to_run(message)
        )
        return ValidationNodeReport(
            status=INVALID,
            primitive_kind=WAREHOUSE_PROCEDURE
            if target.kind != LAKEHOUSE_TARGET
            else PYTHON_VALIDATION,
            executed=True,
            messages=(message,),
            result=failed,
            finished_at=datetime.now(timezone.utc).isoformat(),
            **common,
        )

    return ValidationNodeReport(
        status=PASSED if result.succeeded else FAILED,
        primitive_kind=primitive,
        executed=True,
        result=result,
        diagnostics=diagnostics,
        finished_at=datetime.now(timezone.utc).isoformat(),
        **common,
    )


def _run_warehouse(session, document, target: PhysicalTargetRef):
    """Execute the generated batch directly, creating no procedure.

    A temporary stored procedure would leave something behind for a run whose
    whole promise is that it does not — and the batch is what the procedure's
    body is anyway, so nothing is lost by running it as itself.
    """

    from .declaration.tsql_validation import generate_tsql_validation_batch
    from .targets import ItemRef as _ItemRef
    from .targets import WarehouseTarget as _WarehouseTarget

    executor = session.sql_executor(_WarehouseTarget(_ItemRef(target.name)))
    if executor is None:
        raise ValidationError(
            f"{target} needs a SQL capability to run a validation, and this run "
            "has none"
        )
    batch = generate_tsql_validation_batch(document.document, document.sql_body or "")

    # Every result set, and the *last* is the counts. The batch returns the
    # diagnostic rows first and then projects its locals, exactly as the
    # installed procedure does through `call_procedure_with_results` — so
    # reading only the first set would read a diagnostic row, find no count
    # column on it, and report a failing Test as passing.
    produced = executor.query_result_sets(batch)
    row = produced[-1][0] if produced and produced[-1] else {}
    diagnostics = produced[0] if len(produced) > 1 else ()

    if document.document.kind == ASSUMPTION:
        return (
            AssumptionResult(violation_count=int(row.get("violation_count") or 0)),
            diagnostics,
        )
    return (
        TestResult(
            missing_count=int(row.get("missing_count") or 0),
            unexpected_count=int(row.get("unexpected_count") or 0),
        ),
        diagnostics,
    )


def _run_spark(session, document, target: PhysicalTargetRef):
    """Run the authored Spark SQL program through the same runtime a module uses."""

    from . import tokens
    from .lakehouse import lakehouse_for
    from .runtime.spark_sql_validation import (
        read_spark_sql_assumption,
        read_spark_sql_test,
    )
    from .runtime.test_compare import compare
    from .targets import ItemRef

    if session.spark() is None:
        raise ValidationError("running a Spark SQL validation needs a Spark session")
    if document.language != SPARK_SQL:
        raise CommandError(
            f"a {document.language} validation cannot run against {target}"
        )

    lakehouse = lakehouse_for(session.resolver(), ItemRef(target.name))
    # Addressed exactly as an installed module's program is, so a file run reads
    # the same tables the installed one would.
    sql = tokens.expand(_addressed(document.sql_body or ""), lakehouse.destination)
    what = document.object_id.qualified

    if document.document.kind == ASSUMPTION:
        frame = read_spark_sql_assumption(session.spark(), sql=sql, what=what)
        rows = tuple(row.asDict() for row in frame.collect())
        return AssumptionResult(violation_count=len(rows)), rows

    expected, actual = read_spark_sql_test(session.spark(), sql=sql, what=what)
    frame = compare(
        expected, actual, primary_key=document.document.primary_key, what=what
    )
    rows = tuple(row.asDict() for row in frame.collect())
    sides = [str(row["_weaver_side"]) for row in rows]
    return (
        TestResult(
            missing_count=sum(1 for side in sides if side == "expected"),
            unexpected_count=sum(1 for side in sides if side == "actual"),
        ),
        rows,
    )


def _addressed(body: str) -> str:
    from .declaration.spark_sql_module import addressed

    return addressed(body)


__all__ = ["source_file_node"]
