"""Run validations from source against deployed objects, without a catalogue.

Each validation runs in its item's target from workspace configuration, or the
target of the item's own name, and a semantic Test's Expected source resolves
the same way. Runs use the same compilers and comparison as an installed
validation and record nothing.
"""

from __future__ import annotations

import glob
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

from .declaration.metadata import ASSUMPTION, PYTHON, SPARK_SQL
from .declaration.model import SEMANTIC_MODEL, WeaverDocumentId, WeaverItemId
from .errors import CommandError, ValidationError
from .runtime.validation_result import AssumptionResult, TestResult
from .targets import LAKEHOUSE_TARGET, PhysicalTargetRef
from .test_execution import PYTHON_VALIDATION, SEMANTIC_VALIDATION, WAREHOUSE_PROCEDURE
from .test_report import (
    FAILED,
    INVALID,
    PASSED,
    PLANNED,
    ValidationNodeReport,
)


@dataclass(frozen=True)
class SourceValidation:
    """A Test or Assumption read from source, and the item it validates."""

    item: WeaverItemId
    source: Any
    #: Where it was read, for the report.
    path: str

    @property
    def logical(self) -> WeaverDocumentId:
        return WeaverDocumentId.validation(self.item, self.source.object_id)

    @property
    def qualified(self) -> str:
        return self.source.object_id.qualified


def project_validations(project, items: Sequence[WeaverItemId]):
    """The validations ``project`` declares for ``items``, or for every item."""

    declared = {item.identity for item in project.repository.items}
    missing = [item for item in items if item not in declared]
    if missing:
        raise CommandError(
            ", ".join(map(str, missing))
            + f" {'are' if len(missing) > 1 else 'is'} not in the project folder "
            f"{project.location.value}"
        )
    from .catalogue.builtin import BUILTIN_ITEM

    wanted = set(items)
    return tuple(
        each
        for each in _declared(project)
        if each.item != BUILTIN_ITEM and (not wanted or each.item in wanted)
    )


def _declared(project) -> tuple[SourceValidation, ...]:
    return tuple(
        SourceValidation(identity.item, source, source.relative_path)
        for identity, source in sorted(
            project.repository.source_documents.items(), key=lambda pair: str(pair[0])
        )
        if source.is_validation
    )


def file_validations(files: Sequence[str], *, project, items: Sequence[WeaverItemId]):
    """The validations ``files`` select.

    Each value is a file, a directory or a glob pattern. A file the project
    folder declares runs against its own item, and a directory selects the
    validations the project declares beneath it. Any other file is read on its
    own and runs against the one item named.
    """

    found: dict[str, SourceValidation] = {}
    for value in files:
        if glob.has_magic(value):
            paths = sorted(Path(each) for each in glob.glob(value, recursive=True))
            if not paths:
                raise CommandError(f"no file matches {value!r}")
        else:
            paths = [Path(value)]
            if not paths[0].exists():
                raise CommandError(f"no validation source at {value}")
        for path in paths:
            for each in _validations_at(path, project=project, items=items):
                found.setdefault(each.path, each)
    selected = tuple(found.values())
    named = set(items)
    for each in selected:
        if named and each.item not in named:
            raise CommandError(
                f"{each.path} validates {each.item}, which is not among the items "
                "named. Name it too, or name no items"
            )
    return selected


def _validations_at(path: Path, *, project, items) -> tuple[SourceValidation, ...]:
    relative = _within(path, project)
    if relative is not None:
        declared = tuple(
            each
            for each in _declared(project)
            if each.path == relative
            or path.is_dir()
            and (not relative or each.path.startswith(relative + "/"))
        )
        if declared or path.is_dir():
            if not declared:
                raise CommandError(
                    f"no Test or Assumption in the project folder is under {path}"
                )
            return declared
    if path.is_dir():
        raise CommandError(
            f"{path} is not in the project folder {project.location.value}"
        )
    if len(items) != 1:
        raise CommandError(
            f"{path} is not a Test or Assumption in the project folder "
            f"{project.location.value}. To run it on its own, name the one item "
            "to run it against"
        )
    return (SourceValidation(items[0], _read(path, items[0]), str(path)),)


def _within(path: Path, project) -> str | None:
    """``path`` relative to a local project folder, or ``None`` outside it."""

    if project.location.is_url:
        return None
    root = Path(project.location.value).resolve()
    try:
        relative = path.resolve().relative_to(root)
    except ValueError:
        return None
    return "" if relative == Path(".") else relative.as_posix()


def _read(path: Path, item: WeaverItemId):
    """Parse one file with the structural checks for committed validations."""

    from .declaration import read_source_document

    directory = "assumptions" if b"Assumption ID:" in path.read_bytes() else "tests"
    document = read_source_document(
        f"{item.item_type}/_file/{directory}/{path.name}",
        path.read_bytes(),
        item.item_type,
    )
    if not document.is_validation:
        raise CommandError(
            f"{path} declares a {document.kind}, and test runs a Test or an Assumption"
        )
    return document


def source_validation_nodes(
    session: Any,
    *,
    workspace,
    validations: Sequence[SourceValidation],
    started: datetime,
    dry_run: bool = False,
    collect: bool = False,
) -> tuple[ValidationNodeReport, ...]:
    """Run each validation in turn and report it."""

    from .operations.items import uncatalogued_target

    nodes = []
    for validation in validations:
        target = uncatalogued_target(workspace, validation.item)
        common = {
            "logical_id": str(validation.logical),
            "kind": validation.source.kind,
            "physical_target": str(target),
            "primitive_kind": _primitive(validation, target),
            "dispatch_location": validation.path,
            "started_at": started.isoformat(),
        }
        if dry_run:
            nodes.append(
                ValidationNodeReport(
                    status=PLANNED,
                    finished_at=datetime.now(timezone.utc).isoformat(),
                    **common,
                )
            )
            continue
        nodes.append(
            _execute(
                session,
                workspace=workspace,
                validation=validation,
                target=target,
                collect=collect,
                common=common,
            )
        )
    return tuple(nodes)


def _primitive(validation: SourceValidation, target: PhysicalTargetRef) -> str:
    if validation.item.item_type == SEMANTIC_MODEL:
        return SEMANTIC_VALIDATION
    if target.kind == LAKEHOUSE_TARGET:
        return PYTHON_VALIDATION
    return WAREHOUSE_PROCEDURE


def _execute(session, *, workspace, validation, target, collect, common):
    document = validation.source
    try:
        if document.language == PYTHON:
            raise CommandError(
                f"{validation.path} is Python, which runs only once installed. Build "
                f"{validation.item} and test it, or import the class and call read()"
            )
        if validation.item.item_type == SEMANTIC_MODEL:
            result, diagnostics = _run_semantic(session, workspace, validation, target)
        elif target.kind == LAKEHOUSE_TARGET:
            result, diagnostics = _run_spark(session, document, target)
        else:
            result, diagnostics = _run_warehouse(session, document, target)
    except Exception as exc:  # noqa: BLE001 - any failure is the run's evidence
        message = f"{type(exc).__name__}: {exc}"
        failed = (
            AssumptionResult.failed_to_run(message)
            if document.kind == ASSUMPTION
            else TestResult.failed_to_run(message)
        )
        return ValidationNodeReport(
            status=INVALID,
            executed=True,
            messages=(message,),
            result=failed,
            finished_at=datetime.now(timezone.utc).isoformat(),
            **common,
        )

    return ValidationNodeReport(
        status=PASSED if result.succeeded else FAILED,
        executed=True,
        result=result,
        diagnostics=tuple(diagnostics or ()) if collect else None,
        finished_at=datetime.now(timezone.utc).isoformat(),
        **common,
    )


def _run_semantic(session, workspace, validation: SourceValidation, target):
    """Run a DAX validation as an installed one runs, from its source definition."""

    from .catalogue.projection import semantic_test_definition
    from .catalogue.semantic import json_text
    from .operations.items import uncatalogued_target
    from .semantic_validation import run_semantic_validation
    from .test_plan import InstalledValidation

    document = validation.source.document
    expected = document.expected_source
    installed = InstalledValidation(
        logical=validation.logical,
        kind=document.kind,
        target=target,
        artefact=None,
        primary_key=tuple(document.primary_key or ()),
        definition=json_text(semantic_test_definition(validation.source)),
        bound_item=session.resolve_item(
            target.name, item_type=SEMANTIC_MODEL, workspace=workspace
        ),
        expected_target=None
        if expected is None
        else uncatalogued_target(workspace, WeaverItemId.parse(expected)),
    )
    return run_semantic_validation(
        installed, session=session, workspace=workspace, collect=True
    )


def _run_warehouse(session, document, target: PhysicalTargetRef):
    """Run the generated batch directly so no procedure is left behind."""

    from .declaration.tsql_validation import generate_tsql_validation_batch
    from .targets import ItemRef as _ItemRef
    from .targets import WarehouseTarget as _WarehouseTarget

    executor = session.sql_executor(_WarehouseTarget(_ItemRef(target.name)))
    if executor is None:
        raise ValidationError(
            f"a SQL capability is required to run this validation against {target}"
        )
    batch = generate_tsql_validation_batch(document.document, document.sql_body or "")

    # Every result set, and the last is the counts. The batch returns the
    # diagnostic rows first and then projects its locals, exactly as the
    # installed procedure does through `call_procedure_with_results`, so
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
    """Run the Spark SQL program through the same runtime as an installed module."""

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


__all__ = [
    "SourceValidation",
    "file_validations",
    "project_validations",
    "source_validation_nodes",
]
