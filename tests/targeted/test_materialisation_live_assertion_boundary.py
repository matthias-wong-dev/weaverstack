"""Exercise the Fabric journey's unchanged assertion with public report types."""

import ast
import inspect
from datetime import datetime, timezone

import pytest
from support.weaver_test import weaver_test

from weaver.build_bundle.report import ActionResult, InstallationReport, SequenceResult
from weaver.operations.build import BuildResult


@weaver_test()
@pytest.mark.parametrize("case", ["empty", "unrelated", "protected"])
@pytest.mark.parametrize("phase", ["unchanged", "repeat"])
def test_unchanged_control_consumes_reports_and_protects_pointer_identities(
    case, phase
):
    from fabric import test_protected_mirror_materialisation_journey as journey

    now = datetime.min.replace(tzinfo=timezone.utc)
    actions = (
        ()
        if case == "empty"
        else (
            ActionResult(
                "prune-unused-schema"
                if case == "unrelated"
                else "materialise-protected",
                None
                if case == "unrelated"
                else "Lakehouse/Sales/Files/Raw.CustomerCsv",
                "target",
                "spark_sql" if case == "unrelated" else "materialise_mirror",
                "succeeded",
            ),
        )
    )
    report = InstallationReport(
        "bundle",
        "succeeded",
        now,
        now,
        (SequenceResult(1, "inspection", "succeeded", actions),),
    )
    result = BuildResult(
        "source",
        ("Lakehouse/Sales",),
        "bundle",
        True,
        None,
        "succeeded",
        installation_report=report,
    )
    source = ast.parse(
        inspect.getsource(
            journey.test_protected_mirrors_copy_before_cutover_and_converge
        )
    )
    statements = [
        node
        for node in ast.walk(source)
        if isinstance(node, (ast.Assert, ast.Expr))
        and any(
            isinstance(part, ast.Name) and part.id == phase for part in ast.walk(node)
        )
    ]
    assert len(statements) == 1
    code = compile(
        ast.fix_missing_locations(ast.Module(body=statements, type_ignores=[])),
        "live-unchanged-assertion",
        "exec",
    )
    namespace = dict(journey.__dict__, **{phase: result})
    if case == "protected" or (phase == "repeat" and case != "empty"):
        with pytest.raises(AssertionError):
            exec(code, namespace)
    else:
        exec(code, namespace)
