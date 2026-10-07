import json
from dataclasses import replace
from types import SimpleNamespace

import pytest
from support.weaver_test import weaver_test
from test_report_definition_representation import BINDING, contribution

from weaver.report_definition import encode_report


@weaver_test()
def test_qualification_runner_restores_all_definitions_after_failed_public_build(
    tmp_path, monkeypatch
):
    from support.powerbi_qualification import qualify
    from test_report_build_cycle import prepared_project

    root, session, _, model, report = prepared_project(tmp_path)
    original_report = encode_report(replace(contribution(), binding=BINDING))
    report.definition = original_report
    original_model = model.definition
    writes = []
    model.update_definition = lambda definition, **options: writes.append(
        ("model", definition, options)
    )
    report.update_definition = lambda definition, **options: writes.append(
        ("report", definition, options)
    )
    from datetime import datetime, timezone

    from weaver.build_bundle.report import InstallationReport
    from weaver.operations.build import BuildResult

    failed = BuildResult(
        source=str(root),
        items=(),
        bundle_id="offline",
        installation=True,
        bundle_path=None,
        status="failed",
        installation_report=InstallationReport(
            "offline", "failed", datetime.now(timezone.utc), None, ()
        ),
    )
    monkeypatch.setattr("weaver.build", lambda *a, **k: failed)
    with pytest.raises(AssertionError, match="baseline"):
        qualify(
            source=root,
            output=tmp_path / "evidence",
            session=session,
            model="SemanticModel/Reporting",
            model_target="Reporting_Dev",
            reports={"Report/Executive": "Executive_Dev"},
        )
    assert writes == [
        ("model", original_model, {"allow_purge_data": True, "timeout": 900}),
        ("report", original_report, {"timeout": 900}),
    ]
    assert json.loads((tmp_path / "evidence/cleanup.json").read_text())["restored"]
    assert (
        json.loads((tmp_path / "evidence/baseline.json").read_text())["status"]
        == "failed"
    )


@weaver_test()
def test_qualification_cleanup_uses_keyword_scope_and_preserves_existing_log_ids(
    tmp_path,
):
    from support.powerbi_qualification import cleanup_catalogue

    from weaver.catalogue.render import InstallationScope

    class Connection:
        def __init__(self):
            self.statements = []

        def columns_of(self, table):
            return {c.public_name.casefold(): c.public_name for c in table.columns}

        def rows(self, sql):
            if "FROM [_].[Log]" in sql:
                return [{"log_sk": 10}, {"log_sk": 11}]
            return []

        def execute(self, sql):
            self.statements.append(sql)

    connection = Connection()
    cleanup_catalogue(
        connection,
        (InstallationScope("Report", "Executive"),),
        "[Target name] = 'Executive_Dev'",
        {10},
    )
    sql = "\n".join(connection.statements)
    assert "[Item type] = N'Report'" in sql and "[Item name] = N'Executive'" in sql
    assert "[Log SK] = 11" in sql and "[Log SK] = 10" not in sql


@weaver_test()
def test_qualification_runner_exercises_actual_catalogue_free_build_results_and_plan_fields(
    tmp_path,
):
    from support.powerbi_qualification import qualify
    from test_report_build_cycle import prepared_project

    root, session, _, _, report = prepared_project(tmp_path)
    report.definition = encode_report(replace(contribution(), binding=BINDING))
    qualify(
        source=root,
        output=tmp_path / "evidence",
        session=session,
        model="SemanticModel/Reporting",
        model_target="Reporting_Dev",
        reports={"Report/Executive": "Executive_Dev"},
    )
    output = tmp_path / "evidence"
    for phase in ("baseline", "catalogue-free-repeat"):
        assert (
            json.loads((output / f"{phase}.json").read_text())["status"] == "succeeded"
        )
        plan = json.loads((output / f"{phase}-plan.json").read_text())
        assert plan["targets"][0]["workspace_id"]
    assert json.loads((output / "qualification.json").read_text())["qualified"]


@weaver_test()
def test_qualification_lifecycle_edits_trial_and_checks_actual_selection_shape(
    tmp_path,
):
    from support.powerbi_qualification import exercise_catalogue
    from test_report_build_cycle import prepared_project

    from weaver.declaration.model import WeaverDocumentId, WeaverItemId

    root, *_ = prepared_project(tmp_path)
    model = WeaverDocumentId.model_root(WeaverItemId("SemanticModel", "Reporting"))
    report = WeaverDocumentId.report_root(WeaverItemId("Report", "Executive"))
    calls = []

    from weaver.build_bundle.incremental import BuildSelection, Impact
    from weaver.operations.build import BuildResult

    def phase(name, *, items=None):
        calls.append((name, items))
        selected = {
            "unchanged": (model, report),
            "report-unchanged": (),
            "report-edit": (report,),
            "report-fixed": (),
            "model-edit": (model, report),
            "model-fixed": (model, report),
        }[name]
        return BuildResult(
            source=str(root),
            items=tuple(items or ()),
            bundle_id="offline",
            installation=True,
            bundle_path=None,
            status="succeeded",
            selection=BuildSelection(Impact((), (), ()), (), (), selected),
        )

    exercise_catalogue(
        root, "SemanticModel/Reporting", {"Report/Executive": "Executive_Dev"}, phase
    )
    report_items = ["Report/Executive=Report/Executive_Dev"]
    assert calls == [
        ("unchanged", None),
        ("report-unchanged", report_items),
        ("report-edit", report_items),
        ("report-fixed", report_items),
        ("model-edit", None),
        ("model-fixed", None),
    ]
    page = json.loads(
        (root / "PowerBI/Reporting/Executive.Report/definition/report.json").read_text()
    )
    assert page["settings"]["useStylableVisualContainerHeader"]
    assert (
        "Stage 3 qualification model edit"
        in (root / "PowerBI/Reporting/Reporting.tmdl").read_text()
    )


@weaver_test()
def test_qualification_cli_imports_under_src_only_pythonpath():
    import os
    import subprocess
    import sys
    from pathlib import Path

    root = Path(__file__).parents[1]
    result = subprocess.run(
        [sys.executable, str(root / "tools/qualify_powerbi_ownership.py"), "--help"],
        env={**os.environ, "PYTHONPATH": str(root / "src")},
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert "--report" in result.stdout and "--provision" in result.stdout


@weaver_test()
def test_qualification_readback_checks_report_claims_binding_and_table_order():
    from support.powerbi_qualification import verify_published_rows

    from weaver.catalogue.powerbi import project_report
    from weaver.declaration.model import WeaverItemId

    report = contribution()
    bound = replace(report, binding=BINDING)
    rows = {
        "SemanticModel/Reporting": {
            "SemanticModelTable": [{"table_name": "Calendar", "table_ordinal": 1}]
        },
        "Report/Executive": {
            **project_report(WeaverItemId("Report", "Executive"), bound),
            "Installation": [{**BINDING, "signature": bound.signature}],
        },
    }
    repository = SimpleNamespace(
        semantic_models={
            WeaverItemId("SemanticModel", "Reporting"): SimpleNamespace(
                table_names=("Calendar",)
            )
        },
        reports={WeaverItemId("Report", "Executive"): report},
    )
    verify_published_rows(rows, repository, "SemanticModel/Reporting", BINDING)
    rows["Report/Executive"]["Dependency"] = []
    with pytest.raises(AssertionError, match="Dependency"):
        verify_published_rows(rows, repository, "SemanticModel/Reporting", BINDING)


@weaver_test()
def test_qualification_refuses_active_model_refresh_before_build(tmp_path, monkeypatch):
    from support.powerbi_qualification import qualify
    from test_report_build_cycle import prepared_project

    root, session, _, model, report = prepared_project(tmp_path)
    report.definition = encode_report(replace(contribution(), binding=BINDING))
    model.power_bi = SimpleNamespace(
        get_json=lambda path: {"value": [{"status": "Unknown"}]}
    )
    monkeypatch.setattr(
        "weaver.build", lambda *a, **k: pytest.fail("active refresh reached Build")
    )
    with pytest.raises(AssertionError, match="refresh"):
        qualify(
            source=root,
            output=tmp_path / "evidence",
            session=session,
            model="SemanticModel/Reporting",
            model_target="Reporting_Dev",
            reports={"Report/Executive": "Executive_Dev"},
        )
    assert not model.calls
