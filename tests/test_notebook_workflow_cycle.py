"""A named CLI workflow can execute natively without the optional CLI."""

from __future__ import annotations

from pathlib import Path
from shutil import copytree

from support.weaver_test import weaver_test

import weaver


@weaver_test()
def test_notebook_workflow_executes_a_real_source_check(tmp_path):
    project = tmp_path / "project"
    copytree(Path(__file__).parent / "fixtures" / "build-lakehouse-item", project)
    workflow = tmp_path / "workflow.yml"
    workflow.write_text(
        f"workflows:\n  verify:\n    - 'check \"{project.as_posix()}\"'\n",
        encoding="utf-8",
    )
    report = weaver.workflow("verify", file=workflow, workspace="Parcel Development")
    assert report.succeeded
    assert report.status == "succeeded"
    assert len(report.steps) == 1
    assert report.steps[0].result.project_folder == project.as_posix()


@weaver_test()
def test_notebook_workflow_continues_after_validation_findings(tmp_path, monkeypatch):
    from weaver.test_report import ValidationRunReport

    calls = []
    workflow = tmp_path / "workflow.yml"
    workflow.write_text(
        "workflows:\n  verify:\n    - test Warehouse/Operations\n    - check .\n",
        encoding="utf-8",
    )

    def failing_test(**kwargs):
        calls.append(("test", kwargs))
        return ValidationRunReport(status="failed")

    def source_check(**kwargs):
        calls.append(("check", kwargs))
        return weaver.CheckResult(".")

    monkeypatch.setattr(weaver, "test", failing_test)
    monkeypatch.setattr(weaver, "check", source_check)
    report = weaver.workflow("verify", file=workflow, workspace="Parcel Development")
    assert [call[0] for call in calls] == ["test", "check"]
    assert not report.succeeded
    assert [step.status for step in report.steps] == ["failed", "succeeded"]


@weaver_test()
def test_notebook_workflow_rejects_later_bad_requests_before_any_operation(
    tmp_path, monkeypatch
):
    import pytest

    from weaver.errors import CommandError

    workflow = tmp_path / "workflow.yml"
    workflow.write_text(
        "workflows:\n  verify:\n    - check .\n    - load --not-a-valid-parameter\n",
        encoding="utf-8",
    )
    calls = []
    monkeypatch.setattr(weaver, "check", lambda **kwargs: calls.append(kwargs))
    with pytest.raises(CommandError, match="not a valid Weaver command"):
        weaver.workflow("verify", file=workflow, workspace="Parcel Development")
    assert not calls


@weaver_test()
def test_notebook_workflow_refuses_conflicting_bindings_in_one_workspace(
    tmp_path, monkeypatch
):
    import pytest

    from weaver.errors import CommandError
    from weaver.load_report import LoadRunReport

    first = tmp_path / "first.yml"
    second = tmp_path / "second.yml"
    for path, physical in [(first, "Operations_One"), (second, "Operations_Two")]:
        path.write_text(
            f"workspace: Parcel Development\ntargets:\n  Warehouse/Operations: {physical}\n",
            encoding="utf-8",
        )
    definition = tmp_path / "workflow.yml"
    definition.write_text(
        f"workflows:\n  verify:\n    - 'load Warehouse/Operations --workspace-config \"{second.as_posix()}\"'\n",
        encoding="utf-8",
    )
    calls = []

    def load(items=None, *, workspace_config=None, session=None, **kwargs):
        calls.append(items)
        return LoadRunReport(
            requested=tuple(items or []),
            status="succeeded",
            dry_run=False,
            fault_tolerant=False,
        )

    monkeypatch.setattr(weaver, "load", load)
    with pytest.raises(CommandError, match="different workspace configurations"):
        weaver.workflow("verify", file=definition, workspace_config=first)
    assert not calls


@weaver_test()
def test_notebook_workflow_applies_its_catalogue_override_to_a_reused_session(
    tmp_path, monkeypatch
):
    from weaver.load_report import LoadRunReport

    definition = tmp_path / "workflow.yml"
    definition.write_text(
        "workflows:\n  verify:\n    - load Warehouse/Operations\n", encoding="utf-8"
    )
    calls = []

    def load(items=None, *, catalogue=None, session=None, **kwargs):
        calls.append(catalogue)
        return LoadRunReport(
            requested=tuple(items or []),
            status="succeeded",
            dry_run=False,
            fault_tolerant=False,
        )

    monkeypatch.setattr(weaver, "load", load)
    with weaver.session(
        workspace="Parcel Development", catalogue="Warehouse/Original"
    ) as opened:
        report = weaver.workflow(
            "verify",
            file=definition,
            workspace="Parcel Development",
            catalogue="Warehouse/Alternate",
            session=opened,
        )
        assert not opened.closed
    assert report.succeeded
    assert calls == ["Warehouse/Alternate"]


@weaver_test()
def test_notebook_workflow_uses_native_capacity_control_with_azure_credentials(
    tmp_path, monkeypatch
):
    from types import SimpleNamespace

    definition = tmp_path / "workflow.yml"
    definition.write_text(
        "workflows:\n  verify:\n    - fabric capacity status --resource-group parcel-development --capacity-name parceldev --subscription-id 00000000-0000-0000-0000-000000000000\n",
        encoding="utf-8",
    )
    chosen = SimpleNamespace(get_token=lambda *args: None)
    calls = []

    def capacity(
        action,
        *,
        resource_group,
        capacity_name,
        subscription_id=None,
        credential=None,
        client=None,
    ):
        calls.append((action, credential))
        return weaver.CapacityAction(action, capacity_name, "Paused")

    monkeypatch.setattr(weaver, "capacity", capacity)
    report = weaver.workflow(
        "verify", file=definition, workspace="Parcel Development", arm_credential=chosen
    )
    assert report.succeeded
    assert calls == [("status", chosen)]
