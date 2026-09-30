"""What an ordinary build leaves as evidence of what it did.

The build installs from a temporary bundle and deletes it. Its installation
report is kept elsewhere, and the result carries the action counts.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest
from factories import full_estate
from support.sessions import given_session
from support.weaver_test import weaver_test
from support.workspaces import WORKSPACE
from test_build_fixed_point_cycle import (
    LAKEHOUSE_TARGET_NAME,
    WAREHOUSE_TARGET_NAME,
    _bindings,
    installed_catalogue,
)
from test_build_fixed_point_cycle import build as generate_bundle

import weaver.build_bundle as build_bundle
from weaver.build_bundle import Installer, WarehouseBinding
from weaver.build_bundle.report import InstallationReport
from weaver.catalogue.state import Catalogue
from weaver.operations.build import _run_build
from weaver.store import FilesystemStore
from weaver.targets import ItemRef
from weaver.workspaces import Workspace

EXECUTORS = (
    "spark_sql",
    "spark_sql_batch",
    "spark_schema",
    "spark_table",
    "tsql",
    "folder",
    "shortcut",
    "tsql_batch",
    "sql_endpoint_refresh",
    "load_file",
    "runtime_state",
)


class Recording:
    """Runs nothing, and fails the ``fail_at``-th action it is given."""

    def __init__(self, name, calls, fail_at=None):
        self.name = name
        self.calls = calls
        self.fail_at = fail_at

    def execute(self, action, payload, context):
        self.calls.append(action.id)
        if self.fail_at is not None and len(self.calls) == self.fail_at:
            raise RuntimeError(f"{action.id} failed")
        return None


def _build(tmp_path, monkeypatch, *, catalogue, fail_at=None):
    repository = full_estate(tmp_path / "repo")
    calls: list[str] = []
    executors = {name: Recording(name, calls, fail_at) for name in EXECUTORS}
    bundles = []

    def generate(_repository, *, output, **_kwargs):
        bundles.append(
            generate_bundle(repository, Path(output.value).parent, catalogue=catalogue)
        )
        return bundles[-1]

    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
    monkeypatch.setattr(build_bundle, "catalogue_items_for_build", lambda *_a: ())
    monkeypatch.setattr(build_bundle, "read_build_state", lambda *_a, **_k: None)
    monkeypatch.setattr(build_bundle, "build_repository_bundle", generate)
    monkeypatch.setattr(
        build_bundle,
        "Installer",
        lambda session: Installer(session, executors=executors),
    )
    session = given_session(
        store=FilesystemStore(),
        lakehouses=(LAKEHOUSE_TARGET_NAME,),
        warehouses=(WAREHOUSE_TARGET_NAME, "Weaver_Control"),
    )
    result = _run_build(
        Workspace(workspace=WORKSPACE, catalogue="Warehouse/Weaver_Control"),
        session=session,
        repository=repository,
        source_store=FilesystemStore(),
        bindings=_bindings(),
        catalogue_binding=WarehouseBinding(
            ItemRef("Weaver_Control"), workspace_name=WORKSPACE
        ),
        bundle_only=False,
        bundle_path=None,
        source=".",
    )
    return result, bundles[0]


def _kept(result) -> InstallationReport:
    import yaml

    return InstallationReport.from_mapping(
        yaml.safe_load(Path(result.report_path).read_text(encoding="utf-8"))
    )


def _planned(bundle) -> list[str]:
    return [action.id for _sequence, _batch, action in bundle.plan.actions()]


@weaver_test()
def test_a_build_that_does_work_counts_it(tmp_path, monkeypatch):
    result, bundle = _build(tmp_path, monkeypatch, catalogue=Catalogue({}))
    planned = _planned(bundle)

    assert planned
    assert result.to_mapping()["actions"] == {
        "total": len(planned),
        "succeeded": len(planned),
        "skipped": 0,
        "failed": 0,
    }


@weaver_test()
def test_a_fixed_point_build_records_that_it_did_nothing(tmp_path, monkeypatch):
    repository = full_estate(tmp_path / "estate")

    result, bundle = _build(
        tmp_path, monkeypatch, catalogue=installed_catalogue(repository)
    )

    assert _planned(bundle) == []
    assert result.succeeded
    assert result.to_mapping()["actions"] == {
        "total": 0,
        "succeeded": 0,
        "skipped": 0,
        "failed": 0,
    }
    assert _kept(result).action_counts()["total"] == 0


@weaver_test()
def test_a_failed_build_keeps_what_ran_what_failed_and_what_never_ran(
    tmp_path, monkeypatch
):
    result, bundle = _build(tmp_path, monkeypatch, catalogue=Catalogue({}), fail_at=2)
    kept = _kept(result)
    statuses = {action.action_id: action.status for action in kept.action_results()}

    assert not result.succeeded
    assert {"succeeded", "failed", "skipped"} <= set(statuses.values())
    assert [error.action_id for error in result.errors] == [
        name for name, status in statuses.items() if status == "failed"
    ]
    assert kept.action_counts() == result.to_mapping()["actions"]


@pytest.mark.parametrize("fail_at", [None, 2])
@weaver_test()
def test_every_planned_action_has_exactly_one_result(tmp_path, monkeypatch, fail_at):
    result, bundle = _build(
        tmp_path, monkeypatch, catalogue=Catalogue({}), fail_at=fail_at
    )

    assert [action.action_id for action in _kept(result).action_results()] == (
        _planned(bundle)
    )


@weaver_test()
def test_the_report_outlives_the_temporary_bundle(tmp_path, monkeypatch):
    result, bundle = _build(tmp_path, monkeypatch, catalogue=Catalogue({}))

    assert not Path(bundle.location.value).exists()
    assert Path(result.report_path).name == "install-report.yml"
    assert _kept(result).to_mapping() == result.installation_report.to_mapping()
    assert result.to_mapping()["report_path"] == result.report_path
