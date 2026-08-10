"""Installer orchestration — barriers, skipping, and faithful reporting.

These use a recording fake executor rather than Spark, so the sequencing and
reporting logic is pinned fast: sequences are barriers, a failure stops later
sequences, every planned action gets exactly one result, and the report is
persisted. Payload integrity and the real executors are covered elsewhere.
"""

from __future__ import annotations

from dataclasses import replace

import pytest

from weaver.resolution import LocalResolver
from weaver.store import FilesystemStore
from weaver.workspaces import LocalWorkspace
from weaver.locations import Location
from weaver.build_bundle import (
    BoundTarget,
    InstallAction,
    BuildBatch,
    BuildPlan,
    BuildSequence,
    BuildSelection,
    Impact,
    compute_bundle_id,
    load_bundle,
    write_bundle,
)
from support.sessions import given_installer

from weaver.build_bundle.report import FAILED, SKIPPED, SUCCEEDED
from weaver.errors import BuildError

TARGET = BoundTarget(id="lakehouse-Sales_LH", kind="lakehouse", item_id="Sales_LH")


class Recorder:
    """A stand-in executor that records calls and fails on named actions."""

    name = "spark_sql"

    def __init__(self, fail_on=()):
        self.calls: list[str] = []
        self.fail_on = set(fail_on)

    def execute(self, action, payload, context):
        self.calls.append(action.id)
        if action.id in self.fail_on:
            raise RuntimeError(f"boom {action.id}")
        return {"ran": action.id}


def _action(name: str) -> InstallAction:
    payload = f"payload/{name}/stmt.spark.sql"
    return InstallAction(
        id=name,
        kind="materialise",
        resource_node_id=None,
        executor="spark_sql",
        payload=payload,
        payload_sha256=None,  # filled by _bundle
    )


def _bundle(tmp_path):
    """A three-sequence bundle, one spark_sql action each."""

    import hashlib

    actions = [_action("a1"), _action("a2"), _action("a3")]
    payloads = {}
    filled = []
    for index, action in enumerate(actions):
        data = f"select {index}\n".encode("utf-8")
        payloads[action.payload] = data
        filled.append(replace(action, payload_sha256=hashlib.sha256(data).hexdigest()))

    sequences = tuple(
        BuildSequence(
            number=(index + 1) * 10,
            description=f"step {index}",
            batches=(BuildBatch(id=f"b{index}", target_id=TARGET.id, actions=(action,)),),
        )
        for index, action in enumerate(filled)
    )
    plan = BuildPlan(
        format_version=1,
        bundle_id="",
        repository_name="MyRepo",
        repository_signature="sig",
        targets=(TARGET,),
        sequences=sequences,
        selection=BuildSelection(Impact((), (), ()), (), (), ()),
    )
    plan = replace(plan, bundle_id=compute_bundle_id(plan))

    store = FilesystemStore()
    location = Location(str(tmp_path / "bundle"))
    write_bundle(location, plan=plan, payloads=payloads, store=store)
    return location, store


def test_successful_install_reports_every_action(tmp_path):
    location, store = _bundle(tmp_path)
    recorder = Recorder()
    installer = given_installer(store=store, executors={"spark_sql": recorder})

    report = installer.install(load_bundle(location, store=store))

    assert report.status == SUCCEEDED
    assert recorder.calls == ["a1", "a2", "a3"]
    results = list(report.action_results())
    assert [r.action_id for r in results] == ["a1", "a2", "a3"]
    assert all(r.status == SUCCEEDED for r in results)
    # Each result stays with its batch's target.
    assert all(r.target_id == TARGET.id for r in results)


def test_a_failure_stops_later_sequences_and_is_reported(tmp_path):
    location, store = _bundle(tmp_path)
    recorder = Recorder(fail_on={"a2"})
    installer = given_installer(store=store, executors={"spark_sql": recorder})

    report = installer.install(load_bundle(location, store=store))

    assert report.status == FAILED
    # a3 never ran: its sequence was never started.
    assert recorder.calls == ["a1", "a2"]
    by_id = {r.action_id: r for r in report.action_results()}
    assert by_id["a1"].status == SUCCEEDED
    assert by_id["a2"].status == FAILED
    assert by_id["a2"].error_type == "RuntimeError"
    assert "boom a2" in by_id["a2"].error_message
    assert by_id["a3"].status == SKIPPED


def test_report_is_persisted_beside_the_plan(tmp_path):
    location, store = _bundle(tmp_path)
    installer = given_installer(store=store, executors={"spark_sql": Recorder()})

    report = installer.install(load_bundle(location, store=store))

    report_location = location.join("install-report.yml")
    assert store.exists(report_location)
    assert report.bundle_id in store.read(report_location).decode("utf-8")


def test_preflight_rejects_a_corrupt_bundle_before_running(tmp_path):
    location, store = _bundle(tmp_path)
    bundle = load_bundle(location, store=store)
    # Corrupt a payload after loading; install must refuse on its own preflight.
    store.write(location.join("payload", "a2", "stmt.spark.sql"), b"tampered\n")
    recorder = Recorder()
    installer = given_installer(store=store, executors={"spark_sql": recorder})

    with pytest.raises(BuildError, match="hash mismatch"):
        installer.install(bundle)
    assert recorder.calls == []  # nothing ran


def test_installer_does_not_infer_refreshes_absent_from_the_bundle(tmp_path):
    class Resolver:
        def lakehouse_spark_location(self, _item):
            return None

        def spark_destination(self, _item):
            return None

        def refresh_sql_endpoint(self, _item):
            pytest.fail("the installer must not infer an endpoint refresh")

    location, store = _bundle(tmp_path)
    report = given_installer(
        store=store, resolver=Resolver(), executors={"spark_sql": Recorder()}
    ).install(load_bundle(location, store=store))

    assert report.status == SUCCEEDED


def test_local_endpoint_refresh_is_recorded_as_skipped_without_failing(tmp_path):
    action = InstallAction(
        id="refresh-application-sql-endpoint",
        kind="refresh_sql_endpoint",
        resource_node_id=None,
        executor="sql_endpoint_refresh",
        payload=None,
        payload_sha256=None,
    )
    sequence = BuildSequence(
        number=8990,
        description="refresh affected application Lakehouse SQL endpoints",
        batches=(BuildBatch(id="refresh", target_id=TARGET.id, actions=(action,)),),
    )
    plan = BuildPlan(
        format_version=1,
        bundle_id="",
        repository_name="MyRepo",
        repository_signature="sig",
        targets=(TARGET,),
        sequences=(sequence,),
        selection=BuildSelection(Impact((), (), ()), (), (), ()),
    )
    plan = replace(plan, bundle_id=compute_bundle_id(plan))
    store = FilesystemStore()
    location = Location(str(tmp_path / "refresh-bundle"))
    bundle = write_bundle(
        location, plan=plan, payloads={}, store=store
    )
    workspace = LocalWorkspace(
        workspace=tmp_path / "local", weaver_lakehouse="Weaver"
    )

    report = given_installer(
        workspace=workspace, store=store, resolver=LocalResolver(workspace)
    ).install(bundle)

    assert report.status == SUCCEEDED
    assert report.sequences[0].status == SKIPPED
    result = next(report.action_results())
    assert result.status == SKIPPED
    assert "unsupported" in result.details["reason"]
