"""Installer orchestration — barriers, skipping, and faithful reporting.

These use a recording fake executor rather than Spark, so the sequencing and
reporting logic is pinned fast: sequences are barriers, a failure stops later
sequences, every planned action gets exactly one result, and the report is
persisted. Payload integrity and the real executors are covered elsewhere.
"""

from __future__ import annotations

from dataclasses import replace

import pytest

from weaver import LocalStore, Location
from weaver.build import (
    BoundTarget,
    BuildAction,
    BuildBatch,
    BuildPlan,
    BuildSequence,
    InstallationEnvironment,
    compute_bundle_id,
    install_bundle,
    load_bundle,
    write_bundle,
)
from weaver.build.report import FAILED, SKIPPED, SUCCEEDED
from weaver.errors import BuildError

TARGET = BoundTarget(id="lakehouse-Sales_LH", kind="lakehouse", host_kind="local", item_id="Sales_LH")


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


def _action(name: str) -> BuildAction:
    payload = f"payload/{name}/stmt.spark.sql"
    return BuildAction(
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
    )
    plan = replace(plan, bundle_id=compute_bundle_id(plan))

    store = LocalStore()
    location = Location(str(tmp_path / "bundle"))
    write_bundle(location, plan=plan, payloads=payloads, snapshot={}, store=store)
    return location, store


def test_successful_install_reports_every_action(tmp_path):
    location, store = _bundle(tmp_path)
    recorder = Recorder()
    env = InstallationEnvironment(store=store, resolver=None, executors={"spark_sql": recorder})

    report = install_bundle(load_bundle(location, store=store), environment=env)

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
    env = InstallationEnvironment(store=store, resolver=None, executors={"spark_sql": recorder})

    report = install_bundle(load_bundle(location, store=store), environment=env)

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
    env = InstallationEnvironment(store=store, resolver=None, executors={"spark_sql": Recorder()})

    report = install_bundle(load_bundle(location, store=store), environment=env)

    report_location = location.join("install-report.yml")
    assert store.exists(report_location)
    assert report.bundle_id in store.read(report_location).decode("utf-8")


def test_preflight_rejects_a_corrupt_bundle_before_running(tmp_path):
    location, store = _bundle(tmp_path)
    bundle = load_bundle(location, store=store)
    # Corrupt a payload after loading; install must refuse on its own preflight.
    store.write(location.join("payload", "a2", "stmt.spark.sql"), b"tampered\n")
    recorder = Recorder()
    env = InstallationEnvironment(store=store, resolver=None, executors={"spark_sql": recorder})

    with pytest.raises(BuildError, match="hash mismatch"):
        install_bundle(bundle, environment=env)
    assert recorder.calls == []  # nothing ran


def test_installing_against_a_non_local_host_is_refused(tmp_path):
    location, store = _bundle(tmp_path)
    bundle = load_bundle(location, store=store)
    fabric_plan = replace(
        bundle.plan, targets=(replace(TARGET, host_kind="fabric"),)
    )
    fabric_bundle = replace(bundle, plan=fabric_plan)
    env = InstallationEnvironment(store=store, resolver=None, executors={"spark_sql": Recorder()})

    with pytest.raises(NotImplementedError, match="fabric"):
        install_bundle(fabric_bundle, environment=env)
