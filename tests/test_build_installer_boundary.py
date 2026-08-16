"""Installer orchestration — barriers, skipping, and faithful reporting.

These use a recording fake executor rather than Spark, so the sequencing and
reporting logic is pinned fast: sequences are barriers, a failure stops later
sequences, every planned action gets exactly one result, and the report is
persisted. Payload integrity and the real executors are covered elsewhere.
"""

from __future__ import annotations

from dataclasses import replace

import pytest
from support.sessions import given_installer
from support.weaver_test import weaver_test
from support.workspaces import given_resolver, given_workspace

from weaver.build_bundle import (
    BoundTarget,
    BuildBatch,
    BuildPlan,
    BuildSelection,
    BuildSequence,
    Impact,
    InstallAction,
    compute_bundle_id,
    load_bundle,
    write_bundle,
)
from weaver.build_bundle.bundle import SUPPORTED_FORMAT_VERSION
from weaver.build_bundle.report import FAILED, SKIPPED, SUCCEEDED
from weaver.errors import BuildError
from weaver.locations import Location
from weaver.store import FilesystemStore

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
            batches=(
                BuildBatch(id=f"b{index}", target_id=TARGET.id, actions=(action,)),
            ),
        )
        for index, action in enumerate(filled)
    )
    plan = BuildPlan(
        format_version=SUPPORTED_FORMAT_VERSION,
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


@weaver_test()
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


@weaver_test()
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


@weaver_test()
def test_report_is_persisted_beside_the_plan(tmp_path):
    location, store = _bundle(tmp_path)
    installer = given_installer(store=store, executors={"spark_sql": Recorder()})

    report = installer.install(load_bundle(location, store=store))

    report_location = location.join("install-report.yml")
    assert store.exists(report_location)
    assert report.bundle_id in store.read(report_location).decode("utf-8")


@weaver_test()
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


@weaver_test()
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


@weaver_test()
def test_an_endpoint_refresh_a_host_cannot_perform_is_skipped_not_failed(tmp_path):
    """Inside a Fabric session there is no REST client to refresh with.

    The refresh is a workspace operation, and a notebook resolver reaches the
    workspace through NotebookUtils rather than REST — so it offers no refresh
    and the action is recorded as skipped. A desktop resolver performs it. The
    plan is the same either way, which is what keeps the decision in the
    Builder and out of the host.
    """

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
        format_version=SUPPORTED_FORMAT_VERSION,
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
    bundle = write_bundle(location, plan=plan, payloads={}, store=store)
    workspace = given_workspace(catalogue="Warehouse/Weaver")

    class WithoutRefresh:
        """A resolver that resolves, and cannot refresh an endpoint."""

        def __init__(self, inner):
            self._inner = inner

        def __getattr__(self, name):
            if name == "refresh_sql_endpoint":
                raise AttributeError(name)
            return getattr(self._inner, name)

    report = given_installer(
        workspace=workspace,
        store=store,
        resolver=WithoutRefresh(given_resolver(workspace=workspace)),
    ).install(bundle)

    assert report.status == SUCCEEDED
    assert report.sequences[0].status == SKIPPED
    result = next(report.action_results())
    assert result.status == SKIPPED
    assert "unsupported" in result.details["reason"]


# --- capabilities are acquired by need, not by batch --------------------------


@weaver_test()
def test_an_install_that_needs_no_spark_never_starts_one(tmp_path):
    """A Spark session costs seconds to start and a JVM permits exactly one.

    A batch is handed its capabilities up front, and building Spark eagerly
    meant a bundle of file writes, T-SQL and an endpoint refresh still started
    one — paying for a capability none of its actions would touch and, in a
    process that already had a session, failing outright with *Only one
    SparkContext should be running in this JVM*.
    """

    action = InstallAction(
        id="refresh-application-sql-endpoint",
        kind="refresh_sql_endpoint",
        resource_node_id=None,
        executor="sql_endpoint_refresh",
        payload=None,
        payload_sha256=None,
    )
    plan = BuildPlan(
        format_version=SUPPORTED_FORMAT_VERSION,
        bundle_id="",
        repository_name="MyRepo",
        repository_signature="sig",
        targets=(TARGET,),
        sequences=(
            BuildSequence(
                number=8990,
                description="refresh endpoints",
                batches=(
                    BuildBatch(id="refresh", target_id=TARGET.id, actions=(action,)),
                ),
            ),
        ),
        selection=BuildSelection(Impact((), (), ()), (), (), ()),
    )
    plan = replace(plan, bundle_id=compute_bundle_id(plan))
    store = FilesystemStore()
    bundle = write_bundle(
        Location(str(tmp_path / "refresh-bundle")), plan=plan, payloads={}, store=store
    )
    workspace = given_workspace(catalogue="Warehouse/Weaver")

    installer = given_installer(
        workspace=workspace, store=store, resolver=given_resolver(workspace=workspace)
    )
    asked = []
    installer.session.spark = lambda *a, **k: asked.append(True)

    report = installer.install(bundle)

    assert report.status == SUCCEEDED
    assert asked == [], "a Spark session was started for a batch that never used one"


@weaver_test()
def test_a_context_carries_no_spark_session_for_an_executor_to_find():
    """The last "does this host have Spark?" question, and it is gone.

    An executor that could ask would be classifying itself by position again.
    What it gets instead is a way to run statements, which every host has.
    """

    from weaver.build_bundle.executors.base import InstallationContext

    assert not hasattr(InstallationContext, "spark")
    assert "spark" not in InstallationContext.__dataclass_fields__


# --- concurrency within a batch -----------------------------------------------


def _tsql_batch(tmp_path, count: int):
    """One batch of independent T-SQL actions against one Warehouse target."""

    import hashlib

    target = BoundTarget(
        id="warehouse-Reporting", kind="warehouse", item_id="Reporting"
    )
    payloads = {}
    actions = []
    for index in range(count):
        path = f"payload/tsql/{index}.sql"
        data = f"select {index}\n".encode("utf-8")
        payloads[path] = data
        actions.append(
            InstallAction(
                id=f"a{index}",
                kind="build_procedure",
                resource_node_id=None,
                executor="tsql",
                payload=path,
                payload_sha256=hashlib.sha256(data).hexdigest(),
            )
        )
    plan = BuildPlan(
        format_version=SUPPORTED_FORMAT_VERSION,
        bundle_id="",
        repository_name="MyRepo",
        repository_signature="sig",
        targets=(target,),
        sequences=(
            BuildSequence(
                number=10,
                description="warehouse work",
                batches=(
                    BuildBatch(id="b", target_id=target.id, actions=tuple(actions)),
                ),
            ),
        ),
        selection=BuildSelection(Impact((), (), ()), (), (), ()),
    )
    plan = replace(plan, bundle_id=compute_bundle_id(plan))
    store = FilesystemStore()
    location = Location(str(tmp_path / "tsql-bundle"))
    write_bundle(location, plan=plan, payloads=payloads, store=store)
    return load_bundle(location, store=store), store


class _Concurrent:
    """Records how many actions were in flight at once."""

    name = "tsql"

    def __init__(self):
        import threading

        self.lock = threading.Lock()
        self.running = 0
        self.peak = 0
        self.calls = []

    def execute(self, action, payload, context):
        import time

        with self.lock:
            self.running += 1
            self.peak = max(self.peak, self.running)
            self.calls.append(action.id)
        time.sleep(0.05)
        with self.lock:
            self.running -= 1
        return {"ran": action.id}


@weaver_test()
def test_actions_in_a_batch_run_one_at_a_time(tmp_path):
    """They ran concurrently for one commit, and a real Warehouse said no.

    The manifest calls a batch's actions independent units, which is true of
    *Weaver's* ordering and says nothing about the database's. Concurrent DDL
    and DML against one Warehouse contended on catalogue metadata and on the
    rows they touched, and Fabric's snapshot isolation turned that into aborted
    transactions:

        Transaction (Process ID 55) was deadlocked on lock resources
        Snapshot isolation transaction aborted due to update conflict
    """

    bundle, store = _tsql_batch(tmp_path, 4)
    executor = _Concurrent()

    report = given_installer(store=store, executors={"tsql": executor}).install(bundle)

    assert report.status == SUCCEEDED
    assert executor.peak == 1, "actions in a batch overlapped"


@weaver_test()
def test_a_failure_in_a_batch_fails_the_sequence(tmp_path):
    """The sequence barrier is what stops anything downstream."""

    bundle, store = _tsql_batch(tmp_path, 4)

    class Failing(_Concurrent):
        def execute(self, action, payload, context):
            super().execute(action, payload, context)
            if action.id == "a2":
                raise RuntimeError("boom a2")
            return {"ran": action.id}

    report = given_installer(store=store, executors={"tsql": Failing()}).install(bundle)

    by_id = {result.action_id: result for result in report.action_results()}
    assert report.status == FAILED
    assert by_id["a2"].status == FAILED
    # The others were in flight and their results are true, so they are reported
    # rather than rewritten as skipped.
    assert by_id["a0"].status == SUCCEEDED


@weaver_test()
def test_spark_actions_are_not_run_concurrently(tmp_path):
    """A Spark statement's concurrency is the Fabric session's business, not
    ours. Widening this is a measurement, not an assumption."""

    location, store = _bundle(tmp_path)
    recorder = _Concurrent()
    recorder.name = "spark_sql"

    given_installer(store=store, executors={"spark_sql": recorder}).install(
        load_bundle(location, store=store)
    )

    assert recorder.peak == 1


# --- every action is timed where it runs ---------------------------------------


@weaver_test()
def test_each_action_reports_its_own_duration(tmp_path):
    """Actions in a batch share a target and a context, never a number.

    Spark actions used to cross to the session together and be stamped with the
    offsets the far side reported. Every action runs here now, one at a time, so
    each result carries the time that action actually took and the spans sit on
    one clock in the order they ran.
    """

    bundle, store = _tsql_batch(tmp_path, 3)

    class _Slow(_Concurrent):
        def execute(self, action, payload, context):
            import time

            if action.id == "a1":
                time.sleep(0.2)
            return super().execute(action, payload, context)

    report = given_installer(store=store, executors={"tsql": _Slow()}).install(bundle)

    by_id = {result.action_id: result for result in report.action_results()}
    assert by_id["a1"].duration_seconds > by_id["a0"].duration_seconds
    assert by_id["a1"].duration_seconds > by_id["a2"].duration_seconds
    assert by_id["a0"].finished_at <= by_id["a1"].started_at
    assert by_id["a1"].finished_at <= by_id["a2"].started_at
