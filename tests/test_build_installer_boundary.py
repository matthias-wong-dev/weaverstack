"""Installer orchestration: barriers, skipping, and faithful reporting.

These use a recording fake executor rather than Spark, so the sequencing and
reporting logic is pinned fast: sequences are barriers, a failure stops later
sequences, every planned action gets exactly one result, and the report is
persisted. Payload integrity and the real executors are covered elsewhere.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone

import pytest
from support.bundles import given_execution, with_catalogue
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
        targets=with_catalogue((TARGET,)),
        sequences=sequences,
        selection=BuildSelection(Impact((), (), ()), (), (), ()),
        execution=given_execution(with_catalogue((TARGET,)), sequences),
    )
    plan = replace(plan, bundle_id=compute_bundle_id(plan))

    store = FilesystemStore()
    location = Location(str(tmp_path / "bundle"))
    write_bundle(location, plan=plan, payloads=payloads, store=store)
    return location, store


@pytest.mark.parametrize(
    "outcome", ["success", "failure", "uncertain", "decline", "exception"]
)
@weaver_test()
def test_mixed_install_delegates_lakehouse_batches_in_manifest_order(tmp_path, outcome):
    import hashlib

    from weaver.build_bundle import Installer
    from weaver.build_bundle.bundle import BuildBundle

    location, store = _bundle(tmp_path)
    frozen = load_bundle(location, store=store)
    warehouse = BoundTarget(id="warehouse-WH", kind="warehouse", item_id="WH")
    warehouse_action = replace(
        frozen.plan.sequences[0].batches[0].actions[0],
        id="warehouse-action",
        executor="tsql",
        payload="payload/warehouse.sql",
        payload_sha256=hashlib.sha256(b"select 1;").hexdigest(),
    )
    sequence = replace(
        frozen.plan.sequences[0],
        batches=(
            frozen.plan.sequences[0].batches[0],
            BuildBatch("warehouse-batch", warehouse.id, (warehouse_action,)),
            frozen.plan.sequences[1].batches[0],
        ),
    )
    plan = replace(
        frozen.plan,
        targets=frozen.plan.targets + (warehouse,),
        sequences=(sequence, frozen.plan.sequences[2]),
    )
    payloads = {
        action.payload: store.read(frozen.location.join(*action.payload.split("/")))
        for _, _, action in frozen.plan.actions()
    }
    payloads[warehouse_action.payload] = b"select 1;"
    mixed = write_bundle(location, plan=plan, payloads=payloads, store=store)
    installer = given_installer(store=store, warehouses=("WH",))
    delegated = []
    instants = []
    import types

    warehouse_calls = []
    installer.session.sql_executor = lambda *args, **kwargs: types.SimpleNamespace(
        execute_script=warehouse_calls.append
    )

    def install_batches(
        bundle, *, sequence_number, batch_ids, build_datetime, workspace
    ):
        delegated.append((sequence_number, batch_ids))
        instants.append(build_datetime)
        selected = next(
            row for row in bundle.plan.sequences if row.number == sequence_number
        )
        selected = replace(
            selected,
            batches=tuple(batch for batch in selected.batches if batch.id in batch_ids),
        )
        part = BuildBundle(
            bundle.location, replace(bundle.plan, sequences=(selected,)), bundle.store
        )
        if outcome == "decline":
            return None
        if outcome == "exception":
            raise RuntimeError("unsubmitted carrier failed")
        if outcome == "uncertain":
            from weaver.sessions.install_archive import uncertain_report

            return uncertain_report(part.plan, "receipt lost", "remote-result.json")
        if outcome == "failure":
            return Installer(
                installer.session, executors={"spark_sql": Recorder(fail_on=("a1",))}
            ).install(part)
        return Installer(installer.session).install(
            part, on_sequence=lambda report: None
        )

    installer.session.install_batches = install_batches
    report = installer.install(mixed)
    failed = outcome in ("failure", "uncertain", "exception")
    assert report.status == (FAILED if failed else SUCCEEDED)
    assert delegated == (
        [(10, ("b0",))] if failed else [(10, ("b0",)), (10, ("b1",)), (30, ("b2",))]
    )
    assert len(set(instants)) == 1
    assert warehouse_calls == ([] if failed else ["select 1;"])
    assert [row.status for row in report.action_results()] == (
        [FAILED, SKIPPED, SKIPPED, SKIPPED] if failed else [SUCCEEDED] * 4
    )
    assert [row.action_id for row in report.action_results()] == [
        "a1",
        "warehouse-action",
        "a2",
        "a3",
    ]
    assert (
        load_bundle(location, store=store).plan.to_mapping() == mixed.plan.to_mapping()
    )


@weaver_test()
def test_archive_rejects_a_stored_plan_changed_after_bundle_validation(
    tmp_path, monkeypatch
):
    import yaml

    from weaver.errors import InstallError
    from weaver.sessions import ConsoleSession

    location, store = _bundle(tmp_path)
    bundle = load_bundle(location, store=store)
    changed = yaml.safe_load(store.read(location / "plan.yml"))
    changed["sequences"][0]["description"] = "Changed after validation"
    store.write(location / "plan.yml", yaml.safe_dump(changed).encode())
    session = ConsoleSession(progress=False)
    touched = []
    monkeypatch.setattr(session, "scope", lambda *args: touched.append("scope"))
    with pytest.raises(InstallError, match="frozen installation plan"):
        session.install_bundle(bundle, workspace=given_workspace())
    assert touched == []


@weaver_test()
def test_corrupt_archive_receipt_remains_uncertain(tmp_path, monkeypatch):
    import hashlib
    import json
    import types

    from weaver.sessions import ConsoleSession
    from weaver.targets import ItemRef

    location, store = _bundle(tmp_path)
    bundle = load_bundle(location, store=store)
    expected = given_installer(
        store=store, executors={"spark_sql": Recorder()}
    ).install(bundle)
    destination = tmp_path / "destination"

    resolver = given_resolver(root=destination)
    files = resolver.files_root(ItemRef(TARGET.item_id)).path

    def submit(source, **kwargs):
        stage = next(files.iterdir())
        output = {
            "status": "completed",
            "archive_sha256": hashlib.sha256(
                (stage / "carrier.zip").read_bytes()
            ).hexdigest(),
            "report": expected.to_mapping(),
        }
        data = json.dumps(output).encode()
        (stage / "result.json").write_bytes(data)
        return {"bytes": len(data), "sha256": "incorrect"}

    scope = types.SimpleNamespace(
        resolver=resolver,
        transport_store=FilesystemStore(),
        livy_run=submit,
    )
    session = ConsoleSession(progress=False)
    monkeypatch.setattr(session, "scope", lambda workspace=None: scope)
    report = session.install_bundle(bundle, workspace=given_workspace())
    assert report.status == FAILED
    assert all(
        row.status == FAILED and row.details["uncertain"]
        for row in report.action_results()
    )
    assert len(list(files.iterdir())) == 1


@weaver_test()
def test_archive_upload_failure_removes_unsubmitted_carrier(tmp_path, monkeypatch):
    import types

    from weaver.sessions import ConsoleSession
    from weaver.targets import ItemRef

    location, store = _bundle(tmp_path)
    resolver = given_resolver(root=tmp_path / "destination")
    files = resolver.files_root(ItemRef(TARGET.item_id)).path

    class BrokenUpload(FilesystemStore):
        def write(self, location, data):
            super().write(location, data[:10])
            raise OSError("upload interrupted before submission")

    def forbidden(*args, **kwargs):
        raise AssertionError("unuploaded carrier was submitted")

    scope = types.SimpleNamespace(
        resolver=resolver, transport_store=BrokenUpload(), livy_run=forbidden
    )
    session = ConsoleSession(progress=False)
    monkeypatch.setattr(session, "scope", lambda workspace=None: scope)
    with pytest.raises(OSError, match="upload interrupted"):
        session.install_bundle(
            load_bundle(location, store=store), workspace=given_workspace()
        )
    assert not list(files.iterdir())


@pytest.mark.parametrize("outcome", ["completed", "prefix", "missing", "cancelled"])
@pytest.mark.parametrize("timeout", [None, 7])
@weaver_test()
def test_archive_interruption_preserves_the_acknowledged_boundary(
    tmp_path, monkeypatch, outcome, timeout
):
    import hashlib
    import json
    import types

    from weaver.fabric.livy import DEFAULT_STATEMENT_TIMEOUT
    from weaver.sessions import ConsoleSession
    from weaver.targets import ItemRef

    location, store = _bundle(tmp_path)
    bundle = load_bundle(location, store=store)
    expected = given_installer(
        store=store, executors={"spark_sql": Recorder()}
    ).install(bundle)
    resolver = given_resolver(root=tmp_path / "destination")
    files = resolver.files_root(ItemRef(TARGET.item_id)).path
    calls = []

    def submit(source, **kwargs):
        calls.append(kwargs)
        stage = next(files.iterdir())
        if outcome in ("completed", "prefix"):
            mapping = expected.to_mapping()
            if outcome == "prefix":
                mapping.update(
                    status="running",
                    finished_at=None,
                    sequences=mapping["sequences"][:1],
                )
            result = {
                "status": "completed" if outcome == "completed" else "running",
                "archive_sha256": hashlib.sha256(
                    (stage / "carrier.zip").read_bytes()
                ).hexdigest(),
                "report": mapping,
            }
            (stage / "result.json").write_text(json.dumps(result))
        if outcome == "cancelled":
            raise KeyboardInterrupt("caller cancelled")
        raise TimeoutError("receipt did not arrive")

    scope = types.SimpleNamespace(
        resolver=resolver, transport_store=FilesystemStore(), livy_run=submit
    )
    session = ConsoleSession(progress=False)
    monkeypatch.setattr(session, "scope", lambda workspace=None: scope)
    if outcome == "cancelled":
        with pytest.raises(KeyboardInterrupt, match="caller cancelled"):
            session.install_bundle(bundle, workspace=given_workspace(), timeout=timeout)
    else:
        report = session.install_bundle(
            bundle, workspace=given_workspace(), timeout=timeout
        )
        assert report is not None
        if outcome == "completed":
            assert report.to_mapping() == expected.to_mapping()
            assert len(session.archive_installations) == 1
        else:
            rows = list(report.action_results())
            assert report.status == FAILED
            assert [row.status for row in rows] == (
                [SUCCEEDED, FAILED, FAILED] if outcome == "prefix" else [FAILED] * 3
            )
            uncertain = rows[1:] if outcome == "prefix" else rows
            assert all(row.details["uncertain"] is True for row in uncertain)
            assert all(
                (next(files.iterdir()) / "result.json").as_posix()
                == row.details["remote_result"]
                for row in uncertain
            )
    assert len(calls) == 1
    assert calls[0]["retry_submission"] is False
    assert calls[0]["timeout"] == 3 * (
        DEFAULT_STATEMENT_TIMEOUT if timeout is None else timeout
    )
    assert len(list(files.iterdir())) == (0 if outcome == "completed" else 1)


@weaver_test()
def test_changed_executor_registry_keeps_the_local_install_path(tmp_path):
    location, store = _bundle(tmp_path)
    installer = given_installer(store=store)
    recorder = Recorder()
    installer.executors["spark_sql"] = recorder

    def unsupported(*args, **kwargs):
        raise AssertionError("custom executor was delegated")

    installer.session.install_bundle = unsupported
    installer.session.install_batches = unsupported
    report = installer.install(load_bundle(location, store=store))
    assert report.status == SUCCEEDED
    assert recorder.calls == ["a1", "a2", "a3"]


@weaver_test()
def test_console_archive_declines_before_mutation_without_submission_retry(
    tmp_path, monkeypatch
):
    import importlib
    import shutil
    import sys
    import types

    from weaver.sessions import ConsoleSession
    from weaver.targets import ItemRef

    location, store = _bundle(tmp_path)
    bundle = load_bundle(location, store=store)
    receipts = []
    calls = []
    sdk = types.ModuleType("notebookutils")
    sdk.fs = types.SimpleNamespace(
        cp=lambda source, target, recurse: shutil.copyfile(
            source, target.removeprefix("file:")
        ),
        put=lambda path, content, overwrite: (
            __import__("pathlib").Path(path).write_text(content)
        ),
    )
    monkeypatch.setitem(sys.modules, "notebookutils", sdk)
    original = importlib.import_module

    def missing(name, *args, **kwargs):
        if name == "pyarrow":
            raise ModuleNotFoundError("missing pyarrow")
        return original(name, *args, **kwargs)

    monkeypatch.setattr(importlib, "import_module", missing)

    def submit(source, **kwargs):
        calls.append(kwargs)
        exec(source, {"emit": receipts.append, "spark": object()})
        return receipts[-1]

    resolver = given_resolver(root=tmp_path / "destination")
    scope = types.SimpleNamespace(
        resolver=resolver,
        transport_store=FilesystemStore(),
        livy_run=submit,
    )
    session = ConsoleSession(progress=False)
    monkeypatch.setattr(session, "scope", lambda workspace=None: scope)
    returned = session.install_bundle(bundle, workspace=given_workspace())
    assert returned is None
    assert len(calls) == 1
    assert calls[0]["retry_submission"] is False
    assert not list(resolver.files_root(ItemRef(TARGET.item_id)).path.iterdir())


@pytest.mark.parametrize("outcome", ["completed", "declined"])
@pytest.mark.parametrize("warning_error", [False, True])
@weaver_test()
def test_archive_cleanup_failure_preserves_the_verified_outcome(
    tmp_path, monkeypatch, outcome, warning_error
):
    import hashlib
    import json
    import types

    from weaver.sessions import ConsoleSession
    from weaver.targets import ItemRef

    location, store = _bundle(tmp_path)
    bundle = load_bundle(location, store=store)
    expected = given_installer(
        store=store, executors={"spark_sql": Recorder()}
    ).install(bundle)
    resolver = given_resolver(root=tmp_path / "destination")
    files = resolver.files_root(ItemRef(TARGET.item_id)).path
    calls = []

    class CleanupFailure(FilesystemStore):
        def delete(self, location, *, recursive=False):
            raise OSError("carrier deletion unavailable")

    def submit(source, **kwargs):
        calls.append(kwargs)
        stage = next(files.iterdir())
        result = {
            "status": outcome,
            "archive_sha256": hashlib.sha256(
                (stage / "carrier.zip").read_bytes()
            ).hexdigest(),
        }
        if outcome == "completed":
            result["report"] = expected.to_mapping()
        else:
            result.update(mutated=False, reason="missing runtime dependency")
        data = json.dumps(result).encode()
        (stage / "result.json").write_bytes(data)
        return {"bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}

    scope = types.SimpleNamespace(
        resolver=resolver, transport_store=CleanupFailure(), livy_run=submit
    )
    session = ConsoleSession(progress=False)
    monkeypatch.setattr(session, "scope", lambda workspace=None: scope)
    if warning_error:

        def broken_warning(message):
            raise OSError("warning output unavailable")

        monkeypatch.setattr(session, "warn", broken_warning)
    returned = session.install_bundle(bundle, workspace=given_workspace())
    assert (
        returned.to_mapping() == expected.to_mapping()
        if outcome == "completed"
        else returned is None
    )
    assert len(calls) == 1
    stage = next(files.iterdir())
    assert (stage / "result.json").exists()
    assert len(session.archive_cleanup_failures) == 1
    failure = session.archive_cleanup_failures[0]
    assert failure == {
        "status": outcome,
        "stage": stage.as_posix(),
        "remote_result": (stage / "result.json").as_posix(),
        "error_type": "OSError",
        "error_message": "carrier deletion unavailable",
    }
    if not warning_error:
        assert any(stage.as_posix() in warning for warning in session.warnings)


@pytest.mark.parametrize("receipt", ["valid", "lost", "wrong_selection"])
@weaver_test()
def test_console_archive_uses_borrowed_livy_for_selected_lakehouse_batches(
    tmp_path, monkeypatch, receipt
):
    import hashlib
    import json
    import types
    import zipfile

    from weaver.fabric import LivySession
    from weaver.sessions import ConsoleSession
    from weaver.targets import ItemRef

    location, store = _bundle(tmp_path)
    bundle = load_bundle(location, store=store)
    part = replace(
        bundle, plan=replace(bundle.plan, sequences=(bundle.plan.sequences[1],))
    )
    expected = given_installer(
        store=store, executors={"spark_sql": Recorder()}
    ).install(part)
    resolver = given_resolver(root=tmp_path / "destination")
    files = resolver.files_root(ItemRef(TARGET.item_id)).path
    calls = []

    def submit(source, **kwargs):
        calls.append(kwargs)
        stage = next(files.iterdir())
        with zipfile.ZipFile(stage / "carrier.zip") as carrier:
            request = json.loads(carrier.read("request.json"))
            assert carrier.read("bundle/plan.yml") == store.read(location / "plan.yml")
            assert request["batch_ids"] == ["b1"]
        result = {
            "status": "completed",
            "request": request,
            "archive_sha256": hashlib.sha256(
                (stage / "carrier.zip").read_bytes()
            ).hexdigest(),
            "report": expected.to_mapping(),
        }
        if receipt == "wrong_selection":
            result["request"] = dict(request, batch_ids=["b0"])
        data = json.dumps(result).encode()
        (stage / "result.json").write_bytes(data)
        if receipt == "lost":
            raise TimeoutError("receipt lost")
        return {"bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}

    scope = types.SimpleNamespace(
        resolver=resolver, transport_store=FilesystemStore(), livy_run=submit
    )
    session = ConsoleSession(progress=False, livy=object.__new__(LivySession))
    monkeypatch.setattr(session, "scope", lambda workspace=None: scope)
    report = session.install_batches(
        bundle,
        sequence_number=20,
        batch_ids=("b1",),
        build_datetime="2026-09-30 12:34:56.000000",
        workspace=given_workspace(),
        timeout=7,
    )
    assert [row.action_id for row in report.action_results()] == ["a2"]
    assert report.status == (FAILED if receipt == "wrong_selection" else SUCCEEDED)
    if receipt == "wrong_selection":
        assert next(report.action_results()).details["uncertain"]
    else:
        assert len(session.archive_installations) == 1
    assert calls[0]["retry_submission"] is False
    assert calls[0]["timeout"] == 7
    assert len(list(files.iterdir())) == (1 if receipt == "wrong_selection" else 0)


@pytest.mark.parametrize(
    "sequence_number,batch_ids",
    [
        (999, ("b0",)),
        (10, ()),
        (10, ("unknown",)),
        (10, ("b0", "b0")),
        (10, ("b1", "b0")),
        (10, ("b0", "b2")),
        (10, ("warehouse-batch",)),
    ],
)
@weaver_test()
def test_archive_selection_refuses_invalid_or_noncontiguous_batches(
    tmp_path, sequence_number, batch_ids
):
    from weaver.build_bundle.installer import select_install_batches
    from weaver.errors import InstallError

    location, store = _bundle(tmp_path)
    plan = load_bundle(location, store=store).plan
    warehouse = BoundTarget(id="warehouse-WH", kind="warehouse", item_id="WH")
    original = [sequence.batches[0] for sequence in plan.sequences]
    sequence = replace(
        plan.sequences[0],
        batches=tuple(original) + (BuildBatch("warehouse-batch", warehouse.id, ()),),
    )
    mixed = replace(plan, targets=plan.targets + (warehouse,), sequences=(sequence,))
    with pytest.raises(InstallError, match="installation"):
        select_install_batches(
            mixed, sequence_number=sequence_number, batch_ids=batch_ids
        )


@weaver_test()
def test_archive_runtime_installs_only_the_requested_lakehouse_batches(
    tmp_path, monkeypatch
):
    import json
    import sys
    import types

    from weaver.sessions import archive_runtime

    location, store = _bundle(tmp_path)
    original = store.read(location / "plan.yml")
    request = {
        "sequence_number": 20,
        "batch_ids": ["b1"],
        "build_datetime": "2026-09-30 12:34:56.000000",
    }
    (location.path.parent / "request.json").write_text(json.dumps(request))
    session = given_installer(store=store).session
    monkeypatch.setattr(archive_runtime, "ArchiveSession", lambda **kwargs: session)
    sdk = types.ModuleType("notebookutils")
    sdk.fs = types.SimpleNamespace(put=lambda *args: None)
    monkeypatch.setitem(sys.modules, "notebookutils", sdk)
    result = archive_runtime.run_bundle(
        location.path.parent, object(), "report.json", "carrier", workers=16
    )
    assert [
        row["action_id"]
        for sequence in result["report"]["sequences"]
        for row in sequence["actions"]
    ] == ["a2"]
    assert result["request"] == request
    assert store.read(location / "plan.yml") == original


@weaver_test()
def test_native_archive_install_journals_the_real_installer_report(
    tmp_path, monkeypatch
):
    import json
    import sys
    import types

    from weaver.sessions import archive_runtime

    location, store = _bundle(tmp_path)
    session = given_installer(store=store).session
    monkeypatch.setattr(archive_runtime, "ArchiveSession", lambda **kwargs: session)
    writes = []
    sdk = types.ModuleType("notebookutils")
    sdk.fs = types.SimpleNamespace(
        put=lambda path, content, overwrite: writes.append(json.loads(content))
    )
    monkeypatch.setitem(sys.modules, "notebookutils", sdk)
    result = archive_runtime.run_bundle(
        location.path.parent, object(), "report.json", "carrier", workers=16
    )
    assert result["status"] == "completed"
    assert result["report"]["status"] == SUCCEEDED
    assert [
        row["action_id"]
        for sequence in result["report"]["sequences"]
        for row in sequence["actions"]
    ] == ["a1", "a2", "a3"]
    assert [len(item["report"]["sequences"]) for item in writes] == [1, 2, 3]
    assert all(item["archive_sha256"] == "carrier" for item in writes)


@weaver_test()
def test_install_persists_complete_sequence_prefixes(tmp_path):
    location, store = _bundle(tmp_path)
    installer = given_installer(
        store=store, executors={"spark_sql": Recorder(fail_on=("a2",))}
    )
    prefixes = []
    report = installer.install(
        load_bundle(location, store=store),
        on_sequence=lambda report: prefixes.append(report.to_mapping()),
    )
    assert [[row["number"] for row in item["sequences"]] for item in prefixes] == [
        [10],
        [10, 20],
        [10, 20, 30],
    ]
    assert all(
        item["status"] == "running" and item["finished_at"] is None for item in prefixes
    )
    assert [row.status for row in report.action_results()] == [
        SUCCEEDED,
        FAILED,
        SKIPPED,
    ]


@weaver_test()
def test_carrier_uses_the_exact_frozen_bundle_bytes(tmp_path):
    import io
    import zipfile

    from weaver.sessions.install_archive import pack_bundle

    location, store = _bundle(tmp_path)
    bundle = load_bundle(location, store=store)
    carrier = pack_bundle(bundle)
    assert carrier is not None
    with zipfile.ZipFile(io.BytesIO(carrier.data)) as archive:
        assert archive.read("bundle/plan.yml") == store.read(location / "plan.yml")
        assert archive.read("bundle/payload/a1/stmt.spark.sql") == b"select 0\n"
        assert (
            archive.read("runtime/weaver/sessions/install_archive.py")
            == __import__("pathlib")
            .Path(
                __import__("weaver.sessions.install_archive", fromlist=["x"]).__file__
            )
            .read_bytes()
        )


@weaver_test()
def test_session_owned_installation_persists_the_delegated_report(tmp_path):
    location, store = _bundle(tmp_path)
    bundle = load_bundle(location, store=store)
    original = given_installer(store=store, executors={"spark_sql": Recorder()})
    expected = original.install(bundle)
    installer = given_installer(store=store)
    seen = []

    def install_bundle(incoming, *, workspace=None):
        seen.append(incoming)
        return expected

    installer.session.install_bundle = install_bundle
    returned = installer.install(bundle)
    assert returned is expected
    assert seen == [bundle]
    import yaml

    assert (
        yaml.safe_load(store.read(location / "install-report.yml"))
        == expected.to_mapping()
    )


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
def test_a_failed_install_report_marks_the_install_task_failed(tmp_path, monkeypatch):
    from support.sessions import given_session

    import weaver.build_bundle as build_bundle
    from weaver.build_bundle.report import InstallationReport
    from weaver.operations.install import install

    location, store = _bundle(tmp_path)
    bundle = load_bundle(location, store=store)
    now = datetime.now(timezone.utc)
    failed = InstallationReport(
        bundle_id=bundle.bundle_id,
        status=FAILED,
        started_at=now,
        finished_at=now,
        sequences=(),
    )

    class FailedInstaller:
        def __init__(self, *_args, **_kwargs):
            pass

        def install(self, loaded):
            assert loaded.bundle_id == bundle.bundle_id
            return failed

    monkeypatch.setattr(build_bundle, "Installer", FailedInstaller)
    session = given_session()

    assert install(location, session=session) is failed

    frame = next(frame for frame in session.timings if frame.name == "Install")
    assert frame.failed


@pytest.mark.parametrize(
    ("description", "wording"),
    [
        ("build dependency layer", "Building objects"),
        ("install runtime artefacts", "Installing load and test artefacts"),
        (
            "publish catalogue dictionaries and installations",
            "Updating catalogue definitions",
        ),
        ("publish item registry last", "Finalising catalogue"),
    ],
)
@weaver_test()
def test_operator_sequence_labels_are_aggregate_and_do_not_change_bundle_identity(
    description, wording
):
    from types import SimpleNamespace

    from weaver.build_bundle.installer import _sequence_label

    actions = (_action("customer"), _action("order"), _action("invoice"))
    sequence = BuildSequence(
        number=1,
        description=description,
        batches=(BuildBatch(id="build", target_id=TARGET.id, actions=actions),),
    )
    plan = BuildPlan(
        format_version=SUPPORTED_FORMAT_VERSION,
        bundle_id="",
        repository_name="MyRepo",
        repository_signature="sig",
        targets=with_catalogue((TARGET,)),
        sequences=(sequence,),
        selection=BuildSelection(Impact((), (), ()), (), (), ()),
        execution=given_execution(with_catalogue((TARGET,)), (sequence,)),
    )
    bundle_id = compute_bundle_id(plan)

    label = _sequence_label(sequence, {TARGET.id: SimpleNamespace(bound=TARGET)})

    assert label == f"Lakehouse/Sales_LH · {wording} · 3 actions"
    assert all(action.id not in label for action in actions)
    assert sequence.description == description
    assert compute_bundle_id(plan) == bundle_id


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

    with pytest.raises(BuildError, match="does not match its checksum"):
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
    workspace through NotebookUtils rather than REST, so it offers no refresh
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
        targets=with_catalogue((TARGET,)),
        sequences=(sequence,),
        selection=BuildSelection(Impact((), (), ()), (), (), ()),
        execution=given_execution(with_catalogue((TARGET,)), (sequence,)),
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
    one, paying for a capability none of its actions would touch and, in a
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
    sequences = (
        BuildSequence(
            number=8990,
            description="refresh endpoints",
            batches=(BuildBatch(id="refresh", target_id=TARGET.id, actions=(action,)),),
        ),
    )
    targets = with_catalogue((TARGET,))
    plan = BuildPlan(
        format_version=SUPPORTED_FORMAT_VERSION,
        bundle_id="",
        repository_name="MyRepo",
        repository_signature="sig",
        targets=targets,
        sequences=sequences,
        selection=BuildSelection(Impact((), (), ()), (), (), ()),
        execution=given_execution(targets, sequences),
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
    sequences = (
        BuildSequence(
            number=10,
            description="warehouse work",
            batches=(BuildBatch(id="b", target_id=target.id, actions=tuple(actions)),),
        ),
    )
    plan = BuildPlan(
        format_version=SUPPORTED_FORMAT_VERSION,
        bundle_id="",
        repository_name="MyRepo",
        repository_signature="sig",
        targets=(target,),
        sequences=sequences,
        selection=BuildSelection(Impact((), (), ()), (), (), ()),
        execution=given_execution((target,), sequences),
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
