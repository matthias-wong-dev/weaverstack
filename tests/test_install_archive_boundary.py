"""Carrier integrity before Fabric installation."""

from __future__ import annotations

import hashlib
import zipfile

import pytest
from support.weaver_test import weaver_test


@weaver_test()
def test_verified_carrier_preserves_binary_bytes(tmp_path):
    from weaver.sessions.install_archive import extract_verified

    source = tmp_path / "carrier.zip"
    data = b"\x00\xff\x80\x00binary"
    with zipfile.ZipFile(source, "w") as archive:
        archive.writestr("bundle/payload.bin", data)
    target = tmp_path / "private"
    extract_verified(
        source, target, {"bundle/payload.bin": hashlib.sha256(data).hexdigest()}
    )
    assert (target / "bundle/payload.bin").read_bytes() == data


@pytest.mark.parametrize(
    "name", ["../escape", "/absolute", "drive:C/file", "bundle\\bad", "bundle/./file"]
)
@weaver_test()
def test_unsafe_carrier_is_rejected_before_extraction(tmp_path, name):
    from weaver.sessions.install_archive import extract_verified

    source = tmp_path / "carrier.zip"
    data = b"content"
    with zipfile.ZipFile(source, "w") as archive:
        archive.writestr(name, data)
    target = tmp_path / "private"
    with pytest.raises(ValueError):
        extract_verified(source, target, {name: hashlib.sha256(data).hexdigest()})
    assert not target.exists()


@weaver_test()
def test_unmanifested_member_is_rejected_before_extraction(tmp_path):
    from weaver.sessions.install_archive import extract_verified

    source = tmp_path / "carrier.zip"
    with zipfile.ZipFile(source, "w") as archive:
        archive.writestr("bundle/plan.yml", b"plan")
        archive.writestr("extra.py", b"executable")
    target = tmp_path / "private"
    with pytest.raises(ValueError):
        extract_verified(
            source, target, {"bundle/plan.yml": hashlib.sha256(b"plan").hexdigest()}
        )
    assert not target.exists()


@weaver_test()
def test_durable_result_receipt_rejects_changed_bytes():
    import json

    from weaver.sessions.install_archive import read_receipt

    data = json.dumps({"status": "declined", "reason": "missing dependency"}).encode()
    receipt = {"bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}
    assert read_receipt(receipt, data)["status"] == "declined"
    with pytest.raises(ValueError):
        read_receipt(receipt, data + b" ")


def _report_pair():
    from dataclasses import replace

    from weaver.mutation import (
        BoundTarget,
        MutationAction,
        MutationBatch,
        MutationExecution,
        MutationPlan,
        MutationSequence,
    )
    from weaver.mutation.bundle import compute_bundle_id
    from weaver.mutation.executor import Completed, MutationDriver, MutationExecutor
    from weaver.sessions.mutation_report import encode_report

    plan = MutationPlan(
        execution=MutationExecution("Demo"),
        targets=(BoundTarget("target", "lakehouse", "target-id"),),
        sequences=(
            MutationSequence(
                10,
                "first",
                (
                    MutationBatch(
                        "batch",
                        "target",
                        (
                            MutationAction(
                                "one",
                                "build_folder",
                                None,
                                "folder",
                                None,
                                None,
                                target_id="target",
                                depends_on=(),
                            ),
                        ),
                    ),
                ),
            ),
        ),
    )
    plan = replace(plan, bundle_id=compute_bundle_id(plan))
    report = MutationExecutor(
        {"folder": MutationDriver(lambda request: Completed())}
    ).execute(plan)
    return plan, encode_report(report)


@weaver_test()
def test_remote_report_round_trip_is_complete():
    from weaver.sessions.mutation_report import decode_report, encode_report

    plan, mapping = _report_pair()
    assert encode_report(decode_report(plan, mapping)) == mapping


@pytest.mark.parametrize(
    "change",
    [
        "wrong_action",
        "missing_action",
        "nan",
        "invalid_status",
        "wrong_plan",
        "duplicate_action",
        "wrong_invocation",
    ],
)
@weaver_test()
def test_remote_report_rejects_unsettled_protocol(change):
    from weaver.errors import BuildError
    from weaver.sessions.mutation_report import decode_report

    plan, report = _report_pair()
    results = report["fields"]["results"]["value"]
    row = results[0]["fields"]
    if change == "wrong_action":
        row["action_id"]["value"] = "other"
    elif change == "missing_action":
        results.clear()
    elif change == "nan":
        row["active_seconds"]["value"] = float("nan")
    elif change == "invalid_status":
        row["status"]["value"] = "missing"
    elif change == "wrong_plan":
        report["fields"]["plan_id"]["value"] = "different"
    elif change == "duplicate_action":
        results.append(results[0])
    else:
        report["fields"]["invocation_id"]["value"] = "different"
    with pytest.raises((ValueError, BuildError)):
        decode_report(
            plan,
            report,
            invocation_id="expected" if change == "wrong_invocation" else None,
        )


@weaver_test()
def test_native_archive_dfs_accepts_abfss_publication_locations():
    from weaver.locations import Location
    from weaver.sessions.archive_runtime import ArchiveDfs

    workspace = "11111111-1111-1111-1111-111111111111"
    item = "22222222-2222-2222-2222-222222222222"
    client = ArchiveDfs(token=lambda: "unused")
    assert (
        client._url(
            Location(
                f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/{item}/Tables/DWG/Table"
            )
        )
        == f"https://onelake.dfs.fabric.microsoft.com/{workspace}/{item}/Tables/DWG/Table"
    )


@weaver_test()
def test_archive_native_session_preserves_direct_delta_workers(monkeypatch):
    from support.workspaces import given_workspace

    from weaver.sessions import archive_runtime

    recorded = []

    def create(**arguments):
        recorded.append(arguments["qualified_name"])
        return None

    monkeypatch.setattr(archive_runtime, "create_bound_delta_table", create)
    session = archive_runtime.ArchiveSession(
        workspace=given_workspace(), token=lambda: "unused", direct_delta_workers=2
    )
    session.resolver = lambda workspace: None
    outcomes = session.create_direct_delta_table_actions(
        [
            (str(index), f"table{index}", [["Column", "int", True]], None)
            for index in range(3)
        ]
    )
    assert session.direct_delta_workers == 2
    assert sorted(recorded) == ["table0", "table1", "table2"]
    assert [item["label"] for item in outcomes] == ["0", "1", "2"]
    assert all(item["succeeded"] for item in outcomes)
    session.close()


@pytest.mark.parametrize("batch", [False, True])
@weaver_test()
def test_archive_direct_creation_preserves_frozen_protocol_minima(monkeypatch, batch):
    from support.workspaces import given_workspace

    from weaver.sessions import archive_runtime

    calls = []
    monkeypatch.setattr(
        archive_runtime,
        "create_bound_delta_table",
        lambda **arguments: calls.append(arguments),
    )
    session = archive_runtime.ArchiveSession(
        workspace=given_workspace(), token=lambda: "unused"
    )
    session.resolver = lambda workspace: None
    policy = {"minReaderVersion": 2, "minWriterVersion": 5}
    try:
        if batch:
            results = session.create_direct_delta_table_actions(
                [("a", "table", [["Value", "string", False]], None, policy)]
            )
            assert results[0]["succeeded"], results
        else:
            session.create_direct_delta_table(
                "table", [["Value", "string", False]], protocol_minima=policy
            )
        assert len(calls) == 1
        assert calls[0]["protocol_minima"] == policy
    finally:
        session.close()


@weaver_test()
def test_native_archive_store_preserves_non_utf8_payloads(tmp_path):
    from weaver.locations import Location
    from weaver.sessions.archive_runtime import ArchiveStore
    from weaver.store import FilesystemStore

    class TextStore(FilesystemStore):
        def write(self, location, data):
            data.decode("utf-8")
            super().write(location, data)

    store = ArchiveStore(TextStore(), FilesystemStore())
    destination = Location(str(tmp_path / "binary"))
    store.write(destination, b"\xff\x00\xfe")
    assert store.read(destination) == b"\xff\x00\xfe"


@pytest.mark.parametrize("failure", [False, True])
@weaver_test()
def test_ready_archive_bootstrap_restores_the_live_interpreter(
    tmp_path, monkeypatch, failure
):
    import importlib
    import io
    import shutil
    import sys
    import tempfile
    import types

    from weaver.sessions.install_archive import Carrier, bootstrap_source, read_receipt

    body = (
        "def run_mutation(*args, **kwargs):\n    raise RuntimeError('installer interrupted')\n"
        if failure
        else "def run_mutation(*args, **kwargs):\n    return {'status': 'completed', 'marker': 'extracted runtime'}\n"
    )
    members = {
        "runtime/weaver/__init__.py": b"",
        "runtime/weaver/sessions/__init__.py": b"",
        "runtime/weaver/sessions/archive_runtime.py": body.encode(),
    }
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as zipped:
        for name, data in members.items():
            zipped.writestr(name, data)
    carrier = Carrier(
        stream.getvalue(),
        {name: hashlib.sha256(data).hexdigest() for name, data in members.items()},
    )
    incoming, output = tmp_path / "carrier.zip", tmp_path / "result.json"
    incoming.write_bytes(carrier.data)
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
    original_import = importlib.import_module

    def dependency(name, *args, **kwargs):
        if name in ("pyarrow", "yaml", "requests", "azure.identity", "mssql_python"):
            return types.ModuleType(name)
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(importlib, "import_module", dependency)
    monkeypatch.setattr(importlib.metadata, "version", lambda name: "1.6.6")
    original_mkdtemp = tempfile.mkdtemp
    private = []

    def mkdtemp(*args, **kwargs):
        path = original_mkdtemp(*args, **kwargs)
        private.append(__import__("pathlib").Path(path))
        return path

    monkeypatch.setattr(tempfile, "mkdtemp", mkdtemp)
    previous = {
        name: module
        for name, module in sys.modules.items()
        if name == "weaver" or name.startswith("weaver.")
    }
    paths = list(sys.path)
    emitted = []
    program = bootstrap_source(carrier, str(incoming), str(output), workers=16)
    if failure:
        with pytest.raises(RuntimeError, match="installer interrupted"):
            exec(program, {"spark": object(), "emit": emitted.append})
        assert not emitted
    else:
        exec(program, {"spark": object(), "emit": emitted.append})
        result = read_receipt(emitted[0], output.read_bytes())
        assert result["marker"] == "extracted runtime"
        assert result["archive_sha256"] == carrier.sha256
    assert sys.path == paths
    assert {
        name: module
        for name, module in sys.modules.items()
        if name == "weaver" or name.startswith("weaver.")
    } == previous
    assert len(private) == 1 and not private[0].exists()


@weaver_test()
def test_archive_bootstrap_declines_missing_dependency_before_install(
    tmp_path, monkeypatch
):
    import hashlib
    import importlib
    import io
    import shutil
    import sys
    import types
    import zipfile

    from weaver.sessions.install_archive import Carrier, bootstrap_source, read_receipt

    data = io.BytesIO()
    with zipfile.ZipFile(data, "w") as zipped:
        zipped.writestr("bundle/plan.yml", b"not executed")
    carrier = Carrier(
        data.getvalue(),
        {"bundle/plan.yml": hashlib.sha256(b"not executed").hexdigest()},
    )
    incoming = tmp_path / "carrier.zip"
    incoming.write_bytes(carrier.data)
    output = tmp_path / "result.json"
    module = types.ModuleType("notebookutils")
    module.fs = types.SimpleNamespace(
        cp=lambda source, target, recurse: shutil.copyfile(
            source, target.removeprefix("file:")
        ),
        put=lambda path, content, overwrite: (
            __import__("pathlib").Path(path).write_text(content)
        ),
    )
    monkeypatch.setitem(sys.modules, "notebookutils", module)
    original = importlib.import_module

    def missing(name, *args, **kwargs):
        if name == "pyarrow":
            raise ModuleNotFoundError("missing pyarrow")
        return original(name, *args, **kwargs)

    monkeypatch.setattr(importlib, "import_module", missing)
    receipts = []
    source = bootstrap_source(carrier, str(incoming), str(output), workers=16)
    compile(source, "archive-bootstrap", "exec")
    before = sys.modules["weaver"]
    namespace = {"emit": receipts.append, "spark": object()}
    exec(source, namespace)
    result = read_receipt(receipts[0], output.read_bytes())
    assert result["status"] == "declined"
    assert result["mutated"] is False
    assert sys.modules["weaver"] is before
