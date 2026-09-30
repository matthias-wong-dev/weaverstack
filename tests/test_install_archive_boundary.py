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
    plan = {
        "bundle_id": "bundle",
        "sequences": [
            {
                "number": 10,
                "description": "first",
                "batches": [
                    {
                        "target_id": "target",
                        "actions": [
                            {
                                "id": "one",
                                "executor": "spark_sql",
                                "resource_node_id": None,
                            }
                        ],
                    }
                ],
            }
        ],
    }
    report = {
        "bundle_id": "bundle",
        "status": "succeeded",
        "started_at": "2026-01-01T00:00:00+00:00",
        "finished_at": "2026-01-01T00:00:01+00:00",
        "sequences": [
            {
                "number": 10,
                "description": "first",
                "status": "succeeded",
                "actions": [
                    {
                        "action_id": "one",
                        "executor": "spark_sql",
                        "target_id": "target",
                        "resource_node_id": None,
                        "status": "succeeded",
                        "started_at": "2026-01-01T00:00:00+00:00",
                        "finished_at": "2026-01-01T00:00:01+00:00",
                        "duration_seconds": 1.0,
                    }
                ],
            }
        ],
    }
    return plan, report


@weaver_test()
def test_remote_report_round_trip_is_complete():
    from weaver.sessions.install_archive import decode_report

    plan, mapping = _report_pair()
    assert decode_report(mapping, plan).to_mapping() == mapping


@pytest.mark.parametrize(
    "change",
    [
        "wrong_target",
        "missing_action",
        "nan",
        "false_success",
        "wrong_bundle",
        "wrong_sequence",
        "naive_clock",
    ],
)
@weaver_test()
def test_remote_report_rejects_unsettled_protocol(change):
    from weaver.sessions.install_archive import decode_report

    plan, report = _report_pair()
    row = report["sequences"][0]["actions"][0]
    if change == "wrong_target":
        row["target_id"] = "other"
    elif change == "missing_action":
        report["sequences"][0]["actions"] = []
    elif change == "nan":
        row["duration_seconds"] = float("nan")
    elif change == "false_success":
        row.update(status="failed", error_type="ExampleError", error_message="failure")
    elif change == "wrong_bundle":
        report["bundle_id"] = "different"
    elif change == "wrong_sequence":
        report["sequences"][0]["number"] = 20
    else:
        row["started_at"] = "2026-01-01T00:00:00"
    with pytest.raises(ValueError):
        decode_report(report, plan)


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


@weaver_test()
def test_archive_interruption_fails_every_unacknowledged_action():
    from weaver.sessions.install_archive import uncertain_report

    plan, _ = _report_pair()
    result = uncertain_report(
        plan, "statement outcome unavailable", "private/report.json"
    )
    rows = list(result.action_results())
    assert result.status == "failed"
    assert [row.action_id for row in rows] == ["one"]
    assert rows[0].status == "failed"
    assert rows[0].details["uncertain"] is True
    assert rows[0].details["remote_result"] == "private/report.json"


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
        "def run_bundle(*args, **kwargs):\n    raise RuntimeError('installer interrupted')\n"
        if failure
        else "def run_bundle(*args, **kwargs):\n    return {'status': 'completed', 'marker': 'extracted runtime'}\n"
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
