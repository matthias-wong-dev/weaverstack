"""Build transport carries sealed actions to an already installed runtime."""

import io
import json
import zipfile

import pytest
from support.weaver_test import weaver_test
from test_mutation_archive_install import build_plan_fixture

from weaver.sessions.install_archive import pack_mutation


@weaver_test()
def test_hosted_carrier_contains_only_the_sealed_plan_request_and_exact_payloads():
    plan, payloads = build_plan_fixture()
    request = {"plan_id": plan.bundle_id, "invocation_id": "hosted-invocation"}
    carrier = pack_mutation(plan, payloads, request=request)
    assert carrier is not None
    with zipfile.ZipFile(io.BytesIO(carrier.data)) as archive:
        assert set(archive.namelist()) == {"bundle/plan.yml", "request.json"} | {
            "bundle/" + name for name in payloads
        }
        assert json.loads(archive.read("request.json")) == request
        for name, data in payloads.items():
            assert archive.read("bundle/" + name) == data


@weaver_test()
@pytest.mark.parametrize("failed", [False, True])
def test_carrier_bootstrap_calls_the_installed_runtime_without_import_substitution(
    tmp_path, monkeypatch, failed
):
    import importlib
    import importlib.metadata
    import shutil
    import sys
    import types

    from weaver.sessions import archive_runtime
    from weaver.sessions.install_archive import bootstrap_source, read_receipt

    plan, payloads = build_plan_fixture()
    carrier = pack_mutation(plan, payloads)
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
    monkeypatch.setattr(
        importlib,
        "import_module",
        lambda name, *a, **k: (
            types.ModuleType(name)
            if name in ("pyarrow", "yaml", "requests", "azure.identity", "mssql_python")
            else original_import(name, *a, **k)
        ),
    )
    monkeypatch.setattr(importlib.metadata, "version", lambda name: "1.6.6")
    calls = []

    def run(root, spark, destination, sha, *, workers):
        assert {
            p.relative_to(root).as_posix() for p in root.rglob("*") if p.is_file()
        } == set(carrier.manifest)
        assert not (root / "runtime").exists()
        calls.append((spark, destination, sha, workers))
        if failed:
            raise RuntimeError("hosted execution failed")
        return {"status": "completed", "marker": "installed runtime"}

    monkeypatch.setattr(archive_runtime, "run_mutation", run)
    modules = {
        n: m for n, m in sys.modules.items() if n == "weaver" or n.startswith("weaver.")
    }
    paths = list(sys.path)
    spark, emitted = object(), []
    program = bootstrap_source(carrier, str(incoming), str(output), workers=16)
    if failed:
        with pytest.raises(RuntimeError, match="hosted execution failed"):
            exec(program, {"spark": spark, "emit": emitted.append})
    else:
        exec(program, {"spark": spark, "emit": emitted.append})
        assert (
            read_receipt(emitted[0], output.read_bytes())["marker"]
            == "installed runtime"
        )
    assert calls == [(spark, str(output), carrier.sha256, 16)]
    assert all(sys.modules.get(n) is m for n, m in modules.items())
    assert sys.path == paths


@weaver_test()
def test_hosted_bootstrap_rechecks_runtime_identity_before_plan_execution(
    tmp_path, monkeypatch
):
    import importlib
    import importlib.metadata
    import shutil
    import sys
    import types

    from weaver.sessions import archive_runtime
    from weaver.sessions.hosted_runtime import identity
    from weaver.sessions.install_archive import bootstrap_source, read_receipt

    plan, payloads = build_plan_fixture()
    expected = identity()
    expected["version"] += "1"
    carrier = pack_mutation(
        plan, payloads, request={"plan_id": plan.bundle_id, "runtime": expected}
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
    monkeypatch.setattr(
        importlib,
        "import_module",
        lambda name, *a, **k: (
            types.ModuleType(name)
            if name in ("pyarrow", "yaml", "requests", "azure.identity", "mssql_python")
            else original_import(name, *a, **k)
        ),
    )
    monkeypatch.setattr(importlib.metadata, "version", lambda name: "1.6.6")
    calls = []
    monkeypatch.setattr(
        archive_runtime,
        "run_mutation",
        lambda *a, **k: calls.append(a) or {"status": "completed"},
    )
    emitted = []
    exec(
        bootstrap_source(carrier, str(incoming), str(output), workers=16),
        {"spark": object(), "emit": emitted.append},
    )
    result = read_receipt(emitted[0], output.read_bytes())
    assert result["status"] == "declined" and result["mutated"] is False
    assert not calls
