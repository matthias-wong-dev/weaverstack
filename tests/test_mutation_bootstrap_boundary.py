"""Execute the generated carrier bootstrap against recording native capabilities."""

import importlib
import shutil
import sys
from dataclasses import replace
from importlib import metadata
from pathlib import Path
from types import SimpleNamespace

import pytest
from support.weaver_test import weaver_test
from support.workspaces import given_workspace
from test_mutation_archive_install import build_plan_fixture as legacy

from weaver.locations import Location
from weaver.mutation import BoundTarget
from weaver.mutation.bundle import compute_bundle_id
from weaver.sessions.install_archive import (
    ArchiveStaging,
    bootstrap_source,
    pack_mutation,
    read_receipt,
)
from weaver.sessions.testing import TestSession
from weaver.store import FilesystemStore


@weaver_test()
@pytest.mark.parametrize("failure", [False, True])
@pytest.mark.parametrize("late", [False, True])
@pytest.mark.parametrize("cleanup", [False, True])
def test_generic_bootstrap_runs_extracted_executor_and_restores_borrowed_namespace(
    tmp_path, monkeypatch, failure, late, cleanup
):
    run_bootstrap(tmp_path, monkeypatch, failure=failure, late=late, cleanup=cleanup)


@weaver_test()
def test_catalogue_free_bootstrap_binds_physical_workspace(tmp_path, monkeypatch):
    run_bootstrap(tmp_path, monkeypatch, catalogue=False)


def run_bootstrap(
    tmp_path, monkeypatch, *, failure=False, late=False, cleanup=False, catalogue=True
):
    legacy_plan, payloads = legacy()
    plan = legacy_plan
    if not catalogue:
        plan = replace(
            plan,
            bundle_id="",
            targets=(replace(plan.targets[0], item_name="Sales"),),
            execution=replace(
                plan.execution, catalogue_target_id=None, spark_home_target_id="sales"
            ),
            build_envelope=None,
        )
        plan = replace(plan, bundle_id=compute_bundle_id(plan))
    request = {
        "plan_id": plan.bundle_id,
        "invocation_id": "invocation",
        "staging": ArchiveStaging(
            BoundTarget("stage", "lakehouse", "stage-id"), "Files/stage"
        ).to_mapping(),
        "build_datetime": "2026-01-01 00:00:00.000000",
        "timeout": 600,
    }
    if late and not failure:
        request["timeout"] = 0.005
    carrier = pack_mutation(plan, payloads, request=request)
    incoming, output = tmp_path / "carrier.zip", tmp_path / "result.json"
    incoming.write_bytes(carrier.data)
    root = tmp_path / "physical"

    class Resolver:
        def __init__(self, physical=root):
            self.physical = physical

        def folder_object(self, target, schema, name):
            return Location(str(self.physical / "Files" / schema / name))

        def files_root(self, item):
            return Location(str(self.physical / "Files"))

        def folder_root(self, target):
            return Location(str(self.physical / "Files"))

    active = []

    class Store(FilesystemStore):
        def make_directory(self, location):
            if late and not failure and location.name == "first":
                import time

                active.append("worker started")
                assert Path(loaded[0]).exists()
                time.sleep(0.03)
                assert Path(loaded[0]).exists(), "runtime removed before worker drained"
                active.append("worker drained")
            return super().make_directory(location)

    store = Store()
    if failure:
        store.make_directory(Location(str(root / "Files/Incoming/first")))
    reference_root = tmp_path / "direct"
    reference_store = FilesystemStore()
    if failure:
        reference_store.make_directory(
            Location(str(reference_root / "Files/Incoming/first"))
        )
    from weaver.sessions.archive_runtime import execute_mutation

    with TestSession(
        workspace=given_workspace(),
        store=reference_store,
        resolver=Resolver(reference_root),
    ) as reference_session:
        reference = execute_mutation(
            plan, payloads, reference_session, build_datetime=request["build_datetime"]
        )
    expected = {r.action_id: r.status for r in reference.results}
    native = SimpleNamespace(
        fs=SimpleNamespace(
            cp=lambda source, dest, recurse: shutil.copyfile(
                source, dest.removeprefix("file:")
            ),
            put=lambda path, text, overwrite: store.write(
                Location(path), text.encode()
            ),
        )
    )
    monkeypatch.setitem(sys.modules, "notebookutils", native)
    original_import = importlib.import_module
    loaded = []

    def importing(name, *args, **kwargs):
        if name in ("pyarrow", "yaml", "requests", "azure.identity", "mssql_python"):
            return SimpleNamespace()
        module = original_import(name, *args, **kwargs)
        if name == "weaver.sessions.archive_runtime":
            loaded.append(module.__file__)

            def archive_session(**options):
                assert (options["workspace"].catalogue is not None) == catalogue
                return TestSession(
                    workspace=options["workspace"], store=store, resolver=Resolver()
                )

            module.ArchiveSession = archive_session
        return module

    monkeypatch.setattr(importlib, "import_module", importing)
    monkeypatch.setattr(metadata, "version", lambda name: "1.5.0")
    from weaver.sessions.delta_profile import WRITER_VERSION
    from weaver.sessions.mutation_report import decode_report

    monkeypatch.setattr(metadata, "version", lambda name: WRITER_VERSION)
    paths = list(sys.path)
    modules = {
        name: module
        for name, module in sys.modules.items()
        if name == "weaver" or name.startswith("weaver.")
    }
    emitted = []
    cleanup_faults = []
    original_cleanup = shutil.rmtree
    if cleanup:

        def fail_cleanup(path, **options):
            if Path(path).name.startswith("weaver-install-"):
                cleanup_faults.append(Path(path))
                raise OSError("private runtime cleanup failed")
            return original_cleanup(path, **options)

        monkeypatch.setattr(shutil, "rmtree", fail_cleanup)
    exec(
        bootstrap_source(carrier, str(incoming), output.as_posix(), workers=2),
        {"spark": object(), "emit": emitted.append},
    )
    result = read_receipt(emitted[0], output.read_bytes())
    assert result["status"] == "completed"
    assert len(cleanup_faults) == int(cleanup)
    if cleanup:
        assert (
            result["runtime_cleanup_failure"]["error"]
            == "private runtime cleanup failed"
        )
        original_cleanup(result["runtime_cleanup_failure"]["path"])
    report = decode_report(plan, result["report"], invocation_id="invocation")
    if late and not failure:
        assert report.by_id["first"].status == "uncertain"
        assert report.by_id["second"].status == "not_dispatched"
        assert report.by_id["later"].status == "blocked"
        assert active == ["worker started", "worker drained"]
    else:
        assert report.by_id["first"].status == ("failed" if failure else "succeeded")
        assert report.by_id["second"].status == "succeeded"
        assert report.by_id["later"].status == ("blocked" if failure else "succeeded")
        assert (root / "Files/Incoming/binary.bin").read_bytes() == payloads[
            "payload/binary.payload"
        ]
        assert {r.action_id: r.status for r in report.results} == expected

        def physical_state(path):
            return sorted(
                (
                    p.relative_to(path).as_posix(),
                    p.read_bytes() if p.is_file() else None,
                )
                for p in path.rglob("*")
            )

        assert physical_state(root) == physical_state(reference_root)

    assert paths == sys.path
    assert modules == {
        name: module
        for name, module in sys.modules.items()
        if name == "weaver" or name.startswith("weaver.")
    }
    assert Path(loaded[0]).parts[-5:] == (
        "expanded",
        "runtime",
        "weaver",
        "sessions",
        "archive_runtime.py",
    )


@weaver_test()
def test_bootstrap_serializes_borrowed_namespace_before_second_carrier_copy(
    tmp_path, monkeypatch
):
    import hashlib
    import io
    import threading
    import zipfile

    from weaver.sessions.install_archive import Carrier

    # Isolation is exercised with the real generated bootstrap; no physical engine is modeled.
    files = {
        "runtime/weaver/__init__.py": b"",
        "runtime/weaver/sessions/__init__.py": b"",
        "runtime/weaver/sessions/archive_runtime.py": b"def run_mutation(*args, **kwargs):\n    return {'status': 'completed'}\n",
    }
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as zipped:
        for name, data in files.items():
            zipped.writestr(name, data)
    carrier = Carrier(
        stream.getvalue(),
        {name: hashlib.sha256(data).hexdigest() for name, data in files.items()},
    )
    incoming = tmp_path / "carrier.zip"
    incoming.write_bytes(carrier.data)
    entered, second_entered, release = (
        threading.Event(),
        threading.Event(),
        threading.Event(),
    )
    copies = []

    def copy(source, dest, recursive):
        copies.append(source)
        if len(copies) == 1:
            entered.set()
            assert release.wait(2)
        else:
            second_entered.set()
        shutil.copyfile(source, dest.removeprefix("file:"))

    monkeypatch.setitem(
        sys.modules,
        "notebookutils",
        SimpleNamespace(fs=SimpleNamespace(cp=copy, put=lambda *args: None)),
    )
    original = importlib.import_module
    monkeypatch.setattr(
        importlib,
        "import_module",
        lambda name, *a, **k: (
            SimpleNamespace()
            if name in ("pyarrow", "yaml", "requests", "azure.identity", "mssql_python")
            else original(name, *a, **k)
        ),
    )
    from weaver.sessions.delta_profile import WRITER_VERSION

    monkeypatch.setattr(metadata, "version", lambda name: WRITER_VERSION)
    errors = []
    program = bootstrap_source(
        carrier, str(incoming), str(tmp_path / "result"), workers=1
    )

    def run():
        try:
            exec(program, {"spark": object(), "emit": lambda value: None})
        except BaseException as error:
            errors.append(error)

    first, second = threading.Thread(target=run), threading.Thread(target=run)
    first.start()
    assert entered.wait(2)
    second.start()
    try:
        assert not second_entered.wait(0.05), (
            "second bootstrap entered borrowed namespace"
        )
    finally:
        release.set()
        first.join(3)
        second.join(3)
    assert not first.is_alive() and not second.is_alive()
    assert errors == []
