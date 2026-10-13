"""Import rebuilt Python modules through fresh Fabric mount paths."""

from __future__ import annotations

import json

import pytest
from support.weaver_test import weaver_test

from weaver.sessions.program import FabricProgram
from weaver.targets import ItemRef


def import_program(root, relative, *, missing=False):
    body = f"""
from pathlib import Path
from weaver.errors import LoadError
from weaver.lakehouse import Lakehouse, release_mounts
from weaver.runtime.python_context import RuntimeScope, import_deployed_module
release_mounts()
lakehouse = Lakehouse(name="Sales_LH", spark_root={root!r})
runtime = Path(lakehouse.files_root()) / {relative!r}
with RuntimeScope.new() as scope:
    context = scope.context_for(logical_item="Lakehouse/Sales", physical_target="Lakehouse/Sales_LH", runtime_root=runtime)
    try:
        module = import_deployed_module(context, "Files/Raw__CustomerCsv.py", expected="Raw__CustomerCsv", node_id="mount-probe")
    except LoadError as error:
        if not {missing!r} or "deployed module not found" not in str(error):
            raise
        emit({{"runtime": str(runtime), "missing": True}})
    else:
        emit({{"runtime": str(runtime), "stamp": module.Raw__CustomerCsv.stamp,
              "source": (runtime / "Files/Raw__CustomerCsv.py").read_text()}})
"""

    def call():
        emitted = []
        exec(body, {"emit": emitted.append})
        assert len(emitted) == 1
        return emitted[0]

    return FabricProgram("mount_import_probe", call, body, resubmit=False)


@pytest.fixture(scope="module")
def runtime_target(weaver_session, fabric_workspace, fabric_target_lakehouse):
    resolver = weaver_session.resolver(fabric_workspace)
    target = ItemRef(fabric_target_lakehouse.name)
    return (
        resolver.spark_root(target),
        resolver.files_root(target) / "weaver_runtime_import_probe",
        weaver_session.store(fabric_workspace),
    )


@weaver_test(remote=True, resources={"livy", "onelake"})
def test_deployed_modules_refresh_after_an_external_tree_replacement(
    weaver_session, fabric_workspace, runtime_target
):
    root, remote, store = runtime_target
    file = remote.join("Files", "Raw__CustomerCsv.py")
    paths = []
    try:
        for stamp in ("old", "new", "newer"):
            if store.exists(remote):
                store.delete(remote, recursive=True)
            source = f"class Raw__CustomerCsv:\n    stamp = {stamp!r}\n"
            store.write(file, source.encode())
            assert store.read(file) == source.encode()
            seen = weaver_session.execute_python(
                import_program(root, "weaver_runtime_import_probe"),
                workspace=fabric_workspace,
            )
            print("MOUNT_IMPORT_PROBE", json.dumps(seen, sort_keys=True))
            assert seen["stamp"] == stamp
            assert seen["source"] == source
            paths.append(seen["runtime"])
        assert len(set(paths)) == len(paths)
    finally:
        if store.exists(remote):
            store.delete(remote, recursive=True)


@weaver_test(remote=True, resources={"livy", "onelake"})
def test_a_new_deployed_module_is_visible_after_a_cached_miss(
    weaver_session, fabric_workspace, runtime_target
):
    root, remote, store = runtime_target
    file = remote.join("Files", "Raw__CustomerCsv.py")
    source = "class Raw__CustomerCsv:\n    stamp = 'installed'\n"
    try:
        if store.exists(remote):
            store.delete(remote, recursive=True)
        before = weaver_session.execute_python(
            import_program(root, "weaver_runtime_import_probe", missing=True),
            workspace=fabric_workspace,
        )
        assert before["missing"] is True
        store.write(file, source.encode())
        assert store.read(file) == source.encode()
        after = weaver_session.execute_python(
            import_program(root, "weaver_runtime_import_probe"),
            workspace=fabric_workspace,
        )
        assert after["stamp"] == "installed"
        assert after["source"] == source
        assert after["runtime"] != before["runtime"]
    finally:
        if store.exists(remote):
            store.delete(remote, recursive=True)
