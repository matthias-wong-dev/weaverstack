"""Protected Folder and Delta mirrors preserve contents through public Build."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

from support.build_envs import LAKEHOUSE_JOURNEY_FIXTURE
from support.weaver_test import weaver_test

import weaver
from weaver.catalogue.connection import catalogue_connection
from weaver.catalogue.state import read_target_occupancy
from weaver.declaration.model import WeaverItemId
from weaver.fabric.onelake import abfss_root
from weaver.spark import FabricSparkTarget
from weaver.targets import ItemRef

ITEM = "Lakehouse/Sales"
PROTECTED = (("Files", "Raw", "CustomerCsv"), ("Tables", "DWG", "Customer"))


def assert_unchanged_pointers(result):
    assert result.succeeded
    protected = {f"{ITEM}/{area}/{schema}.{name}" for area, schema, name in PROTECTED}
    assert not [
        action
        for action in result.installation_report.action_results()
        if action.resource_node_id in protected
    ]


def tree_snapshot(store, root):
    prefix = root.value.rstrip("/") + "/"
    return {
        entry.location.value[len(prefix) :]: ["directory"]
        if entry.is_directory
        else ["file", hashlib.sha256(store.read(entry.location)).hexdigest()]
        for entry in store.list(root, recursive=True)
    }


def source_snapshot(session, workspace, name):
    resolver, store = session.resolver(workspace), session.store(workspace)
    root = resolver.lakehouse(ItemRef(name))
    return {
        "/".join(parts): tree_snapshot(store, root.join(*parts)) for parts in PROTECTED
    }


def failure_program(source, workspace, target, catalogue):
    options = json.dumps(
        {
            "source": source,
            "workspace": workspace,
            "target": target,
            "catalogue": catalogue,
        }
    )
    return f"""
import json
import weaver
from weaver.fabric.store import FabricStore
from weaver.sessions import NotebookSession
from weaver.workspaces import Workspace
from weaver.errors import InstallError, WeaverError
options = json.loads({options!r})
class FailingCopyStore(FabricStore):
    def copy(self, source, destination):
        super().copy(source, destination)
        if "/_weaver_materialisation/" in destination.value:
            raise InstallError("Injected protected copy failure")
with NotebookSession(spark=spark, store=FailingCopyStore(), workspace=Workspace(workspace=options["workspace"], catalogue="Warehouse/" + options["catalogue"]), progress=False) as native:
    try:
        result = weaver.build(options["source"], items=[{ITEM!r} + "=Lakehouse/" + options["target"]], session=native)
        errors = [a.error_message for a in result.installation_report.action_results() if a.status == "failed"]
        emit({{"succeeded": result.succeeded, "errors": errors}})
    except WeaverError as error:
        emit({{"succeeded": False, "errors": [str(error)]}})
"""


@weaver_test(remote=True, resources={"rest", "tds", "livy", "onelake"})
def test_protected_mirrors_copy_before_cutover_and_converge(
    fabric_workspace,
    fabric_catalogue,
    fabric_fork_catalogue,
    fabric_target_lakehouse,
    fabric_mirror_lakehouse,
    fabric_staging_lakehouse,
    fabric_initialise_catalogue,
    weaver_session,
    tmp_path,
):
    assert fabric_workspace.workspace == "PYTEST_WORKSPACE"
    session = weaver_session
    source_name, target_name = (
        fabric_target_lakehouse.name,
        fabric_mirror_lakehouse.name,
    )
    source_selector, target_selector = (
        f"{ITEM}=Lakehouse/{source_name}",
        f"{ITEM}=Lakehouse/{target_name}",
    )
    resolver, store = (
        session.resolver(fabric_workspace),
        session.store(fabric_workspace),
    )
    source_catalogue, target_catalogue = (
        fabric_catalogue.name,
        fabric_fork_catalogue.name,
    )
    fabric_initialise_catalogue()
    owners = read_target_occupancy(catalogue_connection(session, fabric_workspace))
    assert set(owners.get(("lakehouse", source_name.casefold()), ())) <= {
        WeaverItemId.parse(ITEM)
    }
    estate = LAKEHOUSE_JOURNEY_FIXTURE.disposable(tmp_path / "source")
    paths = [
        estate.path / ITEM / "Files/Raw__CustomerCsv.py",
        estate.path / ITEM / "Tables/DWG__Customer.py",
    ]
    for path, (area, schema, name) in zip(paths, PROTECTED):
        text = path.read_text()
        label = "Folder" if area == "Files" else "Table"
        edited = text.replace(
            f"{label} ID: {schema}.{name}",
            f"{label} ID: {schema}.{name}\nProhibit rebuild: true",
            1,
        )
        assert edited != text
        path.write_bytes(edited.encode())
    records = {}
    source_attempted = False
    mirror_attempted = False
    staged = resolver.files_root(ItemRef(fabric_staging_lakehouse.name)).join(
        "protected-mirror-source"
    )
    try:
        source_attempted = True
        built = weaver.build(str(estate.path), items=[source_selector], session=session)
        assert built.succeeded
        weaver.load([ITEM], session=session)
        source_root = resolver.lakehouse(ItemRef(source_name))
        store.make_directory(source_root.join("Files", "Raw", "CustomerCsv", "empty"))
        store.write(
            source_root.join("Files", "Raw", "CustomerCsv", "binary.bin"),
            b"\x00\xffretained\x80",
        )
        before = source_snapshot(session, fabric_workspace, source_name)
        assert before["Files/Raw/CustomerCsv"] and before["Tables/DWG/Customer"]
        records["source_before"] = before
        mirror_attempted = True
        mirrored = weaver.mirror(
            [target_selector],
            session=session,
            catalogue=f"Warehouse/{target_catalogue}",
            mirror=f"Warehouse/{source_catalogue}",
        )
        assert mirrored.status == "succeeded"
        unchanged = weaver.build(
            str(estate.path),
            items=[target_selector],
            catalogue=f"Warehouse/{target_catalogue}",
            session=session,
        )
        assert_unchanged_pointers(unchanged)
        records["unchanged_actions"] = [
            a.to_mapping() for a in unchanged.installation_report.action_results()
        ]
        assert {f"{area}/{schema}/{name}" for area, schema, name in PROTECTED} <= {
            s.qualified for s in resolver.onelake_shortcuts(ItemRef(target_name))
        }
        assert source_snapshot(session, fabric_workspace, source_name) == before
        for path in paths:
            text = path.read_text()
            path.write_bytes(
                text.replace("Description:", "Description: revised ", 1).encode()
            )
        for path in estate.path.rglob("*"):
            if path.is_file():
                relative = path.relative_to(estate.path).as_posix()
                store.write(staged.join(*relative.split("/")), path.read_bytes())
        remote_source = (
            abfss_root(
                fabric_staging_lakehouse.workspace_id, fabric_staging_lakehouse.id
            )
            + "/Files/protected-mirror-source"
        )
        failed = session.execute_python(
            failure_program(
                remote_source, fabric_workspace.workspace, target_name, target_catalogue
            ),
            workspace=fabric_workspace,
        )
        assert failed["succeeded"] is False and any(
            "Injected protected copy failure" in str(e) for e in failed["errors"]
        )
        held = resolver.onelake_shortcuts(ItemRef(target_name))
        assert {f"{area}/{schema}/{name}" for area, schema, name in PROTECTED} <= {
            s.qualified for s in held
        }
        assert source_snapshot(session, fabric_workspace, source_name) == before
        records["failed_copy"] = failed
        owned = weaver.build(
            str(estate.path),
            items=[target_selector],
            catalogue=f"Warehouse/{target_catalogue}",
            session=session,
        )
        assert owned.succeeded
        target_root = resolver.lakehouse(ItemRef(target_name))
        assert source_snapshot(session, fabric_workspace, target_name) == before
        held = {s.qualified for s in resolver.onelake_shortcuts(ItemRef(target_name))}
        assert not held & {
            f"{area}/{schema}/{name}" for area, schema, name in PROTECTED
        }

        def detail(name):
            return session.execute_spark_sql(
                "DESCRIBE DETAIL "
                + FabricSparkTarget(fabric_workspace.workspace, name).qualify(
                    "DWG", "Customer"
                ),
                workspace=fabric_workspace,
            )[0]

        source_detail, owned_detail = detail(source_name), detail(target_name)
        for key in ("minReaderVersion", "minWriterVersion", "properties"):
            assert source_detail[key] == owned_detail[key]
        records["protocol"] = {
            k: source_detail[k]
            for k in ("minReaderVersion", "minWriterVersion", "properties")
        }
        store.write(
            target_root.join("Files", "Raw", "CustomerCsv", "binary.bin"), b"owned edit"
        )
        qualified = FabricSparkTarget(fabric_workspace.workspace, target_name).qualify(
            "DWG", "Customer"
        )
        session.execute_spark_sql(
            f"UPDATE {qualified} SET CustomerName = 'owned edit'",
            workspace=fabric_workspace,
        )
        assert source_snapshot(session, fabric_workspace, source_name) == before
        repeat = weaver.build(
            str(estate.path),
            items=[target_selector],
            catalogue=f"Warehouse/{target_catalogue}",
            session=session,
        )
        assert repeat.succeeded and not tuple(
            repeat.installation_report.action_results()
        )
        records.update(
            source_unchanged=True, independent_writes=True, repeat_no_op=True
        )
    finally:
        clean = []
        for attempted, name, catalogue in (
            (mirror_attempted, target_name, target_catalogue),
            (source_attempted, source_name, source_catalogue),
        ):
            if not attempted:
                continue
            try:
                result = weaver.wipe(
                    [f"Lakehouse/{name}"],
                    catalogue=f"Warehouse/{catalogue}",
                    session=session,
                    preserve_data_source=False,
                    unbind=True,
                )
                clean.append(
                    {
                        "target": name,
                        "succeeded": all(
                            item.outcome in {"emptied", "preserved"}
                            for item in result.items
                        ),
                        "result": result.to_mapping(),
                    }
                )
            except Exception as error:
                clean.append({"target": name, "succeeded": False, "error": str(error)})
        if store.exists(staged):
            store.delete(staged, recursive=True)
        records["cleanup"] = clean
        output = os.environ.get("WEAVER_MATERIALISATION_LIVE_RESULT")
        if output:
            Path(output).write_text(json.dumps(records, indent=2, default=str) + "\n")
        assert all(row["succeeded"] for row in clean)
