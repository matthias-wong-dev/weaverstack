import hashlib
from dataclasses import replace

import pytest
from support.bundles import given_execution, with_catalogue
from support.weaver_test import weaver_test
from support.workspaces import given_workspace

from weaver.build_bundle import (
    BoundTarget,
    BuildBatch,
    BuildPlan,
    BuildSelection,
    BuildSequence,
    Impact,
    InstallAction,
    Installer,
)
from weaver.locations import Location
from weaver.mutation.bundle import write_bundle
from weaver.mutation.compatibility import compile_legacy_build
from weaver.sessions.testing import TestSession
from weaver.store import FilesystemStore


def legacy():
    target = BoundTarget("sales", "lakehouse", "sales-id")

    def folder(name):
        return InstallAction(
            name,
            "build_folder",
            f"Lakehouse/Sales/Files/Incoming.{name}",
            "folder",
            None,
            None,
        )

    binary = b"\x00\xffmodule\r\n"
    file = InstallAction(
        "binary",
        "write_file",
        "Lakehouse/Sales/file:Incoming/binary.bin",
        "load_file",
        "payload/binary.payload",
        hashlib.sha256(binary).hexdigest(),
    )
    sequences = (
        BuildSequence(
            10,
            "first",
            (
                BuildBatch("first", "sales", (folder("first"), folder("second"), file)),
                BuildBatch("later", "sales", (folder("later"),)),
            ),
        ),
        BuildSequence(20, "last", (BuildBatch("last", "sales", (folder("last"),)),)),
    )
    targets = with_catalogue((target,))
    return BuildPlan(
        4,
        "",
        "Source",
        "signature",
        targets,
        sequences,
        BuildSelection(Impact((), (), ()), (), (), ()),
        given_execution(targets, sequences),
    ), {file.payload: binary}


@weaver_test()
@pytest.mark.parametrize("failure", [False, True])
def test_gated_compiled_build_uses_shared_physical_executor_with_failure_parity(
    tmp_path, failure
):
    from weaver.sessions import archive_runtime

    assert hasattr(archive_runtime, "execute_mutation"), (
        "generic native binding is missing"
    )
    plan, payloads = legacy()
    compiled = compile_legacy_build(plan)
    states = []
    reports = []
    for mode in ("legacy", "generic"):
        root = tmp_path / mode
        root.mkdir()

        class Resolver:
            def folder_object(self, target, schema, name):
                return Location(str(root / "Files" / schema / name))

            def files_root(self, item):
                return Location(str(root / "Files"))

            def folder_root(self, target):
                return Location(str(root / "Files"))

        store = FilesystemStore()
        if failure:
            store.make_directory(Location(str(root / "Files/Incoming/first")))
        with TestSession(
            workspace=given_workspace(), store=store, resolver=Resolver()
        ) as session:
            location = Location(str(tmp_path / (mode + "-bundle")))
            bundle = write_bundle(
                location,
                plan=plan if mode == "legacy" else compiled,
                payloads=payloads,
                store=store,
                allow_mutation=mode == "generic",
            )
            if mode == "legacy":
                report = Installer(session).install(bundle)
                outcomes = {
                    r.action_id: r.status for s in report.sequences for r in s.actions
                }
            else:
                report = archive_runtime.execute_mutation(
                    compiled,
                    payloads,
                    session,
                    workers=2,
                    build_datetime="2026-01-01 00:00:00.000000",
                )
                outcomes = {
                    k: "skipped" if r.status == "blocked" else r.status
                    for k, r in report.by_id.items()
                    if not k.startswith("complete-batch:")
                }
            reports.append(outcomes)
        states.append(
            sorted(
                (
                    p.relative_to(root).as_posix(),
                    p.read_bytes() if p.is_file() else None,
                )
                for p in root.rglob("*")
            )
        )
    assert reports[0] == reports[1]
    assert states[0] == states[1]
    assert reports[1]["second"] == "succeeded"
    assert reports[1]["later"] == ("skipped" if failure else "succeeded")


@weaver_test()
def test_catalogue_free_native_plan_binds_physical_workspace(tmp_path):
    from weaver.mutation.bundle import compute_bundle_id
    from weaver.sessions.archive_runtime import execute_mutation
    from weaver.sessions.mutation_receipts import decode_report, encode_report

    legacy_plan, payloads = legacy()
    plan = compile_legacy_build(legacy_plan)
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
    root = tmp_path / "physical"

    class Resolver:
        def folder_object(self, target, schema, name):
            return Location(str(root / "Files" / schema / name))

        def files_root(self, item):
            return Location(str(root / "Files"))

        def folder_root(self, target):
            return Location(str(root / "Files"))

    with TestSession(store=FilesystemStore(), resolver=Resolver()) as session:
        report = execute_mutation(plan, payloads, session)
        from weaver.build_bundle.execution import execution_workspace

        workspace = execution_workspace(plan.execution, plan)
        assert workspace.catalogue is None
        assert session.scope(workspace).spark_home == "Sales"
    assert report.succeeded
    assert (root / "Files/Incoming/binary.bin").read_bytes() == payloads[
        "payload/binary.payload"
    ]
    assert (
        decode_report(plan, encode_report(report), invocation_id=report.invocation_id)
        == report
    )


@weaver_test()
def test_native_invocation_retains_central_catalogue_and_publication_instant(tmp_path):
    import json

    from weaver.sessions.archive_runtime import execute_mutation
    from weaver.sessions.mutation_receipts import decode_report, encode_report

    legacy_plan, payloads = legacy()
    instant = "2026-01-01 00:00:00.000000"
    data = json.dumps(
        [
            "SELECT '{{build_datetime}}' AS publication",
            "SELECT '{{build_datetime}}' AS settlement",
        ]
    ).encode()
    action = InstallAction(
        "publish",
        "publish_registry",
        "Warehouse/Catalogue/Registry",
        "tsql_batch",
        "payload/publish.tsql-batch.json",
        hashlib.sha256(data).hexdigest(),
    )
    batch = BuildBatch("publish", legacy_plan.execution.catalogue_target_id, (action,))
    legacy_plan = replace(
        legacy_plan,
        sequences=(*legacy_plan.sequences, BuildSequence(30, "publication", (batch,))),
    )
    plan = compile_legacy_build(legacy_plan)
    payloads = payloads | {action.payload: data}

    class Resolver:
        def folder_object(self, target, schema, name):
            return Location(str(tmp_path / "Files" / schema / name))

        def files_root(self, item):
            return Location(str(tmp_path / "Files"))

        def folder_root(self, target):
            return Location(str(tmp_path / "Files"))

    class RecordingSession(TestSession):
        def sql_executor(self, target, *, workspace=None):
            from types import SimpleNamespace

            return SimpleNamespace(
                execute_script=lambda statement: self.execute_tsql(
                    statement, target=target, workspace=workspace
                )
            )

    with RecordingSession(
        workspace=given_workspace(), resolver=Resolver(), store=FilesystemStore()
    ) as session:
        report = execute_mutation(plan, payloads, session, build_datetime=instant)
        assert report.succeeded, [
            (r.action_id, r.status, r.error)
            for r in report.results
            if r.status != "succeeded"
        ]
        assert session.tsql == (
            f"SELECT '{instant}' AS publication",
            f"SELECT '{instant}' AS settlement",
        )
        assert all(
            call.detail["target"] is not None
            for call in session.calls
            if call.kind == "tsql"
        )
        assert (
            decode_report(
                plan, encode_report(report), invocation_id=report.invocation_id
            ).results
            == report.results
        )
