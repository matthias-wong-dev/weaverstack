import hashlib
from dataclasses import replace

import pytest
from support.bundles import given_build_plan as BuildPlan
from support.bundles import given_execution, with_catalogue
from support.weaver_test import weaver_test
from support.workspaces import given_workspace

from weaver.build_bundle import (
    BoundTarget,
    BuildBatch,
    BuildSelection,
    BuildSequence,
    Impact,
    InstallAction,
)
from weaver.locations import Location
from weaver.mutation.bundle import write_bundle
from weaver.sessions.testing import TestSession
from weaver.store import FilesystemStore


def build_plan_fixture():
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
    from weaver.mutation.bundle import compute_bundle_id

    plan = BuildPlan(
        format_version=5,
        bundle_id="",
        repository_name="Source",
        repository_signature="signature",
        targets=targets,
        sequences=sequences,
        selection=BuildSelection(Impact((), (), ()), (), (), ()),
        execution=given_execution(targets, sequences),
    )
    return replace(plan, bundle_id=compute_bundle_id(plan)), {file.payload: binary}


@weaver_test()
@pytest.mark.parametrize("failure", [False, True])
def test_native_and_extracted_plan_have_equal_physical_outcomes(tmp_path, failure):
    from weaver.mutation.bundle import load_bundle
    from weaver.sessions import archive_runtime

    plan, payloads = build_plan_fixture()
    states, reports = [], []
    for mode in ("direct", "extracted"):
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
            if mode == "extracted":
                location = Location(str(tmp_path / "artifact"))
                write_bundle(location, plan=plan, payloads=payloads, store=store)
                loaded = load_bundle(location, store=store)
                executing = loaded.plan
                received = {
                    path: store.read(location.join(*path.split("/")))
                    for path in payloads
                }
            else:
                executing, received = plan, payloads
            report = archive_runtime.execute_mutation(
                executing, received, session, workers=2
            )
            reports.append({key: r.status for key, r in report.by_id.items()})
        states.append(
            sorted(
                (
                    p.relative_to(root).as_posix(),
                    p.read_bytes() if p.is_file() else None,
                )
                for p in root.rglob("*")
            )
        )
    assert reports[0] == reports[1] and states[0] == states[1]
    assert reports[1]["second"] == "succeeded"
    assert reports[1]["later"] == ("blocked" if failure else "succeeded")


@weaver_test()
def test_catalogue_free_native_plan_binds_physical_workspace(tmp_path):
    from weaver.mutation.bundle import compute_bundle_id
    from weaver.sessions.archive_runtime import execute_mutation
    from weaver.sessions.mutation_report import decode_report, encode_report

    legacy_plan, payloads = build_plan_fixture()
    plan = legacy_plan
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
@pytest.mark.parametrize("provided", [True, False])
def test_native_invocation_retains_central_catalogue_and_publication_instant(
    tmp_path, provided
):
    import json

    from weaver.sessions.archive_runtime import execute_mutation
    from weaver.sessions.mutation_report import decode_report, encode_report

    legacy_plan, payloads = build_plan_fixture()
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
    from support.bundles import serial_sequences

    from weaver.mutation.bundle import compute_bundle_id

    plan = replace(
        legacy_plan,
        bundle_id="",
        sequences=(
            *legacy_plan.sequences,
            *serial_sequences((BuildSequence(30, "publication", (batch,)),)),
        ),
    )
    plan = replace(plan, bundle_id=compute_bundle_id(plan))
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
        report = execute_mutation(
            plan, payloads, session, build_datetime=instant if provided else None
        )
        assert report.succeeded, [
            (r.action_id, r.status, r.error)
            for r in report.results
            if r.status != "succeeded"
        ]
        if provided:
            assert session.tsql == (
                f"SELECT '{instant}' AS publication",
                f"SELECT '{instant}' AS settlement",
            )
        else:
            from datetime import datetime

            actual = session.tsql[0].split("'")[1]
            datetime.strptime(actual, "%Y-%m-%d %H:%M:%S.%f")
            assert session.tsql == (
                f"SELECT '{actual}' AS publication",
                f"SELECT '{actual}' AS settlement",
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


@weaver_test()
def test_build_report_keeps_physical_result_details(tmp_path):
    from weaver.build_bundle.execution_plan import execute_bundle

    plan, payloads = build_plan_fixture()

    class Resolver:
        def folder_object(self, target, schema, name):
            return Location(str(tmp_path / "Files" / schema / name))

        def files_root(self, item):
            return Location(str(tmp_path / "Files"))

        def folder_root(self, target):
            return Location(str(tmp_path / "Files"))

    store = FilesystemStore()
    bundle = write_bundle(
        Location(str(tmp_path / "artifact")), plan=plan, payloads=payloads, store=store
    )
    with TestSession(
        workspace=given_workspace(), store=store, resolver=Resolver()
    ) as session:
        report = execute_bundle(bundle, session)
    result = next(
        r
        for sequence in report.sequences
        for r in sequence.actions
        if r.action_id == "binary"
    )
    assert result.details == {
        "written": (tmp_path / "Files/Incoming/binary.bin").as_posix(),
        "bytes": len(payloads["payload/binary.payload"]),
    }


@weaver_test()
@pytest.mark.parametrize(
    ("error", "status"),
    [("StoreOutcomeUnknown", "uncertain"), ("StoreError", "failed")],
)
def test_a_lost_response_is_uncertain_and_a_refusal_is_a_known_failure(
    tmp_path, error, status
):
    import weaver.store
    from weaver.sessions import archive_runtime

    plan, payloads = build_plan_fixture()

    class Folder:
        name = "folder"

        def execute(self, action, payload, context):
            if action.id == "first":
                raise getattr(weaver.store, error)("PUT Files/Incoming/first")
            return {}

    class File:
        name = "load_file"

        def execute(self, action, payload, context):
            return {}

    class Resolver:
        def files_root(self, item):
            return Location(str(tmp_path / "Files"))

    with TestSession(
        workspace=given_workspace(), store=FilesystemStore(), resolver=Resolver()
    ) as session:
        report = archive_runtime.execute_mutation(
            plan,
            payloads,
            session,
            workers=1,
            executors={"folder": Folder(), "load_file": File()},
        )

    assert report.by_id["first"].status == status
    # An uncertain write keeps its resource, so nothing else writes there.
    assert report.by_id["second"].status == (
        "succeeded" if status == "failed" else "not_dispatched"
    )


def _capacity_plan():
    from weaver.mutation import (
        BoundTarget,
        MutationAction,
        MutationBatch,
        MutationExecution,
        MutationPlan,
        MutationSequence,
    )

    def action(name, target, resources):
        return MutationAction(
            id=name,
            kind="build_folder",
            resource_node_id=None,
            executor="folder",
            payload=None,
            payload_sha256=None,
            target_id=target,
            depends_on=(),
            resources=resources,
        )

    return MutationPlan(
        targets=(
            BoundTarget("raw", "lakehouse", "raw-id", item_name="Raw_Dev"),
            BoundTarget("sales", "warehouse", "sales-id", item_name="Sales_Dev"),
        ),
        sequences=(
            MutationSequence(
                1,
                "work",
                (
                    MutationBatch(
                        "raw",
                        "raw",
                        (
                            action("files", "raw", ("onelake:raw-id",)),
                            action("links", "raw", ("shortcuts:raw-id",)),
                            action("tables", "raw", ("spark",)),
                        ),
                    ),
                    MutationBatch(
                        "sales",
                        "sales",
                        (action("views", "sales", ("warehouse:sales-id",)),),
                    ),
                ),
            ),
        ),
        execution=MutationExecution(workspace_name="Analytics"),
    )


@weaver_test()
def test_parallel_workers_caps_the_executor_and_a_target_caps_its_own_item():
    from weaver.declaration.model import WeaverItemId
    from weaver.sessions.archive_runtime import (
        ONELAKE_LANES,
        SHORTCUT_API_LANES,
        SPARK_LANES,
        WAREHOUSE_LANES,
        WORKERS,
        execution_capacity,
    )
    from weaver.workspaces import ExecutionSettings, TargetDeclaration, Workspace

    plan = _capacity_plan()

    assert execution_capacity(plan) == (
        WORKERS,
        {
            "onelake:raw-id": ONELAKE_LANES,
            "shortcuts:raw-id": SHORTCUT_API_LANES,
            "spark": SPARK_LANES,
            "warehouse:sales-id": WAREHOUSE_LANES,
        },
    )

    throttled = Workspace(
        workspace="Analytics",
        execution=ExecutionSettings(parallel_workers=2),
        targets={
            WeaverItemId.parse("Lakehouse/Raw"): TargetDeclaration(
                "Raw_Dev", ExecutionSettings(parallel_workers=1)
            )
        },
    )
    workers, limits = execution_capacity(plan, throttled)

    assert workers == 2
    assert limits == {
        "onelake:raw-id": 1,
        "shortcuts:raw-id": 1,
        "spark": min(2, SPARK_LANES),
        "warehouse:sales-id": min(2, WAREHOUSE_LANES),
    }
