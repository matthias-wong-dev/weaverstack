"""Complete plans execute in Fabric; an uncertain invocation is not replayed."""

import hashlib
from dataclasses import replace

from support.weaver_test import weaver_test

from weaver.locations import Location
from weaver.mutation import (
    BoundTarget,
    MutationAction,
    MutationBatch,
    MutationExecution,
    MutationPlan,
    MutationSequence,
    PhysicalScope,
)
from weaver.mutation.bundle import compute_bundle_id, load_bundle
from weaver.mutation.execution import BundleEnvironment
from weaver.store import FilesystemStore
from weaver.targets import ItemRef


def physical_plan(workspace, workspace_item, target, actions):
    environment = workspace.environment
    plan = MutationPlan(
        targets=(
            BoundTarget(
                "target",
                "lakehouse",
                target.id,
                item_name=target.name,
                workspace_id=workspace_item.id,
            ),
        ),
        sequences=(
            MutationSequence(
                1,
                "remote physical actions",
                (MutationBatch("physical", "target", actions),),
            ),
        ),
        execution=MutationExecution(
            workspace_name=workspace.workspace,
            workspace_id=workspace_item.id,
            environment=BundleEnvironment(environment.name, environment.workspace)
            if environment
            else None,
            spark_home_target_id="target",
        ),
    )
    return replace(plan, bundle_id=compute_bundle_id(plan))


@weaver_test(remote=True, resources={"livy", "onelake", "rest", "tds"})
def test_desktop_plan_delivers_binary_bytes_and_reports_physical_failure(
    weaver_session,
    fabric_workspace,
    fabric_workspace_item,
    fabric_target_lakehouse,
    fabric_empty_lakehouse,
    fabric_lakehouse_cleanup,
):
    target = fabric_target_lakehouse
    fabric_lakehouse_cleanup(target.name)
    fabric_empty_lakehouse(target.name)
    data = b"\x00\xff\x80complete-plan\r\n"
    folder = MutationAction(
        id="mkdir",
        kind="build_folder",
        executor="folder",
        target_id="target",
        payload=None,
        payload_sha256=None,
        depends_on=(),
        resource_node_id="Lakehouse/Remote/Files/A3.Remote",
        writes=(PhysicalScope("target", "Files/A3/Remote"),),
    )
    write = MutationAction(
        id="bytes",
        kind="write_file",
        executor="load_file",
        target_id="target",
        resource_node_id="Lakehouse/Remote/file:A3/Remote/payload.bin",
        payload="payload/raw.payload",
        payload_sha256=hashlib.sha256(data).hexdigest(),
        depends_on=("mkdir",),
        writes=(PhysicalScope("target", "Files/A3/Remote/payload.bin"),),
    )
    plan = physical_plan(
        fabric_workspace, fabric_workspace_item, target, (folder, write)
    )
    report = weaver_session.execute_mutation_remote(plan, {write.payload: data})
    assert report.succeeded, [(r.action_id, r.status, r.error) for r in report.results]
    store = weaver_session.transport_store(fabric_workspace)
    resolver = weaver_session.resolver(fabric_workspace)
    deployed = resolver.files_root(ItemRef(target.name)).join(
        "A3", "Remote", "payload.bin"
    )
    assert store.read(deployed) == data
    assert store.exists(resolver.files_root(ItemRef(target.name)).join("A3", "Remote"))
    assert weaver_session.archive_mutations[-1]["action_ids"] == ["mkdir", "bytes"]
    failed = weaver_session.execute_mutation_remote(
        physical_plan(fabric_workspace, fabric_workspace_item, target, (folder,))
    )
    assert failed.by_id["mkdir"].status == "failed"
    assert "already exists" in failed.by_id["mkdir"].error
    assert store.read(deployed) == data


@weaver_test(remote=True, resources={"livy", "onelake", "rest", "tds"})
def test_lost_response_is_uncertain_without_replay_and_normal_build_converges(
    tmp_path,
    monkeypatch,
    weaver_session,
    fabric_workspace,
    fabric_target_lakehouse,
    fabric_empty_lakehouse,
    fabric_lakehouse_cleanup,
    fabric_initialise_catalogue,
):
    from factories import folder_document, single_document_repository

    import weaver
    from weaver.build_bundle.execution import execution_workspace

    target = fabric_target_lakehouse
    fabric_lakehouse_cleanup(target.name)
    fabric_empty_lakehouse(target.name)
    fabric_initialise_catalogue()
    root = tmp_path / "source"
    single_document_repository(
        root,
        item="Lakehouse/Remote",
        schemas=("A3",),
        documents={"Files/A3__Remote.py": folder_document("A3.Remote")},
    )
    bindings = [f"Lakehouse/Remote=Lakehouse/{target.name}"]
    artifact = tmp_path / "planned"
    planned = weaver.build(
        str(root),
        items=bindings,
        session=weaver_session,
        bundle_only=True,
        bundle_path=str(artifact),
    )
    assert planned.succeeded
    local = FilesystemStore()
    bundle = load_bundle(Location(artifact.as_posix()), store=local)
    payloads = {
        a.payload: local.read(bundle.location.join(*a.payload.split("/")))
        for _, _, a in bundle.plan.actions()
        if a.payload is not None
    }
    scope = weaver_session.scope(
        execution_workspace(bundle.plan.execution, bundle.plan)
    )
    run = scope.livy_run
    submissions = []

    def lost_response(source, **options):
        submissions.append(options)
        run(source, **options)
        raise OSError("qualification lost response after remote completion")

    try:
        with monkeypatch.context() as patcher:
            patcher.setattr(scope, "livy_run", lost_response)
            uncertain = weaver_session.execute_mutation_remote(bundle.plan, payloads)
        record = weaver_session.archive_mutations[-1]
        assert len(submissions) == 1 and submissions[0]["retry_submission"] is False
        assert all(r.status == "uncertain" for r in uncertain.results)
        assert record["status"] == "uncertain" and "lost response" in record["error"]
        store = weaver_session.transport_store(fabric_workspace)
        folder = (
            weaver_session.resolver(fabric_workspace)
            .files_root(ItemRef(target.name))
            .join("A3", "Remote")
        )
        assert store.exists(folder)
        assert store.exists(Location(record["carrier"]))
        # A real physical/catalogue disagreement must be repaired by ordinary planning.
        store.delete(folder, recursive=True)
        assert not store.exists(folder)
        repaired = weaver.build(str(root), items=bindings, session=weaver_session)
        assert repaired.succeeded, repaired.errors
        assert store.exists(folder)
        assert "Lakehouse/Remote/Files/A3.Remote" in {
            str(identity) for identity in repaired.selection.selected_for_build
        }
        unchanged = weaver.build(str(root), items=bindings, session=weaver_session)
        assert unchanged.succeeded and unchanged.selection.selected_for_build == ()
        assert unchanged.selection.selected_for_drop == ()
        assert tuple(unchanged.installation_report.action_results()) == ()
        assert len(submissions) == 1
    finally:
        # This injected loss happens after confirmed server completion.
        if "record" in locals():
            weaver_session.transport_store(fabric_workspace).delete(
                Location(record["carrier"].rsplit("/", 1)[0]), recursive=True
            )
