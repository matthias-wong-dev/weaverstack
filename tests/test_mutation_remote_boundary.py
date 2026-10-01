import hashlib
import io
import json
import zipfile

import pytest
from support.weaver_test import weaver_test
from test_mutation_executor_primitive import sealed
from test_mutation_plan_representation import _action

from weaver.errors import BuildError
from weaver.sessions import install_archive


@weaver_test()
def test_carrier_accepts_plan_and_binary_payload_without_build_bundle():
    data = b"\x00\xff\x80mutation\r\n"
    action = _action(
        "write",
        kind="write_file",
        executor="load_file",
        resource_node_id="Files/Incoming/data.bin",
        payload="payload/data.payload",
        payload_sha256=hashlib.sha256(data).hexdigest(),
    )
    plan = sealed((action,))
    assert hasattr(install_archive, "pack_mutation"), "plan-centric carrier is missing"
    packed = install_archive.pack_mutation(plan, {action.payload: data})
    assert packed is not None
    with zipfile.ZipFile(io.BytesIO(packed.data)) as carrier:
        assert carrier.read("bundle/" + action.payload) == data
        assert carrier.read("bundle/plan.yml")
        request = json.loads(carrier.read("request.json"))
        assert "selected" not in request and "prerequisites" not in request
        assert request["plan_id"] == plan.bundle_id
        assert (
            packed.manifest["bundle/" + action.payload]
            == hashlib.sha256(data).hexdigest()
        )
        assert all("checkpoint" not in name for name in carrier.namelist())


@weaver_test()
def test_lost_remote_response_is_uncertain_without_replay(tmp_path):
    from dataclasses import replace
    from types import SimpleNamespace

    from weaver.locations import Location
    from weaver.mutation import BoundTarget, MutationExecution
    from weaver.store import FilesystemStore

    plan = sealed((_action(),))
    plan = replace(
        plan,
        bundle_id="",
        execution=MutationExecution(
            workspace_name="Demo", spark_home_target_id="sales"
        ),
    )
    from weaver.mutation.bundle import compute_bundle_id

    plan = replace(plan, bundle_id=compute_bundle_id(plan))
    calls = []

    def lost(source, **options):
        calls.append(options)
        raise OSError("remote response lost")

    scope = SimpleNamespace(
        transport_store=FilesystemStore(),
        resolver=SimpleNamespace(lakehouse=lambda item: Location(tmp_path.as_posix())),
        livy_run=lost,
    )
    session = SimpleNamespace(
        scope=lambda workspace: scope,
        require_spark_home=lambda *args, **kwargs: None,
        direct_delta_workers=16,
    )
    stage = install_archive.ArchiveStaging(
        BoundTarget("stage", "lakehouse", "stage-id"), "Files/carriers"
    )
    assert hasattr(install_archive, "execute_mutation_remote"), (
        "remote plan API is missing"
    )
    report = install_archive.execute_mutation_remote(session, plan, staging=(stage,))
    assert len(calls) == 1 and calls[0]["retry_submission"] is False
    assert all(result.status == "uncertain" for result in report.results)
    assert session.archive_mutations[0]["status"] == "uncertain"
    assert "remote response lost" in session.archive_mutations[0]["error"]
    assert scope.transport_store.exists(
        Location(session.archive_mutations[0]["carrier"])
    )


@weaver_test()
def test_numbered_build_stages_emit_dag_with_batch_failure_barrier():
    from test_mutation_plan_representation import _plan

    from weaver.build_bundle.models import BuildBatch, InstallAction
    from weaver.build_bundle.stages import BUILD, PlannedStage, enumerate_stages
    from weaver.mutation import MutationAction, MutationExecutor
    from weaver.mutation.executor import Completed, Failed, MutationDriver

    def action(name):
        return InstallAction(
            name, "build_folder", "Files/Incoming", "folder", None, None
        )

    stages = (
        PlannedStage(
            BUILD,
            "first",
            (BuildBatch("first", "sales", (action("fail"), action("sibling"))),),
        ),
        PlannedStage(
            BUILD, "later", (BuildBatch("later", "sales", (action("later"),)),), index=1
        ),
    )
    sequences, payloads, changes = enumerate_stages(stages)
    physical = [
        a for sequence in sequences for batch in sequence.batches for a in batch.actions
    ]
    assert all(isinstance(a, MutationAction) for a in physical), (
        "Build still emits legacy actions"
    )
    from dataclasses import replace

    from weaver.mutation.bundle import compute_bundle_id

    plan = replace(_plan(()), bundle_id="", sequences=sequences)
    plan = replace(plan, bundle_id=compute_bundle_id(plan))
    calls = []

    def run(request):
        calls.append(request.action.id)
        return (
            Failed("physical failure") if request.action.id == "fail" else Completed()
        )

    report = MutationExecutor({"folder": MutationDriver(run)}).execute(plan, payloads)
    assert calls == ["fail", "sibling"]
    assert report.by_id["later"].status == "blocked"


@weaver_test()
@pytest.mark.parametrize(
    "kind,route", [("warehouse", "native"), ("lakehouse", "remote")]
)
def test_console_owns_whole_plan_routing_without_warehouse_spark_requirement(
    monkeypatch, kind, route
):
    from dataclasses import replace

    from weaver.mutation import BoundTarget
    from weaver.mutation.bundle import compute_bundle_id
    from weaver.sessions import ConsoleSession, archive_runtime
    from weaver.workspaces import Workspace

    plan = sealed((_action(),))
    plan = replace(
        plan, bundle_id="", targets=(BoundTarget("sales", kind, "sales-id"),)
    )
    plan = replace(plan, bundle_id=compute_bundle_id(plan))
    calls = []
    monkeypatch.setattr(
        archive_runtime,
        "execute_mutation",
        lambda p, b, s, **kw: calls.append(("native", p, b)),
    )
    monkeypatch.setattr(
        ConsoleSession,
        "execute_mutation_remote",
        lambda s, p, b, **kw: calls.append(("remote", p, b)),
    )
    session = ConsoleSession(workspace=Workspace(workspace="Demo"))
    session.execute_mutation(plan, {})
    assert calls == [(route, plan, {})]


@weaver_test()
def test_default_codec_accepts_format_five_without_opt_in():
    from weaver.mutation.bundle import plan_from_yaml, plan_to_yaml

    plan = sealed((_action(),))
    assert plan_from_yaml(plan_to_yaml(plan)) == plan


@weaver_test()
def test_default_codec_rejects_old_bundles_with_regeneration_guidance():
    from weaver.mutation.bundle import plan_from_yaml

    with pytest.raises(BuildError, match="Regenerate"):
        plan_from_yaml("format_version: 4\n")
