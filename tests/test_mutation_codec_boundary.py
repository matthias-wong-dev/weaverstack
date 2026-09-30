import hashlib
from dataclasses import replace

import pytest
import yaml
from support.sessions import given_installer
from support.weaver_test import weaver_test

from weaver.build_bundle.bundle import (
    BuildBundle,
    load_bundle,
    plan_from_yaml,
    plan_to_yaml,
    write_bundle,
)
from weaver.errors import BuildError
from weaver.locations import Location
from weaver.mutation import (
    BoundTarget,
    MutationAction,
    MutationBatch,
    MutationExecution,
    MutationPlan,
    MutationSequence,
)
from weaver.store import FilesystemStore


def _plan():
    action = MutationAction(
        id="runtime",
        kind="write_file",
        resource_node_id="Files/runtime",
        executor="load_file",
        payload="payload/runtime.payload",
        payload_sha256=hashlib.sha256(b"\x00\xff").hexdigest(),
        target_id="sales",
        depends_on=(),
    )
    return MutationPlan(
        targets=(BoundTarget("sales", "lakehouse", "sales-id"),),
        sequences=(
            MutationSequence(
                1, "runtime", (MutationBatch("runtime", "sales", (action,)),)
            ),
        ),
        execution=MutationExecution("Demo"),
    )


@weaver_test()
def test_installer_refuses_mutation_format_before_binding(tmp_path, monkeypatch):
    store = FilesystemStore()
    installer = given_installer(store=store)
    bound = []
    monkeypatch.setattr(installer, "_bind", lambda plan: bound.append(plan))
    bundle = BuildBundle(Location(str(tmp_path / "bundle")), _plan(), store)
    with pytest.raises(BuildError, match="format version 5"):
        installer.install(bundle)
    assert bound == []


@weaver_test()
def test_default_codec_refuses_mutation_write_before_any_store_write(
    tmp_path, monkeypatch
):
    store = FilesystemStore()
    written = []
    monkeypatch.setattr(store, "write", lambda *args: written.append(args))
    with pytest.raises(BuildError, match="format version 5"):
        write_bundle(
            Location(str(tmp_path / "bundle")),
            plan=_plan(),
            payloads={"payload/runtime.payload": b"\x00\xff"},
            store=store,
        )
    assert written == []


@weaver_test()
def test_mutation_writer_rejects_unreferenced_escape_before_manifest(
    tmp_path, monkeypatch
):
    store = FilesystemStore()
    written = []
    monkeypatch.setattr(store, "write", lambda *args: written.append(args))
    with pytest.raises(BuildError):
        write_bundle(
            Location(str(tmp_path / "bundle")),
            plan=_plan(),
            payloads={"payload/runtime.payload": b"\x00\xff", "../escape": b"bad"},
            store=store,
            allow_mutation=True,
        )
    assert written == []


@weaver_test()
def test_mutation_manifest_is_last_and_corrupt_payload_is_refused(
    tmp_path, monkeypatch
):
    store = FilesystemStore()
    location = Location(str(tmp_path / "bundle"))
    written = []
    real_write = store.write

    def recording_write(location, data):
        written.append(location.value)
        return real_write(location, data)

    monkeypatch.setattr(store, "write", recording_write)
    write_bundle(
        location,
        plan=_plan(),
        payloads={"payload/runtime.payload": b"\x00\xff"},
        store=store,
        allow_mutation=True,
    )
    assert written[-1].endswith("plan.yml")
    with pytest.raises(BuildError, match="format version 5"):
        load_bundle(location, store=store)
    store.write(location.join("payload", "runtime.payload"), b"corrupt")
    with pytest.raises(BuildError, match="checksum"):
        load_bundle(location, store=store, allow_mutation=True)


@weaver_test()
@pytest.mark.parametrize(
    "fault",
    [
        "float_version",
        "legacy_graph_field",
        "legacy_top_graph",
        "legacy_coerced_flag",
        "invalid_yaml",
        "duplicate_yaml_field",
    ],
)
def test_codec_refuses_ambiguous_legacy_or_serialized_intent(fault):
    from test_mutation_compatibility_representation import _legacy_plan

    if fault == "float_version":
        text = plan_to_yaml(replace(_legacy_plan(), format_version=4.0))
    elif fault == "legacy_graph_field":
        mapping = _legacy_plan().to_mapping()
        mapping["sequences"][0]["batches"][0]["actions"][0]["depends_on"] = []
        text = yaml.safe_dump(mapping)
    elif fault in {"legacy_top_graph", "legacy_coerced_flag"}:
        mapping = _legacy_plan().to_mapping()
        if fault == "legacy_top_graph":
            mapping["dependency_edges"] = []
        else:
            mapping["sequences"][0]["batches"][0]["actions"][0][
                "awaits_name_release"
            ] = "false"
        text = yaml.safe_dump(mapping)
    elif fault == "invalid_yaml":
        text = "format_version: [5\n"
    else:
        text = plan_to_yaml(_legacy_plan()) + "format_version: 4\n"
    with pytest.raises(BuildError):
        plan_from_yaml(text, allow_mutation=True)


@weaver_test()
def test_legacy_installer_refuses_embedded_dag_actions_before_binding(
    tmp_path, monkeypatch
):
    from test_mutation_compatibility_representation import _legacy_plan

    legacy = _legacy_plan()
    batch = legacy.sequences[0].batches[0]
    action = MutationAction(
        **batch.actions[0].to_mapping(), target_id=batch.target_id, depends_on=()
    )
    legacy = replace(
        legacy,
        sequences=(
            replace(legacy.sequences[0], batches=(replace(batch, actions=(action,)),)),
        ),
    )
    installer = given_installer(store=FilesystemStore())
    bound = []
    monkeypatch.setattr(installer, "_bind", lambda plan: bound.append(plan))
    with pytest.raises(BuildError, match="format-4.*DAG"):
        installer.install(BuildBundle(Location(str(tmp_path / "bundle")), legacy))
    assert bound == []


@weaver_test()
def test_frozen_mutation_manifest_binds_intent_to_its_identity(tmp_path):
    store = FilesystemStore()
    location = Location(str(tmp_path / "bundle"))
    bundle = write_bundle(
        location,
        plan=_plan(),
        payloads={"payload/runtime.payload": b"\x00\xff"},
        store=store,
        allow_mutation=True,
    )
    mapping = bundle.plan.to_mapping()
    mapping["sequences"][0]["batches"][0]["actions"][0]["resource_node_id"] = (
        "Files/Other"
    )
    store.write(location.join("plan.yml"), yaml.safe_dump(mapping).encode("utf-8"))
    with pytest.raises(BuildError, match="identity"):
        load_bundle(location, store=store, allow_mutation=True)
