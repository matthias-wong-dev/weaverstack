import hashlib
from dataclasses import replace

import pytest
import yaml
from support.weaver_test import weaver_test

from weaver.build_bundle.bundle import (
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
    )
    assert written[-1].endswith("plan.yml")
    assert load_bundle(location, store=store).plan.format_version == 5
    store.write(location.join("payload", "runtime.payload"), b"corrupt")
    with pytest.raises(BuildError, match="checksum"):
        load_bundle(location, store=store)


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
    mapping = _plan().to_mapping()
    if fault == "float_version":
        mapping["format_version"] = 5.0
    elif fault == "legacy_graph_field":
        mapping["format_version"] = 4
    elif fault == "legacy_top_graph":
        mapping["dependency_edges"] = []
    elif fault == "legacy_coerced_flag":
        mapping["sequences"][0]["batches"][0]["actions"][0]["awaits_name_release"] = (
            "false"
        )
    text = yaml.safe_dump(mapping)
    if fault == "invalid_yaml":
        text = "format_version: [5\n"
    elif fault == "duplicate_yaml_field":
        text += "format_version: 5\n"
    with pytest.raises(BuildError):
        plan_from_yaml(text)


@weaver_test()
def test_frozen_mutation_manifest_binds_intent_to_its_identity(tmp_path):
    store = FilesystemStore()
    location = Location(str(tmp_path / "bundle"))
    bundle = write_bundle(
        location,
        plan=_plan(),
        payloads={"payload/runtime.payload": b"\x00\xff"},
        store=store,
    )
    mapping = bundle.plan.to_mapping()
    mapping["sequences"][0]["batches"][0]["actions"][0]["resource_node_id"] = (
        "Files/Other"
    )
    store.write(location.join("plan.yml"), yaml.safe_dump(mapping).encode("utf-8"))
    with pytest.raises(BuildError, match="identity"):
        load_bundle(location, store=store)


@weaver_test()
@pytest.mark.parametrize("boundary", ["construction", "decoded", "direct_validation"])
def test_sealed_mutation_identity_rejects_changed_direct_or_decoded_intent(
    tmp_path, boundary
):
    from weaver.mutation.bundle import compute_bundle_id, validate_bundle

    draft = _plan()
    sealed = replace(draft, bundle_id=compute_bundle_id(draft))
    batch = sealed.sequences[0].batches[0]
    changed_action = replace(batch.actions[0], resource_node_id="Files/Other")
    changed = replace(
        sealed,
        bundle_id="",
        sequences=(
            replace(
                sealed.sequences[0],
                batches=(replace(batch, actions=(changed_action,)),),
            ),
        ),
    )
    assert compute_bundle_id(changed) != sealed.bundle_id
    with pytest.raises(BuildError, match="identity"):
        if boundary == "construction":
            replace(changed, bundle_id=sealed.bundle_id)
        elif boundary == "decoded":
            mapping = changed.to_mapping()
            mapping["bundle_id"] = sealed.bundle_id
            MutationPlan.from_mapping(mapping)
        else:
            # Revalidation must also reject an object whose frozen guard was bypassed.
            object.__setattr__(changed, "bundle_id", sealed.bundle_id)
            validate_bundle(
                Location(str(tmp_path / "direct")), changed, store=FilesystemStore()
            )


@weaver_test()
@pytest.mark.parametrize("boundary", ["direct_validation", "load"])
def test_empty_drafting_identity_is_not_an_execution_bundle(tmp_path, boundary):
    from weaver.mutation.bundle import validate_bundle, validate_plan_structure

    draft = _plan()
    assert draft.bundle_id == ""
    validate_plan_structure(draft)
    assert MutationPlan.from_mapping(draft.to_mapping()) == draft
    store = FilesystemStore()
    location = Location(str(tmp_path / "draft"))
    store.write(location.join("payload", "runtime.payload"), b"\x00\xff")
    store.write(location.join("plan.yml"), plan_to_yaml(draft).encode())
    with pytest.raises(BuildError, match="identity"):
        if boundary == "load":
            load_bundle(location, store=store)
        else:
            validate_bundle(location, draft, store=store)
