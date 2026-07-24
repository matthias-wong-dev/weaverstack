"""Build manifest models, canonical serialisation, and bundle validation.

These tests never touch Spark or a repository — they pin the plan/bundle data
contract: a plan round-trips through ``plan.yml``, ``bundle_id`` is a stable
function of content, a written bundle reloads, and loading refuses a corrupt or
malformed one before any action could run. Actions come in two shapes: a
``spark_sql`` action carries a hashed payload; a ``folder`` or ``prune`` action
acts on the target and carries none.
"""

from __future__ import annotations

import hashlib
from dataclasses import replace

import pytest

from weaver import LocalStore, Location
from weaver.build_bundle import (
    BoundTarget,
    BuildAction,
    BuildBatch,
    BuildPlan,
    BuildSequence,
    OmittedNode,
    compute_bundle_id,
    load_bundle,
    plan_from_yaml,
    plan_to_yaml,
    write_bundle,
)
from weaver.errors import BuildError

TARGET = BoundTarget(
    id="lakehouse-Sales_LH", kind="lakehouse", host_kind="local", item_id="Sales_LH"
)

VIEW_PAYLOAD = b"CREATE OR REPLACE VIEW DWG.ActiveCustomer AS\nselect 1\n"
VIEW_PATH = "payload/040-build-view/view-DWG.ActiveCustomer.spark.sql"


def _view_action() -> BuildAction:
    return BuildAction(
        id="view-DWG.ActiveCustomer",
        kind="build_view",
        resource_node_id="delta:DWG.ActiveCustomer",
        executor="spark_sql",
        payload=VIEW_PATH,
        payload_sha256=hashlib.sha256(VIEW_PAYLOAD).hexdigest(),
    )


def _folder_action() -> BuildAction:
    # A folder action reconciles a directory; it carries no payload.
    return BuildAction(
        id="folder-Raw.CustomerCsv",
        kind="build_folder",
        resource_node_id="folder:Raw.CustomerCsv",
        executor="folder",
        payload=None,
        payload_sha256=None,
    )


def _plan(bundle_id: str = "") -> BuildPlan:
    sequences = (
        BuildSequence(
            number=30,
            description="build folders",
            batches=(BuildBatch(id="b-folder", target_id=TARGET.id, actions=(_folder_action(),)),),
        ),
        BuildSequence(
            number=40,
            description="build view",
            batches=(BuildBatch(id="b-view", target_id=TARGET.id, actions=(_view_action(),)),),
        ),
    )
    return BuildPlan(
        format_version=1,
        bundle_id=bundle_id,
        repository_name="MyRepo",
        repository_signature="sig-abc",
        targets=(TARGET,),
        sequences=sequences,
        omitted_nodes=(OmittedNode(node_id="sql:Reporting.Report", reason="target_unbound"),),
    )


def _identified_plan() -> BuildPlan:
    plan = _plan()
    return replace(plan, bundle_id=compute_bundle_id(plan))


def _payloads() -> dict[str, bytes]:
    return {VIEW_PATH: VIEW_PAYLOAD}


# --- serialisation -----------------------------------------------------------


def test_plan_round_trips_through_yaml():
    plan = _identified_plan()
    assert plan_from_yaml(plan_to_yaml(plan)) == plan


def test_bundle_id_is_stable_and_content_addressed():
    first, second = compute_bundle_id(_plan()), compute_bundle_id(_plan())
    assert first == second
    assert len(first) == 64


def test_bundle_id_ignores_the_stored_id_field():
    assert compute_bundle_id(_plan(bundle_id="")) == compute_bundle_id(_plan(bundle_id="stale"))


def test_bundle_id_changes_when_a_payload_hash_changes():
    plan = _plan()
    tampered_action = replace(_view_action(), payload_sha256="0" * 64)
    tampered_batch = BuildBatch(id="b-view", target_id=TARGET.id, actions=(tampered_action,))
    tampered = replace(
        plan, sequences=(plan.sequences[0], replace(plan.sequences[1], batches=(tampered_batch,)))
    )
    assert compute_bundle_id(plan) != compute_bundle_id(tampered)


# --- writing and loading -----------------------------------------------------


def test_write_then_load_returns_an_equal_plan(tmp_path):
    store = LocalStore()
    location = Location(str(tmp_path / "bundle"))
    bundle = write_bundle(
        location,
        plan=_identified_plan(),
        payloads=_payloads(),
        snapshot={"Raw__CustomerCsv.py": b"# snapshot\n"},
        store=store,
    )
    reloaded = load_bundle(location, store=store)
    assert reloaded.plan == bundle.plan
    assert store.exists(location.join("repository", "Raw__CustomerCsv.py"))
    assert store.exists(location.join("plan.yml"))


def test_manifest_is_written_last(tmp_path, monkeypatch):
    store = LocalStore()
    location = Location(str(tmp_path / "bundle"))
    written: list[str] = []
    real_write = store.write

    def recording_write(loc, data):
        written.append(loc.value)
        return real_write(loc, data)

    monkeypatch.setattr(store, "write", recording_write)
    write_bundle(location, plan=_identified_plan(), payloads=_payloads(), snapshot={}, store=store)
    assert written[-1].endswith("plan.yml")


# --- validation --------------------------------------------------------------


def _write_valid(tmp_path):
    store = LocalStore()
    location = Location(str(tmp_path / "bundle"))
    write_bundle(location, plan=_identified_plan(), payloads=_payloads(), snapshot={}, store=store)
    return store, location


def test_load_rejects_a_corrupt_payload(tmp_path):
    store, location = _write_valid(tmp_path)
    store.write(location.join(*VIEW_PATH.split("/")), b"tampered\n")
    with pytest.raises(BuildError, match="hash mismatch"):
        load_bundle(location, store=store)


def test_load_rejects_a_missing_payload(tmp_path):
    store, location = _write_valid(tmp_path)
    store.delete(location.join(*VIEW_PATH.split("/")))
    with pytest.raises(BuildError, match="missing"):
        load_bundle(location, store=store)


def test_load_rejects_a_missing_manifest(tmp_path):
    store = LocalStore()
    location = Location(str(tmp_path / "empty"))
    store.make_directory(location)
    with pytest.raises(BuildError, match="no bundle manifest"):
        load_bundle(location, store=store)


def test_load_rejects_an_unsupported_format_version(tmp_path):
    store = LocalStore()
    location = Location(str(tmp_path / "bundle"))
    plan = replace(_identified_plan(), format_version=2)
    store.write(location.join("plan.yml"), plan_to_yaml(plan).encode("utf-8"))
    with pytest.raises(BuildError, match="format version"):
        load_bundle(location, store=store)


def _validate(plan):
    from weaver.build_bundle.bundle import validate_plan_structure

    validate_plan_structure(plan)


def test_validate_rejects_a_batch_with_unknown_target():
    plan = _identified_plan()
    bad_batch = BuildBatch(id="b-x", target_id="nope", actions=(_view_action(),))
    bad = replace(plan, sequences=(replace(plan.sequences[0], batches=(bad_batch,)),))
    with pytest.raises(BuildError, match="unknown target"):
        _validate(bad)


def test_validate_rejects_duplicate_action_ids():
    plan = _identified_plan()
    dup = replace(_folder_action(), id="view-DWG.ActiveCustomer")  # collides with the view id
    batch = BuildBatch(id="b-dup", target_id=TARGET.id, actions=(dup,))
    bad = replace(plan, sequences=plan.sequences + (BuildSequence(60, "d", (batch,)),))
    with pytest.raises(BuildError, match="duplicate action id"):
        _validate(bad)


def test_validate_rejects_payload_executor_extension_mismatch():
    plan = _identified_plan()
    bad_action = replace(_view_action(), payload="payload/x/thing.py")  # spark_sql wants .spark.sql
    batch = BuildBatch(id="b-x", target_id=TARGET.id, actions=(bad_action,))
    bad = replace(plan, sequences=(replace(plan.sequences[1], batches=(batch,)),))
    with pytest.raises(BuildError, match="extension"):
        _validate(bad)


def test_validate_rejects_a_payload_on_a_payloadless_executor():
    plan = _identified_plan()
    bad_action = replace(_folder_action(), payload="payload/x/thing.spark.sql")
    batch = BuildBatch(id="b-x", target_id=TARGET.id, actions=(bad_action,))
    bad = replace(plan, sequences=(replace(plan.sequences[0], batches=(batch,)),))
    with pytest.raises(BuildError, match="takes no payload"):
        _validate(bad)


def test_validate_rejects_payload_outside_the_bundle():
    plan = _identified_plan()
    bad_action = replace(_view_action(), payload="../escape.spark.sql")
    batch = BuildBatch(id="b-x", target_id=TARGET.id, actions=(bad_action,))
    bad = replace(plan, sequences=(replace(plan.sequences[1], batches=(batch,)),))
    with pytest.raises(BuildError, match="traverse|under"):
        _validate(bad)


def test_validate_rejects_an_action_targeting_an_omitted_node():
    plan = _identified_plan()
    bad_action = replace(_folder_action(), resource_node_id="sql:Reporting.Report")
    batch = BuildBatch(id="b-x", target_id=TARGET.id, actions=(bad_action,))
    bad = replace(plan, sequences=(replace(plan.sequences[0], batches=(batch,)),))
    with pytest.raises(BuildError, match="omitted node"):
        _validate(bad)
