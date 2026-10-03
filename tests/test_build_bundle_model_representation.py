"""Build manifest models, canonical serialisation, and bundle validation.

These tests never touch Spark or a repository. They pin the plan/bundle data
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
import yaml
from support.bundles import given_build_plan as BuildPlan
from support.bundles import given_execution, with_catalogue
from support.weaver_test import weaver_test

from weaver.build_bundle import (
    BoundTarget,
    BuildBatch,
    BuildSelection,
    BuildSequence,
    Impact,
    InstallAction,
    OmittedNode,
    compute_bundle_id,
    load_bundle,
    plan_from_yaml,
    plan_to_yaml,
    write_bundle,
)
from weaver.build_bundle.bundle import SUPPORTED_FORMAT_VERSION
from weaver.errors import BuildError
from weaver.locations import Location
from weaver.store import FilesystemStore

TARGET = BoundTarget(id="lakehouse-Sales_LH", kind="lakehouse", item_id="Sales_LH")

VIEW_PAYLOAD = b"CREATE OR REPLACE VIEW DWG.ActiveCustomer AS\nselect 1\n"
VIEW_PATH = "payload/040-build-view/view-DWG.ActiveCustomer.spark.sql"


def _view_action() -> InstallAction:
    return InstallAction(
        id="view-DWG.ActiveCustomer",
        kind="build_view",
        resource_node_id="delta:DWG.ActiveCustomer",
        executor="spark_sql",
        payload=VIEW_PATH,
        payload_sha256=hashlib.sha256(VIEW_PAYLOAD).hexdigest(),
    )


def _folder_action() -> InstallAction:
    # A folder action reconciles a directory; it carries no payload.
    return InstallAction(
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
            batches=(
                BuildBatch(
                    id="b-folder", target_id=TARGET.id, actions=(_folder_action(),)
                ),
            ),
        ),
        BuildSequence(
            number=40,
            description="build view",
            batches=(
                BuildBatch(id="b-view", target_id=TARGET.id, actions=(_view_action(),)),
            ),
        ),
    )
    targets = with_catalogue((TARGET,))
    return BuildPlan(
        format_version=SUPPORTED_FORMAT_VERSION,
        bundle_id=bundle_id,
        repository_name="MyRepo",
        repository_signature="sig-abc",
        targets=targets,
        sequences=sequences,
        selection=BuildSelection(Impact((), (), ()), (), (), ()),
        execution=given_execution(targets, sequences),
        omitted_nodes=(
            OmittedNode(node_id="sql:Reporting.Report", reason="target_unbound"),
        ),
    )


@weaver_test()
def test_generic_plan_round_trips_without_build_metadata():
    mapping = _plan().to_mapping()
    mapping["build_envelope"] = None
    restored = plan_from_yaml(yaml.safe_dump(mapping))
    assert restored.build_envelope is None


def _identified_plan() -> BuildPlan:
    plan = _plan()
    return replace(plan, bundle_id=compute_bundle_id(plan))


def _payloads() -> dict[str, bytes]:
    return {VIEW_PATH: VIEW_PAYLOAD}


# --- serialisation -----------------------------------------------------------


@weaver_test()
def test_plan_round_trips_through_yaml():
    plan = _identified_plan()
    assert plan_from_yaml(plan_to_yaml(plan)) == plan


@weaver_test()
def test_bound_target_serialises_item_identity_without_runtime_identity():
    target = BoundTarget(
        id="warehouse-Reporting",
        kind="warehouse",
        workspace_id="workspace-id",
        item_id="warehouse-id",
        sql_endpoint_id="endpoint-id",
    )

    assert target.to_mapping() == {
        "id": "warehouse-Reporting",
        "kind": "warehouse",
        "workspace_id": "workspace-id",
        "item_id": "warehouse-id",
        "sql_endpoint_id": "endpoint-id",
    }
    assert BoundTarget.from_mapping(target.to_mapping()) == target


@weaver_test()
def test_bundle_id_is_stable_and_content_addressed():
    first, second = compute_bundle_id(_plan()), compute_bundle_id(_plan())
    assert first == second
    assert len(first) == 64


@weaver_test()
def test_bundle_id_ignores_the_stored_id_field():
    plan = _plan()
    original = compute_bundle_id(plan)
    object.__setattr__(plan, "bundle_id", "stored-id")
    assert compute_bundle_id(plan) == original


@weaver_test()
def test_bundle_id_changes_when_a_payload_hash_changes():
    plan = _plan()
    mapping = plan.to_mapping()
    mapping["sequences"][1]["batches"][0]["actions"][0]["payload_sha256"] = "0" * 64
    changed = plan_from_yaml(yaml.safe_dump(mapping))
    assert compute_bundle_id(changed) != compute_bundle_id(plan)


# --- writing and loading -----------------------------------------------------


@weaver_test()
def test_write_then_load_returns_an_equal_plan(tmp_path):
    store = FilesystemStore()
    location = Location(str(tmp_path / "bundle"))
    bundle = write_bundle(
        location,
        plan=_identified_plan(),
        payloads=_payloads(),
        store=store,
    )
    reloaded = load_bundle(location, store=store)
    assert reloaded.plan == bundle.plan
    # Outputs only. A bundle carries what it installs, never a second copy of
    # the source it was planned from, the installer has no route back to a
    # repository and needs none.
    assert not store.exists(location.join("repository"))
    assert store.exists(location.join("plan.yml"))


@weaver_test()
def test_manifest_is_written_last(tmp_path, monkeypatch):
    store = FilesystemStore()
    location = Location(str(tmp_path / "bundle"))
    written: list[str] = []
    real_write = store.write

    def recording_write(loc, data):
        written.append(loc.value)
        return real_write(loc, data)

    monkeypatch.setattr(store, "write", recording_write)
    write_bundle(location, plan=_identified_plan(), payloads=_payloads(), store=store)
    assert written[-1].endswith("plan.yml")


# --- validation --------------------------------------------------------------


def _write_valid(tmp_path):
    store = FilesystemStore()
    location = Location(str(tmp_path / "bundle"))
    write_bundle(location, plan=_identified_plan(), payloads=_payloads(), store=store)
    return store, location


@weaver_test()
def test_load_rejects_a_corrupt_payload(tmp_path):
    store, location = _write_valid(tmp_path)
    store.write(location.join(*VIEW_PATH.split("/")), b"tampered\n")
    with pytest.raises(BuildError, match="does not match its checksum"):
        load_bundle(location, store=store)


@weaver_test()
def test_load_rejects_a_missing_payload(tmp_path):
    store, location = _write_valid(tmp_path)
    store.delete(location.join(*VIEW_PATH.split("/")))
    with pytest.raises(BuildError, match="missing"):
        load_bundle(location, store=store)


@weaver_test()
def test_load_rejects_a_missing_manifest(tmp_path):
    store = FilesystemStore()
    location = Location(str(tmp_path / "empty"))
    store.make_directory(location)
    with pytest.raises(BuildError, match="no bundle manifest"):
        load_bundle(location, store=store)


@weaver_test()
def test_load_rejects_an_unsupported_format_version(tmp_path):
    store = FilesystemStore()
    location = Location(str(tmp_path / "bundle"))
    mapping = _identified_plan().to_mapping()
    mapping["format_version"] = SUPPORTED_FORMAT_VERSION + 1
    store.write(location / "plan.yml", yaml.safe_dump(mapping).encode())
    with pytest.raises(BuildError, match="format version"):
        load_bundle(location, store=store)


@weaver_test()
def test_a_version_two_bundle_is_refused_rather_than_reinterpreted(tmp_path):
    store = FilesystemStore()
    location = Location(str(tmp_path / "bundle"))
    mapping = _identified_plan().to_mapping()
    mapping["format_version"] = 2
    store.write(location / "plan.yml", yaml.safe_dump(mapping).encode())
    with pytest.raises(BuildError, match="Regenerate the bundle"):
        load_bundle(location, store=store)


def _validate(plan):
    from weaver.build_bundle.bundle import validate_plan_structure

    validate_plan_structure(plan)


@weaver_test()
def test_validate_rejects_a_batch_with_unknown_target():
    mapping = _plan().to_mapping()
    mapping["sequences"][0]["batches"][0]["target_id"] = "nope"
    with pytest.raises(BuildError, match="target"):
        plan_from_yaml(yaml.safe_dump(mapping))


@weaver_test()
def test_validate_rejects_duplicate_action_ids():
    mapping = _plan().to_mapping()
    mapping["sequences"][1]["batches"][0]["actions"][0]["id"] = mapping["sequences"][0][
        "batches"
    ][0]["actions"][0]["id"]
    with pytest.raises(BuildError, match="duplicate|repeats"):
        plan_from_yaml(yaml.safe_dump(mapping))


@weaver_test()
def test_validate_rejects_payload_executor_extension_mismatch():
    mapping = _plan().to_mapping()
    mapping["sequences"][1]["batches"][0]["actions"][0]["payload"] = (
        "payload/x/thing.py"
    )
    with pytest.raises(BuildError, match="must end in"):
        plan_from_yaml(yaml.safe_dump(mapping))


@weaver_test()
def test_validate_rejects_a_payload_on_a_payloadless_executor():
    mapping = _plan().to_mapping()
    mapping["sequences"][0]["batches"][0]["actions"][0]["payload"] = (
        "payload/x/thing.spark.sql"
    )
    with pytest.raises(BuildError, match="unexpected file|payload"):
        plan_from_yaml(yaml.safe_dump(mapping))


@weaver_test()
def test_validate_rejects_payload_outside_the_bundle():
    mapping = _plan().to_mapping()
    mapping["sequences"][1]["batches"][0]["actions"][0]["payload"] = (
        "../escape.spark.sql"
    )
    with pytest.raises(BuildError, match="invalid|unsafe"):
        plan_from_yaml(yaml.safe_dump(mapping))


@weaver_test()
def test_validate_rejects_an_action_targeting_an_omitted_node():
    mapping = _plan().to_mapping()
    mapping["sequences"][0]["batches"][0]["actions"][0]["resource_node_id"] = (
        "sql:Reporting.Report"
    )
    with pytest.raises(BuildError, match="omitted object"):
        plan_from_yaml(yaml.safe_dump(mapping))


@weaver_test()
def test_a_schema_shortcuts_identity_survives_the_manifest():
    """Almost everything a build installs is a document. A schema shortcut is not.

    It presents a namespace rather than an object, so its identity is a
    ``WeaverSchemaId``, and a selection that could not read one back would fail
    the moment a bundle carrying it was reloaded.
    """

    from weaver.declaration.model import WeaverDocumentId, WeaverSchemaId

    schema = WeaverSchemaId.parse("Lakehouse/Curated/Reference")
    table = WeaverDocumentId.parse("Lakehouse/Curated/Tables/Sales.Landed")
    folder = WeaverDocumentId.parse("Lakehouse/Curated/Files/Sales.Incoming")
    selection = BuildSelection(
        impact=Impact(new=(schema, table), changed=(folder,), impacted_descendants=()),
        prohibited=(),
        selected_for_drop=(),
        selected_for_build=(schema, table, folder),
    )

    restored = BuildSelection.from_mapping(selection.to_mapping())

    assert restored == selection
    assert restored.impact.new[0] == schema
