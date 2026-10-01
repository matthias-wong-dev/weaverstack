from dataclasses import replace

import pytest
from support.weaver_test import weaver_test
from test_mutation_executor_primitive import sealed
from test_mutation_plan_representation import _action

from weaver.mutation import BoundTarget, PhysicalScope
from weaver.mutation.bundle import compute_bundle_id


@weaver_test()
@pytest.mark.parametrize("alias", [False, True])
def test_archive_staging_excludes_whole_target_and_protected_source(alias):
    from weaver.sessions import install_archive

    assert hasattr(install_archive, "select_staging"), "safe staging is missing"
    whole = PhysicalScope("sales", "")
    plan = sealed((_action("wipe", writes=(whole,), destructive_scopes=(whole,)),))
    targets = (*plan.targets, BoundTarget("stage", "lakehouse", "other-id"))
    if alias:
        targets += (BoundTarget("alias", "lakehouse", "sales-id"),)
    plan = replace(plan, bundle_id="", targets=targets)
    plan = replace(plan, bundle_id=compute_bundle_id(plan))
    unsafe = PhysicalScope("alias" if alias else "sales", "Files/stage")
    safe = PhysicalScope("stage", "Files/stage")
    assert install_archive.select_staging(plan, (unsafe,)) is None
    assert install_archive.select_staging(plan, (unsafe, safe)) == safe
    source = replace(
        plan, bundle_id="", protected_scopes=(PhysicalScope("stage", "Files"),)
    )
    source = replace(source, bundle_id=compute_bundle_id(source))
    assert install_archive.select_staging(source, (safe,)) is None


@weaver_test()
def test_archive_staging_accepts_disjoint_paths_and_bound_workspaces():
    from weaver.sessions import install_archive

    assert hasattr(install_archive, "select_staging"), "safe staging is missing"
    scope = PhysicalScope("sales", "Tables")
    plan = sealed((_action("wipe", writes=(scope,), destructive_scopes=(scope,)),))
    safe = PhysicalScope("sales", "Files/stage")
    assert install_archive.select_staging(plan, (safe,)) == safe
    with pytest.raises(Exception, match="canonical"):
        install_archive.select_staging(plan, (PhysicalScope("sales", "Files/ stage"),))


@weaver_test()
def test_transitional_actions_without_write_scopes_require_disjoint_staging():
    from weaver.sessions.install_archive import select_staging

    plan = sealed((_action("physical"),))
    assert select_staging(plan, (PhysicalScope("sales", "Files/stage"),)) is None


@weaver_test()
def test_mutation_carrier_validates_payload_before_pack_and_carries_canonical_plan(
    tmp_path,
):
    import hashlib

    from weaver.errors import BuildError
    from weaver.locations import Location
    from weaver.mutation.bundle import plan_to_yaml, write_bundle
    from weaver.sessions.install_archive import extract_verified, pack_bundle
    from weaver.store import FilesystemStore

    payload = b"\x00\xffruntime\r\n"
    action = _action(
        "file",
        executor="load_file",
        payload="payload/file.payload",
        payload_sha256=hashlib.sha256(payload).hexdigest(),
    )
    plan = sealed((action,))
    store = FilesystemStore()
    bundle = write_bundle(
        Location(str(tmp_path / "bundle")),
        plan=plan,
        payloads={action.payload: payload},
        store=store,
        allow_mutation=True,
    )
    request = {
        "plan_id": plan.bundle_id,
        "invocation_id": "invocation",
        "selected": ["file"],
        "prerequisites": [],
        "staging": {
            "target": BoundTarget("stage", "lakehouse", "stage-id").to_mapping(),
            "path": "Files/stage",
        },
        "build_datetime": None,
        "timeout": 600,
    }
    carrier = pack_bundle(bundle, request=request)
    path = tmp_path / "carrier.zip"
    path.write_bytes(carrier.data)
    extract_verified(path, tmp_path / "expanded", carrier.manifest)
    assert (tmp_path / "expanded/bundle/payload/file.payload").read_bytes() == payload
    assert (tmp_path / "expanded/bundle/plan.yml").read_bytes() == plan_to_yaml(
        plan
    ).encode()
    store.write(bundle.location / action.payload, b"tampered")
    with pytest.raises(BuildError, match="checksum"):
        pack_bundle(bundle, request=request)


@weaver_test()
def test_carrier_refuses_duplicate_manifest_fields_before_pack(tmp_path):
    from weaver.errors import BuildError
    from weaver.locations import Location
    from weaver.mutation.bundle import write_bundle
    from weaver.sessions.install_archive import pack_bundle
    from weaver.store import FilesystemStore

    plan = sealed((_action("one"),))
    store = FilesystemStore()
    bundle = write_bundle(
        Location(str(tmp_path / "bundle")),
        plan=plan,
        payloads={},
        store=store,
        allow_mutation=True,
    )
    manifest = bundle.location / "plan.yml"
    store.write(manifest, b"bundle_id: forged\n" + store.read(manifest))
    with pytest.raises(BuildError, match="duplicate"):
        pack_bundle(bundle)


@weaver_test()
def test_carrier_refuses_file_parent_collision_before_extraction(tmp_path):
    import hashlib
    import zipfile

    from weaver.sessions.install_archive import extract_verified

    path = tmp_path / "carrier.zip"
    data = b"content"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("bundle", data)
        archive.writestr("bundle/plan.yml", data)
    destination = tmp_path / "private"
    with pytest.raises(ValueError, match="unsafe"):
        extract_verified(
            path,
            destination,
            {
                name: hashlib.sha256(data).hexdigest()
                for name in ("bundle", "bundle/plan.yml")
            },
        )
    assert not destination.exists()


@weaver_test()
def test_backend_staging_does_not_change_frozen_plan_identity():
    from weaver.sessions import install_archive

    assert hasattr(install_archive, "ArchiveStaging"), (
        "external authorised staging is missing"
    )
    plan = sealed((_action("physical"),))
    identity = plan.bundle_id
    safe = install_archive.ArchiveStaging(
        BoundTarget("external-stage", "lakehouse", "stage-id"), "Files/stage"
    )
    alias = install_archive.ArchiveStaging(
        BoundTarget("external-stage", "lakehouse", "sales-id"), "Files/stage"
    )
    assert install_archive.select_staging(plan, (alias, safe)) == safe
    assert plan.bundle_id == identity == compute_bundle_id(plan)
    assert "external-stage" not in plan.target_ids


@weaver_test()
def test_hashed_receipt_rejects_duplicate_json_fields():
    import hashlib

    from weaver.sessions.install_archive import read_receipt

    data = b'{"status":"running","status":"completed"}'
    with pytest.raises(ValueError, match="duplicate"):
        read_receipt(
            {"bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}, data
        )


@weaver_test()
@pytest.mark.parametrize(
    "fault",
    [
        "identity",
        "selection",
        "missing-dependency",
        "staging",
        "timeout",
        "prerequisite-type",
    ],
)
def test_mutation_carrier_refuses_invalid_hashed_request_before_pack(tmp_path, fault):
    from weaver.errors import BuildError
    from weaver.locations import Location
    from weaver.mutation.bundle import write_bundle
    from weaver.sessions.install_archive import ArchiveStaging, pack_bundle
    from weaver.store import FilesystemStore

    plan = sealed((_action("first"), _action("child", ("first",))))
    bundle = write_bundle(
        Location(str(tmp_path / "bundle")),
        plan=plan,
        payloads={},
        store=FilesystemStore(),
        allow_mutation=True,
    )
    request = {
        "plan_id": plan.bundle_id,
        "invocation_id": "invocation",
        "selected": ["first", "child"],
        "prerequisites": [],
        "staging": ArchiveStaging(
            BoundTarget("stage", "lakehouse", "stage-id"), "Files/stage"
        ).to_mapping(),
        "build_datetime": None,
        "timeout": 600,
    }
    if fault == "identity":
        request["plan_id"] = "forged"
    elif fault == "selection":
        request["selected"] = ["unknown"]
    elif fault == "missing-dependency":
        request["selected"] = ["child"]
    elif fault == "staging":
        request["staging"] = {"target_id": "sales", "path": "Files/stage"}
    elif fault == "timeout":
        request["timeout"] = True
    else:
        request["prerequisites"] = [{"type": "unknown", "fields": {}}]
    with pytest.raises(BuildError):
        pack_bundle(bundle, request=request)
