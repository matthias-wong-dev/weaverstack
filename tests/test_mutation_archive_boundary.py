from dataclasses import replace

import pytest
from support.weaver_test import weaver_test
from test_mutation_executor_primitive import sealed
from test_mutation_plan_representation import _action

from weaver.errors import BuildError
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
    import io
    import zipfile

    from test_mutation_archive_install import build_plan_fixture

    from weaver.sessions.install_archive import pack_mutation

    plan, payloads = build_plan_fixture()
    damaged = {path: data + b"changed" for path, data in payloads.items()}
    with pytest.raises(BuildError, match="invalid payload"):
        pack_mutation(plan, damaged)
    carrier = pack_mutation(plan, payloads)
    with zipfile.ZipFile(io.BytesIO(carrier.data)) as archive:
        for path, data in payloads.items():
            assert archive.read("bundle/" + path) == data
        assert "runtime/weaver/mutation/executor.py" in archive.namelist()


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
