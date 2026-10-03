import pytest
from support.weaver_test import weaver_test

from weaver.errors import BuildError


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
def test_hashed_receipt_rejects_duplicate_json_fields():
    import hashlib

    from weaver.sessions.install_archive import read_receipt

    data = b'{"status":"running","status":"completed"}'
    with pytest.raises(ValueError, match="duplicate"):
        read_receipt(
            {"bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}, data
        )
