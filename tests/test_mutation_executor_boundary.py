"""The executor takes frozen plans and explicit payload bytes."""

import hashlib

import pytest
from support.weaver_test import weaver_test
from test_mutation_executor_primitive import sealed
from test_mutation_plan_representation import _action

from weaver.errors import BuildError
from weaver.locations import Location
from weaver.mutation.bundle import BuildBundle, write_bundle
from weaver.mutation.executor import Completed, MutationDriver, MutationExecutor
from weaver.store import FilesystemStore


@weaver_test()
@pytest.mark.parametrize("explicit_payloads", [False, True])
def test_bundles_are_rejected_before_store_reads_or_driver_admission(
    tmp_path, explicit_payloads
):
    payload = b"\x00\xfffrozen\r\n"
    plan = sealed(
        (
            _action(
                "binary",
                executor="load_file",
                payload="payload/binary.payload",
                payload_sha256=hashlib.sha256(payload).hexdigest(),
            ),
        )
    )
    reads = []
    calls = []

    class RecordingStore(FilesystemStore):
        def read(self, location):
            reads.append(location)
            return super().read(location)

    store = RecordingStore()
    location = Location(str(tmp_path / "bundle"))
    payloads = {"payload/binary.payload": payload}
    write_bundle(
        location, plan=plan, payloads=payloads, store=store, allow_mutation=True
    )
    reads.clear()
    driver = MutationDriver(
        lambda request: calls.append(request) or Completed(),
        preflight=lambda action, data: calls.append((action, data)),
    )
    with pytest.raises(BuildError, match="requires a MutationPlan"):
        MutationExecutor({"load_file": driver}).execute(
            BuildBundle(location, plan, store), payloads if explicit_payloads else None
        )
    assert reads == []
    assert calls == []
