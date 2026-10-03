"""The executor takes frozen plans and explicit payload bytes."""

import hashlib

import pytest
from support.weaver_test import weaver_test
from test_mutation_executor_primitive import sealed
from test_mutation_plan_representation import _action

from weaver.errors import BuildError
from weaver.locations import Location
from weaver.mutation import MutationPlan
from weaver.mutation.bundle import BuildBundle, load_bundle, write_bundle
from weaver.mutation.executor import (
    Completed,
    MutationDriver,
    MutationExecutor,
    physical_driver,
)
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
    write_bundle(location, plan=plan, payloads=payloads, store=store)
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


@weaver_test()
@pytest.mark.parametrize("mode", ["direct", "decoded", "caller_loaded"])
@pytest.mark.parametrize("fault", ["hash", "non_bytes"])
@pytest.mark.parametrize("first_has_payload", [False, True])
def test_all_payloads_checked_before_physical_preflight(
    tmp_path, mode, fault, first_has_payload
):
    payloads = {"payload/second.payload": b"second"}
    if first_has_payload:
        payloads["payload/first.payload"] = b"first"
    plan = sealed(
        tuple(
            _action(
                name,
                executor="load_file" if path in payloads else "folder",
                payload=path if path in payloads else None,
                payload_sha256=(
                    hashlib.sha256(payloads[path]).hexdigest()
                    if path in payloads
                    else None
                ),
            )
            for name in ("first", "second")
            for path in (f"payload/{name}.payload",)
        )
    )
    if mode == "decoded":
        plan = MutationPlan.from_mapping(plan.to_mapping())
    elif mode == "caller_loaded":
        location = Location(str(tmp_path / "bundle"))
        store = FilesystemStore()
        write_bundle(location, plan=plan, payloads=payloads, store=store)
        bundle = load_bundle(location, store=store)
        plan = bundle.plan
        assert isinstance(plan, MutationPlan)
        payloads = {
            action.payload: store.read(location.join(*action.payload.split("/")))
            for _, _, action in plan.actions()
            if action.payload is not None
        }
    payloads["payload/second.payload"] = (
        b"changed" if fault == "hash" else bytearray(b"second")
    )
    events = []

    class Context:
        @property
        def supplied_capability(self):
            events.append("preflight")
            return object()

    class PhysicalExecutor:
        def execute(self, action, data, context):
            events.append("run")

    driver = physical_driver(
        PhysicalExecutor(),
        {"sales": Context()},
        lane="physical",
        required_capabilities=("supplied_capability",),
    )
    executor = MutationExecutor(
        {"load_file": driver, "folder": driver},
        limits={"physical": 1},
    )
    with pytest.raises(BuildError, match="invalid payload for action 'second'"):
        executor.execute(plan, payloads)
    assert events == []
