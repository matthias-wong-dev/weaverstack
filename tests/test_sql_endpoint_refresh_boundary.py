"""SQL endpoint refresh as a typed operation, and its Fabric REST plumbing."""

from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace

from support.weaver_test import weaver_test

from weaver.build_bundle.executors.base import InstallationContext, ResolvedTarget
from weaver.build_bundle.executors.sql_endpoint_refresh import (
    AWAIT_EXECUTOR,
    CONTRACTS,
    REFRESH_RESULT,
    START_EXECUTOR,
    endpoint_refresh_drivers,
)
from weaver.build_bundle.targets import BoundTarget
from weaver.fabric.client import FabricClient, Operation
from weaver.fabric.resolution import FabricResolver
from weaver.fabric.resources import (
    SQL_ENDPOINT,
    Item,
    observe_sql_endpoint_refresh,
    refresh_sql_endpoint_metadata,
    start_sql_endpoint_refresh,
)
from weaver.mutation import (
    MutationAction,
    MutationBatch,
    MutationExecution,
    MutationPlan,
    MutationSequence,
)
from weaver.mutation.bundle import compute_bundle_id
from weaver.mutation.executor import (
    Completed,
    MutationDriver,
    MutationExecutor,
)
from weaver.mutation.models import ResultReference
from weaver.sessions.archive_runtime import error_outcome
from weaver.store import FilesystemStore
from weaver.targets import ItemRef
from weaver.workspaces import Workspace


class Clock:
    def __init__(self):
        self.now = 0.0

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


def _context(resolver):
    bound = BoundTarget(id="sales", kind="lakehouse", item_id="Sales")
    return InstallationContext(
        resolver=resolver,
        store=FilesystemStore(),
        target=ResolvedTarget(bound=bound, lakehouse=ItemRef("Sales")),
    )


def _plan():
    def action(id, executor, **options):
        return MutationAction(
            id=id,
            kind=executor,
            resource_node_id=None,
            executor=executor,
            payload=None,
            payload_sha256=None,
            target_id="sales",
            **{"depends_on": (), **options},
        )

    actions = (
        action("start", START_EXECUTOR),
        action(
            "await",
            AWAIT_EXECUTOR,
            depends_on=("start",),
            result_from=ResultReference("start", REFRESH_RESULT),
        ),
        action("independent", "folder"),
    )
    plan = MutationPlan(
        targets=(BoundTarget("sales", "lakehouse", "Sales"),),
        sequences=(
            MutationSequence(1, "refresh", (MutationBatch("b", "sales", actions),)),
        ),
        execution=MutationExecution(workspace_name="Analytics"),
        driver_contracts=CONTRACTS,
        required_completion=("await",),
    )
    return replace(plan, bundle_id=compute_bundle_id(plan))


class _Resolver:
    def __init__(self, clock, *, polls=2, fail=False):
        self.clock = clock
        self.polls = polls
        self.fail = fail
        self.calls = []

    def start_sql_endpoint_refresh(self, item):
        self.calls.append(("start", item.name, self.clock.now))
        return {
            "lakehouse": item.name,
            "sql_endpoint_id": "endpoint-id",
            "operation_id": "op",
            "location": "operations/op",
            "retry_after": 5.0,
            "done": False,
            "status": "Running",
        }

    def observe_sql_endpoint_refresh(self, refresh):
        from weaver.fabric.client import FabricError

        self.calls.append(("observe", self.clock.now))
        self.polls -= 1
        if self.fail:
            raise FabricError("Fabric operation op failed: endpoint unavailable")
        return {**refresh, "done": self.polls <= 0, "status": "Succeeded"}


def _execute(resolver, clock):
    drivers = endpoint_refresh_drivers(
        {"sales": _context(resolver)},
        outcome=error_outcome,
        clock=clock,
    )
    ran = []

    def independent(request):
        ran.append(clock.now)
        return Completed()

    drivers["folder"] = MutationDriver(independent)
    report = MutationExecutor(drivers, clock=clock, timeout=600).execute(_plan(), {})
    return report, ran


@weaver_test()
def test_refresh_start_acknowledges_and_await_yields_until_fabric_completes():
    clock = Clock()
    resolver = _Resolver(clock)

    report, ran = _execute(resolver, clock)

    assert report.succeeded
    assert ran == [0.0]
    assert resolver.calls == [
        ("start", "Sales", 0.0),
        ("observe", 0.0),
        ("observe", 5.0),
    ]
    assert report.by_id["await"].observations == 1
    assert report.by_id["await"].value == {
        "lakehouse": "Sales",
        "sql_endpoint_id": "endpoint-id",
        "operation_id": "op",
        "status": "Succeeded",
    }
    assert [op.status for op in report.operations] == ["settled"]


@weaver_test()
def test_a_failed_refresh_is_a_known_failure_that_settles_the_operation():
    clock = Clock()

    report, _ran = _execute(_Resolver(clock, fail=True), clock)

    assert report.by_id["await"].status == "failed"
    assert "endpoint unavailable" in report.by_id["await"].error
    assert [op.status for op in report.operations] == ["settled"]
    assert not report.succeeded


@weaver_test()
def test_fabric_refresh_start_returns_a_handle_without_waiting():
    response = SimpleNamespace(
        status_code=202,
        content=b"",
        headers={
            "x-ms-operation-id": "operation-id",
            "Location": "operations/operation-id",
            "Retry-After": "7",
        },
    )
    requests = []

    class Client:
        def request(self, method, path, *, payload=None, expected):
            requests.append((method, path, payload, expected))
            return response

    endpoint = Item(
        id="endpoint-id", name="Sales", type=SQL_ENDPOINT, workspace_id="workspace-id"
    )

    refresh = start_sql_endpoint_refresh(endpoint, client=Client())

    assert requests == [
        (
            "POST",
            "workspaces/workspace-id/sqlEndpoints/endpoint-id/refreshMetadata",
            {"recreateTables": False},
            (200, 202),
        )
    ]
    assert refresh == {
        "lakehouse": "Sales",
        "sql_endpoint_id": "endpoint-id",
        "operation_id": "operation-id",
        "location": "operations/operation-id",
        "retry_after": 7.0,
        "done": False,
        "status": "Running",
    }


@weaver_test()
def test_fabric_refresh_observation_polls_the_operation_once():
    polled = []

    class Client:
        def poll_operation(self, operation):
            polled.append(operation.location)
            return Operation(
                location=operation.location,
                operation_id=operation.operation_id,
                retry_after=3.0,
                done=True,
                body={"status": "Succeeded"},
            )

    started = {
        "lakehouse": "Sales",
        "sql_endpoint_id": "endpoint-id",
        "operation_id": "op",
        "location": "operations/op",
        "retry_after": 7.0,
        "done": False,
        "status": "Running",
    }

    observed = observe_sql_endpoint_refresh(started, client=Client())

    assert polled == ["operations/op"]
    assert observed["done"] is True
    assert observed["status"] == "Succeeded"


@weaver_test()
def test_blocking_refresh_is_start_then_observe(monkeypatch):
    accepted = SimpleNamespace(
        status_code=202,
        content=b"",
        headers={"x-ms-operation-id": "operation-id", "Retry-After": "0"},
    )

    class Client:
        def request(self, method, path, *, payload=None, expected):
            return accepted

        def poll_operation(self, operation):
            return Operation(
                location=operation.location,
                operation_id=operation.operation_id,
                done=True,
                body={"status": "Succeeded"},
            )

    monkeypatch.setattr("weaver.fabric.resources.time.sleep", lambda _seconds: None)
    endpoint = Item(
        id="endpoint-id", name="Sales", type=SQL_ENDPOINT, workspace_id="workspace-id"
    )

    assert refresh_sql_endpoint_metadata(endpoint, client=Client()) == {
        "lakehouse": "Sales",
        "sql_endpoint_id": "endpoint-id",
        "operation_id": "operation-id",
        "status": "Succeeded",
    }


@weaver_test()
def test_fabric_resolver_uses_the_typed_endpoint_paired_with_the_lakehouse():
    response = SimpleNamespace(
        status_code=200,
        content=b"{}",
        headers={},
        json=lambda: {"status": "Succeeded"},
    )
    requests = []

    class Client:
        def request(self, method, path, *, payload=None, expected):
            requests.append(path)
            return response

        def paged(self, path):
            if path == "workspaces":
                return [{"id": "workspace-id", "displayName": "Analytics"}]
            assert path == "workspaces/workspace-id/items?type=SQLEndpoint"
            return [
                {
                    "id": "endpoint-id",
                    "displayName": "Sales",
                    "type": "SQLEndpoint",
                }
            ]

    resolver = FabricResolver(Workspace(workspace="Analytics"), client=Client())

    result = resolver.refresh_sql_endpoint(ItemRef("Sales"))

    assert result["sql_endpoint_id"] == "endpoint-id"
    assert requests == [
        "workspaces/workspace-id/sqlEndpoints/endpoint-id/refreshMetadata"
    ]


@weaver_test()
def test_fabric_client_waits_for_a_long_running_refresh(monkeypatch):
    accepted = SimpleNamespace(
        status_code=202,
        content=b"",
        headers={
            "Location": "https://api.fabric.microsoft.com/v1/operations/op",
            "x-ms-operation-id": "op",
            "Retry-After": "0",
        },
    )
    completed = SimpleNamespace(
        status_code=200,
        content=b"{}",
        headers={},
        json=lambda: {"status": "Succeeded", "percentComplete": 100},
    )
    client = FabricClient(token="token")
    calls = []

    def request(method, path, *, expected):
        calls.append((method, path, expected))
        return completed

    monkeypatch.setattr(client, "request", request)
    monkeypatch.setattr("weaver.fabric.client.time.sleep", lambda _seconds: None)

    result = client.wait_for_operation(accepted)

    assert result["status"] == "Succeeded"
    assert calls == [
        (
            "GET",
            "https://api.fabric.microsoft.com/v1/operations/op",
            (200,),
        )
    ]
