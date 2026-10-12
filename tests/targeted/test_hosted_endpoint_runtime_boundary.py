"""Hosted runtime admission uses the same drivers as native execution."""

import pytest
from support.weaver_test import weaver_test
from test_item_build_planner_install import _bundle, _dependency_estate

from weaver.sessions.hosted_runtime import identity, require_compatible


def mixed_plan(tmp_path, *, whole=False):
    from dataclasses import replace

    from weaver.mutation.bundle import compute_bundle_id
    from weaver.store import FilesystemStore

    bundle = _bundle(
        tmp_path,
        _dependency_estate(tmp_path),
        "Lakehouse/Curated",
        "Warehouse/Reporting",
    )
    plan = bundle.plan
    if whole:

        def action(a):
            return (
                replace(
                    a,
                    executor="start_sql_endpoint_refresh",
                    payload=None,
                    payload_sha256=None,
                )
                if a.executor == "start_sql_endpoint_table_refresh"
                else a
            )

        sequences = tuple(
            replace(
                s,
                batches=tuple(
                    replace(b, actions=tuple(action(a) for a in b.actions))
                    for b in s.batches
                ),
            )
            for s in plan.sequences
        )
        plan = replace(plan, bundle_id="", sequences=sequences)
        plan = replace(plan, bundle_id=compute_bundle_id(plan))
    store = FilesystemStore()
    payloads = {
        a.payload: store.read(bundle.location.join(*a.payload.split("/")))
        for _, _, a in plan.actions()
        if a.payload is not None
    }
    return plan, payloads


@weaver_test()
@pytest.mark.parametrize("whole", [False, True])
def test_desktop_admission_accepts_the_complete_registered_refresh_plan(
    tmp_path, monkeypatch, whole
):
    from types import SimpleNamespace

    from weaver.sessions import ConsoleSession
    from weaver.workspaces import Workspace

    plan, _ = mixed_plan(tmp_path, whole=whole)
    observed = identity()
    calls = []
    scope = SimpleNamespace(
        _check_weaver_available=lambda: None,
        livy_run=lambda source, **options: (
            calls.append(source) or {"runtime": observed}
        ),
    )
    session = ConsoleSession(workspace=Workspace(workspace="Demo"))
    monkeypatch.setattr(session, "scope", lambda workspace: scope)
    monkeypatch.setattr(session, "foreground_livy", lambda scope: object())
    assert (
        session.require_build_runtime(plan=plan, workspace=session.workspace)
        == observed
    )
    assert len(calls) == 1


@weaver_test()
def test_genuinely_unsupported_declared_executor_remains_refused(tmp_path):
    from dataclasses import replace

    from weaver.errors import BuildError
    from weaver.mutation import DriverContract

    plan, _ = mixed_plan(tmp_path)
    from weaver.mutation.bundle import compute_bundle_id

    plan = replace(
        plan,
        bundle_id="",
        driver_contracts=(
            *plan.driver_contracts,
            DriverContract("unsupported_refresh", None),
        ),
    )
    plan = replace(plan, bundle_id=compute_bundle_id(plan))
    observed = identity()
    with pytest.raises(BuildError, match="executor capabilities"):
        require_compatible(observed, expected=observed, plan=plan)


@weaver_test()
def test_advertised_capabilities_are_exactly_the_runtime_registration():
    from weaver.sessions.archive_runtime import mutation_drivers

    assert identity()["executors"] == sorted(mutation_drivers({}))


@weaver_test()
def test_mixed_lakehouse_warehouse_build_admits_its_actual_endpoint_drivers(tmp_path):
    bundle = _bundle(
        tmp_path,
        _dependency_estate(tmp_path),
        "Lakehouse/Curated",
        "Warehouse/Reporting",
    )
    refresh = {
        a.executor for _, _, a in bundle.plan.actions() if "sql_endpoint" in a.executor
    }
    assert refresh == {"start_sql_endpoint_table_refresh", "await_sql_endpoint_refresh"}
    observed = identity()
    assert require_compatible(observed, expected=observed, plan=bundle.plan) == observed


@weaver_test()
@pytest.mark.parametrize(
    "name",
    [
        "start_sql_endpoint_refresh",
        "start_sql_endpoint_table_refresh",
        "await_sql_endpoint_refresh",
    ],
)
def test_runtime_advertises_each_registered_endpoint_refresh(name):
    from weaver.build_bundle.executors.sql_endpoint_refresh import (
        endpoint_refresh_drivers,
    )
    from weaver.sessions.archive_runtime import error_outcome

    assert name in endpoint_refresh_drivers({}, outcome=error_outcome)
    assert name in identity()["executors"]
