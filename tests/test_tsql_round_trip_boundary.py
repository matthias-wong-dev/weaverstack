"""Ready T-SQL actions on one Warehouse share a round trip, each with its outcome."""

from __future__ import annotations

import hashlib
from dataclasses import replace

import pytest
from support.weaver_test import weaver_test

from weaver.build_bundle.executors.base import InstallationContext
from weaver.build_bundle.executors.tsql import TSqlExecutor
from weaver.build_bundle.executors.tsql_round_trip import round_trip_driver
from weaver.mutation import (
    BoundTarget,
    MutationAction,
    MutationBatch,
    MutationExecution,
    MutationPlan,
    MutationSequence,
)
from weaver.mutation.bundle import compute_bundle_id
from weaver.mutation.executor import MutationExecutor, physical_driver
from weaver.sql.round_trip import read_outcomes, round_trip_script


@weaver_test()
def test_each_group_runs_as_dynamic_sql_inside_its_own_try():
    script = round_trip_script(
        [["create view [A].[B] as select 'x' as y;"], ["select 1;", "select 2;"]]
    )

    assert "exec sp_executesql N'create view [A].[B] as select ''x'' as y;';" in script
    assert script.count("begin try") == 2
    assert script.count("begin catch") == 2
    assert script.rstrip().endswith("select @weaver_outcome as outcome;")


@weaver_test()
def test_outcomes_are_read_by_position_and_every_position_is_required():
    reply = f"0{chr(31)}{chr(30)}1{chr(31)}Invalid object name 'X'.{chr(30)}"

    assert read_outcomes(reply, 2) == {0: None, 1: "Invalid object name 'X'."}
    with pytest.raises(ValueError, match="every action"):
        read_outcomes(reply, 3)


class _Warehouse:
    """A Warehouse connection that fails one statement and records round trips."""

    def __init__(self, failing):
        self.failing = failing
        self.round_trips = []

    def execute_each(self, groups):
        self.round_trips.append([statement for group in groups for statement in group])
        return [
            "refused" if any(self.failing in s for s in group) else None
            for group in groups
        ]

    def execute_script(self, script):
        self.round_trips.append([script])


def _plan(scripts, dependent_on):
    actions = []
    payloads = {}
    for name, script in scripts.items():
        data = script.encode()
        path = f"payload/{name}.sql"
        payloads[path] = data
        actions.append(
            MutationAction(
                id=name,
                kind="build_view",
                resource_node_id=None,
                executor="tsql",
                payload=path,
                payload_sha256=hashlib.sha256(data).hexdigest(),
                target_id="sales",
                depends_on=(dependent_on,) if name == "dependent" else (),
                resources=("warehouse:Sales",),
            )
        )
    plan = MutationPlan(
        targets=(BoundTarget("sales", "warehouse", "Sales"),),
        sequences=(
            MutationSequence(
                1, "views", (MutationBatch("b", "sales", tuple(actions)),)
            ),
        ),
        execution=MutationExecution(workspace_name="Demo"),
    )
    return replace(plan, bundle_id=compute_bundle_id(plan)), payloads


@weaver_test()
def test_ready_actions_share_one_round_trip_and_keep_their_own_outcomes():
    warehouse = _Warehouse(failing="[Broken]")
    context = InstallationContext(
        resolver=object(), store=object(), target=None, sql=warehouse
    )
    contexts = {"sales": context}
    plan, payloads = _plan(
        {
            "first": "create view [S].[First] as select 1 as x;",
            "broken": "create view [S].[Broken] as select * from [S].[Missing];",
            "third": "create view [S].[Third] as select 3 as x;",
            "dependent": "create view [S].[Dependent] as select * from [S].[Broken];",
        },
        dependent_on="broken",
    )
    driver = replace(
        physical_driver(TSqlExecutor(), contexts, required_capabilities=()),
        batch=round_trip_driver(contexts, details=lambda action, payload: {}),
        batch_size=25,
    )

    report = MutationExecutor(
        {"tsql": driver}, workers=4, limits={"warehouse:Sales": 1}
    ).execute(plan, payloads)

    assert len(warehouse.round_trips) == 1
    assert len(warehouse.round_trips[0]) == 3
    assert report.by_id["first"].status == "succeeded"
    assert report.by_id["third"].status == "succeeded"
    assert report.by_id["broken"].status == "failed"
    assert report.by_id["broken"].error == "refused"
    assert report.by_id["dependent"].status == "blocked"


@weaver_test()
def test_ready_actions_are_shared_across_the_free_lanes():
    """Eight ready actions and four lanes make four round trips of two."""

    warehouse = _Warehouse(failing="never")
    contexts = {
        "sales": InstallationContext(
            resolver=object(), store=object(), target=None, sql=warehouse
        )
    }
    plan, payloads = _plan(
        {
            f"v{index}": f"create view [S].[V{index}] as select 1 as x;"
            for index in range(8)
        },
        dependent_on="",
    )
    driver = replace(
        physical_driver(TSqlExecutor(), contexts, required_capabilities=()),
        batch=round_trip_driver(contexts, details=lambda action, payload: {}),
        batch_size=25,
    )

    report = MutationExecutor(
        {"tsql": driver}, workers=8, limits={"warehouse:Sales": 4}
    ).execute(plan, payloads)

    assert sorted(len(trip) for trip in warehouse.round_trips) == [2, 2, 2, 2]
    assert all(result.status == "succeeded" for result in report.results)
