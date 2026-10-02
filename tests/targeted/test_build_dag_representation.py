"""Representative Build plans overlap independent work under capability limits."""

from __future__ import annotations

import graphlib
import threading
import time
from collections import Counter

import pytest
from factories import FixtureCatalogue, item_bindings, target_inventory
from support.representative_lakehouse_estate import (
    RepresentativeLakehouseSpec,
    make_representative_lakehouse_plan,
    parse_representative_lakehouse_estate,
    write_representative_lakehouse_estate,
)
from support.representative_warehouse_estate import (
    RepresentativeWarehouseSpec,
    make_representative_oracle,
    parse_representative_estate,
    write_representative_estate,
)
from support.weaver_test import weaver_test
from support.workspaces import WORKSPACE

from weaver.build_bundle import (
    WarehouseBinding,
    effective_item_bindings,
    generate_item_build_bundle,
)
from weaver.locations import Location
from weaver.mutation.executor import (
    Completed,
    MutationDriver,
    MutationExecutor,
    TypedValue,
)
from weaver.sessions.archive_runtime import execution_capacity
from weaver.store import FilesystemStore
from weaver.targets import ItemRef


def _bundle(tmp_path, engine: str, declarations: int):
    source = tmp_path / "source"
    if engine == "warehouse":
        oracle = make_representative_oracle(
            RepresentativeWarehouseSpec.from_declarations(declarations)
        )
        write_representative_estate(source, oracle)
        repository = parse_representative_estate(source)
        kind = "Warehouse"
    else:
        estate = make_representative_lakehouse_plan(
            RepresentativeLakehouseSpec.from_declarations(declarations)
        )
        write_representative_lakehouse_estate(source, estate)
        repository = parse_representative_lakehouse_estate(source)
        kind = "Lakehouse"
    suffix = "WH" if engine == "warehouse" else "LH"
    bindings = effective_item_bindings(
        item_bindings(
            *(
                (f"{kind}/Representative{n:03d}", f"Representative{n:03d}_{suffix}")
                for n in range(2)
            )
        ),
        control_item=ItemRef("Weaver"),
        workspace_name=WORKSPACE,
    )
    inventories = {}
    for binding in bindings.entries:
        target = binding.to_bound_target()
        inventories[binding.item] = target_inventory(
            target_id=target.id, kind=target.kind, target_name=target.name
        )
    return generate_item_build_bundle(
        repository,
        bindings=bindings,
        output=Location(str(tmp_path / "bundle")),
        store=FilesystemStore(),
        target_inventories=inventories,
        catalogue=FixtureCatalogue.from_registry_rows(),
        catalogue_binding=WarehouseBinding(ItemRef("Weaver"), workspace_name=WORKSPACE),
    )


def _critical_path(plan) -> tuple[int, int]:
    actions = {a.id: a for _s, _b, a in plan.actions()}
    physical = {k for k, a in actions.items() if a.executor != "completion_gate"}
    order = graphlib.TopologicalSorter(
        {k: set(a.depends_on) | set(a.settle_after) for k, a in actions.items()}
    ).static_order()
    depth: dict[str, int] = {}
    for key in order:
        action = actions[key]
        depth[key] = (key in physical) + max(
            (depth[p] for p in (*action.depends_on, *action.settle_after)),
            default=0,
        )
    return len(physical), max(depth.values())


@pytest.mark.parametrize("engine", ["warehouse", "lakehouse"])
@weaver_test()
def test_the_physical_dag_is_far_shorter_than_the_action_count(tmp_path, engine):
    """Branches and items share no edge they do not need, so most work overlaps.

    The longest path is the estate's own: five motifs chain through their
    terminal summaries within a component.
    """

    plan = _bundle(tmp_path, engine, 250).plan

    physical, critical = _critical_path(plan)

    assert physical > 300
    assert critical * 6 < physical


@weaver_test()
def test_representative_lakehouse_work_overlaps_without_exceeding_any_limit(tmp_path):
    bundle = _bundle(tmp_path, "lakehouse", 100)
    plan = bundle.plan
    payloads = {
        a.payload: bundle.store.read(bundle.location.join(*a.payload.split("/")))
        for _s, _b, a in plan.actions()
        if a.payload is not None
    }
    contracts = {c.executor: c for c in plan.driver_contracts}
    _workers, limits = execution_capacity(plan)
    # Gates complete inside the executor; their prerequisites are checked here.
    gates = {a.id for _s, _b, a in plan.actions() if a.executor == "completion_gate"}
    lock = threading.Lock()
    active: Counter = Counter()
    peak: Counter = Counter()
    finished: dict[str, float] = {}
    order_violations = []

    def run(request):
        action = request.action
        with lock:
            for parent in action.depends_on:
                if parent not in finished and parent not in gates:
                    order_violations.append((parent, action.id))
            for key in request.resource_keys:
                active[key] += 1
                peak[key] = max(peak[key], active[key])
        time.sleep(0.002)
        with lock:
            for key in request.resource_keys:
                active[key] -= 1
            finished[action.id] = time.monotonic()
        contract = contracts.get(action.executor)
        if contract and contract.produces:
            return Completed(TypedValue(contract.produces, {"done": True}))
        return Completed()

    drivers = {
        executor: MutationDriver(run, contract=contracts.get(executor))
        for executor in {a.executor for _s, _b, a in plan.actions()}
        if executor != "completion_gate"
    }
    report = MutationExecutor(drivers, workers=32, limits=limits).execute(
        plan, payloads
    )

    assert report.succeeded
    assert order_violations == []
    assert peak["spark"] > 1
    assert all(peak[key] <= limit for key, limit in limits.items())
    assert all(peak[key] == 1 for key in limits if key.startswith("warehouse:"))
