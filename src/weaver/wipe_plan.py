"""Compile a settled :class:`~weaver.operations.wipe.WipePlan` to a MutationPlan.

Each target's authorised destructive scope is frozen here; its children are
enumerated when the action runs. A Warehouse is one dynamic-SQL action. A
Lakehouse area detaches its shortcuts, waits for OneLake to release them, and
is then swept. Targets are independent, so they overlap. A catalogue being
removed, or the claims being unbound, follows every other target.
"""

from __future__ import annotations

import json
from dataclasses import replace

from .build_bundle.executors.wipe import (
    CLEAR_FILES,
    CLEAR_TABLES,
    DETACH_FILE_SHORTCUTS,
    DETACH_TABLE_SHORTCUTS,
)
from .build_bundle.payloads import sha256_hex
from .mutation import (
    BoundTarget,
    MutationAction,
    MutationBatch,
    MutationExecution,
    MutationPlan,
    MutationSequence,
    PhysicalScope,
)
from .mutation.bundle import compute_bundle_id
from .targets import FILES_AREA, TABLES_AREA

WIPE_WAREHOUSE = "wipe_warehouse"
UNBIND_CLAIMS = "unbind_catalogue_claims"


def wipe_mutation_plan(plan, *, unbind_statements=()) -> tuple[MutationPlan, dict]:
    """The plan's physical wipe as a sealed MutationPlan and its payloads."""

    from .operations.wipe import LAKEHOUSE, REMOVE, UNBIND
    from .sql import generate_warehouse_wipe_sql

    workspace = str(plan.workspace.workspace)
    payloads: dict[str, bytes] = {}
    targets: list[BoundTarget] = []
    sequences: list[MutationSequence] = []
    completed: list[str] = []
    catalogue_id = None

    def bound(target) -> BoundTarget:
        kind = "lakehouse" if target.item_type == LAKEHOUSE else "warehouse"
        made = BoundTarget(
            id=f"{kind}-{target.physical_name}",
            kind=kind,
            item_id=target.physical_name,
            item_name=target.physical_name,
            workspace_name=workspace,
        )
        if all(each.id != made.id for each in targets):
            targets.append(made)
        return made

    for number, target in enumerate(plan.targets, start=1):
        physical = bound(target)
        last = plan.catalogue_action == REMOVE and plan.is_catalogue(target)
        if last:
            catalogue_id = physical.id
        after = tuple(completed) if last else ()
        if physical.kind == "warehouse":
            script = generate_warehouse_wipe_sql().encode("utf-8")
            path = f"payload/{number:03d}-wipe/{physical.id}.sql"
            payloads[path] = script
            actions = (
                _action(
                    f"wipe-{physical.id}",
                    WIPE_WAREHOUSE,
                    physical,
                    executor="tsql",
                    payload=path,
                    payload_sha256=sha256_hex(script),
                    depends_on=after,
                    resources=(f"warehouse:{physical.item_id}",),
                    scope="",
                ),
            )
        else:
            actions = _lakehouse_actions(physical, after)
        # A sweep follows its detach, so the sweeps and Warehouse wipes finish a target.
        completed.extend(a.id for a in actions if a.kind not in _DETACHES)
        sequences.append(
            MutationSequence(
                number,
                f"empty {target}",
                (MutationBatch(physical.id, physical.id, actions),),
            )
        )

    if plan.catalogue_action == UNBIND and unbind_statements:
        from .operations.wipe import WipeTarget

        catalogue = bound(WipeTarget.parse(plan.catalogue))
        catalogue_id = catalogue.id
        content = (json.dumps(list(unbind_statements), indent=2) + "\n").encode()
        path = f"payload/{len(sequences) + 1:03d}-unbind/unbind.tsql-batch.json"
        payloads[path] = content
        sequences.append(
            MutationSequence(
                len(sequences) + 1,
                f"unbind catalogue claims in {plan.catalogue}",
                (
                    MutationBatch(
                        "unbind",
                        catalogue.id,
                        (
                            _action(
                                "unbind-catalogue-claims",
                                UNBIND_CLAIMS,
                                catalogue,
                                executor="tsql_batch",
                                payload=path,
                                payload_sha256=sha256_hex(content),
                                depends_on=tuple(completed),
                                resources=(f"warehouse:{catalogue.item_id}",),
                            ),
                        ),
                    ),
                ),
            )
        )

    every = [a.id for s in sequences for b in s.batches for a in b.actions]
    mutation = MutationPlan(
        targets=tuple(targets),
        sequences=tuple(sequences),
        execution=MutationExecution(
            workspace_name=workspace, catalogue_target_id=catalogue_id
        ),
        required_completion=tuple(every),
    )
    return replace(mutation, bundle_id=compute_bundle_id(mutation)), payloads


_DETACHES = frozenset({DETACH_FILE_SHORTCUTS, DETACH_TABLE_SHORTCUTS})


def _lakehouse_actions(physical: BoundTarget, after) -> tuple[MutationAction, ...]:
    actions = []
    for area, detach, clear in (
        (FILES_AREA, DETACH_FILE_SHORTCUTS, CLEAR_FILES),
        (TABLES_AREA, DETACH_TABLE_SHORTCUTS, CLEAR_TABLES),
    ):
        detached = f"{detach.replace('_', '-')}-{physical.id}"
        actions.append(
            _action(
                detached,
                detach,
                physical,
                executor="lakehouse_wipe",
                depends_on=after,
                resources=(f"shortcuts:{physical.item_id}",),
                scope=area,
            )
        )
        actions.append(
            _action(
                f"{clear.replace('_', '-')}-{physical.id}",
                clear,
                physical,
                executor="lakehouse_wipe",
                # Only a successful detach makes the area safe to sweep.
                depends_on=(detached,),
                resources=(f"onelake:{physical.item_id}",),
                scope=area,
            )
        )
    return tuple(actions)


def _action(
    id,
    kind,
    target,
    *,
    executor,
    depends_on,
    resources,
    payload=None,
    payload_sha256=None,
    scope=None,
):
    scopes = () if scope is None else (PhysicalScope(target.id, scope),)
    return MutationAction(
        id=id,
        kind=kind,
        resource_node_id=None,
        executor=executor,
        payload=payload,
        payload_sha256=payload_sha256,
        target_id=target.id,
        depends_on=tuple(depends_on),
        resources=tuple(resources),
        writes=scopes,
        destructive_scopes=scopes,
    )
