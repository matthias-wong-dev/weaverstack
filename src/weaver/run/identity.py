"""Confirm that the installations a run's nodes use still name their Fabric items."""

from __future__ import annotations

from ..catalogue.installation import check_installations, recorded_installation
from ..installed import installed_targets
from .result import CATALOGUE_BINDING_INVALID, error

SOURCE = "run.identity"


def identity_messages(catalogue, nodes, *, session, workspace) -> dict:
    """Error messages, by node id, for nodes whose installation is inconsistent.

    Only the installations the nodes use are read.
    """

    on_target: dict = {}
    for item, target in installed_targets(catalogue).items():
        on_target.setdefault(target, []).append(item)

    def items_for(node) -> list:
        found = (
            [node.logical_id.item]
            if node.logical_id is not None
            else list(on_target.get(node.physical_target, ()))
        )
        expected = getattr(node.installed, "expected_target", None)
        return found + list(on_target.get(expected, ())) if expected else found

    used = {node.node_id: items_for(node) for node in nodes}
    rows = {}
    for items in used.values():
        for item in items:
            row = recorded_installation(catalogue, item)
            if row:
                rows[(item.item_type, item.item_name)] = row
    if not rows:
        return {}
    _, faults = check_installations(rows.values(), resolver=session.resolver(workspace))
    messages = {}
    for node_id, items in used.items():
        broken = [
            faults[(item.item_type, item.item_name)]
            for item in items
            if (item.item_type, item.item_name) in faults
        ]
        if broken:
            messages[node_id] = (
                error(CATALOGUE_BINDING_INVALID, str(broken[0]), source=SOURCE),
            )
    return messages
