"""Read-only presentation of the canonical Build plan."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass

from ..mutation.models import MutationPlan
from ..mutation.serialization import freeze_value, thaw_value


@dataclass(frozen=True)
class BuildPreview:
    plan: MutationPlan
    objects: tuple

    def to_mapping(self) -> dict:
        envelope = thaw_value(self.plan.build_envelope or {})
        assert isinstance(envelope, dict)
        objects = [thaw_value(value) for value in self.objects]
        changes = defaultdict(list)
        for target_id, values in envelope.get("target_changes", {}).items():
            for change in values:
                changes[change["action_id"]].append({"target_id": target_id, **change})
        removals = {
            (target_id, change["name"])
            for target_id, values in envelope.get("target_changes", {}).items()
            for change in values
            if change["effect"] == "remove"
        }
        removed_nodes = {
            action.resource_node_id
            for _, _, action in self.plan.actions()
            if action.kind.startswith(("drop_", "delete_file"))
            and action.resource_node_id is not None
        }
        actions = [
            _describe_action(
                action,
                sequence,
                batch,
                changes[action.id],
                objects,
                envelope,
                removals,
                removed_nodes,
            )
            for sequence, batch, action in self.plan.actions()
            if action.executor != "completion_gate"
        ]
        return {
            "plan": self.plan.to_mapping(),
            "objects": objects,
            "actions": actions,
            "no_op": not actions,
            "destructive": any(a["destructive"] for a in actions),
            "uncertain": any(a["uncertainty"] for a in actions),
            "certification": envelope.get(
                "certification",
                {
                    "withdraw_before_work": [],
                    "publish_after_success": [],
                    "remove_on_publication": [],
                },
            ),
            "validation_definitions": envelope.get(
                "validation_definitions", {"publish": [], "remove": []}
            ),
            "runtime_state": envelope.get("runtime_state", []),
            "runtime_state_established": envelope.get("runtime_state_established", []),
            "omitted_nodes": envelope.get("omitted_nodes", []),
            "execution_runtime_checked": False,
        }

    def describe(self) -> str:
        mapping = self.to_mapping()
        lines = [
            "Build plan (dry run)",
            f"  Workspace  {self.plan.execution.workspace_name}",
            "  Targets",
        ]
        for target in self.plan.targets:
            logical = (
                f"{target.logical_item_type}/{target.logical_item_name}"
                if target.logical_item_type
                else target.id
            )
            lines.append(f"    {logical} → {target.display}")
        if mapping["no_op"]:
            lines.append("  No changes")
        for obj in mapping["objects"]:
            lines.append(
                f"  {obj['classification']:<10} {obj['identity']}: {obj['reason']}"
            )
        for action in mapping["actions"]:
            marker = " [destructive]" if action["destructive"] else ""
            subject = action["resource_node_id"] or action["target_id"]
            lines.append(
                f"  {action['classification']:<10} {subject}"
                f" ({action['kind']}){marker}: {action['description']}"
            )
            for change in action["physical_changes"]:
                lines.append(
                    f"    {change['classification']}: {change['target_id']}/"
                    f"{change['object_kind']}/{change['name']}"
                )
            for consequence in action["consequences"]:
                lines.append(f"    {consequence}")
            if action["uncertainty"]:
                lines.append(f"    Unknown: {action['uncertainty']}")
            shape = action["table_shape"]
            if shape and shape["declared_columns"] is not None:
                lines.append(f"    Declared columns: {shape['declared_columns']}")
        for name in ("runtime_state", "runtime_state_established"):
            for state in mapping[name]:
                lines.append(f"  {name}: {state['table']} {state['rows']}")
        for when, identities in mapping["certification"].items():
            for identity in identities:
                lines.append(f"  Certification {when}: {identity}")
        for when, definitions in mapping["validation_definitions"].items():
            for definition in definitions:
                lines.append(
                    f"  Validation definition {when}: {definition['object_id']}"
                )
        for node in mapping["omitted_nodes"]:
            lines.append(
                f"  Omitted {node['node_id']}: {node.get('detail') or node['reason']}"
            )
        lines.extend(
            (
                "  Execution runtime compatibility not checked",
                f"  Bundle  {self.plan.bundle_id}",
            )
        )
        return "\n".join(lines)


def _describe_action(
    action, sequence, batch, changes, objects, envelope, removals, removed_nodes
):
    observed = envelope.get("observed_physical_types", {})
    described_changes = []
    for change in changes:
        physical_name = (
            f"_.{change['name']}"
            if change["object_kind"] == "runtime_reference"
            else change["name"]
        )
        identity = next(
            (
                o["identity"]
                for o in objects
                if o["target_id"] == change["target_id"]
                and o["physical_name"] == physical_name
            ),
            action.resource_node_id,
        )
        if change["effect"] == "remove":
            operation = "drop"
        elif identity in observed:
            replacing = (
                identity in removed_nodes
                or (change["target_id"], change["name"]) in removals
            )
            operation = "replace" if replacing else "refresh"
        else:
            operation = "create"
        described_changes.append(
            {**change, "object_id": identity, "classification": operation}
        )
    operations = {c["classification"] for c in described_changes}
    if operations:
        classification = next(iter(operations)) if len(operations) == 1 else "mixed"
    elif action.kind.startswith(("drop_", "prune_", "delete_file")):
        classification = "drop"
    elif action.kind in {"semantic_model", "report_definition"}:
        classification = "replace"
    elif action.kind in {"semantic_readback", "report_readback"}:
        classification = "inspect"
    else:
        classification = "update"
    destructive = (
        "drop" in operations
        or classification == "drop"
        or action.kind in {"semantic_model", "report_definition"}
    )
    consequences = []
    unknowns = []
    if destructive:
        consequences.append("Remove existing target content or definition")
    if action.kind == "drop_shortcut":
        consequences = ["Remove the shortcut, retaining its source content"]
    if "refresh" in operations and action.kind in {
        "create_shortcut",
    }:
        consequences.append("Refresh existing pointers, retaining source content")
    if action.kind == "semantic_model":
        consequences.append(
            "Replace deployed model definition; Fabric may purge model data"
        )
        unknowns.append("Fabric model-data purge is determined at execution")
    if action.kind == "report_definition":
        consequences.append("Replace deployed Report definition")
    if action.executor in {"tsql", "spark_sql"} and action.kind in {
        "build_table",
        "build_view",
        "build_procedure",
    }:
        unknowns.append("Authored SQL effects are determined at execution")
    shape = envelope.get("table_shapes", {}).get(action.id)
    if shape and shape["query_shape_deferred"]:
        unknowns.append(
            "Spark SQL source-query columns and types are inspected at execution"
        )
    if shape and shape["authored_setup_deferred"]:
        unknowns.append("Authored Spark SQL setup effects are determined at execution")
    return {
        **action.to_mapping(),
        "target_id": batch.target_id,
        "description": sequence.description,
        "classification": classification,
        "physical_changes": described_changes,
        "physical_change": described_changes[0]
        if len(described_changes) == 1
        else None,
        "destructive": destructive,
        "consequences": consequences,
        "uncertainty": "; ".join(unknowns) or None,
        "table_shape": shape,
    }


def preview_build(plan, *, repository, bindings, state) -> BuildPreview:
    from ..etl import runtime_artefacts
    from .planner import certifiable_identities

    envelope = thaw_value(plan.build_envelope)
    assert isinstance(envelope, dict)
    selection = envelope["selection"]
    candidates = {
        str(i): i for i in certifiable_identities(repository, bindings.by_item)
    }
    registered = {
        str(i): i
        for i, document in state.catalogue.registered.items()
        if i.item in bindings.by_item and document.object_role != "source"
    }
    old_validations = {
        str(i): i for i in state.catalogue._validations() if i.item in bindings.by_item
    }
    definitions = envelope.get("validation_definitions", {"publish": [], "remove": []})
    published_definitions = {d["object_id"] for d in definitions["publish"]}
    removed_definitions = {d["object_id"] for d in definitions["remove"]}
    categories = (
        (
            "prohibited",
            set(selection["prohibited"]),
            "Prohibit Rebuild retains the existing object",
        ),
        (
            "new",
            set(selection["impact"]["new"]),
            "No certified object in the expected physical form",
        ),
        (
            "changed",
            set(selection["impact"]["changed"]),
            "Source, physical form or upstream generation changed",
        ),
        (
            "impacted",
            set(selection["impact"]["impacted_descendants"]),
            "Depends on changed selected work",
        ),
        (
            "removed",
            set(registered) - set(candidates),
            "No longer declared in the selected item",
        ),
    )
    artefacts = defaultdict(list)
    for artefact in runtime_artefacts(repository):
        if (
            artefact.is_validation
            and str(artefact.identity) in selection["selected_for_build"]
        ):
            artefacts[str(artefact.origin)].append(str(artefact.identity))
    objects = []
    for identity, logical in sorted(
        (registered | old_validations | candidates).items()
    ):
        classification, reason = next(
            (
                (label, reason)
                for label, values, reason in categories
                if identity in values
            ),
            ("unchanged", "Retained by the canonical Build selection"),
        )
        if identity in removed_definitions:
            classification, reason = (
                "removed",
                "Logical validation definition removed from catalogue publication",
            )
        elif identity in published_definitions:
            classification = "changed" if identity in old_validations else "new"
            reason = "Logical validation definition published by the canonical catalogue decision"
        elif identity in artefacts:
            classification, reason = (
                "impacted",
                "Definition retained; generated validation artefacts selected for replacement",
            )
        elif (
            classification == "unchanged"
            and identity in selection["selected_for_build"]
        ):
            reason = "Selected for deployment by the canonical Build selection"
        physical_name = (
            logical.object_id.qualified
            if hasattr(logical, "object_id")
            else logical.schema
        )
        objects.append(
            freeze_value(
                {
                    "identity": identity,
                    "classification": classification,
                    "reason": reason,
                    "target_id": bindings.by_item[logical.item].to_bound_target().id,
                    "physical_name": physical_name,
                    "definition_published": identity in published_definitions,
                    "definition_removed": identity in removed_definitions,
                    "validation_artefacts_selected": sorted(
                        artefacts.get(identity, ())
                    ),
                    "selected_for_build": identity in selection["selected_for_build"],
                    "selected_for_drop": identity in selection["selected_for_drop"],
                }
            )
        )
    return BuildPreview(plan, tuple(objects))
