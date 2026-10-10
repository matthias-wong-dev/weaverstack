"""Read-only presentation of the canonical Build plan."""

from __future__ import annotations

from dataclasses import dataclass

from ..mutation.models import MutationPlan
from ..mutation.serialization import freeze_value, thaw_value


@dataclass(frozen=True)
class BuildPreview:
    plan: MutationPlan
    objects: tuple

    def to_mapping(self) -> dict:
        envelope = thaw_value(self.plan.build_envelope or {})
        actions = []
        changes = {
            change["action_id"]: change
            for values in envelope.get("target_changes", {}).values()
            for change in values
        }
        built = set(envelope["selection"]["selected_for_build"])
        dropped = set(envelope["selection"]["selected_for_drop"])
        objects = [thaw_value(value) for value in self.objects]
        removed = {o["identity"] for o in objects if o["classification"] == "removed"}
        for sequence, batch, action in self.plan.actions():
            if action.executor == "completion_gate":
                continue
            change = changes.get(action.id)
            removal = (change is not None and change["effect"] == "remove") or (
                action.kind.startswith(("drop_", "prune_", "delete_file"))
            )
            if removal:
                classification = "drop"
            elif action.resource_node_id in built & dropped:
                classification = "replace"
            elif change is not None and change["effect"] == "add":
                classification = "create"
            elif action.kind in {"semantic_model", "report_definition"}:
                classification = "replace"
            elif action.kind in {"semantic_readback", "report_readback"}:
                classification = "inspect"
            else:
                classification = "update"
            unknown = action.executor in {"tsql", "spark_sql"} and action.kind in {
                "build_table",
                "build_view",
                "build_procedure",
            }
            destructive = removal or action.kind in {
                "semantic_model",
                "report_definition",
            }
            consequences = []
            uncertainty = None
            if removal:
                consequences.append("Remove existing target content or definition")
            if action.kind == "drop_shortcut":
                consequences = ["Remove the shortcut, retaining its source content"]
            if action.kind == "semantic_model":
                consequences.append(
                    "Replace deployed model definition; Fabric may purge model data"
                )
                uncertainty = "Fabric model-data purge is determined at execution"
            if action.kind == "report_definition":
                consequences.append("Replace deployed Report definition")
            if unknown:
                uncertainty = "Authored SQL effects are determined at execution"
            actions.append(
                {
                    **action.to_mapping(),
                    "target_id": batch.target_id,
                    "description": sequence.description,
                    "classification": classification,
                    "physical_change": change,
                    "destructive": destructive,
                    "consequences": consequences,
                    "uncertainty": uncertainty,
                }
            )
        return {
            "plan": self.plan.to_mapping(),
            "objects": objects,
            "actions": actions,
            "no_op": not actions,
            "destructive": any(a["destructive"] for a in actions),
            "uncertain": any(a["uncertainty"] for a in actions),
            "certification": {
                "withdraw_before_work": sorted(built | removed)
                if self.plan.execution.catalogue_target_id
                else [],
                "publish_after_success": sorted(built)
                if self.plan.execution.catalogue_target_id
                else [],
            },
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
            change = action["physical_change"]
            subject = action["resource_node_id"] or (
                f"{action['target_id']}/{change['object_kind']}/{change['name']}"
                if change
                else action["target_id"]
            )
            lines.append(
                f"  {action['classification']:<10} {subject}"
                f" ({action['kind']}){marker}: {action['description']}"
            )
            for consequence in action["consequences"]:
                lines.append(f"    {consequence}")
            if action["uncertainty"]:
                lines.append(f"    Unknown: {action['uncertainty']}")
        for name in ("runtime_state", "runtime_state_established"):
            for state in mapping[name]:
                lines.append(f"  {name}: {state['table']} {state['rows']}")
        certification = mapping["certification"]
        if certification["withdraw_before_work"]:
            lines.append(
                "  Certification withdrawn before work; rebuilt objects certified after successful installation"
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


def preview_build(plan, *, repository, bindings, state) -> BuildPreview:
    from .planner import certifiable_identities

    selection = thaw_value(plan.build_envelope["selection"])
    identities = {str(i) for i in certifiable_identities(repository, bindings.by_item)}
    registered = {
        str(i)
        for i, document in state.catalogue.registered.items()
        if i.item in bindings.by_item and document.object_role != "source"
    }
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
        ("removed", registered - identities, "No longer declared in the selected item"),
    )
    objects = []
    for identity in sorted(identities | registered):
        classification, reason = next(
            (
                (label, reason)
                for label, values, reason in categories
                if identity in values
            ),
            ("unchanged", "Retained by the canonical Build selection"),
        )
        if (
            classification == "unchanged"
            and identity in selection["selected_for_build"]
        ):
            reason = "Selected for deployment by the canonical Build selection"
        objects.append(
            freeze_value(
                {
                    "identity": identity,
                    "classification": classification,
                    "reason": reason,
                    "selected_for_build": identity in selection["selected_for_build"],
                    "selected_for_drop": identity in selection["selected_for_drop"],
                }
            )
        )
    return BuildPreview(plan, tuple(objects))
