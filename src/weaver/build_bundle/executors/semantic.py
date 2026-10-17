"""Deploy and read back a semantic definition through Session capabilities."""

import json

from ...catalogue.render import InstallationScope, render_delete_scope, render_merge
from ...catalogue.semantic import project_semantic_model
from ...declaration.model import WeaverItemId
from ...errors import InstallError
from ...semantic_models.definition import decode_model, decode_parts
from ...semantic_models.source import SemanticContribution
from ..semantic import SEMANTIC_TABLES
from .base import read_back, readback_details


class SemanticModelExecutor:
    name = "semantic_model"

    def execute(self, action, payload, context):
        spec = json.loads(payload)
        if (
            spec["target_id"] != context.target.bound.id
            or type(spec["allow_purge_data"]) is not bool
        ):
            raise InstallError(
                "Semantic deployment requires its bound target and a purge option"
            )
        decode_parts(spec["definition"])
        client = context.semantic_model(context.target.bound)
        client.update_definition(
            spec["definition"],
            allow_purge_data=spec["allow_purge_data"],
        )
        details = {}
        if spec.get("bind_data_sources"):
            binding = client.bind_data_sources()
            details["data_sources"] = {
                "bound": list(getattr(binding, "bound", ())),
                "unreached": list(getattr(binding, "unreached", ())),
            }
        if spec.get("refresh"):
            client.refresh()
        details["measure_check"] = require_valid_measures(client, spec["item"])
        return details


def require_valid_measures(client, item) -> str:
    """Fail a deployment whose measures no longer evaluate.

    Only a measure Fabric reports as not `Valid` fails it. When Power BI refuses
    DAX queries, as it does for a service principal without dataset access, the
    check reports that it did not run.
    """

    from ...fabric.client import FabricError

    try:
        invalid = client.invalid_measures()
    except FabricError as exc:
        return f"not run: {exc}"
    if invalid:
        listed = "; ".join(
            f"'{table}'[{measure}]: {reason.rstrip('.')}"
            for table, measure, reason in invalid
        )
        raise InstallError(
            f"{item}: {len(invalid)} measure{'s' if len(invalid) != 1 else ''} "
            f"cannot be evaluated after deployment. {listed}. Fix the DAX or restore "
            "what it references, then build again."
        )
    return "passed"


class SemanticReadbackExecutor:
    name = "semantic_readback"

    def execute(self, action, payload, context):
        spec = json.loads(payload)
        if spec["target_id"] != context.target.bound.id:
            raise InstallError("Semantic readback requires its bound target")
        client = context.semantic_model(context.target.bound)
        model = read_back(spec["item"], lambda: decode_model(client.get_definition()))
        from ...semantic_models.deployed import verify_requested

        differences = verify_requested(
            spec["requested"], model, owned=spec["owned"], absent=spec.get("absent", ())
        )
        return readback_details({"semantic_definition": model}, differences)


class SemanticCatalogueExecutor:
    name = "semantic_catalogue"

    def execute(self, action, payload, context):
        spec = json.loads(payload)
        target = context.resolved(spec["target_id"]).bound
        client = context.semantic_model(target)
        model = read_back(spec["item"], lambda: decode_model(client.get_definition()))
        from ...semantic_models.deployed import verify_requested

        differences = verify_requested(
            spec["requested"], model, owned=spec["owned"], absent=spec.get("absent", ())
        )
        item = WeaverItemId.parse(spec["item"])
        contribution = SemanticContribution(
            parts=decode_parts(spec["definition"]),
            sources={},
            provenance=spec["provenance"],
            requested=spec["requested"],
            owned=tuple(spec["owned"]),
            absent=tuple(
                tuple(tuple(pair) for pair in path) for path in spec.get("absent", ())
            ),
            source_references=spec.get("source_references", {}),
            source_bindings=spec.get("source_bindings", {}),
            expression_sources=spec.get("expression_sources", {}),
            table_order=tuple(spec["table_order"]),
        )
        if contribution.signature != spec["signature"]:
            raise InstallError(f"{item}: semantic payload signature does not match")
        from ...semantic_models.lineage import verify_lineage

        differences += verify_lineage(contribution, model)
        rows = project_semantic_model(item, contribution, deployed=model)
        scope = InstallationScope(item.item_type, item.item_name)
        for table in SEMANTIC_TABLES:
            context.sql.execute_script(render_delete_scope(table, scope=scope))
            statement = render_merge(table, rows[table.name], scope=scope)
            if statement:
                context.sql.execute_script(statement)
        return readback_details({"semantic_definition": model}, differences)
