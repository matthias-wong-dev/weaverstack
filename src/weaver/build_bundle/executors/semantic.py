"""Deploy and read back a semantic definition through Session capabilities."""

import json

from ...catalogue.render import InstallationScope, render_delete_scope, render_merge
from ...catalogue.semantic import project_semantic_model
from ...declaration.model import WeaverItemId
from ...errors import InstallError
from ...semantic_models.definition import decode_model, decode_parts
from ...semantic_models.source import SemanticContribution
from ..semantic import SEMANTIC_TABLES


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
        if spec.get("bind_data_sources"):
            client.bind_data_sources()


class SemanticReadbackExecutor:
    name = "semantic_readback"

    def execute(self, action, payload, context):
        spec = json.loads(payload)
        if spec["target_id"] != context.target.bound.id:
            raise InstallError("Semantic readback requires its bound target")
        model = decode_model(
            context.semantic_model(context.target.bound).get_definition()
        )
        from ...semantic_models.deployed import verify_requested

        verify_requested(spec["requested"], model, owned=spec["owned"])
        return {"semantic_definition": model}


class SemanticCatalogueExecutor:
    name = "semantic_catalogue"

    def execute(self, action, payload, context):
        spec = json.loads(payload)
        target = context.resolved(spec["target_id"]).bound
        model = decode_model(context.semantic_model(target).get_definition())
        from ...semantic_models.deployed import verify_requested

        verify_requested(
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
        )
        if contribution.signature != spec["signature"]:
            raise InstallError(f"{item}: semantic payload signature does not match")
        from ...semantic_models.lineage import verify_lineage

        verify_lineage(contribution, model)
        rows = project_semantic_model(item, contribution, deployed=model)
        scope = InstallationScope(item.item_type, item.item_name)
        for table in SEMANTIC_TABLES:
            context.sql.execute_script(render_delete_scope(table, scope=scope))
            statement = render_merge(table, rows[table.name], scope=scope)
            if statement:
                context.sql.execute_script(statement)
        return {"semantic_definition": model}
