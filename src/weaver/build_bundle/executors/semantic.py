"""Deploy and read back a semantic definition through Session capabilities."""

import json

from ...catalogue.render import InstallationScope, render_delete_scope, render_merge
from ...catalogue.semantic import project_semantic_model
from ...declaration.model import WeaverItemId
from ...errors import InstallError
from ...semantic_models.definition import decode_model, encode_definition
from ...semantic_models.source import SemanticContribution
from ..semantic import SEMANTIC_TABLES


class SemanticModelExecutor:
    name = "semantic_model"

    def execute(self, action, payload, context):
        spec = json.loads(payload)
        if (
            spec["target_id"] != context.target.bound.id
            or spec["allow_purge_data"] is not False
        ):
            raise InstallError(
                "Semantic deployment must update its bound target without purging data"
            )
        client = context.semantic_model(context.target.bound)
        client.update_definition(
            encode_definition(spec["model"], properties=spec["properties"]),
            allow_purge_data=False,
        )


class SemanticCatalogueExecutor:
    name = "semantic_catalogue"

    def execute(self, action, payload, context):
        spec = json.loads(payload)
        target = context.resolved(spec["target_id"]).bound
        model = decode_model(context.semantic_model(target).get_definition())
        from ...semantic_models.deployed import verify_deployed

        verify_deployed(spec["model"], model)
        item = WeaverItemId.parse(spec["item"])
        contribution = SemanticContribution(
            spec["model"],
            {},
            spec["provenance"],
            spec["properties"],
            spec.get("source_references", {}),
            spec.get("source_bindings", {}),
        )
        if contribution.signature != spec["signature"]:
            raise InstallError(f"{item}: semantic payload signature does not match")
        rows = project_semantic_model(item, contribution, deployed=model)
        scope = InstallationScope(item.item_type, item.item_name)
        for table in SEMANTIC_TABLES:
            context.sql.execute_script(render_delete_scope(table, scope=scope))
            statement = render_merge(table, rows[table.name], scope=scope)
            if statement:
                context.sql.execute_script(statement)
        return {"semantic_definition": model}
