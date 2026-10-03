"""Reset one bound semantic model and verify its remaining content."""

import json

from ...errors import InstallError
from ...semantic_models.definition import decode_model, decode_parts
from ...semantic_models.wipe import verify_prepared, verify_reset


class SemanticWipeExecutor:
    name = "semantic_wipe"

    def execute(self, action, payload, context):
        spec = json.loads(payload)
        target = context.target.bound
        if (
            target.kind != "semanticmodel"
            or spec["target_id"] != target.id
            or spec["model_id"] != target.item_id
            or spec["workspace_id"] != target.workspace_id
            or type(spec["preserve_data_source"]) is not bool
        ):
            raise InstallError(
                "Semantic wipe requires its bound model and preservation choice"
            )
        decode_parts(spec["definition"])
        client = context.semantic_model(target)
        verify_prepared(
            spec, decode_model(client.get_definition()), client.get_connections()
        )
        client.update_definition(spec["definition"], allow_purge_data=True)
        observed = decode_model(client.get_definition())
        verify_reset(spec, observed, client.get_connections())
        return {
            "removed": spec["removed"],
            "retained_sources": spec["retained_sources"],
        }
