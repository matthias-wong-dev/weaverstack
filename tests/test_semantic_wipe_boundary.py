"""The Session-owned semantic client reads native sources and item connections."""

from support.weaver_test import weaver_test
from test_semantic_model_rest_boundary import Client, response

from weaver.fabric.semantic_model import SemanticModelClient


@weaver_test()
def test_native_definition_and_connection_reads_use_the_fixed_item():
    native = {"parts": []}
    fabric = Client(response({"definition": native}))
    paths = []

    def paged(path):
        paths.append(path)
        return [{"connectivityType": "Automatic"}]

    fabric.paged = paged
    model = SemanticModelClient(
        "workspace-id", "model-id", fabric=fabric, power_bi=Client()
    )
    assert model.get_definition(format="TMDL") == native
    assert (
        fabric.calls[0][1]
        == "workspaces/workspace-id/semanticModels/model-id/getDefinition?format=TMDL"
    )
    assert model.get_connections() == [{"connectivityType": "Automatic"}]
    assert paths == ["workspaces/workspace-id/items/model-id/connections"]
