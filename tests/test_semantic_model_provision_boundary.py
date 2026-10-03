"""The explicit estate provisioner reuses or creates a typed semantic model."""

from types import SimpleNamespace

import pytest
from fabric import provision_estate
from support.weaver_test import weaver_test

from weaver.fabric.resources import SEMANTIC_MODEL, WorkspaceItem
from weaver.semantic_models.definition import decode_parts


@pytest.mark.parametrize("exists", [False, True])
@weaver_test()
def test_fixed_semantic_model_creation_is_confined_to_the_provisioner(exists):
    calls = []
    inventory = [{"id": "model-id", "displayName": "Reporting", "type": SEMANTIC_MODEL}]

    class Client:
        def paged(self, path):
            return inventory if exists or calls else []

        def request(self, method, path, **kwargs):
            calls.append((method, path, kwargs))
            return SimpleNamespace(status_code=202)

        def wait_for_operation(self, response, **kwargs):
            assert response.status_code == 202
            return {"status": "Succeeded"}

    item, created = provision_estate.find_or_create(
        workspace=WorkspaceItem("workspace-id", "Analytics"),
        client=Client(),
        name="Reporting",
        item_type=SEMANTIC_MODEL,
        create=provision_estate.create_semantic_model,
    )
    assert item.id == "model-id" and item.type == SEMANTIC_MODEL
    assert created is not exists
    if exists:
        assert not calls
    else:
        assert len(calls) == 1
        method, path, options = calls[0]
        assert (method, path) == ("POST", "workspaces/workspace-id/semanticModels")
        assert options["retry_transient"] is False
        assert options["payload"]["displayName"] == "Reporting"
        parts = decode_parts(options["payload"]["definition"])
        assert (
            b"defaultPowerBIDataSourceVersion: powerBI_V3"
            in parts["definition/model.tmdl"]
        )
        assert {path for path in parts if path.startswith("definition/tables/")} == {
            "definition/tables/Product.tmdl",
            "definition/tables/Sales.tmdl",
        }
