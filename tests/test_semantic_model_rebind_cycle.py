"""A semantic certification belongs to one physical workspace/item binding."""

import pytest
from support.weaver_test import weaver_test
from support.workspaces import InventoryClient
from test_semantic_model_build_cycle import (
    ITEM,
    ROOT,
    DefinitionClient,
    answer_catalogue,
    engine_model,
    installed_state,
    prepared,
)
from test_semantic_model_build_load_cycle import answer_built_inventory

import weaver
from weaver.build_bundle.targets import (
    ItemBindings,
    effective_item_bindings,
    parse_build_item,
)
from weaver.fabric.resolution import FabricResolver
from weaver.semantic_models.definition import encode_definition
from weaver.sessions import TestSession
from weaver.store import FilesystemStore


@weaver_test()
@pytest.mark.parametrize("changed", ["name", "item_id", "workspace_id", "missing_ids"])
def test_rebinding_unchanged_source_deploys_before_recertifying(tmp_path, changed):
    root, repository, bindings, old_session, _ = prepared(tmp_path)
    with old_session:
        deployed = engine_model(repository)
        old_session.semantic_model("Reporting_Dev").definition = encode_definition(
            deployed
        )
        assert weaver.build(
            root, items=str(ITEM) + "=SemanticModel/Reporting_Dev", session=old_session
        ).succeeded
        bindings = effective_item_bindings(
            bindings, control_item="Catalogue", workspace_name="Demo"
        )
        old_inventories = answer_built_inventory(old_session, bindings)
        installed = installed_state(repository, bindings, deployed, old_inventories)
    prior = installed.catalogue.rows[ITEM]["Installation"][0]
    if changed == "missing_ids":
        prior["workspace_id"] = prior["item_id"] = None
    target = "Reporting_New" if changed == "name" else "Reporting_Dev"
    inventory = InventoryClient(
        "Demo", [("SemanticModel", target), ("Warehouse", "Catalogue")]
    )
    listing = inventory.paged

    def paged(path, **kwargs):
        rows = listing(path, **kwargs)
        for row in rows:
            if changed == "item_id" and row.get("type") == "SemanticModel":
                row["id"] = "11111111-2222-3333-4444-555555555555"
            if changed == "workspace_id" and path == "workspaces":
                row["id"] = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        return rows

    inventory.paged = paged
    workspace = old_session.workspace
    model = DefinitionClient()
    model.definition = encode_definition(deployed)
    selector = f"{ITEM}=SemanticModel/{target}"
    bindings = effective_item_bindings(
        ItemBindings((parse_build_item(selector),)),
        control_item="Catalogue",
        workspace_name="Demo",
    )
    with TestSession(
        workspace=workspace,
        store=FilesystemStore(),
        resolver=FabricResolver(workspace, client=inventory),
    ) as session:
        session.answer_semantic_model("Demo", target, model)
        answer_built_inventory(session, bindings)
        answer_catalogue(session, installed.catalogue, bindings)
        session.calls.clear()
        result = weaver.build(root, items=selector, session=session)
        assert result.succeeded, result.errors
        assert result.selection.selected_for_build == (ROOT,)
        assert [method for method, _ in model.calls] == [
            "update_definition",
            "invalid_measures",
            "get_definition",
        ]
        assert not result.selection.selected_for_drop
        sql = session.tsql
        revoke = next(i for i, s in enumerate(sql) if "DELETE FROM [_].[Registry]" in s)
        publish = next(
            i for i, s in enumerate(sql) if "MERGE" in s and "[_].[SemanticModel]" in s
        )
        certify = next(
            i for i, s in enumerate(sql) if "MERGE" in s and "[_].[Registry]" in s
        )
        assert revoke < publish < certify
        assert any("[_].[LoadStatus]" in s and "Pending" in s for s in sql)
        assert not session.python and not session.spark_sql
