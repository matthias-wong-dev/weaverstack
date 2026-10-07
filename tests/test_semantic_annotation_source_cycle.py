"""Source annotations use the shared environment mapping before generation."""

import copy

import pytest
from support.semantic_models import source_model
from support.weaver_test import weaver_test
from test_semantic_annotation_declaration import ITEM, source_project
from test_semantic_source_build_cycle import (
    ItemBindings,
    SourceInventory,
    SourceSession,
    SubmittedDefinition,
    answer_catalogue,
    effective_item_bindings,
    parse_build_item,
    source_catalogue,
    submitted_parts,
)

import weaver
from weaver.catalogue.state import Catalogue
from weaver.declaration.model import WeaverItemId
from weaver.fabric.resolution import FabricResolver
from weaver.store import FilesystemStore
from weaver.workspaces import TargetDeclaration, Workspace


@pytest.mark.parametrize("cli_override", [False, True])
@weaver_test()
def test_source_annotation_resolves_environment_mapping_before_generation(
    tmp_path, cli_override
):
    root = source_project(tmp_path)
    workspace = Workspace(
        workspace="Demo",
        catalogue="Warehouse/Catalogue",
        targets={
            WeaverItemId.parse("Warehouse/Serving"): TargetDeclaration(
                physical="Serving_Dev"
            )
        },
        data_sources={
            "Warehouse/Serving": "Warehouse/NotUsed"
            if cli_override
            else "Warehouse/Serving_Prod"
        },
    )
    inventory = SourceInventory(
        "Demo",
        [
            ("Warehouse", "Catalogue"),
            ("Warehouse", "Serving_Dev"),
            ("Warehouse", "Serving_Prod"),
            ("SemanticModel", "Reporting_Dev"),
        ],
    )
    observed = source_model("Serving_Prod", relations={"Sales": "Sales"})
    observed["model"]["tables"][0]["annotations"] = [
        {"name": "Weaver.Source", "value": "Warehouse/Serving/Cake.Sales"}
    ]
    bindings = effective_item_bindings(
        ItemBindings(
            (
                parse_build_item(f"{ITEM}=SemanticModel/Reporting_Dev"),
                parse_build_item("Warehouse/Serving=Warehouse/Serving_Prod"),
            )
        ),
        control_item="Catalogue",
        workspace_name="Demo",
    )
    rows = copy.deepcopy(dict(source_catalogue().rows))
    rows[WeaverItemId.parse("Warehouse/Serving")]["Installation"][0]["target_name"] = (
        "Serving_Prod"
    )
    catalogue = Catalogue(rows)
    with SourceSession(
        workspace=workspace,
        resolver=FabricResolver(workspace, client=inventory),
        store=FilesystemStore(),
    ) as session:
        session.answer_semantic_model(
            "Demo", "Reporting_Dev", SubmittedDefinition(observed)
        )
        answer_catalogue(session, catalogue, bindings)
        result = weaver.build(
            root,
            items=f"{ITEM}=SemanticModel/Reporting_Dev",
            session=session,
            data_sources=["Warehouse/Serving=Warehouse/Serving_Prod"]
            if cli_override
            else None,
        )
        assert result.succeeded, result.errors
        parts = submitted_parts(session)
        assert b'"Serving_Prod")' in parts["definition/expressions.tmdl"]
        assert b"Serving_Dev" not in parts["definition/expressions.tmdl"]
        assert not any("NotUsed" in path for path in inventory.requested)
