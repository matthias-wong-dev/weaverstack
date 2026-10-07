"""A workspace with no Weaver catalogue still builds its semantic models."""

import pytest
from support.weaver_test import weaver_test
from support.workspaces import InventoryClient
from test_semantic_model_build_cycle import (
    ITEM,
    DefinitionClient,
    engine_model,
    project,
)

import weaver
from weaver.declaration.repository import parse_item_repository
from weaver.errors import CommandError
from weaver.fabric.resolution import FabricResolver
from weaver.locations import Location
from weaver.semantic_models.definition import decode_parts, encode_definition
from weaver.sessions import TestSession
from weaver.store import FilesystemStore
from weaver.workspaces import Workspace

SELECTOR = f"{ITEM}=SemanticModel/Reporting_Dev"


def session_without_catalogue(inventory=(("SemanticModel", "Reporting_Dev"),)):
    workspace = Workspace(workspace="Demo")
    session = TestSession(
        workspace=workspace,
        store=FilesystemStore(),
        resolver=FabricResolver(
            workspace, client=InventoryClient("Demo", list(inventory))
        ),
    )
    session.answer_semantic_model("Demo", "Reporting_Dev", DefinitionClient())
    return session


@pytest.mark.parametrize("pbip", [False, True])
@weaver_test()
def test_semantic_build_deploys_and_reads_back_without_a_catalogue(tmp_path, pbip):
    root = project(tmp_path, pbip)
    repository = parse_item_repository(Location(root.as_posix()))
    session = session_without_catalogue()
    model = session.semantic_model("Reporting_Dev")
    model.definition = encode_definition(engine_model(repository))
    first = weaver.build(root, items=SELECTOR, session=session)
    assert first.succeeded, first.errors
    assert [call[0] for call in model.calls] == ["update_definition", "get_definition"]
    assert (
        decode_parts(model.calls[0][1]["definition"])
        == repository.semantic_models[ITEM].parts
    )
    assert first.installation_report.action_counts()["total"] == 2
    assert not session.tsql and not session.spark_sql
    model.calls.clear()
    second = weaver.build(root, items=SELECTOR, session=session)
    assert second.succeeded, second.errors
    assert [call[0] for call in model.calls] == ["update_definition", "get_definition"]


@weaver_test()
def test_failed_readback_fails_a_catalogue_free_build(tmp_path):
    root = project(tmp_path, False)
    session = session_without_catalogue()
    session.semantic_model("Reporting_Dev").definition = encode_definition(
        {"model": {"culture": "en-US", "tables": []}}
    )
    result = weaver.build(root, items=SELECTOR, session=session)
    assert not result.succeeded
    assert any(
        failure.action_id.startswith("semantic_readback") for failure in result.errors
    )


@weaver_test()
def test_other_items_still_need_a_catalogue(tmp_path):
    root = project(tmp_path, False)
    lakehouse = root / "Lakehouse/Sales"
    lakehouse.mkdir(parents=True)
    session = session_without_catalogue(
        (("SemanticModel", "Reporting_Dev"), ("Lakehouse", "Sales"))
    )
    with pytest.raises(CommandError, match="Lakehouse/Sales needs a Weaver catalogue"):
        weaver.build(
            root, items=[SELECTOR, "Lakehouse/Sales=Lakehouse/Sales"], session=session
        )
    assert not session.semantic_model("Reporting_Dev").calls


@weaver_test()
def test_validations_need_a_catalogue(tmp_path):
    root = project(tmp_path, False)
    tests = root / str(ITEM) / "assumptions"
    tests.mkdir()
    (tests / "Sales.Nothing.dax").write_text(
        "/*\nAssumption ID: Sales.Nothing\nDescription: Nothing is wrong.\n*/\n"
        'EVALUATE FILTER(ROW("N", 1), FALSE())\n',
        encoding="utf-8",
    )
    session = session_without_catalogue()
    from weaver.errors import BuildError

    with pytest.raises(BuildError, match="declares tests or assumptions"):
        weaver.build(root, items=SELECTOR, session=session)
    assert not session.semantic_model("Reporting_Dev").calls


@weaver_test()
def test_load_refreshes_named_models_without_recording(monkeypatch):
    from test_semantic_model_load_cycle import (
        COMPLETED,
        MODEL_ID,
        REQUEST_ID,
        WORKSPACE_ID,
    )
    from test_semantic_model_rest_boundary import Client, response

    from weaver.fabric import semantic_model
    from weaver.fabric.resources import Item
    from weaver.fabric.semantic_model import SemanticModelClient

    monkeypatch.setattr(
        semantic_model,
        "time",
        type(
            "Clock",
            (),
            {
                "monotonic": staticmethod(lambda: 0),
                "sleep": staticmethod(lambda seconds: None),
            },
        ),
    )
    session = session_without_catalogue()
    monkeypatch.setattr(
        session,
        "resolve_item",
        lambda name, item_type: Item(
            id=MODEL_ID, workspace_id=WORKSPACE_ID, name=name, type=item_type
        ),
    )
    power_bi = Client(
        response({}, 202, {"x-ms-request-id": REQUEST_ID}), response(COMPLETED, 200)
    )
    session.answer_semantic_model(
        WORKSPACE_ID,
        MODEL_ID,
        SemanticModelClient(WORKSPACE_ID, MODEL_ID, fabric=Client(), power_bi=power_bi),
    )
    with session:
        report = weaver.load(str(ITEM), workspace_config=None, session=session)
    assert report.succeeded, report.to_mapping()
    (node,) = report.nodes
    assert node.primitive_kind == "semantic_refresh"
    assert node.result.request_id == REQUEST_ID
    assert not session.tsql and not session.spark_sql


@pytest.mark.parametrize(
    "items,options,message",
    [
        ((), {}, "every installed item needs a Weaver catalogue"),
        (("Lakehouse/Sales",), {}, "Lakehouse/Sales needs a Weaver catalogue"),
        ((str(ITEM),), {"reload": True}, "need a Weaver catalogue"),
    ],
)
@weaver_test()
def test_load_without_a_catalogue_refreshes_only_named_models(items, options, message):
    session = session_without_catalogue()
    with pytest.raises(CommandError, match=message):
        weaver.load(list(items), session=session, **options)
