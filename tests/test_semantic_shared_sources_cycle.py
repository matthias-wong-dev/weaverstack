"""Shared M source portability uses Build, not connection binding."""

import json
from pathlib import Path

import pytest
from support.weaver_test import weaver_test
from support.workspaces import _identifier
from test_semantic_source_build_cycle import source_session
from test_semantic_tmdl_package_representation import pbip_project

import weaver
from weaver.semantic_models.definition import decode_parts


def shared_project(tmp_path, expression="DataSource1"):
    folder, model = pbip_project(tmp_path)
    (model / "definition/expressions.tmdl").write_text(
        f'expression \'{expression}\' = Sql.Database("old-server", "old-database")\n'
    )
    sales = model / "definition/tables/Sales.tmdl"
    original = sales.read_text()
    position = original.index("\tpartition Sales")
    sales.write_text(
        original[:position]
        + f'''\tpartition Sales = m
\t\tmode: import
\t\tsource =
\t\t\tlet
\t\t\t    Source = #"{expression}",
\t\t\t    Sales = Source{{[Schema="Cake", Item="Sales"]}}[Data]
\t\t\tin
\t\t\t    Sales
'''
    )
    return folder, model


def payload_parts(result):
    (payload,) = Path(result.bundle_path).rglob("*.semantic_model.json")
    return decode_parts(json.loads(payload.read_text())["definition"])


@weaver_test()
def test_public_build_maps_generic_shared_expression_without_changing_queries(tmp_path):
    _, model = shared_project(tmp_path)
    untouched = {
        p.relative_to(model).as_posix(): p.read_bytes()
        for p in model.rglob("*")
        if p.is_file() and p.name != "expressions.tmdl"
    }
    session = source_session()
    with session:
        result = weaver.build(
            tmp_path,
            items=["SemanticModel/Reporting=SemanticModel/Reporting_Dev"],
            data_sources=["DataSource1=Warehouse/Serving_Dev"],
            bundle_only=True,
            session=session,
        )
    assert result.succeeded
    parts = payload_parts(result)
    assert {p: parts[p] for p in untouched} == untouched
    expression = parts["definition/expressions.tmdl"].decode()
    assert 'Sql.Database("serving.datawarehouse.fabric.microsoft.com", ' in expression
    assert '"serving.datawarehouse.fabric.microsoft.com", "Serving_Dev")' in expression
    assert "old-server" not in expression and "old-database" not in expression
    assert "DataSource1" in expression
    assert not session.spark_sql
    assert not result.installation
    assert not session.semantic_model("Reporting_Dev").calls


@weaver_test()
def test_cli_source_mapping_overrides_workspace_config(tmp_path):
    shared_project(tmp_path)
    config = tmp_path / "workspace.yml"
    config.write_text(
        "workspace: Demo\ncatalogue: Warehouse/Catalogue\ndata_sources:\n  DataSource1: Warehouse/Configured\n"
    )
    session = source_session()
    with session:
        result = weaver.build(
            tmp_path,
            items=["SemanticModel/Reporting=SemanticModel/Reporting_Dev"],
            workspace_config=config,
            data_sources={"DataSource1": "Warehouse/Serving_Dev"},
            bundle_only=True,
            session=session,
        )
    expression = payload_parts(result)["definition/expressions.tmdl"].decode()
    assert '"serving.datawarehouse.fabric.microsoft.com", "Serving_Dev")' in expression
    assert "Configured" not in expression


@weaver_test()
def test_logical_expression_resolves_workspace_target_without_addon_or_override(
    tmp_path,
):
    from weaver.workspaces import TargetDeclaration, Workspace

    _, model = shared_project(tmp_path, "Warehouse/Serving")
    original = (model / "definition/tables/Sales.tmdl").read_bytes()
    workspace = Workspace(
        workspace="Demo",
        catalogue="Warehouse/Catalogue",
        targets={"Warehouse/Serving": TargetDeclaration("Serving_Dev")},
    )
    session = source_session(workspace=workspace)
    with session:
        result = weaver.build(
            tmp_path,
            items=["SemanticModel/Reporting=SemanticModel/Reporting_Dev"],
            bundle_only=True,
            session=session,
        )
    parts = payload_parts(result)
    assert (
        '"serving.datawarehouse.fabric.microsoft.com", "Serving_Dev")'
        in parts["definition/expressions.tmdl"].decode()
    )
    assert parts["definition/tables/Sales.tmdl"] == original


@weaver_test()
def test_native_lakehouse_expression_uses_item_ids_without_sql_endpoint(tmp_path):
    from weaver.workspaces import TargetDeclaration, Workspace

    _, model = shared_project(tmp_path, "Lakehouse/Curated")
    (
        model / "definition/expressions.tmdl"
    ).write_text("""expression 'Lakehouse/Curated' =
        let
            Root = Lakehouse.Contents([]),
            Workspace = Root{[workspaceId="old-workspace"]}[Data],
            Lakehouse = Workspace{[lakehouseId="old-lakehouse"]}[Data]
        in
            Lakehouse
""")
    workspace = Workspace(
        workspace="Demo",
        catalogue="Warehouse/Catalogue",
        targets={"Lakehouse/Curated": TargetDeclaration("Serving_Dev")},
    )
    session = source_session(workspace=workspace)
    with session:
        result = weaver.build(
            tmp_path,
            items=["SemanticModel/Reporting=SemanticModel/Reporting_Dev"],
            bundle_only=True,
            session=session,
        )
    body = payload_parts(result)["definition/expressions.tmdl"].decode()
    assert "Lakehouse.Contents([])" in body
    assert _identifier("Lakehouse", "Serving_Dev") in body
    assert _identifier("workspace", "Demo") in body
    assert "old-workspace" not in body and "old-lakehouse" not in body
    assert not session.spark_sql


@weaver_test()
def test_cli_forwards_repeatable_data_source_mapping(tmp_path, monkeypatch, capsys):
    import importlib
    from contextlib import nullcontext

    cli = importlib.import_module("weaver_cli.main")

    shared_project(tmp_path)
    session = source_session()
    monkeypatch.setattr(cli, "_running_session", lambda *_: nullcontext(session))
    args = cli.build_parser().parse_args(
        [
            "build",
            str(tmp_path),
            "--workspace",
            "Demo",
            "--catalogue",
            "Warehouse/Catalogue",
            "--item",
            "SemanticModel/Reporting=SemanticModel/Reporting_Dev",
            "--data-source",
            "DataSource1=Warehouse/Serving_Dev",
            "--bundle-only",
            "--json",
        ]
    )
    assert cli.handle_build(args) == 0
    result = json.loads(capsys.readouterr().out)
    (payload,) = Path(result["bundle_path"]).rglob("*.semantic_model.json")
    body = decode_parts(json.loads(payload.read_text())["definition"])[
        "definition/expressions.tmdl"
    ].decode()
    assert '"serving.datawarehouse.fabric.microsoft.com", "Serving_Dev")' in body


@weaver_test()
def test_shared_source_publishes_managed_table_lineage(tmp_path, monkeypatch):
    from support.semantic_models import probe_model
    from test_semantic_model_build_cycle import ITEM, answer_catalogue
    from test_semantic_source_build_cycle import (
        capture_publication,
        read_bindings,
        source_catalogue,
    )

    from weaver.semantic_models.definition import encode_definition
    from weaver.workspaces import TargetDeclaration, Workspace

    shared_project(tmp_path, "Warehouse/Serving")
    workspace = Workspace(
        workspace="Demo",
        catalogue="Warehouse/Catalogue",
        targets={"Warehouse/Serving": TargetDeclaration("Serving_Dev")},
    )
    session = source_session(workspace=workspace)
    observed = probe_model()
    observed["model"]["expressions"] = [
        {
            "name": "Warehouse/Serving",
            "kind": "m",
            "expression": 'Sql.Database("serving.datawarehouse.fabric.microsoft.com", "Serving_Dev")',
        }
    ]
    sales = next(t for t in observed["model"]["tables"] if t["name"] == "Sales")
    sales["partitions"][0]["source"]["expression"] = (
        'let Source = #"Warehouse/Serving", Sales = Source{[Schema="Cake", Item="Sales"]}[Data] in Sales'
    )
    session.semantic_model("Reporting_Dev").definition = encode_definition(observed)
    answer_catalogue(session, source_catalogue(), read_bindings())
    published = capture_publication(monkeypatch, session)
    with session:
        result = weaver.build(
            tmp_path,
            items=["SemanticModel/Reporting=SemanticModel/Reporting_Dev"],
            session=session,
        )
    assert result.succeeded, result.errors
    rows = published()[ITEM]
    edges = rows.get("Dependency", ())
    assert [
        (
            r["referencing_object_name"],
            r["referenced_item_name"],
            r["referenced_schema_name"],
            r["referenced_object_name"],
        )
        for r in edges
    ] == [("Sales", "Serving", "Cake", "Sales")]
    model_tables = rows["SemanticModelTable"]
    sales_row = next(r for r in model_tables if r["table_name"] == "Sales")
    assert sales_row["source_access"] == "sql"


@pytest.mark.parametrize(
    "mapping,expected",
    [
        ("Warehouse/Curated", "Warehouse/DEV_Curated"),
        ("Warehouse/Reporting_Dev", "Warehouse/Reporting_Dev"),
    ],
)
@weaver_test()
def test_configured_data_source_names_a_logical_item(tmp_path, mapping, expected):
    from weaver.build_bundle.targets import ItemBindings, parse_build_item
    from weaver.declaration.model import WeaverItemId
    from weaver.declaration.repository import parse_item_repository
    from weaver.locations import Location
    from weaver.semantic_models.expressions import configure_sources
    from weaver.workspaces import TargetDeclaration, Workspace

    item = WeaverItemId.parse("SemanticModel/Reporting")
    folder = tmp_path / str(item)
    folder.mkdir(parents=True)
    (folder / f"{folder.name}.tmdl").write_text(
        'expression DataSource = Sql.Database("server", "Curated")\n\tkind: m\n'
    )
    workspace = Workspace(
        workspace="Demo",
        targets={
            WeaverItemId.parse("Warehouse/Curated"): TargetDeclaration("DEV_Curated")
        },
        data_sources={"DataSource": mapping},
    )
    repository = configure_sources(
        parse_item_repository(Location(tmp_path.as_posix())),
        None,
        ItemBindings((parse_build_item(f"{item}=SemanticModel/Reporting_Dev"),)),
        workspace,
    )
    assert repository.semantic_models[item].expression_sources == {
        "DataSource": {"target": expected}
    }
