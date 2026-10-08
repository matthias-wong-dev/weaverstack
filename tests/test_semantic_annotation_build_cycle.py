"""Native source annotations join the public Build and catalogue lifecycle."""

import pytest
from support.semantic_models import source_model
from support.weaver_test import weaver_test
from test_semantic_annotation_declaration import ITEM, source_project
from test_semantic_source_build_cycle import (
    SubmittedDefinition,
    answer_catalogue,
    capture_publication,
    read_bindings,
    source_catalogue,
    source_session,
    submitted_parts,
)

import weaver


@weaver_test()
def test_source_annotation_generates_columns_descriptions_and_managed_lineage(
    tmp_path, monkeypatch
):
    root = source_project(tmp_path)
    observed = source_model(relations={"Sales": "Sales"})
    observed["model"]["tables"][0]["annotations"] = [
        {"name": "Weaver.Source", "value": "Warehouse/Serving/Cake.Sales"}
    ]
    with source_session() as session:
        session.answer_semantic_model(
            "Demo", "Reporting_Dev", SubmittedDefinition(observed)
        )
        answer_catalogue(session, source_catalogue(), read_bindings())
        published = capture_publication(monkeypatch, session)
        result = weaver.build(
            root, items=f"{ITEM}=SemanticModel/Reporting_Dev", session=session
        )
        assert result.succeeded, result.errors
        parts = submitted_parts(session)
        table = parts["definition/tables/Sales.tmdl"]
        assert b"column 'Id'" in table and b"dataType: int64" in table
        assert b"column 'Label'" in table and b"dataType: string" in table
        assert b"Sales description" in table and b"Sales key" in table
        assert b"mode: directLake" in table
        assert b"expressionSource: 'Warehouse/Serving'" in table
        assert b"annotation 'Weaver.Source' = Warehouse/Serving/Cake.Sales" in table
        assert b"expression 'Warehouse/Serving'" in parts["definition/expressions.tmdl"]
        rows = published()[ITEM]
        assert rows["SemanticModelTable"][0]["description"] == "Sales description"
        import json

        provenance = json.loads(rows["SemanticModelTable"][0]["provenance"])
        generated = [
            origin for origin in provenance.values() if origin.get("reference")
        ]
        assert generated and all(o["reason"] == "Weaver.Source" for o in generated)
        assert all(o["source"].endswith("extension.tmdl") for o in generated)
        assert {
            (r["referencing_object_name"], r["dependency_reference"])
            for r in rows["Dependency"]
        } == {("Sales", "Warehouse/Serving/Cake.Sales")}
        assert not session.spark_sql and not session.python


@weaver_test()
def test_source_annotation_reuses_one_expression_for_two_consuming_tables(
    tmp_path, monkeypatch
):
    root = source_project(tmp_path)
    path = root / str(ITEM) / "extension.tmdl"
    path.write_text(
        path.read_text()
        + "\ntable SalesAgain\n\tannotation Weaver.Source = Warehouse/Serving/Cake.Sales\n"
    )
    observed = source_model(relations={"Sales": "Sales", "SalesAgain": "Sales"})
    for table in observed["model"]["tables"]:
        table["annotations"] = [
            {"name": "Weaver.Source", "value": "Warehouse/Serving/Cake.Sales"}
        ]
    with source_session() as session:
        session.answer_semantic_model(
            "Demo", "Reporting_Dev", SubmittedDefinition(observed)
        )
        answer_catalogue(session, source_catalogue(), read_bindings())
        published = capture_publication(monkeypatch, session)
        result = weaver.build(
            root, items=f"{ITEM}=SemanticModel/Reporting_Dev", session=session
        )
        assert result.succeeded, result.errors
        parts = submitted_parts(session)
        assert (
            parts["definition/expressions.tmdl"].count(
                b"expression 'Warehouse/Serving'"
            )
            == 1
        )
        assert {
            (r["referencing_object_name"], r["dependency_reference"])
            for r in published()[ITEM]["Dependency"]
        } == {
            ("Sales", "Warehouse/Serving/Cake.Sales"),
            ("SalesAgain", "Warehouse/Serving/Cake.Sales"),
        }
        assert (
            len(
                [
                    sql
                    for sql in session.tsql
                    if "INFORMATION_SCHEMA.COLUMNS" in sql and "Cake" in sql
                ]
            )
            == 1
        )


@pytest.mark.parametrize("scope", ["model", "table"])
@weaver_test()
def test_source_generated_columns_receive_auto_hide_policy(tmp_path, scope):
    root = source_project(tmp_path)
    path = root / str(ITEM) / "extension.tmdl"
    source = path.read_text()
    policy = 'annotation Weaver.AutoHideColumns = "I?"\n'
    path.write_text(
        ("model Model\n\t" + policy + "\n" + source)
        if scope == "model"
        else source + "\t" + policy
    )
    observed = source_model(relations={"Sales": "Sales"})
    sales = observed["model"]["tables"][0]
    sales["annotations"] = [
        {"name": "Weaver.Source", "value": "Warehouse/Serving/Cake.Sales"}
    ]
    annotations = (
        observed["model"].setdefault("annotations", [])
        if scope == "model"
        else sales["annotations"]
    )
    annotations.append({"name": "Weaver.AutoHideColumns", "value": '"I?"'})
    sales["columns"][0]["isHidden"] = True
    with source_session() as session:
        session.answer_semantic_model(
            "Demo", "Reporting_Dev", SubmittedDefinition(observed)
        )
        answer_catalogue(session, source_catalogue(), read_bindings())
        result = weaver.build(
            root, items=f"{ITEM}=SemanticModel/Reporting_Dev", session=session
        )
        assert result.succeeded, result.errors
        assert b"isHidden" in submitted_parts(session)["definition/tables/Sales.tmdl"]


@weaver_test()
def test_authored_columns_refine_the_columns_a_source_generates(tmp_path):
    root = source_project(tmp_path)
    path = root / str(ITEM) / "extension.tmdl"
    path.write_text(
        path.read_text() + "\n\tcolumn Label\n\t\tisHidden\n\t\tsortByColumn: Id\n"
    )
    observed = source_model(relations={"Sales": "Sales"})
    sales = observed["model"]["tables"][0]
    sales["annotations"] = [
        {"name": "Weaver.Source", "value": "Warehouse/Serving/Cake.Sales"}
    ]
    sales["columns"][1].update(isHidden=True, sortByColumn="Id")
    with source_session() as session:
        session.answer_semantic_model(
            "Demo", "Reporting_Dev", SubmittedDefinition(observed)
        )
        answer_catalogue(session, source_catalogue(), read_bindings())
        result = weaver.build(
            root, items=f"{ITEM}=SemanticModel/Reporting_Dev", session=session
        )
        assert result.succeeded, result.errors
    from weaver.semantic_models import TmdlDefinition

    columns = TmdlDefinition(submitted_parts(session)).model.tables["Sales"].columns
    assert [
        (c.name, c.sourceColumn, c.dataType, c.isHidden, c.sortByColumn)
        for c in columns
    ] == [
        ("Label", "Label", "string", True, "Id"),
        ("Id", "Id", "int64", None, None),
    ]


class _Inventory:
    def __init__(self, installed=()):
        self.installed = set(installed)

    def physical_type(self, identity):
        return "view" if identity in self.installed else None


@pytest.mark.parametrize("state", ["unchanged", "changed", "new"])
@weaver_test()
def test_refined_columns_read_a_selected_source_view_unless_it_is_rebuilt(
    tmp_path, state
):
    """A View built in the same Build has no shape until installation."""

    import copy

    from test_semantic_source_build_cycle import SOURCE

    from weaver.build_bundle.semantic_sources import read_semantic_sources
    from weaver.catalogue.state import Catalogue
    from weaver.declaration.model import WeaverDocumentId
    from weaver.declaration.repository import parse_item_repository
    from weaver.errors import BuildError
    from weaver.locations import Location

    root = source_project(tmp_path)
    path = root / str(ITEM) / "extension.tmdl"
    path.write_text(path.read_text() + "\n\tcolumn Label\n\t\tisHidden\n")
    folder = root / str(SOURCE)
    folder.mkdir(parents=True)
    (folder / "Cake.yml").write_text(
        "Schema ID: Cake\nDescription: Cake records.\n", encoding="utf-8"
    )
    (folder / "Cake.Sales.sql").write_text(
        "/*\nView ID: Cake.Sales\nDescription: Sales facts.\nLineage: Constant\n*/\nSELECT CAST(1 AS BIGINT) AS Id, 'sale' AS Label\n",
        encoding="utf-8",
    )
    repository = parse_item_repository(Location(root.as_posix()))
    view = WeaverDocumentId.parse("Warehouse/Serving/Cake.Sales")
    rows = copy.deepcopy(dict(source_catalogue().rows))
    rows[SOURCE]["Registry"] = tuple(
        {
            **row,
            "object_type": "view",
            "signature": repository.source_documents[view].physical_signature
            if state == "unchanged"
            else "before",
        }
        for row in rows[SOURCE]["Registry"]
        if row["object_name"] == "Sales" and state != "new"
    )
    bindings = read_bindings()
    inventories = {item: _Inventory() for item in bindings.by_item}
    inventories[SOURCE] = _Inventory(() if state == "new" else (view,))
    with source_session() as session:

        def read():
            return read_semantic_sources(
                repository,
                bindings,
                Catalogue(rows),
                session=session,
                workspace=session.workspace,
                inventories=inventories,
            )

        if state == "unchanged":
            observed = read()["Warehouse/Serving/Cake.Sales"]
            assert [c["column_name"] for c in observed["source_columns"]] == [
                "Id",
                "Label",
            ]
        else:
            with pytest.raises(BuildError, match="unavailable before installation"):
                read()


@pytest.mark.parametrize(
    "failure, diagnostic",
    [
        ("missing_object", "no managed Table or View"),
        ("missing_columns", "no source columns"),
        ("unsupported_type", "unsupported type"),
        ("missing_column", "Cake.Sales has no column Missing"),
    ],
)
@weaver_test()
def test_source_annotation_refuses_unresolved_generation_before_mutation(
    tmp_path, failure, diagnostic
):
    from weaver.errors import BuildError

    root = source_project(tmp_path)
    path = root / str(ITEM) / "extension.tmdl"
    if failure == "missing_object":
        path.write_text(path.read_text().replace("Cake.Sales", "Cake.Missing"))

    elif failure == "missing_column":
        path.write_text(path.read_text() + "\n\tcolumn Missing\n\t\tisHidden\n")
    with source_session() as session:
        answer_catalogue(session, source_catalogue(), read_bindings())
        if failure == "missing_columns":
            session.source_columns = []
        elif failure == "unsupported_type":
            session.source_columns = [{"column_name": "Id", "data_type": "binary"}]
        with pytest.raises(BuildError, match=diagnostic):
            weaver.build(
                root, items=f"{ITEM}=SemanticModel/Reporting_Dev", session=session
            )
        assert not any(
            kind == "update_definition"
            for kind, _ in session.semantic_model("Reporting_Dev").calls
        )
        assert not any("MERGE" in statement for statement in session.tsql)
        assert not session.spark_sql and not session.python


@weaver_test()
def test_source_annotation_uses_the_typed_lakehouse_sql_endpoint(tmp_path, monkeypatch):
    import copy
    import json

    from support.workspaces import _identifier
    from test_semantic_source_build_cycle import (
        ItemBindings,
        SourceInventory,
        SourceSession,
        effective_item_bindings,
        parse_build_item,
    )

    from weaver.catalogue.state import Catalogue
    from weaver.declaration.model import WeaverItemId
    from weaver.fabric.resolution import FabricResolver
    from weaver.store import FilesystemStore
    from weaver.workspaces import TargetDeclaration, Workspace

    logical = WeaverItemId.parse("Lakehouse/Curated")
    root = source_project(tmp_path, value="Lakehouse/Curated/Tables/Cake.Sales")
    original = source_catalogue().rows[WeaverItemId.parse("Warehouse/Serving")]
    rows = copy.deepcopy(original)
    rows = {
        key: [
            {
                **row,
                "item_type": "Lakehouse",
                "item_name": "Curated",
                **(
                    {"schema_name": "Tables/" + row["schema_name"]}
                    if "schema_name" in row
                    else {}
                ),
            }
            for row in values
        ]
        for key, values in rows.items()
    }
    catalogue = Catalogue({logical: rows})
    workspace = Workspace(
        workspace="Demo",
        catalogue="Warehouse/Catalogue",
        targets={logical: TargetDeclaration(physical="Serving_Dev")},
    )

    class LakehouseInventory(SourceInventory):
        def get_json(self, path, **kwargs):
            if "/lakehouses/" in path:
                self.requested.append(path)
                return {
                    "properties": {
                        "sqlEndpointProperties": {
                            "id": _identifier("SQLEndpoint", "Serving_Dev"),
                            "connectionString": "lake.datawarehouse.fabric.microsoft.com",
                        }
                    }
                }
            return super().get_json(path, **kwargs)

    inventory = LakehouseInventory(
        "Demo",
        [
            ("Warehouse", "Catalogue"),
            ("Warehouse", "Serving_Dev"),
            ("Lakehouse", "Serving_Dev"),
            ("SemanticModel", "Reporting_Dev"),
        ],
    )
    observed = source_model(lakehouse=True, relations={"Sales": "Sales"})
    observed["model"]["tables"][0]["annotations"] = [
        {"name": "Weaver.Source", "value": "Lakehouse/Curated/Tables/Cake.Sales"}
    ]
    with SourceSession(
        workspace=workspace,
        resolver=FabricResolver(workspace, client=inventory),
        store=FilesystemStore(),
    ) as session:
        session.answer_semantic_model(
            "Demo", "Reporting_Dev", SubmittedDefinition(observed)
        )
        bindings = effective_item_bindings(
            ItemBindings(
                (
                    parse_build_item(f"{ITEM}=SemanticModel/Reporting_Dev"),
                    parse_build_item("Lakehouse/Curated=Lakehouse/Serving_Dev"),
                )
            ),
            control_item="Catalogue",
            workspace_name="Demo",
        )
        answer_catalogue(session, catalogue, bindings)
        published = capture_publication(monkeypatch, session)
        result = weaver.build(
            root, items=f"{ITEM}=SemanticModel/Reporting_Dev", session=session
        )
        assert result.succeeded, result.errors
        parts = submitted_parts(session)
        # The Lakehouse's SQL endpoint, named as people name it in a connection.
        assert (
            b'Sql.Database("lake.datawarehouse.fabric.microsoft.com", "Serving_Dev")'
            in parts["definition/expressions.tmdl"]
        )
        assert any("TABLE_SCHEMA = N'Cake'" in sql for sql in session.tsql)
        table = published()[ITEM]["SemanticModelTable"][0]
        assert table["description"] == "Sales description"
        binding = json.loads(table["source_binding"])
        assert binding["item_id"] == _identifier("Lakehouse", "Serving_Dev")
        assert (
            published()[ITEM]["Dependency"][0]["dependency_reference"]
            == "Lakehouse/Curated/Tables/Cake.Sales"
        )
        assert not session.spark_sql and not session.python


@weaver_test()
def test_import_model_generates_navigation_partitions_with_lineage(
    tmp_path, monkeypatch
):
    root = source_project(tmp_path)
    extension = root / str(ITEM) / "extension.tmdl"
    extension.write_text(
        "model Model\n\tdefaultMode: import\n\n" + extension.read_text()
    )
    navigation = (
        'let\n    Source = #"Warehouse/Serving",\n'
        '    Data = Source{[Schema="Cake",Item="Sales"]}[Data]\nin\n    Data'
    )
    observed = source_model(relations={"Sales": "Sales"})
    observed["model"]["defaultMode"] = "import"
    observed["model"]["tables"][0]["partitions"] = [
        {
            "name": "Sales",
            "mode": "import",
            "source": {"type": "m", "expression": navigation},
        }
    ]
    observed["model"]["tables"][0]["annotations"] = [
        {"name": "Weaver.Source", "value": "Warehouse/Serving/Cake.Sales"}
    ]
    with source_session() as session:
        session.answer_semantic_model(
            "Demo", "Reporting_Dev", SubmittedDefinition(observed)
        )
        answer_catalogue(session, source_catalogue(), read_bindings())
        published = capture_publication(monkeypatch, session)
        result = weaver.build(
            root, items=f"{ITEM}=SemanticModel/Reporting_Dev", session=session
        )
        assert result.succeeded, result.errors
        table = submitted_parts(session)["definition/tables/Sales.tmdl"].decode()
        assert "partition 'Sales' = m" in table and "mode: import" in table
        assert 'Source{[Schema="Cake",Item="Sales"]}[Data]' in table
        assert "column 'Id'" in table and "entity" not in table
        rows = published()[ITEM]
        assert {
            (r["referencing_object_name"], r["dependency_reference"])
            for r in rows["Dependency"]
        } == {("Sales", "Warehouse/Serving/Cake.Sales")}
