"""Build resolves managed semantic sources and publishes table-level lineage."""

import copy
import json
import shutil

import pytest
from support.semantic_models import probe_model, shared_source_tmdl, source_model
from support.weaver_test import weaver_test
from support.workspaces import InventoryClient
from test_semantic_model_build_cycle import ITEM, DefinitionClient, answer_catalogue
from test_semantic_model_load_cycle import COMPLETED, REQUEST_ID, answer_installed

import weaver
from weaver.build_bundle.targets import (
    ItemBindings,
    effective_item_bindings,
    parse_build_item,
)
from weaver.catalogue import render
from weaver.catalogue.state import Catalogue
from weaver.declaration.model import WeaverItemId
from weaver.declaration.repository import parse_item_repository
from weaver.fabric.resolution import FabricResolver
from weaver.locations import Location
from weaver.semantic_models.definition import (
    decode_model,
    decode_parts,
    encode_definition,
)
from weaver.sessions import TestSession
from weaver.store import FilesystemStore
from weaver.workspaces import TargetDeclaration, Workspace

SOURCE = WeaverItemId.parse("Warehouse/Serving")


class SourceInventory(InventoryClient):
    def get_json(self, path, **kwargs):
        self.requested.append(path)
        if path.endswith("/connectionString"):
            return {"connectionString": "serving.datawarehouse.fabric.microsoft.com"}
        raise AssertionError(path)


class SubmittedDefinition(DefinitionClient):
    def __init__(self, model=None):
        super().__init__()
        self.definition = encode_definition(source_model() if model is None else model)


def submitted_parts(session):
    (submitted,) = [
        value
        for method, value in session.semantic_model("Reporting_Dev").calls
        if method == "update_definition"
    ]
    return decode_parts(submitted["definition"])


class SourceSession(TestSession):
    source_columns = [
        {"column_name": "Id", "data_type": "bigint"},
        {"column_name": "Label", "data_type": "varchar"},
    ]

    def query_tsql(self, statement, **kwargs):
        if "INFORMATION_SCHEMA.COLUMNS" in statement and "Cake" in statement:
            self.answer_tsql(statement, self.source_columns)
        return super().query_tsql(statement, **kwargs)


def source_catalogue():
    scope = {"item_type": "Warehouse", "item_name": "Serving"}
    common = {**scope, "schema_name": "Cake", "signature": "source"}
    return Catalogue(
        {
            SOURCE: {
                "Installation": ({**scope, "target_name": "Serving_Dev"},),
                "Registry": tuple(
                    {
                        **common,
                        "object_name": name,
                        "object_type": kind,
                        "object_role": "data",
                    }
                    for name, kind in (("Sales", "table"), ("Summary", "view"))
                ),
                "TableDictionary": tuple(
                    {
                        **common,
                        "object_name": name,
                        "object_type": kind,
                        "description": f"{name} description",
                    }
                    for name, kind in (("Sales", "table"), ("Summary", "view"))
                ),
                "ColumnDictionary": (
                    {
                        **common,
                        "object_name": "Sales",
                        "column_name": "Id",
                        "description": "Sales key",
                    },
                ),
            }
        }
    )


def source_project(tmp_path):
    root = tmp_path / "project"
    folder = root / str(ITEM)
    folder.mkdir(parents=True)
    (folder / f"{folder.name}.tmdl").write_text(shared_source_tmdl(), encoding="utf-8")
    return root


def source_session(storage=None, *, workspace=None, source_name="Serving_Dev"):
    workspace = workspace or Workspace(
        workspace="Demo",
        catalogue="Warehouse/Catalogue",
        targets={SOURCE: TargetDeclaration(physical=source_name)},
    )
    inventory = SourceInventory(
        workspace.workspace,
        [
            ("Warehouse", "Catalogue"),
            ("Warehouse", source_name),
            ("Lakehouse", source_name),
            ("SemanticModel", "Reporting_Dev"),
        ],
    )
    options = {"base_url": storage.as_posix()} if storage else {}
    session = SourceSession(
        workspace=workspace,
        resolver=FabricResolver(workspace, client=inventory, **options),
        store=FilesystemStore(),
    )
    session.answer_semantic_model(
        workspace.workspace,
        "Reporting_Dev",
        SubmittedDefinition(source_model(source_name)),
    )
    return session


def read_bindings():
    return effective_item_bindings(
        ItemBindings(
            tuple(
                parse_build_item(value)
                for value in (
                    f"{ITEM}=SemanticModel/Reporting_Dev",
                    "Warehouse/Serving=Warehouse/Serving_Dev",
                )
            )
        ),
        control_item="Catalogue",
        workspace_name="Demo",
    )


def capture_publication(monkeypatch, session):
    captured = []
    original = render._merge_statement

    def recording(table, rows, *, scope):
        statement = original(table, rows, scope=scope)
        captured.append((table, copy.deepcopy(rows), statement))
        return statement

    monkeypatch.setattr(render, "_merge_statement", recording)

    def published():
        rows = {}
        for table, values, statement in captured:
            source = statement.split("USING (\n", 1)[1].split(") AS source\n", 1)[0]
            if not any(source in sent for sent in session.tsql):
                continue
            for row in values:
                item = WeaverItemId(row["item_type"], row["item_name"])
                entries = rows.setdefault(item, {}).setdefault(table.name, {})
                entries[tuple(row.get(k) for k in table.key)] = row
        return {
            item: {name: tuple(values.values()) for name, values in tables.items()}
            for item, tables in rows.items()
        }

    return published


@weaver_test()
def test_public_build_binds_sources_and_publishes_object_dependencies(
    tmp_path, monkeypatch
):
    root = source_project(tmp_path)
    with source_session() as session:
        answer_catalogue(session, source_catalogue(), read_bindings())
        published = capture_publication(monkeypatch, session)
        result = weaver.build(
            root, items=f"{ITEM}=SemanticModel/Reporting_Dev", session=session
        )
        assert result.succeeded, result.errors
        parts = submitted_parts(session)
        assert all(
            f"definition/tables/{name}.tmdl" in parts
            for name in ("Sales", "SalesAgain", "Summary")
        )
        assert b"mode: directLake" in parts["definition/tables/Sales.tmdl"]
        assert (
            b'"serving.datawarehouse.fabric.microsoft.com", "Serving_Dev")'
            in parts["definition/expressions.tmdl"]
        )
        tables = {
            table["name"]: table
            for table in decode_model(
                session.semantic_model("Reporting_Dev").definition
            )["model"]["tables"]
        }
        assert tables["Sales"]["partitions"][0]["mode"] == "directLake"
        assert tables["Sales"]["partitions"][0]["source"]["entityName"] == "Sales"
        assert tables["Summary"]["partitions"][0]["source"]["entityName"] == "Summary"
        assert tables["Sales"]["description"] == "Sales description"
        assert tables["Sales"]["columns"][0] == {
            "name": "Id",
            "sourceColumn": "Id",
            "dataType": "int64",
            "description": "Sales key",
        }
        rows = published()[ITEM]
        deps = rows["Dependency"]
        semantic_tables = {r["table_name"]: r for r in rows["SemanticModelTable"]}
        assert all(edge["referencing_object_name"] in semantic_tables for edge in deps)
        assert semantic_tables["Sales"]["description"] == "Sales description"
        assert {
            (
                row["referencing_schema_name"],
                row["referencing_object_name"],
                row["dependency_reference"],
                row["referenced_schema_name"],
                row["referenced_object_name"],
            )
            for row in deps
        } == {
            ("", "Sales", "Warehouse/Serving/Cake.Sales", "Cake", "Sales"),
            ("", "SalesAgain", "Warehouse/Serving/Cake.Sales", "Cake", "Sales"),
            ("", "Summary", "Warehouse/Serving/Cake.Summary", "Cake", "Summary"),
        }
        assert {
            (row["referenced_item_type"], row["referenced_item_name"]) for row in deps
        } == {("Warehouse", "Serving")}
        objects = {row["table_name"]: row for row in rows["SemanticModelTable"]}
        assert {name: row["source_access"] for name, row in objects.items()} == {
            "Sales": "sql",
            "SalesAgain": "sql",
            "Summary": "sql",
        }
        # Only Installation names the physical item.
        assert "Serving_Dev" not in json.dumps(objects)
        assert not session.python and not session.spark_sql


def loadable_source_catalogue():
    rows = copy.deepcopy(dict(source_catalogue().rows[SOURCE]))
    owner = {"item_type": SOURCE.item_type, "item_name": SOURCE.item_name}
    rows["Registry"] += (
        {
            **owner,
            "schema_name": "Cake",
            "object_name": "Seed",
            "object_type": "table",
            "object_role": "data",
            "signature": "seed",
        },
        *(
            {
                **owner,
                "schema_name": "_",
                "object_name": f"Load Cake.{name}",
                "object_type": "stored_procedure",
                "object_role": "load",
                "signature": "load",
            }
            for name in ("Sales", "Seed")
        ),
    )
    rows["Dependency"] = (
        {
            **owner,
            "referencing_schema_name": "Cake",
            "referencing_object_name": "Summary",
            "dependency_reference": "Cake.Seed",
            "referenced_item_type": "Warehouse",
            "referenced_item_name": "Serving",
            "referenced_schema_name": "Cake",
            "referenced_object_name": "Seed",
            "signature": "view",
        },
    )
    return Catalogue({SOURCE: rows})


@weaver_test()
@pytest.mark.parametrize("failure", [False, True])
def test_source_absent_load_uses_published_dependencies_for_order_and_blocking(
    tmp_path, monkeypatch, failure
):
    from weaver.declaration.tsql_load import RESULT_PARAMETER_NAMES
    from weaver.errors import LoadError
    from weaver.load_plan import load_dag
    from weaver.runtime.load_result import LoadResult

    root = source_project(tmp_path)
    with source_session(tmp_path / "storage") as session:
        sources = loadable_source_catalogue()
        answer_catalogue(session, sources, read_bindings())
        published = capture_publication(monkeypatch, session)
        built = weaver.build(
            root, items=f"{ITEM}=SemanticModel/Reporting_Dev", session=session
        )
        assert built.succeeded, built.errors
        rows = {**dict(sources.rows), **published()}
        snapshot = tmp_path / "stored-catalogue.json"
        snapshot.write_text(json.dumps(Catalogue(rows).to_mapping()), encoding="utf-8")
        stored = Catalogue.from_mapping(
            json.loads(snapshot.read_text(encoding="utf-8"))
        )
        dag = stored.dag()
        semantic_edges = [e for e in dag.edges if e.downstream.item == ITEM]
        assert {(e.semantic_table, str(e.upstream)) for e in semantic_edges} == {
            ("Sales", "Warehouse/Serving/Cake.Sales"),
            ("SalesAgain", "Warehouse/Serving/Cake.Sales"),
            ("Summary", "Warehouse/Serving/Cake.Summary"),
        }
        plan = load_dag(dag, items=(SOURCE, ITEM))
        assert sum(n.primitive_kind == "semantic_refresh" for n in plan.nodes) == 1
        assert sum(n.primitive_kind == "onelake_publication" for n in plan.nodes) == 1
        order = []

        def call_procedure(procedure, **kwargs):
            order.append(procedure)
            if failure:
                raise RuntimeError("source failed")
            result = LoadResult(succeeded=True).as_row()
            return {
                physical: result.get(logical)
                for logical, physical in RESULT_PARAMETER_NAMES.items()
            }

        original_sql = session.sql_executor

        def executor(target, *, workspace=None):
            connection = original_sql(target, workspace=workspace)
            if str(target) == "Serving_Dev":
                monkeypatch.setattr(
                    connection, "call_procedure", call_procedure, raising=False
                )
            return connection

        monkeypatch.setattr(session, "sql_executor", executor)
        model = session.semantic_model("Reporting_Dev")

        def refresh(**_):
            order.append("refresh")
            return {**COMPLETED, "requestId": REQUEST_ID}

        model.refresh = refresh
        physical = session.resolve_item("Reporting_Dev", item_type="SemanticModel")
        session.answer_semantic_model(physical.workspace_id, physical.id, model)
        answer_installed(session, stored.rows)
        shutil.rmtree(root)
        if failure:
            with pytest.raises(LoadError, match="source failed") as caught:
                weaver.load([str(SOURCE), str(ITEM)], session=session)
            report = caught.value.report
            assert (
                next(
                    n for n in report.nodes if n.primitive_kind == "semantic_refresh"
                ).status
                == "blocked"
            )
            assert "refresh" not in order
        else:
            report = weaver.load([str(SOURCE), str(ITEM)], session=session)
            assert report.succeeded, report.to_mapping()
            assert order[-1:] == ["refresh"]
            assert sorted(order[:-1]) == [
                "[_].[Load Cake.Sales]",
                "[_].[Load Cake.Seed]",
            ]


@weaver_test()
@pytest.mark.parametrize("mode", ["import", "directQuery", "dual"])
def test_pbip_source_rebind_retains_authored_mode_and_properties(tmp_path, mode):
    from pathlib import Path

    root = source_project(tmp_path)
    folder = root / str(ITEM)
    shutil.copytree(
        Path(__file__).parent / "fixtures/semantic_model/Probe",
        folder,
        dirs_exist_ok=True,
    )
    path = folder / "Probe.SemanticModel/definition/tables/Sales.tmdl"
    original = path.read_text(encoding="utf-8")
    prefix = original.split("\tpartition Sales", 1)[0]
    expression = 'let Source = #"Warehouse/Serving", Sales = Source{[Schema="Cake",Item="Sales"]}[Data] in Sales'
    path.write_text(
        prefix
        + f"\tpartition Sales = m\n\t\tmode: {mode}\n\t\tsource = {expression}\n",
        encoding="utf-8",
    )
    (folder / "Probe.SemanticModel/definition/expressions.tmdl").write_text(
        'expression \'Warehouse/Serving\' = Sql.Database("previous", "database")\n'
    )
    (folder / f"{folder.name}.tmdl").write_text("model Model\n", encoding="utf-8")
    parsed = parse_item_repository(Location(root.as_posix()))
    authored = probe_model()
    prior_sales = next(t for t in authored["model"]["tables"] if t["name"] == "Sales")
    prior_sales["partitions"][0]["mode"] = mode
    prior_sales["partitions"][0]["source"]["expression"] = expression
    with source_session() as session:
        observed = copy.deepcopy(authored)
        observed["model"]["expressions"] = source_model()["model"]["expressions"]
        session.semantic_model("Reporting_Dev").definition = encode_definition(observed)
        answer_catalogue(session, source_catalogue(), read_bindings())
        result = weaver.build(
            root, items=f"{ITEM}=SemanticModel/Reporting_Dev", session=session
        )
        assert result.succeeded, result.errors
        deployed = decode_model(session.semantic_model("Reporting_Dev").definition)
        sales = next(t for t in deployed["model"]["tables"] if t["name"] == "Sales")
        parts = submitted_parts(session)
        assert parts["definition/tables/Sales.tmdl"] == path.read_bytes()
        assert f"mode: {mode}".encode() in parts["definition/tables/Sales.tmdl"]
        assert (
            b'"serving.datawarehouse.fabric.microsoft.com", "Serving_Dev")'
            in parts["definition/expressions.tmdl"]
        )
        assert (
            parts["definition/relationships.tmdl"]
            == parsed.semantic_models[ITEM].parts["definition/relationships.tmdl"]
        )
        assert sales == prior_sales
        physical = session.resolve_item("Serving_Dev", item_type="Warehouse")
        bound = deployed["model"]["expressions"][0]["expression"]
        assert (
            f'"{physical.name}")' in bound
            and 'Sql.Database("serving.datawarehouse.fabric.microsoft.com"' in bound
        )
        assert sales["partitions"][0]["source"]["expression"] == expression
        assert deployed["model"]["relationships"] == authored["model"]["relationships"]
        assert not any(
            "Cake" in q and "INFORMATION_SCHEMA.COLUMNS" in q for q in session.tsql
        )


@pytest.mark.parametrize("newline", ["\n", "\r\n"], ids=["lf", "crlf"])
@weaver_test()
def test_direct_lake_pbip_rebind_keeps_partition_properties_and_unmapped_expression(
    tmp_path,
    newline,
):
    from pathlib import Path

    root = source_project(tmp_path)
    folder = root / str(ITEM)
    shutil.copytree(
        Path(__file__).parent / "fixtures/semantic_model/Probe",
        folder,
        dirs_exist_ok=True,
    )
    path = folder / "Probe.SemanticModel/definition/tables/Sales.tmdl"
    prefix = path.read_text(encoding="utf-8").split("\tpartition Sales", 1)[0]
    path.write_text(
        prefix
        + "\t/// Keep partition\n\tpartition Authored = entity\n\t\tmode: directLake\n\t\tsource\n\t\t\tschemaName: Cake\n\t\t\tentityName: Sales\n\t\t\texpressionSource: 'Warehouse/Serving'\n",
        encoding="utf-8",
    )
    expression_file = folder / "Probe.SemanticModel/definition/expressions.tmdl"
    untouched = 'expression DatabaseQuery = Sql.Database("previous", "database", [CreateNavigationProperties=false])\n'
    expression_file.write_text(
        untouched
        + '\nexpression \'Warehouse/Serving\' = Sql.Database("previous", "database")\n',
        encoding="utf-8",
        newline=newline,
    )
    untouched_bytes = expression_file.read_bytes().split(
        b"expression 'Warehouse/Serving'", 1
    )[0]
    (folder / f"{folder.name}.tmdl").write_text("model Model\n", encoding="utf-8")
    with source_session() as session:
        observed = probe_model()
        sales = next(t for t in observed["model"]["tables"] if t["name"] == "Sales")
        source_observed = source_model(relations={"Sales": "Sales"})["model"]
        partition = source_observed["tables"][0]["partitions"][0]
        partition.update(name="Authored", description="Keep partition")
        sales["partitions"] = [partition]
        observed["model"]["expressions"] = [
            {
                "name": "DatabaseQuery",
                "kind": "m",
                "expression": 'Sql.Database("previous", "database", [CreateNavigationProperties=false])',
            },
            source_observed["expressions"][0],
        ]
        session.semantic_model("Reporting_Dev").definition = encode_definition(observed)
        answer_catalogue(session, source_catalogue(), read_bindings())
        result = weaver.build(
            root, items=f"{ITEM}=SemanticModel/Reporting_Dev", session=session
        )
        assert result.succeeded, result.errors
        parts = submitted_parts(session)
        assert parts["definition/expressions.tmdl"].startswith(untouched_bytes)
        assert parts["definition/tables/Sales.tmdl"] == path.read_bytes()
        native = decode_model(session.semantic_model("Reporting_Dev").definition)[
            "model"
        ]
        sales = next(t for t in native["tables"] if t["name"] == "Sales")
        partition = sales["partitions"][0]
        assert partition["mode"] == "directLake" and partition["name"] == "Authored"
        assert partition["description"] == "Keep partition"
        assert (
            partition["source"]["schemaName"] == "Cake"
            and partition["source"]["entityName"] == "Sales"
        )
        expressions = {e["name"]: e for e in native["expressions"]}
        assert (
            expressions["DatabaseQuery"]["expression"]
            == 'Sql.Database("previous", "database", [CreateNavigationProperties=false])'
        )
        assert partition["source"]["expressionSource"] == "Warehouse/Serving"
        assert (
            "serving.datawarehouse.fabric.microsoft.com"
            in expressions["Warehouse/Serving"]["expression"]
        )


@weaver_test()
@pytest.mark.parametrize("failed_source", [False, True])
def test_declared_source_build_precedes_model_deployment(
    tmp_path, monkeypatch, failed_source
):
    from weaver.declaration.model import WeaverDocumentId

    root = source_project(tmp_path)
    (root / str(ITEM) / f"{ITEM.item_name}.tmdl").write_text(
        shared_source_tmdl(
            relations={"Sales": "Sales"},
            descriptions={"Sales": "Sales facts."},
            notes=False,
        ),
        encoding="utf-8",
    )
    folder = root / str(SOURCE)
    folder.mkdir(parents=True)
    (folder / "Cake.yml").write_text(
        "Schema ID: Cake\nDescription: Cake records.\n", encoding="utf-8"
    )
    (folder / "Cake.Sales.sql").write_text(
        "/*\nTable ID: Cake.Sales\nDescription: Sales facts.\nLineage: Constant\nPrimary key: Id\nSchema:\n  Id: bigint\n  Label: varchar(40)\n*/\nSELECT CAST(1 AS BIGINT) AS Id, CAST('sale' AS VARCHAR(40)) AS Label\n",
        encoding="utf-8",
    )
    repository = parse_item_repository(Location(root.as_posix()))
    identity = WeaverDocumentId.parse("Warehouse/Serving/Cake.Sales")
    assert not repository.semantic_models[ITEM].source_references
    # Managed lineage is resolved during Build against selected source declarations.
    with source_session() as session:
        session.semantic_model("Reporting_Dev").definition = encode_definition(
            source_model(
                relations={"Sales": "Sales"},
                descriptions={"Sales": "Sales facts."},
                notes=False,
            )
        )
        from weaver.build_bundle.bundle import load_bundle

        published = capture_publication(monkeypatch, session)
        result = weaver.build(
            root,
            items="Warehouse/Serving=Warehouse/Serving_Dev",
            bundle_only=True,
            bundle_path=tmp_path / "bundle",
            session=session,
        )
        bundle = load_bundle(Location(result.bundle_path), store=FilesystemStore())
        actions = [a for _, _, a in bundle.plan.actions()]
        source_index = next(
            i
            for i, a in enumerate(actions)
            if a.resource_node_id == str(identity) and a.executor == "tsql"
        )
        assert not any(a.executor == "semantic_model" for a in actions)
        assert not any(
            "Cake" in s and "INFORMATION_SCHEMA.COLUMNS" in s for s in session.tsql
        )
        assert not session.semantic_model("Reporting_Dev").calls

        from weaver.build_bundle.execution_plan import execute_bundle

        source_sql = bundle.store.read(
            bundle.location / actions[source_index].payload
        ).decode("utf-8")
        sql_executor = session.sql_executor
        injected = []

        def source_connection(target, *, workspace=None):
            connection = sql_executor(target, workspace=workspace)
            execute_each = connection.execute_each

            def faulting(groups):
                outcomes = []
                for group in groups:
                    if failed_source and source_sql in group:
                        injected.append(source_sql)
                        outcomes.append("source DDL refused")
                    else:
                        outcomes.extend(execute_each([group]))
                return outcomes

            connection.execute_each = faulting
            return connection

        monkeypatch.setattr(session, "sql_executor", source_connection)
        report = execute_bundle(bundle, session)
        assert report.succeeded is (not failed_source), report.to_mapping()
        if failed_source:
            assert injected == [source_sql]
            assert not session.semantic_model("Reporting_Dev").calls
            assert not any("MERGE INTO [_].[Registry]" in s for s in session.tsql)
        else:
            assert not session.semantic_model("Reporting_Dev").calls
            source_rows = published()
            assert SOURCE in source_rows
            answer_catalogue(session, Catalogue(source_rows), read_bindings())
            model_result = weaver.build(
                root, items=f"{ITEM}=SemanticModel/Reporting_Dev", session=session
            )
            assert model_result.succeeded, model_result.errors
            assert [
                method for method, _ in session.semantic_model("Reporting_Dev").calls
            ] == ["update_definition", "get_definition"]


@weaver_test()
def test_environment_rebinding_and_unchanged_public_build_use_source_signatures(
    tmp_path, monkeypatch
):
    from weaver.catalogue.tables import CATALOGUE_TABLES
    from weaver.workspaces import TargetDeclaration

    root = source_project(tmp_path)
    signatures, bindings, definitions = [], [], []
    for environment, physical in (
        ("Development", "Serving_Dev"),
        ("Production", "Serving_Prod"),
    ):
        workspace = Workspace(
            workspace=environment,
            catalogue="Warehouse/Catalogue",
            targets={SOURCE: TargetDeclaration(physical=physical)},
        )
        source_rows = copy.deepcopy(dict(source_catalogue().rows))
        source_rows[SOURCE]["Installation"][0]["target_name"] = physical
        with source_session(workspace=workspace, source_name=physical) as session:
            answer_catalogue(session, Catalogue(source_rows), read_bindings())
            with monkeypatch.context() as patcher:
                published = capture_publication(patcher, session)
                first = weaver.build(
                    root, items=f"{ITEM}=SemanticModel/Reporting_Dev", session=session
                )
                assert first.succeeded, first.errors
                rows = published()
            signatures.append(rows[ITEM]["Registry"][0]["signature"])
            bindings.append(
                [
                    {k: v for k, v in r.items() if k != "signature"}
                    for r in rows[ITEM]["SemanticModelTable"]
                ]
            )
            definitions.append(
                decode_model(session.semantic_model("Reporting_Dev").definition)
            )
            answer_catalogue(
                session, Catalogue({**source_rows, **rows}), read_bindings()
            )
            for statement in tuple(session.tsql):
                if "from sys.objects" in statement:
                    session.answer_tsql(
                        statement,
                        [
                            {
                                "schema_name": "_",
                                "object_name": table.name,
                                "object_type": "U",
                            }
                            for table in CATALOGUE_TABLES
                        ],
                    )
                elif "from sys.schemas" in statement:
                    session.answer_tsql(statement, [{"name": "_"}])
            client = session.semantic_model("Reporting_Dev")
            client.calls.clear()
            second = weaver.build(
                root, items=f"{ITEM}=SemanticModel/Reporting_Dev", session=session
            )
            assert second.succeeded, second.errors
            assert second.selection.selected_for_build
            assert not second.selection.impact.changed
            assert [method for method, _ in client.calls] == [
                "update_definition",
                "get_definition",
            ]
    assert signatures[0] != signatures[1]
    assert definitions[0] != definitions[1]
    # The catalogue's semantic rows are the same in every environment.
    assert bindings[0] == bindings[1]


@weaver_test()
@pytest.mark.parametrize("override", [False, True])
def test_lakehouse_source_keeps_tables_identity_and_plans_sql_readiness(
    tmp_path, monkeypatch, override
):
    from test_semantic_source_session_boundary import EndpointInventory

    from weaver.catalogue.claims import catalogue_columns
    from weaver.declaration.model import WeaverDocumentId
    from weaver.installed import primitive_candidates
    from weaver.load_plan import load_dag

    root = source_project(tmp_path)
    (root / str(ITEM) / f"{ITEM.item_name}.tmdl").write_text(
        shared_source_tmdl(
            logical="Lakehouse/Curated",
            relations={"Customer": "Customer"},
            descriptions={},
            notes=False,
        ),
        encoding="utf-8",
    )
    source = WeaverItemId.parse("Lakehouse/Curated")
    owner = {"item_type": source.item_type, "item_name": source.item_name}
    common = {**owner, "signature": "source"}
    ((kind, artefact),) = primitive_candidates(
        WeaverDocumentId.parse("Lakehouse/Curated/Tables/Cake.Customer"), "table"
    )
    artefact_schema, artefact_name = catalogue_columns(artefact)
    stored = Catalogue(
        {
            source: {
                "Installation": ({**owner, "target_name": "Serving_Dev"},),
                "Registry": (
                    {
                        **common,
                        "schema_name": "Tables/Cake",
                        "object_name": "Customer",
                        "object_type": "table",
                        "object_role": "data",
                    },
                    {
                        **common,
                        "schema_name": artefact_schema,
                        "object_name": artefact_name,
                        "object_type": "file",
                        "object_role": "load",
                    },
                ),
            }
        }
    )
    workspace = Workspace(
        workspace="Demo",
        catalogue="Warehouse/Catalogue",
        targets={} if override else {source: TargetDeclaration("Serving_Dev")},
    )
    inventory = EndpointInventory(
        "Demo",
        [
            ("Lakehouse", "Serving_Dev"),
            ("Warehouse", "Serving_Dev"),
            ("Warehouse", "Catalogue"),
            ("SemanticModel", "Reporting_Dev"),
        ],
    )
    with SourceSession(
        workspace=workspace,
        resolver=FabricResolver(workspace, client=inventory),
        store=FilesystemStore(),
    ) as session:
        session.answer_semantic_model(
            "Demo",
            "Reporting_Dev",
            SubmittedDefinition(
                source_model(
                    relations={"Customer": "Customer"},
                    descriptions={},
                    notes=False,
                    lakehouse=True,
                )
            ),
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
        answer_catalogue(session, stored, bindings)
        published = capture_publication(monkeypatch, session)
        result = weaver.build(
            root,
            items=f"{ITEM}=SemanticModel/Reporting_Dev",
            session=session,
            data_sources={"Lakehouse/Curated": "Lakehouse/Serving_Dev"}
            if override
            else None,
        )
        assert result.succeeded, result.errors
        rows = published()
        (edge,) = rows[ITEM]["Dependency"]
        assert edge["dependency_reference"] == "Lakehouse/Curated/Tables/Cake.Customer"
        assert edge["referenced_schema_name"] == "Tables/Cake"
        customer = next(
            r for r in rows[ITEM]["SemanticModelTable"] if r["table_name"] == "Customer"
        )
        assert customer["source_access"] == "sql"
        assert "Serving_Dev" not in json.dumps(customer)
        table = decode_model(session.semantic_model("Reporting_Dev").definition)[
            "model"
        ]["tables"][0]
        assert table["partitions"][0]["mode"] == "directLake"
        assert table["partitions"][0]["source"]["schemaName"] == "Cake"
        plan = load_dag(Catalogue({**stored.rows, **rows}).dag(), items=(source, ITEM))
        assert [n.primitive_kind for n in plan.order()] == [
            kind,
            "endpoint_refresh",
            "semantic_refresh",
        ]


@weaver_test()
@pytest.mark.parametrize(
    "mapping, diagnostic",
    [
        (["Absent=Warehouse/Serving_Dev"], "No selected semantic model"),
        (
            {"Warehouse/Serving": "SemanticModel/Reporting_Dev"},
            "data-source target must",
        ),
        (
            [
                "Warehouse/Serving=Warehouse/Serving_Dev",
                "Warehouse/Serving=Warehouse/Serving_Dev",
            ],
            "Duplicate data-source mapping",
        ),
        (
            {" Warehouse/Serving": "Warehouse/Serving_Dev"},
            "without surrounding whitespace",
        ),
        (["Warehouse/Serving"], "data-source requires"),
    ],
)
def test_source_mapping_refusals_precede_any_model_update(
    tmp_path, mapping, diagnostic
):
    from weaver.errors import BuildError, ConfigError

    root = source_project(tmp_path)
    with source_session() as session:
        answer_catalogue(session, source_catalogue(), read_bindings())
        with pytest.raises((BuildError, ConfigError), match=diagnostic):
            weaver.build(
                root,
                items=f"{ITEM}=SemanticModel/Reporting_Dev",
                data_sources=mapping,
                session=session,
            )
        assert not session.semantic_model("Reporting_Dev").calls


@weaver_test()
def test_native_tables_without_source_create_no_inferred_item_edges(
    tmp_path, monkeypatch
):
    from pathlib import Path

    from weaver.load_plan import load_dag

    root = source_project(tmp_path)
    folder = root / str(ITEM)
    shutil.copytree(
        Path(__file__).parent / "fixtures/semantic_model/Probe",
        folder,
        dirs_exist_ok=True,
    )
    (folder / f"{folder.name}.tmdl").write_text(
        "/// Authored model\nmodel Model\n", encoding="utf-8"
    )
    with source_session() as session:
        observed = probe_model()
        observed["model"]["description"] = "Authored model"
        session.semantic_model("Reporting_Dev").definition = encode_definition(observed)
        answer_catalogue(session, source_catalogue(), read_bindings())
        published = capture_publication(monkeypatch, session)
        result = weaver.build(
            root, items=f"{ITEM}=SemanticModel/Reporting_Dev", session=session
        )
        assert result.succeeded, result.errors
        rows = published()
        assert not rows[ITEM].get("Dependency")
        dag = Catalogue({**source_catalogue().rows, **rows}).dag()
        assert not [e for e in dag.edges if e.downstream.item == ITEM]
        assert [n.primitive_kind for n in load_dag(dag, items=(ITEM,)).order()] == [
            "semantic_refresh"
        ]
        assert not any(
            "/connectionString" in path for path in session.resolver().client.requested
        )


@pytest.mark.parametrize("catalogued", [True, False])
@weaver_test()
def test_build_warns_about_tables_whose_source_is_not_traced(tmp_path, catalogued):
    root = source_project(tmp_path)
    path = root / str(ITEM) / f"{ITEM.item_name}.tmdl"
    native = (
        "\tpartition {0} = m\n\t\tmode: import\n\t\tsource = "
        'Value.NativeQuery(#"Warehouse/Serving", "SELECT Id FROM Cake.Sales")\n'
    )
    path.write_text(
        path.read_text()
        + "\ntable Notices\n"
        + native.format("Notices")
        + "\ntable Missing\n\tpartition Missing = entity\n\t\tmode: directLake\n"
        "\t\tsource\n\t\t\tschemaName: Cake\n\t\t\tentityName: NoSuch\n"
        "\t\t\texpressionSource: 'Warehouse/Serving'\n"
        + "\ntable Declared\n\tannotation Weaver.Source = Warehouse/Serving/Cake.Sales\n"
        + native.format("Declared")
        + '\ntable Computed\n\tpartition Computed = calculated\n\t\tsource = ROW("Value", 1)\n'
        + "\ntable Bare\n\tmeasure Count = 1\n"
    )
    workspace = Workspace(
        workspace="Demo",
        catalogue="Warehouse/Catalogue" if catalogued else None,
        targets={SOURCE: TargetDeclaration(physical="Serving_Dev")},
    )
    with source_session(workspace=workspace) as session:
        if catalogued:
            answer_catalogue(session, source_catalogue(), read_bindings())
        result = weaver.build(
            root,
            items=f"{ITEM}=SemanticModel/Reporting_Dev",
            bundle_only=True,
            bundle_path=tmp_path / "bundle",
            session=session,
        )
        assert result.succeeded
        assert session.warnings == (
            [
                f"{ITEM}: tables Notices, Missing read data that is not traced to a "
                "managed Table or View. Add Weaver.Source to name each table's source"
            ]
            if catalogued
            else []
        )


@weaver_test()
def test_health_tables_carry_the_model_source_bindings(tmp_path, monkeypatch):
    from weaver.operations.health import HEALTH_TABLES

    root = source_project(tmp_path)
    with source_session(tmp_path / "storage") as session:
        sources = loadable_source_catalogue()
        answer_catalogue(session, sources, read_bindings())
        published = capture_publication(monkeypatch, session)
        built = weaver.build(
            root, items=f"{ITEM}=SemanticModel/Reporting_Dev", session=session
        )
        assert built.succeeded, built.errors
    names = {table.name for table in HEALTH_TABLES}
    rows = {
        item: {name: value for name, value in tables.items() if name in names}
        for item, tables in {**dict(sources.rows), **published()}.items()
    }
    dag = Catalogue(rows).dag()
    assert not dag.unresolved
    assert {
        str(edge.upstream) for edge in dag.edges if edge.downstream.item == ITEM
    } == {"Warehouse/Serving/Cake.Sales", "Warehouse/Serving/Cake.Summary"}
    # Health assesses the model's refresh as a load.
    from weaver.installed import SEMANTIC_REFRESH

    (model,) = (node for node in dag.nodes if node.identity.item == ITEM)
    assert model.artefact_kind == SEMANTIC_REFRESH
