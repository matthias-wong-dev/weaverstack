"""Public semantic Build and Load on fixed items; restoration owns the cleanup."""

import json
import os
import shutil
import time
from pathlib import Path
from types import SimpleNamespace

import pytest
from support.semantic_fixture_source import ConfiguredSemanticSource
from support.semantic_models import policy_path
from support.weaver_test import register_session, weaver_test
from test_semantic_model_boundary import _settle_refreshes

import weaver
from weaver.catalogue.connection import catalogue_connection
from weaver.catalogue.reader import read_table
from weaver.catalogue.reconcile import prune_installation
from weaver.catalogue.render import InstallationScope, render_delete_scope
from weaver.catalogue.tables import (
    BOOKMARK,
    CATALOGUE_TABLES,
    CURRENT_STATE_TABLES,
    DEPENDENCY,
    INSTALLATION,
    LOAD_STATISTIC,
    LOAD_STATUS,
    LOG,
    PROJECTED_TABLES,
    REGISTRY,
    SEMANTIC_MODEL,
    SEMANTIC_MODEL_COLUMN,
    SEMANTIC_MODEL_MEASURE,
    SEMANTIC_MODEL_RELATIONSHIP,
    SEMANTIC_MODEL_TABLE,
)
from weaver.catalogue.tsql import literal
from weaver.declaration.model import WeaverDocumentId, WeaverItemId
from weaver.fabric.resolution import FabricResolver
from weaver.fabric.resources import WAREHOUSE, find_item
from weaver.semantic_models.definition import decode_model
from weaver.sessions import ConsoleSession
from weaver.workspaces import Workspace

ITEM = WeaverItemId.parse("SemanticModel/RefreshAcceptance")
ROOT = WeaverDocumentId.model_root(ITEM)
SCOPE = InstallationScope(ITEM.item_type, ITEM.item_name)
PBIP = Path(__file__).parents[1] / "fixtures/semantic_model/Probe"
OWNED_TABLES = (*PROJECTED_TABLES, *CURRENT_STATE_TABLES, LOAD_STATISTIC)


def _owned_counts(connection):
    return connection.rows(
        " UNION ALL ".join(
            f"SELECT {literal(table.name)} AS [table_name], COUNT(*) AS [rows] "
            f"FROM [_].[{table.name}] WHERE {SCOPE.predicate}"
            for table in OWNED_TABLES
        )
    )


@pytest.fixture
def semantic_build_context(
    fabric_workspace_item, fabric_client, fixed_semantic_model_name, tmp_path
):
    # Unlike self-provisioning fixtures, this path only finds the permanent estate.
    catalogue_name = os.environ.get("WEAVER_PYTEST_WEAVER", "PYTEST_WEAVER")
    find_item(
        fabric_workspace_item, catalogue_name, item_type=WAREHOUSE, client=fabric_client
    )
    workspace = Workspace(
        workspace=fabric_workspace_item.name, catalogue=f"Warehouse/{catalogue_name}"
    )
    resolver = FabricResolver(workspace, client=fabric_client)
    with ConsoleSession(
        workspace=workspace, resolver=resolver, progress=False
    ) as session:
        register_session(session)
        connection = catalogue_connection(session)
        missing = [
            f"{table.name}.{column.public_name}"
            for table in CATALOGUE_TABLES
            for column in table.columns
            if column.public_name.casefold() not in (connection.columns_of(table) or {})
        ]
        assert not missing, (
            f"Initialise the fixed catalogue outside pytest before this test: {missing}"
        )
        model = session.semantic_model(fixed_semantic_model_name)
        claims = read_table(connection, INSTALLATION)
        assert not any(
            row["item_type"] == ITEM.item_type
            and (
                row["target_name"] == fixed_semantic_model_name
                or row["item_id"] == model.model_id
            )
            for row in claims
        ), "The fixed semantic item must have no existing catalogue owner"
        assert all(row["rows"] == 0 for row in _owned_counts(connection)), (
            "RefreshAcceptance has existing catalogue state; clean it outside pytest"
        )
        history_predicate = (
            f"[Target type] = {literal(ITEM.item_type)} AND "
            f"[Target name] = {literal(fixed_semantic_model_name)}"
        )
        history = read_table(connection, LOG, predicate=history_predicate)
        retained_log_ids = {row["log_sk"] for row in history}
        _settle_refreshes(model)
        source = ConfiguredSemanticSource.capture(model)
        original = source.original
        backup = tmp_path / "original-definition.json"
        backup.write_text(json.dumps(original), encoding="utf-8")
        print(f"Original semantic definition: {backup}")
        source.attach(session)
        try:
            yield SimpleNamespace(
                session=session,
                connection=connection,
                model=model,
                target=fixed_semantic_model_name,
                source=source,
                wipe=source.wipe,
                load=lambda *a, **k: source.run("load", weaver.load, *a, **k),
            )
        finally:
            source.detach(session)
            try:
                source.restore(_settle_refreshes)
                if source.touched:
                    print(
                        f"Restored semantic model {model.workspace_id}/{model.model_id}"
                    )
            finally:
                print("SEMANTIC_SOURCE_EVIDENCE " + json.dumps(source.evidence))
                if source.touched:
                    _cleanup_catalogue(
                        session, connection, history_predicate, retained_log_ids, claims
                    )


def _cleanup_catalogue(
    session, connection, history_predicate, retained_log_ids, claims
):
    session.flush()
    statements = list(prune_installation(SCOPE))
    statements.extend(
        render_delete_scope(table, scope=SCOPE)
        for table in (*CURRENT_STATE_TABLES, LOAD_STATISTIC)
    )
    added_log_ids = {
        row["log_sk"]
        for row in read_table(connection, LOG, predicate=history_predicate)
    } - retained_log_ids
    statements.extend(
        f"DELETE FROM [_].[Log] WHERE [Log SK] = {literal(log_id)};"
        for log_id in sorted(added_log_ids)
    )
    connection.execute("\n".join(statements))
    remaining = _owned_counts(connection)
    assert all(row["rows"] == 0 for row in remaining), remaining
    assert {
        row["log_sk"]
        for row in read_table(connection, LOG, predicate=history_predicate)
    } == retained_log_ids
    assert {
        json.dumps(row, default=str, sort_keys=True)
        for row in read_table(connection, INSTALLATION)
    } == {json.dumps(row, default=str, sort_keys=True) for row in claims}
    print("Removed only RefreshAcceptance catalogue rows and its new model logs")


@weaver_test(remote=True, resources={"rest", "tds"})
@pytest.mark.parametrize(
    "pbip, extension",
    [(False, True), (True, True), (True, False)],
    ids=["extension-only", "pbip-extension", "anywhere-pbip"],
)
def test_public_build_catalogue_load_dax_and_unchanged_build(
    semantic_build_context, tmp_path, pbip, extension
):
    context = semantic_build_context
    folder = tmp_path / "project" / str(ITEM)
    folder.mkdir(parents=True)
    if pbip:
        shutil.copytree(PBIP, folder, dirs_exist_ok=True)
    if extension:
        (policy_path(folder.parent.parent)).write_text(
            "/// Organisation model policy\nmodel Model\n\tdiscourageImplicitMeasures\n",
            encoding="utf-8",
        )
        (folder / f"{folder.name}.tmdl").write_text(
            '/// Refresh acceptance model\nmodel Model\n\n/// Calendar years\ntable Calendar\n\tpartition Calendar = calculated\n\t\tsource = ROW("Year", 2026)\n',
            encoding="utf-8",
        )
        if pbip:
            extension_path = folder / f"{folder.name}.tmdl"
            extension_path.write_text(
                extension_path.read_text(encoding="utf-8")
                + "\nref table Sales\n\tcolumn ProductId\n\t\tisHidden\n\n\tmeasure RevenueDouble = [Revenue] * 2\n\t\tformatString: 0.00\n"
                + "\nperspective Reporting\n\tperspectiveTable Sales\n\t\tperspectiveColumn Id\n",
                encoding="utf-8",
            )
    else:
        definition_folder = folder / "Probe.SemanticModel" / "definition"
        perspectives = definition_folder / "perspectives"
        perspectives.mkdir()
        (perspectives / "Reporting.tmdl").write_text(
            "perspective Reporting\n\tperspectiveTable Sales\n\t\tperspectiveColumn Id\n",
            encoding="utf-8",
        )
        model_path = definition_folder / "model.tmdl"
        model_path.write_text(
            model_path.read_text(encoding="utf-8") + "\nref perspective Reporting\n",
            encoding="utf-8",
        )
    context.source.retain(folder)
    root = folder.parent.parent
    original_sources = {
        p.relative_to(root).as_posix(): p.read_bytes()
        for p in root.rglob("*")
        if p.is_file()
    }
    selector = f"{ITEM}=SemanticModel/{context.target}"
    built = weaver.build(root, items=selector, session=context.session)
    print(json.dumps(built.to_mapping(), default=str))
    assert built.succeeded, built.errors
    assert ROOT in built.selection.selected_for_build
    (binding,) = read_table(context.connection, INSTALLATION, scope=SCOPE)
    assert (binding["workspace_id"], binding["item_id"]) == (
        context.model.workspace_id,
        context.model.model_id,
    )
    (definition,) = read_table(context.connection, SEMANTIC_MODEL, scope=SCOPE)
    (registered,) = (
        row
        for row in read_table(context.connection, REGISTRY, scope=SCOPE)
        if (
            row["schema_name"],
            row["object_name"],
            row["object_type"],
            row["object_role"],
        )
        == ("", "", "semantic_model", "data")
    )
    assert registered["signature"] == definition["signature"]
    assert "definition" not in definition
    observed = decode_model(context.model.get_definition())["model"]
    if extension:
        assert definition["description"] == "Refresh acceptance model"
        assert observed["culture"] == "en-US"
        assert observed["discourageImplicitMeasures"] is True
        if pbip:
            sales = next(t for t in observed["tables"] if t["name"] == "Sales")
            assert (
                next(c for c in sales["columns"] if c["name"] == "ProductId")[
                    "isHidden"
                ]
                is True
            )
            doubled = next(m for m in sales["measures"] if m["name"] == "RevenueDouble")
            assert (
                doubled["expression"] == "[Revenue] * 2"
                and doubled["formatString"] == "0.00"
            )
            assert any(p["name"] == "Reporting" for p in observed["perspectives"])
    else:
        assert observed["perspectives"][0]["name"] == "Reporting"
    semantic_tables = read_table(context.connection, SEMANTIC_MODEL_TABLE, scope=SCOPE)
    if extension:
        assert (
            next(row for row in semantic_tables if row["table_name"] == "Calendar")[
                "description"
            ]
            == "Calendar years"
        )
    semantic_columns = read_table(
        context.connection, SEMANTIC_MODEL_COLUMN, scope=SCOPE
    )
    assert semantic_columns
    if extension:
        assert any(
            row["table_name"] == "Calendar" and row["column_name"] == "Year"
            for row in semantic_columns
        )
    measures = read_table(context.connection, SEMANTIC_MODEL_MEASURE, scope=SCOPE)
    relationships = read_table(
        context.connection, SEMANTIC_MODEL_RELATIONSHIP, scope=SCOPE
    )
    if pbip:
        assert any(row["measure_name"] == "Revenue" for row in measures)
        assert any(
            row["from_table"] == "Sales" and row["to_table"] == "Product"
            for row in relationships
        )
    else:
        assert not measures and not relationships
    tables = {table["name"]: table for table in observed["tables"]}
    assert set(tables) == (
        ({"Sales", "Product"} if pbip else set())
        | ({"Calendar"} if extension else set())
        | {"__WeaverSource"}
    )
    if extension:
        assert {column["name"] for column in tables["Calendar"]["columns"]} == {"Year"}
    if pbip:
        assert tables["Sales"]["partitions"][0]["mode"] == "import"
    (pending,) = read_table(context.connection, LOAD_STATUS, scope=SCOPE)
    assert pending["result"] == "pending"

    away = root.with_name("source-not-present")
    root.rename(away)
    try:
        report = context.load(str(ITEM), session=context.session)
    finally:
        away.rename(root)
    print(json.dumps(report.to_mapping(), default=str))
    assert report.succeeded and len(report.nodes) == 1
    (node,) = report.nodes
    assert node.result.status == "Completed" and node.result.request_id
    assert node.result.start_time and node.result.end_time
    assert not hasattr(node.result, "rows_inserted")
    (loaded,) = read_table(context.connection, LOAD_STATUS, scope=SCOPE)
    assert loaded["result"] == "succeeded"
    assert loaded["workflow_id"] == report.workflow_id
    assert loaded["started_datetime"] and loaded["completed_datetime"]
    assert loaded["duration_milliseconds"] >= 0
    logs = read_table(
        context.connection,
        LOG,
        predicate=f"[Workflow ID] = {literal(report.workflow_id)}",
    )
    assert any(node.result.request_id in row["details"] for row in logs)
    assert not read_table(context.connection, BOOKMARK, scope=SCOPE)
    assert not read_table(context.connection, LOAD_STATISTIC, scope=SCOPE)
    unchanged = weaver.build(root, items=selector, session=context.session)
    print(json.dumps(unchanged.to_mapping(), default=str))
    assert unchanged.succeeded, unchanged.errors
    assert unchanged.selection.selected_for_build == (ROOT,)
    assert not unchanged.selection.impact.changed
    assert unchanged.installation_report.action_counts()["total"] > 0
    assert (
        read_table(context.connection, LOAD_STATUS, scope=SCOPE)[0]["result"]
        == "pending"
    )
    assert read_table(context.connection, SEMANTIC_MODEL, scope=SCOPE) == (definition,)
    assert {
        p.relative_to(root).as_posix(): p.read_bytes()
        for p in root.rglob("*")
        if p.is_file()
    } == original_sources
    assert not {"livy", "onelake"} & {
        event.resource for event in context.session.telemetry.events()
    }
    print(
        json.dumps(
            {
                "lifecycle": {
                    "pbip": pbip,
                    "extension": extension,
                    "load_status": loaded,
                    "unchanged": unchanged.to_mapping(),
                    "no_livy_or_onelake": True,
                }
            },
            default=str,
        )
    )
    if extension:
        assert context.model.query_dax('EVALUATE ROW("Year", MAX(Calendar[Year]))') == [
            {"[Year]": 2026}
        ]
    if pbip:
        assert context.model.query_dax('EVALUATE ROW("Revenue", [Revenue])') == [
            {"[Revenue]": 20}
        ]


@weaver_test(remote=True, resources={"rest", "tds"})
@pytest.mark.parametrize(
    "shared", [False, True], ids=["extension-source", "shared-expression"]
)
def test_existing_warehouse_source_build_persists_lineage_and_loads_without_source(
    semantic_build_context, tmp_path, shared
):
    context = semantic_build_context
    folder = tmp_path / "project" / str(ITEM)
    folder.mkdir(parents=True)
    if shared:
        from support.semantic_models import fixture_parts

        parts = fixture_parts()
        shutil.copytree(PBIP, folder, dirs_exist_ok=True)
        model_folder = folder / "Probe.SemanticModel"
        shutil.rmtree(model_folder / "definition")
        parts = {
            "definition.pbism": parts["definition.pbism"],
            "definition/database.tmdl": parts["definition/database.tmdl"],
            "definition/model.tmdl": b"model Model\n\tculture: en-US\n\tdefaultPowerBIDataSourceVersion: powerBI_V3\n\nref table InstalledObjects\n",
            "definition/expressions.tmdl": b'expression \'Warehouse/_weaver\' = Sql.Database("previous", "database")\n',
            "definition/tables/InstalledObjects.tmdl": b"/// Installed catalogue objects\ntable InstalledObjects\n\tcolumn LogicalItem\n\t\tdataType: string\n\t\tsourceColumn: Item name\n\n\tpartition InstalledObjects = entity\n\t\tmode: directLake\n\t\tsource\n\t\t\tentityName: Registry\n\t\t\tschemaName: _\n\t\t\texpressionSource: 'Warehouse/_weaver'\n",
        }
        for name, content in parts.items():
            path = model_folder / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(content)
    else:
        (folder / f"{folder.name}.tmdl").write_text(
            "expression 'Warehouse/_weaver' = Sql.Database(\"previous\", \"database\")\n\n/// Installed catalogue objects\ntable InstalledObjects\n\tcolumn LogicalItem\n\t\tsourceColumn: Item name\n\t\tdataType: string\n\tpartition InstalledObjects = entity\n\t\tmode: directLake\n\t\tsource\n\t\t\tschemaName: _\n\t\t\tentityName: Registry\n\t\t\texpressionSource: 'Warehouse/_weaver'\n",
            encoding="utf-8",
        )
    root = folder.parent.parent
    original_sources = {
        p.relative_to(root).as_posix(): p.read_bytes()
        for p in root.rglob("*")
        if p.is_file()
    }
    selector = f"{ITEM}=SemanticModel/{context.target}"
    built = weaver.build(root, items=selector, session=context.session)
    assert built.succeeded, built.errors
    (edge,) = read_table(context.connection, DEPENDENCY, scope=SCOPE)
    assert edge["referencing_object_name"] == "InstalledObjects"
    assert edge["dependency_reference"] == "Warehouse/_weaver/_.Registry"
    assert (edge["referenced_schema_name"], edge["referenced_object_name"]) == (
        "_",
        "Registry",
    )
    objects = read_table(context.connection, SEMANTIC_MODEL_TABLE, scope=SCOPE)
    table = next(row for row in objects if row["table_name"] == "InstalledObjects")
    assert (table["source_mode"], table["source_access"]) == ("directLake", "sql")
    joined = context.connection.rows(
        "SELECT t.[Table name] AS semantic_table, t.[Description] AS description, "
        "d.[Dependency reference] AS producer FROM [_].[Dependency] d "
        "JOIN [_].[SemanticModelTable] t ON t.[Item type] = d.[Item type] "
        "AND t.[Item name] = d.[Item name] "
        "AND t.[Table name] = d.[Referencing object name] "
        f"WHERE d.[Item type] = {literal(ITEM.item_type)} "
        f"AND d.[Item name] = {literal(ITEM.item_name)}"
    )
    assert joined == [
        {
            "semantic_table": "InstalledObjects",
            "description": "Installed catalogue objects",
            "producer": "Warehouse/_weaver/_.Registry",
        }
    ]
    (pending,) = read_table(context.connection, LOAD_STATUS, scope=SCOPE)
    assert pending["result"] == "pending"
    print(json.dumps({"build": built.to_mapping(), "lineage": joined}, default=str))
    away = root.with_name("source-not-present")
    root.rename(away)
    try:
        loaded = context.load(str(ITEM), session=context.session)
    finally:
        away.rename(root)
    print(json.dumps({"load": loaded.to_mapping()}, default=str))
    assert loaded.succeeded, loaded.to_mapping()
    assert [node.primitive_kind for node in loaded.nodes] == ["semantic_refresh"]
    (status,) = read_table(context.connection, LOAD_STATUS, scope=SCOPE)
    assert (
        status["result"] == "succeeded" and status["workflow_id"] == loaded.workflow_id
    )
    unchanged = weaver.build(root, items=selector, session=context.session)
    assert (
        unchanged.succeeded
        and unchanged.installation_report.action_counts()["total"] == 0
    )
    assert read_table(context.connection, LOAD_STATUS, scope=SCOPE) == (status,)
    assert {
        p.relative_to(root).as_posix(): p.read_bytes()
        for p in root.rglob("*")
        if p.is_file()
    } == original_sources
    assert not {"livy", "onelake"} & {
        event.resource for event in context.session.telemetry.events()
    }
    print(
        json.dumps(
            {
                "lifecycle": {
                    "shared": shared,
                    "load_status": status,
                    "unchanged": unchanged.to_mapping(),
                    "no_livy_or_onelake": True,
                }
            },
            default=str,
        )
    )
    expected = context.connection.rows("SELECT COUNT_BIG(*) AS n FROM [_].[Registry]")[
        0
    ]["n"]
    # The Warehouse publishes Delta commits asynchronously; Direct Lake sees the
    # Build's last Registry row once it does, observed within 30 seconds.
    deadline = time.monotonic() + 120
    while True:
        (row,) = context.model.query_dax(
            'EVALUATE ROW("N", COUNTROWS(InstalledObjects))'
        )
        if row["[N]"] == expected or time.monotonic() > deadline:
            break
        time.sleep(10)
    assert row == {"[N]": expected}
    print(
        json.dumps(
            {
                "dax_rows": expected,
                "load_status": status,
                "unchanged": unchanged.to_mapping(),
            },
            default=str,
        )
    )
