"""The built-in Catalogue Dashboard composes, deploys and builds like a project."""

import json
import re

import pytest
from support.weaver_test import weaver_test
from test_semantic_model_build_cycle import DefinitionClient
from test_semantic_source_build_cycle import SourceInventory

import weaver
from weaver.catalogue_dashboard import (
    DASHBOARD,
    DASHBOARD_ITEMS,
    DASHBOARD_MODEL,
    DASHBOARD_REPORT,
    renderer_html,
    renderer_literal,
)
from weaver.declaration.repository import parse_item_repository
from weaver.errors import BuildError, DiscoveryError
from weaver.fabric.resolution import FabricResolver
from weaver.locations import Location
from weaver.semantic_models.definition import decode_parts, encode_definition
from weaver.semantic_models.objects import TmdlDefinition
from weaver.sessions import TestSession
from weaver.store import FilesystemStore
from weaver.workspaces import TargetDeclaration, Workspace

HTML_CONTENT = "htmlContent443BE3AD55E043BF878BED274D3A6855"
#: Every measure lives here. Analysis Services reserves the name Measures.
MEASURE_TABLE = "Dashboard measures"

#: Every catalogue table the model reads, by model table.
SOURCES = {
    "Lineage node": "LineageNode",
    "Lineage edge": "LineageEdge",
    "Installation": "Installation",
    "Registry": "Registry",
    "Dependency": "Dependency",
    "Load status": "LoadStatus",
    "Log": "Log",
    "Load statistic": "LoadStatistic",
    "Test status": "TestStatus",
    "Test dictionary": "TestDictionary",
    "Schema dictionary": "SchemaDictionary",
    "Table dictionary": "TableDictionary",
    "Folder dictionary": "FolderDictionary",
    "Column dictionary": "ColumnDictionary",
    "Key dictionary": "KeyDictionary",
    "Foreign key dictionary": "ForeignKeyDictionary",
    "Semantic model": "SemanticModel",
    "Semantic model table": "SemanticModelTable",
    "Semantic model column": "SemanticModelColumn",
    "Semantic model measure": "SemanticModelMeasure",
    "Semantic model relationship": "SemanticModelRelationship",
}

#: The page measure each Report page shows, in page order.
PAGES = {
    "overview": "Overview page",
    "health": "Health page",
    "orchestration": "Load orchestration page",
    "gantt": "Gantt page",
    "performance": "Performance page",
    "entities": "Entity dictionary page",
    "semantic": "Semantic model page",
}

#: The pages that refresh themselves, because they show settling work.
LIVE = {"overview", "health", "orchestration", "gantt"}

#: Every measure, by display folder.
MEASURES = {
    "Health": {
        "Failed loads",
        "Rejected loads",
        "Pending loads",
        "Failed tests",
        "Latest settled completion",
        "Observed at",
    },
    "Estate": {"Items", "Installed objects", "Settled log records"},
    "Data": {
        "Catalogue data",
        "Lineage data",
        "Status data",
        "Build data",
        "Workflow data",
        "Log data",
        "Statistic data",
        "Metadata data",
        "Semantic data",
    },
    "Pages": {"Renderer", *PAGES.values()},
}


def project(tmp_path):
    root = tmp_path / "project"
    root.mkdir()
    return root


def parse(root, *, dashboard=True):
    return parse_item_repository(
        Location(root.as_posix()), catalogue_dashboard=dashboard
    )


def dashboard_model(tmp_path):
    return parse(project(tmp_path)).semantic_models[DASHBOARD_MODEL]


def measures(parts):
    found = {}
    for table in TmdlDefinition(parts).model.tables:
        for measure in table.measures:
            found[measure.name] = (
                table.name,
                measure.displayFolder,
                measure.expression,
            )
    return found


@weaver_test()
def test_a_project_composes_the_dashboard_only_when_it_is_configured(tmp_path):
    root = project(tmp_path)

    plain = parse(root, dashboard=False)
    assert not set(DASHBOARD_ITEMS) & {item.identity for item in plain.items}
    assert DASHBOARD not in plain.powerbi_projects

    composed = parse(root)
    assert set(DASHBOARD_ITEMS) <= {item.identity for item in composed.items}
    assert set(composed.powerbi_projects[DASHBOARD].items) == set(DASHBOARD_ITEMS)
    assert composed.reports[DASHBOARD_REPORT].model == DASHBOARD_MODEL
    model = composed.semantic_models[DASHBOARD_MODEL]
    assert model.refresh_after_deploy
    assert dict(model.source_references) == {
        table: f"Warehouse/_weaver/_.{relation}" for table, relation in SOURCES.items()
    }
    # The Dashboard is package content: it changes the repository, never the folder.
    assert composed.signature != plain.signature
    assert not (root / "PowerBI").exists()


@pytest.mark.parametrize(
    "authored",
    [
        "PowerBI/Reporting/Catalogue Dashboard.tmdl",
        "PowerBI/Catalogue Dashboard/Sales.tmdl",
    ],
    ids=["same logical model", "same project folder"],
)
@weaver_test()
def test_a_projects_own_dashboard_is_refused_rather_than_replaced(tmp_path, authored):
    root = project(tmp_path)
    path = root / authored
    path.parent.mkdir(parents=True)
    path.write_text(
        'table Calendar\n\tpartition Calendar = calculated\n\t\tsource = ROW("Year", 2026)\n'
    )

    # Without the Dashboard configured it is an ordinary project.
    parse(root, dashboard=False)
    with pytest.raises(DiscoveryError, match="is the built-in Catalogue Dashboard"):
        parse(root)


@weaver_test()
def test_every_measure_lives_in_one_measure_table_by_display_folder(tmp_path):
    model = dashboard_model(tmp_path)
    found = measures(model.parts)

    assert {
        folder: {name for name, (_, f, _) in found.items() if f == folder}
        for folder in MEASURES
    } == MEASURES
    assert set(found) == set().union(*MEASURES.values())
    assert {table for table, _, _ in found.values()} == {MEASURE_TABLE}
    table = TmdlDefinition(model.parts).model.tables[MEASURE_TABLE]
    assert "Weaver.MeasureTable" in [a.name for a in table.annotations]


@weaver_test()
def test_the_measures_quote_every_table_and_avoid_reserved_names(tmp_path):
    """LOG is a function and ROWS is reserved, so an unquoted table breaks DAX."""

    model = dashboard_model(tmp_path)
    tables = {table.name for table in TmdlDefinition(model.parts).model.tables}
    for name, (_, _, expression) in measures(model.parts).items():
        if name == "Renderer":
            continue
        assert not re.search(r"\bVAR\s+rows\b", expression, re.IGNORECASE), name
        for table in tables:
            unquoted = re.search(rf"(?<!['\w]){re.escape(table)}\[", expression)
            assert unquoted is None, f"{name} reads {table} unquoted"
        # No column name holds a quote, so a quoted table name never lands in one.
        assert not re.search(r"\[[^\]\"]*'[^\]\"]*\]", expression), name


@weaver_test()
def test_the_renderer_reaches_the_model_as_one_string_literal(tmp_path):
    expression = measures(dashboard_model(tmp_path).parts)["Renderer"][2].strip()

    assert expression.startswith('"<style>') and expression.endswith('</script>"')
    inner = expression[1:-1]
    assert '"' not in inner.replace('""', "")
    html = inner.replace('""', '"')
    assert html == renderer_html()
    assert html.count("</script>") == 1 and html.count("</style>") == 1
    # Every script, in name order, inside one function.
    assert "function focusWindow(" in html and "function assessHealth(" in html
    assert html.index("function parsePayload(") < html.index("function start(")
    assert "--paper:" in html
    with pytest.raises(AssertionError, match="</script"):
        renderer_literal("", "'</script>'")


@weaver_test()
def test_each_page_shows_one_html_content_visual_over_its_page_measure(tmp_path):
    repository = parse(project(tmp_path))
    parts = repository.reports[DASHBOARD_REPORT].parts
    names = measures(repository.semantic_models[DASHBOARD_MODEL].parts)

    report = json.loads(parts["definition/report.json"])
    assert report["publicCustomVisuals"] == [HTML_CONTENT]
    pages = json.loads(parts["definition/pages/pages.json"])
    assert pages["pageOrder"] == list(PAGES)
    for page, measure in PAGES.items():
        visuals = [
            json.loads(content)
            for path, content in parts.items()
            if re.fullmatch(rf"definition/pages/{page}/visuals/\w+/visual\.json", path)
        ]
        (visual,) = visuals
        assert visual["visual"]["visualType"] == HTML_CONTENT
        (projection,) = visual["visual"]["query"]["queryState"]["content"][
            "projections"
        ]
        field = projection["field"]["Measure"]
        assert field["Expression"]["SourceRef"]["Entity"] == MEASURE_TABLE
        assert field["Property"] == measure and measure in names
        # The data element names its page, then the renderer reads it.
        expression = names[measure][2]
        assert (
            f'"page" & UNICHAR(57345) & "name" & UNICHAR(57346) & "{page}"'
            in expression
        )
        assert expression.index('id=""wd-data""') < expression.index("[Renderer]")
        assert "SUBSTITUTE(" in expression and '"<", UNICHAR(57344))' in expression
        settings = json.loads(parts[f"definition/pages/{page}/page.json"])
        assert ("pageRefresh" in settings.get("objects", {})) is (page in LIVE)


@weaver_test()
def test_the_dashboard_deploys_with_the_catalogue_it_reads(tmp_path):
    root = project(tmp_path)
    workspace = Workspace(
        workspace="Demo",
        catalogue="Warehouse/Catalogue",
        catalogue_dashboard="Estate Dashboard",
    )
    session = TestSession(
        workspace=workspace,
        store=FilesystemStore(),
        resolver=FabricResolver(
            workspace,
            client=SourceInventory(
                "Demo",
                [
                    ("Warehouse", "Catalogue"),
                    ("SemanticModel", "Estate Dashboard"),
                    ("Report", "Estate Dashboard"),
                ],
            ),
        ),
    )
    model = DefinitionClient()
    session.answer_semantic_model("Demo", "Estate Dashboard", model)
    from test_report_build_cycle import ReportBoundary

    report = ReportBoundary([])
    session.answer_report("Demo", "Estate Dashboard", report)

    execute = session.execute_mutation

    # Fabric answers readback with what Build asked for, as a deployment that kept it.
    def deployed_as_requested(plan, payloads=None, **options):
        for name, content in (payloads or {}).items():
            if name.endswith(".semantic_model.json"):
                requested = json.loads(content)["requested"]
                model.definition = encode_definition(
                    {"compatibilityLevel": 1606, "model": requested}
                )
        return execute(plan, payloads, **options)

    session.execute_mutation = deployed_as_requested

    from weaver.operations.build import deploy_catalogue_dashboard

    result = deploy_catalogue_dashboard(root, session=session)

    assert result.succeeded, result.errors
    assert set(result.items) >= {str(DASHBOARD_MODEL), str(DASHBOARD_REPORT)}
    # The catalogue tables the model reads build in the same request.
    assert "create schema [_];\n" in session.tsql
    assert any("#weaver_shape___LineageNode" in sql for sql in session.tsql)
    calls = [method for method, _ in model.calls]
    assert calls[:3] == ["update_definition", "refresh", "invalid_measures"]
    submitted = decode_parts(model.calls[0][1]["definition"])
    node = submitted["definition/tables/Lineage%20node.tmdl"].decode()
    assert "mode: directLake" in node
    assert "entityName: LineageNode" in node and "schemaName: _" in node
    definition = TmdlDefinition(submitted).model
    columns = definition.tables["Lineage node"].columns
    assert columns["Signature"].isHidden and columns["Node ID"].isHidden
    assert not columns["Label"].isHidden
    assert set(measures(submitted)) == set().union(*MEASURES.values())
    assert report.calls == ["update", "read"]
    assert not session.spark_sql and not session.python


def _dashboard_workspace():
    return Workspace(
        workspace="Demo",
        catalogue="Warehouse/Catalogue",
        catalogue_dashboard="Estate Dashboard",
        targets={"Warehouse/Curated": TargetDeclaration("Curated")},
    )


@weaver_test()
def test_build_leaves_the_dashboard_to_initialise(tmp_path):
    from weaver.operations.build import _item_bindings

    root = project(tmp_path)
    workspace = _dashboard_workspace()
    repository = parse(root, dashboard=False)
    unscoped = _item_bindings(None, workspace, repository=repository)
    assert {str(b.item) for b in unscoped.entries} == {"Warehouse/Curated"}
    with pytest.raises(BuildError, match="Run weaver initialise"):
        _item_bindings(str(DASHBOARD_MODEL), workspace, repository=repository)
    session = TestSession(workspace=workspace, store=FilesystemStore())
    with pytest.raises(BuildError, match="PowerBI: no items match"):
        weaver.build(root, items="PowerBI", session=session, dry_run=True)
