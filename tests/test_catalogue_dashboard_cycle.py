"""The Catalogue Dashboard deploys over one catalogue, outside its lifecycle."""

import json
import re

import pytest
from support.weaver_test import weaver_test
from test_semantic_model_build_cycle import DefinitionClient
from test_semantic_source_build_cycle import SourceInventory

import weaver
from weaver.catalogue.tables import TABLES_BY_NAME
from weaver.catalogue_dashboard import (
    DASHBOARD,
    DASHBOARD_ITEMS,
    DASHBOARD_MODEL,
    DASHBOARD_REPORT,
    renderer_html,
    renderer_literal,
)
from weaver.declaration.repository import parse_item_repository
from weaver.errors import CommandError
from weaver.fabric.resolution import FabricResolver
from weaver.locations import Location
from weaver.operations.catalogue_dashboard import (
    required_tables,
    write_dashboard_project,
)
from weaver.semantic_models.definition import (
    decode_parts,
    encode_definition,
    encode_parts,
)
from weaver.semantic_models.objects import TmdlDefinition
from weaver.sessions import TestSession
from weaver.store import FilesystemStore
from weaver.workspaces import Workspace

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
    "orchestration": "Orchestration page",
    "performance": "Performance page",
    "gantt": "Gantt page",
    "entities": "Entity dictionary page",
    "semantic": "Semantic model page",
}

#: The pages that refresh themselves, because they show settling work.
LIVE = {"overview", "health", "orchestration", "performance", "gantt"}

#: Every measure, by display folder.
MEASURES = {
    "Health": {
        "Failed loads",
        "Rejected loads",
        "Pending loads",
        "Failed tests",
        "Latest processed completion",
        "Observed at",
    },
    "Estate": {"Items", "Installed objects", "Processed log records"},
    "Data": {
        "Catalogue data",
        "Lineage data",
        "Node data",
        "Status data",
        "Build data",
        "Workflow data",
        "Run data",
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


def parse(root):
    return parse_item_repository(Location(root.as_posix()))


def dashboard_repository(tmp_path):
    root = project(tmp_path)
    write_dashboard_project(root)
    return parse(root)


def dashboard_model(tmp_path):
    return dashboard_repository(tmp_path).semantic_models[DASHBOARD_MODEL]


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
def test_the_dashboard_is_an_ordinary_power_bi_project(tmp_path):
    repository = dashboard_repository(tmp_path)

    assert set(repository.powerbi_projects[DASHBOARD].items) == set(DASHBOARD_ITEMS)
    assert repository.reports[DASHBOARD_REPORT].model == DASHBOARD_MODEL
    model = repository.semantic_models[DASHBOARD_MODEL]
    assert dict(model.source_references) == {
        table: f"Warehouse/_weaver/_.{relation}" for table, relation in SOURCES.items()
    }
    assert required_tables() == tuple(sorted(SOURCES.values()))


@weaver_test()
def test_an_authored_model_named_like_the_dashboard_builds_like_any_other(tmp_path):
    from weaver.onboarding.project import semantic_extension
    from weaver.operations.build import _item_bindings
    from weaver.workspaces import TargetDeclaration

    root = project(tmp_path)
    authored = root / "PowerBI/Catalogue Dashboard/Catalogue Dashboard.tmdl"
    authored.parent.mkdir(parents=True)
    authored.write_text(semantic_extension(), encoding="utf-8")
    workspace = Workspace(
        workspace="Demo",
        catalogue="Warehouse/Catalogue",
        catalogue_dashboard="Estate Dashboard",
        targets={
            "SemanticModel/Catalogue Dashboard": TargetDeclaration("Estate"),
            "Warehouse/Curated": TargetDeclaration("Curated"),
        },
    )

    selected = _item_bindings("PowerBI", workspace, repository=parse(root))
    unscoped = _item_bindings(None, workspace, repository=parse(root))

    assert [str(b.item) for b in selected.entries] == [str(DASHBOARD_MODEL)]
    assert {str(b.item) for b in unscoped.entries} == {
        str(DASHBOARD_MODEL),
        "Warehouse/Curated",
    }


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
    repository = dashboard_repository(tmp_path)
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


# --- weaver catalogue-dashboard ---------------------------------------------


class CatalogueSource(TestSession):
    """Answers the source catalogue's table list and each table's columns."""

    def __init__(self, *, tables, **options):
        super().__init__(**options)
        self.tables = tables

    def query_tsql(self, statement, **options):
        if "sys.tables" in statement:
            return [{"name": table} for table in sorted(self.tables)]
        named = re.search(r"TABLE_NAME = N?'(\w+)'", statement)
        if named and "INFORMATION_SCHEMA.COLUMNS" in statement:
            return [
                {"column_name": column, "data_type": "varchar"}
                for column in TABLES_BY_NAME[named.group(1)].public_columns
            ]
        return super().query_tsql(statement, **options)


class DashboardDefinition(DefinitionClient):
    """A deployed model that is, or is not, a Catalogue Dashboard."""

    def __init__(self, *, ours=True):
        super().__init__()
        self.ours = ours

    def get_definition(self, format="TMSL"):
        if format == "TMSL":
            return super().get_definition()
        self.calls.append(("get_definition", format))
        table = "Dashboard measures" if self.ours else "Sales"
        return encode_parts(
            {
                "definition.pbism": b"{}",
                f"definition/tables/{table.replace(' ', '%20')}.tmdl": (
                    f"table '{table}'\n\tmeasure 'Renderer' = 1\n".encode()
                ),
            }
        )


def dashboard_session(*, existing=True, tables=None, ours=True, workspace=None):
    from test_report_build_cycle import ReportBoundary

    workspace = workspace or Workspace(
        workspace="Demo",
        catalogue="Warehouse/Catalogue",
        catalogue_dashboard="Estate Dashboard",
    )
    held = [("Warehouse", "Catalogue")]
    if existing:
        held += [("SemanticModel", "Estate Dashboard"), ("Report", "Estate Dashboard")]
    session = CatalogueSource(
        tables=set(required_tables() if tables is None else tables),
        workspace=workspace,
        store=FilesystemStore(),
        resolver=FabricResolver(workspace, client=SourceInventory("Demo", held)),
    )
    model = DashboardDefinition(ours=ours)
    session.answer_semantic_model("Demo", "Estate Dashboard", model)
    report = ReportBoundary([])
    if existing:
        model_id = session.resolve_item(
            "Estate Dashboard", item_type="SemanticModel", workspace=workspace
        ).id
        bound = report.get_binding
        report.get_binding = lambda: (
            {"datasetId": model_id} if report.definition is None else bound()
        )
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
    return session, model, report


@weaver_test()
def test_the_dashboard_deploys_over_the_catalogue_and_only_reads_it():
    session, model, report = dashboard_session()

    result = weaver.deploy_catalogue_dashboard(session=session)

    assert result.succeeded, result.errors
    assert (result.workspace, result.catalogue, result.name) == (
        "Demo",
        "Demo/Warehouse/Catalogue",
        "Estate Dashboard",
    )
    assert [(i.kind, i.status) for i in result.items] == [
        ("SemanticModel", "updated"),
        ("Report", "updated"),
    ]
    calls = [method for method, _ in model.calls]
    assert "update_definition" in calls and calls[-1] == "refresh"
    submitted = decode_parts(
        next(value for method, value in model.calls if method == "update_definition")[
            "definition"
        ]
    )
    node = submitted["definition/tables/Lineage%20node.tmdl"].decode()
    assert "mode: directLake" in node
    assert "entityName: LineageNode" in node and "schemaName: _" in node
    definition = TmdlDefinition(submitted).model
    columns = definition.tables["Lineage node"].columns
    assert columns["Signature"].isHidden and columns["Node ID"].isHidden
    assert not columns["Label"].isHidden
    assert set(measures(submitted)) == set().union(*MEASURES.values())
    assert report.calls == ["update", "read"]
    # The catalogue is read and never written: no table is built, no row changes.
    assert not session.tsql
    assert not session.spark_sql and not session.python


@weaver_test()
def test_a_dry_run_resolves_everything_and_changes_nothing():
    session, model, report = dashboard_session(existing=False)

    result = weaver.deploy_catalogue_dashboard(dry_run=True, session=session)

    assert result.dry_run and result.succeeded
    assert [(i.kind, i.status) for i in result.items] == [
        ("SemanticModel", "created"),
        ("Report", "created"),
    ]
    assert result.sources == tuple(f"_.{t}" for t in sorted(SOURCES.values()))
    assert not model.calls and not report.calls
    assert not session.tsql


@weaver_test()
def test_a_first_deploy_builds_the_project_without_a_catalogue(monkeypatch):
    import yaml

    session, model, _report = dashboard_session(existing=False)
    seen = {}

    def build(source, *, items, workspace_config, session):
        seen["items"] = items
        seen["config"] = yaml.safe_load(open(workspace_config, encoding="utf-8"))
        seen["files"] = sorted(
            str(path.relative_to(source)) for path in source.rglob("*.tmdl")
        )[:1]
        return weaver.BuildResult(
            source=str(source),
            items=tuple(items),
            bundle_id=None,
            installation=None,
            bundle_path=None,
            status="succeeded",
        )

    monkeypatch.setattr("weaver.operations.build.build", build)
    monkeypatch.setattr(
        session,
        "resolve_item",
        _created_after_build(session.resolve_item, seen),
    )

    result = weaver.deploy_catalogue_dashboard(session=session)

    assert seen["items"] == [str(DASHBOARD_MODEL), str(DASHBOARD_REPORT)]
    assert seen["config"] == {
        "workspace": "Demo",
        "targets": {
            str(DASHBOARD_MODEL): "Estate Dashboard",
            str(DASHBOARD_REPORT): "Estate Dashboard",
            "Warehouse/_weaver": "Catalogue",
        },
    }
    assert [(i.kind, i.status) for i in result.items] == [
        ("SemanticModel", "created"),
        ("Report", "created"),
    ]
    assert [method for method, _ in model.calls] == ["refresh"]


def _created_after_build(resolve, seen):
    """Fabric holds the Dashboard's items only once Build has created them."""

    from types import SimpleNamespace

    from weaver.fabric.resources import ItemNotFoundError

    def resolved(item, *, item_type, workspace=None):
        if item == "Estate Dashboard":
            if "items" not in seen:
                raise ItemNotFoundError(f"{item_type} {item!r} was not found")
            return SimpleNamespace(id=f"{item_type}-id", name=item)
        return resolve(item, item_type=item_type, workspace=workspace)

    return resolved


@pytest.mark.parametrize(
    ("options", "named"),
    [
        (
            {"catalogue": ["Demo/Warehouse/A", "Demo/Warehouse/B"]},
            "reads one catalogue",
        ),
        ({"catalogue": "Prod/Warehouse/Catalogue"}, "Pass --workspace Prod"),
        ({"catalogue": "Lakehouse/Catalogue"}, "Workspace/Warehouse/Name"),
    ],
    ids=["two catalogues", "another workspace", "not a Warehouse"],
)
@weaver_test()
def test_the_source_must_be_one_warehouse_in_the_dashboards_workspace(options, named):
    session, model, _report = dashboard_session()

    with pytest.raises(CommandError, match=named):
        weaver.deploy_catalogue_dashboard(session=session, **options)
    assert not model.calls


@weaver_test()
def test_explicit_arguments_override_configuration():
    session, _model, _report = dashboard_session(existing=False)
    session._resolver = FabricResolver(
        session.workspace,
        client=SourceInventory("Demo", [("Warehouse", "Other")]),
    )

    result = weaver.deploy_catalogue_dashboard(
        catalogue="Demo/Warehouse/Other", name="Ops", dry_run=True, session=session
    )

    assert (result.catalogue, result.name) == ("Demo/Warehouse/Other", "Ops")


@weaver_test()
def test_without_a_name_the_feature_stays_off():
    session, _model, _report = dashboard_session(
        workspace=Workspace(workspace="Demo", catalogue="Warehouse/Catalogue")
    )
    with pytest.raises(CommandError, match="Name the Catalogue Dashboard"):
        weaver.deploy_catalogue_dashboard(session=session)


@weaver_test()
def test_a_warehouse_that_is_no_catalogue_is_refused():
    session, model, _report = dashboard_session(tables=set())

    with pytest.raises(CommandError, match="holds no Weaver catalogue"):
        weaver.deploy_catalogue_dashboard(session=session)
    assert not model.calls


@weaver_test()
def test_a_catalogue_without_the_dashboards_tables_is_refused():
    session, model, _report = dashboard_session(
        tables=set(required_tables()) - {"LineageNode", "LineageEdge"}
    )

    with pytest.raises(CommandError, match="has no _.LineageEdge, _.LineageNode"):
        weaver.deploy_catalogue_dashboard(session=session)
    assert not model.calls


@weaver_test()
def test_an_unrelated_model_of_that_name_is_refused_before_any_change():
    session, model, report = dashboard_session(ours=False)

    with pytest.raises(CommandError, match="is not a Catalogue Dashboard"):
        weaver.deploy_catalogue_dashboard(session=session)
    assert [method for method, _ in model.calls] == ["get_definition"]
    assert not report.calls


@weaver_test()
def test_a_configured_target_of_that_name_is_refused():
    from weaver.workspaces import TargetDeclaration

    session, model, _report = dashboard_session(
        workspace=Workspace(
            workspace="Demo",
            catalogue="Warehouse/Catalogue",
            catalogue_dashboard="Estate Dashboard",
            targets={"SemanticModel/Sales": TargetDeclaration("Estate Dashboard")},
        )
    )
    with pytest.raises(CommandError, match="targets: deploys SemanticModel/Sales"):
        weaver.deploy_catalogue_dashboard(session=session)
    assert not model.calls
