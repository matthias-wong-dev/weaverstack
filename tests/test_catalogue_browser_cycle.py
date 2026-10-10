"""The built-in Catalogue Browser composes, deploys and builds like a project."""

import json
import re

import pytest
from support.weaver_test import weaver_test
from test_semantic_model_build_cycle import DefinitionClient
from test_semantic_source_build_cycle import SourceInventory

import weaver
from weaver.catalogue_browser import (
    BROWSER,
    BROWSER_ITEMS,
    BROWSER_MODEL,
    BROWSER_REPORT,
)
from weaver.declaration.repository import parse_item_repository
from weaver.errors import DiscoveryError
from weaver.fabric.resolution import FabricResolver
from weaver.locations import Location
from weaver.semantic_models.definition import decode_parts, encode_definition
from weaver.semantic_models.objects import TmdlDefinition
from weaver.sessions import TestSession
from weaver.store import FilesystemStore
from weaver.workspaces import Workspace

HTML_CONTENT_SECURE = "htmlContent443BE3AD55E043BF878BED274D3A6865"

#: Every catalogue table the model reads, by model table.
SOURCES = {
    "BrowserNode": "BrowserNode",
    "BrowserEdge": "BrowserEdge",
    "Focus": "BrowserNode",
    "LoadStatus": "LoadStatus",
    "Log": "Log",
    "LoadStatistic": "LoadStatistic",
    "TestStatus": "TestStatus",
    "TestDictionary": "TestDictionary",
    "TableDictionary": "TableDictionary",
    "ColumnDictionary": "ColumnDictionary",
    "SemanticModelTable": "SemanticModelTable",
    "SemanticModelRelationship": "SemanticModelRelationship",
    "KeyDictionary": "KeyDictionary",
    "ForeignKeyDictionary": "ForeignKeyDictionary",
}

#: Every measure, by display folder.
MEASURES = {
    "Health": {
        "Red tests",
        "Failed loads",
        "Pending loads",
        "Stale loads",
        "Latest settled completion",
        "Observed at",
    },
    "Overview": {"Health tiles", "Findings", "Observed at line"},
    "Graph": {
        "Focus node",
        "Upstream hops",
        "Downstream hops",
        "Graph SVG",
        "Focus details",
    },
    "Live run": {"Run timeline", "Readiness SVG"},
}


def project(tmp_path):
    root = tmp_path / "project"
    root.mkdir()
    return root


def parse(root, *, browser=True):
    return parse_item_repository(Location(root.as_posix()), catalogue_browser=browser)


def browser_model(tmp_path):
    return parse(project(tmp_path)).semantic_models[BROWSER_MODEL]


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
def test_a_project_composes_the_browser_only_when_it_is_configured(tmp_path):
    root = project(tmp_path)

    plain = parse(root, browser=False)
    assert not set(BROWSER_ITEMS) & {item.identity for item in plain.items}
    assert BROWSER not in plain.powerbi_projects

    composed = parse(root)
    assert set(BROWSER_ITEMS) <= {item.identity for item in composed.items}
    assert set(composed.powerbi_projects[BROWSER].items) == set(BROWSER_ITEMS)
    assert composed.reports[BROWSER_REPORT].model == BROWSER_MODEL
    model = composed.semantic_models[BROWSER_MODEL]
    assert model.refresh_after_deploy
    assert dict(model.source_references) == {
        table: f"Warehouse/_weaver/_.{relation}" for table, relation in SOURCES.items()
    }
    # The Browser is package content: it changes the repository, never the folder.
    assert composed.signature != plain.signature
    assert not (root / "PowerBI").exists()


@pytest.mark.parametrize(
    "authored",
    [
        "PowerBI/Reporting/Catalogue Browser.tmdl",
        "PowerBI/Catalogue Browser/Sales.tmdl",
    ],
    ids=["same logical model", "same project folder"],
)
@weaver_test()
def test_a_projects_own_browser_is_refused_rather_than_replaced(tmp_path, authored):
    root = project(tmp_path)
    path = root / authored
    path.parent.mkdir(parents=True)
    path.write_text(
        'table Calendar\n\tpartition Calendar = calculated\n\t\tsource = ROW("Year", 2026)\n'
    )

    # Without the Browser configured it is an ordinary project.
    parse(root, browser=False)
    with pytest.raises(DiscoveryError, match="is the built-in Catalogue Browser"):
        parse(root)


@weaver_test()
def test_the_model_declares_its_measures_in_display_folders(tmp_path):
    found = measures(browser_model(tmp_path).parts)

    assert {
        folder: {name for name, (_, f, _) in found.items() if f == folder}
        for folder in MEASURES
    } == MEASURES
    assert set(found) == set().union(*MEASURES.values())
    assert {table for table, _, _ in found.values()} == {"Browser"}


@weaver_test()
def test_the_measures_quote_every_table_and_avoid_reserved_names(tmp_path):
    """LOG is a function and ROWS is reserved, so an unquoted table breaks DAX."""

    model = browser_model(tmp_path)
    tables = {table.name for table in TmdlDefinition(model.parts).model.tables}
    for name, (_, _, expression) in measures(model.parts).items():
        assert not re.search(r"\bVAR\s+rows\b", expression, re.IGNORECASE), name
        for table in tables:
            unquoted = re.search(rf"(?<!['\w]){re.escape(table)}\[", expression)
            assert unquoted is None, f"{name} reads {table} unquoted"


@weaver_test()
def test_the_graph_window_is_bounded_before_any_svg_is_built(tmp_path):
    expression = measures(browser_model(tmp_path).parts)["Graph SVG"][2]

    # Twelve unrolled hops each way, and Max means twelve.
    assert len(re.findall(r"VAR _u\d+ = ", expression)) == 12
    assert len(re.findall(r"VAR _d\d+ = ", expression)) == 12
    assert "IF(_upPick < 0, 12, _upPick)" in expression
    # The budget applies to the reach before the first fragment of SVG.
    nodes = expression.index("TOPN(150, _ranked")
    edges = expression.index("TOPN(450, _edgeAll")
    assert max(nodes, edges) < expression.index("<svg")


@weaver_test()
def test_the_report_uses_html_content_secure_and_native_slicers(tmp_path):
    repository = parse(project(tmp_path))
    parts = repository.reports[BROWSER_REPORT].parts
    names = set(measures(repository.semantic_models[BROWSER_MODEL].parts))

    report = json.loads(parts["definition/report.json"])
    assert report["publicCustomVisuals"] == [HTML_CONTENT_SECURE]
    pages = json.loads(parts["definition/pages/pages.json"])
    assert pages["pageOrder"] == ["overview", "explore", "liveRun"]

    visuals = {}
    for path, content in parts.items():
        match = re.fullmatch(r"definition/pages/(\w+)/visuals/(\w+)/visual\.json", path)
        if match:
            visuals[match.groups()] = json.loads(content)
    assert {visual["visual"]["visualType"] for visual in visuals.values()} == {
        HTML_CONTENT_SECURE,
        "slicer",
    }
    for (page, name), visual in visuals.items():
        state = visual["visual"]["query"]["queryState"]
        if visual["visual"]["visualType"] == HTML_CONTENT_SECURE:
            (projection,) = state["content"]["projections"]
            assert projection["field"]["Measure"]["Property"] in names
    focus = {
        page: visual
        for (page, name), visual in visuals.items()
        if visual["visual"]["visualType"] == "slicer"
        and visual["visual"]["query"]["queryState"]["Values"]["projections"][0][
            "field"
        ]["Column"]["Expression"]["SourceRef"]["Entity"]
        == "Focus"
    }
    assert set(focus) == {"explore", "liveRun"}
    assert {v["visual"]["syncGroup"]["groupName"] for v in focus.values()} == {"Focus"}
    assert all("Is internal" in json.dumps(v["filterConfig"]) for v in focus.values())

    for page, refreshes in (("overview", True), ("explore", False), ("liveRun", True)):
        settings = json.loads(parts[f"definition/pages/{page}/page.json"])
        assert ("pageRefresh" in settings.get("objects", {})) is refreshes


@weaver_test()
def test_build_compiles_and_deploys_the_browser_through_the_power_bi_step(tmp_path):
    root = project(tmp_path)
    workspace = Workspace(
        workspace="Demo",
        catalogue="Warehouse/Catalogue",
        catalogue_browser="Estate Browser",
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
                    ("SemanticModel", "Estate Browser"),
                    ("Report", "Estate Browser"),
                ],
            ),
        ),
    )
    model = DefinitionClient()
    session.answer_semantic_model("Demo", "Estate Browser", model)
    from test_report_build_cycle import ReportBoundary

    report = ReportBoundary([])
    session.answer_report("Demo", "Estate Browser", report)

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

    result = weaver.build(root, items="PowerBI", session=session)

    assert result.succeeded, result.errors
    assert set(result.items) >= {str(BROWSER_MODEL), str(BROWSER_REPORT)}
    calls = [method for method, _ in model.calls]
    assert calls[:3] == ["update_definition", "refresh", "invalid_measures"]
    submitted = decode_parts(model.calls[0][1]["definition"])
    node = submitted["definition/tables/BrowserNode.tmdl"].decode()
    assert "mode: directLake" in node
    assert "entityName: BrowserNode" in node and "schemaName: _" in node
    assert "column 'Node ID'" in node
    assert set(measures(submitted)) == set().union(*MEASURES.values())
    assert report.calls == ["update", "read"]
    assert not session.spark_sql and not session.python
