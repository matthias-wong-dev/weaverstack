"""Generate the example's Power BI project: a Direct Lake model and a Report.

The model is ordinary Power BI Desktop output. Its one shared expression is
named after the logical item it reads, so Build binds it to the physical target
in ``workspace-config.yml`` and records the table it reads for Load ordering.
The Report's page describes the example's chain from source to page.
"""

from __future__ import annotations

import json
import uuid

from ..declaration.model import LAKEHOUSE, WAREHOUSE
from .project import ProjectRequest

_SCHEMAS = "https://developer.microsoft.com/json-schemas/fabric"
_REPORT_SCHEMA = f"{_SCHEMAS}/item/report/definition"

_PAGE = "overview"


def _json(value) -> str:
    return json.dumps(value, indent=2, ensure_ascii=False) + "\n"


def _platform(kind: str, name: str) -> str:
    """`.platform` as Power BI Desktop writes it, with a logical ID that is
    stable for the name."""

    return _json(
        {
            "$schema": f"{_SCHEMAS}/gitIntegration/platformProperties/2.0.0/schema.json",
            "metadata": {"type": kind, "displayName": name},
            "config": {
                "version": "2.0",
                "logicalId": str(uuid.uuid5(uuid.NAMESPACE_URL, f"{kind}/{name}")),
            },
        }
    )


def _source(request: ProjectRequest) -> tuple[str, str, str]:
    """The logical item, Sales object and region column the model reads."""

    if request.warehouse:
        return f"{WAREHOUSE}/{request.warehouse}", "CustomerByRegion", "Region name"
    return f"{LAKEHOUSE}/{request.lakehouse}", "Customer", "Region code"


def _model_files(request: ProjectRequest, name: str) -> dict[str, str]:
    item, entity, region = _source(request)
    folder = f"PowerBI/{name}/{name}.SemanticModel"
    return {
        f"{folder}/.platform": _platform("SemanticModel", name),
        f"{folder}/definition.pbism": _json(
            {
                "$schema": f"{_SCHEMAS}/item/semanticModel/definitionProperties/1.0.0/schema.json",
                "version": "4.2",
                "settings": {},
            }
        ),
        f"{folder}/definition/database.tmdl": "database\n\tcompatibilityLevel: 1604\n",
        f"{folder}/definition/model.tmdl": (
            "model Model\n"
            "\tculture: en-US\n"
            "\tdefaultPowerBIDataSourceVersion: powerBI_V3\n"
            "\tsourceQueryCulture: en-US\n"
            "\n"
            "ref table Customer\n"
        ),
        # Build replaces the empty server and database with the SQL endpoint of
        # the item this expression is named after.
        f"{folder}/definition/expressions.tmdl": (
            f'expression \'{item}\' = Sql.Database("", "")\n'
        ),
        f"{folder}/definition/tables/Customer.tmdl": f"""\
/// One row per customer, with the region it belongs to.
table Customer

\t/// The number of customers.
\tmeasure Customers = COUNTROWS(Customer)
\t\tformatString: #,##0

\tcolumn 'Customer id'
\t\tdataType: string
\t\tsourceColumn: Customer id
\t\tsummarizeBy: none

\tcolumn 'Customer name'
\t\tdataType: string
\t\tsourceColumn: Customer name
\t\tsummarizeBy: none

\tcolumn Region
\t\tdataType: string
\t\tsourceColumn: {region}
\t\tsummarizeBy: none

\tpartition Customer = entity
\t\tmode: directLake
\t\tsource
\t\t\tentityName: {entity}
\t\t\tschemaName: Sales
\t\t\texpressionSource: '{item}'
""",
    }


def _run(text: str, **style) -> dict:
    run = {"value": text}
    if style:
        run["textStyle"] = style
    return run


def _story(request: ProjectRequest, name: str) -> list[dict]:
    """The page's paragraphs, one per step of the example's chain."""

    item, entity, _ = _source(request)
    origin = "a sales export" if request.lakehouse else "reference data"
    steps = []
    if request.lakehouse:
        steps.append(
            (
                f"{LAKEHOUSE}/{request.lakehouse}",
                "The Sales.Customers Folder writes the export to Files, and the "
                "Sales.Customer Table reads it into Delta.",
            )
        )
    if request.warehouse and request.lakehouse:
        steps.append(
            (
                f"{WAREHOUSE}/{request.warehouse}",
                "Sales.CustomerByRegion joins those customers to Sales.Region in "
                "T-SQL. It reads the Lakehouse table through a logical shortcut.",
            )
        )
    elif request.warehouse:
        steps.append(
            (
                f"{WAREHOUSE}/{request.warehouse}",
                "Sales.Customer and Sales.Region are T-SQL Tables, and "
                "Sales.CustomerByRegion joins them.",
            )
        )
    steps.append(
        (
            f"SemanticModel/{name}",
            f"A Direct Lake model over Sales.{entity}. Its source expression is "
            f"named {item}, and workspace-config.yml binds that to a Fabric item.",
        )
    )
    steps.append((f"Report/{name}", "This page."))
    paragraphs = [
        {"textRuns": [_run("Hello from Weaver", fontWeight="bold", fontSize="20pt")]},
        {
            "textRuns": [
                _run(
                    f"This example traces four customers from {origin} to this "
                    "page. Each step is a file in the project folder."
                )
            ]
        },
    ]
    for number, (step, text) in enumerate(steps, start=1):
        paragraphs.append(
            {
                "textRuns": [
                    _run(f"{number}. {step}", fontWeight="bold"),
                    _run(f"  {text}"),
                ]
            }
        )
    paragraphs.append(
        {
            "textRuns": [
                _run(
                    "weaver build creates the tables, the model and this Report. "
                    "weaver load runs each step after the ones it reads, and "
                    "refreshes the model once its table has loaded. weaver test "
                    "checks the example's Assumptions, and weaver health reports "
                    "what is current."
                )
            ]
        }
    )
    return paragraphs


def _field(kind: str, name: str) -> dict:
    return {
        "field": {
            kind: {
                "Expression": {"SourceRef": {"Entity": "Customer"}},
                "Property": name,
            }
        },
        "queryRef": f"Customer.{name}",
        "nativeQueryRef": name,
    }


def _visual(name: str, position: dict, visual: dict) -> str:
    return _json(
        {
            "$schema": f"{_REPORT_SCHEMA}/visualContainer/2.0.0/schema.json",
            "name": name,
            "position": position,
            "visual": {**visual, "drillFilterOtherVisuals": True},
        }
    )


def _report_files(request: ProjectRequest, name: str) -> dict[str, str]:
    folder = f"PowerBI/{name}/{name}.Report"
    page = f"{folder}/definition/pages/{_PAGE}"
    return {
        f"{folder}/.platform": _platform("Report", name),
        f"{folder}/definition.pbir": _json(
            {
                "$schema": f"{_REPORT_SCHEMA}Properties/2.0.0/schema.json",
                "version": "4.0",
                "datasetReference": {"byPath": {"path": f"../{name}.SemanticModel"}},
            }
        ),
        f"{folder}/definition/version.json": _json(
            {
                "$schema": f"{_REPORT_SCHEMA}/versionMetadata/1.0.0/schema.json",
                "version": "2.0.0",
            }
        ),
        f"{folder}/definition/report.json": _json(
            {
                "$schema": f"{_REPORT_SCHEMA}/report/3.3.0/schema.json",
                "themeCollection": {
                    "baseTheme": {
                        "name": "CY26SU07",
                        "reportVersionAtImport": {
                            "visual": "2.11.0",
                            "report": "3.4.0",
                            "page": "2.3.1",
                        },
                        "type": "SharedResources",
                    }
                },
                "settings": {
                    "useStylableVisualContainerHeader": True,
                    "exportDataMode": "AllowSummarized",
                },
            }
        ),
        f"{folder}/definition/pages/pages.json": _json(
            {
                "$schema": f"{_REPORT_SCHEMA}/pagesMetadata/1.1.0/schema.json",
                "pageOrder": [_PAGE],
                "activePageName": _PAGE,
            }
        ),
        f"{page}/page.json": _json(
            {
                "$schema": f"{_REPORT_SCHEMA}/page/2.0.0/schema.json",
                "name": _PAGE,
                "displayName": "How this example works",
                "displayOption": "FitToPage",
                "height": 720,
                "width": 1280,
            }
        ),
        f"{page}/visuals/introduction/visual.json": _visual(
            "introduction",
            {"x": 40, "y": 40, "z": 0, "height": 640, "width": 640, "tabOrder": 0},
            {
                "visualType": "textbox",
                "objects": {
                    "general": [{"properties": {"paragraphs": _story(request, name)}}]
                },
            },
        ),
        f"{page}/visuals/customers/visual.json": _visual(
            "customers",
            {"x": 720, "y": 40, "z": 1, "height": 160, "width": 520, "tabOrder": 1},
            {
                "visualType": "card",
                "query": {
                    "queryState": {
                        "Values": {"projections": [_field("Measure", "Customers")]}
                    }
                },
            },
        ),
        f"{page}/visuals/byRegion/visual.json": _visual(
            "byRegion",
            {"x": 720, "y": 220, "z": 2, "height": 460, "width": 520, "tabOrder": 2},
            {
                "visualType": "clusteredBarChart",
                "query": {
                    "queryState": {
                        "Category": {
                            "projections": [
                                {**_field("Column", "Region"), "active": True}
                            ]
                        },
                        "Y": {"projections": [_field("Measure", "Customers")]},
                    }
                },
            },
        ),
    }


def powerbi_example_files(request: ProjectRequest) -> dict[str, str]:
    name = request.example_model
    if name is None:
        return {}
    return {
        f"PowerBI/{name}/{name}.pbip": _json(
            {
                "$schema": f"{_SCHEMAS}/pbip/pbipProperties/1.0.0/schema.json",
                "version": "1.0",
                "artifacts": [{"report": {"path": f"{name}.Report"}}],
                "settings": {"enableAutoRecovery": True},
            }
        ),
        **_model_files(request, name),
        **_report_files(request, name),
    }
