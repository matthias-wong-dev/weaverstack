"""Generate a fixed inline Sales model and its linked native Report."""

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


def _model_files(request: ProjectRequest, name: str) -> dict[str, str]:
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
        f"{folder}/definition/tables/Customer.tmdl": """\
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
\t\tsourceColumn: Region code
\t\tsummarizeBy: none

\tpartition Customer = m
\t\tmode: import
\t\tsource =
\t\t\t#table(type table [#"Customer id" = text, #"Customer name" = text, #"Region code" = text], {{"C001", "Harbour Freight Pty Ltd", "NSW"}, {"C002", "Riverside Grocers", "VIC"}, {"C003", "Tablelands Coffee Roasters", "QLD"}, {"C004", "Coastal Marine Supplies", "NSW"}})
""",
    }


def _run(text: str, **style) -> dict:
    run = {"value": text}
    if style:
        run["textStyle"] = style
    return run


def _story(request: ProjectRequest, name: str) -> list[dict]:
    """The page's paragraphs, one per step of the example's chain."""

    return [
        {"textRuns": [_run("Sales", fontWeight="bold", fontSize="20pt")]},
        {
            "textRuns": [
                _run(
                    "Four customers in a native inline model. Initialise can add independent Lakehouse and Warehouse pipelines, and separate managed-source models, without replacing this model's sample data."
                )
            ]
        },
    ]


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


def managed_model_files(request: ProjectRequest) -> dict[str, str]:
    """Additional models connect each managed source without changing the native model."""
    if not request.example_model:
        return {}
    result = {}
    for kind, selected in (
        (LAKEHOUSE, request.lakehouse),
        (WAREHOUSE, request.warehouse),
    ):
        if not selected:
            continue
        model = f"{request.example_model}{kind}"
        entity = "CurrentStatus" if request.example == "Parcel" else "Customer"
        area = "Tables/" if kind == LAKEHOUSE else ""
        table = entity if request.example == "Parcel" else "ManagedCustomer"
        result[f"PowerBI/{request.example_model}/{model}.tmdl"] = (
            "model Model\n\tculture: en-US\n\tdefaultMode: import\n"
            f"\tannotation 'Weaver.BaseSemanticModels' = {request.example_model}\n\n"
            f"table {table}\n"
            f"\tannotation 'Weaver.Source' = {kind}/{selected}/{area}{request.example}.{entity}\n"
        )
    return result
