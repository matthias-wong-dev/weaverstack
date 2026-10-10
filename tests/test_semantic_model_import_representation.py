"""Opaque TMDL preparation and independent recorded TMSL metadata."""

import json
import shutil
from pathlib import Path

import pytest
from support.weaver_test import weaver_test

from weaver.declaration.model import WeaverItemId
from weaver.declaration.repository import parse_item_repository
from weaver.locations import Location

FIXTURE = Path(__file__).parent / "fixtures" / "semantic_model" / "Probe"
ITEM = WeaverItemId.parse("SemanticModel/Reporting")


def prepare(tmp_path, text=None):
    folder = tmp_path / str(ITEM)
    shutil.copytree(FIXTURE, folder)
    path = folder / "Probe.SemanticModel/definition/model.tmdl"
    if text is not None:
        path.write_bytes(text.encode())
    before = {p: p.read_bytes() for p in folder.rglob("*") if p.is_file()}
    contribution = parse_item_repository(Location(tmp_path.as_posix())).semantic_models[
        ITEM
    ]
    assert {p: p.read_bytes() for p in before} == before
    return contribution.parts["definition/model.tmdl"], contribution


@weaver_test()
def test_recorded_tmsl_readback_retains_the_representative_pbip_metadata():
    from weaver.semantic_models.deployed import canonical_model

    actual = canonical_model(
        json.loads((FIXTURE.parent / "observed-probe.json").read_text())
    )

    assert actual["compatibilityLevel"] == 1606
    model = actual["model"]
    assert model["culture"] == "en-US"
    assert model["defaultPowerBIDataSourceVersion"] == "powerBI_V3"
    assert model["sourceQueryCulture"] == "en-US"
    assert model["dataAccessOptions"] == {
        "legacyRedirects": True,
        "returnErrorValuesAsNull": True,
    }
    assert [table["name"] for table in model["tables"]] == ["Product", "Sales"]
    product, sales = model["tables"]
    assert sales["description"] == "Sales entered for the importer boundary."
    assert sales["columns"] == sorted(
        [
            {
                "name": "Id",
                "dataType": "int64",
                "sourceColumn": "Id",
                "summarizeBy": "none",
            },
            {
                "name": "ProductId",
                "dataType": "int64",
                "sourceColumn": "ProductId",
                "summarizeBy": "none",
            },
            {
                "name": "Amount",
                "dataType": "decimal",
                "sourceColumn": "Amount",
                "formatString": "$#,##0.00",
                "summarizeBy": "sum",
            },
        ],
        key=lambda column: column["name"],
    )
    assert product["columns"][0]["isKey"] is True
    assert product["columns"][1]["sourceColumn"] == "ProductName"
    assert sales["measures"] == [
        {
            "name": "Revenue",
            "expression": "SUM(Sales[Amount])",
            "formatString": "$#,##0.00",
            "description": "Revenue from the two probe rows.",
        }
    ]
    assert model["relationships"] == [
        {
            "name": "Sales_Product",
            "fromTable": "Sales",
            "fromColumn": "ProductId",
            "toTable": "Product",
            "toColumn": "ProductId",
        }
    ]
    assert sales["partitions"] == [
        {
            "name": "Sales",
            "mode": "import",
            "source": {
                "type": "m",
                "expression": "let\n    Source = #table(type table [Id = Int64.Type, ProductId = Int64.Type, Amount = Currency.Type], {{1, 10, 12.5}, {2, 20, 7.5}})\nin\n    Source",
            },
        }
    ]


@weaver_test()
def test_tmdl_preserves_fenced_expression_whitespace(tmp_path):
    text = "model Model\ntable Sales\n\tmeasure Revenue = ```\n\t\t\tVAR x = 1  \n\t\t\tRETURN x\n\t\t\t\n\t\t\t```\n\t\tformatString: #,##0\n"
    actual, _ = prepare(tmp_path, text)
    assert actual == text.encode()


@pytest.mark.parametrize("property_name", ["inventedProperty", ".weaverDirective"])
@weaver_test()
def test_unknown_tmdl_properties_are_not_interpreted_as_addon_directives(
    tmp_path, property_name
):
    text = f"model Model\n\t{property_name}: true\n"
    actual, contribution = prepare(tmp_path, text)
    assert actual == text.encode()
    assert contribution.requested == {}


@weaver_test()
def test_pbip_preparation_leaves_every_authored_file_unchanged(tmp_path):
    _, contribution = prepare(tmp_path)
    model = FIXTURE / "Probe.SemanticModel"
    expected = {
        p.relative_to(model).as_posix(): p.read_bytes()
        for p in model.rglob("*")
        if p.is_file()
    }
    assert contribution.parts == expected


@weaver_test()
def test_tmdl_keeps_tabs_inside_an_opaque_dax_string(tmp_path):
    expression = '"before\tafter"'
    text = f"model Model\ntable Sales\n\tmeasure Label = {expression}\n"
    actual, _ = prepare(tmp_path, text)
    assert actual == text.encode()


@pytest.mark.parametrize(
    "child", ["column Label\n        dataType: string", "isHidden: true"]
)
@weaver_test()
def test_child_bearing_table_references_remain_opaque_without_an_edit(tmp_path, child):
    text = f"model Model\ntable Product\ntable Sales\nref table Product\n    {child}\n"
    actual, _ = prepare(tmp_path, text)
    assert actual == text.encode()


@pytest.mark.parametrize("prefix", ["\t\t\t", "            "])
@weaver_test()
def test_tmdl_preserves_retained_leading_tabs_in_fenced_m_strings(tmp_path, prefix):
    expression = (
        'let\n    Source = "start\n\tinside the string\n\t\nend"\nin\n    Source'
    )
    body = "\n".join(prefix + line for line in expression.split("\n"))
    text = (
        "model Model\ntable Sales\n\tpartition Sales = m\n\t\tsource = ```\n"
        + body
        + "\n"
        + prefix
        + "```\n"
    )
    actual, _ = prepare(tmp_path, text)
    assert actual == text.encode()
