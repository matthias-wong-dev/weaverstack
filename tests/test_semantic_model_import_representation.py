from pathlib import Path

import pytest
from support.weaver_test import weaver_test

from weaver.errors import ConfigError

FIXTURE = Path(__file__).parent / "fixtures" / "semantic_model" / "Probe"


@weaver_test()
def test_pbip_import_preserves_the_representative_model_as_tmsl():
    from weaver.semantic_models.tmdl import import_pbip

    actual = import_pbip(FIXTURE / "Probe.pbip")

    assert actual["compatibilityLevel"] == 1606
    model = actual["model"]
    assert model["culture"] == "en-US"
    assert model["defaultPowerBIDataSourceVersion"] == "powerBI_V3"
    assert model["sourceQueryCulture"] == "en-US"
    assert model["dataAccessOptions"] == {
        "legacyRedirects": True,
        "returnErrorValuesAsNull": True,
    }
    assert [table["name"] for table in model["tables"]] == ["Sales", "Product"]
    sales, product = model["tables"]
    assert sales["description"] == "Sales entered for the importer boundary."
    assert sales["columns"] == [
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
    ]
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
            "fromCardinality": "many",
            "toCardinality": "one",
            "crossFilteringBehavior": "oneDirection",
            "isActive": True,
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
    from weaver.semantic_models.tmdl import import_model_folder

    definition = tmp_path / "definition"
    definition.mkdir()
    (definition / "model.tmdl").write_text(
        "model Model\n"
        "table Sales\n"
        "\tmeasure Revenue = ```\n"
        "\t\t\tVAR x = 1  \n"
        "\t\t\tRETURN x\n"
        "\t\t\t\n"
        "\t\t\t```\n"
        "\t\tformatString: #,##0\n"
    )
    model = import_model_folder(tmp_path)["model"]
    measure = model["tables"][0]["measures"][0]
    assert measure["expression"] == "VAR x = 1  \nRETURN x\n"
    assert measure["formatString"] == "#,##0"


@pytest.mark.parametrize("property_name", ["inventedProperty", ".weaverDirective"])
@weaver_test()
def test_tmdl_refuses_unsupported_properties_instead_of_dropping_them(
    tmp_path, property_name
):
    from weaver.semantic_models.tmdl import import_model_folder

    definition = tmp_path / "definition"
    definition.mkdir()
    (definition / "model.tmdl").write_text(f"model Model\n\t{property_name}: true\n")
    with pytest.raises(ConfigError, match="unsupported TMDL property"):
        import_model_folder(tmp_path)


@weaver_test()
def test_pbip_import_leaves_every_authored_file_unchanged():
    from weaver.semantic_models.tmdl import import_pbip

    paths = [path for path in FIXTURE.rglob("*") if path.is_file()]
    before = {path: path.read_bytes() for path in paths}
    import_pbip(FIXTURE / "Probe.pbip")
    assert {path: path.read_bytes() for path in paths} == before


@weaver_test()
def test_tmdl_keeps_tabs_inside_an_opaque_dax_string(tmp_path):
    from weaver.semantic_models.tmdl import import_model_folder

    definition = tmp_path / "definition"
    definition.mkdir()
    expression = '"before\tafter"'
    (definition / "model.tmdl").write_text(
        f"model Model\ntable Sales\n\tmeasure Label = {expression}\n"
    )
    model = import_model_folder(tmp_path)["model"]
    assert model["tables"][0]["measures"][0]["expression"] == expression


@pytest.mark.parametrize(
    "child", ["column Label\n        dataType: string", "isHidden: true"]
)
@weaver_test()
def test_tmdl_rejects_child_bearing_table_references(tmp_path, child):
    from weaver.semantic_models.tmdl import import_model_folder

    definition = tmp_path / "definition"
    definition.mkdir()
    (definition / "model.tmdl").write_text(
        f"model Model\ntable Product\ntable Sales\nref table Product\n    {child}\n"
    )
    with pytest.raises(ConfigError, match="child-bearing TMDL table reference"):
        import_model_folder(tmp_path)


@pytest.mark.parametrize("prefix", ["\t\t\t", "            "])
@weaver_test()
def test_tmdl_preserves_retained_leading_tabs_in_fenced_m_strings(tmp_path, prefix):
    from weaver.semantic_models.tmdl import import_model_folder

    definition = tmp_path / "definition"
    definition.mkdir()
    expression = (
        'let\n    Source = "start\n\tinside the string\n\t\nend"\nin\n    Source'
    )
    body = "\n".join(prefix + line for line in expression.split("\n"))
    (definition / "model.tmdl").write_text(
        "model Model\ntable Sales\n\tpartition Sales = m\n"
        "\t\tsource = ```\n" + body + "\n" + prefix + "```\n"
    )
    model = import_model_folder(tmp_path)["model"]
    assert model["tables"][0]["partitions"][0]["source"]["expression"] == expression
