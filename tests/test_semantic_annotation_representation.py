"""Native annotation transformations expose their requested readback values."""

import pytest
from support.weaver_test import weaver_test
from test_semantic_annotation_declaration import ITEM, compile_source

from weaver.errors import ConfigError


def extension_model(tmp_path, text):
    root = tmp_path / "project"
    folder = root / str(ITEM)
    folder.mkdir(parents=True)
    (folder / "extension.tmdl").write_text(text, encoding="utf-8")
    return root


@pytest.mark.parametrize("scope", ["model", "table"])
@weaver_test()
def test_auto_hide_columns_sets_matching_native_column_properties(tmp_path, scope):
    policy = 'annotation Weaver.AutoHideColumns = "*SK"\n'
    root = extension_model(
        tmp_path,
        ("model Model\n\t" + policy + "\n" if scope == "model" else "")
        + "table Sales\n"
        + ("\t" + policy if scope == "table" else "")
        + "\tcolumn ProductSK\n\t\tdataType: int64\n"
        + "\tcolumn ProductId\n\t\tdataType: int64\n",
    )
    before = (root / str(ITEM) / "extension.tmdl").read_bytes()
    semantic = compile_source(root)
    sales = next(t for t in semantic.requested["tables"] if t["name"] == "Sales")
    columns = {c["name"]: c for c in sales["columns"]}
    assert columns["ProductSK"]["isHidden"] is True
    assert "isHidden" not in columns["ProductId"]
    assert b"isHidden" in semantic.parts["definition/tables/Sales.tmdl"]
    assert any(
        b"annotation 'Weaver.AutoHideColumns'" in data
        for data in semantic.parts.values()
    )
    assert (root / str(ITEM) / "extension.tmdl").read_bytes() == before


@pytest.mark.parametrize(
    "from_cardinality,to_cardinality,expected",
    [
        (None, None, {("Sales", "ProductId")}),
        ("one", "many", {("Product", "Id")}),
        ("one", "one", set()),
    ],
)
@weaver_test()
def test_auto_hide_foreign_keys_uses_native_relationship_cardinality(
    tmp_path, from_cardinality, to_cardinality, expected
):
    relationship = "relationship Sales_Product\n\tfromColumn: Sales.ProductId\n\ttoColumn: Product.Id\n"
    if from_cardinality:
        relationship += f"\tfromCardinality: {from_cardinality}\n"
    if to_cardinality:
        relationship += f"\ttoCardinality: {to_cardinality}\n"
    root = extension_model(
        tmp_path,
        "model Model\n\tannotation Weaver.AutoHideForeignKeys = true\n\n"
        "table Sales\n\tcolumn ProductId\n\t\tdataType: int64\n\tcolumn UnrelatedSK\n\t\tdataType: int64\n\n"
        "table Product\n\tcolumn Id\n\t\tdataType: int64\n\n" + relationship,
    )
    semantic = compile_source(root)
    hidden = {
        (t["name"], c["name"])
        for t in semantic.requested["tables"]
        for c in t["columns"]
        if c.get("isHidden")
    }
    assert hidden == expected


@weaver_test()
def test_auto_hide_columns_accepts_multiple_native_glob_patterns(tmp_path):
    root = extension_model(
        tmp_path,
        'model Model\n\tannotation Weaver.AutoHideColumns = ```\n\t\t"*SK"\n\t\t"?d"\n\t\t```\n'
        "table Sales\n\tcolumn CustomerSK\n\t\tdataType: int64\n\tcolumn Id\n\t\tdataType: int64\n\tcolumn Label\n\t\tdataType: string\n",
    )
    semantic = compile_source(root)
    sales = next(t for t in semantic.requested["tables"] if t["name"] == "Sales")
    assert {c["name"] for c in sales["columns"] if c.get("isHidden")} == {
        "CustomerSK",
        "Id",
    }


@pytest.mark.parametrize("hidden", [False, True])
@weaver_test()
def test_measure_table_generates_native_info_partition_and_preserves_visibility(
    tmp_path, hidden
):
    root = extension_model(
        tmp_path,
        "table Metric\n"
        + ("\tisHidden\n" if hidden else "")
        + "\tannotation Weaver.MeasureTable = true\n\tmeasure One = 1\n",
    )
    semantic = compile_source(root)
    table = next(t for t in semantic.requested["tables"] if t["name"] == "Metric")
    assert table.get("isHidden", False) is hidden
    assert table["partitions"] == [
        {
            "name": "Metric",
            "mode": "import",
            "source": {"type": "calculated", "expression": "INFO.VIEW.MEASURES()"},
        }
    ]
    assert table["measures"] == [{"name": "One", "expression": "1"}]
    data = semantic.parts["definition/tables/Metric.tmdl"]
    assert (
        b"INFO.VIEW.MEASURES()" in data
        and b"annotation 'Weaver.MeasureTable' = true" in data
    )


@weaver_test()
def test_switch_generates_value_and_dynamic_format_for_qualified_measures(tmp_path):
    root = extension_model(
        tmp_path,
        "table Sales\n\tmeasure Revenue = 12\n\t\tformatString: 0.00\n\n"
        "table Finance\n\tmeasure 'Gross Margin %' = 0.4\n\t\tformatString: 0.0%\n\n"
        "table Metric\n\tannotation Weaver.MeasureTable = true\n"
        "\tmeasure Value\n\t\tannotation Weaver.Switch = ```\n"
        "\t\t\tSales[Revenue]\n\t\t\tFinance[Gross Margin %]\n\t\t\t```\n",
    )
    semantic = compile_source(root)
    table = next(t for t in semantic.requested["tables"] if t["name"] == "Metric")
    value = next(m for m in table["measures"] if m["name"] == "Value")
    assert (
        value["expression"]
        == "SWITCH(\n    SELECTEDVALUE('Metric'[Name]),\n    \"Revenue\", 'Sales'[Revenue],\n    \"Gross Margin %\", 'Finance'[Gross Margin %]\n)"
    )
    assert (
        value["formatStringDefinition"]["expression"]
        == 'SWITCH(\n    SELECTEDVALUE(\'Metric\'[Name]),\n    "Revenue", "0.00",\n    "Gross Margin %", "0.0%"\n)'
    )
    assert (
        b"formatStringDefinition =" in semantic.parts["definition/tables/Metric.tmdl"]
    )
    assert (
        b"annotation 'Weaver.Switch'" in semantic.parts["definition/tables/Metric.tmdl"]
    )


def switch_model(tmp_path, references, *, duplicate=False, format_definition=None):
    source = "table Sales\n\tmeasure Revenue = 12\n\t\tformatString: 0.00\n"
    if format_definition is not None:
        source += f"\t\tformatStringDefinition = {format_definition}\n"
    if duplicate:
        source += "\ntable Finance\n\tmeasure Revenue = 2\n\t\tformatString: 0%\n"
    return extension_model(
        tmp_path,
        source + "\ntable Metric\n\tannotation Weaver.MeasureTable = true\n"
        "\tmeasure Value\n\t\tannotation Weaver.Switch = ```\n"
        + "".join("\t\t\t" + r + "\n" for r in references)
        + "\t\t\t```\n",
    )


@pytest.mark.parametrize("reference", ["Revenue", "[Revenue]"])
@weaver_test()
def test_switch_resolves_unique_unqualified_measure(tmp_path, reference):
    semantic = compile_source(switch_model(tmp_path, [reference]))
    metric = next(t for t in semantic.requested["tables"] if t["name"] == "Metric")
    value = next(m for m in metric["measures"] if m["name"] == "Value")
    assert "'Sales'[Revenue]" in value["expression"]


@weaver_test()
def test_switch_rejects_ambiguous_unqualified_measure(tmp_path):
    with pytest.raises(ConfigError, match="(?i)ambiguous.*Revenue"):
        compile_source(switch_model(tmp_path, ["Revenue"], duplicate=True))


@weaver_test()
def test_switch_preserves_usable_source_dynamic_format_expression(tmp_path):
    expression = 'IF(TRUE(), "0.0%", "0%")'
    semantic = compile_source(
        switch_model(tmp_path, ["Sales[Revenue]"], format_definition=expression)
    )
    metric = next(t for t in semantic.requested["tables"] if t["name"] == "Metric")
    value = next(m for m in metric["measures"] if m["name"] == "Value")
    assert (
        value["formatStringDefinition"]["expression"]
        == "SWITCH(\n    SELECTEDVALUE('Metric'[Name]),\n    \"Revenue\", "
        + expression
        + "\n)"
    )


@weaver_test()
def test_switch_refuses_measure_context_dependent_dynamic_format(tmp_path):
    with pytest.raises(ConfigError, match="(?i)dynamic format.*context"):
        compile_source(
            switch_model(
                tmp_path,
                ["Sales[Revenue]"],
                format_definition='IF(SELECTEDMEASURE() > 1, "0.0", "0")',
            )
        )
