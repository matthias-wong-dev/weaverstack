"""Native TMDL edits through live objects need no Weaver property schema."""

import pytest
from support.weaver_test import weaver_test

from weaver.errors import ConfigError
from weaver.semantic_models import TmdlDefinition
from weaver.semantic_models.compiler import _SCHEMAS

SALES = b"""table Sales
\tlineageTag: ABC

\t// Quantity is counted, never summed.
\tcolumn Quantity
\t\tdataType: int64
\t\tsourceColumn: Quantity
\t\tisAvailableInMdx: true
\t\tFictionalFutureProperty: before

\tcolumn Description
\t\tdataType: string
\t\tsourceColumn: Description

\tmeasure Revenue = SUM(Sales[Amount])
\t\tformatString: "$#,##0"
\t\tdetailRowsDefinition = ```
\t\t\t\tSELECTCOLUMNS(Sales, "Q", Sales[Quantity])
\t\t\t\t```

\tpartition Sales = entity
\t\tmode: directLake
\t\tsource
\t\t\tentityName: sales
\t\t\tschemaName: dbo
\t\t\texpressionSource: DatabaseQuery

"""
CUSTOMER = b"table Customer\n\tcolumn CustomerId\n\t\tdataType: int64\n"
MODEL = b"model Model\n\tculture: en-US\n\nref table Sales\nref table Customer\n"
RELATIONSHIPS = b"relationship 0f1e\n\tfromColumn: Sales.CustomerId\n\ttoColumn: Customer.CustomerId\n"
FUTURE = b"newThing Foo\n\tnewProperty: bar\n"


def definition(**overrides):
    parts = {
        "definition.pbism": b'{"version":"4.2","settings":{}}\n',
        "definition/model.tmdl": MODEL,
        "definition/tables/Sales.tmdl": SALES,
        "definition/tables/Customer.tmdl": CUSTOMER,
        "definition/relationships.tmdl": RELATIONSHIPS,
        "definition/newThings.tmdl": FUTURE,
    }
    parts.update(overrides)
    return parts, TmdlDefinition(parts)


def changed_only(before, after, path):
    assert {p: v for p, v in after.items() if p != path} == {
        p: v for p, v in before.items() if p != path
    }
    return after[path]


@weaver_test()
def test_unknown_and_unregistered_properties_read_without_a_schema():
    assert "FictionalFutureProperty" not in _SCHEMAS["column"]
    assert "isAvailableInMdx" not in _SCHEMAS["column"]
    assert "detailRowsDefinition" not in _SCHEMAS["measure"]
    _, tmdl = definition()
    sales = tmdl.model.tables["Sales"]
    quantity = sales.columns["Quantity"]
    assert quantity.FictionalFutureProperty == "before"
    assert quantity.isAvailableInMdx is True
    assert quantity.dataType == "int64"
    assert quantity["dataType"] == "int64"
    assert quantity.isHidden is None
    assert sales.measures["Revenue"].expression == "SUM(Sales[Amount])"
    assert sales.measures["Revenue"].formatString == "$#,##0"
    assert (
        sales.measures["Revenue"].detailRowsDefinition
        == 'SELECTCOLUMNS(Sales, "Q", Sales[Quantity])'
    )
    assert sales.lineageTag == "ABC"


@weaver_test()
def test_unknown_property_edits_change_only_their_spans():
    before, tmdl = definition()
    quantity = tmdl.model.tables["Sales"].columns["Quantity"]
    quantity.isAvailableInMdx = False
    quantity.FictionalFutureProperty = "after"
    quantity.AnotherFutureProperty = True
    sales = changed_only(before, tmdl.parts, "definition/tables/Sales.tmdl")
    assert sales == SALES.replace(
        b"\t\tisAvailableInMdx: true\n\t\tFictionalFutureProperty: before\n",
        b"\t\tisAvailableInMdx: false\n\t\tFictionalFutureProperty: after\n"
        b"\t\tAnotherFutureProperty: true\n",
    )


@weaver_test()
def test_added_property_follows_existing_properties_and_keeps_neighbours():
    before, tmdl = definition()
    tmdl.model.tables["sales"].columns["QUANTITY"].isHidden = True
    sales = changed_only(before, tmdl.parts, "definition/tables/Sales.tmdl")
    assert sales == SALES.replace(
        b"\t\tFictionalFutureProperty: before\n",
        b"\t\tFictionalFutureProperty: before\n\t\tisHidden: true\n",
    )


@pytest.mark.parametrize("newline", [b"\n", b"\r\n"], ids=["lf", "crlf"])
@weaver_test()
def test_expression_properties_stay_expressions(newline):
    original = SALES.replace(b"\n", newline)
    _, tmdl = definition(**{"definition/tables/Sales.tmdl": original})
    revenue = tmdl.model.tables["Sales"].measures["Revenue"]
    revenue.detailRowsDefinition = "Sales"
    revenue.expression = "SUMX(Sales, Sales[Amount])"
    revenue.set_expression("formatStringDefinition", '"0.0"')
    sales = tmdl.parts["definition/tables/Sales.tmdl"]
    assert b"\tmeasure Revenue = ```" + newline in sales
    assert b"SUMX(Sales, Sales[Amount])" in sales
    assert b"\t\tdetailRowsDefinition = ```" + newline in sales
    assert b"SELECTCOLUMNS" not in sales
    assert b"\t\tformatStringDefinition = ```" + newline in sales
    assert sales.count(b"\r\n") == (sales.count(b"\n") if newline == b"\r\n" else 0)
    assert revenue.expression == "SUMX(Sales, Sales[Amount])"
    assert revenue.detailRowsDefinition == "Sales"
    assert revenue.formatStringDefinition == '"0.0"'
    assert revenue.formatString == "$#,##0"


@weaver_test()
def test_block_properties_are_objects_and_reject_scalar_assignment():
    before, tmdl = definition()
    source = tmdl.model.tables["Sales"].partitions["Sales"].source
    assert source.entityName == "sales"
    source.entityName = "fact_sales"
    assert tmdl.model.tables["Sales"].partitions["Sales"].mode == "directLake"
    sales = changed_only(before, tmdl.parts, "definition/tables/Sales.tmdl")
    assert sales == SALES.replace(b"entityName: sales", b"entityName: fact_sales")
    with pytest.raises(ConfigError, match="block property"):
        tmdl.model.tables["Sales"].partitions["Sales"].source = "x"


@weaver_test()
def test_collections_iterate_in_definition_order_and_ignore_case():
    _, tmdl = definition()
    model = tmdl.model
    assert [t.name for t in model.tables] == ["Customer", "Sales"]
    assert [c.name for c in model.tables["Sales"].columns] == [
        "Quantity",
        "Description",
    ]
    assert "sales" in model.tables and len(model.tables) == 2
    assert model.tables["SALES"] == model.tables["Sales"]
    assert model.culture == "en-US"
    assert [r.name for r in model.relationships] == ["0f1e"]
    assert len(model.expressions) == 0 and len(model.roles) == 0
    with pytest.raises(KeyError):
        model.tables["Missing"]


@weaver_test()
def test_unknown_object_kinds_are_addressable():
    before, tmdl = definition()
    future = tmdl.model.newThings["Foo"]
    assert future == tmdl.model.objects("newThing")["Foo"]
    assert future.newProperty == "bar"
    future.newProperty = "baz"
    changed = changed_only(before, tmdl.parts, "definition/newThings.tmdl")
    assert changed == FUTURE.replace(b"bar", b"baz")


@weaver_test()
def test_relationships_edit_natively():
    before, tmdl = definition()
    relationship = tmdl.model.relationships["0F1E"]
    assert relationship.fromColumn == "Sales.CustomerId"
    relationship.isActive = False
    changed = changed_only(before, tmdl.parts, "definition/relationships.tmdl")
    assert changed == RELATIONSHIPS + b"\tisActive: false\n"


@weaver_test()
def test_objects_are_added_and_deleted_in_place():
    before, tmdl = definition()
    sales = tmdl.model.tables["Sales"]
    added = sales.measures.add(
        "Margin %", "DIVIDE([Profit], [Revenue])", formatString="0.0%"
    )
    assert added.formatString == "0.0%"
    assert added.expression == "DIVIDE([Profit], [Revenue])"
    del sales.columns["Description"]
    assert [c.name for c in sales.columns] == ["Quantity"]
    with pytest.raises(ConfigError, match="already declares"):
        sales.measures.add("margin %", "1")
    helper = tmdl.model.tables.add("Helper", description="Generated.")
    helper.isHidden = True
    del tmdl.model.tables["Customer"]
    parts = tmdl.parts
    assert "definition/tables/Customer.tmdl" not in parts
    assert parts["definition/model.tmdl"] == MODEL.replace(b"ref table Customer\n", b"")
    assert parts["definition/tables/Helper.tmdl"] == (
        b"/// Generated.\ntable Helper\n\tisHidden: true\n"
    )
    assert b"column Description" not in parts["definition/tables/Sales.tmdl"]
    assert (
        b"\tmeasure 'Margin %' = DIVIDE([Profit], [Revenue])\n\t\tformatString: 0.0%\n"
        in parts["definition/tables/Sales.tmdl"]
    )
    assert parts["definition/newThings.tmdl"] == before["definition/newThings.tmdl"]


@weaver_test()
def test_descriptions_and_removed_properties_edit_their_lines():
    before, tmdl = definition()
    quantity = tmdl.model.tables["Sales"].columns["Quantity"]
    quantity.description = "Units sold.\nNever summed."
    assert quantity.description == "Units sold.\nNever summed."
    del quantity.FictionalFutureProperty
    quantity.isAvailableInMdx = None
    sales = changed_only(before, tmdl.parts, "definition/tables/Sales.tmdl")
    assert sales == SALES.replace(
        b"\tcolumn Quantity\n",
        b"\t/// Units sold.\n\t/// Never summed.\n\tcolumn Quantity\n",
    ).replace(b"\t\tisAvailableInMdx: true\n\t\tFictionalFutureProperty: before\n", b"")
    with pytest.raises(ConfigError, match="renaming"):
        quantity.name = "Units"


@weaver_test()
def test_unchanged_writes_keep_bytes_and_every_edit_is_journalled():
    before, tmdl = definition()
    sales = tmdl.model.tables["Sales"]
    revenue = sales.measures["Revenue"]
    revenue.expression = "SUM(Sales[Amount])"
    revenue.formatString = "$#,##0"
    sales.columns["Quantity"].dataType = "int64"
    assert tmdl.parts == before
    sales.columns["Quantity"].isHidden = True
    sales.measures.add("Count", "COUNTROWS(Sales)")
    del sales.columns["Description"]
    del sales.columns["Quantity"].isHidden
    path = (("table", "Sales"), ("measure", "Revenue"))
    assert tmdl._editor.journal == [
        ("set", path, "expression", "SUM(Sales[Amount])"),
        ("set", path, "formatString", "$#,##0"),
        ("set", (("table", "Sales"), ("column", "Quantity")), "dataType", "int64"),
        ("set", (("table", "Sales"), ("column", "Quantity")), "isHidden", True),
        ("add", (("table", "Sales"), ("measure", "Count"))),
        ("remove", (("table", "Sales"), ("column", "Description"))),
        ("unset", (("table", "Sales"), ("column", "Quantity")), "isHidden"),
    ]


@weaver_test()
def test_reference_values_keep_their_name_quoting():
    _, tmdl = definition(
        **{
            "definition/relationships.tmdl": b"relationship r\n"
            b"\tfromColumn: 'Sales Fact'.'Customer Id'\n\ttoColumn: Customer.CustomerId\n"
            b'\tsecurityFilteringBehavior: \'bothDirections\'\n\tnote: "a ""b"""\n'
        }
    )
    relationship = tmdl.model.relationships["r"]
    assert relationship.fromColumn == "'Sales Fact'.'Customer Id'"
    assert relationship.securityFilteringBehavior == "bothDirections"
    assert relationship.note == 'a "b"'


@weaver_test()
def test_partition_source_type_is_its_inline_header():
    before, tmdl = definition()
    partition = tmdl.model.tables["Sales"].partitions["Sales"]
    assert partition.sourceType == "entity"
    partition.sourceType = "entity"
    assert tmdl.parts == before
    helper = tmdl.model.tables["Customer"].partitions.add("Customer", "calculated")
    helper.set_expression("source", "INFO.VIEW.MEASURES()")
    helper.sourceType = "m"
    customer = tmdl.parts["definition/tables/Customer.tmdl"]
    assert b"\tpartition Customer = m\n\t\tsource = ```\n" in customer


@weaver_test()
def test_new_properties_precede_block_properties():
    model = (
        b"model Model\n\tculture: en-US\n\tdataAccessOptions\n"
        b"\t\tlegacyRedirects\n\nref table Sales\n"
    )
    _, tmdl = definition(**{"definition/model.tmdl": model})
    tmdl.model.discourageImplicitMeasures = True
    assert tmdl.parts["definition/model.tmdl"] == model.replace(
        b"\tdataAccessOptions",
        b"\tdiscourageImplicitMeasures: true\n\tdataAccessOptions",
    )
