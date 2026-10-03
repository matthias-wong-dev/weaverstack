"""Both semantic bases compile through the same native TMSL overlay path."""

import copy
import json
from pathlib import Path

import pytest
from support.weaver_test import weaver_test

from weaver.semantic_models.tmdl import import_pbip

FIXTURE = Path(__file__).parent / "fixtures" / "semantic_model" / "Probe" / "Probe.pbip"


@pytest.mark.parametrize("pbip", [False, True])
@weaver_test()
def test_both_bases_apply_organisation_then_item_policy_and_calculated_tables(pbip):
    from weaver.semantic_models.compiler import compile_model

    base = import_pbip(FIXTURE) if pbip else None
    organisation = {
        "model": {"culture": "en-AU", "discourageImplicitMeasures": True},
        "tables": {"_Measure": {".dax": "INFO.VIEW.MEASURES()"}},
    }
    item = {"model": {"culture": "en-GB"}, "tables": {"_Measure": {"isHidden": True}}}
    original = copy.deepcopy((base, organisation, item))
    model = compile_model("Reporting", base=base, organisation=organisation, item=item)
    assert json.loads(json.dumps(model)) == model
    assert "tables" not in model
    assert model["model"]["culture"] == "en-GB"
    assert model["model"]["discourageImplicitMeasures"] is True
    assert model["model"]["defaultPowerBIDataSourceVersion"] == "powerBI_V3"
    tables = {table["name"]: table for table in model["model"]["tables"]}
    assert tables["_Measure"] == {
        "name": "_Measure",
        "isHidden": True,
        "partitions": [
            {
                "name": "_Measure",
                "source": {"type": "calculated", "expression": "INFO.VIEW.MEASURES()"},
            }
        ],
    }
    if pbip:
        assert model["compatibilityLevel"] == base["compatibilityLevel"]
        assert tables["Sales"] == next(
            table for table in base["model"]["tables"] if table["name"] == "Sales"
        )
    else:
        assert model["name"] == "Reporting" and model["compatibilityLevel"] == 1606
        assert set(tables) == {"_Measure"}
    assert (base, organisation, item) == original


@weaver_test()
def test_nested_named_objects_merge_without_losing_authored_siblings():
    from weaver.semantic_models.compiler import compile_model

    base = import_pbip(FIXTURE)
    organisation = {
        "tables": {
            "Sales": {
                "measures": {
                    "Revenue": {"displayFolder": "Finance"},
                    "Units": {"expression": "COUNTROWS(Sales)"},
                },
                "columns": {"Amount": {"formatString": "0.00"}},
            }
        }
    }
    item = {
        "tables": {
            "Sales": {
                "measures": [{"name": "Revenue", "formatString": "0.000"}],
            }
        }
    }
    model = compile_model("Reporting", base=base, organisation=organisation, item=item)
    sales = next(
        table for table in model["model"]["tables"] if table["name"] == "Sales"
    )
    measures = {measure["name"]: measure for measure in sales["measures"]}
    assert set(measures) == {"Revenue", "Units"}
    assert measures["Revenue"]["expression"] == "SUM(Sales[Amount])"
    assert measures["Revenue"]["displayFolder"] == "Finance"
    assert measures["Revenue"]["formatString"] == "0.000"
    columns = {column["name"]: column for column in sales["columns"]}
    assert set(columns) == {"Id", "ProductId", "Amount"}
    assert columns["Amount"]["dataType"] == "decimal"
    assert columns["Amount"]["formatString"] == "0.00"


@weaver_test()
def test_native_named_collections_merge_by_identity_and_other_lists_replace():
    from weaver.semantic_models.compiler import compile_model

    base = {
        "compatibilityLevel": 1606,
        "model": {
            "tables": [
                {
                    "name": "Sales",
                    "partitions": [
                        {
                            "name": "Historical",
                            "mode": "import",
                            "source": {"type": "m", "expression": "old"},
                        },
                        {
                            "name": "Recent",
                            "mode": "import",
                            "source": {"type": "m", "expression": "current"},
                        },
                    ],
                    "hierarchies": [
                        {
                            "name": "Dates",
                            "levels": [
                                {"name": "Year", "ordinal": 0, "column": "Year"}
                            ],
                        }
                    ],
                }
            ],
            "roles": [
                {
                    "name": "Reader",
                    "modelPermission": "read",
                    "members": [{"memberName": "previous"}],
                }
            ],
            "annotations": [{"name": "Owner", "value": "Finance"}],
        },
    }
    result = compile_model(
        "Reporting",
        base=base,
        item={
            "model": {
                "tables": {
                    "Sales": {
                        "partitions": {"Recent": {"source": {"expression": "rebound"}}},
                        "hierarchies": {
                            "Dates": {
                                "levels": {"Month": {"ordinal": 1, "column": "Month"}}
                            }
                        },
                    }
                },
                "roles": {"Reader": {"members": [{"memberName": "replacement"}]}},
                "annotations": {"Domain": {"value": "Sales"}},
            }
        },
    )["model"]
    sales = result["tables"][0]
    assert sales["partitions"][0] == base["model"]["tables"][0]["partitions"][0]
    assert sales["partitions"][1] == {
        "name": "Recent",
        "mode": "import",
        "source": {"type": "m", "expression": "rebound"},
    }
    assert sales["hierarchies"][0]["levels"] == [
        {"name": "Year", "ordinal": 0, "column": "Year"},
        {"name": "Month", "ordinal": 1, "column": "Month"},
    ]
    assert result["roles"] == [
        {
            "name": "Reader",
            "modelPermission": "read",
            "members": [{"memberName": "replacement"}],
        }
    ]
    assert result["annotations"] == [
        {"name": "Owner", "value": "Finance"},
        {"name": "Domain", "value": "Sales"},
    ]


@pytest.mark.parametrize(
    "tables",
    [
        [{"name": "Sales"}, {"name": "sales"}],
        [{"name": ""}],
        [{}],
        ["Sales"],
        {"Sales": {"name": "Different"}},
        {"Sales": 5},
    ],
)
@weaver_test()
def test_named_collections_reject_ambiguous_or_invalid_identities(tables):
    from weaver.errors import ConfigError
    from weaver.semantic_models.compiler import compile_model

    with pytest.raises(ConfigError, match="tables"):
        compile_model("Reporting", item={"tables": tables})


@pytest.mark.parametrize(
    "addon, location",
    [
        ({".rules": {}}, ".rules"),
        ({"measures": {"Answer": {"expression": "1"}}}, "measures"),
        ({"model": {"typo": True}}, "typo"),
        ({"model": {"culture": {"unexpected": 1}}}, "culture"),
        (
            {"tables": {"Sales": {".source": "Warehouse/Serving/Sales.Order"}}},
            ".source",
        ),
        ({"tables": {"Sales": {"measures": {"A": {".switch": []}}}}}, ".switch"),
        ({"tables": {"Sales": {".dax": 1}}}, ".dax"),
        (
            {"tables": {"Sales": {".dax": 'ROW("A", 1)', "partitions": []}}},
            "partitions",
        ),
        ({"relationships": ["Sales[A] *<-1 Product[A]"]}, "relationships"),
    ],
)
@weaver_test()
def test_unsupported_or_malformed_addons_fail_at_the_authored_property(addon, location):
    from weaver.errors import ConfigError
    from weaver.semantic_models.compiler import compile_model

    with pytest.raises(ConfigError) as failure:
        compile_model("Reporting", item=addon)
    assert location in str(failure.value)


@weaver_test()
def test_dax_refuses_to_replace_an_existing_source_partition():
    from weaver.errors import ConfigError
    from weaver.semantic_models.compiler import compile_model

    with pytest.raises(ConfigError, match="Sales.*partitions"):
        compile_model(
            "Reporting",
            base=import_pbip(FIXTURE),
            item={"tables": {"Sales": {".dax": 'ROW("A", 1)'}}},
        )
