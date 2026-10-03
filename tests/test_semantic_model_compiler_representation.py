"""Both semantic bases use the same supported TMDL patch compiler."""

import copy
from pathlib import Path

import pytest
from support.weaver_test import weaver_test

from weaver.errors import ConfigError
from weaver.semantic_models.compiler import _addon_patch, _merge, _normalise
from weaver.semantic_models.patching import apply_addons
from weaver.semantic_models.source import SemanticContribution

FIXTURE = Path(__file__).parent / "fixtures/semantic_model/Probe/Probe.SemanticModel"


def base_parts():
    return {
        p.relative_to(FIXTURE).as_posix(): p.read_bytes()
        for p in FIXTURE.rglob("*")
        if p.is_file()
    }


def compile_parts(*, base=None, organisation=None, item=None):
    return apply_addons(
        SemanticContribution(base or {}, {}, {}),
        "Reporting",
        ((organisation, "organisation addon"), (item, "item addon")),
    )


@pytest.mark.parametrize("pbip", [False, True])
@weaver_test()
def test_both_bases_apply_organisation_then_item_policy_and_calculated_tables(pbip):
    base = base_parts() if pbip else None
    organisation = {
        "model": {"culture": "en-AU", "discourageImplicitMeasures": True},
        "tables": {"_Measure": {".dax": "INFO.VIEW.MEASURES()"}},
    }
    item = {"model": {"culture": "en-GB"}, "tables": {"_Measure": {"isHidden": True}}}
    original = copy.deepcopy((base, organisation, item))
    result = compile_parts(base=base, organisation=organisation, item=item)
    assert result.requested["culture"] == "en-GB"
    assert result.requested["discourageImplicitMeasures"] is True
    assert b"culture: en-GB" in result.parts["definition/model.tmdl"]
    assert (
        b"defaultPowerBIDataSourceVersion: powerBI_V3"
        in result.parts["definition/model.tmdl"]
    )
    assert result.requested["tables"] == [
        {
            "name": "_Measure",
            "isHidden": True,
            "partitions": [
                {
                    "name": "_Measure",
                    "source": {
                        "type": "calculated",
                        "expression": "INFO.VIEW.MEASURES()",
                    },
                }
            ],
        }
    ]
    assert b"isHidden: true" in result.parts["definition/tables/_Measure.tmdl"]
    if pbip:
        assert (
            result.parts["definition/database.tmdl"] == base["definition/database.tmdl"]
        )
        assert (
            result.parts["definition/tables/Sales.tmdl"]
            == base["definition/tables/Sales.tmdl"]
        )
    else:
        assert b"database 'Reporting'" in result.parts["definition/database.tmdl"]
        assert b"compatibilityLevel: 1606" in result.parts["definition/database.tmdl"]
        assert {p for p in result.parts if p.startswith("definition/tables/")} == {
            "definition/tables/_Measure.tmdl"
        }
    assert (base, organisation, item) == original


@weaver_test()
def test_nested_named_objects_merge_without_losing_authored_siblings():
    base = base_parts()
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
            "Sales": {"measures": [{"name": "Revenue", "formatString": "0.000"}]}
        }
    }
    result = compile_parts(base=base, organisation=organisation, item=item)
    sales = result.parts["definition/tables/Sales.tmdl"].decode()
    assert "measure Revenue = SUM(Sales[Amount])" in sales
    assert "displayFolder: Finance" in sales and "formatString: 0.000" in sales
    assert "measure 'Units'" in sales and "COUNTROWS(Sales)" in sales
    assert (
        "column Id" in sales
        and "column ProductId" in sales
        and "column Amount" in sales
    )
    assert "dataType: decimal" in sales and "formatString: 0.00" in sales
    assert (
        result.parts["definition/tables/Product.tmdl"]
        == base["definition/tables/Product.tmdl"]
    )


@weaver_test()
def test_known_patch_named_collections_merge_by_identity_and_other_lists_replace():
    base = {
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
                        "levels": [{"name": "Year", "ordinal": 0, "column": "Year"}],
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
    }
    patch = _addon_patch(
        {
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
        }
    )
    result = _merge(_normalise(base), patch)
    sales = result["tables"][0]
    assert sales["partitions"][0] == base["tables"][0]["partitions"][0]
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
    with pytest.raises(ConfigError, match="tables"):
        compile_parts(item={"tables": tables})


@pytest.mark.parametrize(
    "addon,location",
    [
        ({".rules": {}}, ".rules"),
        ({"measures": {"Answer": {"expression": "1"}}}, "measures"),
        ({"model": {"typo": True}}, "typo"),
        ({"model": {"culture": {"unexpected": 1}}}, "culture"),
        ({"tables": {"Sales": {".source": "SemanticModel/Reporting"}}}, ".source"),
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
    with pytest.raises(ConfigError) as failure:
        compile_parts(item=addon)
    assert location in str(failure.value)


@weaver_test()
def test_dax_refuses_to_replace_an_existing_source_partition():
    with pytest.raises(ConfigError, match="Sales.*partitions"):
        compile_parts(
            base=base_parts(), item={"tables": {"Sales": {".dax": 'ROW("A", 1)'}}}
        )
