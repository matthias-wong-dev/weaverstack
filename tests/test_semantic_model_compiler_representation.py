"""Both semantic bases use native TMDL overlays and requested-value expectations."""

import copy
from pathlib import Path

import pytest
from support.weaver_test import weaver_test

from weaver.errors import ConfigError
from weaver.semantic_models.compiler import _merge
from weaver.semantic_models.extensions import apply_extensions
from weaver.semantic_models.source import SemanticContribution

FIXTURE = Path(__file__).parent / "fixtures/semantic_model/Probe/Probe.SemanticModel"


def base_parts():
    return {
        p.relative_to(FIXTURE).as_posix(): p.read_bytes()
        for p in FIXTURE.rglob("*")
        if p.is_file()
    }


def compile_parts(*, base=None, organisation=None, item=None):
    fragments = tuple(
        (text.encode(), origin)
        for text, origin in (
            (organisation, "organisation extension.tmdl"),
            (item, "item extension.tmdl"),
        )
        if text is not None
    )
    return apply_extensions(
        SemanticContribution(base or {}, {}, {}), "Reporting", fragments
    )


@pytest.mark.parametrize("pbip", [False, True])
@weaver_test()
def test_both_bases_apply_organisation_then_item_policy_and_calculated_tables(pbip):
    base = base_parts() if pbip else None
    organisation = """model Model
    culture: en-AU
    discourageImplicitMeasures

table _Measure
    partition _Measure = calculated
        source = INFO.VIEW.MEASURES()
"""
    item = """model Model
    culture: en-GB

ref table _Measure
    isHidden
"""
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
    assert b"isHidden" in result.parts["definition/tables/_Measure.tmdl"]
    if pbip:
        for path in ("definition/database.tmdl", "definition/tables/Sales.tmdl"):
            assert result.parts[path] == base[path]
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
    organisation = """ref table Sales
    measure Revenue
        displayFolder: Finance

    measure Units = COUNTROWS(Sales)

    column Amount
        formatString: 0.00
"""
    item = """ref table Sales
    measure Revenue
        formatString: 0.000
"""
    result = compile_parts(base=base, organisation=organisation, item=item)
    sales = result.parts["definition/tables/Sales.tmdl"].decode()
    assert "measure Revenue = SUM(Sales[Amount])" in sales
    assert "displayFolder: Finance" in sales and "formatString: 0.000" in sales
    assert "measure Units" in sales and "COUNTROWS(Sales)" in sales
    assert all(f"column {name}" in sales for name in ("Id", "ProductId", "Amount"))
    assert "dataType: decimal" in sales and "formatString: 0.00" in sales
    assert (
        result.parts["definition/tables/Product.tmdl"]
        == base["definition/tables/Product.tmdl"]
    )


@weaver_test()
def test_known_expectation_collections_merge_by_identity_and_other_lists_replace():
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
    patch = {
        "tables": [
            {
                "name": "Sales",
                "partitions": [{"name": "Recent", "source": {"expression": "rebound"}}],
                "hierarchies": [
                    {
                        "name": "Dates",
                        "levels": [{"name": "Month", "ordinal": 1, "column": "Month"}],
                    }
                ],
            }
        ],
        "roles": [{"name": "Reader", "members": [{"memberName": "replacement"}]}],
        "annotations": [{"name": "Domain", "value": "Sales"}],
    }
    result = _merge(base, patch)
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
    "fragment",
    [
        "table Sales\n\ntable sales\n",
        "table ''\n",
        "table\n",
        "ref table\n",
        "table Sales\n    column Id\n    column Id\n",
        "table Sales\n    isHidden\n    isHidden: false\n",
    ],
)
@weaver_test()
def test_named_declarations_reject_ambiguous_or_invalid_identities(fragment):
    with pytest.raises(ConfigError, match="extension.tmdl"):
        compile_parts(item=fragment)


@pytest.mark.parametrize(
    "fragment,location",
    [
        (".rules: {}\n", "1"),
        ("table Sales\n    .source: Warehouse/Serving/Cake.Sales\n", "2"),
        ("table Sales\n    .dax: ROW(1)\n", "2"),
        ("model Model\n    discourageImplicitMeasures: perhaps\n", "2"),
        ("table Sales\n    measure A = ```\n        unclosed\n", "2"),
        ("model Model\n    dataAccessOptions\n       legacyRedirects\n", "3"),
        ("model Model\n    culture: en-US\n    culture: en-GB\n", "3"),
        ("table Sales\n    column Id\n        isHidden: 1\n", "3"),
        ("table Sales\n    unknown property without syntax\n", "2"),
    ],
)
@weaver_test()
def test_malformed_extensions_fail_at_the_authored_line(fragment, location):
    with pytest.raises(ConfigError, match=f"extension.tmdl:{location}:"):
        compile_parts(item=fragment)


@weaver_test()
def test_native_partition_overlay_replaces_explicitly_addressed_source():
    base = base_parts()
    result = compile_parts(
        base=base,
        item='ref table Sales\n    partition Sales = calculated\n        source = ROW("A", 1)\n',
    )
    sales = result.parts["definition/tables/Sales.tmdl"]
    assert b"partition Sales = calculated" in sales and b'ROW("A", 1)' in sales
    assert b"#table" not in sales
    assert b"measure Revenue = SUM(Sales[Amount])" in sales
    assert (
        result.parts["definition/tables/Product.tmdl"]
        == base["definition/tables/Product.tmdl"]
    )
