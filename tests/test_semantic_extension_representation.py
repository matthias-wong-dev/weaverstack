"""Native partial TMDL edits retain unrelated definition bytes."""

from pathlib import Path

import pytest
from support.weaver_test import weaver_test

from weaver.errors import ConfigError

FIXTURE = Path(__file__).parent / "fixtures/semantic_model/Probe/Probe.SemanticModel"
ORG = b"""model Model
    discourageImplicitMeasures

ref table Sales
    isHidden
    column ProductId
        isHidden: false
    measure 'Double Revenue' = [Revenue] * 2
        formatString: 0.00

table Helper
    partition Helper = calculated
        source = ROW("Value", 1)

expression DataSource1 =
        let
            Value = 1
        in
            Value
    kind: m

perspective Reporting
    perspectiveTable Sales
        perspectiveMeasure Revenue
"""
ITEM = b"""model Model
    discourageImplicitMeasures: false

/// Reporting sales.
ref table Sales
    isHidden: false
    column ProductId
        isHidden

expression DataSource1 = 2
    kind: m
"""


def base_parts():
    parts = {
        p.relative_to(FIXTURE).as_posix(): p.read_bytes()
        for p in FIXTURE.rglob("*")
        if p.is_file()
    }
    parts["definition/expressions.tmdl"] = b"expression DataSource1 = 0\n\tkind: m\n"
    return parts


def merge(parts=None, *fragments):
    from weaver.semantic_models.extensions import merge_extensions

    return merge_extensions(
        base_parts() if parts is None else parts,
        fragments
        or ((ORG, "SemanticModel/extension.tmdl"), (ITEM, "Sales/extension.tmdl")),
    )


@pytest.mark.parametrize("newline", [b"\n", b"\r\n"], ids=["lf", "crlf"])
@pytest.mark.parametrize(
    "claim",
    [
        "model-property",
        "table-property",
        "column-property",
        "new-measure",
        "new-table",
        "expression-replacement",
        "local-precedence",
        "opaque-native-object",
        "unrelated-bytes",
    ],
)
@weaver_test()
def test_native_extension_probe(claim, newline):
    before = {
        path: value.replace(b"\r\n", b"\n").replace(b"\n", newline)
        if path.endswith(".tmdl")
        else value
        for path, value in base_parts().items()
    }
    original = dict(before)
    result = merge(before)
    after = result.parts
    sales = after["definition/tables/Sales.tmdl"]
    if claim == "model-property":
        # Fabric refuses a plain property after a block property.
        assert (
            b"\tdiscourageImplicitMeasures: false" + newline + b"\tdataAccessOptions"
            in after["definition/model.tmdl"]
        )
        assert result.requested["discourageImplicitMeasures"] is False
    elif claim == "table-property":
        assert b"/// Reporting sales." in sales
        assert b"\tisHidden: false" in sales
    elif claim == "column-property":
        assert (
            b"\t\tsummarizeBy: none" + newline + b"\t\tisHidden" + newline + newline
            in sales
        )
        assert result.requested["tables"][0]["columns"][0]["isHidden"] is True
    elif claim == "new-measure":
        assert b"measure 'Double Revenue' = [Revenue] * 2" in sales
        assert b"formatString: 0.00" in sales
    elif claim == "new-table":
        helper = after["definition/tables/Helper.tmdl"]
        assert b"table Helper" in helper and b'ROW("Value", 1)' in helper
        assert "/model/tables/Helper" in result.owned
    elif claim == "expression-replacement":
        assert b"expression DataSource1 = 2" in after["definition/expressions.tmdl"]
        assert b"Value = 1" not in after["definition/expressions.tmdl"]
        assert result.requested["expressions"] == [
            {"name": "DataSource1", "expression": "2", "kind": "m"}
        ]
    elif claim == "local-precedence":
        org = merge(before, (ORG, "organisation"))
        assert org.parts != after
        assert (
            result.provenance["/model/discourageImplicitMeasures"]["source"]
            == "Sales/extension.tmdl"
        )
        assert (
            result.provenance["/model/tables/Sales/isHidden"]["source"]
            == "Sales/extension.tmdl"
        )
        assert merge(before).parts == after
    elif claim == "opaque-native-object":
        assert after["definition/perspectives/Reporting.tmdl"] == (
            b"perspective Reporting\n\tperspectiveTable Sales\n\t\tperspectiveMeasure Revenue\n"
        )
        assert "perspectives" not in result.requested
    else:
        touched = {
            "definition/model.tmdl",
            "definition/tables/Sales.tmdl",
            "definition/expressions.tmdl",
        }
        assert {p: after[p] for p in before if p not in touched} == {
            p: v for p, v in before.items() if p not in touched
        }
        assert before == original


@pytest.mark.parametrize("reference", [True, False])
@weaver_test()
def test_extension_can_merge_an_existing_named_declaration(reference):
    fragment = ("ref " if reference else "") + "table Sales\n    isHidden\n"
    result = merge(None, (fragment.encode(), "extension.tmdl"))
    assert result.parts["definition/tables/Sales.tmdl"].count(b"table Sales") == 1
    assert result.requested["tables"] == [{"name": "Sales", "isHidden": True}]


@weaver_test()
def test_missing_ref_does_not_create_an_object():
    with pytest.raises(ConfigError, match=r"extension.tmdl:1.*Missing.*not found"):
        merge(None, (b"ref table Missing\n    isHidden\n", "extension.tmdl"))


@weaver_test()
def test_ambiguous_target_is_a_source_located_error():
    parts = base_parts()
    parts["definition/tables/duplicate.tmdl"] = b"table Sales\n    isHidden\n"
    with pytest.raises(ConfigError, match=r"extension.tmdl:1.*Sales.*ambiguous"):
        merge(parts, (b"ref table Sales\n    isHidden\n", "extension.tmdl"))


@pytest.mark.parametrize("reference", [True, False])
@weaver_test()
def test_existing_unknown_object_merges_recursively(reference):
    parts = base_parts()
    parts["definition/newThings.tmdl"] = (
        b"newThing Foo\n\tfutureProperty: one\n\tkept: yes\n"
        b"\tnestedThing Bar\n\t\tdeeper: one\n"
    )
    fragment = (b"ref " if reference else b"") + (
        b"newThing Foo\n    futureProperty: two\n    added: three\n"
        b"    nestedThing Bar\n        deeper: two\n"
        b"    nestedThing Baz\n        fresh: true\n"
    )
    result = merge(parts, (fragment, "extension.tmdl"))
    assert result.parts["definition/newThings.tmdl"] == (
        b"newThing Foo\n\tfutureProperty: two\n\tkept: yes\n\tadded: three\n"
        b"\tnestedThing Bar\n\t\tdeeper: two\n"
        b"\n\tnestedThing Baz\n\t\tfresh: true\n"
    )
    assert {
        p: v for p, v in result.parts.items() if p != "definition/newThings.tmdl"
    } == {p: v for p, v in parts.items() if p != "definition/newThings.tmdl"}
    assert result.requested == {}


@weaver_test()
def test_unknown_children_merge_below_a_known_parent():
    parts = base_parts()
    sales = parts["definition/tables/Sales.tmdl"]
    parts["definition/tables/Sales.tmdl"] = (
        sales + b"\n\tfutureChild Kept\n\t\tsetting: one\n"
    )
    result = merge(
        parts,
        (
            b"ref table Sales\n    futureChild Kept\n        setting: two\n"
            b"    column ProductId\n        futureColumnProperty: x\n",
            "extension.tmdl",
        ),
    )
    merged = result.parts["definition/tables/Sales.tmdl"]
    assert merged.endswith(b"\tfutureChild Kept\n\t\tsetting: two\n")
    assert b"\t\tsummarizeBy: none\n\t\tfutureColumnProperty: x\n" in merged
    assert (
        merged.replace(b"setting: two", b"setting: one").replace(
            b"\t\tfutureColumnProperty: x\n", b""
        )
        == parts["definition/tables/Sales.tmdl"]
    )


@weaver_test()
def test_existing_perspective_merges_without_reconstruction():
    parts = base_parts()
    parts["definition/perspectives/Reporting.tmdl"] = b"perspective Reporting\n"
    result = merge(
        parts,
        (b"perspective Reporting\n    perspectiveTable Sales\n", "extension.tmdl"),
    )
    assert result.parts["definition/perspectives/Reporting.tmdl"] == (
        b"perspective Reporting\n\n\tperspectiveTable Sales\n"
    )


@pytest.mark.parametrize(
    "fragment,diagnostic",
    [
        (b"nonsense\n", "object declaration"),
        (b"table 'Unclosed\n", "declaration"),
        (b"    table Sales\n", "indentation"),
        (b"table Sales\n   isHidden\n", "indentation"),
        (b"table Sales\n    measure X = ```\n        unterminated\n", "fence"),
        (b"table Sales\n    isHidden: true\n    isHidden: false\n", "duplicate"),
    ],
)
@weaver_test()
def test_malformed_extension_names_its_source(fragment, diagnostic):
    with pytest.raises(ConfigError, match=rf"broken.tmdl:\d+.*{diagnostic}"):
        merge(None, (fragment, "broken.tmdl"))


@weaver_test()
def test_property_edit_keeps_crlf_fenced_text_and_unknown_neighbours():
    parts = base_parts()
    original = (
        b"table Sales\r\n\tcolumn ProductId\r\n\t\tdataType: int64\r\n"
        b"\t\tunknownProperty: retained\r\n"
        b"\tmeasure Text = ```\r\n"
        b"ref table Sales\r\n  column ProductId  \r\n"
        b"\t\t```\r\n\t\tformatString: 0\r\n"
    )
    parts["definition/tables/Sales.tmdl"] = original
    result = merge(
        parts,
        (
            b"ref table Sales\n    column ProductId\n        isHidden\n",
            "extension.tmdl",
        ),
    )
    assert result.parts["definition/tables/Sales.tmdl"] == original.replace(
        b"\t\tunknownProperty: retained\r\n",
        b"\t\tunknownProperty: retained\r\n\t\tisHidden\r\n",
        1,
    )


@weaver_test()
def test_default_expression_edit_preserves_other_measure_properties():
    parts = base_parts()
    original = parts["definition/tables/Sales.tmdl"]
    fragment = b"ref table Sales\n    measure Revenue = SUMX(Sales, Sales[Amount])\n"
    result = merge(parts, (fragment, "extension.tmdl"))
    assert result.parts["definition/tables/Sales.tmdl"] == original.replace(
        b"measure Revenue = SUM(Sales[Amount])",
        b"measure Revenue = SUMX(Sales, Sales[Amount])",
    )
    assert result.requested["tables"] == [
        {
            "name": "Sales",
            "measures": [
                {"name": "Revenue", "expression": "SUMX(Sales, Sales[Amount])"}
            ],
        }
    ]


@weaver_test()
def test_empty_extension_keeps_every_byte():
    parts = base_parts()
    result = merge(parts, (b"// no changes\n", "extension.tmdl"))
    assert result.parts == parts
    assert result.requested == {} and result.provenance == {} and result.owned == ()


@pytest.mark.parametrize("unit", ["\t", "    "], ids=["tabs", "spaces"])
@weaver_test()
def test_merged_expression_keeps_its_authored_text(unit):
    from weaver.semantic_models.fragments import expression_text
    from weaver.semantic_models.tmdl import PackageEditor

    parts = base_parts()
    parts["definition/expressions.tmdl"] = (
        f"expression Other = 1\n{unit}kind: m\n".encode()
    )
    fragment = (
        b"expression DataSource1 =\n        let\n            Value = 1\n"
        b"        in\n            Value\n    kind: m\n"
    )
    result = merge(parts, (fragment, "extension.tmdl"))
    ((document, node),) = PackageEditor(result.parts).locations(
        (("expression", "DataSource1"),)
    )
    assert expression_text(document, node) == "let\n    Value = 1\nin\n    Value"
    assert result.requested["expressions"][0]["expression"] == expression_text(
        document, node
    )
