"""Native semantic text, collections and engine defaults stay distinct."""

from copy import deepcopy

import pytest
from support.weaver_test import weaver_test

from weaver.errors import InstallError
from weaver.semantic_models.deployed import canonical_model, verify_requested


@weaver_test()
@pytest.mark.parametrize("field", ["description", "value", "filterExpression"])
@pytest.mark.parametrize("reverse", [False, True])
def test_documented_text_forms_have_the_same_canonical_value(field, reverse):
    model = {
        "model": {
            "description": "First line\n\tSecond line",
            "annotations": [{"name": "Note", "value": "First line\n\tSecond line"}],
            "roles": [
                {
                    "name": "Reader",
                    "tablePermissions": [
                        {
                            "name": "Sales",
                            "filterExpression": "First line\n\tSecond line",
                        }
                    ],
                }
            ],
        }
    }
    alternate = deepcopy(model)
    owner = {
        "description": alternate["model"],
        "value": alternate["model"]["annotations"][0],
        "filterExpression": alternate["model"]["roles"][0]["tablePermissions"][0],
    }[field]
    owner[field] = ["First line", "\tSecond line"]
    expected, actual = (alternate, model) if reverse else (model, alternate)
    assert canonical_model(expected) == canonical_model(actual)
    verify_requested(expected["model"], actual, owned=("/model",))


@weaver_test()
@pytest.mark.parametrize(
    "wanted,found",
    [
        ([], [{"memberName": "Old"}]),
        ([{"memberName": "A"}], [{"memberName": "A"}, {"memberName": "B"}]),
        ([{"name": "A"}, {"name": "B"}], [{"name": "B"}, {"name": "A"}]),
    ],
)
def test_ordinary_member_lists_are_compared_exactly(wanted, found):
    expected = {"model": {"roles": [{"name": "Reader", "members": wanted}]}}
    actual = {"model": {"roles": [{"name": "Reader", "members": found}]}}
    with pytest.raises(InstallError, match="members"):
        verify_requested(expected["model"], actual, owned=("/model",))


@weaver_test()
@pytest.mark.parametrize("reverse", [False, True])
def test_known_relationship_defaults_allow_engine_omission(reverse):
    relation = {"name": "Sales_Product", "fromTable": "Sales", "toTable": "Product"}
    defaults = {
        "fromCardinality": "many",
        "toCardinality": "one",
        "crossFilteringBehavior": "oneDirection",
        "isActive": True,
    }
    minimal = {"model": {"relationships": [relation]}}
    explicit = {"model": {"relationships": [{**relation, **defaults}]}}
    expected, actual = (explicit, minimal) if reverse else (minimal, explicit)
    verify_requested(expected["model"], actual, owned=("/model",))


@weaver_test()
def test_nondefault_removed_relationship_property_is_refused():
    expected = {"model": {"relationships": [{"name": "Sales_Product"}]}}
    actual = deepcopy(expected)
    actual["model"]["relationships"][0]["isActive"] = False
    with pytest.raises(InstallError, match="isActive"):
        verify_requested(expected["model"], actual, owned=("/model",))


@weaver_test()
def test_import_default_mode_allows_engine_omission():
    verify_requested({"defaultMode": "import"}, {"model": {"culture": "en-AU"}})
    with pytest.raises(InstallError, match="defaultMode"):
        verify_requested({"defaultMode": "directLake"}, {"model": {}})


def sales(**column):
    return {
        "tables": [
            {"name": "Sales", "columns": [{"name": "ProductId", **column}]},
        ]
    }


@weaver_test()
def test_fabric_quoting_and_recasing_of_names_is_not_a_difference():
    requested = {
        **sales(isHidden=True),
        "annotations": [{"name": "Weaver.Source", "value": "Warehouse/Serving"}],
    }
    actual = {
        "model": {
            "tables": [
                {
                    "name": "sales",
                    "columns": [{"name": "'PRODUCTID'", "isHidden": True}],
                }
            ],
            "annotations": [
                {"name": "'Weaver.Source'", "value": "Warehouse/Serving"},
                {"name": "PBI_ProTooling", "value": '["WebModelingEdit"]'},
            ],
        }
    }
    assert verify_requested(requested, actual, owned=("/model",)) == ()


@weaver_test()
def test_an_annotation_fabric_dropped_or_changed_is_reported_not_raised():
    requested = {
        "annotations": [
            {"name": "Note", "value": "Kept"},
            {"name": "Origin", "value": "Project"},
        ]
    }
    actual = {"model": {"annotations": [{"name": "Origin", "value": "Service"}]}}
    assert verify_requested(requested, actual, owned=("/model",)) == (
        "/model/annotations/Note",
        "/model/annotations/Origin/value",
    )


@weaver_test()
@pytest.mark.parametrize(
    "found, outcome",
    [
        ("\r\n\t\tSUM(Sales[Amount])  \r\n\t\t+ 1\r\n", ()),
        ("SUM(Sales[Amount]) + 1", ("/model/tables/Sales/measures/Total/expression",)),
        ("SUM(Sales[Cost])\n+ 1", None),
    ],
    ids=["layout", "whitespace", "value"],
)
def test_expression_layout_is_ignored_and_a_changed_value_fails(found, outcome):
    requested = {
        "tables": [
            {
                "name": "Sales",
                "measures": [
                    {"name": "Total", "expression": "SUM(Sales[Amount])\n+ 1"}
                ],
            }
        ]
    }
    actual = {"model": deepcopy({"tables": requested["tables"]})}
    actual["model"]["tables"][0]["measures"][0]["expression"] = found
    if outcome is None:
        with pytest.raises(InstallError, match="Total/expression"):
            verify_requested(requested, actual)
    else:
        assert verify_requested(requested, actual) == outcome


@weaver_test()
def test_a_requested_hide_fabric_did_not_keep_fails():
    with pytest.raises(InstallError, match="ProductId/isHidden"):
        verify_requested(sales(isHidden=True), {"model": sales()})


@weaver_test()
def test_an_excluded_object_fabric_kept_fails_under_any_quoting():
    actual = {"model": {"tables": [{"name": "'Sales'", "columns": []}]}}
    with pytest.raises(InstallError, match="excluded"):
        verify_requested({}, actual, absent=((("table", "SALES"),),))


@weaver_test()
def test_an_unrequested_object_in_an_owned_model_fails():
    actual = {"model": {"tables": [{"name": "Sales"}, {"name": "Removed"}]}}
    with pytest.raises(InstallError, match="/model/tables"):
        verify_requested({"tables": [{"name": "Sales"}]}, actual, owned=("/model",))


@weaver_test()
@pytest.mark.parametrize(
    "partition,found,accepted",
    [
        (
            {"type": "calculated", "expression": 'ROW("Depth", "0")'},
            "calculatedTableColumn",
            True,
        ),
        ({"type": "m", "expression": "Source"}, "calculatedTableColumn", False),
    ],
    ids=["calculated table", "data table"],
)
def test_a_declared_column_takes_its_tables_native_column_type(
    partition, found, accepted
):
    """TMDL cannot write a column's type; a calculated table's are calculated."""

    requested = {
        "tables": [
            {
                "name": "Depth",
                "columns": [
                    {"name": "Depth", "dataType": "string", "sourceColumn": "[Depth]"}
                ],
                "partitions": [{"name": "Depth", "source": partition}],
            }
        ]
    }
    actual = {"model": deepcopy(requested)}
    actual["model"]["tables"][0]["columns"][0]["type"] = found
    if accepted:
        assert verify_requested(requested, actual, owned=("/model",)) == ()
    else:
        with pytest.raises(InstallError, match="Depth/columns/Depth/type"):
            verify_requested(requested, actual, owned=("/model",))
