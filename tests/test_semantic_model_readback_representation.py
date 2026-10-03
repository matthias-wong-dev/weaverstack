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
