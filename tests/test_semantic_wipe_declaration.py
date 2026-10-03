"""Semantic-only wipe preserves the catalogue and freezes source preservation."""

import pytest
from support.weaver_test import weaver_test

from weaver import plan_wipe, wipe
from weaver.errors import CommandError
from weaver.sessions.testing import TestSession


@weaver_test()
def test_semantic_only_selection_unbinds_its_claims_without_wiping_the_catalogue():
    plan = plan_wipe(
        "SemanticModel/Reporting",
        workspace="Analytics",
        catalogue="Warehouse/Weaver",
        session=TestSession(),
    )
    assert [str(target) for target in plan.targets] == ["SemanticModel/Reporting"]
    assert plan.catalogue_action == "unbind"
    assert plan.unbound == ("SemanticModel/Reporting",)
    assert not plan.empties_the_catalogue


@pytest.mark.parametrize("override", [True, False])
@weaver_test()
def test_source_preservation_is_in_the_frozen_plan_and_confirmation(override):
    plan = plan_wipe(
        "SemanticModel/Reporting",
        workspace="Analytics",
        preserve_data_source=True,
        session=TestSession(),
    )
    assert plan.preserve_data_source is True
    assert plan.to_mapping()["preserve_data_source"] is True
    assert "hidden columnless source table" in plan.describe()
    with pytest.raises(CommandError, match="settled plan"):
        wipe(plan=plan, preserve_data_source=override, session=TestSession())


@weaver_test()
def test_semantic_only_wipe_cannot_append_a_destructive_catalogue_target():
    with pytest.raises(CommandError, match="preserves the catalogue"):
        plan_wipe(
            "SemanticModel/Reporting",
            workspace="Analytics",
            catalogue="Warehouse/Weaver",
            catalogue_action="remove",
            session=TestSession(),
        )


@weaver_test()
def test_preserve_data_source_requires_a_semantic_target():
    with pytest.raises(CommandError, match="SemanticModel"):
        plan_wipe(
            "Warehouse/Reporting",
            workspace="Analytics",
            preserve_data_source=True,
            session=TestSession(),
        )


@weaver_test()
def test_cli_preservation_flag_reaches_the_same_confirmed_plan(monkeypatch, capsys):
    from test_cli_wipe_representation import _wired

    from weaver_cli import main

    plan = plan_wipe(
        "SemanticModel/Reporting",
        workspace="Analytics",
        preserve_data_source=True,
        session=TestSession(),
    )
    _, planned, executed = _wired(monkeypatch, plan)
    assert (
        main(["wipe", "SemanticModel/Reporting", "--preserve-data-source", "--yes"])
        == 0
    )
    assert planned[0][1]["preserve_data_source"] is True
    assert executed[0]["plan"] is plan
    assert "hidden columnless source table" in capsys.readouterr().out


@weaver_test()
def test_semantic_unbinding_uses_typed_target_and_removes_every_semantic_claim():
    from support.semantic_wipe import catalogue_answers

    from weaver.catalogue.connection import CatalogueConnection
    from weaver.unbind import plan_unbind

    answers = catalogue_answers(
        [
            {
                "item_type": "SemanticModel",
                "item_name": "LogicalModel",
                "target_name": "Reporting",
            },
            {
                "item_type": "Warehouse",
                "item_name": "Source",
                "target_name": "Reporting",
            },
            {
                "item_type": "SemanticModel",
                "item_name": "OtherModel",
                "target_name": "Other",
            },
        ]
    )
    result = plan_unbind(
        CatalogueConnection(lambda statement: answers[statement]),
        semantic_models=("Reporting",),
    )
    assert result.targets == ("SemanticModel/Reporting",)
    assert result.logical_items == ("SemanticModel/LogicalModel",)
    joined = "\n".join(result.statements)
    for table in (
        "SemanticModel",
        "SemanticModelTable",
        "SemanticModelColumn",
        "SemanticModelMeasure",
        "SemanticModelRelationship",
        "Dependency",
        "Registry",
        "LoadStatus",
    ):
        assert f"[_].[{table}]" in joined
    assert all(
        "'SemanticModel'" in statement and "'LogicalModel'" in statement
        for statement in result.statements
    )
    assert "OtherModel" not in joined and "'Source'" not in joined
