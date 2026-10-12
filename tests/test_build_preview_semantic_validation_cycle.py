"""Semantic validation preview follows catalogue definitions without artefacts."""

import pytest
from support.build_preview_guards import guard_preview
from support.weaver_test import weaver_test
from test_semantic_model_build_cycle import ITEM
from test_semantic_source_build_cycle import (
    answer_catalogue,
    capture_publication,
    read_bindings,
    source_catalogue,
    source_session,
)
from test_semantic_validation_build_cycle import (
    SELECTOR,
    answer_installed,
    with_validations,
)

import weaver


@pytest.mark.parametrize("change", ["new", "changed", "removed"])
@weaver_test()
def test_semantic_validation_preview_reports_logical_definition_decision(
    tmp_path, monkeypatch, change
):
    root = with_validations(tmp_path)
    path = root / f"PowerBI/Commerce/tests/{ITEM.item_name}/Sales.RevenueReconciles.dax"
    with source_session() as session:
        answer_catalogue(session, source_catalogue(), read_bindings())
        if change != "new":
            with monkeypatch.context() as patcher:
                published = capture_publication(patcher, session)
                installed = weaver.build(root, items=SELECTOR, session=session)
                assert installed.succeeded, installed.errors
                rows = published()
            answer_installed(session, rows)
            if change == "changed":
                path.write_text(
                    path.read_text().replace("[Revenue])", "[Revenue] * 1)")
                )
            else:
                path.unlink()
        guard_preview(session, monkeypatch)
        result = weaver.build(root, items=SELECTOR, session=session, dry_run=True)
        mapping = result.preview.to_mapping()
    identity = "SemanticModel/Reporting/Sales.RevenueReconciles"
    obj = next(o for o in mapping["objects"] if o["identity"] == identity)
    assert obj["classification"] == change
    assert not obj["validation_artefacts_selected"]
    assert not obj["selected_for_build"] and not obj["selected_for_drop"]
    when = "remove" if change == "removed" else "publish"
    assert identity in {d["object_id"] for d in mapping["validation_definitions"][when]}
    assert f"Validation definition {when}: {identity}" in result.preview.describe()
    if change == "changed":
        assert any(
            row["object_name"] == "RevenueReconciles"
            for state in mapping["runtime_state_established"]
            for row in state["rows"]
        )
