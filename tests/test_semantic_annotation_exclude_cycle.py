"""Exclusion removes addressed native objects and requires observed absence."""

import shutil

import pytest
from support.weaver_test import weaver_test
from test_semantic_annotation_declaration import ITEM, PBIP, compile_source

from weaver.semantic_models.tmdl import PackageEditor


def excluded_project(tmp_path, kind):
    root = tmp_path / "project"
    folder = root / str(ITEM)
    shutil.copytree(PBIP, folder)
    text = (
        "table Obsolete\n\tannotation Weaver.Exclude = true\n\tmeasure Old = 1\n"
        if kind == "table"
        else "ref table Sales\n\tcolumn Id\n\t\tannotation Weaver.Exclude = true\n"
    )
    (folder / "extension.tmdl").write_text(text, encoding="utf-8")
    return root


@pytest.mark.parametrize("kind", ["table", "column"])
@weaver_test()
def test_exclude_removes_target_and_records_absence_expectation(tmp_path, kind):
    root = excluded_project(tmp_path, kind)
    semantic = compile_source(root)
    path = (
        (("table", "Obsolete"),)
        if kind == "table"
        else (("table", "Sales"), ("column", "Id"))
    )
    assert not PackageEditor(semantic.parts).locations(path)
    assert path in semantic.absent
    product = root / str(ITEM) / "Probe.SemanticModel/definition/tables/Product.tmdl"
    assert semantic.parts["definition/tables/Product.tmdl"] == product.read_bytes()
    assert not any(b"Weaver.Exclude" in part for part in semantic.parts.values())


@weaver_test()
def test_excluded_generated_column_is_not_reintroduced_by_source_inference(
    tmp_path, monkeypatch
):
    from support.semantic_models import source_model
    from test_semantic_annotation_declaration import source_project
    from test_semantic_source_build_cycle import (
        SubmittedDefinition,
        answer_catalogue,
        capture_publication,
        read_bindings,
        source_catalogue,
        source_session,
        submitted_parts,
    )

    import weaver

    root = source_project(tmp_path)
    path = root / str(ITEM) / "extension.tmdl"
    path.write_text(
        path.read_text() + "\tcolumn Id\n\t\tannotation Weaver.Exclude = true\n"
    )
    actual = source_model(relations={"Sales": "Sales"})
    table = actual["model"]["tables"][0]
    table["columns"] = [c for c in table["columns"] if c["name"] != "Id"]
    table["annotations"] = [
        {"name": "Weaver.Source", "value": "Warehouse/Serving/Cake.Sales"}
    ]
    with source_session() as session:
        session.answer_semantic_model(
            "Demo", "Reporting_Dev", SubmittedDefinition(actual)
        )
        answer_catalogue(session, source_catalogue(), read_bindings())
        published = capture_publication(monkeypatch, session)
        result = weaver.build(
            root, items=f"{ITEM}=SemanticModel/Reporting_Dev", session=session
        )
        assert result.succeeded, result.errors
        assert not PackageEditor(submitted_parts(session)).locations(
            (("table", "Sales"), ("column", "Id"))
        )
        assert {r["column_name"] for r in published()[ITEM]["SemanticModelColumn"]} == {
            "Label"
        }


@pytest.mark.parametrize("kind", ["table", "column"])
@pytest.mark.parametrize("stale", [False, True])
@weaver_test()
def test_exclusion_is_verified_before_public_catalogue_certification(
    tmp_path, monkeypatch, kind, stale
):
    from support.semantic_models import probe_model
    from test_semantic_source_build_cycle import (
        SubmittedDefinition,
        answer_catalogue,
        capture_publication,
        read_bindings,
        source_catalogue,
        source_session,
    )

    import weaver

    root = excluded_project(tmp_path, kind)
    actual = probe_model()
    if kind == "column" and not stale:
        sales = next(t for t in actual["model"]["tables"] if t["name"] == "Sales")
        sales["columns"] = [c for c in sales["columns"] if c["name"] != "Id"]
    if kind == "table" and stale:
        actual["model"]["tables"].append(
            {"name": "Obsolete", "measures": [{"name": "Old", "expression": "1"}]}
        )
    with source_session() as session:
        session.answer_semantic_model(
            "Demo", "Reporting_Dev", SubmittedDefinition(actual)
        )
        answer_catalogue(session, source_catalogue(), read_bindings())
        published = capture_publication(monkeypatch, session)
        result = weaver.build(
            root, items=f"{ITEM}=SemanticModel/Reporting_Dev", session=session
        )
        if stale:
            assert not result.succeeded
            assert any("excluded" in (e.message or "") for e in result.errors)
            assert not published().get(ITEM, {}).get("SemanticModel")
        else:
            assert result.succeeded, result.errors
            assert len(published()[ITEM]["SemanticModel"]) == 1
