"""Each public annotation is discovered after all native authoring layers."""

import shutil

import pytest
from support.semantic_models import policy_path
from support.weaver_test import weaver_test
from test_semantic_annotation_declaration import ITEM, PBIP, compile_source

from weaver.semantic_models.builtin_annotations import MEASURE_TABLE_SOURCE
from weaver.semantic_models.tmdl import PackageEditor

NAMES = (
    "Weaver.Source",
    "Weaver.MeasureTable",
    "Weaver.Switch",
    "Weaver.AutoHideColumns",
    "Weaver.AutoHideForeignKeys",
    "Weaver.Exclude",
)


def annotated_project(tmp_path, name, origin):
    root = tmp_path / "project"
    folder = root / str(ITEM)
    shutil.copytree(PBIP, folder)
    definition = folder / "Probe.SemanticModel/definition"
    if name in {"Weaver.MeasureTable", "Weaver.Switch"}:
        file = definition / "tables/Metric.tmdl"
        file.write_text(
            "table Metric\n\tmeasure Value = 1\n"
            + (
                "\tannotation Weaver.MeasureTable = true\n"
                if name == "Weaver.Switch"
                else ""
            )
        )
    if name == "Weaver.Source":
        file = definition / "tables/Sales.tmdl"
        addition = "\tannotation Weaver.Source = Warehouse/Serving/Cake.Sales\n"
        overlay = "ref table Sales\n" + addition
    elif name == "Weaver.AutoHideColumns":
        file = definition / "tables/Sales.tmdl"
        addition = '\tannotation Weaver.AutoHideColumns = "Product*"\n'
        overlay = "ref table Sales\n" + addition
    elif name == "Weaver.AutoHideForeignKeys":
        file = definition / "model.tmdl"
        addition = "\tannotation Weaver.AutoHideForeignKeys = true\n"
        overlay = "model Model\n" + addition
    elif name == "Weaver.Exclude":
        file = definition / "tables/Sales.tmdl"
        addition = "\t\tannotation Weaver.Exclude = true\n"
        overlay = "ref table Sales\n\tcolumn Id\n" + addition
    elif name == "Weaver.MeasureTable":
        addition = "\tannotation Weaver.MeasureTable = true\n"
        overlay = "ref table Metric\n" + addition
    else:
        addition = "\t\tannotation Weaver.Switch = Sales[Revenue]\n"
        overlay = "ref table Metric\n\tmeasure Value\n" + addition
    if origin == "pbip":
        text = file.read_text()
        if name == "Weaver.AutoHideForeignKeys":
            text = text.replace(
                "\nref table Sales", "\n" + addition + "\nref table Sales", 1
            )
        elif name == "Weaver.Exclude":
            text = text.replace(
                "\n\tcolumn ProductId", "\n" + addition + "\n\tcolumn ProductId", 1
            )
        elif name == "Weaver.Switch":
            text = text.replace(
                "measure Value = 1\n", "measure Value = 1\n" + addition, 1
            )
        else:
            text += "\n" + addition
        file.write_text(text)
    else:
        (
            policy_path(root) if origin == "organisation" else folder / "Reporting.tmdl"
        ).write_text(overlay)
    return root


@pytest.mark.parametrize("origin", ["pbip", "organisation", "item"])
@pytest.mark.parametrize("name", NAMES)
@weaver_test()
def test_native_annotation_executes_from_each_authoring_layer(tmp_path, name, origin):
    root = annotated_project(tmp_path, name, origin)
    original = {p: p.read_bytes() for p in root.rglob("*") if p.is_file()}
    semantic = compile_source(root)
    if name == "Weaver.Source":
        assert semantic.source_references == {"Sales": "Warehouse/Serving/Cake.Sales"}
    elif name in {"Weaver.AutoHideColumns", "Weaver.AutoHideForeignKeys"}:
        sales = next(t for t in semantic.requested["tables"] if t["name"] == "Sales")
        column = next(c for c in sales["columns"] if c["name"] == "ProductId")
        assert column["isHidden"] is True
    elif name == "Weaver.MeasureTable":
        table = next(t for t in semantic.requested["tables"] if t["name"] == "Metric")
        assert table["partitions"][0]["source"]["expression"] == MEASURE_TABLE_SOURCE
    elif name == "Weaver.Switch":
        table = next(t for t in semantic.requested["tables"] if t["name"] == "Metric")
        value = next(m for m in table["measures"] if m["name"] == "Value")
        assert (
            "SWITCH(" in value["expression"]
            and "SWITCH(" in value["formatStringDefinition"]["expression"]
        )
    else:
        assert not PackageEditor(semantic.parts).locations(
            (("table", "Sales"), ("column", "Id"))
        )
    assert {p: p.read_bytes() for p in original} == original
    assert any(name.encode() in data for data in semantic.parts.values()) is (
        name != "Weaver.Exclude"
    )


@pytest.mark.parametrize("newline", [b"\n", b"\r\n"], ids=["lf", "crlf"])
@pytest.mark.parametrize("quoted", [False, True], ids=["bare", "quoted"])
@weaver_test()
def test_weaver_annotation_namespace_is_quoted_without_rewriting_other_source(
    tmp_path, newline, quoted
):
    root = annotated_project(tmp_path, "Weaver.Source", "pbip")
    folder = root / str(ITEM) / "Probe.SemanticModel/definition"
    source = folder / "tables/Sales.tmdl"
    data = source.read_bytes().replace(b"\r\n", b"\n")
    data += b"\n\tannotation Company.Note = untouched\n"
    if quoted:
        data = data.replace(b"annotation Weaver.Source", b"annotation 'Weaver.Source'")
    before = data.replace(b"\n", newline)
    source.write_bytes(before)
    product = (folder / "tables/Product.tmdl").read_bytes()
    semantic = compile_source(root)
    assert semantic.parts["definition/tables/Sales.tmdl"] == before.replace(
        b"annotation Weaver.Source", b"annotation 'Weaver.Source'"
    )
    assert semantic.parts["definition/tables/Product.tmdl"] == product
    assert source.read_bytes() == before


@weaver_test()
def test_item_extension_overrides_organisation_annotation_before_execution(tmp_path):
    root = annotated_project(tmp_path, "Weaver.AutoHideColumns", "pbip")
    folder = root / str(ITEM)
    (policy_path(folder.parent.parent)).write_text(
        'ref table Sales\n\tannotation Weaver.AutoHideColumns = "Id"\n'
    )
    (folder / f"{folder.name}.tmdl").write_text(
        'ref table Sales\n\tannotation Weaver.AutoHideColumns = "Amount"\n'
    )
    semantic = compile_source(root)
    sales = next(t for t in semantic.requested["tables"] if t["name"] == "Sales")
    assert {c["name"] for c in sales["columns"] if c.get("isHidden")} == {"Amount"}
    assert (
        next(a for a in sales["annotations"] if a["name"] == "Weaver.AutoHideColumns")[
            "value"
        ]
        == '"Amount"'
    )


@weaver_test()
def test_item_false_disables_organisation_foreign_key_policy(tmp_path):
    root = annotated_project(tmp_path, "Weaver.AutoHideForeignKeys", "organisation")
    folder = root / str(ITEM)
    (folder / f"{folder.name}.tmdl").write_text(
        "model Model\n\tannotation Weaver.AutoHideForeignKeys = false\n"
    )
    before = (folder / "Probe.SemanticModel/definition/tables/Sales.tmdl").read_bytes()
    semantic = compile_source(root)
    assert semantic.parts["definition/tables/Sales.tmdl"] == before


@weaver_test()
def test_executed_pbip_annotation_must_survive_observed_readback(tmp_path, monkeypatch):
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

    root = annotated_project(tmp_path, "Weaver.AutoHideColumns", "pbip")
    actual = probe_model()
    sales = next(t for t in actual["model"]["tables"] if t["name"] == "Sales")
    next(c for c in sales["columns"] if c["name"] == "ProductId")["isHidden"] = True
    assert not sales.get("annotations")
    with source_session() as session:
        session.answer_semantic_model(
            "Demo", "Reporting_Dev", SubmittedDefinition(actual)
        )
        answer_catalogue(session, source_catalogue(), read_bindings())
        published = capture_publication(monkeypatch, session)
        result = weaver.build(
            root, items=f"{ITEM}=SemanticModel/Reporting_Dev", session=session
        )
        assert not result.succeeded
        assert any("annotations" in (e.message or "") for e in result.errors)
        assert not published().get(ITEM, {}).get("SemanticModel")
