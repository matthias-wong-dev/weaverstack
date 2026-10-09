"""Weaver annotations are discovered in each native authoring layer."""

import shutil
from pathlib import Path

import pytest
from support.semantic_models import policy_path
from support.weaver_test import weaver_test

from weaver.declaration.model import WeaverItemId
from weaver.declaration.repository import parse_item_repository
from weaver.errors import ConfigError
from weaver.locations import Location

ITEM = WeaverItemId.parse("SemanticModel/Reporting")
PBIP = Path(__file__).parent / "fixtures" / "semantic_model" / "Probe"


def source_project(
    tmp_path,
    *,
    origin="item",
    annotation="Weaver.Source",
    value="Warehouse/Serving/Cake.Sales",
):
    root = tmp_path / "project"
    folder = root / str(ITEM)
    folder.mkdir(parents=True)
    if origin in {"pbip", "organisation"}:
        shutil.copytree(PBIP, folder, dirs_exist_ok=True)
    if origin == "pbip":
        path = folder / "Probe.SemanticModel/definition/tables/Sales.tmdl"
        path.write_bytes(
            path.read_bytes() + f"\n\tannotation {annotation} = {value}\n".encode()
        )
    else:
        path = (
            policy_path(root) if origin == "organisation" else folder / "Reporting.tmdl"
        )
        path.write_text(
            f"table Sales\n\tannotation {annotation} = {value}\n", encoding="utf-8"
        )
    return root


def compile_source(root):
    from weaver.semantic_models.annotation import apply_annotations

    contribution = parse_item_repository(Location(root.as_posix())).semantic_models[
        ITEM
    ]
    return apply_annotations(contribution)


@pytest.mark.parametrize("origin", ["pbip", "organisation", "item"])
@weaver_test()
def test_source_annotation_declares_the_managed_relation_in_each_layer(
    tmp_path, origin
):
    root = source_project(tmp_path, origin=origin)
    semantic = parse_item_repository(Location(root.as_posix())).semantic_models[ITEM]
    assert semantic.source_references == {"Sales": "Warehouse/Serving/Cake.Sales"}
    assert any(
        b"annotation Weaver.Source = Warehouse/Serving/Cake.Sales" in value
        for value in semantic.parts.values()
    )
    assert ("Sales", "Warehouse/Serving/Cake.Sales") in semantic.dependencies


AUTHORED = {
    "pbip": f"{ITEM}/Probe.SemanticModel/definition/tables/Sales.tmdl",
    "organisation": "PowerBI/policy.tmdl",
    "item": f"{ITEM}/{ITEM.item_name}.tmdl",
}


@pytest.mark.parametrize("origin", ["pbip", "organisation", "item"])
@pytest.mark.parametrize(
    "annotation, value, diagnostic",
    [
        ("Weaver.Soruce", "true", "unknown Weaver annotation"),
        ("Weaver.Exclude", "maybe", "requires a boolean true or false"),
    ],
)
@weaver_test()
def test_annotation_errors_cite_the_authored_file_and_line(
    tmp_path, origin, annotation, value, diagnostic
):
    import re

    root = source_project(tmp_path, origin=origin, annotation=annotation, value=value)
    authored = AUTHORED[origin]
    lines = (root / authored).read_text().splitlines()
    line = next(i for i, text in enumerate(lines, 1) if annotation in text)
    with pytest.raises(
        ConfigError,
        match="^" + re.escape(f"{authored}:{line}: {annotation}: {diagnostic}") + "$",
    ):
        compile_source(root)


@weaver_test()
def test_an_unlocated_declaration_names_its_decoded_package_path():
    from weaver.semantic_models.annotation import declared_at
    from weaver.semantic_models.tmdl import Document

    document = Document(
        "definition/tables/Notice%20SQL.tmdl",
        b"table 'Notice SQL'\n\tannotation Weaver.Exclude = maybe\n",
    )
    node = document.spans[1]
    assert declared_at({}, {}, document, node) == "definition/tables/Notice SQL.tmdl:2"


@pytest.mark.parametrize("origin", ["pbip", "organisation", "item"])
@weaver_test()
def test_ordinary_annotations_remain_native(tmp_path, origin):
    root = source_project(tmp_path, origin=origin, annotation="Company.Source")
    semantic = compile_source(root)
    assert semantic.source_references == {}
    assert any(b"annotation Company.Source" in data for data in semantic.parts.values())


@pytest.mark.parametrize("scope", ["model", "column", "measure", "relationship"])
@weaver_test()
def test_source_annotation_rejects_the_wrong_scope(tmp_path, scope):
    root = source_project(tmp_path)
    parents = {
        "model": "model Model\n\t",
        "column": "table Sales\n\tcolumn Id\n\t\t",
        "measure": "table Sales\n\tmeasure Value = 1\n\t\t",
        "relationship": "relationship Sales_Product\n\t",
    }
    (root / str(ITEM) / f"{ITEM.item_name}.tmdl").write_text(
        parents[scope] + "annotation Weaver.Source = Warehouse/Serving/Cake.Sales\n"
    )
    with pytest.raises(ConfigError, match=r".*tmdl.*Weaver.Source.*table"):
        compile_source(root)


@pytest.mark.parametrize(
    "reference",
    [
        "Lakehouse/Curated/CRM.Customer",
        "Lakehouse/Curated/Files/CRM.Customer",
        "Warehouse/Serving/Tables/Cake.Sales",
        "Warehouse/Serving/Sales",
        "Serving/Cake.Sales",
        '"Warehouse/ Serving/Cake.Sales"',
        "true",
    ],
)
@weaver_test()
def test_source_annotation_requires_the_canonical_external_identity(
    tmp_path, reference
):
    root = source_project(tmp_path, value=reference)
    with pytest.raises(ConfigError, match=r".*tmdl.*Weaver.Source"):
        compile_source(root)


@weaver_test()
def test_source_annotation_keeps_the_lakehouse_tables_area(tmp_path):
    root = source_project(tmp_path, value="Lakehouse/Curated/Tables/CRM.Customer")
    semantic = parse_item_repository(Location(root.as_posix())).semantic_models[ITEM]
    assert semantic.source_references == {
        "Sales": "Lakehouse/Curated/Tables/CRM.Customer"
    }
