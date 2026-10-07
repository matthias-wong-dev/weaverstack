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


@pytest.mark.parametrize("origin", ["pbip", "organisation", "item"])
@weaver_test()
def test_unknown_weaver_annotation_is_a_source_located_error(tmp_path, origin):
    root = source_project(tmp_path, origin=origin, annotation="Weaver.Soruce")
    with pytest.raises(ConfigError, match=r".*tmdl.*Weaver.Soruce.*unknown"):
        compile_source(root)


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
