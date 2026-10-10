"""Offline discovery of the inputs used by live semantic acceptance."""

import pytest
from support.semantic_projects import ITEM, annotation_project
from support.weaver_test import weaver_test

from weaver.declaration.repository import parse_item_repository
from weaver.locations import Location


@pytest.mark.parametrize("form", ["source-extension", "pbip", "overlays"])
@weaver_test()
def test_annotation_acceptance_inputs_use_named_tmdl(tmp_path, form):
    root = tmp_path / "project"
    annotation_project(root / str(ITEM), form)
    parsed = parse_item_repository(Location(root.as_posix()))
    assert set(parsed.semantic_models) == {ITEM}
    from weaver.semantic_models import TmdlDefinition

    tables = TmdlDefinition(parsed.semantic_models[ITEM].parts).model.tables
    assert tables["Metric"].isHidden is True
    assert not list(root.rglob("extension.tmdl"))
