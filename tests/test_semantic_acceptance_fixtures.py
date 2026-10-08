"""Offline discovery of the inputs used by live semantic acceptance."""

import importlib
from pathlib import Path

import pytest
from support.weaver_test import weaver_test

from weaver.declaration.repository import parse_item_repository
from weaver.locations import Location


@pytest.mark.parametrize("form", ["source-extension", "pbip", "overlays"])
@weaver_test()
def test_annotation_acceptance_inputs_use_named_tmdl(tmp_path, monkeypatch, form):
    monkeypatch.syspath_prepend(str(Path(__file__).parent / "fabric"))
    fixture = importlib.import_module("test_semantic_annotation_public_cycle")
    root = tmp_path / "project"
    folder = root / str(fixture.ITEM)
    fixture._project(folder, form)
    parsed = parse_item_repository(Location(root.as_posix()))
    assert set(parsed.semantic_models) == {fixture.ITEM}
    from weaver.semantic_models import TmdlDefinition

    tables = TmdlDefinition(parsed.semantic_models[fixture.ITEM].parts).model.tables
    assert tables["Metric"].isHidden is True
    assert not list(root.rglob("extension.tmdl"))
