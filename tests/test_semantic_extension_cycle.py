"""Native extension discovery joins the ordinary repository and scaffold lifecycle."""

import shutil
from pathlib import Path

import pytest
from support.weaver_test import weaver_test

from weaver.declaration.model import WeaverItemId
from weaver.declaration.repository import parse_item_repository
from weaver.errors import ConfigError
from weaver.locations import Location
from weaver.onboarding.project import ProjectRequest, project_files

ITEM = WeaverItemId.parse("SemanticModel/Reporting")
FIXTURE = Path(__file__).parent / "fixtures/semantic_model/Probe"


@pytest.mark.parametrize("pbip", [False, True])
@weaver_test()
def test_repository_applies_native_organisation_then_item_extensions(tmp_path, pbip):
    folder = tmp_path / str(ITEM)
    folder.mkdir(parents=True)
    if pbip:
        shutil.copytree(FIXTURE, folder, dirs_exist_ok=True)
    (folder.parent / "extension.tmdl").write_text("model Model\n    culture: en-AU\n")
    (folder / "extension.tmdl").write_text(
        "model Model\n    culture: en-GB\n\ntable Helper\n"
        '    partition Helper = calculated\n        source = ROW("Value", 1)\n'
    )
    before = {
        p.relative_to(tmp_path).as_posix(): p.read_bytes()
        for p in tmp_path.rglob("*")
        if p.is_file()
    }
    repository = parse_item_repository(Location(tmp_path.as_posix()))
    semantic = repository.semantic_models[ITEM]
    assert semantic.sources == before
    assert semantic.requested["culture"] == "en-GB"
    assert semantic.provenance["/model/culture"]["source"] == f"{ITEM}/extension.tmdl"
    assert not hasattr(semantic, "model")
    assert "definition/tables/Helper.tmdl" in semantic.parts
    assert semantic.parts["definition/model.tmdl"].count(b"culture:") == 1
    assert {
        p.relative_to(tmp_path).as_posix(): p.read_bytes()
        for p in tmp_path.rglob("*")
        if p.is_file()
    } == before
    assert not list(folder.glob("*.pbip")) if not pbip else True
    (folder.parent / "extension.tmdl").write_text("model Model\n    culture: fr-FR\n")
    changed = parse_item_repository(Location(tmp_path.as_posix()))
    assert changed.semantic_models[ITEM].signature == semantic.signature
    assert changed.signature != repository.signature


@pytest.mark.parametrize("organisation", [False, True])
@weaver_test()
def test_removed_yaml_extension_reports_migration_even_beside_pbip(
    tmp_path, organisation
):
    folder = tmp_path / str(ITEM)
    shutil.copytree(FIXTURE, folder)
    path = (folder.parent if organisation else folder) / "addon.yml"
    path.write_text("model: {}\n")
    with pytest.raises(ConfigError, match=r"addon.yml.*extension.tmdl"):
        parse_item_repository(Location(tmp_path.as_posix()))


@weaver_test()
def test_native_extension_only_is_the_semantic_scaffold():
    files = project_files(
        ProjectRequest(
            workspace="Demo",
            catalogue="Catalogue",
            environment="Runtime",
            semantic_model="Reporting",
        )
    )
    assert f"{ITEM}/extension.tmdl" in files
    assert not any(p.endswith(("addon.yml", ".pbip")) for p in files)
    text = files[f"{ITEM}/extension.tmdl"]
    assert "table Example" in text and "partition Example = calculated" in text
    assert ".dax" not in text and ".source" not in text
