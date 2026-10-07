"""Semantic items share repository discovery, identity and target configuration."""

import shutil
from pathlib import Path

import pytest
from support.semantic_models import policy_path
from support.weaver_test import weaver_test

from weaver.config import parse_workspace
from weaver.declaration.model import (
    WeaverDocumentId,
    WeaverItemId,
    parse_installed_identity,
)
from weaver.declaration.repository import parse_item_repository
from weaver.locations import Location

FIXTURE = Path(__file__).parent / "fixtures" / "semantic_model" / "Probe"


@pytest.mark.parametrize("pbip", [False, True])
@weaver_test()
def test_semantic_repository_retains_sources_effective_model_and_property_origins(
    tmp_path, pbip
):
    root = tmp_path / "project"
    item_path = root / "SemanticModel" / "Reporting"
    item_path.mkdir(parents=True)
    if pbip:
        shutil.copytree(FIXTURE, item_path, dirs_exist_ok=True)
    org = policy_path(root)
    org.write_text(
        "model Model\n\tculture: en-AU\n\tdiscourageImplicitMeasures\n",
        encoding="utf-8",
    )
    addon = item_path / f"{item_path.name}.tmdl"
    addon.write_text(
        'model Model\n\tculture: en-GB\n\ntable _Probe\n\tpartition _Probe = calculated\n\t\tsource = ROW("Value", 1)\n',
        encoding="utf-8",
    )
    before = {
        p.relative_to(root).as_posix(): p.read_bytes()
        for p in root.rglob("*")
        if p.is_file()
    }
    repository = parse_item_repository(Location(root.as_posix()))
    item = WeaverItemId.parse("SemanticModel/Reporting")
    identity = WeaverDocumentId.model_root(item)
    assert str(identity) == str(item)
    assert parse_installed_identity(str(identity)) == identity
    semantic = repository.semantic_models[item]
    assert semantic.requested["culture"] == "en-GB"
    assert semantic.requested["discourageImplicitMeasures"] is True
    assert semantic.sources == before
    assert (
        semantic.provenance["/model/culture"]["source"]
        == "SemanticModel/Reporting/Reporting.tmdl"
    )
    assert (
        semantic.provenance["/model/discourageImplicitMeasures"]["source"]
        == "PowerBI/policy.tmdl"
    )
    assert (
        semantic.provenance["/model/tables/_Probe/partitions/_Probe/source/expression"][
            "reason"
        ]
        == "extension"
    )
    model = repository[str(item)]
    assert not model.schemas and not model.documents and not model.programmables
    assert not [s for s in repository.shortcuts if s.owner == item]
    assert str(identity) in repository.dependency_graph
    assert before == {
        p.relative_to(root).as_posix(): p.read_bytes()
        for p in root.rglob("*")
        if p.is_file()
    }
    original_signature = semantic.signature
    # Local policy masks the organisation's culture change.
    org.write_text(
        "model Model\n\tculture: fr-FR\n\tdiscourageImplicitMeasures\n",
        encoding="utf-8",
    )
    changed = parse_item_repository(Location(root.as_posix()))
    assert changed.semantic_models[item].signature == original_signature
    assert changed.signature != repository.signature


@weaver_test()
def test_semantic_targets_are_typed_through_configuration_and_build_bindings():
    from weaver.build_bundle.targets import parse_build_item
    from weaver.errors import BuildError
    from weaver.fabric.preflight import required_items
    from weaver.targets import PhysicalTargetRef, parse_physical_target, physical_kind

    workspace = parse_workspace(
        {
            "workspace": "Development",
            "catalogue": "Warehouse/Catalogue",
            "targets": {"SemanticModel/Reporting": "Reporting_Dev"},
        }
    )
    item = WeaverItemId.parse("SemanticModel/Reporting")
    target = workspace.target_for(item)
    assert physical_kind(target) == "SemanticModel"
    assert str(PhysicalTargetRef.of(target)) == "SemanticModel/Reporting_Dev"
    assert target == parse_physical_target("SemanticModel/Reporting_Dev")
    binding = parse_build_item(str(item), workspace=workspace)
    assert binding.target.kind == "semanticmodel"
    assert binding.to_bound_target().logical_item_type == "SemanticModel"
    assert (
        required_items(
            type("Bindings", (), {"entries": (binding,)})(), control_item="Catalogue"
        )[-1].item_type
        == "SemanticModel"
    )
    with pytest.raises(BuildError, match="Spark"):
        _ = binding.to_bound_target().spark_target
    with pytest.raises(BuildError, match="both must be"):
        parse_build_item("SemanticModel/Reporting=Warehouse/Reporting")


@weaver_test()
@pytest.mark.parametrize(
    "path, content",
    [
        ("Reporting.tmdl", "ref table Missing\n"),
        ("Reporting.tmdl", "model Model\n    culture: en-US\n    culture: en-AU\n"),
        ("Reporting.tmdl", "model Model\n   culture: en-US\n"),
        ("Tables/Dim__Date.py", "invalid authored table"),
        ("Dim__Date.sql", "select 1"),
        ("tests/RowCount.dax", 'EVALUATE ROW("Count", 1)'),
        ("addon.yml", "model: {}\n"),
    ],
)
def test_unsupported_semantic_sources_fail_with_source_location(
    tmp_path, path, content
):
    from weaver.errors import ConfigError

    folder = tmp_path / "SemanticModel/Reporting"
    folder.mkdir(parents=True)
    (folder / f"{folder.name}.tmdl").write_text("model Model\n", encoding="utf-8")
    authored = folder / path
    authored.parent.mkdir(parents=True, exist_ok=True)
    authored.write_text(content, encoding="utf-8")
    with pytest.raises(ConfigError, match="SemanticModel/Reporting"):
        parse_item_repository(Location(tmp_path.as_posix()))


@weaver_test()
def test_report_and_cache_edits_do_not_change_semantic_source_signature(tmp_path):
    folder = tmp_path / "SemanticModel/Reporting"
    shutil.copytree(FIXTURE, folder)
    before = parse_item_repository(Location(tmp_path.as_posix()))
    report = next(folder.glob("*.Report"))
    (report / "report.json").write_text('{"layout": "changed"}', encoding="utf-8")
    cache = folder / ".pbi"
    cache.mkdir()
    (cache / "cache.abf").write_bytes(b"local cache")
    after = parse_item_repository(Location(tmp_path.as_posix()))
    assert before.signature == after.signature
    assert (
        before.semantic_models[WeaverItemId.parse("SemanticModel/Reporting")].signature
        == after.semantic_models[
            WeaverItemId.parse("SemanticModel/Reporting")
        ].signature
    )
