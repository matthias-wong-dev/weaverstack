"""The stress example generates projects Weaver accepts, the same way each time.

The estate only runs in Fabric, so nothing else would notice a generator that
drifted from what a project may declare.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

from support.weaver_test import weaver_test

from weaver.operations.check import check

GENERATOR = Path(__file__).resolve().parents[1] / "examples" / "stress" / "generate.py"

NAMES = {
    "workspace": "Stress",
    "source_workspace": "Stress Sources",
    "environment": "weaver",
    "lakehouse": "Lake",
    "core": "Core",
    "mart": "Mart",
    "catalogue": "Catalogue",
    "source_lakehouse": "Source",
    "source_warehouse": "SourceWarehouse",
    "source_catalogue": "SourceCatalogue",
}


def _generator():
    spec = importlib.util.spec_from_file_location("stress_generate", GENERATOR)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _objects(project: Path) -> list[Path]:
    """Every authored object: what a build materialises or installs as a view."""

    return [
        path
        for path in project.rglob("*")
        if path.suffix in (".py", ".sql")
        and not {"lib", "tests", "schemas"} & set(path.relative_to(project).parts)
        and path.name != "shortcuts.py"
    ]


@weaver_test()
def test_the_generated_projects_pass_check(tmp_path):
    generator = _generator()
    plan = generator.generate(tmp_path, NAMES, generator.Options(objects=300, scale=0.001))

    check(tmp_path / "source")
    check(tmp_path / "estate")

    assert plan["estate"]["objects"] == len(_objects(tmp_path / "estate")) == 300
    assert plan["source"]["objects"] == len(_objects(tmp_path / "source"))


@weaver_test()
def test_the_same_arguments_generate_the_same_files(tmp_path):
    generator = _generator()
    for run in ("first", "second"):
        generator.generate(tmp_path / run, NAMES, generator.Options(objects=300))

    first = {
        path.relative_to(tmp_path / "first"): path.read_bytes()
        for path in (tmp_path / "first").rglob("*")
        if path.is_file()
    }
    second = {
        path.relative_to(tmp_path / "second"): path.read_bytes()
        for path in (tmp_path / "second").rglob("*")
        if path.is_file()
    }
    assert first == second


@weaver_test()
def test_no_large_table_is_loaded_whole():
    generator = _generator()
    options = generator.Options()
    nodes = generator.plan_estate(generator.plan_sources(options), options)

    whole = [
        node.id
        for node in nodes
        if node.table
        and node.rows > generator.LARGE
        and node.behaviour
        not in (generator.APPEND, generator.INCREMENTAL, generator.INCREMENTAL_DELETE)
    ]
    assert not whole
    assert sum(1 for node in nodes if node.table and node.rows > generator.LARGE)


@weaver_test()
def test_no_incremental_table_reads_a_parent_that_removes_rows():
    """A removed row leaves no change behind for an incremental reader to see."""

    generator = _generator()
    options = generator.Options()
    nodes = generator.plan_estate(generator.plan_sources(options), options)
    incremental = (generator.APPEND, generator.INCREMENTAL, generator.INCREMENTAL_DELETE)

    blind = [
        node.id
        for node in nodes
        if node.parent is not None
        and node.behaviour in incremental
        and node.parent.removes
    ]
    assert not blind
