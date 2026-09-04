"""What `weaver initialise` writes, read back by the parsers that read a project.

A generated project is a reference implementation: it is the first Weaver
repository most users see, and the shape they copy. So every generated file is
parsed here by the reader a user's own project goes through, and the three
topologies are each checked for the things that would teach the wrong thing.
"""

from __future__ import annotations

import pytest
from support.weaver_test import weaver_test

from weaver.config import load_workspace
from weaver.errors import CommandError
from weaver.fabric.environment_definition import (
    environment_name_from_path,
    read_environment_definition,
)
from weaver.initialise import _generated_files, _write
from weaver.onboarding import ProjectRequest
from weaver.onboarding.environment import environment_directory
from weaver.operations.check import check

WORKSPACE = "Weaver Example"

SHAPES = {
    "lakehouse": {"lakehouse": "Landing"},
    "warehouse": {"warehouse": "Curated"},
    "both": {"lakehouse": "Landing", "warehouse": "Curated"},
}


def _request(shape: str, *, example: bool) -> ProjectRequest:
    return ProjectRequest(
        workspace=WORKSPACE,
        catalogue="Catalogue",
        environment="Weaver",
        example=example,
        **SHAPES[shape],
    )


def _project(tmp_path, shape: str, *, example: bool):
    request = _request(shape, example=example)
    files = _generated_files(request)
    _write(tmp_path, files)
    return request, files


@pytest.mark.parametrize("shape", sorted(SHAPES))
@pytest.mark.parametrize("example", [False, True])
@weaver_test()
def test_a_generated_project_parses(tmp_path, shape, example):
    """The repository reader is the one a user's own project goes through."""

    _project(tmp_path, shape, example=example)

    check(tmp_path)


@pytest.mark.parametrize("shape", sorted(SHAPES))
@weaver_test()
def test_the_generated_configuration_reads_as_one_workspace(tmp_path, shape):
    request, _ = _project(tmp_path, shape, example=True)

    configured = load_workspace(tmp_path / "workspace-config.yml")

    assert configured.workspace == WORKSPACE
    assert configured.catalogue == "Warehouse/Catalogue"
    assert str(configured.environment) == "Weaver"
    assert {str(item) for item in configured.targets} == set(request.items)


@pytest.mark.parametrize("shape", sorted(SHAPES))
@weaver_test()
def test_each_target_is_bound_to_the_item_the_user_named(tmp_path, shape):
    request, _ = _project(tmp_path, shape, example=False)

    configured = load_workspace(tmp_path / "workspace-config.yml")

    bound = {str(item): target.physical for item, target in configured.targets.items()}
    for item in request.items:
        assert bound[item] == item.split("/", 1)[1]


@weaver_test()
def test_the_environment_definition_reads_and_names_itself(tmp_path):
    _project(tmp_path, "both", example=False)

    directory = tmp_path / environment_directory("Weaver")

    assert environment_name_from_path(directory) == "Weaver"
    definition = read_environment_definition(directory)
    assert ".platform" in definition.parts
    assert "dependencies" in definition.external_libraries()


@weaver_test()
def test_the_composition_runs_build_load_and_test(tmp_path):
    """Read with the reader `weaver compose` uses, which lives in the CLI."""

    from weaver_cli.compose import load_composition

    _project(tmp_path, "both", example=True)

    entries, _path = load_composition("full", file=str(tmp_path / "compose.yml"))

    assert entries == ["build", "load", "test"]


@weaver_test()
def test_no_authored_folder_is_written_for_the_catalogue(tmp_path):
    """The catalogue Warehouse holds Weaver's `_` schema and nothing authored.

    Writing `Warehouse/Catalogue/` would invite a user to author into the item
    Weaver keeps its own tables in.
    """

    _, files = _project(tmp_path, "both", example=True)

    assert not any(path.startswith("Warehouse/Catalogue/") for path in files)
    assert not (tmp_path / "Warehouse" / "Catalogue").exists()


@weaver_test()
def test_lakehouse_tables_are_written_under_the_tables_area(tmp_path):
    _, files = _project(tmp_path, "both", example=True)

    tables = [path for path in files if path.endswith("Sales__Customer.py")]

    assert tables == ["Lakehouse/Landing/Tables/Sales__Customer.py"]


@weaver_test()
def test_no_schema_document_is_written(tmp_path):
    """An authored object implies its schema, so `Sales.yml` would be noise."""

    _, files = _project(tmp_path, "both", example=True)

    assert not [path for path in files if "/schemas/" in path]


@weaver_test()
def test_the_warehouse_reads_the_lakehouse_through_a_shortcut(tmp_path):
    """The both-item example is what teaches the cross-item relationship."""

    _, files = _project(tmp_path, "both", example=True)

    shortcuts = files["Warehouse/Curated/shortcuts.yml"]

    assert (
        "Warehouse/Curated/Sales.Customer: Lakehouse/Landing/Tables/Sales.Customer"
        in shortcuts
    )
    assert "Warehouse/Curated/Sales.Customer.sql" not in files


@weaver_test()
def test_a_warehouse_on_its_own_seeds_its_own_customers(tmp_path):
    """With no Lakehouse there is nothing to shortcut to, so the table is authored."""

    _, files = _project(tmp_path, "warehouse", example=True)

    assert "Warehouse/Curated/Sales.Customer.sql" in files
    assert "Warehouse/Curated/shortcuts.yml" not in files


@weaver_test()
def test_generation_is_deterministic(tmp_path):
    """The same request twice writes the same bytes, which is what lets a rerun
    converge instead of reporting every file as changed."""

    first = _generated_files(_request("both", example=True))
    second = _generated_files(_request("both", example=True))

    assert first == second


@weaver_test()
def test_a_project_with_neither_item_says_so():
    with pytest.raises(CommandError, match="Lakehouse, a Warehouse, or both"):
        ProjectRequest(workspace=WORKSPACE, catalogue="Catalogue", environment="Weaver")


@weaver_test()
def test_an_empty_item_keeps_its_folders(tmp_path):
    """Without the example the item folders are empty, and still committed."""

    _project(tmp_path, "both", example=False)

    assert (tmp_path / "Lakehouse" / "Landing" / "Tables" / ".gitkeep").is_file()
    assert (tmp_path / "Lakehouse" / "Landing" / "Files" / ".gitkeep").is_file()
    assert (tmp_path / "Warehouse" / "Curated" / ".gitkeep").is_file()
