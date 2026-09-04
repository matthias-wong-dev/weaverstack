"""What `weaver initialise` writes, read back by the parsers that read a project.

A generated project is a reference implementation: it is the first Weaver
repository most users see, and the shape they copy. So every generated file is
parsed here by the same parser a user's own project goes through, and each of
the three shapes is checked for what it would otherwise teach.
"""

from __future__ import annotations

import pytest
from support.weaver_test import weaver_test

from weaver.config import load_workspace
from weaver.errors import CommandError, IdentityError
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


def _project(tmp_path, shape: str, *, example: bool, define_environment: bool = True):
    request = _request(shape, example=example)
    files = _generated_files(request, define_environment=define_environment)
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
    """Parsed by `weaver compose`'s own loader, which lives in the CLI."""

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
    """An authored object implies its schema, so `Sales.yml` carries nothing."""

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
    converge. Nothing generated carries the day it was written."""

    first = _generated_files(_request("both", example=True), define_environment=True)
    second = _generated_files(_request("both", example=True), define_environment=True)

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


# --- names become paths, so they are validated before any path is built --------


@pytest.mark.parametrize(
    "name", ["../../outside", "a/b", "..", "", "  ", "with:colon", "star*"]
)
@pytest.mark.parametrize("field", ["lakehouse", "warehouse", "environment"])
@weaver_test()
def test_a_name_that_is_not_an_item_name_is_refused(tmp_path, field, name):
    """Before generation, so nothing reaches a path built from it.

    A generated project is written under the destination and parsed under a
    temporary root, and the identity rules run where the request is made, before
    either path exists.
    """

    values = {"workspace": WORKSPACE, "catalogue": "Catalogue", "environment": "Weaver"}
    values.setdefault("lakehouse", "Landing")
    values[field] = name

    with pytest.raises(IdentityError):
        ProjectRequest(**values)

    assert list(tmp_path.iterdir()) == []


@weaver_test()
def test_a_path_like_name_writes_nothing_anywhere(tmp_path, monkeypatch):
    """The whole claim: nothing in the destination, and nothing outside it.

    Watched at the one call that writes, which sees every path a run produces
    wherever it points.
    """

    import importlib

    import weaver

    operation = importlib.import_module("weaver.initialise")
    written: list[str] = []
    monkeypatch.setattr(
        operation,
        "_write",
        lambda destination, files: written.extend(
            str(destination / relative) for relative in files
        ),
    )

    with pytest.raises(IdentityError):
        weaver.initialise(tmp_path, workspace=WORKSPACE, lakehouse="../../outside")

    assert written == []
    assert list(tmp_path.iterdir()) == []


@weaver_test()
def test_a_name_is_taken_as_it_is_written_once_the_spaces_are_gone(tmp_path):
    """Fabric display names hold spaces, so only the edges are trimmed."""

    request = ProjectRequest(
        workspace=" Weaver Example ",
        catalogue="Catalogue",
        environment="Weaver",
        lakehouse=" Landing Zone ",
    )

    assert request.workspace == "Weaver Example"
    assert request.lakehouse == "Landing Zone"
