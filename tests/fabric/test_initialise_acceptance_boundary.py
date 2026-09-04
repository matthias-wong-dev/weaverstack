"""Setting a project up against a real workspace, where every item is already there.

The suite creates and deletes no Fabric item, so what is proven here is the half
of initialise that a real workspace answers: the listing that decides create
against reuse, and a rerun reaching the same place. Creating an item is proven in
the fast suite, around the same primitives.

The Warehouse journey then takes a generated project the whole way. Warehouse
objects are T-SQL and the catalogue they register in is a Warehouse, so it starts
no Spark session and costs a handful of TDS round trips.
"""

from __future__ import annotations

import pytest
from support.weaver_test import register_session, weaver_test

import weaver
from weaver.config import load_workspace
from weaver.initialise import CREATED, EXISTING, WRITTEN
from weaver.sessions import ConsoleSession


def _status(report, role: str) -> str:
    return next(outcome.status for outcome in report.resources if outcome.role == role)


@pytest.fixture
def estate(fabric_workspace, fabric_catalogue, fabric_target_lakehouse):
    """The fixed items this tenant already holds, named as initialise takes them."""

    return {
        "workspace": fabric_workspace.workspace,
        "catalogue": fabric_catalogue.name,
        "lakehouse": fabric_target_lakehouse.name,
    }


@weaver_test(remote=True, resources={"rest"})
def test_a_dry_run_reports_the_items_the_workspace_already_holds(estate, tmp_path):
    """The read that decides create against reuse, asked of real Fabric."""

    report = weaver.initialise(
        tmp_path,
        workspace=estate["workspace"],
        catalogue=estate["catalogue"],
        lakehouse=estate["lakehouse"],
        dry_run=True,
    )

    assert _status(report, "Catalogue") == EXISTING
    assert _status(report, "Lakehouse") == EXISTING
    assert list(tmp_path.iterdir()) == []


@weaver_test(remote=True, resources={"rest"})
def test_a_project_is_written_against_items_that_are_reused(estate, tmp_path):
    """Nothing is created, and the project binds to the items that were found."""

    report = weaver.initialise(
        tmp_path,
        workspace=estate["workspace"],
        catalogue=estate["catalogue"],
        lakehouse=estate["lakehouse"],
        publish_environment=False,
    )

    assert report.created == ()
    assert _status(report, "Catalogue") == EXISTING
    assert _status(report, "Lakehouse") == EXISTING
    assert _status(report, "Environment") == WRITTEN

    configured = load_workspace(tmp_path / "workspace-config.yml")
    assert configured.workspace == estate["workspace"]
    assert configured.catalogue == f"Warehouse/{estate['catalogue']}"
    assert [target.physical for target in configured.targets.values()] == [
        estate["lakehouse"]
    ]


@weaver_test(remote=True, resources={"rest"})
def test_a_rerun_converges(estate, tmp_path):
    """No rollback exists, so a run that stopped part-way is finished by repeating.

    The same request twice writes the same bytes and creates nothing, which is
    what makes repeating safe.
    """

    first = weaver.initialise(
        tmp_path,
        workspace=estate["workspace"],
        catalogue=estate["catalogue"],
        lakehouse=estate["lakehouse"],
        publish_environment=False,
    )
    second = weaver.initialise(
        tmp_path,
        workspace=estate["workspace"],
        catalogue=estate["catalogue"],
        lakehouse=estate["lakehouse"],
        publish_environment=False,
    )

    assert first.created == ()
    assert second.created == ()
    assert first.files == second.files
    assert CREATED not in {outcome.status for outcome in second.resources}


@weaver_test(remote=True, resources={"rest", "tds"})
def test_a_generated_warehouse_project_builds_loads_and_tests(
    fabric_workspace, fabric_catalogue, emptied_disposable_warehouse, tmp_path
):
    """The onboarding claim, end to end, on the cheapest topology that makes it.

    A Warehouse-only example reaches TDS and REST and starts no Spark. The
    Warehouse is emptied on the way in, which is how this suite isolates against
    a fixed estate.
    """

    with ConsoleSession(workspace=fabric_workspace) as session:
        register_session(session)
        report = weaver.initialise(
            tmp_path,
            workspace=fabric_workspace.workspace,
            catalogue=fabric_catalogue.name,
            warehouse=emptied_disposable_warehouse.item.name,
            example=True,
            publish_environment=False,
            session=session,
        )

    assert report.example.build == "succeeded"
    assert report.example.load == "succeeded"
    assert report.example.test == "passed"
    assert report.succeeded is True
