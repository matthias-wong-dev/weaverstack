"""The desktop lifecycle through one Session and its persisted evidence."""

from __future__ import annotations

import pytest
from support.build_envs import CROSS_ITEM_JOURNEY_FIXTURE, DESKTOP_JOURNEY_NAMES
from support.weaver_test import weaver_test

import weaver


@pytest.fixture(scope="module")
def desktop_estate(tmp_path_factory):
    return CROSS_ITEM_JOURNEY_FIXTURE.renamed(
        tmp_path_factory.mktemp("desktop-journey"), DESKTOP_JOURNEY_NAMES
    )


@weaver_test(integration=True, resources={"livy", "onelake", "rest", "tds"})
def test_the_desktop_drives_build_load_and_test_in_one_session(
    desktop_estate,
    weaver_session,
    fabric_workspace,
    fabric_target_lakehouse,
    disposable_warehouse,
    tmp_path,
):
    """Wipe, build, load and test against a real workspace."""

    lakehouse = f"Lakehouse/{fabric_target_lakehouse.name}"
    warehouse = f"Warehouse/{disposable_warehouse.item.name}"

    weaver.wipe(
        [lakehouse, warehouse],
        unbind_from=fabric_workspace.catalogue,
        session=weaver_session,
    )

    built = weaver.build(
        str(desktop_estate.path),
        bind=[f"{lakehouse}=Stock", f"{warehouse}=Analysis"],
        session=weaver_session,
    )
    assert built.status == "succeeded", [
        (failure.action_id, failure.message) for failure in built.errors
    ]

    loaded = weaver.load([lakehouse, warehouse], session=weaver_session)
    assert loaded.succeeded, loaded.to_mapping()

    tested = weaver.test([lakehouse, warehouse], session=weaver_session)
    totals = tested.totals()
    assert totals["failed"] == 0, tested.to_mapping()
    assert totals["invalid"] == 0, tested.to_mapping()
    assert totals["passed"], tested.to_mapping()

    weaver_session.flush()
    _assert_evidence(weaver_session, fabric_workspace, loaded, "load")
    _assert_evidence(weaver_session, fabric_workspace, tested, "test")

    composition = tmp_path / "compose.yml"
    composition.write_text(
        "compose:\n"
        "  verify:\n"
        f"    - weaver load {lakehouse} {warehouse}\n"
        f"    - weaver test {lakehouse} {warehouse}\n",
        encoding="utf-8",
    )
    previous_workflows = set(_workflow_task_types(weaver_session, fabric_workspace))

    from weaver_cli.compose import run_composition
    from weaver_cli.main import build_parser

    command = build_parser().parse_args(
        ["compose", "verify", "--file", str(composition), "--yes"]
    )
    command.session = weaver_session

    assert run_composition(command) == 0
    workflows = _workflow_task_types(weaver_session, fabric_workspace)
    composed = {
        workflow_id: task_types
        for workflow_id, task_types in workflows.items()
        if workflow_id not in previous_workflows
    }
    assert len(composed) == 1, composed
    workflow_id, task_types = next(iter(composed.items()))
    assert workflow_id
    assert task_types == {"load", "test"}


def _assert_evidence(session, workspace, report, task_type: str) -> None:
    assert report.workflow_id
    rows = _log_rows(session, workspace, report.workflow_id)
    assert len(rows) == len(report.nodes)
    assert {row["Workflow ID"] for row in rows} == {report.workflow_id}
    assert {row["Task type"] for row in rows} == {task_type}
    assert all(row["Log SK"] for row in rows)

    actual = {
        (
            row["Target type"],
            row["Target name"],
            row["Schema name"],
            row["Object name"],
            row["Result"],
        )
        for row in rows
    }
    expected = {(*_node_identity(node), "Succeeded") for node in report.nodes}
    assert actual == expected


def _log_rows(session, workspace, workflow_id: str) -> list[dict]:
    from weaver.catalogue.connection import catalogue_connection

    rows = catalogue_connection(session, workspace).rows(
        "select [Log SK], [Workflow ID], [Task type], [Target type], "
        "[Target name], [Schema name], [Object name], [Result] "
        "from [_].[Log] "
        f"where [Workflow ID] = N'{workflow_id}'"
    )
    return [dict(row) for row in rows]


def _workflow_task_types(session, workspace) -> dict[str, set[str]]:
    from weaver.catalogue.connection import catalogue_connection

    rows = catalogue_connection(session, workspace).rows(
        "select [Workflow ID], [Task type] from [_].[Log] "
        "where [Task type] in (N'load', N'test')"
    )
    workflows: dict[str, set[str]] = {}
    for row in rows:
        workflows.setdefault(str(row["Workflow ID"]), set()).add(row["Task type"])
    return workflows


def _node_identity(node) -> tuple[str | None, str | None, str | None, str | None]:
    target_type, _, target_name = str(node.physical_target).partition("/")
    logical = str(node.logical_id or "").rsplit("/", 1)[-1]
    schema, separator, object_name = logical.rpartition(".")
    if not separator:
        schema, object_name = None, logical or None
    return target_type or None, target_name or None, schema or None, object_name
