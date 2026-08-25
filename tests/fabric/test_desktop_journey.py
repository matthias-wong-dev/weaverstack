"""The desktop lifecycle through one Session and its persisted evidence."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

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

    before_load = datetime.now(timezone.utc) - timedelta(minutes=1)
    loaded = weaver.load([lakehouse, warehouse], session=weaver_session)
    assert loaded.succeeded, loaded.to_mapping()

    consumed = _consume_folder_changes(
        weaver_session,
        fabric_workspace,
        fabric_target_lakehouse.name,
        before_load,
    )
    assert consumed["paths_are_full"] is True
    assert consumed["datetimes_are_utc"] is True
    assert [path.rsplit("/", 1)[-1] for path in consumed["changed"]] == [
        "customers.csv"
    ]
    assert consumed["latest"] == consumed["changed"]
    assert consumed["deleted"] == []
    assert consumed["contents"] and "CustomerId" in consumed["contents"][0]

    tested = weaver.test([lakehouse, warehouse], session=weaver_session)
    totals = tested.totals()
    assert totals["failed"] == 0, tested.to_mapping()
    assert totals["invalid"] == 0, tested.to_mapping()
    assert totals["passed"], tested.to_mapping()

    weaver_session.flush()
    _assert_evidence(weaver_session, fabric_workspace, loaded, "load")
    _assert_evidence(weaver_session, fabric_workspace, tested, "test")
    _assert_load_state(weaver_session, fabric_workspace, loaded)
    _assert_test_state(weaver_session, fabric_workspace, tested)

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


def _consume_folder_changes(session, workspace, lakehouse: str, bookmark) -> dict:
    """Run the authored downstream API where the desktop load ran its Folder."""

    from weaver.sessions.program import RemoteProgram

    source = f"""
import sys
from datetime import datetime, timedelta
from pathlib import Path

from weaver import lakehouse_for
from weaver.resolution import resolver_for
from weaver.targets import ItemRef
from weaver.workspaces import Workspace

workspace = Workspace(
    workspace={workspace.workspace!r},
    catalogue={workspace.catalogue!r},
    environment={workspace.environment!r},
)
resolver = resolver_for(workspace)
destination = lakehouse_for(resolver, ItemRef({lakehouse!r}))
sys.path.insert(0, destination.files_root() + "/_/Load")

from Files.Raw__CustomerCsv import Raw__CustomerCsv

folder = Raw__CustomerCsv(spark, lakehouse=destination)
consumer = Raw__CustomerCsv(folder)
bookmark = datetime.fromisoformat({bookmark.isoformat()!r})
changed = consumer.files_since(bookmark)
latest = consumer.latest_files()
deleted = consumer.deleted_since(bookmark)
emit({{
    "changed": {{str(path): at.isoformat() for path, at in changed.items()}},
    "latest": {{str(path): at.isoformat() for path, at in latest.items()}},
    "deleted": [str(path) for path in deleted],
    "paths_are_full": all(
        isinstance(path, Path) and path.is_absolute()
        for path in (*changed, *latest)
    ),
    "datetimes_are_utc": all(
        at.utcoffset() == timedelta(0)
        for at in (*changed.values(), *latest.values())
    ),
    "contents": [path.read_text(encoding="utf-8") for path in changed],
}})
"""
    return session.execute_python(
        RemoteProgram(
            name="consume Folder changes",
            call=lambda: None,
            source=source,
        ),
        workspace=workspace,
    )


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


def _assert_load_state(session, workspace, report) -> None:
    """What the load left in the catalogue's current-state and history tables.

    The Runner's own path, end to end: this is the composition proof that a real
    load against a real estate leaves the operational record the pure-Python
    tests decide. Every object the load reported is here with the outcome it
    reported, and nothing carries a physical target, where an object lives is
    the Installation's to say.
    """

    loaded = {
        identity
        for identity in map(_recorded_identity, report.nodes)
        if identity is not None
    }

    status = _rows(
        session,
        workspace,
        "select [Schema name] as [schema], [Object name] as [object], "
        "[Result] as result, [Workflow ID] as workflow from [_].[LoadStatus]",
    )
    recorded = {(row["schema"], row["object"]) for row in status}
    assert loaded <= recorded
    assert {
        row["result"] for row in status if (row["schema"], row["object"]) in loaded
    } == {"Succeeded"}
    assert report.workflow_id in {row["workflow"] for row in status}

    statistics = _rows(
        session,
        workspace,
        "select [Schema name] as [schema], [Object name] as [object], "
        "[Rows read] as [read], [Is reload] as reload "
        f"from [_].[LoadStatistic] where [Workflow ID] = N'{report.workflow_id}'",
    )
    assert {(row["schema"], row["object"]) for row in statistics} == loaded
    assert not [row for row in statistics if row["reload"]]
    # At least one object read something: an estate where every count were zero
    # would satisfy the shape of this without the load having moved anything.
    assert [row for row in statistics if (row["read"] or 0) > 0]

    bookmarks = _rows(
        session,
        workspace,
        "select [Schema name] as [schema], [Object name] as [object] "
        "from [_].[Bookmark]",
    )
    # Every clean load advanced its bookmark. Loadable objects only: a view has
    # no load, so it is in the report's nodes and not here.
    assert {(row["schema"], row["object"]) for row in bookmarks} <= loaded


def _assert_test_state(session, workspace, report) -> None:
    """What the validation run left, and which kind each validation was."""

    validated = {
        identity
        for identity in map(_recorded_identity, report.nodes)
        if identity is not None
    }

    status = _rows(
        session,
        workspace,
        "select [Schema name] as [schema], [Object name] as [object], "
        "[Test type] as kind, [Result] as result, "
        "[Failure count] as failures from [_].[TestStatus] "
        f"where [Workflow ID] = N'{report.workflow_id}'",
    )

    assert {(row["schema"], row["object"]) for row in status} == validated
    assert {row["result"] for row in status} == {"Succeeded"}
    assert {row["kind"] for row in status} <= {"Test", "Assumption"}
    assert {row["failures"] for row in status} == {0}


def _rows(session, workspace, statement: str) -> list[dict]:
    from weaver.catalogue.connection import catalogue_connection

    return [
        dict(row) for row in catalogue_connection(session, workspace).rows(statement)
    ]


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
    """One node's identity as ``_.Log`` records it: the target, and the object.

    ``_.Log`` carries the object's ``Schema.Object`` as the run reports it, which
    for a Folder is the display spelling without its ``Files/`` prefix. The
    current-state tables key on the Registry's identity instead, which keeps the
    prefix. See :func:`_recorded_identity`.
    """

    target_type, _, target_name = str(node.physical_target).partition("/")
    logical = str(node.logical_id or "").rsplit("/", 1)[-1]
    schema, separator, object_name = logical.rpartition(".")
    if not separator:
        schema, object_name = None, logical or None
    return target_type or None, target_name or None, schema or None, object_name


def _recorded_identity(node) -> tuple[str, str] | None:
    """One node's ``(schema, object)`` as a current-state table keys it.

    Through the production functions, not by splitting the display id: a Folder's
    catalogue identity carries its ``Files/`` prefix, so a Folder and a Table of
    the same name stay apart, and a test that spelled it itself would disagree
    with the row.
    """

    from weaver.catalogue.claims import bookmark_row
    from weaver.declaration.model import WeaverDocumentId, parse_installed_identity

    if not node.logical_id:
        return None
    identity = parse_installed_identity(str(node.logical_id))
    if not isinstance(identity, WeaverDocumentId):
        return None
    row = bookmark_row(identity)
    return str(row["schema_name"]), str(row["object_name"])
