"""Which Fabric items a run creates, and which it finds already there.

Naming an item to `initialise` is the request to have it, so the decision is
never a question put to the user; it is what the workspace already holds. That
listing is read once, and everything below is decided from it.

A rerun is the safety mechanism. Nothing is rolled back when a run stops
part-way, so the next run has to reach the same place from wherever the last one
got to, and these say it does.
"""

from __future__ import annotations

import pytest
from support.weaver_test import weaver_test

import weaver
from weaver.fabric import resources
from weaver.initialise import (
    CREATED,
    EXISTING,
    PLANNED,
    PUBLISHED,
    UNCHANGED,
    WRITTEN,
    InitialiseError,
)

WORKSPACE = "Weaver Example"


class _Workspace:
    id = "ws-1"
    name = WORKSPACE


class _Item:
    def __init__(self, name: str, item_type: str) -> None:
        self.id = f"id-{name}"
        self.name = name
        self.type = item_type
        self.workspace_id = "ws-1"


class _Publication:
    """What `publish_environment` answers, reduced to what initialise reads."""

    def __init__(self, published: bool) -> None:
        self.published = published


@pytest.fixture
def fabric(monkeypatch):
    """A workspace whose items are a list this test controls."""

    held: list[_Item] = []
    created: list[str] = []
    published: list[str] = []

    def find_workspace(name, *, client=None):
        return _Workspace()

    def list_items(workspace, *, item_type=None, client=None):
        return tuple(held)

    def create_lakehouse(workspace, name, *, client=None):
        created.append(f"Lakehouse/{name}")
        held.append(_Item(name, resources.LAKEHOUSE))
        # Fabric grows the SQL endpoint facet beside it, sharing the name.
        held.append(_Item(name, resources.SQL_ENDPOINT))
        return held[-2]

    def create_warehouse(workspace, name, *, client=None):
        created.append(f"Warehouse/{name}")
        held.append(_Item(name, resources.WAREHOUSE))
        return held[-1]

    def publish_environment(workspace_name, environment=None, *, path=None, **kwargs):
        published.append(str(path))
        return _Publication(published=True)

    monkeypatch.setattr(resources, "find_workspace", find_workspace)
    monkeypatch.setattr(resources, "list_items", list_items)
    monkeypatch.setattr(resources, "create_lakehouse", create_lakehouse)
    monkeypatch.setattr(resources, "create_warehouse", create_warehouse)
    monkeypatch.setattr(
        "weaver.fabric.publish_environment", publish_environment, raising=False
    )

    return type(
        "Fabric",
        (),
        {"held": held, "created": created, "published": published},
    )


def _initialise(tmp_path, **kwargs):
    defaults = {
        "workspace": WORKSPACE,
        "lakehouse": "Landing",
        "warehouse": "Curated",
        "example": False,
    }
    defaults.update(kwargs)
    return weaver.initialise(tmp_path, **defaults)


def _status(report, role: str) -> str:
    return next(outcome.status for outcome in report.resources if outcome.role == role)


@weaver_test()
def test_missing_items_are_created_once_each(tmp_path, fabric):
    report = _initialise(tmp_path)

    assert fabric.created == [
        "Warehouse/Catalogue",
        "Lakehouse/Landing",
        "Warehouse/Curated",
    ]
    assert _status(report, "Catalogue") == CREATED
    assert _status(report, "Lakehouse") == CREATED
    assert _status(report, "Warehouse") == CREATED


@weaver_test()
def test_existing_items_are_reused(tmp_path, fabric):
    fabric.held.extend(
        [
            _Item("Catalogue", resources.WAREHOUSE),
            _Item("Landing", resources.LAKEHOUSE),
            _Item("Curated", resources.WAREHOUSE),
        ]
    )

    report = _initialise(tmp_path)

    assert fabric.created == []
    assert _status(report, "Catalogue") == EXISTING
    assert _status(report, "Lakehouse") == EXISTING
    assert _status(report, "Warehouse") == EXISTING


@weaver_test()
def test_a_rerun_creates_nothing_twice(tmp_path, fabric):
    """Rerunning is how a run that stopped part-way is finished."""

    first = _initialise(tmp_path)
    second = _initialise(tmp_path)

    assert len(fabric.created) == 3
    assert first.created == (
        "Catalogue/Catalogue",
        "Lakehouse/Landing",
        "Warehouse/Curated",
    )
    assert second.created == ()


@weaver_test()
def test_a_lakehouses_sql_endpoint_is_not_a_conflict(tmp_path, fabric):
    """A Lakehouse and its generated endpoint share one display name."""

    fabric.held.extend(
        [
            _Item("Landing", resources.LAKEHOUSE),
            _Item("Landing", resources.SQL_ENDPOINT),
        ]
    )

    report = _initialise(tmp_path)

    assert _status(report, "Lakehouse") == EXISTING


@weaver_test()
def test_a_name_held_by_another_kind_of_item_is_refused_before_anything_is_made(
    tmp_path, fabric
):
    fabric.held.append(_Item("Landing", resources.WAREHOUSE))

    with pytest.raises(InitialiseError, match="already exists"):
        _initialise(tmp_path)

    assert fabric.created == []
    assert not (tmp_path / "workspace-config.yml").exists()


@weaver_test()
def test_a_failed_creation_names_what_fabric_said(tmp_path, fabric, monkeypatch):
    def refuse(workspace, name, *, client=None):
        raise resources.CommandError("You don't have permission to create this item.")

    monkeypatch.setattr(resources, "create_warehouse", refuse)

    with pytest.raises(InitialiseError) as raised:
        _initialise(tmp_path)

    message = str(raised.value)
    assert "You don't have permission to create this item." in message
    assert "run `weaver initialise` again" in message


@weaver_test()
def test_the_environment_is_published_from_a_desktop(tmp_path, fabric):
    report = _initialise(tmp_path)

    assert fabric.published == [str(tmp_path / "Environment" / "Weaver.Environment")]
    assert _status(report, "Environment") == PUBLISHED


@weaver_test()
def test_an_unchanged_environment_reports_no_publication(tmp_path, fabric, monkeypatch):
    monkeypatch.setattr(
        "weaver.fabric.publish_environment",
        lambda *args, **kwargs: _Publication(published=False),
        raising=False,
    )

    report = _initialise(tmp_path)

    assert _status(report, "Environment") == UNCHANGED


@weaver_test()
def test_publishing_can_be_declined(tmp_path, fabric):
    """What a notebook run does: the definition is written, and nothing waits."""

    report = _initialise(tmp_path, publish_environment=False)

    assert fabric.published == []
    assert _status(report, "Environment") == WRITTEN
    assert (tmp_path / "Environment" / "Weaver.Environment" / ".platform").is_file()


@weaver_test()
def test_a_dry_run_changes_nothing(tmp_path, fabric):
    fabric.held.append(_Item("Landing", resources.LAKEHOUSE))

    report = _initialise(tmp_path, dry_run=True)

    assert fabric.created == []
    assert fabric.published == []
    assert list(tmp_path.iterdir()) == []
    assert _status(report, "Catalogue") == PLANNED
    assert _status(report, "Lakehouse") == EXISTING
    assert report.files


@weaver_test()
def test_a_project_directory_holding_edited_files_is_not_overwritten(tmp_path, fabric):
    _initialise(tmp_path)
    (tmp_path / "workspace-config.yml").write_text("workspace: Somewhere Else\n")

    with pytest.raises(InitialiseError, match="workspace-config.yml"):
        _initialise(tmp_path)


# --- running the example -------------------------------------------------------


class _Built:
    status = "succeeded"
    succeeded = True


class _Failed:
    status = "failed"
    succeeded = False


class _Loaded:
    status = "succeeded"
    succeeded = True


class _Passed:
    status = "passed"
    succeeded = True


@pytest.fixture
def operations(monkeypatch):
    """Build, load and test, recorded rather than run."""

    ran: list[str] = []

    def build(source=None, **kwargs):
        ran.append("build")
        return _Built()

    def load(items=None, **kwargs):
        ran.append("load")
        return _Loaded()

    def test(items=None, **kwargs):
        ran.append("test")
        return _Passed()

    monkeypatch.setattr("weaver.operations.build.build", build)
    monkeypatch.setattr("weaver.operations.load.load", load)
    monkeypatch.setattr("weaver.operations.test.test", test)
    return ran


@weaver_test()
def test_the_example_is_built_loaded_and_tested(tmp_path, fabric, operations):
    report = _initialise(tmp_path, example=True)

    assert operations == ["build", "load", "test"]
    assert report.example.generated is True
    assert report.example.succeeded is True
    assert report.succeeded is True


@weaver_test()
def test_no_example_runs_nothing(tmp_path, fabric, operations):
    report = _initialise(tmp_path, example=False)

    assert operations == []
    assert report.example.ran is False
    assert report.succeeded is True


@weaver_test()
def test_a_failed_build_stops_before_the_load(
    tmp_path, fabric, operations, monkeypatch
):
    """The load would run against objects that were never installed."""

    monkeypatch.setattr(
        "weaver.operations.build.build", lambda *args, **kwargs: _Failed()
    )

    report = _initialise(tmp_path, example=True)

    assert operations == []
    assert report.example.build == "failed"
    assert report.example.load is None
    assert report.succeeded is False


@weaver_test()
def test_a_passing_test_is_a_success_however_it_spells_it(tmp_path, fabric, operations):
    """`test` reports `passed` and `load` reports `succeeded`.

    Each report says whether it succeeded, so the answer comes from them and is
    not read back off the word.
    """

    report = _initialise(tmp_path, example=True)

    assert report.example.test == "passed"
    assert report.example.succeeded is True


# --- which workspace a run addresses -------------------------------------------


@weaver_test()
def test_a_notebooks_own_workspace_is_used_when_none_is_named(
    tmp_path, fabric, monkeypatch
):
    monkeypatch.setattr(
        "weaver.sessions.host.current_workspace_name", lambda: WORKSPACE
    )

    report = weaver.initialise(tmp_path, lakehouse="Landing", dry_run=True)

    assert report.workspace == WORKSPACE


@weaver_test()
def test_no_workspace_outside_fabric_says_how_to_give_one(
    tmp_path, fabric, monkeypatch
):
    monkeypatch.setattr("weaver.sessions.host.current_workspace_name", lambda: None)

    with pytest.raises(InitialiseError) as raised:
        weaver.initialise(tmp_path, lakehouse="Landing")

    message = str(raised.value)
    assert "A Fabric workspace could not be found." in message
    assert '--workspace "My Fabric Workspace"' in message
    assert "inside a Fabric notebook" in message
