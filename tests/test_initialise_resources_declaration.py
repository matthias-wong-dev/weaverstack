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
    INSTALL,
    PLANNED,
    READY,
    UNCHANGED,
    UPDATED,
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

    def __init__(self, action: str = CREATED) -> None:
        self.action = action


class _Definition:
    """An Environment definition, as much of one as readiness is read from."""

    def __init__(self, *, weaver: bool) -> None:
        self._weaver = weaver

    def custom_libraries(self):
        return ()

    def external_libraries(self):
        return "dependencies:\n  - pip:\n" + (
            "      - weaverstack\n" if self._weaver else ""
        )


class _Resolver:
    """The one seam a doubled host still has to have: a REST client.

    Initialise reads the workspace through the Session's own client, so what it
    asked of Fabric is counted where the rest of Weaver's crossings are. This
    stands in for that client.
    """

    def __init__(self) -> None:
        self.client = object()


@pytest.fixture
def fabric(monkeypatch):
    """A workspace whose items are a list this test controls."""

    held: list[_Item] = []
    created: list[str] = []
    published: list[str] = []
    #: Whether the workspace's Environment already carries Weaver.
    weaver_installed = [True]

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
        published.append(environment if environment is not None else str(path))
        # Fabric now holds it with Weaver in it, which is what a rerun finds.
        weaver_installed[0] = True
        if environment is None:
            held.append(_Item("Weaver", resources.ENVIRONMENT))
            return _Publication(action=CREATED)
        return _Publication(action=UPDATED)

    monkeypatch.setattr(resources, "find_workspace", find_workspace)
    monkeypatch.setattr(resources, "list_items", list_items)
    monkeypatch.setattr(resources, "create_lakehouse", create_lakehouse)
    monkeypatch.setattr(resources, "create_warehouse", create_warehouse)
    monkeypatch.setattr(
        "weaver.fabric.publish_environment", publish_environment, raising=False
    )
    monkeypatch.setattr(
        "weaver.fabric.environment.read_definition",
        lambda item, *, client=None: _Definition(weaver=weaver_installed[0]),
    )
    monkeypatch.setattr(
        "weaver.fabric.environment.publish_state",
        lambda item, *, client=None: "Success" if weaver_installed[0] else "",
    )

    from weaver.sessions.testing import TestSession

    session = TestSession(resolver=_Resolver())
    return type(
        "Fabric",
        (),
        {
            "held": held,
            "created": created,
            "published": published,
            "weaver_installed": weaver_installed,
            "session": session,
        },
    )


def _initialise(tmp_path, fabric, **kwargs):
    defaults = {
        "workspace": WORKSPACE,
        "lakehouse": "Landing",
        "warehouse": "Curated",
        "example": False,
        "install_weaver": True,
        "session": fabric.session,
    }
    defaults.update(kwargs)
    return weaver.initialise(tmp_path, **defaults)


def _status(report, role: str) -> str:
    return next(outcome.status for outcome in report.resources if outcome.role == role)


def _action(report, role: str) -> str | None:
    return next(outcome.action for outcome in report.resources if outcome.role == role)


@weaver_test()
def test_missing_items_are_created_once_each(tmp_path, fabric):
    report = _initialise(tmp_path, fabric)

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

    report = _initialise(tmp_path, fabric)

    assert fabric.created == []
    assert _status(report, "Catalogue") == EXISTING
    assert _status(report, "Lakehouse") == EXISTING
    assert _status(report, "Warehouse") == EXISTING


@weaver_test()
def test_a_rerun_creates_nothing_twice(tmp_path, fabric):
    """Rerunning is how a run that stopped part-way is finished."""

    first = _initialise(tmp_path, fabric)
    second = _initialise(tmp_path, fabric)

    assert len(fabric.created) == 3
    assert first.created == (
        "Catalogue/Catalogue",
        "Environment/Weaver",
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

    report = _initialise(tmp_path, fabric)

    assert _status(report, "Lakehouse") == EXISTING


@weaver_test()
def test_a_name_held_by_another_kind_of_item_is_refused_before_anything_is_made(
    tmp_path, fabric
):
    fabric.held.append(_Item("Landing", resources.WAREHOUSE))

    with pytest.raises(InitialiseError, match="already exists"):
        _initialise(tmp_path, fabric)

    assert fabric.created == []
    assert not (tmp_path / "workspace-config.yml").exists()


@weaver_test()
def test_a_failed_creation_names_what_fabric_said(tmp_path, fabric, monkeypatch):
    def refuse(workspace, name, *, client=None):
        raise resources.CommandError("You don't have permission to create this item.")

    monkeypatch.setattr(resources, "create_warehouse", refuse)

    with pytest.raises(InitialiseError) as raised:
        _initialise(tmp_path, fabric)

    message = str(raised.value)
    assert "You don't have permission to create this item." in message
    assert "run `weaver initialise` again" in message


# --- the Fabric Environment ----------------------------------------------------
#
# Every project runs against one with Weaver installed in it. Three states, and
# one action that moves the first two to the third. Installing takes minutes, so
# a run that was not given consent stops before anything changes.


def _environment(fabric, *, present: bool, weaver: bool = True):
    """Put the workspace's Environment into one of the three states."""

    fabric.weaver_installed[0] = weaver
    if present:
        fabric.held.append(_Item("Weaver", resources.ENVIRONMENT))


@weaver_test()
def test_an_environment_with_weaver_in_it_is_used_as_it_is(tmp_path, fabric):
    _environment(fabric, present=True, weaver=True)

    report = _initialise(tmp_path, fabric)

    assert fabric.published == []
    assert _status(report, "Environment") == READY
    assert _action(report, "Environment") == UNCHANGED


@weaver_test()
def test_an_environment_without_weaver_has_weaver_installed_in_it(tmp_path, fabric):
    """Only Weaver's own libraries change: the Environment is somebody else's."""

    _environment(fabric, present=True, weaver=False)

    report = _initialise(tmp_path, fabric)

    assert fabric.published == ["Weaver"]
    assert _status(report, "Environment") == READY
    assert _action(report, "Environment") == UPDATED
    assert not (tmp_path / "Environment").exists()


@weaver_test()
def test_a_missing_environment_is_created_from_the_generated_definition(
    tmp_path, fabric
):
    _environment(fabric, present=False)

    report = _initialise(tmp_path, fabric)

    assert fabric.published == [str(tmp_path / "Environment" / "Weaver.Environment")]
    assert _status(report, "Environment") == READY
    assert _action(report, "Environment") == CREATED
    assert (tmp_path / "Environment" / "Weaver.Environment" / ".platform").is_file()
    assert "Environment/Weaver" in report.created


@weaver_test()
def test_no_definition_is_written_for_an_environment_the_workspace_has(
    tmp_path, fabric
):
    """That Environment is not this project's to describe."""

    _environment(fabric, present=True, weaver=True)

    report = _initialise(tmp_path, fabric)

    assert not [path for path in report.files if path.startswith("Environment/")]
    assert not (tmp_path / "Environment").exists()


@weaver_test()
def test_a_missing_environment_without_consent_stops_before_anything_changes(
    tmp_path, fabric
):
    _environment(fabric, present=False)

    with pytest.raises(InitialiseError) as raised:
        _initialise(tmp_path, fabric, install_weaver=False)

    message = str(raised.value)
    assert "The Fabric Environment 'Weaver' does not exist." in message
    assert "interactively" in message
    assert fabric.created == []
    assert list(tmp_path.iterdir()) == []


@weaver_test()
def test_an_unprepared_environment_without_consent_says_what_to_do(tmp_path, fabric):
    _environment(fabric, present=True, weaver=False)

    with pytest.raises(InitialiseError) as raised:
        _initialise(tmp_path, fabric, install_weaver=False)

    message = str(raised.value)
    assert "does not have Weaver installed" in message
    assert "prepare the Environment" in message
    assert fabric.created == []
    assert list(tmp_path.iterdir()) == []


@weaver_test()
def test_a_dry_run_shows_the_installation_without_consent(tmp_path, fabric):
    """A dry run changes nothing, so it needs no consent and asks for none."""

    _environment(fabric, present=True, weaver=False)

    report = _initialise(tmp_path, fabric, install_weaver=False, dry_run=True)

    assert _status(report, "Environment") == INSTALL
    assert fabric.published == []


@weaver_test()
def test_a_dry_run_changes_nothing(tmp_path, fabric):
    fabric.held.append(_Item("Landing", resources.LAKEHOUSE))

    report = _initialise(tmp_path, fabric, dry_run=True)

    assert fabric.created == []
    assert fabric.published == []
    assert list(tmp_path.iterdir()) == []
    assert _status(report, "Catalogue") == PLANNED
    assert _status(report, "Lakehouse") == EXISTING
    assert report.files


@weaver_test()
def test_a_project_directory_holding_edited_files_is_not_overwritten(tmp_path, fabric):
    _initialise(tmp_path, fabric)
    (tmp_path / "workspace-config.yml").write_text("workspace: Somewhere Else\n")

    with pytest.raises(InitialiseError, match="workspace-config.yml"):
        _initialise(tmp_path, fabric)


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
    """Build, load and test, each recording that it was called."""

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
    report = _initialise(tmp_path, fabric, example=True)

    assert operations == ["build", "load", "test"]
    assert report.example.generated is True
    assert report.example.succeeded is True
    assert report.succeeded is True


@weaver_test()
def test_no_example_runs_nothing(tmp_path, fabric, operations):
    report = _initialise(tmp_path, fabric, example=False)

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

    report = _initialise(tmp_path, fabric, example=True)

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

    report = _initialise(tmp_path, fabric, example=True)

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

    report = weaver.initialise(
        tmp_path, lakehouse="Landing", dry_run=True, session=fabric.session
    )

    assert report.workspace == WORKSPACE


@weaver_test()
def test_no_workspace_outside_fabric_says_how_to_give_one(
    tmp_path, fabric, monkeypatch
):
    monkeypatch.setattr("weaver.sessions.host.current_workspace_name", lambda: None)

    with pytest.raises(InitialiseError) as raised:
        weaver.initialise(tmp_path, lakehouse="Landing", session=fabric.session)

    message = str(raised.value)
    assert "A Fabric workspace could not be found." in message
    assert '--workspace "My Fabric Workspace"' in message
    assert "inside a Fabric notebook" in message


@weaver_test()
def test_a_project_keeps_the_environment_it_created(tmp_path, fabric):
    """A successful first run must not change what the second run generates.

    Reading ownership off Fabric alone would drop the definition from the
    generated set the moment the Environment existed, leaving a file in the
    project that initialise no longer recognised and `_refuse_edited_files` no
    longer protected.
    """

    _environment(fabric, present=False)

    first = _initialise(tmp_path, fabric)
    second = _initialise(tmp_path, fabric)

    definition = "Environment/Weaver.Environment/.platform"
    assert definition in first.files
    assert definition in second.files
    assert first.files == second.files
    assert (tmp_path / definition).is_file()


@weaver_test()
def test_a_rerun_writes_the_same_bytes(tmp_path, fabric, operations):
    """Same request, same project: the second run finds every file identical."""

    _environment(fabric, present=False)

    _initialise(tmp_path, fabric, example=True)
    written = {
        path.relative_to(tmp_path).as_posix(): path.read_text(encoding="utf-8")
        for path in sorted(tmp_path.rglob("*"))
        if path.is_file()
    }

    _initialise(tmp_path, fabric, example=True)

    assert {
        path.relative_to(tmp_path).as_posix(): path.read_text(encoding="utf-8")
        for path in sorted(tmp_path.rglob("*"))
        if path.is_file()
    } == written


@weaver_test()
def test_a_rerun_installs_weaver_no_second_time(tmp_path, fabric):
    """The Environment is ready after the first run, so nothing is published."""

    _environment(fabric, present=False)

    _initialise(tmp_path, fabric)
    fabric.published.clear()
    second = _initialise(tmp_path, fabric)

    assert fabric.published == []
    assert _status(second, "Environment") == READY
    assert _action(second, "Environment") == UNCHANGED


@weaver_test()
def test_an_environment_the_project_never_defined_stays_undefined(tmp_path, fabric):
    """One the workspace supplied is somebody else's, on the first run and after."""

    _environment(fabric, present=True, weaver=True)

    first = _initialise(tmp_path, fabric)
    second = _initialise(tmp_path, fabric)

    assert not [path for path in first.files if path.startswith("Environment/")]
    assert first.files == second.files
    assert not (tmp_path / "Environment").exists()
